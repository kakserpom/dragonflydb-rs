//! In-process test harness mirroring the reference `BaseFamilyTest`.
//!
//! Each test gets a fresh server: shard threads, the coordinator thread and the
//! kqueue IO loop all run inside the test process (no binary is spawned), and
//! commands are issued over a real RESP socket like `Run(...)` does in the
//! C++ suite. `TestServer::stop` sends `SHUTDOWN`, which makes the IO loop
//! return, after which dropping the server closes every channel and the shard
//! and coordinator threads exit.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use dragonflydb::commands::lua::ScriptMgr;
use dragonflydb::server::event_loop::IoLoop;
use dragonflydb::server::replication::ReplChunk;
use dragonflydb::server::{ClientPause, Reply, ReplyBus, ServerEnv, Tracking, coordinator, shard};

/// A running in-process server. Dropping it shuts the server down.
pub struct TestServer {
    port: u16,
    io_handle: Option<JoinHandle<()>>,
}

/// A RESP client bound to one connection of a [`TestServer`].
pub struct Client {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
    /// RESP3 push frames (`>N`) received so far, in arrival order. A push is
    /// unsolicited (e.g. a CLIENT TRACKING invalidation), so `cmd` drains any
    /// pushes that precede a reply into this queue instead of returning them.
    pushes: std::collections::VecDeque<Vec<Value>>,
}

/// A decoded RESP value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Value>>),
    /// A RESP3 map (`%N\r\n`), e.g. the HELLO 3 reply.
    Map(Vec<(Value, Value)>),
}

impl Value {
    #[must_use]
    pub fn text(&self) -> Option<String> {
        match self {
            Value::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            Value::Simple(s) => Some(s.clone()),
            _ => None,
        }
    }

    #[must_use]
    pub fn arr(&self) -> Option<&[Value]> {
        match self {
            Value::Array(Some(v)) => Some(v),
            _ => None,
        }
    }

    #[must_use]
    pub fn int(&self) -> Option<i64> {
        match self {
            Value::Integer(n) => Some(*n),
            _ => None,
        }
    }

    /// The raw bytes of a non-null bulk string.
    #[must_use]
    pub fn bulk(&self) -> Option<&[u8]> {
        match self {
            Value::Bulk(Some(b)) => Some(b),
            _ => None,
        }
    }
}

fn free_listener() -> TcpListener {
    TcpListener::bind(("127.0.0.1", 0)).expect("bind")
}

fn set_nonblocking(fd: i32) {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags >= 0 {
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
    }
}

impl TestServer {
    /// Start a server with `num_shards` shards and the default script flags.
    #[must_use]
    pub fn start(num_shards: usize) -> Self {
        Self::start_with(num_shards, None)
    }

    /// Start a server with `num_shards` shards. `lua` configures the shared
    /// `ScriptMgr` the same way `--lua_*` flags do in the binary.
    #[must_use]
    pub fn start_with(num_shards: usize, lua: Option<LuaConfig>) -> Self {
        // Writing to a closed socket must not kill the test process.
        static SIGPIPE: std::sync::Once = std::sync::Once::new();
        SIGPIPE.call_once(|| unsafe {
            libc::signal(libc::SIGPIPE, libc::SIG_IGN);
        });

        // Reply bus + kqueue wakeup pipe.
        let (reply_tx, reply_rx) = mpsc::channel::<Reply>();
        let mut pipefds = [0i32; 2];
        let rc = unsafe { libc::pipe(pipefds.as_mut_ptr()) };
        assert_eq!(rc, 0, "pipe failed");
        set_nonblocking(pipefds[0]);
        set_nonblocking(pipefds[1]);
        let reply_bus = ReplyBus::new(reply_tx, pipefds[1]);

        // Stable-sync journal records from shards to their flow connections.
        let (repl_tx, repl_rx) = mpsc::channel::<ReplChunk>();
        let full_sync_bus =
            dragonflydb::server::replication::FullSyncBus::new(repl_tx.clone(), pipefds[1]);

        // Shard threads.
        let tracking = Arc::new(Mutex::new(Tracking::default()));
        let pause = Arc::new(ClientPause::default());
        let mut shard_txs = Vec::with_capacity(num_shards);
        for s in 0..num_shards {
            let (tx, rx) = mpsc::channel();
            let _ = shard::spawn(s, rx, tracking.clone(), reply_bus.clone());
            shard_txs.push(tx);
        }

        // Transaction coordinator thread.
        let (coord_tx, coord_rx) = mpsc::channel();
        let (gc_tx, gc_rx) = mpsc::channel();
        let mut mgr = ScriptMgr::new();
        if let Some(lua) = lua {
            mgr.lua_auto_async = lua.lua_auto_async;
            if let Err(e) = mgr.configure(
                &lua.default_lua_flags,
                lua.lua_undeclared_keys_shas,
                lua.lua_float_as_int_shas,
                lua.lua_allow_undeclared_auto_correct,
                lua.lua_resp2_legacy_float,
                lua.lua_enable_redis_log,
            ) {
                panic!("invalid lua config: {e}");
            }
        }
        let script_mgr = Arc::new(Mutex::new(mgr));
        let command_stats = Arc::new(Mutex::new(std::collections::HashMap::new()));
        coordinator::spawn(
            num_shards,
            coord_rx,
            gc_rx,
            shard_txs.clone(),
            reply_bus.clone(),
            script_mgr.clone(),
            command_stats.clone(),
        );

        let listener = free_listener();
        let port = listener.local_addr().unwrap().port();

        let env = ServerEnv {
            num_shards,
            shard_txs,
            coord_tx,
            gc_tx,
            reply_bus_tx: reply_bus,
            repl_tx,
            full_sync_bus,
            script_mgr,
            listen_port: port,
            command_stats,
            tracking,
            pause,
        };

        let mut io_loop = IoLoop::new(env, reply_rx, repl_rx, listener, pipefds[0]).unwrap();
        let io_handle = std::thread::Builder::new()
            .name("test-io".into())
            .spawn(move || {
                let _ = io_loop.run();
            })
            .expect("failed to spawn test io thread");

        TestServer {
            port,
            io_handle: Some(io_handle),
        }
    }

    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Open a fresh RESP connection to this server.
    #[must_use]
    pub fn client(&self) -> Client {
        Client::connect(self.port).expect("connect to test server")
    }

    /// Send `SHUTDOWN`, wait for the IO loop to return, and drop the env (which
    /// closes the shard/coordinator channels so those threads exit).
    pub fn stop(&mut self) {
        if let Some(handle) = self.io_handle.take() {
            let mut c = Client::connect(self.port).ok();
            if let Some(c) = c.as_mut() {
                let _ = c.cmd(&["SHUTDOWN"]);
            }
            let _ = handle.join();
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Script-manager configuration, mirroring the binary's `--lua_*` flags.
#[derive(Default)]
pub struct LuaConfig {
    pub lua_auto_async: bool,
    pub default_lua_flags: String,
    pub lua_undeclared_keys_shas: Vec<String>,
    pub lua_float_as_int_shas: Vec<String>,
    pub lua_allow_undeclared_auto_correct: bool,
    pub lua_resp2_legacy_float: bool,
    pub lua_enable_redis_log: bool,
}

impl Client {
    /// Connect with a generous read timeout so blocking commands have room to
    /// run.
    pub fn connect(port: u16) -> std::io::Result<Self> {
        let stream = TcpStream::connect(("127.0.0.1", port))?;
        stream.set_read_timeout(Some(Duration::from_secs(15)))?;
        stream.set_write_timeout(Some(Duration::from_secs(15)))?;
        Ok(Self {
            reader: BufReader::new(stream.try_clone()?),
            stream,
            pushes: std::collections::VecDeque::new(),
        })
    }

    /// Run a command with string arguments.
    pub fn cmd(&mut self, args: &[&str]) -> Result<Value, String> {
        let bytes: Vec<Vec<u8>> = args.iter().map(|a| a.as_bytes().to_vec()).collect();
        self.cmd_bytes(&bytes)
    }

    /// Run a command with arbitrary byte arguments (binary-safe).
    pub fn cmd_bytes(&mut self, args: &[Vec<u8>]) -> Result<Value, String> {
        let mut out = Vec::new();
        out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&out).map_err(|e| e.to_string())?;
        self.read_value()
    }

    /// Send raw bytes verbatim as the request.
    pub fn send_raw(&mut self, bytes: &[u8]) -> Result<Value, String> {
        self.stream.write_all(bytes).map_err(|e| e.to_string())?;
        self.read_value()
    }

    /// Read the reply to a command, draining any RESP3 push frames received
    /// before it into [`Self::pushes`] (invalidation broadcasts, pub/sub
    /// messages). Pushes are stored as their inner arrays.
    fn read_value(&mut self) -> Result<Value, String> {
        loop {
            let frame = self.read_frame()?;
            match frame {
                Frame::Push(items) => self.pushes.push_back(items),
                Frame::Reply(v) => return Ok(v),
            }
        }
    }

    fn read_frame(&mut self) -> Result<Frame, String> {
        let t = self.next_type()?;
        if t == b'>' {
            let mut line = String::new();
            self.reader
                .read_line(&mut line)
                .map_err(|e| e.to_string())?;
            let n: i64 = line[1..]
                .trim()
                .parse()
                .map_err(|_| "bad push count".to_string())?;
            let mut items = Vec::with_capacity(n as usize);
            for _ in 0..n {
                items.push(self.parse_value()?);
            }
            return Ok(Frame::Push(items));
        }
        Ok(Frame::Reply(self.parse_value()?))
    }

    /// The next byte in the stream, without consuming it.
    fn next_type(&mut self) -> Result<u8, String> {
        let buf = self.reader.fill_buf().map_err(|e| e.to_string())?;
        if buf.is_empty() {
            return Err("connection closed".into());
        }
        Ok(buf[0])
    }

    fn parse_value(&mut self) -> Result<Value, String> {
        let mut line = String::new();
        self.reader
            .read_line(&mut line)
            .map_err(|e| e.to_string())?;
        let bytes = line.as_bytes();
        match bytes[0] {
            b'+' => Ok(Value::Simple(line[1..].trim_end().to_string())),
            b'-' => Ok(Value::Error(line[1..].trim_end().to_string())),
            b':' => {
                let n = line[1..]
                    .trim()
                    .parse()
                    .map_err(|_| "bad int".to_string())?;
                Ok(Value::Integer(n))
            }
            b'$' => {
                let n: i64 = line[1..]
                    .trim()
                    .parse()
                    .map_err(|_| "bad len".to_string())?;
                if n < 0 {
                    return Ok(Value::Bulk(None));
                }
                let mut buf = vec![0u8; n as usize];
                self.reader
                    .read_exact(&mut buf)
                    .map_err(|e| e.to_string())?;
                let mut crlf = [0u8; 2];
                self.reader
                    .read_exact(&mut crlf)
                    .map_err(|e| e.to_string())?;
                Ok(Value::Bulk(Some(buf)))
            }
            b'*' => {
                let n: i64 = line[1..]
                    .trim()
                    .parse()
                    .map_err(|_| "bad count".to_string())?;
                if n < 0 {
                    return Ok(Value::Array(None));
                }
                let mut items = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    items.push(self.parse_value()?);
                }
                Ok(Value::Array(Some(items)))
            }
            b'%' => {
                let n: i64 = line[1..]
                    .trim()
                    .parse()
                    .map_err(|_| "bad map count".to_string())?;
                let mut pairs = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    let k = self.parse_value()?;
                    let v = self.parse_value()?;
                    pairs.push((k, v));
                }
                Ok(Value::Map(pairs))
            }
            b'#' => {
                let v = line[1..].trim();
                match v {
                    "t" => Ok(Value::Integer(1)),
                    "f" => Ok(Value::Integer(0)),
                    _ => Err("bad bool".to_string()),
                }
            }
            other => Err(format!("unexpected reply byte {other}")),
        }
    }

    /// The number of push frames received so far.
    #[must_use]
    pub fn push_count(&self) -> usize {
        self.pushes.len()
    }

    /// Block until the next push frame arrives and return its inner array.
    /// Reads without interpreting a reply, so it can be called right after a
    /// write on another connection produced an invalidation.
    pub fn read_push(&mut self) -> Vec<Value> {
        if let Some(items) = self.pushes.pop_front() {
            return items;
        }
        match self.read_frame().expect("read push frame") {
            Frame::Push(items) => items,
            Frame::Reply(v) => panic!("expected a push, got reply {v:?}"),
        }
    }
}

enum Frame {
    Reply(Value),
    Push(Vec<Value>),
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }
}

// ---------------------------------------------------------------------------
// Assertion helpers, mirroring `RunOk` / `RunInt` / `RunArr` / `RunErr`.
// ---------------------------------------------------------------------------

/// A fresh server plus a primary connection, mirroring `BaseFamilyTest`'s
/// `Run(...)` helpers. Each test makes its own `Ctx` so tests stay isolated.
pub struct Ctx {
    pub server: TestServer,
    pub c: Client,
}

impl Default for Ctx {
    fn default() -> Self {
        Self::new()
    }
}

impl Ctx {
    #[must_use]
    pub fn new() -> Self {
        Self::shards(2)
    }

    /// A context with `num_shards` shards (the reference tests use 2 shards).
    #[must_use]
    pub fn shards(num_shards: usize) -> Self {
        let server = TestServer::start(num_shards);
        let c = server.client();
        Self { server, c }
    }

    /// A context with custom script-manager configuration.
    #[must_use]
    pub fn with_lua(num_shards: usize, lua: LuaConfig) -> Self {
        let server = TestServer::start_with(num_shards, Some(lua));
        let c = server.client();
        Self { server, c }
    }

    /// Run a command with string arguments, unwrapping the reply.
    #[track_caller]
    pub fn run(&mut self, args: &[&str]) -> Value {
        self.c
            .cmd(args)
            .unwrap_or_else(|e| panic!("{e} for command {args:?}"))
    }

    /// Run a command with binary arguments, unwrapping the reply.
    pub fn run_b(&mut self, args: &[Vec<u8>]) -> Value {
        self.c.cmd_bytes(args).expect("command failed")
    }

    /// Run a command on a fresh connection in a background thread (for blocking
    /// commands like BLPOP that must not tie up the primary connection).
    #[must_use]
    pub fn spawn(&self, args: &[&str]) -> JoinHandle<Value> {
        let bytes: Vec<Vec<u8>> = args.iter().map(|a| a.as_bytes().to_vec()).collect();
        self.spawn_b(&bytes)
    }

    /// Like [`Self::spawn`] but with binary arguments.
    #[must_use]
    pub fn spawn_b(&self, args: &[Vec<u8>]) -> JoinHandle<Value> {
        let port = self.server.port();
        let args: Vec<Vec<u8>> = args.to_vec();
        std::thread::spawn(move || {
            let mut c = Client::connect(port).expect("connect to test server");
            c.cmd_bytes(&args).expect("command failed")
        })
    }

    /// Run `f` on a fresh connection in a background thread, returning its
    /// last reply. Useful for sequences like `SELECT` + a blocking command.
    #[must_use]
    pub fn spawn_fn(
        &self,
        f: impl FnOnce(&mut Client) -> Value + Send + 'static,
    ) -> JoinHandle<Value> {
        let port = self.server.port();
        std::thread::spawn(move || {
            let mut c = Client::connect(port).expect("connect to test server");
            f(&mut c)
        })
    }

    pub fn ok(&mut self, args: &[&str]) {
        let v = self.run(args);
        expect_ok(&v);
    }

    pub fn ok_b(&mut self, args: &[Vec<u8>]) {
        let v = self.run_b(args);
        expect_ok(&v);
    }

    pub fn int(&mut self, args: &[&str]) -> i64 {
        let v = self.run(args);
        v.int()
            .unwrap_or_else(|| panic!("expected integer, got {v:?}"))
    }

    pub fn text(&mut self, args: &[&str]) -> String {
        let v = self.run(args);
        v.text()
            .unwrap_or_else(|| panic!("expected bulk, got {v:?}"))
    }

    pub fn bulk(&mut self, args: &[&str]) -> Vec<u8> {
        let v = self.run(args);
        v.bulk()
            .map(<[u8]>::to_vec)
            .unwrap_or_else(|| panic!("expected bulk, got {v:?}"))
    }

    /// The raw (possibly null) bulk reply.
    pub fn bulk_opt(&mut self, args: &[&str]) -> Option<Vec<u8>> {
        let v = self.run(args);
        match v {
            Value::Bulk(b) => b,
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    pub fn err(&mut self, args: &[&str]) -> String {
        let v = self.run(args);
        match v {
            Value::Error(e) => e,
            other => panic!("expected error, got {other:?}"),
        }
    }

    pub fn arr(&mut self, args: &[&str]) -> Vec<Value> {
        let v = self.run(args);
        v.arr()
            .map(<[Value]>::to_vec)
            .unwrap_or_else(|| panic!("expected array, got {v:?}"))
    }

    /// Assert the reply is `+OK`.
    #[track_caller]
    pub fn assert_ok(&mut self, args: &[&str]) {
        self.ok(args);
    }

    /// Assert the reply is the integer `n`.
    #[track_caller]
    pub fn assert_int(&mut self, args: &[&str], n: i64) {
        let v = self.run(args);
        expect_int(&v, n);
    }

    /// Assert the reply is an error containing `substr`.
    pub fn assert_err(&mut self, args: &[&str], substr: &str) {
        let v = self.run(args);
        expect_err(&v, substr);
    }

    /// Assert the reply is the bulk string `s`.
    pub fn assert_text(&mut self, args: &[&str], s: &str) {
        let v = self.run(args);
        expect_text(&v, s);
    }

    /// Assert the reply is a null.
    pub fn assert_null(&mut self, args: &[&str]) {
        let v = self.run(args);
        expect_null(&v);
    }

    /// A second, independent connection to the same server.
    #[must_use]
    pub fn conn(&self) -> Client {
        self.server.client()
    }

    /// The number of RESP3 push frames the primary connection has received.
    #[must_use]
    pub fn push_count(&self) -> usize {
        self.c.push_count()
    }

    /// Block for the next RESP3 push frame on the primary connection.
    #[must_use]
    pub fn read_push(&mut self) -> Vec<Value> {
        self.c.read_push()
    }
}

/// Assert that a reply is `+OK` (or a bulk `OK`).
#[track_caller]
pub fn expect_ok(v: &Value) {
    assert!(v.text().as_deref() == Some("OK"), "expected OK, got {v:?}");
}

/// Assert that a reply is an integer with the given value.
#[track_caller]
pub fn expect_int(v: &Value, n: i64) {
    assert_eq!(v.int(), Some(n), "expected integer {n}, got {v:?}");
}

/// Assert that a reply is an error containing `substr` (case-sensitive on the
/// full error text after `ERR `).
#[track_caller]
pub fn expect_err(v: &Value, substr: &str) {
    match v {
        Value::Error(e) => assert!(
            e.contains(substr),
            "expected error containing {substr:?}, got {e:?}"
        ),
        other => panic!("expected error containing {substr:?}, got {other:?}"),
    }
}

/// Assert that a reply is the exact error text `substr`.
pub fn expect_err_exact(v: &Value, substr: &str) {
    match v {
        Value::Error(e) => assert_eq!(e, substr, "expected exact error {substr:?}, got {e:?}"),
        other => panic!("expected exact error {substr:?}, got {other:?}"),
    }
}

/// Assert that a reply is a bulk string with the given text.
pub fn expect_text(v: &Value, s: &str) {
    assert_eq!(
        v.text().as_deref(),
        Some(s),
        "expected bulk {s:?}, got {v:?}"
    );
}

/// Assert that a reply is a null bulk / null array.
pub fn expect_null(v: &Value) {
    assert!(
        matches!(v, Value::Bulk(None) | Value::Array(None)),
        "expected null, got {v:?}"
    );
}

/// Wait (up to `timeout`) until `f` returns true, polling every 20ms.
pub fn wait_for(timeout: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !f() {
        assert!(
            Instant::now() < deadline,
            "wait_for timed out after {timeout:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Serializes the tests that observe time: the fake clock is process-global
/// (like the reference's `TEST_current_time_ms`), so time-dependent tests must
/// run one at a time, pinning their own base and advancing it alone. Tests
/// without TTL assertions run in parallel and ignore the clock.
//
// The clock helpers are only used by the families with TTL tests
// (hset/generic/string/list); bitops_family includes `common` but never
// advances the clock.
#[allow(dead_code)]
static CLOCK: Mutex<()> = Mutex::new(());

/// Pin the fake clock (idempotent; keeps a base a previous test advanced) and
/// hold the serialization lock for the rest of the test.
#[allow(dead_code)]
pub fn clock_guard() -> std::sync::MutexGuard<'static, ()> {
    let g = CLOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _ = dragonflydb::commands::exec::server::pin_test_clock();
    g
}

/// Advance the pinned fake clock by `ms` (the reference's `AdvanceTime`).
#[allow(dead_code)]
pub fn advance(ms: u64) {
    dragonflydb::commands::exec::server::advance_test_clock(ms);
}

/// The pinned clock's current value, in epoch milliseconds.
// `clock_ms` is only used by the families that read absolute timestamps
// (hset/generic/string); list_family only advances.
#[allow(dead_code)]
pub fn clock_ms() -> u64 {
    dragonflydb::commands::exec::server::test_clock_ms()
}
