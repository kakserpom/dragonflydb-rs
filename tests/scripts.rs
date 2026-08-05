//! Script (`EVAL`) semantics integration tests.
//!
//! Spawns the real binary and asserts the Dragonfly script-model behaviors
//! ported for full parity:
//! - `disable-atomicity` (`NON_ATOMIC` mode) scripts may touch undeclared keys,
//!   while atomic (`LOCK_AHEAD`) scripts reject them;
//! - `dragonfly.lock`/`dragonfly.unlock`/`dragonfly.randstr`/`dragonfly.ihash`
//!   work through the real interpreter and dispatcher.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_dragonflydb");

/// A minimal RESP2 client for the test assertions.
struct Client {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

#[derive(Debug)]
#[allow(dead_code)]
enum Value {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Value>>),
}

impl Client {
    fn connect(port: u16) -> std::io::Result<Self> {
        let stream = TcpStream::connect(("127.0.0.1", port))?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        Ok(Self {
            reader: BufReader::new(stream.try_clone()?),
            stream,
        })
    }

    fn cmd(&mut self, args: &[&str]) -> Result<Value, String> {
        let mut out = Vec::new();
        out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&out).map_err(|e| e.to_string())?;
        self.read_value()
    }

    fn read_value(&mut self) -> Result<Value, String> {
        let mut line = String::new();
        self.reader.read_line(&mut line).map_err(|e| e.to_string())?;
        let bytes = line.as_bytes();
        match bytes[0] {
            b'+' => Ok(Value::Simple(line[1..].trim_end().to_string())),
            b'-' => Ok(Value::Error(line[1..].trim_end().to_string())),
            b':' => {
                let n = line[1..].trim().parse().map_err(|_| "bad int".to_string())?;
                Ok(Value::Integer(n))
            }
            b'$' => {
                let n: i64 = line[1..].trim().parse().map_err(|_| "bad len".to_string())?;
                if n < 0 {
                    return Ok(Value::Bulk(None));
                }
                let mut buf = vec![0u8; n as usize];
                self.reader.read_exact(&mut buf).map_err(|e| e.to_string())?;
                self.reader.read_line(&mut line).map_err(|e| e.to_string())?;
                Ok(Value::Bulk(Some(buf)))
            }
            b'*' => {
                let n: i64 = line[1..].trim().parse().map_err(|_| "bad count".to_string())?;
                if n < 0 {
                    return Ok(Value::Array(None));
                }
                let mut items = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    items.push(self.read_value()?);
                }
                Ok(Value::Array(Some(items)))
            }
            other => Err(format!("unexpected reply byte {other}")),
        }
    }
}

impl Value {
    fn text(&self) -> Option<String> {
        match self {
            Value::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            Value::Simple(s) => Some(s.clone()),
            _ => None,
        }
    }
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    l.local_addr().unwrap().port()
}

fn spawn(port: u16, shards: usize) -> Child {
    Command::new(BIN)
        .args(["--port", &port.to_string(), "--num-shards", &shards.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn dragonflydb")
}

/// Wait until `f` returns true, polling every 50ms up to `timeout`.
fn wait_for(timeout: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !f() {
        assert!(
            Instant::now() <= deadline,
            "timed out waiting for condition"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_ready(port: u16) {
    wait_for(Duration::from_secs(15), || {
        let Ok(mut c) = Client::connect(port) else {
            return false;
        };
        matches!(c.cmd(&["PING"]), Ok(Value::Simple(s)) if s == "PONG")
    });
}

fn bulk(client: &mut Client, args: &[&str]) -> String {
    match client.cmd(args) {
        Ok(v) => v.text().unwrap_or_else(|| "<nil>".to_string()),
        Err(e) => panic!("{args:?} failed: {e}"),
    }
}

fn eval_error(client: &mut Client, body: &str) -> String {
    match client.cmd(&["EVAL", body, "1", "a", "value"]) {
        Ok(Value::Error(e)) => e,
        other => panic!("expected EVAL error, got {other:?}"),
    }
}

#[test]
fn non_atomic_scripts_allow_undeclared_keys() {
    let port = free_port();
    let mut server = spawn(port, 2);
    wait_ready(port);

    let mut c = Client::connect(port).unwrap();
    ok(&mut c, &["SET", "a", "v"]);

    // A `disable-atomicity` script may touch keys it did not declare
    // (`CheckKeysDeclared` skips NON_ATOMIC mode), writing to a shard it does
    // not hold up front. The write and read-back run through per-call locks.
    let body = "--!df flags=disable-atomicity\n\
                redis.call('set', 'undeclared', ARGV[1])\n\
                return redis.call('get', 'undeclared')";
    assert_eq!(bulk(&mut c, &["EVAL", body, "1", "a", "value"]), "value");
    assert_eq!(bulk(&mut c, &["GET", "undeclared"]), "value");

    // Without the flag the same body is atomic (`LOCK_AHEAD`) and the
    // undeclared access is rejected.
    let bare = "redis.call('set', 'undeclared', ARGV[1])\n\
                return redis.call('get', 'undeclared')";
    let err = eval_error(&mut c, bare);
    assert!(
        err.contains("script tried accessing undeclared key, key: undeclared"),
        "{err}"
    );

    drop(c);
    server.kill().ok();
    server.wait().ok();
}

#[test]
fn dragonfly_lock_unlock_round_trip() {
    let port = free_port();
    let mut server = spawn(port, 2);
    wait_ready(port);

    let mut c = Client::connect(port).unwrap();
    ok(&mut c, &["SET", "a", "v"]);

    // `dragonfly.lock` pins the shard of an undeclared key so the atomicity
    // flag can be raised, and `dragonfly.unlock` releases everything and drops
    // back to per-call scheduling (`CallFromScript` LOCK/UNLOCK).
    let body = "--!df flags=disable-atomicity\n\
                dragonfly.lock('undeclared')\n\
                redis.call('set', 'undeclared', 'v')\n\
                dragonfly.unlock()\n\
                return redis.call('get', 'undeclared')";
    assert_eq!(bulk(&mut c, &["EVAL", body, "1", "a"]), "v");
    assert_eq!(bulk(&mut c, &["GET", "undeclared"]), "v");

    // `unlock` alone in an atomic script releases the upfront locks; the
    // following commands re-schedule per call.
    let body = "dragonfly.unlock()\n\
                redis.call('set', 'undeclared', 'w')\n\
                return redis.call('get', 'undeclared')";
    assert_eq!(bulk(&mut c, &["EVAL", body, "1", "a"]), "w");
    assert_eq!(bulk(&mut c, &["GET", "undeclared"]), "w");

    drop(c);
    server.kill().ok();
    server.wait().ok();
}

#[test]
fn dragonfly_helpers_work_end_to_end() {
    let port = free_port();
    let mut server = spawn(port, 2);
    wait_ready(port);

    let mut c = Client::connect(port).unwrap();

    // randstr: byte-for-byte against the reference (glibc rand seed 1 + the
    // DRAGONFLY pattern).
    assert_eq!(
        bulk(&mut c, &["EVAL", "return dragonfly.randstr(16)", "0"]),
        "DRAGONFLYas7Vpl8"
    );

    // ihash: a deterministic integer, stable across runs; MGET hashes all keys.
    ok(&mut c, &["SET", "k1", "v1"]);
    ok(&mut c, &["SET", "k2", "v2"]);
    let h = match c.cmd(&["EVAL", "return dragonfly.ihash(0, false, 'mget', 'k1', 'k2')", "0"]) {
        Ok(Value::Integer(h)) => h,
        other => panic!("expected ihash integer, got {other:?}"),
    };
    assert_eq!(
        match c.cmd(&["EVAL", "return dragonfly.ihash(0, false, 'mget', 'k1', 'k2')", "0"]) {
            Ok(Value::Integer(h)) => h,
            other => panic!("expected ihash integer, got {other:?}"),
        },
        h,
        "ihash must be deterministic"
    );

    drop(c);
    server.kill().ok();
    server.wait().ok();
}

#[test]
fn blocking_commands_in_scripts_match_reference() {
    let port = free_port();
    let mut server = spawn(port, 2);
    wait_ready(port);

    let mut c = Client::connect(port).unwrap();

    // NOSCRIPT blocking commands (BLPOP/BRPOP/BRPOPLPUSH/BZPOPMIN/BZPOPMAX) are
    // rejected from scripts in both the reference and the port; BLMOVE/BLMPOP/
    // BZMPOP are not flagged NOSCRIPT and run with blocking disabled.
    let err = eval_error(&mut c, "return redis.call('blpop', KEYS[1], 0)");
    assert!(
        err.contains("This Redis command is not allowed from script"),
        "{err}"
    );

    // Blocking is disabled inside scripts (the transaction is a multi): an
    // empty source replies nil instead of suspending (`is_multi` in the
    // reference's `BPopPusher`/`RunCbOnFirstNonEmptyBlocking`).
    for (body, keys) in [
        (
            "return redis.call('blmove', KEYS[1], KEYS[2], 'left', 'right', 0)",
            &["a", "b"][..],
        ),
        ("return redis.call('blmpop', 0, 1, KEYS[1], 'left')", &["a"][..]),
        ("return redis.call('bzmpop', 0, 1, KEYS[1], 'min')", &["z"][..]),
    ] {
        let n = keys.len().to_string();
        let mut args = vec!["EVAL", body, &n];
        args.extend_from_slice(keys);
        match c.cmd(&args) {
            Ok(Value::Bulk(None)) => {}
            other => panic!("expected nil from {body}, got {other:?}"),
        }
    }

    // With data present the same commands operate normally.
    assert!(matches!(c.cmd(&["RPUSH", "a", "x", "y"]), Ok(Value::Integer(2))));
    assert!(matches!(c.cmd(&["ZADD", "z", "1", "m1"]), Ok(Value::Integer(1))));
    let moved = c.cmd(&["EVAL", "return redis.call('blmove', KEYS[1], KEYS[2], 'left', 'right', 0)", "2", "a", "b"]);
    assert_eq!(moved.unwrap().text().as_deref(), Some("x"));
    let dest = c.cmd(&["LRANGE", "b", "0", "-1"]);
    assert!(matches!(dest, Ok(Value::Array(Some(items))) if items.len() == 1));

    drop(c);
    server.kill().ok();
    server.wait().ok();
}

fn ok(client: &mut Client, args: &[&str]) {
    match client.cmd(args) {
        Ok(Value::Simple(s)) => assert_eq!(s, "OK", "{args:?}"),
        other => panic!("{args:?} expected +OK, got {other:?}"),
    }
}
