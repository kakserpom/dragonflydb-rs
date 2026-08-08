//! Two-instance master -> replica replication test.
//!
//! Spawns a master and a replica of the real binary, wires them with
//! `REPLICAOF`, and asserts that the full-sync snapshot and the stable-sync
//! journal (single- and multi-key commands, global commands, expiry metadata)
//! are reproduced on the replica, and that a replica rejects writes.

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
enum Value {
    Simple(String),
    Error,
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
        self.reader
            .read_line(&mut line)
            .map_err(|e| e.to_string())?;
        let bytes = line.as_bytes();
        match bytes[0] {
            b'+' => Ok(Value::Simple(line[1..].trim_end().to_string())),
            b'-' => Ok(Value::Error),
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
                self.reader
                    .read_line(&mut line)
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

    fn is_err(&self) -> bool {
        matches!(self, Value::Error)
    }
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    l.local_addr().unwrap().port()
}

fn spawn(port: u16, shards: usize) -> Child {
    Command::new(BIN)
        .args([
            "--port",
            &port.to_string(),
            "--num-shards",
            &shards.to_string(),
        ])
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

/// Wait until the replica's ROLE state field equals `target`.
fn wait_replica_state(port: u16, target: &str) {
    wait_for(Duration::from_secs(20), || {
        let Ok(mut c) = Client::connect(port) else {
            return false;
        };
        matches!(
            c.cmd(&["ROLE"]),
            Ok(Value::Array(Some(items))) if items.get(3).and_then(Value::text).as_deref() == Some(target)
        )
    });
}

fn bulk(client: &mut Client, args: &[&str]) -> String {
    match client.cmd(args) {
        Ok(v) => v.text().unwrap_or_else(|| "<nil>".to_string()),
        Err(e) => panic!("{args:?} failed: {e}"),
    }
}

/// The array reply's elements concatenated (used for LRANGE/MGET, which are
/// ordered).
fn list(client: &mut Client, args: &[&str]) -> String {
    match client.cmd(args) {
        Ok(Value::Array(Some(items))) => items.iter().filter_map(Value::text).collect(),
        other => panic!("{args:?} expected array, got {other:?}"),
    }
}

/// The array reply's elements sorted (used for SMEMBERS, which has no order
/// guarantee).
fn set(client: &mut Client, args: &[&str]) -> String {
    let mut members: Vec<String> = match client.cmd(args) {
        Ok(Value::Array(Some(items))) => items.iter().filter_map(Value::text).collect(),
        other => panic!("{args:?} expected array, got {other:?}"),
    };
    members.sort();
    members.concat()
}

fn int(client: &mut Client, args: &[&str]) -> i64 {
    match client.cmd(args) {
        Ok(Value::Integer(n)) => n,
        other => panic!("{args:?} expected integer, got {other:?}"),
    }
}

fn ok(client: &mut Client, args: &[&str]) {
    match client.cmd(args) {
        Ok(Value::Simple(s)) => assert_eq!(s, "OK", "{args:?}"),
        other => panic!("{args:?} expected +OK, got {other:?}"),
    }
}

#[test]
fn master_to_replica_replication() {
    let master_port = free_port();
    let replica_port = free_port();
    let mut master = spawn(master_port, 2);
    let mut replica = spawn(replica_port, 2);
    wait_ready(master_port);
    wait_ready(replica_port);

    let mut m = Client::connect(master_port).unwrap();
    let mut r = Client::connect(replica_port).unwrap();

    // Master data before the replica attaches.
    ok(&mut m, &["SET", "foo", "bar"]);
    ok(&mut m, &["SET", "num", "42"]);
    int(&mut m, &["RPUSH", "mylist", "a", "b", "c"]);
    int(&mut m, &["SADD", "myset", "x", "y", "z"]);
    ok(&mut m, &["SET", "with-ttl", "v", "EX", "100"]);

    // Attach the replica.
    ok(
        &mut r,
        &["REPLICAOF", "localhost", &master_port.to_string()],
    );
    wait_replica_state(replica_port, "connected");

    // Full-sync snapshot reproduced.
    assert_eq!(bulk(&mut r, &["GET", "foo"]), "bar");
    assert_eq!(bulk(&mut r, &["GET", "num"]), "42");
    assert_eq!(list(&mut r, &["LRANGE", "mylist", "0", "-1"]), "abc");
    assert_eq!(set(&mut r, &["SMEMBERS", "myset"]), "xyz");
    let ttl = int(&mut r, &["TTL", "with-ttl"]);
    assert!((90..=100).contains(&ttl), "unexpected ttl {ttl}");

    // A replica rejects writes but serves reads.
    assert!(
        r.cmd(&["SET", "blocked", "x"]).unwrap().is_err(),
        "replica write should fail"
    );
    assert_eq!(bulk(&mut r, &["GET", "foo"]), "bar");

    // Stable-sync journal: single-key commands.
    ok(&mut m, &["SET", "foo", "newbar"]);
    int(&mut m, &["INCR", "num"]);
    int(&mut m, &["RPUSH", "mylist", "d"]);
    int(&mut m, &["SADD", "myset", "w"]);
    int(&mut m, &["DEL", "with-ttl"]);
    bulk(&mut m, &["SPOP", "myset"]);

    // Multi-key commands (coordinator + deferred stores) and global commands.
    int(&mut m, &["SADD", "src", "p", "q", "r"]);
    int(&mut m, &["SADD", "dst", "a"]);
    int(&mut m, &["SMOVE", "src", "dst", "p"]);
    int(&mut m, &["SUNIONSTORE", "bigunion", "src", "dst"]);
    int(&mut m, &["SINTERSTORE", "inter", "src", "dst"]);
    int(&mut m, &["SDIFFSTORE", "diff", "src", "dst"]);
    ok(&mut m, &["RENAME", "foo", "renamed"]);
    ok(&mut m, &["SET", "k1", "v1"]);
    ok(&mut m, &["SET", "k2", "v2"]);
    ok(&mut m, &["MSET", "k3", "v3", "k4", "v4"]);

    // Give the journal a moment to drain, then check everything.
    wait_for(Duration::from_secs(10), || {
        bulk(&mut r, &["GET", "renamed"]) == "newbar"
    });
    assert_eq!(bulk(&mut r, &["GET", "renamed"]), "newbar");
    assert_eq!(bulk(&mut r, &["GET", "num"]), "43");
    assert_eq!(list(&mut r, &["LRANGE", "mylist", "0", "-1"]), "abcd");
    assert_eq!(bulk(&mut r, &["GET", "with-ttl"]), "<nil>");
    assert_eq!(set(&mut r, &["SMEMBERS", "src"]), "qr");
    assert_eq!(set(&mut r, &["SMEMBERS", "dst"]), "ap");
    assert_eq!(set(&mut r, &["SMEMBERS", "bigunion"]), "apqr");
    assert_eq!(set(&mut r, &["SMEMBERS", "inter"]), "");
    assert_eq!(set(&mut r, &["SMEMBERS", "diff"]), "qr");
    assert_eq!(bulk(&mut r, &["GET", "k3"]), "v3");
    assert_eq!(bulk(&mut r, &["GET", "k4"]), "v4");
    assert_eq!(list(&mut r, &["MGET", "k1", "k2"]), "v1v2");

    // Global command with the per-shard barrier.
    ok(&mut m, &["FLUSHALL"]);
    wait_for(Duration::from_secs(10), || {
        bulk(&mut r, &["GET", "k1"]) == "<nil>"
    });
    assert_eq!(bulk(&mut r, &["GET", "k1"]), "<nil>");
    assert_eq!(bulk(&mut r, &["GET", "renamed"]), "<nil>");

    // Detach: back to master mode, writes allowed.
    ok(&mut r, &["REPLICAOF", "NO", "ONE"]);
    wait_for(Duration::from_secs(5), || {
        let Ok(mut c) = Client::connect(replica_port) else {
            return false;
        };
        matches!(
            c.cmd(&["ROLE"]),
            Ok(Value::Array(Some(items))) if items[0].text().as_deref() == Some("master")
        )
    });
    ok(&mut r, &["SET", "fresh", "1"]);
    assert_eq!(bulk(&mut r, &["GET", "fresh"]), "1");

    drop(r);
    drop(m);
    master.kill().ok();
    replica.kill().ok();
    master.wait().ok();
    replica.wait().ok();
}

/// A replica must converge to the master even while the master keeps accepting
/// writes during the full-sync snapshot. The baseline is large enough to span
/// several 8KiB chunks, and a separate thread hammers the master from the
/// moment the replica attaches, so writes land before, during (journal blobs)
/// and after (stable sync) the snapshot — and the replica must agree with the
/// master on all of them.
#[test]
fn writes_during_full_sync_converge() {
    const BASE_KEYS: i64 = 2000;
    const DURING_KEYS: i64 = 600;
    let master_port = free_port();
    let replica_port = free_port();
    let mut master = spawn(master_port, 2);
    let mut replica = spawn(replica_port, 2);
    wait_ready(master_port);
    wait_ready(replica_port);

    let mut m = Client::connect(master_port).unwrap();
    let mut r = Client::connect(replica_port).unwrap();

    for i in 0..BASE_KEYS {
        ok(
            &mut m,
            &[
                "SET",
                &format!("base:{i}"),
                &format!("v-{i:04}-{}", "x".repeat(24)),
            ],
        );
    }

    // Attach, then hammer the master with writes while the snapshot streams.
    ok(&mut m, &["SET", "counter", "1000"]);
    ok(
        &mut r,
        &["REPLICAOF", "localhost", &master_port.to_string()],
    );
    let writer_port = master_port;
    let writer = std::thread::spawn(move || {
        let mut w = Client::connect(writer_port).unwrap();
        for i in 0..DURING_KEYS {
            if w.cmd(&["SET", &format!("during:{i}"), &format!("w-{i}")])
                .is_err()
            {
                break;
            }
            if i % 50 == 0 {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        // A non-idempotent command on a key that predates the snapshot: every
        // INCR must apply exactly once, whether it lands as a journal blob or
        // a stable-sync record. A double-apply (baseline carrying the new value
        // plus a trailing blob) would overshoot the master's value.
        for _ in 0..100 {
            if w.cmd(&["INCR", "counter"]).is_err() {
                break;
            }
        }
    });

    wait_replica_state(replica_port, "connected");
    writer.join().unwrap();

    // The whole baseline is reproduced (sample every 100th key).
    for i in (0..BASE_KEYS).step_by(100) {
        assert_eq!(
            bulk(&mut r, &["GET", &format!("base:{i}")]),
            format!("v-{i:04}-{}", "x".repeat(24)),
            "baseline key base:{i} diverged"
        );
    }
    // Every write that raced the snapshot is present with its final value.
    for i in 0..DURING_KEYS {
        assert_eq!(
            bulk(&mut r, &["GET", &format!("during:{i}")]),
            format!("w-{i}"),
            "concurrent key during:{i} diverged"
        );
    }
    // The counter must agree with the master exactly: no increments lost and,
    // critically, none applied twice.
    let master_counter: i64 = bulk(&mut m, &["GET", "counter"]).parse().unwrap();
    let replica_counter: i64 = bulk(&mut r, &["GET", "counter"]).parse().unwrap();
    assert_eq!(master_counter, 1100, "master counter sanity");
    assert_eq!(replica_counter, master_counter, "counter diverged");
    // A baseline key overwritten mid-snapshot must show the new value.
    ok(&mut m, &["SET", "base:7", "overwritten"]);
    wait_for(Duration::from_secs(10), || {
        bulk(&mut r, &["GET", "base:7"]) == "overwritten"
    });
    assert_eq!(bulk(&mut r, &["GET", "base:7"]), "overwritten");

    drop(r);
    drop(m);
    master.kill().ok();
    replica.kill().ok();
    master.wait().ok();
    replica.wait().ok();
}
