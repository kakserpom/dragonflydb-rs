//! Port of `dragonfly/src/server/multi_test.cc` — the MULTI/EXEC/WATCH/RESET
//! transaction tests plus the script-flag and EVAL tests from the same suite.
//!
//! Adaptations from the C++ original:
//! - The reference's internal hooks (GetDebugInfo, IsLocked, IsShardSetLocked,
//!   shard_set, txq, metrics) are dropped; the protocol-level assertions are
//!   kept 1:1.
//! - `AdvanceTime(1000)` → the port's pinned fake clock [`advance`].
//! - `Run("other", ...)` → a second `Client` opened via [`TestServer::client`].
//! - EVAL/EVALSHA errors are wrapped by the port as `ERR Error running script
//!   (call to <sha>): <message>`, so error checks use substring matches.
//! - The `MultiEvalTest` fixture (default `allow-undeclared-keys` flags) is
//!   replicated via [`Ctx::with_lua`]; the BRPOP fiber is [`Ctx::spawn`].
//!   A GLOBAL-mode EXEC is dispatched as one coordinator batch, since the
//!   port's shard lock is last-writer-wins (no contention): the single-threaded
//!   coordinator runs the whole queue without letting a woken blocked command
//!   slip between the queued commands.
//! - Tests gated on features the port lacks are skipped:
//!   - `UndeclaredKeyFlag`/`LegacyFloatShaFlag` need `CONFIG SET` to apply
//!     `lua_undeclared_keys_shas`/`lua_float_as_int_shas` dynamically.
//!   - `PerDbHitMissInfoOutput` needs `INFO keyspace` hit/miss counters.
//!   - `NoKeyTransactional`/`NoKeyTransactionalMany` exercise FT._LIST (no FT).

mod common;

use std::thread::sleep;
use std::time::Duration;

use common::*;

/// The reference `kExecFail`/`kExecSuccess` matchers: an EXEC abort is a nil
/// reply, a successful EXEC is an array.
fn exec_fails(v: &Value) -> bool {
    matches!(v, Value::Bulk(None))
}

fn exec_succeeds(v: &Value) -> bool {
    v.arr().is_some()
}

/// The reference's `RespElementsAre("OK")`: an element that textually equals
/// "OK" whether the reply was a status (`+OK`) or a bulk string (`"OK"`).
fn is_ok(v: &Value) -> bool {
    match v {
        Value::Simple(s) => s == "OK",
        Value::Bulk(Some(b)) => b == b"OK",
        other => {
            panic!("expected OK, got {other:?}");
        }
    }
}

/// `MultiAndFlush` (multi_test.cc:80): FLUSHALL is rejected while collecting.
#[test]
fn multi_and_flush() {
    let mut c = Ctx::new();
    c.ok(&["MULTI"]);
    assert_eq!(c.run(&["GET", "x"]).text().as_deref(), Some("QUEUED"));
    let err = c.err(&["FLUSHALL"]);
    assert!(err.contains("not allowed inside a transaction"), "{err}");
}

/// `MultiWithError` (multi_test.cc:90): EXEC without MULTI, queue-time errors
/// poison the transaction (EXECABORT), and a clean retry succeeds.
#[test]
fn multi_with_error() {
    let mut c = Ctx::new();
    assert_eq!(c.err(&["EXEC"]), "EXEC without MULTI");
    c.ok(&["MULTI"]);
    assert_eq!(c.run(&["SET", "x", "y"]).text().as_deref(), Some("QUEUED"));
    let err = c.err(&["SET", "x"]);
    assert!(
        err.contains("wrong number of arguments for 'set' command"),
        "{err}"
    );
    assert_eq!(
        c.err(&["EXEC"]),
        "EXECABORT Transaction discarded because of previous errors"
    );

    c.ok(&["MULTI"]);
    assert_eq!(c.run(&["SET", "z", "y"]).text().as_deref(), Some("QUEUED"));
    let exec = c.arr(&["EXEC"]);
    assert_eq!(exec, vec![Value::Simple("OK".into())]);

    assert!(matches!(c.run(&["GET", "x"]), Value::Bulk(None)));
    assert_eq!(c.run(&["GET", "z"]).text().as_deref(), Some("y"));
}

/// `MultiEmpty` (multi_test.cc:297): empty EXEC, queued PING, and empty-string
/// SET all run clean.
#[test]
fn multi_empty() {
    let mut c = Ctx::new();
    c.ok(&["MULTI"]);
    assert_eq!(c.arr(&["EXEC"]), vec![]);

    c.ok(&["MULTI"]);
    assert_eq!(c.run(&["PING", "foo"]).text().as_deref(), Some("QUEUED"));
    assert_eq!(c.arr(&["EXEC"]), vec![Value::Bulk(Some(b"foo".to_vec()))]);

    c.ok(&["MULTI"]);
    assert_eq!(c.run(&["SET", "a", ""]).text().as_deref(), Some("QUEUED"));
    assert_eq!(c.arr(&["EXEC"]), vec![Value::Simple("OK".into())]);

    assert_eq!(c.run(&["GET", "a"]).text().as_deref(), Some(""));
}

/// `MultiSeq` (multi_test.cc:318): sequential SET/GET/MGET with a nested MGET
/// array inside the EXEC array.
#[test]
fn multi_seq() {
    let mut c = Ctx::new();
    c.ok(&["MULTI"]);
    assert_eq!(c.run(&["SET", "x", "1"]).text().as_deref(), Some("QUEUED"));
    assert_eq!(c.run(&["GET", "x"]).text().as_deref(), Some("QUEUED"));
    assert_eq!(c.run(&["MGET", "x", "y"]).text().as_deref(), Some("QUEUED"));
    let exec = c.arr(&["EXEC"]);
    assert_eq!(
        exec,
        vec![
            Value::Simple("OK".into()),
            Value::Bulk(Some(b"1".to_vec())),
            Value::Array(Some(vec![
                Value::Bulk(Some(b"1".to_vec())),
                Value::Bulk(None)
            ])),
        ]
    );
}

/// `MultiWithoutTx` (multi_test.cc:501): PING and keyless EVAL commands run
/// without a transaction, but still reply inside the EXEC array.
#[test]
fn multi_without_tx() {
    let mut c = Ctx::new();
    c.ok(&["MULTI"]);
    c.run(&["PING"]);
    assert_eq!(c.arr(&["EXEC"]), vec![Value::Simple("PONG".into())]);

    c.ok(&["MULTI"]);
    c.run(&["EVAL", "return 'OK1'", "0"]);
    c.run(&["PING"]);
    c.run(&["EVAL", "return 'OK2'", "0", "not-a-key"]);
    c.run(&["PING"]);
    c.run(&["EVAL", "return 'OK3'", "0", "not-a-key", "as-well"]);
    c.run(&["PING"]);
    let exec = c.arr(&["EXEC"]);
    assert_eq!(exec.len(), 6);
    assert_eq!(exec[2].text().as_deref(), Some("OK2"));
    assert_eq!(exec[4].text().as_deref(), Some("OK3"));
}

/// `MultiGlobalCommands` (multi_test.cc:209): MOVE and SAVE run in a MULTI.
#[test]
fn multi_global_commands() {
    let mut c = Ctx::new();
    c.ok(&["SET", "key", "val"]);
    c.ok(&["MULTI"]);
    assert_eq!(
        c.run(&["MOVE", "key", "2"]).text().as_deref(),
        Some("QUEUED")
    );
    assert_eq!(c.run(&["SAVE"]).text().as_deref(), Some("QUEUED"));
    assert_eq!(c.arr(&["EXEC"]).len(), 2);

    assert!(matches!(c.run(&["GET", "key"]), Value::Bulk(None)));
    c.ok(&["SELECT", "2"]);
    assert_eq!(c.run(&["GET", "key"]).text().as_deref(), Some("val"));
}

/// `MultiRename` (multi_test.cc:469): single-shard and cross-shard RENAME in
/// MULTI/EXEC.
#[test]
fn multi_rename() {
    let mut c = Ctx::new();
    c.ok(&["MULTI"]);
    c.run(&["SET", "x", "1"]);
    assert_eq!(
        c.run(&["RENAME", "x", "y"]).text().as_deref(),
        Some("QUEUED")
    );
    assert_eq!(
        c.arr(&["EXEC"]),
        vec![Value::Simple("OK".into()), Value::Simple("OK".into())]
    );

    c.ok(&["MULTI"]);
    assert_eq!(
        c.run(&["RENAME", "y", "b"]).text().as_deref(),
        Some("QUEUED")
    );
    assert_eq!(c.arr(&["EXEC"]), vec![Value::Simple("OK".into())]);
}

/// `MultiTypes` (multi_test.cc:1593): TYPE on missing keys replies "none" for
/// each queued command.
#[test]
fn multi_types() {
    let mut c = Ctx::new();
    c.ok(&["MULTI"]);
    for k in ["sdfx3", "asdasd2", "wer124", "asafdasd", "dsfgser", "erg2"] {
        assert_eq!(c.run(&["TYPE", k]).text().as_deref(), Some("QUEUED"));
    }
    let exec = c.arr(&["EXEC"]);
    assert_eq!(
        exec,
        vec![
            Value::Simple("none".into()),
            Value::Simple("none".into()),
            Value::Simple("none".into()),
            Value::Simple("none".into()),
            Value::Simple("none".into()),
            Value::Simple("none".into()),
        ]
    );
}

/// `MultiAndEval` (multi_test.cc:1556, under the default Lua flags): EVAL and
/// SCRIPT LOAD inside MULTI.
#[test]
fn multi_and_eval() {
    let mut c = Ctx::new();
    c.ok(&["MULTI"]);
    c.run(&["EVAL", "return redis.call('set', 'x', 'y1')", "1", "x"]);
    assert_eq!(c.arr(&["EXEC"]).len(), 1);
    assert_eq!(c.run(&["GET", "x"]).text().as_deref(), Some("y1"));

    c.run(&["EVAL", "return redis.call('set', 'x', 'y1')", "1", "x"]);

    c.ok(&["MULTI"]);
    c.run(&["EVAL", "return 'OK';", "0"]);
    let exec = c.arr(&["EXEC"]);
    assert_eq!(exec.len(), 1);
    assert!(is_ok(&exec[0]));

    c.ok(&["MULTI"]);
    c.run(&["SCRIPT", "LOAD", "return '5'"]);
    c.arr(&["EXEC"]);

    c.ok(&["MULTI"]);
    c.run(&["SCRIPT", "LOAD", "return '5'"]);
    c.run(&["GET", "x"]);
    c.arr(&["EXEC"]);

    c.ok(&["MULTI"]);
    c.run(&["SCRIPT", "LOAD", "return '5'"]);
    c.run(&["MSET", "x1", "y1", "x2", "y2"]);
    c.arr(&["EXEC"]);

    c.ok(&["MULTI"]);
    c.run(&["SCRIPT", "LOAD", "return '5'"]);
    c.run(&["EVAL", "return redis.call('set', 'x', 'y')", "1", "x"]);
    c.run(&["GET", "x"]);
    let exec = c.arr(&["EXEC"]);
    assert_eq!(exec.len(), 3);

    assert_eq!(c.run(&["GET", "x"]).text().as_deref(), Some("y"));
}

/// `MultiEvalModeConflict` (multi_test.cc:1233): an allow-undeclared-keys EVAL
/// (GLOBAL scheduling) cannot run inside a LOCK_AHEAD MULTI transaction.
#[test]
fn multi_eval_mode_conflict() {
    let mut c = Ctx::new();
    let s1 = "--!df flags=allow-undeclared-keys\nreturn redis.call('GET', 'random-key');\n";
    c.ok(&["MULTI"]);
    assert_eq!(
        c.run(&["SET", "random-key", "works"]).text().as_deref(),
        Some("QUEUED")
    );
    assert_eq!(c.run(&["EVAL", s1, "0"]).text().as_deref(), Some("QUEUED"));
    let exec = c.arr(&["EXEC"]);
    assert_eq!(exec.len(), 2);
    assert_eq!(exec[0], Value::Simple("OK".into()));
    let Value::Error(e) = &exec[1] else {
        panic!("expected error, got {exec:?}");
    };
    assert!(
        e.contains("Multi mode conflict when running eval in multi transaction"),
        "{e}"
    );
}

/// `MultiAllEval` (multi_test.cc:1501): with `allow-undeclared-keys` as the
/// default flag the transaction runs in GLOBAL mode, so two undeclared-key
/// EVALs execute as one atomic transaction — a concurrently blocked BRPOP is
/// not woken mid-transaction and times out.
#[test]
fn multi_all_eval() {
    let mut c = Ctx::with_lua(
        2,
        LuaConfig {
            default_lua_flags: "allow-undeclared-keys".into(),
            ..Default::default()
        },
    );
    let fb = c.spawn(&["BRPOP", "x", "1"]);
    sleep(Duration::from_millis(50));

    c.ok(&["MULTI"]);
    c.run(&["EVAL", "return redis.call('lpush', 'x', 'y')", "0"]);
    c.run(&["EVAL", "return redis.call('lpop', 'x')", "0"]);
    let exec = c.arr(&["EXEC"]);
    assert_eq!(exec.len(), 2);
    assert_eq!(exec[0], Value::Integer(1));
    assert_eq!(exec[1], Value::Bulk(Some(b"y".to_vec())));

    let brpop = fb.join().unwrap();
    assert!(
        matches!(brpop, Value::Bulk(None) | Value::Array(None)),
        "expected nil array, got {brpop:?}"
    );
}

/// `MultiAndEval` (multi_test.cc:1556): a declared-key EVAL queued under the
/// `allow-undeclared-keys` default (a "borrowing interpreters" crash
/// regression upstream).
#[test]
fn multi_and_eval_default_global() {
    let mut c = Ctx::with_lua(
        2,
        LuaConfig {
            default_lua_flags: "allow-undeclared-keys".into(),
            ..Default::default()
        },
    );
    c.ok(&["MULTI"]);
    assert_eq!(
        c.run(&["EVAL", "return redis.call('set', 'x', 'y1')", "1", "x"])
            .text()
            .as_deref(),
        Some("QUEUED")
    );
    let exec = c.arr(&["EXEC"]);
    assert_eq!(exec, [Value::Simple("OK".into())]);
    assert_eq!(c.run(&["GET", "x"]).text().as_deref(), Some("y1"));
}

/// `MultiSomeEval` (multi_test.cc:1519): like `MultiAllEval` but only the first
/// queued command is a script; the transaction is still GLOBAL.
#[test]
fn multi_some_eval() {
    let mut c = Ctx::with_lua(
        2,
        LuaConfig {
            default_lua_flags: "allow-undeclared-keys".into(),
            ..Default::default()
        },
    );
    let fb = c.spawn(&["BRPOP", "x", "1"]);
    sleep(Duration::from_millis(50));

    c.ok(&["MULTI"]);
    c.run(&["EVAL", "return redis.call('lpush', 'x', 'y')", "0"]);
    c.run(&["LPOP", "x"]);
    let exec = c.arr(&["EXEC"]);
    assert_eq!(exec.len(), 2);
    assert_eq!(exec[0], Value::Integer(1));
    assert_eq!(exec[1], Value::Bulk(Some(b"y".to_vec())));

    let brpop = fb.join().unwrap();
    assert!(
        matches!(brpop, Value::Bulk(None) | Value::Array(None)),
        "expected nil array, got {brpop:?}"
    );
}

/// `EvalRo` (multi_test.cc:1606): EVAL_RO reads, rejects writes.
#[test]
fn eval_ro() {
    let mut c = Ctx::new();
    c.ok(&["SET", "foo", "bar"]);
    assert_eq!(
        c.run(&["EVAL_RO", "return redis.call('get', KEYS[1])", "1", "foo"])
            .text()
            .as_deref(),
        Some("bar")
    );
    let err = c.err(&[
        "EVAL_RO",
        "return redis.call('set', KEYS[1], 'car')",
        "1",
        "foo",
    ]);
    assert!(
        err.contains("Write commands are not allowed from read-only scripts"),
        "{err}"
    );
}

/// `EvalShaRo` (multi_test.cc:1619): EVALSHA_RO read/write behavior.
#[test]
fn eval_sha_ro() {
    let mut c = Ctx::new();
    let read_sha =
        String::from_utf8(c.bulk(&["SCRIPT", "LOAD", "return redis.call('get', KEYS[1]);"]))
            .unwrap();
    let write_sha = String::from_utf8(c.bulk(&[
        "SCRIPT",
        "LOAD",
        "return redis.call('set', KEYS[1], 'car');",
    ]))
    .unwrap();
    c.ok(&["SET", "foo", "bar"]);

    let read = c.run(&["EVALSHA_RO", &read_sha, "1", "foo"]);
    assert_eq!(read.text().as_deref(), Some("bar"));

    let err = c.err(&["EVALSHA_RO", &write_sha, "1", "foo"]);
    assert!(
        err.contains("Write commands are not allowed from read-only scripts"),
        "{err}"
    );
}

/// `EvalSelect` (multi_test.cc:1639): SELECT inside EVAL switches DB in
/// global/non-atomic scripts, and is rejected in regular transactions.
#[test]
fn eval_select() {
    let mut c = Ctx::new();
    let script_global = "--!df flags=allow-undeclared-keys\nredis.call('SET', 'A', ARGV[1])\nredis.call('SELECT', '1')\nredis.call('SET', 'A', ARGV[2])\nreturn 'OK';\n";
    assert_eq!(
        c.run(&["EVAL", script_global, "0", "G1", "G2"])
            .text()
            .as_deref(),
        Some("OK")
    );

    c.ok(&["SELECT", "0"]);
    assert_eq!(c.run(&["GET", "A"]).text().as_deref(), Some("G1"));
    c.ok(&["SELECT", "1"]);
    assert_eq!(c.run(&["GET", "A"]).text().as_deref(), Some("G2"));
    c.ok(&["SELECT", "0"]);

    let script_nonatomic = "--!df flags=disable-atomicity\nredis.call('SET', 'A', ARGV[1])\nredis.call('SELECT', '1')\nredis.call('SET', 'A', ARGV[2])\nreturn 'OK';\n";
    assert_eq!(
        c.run(&["EVAL", script_nonatomic, "0", "G3", "G4"])
            .text()
            .as_deref(),
        Some("OK")
    );

    c.ok(&["SELECT", "0"]);
    assert_eq!(c.run(&["GET", "A"]).text().as_deref(), Some("G3"));
    c.ok(&["SELECT", "1"]);
    assert_eq!(c.run(&["GET", "A"]).text().as_deref(), Some("G4"));
    c.ok(&["SELECT", "0"]);

    let script_fail = "redis.call('SET', KEYS[1], ARGV[1])\nredis.call('SELECT', '1')\nredis.call('SET', KEYS[1], ARGV[1])\n";
    let err = c.err(&["EVAL", script_fail, "1", "A", "wont-work"]);
    assert!(err.contains("SELECT is not allowed in regular"), "{err}");
}

/// `EvalExpiration` (multi_test.cc:1460): TTL set from Lua is honored.
#[test]
fn eval_expiration() {
    let _clock = clock_guard();
    let mut c = Ctx::new();
    c.run(&["EVAL", "redis.call('set', 'x', 0, 'ex', 5, 'nx')", "1", "x"]);
    let pttl = c.int(&["PTTL", "x"]);
    assert!(pttl <= 5000, "pttl={pttl}");
}

/// `MemoryInScript` (multi_test.cc:1471): `MEMORY USAGE` runs from a script
/// (the reference dropped NOSCRIPT in #2382) and reports 0 for the 1-byte
/// inline key/value pair — both `CompactObj`s are stored inline, so
/// `MallocUsed()` is 0 for each.
#[test]
fn memory_in_script() {
    let mut c = Ctx::new();
    c.ok(&["SET", "x", "y"]);
    assert_eq!(
        c.run(&[
            "EVAL",
            "return redis.call('MEMORY', 'USAGE', KEYS[1])",
            "1",
            "x"
        ]),
        Value::Integer(0)
    );
}

/// `Watch` (multi_test.cc:691): WATCH semantics including expiry and FLUSHDB
/// touch, multi-DB rejection, and per-DB isolation.
#[test]
fn watch() {
    let _clock = clock_guard();
    let mut c = Ctx::new();

    // WATCH is not allowed inside MULTI.
    c.ok(&["MULTI"]);
    let err = c.err(&["WATCH", "a"]);
    assert!(err.contains("not allowed inside a transaction"), "{err}");
    c.ok(&["DISCARD"]);

    // Existing key modified before EXEC aborts.
    c.ok(&["SET", "a", "1"]);
    c.ok(&["WATCH", "a"]);
    c.ok(&["SET", "a", "2"]);
    c.ok(&["MULTI"]);
    assert!(exec_fails(&c.run(&["EXEC"])));

    // Nonempty EXEC body on an unmodified key succeeds.
    c.ok(&["WATCH", "a"]);
    c.ok(&["MULTI"]);
    c.run(&["GET", "a"]);
    c.run(&["GET", "b"]);
    c.run(&["GET", "c"]);
    assert!(exec_succeeds(&c.run(&["EXEC"])));

    // Watch state is cleared after EXEC.
    c.ok(&["SET", "a", "1"]);
    c.ok(&["MULTI"]);
    assert!(exec_succeeds(&c.run(&["EXEC"])));

    // Non-existent key that appears before EXEC aborts.
    c.int(&["DEL", "b"]);
    c.ok(&["WATCH", "b"]);
    c.ok(&["SET", "b", "1"]);
    c.ok(&["MULTI"]);
    assert!(exec_fails(&c.run(&["EXEC"])));

    // EXEC does not miss watched-key expiration.
    c.ok(&["WATCH", "a"]);
    assert_eq!(c.int(&["EXPIRE", "a", "1"]), 1);
    advance(1000);
    c.ok(&["MULTI"]);
    c.run(&["GET", "a"]);
    assert!(exec_fails(&c.run(&["EXEC"])));

    // UNWATCH clears the watch.
    c.ok(&["WATCH", "a"]);
    c.ok(&["UNWATCH"]);
    c.ok(&["SET", "a", "3"]);
    c.ok(&["MULTI"]);
    assert!(exec_succeeds(&c.run(&["EXEC"])));

    // Any touched watched key aborts.
    c.ok(&["WATCH", "a", "b"]);
    c.ok(&["SET", "a", "2"]);
    c.ok(&["SET", "b", "2"]);
    c.ok(&["MULTI"]);
    assert!(exec_fails(&c.run(&["EXEC"])));

    // EXPIRE of a watched key is detected even with a second watch.
    c.ok(&["SET", "a", "1"]);
    c.int(&["DEL", "c"]);
    c.ok(&["WATCH", "c"]);
    c.ok(&["WATCH", "a"]);
    c.ok(&["SET", "c", "1"]);
    assert_eq!(c.int(&["EXPIRE", "a", "1"]), 1);
    advance(1000);
    c.ok(&["MULTI"]);
    assert!(exec_fails(&c.run(&["EXEC"])));

    // FLUSHDB touches watched keys.
    c.ok(&["SELECT", "1"]);
    c.ok(&["SET", "a", "1"]);
    c.ok(&["WATCH", "a"]);
    c.ok(&["FLUSHDB"]);
    c.ok(&["MULTI"]);
    assert!(exec_fails(&c.run(&["EXEC"])));

    // WATCH and EXEC on different DBs are rejected.
    c.ok(&["SELECT", "1"]);
    c.ok(&["SET", "a", "1"]);
    c.ok(&["WATCH", "a"]);
    c.ok(&["SELECT", "0"]);
    c.ok(&["MULTI"]);
    assert!(matches!(c.run(&["EXEC"]), Value::Error(_)));

    // Watches are isolated per database.
    c.ok(&["SET", "a", "1"]);
    c.ok(&["WATCH", "a"]);
    c.ok(&["SELECT", "1"]);
    c.ok(&["SET", "a", "2"]);
    c.ok(&["SELECT", "0"]);
    c.ok(&["MULTI"]);
    assert!(exec_succeeds(&c.run(&["EXEC"])));
}

/// `ResetReturnsResetString` (multi_test.cc:1717).
#[test]
fn reset_returns_reset_string() {
    let mut c = Ctx::new();
    assert_eq!(c.run(&["RESET"]).text().as_deref(), Some("RESET"));
}

/// `ResetClearsMULTIBlock` (multi_test.cc:1721).
#[test]
fn reset_clears_multi_block() {
    let mut c = Ctx::new();
    c.ok(&["MULTI"]);
    assert_eq!(c.run(&["GET", "x"]).text().as_deref(), Some("QUEUED"));
    assert_eq!(c.run(&["RESET"]).text().as_deref(), Some("RESET"));
    assert!(matches!(c.run(&["GET", "x"]), Value::Bulk(None)));
    assert_eq!(c.err(&["EXEC"]), "EXEC without MULTI");
}

/// `ResetClearsWatchState` (multi_test.cc:1734): RESET clears WATCH state even
/// after another connection modified the key.
#[test]
fn reset_clears_watch_state() {
    let mut c = Ctx::new();
    c.ok(&["SET", "a", "1"]);
    c.ok(&["WATCH", "a"]);
    let mut other = c.server.client();
    other.cmd(&["SET", "a", "2"]).expect("other connection");

    assert_eq!(c.run(&["RESET"]).text().as_deref(), Some("RESET"));
    c.ok(&["MULTI"]);
    c.run(&["GET", "a"]);
    assert_eq!(c.arr(&["EXEC"]), vec![Value::Bulk(Some(b"2".to_vec()))]);
}

/// `ResetSelectsDB0` (multi_test.cc:1748).
#[test]
fn reset_selects_db0() {
    let mut c = Ctx::new();
    c.ok(&["SELECT", "1"]);
    c.ok(&["SET", "resetkey", "val"]);
    assert_eq!(c.run(&["RESET"]).text().as_deref(), Some("RESET"));
    assert!(matches!(c.run(&["GET", "resetkey"]), Value::Bulk(None)));
    c.ok(&["SELECT", "1"]);
    assert_eq!(c.run(&["GET", "resetkey"]).text().as_deref(), Some("val"));
}

/// `ScriptFlagsInvalidSha` (multi_test.cc:1021): a short SHA errors.
#[test]
fn script_flags_invalid_sha() {
    let mut c = Ctx::new();
    assert!(
        c.err(&["SCRIPT", "FLAGS", "short", "allow-undeclared-keys"])
            .contains("error")
    );
}

/// `ScriptFlagsEmbedded` (multi_test.cc:1025): `--!df flags=` in the body.
#[test]
fn script_flags_embedded() {
    let mut c = Ctx::new();
    c.ok(&["SET", "random-key", "works"]);
    let s1 = "--!df flags=allow-undeclared-keys\nreturn redis.call('GET', 'random-key');\n";
    assert_eq!(c.run(&["EVAL", s1, "0"]).text().as_deref(), Some("works"));

    let s2 = "--!df flags=this-is-an-error\nredis.call('SET', 'random-key', 'failed')\n";
    let err = c.err(&["EVAL", s2, "0"]);
    assert!(err.contains("Invalid flag: this-is-an-error"), "{err}");
}

/// `CjsonDecodeIntegerBehavior` (multi_test.cc:1108): cjson.decode returns
/// integers for whole numbers, floats for fractions.
#[test]
fn cjson_decode_integer_behavior() {
    let mut c = Ctx::new();
    let script = "local obj = cjson.decode('{\"value\": 42}')\nreturn tostring(obj.value)\n";
    assert_eq!(c.run(&["EVAL", script, "0"]).text().as_deref(), Some("42"));
    let script_float =
        "local obj = cjson.decode('{\"value\": 42.5}')\nreturn tostring(obj.value)\n";
    assert_eq!(
        c.run(&["EVAL", script_float, "0"]).text().as_deref(),
        Some("42.5")
    );
}

/// `ScriptBadCommand` (multi_test.cc:1124): FLUSHALL from a script (call and
/// acall) is rejected; `disable-atomicity` allows it.
#[test]
fn script_bad_command() {
    let mut c = Ctx::new();
    let s1 = "redis.call('FLUSHALL')";
    assert!(
        c.err(&["EVAL", s1, "0"])
            .contains("This Redis command is not allowed from script")
    );

    let s2 = "redis.call('FLUSHALL'); redis.set(KEYS[1], ARGS[1]);";
    assert!(
        c.err(&["EVAL", s2, "1", "works", "false"])
            .contains("This Redis command is not allowed from script")
    );

    let s3 = "redis.acall('FLUSHALL'); redis.set(KEYS[1], ARGS[1]);";
    assert!(
        c.err(&["EVAL", s3, "1", "works", "false"])
            .contains("This Redis command is not allowed from script")
    );

    let s4 = "--!df flags=disable-atomicity\nredis.call('FLUSHALL');\nreturn \"OK\";\n";
    assert_eq!(c.run(&["EVAL", s4, "0"]).text().as_deref(), Some("OK"));
}

/// `GeneralACall` (multi_test.cc:1147): `redis.acall` batching.
#[test]
fn general_acall() {
    let mut c = Ctx::new();
    let script = "redis.acall('PING')\nfor i = 1, 10 do\n  redis.acall('RPUSH', KEYS[1], 'v' .. i)\nend\nreturn \"OK\";\n";
    assert_eq!(
        c.run(&["EVAL", script, "1", "l"]).text().as_deref(),
        Some("OK")
    );
    let l = c.arr(&["LRANGE", "l", "0", "-1"]);
    let expected: Vec<Value> = (1..=10)
        .map(|i| Value::Bulk(Some(format!("v{i}").into_bytes())))
        .collect();
    assert_eq!(l, expected);
}

/// `ACallUndeclaredKeys` (multi_test.cc:1163): async DEL of undeclared keys
/// errors; declaring all keys succeeds.
#[test]
fn acall_undeclared_keys() {
    let mut c = Ctx::new();
    let script = "for i = 1, #KEYS do\n  redis.acall('DEL', KEYS[i])\nend\nfor i = 1, #ARGV do\n  redis.acall('DEL', ARGV[i])\nend\n";
    let keys: Vec<String> = (0..10).map(|i| format!("k{i}")).collect();

    for i in 0..10 {
        let num_keys = i.to_string();
        let mut args = vec!["EVAL", script, &num_keys];
        args.extend(keys[..=i].iter().map(String::as_str));
        let err = c.err(&args);
        assert!(err.contains("undeclared key"), "{err}");
        assert!(err.contains(&format!("k{i}")), "{err}");
    }

    let mut args = vec!["EVAL", script, "10"];
    args.extend(keys.iter().map(String::as_str));
    assert!(matches!(c.run(&args), Value::Bulk(None)));
}
