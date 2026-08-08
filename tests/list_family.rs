//! Port of `dragonfly/src/server/list_family_test.cc` to the in-process
//! harness (`tests/common/mod.rs`).
//!
//! Adaptations from the reference:
//! - `AdvanceTime` is served by the process-global test clock from `common`
//!   (see `clock_guard`); blocking-fiber coordination still uses real sleeps.
//! - Internal blocking-controller state (`IsLocked`, `NumWatched`,
//!   `HasAwakened`, `WaitUntilLocked`) is not observable over the socket and
//!   the assertions are dropped; behavior is asserted instead (blocking
//!   replies only after the wakeup push, correct popped values, etc.).
//! - `RunAsync` / fibers become a background thread with its own connection
//!   (`Ctx::spawn`); the pop fiber is given time to register before the push.
//! - The pure scheduler-stress tests (TwoQueueBug451, BRPopContended,
//!   BLMoveWaves, ContendExpire, AwakeMulti, PressureBLMove) exercise fiber
//!   scheduling internals and are not portable to threads; they are skipped.

mod common;

use common::*;
use std::thread::sleep;
use std::time::Duration;

/// Text entries of an array reply in their reply order (`GetVec`).
fn strs(v: &Value) -> Vec<String> {
    v.arr()
        .unwrap_or_else(|| panic!("expected array, got {v:?}"))
        .iter()
        .map(|x| {
            x.text()
                .unwrap_or_else(|| panic!("expected text, got {x:?}"))
        })
        .collect()
}

/// The list key popped by an LMPop-family reply, which is `[key, [items]]`.
fn key(v: &Value) -> String {
    v.arr()
        .unwrap_or_else(|| panic!("expected array, got {v:?}"))
        .first()
        .and_then(common::Value::text)
        .unwrap_or_else(|| panic!("expected key text in {v:?}"))
}

/// Integer entries of an array reply in their reply order.
fn ints(v: &Value) -> Vec<i64> {
    v.arr()
        .unwrap_or_else(|| panic!("expected array, got {v:?}"))
        .iter()
        .map(|x| {
            x.int()
                .unwrap_or_else(|| panic!("expected integer, got {x:?}"))
        })
        .collect()
}

/// Text entries of an array reply, order-independent.
fn sorted(v: &Value) -> Vec<String> {
    let mut s = strs(v);
    s.sort();
    s
}

#[test]
fn basic() {
    let mut t = Ctx::new();
    t.assert_int(&["lpush", "x", "1"], 1);
    t.assert_int(&["lpush", "b", "2"], 1);
    t.assert_int(&["llen", "x"], 1);
}

#[test]
fn expire() {
    let mut t = Ctx::new();
    let _clock = clock_guard();
    t.assert_int(&["lpush", "x", "1"], 1);
    t.assert_int(&["expire", "x", "1"], 1);
    advance(1100);
    t.assert_int(&["lpush", "x", "1"], 1);
}

#[test]
fn blmpop_nonblocking() {
    let mut t = Ctx::new();
    t.assert_int(&["lpush", "x", "1", "2", "3", "4"], 4);

    let v = t.run(&["blmpop", "0.01", "2", "b", "x", "LEFT"]);
    assert_eq!(
        v.arr().map(<[Value]>::to_vec),
        Some(vec![
            Value::Bulk(Some(b"x".to_vec())),
            Value::Array(Some(vec![Value::Bulk(Some(b"4".to_vec()))])),
        ])
    );

    let v = t.run(&["blmpop", "0.01", "2", "b", "x", "RIGHT", "COUNT", "2"]);
    assert_eq!(
        v.arr().map(<[Value]>::to_vec),
        Some(vec![
            Value::Bulk(Some(b"x".to_vec())),
            Value::Array(Some(vec![
                Value::Bulk(Some(b"1".to_vec())),
                Value::Bulk(Some(b"2".to_vec())),
            ])),
        ])
    );

    // Count exceeds the size: return all of the key's values.
    let v = t.run(&["blmpop", "0.01", "1", "x", "RIGHT", "COUNT", "10"]);
    assert_eq!(
        v.arr().map(<[Value]>::to_vec),
        Some(vec![
            Value::Bulk(Some(b"x".to_vec())),
            Value::Array(Some(vec![Value::Bulk(Some(b"3".to_vec()))])),
        ])
    );
}

#[test]
fn blmpop_invalid_syntax() {
    let mut t = Ctx::new();
    t.assert_err(&["blmpop", "0.1", "1", "x"], "wrong number of arguments");
    t.assert_err(
        &["blmpop", "foo", "1", "x", "LEFT", "COUNT", "1"],
        "timeout is not a float or out of range",
    );
    t.assert_err(
        &["blmpop", "-0.01", "1", "x", "LEFT", "COUNT", "1"],
        "timeout is negative",
    );
    t.assert_err(
        &["blmpop", "0.01", "0", "LEFT", "COUNT", "1"],
        "at least 1 input key is needed",
    );
    t.assert_err(
        &["blmpop", "0.01", "aa", "x", "LEFT"],
        "value is not an integer or out of range",
    );
    t.assert_err(&["blmpop", "0.01", "1", "x", "COUNT", "1"], "syntax error");
    t.assert_err(&["blmpop", "0.01", "1", "x", "b", "LEFT"], "syntax error");
    t.assert_err(
        &["blmpop", "0.01", "1", "x", "LEFT", "COUNT"],
        "syntax error",
    );
    t.assert_err(
        &["blmpop", "0.01", "1", "x", "LEFT", "COUNT", "boo"],
        "value is not an integer or out of range",
    );
    t.assert_err(
        &["blmpop", "0.01", "1", "c", "LEFT", "COUNT", "2", "foo"],
        "syntax error",
    );
}

#[test]
fn blmpop_blocking() {
    let mut t = Ctx::new();

    // Pop from an empty key blocks and returns nil on timeout.
    let fb = t.spawn(&["blmpop", "0.1", "1", "x", "LEFT"]);
    let resp = fb.join().unwrap();
    assert!(
        matches!(resp, Value::Bulk(None) | Value::Array(None)),
        "got {resp:?}"
    );

    // BLMPOP should not block if there is a non-empty key available.
    t.assert_int(&["lpush", "x", "0"], 1);
    let resp = t.run(&["blmpop", "0.1", "1", "x", "LEFT"]);
    let a = resp.arr().expect("array");
    assert_eq!(a[0].text().as_deref(), Some("x"));
    assert_eq!(strs(&a[1]), ["0"]);

    // Block until a key is available, then unblock immediately.
    let fb = t.spawn(&["blmpop", "5", "1", "x", "LEFT"]);
    sleep(Duration::from_millis(100));
    t.assert_int(&["lpush", "x", "1"], 1);
    let resp = fb.join().unwrap();
    let a = resp.arr().expect("array");
    assert_eq!(a[0].text().as_deref(), Some("x"));
    assert_eq!(strs(&a[1]), ["1"]);
}

#[test]
fn blpop_unblocking() {
    let mut t = Ctx::new();
    t.assert_int(&["lpush", "x", "1"], 1);
    t.assert_int(&["lpush", "b", "2"], 1);

    // Missing "0" delimiter.
    t.assert_err(
        &["blpop", "x", "b"],
        "timeout is not a float or out of range",
    );

    let v = t.run(&["blpop", "x", "b", "0"]);
    assert_eq!(strs(&v), ["x", "1"]);

    let v = t.run(&["blpop", "x", "b", "0"]);
    assert_eq!(strs(&v), ["b", "2"]);

    t.ok(&["set", "z", "1"]);
    t.assert_err(&["blpop", "z", "0"], "WRONGTYPE");
}

#[test]
fn blpop_blocking() {
    let mut t = Ctx::new();

    // Two fibers block on the same key; one lpush wakes exactly one of them.
    let fb0 = t.spawn(&["blpop", "x", "0"]);
    sleep(Duration::from_millis(50));
    let fb1 = t.spawn(&["blpop", "x", "0"]);
    sleep(Duration::from_millis(50));
    t.assert_int(&["lpush", "x", "2", "1"], 2);

    let resp0 = fb0.join().unwrap();
    let resp1 = fb1.join().unwrap();
    assert_eq!(strs(&resp0).len(), 2);
    assert_eq!(strs(&resp1).len(), 2);
    let mut popped = vec![strs(&resp0)[1].clone(), strs(&resp1)[1].clone()];
    popped.sort();
    assert_eq!(popped, ["1", "2"]);
    for r in [&resp0, &resp1] {
        assert_eq!(strs(r)[0], "x");
    }
}

#[test]
fn blpop_multiple() {
    let mut t = Ctx::new();

    // Timeout with no data.
    let resp = t.run(&["blpop", "x", "b", "0.01"]);
    assert!(
        matches!(resp, Value::Bulk(None) | Value::Array(None)),
        "got {resp:?}"
    );

    // Block forever, then wake on a push to the first key.
    let fb = t.spawn(&["blpop", "x", "b", "0"]);
    sleep(Duration::from_millis(100));
    t.assert_int(&["lpush", "x", "1", "2", "3"], 3);
    let resp = fb.join().unwrap();
    assert_eq!(strs(&resp), ["x", "3"]);
}

#[test]
fn blpop_timeout() {
    let mut t = Ctx::new();
    let resp = t.run(&["blpop", "x", "b", "c", "0.01"]);
    assert!(
        matches!(resp, Value::Bulk(None) | Value::Array(None)),
        "got {resp:?}"
    );

    // Under MULTI a blocking command replies nil immediately.
    t.ok(&["multi"]);
    t.run(&["blpop", "x", "0"]);
    let v = t.run(&["exec"]);
    let a = v.arr().expect("array");
    assert_eq!(a.len(), 1);
    assert!(
        matches!(a[0], Value::Bulk(None) | Value::Array(None)),
        "got {:?}",
        a[0]
    );
}

#[test]
fn blpop_timeout2() {
    let mut t = Ctx::new();
    let resp = t.run(&["blpop", "blist1", "blist2", "0.1"]);
    assert!(
        matches!(resp, Value::Bulk(None) | Value::Array(None)),
        "got {resp:?}"
    );

    t.assert_int(&["rpush", "blist2", "d"], 1);
    t.assert_int(&["rpush", "blist2", "hello"], 2);

    let resp = t.run(&["blpop", "blist1", "blist2", "1"]);
    assert_eq!(strs(&resp), ["blist2", "d"]);

    t.assert_int(&["rpush", "blist1", "a"], 1);
    t.assert_int(&["del", "blist2"], 1);
    t.assert_int(&["rpush", "blist2", "d"], 1);
    let resp = t.run(&["blpop", "blist1", "blist2", "1"]);
    assert_eq!(strs(&resp), ["blist1", "a"]);
}

#[test]
fn blpop_multi_push() {
    let mut t = Ctx::new();

    let fb = t.spawn(&["blpop", "x", "b", "c", "0"]);
    sleep(Duration::from_millis(100));

    // A MULTI pushes to all three keys.
    t.ok(&["multi"]);
    t.run(&["lpush", "c", "C"]);
    t.run(&["lpush", "b", "B"]);
    t.run(&["lpush", "x", "A"]);
    let v = t.run(&["exec"]);
    assert_eq!(ints(&v), [1, 1, 1]);

    let resp = fb.join().unwrap();
    assert_eq!(strs(&resp).len(), 2);
    assert_eq!(t.int(&["exists", "x", "b", "c"]), 2);
}

#[test]
fn wrong_type_does_not_wake() {
    let mut t = Ctx::new();

    let fb = t.spawn(&["blpop", "x", "0"]);
    sleep(Duration::from_millis(100));

    // A MULTI that pushes then overwrites with a string: the wake is deferred
    // until the key holds a list again.
    t.ok(&["multi"]);
    t.run(&["lpush", "x", "A"]);
    t.run(&["set", "x", "foo"]);
    let v = t.run(&["exec"]);
    let a = v.arr().expect("array");
    assert_eq!(a[0].int(), Some(1));
    assert_eq!(a[1].text().as_deref(), Some("OK"));

    sleep(Duration::from_millis(100));
    t.assert_int(&["del", "x"], 1);
    t.assert_int(&["lpush", "x", "B"], 1);

    let resp = fb.join().unwrap();
    assert_eq!(strs(&resp), ["x", "B"]);
}

#[test]
fn bpop_same_key_twice() {
    let mut t = Ctx::new();

    let fb = t.spawn(&["blpop", "x", "b", "b", "x", "0"]);
    sleep(Duration::from_millis(100));
    t.assert_int(&["lpush", "x", "bar"], 1);
    let resp = fb.join().unwrap();
    assert_eq!(strs(&resp), ["x", "bar"]);

    let fb = t.spawn(&["blpop", "x", "b", "b", "x", "0"]);
    sleep(Duration::from_millis(100));
    t.assert_int(&["lpush", "b", "bar"], 1);
    let resp = fb.join().unwrap();
    assert_eq!(strs(&resp), ["b", "bar"]);
}

#[test]
fn bpop_rename() {
    let mut t = Ctx::new();

    let fb = t.spawn(&["blpop", "x", "0"]);
    sleep(Duration::from_millis(100));
    t.assert_int(&["lpush", "a", "bar"], 1);
    t.ok(&["rename", "a", "x"]);
    let resp = fb.join().unwrap();
    assert_eq!(strs(&resp), ["x", "bar"]);
}

#[test]
fn bpop_flush() {
    let mut t = Ctx::new();

    let fb = t.spawn(&["blpop", "x", "0"]);
    sleep(Duration::from_millis(100));
    t.ok(&["flushdb"]);
    t.assert_int(&["lpush", "x", "bar"], 1);
    let resp = fb.join().unwrap();
    assert_eq!(strs(&resp), ["x", "bar"]);
}

#[test]
fn lrem() {
    let mut t = Ctx::new();
    t.assert_int(&["rpush", "x", "a", "b", "a", "c"], 4);
    t.assert_int(&["lrem", "x", "2", "a"], 2);
    assert_eq!(strs(&t.run(&["lrange", "x", "0", "1"])), ["b", "c"]);

    t.ok(&["set", "foo", "bar"]);
    t.assert_err(&["lrem", "foo", "0", "elem"], "WRONGTYPE");
    t.assert_int(&["lrem", "nexists", "0", "elem"], 0);

    // Triggers QUICKLIST_NODE_CONTAINER_PLAIN coverage.
    let val = "a".repeat(10_000);
    t.assert_int(&["rpush", "b", &val, "12345678"], 2);
    t.assert_int(&["lrem", "b", "1", "12345678"], 1);
    t.assert_int(&["lrem", "b", "1", &val], 1);

    t.assert_int(&["lpush", "c", "bar", "bar", "foo"], 3);
    t.assert_int(&["lrem", "c", "-2", "bar"], 2);
    assert_eq!(strs(&t.run(&["lrange", "c", "0", "-1"])), ["foo"]);
}

#[test]
fn dump_restore_plain() {
    let mut t = Ctx::new();
    let val = "#".repeat(10_000);
    t.assert_int(&["lpush", "x", &val], 1);
    let buf = t.bulk(&["dump", "x"]);
    t.ok_b(&[b"restore".to_vec(), b"b".to_vec(), b"0".to_vec(), buf]);
    t.assert_int(&["llen", "b"], 1);
    assert_eq!(strs(&t.run(&["lrange", "b", "0", "1"])), [val]);
}

#[test]
fn ltrim() {
    let mut t = Ctx::new();
    t.assert_int(&["rpush", "x", "a", "b", "c", "d"], 4);
    t.ok(&["ltrim", "x", "-2", "-1"]);
    assert_eq!(strs(&t.run(&["lrange", "x", "0", "1"])), ["c", "d"]);
    t.ok(&["ltrim", "x", "0", "0"]);
    assert_eq!(strs(&t.run(&["lrange", "x", "0", "1"])), ["c"]);
    t.ok(&["set", "foo", "bar"]);
    t.assert_err(&["ltrim", "foo", "0", "1"], "WRONGTYPE");
    t.ok(&["ltrim", "nexists", "0", "1"]);
}

#[test]
fn lrange() {
    let mut t = Ctx::new();
    assert_eq!(
        t.run(&["lrange", "x", "0", "5"])
            .arr()
            .map(<[Value]>::to_vec),
        Some(vec![])
    );
    t.assert_int(&["rpush", "x", "0", "1", "2"], 3);
    assert_eq!(strs(&t.run(&["lrange", "x", "-2", "-1"])), ["1", "2"]);
}

#[test]
fn lset() {
    let mut t = Ctx::new();
    t.assert_int(&["rpush", "x", "0", "1", "2"], 3);
    t.ok(&["lset", "x", "0", "bar"]);
    assert_eq!(t.text(&["lpop", "x"]), "bar");
    t.ok(&["lset", "x", "-1", "foo"]);
    assert_eq!(t.text(&["rpop", "x"]), "foo");
    t.assert_int(&["rpush", "b", "a"], 1);
    t.assert_err(&["lset", "b", "1", "foo"], "index out of range");
}

#[test]
fn lpop() {
    let mut t = Ctx::new();
    t.assert_int(&["rpush", "foo", "bar"], 1);
    let v = t.run(&["lpop", "foo", "0"]);
    assert_eq!(v.arr().map(<[Value]>::to_vec), Some(vec![]));
    let v = t.run(&["lpop", "bar", "0"]);
    assert!(matches!(v, Value::Bulk(None)), "got {v:?}");
}

#[test]
fn lpos() {
    let mut t = Ctx::new();
    t.assert_int(&["rpush", "x", "1", "a", "b", "1", "1", "a", "1"], 7);

    t.assert_int(&["lpos", "x", "1"], 0);
    let v = t.run(&["lpos", "x", "f"]);
    assert!(matches!(v, Value::Bulk(None)), "got {v:?}");

    t.assert_err(
        &["lpos", "x", "1", "COUNT", "-1"],
        "COUNT can't be negative",
    );
    t.assert_err(
        &["lpos", "x", "1", "MAXLEN", "-1"],
        "MAXLEN can't be negative",
    );
    t.assert_err(&["lpos", "x", "1", "RANK", "0"], "RANK can't be zero");

    assert_eq!(
        ints(&t.run(&["lpos", "x", "a", "RANK", "-1", "COUNT", "2"])),
        [5, 1]
    );
    assert_eq!(
        ints(&t.run(&["lpos", "x", "1", "COUNT", "0"])),
        [0, 3, 4, 6]
    );
    assert_eq!(
        ints(&t.run(&["lpos", "x", "1", "COUNT", "0", "MAXLEN", "5"])),
        [0, 3, 4]
    );
}

#[test]
fn rpoplpush() {
    let mut t = Ctx::new();
    t.assert_int(&["rpush", "x", "1", "a", "b", "1", "2", "3", "4"], 7);

    for expected in ["4", "3", "2", "1"] {
        assert_eq!(t.text(&["rpoplpush", "x", "b"]), expected);
    }
    assert_eq!(strs(&t.run(&["lrange", "x", "0", "-1"])), ["1", "a", "b"]);
    assert_eq!(
        strs(&t.run(&["lrange", "b", "0", "-1"])),
        ["1", "2", "3", "4"]
    );

    for expected in ["b", "a", "1"] {
        assert_eq!(t.text(&["rpoplpush", "x", "b"]), expected);
    }
    assert_eq!(
        t.run(&["lrange", "x", "0", "-1"])
            .arr()
            .map(<[Value]>::to_vec),
        Some(vec![])
    );
    t.assert_int(&["exists", "x"], 0);
    let v = t.run(&["rpoplpush", "x", "b"]);
    assert!(matches!(v, Value::Bulk(None)), "got {v:?}");
    assert_eq!(
        strs(&t.run(&["lrange", "b", "0", "-1"])),
        ["1", "a", "b", "1", "2", "3", "4"]
    );

    // src and dest are the same key.
    t.assert_int(&["rpush", "x", "1", "a", "b", "1", "2", "3", "4"], 7);
    for expected in ["4", "3", "2", "1"] {
        assert_eq!(t.text(&["rpoplpush", "x", "x"]), expected);
    }
    assert_eq!(
        strs(&t.run(&["lrange", "x", "0", "-1"])),
        ["1", "2", "3", "4", "1", "a", "b"]
    );
    for expected in ["b", "a", "1"] {
        assert_eq!(t.text(&["rpoplpush", "x", "x"]), expected);
    }
    assert_eq!(
        strs(&t.run(&["lrange", "x", "0", "-1"])),
        ["1", "a", "b", "1", "2", "3", "4"]
    );
}

#[test]
fn lmove() {
    let mut t = Ctx::new();
    t.assert_int(&["rpush", "x", "1", "2", "3", "4", "5"], 5);

    assert_eq!(t.text(&["lmove", "x", "b", "LEFT", "RIGHT"]), "1");
    t.assert_int(&["llen", "x"], 4);
    assert_eq!(t.text(&["lmove", "x", "b", "LEFT", "LEFT"]), "2");
    assert_eq!(strs(&t.run(&["lrange", "b", "0", "-1"])), ["2", "1"]);
    assert_eq!(t.text(&["lmove", "x", "b", "RIGHT", "LEFT"]), "5");
    assert_eq!(strs(&t.run(&["lrange", "b", "0", "-1"])), ["5", "2", "1"]);
    assert_eq!(t.text(&["lmove", "x", "b", "RIGHT", "RIGHT"]), "4");
    assert_eq!(strs(&t.run(&["lrange", "x", "0", "-1"])), ["3"]);
    assert_eq!(
        strs(&t.run(&["lrange", "b", "0", "-1"])),
        ["5", "2", "1", "4"]
    );
    assert_eq!(t.text(&["lmove", "x", "b", "RIGHT", "RIGHT"]), "3");

    assert_eq!(
        t.run(&["lrange", "x", "0", "-1"])
            .arr()
            .map(<[Value]>::to_vec),
        Some(vec![])
    );
    t.assert_int(&["exists", "x"], 0);
    let v = t.run(&["lmove", "x", "b", "LEFT", "RIGHT"]);
    assert!(matches!(v, Value::Bulk(None)), "got {v:?}");
    let v = t.run(&["lmove", "x", "b", "RIGHT", "RIGHT"]);
    assert!(matches!(v, Value::Bulk(None)), "got {v:?}");
    assert_eq!(
        strs(&t.run(&["lrange", "b", "0", "-1"])),
        ["5", "2", "1", "4", "3"]
    );

    // src and dest are the same key.
    t.assert_int(&["rpush", "x", "1", "2", "3", "4", "5"], 5);
    assert_eq!(t.text(&["lmove", "x", "x", "LEFT", "RIGHT"]), "1");
    assert_eq!(t.text(&["lmove", "x", "x", "LEFT", "LEFT"]), "2");
    assert_eq!(t.text(&["lmove", "x", "x", "RIGHT", "LEFT"]), "1");
    assert_eq!(t.text(&["lmove", "x", "x", "RIGHT", "RIGHT"]), "5");
    assert_eq!(t.text(&["lmove", "x", "x", "LEFT", "RIGHT"]), "1");
    assert_eq!(
        strs(&t.run(&["lrange", "x", "0", "-1"])),
        ["2", "3", "4", "5", "1"]
    );
    assert_eq!(t.text(&["lmove", "x", "x", "LEFT", "RIGHT"]), "2");
    assert_eq!(t.text(&["lmove", "x", "x", "LEFT", "RIGHT"]), "3");
    assert_eq!(t.text(&["lmove", "x", "x", "RIGHT", "RIGHT"]), "3");
    assert_eq!(t.text(&["lmove", "x", "x", "LEFT", "RIGHT"]), "4");
    assert_eq!(
        strs(&t.run(&["lrange", "x", "0", "-1"])),
        ["5", "1", "2", "3", "4"]
    );

    t.assert_err(&["lmove", "x", "x", "LEFT", "R"], "syntax error");
}

#[test]
fn brpoplpush_single_shard() {
    let mut t = Ctx::new();

    let v = t.run(&["brpoplpush", "x", "y", "0.05"]);
    assert!(matches!(v, Value::Bulk(None)), "got {v:?}");

    t.assert_int(&["lpush", "x", "val1"], 1);
    assert_eq!(t.text(&["brpoplpush", "x", "y", "0.01"]), "val1");
    t.assert_int(&["exists", "x"], 0);

    t.ok(&["set", "x", "str"]);
    t.assert_err(&["brpoplpush", "y", "x", "0.01"], "wrong kind of value");

    t.assert_int(&["del", "x", "y"], 2);
    t.ok(&["multi"]);
    t.run(&["brpoplpush", "y", "x", "0"]);
    let v = t.run(&["exec"]);
    let a = v.arr().expect("array");
    assert_eq!(a.len(), 1);
    assert!(
        matches!(a[0], Value::Bulk(None) | Value::Array(None)),
        "got {:?}",
        a[0]
    );
}

#[test]
fn brpoplpush_single_shard_bug2857() {
    let mut t = Ctx::new();
    t.assert_int(&["lpush", "src", "val1"], 1);

    let fb = t.spawn(&["blpop", "dest", "4"]);
    sleep(Duration::from_millis(100));
    assert_eq!(t.text(&["brpoplpush", "src", "dest", "1"]), "val1");
    let resp = fb.join().unwrap();
    assert_eq!(strs(&resp), ["dest", "val1"]);

    // Timeout: src is empty.
    let fb = t.spawn(&["blpop", "dest", "4"]);
    sleep(Duration::from_millis(100));
    let v = t.run(&["brpoplpush", "src", "dest", "1"]);
    assert!(matches!(v, Value::Bulk(None)), "got {v:?}");
    let resp = fb.join().unwrap();
    assert!(
        matches!(resp, Value::Bulk(None) | Value::Array(None)),
        "got {resp:?}"
    );
}

#[test]
fn brpoplpush_single_shard_bug4569() {
    let mut t = Ctx::new();
    let fb = t.spawn(&["brpop", "x", "0"]);
    sleep(Duration::from_millis(100));
    t.assert_int(&["lpush", "y", "val"], 1);
    assert_eq!(t.text(&["rpoplpush", "y", "x"]), "val");
    let resp = fb.join().unwrap();
    assert_eq!(strs(&resp), ["x", "val"]);
}

#[test]
fn brpoplpush_single_shard_blocking() {
    let mut t = Ctx::new();
    let fb = t.spawn(&["brpoplpush", "x", "y", "0"]);
    sleep(Duration::from_millis(100));
    t.assert_int(&["lpush", "y", "2"], 1);
    t.assert_int(&["lpush", "x", "1"], 1);
    let resp = fb.join().unwrap();
    assert_eq!(resp.text().as_deref(), Some("1"));
}

#[test]
fn brpoplpush_two_shards() {
    let mut t = Ctx::new();

    let v = t.run(&["brpoplpush", "x", "z", "0.05"]);
    assert!(matches!(v, Value::Bulk(None)), "got {v:?}");

    t.assert_int(&["lpush", "x", "val"], 1);
    assert_eq!(t.text(&["brpoplpush", "x", "z", "0"]), "val");
    assert_eq!(strs(&t.run(&["lrange", "z", "0", "-1"])), ["val"]);
    t.assert_int(&["del", "z"], 1);

    let fb = t.spawn(&["brpoplpush", "x", "z", "0"]);
    sleep(Duration::from_millis(100));
    t.assert_int(&["lpush", "z", "val2"], 1);
    t.assert_int(&["lpush", "x", "val1"], 1);
    let resp = fb.join().unwrap();
    assert_eq!(resp.text().as_deref(), Some("val1"));
    assert_eq!(strs(&t.run(&["lrange", "z", "0", "-1"])), ["val1", "val2"]);
}

#[test]
fn blmove() {
    let mut t = Ctx::new();

    let v = t.run(&["blmove", "x", "y", "right", "right", "0.05"]);
    assert!(matches!(v, Value::Bulk(None)), "got {v:?}");

    t.assert_int(&["lpush", "x", "val1"], 1);
    t.assert_int(&["lpush", "y", "val2"], 1);
    assert_eq!(
        t.text(&["blmove", "x", "y", "right", "left", "0.01"]),
        "val1"
    );
    assert_eq!(strs(&t.run(&["lrange", "y", "0", "-1"])), ["val1", "val2"]);
}

#[test]
fn blocking_timeout_validation() {
    let mut t = Ctx::new();
    let not_float = "timeout is not a float or out of range";
    let out_of_range = "timeout is out of range";
    let negative = "timeout is negative";

    for c in [
        vec!["brpoplpush", "x", "y"],
        vec!["blmove", "x", "y", "LEFT", "RIGHT"],
        vec!["blpop", "k"],
        vec!["brpop", "k"],
    ] {
        for (timeout, err) in [
            ("abc", not_float),
            ("nan", not_float),
            ("inf", out_of_range),
            ("-inf", negative),
            ("-1", negative),
        ] {
            let mut args = c.clone();
            args.push(timeout);
            t.assert_err(&args, err);
        }
    }

    for (timeout, err) in [
        ("abc", not_float),
        ("nan", not_float),
        ("inf", out_of_range),
        ("-inf", negative),
        ("-1", negative),
        ("1e10", out_of_range),
    ] {
        t.assert_err(&["blmpop", timeout, "1", "k", "LEFT"], err);
    }
    for (timeout, err) in [
        ("abc", not_float),
        ("nan", not_float),
        ("inf", out_of_range),
        ("-inf", negative),
        ("-1", negative),
        ("1e10", out_of_range),
    ] {
        t.assert_err(&["blpop", "k", timeout], err);
    }

    // A large-but-representable timeout is accepted (returns immediately since
    // the key exists).
    t.assert_int(&["rpush", "k", "v"], 1);
    assert_eq!(strs(&t.run(&["blpop", "k", "4000000"])), ["k", "v"]);
}

#[test]
fn blmove_simultaneously() {
    let mut t = Ctx::new();

    let f1 = t.spawn(&["blmove", "src1", "dest110", "LEFT", "RIGHT", "0"]);
    let f2 = t.spawn(&["blmove", "src10", "dest110", "LEFT", "RIGHT", "0"]);
    sleep(Duration::from_millis(100));

    t.ok(&["multi"]);
    t.run(&["rpush", "src1", "v1"]);
    t.run(&["rpush", "src10", "v2"]);
    let v = t.run(&["exec"]);
    assert_eq!(ints(&v), [1, 1]);

    f1.join().unwrap();
    f2.join().unwrap();

    let res = t.run(&["lrange", "dest110", "0", "-1"]);
    let mut got = sorted(&res);
    got.sort();
    assert_eq!(got, ["v1", "v2"]);
}

#[test]
fn blmove_rings() {
    let mut t = Ctx::new();

    // Move 5 times in rings 0 -> 1 -> ... -> 9 -> 0.
    let mut fibers = Vec::new();
    for _j in 0..5 {
        for i in 0..10 {
            let k1 = i.to_string();
            let k2 = ((i + 1) % 10).to_string();
            fibers.push(t.spawn(&["blmove", &k1, &k2, "LEFT", "RIGHT", "0"]));
        }
    }

    sleep(Duration::from_millis(100));
    t.assert_int(&["lpush", "0", "v1"], 1);

    for f in fibers {
        f.join().unwrap();
    }
    for i in 1..10 {
        t.assert_int(&["llen", &i.to_string()], 0);
    }
    assert_eq!(strs(&t.run(&["lrange", "0", "0", "-1"])), ["v1"]);
}

#[test]
fn lpushx() {
    let mut t = Ctx::new();
    t.assert_int(&["lpushx", "x", "val1"], 0);
    t.assert_int(&["llen", "x"], 0);

    t.assert_int(&["lpush", "x", "val1"], 1);
    assert_eq!(strs(&t.run(&["lrange", "x", "0", "-1"])), ["val1"]);
    t.assert_int(&["lpushx", "x", "val2"], 2);
    assert_eq!(strs(&t.run(&["lrange", "x", "0", "-1"])), ["val2", "val1"]);
}

#[test]
fn rpushx() {
    let mut t = Ctx::new();
    t.assert_int(&["rpushx", "x", "val1"], 0);
    t.assert_int(&["llen", "x"], 0);

    t.assert_int(&["rpush", "x", "val1"], 1);
    assert_eq!(strs(&t.run(&["lrange", "x", "0", "-1"])), ["val1"]);
    t.assert_int(&["rpushx", "x", "val2"], 2);
    assert_eq!(strs(&t.run(&["lrange", "x", "0", "-1"])), ["val1", "val2"]);
}

#[test]
fn linsert() {
    let mut t = Ctx::new();

    // List not found.
    t.assert_int(&["linsert", "notfound", "before", "foo", "bar"], 0);

    // Key is not a list.
    t.ok(&["set", "notalist", "x"]);
    t.assert_err(
        &["linsert", "notalist", "before", "foo", "bar"],
        "wrong kind of value",
    );

    // Insert before.
    t.assert_int(&["rpush", "mylist", "foo"], 1);
    t.assert_int(&["linsert", "mylist", "before", "foo", "bar"], 2);
    assert_eq!(
        strs(&t.run(&["lrange", "mylist", "0", "1"])),
        ["bar", "foo"]
    );

    // Insert after.
    t.assert_int(&["linsert", "mylist", "after", "foo", "car"], 3);
    assert_eq!(
        strs(&t.run(&["lrange", "mylist", "0", "2"])),
        ["bar", "foo", "car"]
    );

    // Pivot not found.
    t.assert_int(&["linsert", "mylist", "before", "notfound", "x"], -1);
    t.assert_int(&["linsert", "mylist", "after", "notfound", "x"], -1);

    // Insert empty.
    t.assert_int(&["rpush", "k", "a"], 1);
    t.assert_int(&["linsert", "k", "before", "a", ""], 2);
    assert_eq!(t.text(&["lpop", "k"]), "");
    t.assert_int(&["linsert", "k", "before", "", ""], -1);
}

#[test]
fn blpop_unwakes_in_script() {
    let mut t = Ctx::new();
    let script = r"
        for i = 1, 1000 do
          redis.call('MGET', 'a', 'b', 'c', 'd')
          redis.call('LPUSH', 'l', tostring(i))
        end
    ";

    let f1 = t.spawn(&["blpop", "l", "0"]);
    let f2 = t.spawn(&["eval", script, "5", "a", "b", "c", "d", "l"]);

    // A quick timed-out blpop while the script is running.
    let resp = t.run(&["blpop", "g", "0.01"]);
    assert!(
        matches!(resp, Value::Bulk(None) | Value::Array(None)),
        "got {resp:?}"
    );

    f2.join().unwrap();
    let resp = f1.join().unwrap();
    assert_eq!(strs(&resp), ["l", "1000"]);
}

#[test]
fn other_multi_wakes_blpop() {
    let mut t = Ctx::new();
    let script = r"
        redis.call('LPUSH', 'l', 'bad')
        for i = 1, 1000 do
          redis.call('MGET', 'a', 'b', 'c', 'd')
        end
        redis.call('LPUSH', 'l', 'good')
    ";
    let script_short = r"redis.call('GET', KEYS[1])";

    let f1 = t.spawn(&["blpop", "l", "0"]);
    let f2 = t.spawn(&["eval", script, "5", "a", "b", "c", "d", "l"]);

    // A quick multi transaction that concludes after one hop.
    t.run(&["eval", script_short, "1", "y"]);

    f2.join().unwrap();
    let resp = f1.join().unwrap();
    assert_eq!(strs(&resp), ["l", "good"]);
}

#[test]
fn lmpop_invalid_syntax() {
    let mut t = Ctx::new();
    t.assert_err(&["lmpop", "1", "a"], "wrong number of arguments");
    t.assert_err(
        &["lmpop", "0", "LEFT", "COUNT", "1"],
        "at least 1 input key is needed",
    );
    t.assert_err(
        &["lmpop", "aa", "a", "LEFT"],
        "value is not an integer or out of range",
    );
    t.assert_err(&["lmpop", "1", "a", "COUNT", "1"], "syntax error");
    t.assert_err(&["lmpop", "1", "a", "b", "LEFT"], "syntax error");
    t.assert_err(&["lmpop", "1", "a", "LEFT", "COUNT"], "syntax error");
    t.assert_err(
        &["lmpop", "1", "a", "LEFT", "COUNT", "boo"],
        "value is not an integer or out of range",
    );
    t.assert_err(
        &["lmpop", "1", "c", "LEFT", "COUNT", "2", "foo"],
        "syntax error",
    );
}

#[test]
fn lmpop() {
    let mut t = Ctx::new();

    // All lists empty.
    let v = t.run(&["lmpop", "1", "e", "LEFT"]);
    assert!(
        matches!(v, Value::Bulk(None) | Value::Array(None)),
        "got {v:?}"
    );

    // LEFT operation.
    t.assert_int(&["lpush", "a", "a1", "a2"], 2);
    let v = t.run(&["lmpop", "1", "a", "LEFT"]);
    assert_eq!(key(&v), "a");
    assert_eq!(strs(&v.arr().unwrap()[1]), ["a2"]);

    // RIGHT operation.
    t.assert_int(&["lpush", "b", "b1", "b2"], 2);
    let v = t.run(&["lmpop", "1", "b", "RIGHT"]);
    assert_eq!(key(&v), "b");
    assert_eq!(strs(&v.arr().unwrap()[1]), ["b1"]);

    // COUNT > 1.
    t.assert_int(&["lpush", "c", "c1", "c2"], 2);
    let v = t.run(&["lmpop", "1", "c", "RIGHT", "COUNT", "2"]);
    assert_eq!(strs(&v.arr().unwrap()[1]), ["c1", "c2"]);
    t.assert_int(&["llen", "c"], 0);

    // COUNT > number of elements.
    t.assert_int(&["lpush", "d", "d1", "d2"], 2);
    let v = t.run(&["lmpop", "1", "d", "RIGHT", "COUNT", "3"]);
    assert_eq!(strs(&v.arr().unwrap()[1]), ["d1", "d2"]);
    t.assert_int(&["llen", "d"], 0);

    // First non-empty list is not the first list.
    t.assert_int(&["lpush", "x", "x1"], 1);
    t.assert_int(&["lpush", "y", "y1"], 1);
    let v = t.run(&["lmpop", "3", "empty", "x", "y", "RIGHT"]);
    assert_eq!(key(&v), "x");
    assert_eq!(strs(&v.arr().unwrap()[1]), ["x1"]);
    t.assert_int(&["llen", "x"], 0);
}

#[test]
fn lmpop_multiple_elements() {
    let mut t = Ctx::new();

    t.assert_int(&["rpush", "list1", "a", "b", "c", "d", "e"], 5);
    let v = t.run(&["lmpop", "1", "list1", "LEFT", "COUNT", "3"]);
    assert_eq!(strs(&v.arr().unwrap()[1]), ["a", "b", "c"]);
    assert_eq!(strs(&t.run(&["lrange", "list1", "0", "-1"])), ["d", "e"]);

    t.assert_int(&["rpush", "list2", "v", "w", "x", "y", "z"], 5);
    let v = t.run(&["lmpop", "1", "list2", "RIGHT", "COUNT", "2"]);
    assert_eq!(strs(&v.arr().unwrap()[1]), ["z", "y"]);
    assert_eq!(
        strs(&t.run(&["lrange", "list2", "0", "-1"])),
        ["v", "w", "x"]
    );
}

#[test]
fn lmpop_multiple_lists() {
    let mut t = Ctx::new();
    t.assert_int(&["rpush", "list1", "a", "b"], 2);
    t.assert_int(&["rpush", "list2", "c", "d"], 2);
    t.assert_int(&["rpush", "list3", "e", "f"], 2);

    let v = t.run(&["lmpop", "3", "list1", "list2", "list3", "LEFT"]);
    assert_eq!(key(&v), "list1");
    assert_eq!(strs(&v.arr().unwrap()[1]), ["a"]);

    // Pop from the second list after the first becomes empty.
    t.run(&["lmpop", "1", "list1", "LEFT"]);
    let v = t.run(&[
        "lmpop", "3", "list1", "list2", "list3", "RIGHT", "COUNT", "2",
    ]);
    assert_eq!(key(&v), "list2");
    assert_eq!(strs(&v.arr().unwrap()[1]), ["d", "c"]);
    assert_eq!(strs(&t.run(&["lrange", "list3", "0", "-1"])), ["e", "f"]);
}

#[test]
fn lmpop_edge_cases() {
    let mut t = Ctx::new();

    // Empty list.
    t.assert_int(&["rpush", "empty_list", "a"], 1);
    t.run(&["lpop", "empty_list"]);
    let v = t.run(&["lmpop", "1", "empty_list", "LEFT"]);
    assert!(
        matches!(v, Value::Bulk(None) | Value::Array(None)),
        "got {v:?}"
    );

    // Nonexistent list.
    let v = t.run(&["lmpop", "1", "nonexistent", "LEFT"]);
    assert!(
        matches!(v, Value::Bulk(None) | Value::Array(None)),
        "got {v:?}"
    );

    // Wrong type key.
    t.ok(&["set", "string_key", "value"]);
    t.assert_err(&["lmpop", "1", "string_key", "LEFT"], "WRONGTYPE");

    // Default COUNT of 1.
    t.assert_int(&["rpush", "list", "a", "b"], 2);
    let v = t.run(&["lmpop", "1", "list", "LEFT"]);
    assert_eq!(key(&v), "list");
    assert_eq!(strs(&v.arr().unwrap()[1]), ["a"]);

    // COUNT 0 returns an empty element array.
    let v = t.run(&["lmpop", "1", "list", "LEFT", "COUNT", "0"]);
    assert_eq!(key(&v), "list");
    assert_eq!(
        v.arr().unwrap()[1].arr().map(<[Value]>::to_vec),
        Some(vec![])
    );

    // Negative COUNT.
    t.assert_err(
        &["lmpop", "1", "list", "LEFT", "COUNT", "-1"],
        "value is not an integer or out of range",
    );
}

#[test]
fn lmpop_doc_example() {
    let mut t = Ctx::new();

    let v = t.run(&["lmpop", "2", "non1", "non2", "LEFT", "COUNT", "10"]);
    assert!(
        matches!(v, Value::Bulk(None) | Value::Array(None)),
        "got {v:?}"
    );

    t.assert_int(
        &["lpush", "mylist", "one", "two", "three", "four", "five"],
        5,
    );
    let v = t.run(&["lmpop", "1", "mylist", "LEFT"]);
    assert_eq!(key(&v), "mylist");
    assert_eq!(strs(&v.arr().unwrap()[1]), ["five"]);
    assert_eq!(
        strs(&t.run(&["lrange", "mylist", "0", "-1"])),
        ["four", "three", "two", "one"]
    );

    let v = t.run(&["lmpop", "1", "mylist", "RIGHT", "COUNT", "10"]);
    assert_eq!(strs(&v.arr().unwrap()[1]), ["one", "two", "three", "four"]);

    t.assert_int(
        &["lpush", "mylist", "one", "two", "three", "four", "five"],
        5,
    );
    t.assert_int(&["lpush", "mylist2", "a", "b", "c", "d", "e"], 5);

    let v = t.run(&["lmpop", "2", "mylist", "mylist2", "RIGHT", "COUNT", "3"]);
    assert_eq!(key(&v), "mylist");
    assert_eq!(strs(&v.arr().unwrap()[1]), ["one", "two", "three"]);
    assert_eq!(
        strs(&t.run(&["lrange", "mylist", "0", "-1"])),
        ["five", "four"]
    );

    let v = t.run(&["lmpop", "2", "mylist", "mylist2", "RIGHT", "COUNT", "5"]);
    assert_eq!(key(&v), "mylist");
    assert_eq!(strs(&v.arr().unwrap()[1]), ["four", "five"]);

    let v = t.run(&["lmpop", "2", "mylist", "mylist2", "RIGHT", "COUNT", "10"]);
    assert_eq!(key(&v), "mylist2");
    assert_eq!(strs(&v.arr().unwrap()[1]), ["a", "b", "c", "d", "e"]);

    t.assert_int(&["exists", "mylist", "mylist2"], 0);
}

#[test]
fn lmpop_wrong_type() {
    let mut t = Ctx::new();
    t.assert_int(&["lpush", "l1", "e1"], 1);
    t.assert_int(&["hset", "foo", "k1", "v1"], 1);

    // First key is the wrong type.
    t.assert_err(&["lmpop", "2", "foo", "l1", "left"], "WRONGTYPE");

    // Second key is the wrong type but the first doesn't exist.
    t.assert_err(&["lmpop", "2", "nonexistent", "foo", "left"], "WRONGTYPE");

    // Second key is the wrong type but the first is a valid list.
    let v = t.run(&["lmpop", "2", "l1", "foo", "left"]);
    assert_eq!(key(&v), "l1");
    assert_eq!(strs(&v.arr().unwrap()[1]), ["e1"]);
}

#[test]
fn awake_db1() {
    let mut t = Ctx::new();

    let f1 = t.spawn_fn(|c| {
        c.cmd(&["select", "1"]).unwrap();
        c.cmd(&["brpoplpush", "x", "y", "0"]).unwrap()
    });
    sleep(Duration::from_millis(100));
    t.ok(&["select", "1"]);
    t.assert_int(
        &[
            "eval",
            "redis.call('LPUSH', KEYS[1], 'val'); return 1;",
            "1",
            "x",
        ],
        1,
    );
    let resp = f1.join().unwrap();
    assert_eq!(resp.text().as_deref(), Some("val"));
}

// Skipped scheduler-stress tests from the reference: TwoQueueBug451,
// BRPopContended, BLMoveWaves, ContendExpire, AwakeMulti, PressureBLMove.
// They stress fiber scheduling internals and mostly assert unobservable
// controller state; the port's threads cannot meaningfully reproduce them.
