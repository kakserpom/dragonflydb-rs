//! Port of `dragonfly/src/server/string_family_test.cc`.
//!
//! Adaptations from the C++ original:
//! - `GetMetrics` / `GetDebugInfo` / `ExpectUsedKeys` assertions are dropped
//!   (the port exposes no equivalent counters / shard-count introspection).
//! - `AdvanceTime` is served by the process-global test clock from `common`:
//!   time-dependent tests run one at a time under `clock_guard` and assert
//!   exact TTLs instead of second-boundary ranges.
//! - Internal `CompactObj` pin/orphan/drain tests (they probe C++ memory
//!   mechanics) and `cache_mode`/`cluster_mode` flag-dependent tests are
//!   skipped; the observable RESP behavior they assert is covered here.

mod common;

use common::*;

/// `ToIntArr` from the C++ harness: assert an array of integer-formatted
/// bulk strings and return the parsed values.
fn int_arr(v: &Value) -> Vec<i64> {
    let a = v
        .arr()
        .unwrap_or_else(|| panic!("expected array, got {v:?}"));
    a.iter()
        .map(|x| {
            x.text()
                .unwrap_or_else(|| panic!("expected numeric bulk, got {x:?}"))
                .parse()
                .unwrap_or_else(|_| panic!("expected numeric bulk, got {x:?}"))
        })
        .collect()
}

#[test]
fn set_get() {
    let mut t = Ctx::new();
    t.assert_text(&["set", "key", "val"], "OK");
    t.assert_text(&["get", "key"], "val");
    t.assert_text(&["set", "key1", "1"], "OK");
    t.assert_text(&["get", "key1"], "1");
    t.assert_text(&["set", "key", "2"], "OK");
    t.assert_text(&["get", "key"], "2");
    t.assert_null(&["get", "key3"]);
}

#[test]
fn incr() {
    let mut t = Ctx::new();
    t.ok(&["set", "key", "0"]);
    t.assert_int(&["incr", "key"], 1);

    t.ok(&["set", "key1", "123456789"]);
    t.assert_int(&["incrby", "key1", "0"], 123456789);

    t.ok(&["set", "key1", "-123456789"]);
    t.assert_int(&["incrby", "key1", "0"], -123456789);

    t.ok(&["set", "key1", "   -123  "]);
    t.assert_err(&["incrby", "key1", "1"], "ERR value is not an integer");

    t.assert_int(&["incrby", "ne", "0"], 0);
    t.assert_err(&["decrby", "a", "-9223372036854775808"], "overflow");
}

#[test]
fn append() {
    let mut t = Ctx::new();
    t.ok(&["setex", "key", "100", "val"]);
    assert!(t.int(&["ttl", "key"]) > 0 && t.int(&["ttl", "key"]) <= 100);
    t.assert_int(&["append", "key", "bar"], 6);
    assert!(t.int(&["ttl", "key"]) > 0 && t.int(&["ttl", "key"]) <= 100);
}

#[test]
fn expire() {
    let mut t = Ctx::new();
    let _clock = clock_guard();
    t.ok(&["set", "key", "val", "PX", "100"]);
    advance(20);
    t.assert_text(&["get", "key"], "val");
    advance(120);
    t.assert_null(&["get", "key"]);

    t.ok(&["set", "i", "1", "PX", "100"]);
    t.assert_int(&["incr", "i"], 2);
    advance(150);
    t.assert_int(&["incr", "i"], 1);
}

#[test]
fn keepttl() {
    let mut t = Ctx::new();
    t.ok(&["set", "key", "val", "EX", "100"]);
    t.ok(&["set", "key", "val"]);
    assert_eq!(t.int(&["ttl", "key"]), -1);

    t.ok(&["set", "key", "val", "EX", "200"]);
    t.ok(&["set", "key", "val", "KEEPTTL"]);
    let ttl = t.int(&["ttl", "key"]);
    assert!(ttl > 0 && ttl <= 200);
}

#[test]
fn set_options_syntax_error() {
    let mut t = Ctx::new();
    let now_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let exat = (now_s + 1030).to_string();
    let pxat = (now_ms + 1030).to_string();
    let exat_past = (now_s + 1030).to_string();

    for args in [
        vec!["set", "key", "val", "EX", "1030", "PX", "1030"],
        vec!["set", "key", "val", "EX", "1030", "EXAT", &exat],
        vec!["set", "key", "val", "EX", "1030", "PXAT", &pxat],
        vec!["set", "key", "val", "PX", "1030", "EX", "1030"],
        vec!["set", "key", "val", "PX", "1030", "EXAT", &exat],
        vec!["set", "key", "val", "PX", "1030", "PXAT", &pxat],
        vec!["set", "key", "val", "EXAT", &exat, "EX", "1030"],
        vec!["set", "key", "val", "EXAT", &exat, "PX", "1030"],
        vec!["set", "key", "val", "EXAT", &exat_past, "PXAT", &pxat],
        vec!["set", "key", "val", "PXAT", &pxat, "EX", "1030"],
        vec!["set", "key", "val", "PXAT", &pxat, "PX", "1030"],
        vec!["set", "key", "val", "PXAT", &pxat, "EXAT", &exat],
        vec!["set", "key", "val", "EX", "1030", "KEEPTTL"],
        vec!["set", "key", "val", "PX", "1030", "KEEPTTL"],
        vec!["set", "key", "val", "EXAT", &exat, "KEEPTTL"],
        vec!["set", "key", "val", "PXAT", &pxat, "KEEPTTL"],
        vec!["set", "key", "val", "KEEPTTL", "PX", "1030"],
        vec!["set", "key", "val", "KEEPTTL", "PXAT", &pxat],
        vec!["set", "key", "val", "KEEPTTL", "EX", "1030"],
        vec!["set", "key", "val", "KEEPTTL", "EXAT", &exat],
    ] {
        t.assert_err(&args, "ERR syntax error");
    }

    t.assert_err(&["set", "key", "val", "NX", "XX"], "ERR syntax error");
    t.assert_err(&["set", "key", "val", "XX", "NX"], "ERR syntax error");

    t.assert_err(
        &["set", "key", "val", "PX", "9223372036854775800"],
        "invalid expire time",
    );
    t.assert_err(
        &["SET", "foo", "bar", "EX", "18446744073709561"],
        "invalid expire time",
    );
}

#[test]
fn set() {
    let mut t = Ctx::new();
    t.assert_null(&["set", "foo", "bar", "XX"]);
    t.ok(&["set", "foo", "bar", "NX"]);
    t.assert_null(&["set", "foo", "bar", "NX"]);
    t.ok(&["set", "foo", "bar", "xx"]);
    t.assert_err(
        &["set", "foo", "bar", "ex", "abc"],
        "value is not an integer or out of range",
    );
    t.assert_err(&["set", "foo", "bar", "ex", "-1"], "invalid expire time");
    t.ok(&["set", "foo", "bar", "ex", "1"]);

    t.assert_int(&["sadd", "s1", "1"], 1);
    t.ok(&["set", "s1", "2"]);
}

#[test]
fn mset_long() {
    let mut t = Ctx::new();
    let mut args = vec!["mset"];
    for i in 0..12000 {
        args.push(Box::leak(format!("key{i}").into_boxed_str()));
        args.push(Box::leak(format!("val{i}").into_boxed_str()));
    }
    let v = t.run(&args);
    expect_ok(&v);
}

#[test]
fn mget_set() {
    let mut t = Ctx::new();
    t.ok(&["mset", "z", "0"]);
    let a = t.arr(&["mget", "z"]);
    assert_eq!(a.len(), 1);
    expect_text(&a[0], "0");

    t.ok(&["mset", "x", "0", "b", "0"]);

    // Concurrent MGET vs MSET across two connections: MGET must never observe
    // a torn write (x and b always move together).
    let port = t.server.port();
    let get_h = std::thread::spawn(move || {
        let mut c = Client::connect(port).unwrap();
        for _ in 0..1000 {
            let v = c.cmd(&["mget", "b", "x"]).unwrap();
            let ivec = int_arr(&v);
            assert_eq!(ivec.len(), 2);
            assert!(ivec[1] >= ivec[0]);
        }
    });
    let set_h = std::thread::spawn(move || {
        let mut c = Client::connect(port).unwrap();
        for i in 1..2000 {
            let n = i.to_string();
            c.cmd(&["set", "x", &n]).unwrap();
            c.cmd(&["set", "b", &n]).unwrap();
        }
    });
    get_h.join().unwrap();
    set_h.join().unwrap();
}

#[test]
fn mset_get() {
    let mut t = Ctx::new();
    t.ok(&["mset", "x", "0", "y", "0", "a", "0", "b", "0"]);

    t.ok(&["mset", "x", "0", "y", "0"]);

    // Duplicate key: last write wins.
    t.ok(&["mset", "x", "1", "b", "5", "x", "0"]);
    t.assert_text(&["get", "x"], "0");
    t.assert_text(&["get", "b"], "5");

    // MSET must be atomic with respect to GET: x and b are always set to the
    // same value together, so a reader never sees x ahead of b.
    let port = t.server.port();
    let set_h = std::thread::spawn(move || {
        let mut c = Client::connect(port).unwrap();
        for i in 0..1000 {
            let n = i.to_string();
            let v = c.cmd(&["mset", "x", &n, "b", &n]).unwrap();
            assert_eq!(v.text().as_deref(), Some("OK"), "iteration {i}");
        }
    });
    let get_h = std::thread::spawn(move || {
        let mut c = Client::connect(port).unwrap();
        for _ in 0..1000 {
            let x = c
                .cmd(&["get", "x"])
                .unwrap()
                .text()
                .unwrap()
                .parse::<i64>()
                .unwrap();
            let z = c
                .cmd(&["get", "b"])
                .unwrap()
                .text()
                .unwrap()
                .parse::<i64>()
                .unwrap();
            assert!(x <= z, "Inconsistency: x={x} b={z}");
        }
    });
    set_h.join().unwrap();
    get_h.join().unwrap();
}

#[test]
fn mset_del() {
    let mut t = Ctx::new();
    let port = t.server.port();
    let set_h = std::thread::spawn(move || {
        let mut c = Client::connect(port).unwrap();
        for _ in 0..1000 {
            c.cmd(&["mset", "x", "0", "z", "0"]).unwrap();
        }
    });
    let del_h = std::thread::spawn(move || {
        let mut c = Client::connect(port).unwrap();
        for _ in 0..1000 {
            c.cmd(&["del", "x", "z"]).unwrap();
        }
    });
    set_h.join().unwrap();
    del_h.join().unwrap();
}

#[test]
fn int_key() {
    let mut t = Ctx::new();
    t.ok(&["mset", "1", "1", "-1000", "-1000"]);
    t.assert_text(&["get", "1"], "1");
}

#[test]
fn single_shard() {
    let mut t = Ctx::new();
    t.ok(&["mset", "x", "1", "y", "1"]);
    let resp = t.arr(&["mget", "x", "y"]);
    assert_eq!(int_arr(&Value::Array(Some(resp))), vec![1, 1]);

    let port = t.server.port();
    let set_h = std::thread::spawn(move || {
        let mut c = Client::connect(port).unwrap();
        for _ in 0..100 {
            c.cmd(&["mset", "x", "0", "y", "0"]).unwrap();
        }
    });
    let get_h = std::thread::spawn(move || {
        let mut c = Client::connect(port).unwrap();
        for _ in 0..100 {
            c.cmd(&["mget", "x", "b", "y"]).unwrap();
        }
    });
    set_h.join().unwrap();
    get_h.join().unwrap();
}

#[test]
fn mset_incr() {
    let mut t = Ctx::new();
    t.ok(&["mset", "a", "0", "b", "0", "c", "0"]);

    // MSET writes the same base to b/a/c, INCR bumps them one at a time. If
    // MSET were not atomic with respect to INCR, a and b could get out of
    // order. The invariant: a <= b and a <= c after each INCR pair.
    let port = t.server.port();
    let set_h = std::thread::spawn(move || {
        let mut c = Client::connect(port).unwrap();
        for i in 1..1000 {
            let base = (i * 900).to_string();
            let v = c
                .cmd(&["mset", "b", &base, "a", &base, "c", &base])
                .unwrap();
            assert_eq!(v.text().as_deref(), Some("OK"), "iteration {i}");
        }
    });
    let get_h = std::thread::spawn(move || {
        let mut c = Client::connect(port).unwrap();
        for _ in 0..900 {
            let a = c.cmd(&["incr", "a"]).unwrap().int().unwrap();
            let b = c.cmd(&["incr", "b"]).unwrap().int().unwrap();
            assert!(a <= b, "a={a} > b={b}");
            let cval = c.cmd(&["incr", "c"]).unwrap().int().unwrap();
            assert!(a <= cval, "a={a} > c={cval}");
        }
    });
    set_h.join().unwrap();
    get_h.join().unwrap();
}

#[test]
fn set_ex() {
    let mut t = Ctx::new();
    let _clock = clock_guard();
    t.ok(&["setex", "key", "1", "val"]);
    t.ok(&["setex", "key", "10", "val"]);
    assert_eq!(t.int(&["ttl", "key"]), 10);
    t.assert_err(&["setex", "key", "0", "val"], "invalid expire time");
    t.ok(&["setex", "key", "157680000", "val"]); // 5 * 365 * 24 * 3600
    t.ok(&["setex", "key", "1073741824", "val"]); // 1 << 30
    assert_eq!(t.int(&["ttl", "key"]), 268_435_455); // kMaxExpireDeadlineSec
    t.assert_err(
        &["SETEX", "foo", "18446744073709561", "bar"],
        "invalid expire time",
    );
}

#[test]
fn range() {
    let mut t = Ctx::new();
    t.ok(&["set", "key1", "Hello World"]);
    t.assert_text(&["getrange", "key1", "5", "3"], "");

    t.assert_int(&["SETRANGE", "key1", "6", "Earth"], 11);
    t.assert_text(&["get", "key1"], "Hello Earth");

    t.assert_int(&["SETRANGE", "key2", "2", "Earth"], 7);
    t.assert_text(&["get", "key2"], "\0\0Earth");

    t.assert_int(&["SETRANGE", "key3", "0", ""], 0);
    t.assert_int(&["exists", "key3"], 0);
    t.assert_int(&["SETRANGE", "key3", "0", "abc"], 3);
    t.assert_int(&["exists", "key3"], 1);

    t.ok(&["SET", "key3", "123"]);
    t.assert_text(&["getrange", "key3", "2", "3"], "3");
    t.assert_text(&["getrange", "key3", "3", "3"], "");
    t.assert_text(&["getrange", "key3", "4", "5"], "");

    t.ok(&["SET", "num", "1234"]);
    t.assert_text(&["getrange", "num", "3", "5000"], "4");
    t.assert_text(&["getrange", "num", "-5000", "10000"], "1234");

    t.ok(&["SET", "key4", "1"]);
    t.assert_text(&["getrange", "key4", "-1", "-2"], "");
    t.assert_text(&["getrange", "key4", "0", "-2"], "1");

    t.assert_int(&["SETRANGE", "key5", "1", ""], 0);
    t.assert_null(&["GET", "key5"]);

    t.assert_int(&["SETRANGE", "num", "6", ""], 4);
    t.assert_text(&["GET", "num"], "1234");
}

#[test]
fn incr_by_float() {
    let mut t = Ctx::new();
    t.ok(&["SET", "nonum", "  11"]);
    t.assert_err(&["INCRBYFLOAT", "nonum", "1.0"], "not a valid float");

    t.ok(&["SET", "inf", "+inf"]);
    t.assert_err(
        &["INCRBYFLOAT", "inf", "1.0"],
        "increment would produce NaN or Infinity",
    );

    t.ok(&["SET", "nonum", "11 "]);
    t.assert_err(&["INCRBYFLOAT", "nonum", "1.0"], "not a valid float");

    t.ok(&["SET", "num", "2.566"]);
    t.assert_text(&["INCRBYFLOAT", "num", "1.0"], "3.566");
}

#[test]
fn restore_high_ttl() {
    let mut t = Ctx::new();
    t.ok(&["SET", "X", "1"]);
    let buffer = t.bulk(&["DUMP", "X"]);
    t.int(&["DEL", "X"]);
    t.ok_b(&[
        b"RESTORE".to_vec(),
        b"X".to_vec(),
        b"5430186761345".to_vec(),
        buffer,
    ]);
}

#[test]
fn set_nx() {
    let mut t = Ctx::new();
    for args in [
        &["setnx", "foo", "bar", "XX"][..],
        &["setnx", "foo", "bar", "NX"][..],
        &["setnx", "foo", "bar", "xx"][..],
        &["setnx", "foo", "bar", "ex", "abc"][..],
        &["setnx", "foo", "bar", "ex", "-1"][..],
        &["setnx", "foo", "bar", "ex", "1"][..],
    ] {
        t.assert_err(args, "wrong number of arguments");
    }

    t.assert_int(&["setnx", "foo", "bar"], 1);
    t.assert_text(&["get", "foo"], "bar");
    t.assert_int(&["setnx", "foo", "hello"], 0);
    t.assert_text(&["get", "foo"], "bar");
}

#[test]
fn set_px_at_ex_at() {
    let mut t = Ctx::new();
    let _clock = clock_guard();
    let now_s = clock_ms() / 1000;
    let now_ms = clock_ms();

    t.assert_err(&["set", "foo", "bar", "EXAT", "-1"], "invalid expire time");
    t.ok(&["set", "foo", "bar", "EXAT", &(now_s - 1).to_string()]);
    t.assert_null(&["get", "foo"]);

    t.assert_err(&["set", "foo", "bar", "PXAT", "-1"], "invalid expire time");
    t.ok(&["set", "foo", "bar", "PXAT", &(now_ms - 23).to_string()]);
    t.assert_null(&["get", "foo"]);

    t.ok(&["set", "foo", "bar", "EXAT", &(now_s + 1).to_string()]);
    t.assert_text(&["get", "foo"], "bar");

    t.ok(&["set", "foo2", "abc", "PXAT", &(now_ms + 300).to_string()]);
    t.assert_text(&["get", "foo2"], "abc");
}

#[test]
fn set_stick() {
    let mut t = Ctx::new();
    t.ok(&["set", "foo", "bar", "STICK"]);
    t.assert_int(&["STICK", "foo"], 0);
}

#[test]
fn get_del() {
    let mut t = Ctx::new();
    t.ok(&["set", "foo", "bar"]);
    t.assert_text(&["getdel", "foo"], "bar");
    t.assert_null(&["get", "foo"]);
}

#[test]
fn get_ex() {
    let mut t = Ctx::new();
    let _clock = clock_guard();
    let now_ms = clock_ms();

    t.ok(&["set", "foo", "bar"]);
    t.assert_err(&["getex", "foo", "EX"], "syntax error");
    t.assert_err(&["getex", "foo", "EX", "1", "px", "1"], "syntax error");
    t.assert_err(&["getex", "foo", "bar", "EX"], "syntax error");
    t.assert_err(&["getex", "foo", "PERSIST", "1"], "syntax error");
    t.assert_err(&["getex", "foo", "PERSIST", "EX", "1"], "syntax error");
    t.assert_err(&["getex", "foo", "EX", "1", "PERSIST"], "syntax error");
    t.assert_err(
        &[
            "getex",
            "foo",
            "PXAT",
            &(now_ms + 1000).to_string(),
            "PERSIST",
        ],
        "syntax error",
    );
    t.assert_err(&["getex", "foo", "PERSIST", "PERSIST"], "syntax error");
    t.assert_err(&["getex", "foo", "PXAT"], "syntax error");
    t.assert_err(&["getex", "foo", "EX", "0"], "invalid expire time");
    t.assert_err(&["getex", "foo", "PXAT", "-1"], "invalid expire time");

    t.assert_text(&["getex", "foo"], "bar");

    t.assert_text(&["getex", "foo", "PERSIST"], "bar");
    assert_eq!(t.int(&["TTL", "foo"]), -1);

    // Already-expired PXAT: returns the value and deletes the key.
    t.assert_text(&["getex", "foo", "pxat", &(now_ms - 1).to_string()], "bar");
    t.assert_null(&["getex", "foo"]);

    // Short PX: value readable, then expires.
    t.ok(&["set", "foo", "bar"]);
    t.assert_text(&["getex", "foo", "PX", "150"], "bar");
    t.assert_text(&["getex", "foo"], "bar");
    advance(200);
    t.assert_null(&["getex", "foo"]);
}

#[test]
fn set_with_get_param() {
    let mut t = Ctx::new();
    t.assert_null(&["set", "key1", "val1", "get"]);
    t.assert_text(&["set", "key1", "val2", "get"], "val1");

    t.assert_null(&["set", "key2", "val2", "nx", "get"]);
    t.assert_text(&["set", "key2", "not used", "nx", "get"], "val2");
    t.assert_text(&["get", "key2"], "val2");

    t.assert_null(&["set", "key3", "not used", "xx", "get"]);
    t.assert_text(&["set", "key2", "val3", "xx", "get"], "val2");
    t.assert_text(&["get", "key2"], "val3");

    t.assert_int(&["sadd", "key4", "1"], 1);
    t.assert_err(&["set", "key4", "2", "get"], "WRONGTYPE");
    t.assert_err(&["set", "key4", "2", "xx", "get"], "WRONGTYPE");
}

#[test]
fn empty_keys() {
    let mut t = Ctx::new();
    t.assert_int(&["strlen", "foo"], 0);
    t.assert_text(&["SUBSTR", "foo", "0", "-1"], "");
}

#[test]
fn digest() {
    let mut t = Ctx::new();
    t.ok(&["set", "key", "value"]);
    t.assert_text(&["digest", "key"], "87d57e269b9df0f0");

    t.assert_null(&["digest", "nonexistent"]);

    t.ok(&["set", "key1", "testvalue"]);
    t.ok(&["set", "key2", "testvalue"]);
    assert_eq!(t.text(&["digest", "key1"]), t.text(&["digest", "key2"]));

    t.ok(&["set", "key3", "different"]);
    assert_ne!(t.text(&["digest", "key1"]), t.text(&["digest", "key3"]));

    t.ok(&["set", "intkey", "123"]);
    assert_eq!(t.bulk(&["digest", "intkey"]).len(), 16);

    t.ok(&["set", "empty", ""]);
    assert_eq!(t.bulk(&["digest", "empty"]).len(), 16);

    t.int(&["lpush", "list", "item"]);
    t.assert_err(&["digest", "list"], "WRONGTYPE");
}

#[test]
fn gat_via_redis_protocol() {
    let mut t = Ctx::new();
    t.ok(&["set", "key", "val"]);
    t.assert_err(&["GAT", "key"], "memcache-only");
}

#[test]
fn mset_nx_odd_args() {
    let mut t = Ctx::new();
    t.assert_err(
        &["msetnx", "key", "value", "key2"],
        "wrong number of arguments",
    );
    t.assert_err(
        &["mset", "key", "value", "key2"],
        "wrong number of arguments",
    );
}

#[test]
fn set_with_hashtags() {
    // The reference splits these into no-cluster / emulated-cluster / hashtag
    // lock modes; the observable RESP behavior is identical for all three.
    let mut t = Ctx::new();
    t.ok(&["set", "{key}1", "val1"]);
    t.ok(&["set", "{key}2", "val2"]);
    let a = t.arr(&["mget", "{key}1", "{key}2"]);
    assert_eq!(a.len(), 2);
    expect_text(&a[0], "val1");
    expect_text(&a[1], "val2");
}

#[test]
fn multi_set_with_hashtags() {
    let mut t = Ctx::new();
    t.ok(&["multi"]);
    t.assert_text(&["set", "{key}1", "val1"], "QUEUED");
    t.assert_text(&["set", "{key}2", "val2"], "QUEUED");
    t.assert_text(
        &[
            "eval",
            "return redis.call('set', KEYS[1], 'val3')",
            "1",
            "{key}3",
        ],
        "QUEUED",
    );
    let a = t.arr(&["exec"]);
    assert_eq!(a.len(), 3);
    expect_ok(&a[0]);
    expect_ok(&a[1]);
    expect_ok(&a[2]);
}

#[test]
fn get_large_raw() {
    // Large binary values round-trip intact (the reference asserts the
    // zero-copy borrow counter in INFO, which the port does not expose).
    let mut t = Ctx::new();
    let mut rng = 0xDF1FDF1Fu32;
    let mut rand = || {
        rng ^= rng << 13;
        rng ^= rng >> 17;
        rng ^= rng << 5;
        (rng & 0xff) as u8
    };
    for sz in [16384usize, 32768, 65536] {
        let value: Vec<u8> = (0..sz).map(|_| rand()).collect();
        let mut args = vec![b"set".to_vec(), b"k".to_vec(), value.clone()];
        args[0] = b"set".to_vec();
        let v = t.run_b(&args);
        expect_ok(&v);
        let got = t.run_b(&[b"get".to_vec(), b"k".to_vec()]);
        assert_eq!(got.bulk().unwrap(), value.as_slice(), "size {sz}");
    }
}

#[test]
fn get_large_raw_squashed() {
    let mut t = Ctx::new();
    let mut rng = 0xDF1FDF1Fu32;
    let mut rand = || {
        rng ^= rng << 13;
        rng ^= rng >> 17;
        rng ^= rng << 5;
        (rng & 0xff) as u8
    };
    let mut values = Vec::new();
    for sz in [16384usize, 32768, 65536] {
        values.push((0..sz).map(|_| rand()).collect::<Vec<u8>>());
    }
    for (i, v) in values.iter().enumerate() {
        let mut args = vec![b"set".to_vec(), format!("k{i}").into_bytes(), v.clone()];
        args[0] = b"set".to_vec();
        let r = t.run_b(&args);
        expect_ok(&r);
    }
    t.ok(&["multi"]);
    for i in 0..3 {
        t.assert_text(&["get", &format!("k{i}")], "QUEUED");
    }
    let a = t.arr(&["exec"]);
    assert_eq!(a.len(), 3);
    for (i, v) in values.iter().enumerate() {
        assert_eq!(a[i].bulk().unwrap(), v.as_slice(), "k{i}");
    }
}

#[test]
fn get_large_ascii_chunked() {
    let mut t = Ctx::new();
    let mut build = |sz: usize| -> Vec<u8> { (0..sz).map(|i| 0x20 + (i % 0x5F) as u8).collect() };
    for sz in [16384usize, 16391, 32768, 65535] {
        let value = build(sz);
        t.ok_b(&[b"set".to_vec(), b"k".to_vec(), value.clone()]);
        let got = t.run_b(&[b"get".to_vec(), b"k".to_vec()]);
        assert_eq!(got.bulk().unwrap(), value.as_slice(), "size {sz}");
    }
}

#[test]
fn get_large_ascii_chunked_squashed() {
    let mut t = Ctx::new();
    let mut build = |sz: usize| -> Vec<u8> { (0..sz).map(|i| 0x20 + (i % 0x5F) as u8).collect() };
    let v0 = build(16384);
    let v1 = build(32768);
    t.ok_b(&[b"set".to_vec(), b"k0".to_vec(), v0.clone()]);
    t.ok_b(&[b"set".to_vec(), b"k1".to_vec(), v1.clone()]);
    t.ok(&["multi"]);
    t.assert_text(&["get", "k0"], "QUEUED");
    t.assert_text(&["get", "k1"], "QUEUED");
    let a = t.arr(&["exec"]);
    assert_eq!(a.len(), 2);
    assert_eq!(a[0].bulk().unwrap(), v0.as_slice());
    assert_eq!(a[1].bulk().unwrap(), v1.as_slice());
}
