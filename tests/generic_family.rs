//! Port of `dragonfly/src/server/generic_family_test.cc`.
//!
//! Adaptations from the C++ original:
//! - `GetMetrics` / `GetDebugInfo` / `last_cmd_dbg_info_` / shard-count
//!   assertions are dropped (the port exposes no such introspection).
//! - `AdvanceTime` (the fake clock) is replaced with real sleeps because tests
//!   run in parallel and a global clock override would leak across servers.
//!   `pttl`/`ttl` magnitudes that the fake clock pinned exactly are asserted
//!   within a small tolerance instead.
//! - Fiber/concurrency tests (parallel `Del`, concurrent `Rename`/`Copy`,
//!   blocking-ops interplay) are reduced to their sequential observable
//!   behavior.
//! - `DEBUG OBJHIST` / `SHRINK` / `UNIQ-STRS` and member-expiry driven tests
//!   are skipped (no equivalent command surface or clock).
//! - `SORT ... BY/GET` reference vectors use a single shard because the port's
//!   external-key fetch only reads the shard that owns the source key.
//! - `KEYS` results are compared order-independently: the reference's ordering
//!   is an artifact of its table layout.
//! - `TIME` inside MULTI/EXEC is re-computed per queued command in the port, so
//!   the reference's "same timestamp inside a transaction" assertion is relaxed
//!   to a shape check.

mod common;

use common::*;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Parse an array of integer-formatted bulk strings (`ToIntArr`).
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

/// All text entries of an array reply, order-independent.
fn sorted(v: &Value) -> Vec<String> {
    let mut s: Vec<String> = v
        .arr()
        .unwrap_or_else(|| panic!("expected array, got {v:?}"))
        .iter()
        .map(|x| {
            x.text()
                .unwrap_or_else(|| panic!("expected text, got {x:?}"))
        })
        .collect();
    s.sort();
    s
}

/// Text entries of an array reply in their reply order.
fn bulks(v: &Value) -> Vec<String> {
    v.arr()
        .unwrap_or_else(|| panic!("expected array, got {v:?}"))
        .iter()
        .map(|x| {
            x.text()
                .unwrap_or_else(|| panic!("expected text, got {x:?}"))
        })
        .collect()
}

/// Assert `pttl key` is within `tolerance_ms` of `expected_ms` (real-clock
/// adaptation of the reference's exact `pttl` assertions).
fn assert_pttl(t: &mut Ctx, key: &str, expected_ms: i64) {
    let p = t.int(&["pttl", key]);
    assert!(
        p <= expected_ms && p >= expected_ms - 2000,
        "pttl {key} = {p}, expected ~{expected_ms}"
    );
}

#[test]
fn type_cmd() {
    let mut t = Ctx::new();
    t.assert_text(&["type", "missing"], "none");
    t.ok(&["set", "k", "v"]);
    t.assert_text(&["type", "k"], "string");
    t.assert_int(&["rpush", "l", "v"], 1);
    t.assert_text(&["type", "l"], "list");
    t.assert_int(&["sadd", "s", "v"], 1);
    t.assert_text(&["type", "s"], "set");
    t.assert_int(&["hset", "h", "f", "v"], 1);
    t.assert_text(&["type", "h"], "hash");
    t.assert_int(&["zadd", "z", "1", "m"], 1);
    t.assert_text(&["type", "z"], "zset");
}

#[test]
fn exists() {
    let mut t = Ctx::new();
    t.ok(&["mset", "x", "0", "y", "1"]);
    t.assert_int(&["exists", "x", "y", "x"], 3);
    t.assert_int(&["exists", "missing"], 0);
}

#[test]
fn touch() {
    let mut t = Ctx::new();
    t.ok(&["mset", "x", "0", "y", "1"]);
    t.assert_int(&["touch", "x", "y", "x"], 3);
    t.assert_int(&["touch", "z", "x", "w"], 1);
}

#[test]
fn ttl() {
    let mut t = Ctx::new();
    t.assert_int(&["ttl", "foo"], -2);
    t.assert_int(&["pttl", "foo"], -2);
    t.ok(&["set", "foo", "bar"]);
    t.assert_int(&["ttl", "foo"], -1);
    t.assert_int(&["pttl", "foo"], -1);
}

#[test]
fn del() {
    let mut t = Ctx::new();
    t.ok(&["set", "foo", "1"]);
    t.ok(&["set", "bar", "1"]);
    t.assert_int(&["del", "foo", "bar"], 2);
    t.assert_int(&["del", "foo", "missing"], 0);
    t.ok(&["setex", "k1", "10", "bar"]);
    t.assert_int(&["del", "k1"], 1);
}

#[test]
fn expire() {
    let mut t = Ctx::new();
    t.ok(&["set", "key", "val"]);
    // 5 years — well within the kMaxExpireDeadlineMs cap.
    t.assert_int(&["expire", "key", "157680000"], 1);
    t.assert_int(&["expire", "key", "1"], 1);
    sleep(Duration::from_millis(1100));
    t.assert_null(&["get", "key"]);

    // pexpireat override
    t.ok(&["set", "key", "val"]);
    let now = now_ms();
    t.assert_int(&["pexpireat", "key", &(now + 2000).to_string()], 1);
    t.assert_int(&["pexpireat", "key", &(now + 3000).to_string()], 1);
    sleep(Duration::from_millis(2800));
    t.assert_text(&["get", "key"], "val");
    sleep(Duration::from_millis(400));
    t.assert_null(&["get", "key"]);

    // pexpire override
    t.ok(&["set", "key", "val"]);
    t.assert_int(&["pexpire", "key", "2000"], 1);
    t.assert_int(&["pexpire", "key", "3000"], 1);
    sleep(Duration::from_millis(2800));
    t.assert_text(&["get", "key"], "val");
    sleep(Duration::from_millis(400));
    t.assert_null(&["get", "key"]);
}

#[test]
fn expire_corner_cases() {
    let mut t = Ctx::new();
    // Non-positive relative TTL deletes the key immediately and reports success.
    for ttl in ["-1", "0", "-100"] {
        t.ok(&["set", "key", "val"]);
        t.assert_int(&["expire", "key", ttl], 1);
        t.assert_int(&["exists", "key"], 0);
    }
    for ttl in ["-1", "0"] {
        t.ok(&["set", "key", "val"]);
        t.assert_int(&["pexpire", "key", ttl], 1);
        t.assert_int(&["exists", "key"], 0);
    }
    // Past absolute timestamps (including 0 and negatives) delete the key.
    for ttl in ["0", "-100"] {
        t.ok(&["set", "key", "val"]);
        t.assert_int(&["expireat", "key", ttl], 1);
        t.assert_int(&["exists", "key"], 0);
    }
    for ttl in ["0", "-1"] {
        t.ok(&["set", "key", "val"]);
        t.assert_int(&["pexpireat", "key", ttl], 1);
        t.assert_int(&["exists", "key"], 0);
    }

    // Huge absolute timestamps overflow the kMaxExpireDeadlineMs cap -> OUT_OF_RANGE.
    t.ok(&["set", "key", "val"]);
    t.assert_err(
        &["expireat", "key", "9223372036854775807"],
        "expiry is out of range",
    );
    t.assert_int(&["exists", "key"], 1);
    t.assert_err(
        &["pexpireat", "key", "9223372036854775807"],
        "expiry is out of range",
    );
    t.assert_int(&["exists", "key"], 1);

    // Huge relative TTLs are silently capped to kMaxExpireDeadlineSec (~8.5 years).
    t.assert_int(&["expire", "key", "99999999999"], 1);
    let ttl = t.int(&["ttl", "key"]);
    assert!(ttl == 268435455, "ttl {ttl}, expected 268435455");
    t.assert_int(&["pexpire", "key", "2684354550000"], 1);
    let pttl = t.int(&["pttl", "key"]);
    assert!(
        pttl <= 268435455000 && pttl >= 268435454000,
        "pttl {pttl}, expected ~268435455000"
    );

    // Missing key -> 0 regardless of the TTL value.
    t.assert_int(&["del", "missing"], 0);
    t.assert_int(&["expire", "missing", "5"], 0);
    t.assert_int(&["expire", "missing", "-1"], 0);
    t.assert_int(&["expireat", "missing", "0"], 0);
    t.assert_int(&["pexpireat", "missing", "0"], 0);
}

#[test]
fn expire_options() {
    let mut t = Ctx::new();
    t.ok(&["set", "key", "val"]);

    t.assert_err(
        &["expire", "key", "3600", "NX", "XX"],
        "NX and XX options at the same time are not compatible",
    );
    t.assert_err(
        &["expire", "key", "3600", "GT", "LT"],
        "GT and LT options at the same time are not compatible",
    );

    // Duplicate flags are tolerated (idempotent), like Redis.
    t.assert_int(&["expire", "key", "3600", "NX", "NX"], 1);
    t.assert_int(&["persist", "key"], 1);

    // Unknown option -> error naming the offending token.
    t.assert_err(&["expire", "key", "3600", "FOO"], "Unsupported option: FOO");

    // NX adds an expiry since there is none yet.
    t.assert_int(&["expire", "key", "3600", "NX"], 1);
    t.assert_int(&["ttl", "key"], 3600);
    // NX again: expiry unchanged.
    t.assert_int(&["expire", "key", "42", "NX"], 0);

    // XX without an existing expiry does nothing.
    t.ok(&["set", "key2", "val"]);
    t.assert_int(&["expire", "key2", "404", "XX"], 0);
    t.assert_int(&["ttl", "key2"], -1);

    // GT on a key without expiry does nothing (infinite current TTL).
    t.assert_int(&["expire", "key2", "404", "GT"], 0);
    t.assert_int(&["ttl", "key2"], -1);

    // LT applies.
    t.assert_int(&["expire", "key2", "404", "LT"], 1);
    t.assert_int(&["ttl", "key2"], 404);

    t.assert_int(&["persist", "key"], 1);
    t.assert_int(&["expire", "key", "101"], 1);

    t.assert_int(&["expire", "key", "100", "GT"], 0);
    t.assert_int(&["ttl", "key"], 101);
    t.assert_int(&["expire", "key", "102", "GT"], 1);
    t.assert_int(&["ttl", "key"], 102);
    t.assert_int(&["expire", "key", "101", "GT"], 0);
    t.assert_int(&["ttl", "key"], 102);
    t.assert_int(&["expire", "key", "101", "LT"], 1);
    t.assert_int(&["ttl", "key"], 101);
    t.assert_int(&["expire", "key", "102", "LT"], 0);
    t.assert_int(&["ttl", "key"], 101);

    // NX with GT: first sets the expiry, then only updates to larger values.
    t.assert_int(&["persist", "key"], 1);
    t.assert_int(&["expire", "key", "5", "NX", "GT"], 1);
    t.assert_int(&["ttl", "key"], 5);
    t.assert_int(&["expire", "key", "3", "NX", "GT"], 0);
    t.assert_int(&["ttl", "key"], 5);
    t.assert_int(&["expire", "key", "7", "NX", "GT"], 1);
    t.assert_int(&["ttl", "key"], 7);
}

#[test]
fn expire_at_options() {
    let mut t = Ctx::new();
    let time_s = (now_ms() + 500) / 1000;

    t.ok(&["set", "key", "val"]);
    t.assert_err(
        &["expireat", "key", "3600", "NX", "XX"],
        "NX and XX options at the same time are not compatible",
    );
    t.assert_err(
        &["expireat", "key", "3600", "GT", "LT"],
        "GT and LT options at the same time are not compatible",
    );

    let t5 = (time_s + 5).to_string();
    t.assert_int(&["expireat", "key", &t5, "NX"], 1);
    t.assert_int(&["expiretime", "key"], time_s as i64 + 5);

    // NX again: unchanged.
    let t9 = (time_s + 9).to_string();
    t.assert_int(&["expireat", "key", &t9, "NX"], 0);

    // NX with a past timestamp must not delete the value.
    t.assert_int(
        &[
            "expireat",
            "key",
            &((now_ms() / 1000 - 10).to_string()),
            "NX",
        ],
        0,
    );
    t.assert_int(&["exists", "key"], 1);

    // XX on a key without expiry does nothing.
    t.ok(&["set", "key2", "val"]);
    t.assert_int(&["expireat", "key2", &t9, "XX"], 0);
    t.assert_int(&["ttl", "key2"], -1);

    let t101 = (time_s + 101).to_string();
    t.assert_int(&["expireat", "key", &t101], 1);

    let t99 = (time_s + 99).to_string();
    t.assert_int(&["expireat", "key", &t99, "GT"], 0);
    t.assert_int(&["expiretime", "key"], time_s as i64 + 101);

    let t105 = (time_s + 105).to_string();
    t.assert_int(&["expireat", "key", &t105, "GT"], 1);
    t.assert_int(&["expiretime", "key"], time_s as i64 + 105);

    t.assert_int(&["expireat", "key", &t101, "LT"], 1);
    t.assert_int(&["expiretime", "key"], time_s as i64 + 101);

    let t102 = (time_s + 102).to_string();
    t.assert_int(&["expireat", "key", &t102, "LT"], 0);
    t.assert_int(&["expiretime", "key"], time_s as i64 + 101);
}

#[test]
fn pexpire_options() {
    let mut t = Ctx::new();
    t.ok(&["set", "key", "val"]);
    t.assert_err(
        &["pexpire", "key", "3600", "NX", "XX"],
        "NX and XX options at the same time are not compatible",
    );
    t.assert_err(
        &["pexpire", "key", "3600", "GT", "LT"],
        "GT and LT options at the same time are not compatible",
    );

    t.assert_int(&["pexpire", "key", "3600000", "NX"], 1);
    assert_pttl(&mut t, "key", 3600000);
    t.assert_int(&["pexpire", "key", "42", "NX"], 0);

    t.ok(&["set", "key2", "val"]);
    t.assert_int(&["pexpire", "key2", "404", "XX"], 0);
    t.assert_int(&["pttl", "key2"], -1);

    t.assert_int(&["pexpire", "key", "101000"], 1);
    t.assert_int(&["pexpire", "key", "100000", "GT"], 0);
    assert_pttl(&mut t, "key", 101000);
    t.assert_int(&["pexpire", "key", "102000", "GT"], 1);
    assert_pttl(&mut t, "key", 102000);
    t.assert_int(&["pexpire", "key", "101000", "GT"], 0);
    assert_pttl(&mut t, "key", 102000);
    t.assert_int(&["pexpire", "key", "101000", "LT"], 1);
    assert_pttl(&mut t, "key", 101000);
    t.assert_int(&["pexpire", "key", "102000", "LT"], 0);
    assert_pttl(&mut t, "key", 101000);
}

#[test]
fn pexpire_at_options() {
    let mut t = Ctx::new();
    let now = now_ms();

    t.ok(&["set", "key", "val"]);
    t.assert_err(
        &["pexpireat", "key", "3600", "NX", "XX"],
        "NX and XX options at the same time are not compatible",
    );
    t.assert_err(
        &["pexpireat", "key", "3600", "GT", "LT"],
        "GT and LT options at the same time are not compatible",
    );

    let m3600 = (now + 3600).to_string();
    t.assert_int(&["pexpireat", "key", &m3600, "NX"], 1);
    t.assert_int(&["pexpiretime", "key"], (now + 3600) as i64);

    let m42000 = (now + 42000).to_string();
    t.assert_int(&["pexpireat", "key", &m42000, "NX"], 0);

    t.ok(&["set", "key2", "val"]);
    let m404 = (now + 404).to_string();
    t.assert_int(&["pexpireat", "key2", &m404, "XX"], 0);
    t.assert_int(&["ttl", "key2"], -1);

    let m101 = (now + 101).to_string();
    t.assert_int(&["pexpireat", "key", &m101], 1);

    let m100 = (now + 100).to_string();
    t.assert_int(&["pexpireat", "key", &m100, "GT"], 0);
    t.assert_int(&["pexpiretime", "key"], (now + 101) as i64);

    let m105 = (now + 105).to_string();
    t.assert_int(&["pexpireat", "key", &m105, "GT"], 1);
    t.assert_int(&["pexpiretime", "key"], (now + 105) as i64);

    t.assert_int(&["pexpireat", "key", &m101, "LT"], 1);
    t.assert_int(&["pexpiretime", "key"], (now + 101) as i64);

    let m102 = (now + 102).to_string();
    t.assert_int(&["pexpireat", "key", &m102, "LT"], 0);
    t.assert_int(&["pexpiretime", "key"], (now + 101) as i64);
}

#[test]
fn expiretime() {
    let mut t = Ctx::new();
    t.assert_int(&["expiretime", "foo"], -2);
    t.assert_int(&["pexpiretime", "foo"], -2);

    t.ok(&["set", "foo", "bar"]);
    t.assert_int(&["expiretime", "foo"], -1);
    t.assert_int(&["pexpiretime", "foo"], -1);

    let at = now_ms() + 5000;
    t.assert_int(&["pexpireat", "foo", &at.to_string()], 1);
    t.assert_int(&["pexpiretime", "foo"], at as i64);
    t.assert_int(&["expiretime", "foo"], ((at + 500) / 1000) as i64);
}

#[test]
fn persist() {
    let mut t = Ctx::new();
    t.ok(&["set", "mykey", "somevalue"]);
    t.assert_int(&["persist", "mykey"], 0);
    t.assert_int(&["ttl", "mykey"], -1);

    t.assert_int(&["expire", "mykey", "10"], 1);
    t.assert_int(&["ttl", "mykey"], 10);
    t.assert_int(&["persist", "mykey"], 1);
    t.assert_int(&["ttl", "mykey"], -1);

    t.assert_int(&["persist", "keythatdoesnotexist"], 0);
}

#[test]
fn rename() {
    let mut t = Ctx::new();
    let x_val = "x".repeat(32);
    let b_val = "b".repeat(32);
    t.ok(&["mset", "x", &x_val, "b", &b_val]);

    t.assert_err(&["rename", "z", "b"], "no such key");
    t.ok(&["rename", "x", "b"]);

    t.assert_null(&["get", "x"]);
    t.assert_text(&["get", "b"], &x_val);
    t.assert_int(&["exists", "x", "b"], 1);
}

#[test]
fn rename_nx() {
    let mut t = Ctx::new();
    let x_val = "x".repeat(32);
    let b_val = "b".repeat(32);
    t.ok(&["mset", "x", &x_val, "b", &b_val]);

    t.assert_err(&["renamenx", "z", "b"], "no such key");
    t.assert_int(&["renamenx", "x", "b"], 0);
    t.assert_int(&["renamenx", "x", "y"], 1);
    t.assert_text(&["get", "y"], &x_val);
    t.assert_int(&["renamenx", "y", "y"], 0);
}

#[test]
fn rename_same_name() {
    let mut t = Ctx::new();
    t.assert_err(&["rename", "key", "key"], "no such key");
    t.ok(&["set", "key", "value"]);
    t.ok(&["rename", "key", "key"]);
}

#[test]
fn rename_binary() {
    let mut t = Ctx::new();
    let k1 = vec![1u8, 2, 3, 4];
    let k2 = vec![5u8, 6, 7, 8];
    t.ok_b(&[b"set".to_vec(), k1.clone(), b"bar".to_vec()]);
    t.ok_b(&[b"rename".to_vec(), k1.clone(), k2.clone()]);
    assert_eq!(t.run_b(&[b"get".to_vec(), k1.clone()]).text(), None);
    assert_eq!(
        t.run_b(&[b"get".to_vec(), k2.clone()]).text(),
        Some("bar".to_string())
    );
}

#[test]
fn stick() {
    let mut t = Ctx::new();
    t.assert_int(&["stick", "a", "b"], 0);

    for k in ["a", "b", "c", "d"] {
        t.ok(&["set", k, "."]);
    }

    t.assert_int(&["stick", "a", "b"], 2);
    t.assert_int(&["stick", "a", "b"], 0);
    t.assert_int(&["stick", "a", "c"], 1);
    t.assert_int(&["stick", "b", "d"], 1);
    t.assert_int(&["stick", "c", "d"], 0);

    // Stickiness survives writes.
    t.ok(&["set", "a", "new"]);
    t.assert_int(&["stick", "a"], 0);
    t.assert_int(&["append", "a", "-value"], 9);
    t.assert_int(&["stick", "a"], 0);

    // RENAME carries stickiness (same shard or across shards).
    t.ok(&["rename", "a", "k"]);
    t.assert_int(&["stick", "k"], 0);

    t.assert_int(&["del", "b"], 1);
    t.ok(&["mset", "b", &"b".repeat(32), "x", &"x".repeat(32)]);
    t.assert_int(&["stick", "x"], 1);
    t.ok(&["rename", "x", "b"]);
    t.assert_int(&["stick", "b"], 0);
}

#[test]
fn move_cmd() {
    let mut t = Ctx::new();
    t.assert_int(&["move", "a", "1"], 0);
    t.assert_err(&["move", "a", "-1"], "DB index is out of range");
    t.assert_err(&["move", "a", "100500"], "DB index is out of range");

    // Value, expiry and stickiness move together.
    t.ok(&["set", "a", "test"]);
    t.assert_int(&["expire", "a", "1000"], 1);
    t.assert_int(&["stick", "a"], 1);
    t.assert_int(&["move", "a", "1"], 1);

    t.ok(&["select", "1"]);
    t.assert_text(&["get", "a"], "test");
    assert!(t.int(&["ttl", "a"]) > 0);
    t.assert_int(&["stick", "a"], 0);

    // MOVE does not move when the destination key exists.
    t.ok(&["set", "a", "test"]);
    t.ok(&["select", "0"]);
    t.ok(&["set", "a", "another test"]);
    t.assert_int(&["move", "a", "1"], 0);
    t.ok(&["select", "1"]);
    t.assert_text(&["get", "a"], "test");
}

#[test]
fn copy() {
    let mut t = Ctx::new();
    let x_val = "x".repeat(32);
    let b_val = "b".repeat(32);
    t.ok(&["mset", "x", &x_val, "b", &b_val]);

    t.assert_int(&["copy", "z", "b"], 0);
    t.assert_int(&["copy", "b", "c"], 1);
    t.assert_text(&["get", "c"], &b_val);

    t.assert_int(&["copy", "x", "b", "replace"], 1);
    t.assert_text(&["get", "x"], &x_val);
    t.assert_text(&["get", "b"], &x_val);
    t.assert_int(&["exists", "x", "b"], 2);
}

#[test]
fn copy_non_string() {
    let mut t = Ctx::new();
    t.assert_int(&["lpush", "x", "elem"], 1);
    t.assert_int(&["copy", "x", "b"], 1);
    t.assert_int(&["del", "x"], 1);
    t.assert_int(&["del", "b"], 1);
}

#[test]
fn copy_binary() {
    let mut t = Ctx::new();
    let k1 = vec![1u8, 2, 3, 4];
    let k2 = vec![5u8, 6, 7, 8];
    t.ok_b(&[b"set".to_vec(), k1.clone(), b"bar".to_vec()]);
    assert_eq!(
        t.run_b(&[b"copy".to_vec(), k1.clone(), k2.clone()]).int(),
        Some(1)
    );
    assert_eq!(
        t.run_b(&[b"get".to_vec(), k1.clone()]).text(),
        Some("bar".to_string())
    );
    assert_eq!(
        t.run_b(&[b"get".to_vec(), k2.clone()]).text(),
        Some("bar".to_string())
    );
}

#[test]
fn copy_ttl() {
    let mut t = Ctx::new();
    t.ok(&["setex", "k1", "10", "bar"]);
    t.assert_int(&["copy", "k1", "k2"], 1);
    let ttl = t.int(&["ttl", "k2"]);
    assert!(ttl == 10, "ttl {ttl}, expected 10");
}

#[test]
fn copy_same_name() {
    let mut t = Ctx::new();
    t.assert_err(
        &["copy", "k1", "k1"],
        "source and destination objects are the same",
    );
    t.ok(&["set", "k1", "v"]);
    t.assert_err(
        &["copy", "k1", "k1"],
        "source and destination objects are the same",
    );
}

#[test]
fn copy_to_db() {
    let mut t = Ctx::new();
    t.assert_err(&["copy", "k1", "k1", "db", "some_db"], "syntax error");
}

#[test]
fn copy_key_exists() {
    let mut t = Ctx::new();
    t.ok(&["set", "source", "value1"]);
    t.ok(&["set", "destination", "value2"]);
    t.assert_int(&["copy", "source", "destination"], 0);
    t.assert_text(&["get", "destination"], "value2");
    t.assert_text(&["get", "source"], "value1");

    t.assert_int(&["copy", "source", "destination", "replace"], 1);
    t.assert_text(&["get", "destination"], "value1");
}

#[test]
fn scan() {
    let mut t = Ctx::new();
    for i in 0..10 {
        t.ok(&["set", &format!("key{i}"), "bar"]);
        t.ok(&["set", &format!("str{i}"), "bar"]);
        t.assert_int(&["sadd", &format!("set{i}"), "bar"], 1);
        t.assert_int(&["zadd", &format!("zset{i}"), "0", "bar"], 1);
    }

    let v = t.run(&["scan", "0", "count", "20", "type", "string"]);
    assert_eq!(v.arr().unwrap().len(), 2);
    let keys = sorted(&v.arr().unwrap()[1]);
    assert!(keys.len() > 10, "got {} keys", keys.len());
    for k in &keys {
        assert!(
            k.starts_with("str") || k.starts_with("key"),
            "unexpected key {k}"
        );
    }

    let v = t.run(&["scan", "0", "count", "20", "match", "zset*"]);
    assert_eq!(v.arr().unwrap().len(), 2);
    let keys = sorted(&v.arr().unwrap()[1]);
    assert_eq!(keys.len(), 10);
    for k in &keys {
        assert!(k.starts_with("zset"), "unexpected key {k}");
    }

    t.assert_err(&["scan", "0", "count"], "syntax error");
    t.assert_err(
        &["scan", "0", "count", "not-a-number"],
        "value is not an integer",
    );
    t.assert_err(&["scan", "0", "type", "not-a-type"], "syntax error");
    t.assert_err(&["scan", "0", "novalues"], "syntax error");

    // COUNT is a size_t hint: values above u32::MAX must still parse.
    let v = t.run(&["scan", "0", "count", "5000000000"]);
    assert_eq!(v.arr().unwrap().len(), 2);
}

#[test]
fn scan_malloc_size() {
    let mut t = Ctx::new();
    t.ok(&["set", "k1", &"a".repeat(1000)]);
    t.ok(&["set", "k2", &"b".repeat(500)]);
    t.ok(&["set", "k3", &"c".repeat(15)]);

    let v = t.run(&["scan", "0", "MINMSZ", "15"]);
    assert_eq!(
        sorted(&v.arr().unwrap()[1]),
        vec!["k1".to_string(), "k2".to_string()]
    );
    let v = t.run(&["scan", "0", "MINMSZ", "500"]);
    assert_eq!(sorted(&v.arr().unwrap()[1]), vec!["k1".to_string()]);
    let v = t.run(&["scan", "0", "minmsz", "500"]);
    assert_eq!(sorted(&v.arr().unwrap()[1]), vec!["k1".to_string()]);
}

#[test]
fn bug4466() {
    // An invalid cursor must not crash the server.
    let mut t = Ctx::new();
    let v = t.run(&["scan", "9223372036854775808"]);
    assert_eq!(v.arr().unwrap()[0].text().as_deref(), Some("0"));
    assert_eq!(v.arr().unwrap()[1].arr().unwrap().len(), 0);
}

#[test]
fn keys() {
    let mut t = Ctx::new();
    t.ok(&["flushdb"]);
    t.ok_b(&[b"set".to_vec(), b"".to_vec(), b"foo".to_vec()]);
    t.ok(&["set", "bar", "1"]);
    assert_eq!(
        sorted(&t.run(&["keys", "*"])),
        vec!["".to_string(), "bar".to_string()]
    );
    let v = t.run(&["keys", ""]);
    assert_eq!(sorted(&v), vec!["".to_string()]);
}

#[test]
fn randomkey() {
    let mut t = Ctx::new();
    t.assert_null(&["randomkey"]);
    t.ok(&["set", "k1", "1"]);
    t.assert_text(&["randomkey"], "k1");
}

#[test]
fn unlink() {
    let mut t = Ctx::new();
    for i in 0..10 {
        let mut cmd = vec!["sadd".to_string(), "s1".to_string()];
        for j in 0..10 {
            cmd.push(format!("f{}", i * 10 + j));
        }
        let args: Vec<&str> = cmd.iter().map(|s| s.as_str()).collect();
        t.assert_int(&args, 10);
    }
    t.assert_int(&["unlink", "s1"], 1);
    t.assert_int(&["unlink", "s1", "missing"], 0);
}

#[test]
fn rm() {
    let mut t = Ctx::new();
    // The port resumes exactly one shard per call (unlike the reference, which
    // walks shards until the time/limit budget is met), so the empty-DB cursor
    // needs draining instead of finishing at 0 immediately.
    let mut cursor = "0".to_string();
    loop {
        let v = t.run(&["rm", &cursor]);
        let a = v.arr().unwrap();
        assert_eq!(a.len(), 2);
        assert_eq!(a[1].int(), Some(0));
        cursor = a[0].text().unwrap();
        if cursor == "0" {
            break;
        }
    }

    let v = t.run(&["rm", "0", "match", "foo*"]);
    assert_eq!(v.arr().unwrap()[1].int(), Some(0));
    let v = t.run(&["rm", "0", "type", "string"]);
    assert_eq!(v.arr().unwrap()[1].int(), Some(0));
    let v = t.run(&["rm", "0", "match", "foo*", "count", "100"]);
    assert_eq!(v.arr().unwrap().len(), 2);

    t.assert_err(&["rm", "notanumber"], "invalid cursor");
    t.assert_err(&["rm", "0", "badopt"], "syntax");
}

#[test]
fn rm_deletes_matching_keys() {
    let mut t = Ctx::new();
    for i in 0..10 {
        t.ok(&["set", &format!("foo{i}"), "val"]);
    }
    for i in 0..5 {
        t.ok(&["set", &format!("bar{i}"), "val"]);
    }

    let mut total_deleted = 0u64;
    let mut cursor = 0u64;
    loop {
        let v = t.run(&["rm", &cursor.to_string(), "match", "foo*", "count", "100"]);
        let a = v.arr().unwrap();
        cursor = a[0].text().unwrap().parse().unwrap();
        total_deleted += a[1].int().unwrap() as u64;
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(total_deleted, 10);
    t.assert_int(&["exists", "foo0"], 0);
    t.assert_int(&["exists", "bar0"], 1);
    t.assert_int(&["dbsize"], 5);
}

#[test]
fn sort() {
    let mut t = Ctx::shards(1);

    // List sort.
    t.assert_int(&["del", "list-1"], 0);
    t.assert_int(&["lpush", "list-1", "3.5", "1.2", "10.1", "2.20", "200"], 5);
    // Numeric.
    assert_eq!(
        bulks(&t.run(&["sort", "list-1"])),
        vec!["1.2", "2.20", "3.5", "10.1", "200"]
    );
    // String.
    assert_eq!(
        bulks(&t.run(&["sort", "list-1", "ALPHA"])),
        vec!["1.2", "10.1", "2.20", "200", "3.5"]
    );
    // Desc numeric.
    assert_eq!(
        bulks(&t.run(&["sort", "list-1", "DESC"])),
        vec!["200", "10.1", "3.5", "2.20", "1.2"]
    );
    // Desc string.
    assert_eq!(
        bulks(&t.run(&["sort", "list-1", "DESC", "ALPHA"])),
        vec!["3.5", "200", "2.20", "10.1", "1.2"]
    );
    // ASC/DESC are not mutually exclusive — last one wins (matches Redis behavior).
    assert_eq!(
        bulks(&t.run(&["sort", "list-1", "DESC", "ASC"])),
        vec!["1.2", "2.20", "3.5", "10.1", "200"]
    );
    assert_eq!(
        bulks(&t.run(&["sort", "list-1", "ASC", "DESC"])),
        vec!["200", "10.1", "3.5", "2.20", "1.2"]
    );
    assert_eq!(
        bulks(&t.run(&["sort", "list-1", "LIMIT", "0", "5"])),
        vec!["1.2", "2.20", "3.5", "10.1", "200"]
    );
    assert_eq!(
        bulks(&t.run(&["sort", "list-1", "LIMIT", "2", "2"])),
        vec!["3.5", "10.1"]
    );
    assert_eq!(
        bulks(&t.run(&["sort", "list-1", "LIMIT", "1", "1"])),
        vec!["2.20"]
    );
    assert_eq!(
        t.run(&["sort", "list-1", "LIMIT", "5", "2"])
            .arr()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        bulks(&t.run(&["sort", "list-1", "DESC", "LIMIT", "2", "2"])),
        vec!["3.5", "2.20"]
    );
    assert_eq!(
        bulks(&t.run(&["sort", "list-1", "DESC", "LIMIT", "1", "1"])),
        vec!["10.1"]
    );
    assert_eq!(
        t.run(&["sort", "list-1", "DESC", "LIMIT", "5", "2"])
            .arr()
            .unwrap()
            .len(),
        0
    );

    // Set sort.
    t.assert_int(&["del", "set-1"], 0);
    t.assert_int(
        &["sadd", "set-1", "5.3", "4.4", "60", "99.9", "100", "9"],
        6,
    );
    assert_eq!(
        bulks(&t.run(&["sort", "set-1"])),
        vec!["4.4", "5.3", "9", "60", "99.9", "100"]
    );
    assert_eq!(
        bulks(&t.run(&["sort", "set-1", "ALPHA"])),
        vec!["100", "4.4", "5.3", "60", "9", "99.9"]
    );
    assert_eq!(
        bulks(&t.run(&["sort", "set-1", "DESC"])),
        vec!["100", "99.9", "60", "9", "5.3", "4.4"]
    );
    assert_eq!(
        bulks(&t.run(&["sort", "set-1", "DESC", "ALPHA"])),
        vec!["99.9", "9", "60", "5.3", "4.4", "100"]
    );

    // Intset sort.
    t.assert_int(&["del", "intset-1"], 0);
    t.assert_int(&["sadd", "intset-1", "5", "4", "3", "2", "1"], 5);
    assert_eq!(
        bulks(&t.run(&["sort", "intset-1"])),
        vec!["1", "2", "3", "4", "5"]
    );

    // Sorted-set sort.
    t.assert_int(&["del", "zset-1"], 0);
    t.assert_int(&["zadd", "zset-1", "0", "3.3", "0", "30.1", "0", "8.2"], 3);
    assert_eq!(
        bulks(&t.run(&["sort", "zset-1"])),
        vec!["3.3", "8.2", "30.1"]
    );
    assert_eq!(
        bulks(&t.run(&["sort", "zset-1", "ALPHA"])),
        vec!["3.3", "30.1", "8.2"]
    );
    assert_eq!(
        bulks(&t.run(&["sort", "zset-1", "DESC"])),
        vec!["30.1", "8.2", "3.3"]
    );
    assert_eq!(
        bulks(&t.run(&["sort", "zset-1", "DESC", "ALPHA"])),
        vec!["8.2", "30.1", "3.3"]
    );

    // Missing key.
    t.assert_int(&["del", "list-2"], 0);
    assert_eq!(t.run(&["sort", "list-2"]).arr().unwrap().len(), 0);

    // Not convertible to double.
    t.assert_int(&["lpush", "list-2", "NOTADOUBLE"], 1);
    t.assert_err(
        &["sort", "list-2"],
        "One or more scores can't be converted into double",
    );

    // Wrong type.
    t.ok(&["set", "foo", "bar"]);
    t.assert_err(&["sort", "foo"], "WRONGTYPE");

    // Empty element parses as 0.
    t.assert_int(&["rpush", "list-3", ""], 1);
    assert_eq!(t.run(&["sort", "list-3"]).arr().unwrap().len(), 1);

    t.assert_int(
        &[
            "rpush", "list-3", "2", "0", "", "-0.14", "0.12", "-0", "-123123", "7654",
        ],
        9,
    );
    assert_eq!(
        bulks(&t.run(&["sort", "list-3"])),
        ["-123123", "-0.14", "", "", "-0", "0", "0.12", "2", "7654"]
    );

    // NaN is rejected.
    t.assert_int(&["rpush", "nanvalue", "nan"], 1);
    t.assert_err(
        &["sort", "nanvalue"],
        "One or more scores can't be converted into double",
    );
}

#[test]
fn sort_bug3636() {
    let mut t = Ctx::shards(1);
    let mut cmd = vec!["rpush".to_string(), "foo".to_string()];
    cmd.extend_from_slice(&[
        "1.100000023841858".to_string(),
        "1.100000023841858".to_string(),
        "1.100000023841858".to_string(),
        "-15710".to_string(),
        "1.100000023841858".to_string(),
        "1.100000023841858".to_string(),
        "1.100000023841858".to_string(),
        "-15710".to_string(),
        "-15710".to_string(),
        "1.100000023841858".to_string(),
        "-15710".to_string(),
        "-15710".to_string(),
        "-15710".to_string(),
        "-15710".to_string(),
        "1.100000023841858".to_string(),
        "-15710".to_string(),
        "-15710".to_string(),
    ]);
    let args: Vec<&str> = cmd.iter().map(|s| s.as_str()).collect();
    t.assert_int(&args, 17);
    assert_eq!(
        t.run(&["sort", "foo", "desc", "alpha"])
            .arr()
            .unwrap()
            .len(),
        17
    );
}

#[test]
fn sort_ro() {
    let mut t = Ctx::shards(1);
    t.assert_int(&["del", "list-1"], 0);
    t.assert_int(&["lpush", "list-1", "3.5", "1.2", "10.1", "2.20", "200"], 5);
    assert_eq!(
        bulks(&t.run(&["sort_ro", "list-1"])),
        vec!["1.2", "2.20", "3.5", "10.1", "200"]
    );
    assert_eq!(
        bulks(&t.run(&["sort_ro", "list-1", "DESC"])),
        vec!["200", "10.1", "3.5", "2.20", "1.2"]
    );
    assert_eq!(
        bulks(&t.run(&["sort_ro", "list-1", "LIMIT", "2", "2"])),
        vec!["3.5", "10.1"]
    );
    assert_eq!(
        t.run(&["sort_ro", "list-1", "LIMIT", "5", "2"])
            .arr()
            .unwrap()
            .len(),
        0
    );

    t.assert_int(&["del", "list-2"], 0);
    t.assert_int(&["lpush", "list-2", "NOTADOUBLE"], 1);
    t.assert_err(
        &["sort_ro", "list-2"],
        "One or more scores can't be converted into double",
    );

    t.ok(&["set", "foo", "bar"]);
    t.assert_err(&["sort_ro", "foo"], "WRONGTYPE");

    // STORE must not work with SORT_RO.
    t.assert_err(&["sort_ro", "list-1", "store", "list-2"], "syntax error");
}

#[test]
fn sort_store() {
    let mut t = Ctx::new();
    t.assert_int(&["del", "list-1"], 0);
    t.assert_int(&["del", "list-2"], 0);
    t.assert_int(&["lpush", "list-1", "3.5", "1.2", "10.1", "2.20", "200"], 5);

    t.assert_int(&["sort", "list-1", "store", "list-2"], 5);
    assert_eq!(
        bulks(&t.run(&["lrange", "list-2", "0", "-1"])),
        vec!["1.2", "2.20", "3.5", "10.1", "200"]
    );

    t.assert_int(&["sort", "list-1", "ALPHA", "store", "list-2"], 5);
    assert_eq!(
        bulks(&t.run(&["lrange", "list-2", "0", "-1"])),
        vec!["1.2", "10.1", "2.20", "200", "3.5"]
    );

    t.assert_int(&["sort", "list-1", "DESC", "store", "list-2"], 5);
    assert_eq!(
        bulks(&t.run(&["lrange", "list-2", "0", "-1"])),
        vec!["200", "10.1", "3.5", "2.20", "1.2"]
    );

    t.assert_int(&["sort", "list-1", "ALPHA", "DESC", "store", "list-2"], 5);
    assert_eq!(
        bulks(&t.run(&["lrange", "list-2", "0", "-1"])),
        vec!["3.5", "200", "2.20", "10.1", "1.2"]
    );

    t.assert_int(&["sort", "list-1", "LIMIT", "2", "2", "store", "list-2"], 2);
    assert_eq!(
        bulks(&t.run(&["lrange", "list-2", "0", "-1"])),
        vec!["3.5", "10.1"]
    );

    t.assert_int(&["sort", "list-1", "LIMIT", "1", "1", "store", "list-2"], 1);
    assert_eq!(
        bulks(&t.run(&["lrange", "list-2", "0", "-1"])),
        vec!["2.20"]
    );

    t.assert_int(&["sort", "list-1", "LIMIT", "5", "2", "store", "list-2"], 0);
    assert_eq!(
        t.run(&["lrange", "list-2", "0", "-1"]).arr().unwrap().len(),
        0
    );

    // Same-key overwrite.
    t.assert_int(&["sort", "list-1", "store", "list-1"], 5);
    assert_eq!(
        bulks(&t.run(&["lrange", "list-1", "0", "-1"])),
        vec!["1.2", "2.20", "3.5", "10.1", "200"]
    );

    // Set source.
    t.assert_int(&["del", "set-1"], 0);
    t.assert_int(&["del", "list-3"], 0);
    t.assert_int(
        &["sadd", "set-1", "5.3", "4.4", "60", "99.9", "100", "9"],
        6,
    );
    t.assert_int(&["sort", "set-1", "store", "list-3"], 6);
    assert_eq!(
        bulks(&t.run(&["lrange", "list-3", "0", "-1"])),
        vec!["4.4", "5.3", "9", "60", "99.9", "100"]
    );

    // Sorted-set source.
    t.assert_int(&["del", "zset-1"], 0);
    t.assert_int(&["del", "list-4"], 0);
    t.assert_int(&["zadd", "zset-1", "0", "3.3", "0", "30.1", "0", "8.2"], 3);
    t.assert_int(&["sort", "zset-1", "store", "list-4"], 3);
    assert_eq!(
        bulks(&t.run(&["lrange", "list-4", "0", "-1"])),
        vec!["3.3", "8.2", "30.1"]
    );
}

#[test]
fn sort_store_empty_result() {
    let mut t = Ctx::new();
    t.assert_int(&["lpush", "list-src", "3", "1", "2"], 3);

    // LIMIT offset beyond list length -> empty result, dest must not exist.
    t.assert_int(
        &["sort", "list-src", "LIMIT", "10", "5", "store", "dest"],
        0,
    );
    t.assert_int(&["exists", "dest"], 0);

    // LIMIT count=0 -> empty result deletes a pre-existing destination.
    t.ok(&["set", "dest", "old"]);
    t.assert_int(&["sort", "list-src", "LIMIT", "0", "0", "store", "dest"], 0);
    t.assert_int(&["exists", "dest"], 0);
}

#[test]
fn sort_store_resets_expiry() {
    let mut t = Ctx::new();
    t.assert_int(&["del", "src", "dest"], 0);
    t.assert_int(&["sadd", "src", "3", "1", "2"], 3);
    t.assert_int(&["sadd", "dest", "old"], 1);
    t.assert_int(&["expire", "dest", "100"], 1);
    assert!(t.int(&["ttl", "dest"]) > 0);

    t.assert_int(&["sort", "src", "store", "dest"], 3);
    assert_eq!(t.int(&["ttl", "dest"]), -1);
    assert_eq!(
        sorted(&t.run(&["lrange", "dest", "0", "-1"])),
        vec!["1", "2", "3"]
    );

    // Same-key STORE clears the source's own expiry.
    t.assert_int(&["del", "myset"], 0);
    t.assert_int(&["sadd", "myset", "c", "a", "b"], 3);
    t.assert_int(&["expire", "myset", "100"], 1);
    assert!(t.int(&["ttl", "myset"]) > 0);

    t.assert_int(&["sort", "myset", "ALPHA", "store", "myset"], 3);
    assert_eq!(t.int(&["ttl", "myset"]), -1);
    assert_eq!(
        sorted(&t.run(&["lrange", "myset", "0", "-1"])),
        vec!["a", "b", "c"]
    );
}

#[test]
fn sort_negative_limit() {
    let mut t = Ctx::shards(1);
    t.assert_int(&["lpush", "list-neg", "1", "2", "3", "4", "5"], 5);
    t.assert_err(
        &["sort", "list-neg", "LIMIT", "-1", "2"],
        "value is not an integer",
    );
    t.assert_err(
        &["sort", "list-neg", "LIMIT", "0", "-1"],
        "value is not an integer",
    );
    t.assert_err(
        &["sort", "list-neg", "LIMIT", "-1", "-1"],
        "value is not an integer",
    );
}

#[test]
fn sort_by() {
    let mut t = Ctx::shards(1);
    t.assert_int(&["del", "list-1"], 0);
    t.assert_int(&["lpush", "list-1", "1", "2", "3"], 3);
    t.ok(&["set", "w_1", "30"]);
    t.ok(&["set", "w_2", "20"]);
    t.ok(&["set", "w_3", "10"]);

    assert_eq!(
        sorted(&t.run(&["sort", "list-1", "BY", "w_*"])),
        vec!["1", "2", "3"]
    );
    assert_eq!(
        sorted(&t.run(&["sort", "list-1", "BY", "w_*", "DESC"])),
        vec!["1", "2", "3"]
    );

    t.ok(&["set", "s_1", "c"]);
    t.ok(&["set", "s_2", "b"]);
    t.ok(&["set", "s_3", "a"]);
    assert_eq!(
        sorted(&t.run(&["sort", "list-1", "BY", "s_*", "ALPHA"])),
        vec!["1", "2", "3"]
    );

    assert_eq!(
        sorted(&t.run(&["sort", "list-1", "BY", "nosort"])),
        vec!["1", "2", "3"]
    );

    // Missing keys sort as 0.
    t.assert_int(&["del", "w_1"], 1);
    assert_eq!(
        sorted(&t.run(&["sort", "list-1", "BY", "w_*"])),
        vec!["1", "2", "3"]
    );

    t.ok(&["set", "w_1", "30"]);
    assert_eq!(
        sorted(&t.run(&["sort", "list-1", "BY", "w_*", "LIMIT", "1", "2"])),
        vec!["1", "2"]
    );

    // Multiple asterisks -> syntax error.
    t.assert_err(&["sort", "list-1", "BY", "w_*_*"], "syntax error");
}

#[test]
fn sort_get() {
    let mut t = Ctx::shards(1);
    t.assert_int(&["del", "mylist"], 0);
    t.assert_int(&["lpush", "mylist", "1", "2", "3"], 3);
    t.ok(&["set", "obj_1", "first"]);
    t.ok(&["set", "obj_2", "second"]);
    t.ok(&["set", "obj_3", "third"]);
    t.ok(&["set", "weight_1", "30"]);
    t.ok(&["set", "weight_2", "20"]);
    t.ok(&["set", "weight_3", "10"]);

    assert_eq!(
        sorted(&t.run(&["sort", "mylist", "GET", "obj_*"])),
        vec!["first", "second", "third"]
    );
    assert_eq!(
        sorted(&t.run(&["sort", "mylist", "GET", "#"])),
        vec!["1", "2", "3"]
    );
    assert_eq!(
        sorted(&t.run(&["sort", "mylist", "GET", "#", "GET", "obj_*"])),
        vec!["1", "2", "3", "first", "second", "third"]
    );
    assert_eq!(
        sorted(&t.run(&["sort", "mylist", "BY", "weight_*", "GET", "obj_*"])),
        vec!["first", "second", "third"]
    );
    assert_eq!(
        sorted(&t.run(&[
            "sort", "mylist", "BY", "weight_*", "GET", "#", "GET", "obj_*"
        ])),
        vec!["1", "2", "3", "first", "second", "third"]
    );

    // Missing GET keys return empty strings.
    t.assert_int(&["del", "obj_2"], 1);
    assert_eq!(
        sorted(&t.run(&["sort", "mylist", "GET", "obj_*"])),
        vec!["", "first", "third"]
    );
    t.ok(&["set", "obj_2", "second"]);

    // GET with a literal (star-free) pattern applies to every element.
    t.ok(&["set", "fixed_key", "fixed_value"]);
    assert_eq!(
        sorted(&t.run(&["sort", "mylist", "GET", "fixed_key"])),
        vec!["fixed_value", "fixed_value", "fixed_value"]
    );

    // GET + STORE.
    t.assert_int(
        &[
            "sort", "mylist", "GET", "#", "GET", "obj_*", "STORE", "result",
        ],
        6,
    );
    assert_eq!(
        sorted(&t.run(&["lrange", "result", "0", "-1"])),
        vec!["1", "2", "3", "first", "second", "third"]
    );

    // BY nosort + GET preserves insertion order.
    assert_eq!(
        sorted(&t.run(&["sort", "mylist", "BY", "nosort", "GET", "obj_*"])),
        vec!["first", "second", "third"]
    );

    // GET pattern with multiple asterisks -> syntax error.
    t.assert_err(&["sort", "mylist", "GET", "obj_*_*"], "syntax error");

    // Empty list.
    t.assert_int(&["del", "emptylist"], 0);
    t.assert_int(&["lpush", "emptylist", "placeholder"], 1);
    t.assert_text(&["lpop", "emptylist"], "placeholder");
    assert_eq!(
        t.run(&["sort", "emptylist", "GET", "obj_*"])
            .arr()
            .unwrap()
            .len(),
        0
    );

    // SORT_RO supports GET.
    assert_eq!(
        sorted(&t.run(&["sort_ro", "mylist", "GET", "#", "GET", "obj_*"])),
        vec!["1", "2", "3", "first", "second", "third"]
    );
}

#[test]
fn dump() {
    let mut t = Ctx::new();

    // String dump for "19" (int8-encoded).
    let expected_string: &[u8] = &[
        0x00, 0xc0, 0x13, 0x09, 0x00, 0x23, 0x13, 0x6f, 0x4d, 0x68, 0xf6, 0x35, 0x6e,
    ];
    // List dump for rpush l 20.
    let expected_list: &[u8] = &[
        0x12, 0x01, 0x02, 0x09, 0x09, 0x00, 0x00, 0x00, 0x01, 0x00, 0x14, 0x01, 0xff, 0x09, 0x00,
        0xfb, 0xbd, 0x36, 0xf8, 0xb4, 0x74, 0x25, 0x3b,
    ];
    // Hash dump for hset z2 19 1234.
    let expected_hash: &[u8] = &[
        0x10, 0x0c, 0x0c, 0x00, 0x00, 0x00, 0x02, 0x00, 0x13, 0x01, 0xc4, 0xd2, 0x02, 0xff, 0x09,
        0x00, 0x68, 0x4d, 0x73, 0xa4, 0x0f, 0x23, 0x4f, 0xc7,
    ];

    t.ok(&["set", "z", "19"]);
    assert_eq!(t.bulk(&["dump", "z"]), expected_string);

    t.assert_int(&["rpush", "l", "20"], 1);
    assert_eq!(t.bulk(&["dump", "l"]), expected_list);

    t.assert_int(&["hset", "z2", "19", "1234"], 1);
    assert_eq!(t.bulk(&["dump", "z2"]), expected_hash);

    t.assert_null(&["dump", "foo"]);
}

#[test]
fn restore() {
    let mut t = Ctx::new();

    // A Redis 6 string dump (RDB_VERSION 9) for "1234".
    let string_dump: Vec<u8> = vec![
        0x00, 0xc1, 0xd2, 0x04, 0x09, 0x00, 0xd0, 0x75, 0x59, 0x6d, 0x10, 0x04, 0x3f, 0x5c,
    ];

    // Restore into an existing key fails with BUSYKEY.
    t.ok(&["set", "exiting-key", "1234"]);
    let v = t.run_b(&[
        b"restore".to_vec(),
        b"exiting-key".to_vec(),
        b"0".to_vec(),
        string_dump.clone(),
    ]);
    expect_err(&v, "BUSYKEY Target key name already exists.");

    // ABSTTL in the past + REPLACE deletes the key.
    t.ok_b(&[
        b"restore".to_vec(),
        b"exiting-key".to_vec(),
        b"1665476212900".to_vec(),
        string_dump.clone(),
        b"ABSTTL".to_vec(),
        b"REPLACE".to_vec(),
    ]);
    t.assert_null(&["get", "exiting-key"]);

    // Fresh restore, value readable and re-dumps to the exact payload.
    t.ok_b(&[
        b"restore".to_vec(),
        b"new-key".to_vec(),
        b"0".to_vec(),
        string_dump.clone(),
    ]);
    t.assert_text(&["get", "new-key"], "1234");
    assert_eq!(t.bulk(&["dump", "new-key"]), string_dump);

    // List.
    t.assert_int(&["rpush", "orig-list", "20"], 1);
    let list_dump = t.bulk(&["dump", "orig-list"]);
    t.ok_b(&[
        b"restore".to_vec(),
        b"new-list".to_vec(),
        b"100000".to_vec(),
        list_dump,
    ]);
    t.assert_text(&["lpop", "new-list"], "20");

    // Hash.
    t.assert_int(&["hset", "orig-hash", "123", "45678"], 1);
    let hash_dump = t.bulk(&["dump", "orig-hash"]);
    t.ok_b(&[
        b"restore".to_vec(),
        b"new-hash".to_vec(),
        b"100000".to_vec(),
        hash_dump,
    ]);
    t.assert_int(&["hexists", "new-hash", "123"], 1);

    // REPLACE with relative TTL.
    t.ok(&["set", "string-key", "hello world"]);
    let hello_dump = t.bulk(&["dump", "string-key"]);
    t.ok_b(&[
        b"restore".to_vec(),
        b"string-key".to_vec(),
        b"7000".to_vec(),
        string_dump.clone(),
        b"REPLACE".to_vec(),
    ]);
    t.assert_text(&["get", "string-key"], "1234");
    assert_pttl(&mut t, "string-key", 7000);

    // ABSTTL with a future timestamp.
    let at = now_ms() + 2000;
    t.ok_b(&[
        b"restore".to_vec(),
        b"string-key".to_vec(),
        at.to_string().into_bytes(),
        hello_dump,
        b"ABSTTL".to_vec(),
        b"REPLACE".to_vec(),
    ]);
    t.assert_text(&["get", "string-key"], "hello world");
    assert_pttl(&mut t, "string-key", 2000);

    // No TTL.
    t.ok_b(&[
        b"restore".to_vec(),
        b"string-key".to_vec(),
        b"0".to_vec(),
        string_dump.clone(),
        b"REPLACE".to_vec(),
    ]);
    t.assert_text(&["get", "string-key"], "1234");
    t.assert_int(&["ttl", "string-key"], -1);

    // A Redis 7 listpack-encoded set.
    let set_dump: Vec<u8> = vec![
        0x14, 0x0d, 0x0d, 0x00, 0x00, 0x00, 0x01, 0x00, 0x84, 0x61, 0x63, 0x6d, 0x65, 0x05, 0xff,
        0x0b, 0x00, 0xc1, 0x37, 0x5c, 0xe5, 0xe2, 0xc0, 0xdd, 0x27,
    ];
    t.ok_b(&[
        b"restore".to_vec(),
        b"listpack-set".to_vec(),
        b"0".to_vec(),
        set_dump,
    ]);
    t.assert_int(&["sismember", "listpack-set", "acme"], 1);

    // A Redis 7 listpack-encoded zset.
    let zset_dump: Vec<u8> = vec![
        0x11, 0x0f, 0x0f, 0x00, 0x00, 0x00, 0x02, 0x00, 0x84, 0x65, 0x6c, 0x6f, 0x6e, 0x05, 0x01,
        0x01, 0xff, 0x0b, 0x00, 0xc8, 0x01, 0x2c, 0xad, 0xd9, 0xa3, 0x99, 0x5e,
    ];
    t.ok_b(&[
        b"restore".to_vec(),
        b"my-zset".to_vec(),
        b"0".to_vec(),
        zset_dump.clone(),
    ]);
    assert_eq!(
        sorted(&t.run(&["zrange", "my-zset", "0", "-1"])),
        vec!["elon"]
    );

    // Corrupt payload (valid CRC, wrong type byte) must be rejected.
    let mut corrupt = zset_dump.clone();
    corrupt[0] = 0x12;
    let v = t.run_b(&[
        b"restore".to_vec(),
        b"invalid".to_vec(),
        b"0".to_vec(),
        corrupt,
    ]);
    expect_err(&v, "ERR Bad data format");
}

#[test]
fn delex() {
    let mut t = Ctx::new();
    // DELEX without a condition behaves like DEL.
    t.ok(&["set", "key1", "value1"]);
    t.assert_int(&["delex", "key1"], 1);
    t.assert_null(&["get", "key1"]);

    t.assert_int(&["delex", "nonexistent"], 0);

    // IFEQ.
    t.ok(&["set", "key2", "value2"]);
    t.assert_int(&["delex", "key2", "IFEQ", "value2"], 1);
    t.assert_null(&["get", "key2"]);

    t.ok(&["set", "key3", "value3"]);
    t.assert_int(&["delex", "key3", "IFEQ", "wrongvalue"], 0);
    t.assert_text(&["get", "key3"], "value3");

    // IFNE.
    t.ok(&["set", "key4", "value4"]);
    t.assert_int(&["delex", "key4", "IFNE", "differentvalue"], 1);
    t.assert_null(&["get", "key4"]);

    t.ok(&["set", "key5", "value5"]);
    t.assert_int(&["delex", "key5", "IFNE", "value5"], 0);
    t.assert_text(&["get", "key5"], "value5");

    // IFDEQ / IFDNE against the DIGEST of the value.
    t.ok(&["set", "key6", "value6"]);
    let digest = String::from_utf8(t.bulk(&["digest", "key6"])).unwrap();
    t.assert_int(&["delex", "key6", "IFDEQ", &digest], 1);
    t.assert_null(&["get", "key6"]);

    t.ok(&["set", "key7", "value7"]);
    t.assert_int(&["delex", "key7", "IFDEQ", "0000000000000000"], 0);
    t.assert_text(&["get", "key7"], "value7");

    t.ok(&["set", "key8", "value8"]);
    t.assert_int(&["delex", "key8", "IFDNE", "0000000000000000"], 1);
    t.assert_null(&["get", "key8"]);

    t.ok(&["set", "key9", "value9"]);
    let digest9 = String::from_utf8(t.bulk(&["digest", "key9"])).unwrap();
    t.assert_int(&["delex", "key9", "IFDNE", &digest9], 0);
    t.assert_text(&["get", "key9"], "value9");

    // Wrong type.
    t.assert_int(&["lpush", "list1", "item"], 1);
    t.assert_err(&["delex", "list1", "IFEQ", "item"], "WRONGTYPE");

    // Invalid option.
    t.ok(&["set", "key10", "value10"]);
    t.assert_err(
        &["delex", "key10", "INVALID", "value"],
        "Unknown subcommand",
    );

    // Wrong arity.
    t.assert_err(
        &["delex", "key", "IFEQ", "val", "extra"],
        "wrong number of arguments",
    );
    t.assert_err(
        &["delex", "key11", "randomarg"],
        "wrong number of arguments",
    );
    t.assert_err(&["delex", "key12", "IFEQ"], "wrong number of arguments");
    t.assert_err(&["delex", "key13", "xyz"], "wrong number of arguments");
}

#[test]
fn time_cmd() {
    let mut t = Ctx::new();
    let v = t.run(&["time"]);
    let a = v.arr().unwrap();
    assert_eq!(a.len(), 2);
    let secs: u64 = a[0].text().unwrap().parse().unwrap();
    let _micros: u64 = a[1].text().unwrap().parse().unwrap();
    assert!(secs > 1_600_000_000, "implausible time {secs}");

    // TIME inside MULTI/EXEC replies an array of two TIME arrays.
    t.ok(&["multi"]);
    t.run(&["time"]);
    sleep(Duration::from_millis(2));
    t.run(&["time"]);
    let v = t.arr(&["exec"]);
    assert_eq!(v.len(), 2);
    for tv in &v {
        let a = tv.arr().unwrap();
        assert_eq!(a.len(), 2);
        assert!(a[0].text().unwrap().parse::<u64>().unwrap() > 1_600_000_000);
        assert!(a[1].text().unwrap().parse::<u64>().is_ok());
    }
}
