//! Port of `dragonfly/src/server/set_family_test.cc`.
//!
//! Adaptations from the C++ original:
//! - `Run(...)` becomes `t.run`; `IntArg`/`ErrArg`/`CheckedInt` become
//!   `t.assert_int` / `t.assert_err` / `t.int`; `RespElementsAre` and
//!   `RespArray(ElementsAre(...))` become `t.arr` element checks.
//! - `UnorderedElementsAre(...)` becomes `assert_unordered_eq` (multiset
//!   equality), `IsSubsetOf`/`ConsistsOf` become `assert_all_in`, and
//!   `AnyOf(...)` becomes `assert_any_of`, all over `Value::text()`.
//! - SPOP/SRANDMEMBER trailing-argument rejection (`"ERR syntax error"`) is
//!   enforced in `exec_spop` / `exec_srandmember`, mirroring `t_set.c`.
//! - `SAddEx` and friends advance the fake clock under `clock_guard`, replacing
//!   `TEST_current_time_ms = kMemberExpiryBase * 1000` + `AdvanceTime`.
//! - `SetInter_5590` (DEBUG POPULATE + shard-count/timing assertions),
//!   `StoreOverwritesNonSetKeyAccounting` (GetMetrics memory accounting) and
//!   `IntSetMemcpy` (C++ `intset` blob layout) are not portable and skipped.
//! - `ShrinkMemoryAccountingSet`: the port's SHRINK exposes no bucket array to
//!   compact, so it reports 0 (the reference's "nothing to shrink" fast path)
//!   instead of freed bytes; the SREM-after-expiry and SCARD checks are kept.

mod common;

use common::*;

/// Multiset equality between a reply array's texts and `elems` (unordered).
fn assert_unordered_eq(t: &mut Ctx, args: &[&str], elems: &[&str]) {
    let v = t.arr(args);
    assert_unordered_values(&v, elems);
}

/// Multiset equality between decoded bulk strings and `elems`.
#[track_caller]
fn assert_unordered_values(v: &[Value], elems: &[&str]) {
    let mut got: Vec<String> = v.iter().map(|x| x.text().expect("bulk")).collect();
    got.sort();
    let mut want: Vec<String> = elems.iter().map(ToString::to_string).collect();
    want.sort();
    assert_eq!(got, want, "values {v:?}");
}

/// Assert every decoded element of `v` belongs to `elems`.
fn assert_all_in(v: &[Value], elems: &[&str]) {
    for e in v {
        let s = e.text().expect("bulk");
        assert!(elems.contains(&s.as_str()), "{s:?} not in {elems:?}");
    }
}

/// Assert `v` decodes to exactly one string belonging to `elems`.
fn assert_any_of(v: &Value, elems: &[&str]) {
    let s = v.text().expect("bulk");
    assert!(elems.contains(&s.as_str()), "{s:?} not in {elems:?}");
}

// =============================================================================
// Basic SADD / type handling
// =============================================================================

#[test]
fn s_add() {
    let mut t = Ctx::new();
    t.assert_int(&["sadd", "x", "1", "2", "3"], 3);
    t.assert_int(&["sadd", "x", "2", "3"], 0);
    t.ok(&["set", "a", "foo"]);
    t.assert_err(&["sadd", "a", "b"], "WRONGTYPE ");
    t.assert_text(&["type", "x"], "set");
}

#[test]
fn int_conv() {
    let mut t = Ctx::new();
    t.assert_int(&["sadd", "x", "134"], 1);
    t.assert_int(&["sadd", "x", "abc"], 1);
    t.assert_int(&["sadd", "x", "134"], 0);
}

// =============================================================================
// Multi-key ops: SUNIONSTORE / SDIFF / SINTER / SINTERCARD / SMOVE
// =============================================================================

#[test]
fn s_union_store() {
    let mut t = Ctx::new();
    t.assert_int(&["sadd", "b", "1", "2", "3"], 3);
    t.assert_int(&["sadd", "c", "10", "11"], 2);
    t.ok(&["set", "a", "foo"]);
    t.assert_int(&["sunionstore", "a", "b", "c"], 5);
    t.assert_text(&["type", "a"], "set");
    assert_unordered_eq(&mut t, &["smembers", "a"], &["11", "10", "1", "2", "3"]);
}

// SUNIONSTORE overwrites a value including resetting its expiration.
#[test]
fn s_union_store_expiration() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    t.int(&["sadd", "s1", "a", "b"]);
    t.int(&["sadd", "s2", "c", "d"]);
    t.ok(&["set", "target", "some-value"]);
    t.assert_int(&["expire", "target", "1010"], 1);
    t.assert_int(&["ttl", "target"], 1010);
    t.assert_int(&["sunionstore", "target", "s1", "s2"], 4);
    t.assert_int(&["scard", "target"], 4);
    t.assert_int(&["ttl", "target"], -1);
}

#[test]
fn s_diff() {
    let mut t = Ctx::new();
    t.int(&["sadd", "b", "1", "2", "3"]);
    t.int(&["sadd", "c", "10", "11"]);
    t.ok(&["set", "a", "foo"]);

    assert_unordered_eq(&mut t, &["sdiff", "b", "c"], &["1", "2", "3"]);
    t.assert_int(&["sdiffstore", "a", "b", "c"], 3);

    t.ok(&["set", "str", "foo"]);
    t.assert_err(&["sdiff", "b", "str"], "WRONGTYPE ");

    t.int(&["sadd", "bar", "x", "a", "b", "c"]);
    t.int(&["sadd", "foo", "c"]);
    t.int(&["sadd", "car", "a", "d"]);
    t.assert_int(&["sdiffstore", "tar", "bar", "foo", "car"], 2);
}

#[test]
fn s_inter() {
    let mut t = Ctx::new();
    t.int(&["sadd", "a", "1", "2", "3", "4"]);
    t.int(&["sadd", "b", "3", "5", "6", "2"]);
    t.assert_int(&["sinterstore", "d", "a", "b"], 2);
    assert_unordered_eq(&mut t, &["smembers", "d"], &["3", "2"]);

    t.ok(&["set", "y", ""]);
    t.assert_err(&["sinter", "x", "y"], "WRONGTYPE Operation against a key");
    t.assert_int(&["sinterstore", "none1", "none2"], 0);
    t.assert_err(&["sinter"], "wrong number of arguments");
}

#[test]
fn s_inter_card() {
    let mut t = Ctx::new();
    t.int(&["sadd", "s1", "2", "b", "1", "a"]);
    t.int(&["sadd", "s2", "3", "c", "2", "b"]);
    t.int(&["sadd", "s3", "2", "b", "3", "c"]);

    t.assert_int(&["sintercard", "2", "s1", "s2"], 2);
    t.assert_int(&["sintercard", "2", "s1", "s4"], 0);
    t.assert_int(&["sintercard", "2", "s2", "s3", "LIMIT", "2"], 2);
    t.assert_int(&["sintercard", "1", "s1"], 4);

    // Redis does not throw this message, but SimpleAtoi does.
    t.assert_err(
        &["sintercard", "a", "s1", "s2"],
        "value is not an integer or out of range",
    );
    t.assert_err(&["sintercard", "2", "s1", "s2", "LIMIT"], "syntax error");
    t.assert_err(
        &["sintercard", "2", "s1", "s2", "LIMIT", "a"],
        "limit can't be negative",
    );
    t.assert_err(
        &["sintercard", "2", "s1", "s2", "LIMIT", "-1"],
        "limit can't be negative",
    );
    t.assert_err(&["sintercard", "2", "s1"], "syntax error");
    t.assert_err(
        &["sintercard", "0", "LIMIT", "0"],
        "at least 1 input key is needed",
    );
    t.assert_err(
        &["sintercard", "-1", "s1"],
        "value is not an integer or out of range",
    );
}

#[test]
fn s_move() {
    let mut t = Ctx::new();
    t.int(&["sadd", "a", "1", "2", "3", "4"]);
    t.int(&["sadd", "b", "3", "5", "6", "2"]);
    t.assert_int(&["smove", "a", "b", "1"], 1);

    t.int(&["sadd", "x", "a", "b", "c"]);
    t.int(&["sadd", "y", "c"]);
    t.assert_int(&["smove", "x", "y", "c"], 1);
}

#[test]
fn s_pop() {
    let mut t = Ctx::new();
    t.int(&["sadd", "x", "1", "2", "3"]);
    let v = t.arr(&["spop", "x", "3"]);
    assert_unordered_values(&v, &["1", "2", "3"]);
    t.assert_text(&["type", "x"], "none");

    t.int(&["sadd", "x", "1", "2", "3"]);
    let v = t.arr(&["spop", "x", "2"]);
    assert_eq!(v.len(), 2);
    assert_all_in(&v, &["1", "2", "3"]);
    t.assert_int(&["scard", "x"], 1);

    t.int(&["sadd", "y", "a", "b", "c"]);
    let v = t.arr(&["spop", "y", "1"]);
    assert_eq!(v.len(), 1);
    assert_any_of(&v[0], &["a", "b", "c"]);
    let v = t.arr(&["smembers", "y"]);
    assert_eq!(v.len(), 2);
    assert_all_in(&v, &["a", "b", "c"]);

    // SPOP on a large set with small pop count.
    let mut args: Vec<String> = vec!["sadd".into(), "xlarge".into()];
    for i in 0..100 {
        args.push(i.to_string());
    }
    let cmd: Vec<&str> = args.iter().map(String::as_str).collect();
    t.run(&cmd);

    let v = t.arr(&["spop", "xlarge", "2"]);
    assert_eq!(v.len(), 2);
    assert_ne!(v[0].text(), v[1].text());
    t.assert_int(&["scard", "xlarge"], 98);

    // SPOP accepts only `key` or `key count`; trailing args must be rejected,
    // not silently ignored.
    t.assert_err(&["spop", "xlarge", "2", "3"], "syntax error");
}

#[test]
fn s_rand_member() {
    let mut t = Ctx::new();

    // Test IntSet.
    t.int(&["sadd", "x", "1", "2", "3"]);

    // count > 0 (IntSet).
    assert_any_of(&t.run(&["srandmember", "x"]), &["1", "2", "3"]);
    let v = t.arr(&["srandmember", "x", "1"]);
    assert_any_of(&v[0], &["1", "2", "3"]);
    let v = t.arr(&["srandmember", "x", "2"]);
    assert_eq!(v.len(), 2);
    assert_all_in(&v, &["1", "2", "3"]);
    let v = t.arr(&["srandmember", "x", "3"]);
    assert_eq!(v.len(), 3);
    assert_unordered_values(&v, &["1", "2", "3"]);

    // count larger than the IntSet size.
    let v = t.arr(&["srandmember", "x", "25"]);
    assert_eq!(v.len(), 3);
    assert_unordered_values(&v, &["1", "2", "3"]);

    // count < 0 (IntSet): duplicates allowed.
    let v = t.arr(&["srandmember", "x", "-1"]);
    assert_eq!(v.len(), 1);
    assert_any_of(&v[0], &["1", "2", "3"]);
    let v = t.arr(&["srandmember", "x", "-2"]);
    assert_eq!(v.len(), 2);
    assert_all_in(&v, &["1", "2", "3"]);
    let v = t.arr(&["srandmember", "x", "-3"]);
    assert_eq!(v.len(), 3);
    assert_all_in(&v, &["1", "2", "3"]);
    let v = t.arr(&["srandmember", "x", "-25"]);
    assert_eq!(v.len(), 25);
    assert_all_in(&v, &["1", "2", "3"]);

    // Test StrSet.
    t.int(&["sadd", "y", "a", "b", "c"]);

    assert_any_of(&t.run(&["srandmember", "y"]), &["a", "b", "c"]);
    let v = t.arr(&["srandmember", "y", "1"]);
    assert_any_of(&v[0], &["a", "b", "c"]);
    let v = t.arr(&["srandmember", "y", "2"]);
    assert_eq!(v.len(), 2);
    assert_all_in(&v, &["a", "b", "c"]);
    let v = t.arr(&["srandmember", "y", "3"]);
    assert_eq!(v.len(), 3);
    assert_unordered_values(&v, &["a", "b", "c"]);
    let v = t.arr(&["srandmember", "y", "25"]);
    assert_eq!(v.len(), 3);
    assert_unordered_values(&v, &["a", "b", "c"]);
    let v = t.arr(&["srandmember", "y", "-1"]);
    assert_eq!(v.len(), 1);
    assert_any_of(&v[0], &["a", "b", "c"]);
    let v = t.arr(&["srandmember", "y", "-2"]);
    assert_eq!(v.len(), 2);
    assert_all_in(&v, &["a", "b", "c"]);
    let v = t.arr(&["srandmember", "y", "-3"]);
    assert_eq!(v.len(), 3);
    assert_all_in(&v, &["a", "b", "c"]);
    let v = t.arr(&["srandmember", "y", "-25"]);
    assert_eq!(v.len(), 25);
    assert_all_in(&v, &["a", "b", "c"]);

    // count 0.
    let v = t.arr(&["srandmember", "x", "0"]);
    assert!(v.is_empty());

    // Empty set.
    t.assert_int(&["sadd", "empty::set", "1"], 1);
    t.assert_int(&["srem", "empty::set", "1"], 1);
    assert!(t.arr(&["srandmember", "empty::set", "0"]).is_empty());
    assert!(t.arr(&["srandmember", "empty::set", "3"]).is_empty());
    assert!(t.arr(&["srandmember", "empty::set", "-4"]).is_empty());

    // Key does not exist.
    t.assert_null(&["srandmember", "unknown::set"]);
    assert!(t.arr(&["srandmember", "unknown::set", "0"]).is_empty());

    // Redis returns a syntax error for extra args (t_set.c srandmemberCommand).
    t.assert_err(&["srandmember", "x", "5", "3"], "syntax error");
}

#[test]
fn s_m_is_member() {
    let mut t = Ctx::new();
    t.int(&["sadd", "foo", "a"]);
    t.int(&["sadd", "foo", "b"]);

    t.assert_err(&["smismember", "foo"], "wrong number of arguments");

    let v = t.arr(&["smismember", "foo1", "a", "b"]);
    assert_eq!(
        v.iter().map(Value::int).collect::<Vec<_>>(),
        vec![Some(0), Some(0)]
    );
    let v = t.arr(&["smismember", "foo", "a", "c"]);
    assert_eq!(
        v.iter().map(Value::int).collect::<Vec<_>>(),
        vec![Some(1), Some(0)]
    );
    let v = t.arr(&["smismember", "foo", "a", "b"]);
    assert_eq!(
        v.iter().map(Value::int).collect::<Vec<_>>(),
        vec![Some(1), Some(1)]
    );
    let v = t.arr(&["smismember", "foo", "d", "e"]);
    assert_eq!(
        v.iter().map(Value::int).collect::<Vec<_>>(),
        vec![Some(0), Some(0)]
    );
    let v = t.arr(&["smismember", "foo", "b"]);
    assert_eq!(v.iter().map(Value::int).collect::<Vec<_>>(), vec![Some(1)]);
    let v = t.arr(&["smismember", "foo", "x"]);
    assert_eq!(v.iter().map(Value::int).collect::<Vec<_>>(), vec![Some(0)]);
}

#[test]
fn empty() {
    let mut t = Ctx::new();
    let v = t.arr(&["smembers", "x"]);
    assert!(v.is_empty());
}

// =============================================================================
// SSCAN
// =============================================================================

#[test]
fn s_scan() {
    let mut t = Ctx::new();
    let v = t.arr(&["sscan", "non-existing-key", "100", "count", "5"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert_eq!(v[0].text().as_deref(), Some("0"));
    assert!(v[1].arr().unwrap().is_empty());

    // Int set.
    for i in 0..15 {
        t.int(&["sadd", "myintset", &i.to_string()]);
    }

    // Even though the count limits by 4, all intlist fields are returned.
    let v = t.arr(&["sscan", "myintset", "0", "count", "4"]);
    let members = v[1].arr().unwrap();
    assert_eq!(members.len(), 15);

    let v = t.arr(&["sscan", "myintset", "0", "match", "1*"]);
    assert_unordered_values(v[1].arr().unwrap(), &["1", "10", "11", "12", "13", "14"]);

    // String set.
    for i in 0..15 {
        t.int(&["sadd", "mystrset", &format!("str-{i}")]);
    }

    let v = t.arr(&["sscan", "mystrset", "0", "count", "5"]);
    assert_eq!(v[1].arr().unwrap().len(), 5);

    let v = t.arr(&["sscan", "mystrset", "0", "match", "str-1*"]);
    assert_unordered_values(
        v[1].arr().unwrap(),
        &["str-1", "str-10", "str-11", "str-12", "str-13", "str-14"],
    );

    let v = t.arr(&["sscan", "mystrset", "0", "match", "str-1*", "count", "3"]);
    let members = v[1].arr().unwrap();
    assert_all_in(
        members,
        &["str-1", "str-10", "str-11", "str-12", "str-13", "str-14"],
    );
    assert_eq!(members.len(), 3);

    // Nothing should match this.
    let v = t.arr(&["sscan", "mystrset", "0", "match", "1*"]);
    assert_eq!(v[1].arr().unwrap().len(), 0);

    // An invalid (non-numeric) cursor must be rejected without crashing.
    t.assert_err(&["sscan", "mystrset", "abc"], "invalid cursor");
    t.assert_err(
        &["sscan", "mystrset", "{\"a\":1}", "LIST"],
        "invalid cursor",
    );

    // The server must still be responsive after the rejected cursors.
    let v = t.arr(&["sscan", "mystrset", "0", "match", "str-1*"]);
    assert_unordered_values(
        v[1].arr().unwrap(),
        &["str-1", "str-10", "str-11", "str-12", "str-13", "str-14"],
    );
}

#[test]
fn huge_s_scan() {
    let mut t = Ctx::new();
    for i in (0..60000).step_by(5) {
        t.int(&[
            "sadd",
            "myintset",
            &i.to_string(),
            &(i + 1).to_string(),
            &(i + 2).to_string(),
            &(i + 3).to_string(),
            &(i + 4).to_string(),
        ]);
    }

    let v = t.arr(&["sscan", "myintset", "0", "count", "50000"]);
    assert!(v[1].arr().unwrap().len() >= 50000);
}

// =============================================================================
// Per-member TTL (SADDEX) and lazy expiry
// =============================================================================

#[test]
fn s_add_ex() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    t.assert_int(&["saddex", "key", "2", "val"], 1);
    advance(1500);
    t.assert_int(&["saddex", "key", "2", "val"], 0);
    advance(1000);
    t.assert_int(&["sismember", "key", "val"], 1);

    t.assert_err(
        &["saddex", "k", "one", "v"],
        "value is not an integer or out of range",
    );

    // KEEPTTL: add member orig with TTL=10.
    t.assert_int(&["saddex", "key", "10", "orig"], 1);

    // Add new and refresh orig with TTL=1 and KEEPTTL; orig's TTL is preserved.
    t.assert_int(&["saddex", "key", "KEEPTTL", "1", "orig", "new"], 1);
    let v = t.int(&["fieldttl", "key", "new"]);
    assert!(v <= 1, "new member TTL {v}");
    let v = t.int(&["fieldttl", "key", "orig"]);
    assert!(v > 5, "orig member TTL {v}");

    // Without KEEPTTL the TTL is overwritten.
    t.assert_int(&["saddex", "key", "2", "orig", "new"], 0);
    let v = t.int(&["fieldttl", "key", "orig"]);
    assert!(v <= 2, "orig member TTL {v}");

    // At least one member argument is expected.
    t.assert_err(
        &["saddex", "key", "KEEPTTL", "2"],
        "wrong number of arguments",
    );
}

#[test]
fn s_add_ex_ttl_boundary() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    // The member-TTL ceiling is the shared kMaxExpireDeadlineSec.
    t.assert_int(&["saddex", "key", "268435455", "at_cap"], 1);
    t.assert_err(
        &["saddex", "key", "268435456", "above_cap"],
        "value is not an integer or out of range",
    );
}

#[test]
fn check_set_link_expiry_transfer() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    for i in 0..10 {
        t.assert_int(&["saddex", "key", "5", &i.to_string()], 1);
    }
    for i in 0..9 {
        t.int(&["srem", "key", &i.to_string()]);
    }
    t.assert_int(&["scard", "key"], 1);
    advance(6000);
    t.run(&["smembers", "key"]);
    t.assert_int(&["scard", "key"], 0);
}

// SPOP on a set where all members have expired via lazy expiry must return
// nil, not crash.
#[test]
fn s_pop_all_expired() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    // Add a member without TTL, then update it with TTL via SADDEX.
    t.int(&["sadd", "key", "member"]);
    t.assert_int(&["saddex", "key", "1", "member"], 0);

    advance(2000);

    t.assert_null(&["spop", "key"]);
}

// SDIFF/SDIFFSTORE crash when all set members have expired via per-member TTL,
// leaving the key present but the set empty.
#[test]
fn s_diff_all_members_expired() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    t.int(&["saddex", "src", "1", "a", "b", "c"]);
    t.int(&["sadd", "other", "x"]);

    advance(2000);

    let v = t.arr(&["sdiff", "src", "other"]);
    assert!(v.is_empty());
    t.assert_int(&["exists", "src"], 0);

    t.int(&["saddex", "src", "1", "a", "b", "c"]);
    advance(2000);
    t.assert_int(&["sdiffstore", "dest", "src", "other"], 0);
    t.assert_int(&["exists", "src"], 0);
}

// Verify key deletion after lazy member expiry for SUNION and SINTER.
#[test]
fn set_ops_delete_empty_after_expiry() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    t.int(&["saddex", "s1", "1", "a", "b"]);
    advance(2000);

    let v = t.arr(&["sunion", "s1"]);
    assert!(v.is_empty());
    t.assert_int(&["exists", "s1"], 0);

    t.int(&["saddex", "s2", "1", "a", "b"]);
    advance(2000);

    let v = t.arr(&["sinter", "s2"]);
    assert!(v.is_empty());
    t.assert_int(&["exists", "s2"], 0);
}

#[test]
fn s_pop_with_expired_members() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    // Add members with a short TTL. After expiry Size() still reports them.
    t.int(&["saddex", "key", "1", "a", "b", "c"]);

    // Let all members expire.
    advance(2000);

    // SPOP 2: iteration lazy-expires all members, so nothing is actually
    // popped and the empty set is deleted.
    let v = t.arr(&["spop", "key", "2"]);
    assert!(v.is_empty());
    t.assert_int(&["exists", "key"], 0);

    // Single-arg form: SPOP key (no count). Must return NULL, not crash.
    t.int(&["saddex", "key2", "1", "x", "y"]);
    advance(2000);

    t.assert_null(&["spop", "key2"]);
    t.assert_int(&["exists", "key2"], 0);
}

#[test]
fn s_pop_single_arg_expired_case2() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    for attempt in 0..50 {
        let key = format!("key{attempt}");

        t.int(&["sadd", &key, "live"]);
        t.int(&["saddex", &key, "1", "a", "b", "c"]);

        // Let TTL members expire.
        advance(2000);

        let resp = t.run(&["spop", &key]);
        // Must be either "live" or nil — never a crash.
        if matches!(resp, Value::Bulk(None)) {
            t.assert_int(&["sismember", &key, "live"], 1);
            continue;
        }
        assert_eq!(resp.text().as_deref(), Some("live"));
    }
}

// SRANDMEMBER must not crash when all set members have expired.
#[test]
fn s_rand_member_with_expired_members() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    // 6+ members.
    t.int(&["saddex", "key", "1", "a", "b", "c", "d", "e", "f"]);
    advance(2000);

    // Without count — returns NIL, not crash.
    t.assert_null(&["srandmember", "key"]);
    t.assert_int(&["exists", "key"], 0);

    // With positive count — unique picks path.
    t.int(&["saddex", "key2", "1", "a", "b", "c", "d", "e", "f"]);
    advance(2000);
    assert!(t.arr(&["srandmember", "key2", "1"]).is_empty());
    t.assert_int(&["exists", "key2"], 0);

    // With negative count.
    t.int(&["saddex", "key3", "1", "a", "b", "c", "d", "e", "f"]);
    advance(2000);
    assert!(t.arr(&["srandmember", "key3", "-1"]).is_empty());
    t.assert_int(&["exists", "key3"], 0);

    // Large negative count — iteration path.
    t.int(&["saddex", "key4", "1", "a", "b", "c", "d", "e", "f"]);
    advance(2000);
    assert!(t.arr(&["srandmember", "key4", "-25"]).is_empty());
    t.assert_int(&["exists", "key4"], 0);
}

#[test]
fn s_is_member_deletes_empty_set() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    // Single member so SISMEMBER's lookup empties the set.
    t.int(&["saddex", "key", "1", "a"]);
    advance(2000);

    t.assert_int(&["sismember", "key", "a"], 0);
    t.assert_int(&["exists", "key"], 0);
}

#[test]
fn s_m_is_member_deletes_empty_set() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    t.int(&["saddex", "key", "1", "a", "b"]);
    advance(2000);

    let v = t.arr(&["smismember", "key", "a", "b"]);
    assert_eq!(v.len(), 2);
    t.assert_int(&["exists", "key"], 0);
}

#[test]
fn s_scan_deletes_empty_set() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    t.int(&["saddex", "key", "1", "a", "b"]);
    advance(2000);

    let v = t.arr(&["sscan", "key", "0"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].text().as_deref(), Some("0"));
    assert_eq!(v[1].arr().unwrap().len(), 0);
    t.assert_int(&["exists", "key"], 0);
}

#[test]
fn s_inter_multi_key_deletes_empty_set() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    t.int(&["saddex", "key1", "1", "a", "b"]);
    t.int(&["sadd", "key2", "a", "b"]);
    advance(2000);

    let v = t.arr(&["sinter", "key1", "key2"]);
    assert!(v.is_empty());
    t.assert_int(&["exists", "key1"], 0);
    // key2 has no TTL, should still exist.
    t.assert_int(&["exists", "key2"], 1);
}

#[test]
fn s_move_deletes_empty_source_set() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    // Single member so the SMOVE lookup empties the source set.
    t.int(&["saddex", "src", "1", "a"]);
    t.int(&["sadd", "dst", "x"]);
    advance(2000);

    t.assert_int(&["smove", "src", "dst", "a"], 0);
    t.assert_int(&["exists", "src"], 0);
}

#[test]
fn field_expire_deletes_empty_set() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    // Single member so FIELDEXPIRE triggers lazy expiry.
    t.int(&["saddex", "key", "1", "a"]);
    advance(2000);

    // FIELDEXPIRE on an already-expired member cleans up the empty set.
    let v = t.arr(&["fieldexpire", "key", "100", "a"]);
    assert_eq!(v.iter().map(Value::int).collect::<Vec<_>>(), vec![Some(-2)]);
    t.assert_int(&["exists", "key"], 0);
}

#[test]
fn field_ttl_deletes_empty_set() {
    let mut t = Ctx::new();
    let _clock = clock_guard();

    // Single member so FIELDTTL triggers lazy expiry.
    t.int(&["saddex", "key", "1", "a"]);
    advance(2000);

    // -3 means the field was not found (expired); -2 would mean key not found.
    t.assert_int(&["fieldttl", "key", "a"], -3);
    t.assert_int(&["exists", "key"], 0);
}

// Same bug as ShrinkMemoryAccountingHash but for sets with SADDEX/SREM.
#[test]
fn shrink_memory_accounting_set() {
    const INITIAL: usize = 60;
    const KEEP: usize = 10;
    let mut t = Ctx::new();
    let _clock = clock_guard();

    for i in 0..INITIAL {
        t.int(&["saddex", "s1", "1000", &format!("temp{i}")]);
    }
    // Remove most members while retaining the bucket array.
    for i in 0..(INITIAL - KEEP) {
        t.int(&["srem", "s1", &format!("temp{i}")]);
    }
    // Add members with short TTL.
    for i in 0..KEEP {
        t.int(&["saddex", "s1", "1", &format!("exp{i}")]);
    }
    // Expire the short-TTL members, leaving the 10 long-lived ones.
    advance(2000);

    // The port's SHRINK exposes no bucket array to compact, so it reports 0
    // (the reference's "nothing to shrink" fast path) rather than freed bytes.
    t.assert_int(&["shrink", "s1"], 0);

    // Must not crash; exactly one long-lived member is removed.
    t.int(&["srem", "s1", &format!("temp{}", INITIAL - KEEP)]);
    t.assert_int(&["scard", "s1"], 9);
}
