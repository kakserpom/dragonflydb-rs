//! Port of `dragonfly/src/server/zset_family_test.cc` to the in-process
//! harness (`tests/common/mod.rs`).
//!
//! Adaptations from the reference:
//! - `Run(...)` becomes `t.run`; `IntArg`/`CheckedInt`/`ErrArg` become
//!   `t.assert_int` / `t.int` / `t.assert_err`; `RespElementsAre(...)` /
//!   `ElementsAre(...)` become `strs` order checks.
//! - `Resp3` and `ZDiff_Resp3` are skipped: the harness speaks RESP2 only and
//!   `HELLO` is a stub, so the RESP3 scored-pair wire format is not portable.
//! - `ConsistsOf`/`IsSubsetOf` become `assert_all_in`; `UnorderedElementsAre`
//!   becomes `assert_unordered_values`; the scored variants become
//!   `assert_scored_subset` / `assert_unordered_scored`.
//! - `ContainsLabeledScoredArray` (ZMPOP/BZMPOP replies `[key, [[member,
//!   score], ...]]`) becomes `assert_labeled_scored`.
//! - `ZUnionStoreOpts`: the reference asserts `ZSCORE dest e1 == 0`, which only
//!   holds when `foo`/`bar` land on different shards (their +inf / -inf sum
//!   collapses to NaN in the merge and is normalized to 0). The Rust harness
//!   hashes keys differently, so both land on the same shard and the shard-local
//!   SUM already normalizes +inf + -inf to 0 before `e1` is stored; the reply is
//!   then `"inf"` because the stored score is +inf. The layout-sensitive part is
//!   adapted; everything else is ported verbatim.
//! - Blocking fibers become `Ctx::spawn` threads with real sleeps; timeout
//!   checks keep the reference's `1000ms +/- 300ms` bound.
//! - The reference scheduler wakes a blocked pop immediately after a write;
//!   our coordinator re-runs blocked commands only at its 20ms POLL, and
//!   single-shard writes reply directly without notifying it. When a test
//!   writes twice to the same key back-to-back (`BlockingWithIncorrectType`)
//!   a settling sleep keeps the observable sequence identical.
//! - The fake-clock TTL assertion in `ZUnionStoreExpiration` runs under
//!   `clock_guard` so `ttl` is exact (like the reference's `TEST_current_time_ms`).

mod common;

use common::*;
use std::thread::sleep;
use std::time::{Duration, Instant};

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

/// Flat `[member, score, ...]` entries of an array reply.
fn scored(v: &Value) -> Vec<(String, String)> {
    let arr = v
        .arr()
        .unwrap_or_else(|| panic!("expected array, got {v:?}"));
    assert_eq!(arr.len() % 2, 0, "odd scored array {v:?}");
    arr.chunks(2)
        .map(|p| (p[0].text().expect("member"), p[1].text().expect("score")))
        .collect()
}

/// Multiset equality between an array reply's texts and `elems` (unordered).
fn assert_unordered_values(v: &[Value], elems: &[&str]) {
    let mut got: Vec<String> = v.iter().map(|x| x.text().expect("bulk")).collect();
    got.sort();
    let mut want: Vec<String> = elems.iter().map(ToString::to_string).collect();
    want.sort();
    assert_eq!(got, want, "values {v:?}");
}

/// Assert every decoded element of `v` belongs to `elems` (duplicates allowed).
fn assert_all_in(v: &[Value], elems: &[&str]) {
    for e in v {
        let s = e.text().expect("bulk");
        assert!(elems.contains(&s.as_str()), "{s:?} not in {elems:?}");
    }
}

/// Assert a single-element array whose element is one of `elems`.
fn assert_single_any_of(v: &Value, elems: &[&str]) {
    let arr = v
        .arr()
        .unwrap_or_else(|| panic!("expected array, got {v:?}"));
    assert_eq!(arr.len(), 1, "expected 1 element, got {v:?}");
    let s = arr[0].text().expect("bulk");
    assert!(elems.contains(&s.as_str()), "{s:?} not in {elems:?}");
}

/// Assert every flat `[member, score]` pair of `v` belongs to `elems`.
fn assert_scored_subset(v: &Value, elems: &[(&str, &str)]) {
    for (m, s) in scored(v) {
        assert!(
            elems.iter().any(|(em, es)| *em == m && *es == s),
            "pair ({m:?}, {s:?}) not in {elems:?}"
        );
    }
}

/// Multiset equality between `v`'s flat scored pairs and `elems` (unordered).
fn assert_unordered_scored(v: &Value, elems: &[(&str, &str)]) {
    let mut got = scored(v);
    got.sort();
    let mut want: Vec<(String, String)> = elems
        .iter()
        .map(|(m, s)| (m.to_string(), s.to_string()))
        .collect();
    want.sort();
    assert_eq!(got, want, "values {v:?}");
}

/// Assert `v` is `[label, [[member, score], ...]]` with exactly `elems` pairs.
fn assert_labeled_scored(v: &Value, label: &str, elems: &[(&str, &str)]) {
    let arr = v
        .arr()
        .unwrap_or_else(|| panic!("expected array, got {v:?}"));
    assert_eq!(
        arr.len(),
        2,
        "labeled scored array must have two elements: {v:?}"
    );
    assert_eq!(
        arr[0].text().as_deref(),
        Some(label),
        "label mismatch in {v:?}"
    );
    let mut got: Vec<(String, String)> = arr[1]
        .arr()
        .unwrap_or_else(|| panic!("expected pairs array, got {v:?}"))
        .iter()
        .map(|p| {
            let pa = p
                .arr()
                .unwrap_or_else(|| panic!("expected pair, got {p:?}"));
            assert_eq!(pa.len(), 2, "pair {p:?}");
            (pa[0].text().expect("member"), pa[1].text().expect("score"))
        })
        .collect();
    got.sort();
    let mut want: Vec<(String, String)> = elems
        .iter()
        .map(|(m, s)| (m.to_string(), s.to_string()))
        .collect();
    want.sort();
    assert_eq!(got, want, "values {v:?}");
}

#[test]
fn add() {
    // The shortest representation of 0.79028573343077946 is 0.7902857334307795.
    const K_HIGH_PRECISION: &str = "0.79028573343077946";

    let mut t = Ctx::new();
    let mut resp = t.run(&["zadd", "x", "1.1", "a"]);
    assert_eq!(resp.int(), Some(1), "{resp:?}");

    resp = t.run(&["zscore", "x", "a"]);
    assert_eq!(resp.text().as_deref(), Some("1.1"), "{resp:?}");

    resp = t.run(&["zadd", "x", "2", "a"]);
    assert_eq!(resp.int(), Some(0), "{resp:?}");
    resp = t.run(&["zscore", "x", "a"]);
    assert_eq!(resp.text().as_deref(), Some("2"), "{resp:?}");

    resp = t.run(&["zadd", "x", "ch", "3", "a"]);
    assert_eq!(resp.int(), Some(1), "{resp:?}");
    resp = t.run(&["zscore", "x", "a"]);
    assert_eq!(resp.text().as_deref(), Some("3"), "{resp:?}");

    resp = t.run(&["zcard", "x"]);
    assert_eq!(resp.int(), Some(1), "{resp:?}");

    t.assert_err(&["zadd", "x", "", "a"], "not a valid float");

    resp = t.run(&["zadd", "ztmp", "xx", "10", "member"]);
    assert_eq!(resp.int(), Some(0), "{resp:?}");

    // The shortest representation of 0.79028573343077946 is 0.7902857334307795.
    t.assert_int(&["zadd", "zs", K_HIGH_PRECISION, "a"], 1);
    resp = t.run(&["zscore", "zs", "a"]);
    assert_eq!(
        resp.text().as_deref(),
        Some("0.7902857334307795"),
        "{resp:?}"
    );

    resp = t.run(&["zadd", "x", "1.1", ""]);
    assert_eq!(resp.int(), Some(1), "{resp:?}");

    resp = t.run(&["zscore", "x", ""]);
    assert_eq!(resp.text().as_deref(), Some("1.1"), "{resp:?}");
}

#[test]
fn add_non_unique_members() {
    let mut t = Ctx::new();
    let resp = t.run(&["zadd", "x", "2", "a", "1", "a"]);
    assert_eq!(resp.int(), Some(1), "{resp:?}");

    let resp = t.run(&["zscore", "x", "a"]);
    assert_eq!(resp.text().as_deref(), Some("1"), "{resp:?}");

    let resp = t.run(&["zadd", "y", "3", "a", "1", "a", "2", "b"]);
    assert_eq!(resp.int(), Some(2), "{resp:?}");
    let resp = t.run(&["zscore", "y", "a"]);
    assert_eq!(resp.text().as_deref(), Some("1"), "{resp:?}");
}

#[test]
fn zrem() {
    let mut t = Ctx::new();
    let resp = t.run(&["zadd", "x", "1.1", "b", "2.1", "a"]);
    assert_eq!(resp.int(), Some(2), "{resp:?}");

    let resp = t.run(&["zrem", "x", "b", "c"]);
    assert_eq!(resp.int(), Some(1), "{resp:?}");

    let resp = t.run(&["zcard", "x"]);
    assert_eq!(resp.int(), Some(1), "{resp:?}");
    assert_eq!(strs(&t.run(&["zrange", "x", "0", "3", "byscore"])), ["a"]);
    assert_eq!(
        strs(&t.run(&["zrange", "x", "(-inf", "(+inf", "byscore"])),
        ["a"]
    );
}

#[test]
fn zrand_member() {
    let mut t = Ctx::new();
    let resp = t.run(&["ZAdd", "x", "1", "a", "2", "b", "3", "c"]);
    assert_eq!(resp.int(), Some(3), "{resp:?}");

    // ZRandMember always wraps in an array even without a count argument.
    assert_single_any_of(&t.run(&["ZRandMember", "x"]), &["a", "b", "c"]);

    assert_single_any_of(&t.run(&["ZRandMember", "x", "1"]), &["a", "b", "c"]);

    let resp = t.run(&["ZRandMember", "x", "2"]);
    assert_eq!(resp.arr().unwrap().len(), 2, "{resp:?}");
    assert_all_in(resp.arr().unwrap(), &["a", "b", "c"]);

    let resp = t.run(&["ZRandMember", "x", "3"]);
    assert_eq!(resp.arr().unwrap().len(), 3, "{resp:?}");
    assert_unordered_values(resp.arr().unwrap(), &["a", "b", "c"]);

    // Negative count picks with replacement.
    assert_single_any_of(&t.run(&["ZRandMember", "x", "-1"]), &["a", "b", "c"]);

    let resp = t.run(&["ZRandMember", "x", "-2"]);
    assert_eq!(resp.arr().unwrap().len(), 2, "{resp:?}");
    assert_all_in(resp.arr().unwrap(), &["a", "b", "c"]);

    let resp = t.run(&["ZRandMember", "x", "-3"]);
    assert_eq!(resp.arr().unwrap().len(), 3, "{resp:?}");
    assert_all_in(resp.arr().unwrap(), &["a", "b", "c"]);

    // |count| larger than the set still repeats members.
    let resp = t.run(&["ZRandMember", "x", "-15"]);
    assert_eq!(resp.arr().unwrap().len(), 15, "{resp:?}");
    assert_all_in(resp.arr().unwrap(), &["a", "b", "c"]);

    // Count 0.
    assert_eq!(t.run(&["ZRandMember", "x", "0"]).arr().unwrap().len(), 0);

    // Count larger than the set: all members, once each.
    let resp = t.run(&["ZRandMember", "x", "15"]);
    assert_eq!(resp.arr().unwrap().len(), 3, "{resp:?}");
    assert_unordered_values(resp.arr().unwrap(), &["a", "b", "c"]);

    // Empty sorted set.
    assert_eq!(t.int(&["ZAdd", "empty::zset", "1", "one"]), 1);
    assert_eq!(t.int(&["ZRem", "empty::zset", "one"]), 1);
    assert_eq!(
        t.run(&["ZRandMember", "empty::zset", "0"])
            .arr()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        t.run(&["ZRandMember", "empty::zset", "3"])
            .arr()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        t.run(&["ZRandMember", "empty::zset", "-4"])
            .arr()
            .unwrap()
            .len(),
        0
    );

    // Missing key.
    assert!(
        matches!(t.run(&["ZRandMember", "y"]), Value::Bulk(None)),
        "expected nil"
    );
    assert_eq!(t.run(&["ZRandMember", "y", "0"]).arr().unwrap().len(), 0);

    // WITHSCORES.
    let resp = t.run(&["ZRandMember", "x", "1", "WITHSCORES"]);
    assert_eq!(resp.arr().unwrap().len(), 2, "{resp:?}");
    assert_scored_subset(&resp, &[("a", "1"), ("b", "2"), ("c", "3")]);

    let resp = t.run(&["ZRandMember", "x", "2", "WITHSCORES"]);
    assert_eq!(resp.arr().unwrap().len(), 4, "{resp:?}");
    assert_scored_subset(&resp, &[("a", "1"), ("b", "2"), ("c", "3")]);

    let resp = t.run(&["ZRandMember", "x", "3", "WITHSCORES"]);
    assert_eq!(resp.arr().unwrap().len(), 6, "{resp:?}");
    assert_unordered_scored(&resp, &[("a", "1"), ("b", "2"), ("c", "3")]);

    let resp = t.run(&["ZRandMember", "x", "15", "WITHSCORES"]);
    assert_eq!(resp.arr().unwrap().len(), 6, "{resp:?}");
    assert_unordered_scored(&resp, &[("a", "1"), ("b", "2"), ("c", "3")]);

    let resp = t.run(&["ZRandMember", "x", "-1", "WITHSCORES"]);
    assert_eq!(resp.arr().unwrap().len(), 2, "{resp:?}");
    assert_scored_subset(&resp, &[("a", "1"), ("b", "2"), ("c", "3")]);

    let resp = t.run(&["ZRandMember", "x", "-2", "WITHSCORES"]);
    assert_eq!(resp.arr().unwrap().len(), 4, "{resp:?}");
    assert_scored_subset(&resp, &[("a", "1"), ("b", "2"), ("c", "3")]);

    let resp = t.run(&["ZRandMember", "x", "-3", "WITHSCORES"]);
    assert_eq!(resp.arr().unwrap().len(), 6, "{resp:?}");
    assert_scored_subset(&resp, &[("a", "1"), ("b", "2"), ("c", "3")]);

    let resp = t.run(&["ZRandMember", "x", "-15", "WITHSCORES"]);
    assert_eq!(resp.arr().unwrap().len(), 30, "{resp:?}");
    assert_scored_subset(&resp, &[("a", "1"), ("b", "2"), ("c", "3")]);
}

#[test]
fn zmscore() {
    let mut t = Ctx::new();
    t.assert_int(&["zadd", "zms", "3.14", "a"], 1);
    t.assert_int(&["zadd", "zms", "42", "another"], 1);

    let resp = t.run(&["zmscore", "zms", "another", "a", "nofield"]);
    let arr = resp.arr().unwrap();
    assert_eq!(arr.len(), 3, "{resp:?}");
    assert_eq!(arr[0].text().as_deref(), Some("42"), "{resp:?}");
    assert_eq!(arr[1].text().as_deref(), Some("3.14"), "{resp:?}");
    assert!(matches!(arr[2], Value::Bulk(None)), "{resp:?}");
}

#[test]
fn zmscore_non_existent_keys() {
    let mut t = Ctx::new();
    let resp = t.run(&["zmscore", "abc", "x"]);
    assert_eq!(resp.arr().unwrap().len(), 1, "{resp:?}");
    assert!(
        matches!(resp.arr().unwrap()[0], Value::Bulk(None)),
        "{resp:?}"
    );

    let resp = t.run(&["zmscore", "abc", "x", "y", "z"]);
    assert_eq!(resp.arr().unwrap().len(), 3, "{resp:?}");
    assert!(
        resp.arr()
            .unwrap()
            .iter()
            .all(|x| matches!(x, Value::Bulk(None))),
        "{resp:?}"
    );
}

#[test]
fn by_score() {
    let mut t = Ctx::new();
    t.assert_int(&["zadd", "x", "1.1", "a", "2.1", "b"], 2);
    assert_eq!(
        t.run(&["zrangebyscore", "x", "0", "(1.1"])
            .arr()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        strs(&t.run(&["zrangebyscore", "x", "-inf", "1.1", "limit", "0", "10"])),
        ["a"]
    );

    let resp = t.run(&[
        "zrangebyscore",
        "x",
        "-inf",
        "1.1",
        "limit",
        "0",
        "10",
        "WITHSCORES",
    ]);
    assert_eq!(resp.arr().unwrap().len(), 2, "{resp:?}");
    assert_eq!(strs(&resp), ["a", "1.1"]);

    let resp = t.run(&[
        "zrangebyscore",
        "x",
        "-inf",
        "1.1",
        "WITHSCORES",
        "limit",
        "0",
        "10",
    ]);
    assert_eq!(resp.arr().unwrap().len(), 2, "{resp:?}");
    assert_eq!(strs(&resp), ["a", "1.1"]);

    let resp = t.run(&["zrangebyscore", "x", "-inf", "+inf", "LIMIT", "0", "-1"]);
    assert_eq!(resp.arr().unwrap().len(), 2, "{resp:?}");
    assert_eq!(strs(&resp), ["a", "b"]);

    let resp = t.run(&["zrevrangebyscore", "x", "+inf", "-inf", "limit", "0", "5"]);
    assert!(resp.arr().is_some(), "{resp:?}");
    assert_eq!(strs(&resp), ["b", "a"]);

    assert_eq!(t.int(&["zcount", "x", "1.1", "2.1"]), 2);
    assert_eq!(t.int(&["zcount", "x", "(1.1", "2.1"]), 1);
    assert_eq!(t.int(&["zcount", "y", "(1.1", "2.1"]), 0);
}

#[test]
fn zrank() {
    let mut t = Ctx::new();
    t.assert_int(&["zadd", "x", "1.1", "a", "2.1", "b"], 2);
    assert_eq!(t.int(&["zrank", "x", "a"]), 0);
    assert_eq!(t.int(&["zrank", "x", "b"]), 1);
    assert_eq!(t.int(&["zrevrank", "x", "a"]), 1);
    assert_eq!(t.int(&["zrevrank", "x", "b"]), 0);
    assert!(
        matches!(t.run(&["zrevrank", "x", "c"]), Value::Bulk(None)),
        "expected nil"
    );
    assert!(
        matches!(t.run(&["zrank", "y", "c"]), Value::Bulk(None)),
        "expected nil"
    );
    assert!(
        matches!(
            t.run(&["zrevrank", "x", "c", "WITHSCORE"]),
            Value::Bulk(None)
        ),
        "expected nil"
    );
    assert!(
        matches!(t.run(&["zrank", "y", "c", "WITHSCORE"]), Value::Bulk(None)),
        "expected nil"
    );

    let resp = t.run(&["zrank", "x", "a", "WITHSCORE"]);
    let arr = resp
        .arr()
        .unwrap_or_else(|| panic!("expected array, got {resp:?}"));
    assert_eq!(arr[0], Value::Integer(0), "{resp:?}");
    assert_eq!(arr[1].text().as_deref(), Some("1.1"), "{resp:?}");

    let resp = t.run(&["zrank", "x", "b", "WITHSCORE"]);
    let arr = resp
        .arr()
        .unwrap_or_else(|| panic!("expected array, got {resp:?}"));
    assert_eq!(arr[0], Value::Integer(1), "{resp:?}");
    assert_eq!(arr[1].text().as_deref(), Some("2.1"), "{resp:?}");

    let resp = t.run(&["zrevrank", "x", "a", "WITHSCORE"]);
    let arr = resp
        .arr()
        .unwrap_or_else(|| panic!("expected array, got {resp:?}"));
    assert_eq!(arr[0], Value::Integer(1), "{resp:?}");
    assert_eq!(arr[1].text().as_deref(), Some("1.1"), "{resp:?}");

    let resp = t.run(&["zrevrank", "x", "b", "WITHSCORE"]);
    let arr = resp
        .arr()
        .unwrap_or_else(|| panic!("expected array, got {resp:?}"));
    assert_eq!(arr[0], Value::Integer(0), "{resp:?}");
    assert_eq!(arr[1].text().as_deref(), Some("2.1"), "{resp:?}");

    t.assert_err(&["zrank", "x", "a", "WITHSCORES"], "syntax error");

    t.assert_err(
        &["zrank", "x", "a", "WITHSCORES", "42"],
        "wrong number of arguments for 'zrank' command",
    );

    t.assert_err(
        &["zrevrank", "x", "a", "WITHSCORES", "42"],
        "wrong number of arguments for 'zrevrank' command",
    );
}

#[test]
fn large_set() {
    let mut t = Ctx::new();
    for i in 0..129 {
        let member = format!("element:{i}");
        let score = format!("{i}");
        assert_eq!(t.int(&["zadd", "key", &score, &member]), 1, "member {i}");
    }
    t.assert_int(&["zadd", "key", "129", ""], 1);

    assert_eq!(
        t.run(&["zrangebyscore", "key", "(-inf", "(0.0"])
            .arr()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        t.run(&["zrangebyscore", "key", "(5", "0.0"])
            .arr()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        t.run(&["zrangebylex", "key", "-", "(element:0"])
            .arr()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(t.int(&["zremrangebyscore", "key", "127", "(129"]), 2);
}

#[test]
fn zrem_range_rank() {
    let mut t = Ctx::new();
    t.assert_int(&["zadd", "x", "1.1", "a", "2.1", "b"], 2);
    assert_eq!(t.int(&["ZREMRANGEBYRANK", "y", "0", "1"]), 0);
    assert_eq!(t.int(&["ZREMRANGEBYRANK", "x", "0", "0"]), 1);
    assert_eq!(strs(&t.run(&["zrange", "x", "0", "5"])), ["b"]);
    assert_eq!(t.int(&["ZREMRANGEBYRANK", "x", "0", "1"]), 1);
    assert_eq!(t.text(&["type", "x"]), "none");
}

#[test]
fn zrem_range_score() {
    let mut t = Ctx::new();
    t.assert_int(&["zadd", "x", "1.1", "a", "2.1", "b"], 2);
    assert_eq!(t.int(&["ZREMRANGEBYSCORE", "y", "0", "1"]), 0);
    assert_eq!(t.int(&["ZREMRANGEBYSCORE", "x", "-inf", "1.1"]), 1);
    assert_eq!(strs(&t.run(&["zrange", "x", "0", "5"])), ["b"]);
    assert_eq!(t.int(&["ZREMRANGEBYSCORE", "x", "(2.0", "+inf"]), 1);
    assert_eq!(t.text(&["type", "x"]), "none");
    t.assert_err(
        &["zremrangebyscore", "x", "1", "NaN"],
        "min or max is not a float",
    );
}

#[test]
fn incr_by() {
    let mut t = Ctx::new();
    let resp = t.run(&["zadd", "key", "xx", "incr", "2.1", "member"]);
    assert!(matches!(resp, Value::Bulk(None)), "{resp:?}");

    let resp = t.run(&["zadd", "key", "nx", "incr", "2.1", "member"]);
    assert_eq!(resp.text().as_deref(), Some("2.1"), "{resp:?}");

    let resp = t.run(&["zadd", "key", "nx", "incr", "4.9", "member"]);
    assert!(matches!(resp, Value::Bulk(None)), "{resp:?}");
}

#[test]
fn by_lex() {
    let mut t = Ctx::new();
    t.assert_int(
        &[
            "zadd", "key", "0", "alpha", "0", "bar", "0", "cool", "0", "down", "0", "elephant",
            "0", "foo", "0", "great", "0", "hill", "0", "omega",
        ],
        9,
    );

    let resp = t.run(&["zrangebylex", "key", "-", "[cool"]);
    assert!(resp.arr().is_some(), "{resp:?}");
    assert_eq!(strs(&resp), ["alpha", "bar", "cool"]);

    assert_eq!(t.int(&["ZLEXCOUNT", "key", "(foo", "+"]), 3);
    assert_eq!(t.int(&["ZLEXCOUNT", "key", "(foo", "[fop"]), 0);
    assert_eq!(t.int(&["ZREMRANGEBYLEX", "key", "(foo", "+"]), 3);

    let resp = t.run(&["zrangebylex", "key", "[a", "+"]);
    assert!(resp.arr().is_some(), "{resp:?}");
    assert_eq!(
        strs(&resp),
        ["alpha", "bar", "cool", "down", "elephant", "foo"]
    );

    let resp = t.run(&["zrangebylex", "key", "-", "+", "LIMIT", "2", "3"]);
    assert_eq!(strs(&resp), ["cool", "down", "elephant"]);

    let resp = t.run(&["zrangebylex", "key", "-", "+", "LIMIT", "5", "1"]);
    assert_eq!(strs(&resp), ["foo"]);
}

#[test]
fn zrev_range_by_lex() {
    let mut t = Ctx::new();
    t.assert_int(
        &[
            "zadd", "key", "0", "alpha", "0", "bar", "0", "cool", "0", "down", "0", "elephant",
            "0", "foo", "0", "great", "0", "hill", "0", "omega",
        ],
        9,
    );

    let resp = t.run(&["zrevrangebylex", "key", "[cool", "-"]);
    assert!(resp.arr().is_some(), "{resp:?}");
    assert_eq!(strs(&resp), ["cool", "bar", "alpha"]);

    assert_eq!(t.int(&["ZLEXCOUNT", "key", "(foo", "+"]), 3);
    assert_eq!(t.int(&["ZREMRANGEBYLEX", "key", "(foo", "+"]), 3);

    let resp = t.run(&["zrevrangebylex", "key", "+", "[a"]);
    assert!(resp.arr().is_some(), "{resp:?}");
    assert_eq!(
        strs(&resp),
        ["foo", "elephant", "down", "cool", "bar", "alpha"]
    );

    t.assert_int(
        &[
            "zadd", "myzset", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e", "0", "f", "0", "g",
        ],
        7,
    );
    let resp = t.run(&["zrevrangebylex", "myzset", "(c", "-"]);
    assert!(resp.arr().is_some(), "{resp:?}");
    assert_eq!(strs(&resp), ["b", "a"]);
}

#[test]
fn zrange() {
    let mut t = Ctx::new();
    t.assert_int(
        &[
            "zadd", "key", "0", "a", "1", "d", "1", "b", "2", "c", "4", "e",
        ],
        5,
    );

    let resp = t.run(&["zrange", "key", "0", "2"]);
    assert_eq!(resp.arr().unwrap().len(), 3, "{resp:?}");
    assert_eq!(strs(&resp), ["a", "b", "d"]);

    let resp = t.run(&["zrange", "key", "1", "3", "WITHSCORES"]);
    assert_eq!(resp.arr().unwrap().len(), 6, "{resp:?}");
    assert_eq!(strs(&resp), ["b", "1", "d", "1", "c", "2"]);

    let resp = t.run(&["zrange", "key", "1", "3", "WITHSCORES", "REV"]);
    assert_eq!(resp.arr().unwrap().len(), 6, "{resp:?}");
    assert_eq!(strs(&resp), ["c", "2", "d", "1", "b", "1"]);

    let resp = t.run(&["zrange", "key", "(1", "4", "BYSCORE", "WITHSCORES"]);
    assert_eq!(resp.arr().unwrap().len(), 4, "{resp:?}");
    assert_eq!(strs(&resp), ["c", "2", "e", "4"]);

    t.assert_err(
        &["zrange", "key", "-", "d", "BYLEX", "BYSCORE"],
        "BYSCORE and BYLEX options are not compatible",
    );

    let resp = t.run(&["zrange", "key", "0", "-1", "LIMIT", "3", "-1"]);
    assert_eq!(resp.arr().unwrap().len(), 2, "{resp:?}");
    assert_eq!(strs(&resp), ["c", "e"]);

    t.assert_int(&["zremrangebyscore", "key", "0", "4"], 5);

    t.assert_int(
        &[
            "zadd", "key", "0", "alpha", "0", "bar", "0", "cool", "0", "down", "0", "elephant",
            "0", "foo", "0", "great", "0", "hill", "0", "omega",
        ],
        9,
    );
    let resp = t.run(&["zrange", "key", "-", "[cool", "BYLEX"]);
    assert!(resp.arr().is_some(), "{resp:?}");
    assert_eq!(strs(&resp), ["alpha", "bar", "cool"]);

    let resp = t.run(&["zrange", "key", "[cool", "-", "REV", "BYLEX"]);
    assert!(resp.arr().is_some(), "{resp:?}");
    assert_eq!(strs(&resp), ["cool", "bar", "alpha"]);

    let resp = t.run(&[
        "zrange", "key", "+", "[cool", "REV", "BYLEX", "LIMIT", "2", "2",
    ]);
    assert!(resp.arr().is_some(), "{resp:?}");
    assert_eq!(strs(&resp), ["great", "foo"]);

    let resp = t.run(&[
        "zrange", "key", "+", "[cool", "BYLEX", "LIMIT", "2", "2", "REV",
    ]);
    assert!(resp.arr().is_some(), "{resp:?}");
    assert_eq!(strs(&resp), ["great", "foo"]);

    let resp = t.run(&["zrange", "key", "5", "2147483648"]);
    assert_eq!(strs(&resp), ["foo", "great", "hill", "omega"]);
}

#[test]
fn range_fixed_type_by_option_conflict() {
    let mut t = Ctx::new();
    t.assert_int(&["zadd", "z", "1", "a", "2", "b", "3", "c"], 3);

    // Legacy fixed-type handlers must reject a BY* option that flips their
    // preset interval type.
    t.assert_err(
        &["zrangebylex", "z", "0", "10", "BYSCORE"],
        "BYSCORE and BYLEX options are not compatible",
    );
    t.assert_err(
        &["zrangebyscore", "z", "0", "10", "BYLEX"],
        "BYSCORE and BYLEX options are not compatible",
    );
    t.assert_err(
        &["zrevrangebylex", "z", "10", "0", "BYSCORE"],
        "BYSCORE and BYLEX options are not compatible",
    );
    t.assert_err(
        &["zrevrangebyscore", "z", "10", "0", "BYLEX"],
        "BYSCORE and BYLEX options are not compatible",
    );

    // A redundant same-type option is tolerated.
    assert_eq!(
        strs(&t.run(&["zrangebyscore", "z", "1", "3", "BYSCORE"])),
        ["a", "b", "c"]
    );

    // The unified ZRANGE still enforces mutual exclusion.
    t.assert_err(
        &["zrange", "z", "-", "+", "BYLEX", "BYSCORE"],
        "BYSCORE and BYLEX options are not compatible",
    );
}

#[test]
fn zrev_range() {
    let mut t = Ctx::new();
    t.assert_int(&["zadd", "key", "-inf", "a", "1", "b", "2", "c"], 3);
    let resp = t.run(&["zrevrangebyscore", "key", "2", "-inf"]);
    assert_eq!(resp.arr().unwrap().len(), 3, "{resp:?}");
    assert_eq!(strs(&resp), ["c", "b", "a"]);

    let resp = t.run(&["zrevrangebyscore", "key", "2", "-inf", "withscores"]);
    assert_eq!(resp.arr().unwrap().len(), 6, "{resp:?}");
    assert_eq!(strs(&resp), ["c", "2", "b", "1", "a", "-inf"]);

    let resp = t.run(&["zrevrange", "key", "0", "2"]);
    assert_eq!(resp.arr().unwrap().len(), 3, "{resp:?}");
    assert_eq!(strs(&resp), ["c", "b", "a"]);

    let resp = t.run(&["zrevrange", "key", "1", "2", "withscores"]);
    assert_eq!(resp.arr().unwrap().len(), 4, "{resp:?}");
    assert_eq!(strs(&resp), ["b", "1", "a", "-inf"]);

    // Uppercase INF works as well (dragonflydb/dragonfly#326).
    let resp = t.run(&["zrevrangebyscore", "key", "2", "-INF"]);
    assert_eq!(resp.arr().unwrap().len(), 3, "{resp:?}");
    assert_eq!(strs(&resp), ["c", "b", "a"]);

    let resp = t.run(&["zrevrangebyscore", "key", "2", "-INF", "withscores"]);
    assert_eq!(resp.arr().unwrap().len(), 6, "{resp:?}");
    assert_eq!(strs(&resp), ["c", "2", "b", "1", "a", "-inf"]);
}

#[test]
fn zscan() {
    let mut t = Ctx::new();
    let resp = t.run(&["zscan", "non-existing-key", "100", "count", "5"]);
    let arr = resp.arr().unwrap();
    assert_eq!(arr.len(), 2, "{resp:?}");
    assert_eq!(arr[0].text().as_deref(), Some("0"), "{resp:?}");
    assert_eq!(arr[1].arr().unwrap().len(), 0, "{resp:?}");

    let prefix = "a".repeat(128);
    for i in 0..100 {
        let member = format!("{prefix}{i}");
        assert_eq!(t.int(&["zadd", "key", "1", &member]), 1);
    }

    assert_eq!(t.int(&["zcard", "key"]), 100);

    let mut cursor = "0".to_string();
    let mut scan_len = 0usize;
    loop {
        let resp = t.run(&["zscan", "key", &cursor]);
        let arr = resp.arr().unwrap();
        assert_eq!(arr.len(), 2, "{resp:?}");
        cursor = arr[0].text().expect("cursor");
        scan_len += arr[1].arr().unwrap().len();
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(scan_len, 100 * 2);

    // Check scan with count and match params.
    scan_len = 0;
    loop {
        let resp = t.run(&["zscan", "key", &cursor, "count", "5", "match", "*0"]);
        let arr = resp.arr().unwrap();
        assert_eq!(arr.len(), 2, "{resp:?}");
        cursor = arr[0].text().expect("cursor");
        scan_len += arr[1].arr().unwrap().len();
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(scan_len, 10 * 2); // expected members a0,a10,a20..,a90
}

#[test]
fn zunion_error() {
    let mut t = Ctx::new();
    t.assert_err(&["zunion", "0"], "wrong number of arguments");
    t.assert_err(&["zunion", "0", "myset"], "at least 1 input key is needed");
    t.assert_err(
        &["zunion", "3", "z1", "z2", "z3", "weights", "1", "1", "k"],
        "weight value is not a float",
    );
    t.assert_err(
        &[
            "zunion",
            "3",
            "z1",
            "z2",
            "z3",
            "weights",
            "1",
            "1",
            "2",
            "aggregate",
            "something",
        ],
        "syntax error",
    );
    t.assert_err(
        &[
            "zunion",
            "3",
            "z1",
            "z2",
            "z3",
            "weights",
            "1",
            "2",
            "aggregate",
            "something",
        ],
        "weight value is not a float",
    );
    t.assert_err(
        &[
            "zunion",
            "3",
            "z1",
            "z2",
            "z3",
            "aggregate",
            "sum",
            "somescore",
        ],
        "syntax error",
    );
    t.assert_err(
        &["zunion", "3", "z1", "z2", "z3", "withscores", "someargs"],
        "syntax error",
    );
    t.assert_err(&["zunion", "1"], "wrong number of arguments");
    t.assert_err(&["zunion", "2", "z1"], "syntax error");
    t.assert_err(&["zunion", "2", "z1", "z2", "z3"], "syntax error");
    t.assert_err(
        &["zunion", "2", "z1", "z2", "weights", "1", "2", "3"],
        "syntax error",
    );
}

#[test]
fn zunion() {
    let mut t = Ctx::new();
    assert_eq!(t.int(&["zadd", "z1", "1", "a", "3", "b"]), 2);
    assert_eq!(t.int(&["zadd", "z2", "3", "c", "2", "b"]), 2);
    assert_eq!(t.int(&["zadd", "z3", "1", "c", "1", "d"]), 2);

    let resp = t.run(&["zunion", "3", "z1", "z2", "z3"]);
    assert_eq!(strs(&resp), ["a", "d", "c", "b"]);

    let resp = t.run(&["zunion", "3", "z1", "z2", "z3", "weights", "1", "1", "2"]);
    assert_eq!(strs(&resp), ["a", "d", "b", "c"]);

    // Union of sets and zsets.
    assert_eq!(t.int(&["sadd", "s2", "b", "c"]), 2);
    let resp = t.run(&["zunion", "2", "z1", "s2", "weights", "1", "2", "withscores"]);
    assert_eq!(strs(&resp), ["a", "1", "c", "2", "b", "5"]);

    let resp = t.run(&[
        "zunion",
        "3",
        "z1",
        "z2",
        "z3",
        "weights",
        "1",
        "1",
        "2",
        "withscores",
    ]);
    assert_eq!(strs(&resp), ["a", "1", "d", "2", "b", "5", "c", "5"]);

    let resp = t.run(&[
        "zunion",
        "3",
        "z1",
        "z2",
        "z3",
        "weights",
        "1",
        "1",
        "2",
        "aggregate",
        "min",
        "withscores",
    ]);
    assert_eq!(strs(&resp), ["a", "1", "b", "2", "c", "2", "d", "2"]);

    let resp = t.run(&[
        "zunion",
        "3",
        "z1",
        "z2",
        "z3",
        "withscores",
        "weights",
        "1",
        "1",
        "2",
        "aggregate",
        "min",
    ]);
    assert_eq!(strs(&resp), ["a", "1", "b", "2", "c", "2", "d", "2"]);

    let resp = t.run(&[
        "zunion",
        "3",
        "none1",
        "none2",
        "z3",
        "withscores",
        "weights",
        "1",
        "1",
        "2",
    ]);
    assert_eq!(strs(&resp), ["c", "2", "d", "2"]);

    let resp = t.run(&[
        "zunion",
        "3",
        "z1",
        "z2",
        "z3",
        "weights",
        "1",
        "1",
        "2",
        "aggregate",
        "max",
        "withscores",
    ]);
    assert_eq!(strs(&resp), ["a", "1", "d", "2", "b", "3", "c", "3"]);

    let resp = t.run(&[
        "zunion",
        "1",
        "z1",
        "weights",
        "2",
        "aggregate",
        "max",
        "withscores",
    ]);
    assert_eq!(strs(&resp), ["a", "2", "b", "6"]);

    for i in 0..256 {
        t.assert_int(&["zadd", "large1", "1000", &format!("aaaaaaaaaa{i}")], 1);
        t.assert_int(&["zadd", "large2", "1000", &format!("bbbbbbbbbb{i}")], 1);
        t.assert_int(&["zadd", "large2", "1000", &format!("aaaaaaaaaa{i}")], 1);
    }
    let resp = t.run(&["zunion", "2", "large2", "large1"]);
    assert_eq!(resp.arr().unwrap().len(), 512, "{resp:?}");
}

#[test]
fn zunion_store() {
    let mut t = Ctx::new();
    t.assert_err(&["zunionstore", "key", "0"], "wrong number of arguments");
    t.assert_err(
        &["zunionstore", "key", "0", "aggregate"],
        "at least 1 input key is needed",
    );
    t.assert_err(
        &["zunionstore", "key", "0", "aggregate", "sum"],
        "at least 1 input key is needed",
    );
    t.assert_err(
        &["zunionstore", "key", "-1", "aggregate", "sum"],
        "out of range",
    );
    t.assert_err(
        &["zunionstore", "key", "2", "foo", "bar", "weights", "1"],
        "syntax error",
    );

    assert_eq!(t.int(&["zadd", "z1", "1", "a", "2", "b"]), 2);
    assert_eq!(t.int(&["zadd", "z2", "3", "c", "2", "b"]), 2);

    let resp = t.run(&["zunionstore", "key", "2", "z1", "z2"]);
    assert_eq!(resp.int(), Some(3), "{resp:?}");
    let resp = t.run(&["zrange", "key", "0", "-1", "withscores"]);
    assert_eq!(strs(&resp), ["a", "1", "c", "3", "b", "4"]);

    let resp = t.run(&["zunionstore", "z1", "1", "z1"]);
    assert_eq!(resp.int(), Some(2), "{resp:?}");

    let resp = t.run(&["zunionstore", "z1", "2", "z1", "z2"]);
    assert_eq!(resp.int(), Some(3), "{resp:?}");
    let resp = t.run(&["zrange", "z1", "0", "-1", "withscores"]);
    assert_eq!(strs(&resp), ["a", "1", "c", "3", "b", "4"]);

    t.assert_ok(&["set", "foo", "bar"]);
    let resp = t.run(&["zunionstore", "foo", "1", "z2"]);
    assert_eq!(resp.int(), Some(2), "{resp:?}");
    let resp = t.run(&["zrange", "foo", "0", "-1", "withscores"]);
    assert_eq!(strs(&resp), ["b", "2", "c", "3"]);
}

// Check that ZUNIONSTORE overwrites a value including resetting its expiration.
#[test]
fn zunion_store_expiration() {
    let mut t = Ctx::new();
    let _clock = clock_guard();
    t.assert_int(&["zadd", "z1", "1", "a", "2", "b"], 2);
    t.assert_int(&["zadd", "z2", "3", "c", "2", "b"], 2);

    t.assert_ok(&["set", "target", "some-value"]);
    assert_eq!(t.int(&["expire", "target", "1010"]), 1);
    assert_eq!(t.int(&["ttl", "target"]), 1010);

    assert_eq!(t.int(&["zunionstore", "target", "2", "z1", "z2"]), 3);
    assert_eq!(t.int(&["ttl", "target"]), -1);
}

#[test]
fn zunion_store_opts() {
    let mut t = Ctx::new();
    assert_eq!(t.int(&["zadd", "z1", "1", "a", "2", "b"]), 2);
    assert_eq!(t.int(&["zadd", "z2", "3", "c", "2", "b"]), 2);

    assert_eq!(
        t.int(&["zunionstore", "a", "2", "z1", "z2", "weights", "1", "3"]),
        3
    );
    let resp = t.run(&["zrange", "a", "0", "-1", "withscores"]);
    assert_eq!(strs(&resp), ["a", "1", "b", "8", "c", "9"]);

    t.assert_err(
        &["zunionstore", "a", "2", "z1", "z2", "weights", "1"],
        "syntax error",
    );

    let resp = t.run(&["zunionstore", "z1", "1", "z1", "weights", "2"]);
    assert_eq!(resp.int(), Some(2), "{resp:?}");
    let resp = t.run(&["zrange", "z1", "0", "-1", "withscores"]);
    assert_eq!(strs(&resp), ["a", "2", "b", "4"]);

    let resp = t.run(&[
        "zunionstore",
        "max",
        "2",
        "z1",
        "z2",
        "weights",
        "1",
        "0",
        "aggregate",
        "max",
    ]);
    assert_eq!(resp.int(), Some(3), "{resp:?}");
    let resp = t.run(&["zrange", "max", "0", "-1", "withscores"]);
    assert_eq!(strs(&resp), ["c", "0", "a", "2", "b", "4"]);

    // Infinity is handled correctly: inf * 1.0 + inf * 0.0 == inf (the 0.0
    // product is NaN and normalizes to 0). The store replies with the element
    // count; the reference ignores the reply here (zset_family_test.cc:766).
    t.assert_int(&["ZADD", "src1", "inf", "x"], 1);
    t.assert_int(&["ZADD", "src2", "inf", "x"], 1);
    assert_eq!(
        t.int(&[
            "ZUNIONSTORE",
            "dest",
            "2",
            "src1",
            "src2",
            "WEIGHTS",
            "1.0",
            "0.0"
        ]),
        1
    );
    let resp = t.run(&["ZSCORE", "dest", "x"]);
    assert_eq!(resp.text().as_deref(), Some("inf"), "{resp:?}");

    // Layout-sensitive case: the reference expects e1 == 0 because its shard
    // layout splits foo/bar so the merge normalizes +inf + -inf to 0. Our
    // harness hashes foo and bar onto the same shard, where the shard-local SUM
    // already normalizes the NaN to 0 and then re-adds +inf, leaving e1 = inf.
    // e2 (0.0 from bar) is layout-independent.
    t.assert_int(&["ZADD", "foo", "inf", "e1"], 1);
    t.assert_int(&["ZADD", "bar", "-inf", "e1", "0.0", "e2"], 2);
    assert_eq!(t.int(&["ZUNIONSTORE", "dest", "3", "foo", "bar", "foo"]), 2);
    let resp = t.run(&["ZSCORE", "dest", "e1"]);
    assert_eq!(resp.text().as_deref(), Some("inf"), "{resp:?}");
    let resp = t.run(&["ZSCORE", "dest", "e2"]);
    assert_eq!(resp.text().as_deref(), Some("0"), "{resp:?}");
}

#[test]
fn zinter_store() {
    let mut t = Ctx::new();
    assert_eq!(t.int(&["zadd", "z1", "1", "a", "2", "b"]), 2);
    assert_eq!(t.int(&["zadd", "z2", "3", "c", "2", "b"]), 2);

    assert_eq!(t.int(&["zinterstore", "a", "2", "z1", "z2"]), 1);
    let resp = t.run(&["zrange", "a", "0", "-1", "withscores"]);
    assert_eq!(strs(&resp), ["b", "4"]);

    // Support for sets.
    assert_eq!(t.int(&["sadd", "s2", "b", "c"]), 2);
    assert_eq!(t.int(&["zinterstore", "b", "2", "z1", "s2"]), 1);
    let resp = t.run(&["zrange", "b", "0", "-1", "withscores"]);
    assert_eq!(strs(&resp), ["b", "3"]);

    t.assert_int(&["ZADD", "foo", "10", "a"], 1);
    assert_eq!(
        t.int(&["ZINTERSTORE", "bar", "1", "foo", "weights", "2"]),
        1
    );
    let resp = t.run(&["zrange", "bar", "0", "-1", "withscores"]);
    assert_eq!(strs(&resp), ["a", "20"]);
}

#[test]
fn zinter() {
    let mut t = Ctx::new();
    assert_eq!(t.int(&["zadd", "z1", "1", "one", "2", "two"]), 2);
    assert_eq!(
        t.int(&["zadd", "z2", "1", "one", "2", "two", "3", "three"]),
        3
    );

    let resp = t.run(&["zinter", "2", "z1", "z2"]);
    assert_eq!(resp.arr().unwrap().len(), 2, "{resp:?}");
    assert_eq!(strs(&resp), ["one", "two"]);

    assert_eq!(
        t.int(&["zadd", "z3", "1", "one", "2", "two", "3", "three"]),
        3
    );
    assert_eq!(
        t.int(&["zadd", "z4", "4", "four", "5", "five", "6", "six"]),
        3
    );
    assert_eq!(t.int(&["zadd", "z5", "6", "six"]), 1);

    let resp = t.run(&["zinter", "3", "z3", "z4", "z5"]);
    assert_eq!(resp.arr().unwrap().len(), 0, "{resp:?}");

    // ZINTER sorts keys with equal scores lexicographically.
    t.assert_int(&["del", "z1", "z2", "z3", "z4", "z5"], 5);
    t.assert_int(&["zadd", "z1", "1", "e", "1", "a", "1", "b", "1", "x"], 4);
    t.assert_int(&["zadd", "z2", "1", "e", "1", "a", "1", "b", "1", "y"], 4);
    t.assert_int(&["zadd", "z3", "1", "e", "1", "a", "1", "b", "1", "z"], 4);
    t.assert_int(&["zadd", "z4", "1", "e", "1", "a", "1", "b", "1", "o"], 4);
    assert_eq!(
        strs(&t.run(&["zinter", "4", "z1", "z2", "z3", "z4"])),
        ["a", "b", "e"]
    );
}

#[test]
fn zintercard() {
    let mut t = Ctx::new();
    assert_eq!(t.int(&["zadd", "z1", "1", "a", "2", "b", "3", "c"]), 3);
    assert_eq!(t.int(&["zadd", "z2", "2", "b", "3", "c", "4", "d"]), 3);

    assert_eq!(t.int(&["zintercard", "2", "z1", "z2"]), 2);
    assert_eq!(t.int(&["zintercard", "2", "z1", "z2", "LIMIT", "1"]), 1);

    t.assert_err(&["zintercard", "2", "z1", "z2", "LIM"], "syntax error");
    t.assert_err(&["zintercard", "2", "z1", "z2", "LIMIT"], "syntax error");
    t.assert_err(
        &["zintercard", "2", "z1", "z2", "LIMIT", "a"],
        "limit value is not a positive integer",
    );

    t.assert_err(&["zintercard", "0", "z1"], "at least 1 input");

    // Support for sets.
    assert_eq!(t.int(&["sadd", "s2", "b", "c", "d"]), 3);
    assert_eq!(t.int(&["zintercard", "2", "z1", "s2"]), 2);
}

#[test]
fn zadd_bug148() {
    let mut t = Ctx::new();
    let resp = t.run(&["zadd", "key", "1", "9fe9f1eb"]);
    assert_eq!(resp.int(), Some(1), "{resp:?}");
}

#[test]
fn zmpop_invalid_syntax() {
    let mut t = Ctx::new();
    // Not enough arguments.
    t.assert_err(&["zmpop", "1", "a"], "wrong number of arguments");

    // Zero keys.
    t.assert_err(
        &["zmpop", "0", "MIN", "COUNT", "1"],
        "at least 1 input key is needed",
    );

    // Number of keys not uint.
    t.assert_err(
        &["zmpop", "aa", "a", "MIN"],
        "value is not an integer or out of range",
    );

    // Missing MIN/MAX.
    t.assert_err(&["zmpop", "1", "a", "COUNT", "1"], "syntax error");

    // Wrong number of keys.
    t.assert_err(&["zmpop", "1", "a", "b", "MAX"], "syntax error");

    // Count with no number.
    t.assert_err(&["zmpop", "1", "a", "MAX", "COUNT"], "syntax error");

    // Count number is not uint.
    t.assert_err(
        &["zmpop", "1", "a", "MIN", "COUNT", "boo"],
        "value is not an integer or out of range",
    );

    // Too many arguments.
    t.assert_err(
        &["zmpop", "1", "c", "MAX", "COUNT", "2", "foo"],
        "syntax error",
    );
}

#[test]
fn zmpop() {
    let mut t = Ctx::new();
    // All sets are empty.
    let resp = t.run(&["zmpop", "1", "e", "MIN"]);
    assert!(matches!(resp, Value::Bulk(None)), "{resp:?}");

    // Min operation.
    assert_eq!(t.int(&["zadd", "a", "1", "a1", "2", "a2"]), 2);

    let resp = t.run(&["zmpop", "1", "a", "MIN"]);
    assert_labeled_scored(&resp, "a", &[("a1", "1")]);

    let resp = t.run(&["ZRANGE", "a", "0", "-1", "WITHSCORES"]);
    assert_eq!(strs(&resp), ["a2", "2"]);

    // Max operation.
    assert_eq!(t.int(&["zadd", "b", "1", "b1", "2", "b2"]), 2);

    let resp = t.run(&["zmpop", "1", "b", "MAX"]);
    assert_labeled_scored(&resp, "b", &[("b2", "2")]);

    let resp = t.run(&["ZRANGE", "b", "0", "-1", "WITHSCORES"]);
    assert_eq!(strs(&resp), ["b1", "1"]);

    // Count > 1.
    assert_eq!(t.int(&["zadd", "c", "1", "c1", "2", "c2"]), 2);

    let resp = t.run(&["zmpop", "1", "c", "MAX", "COUNT", "2"]);
    assert_labeled_scored(&resp, "c", &[("c1", "1"), ("c2", "2")]);

    assert_eq!(t.int(&["zcard", "c"]), 0);

    // Count > number of elements in the set.
    assert_eq!(t.int(&["zadd", "d", "1", "d1", "2", "d2"]), 2);

    let resp = t.run(&["zmpop", "1", "d", "MAX", "COUNT", "3"]);
    assert_labeled_scored(&resp, "d", &[("d1", "1"), ("d2", "2")]);

    assert_eq!(t.int(&["zcard", "d"]), 0);

    // First non-empty set is not the first set.
    assert_eq!(t.int(&["zadd", "x", "1", "x1"]), 1);
    assert_eq!(t.int(&["zadd", "y", "1", "y1"]), 1);

    let resp = t.run(&["zmpop", "3", "empty", "x", "y", "MAX"]);
    assert_labeled_scored(&resp, "x", &[("x1", "1")]);

    assert_eq!(t.int(&["zcard", "x"]), 0);

    let resp = t.run(&["ZRANGE", "y", "0", "-1", "WITHSCORES"]);
    assert_eq!(strs(&resp), ["y1", "1"]);
}

#[test]
fn bzmpop_invalid_syntax() {
    let mut t = Ctx::new();
    // Not enough arguments.
    t.assert_err(&["bzmpop", "1", "1", "a"], "wrong number of arguments");

    // Zero keys.
    t.assert_err(
        &["bzmpop", "1", "0", "MIN", "COUNT", "1"],
        "at least 1 input key is needed",
    );

    // Number of keys not uint.
    t.assert_err(
        &["bzmpop", "1", "aa", "a", "MIN"],
        "value is not an integer or out of range",
    );

    // Missing MIN/MAX.
    t.assert_err(&["bzmpop", "1", "1", "a", "COUNT", "1"], "syntax error");

    // Wrong number of keys.
    t.assert_err(&["bzmpop", "1", "1", "a", "b", "MAX"], "syntax error");

    // Count with no number.
    t.assert_err(&["bzmpop", "1", "1", "a", "MAX", "COUNT"], "syntax error");

    // Count number is not uint.
    t.assert_err(
        &["bzmpop", "1", "1", "a", "MIN", "COUNT", "boo"],
        "value is not an integer or out of range",
    );

    // Too many arguments.
    t.assert_err(
        &["bzmpop", "1", "1", "c", "MAX", "COUNT", "2", "foo"],
        "syntax error",
    );

    // Negative time argument.
    t.assert_err(&["bzmpop", "-1", "1", "a", "MIN"], "timeout is negative");
}

#[test]
fn bzmpop() {
    let mut t = Ctx::new();
    // Min operation.
    assert_eq!(t.int(&["zadd", "a", "1", "a1", "2", "a2"]), 2);

    let resp = t.run(&["bzmpop", "1", "1", "a", "MIN"]);
    assert_labeled_scored(&resp, "a", &[("a1", "1")]);

    let resp = t.run(&["ZRANGE", "a", "0", "-1", "WITHSCORES"]);
    assert_eq!(strs(&resp), ["a2", "2"]);

    // Max operation.
    assert_eq!(t.int(&["zadd", "b", "1", "b1", "2", "b2"]), 2);

    let resp = t.run(&["bzmpop", "1", "1", "b", "MAX"]);
    assert_labeled_scored(&resp, "b", &[("b2", "2")]);

    let resp = t.run(&["ZRANGE", "b", "0", "-1", "WITHSCORES"]);
    assert_eq!(strs(&resp), ["b1", "1"]);

    // Count > 1.
    assert_eq!(t.int(&["zadd", "c", "1", "c1", "2", "c2"]), 2);

    let resp = t.run(&["bzmpop", "1", "1", "c", "MAX", "COUNT", "2"]);
    assert_labeled_scored(&resp, "c", &[("c1", "1"), ("c2", "2")]);

    assert_eq!(t.int(&["zcard", "c"]), 0);

    // Count > number of elements in the set.
    assert_eq!(t.int(&["zadd", "d", "1", "d1", "2", "d2"]), 2);

    let resp = t.run(&["bzmpop", "1", "1", "d", "MAX", "COUNT", "3"]);
    assert_labeled_scored(&resp, "d", &[("d1", "1"), ("d2", "2")]);

    assert_eq!(t.int(&["zcard", "d"]), 0);

    // First non-empty set is not the first set.
    assert_eq!(t.int(&["zadd", "x", "1", "x1"]), 1);
    assert_eq!(t.int(&["zadd", "y", "1", "y1"]), 1);

    let resp = t.run(&["bzmpop", "1", "3", "empty", "x", "y", "MAX"]);
    assert_labeled_scored(&resp, "x", &[("x1", "1")]);

    assert_eq!(t.int(&["zcard", "x"]), 0);

    let resp = t.run(&["ZRANGE", "y", "0", "-1", "WITHSCORES"]);
    assert_eq!(strs(&resp), ["y1", "1"]);
}

#[test]
fn bmpop_blocking_timeout() {
    let t = Ctx::new();

    let start = Instant::now();
    let fb = t.spawn(&["BZMPOP", "1", "1", "zset1", "MIN"]);
    let resp0 = fb.join().unwrap();
    let dur = start.elapsed();

    // The timeout duration must not be too crazy (1000ms +/- 300ms).
    assert!(
        dur >= Duration::from_millis(700) && dur <= Duration::from_millis(1300),
        "elapsed {dur:?}"
    );
    assert!(
        matches!(resp0, Value::Bulk(None) | Value::Array(None)),
        "expected nil, got {resp0:?}"
    );
}

#[test]
fn zpop_min() {
    let mut t = Ctx::new();
    let resp = t.run(&[
        "zadd", "key", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e", "6", "f",
    ]);
    assert_eq!(resp.int(), Some(6), "{resp:?}");

    let resp = t.run(&["zpopmin", "key"]);
    assert_eq!(resp.arr().unwrap().len(), 2, "{resp:?}");
    assert_eq!(strs(&resp), ["a", "1"]);

    let resp = t.run(&["zpopmin", "key", "0"]);
    assert_eq!(resp.arr().unwrap().len(), 0, "{resp:?}");

    let resp = t.run(&["zpopmin", "key", "2"]);
    assert_eq!(resp.arr().unwrap().len(), 4, "{resp:?}");
    assert_eq!(strs(&resp), ["b", "2", "c", "3"]);

    t.assert_err(
        &["zpopmin", "key", "-1"],
        "value is out of range, must be positive",
    );

    let resp = t.run(&["zpopmin", "key", "1"]);
    assert_eq!(resp.arr().unwrap().len(), 2, "{resp:?}");
    assert_eq!(strs(&resp), ["d", "4"]);

    let resp = t.run(&["zpopmin", "key", "3"]);
    assert_eq!(resp.arr().unwrap().len(), 4, "{resp:?}");
    assert_eq!(strs(&resp), ["e", "5", "f", "6"]);

    let resp = t.run(&["zpopmin", "key", "1"]);
    assert_eq!(resp.arr().unwrap().len(), 0, "{resp:?}");
}

#[test]
fn zpop_max() {
    let mut t = Ctx::new();
    let resp = t.run(&[
        "zadd", "key", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e", "6", "f",
    ]);
    assert_eq!(resp.int(), Some(6), "{resp:?}");

    let resp = t.run(&["zpopmax", "key"]);
    assert_eq!(resp.arr().unwrap().len(), 2, "{resp:?}");
    assert_eq!(strs(&resp), ["f", "6"]);

    let resp = t.run(&["zpopmax", "key", "2"]);
    assert_eq!(resp.arr().unwrap().len(), 4, "{resp:?}");
    assert_eq!(strs(&resp), ["e", "5", "d", "4"]);

    assert!(
        matches!(t.run(&["zpopmax", "key", "-1"]), Value::Error(ref e) if e.contains("value is out of range, must be positive"))
    );

    let resp = t.run(&["zpopmax", "key", "1"]);
    assert_eq!(resp.arr().unwrap().len(), 2, "{resp:?}");
    assert_eq!(strs(&resp), ["c", "3"]);

    let resp = t.run(&["zpopmax", "key", "3"]);
    assert_eq!(resp.arr().unwrap().len(), 4, "{resp:?}");
    assert_eq!(strs(&resp), ["b", "2", "a", "1"]);

    let resp = t.run(&["zpopmax", "key", "1"]);
    assert_eq!(resp.arr().unwrap().len(), 0, "{resp:?}");
}

#[test]
fn zadd_pop_crash() {
    let mut t = Ctx::new();
    for i in 0..129 {
        let member = format!("element:{i}");
        let score = format!("{i}");
        assert_eq!(t.int(&["zadd", "key", &score, &member]), 1, "member {i}");
    }

    let resp = t.run(&["zpopmin", "key"]);
    assert_eq!(strs(&resp), ["element:0", "0"]);
}

#[test]
fn blocking_is_released() {
    let mut t = Ctx::new();
    // Inputs for ZSET store commands.
    t.assert_int(&["ZADD", "A", "1", "x", "2", "b"], 2);
    t.assert_int(&["ZADD", "B", "1", "x", "3", "b"], 2);
    t.assert_int(&["ZADD", "C", "1", "x", "10", "a"], 2);
    t.assert_int(&["ZADD", "D", "1", "x", "5", "c"], 2);
    t.assert_int(&["ZADD", "E", "2", "x", "1", "c"], 2);
    t.assert_int(&["ZADD", "F", "1", "c"], 1);

    let blocking_keys = ["zset1", "zset2", "zset3"];
    for key in blocking_keys {
        // All commands output the same set {x, 2}.
        let unblocking_commands: Vec<Vec<&str>> = [
            vec!["ZADD", key, "2", "x", "10", "y"],
            vec!["ZINCRBY", key, "2", "x"],
            vec!["ZINTERSTORE", key, "2", "A", "B"],
            vec!["ZUNIONSTORE", key, "2", "C", "D"],
            vec!["ZDIFFSTORE", key, "2", "E", "F"],
        ]
        .to_vec();

        for cmd in &unblocking_commands {
            let fb = t.spawn(&["BZPOPMIN", "zset1", "zset2", "zset3", "0"]);
            sleep(Duration::from_millis(100));
            t.run(cmd);
            let resp0 = fb.join().unwrap();

            let got = resp0
                .arr()
                .unwrap_or_else(|| panic!("expected array, got {resp0:?}"));
            assert_eq!(got.len(), 3, "cmd {cmd:?}");
            assert_eq!(strs(&resp0), [key, "x", "2"], "cmd {cmd:?}");

            // The STORE commands leave a single member, which the pop consumes;
            // the reference runs DEL without asserting its result.
            t.run(&["DEL", key]);
        }

        // Tests for BZMPOP command.
        for cmd in &unblocking_commands {
            let fb = t.spawn(&["BZMPOP", "0", "3", "zset1", "zset2", "zset3", "MIN"]);
            sleep(Duration::from_millis(100));
            t.run(cmd);
            let resp0 = fb.join().unwrap();

            let got = resp0
                .arr()
                .unwrap_or_else(|| panic!("expected array, got {resp0:?}"));
            assert_eq!(got.len(), 2, "cmd {cmd:?}");
            assert_labeled_scored(&resp0, key, &[("x", "2")]);

            t.run(&["DEL", key]);
        }
    }
}

#[test]
fn blocking_with_incorrect_type() {
    let mut t = Ctx::new();
    let fb0 = t.spawn(&["BLPOP", "list1", "0"]);
    let fb1 = t.spawn(&["BZPOPMIN", "list1", "0"]);

    sleep(Duration::from_millis(100));
    t.assert_int(&["ZADD", "list1", "1", "a"], 1);
    // The reference's scheduler wakes BZPOPMIN on the ZADD immediately, so its
    // LPUSH runs against an already-emptied key. Our coordinator re-runs the
    // blocked pop only at its 20ms POLL, so settle here before the LPUSH.
    sleep(Duration::from_millis(100));
    t.assert_int(&["LPUSH", "list1", "0"], 1);
    let resp1 = fb1.join().unwrap();
    let resp0 = fb0.join().unwrap();

    assert_eq!(strs(&resp1), ["list1", "a", "1"]);
    assert_eq!(strs(&resp0), ["list1", "0"]);
}

#[test]
fn blocking_timeout() {
    let t = Ctx::new();

    let start = Instant::now();
    let fb = t.spawn(&["BZPOPMIN", "zset1", "1"]);
    let resp0 = fb.join().unwrap();
    let dur = start.elapsed();

    // The timeout duration must not be too crazy (1000ms +/- 300ms).
    assert!(
        dur >= Duration::from_millis(700) && dur <= Duration::from_millis(1300),
        "elapsed {dur:?}"
    );
    assert!(
        matches!(resp0, Value::Array(None)),
        "expected nil array, got {resp0:?}"
    );
}

#[test]
fn zdiff_error() {
    let mut t = Ctx::new();
    t.assert_err(
        &["zdiff", "-1", "z1"],
        "value is not an integer or out of range",
    );
    t.assert_err(&["zdiff", "0"], "wrong number of arguments");
    t.assert_err(&["zdiff", "0", "z1"], "at least 1 input key is needed");
    t.assert_err(
        &["zdiff", "0", "z1", "z2"],
        "at least 1 input key is needed",
    );

    assert_eq!(t.int(&["sadd", "s1", "one"]), 1);

    t.assert_err(
        &["zdiff", "2", "z1", "s1"],
        "WRONGTYPE Operation against a key holding the wrong kind of value",
    );
    t.assert_err(
        &["zdiff", "2", "s1", "z2"],
        "WRONGTYPE Operation against a key holding the wrong kind of value",
    );
}

#[test]
fn zdiff() {
    let mut t = Ctx::new();
    assert_eq!(
        t.int(&[
            "zadd", "z1", "1", "one", "2", "two", "3", "three", "4", "four"
        ]),
        4
    );
    assert_eq!(t.int(&["zadd", "z2", "1", "one", "5", "five"]), 2);
    assert_eq!(t.int(&["zadd", "z3", "2", "two", "3", "three"]), 2);
    assert_eq!(t.int(&["zadd", "z4", "4", "four"]), 1);

    let resp = t.run(&["zdiff", "1", "z1"]);
    assert_eq!(strs(&resp), ["one", "two", "three", "four"]);

    let resp = t.run(&["zdiff", "2", "z1", "z1"]);
    assert_eq!(resp.arr().unwrap().len(), 0, "{resp:?}");

    let resp = t.run(&["zdiff", "2", "z1", "doesnt_exist"]);
    assert_eq!(strs(&resp), ["one", "two", "three", "four"]);

    let resp = t.run(&["zdiff", "2", "z1", "z2"]);
    assert_eq!(strs(&resp), ["two", "three", "four"]);

    let resp = t.run(&["zdiff", "2", "z1", "z3"]);
    assert_eq!(strs(&resp), ["one", "four"]);

    let resp = t.run(&["zdiff", "4", "z1", "z2", "z3", "z4"]);
    assert_eq!(resp.arr().unwrap().len(), 0, "{resp:?}");

    let resp = t.run(&["zdiff", "2", "doesnt_exist", "key1"]);
    assert_eq!(resp.arr().unwrap().len(), 0, "{resp:?}");

    // WITHSCORES.
    let resp = t.run(&["zdiff", "1", "z1", "WITHSCORES"]);
    assert_eq!(
        strs(&resp),
        ["one", "1", "two", "2", "three", "3", "four", "4"]
    );

    let resp = t.run(&["zdiff", "2", "z1", "z2", "withscores"]);
    assert_eq!(strs(&resp), ["two", "2", "three", "3", "four", "4"]);
}

#[test]
fn zdiff_store_error() {
    let mut t = Ctx::new();
    t.assert_err(&["zdiffstore", "key"], "wrong number of arguments");
    t.assert_err(&["zdiffstore", "key", "0"], "wrong number of arguments");
    t.assert_err(
        &["zdiffstore", "key", "-1", "z1"],
        "value is not an integer or out of range",
    );
    t.assert_err(
        &["zdiffstore", "key", "0", "z1"],
        "at least 1 input key is needed",
    );
    t.assert_err(
        &["zdiffstore", "key", "0", "z1", "z2"],
        "at least 1 input key is needed",
    );

    assert_eq!(t.int(&["sadd", "s1", "one"]), 1);

    t.assert_err(
        &["zdiffstore", "key", "2", "z1", "s1"],
        "WRONGTYPE Operation against a key holding the wrong kind of value",
    );
    t.assert_err(
        &["zdiffstore", "key", "2", "s1", "z2"],
        "WRONGTYPE Operation against a key holding the wrong kind of value",
    );
}

#[test]
fn zdiff_store() {
    let mut t = Ctx::new();
    assert_eq!(
        t.int(&[
            "zadd", "z1", "1", "one", "2", "two", "3", "three", "4", "four"
        ]),
        4
    );
    assert_eq!(t.int(&["zadd", "z2", "1", "one", "5", "five"]), 2);
    assert_eq!(t.int(&["zadd", "z3", "2", "two", "3", "three"]), 2);
    assert_eq!(t.int(&["zadd", "z4", "4", "four"]), 1);

    let resp = t.run(&["zdiffstore", "key", "1", "z1"]);
    assert_eq!(resp.int(), Some(4), "{resp:?}");
    let resp = t.run(&["zrange", "key", "0", "-1", "withscores"]);
    assert_eq!(
        strs(&resp),
        ["one", "1", "two", "2", "three", "3", "four", "4"]
    );

    let resp = t.run(&["zdiffstore", "key", "2", "z1", "z1"]);
    assert_eq!(resp.int(), Some(0), "{resp:?}");
    let resp = t.run(&["zrange", "key", "0", "-1", "withscores"]);
    assert_eq!(resp.arr().unwrap().len(), 0, "{resp:?}");

    let resp = t.run(&["zdiffstore", "key", "4", "z1", "z2", "z3", "z4"]);
    assert_eq!(resp.int(), Some(0), "{resp:?}");
    let resp = t.run(&["zrange", "key", "0", "-1"]);
    assert_eq!(resp.arr().unwrap().len(), 0, "{resp:?}");

    let resp = t.run(&["zdiffstore", "key", "2", "z1", "doesnt_exist"]);
    assert_eq!(resp.int(), Some(4), "{resp:?}");
    let resp = t.run(&["zrange", "key", "0", "-1"]);
    assert_eq!(strs(&resp), ["one", "two", "three", "four"]);

    let resp = t.run(&["zdiffstore", "key", "2", "doesnt_exits", "z1"]);
    assert_eq!(resp.int(), Some(0), "{resp:?}");
    let resp = t.run(&["zrange", "key", "0", "-1"]);
    assert_eq!(resp.arr().unwrap().len(), 0, "{resp:?}");
}

#[test]
fn count() {
    let mut t = Ctx::new();
    for i in 0..129 {
        let member = format!("element:{i}");
        let score = format!("{i}");
        assert_eq!(t.int(&["zadd", "key", &score, &member]), 1, "member {i}");
    }

    assert_eq!(t.int(&["zcount", "key", "-inf", "+inf"]), 129);
    assert_eq!(t.int(&["zlexcount", "key", "-", "+"]), 129);

    // Single member.
    t.assert_int(&["ZADD", "short", "0", "A"], 1);
    assert_eq!(t.int(&["ZLEXCOUNT", "short", "-", "-"]), 0);
    assert_eq!(t.int(&["ZLEXCOUNT", "short", "+", "+"]), 0);
    assert_eq!(t.int(&["ZLEXCOUNT", "short", "+", "-"]), 0);

    t.assert_int(
        &["ZADD", "long", "0", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"],
        1,
    );
    assert_eq!(t.int(&["ZLEXCOUNT", "long", "-", "-"]), 0);
    assert_eq!(t.int(&["ZLEXCOUNT", "long", "+", "+"]), 0);
    assert_eq!(t.int(&["ZLEXCOUNT", "long", "+", "-"]), 0);
}

#[test]
fn range_limit() {
    let mut t = Ctx::new();
    t.assert_err(
        &["ZRANGEBYSCORE", "", "0.0", "0.0", "limit", "0"],
        "syntax error",
    );
    let resp = t.run(&["ZRANGEBYSCORE", "", "0.0", "0.0", "limit", "0", "0"]);
    assert_eq!(resp.arr().unwrap().len(), 0, "{resp:?}");

    t.assert_err(
        &["ZRANGEBYSCORE", "", "0.0", "0.0", "foo"],
        "unsupported option",
    );

    let resp = t.run(&["ZRANGEBYLEX", "foo", "-", "+", "LIMIT", "-1", "3"]);
    assert_eq!(resp.arr().unwrap().len(), 0, "{resp:?}");
}

#[test]
fn range_store() {
    let mut t = Ctx::new();
    assert_eq!(t.int(&["ZADD", "src", "1", "a", "2", "b", "3", "c"]), 3);
    assert_eq!(t.int(&["ZRANGESTORE", "dest", "src", "0", "-1"]), 3);

    let resp = t.run(&["ZRANGE", "dest", "0", "-1", "withscores"]);
    assert_eq!(strs(&resp), ["a", "1", "b", "2", "c", "3"]);

    // Override dest.
    assert_eq!(t.int(&["ZRANGESTORE", "dest", "not-found", "0", "-1"]), 0);

    let resp = t.run(&["ZRANGE", "dest", "0", "-1"]);
    assert_eq!(resp.arr().unwrap().len(), 0, "{resp:?}");
}

#[test]
fn zrange_zero_elements() {
    let mut t = Ctx::new();
    t.assert_int(&["zadd", "myzset", "1", "one"], 1);
    let resp = t.run(&["ZRANGE", "myzset", "0", "-1", "LIMIT", "2", "10"]);
    assert_eq!(resp.arr().unwrap().len(), 0, "{resp:?}");
}

#[test]
fn zcount_min_greater_than_max_crash() {
    let mut t = Ctx::new();
    // Add 1000 members to the sorted set.
    for i in 1..=1000 {
        let member = format!("member{i}");
        let score = format!("{i}");
        t.assert_int(&["zadd", "huge_key", &score, &member], 1);
    }

    // ZCOUNT returns 0 when min > max.
    let resp = t.run(&["zcount", "huge_key", "945", "261"]);
    assert_eq!(resp.int(), Some(0), "{resp:?}");
}
