//! Port of `dragonfly/src/server/topk_family_test.cc`.
//!
//! Adaptations from the C++ original:
//! - `Run(...)` becomes `t.run`, `CheckedInt`/`IntArg` become `t.int` /
//!   `expect_int`; `RespElementsAre(...)` (one-element array) and
//!   `RespArray(ElementsAre(...))` are both `t.arr`.
//! - `ArgType(RespExpr::NIL)` becomes an `is_nil()` element check; the evicted
//!   item (`RespExpr::STRING`) is asserted as a non-nil bulk.
//! - `TOPK.INFO`'s flat `[k, width, depth, decay]` array is asserted with
//!   `assert_info`, comparing the decay bulk exactly (the reference uses
//!   `"0.9"`, `"0"`, `"1"`, etc.).

mod common;

use common::*;

/// Assert the reply is a single-element array containing the integer `n`.
fn assert_int_arr(t: &mut Ctx, args: &[&str], n: i64) {
    let v = t.arr(args);
    assert_eq!(v.len(), 1, "reply {v:?}");
    assert_eq!(v[0].int(), Some(n), "reply {v:?}");
}

/// Assert every element of an array reply is null (no eviction).
fn assert_all_nil(t: &mut Ctx, args: &[&str]) {
    let v = t.arr(args);
    assert!(!v.is_empty(), "expected non-empty array");
    for (i, e) in v.iter().enumerate() {
        assert!(matches!(e, Value::Bulk(None)), "element {i}: {e:?}");
    }
}

/// Assert `TOPK.INFO` reports the given flat `[k, width, depth, decay]` array,
/// with `decay` compared as an exact bulk string.
fn assert_info(t: &mut Ctx, key: &str, k: i64, width: i64, depth: i64, decay: &str) {
    let v = t.arr(&["topk.info", key]);
    let expect: Vec<Value> = vec![
        Value::Bulk(Some(b"k".to_vec())),
        Value::Integer(k),
        Value::Bulk(Some(b"width".to_vec())),
        Value::Integer(width),
        Value::Bulk(Some(b"depth".to_vec())),
        Value::Integer(depth),
        Value::Bulk(Some(b"decay".to_vec())),
        Value::Bulk(Some(decay.as_bytes().to_vec())),
    ];
    assert_eq!(v, expect, "topk.info {key}");
}

/// `TOPK.RESERVE` with default width/depth/decay.
fn reserve_default(t: &mut Ctx, key: &str, k: &str) {
    t.ok(&["topk.reserve", key, k]);
}

/// `TOPK.RESERVE` with custom parameters.
fn reserve_custom(t: &mut Ctx, key: &str, k: &str, width: &str, depth: &str, decay: &str) {
    t.ok(&["topk.reserve", key, k, width, depth, decay]);
}

// =============================================================================
// I. General Key & Type Management
// =============================================================================

#[test]
fn commands_on_non_existent_key() {
    let mut t = Ctx::new();
    t.assert_err(&["topk.add", "noexist", "foo"], "no such key");
    t.assert_err(&["topk.incrby", "noexist", "foo", "1"], "no such key");
    t.assert_err(&["topk.query", "noexist", "foo"], "no such key");
    t.assert_err(&["topk.count", "noexist", "foo"], "no such key");
    t.assert_err(&["topk.list", "noexist"], "no such key");
    t.assert_err(&["topk.info", "noexist"], "no such key");
}

#[test]
fn wrong_type_errors() {
    let mut t = Ctx::new();
    t.ok(&["set", "mystr", "value"]);
    t.assert_err(&["topk.add", "mystr", "foo"], "WRONGTYPE");
    t.assert_err(&["topk.incrby", "mystr", "foo", "1"], "WRONGTYPE");
    t.assert_err(&["topk.query", "mystr", "foo"], "WRONGTYPE");
    t.assert_err(&["topk.count", "mystr", "foo"], "WRONGTYPE");
    t.assert_err(&["topk.list", "mystr"], "WRONGTYPE");
    t.assert_err(&["topk.info", "mystr"], "WRONGTYPE");
}

#[test]
fn type_command() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "myk", "5");
    t.assert_text(&["type", "myk"], "TopK-TYPE");
}

#[test]
fn delete_key() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "myk", "5");
    t.run(&["topk.add", "myk", "foo"]);
    t.assert_int(&["del", "myk"], 1);
    t.assert_err(&["topk.add", "myk", "foo"], "no such key");
}

#[test]
fn reserve_on_existing_wrong_type() {
    let mut t = Ctx::new();
    t.ok(&["set", "mystr", "val"]);
    t.assert_err(&["topk.reserve", "mystr", "5"], "WRONGTYPE");
}

// =============================================================================
// II. TOPK.RESERVE
// =============================================================================

#[test]
fn reserve_default_params() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "10");
    assert_info(&mut t, "tk", 10, 8, 7, "0.9");
}

#[test]
fn reserve_all_custom_params() {
    let mut t = Ctx::new();
    reserve_custom(&mut t, "tk", "20", "100", "5", "0.95");
    assert_info(&mut t, "tk", 20, 100, 5, "0.95");
}

#[test]
fn reserve_min_k() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "1");
    assert_info(&mut t, "tk", 1, 8, 7, "0.9");
}

#[test]
fn reserve_large_k() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "10000");
    assert_info(&mut t, "tk", 10000, 8, 7, "0.9");
}

#[test]
fn reserve_decay_zero() {
    let mut t = Ctx::new();
    reserve_custom(&mut t, "tk", "5", "8", "7", "0.0");
    assert_info(&mut t, "tk", 5, 8, 7, "0");
}

#[test]
fn reserve_decay_one() {
    let mut t = Ctx::new();
    reserve_custom(&mut t, "tk", "5", "8", "7", "1.0");
    assert_info(&mut t, "tk", 5, 8, 7, "1");
}

#[test]
fn reserve_k_zero() {
    let mut t = Ctx::new();
    t.assert_err(&["topk.reserve", "tk", "0"], "k must be greater than 0");
}

#[test]
fn reserve_k_negative() {
    let mut t = Ctx::new();
    t.assert_err(&["topk.reserve", "tk", "-1"], "not an integer");
}

#[test]
fn reserve_k_not_a_number() {
    let mut t = Ctx::new();
    t.assert_err(&["topk.reserve", "tk", "abc"], "not an integer");
}

#[test]
fn reserve_width_zero() {
    let mut t = Ctx::new();
    t.assert_err(
        &["topk.reserve", "tk", "5", "0", "7", "0.9"],
        "width and depth must be greater than 0",
    );
}

#[test]
fn reserve_depth_zero() {
    let mut t = Ctx::new();
    t.assert_err(
        &["topk.reserve", "tk", "5", "8", "0", "0.9"],
        "width and depth must be greater than 0",
    );
}

#[test]
fn reserve_decay_above_one() {
    let mut t = Ctx::new();
    t.assert_err(
        &["topk.reserve", "tk", "5", "8", "7", "1.5"],
        "decay must be between 0 and 1",
    );
}

#[test]
fn reserve_decay_negative() {
    let mut t = Ctx::new();
    t.assert_err(
        &["topk.reserve", "tk", "5", "8", "7", "-0.1"],
        "decay must be between 0 and 1",
    );
}

#[test]
fn reserve_duplicate_key() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.assert_err(&["topk.reserve", "tk", "10"], "item exists");
}

#[test]
fn reserve_too_few_args() {
    let mut t = Ctx::new();
    t.assert_err(&["topk.reserve", "tk"], "wrong number of arguments");
}

#[test]
fn reserve_partial_optional_params() {
    let mut t = Ctx::new();
    // Only width, missing depth and decay — parser OUT_OF_BOUNDS → syntax error.
    t.assert_err(&["topk.reserve", "tk", "5", "100"], "syntax error");
    // width + depth, missing decay.
    t.assert_err(&["topk.reserve", "tk", "5", "100", "7"], "syntax error");
}

#[test]
fn reserve_trailing_args() {
    let mut t = Ctx::new();
    t.assert_err(
        &["topk.reserve", "tk", "5", "8", "7", "0.9", "extra"],
        "syntax error",
    );
}

#[test]
fn reserve_dimensions_exceed_caps() {
    let mut t = Ctx::new();
    // width = 1,000,001 (exceeds kMaxWidth of 1,000,000).
    t.assert_err(
        &["topk.reserve", "tk1", "50", "1000001", "7", "0.9"],
        "must not exceed",
    );
    // depth = 101 (exceeds kMaxDepth of 100).
    t.assert_err(
        &["topk.reserve", "tk2", "50", "100000", "101", "0.9"],
        "must not exceed",
    );
}

// =============================================================================
// III. TOPK.ADD
// =============================================================================

#[test]
fn add_single_item() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    assert_all_nil(&mut t, &["topk.add", "tk", "foo"]);
}

#[test]
fn add_multiple_items() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    assert_all_nil(&mut t, &["topk.add", "tk", "a", "b", "c"]);
}

#[test]
fn add_duplicate_item() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.run(&["topk.add", "tk", "foo"]);
    t.run(&["topk.add", "tk", "foo"]);
    t.run(&["topk.add", "tk", "foo"]);

    let v = t.arr(&["topk.count", "tk", "foo"]);
    assert!(v[0].int().unwrap_or(0) >= 1, "reply {v:?}");
    assert_int_arr(&mut t, &["topk.query", "tk", "foo"], 1);
}

#[test]
fn add_no_items() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.assert_err(&["topk.add", "tk"], "wrong number of arguments");
}

#[test]
fn add_eviction() {
    let mut t = Ctx::new();
    // Wider sketch for more deterministic behavior.
    reserve_custom(&mut t, "tk", "2", "50", "7", "0.9");

    // Build up very strong counts so the top-2 are deterministic.
    t.run(&["topk.incrby", "tk", "heavy1", "10000"]);
    t.run(&["topk.incrby", "tk", "heavy2", "5000"]);

    // A weak item can't beat the heap minimum: nil, no eviction.
    assert_all_nil(&mut t, &["topk.add", "tk", "weak"]);

    let v = t.arr(&["topk.list", "tk"]);
    assert_eq!(v.len(), 2, "reply {v:?}");

    assert_int_arr(&mut t, &["topk.query", "tk", "heavy1"], 1);
    assert_int_arr(&mut t, &["topk.query", "tk", "heavy2"], 1);

    // A strong item evicts the weakest: the reply is a bulk string, not nil.
    let v = t.arr(&["topk.incrby", "tk", "newcomer", "100000"]);
    assert_eq!(v.len(), 1, "reply {v:?}");
    assert!(
        matches!(&v[0], Value::Bulk(Some(_))),
        "expected evicted string, got {:?}",
        v[0]
    );
}

#[test]
fn add_special_characters() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.run(&["topk.add", "tk", "hello world"]);
    t.run(&["topk.add", "tk", "foo\tbar"]);
    t.run(&["topk.add", "tk", ""]);
    assert_int_arr(&mut t, &["topk.query", "tk", "hello world"], 1);
}

#[test]
fn add_large_batch() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "10");
    let mut args: Vec<String> = vec!["topk.add".into(), "tk".into()];
    for i in 0..100 {
        args.push(format!("item{i}"));
    }
    let cmd: Vec<&str> = args.iter().map(String::as_str).collect();
    t.run(&cmd);

    let v = t.arr(&["topk.list", "tk"]);
    assert!(v.len() <= 10, "reply {v:?}");
}

// =============================================================================
// IV. TOPK.INCRBY
// =============================================================================

#[test]
fn incr_by_single_item() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    assert_all_nil(&mut t, &["topk.incrby", "tk", "foo", "10"]);

    let v = t.arr(&["topk.count", "tk", "foo"]);
    assert!(v[0].int().unwrap_or(0) >= 1, "reply {v:?}");
}

#[test]
fn incr_by_multiple_items() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    assert_all_nil(&mut t, &["topk.incrby", "tk", "a", "5", "b", "3", "c", "7"]);
}

#[test]
fn incr_by_accumulates() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.run(&["topk.incrby", "tk", "foo", "10"]);
    t.run(&["topk.incrby", "tk", "foo", "20"]);

    let v = t.arr(&["topk.count", "tk", "foo"]);
    assert!(v[0].int().unwrap_or(0) >= 1, "reply {v:?}");
}

#[test]
fn incr_by_min_increment() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    assert_all_nil(&mut t, &["topk.incrby", "tk", "foo", "1"]);
}

#[test]
fn incr_by_max_increment() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    assert_all_nil(&mut t, &["topk.incrby", "tk", "foo", "100000"]);
}

#[test]
fn incr_by_zero_increment() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.assert_err(
        &["topk.incrby", "tk", "foo", "0"],
        "increment must be between 1 and 100000",
    );
}

#[test]
fn incr_by_exceeds_max_increment() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.assert_err(
        &["topk.incrby", "tk", "foo", "100001"],
        "increment must be between 1 and 100000",
    );
}

#[test]
fn incr_by_non_numeric_increment() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.assert_err(
        &["topk.incrby", "tk", "foo", "notanumber"],
        "not an integer",
    );
}

#[test]
fn incr_by_odd_args() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    // 3 args total: rejected by the arity check.
    t.assert_err(&["topk.incrby", "tk", "foo"], "wrong number of arguments");
    // 5 args total: passes arity, but the handler sees odd item/incr pairs.
    t.assert_err(&["topk.incrby", "tk", "foo", "1", "bar"], "syntax error");
}

#[test]
fn incr_by_no_items() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.assert_err(&["topk.incrby", "tk"], "wrong number of arguments");
}

// =============================================================================
// V. TOPK.QUERY
// =============================================================================

#[test]
fn query_present_item() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.run(&["topk.add", "tk", "foo"]);
    assert_int_arr(&mut t, &["topk.query", "tk", "foo"], 1);
}

#[test]
fn query_absent_item() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    assert_int_arr(&mut t, &["topk.query", "tk", "neveradded"], 0);
}

#[test]
fn query_multiple_mixed() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.run(&["topk.add", "tk", "a"]);
    t.run(&["topk.add", "tk", "b"]);
    let v = t.arr(&["topk.query", "tk", "a", "b", "c"]);
    assert_eq!(
        v.iter().map(Value::int).collect::<Vec<_>>(),
        vec![Some(1), Some(1), Some(0)],
        "reply {v:?}"
    );
}

#[test]
fn query_empty_topk() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    assert_int_arr(&mut t, &["topk.query", "tk", "anything"], 0);
}

#[test]
fn query_no_items() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.assert_err(&["topk.query", "tk"], "wrong number of arguments");
}

// =============================================================================
// VI. TOPK.COUNT
// =============================================================================

#[test]
fn count_single_item() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.run(&["topk.incrby", "tk", "foo", "10"]);
    let v = t.arr(&["topk.count", "tk", "foo"]);
    assert!(v[0].int().unwrap_or(0) >= 1, "reply {v:?}");
}

#[test]
fn count_absent_item() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    assert_int_arr(&mut t, &["topk.count", "tk", "neveradded"], 0);
}

#[test]
fn count_multiple_relative_order() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.run(&["topk.incrby", "tk", "low", "10"]);
    t.run(&["topk.incrby", "tk", "high", "100"]);
    let v = t.arr(&["topk.count", "tk", "high", "low"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert!(v[0].int() >= v[1].int(), "reply {v:?}");
}

#[test]
fn count_empty_topk() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    assert_int_arr(&mut t, &["topk.count", "tk", "anything"], 0);
}

#[test]
fn count_item_outside_of_heap() {
    let mut t = Ctx::new();
    // k=1, decay=1.0 (disables decay of existing counters, though hash
    // collisions remain probabilistic).
    reserve_custom(&mut t, "tk", "1", "50", "7", "1.0");

    t.run(&["topk.incrby", "tk", "heavy", "1000"]);
    t.run(&["topk.incrby", "tk", "victim", "5"]);

    assert_int_arr(&mut t, &["topk.query", "tk", "victim"], 0);

    // Count-Min Sketch guarantees count >= actual, but hash collisions can
    // cause overestimation.
    let v = t.arr(&["topk.count", "tk", "victim"]);
    assert!(v[0].int().unwrap_or(0) >= 5, "reply {v:?}");
    let v = t.arr(&["topk.count", "tk", "heavy"]);
    assert!(v[0].int().unwrap_or(0) >= 1000, "reply {v:?}");
}

#[test]
fn count_no_items() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.assert_err(&["topk.count", "tk"], "wrong number of arguments");
}

// =============================================================================
// VII. TOPK.LIST
// =============================================================================

#[test]
fn list_empty() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    let v = t.arr(&["topk.list", "tk"]);
    assert!(v.is_empty(), "reply {v:?}");
}

#[test]
fn list_after_adds() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    t.run(&["topk.add", "tk", "a", "b", "c"]);
    let v = t.arr(&["topk.list", "tk"]);
    assert_eq!(v.len(), 3, "reply {v:?}");
}

#[test]
fn list_capped_at_k() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "3");
    for i in 0..10 {
        t.run(&["topk.incrby", "tk", &format!("item{i}"), "100"]);
    }
    let v = t.arr(&["topk.list", "tk"]);
    assert_eq!(v.len(), 3, "reply {v:?}");
}

#[test]
fn list_with_count() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "3");
    t.run(&["topk.incrby", "tk", "a", "100"]);
    t.run(&["topk.incrby", "tk", "b", "50"]);
    t.run(&["topk.incrby", "tk", "c", "10"]);

    let v = t.arr(&["topk.list", "tk", "WITHCOUNT"]);
    assert_eq!(v.len(), 6, "reply {v:?}");
    // Pairs of (string, integer).
    for i in (0..v.len()).step_by(2) {
        assert!(
            matches!(&v[i], Value::Bulk(Some(_))),
            "element {i}: {:?}",
            v[i]
        );
        assert!(
            v[i + 1].int().unwrap_or(0) >= 1,
            "element {}: {:?}",
            i + 1,
            v[i + 1]
        );
    }
}

#[test]
fn list_with_count_case_insensitive() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "3");
    t.run(&["topk.add", "tk", "a"]);
    let v = t.arr(&["topk.list", "tk", "withcount"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
}

#[test]
fn list_descending_order() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "3");
    t.run(&["topk.incrby", "tk", "low", "10"]);
    t.run(&["topk.incrby", "tk", "mid", "50"]);
    t.run(&["topk.incrby", "tk", "high", "100"]);

    let v = t.arr(&["topk.list", "tk", "WITHCOUNT"]);
    assert_eq!(v.len(), 6, "reply {v:?}");
    let mut prev = i64::MAX;
    for i in (1..v.len()).step_by(2) {
        let count = v[i].int().expect("count integer");
        assert!(count <= prev, "reply {v:?}");
        prev = count;
    }
}

#[test]
fn list_invalid_flag() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "3");
    t.assert_err(&["topk.list", "tk", "INVALID"], "syntax error");
}

#[test]
fn list_trailing_args() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "3");
    t.assert_err(&["topk.list", "tk", "WITHCOUNT", "extra"], "syntax error");
}

// =============================================================================
// VIII. TOPK.INFO
// =============================================================================

#[test]
fn info_default_params() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    assert_info(&mut t, "tk", 5, 8, 7, "0.9");
}

#[test]
fn info_custom_params() {
    let mut t = Ctx::new();
    reserve_custom(&mut t, "tk", "20", "200", "10", "0.75");
    assert_info(&mut t, "tk", 20, 200, 10, "0.75");
}

#[test]
fn info_trailing_args() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    // Fixed arity 2: the framework rejects extra args.
    t.assert_err(&["topk.info", "tk", "extra"], "wrong number of arguments");
}

#[test]
fn info_response_format() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    let v = t.arr(&["topk.info", "tk"]);
    assert_eq!(v.len(), 8, "reply {v:?}");
    assert_eq!(v[0].text().as_deref(), Some("k"));
    assert_eq!(v[2].text().as_deref(), Some("width"));
    assert_eq!(v[4].text().as_deref(), Some("depth"));
    assert_eq!(v[6].text().as_deref(), Some("decay"));
}

// =============================================================================
// IX. Advanced & Integrity
// =============================================================================

#[test]
fn frequency_accuracy() {
    let mut t = Ctx::new();
    // Wider sketch for more deterministic behavior.
    reserve_custom(&mut t, "tk", "3", "50", "7", "0.9");

    t.run(&["topk.incrby", "tk", "alpha", "50000"]);
    t.run(&["topk.incrby", "tk", "beta", "30000"]);
    t.run(&["topk.incrby", "tk", "gamma", "20000"]);

    for i in 0..50 {
        t.run(&["topk.add", "tk", &format!("noise{i}")]);
    }

    assert_int_arr(&mut t, &["topk.query", "tk", "alpha"], 1);
    assert_int_arr(&mut t, &["topk.query", "tk", "beta"], 1);
    assert_int_arr(&mut t, &["topk.query", "tk", "gamma"], 1);
}

#[test]
fn multiple_keys_isolation() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk1", "3");
    reserve_default(&mut t, "tk2", "5");

    t.run(&["topk.add", "tk1", "onlyin1"]);
    t.run(&["topk.add", "tk2", "onlyin2"]);

    assert_int_arr(&mut t, &["topk.query", "tk1", "onlyin1"], 1);
    assert_int_arr(&mut t, &["topk.query", "tk1", "onlyin2"], 0);
    assert_int_arr(&mut t, &["topk.query", "tk2", "onlyin2"], 1);
    assert_int_arr(&mut t, &["topk.query", "tk2", "onlyin1"], 0);

    assert_info(&mut t, "tk1", 3, 8, 7, "0.9");
    assert_info(&mut t, "tk2", 5, 8, 7, "0.9");
}

#[test]
fn add_and_incr_by_interaction() {
    let mut t = Ctx::new();
    reserve_custom(&mut t, "tk", "5", "100", "7", "0.9");

    // Add via ADD (count +1), then increment via INCRBY.
    t.run(&["topk.add", "tk", "foo"]);
    assert_int_arr(&mut t, &["topk.query", "tk", "foo"], 1);
    t.run(&["topk.incrby", "tk", "foo", "100"]);
    assert_int_arr(&mut t, &["topk.query", "tk", "foo"], 1);

    // A different item: ADD then boost via INCRBY.
    t.run(&["topk.add", "tk", "bar"]);
    t.run(&["topk.incrby", "tk", "bar", "50"]);
    assert_int_arr(&mut t, &["topk.query", "tk", "bar"], 1);
}

#[test]
fn high_contention_equal_counts() {
    let mut t = Ctx::new();
    reserve_default(&mut t, "tk", "5");
    for i in 0..20 {
        t.run(&["topk.incrby", "tk", &format!("item{i}"), "10"]);
    }
    let v = t.arr(&["topk.list", "tk"]);
    assert_eq!(v.len(), 5, "reply {v:?}");
}
