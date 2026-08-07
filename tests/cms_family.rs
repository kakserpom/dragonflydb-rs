//! Port of `dragonfly/src/server/cms_family_test.cc`.
//!
//! Adaptations from the C++ original:
//! - Single-item replies (`RespElementsAre(IntArg(...))`) are decoded as
//!   one-element arrays via `arr_ints`.
//! - Error prefixes are matched with `assert_err` (the C++ `ErrArg`).
//! - The C++ `Run("...")` string form becomes `t.run(&[...])`; `Run({...})`
//!   the list form.
//!
//! CMS.MERGE is multi-key, so these tests exercise cross-shard coordinator
//! routing on the default two-shard context.

mod common;

use common::*;

/// Assert the reply is an array of exactly the given integer elements.
fn arr_ints(t: &mut Ctx, cmd: &[&str], expected: &[i64]) {
    let a = t.arr(cmd);
    assert_eq!(a.len(), expected.len(), "reply {a:?}");
    for (v, e) in a.iter().zip(expected) {
        assert_eq!(v.int(), Some(*e), "reply {a:?}");
    }
}

#[test]
fn init_by_dim() {
    let mut t = Ctx::new();
    t.ok(&["cms.initbydim", "cms1", "1000", "5"]);
    t.assert_text(&["type", "cms1"], "CMSk-TYPE");

    t.assert_err(&["cms.initbydim", "cms1", "100", "5"], "item exists");
    t.assert_err(
        &["cms.initbydim", "cms2", "0", "5"],
        "width and depth must be greater than 0",
    );
    t.assert_err(
        &["cms.initbydim", "cms3", "5", "0"],
        "width and depth must be greater than 0",
    );
}

#[test]
fn init_by_dim_rejects_oversized_dimensions_and_preserves_state() {
    let mut t = Ctx::new();
    t.assert_err(
        &["cms.initbydim", "k", "2147483648", "1073741824"],
        "width must not exceed",
    );
    t.assert_int(&["exists", "k"], 0);

    t.assert_err(&["cms.incrby", "k", "a", "1"], "CMS: key does not exist");
    t.assert_int(&["exists", "k"], 0);

    t.ok(&["cms.initbydim", "safe", "100", "5"]);
    arr_ints(&mut t, &["cms.incrby", "safe", "a", "1"], &[1]);
    arr_ints(&mut t, &["cms.query", "safe", "a"], &[1]);
}

#[test]
fn init_by_prob() {
    let mut t = Ctx::new();
    t.ok(&["cms.initbyprob", "cms1", "0.01", "0.01"]);

    t.assert_err(&["cms.initbyprob", "cms1", "0.01", "0.01"], "item exists");
    t.assert_err(
        &["cms.initbyprob", "cms2", "2", "0.01"],
        "error must be between 0 and 1",
    );
    t.assert_err(
        &["cms.initbyprob", "cms3", "0.01", "0"],
        "probability must be between 0 and 1",
    );
}

#[test]
fn init_by_prob_rejects_oversized_derived_dimensions() {
    let mut t = Ctx::new();
    t.assert_err(
        &["cms.initbyprob", "cms", "0.000001", "0.01"],
        "width must not exceed",
    );
    t.assert_int(&["exists", "cms"], 0);
}

#[test]
fn incr_by() {
    let mut t = Ctx::new();
    t.ok(&["cms.initbydim", "cms", "100", "5"]);

    arr_ints(&mut t, &["cms.incrby", "cms", "foo", "3"], &[3]);
    arr_ints(
        &mut t,
        &["cms.incrby", "cms", "foo", "4", "bar", "1"],
        &[7, 1],
    );

    t.assert_err(
        &["cms.incrby", "noexist", "foo", "1"],
        "CMS: key does not exist",
    );
    t.assert_err(
        &["cms.incrby", "cms", "foo", "notanumber"],
        "CMS: Cannot parse number",
    );
    t.assert_err(
        &["cms.incrby", "cms", "foo", "0"],
        "CMS: increment must be a positive integer",
    );
    t.assert_err(&["cms.incrby", "cms", "foo", "1", "bar"], "syntax error");
}

#[test]
fn query() {
    let mut t = Ctx::new();
    t.ok(&["cms.initbydim", "cms", "100", "5"]);
    t.run(&["cms.incrby", "cms", "foo", "5", "bar", "3"]);

    arr_ints(&mut t, &["cms.query", "cms", "foo"], &[5]);
    arr_ints(&mut t, &["cms.query", "cms", "foo", "bar"], &[5, 3]);
    arr_ints(&mut t, &["cms.query", "cms", "noexist"], &[0]);
    t.assert_err(&["cms.query", "noexist", "foo"], "CMS: key does not exist");
}

#[test]
fn info() {
    let mut t = Ctx::new();
    t.ok(&["cms.initbydim", "cms", "1000", "5"]);
    t.run(&["cms.incrby", "cms", "foo", "5", "bar", "3", "baz", "9"]);

    let v = t.arr(&["cms.info", "cms"]);
    assert_eq!(v.len(), 6, "reply {v:?}");
    assert_eq!(v[0].text().as_deref(), Some("width"));
    assert_eq!(v[1].int(), Some(1000));
    assert_eq!(v[2].text().as_deref(), Some("depth"));
    assert_eq!(v[3].int(), Some(5));
    assert_eq!(v[4].text().as_deref(), Some("count"));
    assert_eq!(v[5].int(), Some(17));

    t.assert_err(&["cms.info", "noexist"], "CMS: key does not exist");
}

#[test]
fn merge() {
    let mut t = Ctx::new();
    t.ok(&["cms.initbydim", "A", "100", "5"]);
    t.ok(&["cms.initbydim", "B", "100", "5"]);
    t.ok(&["cms.initbydim", "C", "100", "5"]);

    t.run(&["cms.incrby", "A", "foo", "5", "bar", "3", "baz", "9"]);
    t.run(&["cms.incrby", "B", "foo", "2", "foobar", "3", "baz", "1"]);

    arr_ints(&mut t, &["cms.query", "A", "foo", "bar", "baz"], &[5, 3, 9]);
    arr_ints(
        &mut t,
        &["cms.query", "B", "foo", "foobar", "baz"],
        &[2, 3, 1],
    );

    t.ok(&["cms.merge", "C", "2", "A", "B"]);
    arr_ints(
        &mut t,
        &["cms.query", "C", "foo", "bar", "baz", "foobar"],
        &[7, 3, 10, 3],
    );

    t.assert_err(
        &["cms.merge", "noexist", "1", "A"],
        "CMS: key does not exist",
    );
    t.assert_err(&["cms.merge", "C", "0", "A"], "CMS: wrong number of keys");
    t.assert_err(
        &["cms.merge", "A", "1", "B", "WEIGHTS", "4", "3"],
        "CMS: wrong number of keys/weights",
    );
    t.assert_err(
        &["cms.merge", "A", "2", "B", "noexist", "WEIGHTS", "4", "3"],
        "CMS: key does not exist",
    );

    // Merge A into B; the destination is reset before merging, so B now
    // holds A's values.
    t.ok(&["cms.merge", "B", "1", "A"]);
    arr_ints(&mut t, &["cms.query", "B", "foo", "bar", "baz"], &[5, 3, 9]);
}

#[test]
fn merge_with_weights() {
    let mut t = Ctx::new();
    t.ok(&["cms.initbydim", "A", "100", "5"]);
    t.ok(&["cms.initbydim", "B", "100", "5"]);
    t.ok(&["cms.initbydim", "C", "100", "5"]);

    t.run(&["cms.incrby", "A", "foo", "5", "bar", "3", "baz", "9"]);
    t.run(&["cms.incrby", "B", "foo", "2", "bar", "3", "baz", "1"]);

    // foo: 5*2 + 2*3 = 16; bar: 3*2 + 3*3 = 15; baz: 9*2 + 1*3 = 21.
    t.ok(&["cms.merge", "C", "2", "A", "B", "WEIGHTS", "2", "3"]);
    arr_ints(
        &mut t,
        &["cms.query", "C", "foo", "bar", "baz"],
        &[16, 15, 21],
    );
}

#[test]
fn merge_with_duplicate_source_keys_preserves_weight_order() {
    let mut t = Ctx::new();
    t.ok(&["cms.initbydim", "A", "100", "5"]);
    t.ok(&["cms.initbydim", "C", "100", "5"]);

    t.run(&["cms.incrby", "A", "foo", "2", "bar", "4"]);

    t.ok(&["cms.merge", "C", "2", "A", "A", "WEIGHTS", "1", "3"]);
    arr_ints(&mut t, &["cms.query", "C", "foo", "bar"], &[8, 16]);

    let v = t.arr(&["cms.info", "C"]);
    assert_eq!(v.len(), 6, "reply {v:?}");
    assert_eq!(v[0].text().as_deref(), Some("width"));
    assert_eq!(v[1].int(), Some(100));
    assert_eq!(v[2].text().as_deref(), Some("depth"));
    assert_eq!(v[3].int(), Some(5));
    assert_eq!(v[4].text().as_deref(), Some("count"));
    assert_eq!(v[5].int(), Some(24));
}

#[test]
fn info_after_merges() {
    let mut t = Ctx::new();
    t.ok(&["cms.initbydim", "A", "1000", "5"]);
    t.ok(&["cms.initbydim", "B", "1000", "5"]);
    t.ok(&["cms.initbydim", "C", "1000", "5"]);

    t.run(&["cms.incrby", "A", "foo", "5", "bar", "3", "baz", "9"]);
    t.run(&["cms.incrby", "B", "foo", "2", "bar", "3", "baz", "1"]);

    arr_ints(&mut t, &["cms.query", "A", "foo", "bar", "baz"], &[5, 3, 9]);
    arr_ints(&mut t, &["cms.query", "B", "foo", "bar", "baz"], &[2, 3, 1]);

    t.ok(&["cms.merge", "C", "2", "A", "B"]);
    arr_ints(
        &mut t,
        &["cms.query", "C", "foo", "bar", "baz"],
        &[7, 6, 10],
    );

    t.ok(&["cms.merge", "C", "2", "A", "B", "WEIGHTS", "1", "2"]);
    arr_ints(
        &mut t,
        &["cms.query", "C", "foo", "bar", "baz"],
        &[9, 9, 11],
    );

    t.ok(&["cms.merge", "C", "2", "A", "B", "WEIGHTS", "2", "3"]);
    arr_ints(
        &mut t,
        &["cms.query", "C", "foo", "bar", "baz"],
        &[16, 15, 21],
    );

    let v = t.arr(&["cms.info", "A"]);
    assert_eq!(v.len(), 6, "reply {v:?}");
    assert_eq!(v[0].text().as_deref(), Some("width"));
    assert_eq!(v[1].int(), Some(1000));
    assert_eq!(v[2].text().as_deref(), Some("depth"));
    assert_eq!(v[3].int(), Some(5));
    assert_eq!(v[4].text().as_deref(), Some("count"));
    assert_eq!(v[5].int(), Some(17));

    t.assert_err(&["cms.info", "noexist"], "CMS: key does not exist");
}
