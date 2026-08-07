//! Port of `dragonfly/src/server/cuckoo_filter_family_test.cc`.
//!
//! Adaptations from the C++ original:
//! - `RespArray(ElementsAre(...))` replies are decoded via `t.arr`; the
//!   CF.INFO layout (Size/Number of buckets are layout-dependent) is asserted
//!   through `assert_cf_info`.
//! - DUMP/RESTORE carry binary payloads, so they are sent via `t.bulk` /
//!   `t.ok_b`.
//! - Error prefixes are matched with `assert_err` (the C++ `ErrArg`).

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

/// Assert a CF.INFO reply: the fixed 16-element key/value array, with the
/// layout-dependent Size/Number of buckets values only range-checked.
/// `stats` is `(filters, items, deletes, bucket, expansion, max_iterations)`.
fn assert_cf_info(t: &mut Ctx, key: &str, stats: (i64, i64, i64, i64, i64, i64)) {
    let v = t.arr(&["cf.info", key]);
    assert_eq!(v.len(), 16, "reply {v:?}");
    assert_eq!(v[0].text().as_deref(), Some("Size"));
    assert!(v[1].int().is_some_and(|n| n > 0));
    assert_eq!(v[2].text().as_deref(), Some("Number of buckets"));
    assert!(v[3].int().is_some_and(|n| n > 0));
    assert_eq!(v[4].text().as_deref(), Some("Number of filters"));
    assert_eq!(v[5].int(), Some(stats.0));
    assert_eq!(v[6].text().as_deref(), Some("Number of items inserted"));
    assert_eq!(v[7].int(), Some(stats.1));
    assert_eq!(v[8].text().as_deref(), Some("Number of items deleted"));
    assert_eq!(v[9].int(), Some(stats.2));
    assert_eq!(v[10].text().as_deref(), Some("Bucket size"));
    assert_eq!(v[11].int(), Some(stats.3));
    assert_eq!(v[12].text().as_deref(), Some("Expansion rate"));
    assert_eq!(v[13].int(), Some(stats.4));
    assert_eq!(v[14].text().as_deref(), Some("Max iterations"));
    assert_eq!(v[15].int(), Some(stats.5));
}

#[test]
fn reserve() {
    let mut t = Ctx::new();
    t.ok(&["cf.reserve", "cf1", "1000"]);
    t.assert_text(&["type", "cf1"], "MBbloomCF");

    t.assert_err(&["cf.reserve", "cf1", "1000"], "item exists");
    t.assert_err(
        &["cf.reserve", "cf2", "0"],
        "capacity must be greater than 0",
    );
}

#[test]
fn reserve_with_options() {
    let mut t = Ctx::new();
    t.ok(&[
        "cf.reserve",
        "cf1",
        "1000",
        "bucketsize",
        "4",
        "maxiterations",
        "10",
        "expansion",
        "2",
    ]);

    t.assert_err(
        &["cf.reserve", "cf2", "1000", "bucketsize", "0"],
        "bucket size must be between 1 and 255",
    );
    t.assert_err(
        &["cf.reserve", "cf3", "1000", "bucketsize", "256"],
        "value is not an integer or out of range",
    );
    t.assert_err(
        &["cf.reserve", "cf4", "1000", "maxiterations", "0"],
        "max iterations must be between 1 and 65535",
    );
    t.assert_err(
        &["cf.reserve", "cf5", "1000", "expansion", "32768"],
        "expansion must be between 0 and 32767",
    );
}

#[test]
fn wrong_type() {
    let mut t = Ctx::new();
    t.ok(&["set", "str1", "foo"]);
    t.assert_err(&["cf.reserve", "str1", "1000"], "WRONGTYPE");
}

#[test]
fn dump_and_restore() {
    let mut t = Ctx::new();
    t.ok(&[
        "cf.reserve",
        "cf1",
        "1000",
        "bucketsize",
        "4",
        "maxiterations",
        "10",
        "expansion",
        "2",
    ]);
    t.assert_int(&["cf.add", "cf1", "foo"], 1);
    t.assert_int(&["cf.add", "cf1", "foo"], 1);
    t.assert_int(&["cf.add", "cf1", "bar"], 1);

    let dump = t.bulk(&["dump", "cf1"]);
    t.ok_b(&[b"restore".to_vec(), b"cf2".to_vec(), b"0".to_vec(), dump]);

    t.assert_text(&["type", "cf2"], "MBbloomCF");
    t.assert_int(&["cf.count", "cf2", "foo"], 2);
    t.assert_int(&["cf.exists", "cf2", "bar"], 1);
    t.assert_int(&["cf.exists", "cf2", "nope"], 0);
    assert_cf_info(&mut t, "cf2", (1, 3, 0, 4, 2, 10));
}

#[test]
fn dump_and_restore_after_expansion() {
    // Force growth past the first sub-filter so the dump covers num_filters > 1.
    let mut t = Ctx::new();
    t.ok(&["cf.reserve", "cf1", "4", "expansion", "2"]);
    for i in 0..100 {
        t.assert_int(&["cf.add", "cf1", &format!("{i}")], 1);
    }

    let dump = t.bulk(&["dump", "cf1"]);
    t.ok_b(&[b"restore".to_vec(), b"cf2".to_vec(), b"0".to_vec(), dump]);

    for i in 0..100 {
        t.assert_int(&["cf.exists", "cf2", &format!("{i}")], 1);
    }
}

#[test]
fn add_auto_creates_and_allows_duplicates() {
    let mut t = Ctx::new();
    t.assert_int(&["cf.add", "f1", "foo"], 1);
    t.assert_text(&["type", "f1"], "MBbloomCF");

    // CF.ADD allows duplicate insertions.
    t.assert_int(&["cf.add", "f1", "foo"], 1);
}

#[test]
fn add_nx_prevents_duplicates() {
    let mut t = Ctx::new();
    t.assert_int(&["cf.addnx", "cf", "k1"], 1);
    t.assert_int(&["cf.addnx", "cf", "k1"], 0);

    // CF.ADD still allows the duplicate that CF.ADDNX rejected.
    t.assert_int(&["cf.add", "cf", "k1"], 1);
}

#[test]
fn add_wrong_arity() {
    let mut t = Ctx::new();
    t.assert_err(&["cf.add"], "wrong number of arguments");
    t.assert_err(&["cf.add", "f1"], "wrong number of arguments");
    t.assert_err(&["cf.addnx"], "wrong number of arguments");
    t.assert_err(&["cf.addnx", "f1"], "wrong number of arguments");
}

#[test]
fn add_wrong_type() {
    let mut t = Ctx::new();
    t.ok(&["set", "str1", "foo"]);
    t.assert_err(&["cf.add", "str1", "foo"], "WRONGTYPE");
    t.assert_err(&["cf.addnx", "str1", "foo"], "WRONGTYPE");
}

#[test]
fn add_filter_full() {
    let mut t = Ctx::new();
    t.ok(&["cf.reserve", "cf", "4", "expansion", "0"]);
    for i in 0..4 {
        t.assert_int(&["cf.add", "cf", &format!("{i}")], 1);
    }
    t.assert_err(&["cf.add", "cf", "overflow"], "Filter is full");
}

#[test]
fn insert_filter_full() {
    // Non-expanding filter with capacity 4; CF.INSERT returns -1 for items
    // that cannot be inserted.
    let mut t = Ctx::new();
    t.ok(&["cf.reserve", "cf", "4", "expansion", "0"]);
    for i in 0..4 {
        arr_ints(&mut t, &["cf.insert", "cf", "items", &format!("{i}")], &[1]);
    }
    arr_ints(
        &mut t,
        &["cf.insert", "cf", "items", "overflow1", "overflow2"],
        &[-1, -1],
    );

    // CF.INSERTNX: -1 for full, 0 for existing, 1 for inserted.
    t.ok(&["cf.reserve", "cfnx", "4", "expansion", "0"]);
    for i in 0..4 {
        arr_ints(
            &mut t,
            &["cf.insertnx", "cfnx", "items", &format!("{i}")],
            &[1],
        );
    }
    // Item 0 already exists -> 0; overflow -> -1.
    arr_ints(
        &mut t,
        &["cf.insertnx", "cfnx", "items", "0", "overflow"],
        &[0, -1],
    );
}

#[test]
fn exists() {
    let mut t = Ctx::new();
    t.assert_int(&["cf.add", "f1", "foo"], 1);
    t.assert_int(&["cf.exists", "f1", "foo"], 1);
    t.assert_int(&["cf.exists", "f1", "bar"], 0);

    // Missing key returns 0, not an error.
    t.assert_int(&["cf.exists", "nonexist-key", "blah"], 0);
}

#[test]
fn exists_wrong_arity() {
    let mut t = Ctx::new();
    t.assert_err(&["cf.exists"], "wrong number of arguments");
    t.assert_err(&["cf.exists", "key"], "wrong number of arguments");
}

#[test]
fn exists_wrong_type() {
    let mut t = Ctx::new();
    t.ok(&["set", "str1", "foo"]);
    t.assert_int(&["cf.exists", "str1", "foo"], 0);
}

#[test]
fn m_exists() {
    let mut t = Ctx::new();
    t.assert_int(&["cf.add", "f1", "foo"], 1);
    t.assert_int(&["cf.add", "f1", "bar"], 1);
    t.assert_int(&["cf.add", "f1", "baz"], 1);

    arr_ints(
        &mut t,
        &["cf.mexists", "f1", "foo", "bar", "baz"],
        &[1, 1, 1],
    );
    arr_ints(&mut t, &["cf.mexists", "f1", "foo", "nope"], &[1, 0]);

    // Missing key returns an all-zero array, not an error.
    arr_ints(&mut t, &["cf.mexists", "nonexist-key", "blah"], &[0]);
}

#[test]
fn m_exists_wrong_arity() {
    let mut t = Ctx::new();
    t.assert_err(&["cf.mexists"], "wrong number of arguments");
    t.assert_err(&["cf.mexists", "key"], "wrong number of arguments");
}

#[test]
fn m_exists_wrong_type() {
    let mut t = Ctx::new();
    t.ok(&["set", "str1", "foo"]);
    arr_ints(&mut t, &["cf.mexists", "str1", "foo"], &[0]);
}

#[test]
fn info() {
    let mut t = Ctx::new();
    t.ok(&[
        "cf.reserve",
        "cf1",
        "1000",
        "bucketsize",
        "4",
        "maxiterations",
        "10",
        "expansion",
        "2",
    ]);
    t.assert_int(&["cf.add", "cf1", "foo"], 1);

    assert_cf_info(&mut t, "cf1", (1, 1, 0, 4, 2, 10));
}

#[test]
fn info_missing_key() {
    let mut t = Ctx::new();
    t.assert_err(&["cf.info", "nonexist-key"], "no such key");
}

#[test]
fn count() {
    let mut t = Ctx::new();
    t.assert_int(&["cf.add", "f1", "foo"], 1);
    t.assert_int(&["cf.count", "f1", "foo"], 1);
    t.assert_int(&["cf.count", "f1", "bar"], 0);

    // Missing key returns 0, not an error.
    t.assert_int(&["cf.count", "nonexist-key", "blah"], 0);
}

#[test]
fn count_after_duplicate_adds() {
    // CF.ADD never dedups, so repeated adds of the same item each bump the count.
    let mut t = Ctx::new();
    t.assert_int(&["cf.add", "f1", "foo"], 1);
    t.assert_int(&["cf.add", "f1", "foo"], 1);
    t.assert_int(&["cf.add", "f1", "foo"], 1);
    t.assert_int(&["cf.count", "f1", "foo"], 3);

    t.assert_int(&["cf.del", "f1", "foo"], 1);
    t.assert_int(&["cf.count", "f1", "foo"], 2);
}

#[test]
fn del() {
    let mut t = Ctx::new();
    t.assert_int(&["cf.add", "f1", "foo"], 1);
    t.assert_int(&["cf.del", "f1", "foo"], 1);
    t.assert_int(&["cf.exists", "f1", "foo"], 0);
}

#[test]
fn del_non_existent_item() {
    let mut t = Ctx::new();
    t.ok(&["cf.reserve", "cf1", "1000"]);
    t.assert_int(&["cf.del", "cf1", "nope"], 0);
}

#[test]
fn del_missing_key() {
    let mut t = Ctx::new();
    t.assert_err(&["cf.del", "nonexist-key", "foo"], "no such key");
}

#[test]
fn compact() {
    let mut t = Ctx::new();
    t.ok(&["cf.reserve", "cf1", "4"]);
    for i in 0..30 {
        t.assert_int(&["cf.add", "cf1", &format!("{i}")], 1);
    }
    for i in 0..29 {
        t.assert_int(&["cf.del", "cf1", &format!("{i}")], 1);
    }

    // Explicit CF.COMPACT should succeed even though CF.DEL's automatic
    // compaction has likely already run by this point.
    t.ok(&["cf.compact", "cf1"]);
    t.assert_int(&["cf.exists", "cf1", "29"], 1);
}

#[test]
fn compact_missing_key() {
    let mut t = Ctx::new();
    t.assert_err(&["cf.compact", "nonexist-key"], "no such key");
}

#[test]
fn insert() {
    let mut t = Ctx::new();
    arr_ints(
        &mut t,
        &["cf.insert", "cf", "items", "a", "b", "c"],
        &[1, 1, 1],
    );
    t.assert_text(&["type", "cf"], "MBbloomCF");

    // Duplicates are allowed (like CF.ADD).
    arr_ints(&mut t, &["cf.insert", "cf", "items", "a", "a"], &[1, 1]);
}

#[test]
fn insert_with_capacity() {
    let mut t = Ctx::new();
    arr_ints(
        &mut t,
        &["cf.insert", "cf", "capacity", "500", "items", "x"],
        &[1],
    );
}

#[test]
fn insert_zero_capacity() {
    let mut t = Ctx::new();
    t.assert_err(
        &["cf.insert", "cf", "capacity", "0", "items", "x"],
        "capacity must be greater than 0",
    );
    t.assert_err(
        &["cf.insert", "cf", "capacity", "0", "nocreate", "items", "x"],
        "no such key",
    );
}

#[test]
fn insert_nocreate() {
    // NOCREATE on missing key returns an error.
    let mut t = Ctx::new();
    t.assert_err(
        &["cf.insert", "cf", "nocreate", "items", "a"],
        "no such key",
    );

    // NOCREATE on existing key works fine.
    t.ok(&["cf.reserve", "cf", "1000"]);
    arr_ints(&mut t, &["cf.insert", "cf", "nocreate", "items", "a"], &[1]);
}

#[test]
fn insert_missing_items_keyword() {
    let mut t = Ctx::new();
    t.assert_err(&["cf.insert", "cf", "a", "b"], "ITEMS");
}

#[test]
fn insert_wrong_type() {
    let mut t = Ctx::new();
    t.ok(&["set", "str1", "foo"]);
    t.assert_err(&["cf.insert", "str1", "items", "a"], "WRONGTYPE");
}

#[test]
fn insert_nx() {
    let mut t = Ctx::new();
    arr_ints(
        &mut t,
        &["cf.insertnx", "cf", "items", "a", "b", "c"],
        &[1, 1, 1],
    );

    // Existing items return 0 (like CF.ADDNX).
    arr_ints(&mut t, &["cf.insertnx", "cf", "items", "a", "d"], &[0, 1]);
}

#[test]
fn insert_nx_nocreate() {
    let mut t = Ctx::new();
    t.assert_err(
        &["cf.insertnx", "cf", "nocreate", "items", "a"],
        "no such key",
    );

    t.ok(&["cf.reserve", "cf", "1000"]);
    arr_ints(
        &mut t,
        &["cf.insertnx", "cf", "nocreate", "items", "a"],
        &[1],
    );
}

#[test]
fn insert_nx_wrong_type() {
    let mut t = Ctx::new();
    t.ok(&["set", "str1", "foo"]);
    t.assert_err(&["cf.insertnx", "str1", "items", "a"], "WRONGTYPE");
}
