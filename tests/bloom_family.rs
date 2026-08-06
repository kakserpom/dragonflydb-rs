//! Port of `dragonfly/src/server/bloom_family_test.cc`.
//!
//! Adaptations from the C++ original:
//! - The C++ `Run`/`RespExpr` replies are decoded here through the harness
//!   `Value` enum; `resp.GetVec()` on BF.SCANDUMP/INFO becomes `v.arr()`.
//! - BF.SCANDUMP/LOADCHUNK carry binary chunk data, so they are sent via
//!   `run_b` instead of the string-based `run`.
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

/// `BF.SCANDUMP key cursor` -> (next cursor, chunk data).
fn scandump(t: &mut Ctx, key: &str, cursor: i64) -> (i64, Vec<u8>) {
    let v = t.run_b(&[
        b"bf.scandump".to_vec(),
        key.as_bytes().to_vec(),
        cursor.to_string().into_bytes(),
    ]);
    let a = v.arr().expect("expected [cursor, data]");
    assert_eq!(a.len(), 2, "reply {v:?}");
    let next = a[0].int().expect("cursor");
    let data = a[1].bulk().expect("data").to_vec();
    (next, data)
}

/// `BF.LOADCHUNK key cursor <binary data>`.
fn loadchunk(t: &mut Ctx, key: &str, cursor: i64, data: &[u8]) {
    let v = t.run_b(&[
        b"bf.loadchunk".to_vec(),
        key.as_bytes().to_vec(),
        cursor.to_string().into_bytes(),
        data.to_vec(),
    ]);
    expect_ok(&v);
}

#[test]
fn basic() {
    let mut t = Ctx::new();
    t.ok(&["bf.reserve", "b1", "0.1", "32"]);
    t.assert_text(&["type", "b1"], "MBbloom--");
    t.assert_int(&["bf.add", "b1", "a"], 1);
    t.assert_int(&["bf.add", "b1", "b"], 1);
    t.assert_int(&["bf.add", "b1", "b"], 0);
    t.assert_int(&["bf.add", "b2", "b"], 1);
    t.assert_text(&["type", "b2"], "MBbloom--");

    t.assert_int(&["bf.exists", "b2", "c"], 0);
    t.assert_int(&["bf.exists", "b3", "c"], 0);
    t.assert_int(&["bf.exists", "b2", "b"], 1);
    t.ok(&["set", "str", "foo"]);
    t.assert_int(&["bf.exists", "str", "b"], 0);
}

#[test]
fn multiple() {
    let mut t = Ctx::new();
    arr_ints(&mut t, &["bf.mexists", "bf1", "a", "b", "c"], &[0, 0, 0]);

    t.ok(&["set", "str", "foo"]);
    arr_ints(&mut t, &["bf.mexists", "str", "a", "b", "c"], &[0, 0, 0]);

    t.assert_err(&["bf.madd", "str", "a"], "WRONGTYPE");

    arr_ints(&mut t, &["bf.madd", "bf1", "a", "b", "c"], &[1, 1, 1]);
    arr_ints(&mut t, &["bf.madd", "bf1", "a", "b", "c"], &[0, 0, 0]);
    arr_ints(&mut t, &["bf.mexists", "bf1", "a", "b", "c"], &[1, 1, 1]);
}

#[test]
fn scan_dump() {
    let mut t = Ctx::new();
    t.ok(&["bf.reserve", "b1", "0.01", "1000"]);
    for i in 0..100 {
        t.int(&["bf.add", "b1", &format!("item{i}")]);
    }

    let (mut cursor, data) = scandump(&mut t, "b1", 0);
    assert_eq!(cursor, 1);
    assert!(!data.is_empty());

    let mut chunk_count = 1;
    while cursor != 0 {
        let (next, data) = scandump(&mut t, "b1", cursor);
        assert!(next > cursor || next == 0);
        cursor = next;
        if cursor != 0 {
            chunk_count += 1;
            assert!(!data.is_empty());
        } else {
            assert!(data.is_empty());
        }
    }
    assert!(chunk_count >= 1);
}

#[test]
fn chunk_round_trip() {
    const TOTAL_ITEMS: usize = 100;
    let mut t = Ctx::new();
    t.ok(&["bf.reserve", "b1", "0.01", "1000"]);
    for i in 0..TOTAL_ITEMS {
        t.int(&["bf.add", "b1", &format!("item{i}")]);
    }

    let mut chunks: Vec<(i64, Vec<u8>)> = Vec::new();
    let mut cursor = 0i64;
    loop {
        let (next, data) = scandump(&mut t, "b1", cursor);
        assert!(next > cursor || next == 0);
        cursor = next;
        if cursor != 0 {
            assert!(!data.is_empty());
            chunks.push((cursor, data));
        }
        if cursor == 0 {
            break;
        }
    }
    assert!(chunks.len() >= 2, "header + filter chunks");

    for (crs, data) in &chunks {
        loadchunk(&mut t, "b2", *crs, data);
    }

    for i in 0..TOTAL_ITEMS {
        t.assert_int(&["bf.exists", "b2", &format!("item{i}")], 1);
    }
}

#[test]
fn scan_dump_past_end() {
    let mut t = Ctx::new();
    t.ok(&["bf.reserve", "b1", "0.01", "100"]);
    t.int(&["bf.add", "b1", "x"]);

    let (cursor, data) = scandump(&mut t, "b1", 999_999);
    assert_eq!(cursor, 0);
    assert!(data.is_empty());
}

#[test]
fn load_chunk_errors() {
    let mut t = Ctx::new();
    t.assert_err(&["bf.loadchunk", "b1", "0", "data"], "not an integer");
    t.assert_err(&["bf.loadchunk", "b1", "-1", "data"], "not an integer");
}

#[test]
fn info() {
    let mut t = Ctx::new();
    t.assert_err(&["bf.info", "missing"], "no such key");

    t.ok(&["bf.reserve", "b1", "0.01", "1000"]);
    let v = t.arr(&["bf.info", "b1"]);
    assert_eq!(v.len(), 10, "reply {v:?}");
    assert_eq!(v[0].text().as_deref(), Some("Capacity"));
    assert_eq!(v[1].int(), Some(1485));
    assert_eq!(v[2].text().as_deref(), Some("Size"));
    assert!(v[3].int().is_some_and(|n| n > 0));
    assert_eq!(v[4].text().as_deref(), Some("Number of filters"));
    assert_eq!(v[5].int(), Some(1));
    assert_eq!(v[6].text().as_deref(), Some("Number of items inserted"));
    assert_eq!(v[7].int(), Some(0));
    assert_eq!(v[8].text().as_deref(), Some("Expansion rate"));
    assert_eq!(v[9].int(), Some(2));

    for i in 0..10 {
        t.int(&["bf.add", "b1", &format!("item{i}")]);
    }
    t.assert_int(&["bf.info", "b1", "items"], 10);
    t.assert_int(&["bf.info", "b1", "filters"], 1);
    t.assert_err(&["bf.info", "b1", "bogus"], "Invalid info arguments");

    t.ok(&["set", "str", "foo"]);
    t.assert_err(&["bf.info", "str"], "WRONGTYPE");
}

#[test]
fn copy_chunked_round_trip() {
    const TOTAL_ITEMS: usize = 100;
    let mut t = Ctx::new();
    t.ok(&["bf.reserve", "b1", "0.01", "1000"]);
    for i in 0..TOTAL_ITEMS {
        t.int(&["bf.add", "b1", &format!("item{i}")]);
    }

    t.assert_int(&["copy", "b1", "b2"], 1);

    for i in 0..TOTAL_ITEMS {
        t.assert_int(&["bf.exists", "b2", &format!("item{i}")], 1);
    }
}
