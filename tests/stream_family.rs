//! Probe for the XREAD/XREADGROUP reply wire format (RESP2).

mod common;

use common::*;

fn pair<'a>(v: &'a Value, key: &str) -> &'a [Value] {
    let pair = v.arr().expect("stream pair");
    assert_eq!(pair.len(), 2, "pair must be [key, entries]");
    assert_eq!(pair[0].text().as_deref(), Some(key), "pair key");
    pair[1].arr().expect("entries array")
}

fn entry(v: &Value, id: &str, k: &str, val: &str) {
    let e = v.arr().expect("entry");
    assert_eq!(e[0].text().as_deref(), Some(id), "entry id");
    let fields = e[1].arr().expect("entry fields");
    assert_eq!(fields.len(), 2, "entry field count");
    assert_eq!(fields[0].text().as_deref(), Some(k), "field name");
    assert_eq!(fields[1].text().as_deref(), Some(val), "field value");
}

#[test]
fn probe_xread_format() {
    let mut t = Ctx::new();
    t.text(&["xadd", "foo", "1-1", "k1", "v1"]);
    t.text(&["xadd", "foo", "1-2", "k2", "v2"]);
    t.text(&["xadd", "bar", "1-1", "k3", "v3"]);

    // Nested: [[foo, [[1-1, [k1, v1]], [1-2, [k2, v2]]]]]
    let outer = t.arr(&["xread", "streams", "foo", "0"]);
    assert_eq!(outer.len(), 1, "one stream pair");
    let entries = pair(&outer[0], "foo");
    assert_eq!(entries.len(), 2);
    entry(&entries[0], "1-1", "k1", "v1");
    entry(&entries[1], "1-2", "k2", "v2");

    // Two streams, input order preserved.
    let outer = t.arr(&["xread", "streams", "foo", "bar", "0", "0"]);
    assert_eq!(outer.len(), 2, "two stream pairs");
    assert_eq!(pair(&outer[0], "foo").len(), 2);
    assert_eq!(pair(&outer[1], "bar").len(), 1);

    // Missing stream: null array.
    assert!(matches!(
        t.run(&["xread", "streams", "notfound", "0"]),
        Value::Array(None)
    ));

    // No new entries for `$` (already at the end): null array.
    assert!(matches!(
        t.run(&["xread", "streams", "foo", "$"]),
        Value::Array(None)
    ));

    // XREADGROUP with a concrete id and an empty PEL: null array.
    t.ok(&["xgroup", "create", "foo", "group", "0"]);
    assert!(matches!(
        t.run(&["xreadgroup", "group", "group", "alice", "streams", "foo", "0"]),
        Value::Array(None)
    ));

    // XREADGROUP `>` delivers everything since last_id 0-0.
    let outer = t.arr(&["xreadgroup", "group", "group", "alice", "streams", "foo", ">"]);
    assert_eq!(outer.len(), 1);
    let entries = pair(&outer[0], "foo");
    assert_eq!(entries.len(), 2);
    entry(&entries[0], "1-1", "k1", "v1");

    // Nothing new: null array.
    assert!(matches!(
        t.run(&["xreadgroup", "group", "group", "alice", "streams", "foo", ">"]),
        Value::Array(None)
    ));

    // XREADGROUP PEL read (id 0) now has both entries, nested format.
    let outer = t.arr(&["xreadgroup", "group", "group", "alice", "streams", "foo", "0"]);
    assert_eq!(outer.len(), 1);
    let entries = pair(&outer[0], "foo");
    assert_eq!(entries.len(), 2);
    entry(&entries[0], "1-1", "k1", "v1");
    entry(&entries[1], "1-2", "k2", "v2");
}
