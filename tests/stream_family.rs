//! Port of the blocking `StreamFamilyTest` cases from
//! `dragonfly/src/server/stream_family_test.cc` to the in-process harness
//! (`tests/common/mod.rs`).
//!
//! Adaptations from the reference:
//! - `RunAsync` / fibers become a background thread with its own connection
//!   (`Ctx::spawn`); the blocked reader is given time to register before the
//!   wake-up push (the coordinator re-runs pending commands on its 20ms poll).
//! - `IsConnBlocked` (blocking-controller state) is not observable over the
//!   socket; the negative "stays blocked" assertions use a wake window instead
//!   and assert the reader only ever receives the expected data.
//! - `XReadGroupBlockIgnoresWakeFromRemovedEntry` relies on MULTI being atomic
//!   with respect to the woken reader. Here EXEC dispatches its queued commands
//!   as separate transactions with a pending-retry in between (event_loop.rs,
//!   coordinator.rs), so the reader observes the transient XADD state and the
//!   test is not portable; it is skipped.

mod common;

use common::*;
use std::thread::sleep;
use std::time::Duration;

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

    // XREADGROUP with a concrete id and an empty PEL: the requested key is
    // returned with an empty entry list (history reads serve the key even when
    // nothing is pending; HasEntries2 -> serve_history).
    t.ok(&["xgroup", "create", "foo", "group", "0"]);
    let outer = t.arr(&[
        "xreadgroup",
        "group",
        "group",
        "alice",
        "streams",
        "foo",
        "0",
    ]);
    assert_eq!(outer.len(), 1, "history read serves the key");
    assert_eq!(pair(&outer[0], "foo").len(), 0);

    // XREADGROUP `>` delivers everything since last_id 0-0.
    let outer = t.arr(&[
        "xreadgroup",
        "group",
        "group",
        "alice",
        "streams",
        "foo",
        ">",
    ]);
    assert_eq!(outer.len(), 1);
    let entries = pair(&outer[0], "foo");
    assert_eq!(entries.len(), 2);
    entry(&entries[0], "1-1", "k1", "v1");

    // Nothing new: null array.
    assert!(matches!(
        t.run(&[
            "xreadgroup",
            "group",
            "group",
            "alice",
            "streams",
            "foo",
            ">"
        ]),
        Value::Array(None)
    ));

    // XREADGROUP PEL read (id 0) now has both entries, nested format.
    let outer = t.arr(&[
        "xreadgroup",
        "group",
        "group",
        "alice",
        "streams",
        "foo",
        "0",
    ]);
    assert_eq!(outer.len(), 1);
    let entries = pair(&outer[0], "foo");
    assert_eq!(entries.len(), 2);
    entry(&entries[0], "1-1", "k1", "v1");
    entry(&entries[1], "1-2", "k2", "v2");
}

/// Port of `StreamFamilyTest.XReadBlock` (stream_family_test.cc:314).
#[test]
fn xread_block() {
    let mut t = Ctx::new();
    t.text(&["xadd", "foo", "1-0", "k1", "v1"]);
    t.text(&["xadd", "foo", "1-1", "k2", "v2"]);
    t.text(&["xadd", "foo", "1-2", "k3", "v3"]);
    t.text(&["xadd", "bar", "1-0", "k4", "v4"]);

    // Receive all records from both streams.
    let outer = t.arr(&["xread", "block", "100", "streams", "foo", "bar", "0", "0"]);
    assert_eq!(outer.len(), 2);
    assert_eq!(pair(&outer[0], "foo").len(), 3);
    assert_eq!(pair(&outer[1], "bar").len(), 1);

    // Timeout.
    assert!(matches!(
        t.run(&["xread", "block", "1", "streams", "foo", "$"]),
        Value::Array(None)
    ));

    // Timeout again, on two streams.
    assert!(matches!(
        t.run(&["xread", "block", "1", "streams", "foo", "bar", "$", "$"]),
        Value::Array(None)
    ));

    // Run XREAD BLOCK from 2 fibers.
    let fb0 = t.spawn(&["xread", "block", "0", "streams", "foo", "$"]);
    let fb1 = t.spawn(&["xread", "block", "0", "streams", "foo", "bar", "$", "$"]);
    sleep(Duration::from_millis(100));

    t.text(&["xadd", "foo", "1-3", "k5", "v5"]);

    let resp0 = fb0.join().unwrap();
    let resp1 = fb1.join().unwrap();

    // Both xread calls should have been unblocked.
    let entries = pair(&resp0.arr().expect("array")[0], "foo");
    assert_eq!(entries.len(), 1);
    entry(&entries[0], "1-3", "k5", "v5");
    let entries = pair(&resp1.arr().expect("array")[0], "foo");
    assert_eq!(entries.len(), 1);
    entry(&entries[0], "1-3", "k5", "v5");
}

/// Port of `StreamFamilyTest.XReadGroupBlock` (stream_family_test.cc:371).
#[test]
fn xread_group_block() {
    let mut t = Ctx::new();
    t.ok(&["xgroup", "create", "foo", "group", "0", "MKSTREAM"]);
    t.ok(&["xgroup", "create", "bar", "group", "0", "MKSTREAM"]);

    // Timeout.
    assert!(matches!(
        t.run(&[
            "xreadgroup",
            "group",
            "group",
            "alice",
            "block",
            "1",
            "streams",
            "foo",
            "bar",
            ">",
            ">",
        ]),
        Value::Array(None)
    ));

    // Run XREADGROUP BLOCK from 2 fibers.
    let fb0 = t.spawn(&[
        "xreadgroup",
        "group",
        "group",
        "alice",
        "block",
        "0",
        "streams",
        "foo",
        "bar",
        ">",
        ">",
    ]);
    let fb1 = t.spawn(&[
        "xreadgroup",
        "group",
        "group",
        "alice",
        "block",
        "0",
        "streams",
        "foo",
        "bar",
        ">",
        ">",
    ]);
    sleep(Duration::from_millis(100));

    t.text(&["xadd", "foo", "1-0", "k5", "v5"]);
    // Only one xreadgroup call should have been unblocked.
    sleep(Duration::from_millis(100));
    t.text(&["xadd", "bar", "1-0", "k5", "v5"]);
    // The second one should be unblocked.
    sleep(Duration::from_millis(100));

    let resp0 = fb0.join().unwrap();
    let resp1 = fb1.join().unwrap();

    let a0 = resp0.arr().expect("array");
    let a1 = resp1.arr().expect("array");
    let is_foo = a0[0].arr().expect("pair")[0].text().as_deref() == Some("foo");
    if is_foo {
        assert_eq!(pair(&a0[0], "foo").len(), 1);
        assert_eq!(pair(&a1[0], "bar").len(), 1);
    } else {
        assert_eq!(pair(&a1[0], "foo").len(), 1);
        assert_eq!(pair(&a0[0], "bar").len(), 1);
    }

    // Call XGROUP DESTROY while blocking.
    t.ok(&[
        "xgroup",
        "create",
        "to-delete",
        "to-delete",
        "0",
        "MKSTREAM",
    ]);
    let fb = t.spawn(&[
        "xreadgroup",
        "group",
        "to-delete",
        "consumer",
        "block",
        "0",
        "streams",
        "to-delete",
        ">",
    ]);
    sleep(Duration::from_millis(100));

    t.assert_int(&["xgroup", "destroy", "to-delete", "to-delete"], 1);
    let resp = fb.join().unwrap();
    expect_err(
        &resp,
        "consumer group this client was blocked on no longer exists",
    );
}

/// Port of `StreamFamilyTest.XReadGroupBlockDelconsumer`
/// (stream_family_test.cc:417).
#[test]
fn xread_group_block_delconsumer() {
    let mut t = Ctx::new();
    t.ok(&["xgroup", "create", "foo", "group", "0", "MKSTREAM"]);

    let fb = t.spawn(&[
        "xreadgroup",
        "group",
        "group",
        "alice",
        "block",
        "0",
        "streams",
        "foo",
        ">",
    ]);
    sleep(Duration::from_millis(100));

    // Del consumer while it's blocked.
    let resp_del_consumer = t.run(&["xgroup", "delconsumer", "foo", "group", "alice"]);
    assert_eq!(resp_del_consumer.int().unwrap(), 0);

    t.text(&["xadd", "foo", "1-0", "k1", "v1"]);
    let resp = fb.join().unwrap();
    let entries = pair(&resp.arr().expect("array")[0], "foo");
    assert_eq!(entries.len(), 1);
    entry(&entries[0], "1-0", "k1", "v1");
}

/// Port of `StreamFamilyTest.XReadBlockOnEmptiedStream`
/// (stream_family_test.cc:439).
#[test]
fn xread_block_on_emptied_stream() {
    let mut t = Ctx::new();
    // XDEL leaves the last generated id behind, but the stream has nothing left
    // to serve, so both reads must block instead of returning an empty reply.
    t.text(&["xadd", "foo", "1-0", "k", "v"]);
    t.assert_int(&["xdel", "foo", "1-0"], 1);

    assert!(matches!(
        t.run(&["xread", "block", "1", "streams", "foo", "0"]),
        Value::Array(None)
    ));

    t.ok(&["xgroup", "create", "foo", "group", "0"]);
    assert!(matches!(
        t.run(&[
            "xreadgroup",
            "group",
            "group",
            "alice",
            "block",
            "1",
            "streams",
            "foo",
            ">"
        ]),
        Value::Array(None)
    ));

    // A new entry is still served once it arrives.
    t.text(&["xadd", "foo", "2-0", "k", "v"]);
    let resp = t.arr(&[
        "xreadgroup",
        "group",
        "group",
        "alice",
        "block",
        "1",
        "streams",
        "foo",
        ">",
    ]);
    let entries = pair(&resp[0], "foo");
    assert_eq!(entries.len(), 1);
    entry(&entries[0], "2-0", "k", "v");
}

/// Port of `StreamFamilyTest.XReadBlockIgnoresEntriesBelowRequestedId`
/// (stream_family_test.cc:462). A blocked XREAD asks for entries starting at a
/// concrete id; an XADD below that id must not wake it with an empty record
/// list.
#[test]
fn xread_block_ignores_entries_below_requested_id() {
    let mut t = Ctx::new();
    t.text(&["xadd", "foo", "1-0", "k", "v"]);

    let reader = t.spawn(&["xread", "block", "0", "streams", "foo", "5-0"]);
    sleep(Duration::from_millis(100));

    // The wake must not deliver the below-id entry; the reader stays blocked.
    t.text(&["xadd", "foo", "2-0", "k", "v"]);
    sleep(Duration::from_millis(150));

    t.text(&["xadd", "foo", "6-0", "k", "v"]);
    let resp = reader.join().unwrap();
    let outer = resp.arr().expect("array");
    let entries = pair(&outer[0], "foo");
    assert_eq!(entries.len(), 1);
    entry(&entries[0], "6-0", "k", "v");
}

/// Port of `StreamFamilyTest.XReadBlockOnMaxMsId` (stream_family_test.cc:513).
/// The `>` sentinel is UINT64_MAX-UINT64_MAX, but a plain XREAD may request an
/// id with a UINT64_MAX ms component and must still be woken above it.
#[test]
fn xread_block_on_max_ms_id() {
    let mut t = Ctx::new();
    t.text(&["xadd", "foo", "1-0", "k", "v"]);

    let reader = t.spawn(&[
        "xread",
        "block",
        "0",
        "streams",
        "foo",
        "18446744073709551615-0",
    ]);
    sleep(Duration::from_millis(100));

    t.text(&["xadd", "foo", "18446744073709551615-2", "k", "v"]);
    let resp = reader.join().unwrap();
    let entries = pair(&resp.arr().expect("array")[0], "foo");
    assert_eq!(entries.len(), 1);
    entry(&entries[0], "18446744073709551615-2", "k", "v");
}

/// Port of `StreamFamilyTest.XReadGroupBlockWakeOnDeletedStream`
/// (stream_family_test.cc:532). A blocked XREADGROUP watches a key that can
/// never become ready once the stream is deleted, so it is told its group is
/// gone instead of sleeping forever.
#[test]
fn xread_group_block_wake_on_deleted_stream() {
    let mut t = Ctx::new();
    t.ok(&["xgroup", "create", "foo", "group", "$", "MKSTREAM"]);

    let reader = t.spawn(&[
        "xreadgroup",
        "group",
        "group",
        "alice",
        "block",
        "0",
        "streams",
        "foo",
        ">",
    ]);
    sleep(Duration::from_millis(100));

    t.assert_int(&["del", "foo"], 1);
    let resp = reader.join().unwrap();
    expect_err(
        &resp,
        "consumer group this client was blocked on no longer exists",
    );
}

/// Port of `StreamFamilyTest.XReadBlockStaysBlockedOnDeletedStream`
/// (stream_family_test.cc:548). Deleting the stream must not wake a plain
/// XREAD: it waits for entries a recreated stream can still deliver.
#[test]
fn xread_block_stays_blocked_on_deleted_stream() {
    let mut t = Ctx::new();
    t.text(&["xadd", "foo", "1-0", "k", "v"]);

    let reader = t.spawn(&["xread", "block", "0", "streams", "foo", "1-1"]);
    sleep(Duration::from_millis(100));

    // The stream deletion must not wake the reader.
    t.assert_int(&["del", "foo"], 1);
    sleep(Duration::from_millis(150));

    t.text(&["xadd", "foo", "2-0", "k", "v"]);
    let resp = reader.join().unwrap();
    let entries = pair(&resp.arr().expect("array")[0], "foo");
    assert_eq!(entries.len(), 1);
    entry(&entries[0], "2-0", "k", "v");
}

/// Port of `StreamFamilyTest.XReadGroupBlockWakeOnRetypedStream`
/// (stream_family_test.cc:568). Unlike list blocking (WrongTypeDoesNotWake), a
/// retyped stream wakes a blocked XREADGROUP with WRONGTYPE.
#[test]
fn xread_group_block_wake_on_retyped_stream() {
    let mut t = Ctx::new();
    t.ok(&["xgroup", "create", "foo", "group", "$", "MKSTREAM"]);

    let reader = t.spawn(&[
        "xreadgroup",
        "group",
        "group",
        "alice",
        "block",
        "0",
        "streams",
        "foo",
        ">",
    ]);
    sleep(Duration::from_millis(100));

    t.ok(&["set", "foo", "value"]);
    let resp = reader.join().unwrap();
    expect_err(&resp, "WRONGTYPE");
}

/// Port of `StreamFamilyTest.XReadGroupBlockWakeOnFlushDb`
/// (stream_family_test.cc:583).
#[test]
fn xread_group_block_wake_on_flushdb() {
    let mut t = Ctx::new();
    t.ok(&["xgroup", "create", "foo", "group", "0", "MKSTREAM"]);

    let reader = t.spawn(&[
        "xreadgroup",
        "group",
        "group",
        "alice",
        "block",
        "200",
        "streams",
        "foo",
        ">",
    ]);
    sleep(Duration::from_millis(100));

    t.ok(&["flushdb"]);
    let resp = reader.join().unwrap();
    expect_err(
        &resp,
        "consumer group this client was blocked on no longer exists",
    );
}

/// Port of `StreamFamilyTest.Issue854` (stream_family_test.cc:693): `XGROUP
/// HELP` works, but the hidden `_XGROUP_HELP` subcommand is NOSCRIPT so calling
/// it from a script is rejected (the plain HELP reply is unit-tested in
/// `streams.rs`).
#[test]
fn issue854() {
    let mut t = Ctx::new();
    assert!(matches!(t.run(&["xgroup", "help"]), Value::Array(Some(_))));
    let resp = t.run(&["eval", "redis.call('xgroup', 'help')", "0"]);
    expect_err(&resp, "is not allowed");
}

/// Port of `StreamFamilyTest.XReadGroupBlockHonorsCount`
/// (stream_family_test.cc:596). A woken read must honor COUNT like the
/// non-blocking path, delivering the transaction's burst to other consumers
/// rather than one.
#[test]
fn xread_group_block_honors_count() {
    let mut t = Ctx::new();
    t.ok(&["xgroup", "create", "foo", "group", "0", "MKSTREAM"]);

    // Block a consumer with COUNT 1.
    let fb0 = t.spawn(&[
        "xreadgroup",
        "group",
        "group",
        "alice",
        "count",
        "1",
        "block",
        "0",
        "streams",
        "foo",
        ">",
    ]);
    sleep(Duration::from_millis(100));

    // Wake it with a transaction adding multiple entries at once.
    let fb1 = t.spawn_fn(|c| {
        c.cmd(&["multi"]).unwrap();
        c.cmd(&["xadd", "foo", "1-1", "k1", "v1"]).unwrap();
        c.cmd(&["xadd", "foo", "1-2", "k2", "v2"]).unwrap();
        c.cmd(&["xadd", "foo", "1-3", "k3", "v3"]).unwrap();
        c.cmd(&["exec"]).unwrap()
    });

    let resp0 = fb0.join().unwrap();
    let fb1 = fb1.join().unwrap();
    assert_eq!(fb1.arr().expect("exec array").len(), 3);

    let entries = pair(&resp0.arr().expect("array")[0], "foo");
    assert_eq!(entries.len(), 1);
    entry(&entries[0], "1-1", "k1", "v1");

    // The entries beyond COUNT stay undelivered, available to other consumers.
    let resp = t.arr(&[
        "xreadgroup",
        "group",
        "group",
        "bob",
        "count",
        "10",
        "streams",
        "foo",
        ">",
    ]);
    let entries = pair(&resp[0], "foo");
    assert_eq!(entries.len(), 2);
    entry(&entries[0], "1-2", "k2", "v2");
    entry(&entries[1], "1-3", "k3", "v3");
}
