//! Port of `dragonfly/src/server/hset_family_test.cc` to the in-process
//! harness (`tests/common/mod.rs`).
//!
//! Adaptations from the reference:
//! - The reference's `TEST_current_time_ms` global + `AdvanceTime` become the
//!   port's process-global fake clock from `common` (`clock_guard` pins
//!   `now_ms()` to a whole-second base, `advance` moves it forward), so TTL
//!   replies are exact values instead of second-boundary ranges. Time-dependent
//!   tests run one at a time under a global mutex (the fake clock is
//!   process-wide, like the reference); tests without TTL assertions run in
//!   parallel and never observe it.
//! - The internal listpack/string-map encodings are not observable; DEBUG
//!   OBJECT encoding assertions are dropped and HSCAN's listpack behavior
//!   (returning every matching pair regardless of COUNT) is asserted instead.
//! - RESP3 (`HELLO 3`) is not supported by the harness, so the parameterized
//!   `Get` test and `HRandFieldRespFormat` cover only the RESP2 replies.
//! - `SHRINK` is a stub in the port (no bucket array to compact, replies 0),
//!   so `ShrinkMemoryAccountingHash` asserts the sequence runs cleanly rather
//!   than the freed bytes.

mod common;

use common::*;

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

/// Integer entries of an array reply in their reply order.
fn ints(v: &Value) -> Vec<i64> {
    v.arr()
        .unwrap_or_else(|| panic!("expected array, got {v:?}"))
        .iter()
        .map(|x| {
            x.int()
                .unwrap_or_else(|| panic!("expected integer, got {x:?}"))
        })
        .collect()
}

/// Text entries of an array reply, order-independent.
fn sorted(v: &Value) -> Vec<String> {
    let mut s = strs(v);
    s.sort();
    s
}

/// Deterministic splitmix64-style PRNG for the HSet mirror test.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
}

/// Random lowercase-hex string of `len` characters.
fn hex_str(rng: &mut Lcg, len: usize) -> String {
    let mut s = String::new();
    for _ in 0..len {
        s.push(char::from_digit(((rng.next() >> 33) % 16) as u32, 16).unwrap());
    }
    s
}

/// FIELDTTL key field, like the reference's `CheckedInt({"FIELDTTL", ...})`.
#[track_caller]
fn fieldttl(t: &mut Ctx, key: &str, field: &str) -> i64 {
    t.int(&["FIELDTTL", key, field])
}

#[test]
fn basic() {
    let mut t = Ctx::new();
    t.assert_err(&["hset", "x", "a"], "wrong number");
    t.assert_err(&["HSET", "hs", "key1", "val1", "key2"], "wrong number");

    t.assert_int(&["hset", "x", "a", "b"], 1);
    t.assert_int(&["hlen", "x"], 1);

    t.assert_int(&["hexists", "x", "a"], 1);
    t.assert_int(&["hexists", "x", "b"], 0);
    t.assert_int(&["hexists", "y", "a"], 0);

    t.assert_int(&["hset", "x", "a", "b"], 0);
    t.assert_int(&["hset", "x", "a", "c"], 0);
    t.assert_int(&["hset", "x", "a", ""], 0);

    t.assert_int(&["hset", "y", "a", "c", "d", "e"], 2);
    t.assert_int(&["hdel", "y", "a", "d"], 2);

    t.assert_int(&["hdel", "nokey", "a"], 0);
}

#[test]
fn hset() {
    let mut t = Ctx::new();
    // Simulate HSET on a mirror map, checking how many new entries were added.
    let mut mirror: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut rng = Lcg(0x1234_5678_9abc_def0);
    while mirror.len() < 600 {
        let mut cmd = vec!["HSET", "hash"];
        let mut new_values = 0i64;
        let mut args: Vec<String> = Vec::new();
        for _ in 0..20 {
            let key = hex_str(&mut rng, 3);
            let value = hex_str(&mut rng, 20);
            new_values += i64::from(!mirror.contains_key(&key));
            mirror.insert(key.clone(), value.clone());
            args.push(key);
            args.push(value);
        }
        let argv: Vec<&str> = cmd
            .iter()
            .copied()
            .chain(args.iter().map(String::as_str))
            .collect();
        let v = t.run(&argv);
        expect_int(&v, new_values);
    }

    // Verify consistency.
    t.assert_int(&["HLEN", "hash"], mirror.len() as i64);
    for (key, value) in &mirror {
        t.assert_text(&["HGET", "hash", key], value);
    }

    // HSET with the same key twice.
    t.run(&["HSET", "hash", "key1", "value1", "key1", "value2"]);
    t.assert_text(&["HGET", "hash", "key1"], "value2");

    // Wrong arity cases.
    t.assert_err(&["HSET", "key"], "wrong number of arguments");
    t.assert_err(&["HSET", "key", "key"], "wrong number of arguments");
    t.assert_err(
        &["HSET", "key", "key", "value", "key2"],
        "wrong number of arguments",
    );
}

#[test]
fn hsetnx() {
    let mut t = Ctx::new();
    t.assert_int(&["HSETNX", "hash", "key1", "value1"], 1);
    t.assert_text(&["HGET", "hash", "key1"], "value1");

    t.assert_int(&["HSETNX", "hash", "key1", "value2"], 0);
    t.assert_text(&["HGET", "hash", "key1"], "value1");

    t.assert_err(&["HSETNX", "key"], "wrong number of arguments");
    t.assert_err(&["HSET", "key", "key"], "wrong number of arguments");
}

#[test]
fn mixed_types() {
    let mut t = Ctx::new();
    for i in 0..100 {
        let key1 = format!("s{i}");
        let key2 = format!("i{i}");
        t.assert_int(&["HSET", "hash", &key1, "VALUE", &key2, "123456"], 2);
    }

    for i in 0..100 {
        let key1 = format!("s{i}");
        let key2 = format!("i{i}");
        t.assert_text(&["HGET", "hash", &key1], "VALUE");
        t.assert_text(&["HGET", "hash", &key2], "123456");
        t.assert_int(&["hincrby", "hash", &key2, "1"], 123457);
    }
}

// The reference parameterizes this over RESP2/RESP3 (`HELLO 2` / `HELLO 3`);
// the harness only speaks RESP2, so the RESP3 half is dropped.
#[test]
fn get() {
    let mut t = Ctx::new();
    t.assert_int(&["hset", "x", "a", "1", "b", "2", "c", "3"], 3);

    let v = t.run(&["hmget", "unkwn", "a", "c"]);
    assert_eq!(
        v.arr().map(<[Value]>::to_vec),
        Some(vec![Value::Bulk(None), Value::Bulk(None)])
    );

    assert_eq!(sorted(&t.run(&["hkeys", "x"])), vec!["a", "b", "c"]);
    assert_eq!(sorted(&t.run(&["hvals", "x"])), vec!["1", "2", "3"]);

    let v = t.run(&["hmget", "x", "a", "c", "d"]);
    assert_eq!(
        v.arr().map(<[Value]>::to_vec),
        Some(vec![
            Value::Bulk(Some(b"1".to_vec())),
            Value::Bulk(Some(b"3".to_vec())),
            Value::Bulk(None),
        ])
    );

    let v = t.run(&["hmget", "x", "a", "c", "d", "d", "c", "a"]);
    assert_eq!(
        v.arr().map(<[Value]>::to_vec),
        Some(vec![
            Value::Bulk(Some(b"1".to_vec())),
            Value::Bulk(Some(b"3".to_vec())),
            Value::Bulk(None),
            Value::Bulk(None),
            Value::Bulk(Some(b"3".to_vec())),
            Value::Bulk(Some(b"1".to_vec())),
        ])
    );

    // The small hash keeps insertion order, so the reply is the exact
    // key/value interleave (the reference asserts the same order).
    assert_eq!(
        strs(&t.run(&["hgetall", "x"])),
        vec!["a", "1", "b", "2", "c", "3"]
    );
}

#[test]
fn hincrby() {
    let mut t = Ctx::new();
    let mut total = 10i64;
    t.assert_int(&["hincrby", "key", "field", "10"], 10);
    t.assert_text(&["hget", "key", "field"], "10");

    let mut i = -100;
    while i < 100 {
        total += i;
        t.assert_int(&["hincrby", "key", "field", &i.to_string()], total);
        i += 7;
    }

    // Overflow.
    t.assert_int(
        &[
            "hset",
            "key",
            "field2",
            &i64::MAX.saturating_sub(1).to_string(),
        ],
        1,
    );
    t.assert_err(&["hincrby", "key", "field2", "2"], "would overflow");

    // Error case: a stored value that is not an integer.
    t.assert_int(&["hset", "key", "a", " 1"], 1);
    t.assert_err(
        &["hincrby", "key", "a", "10"],
        "hash value is not an integer",
    );
}

#[test]
fn hincr_respected() {
    let mut t = Ctx::new();
    t.assert_int(&["hset", "key", "a", "1"], 1);
    t.assert_int(&["hincrby", "key", "a", "10"], 11);
    // HGET returns the value as a bulk string (the reference CheckedInt coerces
    // it to an integer).
    t.assert_text(&["hget", "key", "a"], "11");
}

#[test]
fn hincr_cmds_preserve_ttl() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    t.assert_int(&["hsetex", "key", "2", "a", "1"], 1);
    let before = fieldttl(&mut t, "key", "a");
    assert_eq!(before, 2, "fieldttl = {before}");
    t.assert_int(&["hincrby", "key", "a", "1"], 2);
    // hincrby must preserve the field TTL exactly.
    let after = fieldttl(&mut t, "key", "a");
    assert_eq!(after, before, "fieldttl = {after}, before = {before}");

    // Once the field expires, the next hincrby starts from a fresh TTL-less
    // field.
    advance(2000);
    t.assert_int(&["hincrby", "key", "a", "1"], 1);
    t.assert_int(&["fieldttl", "key", "a"], -1);

    t.assert_int(&["hsetex", "key", "2", "fl", "1.1"], 1);
    t.assert_text(&["hincrbyfloat", "key", "fl", "1.1"], "2.2");
}

#[test]
fn hscan() {
    let mut t = Ctx::new();
    let v = t.run(&["hscan", "non-existing-key", "100", "count", "5"]);
    assert_eq!(
        v.arr().map(<[Value]>::to_vec),
        Some(vec![
            Value::Bulk(Some(b"0".to_vec())),
            Value::Array(Some(vec![])),
        ])
    );

    for i in 0..10 {
        t.run(&[
            "HSET",
            "myhash",
            &format!("Field-{i}"),
            &format!("Value-{i}"),
        ]);
    }

    // The small hash is scanned whole: even though COUNT is 4, all 10 fields
    // (20 entries) come back.
    let v = t.run(&["hscan", "myhash", "0", "count", "4"]);
    assert_eq!(v.arr().map(<[Value]>::to_vec).map(|x| x.len()), Some(2));
    let mut vec = strs(&v.arr().unwrap()[1]);
    assert_eq!(vec.len(), 20);
    assert!(
        vec.iter()
            .all(|s| s.starts_with("Field") || s.starts_with("Value"))
    );

    // A pattern matching nothing.
    let v = t.run(&["hscan", "myhash", "0", "match", "*x*"]);
    vec = strs(&v.arr().unwrap()[1]);
    assert!(vec.is_empty());

    // A positive match: only Field-1 / Value-1 contain "1".
    let v = t.run(&["hscan", "myhash", "0", "match", "*1*"]);
    vec = strs(&v.arr().unwrap()[1]);
    assert_eq!(vec.len(), 2);

    // A large hash limits the number of returned entries.
    for i in 0..200 {
        t.run(&[
            "HSET",
            "largehash",
            &format!("KeyNum-{i}"),
            &format!("KeyValue-{i}"),
        ]);
    }
    let v = t.run(&["hscan", "largehash", "0", "count", "20"]);
    assert_eq!(v.arr().map(<[Value]>::to_vec).map(|x| x.len()), Some(2));
    vec = strs(&v.arr().unwrap()[1]);
    // COUNT is a hint, not an exact limit; the reference returns between 40
    // and 60 entries for COUNT 20.
    assert!(
        (40..60).contains(&vec.len()),
        "largehash scan size = {}",
        vec.len()
    );

    // NOVALUES returns only the fields.
    let v = t.run(&["hscan", "myhash", "0", "NOVALUES"]);
    assert_eq!(v.arr().map(<[Value]>::to_vec).map(|x| x.len()), Some(2));
    vec = strs(&v.arr().unwrap()[1]);
    assert_eq!(vec.len(), 10);
    assert!(vec.iter().all(|s| s.starts_with("Field")));
}

#[test]
fn hscan_no_values_combinations() {
    let mut t = Ctx::new();
    t.run(&[
        "HSET", "h_combos", "user:1", "v1", "user:2", "v2", "admin:1", "v3",
    ]);

    // MATCH + NOVALUES.
    let v = t.run(&["HSCAN", "h_combos", "0", "MATCH", "user:*", "NOVALUES"]);
    assert_eq!(v.arr().map(<[Value]>::to_vec).map(|x| x.len()), Some(2));
    let mut vec = sorted(&v.arr().unwrap()[1]);
    assert_eq!(vec, vec!["user:1", "user:2"]);

    // COUNT + NOVALUES on a larger hash.
    for i in 0..50 {
        t.run(&["HSET", "h_large", &format!("k{i}"), "v"]);
    }
    let v = t.run(&["HSCAN", "h_large", "0", "COUNT", "10", "NOVALUES"]);
    vec = strs(&v.arr().unwrap()[1]);
    assert!(!vec.is_empty());
    assert!(vec.iter().all(|s| s != "v"));
    assert!(vec.iter().all(|s| s.starts_with('k')));
}

#[test]
fn hscan_lp_match_bug() {
    let mut t = Ctx::new();
    t.assert_int(&["HSET", "key", "1", "2"], 1);
    let v = t.run(&["hscan", "key", "0", "match", "1"]);
    assert_eq!(v.arr().map(<[Value]>::to_vec).map(|x| x.len()), Some(2));
    assert_eq!(strs(&v.arr().unwrap()[1]), vec!["1", "2"]);
}

#[test]
fn hincrby_float() {
    let mut t = Ctx::new();
    t.run(&["hincrbyfloat", "k", "a", "1.5"]);
    t.assert_text(&["hget", "k", "a"], "1.5");

    t.run(&["hincrbyfloat", "k", "a", "1.5"]);
    t.assert_text(&["hget", "k", "a"], "3");

    for i in 0..500 {
        t.run(&["hincrbyfloat", "k", &format!("v{i}"), "1.5"]);
    }
    for i in 0..500 {
        t.assert_text(&["hget", "k", &format!("v{i}")], "1.5");
    }
}

#[test]
fn hincrby_float_corner_cases() {
    let mut t = Ctx::new();
    t.assert_int(
        &[
            "hset",
            "k",
            "mhv",
            "-1.8E+308",
            "phv",
            "1.8E+308",
            "nd",
            "-+-inf",
            "+inf",
            "+inf",
            "nan",
            "nan",
            "-inf",
            "-inf",
        ],
        6,
    );
    // Long doubles are not supported: all these stored values fail to parse.
    t.assert_err(
        &["hincrbyfloat", "k", "mhv", "-1"],
        "ERR hash value is not a float",
    );
    t.assert_err(
        &["hincrbyfloat", "k", "phv", "1"],
        "ERR hash value is not a float",
    );
    t.assert_err(
        &["hincrbyfloat", "k", "nd", "1"],
        "ERR hash value is not a float",
    );
    t.assert_err(
        &["hincrbyfloat", "k", "+inf", "1"],
        "increment would produce NaN or Infinity",
    );
    t.assert_err(
        &["hincrbyfloat", "k", "nan", "1"],
        "ERR hash value is not a float",
    );
    t.assert_err(
        &["hincrbyfloat", "k", "-inf", "1"],
        "increment would produce NaN or Infinity",
    );
}

#[test]
fn hrand_float() {
    let mut t = Ctx::new();
    t.assert_int(&["HSET", "k", "1", "2"], 1);
    t.assert_text(&["hrandfield", "k"], "1");

    for i in 0..500 {
        t.run(&["hincrbyfloat", "k", &format!("v{i}"), "1.1"]);
    }
    t.run(&["hrandfield", "k"]);
}

#[test]
fn hrand_field() {
    let mut t = Ctx::new();
    t.assert_int(&["HSET", "k", "a", "0", "b", "1", "c", "2"], 3);

    assert!(["a", "b", "c"].contains(&t.text(&["hrandfield", "k"]).as_str()));

    let mut fields = sorted(&t.run(&["hrandfield", "k", "2"]));
    assert!(fields.iter().all(|f| ["a", "b", "c"].contains(&f.as_str())));
    assert_eq!(fields.len(), 2);

    fields = sorted(&t.run(&["hrandfield", "k", "3"]));
    assert_eq!(fields, vec!["a", "b", "c"]);

    // COUNT greater than the hash size returns the whole hash.
    fields = sorted(&t.run(&["hrandfield", "k", "4"]));
    assert_eq!(fields, vec!["a", "b", "c"]);

    let v = t.run(&["hrandfield", "k", "4", "withvalues"]);
    let items = strs(&v);
    assert_eq!(items.len(), 6);
    let keys: Vec<&String> = items.iter().step_by(2).collect();
    let vals: Vec<&String> = items.iter().skip(1).step_by(2).collect();
    let mut keys_sorted: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    keys_sorted.sort();
    assert_eq!(keys_sorted, vec!["a", "b", "c"]);
    let mut vals_sorted = vals.clone();
    vals_sorted.sort();
    assert_eq!(vals_sorted, vec!["0", "1", "2"]);

    // Negative COUNT allows duplicates and pairs each key with its value.
    let v = t.run(&["hrandfield", "k", "-4", "withvalues"]);
    let items = strs(&v);
    assert_eq!(items.len(), 8);
    for pair in items.chunks(2) {
        let expect = match pair[0].as_str() {
            "a" => "0",
            "b" => "1",
            "c" => "2",
            other => panic!("unexpected field {other:?}"),
        };
        assert_eq!(pair[1], expect);
    }

    // A large (string-map) hash.
    let num_entries = 500usize;
    for i in 0..num_entries {
        t.run(&["HSET", "largehash", &i.to_string(), &(i * 10).to_string()]);
    }

    let v: i64 = t.text(&["hrandfield", "largehash"]).parse().unwrap();
    assert!((0..num_entries as i64).contains(&v));

    let fields = strs(&t.run(&["hrandfield", "largehash", &(num_entries / 2).to_string()]));
    let unique: std::collections::HashSet<&String> = fields.iter().collect();
    assert_eq!(unique.len(), fields.len(), "positive COUNT must not repeat");
    for f in &fields {
        let n: i64 = f.parse().unwrap();
        assert!((0..num_entries as i64).contains(&n));
    }

    let fields = strs(&t.run(&[
        "hrandfield",
        "largehash",
        &(-(num_entries as i64) - 1).to_string(),
    ]));
    assert_eq!(fields.len(), num_entries + 1);
    let unique: std::collections::HashSet<&String> = fields.iter().collect();
    assert!(unique.len() < fields.len(), "negative COUNT repeats fields");
    for f in &fields {
        let n: i64 = f.parse().unwrap();
        assert!((0..num_entries as i64).contains(&n));
    }

    let v = t.run(&[
        "hrandfield",
        "largehash",
        &(-(num_entries as i64) - 1).to_string(),
        "withvalues",
    ]);
    let items = strs(&v);
    assert_eq!(items.len(), (num_entries + 1) * 2);
    for pair in items.chunks(2) {
        let k: i64 = pair[0].parse().unwrap();
        let val: i64 = pair[1].parse().unwrap();
        assert_eq!(val, k * 10);
    }
}

#[test]
fn hsetex() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    t.assert_int(&["HSETEX", "k", "1", "f", "v"], 1);

    // f has a 1s TTL and must be gone after the clock advances one second.
    advance(1000);
    t.assert_null(&["HGET", "k", "f"]);

    let long_time = "100";
    t.assert_int(&["HSETEX", "k", long_time, "field1", "value"], 1);
    t.assert_int(&["HSETEX", "k", long_time, "field1", "new_value"], 0);
    t.assert_text(&["HGET", "k", "field1"], "new_value");

    t.assert_int(&["HSETEX", "k", long_time, "field2", "value"], 1);
    t.assert_int(&["HSETEX", "k", "NX", long_time, "field2", "new_value"], 0);
    t.assert_text(&["HGET", "k", "field2"], "value");

    // Re-setting with a shorter TTL replaces the old expiration.
    t.assert_int(&["HSETEX", "k", long_time, "field3", "value"], 1);
    t.assert_int(&["HSETEX", "k", "1", "field3", "value"], 0);
    advance(1000);
    t.assert_null(&["HGET", "k", "field3"]);

    // NX keeps the old expiration even when a short TTL is supplied.
    t.assert_int(&["HSETEX", "k", long_time, "field4", "value"], 1);
    t.assert_int(&["HSETEX", "k", "NX", "1", "field4", "value"], 0);
    advance(1000);
    t.assert_text(&["HGET", "k", "field4"], "value");

    // KEEPTTL resets the value but preserves the TTL.
    t.assert_int(&["HSETEX", "k", long_time, "kttlfield", "value"], 1);
    t.assert_text(&["HGET", "k", "kttlfield"], "value");
    assert_eq!(fieldttl(&mut t, "k", "kttlfield"), 100);

    // afield is added with a 2s TTL; kttlfield keeps its 100s TTL.
    t.assert_int(
        &[
            "HSETEX",
            "k",
            "KEEPTTL",
            "2",
            "kttlfield",
            "resetvalue",
            "afield",
            "aval",
        ],
        1,
    );
    assert_eq!(fieldttl(&mut t, "k", "kttlfield"), 100);
    assert_eq!(fieldttl(&mut t, "k", "afield"), 2);
    t.assert_text(&["HGET", "k", "afield"], "aval");

    // Let afield expire; kttlfield remains with its updated value.
    advance(2000);
    t.assert_null(&["HGET", "k", "afield"]);
    t.assert_text(&["HGET", "k", "kttlfield"], "resetvalue");
    assert_eq!(fieldttl(&mut t, "k", "kttlfield"), 98);

    // NX, with or without KEEPTTL, updates neither value nor expiry.
    t.assert_int(
        &["HSETEX", "k", "NX", "KEEPTTL", "2", "kttlfield", "value"],
        0,
    );
    t.assert_text(&["HGET", "k", "kttlfield"], "resetvalue");
    assert_eq!(fieldttl(&mut t, "k", "kttlfield"), 98);

    t.assert_int(&["HSETEX", "k", "NX", "2", "kttlfield", "value"], 0);
    t.assert_text(&["HGET", "k", "kttlfield"], "resetvalue");
    assert_eq!(fieldttl(&mut t, "k", "kttlfield"), 98);

    // Invalid TTL.
    t.assert_err(
        &["HSETEX", "k", "NX", "zero", "kttlfield", "value"],
        "ERR value is not an integer or out of range",
    );

    // KEEPTTL with no prior TTL applies the new one.
    t.assert_int(&["HSET", "k", "nottl", "val"], 1);
    t.assert_int(&["HSETEX", "k", "KEEPTTL", long_time, "nottl", "newval"], 0);
    assert_eq!(fieldttl(&mut t, "k", "nottl"), 100);

    // Repeated flags are syntax errors.
    t.assert_err(
        &["HSETEX", "k", "NX", "KEEPTTL", "NX", "1", "v", "v2"],
        "syntax error",
    );
    t.assert_err(
        &["HSETEX", "k", "KEEPTTL", "KEEPTTL", "1", "v", "v2"],
        "syntax error",
    );

    // No field-value pairs.
    t.assert_err(&["HSETEX", "k", "100"], "wrong number of arguments");
    t.assert_err(&["HSETEX", "k", "NX", "100"], "wrong number of arguments");
    t.assert_err(
        &["HSETEX", "k", "NX", "KEEPTTL", "100"],
        "wrong number of arguments",
    );
}

// FNX/FXX apply the collective set-all-or-nothing condition of the Redis
// format while keeping the Dragonfly reply (number of created fields).
#[test]
fn hsetex_dragonfly_condition() {
    let _clock = clock_guard();
    let mut t = Ctx::new();

    // FNX on a fresh key sets everything.
    t.assert_int(&["HSETEX", "dk", "FNX", "100", "a", "1", "b", "2"], 2);
    t.assert_text(&["HGET", "dk", "a"], "1");
    assert_eq!(fieldttl(&mut t, "dk", "a"), 100);

    // FNX fails because a/b already exist -> nothing set.
    t.assert_int(&["HSETEX", "dk", "FNX", "50", "a", "x"], 0);
    t.assert_text(&["HGET", "dk", "a"], "1");
    assert_eq!(fieldttl(&mut t, "dk", "a"), 100);
    t.assert_int(&["HSETEX", "dk", "FNX", "50", "a", "x", "newf", "y"], 0);
    t.assert_int(&["HEXISTS", "dk", "newf"], 0);

    // FXX applies because all fields exist; it overwrites value and TTL.
    t.assert_int(&["HSETEX", "dk", "FXX", "50", "a", "x"], 0);
    t.assert_text(&["HGET", "dk", "a"], "x");
    assert_eq!(fieldttl(&mut t, "dk", "a"), 50);
    // FXX fails because a field is missing -> nothing set.
    t.assert_int(&["HSETEX", "dk", "FXX", "50", "missing", "y"], 0);
    t.assert_int(&["HEXISTS", "dk", "missing"], 0);
    // FXX on a non-existing key fails and leaves no key behind.
    t.assert_int(&["HSETEX", "dk2", "FXX", "50", "a", "1"], 0);
    t.assert_int(&["EXISTS", "dk2"], 0);

    // KEEPTTL composes with the condition.
    t.assert_int(&["HSETEX", "dk", "FXX", "KEEPTTL", "10", "a", "z"], 0);
    t.assert_text(&["HGET", "dk", "a"], "z");
    assert_eq!(fieldttl(&mut t, "dk", "a"), 50);

    // NX and the collective conditions are mutually exclusive; so are repeats.
    t.assert_err(
        &["HSETEX", "dk", "NX", "FNX", "100", "a", "1"],
        "syntax error",
    );
    t.assert_err(
        &["HSETEX", "dk", "FNX", "FXX", "100", "a", "1"],
        "syntax error",
    );
    t.assert_err(
        &["HSETEX", "dk", "FNX", "FNX", "100", "a", "1"],
        "syntax error",
    );
}

#[test]
fn hsetex_redis_format() {
    let _clock = clock_guard();
    let mut t = Ctx::new();

    // Basic Redis format without expiry.
    t.assert_int(&["HSETEX", "k", "FIELDS", "2", "f1", "v1", "f2", "v2"], 1);
    t.assert_text(&["HGET", "k", "f1"], "v1");
    t.assert_text(&["HGET", "k", "f2"], "v2");
    assert_eq!(fieldttl(&mut t, "k", "f1"), -1);

    // EX seconds.
    t.assert_int(&["HSETEX", "k", "EX", "100", "FIELDS", "1", "exf", "v"], 1);
    assert_eq!(fieldttl(&mut t, "k", "exf"), 100);

    // PX milliseconds round up to whole seconds.
    t.assert_int(
        &["HSETEX", "k", "PX", "100000", "FIELDS", "1", "pxf", "v"],
        1,
    );
    assert_eq!(fieldttl(&mut t, "k", "pxf"), 100);

    // EXAT/PXAT absolute timestamps, computed against the pinned clock.
    let now_s = clock_ms() / 1000;
    t.assert_int(
        &[
            "HSETEX",
            "k",
            "EXAT",
            &(now_s + 100).to_string(),
            "FIELDS",
            "1",
            "exatf",
            "v",
        ],
        1,
    );
    assert_eq!(fieldttl(&mut t, "k", "exatf"), 100);
    t.assert_int(
        &[
            "HSETEX",
            "k",
            "PXAT",
            &((now_s + 100) * 1000).to_string(),
            "FIELDS",
            "1",
            "pxatf",
            "v",
        ],
        1,
    );
    assert_eq!(fieldttl(&mut t, "k", "pxatf"), 100);

    // Setting a field again without an expiry option removes its TTL.
    t.assert_int(&["HSETEX", "k", "FIELDS", "1", "exf", "v2"], 1);
    assert_eq!(fieldttl(&mut t, "k", "exf"), -1);

    // KEEPTTL retains the existing TTL while updating the value.
    t.assert_int(&["HSETEX", "k", "EX", "50", "FIELDS", "1", "kf", "v1"], 1);
    assert_eq!(fieldttl(&mut t, "k", "kf"), 50);
    t.assert_int(&["HSETEX", "k", "KEEPTTL", "FIELDS", "1", "kf", "v2"], 1);
    t.assert_text(&["HGET", "k", "kf"], "v2");
    assert_eq!(fieldttl(&mut t, "k", "kf"), 50);

    // FNX: only when none of the fields exist.
    t.assert_int(&["HSETEX", "k", "FNX", "FIELDS", "1", "fnxf", "v1"], 1);
    t.assert_int(&["HSETEX", "k", "FNX", "FIELDS", "1", "fnxf", "v2"], 0);
    t.assert_text(&["HGET", "k", "fnxf"], "v1");
    t.assert_int(
        &[
            "HSETEX", "k", "FNX", "FIELDS", "2", "fnxf", "x", "newf", "y",
        ],
        0,
    );
    t.assert_int(&["HEXISTS", "k", "newf"], 0);

    // FXX: only when all the fields exist.
    t.assert_int(&["HSETEX", "k", "FXX", "FIELDS", "1", "fnxf", "v3"], 1);
    t.assert_text(&["HGET", "k", "fnxf"], "v3");
    t.assert_int(&["HSETEX", "k", "FXX", "FIELDS", "1", "missing", "v"], 0);
    t.assert_int(&["HEXISTS", "k", "missing"], 0);
    t.assert_int(
        &[
            "HSETEX", "k", "FXX", "FIELDS", "2", "fnxf", "a", "missing2", "b",
        ],
        0,
    );
    t.assert_text(&["HGET", "k", "fnxf"], "v3");

    // FNX on a fresh key succeeds; FXX on a fresh key fails.
    t.assert_int(&["HSETEX", "nk", "FNX", "FIELDS", "1", "a", "b"], 1);
    t.assert_int(&["HSETEX", "nk2", "FXX", "FIELDS", "1", "a", "b"], 0);
    t.assert_int(&["EXISTS", "nk2"], 0);

    // Condition flags are mutually exclusive / may not be repeated.
    t.assert_err(
        &["HSETEX", "k", "FNX", "FXX", "FIELDS", "1", "f", "v"],
        "syntax error",
    );
    t.assert_err(
        &["HSETEX", "k", "FNX", "FNX", "FIELDS", "1", "f", "v"],
        "syntax error",
    );
    t.assert_err(
        &["HSETEX", "k", "FXX", "FXX", "FIELDS", "1", "f", "v"],
        "syntax error",
    );

    // Only one expiry option allowed.
    t.assert_err(
        &[
            "HSETEX", "k", "EX", "10", "KEEPTTL", "FIELDS", "1", "f", "v",
        ],
        "syntax error",
    );
    t.assert_err(
        &[
            "HSETEX", "k", "EX", "10", "PX", "10", "FIELDS", "1", "f", "v",
        ],
        "syntax error",
    );

    // Out-of-range / overflow-inducing expiries are rejected for every unit.
    t.assert_err(
        &[
            "HSETEX",
            "k",
            "PX",
            "9223372036854775807",
            "FIELDS",
            "1",
            "f",
            "v",
        ],
        "invalid expire time",
    );
    t.assert_err(
        &[
            "HSETEX",
            "k",
            "EXAT",
            "9223372036854775",
            "FIELDS",
            "1",
            "f",
            "v",
        ],
        "invalid expire time",
    );
    t.assert_err(
        &["HSETEX", "k", "EX", "0", "FIELDS", "1", "f", "v"],
        "invalid expire time",
    );
    // A non-integer expiry reports the integer error.
    t.assert_err(
        &["HSETEX", "k", "EX", "abc", "FIELDS", "1", "f", "v"],
        "value is not an integer or out of range",
    );
    // A past EXAT is rejected.
    t.assert_err(
        &["HSETEX", "k", "EXAT", "1", "FIELDS", "1", "f", "v"],
        "invalid expire time",
    );

    // numfields must match the number of field/value pairs.
    t.assert_err(&["HSETEX", "k", "FIELDS", "2", "f", "v"], "must match");
    t.assert_err(&["HSETEX", "k", "FIELDS", "0", "f", "v"], "must match");

    // A Redis-only flag without FIELDS falls through to the Dragonfly path,
    // where the non-numeric ttl_sec is rejected.
    t.assert_err(
        &["HSETEX", "k", "FNX", "f", "v"],
        "value is not an integer or out of range",
    );

    // EX/PX/EXAT/PXAT without FIELDS are a syntax error, not a positional ttl.
    t.assert_err(
        &["HSETEX", "k", "EX", "10", "100", "f", "v"],
        "syntax error",
    );
    t.assert_err(
        &["HSETEX", "k", "PX", "10", "100", "f", "v"],
        "syntax error",
    );

    // A field expiring during the FNX/FXX check must not leave an empty hash.
    t.assert_int(&["HSETEX", "exp", "1", "only", "v"], 1);
    advance(1000);
    t.assert_int(&["HSETEX", "exp", "FXX", "FIELDS", "1", "only", "v2"], 0);
    t.assert_int(&["EXISTS", "exp"], 0);
}

#[test]
fn trigger_convert_to_str_map() {
    let mut t = Ctx::new();
    let k_elements = 200usize;
    for i in 0..k_elements {
        t.run(&[
            "HSET",
            "hk",
            &(100_500_700u64 + i as u64).to_string(),
            "100500700",
        ]);
    }
    t.assert_int(&["HLEN", "hk"], k_elements as i64);
}

// The reference also asserts the DEBUG OBJECT encodings (listpack for the
// single-field hash, dense_set after the second field); the port has a single
// hash representation so only the round-trips are asserted.
#[test]
fn single_field_large_value_remains_listpack() {
    let mut t = Ctx::new();
    let large_value = "x".repeat(2000);

    t.assert_int(&["HSET", "hmap", "field", &large_value], 1);
    t.assert_text(&["HGET", "hmap", "field"], &large_value);

    t.assert_int(&["HSET", "hmap", "field", &"y".repeat(2000)], 0);
    t.assert_text(&["HGET", "hmap", "field"], &"y".repeat(2000));

    t.assert_int(
        &["HSET", "hmap", "field1", &large_value, "field2", "val"],
        2,
    );
    t.assert_text(&["HGET", "hmap", "field1"], &large_value);
    t.assert_text(&["HGET", "hmap", "field2"], "val");
}

#[test]
fn issue_1140() {
    let mut t = Ctx::new();
    t.run(&["HSET", "CaseKey", "Foo", "Bar"]);
    t.assert_text(&["HGET", "CaseKey", "Foo"], "Bar");
}

#[test]
fn issue_2102() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    t.assert_int(&["HSETEX", "key", "1", "k1", "v1"], 1);
    advance(1000);
    assert_eq!(t.run(&["HGETALL", "key"]), Value::Array(Some(vec![])));
}

#[test]
fn hexpire() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    // All fields expire together.
    t.assert_int(&["HSET", "key", "k0", "v0", "k1", "v1", "k2", "v2"], 3);
    let v = t.run(&["HEXPIRE", "key", "1", "FIELDS", "3", "k0", "k1", "k2"]);
    assert_eq!(ints(&v), vec![1, 1, 1]);
    advance(1000);
    assert_eq!(t.run(&["HGETALL", "key"]), Value::Array(Some(vec![])));

    t.assert_int(&["HSETEX", "key2", "1", "k0", "v0", "k1", "v2"], 2);
    let v = t.run(&["HEXPIRE", "key2", "1", "FIELDS", "2", "k0", "k1"]);
    assert_eq!(ints(&v), vec![1, 1]);
    advance(1000);
    assert_eq!(t.run(&["HGETALL", "key2"]), Value::Array(Some(vec![])));

    // Per-field conditions. TTLs are scaled from the reference's 10/8/12s to
    // 4/2/6s so the tiers (k3=2s < k0/k1/k5=4s < k2=6s, k4 without a TTL)
    // stay distinguishable with whole-second advances.
    t.assert_int(
        &[
            "HSET", "key3", "k0", "v0", "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4", "k5", "v5",
        ],
        6,
    );
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key3", "4", "XX", "FIELDS", "1", "k0"])),
        vec![0]
    );
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key3", "4", "NX", "FIELDS", "1", "k0"])),
        vec![1]
    );
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key3", "4", "NX", "FIELDS", "1", "k0"])),
        vec![0]
    );
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key3", "4", "XX", "FIELDS", "1", "k0"])),
        vec![1]
    );
    let v = t.run(&[
        "HEXPIRE", "key3", "4", "NX", "FIELDS", "3", "k1", "k2", "k3",
    ]);
    assert_eq!(ints(&v), vec![1, 1, 1]);
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key3", "2", "GT", "FIELDS", "1", "k2"])),
        vec![0]
    );
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key3", "6", "GT", "FIELDS", "1", "k2"])),
        vec![1]
    );
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key3", "2", "LT", "FIELDS", "1", "k3"])),
        vec![1]
    );
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key3", "6", "LT", "FIELDS", "1", "k3"])),
        vec![0]
    );
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key3", "4", "GT", "FIELDS", "1", "k4"])),
        vec![0]
    );
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key3", "4", "LT", "FIELDS", "1", "k5"])),
        vec![1]
    );

    advance(2000);
    // k3 (2s) expired; k0,k1,k2,k4,k5 remain (sorted reply).
    assert_eq!(
        sorted(&t.run(&["HGETALL", "key3"])),
        vec!["k0", "k1", "k2", "k4", "k5", "v0", "v1", "v2", "v4", "v5"]
    );
    advance(2000);
    // k0,k1,k5 (4s) expired; k2 (6s) and k4 (no TTL) remain (sorted reply).
    assert_eq!(
        sorted(&t.run(&["HGETALL", "key3"])),
        vec!["k2", "k4", "v2", "v4"]
    );
    advance(2000);
    // k2 (6s) expired; only k4 remains.
    assert_eq!(sorted(&t.run(&["HGETALL", "key3"])), vec!["k4", "v4"]);

    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key3", "4", "FIELDS", "1", "k4"])),
        vec![1]
    );
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key3", "0", "XX", "FIELDS", "1", "k4"])),
        vec![2]
    );
    assert_eq!(t.run(&["HGETALL", "key3"]), Value::Array(Some(vec![])));

    // TTL 0 with the per-field conditions.
    t.assert_int(
        &[
            "HSET", "key4", "k0", "v0", "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4",
        ],
        5,
    );
    let v = t.run(&["HEXPIRE", "key4", "0", "NX", "FIELDS", "2", "k0", "k1"]);
    assert_eq!(ints(&v), vec![2, 2]);
    let v = t.run(&["HEXPIRE", "key4", "0", "LT", "FIELDS", "2", "k2", "k3"]);
    assert_eq!(ints(&v), vec![2, 2]);
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key4", "0", "XX", "FIELDS", "1", "k4"])),
        vec![0]
    );
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key4", "4", "NX", "FIELDS", "1", "k4"])),
        vec![1]
    );
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key4", "0", "NX", "FIELDS", "1", "k4"])),
        vec![0]
    );
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key4", "0", "GT", "FIELDS", "1", "k4"])),
        vec![0]
    );
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key4", "0", "FIELDS", "1", "k4"])),
        vec![2]
    );
    assert_eq!(t.run(&["HGETALL", "key4"]), Value::Array(Some(vec![])));
}

#[test]
fn hexpire_num_fields_errors() {
    let mut t = Ctx::new();
    t.assert_int(&["HSET", "key", "k0", "v0", "k1", "v1"], 2);

    // Missing FIELDS keyword.
    t.assert_err(
        &["HEXPIRE", "key", "10", "1", "k0"],
        "Mandatory argument FIELDS",
    );

    // A wrong number of provided fields reports the must-match message.
    t.assert_err(&["HEXPIRE", "key", "10", "FIELDS", "2", "k0"], "numfields");
    t.assert_err(
        &["HEXPIRE", "key", "10", "FIELDS", "1", "k0", "k1"],
        "numfields",
    );
    t.assert_err(&["HEXPIRE", "key", "10", "FIELDS", "0", "k0"], "numfields");
    t.assert_err(&["HEXPIRE", "key", "10", "FIELDS", "0"], "numfields");
}

#[test]
fn hexpire_no_expire_early() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    t.assert_int(&["HSET", "key", "k0", "v0", "k1", "v1"], 2);
    let v = t.run(&["HEXPIRE", "key", "10", "FIELDS", "2", "k0", "k1"]);
    assert_eq!(ints(&v), vec![1, 1]);
    advance(1000);
    // The fields must not be pruned early; the remaining TTL proves it.
    assert_eq!(fieldttl(&mut t, "key", "k0"), 9);
    assert_eq!(
        sorted(&t.run(&["HGETALL", "key"])),
        vec!["k0", "k1", "v0", "v1"]
    );
}

#[test]
fn hexpire_no_such_field() {
    let mut t = Ctx::new();
    t.assert_int(&["HSET", "key", "k0", "v0"], 1);
    let v = t.run(&["HEXPIRE", "key", "10", "FIELDS", "2", "k0", "k1"]);
    assert_eq!(ints(&v), vec![1, -2]);
}

#[test]
fn hexpire_no_such_key() {
    let mut t = Ctx::new();
    let v = t.run(&["HEXPIRE", "key", "10", "FIELDS", "2", "k0", "k1"]);
    assert_eq!(ints(&v), vec![-2, -2]);
}

#[test]
fn hexpire_no_add_new() {
    let mut t = Ctx::new();
    t.run(&["HEXPIRE", "key", "10", "FIELDS", "1", "k0"]);
    assert_eq!(t.run(&["HGETALL", "key"]), Value::Array(Some(vec![])));
}

#[test]
fn hexpire_with_null_char() {
    let mut t = Ctx::new();
    let val = b"test\0test".to_vec();
    t.run_b(&[
        b"HSET".to_vec(),
        b"hash".to_vec(),
        b"field".to_vec(),
        val.clone(),
    ]);
    let v = t.run_b(&[b"HGET".to_vec(), b"hash".to_vec(), b"field".to_vec()]);
    assert_eq!(v.bulk().unwrap(), val);
    t.run_b(&[
        b"HEXPIRE".to_vec(),
        b"hash".to_vec(),
        b"15".to_vec(),
        b"FIELDS".to_vec(),
        b"1".to_vec(),
        b"field".to_vec(),
    ]);
    let v = t.run_b(&[b"HGET".to_vec(), b"hash".to_vec(), b"field".to_vec()]);
    assert_eq!(v.bulk().unwrap(), val);
}

#[test]
fn httl() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    // Non-existent key returns -2 for all fields.
    let v = t.run(&["HTTL", "nokey", "FIELDS", "2", "f1", "f2"]);
    assert_eq!(ints(&v), vec![-2, -2]);

    // Fields without TTL return -1, non-existent fields -2.
    t.assert_int(&["HSET", "key", "k0", "v0", "k1", "v1"], 2);
    let v = t.run(&["HTTL", "key", "FIELDS", "3", "k0", "k1", "nosuch"]);
    assert_eq!(ints(&v), vec![-1, -1, -2]);

    // Set an expiry and verify the TTL.
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key", "10", "FIELDS", "1", "k0"])),
        vec![1]
    );
    let v = t.run(&["HTTL", "key", "FIELDS", "2", "k0", "k1"]);
    let (t0, k1) = (ints(&v)[0], ints(&v)[1]);
    assert_eq!(t0, 10, "httl(k0) = {t0}");
    assert_eq!(k1, -1);

    // The TTL decreases as the clock advances (remaining 9s of a 10s TTL).
    advance(1000);
    let v = t.run(&["HTTL", "key", "FIELDS", "1", "k0"]);
    let ttl = ints(&v)[0];
    assert_eq!(ttl, 9, "httl = {ttl}");

    // Wrong type.
    t.ok(&["SET", "strkey", "val"]);
    t.assert_err(&["HTTL", "strkey", "FIELDS", "1", "f"], "WRONGTYPE");

    // Syntax errors.
    t.assert_err(&["HTTL", "key", "1", "k0"], "Mandatory argument FIELDS");
    t.assert_err(&["HTTL", "key", "FIELDS", "2", "k0"], "numfields");
}

#[test]
fn hpexpire_time() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    // Non-existent key returns -2 for all fields.
    let v = t.run(&["HPEXPIRETIME", "nokey", "FIELDS", "2", "f1", "f2"]);
    assert_eq!(ints(&v), vec![-2, -2]);

    // Fields without TTL return -1, non-existent fields -2.
    t.assert_int(&["HSET", "key", "k0", "v0", "k1", "v1"], 2);
    let v = t.run(&["HPEXPIRETIME", "key", "FIELDS", "3", "k0", "k1", "nosuch"]);
    assert_eq!(ints(&v), vec![-1, -1, -2]);

    // Set an expiry and verify the absolute Unix-ms timestamp.
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "key", "100", "FIELDS", "1", "k0"])),
        vec![1]
    );
    let now_s = clock_ms() / 1000;
    let expected_ms = ((now_s + 100) as i64) * 1000;
    let v = t.run(&["HPEXPIRETIME", "key", "FIELDS", "2", "k0", "k1"]);
    assert_eq!(ints(&v), vec![expected_ms, -1]);

    // The absolute timestamp does not change as the clock advances.
    advance(1000);
    let v = t.run(&["HPEXPIRETIME", "key", "FIELDS", "1", "k0"]);
    assert_eq!(ints(&v), vec![expected_ms]);

    // Wrong type.
    t.ok(&["SET", "strkey", "val"]);
    t.assert_err(&["HPEXPIRETIME", "strkey", "FIELDS", "1", "f"], "WRONGTYPE");

    // Syntax errors.
    t.assert_err(
        &["HPEXPIRETIME", "key", "notfields", "1", "k0"],
        "Mandatory argument FIELDS",
    );
    t.assert_err(&["HPEXPIRETIME", "key", "FIELDS", "2", "k0"], "numfields");
    t.assert_err(
        &["HPEXPIRETIME", "key", "FIELDS", "0", "k0"],
        "Number of fields must be a positive integer",
    );
    t.assert_err(
        &["HPEXPIRETIME", "key", "FIELDS", "-1", "k0"],
        "Number of fields must be a positive integer",
    );
    t.assert_err(
        &["HPEXPIRETIME", "key", "FIELDS", "abc", "k0"],
        "Number of fields must be a positive integer",
    );
    t.assert_err(&["HPEXPIRETIME", "key", "FIELDS", "1"], "numfields");
}

#[test]
fn hgetex() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    // Missing key -> array of nils.
    let v = t.run(&["HGETEX", "nokey", "FIELDS", "2", "f1", "f2"]);
    assert_eq!(
        v.arr().map(<[Value]>::to_vec),
        Some(vec![Value::Bulk(None), Value::Bulk(None)])
    );

    t.assert_int(&["HSET", "key", "f1", "v1", "f2", "v2", "f3", "v3"], 3);

    // No option: returns values and leaves the TTLs untouched.
    let v = t.run(&["HGETEX", "key", "FIELDS", "3", "f1", "f2", "nosuch"]);
    assert_eq!(
        v.arr().map(<[Value]>::to_vec),
        Some(vec![
            Value::Bulk(Some(b"v1".to_vec())),
            Value::Bulk(Some(b"v2".to_vec())),
            Value::Bulk(None),
        ])
    );
    let v = t.run(&["HTTL", "key", "FIELDS", "2", "f1", "f2"]);
    assert_eq!(ints(&v), vec![-1, -1]);

    // EX sets a relative TTL and still returns the value.
    assert_eq!(
        strs(&t.run(&["HGETEX", "key", "EX", "100", "FIELDS", "1", "f1"])),
        vec!["v1"]
    );
    assert_eq!(fieldttl(&mut t, "key", "f1"), 100);

    // PX rounds up to whole seconds.
    assert_eq!(
        strs(&t.run(&["HGETEX", "key", "PX", "100000", "FIELDS", "1", "f2"])),
        vec!["v2"]
    );
    assert_eq!(fieldttl(&mut t, "key", "f2"), 100);

    // EXAT / PXAT set a TTL relative to the pinned clock.
    let now_s = clock_ms() / 1000;
    assert_eq!(
        strs(&t.run(&[
            "HGETEX",
            "key",
            "EXAT",
            &(now_s + 200).to_string(),
            "FIELDS",
            "1",
            "f1"
        ])),
        vec!["v1"]
    );
    assert_eq!(fieldttl(&mut t, "key", "f1"), 200);
    assert_eq!(
        strs(&t.run(&[
            "HGETEX",
            "key",
            "PXAT",
            &((now_s + 300) * 1000).to_string(),
            "FIELDS",
            "1",
            "f2"
        ])),
        vec!["v2"]
    );
    assert_eq!(fieldttl(&mut t, "key", "f2"), 300);

    // PERSIST removes the TTL and returns the value.
    assert_eq!(
        strs(&t.run(&["HGETEX", "key", "PERSIST", "FIELDS", "1", "f1"])),
        vec!["v1"]
    );
    assert_eq!(fieldttl(&mut t, "key", "f1"), -1);
    // PERSIST on a field without a TTL is a no-op.
    assert_eq!(
        strs(&t.run(&["HGETEX", "key", "PERSIST", "FIELDS", "1", "f3"])),
        vec!["v3"]
    );
    assert_eq!(fieldttl(&mut t, "key", "f3"), -1);

    // A past PXAT (or EX 0) returns the current value, then deletes the field.
    assert_eq!(
        strs(&t.run(&["HGETEX", "key", "PXAT", "1", "FIELDS", "1", "f2"])),
        vec!["v2"]
    );
    t.assert_int(&["HEXISTS", "key", "f2"], 0);
    assert_eq!(
        strs(&t.run(&["HGETEX", "key", "EX", "0", "FIELDS", "1", "f3"])),
        vec!["v3"]
    );
    t.assert_int(&["HEXISTS", "key", "f3"], 0);

    // Deleting the last field removes the key entirely.
    assert_eq!(
        strs(&t.run(&["HGETEX", "key", "EX", "0", "FIELDS", "1", "f1"])),
        vec!["v1"]
    );
    t.assert_int(&["EXISTS", "key"], 0);

    // PERSIST on a hash without TTLs just returns the values.
    t.assert_int(&["HSET", "lp", "a", "1", "b", "2"], 2);
    let v = t.run(&["HGETEX", "lp", "PERSIST", "FIELDS", "2", "a", "missing"]);
    assert_eq!(
        v.arr().map(<[Value]>::to_vec),
        Some(vec![Value::Bulk(Some(b"1".to_vec())), Value::Bulk(None),])
    );
}

#[test]
fn hgetex_errors() {
    let mut t = Ctx::new();
    t.assert_int(&["HSET", "key", "f1", "v1"], 1);

    // At most one expiry option is allowed.
    t.assert_err(
        &[
            "HGETEX", "key", "EX", "10", "PX", "10000", "FIELDS", "1", "f1",
        ],
        "syntax error",
    );
    t.assert_err(
        &["HGETEX", "key", "PERSIST", "EX", "10", "FIELDS", "1", "f1"],
        "syntax error",
    );
    t.assert_err(
        &["HGETEX", "key", "EX", "10", "EX", "20", "FIELDS", "1", "f1"],
        "syntax error",
    );
    // An unknown token where an option/FIELDS is expected still yields the
    // FIELDS error.
    t.assert_err(
        &["HGETEX", "key", "KEEPTTL", "FIELDS", "1", "f1"],
        "Mandatory argument FIELDS",
    );

    // Negative relative expiry and non-integer values are rejected.
    t.assert_err(
        &["HGETEX", "key", "EX", "-1", "FIELDS", "1", "f1"],
        "invalid expire time",
    );
    t.assert_err(
        &["HGETEX", "key", "EX", "abc", "FIELDS", "1", "f1"],
        "not an integer",
    );

    // Out-of-range / overflow-inducing expiries are rejected for every unit.
    t.assert_err(
        &[
            "HGETEX",
            "key",
            "PX",
            "9223372036854775807",
            "FIELDS",
            "1",
            "f1",
        ],
        "invalid expire time",
    );
    t.assert_err(
        &[
            "HGETEX",
            "key",
            "PXAT",
            "9223372036854775807",
            "FIELDS",
            "1",
            "f1",
        ],
        "invalid expire time",
    );
    t.assert_err(
        &[
            "HGETEX",
            "key",
            "EX",
            "9223372036854775807",
            "FIELDS",
            "1",
            "f1",
        ],
        "invalid expire time",
    );
    t.assert_err(
        &[
            "HGETEX",
            "key",
            "EXAT",
            "9223372036854775807",
            "FIELDS",
            "1",
            "f1",
        ],
        "invalid expire time",
    );
    // A far-future absolute timestamp beyond the TTL cap is rejected too.
    t.assert_err(
        &["HGETEX", "key", "EXAT", "9999999999", "FIELDS", "1", "f1"],
        "invalid expire time",
    );

    // Missing FIELDS keyword / numfields mismatch / numfields must be positive.
    t.assert_err(
        &["HGETEX", "key", "notfields", "1", "f1"],
        "Mandatory argument FIELDS",
    );
    t.assert_err(&["HGETEX", "key", "FIELDS", "2", "f1"], "numfields");
    t.assert_err(
        &["HGETEX", "key", "FIELDS", "0", "f1"],
        "Number of fields must be a positive integer",
    );
    t.assert_err(
        &["HGETEX", "key", "FIELDS", "-1", "f1"],
        "Number of fields must be a positive integer",
    );
    t.assert_err(
        &["HGETEX", "key", "FIELDS", "abc", "f1"],
        "Number of fields must be a positive integer",
    );
    // An option placed after FIELDS is treated as a field name.
    t.assert_err(
        &["HGETEX", "key", "FIELDS", "1", "f1", "EX", "10"],
        "numfields",
    );
    t.assert_err(&["HGETEX", "key", "FIELDS", "1"], "numfields");

    // Wrong type.
    t.ok(&["SET", "strkey", "val"]);
    t.assert_err(&["HGETEX", "strkey", "FIELDS", "1", "f"], "WRONGTYPE");
}

#[test]
fn random_field_all_expired() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    for i in 0..10 {
        t.assert_int(&["HSETEX", "key", "1", &format!("k{i}"), "v"], 1);
    }
    advance(1000);
    t.assert_null(&["HRANDFIELD", "key"]);
}

#[test]
fn random_field_1_not_expired() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    for i in 0..10 {
        t.assert_int(&["HSETEX", "key", "1", &format!("k{i}"), "v"], 1);
    }
    t.assert_int(&["HSET", "key", "keep", "v"], 1);

    advance(1000);
    t.assert_text(&["HRANDFIELD", "key"], "keep");
}

// Regression: HRANDFIELD with expired fields must not crash (the reference had
// an out-of-bounds access when UpperBoundSize() > SizeSlow()).
#[test]
fn hrand_field_count_with_expired_fields() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    for i in 0..10 {
        t.assert_int(&["HSETEX", "key", "1", &format!("k{i}"), "v"], 1);
    }
    t.assert_int(&["HSET", "key", "keep", "v"], 1);

    advance(1000);

    // Request count=42 with expired fields: only "keep" remains.
    let v = t.run(&["HRANDFIELD", "key", "42"]);
    assert_eq!(strs(&v), vec!["keep"]);
    let v = t.run(&["HRANDFIELD", "key", "42", "WITHVALUES"]);
    assert_eq!(strs(&v), vec!["keep", "v"]);
}

#[test]
fn empty_hash_bug() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    t.assert_int(&["HSET", "foo", "a_field", "a_value"], 1);
    t.assert_int(&["HSETEX", "foo", "1", "b_field", "b_value"], 1);
    t.assert_int(&["HDEL", "foo", "a_field"], 1);

    advance(1000);

    assert_eq!(t.run(&["HGETALL", "foo"]), Value::Array(Some(vec![])));
    t.assert_int(&["EXISTS", "foo"], 0);
}

#[test]
fn scan_after_expire_set() {
    let mut t = Ctx::new();
    t.assert_int(&["HSET", "aset", "afield", "avalue"], 1);
    assert_eq!(
        ints(&t.run(&["HEXPIRE", "aset", "5", "FIELDS", "1", "afield"])),
        vec![1]
    );

    let v = t.run(&["HSCAN", "aset", "0", "count", "100"]);
    assert_eq!(v.arr().map(<[Value]>::to_vec).map(|x| x.len()), Some(2));
    assert_eq!(strs(&v.arr().unwrap()[1]), vec!["afield", "avalue"]);
}

#[test]
fn key_removed_when_empty() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    let test_cmd = |t: &mut Ctx, f: &dyn Fn(&mut Ctx), tag: &str| {
        t.assert_int(&["HSET", "a", "afield", "avalue"], 1);
        assert_eq!(
            ints(&t.run(&["HEXPIRE", "a", "1", "FIELDS", "1", "afield"])),
            vec![1]
        );
        advance(1000);

        // The expired field is still in the key until a hash command prunes it.
        t.assert_int(&["EXISTS", "a"], 1);
        f(t);
        let exists = t.int(&["EXISTS", "a"]);
        assert_eq!(exists, 0, "key 'a' not removed when testing {tag}");
    };

    test_cmd(&mut t, &|t| t.assert_null(&["HGET", "a", "afield"]), "HGET");
    test_cmd(
        &mut t,
        &|t| assert_eq!(t.run(&["HGETALL", "a"]), Value::Array(Some(vec![]))),
        "HGETALL",
    );
    test_cmd(
        &mut t,
        &|t| t.assert_int(&["HDEL", "a", "afield"], 0),
        "HDEL",
    );
    test_cmd(
        &mut t,
        &|t| {
            let v = t.run(&["HSCAN", "a", "0"]);
            assert_eq!(v.arr().unwrap()[0].text().unwrap(), "0");
        },
        "HSCAN",
    );
    test_cmd(
        &mut t,
        &|t| {
            let v = t.run(&["HMGET", "a", "afield"]);
            assert_eq!(
                v.arr().map(<[Value]>::to_vec),
                Some(vec![Value::Bulk(None)])
            );
        },
        "HMGET",
    );
    test_cmd(
        &mut t,
        &|t| t.assert_int(&["HEXISTS", "a", "afield"], 0),
        "HEXISTS",
    );
    test_cmd(
        &mut t,
        &|t| t.assert_int(&["HSTRLEN", "a", "afield"], 0),
        "HSTRLEN",
    );
}

// RESP3 nests [field, value] pairs; the harness only speaks RESP2, so only the
// flattened RESP2 form is asserted.
#[test]
fn hrand_field_resp_format() {
    let mut t = Ctx::new();
    t.assert_int(&["HSET", "key", "a", "1", "b", "2", "c", "3"], 3);
    let v = t.run(&["HRANDFIELD", "key", "3", "WITHVALUES"]);
    let items = strs(&v);
    assert_eq!(items.len(), 6);
    for pair in items.chunks(2) {
        let expect = match pair[0].as_str() {
            "a" => "1",
            "b" => "2",
            "c" => "3",
            other => panic!("unexpected field {other:?}"),
        };
        assert_eq!(pair[1], expect);
    }
}

// Regression: OpHTtl triggers lazy field expiry but did not delete the emptied
// key, leaving a zombie hash in the DB.
#[test]
fn httl_deletes_empty_hash() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    t.assert_int(&["HSETEX", "key", "1", "f1", "v1"], 1);
    t.assert_int(&["EXISTS", "key"], 1);

    advance(1000);

    // HTTL triggers the lazy expiry of f1 and must remove the now-empty key.
    let v = t.run(&["HTTL", "key", "FIELDS", "1", "f1"]);
    assert_eq!(ints(&v), vec![-2]);
    t.assert_int(&["EXISTS", "key"], 0);
}

// HEXPIRE with TTL 0 must delete the key when the hash becomes empty; a
// leftover zombie key could crash an RDB save or EXISTS.
#[test]
fn hexpire_zero_ttl_deletes_key() {
    let dir = std::env::temp_dir().join(format!("dragonflydb_rs_hset_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    unsafe { std::env::set_var("DRAGONFLYDB_RS_DUMP_DIR", &dir) };

    let mut t = Ctx::new();
    t.assert_int(&["HSET", "zombie", "f", "v"], 1);
    let v = t.run(&["HEXPIRE", "zombie", "0", "FIELDS", "1", "f"]);
    assert_eq!(ints(&v), vec![2]);
    t.assert_int(&["EXISTS", "zombie"], 0);
    t.ok(&["SAVE", "RDB", "zombie_test.rdb"]);
}

// HINCRBYFLOAT with NaN on a non-existing key must not create an empty hash.
#[test]
fn hincr_by_float_nan_does_not_create_key() {
    let mut t = Ctx::new();
    t.assert_err(
        &["HINCRBYFLOAT", "key", "field", "nan"],
        "increment would produce NaN or Infinity",
    );
    t.assert_int(&["EXISTS", "key"], 0);
    t.assert_null(&["HRANDFIELD", "key"]);
}

// The reference guards a memory-accounting DCHECK in SHRINK; the port's SHRINK
// is a stub (no bucket array to compact, replies 0), so the sequence is
// asserted to run cleanly.
#[test]
fn shrink_memory_accounting_hash() {
    let _clock = clock_guard();
    let mut t = Ctx::new();
    for i in 0..60 {
        t.assert_int(
            &[
                "HSETEX",
                "h1",
                "1000",
                &format!("temp{i}"),
                &format!("v{i}"),
            ],
            1,
        );
    }
    for i in 0..50 {
        t.assert_int(&["HDEL", "h1", &format!("temp{i}")], 1);
    }
    for i in 0..10 {
        t.assert_int(
            &["HSETEX", "h1", "1", &format!("exp{i}"), &format!("v{i}")],
            1,
        );
    }

    advance(1000);

    let v = t.run(&["SHRINK", "h1"]);
    assert_eq!(
        v.int(),
        Some(0),
        "SHRINK replies 0 for the port's single-encoding hash"
    );
    t.assert_int(&["HDEL", "h1", "temp50"], 1);
    t.assert_int(&["HLEN", "h1"], 9);
}
