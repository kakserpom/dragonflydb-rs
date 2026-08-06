//! Port of `dragonfly/src/server/bitops_family_test.cc`.
//!
//! Adaptations from the C++ original:
//! - `GetMetrics` / `BitOpOverwritesNonStringKeyAccounting` (db memory
//!   accounting) is dropped: the port exposes no per-type memory counters.
//! - The C++ `Bytes` helper stores `unsigned long long` literals as 8 native
//!   (little-endian) bytes, so the same is done here via `b8`.
//! - Tests use `Ctx::run_b` for binary values instead of the C++ `_b` literals.

mod common;

use common::*;

/// `Bytes(unsigned long long)` from the C++ harness: 8 native LE bytes.
fn b8(n: u64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

/// `SET key <binary value>`.
fn set_b(t: &mut Ctx, key: &str, val: &[u8]) {
    t.ok_b(&[b"set".to_vec(), key.as_bytes().to_vec(), val.to_vec()]);
}

/// Assert the reply is an array of exactly the given integer elements.
fn arr_ints(t: &mut Ctx, cmd: &[&str], expected: &[i64]) {
    let a = t.arr(cmd);
    assert_eq!(a.len(), expected.len(), "reply {a:?}");
    for (v, e) in a.iter().zip(expected) {
        assert_eq!(v.int(), Some(*e), "reply {a:?}");
    }
}

/// Assert the reply is an array whose elements match `expected` (Some = int,
/// None = null).
fn arr_mixed(t: &mut Ctx, cmd: &[&str], expected: &[Option<i64>]) {
    let a = t.arr(cmd);
    assert_eq!(a.len(), expected.len(), "reply {a:?}");
    for (v, e) in a.iter().zip(expected) {
        match e {
            Some(n) => assert_eq!(v.int(), Some(*n), "reply {a:?}"),
            None => assert!(
                matches!(v, Value::Bulk(None)),
                "expected null element, got {v:?} in {a:?}"
            ),
        }
    }
}

// taken from running this on redis
const EXPECTED_VALUE_SETBIT: [i64; 12] = [0, 1, 1, 0, 0, 0, 0, 1, 0, 1, 1, 0];

#[test]
fn get_bit() {
    let mut t = Ctx::new();
    t.ok(&["set", "foo", "abc"]);
    for (i, expect) in EXPECTED_VALUE_SETBIT.iter().enumerate() {
        assert_eq!(t.int(&["getbit", "foo", &i.to_string()]), *expect);
    }
    // accessing a bit out of range returns 0
    assert_eq!(t.int(&["getbit", "foo", &("abc".len() + 5).to_string()]), 0);
}

#[test]
fn set_bit_existing_key() {
    let mut t = Ctx::new();
    t.ok(&["set", "foo", "abc"]);
    for (i, expect) in EXPECTED_VALUE_SETBIT.iter().enumerate() {
        assert_eq!(t.int(&["setbit", "foo", &i.to_string(), "1"]), *expect);
    }
    for i in 0..EXPECTED_VALUE_SETBIT.len() {
        assert_eq!(t.int(&["getbit", "foo", &i.to_string()]), 1);
    }
}

#[test]
fn set_bit_missing_key() {
    let mut t = Ctx::new();
    for i in 0..EXPECTED_VALUE_SETBIT.len() {
        assert_eq!(t.int(&["setbit", "foo", &i.to_string(), "1"]), 0);
    }
    for i in 0..EXPECTED_VALUE_SETBIT.len() {
        assert_eq!(t.int(&["getbit", "foo", &i.to_string()]), 1);
    }
}

#[test]
fn set_bit_incorrect_values() {
    let mut t = Ctx::new();
    assert_eq!(t.int(&["setbit", "foo", "0", "1"]), 0);
    t.assert_err(&["setbit", "foo", "1", "-1"], "ERR value is not an integer or out of range");
    t.assert_err(&["setbit", "foo", "2", "11"], "ERR value is not an integer or out of range");
    t.assert_err(&["setbit", "foo", "3", "a"], "ERR value is not an integer or out of range");
    t.assert_err(&["setbit", "foo", "4", "O"], "ERR value is not an integer or out of range");
    assert_eq!(t.int(&["getbit", "foo", "0"]), 1);
    assert_eq!(t.int(&["getbit", "foo", "1"]), 0);
    assert_eq!(t.int(&["getbit", "foo", "2"]), 0);
    assert_eq!(t.int(&["getbit", "foo", "3"]), 0);
    assert_eq!(t.int(&["getbit", "foo", "4"]), 0);
}

#[test]
fn set_bit_extend_existing_key() {
    let mut t = Ctx::new();
    t.ok(&["set", "foo", "abc"]);
    assert_eq!(t.int(&["strlen", "foo"]), 3);

    // bit 100 (byte 12, bit 4) extends the string to 13 bytes
    assert_eq!(t.int(&["setbit", "foo", "100", "1"]), 0);
    assert_eq!(t.int(&["strlen", "foo"]), 13);
    assert_eq!(t.int(&["getbit", "foo", "100"]), 1);

    assert_eq!(t.int(&["getbit", "foo", "24"]), 0);
    assert_eq!(t.int(&["getbit", "foo", "50"]), 0);
    assert_eq!(t.int(&["getbit", "foo", "99"]), 0);

    assert_eq!(t.int(&["getbit", "foo", "0"]), EXPECTED_VALUE_SETBIT[0]);
    assert_eq!(t.int(&["getbit", "foo", "1"]), EXPECTED_VALUE_SETBIT[1]);
    assert_eq!(t.int(&["getbit", "foo", "2"]), EXPECTED_VALUE_SETBIT[2]);

    // setting the same bit back to 0 returns the current value (1)
    assert_eq!(t.int(&["setbit", "foo", "100", "0"]), 1);
    assert_eq!(t.int(&["getbit", "foo", "100"]), 0);
}

// got this from redis, 0 as start index
const EXPECTED_VALUES_BYTES_BIT_COUNT: [i64; 9] = [4, 7, 11, 14, 17, 21, 21, 21, 21];

#[test]
fn bit_count_byte() {
    let mut t = Ctx::new();
    t.ok(&["set", "foo", "farbar"]);
    // non-existing key counts 0
    assert_eq!(t.int(&["bitcount", "foo2"]), 0);
    for (i, expect) in EXPECTED_VALUES_BYTES_BIT_COUNT.iter().enumerate() {
        assert_eq!(t.int(&["bitcount", "foo", "0", &i.to_string()]), *expect);
    }
    // total number of set bits in "farbar"
    assert_eq!(t.int(&["bitcount", "foo"]), 21);
}

#[test]
fn bit_count_byte_sub_range() {
    let mut t = Ctx::new();
    t.ok(&["set", "foo", "farbar"]);
    assert_eq!(t.int(&["bitcount", "foo", "1", "1"]), 3);
    assert_eq!(t.int(&["bitcount", "foo", "1", "2"]), 7);
    assert_eq!(t.int(&["bitcount", "foo", "2", "2"]), 4);
    assert_eq!(t.int(&["bitcount", "foo", "3", "2"]), 0); // illegal range
    assert_eq!(t.int(&["bitcount", "foo", "-3", "-1"]), 10);
    assert_eq!(t.int(&["bitcount", "foo", "-5", "-2"]), 13);
    assert_eq!(t.int(&["bitcount", "foo", "-1", "-2"]), 0); // illegal range
    assert_eq!(t.int(&["bitcount", "foo", "1", "0"]), 0); // illegal range

    // Negative `end` that resolves to < 0 must be clamped to 0.
    assert_eq!(t.int(&["bitcount", "foo", "0", "-6"]), 4); // end resolves to 0
    assert_eq!(t.int(&["bitcount", "foo", "0", "-100"]), 4); // end resolves far below 0
    assert_eq!(t.int(&["bitcount", "foo", "-100", "-100"]), 4);
    assert_eq!(t.int(&["bitcount", "foo", "-100", "-99"]), 4);

    t.ok(&["set", "A", "A"]);
    assert_eq!(t.int(&["bitcount", "A", "0", "-2"]), 2);

    // both-negative inverted range on a 1-byte key: 0, not a count of byte 0
    assert_eq!(t.int(&["bitcount", "A", "-1", "-2"]), 0);
}

#[test]
fn bit_count_byte_bit_sub_range() {
    let mut t = Ctx::new();
    t.ok(&["set", "foo", "abcdef"]);
    t.assert_err(&["bitcount", "foo", "bar", "BIT"], "value is not an integer or out of range");

    assert_eq!(t.int(&["bitcount", "foo", "1", "1", "BIT"]), 1);
    assert_eq!(t.int(&["bitcount", "foo", "1", "2", "BIT"]), 2);
    assert_eq!(t.int(&["bitcount", "foo", "2", "2", "BIT"]), 1);
    assert_eq!(t.int(&["bitcount", "foo", "3", "2", "bit"]), 0); // illegal range
    assert_eq!(t.int(&["bitcount", "foo", "-3", "-1", "bit"]), 2);
    assert_eq!(t.int(&["bitcount", "foo", "-5", "-2", "bit"]), 2);
    assert_eq!(t.int(&["bitcount", "foo", "1", "9", "bit"]), 4);
    assert_eq!(t.int(&["bitcount", "foo", "2", "19", "bit"]), 7);
    assert_eq!(t.int(&["bitcount", "foo", "-1", "-2", "bit"]), 0); // illegal range

    // both-negative inverted range past the end of a 1-byte key: 0
    set_b(&mut t, "x", &[0xff]);
    assert_eq!(t.int(&["bitcount", "x", "-9", "-10", "bit"]), 0);
}

#[test]
fn bit_count_bit_last_bit_regression() {
    let mut t = Ctx::new();
    // single byte: bit 0 = 1, bit 7 = 1 -> popcount 2
    set_b(&mut t, "k1", &[0x81]);
    assert_eq!(t.int(&["bitcount", "k1", "0", "7", "BIT"]), 2);
    assert_eq!(t.int(&["bitcount", "k1", "1", "7", "BIT"]), 1);
    assert_eq!(t.int(&["bitcount", "k1", "-8", "-1", "BIT"]), 2);
    assert_eq!(t.int(&["bitcount", "k1", "0", "-1", "BIT"]), 2);
    assert_eq!(t.int(&["bitcount", "k1", "8", "8", "BIT"]), 0);

    // multi-byte: "abcdef" has 48 bits
    t.ok(&["set", "k2", "abcdef"]);
    assert_eq!(
        t.int(&["bitcount", "k2", "0", "-1"]), // byte form
        t.int(&["bitcount", "k2", "0", "47", "BIT"])
    );
    assert_eq!(
        t.int(&["bitcount", "k2", "5", "5"]), // last byte only (byte form)
        t.int(&["bitcount", "k2", "40", "47", "BIT"])
    );
    assert_eq!(t.int(&["bitcount", "k2", "48", "48", "BIT"]), 0);
    assert_eq!(t.int(&["bitcount", "k2", "100", "200", "BIT"]), 0);
}

// ---------------------------------------------------------------------------
// BITOP
// ---------------------------------------------------------------------------

/// `KEY_VALUES_BIT_OP` from the C++ test. `_b` numeric literals are stored as
/// 8 native LE bytes; `initializer_list` literals keep their order reversed.
fn key_values_bit_op() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("first_key", b8(0xFFAA_CC01u64)),
        ("key_second", vec![0xBB, 0x01]),
        ("_this_is_the_third_key", vec![0xCC, 0xAA, 0x20, 0x15, 0x05, 0x01]),
        ("the_last_key_we_have", b8(0xAACCu64)),
    ]
}

fn bit_op_set_keys(t: &mut Ctx) {
    for (k, v) in key_values_bit_op() {
        set_b(t, k, &v);
    }
}

#[test]
#[allow(clippy::erasing_op)] // 4-key AND chain is provably 0, mirroring the C++
fn bit_ops_and() {
    let mut t = Ctx::new();
    bit_op_set_keys(&mut t);
    let kv = key_values_bit_op();

    t.assert_err(&["bitop", "foo", "bar", "abc"], "syntax error");

    // none-existing keys
    assert_eq!(t.int(&["bitop", "and", "dest_key", "1", "2", "3"]), 0);

    // single key
    assert_eq!(t.int(&["bitop", "and", "foo_out", kv[0].0]), kv[0].1.len() as i64);
    assert_eq!(t.bulk(&["get", "foo_out"]), kv[0].1);

    // two keys
    assert_eq!(
        t.int(&["bitop", "and", "foo-out", kv[0].0, kv[1].0]),
        8 // EXPECTED_LEN_BITOP
    );
    let expected = b8(0xffaa_cc01u64 & 0x1bbu64);
    assert_eq!(t.bulk(&["get", "foo-out"]), expected);

    // three keys
    assert_eq!(
        t.int(&["bitop", "and", "foo-out", kv[0].0, kv[1].0, kv[2].0]),
        8
    );
    let expected = b8(0xffaa_cc01u64 & 0x1bbu64 & 0x0105_1520_aaccu64);
    assert_eq!(t.bulk(&["get", "foo-out"]), expected);

    // four keys
    assert_eq!(
        t.int(&["bitop", "and", "foo-out", kv[0].0, kv[1].0, kv[2].0, kv[3].0]),
        8
    );
    let expected = b8(0xffaa_cc01u64 & 0x1bbu64 & 0x0105_1520_aaccu64 & 0xaa_ccu64);
    assert_eq!(t.bulk(&["get", "foo-out"]), expected);
}

#[test]
fn bit_ops_or() {
    let mut t = Ctx::new();
    bit_op_set_keys(&mut t);
    let kv = key_values_bit_op();

    assert_eq!(t.int(&["bitop", "or", "dest_key", "1", "2", "3"]), 0);

    // single key
    assert_eq!(t.int(&["bitop", "or", "foo_out", kv[0].0]), kv[0].1.len() as i64);
    assert_eq!(t.bulk(&["get", "foo_out"]), kv[0].1);

    // two keys
    assert_eq!(t.int(&["bitop", "or", "foo-out", kv[0].0, kv[1].0]), 8);
    let expected = b8(0xffaa_cc01u64 | 0x1bbu64);
    assert_eq!(t.bulk(&["get", "foo-out"]), expected);

    // three keys
    assert_eq!(
        t.int(&["bitop", "or", "foo-out", kv[0].0, kv[1].0, kv[2].0]),
        8
    );
    let expected = b8(0xffaa_cc01u64 | 0x1bbu64 | 0x0105_1520_aaccu64);
    assert_eq!(t.bulk(&["get", "foo-out"]), expected);

    // four keys
    assert_eq!(
        t.int(&["bitop", "or", "foo-out", kv[0].0, kv[1].0, kv[2].0, kv[3].0]),
        8
    );
    let expected = b8(0xffaa_cc01u64 | 0x1bbu64 | 0x0105_1520_aaccu64 | 0xaaccu64);
    assert_eq!(t.bulk(&["get", "foo-out"]), expected);
}

#[test]
fn bit_ops_xor() {
    let mut t = Ctx::new();
    bit_op_set_keys(&mut t);
    let kv = key_values_bit_op();

    assert_eq!(t.int(&["bitop", "or", "dest_key", "1", "2", "3"]), 0);

    // single key
    assert_eq!(t.int(&["bitop", "xor", "foo_out", kv[0].0]), kv[0].1.len() as i64);
    assert_eq!(t.bulk(&["get", "foo_out"]), kv[0].1);

    // two keys
    assert_eq!(t.int(&["bitop", "xor", "foo-out", kv[0].0, kv[1].0]), 8);
    let expected = b8(0xffaa_cc01u64 ^ 0x1bbu64);
    assert_eq!(t.bulk(&["get", "foo-out"]), expected);

    // three keys
    assert_eq!(
        t.int(&["bitop", "xor", "foo-out", kv[0].0, kv[1].0, kv[2].0]),
        8
    );
    let expected = b8(0xffaa_cc01u64 ^ 0x1bbu64 ^ 0x0105_1520_aaccu64);
    assert_eq!(t.bulk(&["get", "foo-out"]), expected);

    // four keys
    assert_eq!(
        t.int(&["bitop", "xor", "foo-out", kv[0].0, kv[1].0, kv[2].0, kv[3].0]),
        8
    );
    let expected = b8(0xffaa_cc01u64 ^ 0x1bbu64 ^ 0x0105_1520_aaccu64 ^ 0xaaccu64);
    assert_eq!(t.bulk(&["get", "foo-out"]), expected);
}

#[test]
fn bit_ops_not() {
    let mut t = Ctx::new();
    let kv = key_values_bit_op();

    // illegal number of args
    t.assert_err(&["bitop", "not", "bar", "abc", "efg"], "syntax error");

    // works with none-existing keys
    assert_eq!(
        t.int(&["bitop", "NOT", "bit-op-not-none-existing-key-results", "this-key-do-not-exists"]),
        0
    );
    t.assert_null(&["get", "bit-op-not-none-existing-key-results"]);

    t.ok(&["set", "foo", "bar"]);
    assert_eq!(t.int(&["bitop", "NOT", "foo", "this-key-do-not-exists"]), 0);
    t.assert_null(&["get", "foo"]);

    // change the type of foo: bitops is a blind update
    t.assert_int(&["hset", "foo", "bar", "val"], 1);
    assert_eq!(t.int(&["bitop", "NOT", "foo", "this-key-do-not-exists"]), 0);
    t.assert_null(&["get", "foo"]);

    set_b(&mut t, kv[0].0, &kv[0].1);
    assert_eq!(t.int(&["bitop", "not", "foo_out", kv[0].0]), kv[0].1.len() as i64);
    let expected = b8(!0xffaa_cc01u64);
    assert_eq!(t.bulk(&["get", "foo_out"]), expected);
}

#[test]
fn bit_pos() {
    let mut t = Ctx::new();
    set_b(&mut t, "a", &[0x00, 0x00, 0x06, 0xff, 0xf0]);

    // find clear bits
    assert_eq!(t.int(&["bitpos", "a", "0"]), 0);
    assert_eq!(t.int(&["bitpos", "a", "0", "1"]), 8);
    assert_eq!(t.int(&["bitpos", "a", "0", "2"]), 16);
    assert_eq!(t.int(&["bitpos", "a", "0", "100"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "0", "100", "103"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "0", "100", "0"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "0", "0", "100"]), 0);
    assert_eq!(t.int(&["bitpos", "a", "0", "1", "100"]), 8);
    assert_eq!(t.int(&["bitpos", "a", "0", "0", "-3"]), 0);
    assert_eq!(t.int(&["bitpos", "a", "0", "1", "-2"]), 8);
    assert_eq!(t.int(&["bitpos", "a", "0", "3"]), 36);
    assert_eq!(t.int(&["bitpos", "a", "0", "4"]), 36);
    assert_eq!(t.int(&["bitpos", "a", "0", "-2"]), 36);
    assert_eq!(t.int(&["bitpos", "a", "0", "-2", "-1"]), 36);
    assert_eq!(t.int(&["bitpos", "a", "0", "-1"]), 36);
    assert_eq!(t.int(&["bitpos", "a", "0", "-100"]), 0);

    // explicitly mention BYTE
    assert_eq!(t.int(&["bitpos", "a", "0", "100", "103", "BYTE"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "0", "100", "0", "BYTE"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "0", "0", "100", "BYTE"]), 0);
    assert_eq!(t.int(&["bitpos", "a", "0", "1", "100", "BYTE"]), 8);
    assert_eq!(t.int(&["bitpos", "a", "0", "0", "-3", "BYTE"]), 0);
    assert_eq!(t.int(&["bitpos", "a", "0", "1", "-2", "BYTE"]), 8);
    assert_eq!(t.int(&["bitpos", "a", "0", "-2", "-1", "BYTE"]), 36);

    // find clear bits using BIT
    assert_eq!(t.int(&["bitpos", "a", "0", "100", "103", "BIT"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "0", "100", "0", "BIT"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "0", "0", "100", "BIT"]), 0);
    assert_eq!(t.int(&["bitpos", "a", "0", "1", "100", "BIT"]), 1);
    assert_eq!(t.int(&["bitpos", "a", "0", "2", "100", "BIT"]), 2);
    assert_eq!(t.int(&["bitpos", "a", "0", "16", "100", "BIT"]), 16);
    assert_eq!(t.int(&["bitpos", "a", "0", "21", "100", "BIT"]), 23);
    assert_eq!(t.int(&["bitpos", "a", "0", "24", "100", "BIT"]), 36);
    assert_eq!(t.int(&["bitpos", "a", "0", "0", "-3", "BIT"]), 0);
    assert_eq!(t.int(&["bitpos", "a", "0", "1", "-2", "BIT"]), 1);
    assert_eq!(t.int(&["bitpos", "a", "0", "-2", "-1", "BIT"]), 38);

    // find set bits
    assert_eq!(t.int(&["bitpos", "a", "1"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "0"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "1"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "2"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "3"]), 24);
    assert_eq!(t.int(&["bitpos", "a", "1", "4"]), 32);
    assert_eq!(t.int(&["bitpos", "a", "1", "-1"]), 32);
    assert_eq!(t.int(&["bitpos", "a", "1", "-2"]), 24);
    assert_eq!(t.int(&["bitpos", "a", "1", "-3"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "-4"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "-5"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "-6"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "-100"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "0", "0"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "1", "0", "1"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "1", "0", "3"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "0", "100"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "2", "2"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "2", "3"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "-1", "-1"]), 32);
    assert_eq!(t.int(&["bitpos", "a", "1", "-2", "-1"]), 24);
    assert_eq!(t.int(&["bitpos", "a", "1", "-1", "-2"]), -1);

    // find set bits, explicitly mention BYTE
    assert_eq!(t.int(&["bitpos", "a", "1", "0", "0", "BYTE"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "1", "0", "1", "BYTE"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "1", "0", "3", "BYTE"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "0", "100", "BYTE"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "2", "2", "BYTE"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "2", "3", "BYTE"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "-1", "-1", "BYTE"]), 32);
    assert_eq!(t.int(&["bitpos", "a", "1", "-2", "-1", "BYTE"]), 24);
    assert_eq!(t.int(&["bitpos", "a", "1", "-1", "-2", "BYTE"]), -1);

    // find set bits using BIT
    assert_eq!(t.int(&["bitpos", "a", "1", "0", "0", "BIT"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "1", "0", "1", "BIT"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "1", "0", "21", "BIT"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "21", "21", "BIT"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "21", "100", "BIT"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "0", "100", "BIT"]), 21);
    assert_eq!(t.int(&["bitpos", "a", "1", "-1", "-1", "BIT"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "1", "-4", "-1", "BIT"]), -1);
    assert_eq!(t.int(&["bitpos", "a", "1", "-5", "-1", "BIT"]), 35);
    assert_eq!(t.int(&["bitpos", "a", "1", "-6", "-1", "BIT"]), 34);

    // all-set string
    set_b(&mut t, "b", &[0xff, 0xff, 0xff]);
    assert_eq!(t.int(&["bitpos", "b", "0"]), 24);
    assert_eq!(t.int(&["bitpos", "b", "0", "0"]), 24);
    assert_eq!(t.int(&["bitpos", "b", "0", "1"]), 24);
    assert_eq!(t.int(&["bitpos", "b", "0", "2"]), 24);
    assert_eq!(t.int(&["bitpos", "b", "0", "3"]), -1);
    assert_eq!(t.int(&["bitpos", "b", "0", "0", "1"]), -1);
    assert_eq!(t.int(&["bitpos", "b", "0", "0", "1", "BYTE"]), -1);
    assert_eq!(t.int(&["bitpos", "b", "0", "0", "3"]), -1);
    assert_eq!(t.int(&["bitpos", "b", "0", "0", "3", "BYTE"]), -1);

    // empty string
    set_b(&mut t, "empty", &[]);
    assert_eq!(t.int(&["bitpos", "empty", "0"]), -1);
    assert_eq!(t.int(&["bitpos", "empty", "0", "1"]), -1);

    // non-existent key is treated like a zero-padded string
    assert_eq!(t.int(&["bitpos", "d", "1"]), -1);
    assert_eq!(t.int(&["bitpos", "d", "0"]), 0);

    // bit argument must be 1 or 0
    t.assert_err(&["bitpos", "d", "2"], "ERR The bit argument must be 1 or 0");
    t.assert_err(&["bitpos", "d", "42"], "ERR The bit argument must be 1 or 0");
    t.assert_err(&["bitpos", "d", "-1"], "ERR The bit argument must be 1 or 0");
}

#[test]
fn bit_field_parsing() {
    let mut t = Ctx::new();
    let syntax = "ERR syntax error";

    t.assert_err(&["bitfield", "foo", "set", "u1"], syntax);
    t.assert_err(&["bitfield", "foo", "set", "u1", "0"], syntax);
    t.assert_err(&["bitfield", "foo", "set", "u1", "0", "0", "55"], syntax);
    t.assert_err(&["bitfield", "foo", "set", "u1", "0", "0", "get", "u1"], syntax);
    t.assert_err(&["bitfield", "foo", "incrby", "u1"], syntax);
    t.assert_err(&["bitfield", "foo", "incrby", "u1", "0"], syntax);
    t.assert_err(&["bitfield", "foo", "get", "u1", "0", "15"], syntax);
    t.assert_err(&["bitfield", "foo", "get"], syntax);
    t.assert_err(&["bitfield", "foo", "set", "u1", "0", "0", "set"], syntax);
    t.assert_err(&["bitfield", "foo", "overflow"], syntax);
    t.assert_err(&["bitfield", "foo", "overflow", "nonsense"], syntax);

    let bad_type = "ERR invalid bitfield type. use something like i16 u8. note that u64 is not supported but i64 is.";
    t.assert_err(&["bitfield", "foo", "set", "u0", "0", "0"], bad_type);
    t.assert_err(&["bitfield", "foo", "set", "u0", "0", "0"], bad_type);
    t.assert_err(&["bitfield", "foo", "set", "u64", "0", "0"], bad_type);
    t.assert_err(&["bitfield", "foo", "set", "u65", "0", "0"], bad_type);
    t.assert_err(&["bitfield", "foo", "set", "i65", "0", "0"], bad_type);

    t.assert_err(&["bitfield_ro", "foo", "set", "u1", "0", "0"], "BITFIELD_RO only supports the GET subcommand");
    t.assert_err(&["bitfield_ro", "foo", "incrby", "i64", "0", "15"], "BITFIELD_RO only supports the GET subcommand");
}

#[test]
fn bit_field_create() {
    let mut t = Ctx::new();
    arr_ints(&mut t, &["bitfield", "foo", "set", "u1", "0", "1"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u1", "0"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "incrby", "u1", "1", "1"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u1", "1"], &[1]);
}

#[test]
fn bit_field_overflow_underflow() {
    let mut t = Ctx::new();
    t.run(&["bitfield", "foo", "set", "u2", "0", "2"]);

    // unsigned 1 bit
    arr_ints(&mut t, &["bitfield", "foo", "set", "u1", "0", "2"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u1", "0"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "incrby", "u1", "1", "2"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u1", "1"], &[0]);

    // signed 64 bit wrap
    let max = i64::MAX;
    let min = i64::MIN;
    t.run(&["bitfield", "foo", "set", "i64", "0", &max.to_string()]);
    arr_ints(&mut t, &["bitfield", "foo", "incrby", "i64", "0", "1"], &[min]);

    // signed 1 bit
    t.run(&["bitfield", "foo", "set", "i1", "0", "-2"]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "i1", "0"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "incrby", "i1", "0", "-1"], &[-1]);
    arr_ints(&mut t, &["bitfield", "foo", "incrby", "i1", "0", "-1"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "incrby", "i1", "0", "-3"], &[-1]);

    t.run(&["bitfield", "foo", "set", "i8", "0", &min.to_string()]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "i8", "0"], &[0]);

    // signed 64 bit
    t.run(&["bitfield", "foo", "set", "i64", "0", &min.to_string()]);
    arr_ints(&mut t, &["bitfield", "foo", "incrby", "i64", "0", "-1"], &[max]);

    // overflow sat
    t.run(&["bitfield", "foo", "set", "u1", "0", "0"]);
    arr_ints(&mut t, &["bitfield", "foo", "overflow", "sat", "incrby", "u8", "0", "300"], &[255]);
    arr_ints(&mut t, &["bitfield", "foo", "overflow", "sat", "incrby", "u8", "0", "10"], &[255]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u8", "0"], &[255]);

    // unsigned 63 bit
    t.run(&["bitfield", "foo", "set", "u63", "0", "0"]);
    arr_ints(&mut t, &["bitfield", "foo", "overflow", "sat", "set", "u63", "0", &max.to_string()], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "overflow", "sat", "incrby", "u63", "0", "10"], &[max]);

    // signed 8 bit
    t.run(&["bitfield", "foo", "set", "u8", "0", "0"]);
    arr_ints(&mut t, &["bitfield", "foo", "overflow", "sat", "set", "i8", "0", "300"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "overflow", "sat", "incrby", "i8", "0", "-127"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "overflow", "sat", "incrby", "i8", "0", "-255"], &[-128]);

    // signed 64 bit
    t.run(&["bitfield", "foo", "set", "i64", "0", "0"]);
    arr_ints(&mut t, &["bitfield", "foo", "overflow", "sat", "set", "i64", "0", &max.to_string()], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "overflow", "sat", "incrby", "i64", "0", "100"], &[max]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "i64", "0"], &[max]);
    arr_ints(&mut t, &["bitfield", "foo", "overflow", "sat", "set", "i64", "0", &min.to_string()], &[max]);
    arr_ints(&mut t, &["bitfield", "foo", "overflow", "sat", "incrby", "i64", "0", "-100"], &[min]);

    // overflow fail
    arr_mixed(&mut t, &["bitfield", "foo", "overflow", "fail", "set", "u8", "0", "300"], &[None]);
    arr_mixed(&mut t, &["bitfield", "foo", "overflow", "fail", "incrby", "u1", "0", "10"], &[None]);
    arr_mixed(&mut t, &["bitfield", "foo", "overflow", "fail", "incrby", "u1", "0", "-10"], &[None]);
    arr_mixed(&mut t, &["bitfield", "foo", "overflow", "fail", "incrby", "i8", "0", "300"], &[None]);
    arr_mixed(&mut t, &["bitfield", "foo", "overflow", "fail", "incrby", "i1", "0", "10"], &[None]);
    arr_mixed(&mut t, &["bitfield", "foo", "overflow", "fail", "incrby", "i1", "0", "-10"], &[None]);

    // stickiness of overflow among operations in a chain
    arr_mixed(
        &mut t,
        &["bitfield", "foo", "overflow", "fail", "set", "u8", "0", "300", "set", "u1", "0", "400"],
        &[None, None],
    );
}

#[test]
fn bit_field_operations() {
    let mut t = Ctx::new();
    // aligned offset reads/writes unsigned
    t.run(&["bitfield", "foo", "set", "u32", "0", "0"]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "u8", "0", "120"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u8", "0"], &[120]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "u8", "8", "1"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u8", "8"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "u8", "16", "1"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u8", "16"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "u8", "24", "10"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u8", "24"], &[10]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u32", "0"], &[2_013_331_722]);
    arr_ints(&mut t, &["bitfield", "foo", "incrby", "u8", "0", "120"], &[240]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u8", "0"], &[240]);
    arr_ints(&mut t, &["bitfield", "foo", "incrby", "u16", "0", "120"], &[61_561]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u16", "0"], &[61_561]);

    // aligned offset reads/writes signed
    t.run(&["bitfield", "foo", "set", "u32", "0", "0"]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "i8", "0", "-120"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "i8", "0"], &[-120]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "i8", "8", "-1"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "i8", "8"], &[-1]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "i8", "16", "-1"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "i8", "16"], &[-1]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "i8", "24", "-10"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "i8", "24"], &[-10]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "i32", "0"], &[-1_996_488_714]);
    arr_ints(&mut t, &["bitfield", "foo", "incrby", "i8", "0", "-8"], &[-128]);

    // nonaligned offset reads/writes unsigned
    t.run(&["bitfield", "foo", "set", "i64", "0", "0"]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "u8", "1", "1"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u8", "1"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "u8", "9", "1"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u8", "9"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "u8", "17", "1"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u8", "17"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "u8", "25", "1"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u8", "25"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u8", "0"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u1", "8"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u1", "16"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u1", "24"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u1", "32"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u33", "0"], &[16_843_009]);

    // nonaligned offset reads/writes signed
    t.run(&["bitfield", "foo", "set", "i64", "0", "0"]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "i8", "1", "-1"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "i8", "1"], &[-1]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "i8", "9", "-1"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "i8", "9"], &[-1]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "i8", "17", "0"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "i8", "17"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "set", "i8", "25", "1"], &[0]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "i8", "25"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "i32", "1"], &[-65535]);

    // chaining
    t.run(&[
        "bitfield", "foo", "set", "u1", "0", "1", "set", "u1", "1", "1", "set", "u1", "2", "1",
        "set", "u1", "3", "1", "set", "u1", "4", "1", "set", "u1", "5", "1", "set", "u1", "6",
        "1", "set", "u1", "7", "1",
    ]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u8", "0"], &[255]);
    arr_ints(
        &mut t,
        &["bitfield", "foo", "set", "u1", "0", "0", "incrby", "u1", "0", "1", "get", "u1", "0"],
        &[1, 1, 1],
    );

    // positional offsets
    t.run(&["bitfield", "foo", "set", "u8", "#0", "1", "set", "u8", "#1", "1", "set", "u8", "#2", "1"]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u1", "7"], &[1]);
    arr_ints(&mut t, &["bitfield", "foo", "get", "u1", "15"], &[1]);
}

#[test]
fn bit_field_large_offset() {
    let mut t = Ctx::new();
    t.ok(&["set", "foo", "bar"]);

    arr_mixed(
        &mut t,
        &["bitfield", "foo", "get", "u32", "0", "overflow", "fail", "incrby", "u32", "0", "4294967295"],
        &[Some(1_650_553_344), None],
    );
    assert_eq!(t.int(&["strlen", "foo"]), 4);
    assert_eq!(t.bulk(&["get", "foo"]), b"bar\0".to_vec());

    // a read past the value end returns 0
    arr_ints(&mut t, &["bitfield", "foo", "get", "u32", "4294967295"], &[0]);
}

#[test]
fn bit_field_issue5237_set_overflow_sat() {
    let mut t = Ctx::new();
    set_b(&mut t, "key:bitfield_set", &[0xff, 0xf0, 0x00]);
    arr_ints(
        &mut t,
        &["bitfield", "key:bitfield_set", "overflow", "sat", "set", "i4", "0", "8", "set", "i4", "4", "7"],
        &[-1, -1],
    );
}

#[test]
fn bit_field_issue5237_incrby_correctness() {
    let mut t = Ctx::new();
    set_b(&mut t, "key:bitfield_incr", &[0xff, 0xf0, 0x00]);
    arr_ints(
        &mut t,
        &["bitfield", "key:bitfield_incr", "incrby", "u8", "0", "85", "incrby", "u8", "16", "170"],
        &[84, 170],
    );
}

#[test]
fn bit_field_issue5237_invalid_type_uppercase_set() {
    let mut t = Ctx::new();
    let bad_type = "ERR invalid bitfield type. use something like i16 u8. note that u64 is not supported but i64 is.";
    t.assert_err(&["bitfield", "key:bitfield_set:wrong:args", "set", "I8", "0", "0"], bad_type);
}

#[test]
fn bit_field_issue5237_invalid_type_uppercase_get() {
    let mut t = Ctx::new();
    let bad_type = "ERR invalid bitfield type. use something like i16 u8. note that u64 is not supported but i64 is.";
    t.assert_err(&["bitfield", "key:bitfield_get:wrong:args", "get", "I8", "0"], bad_type);
}

#[test]
fn bit_field_additional_wrong_arguments() {
    let mut t = Ctx::new();
    let syntax = "ERR syntax error";
    let bad_type = "ERR invalid bitfield type. use something like i16 u8. note that u64 is not supported but i64 is.";

    t.assert_err(&["bitfield", "foo", "get", "i-42", "0"], bad_type);
    t.assert_err(&["bitfield", "foo", "get", "i5?", "0"], bad_type);
    t.assert_err(&["bitfield", "foo", "get", "i0", "0"], bad_type);
    t.assert_err(&["bitfield", "foo", "set", "i-42", "0", "0"], bad_type);
    t.assert_err(&["bitfield", "foo", "set", "i5?", "0", "0"], bad_type);
    t.assert_err(&["bitfield", "foo", "set", "i0", "0", "0"], bad_type);

    // negative offsets
    t.assert_err(&["bitfield", "foo", "get", "i16", "-1"], syntax);
    t.assert_err(&["bitfield", "foo", "set", "i16", "-1", "0"], syntax);
    t.assert_err(&["bitfield", "foo", "incrby", "i16", "-1", "1"], syntax);

    // invalid values for SET and INCRBY
    t.assert_err(&["bitfield", "foo", "set", "i16", "0", "foo"], syntax);
    t.assert_err(&["bitfield", "foo", "incrby", "i16", "0", "bar"], syntax);
}

#[test]
fn bit_field_no_ops() {
    let mut t = Ctx::new();
    let a = t.arr(&["BITFIELD", "k", "OVERFLOW", "SAT"]);
    assert!(a.is_empty(), "reply {a:?}");
    let a = t.arr(&["BITFIELD", "k"]);
    assert!(a.is_empty(), "reply {a:?}");
    let a = t.arr(&["BITFIELD_RO", "k", "OVERFLOW", "SAT"]);
    assert!(a.is_empty(), "reply {a:?}");
    let a = t.arr(&["BITFIELD_RO", "k"]);
    assert!(a.is_empty(), "reply {a:?}");
}

#[test]
fn set_bit_offset_out_of_range() {
    let mut t = Ctx::new();
    t.assert_err(&["setbit", "sk", "2200000000", "1"], "out of range");
}

#[test]
fn bit_field_offset_out_of_range() {
    let mut t = Ctx::new();
    // writes are bounded by the max string size
    t.assert_err(&["bitfield", "bk", "set", "u8", "2200000000", "1"], "out of range");
    t.assert_err(&["bitfield", "bk", "incrby", "u8", "2200000000", "1"], "out of range");

    // reads return 0 past the value end; only offsets beyond uint32 are rejected
    arr_ints(&mut t, &["bitfield", "bk", "get", "u8", "2200000000"], &[0]);
    t.assert_err(&["bitfield", "bk", "get", "u8", "5000000000"], "out of range");
}
