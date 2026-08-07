//! Port of `dragonfly/src/server/hll_family_test.cc`.
//!
//! Adaptations from the C++ original:
//! - `Run(...)` becomes `t.run`, `CheckedInt(...)` becomes `t.int`.
//! - `MakeOverflowingSparseHll()` and the truncated-run payload are built
//!   directly and stored with the binary `run_b`/`ok_b` helpers.
//! - `MultipleValues_random` uses a fixed-seed xorshift instead of
//!   `std::random_device`/`std::mt19937`, keeping the test reproducible while
//!   exercising the same 1..20 values-per-command distribution.
//! - `SimdMatchesScalar` and the benchmarks are dropped: the Rust port has a
//!   single scalar HLL implementation, so there is no SIMD fast path to compare.
//! - `MergeInvalid`'s `GetDebugInfo().shards_count == 2` check is implied by the
//!   harness (every `Ctx` starts 2 shards).

mod common;

use common::*;

/// `kInvalidHllError` / `kCorruptedHllError` from the reference.
const INVALID_HLL: &str = "ERR Key is not a valid HyperLogLog string value";
const CORRUPTED_HLL: &str = "INVALIDOBJ Corrupted HLL object detected.";

/// `kHllDenseSize` from the reference (also `getDenseHllSize()` in the C++).
const DENSE_HLL_SIZE: i64 = 12304;

fn generate_unique_value(index: i64) -> String {
    format!("Value_{{{index}}}")
}

/// Builds the CVE-2025-32023 payload: a sparse HLL whose XZERO run lengths sum
/// past INT_MAX so the decoder's `idx` cursor wraps; the trailing VAL slips
/// past the run-length guards unless every branch checks them.
fn make_overflowing_sparse_hll() -> Vec<u8> {
    const K_XZERO_OPS: usize = 155_486;
    let mut hll = Vec::with_capacity(16 + K_XZERO_OPS * 2 + 1);
    hll.extend_from_slice(b"HYLL");
    hll.push(1); // encoding = HLL_SPARSE
    hll.extend_from_slice(&[0u8; 3]); // notused
    hll.extend_from_slice(&[0u8; 8]); // cached cardinality
    for _ in 0..K_XZERO_OPS {
        hll.push(0x7f); // XZERO, 14-bit length-1 == 16383
        hll.push(0xff);
    }
    hll.push(0x80); // VAL: value 1, run length 1
    hll
}

/// Fixed-seed xorshift64, standing in for the reference's `std::mt19937`.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

#[test]
fn simple() {
    let mut t = Ctx::new();
    t.assert_int(&["pfadd", "key", "1"], 1);
    t.assert_int(&["pfadd", "key", "1"], 0);
    t.assert_int(&["pfcount", "key"], 1);
}

#[test]
fn promote() {
    let mut t = Ctx::new();
    // Sparse hll is promoted to dense at the 1660th+- insertion
    // This value varies if any parameter in hyperloglog.c changes.
    let promote_i = 1660;
    // Keep consistent with hyperloglog.c: sparse stays under 3001 bytes.
    let k_hll_sparse_max_bytes = 3000;
    for i in 0..20000 {
        let newkey = generate_unique_value(i);
        t.run(&["pfadd", "key", &newkey]);
        let len = t.int(&["strlen", "key"]);
        if i < promote_i {
            assert!(len < k_hll_sparse_max_bytes + 1, "len {len} at {i}");
        } else {
            assert_eq!(len, DENSE_HLL_SIZE, "at {i}");
        }
    }
    // HyperLogLog computations come with a margin of error, with a standard
    // error rate of 0.81%. Set it to 5% so this test won't fail unless
    // something went wrong badly.
    let count = t.int(&["pfcount", "key"]);
    assert!(
        (count as f64 - 20000.0).abs() / 20000.0 < 0.05,
        "count {count}"
    );
}

#[test]
fn multiple_values() {
    let mut t = Ctx::new();
    t.assert_int(&["pfadd", "key", "1", "2", "3"], 1);
    t.assert_int(&["pfcount", "key"], 3);
    t.assert_int(&["pfadd", "key", "1", "2", "3"], 0);
    t.assert_int(&["pfcount", "key"], 3);
    t.assert_int(&["pfadd", "key", "1"], 0);
    t.assert_int(&["pfcount", "key"], 3);
    t.assert_int(&["pfadd", "key", "2"], 0);
    t.assert_int(&["pfcount", "key"], 3);
    t.assert_int(&["pfadd", "key", "3"], 0);
    t.assert_int(&["pfcount", "key"], 3);
    t.assert_int(&["pfadd", "key", "3", "4"], 1);
    t.assert_int(&["pfcount", "key"], 4);
    t.assert_int(&["pfadd", "key", "5"], 1);
    t.assert_int(&["pfcount", "key"], 5);
    t.assert_int(&["pfadd", "key", "1", "2", "3", "4", "5"], 0);
    t.assert_int(&["pfcount", "key"], 5);
}

#[test]
fn multiple_values_random() {
    let mut t = Ctx::new();
    let insertions = 20000;
    let mut unique_values = 0;
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    // cumulated pfadd result
    for i in 0..insertions {
        // Number of values to insert, uniform in [1, 20] like the reference.
        let num_values = (rng.next() % 20) as usize + 1;
        unique_values += num_values;

        let mut values = Vec::with_capacity(num_values + 2);
        values.push("pfadd".to_string());
        values.push("key".to_string());
        for j in 0..num_values {
            values.push(generate_unique_value((i * 20 + j) as i64));
        }
        let args: Vec<&str> = values.iter().map(String::as_str).collect();
        t.run(&args);
    }
    // Standard error rate is 0.81%; allow 5%.
    let count = t.int(&["pfcount", "key"]);
    assert!(
        (count as f64 - unique_values as f64).abs() / (unique_values as f64) < 0.05,
        "count {count}, expected ~{unique_values}"
    );
}

#[test]
fn add_invalid() {
    let mut t = Ctx::new();
    t.ok(&["set", "key", "..."]);
    t.assert_err(&["pfadd", "key", "1"], INVALID_HLL);
    t.assert_err(&["pfcount", "key"], INVALID_HLL);
}

#[test]
fn other_type() {
    let mut t = Ctx::new();
    t.assert_int(&["zadd", "key", "1", "a"], 1);
    t.assert_err(&["pfadd", "key", "1"], "wrong kind of value");
    t.assert_err(&["pfcount", "key"], "wrong kind of value");
}

#[test]
fn count_empty() {
    let mut t = Ctx::new();
    t.assert_int(&["pfcount", "nonexisting"], 0);
}

#[test]
fn count_invalid() {
    let mut t = Ctx::new();
    t.ok(&["set", "key", "..."]);
    t.assert_err(&["pfcount", "key"], INVALID_HLL);
}

#[test]
fn count_multiple() {
    let mut t = Ctx::new();
    t.assert_int(&["pfadd", "key1", "1", "2", "3"], 1);
    t.assert_int(&["pfcount", "key1"], 3);
    t.assert_int(&["pfadd", "key2", "1", "2", "3"], 1);
    t.assert_int(&["pfcount", "key2"], 3);
    t.assert_int(&["pfadd", "key3", "2", "3"], 1);
    t.assert_int(&["pfcount", "key3"], 2);
    t.assert_int(&["pfadd", "key4", "4", "5"], 1);
    t.assert_int(&["pfcount", "key4"], 2);
    t.assert_int(&["pfcount", "key1", "key4"], 5);
    t.assert_int(&["pfcount", "non-existing-key1", "non-existing-key2"], 0);
    t.assert_int(&["pfcount", "key1", "non-existing-key"], 3);
    t.assert_int(&["pfcount", "key1", "key2"], 3);
    t.assert_int(&["pfcount", "key1", "key3"], 3);
    t.assert_int(&["pfcount", "key1", "key2", "key3"], 3);
    t.assert_int(&["pfcount", "key1", "key2", "key3", "key4"], 5);
    t.assert_int(
        &["pfcount", "key1", "key2", "key3", "key4", "non-existing"],
        5,
    );
    t.assert_int(&["pfcount", "key1", "key4"], 5);
}

#[test]
fn count_multiple_with_wrong_type() {
    let mut t = Ctx::new();
    t.ok(&["set", "key1", "value1"]);
    t.assert_int(&["pfadd", "key", "value"], 1);
    t.assert_int(&["pfadd", "list1 element1", "data"], 1);
    t.assert_err(&["pfcount", "key1", "key", "list1 element1"], CORRUPTED_HLL);
}

#[test]
fn merge_to_new() {
    let mut t = Ctx::new();
    t.assert_int(&["pfadd", "key1", "1", "2", "3"], 1);
    t.assert_int(&["pfadd", "key2", "4", "5"], 1);
    t.ok(&["pfmerge", "key3", "key1", "key2"]);
    t.assert_int(&["pfcount", "key3"], 5);
}

#[test]
fn merge_to_existing() {
    let mut t = Ctx::new();
    t.assert_int(&["pfadd", "key1", "1", "2", "3"], 1);
    t.assert_int(&["pfadd", "key2", "4", "5"], 1);
    t.ok(&["pfmerge", "key3", "key2", "key1"]);
    t.assert_int(&["pfcount", "key3"], 5);
    t.ok(&["pfmerge", "key3", "key3"]);
    t.assert_int(&["pfcount", "key3"], 5);
    t.ok(&["pfmerge", "key3"]);
    t.assert_int(&["pfcount", "key3"], 5);
    t.assert_int(&["pfadd", "key4", "4", "5", "6"], 1);
    t.ok(&["pfmerge", "key3", "key4"]);
    t.assert_int(&["pfcount", "key3"], 6);
}

#[test]
fn merge_non_existing() {
    let mut t = Ctx::new();
    t.assert_int(&["pfadd", "key1", "1", "2", "3"], 1);
    t.ok(&["pfmerge", "key3", "key1", "key2"]);
    t.assert_int(&["pfcount", "key3"], 3);
}

#[test]
fn merge_overlapping() {
    let mut t = Ctx::new();
    t.assert_int(&["pfadd", "key1", "1", "2", "3"], 1);
    t.assert_int(&["pfadd", "key2", "2", "3"], 1);
    t.assert_int(&["pfadd", "key3", "1", "3"], 1);
    t.assert_int(&["pfadd", "key4", "2", "3"], 1);
    t.assert_int(&["pfadd", "key5", "3"], 1);
    t.ok(&["pfmerge", "key6", "key1", "key2", "key3", "key4", "key5"]);
    t.assert_int(&["pfcount", "key6"], 3);
}

#[test]
fn merge_invalid() {
    let mut t = Ctx::new();
    // The reference asserts GetDebugInfo().shards_count == 2 here; the harness
    // always starts 2 shards.
    t.assert_int(&["pfadd", "key1", "1", "2", "3"], 1);
    t.ok(&["set", "key4", "..."]);
    t.assert_err(&["pfmerge", "key1", "key4"], CORRUPTED_HLL);
    t.assert_int(&["pfcount", "key1"], 3);
}

#[test]
fn merge_with_invalid_hll_format() {
    let mut t = Ctx::new();
    let key1 = "complex@key \"weird!field\" \"value\\nwith\\tescape sequences\"";
    let key2 = "\"key with \\\"quotes\\\"\" \"value with \\\\backslashes\\\\\"";
    t.assert_int(&["pfadd", key1, "some_element"], 1);
    t.assert_int(&["append", key1, "corrupt_data"], 33);
    t.assert_int(&["pfadd", key2, "element1"], 1);
    t.assert_err(&["pfmerge", "result_key", key1, key2], CORRUPTED_HLL);
}

// CVE-2025-32023. Reading this payload used to run the sparse decoder's cursor
// past INT_MAX and write a register through a wild pointer; every opcode branch
// now checks the run length, so all three commands report the HLL as corrupted.
#[test]
fn corrupted_sparse_run_length_overflow() {
    let mut t = Ctx::new();
    let payload = make_overflowing_sparse_hll();
    t.ok_b(&[b"set".to_vec(), b"overflow".to_vec(), payload]);

    // PFCOUNT and PFMERGE decode through convertSparseToDenseHll().
    t.assert_err(&["pfcount", "overflow"], CORRUPTED_HLL);

    t.assert_int(&["pfadd", "src", "hi"], 1);
    t.assert_err(&["pfmerge", "dest", "overflow", "src"], CORRUPTED_HLL);

    // PFADD decodes through hllSparseSet()'s promote path: the value is far
    // above HLL_SPARSE_MAX_BYTES, so the very first insert tries to convert to
    // dense.
    t.assert_err(&["pfadd", "overflow", "foo"], INVALID_HLL);
}

// Covers the ZERO/XZERO arm of the same guard on an input small enough not to
// need integer overflow: an over-long run must be rejected, never truncated.
#[test]
fn corrupted_sparse_truncated_run() {
    let mut t = Ctx::new();
    // XZERO covering 16384 registers followed by another one: the second
    // overruns the register space.
    let mut hll = b"HYLL".to_vec();
    hll.push(1);
    hll.extend_from_slice(&[0u8; 3]);
    hll.extend_from_slice(&[0u8; 8]);
    for _ in 0..2 {
        hll.push(0x7f);
        hll.push(0xff);
    }
    t.ok_b(&[b"set".to_vec(), b"truncated".to_vec(), hll]);
    t.assert_err(&["pfcount", "truncated"], CORRUPTED_HLL);
}

// PFCOUNT over several keys merges into a raw register array; the union
// estimate has to match the cardinality of the same keys merged with PFMERGE
// exactly.
#[test]
fn count_multiple_agrees_with_merge() {
    const K_VALUES_PER_KEY: i64 = 20000;
    let mut t = Ctx::new();
    for i in 0..K_VALUES_PER_KEY {
        let v1 = generate_unique_value(i);
        t.run(&["pfadd", "k1", &v1]);
        let v2 = generate_unique_value(K_VALUES_PER_KEY + i);
        t.run(&["pfadd", "k2", &v2]);
    }

    t.ok(&["pfmerge", "merged", "k1", "k2"]);
    let merged = t.int(&["pfcount", "merged"]);
    t.assert_int(&["pfcount", "k1", "k2"], merged);

    // Sanity check that the shared estimate is in the right ballpark.
    assert!(
        (merged as f64 - 2.0 * K_VALUES_PER_KEY as f64).abs() / (2.0 * K_VALUES_PER_KEY as f64)
            < 0.05,
        "merged {merged}"
    );
}

// hllSparseSet() promotes straight to dense when the count cannot be held by a
// VAL opcode. The generated input has 35 zero bits after the 14-bit HLL
// register index, giving hllPatLen() a count of 36.
#[test]
fn sparse_set_promotes_on_large_count() {
    let mut t = Ctx::new();
    t.assert_int(&["pfadd", "key", ".K{bTLLX"], 1);
    t.assert_int(&["strlen", "key"], DENSE_HLL_SIZE);
    t.assert_int(&["pfcount", "key"], 1);
}
