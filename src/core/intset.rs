//! Redis `intset` codec used by the RDB DUMP/RESTORE wire format
//! (`RDB_TYPE_SET_INTSET` = 11).
//!
//! Blob layout (all little-endian):
//!   - `encoding`: u32 = 2 (INT16), 4 (INT32) or 8 (INT64)
//!   - `length`:   u32 = number of records
//!   - `contents`: `length` signed integers, each `encoding` bytes wide,
//!     sorted strictly ascending (no duplicates).
//!
//! Mirrors `dragonfly/src/redis/intset.c` (blob format, `intsetValidateIntegrity`,
//! `_intsetValueEncoding`) and `string2ll` from `util.c` (strict decimal parse,
//! used by `IntsetAddSafe` to decide whether a member is stored as an intset).

const INTSET_ENC_INT16: usize = 2;
const INTSET_ENC_INT32: usize = 4;
const INTSET_ENC_INT64: usize = 8;

const HEADER: usize = 8;

/// The encoding width (2/4/8) required to represent `v`.
#[must_use]
pub fn value_encoding(v: i64) -> usize {
    if v < i64::from(i32::MIN) || v > i64::from(i32::MAX) {
        INTSET_ENC_INT64
    } else if v < i64::from(i16::MIN) || v > i64::from(i16::MAX) {
        INTSET_ENC_INT32
    } else {
        INTSET_ENC_INT16
    }
}

/// Strict decimal parse of a long long, mirroring `string2ll` (`util.c`).
///
/// Accepts `"0"`, optional leading `-`, digits with no leading zeros and no
/// trailing garbage, within `[i64::MIN, i64::MAX]`.
#[must_use]
pub fn string2ll(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s == b"0" {
        return Some(0);
    }
    let mut i = 0;
    let negative = s[0] == b'-';
    if negative {
        i = 1;
        if i == s.len() {
            return None;
        }
    }
    if !s[i].is_ascii_digit() || s[i] == b'0' {
        return None;
    }
    let mut v: u64 = u64::from(s[i] - b'0');
    i += 1;
    while i < s.len() {
        if !s[i].is_ascii_digit() {
            return None;
        }
        let d = u64::from(s[i] - b'0');
        v = v.checked_mul(10).and_then(|x| x.checked_add(d))?;
        i += 1;
    }
    if negative {
        // Allow magnitude up to 2^63 (i.e. i64::MIN).
        if v > 1u64 << 63 {
            return None;
        }
        if v == 1u64 << 63 {
            Some(i64::MIN)
        } else {
            Some(-(v as i64))
        }
    } else {
        if v > i64::MAX as u64 {
            return None;
        }
        Some(v as i64)
    }
}

/// Read a record at position `i` (0-based) from `contents`.
fn read_value(buf: &[u8], enc: usize, i: usize) -> i64 {
    let off = HEADER + i * enc;
    let slice = &buf[off..off + enc];
    let mut b = [0u8; 8];
    b[..enc].copy_from_slice(slice);
    match enc {
        INTSET_ENC_INT16 => i64::from(i16::from_le_bytes([b[0], b[1]])),
        INTSET_ENC_INT32 => i64::from(i32::from_le_bytes([b[0], b[1], b[2], b[3]])),
        _ => i64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]),
    }
}

/// Validate an intset blob, mirroring `intsetValidateIntegrity`.
///
/// `deep` additionally checks strictly-ascending records (no duplicates, no
/// out-of-order). An empty intset is rejected (the reference considers
/// `count == 0` invalid).
#[must_use]
pub fn validate_integrity(buf: &[u8], deep: bool) -> bool {
    if buf.len() < HEADER {
        return false;
    }
    let encoding = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let record_size = match encoding {
        INTSET_ENC_INT64 | INTSET_ENC_INT32 | INTSET_ENC_INT16 => encoding,
        _ => return false,
    };
    let count = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    if HEADER + count * record_size != buf.len() {
        return false;
    }
    if count == 0 {
        return false;
    }
    if deep {
        let mut prev = read_value(buf, record_size, 0);
        for i in 1..count {
            let cur = read_value(buf, record_size, i);
            if cur <= prev {
                return false;
            }
            prev = cur;
        }
    }
    true
}

/// Decode all records of a validated intset blob. Returns `None` on any
/// integrity violation.
#[must_use]
pub fn values(buf: &[u8]) -> Option<Vec<i64>> {
    if !validate_integrity(buf, true) {
        return None;
    }
    let encoding = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let count = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    Some((0..count).map(|i| read_value(buf, encoding, i)).collect())
}

/// Build an intset blob from a set of `i64` values. The record encoding is the
/// minimal one that fits every value (`_intsetValueEncoding`); records are
/// serialized sorted ascending, as the reference always keeps them ordered.
pub fn build<I: IntoIterator<Item = i64>>(values: I) -> Vec<u8> {
    let mut vals: Vec<i64> = values.into_iter().collect();
    vals.sort_unstable();
    let enc = vals
        .iter()
        .map(|&v| value_encoding(v))
        .max()
        .unwrap_or(INTSET_ENC_INT16);
    let mut buf = Vec::with_capacity(HEADER + vals.len() * enc);
    buf.extend_from_slice(&(enc as u32).to_le_bytes());
    buf.extend_from_slice(&(vals.len() as u32).to_le_bytes());
    for v in &vals {
        match enc {
            INTSET_ENC_INT16 => buf.extend_from_slice(&(*v as i16).to_le_bytes()),
            INTSET_ENC_INT32 => buf.extend_from_slice(&(*v as i32).to_le_bytes()),
            _ => buf.extend_from_slice(&v.to_le_bytes()),
        }
    }
    buf
}

/// Number of records in a (validated) intset blob.
#[must_use]
pub fn len(buf: &[u8]) -> Option<usize> {
    if buf.len() < HEADER {
        return None;
    }
    Some(u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string2ll_rules() {
        assert_eq!(string2ll(b"0"), Some(0));
        assert_eq!(string2ll(b"19"), Some(19));
        assert_eq!(string2ll(b"-100"), Some(-100));
        assert_eq!(string2ll(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(string2ll(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(string2ll(b""), None);
        assert_eq!(string2ll(b"-"), None);
        assert_eq!(string2ll(b"00"), None);
        assert_eq!(string2ll(b"0123"), None);
        assert_eq!(string2ll(b"-0"), None);
        assert_eq!(string2ll(b"+1"), None);
        assert_eq!(string2ll(b"1a"), None);
        assert_eq!(string2ll(b" 1"), None);
        assert_eq!(string2ll(b"9223372036854775808"), None);
        assert_eq!(string2ll(b"-9223372036854775809"), None);
    }

    #[test]
    fn blob_roundtrip() {
        let blob = build([1i64, 2, 3]);
        assert_eq!(blob, vec![2, 0, 0, 0, 3, 0, 0, 0, 1, 0, 2, 0, 3, 0]);
        assert!(validate_integrity(&blob, true));
        assert_eq!(values(&blob), Some(vec![1, 2, 3]));

        let blob = build([-5i64, 200_000, -200_000, 1000]);
        // 200000 does not fit i16, so encoding must be INT32.
        assert_eq!(u32::from_le_bytes([blob[0], blob[1], blob[2], blob[3]]), 4);
        assert!(validate_integrity(&blob, true));
        assert_eq!(values(&blob), Some(vec![-200_000, -5, 1000, 200_000]));

        let blob = build([i64::MAX, i64::MIN]);
        assert_eq!(u32::from_le_bytes([blob[0], blob[1], blob[2], blob[3]]), 8);
        assert_eq!(values(&blob), Some(vec![i64::MIN, i64::MAX]));
    }

    #[test]
    fn single_negative_record_uses_i16() {
        // -1 fits INT16: encoding stays 2 even though sign bit is set.
        let blob = build([-1i64]);
        assert_eq!(blob, vec![2, 0, 0, 0, 1, 0, 0, 0, 0xff, 0xff]);
        assert!(validate_integrity(&blob, true));
        assert_eq!(values(&blob), Some(vec![-1]));
    }

    #[test]
    fn rejects_invalid_blobs() {
        // Too short for the header.
        assert!(!validate_integrity(&[2, 0, 0, 0], true));
        // Unknown encoding.
        let mut blob = build([1i64, 2]);
        blob[0] = 3;
        assert!(!validate_integrity(&blob, true));
        // Size mismatch (truncated record).
        let mut blob = build([1i64, 2, 3]);
        blob.truncate(blob.len() - 1);
        assert!(!validate_integrity(&blob, true));
        // Empty intset is invalid.
        let blob = [2u8, 0, 0, 0, 0, 0, 0, 0];
        assert!(!validate_integrity(&blob, true));
        // Unsorted (deep check).
        let mut blob = build([1i64, 2, 3]);
        blob[HEADER] = 3;
        blob[HEADER + 1] = 0;
        blob[HEADER + 2] = 1;
        blob[HEADER + 3] = 0;
        assert!(!validate_integrity(&blob, true));
        assert!(validate_integrity(&blob, false));
    }

    #[test]
    fn rejects_duplicate_records() {
        // 1,1 is a duplicate; deep validation rejects it.
        let mut blob = build([1i64, 2]);
        blob[HEADER + 2] = 1;
        blob[HEADER + 3] = 0;
        assert!(!validate_integrity(&blob, true));
        assert!(validate_integrity(&blob, false));
    }

    #[test]
    fn values_none_on_corruption() {
        let mut blob = build([1i64, 2, 3]);
        blob[HEADER] = 9;
        assert_eq!(values(&blob), None);
    }
}
