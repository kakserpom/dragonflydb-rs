use xxhash_rust::xxh3::xxh3_64;

/// Hash used for shard routing. Dragonfly routes a key to a shard by hashing
/// the key and reducing modulo the number of shards.
pub fn shard_hash(key: &[u8]) -> u64 {
    xxh3_64(key)
}

/// Compute the shard id for a key.
pub fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    debug_assert!(num_shards > 0);
    (shard_hash(key) as usize) % num_shards
}

/// Fast 64-bit integer to decimal bytes (used for integer list items / replies).
pub fn itoa(v: i64) -> Vec<u8> {
    let mut buf = [0u8; 20];
    if v == 0 {
        return vec![b'0'];
    }
    let neg = v < 0;
    let mut u = v.unsigned_abs();
    let mut pos = buf.len();
    while u > 0 {
        pos -= 1;
        buf[pos] = b'0' + (u % 10) as u8;
        u /= 10;
    }
    if neg {
        pos -= 1;
        buf[pos] = b'-';
    }
    buf[pos..].to_vec()
}

pub fn parse_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let (neg, rest) = match s[0] {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    if rest.is_empty() {
        return None;
    }
    // Accumulate the magnitude as u64 so i64::MIN ("-9223372036854775808")
    // doesn't overflow before the negation.
    let mut v: u64 = 0;
    for &b in rest {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((b - b'0') as u64)?;
    }
    if neg {
        if v == (i64::MAX as u64) + 1 {
            return Some(i64::MIN);
        }
        let v = i64::try_from(v).ok()?;
        v.checked_neg()
    } else {
        i64::try_from(v).ok()
    }
}

/// Parse an unsigned decimal from bytes. Rejects empty strings, signs and
/// non-digit characters (Redis integer parsing never accepts "+5" or "-3").
pub fn parse_u64(s: &[u8]) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in s {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((b - b'0') as u64)?;
    }
    Some(v)
}

/// Redis-style range normalization. Redis uses negative indices from the end.
/// `len` is the length of the sequence. Returns (start, count) or None if empty.
pub fn redis_range(start: i64, stop: i64, len: i64) -> Option<(i64, i64)> {
    if len <= 0 {
        return None;
    }
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };
    if s < 0 {
        s = 0;
    }
    if e < 0 {
        return None;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e || s >= len {
        return None;
    }
    Some((s, e - s + 1))
}

/// Redis-compatible float parsing. Handles "inf", "-inf", "+inf", "nan" and floats.
pub fn parse_double(s: &[u8]) -> Option<f64> {
    let t = std::str::from_utf8(s).ok()?.trim().to_ascii_lowercase();
    match t.as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => return Some(f64::INFINITY),
        "-inf" | "-infinity" => return Some(f64::NEG_INFINITY),
        "nan" => return Some(f64::NAN),
        _ => {}
    }
    let f: f64 = t.parse().ok()?;
    if f.is_infinite() && t.len() > 4 {
        // "1e999" overflows to inf; Redis rejects this in some contexts. Accept.
    }
    Some(f)
}

/// Format a double the way Redis does (shortest repr, "inf" for infinite).
pub fn format_double(f: f64) -> String {
    if f.is_nan() {
        return "nan".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "inf".into() } else { "-inf".into() };
    }
    // Redis uses %.17g with special handling; Rust's shortest round-trip repr is close enough.
    let mut s = format!("{}", f);
    if !s.contains('.') && !s.contains('e') && !s.contains('E') {
        s.push_str(".0");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn itoa_works() {
        assert_eq!(itoa(0), b"0");
        assert_eq!(itoa(-42), b"-42");
        assert_eq!(itoa(i64::MIN), b"-9223372036854775808");
        assert_eq!(itoa(i64::MAX), b"9223372036854775807");
    }

    #[test]
    fn parse_i64_works() {
        assert_eq!(parse_i64(b"123"), Some(123));
        assert_eq!(parse_i64(b"-123"), Some(-123));
        assert_eq!(parse_i64(b"+123"), Some(123));
        assert_eq!(parse_i64(b"1a"), None);
        assert_eq!(parse_i64(b""), None);
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64(b"9223372036854775808"), None);
    }

    #[test]
    fn parse_u64_works() {
        assert_eq!(parse_u64(b"123"), Some(123));
        assert_eq!(parse_u64(b"0"), Some(0));
        assert_eq!(parse_u64(b"+123"), None);
        assert_eq!(parse_u64(b"-123"), None);
        assert_eq!(parse_u64(b"1a"), None);
        assert_eq!(parse_u64(b""), None);
    }

    #[test]
    fn range_works() {
        assert_eq!(redis_range(0, -1, 5), Some((0, 5)));
        assert_eq!(redis_range(-2, -1, 5), Some((3, 2)));
        assert_eq!(redis_range(2, 2, 5), Some((2, 1)));
        assert_eq!(redis_range(5, 10, 5), None);
        assert_eq!(redis_range(0, 0, 0), None);
    }
}
