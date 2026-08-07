use xxhash_rust::xxh3::xxh3_64;

/// Hash used for shard routing. Dragonfly routes a key to a shard by hashing
/// the key and reducing modulo the number of shards.
#[must_use]
pub fn shard_hash(key: &[u8]) -> u64 {
    xxh3_64(key)
}

/// Compute the shard id for a key.
#[must_use]
pub fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    debug_assert!(num_shards > 0);
    (shard_hash(key) as usize) % num_shards
}

/// Fast 64-bit integer to decimal bytes (used for integer list items / replies).
#[must_use]
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

/// `absl::AlphaNum(double)`: `%.6g` formatting (SixDigitsToBuffer). The
/// reference uses `AlphaNum` for `absl::StrAppend` of floating-point fields
/// (`string_stats.cc` `AverageLength`, `dragonfly.ihash` reply strings).
#[must_use]
pub fn g6_format(d: f64) -> String {
    let mut buf = [0u8; 64];
    let len = unsafe { libc::snprintf(buf.as_mut_ptr().cast(), buf.len(), c"%.6g".as_ptr(), d) };
    let len = if len < 0 {
        0
    } else {
        (len as usize).min(buf.len())
    };
    String::from_utf8_lossy(&buf[..len]).into_owned()
}

#[must_use]
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
        v = v.checked_mul(10)?.checked_add(u64::from(b - b'0'))?;
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
#[must_use]
pub fn parse_u64(s: &[u8]) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in s {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(u64::from(b - b'0'))?;
    }
    Some(v)
}

/// Redis-style range normalization. Redis uses negative indices from the end.
/// `len` is the length of the sequence. Returns (start, count) or None if empty.
#[must_use]
pub fn redis_range(start: i64, stop: i64, len: i64) -> Option<(i64, i64)> {
    if len <= 0 {
        return None;
    }
    // Reference `OpGetRange`: `if (start < 0 && end < start) return ""`.
    if start < 0 && stop < start {
        return None;
    }
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };
    if s < 0 {
        s = 0;
    }
    if e < 0 {
        // Reference clamps a negative `end` to 0 (`max(strlen + end, 0)`),
        // e.g. `getrange key 0 -100` returns the first character.
        e = 0;
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
#[must_use]
pub fn parse_double(s: &[u8]) -> Option<f64> {
    let t = std::str::from_utf8(s).ok()?.trim().to_ascii_lowercase();
    match t.as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => return Some(f64::INFINITY),
        "-inf" | "-infinity" => return Some(f64::NEG_INFINITY),
        _ => {}
    }
    let f: f64 = t.parse().ok()?;
    if f.is_infinite() {
        // Values that overflow f64 ("1e999", "1.8E+308") are out of range: the
        // reference `ParseDouble` (fast_float from_chars result_out_of_range) and
        // `TryParseNum` (absl::SimpleAtod) both reject them.
        return None;
    }
    Some(f)
}

/// Parse a list blocking-command timeout, replicating the reference `Timeout`
/// rule in `list_family.cc` (`Validated<float, NotNan<kTimeoutNotFloatErr>,
/// NonNegative<kTimeoutNegativeErr>, WithinTimeoutLimit>` with
/// `kMaxBlockingTimeoutSec = u32::MAX / 1000`). Returns the error string with
/// the "ERR " prefix, ready for a reply.
pub fn parse_list_timeout(arg: &[u8]) -> Result<f64, String> {
    let s = std::str::from_utf8(arg).unwrap_or("");
    let v: f64 = s
        .trim()
        .parse()
        .map_err(|_| "ERR timeout is not a float or out of range".to_string())?;
    if v.is_nan() {
        return Err("ERR timeout is not a float or out of range".into());
    }
    if v < 0.0 {
        return Err("ERR timeout is negative".into());
    }
    if v > 4_294_967.296 {
        return Err("ERR timeout is out of range".into());
    }
    Ok(v)
}

/// Format a double the way Redis does (shortest repr, "inf" for infinite).
#[must_use]
pub fn format_double(f: f64) -> String {
    if f.is_nan() {
        return "nan".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "inf".into() } else { "-inf".into() };
    }
    if f == 0.0 {
        return "0".into();
    }
    // Reference reply_builder.cc FormatDouble: DoubleToStringConverter(UNIQUE_ZERO|EMIT_POSITIVE_EXPONENT_SIGN, ...).ToShortest().
    // Rust's shortest round-trip repr matches for the ranges that matter; no forced ".0" suffix.
    format!("{f}")
}

/// Format a Lua numeric argument like the reference's `%.17g` in
/// `Interpreter::PrepareArgs` (interpreter.cc): 17 significant digits with
/// trailing zeros stripped, fixed notation when the base-10 exponent is in
/// `[-4, 17)`, otherwise scientific with a signed, zero-padded exponent. This
/// differs from [`format_double`] (shortest round-trip, RESP double replies):
/// `redis.call('SET', 'k', 0.1)` must send `"0.10000000000000001"` verbatim.
#[must_use]
pub fn format_lua_float(f: f64) -> String {
    if f.is_nan() {
        return "nan".into();
    }
    if f.is_infinite() {
        return if f < 0.0 { "-inf".into() } else { "inf".into() };
    }
    if f == 0.0 {
        return if f.is_sign_negative() {
            "-0".into()
        } else {
            "0".into()
        };
    }
    format_g(f, 17)
}

/// `%.<sig>g` equivalent (C printf semantics): `sig` significant digits with
/// trailing zeros stripped, fixed notation when the base-10 exponent is in
/// `[-4, sig)`, otherwise scientific with a signed, zero-padded exponent.
/// The caller must pass a finite, non-zero value.
#[must_use]
pub(crate) fn format_g(f: f64, sig: i32) -> String {
    // `{:.{prec}e}` with `prec = sig - 1` is correctly rounded to `sig`
    // significant digits, the same value `%.<sig>g` starts from.
    let sci = format!("{f:.prec$e}", prec = (sig - 1) as usize);
    let (mant, exp) = sci.split_once('e').unwrap();
    let exp: i32 = exp.parse().unwrap();
    let mut digits: String = mant.chars().filter(|&c| c != '.').collect();
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
    }
    if (-4..sig).contains(&exp) {
        let ip = exp as i64 + 1;
        if ip >= digits.len() as i64 {
            format!(
                "{}{}",
                digits,
                "0".repeat((ip - digits.len() as i64) as usize)
            )
        } else if ip <= 0 {
            format!("0.{}{}", "0".repeat((-ip) as usize), digits)
        } else {
            let (a, b) = digits.split_at(ip as usize);
            format!("{a}.{b}")
        }
    } else {
        let mantissa = if digits.len() == 1 {
            digits
        } else {
            let mut m = digits;
            m.insert(1, '.');
            m
        };
        format!(
            "{mantissa}e{}{:02}",
            if exp < 0 { "-" } else { "+" },
            exp.abs()
        )
    }
}

/// Lua 5.4 `lua_tolstring` number formatting: integral values that fit in a
/// `lua_Integer` (53-bit) use the precise decimal form, everything else is
/// `%.14g` upgraded to `%.17g` when that avoids a decimal exponent or point
/// (`lua_number2strbuff`). Used by `redis.sha1hex` on number arguments.
#[must_use]
pub fn lua_tolstring(f: f64) -> String {
    const MAX_INT: f64 = 9_007_199_254_740_992.0; // 2^53, `MAX_FP` in lobject.h
    if f.is_nan() {
        return "nan".into();
    }
    if f.is_infinite() {
        return if f < 0.0 { "-inf".into() } else { "inf".into() };
    }
    if f == 0.0 {
        return "0".into();
    }
    if f.fract() == 0.0 && f.abs() <= MAX_INT {
        return String::from_utf8(itoa(f as i64)).unwrap();
    }
    let short = format_g(f, 14);
    // `strpbrk(buff, "E.")` in `lua_float2strbuff`: a plain integer needs no
    // alternative; otherwise try `%.17g` and prefer it if it also has no
    // exponent/point.
    if !short.contains(['.', 'e', 'E']) {
        return short;
    }
    let long = format_g(f, 17);
    if !long.contains(['.', 'e', 'E']) {
        return long;
    }
    short
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
    fn format_double_works() {
        assert_eq!(format_double(5.0), "5");
        assert_eq!(format_double(1.5), "1.5");
        assert_eq!(format_double(3_673_983_950_397_063.0), "3673983950397063");
        assert_eq!(format_double(0.0), "0");
        assert_eq!(format_double(-0.0), "0");
        assert_eq!(format_double(-3.25), "-3.25");
        assert_eq!(format_double(f64::INFINITY), "inf");
        assert_eq!(format_double(f64::NEG_INFINITY), "-inf");
        assert_eq!(format_double(f64::NAN), "nan");
    }

    #[test]
    fn format_lua_float_matches_c_percent_17g() {
        // Reference outputs captured from `printf("%.17g", v)` (glibc).
        let cases: &[(f64, &str)] = &[
            (1.5, "1.5"),
            (2.0, "2"),
            (0.1, "0.10000000000000001"),
            (100.0, "100"),
            (0.0, "0"),
            (-0.0, "-0"),
            (3_673_983_950_397_063.0, "3673983950397063"),
            (1e16, "10000000000000000"),
            (1e17, "1e+17"),
            (0.0001, "0.0001"),
            (1e-5, "1.0000000000000001e-05"),
            (2.5e-5, "2.5000000000000001e-05"),
            (0.300_000_000_000_000_04, "0.30000000000000004"),
            (123.456, "123.456"),
            (123_456_789_012_345_678.0, "1.2345678901234568e+17"),
            (f64::MIN_POSITIVE, "2.2250738585072014e-308"),
            (f64::MAX, "1.7976931348623157e+308"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
        ];
        for (v, want) in cases {
            assert_eq!(format_lua_float(*v), *want, "%.17g mismatch for {v}");
        }
        assert_eq!(format_lua_float(f64::NAN), "nan");
    }

    #[test]
    fn lua_tolstring_matches_lua54() {
        // Values captured from Lua 5.4's `tostring()` (lua_number2strbuff).
        let cases: &[(f64, &str)] = &[
            (3.7, "3.7"),
            (0.1, "0.1"),
            (2.0, "2"),
            (100.0, "100"),
            (0.0, "0"),
            (-0.0, "0"),
            (1e16, "10000000000000000"),
            (1e17, "1e+17"),
            (3.0e6, "3000000"),
            (1.0e9, "1000000000"),
            (0.0001, "0.0001"),
            (2.5e-5, "2.5e-05"),
            (123.456, "123.456"),
            (0.300_000_000_000_000_04, "0.3"),
            (123_456_789_012_345_678.0, "1.2345678901235e+17"),
            (f64::NAN, "nan"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
        ];
        for (v, want) in cases {
            assert_eq!(lua_tolstring(*v), *want, "lua_tolstring mismatch for {v}");
        }
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
