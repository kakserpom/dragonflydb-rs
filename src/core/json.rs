//! JSON value model, parser and serializer, ported from
//! `dragonfly/src/core/json/json_object.{h,cc}`.
//!
//! Parsing delegates to `simd-json`, which produces an owned tree in a single
//! pass; we then convert it into our own representation whose object members
//! are kept sorted by key. This matches the reference behavior, where jsoncons
//! `json` stores objects in a sorted `std::map`, so object iteration and
//! serialization always visit members in lexicographic key order.
//!
//! The serializer mirrors jsoncons's `dump` output exactly: numbers use the
//! grisu3-shortest algorithm with jsoncons's `prettify_string` presentation
//! (integral doubles get a trailing `.0`, scientific notation uses a signed,
//! zero-padded exponent, etc.), strings escape only `"`, `\` and control
//! characters, and the pretty form inserts configurable indent/newline/space
//! strings (the `INDENT`/`NEWLINE`/`SPACE` options of `JSON.GET`).

use simd_json::OwnedValue;
use simd_json::StaticNode;

/// The maximum allowed JSON nesting depth (matching the reference
/// `json_nesting_depth_limit`). The parser additionally caps the effective
/// limit at `input.len() / 2`, since nesting an object requires at least two
/// characters per level.
pub const MAX_NESTING_DEPTH: usize = 64;

/// Parse failure; every malformed document (including ones that exceed the
/// nesting depth limit) reports the same error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonError;

impl std::fmt::Display for JsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to parse JSON")
    }
}

impl std::error::Error for JsonError {}

/// A JSON value, mirroring the reference `JsonType` (a `jsoncons::json`).
///
/// Object members are stored sorted by key; all object operations preserve
/// this invariant so serialization and iteration stay deterministic.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Double(f64),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    /// Parse a JSON document, mirroring `ShardJsonFromString`. The input is
    /// rejected if it exceeds the nesting depth limit or fails to parse.
    pub fn parse(input: &[u8]) -> Result<Json, JsonError> {
        check_depth(input)?;

        let mut buf = input.to_vec();
        let value = simd_json::to_owned_value(&mut buf).map_err(|_| JsonError)?;
        Ok(json_from_owned(value))
    }

    /// A JSON null value.
    #[must_use]
    pub fn null() -> Json {
        Json::Null
    }

    /// The JSON type name reported by `JSON.TYPE`.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            Json::Null => "null",
            Json::Bool(_) => "boolean",
            Json::Int(_) | Json::Uint(_) | Json::Double(_) => "number",
            Json::String(_) => "string",
            Json::Array(_) => "array",
            Json::Object(_) => "object",
        }
    }

    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Json::Null)
    }

    #[must_use]
    pub fn is_bool(&self) -> bool {
        matches!(self, Json::Bool(_))
    }

    #[must_use]
    pub fn is_number(&self) -> bool {
        matches!(self, Json::Int(_) | Json::Uint(_) | Json::Double(_))
    }

    #[must_use]
    pub fn is_int64(&self) -> bool {
        matches!(self, Json::Int(_))
    }

    #[must_use]
    pub fn is_uint64(&self) -> bool {
        matches!(self, Json::Uint(_))
    }

    #[must_use]
    pub fn is_double(&self) -> bool {
        matches!(self, Json::Double(_))
    }

    #[must_use]
    pub fn is_string(&self) -> bool {
        matches!(self, Json::String(_))
    }

    #[must_use]
    pub fn is_array(&self) -> bool {
        matches!(self, Json::Array(_))
    }

    #[must_use]
    pub fn is_object(&self) -> bool {
        matches!(self, Json::Object(_))
    }

    #[must_use]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Json::Bool(b) => Some(*b),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Json::Int(i) => Some(*i),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Json::Uint(u) => Some(*u),
            _ => None,
        }
    }

    /// Lossy floating-point view of a number (`JSON.NUMINCRBY` mixes integer
    /// and double arithmetic through this path).
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Json::Int(i) => Some(*i as f64),
            Json::Uint(u) => Some(*u as f64),
            Json::Double(d) => Some(*d),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::String(s) => Some(s),
            _ => None,
        }
    }

    /// The number of elements of an array or members of an object.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Json::Array(items) => items.len(),
            Json::Object(members) => members.len(),
            _ => 0,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate heap memory used by the value, mirroring the reference
    /// `ComputeMemorySize` (`core_json_json_object.cc`): trivial storage
    /// (null, bool, integer, double, short strings) reports 0, while arrays,
    /// objects, and long strings report their heap usage.
    #[must_use]
    pub fn memory_usage(&self) -> usize {
        const SSO: usize = 15;
        let mut total = 0usize;
        let mut stack = vec![self];
        while let Some(cur) = stack.pop() {
            match cur {
                Json::Null | Json::Bool(_) | Json::Int(_) | Json::Uint(_) | Json::Double(_) => {}
                Json::String(s) => {
                    if s.len() > SSO {
                        total += s.len() + 1;
                    }
                }
                Json::Array(items) => {
                    if !items.is_empty() {
                        total += items.len() * std::mem::size_of::<Json>() + 24;
                    }
                    stack.extend(items.iter());
                }
                Json::Object(members) => {
                    if !members.is_empty() {
                        total += members.len() * std::mem::size_of::<(String, Json)>() + 24;
                    }
                    for (key, value) in members {
                        if key.len() > SSO {
                            total += key.len() + 1;
                        }
                        stack.push(value);
                    }
                }
            }
        }
        total
    }

    #[must_use]
    pub fn array_items(&self) -> &[Json] {
        match self {
            Json::Array(items) => items,
            _ => &[],
        }
    }

    pub fn array_items_mut(&mut self) -> &mut Vec<Json> {
        match self {
            Json::Array(items) => items,
            _ => unreachable!("array_items_mut called on a non-array"),
        }
    }

    #[must_use]
    pub fn object_members(&self) -> &[(String, Json)] {
        match self {
            Json::Object(members) => members,
            _ => &[],
        }
    }

    pub fn object_members_mut(&mut self) -> &mut Vec<(String, Json)> {
        match self {
            Json::Object(members) => members,
            _ => unreachable!("object_members_mut called on a non-object"),
        }
    }

    /// Look up an object member by key (binary search over the sorted
    /// members). Returns `None` for non-objects and missing keys.
    #[must_use]
    pub fn object_get(&self, key: &str) -> Option<&Json> {
        let members = self.object_members();
        members
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|i| &members[i].1)
    }

    pub fn object_get_mut(&mut self, key: &str) -> Option<&mut Json> {
        let members = self.object_members_mut();
        let idx = members
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()?;
        Some(&mut members[idx].1)
    }

    #[must_use]
    pub fn object_contains_key(&self, key: &str) -> bool {
        self.object_get(key).is_some()
    }

    /// Insert or replace an object member, keeping the members sorted.
    /// Returns `true` when a member was added or replaced (non-objects are a
    /// no-op returning `false`).
    pub fn object_insert(&mut self, key: String, value: Json) -> bool {
        let Json::Object(members) = self else {
            return false;
        };
        match members.binary_search_by(|(k, _)| k.as_str().cmp(key.as_str())) {
            Ok(i) => members[i].1 = value,
            Err(i) => members.insert(i, (key, value)),
        }
        true
    }

    /// Remove an object member. Returns `true` when a member was removed.
    pub fn object_remove(&mut self, key: &str) -> bool {
        let Json::Object(members) = self else {
            return false;
        };
        match members.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
            Ok(i) => {
                members.remove(i);
                true
            }
            Err(_) => false,
        }
    }

    /// Apply an RFC 7386 JSON merge patch in place (mirrors
    /// `jsoncons::mergepatch::apply_merge_patch`, used by `JSON.MERGE`):
    /// a null patch replaces the target, a non-object patch replaces the
    /// target outright, and an object patch is merged recursively with null
    /// members removing keys.
    pub fn apply_merge_patch(&mut self, patch: &Json) {
        if patch.is_null() {
            *self = Json::Null;
            return;
        }
        if !patch.is_object() {
            *self = patch.clone();
            return;
        }
        if !self.is_object() {
            *self = Json::Object(Vec::new());
        }
        for (key, value) in patch.object_members() {
            if value.is_null() {
                self.object_remove(key);
            } else if let Some(node) = self.object_get_mut(key) {
                node.apply_merge_patch(value);
            } else {
                self.object_insert(key.clone(), value.clone());
            }
        }
    }

    /// Serialize the value as compact JSON, matching `jsoncons::json::dump`
    /// with default options (sorted keys, `.0`-suffixed integral doubles).
    #[must_use]
    pub fn dump(&self) -> String {
        let mut out = String::with_capacity(64);
        self.write(&mut out, 0, &Format::default());
        out
    }

    /// Serialize with the `JSON.GET` formatting options. `indent`, `newline`
    /// and `space` are the literal strings supplied via `INDENT`, `NEWLINE`
    /// and `SPACE` (empty strings reproduce the compact form).
    #[must_use]
    pub fn dump_with_options(&self, indent: &str, newline: &str, space: &str) -> String {
        let mut out = String::with_capacity(64);
        let fmt = Format::Pretty {
            indent: indent.into(),
            newline: newline.into(),
            space: space.into(),
        };
        self.write(&mut out, 0, &fmt);
        out
    }

    fn write(&self, out: &mut String, depth: usize, fmt: &Format) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(true) => out.push_str("true"),
            Json::Bool(false) => out.push_str("false"),
            Json::Int(i) => out.push_str(&i.to_string()),
            Json::Uint(u) => out.push_str(&u.to_string()),
            Json::Double(d) => write_double(out, *d),
            Json::String(s) => write_string(out, s),
            Json::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    fmt.line(out, depth + 1);
                    item.write(out, depth + 1, fmt);
                }
                if !items.is_empty() {
                    fmt.line(out, depth);
                }
                out.push(']');
            }
            Json::Object(members) => {
                out.push('{');
                for (i, (key, value)) in members.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    fmt.line(out, depth + 1);
                    write_string(out, key);
                    out.push(':');
                    fmt.space(out);
                    value.write(out, depth + 1, fmt);
                }
                if !members.is_empty() {
                    fmt.line(out, depth);
                }
                out.push('}');
            }
        }
    }
}

impl std::fmt::Display for Json {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.dump())
    }
}

/// Serialization formatting: the pretty options of `JSON.GET`, defaulting to
/// empty strings for the compact form.
#[derive(Clone, Default)]
enum Format {
    #[default]
    Compact,
    Pretty {
        indent: String,
        newline: String,
        space: String,
    },
}

impl Format {
    fn line(&self, out: &mut String, depth: usize) {
        if let Format::Pretty {
            indent, newline, ..
        } = self
            && !newline.is_empty()
        {
            out.push_str(newline);
            for _ in 0..depth {
                out.push_str(indent);
            }
        }
    }

    fn space(&self, out: &mut String) {
        if let Format::Pretty { space, .. } = self {
            out.push_str(space);
        }
    }
}

fn json_from_owned(value: OwnedValue) -> Json {
    match value {
        OwnedValue::Static(StaticNode::Null) => Json::Null,
        OwnedValue::Static(StaticNode::Bool(b)) => Json::Bool(b),
        OwnedValue::Static(StaticNode::I64(i)) => Json::Int(i),
        // simd-json stores non-negative integers as u64; jsoncons stores them
        // as int64 whenever they fit, so downcast to keep type identity (and
        // the strict `JsonAreEquals` type check) aligned.
        OwnedValue::Static(StaticNode::U64(u)) => {
            if let Ok(i) = i64::try_from(u) {
                Json::Int(i)
            } else {
                Json::Uint(u)
            }
        }
        OwnedValue::Static(StaticNode::F64(f)) => Json::Double(f),
        OwnedValue::String(s) => Json::String(s),
        OwnedValue::Array(items) => Json::Array(items.into_iter().map(json_from_owned).collect()),
        OwnedValue::Object(obj) => {
            let mut members: Vec<(String, Json)> = obj
                .into_iter()
                .map(|(k, v)| (k, json_from_owned(v)))
                .collect();
            members.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            Json::Object(members)
        }
    }
}

/// Reject documents whose nesting depth exceeds `min(64, len / 2)` without
/// going through the (unbounded) parser, mirroring the reference
/// `max_nesting_depth` check that fires during parse.
fn check_depth(input: &[u8]) -> Result<(), JsonError> {
    let limit = MAX_NESTING_DEPTH.min(input.len() / 2);
    let mut depth = 0usize;
    let mut in_string = false;
    let mut i = 0;
    while i < input.len() {
        let byte = input[i];
        if in_string {
            if byte == b'\\' {
                i = i.saturating_add(2);
            } else {
                i += 1;
                if byte == b'"' {
                    in_string = false;
                }
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > limit {
                    return Err(JsonError);
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
        i += 1;
    }
    Ok(())
}

fn write_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{0009}' => out.push_str("\\t"),
            '\u{000A}' => out.push_str("\\n"),
            '\u{000C}' => out.push_str("\\f"),
            '\u{000D}' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => {
                out.push_str("\\u00");
                out.push(hex_digit(((c as u32) >> 4) & 0xF));
                out.push(hex_digit((c as u32) & 0xF));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn hex_digit(d: u32) -> char {
    match d {
        0..=9 => (b'0' + d as u8) as char,
        _ => (b'a' + (d - 10) as u8) as char,
    }
}

/// Render a finite double the way jsoncons's grisu3-based `write_double` does.
///
/// The shortest significant digits are taken from Rust's `{:e}` formatting
/// (also a Grisu3 shortest representation), then presented with jsoncons's
/// `prettify_string` thresholds: `min_exp = -4`, `max_exp = 15`. Integral
/// values therefore gain a trailing `.0`, and out-of-range values use a
/// signed, zero-padded exponent (`1.7e+308`, `1e-05`).
fn write_double(out: &mut String, value: f64) {
    debug_assert!(value.is_finite(), "JSON cannot hold non-finite doubles");
    if value == 0.0 {
        out.push_str("0.0");
        return;
    }
    let sci = format!("{value:e}");
    let negative = sci.starts_with('-');
    let sci = if negative { &sci[1..] } else { &sci[..] };
    let (mantissa, exp) = sci.split_once('e').expect("scientific format");
    let exp: i64 = exp.parse().expect("scientific exponent");

    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let nb = digits.len() as i64;
    // `value = digits * 10^k` with `kk = nb + k` giving the decimal-point
    // position (jsoncons's `prettify_string` parameterization).
    let kk = nb + (exp - (nb - 1));

    if negative {
        out.push('-');
    }
    prettify_string(out, digits.as_bytes(), nb, kk);
}

fn prettify_string(out: &mut String, digits: &[u8], nb: i64, kk: i64) {
    const MIN_EXP: i64 = -4;
    const MAX_EXP: i64 = 15;
    if nb <= kk && kk <= MAX_EXP {
        // Integral in fixed notation: digits, zero padding, then a forced `.0`.
        out.push_str(std::str::from_utf8(digits).expect("ascii digits"));
        for _ in nb..kk {
            out.push('0');
        }
        out.push_str(".0");
    } else if (0 < kk) && kk <= MAX_EXP {
        // Fixed notation with a fractional part.
        out.push_str(std::str::from_utf8(&digits[..kk as usize]).expect("ascii digits"));
        out.push('.');
        out.push_str(std::str::from_utf8(&digits[kk as usize..]).expect("ascii digits"));
    } else if (MIN_EXP < kk) && kk <= 0 {
        // `0.000...digits` with `2 - kk` leading zeros.
        out.push_str("0.");
        let offset = 2 - kk;
        for _ in 2..offset {
            out.push('0');
        }
        out.push_str(std::str::from_utf8(digits).expect("ascii digits"));
    } else if nb == 1 {
        out.push(digits[0] as char);
        out.push('e');
        fill_exponent(out, kk - 1);
    } else {
        out.push(digits[0] as char);
        out.push('.');
        out.push_str(std::str::from_utf8(&digits[1..]).expect("ascii digits"));
        out.push('e');
        fill_exponent(out, kk - 1);
    }
}

/// Signed, zero-padded exponent, matching `sprintf("%e")` conventions
/// (`e+308`, `e-05`).
fn fill_exponent(out: &mut String, exponent: i64) {
    if exponent < 0 {
        out.push('-');
    } else {
        out.push('+');
    }
    let magnitude = exponent.unsigned_abs();
    if magnitude < 10 {
        out.push('0');
        out.push((b'0' + magnitude as u8) as char);
    } else if magnitude < 100 {
        out.push((b'0' + (magnitude / 10) as u8) as char);
        out.push((b'0' + (magnitude % 10) as u8) as char);
    } else if magnitude < 1000 {
        out.push((b'0' + (magnitude / 100) as u8) as char);
        out.push((b'0' + (magnitude / 10 % 10) as u8) as char);
        out.push((b'0' + (magnitude % 10) as u8) as char);
    } else {
        out.push_str(&magnitude.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(input: &str) -> Json {
        Json::parse(input.as_bytes()).expect("valid JSON")
    }

    #[test]
    fn parse_and_dump_round_trip() {
        let json = parse_ok(r#"{"a":"first","b":[1,2.5,true,null,"x\ny"],"c":{"d":-3}}"#);
        assert_eq!(
            json.dump(),
            r#"{"a":"first","b":[1,2.5,true,null,"x\ny"],"c":{"d":-3}}"#
        );
    }

    #[test]
    fn scalars_parse() {
        assert_eq!(parse_ok("null"), Json::Null);
        assert_eq!(parse_ok("true"), Json::Bool(true));
        assert_eq!(parse_ok("false"), Json::Bool(false));
        assert_eq!(parse_ok("42"), Json::Int(42));
        assert_eq!(parse_ok("-7"), Json::Int(-7));
        assert_eq!(parse_ok("18446744073709551615"), Json::Uint(u64::MAX));
        assert_eq!(parse_ok("\"hi\""), Json::String("hi".into()));
        assert_eq!(parse_ok("42").dump(), "42");
    }

    #[test]
    fn parse_rejects_malformed() {
        assert!(Json::parse(b"{").is_err());
        assert!(Json::parse(b"{invalid}").is_err());
        assert!(Json::parse(b"[1,]").is_err());
        assert!(Json::parse(b"{\"a\":1} extra").is_err());
        assert!(Json::parse(b"1e999").is_err());
        assert!(Json::parse(b"18446744073709551616").is_err());
    }

    #[test]
    fn depth_limit_is_enforced() {
        let doc = format!("{}42{}", "[".repeat(70), "]".repeat(70));
        assert!(Json::parse(doc.as_bytes()).is_err());
        // 64 levels deep is allowed.
        let doc = format!("{}42{}", "[".repeat(64), "]".repeat(64));
        assert!(Json::parse(doc.as_bytes()).is_ok());
    }

    #[test]
    fn depth_scan_ignores_strings() {
        let json = parse_ok(r#"{"a":"{[{","b":[1,2]}"#);
        assert_eq!(json.dump(), r#"{"a":"{[{","b":[1,2]}"#);
    }

    #[test]
    fn duplicate_keys_last_wins() {
        let json = parse_ok(r#"{"a":1,"a":2}"#);
        assert_eq!(json.dump(), r#"{"a":2}"#);
    }

    #[test]
    fn objects_are_sorted_by_key() {
        let json = parse_ok(r#"{"z":1,"a":{"f":1,"b":2},"m":[3]}"#);
        assert_eq!(json.dump(), r#"{"a":{"b":2,"f":1},"m":[3],"z":1}"#);
    }

    #[test]
    fn type_names_match_json_type() {
        let cases = [
            ("null", "null"),
            ("true", "boolean"),
            ("1", "number"),
            ("1.5", "number"),
            ("\"s\"", "string"),
            ("[]", "array"),
            ("{}", "object"),
        ];
        for (input, expected) in cases {
            assert_eq!(parse_ok(input).type_name(), expected);
        }
    }

    #[test]
    fn integral_doubles_get_trailing_zero() {
        assert_eq!(parse_ok("3.0").dump(), "3.0");
        assert_eq!(parse_ok("16.0").dump(), "16.0");
        assert_eq!(parse_ok("135.25").dump(), "135.25");
        assert_eq!(parse_ok("100.0").dump(), "100.0");
        assert_eq!(parse_ok("0.0").dump(), "0.0");
        assert_eq!(parse_ok("12340000000.0").dump(), "12340000000.0");
        assert_eq!(parse_ok("0.001").dump(), "0.001");
        assert_eq!(parse_ok("0.5").dump(), "0.5");
    }

    #[test]
    fn doubles_use_scientific_notation() {
        assert_eq!(parse_ok("1.7e308").dump(), "1.7e+308");
        assert_eq!(parse_ok("1e-5").dump(), "1e-05");
        assert_eq!(parse_ok("1e20").dump(), "1e+20");
        assert_eq!(parse_ok("-1.5e2").dump(), "-150.0");
        assert_eq!(parse_ok("2.1").dump(), "2.1");
        assert_eq!(parse_ok("2.5").dump(), "2.5");
    }

    #[test]
    fn surrounding_whitespace_is_accepted() {
        assert_eq!(parse_ok("  {\"a\": 1} \n").dump(), r#"{"a":1}"#);
        assert_eq!(parse_ok(" 42 "), Json::Int(42));
        assert_eq!(parse_ok("\"s\" "), Json::String("s".into()));
    }

    #[test]
    fn string_escaping() {
        let json = parse_ok(r#""a\"b\\c\td\nf\r\b\u0001""#);
        assert_eq!(json.dump(), r#""a\"b\\c\td\nf\r\b\u0001""#);
    }

    #[test]
    fn pretty_dump_with_options() {
        let json = parse_ok(r#"{"a":[27],"b":"x"}"#);
        assert_eq!(
            json.dump_with_options("indent", "newline", "space"),
            "{newlineindent\"a\":space[newlineindentindent27newlineindent],newlineindent\"b\":space\"x\"newline}"
        );
    }

    #[test]
    fn pretty_space_only_stays_on_one_line() {
        let json = parse_ok(r#"{"city":"New York","state":"NY"}"#);
        assert_eq!(
            json.dump_with_options("", "", "space"),
            r#"{"city":space"New York","state":space"NY"}"#
        );
    }

    #[test]
    fn object_member_operations() {
        let mut json = parse_ok(r#"{"b":1,"d":2}"#);
        assert!(json.object_insert("c".into(), Json::Int(3)));
        assert!(json.object_insert("a".into(), Json::Int(0)));
        assert!(json.object_insert("b".into(), Json::Int(10)));
        assert_eq!(json.dump(), r#"{"a":0,"b":10,"c":3,"d":2}"#);
        assert_eq!(json.object_get("c"), Some(&Json::Int(3)));
        assert_eq!(json.object_get("nope"), None);
        assert!(json.object_remove("b"));
        assert!(!json.object_remove("b"));
        assert_eq!(json.dump(), r#"{"a":0,"c":3,"d":2}"#);
        // Non-objects are inert.
        let mut non_obj = Json::Int(1);
        assert!(!non_obj.object_insert("a".into(), Json::Null));
        assert!(!non_obj.object_remove("a"));
        assert_eq!(non_obj.object_get("a"), None);
    }

    #[test]
    fn merge_patch_follows_rfc_7386() {
        let mut target = parse_ok(r#"{"a":{"b":1,"c":2},"d":[1,2],"e":"x"}"#);
        let patch = parse_ok(r#"{"a":{"b":10,"c":null},"d":[3],"f":true,"e":null}"#);
        target.apply_merge_patch(&patch);
        assert_eq!(target.dump(), r#"{"a":{"b":10},"d":[3],"f":true}"#);

        // A non-object patch replaces the target outright.
        let mut target = parse_ok(r#"{"a":1}"#);
        target.apply_merge_patch(&Json::Int(5));
        assert_eq!(target, Json::Int(5));

        // A null patch replaces the target with null.
        let mut target = parse_ok(r#"{"a":1}"#);
        target.apply_merge_patch(&Json::Null);
        assert_eq!(target, Json::Null);

        // An object patch applied to a non-object target starts from `{}`.
        let mut target = Json::Int(1);
        target.apply_merge_patch(&parse_ok(r#"{"a":2}"#));
        assert_eq!(target.dump(), r#"{"a":2}"#);
    }

    #[test]
    fn strict_equality_between_types() {
        assert_ne!(Json::Int(1), Json::Double(1.0));
        assert_ne!(Json::Int(1), Json::Uint(1));
        assert_eq!(Json::Int(1), Json::Int(1));
        assert_eq!(
            parse_ok(r#"{"a":[1,{"b":null}]}"#),
            parse_ok(r#"{"a":[1,{"b":null}]}"#)
        );
    }

    #[test]
    fn deep_document_round_trips() {
        let doc = format!("{}42{}", "[".repeat(32), "]".repeat(32));
        assert_eq!(Json::parse(doc.as_bytes()).unwrap().dump(), doc);
    }
}
