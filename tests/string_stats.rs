//! Port of `dragonfly/src/server/string_stats_test.cc` (`DEBUG UNIQ-STRS`).
//!
//! Adaptations from the C++ original:
//! - `GetValue`/`ParseStats` are ported verbatim: whitespace-trimmed rows,
//!   the value after the first `:`, and a trailing `" bytes"` suffix removed
//!   before parsing. `SkipWhitespace` drops blank rows.
//! - The reference's HLL estimate bounds are kept as-is; the port uses the
//!   same dense HLL, so the `[2, 5]` windows for 3 distinct values and the
//!   `±3` window for 30 distinct values still hold after the 2-shard merge.

mod common;

use common::*;

#[derive(Debug)]
struct ParsedBucket {
    total_strings: u64,
    unique_strings: u64,
    total_bytes: u64,
    average_length: f64,
    estimated_savings: u64,
}

/// `GetValue` + `ParseStats` (string_stats_test.cc:18, 40).
fn parse_stats(output: &str) -> Option<ParsedBucket> {
    let rows: Vec<&str> = output
        .lines()
        .map(str::trim)
        .filter(|r| !r.is_empty())
        .collect();
    let start = rows.iter().position(|r| r.starts_with("Strings"))?;
    let value = |row: &str| -> String {
        let v = row.split_once(':').map_or("", |(_, v)| v.trim());
        v.strip_suffix(" bytes").unwrap_or(v).to_owned()
    };
    let mut it = rows[start + 1..].iter();
    Some(ParsedBucket {
        total_strings: value(it.next()?).parse().ok()?,
        unique_strings: value(it.next()?).parse().ok()?,
        total_bytes: value(it.next()?).parse().ok()?,
        average_length: value(it.next()?).parse().ok()?,
        estimated_savings: value(it.next()?).parse().ok()?,
    })
}

/// `HashWithDuplicateFields` (string_stats_test.cc:67).
#[test]
fn hash_with_duplicate_fields() {
    let mut c = Ctx::new();
    for i in 0..100 {
        c.int(&[
            "HSET",
            &format!("user:{i}"),
            "name",
            &format!("name_{i}"),
            "email",
            &format!("email_{i}"),
            "age",
            &format!("{}", 20 + i),
        ]);
    }

    let output = c.text(&["DEBUG", "UNIQ-STRS"]);
    assert!(output.contains("hash"));

    let bucket = parse_stats(&output).expect("stats block");
    assert_eq!(bucket.total_strings, 300);
    assert!((2..=5).contains(&bucket.unique_strings), "{bucket:?}");
    assert!(bucket.estimated_savings > 0);
}

/// `SetWithUniqueMembers` (string_stats_test.cc:86).
#[test]
fn set_with_unique_members() {
    let mut c = Ctx::new();
    for i in 0..10 {
        c.int(&[
            "SADD",
            &format!("set:{i}"),
            &format!("unique_member_{i}_a"),
            &format!("unique_member_{i}_b"),
            &format!("unique_member_{i}_c"),
        ]);
    }

    let output = c.text(&["DEBUG", "UNIQ-STRS"]);
    let bucket = parse_stats(&output).expect("stats block");
    assert_eq!(bucket.total_strings, 30);
    assert!(
        (bucket.unique_strings as f64 - 30.0).abs() <= 3.0,
        "{bucket:?}"
    );
    assert!((16.0..=18.0).contains(&bucket.average_length), "{bucket:?}");
    assert!((bucket.estimated_savings as f64) <= (bucket.total_bytes as f64 * 0.15));
}

/// `SetWithDuplicateMembers` (string_stats_test.cc:102).
#[test]
fn set_with_duplicate_members() {
    let mut c = Ctx::new();
    for i in 0..50 {
        c.int(&["SADD", &format!("set:{i}"), "alpha", "beta", "gamma"]);
    }

    let output = c.text(&["DEBUG", "UNIQ-STRS"]);
    let bucket = parse_stats(&output).expect("stats block");
    assert_eq!(bucket.total_strings, 150);
    assert!((2..=5).contains(&bucket.unique_strings), "{bucket:?}");
    assert!(bucket.estimated_savings > 0);
}

/// `MultipleTypes` (string_stats_test.cc:118).
#[test]
fn multiple_types() {
    let mut c = Ctx::new();
    for i in 0..10 {
        c.int(&["HSET", &format!("h:{i}"), "field", "value"]);
        c.int(&["SADD", &format!("s:{i}"), "member"]);
    }

    let output = c.text(&["DEBUG", "UNIQ-STRS"]);
    assert!(output.contains("hash"));
    assert!(output.contains("set"));
}

/// `EmptyDatabase` (string_stats_test.cc:131).
#[test]
fn empty_database() {
    let mut c = Ctx::new();
    let output = c.text(&["DEBUG", "UNIQ-STRS"]);
    assert!(output.contains("___begin unique string stats___"));
    assert!(output.contains("___end unique string stats___"));
    assert!(parse_stats(&output).is_none());
}

/// `NumberKeys` (string_stats_test.cc:142): int-like list entries are counted
/// by their decimal rendering, deduplicating across keys.
#[test]
fn number_keys() {
    let mut c = Ctx::new();
    for i in 0..100 {
        c.int(&["LPUSH", &format!("h:{i}"), "007", "value"]);
    }

    let output = c.text(&["DEBUG", "UNIQ-STRS"]);
    assert!(output.contains("list"));
    let bucket = parse_stats(&output).expect("stats block");
    assert_eq!(bucket.total_strings, 200);
    assert_eq!(bucket.unique_strings, 2);
}
