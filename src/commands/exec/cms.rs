//! Count-min sketch commands (CMS.INITBYDIM/INITBYPROB/INCRBY/QUERY/INFO/
//! MERGE), ported from `dragonfly/src/server/cms_family.cc`.
//!
//! The CMS family is a Rust `PrimeValue::Cms` backed by `core::cms::Cms`.
//! CMS.MERGE is multi-key: like the reference, each shard reports the sources
//! it owns plus the destination status, and `merge_cms_merge` combines them
//! into a `DeferredStore`.

use crate::commands::{
    integer, ok, Command, OpContext, ShardPart, KeyRange, FLAG_DENYOOM, FLAG_FAST, FLAG_MULTI_KEY,
    FLAG_READONLY, FLAG_WRITE,
};
use crate::core::cms::Cms;
use crate::core::compact::CompactString;
use crate::core::PrimeValue;
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::{parse_double, parse_i64, parse_u64};

/// kMaxCmsWidth / kMaxCmsDepth: upper bounds validated by the command layer.
const K_MAX_CMS_WIDTH: u32 = 1_000_000;
const K_MAX_CMS_DEPTH: u32 = 100;

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

fn cms_not_found() -> RespError {
    RespError::new("ERR CMS: key does not exist")
}

fn cms_wrong_num_keys() -> RespError {
    RespError::new("ERR CMS: wrong number of keys")
}

fn cms_wrong_num_keys_weights() -> RespError {
    RespError::new("ERR CMS: wrong number of keys/weights")
}

fn cms_cannot_parse_number() -> RespError {
    RespError::new("ERR CMS: Cannot parse number")
}

fn cms_positive_increment() -> RespError {
    RespError::new("ERR CMS: increment must be a positive integer")
}

fn cms_err_range() -> RespError {
    RespError::new("ERR CMS: error must be between 0 and 1 exclusive")
}

fn cms_prob_range() -> RespError {
    RespError::new("ERR CMS: probability must be between 0 and 1 exclusive")
}

fn cms_dim_zero() -> RespError {
    RespError::new("ERR CMS: width and depth must be greater than 0")
}

fn cms_dim_too_large() -> RespError {
    RespError::new(format!(
        "ERR CMS: width must not exceed {} and depth must not exceed {}",
        K_MAX_CMS_WIDTH, K_MAX_CMS_DEPTH
    ))
}

fn cms_invalid_error_probability() -> RespError {
    RespError::new("ERR CMS: invalid error/probability")
}

fn cms_dimension_mismatch() -> RespError {
    RespError::new("ERR CMS: dimension mismatch")
}

fn item_exists() -> RespError {
    RespError::new("ERR item exists")
}

// ---------------------------------------------------------------------------
// CMS.INITBYDIM / CMS.INITBYPROB
// ---------------------------------------------------------------------------

/// Port of `ValidateCmsDimensions`: rejects zero and oversized dimensions.
fn validate_cms_dimensions(width: u32, depth: u32) -> Result<(), RespError> {
    if width == 0 || depth == 0 {
        return Err(cms_dim_zero());
    }
    if width > K_MAX_CMS_WIDTH || depth > K_MAX_CMS_DEPTH {
        return Err(cms_dim_too_large());
    }
    Ok(())
}

/// Port of `ComputeCmsDimensions`: `width = ceil(e / error)`,
/// `depth = ceil(ln(1 / probability))`, then validated.
fn compute_cms_dimensions(error: f64, probability: f64) -> Result<(u32, u32), RespError> {
    let computed_width = (std::f64::consts::E / error).ceil();
    let computed_depth = (1.0 / probability).ln().ceil();
    if !computed_width.is_finite()
        || !computed_depth.is_finite()
        || computed_width <= 0.0
        || computed_depth <= 0.0
        || computed_width > u32::MAX as f64
        || computed_depth > u32::MAX as f64
    {
        return Err(cms_invalid_error_probability());
    }
    let (width, depth) = (computed_width as u32, computed_depth as u32);
    validate_cms_dimensions(width, depth)?;
    Ok((width, depth))
}

fn exec_cms_initbydim(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let width = match parse_u64(&ctx.args[key_idx + 1]).and_then(|v| u32::try_from(v).ok()) {
        Some(w) => w,
        None => return CmdResult::Err(RespError::syntax()),
    };
    let depth = match parse_u64(&ctx.args[key_idx + 2]).and_then(|v| u32::try_from(v).ok()) {
        Some(d) => d,
        None => return CmdResult::Err(RespError::syntax()),
    };
    if let Err(e) = validate_cms_dimensions(width, depth) {
        return CmdResult::Err(e);
    }
    if ctx.db.contains(key, ctx.now_ms) {
        return CmdResult::Err(item_exists());
    }
    ctx.db.insert(
        CompactString::from_bytes(key),
        PrimeValue::Cms(Cms::new(width, depth)),
    );
    CmdResult::Ok(ok())
}

fn exec_cms_initbyprob(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let error = match parse_double(&ctx.args[key_idx + 1]) {
        Some(e) => e,
        None => return CmdResult::Err(RespError::syntax()),
    };
    let probability = match parse_double(&ctx.args[key_idx + 2]) {
        Some(p) => p,
        None => return CmdResult::Err(RespError::syntax()),
    };
    if !(error > 0.0 && error < 1.0) {
        return CmdResult::Err(cms_err_range());
    }
    if !(probability > 0.0 && probability < 1.0) {
        return CmdResult::Err(cms_prob_range());
    }
    let (width, depth) = match compute_cms_dimensions(error, probability) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    if ctx.db.contains(key, ctx.now_ms) {
        return CmdResult::Err(item_exists());
    }
    ctx.db.insert(
        CompactString::from_bytes(key),
        PrimeValue::Cms(Cms::new(width, depth)),
    );
    CmdResult::Ok(ok())
}

// ---------------------------------------------------------------------------
// CMS.INCRBY / CMS.QUERY
// ---------------------------------------------------------------------------

/// Parse `<item> <increment>` pairs following the key. Returns `Err` on a
/// non-integer increment, a non-positive increment, or an odd trailing
/// argument (mirrors the C++ pair parsing in `CmdIncrBy`).
fn parse_incr_items(args: &[Vec<u8>], key_idx: usize) -> Result<Vec<(Vec<u8>, i64)>, RespError> {
    let mut items = Vec::new();
    let mut i = key_idx + 1;
    while i < args.len() {
        if i + 1 >= args.len() {
            return Err(RespError::syntax());
        }
        let item = args[i].clone();
        let incr = match parse_i64(&args[i + 1]) {
            Some(v) => v,
            None => return Err(cms_cannot_parse_number()),
        };
        if incr <= 0 {
            return Err(cms_positive_increment());
        }
        items.push((item, incr));
        i += 2;
    }
    Ok(items)
}

fn exec_cms_incrby(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let items = match parse_incr_items(ctx.args, key_idx) {
        Ok(items) => items,
        Err(e) => return CmdResult::Err(e),
    };
    let cms = match ctx.db.find_mut(&ctx.args[key_idx], ctx.now_ms) {
        Some(PrimeValue::Cms(c)) => c,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Err(cms_not_found()),
    };
    let results: Vec<i64> = items.iter().map(|(it, inc)| cms.incr_by(it, *inc)).collect();
    CmdResult::Ok(RespValue::Array(results.into_iter().map(integer).collect()))
}

fn exec_cms_query(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let items = &ctx.args[key_idx + 1..];
    let cms = match ctx.db.find(&ctx.args[key_idx], ctx.now_ms) {
        Some(PrimeValue::Cms(c)) => c,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Err(cms_not_found()),
    };
    let results: Vec<i64> = items.iter().map(|it| cms.query(it)).collect();
    CmdResult::Ok(RespValue::Array(results.into_iter().map(integer).collect()))
}

// ---------------------------------------------------------------------------
// CMS.INFO
// ---------------------------------------------------------------------------

fn exec_cms_info(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let cms = match ctx.db.find(&ctx.args[key_idx], ctx.now_ms) {
        Some(PrimeValue::Cms(c)) => c,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Err(cms_not_found()),
    };
    CmdResult::Ok(RespValue::Array(vec![
        RespValue::Bulk(b"width".to_vec()),
        integer(cms.width() as i64),
        RespValue::Bulk(b"depth".to_vec()),
        integer(cms.depth() as i64),
        RespValue::Bulk(b"count".to_vec()),
        integer(cms.total_count()),
    ]))
}

// ---------------------------------------------------------------------------
// CMS.MERGE
// ---------------------------------------------------------------------------

/// Parsed `CMS.MERGE` arguments.
struct MergeArgs {
    numkeys: usize,
    weights: Vec<i64>,
}

/// Port of `ParseMergeArgs`: `dest numkeys key... [WEIGHTS weight...]`. The
/// destination is at args[1] and `numkeys` at args[2].
fn parse_merge_args(args: &[Vec<u8>]) -> Result<MergeArgs, RespError> {
    let numkeys = match parse_u64(&args[2]) {
        Some(n) if n >= 1 => n as usize,
        _ => return Err(cms_wrong_num_keys()),
    };
    let src_start = 3;
    if src_start + numkeys > args.len() {
        return Err(RespError::syntax());
    }
    let mut weights = vec![1i64; numkeys];
    let after = src_start + numkeys;
    if after < args.len() {
        if !args[after].eq_ignore_ascii_case(b"WEIGHTS") {
            return Err(cms_wrong_num_keys_weights());
        }
        let weight_args = &args[after + 1..];
        if weight_args.len() != numkeys {
            return Err(cms_wrong_num_keys_weights());
        }
        for (i, w) in weight_args.iter().enumerate() {
            match parse_i64(w) {
                Some(v) => weights[i] = v,
                None => return Err(cms_cannot_parse_number()),
            }
        }
    }
    Ok(MergeArgs { numkeys, weights })
}

/// Build the merged sketch: validates that every source and the destination
/// share the same dimensions, then replaces the destination with the weighted
/// sum of the sources (the reference resets the destination before merging).
fn merge_sources(
    ctx: &mut OpContext,
    dest_idx: usize,
    dest: Cms,
    srcs: &[(usize, Cms)],
    weights: &[i64],
) -> CmdResult {
    let (ref_width, ref_depth) = match srcs.first() {
        Some((_, c)) => (c.width(), c.depth()),
        None => (dest.width(), dest.depth()),
    };
    for (_, c) in srcs {
        if c.width() != ref_width || c.depth() != ref_depth {
            return CmdResult::Err(cms_dimension_mismatch());
        }
    }
    if dest.width() != ref_width || dest.depth() != ref_depth {
        return CmdResult::Err(cms_dimension_mismatch());
    }
    let mut merged = Cms::new(ref_width, ref_depth);
    for (i, (_, c)) in srcs.iter().enumerate() {
        merged.merge_from(c, weights[i]);
    }
    ctx.db.insert(
        CompactString::from_bytes(&ctx.args[dest_idx]),
        PrimeValue::Cms(merged),
    );
    CmdResult::Ok(ok())
}

/// Executes on every participating shard. When this shard owns every key
/// (single-shard fast path) it performs the merge directly. Otherwise it
/// reports `[dest_owned, dest_present, dest_width, dest_depth, src_idx,
/// serialized_src, ...]` for the coordinator's `merge_cms_merge`.
fn exec_cms_merge(ctx: &mut OpContext) -> CmdResult {
    let dest_idx = ctx.first_key_idx;
    let parsed = match parse_merge_args(ctx.args) {
        Ok(p) => p,
        Err(e) => return CmdResult::Err(e),
    };
    let src_idxs: Vec<usize> = (dest_idx + 2..dest_idx + 2 + parsed.numkeys).collect();

    // Read the sources this shard owns.
    let mut owned_srcs: Vec<(usize, Cms)> = Vec::new();
    for (i, &src_idx) in src_idxs.iter().enumerate() {
        if !ctx.owned_keys.contains(&src_idx) {
            continue;
        }
        match ctx.db.find(&ctx.args[src_idx], ctx.now_ms) {
            Some(PrimeValue::Cms(c)) => owned_srcs.push((i, c.clone())),
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => return CmdResult::Err(cms_not_found()),
        }
    }

    // Read the destination if this shard owns it.
    let dest_owned = ctx.owned_keys.contains(&dest_idx);
    let dest = if dest_owned {
        match ctx.db.find(&ctx.args[dest_idx], ctx.now_ms) {
            Some(PrimeValue::Cms(c)) => Some(c.clone()),
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => return CmdResult::Err(cms_not_found()),
        }
    } else {
        None
    };

    // Single-shard fast path: every key is on this shard.
    if dest_owned && owned_srcs.len() == src_idxs.len() {
        let dest = dest.expect("dest owned means dest read");
        return merge_sources(ctx, dest_idx, dest, &owned_srcs, &parsed.weights);
    }

    // Partial report for the coordinator.
    let mut reply = Vec::new();
    reply.push(integer(dest_owned as i64));
    reply.push(integer(dest.is_some() as i64));
    let (dw, dd) = match &dest {
        Some(c) => (c.width(), c.depth()),
        None => (0, 0),
    };
    reply.push(integer(dw as i64));
    reply.push(integer(dd as i64));
    for (i, c) in &owned_srcs {
        reply.push(integer(*i as i64));
        reply.push(RespValue::Bulk(c.serialize()));
    }
    CmdResult::Ok(RespValue::Array(reply))
}

/// Combine per-shard source reports and store the merged sketch on the
/// destination's shard. `keys[0]` is the destination; `keys[1..]` the sources.
fn merge_cms_merge(parts: &[ShardPart], args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    let parsed = match parse_merge_args(args) {
        Ok(p) => p,
        Err(e) => return CmdResult::Err(e),
    };

    let mut dest_status: Option<(bool, u32, u32)> = None;
    let mut srcs: Vec<(usize, Cms)> = Vec::new();
    for p in parts {
        if let CmdResult::Err(e) = &p.result {
            return CmdResult::Err(e.clone());
        }
        let arr = match &p.result {
            CmdResult::Ok(RespValue::Array(arr)) => arr,
            _ => {
                return CmdResult::Err(RespError::new(
                    "ERR internal: unexpected CMS shard result",
                ))
            }
        };
        let dest_owned = matches!(&arr[0], RespValue::Integer(1));
        if dest_owned {
            let present = matches!(&arr[1], RespValue::Integer(1));
            let width = match &arr[2] {
                RespValue::Integer(v) => *v as u32,
                _ => 0,
            };
            let depth = match &arr[3] {
                RespValue::Integer(v) => *v as u32,
                _ => 0,
            };
            dest_status = Some((present, width, depth));
        }
        for pair in arr[4..].chunks_exact(2) {
            match pair {
                [RespValue::Integer(i), RespValue::Bulk(blob)] => {
                    match Cms::deserialize(blob) {
                        Some(c) => srcs.push((*i as usize, c)),
                        None => {
                            return CmdResult::Err(RespError::new(
                                "ERR internal: bad CMS shard blob",
                            ))
                        }
                    }
                }
                _ => {
                    return CmdResult::Err(RespError::new(
                        "ERR internal: unexpected CMS shard element",
                    ))
                }
            }
        }
    }

    let Some((dest_present, ref_width, ref_depth)) = dest_status else {
        return CmdResult::Err(RespError::new("ERR internal: CMS.MERGE dest shard missing"));
    };
    if !dest_present {
        return CmdResult::Err(cms_not_found());
    }
    if srcs.len() != parsed.numkeys {
        return CmdResult::Err(cms_not_found());
    }
    for (_, c) in &srcs {
        if c.width() != ref_width || c.depth() != ref_depth {
            return CmdResult::Err(cms_dimension_mismatch());
        }
    }

    let mut merged = Cms::new(ref_width, ref_depth);
    for (i, (_, c)) in srcs.iter().enumerate() {
        merged.merge_from(c, parsed.weights[i]);
    }
    let _ = keys;
    CmdResult::DeferredStore {
        key: args[keys[0]].clone(),
        value: Some(PrimeValue::Cms(merged)),
        reply: RespValue::Simple("OK".into()),
    }
}

// ---------------------------------------------------------------------------
// Command definitions
// ---------------------------------------------------------------------------

pub static CMD_CMS_INITBYDIM: Command = Command {
    name: "CMS.INITBYDIM",
    arity: 4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_cms_initbydim,
    merge: None,
};
pub static CMD_CMS_INITBYPROB: Command = Command {
    name: "CMS.INITBYPROB",
    arity: 4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_cms_initbyprob,
    merge: None,
};
pub static CMD_CMS_INCRBY: Command = Command {
    name: "CMS.INCRBY",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_cms_incrby,
    merge: None,
};
pub static CMD_CMS_QUERY: Command = Command {
    name: "CMS.QUERY",
    arity: -3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_cms_query,
    merge: None,
};
pub static CMD_CMS_INFO: Command = Command {
    name: "CMS.INFO",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_cms_info,
    merge: None,
};
pub static CMD_CMS_MERGE: Command = Command {
    name: "CMS.MERGE",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_MULTI_KEY,
    key_range: KeyRange { first: 1, last: 0, step: 1 },
    exec: exec_cms_merge,
    merge: Some(merge_cms_merge),
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db::DbSlice;

    fn dispatch(db: &mut DbSlice, argv: &[Vec<u8>]) -> CmdResult {
        let (exec, first_key_idx, owned): (fn(&mut OpContext) -> CmdResult, usize, Vec<usize>) =
            match argv[0].as_slice() {
                b"CMS.INITBYDIM" => (exec_cms_initbydim, 1, vec![1]),
                b"CMS.INITBYPROB" => (exec_cms_initbyprob, 1, vec![1]),
                b"CMS.INCRBY" => (exec_cms_incrby, 1, vec![1]),
                b"CMS.QUERY" => (exec_cms_query, 1, vec![1]),
                b"CMS.INFO" => (exec_cms_info, 1, vec![1]),
                b"CMS.MERGE" => {
                    // dest (1) + numkeys-prefixed sources (3..3+numkeys).
                    let n = parse_u64(&argv[2]).unwrap_or(0) as usize;
                    let owned: Vec<usize> = (1..=2 + n).collect();
                    (exec_cms_merge, 1, owned)
                }
                _ => panic!("unhandled command {:?}", argv[0]),
            };
        let mut ctx = OpContext { db, args: argv, owned_keys: &owned, first_key_idx, now_ms: 0 };
        exec(&mut ctx)
    }

    macro_rules! run {
        ($db:expr, $($arg:expr),+ $(,)?) => {
            dispatch($db, &[$($arg.to_vec()),+])
        };
    }

    fn err(r: CmdResult) -> String {
        match r {
            CmdResult::Err(e) => e.render().to_string(),
            o => panic!("expected error, got {:?}", o.into_resp_value()),
        }
    }

    fn ok_res(r: CmdResult) {
        match r.into_resp_value() {
            RespValue::Simple(s) => assert_eq!(s, "OK"),
            o => panic!("expected OK, got {o:?}"),
        }
    }

    fn ints(r: CmdResult) -> Vec<i64> {
        match r.into_resp_value() {
            RespValue::Array(a) => a
                .iter()
                .map(|v| match v {
                    RespValue::Integer(i) => *i,
                    o => panic!("expected integer element, got {o:?}"),
                })
                .collect(),
            o => panic!("expected array, got {o:?}"),
        }
    }

    fn type_of(db: &mut DbSlice, key: &str) -> String {
        match db.find(key.as_bytes(), 0) {
            Some(v) => v.type_name().to_string(),
            None => "none".to_string(),
        }
    }

    #[test]
    fn init_by_dim() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"cms1", b"1000", b"5"));
        assert_eq!(type_of(&mut db, "cms1"), "CMSk-TYPE");

        assert!(err(run!(&mut db, b"CMS.INITBYDIM", b"cms1", b"100", b"5"))
            .contains("item exists"));
        assert!(err(run!(&mut db, b"CMS.INITBYDIM", b"cms2", b"0", b"5"))
            .contains("width and depth must be greater than 0"));
        assert!(err(run!(&mut db, b"CMS.INITBYDIM", b"cms3", b"5", b"0"))
            .contains("width and depth must be greater than 0"));
    }

    #[test]
    fn init_by_dim_rejects_oversized_dimensions_and_preserves_state() {
        let mut db = DbSlice::new(0);
        let r = run!(&mut db, b"CMS.INITBYDIM", b"k", b"2147483648", b"1073741824");
        assert!(err(r).contains("width must not exceed"));

        let r = run!(&mut db, b"CMS.INCRBY", b"k", b"a", b"1");
        assert!(err(r).contains("CMS: key does not exist"));

        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"safe", b"100", b"5"));
        assert_eq!(ints(run!(&mut db, b"CMS.INCRBY", b"safe", b"a", b"1")), vec![1]);
        assert_eq!(ints(run!(&mut db, b"CMS.QUERY", b"safe", b"a")), vec![1]);
    }

    #[test]
    fn init_by_prob() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"CMS.INITBYPROB", b"cms1", b"0.01", b"0.01"));

        assert!(err(run!(&mut db, b"CMS.INITBYPROB", b"cms1", b"0.01", b"0.01"))
            .contains("item exists"));
        assert!(err(run!(&mut db, b"CMS.INITBYPROB", b"cms2", b"2", b"0.01"))
            .contains("error must be between 0 and 1"));
        assert!(err(run!(&mut db, b"CMS.INITBYPROB", b"cms3", b"0.01", b"0"))
            .contains("probability must be between 0 and 1"));
    }

    #[test]
    fn init_by_prob_rejects_oversized_derived_dimensions() {
        let mut db = DbSlice::new(0);
        let r = run!(&mut db, b"CMS.INITBYPROB", b"cms", b"0.000001", b"0.01");
        assert!(err(r).contains("width must not exceed"));
    }

    #[test]
    fn incr_by() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"cms", b"100", b"5"));

        assert_eq!(ints(run!(&mut db, b"CMS.INCRBY", b"cms", b"foo", b"3")), vec![3]);
        assert_eq!(
            ints(run!(&mut db, b"CMS.INCRBY", b"cms", b"foo", b"4", b"bar", b"1")),
            vec![7, 1]
        );

        assert!(err(run!(&mut db, b"CMS.INCRBY", b"noexist", b"foo", b"1"))
            .contains("CMS: key does not exist"));
        assert!(err(run!(&mut db, b"CMS.INCRBY", b"cms", b"foo", b"notanumber"))
            .contains("CMS: Cannot parse number"));
        assert!(err(run!(&mut db, b"CMS.INCRBY", b"cms", b"foo", b"0"))
            .contains("increment must be a positive integer"));
        assert!(err(run!(&mut db, b"CMS.INCRBY", b"cms", b"foo", b"1", b"bar"))
            .contains("syntax error"));
    }

    #[test]
    fn query() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"cms", b"100", b"5"));
        let _ = run!(&mut db, b"CMS.INCRBY", b"cms", b"foo", b"5", b"bar", b"3");

        assert_eq!(ints(run!(&mut db, b"CMS.QUERY", b"cms", b"foo")), vec![5]);
        assert_eq!(
            ints(run!(&mut db, b"CMS.QUERY", b"cms", b"foo", b"bar")),
            vec![5, 3]
        );
        assert_eq!(ints(run!(&mut db, b"CMS.QUERY", b"cms", b"noexist")), vec![0]);
        assert!(err(run!(&mut db, b"CMS.QUERY", b"noexist", b"foo"))
            .contains("CMS: key does not exist"));
    }

    #[test]
    fn info() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"cms", b"1000", b"5"));
        let _ = run!(&mut db, b"CMS.INCRBY", b"cms", b"foo", b"5", b"bar", b"3", b"baz", b"9");

        let r = run!(&mut db, b"CMS.INFO", b"cms");
        let arr = match r.into_resp_value() {
            RespValue::Array(a) => a,
            o => panic!("expected array, got {o:?}"),
        };
        assert_eq!(arr.len(), 6);
        assert_eq!(arr[0], RespValue::Bulk(b"width".to_vec()));
        assert_eq!(arr[1], RespValue::Integer(1000));
        assert_eq!(arr[2], RespValue::Bulk(b"depth".to_vec()));
        assert_eq!(arr[3], RespValue::Integer(5));
        assert_eq!(arr[4], RespValue::Bulk(b"count".to_vec()));
        assert_eq!(arr[5], RespValue::Integer(17));

        assert!(err(run!(&mut db, b"CMS.INFO", b"noexist")).contains("CMS: key does not exist"));
    }

    #[test]
    fn merge() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"A", b"100", b"5"));
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"B", b"100", b"5"));
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"C", b"100", b"5"));

        let _ = run!(&mut db, b"CMS.INCRBY", b"A", b"foo", b"5", b"bar", b"3", b"baz", b"9");
        let _ = run!(&mut db, b"CMS.INCRBY", b"B", b"foo", b"2", b"foobar", b"3", b"baz", b"1");

        assert_eq!(
            ints(run!(&mut db, b"CMS.QUERY", b"A", b"foo", b"bar", b"baz")),
            vec![5, 3, 9]
        );
        assert_eq!(
            ints(run!(&mut db, b"CMS.QUERY", b"B", b"foo", b"foobar", b"baz")),
            vec![2, 3, 1]
        );

        ok_res(run!(&mut db, b"CMS.MERGE", b"C", b"2", b"A", b"B"));
        assert_eq!(
            ints(run!(&mut db, b"CMS.QUERY", b"C", b"foo", b"bar", b"baz", b"foobar")),
            vec![7, 3, 10, 3]
        );

        assert!(err(run!(&mut db, b"CMS.MERGE", b"noexist", b"1", b"A"))
            .contains("CMS: key does not exist"));
        assert!(err(run!(&mut db, b"CMS.MERGE", b"C", b"0", b"A"))
            .contains("CMS: wrong number of keys"));
        assert!(err(run!(&mut db, b"CMS.MERGE", b"A", b"1", b"B", b"WEIGHTS", b"4", b"3"))
            .contains("CMS: wrong number of keys/weights"));
        assert!(err(run!(&mut db, b"CMS.MERGE", b"A", b"2", b"B", b"noexist", b"WEIGHTS", b"4", b"3"))
            .contains("CMS: key does not exist"));

        // Merge A into B: the destination is reset, so B takes A's values.
        ok_res(run!(&mut db, b"CMS.MERGE", b"B", b"1", b"A"));
        assert_eq!(ints(run!(&mut db, b"CMS.QUERY", b"B", b"foo", b"bar", b"baz")), vec![5, 3, 9]);
    }

    #[test]
    fn merge_with_weights() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"A", b"100", b"5"));
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"B", b"100", b"5"));
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"C", b"100", b"5"));

        let _ = run!(&mut db, b"CMS.INCRBY", b"A", b"foo", b"5", b"bar", b"3", b"baz", b"9");
        let _ = run!(&mut db, b"CMS.INCRBY", b"B", b"foo", b"2", b"bar", b"3", b"baz", b"1");

        ok_res(run!(&mut db, b"CMS.MERGE", b"C", b"2", b"A", b"B", b"WEIGHTS", b"2", b"3"));
        assert_eq!(
            ints(run!(&mut db, b"CMS.QUERY", b"C", b"foo", b"bar", b"baz")),
            vec![16, 15, 21]
        );
    }

    #[test]
    fn merge_with_duplicate_source_keys_preserves_weight_order() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"A", b"100", b"5"));
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"C", b"100", b"5"));

        let _ = run!(&mut db, b"CMS.INCRBY", b"A", b"foo", b"2", b"bar", b"4");

        ok_res(run!(&mut db, b"CMS.MERGE", b"C", b"2", b"A", b"A", b"WEIGHTS", b"1", b"3"));
        assert_eq!(ints(run!(&mut db, b"CMS.QUERY", b"C", b"foo", b"bar")), vec![8, 16]);

        let r = run!(&mut db, b"CMS.INFO", b"C");
        let arr = match r.into_resp_value() {
            RespValue::Array(a) => a,
            o => panic!("expected array, got {o:?}"),
        };
        assert_eq!(arr[5], RespValue::Integer(24));
    }

    #[test]
    fn info_after_merges() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"A", b"1000", b"5"));
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"B", b"1000", b"5"));
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"C", b"1000", b"5"));

        let _ = run!(&mut db, b"CMS.INCRBY", b"A", b"foo", b"5", b"bar", b"3", b"baz", b"9");
        let _ = run!(&mut db, b"CMS.INCRBY", b"B", b"foo", b"2", b"bar", b"3", b"baz", b"1");

        ok_res(run!(&mut db, b"CMS.MERGE", b"C", b"2", b"A", b"B"));
        assert_eq!(
            ints(run!(&mut db, b"CMS.QUERY", b"C", b"foo", b"bar", b"baz")),
            vec![7, 6, 10]
        );

        ok_res(run!(&mut db, b"CMS.MERGE", b"C", b"2", b"A", b"B", b"WEIGHTS", b"1", b"2"));
        assert_eq!(
            ints(run!(&mut db, b"CMS.QUERY", b"C", b"foo", b"bar", b"baz")),
            vec![9, 9, 11]
        );

        ok_res(run!(&mut db, b"CMS.MERGE", b"C", b"2", b"A", b"B", b"WEIGHTS", b"2", b"3"));
        assert_eq!(
            ints(run!(&mut db, b"CMS.QUERY", b"C", b"foo", b"bar", b"baz")),
            vec![16, 15, 21]
        );

        let r = run!(&mut db, b"CMS.INFO", b"A");
        let arr = match r.into_resp_value() {
            RespValue::Array(a) => a,
            o => panic!("expected array, got {o:?}"),
        };
        assert_eq!(arr[5], RespValue::Integer(17));

        assert!(err(run!(&mut db, b"CMS.INFO", b"noexist")).contains("CMS: key does not exist"));
    }

    #[test]
    fn merge_dimension_mismatch() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"A", b"100", b"5"));
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"B", b"200", b"5"));
        ok_res(run!(&mut db, b"CMS.INITBYDIM", b"C", b"100", b"5"));

        assert!(err(run!(&mut db, b"CMS.MERGE", b"C", b"2", b"A", b"B"))
            .contains("dimension mismatch"));
    }

    #[test]
    fn merge_multishard_combines_reports() {
        let mut a = Cms::new(100, 5);
        a.incr_by(b"foo", 5);
        a.incr_by(b"bar", 3);
        let mut b = Cms::new(100, 5);
        b.incr_by(b"foo", 2);
        b.incr_by(b"baz", 1);
        let dest = Cms::new(100, 5);

        let args = vec![
            b"CMS.MERGE".to_vec(),
            b"C".to_vec(),
            b"2".to_vec(),
            b"A".to_vec(),
            b"B".to_vec(),
            b"WEIGHTS".to_vec(),
            b"2".to_vec(),
            b"3".to_vec(),
        ];
        let keys = [1usize, 3, 4];
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: vec![1],
                result: CmdResult::Ok(RespValue::Array(vec![
                    integer(1),
                    integer(1),
                    integer(100),
                    integer(5),
                ])),
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: vec![3],
                result: CmdResult::Ok(RespValue::Array(vec![
                    integer(0),
                    integer(0),
                    integer(0),
                    integer(0),
                    integer(0),
                    RespValue::Bulk(a.serialize()),
                ])),
            },
            ShardPart {
                shard: 2,
                owned_key_idxs: vec![4],
                result: CmdResult::Ok(RespValue::Array(vec![
                    integer(0),
                    integer(0),
                    integer(0),
                    integer(0),
                    integer(1),
                    RespValue::Bulk(b.serialize()),
                ])),
            },
        ];
        match merge_cms_merge(&parts, &args, &keys, 0) {
            CmdResult::DeferredStore { key, value, reply } => {
                assert_eq!(key, b"C");
                assert_eq!(reply, RespValue::Simple("OK".into()));
                match value {
                    Some(PrimeValue::Cms(c)) => {
                        // foo: 5*2 + 2*3 = 16, bar: 3*2 = 6, baz: 1*3 = 3
                        assert_eq!(c.query(b"foo"), 16);
                        assert_eq!(c.query(b"bar"), 6);
                        assert_eq!(c.query(b"baz"), 3);
                    }
                    o => panic!("expected cms, got {o:?}"),
                }
            }
            o => panic!("expected DeferredStore, got {o:?}"),
        }
    }
}
