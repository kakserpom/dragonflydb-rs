use hashbrown::{HashMap, HashSet};

use crate::commands::exec::keys::glob_match;
use crate::commands::{bulk, integer, Command, OpContext, ShardPart, KeyRange, FLAG_BLOCKING, FLAG_DENYOOM, FLAG_FAST, FLAG_MULTI_KEY, FLAG_READONLY, FLAG_WRITE};
use crate::core::compact::CompactString;
use crate::core::zset::ZSet;
use crate::core::PrimeValue;
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::{format_double, itoa, parse_double, parse_i64, parse_u64, redis_range, shard_hash};

fn zset_mut<'a>(ctx: &'a mut OpContext, key: &[u8]) -> Result<&'a mut ZSet, RespError> {
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => Ok(z),
        Some(_) => Err(RespError::wrong_type()),
        None => Err(RespError::new("ERR no such key")),
    }
}

fn ensure_zset<'a>(ctx: &'a mut OpContext, key: &[u8]) -> Result<&'a mut ZSet, RespError> {
    if ctx.db.find(key, ctx.now_ms).is_none() {
        ctx.db.insert(CompactString::from_bytes(key), PrimeValue::ZSet(ZSet::new()));
    }
    zset_mut(ctx, key)
}

fn build_range_output(items: Vec<(CompactString, f64)>, with_scores: bool) -> RespValue {
    if with_scores {
        let mut out = Vec::with_capacity(items.len() * 2);
        for (m, s) in items {
            out.push(RespValue::Bulk(m.as_bytes().to_vec()));
            out.push(bulk(format_double(s).into_bytes()));
        }
        RespValue::Array(out)
    } else {
        RespValue::Array(items.into_iter().map(|(m, _)| RespValue::Bulk(m.as_bytes().to_vec())).collect())
    }
}

fn err_nan() -> RespError {
    RespError::new("ERR resulting score is not a number (NaN)")
}

fn err_float() -> RespError {
    RespError::float()
}

// ---------------------------------------------------------------------------
// ZADD
// ---------------------------------------------------------------------------

fn exec_zadd(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let mut i = key_idx + 1;
    let (mut nx, mut xx, mut gt, mut lt, mut ch, mut incr) = (false, false, false, false, false, false);
    loop {
        if i >= ctx.args.len() {
            return CmdResult::Err(RespError::syntax());
        }
        match ctx.args[i].to_ascii_uppercase().as_slice() {
            b"NX" => { nx = true; }
            b"XX" => { xx = true; }
            b"GT" => { gt = true; }
            b"LT" => { lt = true; }
            b"CH" => { ch = true; }
            b"INCR" => { incr = true; }
            _ => break,
        }
        i += 1;
    }
    let pairs = &ctx.args[i..];
    if pairs.is_empty() || pairs.len() % 2 != 0 {
        return CmdResult::Err(RespError::syntax());
    }
    if incr && pairs.len() != 2 {
        return CmdResult::Err(RespError::new("ERR INCR option supports a single increment-element pair"));
    }
    if nx && (gt || lt) {
        return CmdResult::Err(RespError::syntax());
    }
    let mut parsed: Vec<(f64, CompactString)> = Vec::with_capacity(pairs.len() / 2);
    for p in pairs.chunks(2) {
        let score = match parse_double(&p[0]) {
            Some(v) => v,
            None => return CmdResult::Err(err_float()),
        };
        if score.is_nan() {
            return CmdResult::Err(err_nan());
        }
        parsed.push((score, CompactString::from_bytes(&p[1])));
    }

    let z = match ensure_zset(ctx, key) {
        Ok(z) => z,
        Err(e) => return CmdResult::Err(e),
    };
    let (mut added, mut changed) = (0i64, 0i64);
    let mut incr_result: Option<f64> = None;
    for (score, member) in parsed {
        let existing = z.score(member.as_bytes());
        let should_update = match existing {
            Some(old) => !nx && !(gt && score <= old) && !(lt && score >= old),
            None => !xx,
        };
        if !should_update {
            continue;
        }
        if incr {
            let new_score = existing.unwrap_or(0.0) + score;
            if new_score.is_nan() {
                return CmdResult::Err(err_nan());
            }
            let was_new = existing.is_none();
            z.insert(member, new_score);
            incr_result = Some(new_score);
            if was_new {
                added += 1;
                changed += 1;
            } else if existing != Some(new_score) {
                changed += 1;
            }
        } else {
            let was_new = existing.is_none();
            z.insert(member, score);
            if was_new {
                added += 1;
                changed += 1;
            } else if existing != Some(score) {
                changed += 1;
            }
        }
    }
    if z.is_empty() {
        ctx.db.remove(key);
    }
    if incr {
        return CmdResult::Ok(match incr_result {
            Some(s) => bulk(format_double(s).into_bytes()),
            None => RespValue::Nil,
        });
    }
    CmdResult::Ok(integer(if ch { changed } else { added }))
}

// ---------------------------------------------------------------------------
// ZREM / ZSCORE / ZMSCORE / ZCARD / ZINCRBY / ZRANK / ZREVRANK
// ---------------------------------------------------------------------------

fn exec_zrem(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let members: Vec<CompactString> = ctx.args[key_idx + 1..].iter().map(|m| CompactString::from_bytes(m)).collect();
    let z = match zset_mut(ctx, key) {
        Ok(z) => z,
        Err(e) => {
            if e.message.starts_with("WRONGTYPE") {
                return CmdResult::Err(e);
            }
            return CmdResult::Ok(integer(0));
        }
    };
    let mut removed = 0i64;
    for m in &members {
        if z.delete(m.as_bytes()) {
            removed += 1;
        }
    }
    if z.is_empty() {
        ctx.db.remove(key);
    }
    CmdResult::Ok(integer(removed))
}

fn exec_zscore(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let member = &ctx.args[key_idx + 1];
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => match z.score(member) {
            Some(s) => CmdResult::Ok(bulk(format_double(s).into_bytes())),
            None => CmdResult::Ok(RespValue::Nil),
        },
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Nil),
    }
}

fn exec_zmscore(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => {
            let out = ctx.args[key_idx + 1..]
                .iter()
                .map(|m| match z.score(m) {
                    Some(s) => bulk(format_double(s).into_bytes()),
                    None => RespValue::Nil,
                })
                .collect();
            CmdResult::Ok(RespValue::Array(out))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Array(vec![RespValue::Nil; ctx.args.len() - key_idx - 1])),
    }
}

fn exec_zcard(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => CmdResult::Ok(integer(z.len() as i64)),
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

fn exec_zincrby(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let incr = match parse_double(&ctx.args[key_idx + 1]) {
        Some(v) => v,
        None => return CmdResult::Err(err_float()),
    };
    let member = CompactString::from_bytes(&ctx.args[key_idx + 2]);
    let z = match ensure_zset(ctx, key) {
        Ok(z) => z,
        Err(e) => return CmdResult::Err(e),
    };
    let cur = z.score(member.as_bytes()).unwrap_or(0.0);
    let new_score = cur + incr;
    if new_score.is_nan() {
        return CmdResult::Err(err_nan());
    }
    z.insert(member, new_score);
    CmdResult::Ok(bulk(format_double(new_score).into_bytes()))
}

fn exec_zrank(ctx: &mut OpContext) -> CmdResult {
    rank_common(ctx, false)
}

fn exec_zrevrank(ctx: &mut OpContext) -> CmdResult {
    rank_common(ctx, true)
}

fn rank_common(ctx: &mut OpContext, rev: bool) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let member = &ctx.args[key_idx + 1];
    let with_score = ctx.args.len() > key_idx + 2
        && ctx.args[key_idx + 2].eq_ignore_ascii_case(b"WITHSCORE");
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => match z.score(member) {
            Some(s) => {
                let rank = if rev { z.len() as i64 - 1 - z.rank(member).unwrap() } else { z.rank(member).unwrap() };
                if with_score {
                    CmdResult::Ok(RespValue::Array(vec![integer(rank), bulk(format_double(s).into_bytes())]))
                } else {
                    CmdResult::Ok(integer(rank))
                }
            }
            None => CmdResult::Ok(RespValue::Nil),
        },
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Nil),
    }
}

// ---------------------------------------------------------------------------
// ZRANGE / ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZCOUNT / ZREMRANGEBYRANK / ZREMRANGEBYSCORE
// ---------------------------------------------------------------------------

#[derive(Default)]
struct RangeOpts {
    byscore: bool,
    bylex: bool,
    rev: bool,
    withscores: bool,
    limit: Option<(usize, usize)>,
}

fn parse_range_opts(args: &[Vec<u8>], start: usize) -> Result<RangeOpts, RespError> {
    let mut o = RangeOpts::default();
    let mut i = start;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"WITHSCORES" => o.withscores = true,
            b"REV" => o.rev = true,
            b"BYSCORE" => o.byscore = true,
            b"BYLEX" => o.bylex = true,
            b"LIMIT" => {
                if i + 2 >= args.len() {
                    return Err(RespError::syntax());
                }
                let off = parse_i64(&args[i + 1]).ok_or_else(RespError::integer)?;
                let cnt = parse_i64(&args[i + 2]).ok_or_else(RespError::integer)?;
                o.limit = Some((off.max(0) as usize, cnt.max(0) as usize));
                i += 2;
            }
            _ => return Err(RespError::syntax()),
        }
        i += 1;
    }
    if o.byscore && o.bylex {
        return Err(RespError::syntax());
    }
    Ok(o)
}

fn parse_score_bound(s: &[u8]) -> Result<(f64, bool), RespError> {
    let (excl, body) = match s.first() {
        Some(b'(') => (true, &s[1..]),
        _ => (false, s),
    };
    let v = parse_double(body).ok_or_else(err_float)?;
    Ok((v, excl))
}

fn score_in_range(score: f64, bound: f64, exclusive: bool, is_lower: bool) -> bool {
    match (is_lower, exclusive) {
        (true, false) => score >= bound,
        (true, true) => score > bound,
        (false, false) => score <= bound,
        (false, true) => score < bound,
    }
}

/// Compute the (member, score) pairs selected by a ZRANGE/ZRANGESTORE style
/// `key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]` argument layout,
/// where `key_idx` is the index of the key argument. A missing key yields an
/// empty range; a wrong-type key is an error.
fn zrange_items(ctx: &mut OpContext, key_idx: usize) -> Result<Vec<(CompactString, f64)>, RespError> {
    let key = &ctx.args[key_idx];
    let opts = parse_range_opts(ctx.args, key_idx + 3)?;
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => {
            let items = if opts.byscore {
                let min = parse_score_bound(&ctx.args[key_idx + 1])?;
                let max = parse_score_bound(&ctx.args[key_idx + 2])?;
                let (lo, hi) = if opts.rev { (max, min) } else { (min, max) };
                z.range_by_score_filtered(|score| {
                    score_in_range(score, lo.0, lo.1, true) && score_in_range(score, hi.0, hi.1, false)
                }, opts.rev, opts.limit)
            } else if opts.bylex {
                let min = ctx.args[key_idx + 1].as_slice();
                let max = ctx.args[key_idx + 2].as_slice();
                let (lo, lo_incl, hi, hi_incl) = parse_lex_range(min, max)?;
                z.range_by_member_filtered(|m| {
                    (if lo.is_empty() { true } else if lo_incl { m.as_bytes() >= lo.as_slice() } else { m.as_bytes() > lo.as_slice() })
                        && (if hi.is_empty() { true } else if hi_incl { m.as_bytes() <= hi.as_slice() } else { m.as_bytes() < hi.as_slice() })
                }, opts.rev, opts.limit)
            } else {
                let start = parse_i64(&ctx.args[key_idx + 1]).ok_or_else(RespError::integer)?;
                let stop = parse_i64(&ctx.args[key_idx + 2]).ok_or_else(RespError::integer)?;
                let (s, c) = match redis_range(start, stop, z.len() as i64) {
                    Some(x) => x,
                    None => return Ok(vec![]),
                };
                let mut items = if opts.rev { z.rev_range(s, s + c as i64 - 1) } else { z.range(s, s + c as i64 - 1, opts.withscores) };
                if let Some((off, cnt)) = opts.limit {
                    items = if off < items.len() {
                        items.into_iter().skip(off).take(cnt).collect()
                    } else {
                        vec![]
                    };
                }
                items
            };
            Ok(items)
        }
        Some(_) => Err(RespError::wrong_type()),
        None => Ok(vec![]),
    }
}

fn exec_zrange(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let opts = match parse_range_opts(ctx.args, key_idx + 3) {
        Ok(o) => o,
        Err(e) => return CmdResult::Err(e),
    };
    let items = match zrange_items(ctx, key_idx) {
        Ok(items) => items,
        Err(e) => return CmdResult::Err(e),
    };
    CmdResult::Ok(build_range_output(items, opts.withscores))
}

/// Delete the destination key, or defer the deletion when it lives on another
/// shard. Used by ZRANGESTORE when the source is missing or the range is empty.
fn empty_store(ctx: &mut OpContext, dest_idx: usize) -> CmdResult {
    if ctx.owned_keys.contains(&dest_idx) {
        ctx.db.remove(&ctx.args[dest_idx]);
        CmdResult::Ok(integer(0))
    } else {
        CmdResult::deferred_store(ctx.args[dest_idx].clone(), None, integer(0))
    }
}

fn exec_zrangestore(ctx: &mut OpContext) -> CmdResult {
    // ZRANGESTORE dest src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
    let dest_idx = ctx.first_key_idx;
    let src_idx = dest_idx + 1;
    if !ctx.owned_keys.contains(&src_idx) {
        // Destination-only shard of a multi-shard STORE: contributes nothing.
        return CmdResult::Ok(RespValue::Array(vec![]));
    }
    let items = match zrange_items(ctx, src_idx) {
        Ok(items) => items,
        Err(e) => return CmdResult::Err(e),
    };
    if items.is_empty() {
        empty_store(ctx, dest_idx)
    } else {
        let mut zs = ZSet::new();
        for (m, s) in items {
            zs.insert(m, s);
        }
        let count = zs.len() as i64;
        if ctx.owned_keys.contains(&dest_idx) {
            ctx.db.insert(
                CompactString::from_bytes(&ctx.args[dest_idx]),
                PrimeValue::ZSet(zs),
            );
            CmdResult::Ok(integer(count))
        } else {
            CmdResult::deferred_store(
                ctx.args[dest_idx].clone(),
                Some(PrimeValue::ZSet(zs)),
                integer(count),
            )
        }
    }
}

fn merge_zrangestore(parts: &[ShardPart], _args: &[Vec<u8>], keys: &[usize], _now_ms: u64) -> CmdResult {
    for p in parts {
        if let CmdResult::Err(e) = &p.result {
            return CmdResult::Err(e.clone());
        }
        if p.owned_key_idxs.contains(&keys[1]) {
            return p.result.clone();
        }
    }
    parts[0].result.clone()
}

// ---------------------------------------------------------------------------
// ZINTERCARD
// ---------------------------------------------------------------------------

fn member_array(members: HashSet<CompactString>) -> RespValue {
    RespValue::Array(members.into_iter().map(|m| RespValue::Bulk(m.as_bytes().to_vec())).collect())
}

fn parts_to_members(p: &ShardPart) -> Result<HashSet<CompactString>, RespError> {
    match &p.result {
        CmdResult::Ok(RespValue::Array(arr)) => {
            let mut set = HashSet::with_capacity(arr.len());
            for v in arr {
                if let RespValue::Bulk(b) = v {
                    set.insert(CompactString::from_bytes(b));
                }
            }
            Ok(set)
        }
        CmdResult::Err(e) => Err(e.clone()),
        _ => Err(RespError::new("ERR internal: unexpected zset shard result")),
    }
}

/// The members of a key treated as a scored collection: a zset uses its own
/// scores, a set contributes score 1 (mirrors `FromObject`/`ScoreMapFromSet`).
fn scored_members(ctx: &mut OpContext, key: &[u8]) -> Result<HashSet<CompactString>, RespError> {
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => Ok(z.iter().map(|(m, _)| m.clone()).collect()),
        Some(PrimeValue::Set(s)) => Ok(s.members().into_iter().collect()),
        Some(_) => Err(RespError::wrong_type()),
        None => Ok(HashSet::new()),
    }
}

fn exec_zintercard(ctx: &mut OpContext) -> CmdResult {
    let numkeys = match parse_i64(&ctx.args[1]) {
        Some(v) if v >= 0 => v as usize,
        _ => return CmdResult::Err(RespError::integer()),
    };
    if numkeys == 0 {
        return CmdResult::Err(RespError::new("ERR at least 1 input key is needed for this command"));
    }
    let key_end = 2usize.saturating_add(numkeys);
    if ctx.args.len() < key_end {
        return CmdResult::Err(RespError::syntax());
    }
    let mut limit: i64 = 0;
    if ctx.args.len() > key_end {
        if !ctx.args[key_end].eq_ignore_ascii_case(b"LIMIT") || ctx.args.len() != key_end + 2 {
            return CmdResult::Err(RespError::syntax());
        }
        match parse_i64(&ctx.args[key_end + 1]) {
            Some(v) if v >= 0 => limit = v,
            _ => return CmdResult::Err(RespError::new("ERR limit value is not a positive integer")),
        }
    }

    let mut acc: Option<HashSet<CompactString>> = None;
    let mut any_missing = false;
    for &ki in ctx.owned_keys {
        if ki < 2 || ki >= key_end {
            continue;
        }
        let members = match scored_members(ctx, &ctx.args[ki]) {
            Ok(m) if m.is_empty() => {
                any_missing = true;
                continue;
            }
            Ok(m) => m,
            Err(e) => return CmdResult::Err(e),
        };
        match &mut acc {
            None => acc = Some(members),
            Some(a) => a.retain(|m| members.contains(m)),
        }
        if acc.as_ref().is_some_and(|a| a.is_empty()) {
            break;
        }
    }
    let members = if any_missing { HashSet::new() } else { acc.unwrap_or_default() };
    let count = if limit > 0 { (members.len() as i64).min(limit) } else { members.len() as i64 };

    if ctx.owned_keys.len() == numkeys {
        // Single shard holds every input key: reply with the final count.
        return CmdResult::Ok(integer(count));
    }
    CmdResult::Ok(member_array(members))
}

fn merge_zintercard(parts: &[ShardPart], args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    for p in parts {
        if let CmdResult::Err(e) = &p.result {
            return CmdResult::Err(e.clone());
        }
    }
    let numkeys = match parse_i64(&args[1]) {
        Some(v) => v as usize,
        None => return CmdResult::Ok(integer(0)),
    };
    let key_end = 2usize.saturating_add(numkeys);
    let mut limit: i64 = 0;
    if args.len() > key_end && args[key_end].eq_ignore_ascii_case(b"LIMIT") {
        limit = args.get(key_end + 1).and_then(|a| parse_i64(a)).unwrap_or(0);
    }
    let mut acc: Option<HashSet<CompactString>> = None;
    for p in parts {
        let members = match parts_to_members(p) {
            Ok(v) => v,
            Err(e) => return CmdResult::Err(e),
        };
        if members.is_empty() {
            return CmdResult::Ok(integer(0));
        }
        match &mut acc {
            None => acc = Some(members),
            Some(a) => a.retain(|m| members.contains(m)),
        }
    }
    let n = acc.map(|a| a.len()).unwrap_or(0) as i64;
    CmdResult::Ok(integer(if limit > 0 { n.min(limit) } else { n }))
}

// ---------------------------------------------------------------------------
// ZUNION / ZINTER (+ ZUNIONSTORE / ZINTERSTORE)
// ---------------------------------------------------------------------------

type ScoredMap = HashMap<CompactString, f64>;

#[derive(Clone, Copy, PartialEq)]
enum AggType {
    Sum,
    Min,
    Max,
}

/// `AGGREGATE` reduction: SUM clamps NaN to 0, MIN/MAX take the extreme value.
/// Mirrors `Aggregate` from the reference.
fn agg(v1: f64, v2: f64, atype: AggType) -> f64 {
    match atype {
        AggType::Sum => {
            let v = v1 + v2;
            if v.is_nan() {
                0.0
            } else {
                v
            }
        }
        AggType::Min => v1.min(v2),
        AggType::Max => v1.max(v2),
    }
}

/// Parse `[dest] <numkeys> <key>... [WEIGHTS w...] [AGGREGATE SUM|MIN|MAX]
/// [WITHSCORES]`. `WEIGHTS` must supply exactly `numkeys` values; any trailing
/// argument that is not a known option is a syntax error. Mirrors
/// `ParseSetOpArgs`/`kSetOpGrammar` from the reference.
fn parse_setop_args(
    args: &[Vec<u8>],
    store: bool,
    cmd: &str,
) -> Result<(usize, Vec<f64>, AggType, bool), RespError> {
    let numkeys_idx = if store { 2 } else { 1 };
    let numkeys = match parse_i64(&args[numkeys_idx]) {
        Some(v) if v >= 0 => v as usize,
        _ => return Err(RespError::integer()),
    };
    if numkeys == 0 {
        return Err(RespError::new(format!(
            "ERR at least 1 input key is needed for {cmd}"
        )));
    }
    let key_end = numkeys_idx + 1 + numkeys;
    if args.len() < key_end {
        return Err(RespError::syntax());
    }
    let mut weights = vec![1.0f64; numkeys];
    let mut atype = AggType::Sum;
    let mut with_scores = false;
    let mut i = key_end;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"WEIGHTS" => {
                if i + numkeys >= args.len() {
                    return Err(RespError::syntax());
                }
                for (j, w) in weights.iter_mut().enumerate() {
                    *w = match parse_double(&args[i + 1 + j]) {
                        Some(v) => v,
                        None => return Err(RespError::new("ERR weight value is not a float")),
                    };
                }
                i += 1 + numkeys;
            }
            b"AGGREGATE" => {
                if i + 1 >= args.len() {
                    return Err(RespError::syntax());
                }
                atype = match args[i + 1].to_ascii_uppercase().as_slice() {
                    b"SUM" => AggType::Sum,
                    b"MIN" => AggType::Min,
                    b"MAX" => AggType::Max,
                    _ => return Err(RespError::syntax()),
                };
                i += 2;
            }
            b"WITHSCORES" => {
                if store {
                    return Err(RespError::syntax());
                }
                with_scores = true;
                i += 1;
            }
            _ => return Err(RespError::syntax()),
        }
    }
    Ok((numkeys, weights, atype, with_scores))
}

/// A key as a scored map: a zset uses its own scores multiplied by `weight`
/// (NaN becomes 0), a set contributes every member with score `weight`.
/// Mirrors `FromObject`/`ScoreMapFromSet`. `None` for a missing key.
fn weighted_map_of(ctx: &mut OpContext, key: &[u8], weight: f64) -> Result<Option<ScoredMap>, RespError> {
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => {
            let mut map = HashMap::with_capacity(z.len());
            for (m, s) in z.iter() {
                let score = s * weight;
                map.insert(m.clone(), if score.is_nan() { 0.0 } else { score });
            }
            Ok(Some(map))
        }
        Some(PrimeValue::Set(s)) => Ok(Some(s.members().into_iter().map(|m| (m, weight)).collect())),
        Some(_) => Err(RespError::wrong_type()),
        None => Ok(None),
    }
}

fn merge_scored_map(target: &mut ScoredMap, src: ScoredMap, atype: AggType) {
    for (m, s) in src {
        match target.entry(m) {
            hashbrown::hash_map::Entry::Occupied(mut e) => {
                *e.get_mut() = agg(*e.get(), s, atype);
            }
            hashbrown::hash_map::Entry::Vacant(e) => {
                e.insert(s);
            }
        }
    }
}

fn intersect_scored_map(target: &mut ScoredMap, src: &ScoredMap, atype: AggType) {
    target.retain(|m, v| match src.get(m) {
        Some(s) => {
            *v = agg(*v, *s, atype);
            true
        }
        None => false,
    });
}

/// Union or intersection of the weighted scored maps owned by this shard.
/// Mirrors `UnionShardKeysWithScore`/`OpInter`: a missing key empties an
/// intersection but is skipped by a union.
fn union_inter(
    ctx: &mut OpContext,
    is_inter: bool,
    numkeys_idx: usize,
    key_end: usize,
    weights: &[f64],
    atype: AggType,
) -> Result<ScoredMap, RespError> {
    let mut acc: Option<ScoredMap> = None;
    let mut any_missing = false;
    for &ki in ctx.owned_keys {
        if ki < numkeys_idx + 1 || ki >= key_end {
            continue;
        }
        let w = weights[ki - (numkeys_idx + 1)];
        let map = match weighted_map_of(ctx, &ctx.args[ki], w)? {
            Some(m) => m,
            None => {
                any_missing = true;
                continue;
            }
        };
        match &mut acc {
            None => acc = Some(map),
            Some(a) => {
                if is_inter {
                    intersect_scored_map(a, &map, atype);
                } else {
                    merge_scored_map(a, map, atype);
                }
            }
        }
        if is_inter && acc.as_ref().is_some_and(|a| a.is_empty()) {
            break;
        }
    }
    if is_inter && any_missing {
        return Ok(ScoredMap::new());
    }
    Ok(acc.unwrap_or_default())
}

fn exec_union_inter(ctx: &mut OpContext, is_inter: bool, store: bool, cmd: &str) -> CmdResult {
    let (numkeys, weights, atype, with_scores) = match parse_setop_args(ctx.args, store, cmd) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    let numkeys_idx = if store { 2 } else { 1 };
    let key_end = numkeys_idx + 1 + numkeys;
    if store && !ctx.owned_keys.iter().any(|&ki| ki > numkeys_idx && ki < key_end) {
        // Shard holding only the destination key contributes nothing.
        return CmdResult::Ok(RespValue::Nil);
    }
    let map = match union_inter(ctx, is_inter, numkeys_idx, key_end, &weights, atype) {
        Ok(m) => m,
        Err(e) => return CmdResult::Err(e),
    };
    let single = if store {
        ctx.owned_keys.len() == numkeys + 1
    } else {
        ctx.owned_keys.len() == numkeys
    };
    if single {
        if store {
            let dest = &ctx.args[ctx.first_key_idx];
            let count = map.len() as i64;
            write_zset_or_delete(ctx, dest, map);
            return CmdResult::Ok(integer(count));
        }
        return CmdResult::Ok(scored_reply(map, with_scores));
    }
    CmdResult::Ok(scored_array(map))
}

fn exec_zunion(ctx: &mut OpContext) -> CmdResult {
    exec_union_inter(ctx, false, false, "zunion")
}

fn exec_zinter(ctx: &mut OpContext) -> CmdResult {
    exec_union_inter(ctx, true, false, "zinter")
}

fn exec_zunionstore(ctx: &mut OpContext) -> CmdResult {
    exec_union_inter(ctx, false, true, "zunionstore")
}

fn exec_zinterstore(ctx: &mut OpContext) -> CmdResult {
    exec_union_inter(ctx, true, true, "zinterstore")
}

/// Encode a scored map as a flat `[member, score, ...]` RESP array.
fn scored_array(map: ScoredMap) -> RespValue {
    let mut out = Vec::with_capacity(map.len() * 2);
    for (m, s) in map {
        out.push(RespValue::Bulk(m.as_bytes().to_vec()));
        out.push(bulk(format_double(s).into_bytes()));
    }
    RespValue::Array(out)
}

/// A scored map sorted by `(score, member)` as `ScoredMemberView` does, then
/// formatted with or without scores.
fn scored_reply(map: ScoredMap, with_scores: bool) -> RespValue {
    let mut items: Vec<(CompactString, f64)> = map.into_iter().collect();
    items.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    build_range_output(items, with_scores)
}

/// A zset value for the destination, or `None` when empty (deletes the key).
fn zset_value(map: ScoredMap) -> Option<PrimeValue> {
    if map.is_empty() {
        None
    } else {
        let mut zs = ZSet::new();
        for (m, s) in map {
            zs.insert(m, s);
        }
        Some(PrimeValue::ZSet(zs))
    }
}

/// Write `map` to `dest` on this shard, removing the key when the result is
/// empty (the reference never persists empty zsets).
fn write_zset_or_delete(ctx: &mut OpContext, dest: &[u8], map: ScoredMap) {
    match zset_value(map) {
        Some(v) => {
            ctx.db.clear_expiry(dest);
            ctx.db.insert(CompactString::from_bytes(dest), v);
        }
        None => {
            ctx.db.remove(dest);
        }
    }
}

fn parts_to_scored(p: &ShardPart) -> Result<Option<ScoredMap>, RespError> {
    match &p.result {
        CmdResult::Ok(RespValue::Nil) => Ok(None),
        CmdResult::Ok(RespValue::Array(arr)) => {
            let mut map = ScoredMap::with_capacity(arr.len() / 2);
            for chunk in arr.chunks_exact(2) {
                let (m, s) = match chunk {
                    [RespValue::Bulk(m), RespValue::Bulk(s)] => (m, s),
                    _ => continue,
                };
                if let Some(score) = parse_double(s) {
                    map.insert(CompactString::from_bytes(m), score);
                }
            }
            Ok(Some(map))
        }
        CmdResult::Err(e) => Err(e.clone()),
        _ => Err(RespError::new("ERR internal: unexpected zset shard result")),
    }
}

fn merge_zunion(parts: &[ShardPart], args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    let (_, _, atype, with_scores) = match parse_setop_args(args, false, "zunion") {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    let mut acc: ScoredMap = ScoredMap::new();
    for p in parts {
        match parts_to_scored(p) {
            Ok(Some(map)) => merge_scored_map(&mut acc, map, atype),
            Ok(None) => {}
            Err(e) => return CmdResult::Err(e),
        }
    }
    CmdResult::Ok(scored_reply(acc, with_scores))
}

fn merge_zinter(parts: &[ShardPart], args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    let (_, _, atype, with_scores) = match parse_setop_args(args, false, "zinter") {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    let mut acc: Option<ScoredMap> = None;
    for p in parts {
        match parts_to_scored(p) {
            Ok(Some(map)) => {
                if map.is_empty() {
                    return CmdResult::Ok(scored_reply(ScoredMap::new(), with_scores));
                }
                match &mut acc {
                    None => acc = Some(map),
                    Some(a) => intersect_scored_map(a, &map, atype),
                }
                if acc.as_ref().is_some_and(|a| a.is_empty()) {
                    break;
                }
            }
            Ok(None) => {}
            Err(e) => return CmdResult::Err(e),
        }
    }
    CmdResult::Ok(scored_reply(acc.unwrap_or_default(), with_scores))
}

fn merge_zunionstore(parts: &[ShardPart], args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    let (_, _, atype, _) = match parse_setop_args(args, true, "zunionstore") {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    let dest = &args[keys[0]];
    let mut acc: ScoredMap = ScoredMap::new();
    for p in parts {
        match parts_to_scored(p) {
            Ok(Some(map)) => merge_scored_map(&mut acc, map, atype),
            Ok(None) => {}
            Err(e) => return CmdResult::Err(e),
        }
    }
    let count = acc.len() as i64;
    CmdResult::deferred_store(dest.clone(), zset_value(acc), integer(count))
}

fn merge_zinterstore(parts: &[ShardPart], args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    let (_, _, atype, _) = match parse_setop_args(args, true, "zinterstore") {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    let dest = &args[keys[0]];
    let mut acc: Option<ScoredMap> = None;
    for p in parts {
        match parts_to_scored(p) {
            Ok(Some(map)) => {
                if map.is_empty() {
                    return CmdResult::deferred_store(dest.clone(), None, integer(0));
                }
                match &mut acc {
                    None => acc = Some(map),
                    Some(a) => intersect_scored_map(a, &map, atype),
                }
                if acc.as_ref().is_some_and(|a| a.is_empty()) {
                    break;
                }
            }
            Ok(None) => {}
            Err(e) => return CmdResult::Err(e),
        }
    }
    let acc = acc.unwrap_or_default();
    let count = acc.len() as i64;
    CmdResult::deferred_store(dest.clone(), zset_value(acc), integer(count))
}

// ---------------------------------------------------------------------------
// ZDIFF / ZDIFFSTORE
// ---------------------------------------------------------------------------

/// Parse `[dest] <numkeys> <key>... [WITHSCORES]` (WITHSCORES only for the
/// read variant, and only as the final argument).
fn parse_diff_args(args: &[Vec<u8>], store: bool) -> Result<(usize, bool), RespError> {
    let numkeys_idx = if store { 2 } else { 1 };
    let key_start = numkeys_idx + 1;
    let numkeys = match parse_i64(&args[numkeys_idx]) {
        Some(v) if v >= 0 => v as usize,
        _ => return Err(RespError::integer()),
    };
    if numkeys == 0 {
        return Err(RespError::new("ERR at least 1 input key is needed for zdiff"));
    }
    let key_end = key_start.saturating_add(numkeys);
    if args.len() < key_end {
        return Err(RespError::syntax());
    }
    if args.len() > key_end {
        if store || args.len() != key_end + 1 || !args[key_end].eq_ignore_ascii_case(b"WITHSCORES") {
            return Err(RespError::syntax());
        }
        return Ok((numkeys, true));
    }
    Ok((numkeys, false))
}

/// The result of diffing all keys on this shard, anchored at `base_idx`. The
/// base is only ever zsets (mirrors `OpFetch`); its members' scores are kept.
fn diff_local(
    ctx: &mut OpContext,
    base_idx: usize,
    key_start: usize,
    key_end: usize,
    store: bool,
) -> Result<ScoredMap, RespError> {
    let mut base: ScoredMap = match ctx.db.find(&ctx.args[base_idx], ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => z.iter().map(|(m, s)| (m.clone(), s)).collect(),
        Some(_) => return Err(RespError::wrong_type()),
        None => ScoredMap::new(),
    };
    for &ki in ctx.owned_keys {
        if ki == base_idx || (store && ki == ctx.first_key_idx) || ki < key_start || ki >= key_end {
            continue;
        }
        match ctx.db.find(&ctx.args[ki], ctx.now_ms) {
            Some(PrimeValue::ZSet(z)) => {
                for (m, _) in z.iter() {
                    base.remove(&m);
                }
            }
            Some(_) => return Err(RespError::wrong_type()),
            None => {}
        }
    }
    Ok(base)
}

fn exec_zdiff(ctx: &mut OpContext, store: bool) -> CmdResult {
    let (numkeys, with_scores) = match parse_diff_args(ctx.args, store) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    let key_start: usize = if store { 3 } else { 2 };
    let key_end = key_start.saturating_add(numkeys);
    if store && !ctx.owned_keys.iter().any(|&ki| ki >= key_start && ki < key_end) {
        // Shard holding only the destination key contributes nothing.
        return CmdResult::Ok(RespValue::Nil);
    }
    let single = if store {
        ctx.owned_keys.len() == numkeys + 1
    } else {
        ctx.owned_keys.len() == numkeys
    };
    if single {
        let map = match diff_local(ctx, key_start, key_start, key_end, store) {
            Ok(m) => m,
            Err(e) => return CmdResult::Err(e),
        };
        if store {
            let dest = &ctx.args[ctx.first_key_idx];
            let count = map.len() as i64;
            write_zset_or_delete(ctx, dest, map);
            return CmdResult::Ok(integer(count));
        }
        return CmdResult::Ok(scored_reply(map, with_scores));
    }
    // Multi-shard partial: one flat scored map per owned source, in key order
    // so the base key's map is always first on its shard (mirrors `OpFetch`).
    let mut maps: Vec<RespValue> = Vec::new();
    for &ki in ctx.owned_keys {
        if (store && ki == ctx.first_key_idx) || ki < key_start || ki >= key_end {
            continue;
        }
        let map = match ctx.db.find(&ctx.args[ki], ctx.now_ms) {
            Some(PrimeValue::ZSet(z)) => z.iter().map(|(m, s)| (m.clone(), s)).collect::<ScoredMap>(),
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => ScoredMap::new(),
        };
        maps.push(scored_array(map));
    }
    CmdResult::Ok(RespValue::Array(maps))
}

fn exec_zdiff_cmd(ctx: &mut OpContext) -> CmdResult {
    exec_zdiff(ctx, false)
}

fn exec_zdiffstore(ctx: &mut OpContext) -> CmdResult {
    exec_zdiff(ctx, true)
}

/// A shard result as a list of scored maps (`None` for a destination-only
/// shard that returned `Nil`).
fn parts_to_maps(p: &ShardPart) -> Result<Option<Vec<ScoredMap>>, RespError> {
    match &p.result {
        CmdResult::Ok(RespValue::Nil) => Ok(None),
        CmdResult::Ok(RespValue::Array(outer)) => {
            let mut maps = Vec::with_capacity(outer.len());
            for v in outer {
                if let RespValue::Array(inner) = v {
                    let mut map = ScoredMap::new();
                    for chunk in inner.chunks_exact(2) {
                        let (m, s) = match chunk {
                            [RespValue::Bulk(m), RespValue::Bulk(s)] => (m, s),
                            _ => continue,
                        };
                        if let Some(score) = parse_double(s) {
                            map.insert(CompactString::from_bytes(m), score);
                        }
                    }
                    maps.push(map);
                }
            }
            Ok(Some(maps))
        }
        CmdResult::Err(e) => Err(e.clone()),
        _ => Err(RespError::new("ERR internal: unexpected zset shard result")),
    }
}

/// The base shard's first map is the base; the base shard's remaining maps and
/// every other shard's maps remove members from it (mirrors `ZDiffOp`).
fn diff_from_parts(
    parts: &[ShardPart],
    base_idx: usize,
) -> Result<Option<ScoredMap>, RespError> {
    let mut base: Option<ScoredMap> = None;
    let mut others: Vec<ScoredMap> = Vec::new();
    for p in parts {
        let is_base = p.owned_key_idxs.contains(&base_idx);
        match parts_to_maps(p) {
            Ok(Some(maps)) => {
                for (i, map) in maps.into_iter().enumerate() {
                    if is_base && i == 0 {
                        base = Some(map);
                    } else {
                        others.push(map);
                    }
                }
            }
            Ok(None) => {}
            Err(e) => return Err(e),
        }
    }
    let mut base = match base {
        Some(b) => b,
        None => return Ok(None),
    };
    for map in others {
        for m in map.keys() {
            base.remove(m);
        }
    }
    Ok(Some(base))
}

fn merge_zdiff(parts: &[ShardPart], args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    let (_, with_scores) = match parse_diff_args(args, false) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    match diff_from_parts(parts, keys[0]) {
        Ok(Some(base)) => CmdResult::Ok(scored_reply(base, with_scores)),
        Ok(None) => CmdResult::Err(RespError::new("ERR internal: ZDIFF base shard missing")),
        Err(e) => CmdResult::Err(e),
    }
}

fn merge_zdiffstore(parts: &[ShardPart], args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    match parse_diff_args(args, true) {
        Ok(_) => {}
        Err(e) => return CmdResult::Err(e),
    };
    let dest = &args[keys[0]];
    match diff_from_parts(parts, keys[1]) {
        Ok(Some(base)) => {
            let count = base.len() as i64;
            CmdResult::deferred_store(dest.clone(), zset_value(base), integer(count))
        }
        Ok(None) => CmdResult::Err(RespError::new("ERR internal: ZDIFFSTORE base shard missing")),
        Err(e) => CmdResult::Err(e),
    }
}

fn parse_lex_range(min: &[u8], max: &[u8]) -> Result<(Vec<u8>, bool, Vec<u8>, bool), RespError> {
    let parse = |b: &[u8]| -> Result<(Vec<u8>, bool), RespError> {
        if b == b"-" {
            Ok((Vec::new(), true))
        } else if b == b"+" {
            Ok((Vec::new(), false)) // handled as no upper bound
        } else if let Some(rest) = b.strip_prefix(b"[") {
            Ok((rest.to_vec(), true))
        } else if let Some(rest) = b.strip_prefix(b"(") {
            Ok((rest.to_vec(), false))
        } else {
            Err(RespError::new("ERR min or max not valid string range item"))
        }
    };
    let (l, li) = parse(min)?;
    let (h, hi) = parse(max)?;
    // distinguish "+" as upper bound: use empty vec for both - and +; the filter uses
    // empty lo => unbounded low, empty hi => unbounded high. Both bound checks must
    // be applied, so return a flag when the value is literally "+".
    Ok((l, li, h, hi))
}

fn exec_zrangebyscore(ctx: &mut OpContext) -> CmdResult {
    zrange_by_score_common(ctx, false)
}

fn exec_zrevrangebyscore(ctx: &mut OpContext) -> CmdResult {
    zrange_by_score_common(ctx, true)
}

fn zrange_by_score_common(ctx: &mut OpContext, rev: bool) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let (mut with_scores, mut limit) = (false, None);
    let mut i = key_idx + 3;
    while i < ctx.args.len() {
        match ctx.args[i].to_ascii_uppercase().as_slice() {
            b"WITHSCORES" => with_scores = true,
            b"LIMIT" => {
                if i + 2 >= ctx.args.len() {
                    return CmdResult::Err(RespError::syntax());
                }
                let off = match parse_i64(&ctx.args[i + 1]) {
                    Some(v) => v,
                    None => return CmdResult::Err(RespError::integer()),
                };
                let cnt = match parse_i64(&ctx.args[i + 2]) {
                    Some(v) => v,
                    None => return CmdResult::Err(RespError::integer()),
                };
                limit = Some((off.max(0) as usize, cnt.max(0) as usize));
                i += 2;
            }
            _ => return CmdResult::Err(RespError::syntax()),
        }
        i += 1;
    }
    let min = match parse_score_bound(&ctx.args[key_idx + 1]) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    let max = match parse_score_bound(&ctx.args[key_idx + 2]) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    let (lo, hi) = if rev { (max, min) } else { (min, max) };
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => {
            let items = z.range_by_score_filtered(
                |score| score_in_range(score, lo.0, lo.1, true) && score_in_range(score, hi.0, hi.1, false),
                rev,
                limit,
            );
            CmdResult::Ok(build_range_output(items, with_scores))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Array(vec![])),
    }
}

fn exec_zcount(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let min = match parse_score_bound(&ctx.args[key_idx + 1]) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    let max = match parse_score_bound(&ctx.args[key_idx + 2]) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => {
            let count = z.iter()
                .filter(|(_, s)| score_in_range(*s, min.0, min.1, true) && score_in_range(*s, max.0, max.1, false))
                .count();
            CmdResult::Ok(integer(count as i64))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

fn exec_zremrangebyrank(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let start = match parse_i64(&ctx.args[key_idx + 1]) {
        Some(v) => v,
        None => return CmdResult::Err(RespError::integer()),
    };
    let stop = match parse_i64(&ctx.args[key_idx + 2]) {
        Some(v) => v,
        None => return CmdResult::Err(RespError::integer()),
    };
    let z = match zset_mut(ctx, key) {
        Ok(z) => z,
        Err(e) => {
            if e.message.starts_with("WRONGTYPE") {
                return CmdResult::Err(e);
            }
            return CmdResult::Ok(integer(0));
        }
    };
    let (s, c) = match redis_range(start, stop, z.len() as i64) {
        Some(x) => x,
        None => return CmdResult::Ok(integer(0)),
    };
    let to_remove: Vec<CompactString> = z.range(s, s + c as i64 - 1, false).into_iter().map(|(m, _)| m).collect();
    let mut removed = 0i64;
    for m in to_remove {
        z.delete(&m);
        removed += 1;
    }
    if z.is_empty() {
        ctx.db.remove(key);
    }
    CmdResult::Ok(integer(removed))
}

fn exec_zremrangebyscore(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let min = match parse_score_bound(&ctx.args[key_idx + 1]) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    let max = match parse_score_bound(&ctx.args[key_idx + 2]) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    let z = match zset_mut(ctx, key) {
        Ok(z) => z,
        Err(e) => {
            if e.message.starts_with("WRONGTYPE") {
                return CmdResult::Err(e);
            }
            return CmdResult::Ok(integer(0));
        }
    };
    let to_remove: Vec<CompactString> = z
        .iter()
        .filter(|(_, s)| score_in_range(*s, min.0, min.1, true) && score_in_range(*s, max.0, max.1, false))
        .map(|(m, _)| m)
        .collect();
    let mut removed = 0i64;
    for m in to_remove {
        z.delete(&m);
        removed += 1;
    }
    if z.is_empty() {
        ctx.db.remove(key);
    }
    CmdResult::Ok(integer(removed))
}

fn exec_zpopminmax(ctx: &mut OpContext, max: bool) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let with_count = ctx.args.len() > key_idx + 1;
    let count = if with_count {
        let c = match parse_i64(&ctx.args[key_idx + 1]) {
            Some(v) => v,
            None => return CmdResult::Err(RespError::integer()),
        };
        if c < 0 {
            return CmdResult::Err(RespError::new("ERR value is out of range, must be positive"));
        }
        c as usize
    } else {
        1
    };
    let z = match zset_mut(ctx, key) {
        Ok(z) => z,
        Err(e) => {
            if e.message.starts_with("WRONGTYPE") {
                return CmdResult::Err(e);
            }
            return CmdResult::Ok(RespValue::Array(vec![]));
        }
    };
    let mut out = Vec::new();
    for _ in 0..count {
        let item = if max { z.pop_max() } else { z.pop_min() };
        match item {
            Some((m, s)) => {
                out.push(RespValue::Bulk(m.as_bytes().to_vec()));
                out.push(bulk(format_double(s).into_bytes()));
            }
            None => break,
        }
    }
    if z.is_empty() {
        ctx.db.remove(key);
    }
    CmdResult::Ok(RespValue::Array(out))
}

fn exec_zpopmin(ctx: &mut OpContext) -> CmdResult {
    exec_zpopminmax(ctx, false)
}
fn exec_zpopmax(ctx: &mut OpContext) -> CmdResult {
    exec_zpopminmax(ctx, true)
}

fn exec_zrangebylex(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let opts = match parse_range_opts(ctx.args, key_idx + 3) {
        Ok(o) => o,
        Err(e) => return CmdResult::Err(e),
    };
    let min = ctx.args[key_idx + 1].as_slice();
    let max = ctx.args[key_idx + 2].as_slice();
    let (lo, lo_incl, hi, hi_incl) = match parse_lex_range(min, max) {
        Ok(x) => x,
        Err(e) => return CmdResult::Err(e),
    };
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => {
            let lo_unbound = min == b"-";
            let hi_unbound = max == b"+";
            let items = z.range_by_member_filtered(
                |m| {
                    let below_ok = if lo_unbound { true } else if lo_incl { m.as_bytes() >= lo.as_slice() } else { m.as_bytes() > lo.as_slice() };
                    let above_ok = if hi_unbound { true } else if hi_incl { m.as_bytes() <= hi.as_slice() } else { m.as_bytes() < hi.as_slice() };
                    below_ok && above_ok
                },
                opts.rev,
                opts.limit,
            );
            CmdResult::Ok(build_range_output(items, opts.withscores))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Array(vec![])),
    }
}

pub static CMD_ZADD: Command = Command {
    name: "ZADD",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_zadd,
    merge: None,
};
pub static CMD_ZREM: Command = Command {
    name: "ZREM",
    arity: -3,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_zrem,
    merge: None,
};
pub static CMD_ZSCORE: Command = Command {
    name: "ZSCORE",
    arity: 3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_zscore,
    merge: None,
};
pub static CMD_ZMSCORE: Command = Command {
    name: "ZMSCORE",
    arity: -3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_zmscore,
    merge: None,
};
pub static CMD_ZCARD: Command = Command {
    name: "ZCARD",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_zcard,
    merge: None,
};
pub static CMD_ZINCRBY: Command = Command {
    name: "ZINCRBY",
    arity: 4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_zincrby,
    merge: None,
};
pub static CMD_ZRANK: Command = Command {
    name: "ZRANK",
    arity: -3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_zrank,
    merge: None,
};
pub static CMD_ZREVRANK: Command = Command {
    name: "ZREVRANK",
    arity: -3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_zrevrank,
    merge: None,
};
pub static CMD_ZRANGE: Command = Command {
    name: "ZRANGE",
    arity: -4,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_zrange,
    merge: None,
};
pub static CMD_ZRANGESTORE: Command = Command {
    name: "ZRANGESTORE",
    arity: -5,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    key_range: KeyRange::TWO,
    exec: exec_zrangestore,
    merge: Some(merge_zrangestore),
};
pub static CMD_ZINTERCARD: Command = Command {
    name: "ZINTERCARD",
    arity: -3,
    flags: FLAG_READONLY | FLAG_MULTI_KEY,
    key_range: KeyRange { first: 2, last: 0, step: 1 },
    exec: exec_zintercard,
    merge: Some(merge_zintercard),
};
pub static CMD_ZUNION: Command = Command {
    name: "ZUNION",
    arity: -3,
    flags: FLAG_READONLY | FLAG_MULTI_KEY,
    key_range: KeyRange { first: 2, last: 0, step: 1 },
    exec: exec_zunion,
    merge: Some(merge_zunion),
};
pub static CMD_ZUNIONSTORE: Command = Command {
    name: "ZUNIONSTORE",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_MULTI_KEY,
    key_range: KeyRange { first: 1, last: 0, step: 1 },
    exec: exec_zunionstore,
    merge: Some(merge_zunionstore),
};
pub static CMD_ZINTER: Command = Command {
    name: "ZINTER",
    arity: -3,
    flags: FLAG_READONLY | FLAG_MULTI_KEY,
    key_range: KeyRange { first: 2, last: 0, step: 1 },
    exec: exec_zinter,
    merge: Some(merge_zinter),
};
pub static CMD_ZINTERSTORE: Command = Command {
    name: "ZINTERSTORE",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_MULTI_KEY,
    key_range: KeyRange { first: 1, last: 0, step: 1 },
    exec: exec_zinterstore,
    merge: Some(merge_zinterstore),
};
pub static CMD_ZDIFF: Command = Command {
    name: "ZDIFF",
    arity: -3,
    flags: FLAG_READONLY | FLAG_MULTI_KEY,
    key_range: KeyRange { first: 2, last: 0, step: 1 },
    exec: exec_zdiff_cmd,
    merge: Some(merge_zdiff),
};
pub static CMD_ZDIFFSTORE: Command = Command {
    name: "ZDIFFSTORE",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_MULTI_KEY,
    key_range: KeyRange { first: 1, last: 0, step: 1 },
    exec: exec_zdiffstore,
    merge: Some(merge_zdiffstore),
};
pub static CMD_ZRANGEBYSCORE: Command = Command {
    name: "ZRANGEBYSCORE",
    arity: -4,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_zrangebyscore,
    merge: None,
};
pub static CMD_ZREVRANGEBYSCORE: Command = Command {
    name: "ZREVRANGEBYSCORE",
    arity: -4,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_zrevrangebyscore,
    merge: None,
};
pub static CMD_ZCOUNT: Command = Command {
    name: "ZCOUNT",
    arity: 4,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_zcount,
    merge: None,
};
pub static CMD_ZREMRANGEBYRANK: Command = Command {
    name: "ZREMRANGEBYRANK",
    arity: 4,
    flags: FLAG_WRITE,
    key_range: KeyRange::ONE,
    exec: exec_zremrangebyrank,
    merge: None,
};
pub static CMD_ZREMRANGEBYSCORE: Command = Command {
    name: "ZREMRANGEBYSCORE",
    arity: 4,
    flags: FLAG_WRITE,
    key_range: KeyRange::ONE,
    exec: exec_zremrangebyscore,
    merge: None,
};
pub static CMD_ZPOPMIN: Command = Command {
    name: "ZPOPMIN",
    arity: -2,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_zpopmin,
    merge: None,
};
pub static CMD_ZPOPMAX: Command = Command {
    name: "ZPOPMAX",
    arity: -2,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_zpopmax,
    merge: None,
};
pub static CMD_ZRANGEBYLEX: Command = Command {
    name: "ZRANGEBYLEX",
    arity: -4,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_zrangebylex,
    merge: None,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DbSlice;

    fn b_args(a: &[&str]) -> Vec<Vec<u8>> {
        a.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn dispatch_at(db: &mut DbSlice, now_ms: u64, argv: &[Vec<u8>]) -> CmdResult {
        let (exec, first_key_idx, owned): (fn(&mut OpContext) -> CmdResult, usize, Vec<usize>) =
            match argv[0].to_ascii_uppercase().as_slice() {
                b"ZADD" => (exec_zadd, 1, vec![1]),
                b"ZRANGE" => (exec_zrange, 1, vec![1]),
                b"ZRANGESTORE" => (exec_zrangestore, 1, vec![1, 2]),
                b"ZINTERCARD" => (exec_zintercard, 2, vec![2, 3, 4]),
                _ => panic!("unhandled command {:?}", argv[0]),
            };
        let mut ctx = OpContext { db, args: argv, owned_keys: &owned, first_key_idx, now_ms };
        let r = (exec)(&mut ctx);
        // Apply deferred stores so STORE results are visible to later commands.
        match r {
            CmdResult::DeferredStore { key, value, reply } => {
                apply_store(db, key, value);
                CmdResult::Ok(reply)
            }
            CmdResult::DeferredStores { stores, reply } => {
                for (key, value, _exp, _sticky) in stores {
                    apply_store(db, key, value);
                }
                CmdResult::Ok(reply)
            }
            other => other,
        }
    }

    fn apply_store(db: &mut DbSlice, key: Vec<u8>, value: Option<PrimeValue>) {
        match value {
            Some(v) => db.insert(CompactString::from_bytes(&key), v),
            None => {
                db.remove(&key);
            }
        }
    }

    fn int(r: CmdResult) -> i64 {
        match r.into_resp_value() {
            RespValue::Integer(v) => v,
            o => panic!("expected integer, got {o:?}"),
        }
    }

    fn err(r: CmdResult) -> String {
        match r.into_resp_value() {
            RespValue::Error(e) => e,
            o => panic!("expected error, got {o:?}"),
        }
    }

    /// Flat array of Bulk values.
    fn flat(r: CmdResult) -> Vec<String> {
        match r.into_resp_value() {
            RespValue::Array(v) => v
                .into_iter()
                .map(|x| match x {
                    RespValue::Bulk(b) => String::from_utf8_lossy(&b).into_owned(),
                    o => panic!("unexpected element {o:?}"),
                })
                .collect(),
            o => panic!("expected array, got {o:?}"),
        }
    }

    fn add(db: &mut DbSlice, key: &str, members: &[(&str, &str)]) -> i64 {
        let mut argv = vec![b"ZADD".to_vec(), key.as_bytes().to_vec()];
        for (s, m) in members {
            argv.push(s.as_bytes().to_vec());
            argv.push(m.as_bytes().to_vec());
        }
        int(dispatch_at(db, 0, &argv))
    }

    fn range(db: &mut DbSlice, key: &str) -> Vec<String> {
        flat(dispatch_at(db, 0, &b_args(&["ZRANGE", key, "0", "-1"])))
    }

    #[test]
    fn zrangestore_basic() {
        let mut db = DbSlice::new(0);
        assert_eq!(add(&mut db, "src", &[("1", "a"), ("2", "b"), ("3", "c")]), 3);
        assert_eq!(int(dispatch_at(&mut db, 0, &b_args(&["ZRANGESTORE", "dest", "src", "0", "-1"]))), 3);
        assert_eq!(range(&mut db, "dest"), ["a", "b", "c"]);
        // Partial rank range.
        assert_eq!(int(dispatch_at(&mut db, 0, &b_args(&["ZRANGESTORE", "p", "src", "1", "2"]))), 2);
        assert_eq!(range(&mut db, "p"), ["b", "c"]);
        // REV selects the tail; the stored zset is still score-ordered.
        assert_eq!(int(dispatch_at(&mut db, 0, &b_args(&["ZRANGESTORE", "r", "src", "0", "-1", "REV", "LIMIT", "0", "2"]))), 2);
        assert_eq!(range(&mut db, "r"), ["b", "c"]);
    }

    #[test]
    fn zrangestore_preserves_scores() {
        let mut db = DbSlice::new(0);
        add(&mut db, "src", &[("1", "a"), ("2", "b"), ("3", "c")]);
        dispatch_at(&mut db, 0, &b_args(&["ZRANGESTORE", "dest", "src", "0", "-1"]));
        match db.find(b"dest", 0) {
            Some(PrimeValue::ZSet(z)) => assert_eq!(z.score(b"a"), Some(1.0)),
            o => panic!("expected zset, got {o:?}"),
        }
    }

    #[test]
    fn zrangestore_empty_removes_dest() {
        let mut db = DbSlice::new(0);
        add(&mut db, "src", &[("1", "a"), ("2", "b")]);
        assert_eq!(int(dispatch_at(&mut db, 0, &b_args(&["ZRANGESTORE", "dest", "src", "0", "-1"]))), 2);
        // Missing source empties the destination.
        assert_eq!(int(dispatch_at(&mut db, 0, &b_args(&["ZRANGESTORE", "dest", "nope", "0", "-1"]))), 0);
        assert!(db.find(b"dest", 0).is_none());
        // Empty range empties the destination too.
        assert_eq!(int(dispatch_at(&mut db, 0, &b_args(&["ZRANGESTORE", "dest", "src", "5", "9"]))), 0);
        assert!(db.find(b"dest", 0).is_none());
    }

    #[test]
    fn zrangestore_byscore_bylex() {
        let mut db = DbSlice::new(0);
        add(&mut db, "src", &[("1", "a"), ("2", "b"), ("3", "c")]);
        assert_eq!(int(dispatch_at(&mut db, 0, &b_args(&["ZRANGESTORE", "d", "src", "(1", "2", "BYSCORE"]))), 1);
        assert_eq!(range(&mut db, "d"), ["b"]);
        assert_eq!(int(dispatch_at(&mut db, 0, &b_args(&["ZRANGESTORE", "e", "src", "[a", "[c", "BYLEX"]))), 3);
        assert_eq!(range(&mut db, "e"), ["a", "b", "c"]);
        assert_eq!(int(dispatch_at(&mut db, 0, &b_args(&["ZRANGESTORE", "f", "src", "(a", "(c", "BYLEX"]))), 1);
        assert_eq!(range(&mut db, "f"), ["b"]);
    }

    #[test]
    fn zrangestore_errors() {
        let mut db = DbSlice::new(0);
        add(&mut db, "src", &[("1", "a")]);
        // Wrong type on the source.
        db.insert(CompactString::from_bytes(b"s"), PrimeValue::Str(CompactString::from("x")));
        assert!(err(dispatch_at(&mut db, 0, &b_args(&["ZRANGESTORE", "dest", "s", "0", "-1"]))).contains("WRONGTYPE"));
        // Non-integer rank bounds.
        assert!(err(dispatch_at(&mut db, 0, &b_args(&["ZRANGESTORE", "dest", "src", "abc", "def"]))).contains("not an integer"));
        // Unknown option.
        assert!(err(dispatch_at(&mut db, 0, &b_args(&["ZRANGESTORE", "dest", "src", "0", "-1", "FOO"]))).contains("syntax error"));
    }

    #[test]
    fn merge_zrangestore_picks_src_shard() {
        let args = b_args(&["ZRANGESTORE", "dest", "src", "0", "-1"]);
        let keys = [1usize, 2];
        // Dest on shard 0 contributes nothing; src on shard 1 computes the store.
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: vec![1],
                result: CmdResult::Ok(RespValue::Array(vec![])),
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: vec![2],
                result: CmdResult::deferred_store(
                    b"dest".to_vec(),
                    Some(PrimeValue::ZSet({
                        let mut z = ZSet::new();
                        z.insert(CompactString::from("a"), 1.0);
                        z.insert(CompactString::from("b"), 2.0);
                        z
                    })),
                    integer(2),
                ),
            },
        ];
        let r = merge_zrangestore(&parts, &args, &keys, 0);
        match r {
            CmdResult::DeferredStore { key, value, reply } => {
                assert_eq!(key, b"dest");
                assert_eq!(reply, RespValue::Integer(2));
                match value {
                    Some(PrimeValue::ZSet(z)) => assert_eq!(z.len(), 2),
                    o => panic!("expected zset, got {o:?}"),
                }
            }
            o => panic!("expected DeferredStore, got {o:?}"),
        }
    }

    #[test]
    fn merge_zrangestore_propagates_error() {
        let args = b_args(&["ZRANGESTORE", "dest", "src", "0", "-1"]);
        let keys = [1usize, 2];
        let parts = [
            ShardPart { shard: 0, owned_key_idxs: vec![1], result: CmdResult::Ok(RespValue::Array(vec![])) },
            ShardPart { shard: 1, owned_key_idxs: vec![2], result: CmdResult::Err(RespError::wrong_type()) },
        ];
        assert!(err(merge_zrangestore(&parts, &args, &keys, 0)).contains("WRONGTYPE"));
    }

    /// Dispatch `exec_zintercard` with `owned` as the owned key indices.
    fn intercard_at(db: &mut DbSlice, argv: &[Vec<u8>], owned: Vec<usize>) -> CmdResult {
        let mut ctx = OpContext { db, args: argv, owned_keys: &owned, first_key_idx: 2, now_ms: 0 };
        exec_zintercard(&mut ctx)
    }

    #[test]
    fn zintercard_basic() {
        let mut db = DbSlice::new(0);
        add(&mut db, "z1", &[("1", "a"), ("2", "b"), ("3", "c")]);
        add(&mut db, "z2", &[("2", "b"), ("3", "c"), ("4", "d")]);
        assert_eq!(int(intercard_at(&mut db, &b_args(&["ZINTERCARD", "2", "z1", "z2"]), vec![2, 3])), 2);
        // LIMIT caps the reply.
        assert_eq!(int(intercard_at(&mut db, &b_args(&["ZINTERCARD", "2", "z1", "z2", "LIMIT", "1"]), vec![2, 3])), 1);
        // LIMIT 0 means no cap.
        assert_eq!(int(intercard_at(&mut db, &b_args(&["ZINTERCARD", "2", "z1", "z2", "LIMIT", "0"]), vec![2, 3])), 2);
        // A single key is its own cardinality.
        assert_eq!(int(intercard_at(&mut db, &b_args(&["ZINTERCARD", "1", "z1"]), vec![2])), 3);
        // A missing key empties the intersection.
        assert_eq!(int(intercard_at(&mut db, &b_args(&["ZINTERCARD", "2", "z1", "nope"]), vec![2, 3])), 0);
    }

    #[test]
    fn zintercard_with_sets() {
        let mut db = DbSlice::new(0);
        add(&mut db, "z", &[("1", "a"), ("2", "b")]);
        // Build a plain set via the Set type directly.
        let mut s = crate::core::set::Set::new();
        s.add(CompactString::from("b"));
        db.insert(CompactString::from_bytes(b"set"), PrimeValue::Set(s));
        assert_eq!(int(intercard_at(&mut db, &b_args(&["ZINTERCARD", "2", "z", "set"]), vec![2, 3])), 1);
    }

    #[test]
    fn zintercard_errors() {
        let mut db = DbSlice::new(0);
        add(&mut db, "z1", &[("1", "a")]);
        assert!(err(intercard_at(&mut db, &b_args(&["ZINTERCARD", "0", "z1"]), vec![])).contains("at least 1 input key"));
        assert!(err(intercard_at(&mut db, &b_args(&["ZINTERCARD", "-1", "z1"]), vec![])).contains("not an integer"));
        assert!(err(intercard_at(&mut db, &b_args(&["ZINTERCARD", "abc", "z1"]), vec![])).contains("not an integer"));
        assert!(err(intercard_at(&mut db, &b_args(&["ZINTERCARD", "2", "z1"]), vec![])).contains("syntax error"));
        assert!(err(intercard_at(&mut db, &b_args(&["ZINTERCARD", "1", "z1", "LIMIT", "-1"]), vec![2])).contains("limit value is not a positive integer"));
        assert!(err(intercard_at(&mut db, &b_args(&["ZINTERCARD", "1", "z1", "LIMIT", "abc"]), vec![2])).contains("limit value is not a positive integer"));
        assert!(err(intercard_at(&mut db, &b_args(&["ZINTERCARD", "1", "z1", "FOO"]), vec![2])).contains("syntax error"));
        db.insert(CompactString::from_bytes(b"s"), PrimeValue::Str(CompactString::from("x")));
        assert!(err(intercard_at(&mut db, &b_args(&["ZINTERCARD", "1", "s"]), vec![2])).contains("WRONGTYPE"));
    }

    #[test]
    fn merge_zintercard_basic() {
        let args = b_args(&["ZINTERCARD", "3", "z1", "z2", "z3"]);
        let keys = [2usize, 3, 4];
        let parts = [
            ShardPart { shard: 0, owned_key_idxs: vec![2], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"a"), bulk(b"b")])) },
            ShardPart { shard: 1, owned_key_idxs: vec![3], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"b"), bulk(b"c")])) },
            ShardPart { shard: 2, owned_key_idxs: vec![4], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"b")])) },
        ];
        assert_eq!(int(merge_zintercard(&parts, &args, &keys, 0)), 1);
    }

    #[test]
    fn merge_zintercard_empty_partial() {
        let args = b_args(&["ZINTERCARD", "2", "z1", "z2"]);
        let keys = [2usize, 3];
        let parts = [
            ShardPart { shard: 0, owned_key_idxs: vec![2], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"a")])) },
            ShardPart { shard: 1, owned_key_idxs: vec![3], result: CmdResult::Ok(RespValue::Array(vec![])) },
        ];
        assert_eq!(int(merge_zintercard(&parts, &args, &keys, 0)), 0);
    }

    #[test]
    fn merge_zintercard_limit() {
        let args = b_args(&["ZINTERCARD", "2", "z1", "z2", "LIMIT", "1"]);
        let keys = [2usize, 3];
        let parts = [
            ShardPart { shard: 0, owned_key_idxs: vec![2], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"a"), bulk(b"b")])) },
            ShardPart { shard: 1, owned_key_idxs: vec![3], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"b"), bulk(b"a")])) },
        ];
        assert_eq!(int(merge_zintercard(&parts, &args, &keys, 0)), 1);
    }

    #[test]
    fn merge_zintercard_propagates_error() {
        let args = b_args(&["ZINTERCARD", "2", "z1", "z2"]);
        let keys = [2usize, 3];
        let parts = [
            ShardPart { shard: 0, owned_key_idxs: vec![2], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"a")])) },
            ShardPart { shard: 1, owned_key_idxs: vec![3], result: CmdResult::Err(RespError::wrong_type()) },
        ];
        assert!(err(merge_zintercard(&parts, &args, &keys, 0)).contains("WRONGTYPE"));
    }

    /// Dispatch a set-operation command (union/inter/diff, read or store) with
    /// `owned` as the owned key indices. Applies deferred stores for the
    /// multi-shard store path.
    fn setop(db: &mut DbSlice, argv: &[Vec<u8>], owned: Vec<usize>) -> CmdResult {
        let (exec, first_key_idx): (fn(&mut OpContext) -> CmdResult, usize) =
            match argv[0].to_ascii_uppercase().as_slice() {
                b"ZUNION" => (exec_zunion, 2),
                b"ZINTER" => (exec_zinter, 2),
                b"ZDIFF" => (exec_zdiff_cmd, 2),
                b"ZUNIONSTORE" => (exec_zunionstore, 1),
                b"ZINTERSTORE" => (exec_zinterstore, 1),
                b"ZDIFFSTORE" => (exec_zdiffstore, 1),
                _ => panic!("unhandled command {:?}", argv[0]),
            };
        let mut ctx = OpContext { db, args: argv, owned_keys: &owned, first_key_idx, now_ms: 0 };
        let r = (exec)(&mut ctx);
        match r {
            CmdResult::DeferredStore { key, value, reply } => {
                apply_store(db, key, value);
                CmdResult::Ok(reply)
            }
            other => other,
        }
    }

    #[test]
    fn zunion_basic() {
        let mut db = DbSlice::new(0);
        add(&mut db, "z1", &[("1", "a"), ("2", "b")]);
        add(&mut db, "z2", &[("3", "b"), ("4", "c")]);
        let owned = vec![2, 3];
        assert_eq!(flat(setop(&mut db, &b_args(&["ZUNION", "2", "z1", "z2"]), owned.clone())), ["a", "c", "b"]);
        assert_eq!(flat(setop(&mut db, &b_args(&["ZUNION", "2", "z1", "z2", "WITHSCORES"]), owned.clone())), ["a", "1", "c", "4", "b", "5"]);
    }

    #[test]
    fn zunion_weights_aggregate() {
        let mut db = DbSlice::new(0);
        add(&mut db, "z1", &[("1", "a"), ("2", "b")]);
        add(&mut db, "z2", &[("3", "b"), ("4", "c")]);
        let owned = vec![2, 3];
        assert_eq!(flat(setop(&mut db, &b_args(&["ZUNION", "2", "z1", "z2", "WEIGHTS", "2", "3"]), owned.clone())), ["a", "c", "b"]);
        assert_eq!(flat(setop(&mut db, &b_args(&["ZUNION", "2", "z1", "z2", "WEIGHTS", "2", "3", "AGGREGATE", "MIN"]), owned.clone())), ["a", "b", "c"]);
        assert_eq!(flat(setop(&mut db, &b_args(&["ZUNION", "2", "z1", "z2", "AGGREGATE", "MAX"]), owned.clone())), ["a", "b", "c"]);
        // WITHSCORES after other options, score ordering with ties broken lexically.
        assert_eq!(flat(setop(&mut db, &b_args(&["ZUNION", "2", "z1", "z2", "AGGREGATE", "MIN", "WITHSCORES"]), owned)), ["a", "1", "b", "2", "c", "4"]);
    }

    #[test]
    fn zunion_with_set_and_missing() {
        let mut db = DbSlice::new(0);
        add(&mut db, "z1", &[("1", "a"), ("2", "b")]);
        let mut s = crate::core::set::Set::new();
        s.add(CompactString::from("b"));
        s.add(CompactString::from("c"));
        db.insert(CompactString::from_bytes(b"set"), PrimeValue::Set(s));
        // A set contributes score 1 per member.
        assert_eq!(flat(setop(&mut db, &b_args(&["ZUNION", "2", "z1", "set"]), vec![2, 3])), ["a", "c", "b"]);
        // A missing key contributes nothing.
        assert_eq!(flat(setop(&mut db, &b_args(&["ZUNION", "2", "z1", "nope"]), vec![2, 3])), ["a", "b"]);
    }

    #[test]
    fn zinter_basic() {
        let mut db = DbSlice::new(0);
        add(&mut db, "z1", &[("1", "a"), ("2", "b")]);
        add(&mut db, "z2", &[("3", "b"), ("4", "c")]);
        let owned = vec![2, 3];
        assert_eq!(flat(setop(&mut db, &b_args(&["ZINTER", "2", "z1", "z2"]), owned.clone())), ["b"]);
        assert_eq!(flat(setop(&mut db, &b_args(&["ZINTER", "2", "z1", "z2", "WITHSCORES"]), owned.clone())), ["b", "5"]);
        assert_eq!(flat(setop(&mut db, &b_args(&["ZINTER", "2", "z1", "z2", "WEIGHTS", "2", "3"]), owned.clone())), ["b"]);
        assert_eq!(flat(setop(&mut db, &b_args(&["ZINTER", "2", "z1", "z2", "WEIGHTS", "2", "3", "WITHSCORES"]), owned.clone())), ["b", "13"]);
        assert_eq!(flat(setop(&mut db, &b_args(&["ZINTER", "2", "z1", "z2", "WEIGHTS", "2", "3", "AGGREGATE", "MIN", "WITHSCORES"]), owned.clone())), ["b", "4"]);
        // A missing key empties the intersection.
        assert_eq!(flat(setop(&mut db, &b_args(&["ZINTER", "2", "z1", "nope"]), owned)), Vec::<String>::new());
    }

    #[test]
    fn zunion_store() {
        let mut db = DbSlice::new(0);
        add(&mut db, "z1", &[("1", "a"), ("2", "b")]);
        add(&mut db, "z2", &[("3", "b"), ("4", "c")]);
        let owned = vec![1, 3, 4];
        assert_eq!(int(setop(&mut db, &b_args(&["ZUNIONSTORE", "d", "2", "z1", "z2"]), owned.clone())), 3);
        assert_eq!(range(&mut db, "d"), ["a", "c", "b"]);
        assert_eq!(int(setop(&mut db, &b_args(&["ZUNIONSTORE", "d", "2", "z1", "z2", "WEIGHTS", "2", "3", "AGGREGATE", "MIN"]), owned.clone())), 3);
        assert_eq!(range(&mut db, "d"), ["a", "b", "c"]);
        // An all-missing union removes the destination.
        assert_eq!(int(setop(&mut db, &b_args(&["ZUNIONSTORE", "d", "1", "nope"]), vec![1, 3])), 0);
        assert!(db.find(b"d", 0).is_none());
    }

    #[test]
    fn zinter_store() {
        let mut db = DbSlice::new(0);
        add(&mut db, "z1", &[("1", "a"), ("2", "b")]);
        add(&mut db, "z2", &[("3", "b"), ("4", "c")]);
        assert_eq!(int(setop(&mut db, &b_args(&["ZINTERSTORE", "d", "2", "z1", "z2"]), vec![1, 3, 4])), 1);
        assert_eq!(range(&mut db, "d"), ["b"]);
        // An empty intersection removes the destination.
        assert_eq!(int(setop(&mut db, &b_args(&["ZINTERSTORE", "d", "2", "z1", "nope"]), vec![1, 3, 4])), 0);
        assert!(db.find(b"d", 0).is_none());
    }

    #[test]
    fn zdiff_basic() {
        let mut db = DbSlice::new(0);
        add(&mut db, "z1", &[("1", "a"), ("2", "b"), ("3", "c")]);
        add(&mut db, "z2", &[("4", "b")]);
        assert_eq!(flat(setop(&mut db, &b_args(&["ZDIFF", "2", "z1", "z2"]), vec![2, 3])), ["a", "c"]);
        // Result scores come from the base set only.
        assert_eq!(flat(setop(&mut db, &b_args(&["ZDIFF", "2", "z1", "z2", "WITHSCORES"]), vec![2, 3])), ["a", "1", "c", "3"]);
        // A missing other set changes nothing.
        assert_eq!(flat(setop(&mut db, &b_args(&["ZDIFF", "2", "z1", "nope"]), vec![2, 3])), ["a", "b", "c"]);
        // A missing base set yields an empty result.
        assert_eq!(flat(setop(&mut db, &b_args(&["ZDIFF", "2", "nope", "z2"]), vec![2, 3])), Vec::<String>::new());
    }

    #[test]
    fn zdiffstore_basic() {
        let mut db = DbSlice::new(0);
        add(&mut db, "z1", &[("1", "a"), ("2", "b"), ("3", "c")]);
        add(&mut db, "z2", &[("4", "b")]);
        assert_eq!(int(setop(&mut db, &b_args(&["ZDIFFSTORE", "d", "2", "z1", "z2"]), vec![1, 3, 4])), 2);
        assert_eq!(range(&mut db, "d"), ["a", "c"]);
        // Empty diff removes the destination.
        assert_eq!(int(setop(&mut db, &b_args(&["ZDIFFSTORE", "d", "1", "nope"]), vec![1, 3])), 0);
        assert!(db.find(b"d", 0).is_none());
    }

    #[test]
    fn zsetop_errors() {
        let mut db = DbSlice::new(0);
        add(&mut db, "z1", &[("1", "a")]);
        assert!(err(setop(&mut db, &b_args(&["ZUNION", "0", "z1"]), vec![])).contains("at least 1 input key is needed for zunion"));
        assert!(err(setop(&mut db, &b_args(&["ZUNION", "-1", "z1"]), vec![])).contains("not an integer"));
        assert!(err(setop(&mut db, &b_args(&["ZUNION", "abc", "z1"]), vec![])).contains("not an integer"));
        assert!(err(setop(&mut db, &b_args(&["ZUNION", "2", "z1"]), vec![])).contains("syntax error"));
        assert!(err(setop(&mut db, &b_args(&["ZUNION", "1", "z1", "WEIGHTS", "2", "3"]), vec![])).contains("syntax error"));
        assert!(err(setop(&mut db, &b_args(&["ZUNION", "1", "z1", "WEIGHTS", "abc"]), vec![])).contains("weight value is not a float"));
        assert!(err(setop(&mut db, &b_args(&["ZUNION", "1", "z1", "AGGREGATE", "foo"]), vec![])).contains("syntax error"));
        assert!(err(setop(&mut db, &b_args(&["ZUNION", "1", "z1", "FOO"]), vec![])).contains("syntax error"));
        assert!(err(setop(&mut db, &b_args(&["ZUNIONSTORE", "d", "1", "z1", "WITHSCORES"]), vec![])).contains("syntax error"));
        assert!(err(setop(&mut db, &b_args(&["ZINTERSTORE", "d", "1", "z1", "WEIGHTS"]), vec![])).contains("syntax error"));
        // ZDIFF argument validation.
        assert!(err(setop(&mut db, &b_args(&["ZDIFF", "0", "z1"]), vec![])).contains("at least 1 input key is needed for zdiff"));
        assert!(err(setop(&mut db, &b_args(&["ZDIFF", "2", "z1"]), vec![])).contains("syntax error"));
        assert!(err(setop(&mut db, &b_args(&["ZDIFF", "2", "z1", "z1", "FOO"]), vec![])).contains("syntax error"));
        assert!(err(setop(&mut db, &b_args(&["ZDIFF", "1", "z1", "WITHSCORES", "extra"]), vec![])).contains("syntax error"));
        assert!(err(setop(&mut db, &b_args(&["ZDIFFSTORE", "d", "1", "z1", "WITHSCORES"]), vec![])).contains("syntax error"));
        // Wrong types are reported on any source.
        db.insert(CompactString::from_bytes(b"s"), PrimeValue::Str(CompactString::from("x")));
        assert!(err(setop(&mut db, &b_args(&["ZUNION", "2", "z1", "s"]), vec![2, 3])).contains("WRONGTYPE"));
        assert!(err(setop(&mut db, &b_args(&["ZINTER", "2", "z1", "s"]), vec![2, 3])).contains("WRONGTYPE"));
        assert!(err(setop(&mut db, &b_args(&["ZDIFF", "2", "z1", "s"]), vec![2, 3])).contains("WRONGTYPE"));
        assert!(err(setop(&mut db, &b_args(&["ZDIFF", "2", "s", "z1"]), vec![2, 3])).contains("WRONGTYPE"));
    }

    #[test]
    fn merge_zunion_aggregates_across_shards() {
        let args = b_args(&["ZUNION", "3", "z1", "z2", "z3", "AGGREGATE", "MIN", "WITHSCORES"]);
        let keys = [2usize, 3, 4];
        let parts = [
            ShardPart { shard: 0, owned_key_idxs: vec![2], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"a"), bulk(b"1"), bulk(b"b"), bulk(b"2")])) },
            ShardPart { shard: 1, owned_key_idxs: vec![3], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"b"), bulk(b"3"), bulk(b"c"), bulk(b"4")])) },
            ShardPart { shard: 2, owned_key_idxs: vec![4], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"a"), bulk(b"5")])) },
        ];
        assert_eq!(flat(merge_zunion(&parts, &args, &keys, 0)), ["a", "1", "b", "2", "c", "4"]);
    }

    #[test]
    fn merge_zinter_aggregates_across_shards() {
        let args = b_args(&["ZINTER", "2", "z1", "z2", "WITHSCORES"]);
        let keys = [2usize, 3];
        let parts = [
            ShardPart { shard: 0, owned_key_idxs: vec![2], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"a"), bulk(b"1"), bulk(b"b"), bulk(b"2")])) },
            ShardPart { shard: 1, owned_key_idxs: vec![3], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"b"), bulk(b"3"), bulk(b"c"), bulk(b"4")])) },
        ];
        assert_eq!(flat(merge_zinter(&parts, &args, &keys, 0)), ["b", "5"]);
        // An empty partial short-circuits the intersection.
        let parts = [
            ShardPart { shard: 0, owned_key_idxs: vec![2], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"a")])) },
            ShardPart { shard: 1, owned_key_idxs: vec![3], result: CmdResult::Ok(RespValue::Array(vec![])) },
        ];
        assert_eq!(flat(merge_zinter(&parts, &args, &keys, 0)), Vec::<String>::new());
    }

    #[test]
    fn merge_zsetop_stores() {
        let args = b_args(&["ZUNIONSTORE", "d", "2", "z1", "z2"]);
        let keys = [1usize, 3, 4];
        let parts = [
            ShardPart { shard: 0, owned_key_idxs: vec![1], result: CmdResult::Ok(RespValue::Nil) },
            ShardPart { shard: 1, owned_key_idxs: vec![3], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"a"), bulk(b"1")])) },
            ShardPart { shard: 2, owned_key_idxs: vec![4], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"b"), bulk(b"2")])) },
        ];
        match merge_zunionstore(&parts, &args, &keys, 0) {
            CmdResult::DeferredStore { key, value, reply } => {
                assert_eq!(key, b"d");
                assert_eq!(reply, RespValue::Integer(2));
                match value {
                    Some(PrimeValue::ZSet(z)) => {
                        assert_eq!(z.len(), 2);
                        assert_eq!(z.score(b"b"), Some(2.0));
                    }
                    o => panic!("expected zset, got {o:?}"),
                }
            }
            o => panic!("expected DeferredStore, got {o:?}"),
        }

        // An empty intersection deletes the destination.
        let args = b_args(&["ZINTERSTORE", "d", "2", "z1", "z2"]);
        let parts = [
            ShardPart { shard: 0, owned_key_idxs: vec![1], result: CmdResult::Ok(RespValue::Nil) },
            ShardPart { shard: 1, owned_key_idxs: vec![3], result: CmdResult::Ok(RespValue::Array(vec![])) },
        ];
        match merge_zinterstore(&parts, &args, &keys, 0) {
            CmdResult::DeferredStore { key, value, reply } => {
                assert_eq!(key, b"d");
                assert_eq!(reply, RespValue::Integer(0));
                assert!(value.is_none());
            }
            o => panic!("expected DeferredStore, got {o:?}"),
        }
    }

    #[test]
    fn merge_zdiff_base_shard() {
        let args = b_args(&["ZDIFF", "3", "z1", "z2", "z3", "WITHSCORES"]);
        let keys = [2usize, 3, 4];
        // The base shard also owns a second source; its first map is the base.
        let parts = [
            ShardPart { shard: 0, owned_key_idxs: vec![2, 3], result: CmdResult::Ok(RespValue::Array(vec![
                RespValue::Array(vec![bulk(b"a"), bulk(b"1"), bulk(b"b"), bulk(b"2"), bulk(b"c"), bulk(b"3")]),
                RespValue::Array(vec![bulk(b"b"), bulk(b"9")]),
            ])) },
            ShardPart { shard: 1, owned_key_idxs: vec![4], result: CmdResult::Ok(RespValue::Array(vec![
                RespValue::Array(vec![bulk(b"c"), bulk(b"8")]),
            ])) },
        ];
        assert_eq!(flat(merge_zdiff(&parts, &args, &keys, 0)), ["a", "1"]);

        // STORE variant writes the diff to the destination.
        let args = b_args(&["ZDIFFSTORE", "d", "2", "z1", "z2"]);
        let keys = [1usize, 3, 4];
        let parts = [
            ShardPart { shard: 0, owned_key_idxs: vec![1], result: CmdResult::Ok(RespValue::Nil) },
            ShardPart { shard: 1, owned_key_idxs: vec![3], result: CmdResult::Ok(RespValue::Array(vec![
                RespValue::Array(vec![bulk(b"a"), bulk(b"1"), bulk(b"b"), bulk(b"2")]),
            ])) },
            ShardPart { shard: 2, owned_key_idxs: vec![4], result: CmdResult::Ok(RespValue::Array(vec![
                RespValue::Array(vec![bulk(b"b"), bulk(b"5")]),
            ])) },
        ];
        match merge_zdiffstore(&parts, &args, &keys, 0) {
            CmdResult::DeferredStore { key, value, reply } => {
                assert_eq!(key, b"d");
                assert_eq!(reply, RespValue::Integer(1));
                match value {
                    Some(PrimeValue::ZSet(z)) => {
                        assert_eq!(z.len(), 1);
                        assert_eq!(z.score(b"a"), Some(1.0));
                    }
                    o => panic!("expected zset, got {o:?}"),
                }
            }
            o => panic!("expected DeferredStore, got {o:?}"),
        }
    }

    #[test]
    fn merge_zsetop_propagates_error() {
        let args = b_args(&["ZUNION", "2", "z1", "z2"]);
        let keys = [2usize, 3];
        let parts = [
            ShardPart { shard: 0, owned_key_idxs: vec![2], result: CmdResult::Ok(RespValue::Array(vec![bulk(b"a"), bulk(b"1")])) },
            ShardPart { shard: 1, owned_key_idxs: vec![3], result: CmdResult::Err(RespError::wrong_type()) },
        ];
        assert!(err(merge_zunion(&parts, &args, &keys, 0)).contains("WRONGTYPE"));
        assert!(err(merge_zdiff(&parts, &args, &keys, 0)).contains("WRONGTYPE"));

        let store_args = b_args(&["ZINTERSTORE", "d", "2", "z1", "z2"]);
        let store_keys = [1usize, 3, 4];
        assert!(err(merge_zinterstore(&parts, &store_args, &store_keys, 0)).contains("WRONGTYPE"));
        assert!(err(merge_zdiffstore(&parts, &store_args, &store_keys, 0)).contains("WRONGTYPE"));
    }
}
