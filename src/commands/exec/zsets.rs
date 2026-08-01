use crate::commands::{bulk, integer, Command, OpContext, KeyRange, FLAG_DENYOOM, FLAG_FAST, FLAG_READONLY, FLAG_WRITE};
use crate::core::compact::CompactString;
use crate::core::zset::ZSet;
use crate::core::PrimeValue;
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::{format_double, parse_double, parse_i64, redis_range};

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

fn exec_zrange(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let opts = match parse_range_opts(ctx.args, key_idx + 3) {
        Ok(o) => o,
        Err(e) => return CmdResult::Err(e),
    };
    if opts.withscores && (opts.byscore || opts.bylex) {
        // allowed in Redis 6.2+; keep it simple by ignoring the conflict
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => {
            let items = if opts.byscore {
                let min = match parse_score_bound(&ctx.args[key_idx + 1]) {
                    Ok(v) => v,
                    Err(e) => return CmdResult::Err(e),
                };
                let max = match parse_score_bound(&ctx.args[key_idx + 2]) {
                    Ok(v) => v,
                    Err(e) => return CmdResult::Err(e),
                };
                let (lo, hi) = if opts.rev { (max, min) } else { (min, max) };
                z.range_by_score_filtered(|score| {
                    score_in_range(score, lo.0, lo.1, true) && score_in_range(score, hi.0, hi.1, false)
                }, opts.rev, opts.limit)
            } else if opts.bylex {
                let min = ctx.args[key_idx + 1].as_slice();
                let max = ctx.args[key_idx + 2].as_slice();
                let (lo, lo_incl, hi, hi_incl) = match parse_lex_range(min, max) {
                    Ok(v) => v,
                    Err(e) => return CmdResult::Err(e),
                };
                z.range_by_member_filtered(|m| {
                    (if lo.is_empty() { true } else if lo_incl { m.as_bytes() >= lo.as_slice() } else { m.as_bytes() > lo.as_slice() })
                        && (if hi.is_empty() { true } else if hi_incl { m.as_bytes() <= hi.as_slice() } else { m.as_bytes() < hi.as_slice() })
                }, opts.rev, opts.limit)
            } else {
                let start = match parse_i64(&ctx.args[key_idx + 1]) {
                    Some(v) => v,
                    None => return CmdResult::Err(RespError::integer()),
                };
                let stop = match parse_i64(&ctx.args[key_idx + 2]) {
                    Some(v) => v,
                    None => return CmdResult::Err(RespError::integer()),
                };
                let (s, c) = match redis_range(start, stop, z.len() as i64) {
                    Some(x) => x,
                    None => return CmdResult::Ok(RespValue::Array(vec![])),
                };
                let mut items = if opts.rev { z.rev_range(s, s + c as i64 - 1) } else { z.range(s, s + c as i64 - 1, opts.withscores) };
                if let Some((off, cnt)) = opts.limit {
                    if off < items.len() {
                        items = items.into_iter().skip(off).take(cnt).collect();
                    } else {
                        items = vec![];
                    }
                }
                items
            };
            CmdResult::Ok(build_range_output(items, opts.withscores))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Array(vec![])),
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
