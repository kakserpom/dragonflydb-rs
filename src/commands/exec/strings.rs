use crate::commands::{
    Command, FLAG_DENYOOM, FLAG_FAST, FLAG_MULTI_KEY, FLAG_READONLY, FLAG_WRITE, KeyRange,
    OpContext, ShardPart, bulk, integer, ok,
};
use crate::core::PrimeValue;
use crate::core::compact::CompactString;
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::{format_double, parse_i64, parse_u64, redis_range};
use xxhash_rust::xxh3::xxh3_64;

// ---------------------------------------------------------------------------
// SET
// ---------------------------------------------------------------------------

const K_MAX_EXPIRE_DEADLINE_MS: i64 = 268_435_455_000; // kMaxExpireDeadlineSec * 1000

fn invalid_expire(cmd: &str) -> RespError {
    RespError::new(format!("ERR invalid expire time in '{cmd}' command"))
}

/// Absolute expiry (ms) for a relative (`Rel`) or absolute (`Abs`) SET expiry
/// option, mirroring `DbSlice::ExpireParams` overflow semantics: a relative
/// value that overflows `now_ms + ms` is an error.
#[derive(Debug, Clone, Copy)]
enum SetExpire {
    Rel(i64),
    Abs(i64),
}

fn parse_set_args(args: &[Vec<u8>]) -> Result<SetOpts, RespError> {
    let mut opts = SetOpts::default();
    let mut i = 3;
    while i < args.len() {
        let t = args[i].to_ascii_uppercase();
        match t.as_slice() {
            b"NX" => opts.nx = true,
            b"XX" => opts.xx = true,
            b"KEEPTTL" => opts.keepttl = true,
            b"GET" => opts.get = true,
            b"STICK" => opts.stick = true,
            b"EX" | b"PX" | b"EXAT" | b"PXAT" => {
                if i + 1 >= args.len() {
                    return Err(RespError::syntax());
                }
                // "We can set expiry only once": a second expiry option, in any
                // combination, is a syntax error (`CmdSet`).
                if opts.expire.is_some() {
                    return Err(RespError::syntax());
                }
                let n = parse_i64(&args[i + 1]).ok_or_else(RespError::integer)?;
                if n <= 0 {
                    return Err(invalid_expire("set"));
                }
                let sec = t.as_slice() == b"EX" || t.as_slice() == b"EXAT";
                let abs = t.as_slice() == b"EXAT" || t.as_slice() == b"PXAT";
                let ms = if sec {
                    if n > i64::MAX / 1000 {
                        return Err(invalid_expire("set"));
                    }
                    n * 1000
                } else {
                    n
                };
                opts.expire = Some(if abs { SetExpire::Abs(ms) } else { SetExpire::Rel(ms) });
                i += 1;
            }
            _ => return Err(RespError::syntax()),
        }
        i += 1;
    }
    if opts.nx && opts.xx {
        return Err(RespError::syntax());
    }
    if opts.keepttl && opts.expire.is_some() {
        return Err(RespError::syntax());
    }
    Ok(opts)
}

#[derive(Default)]
struct SetOpts {
    nx: bool,
    xx: bool,
    keepttl: bool,
    get: bool,
    stick: bool,
    expire: Option<SetExpire>,
}

fn exec_set(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let value = &ctx.args[key_idx + 1];
    let opts = match parse_set_args(ctx.args) {
        Ok(o) => o,
        Err(e) => return CmdResult::Err(e),
    };

    // Resolve the expiry to a relative ms offset. A relative offset that
    // overflows `now + ms` is "invalid expire time" (`ExpireParams` kOverflow);
    // an absolute offset in the past is the NegativeExpire path: delete the key
    // and reply OK without writing the new value.
    let rel_ms = match opts.expire {
        Some(SetExpire::Rel(ms)) => {
            let now = ctx.now_ms as i64;
            match now.checked_add(ms) {
                Some(_) => Some(ms),
                None => return CmdResult::Err(invalid_expire("set")),
            }
        }
        Some(SetExpire::Abs(at)) => {
            if at < ctx.now_ms as i64 {
                ctx.db.remove(key);
                return CmdResult::Ok(ok());
            }
            Some(at - ctx.now_ms as i64)
        }
        None => None,
    };

    let old = ctx.db.find(key, ctx.now_ms);
    let exists = old.is_some();
    let old_value = if opts.get {
        match old {
            Some(PrimeValue::Str(s)) => Some(s.as_bytes().to_vec()),
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => None,
        }
    } else {
        None
    };

    if opts.nx && exists {
        return CmdResult::Ok(match old_value {
            Some(v) => RespValue::Bulk(v),
            None => RespValue::Nil,
        });
    }
    if opts.xx && !exists {
        return CmdResult::Ok(match old_value {
            Some(v) => RespValue::Bulk(v),
            None => RespValue::Nil,
        });
    }

    if !opts.keepttl {
        ctx.db.clear_expiry(key);
    }
    ctx.db
        .insert(key, PrimeValue::Str(CompactString::from_bytes(value)));
    if opts.stick {
        ctx.db.set_sticky(key, ctx.now_ms);
    }
    if let Some(rel) = rel_ms {
        // Cap the relative TTL to kMaxExpireDeadlineMs (`Calculate(now, true)`).
        let rel = rel.min(K_MAX_EXPIRE_DEADLINE_MS);
        ctx.db
            .set_expiry(key, (ctx.now_ms as i64 + rel) as u64, ctx.now_ms);
    }

    // With GET the reply is always the previous value (nil when absent).
    CmdResult::Ok(match (opts.get, old_value) {
        (true, Some(v)) => RespValue::Bulk(v),
        (true, None) => RespValue::Nil,
        (false, _) => RespValue::Simple("OK".into()),
    })
}

// ---------------------------------------------------------------------------
// GET / GETDEL / GETSET / SETNX / SETEX / PSETEX
// ---------------------------------------------------------------------------

fn get_str(ctx: &mut OpContext, key: &[u8]) -> CmdResult {
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => CmdResult::Ok(RespValue::Bulk(s.as_bytes().to_vec())),
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Nil),
    }
}

fn exec_get(ctx: &mut OpContext) -> CmdResult {
    get_str(ctx, &ctx.args[ctx.owned_keys[0]])
}

fn exec_getdel(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => {
            let v = s.as_bytes().to_vec();
            ctx.db.remove(key);
            CmdResult::Ok(RespValue::Bulk(v))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Nil),
    }
}

fn exec_getset(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let new_val = CompactString::from_bytes(&ctx.args[key_idx + 1]);
    let old = match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => Some(s.as_bytes().to_vec()),
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => None,
    };
    ctx.db.insert(key, PrimeValue::Str(new_val));
    CmdResult::Ok(match old {
        Some(v) => RespValue::Bulk(v),
        None => RespValue::Nil,
    })
}

fn exec_setnx(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let value = CompactString::from_bytes(&ctx.args[key_idx + 1]);
    let inserted = ctx
        .db
        .insert_if_absent(key, PrimeValue::Str(value), ctx.now_ms);
    CmdResult::Ok(integer(i64::from(inserted)))
}

fn exec_setex_common(ctx: &mut OpContext, unit_ms: bool) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let cmd_name = if unit_ms { "psetex" } else { "setex" };
    let Some(ttl) = parse_i64(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::integer());
    };
    if ttl <= 0 {
        return CmdResult::Err(invalid_expire(cmd_name));
    }
    let ms = if unit_ms {
        ttl
    } else {
        if ttl > i64::MAX / 1000 {
            return CmdResult::Err(invalid_expire("set"));
        }
        ttl * 1000
    };
    // `ExpireParams` overflow: `now + ms` must fit an i64 (kOverflow -> error).
    let now = ctx.now_ms as i64;
    match now.checked_add(ms) {
        Some(_) => {}
        None => return CmdResult::Err(invalid_expire("set")),
    }
    let value = CompactString::from_bytes(&ctx.args[key_idx + 2]);
    ctx.db.insert(key, PrimeValue::Str(value));
    // Clamp the relative TTL to kMaxExpireDeadlineMs (`Calculate(now, true)`).
    let rel = ms.min(K_MAX_EXPIRE_DEADLINE_MS);
    ctx.db.set_expiry(key, (now + rel) as u64, ctx.now_ms);
    CmdResult::Ok(ok())
}

fn exec_setex(ctx: &mut OpContext) -> CmdResult {
    exec_setex_common(ctx, false)
}

fn exec_psetex(ctx: &mut OpContext) -> CmdResult {
    exec_setex_common(ctx, true)
}

// ---------------------------------------------------------------------------
// APPEND / STRLEN / GETRANGE / SETRANGE
// ---------------------------------------------------------------------------

fn exec_append(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let suffix = &ctx.args[key_idx + 1];
    let new_len = match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => {
            let mut v = s.as_bytes().to_vec();
            v.extend_from_slice(suffix);
            let len = v.len();
            *s = CompactString::from_bytes(&v);
            len
        }
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => {
            let v = suffix.clone();
            let len = v.len();
            ctx.db
                .insert(key, PrimeValue::Str(CompactString::from_bytes(&v)));
            len
        }
    };
    CmdResult::Ok(integer(new_len as i64))
}

fn exec_strlen(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => CmdResult::Ok(integer(s.len() as i64)),
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

fn exec_getrange(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let Some(start) = parse_i64(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::integer());
    };
    let Some(stop) = parse_i64(&ctx.args[key_idx + 2]) else {
        return CmdResult::Err(RespError::integer());
    };
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Str(value)) => {
            let len = value.len() as i64;
            match redis_range(start, stop, len) {
                Some((rs, rc)) => {
                    CmdResult::Ok(bulk(&value.as_bytes()[rs as usize..(rs + rc) as usize]))
                }
                None => CmdResult::Ok(bulk([])),
            }
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(bulk([])),
    }
}

// GETEX / DIGEST / PREPEND / SUBSTR / GAT
// ---------------------------------------------------------------------------

const K_GETEX_EXPIRY_ERR: &str = "ERR invalid expire time in 'getex' command";

enum GetExOpt {
    Persist,
    ExpiryAt(u64),
}

/// Parse `GETEX key [EX|PX|EXAT|PXAT n | PERSIST]`. The expiry option and
/// PERSIST are mutually exclusive (single grammar `OneOf`); a value below 1
/// is rejected at parse time (Redis GETEX requires positive values).
fn parse_getex_args(args: &[Vec<u8>], now_ms: u64) -> Result<Option<GetExOpt>, RespError> {
    match args.len() {
        2 => Ok(None),
        3 => match args[2].to_ascii_uppercase().as_slice() {
            b"PERSIST" => Ok(Some(GetExOpt::Persist)),
            _ => Err(RespError::syntax()),
        },
        4 => {
            let unit = args[2].to_ascii_uppercase();
            match unit.as_slice() {
                b"EX" | b"PX" | b"EXAT" | b"PXAT" => {
                    let n = parse_i64(&args[3]).ok_or_else(RespError::integer)?;
                    if n < 1 {
                        return Err(RespError::new(K_GETEX_EXPIRY_ERR));
                    }
                    match getex_expiry_at(unit.as_slice(), n, now_ms) {
                        Ok(at) => Ok(Some(GetExOpt::ExpiryAt(at))),
                        Err(()) => Err(RespError::out_of_range()),
                    }
                }
                _ => Err(RespError::syntax()),
            }
        }
        _ => Err(RespError::syntax()),
    }
}

/// Absolute expiry (ms) for a GETEX option, mirroring `DbSlice::ExpireParams`
/// plus `UpdateExpire`'s range checks: values that overflow or exceed the max
/// expire deadline surface `OUT_OF_RANGE` ("index out of range").
fn getex_expiry_at(unit: &[u8], value: i64, now_ms: u64) -> Result<u64, ()> {
    const K_OVERFLOW: i64 = i64::MAX;
    const K_MAX_DEADLINE_MS: i64 = 268_435_455_000;

    let is_ms = unit == b"PX" || unit == b"PXAT";
    let is_absolute = unit == b"EXAT" || unit == b"PXAT";
    let ms_value = if is_ms {
        value
    } else if value <= K_OVERFLOW / 1000 {
        value * 1000
    } else {
        K_OVERFLOW
    };
    let now = now_ms as i64;
    let abs_ms = if is_absolute {
        ms_value
    } else if K_OVERFLOW - now < ms_value {
        K_OVERFLOW
    } else {
        ms_value + now
    };
    if abs_ms == K_OVERFLOW || abs_ms - now > K_MAX_DEADLINE_MS {
        return Err(());
    }
    Ok(abs_ms as u64)
}

fn exec_getex(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let opt = match parse_getex_args(ctx.args, ctx.now_ms) {
        Ok(o) => o,
        Err(e) => return CmdResult::Err(e),
    };

    let value = match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => Some(s.as_bytes().to_vec()),
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => None,
    };
    let Some(value) = value else {
        return CmdResult::Ok(RespValue::Nil);
    };

    match opt {
        Some(GetExOpt::Persist) => ctx.db.clear_expiry(key),
        Some(GetExOpt::ExpiryAt(at)) => ctx.db.set_expiry(key, at, ctx.now_ms),
        None => {}
    }
    CmdResult::Ok(bulk(value))
}

fn exec_digest(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => CmdResult::Ok(bulk(format!("{:016x}", xxh3_64(s.as_bytes())))),
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Nil),
    }
}

fn exec_prepend(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let prefix = &ctx.args[key_idx + 1];
    let new_len = match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => {
            let mut v = prefix.clone();
            v.extend_from_slice(s.as_bytes());
            let len = v.len();
            *s = CompactString::from_bytes(&v);
            len
        }
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => {
            let len = prefix.len();
            ctx.db
                .insert(key, PrimeValue::Str(CompactString::from_bytes(prefix)));
            len
        }
    };
    CmdResult::Ok(integer(new_len as i64))
}

// GAT is a memcache-only command; over RESP it always errors.
fn exec_gat(_ctx: &mut OpContext) -> CmdResult {
    CmdResult::err("ERR GAT is a memcache-only command")
}

fn exec_setrange(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let Some(offset) = parse_i64(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::integer());
    };
    if offset < 0 {
        return CmdResult::Err(RespError::new("ERR offset is out of range"));
    }
    let val = &ctx.args[key_idx + 2];
    // An empty value is a no-op: return the current length without creating
    // or modifying the key (reference `OpSetRange` -> `OpStrLen`).
    if val.is_empty() {
        let len = match ctx.db.find(key, ctx.now_ms) {
            Some(PrimeValue::Str(s)) => s.len(),
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => 0,
        };
        return CmdResult::Ok(integer(len as i64));
    }
    let new_len = match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => {
            let mut v = s.as_bytes().to_vec();
            let end = offset as usize + val.len();
            if v.len() < end {
                v.resize(end, 0);
            }
            v[offset as usize..end].copy_from_slice(val);
            let len = v.len();
            *s = CompactString::from_bytes(&v);
            len
        }
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => {
            let mut v = vec![0u8; offset as usize + val.len()];
            v[offset as usize..].copy_from_slice(val);
            let len = v.len();
            ctx.db
                .insert(key, PrimeValue::Str(CompactString::from_bytes(&v)));
            len
        }
    };
    CmdResult::Ok(integer(new_len as i64))
}

// ---------------------------------------------------------------------------
// INCR / DECR / INCRBY / DECRBY / INCRBYFLOAT
// ---------------------------------------------------------------------------

fn incr_by(ctx: &mut OpContext, delta: i64) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let cur = match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => {
            if s.is_empty() {
                0
            } else {
                match parse_i64(s.as_bytes()) {
                    Some(v) => v,
                    None => return CmdResult::Err(RespError::integer()),
                }
            }
        }
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => 0,
    };
    let Some(new_val) = cur.checked_add(delta) else {
        return CmdResult::Err(incr_overflow());
    };
    let s = crate::util::itoa(new_val);
    ctx.db
        .insert(key, PrimeValue::Str(CompactString::from_bytes(&s)));
    CmdResult::Ok(integer(new_val))
}

fn incr_overflow() -> RespError {
    RespError::new("ERR increment or decrement would overflow")
}

fn exec_incr(ctx: &mut OpContext) -> CmdResult {
    incr_by(ctx, 1)
}
fn exec_incrby(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let Some(delta) = parse_i64(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::integer());
    };
    incr_by(ctx, delta)
}
fn exec_decr(ctx: &mut OpContext) -> CmdResult {
    incr_by(ctx, -1)
}
fn exec_decrby(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let Some(delta) = parse_i64(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::integer());
    };
    // DECRBY with INT64_MIN cannot be negated; the reference rejects it at
    // parse time (`Validated<int64_t, NotEq<INT64_MIN, kIncrOverflow>>`).
    let Some(delta) = delta.checked_neg() else {
        return CmdResult::Err(incr_overflow());
    };
    incr_by(ctx, delta)
}

/// Strict float parse matching `ParseDouble` in the reference: no leading or
/// trailing whitespace (fast_float::from_chars semantics) and no NaN.
fn parse_float_strict(s: &[u8]) -> Option<f64> {
    let t = std::str::from_utf8(s).ok()?;
    let f: f64 = t.parse().ok()?;
    if f.is_nan() {
        return None;
    }
    Some(f)
}

fn exec_incrbyfloat(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let Some(delta) = parse_float_strict(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::float());
    };
    let cur = match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => match parse_float_strict(s.as_bytes()) {
            Some(v) => v,
            None => return CmdResult::Err(RespError::float()),
        },
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => 0.0,
    };
    let new_val = cur + delta;
    if !new_val.is_finite() {
        return CmdResult::Err(RespError::new(
            "ERR increment would produce NaN or Infinity",
        ));
    }
    let s = format_double(new_val);
    ctx.db.insert(
        key,
        PrimeValue::Str(CompactString::from_bytes(s.as_bytes())),
    );
    CmdResult::Ok(RespValue::Bulk(s.into_bytes()))
}

// ---------------------------------------------------------------------------
// MSET / MSETNX / MGET
// ---------------------------------------------------------------------------

fn exec_mset(ctx: &mut OpContext) -> CmdResult {
    if !(ctx.args.len() - 1).is_multiple_of(2) {
        return CmdResult::Err(RespError::new(
            "ERR wrong number of arguments for 'mset' command",
        ));
    }
    for &ki in ctx.owned_keys {
        let key = CompactString::from_bytes(&ctx.args[ki]);
        let value = CompactString::from_bytes(&ctx.args[ki + 1]);
        ctx.db.insert(&key, PrimeValue::Str(value));
    }
    CmdResult::Ok(ok())
}

fn merge_mset(parts: &[ShardPart], _args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    for p in parts {
        if let CmdResult::Err(e) = &p.result {
            return CmdResult::Err(e.clone());
        }
    }
    CmdResult::Ok(ok())
}

fn exec_msetnx(ctx: &mut OpContext) -> CmdResult {
    if !(ctx.args.len() - 1).is_multiple_of(2) {
        return CmdResult::Err(RespError::new(
            "ERR wrong number of arguments for 'msetnx' command",
        ));
    }
    let mut set = 0usize;
    for &ki in ctx.owned_keys {
        let key = &ctx.args[ki];
        if ctx.db.find(key, ctx.now_ms).is_none() {
            set += 1;
        }
    }
    for &ki in ctx.owned_keys {
        let key = &ctx.args[ki];
        if ctx.db.find(key, ctx.now_ms).is_none() {
            let value = CompactString::from_bytes(&ctx.args[ki + 1]);
            ctx.db.insert(key, PrimeValue::Str(value));
        }
    }
    CmdResult::Ok(integer(set as i64))
}

fn merge_msetnx(parts: &[ShardPart], _args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    let mut total = 0i64;
    for p in parts {
        match &p.result {
            CmdResult::Ok(RespValue::Integer(i)) => total += i,
            CmdResult::Err(e) => return CmdResult::Err(e.clone()),
            _ => return CmdResult::Err(RespError::new("ERR internal: bad MSETNX shard result")),
        }
    }
    CmdResult::Ok(integer(i64::from(total == keys.len() as i64)))
}

fn exec_mget(ctx: &mut OpContext) -> CmdResult {
    let mut out = Vec::with_capacity(ctx.owned_keys.len());
    for &ki in ctx.owned_keys {
        let key = &ctx.args[ki];
        match ctx.db.find(key, ctx.now_ms) {
            Some(PrimeValue::Str(s)) => out.push(RespValue::Bulk(s.as_bytes().to_vec())),
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => out.push(RespValue::Nil),
        }
    }
    CmdResult::Ok(RespValue::Array(out))
}

fn merge_mget(parts: &[ShardPart], _args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    let mut result: Vec<Option<RespValue>> = vec![None; keys.len()];
    for p in parts {
        match &p.result {
            CmdResult::Ok(RespValue::Array(arr)) => {
                if arr.len() != p.owned_key_idxs.len() {
                    return CmdResult::Err(RespError::new(
                        "ERR internal: MGET array length mismatch",
                    ));
                }
                for (j, &ki) in p.owned_key_idxs.iter().enumerate() {
                    if let Some(pos) = keys.iter().position(|&k| k == ki) {
                        result[pos] = Some(arr[j].clone());
                    }
                }
            }
            CmdResult::Err(e) => return CmdResult::Err(e.clone()),
            _ => return CmdResult::Err(RespError::new("ERR internal: bad MGET shard result")),
        }
    }
    CmdResult::Ok(RespValue::Array(
        result
            .into_iter()
            .map(|v| v.unwrap_or(RespValue::Nil))
            .collect(),
    ))
}

// ---------------------------------------------------------------------------
// Command definitions
// ---------------------------------------------------------------------------

pub static CMD_SET: Command = Command {
    name: "SET",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_set,
    merge: None,
};
pub static CMD_GET: Command = Command {
    name: "GET",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_get,
    merge: None,
};
pub static CMD_GETDEL: Command = Command {
    name: "GETDEL",
    arity: 2,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_getdel,
    merge: None,
};
pub static CMD_GETSET: Command = Command {
    name: "GETSET",
    arity: 3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_getset,
    merge: None,
};
pub static CMD_SETNX: Command = Command {
    name: "SETNX",
    arity: 3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_setnx,
    merge: None,
};
pub static CMD_SETEX: Command = Command {
    name: "SETEX",
    arity: 4,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    key_range: KeyRange::ONE,
    exec: exec_setex,
    merge: None,
};
pub static CMD_PSETEX: Command = Command {
    name: "PSETEX",
    arity: 4,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    key_range: KeyRange::ONE,
    exec: exec_psetex,
    merge: None,
};
pub static CMD_APPEND: Command = Command {
    name: "APPEND",
    arity: 3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_append,
    merge: None,
};
pub static CMD_STRLEN: Command = Command {
    name: "STRLEN",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_strlen,
    merge: None,
};
pub static CMD_GETRANGE: Command = Command {
    name: "GETRANGE",
    arity: 4,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_getrange,
    merge: None,
};
pub static CMD_SUBSTR: Command = Command {
    name: "SUBSTR",
    arity: 4,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_getrange,
    merge: None,
};
pub static CMD_GETEX: Command = Command {
    name: "GETEX",
    arity: -2,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_getex,
    merge: None,
};
pub static CMD_DIGEST: Command = Command {
    name: "DIGEST",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_digest,
    merge: None,
};
pub static CMD_PREPEND: Command = Command {
    name: "PREPEND",
    arity: 3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_prepend,
    merge: None,
};
pub static CMD_GAT: Command = Command {
    name: "GAT",
    arity: -2,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    key_range: KeyRange::ALL,
    exec: exec_gat,
    merge: None,
};
pub static CMD_SETRANGE: Command = Command {
    name: "SETRANGE",
    arity: 4,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    key_range: KeyRange::ONE,
    exec: exec_setrange,
    merge: None,
};
pub static CMD_INCR: Command = Command {
    name: "INCR",
    arity: 2,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_incr,
    merge: None,
};
pub static CMD_INCRBY: Command = Command {
    name: "INCRBY",
    arity: 3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_incrby,
    merge: None,
};
pub static CMD_DECR: Command = Command {
    name: "DECR",
    arity: 2,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_decr,
    merge: None,
};
pub static CMD_DECRBY: Command = Command {
    name: "DECRBY",
    arity: 3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_decrby,
    merge: None,
};
pub static CMD_INCRBYFLOAT: Command = Command {
    name: "INCRBYFLOAT",
    arity: 3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_incrbyfloat,
    merge: None,
};
// ---------------------------------------------------------------------------
// CL.THROTTLE
// ---------------------------------------------------------------------------

const THROTTLE_SEC_TO_MS: i64 = 1000;
const THROTTLE_MS_TO_NS: i64 = 1_000_000;
const THROTTLE_SEC_TO_NS: u64 = 1_000_000_000;
/// Sentinel for "action was allowed": retry-after is always -1 in that case.
const THROTTLE_NOT_LIMITED: i64 = -THROTTLE_SEC_TO_MS;

struct ThrottleOut {
    limited: bool,
    remaining: i64,
    retry_after_ms: i64,
    reset_after_ms: i64,
    /// New theoretical-arrival-time to persist; `None` when limited (no write).
    new_tat_ns: Option<i64>,
}

/// Overflow guard mirroring `IsValueWithinBounds` in `string_family.cc`.
fn throttle_within_bounds(value: i64, bound: i64) -> bool {
    if bound >= 0 {
        value >= i64::MIN + bound
    } else {
        value <= i64::MAX + bound
    }
}

/// Port of Dragonfly's `OpThrottle` (`string_family.cc)`: a token bucket whose
/// entire state is a single "theoretical arrival time" (tat). The caller
/// guarantees `limit > 0` and `emission_interval_ns > 0`.
fn op_throttle(
    tat_ns: i64,
    now_ns: i64,
    limit: i64,
    emission_interval_ns: i64,
    quantity: u64,
) -> Result<ThrottleOut, RespError> {
    let err = RespError::integer;
    let tolerance_ns = emission_interval_ns.checked_mul(limit).ok_or_else(err)?;
    let increment_ns = emission_interval_ns
        .checked_mul(quantity.min(i64::MAX as u64) as i64)
        .ok_or_else(err)?;

    let mut new_tat_ns = tat_ns.max(now_ns);
    new_tat_ns = new_tat_ns.checked_add(increment_ns).ok_or_else(err)?;
    if new_tat_ns < i64::MIN.saturating_add(tolerance_ns) {
        return Err(err());
    }

    let allow_at_ns = new_tat_ns - tolerance_ns;
    if !throttle_within_bounds(now_ns, allow_at_ns) {
        return Err(err());
    }

    let diff_ns = now_ns - allow_at_ns;
    let limited = diff_ns < 0;

    let mut retry_after_ms = THROTTLE_NOT_LIMITED;
    let ttl_ns = if limited {
        if increment_ns <= tolerance_ns {
            if diff_ns == i64::MIN {
                return Err(err());
            }
            retry_after_ms = (-diff_ns).saturating_add(THROTTLE_MS_TO_NS - 1) / THROTTLE_MS_TO_NS;
        }
        if (now_ns >= 0 && tat_ns < i64::MIN.saturating_add(now_ns))
            || (now_ns < 0 && tat_ns > i64::MAX.saturating_add(now_ns))
        {
            return Err(err());
        }
        tat_ns - now_ns
    } else {
        if !throttle_within_bounds(new_tat_ns, now_ns) {
            return Err(err());
        }
        new_tat_ns - now_ns
    };

    if ttl_ns < tolerance_ns.saturating_sub(i64::MAX) {
        return Err(err());
    }
    let next_ns = tolerance_ns - ttl_ns;
    let remaining = if next_ns > -emission_interval_ns {
        next_ns / emission_interval_ns
    } else {
        0
    };
    let reset_after_ms = ttl_ns.saturating_add(THROTTLE_MS_TO_NS - 1) / THROTTLE_MS_TO_NS;
    let new_tat_ns = if limited { None } else { Some(new_tat_ns) };

    Ok(ThrottleOut {
        limited,
        remaining,
        retry_after_ms,
        reset_after_ms,
        new_tat_ns,
    })
}

/// Round a duration in ms up to whole seconds; negatives pass through as-is.
/// Mirrors the ms→s conversion in `CmdClThrottle` (array[3]/array[4]).
fn throttle_seconds_from_ms(ms: i64) -> i64 {
    let s = ms / THROTTLE_SEC_TO_MS;
    if ms > 0 { s + 1 } else { s }
}

/// CL.THROTTLE <key> <max_burst> <count per period> <period> [<quantity>]
fn exec_cl_throttle(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let parse = |i: usize| parse_u64(&ctx.args[key_idx + i]);
    let (Some(max_burst), Some(count), Some(period)) = (parse(1), parse(2), parse(3)) else {
        return CmdResult::Err(RespError::integer());
    };
    let quantity = match ctx.args.get(key_idx + 4) {
        Some(s) => match parse_u64(s) {
            Some(v) => v,
            None => return CmdResult::Err(RespError::integer()),
        },
        None => 1,
    };

    if max_burst > (i64::MAX as u64) - 1 {
        return CmdResult::Err(RespError::integer());
    }
    let limit = (max_burst + 1) as i64;

    if period > u64::MAX / THROTTLE_SEC_TO_NS
        || count == 0
        || period * THROTTLE_SEC_TO_NS / count > i64::MAX as u64
    {
        return CmdResult::Err(RespError::integer());
    }
    let emission_interval_ns = (period * THROTTLE_SEC_TO_NS / count) as i64;
    if emission_interval_ns == 0 {
        return CmdResult::Err(RespError::new("zero rates are not supported"));
    }
    if emission_interval_ns > i64::MAX / limit {
        return CmdResult::Err(RespError::integer());
    }
    if quantity != 0 && emission_interval_ns as u64 > (i64::MAX as u64) / quantity {
        return CmdResult::Err(RespError::integer());
    }

    let now_ns = (ctx.now_ms as i64).saturating_mul(THROTTLE_MS_TO_NS);
    let cached_tat: Option<i64> = match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => match parse_i64(s.as_bytes()) {
            Some(v) => Some(v),
            None => return CmdResult::Err(RespError::integer()),
        },
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => None,
    };
    let tat_ns = match cached_tat {
        Some(v) => v,
        None => now_ns,
    };

    let out = match op_throttle(tat_ns, now_ns, limit, emission_interval_ns, quantity) {
        Ok(o) => o,
        Err(e) => return CmdResult::Err(e),
    };

    if let Some(new_tat_ns) = out.new_tat_ns {
        let value = PrimeValue::Str(CompactString::from_bytes(&crate::util::itoa(new_tat_ns)));
        let new_tat_ms = new_tat_ns.saturating_add(THROTTLE_MS_TO_NS - 1) / THROTTLE_MS_TO_NS;
        let expire_at_ms = new_tat_ms.max(0) as u64;
        if cached_tat.is_some() {
            if let Some(PrimeValue::Str(s)) = ctx.db.find_mut(key, ctx.now_ms) {
                *s = CompactString::from_bytes(&crate::util::itoa(new_tat_ns));
            }
            ctx.db.set_expiry(key, expire_at_ms, ctx.now_ms);
        } else {
            ctx.db.insert(key, value);
            ctx.db.set_expiry(key, expire_at_ms, ctx.now_ms);
        }
    }

    CmdResult::Ok(RespValue::Array(vec![
        integer(i64::from(out.limited)),
        integer(limit),
        integer(out.remaining),
        integer(throttle_seconds_from_ms(out.retry_after_ms)),
        integer(throttle_seconds_from_ms(out.reset_after_ms)),
    ]))
}

pub static CMD_CL_THROTTLE: Command = Command {
    name: "CL.THROTTLE",
    arity: -5,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_cl_throttle,
    merge: None,
};

pub static CMD_MSET: Command = Command {
    name: "MSET",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_MULTI_KEY,
    key_range: KeyRange::PAIRS,
    exec: exec_mset,
    merge: Some(merge_mset),
};
pub static CMD_MSETNX: Command = Command {
    name: "MSETNX",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_MULTI_KEY,
    key_range: KeyRange::PAIRS,
    exec: exec_msetnx,
    merge: Some(merge_msetnx),
};
pub static CMD_MGET: Command = Command {
    name: "MGET",
    arity: -2,
    flags: FLAG_READONLY | FLAG_MULTI_KEY | FLAG_FAST,
    key_range: KeyRange::ALL,
    exec: exec_mget,
    merge: Some(merge_mget),
};

#[cfg(test)]
mod tests {
    use super::*;

    const MS_TO_NS: i64 = 1_000_000;

    /// Drives `op_throttle` with a virtual clock, persisting `new_tat_ns` like
    /// the real command does. Mirrors `StringFamilyTest.ClThrottle`.
    fn run(
        tat: &mut Option<i64>,
        now_ms: i64,
        limit: i64,
        emission_interval_ns: i64,
        quantity: u64,
    ) -> Vec<i64> {
        let now_ns = now_ms * MS_TO_NS;
        let cached = match tat {
            Some(v) => *v,
            None => now_ns,
        };
        let out = op_throttle(cached, now_ns, limit, emission_interval_ns, quantity)
            .expect("throttle should succeed");
        if let Some(new_tat_ns) = out.new_tat_ns {
            *tat = Some(new_tat_ns);
        }
        vec![
            i64::from(out.limited),
            limit,
            out.remaining,
            throttle_seconds_from_ms(out.retry_after_ms),
            throttle_seconds_from_ms(out.reset_after_ms),
        ]
    }

    #[test]
    fn cl_throttle_matches_reference() {
        const LIMIT: i64 = 5;
        // max_burst=4, count=1, period=10s -> one token every 10s.
        const EMISSION_NS: i64 = 10_000_000_000;
        let mut tat: Option<i64> = None;
        let mut now_ms: i64 = 0;

        // A request larger than the bucket is always limited and never consumed.
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 6),
            vec![1, 5, 5, -1, 0]
        );

        // Normal requests drain the bucket one token at a time.
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 1),
            vec![0, 5, 4, -1, 11]
        );
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 1),
            vec![0, 5, 3, -1, 21]
        );
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 1),
            vec![0, 5, 2, -1, 31]
        );
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 1),
            vec![0, 5, 1, -1, 41]
        );
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 1),
            vec![0, 5, 0, -1, 51]
        );
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 1),
            vec![1, 5, 0, 11, 51]
        );

        now_ms += 30_000;
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 1),
            vec![0, 5, 2, -1, 31]
        );

        now_ms += 1_000;
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 1),
            vec![0, 5, 1, -1, 40]
        );

        now_ms += 9_000;
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 1),
            vec![0, 5, 1, -1, 41]
        );

        now_ms += 40_000;
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 1),
            vec![0, 5, 4, -1, 11]
        );

        now_ms += 15_000;
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 1),
            vec![0, 5, 4, -1, 11]
        );

        // Zero-volume requests only peek at the state.
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 0),
            vec![0, 5, 4, -1, 11]
        );

        // High-volume requests use up more of the limit.
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 2),
            vec![0, 5, 2, -1, 31]
        );

        // A large-but-legal request can still be limited.
        assert_eq!(
            run(&mut tat, now_ms, LIMIT, EMISSION_NS, 5),
            vec![1, 5, 2, 31, 31]
        );

        // emission interval of 2us with cost 2 consumes 2 tokens per request.
        let mut tat2: Option<i64> = None;
        assert_eq!(
            run(&mut tat2, 0, LIMIT, 2_000, 2),
            vec![0, 5, LIMIT - 2, -1, 1]
        );
    }

    #[test]
    fn throttle_seconds_rounds_up_positive() {
        assert_eq!(throttle_seconds_from_ms(-1000), -1);
        assert_eq!(throttle_seconds_from_ms(0), 0);
        assert_eq!(throttle_seconds_from_ms(1000), 2);
        assert_eq!(throttle_seconds_from_ms(10000), 11);
    }

    // ---------------------------------------------------------------------
    // GETEX / DIGEST / PREPEND / SUBSTR / GAT
    // ---------------------------------------------------------------------

    use crate::core::db::DbSlice;

    fn bulk_of(r: CmdResult) -> Vec<u8> {
        match r {
            CmdResult::Ok(RespValue::Bulk(b)) => b,
            o => panic!("expected bulk, got {:?}", o.into_resp_value()),
        }
    }

    fn nil_of(r: &CmdResult) -> bool {
        matches!(r, CmdResult::Ok(RespValue::Nil))
    }

    fn err_of(r: CmdResult) -> String {
        match r {
            CmdResult::Err(e) => e.message,
            o => panic!("expected error, got {:?}", o.into_resp_value()),
        }
    }

    fn int_of(r: CmdResult) -> i64 {
        match r {
            CmdResult::Ok(RespValue::Integer(v)) => v,
            o => panic!("expected integer, got {:?}", o.into_resp_value()),
        }
    }

    fn dispatch_at(db: &mut DbSlice, now_ms: u64, argv: &[Vec<u8>]) -> CmdResult {
        let (exec, first_key_idx, owned): (fn(&mut OpContext) -> CmdResult, usize, Vec<usize>) =
            match argv[0].as_slice() {
                b"SET" => (exec_set, 1, (1..2).collect()),
                b"GET" => (exec_get, 1, (1..2).collect()),
                b"APPEND" => (exec_append, 1, (1..2).collect()),
                b"PREPEND" => (exec_prepend, 1, (1..2).collect()),
                b"GETRANGE" | b"SUBSTR" => (exec_getrange, 1, (1..2).collect()),
                b"GETEX" => (exec_getex, 1, (1..2).collect()),
                b"DIGEST" => (exec_digest, 1, (1..2).collect()),
                b"GAT" => (exec_gat, 1, (1..2).collect()),
                _ => panic!("unhandled command {:?}", argv[0]),
            };
        let mut ctx = OpContext {
            db,
            args: argv,
            owned_keys: &owned,
            first_key_idx,
            conn_id: 0,
            now_ms,
        };
        exec(&mut ctx)
    }

    fn cmd(db: &mut DbSlice, args: &[&[u8]]) -> CmdResult {
        dispatch_at(db, 0, &args.iter().map(|a| a.to_vec()).collect::<Vec<_>>())
    }

    fn cmd_at(db: &mut DbSlice, now_ms: u64, args: &[&[u8]]) -> CmdResult {
        dispatch_at(
            db,
            now_ms,
            &args.iter().map(|a| a.to_vec()).collect::<Vec<_>>(),
        )
    }

    fn str_of(db: &mut DbSlice, key: &str, value: &str) {
        db.insert(key.as_bytes(), PrimeValue::Str(CompactString::from(value)));
    }

    #[test]
    fn getex_basic_and_options() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "k", "hello");

        assert_eq!(bulk_of(cmd(&mut db, &[b"GETEX", b"k"])), b"hello");
        assert_eq!(db.ttl_ms(b"k", 0), -1);

        assert_eq!(
            bulk_of(cmd(&mut db, &[b"GETEX", b"k", b"EX", b"100"])),
            b"hello"
        );
        assert_eq!(db.ttl_ms(b"k", 0), 100_000);
        assert_eq!(
            bulk_of(cmd(&mut db, &[b"GETEX", b"k", b"PX", b"500"])),
            b"hello"
        );
        assert_eq!(db.ttl_ms(b"k", 0), 500);

        assert_eq!(
            bulk_of(cmd(&mut db, &[b"GETEX", b"k", b"PERSIST"])),
            b"hello"
        );
        assert_eq!(db.ttl_ms(b"k", 0), -1);
    }

    #[test]
    fn getex_absolute_and_past_expiry() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "k", "v");

        // PXAT 2000 (absolute ms) at now=0 sets a 2s expiry.
        assert_eq!(
            bulk_of(cmd(&mut db, &[b"GETEX", b"k", b"PXAT", b"2000"])),
            b"v"
        );
        assert_eq!(db.ttl_ms(b"k", 0), 2000);

        // A past absolute expiry returns the value but deletes the key.
        str_of(&mut db, "k2", "v");
        assert_eq!(
            bulk_of(cmd_at(&mut db, 5000, &[b"GETEX", b"k2", b"PXAT", b"1"])),
            b"v"
        );
        assert_eq!(db.ttl_ms(b"k2", 5000), -2);

        str_of(&mut db, "k3", "v");
        assert_eq!(
            bulk_of(cmd_at(&mut db, 5000, &[b"GETEX", b"k3", b"EXAT", b"1"])),
            b"v"
        );
        assert_eq!(db.ttl_ms(b"k3", 5000), -2);
    }

    #[test]
    fn getex_errors() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "k", "v");

        assert_eq!(
            err_of(cmd(&mut db, &[b"GETEX", b"k", b"EX", b"0"])),
            K_GETEX_EXPIRY_ERR
        );
        assert_eq!(
            err_of(cmd(&mut db, &[b"GETEX", b"k", b"PX", b"-1"])),
            K_GETEX_EXPIRY_ERR
        );
        assert_eq!(
            err_of(cmd(&mut db, &[b"GETEX", b"k", b"EX", b"abc"])),
            "ERR value is not an integer or out of range"
        );
        assert_eq!(
            err_of(cmd(&mut db, &[b"GETEX", b"k", b"EX"])),
            "ERR syntax error"
        );
        assert_eq!(
            err_of(cmd(&mut db, &[b"GETEX", b"k", b"BOGUS"])),
            "ERR syntax error"
        );
        assert_eq!(
            err_of(cmd(&mut db, &[b"GETEX", b"k", b"EX", b"10", b"EX", b"20"])),
            "ERR syntax error"
        );
        assert_eq!(
            err_of(cmd(&mut db, &[b"GETEX", b"k", b"PERSIST", b"extra"])),
            "ERR syntax error"
        );
        assert_eq!(
            err_of(cmd(&mut db, &[b"GETEX", b"k", b"EX", b"268435456"])),
            "ERR index out of range"
        );
    }

    #[test]
    fn getex_missing_and_wrong_type() {
        let mut db = DbSlice::new(0);
        assert!(nil_of(&cmd(&mut db, &[b"GETEX", b"missing"])));
        assert!(nil_of(&cmd(&mut db, &[b"GETEX", b"missing", b"EX", b"10"])));

        let mut l = crate::core::quicklist::QuickList::new();
        l.push_back(crate::core::quicklist::ListItem::Str(CompactString::from(
            "x",
        )));
        db.insert(b"l", PrimeValue::List(l));
        assert!(err_of(cmd(&mut db, &[b"GETEX", b"l"])).starts_with("WRONGTYPE"));
    }

    #[test]
    fn digest_matches_reference() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "key", "value");
        assert_eq!(
            bulk_of(cmd(&mut db, &[b"DIGEST", b"key"])),
            b"87d57e269b9df0f0"
        );

        assert!(nil_of(&cmd(&mut db, &[b"DIGEST", b"nonexistent"])));

        str_of(&mut db, "key1", "testvalue");
        str_of(&mut db, "key2", "testvalue");
        let d1 = bulk_of(cmd(&mut db, &[b"DIGEST", b"key1"]));
        let d2 = bulk_of(cmd(&mut db, &[b"DIGEST", b"key2"]));
        assert_eq!(d1, d2);

        str_of(&mut db, "key3", "different");
        assert_ne!(d1, bulk_of(cmd(&mut db, &[b"DIGEST", b"key3"])));

        str_of(&mut db, "intkey", "123");
        assert_eq!(bulk_of(cmd(&mut db, &[b"DIGEST", b"intkey"])).len(), 16);

        str_of(&mut db, "empty", "");
        assert_eq!(bulk_of(cmd(&mut db, &[b"DIGEST", b"empty"])).len(), 16);

        let mut list = crate::core::quicklist::QuickList::new();
        list.push_back(crate::core::quicklist::ListItem::Str(CompactString::from(
            "item",
        )));
        db.insert(b"list", PrimeValue::List(list));
        assert!(err_of(cmd(&mut db, &[b"DIGEST", b"list"])).starts_with("WRONGTYPE"));
    }

    #[test]
    fn prepend_works() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "k", "world");
        assert_eq!(int_of(cmd(&mut db, &[b"PREPEND", b"k", b"hello"])), 10);
        assert_eq!(bulk_of(cmd(&mut db, &[b"GET", b"k"])), b"helloworld");

        assert_eq!(int_of(cmd(&mut db, &[b"PREPEND", b"new", b"abc"])), 3);
        assert_eq!(bulk_of(cmd(&mut db, &[b"GET", b"new"])), b"abc");

        let mut l = crate::core::quicklist::QuickList::new();
        l.push_back(crate::core::quicklist::ListItem::Str(CompactString::from(
            "x",
        )));
        db.insert(b"l", PrimeValue::List(l));
        assert!(err_of(cmd(&mut db, &[b"PREPEND", b"l", b"x"])).starts_with("WRONGTYPE"));
    }

    #[test]
    fn substr_is_getrange_alias() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "foo", "");
        assert_eq!(
            bulk_of(cmd(&mut db, &[b"SUBSTR", b"foo", b"0", b"-1"])),
            b""
        );

        str_of(&mut db, "bar", "hello");
        assert_eq!(
            bulk_of(cmd(&mut db, &[b"SUBSTR", b"bar", b"1", b"3"])),
            b"ell"
        );
    }

    #[test]
    fn gat_via_resp_errors() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "key", "val");
        assert_eq!(
            err_of(cmd(&mut db, &[b"GAT", b"key"])),
            "ERR GAT is a memcache-only command"
        );
    }
}
