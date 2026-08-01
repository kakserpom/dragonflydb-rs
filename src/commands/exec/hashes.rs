use crate::commands::exec::keys::glob_match;
use crate::commands::{integer, ok, Command, OpContext, KeyRange, FLAG_DENYOOM, FLAG_FAST, FLAG_READONLY, FLAG_WRITE};
use crate::core::compact::CompactString;
use crate::core::hash::Hash;
use crate::core::PrimeValue;
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::{format_double, parse_double, parse_i64, parse_u64};

/// Ceiling for per-field TTLs, shared with the reference
/// `kMaxExpireDeadlineSec` (dragonfly/src/server/common.h).
const MAX_EXPIRE_SEC: i64 = (1u64 << 28) as i64 - 1;

const K_INVALID_NUM_FIELDS: &str = "ERR Number of fields must be a positive integer";
const K_NUM_FIELDS_MISMATCH: &str = "ERR The `numfields` parameter must match the number of arguments";
const K_MANDATORY_FIELDS: &str = "ERR Mandatory argument FIELDS is missing or not at the right position";

/// Expiry unit tags for the EX/PX/EXAT/PXAT options.
const EX_SEC: u8 = 0;
const PX_MSEC: u8 = 1;
const EX_AT_SEC: u8 = 2;
const PX_AT_MSEC: u8 = 3;

fn invalid_expire_time(cmd: &str) -> String {
    format!("ERR invalid expire time in '{}' command", cmd)
}

fn wrong_num_args(cmd: &str) -> String {
    format!("ERR wrong number of arguments for '{}' command", cmd)
}

/// Port of `MakeFieldExpireParams` + `ExpireParams::Calculate`: returns the
/// relative TTL in ms for `(unit, value)` against `now_ms`, or None when the
/// expiry is malformed. `allow_expired` permits an already-due TTL (HGETEX).
fn field_expiry_ttl_ms(unit: u8, value: i64, now_ms: u64, allow_expired: bool) -> Option<i64> {
    if value < 0 {
        return None;
    }
    let now = now_ms as i128;
    let sec_to_ms = |v: i64| (v as i128).saturating_mul(1000);
    let (rel_ms, at_ms) = match unit {
        EX_SEC => (sec_to_ms(value), now + sec_to_ms(value)),
        PX_MSEC => (value as i128, now + value as i128),
        EX_AT_SEC => (sec_to_ms(value) - now, sec_to_ms(value)),
        PX_AT_MSEC => (value as i128 - now, value as i128),
        _ => unreachable!(),
    };
    let max_ms = (MAX_EXPIRE_SEC as i128) * 1000;
    if rel_ms > max_ms || (!allow_expired && rel_ms <= 0) {
        return None;
    }
    if at_ms < 0 {
        // Already-due absolute timestamp (only reachable with allow_expired, HGETEX):
        // the field expires immediately but its value is still returned.
        return Some(0);
    }
    Some(rel_ms as i64)
}

/// HSETEX field-set mode (NX | FNX | FXX).
#[derive(PartialEq, Clone, Copy)]
enum SetMode {
    None,
    Nx,
    Fnx,
    Fxx,
}

/// HEXPIRE per-field condition flag.
#[derive(PartialEq, Clone, Copy)]
enum ExpireFlag {
    Always,
    Nx,
    Xx,
    Gt,
    Lt,
}

fn hash_mut<'a>(ctx: &'a mut OpContext, key: &[u8]) -> Result<&'a mut Hash, RespError> {
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Hash(h)) => {
            h.prune_expired(ctx.now_ms);
            Ok(h)
        }
        Some(_) => Err(RespError::wrong_type()),
        None => Err(RespError::new("ERR no such key")),
    }
}

/// Lazily prune fields of the hash at `key` that expired before `now_ms`,
/// deleting the key when it is emptied. Wrong-type keys error; missing keys are
/// a no-op.
fn prune_hash_key(ctx: &mut OpContext, key: &[u8]) -> Result<(), RespError> {
    let empty = match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Hash(h)) => {
            h.prune_expired(ctx.now_ms);
            h.is_empty()
        }
        Some(_) => return Err(RespError::wrong_type()),
        None => false,
    };
    if empty {
        ctx.db.remove(key);
    }
    Ok(())
}

fn ensure_hash<'a>(ctx: &'a mut OpContext, key: &[u8]) -> Result<&'a mut Hash, RespError> {
    if ctx.db.find(key, ctx.now_ms).is_none() {
        ctx.db.insert(CompactString::from_bytes(key), PrimeValue::Hash(Hash::new()));
    }
    hash_mut(ctx, key)
}

fn exec_hset_common(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let fvs = &ctx.args[key_idx + 1..];
    if fvs.is_empty() || !fvs.len().is_multiple_of(2) {
        return CmdResult::Err(RespError::new("ERR wrong number of arguments for 'hset' command"));
    }
    let h = match ensure_hash(ctx, key) {
        Ok(h) => h,
        Err(e) => return CmdResult::Err(e),
    };
    let mut added = 0i64;
    for pair in fvs.chunks(2) {
        let f = CompactString::from_bytes(&pair[0]);
        let v = CompactString::from_bytes(&pair[1]);
        if h.add_expirable(f, v, None, false) {
            added += 1;
        }
    }
    CmdResult::Ok(integer(added))
}

fn exec_hmset(ctx: &mut OpContext) -> CmdResult {
    match exec_hset_common(ctx) {
        CmdResult::Ok(_) => CmdResult::Ok(ok()),
        other => other,
    }
}

fn exec_hget(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let field = &ctx.args[key_idx + 1];
    if let Err(e) = prune_hash_key(ctx, key) {
        return CmdResult::Err(e);
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Hash(h)) => match h.get(field) {
            Some(v) => CmdResult::Ok(RespValue::Bulk(v.as_bytes().to_vec())),
            None => CmdResult::Ok(RespValue::Nil),
        },
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Nil),
    }
}

fn exec_hmget(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let fields = &ctx.args[key_idx + 1..];
    if let Err(e) = prune_hash_key(ctx, key) {
        return CmdResult::Err(e);
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Hash(h)) => {
            let out = fields
                .iter()
                .map(|f| match h.get(f) {
                    Some(v) => RespValue::Bulk(v.as_bytes().to_vec()),
                    None => RespValue::Nil,
                })
                .collect();
            CmdResult::Ok(RespValue::Array(out))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Array(vec![RespValue::Nil; fields.len()])),
    }
}

fn exec_hdel(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let fields = &ctx.args[key_idx + 1..];
    if let Err(e) = prune_hash_key(ctx, key) {
        return CmdResult::Err(e);
    }
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Hash(h)) => {
            let mut removed = 0i64;
            for f in fields {
                if h.remove(f).is_some() {
                    removed += 1;
                }
            }
            if h.is_empty() {
                ctx.db.remove(key);
            }
            CmdResult::Ok(integer(removed))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

fn exec_hlen(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    if let Err(e) = prune_hash_key(ctx, key) {
        return CmdResult::Err(e);
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Hash(h)) => CmdResult::Ok(integer(h.len() as i64)),
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

fn exec_hgetall(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    if let Err(e) = prune_hash_key(ctx, key) {
        return CmdResult::Err(e);
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Hash(h)) => {
            let mut out = Vec::with_capacity(h.len() * 2);
            for (f, v) in h.iter() {
                out.push(RespValue::Bulk(f.as_bytes().to_vec()));
                out.push(RespValue::Bulk(v.as_bytes().to_vec()));
            }
            CmdResult::Ok(RespValue::Array(out))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Array(vec![])),
    }
}

fn exec_hkeys(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    if let Err(e) = prune_hash_key(ctx, key) {
        return CmdResult::Err(e);
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Hash(h)) => {
            let out = h.iter().map(|(f, _)| RespValue::Bulk(f.as_bytes().to_vec())).collect();
            CmdResult::Ok(RespValue::Array(out))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Array(vec![])),
    }
}

fn exec_hvals(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    if let Err(e) = prune_hash_key(ctx, key) {
        return CmdResult::Err(e);
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Hash(h)) => {
            let out = h.iter().map(|(_, v)| RespValue::Bulk(v.as_bytes().to_vec())).collect();
            CmdResult::Ok(RespValue::Array(out))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Array(vec![])),
    }
}

fn exec_hexists(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let field = &ctx.args[key_idx + 1];
    if let Err(e) = prune_hash_key(ctx, key) {
        return CmdResult::Err(e);
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Hash(h)) => CmdResult::Ok(integer(h.contains(field) as i64)),
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

fn exec_hincrby(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let field = CompactString::from_bytes(&ctx.args[key_idx + 1]);
    let delta = match parse_i64(&ctx.args[key_idx + 2]) {
        Some(v) => v,
        None => return CmdResult::Err(RespError::integer()),
    };
    let h = match ensure_hash(ctx, key) {
        Ok(h) => h,
        Err(e) => return CmdResult::Err(e),
    };
    let cur = match h.get(field.as_bytes()) {
        Some(v) if v.is_empty() => 0i64,
        Some(v) => match parse_i64(v.as_bytes()) {
            Some(n) => n,
            None => return CmdResult::Err(RespError::integer()),
        },
        None => 0,
    };
    let new_val = match cur.checked_add(delta) {
        Some(v) => v,
        None => return CmdResult::Err(RespError::integer()),
    };
    h.set(field, CompactString::from_bytes(&crate::util::itoa(new_val)));
    CmdResult::Ok(integer(new_val))
}

fn exec_hincrbyfloat(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let field = CompactString::from_bytes(&ctx.args[key_idx + 1]);
    let delta = match parse_double(&ctx.args[key_idx + 2]) {
        Some(v) => v,
        None => return CmdResult::Err(RespError::float()),
    };
    let h = match ensure_hash(ctx, key) {
        Ok(h) => h,
        Err(e) => return CmdResult::Err(e),
    };
    let cur = match h.get(field.as_bytes()) {
        Some(v) if v.is_empty() => 0.0,
        Some(v) => match parse_double(v.as_bytes()) {
            Some(n) => n,
            None => return CmdResult::Err(RespError::float()),
        },
        None => 0.0,
    };
    let new_val = cur + delta;
    if !new_val.is_finite() {
        return CmdResult::Err(RespError::new("ERR increment would produce NaN or Infinity"));
    }
    let s = format_double(new_val);
    h.set(field, CompactString::from_bytes(s.as_bytes()));
    CmdResult::Ok(RespValue::Bulk(s.into_bytes()))
}

fn exec_hstrlen(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let field = &ctx.args[key_idx + 1];
    if let Err(e) = prune_hash_key(ctx, key) {
        return CmdResult::Err(e);
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Hash(h)) => match h.get(field) {
            Some(v) => CmdResult::Ok(integer(v.len() as i64)),
            None => CmdResult::Ok(integer(0)),
        },
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

fn exec_hsetnx(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let field = CompactString::from_bytes(&ctx.args[key_idx + 1]);
    let value = CompactString::from_bytes(&ctx.args[key_idx + 2]);
    let h = match ensure_hash(ctx, key) {
        Ok(h) => h,
        Err(e) => return CmdResult::Err(e),
    };
    if h.contains(field.as_bytes()) {
        return CmdResult::Ok(integer(0));
    }
    h.set(field, value);
    CmdResult::Ok(integer(1))
}

type ExpireOptions = (usize, SetMode, bool, Option<(u8, i64)>);

/// Parses the leading NX/FNX/FXX/KEEPTTL/EX/PX/EXAT/PXAT options shared by the
/// HSETEX and HGETEX grammar, returning (mode, keepttl, expiry).
fn parse_expire_options(
    ctx: &OpContext,
    key_idx: usize,
) -> Result<ExpireOptions, RespError> {
    let mut i = key_idx + 1;
    let mut mode = SetMode::None;
    let mut keepttl = false;
    let mut expiry: Option<(u8, i64)> = None;
    while i < ctx.args.len() {
        let tok = &ctx.args[i];
        if tok.eq_ignore_ascii_case(b"NX") || tok.eq_ignore_ascii_case(b"FNX") || tok.eq_ignore_ascii_case(b"FXX") {
            let m = if tok.eq_ignore_ascii_case(b"NX") {
                SetMode::Nx
            } else if tok.eq_ignore_ascii_case(b"FNX") {
                SetMode::Fnx
            } else {
                SetMode::Fxx
            };
            if mode != SetMode::None {
                return Err(RespError::syntax());
            }
            mode = m;
            i += 1;
        } else if tok.eq_ignore_ascii_case(b"KEEPTTL") {
            if keepttl {
                return Err(RespError::syntax());
            }
            keepttl = true;
            i += 1;
        } else if tok.eq_ignore_ascii_case(b"EX") || tok.eq_ignore_ascii_case(b"PX")
            || tok.eq_ignore_ascii_case(b"EXAT") || tok.eq_ignore_ascii_case(b"PXAT")
        {
            if expiry.is_some() {
                return Err(RespError::syntax());
            }
            let unit = if tok.eq_ignore_ascii_case(b"EX") {
                EX_SEC
            } else if tok.eq_ignore_ascii_case(b"PX") {
                PX_MSEC
            } else if tok.eq_ignore_ascii_case(b"EXAT") {
                EX_AT_SEC
            } else {
                PX_AT_MSEC
            };
            i += 1;
            match ctx.args.get(i).and_then(|a| parse_i64(a)) {
                Some(v) => {
                    expiry = Some((unit, v));
                    i += 1;
                }
                None => return Err(RespError::integer()),
            }
        } else {
            break;
        }
    }
    Ok((i, mode, keepttl, expiry))
}

/// Parses the mandatory "FIELDS numfields" prefix, returning (index past the
/// field names, number of fields). `count_err` distinguishes HEXPIRE (which
/// reports the mismatch text for a bad count) from HTTL/HPEXPIRETIME/HGETEX
/// (which report "Number of fields must be a positive integer").
fn parse_fields_prefix(
    ctx: &OpContext,
    i: &mut usize,
    count_err: Option<&str>,
) -> Result<usize, RespError> {
    if !ctx.args.get(*i).is_some_and(|a| a.eq_ignore_ascii_case(b"FIELDS")) {
        return Err(RespError::new(K_MANDATORY_FIELDS));
    }
    *i += 1;
    let numfields = match ctx.args.get(*i).and_then(|a| parse_u64(a)) {
        Some(n) if n >= 1 && n <= u32::MAX as u64 => n as usize,
        _ => {
            return Err(RespError::new(
                count_err.unwrap_or(K_NUM_FIELDS_MISMATCH),
            ))
        }
    };
    *i += 1;
    let fields = &ctx.args[*i..];
    if fields.len() != numfields {
        return Err(RespError::new(K_NUM_FIELDS_MISMATCH));
    }
    Ok(numfields)
}

/// Evaluates the FNX/FXX collective condition of HSETEX (CheckHSetExCondition):
/// FNX holds only if none of the fields exist, FXX only if all exist. A missing
/// key counts as "no fields exist".
fn hsetex_condition(ctx: &mut OpContext, key: &[u8], fvs: &[Vec<u8>], fnx: bool) -> Result<bool, RespError> {
    prune_hash_key(ctx, key)?;
    let Some(PrimeValue::Hash(h)) = ctx.db.find(key, ctx.now_ms) else {
        return Ok(fnx);
    };
    for pair in fvs.chunks(2) {
        let found = h.contains(&pair[0]);
        if fnx == found {
            return Ok(false);
        }
    }
    Ok(true)
}

/// HSETEX key [NX | FNX | FXX] [KEEPTTL] ttl_sec field value [field value ...]
/// HSETEX key [FNX | FXX] [EX sec | PX ms | EXAT ts-sec | PXAT ts-ms | KEEPTTL]
///                FIELDS numfields field value [field value ...]
///
/// The Redis (FIELDS) form replies 1, the Dragonfly bare-ttl form the number of
/// fields created. NX (per-field skip) is Dragonfly-only; FNX/FXX are collective
/// all-or-nothing conditions valid in both forms.
fn exec_hsetex(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = ctx.args[key_idx].clone();
    let (mut i, mode, keepttl, expiry) = match parse_expire_options(ctx, key_idx) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };

    // Redis format.
    if ctx.args.get(i).is_some_and(|a| a.eq_ignore_ascii_case(b"FIELDS")) {
        if mode == SetMode::Nx || (keepttl && expiry.is_some()) {
            return CmdResult::Err(RespError::syntax());
        }
        i += 1;
        let numfields = match ctx.args.get(i).and_then(|a| parse_u64(a)) {
            Some(n) if n >= 1 && n <= u32::MAX as u64 => n as usize,
            _ => return CmdResult::Err(RespError::new(K_NUM_FIELDS_MISMATCH)),
        };
        i += 1;
        let fvs = &ctx.args[i..];
        if fvs.len() != numfields * 2 {
            return CmdResult::Err(RespError::new(K_NUM_FIELDS_MISMATCH));
        }
        let expire_ms = match expiry {
            Some((unit, value)) => match field_expiry_ttl_ms(unit, value, ctx.now_ms, false) {
                Some(ttl_ms) => {
                    let ttl_sec = (ttl_ms + 999) / 1000;
                    Some((ctx.now_ms / 1000).saturating_add(ttl_sec as u64).saturating_mul(1000))
                }
                None => return CmdResult::Err(RespError::new(invalid_expire_time("hsetex"))),
            },
            None => None,
        };
        if mode == SetMode::Fnx || mode == SetMode::Fxx {
            match hsetex_condition(ctx, &key, fvs, mode == SetMode::Fnx) {
                Ok(false) => return CmdResult::Ok(integer(0)),
                Ok(true) => {}
                Err(e) => return CmdResult::Err(e),
            }
        }
        let h = match ensure_hash(ctx, &key) {
            Ok(h) => h,
            Err(e) => return CmdResult::Err(e),
        };
        for pair in fvs.chunks(2) {
            let f = CompactString::from_bytes(&pair[0]);
            let v = CompactString::from_bytes(&pair[1]);
            h.add_expirable(f, v, expire_ms, keepttl);
        }
        return CmdResult::Ok(integer(1));
    }

    // EX/PX/EXAT/PXAT without FIELDS is malformed.
    if expiry.is_some() {
        return CmdResult::Err(RespError::syntax());
    }

    // Dragonfly format: bare ttl_sec followed by field/value pairs.
    let ttl_sec = match ctx.args.get(i).and_then(|a| parse_i64(a)) {
        Some(v) if (1..=MAX_EXPIRE_SEC).contains(&v) => v,
        _ => return CmdResult::Err(RespError::integer()),
    };
    i += 1;
    let fvs = &ctx.args[i..];
    if fvs.is_empty() || !fvs.len().is_multiple_of(2) {
        return CmdResult::Err(RespError::new(wrong_num_args("hsetex")));
    }
    if mode == SetMode::Fnx || mode == SetMode::Fxx {
        match hsetex_condition(ctx, &key, fvs, mode == SetMode::Fnx) {
            Ok(false) => return CmdResult::Ok(integer(0)),
            Ok(true) => {}
            Err(e) => return CmdResult::Err(e),
        }
    }
    let expire_ms = (ctx.now_ms / 1000).saturating_add(ttl_sec as u64).saturating_mul(1000);
    let h = match ensure_hash(ctx, &key) {
        Ok(h) => h,
        Err(e) => return CmdResult::Err(e),
    };
    let mut created = 0i64;
    for pair in fvs.chunks(2) {
        let f = CompactString::from_bytes(&pair[0]);
        let v = CompactString::from_bytes(&pair[1]);
        let added = if mode == SetMode::Nx {
            h.add_or_skip(f, v, Some(expire_ms))
        } else {
            h.add_expirable(f, v, Some(expire_ms), keepttl)
        };
        if added {
            created += 1;
        }
    }
    CmdResult::Ok(integer(created))
}

/// HEXPIRE key ttl_sec [NX | XX | GT | LT] FIELDS numfields field [field ...]
///
/// Replies per field: -2 missing, 0 condition not met, 1 TTL set, 2 removed
/// (ttl 0). A key emptied by expiration is deleted.
fn exec_hexpire(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = ctx.args[key_idx].clone();
    let mut i = key_idx + 1;
    let ttl_sec = match ctx.args.get(i).and_then(|a| parse_i64(a)) {
        Some(v) if (0..=MAX_EXPIRE_SEC).contains(&v) => v as u64,
        _ => return CmdResult::Err(RespError::integer()),
    };
    i += 1;
    let mut flag = ExpireFlag::Always;
    if let Some(tok) = ctx.args.get(i) {
        let f = if tok.eq_ignore_ascii_case(b"NX") {
            Some(ExpireFlag::Nx)
        } else if tok.eq_ignore_ascii_case(b"XX") {
            Some(ExpireFlag::Xx)
        } else if tok.eq_ignore_ascii_case(b"GT") {
            Some(ExpireFlag::Gt)
        } else if tok.eq_ignore_ascii_case(b"LT") {
            Some(ExpireFlag::Lt)
        } else {
            None
        };
        if let Some(f) = f {
            flag = f;
            i += 1;
        }
    }
    let numfields = match parse_fields_prefix(ctx, &mut i, None) {
        Ok(n) => n,
        Err(e) => return CmdResult::Err(e),
    };
    let fields = &ctx.args[i..];

    if let Err(e) = prune_hash_key(ctx, &key) {
        return CmdResult::Err(e);
    }
    let Some(PrimeValue::Hash(h)) = ctx.db.find_mut(&key, ctx.now_ms) else {
        return CmdResult::Ok(RespValue::Array(vec![integer(-2); numfields]));
    };
    let now_sec = (ctx.now_ms / 1000) as i64;
    let expire_ms = (ctx.now_ms / 1000).saturating_add(ttl_sec).saturating_mul(1000);
    let mut res = Vec::with_capacity(numfields);
    for f in fields {
        if !h.contains(f) {
            res.push(-2);
            continue;
        }
        let skip = match flag {
            ExpireFlag::Nx => h.field_expire_ms(f).is_some(),
            ExpireFlag::Xx => h.field_expire_ms(f).is_none(),
            // A field without a TTL has an infinite remaining time (UINT32_MAX
            // in the reference), so GT never applies and LT always does.
            ExpireFlag::Gt => match h.field_expire_ms(f) {
                Some(at_ms) => (at_ms as i64) / 1000 - now_sec >= ttl_sec as i64,
                None => true,
            },
            ExpireFlag::Lt => match h.field_expire_ms(f) {
                Some(at_ms) => (at_ms as i64) / 1000 - now_sec <= ttl_sec as i64,
                None => false,
            },
            ExpireFlag::Always => false,
        };
        if skip {
            res.push(0);
            continue;
        }
        if ttl_sec == 0 {
            h.remove(f);
            res.push(2);
        } else {
            let v = h.get(f).cloned().unwrap_or_else(|| CompactString::from_bytes(f));
            h.add_expirable(CompactString::from_bytes(f), v, Some(expire_ms), false);
            res.push(1);
        }
    }
    if h.is_empty() {
        ctx.db.remove(&key);
    }
    CmdResult::Ok(RespValue::Array(res.into_iter().map(integer).collect()))
}

/// Shared body of HTTL and HPEXPIRETIME; `ms` selects the HPEXPIRETIME reply
/// (absolute Unix ms) over the HTTL one (remaining seconds).
fn exec_httl_common(ctx: &mut OpContext, ms: bool) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = ctx.args[key_idx].clone();
    let mut i = key_idx + 1;
    let numfields = match parse_fields_prefix(ctx, &mut i, Some(K_INVALID_NUM_FIELDS)) {
        Ok(n) => n,
        Err(e) => return CmdResult::Err(e),
    };
    let fields = &ctx.args[i..];

    if let Err(e) = prune_hash_key(ctx, &key) {
        return CmdResult::Err(e);
    }
    let Some(PrimeValue::Hash(h)) = ctx.db.find(&key, ctx.now_ms) else {
        return CmdResult::Ok(RespValue::Array(vec![integer(-2); numfields]));
    };
    let now_sec = (ctx.now_ms / 1000) as i64;
    let mut res = Vec::with_capacity(numfields);
    for f in fields {
        if !h.contains(f) {
            res.push(-2);
        } else if let Some(at_ms) = h.field_expire_ms(f) {
            if ms {
                res.push(at_ms as i64);
            } else {
                res.push((at_ms as i64) / 1000 - now_sec);
            }
        } else {
            res.push(-1);
        }
    }
    CmdResult::Ok(RespValue::Array(res.into_iter().map(integer).collect()))
}

fn exec_httl(ctx: &mut OpContext) -> CmdResult {
    exec_httl_common(ctx, false)
}

fn exec_hpexpiretime(ctx: &mut OpContext) -> CmdResult {
    exec_httl_common(ctx, true)
}

/// HGETEX key [EX sec | PX ms | EXAT ts-sec | PXAT ts-ms | PERSIST]
///               FIELDS numfields field [field ...]
///
/// Returns the current values (nil for missing fields) while setting/persisting
/// their TTLs. A past/zero TTL deletes the field but still returns its value.
fn exec_hgetex(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = ctx.args[key_idx].clone();
    let mut i = key_idx + 1;
    let mut persist = false;
    let mut expiry: Option<(u8, i64)> = None;
    while i < ctx.args.len() {
        let tok = &ctx.args[i];
        if tok.eq_ignore_ascii_case(b"PERSIST") {
            if persist || expiry.is_some() {
                return CmdResult::Err(RespError::syntax());
            }
            persist = true;
            i += 1;
        } else if tok.eq_ignore_ascii_case(b"EX") || tok.eq_ignore_ascii_case(b"PX")
            || tok.eq_ignore_ascii_case(b"EXAT") || tok.eq_ignore_ascii_case(b"PXAT")
        {
            if persist || expiry.is_some() {
                return CmdResult::Err(RespError::syntax());
            }
            let unit = if tok.eq_ignore_ascii_case(b"EX") {
                EX_SEC
            } else if tok.eq_ignore_ascii_case(b"PX") {
                PX_MSEC
            } else if tok.eq_ignore_ascii_case(b"EXAT") {
                EX_AT_SEC
            } else {
                PX_AT_MSEC
            };
            i += 1;
            match ctx.args.get(i).and_then(|a| parse_i64(a)) {
                Some(v) => {
                    expiry = Some((unit, v));
                    i += 1;
                }
                None => return CmdResult::Err(RespError::integer()),
            }
        } else {
            break;
        }
    }
    let numfields = match parse_fields_prefix(ctx, &mut i, Some(K_INVALID_NUM_FIELDS)) {
        Ok(n) => n,
        Err(e) => return CmdResult::Err(e),
    };
    let fields = &ctx.args[i..];

    // Relative TTL in seconds: -1 persist, -2 no expiry option, else >= 0.
    let ttl_sec = if persist {
        -1
    } else if let Some((unit, value)) = expiry {
        match field_expiry_ttl_ms(unit, value, ctx.now_ms, true) {
            Some(ttl_ms) if ttl_ms > 0 => (ttl_ms + 999) / 1000,
            Some(_) => 0,
            None => return CmdResult::Err(RespError::new(invalid_expire_time("hgetex"))),
        }
    } else {
        -2
    };

    if let Err(e) = prune_hash_key(ctx, &key) {
        return CmdResult::Err(e);
    }
    let Some(PrimeValue::Hash(h)) = ctx.db.find_mut(&key, ctx.now_ms) else {
        return CmdResult::Ok(RespValue::Array(vec![RespValue::Nil; numfields]));
    };
    let values: Vec<Option<Vec<u8>>> =
        fields.iter().map(|f| h.get(f).map(|v| v.as_bytes().to_vec())).collect();
    let expire_ms = (ctx.now_ms / 1000).saturating_add(ttl_sec as u64).saturating_mul(1000);
    for f in fields {
        if !h.contains(f) {
            continue;
        }
        match ttl_sec {
            -1 => {
                let v = h.get(f).cloned().unwrap_or_default();
                h.add_expirable(CompactString::from_bytes(f), v, None, false);
            }
            -2 => {}
            0 => {
                h.remove(f);
            }
            _ => {
                let v = h.get(f).cloned().unwrap_or_default();
                h.add_expirable(CompactString::from_bytes(f), v, Some(expire_ms), false);
            }
        }
    }
    if h.is_empty() {
        ctx.db.remove(&key);
    }
    let out = values
        .into_iter()
        .map(|v| v.map_or(RespValue::Nil, RespValue::Bulk))
        .collect();
    CmdResult::Ok(RespValue::Array(out))
}

/// HRANDFIELD key [count [WITHVALUES]]
///
/// Without count: a random field (nil when missing). With count: up to |count|
/// random fields — unique when count is non-negative, with replacement when
/// negative. WITHVALUES appends each value, flattening the reply.
fn exec_hrandfield(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = ctx.args[key_idx].clone();
    let has_count = ctx.args.len() > key_idx + 1;
    let mut count: i64 = 0;
    let mut with_values = false;
    if has_count {
        match ctx.args.get(key_idx + 1).and_then(|a| parse_i64(a)) {
            Some(v) if (i32::MIN as i64..=i32::MAX as i64).contains(&v) => count = v,
            _ => return CmdResult::Err(RespError::new("ERR count value is not an integer")),
        }
        with_values =
            ctx.args.get(key_idx + 2).is_some_and(|a| a.eq_ignore_ascii_case(b"WITHVALUES"));
        if ctx.args.len() > key_idx + 2 + with_values as usize {
            return CmdResult::Err(RespError::syntax());
        }
    }

    if let Err(e) = prune_hash_key(ctx, &key) {
        return CmdResult::Err(e);
    }
    let Some(PrimeValue::Hash(h)) = ctx.db.find(&key, ctx.now_ms) else {
        return if has_count {
            CmdResult::Ok(RespValue::Array(vec![]))
        } else {
            CmdResult::Ok(RespValue::Nil)
        };
    };

    if !has_count {
        return match h.rand_pair() {
            Some((f, _)) => CmdResult::Ok(RespValue::Bulk(f.as_bytes().to_vec())),
            None => CmdResult::Ok(RespValue::Nil),
        };
    }

    let real_size = h.len();
    let actual_count = if count >= 0 {
        (count as usize).min(real_size)
    } else {
        count.unsigned_abs() as usize
    };
    let mut out = Vec::new();
    if real_size > 0 && actual_count > 0 {
        let pairs = if count >= 0 {
            h.rand_pairs_unique(actual_count)
        } else {
            h.rand_pairs(actual_count)
        };
        for (f, v) in pairs {
            out.push(RespValue::Bulk(f.as_bytes().to_vec()));
            if with_values {
                out.push(RespValue::Bulk(v.as_bytes().to_vec()));
            }
        }
    }
    CmdResult::Ok(RespValue::Array(out))
}

/// HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
fn exec_hscan(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = ctx.args[key_idx].clone();
    let cursor = match parse_u64(&ctx.args[key_idx + 1]) {
        Some(c) => c,
        None => return CmdResult::Err(RespError::new("ERR invalid cursor")),
    };
    let opts = &ctx.args[key_idx + 2..];
    if opts.len() > 5 {
        return CmdResult::Err(RespError::syntax());
    }
    let mut pattern: Option<&[u8]> = None;
    let mut count: usize = 10;
    let mut novalues = false;
    let mut i = 0;
    while i < opts.len() {
        if opts[i].eq_ignore_ascii_case(b"MATCH") {
            if i + 1 >= opts.len() {
                return CmdResult::Err(RespError::syntax());
            }
            pattern = Some(&opts[i + 1]);
            i += 2;
        } else if opts[i].eq_ignore_ascii_case(b"COUNT") {
            if i + 1 >= opts.len() {
                return CmdResult::Err(RespError::syntax());
            }
            match parse_u64(&opts[i + 1]) {
                Some(v) => count = v.max(1) as usize,
                None => return CmdResult::Err(RespError::integer()),
            }
            i += 2;
        } else if opts[i].eq_ignore_ascii_case(b"NOVALUES") {
            novalues = true;
            i += 1;
        } else {
            return CmdResult::Err(RespError::syntax());
        }
    }

    if let Err(e) = prune_hash_key(ctx, &key) {
        return CmdResult::Err(e);
    }
    let Some(PrimeValue::Hash(h)) = ctx.db.find(&key, ctx.now_ms) else {
        return CmdResult::Ok(hscan_reply(0, vec![]));
    };
    let mut entries: Vec<(CompactString, CompactString)> =
        h.iter().map(|(f, v)| (f.clone(), v.clone())).collect();
    if entries.is_empty() {
        return CmdResult::Ok(hscan_reply(0, vec![]));
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let start = (cursor as usize).min(entries.len());
    let mut out = Vec::new();
    let mut matched = 0usize;
    let mut pos = start;
    while pos < entries.len() && matched < count {
        let (f, v) = &entries[pos];
        pos += 1;
        if pattern.is_none_or(|p| glob_match(p, f.as_bytes())) {
            out.push(RespValue::Bulk(f.as_bytes().to_vec()));
            if !novalues {
                out.push(RespValue::Bulk(v.as_bytes().to_vec()));
            }
            matched += 1;
        }
    }
    let next = if pos >= entries.len() { 0u64 } else { pos as u64 };
    CmdResult::Ok(hscan_reply(next, out))
}

/// `[cursor_bulk, [field[, value] ...]]` reply shape for HSCAN.
fn hscan_reply(cursor: u64, items: Vec<RespValue>) -> RespValue {
    RespValue::Array(vec![
        RespValue::Bulk(crate::util::itoa(cursor as i64)),
        RespValue::Array(items),
    ])
}

pub static CMD_HSET: Command = Command {
    name: "HSET",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hset_common,
    merge: None,
};
pub static CMD_HMSET: Command = Command {
    name: "HMSET",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hmset,
    merge: None,
};
pub static CMD_HGET: Command = Command {
    name: "HGET",
    arity: 3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hget,
    merge: None,
};
pub static CMD_HMGET: Command = Command {
    name: "HMGET",
    arity: -3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hmget,
    merge: None,
};
pub static CMD_HDEL: Command = Command {
    name: "HDEL",
    arity: -3,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hdel,
    merge: None,
};
pub static CMD_HLEN: Command = Command {
    name: "HLEN",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hlen,
    merge: None,
};
pub static CMD_HGETALL: Command = Command {
    name: "HGETALL",
    arity: 2,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_hgetall,
    merge: None,
};
pub static CMD_HKEYS: Command = Command {
    name: "HKEYS",
    arity: 2,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_hkeys,
    merge: None,
};
pub static CMD_HVALS: Command = Command {
    name: "HVALS",
    arity: 2,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_hvals,
    merge: None,
};
pub static CMD_HEXISTS: Command = Command {
    name: "HEXISTS",
    arity: 3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hexists,
    merge: None,
};
pub static CMD_HINCRBY: Command = Command {
    name: "HINCRBY",
    arity: 4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hincrby,
    merge: None,
};
pub static CMD_HINCRBYFLOAT: Command = Command {
    name: "HINCRBYFLOAT",
    arity: 4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hincrbyfloat,
    merge: None,
};
pub static CMD_HSTRLEN: Command = Command {
    name: "HSTRLEN",
    arity: 3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hstrlen,
    merge: None,
};
pub static CMD_HSETNX: Command = Command {
    name: "HSETNX",
    arity: 4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hsetnx,
    merge: None,
};
pub static CMD_HSETEX: Command = Command {
    name: "HSETEX",
    arity: -5,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hsetex,
    merge: None,
};
pub static CMD_HEXPIRE: Command = Command {
    name: "HEXPIRE",
    arity: -5,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hexpire,
    merge: None,
};
pub static CMD_HTTL: Command = Command {
    name: "HTTL",
    arity: -4,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_httl,
    merge: None,
};
pub static CMD_HPEXPIRETIME: Command = Command {
    name: "HPEXPIRETIME",
    arity: -4,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hpexpiretime,
    merge: None,
};
pub static CMD_HGETEX: Command = Command {
    name: "HGETEX",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_hgetex,
    merge: None,
};
pub static CMD_HRANDFIELD: Command = Command {
    name: "HRANDFIELD",
    arity: -2,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_hrandfield,
    merge: None,
};
pub static CMD_HSCAN: Command = Command {
    name: "HSCAN",
    arity: -3,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_hscan,
    merge: None,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db::DbSlice;

    fn int(r: CmdResult) -> i64 {
        match r {
            CmdResult::Ok(RespValue::Integer(v)) => v,
            o => panic!("expected integer, got {:?}", o.into_resp_value()),
        }
    }

    fn bulk(r: CmdResult) -> Vec<u8> {
        match r {
            CmdResult::Ok(RespValue::Bulk(b)) => b,
            o => panic!("expected bulk, got {:?}", o.into_resp_value()),
        }
    }

    fn nil(r: CmdResult) -> bool {
        matches!(r, CmdResult::Ok(RespValue::Nil))
    }

    /// Flat array: Bulk/Integer decoded, Nil kept as None, in order.
    fn flat(r: CmdResult) -> Vec<Option<String>> {
        match r {
            CmdResult::Ok(RespValue::Array(v)) => v
                .into_iter()
                .map(|x| match x {
                    RespValue::Bulk(b) => Some(String::from_utf8_lossy(&b).into_owned()),
                    RespValue::Integer(i) => Some(i.to_string()),
                    RespValue::Nil => None,
                    o => panic!("unexpected element {:?}", o),
                })
                .collect(),
            o => panic!("expected array, got {:?}", o.into_resp_value()),
        }
    }

    fn arr(r: CmdResult) -> Vec<String> {
        let mut v: Vec<String> = flat(r).into_iter().map(|x| x.unwrap()).collect();
        v.sort();
        v
    }

    fn err(r: CmdResult) -> String {
        match r {
            CmdResult::Err(e) => e.message,
            o => panic!("expected error, got {:?}", o.into_resp_value()),
        }
    }

    /// Dispatch against a single DbSlice holding every key (single-shard path).
    fn dispatch_at(db: &mut DbSlice, now_ms: u64, argv: &[Vec<u8>]) -> CmdResult {
        let (exec, first_key_idx, owned): (fn(&mut OpContext) -> CmdResult, usize, Vec<usize>) =
            match argv[0].as_slice() {
                b"HSET" => (exec_hset_common, 1, (1..2).collect()),
                b"HMSET" => (exec_hmset, 1, (1..2).collect()),
                b"HGET" => (exec_hget, 1, (1..3).collect()),
                b"HMGET" => (exec_hmget, 1, (1..2).collect()),
                b"HDEL" => (exec_hdel, 1, (1..2).collect()),
                b"HLEN" => (exec_hlen, 1, (1..2).collect()),
                b"HGETALL" => (exec_hgetall, 1, (1..2).collect()),
                b"HEXISTS" => (exec_hexists, 1, (1..3).collect()),
                b"HSTRLEN" => (exec_hstrlen, 1, (1..3).collect()),
                b"HSETEX" => (exec_hsetex, 1, (1..2).collect()),
                b"HEXPIRE" => (exec_hexpire, 1, (1..2).collect()),
                b"HTTL" => (exec_httl, 1, (1..2).collect()),
                b"HPEXPIRETIME" => (exec_hpexpiretime, 1, (1..2).collect()),
                b"HGETEX" => (exec_hgetex, 1, (1..2).collect()),
                b"HRANDFIELD" => (exec_hrandfield, 1, (1..2).collect()),
                b"HSCAN" => (exec_hscan, 1, (1..2).collect()),
                _ => panic!("unhandled command {:?}", argv[0]),
            };
        let mut ctx = OpContext { db, args: argv, owned_keys: &owned, first_key_idx, now_ms };
        exec(&mut ctx)
    }

    macro_rules! run_at {
        ($db:expr, $now:expr, $($arg:expr),+) => {
            dispatch_at($db, $now, &[$(($arg).to_vec()),+])
        };
    }

    fn run(db: &mut DbSlice, args: &[&[u8]]) -> CmdResult {
        dispatch_at(db, 0, &args.iter().map(|a| a.to_vec()).collect::<Vec<_>>())
    }

    fn run_at_sec(db: &mut DbSlice, now_sec: u64, args: &[&[u8]]) -> CmdResult {
        dispatch_at(db, now_sec * 1000, &args.iter().map(|a| a.to_vec()).collect::<Vec<_>>())
    }

    fn str_of(db: &mut DbSlice, key: &str, value: &str) {
        db.insert(
            CompactString::from_bytes(key.as_bytes()),
            PrimeValue::Str(CompactString::from(value)),
        );
    }

    fn exists(db: &mut DbSlice, key: &str) -> bool {
        db.find(key.as_bytes(), 0).is_some()
    }

    #[test]
    fn hsetex_basic_replies() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run(&mut db, &[b"HSETEX", b"k", b"100", b"f1", b"v1"])));
        assert_eq!(0, int(run(&mut db, &[b"HSETEX", b"k", b"100", b"f1", b"v2"])));
        assert_eq!(0, int(run(&mut db, &[b"HSETEX", b"k", b"NX", b"100", b"f1", b"v3"])));
        assert_eq!(1, int(run(&mut db, &[b"HSETEX", b"k", b"NX", b"100", b"f2", b"v1"])));
        assert_eq!(vec![Some("100".into())], flat(run(&mut db, &[b"HTTL", b"k", b"FIELDS", b"1", b"f1"])));
    }

    #[test]
    fn hsetex_keepttl_keeps_existing() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run(&mut db, &[b"HSETEX", b"k", b"100", b"f1", b"v1"])));
        assert_eq!(0, int(run(&mut db, &[b"HSETEX", b"k", b"KEEPTTL", b"200", b"f1", b"v2"])));
        assert_eq!(vec![Some("100".into())], flat(run(&mut db, &[b"HTTL", b"k", b"FIELDS", b"1", b"f1"])));
    }

    #[test]
    fn hsetex_keepttl_applies_when_no_existing_ttl() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run(&mut db, &[b"HSET", b"k", b"f1", b"v1"])));
        assert_eq!(0, int(run(&mut db, &[b"HSETEX", b"k", b"KEEPTTL", b"100", b"f1", b"v2"])));
        assert_eq!(vec![Some("100".into())], flat(run(&mut db, &[b"HTTL", b"k", b"FIELDS", b"1", b"f1"])));
    }

    #[test]
    fn hsetex_fields_form() {
        let mut db = DbSlice::new(0);
        let now = 1_000_000_000_000u64;
        assert_eq!(1, int(run_at!(&mut db, now, b"HSETEX", b"k", b"EX", b"50", b"FIELDS", b"1", b"f1", b"v1")));
        assert_eq!(1, int(run_at!(&mut db, now, b"HSETEX", b"k", b"PX", b"30000", b"FIELDS", b"1", b"f2", b"v2")));
        assert_eq!(1, int(run_at!(&mut db, now, b"HSETEX", b"k", b"EXAT", b"1000000050", b"FIELDS", b"1", b"f3", b"v3")));
        assert_eq!(1, int(run_at!(&mut db, now, b"HSETEX", b"k", b"PXAT", b"1000000030000", b"FIELDS", b"1", b"f4", b"v4")));
        let ttl = flat(run_at!(&mut db, now, b"HTTL", b"k", b"FIELDS", b"4", b"f1", b"f2", b"f3", b"f4"));
        assert_eq!(
            ttl,
            vec![Some("50".into()), Some("30".into()), Some("50".into()), Some("30".into())]
        );
    }

    #[test]
    fn hsetex_errors() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run(&mut db, &[b"HSET", b"k", b"f", b"v"])));
        let e = err(run(&mut db, &[b"HSETEX", b"k", b"100"]));
        assert!(e.contains("wrong number of arguments"), "{}", e);
        assert_eq!(
            "ERR value is not an integer or out of range",
            err(run(&mut db, &[b"HSETEX", b"k", b"NX", b"zero", b"f", b"v"]))
        );
        assert_eq!("ERR syntax error", err(run(&mut db, &[b"HSETEX", b"k", b"NX", b"KEEPTTL", b"NX", b"1", b"v", b"v2"])));
        assert_eq!("ERR syntax error", err(run(&mut db, &[b"HSETEX", b"k", b"KEEPTTL", b"EX", b"10", b"FIELDS", b"1", b"f", b"v"])));
        // Bare-form ttl above the cap is rejected as out of range.
        let e = err(run(&mut db, &[b"HSETEX", b"k", b"268435456", b"f", b"v"]));
        assert!(e.contains("not an integer or out of range"), "{}", e);
    }

    #[test]
    fn hexpire_basic_and_flags() {
        let mut db = DbSlice::new(0);
        assert_eq!(3, int(run(&mut db, &[b"HSET", b"k", b"f1", b"v1", b"f2", b"v2", b"f3", b"v3"])));
        assert_eq!(
            vec![Some("1".into())],
            flat(run(&mut db, &[b"HEXPIRE", b"k", b"10", b"FIELDS", b"1", b"f1"]))
        );
        assert_eq!(vec![Some("10".into()), Some("-1".into()), Some("-1".into())],
            flat(run(&mut db, &[b"HTTL", b"k", b"FIELDS", b"3", b"f1", b"f2", b"f3"])));
        assert_eq!(
            vec![Some("-2".into())],
            flat(run(&mut db, &[b"HEXPIRE", b"k", b"10", b"FIELDS", b"1", b"nosuch"]))
        );
    }

    #[test]
    fn hexpire_gt_lt_on_no_ttl() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run(&mut db, &[b"HSET", b"k", b"f", b"v"])));
        // GT never applies to a TTL-less field (infinite remaining), LT always does.
        assert_eq!(
            vec![Some("0".into())],
            flat(run(&mut db, &[b"HEXPIRE", b"k", b"10", b"GT", b"FIELDS", b"1", b"f"]))
        );
        assert_eq!(
            vec![Some("1".into())],
            flat(run(&mut db, &[b"HEXPIRE", b"k", b"10", b"LT", b"FIELDS", b"1", b"f"]))
        );
        // Now f has TTL 10.
        assert_eq!(
            vec![Some("0".into())],
            flat(run(&mut db, &[b"HEXPIRE", b"k", b"5", b"GT", b"FIELDS", b"1", b"f"]))
        );
        assert_eq!(
            vec![Some("1".into())],
            flat(run(&mut db, &[b"HEXPIRE", b"k", b"20", b"GT", b"FIELDS", b"1", b"f"]))
        );
        assert_eq!(
            vec![Some("0".into())],
            flat(run(&mut db, &[b"HEXPIRE", b"k", b"20", b"NX", b"FIELDS", b"1", b"f"]))
        );
        assert_eq!(
            vec![Some("1".into())],
            flat(run(&mut db, &[b"HEXPIRE", b"k", b"20", b"XX", b"FIELDS", b"1", b"f"]))
        );
    }

    #[test]
    fn hexpire_zero_ttl_deletes_field_and_key() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run(&mut db, &[b"HSET", b"k", b"f", b"v"])));
        assert_eq!(
            vec![Some("2".into())],
            flat(run(&mut db, &[b"HEXPIRE", b"k", b"0", b"FIELDS", b"1", b"f"]))
        );
        assert!(!exists(&mut db, "k"));
        // HEXPIRE on a missing key does not create it and reports -2.
        assert_eq!(
            vec![Some("-2".into())],
            flat(run(&mut db, &[b"HEXPIRE", b"missing", b"10", b"FIELDS", b"1", b"f"]))
        );
        assert!(!exists(&mut db, "missing"));
    }

    #[test]
    fn hexpire_numfields_errors() {
        let mut db = DbSlice::new(0);
        assert_eq!(2, int(run(&mut db, &[b"HSET", b"key", b"k0", b"v0", b"k1", b"v1"])));
        let e = err(run(&mut db, &[b"HEXPIRE", b"key", b"10", b"1", b"k0"]));
        assert!(e.contains("Mandatory argument FIELDS"), "{}", e);
        for a in ["HEXPIRE key 10 FIELDS 2 k0", "HEXPIRE key 10 FIELDS 1 k0 k1", "HEXPIRE key 10 FIELDS 0 k0", "HEXPIRE key 10 FIELDS 0"] {
            let args: Vec<&[u8]> = a.split(' ').map(|s| s.as_bytes()).collect();
            let e = err(run(&mut db, &args));
            assert!(e.contains("numfields"), "{} -> {}", a, e);
        }
    }

    #[test]
    fn httl_and_hpexpiretime() {
        let mut db = DbSlice::new(0);
        // Non-existent key -> -2 for all fields.
        assert_eq!(
            vec![Some("-2".into()), Some("-2".into())],
            flat(run(&mut db, &[b"HTTL", b"nokey", b"FIELDS", b"2", b"f1", b"f2"]))
        );
        assert_eq!(2, int(run(&mut db, &[b"HSET", b"key", b"k0", b"v0", b"k1", b"v1"])));
        assert_eq!(
            vec![Some("-1".into()), Some("-1".into()), Some("-2".into())],
            flat(run(&mut db, &[b"HTTL", b"key", b"FIELDS", b"3", b"k0", b"k1", b"nosuch"]))
        );
        assert_eq!(
            vec![Some("1".into())],
            flat(run(&mut db, &[b"HEXPIRE", b"key", b"10", b"FIELDS", b"1", b"k0"]))
        );
        // Advance 3s: relative TTL drops, absolute timestamp is unchanged.
        let httl = flat(run_at_sec(&mut db, 3, &[b"HTTL", b"key", b"FIELDS", b"1", b"k0"]));
        assert_eq!(vec![Some("7".into())], httl);
        // The stored absolute expiry is (write_now_sec + ttl) * 1000 = 10000.
        let hpet = flat(run_at_sec(&mut db, 3, &[b"HPEXPIRETIME", b"key", b"FIELDS", b"1", b"k0"]));
        assert_eq!(vec![Some("10000".into())], hpet);
    }

    #[test]
    fn httl_deletes_empty_hash() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run(&mut db, &[b"HSETEX", b"key", b"1", b"f1", b"v1"])));
        // After expiry, HTTL triggers lazy pruning and removes the empty key.
        assert_eq!(
            vec![Some("-2".into())],
            flat(run_at_sec(&mut db, 2000, &[b"HTTL", b"key", b"FIELDS", b"1", b"f1"]))
        );
        assert!(!exists(&mut db, "key"));
    }

    #[test]
    fn hgetex_sets_and_reads() {
        let mut db = DbSlice::new(0);
        let now = 1_000_000_000_000u64;
        assert_eq!(3, int(run_at!(&mut db, now, b"HSET", b"key", b"f1", b"v1", b"f2", b"v2", b"f3", b"v3")));
        // No option: return values, leave TTLs untouched.
        assert_eq!(
            vec![Some("v1".into()), Some("v2".into()), None],
            flat(run_at!(&mut db, now, b"HGETEX", b"key", b"FIELDS", b"3", b"f1", b"f2", b"nosuch"))
        );
        assert_eq!(
            vec![Some("-1".into()), Some("-1".into())],
            flat(run_at!(&mut db, now, b"HTTL", b"key", b"FIELDS", b"2", b"f1", b"f2"))
        );
        // EX sets a relative TTL and still returns the value.
        assert_eq!(
            vec![Some("v1".into())],
            flat(run_at!(&mut db, now, b"HGETEX", b"key", b"EX", b"100", b"FIELDS", b"1", b"f1"))
        );
        assert_eq!(vec![Some("100".into())], flat(run_at!(&mut db, now, b"HTTL", b"key", b"FIELDS", b"1", b"f1")));
        // PERSIST removes the TTL.
        assert_eq!(
            vec![Some("v1".into())],
            flat(run_at!(&mut db, now, b"HGETEX", b"key", b"PERSIST", b"FIELDS", b"1", b"f1"))
        );
        assert_eq!(vec![Some("-1".into())], flat(run_at!(&mut db, now, b"HTTL", b"key", b"FIELDS", b"1", b"f1")));
    }

    #[test]
    fn hgetex_past_ttl_deletes_field_but_returns_value() {
        let mut db = DbSlice::new(0);
        let now = 1_000_000_000_000u64;
        assert_eq!(2, int(run_at!(&mut db, now, b"HSET", b"key", b"f2", b"v2", b"f3", b"v3")));
        assert_eq!(
            vec![Some("v2".into())],
            flat(run_at!(&mut db, now, b"HGETEX", b"key", b"PXAT", b"1", b"FIELDS", b"1", b"f2"))
        );
        assert_eq!(0, int(run_at!(&mut db, now, b"HEXISTS", b"key", b"f2")));
        assert_eq!(
            vec![Some("v3".into())],
            flat(run_at!(&mut db, now, b"HGETEX", b"key", b"EX", b"0", b"FIELDS", b"1", b"f3"))
        );
        assert_eq!(0, int(run_at!(&mut db, now, b"HEXISTS", b"key", b"f3")));
        // Missing key -> array of nils, key stays gone.
        assert_eq!(vec![None, None], flat(run_at!(&mut db, now, b"HGETEX", b"key", b"FIELDS", b"2", b"f1", b"f2")));
        assert!(!exists(&mut db, "key"));
    }

    #[test]
    fn hgetex_errors() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run(&mut db, &[b"HSET", b"key", b"f1", b"v1"])));
        assert_eq!("ERR syntax error", err(run(&mut db, &[b"HGETEX", b"key", b"PERSIST", b"EX", b"10", b"FIELDS", b"1", b"f1"])));
        assert_eq!("ERR syntax error", err(run(&mut db, &[b"HGETEX", b"key", b"EX", b"10", b"EX", b"20", b"FIELDS", b"1", b"f1"])));
        let e = err(run(&mut db, &[b"HGETEX", b"key", b"KEEPTTL", b"FIELDS", b"1", b"f1"]));
        assert!(e.contains("Mandatory argument FIELDS"), "{}", e);
        let e = err(run(&mut db, &[b"HGETEX", b"key", b"EX", b"-1", b"FIELDS", b"1", b"f1"]));
        assert!(e.contains("invalid expire time"), "{}", e);
        let e = err(run(&mut db, &[b"HGETEX", b"key", b"EX", b"abc", b"FIELDS", b"1", b"f1"]));
        assert!(e.contains("not an integer"), "{}", e);
        for unit in ["EX", "PX", "EXAT", "PXAT"] {
            let e = err(run(&mut db, &[b"HGETEX", b"key", unit.as_bytes(), b"9223372036854775807", b"FIELDS", b"1", b"f1"]));
            assert!(e.contains("invalid expire time"), "{} -> {}", unit, e);
        }
        let e = err(run(&mut db, &[b"HGETEX", b"key", b"EXAT", b"9999999999", b"FIELDS", b"1", b"f1"]));
        assert!(e.contains("invalid expire time"), "{}", e);
        let e = err(run(&mut db, &[b"HGETEX", b"key", b"notfields", b"1", b"f1"]));
        assert!(e.contains("Mandatory argument FIELDS"), "{}", e);
        for a in ["HGETEX key FIELDS 2 f1", "HGETEX key FIELDS 1 f1 EX 10"] {
            let args: Vec<&[u8]> = a.split(' ').map(|s| s.as_bytes()).collect();
            let e = err(run(&mut db, &args));
            assert!(e.contains("numfields"), "{} -> {}", a, e);
        }
        for a in ["HGETEX key FIELDS 0 f1", "HGETEX key FIELDS -1 f1", "HGETEX key FIELDS abc f1"] {
            let args: Vec<&[u8]> = a.split(' ').map(|s| s.as_bytes()).collect();
            let e = err(run(&mut db, &args));
            assert!(e.contains("Number of fields must be a positive integer"), "{} -> {}", a, e);
        }
        let e = err(run(&mut db, &[b"HGETEX", b"key", b"FIELDS", b"1"]));
        assert!(e.contains("must match the number of arguments"), "{}", e);
    }

    #[test]
    fn hash_commands_wrongtype() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "strkey", "val");
        for cmd in [b"HTTL".as_slice(), b"HPEXPIRETIME".as_slice(), b"HGETEX".as_slice(), b"HEXPIRE".as_slice()] {
            let args = if cmd == b"HEXPIRE" {
                vec![cmd.to_vec(), b"strkey".to_vec(), b"10".to_vec(), b"FIELDS".to_vec(), b"1".to_vec(), b"f".to_vec()]
            } else {
                vec![cmd.to_vec(), b"strkey".to_vec(), b"FIELDS".to_vec(), b"1".to_vec(), b"f".to_vec()]
            };
            let e = err(dispatch_at(&mut db, 0, &args));
            assert!(e.contains("WRONGTYPE"), "{:?} -> {}", cmd, e);
        }
        let e = err(run(&mut db, &[b"HRANDFIELD", b"strkey"]));
        assert!(e.contains("WRONGTYPE"), "{}", e);
        let e = err(run(&mut db, &[b"HSCAN", b"strkey", b"0"]));
        assert!(e.contains("WRONGTYPE"), "{}", e);
    }

    #[test]
    fn hrandfield_basic() {
        let mut db = DbSlice::new(0);
        assert!(nil(run(&mut db, &[b"HRANDFIELD", b"nokey"])));
        assert_eq!(3, int(run(&mut db, &[b"HSET", b"key", b"a", b"1", b"b", b"2", b"c", b"3"])));
        let single = bulk(run(&mut db, &[b"HRANDFIELD", b"key"]));
        assert!(["a", "b", "c"].contains(&String::from_utf8_lossy(&single).as_ref()), "{:?}", single);
        // Positive count: unique members.
        let v = arr(run(&mut db, &[b"HRANDFIELD", b"key", b"3"]));
        assert_eq!(v, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        // Negative count: with replacement, up to |count|.
        let v = arr(run(&mut db, &[b"HRANDFIELD", b"key", b"-25"]));
        assert_eq!(v.len(), 25);
        // WITHVALUES flattens into field,value pairs.
        let flat = flat(run(&mut db, &[b"HRANDFIELD", b"key", b"3", b"WITHVALUES"]));
        assert_eq!(flat.len(), 6);
        let e = err(run(&mut db, &[b"HRANDFIELD", b"key", b"abc"]));
        assert!(e.contains("count value is not an integer"), "{}", e);
    }

    #[test]
    fn hrandfield_after_expiry() {
        let mut db = DbSlice::new(0);
        for i in 0..10 {
            assert_eq!(1, int(run_at!(&mut db, 0, b"HSETEX", b"key", b"10", format!("k{}", i).into_bytes().as_slice(), b"v")));
        }
        // One permanent field.
        assert_eq!(1, int(run_at!(&mut db, 0, b"HSET", b"key", b"keep", b"v")));
        // All short-TTL fields expired: only "keep" remains.
        assert_eq!(b"keep".to_vec(), bulk(run_at!(&mut db, 10_000, b"HRANDFIELD", b"key")));
        // Count larger than live size must not crash.
        run_at!(&mut db, 10_000, b"HRANDFIELD", b"key", b"42");
        run_at!(&mut db, 10_000, b"HRANDFIELD", b"key", b"42", b"WITHVALUES");
        // All fields expired: nil.
        assert_eq!(1, int(run_at!(&mut db, 0, b"HSETEX", b"all", b"1", b"x", b"y")));
        assert_eq!(1, int(run_at!(&mut db, 0, b"HSETEX", b"all", b"1", b"z", b"w")));
        assert!(nil(run_at!(&mut db, 2000, b"HRANDFIELD", b"all")));
    }

    #[test]
    fn hscan_cursor_and_options() {
        let mut db = DbSlice::new(0);
        assert_eq!(3, int(run(&mut db, &[b"HSET", b"key", b"a", b"1", b"b", b"2", b"c", b"3"])));
        let (cursor, entries) = match run(&mut db, &[b"HSCAN", b"key", b"0"]) {
            CmdResult::Ok(RespValue::Array(v)) => {
                let c = match &v[0] {
                    RespValue::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
                    o => panic!("bad cursor {:?}", o),
                };
                let e = flat(CmdResult::Ok(v[1].clone()));
                (c, e)
            }
            o => panic!("bad hscan {:?}", o.into_resp_value()),
        };
        assert_eq!("0", cursor);
        assert_eq!(6, entries.len());
        // NOVALUES keeps only field names.
        let no_values = match run(&mut db, &[b"HSCAN", b"key", b"0", b"NOVALUES"]) {
            CmdResult::Ok(RespValue::Array(v)) => v,
            o => panic!("bad hscan {:?}", o.into_resp_value()),
        };
        assert_eq!(2, no_values.len());
        // Invalid cursor.
        let e = err(run(&mut db, &[b"HSCAN", b"key", b"abc"]));
        assert!(e.contains("invalid cursor"), "{}", e);
    }
}

