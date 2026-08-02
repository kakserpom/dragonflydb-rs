use std::collections::HashMap;

use crate::commands::{integer, ok, Command, OpContext, ShardPart, KeyRange, FLAG_DENYOOM, FLAG_FAST, FLAG_GLOBAL, FLAG_MOVABLEKEYS, FLAG_MULTI_KEY, FLAG_READONLY, FLAG_WRITE};
use crate::core::compact::CompactString;
use crate::core::db::DbSlice;
use crate::core::quicklist::{ListItem, QuickList};
use crate::core::rdb::{dump_value, restore_value, RestoreError, RestoreOutcome};
use crate::core::value::ObjType;
use crate::core::PrimeValue;
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::{parse_double, parse_i64, parse_u64, shard_hash};
use xxhash_rust::xxh3::xxh3_64;

// ---------------------------------------------------------------------------
// DEL / EXISTS
// ---------------------------------------------------------------------------

fn exec_del(ctx: &mut OpContext) -> CmdResult {
    let mut removed = 0i64;
    for &ki in ctx.owned_keys {
        if ctx.db.remove_if_exists(&ctx.args[ki]) {
            removed += 1;
        }
    }
    CmdResult::Ok(integer(removed))
}

fn merge_sum(parts: &[ShardPart], _args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    let mut total = 0i64;
    for p in parts {
        match &p.result {
            CmdResult::Ok(RespValue::Integer(i)) => total += i,
            CmdResult::Err(e) => return CmdResult::Err(e.clone()),
            _ => return CmdResult::Err(RespError::new("ERR internal: bad sum shard result")),
        }
    }
    CmdResult::Ok(integer(total))
}

// ---------------------------------------------------------------------------
// DELEX — conditional delete (IFEQ/IFNE/IFDEQ/IFDNE)
// ---------------------------------------------------------------------------

fn exec_delex(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];

    // `DELEX key` with no condition behaves like DEL.
    if ctx.args.len() == 2 {
        return CmdResult::Ok(integer(if ctx.db.remove_if_exists(key) { 1 } else { 0 }));
    }
    // Otherwise the syntax is `DELEX key <cond> <value>` exactly.
    if ctx.args.len() != 4 {
        return CmdResult::Err(RespError::new(
            "ERR wrong number of arguments for 'delex' command",
        ));
    }

    let opt = &ctx.args[2];
    let compare_value = &ctx.args[3];
    let digest_mode = opt.eq_ignore_ascii_case(b"IFDEQ") || opt.eq_ignore_ascii_case(b"IFDNE");
    let negate = opt.eq_ignore_ascii_case(b"IFNE") || opt.eq_ignore_ascii_case(b"IFDNE");
    if !opt.eq_ignore_ascii_case(b"IFEQ")
        && !opt.eq_ignore_ascii_case(b"IFNE")
        && !digest_mode
    {
        return CmdResult::Err(RespError::new(format!(
            "ERR Unknown subcommand or wrong number of arguments for '{}'. Try DELEX HELP.",
            String::from_utf8_lossy(opt)
        )));
    }

    let matches = match ctx.db.find(key, ctx.now_ms) {
        None => return CmdResult::Ok(integer(0)),
        Some(PrimeValue::Str(s)) => {
            if digest_mode {
                format!("{:016x}", xxh3_64(s.as_bytes())).as_bytes() == compare_value.as_slice()
            } else {
                s.as_bytes() == compare_value.as_slice()
            }
        }
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
    };

    if matches != negate {
        ctx.db.remove(key);
        return CmdResult::Ok(integer(1));
    }
    CmdResult::Ok(integer(0))
}

fn exec_exists(ctx: &mut OpContext) -> CmdResult {
    let mut count = 0i64;
    for &ki in ctx.owned_keys {
        if ctx.db.contains(&ctx.args[ki], ctx.now_ms) {
            count += 1;
        }
    }    CmdResult::Ok(integer(count))
}

// ---------------------------------------------------------------------------
// EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT / TTL / PTTL / PERSIST
// ---------------------------------------------------------------------------

fn expire_common(ctx: &mut OpContext, unit_ms: bool, is_at: bool) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let t = match parse_i64(&ctx.args[key_idx + 1]) {
        Some(v) => v,
        None => return CmdResult::Err(RespError::integer()),
    };
    // condition flags: NX XX GT LT
    let mut cond = None;
    if ctx.args.len() > key_idx + 2 {
        let c = ctx.args[key_idx + 2].to_ascii_uppercase();
        cond = match c.as_slice() {
            b"NX" => Some("NX"),
            b"XX" => Some("XX"),
            b"GT" => Some("GT"),
            b"LT" => Some("LT"),
            _ => return CmdResult::Err(RespError::syntax()),
        };
    }
    let expire_at_ms: i64 = if is_at {
        if unit_ms {
            t
        } else {
            t.saturating_mul(1000)
        }
    } else {
        let delta = if unit_ms { t } else { t.saturating_mul(1000) };
        (ctx.now_ms as i64).saturating_add(delta)
    };
    if !ctx.db.contains(key, ctx.now_ms) {
        return CmdResult::Ok(integer(0));
    }
    // apply condition
    if let Some(cond) = cond {
        match cond {
            "NX" => {
                if ctx.db.has_expiry(key, ctx.now_ms) {
                    return CmdResult::Ok(integer(0));
                }
            }
            "XX" => {
                if !ctx.db.has_expiry(key, ctx.now_ms) {
                    return CmdResult::Ok(integer(0));
                }
            }
            "GT" | "LT" => {
                if let Some(cur) = ctx.db.expire_at(key) {
                    let cur = cur as i64;
                    let ok = if cond == "GT" {
                        expire_at_ms > cur
                    } else {
                        expire_at_ms < cur
                    };
                    if !ok {
                        return CmdResult::Ok(integer(0));
                    }
                } else if cond == "GT" {
                    // no TTL set: GT treats as infinite TTL, so never larger
                    return CmdResult::Ok(integer(0));
                }
            }
            _ => unreachable!(),
        }
    }
    ctx.db.set_expiry(key, expire_at_ms.max(0) as u64, ctx.now_ms);
    CmdResult::Ok(integer(1))
}

fn exec_expire(ctx: &mut OpContext) -> CmdResult {
    expire_common(ctx, false, false)
}
fn exec_pexpire(ctx: &mut OpContext) -> CmdResult {
    expire_common(ctx, true, false)
}
fn exec_expireat(ctx: &mut OpContext) -> CmdResult {
    expire_common(ctx, false, true)
}
fn exec_pexpireat(ctx: &mut OpContext) -> CmdResult {
    expire_common(ctx, true, true)
}

fn ttl_common(ctx: &mut OpContext, ms: bool) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    let ttl = ctx.db.ttl_ms(key, ctx.now_ms);
    if ttl < 0 {
        return CmdResult::Ok(integer(ttl));
    }
    if ms {
        CmdResult::Ok(integer(ttl))
    } else {
        CmdResult::Ok(integer(ttl.saturating_add(500) / 1000))
    }
}

fn exec_ttl(ctx: &mut OpContext) -> CmdResult {
    ttl_common(ctx, false)
}
fn exec_pttl(ctx: &mut OpContext) -> CmdResult {
    ttl_common(ctx, true)
}

fn exec_persist(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    if ctx.db.has_expiry(key, ctx.now_ms) {
        ctx.db.clear_expiry(key);
        CmdResult::Ok(integer(1))
    } else if ctx.db.contains(key, ctx.now_ms) {
        CmdResult::Ok(integer(0))
    } else {
        CmdResult::Ok(integer(0))
    }
}

// ---------------------------------------------------------------------------
// TYPE
// ---------------------------------------------------------------------------

fn exec_type(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    match ctx.db.find(key, ctx.now_ms) {
        Some(v) => CmdResult::Ok(RespValue::Simple(v.type_name().to_string())),
        None => CmdResult::Ok(RespValue::Simple("none".to_string())),
    }
}

// ---------------------------------------------------------------------------
// DUMP / RESTORE
// ---------------------------------------------------------------------------

fn exec_dump(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    match ctx.db.find(key, ctx.now_ms) {
        Some(v) => CmdResult::Ok(RespValue::Bulk(dump_value(v))),
        None => CmdResult::Ok(RespValue::Nil),
    }
}

fn exec_restore(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let ttl = match parse_i64(&ctx.args[key_idx + 1]) {
        Some(v) => v,
        None => return CmdResult::Err(RespError::integer()),
    };
    if ttl < 0 {
        return CmdResult::Err(RespError::new("ERR Invalid TTL value, must be >= 0"));
    }
    let payload = &ctx.args[key_idx + 2];

    let mut replace = false;
    let mut absttl = false;
    for opt in &ctx.args[key_idx + 3..] {
        match opt.to_ascii_uppercase().as_slice() {
            b"REPLACE" => replace = true,
            b"ABSTTL" => absttl = true,
            _ => return CmdResult::Err(RespError::syntax()),
        }
    }

    if !replace && ctx.db.contains(key, ctx.now_ms) {
        return CmdResult::Err(RespError::new("BUSYKEY Target key name already exists."));
    }

    let value = match restore_value(payload, ctx.now_ms) {
        Ok(RestoreOutcome::Value(v)) => v,
        Ok(RestoreOutcome::Expired) => return CmdResult::Ok(ok()),
        Err(RestoreError::Expired) => return CmdResult::Ok(ok()),
        Err(RestoreError::BadDataFormat) => {
            return CmdResult::Err(RespError::new("ERR Bad data format"));
        }
    };

    let key_cs = CompactString::from_bytes(key);
    ctx.db.insert(key_cs.clone(), value);
    if ttl > 0 {
        let expire_at_ms = if absttl {
            ttl
        } else {
            (ctx.now_ms as i64).saturating_add(ttl)
        };
        ctx.db.set_expiry(&key_cs, expire_at_ms.max(0) as u64, ctx.now_ms);
    }
    CmdResult::Ok(ok())
}

// ---------------------------------------------------------------------------
// KEYS (global: scans all shards)
// ---------------------------------------------------------------------------

fn exec_keys(ctx: &mut OpContext) -> CmdResult {
    let pattern = &ctx.args[1];
    let out: Vec<RespValue> = ctx
        .db
        .iter()
        .filter(|(k, _)| glob_match(pattern, k.as_bytes()))
        .map(|(k, _)| RespValue::Bulk(k.as_bytes().to_vec()))
        .collect();
    CmdResult::Ok(RespValue::Array(out))
}

fn merge_concat(parts: &[ShardPart], _args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    let mut out = Vec::new();
    for p in parts {
        match &p.result {
            CmdResult::Ok(RespValue::Array(arr)) => out.extend(arr.iter().cloned()),
            CmdResult::Err(e) => return CmdResult::Err(e.clone()),
            _ => return CmdResult::Err(RespError::new("ERR internal: bad concat shard result")),
        }
    }
    CmdResult::Ok(RespValue::Array(out))
}

/// Glob-style pattern matching compatible with Redis KEYS semantics
/// (`*`, `?`, `[...]`, `[^...]`, escapes with `\`).
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0usize;
    let mut s = 0usize;
    let mut star: Option<usize> = None;
    let mut mark = 0usize;
    while s < text.len() {
        let matched = if p < pattern.len() {
            match pattern[p] {
                b'*' => {
                    star = Some(p);
                    mark = s;
                    p += 1;
                    continue;
                }
                b'?' => true,
                b'[' => match match_class(pattern, p, text[s]) {
                    Some((true, new_p)) => {
                        p = new_p;
                        s += 1;
                        continue;
                    }
                    Some((false, _)) => false,
                    None => {
                        // malformed class: treat '[' literally
                        pattern[p] == text[s]
                    }
                },
                b'\\' if p + 1 < pattern.len() => {
                    p += 1;
                    true
                }
                c => c == text[s],
            }
        } else {
            false
        };
        if matched {
            p += 1;
            s += 1;
        } else if let Some(sp) = star {
            p = sp + 1;
            mark += 1;
            s = mark;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

/// Match `ch` against a character class at pattern[p] == '['.
/// Returns Some((matched, new_pattern_pos)) or None if malformed.
fn match_class(pattern: &[u8], p: usize, ch: u8) -> Option<(bool, usize)> {
    let mut i = p + 1;
    let mut negate = false;
    if i < pattern.len() && pattern[i] == b'^' {
        negate = true;
        i += 1;
    }
    let mut matched = false;
    let mut first = true;
    while i < pattern.len() && (pattern[i] != b']' || first) {
        if first {
            first = false;
        }
        // handle ranges a-z
        if i + 2 < pattern.len() && pattern[i + 1] == b'-' && pattern[i + 2] != b']' {
            let (lo, hi) = (pattern[i], pattern[i + 2]);
            if ch >= lo && ch <= hi {
                matched = true;
            }
            i += 3;
        } else {
            let c = pattern[i];
            if c == b'\\' && i + 1 < pattern.len() {
                i += 1;
            }
            if pattern[i] == ch {
                matched = true;
            }
            i += 1;
        }
    }
    if i >= pattern.len() {
        return None; // malformed
    }
    Some((matched != negate, i + 1))
}

// ---------------------------------------------------------------------------
// TOUCH / UNLINK: same semantics as EXISTS / DEL (reference registers TOUCH
// with the EXISTS handler and UNLINK with the DEL handler).
// ---------------------------------------------------------------------------

fn exec_stick(ctx: &mut OpContext) -> CmdResult {
    let mut count = 0i64;
    for &ki in ctx.owned_keys {
        if ctx.db.set_sticky(&ctx.args[ki], ctx.now_ms) {
            count += 1;
        }
    }
    CmdResult::Ok(integer(count))
}

// ---------------------------------------------------------------------------
// RENAME / RENAMENX
// ---------------------------------------------------------------------------

fn rename_common(ctx: &mut OpContext, destination_should_not_exist: bool) -> CmdResult {
    let src_idx = ctx.first_key_idx;
    let dst_idx = src_idx + 1;
    let src = &ctx.args[src_idx];
    let dst = &ctx.args[dst_idx];
    let owns_src = ctx.owned_keys.contains(&src_idx);
    let owns_dst = ctx.owned_keys.contains(&dst_idx);

    if !owns_src {
        // Destination shard: report whether the destination exists.
        let dst_exists = ctx.db.contains(dst, ctx.now_ms);
        return CmdResult::Ok(integer(dst_exists as i64));
    }

    if !ctx.db.contains(src, ctx.now_ms) {
        return CmdResult::Err(RespError::new("ERR no such key"));
    }
    if src == dst {
        // RENAME is a no-op, RENAMENX treats it as an existing destination.
        return if destination_should_not_exist {
            CmdResult::Ok(integer(0))
        } else {
            CmdResult::Ok(ok())
        };
    }

    if owns_dst {
        // Single-shard fast path: both keys live on this shard.
        if destination_should_not_exist && ctx.db.contains(dst, ctx.now_ms) {
            return CmdResult::Ok(integer(0));
        }
        let exp = ctx.db.expire_at(src);
        let sticky = ctx.db.is_sticky(src);
        let val = ctx.db.remove(src).expect("key exists");
        ctx.db.insert(CompactString::from_bytes(dst), val);
        match exp {
            Some(at) => ctx.db.set_expiry(dst, at, ctx.now_ms),
            None => ctx.db.clear_expiry(dst),
        }
        ctx.db.set_sticky_flag(dst, sticky);
        return if destination_should_not_exist {
            CmdResult::Ok(integer(1))
        } else {
            CmdResult::Ok(ok())
        };
    }

    // Cross-shard: report a store plan without mutating. The coordinator
    // applies it only if the merge confirms (RENAMENX requires an absent
    // destination). The source deletion and destination write (with the source
    // TTL and stickiness) happen atomically via deferred stores.
    let exp = ctx.db.expire_at(src);
    let sticky = ctx.db.is_sticky(src);
    let val = ctx.db.find(src, ctx.now_ms).expect("key exists").clone();
    let reply = if destination_should_not_exist { integer(1) } else { ok() };
    CmdResult::deferred_stores(
        vec![(src.to_vec(), None, None, false), (dst.to_vec(), Some(val), exp, sticky)],
        reply,
    )
}

fn exec_rename(ctx: &mut OpContext) -> CmdResult {
    rename_common(ctx, false)
}

fn exec_renamenx(ctx: &mut OpContext) -> CmdResult {
    rename_common(ctx, true)
}

fn merge_rename(parts: &[ShardPart], args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    let dst_idx = keys[1];
    let mut dst_exists: Option<bool> = None;
    let mut stores: Option<CmdResult> = None;
    let mut first_err = None;
    for p in parts {
        if p.result.is_err() {
            if first_err.is_none() {
                first_err = Some(p.result.clone());
            }
            continue;
        }
        if p.owned_key_idxs.contains(&dst_idx)
            && let CmdResult::Ok(RespValue::Integer(v)) = &p.result
        {
            dst_exists = Some(*v != 0);
        }
        if matches!(p.result, CmdResult::DeferredStores { .. }) {
            stores = Some(p.result.clone());
        }
    }
    if let Some(e) = first_err {
        return e;
    }
    if args[0].eq_ignore_ascii_case(b"RENAMENX") && dst_exists == Some(true) {
        return CmdResult::Ok(integer(0));
    }
    if let Some(s) = stores {
        return s;
    }
    parts
        .first()
        .map(|p| p.result.clone())
        .unwrap_or_else(|| CmdResult::err("ERR internal: rename merge"))
}

// ---------------------------------------------------------------------------
// COPY
// ---------------------------------------------------------------------------

fn exec_copy(ctx: &mut OpContext) -> CmdResult {
    let src_idx = ctx.first_key_idx;
    let dst_idx = src_idx + 1;
    let src = &ctx.args[src_idx];
    let dst = &ctx.args[dst_idx];

    let mut i = dst_idx + 1;
    let mut replace = false;
    while i < ctx.args.len() {
        if ctx.args[i].eq_ignore_ascii_case(b"REPLACE") && !replace {
            replace = true;
            i += 1;
        } else {
            return CmdResult::Err(RespError::syntax());
        }
    }

    if src == dst {
        return CmdResult::Err(RespError::new("source and destination objects are the same"));
    }

    let owns_src = ctx.owned_keys.contains(&src_idx);
    let owns_dst = ctx.owned_keys.contains(&dst_idx);

    if !owns_src {
        // Destination shard: report whether the destination exists.
        let dst_exists = ctx.db.contains(dst, ctx.now_ms);
        return CmdResult::Ok(integer(dst_exists as i64));
    }

    if !ctx.db.contains(src, ctx.now_ms) {
        return CmdResult::Ok(integer(0));
    }

    if owns_dst {
        // Single-shard fast path: both keys live on this shard.
        if ctx.db.contains(dst, ctx.now_ms) && !replace {
            return CmdResult::Ok(integer(0));
        }
        let exp = ctx.db.expire_at(src);
        let sticky = ctx.db.is_sticky(src);
        let val = ctx.db.find(src, ctx.now_ms).expect("key exists").clone();
        ctx.db.insert(CompactString::from_bytes(dst), val);
        match exp {
            Some(at) => ctx.db.set_expiry(dst, at, ctx.now_ms),
            None => ctx.db.clear_expiry(dst),
        }
        ctx.db.set_sticky_flag(dst, sticky);
        return CmdResult::Ok(integer(1));
    }

    // Cross-shard: report the copy plan (destination write with the source TTL
    // and stickiness).
    let exp = ctx.db.expire_at(src);
    let sticky = ctx.db.is_sticky(src);
    let val = ctx.db.find(src, ctx.now_ms).expect("key exists").clone();
    CmdResult::deferred_stores(vec![(dst.to_vec(), Some(val), exp, sticky)], integer(1))
}

fn merge_copy(parts: &[ShardPart], args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    let dst_idx = keys[1];
    let mut replace = false;
    for a in &args[dst_idx + 1..] {
        if a.eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
        }
    }
    let mut dst_exists: Option<bool> = None;
    let mut stores: Option<CmdResult> = None;
    let mut first_err = None;
    for p in parts {
        if p.result.is_err() {
            if first_err.is_none() {
                first_err = Some(p.result.clone());
            }
            continue;
        }
        if p.owned_key_idxs.contains(&dst_idx)
            && let CmdResult::Ok(RespValue::Integer(v)) = &p.result
        {
            dst_exists = Some(*v != 0);
        }
        if matches!(p.result, CmdResult::DeferredStores { .. }) {
            stores = Some(p.result.clone());
        }
    }
    if let Some(e) = first_err {
        return e;
    }
    if !replace && dst_exists == Some(true) {
        return CmdResult::Ok(integer(0));
    }
    if let Some(s) = stores {
        return s;
    }
    parts
        .first()
        .map(|p| p.result.clone())
        .unwrap_or_else(|| CmdResult::err("ERR internal: copy merge"))
}

// ---------------------------------------------------------------------------
// EXPIRETIME / PEXPIRETIME
// ---------------------------------------------------------------------------

fn expiretime_common(ctx: &mut OpContext, ms: bool) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    if !ctx.db.contains(key, ctx.now_ms) {
        return CmdResult::Ok(integer(-2));
    }
    match ctx.db.expire_at(key) {
        Some(at) => {
            if ms {
                CmdResult::Ok(integer(at as i64))
            } else {
                CmdResult::Ok(integer((at as i64 + 500) / 1000))
            }
        }
        None => CmdResult::Ok(integer(-1)),
    }
}

fn exec_expiretime(ctx: &mut OpContext) -> CmdResult {
    expiretime_common(ctx, false)
}

fn exec_pexpiretime(ctx: &mut OpContext) -> CmdResult {
    expiretime_common(ctx, true)
}

// ---------------------------------------------------------------------------
// RANDOMKEY
// ---------------------------------------------------------------------------

fn exec_randomkey(ctx: &mut OpContext) -> CmdResult {
    let keys: Vec<Vec<u8>> = ctx.db.iter().map(|(k, _)| k.as_bytes().to_vec()).collect();
    if keys.is_empty() {
        return CmdResult::Ok(RespValue::Nil);
    }
    let idx = (crate::util::shard_hash(&ctx.now_ms.to_le_bytes()) as usize) % keys.len();
    CmdResult::Ok(RespValue::Bulk(keys[idx].clone()))
}

fn merge_rand(parts: &[ShardPart], _args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    for p in parts {
        if let CmdResult::Ok(RespValue::Bulk(_)) = &p.result {
            return p.result.clone();
        }
    }
    CmdResult::Ok(RespValue::Nil)
}

// ---------------------------------------------------------------------------
// MOVE — move a key to another DB (runs on the shard, which has all DBs)
// ---------------------------------------------------------------------------

/// Move `key` from `src` to `dst` (different DBs on the same shard), carrying
/// value, TTL and sticky flag. Returns 0 if the key is missing or the
/// destination key exists, 1 otherwise.
pub fn move_key(src: &mut DbSlice, dst: &mut DbSlice, key: &[u8], now_ms: u64) -> i64 {
    if !src.contains(key, now_ms) || dst.contains(key, now_ms) {
        return 0;
    }
    let Some((value, expire_at, sticky)) = src.take(key, now_ms) else {
        return 0;
    };
    dst.insert(CompactString::from_bytes(key), value);
    if let Some(at) = expire_at {
        dst.set_expiry(key, at, now_ms);
    }
    dst.set_sticky_flag(key, sticky);
    1
}

/// Validate and execute `MOVE key <db>` against a shard's DB vector. Both
/// `db_idx` and the target DB must already exist (the shard ensures this).
/// Mirrors `GenericFamily::Move`: DB range and same-DB errors are sent as-is
/// (no `ERR ` prefix), matching upstream `kDbIndOutOfRangeErr` and
/// "source and destination objects are the same".
pub fn exec_move_on_dbs(
    dbs: &mut [DbSlice],
    db_idx: usize,
    args: &[Vec<u8>],
    now_ms: u64,
) -> CmdResult {
    let Some(target) = args.get(2).and_then(|a| parse_i64(a)) else {
        return CmdResult::err("DB index is out of range");
    };
    if target < 0 || (target as usize) >= crate::server::MAX_DB {
        return CmdResult::err("DB index is out of range");
    }
    let target = target as usize;
    if target == db_idx {
        return CmdResult::err("source and destination objects are the same");
    }
    if dbs.len() <= db_idx || dbs.len() <= target {
        return CmdResult::err("ERR internal: MOVE target DB not active");
    }
    let (lo, hi) = if db_idx < target { (db_idx, target) } else { (target, db_idx) };
    let (left, right) = dbs.split_at_mut(hi);
    let (src, dst) = if db_idx < target {
        (&mut left[lo], &mut right[0])
    } else {
        (&mut right[0], &mut left[lo])
    };
    CmdResult::Ok(RespValue::Integer(move_key(src, dst, &args[1], now_ms)))
}

/// Never invoked: MOVE runs as a global transaction handled by `Shard::run_move`,
/// which has access to both the source and target DBs.
fn exec_move(_ctx: &mut OpContext) -> CmdResult {
    CmdResult::err("ERR internal: MOVE handled at shard level")
}

// ---------------------------------------------------------------------------
// SCAN (global: per-shard cursors merged by the coordinator)
// ---------------------------------------------------------------------------

/// Dragonfly encodes the shard index in the low 10 bits of the SCAN cursor
/// (`cursor % 1024`), with the continuation position in the high bits.
const SCAN_SHARD_BITS: u32 = 10;

#[derive(Clone, Copy)]
enum ScanMask {
    Volatile,
    Permanent,
    Accessed,
    Untouched,
}

/// A TYPE filter. `Some(t)` matches values of type `t`; `None` matches nothing
/// (a valid but unrepresentable pseudo-type like "key").
type ScanType = Option<ObjType>;

struct ScanOpts {
    limit: usize,
    pattern: Option<Vec<u8>>,
    type_filter: Option<ScanType>,
    mask: Option<ScanMask>,
    min_malloc_size: usize,
}

/// Map a TYPE argument (case-insensitive) to a filter, or `None` for an
/// unknown name. Pseudo-types from `kObjTypeToString` ("key", "ReJSON-RL")
/// are valid but never match a stored value.
fn scan_type_from_name(s: &[u8]) -> Option<ScanType> {
    if let Some(t) = ObjType::from_name(s) {
        return Some(Some(t));
    }
    match s.to_ascii_lowercase().as_slice() {
        b"key" | b"rejson-rl" => Some(None),
        _ => None,
    }
}

/// Parse the optional SCAN clauses (COUNT/MATCH/TYPE/BUCKET/ATTR/MINMSZ),
/// mirroring `ScanOpts::TryFrom`. NOVALUES is rejected for SCAN (it is only
/// valid for HSCAN).
fn parse_scan_opts(args: &[Vec<u8>]) -> Result<ScanOpts, RespError> {
    let mut opts = ScanOpts {
        limit: 10,
        pattern: None,
        type_filter: None,
        mask: None,
        min_malloc_size: 0,
    };
    let mut i = 2;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        let next = || args.get(i + 1).ok_or_else(RespError::syntax).map(|a| a.as_slice());
        match opt.as_slice() {
            b"COUNT" => {
                let n = parse_u64(next()?).ok_or_else(RespError::integer)? as usize;
                opts.limit = n.max(1);
                i += 2;
            }
            b"MATCH" => {
                let p = next()?;
                if p != b"*" {
                    opts.pattern = Some(p.to_vec());
                }
                i += 2;
            }
            b"TYPE" => {
                opts.type_filter = Some(scan_type_from_name(next()?).ok_or_else(RespError::syntax)?);
                i += 2;
            }
            b"BUCKET" => {
                let _ = parse_u64(next()?).ok_or_else(RespError::integer)?;
                i += 2;
            }
            b"ATTR" => {
                let m = next()?;
                opts.mask = Some(match m.to_ascii_lowercase().as_slice() {
                    b"v" => ScanMask::Volatile,
                    b"p" => ScanMask::Permanent,
                    b"a" => ScanMask::Accessed,
                    b"u" => ScanMask::Untouched,
                    _ => return Err(RespError::syntax()),
                });
                i += 2;
            }
            b"MINMSZ" => {
                opts.min_malloc_size = parse_u64(next()?).ok_or_else(RespError::integer)? as usize;
                i += 2;
            }
            _ => return Err(RespError::syntax()),
        }
    }
    Ok(opts)
}

/// Apply the SCAN filters to one key, mirroring `ScanCb`: type, ATTR mask
/// (volatile/permanent by TTL, accessed/untouched by the touched flag), MINMSZ
/// and the MATCH glob. (BUCKET is parsed but not filtered on.)
fn scan_key_matches(db: &DbSlice, key: &CompactString, val: &PrimeValue, opts: &ScanOpts) -> bool {
    if let Some(filter) = opts.type_filter {
        let ok = match filter {
            Some(t) => val.obj_type() == t,
            None => false,
        };
        if !ok {
            return false;
        }
    }
    if let Some(mask) = opts.mask {
        let has_ttl = db.expire_at(key.as_bytes()).is_some();
        let ok = match mask {
            ScanMask::Volatile => has_ttl,
            ScanMask::Permanent => !has_ttl,
            ScanMask::Accessed => db.is_touched(key.as_bytes()),
            ScanMask::Untouched => !db.is_touched(key.as_bytes()),
        };
        if !ok {
            return false;
        }
    }
    if opts.min_malloc_size > 0 && val.malloc_used() < opts.min_malloc_size {
        return false;
    }
    match &opts.pattern {
        Some(pattern) => glob_match(pattern, key.as_bytes()),
        None => true,
    }
}

/// Build the SCAN reply: `[cursor, keys]` with the cursor as a decimal string.
fn scan_reply(cursor: u64, keys: Vec<Vec<u8>>) -> RespValue {
    RespValue::Array(vec![
        RespValue::Bulk(cursor.to_string().into_bytes()),
        RespValue::Array(keys.into_iter().map(RespValue::Bulk).collect()),
    ])
}

const SCAN_HELP: &[&str] = &[
    "SCAN cursor [MATCH <glob>] [TYPE <type>] [COUNT <count>] [ATTR <mask>] [MINMSZ <len>]",
    "    MATCH <glob> - pattern to match keys against",
    "    TYPE <type> - type of values to match",
    "    COUNT <count> - number of keys to return",
    "    ATTR <v|p|a|u> - filter by attributes: v - volatile (ttl), ",
    "    p - persistent (no ttl), a - accessed since creation, u - untouched",
    "    MINMSZ <len> - keeps keys with values, whose allocated size is greater or equal to",
    "        the specified length",
];

/// Executed on every shard for a SCAN. Shards below the cursor's shard index
/// were already consumed; the cursor's shard resumes at `cursor >> 10`, and
/// higher shards start from position 0. Matched keys are sorted by shard hash
/// so the continuation position is stable across calls.
fn exec_scan(ctx: &mut OpContext) -> CmdResult {
    if ctx.args[1].eq_ignore_ascii_case(b"HELP") {
        return CmdResult::Ok(RespValue::Array(
            SCAN_HELP.iter().map(|s| RespValue::Simple(s.to_string())).collect(),
        ));
    }
    let cursor = match parse_u64(&ctx.args[1]) {
        Some(c) => c,
        None => return CmdResult::err("ERR invalid cursor"),
    };
    let opts = match parse_scan_opts(ctx.args) {
        Ok(o) => o,
        Err(e) => return CmdResult::Err(e),
    };

    let shard_id = ctx.db.shard_id() as u64;
    let sid = cursor % 1024;
    if shard_id < sid {
        return CmdResult::Ok(scan_reply(0, Vec::new()));
    }
    let start_pos = if sid == shard_id { (cursor >> SCAN_SHARD_BITS) as usize } else { 0 };

    let db = &*ctx.db;
    let mut matched: Vec<(u64, Vec<u8>)> = db
        .iter()
        .filter(|(k, v)| scan_key_matches(db, k, v, &opts))
        .map(|(k, _)| (shard_hash(k.as_bytes()), k.as_bytes().to_vec()))
        .collect();
    matched.sort_by_key(|(h, _)| *h);

    let next = (start_pos + opts.limit).min(matched.len());
    let keys: Vec<Vec<u8>> = matched
        .iter()
        .skip(start_pos)
        .take(opts.limit)
        .map(|(_, k)| k.clone())
        .collect();
    let cursor_out = if next >= matched.len() {
        0
    } else {
        ((next as u64) << SCAN_SHARD_BITS) | shard_id
    };
    CmdResult::Ok(scan_reply(cursor_out, keys))
}

/// Decode a per-shard SCAN result `[cursor, keys]` into its continuation token
/// and key list.
fn decode_scan_shard(result: &CmdResult) -> Result<(u64, Vec<Vec<u8>>), RespError> {
    match result {
        CmdResult::Ok(RespValue::Array(a)) if a.len() == 2 => {
            let token = match &a[0] {
                RespValue::Bulk(b) => String::from_utf8_lossy(b).parse::<u64>().unwrap_or(0),
                _ => 0,
            };
            let keys = match &a[1] {
                RespValue::Array(arr) => arr
                    .iter()
                    .map(|v| match v {
                        RespValue::Bulk(b) => b.clone(),
                        _ => Vec::new(),
                    })
                    .collect(),
                _ => Vec::new(),
            };
            Ok((token, keys))
        }
        _ => Err(RespError::new("ERR internal: bad scan shard result")),
    }
}

/// Merge per-shard SCAN results, mirroring `ScanGeneric`: walk the shards in id
/// order (resuming at the cursor's shard) until `limit` keys were collected,
/// encoding the continuation as `(position << 10) | shard_id`.
fn merge_scan(parts: &[ShardPart], args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    if args[1].eq_ignore_ascii_case(b"HELP") {
        return parts[0].result.clone();
    }
    let cursor = match parse_u64(&args[1]) {
        Some(c) => c,
        None => return CmdResult::err("ERR invalid cursor"),
    };
    let opts = match parse_scan_opts(args) {
        Ok(o) => o,
        Err(e) => return CmdResult::Err(e),
    };
    for p in parts {
        if let CmdResult::Err(e) = &p.result {
            return CmdResult::Err(e.clone());
        }
    }
    let sid = (cursor % 1024) as usize;
    let shard_count = parts.iter().map(|p| p.shard).max().unwrap_or(0) + 1;
    if sid >= shard_count {
        return CmdResult::Ok(scan_reply(0, Vec::new()));
    }

    let by_shard: HashMap<usize, &ShardPart> = parts.iter().map(|p| (p.shard, p)).collect();
    let mut result: Vec<Vec<u8>> = Vec::new();
    let mut remaining = opts.limit;
    let mut final_cursor: u64 = 0;
    let mut s = sid;
    while s < shard_count {
        if remaining == 0 {
            break;
        }
        let Some(part) = by_shard.get(&s) else {
            s += 1;
            continue;
        };
        let (token, keys_s) = match decode_scan_shard(&part.result) {
            Ok(t) => t,
            Err(e) => return CmdResult::Err(e),
        };
        let pos_start = if s == sid { (cursor >> SCAN_SHARD_BITS) as usize } else { 0 };
        let take = keys_s.len().min(remaining);
        result.extend(keys_s[..take].iter().cloned());
        remaining -= take;
        if token == 0 {
            // Shard `s` was fully consumed; move on to the next shard.
            if remaining == 0 && s + 1 < shard_count {
                final_cursor = (s + 1) as u64;
                break;
            }
            s += 1;
            continue;
        }
        // Shard `s` still has more keys; continue within it next time.
        final_cursor = if take < keys_s.len() {
            (((pos_start + take) as u64) << SCAN_SHARD_BITS) | s as u64
        } else {
            token
        };
        break;
    }
    CmdResult::Ok(scan_reply(final_cursor, result))
}

// ---------------------------------------------------------------------------
// RM — cursor-based multi-key delete
// ---------------------------------------------------------------------------

const RM_HELP: &[&str] = &[
    "RM cursor [MATCH <glob>] [TYPE <type>] [COUNT <count>]",
    "    MATCH <glob> - pattern to match keys against",
    "    TYPE <type> - type of values to match (string, list, set, zset, hash, stream)",
    "    COUNT <count> - number of keys to delete per call",
];

/// `[cursor, deleted]` reply shape for RM.
fn rm_reply(cursor: u64, deleted: u64) -> RespValue {
    RespValue::Array(vec![
        RespValue::Bulk(cursor.to_string().into_bytes()),
        RespValue::Integer(deleted as i64),
    ])
}

/// Per-shard half of RM, mirroring `OpScanAndDelete`. Only the shard the
/// cursor points at acts; every other shard contributes nothing (matching the
/// reference `RmGeneric`, which processes shards strictly sequentially). The
/// continuation token is `(hash << 10) | shard_id` where `hash` is the shard
/// hash of the last key examined: because deleting a key never changes the
/// hashes of the survivors, that watermark stays valid across calls even
/// though the table keeps shrinking. A token of 0 means the shard was
/// exhausted.
fn exec_rm(ctx: &mut OpContext) -> CmdResult {
    if ctx.args[1].eq_ignore_ascii_case(b"HELP") {
        return CmdResult::Ok(RespValue::Array(
            RM_HELP.iter().map(|s| RespValue::Simple(s.to_string())).collect(),
        ));
    }
    let cursor = match parse_u64(&ctx.args[1]) {
        Some(c) => c,
        None => return CmdResult::err("ERR invalid cursor"),
    };
    let opts = match parse_scan_opts(ctx.args) {
        Ok(o) => o,
        Err(e) => return CmdResult::Err(e),
    };

    let shard_id = ctx.db.shard_id() as u64;
    let sid = cursor % 1024;
    if shard_id != sid {
        return CmdResult::Ok(rm_reply(0, 0));
    }
    let watermark = cursor >> SCAN_SHARD_BITS;

    let mut matched: Vec<(u64, Vec<u8>)> = ctx
        .db
        .iter()
        .filter(|(k, v)| scan_key_matches(ctx.db, k, v, &opts))
        .map(|(k, _)| (shard_hash(k.as_bytes()), k.as_bytes().to_vec()))
        .filter(|(h, _)| *h >= watermark)
        .collect();
    matched.sort_unstable_by_key(|(h, _)| *h);
    let total = matched.len();

    let mut deleted = 0u64;
    let mut visited = 0usize;
    let mut last_hash = watermark;
    for (h, key) in matched {
        if visited >= opts.limit {
            break;
        }
        visited += 1;
        last_hash = h;
        if ctx.db.remove_if_exists(&key) {
            deleted += 1;
        }
    }
    let token = if visited >= total {
        0
    } else {
        (last_hash << SCAN_SHARD_BITS) | shard_id
    };
    CmdResult::Ok(rm_reply(token, deleted))
}

/// Decode a per-shard RM result into its continuation token and deleted count.
fn decode_rm_shard(result: &CmdResult) -> Result<(u64, u64), RespError> {
    match result {
        CmdResult::Ok(RespValue::Array(a)) if a.len() == 2 => {
            let token = match &a[0] {
                RespValue::Bulk(b) => String::from_utf8_lossy(b).parse::<u64>().unwrap_or(0),
                _ => 0,
            };
            let deleted = match &a[1] {
                RespValue::Integer(n) => (*n).max(0) as u64,
                _ => 0,
            };
            Ok((token, deleted))
        }
        _ => Err(RespError::new("ERR internal: bad rm shard result")),
    }
}

/// Merge per-shard RM results, mirroring `RmGeneric`: only the cursor's shard
/// ran, so its token is forwarded as the continuation. A 0 token means that
/// shard was exhausted, so the next call resumes at the following shard.
fn merge_rm(parts: &[ShardPart], args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    if args[1].eq_ignore_ascii_case(b"HELP") {
        return parts[0].result.clone();
    }
    let cursor = match parse_u64(&args[1]) {
        Some(c) => c,
        None => return CmdResult::err("ERR invalid cursor"),
    };
    let _opts = match parse_scan_opts(args) {
        Ok(o) => o,
        Err(e) => return CmdResult::Err(e),
    };
    for p in parts {
        if let CmdResult::Err(e) = &p.result {
            return CmdResult::Err(e.clone());
        }
    }
    let sid = (cursor % 1024) as usize;
    let shard_count = parts.iter().map(|p| p.shard).max().unwrap_or(0) + 1;
    if sid >= shard_count {
        return CmdResult::Ok(rm_reply(0, 0));
    }

    let by_shard: HashMap<usize, &ShardPart> = parts.iter().map(|p| (p.shard, p)).collect();
    let Some(part) = by_shard.get(&sid) else {
        return CmdResult::Ok(rm_reply(0, 0));
    };
    let (token, deleted) = match decode_rm_shard(&part.result) {
        Ok(d) => d,
        Err(e) => return CmdResult::Err(e),
    };
    // Only the cursor's shard ran (mirroring `RmGeneric`'s strictly sequential
    // shard processing), so its token is the continuation. A 0 token means the
    // shard was exhausted: resume at the next shard, or finish.
    let cursor_out = if token != 0 {
        token
    } else if sid + 1 < shard_count {
        (sid + 1) as u64
    } else {
        0
    };
    CmdResult::Ok(rm_reply(cursor_out, deleted))
}

// ---------------------------------------------------------------------------
// SORT / SORT_RO
// ---------------------------------------------------------------------------

/// Options parsed from a SORT/SORT_RO argument list, mirroring the reference
/// `SortParams`.
struct SortOpts {
    alpha: bool,
    reversed: bool,
    to_sort: bool,
    store_key: Option<usize>,
    by_pattern: Option<Vec<u8>>,
    get_patterns: Vec<Vec<u8>>,
    bounds: Option<(usize, usize)>,
}

/// Parse SORT options from `args[2..]`. Mirrors the reference grammar
/// (Options(ALPHA, DESC/ASC, LIMIT, STORE, BY, GET)): options may appear in
/// any order and repeat (last one wins), unknown options are a syntax error,
/// and STORE is rejected for SORT_RO. LIMIT values must fit in u32; anything
/// else (including negatives) is an integer error.
fn parse_sort_opts(args: &[Vec<u8>], is_ro: bool) -> Result<SortOpts, RespError> {
    let mut opts = SortOpts {
        alpha: false,
        reversed: false,
        to_sort: true,
        store_key: None,
        by_pattern: None,
        get_patterns: Vec::new(),
        bounds: None,
    };
    let mut i = 2;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        let next = || args.get(i + 1).ok_or_else(RespError::syntax);
        match opt.as_slice() {
            b"ALPHA" => {
                opts.alpha = true;
                i += 1;
            }
            b"ASC" => {
                opts.reversed = false;
                i += 1;
            }
            b"DESC" => {
                opts.reversed = true;
                i += 1;
            }
            b"LIMIT" => {
                let off = parse_limit_u32(args.get(i + 1).ok_or_else(RespError::syntax)?)?;
                let cnt = parse_limit_u32(args.get(i + 2).ok_or_else(RespError::syntax)?)?;
                opts.bounds = Some((off, cnt));
                i += 3;
            }
            b"STORE" => {
                if is_ro {
                    return Err(RespError::syntax());
                }
                opts.store_key = Some(i + 1);
                i += 2;
            }
            b"BY" => {
                opts.by_pattern = Some(next()?.to_vec());
                i += 2;
            }
            b"GET" => {
                opts.get_patterns.push(next()?.to_vec());
                i += 2;
            }
            _ => return Err(RespError::syntax()),
        }
    }
    // "nosort" (BY with no '*') disables sorting; 2+ '*' is a syntax error.
    if let Some(p) = &opts.by_pattern {
        let stars = p.iter().filter(|&&b| b == b'*').count();
        if stars == 0 {
            opts.to_sort = false;
            opts.by_pattern = None;
        } else if stars != 1 {
            return Err(RespError::syntax());
        }
    }
    // Each GET pattern must be "#" or contain at most one '*'.
    for p in &opts.get_patterns {
        if p != b"#" && p.iter().filter(|&&b| b == b'*').count() > 1 {
            return Err(RespError::syntax());
        }
    }
    Ok(opts)
}

fn parse_limit_u32(s: &[u8]) -> Result<usize, RespError> {
    match parse_i64(s) {
        Some(v) if (0..=u32::MAX as i64).contains(&v) => Ok(v as usize),
        _ => Err(RespError::integer()),
    }
}

/// One sortable element, the analogue of the reference `SortEntry`.
struct SortEntry {
    /// Sort key: the parsed string (alpha mode) or the raw score string
    /// (numeric mode, used for the lexicographic tie-break).
    key: Vec<u8>,
    /// Numeric score (only meaningful in non-alpha mode).
    score: f64,
    /// The element used for the reply and GET pattern substitution: the
    /// original container member, or the BY-bound element for BY lookups.
    result: Vec<u8>,
    /// Fetched GET pattern values, one per pattern.
    get_values: Vec<Vec<u8>>,
}

/// Comparator mirroring `SortEntry::less`/`SortEntry::greater`: numeric score
/// first with a lexicographic key tie-break, reversed for DESC.
fn sort_entry_cmp(l: &SortEntry, r: &SortEntry, alpha: bool, reversed: bool) -> std::cmp::Ordering {
    let ord = if alpha {
        l.key.cmp(&r.key)
    } else {
        l.score
            .partial_cmp(&r.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| l.key.cmp(&r.key))
    };
    if reversed {
        ord.reverse()
    } else {
        ord
    }
}

/// Parse one sort key. In alpha mode the raw bytes are kept as-is; in numeric
/// mode the value must be a double (empty parses to 0, NaN or garbage is the
/// reference's "can't be converted into double" error).
fn parse_sort_value(raw: Vec<u8>, alpha: bool) -> Result<(Vec<u8>, f64), RespError> {
    if alpha || raw.is_empty() {
        return Ok((raw, 0.0));
    }
    match parse_double(&raw) {
        Some(f) if !f.is_nan() => Ok((raw, f)),
        _ => Err(RespError::new(SORT_SCORE_ERR)),
    }
}

/// Fetch a string value by key for GET/BY lookups, defaulting to the empty
/// string when missing or not a string (mirrors `OpFetchStringValue`). Note:
/// only the local shard's data is consulted, so multi-shard deployments with
/// external GET/BY keys on other shards read them as empty (a port limitation).
fn local_string_value(db: &mut DbSlice, key: &[u8], now_ms: u64) -> Vec<u8> {
    match db.find(key, now_ms) {
        Some(PrimeValue::Str(s)) => s.as_bytes().to_vec(),
        _ => Vec::new(),
    }
}

/// Fetch a container's elements in iteration order, mirroring
/// `OpFetchContainerElements`. Prunes expired set members and removes the key
/// when the set became empty. Returns an empty vector for a missing or empty
/// container; wrong types are an error.
fn fetch_container_elements(
    db: &mut DbSlice,
    key: &[u8],
    now_ms: u64,
) -> Result<Vec<Vec<u8>>, RespError> {
    match db.find_mut(key, now_ms) {
        None => Ok(Vec::new()),
        Some(PrimeValue::List(l)) => Ok(l.iter().map(|it| it.as_bytes()).collect()),
        Some(PrimeValue::Set(s)) => {
            s.prune_expired(now_ms);
            let elems = s.members().into_iter().map(|m| m.as_bytes().to_vec()).collect();
            if s.is_empty() {
                db.remove(key);
            }
            Ok(elems)
        }
        Some(PrimeValue::ZSet(z)) => Ok(z.iter().map(|(m, _)| m.as_bytes().to_vec()).collect()),
        Some(_) => Err(RespError::wrong_type()),
    }
}

/// Expand a GET/BY pattern by substituting the element into the first '*'.
/// Patterns without a '*' are used literally.
fn expand_pattern(pattern: &[u8], element: &[u8]) -> Vec<u8> {
    let star_pos = pattern.iter().position(|&b| b == b'*');
    match star_pos {
        Some(p) => {
            let mut out = Vec::with_capacity(pattern.len() + element.len());
            out.extend_from_slice(&pattern[..p]);
            out.extend_from_slice(element);
            out.extend_from_slice(&pattern[p + 1..]);
            out
        }
        None => pattern.to_vec(),
    }
}

/// The reference error for a non-numeric element under numeric SORT.
const SORT_SCORE_ERR: &str = "One or more scores can't be converted into double";

/// Shared executor for SORT and SORT_RO. On a multi-shard STORE the shard that
/// owns the source key returns the computed list via `DeferredStore` and the
/// merge forwards it; the shard owning only the destination returns an empty
/// array and is ignored.
fn exec_sort(ctx: &mut OpContext) -> CmdResult {
    let is_ro = ctx.args[0].eq_ignore_ascii_case(b"SORT_RO");
    let opts = match parse_sort_opts(ctx.args, is_ro) {
        Ok(o) => o,
        Err(e) => return CmdResult::Err(e),
    };
    let key_idx = ctx.first_key_idx;
    // A shard owning only the STORE destination has nothing to contribute.
    if !ctx.owned_keys.contains(&key_idx) {
        return CmdResult::Ok(RespValue::Array(vec![]));
    }
    let key = &ctx.args[key_idx];

    // Missing source key -> empty array (the STORE destination is untouched).
    if !ctx.db.contains(key, ctx.now_ms) {
        return CmdResult::Ok(RespValue::Array(vec![]));
    }
    let elems = match fetch_container_elements(ctx.db, key, ctx.now_ms) {
        Ok(e) => e,
        Err(e) => return CmdResult::Err(e),
    };

    let mut entries: Vec<SortEntry> = Vec::with_capacity(elems.len());
    if !opts.to_sort {
        // BY nosort: preserve insertion order, no parsing, no sorting.
        for el in &elems {
            entries.push(SortEntry {
                key: el.clone(),
                score: 0.0,
                result: el.clone(),
                get_values: Vec::new(),
            });
        }
    } else if let Some(by) = &opts.by_pattern {
        // Sort by external key values; reply with the original elements.
        for el in &elems {
            let ext_key = expand_pattern(by, el);
            let external = local_string_value(ctx.db, &ext_key, ctx.now_ms);
            let (key, score) = match parse_sort_value(external, opts.alpha) {
                Ok(k) => k,
                Err(e) => return CmdResult::Err(e),
            };
            entries.push(SortEntry { key, score, result: el.clone(), get_values: Vec::new() });
        }
    } else {
        // Sort the elements themselves.
        for el in &elems {
            let (key, score) = match parse_sort_value(el.clone(), opts.alpha) {
                Ok(k) => k,
                Err(e) => return CmdResult::Err(e),
            };
            entries.push(SortEntry { key, score, result: el.clone(), get_values: Vec::new() });
        }
    }

    if opts.to_sort {
        entries.sort_by(|l, r| sort_entry_cmp(l, r, opts.alpha, opts.reversed));
    }

    // LIMIT is applied to the sorted entries.
    let (off, cnt) = opts.bounds.unwrap_or((0, entries.len()));
    let start = off.min(entries.len());
    let end = (off.saturating_add(cnt)).min(entries.len());

    // Fetch GET pattern values for the entries in range.
    if !opts.get_patterns.is_empty() {
        for e in entries.iter_mut() {
            e.get_values.resize(opts.get_patterns.len(), Vec::new());
        }
        for (pi, pattern) in opts.get_patterns.iter().enumerate() {
            for e in entries.iter_mut() {
                let value = if pattern == b"#" {
                    e.result.clone()
                } else {
                    local_string_value(ctx.db, &expand_pattern(pattern, &e.result), ctx.now_ms)
                };
                e.get_values[pi] = value;
            }
        }
    }

    // Flatten the values to reply or store: with GET patterns all pattern
    // values per entry in order, otherwise the elements themselves.
    let has_get = !opts.get_patterns.is_empty();
    let stored: Vec<Vec<u8>> = if has_get {
        let mut out = Vec::new();
        for e in &entries[start..end] {
            out.extend(e.get_values.iter().cloned());
        }
        out
    } else {
        entries[start..end].iter().map(|e| e.result.clone()).collect()
    };
    let count = stored.len() as i64;

    if let Some(dest_idx) = opts.store_key {
        let dest = &ctx.args[dest_idx];
        let value = if stored.is_empty() {
            None
        } else {
            let mut list = QuickList::new();
            for v in &stored {
                list.push_back(ListItem::from_bytes(v));
            }
            Some(PrimeValue::List(list))
        };
        // Single-shard command: write the destination here. Multi-shard:
        // hand it to the coordinator as a deferred store.
        if ctx.owned_keys.contains(&dest_idx) {
            match value {
                Some(v) => {
                    ctx.db.clear_expiry(dest);
                    ctx.db.insert(CompactString::from_bytes(dest), v);
                }
                None => {
                    ctx.db.remove(dest);
                }
            }
            return CmdResult::Ok(integer(count));
        }
        return CmdResult::deferred_store(dest.clone(), value, integer(count));
    }

    CmdResult::Ok(RespValue::Array(stored.into_iter().map(RespValue::Bulk).collect()))
}

/// Merge for SORT: forward the source shard's result (an array, or a
/// `DeferredStore` for multi-shard STORE). Other shards only owned the
/// destination and returned placeholders.
fn merge_sort(parts: &[ShardPart], _args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    for p in parts {
        if let CmdResult::Err(e) = &p.result {
            return CmdResult::Err(e.clone());
        }
        if p.owned_key_idxs.contains(&keys[0]) {
            return p.result.clone();
        }
    }
    parts[0].result.clone()
}

// ---------------------------------------------------------------------------
// FIELDEXPIRE / FIELDTTL
// ---------------------------------------------------------------------------

/// Ceiling for FIELDEXPIRE TTLs, shared with the reference
/// `kMaxExpireDeadlineSec` (dragonfly/src/server/common.h).
const MAX_EXPIRE_SEC: i64 = (1u64 << 28) as i64 - 1;

/// FIELDEXPIRE key ttl_sec field [field ...]
///
/// Sets the per-member/field TTL of a set or hash. Replies an integer per
/// field: 1 on success, -2 for a missing key, field or wrong type (mirroring
/// `OpFieldExpire`, which never errors on a wrong type). A key emptied by
/// lazy pruning is deleted. Stale (already expired) members are pruned first,
/// matching the reference's `Find` which lazily removes them, so they reply
/// -2 rather than being re-armed.
fn exec_fieldexpire(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let ttl_sec = match parse_i64(&ctx.args[key_idx + 1]) {
        Some(v) if (1..=MAX_EXPIRE_SEC).contains(&v) => v as u64,
        _ => return CmdResult::Err(RespError::integer()),
    };
    let fields = &ctx.args[key_idx + 2..];
    let expire_ms = ctx.now_ms.saturating_add(ttl_sec.saturating_mul(1000));

    let res = match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Set(s)) => {
            s.prune_expired(ctx.now_ms);
            let mut out = Vec::with_capacity(fields.len());
            for f in fields {
                if s.contains(f) {
                    s.add_expirable(CompactString::from_bytes(f), expire_ms, false);
                    out.push(1);
                } else {
                    out.push(-2);
                }
            }
            if s.is_empty() {
                ctx.db.remove(key);
            }
            out
        }
        Some(PrimeValue::Hash(h)) => {
            h.prune_expired(ctx.now_ms);
            let mut out = Vec::with_capacity(fields.len());
            for f in fields {
                if h.contains(f) {
                    let v = h.get(f).cloned().unwrap_or_else(|| CompactString::from_bytes(f));
                    h.add_expirable(CompactString::from_bytes(f), v, Some(expire_ms), false);
                    out.push(1);
                } else {
                    out.push(-2);
                }
            }
            if h.is_empty() {
                ctx.db.remove(key);
            }
            out
        }
        Some(_) | None => vec![-2; fields.len()],
    };
    CmdResult::Ok(RespValue::Array(res.into_iter().map(integer).collect()))
}

/// FIELDTTL key field
///
/// Returns the remaining TTL in seconds of one set member / hash field:
/// -2 for a missing key, -3 for a missing field, -1 for a field without a TTL.
/// Wrong types are an error (unlike FIELDEXPIRE). Stale members are pruned
/// lazily first, mirroring `OpFieldTtl`.
fn exec_fieldttl(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let field = &ctx.args[key_idx + 1];
    let now_sec = (ctx.now_ms as i64) / 1000;
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Set(s)) => {
            s.prune_expired(ctx.now_ms);
            if !s.contains(field) {
                if s.is_empty() {
                    ctx.db.remove(key);
                }
                return CmdResult::Ok(integer(-3));
            }
            match s.member_expire_ms(field) {
                Some(at) => CmdResult::Ok(integer((at as i64) / 1000 - now_sec)),
                None => CmdResult::Ok(integer(-1)),
            }
        }
        Some(PrimeValue::Hash(h)) => {
            h.prune_expired(ctx.now_ms);
            if !h.contains(field) {
                if h.is_empty() {
                    ctx.db.remove(key);
                }
                return CmdResult::Ok(integer(-3));
            }
            match h.field_expire_ms(field) {
                Some(at) => CmdResult::Ok(integer((at as i64) / 1000 - now_sec)),
                None => CmdResult::Ok(integer(-1)),
            }
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(-2)),
    }
}

// ---------------------------------------------------------------------------
// Command definitions
// ---------------------------------------------------------------------------

pub static CMD_DEL: Command = Command {
    name: "DEL",
    arity: -2,
    flags: FLAG_WRITE | FLAG_MULTI_KEY,
    key_range: KeyRange::ALL,
    exec: exec_del,
    merge: Some(merge_sum),
};
pub static CMD_EXISTS: Command = Command {
    name: "EXISTS",
    arity: -2,
    flags: FLAG_READONLY | FLAG_FAST | FLAG_MULTI_KEY,
    key_range: KeyRange::ALL,
    exec: exec_exists,
    merge: Some(merge_sum),
};
pub static CMD_DELEX: Command = Command {
    name: "DELEX",
    arity: -2,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_delex,
    merge: None,
};
pub static CMD_EXPIRE: Command = Command {
    name: "EXPIRE",
    arity: -3,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_expire,
    merge: None,
};
pub static CMD_PEXPIRE: Command = Command {
    name: "PEXPIRE",
    arity: -3,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_pexpire,
    merge: None,
};
pub static CMD_EXPIREAT: Command = Command {
    name: "EXPIREAT",
    arity: -3,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_expireat,
    merge: None,
};
pub static CMD_PEXPIREAT: Command = Command {
    name: "PEXPIREAT",
    arity: -3,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_pexpireat,
    merge: None,
};
pub static CMD_TTL: Command = Command {
    name: "TTL",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_ttl,
    merge: None,
};
pub static CMD_PTTL: Command = Command {
    name: "PTTL",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_pttl,
    merge: None,
};
pub static CMD_PERSIST: Command = Command {
    name: "PERSIST",
    arity: 2,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_persist,
    merge: None,
};
pub static CMD_TYPE: Command = Command {
    name: "TYPE",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_type,
    merge: None,
};
pub static CMD_DUMP: Command = Command {
    name: "DUMP",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_dump,
    merge: None,
};
pub static CMD_RESTORE: Command = Command {
    name: "RESTORE",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    key_range: KeyRange::ONE,
    exec: exec_restore,
    merge: None,
};
pub static CMD_KEYS: Command = Command {
    name: "KEYS",
    arity: 2,
    flags: FLAG_READONLY | FLAG_GLOBAL,
    key_range: KeyRange::NONE,
    exec: exec_keys,
    merge: Some(merge_concat),
};
pub static CMD_TOUCH: Command = Command {
    name: "TOUCH",
    arity: -2,
    flags: FLAG_READONLY | FLAG_FAST | FLAG_MULTI_KEY,
    key_range: KeyRange::ALL,
    exec: exec_exists,
    merge: Some(merge_sum),
};
pub static CMD_UNLINK: Command = Command {
    name: "UNLINK",
    arity: -2,
    flags: FLAG_WRITE | FLAG_MULTI_KEY,
    key_range: KeyRange::ALL,
    exec: exec_del,
    merge: Some(merge_sum),
};
pub static CMD_STICK: Command = Command {
    name: "STICK",
    arity: -2,
    flags: FLAG_WRITE | FLAG_MULTI_KEY,
    key_range: KeyRange::ALL,
    exec: exec_stick,
    merge: Some(merge_sum),
};
pub static CMD_MOVE: Command = Command {
    name: "MOVE",
    arity: 3,
    flags: FLAG_WRITE | FLAG_GLOBAL,
    key_range: KeyRange::ONE,
    exec: exec_move,
    merge: Some(merge_sum),
};
pub static CMD_RENAME: Command = Command {
    name: "RENAME",
    arity: 3,
    flags: FLAG_WRITE,
    key_range: KeyRange::TWO,
    exec: exec_rename,
    merge: Some(merge_rename),
};
pub static CMD_RENAMENX: Command = Command {
    name: "RENAMENX",
    arity: 3,
    flags: FLAG_WRITE,
    key_range: KeyRange::TWO,
    exec: exec_renamenx,
    merge: Some(merge_rename),
};
pub static CMD_COPY: Command = Command {
    name: "COPY",
    arity: -3,
    flags: FLAG_WRITE,
    key_range: KeyRange::TWO,
    exec: exec_copy,
    merge: Some(merge_copy),
};
pub static CMD_RANDOMKEY: Command = Command {
    name: "RANDOMKEY",
    arity: 1,
    flags: FLAG_READONLY | FLAG_GLOBAL,
    key_range: KeyRange::NONE,
    exec: exec_randomkey,
    merge: Some(merge_rand),
};
pub static CMD_EXPIRETIME: Command = Command {
    name: "EXPIRETIME",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_expiretime,
    merge: None,
};
pub static CMD_PEXPIRETIME: Command = Command {
    name: "PEXPIRETIME",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_pexpiretime,
    merge: None,
};
pub static CMD_SCAN: Command = Command {
    name: "SCAN",
    arity: -2,
    flags: FLAG_READONLY | FLAG_FAST | FLAG_GLOBAL,
    key_range: KeyRange::NONE,
    exec: exec_scan,
    merge: Some(merge_scan),
};
pub static CMD_RM: Command = Command {
    name: "RM",
    arity: -2,
    flags: FLAG_WRITE | FLAG_GLOBAL,
    key_range: KeyRange::NONE,
    exec: exec_rm,
    merge: Some(merge_rm),
};
pub static CMD_SORT: Command = Command {
    name: "SORT",
    arity: -2,
    flags: FLAG_WRITE | FLAG_MOVABLEKEYS,
    key_range: KeyRange::ONE,
    exec: exec_sort,
    merge: Some(merge_sort),
};
pub static CMD_SORT_RO: Command = Command {
    name: "SORT_RO",
    arity: -2,
    flags: FLAG_READONLY | FLAG_MOVABLEKEYS,
    key_range: KeyRange::ONE,
    exec: exec_sort,
    merge: Some(merge_sort),
};
pub static CMD_FIELDEXPIRE: Command = Command {
    name: "FIELDEXPIRE",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_fieldexpire,
    merge: None,
};
pub static CMD_FIELDTTL: Command = Command {
    name: "FIELDTTL",
    arity: 3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_fieldttl,
    merge: None,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db::DbSlice;
    use crate::core::set::Set;
    use crate::core::PrimeValue;
    use crate::core::zset::ZSet;

    macro_rules! bvecs {
        () => {
            Vec::<Vec<u8>>::new()
        };
        ($($x:literal),* $(,)?) => {
            vec![$(($x).as_bytes().to_vec()),*]
        };
    }

    fn val(r: CmdResult) -> RespValue {
        r.into_resp_value()
    }

    fn int_of(r: CmdResult) -> i64 {
        match r {
            CmdResult::Ok(RespValue::Integer(v)) => v,
            o => panic!("expected integer, got {:?}", o.into_resp_value()),
        }
    }

    fn err_of(r: CmdResult) -> String {
        match r {
            CmdResult::Err(e) => e.message,
            o => panic!("expected error, got {:?}", o.into_resp_value()),
        }
    }

    fn dispatch_at(db: &mut DbSlice, now_ms: u64, argv: &[Vec<u8>]) -> CmdResult {
        let cmd = argv[0].to_ascii_uppercase();
        let (exec, first_key_idx, owned): (crate::commands::ExecFn, usize, Vec<usize>) =
            match cmd.as_slice() {
                b"DEL" => (exec_del, 1, (1..argv.len()).collect()),
                b"DELEX" => (exec_delex, 1, (1..2).collect()),
                b"STICK" => (exec_stick, 1, (1..argv.len()).collect()),
                b"UNLINK" => (exec_del, 1, (1..argv.len()).collect()),
                b"TOUCH" => (exec_exists, 1, (1..argv.len()).collect()),
                b"RENAME" => (exec_rename, 1, (1..3).collect()),
                b"RENAMENX" => (exec_renamenx, 1, (1..3).collect()),
                b"COPY" => (exec_copy, 1, (1..3).collect()),
                b"RANDOMKEY" => (exec_randomkey, 1, vec![]),
                b"EXPIRETIME" => (exec_expiretime, 1, (1..2).collect()),
                b"PEXPIRETIME" => (exec_pexpiretime, 1, (1..2).collect()),
                b"SCAN" => (exec_scan, 1, vec![]),
                b"RM" => (exec_rm, 0, vec![]),
                b"SORT" | b"SORT_RO" => (exec_sort, 1, (1..argv.len()).collect()),
                b"FIELDEXPIRE" => (exec_fieldexpire, 1, (1..2).collect()),
                b"FIELDTTL" => (exec_fieldttl, 1, (1..2).collect()),
                b"SADD" => (crate::commands::exec::sets::CMD_SADD.exec, 1, (1..2).collect()),
                b"SMEMBERS" => (crate::commands::exec::sets::CMD_SMEMBERS.exec, 1, (1..2).collect()),
                b"SADDEX" => (crate::commands::exec::sets::CMD_SADDEX.exec, 1, (1..2).collect()),
                b"HSET" => (crate::commands::exec::hashes::CMD_HSET.exec, 1, (1..2).collect()),
                b"HGETALL" => (crate::commands::exec::hashes::CMD_HGETALL.exec, 1, (1..2).collect()),
                b"HSETEX" => (crate::commands::exec::hashes::CMD_HSETEX.exec, 1, (1..2).collect()),
                b"TTL" => (exec_ttl, 1, (1..2).collect()),
                b"PTTL" => (exec_pttl, 1, (1..2).collect()),
                b"EXPIRE" => (exec_expire, 1, (1..2).collect()),
                b"KEYS" => (exec_keys, 1, vec![]),
                b"EXISTS" => (exec_exists, 1, (1..argv.len()).collect()),
                b"PEXPIREAT" => (exec_pexpireat, 1, (1..2).collect()),
                b"DUMP" => (exec_dump, 1, (1..2).collect()),
                b"RESTORE" => (exec_restore, 1, (1..2).collect()),
                _ => panic!("unhandled command {:?}", argv[0]),
            };
        let mut ctx = OpContext { db, args: argv, owned_keys: &owned, first_key_idx, now_ms };
        exec(&mut ctx)
    }

    fn cmd(db: &mut DbSlice, args: &[&[u8]]) -> CmdResult {
        dispatch_at(db, 0, &args.iter().map(|a| a.to_vec()).collect::<Vec<_>>())
    }

    fn cmd_at(db: &mut DbSlice, now_ms: u64, args: &[&[u8]]) -> CmdResult {
        dispatch_at(db, now_ms, &args.iter().map(|a| a.to_vec()).collect::<Vec<_>>())
    }

    fn array_keys(r: CmdResult) -> Vec<Vec<u8>> {
        match val(r) {
            RespValue::Array(ks) => ks
                .iter()
                .map(|v| match v {
                    RespValue::Bulk(b) => b.clone(),
                    o => panic!("expected bulk key, got {:?}", o),
                })
                .collect(),
            o => panic!("expected array, got {:?}", o),
        }
    }

    /// Run a SCAN/KEYS-ish command and return its key list. For SCAN the reply
    /// is `[cursor, keys]`; for KEYS it is a plain key array.
    fn scan_keys(db: &mut DbSlice, now_ms: u64, args: &[&[u8]]) -> Vec<Vec<u8>> {
        let r = dispatch_at(db, now_ms, &args.iter().map(|a| a.to_vec()).collect::<Vec<_>>());
        match val(r) {
            RespValue::Array(a) if a.len() == 2 => array_keys(CmdResult::Ok(a[1].clone())),
            o => panic!("expected scan reply [cursor, keys], got {:?}", o),
        }
    }

    fn str_of(db: &mut DbSlice, key: &str, value: &str) {
        db.insert(
            CompactString::from_bytes(key.as_bytes()),
            PrimeValue::Str(CompactString::from(value)),
        );
    }

    /// Port of `GenericFamilyTest.Touch`.
    #[test]
    fn touch() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "x", "0");
        str_of(&mut db, "y", "1");
        assert_eq!(int_of(cmd(&mut db, &[b"TOUCH", b"x", b"y", b"x"])), 3);
        assert_eq!(int_of(cmd(&mut db, &[b"TOUCH", b"z", b"x", b"w"])), 1);
    }

    /// DUMP + RESTORE round-trip through the command layer.
    #[test]
    fn dump_restore_roundtrip() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "src", "hello");

        let dump = match val(cmd(&mut db, &[b"DUMP", b"src"])) {
            RespValue::Bulk(b) => b,
            o => panic!("expected bulk dump, got {:?}", o),
        };
        assert_eq!(&dump[0..1], &[0]); // RDB_TYPE_STRING

        // DUMP on a missing key -> nil.
        assert_eq!(val(cmd(&mut db, &[b"DUMP", b"nope"])), RespValue::Nil);

        // RESTORE into a fresh key.
        let payload: Vec<Vec<u8>> =
            vec![b"RESTORE".to_vec(), b"dst".to_vec(), b"0".to_vec(), dump.clone()];
        let mut argv = vec![b"RESTORE".to_vec()];
        argv.extend_from_slice(&payload[1..]);
        assert_eq!(
            val(dispatch_at(&mut db, 1000, &argv)),
            val(CmdResult::Ok(ok()))
        );
        match db.find(b"dst", 0) {
            Some(PrimeValue::Str(s)) => assert_eq!(s.as_bytes(), b"hello"),
            o => panic!("expected restored string, got {:?}", o),
        }
        assert_eq!(int_of(cmd(&mut db, &[b"TTL", b"dst"])), -1);

        // Existing key without REPLACE -> BUSYKEY.
        let argv = vec![
            b"RESTORE".to_vec(),
            b"src".to_vec(),
            b"0".to_vec(),
            dump.clone(),
        ];
        assert_eq!(
            err_of(dispatch_at(&mut db, 1000, &argv)),
            "BUSYKEY Target key name already exists."
        );

        // With REPLACE it overwrites.
        let argv = vec![
            b"RESTORE".to_vec(),
            b"src".to_vec(),
            b"0".to_vec(),
            dump.clone(),
            b"REPLACE".to_vec(),
        ];
        assert_eq!(
            val(dispatch_at(&mut db, 1000, &argv)),
            val(CmdResult::Ok(ok()))
        );

        // RESTORE with a relative TTL stores an expiry.
        let argv = vec![
            b"RESTORE".to_vec(),
            b"tmp".to_vec(),
            b"5000".to_vec(),
            dump.clone(),
        ];
        assert_eq!(
            val(dispatch_at(&mut db, 1000, &argv)),
            val(CmdResult::Ok(ok()))
        );
        assert_eq!(int_of(cmd_at(&mut db, 1000, &[b"PTTL", b"tmp"])), 5000);
        // Expired by 6000ms.
        assert_eq!(int_of(cmd_at(&mut db, 6000, &[b"EXISTS", b"tmp"])), 0);

        // Corrupt payload -> Bad data format.
        let argv = vec![
            b"RESTORE".to_vec(),
            b"bad".to_vec(),
            b"0".to_vec(),
            b"garbage-not-an-rdb-payload".to_vec(),
        ];
        assert_eq!(
            err_of(dispatch_at(&mut db, 1000, &argv)),
            "ERR Bad data format"
        );
    }

    /// Port of `GenericFamilyTest.Rename`.
    #[test]
    fn rename() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "x", "xxx");
        str_of(&mut db, "b", "bbb");

        assert_eq!(err_of(cmd(&mut db, &[b"RENAME", b"z", b"b"])), "ERR no such key");
        assert_eq!(val(cmd(&mut db, &[b"RENAME", b"x", b"b"])), val(CmdResult::Ok(ok())));

        // x no longer exists, b holds the old value of x.
        assert_eq!(int_of(cmd(&mut db, &[b"EXISTS", b"x", b"b"])), 1);
        match db.find(b"b", 0) {
            Some(PrimeValue::Str(s)) => assert_eq!(s.as_bytes(), b"xxx"),
            o => panic!("expected string, got {:?}", o),
        }
        assert!(!db.contains(b"x", 0));
    }

    /// Port of `GenericFamilyTest.RenameBinary`.
    #[test]
    fn rename_binary() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "\u{1}\u{2}\u{3}\u{4}", "bar");
        cmd(&mut db, &[b"RENAME", "\u{1}\u{2}\u{3}\u{4}".as_bytes(), "\u{5}\u{6}\u{7}\u{8}".as_bytes()]);
        assert!(!db.contains("\u{1}\u{2}\u{3}\u{4}".as_bytes(), 0));
        match db.find("\u{5}\u{6}\u{7}\u{8}".as_bytes(), 0) {
            Some(PrimeValue::Str(s)) => assert_eq!(s.as_bytes(), b"bar"),
            o => panic!("expected string, got {:?}", o),
        }
    }

    /// Port of `GenericFamilyTest.RenameNx`.
    #[test]
    fn renamenx() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "x", "xxx");
        str_of(&mut db, "b", "bbb");

        assert_eq!(err_of(cmd(&mut db, &[b"RENAMENX", b"z", b"b"])), "ERR no such key");
        assert_eq!(int_of(cmd(&mut db, &[b"RENAMENX", b"x", b"b"])), 0); // b exists
        assert_eq!(int_of(cmd(&mut db, &[b"RENAMENX", b"x", b"y"])), 1);
        match db.find(b"y", 0) {
            Some(PrimeValue::Str(s)) => assert_eq!(s.as_bytes(), b"xxx"),
            o => panic!("expected string, got {:?}", o),
        }
        assert_eq!(int_of(cmd(&mut db, &[b"RENAMENX", b"y", b"y"])), 0);
    }

    /// Port of `GenericFamilyTest.RenameSameName`.
    #[test]
    fn rename_same_name() {
        let mut db = DbSlice::new(0);
        assert_eq!(err_of(cmd(&mut db, &[b"RENAME", b"key", b"key"])), "ERR no such key");

        str_of(&mut db, "key", "value");
        assert_eq!(val(cmd(&mut db, &[b"RENAME", b"key", b"key"])), val(CmdResult::Ok(ok())));
    }

    /// Port of `GenericFamilyTest.Copy`.
    #[test]
    fn copy() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "x", "xxx");
        str_of(&mut db, "b", "bbb");

        assert_eq!(int_of(cmd(&mut db, &[b"COPY", b"z", b"b"])), 0);
        assert_eq!(int_of(cmd(&mut db, &[b"COPY", b"b", b"c"])), 1);
        match db.find(b"c", 0) {
            Some(PrimeValue::Str(s)) => assert_eq!(s.as_bytes(), b"bbb"),
            o => panic!("expected string, got {:?}", o),
        }

        assert_eq!(int_of(cmd(&mut db, &[b"COPY", b"x", b"b", b"REPLACE"])), 1);
        // Both keys now hold x's value.
        assert_eq!(int_of(cmd(&mut db, &[b"EXISTS", b"x", b"b"])), 2);
        match db.find(b"x", 0) {
            Some(PrimeValue::Str(s)) => assert_eq!(s.as_bytes(), b"xxx"),
            o => panic!("expected string, got {:?}", o),
        }
        match db.find(b"b", 0) {
            Some(PrimeValue::Str(s)) => assert_eq!(s.as_bytes(), b"xxx"),
            o => panic!("expected string, got {:?}", o),
        }
    }

    /// Port of `GenericFamilyTest.CopyNonString`: any value type is copied.
    #[test]
    fn copy_non_string() {
        let mut db = DbSlice::new(0);
        db.insert(CompactString::from("x"), PrimeValue::List(crate::core::quicklist::QuickList::default()));
        assert_eq!(int_of(cmd(&mut db, &[b"COPY", b"x", b"b"])), 1);
        assert!(db.contains(b"b", 0));
        assert_eq!(int_of(cmd(&mut db, &[b"DEL", b"x"])), 1);
        assert_eq!(int_of(cmd(&mut db, &[b"DEL", b"b"])), 1);
    }

    /// Port of `GenericFamilyTest.CopyTTL`: TTL is preserved on copy.
    #[test]
    fn copy_ttl() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "k1", "bar");
        db.set_expiry(b"k1", 10_000, 0);

        assert_eq!(int_of(cmd(&mut db, &[b"COPY", b"k1", b"k2"])), 1);
        assert_eq!(db.ttl_ms(b"k2", 0), 10_000);
    }

    /// Port of `GenericFamilyTest.CopySameName`.
    #[test]
    fn copy_same_name() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            err_of(cmd(&mut db, &[b"COPY", b"k1", b"k1"])),
            "source and destination objects are the same"
        );

        str_of(&mut db, "k1", "v");
        assert_eq!(
            err_of(cmd(&mut db, &[b"COPY", b"k1", b"k1"])),
            "source and destination objects are the same"
        );
    }

    /// Port of `GenericFamilyTest.CopyToDB`: unknown option is a syntax error.
    #[test]
    fn copy_to_db_unsupported() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            err_of(cmd(&mut db, &[b"COPY", b"k1", b"k1", b"DB", b"SOME_DB"])),
            "ERR syntax error"
        );
    }

    /// Port of `GenericFamilyTest.CopyKeyExists`.
    #[test]
    fn copy_key_exists() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "source", "value1");
        str_of(&mut db, "destination", "value2");

        assert_eq!(int_of(cmd(&mut db, &[b"COPY", b"source", b"destination"])), 0);
        assert_eq!(int_of(cmd(&mut db, &[b"COPY", b"source", b"destination", b"REPLACE"])), 1);
        match db.find(b"destination", 0) {
            Some(PrimeValue::Str(s)) => assert_eq!(s.as_bytes(), b"value1"),
            o => panic!("expected string, got {:?}", o),
        }
        // Source is untouched.
        match db.find(b"source", 0) {
            Some(PrimeValue::Str(s)) => assert_eq!(s.as_bytes(), b"value1"),
            o => panic!("expected string, got {:?}", o),
        }
    }

    /// Port of `GenericFamilyTest.RandomKey`.
    #[test]
    fn randomkey() {
        let mut db = DbSlice::new(0);
        assert!(matches!(val(cmd(&mut db, &[b"RANDOMKEY"])), RespValue::Nil));

        str_of(&mut db, "k1", "1");
        match val(cmd(&mut db, &[b"RANDOMKEY"])) {
            RespValue::Bulk(b) => assert_eq!(b, b"k1"),
            o => panic!("expected bulk, got {:?}", o),
        }
    }

    /// Port of `GenericFamilyTest.ExpireTime`.
    #[test]
    fn expiretime() {
        let mut db = DbSlice::new(0);
        assert_eq!(int_of(cmd(&mut db, &[b"EXPIRETIME", b"foo"])), -2);
        assert_eq!(int_of(cmd(&mut db, &[b"PEXPIRETIME", b"foo"])), -2);

        str_of(&mut db, "foo", "bar");
        assert_eq!(int_of(cmd(&mut db, &[b"EXPIRETIME", b"foo"])), -1);
        assert_eq!(int_of(cmd(&mut db, &[b"PEXPIRETIME", b"foo"])), -1);

        // pexpireat foo <now + 5000> -> absolute expiry in ms.
        cmd_at(&mut db, 0, &[b"PEXPIREAT", b"foo", b"5000"]);
        assert_eq!(int_of(cmd_at(&mut db, 0, &[b"EXPIRETIME", b"foo"])), 5);
        assert_eq!(int_of(cmd_at(&mut db, 0, &[b"PEXPIRETIME", b"foo"])), 5000);
    }

    /// Port of `GenericFamilyTest.Unlink`: same reply as DEL.
    #[test]
    fn unlink() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "s1", "v");
        str_of(&mut db, "s2", "v");
        assert_eq!(int_of(cmd(&mut db, &[b"UNLINK", b"s1", b"s2"])), 2);
        assert!(!db.contains(b"s1", 0));
        assert!(!db.contains(b"s2", 0));
    }

    /// RENAME carries the source TTL to the destination (mirrors OpRen).
    #[test]
    fn rename_preserves_ttl() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "src", "v");
        db.set_expiry(b"src", 10_000, 0);
        str_of(&mut db, "dst", "old");
        db.set_expiry(b"dst", 99_000, 0);

        assert_eq!(val(cmd(&mut db, &[b"RENAME", b"src", b"dst"])), val(CmdResult::Ok(ok())));
        assert_eq!(db.ttl_ms(b"dst", 0), 10_000);
        assert!(!db.contains(b"src", 0));
    }

    /// Port of `GenericFamilyTest.Delex`.
    #[test]
    fn delex() {
        let mut db = DbSlice::new(0);

        // DELEX without condition behaves like DEL.
        str_of(&mut db, "key1", "value1");
        assert_eq!(int_of(cmd(&mut db, &[b"DELEX", b"key1"])), 1);
        assert!(!db.contains(b"key1", 0));

        // Non-existent key returns 0.
        assert_eq!(int_of(cmd(&mut db, &[b"DELEX", b"nonexistent"])), 0);

        // IFEQ deletes when values match, not otherwise.
        str_of(&mut db, "key2", "value2");
        assert_eq!(int_of(cmd(&mut db, &[b"DELEX", b"key2", b"IFEQ", b"value2"])), 1);
        assert!(!db.contains(b"key2", 0));

        str_of(&mut db, "key3", "value3");
        assert_eq!(int_of(cmd(&mut db, &[b"DELEX", b"key3", b"IFEQ", b"wrongvalue"])), 0);
        assert!(db.contains(b"key3", 0));

        // IFNE deletes when values differ, not otherwise.
        str_of(&mut db, "key4", "value4");
        assert_eq!(int_of(cmd(&mut db, &[b"DELEX", b"key4", b"IFNE", b"differentvalue"])), 1);
        assert!(!db.contains(b"key4", 0));

        str_of(&mut db, "key5", "value5");
        assert_eq!(int_of(cmd(&mut db, &[b"DELEX", b"key5", b"IFNE", b"value5"])), 0);
        assert!(db.contains(b"key5", 0));

        // IFDEQ uses the same digest as DIGEST.
        str_of(&mut db, "key6", "value6");
        let digest = format!("{:016x}", xxh3_64(b"value6"));
        assert_eq!(int_of(cmd(&mut db, &[b"DELEX", b"key6", b"IFDEQ", digest.as_bytes()])), 1);
        assert!(!db.contains(b"key6", 0));

        str_of(&mut db, "key7", "value7");
        assert_eq!(int_of(cmd(&mut db, &[b"DELEX", b"key7", b"IFDEQ", b"0000000000000000"])), 0);
        assert!(db.contains(b"key7", 0));

        // IFDNE deletes when digests differ, not when they match.
        str_of(&mut db, "key8", "value8");
        assert_eq!(int_of(cmd(&mut db, &[b"DELEX", b"key8", b"IFDNE", b"0000000000000000"])), 1);
        assert!(!db.contains(b"key8", 0));

        str_of(&mut db, "key9", "value9");
        let digest9 = format!("{:016x}", xxh3_64(b"value9"));
        assert_eq!(int_of(cmd(&mut db, &[b"DELEX", b"key9", b"IFDNE", digest9.as_bytes()])), 0);
        assert!(db.contains(b"key9", 0));

        // Condition against a non-string key is WRONGTYPE.
        db.insert(CompactString::from("list1"), PrimeValue::List(crate::core::quicklist::QuickList::default()));
        assert_eq!(
            err_of(cmd(&mut db, &[b"DELEX", b"list1", b"IFEQ", b"item"])),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );

        // Invalid option is an unknown-subcommand error.
        str_of(&mut db, "key10", "value10");
        assert!(err_of(cmd(&mut db, &[b"DELEX", b"key10", b"INVALID", b"value"]))
            .contains("Unknown subcommand"));

        // Wrong number of arguments in several shapes.
        str_of(&mut db, "key11", "v");
        assert!(err_of(cmd(&mut db, &[b"DELEX", b"key11", b"randomarg"])).contains("wrong number"));
        assert!(err_of(cmd(&mut db, &[b"DELEX", b"key12", b"IFEQ"])).contains("wrong number"));
        assert!(err_of(cmd(&mut db, &[b"DELEX", b"key13", b"xyz"])).contains("wrong number"));
        assert!(err_of(cmd(&mut db, &[b"DELEX", b"key14", b"IFEQ", b"val", b"extra"]))
            .contains("wrong number"));
    }

    /// Port of `GenericFamilyTest.Stick`.
    #[test]
    fn stick() {
        let mut db = DbSlice::new(0);

        // STICK returns 0 on non-existent keys.
        assert_eq!(int_of(cmd(&mut db, &[b"STICK", b"a", b"b"])), 0);

        for key in ["a", "b", "c", "d"] {
            str_of(&mut db, key, ".");
        }

        // STICK is applied only once per key.
        assert_eq!(int_of(cmd(&mut db, &[b"STICK", b"a", b"b"])), 2);
        assert_eq!(int_of(cmd(&mut db, &[b"STICK", b"a", b"b"])), 0);
        assert_eq!(int_of(cmd(&mut db, &[b"STICK", b"a", b"c"])), 1);
        assert_eq!(int_of(cmd(&mut db, &[b"STICK", b"b", b"d"])), 1);
        assert_eq!(int_of(cmd(&mut db, &[b"STICK", b"c", b"d"])), 0);

        // Stickiness persists across writes (SET/APPEND replace the value).
        str_of(&mut db, "a", "new");
        assert_eq!(int_of(cmd(&mut db, &[b"STICK", b"a"])), 0);

        // RENAME moves stickiness (single-shard path here).
        assert_eq!(val(cmd(&mut db, &[b"RENAME", b"a", b"k"])), val(CmdResult::Ok(ok())));
        assert_eq!(int_of(cmd(&mut db, &[b"STICK", b"k"])), 0);
        assert_eq!(db.ttl_ms(b"k", 0), -1);
    }

    /// A sticky key does not expire even with a TTL.
    #[test]
    fn stick_prevents_expiry() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "k", "v");
        db.set_expiry(b"k", 10_000, 0);
        assert_eq!(int_of(cmd(&mut db, &[b"STICK", b"k"])), 1);
        assert!(db.contains(b"k", 50_000));
        assert!(!db.is_sticky(b"x"));
        assert_eq!(int_of(cmd(&mut db, &[b"DEL", b"k"])), 1);
        assert!(!db.is_sticky(b"k"));
    }

    /// COPY carries stickiness to the destination.
    #[test]
    fn copy_preserves_sticky() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "s", "v");
        assert_eq!(int_of(cmd(&mut db, &[b"STICK", b"s"])), 1);
        assert_eq!(int_of(cmd(&mut db, &[b"COPY", b"s", b"d"])), 1);
        assert!(db.is_sticky(b"d"));
        assert_eq!(int_of(cmd(&mut db, &[b"STICK", b"d"])), 0);
    }

    fn move_dbs(dbs: &mut [DbSlice], db_idx: usize, now: u64, args: &[&[u8]]) -> CmdResult {
        let argv: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
        exec_move_on_dbs(dbs, db_idx, &argv, now)
    }

    /// Port of `GenericFamilyTest.Move`.
    #[test]
    fn move_key() {
        let now = 1000;
        let mut dbs = vec![DbSlice::new(0), DbSlice::new(1)];

        // Missing key: 0.
        assert_eq!(int_of(move_dbs(&mut dbs, 0, now, &[b"MOVE", b"a", b"1"])), 0);

        // Non-existent DB indices.
        assert_eq!(
            err_of(move_dbs(&mut dbs, 0, now, &[b"MOVE", b"a", b"-1"])),
            "DB index is out of range"
        );
        assert_eq!(
            err_of(move_dbs(&mut dbs, 0, now, &[b"MOVE", b"a", b"100500"])),
            "DB index is out of range"
        );
        assert_eq!(
            err_of(move_dbs(&mut dbs, 0, now, &[b"MOVE", b"a", b"0"])),
            "source and destination objects are the same"
        );

        // MOVE moves value & expiry & stickiness.
        str_of(&mut dbs[0], "a", "test");
        dbs[0].set_expiry(b"a", now + 1000, now);
        dbs[0].set_sticky_flag(b"a", true);
        assert_eq!(int_of(move_dbs(&mut dbs, 0, now, &[b"MOVE", b"a", b"1"])), 1);
        assert!(!dbs[0].contains(b"a", now));
        assert_eq!(dbs[1].ttl_ms(b"a", now), 1000);
        assert!(dbs[1].is_sticky(b"a"));
        match dbs[1].find(b"a", now) {
            Some(PrimeValue::Str(s)) => assert_eq!(s.as_bytes(), b"test"),
            o => panic!("expected string, got {:?}", o),
        }

        // MOVE doesn't move if the destination key exists.
        str_of(&mut dbs[1], "a", "existing");
        str_of(&mut dbs[0], "a", "other");
        assert_eq!(int_of(move_dbs(&mut dbs, 0, now, &[b"MOVE", b"a", b"1"])), 0);
        match dbs[1].find(b"a", now) {
            Some(PrimeValue::Str(s)) => assert_eq!(s.as_bytes(), b"existing"),
            o => panic!("expected string, got {:?}", o),
        }
        assert!(dbs[0].contains(b"a", now));
    }

    /// Port of `GenericFamilyTest.Scan` (SCAN clauses) plus the KEYS tail. The
    /// reference test flushes the DB before the KEYS assertions; here the
    /// empty-key case is checked on a fresh DbSlice instead.
    #[test]
    fn scan() {
        let mut db = DbSlice::new(0);
        for i in 0..10 {
            str_of(&mut db, &format!("key{i}"), "bar");
        }
        for i in 0..10 {
            str_of(&mut db, &format!("str{i}"), "bar");
        }
        for i in 0..10 {
            db.insert(
                CompactString::from_bytes(format!("set{i}").as_bytes()),
                PrimeValue::Set(Set::new()),
            );
        }
        for i in 0..10 {
            db.insert(
                CompactString::from_bytes(format!("zset{i}").as_bytes()),
                PrimeValue::ZSet(ZSet::new()),
            );
        }

        let keys = scan_keys(&mut db, 0, &[b"SCAN", b"0", b"COUNT", b"20", b"TYPE", b"string"]);
        assert!(keys.len() > 10);
        assert!(keys.iter().all(|k| k.starts_with(b"str") || k.starts_with(b"key")));

        let keys = scan_keys(&mut db, 0, &[b"SCAN", b"0", b"COUNT", b"20", b"MATCH", b"zset*"]);
        assert_eq!(keys.len(), 10);
        assert!(keys.iter().all(|k| k.starts_with(b"zset")));

        assert_eq!(err_of(cmd(&mut db, &[b"SCAN", b"0", b"COUNT"])), "ERR syntax error");
        assert_eq!(
            err_of(cmd(&mut db, &[b"SCAN", b"0", b"COUNT", b"not-a-number"])),
            "ERR value is not an integer or out of range"
        );
        assert_eq!(err_of(cmd(&mut db, &[b"SCAN", b"0", b"TYPE", b"not-a-type"])), "ERR syntax error");
        assert_eq!(err_of(cmd(&mut db, &[b"SCAN", b"0", b"NOVALUES"])), "ERR syntax error");

        // COUNT is a size_t hint: values above UINT32_MAX must still parse.
        let resp = val(cmd(&mut db, &[b"SCAN", b"0", b"COUNT", b"5000000000"]));
        match resp {
            RespValue::Array(a) => assert_eq!(a.len(), 2),
            o => panic!("expected scan reply array, got {:?}", o),
        }
    }

    /// KEYS handles empty-string keys (reference `GenericFamilyTest.Scan` tail).
    #[test]
    fn keys_empty_string_key() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "", "foo");
        str_of(&mut db, "bar", "1");

        let mut keys = array_keys(cmd(&mut db, &[b"KEYS", b"*"]));
        keys.sort();
        assert_eq!(keys, vec![Vec::<u8>::new(), b"bar".to_vec()]);

        assert_eq!(array_keys(cmd(&mut db, &[b"KEYS", b""])), vec![Vec::<u8>::new()]);
    }

    /// Port of `GenericFamilyTest.ScanWithAttr`: ATTR filters by TTL
    /// (v/p) and by the touched flag (a/u), which is set by reads and TTL
    /// lookups but not by plain SET inserts.
    #[test]
    fn scan_with_attr() {
        let now = 0u64;
        let mut db = DbSlice::new(0);
        str_of(&mut db, "hello", "world");
        str_of(&mut db, "foo", "bar");

        // expire hello 1000 -> PEXPIREAT (ms) in the future.
        cmd_at(&mut db, now, &[b"PEXPIREAT", b"hello", b"1000000"]);

        assert_eq!(scan_keys(&mut db, now, &[b"SCAN", b"0", b"ATTR", b"v"]), vec![b"hello".to_vec()]);
        assert_eq!(scan_keys(&mut db, now, &[b"SCAN", b"0", b"ATTR", b"p"]), vec![b"foo".to_vec()]);
        // Before the GET, only "hello" was touched (by the expire lookup).
        assert_eq!(scan_keys(&mut db, now, &[b"SCAN", b"0", b"ATTR", b"a"]), vec![b"hello".to_vec()]);
        assert_eq!(scan_keys(&mut db, now, &[b"SCAN", b"0", b"ATTR", b"u"]), vec![b"foo".to_vec()]);

        // GET "foo" is a read: it marks "foo" as touched.
        match db.find(b"foo", now) {
            Some(PrimeValue::Str(s)) => assert_eq!(s.as_bytes(), b"bar"),
            o => panic!("expected string, got {:?}", o),
        }

        assert_eq!(scan_keys(&mut db, now, &[b"SCAN", b"0", b"ATTR", b"a"]).len(), 2);
        assert_eq!(scan_keys(&mut db, now, &[b"SCAN", b"0", b"ATTR", b"u"]).len(), 0);
    }

    /// Port of `GenericFamilyTest.ScanMallocSize`: MINMSZ filters by the
    /// value's approximated allocated size (0/496/1000 for 15/500/1000-byte
    /// values).
    #[test]
    fn scan_malloc_size() {
        let mut db = DbSlice::new(0);
        let v1 = "a".repeat(1000);
        let v2 = "b".repeat(500);
        let v3 = "c".repeat(15);
        str_of(&mut db, "k1", &v1);
        str_of(&mut db, "k2", &v2);
        str_of(&mut db, "k3", &v3);

        let mut keys = scan_keys(&mut db, 0, &[b"SCAN", b"0", b"MINMSZ", b"15"]);
        keys.sort();
        assert_eq!(keys, vec![b"k1".to_vec(), b"k2".to_vec()]);

        assert_eq!(scan_keys(&mut db, 0, &[b"SCAN", b"0", b"MINMSZ", b"500"]), vec![b"k1".to_vec()]);
    }

    /// Run one RM call and return (next_cursor, deleted).
    fn rm_step(db: &mut DbSlice, cursor: &str, count: &str) -> (u64, i64) {
        let argv = vec![
            b"RM".to_vec(),
            cursor.as_bytes().to_vec(),
            b"COUNT".to_vec(),
            count.as_bytes().to_vec(),
        ];
        match val(dispatch_at(db, 0, &argv)) {
            RespValue::Array(a) if a.len() == 2 => {
                let next = match &a[0] {
                    RespValue::Bulk(b) => String::from_utf8_lossy(b).parse::<u64>().unwrap(),
                    o => panic!("expected cursor, got {o:?}"),
                };
                let deleted = match &a[1] {
                    RespValue::Integer(n) => *n,
                    o => panic!("expected count, got {o:?}"),
                };
                (next, deleted)
            }
            o => panic!("expected rm reply, got {o:?}"),
        }
    }

    #[test]
    fn rm_deletes_in_pages() {
        let mut db = DbSlice::new(0);
        let keys: Vec<String> = (0..25).map(|i| format!("k{i:02}")).collect();
        for k in &keys {
            str_of(&mut db, k, "v");
        }

        // Deleting 10 at a time walks through the whole keyspace.
        let mut cursor = 0u64;
        let mut total = 0i64;
        loop {
            let (next, deleted) = rm_step(&mut db, &cursor.to_string(), "10");
            total += deleted;
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(total, 25);
        for k in &keys {
            assert!(db.find(k.as_bytes(), 0).is_none());
        }
        // A further call on an empty database reports zero and stays at zero.
        assert_eq!(rm_step(&mut db, "0", "10"), (0, 0));
    }

    #[test]
    fn rm_matches_and_types() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "str1", "v");
        str_of(&mut db, "str2", "v");
        list_of(&mut db, "list1", &["a", "b"]);
        set_of(&mut db, "set1", &["a"]);
        zset_of(&mut db, "zset1", &["a"]);

        // MATCH filters by glob.
        let argv = vec![b"RM".to_vec(), b"0".to_vec(), b"COUNT".to_vec(), b"10".to_vec(), b"MATCH".to_vec(), b"str*".to_vec()];
        match val(dispatch_at(&mut db, 0, &argv)) {
            RespValue::Array(a) if a.len() == 2 => {
                assert_eq!(a[0], RespValue::Bulk(b"0".to_vec()));
                assert_eq!(a[1], RespValue::Integer(2));
            }
            o => panic!("expected rm reply, got {o:?}"),
        }
        assert!(db.find(b"str1", 0).is_none());
        assert!(db.find(b"str2", 0).is_none());
        assert!(db.find(b"list1", 0).is_some());

        // TYPE filters by value type (list).
        let argv = vec![b"RM".to_vec(), b"0".to_vec(), b"COUNT".to_vec(), b"10".to_vec(), b"TYPE".to_vec(), b"list".to_vec()];
        match val(dispatch_at(&mut db, 0, &argv)) {
            RespValue::Array(a) if a.len() == 2 => {
                assert_eq!(a[0], RespValue::Bulk(b"0".to_vec()));
                assert_eq!(a[1], RespValue::Integer(1));
            }
            o => panic!("expected rm reply, got {o:?}"),
        }
        assert!(db.find(b"list1", 0).is_none());
        assert!(db.find(b"set1", 0).is_some());
    }

    #[test]
    fn rm_help_and_invalid_cursor() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "k", "v");
        let argv = bvecs!["RM", "HELP"];
        match val(dispatch_at(&mut db, 0, &argv)) {
            RespValue::Array(help) => {
                assert!(help.iter().any(|v| matches!(v, RespValue::Simple(s) if s.contains("RM cursor"))));
            }
            o => panic!("expected help array, got {o:?}"),
        }
        assert_eq!(err_of(dispatch_at(&mut db, 0, &bvecs!["RM", "abc"])), "ERR invalid cursor");
        // Invalid COUNT is an integer error.
        assert!(err_of(dispatch_at(&mut db, 0, &bvecs!["RM", "0", "COUNT", "x"]))
            .contains("not an integer"));
    }

    #[test]
    fn rm_merge_accumulates_across_shards() {
        let parts = |v: &[(usize, u64, u64)]| -> Vec<ShardPart> {
            v.iter()
                .map(|&(shard, token, deleted)| ShardPart {
                    shard,
                    owned_key_idxs: vec![],
                    result: CmdResult::Ok(rm_reply(token, deleted)),
                })
                .collect()
        };
        let decode = |args: &[Vec<u8>], p: &[(usize, u64, u64)]| -> (u64, i64) {
            match val(merge_rm(&parts(p), args, &[], 0)) {
                RespValue::Array(a) if a.len() == 2 => {
                    let c = match &a[0] {
                        RespValue::Bulk(b) => String::from_utf8_lossy(b).parse::<u64>().unwrap(),
                        o => panic!("expected cursor, got {o:?}"),
                    };
                    let d = match &a[1] {
                        RespValue::Integer(n) => *n,
                        o => panic!("expected count, got {o:?}"),
                    };
                    (c, d)
                }
                o => panic!("expected rm reply, got {o:?}"),
            }
        };
        let args = vec![b"RM".to_vec(), b"0".to_vec(), b"COUNT".to_vec(), b"10".to_vec()];

        // Shard 0 is mid-scan: its token is forwarded unchanged.
        assert_eq!(decode(&args, &[(0, (2 << 10) | 0, 10), (1, 0, 0)]), ((2 << 10) | 0, 10));

        // Shard 0 exhausted: resume at shard 1.
        assert_eq!(decode(&args, &[(0, 0, 8), (1, (2 << 10) | 1, 10)]), (1, 8));
        assert_eq!(decode(&args, &[(0, 0, 10), (1, 0, 3)]), (1, 10));

        // Cursor already in shard 1, which still has more keys.
        let args1 = vec![b"RM".to_vec(), b"1".to_vec(), b"COUNT".to_vec(), b"10".to_vec()];
        assert_eq!(decode(&args1, &[(0, 0, 0), (1, (3 << 10) | 1, 10)]), ((3 << 10) | 1, 10));

        // Last shard exhausted: the whole scan is finished.
        assert_eq!(decode(&args1, &[(0, 0, 0), (1, 0, 10)]), (0, 10));

        // Cursor past the last shard: empty reply.
        let args2 = vec![b"RM".to_vec(), b"9".to_vec(), b"COUNT".to_vec(), b"10".to_vec()];
        assert_eq!(decode(&args2, &[(0, 0, 0), (1, 0, 0)]), (0, 0));
    }

    fn list_of(db: &mut DbSlice, key: &str, items: &[&str]) {
        let ql = QuickList::from_items(
            items.iter().map(|s| ListItem::Str(CompactString::from(*s))).collect(),
        );
        db.insert(CompactString::from_bytes(key.as_bytes()), PrimeValue::List(ql));
    }

    fn set_of(db: &mut DbSlice, key: &str, members: &[&str]) {
        let mut s = Set::new();
        for &m in members {
            s.add(CompactString::from(m));
        }
        db.insert(CompactString::from_bytes(key.as_bytes()), PrimeValue::Set(s));
    }

    fn zset_of(db: &mut DbSlice, key: &str, members: &[&str]) {
        let mut z = ZSet::new();
        for &m in members {
            z.insert(CompactString::from(m), 0.0);
        }
        db.insert(CompactString::from_bytes(key.as_bytes()), PrimeValue::ZSet(z));
    }

    /// Reply values as bulk strings.
    fn bulks(r: CmdResult) -> Vec<Vec<u8>> {
        match val(r) {
            RespValue::Array(a) => a
                .iter()
                .map(|v| match v {
                    RespValue::Bulk(b) => b.clone(),
                    o => panic!("expected bulk, got {:?}", o),
                })
                .collect(),
            RespValue::Bulk(b) => vec![b.clone()],
            o => panic!("expected array, got {:?}", o),
        }
    }

    fn lrange_of(db: &mut DbSlice, key: &str) -> Vec<Vec<u8>> {
        match db.find(key.as_bytes(), 0) {
            Some(PrimeValue::List(l)) => l.iter().map(|it| it.as_bytes()).collect(),
            o => panic!("expected list, got {:?}", o),
        }
    }

    /// Port of `GenericFamilyTest.Sort`: numeric/alpha/desc/limit sorting over
    /// lists, sets, intsets and sorted sets, plus error cases.
    #[test]
    fn sort() {
        let mut db = DbSlice::new(0);
        list_of(&mut db, "list-1", &["3.5", "1.2", "10.1", "2.20", "200"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1"])), bvecs!["1.2", "2.20", "3.5", "10.1", "200"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"ALPHA"])), bvecs!["1.2", "10.1", "2.20", "200", "3.5"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"DESC"])), bvecs!["200", "10.1", "3.5", "2.20", "1.2"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"DESC", b"ALPHA"])), bvecs!["3.5", "200", "2.20", "10.1", "1.2"]);
        // ASC/DESC last-one-wins.
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"DESC", b"ASC"])), bvecs!["1.2", "2.20", "3.5", "10.1", "200"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"ASC", b"DESC"])), bvecs!["200", "10.1", "3.5", "2.20", "1.2"]);
        // Limits.
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"LIMIT", b"0", b"5"])), bvecs!["1.2", "2.20", "3.5", "10.1", "200"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"LIMIT", b"0", b"10"])), bvecs!["1.2", "2.20", "3.5", "10.1", "200"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"LIMIT", b"2", b"2"])), bvecs!["3.5", "10.1"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"LIMIT", b"1", b"1"])), bvecs!["2.20"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"LIMIT", b"4", b"2"])), bvecs!["200"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"LIMIT", b"5", b"2"])), bvecs![]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"DESC", b"LIMIT", b"0", b"5"])), bvecs!["200", "10.1", "3.5", "2.20", "1.2"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"DESC", b"LIMIT", b"2", b"2"])), bvecs!["3.5", "2.20"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"DESC", b"LIMIT", b"1", b"1"])), bvecs!["10.1"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"DESC", b"LIMIT", b"5", b"2"])), bvecs![]);

        set_of(&mut db, "set-1", &["5.3", "4.4", "60", "99.9", "100", "9"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"set-1"])), bvecs!["4.4", "5.3", "9", "60", "99.9", "100"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"set-1", b"ALPHA"])), bvecs!["100", "4.4", "5.3", "60", "9", "99.9"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"set-1", b"DESC"])), bvecs!["100", "99.9", "60", "9", "5.3", "4.4"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"set-1", b"DESC", b"ALPHA"])), bvecs!["99.9", "9", "60", "5.3", "4.4", "100"]);

        set_of(&mut db, "intset-1", &["5", "4", "3", "2", "1"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"intset-1"])), bvecs!["1", "2", "3", "4", "5"]);

        zset_of(&mut db, "zset-1", &["3.3", "30.1", "8.2"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"zset-1"])), bvecs!["3.3", "8.2", "30.1"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"zset-1", b"ALPHA"])), bvecs!["3.3", "30.1", "8.2"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"zset-1", b"DESC"])), bvecs!["30.1", "8.2", "3.3"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"zset-1", b"DESC", b"ALPHA"])), bvecs!["8.2", "30.1", "3.3"]);

        // Missing key -> empty array.
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-2"])), bvecs![]);

        // Non-numeric and NaN elements error.
        list_of(&mut db, "list-2", &["NOTADOUBLE"]);
        assert_eq!(err_of(cmd(&mut db, &[b"SORT", b"list-2"])), SORT_SCORE_ERR);
        list_of(&mut db, "NANvalue", &["nan"]);
        assert_eq!(err_of(cmd(&mut db, &[b"SORT", b"NANvalue"])), SORT_SCORE_ERR);

        // Wrong type.
        str_of(&mut db, "foo", "bar");
        assert_eq!(err_of(cmd(&mut db, &[b"SORT", b"foo"])), "WRONGTYPE Operation against a key holding the wrong kind of value");

        // Empty string parses to 0 and ties are broken lexicographically.
        list_of(&mut db, "list-3", &[""]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-3"])), vec![vec![]]);
        list_of(&mut db, "list-3", &["", "2", "0", "", "-0.14", "0.12", "-0", "-123123", "7654"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-3"])), bvecs!["-123123", "-0.14", "", "", "-0", "0", "0.12", "2", "7654"]);
    }

    /// Port of `GenericFamilyTest.SortBug3636`: alpha sort of floats with a
    /// stable size.
    #[test]
    fn sort_bug3636() {
        let mut db = DbSlice::new(0);
        list_of(
            &mut db,
            "foo",
            &[
                "1.100000023841858", "1.100000023841858", "1.100000023841858", "-15710",
                "1.100000023841858", "1.100000023841858", "1.100000023841858", "-15710", "-15710",
                "1.100000023841858", "-15710", "-15710", "-15710", "-15710", "1.100000023841858",
                "-15710", "-15710",
            ],
        );
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"foo", b"desc", b"alpha"])).len(), 17);
    }

    /// Port of `GenericFamilyTest.SortStore`.
    #[test]
    fn sort_store() {
        let mut db = DbSlice::new(0);
        list_of(&mut db, "list-1", &["3.5", "1.2", "10.1", "2.20", "200"]);
        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"list-1", b"store", b"list-2"])), 5);
        assert_eq!(lrange_of(&mut db, "list-2"), bvecs!["1.2", "2.20", "3.5", "10.1", "200"]);
        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"list-1", b"ALPHA", b"store", b"list-2"])), 5);
        assert_eq!(lrange_of(&mut db, "list-2"), bvecs!["1.2", "10.1", "2.20", "200", "3.5"]);
        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"list-1", b"DESC", b"store", b"list-2"])), 5);
        assert_eq!(lrange_of(&mut db, "list-2"), bvecs!["200", "10.1", "3.5", "2.20", "1.2"]);
        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"list-1", b"ALPHA", b"DESC", b"store", b"list-2"])), 5);
        assert_eq!(lrange_of(&mut db, "list-2"), bvecs!["3.5", "200", "2.20", "10.1", "1.2"]);

        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"list-1", b"LIMIT", b"2", b"2", b"store", b"list-2"])), 2);
        assert_eq!(lrange_of(&mut db, "list-2"), bvecs!["3.5", "10.1"]);
        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"list-1", b"LIMIT", b"1", b"1", b"store", b"list-2"])), 1);
        assert_eq!(lrange_of(&mut db, "list-2"), bvecs!["2.20"]);
        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"list-1", b"LIMIT", b"5", b"2", b"store", b"list-2"])), 0);
        assert_eq!(int_of(cmd(&mut db, &[b"EXISTS", b"list-2"])), 0);

        set_of(&mut db, "set-1", &["5.3", "4.4", "60", "99.9", "100", "9"]);
        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"set-1", b"store", b"list-3"])), 6);
        assert_eq!(lrange_of(&mut db, "list-3"), bvecs!["4.4", "5.3", "9", "60", "99.9", "100"]);

        zset_of(&mut db, "zset-1", &["3.3", "30.1", "8.2"]);
        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"zset-1", b"store", b"list-4"])), 3);
        assert_eq!(lrange_of(&mut db, "list-4"), bvecs!["3.3", "8.2", "30.1"]);

        // Same key overwrite.
        list_of(&mut db, "list-1", &["3.5", "1.2", "10.1", "2.20", "200"]);
        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"list-1", b"store", b"list-1"])), 5);
        assert_eq!(lrange_of(&mut db, "list-1"), bvecs!["1.2", "2.20", "3.5", "10.1", "200"]);
    }

    /// Port of `GenericFamilyTest.SortStoreEmptyResult`: an empty stored result
    /// must delete the destination key rather than leaving an empty list.
    #[test]
    fn sort_store_empty_result() {
        let mut db = DbSlice::new(0);
        list_of(&mut db, "list-src", &["3", "1", "2"]);
        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"list-src", b"LIMIT", b"10", b"5", b"store", b"dest"])), 0);
        assert_eq!(int_of(cmd(&mut db, &[b"EXISTS", b"dest"])), 0);

        str_of(&mut db, "dest", "old");
        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"list-src", b"LIMIT", b"0", b"0", b"store", b"dest"])), 0);
        assert_eq!(int_of(cmd(&mut db, &[b"EXISTS", b"dest"])), 0);
    }

    /// Port of `GenericFamilyTest.SortStoreResetsExpiry`: SORT STORE clears the
    /// destination's expiry.
    #[test]
    fn sort_store_resets_expiry() {
        let mut db = DbSlice::new(0);
        set_of(&mut db, "src", &["3", "1", "2"]);
        set_of(&mut db, "dest", &["old"]);
        cmd_at(&mut db, 0, &[b"EXPIRE", b"dest", b"100"]);
        assert!(int_of(cmd_at(&mut db, 0, &[b"TTL", b"dest"])) > 0);

        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"src", b"store", b"dest"])), 3);
        assert_eq!(int_of(cmd_at(&mut db, 0, &[b"TTL", b"dest"])), -1);
        assert_eq!(lrange_of(&mut db, "dest"), bvecs!["1", "2", "3"]);

        set_of(&mut db, "myset", &["c", "a", "b"]);
        cmd_at(&mut db, 0, &[b"EXPIRE", b"myset", b"100"]);
        assert!(int_of(cmd_at(&mut db, 0, &[b"TTL", b"myset"])) > 0);
        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"myset", b"ALPHA", b"store", b"myset"])), 3);
        assert_eq!(int_of(cmd_at(&mut db, 0, &[b"TTL", b"myset"])), -1);
        assert_eq!(lrange_of(&mut db, "myset"), bvecs!["a", "b", "c"]);
    }

    /// Port of `GenericFamilyTest.Sort_RO`.
    #[test]
    fn sort_ro() {
        let mut db = DbSlice::new(0);
        list_of(&mut db, "list-1", &["3.5", "1.2", "10.1", "2.20", "200"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"list-1"])), bvecs!["1.2", "2.20", "3.5", "10.1", "200"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"list-1", b"ALPHA"])), bvecs!["1.2", "10.1", "2.20", "200", "3.5"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"list-1", b"DESC"])), bvecs!["200", "10.1", "3.5", "2.20", "1.2"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"list-1", b"DESC", b"ALPHA"])), bvecs!["3.5", "200", "2.20", "10.1", "1.2"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"list-1", b"LIMIT", b"2", b"2"])), bvecs!["3.5", "10.1"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"list-1", b"DESC", b"LIMIT", b"1", b"1"])), bvecs!["10.1"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"list-1", b"LIMIT", b"5", b"2"])), bvecs![]);

        set_of(&mut db, "set-1", &["5.3", "4.4", "60", "99.9", "100", "9"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"set-1"])), bvecs!["4.4", "5.3", "9", "60", "99.9", "100"]);
        set_of(&mut db, "intset-1", &["5", "4", "3", "2", "1"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"intset-1"])), bvecs!["1", "2", "3", "4", "5"]);
        zset_of(&mut db, "zset-1", &["3.3", "30.1", "8.2"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"zset-1"])), bvecs!["3.3", "8.2", "30.1"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"zset-1", b"ALPHA"])), bvecs!["3.3", "30.1", "8.2"]);

        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"list-2"])), bvecs![]);
        list_of(&mut db, "list-2", &["NOTADOUBLE"]);
        assert_eq!(err_of(cmd(&mut db, &[b"SORT_RO", b"list-2"])), SORT_SCORE_ERR);
        str_of(&mut db, "foo", "bar");
        assert_eq!(err_of(cmd(&mut db, &[b"SORT_RO", b"foo"])), "WRONGTYPE Operation against a key holding the wrong kind of value");

        list_of(&mut db, "list-3", &["", "2", "0", "", "-0.14", "0.12", "-0", "-123123", "7654"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"list-3"])), bvecs!["-123123", "-0.14", "", "", "-0", "0", "0.12", "2", "7654"]);
        list_of(&mut db, "NANvalue", &["nan"]);
        assert_eq!(err_of(cmd(&mut db, &[b"SORT_RO", b"NANvalue"])), SORT_SCORE_ERR);

        // STORE is rejected for SORT_RO.
        assert_eq!(err_of(cmd(&mut db, &[b"SORT_RO", b"list-1", b"store", b"list-2"])), "ERR syntax error");
    }

    /// Port of `GenericFamilyTest.SortROBug3636`.
    #[test]
    fn sort_ro_bug3636() {
        let mut db = DbSlice::new(0);
        list_of(
            &mut db,
            "foo",
            &[
                "1.100000023841858", "1.100000023841858", "1.100000023841858", "-15710",
                "1.100000023841858", "1.100000023841858", "1.100000023841858", "-15710", "-15710",
                "1.100000023841858", "-15710", "-15710", "-15710", "-15710", "1.100000023841858",
                "-15710", "-15710",
            ],
        );
        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"foo", b"desc", b"alpha"])).len(), 17);
    }

    /// Port of `GenericFamilyTest.SortNegativeLimit`.
    #[test]
    fn sort_negative_limit() {
        let mut db = DbSlice::new(0);
        list_of(&mut db, "list-neg", &["1", "2", "3", "4", "5"]);
        let cases: [[&[u8]; 2]; 3] = [[b"-1", b"2"], [b"0", b"-1"], [b"-1", b"-1"]];
        for limit in &cases {
            assert_eq!(
                err_of(cmd(&mut db, &[b"SORT", b"list-neg", b"LIMIT", limit[0], limit[1]])),
                "ERR value is not an integer or out of range"
            );
        }
    }

    /// Port of `GenericFamilyTest.SortBy`.
    #[test]
    fn sort_by() {
        let mut db = DbSlice::new(0);
        list_of(&mut db, "list-1", &["1", "2", "3"]);
        str_of(&mut db, "w_1", "30");
        str_of(&mut db, "w_2", "20");
        str_of(&mut db, "w_3", "10");
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"BY", b"w_*"])), bvecs!["3", "2", "1"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"BY", b"w_*", b"DESC"])), bvecs!["1", "2", "3"]);

        str_of(&mut db, "s_1", "c");
        str_of(&mut db, "s_2", "b");
        str_of(&mut db, "s_3", "a");
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"BY", b"s_*", b"ALPHA"])), bvecs!["3", "2", "1"]);
        // nosort preserves insertion order.
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"BY", b"nosort"])), bvecs!["1", "2", "3"]);
        // Missing weights read as 0.
        cmd(&mut db, &[b"DEL", b"w_1"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"BY", b"w_*"])), bvecs!["1", "3", "2"]);
        str_of(&mut db, "w_1", "30");
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"list-1", b"BY", b"w_*", b"LIMIT", b"1", b"2"])), bvecs!["2", "1"]);
        assert_eq!(err_of(cmd(&mut db, &[b"SORT", b"list-1", b"BY", b"w_*_*"])), "ERR syntax error");
    }

    /// Port of `GenericFamilyTest.SortGet`.
    #[test]
    fn sort_get() {
        let mut db = DbSlice::new(0);
        list_of(&mut db, "mylist", &["1", "2", "3"]);
        str_of(&mut db, "obj_1", "first");
        str_of(&mut db, "obj_2", "second");
        str_of(&mut db, "obj_3", "third");
        str_of(&mut db, "weight_1", "30");
        str_of(&mut db, "weight_2", "20");
        str_of(&mut db, "weight_3", "10");

        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"mylist", b"GET", b"obj_*"])), bvecs!["first", "second", "third"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"mylist", b"GET", b"#"])), bvecs!["1", "2", "3"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"mylist", b"GET", b"#", b"GET", b"obj_*"])), bvecs!["1", "first", "2", "second", "3", "third"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"mylist", b"BY", b"weight_*", b"GET", b"obj_*"])), bvecs!["third", "second", "first"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"mylist", b"BY", b"weight_*", b"GET", b"#", b"GET", b"obj_*"])), bvecs!["3", "third", "2", "second", "1", "first"]);

        // Missing GET key -> empty string.
        cmd(&mut db, &[b"DEL", b"obj_2"]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"mylist", b"GET", b"obj_*"])), bvecs!["first", "", "third"]);
        str_of(&mut db, "obj_2", "second");

        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"mylist", b"DESC", b"GET", b"obj_*"])), bvecs!["third", "second", "first"]);

        list_of(&mut db, "strlist", &["c", "b", "a"]);
        str_of(&mut db, "obj_a", "alpha");
        str_of(&mut db, "obj_b", "beta");
        str_of(&mut db, "obj_c", "gamma");
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"strlist", b"ALPHA", b"GET", b"obj_*"])), bvecs!["alpha", "beta", "gamma"]);

        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"mylist", b"GET", b"#", b"GET", b"obj_*", b"LIMIT", b"1", b"2"])), bvecs!["2", "second", "3", "third"]);

        assert_eq!(int_of(cmd(&mut db, &[b"SORT", b"mylist", b"GET", b"#", b"GET", b"obj_*", b"STORE", b"result"])), 6);
        assert_eq!(lrange_of(&mut db, "result"), bvecs!["1", "first", "2", "second", "3", "third"]);

        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"mylist", b"BY", b"nosort", b"GET", b"obj_*"])), bvecs!["first", "second", "third"]);
        assert_eq!(err_of(cmd(&mut db, &[b"SORT", b"mylist", b"GET", b"obj_*_*"])), "ERR syntax error");

        // Empty source list.
        list_of(&mut db, "emptylist", &[]);
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"emptylist", b"GET", b"obj_*"])), bvecs![]);

        // Literal pattern without '*'.
        str_of(&mut db, "fixed_key", "fixed_value");
        assert_eq!(bulks(cmd(&mut db, &[b"SORT", b"mylist", b"GET", b"fixed_key"])), bvecs!["fixed_value", "fixed_value", "fixed_value"]);

        assert_eq!(bulks(cmd(&mut db, &[b"SORT_RO", b"mylist", b"GET", b"#", b"GET", b"obj_*"])), bvecs!["1", "first", "2", "second", "3", "third"]);
    }

    /// Port of `GenericFamilyTest.SortDeletesEmptySet`: iterating an
    /// all-expired set removes the key.
    #[test]
    fn sort_deletes_empty_set() {
        let mut db = DbSlice::new(0);
        let mut s = Set::new();
        for i in 0..20 {
            s.add_expirable(CompactString::from(format!("m{i}")), 1000, false);
        }
        db.insert(CompactString::from("skey"), PrimeValue::Set(s));
        let now = 2000;
        assert_eq!(int_of(cmd_at(&mut db, now, &[b"EXISTS", b"skey"])), 1);
        assert_eq!(bulks(cmd_at(&mut db, now, &[b"SORT", b"skey"])), bvecs![]);
        assert_eq!(int_of(cmd_at(&mut db, now, &[b"EXISTS", b"skey"])), 0);
    }

    /// Port of `GenericFamilyTest.SortByPatternDeletesEmptySet`.
    #[test]
    fn sort_by_pattern_deletes_empty_set() {
        let mut db = DbSlice::new(0);
        let mut s = Set::new();
        for i in 0..20 {
            s.add_expirable(CompactString::from(format!("m{i}")), 1000, false);
        }
        db.insert(CompactString::from("skey"), PrimeValue::Set(s));
        let now = 2000;
        assert_eq!(int_of(cmd_at(&mut db, now, &[b"EXISTS", b"skey"])), 1);
        assert_eq!(bulks(cmd_at(&mut db, now, &[b"SORT", b"skey", b"BY", b"nosort"])), bvecs![]);
        assert_eq!(int_of(cmd_at(&mut db, now, &[b"EXISTS", b"skey"])), 0);
    }

    /// Decode an integer array reply (FIELDEXPIRE results).
    fn ints(r: CmdResult) -> Vec<i64> {
        match val(r) {
            RespValue::Array(v) => v
                .into_iter()
                .map(|x| match x {
                    RespValue::Integer(i) => i,
                    o => panic!("expected integer element, got {:?}", o),
                })
                .collect(),
            o => panic!("expected array, got {:?}", o),
        }
    }

    /// Port of `GenericFamilyTest.FieldTtl`.
    #[test]
    fn fieldttl() {
        let mut db = DbSlice::new(0);
        assert_eq!(int_of(cmd(&mut db, &[b"SADDEX", b"key", b"1", b"val1"])), 1);
        assert_eq!(int_of(cmd(&mut db, &[b"SADDEX", b"key", b"2", b"val2"])), 1);
        assert_eq!(int_of(cmd(&mut db, &[b"SADD", b"key", b"val3"])), 1);

        assert_eq!(-2, int_of(cmd(&mut db, &[b"FIELDTTL", b"nokey", b"val1"])));
        assert_eq!(-3, int_of(cmd(&mut db, &[b"FIELDTTL", b"key", b"bar"])));
        assert_eq!(1, int_of(cmd(&mut db, &[b"FIELDTTL", b"key", b"val1"])));
        assert_eq!(2, int_of(cmd(&mut db, &[b"FIELDTTL", b"key", b"val2"])));
        assert_eq!(-1, int_of(cmd(&mut db, &[b"FIELDTTL", b"key", b"val3"])));

        // 1100ms later val1 (ttl 1s) is expired, val2 has 1s left.
        assert_eq!(-3, int_of(cmd_at(&mut db, 1100, &[b"FIELDTTL", b"key", b"val1"])));
        assert_eq!(1, int_of(cmd_at(&mut db, 1100, &[b"FIELDTTL", b"key", b"val2"])));

        str_of(&mut db, "str", "val");
        assert!(err_of(cmd(&mut db, &[b"FIELDTTL", b"str", b"bar"])).starts_with("WRONGTYPE"));

        assert_eq!(2, int_of(cmd(&mut db, &[b"HSETEX", b"k2", b"1", b"f1", b"v1", b"f2", b"v2"])));
        assert_eq!(1, int_of(cmd(&mut db, &[b"HSET", b"k2", b"f3", b"v3"])));
        assert_eq!(1, int_of(cmd(&mut db, &[b"FIELDTTL", b"k2", b"f1"])));
        assert_eq!(-1, int_of(cmd(&mut db, &[b"FIELDTTL", b"k2", b"f3"])));
        assert_eq!(-3, int_of(cmd(&mut db, &[b"FIELDTTL", b"k2", b"f4"])));
    }

    /// Port of `GenericFamilyTest.FieldExpireSet`.
    #[test]
    fn fieldexpire_set() {
        let mut db = DbSlice::new(0);
        assert_eq!(3, int_of(cmd(&mut db, &[b"SADD", b"key", b"a", b"b", b"c"])));
        let now = 2_000u64;
        assert_eq!(
            ints(cmd_at(&mut db, now, &[b"FIELDEXPIRE", b"key", b"10", b"a", b"b", b"c"])),
            [1, 1, 1]
        );
        assert_eq!(10, int_of(cmd_at(&mut db, now, &[b"FIELDTTL", b"key", b"a"])));
        // 10s later all members expired; reading the set removes the key.
        let later = now + 10_000;
        assert_eq!(bulks(cmd_at(&mut db, later, &[b"SMEMBERS", b"key"])), bvecs![]);
    }

    /// Port of `GenericFamilyTest.FieldExpireHset`.
    #[test]
    fn fieldexpire_hset() {
        let mut db = DbSlice::new(0);
        assert_eq!(3, int_of(cmd(&mut db, &[b"HSET", b"key", b"k0", b"v", b"k1", b"v", b"k2", b"v"])));
        let now = 2_000u64;
        assert_eq!(
            ints(cmd_at(&mut db, now, &[b"FIELDEXPIRE", b"key", b"10", b"k0", b"k1", b"k2"])),
            [1, 1, 1]
        );
        assert_eq!(10, int_of(cmd_at(&mut db, now, &[b"FIELDTTL", b"key", b"k0"])));
        let later = now + 10_000;
        assert_eq!(bulks(cmd_at(&mut db, later, &[b"HGETALL", b"key"])), bvecs![]);
    }

    /// Port of `GenericFamilyTest.FieldExpireNoSuchField`.
    #[test]
    fn fieldexpire_no_such_field() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int_of(cmd(&mut db, &[b"SADD", b"key", b"a"])));
        assert_eq!(1, int_of(cmd(&mut db, &[b"HSET", b"key2", b"k0", b"v0"])));
        assert_eq!(ints(cmd(&mut db, &[b"FIELDEXPIRE", b"key", b"10", b"a", b"b"])), [1, -2]);
        assert_eq!(ints(cmd(&mut db, &[b"FIELDEXPIRE", b"key2", b"10", b"k0", b"b"])), [1, -2]);
    }

    /// Port of `GenericFamilyTest.FieldExpireNoSuchKey`.
    #[test]
    fn fieldexpire_no_such_key() {
        let mut db = DbSlice::new(0);
        assert_eq!(ints(cmd(&mut db, &[b"FIELDEXPIRE", b"key", b"10", b"a", b"b"])), [-2, -2]);
    }

    #[test]
    fn fieldexpire_errors_and_wrong_type() {
        let mut db = DbSlice::new(0);
        // ttl 0 is below the 1..=kMaxExpireDeadlineSec range -> integer error.
        assert_eq!(
            err_of(cmd(&mut db, &[b"FIELDEXPIRE", b"key", b"0", b"a"])),
            "ERR value is not an integer or out of range"
        );
        assert_eq!(
            err_of(cmd(&mut db, &[b"FIELDEXPIRE", b"key", b"-1", b"a"])),
            "ERR value is not an integer or out of range"
        );
        assert_eq!(
            err_of(cmd(&mut db, &[b"FIELDEXPIRE", b"key", b"abc", b"a"])),
            "ERR value is not an integer or out of range"
        );
        // A wrong-type key is reported per field as -2, not as an error.
        str_of(&mut db, "str", "val");
        assert_eq!(ints(cmd(&mut db, &[b"FIELDEXPIRE", b"str", b"10", b"a", b"b"])), [-2, -2]);
        // FIELDTTL on the same key errors.
        assert!(err_of(cmd(&mut db, &[b"FIELDTTL", b"str", b"a"])).starts_with("WRONGTYPE"));
    }
}

