use crate::commands::{integer, ok, Command, OpContext, ShardPart, KeyRange, FLAG_FAST, FLAG_GLOBAL, FLAG_MULTI_KEY, FLAG_READONLY, FLAG_WRITE};
use crate::core::compact::CompactString;
use crate::core::PrimeValue;
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::parse_i64;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db::DbSlice;
    use crate::core::PrimeValue;

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
                b"EXISTS" => (exec_exists, 1, (1..argv.len()).collect()),
                b"PEXPIREAT" => (exec_pexpireat, 1, (1..2).collect()),
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
}

