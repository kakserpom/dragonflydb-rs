use crate::commands::{integer, Command, OpContext, ShardPart, KeyRange, FLAG_FAST, FLAG_GLOBAL, FLAG_MULTI_KEY, FLAG_READONLY, FLAG_WRITE};
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::parse_i64;

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

fn exec_exists(ctx: &mut OpContext) -> CmdResult {
    let mut count = 0i64;
    for &ki in ctx.owned_keys {
        if ctx.db.contains(&ctx.args[ki], ctx.now_ms) {
            count += 1;
        }
    }
    CmdResult::Ok(integer(count))
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
