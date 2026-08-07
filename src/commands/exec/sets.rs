use hashbrown::HashSet;

use crate::commands::exec::keys::glob_match;
use crate::commands::{
    Command, FLAG_DENYOOM, FLAG_FAST, FLAG_MULTI_KEY, FLAG_NO_AUTOJOURNAL, FLAG_NO_REDUCED,
    FLAG_READONLY, FLAG_WRITE, KeyRange, OpContext, ShardPart, integer,
};
use crate::core::PrimeValue;
use crate::core::compact::CompactString;
use crate::core::set::Set;
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::{parse_i64, parse_u64};

/// Ceiling for SADDEX member TTLs, shared with the reference
/// `kMaxExpireDeadlineSec` (dragonfly/src/server/common.h).
const MAX_EXPIRE_SEC: i64 = (1u64 << 28) as i64 - 1;

fn set_mut<'a>(ctx: &'a mut OpContext, key: &[u8]) -> Result<&'a mut Set, RespError> {
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Set(s)) => {
            s.prune_expired(ctx.now_ms);
            Ok(s)
        }
        Some(_) => Err(RespError::wrong_type()),
        None => Err(RespError::new("ERR no such key")),
    }
}

/// Lazily prune members of the set at `key` that expired before `now_ms`,
/// deleting the key when it is emptied. Wrong-type keys error; missing keys are
/// a no-op.
fn prune_set_key(ctx: &mut OpContext, key: &[u8]) -> Result<(), RespError> {
    let empty = match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Set(s)) => {
            s.prune_expired(ctx.now_ms);
            s.is_empty()
        }
        Some(_) => return Err(RespError::wrong_type()),
        None => false,
    };
    if empty {
        ctx.db.remove(key);
    }
    Ok(())
}

fn ensure_set<'a>(ctx: &'a mut OpContext, key: &[u8]) -> Result<&'a mut Set, RespError> {
    if ctx.db.find(key, ctx.now_ms).is_none() {
        ctx.db.insert(key, PrimeValue::Set(Set::new()));
    }
    set_mut(ctx, key)
}

fn exec_sadd(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let members: Vec<CompactString> = ctx.args[key_idx + 1..]
        .iter()
        .map(|m| CompactString::from_bytes(m))
        .collect();
    let s = match ensure_set(ctx, key) {
        Ok(s) => s,
        Err(e) => return CmdResult::Err(e),
    };
    let mut added = 0i64;
    for m in &members {
        if s.add(m.clone()) {
            added += 1;
        }
    }
    CmdResult::Ok(integer(added))
}

/// SADDEX key [KEEPTTL] ttl member [member ...]
///
/// Like SADD but attaches a per-member TTL in seconds (`1..=MAX_EXPIRE_SEC`).
/// Expired members are treated as absent: an SADDEX that refreshes an expired
/// member reports it as newly added.
fn exec_saddex(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let mut i = key_idx + 1;
    let keepttl = ctx
        .args
        .get(i)
        .is_some_and(|a| a.eq_ignore_ascii_case(b"KEEPTTL"));
    if keepttl {
        i += 1;
    }
    let ttl_sec = match ctx.args.get(i).and_then(|a| parse_i64(a)) {
        Some(v) if (1..=MAX_EXPIRE_SEC).contains(&v) => v as u64,
        _ => return CmdResult::Err(RespError::integer()),
    };
    i += 1;
    if i >= ctx.args.len() {
        return CmdResult::Err(RespError::new(
            "ERR wrong number of arguments for 'saddex' command",
        ));
    }
    if let Some(PrimeValue::Set(s)) = ctx.db.find_mut(key, ctx.now_ms) {
        s.prune_expired(ctx.now_ms);
    }
    let expire_ms = ctx.now_ms.saturating_add(ttl_sec.saturating_mul(1000));
    let members: Vec<Vec<u8>> = ctx.args[i..].to_vec();
    let s = match ensure_set(ctx, key) {
        Ok(s) => s,
        Err(e) => return CmdResult::Err(e),
    };
    let mut added = 0i64;
    for m in &members {
        if s.add_expirable(CompactString::from_bytes(m), expire_ms, keepttl) {
            added += 1;
        }
    }
    CmdResult::Ok(integer(added))
}

/// SSCAN key cursor [MATCH pattern] [COUNT count]
///
/// Small all-integer sets mirror the reference intset: every matching member is
/// returned at once with cursor 0 (COUNT is ignored). Other sets paginate in
/// sorted order, the cursor doubling as a position marker.
fn exec_sscan(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let Some(cursor) = parse_u64(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::new("ERR invalid cursor"));
    };
    let opts = &ctx.args[key_idx + 2..];
    if opts.len() > 4 {
        return CmdResult::Err(RespError::syntax());
    }
    let mut pattern: Option<&[u8]> = None;
    let mut count: usize = 10;
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
                Some(v) if v >= 1 => count = v as usize,
                _ => return CmdResult::Err(RespError::syntax()),
            }
            i += 2;
        } else {
            return CmdResult::Err(RespError::syntax());
        }
    }
    if let Err(e) = prune_set_key(ctx, key) {
        return CmdResult::Err(e);
    }
    let Some(PrimeValue::Set(s)) = ctx.db.find(key, ctx.now_ms) else {
        return CmdResult::Ok(scan_reply(0, vec![]));
    };
    let members = s.members();
    if members.is_empty() {
        return CmdResult::Ok(scan_reply(0, vec![]));
    }
    let all_int = members.iter().all(|m| parse_i64(m.as_bytes()).is_some());
    if all_int && members.len() <= 256 {
        let mut out = Vec::with_capacity(members.len());
        for m in &members {
            if pattern.is_none_or(|p| glob_match(p, m.as_bytes())) {
                out.push(RespValue::Bulk(m.as_bytes().to_vec()));
            }
        }
        return CmdResult::Ok(scan_reply(0, out));
    }
    let mut sorted: Vec<&CompactString> = members.iter().collect();
    sorted.sort();
    let start = (cursor as usize).min(sorted.len());
    let mut out = Vec::new();
    let mut pos = start;
    while pos < sorted.len() && out.len() < count {
        let m = sorted[pos];
        pos += 1;
        if pattern.is_none_or(|p| glob_match(p, m.as_bytes())) {
            out.push(RespValue::Bulk(m.as_bytes().to_vec()));
        }
    }
    let next = if pos >= sorted.len() {
        0u64
    } else {
        pos as u64
    };
    CmdResult::Ok(scan_reply(next, out))
}

/// `[cursor_bulk, [member ...]]` reply shape for SSCAN.
fn scan_reply(cursor: u64, members: Vec<RespValue>) -> RespValue {
    RespValue::Array(vec![
        RespValue::Bulk(crate::util::itoa(cursor as i64)),
        RespValue::Array(members),
    ])
}

fn exec_srem(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let members: Vec<CompactString> = ctx.args[key_idx + 1..]
        .iter()
        .map(|m| CompactString::from_bytes(m))
        .collect();
    let s = match set_mut(ctx, key) {
        Ok(s) => s,
        Err(e) => {
            if e.message.starts_with("WRONGTYPE") {
                return CmdResult::Err(e);
            }
            return CmdResult::Ok(integer(0));
        }
    };
    let mut removed = 0i64;
    for m in &members {
        if s.remove(m.as_bytes()) {
            removed += 1;
        }
    }
    if s.is_empty() {
        ctx.db.remove(key);
    }
    CmdResult::Ok(integer(removed))
}

fn exec_smembers(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    if let Err(e) = prune_set_key(ctx, key) {
        return CmdResult::Err(e);
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Set(s)) => {
            let out = s
                .members()
                .into_iter()
                .map(|m| RespValue::Bulk(m.as_bytes().to_vec()))
                .collect();
            CmdResult::Ok(RespValue::Array(out))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Array(vec![])),
    }
}

fn exec_sismember(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let member = &ctx.args[key_idx + 1];
    if let Err(e) = prune_set_key(ctx, key) {
        return CmdResult::Err(e);
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Set(s)) => CmdResult::Ok(integer(i64::from(s.contains(member)))),
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

fn exec_smismember(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    if let Err(e) = prune_set_key(ctx, key) {
        return CmdResult::Err(e);
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Set(s)) => {
            let out = ctx.args[key_idx + 1..]
                .iter()
                .map(|m| integer(i64::from(s.contains(m))))
                .collect();
            CmdResult::Ok(RespValue::Array(out))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Array(vec![
            integer(0);
            ctx.args.len() - key_idx - 1
        ])),
    }
}

fn exec_scard(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    if let Err(e) = prune_set_key(ctx, key) {
        return CmdResult::Err(e);
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Set(s)) => CmdResult::Ok(integer(s.len() as i64)),
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

fn exec_spop(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    if ctx.args.len() > key_idx + 2 {
        return CmdResult::Err(RespError::syntax());
    }
    let with_count = ctx.args.len() > key_idx + 1;
    let count = if with_count {
        let Some(c) = parse_i64(&ctx.args[key_idx + 1]) else {
            return CmdResult::Err(RespError::integer());
        };
        if c < 0 {
            return CmdResult::Err(RespError::new(
                "ERR value is out of range, must be positive",
            ));
        }
        c as usize
    } else {
        1
    };
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Set(s)) => {
            s.prune_expired(ctx.now_ms);
            if s.is_empty() {
                ctx.db.remove(key);
                return CmdResult::Ok(if with_count {
                    RespValue::Array(vec![])
                } else {
                    RespValue::Nil
                });
            }
            let mut out = Vec::new();
            for _ in 0..count {
                match s.pop_random() {
                    Some(m) => out.push(RespValue::Bulk(m.as_bytes().to_vec())),
                    None => break,
                }
            }
            if s.is_empty() {
                ctx.db.remove(key);
            }
            if with_count {
                CmdResult::Ok(RespValue::Array(out))
            } else {
                CmdResult::Ok(out.into_iter().next().unwrap_or(RespValue::Nil))
            }
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(if with_count {
            RespValue::Array(vec![])
        } else {
            RespValue::Nil
        }),
    }
}

fn exec_srandmember(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    if ctx.args.len() > key_idx + 2 {
        return CmdResult::Err(RespError::syntax());
    }
    let with_count = ctx.args.len() > key_idx + 1;
    let count = if with_count {
        match parse_i64(&ctx.args[key_idx + 1]) {
            Some(v) => v,
            None => return CmdResult::Err(RespError::integer()),
        }
    } else {
        0
    };
    if let Err(e) = prune_set_key(ctx, key) {
        return CmdResult::Err(e);
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Set(s)) => {
            if !with_count {
                return CmdResult::Ok(match s.rand_member() {
                    Some(m) => RespValue::Bulk(m.as_bytes().to_vec()),
                    None => RespValue::Nil,
                });
            }
            let members = if count < 0 {
                // Allow duplicates, exactly |count| elements.
                s.rand_members(count.unsigned_abs() as usize)
            } else {
                // Unique picks, at most the set size.
                s.rand_members_unique(count as usize)
            };
            CmdResult::Ok(RespValue::Array(
                members
                    .into_iter()
                    .map(|m| RespValue::Bulk(m.as_bytes().to_vec()))
                    .collect(),
            ))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(if with_count {
            RespValue::Array(vec![])
        } else {
            RespValue::Nil
        }),
    }
}

// ---------------------------------------------------------------------------
// Multi-key set ops: SINTER / SUNION / SDIFF, their STORE variants, SINTERCARD
// ---------------------------------------------------------------------------

/// Intersection of this shard's owned source sets (excluding the first key when
/// `skip_first`, i.e. the STORE destination). A missing source behaves as an
/// empty set, so any missing key empties the intersection; a wrong-type source
/// takes precedence, mirroring the reference `OpInter`.
fn inter_of(ctx: &mut OpContext, skip_first: bool) -> Result<HashSet<CompactString>, RespError> {
    let mut acc: Option<HashSet<CompactString>> = None;
    let mut any_missing = false;
    for &ki in ctx.owned_keys {
        if skip_first && ki == ctx.first_key_idx {
            continue;
        }
        let key = &ctx.args[ki];
        prune_set_key(ctx, key)?;
        match ctx.db.find(key, ctx.now_ms) {
            Some(PrimeValue::Set(s)) => {
                let members: HashSet<CompactString> = s.members().into_iter().collect();
                match &mut acc {
                    None => acc = Some(members),
                    Some(a) => a.retain(|m| members.contains(m)),
                }
            }
            Some(_) => return Err(RespError::wrong_type()),
            None => any_missing = true,
        }
    }
    if any_missing {
        return Ok(HashSet::new());
    }
    Ok(acc.unwrap_or_default())
}

/// Union of this shard's owned source sets (excluding the first key when
/// `skip_first`).
fn union_of(ctx: &mut OpContext, skip_first: bool) -> Result<HashSet<CompactString>, RespError> {
    let mut union: HashSet<CompactString> = HashSet::new();
    for &ki in ctx.owned_keys {
        if skip_first && ki == ctx.first_key_idx {
            continue;
        }
        let key = &ctx.args[ki];
        prune_set_key(ctx, key)?;
        match ctx.db.find(key, ctx.now_ms) {
            Some(PrimeValue::Set(s)) => {
                for m in s.members() {
                    union.insert(m);
                }
            }
            Some(_) => return Err(RespError::wrong_type()),
            None => {}
        }
    }
    Ok(union)
}

/// SDIFF of this shard's owned sets. The base is the set at `base_idx`
/// (`args[1]` for SDIFF, `args[2]` for SDIFFSTORE); the destination key
/// (`ctx.first_key_idx`) is never a source. Shards that do not own the base
/// contribute the union of their remaining sources.
fn diff_of(ctx: &mut OpContext, base_idx: usize) -> Result<HashSet<CompactString>, RespError> {
    if !ctx.owned_keys.contains(&base_idx) {
        return union_of(ctx, true);
    }
    let base_key = &ctx.args[base_idx];
    prune_set_key(ctx, base_key)?;
    let mut base: HashSet<CompactString> = match ctx.db.find(base_key, ctx.now_ms) {
        Some(PrimeValue::Set(s)) => s.members().into_iter().collect(),
        Some(_) => return Err(RespError::wrong_type()),
        None => return Ok(HashSet::new()),
    };
    for &ki in ctx.owned_keys {
        if ki == base_idx || ki == ctx.first_key_idx {
            continue;
        }
        let key = &ctx.args[ki];
        prune_set_key(ctx, key)?;
        match ctx.db.find(key, ctx.now_ms) {
            Some(PrimeValue::Set(s)) => {
                for m in s.members() {
                    base.remove(&m);
                }
            }
            Some(_) => return Err(RespError::wrong_type()),
            None => {}
        }
    }
    Ok(base)
}

fn array_of(set: HashSet<CompactString>) -> RespValue {
    RespValue::Array(
        set.into_iter()
            .map(|m| RespValue::Bulk(m.as_bytes().to_vec()))
            .collect(),
    )
}

/// Single-shard STORE fast path: write the result set to the destination on
/// this shard (removing it when empty) and reply with the member count.
fn store_single(ctx: &mut OpContext, result: HashSet<CompactString>) -> CmdResult {
    let len = result.len() as i64;
    let dest = &ctx.args[ctx.first_key_idx];
    if result.is_empty() {
        ctx.db.remove(dest);
    } else {
        let mut set = Set::new();
        set.extend(result.into_iter());
        ctx.db.clear_expiry(dest);
        ctx.db.insert(dest, PrimeValue::Set(set));
    }
    CmdResult::Ok(integer(len))
}

/// Wrap a merged member set for deferred storage: `None` when empty (deletes).
fn set_value(members: HashSet<CompactString>) -> Option<PrimeValue> {
    if members.is_empty() {
        None
    } else {
        let mut set = Set::new();
        set.extend(members.into_iter());
        Some(PrimeValue::Set(set))
    }
}

fn exec_sinter(ctx: &mut OpContext) -> CmdResult {
    match inter_of(ctx, false) {
        Ok(s) => CmdResult::Ok(array_of(s)),
        Err(e) => CmdResult::Err(e),
    }
}

fn exec_sunion(ctx: &mut OpContext) -> CmdResult {
    match union_of(ctx, false) {
        Ok(s) => CmdResult::Ok(array_of(s)),
        Err(e) => CmdResult::Err(e),
    }
}

fn exec_sdiff(ctx: &mut OpContext) -> CmdResult {
    match diff_of(ctx, ctx.first_key_idx) {
        Ok(s) => CmdResult::Ok(array_of(s)),
        Err(e) => CmdResult::Err(e),
    }
}

fn exec_sinterstore(ctx: &mut OpContext) -> CmdResult {
    let single = ctx.owned_keys.len() == ctx.args.len() - ctx.first_key_idx;
    match inter_of(ctx, true) {
        Ok(s) if single => store_single(ctx, s),
        Ok(s) => CmdResult::Ok(array_of(s)),
        Err(e) => CmdResult::Err(e),
    }
}

fn exec_sunionstore(ctx: &mut OpContext) -> CmdResult {
    let single = ctx.owned_keys.len() == ctx.args.len() - ctx.first_key_idx;
    match union_of(ctx, true) {
        Ok(s) if single => store_single(ctx, s),
        Ok(s) => CmdResult::Ok(array_of(s)),
        Err(e) => CmdResult::Err(e),
    }
}

fn exec_sdiffstore(ctx: &mut OpContext) -> CmdResult {
    let single = ctx.owned_keys.len() == ctx.args.len() - ctx.first_key_idx;
    match diff_of(ctx, ctx.first_key_idx + 1) {
        Ok(s) if single => store_single(ctx, s),
        Ok(s) => CmdResult::Ok(array_of(s)),
        Err(e) => CmdResult::Err(e),
    }
}

fn exec_sintercard(ctx: &mut OpContext) -> CmdResult {
    let numkeys = match parse_i64(&ctx.args[1]) {
        Some(v) if v >= 0 => v as usize,
        _ => return CmdResult::Err(RespError::integer()),
    };
    if numkeys == 0 {
        return CmdResult::Err(RespError::new(
            "ERR at least 1 input key is needed for this command",
        ));
    }
    // Real keys occupy args[2..key_end); anything after is the optional LIMIT
    // clause. Validated on every shard so errors surface regardless of routing.
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
            _ => return CmdResult::Err(RespError::new("ERR limit can't be negative")),
        }
    }

    let mut acc: Option<HashSet<CompactString>> = None;
    let mut any_missing = false;
    for &ki in ctx.owned_keys {
        if ki < 2 || ki >= key_end {
            continue; // phantom key (LIMIT token / limit value)
        }
        let key = &ctx.args[ki];
        if let Err(e) = prune_set_key(ctx, key) {
            return CmdResult::Err(e);
        }
        match ctx.db.find(key, ctx.now_ms) {
            Some(PrimeValue::Set(s)) => {
                let members: HashSet<CompactString> = s.members().into_iter().collect();
                match &mut acc {
                    None => acc = Some(members),
                    Some(a) => a.retain(|m| members.contains(m)),
                }
            }
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => any_missing = true,
        }
    }
    let members = if any_missing {
        HashSet::new()
    } else {
        acc.unwrap_or_default()
    };
    let count = if limit > 0 {
        (members.len() as i64).min(limit)
    } else {
        members.len() as i64
    };

    let single = ctx.owned_keys.len() == ctx.args.len() - 2;
    if single {
        return CmdResult::Ok(integer(count));
    }
    let has_real = ctx.owned_keys.iter().any(|&ki| ki >= 2 && ki < key_end);
    if !has_real {
        return CmdResult::Ok(RespValue::Nil);
    }
    CmdResult::Ok(array_of(members))
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
        _ => Err(RespError::new("ERR internal: unexpected set shard result")),
    }
}

fn merge_sinter(parts: &[ShardPart], _args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    let mut acc: Option<HashSet<CompactString>> = None;
    for p in parts {
        let members = match parts_to_members(p) {
            Ok(v) => v,
            Err(e) => return CmdResult::Err(e),
        };
        if members.is_empty() {
            return CmdResult::Ok(RespValue::Array(vec![]));
        }
        match &mut acc {
            None => acc = Some(members),
            Some(a) => a.retain(|m| members.contains(m)),
        }
    }
    CmdResult::Ok(array_of(acc.unwrap_or_default()))
}

fn merge_sunion(parts: &[ShardPart], _args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    let mut acc: HashSet<CompactString> = HashSet::new();
    for p in parts {
        let members = match parts_to_members(p) {
            Ok(v) => v,
            Err(e) => return CmdResult::Err(e),
        };
        acc.extend(members);
    }
    CmdResult::Ok(array_of(acc))
}

fn merge_sdiff(parts: &[ShardPart], _args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    let first_key_idx = keys[0];
    let mut base: HashSet<CompactString> = HashSet::new();
    let mut have_base = false;
    for p in parts {
        if p.owned_key_idxs.contains(&first_key_idx) {
            base = match parts_to_members(p) {
                Ok(v) => v,
                Err(e) => return CmdResult::Err(e),
            };
            have_base = true;
        }
    }
    if !have_base {
        return CmdResult::Err(RespError::new("ERR internal: SDIFF base shard missing"));
    }
    for p in parts {
        if p.owned_key_idxs.contains(&first_key_idx) {
            continue;
        }
        let members = match parts_to_members(p) {
            Ok(v) => v,
            Err(e) => return CmdResult::Err(e),
        };
        base.retain(|m| !members.contains(m));
    }
    CmdResult::Ok(array_of(base))
}

fn merge_sinterstore(
    parts: &[ShardPart],
    args: &[Vec<u8>],
    keys: &[usize],
    _now: u64,
) -> CmdResult {
    let dest = &args[keys[0]];
    let mut acc: Option<HashSet<CompactString>> = None;
    for p in parts {
        // A shard holding only the destination key is not a source shard.
        if p.owned_key_idxs.iter().all(|&k| k == keys[0]) {
            continue;
        }
        let members = match parts_to_members(p) {
            Ok(v) => v,
            Err(e) => return CmdResult::Err(e),
        };
        if members.is_empty() {
            return CmdResult::deferred_store(dest.clone(), None, integer(0));
        }
        match &mut acc {
            None => acc = Some(members),
            Some(a) => a.retain(|m| members.contains(m)),
        }
    }
    let members = acc.unwrap_or_default();
    let len = members.len() as i64;
    CmdResult::deferred_store(dest.clone(), set_value(members), integer(len))
}

fn merge_sunionstore(
    parts: &[ShardPart],
    args: &[Vec<u8>],
    keys: &[usize],
    _now: u64,
) -> CmdResult {
    let dest = &args[keys[0]];
    let mut acc: HashSet<CompactString> = HashSet::new();
    for p in parts {
        let members = match parts_to_members(p) {
            Ok(v) => v,
            Err(e) => return CmdResult::Err(e),
        };
        acc.extend(members);
    }
    let len = acc.len() as i64;
    CmdResult::deferred_store(dest.clone(), set_value(acc), integer(len))
}

fn merge_sdiffstore(parts: &[ShardPart], args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    let dest = &args[keys[0]];
    let base_idx = keys[1];
    let mut base: HashSet<CompactString> = HashSet::new();
    let mut have_base = false;
    for p in parts {
        if p.owned_key_idxs.contains(&base_idx) {
            base = match parts_to_members(p) {
                Ok(v) => v,
                Err(e) => return CmdResult::Err(e),
            };
            have_base = true;
            break;
        }
    }
    if !have_base {
        return CmdResult::Err(RespError::new(
            "ERR internal: SDIFFSTORE base shard missing",
        ));
    }
    for p in parts {
        if p.owned_key_idxs.contains(&base_idx) {
            continue;
        }
        let members = match parts_to_members(p) {
            Ok(v) => v,
            Err(e) => return CmdResult::Err(e),
        };
        base.retain(|m| !members.contains(m));
    }
    let len = base.len() as i64;
    CmdResult::deferred_store(dest.clone(), set_value(base), integer(len))
}

fn merge_sintercard(
    parts: &[ShardPart],
    args: &[Vec<u8>],
    _keys: &[usize],
    _now: u64,
) -> CmdResult {
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
        limit = args
            .get(key_end + 1)
            .and_then(|a| parse_i64(a))
            .unwrap_or(0);
    }
    let mut acc: Option<HashSet<CompactString>> = None;
    for p in parts {
        if let CmdResult::Err(e) = &p.result {
            return CmdResult::Err(e.clone());
        }
        if !p.owned_key_idxs.iter().any(|&k| k >= 2 && k < key_end) {
            continue; // shard held only phantom keys (LIMIT token / value)
        }
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
    let n = acc.map_or(0, |a| a.len()) as i64;
    CmdResult::Ok(integer(if limit > 0 { n.min(limit) } else { n }))
}

/// Convert a partial member-array reply to a member set. `Nil` (a missing src)
/// yields `None`.
fn parts_members_or_nil(p: &ShardPart) -> Result<Option<HashSet<CompactString>>, RespError> {
    match &p.result {
        CmdResult::Ok(RespValue::Array(arr)) => {
            let mut set = HashSet::with_capacity(arr.len());
            for v in arr {
                if let RespValue::Bulk(b) = v {
                    set.insert(CompactString::from_bytes(b));
                }
            }
            Ok(Some(set))
        }
        CmdResult::Ok(RespValue::Nil) => Ok(None),
        CmdResult::Err(e) => Err(e.clone()),
        o => Err(RespError::new(format!(
            "ERR internal: SMOVE unexpected partial {:?}",
            o.clone().into_resp_value()
        ))),
    }
}

/// SMOVE src dest member.
///
/// Single-shard execution performs the move in place. Across shards each shard
/// reports the member set of the key(s) it owns; the merge reconstructs the
/// post-move sets and issues a deferred store for both keys.
fn exec_smove(ctx: &mut OpContext) -> CmdResult {
    let src_idx = ctx.first_key_idx;
    let dest_idx = src_idx + 1;
    let member = &ctx.args[ctx.args.len() - 1];
    let owns_src = ctx.owned_keys.contains(&src_idx);
    let owns_dest = ctx.owned_keys.contains(&dest_idx);

    if owns_src && owns_dest {
        // Single-shard fast path: both keys live here. Type-check both keys
        // first; a wrong-type key takes precedence over a missing member.
        if let Err(e) = prune_set_key(ctx, &ctx.args[src_idx]) {
            return CmdResult::Err(e);
        }
        if let Err(e) = prune_set_key(ctx, &ctx.args[dest_idx]) {
            return CmdResult::Err(e);
        }
        let src = match ctx.db.find(&ctx.args[src_idx], ctx.now_ms) {
            Some(PrimeValue::Set(s)) => Some(s.clone()),
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => None,
        };
        let dest = match ctx.db.find(&ctx.args[dest_idx], ctx.now_ms) {
            Some(PrimeValue::Set(s)) => Some(s.clone()),
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => None,
        };
        let Some(src) = src else {
            return CmdResult::Ok(integer(0));
        };
        if !src.contains(member) {
            return CmdResult::Ok(integer(0));
        }
        if src_idx == dest_idx {
            return CmdResult::Ok(integer(1)); // noop
        }
        let mut src = src;
        src.remove(member);
        let mut dest = dest.unwrap_or_else(Set::new);
        dest.add(CompactString::from_bytes(member));
        if src.is_empty() {
            ctx.db.remove(&ctx.args[src_idx]);
        } else {
            ctx.db.insert(&ctx.args[src_idx], PrimeValue::Set(src));
        }
        ctx.db.insert(&ctx.args[dest_idx], PrimeValue::Set(dest));
        return CmdResult::Ok(integer(1));
    }

    // Multi-shard partial: report the owned key's member set. Missing src is
    // `Nil`, a missing dest is an empty set.
    let mut result = CmdResult::Ok(RespValue::Nil);
    if owns_src {
        if let Err(e) = prune_set_key(ctx, &ctx.args[src_idx]) {
            return CmdResult::Err(e);
        }
        match ctx.db.find(&ctx.args[src_idx], ctx.now_ms) {
            Some(PrimeValue::Set(s)) => {
                result = CmdResult::Ok(RespValue::Array(
                    s.members()
                        .into_iter()
                        .map(|m| RespValue::Bulk(m.as_bytes().to_vec()))
                        .collect(),
                ));
            }
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => result = CmdResult::Ok(RespValue::Nil),
        }
    }
    if owns_dest {
        if let Err(e) = prune_set_key(ctx, &ctx.args[dest_idx]) {
            return CmdResult::Err(e);
        }
        match ctx.db.find(&ctx.args[dest_idx], ctx.now_ms) {
            Some(PrimeValue::Set(s)) => {
                result = CmdResult::Ok(RespValue::Array(
                    s.members()
                        .into_iter()
                        .map(|m| RespValue::Bulk(m.as_bytes().to_vec()))
                        .collect(),
                ));
            }
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => result = CmdResult::Ok(RespValue::Array(vec![])),
        }
    }
    result
}

fn merge_smove(parts: &[ShardPart], args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    let src_idx = keys[0];
    let dest_idx = keys[1];
    let member = &args[args.len() - 1];
    let mut src: Option<HashSet<CompactString>> = None;
    let mut dest: HashSet<CompactString> = HashSet::new();
    for p in parts {
        if p.owned_key_idxs.contains(&src_idx) {
            match parts_members_or_nil(p) {
                Ok(v) => src = v,
                Err(e) => return CmdResult::Err(e),
            }
        }
        if p.owned_key_idxs.contains(&dest_idx) {
            match parts_members_or_nil(p) {
                Ok(Some(v)) => dest = v,
                Ok(None) => dest = HashSet::new(),
                Err(e) => return CmdResult::Err(e),
            }
        }
    }
    let Some(src) = src else {
        return CmdResult::Ok(integer(0));
    };
    if !src.contains(member.as_slice()) {
        return CmdResult::Ok(integer(0));
    }
    let mut new_src = src;
    new_src.remove(member.as_slice());
    dest.insert(CompactString::from_bytes(member));
    let mut stores: Vec<crate::error::DeferredStoreItem> = Vec::with_capacity(2);
    if new_src.is_empty() {
        stores.push((args[src_idx].clone(), None, None, false));
    } else {
        let mut set = Set::new();
        set.extend(new_src.into_iter());
        stores.push((
            args[src_idx].clone(),
            Some(PrimeValue::Set(set)),
            None,
            false,
        ));
    }
    let mut dest_set = Set::new();
    dest_set.extend(dest.into_iter());
    stores.push((
        args[dest_idx].clone(),
        Some(PrimeValue::Set(dest_set)),
        None,
        false,
    ));
    CmdResult::deferred_stores(stores, integer(1))
}

pub static CMD_SADD: Command = Command {
    name: "SADD",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_sadd,
    merge: None,
};
pub static CMD_SADDEX: Command = Command {
    name: "SADDEX",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_saddex,
    merge: None,
};
pub static CMD_SREM: Command = Command {
    name: "SREM",
    arity: -3,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_srem,
    merge: None,
};
pub static CMD_SMEMBERS: Command = Command {
    name: "SMEMBERS",
    arity: 2,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_smembers,
    merge: None,
};
pub static CMD_SISMEMBER: Command = Command {
    name: "SISMEMBER",
    arity: 3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_sismember,
    merge: None,
};
pub static CMD_SMISMEMBER: Command = Command {
    name: "SMISMEMBER",
    arity: -3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_smismember,
    merge: None,
};
pub static CMD_SCARD: Command = Command {
    name: "SCARD",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_scard,
    merge: None,
};
pub static CMD_SPOP: Command = Command {
    name: "SPOP",
    arity: -2,
    flags: FLAG_WRITE | FLAG_FAST | FLAG_NO_AUTOJOURNAL,
    key_range: KeyRange::ONE,
    exec: exec_spop,
    merge: None,
};
pub static CMD_SRANDMEMBER: Command = Command {
    name: "SRANDMEMBER",
    arity: -2,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_srandmember,
    merge: None,
};
pub static CMD_SSCAN: Command = Command {
    name: "SSCAN",
    arity: -3,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_sscan,
    merge: None,
};
pub static CMD_SINTER: Command = Command {
    name: "SINTER",
    arity: -2,
    flags: FLAG_READONLY | FLAG_MULTI_KEY,
    key_range: KeyRange::ALL,
    exec: exec_sinter,
    merge: Some(merge_sinter),
};
pub static CMD_SUNION: Command = Command {
    name: "SUNION",
    arity: -2,
    flags: FLAG_READONLY | FLAG_MULTI_KEY,
    key_range: KeyRange::ALL,
    exec: exec_sunion,
    merge: Some(merge_sunion),
};
pub static CMD_SDIFF: Command = Command {
    name: "SDIFF",
    arity: -2,
    flags: FLAG_READONLY | FLAG_MULTI_KEY,
    key_range: KeyRange::ALL,
    exec: exec_sdiff,
    merge: Some(merge_sdiff),
};
pub static CMD_SINTERSTORE: Command = Command {
    name: "SINTERSTORE",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_MULTI_KEY | FLAG_NO_REDUCED,
    key_range: KeyRange::ALL,
    exec: exec_sinterstore,
    merge: Some(merge_sinterstore),
};
pub static CMD_SUNIONSTORE: Command = Command {
    name: "SUNIONSTORE",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_MULTI_KEY | FLAG_NO_REDUCED,
    key_range: KeyRange::ALL,
    exec: exec_sunionstore,
    merge: Some(merge_sunionstore),
};
pub static CMD_SDIFFSTORE: Command = Command {
    name: "SDIFFSTORE",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_MULTI_KEY | FLAG_NO_REDUCED,
    key_range: KeyRange::ALL,
    exec: exec_sdiffstore,
    merge: Some(merge_sdiffstore),
};
pub static CMD_SINTERCARD: Command = Command {
    name: "SINTERCARD",
    arity: -3,
    flags: FLAG_READONLY | FLAG_MULTI_KEY,
    key_range: KeyRange {
        first: 2,
        last: 0,
        step: 1,
    },
    exec: exec_sintercard,
    merge: Some(merge_sintercard),
};
pub static CMD_SMOVE: Command = Command {
    name: "SMOVE",
    arity: 4,
    flags: FLAG_WRITE | FLAG_FAST | FLAG_MULTI_KEY,
    key_range: KeyRange::TWO,
    exec: exec_smove,
    merge: Some(merge_smove),
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db::DbSlice;

    fn set_of(db: &mut DbSlice, key: &str, members: &[&str]) {
        let mut s = Set::new();
        for &m in members {
            s.add(CompactString::from(m));
        }
        db.insert(key.as_bytes(), PrimeValue::Set(s));
    }

    fn str_of(db: &mut DbSlice, key: &str, value: &str) {
        db.insert(key.as_bytes(), PrimeValue::Str(CompactString::from(value)));
    }

    fn members_of(db: &mut DbSlice, key: &str) -> Vec<String> {
        match db.find(key.as_bytes(), 0) {
            Some(PrimeValue::Set(s)) => {
                let mut v: Vec<String> = s.members().into_iter().map(|m| m.to_string()).collect();
                v.sort();
                v
            }
            _ => panic!("expected set at {key}"),
        }
    }

    fn int(r: CmdResult) -> i64 {
        match r {
            CmdResult::Ok(RespValue::Integer(v)) => v,
            o => panic!("expected integer, got {:?}", o.into_resp_value()),
        }
    }

    fn arr(r: CmdResult) -> Vec<String> {
        let mut v: Vec<String> = match r {
            CmdResult::Ok(RespValue::Array(v)) => v
                .into_iter()
                .map(|x| match x {
                    RespValue::Bulk(b) => String::from_utf8_lossy(&b).into_owned(),
                    _ => panic!("unexpected element {x:?}"),
                })
                .collect(),
            o => panic!("expected array, got {:?}", o.into_resp_value()),
        };
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
    fn dispatch(db: &mut DbSlice, argv: &[Vec<u8>]) -> CmdResult {
        dispatch_at(db, 0, argv)
    }

    fn dispatch_at(db: &mut DbSlice, now_ms: u64, argv: &[Vec<u8>]) -> CmdResult {
        let (exec, first_key_idx, owned): (fn(&mut OpContext) -> CmdResult, usize, Vec<usize>) =
            match argv[0].as_slice() {
                b"SDIFF" => (exec_sdiff, 1, (1..argv.len()).collect()),
                b"SINTER" => (exec_sinter, 1, (1..argv.len()).collect()),
                b"SUNION" => (exec_sunion, 1, (1..argv.len()).collect()),
                b"SINTERSTORE" => (exec_sinterstore, 1, (1..argv.len()).collect()),
                b"SUNIONSTORE" => (exec_sunionstore, 1, (1..argv.len()).collect()),
                b"SDIFFSTORE" => (exec_sdiffstore, 1, (1..argv.len()).collect()),
                b"SINTERCARD" => (exec_sintercard, 2, (2..argv.len()).collect()),
                b"SMOVE" => (exec_smove, 1, (1..3).collect()),
                b"SADDEX" => (exec_saddex, 1, (1..2).collect()),
                b"SSCAN" => (exec_sscan, 1, (1..2).collect()),
                b"SISMEMBER" => (exec_sismember, 1, (1..2).collect()),
                b"SMISMEMBER" => (exec_smismember, 1, (1..2).collect()),
                b"SCARD" => (exec_scard, 1, (1..2).collect()),
                b"SPOP" => (exec_spop, 1, (1..2).collect()),
                b"SRANDMEMBER" => (exec_srandmember, 1, (1..2).collect()),
                b"SMEMBERS" => (exec_smembers, 1, (1..2).collect()),
                b"SREM" => (exec_srem, 1, (1..2).collect()),
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

    macro_rules! run {
        ($db:expr, $($arg:expr),+) => {
            dispatch($db, &[$(($arg).to_vec()),+])
        };
    }

    macro_rules! run_at {
        ($db:expr, $now:expr, $($arg:expr),+) => {
            dispatch_at($db, $now, &[$(($arg).to_vec()),+])
        };
    }

    fn bulk(s: &[u8]) -> RespValue {
        RespValue::Bulk(s.to_vec())
    }

    fn deferred_set(r: CmdResult) -> (Vec<u8>, Vec<String>, i64) {
        match r {
            CmdResult::DeferredStore { key, value, reply } => {
                let members = match value {
                    Some(PrimeValue::Set(s)) => {
                        let mut v: Vec<String> =
                            s.members().into_iter().map(|m| m.to_string()).collect();
                        v.sort();
                        v
                    }
                    None => vec![],
                    _ => panic!("expected set value"),
                };
                (key, members, int(CmdResult::Ok(reply)))
            }
            o => panic!("expected DeferredStore, got {:?}", o.into_resp_value()),
        }
    }

    /// Extract `DeferredStores` as `(key, members)` pairs.
    fn deferred_stores(r: CmdResult) -> (Vec<(Vec<u8>, Vec<String>)>, i64) {
        match r {
            CmdResult::DeferredStores { stores, reply } => {
                let mut out = Vec::with_capacity(stores.len());
                for (key, value, _expiry, _sticky) in stores {
                    let members = match value {
                        Some(PrimeValue::Set(s)) => {
                            let mut v: Vec<String> =
                                s.members().into_iter().map(|m| m.to_string()).collect();
                            v.sort();
                            v
                        }
                        None => vec![],
                        _ => panic!("expected set value"),
                    };
                    out.push((key, members));
                }
                (out, int(CmdResult::Ok(reply)))
            }
            o => panic!("expected DeferredStores, got {:?}", o.into_resp_value()),
        }
    }

    #[test]
    fn s_union_store() {
        let mut db = DbSlice::new(0);
        set_of(&mut db, "b", &["1", "2", "3"]);
        set_of(&mut db, "c", &["10", "11"]);
        str_of(&mut db, "a", "foo");

        assert_eq!(5, int(run!(&mut db, b"SUNIONSTORE", b"a", b"b", b"c")));
        assert_eq!(members_of(&mut db, "a"), ["1", "10", "11", "2", "3"]);
    }

    // SUNIONSTORE overwrites a value including resetting its expiration.
    #[test]
    fn s_union_store_expiration() {
        let mut db = DbSlice::new(0);
        set_of(&mut db, "s1", &["a", "b"]);
        set_of(&mut db, "s2", &["c", "d"]);
        str_of(&mut db, "target", "some-value");
        db.set_expiry(b"target", 1010, 0);
        assert_eq!(db.ttl_ms(b"target", 0), 1010);

        assert_eq!(
            4,
            int(run!(&mut db, b"SUNIONSTORE", b"target", b"s1", b"s2"))
        );
        assert_eq!(members_of(&mut db, "target"), ["a", "b", "c", "d"]);
        assert_eq!(db.ttl_ms(b"target", 0), -1);
    }

    #[test]
    fn s_diff() {
        let mut db = DbSlice::new(0);
        set_of(&mut db, "b", &["1", "2", "3"]);
        set_of(&mut db, "c", &["10", "11"]);
        str_of(&mut db, "a", "foo");

        assert_eq!(arr(run!(&mut db, b"SDIFF", b"b", b"c")), ["1", "2", "3"]);
        assert_eq!(3, int(run!(&mut db, b"SDIFFSTORE", b"a", b"b", b"c")));
        assert_eq!(members_of(&mut db, "a"), ["1", "2", "3"]);

        str_of(&mut db, "str", "foo");
        assert!(err(run!(&mut db, b"SDIFF", b"b", b"str")).starts_with("WRONGTYPE"));

        set_of(&mut db, "bar", &["x", "a", "b", "c"]);
        set_of(&mut db, "foo", &["c"]);
        set_of(&mut db, "car", &["a", "d"]);
        assert_eq!(
            2,
            int(run!(&mut db, b"SDIFFSTORE", b"tar", b"bar", b"foo", b"car"))
        );
        assert_eq!(members_of(&mut db, "tar"), ["b", "x"]);
    }

    #[test]
    fn s_inter() {
        let mut db = DbSlice::new(0);
        set_of(&mut db, "a", &["1", "2", "3", "4"]);
        set_of(&mut db, "b", &["3", "5", "6", "2"]);

        assert_eq!(2, int(run!(&mut db, b"SINTERSTORE", b"d", b"a", b"b")));
        assert_eq!(members_of(&mut db, "d"), ["2", "3"]);

        // Wrong-type source surfaces the error.
        str_of(&mut db, "y", "");
        assert!(err(run!(&mut db, b"SINTER", b"x", b"y")).starts_with("WRONGTYPE"));

        // All-missing sources produce 0 and leave no destination.
        assert_eq!(0, int(run!(&mut db, b"SINTERSTORE", b"none1", b"none2")));
        assert!(db.find(b"none1", 0).is_none());
    }

    // Store variants must overwrite a non-set destination with a set.
    #[test]
    fn s_store_overwrites_non_set() {
        let mut db = DbSlice::new(0);
        set_of(&mut db, "src", &["x", "y", "z"]);
        str_of(&mut db, "dest1", "foo");
        assert_eq!(3, int(run!(&mut db, b"SUNIONSTORE", b"dest1", b"src")));
        assert!(matches!(db.find(b"dest1", 0), Some(PrimeValue::Set(_))));

        str_of(&mut db, "dest2", "foo");
        assert_eq!(3, int(run!(&mut db, b"SDIFFSTORE", b"dest2", b"src")));
        assert!(matches!(db.find(b"dest2", 0), Some(PrimeValue::Set(_))));

        set_of(&mut db, "src2", &["x", "y"]);
        str_of(&mut db, "dest3", "foo");
        assert_eq!(
            2,
            int(run!(&mut db, b"SINTERSTORE", b"dest3", b"src", b"src2"))
        );
        assert!(matches!(db.find(b"dest3", 0), Some(PrimeValue::Set(_))));
    }

    #[test]
    fn s_inter_card() {
        let mut db = DbSlice::new(0);
        set_of(&mut db, "s1", &["2", "b", "1", "a"]);
        set_of(&mut db, "s2", &["3", "c", "2", "b"]);
        set_of(&mut db, "s3", &["2", "b", "3", "c"]);

        assert_eq!(2, int(run!(&mut db, b"SINTERCARD", b"2", b"s1", b"s2")));
        assert_eq!(0, int(run!(&mut db, b"SINTERCARD", b"2", b"s1", b"s4")));
        assert_eq!(
            2,
            int(run!(
                &mut db,
                b"SINTERCARD",
                b"2",
                b"s2",
                b"s3",
                b"LIMIT",
                b"2"
            ))
        );
        assert_eq!(4, int(run!(&mut db, b"SINTERCARD", b"1", b"s1")));

        assert!(err(run!(&mut db, b"SINTERCARD", b"a", b"s1", b"s2")).contains("not an integer"));
        assert!(
            err(run!(&mut db, b"SINTERCARD", b"2", b"s1", b"s2", b"LIMIT"))
                .contains("syntax error")
        );
        assert!(
            err(run!(
                &mut db,
                b"SINTERCARD",
                b"2",
                b"s1",
                b"s2",
                b"LIMIT",
                b"a"
            ))
            .contains("limit can't be negative")
        );
        assert!(
            err(run!(
                &mut db,
                b"SINTERCARD",
                b"2",
                b"s1",
                b"s2",
                b"LIMIT",
                b"-1"
            ))
            .contains("limit can't be negative")
        );
        assert!(err(run!(&mut db, b"SINTERCARD", b"2", b"s1")).contains("syntax error"));
        assert!(
            err(run!(&mut db, b"SINTERCARD", b"0", b"LIMIT", b"0"))
                .contains("at least 1 input key")
        );
        assert!(err(run!(&mut db, b"SINTERCARD", b"-1", b"s1")).contains("not an integer"));
    }

    #[test]
    fn merge_sinterstore_skips_dest_only_shard() {
        let args = vec![
            b"SINTERSTORE".to_vec(),
            b"d".to_vec(),
            b"a".to_vec(),
            b"b".to_vec(),
        ];
        let keys = [1usize, 2, 3];
        // Shard 0 owns only the destination; shards 1/2 own the sources.
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: vec![1],
                result: CmdResult::Ok(RespValue::Array(vec![])),
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: vec![2],
                result: CmdResult::Ok(RespValue::Array(vec![bulk(b"2"), bulk(b"3")])),
            },
            ShardPart {
                shard: 2,
                owned_key_idxs: vec![3],
                result: CmdResult::Ok(RespValue::Array(vec![bulk(b"2"), bulk(b"5")])),
            },
        ];
        let (key, members, reply) = deferred_set(merge_sinterstore(&parts, &args, &keys, 0));
        assert_eq!(key, b"d");
        assert_eq!(reply, 1);
        assert_eq!(members, ["2"]);
    }

    #[test]
    fn merge_sinterstore_empty_removes_dest() {
        let args = vec![
            b"SINTERSTORE".to_vec(),
            b"d".to_vec(),
            b"a".to_vec(),
            b"b".to_vec(),
        ];
        let keys = [1usize, 2, 3];
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: vec![1],
                result: CmdResult::Ok(RespValue::Array(vec![])),
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: vec![2],
                result: CmdResult::Ok(RespValue::Array(vec![bulk(b"2")])),
            },
            ShardPart {
                shard: 2,
                owned_key_idxs: vec![3],
                result: CmdResult::Ok(RespValue::Array(vec![])),
            },
        ];
        let (key, members, reply) = deferred_set(merge_sinterstore(&parts, &args, &keys, 0));
        assert_eq!(key, b"d");
        assert_eq!(reply, 0);
        assert!(members.is_empty());
    }

    #[test]
    fn merge_sunionstore_basic() {
        let args = vec![
            b"SUNIONSTORE".to_vec(),
            b"d".to_vec(),
            b"a".to_vec(),
            b"b".to_vec(),
        ];
        let keys = [1usize, 2, 3];
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: vec![1],
                result: CmdResult::Ok(RespValue::Array(vec![])),
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: vec![2],
                result: CmdResult::Ok(RespValue::Array(vec![bulk(b"1"), bulk(b"2")])),
            },
            ShardPart {
                shard: 2,
                owned_key_idxs: vec![3],
                result: CmdResult::Ok(RespValue::Array(vec![bulk(b"2"), bulk(b"3")])),
            },
        ];
        let (key, members, reply) = deferred_set(merge_sunionstore(&parts, &args, &keys, 0));
        assert_eq!(key, b"d");
        assert_eq!(reply, 3);
        assert_eq!(members, ["1", "2", "3"]);
    }

    #[test]
    fn merge_sdiffstore_basic() {
        let args = vec![
            b"SDIFFSTORE".to_vec(),
            b"d".to_vec(),
            b"base".to_vec(),
            b"sub".to_vec(),
        ];
        let keys = [1usize, 2, 3];
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: vec![1],
                result: CmdResult::Ok(RespValue::Array(vec![])),
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: vec![2],
                result: CmdResult::Ok(RespValue::Array(vec![bulk(b"x"), bulk(b"y"), bulk(b"z")])),
            },
            ShardPart {
                shard: 2,
                owned_key_idxs: vec![3],
                result: CmdResult::Ok(RespValue::Array(vec![bulk(b"y")])),
            },
        ];
        let (key, members, reply) = deferred_set(merge_sdiffstore(&parts, &args, &keys, 0));
        assert_eq!(key, b"d");
        assert_eq!(reply, 2);
        assert_eq!(members, ["x", "z"]);
    }

    #[test]
    fn s_intercard_multi_shard() {
        let mut db = DbSlice::new(0);
        set_of(&mut db, "s1", &["2", "b", "1", "a"]);
        set_of(&mut db, "s2", &["3", "c", "2", "b"]);
        let argv = vec![
            b"SINTERCARD".to_vec(),
            b"2".to_vec(),
            b"s1".to_vec(),
            b"s2".to_vec(),
        ];
        let keys = [2usize, 3];

        // Shard 0 owns s1, shard 1 owns s2; "LIMIT"/value phantom keys live elsewhere.
        let owned0 = vec![2usize];
        let owned1 = vec![3usize];
        let mut ctx0 = OpContext {
            db: &mut db,
            args: &argv,
            owned_keys: &owned0,
            first_key_idx: 2,
            conn_id: 0,
            now_ms: 0,
        };
        let p0 = exec_sintercard(&mut ctx0);
        let mut ctx1 = OpContext {
            db: &mut db,
            args: &argv,
            owned_keys: &owned1,
            first_key_idx: 2,
            conn_id: 0,
            now_ms: 0,
        };
        let p1 = exec_sintercard(&mut ctx1);
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: owned0,
                result: p0,
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: owned1,
                result: p1,
            },
        ];
        assert_eq!(2, int(merge_sintercard(&parts, &argv, &keys, 0)));
    }

    #[test]
    fn s_intercard_phantom_shard_skipped() {
        let mut db = DbSlice::new(0);
        set_of(&mut db, "s1", &["2", "b", "1", "a"]);
        let argv = vec![
            b"SINTERCARD".to_vec(),
            b"1".to_vec(),
            b"s1".to_vec(),
            b"LIMIT".to_vec(),
            b"5".to_vec(),
        ];
        let keys = [2usize, 3, 4];

        // The real key is on shard 0; a phantom shard owns only "LIMIT"/"5".
        let owned0 = vec![2usize];
        let owned1 = vec![3usize, 4usize];
        let mut ctx0 = OpContext {
            db: &mut db,
            args: &argv,
            owned_keys: &owned0,
            first_key_idx: 2,
            conn_id: 0,
            now_ms: 0,
        };
        let p0 = exec_sintercard(&mut ctx0);
        let mut ctx1 = OpContext {
            db: &mut db,
            args: &argv,
            owned_keys: &owned1,
            first_key_idx: 2,
            conn_id: 0,
            now_ms: 0,
        };
        let p1 = exec_sintercard(&mut ctx1);
        assert!(matches!(p1, CmdResult::Ok(RespValue::Nil)));
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: owned0,
                result: p0,
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: owned1,
                result: p1,
            },
        ];
        assert_eq!(4, int(merge_sintercard(&parts, &argv, &keys, 0)));
    }

    // Multi-shard merges must surface exec errors (e.g. an invalid numkeys)
    // rather than synthesizing a result from the unparsed arguments.
    #[test]
    fn s_intercard_merge_propagates_exec_error() {
        let mut db = DbSlice::new(0);
        set_of(&mut db, "s1", &["a"]);
        set_of(&mut db, "s2", &["b"]);
        let argv = vec![
            b"SINTERCARD".to_vec(),
            b"a".to_vec(),
            b"s1".to_vec(),
            b"s2".to_vec(),
        ];
        let keys = [2usize, 3];
        let owned0 = vec![2usize];
        let owned1 = vec![3usize];
        let mut ctx0 = OpContext {
            db: &mut db,
            args: &argv,
            owned_keys: &owned0,
            first_key_idx: 2,
            conn_id: 0,
            now_ms: 0,
        };
        let p0 = exec_sintercard(&mut ctx0);
        let mut ctx1 = OpContext {
            db: &mut db,
            args: &argv,
            owned_keys: &owned1,
            first_key_idx: 2,
            conn_id: 0,
            now_ms: 0,
        };
        let p1 = exec_sintercard(&mut ctx1);
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: owned0,
                result: p0,
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: owned1,
                result: p1,
            },
        ];
        assert!(merge_sintercard(&parts, &argv, &keys, 0).is_err());
    }

    #[test]
    fn s_move() {
        let mut db = DbSlice::new(0);
        set_of(&mut db, "a", &["1", "2", "3", "4"]);
        set_of(&mut db, "b", &["3", "5", "6", "2"]);

        assert_eq!(1, int(run!(&mut db, b"SMOVE", b"a", b"b", b"1")));
        assert_eq!(members_of(&mut db, "a"), ["2", "3", "4"]);
        assert_eq!(members_of(&mut db, "b"), ["1", "2", "3", "5", "6"]);

        // Already present in dest still succeeds.
        assert_eq!(1, int(run!(&mut db, b"SMOVE", b"a", b"b", b"2")));
        assert_eq!(members_of(&mut db, "a"), ["3", "4"]);
        assert_eq!(members_of(&mut db, "b"), ["1", "2", "3", "5", "6"]);

        // Member not in src: no-op, replies 0.
        assert_eq!(0, int(run!(&mut db, b"SMOVE", b"a", b"b", b"99")));
        assert_eq!(members_of(&mut db, "a"), ["3", "4"]);

        // Missing src replies 0.
        assert_eq!(0, int(run!(&mut db, b"SMOVE", b"nokey", b"b", b"3")));

        // Moving the last member deletes the src key.
        set_of(&mut db, "tiny", &["only"]);
        assert_eq!(1, int(run!(&mut db, b"SMOVE", b"tiny", b"b", b"only")));
        assert!(db.find(b"tiny", 0).is_none());

        // src == dest: report membership without mutating.
        set_of(&mut db, "self", &["m"]);
        assert_eq!(1, int(run!(&mut db, b"SMOVE", b"self", b"self", b"m")));
        assert_eq!(0, int(run!(&mut db, b"SMOVE", b"self", b"self", b"nope")));
        assert_eq!(members_of(&mut db, "self"), ["m"]);

        // Moving to a missing dest creates it.
        assert_eq!(1, int(run!(&mut db, b"SMOVE", b"self", b"newdest", b"m")));
        assert_eq!(members_of(&mut db, "newdest"), ["m"]);
        assert!(db.find(b"self", 0).is_none());
    }

    #[test]
    fn s_move_wrong_type() {
        let mut db = DbSlice::new(0);
        set_of(&mut db, "a", &["1"]);
        str_of(&mut db, "str", "foo");

        assert!(err(run!(&mut db, b"SMOVE", b"str", b"a", b"1")).starts_with("WRONGTYPE"));
        // Wrong-type dest takes precedence even when the member is not in src.
        assert!(err(run!(&mut db, b"SMOVE", b"a", b"str", b"zzz")).starts_with("WRONGTYPE"));
        // ... and even when src is missing entirely.
        assert!(err(run!(&mut db, b"SMOVE", b"nokey", b"str", b"zzz")).starts_with("WRONGTYPE"));
    }

    // Multi-shard merge: src on one shard, dest on another.
    #[test]
    fn s_move_multi_shard_merge() {
        let args = vec![
            b"SMOVE".to_vec(),
            b"src".to_vec(),
            b"dest".to_vec(),
            b"m".to_vec(),
        ];
        let keys = [1usize, 2];
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: vec![1],
                result: CmdResult::Ok(RespValue::Array(vec![bulk(b"m"), bulk(b"x")])),
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: vec![2],
                result: CmdResult::Ok(RespValue::Array(vec![bulk(b"y")])),
            },
        ];
        let (stores, reply) = deferred_stores(merge_smove(&parts, &args, &keys, 0));
        assert_eq!(reply, 1);
        assert_eq!(stores.len(), 2);
        assert_eq!(stores[0].0, b"src");
        assert_eq!(stores[0].1, ["x"]);
        assert_eq!(stores[1].0, b"dest");
        assert_eq!(stores[1].1, ["m", "y"]);
    }

    // Multi-shard merge: moving the last member deletes src (store with None).
    #[test]
    fn s_move_multi_shard_merge_removes_src() {
        let args = vec![
            b"SMOVE".to_vec(),
            b"src".to_vec(),
            b"dest".to_vec(),
            b"m".to_vec(),
        ];
        let keys = [1usize, 2];
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: vec![1],
                result: CmdResult::Ok(RespValue::Array(vec![bulk(b"m")])),
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: vec![2],
                result: CmdResult::Ok(RespValue::Array(vec![bulk(b"y")])),
            },
        ];
        let (stores, reply) = deferred_stores(merge_smove(&parts, &args, &keys, 0));
        assert_eq!(reply, 1);
        assert_eq!(stores.len(), 2);
        assert_eq!(stores[0].0, b"src");
        assert!(stores[0].1.is_empty());
        assert_eq!(stores[1].0, b"dest");
        assert_eq!(stores[1].1, ["m", "y"]);
    }

    // Multi-shard merge: member absent from src replies 0 with no stores.
    #[test]
    fn s_move_multi_shard_merge_member_absent() {
        let args = vec![
            b"SMOVE".to_vec(),
            b"src".to_vec(),
            b"dest".to_vec(),
            b"nope".to_vec(),
        ];
        let keys = [1usize, 2];
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: vec![1],
                result: CmdResult::Ok(RespValue::Array(vec![bulk(b"m")])),
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: vec![2],
                result: CmdResult::Ok(RespValue::Array(vec![])),
            },
        ];
        assert_eq!(0, int(merge_smove(&parts, &args, &keys, 0)));
    }

    // Multi-shard merge: missing src replies 0.
    #[test]
    fn s_move_multi_shard_merge_missing_src() {
        let args = vec![
            b"SMOVE".to_vec(),
            b"src".to_vec(),
            b"dest".to_vec(),
            b"m".to_vec(),
        ];
        let keys = [1usize, 2];
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: vec![1],
                result: CmdResult::Ok(RespValue::Nil),
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: vec![2],
                result: CmdResult::Ok(RespValue::Array(vec![bulk(b"y")])),
            },
        ];
        assert_eq!(0, int(merge_smove(&parts, &args, &keys, 0)));
    }

    // Multi-shard merge: wrong-type on either shard propagates.
    #[test]
    fn s_move_multi_shard_merge_wrong_type() {
        let args = vec![
            b"SMOVE".to_vec(),
            b"src".to_vec(),
            b"dest".to_vec(),
            b"m".to_vec(),
        ];
        let keys = [1usize, 2];
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: vec![1],
                result: CmdResult::Ok(RespValue::Array(vec![bulk(b"m")])),
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: vec![2],
                result: CmdResult::Err(RespError::wrong_type()),
            },
        ];
        assert!(merge_smove(&parts, &args, &keys, 0).is_err());
    }

    // -----------------------------------------------------------------------
    // SADDEX / SSCAN / lazy member expiry
    // -----------------------------------------------------------------------

    /// `kMemberExpiryBase * 1000` (dragonfly/src/server/common.h).
    const T0: u64 = 1_675_209_600_000;

    fn exists(db: &mut DbSlice, key: &str) -> bool {
        db.find(key.as_bytes(), 0).is_some()
    }

    /// Remaining TTL in ms for a member, read directly from the stored set.
    fn ttl_of(db: &mut DbSlice, key: &str, member: &str, now_ms: u64) -> i64 {
        match db.find(key.as_bytes(), 0) {
            Some(PrimeValue::Set(s)) => s.member_ttl_ms(member.as_bytes(), now_ms),
            _ => panic!("expected set at {key}"),
        }
    }

    fn array_len(r: &CmdResult) -> usize {
        match r {
            CmdResult::Ok(RespValue::Array(v)) => v.len(),
            o => panic!("expected array, got {:?}", o.clone().into_resp_value()),
        }
    }

    /// Parse an SSCAN reply `[cursor_bulk, [member ...]]`.
    fn scan(r: CmdResult) -> (u64, Vec<String>) {
        match r {
            CmdResult::Ok(RespValue::Array(v)) => {
                let mut it = v.into_iter();
                let cursor = match it.next().unwrap() {
                    RespValue::Bulk(b) => String::from_utf8_lossy(&b).parse::<u64>().unwrap(),
                    o => panic!("expected cursor bulk, got {o:?}"),
                };
                let members = match it.next().unwrap() {
                    RespValue::Array(a) => a
                        .into_iter()
                        .map(|x| match x {
                            RespValue::Bulk(b) => String::from_utf8_lossy(&b).into_owned(),
                            o => panic!("unexpected scan member {o:?}"),
                        })
                        .collect(),
                    o => panic!("expected members array, got {o:?}"),
                };
                (cursor, members)
            }
            o => panic!("expected scan reply, got {:?}", o.into_resp_value()),
        }
    }

    fn set_of_i64(db: &mut DbSlice, key: &str, n: usize) {
        let mut s = Set::new();
        for i in 0..n {
            s.add(CompactString::from(format!("{i}")));
        }
        db.insert(key.as_bytes(), PrimeValue::Set(s));
    }

    #[test]
    fn s_scan() {
        let mut db = DbSlice::new(0);
        // Missing key -> cursor 0, empty members.
        let (c, v) = scan(run!(
            &mut db,
            b"SSCAN",
            b"non-existing-key",
            b"100",
            b"count",
            b"5"
        ));
        assert_eq!(c, 0);
        assert!(v.is_empty());

        // All-integer set behaves like an intset: everything returned, cursor 0.
        set_of_i64(&mut db, "myintset", 15);
        let (c, v) = scan(run!(&mut db, b"SSCAN", b"myintset", b"0", b"count", b"4"));
        assert_eq!(c, 0);
        assert_eq!(v.len(), 15);

        let (_, v) = scan(run!(&mut db, b"SSCAN", b"myintset", b"0", b"match", b"1*"));
        assert_eq!(v, ["1", "10", "11", "12", "13", "14"]);

        // String set: paginated with COUNT.
        let all: Vec<String> = (0..15).map(|i| format!("str-{i}")).collect();
        let refs: Vec<&str> = all.iter().map(std::string::String::as_str).collect();
        set_of(&mut db, "mystrset", &refs);

        let (_, v) = scan(run!(&mut db, b"SSCAN", b"mystrset", b"0", b"count", b"5"));
        assert_eq!(v.len(), 5);

        let (_, v) = scan(run!(
            &mut db,
            b"SSCAN",
            b"mystrset",
            b"0",
            b"match",
            b"str-1*"
        ));
        assert_eq!(
            v,
            ["str-1", "str-10", "str-11", "str-12", "str-13", "str-14"]
        );

        let (_, v) = scan(run!(
            &mut db,
            b"SSCAN",
            b"mystrset",
            b"0",
            b"match",
            b"str-1*",
            b"count",
            b"3"
        ));
        assert_eq!(v.len(), 3);
        assert!(v.iter().all(|m| m.starts_with("str-1")));

        let (_, v) = scan(run!(&mut db, b"SSCAN", b"mystrset", b"0", b"match", b"1*"));
        assert!(v.is_empty());

        // Invalid (non-numeric) cursors are rejected without crashing.
        let r = run!(&mut db, b"SSCAN", b"mystrset", b"abc");
        assert!(err(r).contains("invalid cursor"));
        let r = run!(&mut db, b"SSCAN", b"mystrset", br#"{"a":1}"#, b"LIST");
        assert!(err(r).contains("invalid cursor"));

        // Still responsive after the rejected cursors.
        let (_, v) = scan(run!(
            &mut db,
            b"SSCAN",
            b"mystrset",
            b"0",
            b"match",
            b"str-1*"
        ));
        assert_eq!(v.len(), 6);
    }

    #[test]
    fn s_scan_cursor_resume() {
        let mut db = DbSlice::new(0);
        let all: Vec<String> = (0..100).map(|i| format!("key-{i}")).collect();
        let refs: Vec<&str> = all.iter().map(std::string::String::as_str).collect();
        set_of(&mut db, "big", &refs);

        let mut cursor = 0u64;
        let mut seen: HashSet<String> = HashSet::new();
        let mut iterations = 0;
        loop {
            let argv = vec![
                b"SSCAN".to_vec(),
                b"big".to_vec(),
                cursor.to_string().into_bytes(),
                b"count".to_vec(),
                b"7".to_vec(),
            ];
            let (c, v) = scan(dispatch(&mut db, &argv));
            seen.extend(v);
            iterations += 1;
            if c == 0 {
                break;
            }
            cursor = c;
        }
        assert_eq!(seen.len(), 100);
        assert!(iterations > 1);
    }

    #[test]
    fn s_huge_scan() {
        let mut db = DbSlice::new(0);
        set_of_i64(&mut db, "big", 60000);
        let (_, v) = scan(run!(&mut db, b"SSCAN", b"big", b"0", b"count", b"50000"));
        assert!(v.len() >= 50000);
    }

    #[test]
    fn s_saddex() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            1,
            int(run_at!(&mut db, T0, b"SADDEX", b"key", b"2", b"val"))
        );
        // Refresh: member still live, TTL renewed from now.
        assert_eq!(
            0,
            int(run_at!(&mut db, T0 + 1500, b"SADDEX", b"key", b"2", b"val"))
        );
        assert_eq!(
            1,
            int(run_at!(&mut db, T0 + 2500, b"SISMEMBER", b"key", b"val"))
        );

        // Non-numeric TTL is rejected.
        let r = run_at!(&mut db, T0, b"SADDEX", b"k", b"one", b"v");
        assert!(err(r).contains("value is not an integer or out of range"));

        // orig with TTL 10.
        assert_eq!(
            1,
            int(run_at!(
                &mut db,
                T0 + 2500,
                b"SADDEX",
                b"key",
                b"10",
                b"orig"
            ))
        );
        // KEEPTTL: new gets TTL 1, orig's expiry is preserved.
        assert_eq!(
            1,
            int(run_at!(
                &mut db,
                T0 + 2500,
                b"SADDEX",
                b"key",
                b"KEEPTTL",
                b"1",
                b"orig",
                b"new"
            ))
        );
        assert!(ttl_of(&mut db, "key", "new", T0 + 2500) <= 1000);
        assert!(ttl_of(&mut db, "key", "orig", T0 + 2500) > 5000);
        // Without KEEPTTL the TTL is overwritten.
        assert_eq!(
            0,
            int(run_at!(
                &mut db,
                T0 + 2500,
                b"SADDEX",
                b"key",
                b"2",
                b"orig",
                b"new"
            ))
        );
        assert!(ttl_of(&mut db, "key", "orig", T0 + 2500) <= 2000);
        // At least one member argument is required.
        let r = run_at!(&mut db, T0 + 2500, b"SADDEX", b"key", b"KEEPTTL", b"2");
        assert!(err(r).contains("wrong number of arguments"));
    }

    #[test]
    fn s_saddex_ttl_boundary() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            1,
            int(run_at!(
                &mut db,
                T0,
                b"SADDEX",
                b"key",
                b"268435455",
                b"at_cap"
            ))
        );
        let r = run_at!(&mut db, T0, b"SADDEX", b"key", b"268435456", b"above_cap");
        assert!(err(r).contains("value is not an integer or out of range"));
    }

    #[test]
    fn s_saddex_expiry_transfer() {
        let mut db = DbSlice::new(0);
        for i in 0..10 {
            assert_eq!(
                1,
                int(run_at!(
                    &mut db,
                    T0,
                    b"SADDEX",
                    b"key",
                    b"5",
                    format!("{i}").into_bytes()
                ))
            );
        }
        for i in 0..9 {
            run_at!(&mut db, T0, b"SREM", b"key", format!("{i}").into_bytes());
        }
        assert_eq!(1, int(run_at!(&mut db, T0, b"SCARD", b"key")));
        // Advancing past the last member's TTL empties the set.
        run_at!(&mut db, T0 + 6000, b"SMEMBERS", b"key");
        assert_eq!(0, int(run_at!(&mut db, T0 + 6000, b"SCARD", b"key")));
    }

    #[test]
    fn s_pop_all_expired() {
        let mut db = DbSlice::new(0);
        set_of(&mut db, "key", &["member"]);
        assert_eq!(
            0,
            int(run_at!(&mut db, T0, b"SADDEX", b"key", b"1", b"member"))
        );
        let r = run_at!(&mut db, T0 + 2000, b"SPOP", b"key");
        assert!(matches!(r.into_resp_value(), RespValue::Nil));
        assert!(!exists(&mut db, "key"));
    }

    #[test]
    fn s_pop_with_expired_members() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            3,
            int(run_at!(
                &mut db, T0, b"SADDEX", b"key", b"1", b"a", b"b", b"c"
            ))
        );
        let r = run_at!(&mut db, T0 + 2000, b"SPOP", b"key", b"2");
        assert!(matches!(r.into_resp_value(), RespValue::Array(v) if v.is_empty()));
        assert!(!exists(&mut db, "key"));
        // Single-arg form -> NIL, key deleted.
        assert_eq!(
            2,
            int(run_at!(&mut db, T0, b"SADDEX", b"key2", b"1", b"x", b"y"))
        );
        let r = run_at!(&mut db, T0 + 2000, b"SPOP", b"key2");
        assert!(matches!(r.into_resp_value(), RespValue::Nil));
        assert!(!exists(&mut db, "key2"));
    }

    #[test]
    fn s_pop_single_arg_expired_case2() {
        let mut db = DbSlice::new(0);
        for attempt in 0..50 {
            let key = format!("key{attempt}");
            set_of(&mut db, &key, &["live"]);
            assert_eq!(
                3,
                int(run_at!(
                    &mut db,
                    T0,
                    b"SADDEX",
                    key.as_bytes(),
                    b"1",
                    b"a",
                    b"b",
                    b"c"
                ))
            );
            let r = run_at!(&mut db, T0 + 2000, b"SPOP", key.as_bytes());
            match r.into_resp_value() {
                RespValue::Bulk(b) => assert_eq!(b, b"live"),
                RespValue::Nil => {
                    assert_eq!(
                        1,
                        int(run_at!(
                            &mut db,
                            T0 + 2000,
                            b"SISMEMBER",
                            key.as_bytes(),
                            b"live"
                        ))
                    );
                }
                o => panic!("unexpected SPOP reply {o:?}"),
            }
        }
    }

    #[test]
    fn s_rand_member_with_expired_members() {
        let mut db = DbSlice::new(0);
        // Without count -> NIL, key deleted.
        run_at!(
            &mut db, T0, b"SADDEX", b"seed", b"1", b"a", b"b", b"c", b"d", b"e", b"f"
        );
        let r = run_at!(&mut db, T0 + 2000, b"SRANDMEMBER", b"seed");
        assert!(matches!(r.into_resp_value(), RespValue::Nil));
        assert!(!exists(&mut db, "seed"));
        // Positive count -> empty array.
        run_at!(
            &mut db, T0, b"SADDEX", b"seed", b"1", b"a", b"b", b"c", b"d", b"e", b"f"
        );
        let r = run_at!(&mut db, T0 + 2000, b"SRANDMEMBER", b"seed", b"1");
        assert!(matches!(r.into_resp_value(), RespValue::Array(v) if v.is_empty()));
        assert!(!exists(&mut db, "seed"));
        // Negative count -> empty array.
        run_at!(
            &mut db, T0, b"SADDEX", b"seed", b"1", b"a", b"b", b"c", b"d", b"e", b"f"
        );
        let r = run_at!(&mut db, T0 + 2000, b"SRANDMEMBER", b"seed", b"-1");
        assert!(matches!(r.into_resp_value(), RespValue::Array(v) if v.is_empty()));
        assert!(!exists(&mut db, "seed"));
        // Large negative count -> empty array.
        run_at!(
            &mut db, T0, b"SADDEX", b"seed", b"1", b"a", b"b", b"c", b"d", b"e", b"f"
        );
        let r = run_at!(&mut db, T0 + 2000, b"SRANDMEMBER", b"seed", b"-25");
        assert!(matches!(r.into_resp_value(), RespValue::Array(v) if v.is_empty()));
        assert!(!exists(&mut db, "seed"));
    }

    #[test]
    fn s_sismember_deletes_empty_set() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run_at!(&mut db, T0, b"SADDEX", b"key", b"1", b"a")));
        assert_eq!(
            0,
            int(run_at!(&mut db, T0 + 2000, b"SISMEMBER", b"key", b"a"))
        );
        assert!(!exists(&mut db, "key"));
    }

    #[test]
    fn s_smismember_deletes_empty_set() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            2,
            int(run_at!(&mut db, T0, b"SADDEX", b"key", b"1", b"a", b"b"))
        );
        let r = run_at!(&mut db, T0 + 2000, b"SMISMEMBER", b"key", b"a", b"b");
        match r.into_resp_value() {
            RespValue::Array(v) => assert_eq!(v.len(), 2),
            o => panic!("expected array, got {o:?}"),
        }
        assert!(!exists(&mut db, "key"));
    }

    #[test]
    fn s_sdiff_all_members_expired() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            3,
            int(run_at!(
                &mut db, T0, b"SADDEX", b"src", b"1", b"a", b"b", b"c"
            ))
        );
        set_of(&mut db, "other", &["x"]);
        let r = run_at!(&mut db, T0 + 2000, b"SDIFF", b"src", b"other");
        assert_eq!(0, array_len(&r));
        assert!(!exists(&mut db, "src"));
        // SDIFFSTORE stores nothing and returns 0.
        assert_eq!(
            3,
            int(run_at!(
                &mut db, T0, b"SADDEX", b"src", b"1", b"a", b"b", b"c"
            ))
        );
        assert_eq!(
            0,
            int(run_at!(
                &mut db,
                T0 + 2000,
                b"SDIFFSTORE",
                b"dest",
                b"src",
                b"other"
            ))
        );
        assert!(!exists(&mut db, "src"));
    }

    #[test]
    fn s_set_ops_delete_empty_after_expiry() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            2,
            int(run_at!(&mut db, T0, b"SADDEX", b"s1", b"1", b"a", b"b"))
        );
        let r = run_at!(&mut db, T0 + 2000, b"SUNION", b"s1");
        assert_eq!(0, array_len(&r));
        assert!(!exists(&mut db, "s1"));
        assert_eq!(
            2,
            int(run_at!(&mut db, T0, b"SADDEX", b"s2", b"1", b"a", b"b"))
        );
        let r = run_at!(&mut db, T0 + 2000, b"SINTER", b"s2");
        assert_eq!(0, array_len(&r));
        assert!(!exists(&mut db, "s2"));
    }

    #[test]
    fn s_scan_deletes_empty_set() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            2,
            int(run_at!(&mut db, T0, b"SADDEX", b"key", b"1", b"a", b"b"))
        );
        let (c, v) = scan(run_at!(&mut db, T0 + 2000, b"SSCAN", b"key", b"0"));
        assert_eq!(c, 0);
        assert!(v.is_empty());
        assert!(!exists(&mut db, "key"));
    }

    #[test]
    fn s_inter_multi_key_deletes_empty_set() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            2,
            int(run_at!(&mut db, T0, b"SADDEX", b"key1", b"1", b"a", b"b"))
        );
        set_of(&mut db, "key2", &["a", "b"]);
        let r = run_at!(&mut db, T0 + 2000, b"SINTER", b"key1", b"key2");
        assert_eq!(0, array_len(&r));
        assert!(!exists(&mut db, "key1"));
        assert!(exists(&mut db, "key2"));
    }

    #[test]
    fn s_move_deletes_empty_source_set() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run_at!(&mut db, T0, b"SADDEX", b"src", b"1", b"a")));
        set_of(&mut db, "dst", &["x"]);
        assert_eq!(
            0,
            int(run_at!(&mut db, T0 + 2000, b"SMOVE", b"src", b"dst", b"a"))
        );
        assert!(!exists(&mut db, "src"));
    }
}
