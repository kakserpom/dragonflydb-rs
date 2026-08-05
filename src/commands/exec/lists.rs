use crate::commands::{
    Command, FLAG_BLOCKING, FLAG_DENYOOM, FLAG_FAST, FLAG_MOVABLEKEYS, FLAG_MULTI_KEY,
    FLAG_NO_AUTOJOURNAL, FLAG_NOSCRIPT, FLAG_NO_REDUCED, FLAG_READONLY, FLAG_WRITE, KeyRange, OpContext, ShardPart,
    integer, ok,
};
use crate::core::PrimeValue;
use crate::core::quicklist::{ListItem, QuickList};
use crate::error::{CmdResult, DeferredStoreItem, RespError, RespValue};
use crate::util::{parse_i64, parse_list_timeout};

fn list_mut<'a>(ctx: &'a mut OpContext, key: &[u8]) -> Result<&'a mut QuickList, RespError> {
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::List(l)) => Ok(l),
        Some(_) => Err(RespError::wrong_type()),
        None => Err(RespError::new("ERR no such key")),
    }
}

fn ensure_list<'a>(ctx: &'a mut OpContext, key: &[u8]) -> Result<&'a mut QuickList, RespError> {
    if ctx.db.find(key, ctx.now_ms).is_none() {
        ctx.db.insert(key, PrimeValue::List(QuickList::new()));
    }
    list_mut(ctx, key)
}

fn push(ctx: &mut OpContext, front: bool, only_if_exists: bool) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let items: Vec<ListItem> = ctx.args[key_idx + 1..]
        .iter()
        .map(|a| ListItem::from_bytes(a))
        .collect();
    if only_if_exists {
        let ql = match list_mut(ctx, key) {
            Ok(l) => l,
            Err(e) => {
                if e.message.starts_with("WRONGTYPE") {
                    return CmdResult::Err(e);
                }
                return CmdResult::Ok(integer(0));
            }
        };
        // `LPUSH a b c` must yield `[c, b, a]`: each value pushes onto the head,
        // so values iterate in the order given.
        for item in items {
            if front {
                ql.push_front(item);
            } else {
                ql.push_back(item);
            }
        }
        return CmdResult::Ok(integer(ql.len() as i64));
    }
    let ql = match ensure_list(ctx, key) {
        Ok(l) => l,
        Err(e) => return CmdResult::Err(e),
    };
    for item in items {
        if front {
            ql.push_front(item);
        } else {
            ql.push_back(item);
        }
    }
    let len = ql.len() as i64;
    CmdResult::Ok(integer(len))
}

fn exec_lpush(ctx: &mut OpContext) -> CmdResult {
    push(ctx, true, false)
}
fn exec_rpush(ctx: &mut OpContext) -> CmdResult {
    push(ctx, false, false)
}
fn exec_lpushx(ctx: &mut OpContext) -> CmdResult {
    push(ctx, true, true)
}
fn exec_rpushx(ctx: &mut OpContext) -> CmdResult {
    push(ctx, false, true)
}

fn pop(ctx: &mut OpContext, front: bool) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
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
    let Some(ql) = (match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::List(l)) => Some(l),
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => None,
    }) else {
        return CmdResult::Ok(if with_count {
            RespValue::Array(vec![])
        } else {
            RespValue::Nil
        });
    };
    let mut out = Vec::new();
    for _ in 0..count {
        let item = if front { ql.pop_front() } else { ql.pop_back() };
        match item {
            Some(it) => out.push(RespValue::Bulk(it.as_bytes())),
            None => break,
        }
    }
    if ql.is_empty() {
        ctx.db.remove(key);
    }
    if with_count {
        CmdResult::Ok(RespValue::Array(out))
    } else {
        CmdResult::Ok(out.into_iter().next().unwrap_or(RespValue::Nil))
    }
}

fn exec_lpop(ctx: &mut OpContext) -> CmdResult {
    pop(ctx, true)
}
fn exec_rpop(ctx: &mut OpContext) -> CmdResult {
    pop(ctx, false)
}

fn exec_llen(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::List(l)) => CmdResult::Ok(integer(l.len() as i64)),
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

fn exec_lrange(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let Some(start) = parse_i64(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::integer());
    };
    let Some(stop) = parse_i64(&ctx.args[key_idx + 2]) else {
        return CmdResult::Err(RespError::integer());
    };
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::List(l)) => {
            let items: Vec<RespValue> = match l.range(start, stop) {
                Some(it) => it.map(|x| RespValue::Bulk(x.as_bytes())).collect(),
                None => vec![],
            };
            CmdResult::Ok(RespValue::Array(items))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Array(vec![])),
    }
}

fn exec_lindex(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let Some(idx) = parse_i64(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::integer());
    };
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::List(l)) => match l.get(idx) {
            Some(it) => CmdResult::Ok(RespValue::Bulk(it.as_bytes())),
            None => CmdResult::Ok(RespValue::Nil),
        },
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Nil),
    }
}

fn exec_lset(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let Some(idx) = parse_i64(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::integer());
    };
    let value = &ctx.args[key_idx + 2];
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::List(l)) => match l.set(idx, ListItem::from_bytes(value)) {
            Some(_) => CmdResult::Ok(ok()),
            None => CmdResult::Err(RespError::out_of_range()),
        },
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Err(RespError::new("ERR no such key")),
    }
}

fn exec_lrem(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let Some(count) = parse_i64(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::integer());
    };
    let value = &ctx.args[key_idx + 2];
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::List(l)) => {
            let removed = if count >= 0 {
                l.remove_value(value, count)
            } else {
                // negative: remove from tail. Rebuild reversed.
                let mut items: Vec<ListItem> = l.iter().cloned().collect();
                items.reverse();
                let mut ql = QuickList::from_items(items);
                let removed = ql.remove_value(value, -count);
                let mut items: Vec<ListItem> = ql.iter().cloned().collect();
                items.reverse();
                *l = QuickList::from_items(items);
                removed
            };
            if l.is_empty() {
                ctx.db.remove(key);
            }
            CmdResult::Ok(integer(removed as i64))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

fn exec_ltrim(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let Some(start) = parse_i64(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::integer());
    };
    let Some(stop) = parse_i64(&ctx.args[key_idx + 2]) else {
        return CmdResult::Err(RespError::integer());
    };
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::List(l)) => {
            let keep: Vec<ListItem> = match l.range(start, stop) {
                Some(it) => it.cloned().collect(),
                None => vec![],
            };
            *l = QuickList::from_items(keep);
            if l.is_empty() {
                ctx.db.remove(key);
            }
            CmdResult::Ok(ok())
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(ok()),
    }
}

fn exec_lpos(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let value = &ctx.args[key_idx + 1];
    let mut rank: i64 = 1;
    let mut count: Option<i64> = None;
    let mut maxlen: Option<usize> = None;
    let mut i = key_idx + 2;
    while i < ctx.args.len() {
        let t = ctx.args[i].to_ascii_uppercase();
        if i + 1 >= ctx.args.len() {
            return CmdResult::Err(RespError::syntax());
        }
        match t.as_slice() {
            b"RANK" => {
                rank = match parse_i64(&ctx.args[i + 1]) {
                    Some(v) => v,
                    None => return CmdResult::Err(RespError::integer()),
                };
                if rank == 0 {
                    return CmdResult::Err(RespError::new(
                        "ERR RANK can't be zero. Use 1 to start searching from the first match or -1 to start searching from the last match.",
                    ));
                }
            }
            b"COUNT" => {
                let Some(c) = parse_i64(&ctx.args[i + 1]) else {
                    return CmdResult::Err(RespError::integer());
                };
                if c < 0 {
                    return CmdResult::Err(RespError::new("ERR COUNT can't be negative"));
                }
                count = Some(c);
            }
            b"MAXLEN" => {
                maxlen = Some(match parse_i64(&ctx.args[i + 1]) {
                    Some(v) => v as usize,
                    None => return CmdResult::Err(RespError::integer()),
                });
            }
            _ => return CmdResult::Err(RespError::syntax()),
        }
        i += 2;
    }
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::List(l)) => {
            let items: Vec<&ListItem> = if rank < 0 {
                let mut v: Vec<&ListItem> = l.iter().collect();
                v.reverse();
                v
            } else {
                l.iter().collect()
            };
            let maxlen = maxlen.unwrap_or(items.len());
            let mut matches = Vec::new();
            let skip = rank.unsigned_abs().saturating_sub(1) as usize;
            for (pos, item) in items.iter().enumerate().take(maxlen) {
                if item.as_bytes() == *value && matches.len() >= skip {
                    matches.push(pos as i64);
                    if count.is_some_and(|c| matches.len() as i64 >= c) {
                        break;
                    }
                }
            }
            if count.is_some() {
                CmdResult::Ok(RespValue::Array(matches.into_iter().map(integer).collect()))
            } else if matches.is_empty() {
                CmdResult::Ok(RespValue::Nil)
            } else {
                CmdResult::Ok(integer(matches[0]))
            }
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(if count.is_some() {
            RespValue::Array(vec![])
        } else {
            RespValue::Nil
        }),
    }
}

// ---------------------------------------------------------------------------
// LINSERT / LMOVE / RPOPLPUSH / LMPOP / BLPOP / BRPOP / BLMOVE / BRPOPLPUSH /
// BLMPOP
//
// Multi-key commands run per-shard as read-only *reports* (never mutating) and
// the merge function reconstructs the final state and issues deferred stores,
// mirroring the sets-family SMOVE pattern. Single-shard commands (one part
// owning every key) execute in place and return the final reply directly.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dir {
    Left,
    Right,
}

fn parse_dir(arg: &[u8]) -> Option<Dir> {
    if arg.eq_ignore_ascii_case(b"LEFT") {
        Some(Dir::Left)
    } else if arg.eq_ignore_ascii_case(b"RIGHT") {
        Some(Dir::Right)
    } else {
        None
    }
}

fn pop_from(l: &mut QuickList, dir: Dir) -> Option<ListItem> {
    match dir {
        Dir::Left => l.pop_front(),
        Dir::Right => l.pop_back(),
    }
}

fn push_to(l: &mut QuickList, dir: Dir, item: ListItem) {
    match dir {
        Dir::Left => l.push_front(item),
        Dir::Right => l.push_back(item),
    }
}

fn list_items(l: &QuickList) -> Vec<ListItem> {
    l.iter().cloned().collect()
}

fn items_array(items: &[ListItem]) -> RespValue {
    RespValue::Array(
        items
            .iter()
            .map(|i| RespValue::Bulk(i.as_bytes()))
            .collect(),
    )
}

/// Decode a report's element array (produced by `items_array`) back into items.
fn resp_items_to_list(rep: &[RespValue]) -> Vec<ListItem> {
    rep.iter()
        .filter_map(|v| match v {
            RespValue::Bulk(b) => Some(ListItem::from_bytes(b)),
            _ => None,
        })
        .collect()
}

/// A store entry that deletes the key when `items` is empty.
fn list_store(key: &[u8], items: Vec<ListItem>) -> DeferredStoreItem {
    if items.is_empty() {
        (key.to_vec(), None, None, false)
    } else {
        (
            key.to_vec(),
            Some(PrimeValue::List(QuickList::from_items(items))),
            None,
            false,
        )
    }
}

/// Pop up to `count` elements from `dir` without mutating, returning the popped
/// values (in pop order) and the remaining elements. `count == 0` pops nothing.
fn pop_values(l: &QuickList, dir: Dir, count: usize) -> (Vec<ListItem>, Vec<ListItem>) {
    let mut all = list_items(l);
    let count = count.min(all.len());
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let v = if dir == Dir::Left {
            all.remove(0)
        } else {
            all.pop().unwrap()
        };
        values.push(v);
    }
    (values, all)
}

fn peek_one(l: &QuickList, dir: Dir) -> (ListItem, Vec<ListItem>) {
    let mut all = list_items(l);
    let v = if dir == Dir::Left {
        all.remove(0)
    } else {
        all.pop().unwrap()
    };
    (v, all)
}

// ---------------------------------------------------------------------------
// LINSERT key BEFORE|AFTER pivot element
// ---------------------------------------------------------------------------

fn exec_linsert(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let after = {
        let a = &ctx.args[key_idx + 1];
        if a.eq_ignore_ascii_case(b"AFTER") {
            true
        } else if a.eq_ignore_ascii_case(b"BEFORE") {
            false
        } else {
            return CmdResult::Err(RespError::syntax());
        }
    };
    let pivot = &ctx.args[key_idx + 2];
    let elem = ListItem::from_bytes(&ctx.args[key_idx + 3]);
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::List(l)) => {
            if l.insert_relative(pivot, elem, after) {
                CmdResult::Ok(integer(l.len() as i64))
            } else {
                CmdResult::Ok(integer(-1))
            }
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

// ---------------------------------------------------------------------------
// LMOVE src dest LEFT|RIGHT LEFT|RIGHT  /  RPOPLPUSH src dest
// ---------------------------------------------------------------------------

/// Single-shard move: pop from `src`, push to `dest` (which may be the same
/// key, rotating in place). Mirrors `OpMoveSingleShard`. `missing` is returned
/// when the source does not exist (nil for the non-blocking commands, Blocked
/// for the blocking ones).
fn move_single_shard(
    ctx: &mut OpContext,
    src_idx: usize,
    dest_idx: usize,
    src_dir: Dir,
    dest_dir: Dir,
    missing: CmdResult,
) -> CmdResult {
    let src_key = &ctx.args[src_idx];
    let dest_key = &ctx.args[dest_idx];
    if src_idx == dest_idx {
        // src and dest are the same key: the pop happens in place and the
        // result rotates within the single list (upstream `OpMoveSingleShard`
        // fast path).
        let ql = match ctx.db.find_mut(src_key, ctx.now_ms) {
            Some(PrimeValue::List(l)) => l,
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => return missing,
        };
        let Some(v) = pop_from(ql, src_dir) else {
            return missing;
        };
        let bytes = v.as_bytes();
        push_to(ql, dest_dir, v);
        return CmdResult::Ok(RespValue::Bulk(bytes));
    }
    // Type-check the destination first (upstream `AddOrFind` runs before the
    // source lookup, so a wrong-type dest shadows a missing source).
    match ctx.db.find(dest_key, ctx.now_ms) {
        Some(PrimeValue::List(_)) | None => {}
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
    }
    let (v, remaining) = {
        let ql = match ctx.db.find_mut(src_key, ctx.now_ms) {
            Some(PrimeValue::List(l)) => l,
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => return missing,
        };
        let mut all: Vec<ListItem> = ql.iter().cloned().collect();
        let v = if src_dir == Dir::Left {
            all.remove(0)
        } else {
            all.pop().unwrap()
        };
        (v, all)
    };
    let bytes = v.as_bytes();
    if remaining.is_empty() {
        ctx.db.remove(src_key);
    } else {
        let Some(PrimeValue::List(ql)) = ctx.db.find_mut(src_key, ctx.now_ms) else {
            return CmdResult::Err(RespError::wrong_type());
        };
        *ql = QuickList::from_items(remaining);
    }
    let Ok(ql) = ensure_list(ctx, dest_key) else {
        return CmdResult::Err(RespError::wrong_type());
    };
    push_to(ql, dest_dir, v);
    CmdResult::Ok(RespValue::Bulk(bytes))
}

/// Multi-shard partial for the move commands: the source shard reports
/// `[src_idx, value, remaining]` (or a wrong-type marker / Blocked when missing),
/// the destination shard reports `[dest_idx, elements]` (or its wrong-type
/// marker). Nothing is mutated.
fn move_partial(ctx: &mut OpContext, src_idx: usize, dest_idx: usize, src_dir: Dir) -> CmdResult {
    let src_key = &ctx.args[src_idx];
    let dest_key = &ctx.args[dest_idx];
    if ctx.owned_keys.contains(&src_idx) {
        match ctx.db.find(src_key, ctx.now_ms) {
            Some(PrimeValue::List(l)) if !l.is_empty() => {
                let (v, remaining) = peek_one(l, src_dir);
                CmdResult::Ok(RespValue::Array(vec![
                    integer(src_idx as i64),
                    RespValue::Bulk(v.as_bytes()),
                    items_array(&remaining),
                ]))
            }
            Some(PrimeValue::List(_)) | None => CmdResult::Blocked,
            Some(_) => CmdResult::Ok(RespValue::Array(vec![integer(src_idx as i64)])),
        }
    } else {
        match ctx.db.find(dest_key, ctx.now_ms) {
            Some(PrimeValue::List(l)) => CmdResult::Ok(RespValue::Array(vec![
                integer(dest_idx as i64),
                items_array(&list_items(l)),
            ])),
            Some(_) => CmdResult::Ok(RespValue::Array(vec![integer(dest_idx as i64)])),
            None => CmdResult::Ok(RespValue::Array(vec![
                integer(dest_idx as i64),
                RespValue::Array(vec![]),
            ])),
        }
    }
}

struct MoveReport {
    src_val: Option<(Vec<u8>, Vec<ListItem>)>,
    src_wrong: bool,
    dest_list: Option<Vec<ListItem>>,
    dest_wrong: bool,
}

fn collect_move_report(
    parts: &[ShardPart],
    src_idx: usize,
    dest_idx: usize,
) -> Result<MoveReport, RespError> {
    let mut rep = MoveReport {
        src_val: None,
        src_wrong: false,
        dest_list: None,
        dest_wrong: false,
    };
    for p in parts {
        match &p.result {
            CmdResult::Ok(RespValue::Array(r)) if r.len() == 1 => {
                if let RespValue::Integer(ki) = r[0] {
                    let ki = ki as usize;
                    if ki == src_idx {
                        rep.src_wrong = true;
                    } else {
                        rep.dest_wrong = true;
                    }
                }
            }
            CmdResult::Ok(RespValue::Array(r)) if r.len() == 3 => {
                if let (RespValue::Integer(ki), RespValue::Bulk(v), RespValue::Array(rem)) =
                    (&r[0], &r[1], &r[2])
                {
                    let ki = *ki as usize;
                    if ki == src_idx {
                        rep.src_val = Some((v.clone(), resp_items_to_list(rem)));
                    }
                }
            }
            CmdResult::Ok(RespValue::Array(r)) if r.len() == 2 => {
                if let (RespValue::Integer(ki), RespValue::Array(items)) = (&r[0], &r[1]) {
                    let ki = *ki as usize;
                    if ki == dest_idx {
                        rep.dest_list = Some(resp_items_to_list(items));
                    }
                }
            }
            CmdResult::Err(e) => return Err(e.clone()),
            _ => {}
        }
    }
    Ok(rep)
}

fn move_finish(
    rep: MoveReport,
    src_idx: usize,
    dest_idx: usize,
    args: &[Vec<u8>],
    dest_dir: Dir,
    missing: CmdResult,
) -> CmdResult {
    if rep.src_wrong {
        return CmdResult::Err(RespError::wrong_type());
    }
    if rep.dest_wrong {
        return CmdResult::Err(RespError::wrong_type());
    }
    let Some((value, remaining)) = rep.src_val else {
        return missing;
    };
    let mut dest = QuickList::from_items(rep.dest_list.unwrap_or_default());
    push_to(&mut dest, dest_dir, ListItem::from_bytes(&value));
    let stores = vec![
        list_store(&args[src_idx], remaining),
        list_store(&args[dest_idx], list_items(&dest)),
    ];
    CmdResult::deferred_stores(stores, RespValue::Bulk(value))
}

/// The pop/push side for a move command, taken from the argument list
/// (RPOPLPUSH/BRPOPLPUSH have fixed RIGHT/LEFT).
fn move_dirs(args: &[Vec<u8>]) -> (Dir, Dir) {
    if args[0].eq_ignore_ascii_case(b"RPOPLPUSH") || args[0].eq_ignore_ascii_case(b"BRPOPLPUSH") {
        (Dir::Right, Dir::Left)
    } else {
        (
            parse_dir(&args[3]).unwrap_or(Dir::Left),
            parse_dir(&args[4]).unwrap_or(Dir::Right),
        )
    }
}

fn merge_move(parts: &[ShardPart], args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    if parts.len() == 1 {
        return parts[0].result.clone(); // single-shard in-place execution
    }
    let (src_idx, dest_idx) = (keys[0], keys[1]);
    let rep = match collect_move_report(parts, src_idx, dest_idx) {
        Ok(r) => r,
        Err(e) => return CmdResult::Err(e),
    };
    let (_src_dir, dest_dir) = move_dirs(args);
    move_finish(
        rep,
        src_idx,
        dest_idx,
        args,
        dest_dir,
        CmdResult::Ok(RespValue::Nil),
    )
}

fn merge_move_blocking(
    parts: &[ShardPart],
    args: &[Vec<u8>],
    keys: &[usize],
    _now: u64,
) -> CmdResult {
    if parts.len() == 1 {
        return parts[0].result.clone();
    }
    let (src_idx, dest_idx) = (keys[0], keys[1]);
    let rep = match collect_move_report(parts, src_idx, dest_idx) {
        Ok(r) => r,
        Err(e) => return CmdResult::Err(e),
    };
    let (_src_dir, dest_dir) = move_dirs(args);
    move_finish(rep, src_idx, dest_idx, args, dest_dir, CmdResult::Blocked)
}

fn exec_lmove(ctx: &mut OpContext) -> CmdResult {
    let Some(src_dir) = parse_dir(&ctx.args[3]) else {
        return CmdResult::Err(RespError::syntax());
    };
    let Some(dest_dir) = parse_dir(&ctx.args[4]) else {
        return CmdResult::Err(RespError::syntax());
    };
    let (src_idx, dest_idx) = (ctx.first_key_idx, ctx.first_key_idx + 1);
    if ctx.owned_keys.contains(&src_idx) && ctx.owned_keys.contains(&dest_idx) {
        move_single_shard(
            ctx,
            src_idx,
            dest_idx,
            src_dir,
            dest_dir,
            CmdResult::Ok(RespValue::Nil),
        )
    } else {
        move_partial(ctx, src_idx, dest_idx, src_dir)
    }
}

fn exec_rpoplpush(ctx: &mut OpContext) -> CmdResult {
    let (src_idx, dest_idx) = (ctx.first_key_idx, ctx.first_key_idx + 1);
    if ctx.owned_keys.contains(&src_idx) && ctx.owned_keys.contains(&dest_idx) {
        move_single_shard(
            ctx,
            src_idx,
            dest_idx,
            Dir::Right,
            Dir::Left,
            CmdResult::Ok(RespValue::Nil),
        )
    } else {
        move_partial(ctx, src_idx, dest_idx, Dir::Right)
    }
}

// ---------------------------------------------------------------------------
// LMPOP numkeys key... LEFT|RIGHT [COUNT n]
// ---------------------------------------------------------------------------

fn parse_lmpop_numkeys(args: &[Vec<u8>], numkeys_idx: usize) -> Result<usize, RespError> {
    let Some(n) = parse_i64(&args[numkeys_idx]) else {
        return Err(RespError::integer());
    };
    if n < 1 {
        return Err(RespError::new("ERR at least 1 input key is needed"));
    }
    Ok(n as usize)
}

fn parse_lmpop_tail(
    args: &[Vec<u8>],
    numkeys_idx: usize,
    numkeys: usize,
) -> Result<(Dir, usize), RespError> {
    let dir_idx = numkeys_idx + 1 + numkeys;
    let Some(dir_arg) = args.get(dir_idx) else {
        return Err(RespError::syntax());
    };
    let Some(dir) = parse_dir(dir_arg) else {
        return Err(RespError::syntax());
    };
    let mut i = dir_idx + 1;
    let mut count = 1usize;
    if i < args.len() {
        if !args[i].eq_ignore_ascii_case(b"COUNT") {
            return Err(RespError::syntax());
        }
        let Some(count_arg) = args.get(i + 1) else {
            return Err(RespError::syntax());
        };
        let Some(c) = parse_i64(count_arg) else {
            return Err(RespError::integer());
        };
        if c < 0 {
            return Err(RespError::integer());
        }
        count = c as usize;
        i += 2;
    }
    if i != args.len() {
        return Err(RespError::syntax());
    }
    Ok((dir, count))
}

/// Multi-shard partial for the pop-family commands. Each shard reports the
/// first (in argument order) non-empty or wrong-type key it owns:
/// `[key_idx, values, remaining]` for data, `[key_idx]` for a wrong-type key,
/// otherwise `when_empty`.
fn pop_partial(ctx: &mut OpContext, dir: Dir, count: usize, when_empty: CmdResult) -> CmdResult {
    for &ki in ctx.owned_keys {
        let key = &ctx.args[ki];
        match ctx.db.find(key, ctx.now_ms) {
            Some(PrimeValue::List(l)) if !l.is_empty() => {
                let (values, remaining) = pop_values(l, dir, count);
                return CmdResult::Ok(RespValue::Array(vec![
                    integer(ki as i64),
                    items_array(&values),
                    items_array(&remaining),
                ]));
            }
            Some(PrimeValue::List(_)) | None => {}
            Some(_) => return CmdResult::Ok(RespValue::Array(vec![integer(ki as i64)])),
        }
    }
    when_empty
}

/// Single-shard in-place execution for the pop-family commands: pop from the
/// first non-empty key in argument order, apply the write immediately, and
/// return the final reply (the single-shard path never runs the merge).
/// `on_wrong_type`/`on_empty` are returned when no element was popped.
fn pop_inplace(
    ctx: &mut OpContext,
    dir: Dir,
    count: usize,
    shape: PopReplyShape,
    on_wrong_type: CmdResult,
    on_empty: CmdResult,
) -> CmdResult {
    for &ki in ctx.owned_keys {
        let key = &ctx.args[ki];
        match ctx.db.find(key, ctx.now_ms) {
            Some(PrimeValue::List(l)) if !l.is_empty() => {
                let (values, remaining) = pop_values(l, dir, count);
                if remaining.is_empty() {
                    ctx.db.remove(key);
                } else {
                    let Some(PrimeValue::List(ql)) = ctx.db.find_mut(key, ctx.now_ms) else {
                        return CmdResult::Err(RespError::wrong_type());
                    };
                    *ql = QuickList::from_items(remaining);
                }
                let vals: Vec<RespValue> = values
                    .into_iter()
                    .map(|v| RespValue::Bulk(v.as_bytes()))
                    .collect();
                return CmdResult::Ok(pop_reply(ctx.args, ki, vals, shape));
            }
            Some(PrimeValue::List(_)) | None => {}
            Some(_) => return on_wrong_type,
        }
    }
    on_empty
}

/// Collect per-part reports for the pop-family merge: the best (lowest key
/// index) data candidate and the lowest-index wrong-type key.
fn collect_pop_reports(
    parts: &[ShardPart],
    data: &mut Option<(usize, Vec<RespValue>, Vec<ListItem>)>,
    wrong: &mut Option<usize>,
) -> Result<(), RespError> {
    for p in parts {
        match &p.result {
            CmdResult::Ok(RespValue::Array(rep)) if rep.len() == 1 => {
                if let RespValue::Integer(ki) = rep[0] {
                    let ki = ki as usize;
                    *wrong = Some(match *wrong {
                        Some(b) => b.min(ki),
                        None => ki,
                    });
                }
            }
            CmdResult::Ok(RespValue::Array(rep)) if rep.len() == 3 => {
                if let (RespValue::Integer(ki), RespValue::Array(vals), RespValue::Array(rem)) =
                    (&rep[0], &rep[1], &rep[2])
                {
                    let ki = *ki as usize;
                    let better = match data {
                        Some((b, _, _)) => ki < *b,
                        None => true,
                    };
                    if better {
                        *data = Some((ki, vals.clone(), resp_items_to_list(rem)));
                    }
                }
            }
            CmdResult::Err(e) => return Err(e.clone()),
            _ => {}
        }
    }
    Ok(())
}

fn exec_lmpop(ctx: &mut OpContext) -> CmdResult {
    let numkeys_idx = 1;
    let numkeys = match parse_lmpop_numkeys(ctx.args, numkeys_idx) {
        Ok(n) => n,
        Err(e) => return CmdResult::Err(e),
    };
    let (dir, count) = match parse_lmpop_tail(ctx.args, numkeys_idx, numkeys) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    if ctx.owned_keys.len() == numkeys {
        pop_inplace(
            ctx,
            dir,
            count,
            PopReplyShape::Arrayed,
            CmdResult::Err(RespError::wrong_type()),
            CmdResult::Ok(RespValue::Nil),
        )
    } else {
        pop_partial(ctx, dir, count, CmdResult::Ok(RespValue::Nil))
    }
}

/// Final reply shape for the pop family. `Flat` is BLPOP/BRPOP's `[key, value]`,
/// `Arrayed` is LMPOP/BLMPOP's `[key, [values]]`.
#[derive(Clone, Copy)]
enum PopReplyShape {
    Flat,
    Arrayed,
}

fn pop_reply(
    args: &[Vec<u8>],
    ki: usize,
    mut vals: Vec<RespValue>,
    shape: PopReplyShape,
) -> RespValue {
    match shape {
        PopReplyShape::Arrayed => RespValue::Array(vec![
            RespValue::Bulk(args[ki].clone()),
            RespValue::Array(vals),
        ]),
        PopReplyShape::Flat => {
            let v = vals.remove(0);
            RespValue::Array(vec![RespValue::Bulk(args[ki].clone()), v])
        }
    }
}

/// Shared merge for the pop family. Single-shard runs arrive here only via the
/// coordinator's `finish_tx` (blocking commands always take that path) and the
/// executor has already returned the final reply, so a lone part is passed
/// through. Multi-shard runs carry data reports that are reshaped into the
/// reply and persisted via a deferred store. `on_wrong`/`on_blocked` select the
/// reply when a wrong-type key precedes any data or no data exists at all.
fn merge_pop(
    parts: &[ShardPart],
    args: &[Vec<u8>],
    shape: PopReplyShape,
    on_wrong: CmdResult,
    on_blocked: CmdResult,
) -> CmdResult {
    if parts.len() == 1 {
        return parts[0].result.clone();
    }
    let (mut data, mut wrong) = (None, None);
    if let Err(e) = collect_pop_reports(parts, &mut data, &mut wrong) {
        return CmdResult::Err(e);
    }
    if let (Some((k, vals, remaining)), w) = (data, wrong) {
        if w.is_none() || k < w.unwrap() {
            let reply = pop_reply(args, k, vals, shape);
            return CmdResult::deferred_stores(vec![list_store(&args[k], remaining)], reply);
        }
        CmdResult::Err(RespError::wrong_type())
    } else if wrong.is_some() {
        on_wrong
    } else {
        on_blocked
    }
}
fn merge_lmpop(parts: &[ShardPart], args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    merge_pop(
        parts,
        args,
        PopReplyShape::Arrayed,
        CmdResult::Err(RespError::wrong_type()),
        CmdResult::Ok(RespValue::Nil),
    )
}

// ---------------------------------------------------------------------------
// BLPOP / BRPOP key... timeout
// ---------------------------------------------------------------------------

fn exec_bpop(ctx: &mut OpContext, dir: Dir) -> CmdResult {
    let Some(timeout_arg) = ctx.args.last() else {
        return CmdResult::err("ERR wrong number of arguments for 'blpop' command");
    };
    let _timeout = match parse_list_timeout(timeout_arg) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(RespError::new(e)),
    };
    let key_count = ctx.args.len().saturating_sub(2);
    if ctx.owned_keys.len() == key_count {
        pop_inplace(
            ctx,
            dir,
            1,
            PopReplyShape::Flat,
            CmdResult::Err(RespError::wrong_type()),
            CmdResult::Blocked,
        )
    } else {
        pop_partial(ctx, dir, 1, CmdResult::Blocked)
    }
}

/// BLPOP replies `[key, value]`; BRPOP follows the same shape.
fn merge_bpop(parts: &[ShardPart], args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    merge_pop(
        parts,
        args,
        PopReplyShape::Flat,
        CmdResult::Err(RespError::wrong_type()),
        CmdResult::Blocked,
    )
}

fn exec_blpop(ctx: &mut OpContext) -> CmdResult {
    exec_bpop(ctx, Dir::Left)
}
fn exec_brpop(ctx: &mut OpContext) -> CmdResult {
    exec_bpop(ctx, Dir::Right)
}

// ---------------------------------------------------------------------------
// BLMOVE src dest LEFT|RIGHT LEFT|RIGHT timeout  /  BRPOPLPUSH src dest timeout
// ---------------------------------------------------------------------------

fn exec_blmove(ctx: &mut OpContext) -> CmdResult {
    let (src_dir, dest_dir, timeout_idx) = if ctx.args[0].eq_ignore_ascii_case(b"BRPOPLPUSH") {
        (Dir::Right, Dir::Left, 3)
    } else {
        let Some(src_dir) = parse_dir(&ctx.args[3]) else {
            return CmdResult::Err(RespError::syntax());
        };
        let Some(dest_dir) = parse_dir(&ctx.args[4]) else {
            return CmdResult::Err(RespError::syntax());
        };
        (src_dir, dest_dir, 5)
    };
    let _timeout = match parse_list_timeout(&ctx.args[timeout_idx]) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(RespError::new(e)),
    };
    let (src_idx, dest_idx) = (ctx.first_key_idx, ctx.first_key_idx + 1);
    if ctx.owned_keys.contains(&src_idx) && ctx.owned_keys.contains(&dest_idx) {
        move_single_shard(
            ctx,
            src_idx,
            dest_idx,
            src_dir,
            dest_dir,
            CmdResult::Blocked,
        )
    } else {
        move_partial(ctx, src_idx, dest_idx, src_dir)
    }
}

// ---------------------------------------------------------------------------
// BLMPOP timeout numkeys key... LEFT|RIGHT [COUNT n]
// ---------------------------------------------------------------------------

fn exec_blmpop(ctx: &mut OpContext) -> CmdResult {
    let _timeout = match parse_list_timeout(&ctx.args[1]) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(RespError::new(e)),
    };
    let numkeys_idx = 2;
    let numkeys = match parse_lmpop_numkeys(ctx.args, numkeys_idx) {
        Ok(n) => n,
        Err(e) => return CmdResult::Err(e),
    };
    let (dir, count) = match parse_lmpop_tail(ctx.args, numkeys_idx, numkeys) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    if ctx.owned_keys.len() == numkeys {
        pop_inplace(
            ctx,
            dir,
            count,
            PopReplyShape::Arrayed,
            CmdResult::Ok(RespValue::Nil),
            CmdResult::Blocked,
        )
    } else {
        pop_partial(ctx, dir, count, CmdResult::Blocked)
    }
}

/// BLMPOP replies `[key, [values]]`; a wrong-type key yields nil (not an
/// error), per `CmdBLMPop`.
fn merge_blmpop(parts: &[ShardPart], args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    merge_pop(
        parts,
        args,
        PopReplyShape::Arrayed,
        CmdResult::Ok(RespValue::Nil),
        CmdResult::Blocked,
    )
}

pub static CMD_LPUSH: Command = Command {
    name: "LPUSH",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_lpush,
    merge: None,
};
pub static CMD_RPUSH: Command = Command {
    name: "RPUSH",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_rpush,
    merge: None,
};
pub static CMD_LPUSHX: Command = Command {
    name: "LPUSHX",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_lpushx,
    merge: None,
};
pub static CMD_RPUSHX: Command = Command {
    name: "RPUSHX",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_rpushx,
    merge: None,
};
pub static CMD_LPOP: Command = Command {
    name: "LPOP",
    arity: -2,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_lpop,
    merge: None,
};
pub static CMD_RPOP: Command = Command {
    name: "RPOP",
    arity: -2,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_rpop,
    merge: None,
};
pub static CMD_LLEN: Command = Command {
    name: "LLEN",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_llen,
    merge: None,
};
pub static CMD_LRANGE: Command = Command {
    name: "LRANGE",
    arity: 4,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_lrange,
    merge: None,
};
pub static CMD_LINDEX: Command = Command {
    name: "LINDEX",
    arity: 3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_lindex,
    merge: None,
};
pub static CMD_LSET: Command = Command {
    name: "LSET",
    arity: 4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_lset,
    merge: None,
};
pub static CMD_LREM: Command = Command {
    name: "LREM",
    arity: 4,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_lrem,
    merge: None,
};
pub static CMD_LTRIM: Command = Command {
    name: "LTRIM",
    arity: 4,
    flags: FLAG_WRITE,
    key_range: KeyRange::ONE,
    exec: exec_ltrim,
    merge: None,
};
pub static CMD_LPOS: Command = Command {
    name: "LPOS",
    arity: -3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_lpos,
    merge: None,
};
pub static CMD_LINSERT: Command = Command {
    name: "LINSERT",
    arity: 5,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_linsert,
    merge: None,
};
pub static CMD_LMOVE: Command = Command {
    name: "LMOVE",
    arity: 5,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_MULTI_KEY | FLAG_NO_REDUCED,
    key_range: KeyRange::TWO,
    exec: exec_lmove,
    merge: Some(merge_move),
};
pub static CMD_RPOPLPUSH: Command = Command {
    name: "RPOPLPUSH",
    arity: 3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_MULTI_KEY | FLAG_NO_REDUCED,
    key_range: KeyRange::TWO,
    exec: exec_rpoplpush,
    merge: Some(merge_move),
};
pub static CMD_LMPOP: Command = Command {
    name: "LMPOP",
    arity: -4,
    flags: FLAG_WRITE | FLAG_FAST | FLAG_MULTI_KEY | FLAG_MOVABLEKEYS,
    key_range: KeyRange::NONE,
    exec: exec_lmpop,
    merge: Some(merge_lmpop),
};
pub static CMD_BLPOP: Command = Command {
    name: "BLPOP",
    arity: -3,
    flags: FLAG_WRITE | FLAG_BLOCKING | FLAG_MULTI_KEY | FLAG_NOSCRIPT | FLAG_NO_AUTOJOURNAL,
    key_range: KeyRange::ALL_BUT_LAST,
    exec: exec_blpop,
    merge: Some(merge_bpop),
};
pub static CMD_BRPOP: Command = Command {
    name: "BRPOP",
    arity: -3,
    flags: FLAG_WRITE | FLAG_BLOCKING | FLAG_MULTI_KEY | FLAG_NOSCRIPT | FLAG_NO_AUTOJOURNAL,
    key_range: KeyRange::ALL_BUT_LAST,
    exec: exec_brpop,
    merge: Some(merge_bpop),
};
pub static CMD_BLMOVE: Command = Command {
    name: "BLMOVE",
    arity: 6,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_BLOCKING | FLAG_MULTI_KEY | FLAG_NO_AUTOJOURNAL,
    key_range: KeyRange::TWO,
    exec: exec_blmove,
    merge: Some(merge_move_blocking),
};
pub static CMD_BRPOPLPUSH: Command = Command {
    name: "BRPOPLPUSH",
    arity: 4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_BLOCKING | FLAG_MULTI_KEY | FLAG_NOSCRIPT | FLAG_NO_AUTOJOURNAL,
    key_range: KeyRange::TWO,
    exec: exec_blmove,
    merge: Some(merge_move_blocking),
};
pub static CMD_BLMPOP: Command = Command {
    name: "BLMPOP",
    arity: -5,
    flags: FLAG_WRITE | FLAG_BLOCKING | FLAG_MULTI_KEY | FLAG_MOVABLEKEYS | FLAG_NO_AUTOJOURNAL,
    key_range: KeyRange::NONE,
    exec: exec_blmpop,
    merge: Some(merge_blmpop),
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::MergeFn;
    use crate::core::compact::CompactString;
    use crate::core::db::DbSlice;

    fn rpush_of(db: &mut DbSlice, key: &str, vals: &[&str]) {
        let items: Vec<ListItem> = vals
            .iter()
            .map(|s| ListItem::from_bytes(s.as_bytes()))
            .collect();
        db.insert(
            key.as_bytes(),
            PrimeValue::List(QuickList::from_items(items)),
        );
    }

    fn str_of(db: &mut DbSlice, key: &str, value: &str) {
        db.insert(key.as_bytes(), PrimeValue::Str(CompactString::from(value)));
    }

    fn list_of(db: &mut DbSlice, key: &str) -> Vec<String> {
        match db.find(key.as_bytes(), 0) {
            Some(PrimeValue::List(l)) => l
                .iter()
                .map(|i| String::from_utf8_lossy(&i.as_bytes()).into_owned())
                .collect(),
            _ => panic!("expected list at {key}"),
        }
    }

    fn missing(db: &mut DbSlice, key: &str) -> bool {
        db.find(key.as_bytes(), 0).is_none()
    }

    fn int(r: CmdResult) -> i64 {
        match r {
            CmdResult::Ok(RespValue::Integer(v)) => v,
            o => panic!("expected integer, got {:?}", o.into_resp_value()),
        }
    }

    fn bulk(r: CmdResult) -> String {
        match r {
            CmdResult::Ok(RespValue::Bulk(b)) => String::from_utf8_lossy(&b).into_owned(),
            CmdResult::Ok(RespValue::Nil) => panic!("expected bulk, got nil"),
            o => panic!("expected bulk, got {:?}", o.into_resp_value()),
        }
    }

    fn nil(r: CmdResult) {
        assert!(
            matches!(r, CmdResult::Ok(RespValue::Nil)),
            "expected nil, got {:?}",
            r.into_resp_value()
        );
    }

    fn err(r: CmdResult) -> String {
        match r {
            CmdResult::Err(e) => e.message,
            o => panic!("expected error, got {:?}", o.into_resp_value()),
        }
    }

    fn blocked(r: CmdResult) {
        assert!(
            matches!(r, CmdResult::Blocked),
            "expected Blocked, got {:?}",
            r.into_resp_value()
        );
    }

    fn strings(v: &[RespValue]) -> Vec<String> {
        v.iter()
            .map(|x| match x {
                RespValue::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
                _ => panic!("unexpected element {x:?}"),
            })
            .collect()
    }

    /// Single-shard dispatch: every key is owned by one shard. Replicates the
    /// production pipeline — arity gate, then exec, then the merge (which
    /// reshapes the data report into the final reply).
    type Dispatch = (
        fn(&mut OpContext) -> CmdResult,
        usize,
        Vec<usize>,
        Option<MergeFn>,
    );
    fn dispatch(db: &mut DbSlice, argv: &[Vec<u8>]) -> CmdResult {
        let (exec, first_key_idx, owned, merge): Dispatch = match argv[0].as_slice() {
            b"LINSERT" => (exec_linsert, 1, vec![1], None),
            b"LMOVE" => (exec_lmove, 1, vec![1, 2], Some(merge_move)),
            b"RPOPLPUSH" => (exec_rpoplpush, 1, vec![1, 2], Some(merge_move)),
            b"LMPOP" => {
                let n = parse_i64(&argv[1]).unwrap_or(0) as usize;
                (exec_lmpop, 0, (2..2 + n).collect(), Some(merge_lmpop))
            }
            b"BLPOP" => (
                exec_blpop,
                1,
                (1..argv.len() - 1).collect(),
                Some(merge_bpop),
            ),
            b"BRPOP" => (
                exec_brpop,
                1,
                (1..argv.len() - 1).collect(),
                Some(merge_bpop),
            ),
            b"BLMOVE" | b"BRPOPLPUSH" => (exec_blmove, 1, vec![1, 2], Some(merge_move_blocking)),
            b"BLMPOP" => {
                let n = parse_i64(&argv[2]).unwrap_or(0) as usize;
                (exec_blmpop, 0, (3..3 + n).collect(), Some(merge_blmpop))
            }
            _ => panic!("unhandled command {:?}", argv[0]),
        };
        let cmd = match argv[0].as_slice() {
            b"LINSERT" => CMD_LINSERT,
            b"LMOVE" => CMD_LMOVE,
            b"RPOPLPUSH" => CMD_RPOPLPUSH,
            b"LMPOP" => CMD_LMPOP,
            b"BLPOP" => CMD_BLPOP,
            b"BRPOP" => CMD_BRPOP,
            b"BLMOVE" => CMD_BLMOVE,
            b"BRPOPLPUSH" => CMD_BRPOPLPUSH,
            b"BLMPOP" => CMD_BLMPOP,
            _ => panic!("unhandled command"),
        };
        if let Some(m) = cmd.check_arity(argv.len()) {
            return CmdResult::err(m);
        }
        let mut ctx = OpContext {
            db,
            args: argv,
            owned_keys: &owned,
            first_key_idx,
            now_ms: 0,
        };
        let result = exec(&mut ctx);
        if let Some(m) = merge {
            let part = ShardPart {
                shard: 0,
                owned_key_idxs: owned.clone(),
                result,
            };
            return m(&[part], argv, &owned, 0);
        }
        result
    }

    /// Coerce str/byte/vec arguments to `Vec<u8>` for the `run!` macro.
    fn b(s: impl AsRef<[u8]>) -> Vec<u8> {
        s.as_ref().to_vec()
    }

    macro_rules! run {
        ($db:expr, $($arg:expr),+) => {
            dispatch($db, &[$(b($arg)),+])
        };
    }

    fn part(result: CmdResult) -> ShardPart {
        ShardPart {
            shard: 0,
            owned_key_idxs: vec![],
            result,
        }
    }

    #[test]
    fn linsert_basic() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            int(run!(&mut db, "LINSERT", "notfound", "before", "foo", "bar")),
            0
        );

        str_of(&mut db, "notalist", "x");
        let e = err(run!(&mut db, "LINSERT", "notalist", "before", "foo", "bar"));
        assert!(e.starts_with("WRONGTYPE"), "{}", e);

        rpush_of(&mut db, "mylist", &["foo"]);
        assert_eq!(
            int(run!(&mut db, "LINSERT", "mylist", "before", "foo", "bar")),
            2
        );
        assert_eq!(list_of(&mut db, "mylist"), vec!["bar", "foo"]);

        assert_eq!(
            int(run!(&mut db, "LINSERT", "mylist", "after", "foo", "car")),
            3
        );
        assert_eq!(list_of(&mut db, "mylist"), vec!["bar", "foo", "car"]);

        assert_eq!(
            int(run!(
                &mut db, "LINSERT", "mylist", "before", "notfound", "x"
            )),
            -1
        );
        assert_eq!(
            int(run!(&mut db, "LINSERT", "mylist", "after", "notfound", "x")),
            -1
        );

        // Empty element can be inserted and used as a pivot; after the empty
        // element is popped the pivot is gone and the insert reports -1.
        rpush_of(&mut db, "k", &["a"]);
        assert_eq!(int(run!(&mut db, "LINSERT", "k", "before", "a", "")), 2);
        assert_eq!(list_of(&mut db, "k"), vec!["", "a"]);
        let r = run!(&mut db, "LMPOP", "1", "k", "LEFT");
        match r {
            CmdResult::Ok(RespValue::Array(p)) => match &p[1] {
                RespValue::Array(vals) => assert_eq!(strings(vals), vec![""]),
                _ => panic!(),
            },
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
        assert_eq!(int(run!(&mut db, "LINSERT", "k", "before", "", "")), -1);
    }

    #[test]
    fn lmove_single_shard() {
        let mut db = DbSlice::new(0);
        rpush_of(&mut db, "k1", &["1", "2", "3", "4", "5"]);
        rpush_of(&mut db, "k2", &["9"]);

        assert_eq!(
            bulk(run!(&mut db, "LMOVE", "k1", "k2", "LEFT", "RIGHT")),
            "1"
        );
        assert_eq!(list_of(&mut db, "k2"), vec!["9", "1"]);
        assert_eq!(
            bulk(run!(&mut db, "LMOVE", "k1", "k2", "LEFT", "LEFT")),
            "2"
        );
        assert_eq!(list_of(&mut db, "k2"), vec!["2", "9", "1"]);
        assert_eq!(
            bulk(run!(&mut db, "LMOVE", "k1", "k2", "RIGHT", "RIGHT")),
            "5"
        );
        assert_eq!(list_of(&mut db, "k2"), vec!["2", "9", "1", "5"]);
        assert_eq!(list_of(&mut db, "k1"), vec!["3", "4"]);

        // Empty source returns nil and the source key is removed.
        assert_eq!(
            bulk(run!(&mut db, "LMOVE", "k1", "k2", "LEFT", "RIGHT")),
            "3"
        );
        assert_eq!(
            bulk(run!(&mut db, "LMOVE", "k1", "k2", "LEFT", "RIGHT")),
            "4"
        );
        nil(run!(&mut db, "LMOVE", "k1", "k2", "LEFT", "RIGHT"));
        assert!(missing(&mut db, "k1"));

        // Invalid direction argument.
        assert!(err(run!(&mut db, "LMOVE", "k1", "k2", "LEFT", "R")).contains("syntax"));
    }

    #[test]
    fn lmove_same_key_rotates() {
        let mut db = DbSlice::new(0);
        rpush_of(&mut db, "k", &["1", "2", "3", "4", "5"]);
        assert_eq!(bulk(run!(&mut db, "LMOVE", "k", "k", "LEFT", "RIGHT")), "1");
        assert_eq!(list_of(&mut db, "k"), vec!["2", "3", "4", "5", "1"]);
        assert_eq!(bulk(run!(&mut db, "LMOVE", "k", "k", "RIGHT", "LEFT")), "1");
        assert_eq!(list_of(&mut db, "k"), vec!["1", "2", "3", "4", "5"]);
    }

    #[test]
    fn rpoplpush_single_shard() {
        let mut db = DbSlice::new(0);
        rpush_of(&mut db, "k1", &["1", "2", "3", "4"]);
        assert_eq!(bulk(run!(&mut db, "RPOPLPUSH", "k1", "k2")), "4");
        assert_eq!(list_of(&mut db, "k2"), vec!["4"]);
        assert_eq!(bulk(run!(&mut db, "RPOPLPUSH", "k1", "k2")), "3");
        assert_eq!(list_of(&mut db, "k2"), vec!["3", "4"]);
        assert_eq!(bulk(run!(&mut db, "RPOPLPUSH", "k1", "k2")), "2");
        assert_eq!(bulk(run!(&mut db, "RPOPLPUSH", "k1", "k2")), "1");
        nil(run!(&mut db, "RPOPLPUSH", "k1", "k2"));
        assert_eq!(list_of(&mut db, "k2"), vec!["1", "2", "3", "4"]);
        assert!(missing(&mut db, "k1"));

        // Wrong-type destination shadows the result even when the source is empty.
        str_of(&mut db, "k3", "str");
        let e = err(run!(&mut db, "RPOPLPUSH", "k1", "k3"));
        assert!(e.starts_with("WRONGTYPE"), "{}", e);
    }

    #[test]
    fn lmpop_invalid_syntax() {
        let mut db = DbSlice::new(0);
        assert!(err(run!(&mut db, "LMPOP", "1", "a")).contains("wrong number of arguments"));
        assert!(
            err(run!(&mut db, "LMPOP", "0", "LEFT", "COUNT", "1")).contains("at least 1 input key")
        );
        assert!(err(run!(&mut db, "LMPOP", "aa", "a", "LEFT")).contains("not an integer"));
        assert!(err(run!(&mut db, "LMPOP", "1", "a", "COUNT", "1")).contains("syntax"));
        assert!(err(run!(&mut db, "LMPOP", "1", "a", "b", "LEFT")).contains("syntax"));
        assert!(err(run!(&mut db, "LMPOP", "1", "a", "LEFT", "COUNT")).contains("syntax"));
        assert!(
            err(run!(&mut db, "LMPOP", "1", "a", "LEFT", "COUNT", "boo"))
                .contains("not an integer")
        );
        assert!(
            err(run!(
                &mut db, "LMPOP", "1", "c", "LEFT", "COUNT", "2", "foo"
            ))
            .contains("syntax")
        );
    }

    #[test]
    fn lmpop_basic() {
        let mut db = DbSlice::new(0);
        nil(run!(&mut db, "LMPOP", "1", "e", "LEFT"));

        rpush_of(&mut db, "a", &["a1", "a2"]);
        let r = run!(&mut db, "LMPOP", "1", "a", "LEFT");
        match r {
            CmdResult::Ok(RespValue::Array(p)) => {
                assert_eq!(strings(&p[..1]), vec!["a"]);
                match &p[1] {
                    RespValue::Array(vals) => assert_eq!(strings(vals), vec!["a1"]),
                    _ => panic!("expected values array"),
                }
            }
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }

        rpush_of(&mut db, "b", &["b1", "b2"]);
        let r = run!(&mut db, "LMPOP", "1", "b", "RIGHT");
        match r {
            CmdResult::Ok(RespValue::Array(p)) => {
                assert_eq!(strings(&p[..1]), vec!["b"]);
                match &p[1] {
                    RespValue::Array(vals) => assert_eq!(strings(vals), vec!["b2"]),
                    _ => panic!("expected values array"),
                }
            }
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }

        // COUNT > 1 and COUNT > len.
        rpush_of(&mut db, "c", &["c1", "c2"]);
        let r = run!(&mut db, "LMPOP", "1", "c", "RIGHT", "COUNT", "2");
        match r {
            CmdResult::Ok(RespValue::Array(p)) => match &p[1] {
                RespValue::Array(vals) => assert_eq!(strings(vals), vec!["c2", "c1"]),
                _ => panic!(),
            },
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
        assert!(missing(&mut db, "c"));

        rpush_of(&mut db, "d", &["d1", "d2"]);
        let r = run!(&mut db, "LMPOP", "1", "d", "RIGHT", "COUNT", "3");
        match r {
            CmdResult::Ok(RespValue::Array(p)) => match &p[1] {
                RespValue::Array(vals) => assert_eq!(strings(vals), vec!["d2", "d1"]),
                _ => panic!(),
            },
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
        assert!(missing(&mut db, "d"));

        // First non-empty list wins.
        rpush_of(&mut db, "x", &["x1"]);
        rpush_of(&mut db, "y", &["y1"]);
        let r = run!(&mut db, "LMPOP", "3", "empty", "x", "y", "RIGHT");
        match r {
            CmdResult::Ok(RespValue::Array(p)) => {
                assert_eq!(strings(&p[..1]), vec!["x"]);
                match &p[1] {
                    RespValue::Array(vals) => assert_eq!(strings(vals), vec!["x1"]),
                    _ => panic!(),
                }
            }
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
        assert!(missing(&mut db, "x"));
    }

    #[test]
    fn lmpop_count_zero() {
        let mut db = DbSlice::new(0);
        rpush_of(&mut db, "list", &["a", "b"]);
        // COUNT 0 pops nothing and replies [key, []].
        let r = run!(&mut db, "LMPOP", "1", "list", "LEFT", "COUNT", "0");
        match r {
            CmdResult::Ok(RespValue::Array(p)) => {
                assert_eq!(strings(&p[..1]), vec!["list"]);
                match &p[1] {
                    RespValue::Array(vals) => assert!(vals.is_empty()),
                    _ => panic!(),
                }
            }
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
        assert_eq!(list_of(&mut db, "list"), vec!["a", "b"]);
        // Negative COUNT is rejected.
        assert!(
            err(run!(&mut db, "LMPOP", "1", "list", "LEFT", "COUNT", "-1"))
                .contains("not an integer")
        );
    }

    #[test]
    fn lmpop_wrong_type() {
        let mut db = DbSlice::new(0);
        rpush_of(&mut db, "l1", &["e1"]);
        str_of(&mut db, "foo", "v");

        // First key wrong type -> error.
        let e = err(run!(&mut db, "LMPOP", "2", "foo", "l1", "left"));
        assert!(e.starts_with("WRONGTYPE"), "{}", e);
        // Second key wrong type, first missing -> error.
        let e = err(run!(&mut db, "LMPOP", "2", "nonexistent", "foo", "left"));
        assert!(e.starts_with("WRONGTYPE"), "{}", e);
        // Wrong type after a valid list -> the list wins.
        let r = run!(&mut db, "LMPOP", "2", "l1", "foo", "left");
        match r {
            CmdResult::Ok(RespValue::Array(p)) => {
                assert_eq!(strings(&p[..1]), vec!["l1"]);
                match &p[1] {
                    RespValue::Array(vals) => assert_eq!(strings(vals), vec!["e1"]),
                    _ => panic!(),
                }
            }
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
    }

    #[test]
    fn blmpop_invalid_syntax() {
        let mut db = DbSlice::new(0);
        assert!(
            err(run!(&mut db, "BLMPOP", "0.1", "1", "k")).contains("wrong number of arguments")
        );
        assert!(
            err(run!(
                &mut db, "BLMPOP", "foo", "1", "k1", "LEFT", "COUNT", "1"
            ))
            .contains("not a float")
        );
        assert!(
            err(run!(
                &mut db, "BLMPOP", "-0.01", "1", "k1", "LEFT", "COUNT", "1"
            ))
            .contains("negative")
        );
        assert!(
            err(run!(&mut db, "BLMPOP", "0.01", "0", "LEFT", "COUNT", "1"))
                .contains("at least 1 input key")
        );
        assert!(
            err(run!(&mut db, "BLMPOP", "0.01", "aa", "k1", "LEFT")).contains("not an integer")
        );
        assert!(err(run!(&mut db, "BLMPOP", "0.01", "1", "k1", "COUNT", "1")).contains("syntax"));
        assert!(err(run!(&mut db, "BLMPOP", "0.01", "1", "k1", "k2", "LEFT")).contains("syntax"));
        assert!(
            err(run!(&mut db, "BLMPOP", "0.01", "1", "k1", "LEFT", "COUNT")).contains("syntax")
        );
        assert!(
            err(run!(
                &mut db, "BLMPOP", "0.01", "1", "k1", "LEFT", "COUNT", "boo"
            ))
            .contains("not an integer")
        );
        assert!(
            err(run!(
                &mut db, "BLMPOP", "0.01", "1", "c", "LEFT", "COUNT", "2", "foo"
            ))
            .contains("syntax")
        );
    }

    #[test]
    fn blocking_timeout_validation() {
        let mut db = DbSlice::new(0);
        assert!(err(run!(&mut db, "BRPOPLPUSH", "x", "y", "abc")).contains("not a float"));
        assert!(err(run!(&mut db, "BRPOPLPUSH", "x", "y", "nan")).contains("not a float"));
        assert!(err(run!(&mut db, "BRPOPLPUSH", "x", "y", "inf")).contains("out of range"));
        assert!(err(run!(&mut db, "BRPOPLPUSH", "x", "y", "-inf")).contains("negative"));
        assert!(err(run!(&mut db, "BRPOPLPUSH", "x", "y", "-1")).contains("negative"));

        assert!(
            err(run!(&mut db, "BLMOVE", "x", "y", "LEFT", "RIGHT", "abc")).contains("not a float")
        );
        assert!(
            err(run!(&mut db, "BLMOVE", "x", "y", "LEFT", "RIGHT", "1e10"))
                .contains("out of range")
        );

        assert!(err(run!(&mut db, "BLMPOP", "abc", "1", "k", "LEFT")).contains("not a float"));
        assert!(err(run!(&mut db, "BLMPOP", "1e10", "1", "k", "LEFT")).contains("out of range"));

        assert!(err(run!(&mut db, "BLPOP", "k", "abc")).contains("not a float"));
        assert!(err(run!(&mut db, "BLPOP", "k", "nan")).contains("not a float"));
        assert!(err(run!(&mut db, "BLPOP", "k", "inf")).contains("out of range"));
        assert!(err(run!(&mut db, "BLPOP", "k", "-inf")).contains("negative"));
        assert!(err(run!(&mut db, "BLPOP", "k", "-1")).contains("negative"));
        assert!(err(run!(&mut db, "BLPOP", "k", "1e10")).contains("out of range"));

        // A large-but-representable timeout is accepted.
        rpush_of(&mut db, "k", &["v"]);
        let r = run!(&mut db, "BLPOP", "k", "4000000");
        match r {
            CmdResult::Ok(RespValue::Array(p)) => assert_eq!(strings(&p), vec!["k", "v"]),
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
    }

    #[test]
    fn blocking_returns_blocked_on_empty() {
        let mut db = DbSlice::new(0);
        blocked(run!(&mut db, "BLPOP", "x", "0"));
        blocked(run!(&mut db, "BRPOP", "x", "y", "0"));
        blocked(run!(&mut db, "BLMPOP", "0", "1", "x", "LEFT"));
        blocked(run!(&mut db, "BLMPOP", "0", "2", "x", "y", "LEFT"));
        blocked(run!(&mut db, "BLMOVE", "x", "y", "LEFT", "RIGHT", "0"));
        blocked(run!(&mut db, "BRPOPLPUSH", "x", "y", "0"));
        blocked(run!(&mut db, "BLMOVE", "x", "x", "LEFT", "RIGHT", "0"));
    }

    #[test]
    fn blpop_wrong_type_errors() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "z", "1");
        let e = err(run!(&mut db, "BLPOP", "z", "0"));
        assert!(e.starts_with("WRONGTYPE"), "{}", e);
    }

    #[test]
    fn blmpop_wrong_type_nils() {
        let mut db = DbSlice::new(0);
        str_of(&mut db, "z", "1");
        // BLMPOP returns nil (not an error) for a wrong-type key.
        nil(run!(&mut db, "BLMPOP", "0.01", "1", "z", "LEFT"));
    }

    #[test]
    fn blpop_nonblocking_when_data() {
        let mut db = DbSlice::new(0);
        rpush_of(&mut db, "x", &["a", "b", "c"]);
        let r = run!(&mut db, "BLPOP", "x", "0");
        match r {
            CmdResult::Ok(RespValue::Array(p)) => assert_eq!(strings(&p), vec!["x", "a"]),
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
        let r = run!(&mut db, "BRPOP", "x", "0");
        match r {
            CmdResult::Ok(RespValue::Array(p)) => assert_eq!(strings(&p), vec!["x", "c"]),
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
    }

    #[test]
    fn blmove_with_data_single_shard() {
        let mut db = DbSlice::new(0);
        rpush_of(&mut db, "x", &["val1"]);
        rpush_of(&mut db, "y", &["val2"]);
        assert_eq!(
            bulk(run!(&mut db, "BLMOVE", "x", "y", "right", "left", "0.01")),
            "val1"
        );
        assert_eq!(list_of(&mut db, "y"), vec!["val1", "val2"]);
        assert!(missing(&mut db, "x"));

        // Wrong-type destination.
        rpush_of(&mut db, "s", &["v"]);
        str_of(&mut db, "t", "str");
        let e = err(run!(&mut db, "BRPOPLPUSH", "s", "t", "0.01"));
        assert!(e.starts_with("WRONGTYPE"), "{}", e);
    }

    #[test]
    fn blmpop_with_data() {
        let mut db = DbSlice::new(0);
        rpush_of(&mut db, "k1", &["1", "2", "3", "4"]);
        // k2 (index 3) empty, k1 (index 4) has data: first non-empty wins.
        let r = run!(&mut db, "BLMPOP", "0.01", "2", "k2", "k1", "LEFT");
        match r {
            CmdResult::Ok(RespValue::Array(p)) => {
                assert_eq!(strings(&p[..1]), vec!["k1"]);
                match &p[1] {
                    RespValue::Array(vals) => assert_eq!(strings(vals), vec!["1"]),
                    _ => panic!(),
                }
            }
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
        let r = run!(&mut db, "BLMPOP", "0.01", "1", "k1", "RIGHT", "COUNT", "2");
        match r {
            CmdResult::Ok(RespValue::Array(p)) => match &p[1] {
                RespValue::Array(vals) => assert_eq!(strings(vals), vec!["4", "3"]),
                _ => panic!(),
            },
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
        let r = run!(&mut db, "BLMPOP", "0.01", "1", "k1", "RIGHT", "COUNT", "10");
        match r {
            CmdResult::Ok(RespValue::Array(p)) => match &p[1] {
                RespValue::Array(vals) => assert_eq!(strings(vals), vec!["2"]),
                _ => panic!(),
            },
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
    }

    // ------------------------------------------------------------------
    // Multi-shard merge tests (parts come from different shards).
    // ------------------------------------------------------------------

    #[test]
    fn merge_lmpop_multi_shard() {
        let args: Vec<Vec<u8>> = vec![
            b"LMPOP".to_vec(),
            b"3".to_vec(),
            b"e".to_vec(),
            b"x".to_vec(),
            b"y".to_vec(),
            b"LEFT".to_vec(),
        ];
        let keys = vec![2usize, 3, 4];
        let parts = vec![
            part(CmdResult::Ok(RespValue::Nil)), // shard with only empty keys
            part(CmdResult::Ok(RespValue::Array(vec![
                integer(3),
                RespValue::Array(vec![RespValue::Bulk(b"x1".to_vec())]),
                RespValue::Array(vec![]),
            ]))),
            part(CmdResult::Blocked),
        ];
        match merge_lmpop(&parts, &args, &keys, 0) {
            CmdResult::DeferredStores { stores, reply } => {
                assert_eq!(stores.len(), 1);
                assert_eq!(stores[0].0, b"x".to_vec());
                assert!(stores[0].1.is_none()); // key emptied -> delete
                match reply {
                    RespValue::Array(p) => {
                        assert_eq!(strings(&p[..1]), vec!["x"]);
                        match &p[1] {
                            RespValue::Array(vals) => assert_eq!(strings(vals), vec!["x1"]),
                            _ => panic!(),
                        }
                    }
                    _ => panic!("unexpected reply {reply:?}"),
                }
            }
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
    }

    #[test]
    fn merge_bpop_multi_shard() {
        let args: Vec<Vec<u8>> = vec![
            b"BLPOP".to_vec(),
            b"e".to_vec(),
            b"x".to_vec(),
            b"0".to_vec(),
        ];
        let keys = vec![1usize, 2];
        let parts = vec![
            part(CmdResult::Blocked), // empty shard
            part(CmdResult::Ok(RespValue::Array(vec![
                integer(2),
                RespValue::Array(vec![RespValue::Bulk(b"v".to_vec())]),
                RespValue::Array(vec![RespValue::Bulk(b"rest".to_vec())]),
            ]))),
        ];
        match merge_bpop(&parts, &args, &keys, 0) {
            CmdResult::DeferredStores { stores, reply } => {
                assert_eq!(stores.len(), 1);
                assert_eq!(stores[0].0, b"x".to_vec());
                assert!(stores[0].1.is_some());
                match reply {
                    RespValue::Array(p) => assert_eq!(strings(&p), vec!["x", "v"]),
                    _ => panic!("unexpected reply {reply:?}"),
                }
            }
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
    }

    #[test]
    fn merge_blmpop_wrong_type_nils() {
        let args: Vec<Vec<u8>> = vec![
            b"BLMPOP".to_vec(),
            b"0".to_vec(),
            b"1".to_vec(),
            b"z".to_vec(),
            b"LEFT".to_vec(),
        ];
        let keys = vec![3usize];
        // Wrong-type key report `[key_idx]` and an empty-list shard report.
        let parts = vec![
            part(CmdResult::Ok(RespValue::Array(vec![integer(3)]))),
            part(CmdResult::Blocked),
        ];
        nil(merge_blmpop(&parts, &args, &keys, 0));
    }

    #[test]
    fn merge_lmpop_wrong_type_errors() {
        let args: Vec<Vec<u8>> = vec![
            b"LMPOP".to_vec(),
            b"2".to_vec(),
            b"foo".to_vec(),
            b"l1".to_vec(),
            b"left".to_vec(),
        ];
        let keys = vec![2usize, 3];
        let parts = vec![
            part(CmdResult::Ok(RespValue::Array(vec![integer(2)]))), // foo wrong type
            part(CmdResult::Ok(RespValue::Array(vec![
                integer(3),
                RespValue::Array(vec![RespValue::Bulk(b"e1".to_vec())]),
                RespValue::Array(vec![]),
            ]))),
        ];
        let e = err(merge_lmpop(&parts, &args, &keys, 0));
        assert!(e.starts_with("WRONGTYPE"), "{}", e);
    }

    #[test]
    fn merge_move_multi_shard() {
        // src on one shard, dest on another: report then deferred stores.
        let args: Vec<Vec<u8>> = vec![
            b"LMOVE".to_vec(),
            b"src".to_vec(),
            b"dest".to_vec(),
            b"LEFT".to_vec(),
            b"RIGHT".to_vec(),
        ];
        let keys = vec![1usize, 2];
        let parts = vec![
            // src shard: value + remaining
            part(CmdResult::Ok(RespValue::Array(vec![
                integer(1),
                RespValue::Bulk(b"a".to_vec()),
                RespValue::Array(vec![RespValue::Bulk(b"b".to_vec())]),
            ]))),
            // dest shard: existing elements
            part(CmdResult::Ok(RespValue::Array(vec![
                integer(2),
                RespValue::Array(vec![RespValue::Bulk(b"c".to_vec())]),
            ]))),
        ];
        match merge_move(&parts, &args, &keys, 0) {
            CmdResult::DeferredStores { stores, reply } => {
                assert_eq!(stores.len(), 2);
                assert_eq!(stores[0].0, b"src".to_vec());
                assert_eq!(stores[1].0, b"dest".to_vec());
                match &stores[1].1 {
                    Some(PrimeValue::List(l)) => {
                        let v: Vec<String> = l
                            .iter()
                            .map(|i| String::from_utf8_lossy(&i.as_bytes()).into_owned())
                            .collect();
                        assert_eq!(v, vec!["c", "a"]); // pushed RIGHT onto [c]
                    }
                    _ => panic!("expected list store"),
                }
                match reply {
                    RespValue::Bulk(b) => assert_eq!(b, b"a"),
                    _ => panic!("unexpected reply {reply:?}"),
                }
            }
            o => panic!("unexpected {:?}", o.into_resp_value()),
        }
    }
}
