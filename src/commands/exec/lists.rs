use crate::commands::{integer, ok, Command, OpContext, KeyRange, FLAG_DENYOOM, FLAG_FAST, FLAG_READONLY, FLAG_WRITE};
use crate::core::compact::CompactString;
use crate::core::quicklist::{ListItem, QuickList};
use crate::core::PrimeValue;
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::parse_i64;

fn list_mut<'a>(ctx: &'a mut OpContext, key: &[u8]) -> Result<&'a mut QuickList, RespError> {
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::List(l)) => Ok(l),
        Some(_) => Err(RespError::wrong_type()),
        None => Err(RespError::new("ERR no such key")),
    }
}

fn ensure_list<'a>(ctx: &'a mut OpContext, key: &[u8]) -> Result<&'a mut QuickList, RespError> {
    if ctx.db.find(key, ctx.now_ms).is_none() {
        ctx.db.insert(CompactString::from_bytes(key), PrimeValue::List(QuickList::new()));
    }
    list_mut(ctx, key)
}

fn push(ctx: &mut OpContext, front: bool, only_if_exists: bool) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let items: Vec<ListItem> = ctx.args[key_idx + 1..].iter().map(|a| ListItem::from_bytes(a)).collect();
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
        for item in items.into_iter().rev() {
            if front {
                ql.push_back(item);
            } else {
                ql.push_front(item);
            }
        }
        return CmdResult::Ok(integer(ql.len() as i64));
    }
    let ql = match ensure_list(ctx, key) {
        Ok(l) => l,
        Err(e) => return CmdResult::Err(e),
    };
    // iterate args in reverse so the first provided value ends up at the head
    for item in items.into_iter().rev() {
        if front {
            ql.push_back(item);
        } else {
            ql.push_front(item);
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
    let Some(ql) = (match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::List(l)) => Some(l),
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => None,
    }) else {
        return CmdResult::Ok(if with_count { RespValue::Array(vec![]) } else { RespValue::Nil });
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
    let start = match parse_i64(&ctx.args[key_idx + 1]) {
        Some(v) => v,
        None => return CmdResult::Err(RespError::integer()),
    };
    let stop = match parse_i64(&ctx.args[key_idx + 2]) {
        Some(v) => v,
        None => return CmdResult::Err(RespError::integer()),
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
    let idx = match parse_i64(&ctx.args[key_idx + 1]) {
        Some(v) => v,
        None => return CmdResult::Err(RespError::integer()),
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
    let idx = match parse_i64(&ctx.args[key_idx + 1]) {
        Some(v) => v,
        None => return CmdResult::Err(RespError::integer()),
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
    let count = match parse_i64(&ctx.args[key_idx + 1]) {
        Some(v) => v,
        None => return CmdResult::Err(RespError::integer()),
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
    let start = match parse_i64(&ctx.args[key_idx + 1]) {
        Some(v) => v,
        None => return CmdResult::Err(RespError::integer()),
    };
    let stop = match parse_i64(&ctx.args[key_idx + 2]) {
        Some(v) => v,
        None => return CmdResult::Err(RespError::integer()),
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
                    return CmdResult::Err(RespError::new("ERR RANK can't be zero. Use 1 to start searching from the first match or -1 to start searching from the last match."));
                }
            }
            b"COUNT" => {
                let c = match parse_i64(&ctx.args[i + 1]) {
                    Some(v) => v,
                    None => return CmdResult::Err(RespError::integer()),
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
            let mut seen = 0usize;
            let skip = rank.unsigned_abs().saturating_sub(1) as usize;
            for (pos, item) in items.iter().enumerate() {
                if seen >= maxlen {
                    break;
                }
                seen += 1;
                if item.as_bytes() == *value {
                    if matches.len() >= skip {
                        matches.push(pos as i64);
                        if count.map(|c| matches.len() as i64 >= c).unwrap_or(false) {
                            break;
                        }
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
