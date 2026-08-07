use std::ops::Bound;

use crate::commands::{
    Command, FLAG_BLOCKING, FLAG_DENYOOM, FLAG_FAST, FLAG_MOVABLEKEYS, FLAG_MULTI_KEY,
    FLAG_NO_AUTOJOURNAL, FLAG_READONLY, FLAG_WRITE, KeyRange, OpContext, ShardPart, bulk, integer,
};
use crate::core::PrimeValue;
use crate::core::compact::CompactString;
use crate::core::stream::{
    GroupReadErr, PendingEntry, Stream, StreamId, SCG_INVALID_ENTRIES_READ, SCG_INVALID_LAG,
};
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::parse_i64;
use crate::util::parse_u64;

fn stream_mut<'a>(ctx: &'a mut OpContext, key: &[u8]) -> Result<&'a mut Stream, RespError> {
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Stream(s)) => Ok(s),
        Some(_) => Err(RespError::wrong_type()),
        None => Err(RespError::new("ERR no such key")),
    }
}

fn ensure_stream<'a>(ctx: &'a mut OpContext, key: &[u8]) -> Result<&'a mut Stream, RespError> {
    if ctx.db.find(key, ctx.now_ms).is_none() {
        ctx.db.insert(key, PrimeValue::Stream(Stream::new()));
    }
    stream_mut(ctx, key)
}

fn parse_stream_id_literal(s: &[u8], missing_seq: u64) -> Result<StreamId, RespError> {
    if s == b"*" {
        return Err(RespError::new(
            "ERR Invalid stream ID specified as stream command argument",
        ));
    }
    if let Some(idx) = s.iter().position(|&b| b == b'-') {
        let ms = parse_u64(&s[..idx]).ok_or_else(|| {
            RespError::new("ERR Invalid stream ID specified as stream command argument")
        })?;
        let seq = parse_u64(&s[idx + 1..]).ok_or_else(|| {
            RespError::new("ERR Invalid stream ID specified as stream command argument")
        })?;
        Ok(StreamId { ms, seq })
    } else {
        // Bare "<ms>" is shorthand for "<ms>-<missing_seq>".
        let ms = parse_u64(s).ok_or_else(|| {
            RespError::new("ERR Invalid stream ID specified as stream command argument")
        })?;
        Ok(StreamId {
            ms,
            seq: missing_seq,
        })
    }
}

fn next_star_id(last: StreamId, now_ms: u64) -> StreamId {
    if now_ms > last.ms {
        StreamId { ms: now_ms, seq: 0 }
    } else {
        StreamId {
            ms: last.ms,
            seq: last.seq + 1,
        }
    }
}

fn parse_xadd_id(s: &[u8], last: StreamId, now_ms: u64) -> Result<StreamId, RespError> {
    if s == b"*" {
        return Ok(next_star_id(last, now_ms));
    }
    let invalid = || RespError::new("ERR Invalid stream ID specified as stream command argument");
    let (ms, seq_str) = match s.iter().position(|&b| b == b'-') {
        // A missing sequence part is auto-completed (`xadd key 5` == `5-*`).
        None => (parse_u64(s).ok_or_else(invalid)?, None),
        Some(idx) => {
            let ms = if &s[..idx] == b"*" {
                last.ms
            } else {
                parse_u64(&s[..idx]).ok_or_else(invalid)?
            };
            (ms, Some(&s[idx + 1..]))
        }
    };
    let seq_auto = seq_str.map_or(true, |s| s == b"*");
    if seq_auto {
        if ms < last.ms {
            return Err(RespError::new(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
            ));
        }
        return Ok(if ms == last.ms {
            StreamId { ms, seq: last.seq + 1 }
        } else {
            StreamId { ms, seq: 0 }
        });
    }
    let seq = parse_u64(seq_str.unwrap()).ok_or_else(invalid)?;
    let id = StreamId { ms, seq };
    if id <= last {
        return Err(RespError::new(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        ));
    }
    Ok(id)
}

fn render_id(id: &StreamId) -> Vec<u8> {
    format!("{}-{}", id.ms, id.seq).into_bytes()
}

// ---------------------------------------------------------------------------
// XADD
// ---------------------------------------------------------------------------

fn parse_xadd(ctx: &mut OpContext) -> Result<XAddArgs, RespError> {
    let mut nomkstream = false;
    let mut maxlen: Option<u64> = None;
    let mut minid: Option<StreamId> = None;
    let mut approx = false;
    let mut limit: Option<u64> = None;
    let mut i = ctx.owned_keys[0] + 1;
    loop {
        if i >= ctx.args.len() {
            return Err(RespError::syntax());
        }
        match ctx.args[i].to_ascii_uppercase().as_slice() {
            b"NOMKSTREAM" => {
                nomkstream = true;
                i += 1;
            }
            b"MAXLEN" | b"MINID" => {
                if maxlen.is_some() || minid.is_some() {
                    return Err(RespError::new(
                        "ERR MAXLEN and MINID options at the same time are not compatible",
                    ));
                }
                let is_maxlen = ctx.args[i].eq_ignore_ascii_case(b"MAXLEN");
                i += 1;
                if i < ctx.args.len()
                    && (ctx.args[i] == b"~".to_vec() || ctx.args[i] == b"=".to_vec())
                {
                    approx = ctx.args[i] == b"~".to_vec();
                    i += 1;
                }
                if i >= ctx.args.len() {
                    return Err(RespError::syntax());
                }
                if is_maxlen {
                    maxlen = Some(match parse_u64(&ctx.args[i]) {
                        Some(v) => v,
                        None => return Err(RespError::integer()),
                    });
                } else {
                    minid = Some(match parse_stream_id_literal(&ctx.args[i], 0) {
                        Ok(v) => v,
                        Err(e) => return Err(e),
                    });
                }
                i += 1;
                if i < ctx.args.len() && ctx.args[i].eq_ignore_ascii_case(b"LIMIT") {
                    if !approx {
                        return Err(RespError::syntax());
                    }
                    i += 1;
                    if i >= ctx.args.len() {
                        return Err(RespError::syntax());
                    }
                    limit = Some(match parse_u64(&ctx.args[i]) {
                        Some(v) => v,
                        None => return Err(RespError::integer()),
                    });
                    i += 1;
                }
            }
            _ => break,
        }
    }
    Ok(XAddArgs {
        id_args_idx: i,
        nomkstream,
        maxlen,
        minid,
        approx,
        limit,
    })
}

struct XAddArgs {
    /// Index of the ID argument (start of field-value pairs).
    id_args_idx: usize,
    nomkstream: bool,
    maxlen: Option<u64>,
    minid: Option<StreamId>,
    approx: bool,
    limit: Option<u64>,
}

fn exec_xadd(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let parsed = match parse_xadd(ctx) {
        Ok(p) => p,
        Err(e) => return CmdResult::Err(e),
    };
    let fvs = &ctx.args[parsed.id_args_idx..];
    if fvs.len() < 2 || !(fvs.len() - 1).is_multiple_of(2) {
        return CmdResult::Err(RespError::new(
            "ERR wrong number of arguments for 'xadd' command",
        ));
    }
    let id_arg = fvs[0].as_slice();

    let key_exists = ctx.db.find(key, ctx.now_ms).is_some();
    if parsed.nomkstream && !key_exists {
        return CmdResult::Ok(RespValue::Nil);
    }
    let now = ctx.now_ms;
    let s = match ensure_stream(ctx, key) {
        Ok(s) => s,
        Err(e) => return CmdResult::Err(e),
    };
    let id = match parse_xadd_id(id_arg, s.last_id, now) {
        Ok(id) => id,
        Err(e) => {
            // A failed XADD must not leave a freshly created stream behind.
            if !key_exists {
                ctx.db.remove(key);
            }
            return CmdResult::Err(e);
        }
    };
    let mut fields = Vec::with_capacity(fvs.len() - 1);
    for pair in fvs[1..].chunks(2) {
        fields.push((
            CompactString::from_bytes(&pair[0]),
            CompactString::from_bytes(&pair[1]),
        ));
    }
    s.append(id, fields);
    if parsed.maxlen.is_some() || parsed.minid.is_some() {
        let limit = if parsed.approx {
            parsed.limit.or(Some(100 * crate::core::stream::NODE_MAX_ENTRIES as u64))
        } else {
            parsed.limit
        };
        s.trim(parsed.maxlen, parsed.minid, parsed.approx, limit);
    }
    CmdResult::Ok(bulk(render_id(&id)))
}

// ---------------------------------------------------------------------------
// XLEN / XRANGE / XREVRANGE / XDEL / XTRIM
// ---------------------------------------------------------------------------

fn exec_xlen(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Stream(s)) => CmdResult::Ok(integer(s.len() as i64)),
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

fn parse_range_bound(s: &[u8], is_end: bool) -> Result<StreamId, RespError> {
    if s == b"-" {
        Ok(StreamId::MIN)
    } else if s == b"+" {
        Ok(StreamId::MAX)
    } else {
        parse_stream_id_literal(s, if is_end { u64::MAX } else { 0 })
    }
}

fn parse_count(args: &[Vec<u8>], mut i: usize, end: usize) -> Result<Option<usize>, RespError> {
    let mut count = None;
    while i < end {
        if args[i].eq_ignore_ascii_case(b"COUNT") {
            if i + 1 >= end {
                return Err(RespError::syntax());
            }
            let c = parse_i64(&args[i + 1]).ok_or_else(RespError::integer)?;
            count = Some(c.max(0) as usize);
            i += 2;
        } else {
            return Err(RespError::syntax());
        }
    }
    Ok(count)
}

fn entry_to_resp(eid: StreamId, fields: &[(CompactString, CompactString)]) -> RespValue {
    let mut arr = Vec::with_capacity(fields.len() * 2);
    for (f, v) in fields {
        arr.push(RespValue::Bulk(f.as_bytes().to_vec()));
        arr.push(RespValue::Bulk(v.as_bytes().to_vec()));
    }
    RespValue::Array(vec![bulk(render_id(&eid)), RespValue::Array(arr)])
}

fn exec_xrange_common(ctx: &mut OpContext, rev: bool) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let (start_arg, end_arg) = if rev {
        (&ctx.args[key_idx + 2], &ctx.args[key_idx + 1])
    } else {
        (&ctx.args[key_idx + 1], &ctx.args[key_idx + 2])
    };
    let count = match parse_count(ctx.args, key_idx + 3, ctx.args.len()) {
        Ok(c) => c,
        Err(e) => return CmdResult::Err(e),
    };
    let start = match parse_range_bound(start_arg, false) {
        Ok(s) => s,
        Err(e) => return CmdResult::Err(e),
    };
    let end = match parse_range_bound(end_arg, true) {
        Ok(s) => s,
        Err(e) => return CmdResult::Err(e),
    };
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Stream(s)) => {
            let mut out = Vec::new();
            let it: Box<dyn Iterator<Item = (&StreamId, &crate::core::stream::StreamEntry)>> =
                if rev {
                    Box::new(s.entries.iter().rev())
                } else {
                    Box::new(s.entries.iter())
                };
            for (eid, e) in it {
                if *eid < start || *eid > end || e.deleted {
                    continue;
                }
                out.push(entry_to_resp(*eid, &e.fields));
                if count.is_some_and(|c| out.len() >= c) {
                    break;
                }
            }
            CmdResult::Ok(RespValue::Array(out))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(RespValue::Array(vec![])),
    }
}

fn exec_xrange(ctx: &mut OpContext) -> CmdResult {
    exec_xrange_common(ctx, false)
}
fn exec_xrevrange(ctx: &mut OpContext) -> CmdResult {
    exec_xrange_common(ctx, true)
}

fn exec_xdel(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let mut ids = Vec::new();
    for a in &ctx.args[key_idx + 1..] {
        match parse_stream_id_literal(a, 0) {
            Ok(id) => ids.push(id),
            Err(e) => return CmdResult::Err(e),
        }
    }
    let s = match stream_mut(ctx, key) {
        Ok(s) => s,
        Err(e) => {
            if e.message.starts_with("WRONGTYPE") {
                return CmdResult::Err(e);
            }
            return CmdResult::Ok(integer(0));
        }
    };
    let mut removed = 0i64;
    for id in ids {
        if s.delete(id) {
            removed += 1;
        }
    }
    CmdResult::Ok(integer(removed))
}

fn parse_trim_marker(args: &[Vec<u8>], i: &mut usize) -> bool {
    let approx = args.get(*i).is_some_and(|a| a == b"~");
    if approx || args.get(*i).is_some_and(|a| a == b"=") {
        *i += 1;
    }
    approx
}

fn exec_xtrim(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    if ctx.args.len() < key_idx + 3 {
        return CmdResult::Err(RespError::syntax());
    }
    let kind = ctx.args[key_idx + 1].to_ascii_uppercase();
    let mut i = key_idx + 2;
    let (maxlen, minid, approx) = match kind.as_slice() {
        b"MAXLEN" => {
            let approx = parse_trim_marker(ctx.args, &mut i);
            if i >= ctx.args.len() {
                return CmdResult::Err(RespError::syntax());
            }
            let maxlen = match parse_u64(&ctx.args[i]) {
                Some(v) => v,
                None => return CmdResult::Err(RespError::integer()),
            };
            i += 1;
            (Some(maxlen), None, approx)
        }
        b"MINID" => {
            let approx = parse_trim_marker(ctx.args, &mut i);
            if i >= ctx.args.len() {
                return CmdResult::Err(RespError::syntax());
            }
            let minid = match parse_stream_id_literal(&ctx.args[i], 0) {
                Ok(v) => v,
                Err(_) => return CmdResult::Err(RespError::syntax()),
            };
            i += 1;
            (None, Some(minid), approx)
        }
        _ => return CmdResult::Err(RespError::syntax()),
    };
    let mut limit: Option<u64> = None;
    if i < ctx.args.len() && ctx.args[i].eq_ignore_ascii_case(b"LIMIT") {
        if !approx {
            return CmdResult::Err(RespError::syntax());
        }
        i += 1;
        if i >= ctx.args.len() {
            return CmdResult::Err(RespError::syntax());
        }
        limit = Some(match parse_u64(&ctx.args[i]) {
            Some(v) => v,
            None => return CmdResult::Err(RespError::integer()),
        });
        i += 1;
    }
    if i < ctx.args.len()
        && (ctx.args[i].eq_ignore_ascii_case(b"MAXLEN")
            || ctx.args[i].eq_ignore_ascii_case(b"MINID"))
    {
        return CmdResult::Err(RespError::new(
            "ERR MAXLEN and MINID options at the same time are not compatible",
        ));
    }
    if i != ctx.args.len() {
        return CmdResult::Err(RespError::syntax());
    }
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Stream(s)) => {
            let limit = if approx {
                limit.or(Some(100 * crate::core::stream::NODE_MAX_ENTRIES as u64))
            } else {
                limit
            };
            let removed = s.trim(maxlen, minid, approx, limit);
            CmdResult::Ok(integer(removed as i64))
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

// ---------------------------------------------------------------------------
// XREAD
// ---------------------------------------------------------------------------

struct XReadArgs {
    count: Option<usize>,
    block_ms: Option<u64>,
    /// indices of keys in args
    key_idxs: Vec<usize>,
    /// id args, parallel to `key_idxs`
    id_args: Vec<Vec<u8>>,
}

fn parse_xread_args(ctx: &OpContext) -> Result<XReadArgs, RespError> {
    let args = ctx.args;
    let mut count = None;
    let mut block_ms = None;
    let mut i = 1;
    let mut streams_idx = None;
    while i < args.len() {
        let t = args[i].to_ascii_uppercase();
        match t.as_slice() {
            b"COUNT" => {
                if i + 1 >= args.len() {
                    return Err(RespError::syntax());
                }
                count = Some(
                    parse_i64(&args[i + 1])
                        .ok_or_else(RespError::integer)?
                        .max(0) as usize,
                );
                i += 2;
            }
            b"BLOCK" => {
                if i + 1 >= args.len() {
                    return Err(RespError::syntax());
                }
                let ms = parse_i64(&args[i + 1]).ok_or_else(RespError::integer)?;
                if ms < 0 {
                    return Err(RespError::new("ERR timeout is negative"));
                }
                block_ms = Some(ms as u64);
                i += 2;
            }
            b"STREAMS" => {
                streams_idx = Some(i);
                break;
            }
            _ => return Err(RespError::syntax()),
        }
    }
    let Some(si) = streams_idx else {
        return Err(RespError::new("ERR syntax error"));
    };
    let remaining = args.len() - si - 1;
    if remaining == 0 || !remaining.is_multiple_of(2) {
        return Err(RespError::new(
            "ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified",
        ));
    }
    let n = remaining / 2;
    let mut key_idxs = Vec::with_capacity(n);
    let mut id_args = Vec::with_capacity(n);
    for j in 0..n {
        key_idxs.push(si + 1 + j);
        id_args.push(args[si + 1 + n + j].clone());
    }
    Ok(XReadArgs {
        count,
        block_ms,
        key_idxs,
        id_args,
    })
}

/// Read entries after `id` for a single stream key.
fn read_after(
    ctx: &mut OpContext,
    key: &[u8],
    id: StreamId,
    count: Option<usize>,
) -> Result<Vec<(StreamId, Vec<(CompactString, CompactString)>)>, RespError> {
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Stream(s)) => {
            let mut out = Vec::new();
            for (eid, e) in s.entries.range((Bound::Excluded(id), Bound::Unbounded)) {
                if e.deleted {
                    continue;
                }
                out.push((*eid, e.fields.clone()));
                if count.is_some_and(|c| out.len() >= c) {
                    break;
                }
            }
            Ok(out)
        }
        Some(_) => Err(RespError::wrong_type()),
        None => Ok(vec![]),
    }
}

fn exec_xread(ctx: &mut OpContext) -> CmdResult {
    let parsed = match parse_xread_args(ctx) {
        Ok(p) => p,
        Err(e) => return CmdResult::Err(e),
    };
    // Each stream contributes one `[key, [entries]]` pair; the reference wraps
    // each pair in its own array (`StartArray(2)` + `SendStreamRecords`,
    // stream_family.cc:3298), giving `[[key, [entries]], ...]` (RESP2).
    let mut out: Vec<RespValue> = Vec::new();
    let mut any = false;
    for &ki in ctx.owned_keys {
        let key = &ctx.args[ki];
        let pos = parsed.key_idxs.iter().position(|&k| k == ki).unwrap();
        let id_arg = &parsed.id_args[pos];
        let id = if id_arg == b"$" {
            // Resolve $ to the last ID once; a blocking read remembers it in a
            // per-connection watermark so retries continue from the same point.
            match ctx.db.stream_watermark(ctx.conn_id, key) {
                Some(w) => w,
                None => match ctx.db.find(key, ctx.now_ms) {
                    Some(PrimeValue::Stream(s)) => s.last_entry().copied().unwrap_or(StreamId::MIN),
                    Some(_) => return CmdResult::Err(RespError::wrong_type()),
                    None => StreamId::MIN,
                },
            }
        } else if id_arg == b">" {
            return CmdResult::Err(RespError::new(
                "ERR The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option.",
            ));
        } else {
            match parse_stream_id_literal(id_arg, 0) {
                Ok(id) => id,
                Err(e) => return CmdResult::Err(e),
            }
        };
        match read_after(ctx, key, id, parsed.count) {
            Ok(entries) => {
                if !entries.is_empty() {
                    any = true;
                }
                let arr: Vec<RespValue> = entries
                    .into_iter()
                    .map(|(eid, f)| entry_to_resp(eid, &f))
                    .collect();
                out.push(RespValue::Array(vec![
                    RespValue::Bulk(key.clone()),
                    RespValue::Array(arr),
                ]));
            }
            Err(e) => return CmdResult::Err(e),
        }
    }
    if !any {
        // Blocking: remember where this connection's `$` reads resume from and
        // wait for new entries. Non-blocking: the per-key empty pairs flow to
        // `merge_xread`, which replies nil when nothing was read.
        if parsed.block_ms.is_some() {
            for &ki in ctx.owned_keys {
                let key = &ctx.args[ki];
                let pos = parsed.key_idxs.iter().position(|&k| k == ki).unwrap();
                if parsed.id_args[pos] == b"$" {
                    let last = match ctx.db.find(key, ctx.now_ms) {
                        Some(PrimeValue::Stream(s)) => {
                            s.last_entry().copied().unwrap_or(StreamId::MIN)
                        }
                        _ => StreamId::MIN,
                    };
                    ctx.db.set_stream_watermark(ctx.conn_id, key.clone(), last);
                }
            }
            return CmdResult::Blocked;
        }
    }
    if parsed.block_ms.is_some() {
        for &ki in ctx.owned_keys {
            let key = &ctx.args[ki];
            let pos = parsed.key_idxs.iter().position(|&k| k == ki).unwrap();
            if parsed.id_args[pos] == b"$" {
                ctx.db.remove_stream_watermark(ctx.conn_id, key);
            }
        }
    }
    CmdResult::Ok(RespValue::Array(out))
}

fn merge_xread(parts: &[ShardPart], _args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    let mut result: Vec<RespValue> = Vec::new();
    for &ki in keys {
        for p in parts {
            if p.owned_key_idxs.contains(&ki) {
                match &p.result {
                    CmdResult::Ok(RespValue::Array(sub)) => {
                        let pos = p.owned_key_idxs.iter().position(|&k| k == ki).unwrap();
                        match sub.get(pos) {
                            Some(RespValue::Array(pair)) => {
                                // XREAD omits streams with no entries from the
                                // reply when at least one stream returned data.
                                if matches!(pair.get(1), Some(RespValue::Array(v)) if !v.is_empty())
                                {
                                    result.push(RespValue::Array(pair.clone()));
                                }
                            }
                            _ => {
                                return CmdResult::Err(RespError::new(
                                    "ERR internal: bad XREAD shard result",
                                ));
                            }
                        }
                    }
                    CmdResult::Blocked => {
                        // No data for this stream; XREAD omits it.
                    }
                    CmdResult::Err(e) => return CmdResult::Err(e.clone()),
                    _ => {
                        return CmdResult::Err(RespError::new(
                            "ERR internal: bad XREAD shard result",
                        ));
                    }
                }
            }
        }
    }
    if result.is_empty() {
        CmdResult::Ok(RespValue::NilArray)
    } else {
        CmdResult::Ok(RespValue::Array(result))
    }
}

fn merge_xreadgroup(
    parts: &[ShardPart],
    args: &[Vec<u8>],
    keys: &[usize],
    _now: u64,
) -> CmdResult {
    let mut result: Vec<RespValue> = Vec::new();
    let mut any = false;
    for &ki in keys {
        for p in parts {
            if p.owned_key_idxs.contains(&ki) {
                match &p.result {
                    CmdResult::Ok(RespValue::Array(sub)) => {
                        let pos = p.owned_key_idxs.iter().position(|&k| k == ki).unwrap();
                        match sub.get(pos) {
                            Some(RespValue::Array(pair)) => {
                                if matches!(pair.get(1), Some(RespValue::Array(v)) if !v.is_empty())
                                {
                                    any = true;
                                }
                                result.push(RespValue::Array(pair.clone()));
                            }
                            _ => {
                                return CmdResult::Err(RespError::new(
                                    "ERR internal: bad XREADGROUP shard result",
                                ));
                            }
                        }
                    }
                    CmdResult::Blocked => {
                        // No data for this stream: emit an empty `[key, []]`
                        // pair (the reference sends one per requested stream
                        // once any stream carries entries).
                        result.push(RespValue::Array(vec![
                            RespValue::Bulk(args[ki].clone()),
                            RespValue::Array(vec![]),
                        ]));
                    }
                    CmdResult::Err(e) => return CmdResult::Err(e.clone()),
                    _ => {
                        return CmdResult::Err(RespError::new(
                            "ERR internal: bad XREADGROUP shard result",
                        ));
                    }
                }
            }
        }
    }
    if any {
        CmdResult::Ok(RespValue::Array(result))
    } else {
        CmdResult::Ok(RespValue::NilArray)
    }
}

// ---------------------------------------------------------------------------
// XREADGROUP
// ---------------------------------------------------------------------------

fn exec_xreadgroup(ctx: &mut OpContext) -> CmdResult {
    let args = ctx.args;
    if !args[1].eq_ignore_ascii_case(b"GROUP") {
        return CmdResult::Err(RespError::new("ERR Missing 'GROUP' in 'XREADGROUP' command"));
    }
    let mut i = 1;
    let (mut group_name, mut consumer_name) = (None, None);
    let mut count = None;
    let mut block_ms = None;
    let mut noack = false;
    while i < args.len() {
        let t = args[i].to_ascii_uppercase();
        match t.as_slice() {
            b"GROUP" => {
                if i + 2 >= args.len() {
                    return CmdResult::Err(RespError::syntax());
                }
                group_name = Some(args[i + 1].clone());
                consumer_name = Some(args[i + 2].clone());
                i += 3;
            }
            b"COUNT" => {
                if i + 1 >= args.len() {
                    return CmdResult::Err(RespError::syntax());
                }
                count = Some(
                    match parse_i64(&args[i + 1]) {
                        Some(v) => v,
                        None => return CmdResult::Err(RespError::integer()),
                    }
                    .max(0) as usize,
                );
                i += 2;
            }
            b"BLOCK" => {
                if i + 1 >= args.len() {
                    return CmdResult::Err(RespError::syntax());
                }
                let Some(ms) = parse_i64(&args[i + 1]) else {
                    return CmdResult::Err(RespError::integer());
                };
                if ms < 0 {
                    return CmdResult::Err(RespError::new("ERR timeout is negative"));
                }
                block_ms = Some(ms as u64);
                i += 2;
            }
            b"NOACK" => {
                noack = true;
                i += 1;
            }
            b"STREAMS" => break,
            _ => return CmdResult::Err(RespError::syntax()),
        }
    }
    let (Some(g), Some(c)) = (group_name, consumer_name) else {
        return CmdResult::Err(RespError::syntax());
    };
    if c.is_empty() {
        return CmdResult::Err(RespError::new("ERR consumer name can't be empty"));
    }
    let g = CompactString::from_bytes(&g);
    let c = CompactString::from_bytes(&c);
    if i >= args.len() {
        return CmdResult::Err(RespError::syntax());
    }
    let si = i;
    let remaining = args.len() - si - 1;
    if remaining == 0 || !remaining.is_multiple_of(2) {
        return CmdResult::Err(RespError::new(
            "ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified",
        ));
    }
    let n = remaining / 2;
    let mut key_idxs = Vec::with_capacity(n);
    let mut id_args = Vec::with_capacity(n);
    for j in 0..n {
        key_idxs.push(si + 1 + j);
        id_args.push(args[si + 1 + n + j].clone());
    }

    let mut out: Vec<RespValue> = Vec::new();
    let mut any = false;
    for &ki in ctx.owned_keys {
        let key = &ctx.args[ki];
        let pos = key_idxs.iter().position(|&k| k == ki).unwrap();
        let id_arg = &id_args[pos];
        match ctx.db.find_mut(key, ctx.now_ms) {
            Some(PrimeValue::Stream(s)) => {
                let id = if id_arg == b">" {
                    StreamId { ms: 0, seq: 1 }
                } else if id_arg == b"$" {
                    return CmdResult::Err(RespError::new(
                        "ERR The $ can be specified only when calling XREAD.",
                    ));
                } else {
                    match parse_stream_id_literal(id_arg, 0) {
                        Ok(id) => id,
                        Err(e) => return CmdResult::Err(e),
                    }
                };
                match s.read_group(&g, &c, id, count, noack, ctx.now_ms) {
                    Ok(entries) => {
                        if !entries.is_empty() {
                            any = true;
                        }
                        let arr: Vec<RespValue> = entries
                            .into_iter()
                            .map(|(eid, f)| entry_to_resp(eid, &f))
                            .collect();
                        out.push(RespValue::Array(vec![
                            RespValue::Bulk(key.clone()),
                            RespValue::Array(arr),
                        ]));
                    }
                    Err(GroupReadErr::NoGroup) => {
                        return CmdResult::Err(RespError::new(format!(
                            "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP option",
                            String::from_utf8_lossy(key),
                            String::from_utf8_lossy(g.as_bytes())
                        )));
                    }
                }
            }
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => {
                // A missing key is an error for XREADGROUP (HasEntries2).
                return CmdResult::Err(RespError::new(format!(
                    "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP option",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(g.as_bytes())
                )));
            }
        }
    }
    if block_ms.is_some() && !any {
        return CmdResult::Blocked;
    }
    CmdResult::Ok(RespValue::Array(out))
}

// ---------------------------------------------------------------------------
// XACK / XPENDING / XGROUP / XINFO
// ---------------------------------------------------------------------------

fn exec_xack(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let group = CompactString::from_bytes(&ctx.args[key_idx + 1]);
    let mut ids = Vec::new();
    for a in &ctx.args[key_idx + 2..] {
        match parse_stream_id_literal(a, 0) {
            Ok(id) => ids.push(id),
            Err(e) => return CmdResult::Err(e),
        }
    }
    let s = match stream_mut(ctx, key) {
        Ok(s) => s,
        Err(e) => {
            if e.message.starts_with("WRONGTYPE") {
                return CmdResult::Err(e);
            }
            return CmdResult::Ok(integer(0));
        }
    };
    CmdResult::Ok(integer(s.ack(&group, &ids) as i64))
}

fn exec_xpending(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let group = CompactString::from_bytes(&ctx.args[key_idx + 1]);
    let s = match stream_mut(ctx, key) {
        Ok(s) => s,
        Err(e) => {
            if e.message.starts_with("WRONGTYPE") {
                return CmdResult::Err(e);
            }
            return CmdResult::Err(RespError::no_such_key_or_group(key, group.as_bytes()));
        }
    };
    let Some(grp) = s.group(&group) else {
        return CmdResult::Err(RespError::no_such_key_or_group(key, group.as_bytes()));
    };
    let grp = grp.clone();
    // XPENDING key group [IDLE ms] [start end count [consumer]]
    // ParseXpendingOptions (stream_family.cc:3309) requires the full
    // start/end/count triple whenever any range args are present.
    let mut i = key_idx + 2;
    let mut idle_ms: Option<u64> = None;
    if i < ctx.args.len() && ctx.args[i].eq_ignore_ascii_case(b"IDLE") {
        if i + 3 >= ctx.args.len() {
            return CmdResult::Err(RespError::new(
                "ERR wrong number of arguments for 'xpending' command",
            ));
        }
        idle_ms = Some(
            match parse_i64(&ctx.args[i + 1]) {
                Some(v) => v,
                None => return CmdResult::Err(RespError::integer()),
            }
            .max(0) as u64,
        );
        i += 2;
    }
    if i + 3 <= ctx.args.len() {
        // detailed form: start end count [consumer]
        let start = match parse_range_bound(&ctx.args[i], false) {
            Ok(id) => id,
            Err(e) => return CmdResult::Err(e),
        };
        let end = match parse_range_bound(&ctx.args[i + 1], true) {
            Ok(id) => id,
            Err(e) => return CmdResult::Err(e),
        };
        let count = match parse_i64(&ctx.args[i + 2]) {
            Some(v) => v,
            None => return CmdResult::Err(RespError::integer()),
        }
        .max(0) as usize;
        let consumer_filter = if ctx.args.len() > i + 3 {
            Some(CompactString::from_bytes(&ctx.args[i + 3]))
        } else {
            None
        };
        let now = ctx.now_ms;
        let mut out = Vec::new();
        for (eid, pe) in grp.pel.range(start..=end) {
            if let Some(cf) = &consumer_filter
                && &pe.consumer != cf
            {
                continue;
            }
            if let Some(idle) = idle_ms
                && now.saturating_sub(pe.delivery_time) < idle
            {
                continue;
            }
            out.push(RespValue::Array(vec![
                bulk(render_id(eid)),
                bulk(pe.consumer.as_bytes()),
                integer(now.saturating_sub(pe.delivery_time) as i64),
                integer(pe.delivery_count as i64),
            ]));
            if out.len() >= count {
                break;
            }
        }
        return CmdResult::Ok(RespValue::Array(out));
    }
    if i != ctx.args.len() {
        // start/end without count (or a stray trailing token).
        return CmdResult::Err(RespError::new(
            "ERR wrong number of arguments for 'xpending' command",
        ));
    }
    // summary form: aggregate pending entries per consumer (the reduced
    // result, stream_family.cc PendingReducedResult).
    if grp.pel.is_empty() {
        return CmdResult::Ok(RespValue::Array(vec![
            integer(0),
            RespValue::Nil,
            RespValue::Nil,
            RespValue::Nil,
        ]));
    }
    let mut min_id = StreamId::MAX;
    let mut max_id = StreamId::MIN;
    for eid in grp.pel.keys() {
        if *eid < min_id {
            min_id = *eid;
        }
        if *eid > max_id {
            max_id = *eid;
        }
    }
    let mut per_consumer: std::collections::BTreeMap<&CompactString, u64> =
        std::collections::BTreeMap::new();
    for pe in grp.pel.values() {
        *per_consumer.entry(&pe.consumer).or_insert(0) += 1;
    }
    let details: Vec<RespValue> = per_consumer
        .into_iter()
        .map(|(c, n)| {
            RespValue::Array(vec![bulk(c.as_bytes()), integer(n as i64)])
        })
        .collect();
    CmdResult::Ok(RespValue::Array(vec![
        integer(grp.pel.len() as i64),
        bulk(render_id(&min_id)),
        bulk(render_id(&max_id)),
        RespValue::Array(details),
    ]))
}

fn parse_group_id_arg(arg: &[u8], stream: &Stream) -> Result<StreamId, RespError> {
    if arg == b"$" {
        Ok(stream.last_id)
    } else {
        parse_stream_id_literal(arg, 0)
    }
}

/// `XGROUP HELP` reply text (mirrors `XGroupHelp`, stream_family.cc:2575-2595).
const XGROUP_HELP: [&[u8]; 17] = [
    b"XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"CREATE <key> <groupname> <id|$> [option]",
    b"    Create a new consumer group. Options are:",
    b"    * MKSTREAM",
    b"      Create the empty stream if it does not exist.",
    b"    * ENTRIESREAD entries_read",
    b"      Set the group's entries_read counter (internal use).",
    b"CREATECONSUMER <key> <groupname> <consumer>",
    b"    Create a new consumer in the specified group.",
    b"DELCONSUMER <key> <groupname> <consumer>",
    b"    Remove the specified consumer.",
    b"DESTROY <key> <groupname>",
    b"    Remove the specified group.",
    b"SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
    b"    Set the current group ID and entries_read counter.",
    b"HELP",
    b"    Print this help.",
];

fn exec_xgroup(ctx: &mut OpContext) -> CmdResult {
    // `XGROUP HELP` takes no key. The reference routes it to the hidden
    // `_XGROUP_HELP` command (command_registry.cc:347-352), which is arity-2
    // and NOSCRIPT (so scripts get "not allowed"); the plain XGROUP path here
    // handles both by short-circuiting the keyless form.
    if ctx.args.len() == 2 && ctx.args[1].eq_ignore_ascii_case(b"HELP") {
        return CmdResult::Ok(RespValue::Array(
            XGROUP_HELP.iter().map(|s| bulk(s.to_vec())).collect(),
        ));
    }
    if ctx.owned_keys.is_empty() {
        return CmdResult::Err(RespError::syntax());
    }
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    if ctx.args.len() < key_idx + 2 {
        return CmdResult::Err(RespError::syntax());
    }
    let sub = ctx.args[1].to_ascii_uppercase();
    match sub.as_slice() {
        b"CREATE" => {
            if ctx.args.len() < key_idx + 3 {
                return CmdResult::Err(RespError::syntax());
            }
            let group = CompactString::from_bytes(&ctx.args[key_idx + 1]);
            let id_arg = &ctx.args[key_idx + 2];
            let mkstream = ctx.args.len() > key_idx + 3
                && ctx.args[key_idx + 3].eq_ignore_ascii_case(b"MKSTREAM");
            let key_exists = ctx.db.find(key, ctx.now_ms).is_some();
            let id_is_dollar = id_arg == b"$";
            // Parse the ID before touching the DB (OpCreate, stream_family.cc:
            // 1775-1817): a bad ID must not leave an MKSTREAM-created stream
            // behind, and `$` resolves against the existing stream's last id.
            let id = if id_is_dollar {
                match ctx.db.find(key, ctx.now_ms) {
                    Some(PrimeValue::Stream(s)) => s.last_id,
                    Some(_) => return CmdResult::Err(RespError::wrong_type()),
                    None => StreamId::MIN,
                }
            } else {
                match parse_stream_id_literal(id_arg, 0) {
                    Ok(id) => id,
                    // OpCreate (stream_family.cc:1775-1817) returns SYNTAX_ERR
                    // for a bad id and deletes an MKSTREAM-created stream.
                    Err(_) => return CmdResult::Err(RespError::syntax()),
                }
            };
            if !key_exists && !mkstream {
                return CmdResult::Err(RespError::new(
                    "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
                ));
            }
            if !key_exists {
                ctx.db.insert(key, PrimeValue::Stream(Stream::new()));
            }
            let s = match stream_mut(ctx, key) {
                Ok(s) => s,
                Err(e) => return CmdResult::Err(e),
            };
            match s.create_group(group, id, mkstream, id_is_dollar) {
                Ok(()) => CmdResult::Ok(crate::commands::ok()),
                Err(crate::core::stream::GroupCreateErr::Exists) => {
                    CmdResult::Err(RespError::new(
                        "BUSYGROUP Consumer Group name already exists",
                    ))
                }
                Err(crate::core::stream::GroupCreateErr::Empty) => CmdResult::Err(RespError::new(
                    "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
                )),
            }
        }
        b"SETID" => {
            if ctx.args.len() < key_idx + 3 {
                return CmdResult::Err(RespError::syntax());
            }
            let group = CompactString::from_bytes(&ctx.args[key_idx + 1]);
            let id_arg = &ctx.args[key_idx + 2];
            let mut entries_read = None;
            let mut oi = key_idx + 3;
            while oi < ctx.args.len() {
                if ctx.args[oi].eq_ignore_ascii_case(b"ENTRIESREAD") && oi + 1 < ctx.args.len() {
                    let Some(v) = parse_i64(&ctx.args[oi + 1]) else {
                        return CmdResult::Err(RespError::integer());
                    };
                    if v < SCG_INVALID_ENTRIES_READ {
                        return CmdResult::Err(RespError::syntax());
                    }
                    entries_read = Some(v);
                    oi += 2;
                } else {
                    return CmdResult::Err(RespError::syntax());
                }
            }
            let s = match stream_mut(ctx, key) {
                Ok(s) => s,
                Err(e) => {
                    if e.message.starts_with("WRONGTYPE") {
                        return CmdResult::Err(e);
                    }
                    return CmdResult::Err(RespError::no_such_key_or_group(key, group.as_bytes()));
                }
            };
            let id = match parse_group_id_arg(id_arg, s) {
                Ok(id) => id,
                Err(e) => return CmdResult::Err(e),
            };
            let Some(grp) = s.group_mut(&group) else {
                return CmdResult::Err(RespError::no_such_key_or_group(key, group.as_bytes()));
            };
            grp.last_delivered = id;
            if let Some(v) = entries_read {
                grp.entries_read = v;
            }
            CmdResult::Ok(crate::commands::ok())
        }
        b"DESTROY" => {
            let group = CompactString::from_bytes(&ctx.args[key_idx + 1]);
            let s = match stream_mut(ctx, key) {
                Ok(s) => s,
                Err(e) => {
                    if e.message.starts_with("WRONGTYPE") {
                        return CmdResult::Err(e);
                    }
                    return CmdResult::Ok(integer(0));
                }
            };
            CmdResult::Ok(integer(i64::from(s.destroy_group(&group))))
        }
        b"DELCONSUMER" => {
            if ctx.args.len() < key_idx + 3 {
                return CmdResult::Err(RespError::syntax());
            }
            let group = CompactString::from_bytes(&ctx.args[key_idx + 1]);
            let consumer = CompactString::from_bytes(&ctx.args[key_idx + 2]);
            let s = match stream_mut(ctx, key) {
                Ok(s) => s,
                Err(e) => {
                    if e.message.starts_with("WRONGTYPE") {
                        return CmdResult::Err(e);
                    }
                    return CmdResult::Ok(integer(0));
                }
            };
            let Some(grp) = s.group_mut(&group) else {
                return CmdResult::Err(RespError::no_such_key_or_group(key, group.as_bytes()));
            };
            let before = grp.pel.len();
            grp.pel.retain(|_, pe| pe.consumer != consumer);
            grp.consumers.remove(&consumer);
            CmdResult::Ok(integer((before - grp.pel.len()) as i64))
        }
        b"CREATECONSUMER" => {
            if ctx.args.len() < key_idx + 3 {
                return CmdResult::Err(RespError::syntax());
            }
            let group = CompactString::from_bytes(&ctx.args[key_idx + 1]);
            let consumer = CompactString::from_bytes(&ctx.args[key_idx + 2]);
            let now = ctx.now_ms;
            let s = match stream_mut(ctx, key) {
                Ok(s) => s,
                Err(e) => {
                    if e.message.starts_with("WRONGTYPE") {
                        return CmdResult::Err(e);
                    }
                    return CmdResult::Ok(integer(0));
                }
            };
            let Some(grp) = s.group_mut(&group) else {
                return CmdResult::Err(RespError::no_such_key_or_group(key, group.as_bytes()));
            };
            if grp.consumers.contains_key(&consumer) {
                CmdResult::Ok(integer(0))
            } else {
                grp.consumers.insert(
                    consumer,
                    crate::core::stream::Consumer {
                        seen_time: now,
                        active_time: -1,
                        pending: 0,
                    },
                );
                CmdResult::Ok(integer(1))
            }
        }
        _ => CmdResult::Err(RespError::syntax()),
    }
}

fn exec_xinfo(ctx: &mut OpContext) -> CmdResult {
    if ctx.args.len() < 3 {
        return CmdResult::Err(RespError::syntax());
    }
    let sub = ctx.args[1].to_ascii_uppercase();
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    match sub.as_slice() {
        b"STREAM" => {
            let mut full = false;
            // Default `count` for XINFO STREAM FULL is 10 (stream_family.cc:3598);
            // an explicit COUNT 0 means no limit.
            let mut count: usize = 10;
            if ctx.args.len() >= key_idx + 2 {
                if !ctx.args[key_idx + 1].eq_ignore_ascii_case(b"FULL") {
                    return CmdResult::Err(RespError::new(
                        "unknown subcommand or wrong number of arguments for 'STREAM'. Try XINFO HELP.",
                    ));
                }
                full = true;
                if ctx.args.len() > key_idx + 2 {
                    // Only the exact `FULL COUNT <n>` form is accepted; `FULL
                    // COUNT` (missing value) and extra trailing args both error.
                    if !ctx.args[key_idx + 2].eq_ignore_ascii_case(b"COUNT")
                        || ctx.args.len() != key_idx + 4
                    {
                        return CmdResult::Err(RespError::new(
                            "unknown subcommand or wrong number of arguments for 'STREAM'. Try XINFO HELP.",
                        ));
                    }
                    match parse_u64(&ctx.args[key_idx + 3]) {
                        Some(v) => count = v as usize,
                        None => {
                            return CmdResult::Err(RespError::new(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    }
                }
            }
            let s = match stream_mut(ctx, key) {
                Ok(s) => s,
                Err(e) => {
                    if e.message.starts_with("WRONGTYPE") {
                        return CmdResult::Err(e);
                    }
                    return CmdResult::Err(RespError::new("ERR no such key"));
                }
            };
            let radix_tree_keys = u64::from(s.length > 0);
            let radix_tree_nodes =
                1 + s.length.div_ceil(crate::core::stream::NODE_MAX_ENTRIES as u64);
            let mut out: Vec<RespValue> = vec![
                bulk(b"length"),
                integer(s.length as i64),
                bulk(b"radix-tree-keys"),
                integer(radix_tree_keys as i64),
                bulk(b"radix-tree-nodes"),
                integer(radix_tree_nodes as i64),
                bulk(b"last-generated-id"),
                bulk(render_id(&s.last_id)),
                bulk(b"max-deleted-entry-id"),
                bulk(render_id(&s.max_deleted_id)),
                bulk(b"entries-added"),
                integer(s.entries_added() as i64),
                bulk(b"recorded-first-entry-id"),
                bulk(render_id(
                    &s.first_entry().copied().unwrap_or(StreamId::MIN),
                )),
            ];
            let record = |id: &StreamId, s: &Stream| -> RespValue {
                let fields = &s.entries[id].fields;
                let mut arr = Vec::with_capacity(fields.len() * 2);
                for (f, v) in fields {
                    arr.push(bulk(f.as_bytes()));
                    arr.push(bulk(v.as_bytes()));
                }
                RespValue::Array(vec![bulk(render_id(id)), RespValue::Array(arr)])
            };
            if full {
                out.push(bulk(b"entries"));
                let mut entries: Vec<RespValue> = Vec::new();
                for (id, e) in &s.entries {
                    if e.deleted {
                        continue;
                    }
                    if count > 0 && entries.len() >= count {
                        break;
                    }
                    entries.push(record(id, s));
                }
                out.push(RespValue::Array(entries));
                out.push(bulk(b"groups"));
                let mut group_names: Vec<&CompactString> = s.groups.keys().collect();
                group_names.sort();
                let mut groups: Vec<RespValue> = Vec::new();
                for name in group_names {
                    let g = &s.groups[name];
                    let lag = s.cg_lag(g);
                    let mut pending: Vec<RespValue> = Vec::new();
                    for (eid, pe) in &g.pel {
                        if count > 0 && pending.len() >= count {
                            break;
                        }
                        pending.push(RespValue::Array(vec![
                            bulk(render_id(eid)),
                            bulk(pe.consumer.as_bytes()),
                            integer(pe.delivery_time as i64),
                            integer(pe.delivery_count as i64),
                        ]));
                    }
                    let mut consumers: Vec<RespValue> = Vec::new();
                    let mut cnames: Vec<&CompactString> = g.consumers.keys().collect();
                    cnames.sort();
                    for cname in cnames {
                        let c = &g.consumers[cname];
                        let mut cpending: Vec<RespValue> = Vec::new();
                        for (eid, pe) in &g.pel {
                            if pe.consumer != *cname {
                                continue;
                            }
                            if count > 0 && cpending.len() >= count {
                                break;
                            }
                            cpending.push(RespValue::Array(vec![
                                bulk(render_id(eid)),
                                integer(pe.delivery_time as i64),
                                integer(pe.delivery_count as i64),
                            ]));
                        }
                        consumers.push(RespValue::Array(vec![
                            bulk(b"name"),
                            bulk(cname.as_bytes()),
                            bulk(b"seen-time"),
                            integer(c.seen_time as i64),
                            bulk(b"active-time"),
                            integer(c.active_time),
                            bulk(b"pel-count"),
                            integer(c.pending as i64),
                            bulk(b"pending"),
                            RespValue::Array(cpending),
                        ]));
                    }
                    groups.push(RespValue::Array(vec![
                        bulk(b"name"),
                        bulk(name.as_bytes()),
                        bulk(b"last-delivered-id"),
                        bulk(render_id(&g.last_delivered)),
                        bulk(b"entries-read"),
                        if g.entries_read == SCG_INVALID_ENTRIES_READ {
                            RespValue::Nil
                        } else {
                            integer(g.entries_read)
                        },
                        bulk(b"lag"),
                        if lag == SCG_INVALID_LAG {
                            RespValue::Nil
                        } else {
                            integer(lag)
                        },
                        bulk(b"pel-count"),
                        integer(g.pel.len() as i64),
                        bulk(b"pending"),
                        RespValue::Array(pending),
                        bulk(b"consumers"),
                        RespValue::Array(consumers),
                    ]));
                }
                out.push(RespValue::Array(groups));
            } else {
                let first = s.first_entry().map(|id| record(id, s));
                let last = s.last_entry().map(|id| record(id, s));
                out.push(bulk(b"groups"));
                out.push(integer(s.groups.len() as i64));
                out.push(bulk(b"first-entry"));
                out.push(first.unwrap_or(RespValue::NilArray));
                out.push(bulk(b"last-entry"));
                out.push(last.unwrap_or(RespValue::NilArray));
            }
            CmdResult::Ok(RespValue::Array(out))
        }
        b"GROUPS" => {
            let s = match stream_mut(ctx, key) {
                Ok(s) => s,
                Err(e) => {
                    if e.message.starts_with("WRONGTYPE") {
                        return CmdResult::Err(e);
                    }
                    return CmdResult::Err(RespError::new("ERR no such key"));
                }
            };
            let mut out = Vec::new();
            let mut names: Vec<&CompactString> = s.groups.keys().collect();
            names.sort();
            for name in names {
                let g = &s.groups[name];
                let lag = s.cg_lag(g);
                out.push(RespValue::Array(vec![
                    bulk(b"name"),
                    bulk(name.as_bytes()),
                    bulk(b"consumers"),
                    integer(g.consumers.len() as i64),
                    bulk(b"pending"),
                    integer(g.pel.len() as i64),
                    bulk(b"last-delivered-id"),
                    bulk(render_id(&g.last_delivered)),
                    bulk(b"entries-read"),
                    if g.entries_read == SCG_INVALID_ENTRIES_READ {
                        RespValue::Nil
                    } else {
                        integer(g.entries_read)
                    },
                    bulk(b"lag"),
                    if lag == SCG_INVALID_LAG {
                        RespValue::Nil
                    } else {
                        integer(lag)
                    },
                ]));
            }
            CmdResult::Ok(RespValue::Array(out))
        }
        b"CONSUMERS" => {
            if ctx.args.len() < key_idx + 2 {
                return CmdResult::Err(RespError::syntax());
            }
            let now = ctx.now_ms;
            let group = CompactString::from_bytes(&ctx.args[key_idx + 1]);
            let s = match stream_mut(ctx, key) {
                Ok(s) => s,
                Err(e) => {
                    if e.message.starts_with("WRONGTYPE") {
                        return CmdResult::Err(e);
                    }
                    return CmdResult::Err(RespError::new("ERR no such key"));
                }
            };
            let Some(g) = s.group(&group) else {
                return CmdResult::Err(RespError::no_such_key_or_group(key, group.as_bytes()));
            };
            let mut out = Vec::new();
            let mut cnames: Vec<&CompactString> = g.consumers.keys().collect();
            cnames.sort();
            for name in cnames {
                let c = &g.consumers[name];
                let idle = now.saturating_sub(c.seen_time);
                let inactive = if c.active_time == -1 {
                    -1
                } else {
                    (now as i64).saturating_sub(c.active_time)
                };
                out.push(RespValue::Array(vec![
                    bulk(b"name"),
                    bulk(name.as_bytes()),
                    bulk(b"pending"),
                    integer(c.pending as i64),
                    bulk(b"idle"),
                    integer(idle as i64),
                    bulk(b"inactive"),
                    integer(inactive),
                ]));
            }
            CmdResult::Ok(RespValue::Array(out))
        }
        _ => CmdResult::Err(RespError::syntax()),
    }
}

// ---------------------------------------------------------------------------
// XSETID
// ---------------------------------------------------------------------------

fn exec_xsetid(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    if ctx.args.len() < key_idx + 2 {
        return CmdResult::Err(RespError::syntax());
    }
    let id = match parse_stream_id_literal(&ctx.args[key_idx + 1], 0) {
        Ok(id) => id,
        Err(e) => return CmdResult::Err(e),
    };
    let s = match stream_mut(ctx, key) {
        Ok(s) => s,
        Err(e) => return CmdResult::Err(e),
    };
    if id < s.max_deleted_id {
        return CmdResult::Err(RespError::new(
            "stream_smaller_deleted The ID specified in XSETID is smaller than current max_deleted_entry_id",
        ));
    }
    if let Some(top) = s
        .entries
        .iter()
        .rev()
        .find(|(_, e)| !e.deleted)
        .map(|(id, _)| *id)
        && id < top
    {
        return CmdResult::Err(RespError::new(
            "ERR The ID specified in XSETID is equal or smaller than the target stream top item",
        ));
    }
    s.last_id = id;
    CmdResult::Ok(crate::commands::ok())
}

// ---------------------------------------------------------------------------
// XCLAIM
// ---------------------------------------------------------------------------

const CLAIM_COUNT_LIMIT: i64 = 1 << 18;

fn exec_xclaim(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    if ctx.args.len() < key_idx + 4 {
        return CmdResult::Err(RespError::syntax());
    }
    let group = CompactString::from_bytes(&ctx.args[key_idx + 1]);
    let consumer = CompactString::from_bytes(&ctx.args[key_idx + 2]);
    if group.is_empty() || consumer.is_empty() {
        return CmdResult::Err(RespError::syntax());
    }
    let min_idle_time = match parse_i64(&ctx.args[key_idx + 3]) {
        Some(v) => v.max(0) as u64,
        None => return CmdResult::Err(RespError::syntax()),
    };

    // Leading stream IDs.
    let mut i = key_idx + 4;
    let mut ids = Vec::new();
    while i < ctx.args.len() {
        match parse_stream_id_literal(&ctx.args[i], 0) {
            Ok(id) => {
                ids.push(id);
                i += 1;
            }
            Err(_) => break,
        }
    }
    if ids.is_empty() {
        return CmdResult::Err(RespError::new(
            "ERR Invalid stream ID specified as stream command argument",
        ));
    }

    // Options following the IDs.
    let mut delivery_time: i64 = -1;
    let mut retry: i64 = -1;
    let mut force = false;
    let mut justid = false;
    let mut last_id: Option<StreamId> = None;
    while i < ctx.args.len() {
        let t = ctx.args[i].to_ascii_uppercase();
        match t.as_slice() {
            b"IDLE" | b"TIME" => {
                if i + 1 >= ctx.args.len() {
                    return CmdResult::Err(RespError::syntax());
                }
                delivery_time = match parse_i64(&ctx.args[i + 1]) {
                    Some(v) => v,
                    None => return CmdResult::Err(RespError::integer()),
                };
                i += 2;
            }
            b"RETRYCOUNT" => {
                if i + 1 >= ctx.args.len() {
                    return CmdResult::Err(RespError::syntax());
                }
                retry = match parse_i64(&ctx.args[i + 1]) {
                    Some(v) => v,
                    None => return CmdResult::Err(RespError::integer()),
                };
                i += 2;
            }
            b"LASTID" => {
                if i + 1 >= ctx.args.len() {
                    return CmdResult::Err(RespError::syntax());
                }
                last_id = Some(match parse_stream_id_literal(&ctx.args[i + 1], 0) {
                    Ok(id) => id,
                    Err(e) => return CmdResult::Err(e),
                });
                i += 2;
            }
            b"FORCE" => {
                force = true;
                i += 1;
            }
            b"JUSTID" => {
                justid = true;
                i += 1;
            }
            _ => {
                return CmdResult::Err(RespError::new(
                    "ERR Unknown argument given for XCLAIM command",
                ));
            }
        }
    }

    let now = ctx.now_ms;
    let delivery_time = if delivery_time < 0 || delivery_time as u64 > now {
        now
    } else {
        delivery_time as u64
    };

    let s = match stream_mut(ctx, key) {
        Ok(s) => s,
        Err(e) => return CmdResult::Err(e),
    };
    let exists: Vec<bool> = ids
        .iter()
        .map(|id| s.entries.get(id).is_some_and(|e| !e.deleted))
        .collect();
    let Some(grp) = s.group_mut(&group) else {
        // Missing group: XCLAIM returns an empty array (OpStatus::SKIPPED).
        return CmdResult::Ok(RespValue::Array(vec![]));
    };
    if let Some(li) = last_id
        && li > grp.last_delivered
    {
        grp.last_delivered = li;
    }
    grp.consumer_mut(&consumer, now);

    let mut claimed: Vec<StreamId> = Vec::new();
    for (k, &id) in ids.iter().enumerate() {
        let entry_exists = exists[k];
        let nack_present = grp.pel.contains_key(&id);
        if !entry_exists {
            if let Some(pe) = grp.pel.remove(&id)
                && let Some(c) = grp.consumers.get_mut(&pe.consumer)
            {
                c.pending = c.pending.saturating_sub(1);
            }
            continue;
        }
        if !nack_present && force {
            grp.pel.insert(
                id,
                PendingEntry {
                    consumer: CompactString::new(),
                    delivery_time: now,
                    delivery_count: 0,
                },
            );
        }
        let Some(pe) = grp.pel.get_mut(&id) else {
            continue;
        };
        if !pe.consumer.is_empty()
            && min_idle_time > 0
            && now.saturating_sub(pe.delivery_time) < min_idle_time
        {
            continue;
        }
        let old_consumer = pe.consumer.clone();
        pe.delivery_time = delivery_time;
        if retry >= 0 {
            pe.delivery_count = retry.max(0) as u64;
        } else if !justid {
            pe.delivery_count += 1;
        }
        if old_consumer != consumer {
            if let Some(c) = grp.consumers.get_mut(&old_consumer) {
                c.pending = c.pending.saturating_sub(1);
            }
            if let Some(pe) = grp.pel.get_mut(&id) {
                pe.consumer = consumer.clone();
            }
            if let Some(c) = grp.consumers.get_mut(&consumer) {
                c.pending += 1;
                c.active_time = now as i64;
            }
        } else if let Some(c) = grp.consumers.get_mut(&consumer) {
            c.active_time = now as i64;
        }
        claimed.push(id);
    }

    let out = if justid {
        claimed.iter().map(|id| bulk(render_id(id))).collect()
    } else {
        claimed
            .iter()
            .map(|id| entry_to_resp(*id, &s.entries[id].fields))
            .collect()
    };
    CmdResult::Ok(RespValue::Array(out))
}

// ---------------------------------------------------------------------------
// XAUTOCLAIM
// ---------------------------------------------------------------------------

fn parse_autoclaim_start(s: &[u8]) -> Result<StreamId, RespError> {
    let (exclude, rest) = if s.first() == Some(&b'(') {
        (true, &s[1..])
    } else {
        (false, s)
    };
    let id = if rest == b"-" {
        StreamId::MIN
    } else if rest == b"+" {
        StreamId::MAX
    } else {
        parse_stream_id_literal(rest, 0)?
    };
    if exclude {
        if id.ms == 0 && id.seq == 0 {
            return Err(RespError::new("invalid start ID for the interval"));
        }
        Ok(if id.seq > 0 {
            StreamId {
                ms: id.ms,
                seq: id.seq - 1,
            }
        } else {
            StreamId {
                ms: id.ms - 1,
                seq: u64::MAX,
            }
        })
    } else {
        Ok(id)
    }
}

fn exec_xautoclaim(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    if ctx.args.len() < key_idx + 5 {
        return CmdResult::Err(RespError::syntax());
    }
    let group = CompactString::from_bytes(&ctx.args[key_idx + 1]);
    let consumer = CompactString::from_bytes(&ctx.args[key_idx + 2]);
    if group.is_empty() || consumer.is_empty() {
        return CmdResult::Err(RespError::syntax());
    }
    let min_idle_time = match parse_i64(&ctx.args[key_idx + 3]) {
        Some(v) => v.max(0) as u64,
        None => return CmdResult::Err(RespError::syntax()),
    };
    let start_id = match parse_autoclaim_start(&ctx.args[key_idx + 4]) {
        Ok(id) => id,
        Err(e) => return CmdResult::Err(e),
    };

    let mut count: i64 = 100;
    let mut justid = false;
    let mut i = key_idx + 5;
    while i < ctx.args.len() {
        let t = ctx.args[i].to_ascii_uppercase();
        let has_next = i + 1 < ctx.args.len();
        if has_next && t == b"COUNT" {
            let Some(v) = parse_i64(&ctx.args[i + 1]) else {
                return CmdResult::Err(RespError::integer());
            };
            if v <= 0 || v >= CLAIM_COUNT_LIMIT {
                return CmdResult::Err(RespError::new("ERR COUNT must be > 0 and less than 2^18"));
            }
            count = v;
            i += 2;
            continue;
        }
        if t == b"JUSTID" {
            justid = true;
            i += 1;
        } else {
            return CmdResult::Err(RespError::new(
                "ERR Unknown argument given for XAUTOCLAIM command",
            ));
        }
    }

    let now = ctx.now_ms;
    let s = match stream_mut(ctx, key) {
        Ok(s) => s,
        Err(e) => {
            if e.message.starts_with("WRONGTYPE") {
                return CmdResult::Err(e);
            }
            return CmdResult::Err(RespError::no_such_key_or_group(key, group.as_bytes()));
        }
    };
    let Some(grp) = s.group(&group) else {
        return CmdResult::Err(RespError::no_such_key_or_group(key, group.as_bytes()));
    };
    let pel_ids: Vec<StreamId> = grp.pel.range(start_id..).map(|(id, _)| *id).collect();
    let exists: Vec<bool> = pel_ids
        .iter()
        .map(|id| s.entries.get(id).is_some_and(|e| !e.deleted))
        .collect();
    let grp = s.group_mut(&group).unwrap();
    grp.consumer_mut(&consumer, now);

    let mut attempts = count * 10;
    let mut remaining = count;
    let mut idx = 0;
    let mut claimed: Vec<StreamId> = Vec::new();
    let mut deleted: Vec<StreamId> = Vec::new();
    while attempts > 0 && remaining > 0 && idx < pel_ids.len() {
        attempts -= 1;
        let id = pel_ids[idx];
        idx += 1;
        let entry_exists = exists[idx - 1];
        if let Some(pe) = if entry_exists {
            None
        } else {
            grp.pel.remove(&id)
        } {
            if let Some(c) = grp.consumers.get_mut(&pe.consumer) {
                c.pending = c.pending.saturating_sub(1);
            }
            deleted.push(id);
            remaining -= 1;
            continue;
        }
        let Some(pe) = grp.pel.get_mut(&id) else {
            continue;
        };
        if min_idle_time > 0 && now.saturating_sub(pe.delivery_time) < min_idle_time {
            continue;
        }
        let old_consumer = pe.consumer.clone();
        pe.delivery_time = now;
        if !justid {
            pe.delivery_count += 1;
        }
        if old_consumer != consumer {
            if let Some(c) = grp.consumers.get_mut(&old_consumer) {
                c.pending = c.pending.saturating_sub(1);
            }
            if let Some(pe) = grp.pel.get_mut(&id) {
                pe.consumer = consumer.clone();
            }
            if let Some(c) = grp.consumers.get_mut(&consumer) {
                c.pending += 1;
                c.active_time = now as i64;
            }
        } else if let Some(c) = grp.consumers.get_mut(&consumer) {
            c.active_time = now as i64;
        }
        claimed.push(id);
        remaining -= 1;
    }
    let end_id = if idx >= pel_ids.len() {
        StreamId::MIN
    } else {
        pel_ids[idx]
    };

    let claimed_arr = if justid {
        claimed.iter().map(|id| bulk(render_id(id))).collect()
    } else {
        claimed
            .iter()
            .map(|id| entry_to_resp(*id, &s.entries[id].fields))
            .collect()
    };
    let deleted_arr: Vec<RespValue> = deleted.iter().map(|id| bulk(render_id(id))).collect();
    CmdResult::Ok(RespValue::Array(vec![
        bulk(render_id(&end_id)),
        RespValue::Array(claimed_arr),
        RespValue::Array(deleted_arr),
    ]))
}

// ---------------------------------------------------------------------------
// Command definitions
// ---------------------------------------------------------------------------

pub static CMD_XADD: Command = Command {
    name: "XADD",
    arity: -5,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_xadd,
    merge: None,
};
pub static CMD_XLEN: Command = Command {
    name: "XLEN",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_xlen,
    merge: None,
};
pub static CMD_XRANGE: Command = Command {
    name: "XRANGE",
    arity: -4,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_xrange,
    merge: None,
};
pub static CMD_XREVRANGE: Command = Command {
    name: "XREVRANGE",
    arity: -4,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_xrevrange,
    merge: None,
};
pub static CMD_XDEL: Command = Command {
    name: "XDEL",
    arity: -3,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_xdel,
    merge: None,
};
pub static CMD_XTRIM: Command = Command {
    name: "XTRIM",
    arity: -4,
    flags: FLAG_WRITE,
    key_range: KeyRange::ONE,
    exec: exec_xtrim,
    merge: None,
};
pub static CMD_XREAD: Command = Command {
    name: "XREAD",
    arity: -3,
    flags: FLAG_READONLY | FLAG_BLOCKING | FLAG_MULTI_KEY | FLAG_MOVABLEKEYS,
    key_range: KeyRange::NONE,
    exec: exec_xread,
    merge: Some(merge_xread),
};
pub static CMD_XGROUP: Command = Command {
    name: "XGROUP",
    // 2-arg `XGROUP HELP` bypasses this via a dispatch rewrite to the hidden
    // `_XGROUP_HELP` (arity 2), mirroring command_registry.cc:347-352.
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    // Syntax is XGROUP <subcommand> key [...], so the key is the 3rd argument.
    key_range: KeyRange {
        first: 2,
        last: 2,
        step: 1,
    },
    exec: exec_xgroup,
    merge: None,
};
pub static CMD_XREADGROUP: Command = Command {
    name: "XREADGROUP",
    arity: -6,
    flags: FLAG_WRITE | FLAG_BLOCKING | FLAG_MULTI_KEY | FLAG_MOVABLEKEYS | FLAG_NO_AUTOJOURNAL,
    key_range: KeyRange::NONE,
    exec: exec_xreadgroup,
    merge: Some(merge_xreadgroup),
};
pub static CMD_XACK: Command = Command {
    name: "XACK",
    arity: -4,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_xack,
    merge: None,
};
pub static CMD_XPENDING: Command = Command {
    name: "XPENDING",
    arity: -3,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_xpending,
    merge: None,
};
pub static CMD_XINFO: Command = Command {
    name: "XINFO",
    arity: -2,
    flags: FLAG_READONLY,
    // Syntax is XINFO <subcommand> key [...], so the key is the 3rd argument.
    key_range: KeyRange {
        first: 2,
        last: 2,
        step: 1,
    },
    exec: exec_xinfo,
    merge: None,
};
pub static CMD_XSETID: Command = Command {
    name: "XSETID",
    arity: 3,
    flags: FLAG_WRITE,
    key_range: KeyRange::ONE,
    exec: exec_xsetid,
    merge: None,
};
pub static CMD_XCLAIM: Command = Command {
    name: "XCLAIM",
    arity: -6,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_xclaim,
    merge: None,
};
pub static CMD_XAUTOCLAIM: Command = Command {
    name: "XAUTOCLAIM",
    arity: -6,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_xautoclaim,
    merge: None,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db::DbSlice;

    fn blk(s: &[u8]) -> RespValue {
        RespValue::Bulk(s.to_vec())
    }

    fn arr(v: Vec<RespValue>) -> RespValue {
        RespValue::Array(v)
    }

    /// Element `i` of the array `v`.
    fn idx<'a>(v: &'a RespValue, i: usize) -> &'a RespValue {
        &v.as_array().unwrap()[i]
    }

    fn bulk_of(r: CmdResult) -> Vec<u8> {
        match r {
            CmdResult::Ok(RespValue::Bulk(b)) => b,
            o => panic!("expected bulk, got {:?}", o.into_resp_value()),
        }
    }

    fn err_of(r: CmdResult) -> String {
        match r {
            CmdResult::Err(e) => e.message,
            o => panic!("expected error, got {:?}", o.into_resp_value()),
        }
    }

    fn arr_of(r: CmdResult) -> Vec<RespValue> {
        match r {
            CmdResult::Ok(RespValue::Array(v)) => v,
            o => panic!("expected array, got {:?}", o.into_resp_value()),
        }
    }

    fn val(r: CmdResult) -> RespValue {
        r.into_resp_value()
    }

    fn dispatch_at(db: &mut DbSlice, now_ms: u64, argv: &[Vec<u8>]) -> CmdResult {
        let cmd = argv[0].to_ascii_uppercase();
        let (exec, first_key_idx, owned): (crate::commands::ExecFn, usize, Vec<usize>) =
            match cmd.as_slice() {
                b"XADD" => (exec_xadd, 1, vec![1]),
                b"XDEL" => (exec_xdel, 1, vec![1]),
                b"XACK" => (exec_xack, 1, vec![1]),
                b"XSETID" => (exec_xsetid, 1, vec![1]),
                b"XCLAIM" => (exec_xclaim, 1, vec![1]),
                b"XAUTOCLAIM" => (exec_xautoclaim, 1, vec![1]),
                b"XPENDING" => (exec_xpending, 1, vec![1]),
                b"XRANGE" => (exec_xrange, 1, vec![1]),
                b"XLEN" => (exec_xlen, 1, vec![1]),
                b"XGROUP" => (exec_xgroup, 2, vec![2]),
                b"XINFO" => (exec_xinfo, 2, vec![2]),
                b"XREADGROUP" => {
                    let si = argv
                        .iter()
                        .position(|a| a.eq_ignore_ascii_case(b"STREAMS"))
                        .expect("XREADGROUP without STREAMS");
                    (exec_xreadgroup, 0, vec![si + 1])
                }
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

    /// Port of `StreamFamilyTest.Add`.
    #[test]
    fn add() {
        let mut db = DbSlice::new(0);
        let now = 1_700_000_000_000;

        // Auto id: `*` resolves to `<now>-0`.
        let resp = cmd_at(&mut db, now, &[b"XADD", b"key", b"*", b"field", b"value"]);
        let id = bulk_of(resp);
        assert!(id.ends_with(b"-0"), "auto id {id:?} must end in -0");

        // xrange on a missing key is an empty array.
        let resp = cmd_at(&mut db, now, &[b"XRANGE", b"null", b"-", b"+"]);
        assert_eq!(val(resp), arr(vec![]));

        // xrange returns `[[id, [field, value]]]`.
        let resp = cmd_at(&mut db, now, &[b"XRANGE", b"key", b"-", b"+"]);
        assert_eq!(
            val(resp),
            arr(vec![arr(vec![
                blk(&id),
                arr(vec![blk(b"field"), blk(b"value")])
            ])])
        );

        let resp = cmd_at(&mut db, now, &[b"XLEN", b"key"]);
        assert_eq!(val(resp), RespValue::Integer(1));

        // A malformed id is rejected.
        let resp = cmd_at(&mut db, now, &[b"XADD", b"key", b"badid", b"f1", b"val1"]);
        assert!(err_of(resp).contains("Invalid stream ID"));

        // nomkstream on an existing key still appends.
        let resp = cmd_at(
            &mut db,
            now,
            &[b"XADD", b"key", b"NOMKSTREAM", b"*", b"field2", b"value2"],
        );
        let _ = bulk_of(resp);

        // nomkstream on a missing key returns nil.
        let resp = cmd_at(
            &mut db,
            now,
            &[b"XADD", b"noexist", b"NOMKSTREAM", b"*", b"field", b"value"],
        );
        assert_eq!(val(resp), RespValue::Nil);
    }

    /// Port of `StreamFamilyTest.AddExtended`.
    #[test]
    fn add_extended() {
        let mut db = DbSlice::new(0);

        // An explicit ms id autocompletes the sequence to 0.
        let resp = cmd(&mut db, &[b"XADD", b"key", b"5", b"f1", b"v1", b"f2", b"v2"]);
        assert_eq!(bulk_of(resp), b"5-0");

        let resp = cmd(&mut db, &[b"XRANGE", b"key", b"5-0", b"5-0"]);
        assert_eq!(
            val(resp),
            arr(vec![arr(vec![
                blk(b"5-0"),
                arr(vec![blk(b"f1"), blk(b"v1"), blk(b"f2"), blk(b"v2")])
            ])])
        );

        // maxlen 1 keeps only the newest entry.
        let resp = cmd(&mut db, &[b"XADD", b"key", b"MAXLEN", b"1", b"*", b"field1", b"val1"]);
        let id1 = bulk_of(resp);
        let resp = cmd(&mut db, &[b"XADD", b"key", b"MAXLEN", b"1", b"*", b"field2", b"val2"]);
        let id2 = bulk_of(resp);

        let resp = cmd(&mut db, &[b"XLEN", b"key"]);
        assert_eq!(val(resp), RespValue::Integer(1));

        // id1 was trimmed away.
        let resp = cmd(&mut db, &[b"XRANGE", b"key", &id1, &id1]);
        assert_eq!(val(resp), arr(vec![]));

        // Re-adding id2 is rejected as not strictly greater.
        let resp = cmd(&mut db, &[b"XADD", b"key", &id2, b"f1", b"val1"]);
        assert!(err_of(resp).contains("equal or smaller than"));

        // minid 6 trims everything below 6-0.
        cmd(&mut db, &[b"XADD", b"key2", b"5-0", b"field", b"val"]);
        cmd(&mut db, &[b"XADD", b"key2", b"6-0", b"field1", b"val1"]);
        cmd(&mut db, &[b"XADD", b"key2", b"7-0", b"field2", b"val2"]);
        let _ = cmd(&mut db, &[b"XADD", b"key2", b"MINID", b"6", b"*", b"field3", b"val3"]);
        let resp = cmd(&mut db, &[b"XLEN", b"key2"]);
        assert_eq!(val(resp), RespValue::Integer(3));
        let resp = cmd(&mut db, &[b"XRANGE", b"key2", b"5-0", b"5-0"]);
        assert_eq!(val(resp), arr(vec![]));

        // Approximate maxlen trimming removes whole 100-entry nodes.
        for _ in 0..700 {
            cmd(&mut db, &[b"XADD", b"key3", b"*", b"field", b"val"]);
        }
        cmd(
            &mut db,
            &[b"XADD", b"key3", b"MAXLEN", b"~", b"500", b"*", b"field", b"val"],
        );
        let resp = cmd(&mut db, &[b"XLEN", b"key3"]);
        assert_eq!(val(resp), RespValue::Integer(501));

        // ... capped by LIMIT.
        for _ in 0..700 {
            cmd(&mut db, &[b"XADD", b"key4", b"*", b"field", b"val"]);
        }
        cmd(
            &mut db,
            &[
                b"XADD", b"key4", b"MAXLEN", b"~", b"500", b"LIMIT", b"100", b"*", b"field", b"val",
            ],
        );
        let resp = cmd(&mut db, &[b"XLEN", b"key4"]);
        assert_eq!(val(resp), RespValue::Integer(601));
    }

    /// Port of `StreamFamilyTest.Xclaim`.
    #[test]
    fn xclaim() {
        let mut db = DbSlice::new(0);
        cmd(&mut db, &[b"XADD", b"foo", b"1-0", b"k1", b"v1"]);
        cmd(&mut db, &[b"XADD", b"foo", b"1-1", b"k2", b"v2"]);
        cmd(&mut db, &[b"XADD", b"foo", b"1-2", b"k3", b"v3"]);
        cmd(&mut db, &[b"XADD", b"foo", b"1-3", b"k4", b"v4"]);
        cmd(&mut db, &[b"XGROUP", b"CREATE", b"foo", b"group", b"0"]);
        cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"group",
                b"alice",
                b"STREAMS",
                b"foo",
                b">",
            ],
        );

        // bob claims alice's two pending stream entries.
        let resp = cmd(
            &mut db,
            &[b"XCLAIM", b"foo", b"group", b"bob", b"0", b"1-2", b"1-3"],
        );
        assert_eq!(
            val(resp),
            arr(vec![
                arr(vec![blk(b"1-2"), arr(vec![blk(b"k3"), blk(b"v3")])]),
                arr(vec![blk(b"1-3"), arr(vec![blk(b"k4"), blk(b"v4")])]),
            ])
        );

        // bob really has these claimed entries.
        let resp = cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"group",
                b"bob",
                b"STREAMS",
                b"foo",
                b"0",
            ],
        );
        assert_eq!(
            val(resp),
            arr(vec![
                arr(vec![
                    blk(b"foo"),
                    arr(vec![
                        arr(vec![blk(b"1-2"), arr(vec![blk(b"k3"), blk(b"v3")])]),
                        arr(vec![blk(b"1-3"), arr(vec![blk(b"k4"), blk(b"v4")])]),
                    ])
                ])
            ])
        );

        // alice no longer has those entries.
        let resp = cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"group",
                b"alice",
                b"STREAMS",
                b"foo",
                b"0",
            ],
        );
        assert_eq!(
            val(resp),
            arr(vec![
                arr(vec![
                    blk(b"foo"),
                    arr(vec![
                        arr(vec![blk(b"1-0"), arr(vec![blk(b"k1"), blk(b"v1")])]),
                        arr(vec![blk(b"1-1"), arr(vec![blk(b"k2"), blk(b"v2")])]),
                    ])
                ])
            ])
        );

        // xclaim ensures that entries before the min-idle-time are not claimed by bob.
        let resp = cmd(
            &mut db,
            &[b"XCLAIM", b"foo", b"group", b"bob", b"3600000", b"1-0"],
        );
        assert_eq!(val(resp), arr(vec![]));

        cmd(&mut db, &[b"XADD", b"foo", b"1-4", b"k5", b"v5"]);
        cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"group",
                b"alice",
                b"STREAMS",
                b"foo",
                b">",
            ],
        );
        // xclaim returns only claimed ids when justid is set.
        let resp = cmd(
            &mut db,
            &[
                b"XCLAIM", b"foo", b"group", b"bob", b"0", b"1-0", b"1-4", b"JUSTID",
            ],
        );
        assert_eq!(val(resp), arr(vec![blk(b"1-0"), blk(b"1-4")]));

        cmd(&mut db, &[b"XADD", b"foo", b"1-5", b"k6", b"v6"]);
        // bob should claim the id forcefully even if it is not yet present in group pel.
        let resp = cmd(
            &mut db,
            &[
                b"XCLAIM", b"foo", b"group", b"bob", b"0", b"1-5", b"FORCE", b"JUSTID",
            ],
        );
        assert_eq!(val(resp), arr(vec![blk(b"1-5")]));
        let resp = cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"group",
                b"bob",
                b"STREAMS",
                b"foo",
                b"0",
            ],
        );
        let bob_pel = match resp {
            CmdResult::Ok(RespValue::Array(v)) => match &v[0] {
                RespValue::Array(pair) => match &pair[1] {
                    RespValue::Array(entries) => entries.clone(),
                    _ => panic!("expected entries"),
                },
                _ => panic!("expected key/entries pair"),
            },
            o => panic!("expected array, got {:?}", o.into_resp_value()),
        };
        assert_eq!(
            bob_pel[4],
            arr(vec![blk(b"1-5"), arr(vec![blk(b"k6"), blk(b"v6")])])
        );

        let now = 2000;
        let time = now - 500;
        let resp = cmd_at(
            &mut db,
            now,
            &[
                b"XCLAIM",
                b"foo",
                b"group",
                b"alice",
                b"0",
                b"1-4",
                b"TIME",
                format!("{time}").as_bytes(),
                b"JUSTID",
            ],
        );
        assert_eq!(val(resp), arr(vec![blk(b"1-4")]));

        // min idle time is exceeded for this entry (idle == 500 < 600).
        let resp = cmd_at(
            &mut db,
            now,
            &[b"XCLAIM", b"foo", b"group", b"bob", b"600", b"1-4"],
        );
        assert_eq!(val(resp), arr(vec![]));

        let resp = cmd_at(
            &mut db,
            now,
            &[
                b"XCLAIM", b"foo", b"group", b"bob", b"400", b"1-4", b"JUSTID",
            ],
        );
        assert_eq!(val(resp), arr(vec![blk(b"1-4")]));

        // test RETRYCOUNT.
        cmd(&mut db, &[b"XADD", b"foo", b"1-6", b"k7", b"v7"]);
        let resp = cmd(
            &mut db,
            &[
                b"XCLAIM",
                b"foo",
                b"group",
                b"bob",
                b"0",
                b"1-6",
                b"FORCE",
                b"JUSTID",
                b"RETRYCOUNT",
                b"5",
            ],
        );
        assert_eq!(val(resp), arr(vec![blk(b"1-6")]));
        let resp = cmd(
            &mut db,
            &[b"XPENDING", b"foo", b"group", b"1-6", b"1-6", b"1"],
        );
        let pending = arr_of(resp);
        assert_eq!(
            pending[0],
            arr(vec![
                blk(b"1-6"),
                blk(b"bob"),
                RespValue::Integer(0),
                RespValue::Integer(5)
            ])
        );

        // test LASTID.
        cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"group",
                b"bob",
                b"COUNT",
                b"2",
                b"STREAMS",
                b"foo",
                b">",
            ],
        );
        cmd(
            &mut db,
            &[
                b"XCLAIM", b"foo", b"group", b"alice", b"0", b"1-6", b"LASTID", b"1-4",
            ],
        );
        let resp = cmd(&mut db, &[b"XINFO", b"GROUPS", b"foo"]);
        let groups = arr_of(resp);
        let g = match &groups[0] {
            RespValue::Array(v) => v.clone(),
            o => panic!("expected array, got {o:?}"),
        };
        assert_eq!(g[7], blk(b"1-6"));

        cmd(
            &mut db,
            &[
                b"XCLAIM", b"foo", b"group", b"bob", b"0", b"1-6", b"LASTID", b"1-9",
            ],
        );
        let resp = cmd(&mut db, &[b"XINFO", b"GROUPS", b"foo"]);
        let groups = arr_of(resp);
        let g = match &groups[0] {
            RespValue::Array(v) => v.clone(),
            o => panic!("expected array, got {o:?}"),
        };
        assert_eq!(g[7], blk(b"1-9"));
    }

    /// Port of `StreamFamilyTest.XAutoClaim`.
    #[test]
    fn xautoclaim() {
        let mut db = DbSlice::new(0);
        cmd(&mut db, &[b"XADD", b"foo", b"1-0", b"k1", b"v1"]);
        cmd(&mut db, &[b"XADD", b"foo", b"1-1", b"k2", b"v2"]);
        cmd(&mut db, &[b"XADD", b"foo", b"1-2", b"k3", b"v3"]);
        cmd(&mut db, &[b"XADD", b"foo", b"1-3", b"k4", b"v4"]);
        cmd(&mut db, &[b"XGROUP", b"CREATE", b"foo", b"group", b"0"]);
        cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"group",
                b"alice",
                b"STREAMS",
                b"foo",
                b">",
            ],
        );

        // bob claims alice's two pending stream entries.
        let resp = cmd(
            &mut db,
            &[b"XAUTOCLAIM", b"foo", b"group", b"bob", b"0", b"1-2"],
        );
        assert_eq!(
            val(resp),
            arr(vec![
                blk(b"0-0"),
                arr(vec![
                    arr(vec![blk(b"1-2"), arr(vec![blk(b"k3"), blk(b"v3")])]),
                    arr(vec![blk(b"1-3"), arr(vec![blk(b"k4"), blk(b"v4")])]),
                ]),
                arr(vec![]),
            ])
        );

        // bob really has these claimed entries.
        let resp = cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"group",
                b"bob",
                b"STREAMS",
                b"foo",
                b"0",
            ],
        );
        assert_eq!(
            val(resp),
            arr(vec![
                arr(vec![
                    blk(b"foo"),
                    arr(vec![
                        arr(vec![blk(b"1-2"), arr(vec![blk(b"k3"), blk(b"v3")])]),
                        arr(vec![blk(b"1-3"), arr(vec![blk(b"k4"), blk(b"v4")])]),
                    ])
                ])
            ])
        );

        // alice no longer has those entries.
        let resp = cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"group",
                b"alice",
                b"STREAMS",
                b"foo",
                b"0",
            ],
        );
        assert_eq!(
            val(resp),
            arr(vec![
                arr(vec![
                    blk(b"foo"),
                    arr(vec![
                        arr(vec![blk(b"1-0"), arr(vec![blk(b"k1"), blk(b"v1")])]),
                        arr(vec![blk(b"1-1"), arr(vec![blk(b"k2"), blk(b"v2")])]),
                    ])
                ])
            ])
        );

        // xautoclaim ensures that entries before the min-idle-time are not claimed by bob.
        let resp = cmd(
            &mut db,
            &[b"XAUTOCLAIM", b"foo", b"group", b"bob", b"3600000", b"0-0"],
        );
        assert_eq!(val(resp), arr(vec![blk(b"0-0"), arr(vec![]), arr(vec![])]));

        cmd(&mut db, &[b"XADD", b"foo", b"1-4", b"k5", b"v5"]);
        cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"group",
                b"alice",
                b"STREAMS",
                b"foo",
                b">",
            ],
        );
        // xautoclaim returns only claimed ids when justid is set.
        let resp = cmd(
            &mut db,
            &[
                b"XAUTOCLAIM",
                b"foo",
                b"group",
                b"bob",
                b"0",
                b"0-0",
                b"JUSTID",
            ],
        );
        assert_eq!(
            val(resp),
            arr(vec![
                blk(b"0-0"),
                arr(vec![
                    blk(b"1-0"),
                    blk(b"1-1"),
                    blk(b"1-2"),
                    blk(b"1-3"),
                    blk(b"1-4")
                ]),
                arr(vec![]),
            ])
        );

        cmd(&mut db, &[b"XADD", b"foo", b"1-5", b"k6", b"v6"]);
        cmd(&mut db, &[b"XADD", b"foo", b"1-6", b"k7", b"v7"]);
        cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"group",
                b"alice",
                b"STREAMS",
                b"foo",
                b">",
            ],
        );
        // test count and end_id.
        let resp = cmd(
            &mut db,
            &[
                b"XAUTOCLAIM",
                b"foo",
                b"group",
                b"bob",
                b"0",
                b"1-5",
                b"COUNT",
                b"1",
                b"JUSTID",
            ],
        );
        assert_eq!(
            val(resp),
            arr(vec![blk(b"1-6"), arr(vec![blk(b"1-5")]), arr(vec![])])
        );

        let resp = cmd(
            &mut db,
            &[
                b"XAUTOCLAIM",
                b"foo",
                b"group",
                b"bob",
                b"0",
                b"1-6",
                b"COUNT",
                b"1",
                b"JUSTID",
            ],
        );
        assert_eq!(
            val(resp),
            arr(vec![blk(b"0-0"), arr(vec![blk(b"1-6")]), arr(vec![])])
        );

        let resp = cmd(
            &mut db,
            &[
                b"XAUTOCLAIM",
                b"foo",
                b"group",
                b"bob",
                b"0",
                b"1-10",
                b"COUNT",
                b"1",
                b"JUSTID",
            ],
        );
        assert_eq!(val(resp), arr(vec![blk(b"0-0"), arr(vec![]), arr(vec![])]));

        // if a message being claimed is deleted, it should be listed separately.
        cmd(&mut db, &[b"XDEL", b"foo", b"1-2", b"1-4"]);
        let resp = cmd(
            &mut db,
            &[
                b"XAUTOCLAIM",
                b"foo",
                b"group",
                b"alice",
                b"0",
                b"0-0",
                b"JUSTID",
            ],
        );
        assert_eq!(
            val(resp),
            arr(vec![
                blk(b"0-0"),
                arr(vec![
                    blk(b"1-0"),
                    blk(b"1-1"),
                    blk(b"1-3"),
                    blk(b"1-5"),
                    blk(b"1-6")
                ]),
                arr(vec![blk(b"1-2"), blk(b"1-4")]),
            ])
        );
    }

    /// Port of `StreamFamilyTest.AutoClaimPelItemsFromAnotherConsumer`.
    #[test]
    fn autoclaim_pel_items_from_another_consumer() {
        let mut db = DbSlice::new(0);
        let mut now = 0u64;
        let id1 = bulk_of(cmd_at(
            &mut db,
            now,
            &[b"XADD", b"mystream", b"*", b"a", b"1"],
        ));
        let id2 = bulk_of(cmd_at(
            &mut db,
            now,
            &[b"XADD", b"mystream", b"*", b"b", b"2"],
        ));
        let id3 = bulk_of(cmd_at(
            &mut db,
            now,
            &[b"XADD", b"mystream", b"*", b"c", b"3"],
        ));
        let id4 = bulk_of(cmd_at(
            &mut db,
            now,
            &[b"XADD", b"mystream", b"*", b"d", b"4"],
        ));
        cmd(
            &mut db,
            &[b"XGROUP", b"CREATE", b"mystream", b"mygroup", b"0"],
        );

        let resp = cmd_at(
            &mut db,
            now,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"mygroup",
                b"consumer1",
                b"COUNT",
                b"1",
                b"STREAMS",
                b"mystream",
                b">",
            ],
        );
        assert_eq!(
            val(resp),
            arr(vec![
                arr(vec![
                    blk(b"mystream"),
                    arr(vec![arr(vec![blk(&id1), arr(vec![blk(b"a"), blk(b"1")]),])])
                ])
            ])
        );

        now += 200;
        let resp = cmd_at(
            &mut db,
            now,
            &[
                b"XAUTOCLAIM",
                b"mystream",
                b"mygroup",
                b"consumer2",
                b"10",
                b"-",
                b"COUNT",
                b"1",
            ],
        );
        let v = arr_of(resp);
        assert_eq!(v[0], blk(b"0-0"));
        assert_eq!(
            v[1],
            arr(vec![arr(vec![blk(&id1), arr(vec![blk(b"a"), blk(b"1")])])])
        );
        assert_eq!(v[2], arr(vec![]));

        cmd_at(
            &mut db,
            now,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"mygroup",
                b"consumer1",
                b"COUNT",
                b"3",
                b"STREAMS",
                b"mystream",
                b">",
            ],
        );
        now += 200;

        // Delete item 2 from the stream.
        cmd_at(&mut db, now, &[b"XDEL", b"mystream", id2.as_slice()]);
        let resp = cmd_at(
            &mut db,
            now,
            &[
                b"XAUTOCLAIM",
                b"mystream",
                b"mygroup",
                b"consumer2",
                b"10",
                b"-",
                b"COUNT",
                b"3",
            ],
        );
        let v = arr_of(resp);
        assert_eq!(v[0], blk(&id4));
        assert_eq!(
            v[1],
            arr(vec![
                arr(vec![blk(&id1), arr(vec![blk(b"a"), blk(b"1")])]),
                arr(vec![blk(&id3), arr(vec![blk(b"c"), blk(b"3")])]),
            ])
        );
        assert_eq!(v[2], arr(vec![blk(&id2)]));

        now += 200;
        cmd_at(&mut db, now, &[b"XDEL", b"mystream", id4.as_slice()]);

        let resp = cmd_at(
            &mut db,
            now,
            &[
                b"XAUTOCLAIM",
                b"mystream",
                b"mygroup",
                b"consumer2",
                b"10",
                b"-",
                b"JUSTID",
            ],
        );
        assert_eq!(
            val(resp),
            arr(vec![
                blk(b"0-0"),
                arr(vec![blk(&id1), blk(&id3)]),
                arr(vec![blk(&id4)]),
            ])
        );
    }

    /// Port of `StreamFamilyTest.AutoClaimDelCount`.
    #[test]
    fn autoclaim_del_count() {
        let mut db = DbSlice::new(0);
        cmd(&mut db, &[b"XADD", b"x", b"1-0", b"f", b"v"]);
        cmd(&mut db, &[b"XADD", b"x", b"2-0", b"f", b"v"]);
        cmd(&mut db, &[b"XADD", b"x", b"3-0", b"f", b"v"]);
        cmd(&mut db, &[b"XGROUP", b"CREATE", b"x", b"grp", b"0"]);
        cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"grp",
                b"Alice",
                b"STREAMS",
                b"x",
                b">",
            ],
        );

        cmd(&mut db, &[b"XDEL", b"x", b"1-0"]);
        cmd(&mut db, &[b"XDEL", b"x", b"2-0"]);

        let resp = cmd(
            &mut db,
            &[
                b"XAUTOCLAIM",
                b"x",
                b"grp",
                b"Bob",
                b"0",
                b"0-0",
                b"COUNT",
                b"1",
            ],
        );
        assert_eq!(
            val(resp),
            arr(vec![blk(b"2-0"), arr(vec![]), arr(vec![blk(b"1-0")])])
        );

        let resp = cmd(
            &mut db,
            &[
                b"XAUTOCLAIM",
                b"x",
                b"grp",
                b"Bob",
                b"0",
                b"2-0",
                b"COUNT",
                b"1",
            ],
        );
        assert_eq!(
            val(resp),
            arr(vec![blk(b"3-0"), arr(vec![]), arr(vec![blk(b"2-0")])])
        );

        let resp = cmd(
            &mut db,
            &[
                b"XAUTOCLAIM",
                b"x",
                b"grp",
                b"Bob",
                b"0",
                b"3-0",
                b"COUNT",
                b"1",
            ],
        );
        assert_eq!(
            val(resp),
            arr(vec![
                blk(b"0-0"),
                arr(vec![arr(vec![
                    blk(b"3-0"),
                    arr(vec![blk(b"f"), blk(b"v")])
                ])]),
                arr(vec![]),
            ])
        );

        let resp = cmd(
            &mut db,
            &[b"XPENDING", b"x", b"grp", b"-", b"+", b"10", b"Alice"],
        );
        assert_eq!(val(resp), arr(vec![]));

        let resp = cmd(
            &mut db,
            &[
                b"XAUTOCLAIM",
                b"x",
                b"grp",
                b"Bob",
                b"0",
                b"3-0",
                b"COUNT",
                b"704505322",
            ],
        );
        assert!(err_of(resp).contains("COUNT"));
    }

    /// Port of `StreamFamilyTest.XClaimWithNonExistentGroup`.
    #[test]
    fn xclaim_with_nonexistent_group() {
        let mut db = DbSlice::new(0);
        cmd(
            &mut db,
            &[b"XADD", b"mystream", b"1-0", b"field1", b"value1"],
        );
        cmd(
            &mut db,
            &[b"XADD", b"mystream", b"1-1", b"field2", b"value2"],
        );

        let resp = cmd(
            &mut db,
            &[
                b"XCLAIM",
                b"mystream",
                b"nonexistent-group",
                b"consumer1",
                b"0",
                b"1-0",
            ],
        );
        assert_eq!(val(resp), arr(vec![]));

        let resp = cmd(
            &mut db,
            &[
                b"XCLAIM",
                b"mystream",
                b"nonexistent-group",
                b"consumer1",
                b"0",
                b"1-0",
                b"1-1",
            ],
        );
        assert_eq!(val(resp), arr(vec![]));

        let resp = cmd(
            &mut db,
            &[
                b"XCLAIM",
                b"mystream",
                b"nonexistent-group",
                b"consumer1",
                b"0",
                b"1-0",
                b"JUSTID",
            ],
        );
        assert_eq!(val(resp), arr(vec![]));
    }

    /// Port of `StreamFamilyTest.XsetIdSmallerMaxDeleted`.
    #[test]
    fn xsetid_smaller_max_deleted() {
        let mut db = DbSlice::new(0);
        cmd(&mut db, &[b"XADD", b"x", b"1-1", b"a", b"1"]);
        cmd(&mut db, &[b"XADD", b"x", b"1-2", b"b", b"2"]);
        cmd(&mut db, &[b"XADD", b"x", b"1-3", b"c", b"3"]);
        cmd(&mut db, &[b"XDEL", b"x", b"1-2"]);
        cmd(&mut db, &[b"XDEL", b"x", b"1-3"]);

        let resp = cmd(&mut db, &[b"XINFO", b"STREAM", b"x"]);
        let info = arr_of(resp);
        let mut max_del = None;
        for chunk in info.chunks(2) {
            if chunk[0] == blk(b"max-deleted-entry-id") {
                max_del = Some(chunk[1].clone());
                break;
            }
        }
        assert_eq!(max_del, Some(blk(b"1-3")));

        let resp = cmd(&mut db, &[b"XSETID", b"x", b"1-2"]);
        assert!(err_of(resp).contains("smaller"));
    }

    /// Port of `StreamFamilyTest.XAutoClaimEmptyConsumer`.
    #[test]
    fn xautoclaim_empty_consumer() {
        let mut db = DbSlice::new(0);
        cmd(&mut db, &[b"XADD", b"stream4", b"*", b"field", b"val1"]);
        cmd(
            &mut db,
            &[b"XGROUP", b"CREATE", b"stream4", b"group2", b"0"],
        );
        let resp = cmd(
            &mut db,
            &[b"XAUTOCLAIM", b"stream4", b"group2", b"", b"0", b"0-0"],
        );
        assert!(matches!(resp, CmdResult::Err(_)));
    }

    /// Port of `StreamFamilyTest.XInfoGroups`.
    #[test]
    fn xinfo_groups() {
        let mut db = DbSlice::new(0);
        cmd(
            &mut db,
            &[
                b"XGROUP",
                b"CREATE",
                b"mystream",
                b"mygroup",
                b"$",
                b"MKSTREAM",
            ],
        );

        let resp = cmd(&mut db, &[b"XINFO", b"GROUPS", b"non-existent-stream"]);
        assert!(err_of(resp).contains("no such key"));

        let resp = cmd(&mut db, &[b"XINFO", b"GROUPS", b"mystream"]);
        let info = arr_of(resp);
        assert_eq!(info.len(), 1);
        assert_eq!(
            info[0],
            arr(vec![
                blk(b"name"),
                blk(b"mygroup"),
                blk(b"consumers"),
                RespValue::Integer(0),
                blk(b"pending"),
                RespValue::Integer(0),
                blk(b"last-delivered-id"),
                blk(b"0-0"),
                blk(b"entries-read"),
                RespValue::Nil,
                blk(b"lag"),
                RespValue::Integer(0),
            ])
        );

        cmd(
            &mut db,
            &[
                b"XGROUP",
                b"CREATECONSUMER",
                b"mystream",
                b"mygroup",
                b"consumer1",
            ],
        );
        cmd(
            &mut db,
            &[
                b"XGROUP",
                b"CREATECONSUMER",
                b"mystream",
                b"mygroup",
                b"consumer2",
            ],
        );
        let resp = cmd(&mut db, &[b"XINFO", b"GROUPS", b"mystream"]);
        let info = arr_of(resp);
        assert_eq!(info.len(), 1);
        assert_eq!(*idx(&info[0], 3), RespValue::Integer(2));

        cmd(&mut db, &[b"XADD", b"mystream", b"1-0", b"test-field-1", b"test-value-1"]);
        cmd(&mut db, &[b"XADD", b"mystream", b"2-0", b"test-field-2", b"test-value-2"]);
        let resp = cmd(&mut db, &[b"XINFO", b"GROUPS", b"mystream"]);
        let info = arr_of(resp);
        assert_eq!(*idx(&info[0], 11), RespValue::Integer(2));
        assert_eq!(*idx(&info[0], 7), blk(b"0-0"));

        cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"mygroup",
                b"consumer1",
                b"STREAMS",
                b"mystream",
                b">",
            ],
        );
        let resp = cmd(&mut db, &[b"XINFO", b"GROUPS", b"mystream"]);
        let info = arr_of(resp);
        assert_eq!(
            info[0],
            arr(vec![
                blk(b"name"),
                blk(b"mygroup"),
                blk(b"consumers"),
                RespValue::Integer(2),
                blk(b"pending"),
                RespValue::Integer(2),
                blk(b"last-delivered-id"),
                blk(b"2-0"),
                blk(b"entries-read"),
                RespValue::Integer(2),
                blk(b"lag"),
                RespValue::Integer(0),
            ])
        );

        cmd(&mut db, &[b"XACK", b"mystream", b"mygroup", b"1-0"]);
        cmd(&mut db, &[b"XACK", b"mystream", b"mygroup", b"2-0"]);
        let resp = cmd(&mut db, &[b"XINFO", b"GROUPS", b"mystream"]);
        let info = arr_of(resp);
        assert_eq!(
            info[0],
            arr(vec![
                blk(b"name"),
                blk(b"mygroup"),
                blk(b"consumers"),
                RespValue::Integer(2),
                blk(b"pending"),
                RespValue::Integer(0),
                blk(b"last-delivered-id"),
                blk(b"2-0"),
                blk(b"entries-read"),
                RespValue::Integer(2),
                blk(b"lag"),
                RespValue::Integer(0),
            ])
        );
    }

    /// Port of `StreamFamilyTest.XInfoConsumers`.
    #[test]
    fn xinfo_consumers() {
        let mut db = DbSlice::new(0);
        cmd(
            &mut db,
            &[
                b"XGROUP",
                b"CREATE",
                b"mystream",
                b"mygroup",
                b"$",
                b"MKSTREAM",
            ],
        );

        let resp = cmd(&mut db, &[b"XINFO", b"CONSUMERS", b"mystream", b"mygroup"]);
        assert_eq!(val(resp), arr(vec![]));

        let resp = cmd(
            &mut db,
            &[b"XINFO", b"CONSUMERS", b"non-existent-stream", b"mygroup"],
        );
        assert!(err_of(resp).contains("no such key"));

        let resp = cmd(
            &mut db,
            &[b"XINFO", b"CONSUMERS", b"mystream", b"non-existent-group"],
        );
        assert!(err_of(resp).contains("NOGROUP"));

        cmd(
            &mut db,
            &[
                b"XGROUP",
                b"CREATECONSUMER",
                b"mystream",
                b"mygroup",
                b"first-consumer",
            ],
        );
        cmd(
            &mut db,
            &[
                b"XGROUP",
                b"CREATECONSUMER",
                b"mystream",
                b"mygroup",
                b"second-consumer",
            ],
        );
        let resp = cmd(&mut db, &[b"XINFO", b"CONSUMERS", b"mystream", b"mygroup"]);
        let info = arr_of(resp);
        assert_eq!(info.len(), 2);
        assert_eq!(info[0].as_array().map(|a| a.len()), Some(8));
        assert_eq!(info[1].as_array().map(|a| a.len()), Some(8));
        assert_eq!(*idx(&info[0], 1), blk(b"first-consumer"));
        assert_eq!(*idx(&info[1], 1), blk(b"second-consumer"));

        cmd(&mut db, &[b"XADD", b"mystream", b"1-0", b"test-field-1", b"test-value-1"]);
        cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"mygroup",
                b"consumer1",
                b"STREAMS",
                b"mystream",
                b">",
            ],
        );
        // Consumers iterate lexicographically (rax order in the reference), so
        // the list is [consumer1, first-consumer, second-consumer].
        let resp = cmd(&mut db, &[b"XINFO", b"CONSUMERS", b"mystream", b"mygroup"]);
        let info = arr_of(resp);
        assert_eq!(*idx(&info[0], 1), blk(b"consumer1"));
        assert_eq!(*idx(&info[0], 3), RespValue::Integer(1)); // pending for consumer1
        assert_eq!(*idx(&info[1], 3), RespValue::Integer(0)); // pending for first-consumer
    }

    /// Port of `StreamFamilyTest.XInfoStream`.
    #[test]
    fn xinfo_stream() {
        let mut db = DbSlice::new(0);
        cmd(
            &mut db,
            &[
                b"XGROUP",
                b"CREATE",
                b"mystream",
                b"mygroup",
                b"$",
                b"MKSTREAM",
            ],
        );
        cmd(
            &mut db,
            &[
                b"XGROUP",
                b"CREATECONSUMER",
                b"mystream",
                b"mygroup",
                b"first-consumer",
            ],
        );

        let resp = cmd(&mut db, &[b"XINFO", b"STREAM", b"non-existent-stream"]);
        assert!(err_of(resp).contains("no such key"));

        let resp = cmd(&mut db, &[b"XINFO", b"STREAM", b"mystream", b"extra-arg"]);
        assert!(err_of(resp).contains("unknown subcommand or wrong number of arguments for 'STREAM'"));
        let resp = cmd(&mut db, &[b"XINFO", b"STREAM", b"mystream", b"FULL", b"COUNT"]);
        assert!(err_of(resp).contains("unknown subcommand or wrong number of arguments for 'STREAM'"));
        let resp = cmd(&mut db, &[b"XINFO", b"STREAM", b"mystream", b"FULL", b"COUNT", b"a"]);
        assert!(err_of(resp).contains("value is not an integer or out of range"));

        let resp = cmd(&mut db, &[b"XINFO", b"STREAM", b"mystream"]);
        let info = arr_of(resp);
        assert_eq!(info.len(), 20);
        assert_eq!(
            RespValue::Array(info),
            arr(vec![
                blk(b"length"),
                RespValue::Integer(0),
                blk(b"radix-tree-keys"),
                RespValue::Integer(0),
                blk(b"radix-tree-nodes"),
                RespValue::Integer(1),
                blk(b"last-generated-id"),
                blk(b"0-0"),
                blk(b"max-deleted-entry-id"),
                blk(b"0-0"),
                blk(b"entries-added"),
                RespValue::Integer(0),
                blk(b"recorded-first-entry-id"),
                blk(b"0-0"),
                blk(b"groups"),
                RespValue::Integer(1),
                blk(b"first-entry"),
                RespValue::NilArray,
                blk(b"last-entry"),
                RespValue::NilArray,
            ])
        );

        for i in 1..=11 {
            let id = format!("{i}-1");
            cmd(
                &mut db,
                &[b"XADD", b"mystream", id.as_bytes(), b"message", b"msg"],
            );
        }
        let resp = cmd(&mut db, &[b"XINFO", b"STREAM", b"mystream"]);
        let info = arr_of(resp);
        assert_eq!(
            RespValue::Array(info),
            arr(vec![
                blk(b"length"),
                RespValue::Integer(11),
                blk(b"radix-tree-keys"),
                RespValue::Integer(1),
                blk(b"radix-tree-nodes"),
                RespValue::Integer(2),
                blk(b"last-generated-id"),
                blk(b"11-1"),
                blk(b"max-deleted-entry-id"),
                blk(b"0-0"),
                blk(b"entries-added"),
                RespValue::Integer(11),
                blk(b"recorded-first-entry-id"),
                blk(b"1-1"),
                blk(b"groups"),
                RespValue::Integer(1),
                blk(b"first-entry"),
                arr(vec![blk(b"1-1"), arr(vec![blk(b"message"), blk(b"msg")])]),
                blk(b"last-entry"),
                arr(vec![blk(b"11-1"), arr(vec![blk(b"message"), blk(b"msg")])]),
            ])
        );

        // full - default (count is capped at 10).
        let resp = cmd(&mut db, &[b"XINFO", b"STREAM", b"mystream", b"FULL"]);
        let info = arr_of(resp);
        assert_eq!(info.len(), 18);
        assert_eq!(info[15].as_array().map(|a| a.len()), Some(10));
        assert_eq!(info[17].as_array().map(|a| a.len()), Some(1));
        assert_eq!(idx(&info[17], 0).as_array().map(|a| a.len()), Some(14));
        assert_eq!(
            *idx(&info[17], 0),
            arr(vec![
                blk(b"name"),
                blk(b"mygroup"),
                blk(b"last-delivered-id"),
                blk(b"0-0"),
                blk(b"entries-read"),
                RespValue::Nil,
                blk(b"lag"),
                RespValue::Integer(11),
                blk(b"pel-count"),
                RespValue::Integer(0),
                blk(b"pending"),
                arr(vec![]),
                blk(b"consumers"),
                arr(vec![arr(vec![
                    blk(b"name"),
                    blk(b"first-consumer"),
                    blk(b"seen-time"),
                    RespValue::Integer(0),
                    blk(b"active-time"),
                    RespValue::Integer(-1),
                    blk(b"pel-count"),
                    RespValue::Integer(0),
                    blk(b"pending"),
                    arr(vec![]),
                ])]),
            ])
        );

        let resp = cmd(
            &mut db,
            &[b"XINFO", b"STREAM", b"mystream", b"FULL", b"COUNT", b"5"],
        );
        let info = arr_of(resp);
        assert_eq!(info[15].as_array().map(|a| a.len()), Some(5));

        let resp = cmd(
            &mut db,
            &[b"XINFO", b"STREAM", b"mystream", b"FULL", b"COUNT", b"12"],
        );
        let info = arr_of(resp);
        assert_eq!(info[15].as_array().map(|a| a.len()), Some(11));

        let resp = cmd(
            &mut db,
            &[b"XINFO", b"STREAM", b"mystream", b"FULL", b"COUNT", b"0"],
        );
        let info = arr_of(resp);
        assert_eq!(info[15].as_array().map(|a| a.len()), Some(11));

        cmd(
            &mut db,
            &[
                b"XREADGROUP",
                b"GROUP",
                b"mygroup",
                b"first-consumer",
                b"STREAMS",
                b"mystream",
                b">",
            ],
        );
        let resp = cmd(
            &mut db,
            &[b"XINFO", b"STREAM", b"mystream", b"FULL", b"COUNT", b"0"],
        );
        let info = arr_of(resp);
        assert_eq!(info[15].as_array().map(|a| a.len()), Some(11));
        let group = idx(&info[17], 0).as_array().cloned().unwrap();
        assert_eq!(group[5], RespValue::Integer(11)); // entries-read
        assert_eq!(group[7], RespValue::Integer(0)); // lag
        assert_eq!(group[9], RespValue::Integer(11)); // pel-count
        assert_eq!(group[11].as_array().map(|a| a.len()), Some(11)); // pending
        let consumer = group[13].as_array().unwrap()[0].as_array().cloned().unwrap();
        assert_eq!(consumer[7], RespValue::Integer(11)); // consumer pel-count
        assert_eq!(consumer[9].as_array().map(|a| a.len()), Some(11)); // consumer pending

        cmd(&mut db, &[b"XDEL", b"mystream", b"1-1"]);
        let resp = cmd(&mut db, &[b"XINFO", b"STREAM", b"mystream"]);
        let info = arr_of(resp);
        assert_eq!(
            RespValue::Array(info),
            arr(vec![
                blk(b"length"),
                RespValue::Integer(10),
                blk(b"radix-tree-keys"),
                RespValue::Integer(1),
                blk(b"radix-tree-nodes"),
                RespValue::Integer(2),
                blk(b"last-generated-id"),
                blk(b"11-1"),
                blk(b"max-deleted-entry-id"),
                blk(b"1-1"),
                blk(b"entries-added"),
                RespValue::Integer(11),
                blk(b"recorded-first-entry-id"),
                blk(b"2-1"),
                blk(b"groups"),
                RespValue::Integer(1),
                blk(b"first-entry"),
                arr(vec![blk(b"2-1"), arr(vec![blk(b"message"), blk(b"msg")])]),
                blk(b"last-entry"),
                arr(vec![blk(b"11-1"), arr(vec![blk(b"message"), blk(b"msg")])]),
            ])
        );

        let resp = cmd(
            &mut db,
            &[b"XINFO", b"STREAM", b"mystream", b"FULL", b"COUNT", b"0"],
        );
        let info = arr_of(resp);
        assert_eq!(info[15].as_array().map(|a| a.len()), Some(10));
        let group = idx(&info[17], 0).as_array().cloned().unwrap();
        assert_eq!(group[1], blk(b"mygroup"));
        assert_eq!(group[3], blk(b"11-1")); // last-delivered-id
        assert_eq!(group[5], RespValue::Integer(11)); // entries-read
        assert_eq!(group[7], RespValue::Integer(0)); // lag
        assert_eq!(group[9], RespValue::Integer(11)); // pel-count
        assert_eq!(group[11].as_array().map(|a| a.len()), Some(11)); // pending
        let consumers = group[13].as_array().unwrap();
        assert_eq!(consumers.len(), 1);
        let consumer = consumers[0].as_array().cloned().unwrap();
        assert_eq!(consumer[1], blk(b"first-consumer"));
        assert!(matches!(consumer[3], RespValue::Integer(_))); // seen-time set
        assert_ne!(consumer[5], RespValue::Integer(-1)); // active-time now set
        assert_eq!(consumer[7], RespValue::Integer(11)); // consumer pel-count
        assert_eq!(consumer[9].as_array().map(|a| a.len()), Some(11)); // consumer pending
    }
}
