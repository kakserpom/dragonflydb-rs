use std::time::{SystemTime, UNIX_EPOCH};

use crate::commands::{ok, Command, OpContext, ShardPart, KeyRange, FLAG_ADMIN, FLAG_FAST, FLAG_GLOBAL, FLAG_LOCAL, FLAG_READONLY, FLAG_WRITE};
use crate::error::{CmdResult, RespError, RespValue};

/// Stub for commands handled entirely on the connection (IO) thread.
fn local_stub(_ctx: &mut OpContext) -> CmdResult {
    CmdResult::Err(RespError::new("ERR internal: local command should not reach a shard"))
}

// ---------------------------------------------------------------------------
// GLOBAL commands (run on every shard, merged)
// ---------------------------------------------------------------------------

fn exec_dbsize(ctx: &mut OpContext) -> CmdResult {
    CmdResult::Ok(RespValue::Integer(ctx.db.key_count() as i64))
}

fn merge_dbsize(parts: &[ShardPart], _args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    let mut total = 0i64;
    for p in parts {
        if let CmdResult::Ok(RespValue::Integer(i)) = &p.result {
            total += i;
        }
    }
    CmdResult::Ok(RespValue::Integer(total))
}

fn exec_flush(ctx: &mut OpContext) -> CmdResult {
    // Drain the prime table.
    let keys: Vec<crate::core::CompactString> = ctx.db.iter().map(|(k, _)| k.clone()).collect();
    for k in keys {
        ctx.db.remove(k.as_bytes());
    }
    // Dirty every WATCH in this DB, including watched keys that never existed.
    ctx.db.bump_db_epoch();
    CmdResult::Ok(ok())
}

fn merge_ok(parts: &[ShardPart], _args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    for p in parts {
        if let CmdResult::Err(e) = &p.result {
            return CmdResult::Err(e.clone());
        }
    }
    CmdResult::Ok(ok())
}

// INFO is executed on shards to collect stats, then merged.
fn exec_info(ctx: &mut OpContext) -> CmdResult {
    let mut expires = 0u64;
    for (k, _) in ctx.db.iter() {
        if let Some(at) = ctx.db.expire_at(k.as_bytes()) {
            let _ = at;
            expires += 1;
        }
    }
    CmdResult::Ok(RespValue::Array(vec![
        RespValue::Integer(ctx.db.key_count() as i64),
        RespValue::Integer(expires as i64),
    ]))
}

fn merge_info(parts: &[ShardPart], _args: &[Vec<u8>], _keys: &[usize], now_ms: u64) -> CmdResult {
    let (mut keys, mut expires) = (0i64, 0i64);
    for p in parts {
        if let CmdResult::Ok(RespValue::Array(a)) = &p.result
            && let (Some(RespValue::Integer(k)), Some(RespValue::Integer(e))) = (a.first(), a.get(1))
        {
            keys += k;
            expires += e;
        }
    }
    let uptime = now_ms / 1000;
    let mut lines = String::new();
    lines.push_str("# Server\r\n");
    lines.push_str("redis_version:7.2.0\r\n");
    lines.push_str("redis_mode:standalone\r\n");
    lines.push_str("os:macos\r\n");
    lines.push_str("arch_bits:64\r\n");
    lines.push_str("process_id:1\r\n");
    lines.push_str(&format!("uptime_in_seconds:{}\r\n", uptime));
    lines.push_str("# Clients\r\n");
    lines.push_str("connected_clients:0\r\n");
    lines.push_str("# Memory\r\n");
    lines.push_str("used_memory:0\r\n");
    lines.push_str("# Persistence\r\n");
    lines.push_str("loading:0\r\n");
    lines.push_str("# Stats\r\n");
    lines.push_str("total_commands_processed:0\r\n");
    lines.push_str("instantaneous_ops_per_sec:0\r\n");
    lines.push_str("# Keyspace\r\n");
    lines.push_str(&format!("db0:keys={},expires={},avg_ttl=0\r\n", keys, expires));
    CmdResult::Ok(RespValue::Bulk(lines.into_bytes()))
}

// ---------------------------------------------------------------------------
// LOCAL commands handled by the connection thread
// ---------------------------------------------------------------------------

pub fn local_ping(args: &[Vec<u8>]) -> RespValue {
    if args.len() > 2 {
        return RespValue::Error("ERR wrong number of arguments for 'ping' command".into());
    }
    match args.get(1) {
        Some(msg) => RespValue::Bulk(msg.clone()),
        None => RespValue::Simple("PONG".into()),
    }
}

pub fn local_echo(args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'echo' command".into());
    }
    RespValue::Bulk(args[1].clone())
}

pub fn local_select(args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'select' command".into());
    }
    match crate::util::parse_i64(&args[1]) {
        Some(n) if n >= 0 && (n as usize) < crate::server::MAX_DB => RespValue::Simple("OK".into()),
        _ => RespValue::Error("ERR DB index is out of range".into()),
    }
}

pub fn local_auth(args: &[Vec<u8>]) -> RespValue {
    // No password configured: accept anything.
    if args.len() < 2 || args.len() > 3 {
        return RespValue::Error("ERR wrong number of arguments for 'auth' command".into());
    }
    RespValue::Simple("OK".into())
}

pub fn local_command(_args: &[Vec<u8>]) -> RespValue {
    RespValue::Array(vec![])
}

pub fn local_hello(args: &[Vec<u8>]) -> RespValue {
    let proto = args.get(1).and_then(|a| crate::util::parse_i64(a)).unwrap_or(2);
    if proto != 2 && proto != 3 {
        return RespValue::Error("NOPROTO unsupported protocol version".into());
    }
    let mut m: Vec<(RespValue, RespValue)> = vec![
        (RespValue::Bulk(b"server".to_vec()), RespValue::Bulk(b"dragonflydb-rs".to_vec())),
        (RespValue::Bulk(b"version".to_vec()), RespValue::Bulk(b"0.1.0".to_vec())),
        (RespValue::Bulk(b"proto".to_vec()), RespValue::Integer(2)),
        (RespValue::Bulk(b"id".to_vec()), RespValue::Integer(0)),
        (RespValue::Bulk(b"mode".to_vec()), RespValue::Bulk(b"standalone".to_vec())),
        (RespValue::Bulk(b"role".to_vec()), RespValue::Bulk(b"master".to_vec())),
        (RespValue::Bulk(b"modules".to_vec()), RespValue::Array(vec![])),
    ];
    let _ = &mut m;
    RespValue::Map(m)
}

pub fn local_config(args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'config' command".into());
    }
    match args[1].to_ascii_uppercase().as_slice() {
        b"GET" => RespValue::Array(vec![]),
        b"SET" => RespValue::Simple("OK".into()),
        b"RESETSTAT" => RespValue::Simple("OK".into()),
        _ => RespValue::Error("ERR Unknown CONFIG subcommand or wrong number of arguments for 'config' command".into()),
    }
}

pub fn local_client(args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'client' command".into());
    }
    match args[1].to_ascii_uppercase().as_slice() {
        b"SETNAME" | b"SETINFO" => RespValue::Simple("OK".into()),
        b"GETNAME" => RespValue::Bulk(b"".to_vec()),
        b"ID" => RespValue::Integer(0),
        b"INFO" => RespValue::Bulk(b"id=0 addr=127.0.0.1:0 fd=0 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client user=default redir=-1 resp=2".to_vec()),
        b"NO-EVICT" | b"NO-TOUCH" => RespValue::Simple("OK".into()),
        b"LIST" => RespValue::Array(vec![]),
        _ => RespValue::Error("ERR Unknown CLIENT subcommand or wrong number of arguments for 'client' command".into()),
    }
}

pub fn local_time(_args: &[Vec<u8>]) -> RespValue {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs();
    let micros = now.subsec_micros();
    RespValue::Array(vec![
        RespValue::Bulk(secs.to_string().into_bytes()),
        RespValue::Bulk(micros.to_string().into_bytes()),
    ])
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

// ---------------------------------------------------------------------------
// Command definitions
// ---------------------------------------------------------------------------

pub static CMD_PING: Command = Command {
    name: "PING",
    arity: -1,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_ECHO: Command = Command {
    name: "ECHO",
    arity: 2,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_SELECT: Command = Command {
    name: "SELECT",
    arity: 2,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_AUTH: Command = Command {
    name: "AUTH",
    arity: -2,
    flags: FLAG_FAST | FLAG_LOCAL | FLAG_ADMIN,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_COMMAND: Command = Command {
    name: "COMMAND",
    arity: -1,
    flags: FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_HELLO: Command = Command {
    name: "HELLO",
    arity: -1,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_INFO: Command = Command {
    name: "INFO",
    arity: -1,
    flags: FLAG_READONLY | FLAG_GLOBAL,
    key_range: KeyRange::NONE,
    exec: exec_info,
    merge: Some(merge_info),
};
pub static CMD_CONFIG: Command = Command {
    name: "CONFIG",
    arity: -2,
    flags: FLAG_LOCAL | FLAG_ADMIN,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_CLIENT: Command = Command {
    name: "CLIENT",
    arity: -2,
    flags: FLAG_LOCAL | FLAG_ADMIN,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_DBSIZE: Command = Command {
    name: "DBSIZE",
    arity: 1,
    flags: FLAG_READONLY | FLAG_FAST | FLAG_GLOBAL,
    key_range: KeyRange::NONE,
    exec: exec_dbsize,
    merge: Some(merge_dbsize),
};
pub static CMD_FLUSHDB: Command = Command {
    name: "FLUSHDB",
    arity: -1,
    flags: FLAG_WRITE | FLAG_GLOBAL | FLAG_ADMIN,
    key_range: KeyRange::NONE,
    exec: exec_flush,
    merge: Some(merge_ok),
};
pub static CMD_FLUSHALL: Command = Command {
    name: "FLUSHALL",
    arity: -1,
    flags: FLAG_WRITE | FLAG_GLOBAL | FLAG_ADMIN,
    key_range: KeyRange::NONE,
    exec: exec_flush,
    merge: Some(merge_ok),
};
pub static CMD_TIME: Command = Command {
    name: "TIME",
    arity: 1,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_MULTI: Command = Command {
    name: "MULTI",
    arity: 1,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_EXEC: Command = Command {
    name: "EXEC",
    arity: 1,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_DISCARD: Command = Command {
    name: "DISCARD",
    arity: 1,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_WATCH: Command = Command {
    name: "WATCH",
    arity: -2,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_UNWATCH: Command = Command {
    name: "UNWATCH",
    arity: 1,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_RESET: Command = Command {
    name: "RESET",
    arity: 1,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
