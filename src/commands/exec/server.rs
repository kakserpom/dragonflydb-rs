use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::commands::{ok, Command, OpContext, ShardPart, KeyRange, FLAG_ADMIN, FLAG_FAST, FLAG_GLOBAL, FLAG_LOCAL, FLAG_READONLY, FLAG_WRITE};
use crate::core::value::ObjType;
use crate::error::{CmdResult, RespError, RespValue};

/// Last completed snapshot timestamp (epoch seconds), backing LASTSAVE. Starts
/// at 0: no save has ever run, mirroring the reference `SaveInfo` default.
static LAST_SAVE: AtomicU64 = AtomicU64::new(0);

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

/// BGSAVE: forward the first error, otherwise the standard "started" reply.
fn merge_bgsave(parts: &[ShardPart], _args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    for p in parts {
        if let CmdResult::Err(e) = &p.result {
            return CmdResult::Err(e.clone());
        }
    }
    CmdResult::Ok(RespValue::bulk("Background saving started"))
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
// ROLE / LASTSAVE / LATENCY / SLOWLOG (connection-thread commands)
// ---------------------------------------------------------------------------

/// ROLE on a standalone master: `["master", []]`. The port never takes the
/// replica role and tracks no connected replicas, mirroring
/// `ServerFamily::Role` with an empty `GetReplicasRoleInfo`.
pub fn local_role(_args: &[Vec<u8>]) -> RespValue {
    RespValue::Array(vec![
        RespValue::bulk("master"),
        RespValue::Array(vec![]),
    ])
}

/// LASTSAVE: epoch seconds of the last completed snapshot (0 before any save).
pub fn local_lastsave(_args: &[Vec<u8>]) -> RespValue {
    RespValue::Integer(LAST_SAVE.load(Ordering::Relaxed) as i64)
}

/// LATENCY: the reference tracks no latency samples and replies with an empty
/// array for LATEST/HISTOGRAM; every other subcommand errors.
pub fn local_latency(args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'latency' command".into());
    }
    match args[1].to_ascii_uppercase().as_slice() {
        b"LATEST" | b"HISTOGRAM" => RespValue::Array(vec![]),
        other => RespValue::Error(unknown_subcmd(other, "LATENCY")),
    }
}

/// SLOWLOG: commands are not timed in this port, so the log is always empty,
/// but the full subcommand surface (HELP/LEN/RESET/GET) is honored. Mirrors
/// `ServerFamily::SlowLog`.
pub fn local_slowlog(args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'slowlog' command".into());
    }
    match args[1].to_ascii_uppercase().as_slice() {
        b"HELP" => RespValue::Array(vec![
            RespValue::Simple("SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".into()),
            RespValue::Simple("GET [<count>]".into()),
            RespValue::Simple("    Return top <count> entries from the slowlog (default: 10, -1 mean all).".into()),
            RespValue::Simple("    Entries are made of:".into()),
            RespValue::Simple("    id, timestamp, time in microseconds, arguments array, client IP and port,".into()),
            RespValue::Simple("    client name".into()),
            RespValue::Simple("LEN".into()),
            RespValue::Simple("    Return the length of the slowlog.".into()),
            RespValue::Simple("RESET".into()),
            RespValue::Simple("    Reset the slowlog.".into()),
            RespValue::Simple("HELP".into()),
            RespValue::Simple("    Prints this help.".into()),
        ]),
        b"LEN" => RespValue::Integer(0),
        b"RESET" => RespValue::Simple("OK".into()),
        b"GET" => slowlog_get(args),
        other => RespValue::Error(unknown_subcmd(other, "SLOWLOG")),
    }
}

fn slowlog_get(args: &[Vec<u8>]) -> RespValue {
    // args = ["SLOWLOG", "GET"[, count]]: 4+ arguments is a parse error.
    if args.len() > 3 {
        return RespValue::Error(unknown_subcmd(b"GET", "SLOWLOG"));
    }
    if args.len() == 3 {
        match crate::util::parse_i64(&args[2]) {
            Some(n) if n >= -1 => {}
            _ => {
                return RespValue::Error(
                    "ERR count should be greater than or equal to -1".into(),
                )
            }
        }
    }
    RespValue::Array(vec![])
}

/// `ERR Unknown subcommand or wrong number of arguments for '<sub>'. Try <cmd>
/// HELP.` (`facade::UnknownSubCmd`).
fn unknown_subcmd(sub: &[u8], cmd: &str) -> String {
    format!(
        "ERR Unknown subcommand or wrong number of arguments for '{}'. Try {} HELP.",
        String::from_utf8_lossy(sub),
        cmd
    )
}

// ---------------------------------------------------------------------------
// MEMORY (shard command; the key lives at argument index 2)
// ---------------------------------------------------------------------------

fn exec_memory(ctx: &mut OpContext) -> CmdResult {
    let sub = ctx.args[1].to_ascii_uppercase();
    match sub.as_slice() {
        b"HELP" => CmdResult::Ok(RespValue::Array(vec![
            RespValue::Simple("MEMORY <subcommand> [<arg> ...]. Subcommands are:".into()),
            RespValue::Simple("STATS".into()),
            RespValue::Simple("    Shows breakdown of memory.".into()),
            RespValue::Simple("USAGE <key> [WITHOUTKEY]".into()),
            RespValue::Simple("    Show memory usage of a key.".into()),
            RespValue::Simple("    If WITHOUTKEY is specified, the key itself is not accounted.".into()),
            RespValue::Simple("HELP".into()),
            RespValue::Simple("    Prints this help.".into()),
        ])),
        b"USAGE" => memory_usage(ctx),
        b"STATS" => CmdResult::Ok(RespValue::Map(vec![
            (
                RespValue::bulk("connections.direct_bytes"),
                RespValue::Integer(0),
            ),
            (
                RespValue::bulk("replication.connections_count"),
                RespValue::Integer(0),
            ),
            (
                RespValue::bulk("replication.direct_bytes"),
                RespValue::Integer(0),
            ),
        ])),
        other => CmdResult::err(unknown_subcmd(other, "MEMORY")),
    }
}

fn memory_usage(ctx: &mut OpContext) -> CmdResult {
    if ctx.args.len() < 3 {
        return CmdResult::Err(RespError::syntax());
    }
    let key = ctx.args[2].as_slice();
    let account_key = !(ctx.args.len() >= 4 && ctx.args[3].eq_ignore_ascii_case(b"WITHOUTKEY"));
    match ctx.db.find(key, ctx.now_ms) {
        Some(value) => {
            let key_size = if account_key { key.len() } else { 0 };
            CmdResult::Ok(RespValue::Integer((key_size + value.malloc_used()) as i64))
        }
        None => CmdResult::Ok(RespValue::Nil),
    }
}

// ---------------------------------------------------------------------------
// DEBUG (shard command; OBJECT's key lives at argument index 2)
// ---------------------------------------------------------------------------

fn exec_debug(ctx: &mut OpContext) -> CmdResult {
    let sub = ctx.args[1].to_ascii_uppercase();
    match sub.as_slice() {
        b"HELP" => CmdResult::Ok(RespValue::Array(vec![
            RespValue::Simple("DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".into()),
            RespValue::Simple("OBJECT <key>".into()),
            RespValue::Simple("    Show low-level info about `key` and associated value.".into()),
            RespValue::Simple("HELP".into()),
            RespValue::Simple("    Prints this help.".into()),
        ])),
        b"OBJECT" if ctx.args.len() >= 3 => debug_object(ctx),
        other => CmdResult::err(unknown_subcmd(other, "DEBUG")),
    }
}

fn debug_object(ctx: &mut OpContext) -> CmdResult {
    let key = ctx.args[2].as_slice();
    match ctx.db.find(key, ctx.now_ms) {
        None => CmdResult::Err(RespError::new("ERR no such key")),
        Some(value) => {
            let mut s = format!(
                "encoding:{} bucket_id:0 slot:0 shard:{}",
                encoding_name(value.obj_type()),
                ctx.db.shard_id()
            );
            if let Some(at) = ctx.db.expire_at(key)
                && let Some(remaining) = at.checked_sub(ctx.now_ms)
            {
                s.push_str(&format!(" ttl:{}ms", remaining));
            }
            CmdResult::Ok(RespValue::Simple(s))
        }
    }
}

/// `EncodingName` (`debugcmd.cc`) for the object types the port stores.
fn encoding_name(t: ObjType) -> &'static str {
    match t {
        ObjType::Str => "raw",
        ObjType::List => "quicklist",
        ObjType::Set | ObjType::Hash => "dense_set",
        ObjType::ZSet => "btree",
        ObjType::Stream => "stream",
        ObjType::Json => "jsonflat",
        ObjType::Sbf | ObjType::Cms | ObjType::Cuckoo | ObjType::Topk => "unknown",
    }
}

// ---------------------------------------------------------------------------
// SAVE / BGSAVE (global commands; each shard snapshots itself)
// ---------------------------------------------------------------------------

fn exec_save(ctx: &mut OpContext) -> CmdResult {
    save_inner(ctx, false)
}

fn exec_bgsave(ctx: &mut OpContext) -> CmdResult {
    save_inner(ctx, true)
}

fn save_inner(ctx: &mut OpContext, is_bgsave: bool) -> CmdResult {
    let mut rest = &ctx.args[1..];
    // SCHEDULE is parsed for client compatibility but is a no-op.
    if is_bgsave && !rest.is_empty() && rest[0].eq_ignore_ascii_case(b"SCHEDULE") {
        rest = &rest[1..];
    }
    if !rest.is_empty() && rest[0].eq_ignore_ascii_case(b"HELP") {
        return CmdResult::Ok(save_help(is_bgsave));
    }
    if rest.len() > 3 {
        return CmdResult::Err(RespError::syntax());
    }
    if !rest.is_empty() {
        let sub = rest[0].to_ascii_uppercase();
        if sub != b"DF" && sub != b"RDB" {
            return CmdResult::err(unknown_subcmd(&sub, "SAVE"));
        }
    }
    let basename = (rest.len() >= 2).then(|| rest[rest.len() - 1].clone());

    let bytes = crate::core::rdb::save_db(ctx.db);
    let path = snapshot_path(ctx.db.shard_id(), basename.as_deref());
    if let Err(e) = std::fs::write(&path, &bytes) {
        return CmdResult::err(format!("ERR {}", e));
    }
    LAST_SAVE.store(ctx.now_ms / 1000, Ordering::Relaxed);
    if is_bgsave {
        CmdResult::Ok(RespValue::bulk("Background saving started"))
    } else {
        CmdResult::Ok(ok())
    }
}

/// Snapshot file path: `$DRAGONFLYDB_RS_DUMP_DIR` (or the working directory)
/// joined with an explicit BASENAME, else `dump.rdb` for shard 0 and
/// `dump-<shard>.rdb` for the other shards of a multi-shard process.
fn snapshot_path(shard_id: usize, basename: Option<&[u8]>) -> PathBuf {
    let name = match basename {
        Some(b) => String::from_utf8_lossy(b).into_owned(),
        None if shard_id == 0 => "dump.rdb".to_string(),
        None => format!("dump-{}.rdb", shard_id),
    };
    match std::env::var_os("DRAGONFLYDB_RS_DUMP_DIR") {
        Some(dir) => PathBuf::from(dir).join(name),
        None => PathBuf::from(name),
    }
}

fn save_help(is_bgsave: bool) -> RespValue {
    let mut v = vec![RespValue::Simple(format!(
        "{} [DF|RDB [CLOUD_URI [BASENAME]]]. Sub-options are:",
        if is_bgsave { "BGSAVE [SCHEDULE]" } else { "SAVE" }
    ))];
    if is_bgsave {
        v.push(RespValue::Simple("SCHEDULE".into()));
        v.push(RespValue::Simple(
            "    Optional. Parsed for client compatibility (no-op).".into(),
        ));
    }
    for line in [
        "DF",
        "    Save in dragonfly-specific snapshotting format (default).",
        "RDB",
        "    Save in standard redis rdb format.",
        "CLOUD_URI",
        "    Specifies a cloud storage URI (s3://, gs://, azure://) to save the snapshot.",
        "BASENAME",
        "    The base filename for the snapshot files. <dbfilename> if omitted",
    ] {
        v.push(RespValue::Simple(line.into()));
    }
    RespValue::Array(v)
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
pub static CMD_ROLE: Command = Command {
    name: "ROLE",
    arity: 1,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_LASTSAVE: Command = Command {
    name: "LASTSAVE",
    arity: 1,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_LATENCY: Command = Command {
    name: "LATENCY",
    arity: -2,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_SLOWLOG: Command = Command {
    name: "SLOWLOG",
    arity: -2,
    flags: FLAG_FAST | FLAG_LOCAL | FLAG_ADMIN,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_MEMORY: Command = Command {
    name: "MEMORY",
    arity: -2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange { first: 2, last: 2, step: 1 },
    exec: exec_memory,
    merge: None,
};
pub static CMD_DEBUG: Command = Command {
    name: "DEBUG",
    arity: -2,
    flags: FLAG_ADMIN,
    key_range: KeyRange { first: 2, last: 2, step: 1 },
    exec: exec_debug,
    merge: None,
};
pub static CMD_SAVE: Command = Command {
    name: "SAVE",
    arity: -1,
    flags: FLAG_ADMIN | FLAG_GLOBAL,
    key_range: KeyRange::NONE,
    exec: exec_save,
    merge: Some(merge_ok),
};
pub static CMD_BGSAVE: Command = Command {
    name: "BGSAVE",
    arity: -1,
    flags: FLAG_ADMIN | FLAG_GLOBAL,
    key_range: KeyRange::NONE,
    exec: exec_bgsave,
    merge: Some(merge_bgsave),
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::value::PrimeValue;
    use crate::core::DbSlice;

    fn b_args(a: &[&str]) -> Vec<Vec<u8>> {
        a.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn db() -> DbSlice {
        DbSlice::new(0)
    }

    fn dispatch_at(db: &mut DbSlice, now: u64, argv: &[Vec<u8>]) -> CmdResult {
        let cmd = crate::commands::lookup(&argv[0])
            .unwrap_or_else(|| panic!("unknown cmd {:?}", argv[0]));
        if let Some(err) = cmd.check_arity(argv.len()) {
            return CmdResult::err(err);
        }
        let owned = cmd.key_range.keys(argv.len());
        let mut ctx = OpContext {
            db,
            args: argv,
            owned_keys: &owned,
            first_key_idx: 1,
            now_ms: now,
        };
        (cmd.exec)(&mut ctx)
    }

    fn render(v: &RespValue) -> String {
        match v {
            RespValue::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            RespValue::Simple(s) => s.clone(),
            RespValue::Integer(i) => i.to_string(),
            RespValue::Nil => "(nil)".into(),
            RespValue::Error(e) => e.clone(),
            RespValue::Array(a) => {
                format!("[{}]", a.iter().map(render).collect::<Vec<_>>().join(", "))
            }
            RespValue::Map(m) => format!("MAP{}", m.len()),
            RespValue::Bool(b) => b.to_string(),
            RespValue::Double(f) => crate::util::format_double(*f),
        }
    }

    fn s(db: &mut DbSlice, argv: &[&str]) -> String {
        render(&dispatch_at(db, 0, &b_args(argv)).into_resp_value())
    }

    /// Redirect SAVE/BGSAVE output into a scratch directory owned by this test
    /// process so tests never write into the repo.
    fn dump_dir() -> std::path::PathBuf {
        static ONCE: std::sync::Once = std::sync::Once::new();
        let dir = std::env::temp_dir().join(format!("dragonflydb_rs_dump_{}", std::process::id()));
        ONCE.call_once(|| {
            std::fs::create_dir_all(&dir).unwrap();
            unsafe { std::env::set_var("DRAGONFLYDB_RS_DUMP_DIR", &dir) };
        });
        dir
    }

    #[test]
    fn role_reply() {
        let v = local_role(&[]);
        assert_eq!(render(&v), "[master, []]");
    }

    #[test]
    fn lastsave_after_save() {
        dump_dir();
        let mut d = db();
        assert_eq!(s(&mut d, &["SET", "k", "v"]), "OK");
        // SAVE at t=1_000_000ms -> epoch 1000s recorded for LASTSAVE.
        assert_eq!(
            render(&dispatch_at(&mut d, 1_000_000, &b_args(&["SAVE"])).into_resp_value()),
            "OK"
        );
        assert_eq!(render(&local_lastsave(&[])), "1000");
    }

    #[test]
    fn latency_replies() {
        assert_eq!(render(&local_latency(&b_args(&["LATENCY", "LATEST"]))), "[]");
        assert_eq!(
            render(&local_latency(&b_args(&["LATENCY", "HISTOGRAM"]))),
            "[]"
        );
        assert_eq!(
            render(&local_latency(&b_args(&["LATENCY", "RESET"]))),
            "ERR Unknown subcommand or wrong number of arguments for 'RESET'. Try LATENCY HELP."
        );
    }

    #[test]
    fn slowlog_replies() {
        assert_eq!(render(&local_slowlog(&b_args(&["SLOWLOG", "LEN"]))), "0");
        assert_eq!(
            render(&local_slowlog(&b_args(&["SLOWLOG", "RESET"]))),
            "OK"
        );
        assert_eq!(render(&local_slowlog(&b_args(&["SLOWLOG", "GET"]))), "[]");
        assert_eq!(
            render(&local_slowlog(&b_args(&["SLOWLOG", "GET", "10"]))),
            "[]"
        );
        assert_eq!(
            render(&local_slowlog(&b_args(&["SLOWLOG", "GET", "-1"]))),
            "[]"
        );
        assert_eq!(
            render(&local_slowlog(&b_args(&["SLOWLOG", "GET", "-2"]))),
            "ERR count should be greater than or equal to -1"
        );
        assert_eq!(
            render(&local_slowlog(&b_args(&["SLOWLOG", "GET", "1", "2"]))),
            "ERR Unknown subcommand or wrong number of arguments for 'GET'. Try SLOWLOG HELP."
        );
        assert_eq!(
            render(&local_slowlog(&b_args(&["SLOWLOG", "BOGUS"]))),
            "ERR Unknown subcommand or wrong number of arguments for 'BOGUS'. Try SLOWLOG HELP."
        );
        assert!(
            render(&local_slowlog(&b_args(&["SLOWLOG", "HELP"])))
                .contains("SLOWLOG <subcommand>")
        );
    }

    #[test]
    fn memory_usage() {
        let mut d = db();
        // A value short enough to be stored inline has MallocUsed() == 0, so
        // USAGE reports just the key length.
        assert_eq!(s(&mut d, &["SET", "key", "val"]), "OK");
        assert_eq!(s(&mut d, &["MEMORY", "USAGE", "key"]), "3");
        assert_eq!(s(&mut d, &["MEMORY", "USAGE", "key", "WITHOUTKEY"]), "0");
        assert_eq!(s(&mut d, &["MEMORY", "USAGE", "nosuch"]), "(nil)");

        // A heap-backed value is accounted: key(3) + malloc_used(30-char = 24).
        assert_eq!(s(&mut d, &["SET", "big", "x".repeat(30).as_str()]), "OK");
        assert_eq!(s(&mut d, &["MEMORY", "USAGE", "big"]), "27");
        assert_eq!(s(&mut d, &["MEMORY", "USAGE", "big", "WITHOUTKEY"]), "24");

        assert_eq!(s(&mut d, &["MEMORY", "USAGE"]), "ERR syntax error");
        assert_eq!(
            s(&mut d, &["MEMORY", "BOGUS"]),
            "ERR Unknown subcommand or wrong number of arguments for 'BOGUS'. Try MEMORY HELP."
        );
        assert!(s(&mut d, &["MEMORY", "HELP"]).contains("MEMORY <subcommand>"));
    }

    #[test]
    fn memory_stats() {
        let mut d = db();
        assert_eq!(s(&mut d, &["MEMORY", "STATS"]), "MAP3");
    }

    #[test]
    fn debug_object() {
        let mut d = db();
        assert_eq!(s(&mut d, &["SET", "k", "v"]), "OK");
        assert!(s(&mut d, &["DEBUG", "OBJECT", "k"])
            .starts_with("encoding:raw bucket_id:0 slot:0 shard:0"));
        assert_eq!(s(&mut d, &["DEBUG", "OBJECT", "nosuch"]), "ERR no such key");
        assert_eq!(
            s(&mut d, &["DEBUG", "BOGUS"]),
            "ERR Unknown subcommand or wrong number of arguments for 'BOGUS'. Try DEBUG HELP."
        );
        assert!(s(&mut d, &["DEBUG", "HELP"]).contains("DEBUG <subcommand>"));
    }

    #[test]
    fn save_writes_snapshot() {
        let dir = dump_dir();
        let mut d = db();
        assert_eq!(s(&mut d, &["SET", "k", "v"]), "OK");
        assert_eq!(s(&mut d, &["SAVE", "RDB", "save_writes_snapshot.rdb"]), "OK");

        let bytes = std::fs::read(dir.join("save_writes_snapshot.rdb")).unwrap();
        let decoded = crate::core::rdb::decode_snapshot(&bytes, 0);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, b"k");
        assert!(matches!(&decoded[0].1, PrimeValue::Str(s) if s.as_bytes() == b"v"));
    }

    #[test]
    fn save_roundtrip_all_types() {
        let dir = dump_dir();
        let mut d = db();
        for (key, argv, expect) in [
            ("str", &["SET", "str", "s"] as &[&str], "OK"),
            ("list", &["RPUSH", "list", "a", "b"], "2"),
            ("set", &["SADD", "set", "x", "y"], "2"),
            ("hash", &["HSET", "hash", "f", "1"], "1"),
            ("zset", &["ZADD", "zset", "1.5", "m"], "1"),
        ] {
            assert_eq!(s(&mut d, argv), expect, "setup {}", key);
        }
        assert_eq!(
            s(&mut d, &["SAVE", "RDB", "save_roundtrip_all_types.rdb"]),
            "OK"
        );

        // Decoded keys come back in sorted order.
        let bytes = std::fs::read(dir.join("save_roundtrip_all_types.rdb")).unwrap();
        let decoded = crate::core::rdb::decode_snapshot(&bytes, 0);
        assert_eq!(decoded.len(), 5);
        let by_key: std::collections::BTreeMap<Vec<u8>, &PrimeValue> =
            decoded.iter().map(|(k, v)| (k.clone(), v)).collect();
        assert!(matches!(by_key[b"hash".as_slice()], PrimeValue::Hash(_)));
        assert!(matches!(by_key[b"list".as_slice()], PrimeValue::List(_)));
        assert!(matches!(by_key[b"set".as_slice()], PrimeValue::Set(_)));
        assert!(matches!(
            by_key[b"str".as_slice()],
            PrimeValue::Str(s) if s.as_bytes() == b"s"
        ));
        assert!(matches!(by_key[b"zset".as_slice()], PrimeValue::ZSet(_)));
    }

    #[test]
    fn save_unknown_subcmd() {
        let mut d = db();
        assert_eq!(
            s(&mut d, &["SAVE", "DFX"]),
            "ERR Unknown subcommand or wrong number of arguments for 'DFX'. Try SAVE HELP."
        );
    }

    #[test]
    fn save_help() {
        let mut d = db();
        assert!(s(&mut d, &["SAVE", "HELP"]).contains("SAVE [DF|RDB"));
        assert!(s(&mut d, &["BGSAVE", "HELP"]).contains("BGSAVE [SCHEDULE]"));
    }

    #[test]
    fn bgsave_replies() {
        dump_dir();
        let mut d = db();
        assert_eq!(s(&mut d, &["BGSAVE"]), "Background saving started");
        assert_eq!(
            s(&mut d, &["BGSAVE", "SCHEDULE"]),
            "Background saving started"
        );
    }
}
