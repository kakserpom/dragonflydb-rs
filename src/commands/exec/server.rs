use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use std::cell::RefCell;
use std::fmt::Write as _;

use crate::commands::{
    Command, FLAG_ADMIN, FLAG_FAST, FLAG_GLOBAL, FLAG_LOCAL, FLAG_NOSCRIPT, FLAG_READONLY,
    FLAG_WRITE, KeyRange, OpContext, ShardPart, ok,
};
use crate::core::PrimeValue;
use crate::core::compact::CompactString;
use crate::core::value::ObjType;
use crate::error::{CmdResult, RespError, RespValue};

/// Last completed snapshot timestamp (epoch seconds), backing LASTSAVE. Starts
/// at 0: no save has ever run, mirroring the reference `SaveInfo` default.
static LAST_SAVE: AtomicU64 = AtomicU64::new(0);

/// Stub for commands handled entirely on the connection (IO) thread.
fn local_stub(_ctx: &mut OpContext) -> CmdResult {
    CmdResult::Err(RespError::new(
        "ERR internal: local command should not reach a shard",
    ))
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

// The current server's command-stats map (`ServerEnv::command_stats`),
// installed on the coordinator thread by `coordinator::spawn`. The map itself
// is shared with the IO thread (which bumps on dispatch); the thread-local
// only lets the static INFO merge fn reach it.
/// Per-command invocation counters (`cmd_stats_map`). The alias keeps the
/// hasher implicit in signatures (Clippy `implicit_hasher`).
pub type CommandStatsMap = HashMap<&'static str, u64>;

thread_local! {
    static CURRENT_COMMAND_STATS: RefCell<Option<Arc<Mutex<CommandStatsMap>>>> =
        const { RefCell::new(None) };
}

/// Install `ServerEnv::command_stats` on the coordinator thread.
pub fn set_current_command_stats(stats: Arc<Mutex<CommandStatsMap>>) {
    CURRENT_COMMAND_STATS.with(|s| *s.borrow_mut() = Some(stats));
}

fn current_command_stats() -> Arc<Mutex<CommandStatsMap>> {
    CURRENT_COMMAND_STATS.with(|s| {
        s.borrow()
            .as_ref()
            .expect("command stats installed on coordinator thread")
            .clone()
    })
}

/// Count one execution of `name` (`UpdateCmdStatsMap`).
pub fn bump_cmd_stat(stats: &Mutex<CommandStatsMap>, name: &'static str) {
    let mut stats = stats.lock().unwrap();
    *stats.entry(name).or_default() += 1;
}

/// `CONFIG RESETSTAT`: clear every counter.
pub fn reset_cmd_stats(stats: &Mutex<CommandStatsMap>) {
    stats.lock().unwrap().clear();
}

/// Total commands executed across all names (`GetTotalCmdStats`).
fn total_cmd_calls() -> u64 {
    let stats = current_command_stats();
    stats.lock().unwrap().values().sum()
}

/// `(name, calls)` pairs with `calls > 0`, sorted by (lowercased) name for
/// deterministic INFO output (zero-call commands are skipped, matching
/// `GetMetrics`).
fn cmd_stats_with_calls() -> Vec<(String, u64)> {
    let stats = current_command_stats();
    let stats = stats.lock().unwrap();
    let mut v: Vec<(String, u64)> = stats
        .iter()
        .filter(|(_, c)| **c > 0)
        .map(|(n, c)| (n.to_ascii_lowercase(), *c))
        .collect();
    v.sort_by(|a, b| a.0.cmp(&b.0));
    v
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

/// Renders the INFO body for the requested sections (`ServerFamily::Info`,
/// `GetMetrics`). With no sections, everything except the hidden COMMANDSTATS
/// is emitted (LATENCYSTATS included); unknown section names are skipped.
fn merge_info(parts: &[ShardPart], args: &[Vec<u8>], _keys: &[usize], now_ms: u64) -> CmdResult {
    let (mut keys, mut expires) = (0i64, 0i64);
    for p in parts {
        if let CmdResult::Ok(RespValue::Array(a)) = &p.result
            && let (Some(RespValue::Integer(k)), Some(RespValue::Integer(e))) =
                (a.first(), a.get(1))
        {
            keys += k;
            expires += e;
        }
    }
    let uptime = now_ms / 1000;
    let requested: Vec<String> = args[1..]
        .iter()
        .map(|a| String::from_utf8_lossy(a).to_ascii_lowercase())
        .collect();
    let want = |name: &str| {
        if requested.is_empty() {
            name != "commandstats"
        } else {
            requested
                .iter()
                .any(|r| r == name || r == "all" || r == "everything")
        }
    };

    let mut lines = String::new();
    if want("server") {
        lines.push_str("# Server\r\n");
        lines.push_str("redis_version:7.2.0\r\n");
        lines.push_str("redis_mode:standalone\r\n");
        lines.push_str("os:macos\r\n");
        lines.push_str("arch_bits:64\r\n");
        lines.push_str("process_id:1\r\n");
        write!(lines, "uptime_in_seconds:{uptime}\r\n").unwrap();
    }
    if want("clients") {
        lines.push_str("# Clients\r\nconnected_clients:0\r\n");
    }
    if want("memory") {
        lines.push_str(
            "# Memory\r\nused_memory:0\r\nreplication_streaming_buffer_bytes:0\r\n\
             replication_full_sync_buffer_bytes:0\r\n",
        );
    }
    if want("persistence") {
        lines.push_str("# Persistence\r\nloading:0\r\n");
    }
    if want("stats") {
        write!(
            lines,
            "# Stats\r\ntotal_commands_processed:{}\r\ninstantaneous_ops_per_sec:0\r\n",
            total_cmd_calls()
        )
        .unwrap();
    }
    if want("replication") {
        lines.push_str("# Replication\r\nrole:master\r\nconnected_slaves:0\r\nmaster_replid:0\r\n");
    }
    if want("latencystats") {
        lines.push_str("# Latencystats\r\n");
        for (name, calls) in cmd_stats_with_calls() {
            write!(
                lines,
                "latency_percentiles_usec_{name}:calls={calls},p50=0.000,p99=0.000,p99.9=0.000\r\n"
            )
            .unwrap();
        }
    }
    if want("commandstats") {
        lines.push_str("# Commandstats\r\n");
        for (name, calls) in cmd_stats_with_calls() {
            write!(
                lines,
                "cmdstat_{name}:calls={calls},usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=0\r\n"
            )
            .unwrap();
        }
    }
    if want("cluster") {
        lines.push_str("# Cluster\r\nmigration_errors_total:0\r\n");
    }
    if want("keyspace") {
        write!(
            lines,
            "# Keyspace\r\ndb0:keys={keys},expires={expires},avg_ttl=0\r\n"
        )
        .unwrap();
    }
    CmdResult::Ok(RespValue::Bulk(lines.into_bytes()))
}

// ---------------------------------------------------------------------------
// LOCAL commands handled by the connection thread
// ---------------------------------------------------------------------------

#[must_use]
pub fn local_ping(args: &[Vec<u8>]) -> RespValue {
    if args.len() > 2 {
        return RespValue::Error("ERR wrong number of arguments for 'ping' command".into());
    }
    match args.get(1) {
        Some(msg) => RespValue::Bulk(msg.clone()),
        None => RespValue::Simple("PONG".into()),
    }
}

#[must_use]
pub fn local_echo(args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'echo' command".into());
    }
    RespValue::Bulk(args[1].clone())
}

#[must_use]
pub fn local_select(args: &[Vec<u8>]) -> RespValue {
    if args.len() != 2 {
        return RespValue::Error("ERR wrong number of arguments for 'select' command".into());
    }
    match crate::util::parse_i64(&args[1]) {
        Some(n) if n >= 0 && (n as usize) < crate::server::MAX_DB => RespValue::Simple("OK".into()),
        _ => RespValue::Error("ERR DB index is out of range".into()),
    }
}

#[must_use]
pub fn local_auth(args: &[Vec<u8>]) -> RespValue {
    // No password configured: accept anything.
    if args.len() < 2 || args.len() > 3 {
        return RespValue::Error("ERR wrong number of arguments for 'auth' command".into());
    }
    RespValue::Simple("OK".into())
}

#[must_use]
pub fn local_command(args: &[Vec<u8>]) -> RespValue {
    // `CommandDocs` (server_family.cc): the subcommand is not implemented.
    if args.len() > 1 && args[1].eq_ignore_ascii_case(b"DOCS") {
        return RespValue::Error("ERR COMMAND DOCS Not Implemented".into());
    }
    RespValue::Array(vec![])
}

#[must_use]
pub fn local_hello(args: &[Vec<u8>]) -> RespValue {
    let proto = args
        .get(1)
        .and_then(|a| crate::util::parse_i64(a))
        .unwrap_or(2);
    if proto != 2 && proto != 3 {
        return RespValue::Error("NOPROTO unsupported protocol version".into());
    }
    let mut m: Vec<(RespValue, RespValue)> = vec![
        (
            RespValue::Bulk(b"server".to_vec()),
            RespValue::Bulk(b"dragonflydb-rs".to_vec()),
        ),
        (
            RespValue::Bulk(b"version".to_vec()),
            RespValue::Bulk(b"0.1.0".to_vec()),
        ),
        (RespValue::Bulk(b"proto".to_vec()), RespValue::Integer(2)),
        (RespValue::Bulk(b"id".to_vec()), RespValue::Integer(0)),
        (
            RespValue::Bulk(b"mode".to_vec()),
            RespValue::Bulk(b"standalone".to_vec()),
        ),
        (
            RespValue::Bulk(b"role".to_vec()),
            RespValue::Bulk(b"master".to_vec()),
        ),
        (
            RespValue::Bulk(b"modules".to_vec()),
            RespValue::Array(vec![]),
        ),
    ];
    let _ = &mut m;
    RespValue::Map(m)
}

#[must_use]
pub fn local_config(args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'config' command".into());
    }
    match args[1].to_ascii_uppercase().as_slice() {
        b"GET" => RespValue::Array(vec![]),
        b"SET" | b"RESETSTAT" => RespValue::Simple("OK".into()),
        _ => RespValue::Error(
            "ERR Unknown CONFIG subcommand or wrong number of arguments for 'config' command"
                .into(),
        ),
    }
}

#[must_use]
pub fn local_time(_args: &[Vec<u8>]) -> RespValue {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let micros = now.subsec_micros();
    RespValue::Array(vec![
        RespValue::Bulk(secs.to_string().into_bytes()),
        RespValue::Bulk(micros.to_string().into_bytes()),
    ])
}

/// Test-only fake clock, mirroring the reference `TEST_current_time_ms` +
/// `AdvanceTime`. The binary never touches it; integration tests pin it with
/// [`pin_test_clock`] and move it with [`advance_test_clock`]. Every thread
/// (shard, coordinator, IO) reads the same value through [`now_ms`], so a
/// pinned clock makes TTL math fully deterministic.
static TEST_CLOCK: OnceLock<AtomicU64> = OnceLock::new();

fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[must_use]
pub fn now_ms() -> u64 {
    match TEST_CLOCK.get() {
        Some(c) => c.load(Ordering::Relaxed),
        None => wall_clock_ms(),
    }
}

/// Pin the clock to the current wall-clock time, rounded down to a whole
/// second, and return that value. Idempotent: re-pinning never rewinds a clock
/// a previous test already advanced. Every expiry is stored relative to the
/// returned base, so `field_ttl_ms`/`httl`/`hpexpiretime` replies are exact.
#[must_use]
pub fn pin_test_clock() -> u64 {
    let c = TEST_CLOCK.get_or_init(|| AtomicU64::new((wall_clock_ms() / 1000) * 1000));
    c.load(Ordering::Relaxed)
}

/// Current test-clock value. Panics when the clock was never pinned.
#[must_use]
pub fn test_clock_ms() -> u64 {
    TEST_CLOCK
        .get()
        .expect("test clock not pinned")
        .load(Ordering::Relaxed)
}

/// Advance the test clock by `ms`. Panics when the clock was never pinned.
pub fn advance_test_clock(ms: u64) {
    TEST_CLOCK
        .get()
        .expect("test clock not pinned")
        .fetch_add(ms, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// ROLE / LASTSAVE / LATENCY / SLOWLOG (connection-thread commands)
// ---------------------------------------------------------------------------

/// ROLE on a standalone master: `["master", []]`. The port never takes the
/// replica role and tracks no connected replicas, mirroring
/// `ServerFamily::Role` with an empty `GetReplicasRoleInfo`.
#[must_use]
pub fn local_role(_args: &[Vec<u8>]) -> RespValue {
    RespValue::Array(vec![RespValue::bulk("master"), RespValue::Array(vec![])])
}

/// LASTSAVE: epoch seconds of the last completed snapshot (0 before any save).
pub fn local_lastsave(_args: &[Vec<u8>]) -> RespValue {
    RespValue::Integer(LAST_SAVE.load(Ordering::Relaxed) as i64)
}

/// LATENCY: the reference tracks no latency samples and replies with an empty
/// array for LATEST/HISTOGRAM; every other subcommand errors.
#[must_use]
pub fn local_latency(args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::Error("ERR wrong number of arguments for 'latency' command".into());
    }
    match args[1].to_ascii_uppercase().as_slice() {
        b"LATEST" | b"HISTOGRAM" => RespValue::Array(vec![]),
        other => RespValue::Error(unknown_subcmd(other, "LATENCY")),
    }
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
            RespValue::Simple(
                "    If WITHOUTKEY is specified, the key itself is not accounted.".into(),
            ),
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
        b"DEFRAGMENT" => memory_defragment(ctx),
        other => CmdResult::err(unknown_subcmd(other, "MEMORY")),
    }
}

/// `MEMORY DEFRAGMENT <ratio>` (memory_family.cc): the argument must parse as
/// a float; the port performs no defragmentation.
fn memory_defragment(ctx: &mut OpContext) -> CmdResult {
    let Some(raw) = ctx.args.get(2) else {
        return CmdResult::Err(RespError::syntax());
    };
    match String::from_utf8_lossy(raw).parse::<f64>() {
        Ok(_) => CmdResult::Ok(RespValue::Simple("OK".into())),
        Err(_) => CmdResult::Err(RespError::new("ERR not a valid float")),
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
            RespValue::Simple(
                "DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".into(),
            ),
            RespValue::Simple("OBJECT <key>".into()),
            RespValue::Simple("    Show low-level info about `key` and associated value.".into()),
            RespValue::Simple("HELP".into()),
            RespValue::Simple("    Prints this help.".into()),
        ])),
        b"OBJECT" if ctx.args.len() >= 3 => debug_object(ctx),
        b"POPULATE" if ctx.args.len() >= 3 => debug_populate(ctx),
        other => CmdResult::err(unknown_subcmd(other, "DEBUG")),
    }
}

/// `DEBUG POPULATE [count] [prefix] [val_size]` (debug_family.cc `OpDebug`).
/// `val_size == 0` is rejected; valid invocations insert `count` blobs of that
/// size under `prefix<i>` on this shard.
fn debug_populate(ctx: &mut OpContext) -> CmdResult {
    let Some(count) = crate::util::parse_u64(&ctx.args[2]) else {
        return CmdResult::Err(RespError::syntax());
    };
    let prefix = ctx.args.get(3).cloned().unwrap_or_else(|| b"key".to_vec());
    let val_size = match ctx.args.get(4) {
        Some(v) => match crate::util::parse_u64(v) {
            Some(n) => n,
            None => return CmdResult::Err(RespError::syntax()),
        },
        None => 10,
    };
    if val_size == 0 {
        return CmdResult::Err(RespError::new("ERR val_size must be positive"));
    }
    let blob = CompactString::from("x".repeat(val_size as usize));
    let prefix = String::from_utf8_lossy(&prefix);
    for i in 0..count {
        let key = format!("{prefix}{i}");
        ctx.db.insert(key.as_bytes(), PrimeValue::Str(blob.clone()));
    }
    CmdResult::Ok(RespValue::Simple("OK".into()))
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
                write!(s, " ttl:{remaining}ms").unwrap();
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
// SHRINK (keyed shard command; the key lives at argument index 1)
// ---------------------------------------------------------------------------

/// SHRINK key: the reference compacts the bucket array of a DenseSet-encoded
/// SET/HASH and reports the freed bytes. The port's set/hash storage exposes
/// no bucket array to compact, so valid SET/HASH keys take the reference's
/// "nothing to shrink" fast path (reply 0), missing keys reply nil, and any
/// other type is WRONGTYPE.
fn exec_shrink(ctx: &mut OpContext) -> CmdResult {
    let key = ctx.args[1].as_slice();
    match ctx.db.find(key, ctx.now_ms) {
        None => CmdResult::Ok(RespValue::Nil),
        Some(value) => match value.obj_type() {
            ObjType::Set | ObjType::Hash => CmdResult::Ok(RespValue::Integer(0)),
            _ => CmdResult::Err(RespError::new(
                "WRONGTYPE Key is not a set or hash with DenseSet encoding",
            )),
        },
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
        return CmdResult::err(format!("ERR {e}"));
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
        None => format!("dump-{shard_id}.rdb"),
    };
    match std::env::var_os("DRAGONFLYDB_RS_DUMP_DIR") {
        Some(dir) => PathBuf::from(dir).join(name),
        None => PathBuf::from(name),
    }
}

// ---------------------------------------------------------------------------
// Replication / shutdown / admin (connection-thread commands)
// ---------------------------------------------------------------------------

/// WAIT numreplicas timeout: this standalone port never has connected
/// replicas, so the reply is always `:0` immediately (the reference's
/// `replicas.empty()` fast path). Both arguments must be non-negative
/// integers; anything else is the reference `FInt<0, max>` parse error.
#[must_use]
pub fn local_wait(args: &[Vec<u8>]) -> RespValue {
    for a in &args[1..] {
        match crate::util::parse_i64(a) {
            Some(n) if n >= 0 => {}
            _ => return RespValue::Error("ERR value is not an integer or out of range".into()),
        }
    }
    RespValue::Integer(0)
}

/// Parse SHUTDOWN options ([SAVE|SAFE][NOSAVE][NOW][FORCE][ABORT]) following
/// the reference grammar: SAVE/SAFE are synonyms, SAVE+NOSAVE together are a
/// syntax error, ABORT is unsupported, and anything else fails the grammar.
/// `Ok` means the server may shut down.
pub fn local_shutdown(args: &[Vec<u8>]) -> Result<(), RespValue> {
    let (mut save, mut no_save) = (false, false);
    for opt in &args[1..] {
        match opt.to_ascii_uppercase().as_slice() {
            b"SAVE" | b"SAFE" => save = true,
            b"NOSAVE" => no_save = true,
            b"NOW" | b"FORCE" => {}
            b"ABORT" => {
                return Err(RespValue::Error(
                    "ERR SHUTDOWN ABORT is not supported".into(),
                ));
            }
            _ => return Err(RespValue::Error("ERR syntax error".into())),
        }
    }
    if save && no_save {
        return Err(RespValue::Error("ERR syntax error".into()));
    }
    Ok(())
}

/// REPLCONF option value...: validate the option/value pairs the reference
/// accepts and reply OK. A single `REPLCONF ACK <n>` is answered with silence
/// (this port has no replication flow, matching the reference's no-flow path),
/// represented by `None`. Odd argument counts and unknown options are syntax
/// errors; LISTENING-PORT / CLIENT-VERSION values must be 32-bit integers.
#[must_use]
pub fn local_replconf(args: &[Vec<u8>]) -> Option<RespValue> {
    let rest = &args[1..];
    if rest.len() % 2 == 1 {
        return Some(RespValue::Error("ERR syntax error".into()));
    }
    let single = rest.len() == 2;
    for pair in rest.chunks(2) {
        match pair[0].to_ascii_uppercase().as_slice() {
            b"CAPA" | b"IP-ADDRESS" => {}
            b"LISTENING-PORT" | b"CLIENT-ID" | b"CLIENT-VERSION" => {
                if !is_u32(&pair[1]) {
                    return Some(RespValue::Error(
                        "ERR value is not an integer or out of range".into(),
                    ));
                }
            }
            b"ACK" if single => return None,
            _ => return Some(RespValue::Error("ERR syntax error".into())),
        }
    }
    Some(RespValue::Simple("OK".into()))
}

/// The action a `REPLICAOF`/`SLAVEOF` request asks for (`GetReplicaOfCommand`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaOf {
    /// `REPLICAOF NO ONE`: stop replicating and resume master mode.
    NoOne,
    /// `REPLICAOF <host> <port>`: start replicating from a master.
    Start { host: String, port: u16 },
}

/// Parse a REPLICAOF/SLAVEOF argument list. The reference's
/// `ParseReplicaOf` accepts `NO ONE`, a `host port` pair, or `""` as the host
/// (an empty host restarts the replica in place).
pub fn parse_replicaof(args: &[Vec<u8>]) -> Result<ReplicaOf, RespValue> {
    let no = args.get(1).is_some_and(|a| a.eq_ignore_ascii_case(b"NO"));
    if no {
        if args.get(2).is_some_and(|a| a.eq_ignore_ascii_case(b"ONE")) {
            return Ok(ReplicaOf::NoOne);
        }
        // "NO" without "ONE": the reference's ExpectTag fails.
        return Err(RespValue::Error("ERR syntax error".into()));
    }
    if args.len() == 3 {
        let host = String::from_utf8_lossy(&args[1]).into_owned();
        let port = std::str::from_utf8(&args[2])
            .ok()
            .and_then(|s| s.parse::<u16>().ok());
        return match port {
            Some(p) if p > 0 => Ok(ReplicaOf::Start { host, port: p }),
            _ => Err(RespValue::Error("ERR port is out of range".into())),
        };
    }
    Err(RespValue::Error("ERR syntax error".into()))
}

/// ADDREPLICAOF always errors on this port: the reference rejects the command
/// whenever the server is already a master (note the "OFF" typo upstream).
#[must_use]
pub fn local_addreplicaof(_args: &[Vec<u8>]) -> RespValue {
    RespValue::Error(
        "ERR Calling ADDREPLICAOFF allowed only after server is already a replica".into(),
    )
}

/// REPLTAKEOVER seconds [SAVE]. On a master the reference validates the
/// arguments and returns OK (idempotency semantics); parse/option errors keep
/// the reference order so "Unsupported option" wins over the integer error.
#[must_use]
pub fn local_repltakeover(args: &[Vec<u8>]) -> RespValue {
    let timeout = crate::util::parse_i64(&args[1]);
    let mut rest = &args[2..];
    if rest
        .first()
        .is_some_and(|a| a.eq_ignore_ascii_case(b"SAVE"))
    {
        rest = &rest[1..];
    }
    if let Some(extra) = rest.first() {
        return RespValue::Error(format!(
            "ERR Unsupported option:{}",
            String::from_utf8_lossy(extra)
        ));
    }
    let Some(n) = timeout else {
        return RespValue::Error("ERR value is not an integer or out of range".into());
    };
    if n < 0 {
        return RespValue::Error("ERR timeout is negative".into());
    }
    RespValue::Simple("OK".into())
}

/// MODULE LIST replies with the two statically loaded modules (`ReJSON` v20808,
/// search v21015); the reference errors on any other subcommand.
#[must_use]
pub fn local_module(args: &[Vec<u8>]) -> RespValue {
    let sub = args
        .get(1)
        .map(|a| a.to_ascii_uppercase())
        .unwrap_or_default();
    if sub.as_slice() != b"LIST" {
        return RespValue::Error("ERR syntax error".into());
    }
    RespValue::Array(vec![
        RespValue::Array(vec![
            RespValue::Simple("name".into()),
            RespValue::Simple("ReJSON".into()),
            RespValue::Simple("ver".into()),
            RespValue::Integer(20808),
        ]),
        RespValue::Array(vec![
            RespValue::Simple("name".into()),
            RespValue::Simple("search".into()),
            RespValue::Simple("ver".into()),
            RespValue::Integer(21015),
        ]),
    ])
}

/// DFLY is the replication control protocol (`dflycmd.cc`); the port has no
/// replication stack, so it is rejected explicitly.
#[must_use]
pub fn local_dfly(_args: &[Vec<u8>]) -> RespValue {
    RespValue::Error("ERR DFLY replication control is not supported".into())
}

fn is_u32(s: &[u8]) -> bool {
    std::str::from_utf8(s).is_ok_and(|s| s.parse::<u32>().is_ok())
}

fn save_help(is_bgsave: bool) -> RespValue {
    let mut v = vec![RespValue::Simple(format!(
        "{} [DF|RDB [CLOUD_URI [BASENAME]]]. Sub-options are:",
        if is_bgsave {
            "BGSAVE [SCHEDULE]"
        } else {
            "SAVE"
        }
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
    flags: FLAG_FAST | FLAG_LOCAL | FLAG_NOSCRIPT,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_EXEC: Command = Command {
    name: "EXEC",
    arity: 1,
    flags: FLAG_FAST | FLAG_LOCAL | FLAG_NOSCRIPT,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_DISCARD: Command = Command {
    name: "DISCARD",
    arity: 1,
    flags: FLAG_FAST | FLAG_LOCAL | FLAG_NOSCRIPT,
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
    flags: FLAG_FAST | FLAG_LOCAL | FLAG_NOSCRIPT,
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
    key_range: KeyRange {
        first: 2,
        last: 2,
        step: 1,
    },
    exec: exec_memory,
    merge: None,
};
pub static CMD_DEBUG: Command = Command {
    name: "DEBUG",
    arity: -2,
    flags: FLAG_ADMIN,
    key_range: KeyRange {
        first: 2,
        last: 2,
        step: 1,
    },
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
pub static CMD_PUBLISH: Command = Command {
    name: "PUBLISH",
    arity: 3,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_SUBSCRIBE: Command = Command {
    name: "SUBSCRIBE",
    arity: -2,
    flags: FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_UNSUBSCRIBE: Command = Command {
    name: "UNSUBSCRIBE",
    arity: -1,
    flags: FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_PSUBSCRIBE: Command = Command {
    name: "PSUBSCRIBE",
    arity: -2,
    flags: FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_PUNSUBSCRIBE: Command = Command {
    name: "PUNSUBSCRIBE",
    arity: -1,
    flags: FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_PUBSUB: Command = Command {
    name: "PUBSUB",
    arity: -2,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_SPUBLISH: Command = Command {
    name: "SPUBLISH",
    arity: 3,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_SSUBSCRIBE: Command = Command {
    name: "SSUBSCRIBE",
    arity: -2,
    flags: FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_SUNSUBSCRIBE: Command = Command {
    name: "SUNSUBSCRIBE",
    arity: -1,
    flags: FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_QUIT: Command = Command {
    name: "QUIT",
    arity: 1,
    flags: FLAG_FAST | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_MONITOR: Command = Command {
    name: "MONITOR",
    arity: 1,
    flags: FLAG_ADMIN | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_SHUTDOWN: Command = Command {
    name: "SHUTDOWN",
    arity: -1,
    flags: FLAG_ADMIN | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_WAIT: Command = Command {
    name: "WAIT",
    arity: 3,
    flags: FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_REPLCONF: Command = Command {
    name: "REPLCONF",
    arity: -1,
    flags: FLAG_ADMIN | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_REPLICAOF: Command = Command {
    name: "REPLICAOF",
    arity: -3,
    flags: FLAG_ADMIN | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_SLAVEOF: Command = Command {
    name: "SLAVEOF",
    arity: 3,
    flags: FLAG_ADMIN | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_ADDREPLICAOF: Command = Command {
    name: "ADDREPLICAOF",
    arity: 5,
    flags: FLAG_ADMIN | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_REPLTAKEOVER: Command = Command {
    name: "REPLTAKEOVER",
    arity: -2,
    flags: FLAG_ADMIN | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_MODULE: Command = Command {
    name: "MODULE",
    arity: 2,
    flags: FLAG_ADMIN | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_FUNCTION: Command = Command {
    name: "FUNCTION",
    arity: -2,
    flags: FLAG_LOCAL | FLAG_NOSCRIPT,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_SCRIPT: Command = Command {
    name: "SCRIPT",
    arity: -2,
    flags: FLAG_LOCAL | FLAG_NOSCRIPT,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_SHRINK: Command = Command {
    name: "SHRINK",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_shrink,
    merge: None,
};
pub static CMD_EVAL: Command = Command {
    name: "EVAL",
    arity: -3,
    flags: FLAG_NOSCRIPT,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_EVALSHA: Command = Command {
    name: "EVALSHA",
    arity: -3,
    flags: FLAG_NOSCRIPT,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_EVAL_RO: Command = Command {
    name: "EVAL_RO",
    arity: -3,
    flags: FLAG_READONLY | FLAG_NOSCRIPT,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_EVALSHA_RO: Command = Command {
    name: "EVALSHA_RO",
    arity: -3,
    flags: FLAG_READONLY | FLAG_NOSCRIPT,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_FCALL: Command = Command {
    name: "FCALL",
    arity: -3,
    flags: FLAG_NOSCRIPT,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_FCALL_RO: Command = Command {
    name: "FCALL_RO",
    arity: -3,
    flags: FLAG_READONLY | FLAG_NOSCRIPT,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};
pub static CMD_DFLY: Command = Command {
    name: "DFLY",
    arity: -2,
    flags: FLAG_ADMIN | FLAG_LOCAL,
    key_range: KeyRange::NONE,
    exec: local_stub,
    merge: None,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DbSlice;
    use crate::core::value::PrimeValue;

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
            conn_id: 0,
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
            RespValue::NilArray => "(nil)".into(),
            RespValue::Error(e) => e.clone(),
            RespValue::Array(a) => {
                format!("[{}]", a.iter().map(render).collect::<Vec<_>>().join(", "))
            }
            RespValue::Map(m) => format!("MAP{}", m.len()),
            RespValue::Bool(b) => b.to_string(),
            RespValue::Double(f) => crate::util::format_double(*f),
        }
    }

    /// Like [`render`] but recurses into maps (FUNCTION LIST/STATS replies).
    fn render_full(v: &RespValue) -> String {
        match v {
            RespValue::Map(pairs) => format!(
                "{{{}}}",
                pairs
                    .iter()
                    .map(|(k, val)| format!("{} => {}", render_full(k), render_full(val)))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            RespValue::Array(items) => format!(
                "[{}]",
                items.iter().map(render_full).collect::<Vec<_>>().join(", ")
            ),
            other => render(other),
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
        assert_eq!(
            render(&local_latency(&b_args(&["LATENCY", "LATEST"]))),
            "[]"
        );
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
        assert!(
            s(&mut d, &["DEBUG", "OBJECT", "k"])
                .starts_with("encoding:raw bucket_id:0 slot:0 shard:0")
        );
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
        assert_eq!(
            s(&mut d, &["SAVE", "RDB", "save_writes_snapshot.rdb"]),
            "OK"
        );

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
            assert_eq!(s(&mut d, argv), expect, "setup {key}");
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

    #[test]
    fn wait_reply() {
        // No connected replicas on this port: always `:0`, but arguments are
        // still validated as non-negative integers.
        assert_eq!(render(&local_wait(&b_args(&["WAIT", "0", "0"]))), "0");
        assert_eq!(render(&local_wait(&b_args(&["WAIT", "2", "1000"]))), "0");
        assert_eq!(
            render(&local_wait(&b_args(&["WAIT", "-1", "1000"]))),
            "ERR value is not an integer or out of range"
        );
        assert_eq!(
            render(&local_wait(&b_args(&["WAIT", "x", "1000"]))),
            "ERR value is not an integer or out of range"
        );
    }

    #[test]
    fn shutdown_grammar() {
        assert_eq!(render(&shutdown(&["SHUTDOWN"])), "OK");
        assert_eq!(render(&shutdown(&["SHUTDOWN", "NOSAVE"])), "OK");
        assert_eq!(render(&shutdown(&["SHUTDOWN", "SAVE"])), "OK");
        assert_eq!(
            render(&shutdown(&["SHUTDOWN", "SAFE", "FORCE", "NOW"])),
            "OK"
        );
        assert_eq!(
            render(&shutdown(&["SHUTDOWN", "SAVE", "NOSAVE"])),
            "ERR syntax error"
        );
        assert_eq!(
            render(&shutdown(&["SHUTDOWN", "ABORT"])),
            "ERR SHUTDOWN ABORT is not supported"
        );
        assert_eq!(
            render(&shutdown(&["SHUTDOWN", "BOGUS"])),
            "ERR syntax error"
        );
    }

    fn shutdown(a: &[&str]) -> RespValue {
        match local_shutdown(&b_args(a)) {
            Ok(()) => RespValue::Simple("OK".into()),
            Err(e) => e,
        }
    }

    #[test]
    fn replconf_options() {
        let c = |a: &[&str]| -> String {
            match local_replconf(&b_args(a)) {
                Some(v) => render(&v),
                None => "<silence>".into(),
            }
        };
        assert_eq!(c(&["REPLCONF", "LISTENING-PORT", "6379"]), "OK");
        assert_eq!(
            c(&["REPLCONF", "LISTENING-PORT", "abc"]),
            "ERR value is not an integer or out of range"
        );
        assert_eq!(c(&["REPLCONF", "CAPA", "eof", "CAPA", "psync2"]), "OK");
        assert_eq!(
            c(&["REPLCONF", "CLIENT-ID", "42", "CLIENT-VERSION", "1"]),
            "OK"
        );
        assert_eq!(c(&["REPLCONF", "ACK", "1"]), "<silence>");
        assert_eq!(c(&["REPLCONF", "ACK"]), "ERR syntax error");
        assert_eq!(
            c(&["REPLCONF", "IP-ADDRESS", "1.2.3.4", "CAPA", "eof"]),
            "OK"
        );
        assert_eq!(c(&["REPLCONF", "BOGUS", "x"]), "ERR syntax error");
        assert_eq!(
            c(&["REPLCONF", "LISTENING-PORT", "6379", "ACK", "1"]),
            "ERR syntax error"
        );
    }

    #[test]
    fn replicaof_reply() {
        let r = |a: &[&str]| match parse_replicaof(&b_args(a)) {
            Ok(ReplicaOf::NoOne) => "OK".to_string(),
            Ok(ReplicaOf::Start { host, port }) => format!("OK {host}:{port}"),
            Err(e) => render(&e),
        };
        assert_eq!(r(&["REPLICAOF", "NO", "ONE"]), "OK");
        assert_eq!(r(&["SLAVEOF", "NO", "ONE"]), "OK");
        assert_eq!(r(&["REPLICAOF", "localhost", "7000"]), "OK localhost:7000");
        assert_eq!(
            r(&["REPLICAOF", "localhost", "99999"]),
            "ERR port is out of range"
        );
        assert_eq!(
            r(&["REPLICAOF", "localhost", "-1"]),
            "ERR port is out of range"
        );
        assert_eq!(r(&["REPLICAOF", "NO", "TWO"]), "ERR syntax error");
        assert_eq!(r(&["REPLICAOF"]), "ERR syntax error");
    }

    #[test]
    fn addreplicaof_reply() {
        assert_eq!(
            render(&local_addreplicaof(&b_args(&[
                "ADDREPLICAOF",
                "h",
                "1",
                "2",
                "3"
            ]))),
            "ERR Calling ADDREPLICAOFF allowed only after server is already a replica"
        );
    }

    #[test]
    fn repltakeover_reply() {
        let r = |a: &[&str]| render(&local_repltakeover(&b_args(a)));
        assert_eq!(r(&["REPLTAKEOVER", "1"]), "OK");
        assert_eq!(r(&["REPLTAKEOVER", "1", "SAVE"]), "OK");
        assert_eq!(
            r(&["REPLTAKEOVER", "1", "NOSAVE"]),
            "ERR Unsupported option:NOSAVE"
        );
        assert_eq!(
            r(&["REPLTAKEOVER", "x"]),
            "ERR value is not an integer or out of range"
        );
        assert_eq!(r(&["REPLTAKEOVER", "-1"]), "ERR timeout is negative");
    }

    #[test]
    fn module_list() {
        let v = local_module(&b_args(&["MODULE", "LIST"]));
        match v {
            RespValue::Array(mods) if mods.len() == 2 => {
                let m0 = render(&mods[0]);
                assert!(m0.contains("ReJSON") && m0.contains("20808"));
                let m1 = render(&mods[1]);
                assert!(m1.contains("search") && m1.contains("21015"));
            }
            other => panic!("unexpected MODULE LIST: {other:?}"),
        }
        assert_eq!(
            render(&local_module(&b_args(&["MODULE", "LOAD", "foo"]))),
            "ERR syntax error"
        );
    }

    #[test]
    fn function_reply() {
        use crate::commands::lua::ScriptMgr;
        use crate::server::local_function;
        let mut mgr = ScriptMgr::new();
        let r = |m: &mut ScriptMgr, a: &[&str]| render(&local_function(m, &b_args(a)));
        let rf = |m: &mut ScriptMgr, a: &[&str]| render_full(&local_function(m, &b_args(a)));
        assert_eq!(r(&mut mgr, &["FUNCTION", "FLUSH"]), "OK");
        assert_eq!(
            r(&mut mgr, &["FUNCTION", "KILL"]),
            "NOTBUSY No scripts in execution right now."
        );
        assert!(r(&mut mgr, &["FUNCTION", "HELP"]).contains("FUNCTION <subcommand>"));
        assert_eq!(r(&mut mgr, &["FUNCTION", "LIST"]), "[]");
        assert_eq!(
            r(&mut mgr, &["FUNCTION", "BOGUS"]),
            "ERR Unknown subcommand or wrong number of arguments for 'BOGUS'. Try FUNCTION HELP."
        );

        let lib1 = "#!lua name=lib1\n\
            redis.register_function('f1', function(keys, args) return 1 end)";
        assert_eq!(r(&mut mgr, &["FUNCTION", "LOAD", lib1]), "lib1");
        assert_eq!(
            r(&mut mgr, &["FUNCTION", "LOAD", lib1]),
            "ERR Library 'lib1' already exists"
        );
        // A duplicate function name in another library is rejected.
        let lib2 = "#!lua name=lib2\n\
            redis.register_function('f1', function(keys, args) return 2 end)";
        assert_eq!(
            r(&mut mgr, &["FUNCTION", "LOAD", lib2]),
            "ERR Function 'f1' already exists"
        );
        // REPLACE redefines the same library without a duplicate-function error.
        assert_eq!(r(&mut mgr, &["FUNCTION", "LOAD", "REPLACE", lib1]), "lib1");
        // The name is only freed when its library is deleted.
        assert_eq!(r(&mut mgr, &["FUNCTION", "DELETE", "lib1"]), "OK");
        assert_eq!(r(&mut mgr, &["FUNCTION", "LOAD", lib2]), "lib2");
        assert!(rf(&mut mgr, &["FUNCTION", "LIST"]).contains("lib2"));

        // LIST filters by library name and carries flags + optional code.
        let lib3 = "#!lua name=lib3 flags=no-writes\n\
            redis.register_function{function_name='ro', callback=function() return 3 end}";
        assert_eq!(r(&mut mgr, &["FUNCTION", "LOAD", lib3]), "lib3");
        let list = rf(
            &mut mgr,
            &["FUNCTION", "LIST", "LIBRARYNAME", "lib3", "WITHCODE"],
        );
        assert!(
            list.contains("library_name") && list.contains("lib3"),
            "{list}"
        );
        assert!(list.contains("library_code"), "{list}");
        assert!(list.contains("no-writes"), "{list}");
        assert_eq!(
            r(&mut mgr, &["FUNCTION", "LIST", "LIBRARYNAME", "nope"]),
            "[]"
        );

        let stats = rf(&mut mgr, &["FUNCTION", "STATS"]);
        assert!(
            stats.contains("engines") && stats.contains("libraries_count"),
            "{stats}"
        );

        // Bad payloads.
        assert!(
            r(&mut mgr, &["FUNCTION", "LOAD", "return 1"])
                .starts_with("ERR Missing library metadata")
        );
        assert_eq!(
            r(&mut mgr, &["FUNCTION", "LOAD", "#!lua name=empty"]),
            "ERR No functions registered"
        );
        assert!(
            r(&mut mgr, &["FUNCTION", "LOAD", "#!js name=x\n"])
                .starts_with("ERR Invalid engine type")
        );
        assert!(
            r(
                &mut mgr,
                &["FUNCTION", "LOAD", "#!lua\nredis.register_function('x', 1)"]
            )
            .starts_with("ERR Missing library name")
        );

        // DUMP/RESTORE round-trips the registry into a fresh manager.
        let dump = match local_function(&mut mgr, &b_args(&["FUNCTION", "DUMP"])) {
            RespValue::Bulk(b) => b,
            other => panic!("unexpected DUMP reply: {other:?}"),
        };
        let mut mgr2 = ScriptMgr::new();
        let restore_args = vec![b"FUNCTION".to_vec(), b"RESTORE".to_vec(), dump.clone()];
        assert_eq!(render(&local_function(&mut mgr2, &restore_args)), "OK");
        assert_eq!(
            r(&mut mgr2, &["FUNCTION", "LIST"]),
            r(&mut mgr, &["FUNCTION", "LIST"])
        );
        assert_eq!(
            r(&mut mgr2, &["FUNCTION", "RESTORE", "bogus"]),
            "ERR Invalid function dump payload"
        );

        // DELETE (lib1 was already deleted above to free the 'f1' name).
        assert_eq!(r(&mut mgr, &["FUNCTION", "DELETE", "lib3"]), "OK");
        assert_eq!(
            r(&mut mgr, &["FUNCTION", "DELETE", "lib3"]),
            "ERR Library not found"
        );
        assert_eq!(r(&mut mgr, &["FUNCTION", "FLUSH"]), "OK");
        assert_eq!(r(&mut mgr, &["FUNCTION", "LIST"]), "[]");
    }

    #[test]
    fn script_reply() {
        use crate::commands::lua::ScriptMgr;
        use crate::server::local_script;
        let mut mgr = ScriptMgr::new();
        let r = |m: &mut ScriptMgr, a: &[&str]| render(&local_script(m, &b_args(a)));
        assert_eq!(r(&mut mgr, &["SCRIPT", "FLUSH"]), "OK");
        assert_eq!(r(&mut mgr, &["SCRIPT", "GC"]), "OK");
        assert_eq!(r(&mut mgr, &["SCRIPT", "EXISTS", "a", "b"]), "[0, 0]");
        assert_eq!(r(&mut mgr, &["SCRIPT", "LIST"]), "[]");
        let sha = "e0e1f9fabfc9d4800c877a703b823ac0578ff8db";
        assert_eq!(
            r(&mut mgr, &["SCRIPT", "FLAGS", sha, "allow-undeclared-keys"]),
            "OK"
        );
        // A sha with no flags applies nothing and replies OK (reference ConfigCmd).
        assert_eq!(r(&mut mgr, &["SCRIPT", "FLAGS", sha]), "OK");
        assert_eq!(
            r(&mut mgr, &["SCRIPT", "FLAGS"]),
            "ERR Unknown subcommand or wrong number of arguments for 'FLAGS'. Try SCRIPT HELP."
        );
        let sha40 = "0123456789012345678901234567890123456789";
        assert_eq!(r(&mut mgr, &["SCRIPT", "FLAGS", sha40]), "OK");
        // SCRIPT LIST includes flag-only entries (no body yet) like the
        // reference's `GetAll`, which iterates the superset map.
        let listed = r(&mut mgr, &["SCRIPT", "LIST"]);
        assert!(listed.contains(sha40), "{listed}");
        assert!(
            listed.contains(&format!("[{sha40}, ]")),
            "flag-only sha must render an empty body: {listed}"
        );
        assert_eq!(
            r(&mut mgr, &["SCRIPT", "FLAGS", "short", "x"]),
            "ERR syntax error"
        );
        assert_eq!(r(&mut mgr, &["SCRIPT", "LOAD", "return 1"]), sha);
        assert_eq!(r(&mut mgr, &["SCRIPT", "EXISTS", sha]), "[1]");
        // LATENCY is empty until scripts have actually run through the
        // coordinator (which records per-SHA usec stats).
        assert_eq!(r(&mut mgr, &["SCRIPT", "LATENCY"]), "[]");
        assert_eq!(
            r(&mut mgr, &["SCRIPT", "FLAGS", sha, "bogus"]),
            "ERR Invalid config format: Invalid flag: bogus"
        );
        assert!(r(&mut mgr, &["SCRIPT", "LOAD", "return {"]).starts_with("ERR syntax error"));
        assert!(r(&mut mgr, &["SCRIPT", "HELP"]).contains("SCRIPT <subcommand>"));
    }

    #[test]
    fn admin_commands_are_noscript() {
        use crate::commands::FLAG_NOSCRIPT;
        for name in ["MULTI", "EXEC", "DISCARD", "RESET", "SCRIPT", "FUNCTION"] {
            let cmd = crate::commands::lookup(name.as_bytes())
                .unwrap_or_else(|| panic!("{name} not in command table"));
            assert!(
                cmd.has_flag(FLAG_NOSCRIPT),
                "{name} must be flagged NOSCRIPT like the reference's CO::NOSCRIPT"
            );
        }
    }

    #[test]
    fn script_load_auto_async_rewrite() {
        use crate::commands::lua::ScriptMgr;
        use crate::server::local_script;
        let mut mgr = ScriptMgr::new();
        mgr.lua_auto_async = true;
        let r = |m: &mut ScriptMgr, a: &[&str]| render(&local_script(m, &b_args(a)));
        let bodies = |m: &ScriptMgr| {
            let mut map = std::collections::HashMap::new();
            for (sha, body) in m.get_all() {
                map.insert(sha, String::from_utf8(body).unwrap());
            }
            map
        };

        // Atomic script: standalone `redis.call` is rewritten at LOAD time.
        let body = "redis.call('set', KEYS[1], '1')";
        let sha = r(&mut mgr, &["SCRIPT", "LOAD", body]);
        assert_eq!(sha.len(), 40);
        assert_eq!(
            bodies(&mgr).get(&sha).unwrap(),
            "redis.acall('set', KEYS[1], '1')"
        );
        // A second LOAD of the same body is a no-op (sha already cached).
        assert_eq!(r(&mut mgr, &["SCRIPT", "LOAD", body]), sha);

        // A used return value is never rewritten.
        let used = "local v = redis.call('get', KEYS[1])\nreturn v";
        let sha_used = r(&mut mgr, &["SCRIPT", "LOAD", used]);
        assert_eq!(bodies(&mgr).get(&sha_used).unwrap(), used);

        // disable-atomicity suppresses the rewrite even with the flag on.
        let non_atomic = "--!df flags=disable-atomicity\nredis.call('set', KEYS[1], '1')";
        let sha_na = r(&mut mgr, &["SCRIPT", "LOAD", non_atomic]);
        assert_eq!(bodies(&mgr).get(&sha_na).unwrap(), non_atomic);

        // The flag defaults off: no rewrite.
        let mut mgr2 = ScriptMgr::new();
        assert_eq!(
            render(&local_script(&mut mgr2, &b_args(&["SCRIPT", "LOAD", body]))),
            sha
        );
        assert_eq!(bodies(&mgr2).get(&sha).unwrap(), body);
    }

    #[test]
    fn shrink_reply() {
        let mut d = db();
        assert_eq!(s(&mut d, &["SADD", "set", "a"]), "1");
        assert_eq!(s(&mut d, &["HSET", "h", "f", "1"]), "1");
        assert_eq!(s(&mut d, &["SET", "str", "v"]), "OK");
        // Set/hash: DenseSet encoding has no bucket array to compact here.
        assert_eq!(s(&mut d, &["SHRINK", "set"]), "0");
        assert_eq!(s(&mut d, &["SHRINK", "h"]), "0");
        assert_eq!(s(&mut d, &["SHRINK", "nosuch"]), "(nil)");
        assert_eq!(
            s(&mut d, &["SHRINK", "str"]),
            "WRONGTYPE Key is not a set or hash with DenseSet encoding"
        );
    }
}
