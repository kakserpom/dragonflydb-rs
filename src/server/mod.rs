pub mod coordinator;
pub mod event_loop;
pub mod pubsub;
pub mod shard;

/// Number of logical databases (matches upstream `FLAGS_dbnum` default).
pub const MAX_DB: usize = 16;

use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::mpsc;

use crate::commands::exec::server::now_ms;
use crate::commands::lua::{
    FunctionLib, SandboxedInterpreter, ScriptMgr, compile_check, parse_function_header, sha1_hex,
};
use crate::commands::{Command, lookup};
use crate::core::histogram::Histogram;
use crate::error::{CmdResult, ReplyBytes, RespValue};
use crate::protocol::resp::encode_reply;
use crate::util::shard_for_key;

/// SCRIPT subcommands against a shared script cache (`ScriptMgr::Run`). LOAD
/// compiles the body in a throwaway Lua state so a compile error never enters
/// the cache; the coordinator owns the long-lived interpreter used by EVAL.
pub fn local_script(mgr: &mut ScriptMgr, args: &[Vec<u8>]) -> RespValue {
    let sub = args
        .get(1)
        .map(|a| a.to_ascii_uppercase())
        .unwrap_or_default();
    match sub.as_slice() {
        b"HELP" => RespValue::Array(vec![
            RespValue::Simple("SCRIPT <subcommand> [<arg> [value] [opt] ...]".into()),
            RespValue::Simple("Subcommands are:".into()),
            RespValue::Simple("EXISTS <sha1> [<sha1> ...]".into()),
            RespValue::Simple(
                "   Return information about the existence of the scripts in the script cache."
                    .into(),
            ),
            RespValue::Simple("FLUSH".into()),
            RespValue::Simple("   Flush the Lua scripts cache. Very dangerous on replicas.".into()),
            RespValue::Simple("LOAD <script>".into()),
            RespValue::Simple(
                "   Load a script into the scripts cache without executing it.".into(),
            ),
            RespValue::Simple("FLAGS <sha> [flags ...]".into()),
            RespValue::Simple(
                "   Set specific flags for script. Can be called before the sript is loaded."
                    .into(),
            ),
            RespValue::Simple("   The following flags are possible: ".into()),
            RespValue::Simple(
                "      - Use 'allow-undeclared-keys' to allow accessing undeclared keys".into(),
            ),
            RespValue::Simple(
                "      - Use 'disable-atomicity' to allow running scripts non-atomically".into(),
            ),
            RespValue::Simple("      - Use 'legacy-float' to return floats as integers".into()),
            RespValue::Simple("LIST".into()),
            RespValue::Simple("   Lists loaded scripts.".into()),
            RespValue::Simple("LATENCY".into()),
            RespValue::Simple(
                "   Prints latency histograms in usec for every called function.".into(),
            ),
            RespValue::Simple("GC".into()),
            RespValue::Simple(
                "   Invokes garbage collection on all unused interpreter instances.".into(),
            ),
            RespValue::Simple("HELP".into()),
            RespValue::Simple("   Prints this help.".into()),
        ]),
        b"EXISTS" if args.len() >= 3 => RespValue::Array(
            args[2..]
                .iter()
                .map(|sha| RespValue::Integer(i64::from(mgr.exists(&String::from_utf8_lossy(sha)))))
                .collect(),
        ),
        b"FLUSH" => {
            mgr.flush();
            RespValue::Simple("OK".into())
        }
        b"LIST" => RespValue::Array(
            mgr.get_all()
                .into_iter()
                .map(|(sha, body)| {
                    RespValue::Array(vec![
                        RespValue::Bulk(sha.into_bytes()),
                        RespValue::Bulk(body),
                    ])
                })
                .collect(),
        ),
        b"LATENCY" => {
            // Like `ScriptMgr::LatencyCmd`: one `[sha, histogram dump]` pair per
            // SHA. The reference merges per-shard histograms before dumping;
            // the coordinator records a single histogram per SHA, so its dump
            // is the merge result. The dump is sent as a bulk string, which is
            // exactly `SendVerbatimString`'s RESP2 encoding.
            let mut entries: Vec<(&String, &Histogram)> = mgr.latency().iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            RespValue::Array(
                entries
                    .into_iter()
                    .map(|(sha, hist)| {
                        RespValue::Array(vec![
                            RespValue::Bulk(sha.clone().into_bytes()),
                            RespValue::Bulk(hist.to_string().into_bytes()),
                        ])
                    })
                    .collect(),
            )
        }
        b"LOAD" if args.len() == 3 => {
            let body = &args[2];
            if body.is_empty() {
                // `LoadCmd` returns the empty-body SHA without caching it.
                return RespValue::Bulk(sha1_hex(b"").into_bytes());
            }
            if let Err(e) = compile_check(body) {
                return RespValue::Error(format!("ERR {e}"));
            }
            let sha = sha1_hex(body);
            let params = match mgr.deduce_and_override(body) {
                Ok(p) => p,
                Err(e) => return RespValue::Error(format!("ERR {e}")),
            };
            if !mgr.exists(&sha) {
                // The `lua_auto_async` rewrite applies at load time (`Insert`).
                let body = mgr.auto_async_body(body, &params);
                mgr.store(sha.clone(), body, params);
            }
            RespValue::Bulk(sha.into_bytes())
        }
        b"FLAGS" if args.len() >= 3 => {
            let sha = &args[2];
            if sha.len() != 40 {
                return RespValue::Error("ERR syntax error".into());
            }
            let flags: Vec<String> = args[3..]
                .iter()
                .map(|f| String::from_utf8_lossy(f).into_owned())
                .collect();
            match mgr.apply_flags(&String::from_utf8_lossy(sha), &flags) {
                Ok(()) => RespValue::Simple("OK".into()),
                Err(e) => RespValue::Error(format!("ERR {e}")),
            }
        }
        b"GC" => RespValue::Simple("OK".into()),
        other => RespValue::Error(format!(
            "ERR Unknown subcommand or wrong number of arguments for '{}'. Try SCRIPT HELP.",
            String::from_utf8_lossy(other)
        )),
    }
}

fn unknown_function_subcmd(sub: &[u8]) -> String {
    format!(
        "ERR Unknown subcommand or wrong number of arguments for '{}'. Try FUNCTION HELP.",
        String::from_utf8_lossy(sub)
    )
}

/// FUNCTION subcommands against the shared library registry. `LOAD` validates
/// the payload in a throwaway Lua state (collecting the `redis.register_function`
/// calls); the coordinator recreates the callbacks in its own interpreter the
/// first time a library's function runs (`FCALL`).
pub fn local_function(mgr: &mut ScriptMgr, args: &[Vec<u8>]) -> RespValue {
    let sub = args
        .get(1)
        .map(|a| a.to_ascii_uppercase())
        .unwrap_or_default();
    match sub.as_slice() {
        b"HELP" => RespValue::Array(vec![
            RespValue::Simple("FUNCTION <subcommand> [<arg> [value] [opt] ...]".into()),
            RespValue::Simple("Subcommands are:".into()),
            RespValue::Simple("LOAD [REPLACE] <code>".into()),
            RespValue::Simple("   Load a new library to the server.".into()),
            RespValue::Simple("DELETE <library-name>".into()),
            RespValue::Simple("   Delete the given library.".into()),
            RespValue::Simple("FLUSH [ASYNC|SYNC]".into()),
            RespValue::Simple("   Delete all the libraries.".into()),
            RespValue::Simple("LIST [LIBRARYNAME <library-name>] [WITHCODE]".into()),
            RespValue::Simple("   Return information about the functions.".into()),
            RespValue::Simple("STATS".into()),
            RespValue::Simple("   Return information about the current function execution.".into()),
            RespValue::Simple("DUMP".into()),
            RespValue::Simple("   Return the payload of all functions.".into()),
            RespValue::Simple("RESTORE <payload> [FLUSH|APPEND|REPLACE]".into()),
            RespValue::Simple("   Restore the functions from the payload.".into()),
            RespValue::Simple("KILL".into()),
            RespValue::Simple("   Kill the currently executing function.".into()),
            RespValue::Simple("HELP".into()),
            RespValue::Simple("   Prints this help.".into()),
        ]),
        b"LOAD" => {
            let (replace, code_idx) = if args
                .get(2)
                .is_some_and(|a| a.eq_ignore_ascii_case(b"REPLACE"))
            {
                (true, 3)
            } else {
                (false, 2)
            };
            let Some(code) = args.get(code_idx) else {
                return RespValue::Error(unknown_function_subcmd(&sub));
            };
            match load_library(mgr, code, replace) {
                Ok(name) => RespValue::Bulk(name.into_bytes()),
                Err(e) => RespValue::Error(format!("ERR {e}")),
            }
        }
        b"DELETE" => {
            let Some(name) = args.get(2).map(|a| String::from_utf8_lossy(a).into_owned()) else {
                return RespValue::Error(unknown_function_subcmd(&sub));
            };
            if mgr.delete_library(&name) {
                RespValue::Simple("OK".into())
            } else {
                RespValue::Error("ERR Library not found".into())
            }
        }
        b"FLUSH" => {
            if args.len() > 3 {
                return RespValue::Error(unknown_function_subcmd(&sub));
            }
            mgr.flush_libraries();
            RespValue::Simple("OK".into())
        }
        b"LIST" => function_list(mgr, args),
        b"STATS" => function_stats(mgr),
        b"DUMP" => RespValue::Bulk(mgr.dump_libraries()),
        b"RESTORE" => function_restore(mgr, args),
        b"KILL" => {
            // Functions run synchronously on the coordinator; KILL sets a flag
            // the `LUA_MASKCOUNT` instruction hook polls, so even a tight loop
            // that never calls out is interrupted.
            if mgr.running().is_none() {
                RespValue::Error("NOTBUSY No scripts in execution right now.".into())
            } else {
                mgr.request_kill();
                RespValue::Simple("OK".into())
            }
        }
        other => RespValue::Error(unknown_function_subcmd(other)),
    }
}

/// Validate `code` (header, compile and `redis.register_function` calls) and
/// insert it into the registry, enforcing the Redis uniqueness rules: the
/// library name and every function name must be free unless `replace` allows
/// redefining the same library. Returns the library name on success.
fn load_library(mgr: &mut ScriptMgr, code: &[u8], replace: bool) -> Result<String, String> {
    let header = parse_function_header(code)?;
    if !replace && mgr.library(&header.name).is_some() {
        return Err(format!("Library '{}' already exists", header.name));
    }
    let interp = SandboxedInterpreter::new()?;
    let functions = interp.load_function_lib(code)?;
    if functions.is_empty() {
        return Err("No functions registered".into());
    }
    let lib = FunctionLib {
        name: header.name.clone(),
        engine: "LUA".into(),
        sha: sha1_hex(code),
        code: code.to_vec(),
        header_flags: header.header_flags,
        functions,
    };
    for f in &lib.functions {
        if let Some(other) = mgr.function_lib(&f.name)
            && other.name != lib.name
        {
            return Err(format!("Function '{}' already exists", f.name));
        }
    }
    let name = lib.name.clone();
    mgr.store_library(lib);
    Ok(name)
}

fn function_list(mgr: &ScriptMgr, args: &[Vec<u8>]) -> RespValue {
    let mut filter: Option<String> = None;
    let mut with_code = false;
    let mut i = 2;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"LIBRARYNAME" => {
                let Some(name) = args.get(i + 1) else {
                    return RespValue::Error(unknown_function_subcmd(b"LIST"));
                };
                filter = Some(String::from_utf8_lossy(name).into_owned());
                i += 2;
            }
            b"WITHCODE" => {
                with_code = true;
                i += 1;
            }
            _ => return RespValue::Error(unknown_function_subcmd(b"LIST")),
        }
    }
    let mut libs: Vec<&FunctionLib> = mgr.libraries().into_iter().map(|(_, l)| l).collect();
    if let Some(f) = filter {
        libs.retain(|l| l.name == f);
    }
    libs.sort_by(|a, b| a.name.cmp(&b.name));
    RespValue::Array(
        libs.into_iter()
            .map(|lib| {
                let mut pairs = vec![
                    (
                        RespValue::Bulk(b"library_name".to_vec()),
                        RespValue::Bulk(lib.name.clone().into_bytes()),
                    ),
                    (
                        RespValue::Bulk(b"engine".to_vec()),
                        RespValue::Bulk(lib.engine.clone().into_bytes()),
                    ),
                    (
                        RespValue::Bulk(b"functions".to_vec()),
                        RespValue::Array(
                            lib.functions
                                .iter()
                                .map(|f| {
                                    RespValue::Map(vec![
                                        (
                                            RespValue::Bulk(b"name".to_vec()),
                                            RespValue::Bulk(f.name.clone().into_bytes()),
                                        ),
                                        (
                                            RespValue::Bulk(b"description".to_vec()),
                                            RespValue::Bulk(Vec::new()),
                                        ),
                                        (
                                            RespValue::Bulk(b"flags".to_vec()),
                                            RespValue::Array(
                                                f.flags
                                                    .iter()
                                                    .map(|s| {
                                                        RespValue::Bulk(s.clone().into_bytes())
                                                    })
                                                    .collect(),
                                            ),
                                        ),
                                    ])
                                })
                                .collect(),
                        ),
                    ),
                ];
                if with_code {
                    pairs.push((
                        RespValue::Bulk(b"library_code".to_vec()),
                        RespValue::Bulk(lib.code.clone()),
                    ));
                }
                RespValue::Map(pairs)
            })
            .collect(),
    )
}

fn function_stats(mgr: &ScriptMgr) -> RespValue {
    let running = match mgr.running() {
        Some(r) => RespValue::Map(vec![
            (
                RespValue::Bulk(b"name".to_vec()),
                RespValue::Bulk(r.name.clone().into_bytes()),
            ),
            (
                RespValue::Bulk(b"command".to_vec()),
                RespValue::Bulk(r.command.clone().into_bytes()),
            ),
            (
                RespValue::Bulk(b"duration_ms".to_vec()),
                RespValue::Integer(now_ms().saturating_sub(r.started_ms) as i64),
            ),
        ]),
        None => RespValue::Nil,
    };
    let libs = mgr.libraries();
    let functions_count: usize = libs.iter().map(|(_, l)| l.functions.len()).sum();
    RespValue::Map(vec![
        (RespValue::Bulk(b"running_script".to_vec()), running),
        (
            RespValue::Bulk(b"engines".to_vec()),
            RespValue::Map(vec![(
                RespValue::Bulk(b"LUA".to_vec()),
                RespValue::Map(vec![
                    (
                        RespValue::Bulk(b"libraries_count".to_vec()),
                        RespValue::Integer(libs.len() as i64),
                    ),
                    (
                        RespValue::Bulk(b"functions_count".to_vec()),
                        RespValue::Integer(functions_count as i64),
                    ),
                ]),
            )]),
        ),
    ])
}

fn function_restore(mgr: &mut ScriptMgr, args: &[Vec<u8>]) -> RespValue {
    let Some(payload) = args.get(2) else {
        return RespValue::Error(unknown_function_subcmd(b"RESTORE"));
    };
    let mut flush = false;
    let mut replace = false;
    if let Some(p) = args.get(3) {
        match p.to_ascii_uppercase().as_slice() {
            b"FLUSH" => flush = true,
            b"REPLACE" => replace = true,
            b"APPEND" => {}
            _ => return RespValue::Error(unknown_function_subcmd(b"RESTORE")),
        }
    }
    let libs = match ScriptMgr::restore_libraries(payload) {
        Ok(l) => l,
        Err(e) => return RespValue::Error(format!("ERR {e}")),
    };
    if flush {
        mgr.flush_libraries();
    }
    for lib in libs {
        if !replace && mgr.library(&lib.name).is_some() {
            return RespValue::Error(format!("ERR Library '{}' already exists", lib.name));
        }
        if let Err(e) = load_library(mgr, &lib.code, replace) {
            return RespValue::Error(format!("ERR {e}"));
        }
    }
    RespValue::Simple("OK".into())
}

/// A reply routed back to a specific connection. `seq` preserves request order.
#[derive(Debug)]
pub struct Reply {
    pub conn_id: u64,
    pub seq: u64,
    pub bytes: ReplyBytes,
}

/// The watched-state snapshot of a single key, backing WATCH/EXEC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchState {
    /// Key modification version at snapshot time.
    pub version: u64,
    /// Whether the key existed at snapshot time (after lazy expiry).
    pub existed: bool,
    /// DB epoch at snapshot time; a FLUSHDB bump dirties every WATCH.
    pub db_epoch: u64,
}

/// A reply bus: a channel plus a kqueue wakeup pipe. Every reply sent through
/// it pokes the IO thread so its event loop wakes without polling.
#[derive(Clone)]
pub struct ReplyBus {
    tx: Arc<mpsc::Sender<Reply>>,
    wake_w: RawFd,
}

impl ReplyBus {
    #[must_use]
    pub fn new(tx: mpsc::Sender<Reply>, wake_w: RawFd) -> Self {
        ReplyBus {
            tx: Arc::new(tx),
            wake_w,
        }
    }

    pub fn send(&self, reply: Reply) {
        if self.tx.send(reply).is_err() {
            return;
        }
        let one = [1u8];
        unsafe {
            libc::write(self.wake_w, one.as_ptr().cast::<libc::c_void>(), 1);
        }
    }
}

impl std::fmt::Debug for ReplyBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplyBus")
            .field("tx", &self.tx)
            .field("wake_w", &self.wake_w)
            .finish()
    }
}

/// A single-shard or fast-path command.
#[derive(Debug)]
pub struct SingleOp {
    pub conn_id: u64,
    pub seq: u64,
    pub args: Vec<Vec<u8>>,
    pub owned_key_idxs: Vec<usize>,
    /// The connection's selected DB index.
    pub db_idx: usize,
    pub reply: ReplyBus,
}

/// Messages to a shard thread.
#[derive(Debug)]
pub enum ShardMsg {
    Single(SingleOp),
    TxLock {
        tx_id: u64,
        conn_id: u64,
        seq: u64,
        args: Vec<Vec<u8>>,
        owned_key_idxs: Vec<usize>,
        first_key_idx: usize,
        db_idx: usize,
        ack: mpsc::Sender<()>,
    },
    TxExec {
        tx_id: u64,
        result_tx: mpsc::Sender<crate::commands::ShardPart>,
    },
    TxUnlock {
        tx_id: u64,
    },
    /// A raw store/delete performed by the coordinator on behalf of a
    /// multi-shard command (e.g. BITOP's destination key). Locks the shard like
    /// a transaction and acks when the write completed.
    StoreValue {
        tx_id: u64,
        key: Vec<u8>,
        /// `None` deletes the key.
        value: Option<crate::core::PrimeValue>,
        /// Absolute expiry in ms to set on the stored key, if any.
        expire_at: Option<u64>,
        /// Applies/clears the STICK flag on the stored key.
        sticky: bool,
        /// The DB index to write into.
        db_idx: usize,
        ack: mpsc::Sender<()>,
    },
    /// Snapshot the (version, existed, `db_epoch`) of each key, in order. Queued
    /// behind an active transaction like a single op. Backs WATCH.
    WatchQuery {
        keys: Vec<Vec<u8>>,
        db_idx: usize,
        result_tx: mpsc::Sender<Vec<(Vec<u8>, WatchState)>>,
    },
    /// A single `redis.call(...)` dispatched from a script running on the
    /// coordinator. The target shard is already locked by the script's
    /// transaction, so the subcommand executes immediately and its result is
    /// sent back on `result_tx`.
    ScriptOp {
        args: Vec<Vec<u8>>,
        owned_key_idxs: Vec<usize>,
        /// The subcommand's `KeyRange::first` for the `OpContext`.
        first_key_idx: usize,
        db_idx: usize,
        result_tx: mpsc::Sender<crate::error::CmdResult>,
    },
    /// A squashed batch of `redis.acall`/`redis.apcall` subcommands that all
    /// target this shard, dispatched as one hop (`MultiCommandSquasher`). The
    /// shard is already locked by the script's transaction, so every entry runs
    /// inline in order and the results are sent back together.
    ScriptBatch {
        cmds: Vec<ScriptBatchEntry>,
        result_tx: mpsc::Sender<Vec<crate::error::CmdResult>>,
    },
}

/// One entry of a squashed script batch (a `MultiCommandSquasher` hop). Each
/// entry is a subcommand that runs on the shard receiving the batch.
#[derive(Debug, Clone)]
pub struct ScriptBatchEntry {
    pub args: Vec<Vec<u8>>,
    pub owned_key_idxs: Vec<usize>,
    /// The subcommand's `KeyRange::first` for the `OpContext`.
    pub first_key_idx: usize,
    pub db_idx: usize,
}

/// Messages to the transaction coordinator.
#[derive(Debug)]
pub struct CoordMsg {
    pub conn_id: u64,
    pub seq: u64,
    pub args: Vec<Vec<u8>>,
    /// Key arg indices for the command (empty for global commands).
    pub keys: Vec<usize>,
    /// Involved shards (all shards for global commands).
    pub shards: Vec<usize>,
    pub first_key_idx: usize,
    /// The connection's selected DB index.
    pub db_idx: usize,
    /// True when the command runs inside a MULTI block: a blocking command must
    /// not wait, so the coordinator replies nil instead of re-queueing it.
    pub no_block: bool,
}

/// `SCRIPT GC`: a request for the coordinator to run a full Lua GC over its
/// interpreter (`ScriptMgr::GCCmd`). The coordinator ackowledges on `ack` once
/// the collection finished, so the IO thread can reply `+OK`.
#[derive(Debug)]
pub struct GcRequest {
    pub ack: mpsc::Sender<()>,
}

/// Shared handles owned by the IO thread.
pub struct ServerEnv {
    pub num_shards: usize,
    pub shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    pub coord_tx: mpsc::Sender<CoordMsg>,
    pub gc_tx: mpsc::Sender<GcRequest>,
    pub reply_bus_tx: ReplyBus,
    /// Shared script cache: SCRIPT subcommands (IO thread) and EVAL
    /// (coordinator) both read/write it.
    pub script_mgr: std::sync::Arc<std::sync::Mutex<crate::commands::lua::ScriptMgr>>,
}

impl ServerEnv {
    #[must_use]
    pub fn shard_for_key(&self, key: &[u8]) -> usize {
        shard_for_key(key, self.num_shards)
    }
}

/// Key indices for a command. Handles movable keys (XREAD/XREADGROUP,
/// SORT's runtime STORE destination) and numkeys-prefixed keys (LMPOP)
/// by scanning the argument list.
#[must_use]
pub fn extract_keys(cmd: &'static Command, args: &[Vec<u8>]) -> Vec<usize> {
    if cmd.name == "CMS.MERGE" {
        // `CMS.MERGE <dest> <numkeys> <key>... [WEIGHTS w...]`: the
        // destination (args[1]) plus the numkeys-prefixed sources.
        let mut keys = extract_numkeys_keys(args, 2);
        keys.insert(0, 1);
        keys
    } else if cmd.name == "LMPOP" || cmd.name == "BLMPOP" {
        // `LMPOP <numkeys> <key>...` / `BLMPOP <timeout> <numkeys> <key>...`
        let numkeys_idx = if cmd.name == "LMPOP" { 1 } else { 2 };
        extract_numkeys_keys(args, numkeys_idx)
    } else if matches!(
        cmd.name,
        "ZUNION"
            | "ZINTER"
            | "ZDIFF"
            | "ZINTERCARD"
            | "ZMPOP"
            | "BZMPOP"
            | "ZUNIONSTORE"
            | "ZINTERSTORE"
            | "ZDIFFSTORE"
    ) {
        // `ZUNION <numkeys> <key>...` / `ZUNIONSTORE <dest> <numkeys> <key>...` /
        // `BZMPOP <timeout> <numkeys> <key>...`. The store variants add the
        // destination key as a leading bonus key (mirrors the `STORE` bonus in
        // `transaction.cc DetermineKeys`).
        let numkeys_idx = if cmd.name.ends_with("STORE") || cmd.name == "BZMPOP" {
            2
        } else {
            1
        };
        let mut keys = extract_numkeys_keys(args, numkeys_idx);
        if cmd.name.ends_with("STORE") && !keys.is_empty() {
            keys.insert(0, 1);
        }
        keys
    } else if cmd.flags & crate::commands::FLAG_MOVABLEKEYS != 0 {
        if cmd.name == "SORT" || cmd.name == "SORT_RO" {
            extract_sort_keys(args)
        } else if cmd.name == "GEORADIUS" || cmd.name == "GEORADIUSBYMEMBER" {
            extract_geo_radius_keys(args)
        } else {
            extract_movable_keys(args)
        }
    } else {
        cmd.key_range.keys(args.len())
    }
}

/// Key indices for `SORT/SORT_RO`: the source key plus the STORE destination
/// when present (mirrors `CO::STORE_LAST_KEY`). Options are skipped so a GET
/// pattern argument is never mistaken for a STORE key.
#[must_use]
pub fn extract_sort_keys(args: &[Vec<u8>]) -> Vec<usize> {
    let mut keys = vec![1];
    let mut i = 2;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"LIMIT" => i += 3,
            b"STORE" => {
                if i + 1 < args.len() {
                    keys.push(i + 1);
                }
                i += 2;
            }
            b"BY" | b"GET" => i += 2,
            _ => i += 1, // ALPHA / ASC / DESC and anything malformed (exec errors)
        }
    }
    keys
}

/// Key indices for GEORADIUS / GEORADIUSBYMEMBER: the source key plus the
/// STORE/STOREDIST destination as the last argument (mirrors `STORE_LAST_KEY`
/// in `transaction.cc`: the penultimate arg must be STORE/STOREDIST).
#[must_use]
pub fn extract_geo_radius_keys(args: &[Vec<u8>]) -> Vec<usize> {
    let mut keys = vec![1];
    if args.len() >= 3 {
        let opt = &args[args.len() - 2];
        if opt.eq_ignore_ascii_case(b"STORE") || opt.eq_ignore_ascii_case(b"STOREDIST") {
            keys.push(args.len() - 1);
        }
    }
    keys
}

#[must_use]
pub fn extract_movable_keys(args: &[Vec<u8>]) -> Vec<usize> {
    for i in 1..args.len() {
        if args[i].eq_ignore_ascii_case(b"STREAMS") {
            let remaining = args.len() - i - 1;
            if remaining == 0 || !remaining.is_multiple_of(2) {
                return vec![];
            }
            let n = remaining / 2;
            return (i + 1..i + 1 + n).collect();
        }
    }
    vec![]
}

/// Key indices for numkeys-prefixed commands (LMPOP/BLMPOP): the `numkeys`
/// argument at `numkeys_idx` names how many of the following args are keys.
/// Malformed counts yield an empty range so the executor reports the error.
#[must_use]
pub fn extract_numkeys_keys(args: &[Vec<u8>], numkeys_idx: usize) -> Vec<usize> {
    let Some(n) = args
        .get(numkeys_idx)
        .and_then(|a| crate::util::parse_i64(a))
    else {
        return vec![];
    };
    if n < 1 {
        return vec![];
    }
    let n = n as usize;
    let start = numkeys_idx + 1;
    (start..start + n.min(args.len().saturating_sub(start))).collect()
}

/// Blocking timeout in milliseconds for a command that returned `Blocked`,
/// or `None` when it has no waitable timeout (immediate retry). A `Some(0)`
/// means "wait forever". The reference parses the timeout as float seconds
/// (already validated by the executor) and scales it by 1000, with `u32::MAX`
/// the maximum millisecond counter.
pub fn blocking_timeout_ms(cmd: &Command, args: &[Vec<u8>]) -> Option<u64> {
    match cmd.name {
        "XREAD" | "XREADGROUP" => parse_block_ms(args),
        "BLPOP" | "BRPOP" => args
            .last()
            .and_then(|a| crate::util::parse_list_timeout(a).ok())
            .map(secs_to_ms),
        "BRPOPLPUSH" => args
            .get(3)
            .and_then(|a| crate::util::parse_list_timeout(a).ok())
            .map(secs_to_ms),
        "BLMOVE" => args
            .get(5)
            .and_then(|a| crate::util::parse_list_timeout(a).ok())
            .map(secs_to_ms),
        "BLMPOP" => args
            .get(1)
            .and_then(|a| crate::util::parse_list_timeout(a).ok())
            .map(secs_to_ms),
        "BZMPOP" => args
            .get(1)
            .and_then(|a| crate::util::parse_list_timeout(a).ok())
            .map(secs_to_ms),
        "BZPOPMIN" | "BZPOPMAX" => args
            .last()
            .and_then(|a| crate::util::parse_list_timeout(a).ok())
            .map(secs_to_ms),
        _ => None,
    }
}

fn secs_to_ms(secs: f64) -> u64 {
    ((secs * 1000.0) as u64).min(u64::from(u32::MAX))
}

/// Group key indices by shard.
#[must_use]
pub fn keys_per_shard(
    args: &[Vec<u8>],
    keys: &[usize],
    num_shards: usize,
) -> Vec<(usize, Vec<usize>)> {
    let mut map: std::collections::BTreeMap<usize, Vec<usize>> = std::collections::BTreeMap::new();
    for &ki in keys {
        let s = shard_for_key(&args[ki], num_shards);
        map.entry(s).or_default().push(ki);
    }
    map.into_iter().collect()
}

/// The command for a request.
#[must_use]
pub fn command_for(args: &[Vec<u8>]) -> Option<&'static Command> {
    lookup(args.first()?)
}

/// True for the EVAL family. These run on the coordinator (they own the Lua
/// interpreter), so they never touch a shard's `run_exec`.
#[must_use]
pub fn is_eval_cmd(name: &str) -> bool {
    matches!(name, "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO")
}

/// True for the FCALL family, which also runs on the coordinator (the function
/// callbacks live in its Lua interpreter).
#[must_use]
pub fn is_function_cmd(name: &str) -> bool {
    matches!(name, "FCALL" | "FCALL_RO")
}

/// Parse the BLOCK timeout in ms from XREAD/XREADGROUP args.
#[must_use]
pub fn parse_block_ms(args: &[Vec<u8>]) -> Option<u64> {
    for i in 1..args.len() {
        if args[i].eq_ignore_ascii_case(b"BLOCK") {
            return args
                .get(i + 1)
                .and_then(|a| crate::util::parse_i64(a))
                .map(|v| v.max(0) as u64);
        }
    }
    None
}

/// Encode a `RespValue` to RESP wire bytes.
#[must_use]
pub fn encode_value(v: &RespValue) -> Vec<u8> {
    let mut out = Vec::new();
    encode_reply(v, &mut out);
    out
}

/// Encode a command result to RESP wire bytes.
#[must_use]
pub fn encode_result(r: CmdResult) -> Vec<u8> {
    encode_value(&r.into_resp_value())
}
