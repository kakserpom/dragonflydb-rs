pub mod coordinator;
pub mod event_loop;
pub mod pubsub;
pub mod shard;

/// Number of logical databases (matches upstream `FLAGS_dbnum` default).
pub const MAX_DB: usize = 16;

use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::mpsc;

use crate::commands::lua::{ScriptMgr, compile_check, sha1_hex};
use crate::commands::{Command, lookup};
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
        b"LATENCY" => RespValue::Array(vec![]),
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
            let params = match ScriptMgr::deduce_and_override(body) {
                Ok(p) => p,
                Err(e) => return RespValue::Error(format!("ERR {e}")),
            };
            if !mgr.exists(&sha) {
                mgr.store(sha.clone(), body.clone(), params);
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

/// Shared handles owned by the IO thread.
pub struct ServerEnv {
    pub num_shards: usize,
    pub shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    pub coord_tx: mpsc::Sender<CoordMsg>,
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
