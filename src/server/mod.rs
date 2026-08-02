pub mod coordinator;
pub mod event_loop;
pub mod shard;

/// Number of logical databases (matches upstream `FLAGS_dbnum` default).
pub const MAX_DB: usize = 16;

use std::os::fd::RawFd;
use std::sync::mpsc;
use std::sync::Arc;

use crate::commands::{lookup, Command};
use crate::error::{CmdResult, ReplyBytes, RespValue};
use crate::protocol::resp::encode_reply;
use crate::util::shard_for_key;

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
    pub fn new(tx: mpsc::Sender<Reply>, wake_w: RawFd) -> Self {
        ReplyBus { tx: Arc::new(tx), wake_w }
    }

    pub fn send(&self, reply: Reply) {
        if self.tx.send(reply).is_err() {
            return;
        }
        let one = [1u8];
        unsafe {
            libc::write(self.wake_w, one.as_ptr() as *const libc::c_void, 1);
        }
    }
}

impl std::fmt::Debug for ReplyBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplyBus").field("wake_w", &self.wake_w).finish()
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
    /// Snapshot the (version, existed, db_epoch) of each key, in order. Queued
    /// behind an active transaction like a single op. Backs WATCH.
    WatchQuery {
        keys: Vec<Vec<u8>>,
        db_idx: usize,
        result_tx: mpsc::Sender<Vec<(Vec<u8>, WatchState)>>,
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
}

/// Shared handles owned by the IO thread.
pub struct ServerEnv {
    pub num_shards: usize,
    pub shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    pub coord_tx: mpsc::Sender<CoordMsg>,
    pub reply_bus_tx: ReplyBus,
}

impl ServerEnv {
    pub fn shard_for_key(&self, key: &[u8]) -> usize {
        shard_for_key(key, self.num_shards)
    }

    /// Key indices for a command. Handles movable keys (XREAD/XREADGROUP,
    /// SORT's runtime STORE destination) by scanning the argument list.
    pub fn extract_keys(&self, cmd: &'static Command, args: &[Vec<u8>]) -> Vec<usize> {
        if cmd.flags & crate::commands::FLAG_MOVABLEKEYS != 0 {
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
}

/// Key indices for SORT/SORT_RO: the source key plus the STORE destination
/// when present (mirrors `CO::STORE_LAST_KEY`). Options are skipped so a GET
/// pattern argument is never mistaken for a STORE key.
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

/// Group key indices by shard.
pub fn keys_per_shard(args: &[Vec<u8>], keys: &[usize], num_shards: usize) -> Vec<(usize, Vec<usize>)> {
    let mut map: std::collections::BTreeMap<usize, Vec<usize>> = std::collections::BTreeMap::new();
    for &ki in keys {
        let s = shard_for_key(&args[ki], num_shards);
        map.entry(s).or_default().push(ki);
    }
    map.into_iter().collect()
}

/// The command for a request.
pub fn command_for(args: &[Vec<u8>]) -> Option<&'static Command> {
    lookup(args.first()?)
}

/// Parse the BLOCK timeout in ms from XREAD/XREADGROUP args.
pub fn parse_block_ms(args: &[Vec<u8>]) -> Option<u64> {
    for i in 1..args.len() {
        if args[i].eq_ignore_ascii_case(b"BLOCK") {
            return args.get(i + 1).and_then(|a| crate::util::parse_i64(a)).map(|v| v.max(0) as u64);
        }
    }
    None
}

/// Encode a `RespValue` to RESP wire bytes.
pub fn encode_value(v: &RespValue) -> Vec<u8> {
    let mut out = Vec::new();
    encode_reply(v, &mut out);
    out
}

/// Encode a command result to RESP wire bytes.
pub fn encode_result(r: CmdResult) -> Vec<u8> {
    encode_value(&r.into_resp_value())
}
