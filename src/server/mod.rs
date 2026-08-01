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

    /// Key indices for a command. Handles movable keys (XREAD/XREADGROUP) by
    /// locating the STREAMS section.
    pub fn extract_keys(&self, cmd: &'static Command, args: &[Vec<u8>]) -> Vec<usize> {
        if cmd.flags & crate::commands::FLAG_MOVABLEKEYS != 0 {
            extract_movable_keys(args)
        } else {
            cmd.key_range.keys(args.len())
        }
    }
}

pub fn extract_movable_keys(args: &[Vec<u8>]) -> Vec<usize> {
    for i in 1..args.len() {
        if args[i].eq_ignore_ascii_case(b"STREAMS") {
            let remaining = args.len() - i - 1;
            if remaining == 0 || remaining % 2 != 0 {
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
