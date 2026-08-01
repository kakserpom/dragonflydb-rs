use std::collections::VecDeque;
use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use crate::commands::exec::server::now_ms;
use crate::commands::ShardPart;
use crate::error::{CmdResult, RespValue};
use crate::server::{
    command_for, encode_result, keys_per_shard, parse_block_ms, shard_for_key, CoordMsg, Reply,
    ReplyBus, ShardMsg,
};

/// A blocking command (XREAD/XREADGROUP) waiting for data or a timeout. The
/// coordinator re-runs it until it returns data or the deadline passes.
struct PendingTx {
    msg: CoordMsg,
    deadline_ms: Option<u64>,
}

pub fn spawn(
    num_shards: usize,
    rx: mpsc::Receiver<CoordMsg>,
    shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    reply_bus: ReplyBus,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("coordinator".into())
        .spawn(move || {
            Coordinator {
                num_shards,
                rx,
                shard_txs,
                reply_bus,
                tx_counter: 0,
                pending: VecDeque::new(),
            }
            .run();
        })
        .expect("failed to spawn coordinator thread")
}

struct Coordinator {
    num_shards: usize,
    rx: mpsc::Receiver<CoordMsg>,
    shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    reply_bus: ReplyBus,
    tx_counter: u64,
    pending: VecDeque<PendingTx>,
}

impl Coordinator {
    fn run(&mut self) {
        const POLL: Duration = Duration::from_millis(20);
        loop {
            match self.rx.recv_timeout(POLL) {
                Ok(msg) => self.handle(msg),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            self.retry_pending(now_ms());
        }
    }

    fn handle(&mut self, msg: CoordMsg) {
        match self.execute_tx(&msg) {
            CmdResult::Blocked => {
                let deadline_ms = match parse_block_ms(&msg.args) {
                    Some(0) => None, // wait forever
                    Some(ms) => Some(now_ms().saturating_add(ms)),
                    None => Some(now_ms()),
                };
                self.pending.push_back(PendingTx { msg, deadline_ms });
            }
            other => self.reply_result(msg.conn_id, msg.seq, other),
        }
    }

    fn retry_pending(&mut self, now: u64) {
        if self.pending.is_empty() {
            return;
        }
        let mut remaining = Vec::with_capacity(self.pending.len());
        while let Some(p) = self.pending.pop_front() {
            if let Some(dl) = p.deadline_ms {
                if now >= dl {
                    let bytes = encode_result(CmdResult::Ok(RespValue::Nil));
                    self.reply(p.msg.conn_id, p.msg.seq, bytes);
                    continue;
                }
            }
            match self.execute_tx(&p.msg) {
                CmdResult::Blocked => remaining.push(p),
                other => self.reply_result(p.msg.conn_id, p.msg.seq, other),
            }
        }
        self.pending.extend(remaining);
    }

    fn execute_tx(&mut self, msg: &CoordMsg) -> CmdResult {
        let tx_id = self.next_tx_id();
        let owned = keys_per_shard(&msg.args, &msg.keys, self.num_shards);

        // Phase 1: lock every involved shard and wait until all have acked.
        let mut ack_rxs = Vec::new();
        for &s in &msg.shards {
            let (ack_tx, ack_rx) = mpsc::channel();
            let ok = self.shard_txs[s].send(ShardMsg::TxLock {
                tx_id,
                conn_id: msg.conn_id,
                seq: msg.seq,
                args: msg.args.clone(),
                owned_key_idxs: owned_for(&owned, s),
                first_key_idx: msg.first_key_idx,
                ack: ack_tx,
            });
            if ok.is_ok() {
                ack_rxs.push(ack_rx);
            }
        }
        for rx in &ack_rxs {
            let _ = rx.recv();
        }

        // Phase 2: run the executor on each shard and collect partial results.
        let mut parts: Vec<ShardPart> = Vec::new();
        for &s in &msg.shards {
            let (res_tx, res_rx) = mpsc::channel();
            if self.shard_txs[s].send(ShardMsg::TxExec { tx_id, result_tx: res_tx }).is_ok() {
                if let Ok(p) = res_rx.recv() {
                    parts.push(p);
                }
            }
        }

        // Phase 3: release the locks.
        for &s in &msg.shards {
            let _ = self.shard_txs[s].send(ShardMsg::TxUnlock { tx_id });
        }

        match self.finish_tx(msg, parts) {
            CmdResult::DeferredStore { key, value, reply } => {
                self.perform_deferred_store(&key, value, None, false);
                CmdResult::Ok(reply)
            }
            CmdResult::DeferredStores { stores, reply } => {
                for (key, value, expire_at, sticky) in stores {
                    self.perform_deferred_store(&key, value, expire_at, sticky);
                }
                CmdResult::Ok(reply)
            }
            other => other,
        }
    }

    /// Store (or delete) a key produced by a multi-shard command on its shard,
    /// holding the shard lock like a normal transaction.
    fn perform_deferred_store(
        &mut self,
        key: &[u8],
        value: Option<crate::core::PrimeValue>,
        expire_at: Option<u64>,
        sticky: bool,
    ) {
        let tx_id = self.next_tx_id();
        let shard = shard_for_key(key, self.num_shards);
        let (ack_tx, ack_rx) = mpsc::channel();
        if self.shard_txs[shard]
            .send(ShardMsg::StoreValue {
                tx_id,
                key: key.to_vec(),
                value,
                expire_at,
                sticky,
                ack: ack_tx,
            })
            .is_ok()
        {
            let _ = ack_rx.recv();
            let _ = self.shard_txs[shard].send(ShardMsg::TxUnlock { tx_id });
        }
    }

    fn finish_tx(&self, msg: &CoordMsg, parts: Vec<ShardPart>) -> CmdResult {
        let any_err = parts.iter().any(|p| p.result.is_err());
        let any_ok = parts.iter().any(|p| matches!(&p.result, CmdResult::Ok(_)));
        if parts.is_empty() {
            return CmdResult::err("ERR internal: no shards participated");
        }
        if any_err || any_ok {
            let Some(cmd) = command_for(&msg.args) else {
                return CmdResult::err("ERR unknown command");
            };
            if let Some(merge) = cmd.merge {
                merge(&parts, &msg.args, &msg.keys, now_ms())
            } else {
                parts[0].result.clone()
            }
        } else {
            CmdResult::Blocked
        }
    }

    fn next_tx_id(&mut self) -> u64 {
        self.tx_counter += 1;
        self.tx_counter
    }

    fn reply_result(&self, conn_id: u64, seq: u64, r: CmdResult) {
        self.reply(conn_id, seq, encode_result(r));
    }

    fn reply(&self, conn_id: u64, seq: u64, bytes: Vec<u8>) {
        self.reply_bus.send(Reply { conn_id, seq, bytes });
    }
}

/// Key indices owned by `shard` from a `keys_per_shard` grouping.
fn owned_for(per: &[(usize, Vec<usize>)], shard: usize) -> Vec<usize> {
    per.iter().find(|(s, _)| *s == shard).map(|(_, v)| v.clone()).unwrap_or_default()
}
