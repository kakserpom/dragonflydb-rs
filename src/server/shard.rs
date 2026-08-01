use std::collections::{HashMap, VecDeque};
use std::sync::mpsc;

use crate::commands::exec::server::now_ms;
use crate::commands::{OpContext, ShardPart};
use crate::core::DbSlice;
use crate::error::CmdResult;
use crate::server::{command_for, encode_result, Reply, ShardMsg, SingleOp};

/// Context for an active transaction on this shard, stored between TxLock and
/// TxExec.
struct TxCtx {
    args: Vec<Vec<u8>>,
    owned_key_idxs: Vec<usize>,
    first_key_idx: usize,
}

struct Shard {
    shard_id: usize,
    /// Logical databases, index 0..N. Only db0 is used today.
    dbs: Vec<DbSlice>,
    /// The tx currently holding this shard, if any. While set, single ops are
    /// queued so a multi-shard transaction executes atomically w.r.t. singles.
    active_tx: Option<u64>,
    tx_ctx: HashMap<u64, TxCtx>,
    pending_singles: VecDeque<SingleOp>,
}

pub fn spawn(shard_id: usize, rx: mpsc::Receiver<ShardMsg>) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("shard-{}", shard_id))
        .spawn(move || {
            let mut shard = Shard {
                shard_id,
                dbs: vec![DbSlice::new(shard_id)],
                active_tx: None,
                tx_ctx: HashMap::new(),
                pending_singles: VecDeque::new(),
            };
            shard.run(rx);
        })
        .expect("failed to spawn shard thread")
}

impl Shard {
    fn run(&mut self, rx: mpsc::Receiver<ShardMsg>) {
        while let Ok(msg) = rx.recv() {
            self.handle(msg);
        }
    }

    fn handle(&mut self, msg: ShardMsg) {
        match msg {
            ShardMsg::Single(op) => {
                if self.active_tx.is_some() {
                    self.pending_singles.push_back(op);
                } else {
                    self.execute_single(op);
                }
            }
            ShardMsg::TxLock { tx_id, args, owned_key_idxs, first_key_idx, ack, .. } => {
                self.active_tx = Some(tx_id);
                self.tx_ctx.insert(
                    tx_id,
                    TxCtx { args, owned_key_idxs, first_key_idx },
                );
                let _ = ack.send(());
            }
            ShardMsg::TxExec { tx_id, result_tx } => {
                let part = match self.tx_ctx.remove(&tx_id) {
                    Some(ctx) => {
                        let result =
                            self.run_exec(&ctx.args, &ctx.owned_key_idxs, ctx.first_key_idx);
                        ShardPart {
                            shard: self.shard_id,
                            owned_key_idxs: ctx.owned_key_idxs,
                            result,
                        }
                    }
                    None => ShardPart {
                        shard: self.shard_id,
                        owned_key_idxs: vec![],
                        result: CmdResult::err("ERR internal: transaction not locked"),
                    },
                };
                let _ = result_tx.send(part);
            }
            ShardMsg::TxUnlock { tx_id } => {
                if self.active_tx == Some(tx_id) {
                    self.active_tx = None;
                    while self.active_tx.is_none() {
                        match self.pending_singles.pop_front() {
                            Some(op) => self.execute_single(op),
                            None => break,
                        }
                    }
                }
            }
            ShardMsg::StoreValue { tx_id, key, value, ack } => {
                self.active_tx = Some(tx_id);
                match value {
                    Some(v) => {
                        let db = &mut self.dbs[0];
                        db.insert(crate::core::compact::CompactString::from_bytes(&key), v);
                        db.clear_expiry(&key);
                    }
                    None => {
                        self.dbs[0].remove(&key);
                    }
                }
                let _ = ack.send(());
            }
        }
    }

    fn execute_single(&mut self, op: SingleOp) {
        let first_key_idx =
            command_for(&op.args).map(|c| c.key_range.first).unwrap_or(0);
        let result = self.run_exec(&op.args, &op.owned_key_idxs, first_key_idx);
        let reply = Reply {
            conn_id: op.conn_id,
            seq: op.seq,
            bytes: encode_result(result),
        };
        let _ = op.reply.send(reply);
    }

    fn run_exec(&mut self, args: &[Vec<u8>], owned: &[usize], first_key_idx: usize) -> CmdResult {
        let Some(cmd) = command_for(args) else {
            return CmdResult::err("ERR unknown command");
        };
        let db = &mut self.dbs[0];
        let mut ctx = OpContext {
            db,
            args,
            owned_keys: owned,
            first_key_idx,
            now_ms: now_ms(),
        };
        (cmd.exec)(&mut ctx)
    }
}
