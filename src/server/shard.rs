use std::collections::{HashMap, VecDeque};
use std::sync::mpsc;

use crate::commands::exec::server::now_ms;
use crate::commands::{OpContext, ShardPart};
use crate::core::DbSlice;
use crate::core::compact::CompactString;
use crate::error::CmdResult;
use crate::server::{MAX_DB, Reply, ShardMsg, SingleOp, WatchState, command_for, encode_result};

/// Context for an active transaction on this shard, stored between `TxLock` and
/// `TxExec`.
struct TxCtx {
    args: Vec<Vec<u8>>,
    owned_key_idxs: Vec<usize>,
    first_key_idx: usize,
    db_idx: usize,
}

/// A watch snapshot query queued while a transaction holds the shard:
/// (keys, db index, reply channel).
type PendingWatch = (
    Vec<Vec<u8>>,
    usize,
    mpsc::Sender<Vec<(Vec<u8>, WatchState)>>,
);

struct Shard {
    id: usize,
    /// Logical databases, index 0..N, grown lazily on demand.
    dbs: Vec<DbSlice>,
    /// The tx currently holding this shard, if any. While set, single ops are
    /// queued so a multi-shard transaction executes atomically w.r.t. singles.
    active_tx: Option<u64>,
    tx_ctx: HashMap<u64, TxCtx>,
    pending_singles: VecDeque<SingleOp>,
    /// Watch snapshots queued while a transaction holds the shard.
    pending_watches: VecDeque<PendingWatch>,
}

#[must_use]
pub fn spawn(shard_id: usize, rx: mpsc::Receiver<ShardMsg>) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("shard-{shard_id}"))
        .spawn(move || {
            let mut shard = Shard {
                id: shard_id,
                dbs: vec![DbSlice::new(shard_id)],
                active_tx: None,
                tx_ctx: HashMap::new(),
                pending_singles: VecDeque::new(),
                pending_watches: VecDeque::new(),
            };
            shard.run(&rx);
        })
        .expect("failed to spawn shard thread")
}

impl Shard {
    fn run(&mut self, rx: &mpsc::Receiver<ShardMsg>) {
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
                    self.execute_single(&op);
                }
            }
            ShardMsg::TxLock {
                tx_id,
                args,
                owned_key_idxs,
                first_key_idx,
                db_idx,
                ack,
                ..
            } => {
                self.active_tx = Some(tx_id);
                self.tx_ctx.insert(
                    tx_id,
                    TxCtx {
                        args,
                        owned_key_idxs,
                        first_key_idx,
                        db_idx,
                    },
                );
                let _ = ack.send(());
            }
            ShardMsg::TxExec { tx_id, result_tx } => {
                let part = match self.tx_ctx.remove(&tx_id) {
                    Some(ctx) => {
                        let result = self.run_exec(
                            &ctx.args,
                            &ctx.owned_key_idxs,
                            ctx.first_key_idx,
                            ctx.db_idx,
                        );
                        ShardPart {
                            shard: self.id,
                            owned_key_idxs: ctx.owned_key_idxs,
                            result,
                        }
                    }
                    None => ShardPart {
                        shard: self.id,
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
                            Some(op) => self.execute_single(&op),
                            None => break,
                        }
                    }
                    while let Some((keys, db_idx, tx)) = self.pending_watches.pop_front() {
                        self.run_watch_query(&keys, db_idx, &tx);
                    }
                }
            }
            ShardMsg::WatchQuery {
                keys,
                db_idx,
                result_tx,
            } => {
                if self.active_tx.is_some() {
                    self.pending_watches.push_back((keys, db_idx, result_tx));
                } else {
                    self.run_watch_query(&keys, db_idx, &result_tx);
                }
            }
            ShardMsg::StoreValue {
                tx_id,
                key,
                value,
                expire_at,
                sticky,
                db_idx,
                ack,
            } => {
                self.active_tx = Some(tx_id);
                match value {
                    Some(v) => {
                        let db = self.ensure_db(db_idx);
                        db.insert(&key, v);
                        match expire_at {
                            Some(at) => db.set_expiry(&key, at, now_ms()),
                            None => db.clear_expiry(&key),
                        }
                        db.set_sticky_flag(&key, sticky);
                    }
                    None => {
                        self.ensure_db(db_idx).remove(&key);
                    }
                }
                let _ = ack.send(());
            }
            ShardMsg::ScriptOp {
                args,
                owned_key_idxs,
                first_key_idx,
                db_idx,
                result_tx,
            } => {
                let result = self.run_exec(&args, &owned_key_idxs, first_key_idx, db_idx);
                let _ = result_tx.send(result);
            }
            ShardMsg::ScriptBatch { cmds, result_tx } => {
                // The shard is locked by the script's transaction, so every
                // entry runs inline in order and the results go back as one
                // reply (one squashed hop, like `MultiCommandSquasher`).
                let results = cmds
                    .iter()
                    .map(|c| self.run_exec(&c.args, &c.owned_key_idxs, c.first_key_idx, c.db_idx))
                    .collect();
                let _ = result_tx.send(results);
            }
        }
    }

    fn execute_single(&mut self, op: &SingleOp) {
        let first_key_idx = command_for(&op.args).map_or(0, |c| c.key_range.first);
        let result = self.run_exec(&op.args, &op.owned_key_idxs, first_key_idx, op.db_idx);
        let reply = Reply {
            conn_id: op.conn_id,
            seq: op.seq,
            bytes: encode_result(result),
        };
        op.reply.send(reply);
    }

    /// Snapshot the watched-state of each key, in input order.
    fn run_watch_query(
        &mut self,
        keys: &[Vec<u8>],
        db_idx: usize,
        result_tx: &mpsc::Sender<Vec<(Vec<u8>, WatchState)>>,
    ) {
        let now = now_ms();
        let out = {
            let db = self.ensure_db(db_idx);
            keys.iter()
                .map(|k| {
                    let existed = db.contains(k, now);
                    let state = WatchState {
                        version: db.version_of(k),
                        existed,
                        db_epoch: db.db_epoch(),
                    };
                    (k.clone(), state)
                })
                .collect()
        };
        let _ = result_tx.send(out);
    }

    fn run_exec(
        &mut self,
        args: &[Vec<u8>],
        owned: &[usize],
        first_key_idx: usize,
        db_idx: usize,
    ) -> CmdResult {
        let Some(cmd) = command_for(args) else {
            return CmdResult::err("ERR unknown command");
        };
        // MOVE operates on two DBs on the same shard, so it needs the raw
        // `dbs` vector rather than a single `OpContext`.
        if cmd.name == "MOVE" {
            return self.run_move(args, db_idx);
        }
        // FLUSHALL clears every DB on the shard and dirties every WATCH (across
        // all DBs), mirroring upstream `FlushDbIndexes` + `InvalidateDbWatches`.
        if cmd.name == "FLUSHALL" {
            return self.run_flushall();
        }
        let db = self.ensure_db(db_idx);
        let mut ctx = OpContext {
            db,
            args,
            owned_keys: owned,
            first_key_idx,
            now_ms: now_ms(),
        };
        (cmd.exec)(&mut ctx)
    }

    /// Ensure db `db_idx` exists (created lazily, like `ActivateDb`).
    fn ensure_db(&mut self, db_idx: usize) -> &mut DbSlice {
        if self.dbs.len() <= db_idx {
            self.dbs.resize_with(db_idx + 1, || DbSlice::new(self.id));
        }
        &mut self.dbs[db_idx]
    }

    /// `MOVE key <db>`: move the value, TTL and sticky flag to another DB on
    /// the same shard. Runs on every shard (global tx); only the shard owning
    /// `key` can move it. Returns 0 if the key is missing or the destination
    /// key already exists, 1 otherwise.
    fn run_move(&mut self, args: &[Vec<u8>], db_idx: usize) -> CmdResult {
        if let Some(t) = args.get(2).and_then(|a| crate::util::parse_i64(a))
            && (0..MAX_DB as i64).contains(&t)
        {
            self.ensure_db(db_idx);
            self.ensure_db(t as usize);
        }
        crate::commands::exec::keys::exec_move_on_dbs(&mut self.dbs, db_idx, args, now_ms())
    }

    /// `FLUSHALL`: drain every DB on this shard and bump each DB epoch so that
    /// every WATCH (in any DB) becomes dirty at the next EXEC.
    fn run_flushall(&mut self) -> CmdResult {
        for db in &mut self.dbs {
            let keys: Vec<CompactString> = db.iter().map(|(k, _)| k.clone()).collect();
            for k in keys {
                db.remove(k.as_bytes());
            }
            db.bump_db_epoch();
        }
        CmdResult::Ok(crate::commands::ok())
    }
}
