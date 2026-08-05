use std::collections::{HashMap, VecDeque};
use std::sync::mpsc;

use crate::commands::exec::server::now_ms;
use crate::commands::{OpContext, ShardPart};
use crate::core::DbSlice;
use crate::core::compact::CompactString;
use crate::error::CmdResult;
use crate::server::journal::{self, JournalItem, JournalSlice, OP_COMMAND};
use crate::server::{MAX_DB, Reply, ShardMsg, SingleOp, WatchState, command_for, encode_result};

/// Context for an active transaction on this shard, stored between `TxLock` and
/// `TxExec`.
struct TxCtx {
    args: Vec<Vec<u8>>,
    owned_key_idxs: Vec<usize>,
    first_key_idx: usize,
    db_idx: usize,
    owns_all_keys: bool,
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
    /// The replication journal for this shard, created once a replica's
    /// `DFLY FLOW` arrives. While present, every write is recorded into it.
    journal: Option<JournalSlice>,
    /// Stable-sync consumers registered for replica flows: `(sync_id, flow_id,
    /// consumer_id)`. The consumer id unregisters a flow's subscription.
    repl_consumers: Vec<(u32, usize, usize)>,
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
                journal: None,
                repl_consumers: Vec::new(),
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
                owns_all_keys,
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
                        owns_all_keys,
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
                            ctx.owns_all_keys,
                            tx_id,
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
                // Drop any leftover tx context (e.g. a script subcommand that
                // locked the shard without ever dispatching a `TxExec`).
                self.tx_ctx.remove(&tx_id);
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
                self.journal_store_value(&key, value.as_ref(), expire_at, sticky, db_idx, tx_id);
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
                owns_all_keys,
                result_tx,
            } => {
                let result = self.run_exec(
                    &args,
                    &owned_key_idxs,
                    first_key_idx,
                    db_idx,
                    owns_all_keys,
                    0,
                );
                let _ = result_tx.send(result);
            }
            ShardMsg::ScriptBatch { cmds, result_tx } => {
                // The shard is locked by the script's transaction, so every
                // entry runs inline in order and the results go back as one
                // reply (one squashed hop, like `MultiCommandSquasher`).
                let results = cmds
                    .iter()
                    .map(|c| {
                        self.run_exec(
                            &c.args,
                            &c.owned_key_idxs,
                            c.first_key_idx,
                            c.db_idx,
                            c.owns_all_keys,
                            0,
                        )
                    })
                    .collect();
                let _ = result_tx.send(results);
            }
            ShardMsg::EnableJournal { enabled } => {
                if enabled && self.journal.is_none() {
                    self.journal = Some(JournalSlice::new());
                } else if !enabled {
                    self.journal = None;
                    self.repl_consumers.clear();
                }
            }
            ShardMsg::FullSyncSnapshot { result_tx } => {
                // Cut the snapshot at the current journal LSN; records issued
                // after it are replayed from the ring at stable-sync start.
                let journal_lsn = self.journal.as_ref().map_or(0, JournalSlice::lsn);
                let stream = crate::core::rdb::save_shard_full_sync(&self.dbs, journal_lsn);
                let _ = result_tx.send(crate::server::replication::FullSyncData {
                    stream,
                    journal_lsn,
                });
            }
            ShardMsg::StartStableSync {
                sync_id,
                flow_id,
                from_lsn,
                repl_tx,
                result_tx,
            } => {
                let result = self.start_stable_sync(sync_id, flow_id, from_lsn, repl_tx);
                let _ = result_tx.send(result);
            }
            ShardMsg::StopReplication { sync_id, flow_id } => {
                self.stop_replication(sync_id, flow_id);
            }
            ShardMsg::IsLsnInBuffer { lsn, result_tx } => {
                let in_buffer = self
                    .journal
                    .as_ref()
                    .map_or(false, |j| j.is_lsn_in_buffer(lsn));
                let _ = result_tx.send(in_buffer);
            }
            ShardMsg::ReplicaOp { args, db_idx, ack } => {
                let _ = self.apply_replica(&args, db_idx);
                let _ = ack.send(());
            }
            ShardMsg::ReplicaLoadValue {
                db_idx,
                key,
                value,
                expire_at,
            } => {
                let db = self.ensure_db(db_idx);
                db.insert(&key, value);
                match expire_at {
                    Some(at) => db.set_expiry(&key, at, now_ms()),
                    None => db.clear_expiry(&key),
                }
            }
            ShardMsg::ReplicaFlushAll { ack } => {
                let _ = self.run_flushall();
                let _ = ack.send(());
            }
        }
    }

    /// `JournalStreamer::MaybePartialStreamLSNs` then `RegisterConsumer`:
    /// replay every record in `[from_lsn, current)` from the ring (closing the
    /// gap between the full-sync cut and this moment), then subscribe to new
    /// records. Replays and live records are both forwarded to the flow's
    /// connection through `repl_tx`. A failed catch-up (a needed record was
    /// evicted from the ring) is an error: the replica must full-sync again.
    fn start_stable_sync(
        &mut self,
        sync_id: u32,
        flow_id: usize,
        from_lsn: u64,
        repl_tx: mpsc::Sender<crate::server::replication::ReplChunk>,
    ) -> Result<(), String> {
        let Some(journal) = &mut self.journal else {
            return Err("ERR replication journal is not enabled".into());
        };
        let mut cb = crate::server::replication::flow_consumer(repl_tx, sync_id, flow_id);
        let mut lsn = from_lsn;
        while lsn < journal.lsn() {
            let Some(data) = journal.get_entry(lsn) else {
                return Err(format!(
                    "ERR journal entry {lsn} dropped from the buffer, full sync required"
                ));
            };
            let item = JournalItem {
                lsn,
                data: data.to_vec(),
            };
            cb(&item);
            lsn += 1;
        }
        let consumer_id = journal.register_consumer(cb);
        self.repl_consumers.push((sync_id, flow_id, consumer_id));
        Ok(())
    }

    /// Drop a flow's stable-sync subscription, if one was registered.
    fn stop_replication(&mut self, sync_id: u32, flow_id: usize) {
        let Some(idx) = self
            .repl_consumers
            .iter()
            .position(|(s, f, _)| *s == sync_id && *f == flow_id)
        else {
            return;
        };
        let (_, _, consumer_id) = self.repl_consumers.remove(idx);
        if let Some(journal) = &mut self.journal {
            journal.unregister_consumer(consumer_id);
        }
    }

    fn execute_single(&mut self, op: &SingleOp) {
        let first_key_idx = command_for(&op.args).map_or(0, |c| c.key_range.first);
        let result = self.run_exec(
            &op.args,
            &op.owned_key_idxs,
            first_key_idx,
            op.db_idx,
            true,
            0,
        );
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
        owns_all_keys: bool,
        txid: u64,
    ) -> CmdResult {
        let Some(cmd) = command_for(args) else {
            return CmdResult::err("ERR unknown command");
        };
        let journal_enabled = self.journal.is_some();
        let (result, spop) = self.exec_core(args, owned, first_key_idx, db_idx);
        // SPOP pops random members, so the journal can't replay it verbatim:
        // journal the deterministic rewrite (SREM of the popped members, or DEL
        // when the pop drained the set) instead. `exec_core` computes the
        // rewrite while the DB borrow is live; it is journaled here afterwards.
        if let Some(record) = spop {
            if journal_enabled {
                if let Some(j) = &mut self.journal {
                    let data = journal::serialize_record(
                        txid,
                        OP_COMMAND,
                        db_idx as u64,
                        0,
                        &record[0],
                        &record[1..],
                    );
                    j.record(data);
                }
            }
        }
        self.maybe_journal(cmd, args, owned, owns_all_keys, db_idx, txid, &result);
        result
    }

    /// Execute `args` against `db_idx` and return `(result, optional SPOP
    /// rewrite)`. The SPOP rewrite is computed while the DB borrow is live and
    /// journaled by `run_exec` after the borrow ends; replica applies share
    /// this path, so journal replay never journals a second time.
    fn exec_core(
        &mut self,
        args: &[Vec<u8>],
        owned: &[usize],
        first_key_idx: usize,
        db_idx: usize,
    ) -> (CmdResult, Option<Vec<Vec<u8>>>) {
        let Some(cmd) = command_for(args) else {
            return (CmdResult::err("ERR unknown command"), None);
        };
        // MOVE operates on two DBs on the same shard, so it needs the raw
        // `dbs` vector rather than a single `OpContext`.
        if cmd.name == "MOVE" {
            return (self.run_move(args, db_idx), None);
        }
        // FLUSHALL clears every DB on the shard and dirties every WATCH (across
        // all DBs), mirroring upstream `FlushDbIndexes` + `InvalidateDbWatches`.
        if cmd.name == "FLUSHALL" {
            return (self.run_flushall(), None);
        }
        let db = self.ensure_db(db_idx);
        let mut ctx = OpContext {
            db,
            args,
            owned_keys: owned,
            first_key_idx,
            now_ms: now_ms(),
        };
        let result = (cmd.exec)(&mut ctx);
        let mut spop: Option<Vec<Vec<u8>>> = None;
        if cmd.name == "SPOP" {
            let members = spop_members(&result);
            if !members.is_empty() {
                let key = args[1].clone();
                let still_exists = ctx.db.contains(&key, ctx.now_ms);
                let record = if still_exists {
                    let mut rec = Vec::with_capacity(2 + members.len());
                    rec.push(b"SREM".to_vec());
                    rec.push(key);
                    rec.extend(members.iter().cloned());
                    rec
                } else {
                    vec![b"DEL".to_vec(), key]
                };
                spop = Some(record);
            }
        }
        (result, spop)
    }

    /// Apply a journal record on a replica: re-run `args` on this shard without
    /// touching the journal. Mirrors the reference replica's single-shard
    /// apply (`replica.cc::ExecuteTx`).
    fn apply_replica(&mut self, args: &[Vec<u8>], db_idx: usize) -> CmdResult {
        let Some(cmd) = command_for(args) else {
            return CmdResult::err("ERR unknown command");
        };
        let owned = crate::server::extract_keys(cmd, args);
        self.exec_core(args, &owned, cmd.key_range.first, db_idx).0
    }

    /// Record a write command into the replication journal, if one is enabled.

    /// Record a write command into the replication journal, if one is enabled.
    ///
    /// The record carries the full command tail when this shard owns all the
    /// command's keys, and the reduced per-shard args (`ShardArgs`) otherwise;
    /// the replica re-runs either form on the flow's own shard. Only commands
    /// that actually mutated (an `Ok` shard result) are journaled, and only
    /// commands whose replay is deterministic: `FLAG_NO_AUTOJOURNAL` commands
    /// (SPOP, blocking writes) journal an explicit rewrite or nothing at all,
    /// and `FLAG_NO_REDUCED` commands (multi-key stores) are journaled by the
    /// coordinator's `ShardMsg::StoreValue` when their keys span shards.
    fn maybe_journal(
        &mut self,
        cmd: &'static crate::commands::Command,
        args: &[Vec<u8>],
        owned: &[usize],
        owns_all_keys: bool,
        db_idx: usize,
        txid: u64,
        result: &CmdResult,
    ) {
        let Some(journal) = &mut self.journal else {
            return;
        };
        if !cmd.has_flag(crate::commands::FLAG_WRITE) {
            return;
        }
        if !matches!(result, CmdResult::Ok(_)) {
            return;
        }
        if cmd.has_flag(crate::commands::FLAG_NO_AUTOJOURNAL) {
            return;
        }
        if !owns_all_keys && cmd.has_flag(crate::commands::FLAG_NO_REDUCED) {
            return;
        }
        let tail: Vec<Vec<u8>> = if owns_all_keys {
            args[1..].to_vec()
        } else {
            let jargs = journal::shard_args(cmd, args, owned);
            jargs[1..].to_vec()
        };
        let data = journal::serialize_record(txid, OP_COMMAND, db_idx as u64, 0, cmd.name.as_bytes(), &tail);
        journal.record(data);
    }

    /// Record a deferred store/delete (e.g. BITOP's destination key) as the
    /// equivalent client commands, so the replica applies the same mutation.
    fn journal_store_value(
        &mut self,
        key: &[u8],
        value: Option<&crate::core::PrimeValue>,
        expire_at: Option<u64>,
        sticky: bool,
        db_idx: usize,
        txid: u64,
    ) {
        let Some(journal) = &mut self.journal else {
            return;
        };
        let mut records = Vec::new();
        match value {
            None => {
                records.push(vec![b"DEL".to_vec(), key.to_vec()]);
            }
            Some(crate::core::PrimeValue::Str(s)) => {
                let mut args = vec![b"SET".to_vec(), key.to_vec(), s.as_bytes().to_vec()];
                if let Some(at) = expire_at {
                    args.extend_from_slice(&[b"PXAT".to_vec(), at.to_string().into_bytes()]);
                }
                records.push(args);
                if sticky {
                    records.push(vec![b"STICK".to_vec(), key.to_vec()]);
                }
            }
            Some(v) => {
                let ttl = expire_at.unwrap_or(0).to_string().into_bytes();
                let args = vec![
                    b"RESTORE".to_vec(),
                    key.to_vec(),
                    ttl,
                    crate::core::rdb::dump_value(v),
                    b"ABSTTL".to_vec(),
                    b"REPLACE".to_vec(),
                ];
                records.push(args);
                if sticky {
                    records.push(vec![b"STICK".to_vec(), key.to_vec()]);
                }
            }
        }
        for args in records {
            let data = journal::serialize_record(
                txid,
                OP_COMMAND,
                db_idx as u64,
                0,
                &args[0],
                &args[1..],
            );
            journal.record(data);
        }
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

/// The members SPOP removed, from its reply (`SPOP key` -> one bulk,
/// `SPOP key count` -> an array of bulks). Empty when nothing was popped.
fn spop_members(result: &CmdResult) -> Vec<Vec<u8>> {
    match result {
        CmdResult::Ok(crate::error::RespValue::Bulk(b)) => vec![b.clone()],
        CmdResult::Ok(crate::error::RespValue::Array(items)) => items
            .iter()
            .filter_map(|i| match i {
                crate::error::RespValue::Bulk(b) => Some(b.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}
