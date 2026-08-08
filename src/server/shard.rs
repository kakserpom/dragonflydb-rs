use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, mpsc};

use crate::commands::exec::server::now_ms;
use crate::commands::{OpContext, ShardPart};
use crate::core::DbSlice;
use crate::core::compact::CompactString;
use crate::error::{CmdResult, RespValue};
use crate::server::journal::{self, JournalItem, JournalSlice, OP_COMMAND};
use crate::server::replication::{ChunkKind, FullSyncBus, ReplChunk};
use crate::server::{
    MAX_DB, Reply, ReplyBus, ShardMsg, SingleOp, Tracking, WatchState, command_for, encode_result,
    encode_value,
};

/// Context for an active transaction on this shard, stored between `TxLock` and
/// `TxExec`.
struct TxCtx {
    conn_id: u64,
    args: Vec<Vec<u8>>,
    owned_key_idxs: Vec<usize>,
    first_key_idx: usize,
    db_idx: usize,
    owns_all_keys: bool,
    /// Whether the issuing connection tracks keys (CLIENT TRACKING).
    track_keys: bool,
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
    /// In-flight chunked full-sync snapshots, keyed by `(sync_id, flow_id)`.
    /// A snapshot serializes its baseline one chunk at a time, returning to the
    /// message loop (and draining pending writes) between chunks so a full sync
    /// never stalls the shard.
    full_syncs: HashMap<(u32, usize), FullSyncState>,
    /// Shared CLIENT TRACKING table (connections' tracking state + key index).
    tracking: Arc<Mutex<Tracking>>,
    /// The reply bus for invalidation pushes, drained by the IO thread.
    tracking_bus: ReplyBus,
}

/// One DB's frozen baseline: the sorted key list captured at snapshot start
/// plus the `RESIZEDB` counts derived from it. Values are read live at
/// serialization time, so mutations that happen mid-snapshot surface here and
/// are replayed again as journal blobs (idempotent on the replica).
struct DbBaseline {
    dbid: usize,
    keys: Vec<Vec<u8>>,
    num_expires: usize,
}

/// The serialization state of one full-sync snapshot on this shard.
struct FullSyncState {
    /// Chunk delivery to the flow connection (pokes the IO thread's wake pipe).
    bus: FullSyncBus,
    dbs: Vec<DbBaseline>,
    db_idx: usize,
    key_idx: usize,
    /// Whether the current DB's `SELECTDB`/`RESIZEDB` header was written.
    db_header_written: bool,
    /// Whether the stream header (magic + AUX) was written (first chunk only).
    header_written: bool,
    /// Raw journal records captured since the snapshot started, replayed to the
    /// replica as `RDB_OPCODE_JOURNAL_BLOB` records on the final chunk. Shared
    /// with the journal consumer so records keep landing here while the shard
    /// serves writes between steps.
    records: Arc<Mutex<Vec<Vec<u8>>>>,
    consumer_id: usize,
}

#[must_use]
pub fn spawn(
    shard_id: usize,
    rx: mpsc::Receiver<ShardMsg>,
    tracking: Arc<Mutex<Tracking>>,
    tracking_bus: ReplyBus,
) -> std::thread::JoinHandle<()> {
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
                full_syncs: HashMap::new(),
                tracking,
                tracking_bus,
            };
            shard.run(&rx);
        })
        .expect("failed to spawn shard thread")
}

impl Shard {
    fn run(&mut self, rx: &mpsc::Receiver<ShardMsg>) {
        while let Ok(msg) = rx.recv() {
            self.handle(rx, msg);
        }
    }

    fn handle(&mut self, rx: &mpsc::Receiver<ShardMsg>, msg: ShardMsg) {
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
                conn_id,
                args,
                owned_key_idxs,
                first_key_idx,
                db_idx,
                owns_all_keys,
                track_keys,
                ack,
                ..
            } => {
                self.active_tx = Some(tx_id);
                self.tx_ctx.insert(
                    tx_id,
                    TxCtx {
                        conn_id,
                        args,
                        owned_key_idxs,
                        first_key_idx,
                        db_idx,
                        owns_all_keys,
                        track_keys,
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
                            ctx.conn_id,
                            tx_id,
                            ctx.track_keys,
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
                self.flush_invalidations();
                let _ = ack.send(());
            }
            ShardMsg::ScriptOp {
                args,
                owned_key_idxs,
                first_key_idx,
                db_idx,
                owns_all_keys,
                conn_id,
                track_keys,
                result_tx,
            } => {
                let result = self.run_exec(
                    &args,
                    &owned_key_idxs,
                    first_key_idx,
                    db_idx,
                    owns_all_keys,
                    conn_id,
                    0,
                    track_keys,
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
                            c.conn_id,
                            0,
                            c.track_keys,
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
            ShardMsg::FullSyncSnapshot {
                sync_id,
                flow_id,
                bus,
            } => {
                // Freeze the baseline key lists and start capturing journal
                // records; every write executed mid-snapshot is replayed to the
                // replica as a blob on the final chunk, so the snapshot can
                // preempt itself between chunks (no shard-wide write stall).
                let Some(journal) = &mut self.journal else {
                    return;
                };
                let records: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
                let records_cb = Arc::clone(&records);
                let consumer_id = journal.register_consumer(Box::new(move |item: &JournalItem| {
                    records_cb.lock().unwrap().push(item.data.clone());
                }));
                let dbs = self
                    .dbs
                    .iter()
                    .enumerate()
                    .map(|(dbid, db)| {
                        let mut keys: Vec<Vec<u8>> =
                            db.iter().map(|(k, _)| k.as_bytes().to_vec()).collect();
                        keys.sort_unstable();
                        let num_expires = keys.iter().filter(|k| db.expire_at(k).is_some()).count();
                        DbBaseline {
                            dbid,
                            keys,
                            num_expires,
                        }
                    })
                    .collect();
                let state = FullSyncState {
                    bus,
                    dbs,
                    db_idx: 0,
                    key_idx: 0,
                    db_header_written: false,
                    header_written: false,
                    records,
                    consumer_id,
                };
                self.full_syncs.insert((sync_id, flow_id), state);
                self.snapshot_step(rx, sync_id, flow_id);
            }
            ShardMsg::SnapshotStep { sync_id, flow_id } => {
                self.snapshot_step(rx, sync_id, flow_id);
            }
            ShardMsg::CancelFullSync { sync_id, flow_id } => {
                if let Some(state) = self.full_syncs.remove(&(sync_id, flow_id))
                    && let Some(journal) = &mut self.journal
                {
                    journal.unregister_consumer(state.consumer_id);
                }
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
                    .is_some_and(|j| j.is_lsn_in_buffer(lsn));
                let _ = result_tx.send(in_buffer);
            }
            ShardMsg::ReplicaOp { args, db_idx, ack } => {
                let _ = self.apply_replica(&args, db_idx);
                self.flush_invalidations();
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
                self.flush_invalidations();
            }
            ShardMsg::ReplicaFlushAll { ack } => {
                let _ = self.run_flushall();
                self.flush_invalidations();
                let _ = ack.send(());
            }
        }
    }

    /// Serialize one chunk of a full-sync snapshot: stream header (once),
    /// then baseline entries until `FULL_SYNC_CHUNK_BYTES` is reached. Between
    /// chunks the shard drains every pending message, so writes are stalled at
    /// most one chunk's serialization. The final chunk appends the cut
    /// marker, the `JOURNAL_OFFSET` and the RDB EOF, then unregisters the
    /// consumer.
    ///
    /// Ordering: journal records captured since the last chunk are folded into
    /// the chunk as blobs right after the baseline entries but before sending,
    /// so for any key the replica sees its baseline value strictly before a
    /// mutation blob that follows it, and a mutation blob strictly before any
    /// later baseline entry (which then carries the mutation's result, read
    /// live). Either interleaving converges on the master's final value; the
    /// alternative of trailing every blob after the whole baseline would
    /// double-apply non-idempotent commands.
    fn snapshot_step(&mut self, rx: &mpsc::Receiver<ShardMsg>, sync_id: u32, flow_id: usize) {
        // Phase 1: serialize one chunk (stream header once, then baseline
        // entries until `FULL_SYNC_CHUNK_BYTES`). The state borrow ends here.
        let (bus, mut chunk, done) = {
            let Some(state) = self.full_syncs.get_mut(&(sync_id, flow_id)) else {
                return;
            };
            let bus = state.bus.clone();
            let mut chunk = Vec::new();
            if !state.header_written {
                chunk.extend_from_slice(&crate::core::rdb::write_full_sync_header());
                state.header_written = true;
            }
            while chunk.len() < crate::core::rdb::FULL_SYNC_CHUNK_BYTES
                && state.db_idx < state.dbs.len()
            {
                let baseline = &state.dbs[state.db_idx];
                if !state.db_header_written {
                    crate::core::rdb::write_full_sync_db_header(
                        &mut chunk,
                        baseline.dbid,
                        baseline.keys.len(),
                        baseline.num_expires,
                    );
                    state.db_header_written = true;
                }
                if state.key_idx >= baseline.keys.len() {
                    state.db_idx += 1;
                    state.key_idx = 0;
                    state.db_header_written = false;
                    continue;
                }
                let key = baseline.keys[state.key_idx].clone();
                state.key_idx += 1;
                let _ = crate::core::rdb::write_full_sync_entry(
                    &mut chunk,
                    &mut self.dbs[baseline.dbid],
                    &key,
                    now_ms(),
                );
            }
            (bus, chunk, state.db_idx >= state.dbs.len())
        };

        // Phase 2: drain everything pending so no write outlives one chunk's
        // serialization, then fold the journal records captured by the drain
        // into the chunk as blobs (see the ordering note above).
        while let Ok(msg) = rx.try_recv() {
            self.handle(rx, msg);
        }
        if let Some(state) = self.full_syncs.get_mut(&(sync_id, flow_id)) {
            let records = std::mem::take(&mut *state.records.lock().unwrap());
            for record in &records {
                crate::core::rdb::write_journal_blob(&mut chunk, record);
            }
        }

        // Phase 3: finalize (tail + cut LSN) or ship the interim chunk.
        if done {
            if !self.full_syncs.contains_key(&(sync_id, flow_id)) {
                return;
            }
            let cut = self.journal.as_ref().map_or(0, JournalSlice::lsn);
            crate::core::rdb::write_full_sync_tail(&mut chunk, cut);
            let state = self.full_syncs.remove(&(sync_id, flow_id)).unwrap();
            if let Some(journal) = &mut self.journal {
                journal.unregister_consumer(state.consumer_id);
            }
            bus.send(ReplChunk {
                sync_id,
                flow_id,
                bytes: chunk,
                kind: ChunkKind::FullSync {
                    journal_lsn: Some(cut),
                },
            });
        } else {
            bus.send(ReplChunk {
                sync_id,
                flow_id,
                bytes: chunk,
                kind: ChunkKind::FullSync { journal_lsn: None },
            });
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
            op.conn_id,
            0,
            op.track_keys,
        );
        let reply = Reply {
            conn_id: op.conn_id,
            seq: op.seq,
            bytes: encode_result(result),
            slowlog_args: None,
            is_push: false,
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

    #[allow(clippy::too_many_arguments)]
    fn run_exec(
        &mut self,
        args: &[Vec<u8>],
        owned: &[usize],
        first_key_idx: usize,
        db_idx: usize,
        owns_all_keys: bool,
        conn_id: u64,
        txid: u64,
        track_keys: bool,
    ) -> CmdResult {
        let Some(cmd) = command_for(args) else {
            return CmdResult::err("ERR unknown command");
        };
        let journal_enabled = self.journal.is_some();
        let (result, spop) = self.exec_core(args, owned, first_key_idx, db_idx, conn_id);
        // SPOP pops random members, so the journal can't replay it verbatim:
        // journal the deterministic rewrite (SREM of the popped members, or DEL
        // when the pop drained the set) instead. `exec_core` computes the
        // rewrite while the DB borrow is live; it is journaled here afterwards.
        if let Some(record) = spop
            && journal_enabled
            && let Some(j) = &mut self.journal
        {
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
        self.maybe_journal(cmd, args, owned, owns_all_keys, db_idx, txid, &result);
        // Send invalidation pushes for every key the command modified; the
        // pushes land on the connection before the command's own reply, so a
        // tracked client observing a write sees the invalidate first. A flush
        // instead clears the whole map and broadcasts one null-keyed push.
        if matches!(cmd.name, "FLUSHDB" | "FLUSHALL") {
            self.flush_db_invalidations();
        } else {
            self.flush_invalidations();
        }
        // `TrackIfNeeded` (main_service.cc:816): only readonly commands record
        // their keys into the tracking map. Like the reference's transaction
        // tracking callback (invoked after the command ran, transaction.cc:721)
        // and its invalidations (computed at delete/update time, before the
        // callback), the keys are recorded after the flush so a lazy-expiry
        // delete the read triggers cannot invalidate its own freshly tracked
        // key - the reading connection gets re-added only after.
        if track_keys && cmd.has_flag(crate::commands::FLAG_READONLY) {
            let keys: Vec<Vec<u8>> = owned.iter().filter_map(|&i| args.get(i)).cloned().collect();
            if !keys.is_empty() {
                self.tracking.lock().unwrap().record_reads(conn_id, &keys);
            }
        }
        result
    }

    /// `SendQueuedInvalidationMessagesCb` (db_slice.cc): every key modified
    /// since the last drain is removed from the tracking map, and each of its
    /// tracked readers gets a `["invalidate", [key]]` push. Pushes are routed
    /// through the reply bus as unsequenced (`is_push`) messages.
    fn flush_invalidations(&mut self) {
        let keys: Vec<CompactString> = self
            .dbs
            .iter_mut()
            .flat_map(DbSlice::drain_modified)
            .collect();
        if keys.is_empty() {
            return;
        }
        let mut frames: Vec<(u64, Vec<u8>)> = Vec::new();
        {
            let mut tracking = self.tracking.lock().unwrap();
            for key in &keys {
                for conn_id in tracking.invalidate_key(key.as_bytes()) {
                    if tracking.conn(conn_id).is_some_and(|c| c.enabled) {
                        frames.push((conn_id, invalidation_push_frame(key.as_bytes())));
                    }
                }
            }
        }
        for (conn_id, bytes) in frames {
            self.tracking_bus.send(Reply {
                conn_id,
                seq: 0,
                bytes,
                slowlog_args: None,
                is_push: true,
            });
        }
    }

    /// `FlushDb` + `SendInvalidationMessages` (db_slice.cc:1100,
    /// server_family.cc:1985): a FLUSHDB/FLUSHALL drops every tracked key
    /// without per-key messages, then broadcasts a null-keyed
    /// `["invalidate", nil]` push to every connection with tracking on. Only
    /// shard 0 sends the push: FLUSHDB/FLUSHALL run on every shard but the
    /// broadcast must hit each connection exactly once.
    fn flush_db_invalidations(&mut self) {
        let conn_ids = self.tracking.lock().unwrap().invalidate_all();
        // The flushed keys were recorded as modified by `exec_core`; drop them
        // so a later write cannot resurrect a stale per-key invalidation.
        for db in &mut self.dbs {
            db.drain_modified();
        }
        if self.id != 0 {
            return;
        }
        let bytes = flush_invalidation_frame();
        for conn_id in conn_ids {
            self.tracking_bus.send(Reply {
                conn_id,
                seq: 0,
                bytes: bytes.clone(),
                slowlog_args: None,
                is_push: true,
            });
        }
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
        conn_id: u64,
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
        // INFO aggregates every DB on the shard (`ServerFamily::Info` reads the
        // whole per-thread `db_stats` array), not just the connection's current
        // DB, so it needs the raw `dbs` vector like MOVE.
        if cmd.name == "INFO" {
            return (self.run_info(), None);
        }
        let db = self.ensure_db(db_idx);
        // `DbSlice::find` counts a hit/miss like `FindInternal` with
        // `kReadStats`; write commands also call `find` for existence checks
        // (the reference uses `FindMutable` there, which does not count), so
        // roll the counters back when the command is not a read.
        let (hits_before, misses_before) = (db.stats.hits, db.stats.misses);
        let mut ctx = OpContext {
            db,
            args,
            owned_keys: owned,
            first_key_idx,
            conn_id,
            now_ms: now_ms(),
        };
        let result = (cmd.exec)(&mut ctx);
        if !cmd.has_flag(crate::commands::FLAG_READONLY) {
            ctx.db.stats.hits = hits_before;
            ctx.db.stats.misses = misses_before;
        }
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
        self.exec_core(args, &owned, cmd.key_range.first, db_idx, 0)
            .0
    }

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
    #[allow(clippy::too_many_arguments)]
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
        let data = journal::serialize_record(
            txid,
            OP_COMMAND,
            db_idx as u64,
            0,
            cmd.name.as_bytes(),
            &tail,
        );
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
            let data =
                journal::serialize_record(txid, OP_COMMAND, db_idx as u64, 0, &args[0], &args[1..]);
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

    /// Per-DB stats for INFO keyspace: one `[db, keys, expires, hits, misses]`
    /// entry per DB, which `merge_info` aggregates across shards
    /// (`ServerFamily::Info` reads the whole per-thread `db_stats` array).
    fn run_info(&self) -> CmdResult {
        let entries = self
            .dbs
            .iter()
            .enumerate()
            .map(|(i, db)| {
                RespValue::Array(vec![
                    RespValue::Integer(i as i64),
                    RespValue::Integer(db.key_count() as i64),
                    RespValue::Integer(db.stats.expiry_count as i64),
                    RespValue::Integer(db.stats.hits as i64),
                    RespValue::Integer(db.stats.misses as i64),
                ])
            })
            .collect();
        CmdResult::Ok(RespValue::Array(entries))
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

/// Encode a client-tracking invalidation push: `["invalidate", [key]]` as an
/// RESP3 push frame (`StartCollection(2, PUSH)`, dragonfly_connection.cc:792).
fn invalidation_push_frame(key: &[u8]) -> Vec<u8> {
    encode_value(&RespValue::Push(vec![
        RespValue::bulk("invalidate"),
        RespValue::Array(vec![RespValue::bulk(key)]),
    ]))
}

/// Encode the flush invalidation push `["invalidate", nil]`
/// (`invalidate_due_to_flush`, dragonfly_connection.cc:829).
fn flush_invalidation_frame() -> Vec<u8> {
    encode_value(&RespValue::Push(vec![
        RespValue::bulk("invalidate"),
        RespValue::Nil,
    ]))
}
