use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

use crate::commands::exec::server::now_ms;
use crate::commands::lua::{FUNCTION_KILLED_ERR, SandboxedInterpreter, ScriptDispatch, ScriptMgr};
use crate::commands::{Command, FLAG_GLOBAL, FLAG_NOSCRIPT, FLAG_WRITE, OpContext, ShardPart};
use crate::core::DbSlice;
use crate::core::compact::CompactString;
use crate::error::{CmdResult, RespValue};
use crate::server::journal::{self, JournalItem, JournalSlice, OP_COMMAND};
use crate::server::replication::{ChunkKind, FullSyncBus, ReplChunk};
use crate::server::{
    MAX_DB, Reply, ReplyBus, ScriptBatchEntry, ScriptRunKind, ScriptRunRequest, ScriptRunResult,
    ShardMsg, SingleOp, Tracking, WatchState, command_for, encode_result, encode_value,
    extract_keys, keys_per_shard, shard_for_key,
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
    /// Shared script cache + FUNCTION registry (`ScriptMgr`), for script lookup
    /// and the `FUNCTION KILL` flag.
    script_mgr: Arc<Mutex<ScriptMgr>>,
    /// The per-shard Lua interpreter, created here on the shard thread (mlua is
    /// `!Send`). `Option` so it can be taken out while a script runs (the
    /// dispatch context borrows the whole `Shard`).
    sandbox: Option<SandboxedInterpreter>,
    /// `FUNCTION KILL` flag shared with the IO thread; polled by the
    /// `LUA_MASKCOUNT` instruction hook and the dispatch path.
    kill: Arc<AtomicBool>,
    /// SHAs already compiled into `sandbox`, so a repeated EVAL/EVALSHA skips
    /// the recompile (like the reference's per-thread `InterpreterManager`,
    /// which compiles each script once per thread).
    defined_scripts: HashSet<String>,
    /// Libraries already loaded into `sandbox`, keyed by library name with the
    /// loaded sha and its function names (so `FUNCTION LOAD REPLACE` invalidates
    /// the cached callbacks and purges names the new version dropped).
    loaded_libs: HashMap<String, (String, Vec<String>)>,
    /// Channels to every shard (including this one), for cross-shard script
    /// hops and locks: a multi-shard script runs on the first-key shard and
    /// dispatches subcommands to peers over these channels.
    peer_txs: Vec<mpsc::Sender<ShardMsg>>,
    /// Total shard count, for `keys_per_shard` grouping.
    num_shards: usize,
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
    script_mgr: Arc<Mutex<ScriptMgr>>,
    peer_txs: Vec<mpsc::Sender<ShardMsg>>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("shard-{shard_id}"))
        .spawn(move || {
            let num_shards = peer_txs.len();
            // The Lua state is not `Send`, so it must be created here on the
            // shard thread (the only thread that ever runs scripts on it).
            let enable_redis_log = script_mgr.lock().unwrap().lua_enable_redis_log;
            let sandbox = match SandboxedInterpreter::with_redis_log(enable_redis_log) {
                Ok(s) => Some(s),
                Err(e) => {
                    eprintln!("shard-{shard_id}: failed to init Lua interpreter: {e}");
                    None
                }
            };
            let kill = script_mgr.lock().unwrap().kill_flag();
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
                script_mgr,
                sandbox,
                kill,
                defined_scripts: HashSet::new(),
                loaded_libs: HashMap::new(),
                peer_txs,
                num_shards,
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
            ShardMsg::TxUnlock { tx_id } => self.unlock_tx(tx_id),
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
                self.store_value(tx_id, &key, value, expire_at, sticky, db_idx);
                let _ = ack.send(());
            }
            ShardMsg::ScriptOp {
                tx_id,
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
                    tx_id,
                    track_keys,
                );
                let _ = result_tx.send(result);
            }
            ShardMsg::ScriptBatch {
                tx_id,
                cmds,
                result_tx,
            } => {
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
                            tx_id,
                            c.track_keys,
                        )
                    })
                    .collect();
                let _ = result_tx.send(results);
            }
            ShardMsg::RunScript { req, result_tx } => self.run_script(&req, &result_tx),
            ShardMsg::ScriptGc { ack } => {
                // `ScriptMgr::GCCmd`: a full GC over this shard's interpreter.
                // The ack may lag behind a running script: this message is only
                // dequeued once the current run returns to the message loop.
                if let Some(sandbox) = &self.sandbox {
                    let _ = sandbox.run_gc();
                }
                let _ = ack.send(());
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

    /// Release the shard lock for `tx_id`: clear `active_tx`, drain queued
    /// singles and watch queries, and drop any leftover tx context.
    fn unlock_tx(&mut self, tx_id: u64) {
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
        self.tx_ctx.remove(&tx_id);
    }

    /// A raw store/delete performed on behalf of a command or script (e.g.
    /// BITOP's destination key), recorded in the journal under `tx_id`.
    fn store_value(
        &mut self,
        tx_id: u64,
        key: &[u8],
        value: Option<crate::core::PrimeValue>,
        expire_at: Option<u64>,
        sticky: bool,
        db_idx: usize,
    ) {
        self.journal_store_value(key, value.as_ref(), expire_at, sticky, db_idx, tx_id);
        match value {
            Some(v) => {
                let db = self.ensure_db(db_idx);
                db.insert(key, v);
                match expire_at {
                    Some(at) => db.set_expiry(key, at, now_ms()),
                    None => db.clear_expiry(key),
                }
                db.set_sticky_flag(key, sticky);
            }
            None => {
                self.ensure_db(db_idx).remove(key);
            }
        }
        self.flush_invalidations();
    }

    /// Run a script (`EVAL`/`EVALSHA`/`FCALL`) on this shard's interpreter and
    /// send the outcome back on `result_tx`. The interpreter is taken out of
    /// `self` so the dispatch context can borrow the whole `Shard`.
    fn run_script(&mut self, req: &ScriptRunRequest, result_tx: &mpsc::Sender<ScriptRunResult>) {
        let Some(sandbox) = self.sandbox.take() else {
            let _ = result_tx.send(ScriptRunResult {
                result: Err("ERR internal: no script interpreter".into()),
                num_commands: 0,
                slow_commands: 0,
            });
            return;
        };
        let (result, num_commands, slow_commands) = self.run_script_inner(&sandbox, req);
        self.sandbox = Some(sandbox);
        let _ = result_tx.send(ScriptRunResult {
            result,
            num_commands,
            slow_commands,
        });
    }

    /// The body of [`run_script`], with the interpreter passed separately so
    /// the dispatch context can borrow the whole shard.
    fn run_script_inner(
        &mut self,
        sandbox: &SandboxedInterpreter,
        req: &ScriptRunRequest,
    ) -> (Result<RespValue, String>, usize, usize) {
        let params = req.params;
        let atomic = params.atomic;
        let keys = &req.keys;

        // FCALL: (re)load the function's library when the shard's cached sha
        // differs (first FCALL or after `FUNCTION LOAD REPLACE`), purging
        // callback names the new version dropped.
        if let ScriptRunKind::Function {
            lib_name,
            lib_sha,
            code,
            ..
        } = &req.kind
            && self.loaded_libs.get(lib_name).map(|(sha, _)| sha) != Some(lib_sha)
        {
            let to_purge: Vec<String> = match self.loaded_libs.get(lib_name) {
                Some((_, old_names)) => {
                    let mgr = self.script_mgr.lock().unwrap();
                    old_names
                        .iter()
                        .filter(|n| mgr.function_lib(n).is_none())
                        .cloned()
                        .collect()
                }
                None => Vec::new(),
            };
            sandbox.purge_functions(&to_purge);
            let functions = match sandbox.load_function_lib(code) {
                Ok(f) => f,
                Err(e) => return (Err(e), 0, 0),
            };
            let names: Vec<String> = functions.into_iter().map(|f| f.name).collect();
            self.loaded_libs
                .insert(lib_name.clone(), (lib_sha.clone(), names));
        }

        // EVAL: compile the script into this shard's interpreter once, like the
        // reference's per-thread `InterpreterManager` (`AddInternal`). The sha
        // is content-addressed, so a cached hit implies an identical body.
        if let ScriptRunKind::Eval { sha, body } = &req.kind
            && !self.defined_scripts.contains(sha)
        {
            if let Err(e) = sandbox.define(sha, body) {
                return (Err(e), 0, 0);
            }
            self.defined_scripts.insert(sha.clone());
        }

        // Install the KEYS/ARGV globals like the coordinator's EVAL path did
        // (`SetGlobalArrayInternal`). EVAL reads them from the environment;
        // FCALL passes the keys as callback arguments instead.
        if let ScriptRunKind::Eval { .. } = &req.kind {
            if let Err(e) = sandbox.set_global_array("KEYS", keys) {
                return (Err(e), 0, 0);
            }
            if let Err(e) = sandbox.set_global_array("ARGV", &req.argv) {
                return (Err(e), 0, 0);
            }
        }

        // `DetermineMultiMode` (main_service.cc): atomic scripts lock their
        // declared-key shards for the whole body (`LOCK_AHEAD`, or every shard
        // in GLOBAL mode when undeclared keys are allowed); `disable-atomicity`
        // scripts hold no locks up front — each subcommand locks its own shards
        // only for the call.
        let shards: Vec<usize> = if atomic {
            let key_idxs: Vec<usize> = (0..keys.len()).collect();
            let per = keys_per_shard(keys, &key_idxs, self.num_shards);
            if params.undeclared_keys {
                (0..self.num_shards).collect()
            } else {
                per.into_iter().map(|(s, _)| s).collect()
            }
        } else {
            Vec::new()
        };

        let ctx = ScriptCtx {
            declared: keys.clone(),
            undeclared_keys: params.undeclared_keys,
            read_only: req.read_only,
            num_shards: self.num_shards,
            db_idx: req.db_idx,
            conn_id: req.conn_id,
            track_keys: req.track_keys,
            atomic,
            tx_id: req.tx_id,
            locked_shards: Vec::new(),
            pinned_shards: Vec::new(),
            async_cmds: Vec::new(),
            async_bytes: 0,
            num_commands: 0,
            slow_commands: 0,
            slowlog_threshold_usec: req.slowlog_threshold_usec,
        };

        // `CallSHA` records the script's run duration in usec for SCRIPT LATENCY.
        let float_as_int =
            params.float_as_int || self.script_mgr.lock().unwrap().lua_resp2_legacy_float;
        let (result, held, stats) = {
            let kill = Arc::clone(&self.kill);
            let mut dctx = ShardScriptDispatchCtx { shard: self, ctx };
            if let Err(e) = dctx.ensure_locked(&shards) {
                let held = std::mem::take(&mut dctx.ctx.locked_shards);
                for s in held {
                    dctx.shard.unlock_script_shard(dctx.ctx.tx_id, s);
                }
                return (Err(e), 0, 0);
            }
            let run = match &req.kind {
                ScriptRunKind::Eval { sha, .. } => sandbox.run(sha, &mut dctx, float_as_int, &kill),
                ScriptRunKind::Function { name, .. } => {
                    sandbox.run_function(name, keys, &req.argv, &mut dctx, float_as_int, &kill)
                }
            };
            // Force-flush pending `redis.acall` commands; a flush error
            // overrides the script's own result (`FlushEvalAsyncCmds(true)`).
            let flushed = dctx.flush();
            let held = std::mem::take(&mut dctx.ctx.locked_shards);
            let stats = (dctx.ctx.num_commands, dctx.ctx.slow_commands);
            (
                match flushed {
                    Ok(()) => run,
                    Err(e) => Err(e),
                },
                held,
                stats,
            )
        };
        for s in &held {
            self.unlock_script_shard(req.tx_id, *s);
        }
        (result, stats.0, stats.1)
    }

    /// TxLock `shard` for a script's transaction, or take this shard's own
    /// lock. Messages are FIFO per shard, so a lock is in effect before any
    /// subsequent `script_op` to the same shard; the ack is still awaited to
    /// detect a dead shard thread.
    fn lock_script_shard(
        &mut self,
        tx_id: u64,
        shard: usize,
        conn_id: u64,
        db_idx: usize,
        track_keys: bool,
    ) -> Result<(), String> {
        if shard == self.id {
            self.active_tx = Some(tx_id);
            return Ok(());
        }
        let (ack_tx, ack_rx) = mpsc::channel();
        self.peer_txs[shard]
            .send(ShardMsg::TxLock {
                tx_id,
                conn_id,
                seq: 0,
                args: Vec::new(),
                owned_key_idxs: Vec::new(),
                first_key_idx: 0,
                db_idx,
                owns_all_keys: false,
                track_keys,
                ack: ack_tx,
            })
            .map_err(|_| "ERR internal: shard thread exited".to_string())?;
        ack_rx
            .recv()
            .map_err(|_| "ERR internal: shard thread exited".to_string())
    }

    /// TxUnlock `shard` for a script's transaction (or this shard itself).
    fn unlock_script_shard(&mut self, tx_id: u64, shard: usize) {
        if shard == self.id {
            self.unlock_tx(tx_id);
        } else {
            let _ = self.peer_txs[shard].send(ShardMsg::TxUnlock { tx_id });
        }
    }

    /// Store (or delete) a key produced by a script subcommand on its shard.
    /// The destination key is one of the subcommand's declared keys, so its
    /// shard is already locked by the script's transaction (LOCK_AHEAD holds it
    /// for the whole body; a NON_ATOMIC call holds it for the duration of the
    /// call). The write is recorded under the script's `tx_id`.
    fn perform_deferred_store(
        &mut self,
        key: &[u8],
        value: Option<crate::core::PrimeValue>,
        expire_at: Option<u64>,
        sticky: bool,
        db_idx: usize,
        tx_id: u64,
    ) {
        let shard = shard_for_key(key, self.num_shards);
        if shard == self.id {
            self.store_value(tx_id, key, value, expire_at, sticky, db_idx);
            return;
        }
        let (ack_tx, ack_rx) = mpsc::channel();
        if self.peer_txs[shard]
            .send(ShardMsg::StoreValue {
                tx_id,
                key: key.to_vec(),
                value,
                expire_at,
                sticky,
                db_idx,
                ack: ack_tx,
            })
            .is_ok()
        {
            let _ = ack_rx.recv();
        }
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

// ---------------------------------------------------------------------------
// Script execution
// ---------------------------------------------------------------------------

/// Per-run state a script's `redis.call`/`redis.pcall` subcommands need. Lives
/// on the shard owning the script's first key, which runs the body and
/// dispatches subcommands to itself inline and to peers over their channels.
struct ScriptCtx {
    /// Values of the declared KEYS (the script's lock tags).
    declared: Vec<Vec<u8>>,
    /// Whether subcommands may touch undeclared keys (allow-undeclared-keys).
    undeclared_keys: bool,
    /// `EVAL_RO` / `EVALSHA_RO` / `FCALL_RO`: reject any write subcommand.
    read_only: bool,
    num_shards: usize,
    /// The DB all subcommands run in (the connection's selected DB).
    db_idx: usize,
    /// The connection running the script, forwarded to shards so the CLIENT
    /// TRACKING read hook can attribute reads (`TrackIfNeeded`).
    conn_id: u64,
    /// Whether reads inside the script are tracked for this connection
    /// (`ShouldTrackKeys` evaluated at EVAL dispatch time).
    track_keys: bool,
    /// `ScriptParams::atomic` (i.e. `DetermineMultiMode`): atomic scripts hold
    /// their declared-key shards locked for the whole body (`LOCK_AHEAD`);
    /// `disable-atomicity` scripts lock each subcommand's shards only for the
    /// call (`NON_ATOMIC`).
    atomic: bool,
    /// `tx_id` shared by every lock the script's transaction takes.
    tx_id: u64,
    /// Shards currently TxLocked by this script (the transaction's lock set).
    locked_shards: Vec<usize>,
    /// Shards explicitly pinned via `dragonfly.lock`, surviving the per-call
    /// release in non-atomic mode.
    pinned_shards: Vec<usize>,
    /// Pending `redis.acall`/`redis.apcall` commands batched for one squashed
    /// flush (`ConnectionState::ScriptInfo::async_cmds`).
    async_cmds: Vec<AsyncCmd>,
    /// The batch's `used_mem` (`FlushEvalAsyncCmds`): every command's
    /// `BackedArguments` heap + struct size plus a `StoredCmd` per slot, the
    /// `--multi_eval_squash_buffer` budget.
    async_bytes: usize,
    /// `ConnectionState::ScriptInfo::stats.num_commands`: every subcommand the
    /// script invoked, including squashed ones (main_service.cc:2109).
    num_commands: usize,
    /// `stats.slow_commands`: subcommands whose latency met the slowlog
    /// threshold (conn_context.cc:351).
    slow_commands: usize,
    /// The slowlog threshold (usec) for this run (`log_slower_than_usec`).
    slowlog_threshold_usec: u64,
}

/// A single pending async subcommand.
struct AsyncCmd {
    args: Vec<Vec<u8>>,
    /// `acall` (true): the command's runtime error aborts the run; `apcall`
    /// (false) suppresses per-command errors (`ReplyMode::ONLY_ERR` vs `NONE`).
    abort_on_error: bool,
}

/// `--multi_eval_squash_buffer`: max bytes of queued async commands before the
/// batch is flushed mid-script (`FLAGS_multi_eval_squash_buffer`, default 8096).
const ASYNC_FLUSH_LIMIT: usize = 8096;

/// Commands executed per squashed hop (`max_squash_cmd_num`, default 32): when
/// a shard's accumulated batch reaches this many commands the accumulated hop
/// runs (`SquashResult::SQUASHED_FULL`).
const MAX_SQUASH_SIZE: usize = 32;

/// `sizeof(cmn::BackedArguments)` in the reference: two `absl::InlinedVector`s
/// (a `uint32_t` offsets array with inline capacity 5 and a `char` storage
/// array with inline capacity 128) plus their heap pointers/sizes. Included in
/// `StoredCmd::UsedMemory`.
const BACKED_ARGS_STRUCT_SIZE: usize = 200;

/// `sizeof(StoredCmd)` in the reference: command id pointer + `ParsedArgs`
/// variant + `BackedArguments` unique_ptr + reply mode.
const STORED_CMD_SIZE: usize = 48;

/// `cmn::BackedArguments::kLenCap`: arguments stored inline before any heap
/// allocation.
const BACKED_ARGS_INLINE_ARGS: usize = 5;

/// `cmn::BackedArguments::kStorageCap`: total argument bytes stored inline
/// before any heap allocation.
const BACKED_ARGS_INLINE_BYTES: usize = 128;

/// Heap bytes `cmn::BackedArguments::HeapMemory` attributes to a command's
/// tail arguments (excluding the command name, which `FindExtended` strips).
/// Arguments that fit the inline buffer cost nothing; otherwise the cost is the
/// offset array's `capacity * sizeof(uint32_t)` plus the storage capacity.
fn backed_args_heap_cost(tail_args: &[Vec<u8>]) -> usize {
    let bytes: usize = tail_args.iter().map(Vec::len).sum();
    if tail_args.len() <= BACKED_ARGS_INLINE_ARGS && bytes <= BACKED_ARGS_INLINE_BYTES {
        0
    } else {
        // `capacity` is approximated by the current size, as the reference
        // charges the actual (possibly larger) capacity.
        tail_args.len() * 4 + bytes
    }
}

/// Routes a script subcommand to the shards owning its keys while the script's
/// tx holds the declared-key locks. Runs on the shard owning the script's
/// first key: subcommands on this shard execute inline via `run_exec`, the
/// rest hop to peers over their channels.
struct ShardScriptDispatchCtx<'a> {
    shard: &'a mut Shard,
    ctx: ScriptCtx,
}

impl ScriptDispatch for ShardScriptDispatchCtx<'_> {
    fn dispatch(&mut self, args: Vec<Vec<u8>>) -> Result<RespValue, String> {
        // `CallFromScript` (main_service.cc:2109) counts every invocation.
        self.ctx.num_commands += 1;
        // `FUNCTION KILL` from the IO thread: abort at the next dispatch
        // boundary (mirrors the count hook, which cannot fire while a
        // subcommand is dispatched from Rust).
        if self.shard.kill.load(Ordering::Relaxed) {
            return Err(FUNCTION_KILLED_ERR.to_string());
        }
        // A synchronous call flushes the pending async batch first; a flush
        // error aborts this call too (`requested_abort` in TryEnqueueEvalAsyncCmd).
        self.flush_pending(true)?;
        let cmd = self.verify_script_cmd(&args)?;
        // A NON_ATOMIC script (`disable-atomicity`) locks the subcommand's
        // shards only for the call (`CallFromScript` -> `DispatchCommand` ->
        // `ScheduleSingleHop`), releasing them again afterwards; atomic scripts
        // already hold the declared-key shards for the whole body.
        let started = Instant::now();
        // FLAG_LOCAL subcommands (PING, ECHO, TIME, SELECT, ...) run on the
        // connection thread in the reference too (`GenericFamily::Ping`,
        // `GenericFamily::Select`), so execute them inline instead of routing
        // them to a shard's `local_stub`.
        if let Some(r) = self.script_local(cmd, &args) {
            self.count_slow(started);
            // An error reply must abort the script (`redis.call` raises on an
            // error from `DispatchCommand`, lua_libs.cc `RedisCallCommand`).
            return match r {
                CmdResult::Err(e) => Err(e.message),
                other => Ok(other.into_resp_value()),
            };
        }
        if !self.ctx.atomic {
            let shards = self.cmd_shards(cmd, &args);
            self.ensure_locked(&shards)?;
            let r = self.execute_script_cmd(cmd, &args);
            self.release_unpinned();
            self.count_slow(started);
            return Ok(r.into_resp_value());
        }
        let r = self.execute_script_cmd(cmd, &args);
        self.count_slow(started);
        Ok(r.into_resp_value())
    }

    fn lock(&mut self, keys: Vec<Vec<u8>>) -> Result<(), String> {
        self.ctx.num_commands += 1;
        // `CallFromScript` LOCK: an atomic transaction is already locked ahead,
        // so the call is a no-op.
        if self.ctx.atomic {
            return Ok(());
        }
        // The keys are already stringified (`key_backing`); lock their shards
        // and pin them so the per-call release keeps them held
        // (`StartMultiLockedAhead`).
        let key_idxs: Vec<usize> = (0..keys.len()).collect();
        let mut shards: Vec<usize> = Vec::new();
        for (s, _) in keys_per_shard(&keys, &key_idxs, self.ctx.num_shards) {
            if !shards.contains(&s) {
                shards.push(s);
            }
        }
        self.ensure_locked(&shards)?;
        for &s in &shards {
            if !self.ctx.pinned_shards.contains(&s) {
                self.ctx.pinned_shards.push(s);
            }
        }
        Ok(())
    }

    fn unlock(&mut self) -> Result<(), String> {
        self.ctx.num_commands += 1;
        // `CallFromScript` UNLOCK: flush the pending async batch, release every
        // lock the transaction holds and continue non-atomically
        // (`UnlockMulti(true)` + `StartMultiNonAtomic`).
        self.flush_pending(true)?;
        self.release_all();
        self.ctx.pinned_shards.clear();
        self.ctx.atomic = false;
        Ok(())
    }

    fn dispatch_async(&mut self, args: Vec<Vec<u8>>, abort_on_error: bool) -> Result<(), String> {
        self.ctx.num_commands += 1;
        if command_for(&args).is_none() {
            if abort_on_error {
                // acall: an unknown command is fatal (`early_async_error` =
                // `ReportUnknownCmd`, which uppercases and uses backticks). The
                // pending batch is still flushed first, so its errors win.
                self.flush_pending(true)?;
                return Err(format!(
                    "unknown command `{}`",
                    String::from_utf8_lossy(&args[0]).to_ascii_uppercase()
                ));
            }
            // apcall: drop the unknown command silently, but keep the budget.
            return self.flush_pending(false);
        }
        // Full verification (NOSCRIPT, write-in-read-only, undeclared keys) is
        // deferred to the flush, mirroring `VerifyCommandState` in
        // `FlushEvalAsyncCmds` — so `pcall(redis.acall, ...)` cannot catch an
        // error that only surfaces at the flush boundary.
        // The byte cost mirrors the reference's `used_mem`: each command's
        // `BackedArguments` heap + struct size, plus a `StoredCmd` per slot.
        let tail: Vec<Vec<u8>> = args[1..].to_vec();
        self.ctx.async_bytes +=
            backed_args_heap_cost(&tail) + BACKED_ARGS_STRUCT_SIZE + STORED_CMD_SIZE;
        self.ctx.async_cmds.push(AsyncCmd {
            args,
            abort_on_error,
        });
        self.flush_pending(false)
    }

    fn flush(&mut self) -> Result<(), String> {
        self.flush_pending(true)
    }
}

impl ShardScriptDispatchCtx<'_> {
    /// The shards owning a subcommand's keys (`keys_per_shard`), deduplicated.
    fn cmd_shards(&self, cmd: &'static Command, args: &[Vec<u8>]) -> Vec<usize> {
        let keys = extract_keys(cmd, args);
        let mut out = Vec::new();
        for (s, _) in keys_per_shard(args, &keys, self.ctx.num_shards) {
            if !out.contains(&s) {
                out.push(s);
            }
        }
        out
    }

    /// TxLock the shards the script's transaction does not already hold,
    /// recording them in `locked_shards`. Messages are FIFO per shard, so a
    /// lock is in effect before any subsequent `script_op` to the same shard
    /// without waiting for acks (`MultiSwitchCmd(EVAL)` +
    /// `StartMultiLockedAhead` / a squashed hop's shard scheduling).
    fn ensure_locked(&mut self, shards: &[usize]) -> Result<(), String> {
        for &s in shards {
            if self.ctx.locked_shards.contains(&s) {
                continue;
            }
            self.shard.lock_script_shard(
                self.ctx.tx_id,
                s,
                self.ctx.conn_id,
                self.ctx.db_idx,
                self.ctx.track_keys,
            )?;
            self.ctx.locked_shards.push(s);
        }
        Ok(())
    }

    /// Release the locks that were taken per-call (not pinned by
    /// `dragonfly.lock`) — the NON_ATOMIC mode's per-subcommand scheduling.
    fn release_unpinned(&mut self) {
        let mut still = Vec::new();
        for s in std::mem::take(&mut self.ctx.locked_shards) {
            if self.ctx.pinned_shards.contains(&s) {
                still.push(s);
            } else {
                self.shard.unlock_script_shard(self.ctx.tx_id, s);
            }
        }
        self.ctx.locked_shards = still;
    }

    /// Release every lock the script's transaction holds (`UnlockMulti(true)`).
    fn release_all(&mut self) {
        for s in std::mem::take(&mut self.ctx.locked_shards) {
            self.shard.unlock_script_shard(self.ctx.tx_id, s);
        }
    }

    /// Execute the pending async batch as a squashed phase (`FlushEvalAsyncCmds`
    /// + `MultiCommandSquasher::Execute` with `error_abort=true`).
    ///
    /// `force=false` only flushes when the byte budget is exhausted. Every
    /// command is verified before any runs. Commands accumulate per shard and
    /// run in one hop per shard (`ShardMsg::ScriptBatch`, dispatched in
    /// parallel); when a shard's batch reaches `max_squash_size` the
    /// accumulated hop runs; keyless and multi-shard commands run standalone
    /// (flushing the accumulated hop first). An `acall` error in a hop aborts
    /// the remaining batch (`error_abort`); standalone errors surface at the
    /// end of the flush. `apcall` errors are suppressed (`ReplyMode::NONE`) and
    /// the batch continues.
    fn flush_pending(&mut self, force: bool) -> Result<(), String> {
        if self.ctx.async_cmds.is_empty() {
            return Ok(());
        }
        if !force && self.ctx.async_bytes < ASYNC_FLUSH_LIMIT {
            return Ok(());
        }
        if self.shard.kill.load(Ordering::Relaxed) {
            return Err(FUNCTION_KILLED_ERR.to_string());
        }
        for cmd in &self.ctx.async_cmds {
            if let Err(e) = self.verify_script_cmd(&cmd.args) {
                // The reference clears the batch on a verification error.
                self.ctx.async_cmds.clear();
                self.ctx.async_bytes = 0;
                return Err(e);
            }
        }
        let cmds = std::mem::take(&mut self.ctx.async_cmds);
        self.ctx.async_bytes = 0;
        let num_cmds = cmds.len();
        let flush_started = Instant::now();

        if !self.ctx.atomic {
            // A NON_ATOMIC batch locks every shard it touches for the duration
            // of the flush and releases them again afterwards (a squashed hop's
            // shard scheduling).
            let mut shards: Vec<usize> = Vec::new();
            for cmd in &cmds {
                let exec = command_for(&cmd.args).expect("verified async command");
                for (s, _) in keys_per_shard(
                    &cmd.args,
                    &extract_keys(exec, &cmd.args),
                    self.ctx.num_shards,
                ) {
                    if !shards.contains(&s) {
                        shards.push(s);
                    }
                }
            }
            self.ensure_locked(&shards)?;
        }

        let mut batches: Vec<Vec<BatchEntry>> =
            (0..self.ctx.num_shards).map(|_| Vec::new()).collect();
        // Call positions accumulated in the current hop (for the in-order error
        // scan), and the abort flag of every command by call position.
        let mut hop_positions: Vec<usize> = Vec::new();
        let mut abort_flags: Vec<bool> = Vec::with_capacity(cmds.len());
        let mut results: Vec<Option<CmdResult>> = vec![None; cmds.len()];
        let mut fatal: Option<String> = None;

        for (pos, cmd) in cmds.into_iter().enumerate() {
            abort_flags.push(cmd.abort_on_error);
            let exec = command_for(&cmd.args).expect("verified async command");
            let keys = extract_keys(exec, &cmd.args);
            let per = keys_per_shard(&cmd.args, &keys, self.ctx.num_shards);
            if per.len() == 1 {
                let shard = per[0].0;
                batches[shard].push(BatchEntry {
                    pos,
                    args: cmd.args,
                    owned: per[0].1.clone(),
                    first_key_idx: exec.key_range.first,
                    owns_all_keys: true,
                });
                hop_positions.push(pos);
                if batches[shard].len() >= MAX_SQUASH_SIZE {
                    fatal = run_squash_hop(
                        self.shard,
                        self.ctx.tx_id,
                        self.ctx.db_idx,
                        self.ctx.conn_id,
                        self.ctx.track_keys,
                        &mut batches,
                        &mut results,
                        &abort_flags,
                        &mut hop_positions,
                    );
                    if fatal.is_some() {
                        break;
                    }
                }
            } else {
                // A keyless or multi-shard command cannot be squashed
                // (`keys->NumArgs() == 0` or keys on ≥2 shards in `TrySquash`):
                // flush the accumulated hop first, then run it standalone
                // (`ExecuteStandalone`). Standalone runtime errors do not abort
                // the remaining batch; they surface at the end.
                fatal = run_squash_hop(
                    self.shard,
                    self.ctx.tx_id,
                    self.ctx.db_idx,
                    self.ctx.conn_id,
                    self.ctx.track_keys,
                    &mut batches,
                    &mut results,
                    &abort_flags,
                    &mut hop_positions,
                );
                if fatal.is_some() {
                    break;
                }
                let result = self
                    .script_local(exec, &cmd.args)
                    .unwrap_or_else(|| self.execute_script_cmd(exec, &cmd.args));
                results[pos] = Some(result);
            }
        }
        if fatal.is_none() {
            fatal = run_squash_hop(
                self.shard,
                self.ctx.tx_id,
                self.ctx.db_idx,
                self.ctx.conn_id,
                self.ctx.track_keys,
                &mut batches,
                &mut results,
                &abort_flags,
                &mut hop_positions,
            );
        }

        // The first `acall` error in call order surfaces as the flush error;
        // `apcall` errors are invisible (`ReplyMode::NONE`).
        if fatal.is_none() {
            for (pos, slot) in results.iter().enumerate() {
                if abort_flags[pos]
                    && let Some(CmdResult::Err(e)) = slot
                {
                    fatal = Some(e.message.clone());
                    break;
                }
            }
        }
        if !self.ctx.atomic {
            self.release_unpinned();
        }
        // `RecordLatency` (conn_context.cc:351) counts a script's slow
        // subcommands: a squashed batch met the threshold as a whole, so every
        // command in it counts.
        let elapsed_usec = flush_started.elapsed().as_micros() as u64;
        if elapsed_usec >= self.ctx.slowlog_threshold_usec {
            self.ctx.slow_commands += num_cmds;
        }
        match fatal {
            Some(msg) => Err(msg),
            None => Ok(()),
        }
    }

    /// `RecordLatency`: a subcommand that met the slowlog threshold counts
    /// toward the script's `stats.slow_commands`.
    fn count_slow(&mut self, started: Instant) {
        let elapsed_usec = started.elapsed().as_micros() as u64;
        if elapsed_usec >= self.ctx.slowlog_threshold_usec {
            self.ctx.slow_commands += 1;
        }
    }

    /// Validate a script subcommand before it touches a shard: known command,
    /// arity, not NOSCRIPT/GLOBAL, no writes in read-only scripts, declared
    /// keys.
    fn verify_script_cmd(&self, args: &[Vec<u8>]) -> Result<&'static Command, String> {
        let cmd = command_for(args).ok_or_else(|| {
            // `ReportUnknownCmd` uppercases the name and wraps it in backticks
            // (`unknown command \`FOO\``); the inline "ERR " keeps the port's
            // error-table rendering byte-identical to the reference's.
            format!(
                "ERR unknown command `{}`",
                String::from_utf8_lossy(&args[0]).to_ascii_uppercase()
            )
        })?;
        // `DispatchCommand` (main_service.cc): GLOBAL_TRANS / NO_KEY_TRANSACTIONAL
        // commands may run only when the script schedules globally or re-schedules
        // per operation (GLOBAL / NON_ATOMIC); NOSCRIPT commands never run.
        // `XGROUP HELP` resolves to the hidden `_XGROUP_HELP` command which is
        // NOSCRIPT (command_registry.cc:347-352, issue #854), so scripts must be
        // rejected even though the top-level XGROUP is not flagged — and before
        // the arity check, since the rewritten command's arity is 2.
        if args.len() == 2 && cmd.name == "XGROUP" && args[1].eq_ignore_ascii_case(b"HELP") {
            return Err("This Redis command is not allowed from script".to_string());
        }
        // The reference's `DispatchCommand` arity check runs for script
        // subcommands too; without it a keyless subcommand with too few args
        // would reach the executor and panic (`GET` -> `owned_keys[0]`).
        if let Some(e) = cmd.check_arity(args.len()) {
            return Err(e);
        }
        if cmd.has_flag(FLAG_NOSCRIPT)
            || (cmd.has_flag(FLAG_GLOBAL)
                // MEMORY is READONLY|FAST in the reference (NOSCRIPT was
                // dropped in #2382), so it runs from any script; the port flags
                // it GLOBAL only so top-level `MEMORY USAGE` reaches every
                // shard (`MemoryCmd::Usage` picks the key's shard internally).
                && cmd.name != "MEMORY"
                && self.ctx.atomic
                && !self.ctx.undeclared_keys)
        {
            return Err("This Redis command is not allowed from script".to_string());
        }
        if self.ctx.read_only && cmd.has_flag(FLAG_WRITE) {
            return Err("Write commands are not allowed from read-only scripts".to_string());
        }
        // `CheckKeysDeclared` runs only in LOCK_AHEAD mode (atomic without the
        // allow-undeclared-keys flag): GLOBAL and NON_ATOMIC scripts schedule
        // per operation, so undeclared keys are unrestricted.
        let keys = extract_keys(cmd, args);
        if self.ctx.atomic && !self.ctx.undeclared_keys {
            for &ki in &keys {
                if !self.ctx.declared.contains(&args[ki]) {
                    return Err(format!(
                        "script tried accessing undeclared key, key: {}",
                        String::from_utf8_lossy(&args[ki])
                    ));
                }
            }
        }
        Ok(cmd)
    }

    /// Run a FLAG_LOCAL subcommand on the run-shard. The reference executes
    /// these on the connection thread (`GenericFamily::Ping`,
    /// `GenericFamily::Echo`, `GenericFamily::Time`), so routing them to a
    /// shard would hit `local_stub`'s "local command should not reach a shard"
    /// error. Returns `None` for commands that must reach a shard.
    fn script_local(&mut self, cmd: &'static Command, args: &[Vec<u8>]) -> Option<CmdResult> {
        let resp = match cmd.name {
            "PING" => crate::commands::exec::server::local_ping(args),
            "ECHO" => crate::commands::exec::server::local_echo(args),
            "TIME" => crate::commands::exec::server::local_time(args),
            "LASTSAVE" => crate::commands::exec::server::local_lastsave(args),
            "SELECT" => return Some(self.script_select(args)),
            _ => return None,
        };
        Some(CmdResult::Ok(resp))
    }

    /// `GenericFamily::Select` (generic_family.cc:2439): parse and range-check
    /// the target DB, accept a same-DB select as a noop, reject it for
    /// LOCK_AHEAD (regular) scripts, and switch the script's DB for GLOBAL /
    /// NON_ATOMIC scripts so subsequent subcommands target the new DB.
    fn script_select(&mut self, args: &[Vec<u8>]) -> CmdResult {
        let Some(index) = args.get(1).and_then(|a| crate::util::parse_i64(a)) else {
            return CmdResult::err("ERR DB index is out of range");
        };
        if index < 0 || index as usize >= crate::server::MAX_DB {
            return CmdResult::err("ERR DB index is out of range");
        }
        if self.ctx.db_idx == index as usize {
            return CmdResult::ok(RespValue::Simple("OK".into()));
        }
        if self.ctx.atomic && !self.ctx.undeclared_keys {
            return CmdResult::err("SELECT is not allowed in regular EXEC/EVAL");
        }
        self.ctx.db_idx = index as usize;
        CmdResult::ok(RespValue::Simple("OK".into()))
    }

    /// Run one (already verified) subcommand: inline on the run-shard, or via
    /// `ShardMsg::ScriptOp` to peers, merging partial results and resolving
    /// deferred stores.
    fn execute_script_cmd(&mut self, cmd: &'static Command, args: &[Vec<u8>]) -> CmdResult {
        let keys = extract_keys(cmd, args);
        let first_key_idx = cmd.key_range.first;
        let mut parts = Vec::new();
        if keys.is_empty() {
            // A GLOBAL subcommand (MEMORY, DBSIZE, FLUSHALL, ...) runs on every
            // shard like the top-level GLOBAL dispatch (event_loop.rs), so the
            // merge can pick the shard that owns a `MEMORY USAGE` key; other
            // keyless commands validate/reply from shard 0.
            let shards: Vec<usize> = if cmd.has_flag(FLAG_GLOBAL) {
                (0..self.ctx.num_shards).collect()
            } else {
                vec![0]
            };
            for s in shards {
                let result = self.run_script_op(s, args.to_owned(), vec![], first_key_idx, true);
                parts.push(ShardPart {
                    shard: s,
                    owned_key_idxs: vec![],
                    result,
                });
            }
        } else {
            for (s, owned) in keys_per_shard(args, &keys, self.ctx.num_shards) {
                // A subcommand whose keys all live on one shard journals the
                // full tail; split keys journal reduced args per shard.
                let owns_all_keys = owned.len() == keys.len();
                let result = self.run_script_op(
                    s,
                    args.to_owned(),
                    owned.clone(),
                    first_key_idx,
                    owns_all_keys,
                );
                parts.push(ShardPart {
                    shard: s,
                    owned_key_idxs: owned,
                    result,
                });
            }
        }
        let merged = if parts.len() > 1 {
            match cmd.merge {
                Some(m) => m(&parts, args, &keys, now_ms()),
                None => parts[0].result.clone(),
            }
        } else {
            parts[0].result.clone()
        };
        match merged {
            CmdResult::DeferredStore { key, value, reply } => {
                self.shard.perform_deferred_store(
                    &key,
                    value,
                    None,
                    false,
                    self.ctx.db_idx,
                    self.ctx.tx_id,
                );
                CmdResult::Ok(reply)
            }
            CmdResult::DeferredStores { stores, reply } => {
                for (key, value, expire_at, sticky) in stores {
                    self.shard.perform_deferred_store(
                        &key,
                        value,
                        expire_at,
                        sticky,
                        self.ctx.db_idx,
                        self.ctx.tx_id,
                    );
                }
                CmdResult::Ok(reply)
            }
            CmdResult::Blocked => CmdResult::Ok(RespValue::Nil),
            other => other,
        }
    }

    /// Execute a subcommand on `shard`: inline on the run-shard, or via
    /// `ShardMsg::ScriptOp` to a peer. The target shard is locked by the
    /// script's transaction, so the subcommand executes immediately.
    fn run_script_op(
        &mut self,
        shard: usize,
        args: Vec<Vec<u8>>,
        owned: Vec<usize>,
        first_key_idx: usize,
        owns_all_keys: bool,
    ) -> CmdResult {
        if shard == self.shard.id {
            return self.shard.run_exec(
                &args,
                &owned,
                first_key_idx,
                self.ctx.db_idx,
                owns_all_keys,
                self.ctx.conn_id,
                self.ctx.tx_id,
                self.ctx.track_keys,
            );
        }
        let (result_tx, result_rx) = mpsc::channel();
        if self.shard.peer_txs[shard]
            .send(ShardMsg::ScriptOp {
                tx_id: self.ctx.tx_id,
                args,
                owned_key_idxs: owned,
                first_key_idx,
                db_idx: self.ctx.db_idx,
                owns_all_keys,
                conn_id: self.ctx.conn_id,
                track_keys: self.ctx.track_keys,
                result_tx,
            })
            .is_err()
        {
            return CmdResult::err("ERR internal: shard thread exited");
        }
        result_rx
            .recv()
            .unwrap_or_else(|_| CmdResult::err("ERR internal: shard thread exited"))
    }
}

/// One squashed-batch entry destined for a specific shard, keeping its call
/// position so results reassemble in script order.
struct BatchEntry {
    pos: usize,
    args: Vec<Vec<u8>>,
    owned: Vec<usize>,
    first_key_idx: usize,
    owns_all_keys: bool,
}

/// Run one squashed hop: every shard with queued entries executes them in a
/// single message (`ShardMsg::ScriptBatch`, dispatched in parallel). Results
/// are reassembled by call position; the first `acall` error in call order is
/// returned (`error_abort` in `ExecuteSquashed`). The whole hop runs even when
/// it contains an error, mirroring the reference's shard-side execution.
/// Clears the batches and the hop position list.
#[allow(clippy::too_many_arguments)]
fn run_squash_hop(
    shard: &mut Shard,
    tx_id: u64,
    db_idx: usize,
    conn_id: u64,
    track_keys: bool,
    batches: &mut [Vec<BatchEntry>],
    results: &mut [Option<CmdResult>],
    abort_flags: &[bool],
    hop_positions: &mut Vec<usize>,
) -> Option<String> {
    let mut hops: Vec<(usize, mpsc::Receiver<Vec<CmdResult>>)> = Vec::new();
    for (s, entries) in batches.iter().enumerate() {
        if entries.is_empty() {
            continue;
        }
        if s == shard.id {
            // The run-shard's own batch executes inline below.
            continue;
        }
        let (result_tx, result_rx) = mpsc::channel();
        if shard.peer_txs[s]
            .send(ShardMsg::ScriptBatch {
                tx_id,
                cmds: entries
                    .iter()
                    .map(|e| ScriptBatchEntry {
                        args: e.args.clone(),
                        owned_key_idxs: e.owned.clone(),
                        first_key_idx: e.first_key_idx,
                        db_idx,
                        owns_all_keys: e.owns_all_keys,
                        conn_id,
                        track_keys,
                    })
                    .collect(),
                result_tx,
            })
            .is_err()
        {
            return Some("ERR internal: shard thread exited".into());
        }
        hops.push((s, result_rx));
    }
    // Run the run-shard's own batch inline while the peers process theirs.
    if let Some(entries) = batches.get(shard.id) {
        for entry in entries {
            let result = shard.run_exec(
                &entry.args,
                &entry.owned,
                entry.first_key_idx,
                db_idx,
                entry.owns_all_keys,
                conn_id,
                tx_id,
                track_keys,
            );
            results[entry.pos] = Some(result);
        }
    }
    for (s, rx) in hops {
        match rx.recv() {
            Ok(per_shard) => {
                for (entry, result) in batches[s].iter().zip(per_shard) {
                    results[entry.pos] = Some(result);
                }
            }
            Err(_) => {
                for entry in &batches[s] {
                    results[entry.pos] = Some(CmdResult::err("ERR internal: shard thread exited"));
                }
            }
        }
    }
    batches.iter_mut().for_each(Vec::clear);
    let positions = std::mem::take(hop_positions);
    for pos in positions {
        if abort_flags[pos]
            && let Some(CmdResult::Err(e)) = &results[pos]
        {
            return Some(e.message.clone());
        }
    }
    None
}
