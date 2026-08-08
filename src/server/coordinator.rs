use std::collections::VecDeque;
use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::commands::exec::server::now_ms;
use crate::commands::lua::{ScriptMgr, sha1_hex};
use crate::commands::{FLAG_GLOBAL, ShardPart};
use crate::error::{CmdResult, RespError, RespValue};
use crate::server::{
    CoordMsg, GcRequest, Reply, ReplyBus, ScriptRunKind, ScriptRunRequest, ScriptRunResult,
    ShardMsg, TxMultiMode, blocking_timeout_is_nil_array, blocking_timeout_ms, command_for,
    encode_result, encode_value, extract_keys, is_eval_cmd, is_function_cmd, keys_per_shard,
    shard_for_key,
};

/// A blocking command (XREAD/XREADGROUP) waiting for data or a timeout. The
/// coordinator re-runs it until it returns data or the deadline passes. The
/// deadline uses the real wall clock (`Instant`): blocking waits observe real
/// time, independent of the test fake clock that drives TTL expiry.
struct PendingTx {
    msg: CoordMsg,
    deadline: Option<Instant>,
}

/// Whether a re-ran blocked command found its key holding the wrong type, in
/// which case it remains blocked rather than erroring (`WrongTypeDoesNotWake`).
/// XREADGROUP is the exception: a retyped stream wakes it with WRONGTYPE
/// (`XReadGroupBlockWakeOnRetypedStream`).
fn is_blocked_wrong_type(msg: &CoordMsg, err: &RespError) -> bool {
    let Some(cmd) = command_for(&msg.args) else {
        return false;
    };
    if cmd.name == "XREADGROUP" {
        return false;
    }
    blocking_timeout_ms(cmd, &msg.args).is_some() && err.message.starts_with("WRONGTYPE")
}

fn is_xreadgroup(msg: &CoordMsg) -> bool {
    command_for(&msg.args).is_some_and(|c| c.name == "XREADGROUP")
}

/// Reply to a blocked XREADGROUP that woke: the wake only serves the woken
/// key (`SendStreamRecords` on `wake_key` alone), so sibling streams that
/// still hold no entries are dropped from the reply. Each item is one
/// `[key, [entries]]` pair.
fn wake_xreadgroup_reply(items: Vec<RespValue>) -> Vec<RespValue> {
    items
        .into_iter()
        .filter(|item| {
            matches!(
                item,
                RespValue::Array(pair)
                    if matches!(pair.get(1), Some(RespValue::Array(v)) if !v.is_empty())
            )
        })
        .collect()
}

pub fn spawn(
    num_shards: usize,
    rx: mpsc::Receiver<CoordMsg>,
    gc_rx: mpsc::Receiver<GcRequest>,
    shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    reply_bus: ReplyBus,
    script_mgr: Arc<Mutex<ScriptMgr>>,
    command_stats: Arc<Mutex<crate::commands::exec::server::CommandStatsMap>>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("coordinator".into())
        .spawn(move || {
            // INFO COMMANDSTATS is rendered in `merge_info` on this thread; the
            // shared map is installed here so the static merge fn can reach it.
            crate::commands::exec::server::set_current_command_stats(command_stats);
            Coordinator {
                num_shards,
                rx,
                gc_rx,
                shard_txs,
                reply_bus,
                script_mgr,
                tx_counter: 0,
                pending: VecDeque::new(),
                script_slowlog_args: None,
            }
            .run();
        })
        .expect("failed to spawn coordinator thread")
}

struct Coordinator {
    num_shards: usize,
    rx: mpsc::Receiver<CoordMsg>,
    gc_rx: mpsc::Receiver<GcRequest>,
    shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    reply_bus: ReplyBus,
    script_mgr: Arc<Mutex<ScriptMgr>>,
    tx_counter: u64,
    pending: VecDeque<PendingTx>,
    /// Augmented slowlog arguments for the last EVAL run
    /// (`FormatEvalSlowlog`). The IO thread attaches them to the reply.
    script_slowlog_args: Option<Vec<Vec<u8>>>,
}

impl Coordinator {
    fn run(&mut self) {
        const POLL: Duration = Duration::from_millis(20);
        loop {
            match self.rx.recv_timeout(POLL) {
                Ok(msg) => {
                    self.drain_gc_requests();
                    self.handle(msg);
                }
                Err(RecvTimeoutError::Timeout) => {
                    self.drain_gc_requests();
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
            self.retry_pending(Instant::now());
        }
    }

    /// `SCRIPT GC`: run a full GC over every interpreter — one per shard now —
    /// and ack when all have collected (`ScriptMgr::GCCmd`, which does the same
    /// on every interpreter across all threads). Requests are drained before
    /// pending commands so a GC never waits behind a queued blocking command.
    fn drain_gc_requests(&mut self) {
        while let Ok(req) = self.gc_rx.try_recv() {
            let mut acks = Vec::new();
            for s in &self.shard_txs {
                let (ack_tx, ack_rx) = mpsc::channel();
                if s.send(ShardMsg::ScriptGc { ack: ack_tx }).is_ok() {
                    acks.push(ack_rx);
                }
            }
            for rx in acks {
                let _ = rx.recv();
            }
            let _ = req.ack.send(());
        }
    }

    fn handle(&mut self, msg: CoordMsg) {
        // A GLOBAL-mode EXEC arrives as one batch; run it synchronously so no
        // message (a woken blocked command, another connection's command)
        // interleaves between the queued commands.
        if msg.multi_queue.is_some() {
            self.execute_multi_batch(&msg);
            return;
        }
        // The EVAL family runs scripts in the coordinator's Lua state; it never
        // goes through the shard-based `execute_tx`.
        if let Some(cmd) = command_for(&msg.args)
            && is_eval_cmd(cmd.name)
        {
            let is_evalsha = matches!(cmd.name, "EVALSHA" | "EVALSHA_RO");
            let read_only = cmd.name.ends_with("_RO");
            let result = self.execute_script(&msg, is_evalsha, read_only);
            let slowlog_args = self.script_slowlog_args.take();
            self.reply_with_slowlog(msg.conn_id, msg.seq, result, slowlog_args);
            return;
        }
        // The FCALL family runs registered functions the same way.
        if let Some(cmd) = command_for(&msg.args)
            && is_function_cmd(cmd.name)
        {
            let read_only = cmd.name.ends_with("_RO");
            let result = self.execute_function(&msg, read_only);
            let slowlog_args = self.script_slowlog_args.take();
            self.reply_with_slowlog(msg.conn_id, msg.seq, result, slowlog_args);
            return;
        }
        match self.execute_tx(&msg) {
            CmdResult::Blocked => {
                if msg.no_block {
                    // Inside MULTI a blocking command never waits: it returns
                    // nil immediately (mirrors `RunCbOnFirstNonEmptyBlocking`'s
                    // `IsMulti` -> TIMED_OUT path). List/blocks and XREAD send
                    // a null *array*, the rest a null bulk.
                    let bytes = if command_for(&msg.args).is_some_and(blocking_timeout_is_nil_array)
                    {
                        crate::protocol::resp::encode_nil_array()
                    } else {
                        encode_result(CmdResult::Ok(RespValue::Nil))
                    };
                    self.reply(msg.conn_id, msg.seq, bytes);
                    return;
                }
                let cmd = command_for(&msg.args);
                let deadline = cmd.and_then(|c| blocking_timeout_ms(c, &msg.args)).map_or(
                    Some(Instant::now()),
                    |ms| {
                        if ms == 0 {
                            None // wait forever
                        } else {
                            Some(Instant::now() + Duration::from_millis(ms))
                        }
                    },
                );
                self.pending.push_back(PendingTx { msg, deadline });
            }
            other => self.reply_result(msg.conn_id, msg.seq, other),
        }
    }

    /// Execute a GLOBAL-mode MULTI transaction: run every queued command
    /// synchronously within this `handle` call. The single-threaded coordinator
    /// never returns to its message loop mid-batch, so a woken blocked command
    /// or any other connection's transaction cannot run between the queued
    /// commands — the all-shards atomicity a GLOBAL transaction provides in the
    /// reference (`TxMultiMode::Global`, where the shards stay locked for the
    /// whole EXEC).
    fn execute_multi_batch(&mut self, msg: &CoordMsg) {
        let Some(queue) = &msg.multi_queue else {
            return;
        };
        for (seq, args) in queue {
            let sub = CoordMsg {
                conn_id: msg.conn_id,
                seq: *seq,
                args: args.clone(),
                keys: Vec::new(),
                shards: Vec::new(),
                first_key_idx: 0,
                db_idx: msg.db_idx,
                no_block: msg.no_block,
                multi_mode: msg.multi_mode,
                track_keys: msg.track_keys,
                slowlog_threshold_usec: msg.slowlog_threshold_usec,
                multi_queue: None,
            };
            self.dispatch_batch_one(sub);
        }
    }

    /// Run one queued command of a GLOBAL EXEC batch. EVAL/FCALL run on the
    /// coordinator like outside EXEC; everything else dispatches through
    /// `execute_tx`, deriving its shards the way the IO thread's
    /// `dispatch_keyed` does for a non-queued command.
    fn dispatch_batch_one(&mut self, mut msg: CoordMsg) {
        let Some(cmd) = command_for(&msg.args) else {
            self.reply(
                msg.conn_id,
                msg.seq,
                encode_value(&RespValue::Error("ERR unknown command".into())),
            );
            return;
        };
        if is_eval_cmd(cmd.name) {
            let is_evalsha = matches!(cmd.name, "EVALSHA" | "EVALSHA_RO");
            let read_only = cmd.name.ends_with("_RO");
            let result = self.execute_script(&msg, is_evalsha, read_only);
            let slowlog_args = self.script_slowlog_args.take();
            self.reply_with_slowlog(msg.conn_id, msg.seq, result, slowlog_args);
            return;
        }
        if is_function_cmd(cmd.name) {
            let read_only = cmd.name.ends_with("_RO");
            let result = self.execute_function(&msg, read_only);
            let slowlog_args = self.script_slowlog_args.take();
            self.reply_with_slowlog(msg.conn_id, msg.seq, result, slowlog_args);
            return;
        }
        // `dispatch_keyed`'s shard derivation (event_loop.rs): global commands
        // and `DEBUG UNIQ-STRS` span every shard; everything else is keyed.
        if cmd.name == "DEBUG"
            && msg
                .args
                .get(1)
                .is_some_and(|a| a.eq_ignore_ascii_case(b"UNIQ-STRS"))
        {
            msg.shards = (0..self.num_shards).collect();
        } else if cmd.has_flag(FLAG_GLOBAL) {
            msg.shards = (0..self.num_shards).collect();
            msg.first_key_idx = cmd.key_range.first;
        } else {
            let keys = extract_keys(cmd, &msg.args);
            if keys.is_empty() {
                // Malformed/movable-key command without keys: let the executor
                // validate and reply with an error from shard 0.
                msg.shards = vec![0];
            } else {
                msg.keys = keys;
                msg.first_key_idx = cmd.key_range.first;
                msg.shards = keys_per_shard(&msg.args, &msg.keys, self.num_shards)
                    .iter()
                    .map(|(s, _)| *s)
                    .collect();
            }
        }
        match self.execute_tx(&msg) {
            // Inside EXEC a blocking command never waits (`no_block` is set).
            CmdResult::Blocked if msg.no_block => {
                let bytes = if command_for(&msg.args).is_some_and(blocking_timeout_is_nil_array) {
                    crate::protocol::resp::encode_nil_array()
                } else {
                    encode_result(CmdResult::Ok(RespValue::Nil))
                };
                self.reply(msg.conn_id, msg.seq, bytes);
            }
            CmdResult::Blocked => {
                let cmd = command_for(&msg.args);
                let deadline = cmd.and_then(|c| blocking_timeout_ms(c, &msg.args)).map_or(
                    Some(Instant::now()),
                    |ms| {
                        if ms == 0 {
                            None // wait forever
                        } else {
                            Some(Instant::now() + Duration::from_millis(ms))
                        }
                    },
                );
                self.pending.push_back(PendingTx { msg, deadline });
            }
            other => self.reply_result(msg.conn_id, msg.seq, other),
        }
    }

    fn retry_pending(&mut self, now: Instant) {
        if self.pending.is_empty() {
            return;
        }
        let mut remaining = Vec::with_capacity(self.pending.len());
        while let Some(p) = self.pending.pop_front() {
            if let Some(dl) = p.deadline
                && now >= dl
            {
                // The timeout reply is command-specific: BLPOP/BRPOP/BZPOPMIN/
                // BZPOPMAX/XREAD/XREADGROUP send a null *array* (`*-1`), the
                // rest a null bulk (`$-1`) (see `blocking_timeout_is_nil_array`).
                let bytes = if command_for(&p.msg.args).is_some_and(blocking_timeout_is_nil_array) {
                    crate::protocol::resp::encode_nil_array()
                } else {
                    encode_result(CmdResult::Ok(RespValue::Nil))
                };
                self.reply(p.msg.conn_id, p.msg.seq, bytes);
                continue;
            }
            match self.execute_tx(&p.msg) {
                CmdResult::Blocked => remaining.push(p),
                // A blocked command that wakes to find its key holding the
                // wrong type stays blocked (e.g. BLPOP blocked on `x`, then
                // `SET x str`): the wake is deferred until the key holds the
                // right type again (`WrongTypeDoesNotWake`).
                CmdResult::Err(e) if is_blocked_wrong_type(&p.msg, &e) => remaining.push(p),
                // A blocked XREADGROUP that wakes to find its group destroyed
                // replies with the wake-specific NOGROUP text (the generic
                // first-run message would be misleading; stream_family.cc:3160).
                CmdResult::Err(e) if is_xreadgroup(&p.msg) && e.message.starts_with("NOGROUP ") => {
                    let bytes = encode_result(CmdResult::Err(RespError::new(
                        "NOGROUP the consumer group this client was blocked on no longer exists",
                    )));
                    self.reply(p.msg.conn_id, p.msg.seq, bytes);
                }
                // A woken XREADGROUP only reports the stream that carries new
                // entries (`XReadGroupBlock` asserts a single `[key, n]` pair).
                CmdResult::Ok(RespValue::Array(items)) if is_xreadgroup(&p.msg) => {
                    let filtered = wake_xreadgroup_reply(items);
                    if filtered.is_empty() {
                        remaining.push(p);
                    } else {
                        self.reply_result(
                            p.msg.conn_id,
                            p.msg.seq,
                            CmdResult::Ok(RespValue::Array(filtered)),
                        );
                    }
                }
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
            let owned_for_s = owned_for(&owned, s);
            // A shard that owns all the command's keys journals the full
            // command tail; a partial owner journals reduced args (or nothing,
            // for `FLAG_NO_REDUCED` commands). Global commands own no keys but
            // still journal the full tail.
            let owns_all_keys = owned_for_s.len() == msg.keys.len();
            let ok = self.shard_txs[s].send(ShardMsg::TxLock {
                tx_id,
                conn_id: msg.conn_id,
                seq: msg.seq,
                args: msg.args.clone(),
                owned_key_idxs: owned_for_s,
                first_key_idx: msg.first_key_idx,
                db_idx: msg.db_idx,
                owns_all_keys,
                track_keys: msg.track_keys,
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
            if self.shard_txs[s]
                .send(ShardMsg::TxExec {
                    tx_id,
                    result_tx: res_tx,
                })
                .is_ok()
                && let Ok(p) = res_rx.recv()
            {
                parts.push(p);
            }
        }

        // Phase 3: release the locks.
        for &s in &msg.shards {
            let _ = self.shard_txs[s].send(ShardMsg::TxUnlock { tx_id });
        }

        match Self::finish_tx(msg, &parts) {
            CmdResult::DeferredStore { key, value, reply } => {
                self.perform_deferred_store(&key, value, None, false, msg.db_idx);
                CmdResult::Ok(reply)
            }
            CmdResult::DeferredStores { stores, reply } => {
                for (key, value, expire_at, sticky) in stores {
                    self.perform_deferred_store(&key, value, expire_at, sticky, msg.db_idx);
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
        db_idx: usize,
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
                db_idx,
                ack: ack_tx,
            })
            .is_ok()
        {
            let _ = ack_rx.recv();
            let _ = self.shard_txs[shard].send(ShardMsg::TxUnlock { tx_id });
        }
    }

    /// Run an EVAL-family command: resolve the script and its params, then hand
    /// the run to the shard owning the script's first key via
    /// `ShardMsg::RunScript` ([`run_script_request`]). The shard's interpreter
    /// compiles and executes the body; subcommands dispatch to peer shards from
    /// there (`CallFromScript`). The coordinator blocks for the
    /// `ScriptRunResult` like the reference's connection thread awaits the
    /// multi transaction.
    fn execute_script(&mut self, msg: &CoordMsg, is_evalsha: bool, read_only: bool) -> CmdResult {
        let args = &msg.args;
        let numkeys = match args.get(2).and_then(|a| crate::util::parse_i64(a)) {
            Some(n) if n >= 0 => n as usize,
            _ => return CmdResult::err("ERR value is not an integer or out of range"),
        };
        if args.len() < numkeys + 3 {
            return CmdResult::err("ERR Number of keys can't be greater than number of args");
        }
        let key_idxs: Vec<usize> = (3..3 + numkeys).collect();
        let keys: Vec<Vec<u8>> = key_idxs.iter().map(|&i| args[i].clone()).collect();
        let argv: Vec<Vec<u8>> = args[3 + numkeys..].to_vec();

        // Resolve the script body. An EVALSHA of an unknown script fails here:
        // the shared `ScriptMgr` is the only registry the shards compile from.
        let (sha, mut body) = if is_evalsha {
            let sha = String::from_utf8_lossy(&args[1]).to_ascii_lowercase();
            if sha.len() != 40 || !sha.bytes().all(|b| b.is_ascii_hexdigit()) {
                return CmdResult::err("NOSCRIPT No matching script. Please use EVAL.");
            }
            match self.script_mgr.lock().unwrap().find(&sha) {
                Some(s) => (sha, s.body.clone()),
                None => return CmdResult::err("NOSCRIPT No matching script. Please use EVAL."),
            }
        } else {
            let body = args[1].clone();
            // `Eval` replies null and caches nothing for an empty body.
            if body.is_empty() {
                return CmdResult::ok(RespValue::Nil);
            }
            (sha1_hex(&body), body)
        };

        // Reuse a flag-only or loaded entry's params (`GetScriptParams`);
        // otherwise deduce them, letting a bad `--!df flags=` line abort the
        // EVAL exactly like `ScriptMgr::Insert`.
        let params = {
            let mgr = self.script_mgr.lock().unwrap();
            match mgr.params(&sha) {
                Some(p) => Ok(p),
                None => mgr.deduce_and_override(&body),
            }
        };
        let params = match params {
            Ok(p) => p,
            Err(e) => return CmdResult::err(format!("ERR {e}")),
        };
        // `CallSHA` (main_service.cc:2389): a script whose mode is lower than
        // the running transaction's is rejected — a GLOBAL script
        // (`allow-undeclared-keys`) inside a LOCK_AHEAD MULTI, for example.
        // The transaction mode was deduced at EXEC time by the IO thread
        // (`DeduceExecMode`); outside EXEC it is `NotDetermined` and never
        // conflicts. The reference renders the modes as their integer values
        // (`absl::StrCat` over the enum).
        let script_mode = if params.atomic && params.undeclared_keys {
            TxMultiMode::Global
        } else if params.atomic {
            TxMultiMode::LockAhead
        } else {
            TxMultiMode::NonAtomic
        };
        if msg.no_block && msg.multi_mode > script_mode {
            return CmdResult::err(format!(
                "Multi mode conflict when running eval in multi transaction. Multi mode is: {} eval mode is: {}",
                msg.multi_mode as u8, script_mode as u8
            ));
        }
        // `lua_auto_async`: rewrite statement-context `redis.call`/`redis.pcall`
        // into `acall`/`apcall` for atomic bodies before the first compile
        // (`ScriptMgr::Insert`). The SHA stays computed over the original body,
        // and cached bodies (EVALSHA) are already rewritten.
        if !is_evalsha {
            body = self
                .script_mgr
                .lock()
                .unwrap()
                .auto_async_body(&body, &params);
        }

        let started = Instant::now();
        let (result, num_commands, slow_commands) = self.run_script_request(
            msg,
            read_only,
            ScriptRunKind::Eval {
                sha: sha.clone(),
                body: body.clone(),
            },
            keys.clone(),
            argv,
            params,
            &format!("script (call to {sha})"),
            Some(&sha),
        );
        // `CallSHA` records the script's run duration in usec for SCRIPT LATENCY.
        let elapsed_usec = started.elapsed().as_micros() as u64;
        self.script_mgr
            .lock()
            .unwrap()
            .record_latency(&sha, elapsed_usec);

        // `FormatEvalSlowlog` metadata for the SLOWLOG entry (conn_context.cc).
        let tx_mode = if params.atomic && params.undeclared_keys {
            1 // GLOBAL
        } else if params.atomic {
            2 // LOCK_AHEAD
        } else {
            3 // NON_ATOMIC
        };
        let tx_shards = eval_tx_shards(&keys, tx_mode, self.num_shards);
        self.script_slowlog_args = Some(format_eval_slowlog(
            &sha,
            num_commands,
            slow_commands,
            tx_mode,
            tx_shards,
            !read_only,
            keys.len(),
            args,
        ));

        // Cache the script only once it has compiled and run on a shard
        // (`ScriptMgr::Insert`); a failed EVAL must not leave an entry behind,
        // so a later EVALSHA replies NOSCRIPT like the reference.
        if matches!(result, CmdResult::Ok(_))
            && !is_evalsha
            && !self.script_mgr.lock().unwrap().exists(&sha)
        {
            self.script_mgr
                .lock()
                .unwrap()
                .store(sha.clone(), body, params);
        }

        result
    }

    /// Run a registered function (`FCALL`/`FCALL_RO`): resolve the function
    /// through the shared library registry, record it for `FUNCTION STATS`, and
    /// dispatch the run to the shard owning its first key like the EVAL path.
    fn execute_function(&mut self, msg: &CoordMsg, read_only: bool) -> CmdResult {
        let args = &msg.args;
        let name = String::from_utf8_lossy(&args[1]).into_owned();
        let numkeys = match args.get(2).and_then(|a| crate::util::parse_i64(a)) {
            Some(n) if n >= 0 => n as usize,
            _ => return CmdResult::err("ERR value is not an integer or out of range"),
        };
        if args.len() < numkeys + 3 {
            return CmdResult::err("ERR Number of keys can't be greater than number of args");
        }
        let key_idxs: Vec<usize> = (3..3 + numkeys).collect();
        let keys: Vec<Vec<u8>> = key_idxs.iter().map(|&i| args[i].clone()).collect();
        let argv: Vec<Vec<u8>> = args[3 + numkeys..].to_vec();

        let (lib, func) = {
            let mgr = self.script_mgr.lock().unwrap();
            let Some(lib) = mgr.function_lib(&name) else {
                return CmdResult::err("ERR Function not found");
            };
            let Some(func) = lib.functions.iter().find(|f| f.name == name) else {
                return CmdResult::err("ERR Function not found");
            };
            (lib.clone(), func.clone())
        };
        // `no-writes` (header or per-function) forces the read-only path.
        let read_only =
            read_only || lib.is_no_writes() || func.flags.iter().any(|f| f == "no-writes");
        let mut params = match lib.params() {
            Ok(p) => p,
            Err(e) => return CmdResult::err(format!("ERR {e}")),
        };
        params.undeclared_keys =
            params.undeclared_keys || func.flags.iter().any(|f| f == "allow-undeclared-keys");

        // Record the running function so `FUNCTION STATS` (IO thread) can see it
        // while the coordinator blocks for this run.
        self.script_mgr.lock().unwrap().set_running(
            &name,
            Self::render_fcall_command(args),
            now_ms(),
        );
        let (result, _num_commands, _slow_commands) = self.run_script_request(
            msg,
            read_only,
            ScriptRunKind::Function {
                name: name.clone(),
                lib_name: lib.name,
                lib_sha: lib.sha,
                code: lib.code,
            },
            keys,
            argv,
            params,
            &format!("function (call to {name})"),
            None,
        );
        self.script_mgr.lock().unwrap().clear_running();
        result
    }

    /// The original FCALL command text for `FUNCTION STATS` (`fcall <name> ...`).
    fn render_fcall_command(args: &[Vec<u8>]) -> String {
        let mut parts: Vec<String> = args
            .iter()
            .map(|a| String::from_utf8_lossy(a).into_owned())
            .collect();
        parts[0] = parts[0].to_ascii_lowercase();
        parts.join(" ")
    }

    /// Shared EVAL/FCALL launcher: build the `ScriptRunRequest` and dispatch it
    /// to the shard owning the script's first key (shard 0 for keyless GLOBAL
    /// scripts), then block for the `ScriptRunResult`. The run-shard locks every
    /// shard it needs before running (`ensure_locked`); peer shards stay free to
    /// process its cross-shard `ScriptOp`/`ScriptBatch` hops and lock acks while
    /// it executes. Runs are serialized on the coordinator, so a multi-shard
    /// script can never deadlock against another one on a peer shard.
    #[allow(clippy::too_many_arguments)]
    fn run_script_request(
        &mut self,
        msg: &CoordMsg,
        read_only: bool,
        kind: ScriptRunKind,
        keys: Vec<Vec<u8>>,
        argv: Vec<Vec<u8>>,
        params: crate::commands::lua::ScriptParams,
        err_desc: &str,
        on_error_key: Option<&str>,
    ) -> (CmdResult, usize, usize) {
        let tx_id = self.next_tx_id();
        let run_shard = if keys.is_empty() {
            0 // keyless GLOBAL script; the run-shard locks every shard up front
        } else {
            shard_for_key(&keys[0], self.num_shards)
        };
        let req = ScriptRunRequest {
            kind,
            keys,
            argv,
            params,
            conn_id: msg.conn_id,
            db_idx: msg.db_idx,
            track_keys: msg.track_keys,
            tx_id,
            read_only,
            slowlog_threshold_usec: msg.slowlog_threshold_usec,
            num_shards: self.num_shards,
            peer_txs: self.shard_txs.clone(),
        };
        let (result_tx, result_rx) = mpsc::channel();
        if self.shard_txs[run_shard]
            .send(ShardMsg::RunScript { req, result_tx })
            .is_err()
        {
            return (CmdResult::err("ERR internal: shard thread exited"), 0, 0);
        }
        let run_result = result_rx.recv().unwrap_or(ScriptRunResult {
            result: Err("ERR internal: shard thread exited".into()),
            num_commands: 0,
            slow_commands: 0,
        });
        let result = match run_result.result {
            Ok(v) => CmdResult::Ok(v),
            Err(e) => {
                if let Some(key) = on_error_key {
                    // `ScriptMgr::OnScriptError` runs for every `RUN_ERR`; only
                    // the undeclared-key message triggers the auto-correct flag
                    // flip.
                    self.script_mgr.lock().unwrap().on_script_error(key, &e);
                }
                // `IsResultSafe` failing sends the message bare, without the
                // `Error running ...` wrapper.
                if e == "reached lua stack limit" {
                    CmdResult::err("ERR reached lua stack limit")
                } else {
                    CmdResult::err(format!("ERR Error running {err_desc}: {e}"))
                }
            }
        };
        (result, run_result.num_commands, run_result.slow_commands)
    }

    fn finish_tx(msg: &CoordMsg, parts: &[ShardPart]) -> CmdResult {
        let any_err = parts.iter().any(|p| p.result.is_err());
        let any_ok = parts
            .iter()
            .any(|p| matches!(&p.result, CmdResult::Ok(_) | CmdResult::UniqueStrings(_)));
        if parts.is_empty() {
            return CmdResult::err("ERR internal: no shards participated");
        }
        if any_err || any_ok {
            let Some(cmd) = command_for(&msg.args) else {
                return CmdResult::err("ERR unknown command");
            };
            if let Some(merge) = cmd.merge {
                merge(parts, &msg.args, &msg.keys, now_ms())
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

    fn reply_with_slowlog(
        &self,
        conn_id: u64,
        seq: u64,
        result: CmdResult,
        slowlog_args: Option<Vec<Vec<u8>>>,
    ) {
        self.reply_bus.send(Reply {
            conn_id,
            seq,
            bytes: encode_result(result),
            slowlog_args,
            is_push: false,
        });
    }

    fn reply(&self, conn_id: u64, seq: u64, bytes: Vec<u8>) {
        self.reply_bus.send(Reply {
            conn_id,
            seq,
            bytes,
            slowlog_args: None,
            is_push: false,
        });
    }
}

/// Key indices owned by `shard` from a `keys_per_shard` grouping.
fn owned_for(per: &[(usize, Vec<usize>)], shard: usize) -> Vec<usize> {
    per.iter()
        .find(|(s, _)| *s == shard)
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}

/// `FormatExecSlowlog` (conn_context.cc:39): the augmented arguments of an
/// EXEC slowlog entry, prepended to the raw tail by `SlowLogShard::Add`.
pub(crate) fn format_exec_slowlog(num_cmds: usize, is_write: bool) -> Vec<Vec<u8>> {
    vec![
        format!("num_cmds: {num_cmds}").into_bytes(),
        format!("is_write: {}", u8::from(is_write)).into_bytes(),
    ]
}

/// `FormatEvalSlowlog` (conn_context.cc:44): the augmented arguments of an
/// EVAL/EVALSHA slowlog entry: the script sha, run stats, and then the
/// command's raw tail after the script/sha slot.
#[allow(clippy::too_many_arguments)]
fn format_eval_slowlog(
    sha: &str,
    num_commands: usize,
    slow_commands: usize,
    tx_mode: u8,
    tx_shards: u32,
    is_write: bool,
    lock_tags: usize,
    args: &[Vec<u8>],
) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(8 + args.len().saturating_sub(2));
    out.push(sha.as_bytes().to_vec());
    out.push(format!("num_cmds: {num_commands}").into_bytes());
    out.push(format!("slow_cmds: {slow_commands}").into_bytes());
    out.push(format!("tx_mode: {tx_mode}").into_bytes());
    out.push(format!("tx_shards: {tx_shards}").into_bytes());
    out.push(format!("is_write: {}", u8::from(is_write)).into_bytes());
    out.push(format!("lock_tags: {lock_tags}").into_bytes());
    out.extend(args[2..].iter().cloned());
    out
}

/// `stats.tx_shards`: the number of shards the transaction covers
/// (main_service.cc:2356-2407). Single-shard runs report 1; `LOCK_AHEAD`
/// without keys skips scheduling (`StartMulti` returns false) so no shards are
/// reported.
fn eval_tx_shards(keys: &[Vec<u8>], tx_mode: u8, num_shards: usize) -> u32 {
    let key_idxs: Vec<usize> = (0..keys.len()).collect();
    let mut distinct: Vec<usize> = Vec::new();
    for (s, _) in keys_per_shard(keys, &key_idxs, num_shards) {
        if !distinct.contains(&s) {
            distinct.push(s);
        }
    }
    let one_shard = distinct.len() <= 1;
    let can_run_single_shard = (num_shards == 1 && tx_mode == 1) || (one_shard && tx_mode == 2);
    if can_run_single_shard {
        1
    } else if tx_mode == 2 && keys.is_empty() {
        0
    } else {
        match tx_mode {
            1 => num_shards as u32,
            _ => distinct.len() as u32,
        }
    }
}
