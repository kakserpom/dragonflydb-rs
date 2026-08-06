use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::commands::exec::server::now_ms;
use crate::commands::lua::{
    FUNCTION_KILLED_ERR, SandboxedInterpreter, ScriptDispatch, ScriptMgr, sha1_hex,
};
use crate::commands::{Command, FLAG_GLOBAL, FLAG_NOSCRIPT, FLAG_WRITE, ShardPart};
use crate::error::{CmdResult, RespError, RespValue};
use crate::server::{
    CoordMsg, GcRequest, Reply, ReplyBus, ScriptBatchEntry, ShardMsg, blocking_timeout_ms,
    command_for, encode_result, extract_keys, is_eval_cmd, is_function_cmd, keys_per_shard,
    shard_for_key,
};

/// A blocking command (XREAD/XREADGROUP) waiting for data or a timeout. The
/// coordinator re-runs it until it returns data or the deadline passes.
struct PendingTx {
    msg: CoordMsg,
    deadline_ms: Option<u64>,
}

/// Whether a re-ran blocked command found its key holding the wrong type, in
/// which case it remains blocked rather than erroring (`WrongTypeDoesNotWake`).
fn is_blocked_wrong_type(msg: &CoordMsg, err: &RespError) -> bool {
    let Some(cmd) = command_for(&msg.args) else {
        return false;
    };
    blocking_timeout_ms(cmd, &msg.args).is_some() && err.message.starts_with("WRONGTYPE")
}

pub fn spawn(
    num_shards: usize,
    rx: mpsc::Receiver<CoordMsg>,
    gc_rx: mpsc::Receiver<GcRequest>,
    shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    reply_bus: ReplyBus,
    script_mgr: Arc<Mutex<ScriptMgr>>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("coordinator".into())
        .spawn(move || {
            // The Lua state is not `Send`, so it must be created here on the
            // coordinator thread (the only thread that ever runs scripts).
            let enable_redis_log = script_mgr.lock().unwrap().lua_enable_redis_log;
            let sandbox = match SandboxedInterpreter::with_redis_log(enable_redis_log) {
                Ok(s) => Some(s),
                Err(e) => {
                    eprintln!("coordinator: failed to init Lua interpreter: {e}");
                    None
                }
            };
            let kill = script_mgr.lock().unwrap().kill_flag();
            Coordinator {
                num_shards,
                rx,
                gc_rx,
                shard_txs,
                reply_bus,
                script_mgr,
                sandbox,
                kill,
                loaded_libs: HashMap::new(),
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
    gc_rx: mpsc::Receiver<GcRequest>,
    shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    reply_bus: ReplyBus,
    script_mgr: Arc<Mutex<ScriptMgr>>,
    /// The coordinator-owned Lua state. `Option` so it can be taken out while a
    /// script runs (the dispatch context borrows the whole `Coordinator`).
    sandbox: Option<SandboxedInterpreter>,
    /// `FUNCTION KILL` flag shared with the IO thread; polled by the
    /// `LUA_MASKCOUNT` instruction hook and the dispatch path.
    kill: Arc<AtomicBool>,
    /// Libraries already loaded into `sandbox`, keyed by library name with the
    /// loaded sha and its function names (so `FUNCTION LOAD REPLACE` invalidates
    /// the cached callbacks and purges names the new version dropped).
    loaded_libs: HashMap<String, (String, Vec<String>)>,
    tx_counter: u64,
    pending: VecDeque<PendingTx>,
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
            self.retry_pending(now_ms());
        }
    }

    /// `SCRIPT GC`: run a full GC over the coordinator's interpreter and ack
    /// (`ScriptMgr::GCCmd`, which does the same on every interpreter across all
    /// fibers before replying `+OK`). Requests are drained before pending
    /// commands so a GC never waits behind a queued blocking command.
    fn drain_gc_requests(&mut self) {
        while let Ok(req) = self.gc_rx.try_recv() {
            if let Some(sandbox) = &self.sandbox {
                let _ = sandbox.run_gc();
            }
            let _ = req.ack.send(());
        }
    }

    fn handle(&mut self, msg: CoordMsg) {
        // The EVAL family runs scripts in the coordinator's Lua state; it never
        // goes through the shard-based `execute_tx`.
        if let Some(cmd) = command_for(&msg.args)
            && is_eval_cmd(cmd.name)
        {
            let is_evalsha = matches!(cmd.name, "EVALSHA" | "EVALSHA_RO");
            let read_only = cmd.name.ends_with("_RO");
            let result = self.execute_script(&msg, is_evalsha, read_only);
            self.reply_result(msg.conn_id, msg.seq, result);
            return;
        }
        // The FCALL family runs registered functions the same way.
        if let Some(cmd) = command_for(&msg.args)
            && is_function_cmd(cmd.name)
        {
            let read_only = cmd.name.ends_with("_RO");
            let result = self.execute_function(&msg, read_only);
            self.reply_result(msg.conn_id, msg.seq, result);
            return;
        }
        match self.execute_tx(&msg) {
            CmdResult::Blocked => {
                if msg.no_block {
                    // Inside MULTI a blocking command never waits: it returns
                    // nil immediately (mirrors `RunCbOnFirstNonEmptyBlocking`'s
                    // `IsMulti` -> TIMED_OUT path).
                    let bytes = encode_result(CmdResult::Ok(RespValue::Nil));
                    self.reply(msg.conn_id, msg.seq, bytes);
                    return;
                }
                let cmd = command_for(&msg.args);
                let deadline_ms = cmd.and_then(|c| blocking_timeout_ms(c, &msg.args)).map_or(
                    Some(now_ms()),
                    |ms| {
                        if ms == 0 {
                            None // wait forever
                        } else {
                            Some(now_ms().saturating_add(ms))
                        }
                    },
                );
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
            if let Some(dl) = p.deadline_ms
                && now >= dl
            {
                let bytes = encode_result(CmdResult::Ok(RespValue::Nil));
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

    /// Run an EVAL-family command: resolve the script, lock its declared-key
    /// shards, install KEYS/ARGV, and run the body with `redis.call` dispatching
    /// subcommands straight to shards (`CallFromScript`).
    fn execute_script(&mut self, msg: &CoordMsg, is_evalsha: bool, read_only: bool) -> CmdResult {
        // A stale kill flag (set while a function ran) must not leak into an
        // EVAL; FUNCTION KILL only targets the running function.
        self.kill.store(false, Ordering::Relaxed);
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
        // EVAL exactly like `ScriptMgr::Insert`. The guard is taken once: a
        // `MutexGuard` temporary in a `match` scrutinee lives through the whole
        // match, so a second `lock()` in the `None` arm would self-deadlock.
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
        if !is_evalsha && !self.script_mgr.lock().unwrap().exists(&sha) {
            // Compile before caching (`Insert` short-circuits when the function
            // is already compiled); a compile error must not leave a cache
            // entry behind. The sandbox is taken out so the failed compile
            // cannot corrupt the script definition.
            let Some(sandbox) = self.sandbox.take() else {
                return CmdResult::err("ERR internal: no script interpreter");
            };
            let res = sandbox.define(&sha, &body);
            self.sandbox = Some(sandbox);
            if let Err(e) = res {
                // `SendError` renders bare messages with an `-ERR ` prefix.
                return CmdResult::err(format!("ERR {e}"));
            }
            self.script_mgr
                .lock()
                .unwrap()
                .store(sha.clone(), body.clone(), params);
        }

        // `DetermineMultiMode` (main_service.cc): atomic scripts (`LOCK_AHEAD`)
        // lock the declared-key shards for the whole body; `disable-atomicity`
        // scripts (`NON_ATOMIC`) hold no locks up front — each subcommand locks
        // its own shards only for the call, and `dragonfly.lock`/`unlock`
        // manage the held set explicitly.
        let atomic = params.atomic;
        let tx_id = self.next_tx_id();
        let (shards, locked_shards) = if atomic {
            let per = keys_per_shard(args, &key_idxs, self.num_shards);
            let per = if params.undeclared_keys {
                // GLOBAL mode (`DetermineMultiMode`): undeclared keys may land
                // on any shard, so lock them all up front.
                (0..self.num_shards)
                    .map(|s| (s, owned_for(&per, s)))
                    .collect()
            } else {
                per
            };
            let shards: Vec<usize> = per.iter().map(|(s, _)| *s).collect();
            let locked = self.lock_shards(tx_id, msg, &per);
            (shards, locked)
        } else {
            (Vec::new(), Vec::new())
        };

        // Run the script on the coordinator. The interpreter is taken out of
        // `self` so the dispatch context can borrow the whole coordinator.
        let Some(sandbox) = self.sandbox.take() else {
            self.unlock_script(tx_id, &shards);
            return CmdResult::err("ERR internal: no script interpreter");
        };
        if let Err(e) = sandbox.define(&sha, &body) {
            self.sandbox = Some(sandbox);
            self.unlock_script(tx_id, &shards);
            return CmdResult::err(format!("ERR {e}"));
        }
        if let Err(e) = sandbox.set_global_array("KEYS", &keys) {
            self.sandbox = Some(sandbox);
            self.unlock_script(tx_id, &shards);
            return CmdResult::err(format!("ERR Error running script (call to {sha}): {e}"));
        }
        if let Err(e) = sandbox.set_global_array("ARGV", &argv) {
            self.sandbox = Some(sandbox);
            self.unlock_script(tx_id, &shards);
            return CmdResult::err(format!("ERR Error running script (call to {sha}): {e}"));
        }

        let ctx = ScriptCtx {
            declared: keys,
            undeclared_keys: params.undeclared_keys,
            read_only,
            num_shards: self.num_shards,
            db_idx: msg.db_idx,
            atomic,
            tx_id,
            locked_shards,
            pinned_shards: Vec::new(),
            async_cmds: Vec::new(),
            async_bytes: 0,
        };
        // `CallSHA` records the script's run duration in usec for SCRIPT LATENCY.
        let started = Instant::now();
        let kill = Arc::clone(&self.kill);
        let float_as_int =
            params.float_as_int || self.script_mgr.lock().unwrap().lua_resp2_legacy_float;
        let (result, held) = {
            let mut dctx = ScriptDispatchCtx { coord: self, ctx };
            let run = sandbox.run(&sha, &mut dctx, float_as_int, &kill);
            // Force-flush pending `redis.acall` commands; a flush error
            // overrides the script's own result (`FlushEvalAsyncCmds(true)`).
            let flushed = dctx.flush();
            let held = std::mem::take(&mut dctx.ctx.locked_shards);
            (match flushed {
                Ok(()) => run,
                Err(e) => Err(e),
            }, held)
        };
        let elapsed_usec = started.elapsed().as_micros() as u64;
        self.script_mgr
            .lock()
            .unwrap()
            .record_latency(&sha, elapsed_usec);
        self.sandbox = Some(sandbox);
        self.unlock_script(tx_id, &held);

        match result {
            Ok(v) => CmdResult::Ok(v),
            // `ScriptMgr::OnScriptError` runs for every `RUN_ERR`; only the
            // undeclared-key message triggers the auto-correct flag flip.
            Err(e) => {
                self.script_mgr.lock().unwrap().on_script_error(&sha, &e);
                // `IsResultSafe` failing sends the message bare, without the
                // `Error running script (call to ...)` wrapper.
                if e == "reached lua stack limit" {
                    CmdResult::err("ERR reached lua stack limit")
                } else {
                    CmdResult::err(format!("ERR Error running script (call to {sha}): {e}"))
                }
            }
        }
    }

    /// Run a registered function (`FCALL`/`FCALL_RO`): resolve the function
    /// through the shared library registry, load its library into the
    /// coordinator's interpreter if needed, lock the declared-key shards, and
    /// invoke the callback with `(keys, args)` tables like the EVAL path.
    fn execute_function(&mut self, msg: &CoordMsg, read_only: bool) -> CmdResult {
        // Start clean; the flag is set (and honored) only while this run lives.
        self.kill.store(false, Ordering::Relaxed);
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

        let (lib, func, to_purge) = {
            let mgr = self.script_mgr.lock().unwrap();
            let Some(lib) = mgr.function_lib(&name) else {
                return CmdResult::err("ERR Function not found");
            };
            let Some(func) = lib.functions.iter().find(|f| f.name == name) else {
                return CmdResult::err("ERR Function not found");
            };
            // Callbacks of the previously loaded version that are no longer
            // registered anywhere (a REPLACE dropping them). Names still owned
            // by this or another library are left alone.
            let to_purge: Vec<String> = self
                .loaded_libs
                .get(&lib.name)
                .map(|(_, old_names)| {
                    old_names
                        .iter()
                        .filter(|n| mgr.function_lib(n).is_none())
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
            (lib.clone(), func.clone(), to_purge)
        };
        // `no-writes` (header or per-function) forces the read-only path.
        let read_only =
            read_only || lib.is_no_writes() || func.flags.iter().any(|f| f == "no-writes");
        let params = match lib.params() {
            Ok(p) => p,
            Err(e) => return CmdResult::err(format!("ERR {e}")),
        };
        let undeclared =
            params.undeclared_keys || func.flags.iter().any(|f| f == "allow-undeclared-keys");

        // (Re)create the library's callbacks in the coordinator's interpreter
        // on first FCALL or after a REPLACE (sha change).
        let Some(sandbox) = self.sandbox.take() else {
            return CmdResult::err("ERR internal: no script interpreter");
        };
        if self.loaded_libs.get(&lib.name).map(|(sha, _)| sha) != Some(&lib.sha) {
            sandbox.purge_functions(&to_purge);
            let functions = match sandbox.load_function_lib(&lib.code) {
                Ok(f) => f,
                Err(e) => {
                    self.sandbox = Some(sandbox);
                    return CmdResult::err(format!("ERR {e}"));
                }
            };
            let names: Vec<String> = functions.into_iter().map(|f| f.name).collect();
            self.loaded_libs
                .insert(lib.name.clone(), (lib.sha.clone(), names));
        }

        // Lock the shards of the declared keys (like `execute_script`). Library
        // functions follow the library's script params (`DetermineMultiMode`).
        let atomic = params.atomic;
        let tx_id = self.next_tx_id();
        let (_shards, locked_shards) = if atomic {
            let per = keys_per_shard(args, &key_idxs, self.num_shards);
            let per = if undeclared {
                // GLOBAL mode (`DetermineMultiMode`): undeclared keys may land
                // on any shard, so lock them all up front.
                (0..self.num_shards)
                    .map(|s| (s, owned_for(&per, s)))
                    .collect()
            } else {
                per
            };
            let shards: Vec<usize> = per.iter().map(|(s, _)| *s).collect();
            let locked = self.lock_shards(tx_id, msg, &per);
            (shards, locked)
        } else {
            (Vec::new(), Vec::new())
        };

        // Record the running function so `FUNCTION STATS` (IO thread) can see it.
        self.script_mgr.lock().unwrap().set_running(
            &name,
            Self::render_fcall_command(args),
            now_ms(),
        );

        let ctx = ScriptCtx {
            declared: keys.clone(),
            undeclared_keys: undeclared,
            read_only,
            num_shards: self.num_shards,
            db_idx: msg.db_idx,
            atomic,
            tx_id,
            locked_shards,
            pinned_shards: Vec::new(),
            async_cmds: Vec::new(),
            async_bytes: 0,
        };
        let (result, held) = {
            let kill = Arc::clone(&self.kill);
            let float_as_int =
                params.float_as_int || self.script_mgr.lock().unwrap().lua_resp2_legacy_float;
            let mut dctx = ScriptDispatchCtx { coord: self, ctx };
            let run = sandbox.run_function(&name, &keys, &argv, &mut dctx, float_as_int, &kill);
            // Force-flush pending `redis.acall` commands; a flush error
            // overrides the function's own result (`FlushEvalAsyncCmds(true)`).
            let flushed = dctx.flush();
            let held = std::mem::take(&mut dctx.ctx.locked_shards);
            (match flushed {
                Ok(()) => run,
                Err(e) => Err(e),
            }, held)
        };
        self.script_mgr.lock().unwrap().clear_running();
        self.sandbox = Some(sandbox);
        self.unlock_script(tx_id, &held);

        match result {
            Ok(v) => CmdResult::Ok(v),
            Err(e) if e == "reached lua stack limit" => {
                CmdResult::err("ERR reached lua stack limit")
            }
            Err(e) => CmdResult::err(format!("ERR Error running function (call to {name}): {e}")),
        }
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

    /// Release the shard locks held for a script run.
    fn unlock_script(&mut self, tx_id: u64, shards: &[usize]) {
        for &s in shards {
            let _ = self.shard_txs[s].send(ShardMsg::TxUnlock { tx_id });
        }
    }

    /// Phase 1 of a transaction: lock every shard in `per` and wait until all
    /// have acked, returning the shards that acknowledged. Used for the
    /// upfront `LOCK_AHEAD` lock of atomic scripts (and `execute_tx`).
    fn lock_shards(&mut self, tx_id: u64, msg: &CoordMsg, per: &[(usize, Vec<usize>)]) -> Vec<usize> {
        let mut ack_rxs = Vec::new();
        let mut shards = Vec::new();
        for &(s, _) in per {
            let (ack_tx, ack_rx) = mpsc::channel();
            if self.shard_txs[s]
                .send(ShardMsg::TxLock {
                    tx_id,
                    conn_id: msg.conn_id,
                    seq: msg.seq,
                    args: msg.args.clone(),
                    owned_key_idxs: owned_for(per, s),
                    first_key_idx: 0,
                    db_idx: msg.db_idx,
                    owns_all_keys: false,
                    ack: ack_tx,
                })
                .is_ok()
            {
                shards.push(s);
                ack_rxs.push(ack_rx);
            }
        }
        for rx in &ack_rxs {
            let _ = rx.recv();
        }
        shards
    }

    /// Dispatch a single `redis.call(...)` subcommand to one shard and wait for
    /// its result. The shard is normally already locked by the script's tx.
    fn script_op(
        &mut self,
        shard: usize,
        args: Vec<Vec<u8>>,
        owned: Vec<usize>,
        first_key_idx: usize,
        db_idx: usize,
        owns_all_keys: bool,
    ) -> CmdResult {
        let (result_tx, result_rx) = mpsc::channel();
        if self.shard_txs[shard]
            .send(ShardMsg::ScriptOp {
                args,
                owned_key_idxs: owned,
                first_key_idx,
                db_idx,
                owns_all_keys,
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

    fn finish_tx(msg: &CoordMsg, parts: &[ShardPart]) -> CmdResult {
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

    fn reply(&self, conn_id: u64, seq: u64, bytes: Vec<u8>) {
        self.reply_bus.send(Reply {
            conn_id,
            seq,
            bytes,
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

/// Per-run state a script's `redis.call`/`redis.pcall` subcommands need.
struct ScriptCtx {
    /// Values of the declared KEYS (the script's lock tags).
    declared: Vec<Vec<u8>>,
    /// Whether subcommands may touch undeclared keys (allow-undeclared-keys).
    undeclared_keys: bool,
    /// `EVAL_RO` / `EVALSHA_RO`: reject any write subcommand.
    read_only: bool,
    num_shards: usize,
    /// The DB all subcommands run in (the connection's selected DB).
    db_idx: usize,
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
/// tx holds the declared-key locks.
struct ScriptDispatchCtx<'a> {
    coord: &'a mut Coordinator,
    ctx: ScriptCtx,
}

impl ScriptDispatch for ScriptDispatchCtx<'_> {
    fn dispatch(&mut self, args: Vec<Vec<u8>>) -> Result<RespValue, String> {
        // `FUNCTION KILL` from the IO thread: abort at the next dispatch
        // boundary (mirrors the count hook, which cannot fire while a
        // subcommand is dispatched from Rust).
        if self.coord.kill.load(Ordering::Relaxed) {
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
        if !self.ctx.atomic {
            let shards = self.cmd_shards(cmd, &args);
            self.ensure_locked(&shards)?;
            let r = self.execute_script_cmd(cmd, &args);
            self.release_unpinned();
            return Ok(r.into_resp_value());
        }
        Ok(self.execute_script_cmd(cmd, &args).into_resp_value())
    }

    fn lock(&mut self, keys: Vec<Vec<u8>>) -> Result<(), String> {
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

impl ScriptDispatchCtx<'_> {
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
            let (ack_tx, _ack_rx) = mpsc::channel();
            if self.coord.shard_txs[s]
                .send(ShardMsg::TxLock {
                    tx_id: self.ctx.tx_id,
                    conn_id: 0,
                    seq: 0,
                    args: Vec::new(),
                    owned_key_idxs: Vec::new(),
                    first_key_idx: 0,
                    db_idx: self.ctx.db_idx,
                    owns_all_keys: false,
                    ack: ack_tx,
                })
                .is_err()
            {
                return Err("ERR internal: shard thread exited".into());
            }
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
                self.coord.unlock_script(self.ctx.tx_id, &[s]);
            }
        }
        self.ctx.locked_shards = still;
    }

    /// Release every lock the script's transaction holds (`UnlockMulti(true)`).
    fn release_all(&mut self) {
        for s in std::mem::take(&mut self.ctx.locked_shards) {
            self.coord.unlock_script(self.ctx.tx_id, &[s]);
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
        if self.coord.kill.load(Ordering::Relaxed) {
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
                        self.coord,
                        self.ctx.db_idx,
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
                    self.coord,
                    self.ctx.db_idx,
                    &mut batches,
                    &mut results,
                    &abort_flags,
                    &mut hop_positions,
                );
                if fatal.is_some() {
                    break;
                }
                let result = self.execute_script_cmd(exec, &cmd.args);
                results[pos] = Some(result);
            }
        }
        if fatal.is_none() {
            fatal = run_squash_hop(
                self.coord,
                self.ctx.db_idx,
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
        match fatal {
            Some(msg) => Err(msg),
            None => Ok(()),
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
        // The reference's `DispatchCommand` arity check runs for script
        // subcommands too; without it a keyless subcommand with too few args
        // would reach the executor and panic (`GET` -> `owned_keys[0]`).
        if let Some(e) = cmd.check_arity(args.len()) {
            return Err(e);
        }
        // `DispatchCommand` (main_service.cc): GLOBAL_TRANS / NO_KEY_TRANSACTIONAL
        // commands may run only when the script schedules globally or re-schedules
        // per operation (GLOBAL / NON_ATOMIC); NOSCRIPT commands never run.
        if cmd.has_flag(FLAG_NOSCRIPT)
            || (cmd.has_flag(FLAG_GLOBAL) && self.ctx.atomic && !self.ctx.undeclared_keys)
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

    /// Execute one (already verified) subcommand against the shards owning its
    /// keys, merging partial results and resolving deferred stores.
    fn execute_script_cmd(&mut self, cmd: &'static Command, args: &[Vec<u8>]) -> CmdResult {
        let keys = extract_keys(cmd, args);
        let first_key_idx = cmd.key_range.first;
        let mut parts = Vec::new();
        if keys.is_empty() {
            let result = self.coord.script_op(
                0,
                args.to_owned(),
                vec![],
                first_key_idx,
                self.ctx.db_idx,
                true,
            );
            parts.push(ShardPart {
                shard: 0,
                owned_key_idxs: vec![],
                result,
            });
        } else {
            for (s, owned) in keys_per_shard(args, &keys, self.ctx.num_shards) {
                // A subcommand whose keys all live on one shard journals the
                // full tail; split keys journal reduced args per shard.
                let owns_all_keys = owned.len() == keys.len();
                let result = self.coord.script_op(
                    s,
                    args.to_owned(),
                    owned.clone(),
                    first_key_idx,
                    self.ctx.db_idx,
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
                self.coord
                    .perform_deferred_store(&key, value, None, false, self.ctx.db_idx);
                CmdResult::Ok(reply)
            }
            CmdResult::DeferredStores { stores, reply } => {
                for (key, value, expire_at, sticky) in stores {
                    self.coord.perform_deferred_store(
                        &key,
                        value,
                        expire_at,
                        sticky,
                        self.ctx.db_idx,
                    );
                }
                CmdResult::Ok(reply)
            }
            CmdResult::Blocked => CmdResult::Ok(RespValue::Nil),
            other => other,
        }
    }
}

/// Run one squashed hop: every shard with queued entries executes them in a
/// single message (`ShardMsg::ScriptBatch`, dispatched in parallel). Results
/// are reassembled by call position; the first `acall` error in call order is
/// returned (`error_abort` in `ExecuteSquashed`). The whole hop runs even when
/// it contains an error, mirroring the reference's shard-side execution.
/// Clears the batches and the hop position list.
fn run_squash_hop(
    coord: &mut Coordinator,
    db_idx: usize,
    batches: &mut [Vec<BatchEntry>],
    results: &mut [Option<CmdResult>],
    abort_flags: &[bool],
    hop_positions: &mut Vec<usize>,
) -> Option<String> {
    let mut hops: Vec<(usize, mpsc::Receiver<Vec<CmdResult>>)> = Vec::new();
    for (shard, entries) in batches.iter().enumerate() {
        if entries.is_empty() {
            continue;
        }
        let (result_tx, result_rx) = mpsc::channel();
        if coord.shard_txs[shard]
            .send(ShardMsg::ScriptBatch {
                cmds: entries
                    .iter()
                    .map(|e| ScriptBatchEntry {
                        args: e.args.clone(),
                        owned_key_idxs: e.owned.clone(),
                        first_key_idx: e.first_key_idx,
                        db_idx,
                        owns_all_keys: e.owns_all_keys,
                    })
                    .collect(),
                result_tx,
            })
            .is_err()
        {
            return Some("ERR internal: shard thread exited".into());
        }
        hops.push((shard, result_rx));
    }
    for (shard, rx) in hops {
        match rx.recv() {
            Ok(per_shard) => {
                for (entry, result) in batches[shard].iter().zip(per_shard) {
                    results[entry.pos] = Some(result);
                }
            }
            Err(_) => {
                for entry in &batches[shard] {
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

/// One squashed-batch entry destined for a specific shard, keeping its call
/// position so results reassemble in script order.
struct BatchEntry {
    pos: usize,
    args: Vec<Vec<u8>>,
    owned: Vec<usize>,
    first_key_idx: usize,
    owns_all_keys: bool,
}
