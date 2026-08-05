use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::commands::exec::server::now_ms;
use crate::commands::lua::{SandboxedInterpreter, ScriptDispatch, ScriptMgr, sha1_hex};
use crate::commands::{FLAG_GLOBAL, FLAG_NOSCRIPT, FLAG_WRITE, ShardPart};
use crate::error::{CmdResult, RespValue};
use crate::server::{
    CoordMsg, Reply, ReplyBus, ShardMsg, blocking_timeout_ms, command_for, encode_result,
    extract_keys, is_eval_cmd, is_function_cmd, keys_per_shard, shard_for_key,
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
    script_mgr: Arc<Mutex<ScriptMgr>>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("coordinator".into())
        .spawn(move || {
            // The Lua state is not `Send`, so it must be created here on the
            // coordinator thread (the only thread that ever runs scripts).
            let sandbox = match SandboxedInterpreter::new() {
                Ok(s) => Some(s),
                Err(e) => {
                    eprintln!("coordinator: failed to init Lua interpreter: {e}");
                    None
                }
            };
            Coordinator {
                num_shards,
                rx,
                shard_txs,
                reply_bus,
                script_mgr,
                sandbox,
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
    shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    reply_bus: ReplyBus,
    script_mgr: Arc<Mutex<ScriptMgr>>,
    /// The coordinator-owned Lua state. `Option` so it can be taken out while a
    /// script runs (the dispatch context borrows the whole `Coordinator`).
    sandbox: Option<SandboxedInterpreter>,
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
                Ok(msg) => self.handle(msg),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            self.retry_pending(now_ms());
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
                db_idx: msg.db_idx,
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

        let (sha, body) = if is_evalsha {
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
        let params = match self.script_mgr.lock().unwrap().params(&sha) {
            Some(p) => p,
            None => match ScriptMgr::deduce_and_override(&body) {
                Ok(p) => p,
                Err(e) => return CmdResult::err(format!("ERR {e}")),
            },
        };
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

        // Lock the shards of the declared keys (like `execute_tx` phase 1).
        let tx_id = self.next_tx_id();
        let per = keys_per_shard(args, &key_idxs, self.num_shards);
        let shards: Vec<usize> = per.iter().map(|(s, _)| *s).collect();
        let mut ack_rxs = Vec::new();
        for &s in &shards {
            let (ack_tx, ack_rx) = mpsc::channel();
            if self.shard_txs[s]
                .send(ShardMsg::TxLock {
                    tx_id,
                    conn_id: msg.conn_id,
                    seq: msg.seq,
                    args: args.clone(),
                    owned_key_idxs: owned_for(&per, s),
                    first_key_idx: 0,
                    db_idx: msg.db_idx,
                    ack: ack_tx,
                })
                .is_ok()
            {
                ack_rxs.push(ack_rx);
            }
        }
        for rx in &ack_rxs {
            let _ = rx.recv();
        }

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
        };
        let result = {
            let mut dctx = ScriptDispatchCtx { coord: self, ctx };
            sandbox.run(&sha, &mut dctx, params.float_as_int)
        };
        self.sandbox = Some(sandbox);
        self.unlock_script(tx_id, &shards);

        match result {
            Ok(v) => CmdResult::Ok(v),
            Err(e) => CmdResult::err(format!("ERR Error running script (call to {sha}): {e}")),
        }
    }

    /// Run a registered function (`FCALL`/`FCALL_RO`): resolve the function
    /// through the shared library registry, load its library into the
    /// coordinator's interpreter if needed, lock the declared-key shards, and
    /// invoke the callback with `(keys, args)` tables like the EVAL path.
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

        // Lock the shards of the declared keys (like `execute_script`).
        let tx_id = self.next_tx_id();
        let per = keys_per_shard(args, &key_idxs, self.num_shards);
        let shards: Vec<usize> = per.iter().map(|(s, _)| *s).collect();
        let mut ack_rxs = Vec::new();
        for &s in &shards {
            let (ack_tx, ack_rx) = mpsc::channel();
            if self.shard_txs[s]
                .send(ShardMsg::TxLock {
                    tx_id,
                    conn_id: msg.conn_id,
                    seq: msg.seq,
                    args: args.clone(),
                    owned_key_idxs: owned_for(&per, s),
                    first_key_idx: 0,
                    db_idx: msg.db_idx,
                    ack: ack_tx,
                })
                .is_ok()
            {
                ack_rxs.push(ack_rx);
            }
        }
        for rx in &ack_rxs {
            let _ = rx.recv();
        }

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
        };
        let result = {
            let mut dctx = ScriptDispatchCtx { coord: self, ctx };
            sandbox.run_function(&name, &keys, &argv, &mut dctx, params.float_as_int)
        };
        self.script_mgr.lock().unwrap().clear_running();
        self.sandbox = Some(sandbox);
        self.unlock_script(tx_id, &shards);

        match result {
            Ok(v) => CmdResult::Ok(v),
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

    /// Dispatch a single `redis.call(...)` subcommand to one shard and wait for
    /// its result. The shard is normally already locked by the script's tx.
    fn script_op(
        &mut self,
        shard: usize,
        args: Vec<Vec<u8>>,
        owned: Vec<usize>,
        first_key_idx: usize,
        db_idx: usize,
    ) -> CmdResult {
        let (result_tx, result_rx) = mpsc::channel();
        if self.shard_txs[shard]
            .send(ShardMsg::ScriptOp {
                args,
                owned_key_idxs: owned,
                first_key_idx,
                db_idx,
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
}

/// Routes a script subcommand to the shards owning its keys while the script's
/// tx holds the declared-key locks.
struct ScriptDispatchCtx<'a> {
    coord: &'a mut Coordinator,
    ctx: ScriptCtx,
}

impl ScriptDispatch for ScriptDispatchCtx<'_> {
    fn dispatch(&mut self, args: Vec<Vec<u8>>) -> Result<RespValue, String> {
        let cmd = command_for(&args).ok_or_else(|| {
            format!(
                "ERR unknown command '{}'",
                String::from_utf8_lossy(&args[0])
            )
        })?;
        // Blocking commands and shard-spanning commands cannot run inside the
        // script's lock (`VerifyCommandState` NOSCRIPT / GLOBAL_TRANS checks).
        if cmd.has_flag(FLAG_NOSCRIPT) || cmd.has_flag(FLAG_GLOBAL) {
            return Err("This Redis command is not allowed from script".to_string());
        }
        if self.ctx.read_only && cmd.has_flag(FLAG_WRITE) {
            return Err("Write commands are not allowed from read-only scripts".to_string());
        }
        let keys = extract_keys(cmd, &args);
        if !self.ctx.undeclared_keys {
            for &ki in &keys {
                if !self.ctx.declared.contains(&args[ki]) {
                    return Err(format!(
                        "script tried accessing undeclared key, key: {}",
                        String::from_utf8_lossy(&args[ki])
                    ));
                }
            }
        }

        let first_key_idx = cmd.key_range.first;
        let mut parts = Vec::new();
        if keys.is_empty() {
            let result =
                self.coord
                    .script_op(0, args.clone(), vec![], first_key_idx, self.ctx.db_idx);
            parts.push(ShardPart {
                shard: 0,
                owned_key_idxs: vec![],
                result,
            });
        } else {
            for (s, owned) in keys_per_shard(&args, &keys, self.ctx.num_shards) {
                let result = self.coord.script_op(
                    s,
                    args.clone(),
                    owned.clone(),
                    first_key_idx,
                    self.ctx.db_idx,
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
                Some(m) => m(&parts, &args, &keys, now_ms()),
                None => parts[0].result.clone(),
            }
        } else {
            parts[0].result.clone()
        };
        let result = match merged {
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
        };
        Ok(result.into_resp_value())
    }
}
