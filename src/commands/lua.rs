//! Lua scripting engine backing EVAL/EVALSHA and the SCRIPT cache.
//!
//! Mirrors the reference implementation (`dragonfly/src/core/interpreter.cc`
//! and `dragonfly/src/server/script_mgr.cc`): a per-thread sandboxed Lua 5.4
//! interpreter, a SHA-1 keyed script cache with flags, and strict-global
//! enforcement installed exactly once per Lua state.

use std::cell::RefCell;
use std::collections::HashMap;
use std::os::raw::c_char;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use mlua::chunk::ChunkMode;
use mlua::ffi;
use mlua::{Function, HookTriggers, Lua, MultiValue, StdLib, Table, Value, VmState};

use crate::commands::lua_libs;
use crate::core::histogram::Histogram;
use crate::error::RespValue;
use crate::util::{format_lua_float, itoa, lua_tolstring};

use xxhash_rust::xxh64::xxh64;

/// Error returned when a script/EVALSHA SHA is unknown.
pub const NOSCRIPT_ERR: &str = "NOSCRIPT No matching script. Please use EVAL.";
/// Error raised by a script accessing a key it did not declare.
pub const UNDECLARED_KEY_ERR: &str = "script tried accessing undeclared key";
/// Error raised by a write command inside a read-only script.
pub const READONLY_WRITE_ERR: &str = "Write commands are not allowed from read-only scripts";
/// Error raised by a command flagged NOSCRIPT (blocking, SUBSCRIBE, ...).
pub const NOSCRIPT_CMD_ERR: &str = "This Redis command is not allowed from script";
/// Error raised when a `redis.call`/`redis.pcall` argument is not a string/integer.
pub const ARG_TYPE_ERR: &str = "Lua redis() command arguments must be strings or integers";
/// Error raised inside a function when `FUNCTION KILL` interrupts it. Redis
/// reuses the SCRIPT KILL text for functions (the count hook is shared).
pub const FUNCTION_KILLED_ERR: &str = "Script killed by user with SCRIPT KILL...";

/// Redis's `LUA_MASKCOUNT` hook interval: the kill flag is polled after every
/// 100k VM instructions, so a CPU-bound tight loop is interruptible.
const KILL_HOOK_INTERVAL: u32 = 100_000;

// ---------------------------------------------------------------------------
// SHA-1
// ---------------------------------------------------------------------------

/// Compute a SHA-1 digest. Pure Rust, no new dependencies (mirrors `EVP_Digest`).
fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [
        0x6745_2301,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];

    let mut msg = Vec::with_capacity(data.len() + 72);
    msg.extend_from_slice(data);
    let bit_len = (data.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 80];
        for (i, b) in chunk.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h[0], h[1], h[2], h[3], h[4]);
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999_u32),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let tmp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = tmp;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }

    let mut out = [0u8; 20];
    for (i, v) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&v.to_be_bytes());
    }
    out
}

/// Lowercase hex SHA-1 of `data` (`Interpreter::FuncSha1` + `ToHex`).
#[must_use]
pub fn sha1_hex(data: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(40);
    for b in sha1(data) {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xF) as usize] as char);
    }
    out
}

// ---------------------------------------------------------------------------
// Script cache
// ---------------------------------------------------------------------------

/// Flags controlling how a script executes (`ScriptMgr::ScriptParams`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScriptParams {
    /// Whether the script must run atomically (locks its keys).
    pub atomic: bool,
    /// Whether the script may access keys it did not declare.
    pub undeclared_keys: bool,
    /// Whether floats are returned as integers (legacy-float).
    pub float_as_int: bool,
}

impl Default for ScriptParams {
    fn default() -> Self {
        ScriptParams {
            atomic: true,
            undeclared_keys: false,
            float_as_int: false,
        }
    }
}

impl ScriptParams {
    /// Parse comma/semicolon/space separated flags, mirroring
    /// `ScriptMgr::ScriptParams::ApplyFlags`.
    pub fn apply_flags(&mut self, flags: &str) -> Result<(), String> {
        for flag in flags.split([' ', ',', ';']).filter(|f| !f.is_empty()) {
            match flag {
                "disable-atomicity" => self.atomic = false,
                "allow-undeclared-keys" => self.undeclared_keys = true,
                "legacy-float" => self.float_as_int = true,
                "no-writes" => {} // Redis compat flag, no-op like upstream
                other => return Err(format!("Invalid flag: {other}")),
            }
        }
        Ok(())
    }
}

/// A compiled script body plus its execution flags.
#[derive(Debug, Clone)]
pub struct Script {
    pub sha: String,
    pub body: Vec<u8>,
    pub params: ScriptParams,
}

// ---------------------------------------------------------------------------
// Function library registry (FUNCTION / FCALL)
// ---------------------------------------------------------------------------

/// One function registered by a library's `redis.register_function(...)` call.
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    pub name: String,
    /// Per-function flags from the table form (`no-writes`,
    /// `allow-undeclared-keys`).
    pub flags: Vec<String>,
}

/// A loaded function library: the metadata `FUNCTION LOAD` parsed out of the
/// `#!lua name=...` header plus the registered function list. The callbacks
/// themselves live in the coordinator's Lua state (recreated lazily on first
/// FCALL), keyed by the library `sha`.
#[derive(Debug, Clone)]
pub struct FunctionLib {
    pub name: String,
    pub engine: String,
    pub code: Vec<u8>,
    pub sha: String,
    /// Flags from the `#!lua ... flags=` header line.
    pub header_flags: Vec<String>,
    pub functions: Vec<FunctionInfo>,
}

/// A function currently executing on the coordinator (`FUNCTION STATS`).
#[derive(Debug, Clone)]
pub struct RunningFunction {
    pub name: String,
    /// The original FCALL/FCALL_RO command as text.
    pub command: String,
    pub started_ms: u64,
}

impl FunctionLib {
    /// Execution flags derived from the `#!lua flags=` header.
    pub fn params(&self) -> Result<ScriptParams, String> {
        let mut params = ScriptParams::default();
        for f in &self.header_flags {
            params.apply_flags(f)?;
        }
        Ok(params)
    }

    #[must_use]
    pub fn is_no_writes(&self) -> bool {
        self.header_flags.iter().any(|f| f == "no-writes")
    }
}

/// SHA-1 keyed script cache plus the FUNCTION library registry (`ScriptMgr`).
/// Shared behind a `Mutex` between the IO thread (SCRIPT/FUNCTION subcommands)
/// and the coordinator thread (EVAL/FCALL).
#[derive(Debug, Default)]
pub struct ScriptMgr {
    scripts: HashMap<String, Script>,
    /// Flag-only entries created by SCRIPT FLAGS before the script is loaded.
    params: HashMap<String, ScriptParams>,
    /// Function libraries by library name (FUNCTION registry).
    libraries: HashMap<String, FunctionLib>,
    /// Function name -> library name index (function names are unique across
    /// libraries, like Redis).
    functions: HashMap<String, String>,
    /// The function running on the coordinator, if any.
    running: Option<RunningFunction>,
    /// `FUNCTION KILL`: set by the IO thread, polled by the `LUA_MASKCOUNT`
    /// instruction hook (every `KILL_HOOK_INTERVAL` VM instructions) and by the
    /// dispatch path between `redis.call` subcommands. `Arc` so the coordinator
    /// can read it without holding the mgr mutex on every subcommand.
    kill: Arc<AtomicBool>,
    /// Per-SHA script run durations (`SCRIPT LATENCY`, like the reference's
    /// `ServerState::call_latency_histos_`).
    latency: HashMap<String, Histogram>,
    /// `--lua_auto_async`: rewrite statement-context `redis.call`/`redis.pcall`
    /// into `redis.acall`/`redis.apcall` at load time (`FLAGS_lua_auto_async`).
    /// Defaults off; applies only to atomic scripts.
    pub lua_auto_async: bool,
    /// `--default_lua_flags`: flags applied to scripts that carry no own
    /// `--!df flags=` line (`default_params_` in `ScriptMgr`'s constructor).
    pub default_params: ScriptParams,
    /// `--lua_undeclared_keys_shas`: SHAs force-flagged `undeclared_keys` at
    /// load time (`FLAGS_lua_undeclared_keys_shas`, only read at insert).
    pub undeclared_keys_shas: Vec<String>,
    /// `--lua_float_as_int_shas`: SHAs force-flagged `float_as_int` at load
    /// time (`FLAGS_lua_float_as_int_shas`).
    pub float_as_int_shas: Vec<String>,
    /// `--lua_allow_undeclared_auto_correct`: on an undeclared-key error, flip
    /// the cached script's flag so the next run is global (`ScriptMgr::OnScriptError`).
    pub lua_allow_undeclared_auto_correct: bool,
    /// `--lua_resp2_legacy_float`: treat every script float result as
    /// `legacy-float` regardless of the script's flags (`EvalSerializer::OnDouble`).
    pub lua_resp2_legacy_float: bool,
    /// `--lua_enable_redis_log`: `redis.log` validates its level and actually
    /// logs to stderr (`FLAGS_lua_enable_redis_log`); off, the call is a silent
    /// no-op.
    pub lua_enable_redis_log: bool,
}

/// Script SHAs upstream force-flags (buggy clients, see `script_mgr.cc:284`).
const HARDCODED_UNDECLARED: &[&str] = &[
    "351130589c64523cb98978dc32c64173a31244f3",
    "6ae15ef4678593dc61f991c9953722d67d822776",
    "34b1048274c8e50a0cc587a3ed9c383a82bb78c5",
    "b725ca33e5b36f318ab1150b8ac955a3d997c872",
    "8c4dafdf9b6b7bcf511a0d1ec0518bed9260e16d",
    "3fc258d735c924d5652fceb90b41bea1f1f29e4b",
    "43d401bd2bd0ad864c3ca221512cda1b6215ec23",
    "1617c9fb2bda7d790bb1aaa320c1099d81825e64",
    "39383dcf36d2e71364a666b2a806bc8219cd332d",
    "6990147f5d1999b936dac3b6f7e5d2071908bcf3",
];

/// SHA forced `atomic` because the script's own `--!df flags=` says otherwise
/// and the script breaks without it (buggy client, see `script_mgr.cc:284`).
const HARDCODED_ATOMIC: &str = "f8133be7f04abd9dfefa83c3b29a9d837cfbda86";

impl ScriptMgr {
    #[must_use]
    pub fn new() -> Self {
        ScriptMgr::default()
    }

    /// Apply the Lua-related CLI flags, called once at startup (`main.rs`).
    /// `default_lua_flags` is parsed like a script's `--!df flags=` line and
    /// errors exactly like `ScriptParams::ApplyFlags`.
    pub fn configure(
        &mut self,
        default_lua_flags: &str,
        undeclared_keys_shas: Vec<String>,
        float_as_int_shas: Vec<String>,
        lua_allow_undeclared_auto_correct: bool,
        lua_resp2_legacy_float: bool,
        lua_enable_redis_log: bool,
    ) -> Result<(), String> {
        self.default_params.apply_flags(default_lua_flags)?;
        self.undeclared_keys_shas = undeclared_keys_shas;
        self.float_as_int_shas = float_as_int_shas;
        self.lua_allow_undeclared_auto_correct = lua_allow_undeclared_auto_correct;
        self.lua_resp2_legacy_float = lua_resp2_legacy_float;
        self.lua_enable_redis_log = lua_enable_redis_log;
        Ok(())
    }

    /// Parse the `--!df flags=` prefix a script may start with
    /// (`DeduceParams` in `script_mgr.cc`). `Ok(None)` when the prefix is
    /// absent or the flags line has no trailing whitespace.
    pub fn deduce_params(body: &[u8]) -> Result<Option<ScriptParams>, String> {
        const PREFIX: &[u8] = b"--!df flags=";
        let body = trim_ascii_start(body);
        if !body.starts_with(PREFIX) {
            return Ok(None);
        }
        let rest = &body[PREFIX.len()..];
        let len = rest
            .iter()
            .position(|&b| b.is_ascii_whitespace())
            .unwrap_or(rest.len());
        if len == rest.len() {
            return Ok(None);
        }
        let mut params = ScriptParams::default();
        let flags = std::str::from_utf8(&rest[..len]).map_err(|_| "Invalid flag".to_string())?;
        params.apply_flags(flags)?;
        Ok(Some(params))
    }

    /// Store a compiled script and return its SHA. The caller must have already
    /// compiled the body (through an interpreter) so a compile error never
    /// leaves a cache entry, mirroring `ScriptMgr::Insert`.
    pub fn store(&mut self, sha: String, body: Vec<u8>, params: ScriptParams) {
        self.scripts.insert(
            sha.clone(),
            Script {
                sha: sha.clone(),
                body,
                params,
            },
        );
        self.params.insert(sha, params);
    }

    #[must_use]
    pub fn exists(&self, sha: &str) -> bool {
        self.scripts.contains_key(sha)
    }

    #[must_use]
    pub fn find(&self, sha: &str) -> Option<&Script> {
        self.scripts.get(sha)
    }

    #[must_use]
    pub fn params(&self, sha: &str) -> Option<ScriptParams> {
        self.scripts
            .get(sha)
            .map(|s| s.params)
            .or_else(|| self.params.get(sha).copied())
    }

    /// `(sha, body)` for every cached script, plus flag-only entries (no body)
    /// created by SCRIPT FLAGS before the script was loaded, unordered
    /// (`ScriptMgr::GetAll`).
    #[must_use]
    pub fn get_all(&self) -> Vec<(String, Vec<u8>)> {
        let mut out: Vec<(String, Vec<u8>)> = self
            .scripts
            .iter()
            .map(|(sha, s)| (sha.clone(), s.body.clone()))
            .collect();
        for sha in self.params.keys() {
            if !self.scripts.contains_key(sha) {
                out.push((sha.clone(), Vec::new()));
            }
        }
        out
    }

    /// SCRIPT FLUSH: clears the cache (`FlushAllScript`).
    pub fn flush(&mut self) {
        self.scripts.clear();
        self.params.clear();
    }

    /// SCRIPT FLAGS: record flags for `sha`, creating a flag-only entry even
    /// when the script is not loaded yet (`ConfigCmd`). Returns the "Invalid
    /// config format: <err>" error on a bad flag.
    pub fn apply_flags(&mut self, sha: &str, flags: &[String]) -> Result<(), String> {
        let entry = self.params.entry(sha.to_string()).or_default();
        for flag in flags {
            if let Err(e) = entry.apply_flags(flag) {
                return Err(format!("Invalid config format: {e}"));
            }
        }
        if let Some(script) = self.scripts.get_mut(sha) {
            script.params = *entry;
        }
        Ok(())
    }

    /// Full `ScriptParams` for a body, applying the hardcoded SHA overrides and
    /// the CLI `--lua_undeclared_keys_shas`/`--lua_float_as_int_shas` lists
    /// (`ScriptMgr::Insert`). Scripts without a `--!df flags=` line inherit
    /// `--default_lua_flags` (`params_opt->value_or(default_params_)`).
    pub fn deduce_and_override(&self, body: &[u8]) -> Result<ScriptParams, String> {
        let sha = sha1_hex(body);
        let mut params = ScriptMgr::deduce_params(body)?.unwrap_or(self.default_params);
        Self::apply_hardcoded_overrides(&sha, &mut params);
        self.apply_cli_sha_overrides(&sha, &mut params);
        Ok(params)
    }

    /// Apply the `--lua_undeclared_keys_shas`/`--lua_float_as_int_shas` lists,
    /// mirroring the else-if in `ScriptMgr::Insert` (the hardcoded list takes
    /// precedence for undeclared keys).
    fn apply_cli_sha_overrides(&self, sha: &str, params: &mut ScriptParams) {
        if !HARDCODED_UNDECLARED.contains(&sha)
            && self.undeclared_keys_shas.iter().any(|s| s == sha)
        {
            params.undeclared_keys = true;
        }
        if self.float_as_int_shas.iter().any(|s| s == sha) {
            params.float_as_int = true;
        }
    }

    /// `ScriptMgr::OnScriptError`: with `--lua_allow_undeclared_auto_correct`,
    /// an undeclared-key run error flips the cached script's flag so the next
    /// run is allowed to touch undeclared keys. Flag-only entries are left
    /// alone (the reference only rewrites loaded scripts).
    pub fn on_script_error(&mut self, sha: &str, error: &str) {
        if !self.lua_allow_undeclared_auto_correct || !error.contains(UNDECLARED_KEY_ERR) {
            return;
        }
        if let Some(script) = self.scripts.get_mut(sha) {
            script.params.undeclared_keys = true;
            self.params.insert(sha.to_string(), script.params);
        }
    }

    /// Force flags for known buggy client scripts (`ScriptMgr::Insert`):
    /// allow undeclared keys for the `kUndeclaredShas` list, and restore
    /// atomicity for the Sidekiq script of issue #4522 even when its own
    /// `--!df flags=` disables it.
    fn apply_hardcoded_overrides(sha: &str, params: &mut ScriptParams) {
        if HARDCODED_UNDECLARED.contains(&sha) {
            params.undeclared_keys = true;
        }
        if !params.atomic && sha == HARDCODED_ATOMIC {
            params.atomic = true;
        }
    }

    /// The body to compile and cache for a script, applying the `lua_auto_async`
    /// rewrite for atomic scripts exactly like `Insert` in `script_mgr.cc`.
    /// The SHA stays computed over the original body (the 'a' insertions are
    /// transparent to callers).
    #[must_use]
    pub fn auto_async_body(&self, body: &[u8], params: &ScriptParams) -> Vec<u8> {
        if !self.lua_auto_async || !params.atomic {
            return body.to_vec();
        }
        detect_possible_async_calls(body).unwrap_or_else(|| body.to_vec())
    }

    /// Insert a library into the FUNCTION registry, replacing any existing one
    /// (used by both `FUNCTION LOAD REPLACE` and `FUNCTION RESTORE REPLACE`).
    pub fn store_library(&mut self, lib: FunctionLib) {
        self.remove_library_index(&lib.name);
        for f in &lib.functions {
            self.functions.insert(f.name.clone(), lib.name.clone());
        }
        self.libraries.insert(lib.name.clone(), lib);
    }

    /// Drop a library and its functions from the registry. Returns false when
    /// no library by that name existed (`FUNCTION DELETE`).
    pub fn delete_library(&mut self, name: &str) -> bool {
        let Some(lib) = self.libraries.remove(name) else {
            return false;
        };
        self.remove_library_index(&lib.name);
        true
    }

    fn remove_library_index(&mut self, name: &str) {
        if let Some(lib) = self.libraries.get(name) {
            for f in &lib.functions {
                self.functions.remove(&f.name);
            }
        }
    }

    /// `FUNCTION FLUSH`: drop every library.
    pub fn flush_libraries(&mut self) {
        self.libraries.clear();
        self.functions.clear();
    }

    #[must_use]
    pub fn library(&self, name: &str) -> Option<&FunctionLib> {
        self.libraries.get(name)
    }

    /// Unordered `(name, library)` pairs (`FUNCTION LIST`).
    #[must_use]
    pub fn libraries(&self) -> Vec<(&String, &FunctionLib)> {
        self.libraries.iter().collect()
    }

    /// The library owning `function`, if registered (`FCALL` lookup).
    #[must_use]
    pub fn function_lib(&self, name: &str) -> Option<&FunctionLib> {
        let lib = self.functions.get(name)?;
        self.libraries.get(lib)
    }

    /// Record the function the coordinator is about to run (`FUNCTION STATS`).
    pub fn set_running(&mut self, name: &str, command: String, started_ms: u64) {
        self.running = Some(RunningFunction {
            name: name.to_string(),
            command,
            started_ms,
        });
    }

    pub fn clear_running(&mut self) {
        self.running = None;
    }

    #[must_use]
    pub fn running(&self) -> Option<&RunningFunction> {
        self.running.as_ref()
    }

    /// `FUNCTION KILL`: request the running function to abort. The coordinator
    /// polls this from the `LUA_MASKCOUNT` instruction hook (interrupting even a
    /// CPU-bound tight loop) and between `redis.call` dispatches.
    pub fn request_kill(&self) {
        self.kill.store(true, Ordering::Relaxed);
    }

    pub fn clear_kill(&self) {
        self.kill.store(false, Ordering::Relaxed);
    }

    #[must_use]
    pub fn kill_requested(&self) -> bool {
        self.kill.load(Ordering::Relaxed)
    }

    /// A clone of the kill flag for the coordinator to poll lock-free.
    #[must_use]
    pub fn kill_flag(&self) -> Arc<AtomicBool> {
        self.kill.clone()
    }

    /// Record a script run duration in microseconds (`CallSHA`'s
    /// `RecordCallLatency`).
    pub fn record_latency(&mut self, sha: &str, usec: u64) {
        self.latency
            .entry(sha.to_string())
            .or_default()
            .add(usec as f64);
    }

    /// Per-SHA latency histograms for `SCRIPT LATENCY`.
    #[must_use]
    pub fn latency(&self) -> &HashMap<String, Histogram> {
        &self.latency
    }

    /// FUNCTION DUMP: an opaque binary snapshot of every loaded library. The
    /// restored payload is re-validated by `FUNCTION RESTORE`, so a valid dump
    /// is interchangeable with `FUNCTION LOAD` of its `library_code` values.
    #[must_use]
    pub fn dump_libraries(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"DFYFLIB1");
        out.extend_from_slice(&(self.libraries.len() as u32).to_be_bytes());
        for lib in self.libraries.values() {
            out.extend_from_slice(&(lib.name.len() as u16).to_be_bytes());
            out.extend_from_slice(lib.name.as_bytes());
            let flags = lib.header_flags.join(",");
            out.extend_from_slice(&(flags.len() as u16).to_be_bytes());
            out.extend_from_slice(flags.as_bytes());
            out.extend_from_slice(&(lib.code.len() as u32).to_be_bytes());
            out.extend_from_slice(&lib.code);
        }
        out
    }

    /// FUNCTION RESTORE: decode a [`dump_libraries`] payload. The returned
    /// libraries have an empty `functions` list; the caller re-runs each
    /// `code` through an interpreter (validating the payload like `LOAD`).
    pub fn restore_libraries(data: &[u8]) -> Result<Vec<FunctionLib>, String> {
        const BAD: &str = "Invalid function dump payload";
        fn take<'a>(data: &mut &'a [u8], n: usize) -> Result<&'a [u8], String> {
            if data.len() < n {
                return Err(BAD.into());
            }
            let (head, rest) = data.split_at(n);
            *data = rest;
            Ok(head)
        }
        let mut data = data;
        let magic = take(&mut data, 8).map_err(|_| BAD.to_string())?;
        if magic != b"DFYFLIB1" {
            return Err(BAD.into());
        }
        let n = take(&mut data, 4).map_err(|_| BAD.to_string())?;
        let count = u32::from_be_bytes(n.try_into().map_err(|_| BAD.to_string())?) as usize;
        let mut libs = Vec::with_capacity(count);
        for _ in 0..count {
            let name_len = take(&mut data, 2).map_err(|_| BAD.to_string())?;
            let name_len =
                u16::from_be_bytes(name_len.try_into().map_err(|_| BAD.to_string())?) as usize;
            let name =
                String::from_utf8_lossy(take(&mut data, name_len).map_err(|_| BAD.to_string())?)
                    .into_owned();
            let flags_len = take(&mut data, 2).map_err(|_| BAD.to_string())?;
            let flags_len =
                u16::from_be_bytes(flags_len.try_into().map_err(|_| BAD.to_string())?) as usize;
            let flags =
                String::from_utf8_lossy(take(&mut data, flags_len).map_err(|_| BAD.to_string())?)
                    .into_owned();
            let code_len = take(&mut data, 4).map_err(|_| BAD.to_string())?;
            let code_len =
                u32::from_be_bytes(code_len.try_into().map_err(|_| BAD.to_string())?) as usize;
            let code = take(&mut data, code_len)
                .map_err(|_| BAD.to_string())?
                .to_vec();
            libs.push(FunctionLib {
                name,
                engine: "LUA".into(),
                sha: sha1_hex(&code),
                code,
                header_flags: flags
                    .split(',')
                    .filter(|f| !f.is_empty())
                    .map(str::to_owned)
                    .collect(),
                functions: Vec::new(),
            });
        }
        if !data.is_empty() {
            return Err(BAD.into());
        }
        Ok(libs)
    }
}

fn trim_ascii_start(mut b: &[u8]) -> &[u8] {
    while let [first, rest @ ..] = b {
        if !first.is_ascii_whitespace() {
            break;
        }
        b = rest;
    }
    b
}

// ---------------------------------------------------------------------------
// lua_auto_async: `redis.call` -> `redis.acall` rewrite
// ---------------------------------------------------------------------------

/// Continuation operators: a line ending with one likely continues into the
/// next, so the following `redis.call` is part of an expression
/// (`kContOperators` in `DetectPossibleAsyncCalls`).
const CONT_OPERATORS: &[&[u8]] = &[
    b"+", b"-", b"*", b"/", b"%", b"^", b"#", b"&", b"~", b"|", b"<<", b">>", b"//", b"==", b"~=",
    b"<=", b">=", b"<", b">", b"=", b"(", b"{", b"[", b"::", b":", b",", b".", b"..",
];

/// Continuation tokens: a line ending with one likely continues into the next
/// (`kContTokens`).
const CONT_TOKENS: &[&[u8]] = &[
    b"and", b"else", b"elseif", b"for", b"goto", b"if", b"in", b"local", b"not", b"or", b"repeat",
    b"return", b"until", b"while",
];

/// Rewrite `redis.call`/`redis.pcall` calls whose return value is discarded
/// (standalone statements) into `redis.acall`/`redis.apcall` — a byte-level
/// port of `Interpreter::DetectPossibleAsyncCalls`. Returns `None` when nothing
/// qualifies, or when the body contains a `--[[` block comment (which the
/// reference does not parse and bails on).
///
/// A call is a target when, reading the reference regex
/// `(?:(\\S+)(\\s*--.*?)*\\s*\n|(then)|(do)|(^))\\s*redis\\.(p*call)`:
/// * it is the first thing in the script (`^`),
/// * it is preceded on the same line by `then` or `do` (a block's first
///   statement, where the return value is certainly unused), or
/// * it starts a line whose previous line's last word is not a continuation
///   operator/token (so it is not a multi-line expression).
#[must_use]
pub fn detect_possible_async_calls(body: &[u8]) -> Option<Vec<u8>> {
    // Block comments are not handled by the reference; bail like it does.
    if body.windows(4).any(|w| w == b"--[[") {
        return None;
    }
    let mut targets: Vec<usize> = Vec::new();
    let mut i = 0;
    while i + 6 <= body.len() {
        if &body[i..i + 6] != b"redis." {
            i += 1;
            continue;
        }
        // `redis\.(p*call)`: any run of `p` then `call`.
        let mut call_start = i + 6;
        while call_start < body.len() && body[call_start] == b'p' {
            call_start += 1;
        }
        if call_start + 4 > body.len() || &body[call_start..call_start + 4] != b"call" {
            i += 1;
            continue;
        }
        if is_async_target(body, i) {
            // The reference inserts 'a' at the start of the `(p*call)` group
            // (`it->position(it->size() - 1)`), i.e. before any `p`s.
            targets.push(i + 6);
        }
        i = call_start;
    }
    if targets.is_empty() {
        return None;
    }
    let mut out = body.to_vec();
    // Insert 'a' before 'call'/'pcall', reverse order to preserve positions.
    for pos in targets.into_iter().rev() {
        out.insert(pos, b'a');
    }
    Some(out)
}

/// Whether the `redis.` at `redis_pos` is a rewrite target.
fn is_async_target(body: &[u8], redis_pos: usize) -> bool {
    let line_start = body[..redis_pos]
        .iter()
        .rposition(|&b| b == b'\n')
        .map_or(0, |p| p + 1);
    let at_line_start = body[line_start..redis_pos]
        .iter()
        .all(|&b| b.is_ascii_whitespace());

    // Reference alternative A: a previous line exists and its last word is a
    // valid group-1 match. The operator/token skip applies here.
    if at_line_start && line_start > 0 {
        if let Some(last) = prev_line_last_word(&body[..line_start - 1]) {
            // The reference checks the word's final two bytes, its final byte,
            // and the whole word (`last_n` + `kContTokens.count`).
            if last.len() >= 2 && CONT_OPERATORS.contains(&&last[last.len() - 2..]) {
                return false;
            }
            if CONT_OPERATORS.contains(&&last[last.len() - 1..]) {
                return false;
            }
            if CONT_TOKENS.contains(&last) {
                return false;
            }
            return true;
        }
        // No group-1 word (empty or comment-only previous line): the `then`/`do`
        // alternatives cannot fire for a line-start call either (the word would
        // have matched group A and been skipped above).
        return false;
    }

    // Reference alternative D: the call is the first thing in the script.
    if body[..redis_pos].iter().all(|&b| b.is_ascii_whitespace()) {
        return true;
    }

    // Reference alternatives B/C: `then` or `do` immediately before the call
    // (whitespace may span the call, so this covers `do\n redis.call` too).
    let prefix = &body[..redis_pos];
    let mut end = prefix.len();
    while end > 0 && prefix[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    prefix[..end].ends_with(b"then") || prefix[..end].ends_with(b"do")
}

/// The group-1 word the reference regex captures for a line: the leftmost
/// `\S+` run followed only by whitespace and `--` comments up to the newline.
/// A pure-comment line therefore yields its comment text (matching the regex's
/// behavior when no earlier run qualifies).
fn prev_line_last_word(line: &[u8]) -> Option<&[u8]> {
    let mut i = 0;
    while i < line.len() {
        if line[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        while i < line.len() && !line[i].is_ascii_whitespace() {
            i += 1;
        }
        let rest = trim_ascii_start(&line[i..]);
        if rest.is_empty() || rest.starts_with(b"--") {
            return Some(&line[start..i]);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Function library loading
// ---------------------------------------------------------------------------

/// The registry key holding the `__dfly_functions__` callback table (hidden
/// from scripts: `LUA_REGISTRYINDEX`, not `_G`).
const DFLY_FUNCTIONS: &str = "__dfly_functions__";

/// `redis.register_function` error outside a `FUNCTION LOAD` body
/// (`luaRegisterFunction` when no `loadCtx` is present).
const REGISTER_FUNCTION_ERR: &str =
    "redis.register_function can only be called on FUNCTION LOAD command";

/// Metadata parsed from a library's `#!lua name=<name> [flags=...]` header.
#[derive(Debug)]
pub struct FunctionHeader {
    pub name: String,
    pub header_flags: Vec<String>,
}

/// Parse the `#!lua ...` first line of a FUNCTION LOAD payload. Mirrors
/// `functionLibParseMetaData` (functions.c): the code must start with `#!`,
/// the engine must be `lua`, and `name=` is required.
pub fn parse_function_header(code: &[u8]) -> Result<FunctionHeader, String> {
    let first_line = code.split(|&b| b == b'\n').next().unwrap_or(code);
    let Some(rest) = first_line.strip_prefix(b"#!") else {
        return Err("Missing library metadata".into());
    };
    let mut tokens = rest.split(|&b| b.is_ascii_whitespace());
    let engine = tokens.next().unwrap_or(&[]);
    if engine != b"lua" {
        return Err("Invalid engine type".into());
    }
    let mut name: Option<String> = None;
    let mut header_flags: Vec<String> = Vec::new();
    for tok in tokens {
        if let Some(v) = tok.strip_prefix(b"name=") {
            if v.is_empty() {
                return Err("Missing library name".into());
            }
            name = Some(String::from_utf8_lossy(v).into_owned());
        } else if let Some(v) = tok.strip_prefix(b"flags=") {
            for f in v.split(|&b| b == b',') {
                if !f.is_empty() {
                    header_flags.push(String::from_utf8_lossy(f).into_owned());
                }
            }
        } else if !tok.is_empty() {
            return Err("Invalid metadata".into());
        }
    }
    let Some(name) = name else {
        return Err("Missing library name".into());
    };
    if !is_valid_function_name(&name) {
        return Err("Library names can only contain letters, numbers, '_', '-' and '.'".into());
    }
    // Validate the header flags like SCRIPT FLAGS does.
    let mut params = ScriptParams::default();
    for f in &header_flags {
        params.apply_flags(f)?;
    }
    Ok(FunctionHeader { name, header_flags })
}

/// The bytes of a library payload after its `#!` metadata line (the chunk
/// `FUNCTION LOAD` executes).
#[must_use]
pub fn function_body(code: &[u8]) -> &[u8] {
    match code.iter().position(|&b| b == b'\n') {
        Some(pos) => &code[pos + 1..],
        None => &[],
    }
}

/// Redis' `functionVerifyName`: letters, numbers, `_`, `-` and `.`, matching
/// `LIBRARY_NAMES`/`FUNCTION_NAMES` in functions.c. Used for both library and
/// function names (the two callers emit their own error text).
fn is_valid_function_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

/// Extract `(name, callback, flags)` from a `redis.register_function` call:
/// either `(name, callback)` or the `{function_name=..., callback=...,
/// flags={...}}` table form. Mirrors `luaRegisterFunction` (function_lua.c).
fn register_function_args(args: &MultiValue) -> mlua::Result<(String, Value, Vec<String>)> {
    let two = args.len() == 2 && matches!(&args[0], Value::String(_));
    let one = args.len() == 1 && matches!(&args[0], Value::Table(_));
    if !two && !one {
        return Err(mlua::Error::runtime("wrong number or type of arguments"));
    }
    let (name, callback, flags) = if two {
        let Value::String(name) = &args[0] else {
            unreachable!()
        };
        if !matches!(&args[1], Value::Function(_)) {
            return Err(mlua::Error::runtime("Function callback must be a function"));
        }
        (
            String::from_utf8_lossy(&name.as_bytes()).into_owned(),
            args[1].clone(),
            Vec::new(),
        )
    } else {
        let Value::Table(t) = &args[0] else {
            unreachable!()
        };
        let name = match t.raw_get::<Value>("function_name")? {
            Value::String(name) => String::from_utf8_lossy(&name.as_bytes()).into_owned(),
            _ => return Err(mlua::Error::runtime("Function name must be a string")),
        };
        let callback = match t.raw_get::<Value>("callback")? {
            Value::Function(f) => Value::Function(f),
            _ => return Err(mlua::Error::runtime("Function callback must be a function")),
        };
        let mut flags = Vec::new();
        match t.raw_get::<Value>("flags")? {
            Value::Nil => {}
            Value::Table(ft) => {
                for v in ft.sequence_values::<Value>() {
                    let Value::String(s) = v? else {
                        return Err(mlua::Error::runtime(
                            "Function flags must be a table of strings",
                        ));
                    };
                    flags.push(String::from_utf8_lossy(&s.as_bytes()).into_owned());
                }
            }
            _ => return Err(mlua::Error::runtime("Function flags must be a table")),
        }
        (name, callback, flags)
    };
    if !is_valid_function_name(&name) {
        return Err(mlua::Error::runtime(
            "Function names can only contain letters, numbers, '_', '-' and '.'",
        ));
    }
    Ok((name, callback, flags))
}

// ---------------------------------------------------------------------------
// Sandboxed interpreter
// ---------------------------------------------------------------------------

/// Error handler defined before the strict globals chunk, so it keeps access to
/// the debug library after `debug` is nilled (`@err_handler_def`).
const ERR_HANDLER: &str = "local dbg = debug\n\
function __redis__err__handler(err)\n\
  local i = dbg.getinfo(2,'nSl')\n\
  if i and i.what == 'C' then\n\
    i = dbg.getinfo(3,'nSl')\n\
  end\n\
  if i then\n\
    return i.source .. ':' .. i.currentline .. ': ' .. err\n\
  else\n\
    return err\n\
  end\n\
end\n";

/// The strict-global enforcement chunk from `interpreter.cc:453` (`@enable_strictlua`).
const STRICT: &str = r#"
local dbg=debug
local mt = {}
local _orig_rawset=rawset
setmetatable(_G, mt)
mt.__newindex = function (t, n, v)
  if dbg.getinfo(2) then
    local w = dbg.getinfo(2, "S").what
    if w ~= "main" and w ~= "C" then
      error("Script attempted to create global variable '"..tostring(n).."'", 2)
    end
  end
  _orig_rawset(t, n, v)
end
mt.__index = function (t, n)
  if dbg.getinfo(2) and dbg.getinfo(2, "S").what ~= "C" then
    error("Script attempted to access nonexistent global variable '"..tostring(n).."'", 2)
  end
  return rawget(t, n)
end
local _orig_load = load
load = function(chunk, chunkname, mode, env)
  return _orig_load(chunk, chunkname, "t", env)
end
local _orig_getmetatable = getmetatable
getmetatable = function(t)
  if t == _G then
    error("Script attempted to access metatable of global environment", 2)
  end
  return _orig_getmetatable(t)
end
debug = nil
local global_guard = {}
global_guard.__metatable = "Script attempted to access metatable of global table"
for _, v in pairs(_G) do
  if type(v) == "table" and v ~= _G then
    setmetatable(v, global_guard)
  end
end
"#;

/// Runner used in place of the reference's `lua_pcall(..., errh)` error handler.
/// It forwards the script result, or re-raises the errh-formatted error so
/// `Function::call` surfaces `@user_script:<line>: <message>`.
const RUNNER: &str = "function __dfly__run(sha)\n\
  local fn = _G['f_' .. sha]\n\
  local ok, res = xpcall(fn, __redis__err__handler)\n\
  if ok then\n\
    return res\n\
  end\n\
  error(res, 0)\n\
end\n";

/// Wrap `body` exactly like the reference's `AddInternal`:
/// `function f_<sha>() \n <body> \n end`. The body's first line is chunk line 2,
/// so error positions read `@user_script:<body_line + 1>: <message>`, and the
/// trailing newline keeps a terminal `--` comment from swallowing the `end`.
fn script_chunk(sha: &str, body: &[u8]) -> Vec<u8> {
    let mut chunk = Vec::with_capacity(body.len() + 32);
    chunk.extend_from_slice(format!("function f_{sha}() \n").as_bytes());
    chunk.extend_from_slice(body);
    chunk.extend_from_slice(b"\nend");
    chunk
}

/// Compile-check `body` in a throwaway Lua state without executing it. SCRIPT
/// LOAD uses this on the IO thread so a compile error never enters the cache
/// (`ScriptMgr::Insert`). A full `SandboxedInterpreter` is built so parsing runs
/// under the identical strict environment as EVAL (polyfills, protected globals,
/// `redis.*` table), not a bare state.
pub fn compile_check(body: &[u8]) -> Result<(), String> {
    let interp = SandboxedInterpreter::new()?;
    interp
        .lua
        .load(script_chunk("check", body))
        .set_name("@user_script")
        .set_mode(ChunkMode::Text)
        .exec()
        .map_err(|e| e.to_string())
}

/// Lua 5.1 compat helpers (`register_polyfills` in `interpreter_polyfill.h`).
const POLYFILLS: &str = "unpack = table.unpack\n\
table.getn = function(t) return #t end\n\
table.setn = function() error('setn is obsolete') end\n\
function table.foreach(t, f)\n\
  for k, v in pairs(t) do\n\
    local r = f(k, v)\n\
    if r ~= nil then return r end\n\
  end\n\
end\n\
function table.foreachi(t, f)\n\
  for i = 1, #t do\n\
    local r = f(i, t[i])\n\
    if r ~= nil then return r end\n\
  end\n\
end\n";

/// A sandboxed Lua 5.4 state with the strict-global environment installed.
/// Owned by a single thread; scripts run through [`run`](Self::run).
pub struct SandboxedInterpreter {
    lua: Lua,
}

impl SandboxedInterpreter {
    /// Create a state and run the full bootstrap exactly once, with
    /// `--lua_enable_redis_log` off.
    pub fn new() -> Result<Self, String> {
        Self::with_redis_log(false)
    }

    /// Create a state with the `--lua_enable_redis_log` flag applied to
    /// `redis.log` (`FLAGS_lua_enable_redis_log`).
    pub fn with_redis_log(enable_redis_log: bool) -> Result<Self, String> {
        // The reference loads the base/table/string/math/debug libraries only.
        // StdLib::DEBUG trips mlua's safety check, so this must go through the
        // unsafe constructor; the sandbox hides `debug` from scripts anyway.
        let lua = unsafe {
            Lua::unsafe_new_with(
                StdLib::TABLE | StdLib::STRING | StdLib::MATH | StdLib::DEBUG,
                mlua::LuaOptions::new(),
            )
        };
        let interp = SandboxedInterpreter { lua };
        interp.bootstrap(enable_redis_log)?;
        Ok(interp)
    }

    fn bootstrap(&self, enable_redis_log: bool) -> Result<(), String> {
        self.exec(ERR_HANDLER, "@err_handler_def")?;
        self.exec(STRICT, "@enable_strictlua")?;
        self.install_protected_funcs()?;
        // `loadfile`/`dofile` are disabled (`interpreter.cc:512`).
        self.lua
            .globals()
            .set("loadfile", Value::Nil)
            .map_err(|e| e.to_string())?;
        self.lua
            .globals()
            .set("dofile", Value::Nil)
            .map_err(|e| e.to_string())?;
        self.exec(POLYFILLS, "@dfly_polyfills")?;
        lua_libs::install_all(&self.lua).map_err(|e| e.to_string())?;
        self.exec(RUNNER, "@dfly_runner")?;
        self.setup_redis_table(enable_redis_log)?;
        self.setup_dragonfly_table()?;
        self.setup_function_table()?;
        Ok(())
    }

    /// Force a full Lua GC cycle, returning bytes freed (`Interpreter::RunGC`).
    /// The reference measures in KiB via `LUA_GCCOUNT`; the port uses mlua's
    /// byte-accurate `used_memory` instead, so this only mirrors the behavior
    /// (free unused objects), not the exact figure.
    #[must_use]
    pub fn run_gc(&self) -> i64 {
        let before = self.lua.used_memory();
        let _ = self.lua.gc_collect();
        let after = self.lua.used_memory();
        i64::try_from(before.saturating_sub(after)).unwrap_or(i64::MAX)
    }

    /// The `__dfly_functions__` table: every registered function callback, keyed
    /// by function name, created when a library is (re)loaded. It lives in the
    /// Lua registry rather than `_G` so scripts cannot reach it (Redis never
    /// exposes functions to scripts; direct access would bypass FCALL's locking,
    /// `no-writes` and undeclared-keys enforcement).
    fn setup_function_table(&self) -> Result<(), String> {
        let t = self.lua.create_table().map_err(|e| e.to_string())?;
        self.lua
            .set_named_registry_value(DFLY_FUNCTIONS, t)
            .map_err(|e| e.to_string())
    }

    fn exec(&self, code: &str, name: &str) -> Result<(), String> {
        self.lua
            .load(code)
            .set_name(name)
            .set_mode(ChunkMode::Text)
            .exec()
            .map_err(|e| e.to_string())
    }

    /// `rawset`/`setmetatable` wrappers blocking writes to _G, its metatable or
    /// any global table (`ProtectedRawset`/`ProtectedSetmetatable`).
    fn install_protected_funcs(&self) -> Result<(), String> {
        let rawset = self
            .lua
            .create_function(|lua, args: MultiValue| -> mlua::Result<Value> {
                if args.len() != 3 || !matches!(&args[0], Value::Table(_)) {
                    return Err(mlua::Error::runtime(
                        "rawset requires a table and two arguments",
                    ));
                }
                let Value::Table(t) = &args[0] else {
                    unreachable!()
                };
                if is_global_table_or_metatable(lua, &args[0])? {
                    return Err(mlua::Error::runtime(
                        "Script attempted to access rawset with global environment",
                    ));
                }
                t.raw_set(args[1].clone(), args[2].clone())?;
                Ok(Value::Table(t.clone()))
            })
            .map_err(|e| e.to_string())?;
        let setmetatable = self
            .lua
            .create_function(|lua, args: MultiValue| -> mlua::Result<Value> {
                if args.len() != 2 || !matches!(&args[0], Value::Table(_)) {
                    return Err(mlua::Error::runtime(
                        "setmetatable requires a table and one argument",
                    ));
                }
                let Value::Table(t) = &args[0] else {
                    unreachable!()
                };
                if is_global_table_or_metatable(lua, &args[0])? {
                    return Err(mlua::Error::runtime(
                        "Script attempted to set metatable of global environment",
                    ));
                }
                let mt = match &args[1] {
                    Value::Nil => None,
                    Value::Table(m) => Some(m.clone()),
                    _ => return Err(mlua::Error::runtime("nil or table expected")),
                };
                t.set_metatable(mt)?;
                Ok(Value::Table(t.clone()))
            })
            .map_err(|e| e.to_string())?;
        let globals = self.lua.globals();
        globals.set("rawset", rawset).map_err(|e| e.to_string())?;
        globals
            .set("setmetatable", setmetatable)
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// The `redis.*` table with the always-static helpers. `call`/`pcall` are
    /// (re)installed per run with the dispatch context.
    fn setup_redis_table(&self, enable_redis_log: bool) -> Result<(), String> {
        let t = self.lua.create_table().map_err(|e| e.to_string())?;

        let sha1hex = self
            .lua
            .create_function(|lua, args: MultiValue| -> mlua::Result<Value> {
                if args.len() != 1 {
                    raise_string_error(lua, "wrong number of arguments".into())
                }
                // `lua_tolstring` coerces numbers to their string form
                // (`RedisSha1Command`); other types are rejected (NULL in the
                // reference, i.e. undefined).
                let bytes = match &args[0] {
                    Value::String(s) => s.as_bytes().to_vec(),
                    Value::Integer(i) => itoa(*i),
                    Value::Number(f) => lua_tolstring(*f).into_bytes(),
                    _ => raise_string_error(lua, "wrong number or type of arguments".into()),
                };
                Ok(Value::String(lua.create_string(sha1_hex(&bytes))?))
            })
            .map_err(|e| e.to_string())?;
        t.raw_set("sha1hex", sha1hex).map_err(|e| e.to_string())?;

        let error_reply = self
            .lua
            .create_function(|lua, args: MultiValue| single_field_table(lua, "err", &args))
            .map_err(|e| e.to_string())?;
        let status_reply = self
            .lua
            .create_function(|lua, args: MultiValue| single_field_table(lua, "ok", &args))
            .map_err(|e| e.to_string())?;
        t.raw_set("error_reply", error_reply)
            .map_err(|e| e.to_string())?;
        t.raw_set("status_reply", status_reply)
            .map_err(|e| e.to_string())?;

        let replicate = self
            .lua
            .create_function(|_, (): ()| -> mlua::Result<i64> { Ok(1) })
            .map_err(|e| e.to_string())?;
        t.raw_set("replicate_commands", replicate)
            .map_err(|e| e.to_string())?;

        let log = self
            .lua
            .create_function(move |lua, args: MultiValue| -> mlua::Result<Value> {
                if args.len() < 2 {
                    raise_string_error(lua, "redis.log() requires two arguments or more.".into())
                }
                let level: f64 = match args[0] {
                    Value::Integer(i) => i as f64,
                    Value::Number(f) => f,
                    _ => raise_string_error(
                        lua,
                        "First argument must be a number (log level).".into(),
                    ),
                };
                // `RedisLogCommand`: with `--lua_enable_redis_log` off the call
                // is a silent no-op; when on, the level must be a valid
                // `LOG_DEBUG`..`LOG_WARNING` and the message is emitted.
                if enable_redis_log {
                    let level = level as i64;
                    if !(0..=3).contains(&level) {
                        raise_string_error(lua, "Invalid log level.".into())
                    }
                    let msg = args
                        .iter()
                        .skip(1)
                        .filter_map(|a| match a {
                            Value::String(s) => s.to_str().ok().as_deref().map(str::to_owned),
                            Value::Integer(i) => {
                                Some(String::from_utf8_lossy(&itoa(*i)).into_owned())
                            }
                            Value::Number(f) => Some(format_lua_float(*f)),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    eprintln!("lua[{level}] {msg}");
                }
                Ok(Value::Nil)
            })
            .map_err(|e| e.to_string())?;
        t.raw_set("log", log).map_err(|e| e.to_string())?;

        // `register_function` is only legal inside a `FUNCTION LOAD` body;
        // anything else gets a clear error (see `load_function_lib`).
        t.raw_set(
            "register_function",
            register_function_blocker(&self.lua).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;

        t.raw_set("LOG_DEBUG", 0).map_err(|e| e.to_string())?;
        t.raw_set("LOG_VERBOSE", 1).map_err(|e| e.to_string())?;
        t.raw_set("LOG_NOTICE", 2).map_err(|e| e.to_string())?;
        t.raw_set("LOG_WARNING", 3).map_err(|e| e.to_string())?;

        self.lua
            .globals()
            .set("redis", t)
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// The `dragonfly.*` global table (`Interpreter`'s constructor registers
    /// `ihash`, `randstr`, `lock` and `unlock`). `randstr` is static; `ihash`,
    /// `lock` and `unlock` close over the per-run dispatch context and are
    /// (re)installed like `redis.call`/`redis.pcall`.
    fn setup_dragonfly_table(&self) -> Result<(), String> {
        let t = self.lua.create_table().map_err(|e| e.to_string())?;
        let randstr = self
            .lua
            .create_function(|lua, args: MultiValue| -> mlua::Result<Value> {
                dragonfly_randstr(lua, &args)
            })
            .map_err(|e| e.to_string())?;
        t.raw_set("randstr", randstr).map_err(|e| e.to_string())?;
        self.lua
            .globals()
            .set("dragonfly", t)
            .map_err(|e| e.to_string())
    }

    /// Install a global string array (KEYS / ARGV), like
    /// `SetGlobalArrayInternal`.
    pub fn set_global_array(&self, name: &str, vals: &[Vec<u8>]) -> Result<(), String> {
        let t = self.lua.create_table().map_err(|e| e.to_string())?;
        for (i, v) in vals.iter().enumerate() {
            let s = self.lua.create_string(v).map_err(|e| e.to_string())?;
            t.raw_set(i + 1, s).map_err(|e| e.to_string())?;
        }
        self.lua.globals().set(name, t).map_err(|e| e.to_string())
    }

    /// Compile `body` as `function f_<sha>() ... end` under the `@user_script`
    /// chunk name, so compile errors read `@user_script:<line>: <message>`
    /// (`AddInternal`/`AddFunction`). Returns the compile error string on
    /// failure.
    pub fn define(&self, sha: &str, body: &[u8]) -> Result<(), String> {
        self.lua
            .load(script_chunk(sha, body))
            .set_name("@user_script")
            .set_mode(ChunkMode::Text)
            .exec()
            .map_err(|e| e.to_string())
    }

    /// Run the script for `sha`, dispatching `redis.call`/`redis.pcall`
    /// through `dispatch`, and serialize the first return value.
    pub fn run<D: ScriptDispatch>(
        &self,
        sha: &str,
        dispatch: &mut D,
        float_as_int: bool,
        kill: &Arc<AtomicBool>,
    ) -> Result<RespValue, String> {
        let lua = &self.lua;
        let cell = RefCell::new(&mut *dispatch);
        let dispatch = &cell;
        let result = lua.scope(|scope| {
            let call = scope.create_function_mut(move |lua, args: MultiValue| {
                let Ok(cmd_args) = prepare_args(&args) else {
                    raise_string_error(lua, ARG_TYPE_ERR.into())
                };
                // Drop the RefCell guard before raising: `lua_error` longjmps
                // past this frame, skipping its destructor.
                let dispatched = {
                    let mut d = dispatch.borrow_mut();
                    d.dispatch(cmd_args)
                };
                match dispatched {
                    Ok(v) => resp_to_lua(lua, v),
                    Err(msg) => raise_string_error(lua, msg),
                }
            })?;
            let pcall = scope.create_function_mut(move |lua, args: MultiValue| {
                let cmd_args = prepare_args(&args)?;
                match dispatch.borrow_mut().dispatch(cmd_args) {
                    Ok(v) => resp_to_lua(lua, v),
                    Err(msg) => {
                        let t = lua.create_table()?;
                        t.raw_set("err", lua.create_string(msg.as_bytes())?)?;
                        Ok(Value::Table(t))
                    }
                }
            })?;
            // `redis.acall`/`redis.apcall`: enqueue for batched execution and
            // return nil, like the reference's `RedisACallCommand`. acall
            // raises abort errors, apcall suppresses per-command errors (the
            // dispatch decides which are fatal).
            let acall = scope.create_function_mut(move |lua, args: MultiValue| {
                let Ok(cmd_args) = prepare_args(&args) else {
                    raise_string_error(lua, ARG_TYPE_ERR.into())
                };
                let dispatched = {
                    let mut d = dispatch.borrow_mut();
                    d.dispatch_async(cmd_args, true)
                };
                match dispatched {
                    Ok(()) => Ok(Value::Nil),
                    Err(msg) => raise_string_error(lua, msg),
                }
            })?;
            let apcall = scope.create_function_mut(move |lua, args: MultiValue| {
                let Ok(cmd_args) = prepare_args(&args) else {
                    raise_string_error(lua, ARG_TYPE_ERR.into())
                };
                let dispatched = {
                    let mut d = dispatch.borrow_mut();
                    d.dispatch_async(cmd_args, false)
                };
                match dispatched {
                    Ok(()) => Ok(Value::Nil),
                    Err(msg) => raise_string_error(lua, msg),
                }
            })?;
            let redis: Table = lua.globals().get("redis")?;
            redis.set("call", call)?;
            redis.set("pcall", pcall)?;
            redis.set("acall", acall)?;
            redis.set("apcall", apcall)?;
            install_dragonfly_functions(lua, scope, dispatch)?;
            let runner: Function = lua.globals().get("__dfly__run")?;
            let v: Value = with_kill_hook(lua, kill, || runner.call(sha))?;
            serialize_value(v, float_as_int, 0)
        });
        result.map_err(|e| clean_script_error(&e.to_string()))
    }

    /// Execute a FUNCTION library payload's body, collecting every
    /// `redis.register_function` call and storing the callbacks in the
    /// `__dfly_functions__` registry table (keyed by function name). `redis.call`
    /// and `redis.pcall` are unavailable during a load, mirroring Redis.
    pub fn load_function_lib(&self, code: &[u8]) -> Result<Vec<FunctionInfo>, String> {
        let body = function_body(code);
        let lua = &self.lua;
        let collected = RefCell::new(Vec::new());
        let result = lua.scope(|scope| {
            let collected = &collected;
            let register =
                scope.create_function_mut(move |lua, args: MultiValue| -> mlua::Result<Value> {
                    let (name, callback, flags) = register_function_args(&args)?;
                    collected.borrow_mut().push(FunctionInfo {
                        name: name.clone(),
                        flags,
                    });
                    let funcs: Table = lua.named_registry_value(DFLY_FUNCTIONS)?;
                    funcs.raw_set(name, callback)?;
                    Ok(Value::Nil)
                })?;
            let no_call = scope.create_function(|_, _: MultiValue| -> mlua::Result<Value> {
                Err(mlua::Error::runtime(
                    "redis.call is not allowed during function library load",
                ))
            })?;
            let redis: Table = lua.globals().get("redis")?;
            redis.set("register_function", register)?;
            redis.set("call", no_call.clone())?;
            redis.set("pcall", no_call.clone())?;
            redis.set("acall", no_call.clone())?;
            redis.set("apcall", no_call.clone())?;
            // Overwrite any per-run `dragonfly.*` closures (which borrow a
            // dispatch context) so a library body cannot reach them.
            let dragonfly: Table = lua.globals().get("dragonfly")?;
            dragonfly.set("ihash", no_call.clone())?;
            dragonfly.set("lock", no_call.clone())?;
            dragonfly.set("unlock", no_call)?;
            let res = lua
                .load(body)
                .set_name("@user_function")
                .set_mode(ChunkMode::Text)
                .exec();
            // Restore the idle `register_function` (an execution-time error)
            // before the load-only collector goes out of scope.
            redis.set("register_function", register_function_blocker(lua)?)?;
            res?;
            Ok(collected.borrow().clone())
        });
        result.map_err(|e| clean_script_error(&e.to_string()))
    }

    /// Drop the callbacks for `names` from `__dfly_functions__`, purging stale
    /// entries when a library is redefined (`FUNCTION LOAD REPLACE`) or removed.
    /// The caller must only pass names no longer registered to any library.
    pub fn purge_functions(&self, names: &[String]) {
        let Ok(funcs) = self.lua.named_registry_value::<Table>(DFLY_FUNCTIONS) else {
            return;
        };
        for name in names {
            let _ = funcs.raw_remove(name.clone());
        }
    }

    /// Run a registered function with `keys`/`argv` as its two arguments and
    /// `redis.call`/`redis.pcall` dispatching through `dispatch`, mirroring
    /// `FunctionRunner`/`FunctionLibInvoke` plus the EVAL path above.
    pub fn run_function<D: ScriptDispatch>(
        &self,
        name: &str,
        keys: &[Vec<u8>],
        argv: &[Vec<u8>],
        dispatch: &mut D,
        float_as_int: bool,
        kill: &Arc<AtomicBool>,
    ) -> Result<RespValue, String> {
        let lua = &self.lua;
        let cell = RefCell::new(&mut *dispatch);
        let dispatch = &cell;
        let result = lua.scope(|scope| {
            let call = scope.create_function_mut(move |lua, args: MultiValue| {
                let Ok(cmd_args) = prepare_args(&args) else {
                    raise_string_error(lua, ARG_TYPE_ERR.into())
                };
                let dispatched = {
                    let mut d = dispatch.borrow_mut();
                    d.dispatch(cmd_args)
                };
                match dispatched {
                    Ok(v) => resp_to_lua(lua, v),
                    Err(msg) => raise_string_error(lua, msg),
                }
            })?;
            let pcall = scope.create_function_mut(move |lua, args: MultiValue| {
                let cmd_args = prepare_args(&args)?;
                match dispatch.borrow_mut().dispatch(cmd_args) {
                    Ok(v) => resp_to_lua(lua, v),
                    Err(msg) => {
                        let t = lua.create_table()?;
                        t.raw_set("err", lua.create_string(msg.as_bytes())?)?;
                        Ok(Value::Table(t))
                    }
                }
            })?;
            // `redis.acall`/`redis.apcall`: enqueue for batched execution and
            // return nil, like the reference's `RedisACallCommand`.
            let acall = scope.create_function_mut(move |lua, args: MultiValue| {
                let Ok(cmd_args) = prepare_args(&args) else {
                    raise_string_error(lua, ARG_TYPE_ERR.into())
                };
                let dispatched = {
                    let mut d = dispatch.borrow_mut();
                    d.dispatch_async(cmd_args, true)
                };
                match dispatched {
                    Ok(()) => Ok(Value::Nil),
                    Err(msg) => raise_string_error(lua, msg),
                }
            })?;
            let apcall = scope.create_function_mut(move |lua, args: MultiValue| {
                let Ok(cmd_args) = prepare_args(&args) else {
                    raise_string_error(lua, ARG_TYPE_ERR.into())
                };
                let dispatched = {
                    let mut d = dispatch.borrow_mut();
                    d.dispatch_async(cmd_args, false)
                };
                match dispatched {
                    Ok(()) => Ok(Value::Nil),
                    Err(msg) => raise_string_error(lua, msg),
                }
            })?;
            let redis: Table = lua.globals().get("redis")?;
            redis.set("call", call)?;
            redis.set("pcall", pcall)?;
            redis.set("acall", acall)?;
            redis.set("apcall", apcall)?;
            install_dragonfly_functions(lua, scope, dispatch)?;
            let funcs: Table = lua.named_registry_value(DFLY_FUNCTIONS)?;
            let f: Function = funcs.get(name)?;
            let keys_t = lua.create_table()?;
            for (i, k) in keys.iter().enumerate() {
                keys_t.raw_set(i + 1, lua.create_string(k)?)?;
            }
            let args_t = lua.create_table()?;
            for (i, a) in argv.iter().enumerate() {
                args_t.raw_set(i + 1, lua.create_string(a)?)?;
            }
            let v: Value = with_kill_hook(lua, kill, || f.call((keys_t, args_t)))?;
            serialize_value(v, float_as_int, 0)
        });
        result.map_err(|e| clean_script_error(&e.to_string()))
    }
}

/// Raise `msg` as a plain Lua string error so Lua-side error handlers (our
/// `__redis__err__handler`) can format it. Returning `Err(Error::runtime(_))`
/// from a callback instead would propagate a typed mlua error (a userdata) to
/// `xpcall`, which the handler cannot concatenate. `CallFromScript` pushes the
/// reply error string and calls `lua_error`, so this mirrors the reference.
/// `lua_pushlstring` copies the bytes into Lua memory before `msg` is freed.
pub(crate) fn raise_string_error(lua: &Lua, msg: String) -> ! {
    lua.exec_raw_lua(|raw| unsafe {
        let len = msg.len();
        ffi::lua_pushlstring(raw.state(), msg.as_ptr().cast::<c_char>(), len);
        std::mem::drop(msg);
        ffi::lua_error(raw.state())
    })
}

/// The idle `redis.register_function`: an owned closure (not scoped) that always
/// errors, installed at bootstrap and restored after every library load so
/// scripts/functions get a clear message instead of a nil-call.
fn register_function_blocker(lua: &Lua) -> mlua::Result<Function> {
    lua.create_function(|lua, _: MultiValue| -> mlua::Result<Value> {
        raise_string_error(lua, REGISTER_FUNCTION_ERR.into())
    })
}

/// Strip the mlua-specific error decorations so script errors read like the
/// reference's (`@user_script:<line>: <message>`): mlua prefixes `runtime
/// error:` and appends a `stack traceback:` block.
fn clean_script_error(msg: &str) -> String {
    let msg = msg.strip_prefix("runtime error: ").unwrap_or(msg);
    msg.split("\nstack traceback:")
        .next()
        .unwrap_or(msg)
        .to_string()
}

/// Run `f` under Redis's `LUA_MASKCOUNT` kill hook: every `KILL_HOOK_INTERVAL`
/// VM instructions the hook checks `kill` and, when set, raises
/// `FUNCTION_KILLED_ERR` (aborting the running script exactly like `lua_error`
/// from the reference's count hook). The hook is removed unconditionally
/// afterwards.
fn with_kill_hook<T>(
    lua: &Lua,
    kill: &Arc<AtomicBool>,
    f: impl FnOnce() -> mlua::Result<T>,
) -> mlua::Result<T> {
    lua.set_hook(
        HookTriggers::new().every_nth_instruction(KILL_HOOK_INTERVAL),
        {
            let kill = Arc::clone(kill);
            move |lua, _| {
                if kill.load(Ordering::Relaxed) {
                    // The reference's `luaMaskCountHook` calls `lua_error` with
                    // a plain string; returning a typed mlua error instead would
                    // surface to the script's error handler as an
                    // unconcatenatable userdata.
                    lua.exec_raw_lua(|raw| unsafe {
                        ffi::lua_pushlstring(
                            raw.state(),
                            FUNCTION_KILLED_ERR.as_ptr().cast::<c_char>(),
                            FUNCTION_KILLED_ERR.len(),
                        );
                        ffi::lua_error(raw.state());
                    });
                }
                Ok(VmState::Continue)
            }
        },
    )?;
    let result = f();
    lua.remove_hook();
    result
}

/// True when `v` is `_G`, `_G`'s metatable, or any table stored as a global
/// value (`IsGlobalTableOrMetatable`).
fn is_global_table_or_metatable(lua: &Lua, v: &Value) -> mlua::Result<bool> {
    let Value::Table(t) = v else { return Ok(false) };
    let globals = lua.globals();
    if globals == *t {
        return Ok(true);
    }
    if let Some(mt) = globals.metatable()
        && mt == *t
    {
        return Ok(true);
    }
    for pair in globals.pairs::<Value, Value>() {
        let (_k, val) = pair?;
        if let Value::Table(gt) = val
            && gt == *t
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `SingleFieldTable`: build `{field = <string>}`, erroring with a `{err=...}`
/// table on bad arguments (the reference returns the table, not a raise). The
/// error carries the `source:line:` trace prefix of `PushError` (interpreter.cc).
fn single_field_table(lua: &Lua, field: &str, args: &MultiValue) -> mlua::Result<Value> {
    if args.len() != 1 || !matches!(&args[0], Value::String(_)) {
        let prefix = lua
            .inspect_stack(1, |dbg| {
                let src = dbg.source();
                let source = src.source.as_deref().unwrap_or("");
                let line = dbg.current_line().unwrap_or(0);
                format!("{source}:{line}: ")
            })
            .unwrap_or_default();
        let t = lua.create_table()?;
        t.raw_set(
            "err",
            lua.create_string(format!("{prefix}wrong number or type of arguments"))?,
        )?;
        return Ok(Value::Table(t));
    }
    let t = lua.create_table()?;
    t.raw_set(field, args[0].clone())?;
    Ok(Value::Table(t))
}

/// Synchronous execution of a `redis.call`/`redis.pcall` subcommand during a
/// script. `Err` becomes a raised error for `call` and a `{err=...}` table for
/// `pcall`.
pub trait ScriptDispatch {
    fn dispatch(&mut self, args: Vec<Vec<u8>>) -> Result<RespValue, String>;

    /// Enqueue a subcommand for batched execution (`redis.acall`/`redis.apcall`),
    /// mirroring `TryEnqueueEvalAsyncCmd`. `abort_on_error` (acall) makes an
    /// unknown command fatal; apcall drops it silently. `Err` is only for
    /// errors that must abort the script (an unknown command under acall, or a
    /// flush error) — plain command errors are deferred to the flush.
    fn dispatch_async(&mut self, args: Vec<Vec<u8>>, abort_on_error: bool) -> Result<(), String> {
        let _ = (args, abort_on_error);
        Ok(())
    }

    /// Execute the pending async batch (`FlushEvalAsyncCmds`). Called after the
    /// script body finishes, and by `dispatch` when a synchronous call flushes
    /// the buffer. `Err` aborts the run.
    fn flush(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// `dragonfly.lock(keys...)`: transactionally lock the keys' shards until
    /// `dragonfly.unlock()` or the end of the run (`DragonflyLockCommand`). A
    /// no-op in atomic mode, like the reference's early return for a
    /// non-NON_ATOMIC transaction.
    fn lock(&mut self, keys: Vec<Vec<u8>>) -> Result<(), String> {
        let _ = keys;
        Ok(())
    }

    /// `dragonfly.unlock()`: release every lock held by the script's
    /// transaction and continue non-atomically (`DragonflyUnlockCommand`).
    fn unlock(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// Convert `redis.call` args to command argument bytes
/// (`Interpreter::PrepareArgs`). Only strings and numbers are accepted.
fn prepare_args(args: &MultiValue) -> mlua::Result<Vec<Vec<u8>>> {
    let mut out = Vec::with_capacity(args.len());
    for v in args {
        match v {
            Value::String(s) => out.push(s.as_bytes().to_vec()),
            Value::Integer(i) => out.push(itoa(*i)),
            Value::Number(d) => out.push(format_lua_float(*d).into_bytes()),
            _ => return Err(mlua::Error::runtime(ARG_TYPE_ERR)),
        }
    }
    Ok(out)
}

/// `absl::AlphaNum(double)`: `%.6g` formatting (SixDigitsToBuffer), used by
/// `StringCollectorTranslator::OnDouble` for `dragonfly.ihash` reply strings.
fn g6_format(d: f64) -> String {
    let mut buf = [0u8; 64];
    let len = unsafe { libc::snprintf(buf.as_mut_ptr().cast(), buf.len(), c"%.6g".as_ptr(), d) };
    let len = if len < 0 {
        0
    } else {
        (len as usize).min(buf.len())
    };
    String::from_utf8_lossy(&buf[..len]).into_owned()
}

/// Flatten a subcommand reply into the strings `StringCollectorTranslator`
/// (interpreter.cc:147) would emit for `dragonfly.ihash`. Errors are logged
/// and contribute nothing, nil maps to the empty string, and maps visit key
/// then value.
fn collect_reply_bytes(values: &mut Vec<Vec<u8>>, r: &RespValue) {
    match r {
        RespValue::Nil => values.push(Vec::new()),
        RespValue::NilArray => values.push(Vec::new()),
        RespValue::Bool(b) => values.push(if *b { b"1".to_vec() } else { b"0".to_vec() }),
        RespValue::Integer(i) => values.push(itoa(*i)),
        RespValue::Double(d) => values.push(g6_format(*d).into_bytes()),
        RespValue::Simple(s) => values.push(s.as_bytes().to_vec()),
        RespValue::Error(_) => {}
        RespValue::Bulk(b) => values.push(b.clone()),
        RespValue::Array(items) => items.iter().for_each(|v| collect_reply_bytes(values, v)),
        RespValue::Map(pairs) => pairs.iter().for_each(|(k, v)| {
            collect_reply_bytes(values, k);
            collect_reply_bytes(values, v);
        }),
    }
}

/// `DragonflyRandstrCommand` (interpreter.cc:569): generate a random string of
/// `size` bytes, or a table of `count` of them. Bytes follow glibc `rand()`
/// (an LCG with seed 1, `rand()` returning `(state >> 16) & 0x7fff`) and the
/// repeating `DRAGONFLY` pattern, so output matches the reference byte-for-byte.
fn dragonfly_randstr(lua: &Lua, args: &MultiValue) -> mlua::Result<Value> {
    const K_MAX_RANDSTR_SIZE: i64 = 16 << 20;
    const K_MAX_RANDSTR_COUNT: i64 = 32 << 10;
    const ALPHANUM: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    const PATTERN: &[u8] = b"DRAGONFLY";
    const PATTERN_LEN: i64 = PATTERN.len() as i64;
    const PATTERN_INTERVAL: i64 = 53;

    let argc = args.len();
    if !(1..=2).contains(&argc) || !matches!(&args[0], Value::Integer(_) | Value::Number(_)) {
        raise_string_error(
            lua,
            "randstr: expected randstr(size) or randstr(size, count)".into(),
        )
    }
    let dsize = match &args[0] {
        Value::Integer(i) => *i,
        Value::Number(f) => *f as i64,
        _ => 0,
    };
    if !(1..=K_MAX_RANDSTR_SIZE).contains(&dsize) {
        raise_string_error(
            lua,
            format!("randstr: size must be between 1 and {K_MAX_RANDSTR_SIZE}"),
        )
    }
    let count = if argc == 2 {
        let c = match &args[1] {
            Value::Integer(i) => *i,
            Value::Number(f) => *f as i64,
            _ => 0,
        };
        if !(1..=K_MAX_RANDSTR_COUNT).contains(&c) {
            raise_string_error(
                lua,
                format!("randstr: count must be between 1 and {K_MAX_RANDSTR_COUNT}"),
            )
        }
        c
    } else {
        1
    };

    // glibc `rand()`: `state = (state * 1103515245 + 12345)` mod 2^32 (signed
    // wrap), returning `(state >> 16) & 0x7fff`. The default seed is 1.
    let mut state: i32 = 1;
    let mut next_rand = move || -> i64 {
        state = state.wrapping_mul(1_103_515_245).wrapping_add(12_345);
        (((state as u32) >> 16) & 0x7fff) as i64
    };

    let mut buf = Vec::with_capacity(dsize as usize);
    let mut push_str = || -> mlua::Result<Value> {
        buf.clear();
        buf.resize(dsize as usize, b' ');
        let mut i = 0i64;
        while i < dsize {
            if i % PATTERN_INTERVAL == 0 && i + PATTERN_LEN <= dsize {
                buf[i as usize..(i + PATTERN_LEN) as usize].copy_from_slice(PATTERN);
                i += PATTERN_LEN - 1;
            } else {
                buf[i as usize] = ALPHANUM[(next_rand() % 62) as usize];
            }
            i += 1;
        }
        lua.create_string(&buf).map(Value::String)
    };

    if count == 1 {
        push_str()
    } else {
        let t = lua.create_table()?;
        for i in 1..=count {
            t.raw_set(i, push_str()?)?;
        }
        Ok(Value::Table(t))
    }
}

/// `DragonflyHashCommand` (interpreter.cc:531): `dragonfly.ihash(seed, sort,
/// cmd, args...)` dispatches the command with PCALL semantics and folds the
/// flattened reply strings into an XXH64 hash seeded with `seed` (bit-cast)
/// and the command's keys (all keys for MGET, otherwise the first). Returns
/// the hash as an integer. A command error contributes nothing.
fn dragonfly_ihash<D: ScriptDispatch>(
    lua: &Lua,
    dispatch: &RefCell<&mut D>,
    args: MultiValue,
) -> mlua::Value {
    // `lua_tointeger`/`lua_toboolean` are lenient about non-number/boolean
    // args; mirror that by defaulting to 0 / false.
    let seed: u64 = match args.front() {
        Some(Value::Integer(i)) => *i as u64,
        Some(Value::Number(f)) => *f as i64 as u64,
        _ => 0,
    };
    let requires_sort = matches!(args.get(1), Some(Value::Boolean(true)));
    let tail: MultiValue = args.into_iter().skip(2).collect();
    let Ok(cmd_args) = prepare_args(&tail) else {
        raise_string_error(lua, ARG_TYPE_ERR.into())
    };
    if cmd_args.is_empty() {
        // `RedisGenericCommand` pushes an error for an empty arg list but the
        // reference ignores it; the hash stays the seed.
        return Value::Integer(seed as i64);
    }

    // Compute the key hash: all key arguments for MGET, otherwise just the
    // first (`lua_tolstring` on a missing index hashes empty bytes).
    let cmd = &cmd_args[0];
    let key_end = if cmd.eq_ignore_ascii_case(b"mget") {
        cmd_args.len()
    } else {
        2
    };
    let mut hash = seed;
    for i in 2..=key_end {
        let key: &[u8] = cmd_args.get(i - 1).map(Vec::as_slice).unwrap_or_default();
        hash = xxh64(key, hash);
    }

    let mut values: Vec<Vec<u8>> = Vec::new();
    if let Ok(v) = dispatch.borrow_mut().dispatch(cmd_args) {
        collect_reply_bytes(&mut values, &v);
    }
    if requires_sort {
        values.sort();
    }
    for s in &values {
        hash = xxh64(s, hash);
    }
    Value::Integer(hash as i64)
}

/// Install the per-run `dragonfly.*` helpers that close over the dispatch
/// context (`ihash`, `lock`, `unlock`), mirroring how `redis.call`/`pcall`
/// are reinstalled per run.
fn install_dragonfly_functions<'a, D: ScriptDispatch>(
    lua: &Lua,
    scope: &'a mlua::Scope<'a, '_>,
    dispatch: &'a RefCell<&mut D>,
) -> mlua::Result<()> {
    let ihash = scope.create_function_mut(move |lua, args: MultiValue| {
        Ok(dragonfly_ihash(lua, dispatch, args))
    })?;
    let lock = scope.create_function_mut(move |lua, args: MultiValue| {
        let Ok(keys) = prepare_args(&args) else {
            raise_string_error(lua, ARG_TYPE_ERR.into())
        };
        if keys.is_empty() {
            // `RedisGenericCommand`: `backed_args_.empty()` with no UNLOCK bit.
            raise_string_error(
                lua,
                "Please specify at least one argument for this call".into(),
            )
        }
        match dispatch.borrow_mut().lock(keys) {
            Ok(()) => Ok(Value::Nil),
            Err(msg) => raise_string_error(lua, msg),
        }
    })?;
    let unlock = scope.create_function_mut(move |lua, _: MultiValue| {
        match dispatch.borrow_mut().unlock() {
            Ok(()) => Ok(Value::Nil),
            Err(msg) => raise_string_error(lua, msg),
        }
    })?;
    let dragonfly: Table = lua.globals().get("dragonfly")?;
    dragonfly.set("ihash", ihash)?;
    dragonfly.set("lock", lock)?;
    dragonfly.set("unlock", unlock)?;
    Ok(())
}

/// Convert a subcommand reply into the Lua value a script observes
/// (`RedisTranslator`): status -> `{ok=...}`, error -> `{err=...}`, nil ->
/// `false`, integral doubles -> integers.
fn resp_to_lua(lua: &Lua, r: RespValue) -> mlua::Result<Value> {
    Ok(match r {
        RespValue::Nil => Value::Boolean(false),
        RespValue::NilArray => Value::Boolean(false),
        RespValue::Bool(b) => Value::Boolean(b),
        RespValue::Integer(i) => Value::Integer(i),
        RespValue::Double(d) => {
            // `RedisTranslator::OnDouble` (interpreter.cc): convert to an
            // integer only when the fraction is within `epsilon` of an integer
            // and the value is strictly inside the Lua integer range; anything
            // else (including exactly 2^63) stays a number.
            const EPS: f64 = f64::EPSILON;
            let fract = d.fract();
            if fract.abs() < EPS && d < i64::MAX as f64 && d > i64::MIN as f64 {
                Value::Integer(d as i64)
            } else {
                Value::Number(d)
            }
        }
        RespValue::Bulk(b) => Value::String(lua.create_string(&b)?),
        RespValue::Simple(s) => {
            let t = lua.create_table()?;
            t.raw_set("ok", s)?;
            Value::Table(t)
        }
        RespValue::Error(e) => {
            let t = lua.create_table()?;
            t.raw_set("err", e)?;
            Value::Table(t)
        }
        RespValue::Array(items) => {
            let t = lua.create_table()?;
            for (i, v) in items.into_iter().enumerate() {
                t.raw_set(i + 1, resp_to_lua(lua, v)?)?;
            }
            Value::Table(t)
        }
        RespValue::Map(pairs) => {
            let t = lua.create_table()?;
            for (k, v) in pairs {
                t.raw_set(resp_to_lua(lua, k)?, resp_to_lua(lua, v)?)?;
            }
            Value::Table(t)
        }
    })
}

fn strip_crlf(b: &[u8]) -> String {
    b.iter()
        .filter(|&&c| c != b'\r' && c != b'\n')
        .map(|&c| c as char)
        .collect()
}

/// Format a Lua error table value for the wire: the reference's
/// `EvalSerializer::OnError` passes the message through, adding a leading `-`
/// when missing.
fn fmt_error(b: &[u8]) -> String {
    let s = strip_crlf(b);
    if s.starts_with('-') {
        s
    } else {
        format!("-{s}")
    }
}

/// Serialize a script return value to RESP (`SerializeResult` +
/// `EvalSerializer`). Depth is capped at 128 like `IsResultSafe`.
fn serialize_value(v: Value, float_as_int: bool, depth: usize) -> mlua::Result<RespValue> {
    if depth > 128 {
        return Err(mlua::Error::runtime("reached lua stack limit"));
    }
    Ok(match v {
        Value::Boolean(true) => RespValue::Integer(1),
        Value::Integer(i) => RespValue::Integer(i),
        Value::Number(d) => {
            if float_as_int {
                let val = if d >= 0.0 { d.floor() } else { d.ceil() } as i64;
                RespValue::Integer(val)
            } else {
                RespValue::Double(d)
            }
        }
        Value::String(s) => RespValue::Bulk(s.as_bytes().to_vec()),
        Value::Table(t) => {
            if let Ok(Value::String(s)) = t.raw_get::<Value>("err") {
                return Ok(RespValue::Error(fmt_error(&s.as_bytes())));
            }
            if let Ok(Value::String(s)) = t.raw_get::<Value>("ok") {
                return Ok(RespValue::Simple(strip_crlf(&s.as_bytes())));
            }
            if let Ok(Value::Table(m)) = t.raw_get::<Value>("map") {
                let mut pairs = Vec::new();
                for pair in m.pairs::<Value, Value>() {
                    let (k, val) = pair?;
                    pairs.push((
                        serialize_value(k, float_as_int, depth + 1)?,
                        serialize_value(val, float_as_int, depth + 1)?,
                    ));
                }
                return Ok(RespValue::Map(pairs));
            }
            let len = t.raw_len();
            let mut items = Vec::with_capacity(len);
            for i in 1..=len {
                items.push(serialize_value(
                    t.raw_get::<Value>(i)?,
                    float_as_int,
                    depth + 1,
                )?);
            }
            RespValue::Array(items)
        }
        _ => RespValue::Nil,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Noop;

    impl ScriptDispatch for Noop {
        fn dispatch(&mut self, _: Vec<Vec<u8>>) -> Result<RespValue, String> {
            Err("ERR noop".into())
        }
    }

    fn no_kill() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    struct Failing;

    impl ScriptDispatch for Failing {
        fn dispatch(&mut self, _: Vec<Vec<u8>>) -> Result<RespValue, String> {
            Ok(RespValue::Nil)
        }
        fn dispatch_async(&mut self, _: Vec<Vec<u8>>, _: bool) -> Result<(), String> {
            Err("ERR boom".into())
        }
    }

    #[test]
    fn sha1_known_vectors() {
        assert_eq!(sha1_hex(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(sha1_hex(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(
            sha1_hex(b"return 1"),
            "e0e1f9fabfc9d4800c877a703b823ac0578ff8db"
        );
        assert_eq!(
            sha1_hex(b"return {ARGV[1], KEYS[1]}"),
            "97e48688d01c6016d34396d1b844b1229ef64bc3"
        );
    }

    #[test]
    fn params_deduction() {
        assert_eq!(ScriptMgr::deduce_params(b"return 1").unwrap(), None);
        assert_eq!(
            ScriptMgr::deduce_params(b"--!df flags=legacy-float,disable-atomicity\nreturn 1")
                .unwrap(),
            Some(ScriptParams {
                atomic: false,
                undeclared_keys: false,
                float_as_int: true
            })
        );
        assert_eq!(
            ScriptMgr::deduce_params(b"--!df flags=allow-undeclared-keys  return 1").unwrap(),
            Some(ScriptParams {
                undeclared_keys: true,
                ..Default::default()
            })
        );
        // Flags line running to EOF is treated as absent.
        assert_eq!(
            ScriptMgr::deduce_params(b"--!df flags=legacy-float").unwrap(),
            None
        );
        assert!(ScriptMgr::deduce_params(b"--!df flags=bogus\nreturn 1").is_err());
    }

    #[test]
    fn hardcoded_sha_overrides() {
        let mut p = ScriptParams {
            atomic: false,
            ..Default::default()
        };
        // The Sidekiq script of issue #4522 is forced atomic even though its
        // own flags say otherwise.
        ScriptMgr::apply_hardcoded_overrides(HARDCODED_ATOMIC, &mut p);
        assert!(p.atomic, "sha_4522 must be forced atomic");
        // An unrelated sha keeps its declared flags.
        let mut p2 = ScriptParams {
            atomic: false,
            ..Default::default()
        };
        ScriptMgr::apply_hardcoded_overrides("deadbeef", &mut p2);
        assert!(!p2.atomic);
        // The undeclared-keys list forces the flag.
        let mut p3 = ScriptParams::default();
        ScriptMgr::apply_hardcoded_overrides(HARDCODED_UNDECLARED[0], &mut p3);
        assert!(p3.undeclared_keys);
    }

    #[test]
    fn cli_sha_lists_and_default_flags() {
        let mut mgr = ScriptMgr::new();
        mgr.configure(
            "allow-undeclared-keys",
            vec!["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()],
            vec!["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into()],
            false,
            false,
            false,
        )
        .unwrap();

        // `--default_lua_flags` applies to scripts without their own flags line.
        let mut p = mgr.deduce_and_override(b"return 1").unwrap();
        assert!(p.undeclared_keys, "default flags must apply");
        assert!(p.atomic);
        // A script with its own `--!df flags=` keeps them (no merge).
        p = mgr
            .deduce_and_override(b"--!df flags=disable-atomicity\nreturn 1")
            .unwrap();
        assert!(!p.atomic);
        assert!(!p.undeclared_keys, "own flags win over defaults");

        // The CLI SHA lists force flags, after the hardcoded else-if.
        let mut p = ScriptParams::default();
        mgr.apply_cli_sha_overrides("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", &mut p);
        assert!(p.undeclared_keys);
        let mut p = ScriptParams::default();
        mgr.apply_cli_sha_overrides("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", &mut p);
        assert!(p.float_as_int);
        // A hardcoded SHA is not re-checked against the undeclared list.
        let mut p = ScriptParams::default();
        mgr.undeclared_keys_shas = vec![HARDCODED_UNDECLARED[0].into()];
        mgr.apply_cli_sha_overrides(HARDCODED_UNDECLARED[0], &mut p);
        assert!(!p.undeclared_keys, "hardcoded list takes precedence");
    }

    #[test]
    fn on_script_error_auto_corrects_undeclared_keys() {
        let mut mgr = ScriptMgr::new();
        mgr.configure("", vec![], vec![], true, false, false)
            .unwrap();
        let body = b"return KEYS[1]";
        let sha = sha1_hex(body);
        let params = ScriptParams::default();
        mgr.store(sha.clone(), body.to_vec(), params);
        assert!(!mgr.params(&sha).unwrap().undeclared_keys);

        // An unrelated error leaves the flag alone.
        mgr.on_script_error(&sha, "boom");
        assert!(!mgr.params(&sha).unwrap().undeclared_keys);

        // The undeclared-key message flips the cached script's flag.
        mgr.on_script_error(&sha, "script tried accessing undeclared key, key: x");
        assert!(mgr.params(&sha).unwrap().undeclared_keys);
        assert!(mgr.find(&sha).unwrap().params.undeclared_keys);

        // Flag-only entries are untouched (`OnScriptError` looks up loaded scripts).
        mgr.params
            .insert("flagonly".into(), ScriptParams::default());
        mgr.on_script_error("flagonly", "script tried accessing undeclared key, key: x");
        assert!(!mgr.params("flagonly").unwrap().undeclared_keys);

        // With the flag off, nothing is auto-corrected.
        let mut off = ScriptMgr::new();
        let sha2 = sha1_hex(b"return 2");
        off.store(sha2.clone(), b"return 2".to_vec(), ScriptParams::default());
        off.on_script_error(&sha2, "script tried accessing undeclared key, key: y");
        assert!(!off.params(&sha2).unwrap().undeclared_keys);
    }

    #[test]
    fn resp2_legacy_float_converts_script_floats() {
        let interp = SandboxedInterpreter::new().unwrap();
        interp.define("aaaa", b"return 3.7").unwrap();
        // float_as_int=false alone keeps the double.
        let v = interp.run("aaaa", &mut Noop, false, &no_kill()).unwrap();
        assert_eq!(v, RespValue::Double(3.7));
        // The caller ORs `lua_resp2_legacy_float` into `float_as_int`, so the
        // flag makes every float floor/ceil like `EvalSerializer::OnDouble`.
        let v = interp.run("aaaa", &mut Noop, true, &no_kill()).unwrap();
        assert_eq!(v, RespValue::Integer(3));
    }

    #[test]
    fn deep_return_table_hits_stack_limit() {
        let interp = SandboxedInterpreter::new().unwrap();
        interp
            .define(
                "aaaa",
                b"local t = {}\nlocal cur = t\nfor i = 1, 200 do cur[1] = {} cur = cur[1] end\nreturn t",
            )
            .unwrap();
        let err = interp
            .run("aaaa", &mut Noop, false, &no_kill())
            .unwrap_err();
        // `serialize_value` caps at depth 128; the coordinator sends this
        // message bare (no `Error running script` wrapper).
        assert_eq!(err, "reached lua stack limit");
    }

    #[test]
    fn resp_double_to_lua_uses_epsilon() {
        let interp = SandboxedInterpreter::new().unwrap();
        let lua = &interp.lua;
        let to_int = |d: f64| {
            matches!(
                resp_to_lua(lua, RespValue::Double(d)).unwrap(),
                Value::Integer(_)
            )
        };
        assert!(to_int(5.0));
        assert!(to_int(-0.0));
        assert!(!to_int(1.5));
        assert!(!to_int(0.300_000_000_000_000_04));
        // `OnDouble` converts when |fract| < epsilon: 1e-17 truncates to 0.
        assert!(to_int(1e-17), "fraction within epsilon converts to integer");
        assert!(to_int(4_503_599_627_370_496.0));
        // Strict bounds: exactly +/-2^63 stays a Lua number.
        assert!(!to_int(9_223_372_036_854_775_808.0), "2^63 stays a number");
        assert!(
            !to_int(-9_223_372_036_854_775_808.0),
            "-2^63 stays a number"
        );
        match resp_to_lua(lua, RespValue::Double(1e-17)).unwrap() {
            Value::Integer(i) => assert_eq!(i, 0),
            v => panic!("expected integer 0, got {v:?}"),
        }
    }

    #[test]
    fn apply_flags_errors() {
        let mut p = ScriptParams::default();
        assert_eq!(
            p.apply_flags("allow-undeclared-keys;disable-atomicity"),
            Ok(())
        );
        assert!(!p.atomic && p.undeclared_keys);
        assert_eq!(p.apply_flags("no-writes"), Ok(()));
        assert_eq!(p.apply_flags("bogus"), Err("Invalid flag: bogus".into()));
    }

    #[test]
    fn detect_async_calls() {
        let rw = |s: &str| {
            detect_possible_async_calls(s.as_bytes()).map(|b| String::from_utf8(b).unwrap())
        };

        // A standalone statement at the start of the script.
        assert_eq!(
            rw("redis.call('set', KEYS[1], '1')"),
            Some("redis.acall('set', KEYS[1], '1')".into())
        );
        // Statement start after an ordinary previous line.
        assert_eq!(
            rw("local x = 1\nredis.call('set', KEYS[1], '1')"),
            Some("local x = 1\nredis.acall('set', KEYS[1], '1')".into())
        );
        // Previous line is a plain comment: the call is a target.
        assert_eq!(
            rw("-- hello\nredis.call('set', KEYS[1], '1')"),
            Some("-- hello\nredis.acall('set', KEYS[1], '1')".into())
        );
        // First statement of a `then`/`do` block (same or next line).
        assert_eq!(
            rw("if x then redis.call('set', KEYS[1], '1') end"),
            Some("if x then redis.acall('set', KEYS[1], '1') end".into())
        );
        assert_eq!(
            rw("do redis.call('set', KEYS[1], '1') end"),
            Some("do redis.acall('set', KEYS[1], '1') end".into())
        );
        assert_eq!(
            rw("while true do\nredis.call('set', KEYS[1], '1')"),
            Some("while true do\nredis.acall('set', KEYS[1], '1')".into())
        );
        // pcall rewrites to apcall, inserting before the `p`.
        assert_eq!(
            rw("redis.pcall('set', KEYS[1], '1')"),
            Some("redis.apcall('set', KEYS[1], '1')".into())
        );

        // The return value is used: never rewritten.
        assert_eq!(rw("local x = redis.call('get', KEYS[1])"), None);
        assert_eq!(rw("print(redis.call('get', KEYS[1]))"), None);
        // Multi-line expression: previous line ends with a continuation op.
        assert_eq!(rw("local x = f(\nredis.call('get', KEYS[1])"), None);
        assert_eq!(rw("local x = 1 +\nredis.call('get', KEYS[1])"), None);
        assert_eq!(rw("local x = 1 ,\nredis.call('get', KEYS[1])"), None);
        // Continuation token: previous line ends with `return`.
        assert_eq!(rw("return\nredis.call('get', KEYS[1])"), None);
        // Block comments make the scanner bail entirely.
        assert_eq!(rw("--[[ c\nredis.call('set', KEYS[1], '1')]]"), None);
        // Only standalone calls are rewritten in a mixed body.
        assert_eq!(
            rw("redis.call('set', KEYS[1], '1')\nlocal v = redis.call('get', KEYS[1])"),
            Some("redis.acall('set', KEYS[1], '1')\nlocal v = redis.call('get', KEYS[1])".into())
        );
        // `then`/`do` on their own line are ordinary words (not continuation
        // tokens), so a following call is still a target.
        assert_eq!(
            rw("if x then\nredis.call('set', KEYS[1], '1')"),
            Some("if x then\nredis.acall('set', KEYS[1], '1')".into())
        );
        // No calls at all.
        assert_eq!(rw("return 1"), None);
    }

    #[test]
    fn auto_async_body_gating() {
        let mut mgr = ScriptMgr::new();
        let body = b"redis.call('set', KEYS[1], '1')";
        // The flag defaults off.
        assert!(!mgr.lua_auto_async);
        assert_eq!(mgr.auto_async_body(body, &ScriptParams::default()), body);
        mgr.lua_auto_async = true;
        // Non-atomic scripts are never rewritten.
        let non_atomic = ScriptParams {
            atomic: false,
            ..Default::default()
        };
        assert_eq!(mgr.auto_async_body(body, &non_atomic), body);
        // Atomic + flag: rewritten, and idempotent on the rewritten result.
        let rewritten = mgr.auto_async_body(body, &ScriptParams::default());
        assert_eq!(
            String::from_utf8(rewritten.clone()).unwrap(),
            "redis.acall('set', KEYS[1], '1')"
        );
        assert_eq!(
            mgr.auto_async_body(&rewritten, &ScriptParams::default()),
            rewritten
        );
    }

    struct RecordingDispatch {
        calls: Vec<(Vec<Vec<u8>>, bool)>,
    }

    impl ScriptDispatch for RecordingDispatch {
        fn dispatch(&mut self, _: Vec<Vec<u8>>) -> Result<RespValue, String> {
            Ok(RespValue::Integer(1))
        }
        fn dispatch_async(
            &mut self,
            args: Vec<Vec<u8>>,
            abort_on_error: bool,
        ) -> Result<(), String> {
            self.calls.push((args, abort_on_error));
            Ok(())
        }
        fn flush(&mut self) -> Result<(), String> {
            self.calls.push((vec![b"__flush__".to_vec()], false));
            Ok(())
        }
    }

    #[test]
    fn async_call_wiring() {
        let interp = SandboxedInterpreter::new().unwrap();
        let sha = "aaaa";
        let mut d = RecordingDispatch { calls: Vec::new() };
        let call_before: Vec<Vec<u8>> = vec![b"SET".to_vec(), b"k".to_vec(), b"1".to_vec()];
        let get: Vec<Vec<u8>> = vec![b"GET".to_vec(), b"k".to_vec()];

        // acall routes with abort_on_error=true and evaluates to nil.
        interp
            .define(
                sha,
                b"redis.acall('SET', KEYS[1], '1')\nreturn redis.acall('GET', KEYS[1])",
            )
            .unwrap();
        interp.set_global_array("KEYS", &[b"k".to_vec()]).unwrap();
        let v = interp.run(sha, &mut d, false, &no_kill()).unwrap();
        assert_eq!(v, RespValue::Nil);
        assert_eq!(d.calls.len(), 2);
        assert_eq!(d.calls[0].0, call_before);
        assert!(d.calls[0].1);
        assert_eq!(d.calls[1].0, get);
        assert!(d.calls[1].1);

        // apcall routes with abort_on_error=false.
        d.calls.clear();
        interp
            .define(sha, b"redis.apcall('GET', KEYS[1])\nreturn 1")
            .unwrap();
        assert_eq!(
            interp.run(sha, &mut d, false, &no_kill()).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(d.calls.len(), 1);
        assert!(!d.calls[0].1);

        // Both acall and apcall raise dispatch errors (flush failures set the
        // reference's `requested_abort`, which raises regardless of mode); the
        // two differ only in unknown-command handling and per-command runtime
        // errors, both decided inside the dispatcher.
        interp
            .define(sha, b"redis.acall('SET', KEYS[1], '1')")
            .unwrap();
        let err = interp
            .run(sha, &mut Failing, false, &no_kill())
            .unwrap_err();
        assert!(err.contains("ERR boom"), "{err}");
        interp
            .define(sha, b"redis.apcall('SET', KEYS[1], '1')\nreturn 1")
            .unwrap();
        let err = interp
            .run(sha, &mut Failing, false, &no_kill())
            .unwrap_err();
        assert!(err.contains("ERR boom"), "{err}");
    }

    #[test]
    fn count_hook_kills_cpu_bound_loop() {
        let interp = SandboxedInterpreter::new().unwrap();
        interp.define("aaaa", b"while true do end").unwrap();
        let kill = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&kill);
        // `FUNCTION KILL` arrives from the IO thread while the loop runs.
        let killer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            flag.store(true, Ordering::Relaxed);
        });
        let err = interp.run("aaaa", &mut Noop, false, &kill).unwrap_err();
        killer.join().unwrap();
        assert!(err.contains("Script killed by user"), "{err}");
    }

    #[test]
    fn count_hook_kills_cpu_bound_function() {
        let interp = SandboxedInterpreter::new().unwrap();
        interp
            .load_function_lib(b"#!lua name=lib\nredis.register_function('spin', function() while true do end end)")
            .unwrap();
        let kill = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&kill);
        let killer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            flag.store(true, Ordering::Relaxed);
        });
        let err = interp
            .run_function("spin", &[], &[], &mut Noop, false, &kill)
            .unwrap_err();
        killer.join().unwrap();
        assert!(err.contains("Script killed by user"), "{err}");
    }

    #[test]
    fn sandbox_rejects_bad_globals() {
        let interp = SandboxedInterpreter::new().unwrap();
        let run = |body: &str| {
            interp.define("aaaa", body.as_bytes()).unwrap();
            interp.run("aaaa", &mut Noop, false, &no_kill())
        };
        assert_eq!(run("return 1 + 2").unwrap(), RespValue::Integer(3));
        // Missing global read -> strict error.
        let err = run("return no_such").unwrap_err();
        assert!(
            err.contains("Script attempted to access nonexistent global variable 'no_such'"),
            "{err}"
        );
        // Global write from inside a function -> strict error.
        let err = run("x = 5 return x").unwrap_err();
        assert!(
            err.contains("Script attempted to create global variable 'x'"),
            "{err}"
        );
        // debug is hidden.
        let err = run("return debug").unwrap_err();
        assert!(
            err.contains("Script attempted to access nonexistent global variable 'debug'"),
            "{err}"
        );
        // loadfile/dofile are nilled, so reading them trips the strict guard.
        let err = run("return loadfile").unwrap_err();
        assert!(
            err.contains("nonexistent global variable 'loadfile'"),
            "{err}"
        );
        // Error handler prefixes @user_script:<line>.
        let err = run("error('boom')").unwrap_err();
        assert!(err.starts_with("@user_script:"), "{err}");
        assert!(err.contains(": boom"), "{err}");
        // redis.call errors are raised as plain Lua strings, so the error
        // handler can concatenate them instead of choking on a userdata.
        let err = run("return redis.call('nosuch')").unwrap_err();
        assert!(err.starts_with("@user_script:2:"), "{err}");
        assert!(err.contains("ERR noop"), "{err}");
        // redis.pcall returns a single {err=...} table rather than raising.
        let err = run("local r = redis.pcall('nosuch') return r.err").unwrap();
        assert_eq!(err, RespValue::Bulk(b"ERR noop".to_vec()));
        // A redis.call error inside pcall is caught as a plain string
        // (false serializes to nil, like RESP2).
        let err = run("local ok, r = pcall(redis.call, 'nosuch') return ok").unwrap();
        assert_eq!(err, RespValue::Nil);
        let err = run("local ok, r = pcall(redis.call, 'nosuch') return r").unwrap();
        assert_eq!(err, RespValue::Bulk(b"ERR noop".to_vec()));
    }

    #[test]
    fn sandbox_setup_is_once_only() {
        let interp = SandboxedInterpreter::new().unwrap();
        // Re-define of another script in the same state must not trip the strict chunk.
        interp.define("bbbb", b"return 42").unwrap();
        assert_eq!(
            interp.run("bbbb", &mut Noop, false, &no_kill()).unwrap(),
            RespValue::Integer(42)
        );
    }

    #[test]
    fn function_header_parsing() {
        assert_eq!(
            parse_function_header(b"#!lua name=lib1").unwrap().name,
            "lib1"
        );
        let h = parse_function_header(b"#!lua name=lib1 flags=no-writes,allow-undeclared-keys")
            .unwrap();
        assert_eq!(h.header_flags, vec!["no-writes", "allow-undeclared-keys"]);
        assert_eq!(
            parse_function_header(b"return 1").unwrap_err(),
            "Missing library metadata"
        );
        assert_eq!(
            parse_function_header(b"#!js name=x").unwrap_err(),
            "Invalid engine type"
        );
        assert_eq!(
            parse_function_header(b"#!lua").unwrap_err(),
            "Missing library name"
        );
        assert_eq!(
            parse_function_header(b"#!lua name=").unwrap_err(),
            "Missing library name"
        );
        assert_eq!(
            parse_function_header(b"#!lua name=x flags=bogus").unwrap_err(),
            "Invalid flag: bogus"
        );
        assert_eq!(
            parse_function_header(b"#!lua name=my-lib!").unwrap_err(),
            "Library names can only contain letters, numbers, '_', '-' and '.'"
        );
        assert_eq!(function_body(b"#!lua name=x\nreturn 1"), b"return 1");
        assert_eq!(function_body(b"#!lua name=x"), b"");
    }

    #[test]
    fn function_lib_load_and_run() {
        struct Get;
        impl ScriptDispatch for Get {
            fn dispatch(&mut self, args: Vec<Vec<u8>>) -> Result<RespValue, String> {
                assert_eq!(args[0], b"get");
                Ok(RespValue::Bulk(b"v".to_vec()))
            }
        }

        let interp = SandboxedInterpreter::new().unwrap();
        let code = b"#!lua name=lib1\n\
            redis.register_function('add', function(keys, args)\n\
              return redis.call('get', keys[1]) .. ':' .. args[1]\n\
            end)\n\
            redis.register_function{function_name='one', callback=function() return 1 end, flags={'no-writes'}}\n\
            return 0";
        let fns = interp.load_function_lib(code).unwrap();
        assert_eq!(fns.len(), 2);
        assert_eq!(fns[0].name, "add");
        assert!(fns[0].flags.is_empty());
        assert_eq!(fns[1].name, "one");
        assert_eq!(fns[1].flags, vec!["no-writes"]);

        // The callback receives the (keys, args) tables and redis.call works.
        assert_eq!(
            interp
                .run_function(
                    "add",
                    &[b"k".to_vec()],
                    &[b"a".to_vec()],
                    &mut Get,
                    false,
                    &no_kill()
                )
                .unwrap(),
            RespValue::Bulk(b"v:a".to_vec())
        );
        assert_eq!(
            interp
                .run_function("one", &[], &[], &mut Get, false, &no_kill())
                .unwrap(),
            RespValue::Integer(1)
        );
        // Unknown function name.
        assert!(
            interp
                .run_function("nope", &[], &[], &mut Get, false, &no_kill())
                .is_err()
        );
        // Reloading a library replaces the callback in place.
        let code2 = b"#!lua name=lib1\nredis.register_function('one', function() return 2 end)";
        interp.load_function_lib(code2).unwrap();
        assert_eq!(
            interp
                .run_function("one", &[], &[], &mut Get, false, &no_kill())
                .unwrap(),
            RespValue::Integer(2)
        );

        // Bad register_function calls fail the whole load.
        let err = interp
            .load_function_lib(b"#!lua name=l\nredis.register_function('x', 5)")
            .unwrap_err();
        assert!(
            err.contains("Function callback must be a function"),
            "{err}"
        );
        let err = interp
            .load_function_lib(b"#!lua name=l\nredis.register_function()")
            .unwrap_err();
        assert!(err.contains("wrong number or type of arguments"), "{err}");
        let err = interp
            .load_function_lib(
                b"#!lua name=l\nredis.register_function('bad name!', function() end)",
            )
            .unwrap_err();
        assert!(
            err.contains("Function names can only contain letters, numbers, '_', '-' and '.'"),
            "{err}"
        );
        // redis.call at library top level is rejected.
        let err = interp
            .load_function_lib(b"#!lua name=l\nredis.call('get', 'k')")
            .unwrap_err();
        assert!(
            err.contains("redis.call is not allowed during function library load"),
            "{err}"
        );
    }

    #[test]
    fn function_table_is_hidden_and_purged() {
        struct Get;
        impl ScriptDispatch for Get {
            fn dispatch(&mut self, args: Vec<Vec<u8>>) -> Result<RespValue, String> {
                assert_eq!(args[0], b"get");
                Ok(RespValue::Bulk(b"v".to_vec()))
            }
        }

        let interp = SandboxedInterpreter::new().unwrap();
        // The callback table lives in the Lua registry, not `_G`, so scripts
        // cannot read it (direct access would bypass FCALL's checks).
        interp.define("aaaa", b"return __dfly_functions__").unwrap();
        let err = interp
            .run("aaaa", &mut Noop, false, &no_kill())
            .unwrap_err();
        assert!(
            err.contains("nonexistent global variable '__dfly_functions__'"),
            "{err}"
        );

        interp
            .load_function_lib(
                b"#!lua name=l\nredis.register_function('f', function() return 1 end)",
            )
            .unwrap();
        assert_eq!(
            interp
                .run_function("f", &[], &[], &mut Get, false, &no_kill())
                .unwrap(),
            RespValue::Integer(1)
        );

        // purge_functions drops the callback (used when a REPLACE drops a name).
        interp.purge_functions(&["f".to_string()]);
        assert!(
            interp
                .run_function("f", &[], &[], &mut Get, false, &no_kill())
                .is_err()
        );
    }

    #[test]
    fn register_function_errors_outside_load() {
        let interp = SandboxedInterpreter::new().unwrap();
        // The bootstrap blocker covers the pre-load state.
        interp
            .define(
                "aaaa",
                b"return redis.register_function('x', function() end)",
            )
            .unwrap();
        let err = interp
            .run("aaaa", &mut Noop, false, &no_kill())
            .unwrap_err();
        assert!(
            err.contains("redis.register_function can only be called on FUNCTION LOAD command"),
            "{err}"
        );
        // After a load the blocker is restored, not the load-only collector.
        interp
            .load_function_lib(b"#!lua name=l\nredis.register_function('f', function() end)")
            .unwrap();
        interp
            .define(
                "bbbb",
                b"return redis.register_function('y', function() end)",
            )
            .unwrap();
        let err = interp
            .run("bbbb", &mut Noop, false, &no_kill())
            .unwrap_err();
        assert!(
            err.contains("redis.register_function can only be called on FUNCTION LOAD command"),
            "{err}"
        );
    }

    #[test]
    fn library_dump_restore() {
        let mut mgr = ScriptMgr::new();
        let lib = FunctionLib {
            name: "lib1".into(),
            engine: "LUA".into(),
            code: b"#!lua name=lib1\nredis.register_function('f', function() end)".to_vec(),
            sha: "abc".into(),
            header_flags: vec!["no-writes".into()],
            functions: vec![FunctionInfo {
                name: "f".into(),
                flags: vec![],
            }],
        };
        mgr.store_library(lib);
        let dump = mgr.dump_libraries();
        let mut restored = ScriptMgr::restore_libraries(&dump).unwrap();
        assert_eq!(restored.len(), 1);
        let r = restored.remove(0);
        assert_eq!(r.name, "lib1");
        assert_eq!(r.header_flags, vec!["no-writes"]);
        assert_eq!(
            r.code,
            b"#!lua name=lib1\nredis.register_function('f', function() end)"
        );
        assert!(ScriptMgr::restore_libraries(b"junk").is_err());
        assert!(ScriptMgr::restore_libraries(b"").is_err());
    }

    #[test]
    fn latency_aggregates_per_sha() {
        let mut mgr = ScriptMgr::new();
        assert!(mgr.latency().is_empty());
        mgr.record_latency("sha1", 100);
        mgr.record_latency("sha1", 300);
        mgr.record_latency("sha2", 50);
        let s1 = &mgr.latency()["sha1"];
        assert_eq!(
            (
                s1.count(),
                s1.sum() as u64,
                s1.min() as u64,
                s1.max() as u64
            ),
            (2, 400, 100, 300)
        );
        assert_eq!(mgr.latency()["sha2"].count(), 1);
        // The reference sends the merged histogram's text dump verbatim.
        assert!(s1.to_string().starts_with("Count: 2 Average: 200.0000\n"));
    }

    #[test]
    fn kill_flag_shared_with_io_thread() {
        let mgr = ScriptMgr::new();
        let flag = mgr.kill_flag();
        assert!(!mgr.kill_requested());
        mgr.request_kill();
        // The coordinator's clone observes the write without touching the mutex.
        assert!(flag.load(std::sync::atomic::Ordering::Relaxed));
        assert!(mgr.kill_requested());
        mgr.clear_kill();
        assert!(!mgr.kill_requested());
    }

    #[test]
    fn sha1hex_coerces_numbers() {
        let interp = SandboxedInterpreter::new().unwrap();
        let run_body = |sha: &str, body: &str| -> RespValue {
            interp.define(sha, body.as_bytes()).unwrap();
            interp.run(sha, &mut Noop, false, &no_kill()).unwrap()
        };
        // `lua_tolstring` coerces strings, integers and floats (`RedisSha1Command`).
        assert_eq!(
            run_body("s1", "return redis.sha1hex('x')"),
            RespValue::Bulk(sha1_hex(b"x").into_bytes())
        );
        assert_eq!(
            run_body("s2", "return redis.sha1hex(123)"),
            RespValue::Bulk(sha1_hex(b"123").into_bytes())
        );
        assert_eq!(
            run_body("s3", "return redis.sha1hex(100)"),
            RespValue::Bulk(sha1_hex(b"100").into_bytes())
        );
        assert_eq!(
            run_body("s4", "return redis.sha1hex(3.7)"),
            RespValue::Bulk(sha1_hex(b"3.7").into_bytes())
        );
        assert_eq!(
            run_body("s5", "return redis.sha1hex(1e16)"),
            RespValue::Bulk(sha1_hex(b"10000000000000000").into_bytes())
        );
        // Non-coercible types are rejected.
        interp.define("s6", b"return redis.sha1hex(true)").unwrap();
        let err = interp.run("s6", &mut Noop, false, &no_kill()).unwrap_err();
        assert!(err.contains("wrong number or type of arguments"), "{err}");
    }

    #[test]
    fn redis_log_level_validation() {
        // Flag off (default): a silent no-op, even for out-of-range levels.
        let interp = SandboxedInterpreter::new().unwrap();
        interp
            .define("aaaa", b"return redis.log(999, 'noop')")
            .unwrap();
        assert_eq!(
            interp.run("aaaa", &mut Noop, false, &no_kill()).unwrap(),
            RespValue::Nil
        );
        // Arity and level-type checks run regardless of the flag.
        interp.define("bbbb", b"return redis.log('x')").unwrap();
        let err = interp
            .run("bbbb", &mut Noop, false, &no_kill())
            .unwrap_err();
        assert!(
            err.contains("redis.log() requires two arguments or more."),
            "{err}"
        );
        interp
            .define("cccc", b"return redis.log('not-a-number', 'x')")
            .unwrap();
        let err = interp
            .run("cccc", &mut Noop, false, &no_kill())
            .unwrap_err();
        assert!(
            err.contains("First argument must be a number (log level)."),
            "{err}"
        );
        // Flag on: the level must be a valid LOG_DEBUG..LOG_WARNING.
        let interp = SandboxedInterpreter::with_redis_log(true).unwrap();
        interp
            .define("dddd", b"return redis.log(999, 'x')")
            .unwrap();
        let err = interp
            .run("dddd", &mut Noop, false, &no_kill())
            .unwrap_err();
        assert!(err.contains("Invalid log level."), "{err}");
        interp
            .define("eeee", b"return redis.log(1, 'hi', 42)")
            .unwrap();
        assert_eq!(
            interp.run("eeee", &mut Noop, false, &no_kill()).unwrap(),
            RespValue::Nil
        );
    }

    #[test]
    fn error_reply_bad_args_carries_trace_prefix() {
        let interp = SandboxedInterpreter::new().unwrap();
        interp
            .define("aaaa", b"return redis.error_reply(1, 2).err")
            .unwrap();
        let v = interp.run("aaaa", &mut Noop, false, &no_kill()).unwrap();
        let RespValue::Bulk(b) = v else {
            panic!("expected bulk, got {v:?}");
        };
        let s = String::from_utf8(b).unwrap();
        // `PushError` prefixes `source:currentline` (interpreter.cc).
        assert!(s.starts_with("@user_script:"), "{s}");
        assert!(s.ends_with(": wrong number or type of arguments"), "{s}");
    }

    #[test]
    fn dragonfly_randstr_matches_reference() {
        // Byte-for-byte against glibc `rand()` (seed 1) with the DRAGONFLY
        // pattern every 53 bytes (`DragonflyRandstrCommand`).
        let interp = SandboxedInterpreter::new().unwrap();
        interp
            .define("aaaa", b"return dragonfly.randstr(16)")
            .unwrap();
        assert_eq!(
            interp.run("aaaa", &mut Noop, false, &no_kill()).unwrap(),
            RespValue::Bulk(b"DRAGONFLYas7Vpl8".to_vec())
        );
        interp
            .define("bbbb", b"return dragonfly.randstr(7)")
            .unwrap();
        assert_eq!(
            interp.run("bbbb", &mut Noop, false, &no_kill()).unwrap(),
            RespValue::Bulk(b"as7Vpl8".to_vec())
        );
        // A table of `count` strings; the LCG advances across calls like the
        // reference's global `rand()`, so each string differs.
        interp
            .define("cccc", b"return dragonfly.randstr(7, 3)")
            .unwrap();
        assert_eq!(
            interp.run("cccc", &mut Noop, false, &no_kill()).unwrap(),
            RespValue::Array(vec![
                RespValue::Bulk(b"as7Vpl8".to_vec()),
                RespValue::Bulk(b"fUuLvWW".to_vec()),
                RespValue::Bulk(b"lxffRMw".to_vec()),
            ])
        );
        // Bounds mirror the reference (`kMaxRandstrSize`/`kMaxRandstrCount`).
        interp
            .define("dddd", b"return dragonfly.randstr(0)")
            .unwrap();
        let err = interp
            .run("dddd", &mut Noop, false, &no_kill())
            .unwrap_err();
        assert!(
            err.contains("randstr: size must be between 1 and 16777216"),
            "{err}"
        );
        interp
            .define("eeee", b"return dragonfly.randstr(16, 0)")
            .unwrap();
        let err = interp
            .run("eeee", &mut Noop, false, &no_kill())
            .unwrap_err();
        assert!(
            err.contains("randstr: count must be between 1 and 32768"),
            "{err}"
        );
    }

    #[test]
    fn dragonfly_ihash_hashes_keys_and_reply() {
        // Reply with the subcommand's key arguments so the folded values are
        // deterministic.
        struct Echo;
        impl ScriptDispatch for Echo {
            fn dispatch(&mut self, args: Vec<Vec<u8>>) -> Result<RespValue, String> {
                Ok(RespValue::Array(
                    args.iter()
                        .skip(1)
                        .map(|a| RespValue::Bulk(a.clone()))
                        .collect(),
                ))
            }
        }
        // A command error contributes nothing; the hash stays the seed + keys.
        struct Boom;
        impl ScriptDispatch for Boom {
            fn dispatch(&mut self, _: Vec<Vec<u8>>) -> Result<RespValue, String> {
                Err("boom".into())
            }
        }

        let interp = SandboxedInterpreter::new().unwrap();
        // Non-MGET: only the first key is hashed (`key_end = 2`).
        interp
            .define(
                "aaaa",
                b"return dragonfly.ihash(0, false, 'get', 'k1', 'k2')",
            )
            .unwrap();
        let mut h = xxh64(b"k1", 0);
        h = xxh64(b"k1", h);
        h = xxh64(b"k2", h);
        assert_eq!(
            interp.run("aaaa", &mut Echo, false, &no_kill()).unwrap(),
            RespValue::Integer(h as i64)
        );
        // MGET hashes all keys; `requires_sort` sorts the collected reply
        // strings before folding.
        interp
            .define("bbbb", b"return dragonfly.ihash(0, true, 'mget', 'b', 'a')")
            .unwrap();
        let mut h = xxh64(b"b", 0);
        h = xxh64(b"a", h);
        h = xxh64(b"a", h);
        h = xxh64(b"b", h);
        assert_eq!(
            interp.run("bbbb", &mut Echo, false, &no_kill()).unwrap(),
            RespValue::Integer(h as i64)
        );
        interp
            .define("cccc", b"return dragonfly.ihash(5, false, 'get', 'k')")
            .unwrap();
        assert_eq!(
            interp.run("cccc", &mut Boom, false, &no_kill()).unwrap(),
            RespValue::Integer(xxh64(b"k", 5) as i64)
        );
    }

    #[test]
    fn dragonfly_lock_unlock_reach_dispatch() {
        #[derive(Default)]
        struct Recording {
            locks: Vec<Vec<Vec<u8>>>,
            unlocks: usize,
        }
        impl ScriptDispatch for Recording {
            fn dispatch(&mut self, _: Vec<Vec<u8>>) -> Result<RespValue, String> {
                Ok(RespValue::Integer(0))
            }
            fn lock(&mut self, keys: Vec<Vec<u8>>) -> Result<(), String> {
                self.locks.push(keys);
                Ok(())
            }
            fn unlock(&mut self) -> Result<(), String> {
                self.unlocks += 1;
                Ok(())
            }
        }

        let interp = SandboxedInterpreter::new().unwrap();
        interp
            .define(
                "aaaa",
                b"dragonfly.lock('k1', 'k2') dragonfly.unlock() return 1",
            )
            .unwrap();
        let mut rec = Recording::default();
        assert_eq!(
            interp.run("aaaa", &mut rec, false, &no_kill()).unwrap(),
            RespValue::Integer(1)
        );
        assert_eq!(rec.locks, vec![vec![b"k1".to_vec(), b"k2".to_vec()]]);
        assert_eq!(rec.unlocks, 1);
    }
}
