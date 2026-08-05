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
use mlua::{Function, Lua, MultiValue, StdLib, Table, Value};

use crate::error::RespValue;
use crate::util::{format_double, itoa};

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
    /// `FUNCTION KILL`: set by the IO thread, polled by the coordinator between
    /// `redis.call` dispatches. `Arc` so the coordinator can read it without
    /// holding the mgr mutex on every subcommand.
    kill: Arc<AtomicBool>,
    /// Per-SHA script run durations (`SCRIPT LATENCY`, like the reference's
    /// `ServerState::call_latency_histos_`).
    latency: HashMap<String, LatencyStats>,
    /// `--lua_auto_async`: rewrite statement-context `redis.call`/`redis.pcall`
    /// into `redis.acall`/`redis.apcall` at load time (`FLAGS_lua_auto_async`).
    /// Defaults off; applies only to atomic scripts.
    pub lua_auto_async: bool,
}

/// Aggregated script run time in microseconds, keyed by script SHA.
#[derive(Debug, Clone, Copy, Default)]
pub struct LatencyStats {
    pub count: u64,
    pub total_usec: u64,
    pub min_usec: u64,
    pub max_usec: u64,
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

impl ScriptMgr {
    #[must_use]
    pub fn new() -> Self {
        ScriptMgr::default()
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

    /// `(sha, body)` for every cached script, unordered (`ScriptMgr::GetAll`).
    #[must_use]
    pub fn get_all(&self) -> Vec<(String, Vec<u8>)> {
        self.scripts
            .iter()
            .map(|(sha, s)| (sha.clone(), s.body.clone()))
            .collect()
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

    /// Full `ScriptParams` for a body, applying the hardcoded SHA overrides
    /// (`ScriptMgr::Insert`).
    pub fn deduce_and_override(body: &[u8]) -> Result<ScriptParams, String> {
        let sha = sha1_hex(body);
        let mut params = ScriptMgr::deduce_params(body)?.unwrap_or_default();
        if HARDCODED_UNDECLARED.contains(&sha.as_str()) {
            params.undeclared_keys = true;
        }
        Ok(params)
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
    /// polls this between `redis.call` dispatches (a CPU-bound tight loop that
    /// never calls out is not interruptible).
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
        let e = self
            .latency
            .entry(sha.to_string())
            .or_insert_with(|| LatencyStats {
                count: 0,
                total_usec: 0,
                min_usec: usec,
                max_usec: usec,
            });
        e.count += 1;
        e.total_usec += usec;
        e.min_usec = e.min_usec.min(usec);
        e.max_usec = e.max_usec.max(usec);
    }

    /// Per-SHA latency stats for `SCRIPT LATENCY`.
    #[must_use]
    pub fn latency(&self) -> &HashMap<String, LatencyStats> {
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
    /// Create a state and run the full bootstrap exactly once.
    pub fn new() -> Result<Self, String> {
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
        interp.bootstrap()?;
        Ok(interp)
    }

    fn bootstrap(&self) -> Result<(), String> {
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
        self.exec(RUNNER, "@dfly_runner")?;
        self.setup_redis_table()?;
        self.setup_function_table()?;
        Ok(())
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
    fn setup_redis_table(&self) -> Result<(), String> {
        let t = self.lua.create_table().map_err(|e| e.to_string())?;

        let sha1hex = self
            .lua
            .create_function(|lua, args: MultiValue| -> mlua::Result<Value> {
                if args.len() != 1 {
                    return Err(mlua::Error::runtime("wrong number of arguments"));
                }
                let Value::String(s) = &args[0] else {
                    return Err(mlua::Error::runtime("wrong number or type of arguments"));
                };
                Ok(Value::String(lua.create_string(sha1_hex(&s.as_bytes()))?))
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
            .create_function(|_, args: MultiValue| -> mlua::Result<Value> {
                if args.len() < 2 {
                    return Err(mlua::Error::runtime(
                        "redis.log() requires two arguments or more.",
                    ));
                }
                if !matches!(args[0], Value::Integer(_) | Value::Number(_)) {
                    return Err(mlua::Error::runtime(
                        "First argument must be a number (log level).",
                    ));
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
            let runner: Function = lua.globals().get("__dfly__run")?;
            let v: Value = runner.call(sha)?;
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
            redis.set("apcall", no_call)?;
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
            let v: Value = f.call((keys_t, args_t))?;
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
fn raise_string_error(lua: &Lua, msg: String) -> ! {
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
/// table on bad arguments (the reference returns the table, not a raise).
fn single_field_table(lua: &Lua, field: &str, args: &MultiValue) -> mlua::Result<Value> {
    if args.len() != 1 || !matches!(&args[0], Value::String(_)) {
        let t = lua.create_table()?;
        t.raw_set(
            "err",
            lua.create_string("wrong number or type of arguments")?,
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
}

/// Convert `redis.call` args to command argument bytes
/// (`Interpreter::PrepareArgs`). Only strings and numbers are accepted.
fn prepare_args(args: &MultiValue) -> mlua::Result<Vec<Vec<u8>>> {
    let mut out = Vec::with_capacity(args.len());
    for v in args {
        match v {
            Value::String(s) => out.push(s.as_bytes().to_vec()),
            Value::Integer(i) => out.push(itoa(*i)),
            Value::Number(d) => out.push(format_double(*d).into_bytes()),
            _ => return Err(mlua::Error::runtime(ARG_TYPE_ERR)),
        }
    }
    Ok(out)
}

/// Convert a subcommand reply into the Lua value a script observes
/// (`RedisTranslator`): status -> `{ok=...}`, error -> `{err=...}`, nil ->
/// `false`, integral doubles -> integers.
fn resp_to_lua(lua: &Lua, r: RespValue) -> mlua::Result<Value> {
    Ok(match r {
        RespValue::Nil => Value::Boolean(false),
        RespValue::Bool(b) => Value::Boolean(b),
        RespValue::Integer(i) => Value::Integer(i),
        RespValue::Double(d) => {
            if d.fract() == 0.0 && (i64::MIN as f64..=i64::MAX as f64).contains(&d) {
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
        let v = interp.run(sha, &mut d, false).unwrap();
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
            interp.run(sha, &mut d, false).unwrap(),
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
        let err = interp.run(sha, &mut Failing, false).unwrap_err();
        assert!(err.contains("ERR boom"), "{err}");
        interp
            .define(sha, b"redis.apcall('SET', KEYS[1], '1')\nreturn 1")
            .unwrap();
        let err = interp.run(sha, &mut Failing, false).unwrap_err();
        assert!(err.contains("ERR boom"), "{err}");
    }

    #[test]
    fn sandbox_rejects_bad_globals() {
        let interp = SandboxedInterpreter::new().unwrap();
        let run = |body: &str| {
            interp.define("aaaa", body.as_bytes()).unwrap();
            interp.run("aaaa", &mut Noop, false)
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
            interp.run("bbbb", &mut Noop, false).unwrap(),
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
                .run_function("add", &[b"k".to_vec()], &[b"a".to_vec()], &mut Get, false)
                .unwrap(),
            RespValue::Bulk(b"v:a".to_vec())
        );
        assert_eq!(
            interp
                .run_function("one", &[], &[], &mut Get, false)
                .unwrap(),
            RespValue::Integer(1)
        );
        // Unknown function name.
        assert!(
            interp
                .run_function("nope", &[], &[], &mut Get, false)
                .is_err()
        );
        // Reloading a library replaces the callback in place.
        let code2 = b"#!lua name=lib1\nredis.register_function('one', function() return 2 end)";
        interp.load_function_lib(code2).unwrap();
        assert_eq!(
            interp
                .run_function("one", &[], &[], &mut Get, false)
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
        let err = interp.run("aaaa", &mut Noop, false).unwrap_err();
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
            interp.run_function("f", &[], &[], &mut Get, false).unwrap(),
            RespValue::Integer(1)
        );

        // purge_functions drops the callback (used when a REPLACE drops a name).
        interp.purge_functions(&["f".to_string()]);
        assert!(interp.run_function("f", &[], &[], &mut Get, false).is_err());
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
        let err = interp.run("aaaa", &mut Noop, false).unwrap_err();
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
        let err = interp.run("bbbb", &mut Noop, false).unwrap_err();
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
            (s1.count, s1.total_usec, s1.min_usec, s1.max_usec),
            (2, 400, 100, 300)
        );
        assert_eq!(mgr.latency()["sha2"].count, 1);
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
}
