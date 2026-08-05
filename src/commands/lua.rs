//! Lua scripting engine backing EVAL/EVALSHA and the SCRIPT cache.
//!
//! Mirrors the reference implementation (`dragonfly/src/core/interpreter.cc`
//! and `dragonfly/src/server/script_mgr.cc`): a per-thread sandboxed Lua 5.4
//! interpreter, a SHA-1 keyed script cache with flags, and strict-global
//! enforcement installed exactly once per Lua state.

use std::collections::HashMap;
use std::cell::RefCell;
use std::os::raw::c_char;

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

// ---------------------------------------------------------------------------
// SHA-1
// ---------------------------------------------------------------------------

/// Compute a SHA-1 digest. Pure Rust, no new dependencies (mirrors `EVP_Digest`).
fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];

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
        let (mut a, mut b, mut c, mut d, mut e) =
            (h[0], h[1], h[2], h[3], h[4]);
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
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
        ScriptParams { atomic: true, undeclared_keys: false, float_as_int: false }
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

/// SHA-1 keyed script cache (`ScriptMgr`). Shared behind a `Mutex` between the
/// IO thread (SCRIPT subcommands) and the coordinator thread (EVAL).
#[derive(Debug, Default)]
pub struct ScriptMgr {
    scripts: HashMap<String, Script>,
    /// Flag-only entries created by SCRIPT FLAGS before the script is loaded.
    params: HashMap<String, ScriptParams>,
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
    pub fn new() -> Self {
        ScriptMgr::default()
    }

    /// Parse the `--!df flags=` prefix a script may start with
    /// (`DeduceParams` in `script_mgr.cc`). `Ok(None)` when the prefix is
    /// absent or the flags line has no trailing whitespace.
    pub fn deduce_params(body: &[u8]) -> Result<Option<ScriptParams>, String> {
        let body = trim_ascii_start(body);
        const PREFIX: &[u8] = b"--!df flags=";
        if !body.starts_with(PREFIX) {
            return Ok(None);
        }
        let rest = &body[PREFIX.len()..];
        let len = rest.iter().position(|&b| b.is_ascii_whitespace()).unwrap_or(rest.len());
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
        self.scripts.insert(sha.clone(), Script { sha: sha.clone(), body, params });
        self.params.insert(sha, params);
    }

    pub fn exists(&self, sha: &str) -> bool {
        self.scripts.contains_key(sha)
    }

    pub fn find(&self, sha: &str) -> Option<&Script> {
        self.scripts.get(sha)
    }

    pub fn params(&self, sha: &str) -> Option<ScriptParams> {
        self.scripts.get(sha).map(|s| s.params).or_else(|| self.params.get(sha).copied())
    }

    /// `(sha, body)` for every cached script, unordered (`ScriptMgr::GetAll`).
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

/// The strict-global enforcement chunk from `interpreter.cc:453` (`@enable_strict_lua`).
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
/// (`ScriptMgr::Insert`); parsing is identical to the sandboxed state.
pub fn compile_check(body: &[u8]) -> Result<(), String> {
    let lua = Lua::new();
    lua.load(script_chunk("check", body))
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
        self.exec(STRICT, "@enable_strict_lua")?;
        self.install_protected_funcs()?;
        // `loadfile`/`dofile` are disabled (`interpreter.cc:512`).
        self.lua.globals().set("loadfile", Value::Nil).map_err(|e| e.to_string())?;
        self.lua.globals().set("dofile", Value::Nil).map_err(|e| e.to_string())?;
        self.exec(POLYFILLS, "@dfly_polyfills")?;
        self.exec(RUNNER, "@dfly_runner")?;
        self.setup_redis_table()?;
        Ok(())
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
                    return Err(mlua::Error::runtime("rawset requires a table and two arguments"));
                }
                let Value::Table(t) = &args[0] else { unreachable!() };
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
                let Value::Table(t) = &args[0] else { unreachable!() };
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
        globals.set("setmetatable", setmetatable).map_err(|e| e.to_string())?;
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
            .create_function(|lua, args: MultiValue| single_field_table(lua, "err", args))
            .map_err(|e| e.to_string())?;
        let status_reply = self
            .lua
            .create_function(|lua, args: MultiValue| single_field_table(lua, "ok", args))
            .map_err(|e| e.to_string())?;
        t.raw_set("error_reply", error_reply).map_err(|e| e.to_string())?;
        t.raw_set("status_reply", status_reply).map_err(|e| e.to_string())?;

        let replicate = self
            .lua
            .create_function(|_, _: ()| -> mlua::Result<i64> { Ok(1) })
            .map_err(|e| e.to_string())?;
        t.raw_set("replicate_commands", replicate).map_err(|e| e.to_string())?;

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

        t.raw_set("LOG_DEBUG", 0).map_err(|e| e.to_string())?;
        t.raw_set("LOG_VERBOSE", 1).map_err(|e| e.to_string())?;
        t.raw_set("LOG_NOTICE", 2).map_err(|e| e.to_string())?;
        t.raw_set("LOG_WARNING", 3).map_err(|e| e.to_string())?;

        self.lua.globals().set("redis", t).map_err(|e| e.to_string())?;
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
                let cmd_args = match prepare_args(args) {
                    Ok(a) => a,
                    Err(_) => raise_string_error(lua, ARG_TYPE_ERR.into()),
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
                let cmd_args = prepare_args(args)?;
                match dispatch.borrow_mut().dispatch(cmd_args) {
                    Ok(v) => resp_to_lua(lua, v),
                    Err(msg) => {
                        let t = lua.create_table()?;
                        t.raw_set("err", lua.create_string(msg.as_bytes())?)?;
                        Ok(Value::Table(t))
                    }
                }
            })?;
            let redis: Table = lua.globals().get("redis")?;
            redis.set("call", call)?;
            redis.set("pcall", pcall)?;
            let runner: Function = lua.globals().get("__dfly__run")?;
            let v: Value = runner.call(sha)?;
            serialize_value(lua, v, float_as_int, 0)
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
        ffi::lua_pushlstring(raw.state(), msg.as_ptr() as *const c_char, len);
        std::mem::drop(msg);
        ffi::lua_error(raw.state())
    })
}

/// Strip the mlua-specific error decorations so script errors read like the
/// reference's (`@user_script:<line>: <message>`): mlua prefixes `runtime
/// error:` and appends a `stack traceback:` block.
fn clean_script_error(msg: &str) -> String {
    let msg = msg.strip_prefix("runtime error: ").unwrap_or(msg);
    msg.split("\nstack traceback:").next().unwrap_or(msg).to_string()
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
fn single_field_table(lua: &Lua, field: &str, args: MultiValue) -> mlua::Result<Value> {
    if args.len() != 1 || !matches!(&args[0], Value::String(_)) {
        let t = lua.create_table()?;
        t.raw_set("err", lua.create_string("wrong number or type of arguments")?)?;
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
}

/// Convert `redis.call` args to command argument bytes
/// (`Interpreter::PrepareArgs`). Only strings and numbers are accepted.
fn prepare_args(args: MultiValue) -> mlua::Result<Vec<Vec<u8>>> {
    let mut out = Vec::with_capacity(args.len());
    for v in args.iter() {
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
    b.iter().filter(|&&c| c != b'\r' && c != b'\n').map(|&c| c as char).collect()
}

/// Format a Lua error table value for the wire: the reference's
/// `EvalSerializer::OnError` passes the message through, adding a leading `-`
/// when missing.
fn fmt_error(b: &[u8]) -> String {
    let s = strip_crlf(b);
    if s.starts_with('-') { s } else { format!("-{s}") }
}

/// Serialize a script return value to RESP (`SerializeResult` +
/// `EvalSerializer`). Depth is capped at 128 like `IsResultSafe`.
fn serialize_value(
    _lua: &Lua,
    v: Value,
    float_as_int: bool,
    depth: usize,
) -> mlua::Result<RespValue> {
    if depth > 128 {
        return Err(mlua::Error::runtime("reached lua stack limit"));
    }
    Ok(match v {
        Value::Nil => RespValue::Nil,
        Value::Boolean(true) => RespValue::Integer(1),
        Value::Boolean(false) => RespValue::Nil,
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
                        serialize_value(_lua, k, float_as_int, depth + 1)?,
                        serialize_value(_lua, val, float_as_int, depth + 1)?,
                    ));
                }
                return Ok(RespValue::Map(pairs));
            }
            let len = t.raw_len();
            let mut items = Vec::with_capacity(len);
            for i in 1..=len {
                items.push(serialize_value(_lua, t.raw_get::<Value>(i)?, float_as_int, depth + 1)?);
            }
            RespValue::Array(items)
        }
        _ => RespValue::Nil,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
            Some(ScriptParams { atomic: false, undeclared_keys: false, float_as_int: true })
        );
        assert_eq!(
            ScriptMgr::deduce_params(b"--!df flags=allow-undeclared-keys  return 1").unwrap(),
            Some(ScriptParams { undeclared_keys: true, ..Default::default() })
        );
        // Flags line running to EOF is treated as absent.
        assert_eq!(ScriptMgr::deduce_params(b"--!df flags=legacy-float").unwrap(), None);
        assert!(ScriptMgr::deduce_params(b"--!df flags=bogus\nreturn 1").is_err());
    }

    #[test]
    fn apply_flags_errors() {
        let mut p = ScriptParams::default();
        assert_eq!(p.apply_flags("allow-undeclared-keys;disable-atomicity"), Ok(()));
        assert!(!p.atomic && p.undeclared_keys);
        assert_eq!(p.apply_flags("no-writes"), Ok(()));
        assert_eq!(p.apply_flags("bogus"), Err("Invalid flag: bogus".into()));
    }

    #[test]
    fn sandbox_rejects_bad_globals() {
        let interp = SandboxedInterpreter::new().unwrap();
        let run = |body: &str| {
            interp.define("aaaa", body.as_bytes()).unwrap();
            struct Noop;
            impl ScriptDispatch for Noop {
                fn dispatch(&mut self, _: Vec<Vec<u8>>) -> Result<RespValue, String> {
                    Err("ERR noop".into())
                }
            }
            interp.run("aaaa", &mut Noop, false)
        };
        assert_eq!(run("return 1 + 2").unwrap(), RespValue::Integer(3));
        // Missing global read -> strict error.
        let err = run("return no_such").unwrap_err();
        assert!(err.contains("Script attempted to access nonexistent global variable 'no_such'"), "{err}");
        // Global write from inside a function -> strict error.
        let err = run("x = 5 return x").unwrap_err();
        assert!(err.contains("Script attempted to create global variable 'x'"), "{err}");
        // debug is hidden.
        let err = run("return debug").unwrap_err();
        assert!(err.contains("Script attempted to access nonexistent global variable 'debug'"), "{err}");
        // loadfile/dofile are nilled, so reading them trips the strict guard.
        let err = run("return loadfile").unwrap_err();
        assert!(err.contains("nonexistent global variable 'loadfile'"), "{err}");
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
        struct Noop;
        impl ScriptDispatch for Noop {
            fn dispatch(&mut self, _: Vec<Vec<u8>>) -> Result<RespValue, String> {
                Err("ERR noop".into())
            }
        }
        assert_eq!(interp.run("bbbb", &mut Noop, false).unwrap(), RespValue::Integer(42));
    }
}
