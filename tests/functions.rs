//! `FUNCTION` / `FCALL` semantics integration tests over the in-process
//! harness (`tests/common/mod.rs`).
//!
//! The reference (`dragonfly/src/server/function_family_test.cc`) does not
//! exist upstream, so this file is authored from the port's own documented
//! semantics: the `local_function` admin matrix (`src/server/mod.rs`,
//! unit-tested as `function_reply` in `src/commands/exec/server.rs`) plus the
//! coordinator `execute_function` path (`src/server/coordinator.rs`).
//!
//! Coverage split exercised here:
//! - `FUNCTION` admin subcommands run on the IO thread (`local_function`,
//!   event_loop `handle_local`); `FCALL`/`FCALL_RO` run on the coordinator
//!   (`execute_function`) against the same shared `ScriptMgr`.
//! - `DFLY` (replication control, `dflycmd.cc`) has no replication stack in
//!   the port and is rejected; `DFLY FLOW/SYNC/STARTSTABLE` partial paths are
//!   covered separately by `tests/replication.rs`.

mod common;

use common::*;

/// Collect every string-ish leaf of a reply (maps render as flat arrays in
/// RESP2), for presence checks.
fn texts(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Bulk(Some(b)) => out.push(String::from_utf8_lossy(b).into_owned()),
        Value::Simple(s) => out.push(s.clone()),
        Value::Array(Some(items)) => items.iter().for_each(|i| texts(i, out)),
        _ => {}
    }
}

/// A read function: echoes `keys[1] .. ':' .. args[1]`.
const LIB_ECHO: &str = "#!lua name=lib1\n\
    redis.register_function('echo', function(keys, args)\n\
      return keys[1] .. ':' .. args[1]\n\
    end)";

/// A function with no declared keys: `'hello ' .. args[1]`.
const LIB_GREET: &str = "#!lua name=lib2\n\
    redis.register_function('greet', function(keys, args)\n\
      return 'hello ' .. args[1]\n\
    end)";

/// A write function: SETs both declared keys and returns the first.
const LIB_SET2: &str = "#!lua name=libw\n\
    redis.register_function('set2', function(keys, args)\n\
      redis.call('set', keys[1], args[1])\n\
      redis.call('set', keys[2], args[2])\n\
      return redis.call('get', keys[1])\n\
    end)";

/// A function that writes to an undeclared key (must be rejected in atomic
/// mode).
const LIB_UNDECLARED: &str = "#!lua name=libu\n\
    redis.register_function('undeclared', function(keys, args)\n\
      redis.call('set', 'other', 'v')\n\
      return 1\n\
    end)";

/// A `no-writes` library attempting a write (must be rejected as read-only).
const LIB_RO_WRITE: &str = "#!lua name=libro flags=no-writes\n\
    redis.register_function('badwrite', function(keys, args)\n\
      redis.call('set', keys[1], args[1])\n\
      return 1\n\
    end)";

/// A function that calls `FUNCTION` (NOSCRIPT) from inside itself.
const LIB_CALLS_FUNCTION: &str = "#!lua name=libf\n\
    redis.register_function('f', function(keys, args)\n\
      return redis.call('function', 'list')\n\
    end)";

#[test]
fn function_load_fcall_roundtrip() {
    let mut ctx = Ctx::new();
    let lib = ctx.text(&["FUNCTION", "LOAD", LIB_ECHO]);
    assert_eq!(lib, "lib1");

    // FCALL passes declared keys and args to the callback.
    ctx.assert_text(&["FCALL", "echo", "1", "k", "v"], "k:v");
    // No keys, args only (function does not touch `keys`).
    ctx.assert_text(&["FUNCTION", "LOAD", LIB_GREET], "lib2");
    ctx.assert_text(&["FCALL", "greet", "0", "world"], "hello world");
    // FCALL_RO runs a read function identically.
    ctx.assert_text(&["FCALL_RO", "echo", "1", "k2", "v2"], "k2:v2");
}

#[test]
fn function_duplicates_replace_delete() {
    let mut ctx = Ctx::new();
    ctx.assert_text(&["FUNCTION", "LOAD", LIB_ECHO], "lib1");
    ctx.assert_err(&["FUNCTION", "LOAD", LIB_ECHO], "Library 'lib1' already exists");

    // A duplicate function name in another library is rejected.
    let lib2 = "#!lua name=lib2\n\
        redis.register_function('echo', function(keys, args) return 2 end)";
    ctx.assert_err(&["FUNCTION", "LOAD", lib2], "Function 'echo' already exists");

    // REPLACE redefines the same library without the duplicate error.
    ctx.assert_text(&["FUNCTION", "LOAD", "REPLACE", LIB_ECHO], "lib1");

    // DELETE frees the library; deleting it again errors.
    ctx.ok(&["FUNCTION", "DELETE", "lib1"]);
    ctx.assert_err(&["FUNCTION", "DELETE", "lib1"], "Library not found");
    // The freed name is reusable.
    ctx.assert_text(&["FUNCTION", "LOAD", lib2], "lib2");
    ctx.assert_int(&["FCALL", "echo", "0"], 2);
}

#[test]
fn function_admin_over_socket() {
    let mut ctx = Ctx::new();
    assert_eq!(ctx.arr(&["FUNCTION", "LIST"]).len(), 0);
    let help = ctx.arr(&["FUNCTION", "HELP"]);
    assert!(help.iter().any(|v| v.text().as_deref() == Some("FUNCTION <subcommand> [<arg> [value] [opt] ...]")));

    let bogus = ctx.err(&["FUNCTION", "BOGUS"]);
    assert!(
        bogus.contains("Unknown subcommand or wrong number of arguments for 'BOGUS'"),
        "{bogus}"
    );
    // KILL with nothing running is NOTBUSY (no `ERR ` prefix).
    assert!(ctx.err(&["FUNCTION", "KILL"]).starts_with("NOTBUSY"));

    ctx.assert_text(&["FUNCTION", "LOAD", LIB_ECHO], "lib1");

    // LIST renders libraries as (flattened) maps.
    let list = ctx.arr(&["FUNCTION", "LIST"]);
    assert_eq!(list.len(), 1, "one library listed");
    let mut t = Vec::new();
    texts(&list[0], &mut t);
    assert!(t.iter().any(|s| s == "library_name"), "{t:?}");
    assert!(t.iter().any(|s| s == "lib1"), "{t:?}");
    assert!(t.iter().any(|s| s == "echo"), "{t:?}");
    // LIBRARYNAME filters; a miss yields an empty list.
    assert_eq!(ctx.arr(&["FUNCTION", "LIST", "LIBRARYNAME", "lib1"]).len(), 1);
    assert_eq!(ctx.arr(&["FUNCTION", "LIST", "LIBRARYNAME", "nope"]).len(), 0);
    // WITHCODE carries the source.
    let with_code = ctx.arr(&["FUNCTION", "LIST", "LIBRARYNAME", "lib1", "WITHCODE"]);
    let mut t = Vec::new();
    texts(&with_code[0], &mut t);
    assert!(t.iter().any(|s| s == "library_code"), "{t:?}");

    // STATS reports the engine counts (maps flatten in RESP2).
    let stats = ctx.arr(&["FUNCTION", "STATS"]);
    let mut t = Vec::new();
    for v in &stats {
        texts(v, &mut t);
    }
    assert!(t.iter().any(|s| s == "running_script"), "{t:?}");
    assert!(t.iter().any(|s| s == "libraries_count"), "{t:?}");
    assert!(t.iter().any(|s| s == "functions_count"), "{t:?}");

    ctx.ok(&["FUNCTION", "FLUSH"]);
    assert_eq!(ctx.arr(&["FUNCTION", "LIST"]).len(), 0);
    // The flushed library is gone from FCALL too.
    ctx.assert_err(&["FCALL", "echo", "0"], "Function not found");
}

#[test]
fn function_replace_purges_dropped_names() {
    let mut ctx = Ctx::new();
    let lib = "#!lua name=lib1\n\
        redis.register_function('fa', function(keys, args) return 1 end)\n\
        redis.register_function('fb', function(keys, args) return 2 end)";
    ctx.assert_text(&["FUNCTION", "LOAD", lib], "lib1");
    ctx.assert_int(&["FCALL", "fa", "0"], 1);
    ctx.assert_int(&["FCALL", "fb", "0"], 2);

    // REPLACE drops `fb`; the coordinator must purge its stale callback.
    let lib1 = "#!lua name=lib1\n\
        redis.register_function('fa', function(keys, args) return 11 end)";
    ctx.assert_text(&["FUNCTION", "LOAD", "REPLACE", lib1], "lib1");
    ctx.assert_int(&["FCALL", "fa", "0"], 11);
    ctx.assert_err(&["FCALL", "fb", "0"], "Function not found");

    // The purged name is reusable by another library.
    let lib3 = "#!lua name=lib3\n\
        redis.register_function('fb', function(keys, args) return 22 end)";
    ctx.assert_text(&["FUNCTION", "LOAD", lib3], "lib3");
    ctx.assert_int(&["FCALL", "fb", "0"], 22);
}

#[test]
fn function_bad_payloads() {
    let mut ctx = Ctx::new();
    ctx.assert_err(&["FUNCTION", "LOAD", "return 1"], "Missing library metadata");
    ctx.assert_err(&["FUNCTION", "LOAD", "#!lua name=empty"], "No functions registered");
    ctx.assert_err(&["FUNCTION", "LOAD", "#!js name=x\n"], "Invalid engine type");
    ctx.assert_err(
        &["FUNCTION", "LOAD", "#!lua\nredis.register_function('x', 1)"],
        "Missing library name",
    );
    ctx.assert_err(
        &["FUNCTION", "LOAD", "#!lua name=l\nredis.register_function('x', 5)"],
        "Function callback must be a function",
    );
    ctx.assert_err(
        &["FUNCTION", "LOAD", "#!lua name=l\nredis.call('get', 'k')"],
        "redis.call is not allowed during function library load",
    );
}

#[test]
fn fcall_error_paths() {
    let mut ctx = Ctx::new();
    // Unknown function.
    ctx.assert_err(&["FCALL", "nope", "0"], "Function not found");
    // Malformed numkeys.
    ctx.assert_err(&["FCALL", "nope", "x"], "value is not an integer or out of range");
    ctx.assert_err(&["FCALL", "nope", "-1"], "value is not an integer or out of range");
    // numkeys greater than the available args.
    ctx.assert_err(&["FCALL", "nope", "2", "only"], "Number of keys can't be greater than number of args");

    ctx.assert_text(&["FUNCTION", "LOAD", LIB_ECHO], "lib1");
    ctx.assert_text(&["FCALL", "echo", "1", "k", "v"], "k:v");
    // A runtime Lua error carries the function name.
    ctx.assert_err(
        &["FCALL", "echo", "2", "a", "b"],
        "Error running function (call to echo)",
    );
}

#[test]
fn fcall_writes_declared_keys() {
    let mut ctx = Ctx::new();
    ctx.assert_text(&["FUNCTION", "LOAD", LIB_SET2], "libw");
    ctx.assert_text(&["FCALL", "set2", "2", "k1", "k2", "v1", "v2"], "v1");
    ctx.assert_text(&["GET", "k1"], "v1");
    ctx.assert_text(&["GET", "k2"], "v2");
    // The returned value is the post-write GET.
    ctx.assert_text(&["FCALL", "set2", "2", "k3", "k4", "a", "b"], "a");
    ctx.assert_int(&["DEL", "k1", "k2", "k3", "k4"], 4);
}

#[test]
fn fcall_undeclared_key_rejected() {
    let mut ctx = Ctx::new();
    ctx.assert_text(&["FUNCTION", "LOAD", LIB_UNDECLARED], "libu");
    ctx.assert_err(
        &["FCALL", "undeclared", "0"],
        "script tried accessing undeclared key, key: other",
    );
}

#[test]
fn fcall_read_only_enforcement() {
    let mut ctx = Ctx::new();
    // A no-writes function cannot write.
    ctx.assert_text(&["FUNCTION", "LOAD", LIB_RO_WRITE], "libro");
    ctx.assert_err(
        &["FCALL", "badwrite", "1", "k", "v"],
        "Write commands are not allowed from read-only scripts",
    );
    // FCALL_RO on a plain write function is read-only too.
    ctx.assert_text(&["FUNCTION", "LOAD", LIB_SET2], "libw");
    ctx.assert_err(
        &["FCALL_RO", "set2", "2", "k1", "v1", "k2", "v2"],
        "Write commands are not allowed from read-only scripts",
    );
}

#[test]
fn function_and_fcall_noscript_from_script() {
    let mut ctx = Ctx::new();
    // FUNCTION is NOSCRIPT: not callable from EVAL.
    ctx.assert_err(
        &["EVAL", "return redis.call('function', 'list')", "0"],
        "is not allowed from script",
    );
    // FCALL is NOSCRIPT: not callable from EVAL.
    ctx.assert_err(
        &["EVAL", "return redis.call('fcall', 'echo', 1, 'k', 'v')", "0"],
        "is not allowed from script",
    );
    // Nor from inside a running function.
    ctx.assert_text(&["FUNCTION", "LOAD", LIB_CALLS_FUNCTION], "libf");
    ctx.assert_err(
        &["FCALL", "f", "0"],
        "Error running function (call to f): This Redis command is not allowed from script",
    );
}

#[test]
fn dfly_replication_control_rejected() {
    let mut ctx = Ctx::new();
    // The replication-control protocol (`dflycmd.cc`) is unsupported; every
    // unrecognized path is rejected explicitly.
    ctx.assert_err(&["DFLY"], "wrong number of arguments");
    ctx.assert_err(&["DFLY", "BOGUS"], "DFLY replication control is not supported");
    ctx.assert_err(
        &["DFLY", "FLOW"],
        "wrong number of arguments for 'DFLY FLOW' command",
    );
}
