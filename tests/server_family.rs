//! Port of `dragonfly/src/server/server_family_test.cc`.
//!
//! Adaptations from the C++ original:
//! - The tracking suite runs RESP3 (`HELLO 3`); the remaining tests run RESP2.
//! - The slowlog tests are byte-identical: they set `slowlog_max_len` and
//!   `slowlog_log_slower_than`, then inspect `SLOWLOG GET` entries.
//! - The `SlowLogExecEval` EVAL sha is the reference's hardcoded value
//!   (`41e84cf...`), which validates the port's `sha1_hex` and the exact
//!   script bytes end to end.
//! - `ClientListAccepted` is adapted: the reference's mocked connection is
//!   invisible to `CLIENT LIST`, so every accepted form returns an empty string;
//!   the port lists its real connection, so the no-filter/normal/id forms are
//!   asserted to contain the connection's info line instead.
//! - `ClientListRejected` and `ClientInfoSingleDbField` are byte-identical.
//! - `ConfigNormalization`/`ConfigGetMemoryBytes` need no `FlagSaver`: each
//!   test spawns a fresh server, so `replica_priority`/`maxmemory` start at
//!   their defaults (100 / unset).
//! - `PubSubCommandErr` always runs the standalone-mode branch (the port has no
//!   cluster mode).
//! - `MemoryArenaSummary` asserts the upstream report *shape* (per-shard
//!   `Arena statistics for thread N` sections plus a machine-wide section);
//!   the port tracks no allocator arenas, so each section contains only the
//!   header and totals rows.
//! - `ClientPause` is not ported yet: the port has no `CLIENT PAUSE`.
//! - `ReadTcpInfo` / `GetTcpSocketInfoIPv6` are not ported: they exercise the
//!   Linux-only `/proc/net/{tcp,tcp6}` socket-info reporting.
//! - The `ClientTracking*` tests use the real push-message API: `CLIENT
//!   TRACKING` invalidations arrive as RESP3 push frames, drained by `cmd` into
//!   the client's push queue (`push_count`/`read_push`). Writes on a second
//!   connection are followed by `read_push` on the tracking client, replacing
//!   the reference's `AwaitFiberOnAll`; exact single-client sequences rely on
//!   the invalidation being appended before the triggering write's reply.

mod common;

use std::time::{Duration, Instant};

use common::*;

/// The rendered arguments array (`entry[3]`) of one SLOWLOG entry.
fn slowlog_args(entry: &Value) -> Vec<Value> {
    entry
        .arr()
        .and_then(|e| e.get(3))
        .and_then(Value::arr)
        .map_or_else(
            || panic!("expected slowlog entry shape, got {entry:?}"),
            <[Value]>::to_vec,
        )
}

fn lpush_ints(c: &mut Ctx, n: u32) -> Vec<String> {
    let mut cmd = vec!["LPUSH".to_string(), "mykey".to_string()];
    for i in 1..=n {
        cmd.push(i.to_string());
    }
    let args: Vec<&str> = cmd.iter().map(String::as_str).collect();
    c.int(&args);
    cmd
}

/// `SlowLogTruncation` (server_family_test.cc:106): the entry keeps at most
/// 31 arguments and 128 bytes per argument, with `"... (N more ...)"` suffix
/// pseudo-arguments.
#[test]
fn slowlog_truncation() {
    let mut c = Ctx::new();
    c.ok(&["config", "set", "slowlog_max_len", "3"]);
    c.ok(&["config", "set", "slowlog_log_slower_than", "0"]);

    // 32 args: no truncation.
    let cmd = lpush_ints(&mut c, 30);
    let slowlog = c.arr(&["slowlog", "get"]);
    let got = slowlog_args(&slowlog[0]);
    assert_eq!(
        got.iter().map(|a| a.text().unwrap()).collect::<Vec<_>>(),
        cmd
    );

    // 33 args: truncated to 31 stored args + the pseudo-argument.
    lpush_ints(&mut c, 31);
    let slowlog = c.arr(&["slowlog", "get"]);
    let got = slowlog_args(&slowlog[0]);
    assert_eq!(got.len(), 32);
    assert_eq!(got[31].text().as_deref(), Some("... (2 more arguments)"));
    assert_eq!(got[0].text().as_deref(), Some("LPUSH"));

    // 128-byte arg: stored as-is.
    let at_limit = vec![b'A'; 128];
    c.run_b(&[b"lpush".to_vec(), b"key1".to_vec(), at_limit.clone()]);
    let slowlog = c.arr(&["slowlog", "get"]);
    let got = slowlog_args(&slowlog[0]);
    assert_eq!(got[2].bulk(), Some(at_limit.as_slice()));

    // 129-byte arg: truncated to 110 bytes + the suffix.
    let over_limit = vec![b'A'; 129];
    c.run_b(&[b"lpush".to_vec(), b"key2".to_vec(), over_limit]);
    let slowlog = c.arr(&["slowlog", "get"]);
    let got = slowlog_args(&slowlog[0]);
    let mut expected = vec![b'A'; 110];
    expected.extend_from_slice(b"... (1 more bytes)");
    assert_eq!(got[2].bulk(), Some(expected.as_slice()));
}

/// `SlowLogMaxLengthZero` (server_family_test.cc:147): a zero max length
/// disables the slowlog entirely.
#[test]
fn slowlog_max_length_zero() {
    let mut c = Ctx::new();
    c.ok(&["config", "set", "slowlog_max_len", "0"]);
    c.ok(&["config", "set", "slowlog_log_slower_than", "0"]);
    c.ok(&["slowlog", "reset"]);

    c.ok(&["set", "foo", "bar"]);
    let slowlog = c.run(&["slowlog", "get"]);
    assert_eq!(slowlog.arr().unwrap().len(), 0);
}

/// `SlowLogGetLen` (server_family_test.cc:163): GET count handling, including
/// `0` (empty), `-1` (all) and values below `-1` (syntax error).
#[test]
fn slowlog_get_len() {
    let mut c = Ctx::new();
    c.ok(&["config", "set", "slowlog_max_len", "3"]);
    c.ok(&["config", "set", "slowlog_log_slower_than", "0"]);

    for i in 1..=3 {
        c.assert_int(&["lpush", "mykey", &i.to_string()], i);
    }

    assert_eq!(c.run(&["slowlog", "get", "0"]).arr().unwrap().len(), 0);
    assert_eq!(c.run(&["slowlog", "get", "-1"]).arr().unwrap().len(), 3);
    assert_eq!(
        c.err(&["slowlog", "get", "-2"]),
        "ERR count should be greater than or equal to -1"
    );
}

/// `SlowLogLen` (server_family_test.cc:187).
#[test]
fn slowlog_len() {
    let mut c = Ctx::new();
    c.ok(&["config", "set", "slowlog_max_len", "3"]);
    c.ok(&["config", "set", "slowlog_log_slower_than", "0"]);
    c.ok(&["slowlog", "reset"]);

    for i in 1..=3 {
        c.assert_int(&["lpush", "mykey", &i.to_string()], i);
    }
    c.assert_int(&["slowlog", "len"], 3);
}

/// `SlowLogMinusOneDisabled` (server_family_test.cc:203): a negative
/// `slowlog_log_slower_than` disables the slowlog.
#[test]
fn slowlog_minus_one_disabled() {
    let mut c = Ctx::new();
    c.ok(&["config", "set", "slowlog_max_len", "3"]);
    c.ok(&["config", "set", "slowlog_log_slower_than", "-1"]);
    c.ok(&["slowlog", "reset"]);

    for i in 1..=3 {
        c.assert_int(&["lpush", "mykey", &i.to_string()], i);
    }

    assert_eq!(c.run(&["slowlog", "get"]).arr().unwrap().len(), 0);
    c.assert_int(&["slowlog", "len"], 0);
}

/// `SlowLogExecEval` (server_family_test.cc:224): EXEC and EVAL slowlog
/// entries carry augmented stats arguments.
#[test]
fn slowlog_exec_eval() {
    let mut c = Ctx::new();
    c.ok(&["config", "set", "slowlog_max_len", "20"]);
    c.ok(&["config", "set", "slowlog_log_slower_than", "0"]);

    c.run(&["multi"]);
    c.run(&["set", "first", "ok"]);
    c.run(&["set", "second2", "ok"]);
    c.run(&["get", "third3"]);
    let exec = c.run(&["exec"]);
    assert_eq!(exec.arr().unwrap().len(), 3);

    // Byte-identical to the C++ test's raw string; its SHA-1 is the
    // reference's hardcoded value below.
    let script = r"
for i, key in ipairs(KEYS) do
  redis.call('GET', key)
end
for i, key in ipairs(KEYS) do
  redis.call('SET', key, 'some-data')
end
return 'OK';
    ";
    let resp = c.run_b(&[
        b"EVAL".to_vec(),
        script.as_bytes().to_vec(),
        b"3".to_vec(),
        b"first".to_vec(),
        b"second2".to_vec(),
        b"third3".to_vec(),
        b"second2".to_vec(),
    ]);
    assert_eq!(resp.text().as_deref(), Some("OK"));

    let mut found = 0;
    for entry in c.arr(&["slowlog", "get"]) {
        let args: Vec<String> = slowlog_args(&entry)
            .iter()
            .map(|a| a.text().unwrap())
            .collect();
        match args.first().map(String::as_str) {
            Some("EXEC") => {
                assert_eq!(args, vec!["EXEC", "num_cmds: 3", "is_write: 1"]);
                found += 1;
            }
            Some("EVAL") => {
                assert_eq!(
                    args,
                    vec![
                        "EVAL",
                        "41e84cf7973712deda6c1737a69bd1365eeb060f",
                        "num_cmds: 6",
                        "slow_cmds: 6",
                        "tx_mode: 2",
                        "tx_shards: 2",
                        "is_write: 1",
                        "lock_tags: 3",
                        "3",
                        "first",
                        "second2",
                        "third3",
                        "second2",
                    ]
                );
                found += 1;
            }
            _ => {}
        }
    }
    assert_eq!(found, 2);
}

/// `ClientListAccepted` (server_family_test.cc:289): every filter form is
/// accepted without error. Adapted: the reference's mocked connection is
/// invisible to `CLIENT LIST` (so all forms return `""`); here the real
/// connection is listed, so forms matching it return its info line.
#[test]
fn client_list_accepted() {
    let mut c = Ctx::new();

    let list = c.text(&["CLIENT", "LIST"]);
    assert!(list.contains("id=1"), "{list}");

    let normal = c.text(&["CLIENT", "LIST", "TYPE", "normal"]);
    assert!(normal.contains("flags=N"), "{normal}");

    for filtered in ["master", "replica", "slave", "pubsub"] {
        assert_eq!(
            c.text(&["CLIENT", "LIST", "TYPE", filtered]),
            "",
            "TYPE {filtered}"
        );
    }

    let by_id = c.text(&["CLIENT", "LIST", "ID", "1"]);
    assert!(by_id.contains("id=1"), "{by_id}");

    let by_ids = c.text(&["CLIENT", "LIST", "ID", "1", "2", "3"]);
    assert!(by_ids.contains("id=1"), "{by_ids}");
}

/// `ClientListRejected` (server_family_test.cc:305): filter parse errors,
/// byte-identical to the reference.
#[test]
fn client_list_rejected() {
    let mut c = Ctx::new();
    let bad: [(&[&str], &str); 6] = [
        (
            &["CLIENT", "LIST", "TYPE", "bogus"],
            "Unknown client type 'bogus'",
        ),
        (&["CLIENT", "LIST", "TYPE"], "syntax error"),
        (&["CLIENT", "LIST", "ID"], "syntax error"),
        (&["CLIENT", "LIST", "ID", "abc"], "Invalid client ID"),
        (
            &["CLIENT", "LIST", "TYPE", "normal", "ID", "1"],
            "syntax error",
        ),
        (&["CLIENT", "LIST", "FOO"], "syntax error"),
    ];
    for (args, msg) in bad {
        c.assert_err(args, msg);
    }
}

/// `ClientInfoSingleDbField` (server_family_test.cc:319): `CLIENT INFO`
/// returns a single line with exactly one `db=` field and no trailing newline.
#[test]
fn client_info_single_db_field() {
    let mut c = Ctx::new();
    let info = c.text(&["CLIENT", "INFO"]);
    assert_eq!(info.matches(" db=").count(), 1, "{info}");
    assert!(!info.ends_with("\r\n"), "{info}");
}

/// `ClientPause` (server_family_test.cc:271): `CLIENT PAUSE <ms>` gates every
/// command until the timeout expires; the `WRITE` mode gates only writes, so
/// reads slip through while writes block. The reference blocks each connection's
/// fiber; the port blocks the single IO-thread dispatcher on the shared gate.
#[test]
fn client_pause() {
    let mut c = Ctx::new();

    let start = Instant::now();
    expect_ok(&c.run(&["CLIENT", "PAUSE", "50"]));
    expect_null(&c.run(&["GET", "key"]));
    assert!(start.elapsed() > Duration::from_millis(50));

    let start = Instant::now();
    expect_ok(&c.run(&["CLIENT", "PAUSE", "50", "WRITE"]));
    let get_start = Instant::now();
    expect_null(&c.run(&["GET", "key"]));
    assert!(
        get_start.elapsed() < Duration::from_millis(50),
        "a read must not be gated by a WRITE pause"
    );
    expect_ok(&c.run(&["SET", "key", "value2"]));
    assert!(start.elapsed() > Duration::from_millis(50));
}

/// `ConfigNormalization` (server_family_test.cc:672): dashes and underscores
/// are interchangeable in CONFIG parameter names.
#[test]
fn config_normalization() {
    let mut c = Ctx::new();

    let get = |c: &mut Ctx, pattern: &str| c.arr(&["config", "get", pattern]);

    for pattern in ["replica-priority", "replica_priority"] {
        let got = get(&mut c, pattern);
        assert_eq!(
            got[0].text().as_deref(),
            Some("replica_priority"),
            "{pattern}"
        );
        assert_eq!(got[1].text().as_deref(), Some("100"), "{pattern}");
    }

    c.ok(&["config", "set", "replica-priority", "7"]);
    for pattern in ["replica-priority", "replica_priority"] {
        let got = get(&mut c, pattern);
        assert_eq!(got[1].text().as_deref(), Some("7"), "{pattern}");
    }

    c.ok(&["config", "set", "replica_priority", "13"]);
    for pattern in ["replica-priority", "replica_priority"] {
        let got = get(&mut c, pattern);
        assert_eq!(got[1].text().as_deref(), Some("13"), "{pattern}");
    }
}

/// `ConfigGetMemoryBytes` (server_family_test.cc:702): human-readable memory
/// sizes are stored and reported as numeric bytes.
#[test]
fn config_get_memory_bytes() {
    let mut c = Ctx::new();

    c.ok(&["config", "set", "maxmemory", "1GB"]);
    let got = c.arr(&["config", "get", "maxmemory"]);
    assert_eq!(got[0].text().as_deref(), Some("maxmemory"));
    assert_eq!(got[1].text().as_deref(), Some("1073741824"));

    c.ok(&["config", "set", "maxmemory", "512MB"]);
    let got = c.arr(&["config", "get", "maxmemory"]);
    assert_eq!(got[1].text().as_deref(), Some("536870912"));
}

/// `CommandDocsOk` (server_family_test.cc:718).
#[test]
fn command_docs_ok() {
    let mut c = Ctx::new();
    c.assert_err(&["command", "docs"], "COMMAND DOCS Not Implemented");
}

/// `PubSubCommandErr` (server_family_test.cc:722): SHARD* subcommands are
/// rejected in standalone mode; unknown subcommands get the generic error.
#[test]
fn pubsub_command_err() {
    let mut c = Ctx::new();
    c.assert_err(
        &["PUBSUB", "SHARDCHANNELS"],
        "PUBSUB SHARDCHANNELS is not supported in non cluster mode",
    );
    c.assert_err(
        &["PUBSUB", "SHARDNUMSUB"],
        "PUBSUB SHARDNUMSUB is not supported in non cluster mode",
    );
    c.assert_err(
        &["PUBSUB", "INVALIDSUBCOMMAND"],
        "Unknown subcommand or wrong number of arguments for 'INVALIDSUBCOMMAND'. Try PUBSUB HELP.",
    );
}

/// `InfoMultipleSections` (server_family_test.cc:735): querying several valid
/// sections renders each of them.
#[test]
fn info_multiple_sections() {
    let mut c = Ctx::new();
    c.ok(&["set", "foo", "bar"]);
    let info = c.text(&["info", "replication", "persistence"]);
    assert!(info.contains("# Replication"), "{info}");
    assert!(info.contains("# Persistence"), "{info}");
}

/// `InfoMultipleSectionsInvalid` (server_family_test.cc:744): an unknown
/// section name is skipped.
#[test]
fn info_multiple_sections_invalid() {
    let mut c = Ctx::new();
    c.ok(&["set", "foo", "bar"]);
    let info = c.text(&["info", "replication", "invalidsection"]);
    assert!(info.contains("# Replication"), "{info}");
    assert!(!info.contains("# invalidsection"), "{info}");
}

/// `DebugPopulateZeroValSize` (server_family_test.cc:754): `val_size == 0`
/// must be rejected, not crash the server.
#[test]
fn debug_populate_zero_val_size() {
    let mut c = Ctx::new();
    c.assert_err(
        &["DEBUG", "POPULATE", "1", "key", "0"],
        "val_size must be positive",
    );
}

/// `MemoryParserErrorHandling` (server_family_test.cc:788).
#[test]
fn memory_parser_error_handling() {
    let mut c = Ctx::new();
    c.assert_err(
        &["MEMORY", "DEFRAGMENT", "not-a-float"],
        "not a valid float",
    );
}

/// `MemoryArenaSummary` (server_family_test.cc:760): SUMMARY reports a section
/// per shard plus a machine-wide section; bare MEMORY ARENA reports the
/// `Count`-style block list; trailing arguments are syntax errors.
#[test]
fn memory_arena_summary() {
    let mut c = Ctx::new();

    let response = c.text(&["MEMORY", "ARENA", "SUMMARY"]);
    assert!(response.contains("BlockSize"), "{response}");
    // Ctx::new spawns 2 shards.
    for shard_id in 0..2 {
        assert!(
            response.contains(&format!("Arena statistics for thread {shard_id}")),
            "{response}"
        );
    }
    assert!(
        response.contains("Arena statistics for machine"),
        "{response}"
    );

    c.assert_err(&["MEMORY", "ARENA", "SUMMARY", "0"], "syntax error");
    c.assert_err(&["MEMORY", "ARENA", "SUMMARY", "X"], "syntax error");

    let backing = c.text(&["MEMORY", "ARENA", "SUMMARY", "BACKING"]);
    assert!(backing.contains("BlockSize"), "{backing}");

    c.assert_err(
        &["MEMORY", "ARENA", "SUMMARY", "BACKING", "0"],
        "syntax error",
    );

    let arena = c.text(&["MEMORY", "ARENA"]);
    assert!(arena.contains("Count"), "{arena}");
}

/// `InfoReplicationMemoryNoReplicas` (server_family_test.cc:792): the memory
/// section reports zero buffer bytes without replicas.
#[test]
fn info_replication_memory_no_replicas() {
    let mut c = Ctx::new();
    let info = c.text(&["INFO", "MEMORY"]);
    assert!(
        info.contains("replication_streaming_buffer_bytes:0"),
        "{info}"
    );
    assert!(
        info.contains("replication_full_sync_buffer_bytes:0"),
        "{info}"
    );
}

/// `InfoReplicationMemoryOnlyInMemorySection` (server_family_test.cc:799): the
/// replication buffer fields appear only in the memory section (and the
/// default/ALL output), never in the replication section itself.
#[test]
fn info_replication_memory_only_in_memory_section() {
    let mut c = Ctx::new();
    assert!(
        !c.text(&["INFO", "REPLICATION"])
            .contains("replication_streaming_buffer_bytes")
    );
    assert!(
        c.text(&["INFO", "MEMORY"])
            .contains("replication_streaming_buffer_bytes")
    );
    assert!(
        c.text(&["INFO"])
            .contains("replication_streaming_buffer_bytes")
    );
    assert!(
        c.text(&["INFO", "ALL"])
            .contains("replication_streaming_buffer_bytes")
    );
}

/// `InfoCommandAndLatencyStatsGating` (server_family_test.cc:811): COMMANDSTATS
/// is a hidden section (rendered only when named or via ALL), while
/// LATENCYSTATS appears in the default INFO output.
#[test]
fn info_command_and_latency_stats_gating() {
    let mut c = Ctx::new();
    for i in 0..5 {
        c.ok(&["set", &format!("k{i}"), "v"]);
        c.run(&["get", &format!("k{i}")]);
    }
    c.run(&["ping"]);

    let def = c.text(&["INFO"]);
    assert!(!def.contains("# Commandstats"), "{def}");
    assert!(!def.contains("cmdstat_"), "{def}");
    assert!(def.contains("# Latencystats"), "{def}");

    let stats = c.text(&["INFO", "STATS"]);
    assert!(!stats.contains("cmdstat_"), "{stats}");
    assert!(!stats.contains("# Latencystats"), "{stats}");
    assert!(!stats.contains("latency_percentiles_usec_"), "{stats}");

    assert!(c.text(&["INFO", "COMMANDSTATS"]).contains("# Commandstats"));
    assert!(c.text(&["INFO", "LATENCYSTATS"]).contains("# Latencystats"));
    let all = c.text(&["INFO", "ALL"]);
    assert!(all.contains("# Commandstats"), "{all}");
    assert!(all.contains("# Latencystats"), "{all}");
}

/// Extract the `calls=` counter for a `cmdstat_*` line
/// (`InfoCommandStatsAggregation`'s `extract_calls` helper).
fn extract_calls(info: &str, stat: &str) -> i64 {
    const NEEDLE: &str = "calls=";
    let Some(pos) = info.find(stat) else {
        return -1;
    };
    let rest = &info[pos..];
    let Some(cpos) = rest.find(NEEDLE) else {
        return -1;
    };
    let rest = &rest[cpos + NEEDLE.len()..];
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().unwrap_or(0)
}

/// `InfoCommandStatsAggregation` (server_family_test.cc:842): per-command
/// counters aggregate across connections and survive `CONFIG RESETSTAT`.
#[test]
fn info_command_stats_aggregation() {
    const K_GETS: i64 = 17;
    let mut c = Ctx::new();
    c.ok(&["config", "resetstat"]);

    for _ in 0..K_GETS {
        c.run(&["get", "nonexistent"]);
    }

    let cmdstats = c.text(&["INFO", "COMMANDSTATS"]);
    assert_eq!(extract_calls(&cmdstats, "cmdstat_get:"), K_GETS);

    // A command never invoked must not appear at all (zero-call commands are
    // skipped).
    assert!(!cmdstats.contains("cmdstat_getex:"), "{cmdstats}");

    let all = c.text(&["INFO", "ALL"]);
    assert!(extract_calls(&all, "cmdstat_get:") >= K_GETS);
}

/// `InfoClusterMigrationErrors` (server_family_test.cc:881).
#[test]
fn info_cluster_migration_errors() {
    let mut c = Ctx::new();
    assert!(
        c.text(&["INFO", "CLUSTER"])
            .contains("migration_errors_total:0")
    );
}

// ---------------------------------------------------------------------------
// CLIENT TRACKING (RESP3 invalidation pushes)
// ---------------------------------------------------------------------------

/// The key of an invalidation push: `["invalidate", [key]]` or, for the flush
/// broadcast, `["invalidate", nil]` (no key).
fn invalidation_key(push: &[Value]) -> Option<String> {
    assert_eq!(
        push.first().and_then(Value::text).as_deref(),
        Some("invalidate"),
        "expected an invalidate push, got {push:?}"
    );
    push.get(1)
        .and_then(Value::arr)
        .and_then(|a| a.first())
        .and_then(Value::text)
}

/// `ClientTrackingOnAndOff` (server_family_test.cc:330).
#[test]
fn client_tracking_on_and_off() {
    let mut c = Ctx::new();
    // RESP2 rejects tracking entirely.
    expect_err_exact(
        &c.run(&["CLIENT", "TRACKING", "ON"]),
        "ERR Client tracking is currently not supported for RESP2. Please use RESP3.",
    );

    c.run(&["HELLO", "3"]);
    expect_ok(&c.run(&["CLIENT", "TRACKING", "ON"]));
    // In NONE mode CACHING is rejected with the mode-specific error.
    expect_err_exact(
        &c.run(&["CLIENT", "CACHING", "YES"]),
        "ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode",
    );
    expect_err_exact(
        &c.run(&["CLIENT", "CACHING", "NO"]),
        "ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode",
    );

    // Turn tracking off: CACHING becomes invalid entirely.
    expect_ok(&c.run(&["CLIENT", "TRACKING", "OFF"]));
    expect_err_exact(
        &c.run(&["CLIENT", "CACHING", "YES"]),
        "ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or \
         OPTOUT mode enabled",
    );
}

/// `ToggleTrackingOnAndOff` (server_family_test.cc:360).
#[test]
fn toggle_tracking_on_and_off() {
    let mut c = Ctx::new();
    c.run(&["HELLO", "3"]);
    // seq = 0 -> CLIENT TRACKING ON OPTIN bumps to 1.
    expect_ok(&c.run(&["CLIENT", "TRACKING", "ON", "OPTIN"]));
    // CACHING YES captures seq 1.
    expect_ok(&c.run(&["CLIENT", "CACHING", "YES"]));
    // OFF then ON again: seq = 3, caching stays 1, so OPTIN no longer matches.
    c.run(&["CLIENT", "TRACKING", "OFF"]);
    expect_ok(&c.run(&["CLIENT", "TRACKING", "ON", "OPTIN"]));
    c.run(&["GET", "foo"]);
    c.run(&["SET", "foo", "tmp"]);
    assert_eq!(c.push_count(), 0);
}

/// `ClientTrackingReadKey` (server_family_test.cc:383).
#[test]
fn client_tracking_read_key() {
    let mut c = Ctx::new();
    c.run(&["HELLO", "3"]);
    c.run(&["CLIENT", "TRACKING", "ON"]);

    c.run(&["SET", "FOO", "10"]);
    c.run(&["GET", "FOO"]);
    assert_eq!(c.push_count(), 0);

    c.run(&["GET", "BAR"]);
    assert_eq!(c.push_count(), 0);
}

/// `ClientTrackingOptin` (server_family_test.cc:396).
#[test]
fn client_tracking_optin() {
    let mut c = Ctx::new();
    c.run(&["HELLO", "3"]);
    c.run(&["CLIENT", "TRACKING", "ON", "OPTIN"]);

    c.run(&["GET", "FOO"]);
    c.run(&["SET", "FOO", "10"]);
    assert_eq!(c.push_count(), 0);
    c.run(&["GET", "FOO"]);
    assert_eq!(c.push_count(), 0);

    // CACHING YES: the next read is tracked.
    c.run(&["CLIENT", "CACHING", "YES"]);
    c.run(&["GET", "FOO"]);
    c.run(&["SET", "FOO", "20"]);
    c.run(&["GET", "FOO"]);
    assert_eq!(c.push_count(), 1);

    // BAR was never tracked (its reads came before CACHING YES).
    c.run(&["GET", "BAR"]);
    c.run(&["SET", "BAR", "20"]);
    c.run(&["GET", "BAR"]);
    assert_eq!(c.push_count(), 1);

    c.run(&["CLIENT", "CACHING", "YES"]);
    c.run(&["GET", "BAR"]);
    c.run(&["SET", "BAR", "20"]);
    c.run(&["GET", "BAR"]);
    assert_eq!(c.push_count(), 2);
}

/// `ClientTrackingMulti` (server_family_test.cc:426).
#[test]
fn client_tracking_multi() {
    let mut c = Ctx::new();
    c.run(&["HELLO", "3"]);
    c.run(&["CLIENT", "TRACKING", "ON"]);
    c.run(&["MULTI"]);
    c.run(&["GET", "FOO"]);
    c.run(&["SET", "TMP", "10"]);
    c.run(&["GET", "FOOBAR"]);
    c.run(&["EXEC"]);

    c.run(&["SET", "FOO", "10"]);
    c.run(&["SET", "FOOBAR", "10"]);
    assert_eq!(c.push_count(), 2);
}

/// `ClientTrackingCompatibilityMulti` (server_family_test.cc:440).
#[test]
fn client_tracking_compatibility_multi() {
    let mut c = Ctx::new();
    c.run(&["HELLO", "3"]);
    c.run(&["MULTI"]);
    expect_text(&c.run(&["CLIENT", "TRACKING", "ON"]), "QUEUED");
    expect_text(&c.run(&["CLIENT", "KILL", "127.0.0.1:6380"]), "QUEUED");
    expect_text(&c.run(&["CLIENT", "SETNAME", "YO"]), "QUEUED");
    expect_text(&c.run(&["CLIENT", "GETNAME"]), "QUEUED");
    c.run(&["EXEC"]);

    c.run(&["GET", "FOO"]);
    c.run(&["SET", "FOO", "10"]);
    assert_eq!(c.push_count(), 1);

    c.run(&["MULTI"]);
    expect_text(&c.run(&["CLIENT", "PAUSE", "0", "WRITE"]), "QUEUED");
    c.run(&["EXEC"]);
}

/// `ClientTrackingMultiOptin` (server_family_test.cc:465).
#[test]
fn client_tracking_multi_optin() {
    let mut c = Ctx::new();
    c.run(&["HELLO", "3"]);
    c.run(&["CLIENT", "TRACKING", "ON", "OPTIN"]);
    c.run(&["CLIENT", "CACHING", "YES"]);

    // A discarded MULTI must not track anything.
    c.run(&["MULTI"]);
    c.run(&["GET", "FOO"]);
    c.run(&["SET", "TMP", "10"]);
    c.run(&["GET", "FOOBAR"]);
    c.run(&["DISCARD"]);
    c.run(&["SET", "FOO", "10"]);
    assert_eq!(c.push_count(), 0);

    // Reads inside the EXEC are tracked (stickiness).
    c.run(&["CLIENT", "CACHING", "YES"]);
    c.run(&["MULTI"]);
    c.run(&["GET", "FOO"]);
    c.run(&["SET", "TMP", "10"]);
    c.run(&["GET", "FOOBAR"]);
    c.run(&["EXEC"]);
    c.run(&["SET", "FOO", "10"]);
    c.run(&["SET", "FOOBAR", "10"]);
    assert_eq!(c.push_count(), 2);

    // CACHING YES enclosed in MULTI.
    c.run(&["MULTI"]);
    c.run(&["GET", "TMP"]);
    c.run(&["GET", "TMP_TMP"]);
    c.run(&["SET", "TMP", "10"]);
    c.run(&["CLIENT", "CACHING", "YES"]);
    c.run(&["GET", "FOO"]);
    c.run(&["GET", "FOOBAR"]);
    c.run(&["EXEC"]);
    assert_eq!(c.push_count(), 2);
    c.run(&["SET", "TMP", "10"]);
    assert_eq!(c.push_count(), 2);
    c.run(&["SET", "FOO", "10"]);
    assert_eq!(c.push_count(), 3);
    c.run(&["SET", "FOOBAR", "10"]);
    assert_eq!(c.push_count(), 4);

    // CACHING YES enclosed in MULTI, with an untracked GET.
    c.run(&["MULTI"]);
    c.run(&["GET", "TMP"]);
    c.run(&["SET", "TMP", "10"]);
    c.run(&["CLIENT", "CACHING", "YES"]);
    c.run(&["GET", "FOO"]);
    c.run(&["GET", "BAR"]);
    c.run(&["EXEC"]);
    assert_eq!(c.push_count(), 4);
    c.run(&["SET", "FOO", "10"]);
    c.run(&["GET", "FOO"]);
    assert_eq!(c.push_count(), 5);
    c.run(&["SET", "BAR", "10"]);
    c.run(&["GET", "BAR"]);
    assert_eq!(c.push_count(), 6);
}

/// `ClientTrackingOptout` (server_family_test.cc:526).
#[test]
fn client_tracking_optout() {
    let mut c = Ctx::new();
    c.run(&["HELLO", "3"]);
    c.run(&["CLIENT", "TRACKING", "ON", "OPTOUT"]);
    c.run(&["GET", "FOO"]);
    c.run(&["SET", "FOO", "BAR"]);
    c.run(&["GET", "BAR"]);
    c.run(&["SET", "BAR", "FOO"]);
    assert_eq!(c.push_count(), 2);

    // CACHING NO excludes the next read.
    c.run(&["CLIENT", "CACHING", "NO"]);
    c.run(&["GET", "FOO"]);
    c.run(&["SET", "FOO", "BAR"]);
    assert_eq!(c.push_count(), 2);
}

/// `ClientTrackingMultiOptout` (server_family_test.cc:543).
#[test]
fn client_tracking_multi_optout() {
    let mut c = Ctx::new();
    c.run(&["HELLO", "3"]);
    c.run(&["CLIENT", "TRACKING", "ON", "OPTOUT"]);

    c.run(&["MULTI"]);
    c.run(&["GET", "FOO"]);
    c.run(&["SET", "TMP", "10"]);
    c.run(&["GET", "FOOBAR"]);
    c.run(&["EXEC"]);
    c.run(&["SET", "FOO", "10"]);
    c.run(&["SET", "FOOBAR", "10"]);
    assert_eq!(c.push_count(), 2);

    // CACHING NO enclosed in MULTI.
    c.run(&["MULTI"]);
    c.run(&["CLIENT", "CACHING", "NO"]);
    c.run(&["GET", "TMP"]);
    c.run(&["GET", "TMP_TMP"]);
    c.run(&["SET", "TMP", "10"]);
    c.run(&["SET", "TMP_TMP", "10"]);
    c.run(&["EXEC"]);
    assert_eq!(c.push_count(), 2);
}

/// `ClientTrackingUpdateKey` (server_family_test.cc:570). Writes come from a
/// second connection; the tracking client reads each invalidation push.
#[test]
fn client_tracking_update_key() {
    let mut c = Ctx::new();
    let mut w = c.conn();
    c.run(&["HELLO", "3"]);
    c.run(&["CLIENT", "TRACKING", "ON"]);

    c.run(&["GET", "FOO"]);
    w.cmd(&["SET", "FOO", "10"]).unwrap();
    assert_eq!(invalidation_key(&c.read_push()), Some("FOO".into()));

    // Invalidation is sent once per write; a re-read alone adds nothing.
    c.run(&["GET", "FOO"]);
    assert_eq!(c.push_count(), 0);

    // Update from the other connection invalidates the re-initialized key.
    c.run(&["GET", "FOO"]);
    w.cmd(&["SET", "FOO", "30"]).unwrap();
    assert_eq!(invalidation_key(&c.read_push()), Some("FOO".into()));

    // MGET tracks many keys; MSET invalidates only the written subset.
    c.run(&[
        "MGET", "X1", "X2", "X3", "X4", "Y1", "Y2", "Y3", "Y4", "Z1", "Z2", "Z3", "Z4",
    ]);
    w.cmd(&["MSET", "X1", "1", "Y3", "2", "Z2", "3", "Z4", "5"])
        .unwrap();
    let mut keys: Vec<String> = (0..4)
        .map(|_| invalidation_key(&c.read_push()).expect("key push"))
        .collect();
    keys.sort();
    assert_eq!(keys, ["X1", "Y3", "Z2", "Z4"]);

    // FLUSHDB drops the whole tracking map and broadcasts a null-keyed push
    // (`SendInvalidationMessages`, invalidate_due_to_flush).
    w.cmd(&["FLUSHDB"]).unwrap();
    assert_eq!(invalidation_key(&c.read_push()), None);
}

/// `ClientTrackingDeleteKey` (server_family_test.cc:604).
#[test]
fn client_tracking_delete_key() {
    let mut c = Ctx::new();
    let mut w = c.conn();
    c.run(&["HELLO", "3"]);
    c.run(&["CLIENT", "TRACKING", "ON"]);
    c.run(&["SET", "FOO", "10"]);
    c.run(&["GET", "FOO"]);
    w.cmd(&["DEL", "FOO"]).unwrap();
    assert_eq!(invalidation_key(&c.read_push()), Some("FOO".into()));
}

/// `ClientTrackingRenameKey` (server_family_test.cc:614): the source key is
/// invalidated.
#[test]
fn client_tracking_rename_key() {
    let mut c = Ctx::new();
    let mut w = c.conn();
    c.run(&["HELLO", "3"]);
    c.run(&["CLIENT", "TRACKING", "ON"]);
    c.run(&["SET", "FOO", "10"]);
    c.run(&["GET", "FOO"]);
    w.cmd(&["RENAME", "FOO", "BAR"]).unwrap();
    assert_eq!(invalidation_key(&c.read_push()), Some("FOO".into()));
}

/// `ClientTrackingExpireKey` (server_family_test.cc:624): EXPIRE is a write
/// that invalidates the tracked key; the key then reads as nil after expiry.
#[test]
fn client_tracking_expire_key() {
    let _g = clock_guard();
    let mut c = Ctx::new();
    c.run(&["HELLO", "3"]);
    c.run(&["CLIENT", "TRACKING", "ON"]);
    c.run(&["SET", "C", "10"]);
    c.run(&["GET", "C"]);
    c.run(&["EXPIRE", "C", "1"]);
    assert_eq!(invalidation_key(&c.read_push()), Some("C".into()));
    advance(1000);
    expect_null(&c.run(&["GET", "C"]));
    assert_eq!(c.push_count(), 0);
}

/// `ClientTrackingSelectDB` (server_family_test.cc:637): the tracking map is
/// shared across DBs, so a write in another DB invalidates the same key.
#[test]
fn client_tracking_select_db() {
    let mut c = Ctx::new();
    let mut w = c.conn();
    c.run(&["HELLO", "3"]);
    c.run(&["CLIENT", "TRACKING", "ON"]);
    c.run(&["SET", "C", "10"]);
    c.run(&["GET", "C"]);
    w.cmd(&["SELECT", "2"]).unwrap();
    w.cmd(&["SET", "C", "1000"]).unwrap();
    assert_eq!(invalidation_key(&c.read_push()), Some("C".into()));
}

/// `ClientTrackingNonTransactionalBug` (server_family_test.cc:649): running a
/// non-transactional command right after enabling tracking must not crash.
#[test]
fn client_tracking_non_transactional_bug() {
    let mut c = Ctx::new();
    c.run(&["HELLO", "3"]);
    c.run(&["CLIENT", "TRACKING", "ON"]);
    let v = c.run(&["CLUSTER", "SLOTS"]);
    // The port has no CLUSTER support; the reference only checks for a crash.
    assert!(matches!(v, Value::Error(_)));
}

/// `ClientTrackingLuaBug` (server_family_test.cc:656): reads and writes inside
/// EVAL are tracked and invalidated, and tracking sticks across scripts.
#[test]
fn client_tracking_lua_bug() {
    let mut c = Ctx::new();
    c.run(&["HELLO", "3"]);
    c.run(&["CLIENT", "TRACKING", "ON"]);

    let eval = "redis.call('get', 'foo'); redis.call('set', 'foo', 'bar'); ";
    c.run(&["EVAL", &format!("{eval}return 1"), "1", "foo"]);
    c.run(&["PING"]);
    assert_eq!(c.push_count(), 1);

    let eval2 =
        format!("{eval}redis.call('get', 'oof'); redis.call('set', 'oof', 'bar'); return 1");
    c.run(&["EVAL", &eval2, "2", "foo", "oof"]);
    c.run(&["PING"]);
    assert_eq!(c.push_count(), 3);
}
