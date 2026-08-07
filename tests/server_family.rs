//! Port of `dragonfly/src/server/server_family_test.cc`.
//!
//! Adaptations from the C++ original:
//! - The whole suite runs RESP2 (the port has no RESP3/HELLO 3 handshake yet).
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
//! - `ClientPause` is not ported yet: the port has no `CLIENT PAUSE`.

mod common;

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
