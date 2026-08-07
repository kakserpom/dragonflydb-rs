//! Per-IO-thread SLOWLOG ring, mirroring the reference `SlowLogShard`
//! (`slowlog.h`/`slowlog.cc`). Commands that run longer than
//! `slowlog_log_slower_than` microseconds are recorded with their arguments,
//! subject to the `slowlog_max_len` capacity; EXEC/EVAL/FCALL entries carry
//! augmented stats arguments (`FormatExecSlowlog`/`FormatEvalSlowlog`).

use std::collections::VecDeque;

use crate::error::RespValue;

/// Maximum number of stored arguments including the command name
/// (`kMaximumSlowlogArgCount`, slowlog.h): one slot is reserved for the
/// pseudo-argument that reports how many further arguments were truncated.
pub const MAX_SLOWLOG_ARG_COUNT: usize = 31;

/// Maximum stored length of one slowlog argument in bytes
/// (`kMaximumSlowlogArgLength`).
pub const MAX_SLOWLOG_ARG_LENGTH: usize = 128;

/// One slowlog entry (`SlowLogEntry`, slowlog.h).
#[derive(Debug, Clone)]
pub struct SlowLogEntry {
    /// Entry id, unique per server and never reused across RESET.
    pub id: u64,
    /// When the command finished, as epoch microseconds.
    pub unix_ts_usec: u64,
    /// The command's execution time in microseconds.
    pub exec_time_usec: u64,
    /// The command's real argument count (including the command name) before
    /// truncation.
    pub original_length: usize,
    /// Stored arguments; the first is the command name. Each entry carries the
    /// number of bytes truncated past `MAX_SLOWLOG_ARG_LENGTH` (> 0 when the
    /// argument had to be cut).
    pub args: Vec<(Vec<u8>, u32)>,
    /// The client's `ip:port`.
    pub client_ip: String,
    /// The client's name (empty without `CLIENT SETNAME`).
    pub client_name: String,
}

/// The IO-thread slowlog ring (`SlowLogShard`). `capacity` mirrors
/// `slowlog_max_len` (0 disables logging); `log_slower_than` mirrors the
/// `slowlog_log_slower_than` flag (negative disables the log).
#[derive(Debug)]
pub struct SlowLog {
    capacity: usize,
    log_slower_than: i64,
    entries: VecDeque<SlowLogEntry>,
    entry_id: u64,
}

impl SlowLog {
    /// Defaults mirror the reference flags `slowlog_log_slower_than=10000` and
    /// `slowlog_max_len=20` (server_family.cc:133-136).
    pub fn new() -> Self {
        SlowLog {
            capacity: 20,
            log_slower_than: 10000,
            entries: VecDeque::new(),
            entry_id: 0,
        }
    }

    /// `SetSlowLogMaxLen` (server_family.cc:1191): resizes the ring; 0
    /// disables logging.
    pub fn change_length(&mut self, new_length: usize) {
        self.capacity = new_length;
        while self.entries.len() > new_length {
            self.entries.pop_front();
        }
    }

    /// `SetSlowLogThreshold` (server_family.cc:1196): negative disables the
    /// log entirely (`log_slower_than_usec = UINT32_MAX`).
    pub fn set_threshold(&mut self, val: i64) {
        self.log_slower_than = val;
    }

    /// The configured `slowlog_log_slower_than` value.
    pub fn log_slower_than(&self) -> i64 {
        self.log_slower_than
    }

    /// The configured `slowlog_max_len` value.
    pub fn max_len(&self) -> usize {
        self.capacity
    }

    /// The effective threshold in usec (`log_slower_than_usec`).
    pub fn threshold(&self) -> u64 {
        if self.log_slower_than < 0 {
            u32::MAX as u64
        } else {
            self.log_slower_than as u64
        }
    }

    /// `SlowLogShard::IsEnabled` (slowlog.h:46).
    pub fn is_enabled(&self) -> bool {
        self.capacity > 0
    }

    /// `ServerState::ShouldLogSlowCmd` (server_state.cc:295).
    pub fn should_log(&self, exec_time_usec: u64) -> bool {
        self.is_enabled() && exec_time_usec >= self.threshold()
    }

    /// `SlowLogShard::Length`.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// `SlowLogShard::Reset` (slowlog.cc:18): clears the entries but keeps the
    /// id counter, so ids never repeat.
    pub fn reset(&mut self) {
        self.entries.clear();
    }

    /// `SlowLogShard::Add` (slowlog.cc:22): append one entry, truncating
    /// arguments the way the reference does. `tail` is the command's arguments
    /// after the command name; `name` is prepended. For EXEC/EVAL/FCALL `tail`
    /// carries the augmented stats args instead of the raw command tail.
    pub fn add(
        &mut self,
        name: &str,
        tail: Vec<Vec<u8>>,
        client_ip: &str,
        client_name: &str,
        exec_time_usec: u64,
        unix_ts_usec: u64,
    ) {
        debug_assert!(self.is_enabled());
        let original_length = tail.len() + 1;
        // One fewer argument when over the cap: the last slot is "wasted" on
        // the pseudo-argument reporting how many further arguments there are.
        let effective = if tail.len() > MAX_SLOWLOG_ARG_COUNT {
            MAX_SLOWLOG_ARG_COUNT - 1
        } else {
            tail.len()
        };
        let mut args: Vec<(Vec<u8>, u32)> = Vec::with_capacity(effective + 1);
        args.push((name.as_bytes().to_vec(), 0));
        for arg in tail.into_iter().take(effective) {
            let extra = arg.len().saturating_sub(MAX_SLOWLOG_ARG_LENGTH) as u32;
            let kept = arg[..arg.len().min(MAX_SLOWLOG_ARG_LENGTH)].to_vec();
            args.push((kept, extra));
        }
        self.entries.push_back(SlowLogEntry {
            id: self.entry_id,
            unix_ts_usec,
            exec_time_usec,
            original_length,
            args,
            client_ip: client_ip.to_string(),
            client_name: client_name.to_string(),
        });
        self.entry_id += 1;
        while self.entries.len() > self.capacity {
            self.entries.pop_front();
        }
    }

    /// `SlowLogGet` (server_family.cc:1027): a snapshot of the entries sorted
    /// by timestamp (newest first) and limited to `requested` (pass
    /// `u32::MAX` for all). With a single IO thread the reference's
    /// per-thread merge degenerates to this ring.
    pub fn snapshot(&self, requested: u64) -> Vec<SlowLogEntry> {
        let mut out: Vec<SlowLogEntry> = self.entries.iter().cloned().collect();
        // Newest first; equal timestamps (same-µs back-to-back commands) keep
        // newest-first order via the never-reused entry id.
        out.sort_by(|a, b| b.unix_ts_usec.cmp(&a.unix_ts_usec).then(b.id.cmp(&a.id)));
        out.truncate(requested as usize);
        out
    }
}

impl SlowLogEntry {
    /// Render the entry for a `SLOWLOG GET` reply: `[id, created_at(sec),
    /// exec_time(usec), args, client_ip, client_name]` (server_family.cc:1071).
    /// Argument truncation is applied here, so the stored prefix is re-cut to
    /// leave room for the `"... (N more bytes)"` suffix.
    pub fn into_resp(&self) -> RespValue {
        RespValue::Array(vec![
            RespValue::Integer(self.id as i64),
            RespValue::Integer((self.unix_ts_usec / 1_000_000) as i64),
            RespValue::Integer(self.exec_time_usec as i64),
            self.render_args(),
            RespValue::Bulk(self.client_ip.clone().into_bytes()),
            RespValue::Bulk(self.client_name.clone().into_bytes()),
        ])
    }

    fn render_args(&self) -> RespValue {
        let mut args = Vec::with_capacity(self.args.len() + 1);
        for (bytes, extra) in &self.args {
            if *extra > 0 {
                let suffix = format!("... ({extra} more bytes)");
                let keep = MAX_SLOWLOG_ARG_LENGTH.saturating_sub(suffix.len());
                let mut out = bytes[..keep.min(bytes.len())].to_vec();
                out.extend_from_slice(suffix.as_bytes());
                args.push(RespValue::Bulk(out));
            } else {
                args.push(RespValue::Bulk(bytes.clone()));
            }
        }
        if self.args.len() < self.original_length {
            args.push(RespValue::Bulk(
                format!(
                    "... ({} more arguments)",
                    self.original_length - self.args.len()
                )
                .into_bytes(),
            ));
        }
        RespValue::Array(args)
    }
}

impl Default for SlowLog {
    fn default() -> Self {
        SlowLog::new()
    }
}
