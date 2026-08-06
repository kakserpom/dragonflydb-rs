pub mod exec;
pub mod lua;
pub mod lua_libs;

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::core::DbSlice;
use crate::error::{CmdResult, RespValue};

pub const FLAG_WRITE: u32 = 1 << 0;
pub const FLAG_READONLY: u32 = 1 << 1;
pub const FLAG_FAST: u32 = 1 << 2;
pub const FLAG_DENYOOM: u32 = 1 << 3;
pub const FLAG_MULTI_KEY: u32 = 1 << 4;
pub const FLAG_BLOCKING: u32 = 1 << 5;
pub const FLAG_LOCAL: u32 = 1 << 6;
pub const FLAG_GLOBAL: u32 = 1 << 7;
pub const FLAG_ADMIN: u32 = 1 << 8;
pub const FLAG_MOVABLEKEYS: u32 = 1 << 9;
pub const FLAG_NOSCRIPT: u32 = 1 << 10;
/// Never auto-journaled: the command is nondeterministic (SPOP) or its replay
/// would block/duplicate work on the replica (blocking writes). Such commands
/// journal an explicit deterministic rewrite instead (SPOP -> SREM/DEL).
pub const FLAG_NO_AUTOJOURNAL: u32 = 1 << 11;
/// Not journaled when the shard owns only some of the command's keys (the
/// reduced per-shard args would re-run a partial command on the replica and
/// produce a different result). The coordinator journals these commands'
/// destination writes separately (`ShardMsg::StoreValue`).
pub const FLAG_NO_REDUCED: u32 = 1 << 12;

/// Sentinel for `KeyRange::last`: keys run through the second-to-last argument
/// (used by BLPOP/BRPOP whose trailing argument is the float timeout).
pub const LAST_BUT_ONE: usize = usize::MAX;

/// Key extraction spec, using Redis's (firstkey, lastkey, step) convention where
/// last == 0 means "through the last argument".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRange {
    pub first: usize,
    pub last: usize,
    pub step: usize,
}

impl KeyRange {
    pub const NONE: KeyRange = KeyRange {
        first: 0,
        last: 0,
        step: 0,
    };
    pub const ONE: KeyRange = KeyRange {
        first: 1,
        last: 1,
        step: 1,
    };
    pub const ALL: KeyRange = KeyRange {
        first: 1,
        last: 0,
        step: 1,
    };
    pub const PAIRS: KeyRange = KeyRange {
        first: 1,
        last: 0,
        step: 2,
    };
    pub const TWO: KeyRange = KeyRange {
        first: 1,
        last: 2,
        step: 1,
    };
    /// `<key>... <timeout>`: every argument except the last one (BLPOP/BRPOP).
    pub const ALL_BUT_LAST: KeyRange = KeyRange {
        first: 1,
        last: LAST_BUT_ONE,
        step: 1,
    };

    /// Return indices into args that are keys.
    #[must_use]
    pub fn keys(&self, argc: usize) -> Vec<usize> {
        if self.first == 0 || argc <= self.first {
            return Vec::new();
        }
        let last = if self.last == 0 {
            argc - 1
        } else if self.last == LAST_BUT_ONE {
            argc.saturating_sub(2)
        } else {
            self.last.min(argc - 1)
        };
        let mut out = Vec::new();
        let mut i = self.first;
        while i <= last {
            out.push(i);
            i += self.step;
        }
        out
    }
}

/// Context passed to a command executor.
pub struct OpContext<'a> {
    pub db: &'a mut DbSlice,
    pub args: &'a [Vec<u8>],
    /// Indices into `args` that this execution is responsible for.
    pub owned_keys: &'a [usize],
    /// The index of the first key for the command overall (KeyRange.first).
    pub first_key_idx: usize,
    pub now_ms: u64,
}

/// A partial result from one shard during a multi-shard command.
pub struct ShardPart {
    pub shard: usize,
    pub owned_key_idxs: Vec<usize>,
    pub result: CmdResult,
}

pub type ExecFn = fn(&mut OpContext) -> CmdResult;
pub type MergeFn =
    fn(parts: &[ShardPart], args: &[Vec<u8>], keys: &[usize], now_ms: u64) -> CmdResult;

#[derive(Clone, Copy)]
pub struct Command {
    pub name: &'static str,
    pub arity: i64,
    pub flags: u32,
    pub key_range: KeyRange,
    pub exec: ExecFn,
    pub merge: Option<MergeFn>,
}

impl Command {
    #[must_use]
    pub fn check_arity(&self, argc: usize) -> Option<String> {
        let ok = if self.arity >= 0 {
            argc == self.arity as usize
        } else {
            argc >= (-self.arity) as usize
        };
        // MSET/MSETNX declare interleaved key/value args (`KeyRange::PAIRS`);
        // an odd tail is a wrong-number-of-arguments error, enforced before
        // sharding (reference: `command_registry.cc` `interleave_step_ = 2`).
        let interleaved = self.key_range.step != 2 || (argc > 0 && (argc - 1).is_multiple_of(2));
        if ok && interleaved {
            None
        } else {
            Some(format!(
                "ERR wrong number of arguments for '{}' command",
                self.name.to_ascii_lowercase()
            ))
        }
    }

    #[must_use]
    pub fn has_flag(&self, flag: u32) -> bool {
        self.flags & flag != 0
    }
}

pub fn lookup(name: &[u8]) -> Option<&'static Command> {
    // Command names are ASCII; compare case-insensitively by folding bytes.
    static REGISTRY: OnceLock<HashMap<Vec<u8>, &'static Command>> = OnceLock::new();
    let reg = REGISTRY.get_or_init(|| {
        let mut m = HashMap::new();
        for cmd in exec::ALL_COMMANDS {
            m.insert(cmd.name.as_bytes().to_ascii_uppercase(), cmd);
        }
        m
    });
    let mut key = Vec::with_capacity(name.len());
    for b in name {
        key.push(b.to_ascii_uppercase());
    }
    reg.get(&key).copied()
}

// ---------------------------------------------------------------------------
// Reply helpers shared by executors
// ---------------------------------------------------------------------------

pub fn bulk<B: AsRef<[u8]>>(s: B) -> RespValue {
    RespValue::Bulk(s.as_ref().to_vec())
}

#[must_use]
pub fn simple(s: &str) -> RespValue {
    RespValue::Simple(s.to_string())
}

#[must_use]
pub fn integer(i: i64) -> RespValue {
    RespValue::Integer(i)
}

#[must_use]
pub fn ok() -> RespValue {
    RespValue::Simple("OK".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_has_no_duplicate_names() {
        let mut seen = std::collections::HashSet::new();
        for cmd in exec::ALL_COMMANDS {
            assert!(seen.insert(cmd.name), "duplicate command name {}", cmd.name);
        }
    }

    #[test]
    fn arity_check_message_format() {
        // The script dispatch path rejects bad arities with this exact message
        // (`redis.call('GET')` would otherwise reach the executor and panic).
        let get = lookup(b"GET").unwrap();
        assert_eq!(
            get.check_arity(1),
            Some("ERR wrong number of arguments for 'get' command".into())
        );
        assert_eq!(get.check_arity(2), None);
    }
}
