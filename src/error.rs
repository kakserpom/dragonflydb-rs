use std::fmt;

use crate::core::PrimeValue;

/// A single deferred store: key, optional value (`None` deletes), absolute
/// expiry in ms (if any), and whether the STICK flag should be applied.
pub type DeferredStoreItem = (Vec<u8>, Option<PrimeValue>, Option<u64>, bool);

/// Outcome of a command execution.
///
/// * `Ok(reply)`     - the reply value to encode
/// * `Err(err)`      - an error reply
/// * `Blocked`       - a blocking command (XREAD/XREADGROUP) would block; the
///   coordinator re-runs it until data arrives or the timeout.
/// * `DeferredStore` - a multi-shard command computed a value that must be
///   stored on the key's shard; the coordinator performs this as a follow-up
///   single-shard write and replies with `reply`.
/// * `DeferredStores` - like `DeferredStore` but for a sequence of writes on
///   possibly different shards (e.g. SMOVE's src/dest).
#[derive(Debug, Clone)]
pub enum CmdResult {
    Ok(RespValue),
    Err(RespError),
    Blocked,
    DeferredStore {
        key: Vec<u8>,
        /// `None` deletes the key.
        value: Option<PrimeValue>,
        reply: RespValue,
    },
    DeferredStores {
        /// `(key, value, expire_at, sticky)` tuples; `None` value deletes the
        /// key, `Some(expire_at)` sets the absolute expiry in ms on the stored
        /// key, and `sticky` applies/clears the STICK flag.
        stores: Vec<DeferredStoreItem>,
        reply: RespValue,
    },
}

impl CmdResult {
    #[must_use]
    pub fn ok(reply: RespValue) -> Self {
        CmdResult::Ok(reply)
    }
    pub fn err(message: impl Into<String>) -> Self {
        CmdResult::Err(RespError {
            message: message.into(),
        })
    }
    #[must_use]
    pub fn blocked() -> Self {
        CmdResult::Blocked
    }
    #[must_use]
    pub fn deferred_store(key: Vec<u8>, value: Option<PrimeValue>, reply: RespValue) -> Self {
        CmdResult::DeferredStore { key, value, reply }
    }
    #[must_use]
    pub fn deferred_stores(stores: Vec<DeferredStoreItem>, reply: RespValue) -> Self {
        CmdResult::DeferredStores { stores, reply }
    }
    #[must_use]
    pub fn is_err(&self) -> bool {
        matches!(self, CmdResult::Err(_))
    }

    /// Convert into a `RespValue` for encoding. `Blocked`/`DeferredStore` are
    /// never expected on a reply path (the coordinator handles them) and map to
    /// `Nil` defensively.
    #[must_use]
    pub fn into_resp_value(self) -> RespValue {
        match self {
            CmdResult::Ok(v) => v,
            CmdResult::Err(e) => RespValue::Error(e.message),
            CmdResult::Blocked => RespValue::Nil,
            CmdResult::DeferredStore { reply, .. } | CmdResult::DeferredStores { reply, .. } => {
                reply
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct RespError {
    /// Fully rendered error message including the prefix, e.g.
    /// "WRONGTYPE Operation against a key holding the wrong kind of value".
    pub message: String,
}

impl RespError {
    pub fn new(message: impl Into<String>) -> Self {
        RespError {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn wrong_type() -> Self {
        RespError::new("WRONGTYPE Operation against a key holding the wrong kind of value")
    }

    #[must_use]
    pub fn syntax() -> Self {
        RespError::new("ERR syntax error")
    }

    #[must_use]
    pub fn integer() -> Self {
        RespError::new("ERR value is not an integer or out of range")
    }

    #[must_use]
    pub fn float() -> Self {
        RespError::new("ERR value is not a valid float")
    }

    #[must_use]
    pub fn out_of_range() -> Self {
        RespError::new("ERR index out of range")
    }

    #[must_use]
    pub fn no_such_key_or_group(key: &[u8], group: &[u8]) -> Self {
        RespError::new(format!(
            "NOGROUP No such key '{}' or consumer group '{}'",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(group)
        ))
    }

    #[must_use]
    pub fn render(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for RespError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// A reply value that gets encoded to the RESP wire protocol.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Nil,
    Array(Vec<RespValue>),
    Double(f64),
    Bool(bool),
    Map(Vec<(RespValue, RespValue)>),
}

impl RespValue {
    pub fn bulk(s: impl Into<Vec<u8>>) -> Self {
        RespValue::Bulk(s.into())
    }
    #[must_use]
    pub fn array(v: Vec<RespValue>) -> Self {
        RespValue::Array(v)
    }
    #[must_use]
    pub fn ok() -> Self {
        RespValue::Simple("OK".into())
    }
}

pub type ReplyBytes = Vec<u8>;
