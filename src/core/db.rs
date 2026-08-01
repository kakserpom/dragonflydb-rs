use hashbrown::HashMap;
use hashbrown::HashSet;

use crate::core::compact::CompactString;
use crate::core::stream::StreamId;
use crate::core::value::PrimeValue;

/// A single database shard, mirroring Dragonfly's `DbSlice`: it owns a slice of
/// the keyspace partitioned by shard id. Only the owning shard thread touches it.
pub struct DbSlice {
    shard_id: usize,
    /// The prime table: key -> value.
    prime_table: HashMap<CompactString, PrimeValue>,
    /// Expiration map: key -> unix time (ms) at which it expires.
    expire: HashMap<CompactString, u64>,
    /// Sticky keys (STICK): never expire, even with a TTL.
    sticky: HashSet<CompactString>,
    /// Resolved stream IDs for blocking XREAD `$` arguments: a blocked read
    /// resolves `$` once (to the stream's last ID at call time) and the
    /// coordinator re-runs the command, so we remember the resolved ID here.
    stream_block_watermarks: HashMap<Vec<u8>, StreamId>,
    pub stats: DbStats,
}

#[derive(Debug, Clone, Default)]
pub struct DbStats {
    pub key_count: usize,
    pub expiry_count: usize,
    pub expired_checked: u64,
}

impl DbSlice {
    pub fn new(shard_id: usize) -> Self {
        DbSlice {
            shard_id,
            prime_table: HashMap::new(),
            expire: HashMap::new(),
            sticky: HashSet::new(),
            stream_block_watermarks: HashMap::new(),
            stats: DbStats::default(),
        }
    }

    pub fn shard_id(&self) -> usize {
        self.shard_id
    }

    pub fn key_count(&self) -> usize {
        self.prime_table.len()
    }

    /// Look up a value, transparently removing it if expired.
    pub fn find(&mut self, key: &[u8], now_ms: u64) -> Option<&PrimeValue> {
        self.expire_if_needed(key, now_ms);
        self.prime_table.get(key)
    }

    pub fn find_mut(&mut self, key: &[u8], now_ms: u64) -> Option<&mut PrimeValue> {
        self.expire_if_needed(key, now_ms);
        self.prime_table.get_mut(key)
    }

    pub fn contains(&mut self, key: &[u8], now_ms: u64) -> bool {
        self.expire_if_needed(key, now_ms);
        self.prime_table.contains_key(key)
    }

    pub fn insert(&mut self, key: CompactString, value: PrimeValue) {
        self.prime_table.insert(key, value);
        self.refresh_stats();
    }

    /// Insert only if the key does not exist. Returns true if inserted.
    pub fn insert_if_absent(&mut self, key: CompactString, value: PrimeValue, now_ms: u64) -> bool {
        self.expire_if_needed(&key, now_ms);
        if self.prime_table.contains_key(&key) {
            return false;
        }
        self.prime_table.insert(key, value);
        self.refresh_stats();
        true
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<PrimeValue> {
        let v = self.prime_table.remove(key);
        if v.is_some() {
            self.expire.remove(key);
            self.sticky.remove(key);
        }
        v
    }

    pub fn remove_if_exists(&mut self, key: &[u8]) -> bool {
        self.remove(key).is_some()
    }

    pub fn set_expiry(&mut self, key: &[u8], expire_at_ms: u64, now_ms: u64) {
        // Only meaningful for existing keys; mirror redis: PEXPIRE on missing key is no-op.
        self.expire_if_needed(key, now_ms);
        if self.prime_table.contains_key(key) {
            if expire_at_ms <= now_ms {
                self.prime_table.remove(key);
                self.expire.remove(key);
            } else {
                self.expire.insert(CompactString::from_bytes(key), expire_at_ms);
            }
        }
        self.refresh_stats();
    }

    pub fn clear_expiry(&mut self, key: &[u8]) {
        self.expire.remove(key);
    }

    /// Return remaining TTL in ms; -2 missing, -1 no expiry.
    pub fn ttl_ms(&mut self, key: &[u8], now_ms: u64) -> i64 {
        self.expire_if_needed(key, now_ms);
        if !self.prime_table.contains_key(key) {
            return -2;
        }
        match self.expire.get(key) {
            Some(at) => (*at as i64) - (now_ms as i64),
            None => -1,
        }
    }

    /// Returns true if the key was removed due to expiration. Sticky keys never
    /// expire (mirrors Dragonfly's `DbEntry::IsSticky`).
    pub fn expire_if_needed(&mut self, key: &[u8], now_ms: u64) -> bool {
        self.stats.expired_checked += 1;
        if self.sticky.contains(key) {
            return false;
        }
        let Some(&at) = self.expire.get(key) else {
            return false;
        };
        if now_ms >= at {
            self.prime_table.remove(key);
            self.expire.remove(key);
            return true;
        }
        false
    }

    pub fn has_expiry(&mut self, key: &[u8], now_ms: u64) -> bool {
        self.expire_if_needed(key, now_ms);
        self.expire.contains_key(key)
    }

    /// Absolute expiration time in ms for a key, if any (no expiry check).
    pub fn expire_at(&self, key: &[u8]) -> Option<u64> {
        self.expire.get(key).copied()
    }

    /// Mark a key sticky if it exists and was not already sticky. Returns true
    /// if the flag was newly applied (mirrors OpStick).
    pub fn set_sticky(&mut self, key: &[u8], now_ms: u64) -> bool {
        self.expire_if_needed(key, now_ms);
        if self.prime_table.contains_key(key) && !self.sticky.contains(key) {
            self.sticky.insert(CompactString::from_bytes(key));
            return true;
        }
        false
    }

    /// Whether the key is sticky.
    pub fn is_sticky(&self, key: &[u8]) -> bool {
        self.sticky.contains(key)
    }

    /// Set the sticky flag unconditionally (used when applying deferred stores).
    pub fn set_sticky_flag(&mut self, key: &[u8], sticky: bool) {
        if sticky {
            self.sticky.insert(CompactString::from_bytes(key));
        } else {
            self.sticky.remove(key);
        }
    }

    /// Iterate over all (key, value) pairs. The iterator borrows self mutably
    /// internally to handle expiration lazily.
    pub fn iter(&self) -> impl Iterator<Item = (&CompactString, &PrimeValue)> {
        self.prime_table.iter()
    }

    /// The resolved last ID for a blocking XREAD `$` on `key`, if one is pending.
    pub fn stream_watermark(&self, key: &[u8]) -> Option<StreamId> {
        self.stream_block_watermarks.get(key).copied()
    }

    pub fn set_stream_watermark(&mut self, key: Vec<u8>, id: StreamId) {
        self.stream_block_watermarks.insert(key, id);
    }

    pub fn remove_stream_watermark(&mut self, key: &[u8]) {
        self.stream_block_watermarks.remove(key);
    }

    fn refresh_stats(&mut self) {
        self.stats.key_count = self.prime_table.len();
        self.stats.expiry_count = self.expire.len();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> u64 {
        1000
    }

    #[test]
    fn insert_find_remove() {
        let mut db = DbSlice::new(0);
        db.insert(CompactString::from("k"), PrimeValue::Str(CompactString::from("v")));
        assert_eq!(db.find(b"k", now()).map(|v| v.obj_type()), Some(crate::core::ObjType::Str));
        assert_eq!(db.key_count(), 1);
        assert!(db.remove_if_exists(b"k"));
        assert!(db.find(b"k", now()).is_none());
    }

    #[test]
    fn expiry_works() {
        let mut db = DbSlice::new(0);
        db.insert(CompactString::from("k"), PrimeValue::Str(CompactString::from("v")));
        db.set_expiry(b"k", 2000, now());
        assert_eq!(db.ttl_ms(b"k", now()), 1000);
        assert!(db.find(b"k", now()).is_some());
        assert!(db.find(b"k", 3000).is_none());
        assert_eq!(db.ttl_ms(b"k", 3000), -2);
    }
}
