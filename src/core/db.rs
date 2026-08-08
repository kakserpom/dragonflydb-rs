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
    /// Keys accessed since they were created (reads, TTL lookups). Backs the
    /// SCAN `ATTR a|u` filter, mirroring Dragonfly's `WasTouched` flag.
    touched: HashSet<CompactString>,
    /// Resolved stream IDs for blocking XREAD `$` arguments: a blocked read
    /// resolves `$` once (to the stream's last ID at call time) and the
    /// coordinator re-runs the command, so we remember the resolved ID here.
    /// Resolved `$` watermarks for blocking XREAD, keyed by connection.
    stream_block_watermarks: HashMap<(u64, Vec<u8>), StreamId>,
    /// Monotonic per-key version, bumped on every modification. Backs WATCH:
    /// EXEC re-queries a watched key's version and aborts if it moved.
    versions: HashMap<CompactString, u64>,
    /// Bumped on whole-DB flush. Every WATCH in this DB is dirty after a
    /// FLUSHDB, even for watched keys that did not exist.
    db_epoch: u64,
    /// Keys modified since the last invalidation drain (`PostUpdate`).
    /// Client-tracking consumers read it on the next write; the shard drains it
    /// at the end of every executed command.
    modified: Vec<CompactString>,
    pub stats: DbStats,
}

#[derive(Debug, Clone, Default)]
pub struct DbStats {
    pub key_count: usize,
    pub expiry_count: usize,
    pub expired_checked: u64,
    /// Read hits (key found), `INFO keyspace` `hits=`.
    pub hits: u64,
    /// Read misses (key absent), `INFO keyspace` `misses=`.
    pub misses: u64,
}

impl DbSlice {
    #[must_use]
    pub fn new(shard_id: usize) -> Self {
        DbSlice {
            shard_id,
            prime_table: HashMap::new(),
            expire: HashMap::new(),
            sticky: HashSet::new(),
            touched: HashSet::new(),
            stream_block_watermarks: HashMap::new(),
            versions: HashMap::new(),
            db_epoch: 0,
            modified: Vec::new(),
            stats: DbStats::default(),
        }
    }

    #[must_use]
    pub fn shard_id(&self) -> usize {
        self.shard_id
    }

    #[must_use]
    pub fn key_count(&self) -> usize {
        self.prime_table.len()
    }

    /// Look up a value, transparently removing it if expired. Marks the key as
    /// touched when found (backing SCAN `ATTR a`). A successful read counts a
    /// hit, a missing/expired key a miss (`DbSlice::FindInternal` with
    /// `UpdateStatsMode::kReadStats`).
    pub fn find(&mut self, key: &[u8], now_ms: u64) -> Option<&PrimeValue> {
        self.expire_if_needed(key, now_ms);
        let v = self.prime_table.get(key);
        if v.is_some() {
            self.touched.insert(CompactString::from_bytes(key));
            self.stats.hits += 1;
        } else {
            self.stats.misses += 1;
        }
        v
    }

    pub fn find_mut(&mut self, key: &[u8], now_ms: u64) -> Option<&mut PrimeValue> {
        self.expire_if_needed(key, now_ms);
        let v = self.prime_table.get_mut(key);
        if v.is_some() {
            self.touched.insert(CompactString::from_bytes(key));
        }
        v
    }

    pub fn contains(&mut self, key: &[u8], now_ms: u64) -> bool {
        self.expire_if_needed(key, now_ms);
        let found = self.prime_table.contains_key(key);
        if found {
            self.touched.insert(CompactString::from_bytes(key));
        }
        found
    }

    pub fn insert(&mut self, key: &[u8], value: PrimeValue) {
        self.prime_table
            .insert(CompactString::from_bytes(key), value);
        self.bump_version(key);
        self.refresh_stats();
    }

    /// Insert only if the key does not exist. Returns true if inserted.
    pub fn insert_if_absent(&mut self, key: &[u8], value: PrimeValue, now_ms: u64) -> bool {
        self.expire_if_needed(key, now_ms);
        if self.prime_table.contains_key(key) {
            return false;
        }
        self.prime_table
            .insert(CompactString::from_bytes(key), value);
        self.bump_version(key);
        self.refresh_stats();
        true
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<PrimeValue> {
        let v = self.prime_table.remove(key);
        if v.is_some() {
            self.bump_version(key);
            self.expire.remove(key);
            self.sticky.remove(key);
            self.touched.remove(key);
        }
        v
    }

    /// Remove `key`, returning its value, absolute expiry in ms (if any) and
    /// sticky flag. Used by MOVE to transfer a key between DBs on one shard.
    pub fn take(&mut self, key: &[u8], now_ms: u64) -> Option<(PrimeValue, Option<u64>, bool)> {
        self.expire_if_needed(key, now_ms);
        let value = self.prime_table.remove(key)?;
        self.bump_version(key);
        let expire_at = self.expire.remove(key);
        let sticky = self.sticky.remove(key);
        self.touched.remove(key);
        self.refresh_stats();
        Some((value, expire_at, sticky))
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
                self.bump_version(key);
                self.expire.remove(key);
            } else {
                self.bump_version(key);
                self.expire
                    .insert(CompactString::from_bytes(key), expire_at_ms);
            }
        }
        self.refresh_stats();
    }

    pub fn clear_expiry(&mut self, key: &[u8]) {
        if self.expire.remove(key).is_some() {
            self.bump_version(key);
        }
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
            self.bump_version(key);
            self.expire.remove(key);
            self.touched.remove(key);
            return true;
        }
        false
    }

    pub fn has_expiry(&mut self, key: &[u8], now_ms: u64) -> bool {
        self.expire_if_needed(key, now_ms);
        let has = self.expire.contains_key(key);
        if has {
            self.touched.insert(CompactString::from_bytes(key));
        }
        has
    }

    /// Absolute expiration time in ms for a key, if any (no expiry check).
    #[must_use]
    pub fn expire_at(&self, key: &[u8]) -> Option<u64> {
        self.expire.get(key).copied()
    }

    /// Mark a key sticky if it exists and was not already sticky. Returns true
    /// if the flag was newly applied (mirrors `OpStick`).
    pub fn set_sticky(&mut self, key: &[u8], now_ms: u64) -> bool {
        self.expire_if_needed(key, now_ms);
        if self.prime_table.contains_key(key) && !self.sticky.contains(key) {
            self.sticky.insert(CompactString::from_bytes(key));
            self.bump_version(key);
            return true;
        }
        false
    }

    /// Whether the key is sticky.
    #[must_use]
    pub fn is_sticky(&self, key: &[u8]) -> bool {
        self.sticky.contains(key)
    }

    /// Whether the key has been accessed since it was created (SCAN `ATTR a|u`).
    #[must_use]
    pub fn is_touched(&self, key: &[u8]) -> bool {
        self.touched.contains(key)
    }

    /// Set the sticky flag unconditionally (used when applying deferred stores).
    pub fn set_sticky_flag(&mut self, key: &[u8], sticky: bool) {
        if sticky {
            if self.sticky.insert(CompactString::from_bytes(key)) {
                self.bump_version(key);
            }
        } else if self.sticky.remove(key) {
            self.bump_version(key);
        }
    }

    /// The current modification version of `key` (0 if never modified).
    #[must_use]
    pub fn version_of(&self, key: &[u8]) -> u64 {
        self.versions.get(key).copied().unwrap_or(0)
    }

    /// Monotonic DB epoch, bumped on whole-DB flush.
    #[must_use]
    pub fn db_epoch(&self) -> u64 {
        self.db_epoch
    }

    /// Bump the DB epoch (FLUSHDB): every WATCH in this DB becomes dirty.
    pub fn bump_db_epoch(&mut self) {
        self.db_epoch += 1;
    }

    fn bump_version(&mut self, key: &[u8]) {
        let e = self
            .versions
            .entry(CompactString::from_bytes(key))
            .or_insert(0);
        *e += 1;
        // A version bump is the canonical write hook (`PostUpdate`): record the
        // key for the client-tracking invalidation drain.
        self.modified.push(CompactString::from_bytes(key));
    }

    /// Take the keys modified since the last call, for invalidation
    /// (`SendQueuedInvalidationMessagesCb` drains per executed command).
    pub fn drain_modified(&mut self) -> Vec<CompactString> {
        std::mem::take(&mut self.modified)
    }

    /// Iterate over all (key, value) pairs. The iterator borrows self mutably
    /// internally to handle expiration lazily.
    pub fn iter(&self) -> impl Iterator<Item = (&CompactString, &PrimeValue)> {
        self.prime_table.iter()
    }

    /// The resolved last ID for a blocking XREAD `$` on `key` issued by
    /// connection `conn_id`, if one is pending.
    #[must_use]
    pub fn stream_watermark(&self, conn_id: u64, key: &[u8]) -> Option<StreamId> {
        self.stream_block_watermarks
            .get(&(conn_id, key.to_vec()))
            .copied()
    }

    pub fn set_stream_watermark(&mut self, conn_id: u64, key: Vec<u8>, id: StreamId) {
        self.stream_block_watermarks.insert((conn_id, key), id);
    }

    pub fn remove_stream_watermark(&mut self, conn_id: u64, key: &[u8]) {
        self.stream_block_watermarks
            .remove(&(conn_id, key.to_vec()));
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
        db.insert(b"k", PrimeValue::Str(CompactString::from("v")));
        assert_eq!(
            db.find(b"k", now())
                .map(super::super::value::PrimeValue::obj_type),
            Some(crate::core::ObjType::Str)
        );
        assert_eq!(db.key_count(), 1);
        assert!(db.remove_if_exists(b"k"));
        assert!(db.find(b"k", now()).is_none());
    }

    #[test]
    fn expiry_works() {
        let mut db = DbSlice::new(0);
        db.insert(b"k", PrimeValue::Str(CompactString::from("v")));
        db.set_expiry(b"k", 2000, now());
        assert_eq!(db.ttl_ms(b"k", now()), 1000);
        assert!(db.find(b"k", now()).is_some());
        assert!(db.find(b"k", 3000).is_none());
        assert_eq!(db.ttl_ms(b"k", 3000), -2);
    }

    /// Every mutation bumps the key version (so WATCH/EXEC can detect changes)
    /// while reads leave it untouched.
    #[test]
    fn versions_bump_on_mutation_only() {
        let mut db = DbSlice::new(0);
        assert_eq!(db.version_of(b"k"), 0);
        db.insert(b"k", PrimeValue::Str(CompactString::from("v")));
        assert_eq!(db.version_of(b"k"), 1);
        // Reads do not bump.
        let _ = db.find(b"k", now());
        assert_eq!(db.version_of(b"k"), 1);
        // Overwrite bumps.
        db.insert(b"k", PrimeValue::Str(CompactString::from("v2")));
        assert_eq!(db.version_of(b"k"), 2);
        // insert_if_absent on an existing key does not bump.
        assert!(!db.insert_if_absent(b"k", PrimeValue::Str(CompactString::from("v3")), now()));
        assert_eq!(db.version_of(b"k"), 2);
        // Remove bumps.
        assert!(db.remove_if_exists(b"k"));
        assert_eq!(db.version_of(b"k"), 3);
        // Expiry assignment bumps; clearing expiry bumps; lazy expiry bumps.
        db.insert(b"k", PrimeValue::Str(CompactString::from("v")));
        let v = db.version_of(b"k");
        db.set_expiry(b"k", 2000, now());
        assert_eq!(db.version_of(b"k"), v + 1);
        db.clear_expiry(b"k");
        assert_eq!(db.version_of(b"k"), v + 2);
        db.set_sticky(b"k", now());
        assert_eq!(db.version_of(b"k"), v + 3);
        // Lazy expiry bumps: set a TTL, then read past it.
        db.set_expiry(b"k", 1500, now());
        let _ = db.find(b"k", 3000); // expires the key
        assert_eq!(db.version_of(b"k"), v + 4);
    }

    /// The DB epoch is bumped on flush only; it is shared by every key in the
    /// DB so that FLUSHDB dirties watches on keys that never existed.
    #[test]
    fn db_epoch_bumps_on_flush() {
        let mut db = DbSlice::new(0);
        assert_eq!(db.db_epoch(), 0);
        db.insert(b"k", PrimeValue::Str(CompactString::from("v")));
        db.bump_db_epoch();
        assert_eq!(db.db_epoch(), 1);
        // A plain write must not bump the epoch.
        db.insert(b"k", PrimeValue::Str(CompactString::from("v2")));
        assert_eq!(db.db_epoch(), 1);
    }
}
