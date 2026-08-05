//! Cuckoo filter, ported from `dragonfly/src/core/cuckoo.{h,cc}`.
//!
//! A cuckoo filter stores a per-item fingerprint in one of two candidate
//! buckets (each with `slots_per_bucket` slots). The two candidates are
//! `h` and `alt_index(fp, h)` where `alt_index` XORs with a fingerprint-mixed
//! constant, so both directions map to each other. When both candidate buckets
//! are full, the filter evicts an existing fingerprint to its *alternate*
//! bucket and keeps trying (up to `max_iterations`). Because the alternate
//! bucket is always a deterministic function of the fingerprint, deletion is
//! supported (unlike Bloom filters).
//!
//! The filter grows by appending new sub-filters whose bucket count is
//! `expansion ^ i` times the base `num_buckets` (no rehashing of existing
//! data). `expansion == 0` disables growth, so inserts fail once the initial
//! filter is full.

use xxhash_rust::xxh3::xxh3_64_with_seed;

/// The seed used by `CuckooFilter::Hash` (`XXH3_64bits_withSeed`).
const K_CUCKOO_SEED: u64 = 0xc6a4_a793_5bd1_e995;

/// Options for constructing a filter (`CuckooFilterOptions`).
#[derive(Debug, Clone, Copy)]
pub struct CuckooFilterOptions {
    pub capacity: u64,
    pub slots_per_bucket: u8,
    pub max_iterations: u16,
    pub expansion: u16,
}

impl Default for CuckooFilterOptions {
    fn default() -> Self {
        CuckooFilterOptions {
            capacity: 0,
            slots_per_bucket: 2,
            max_iterations: 20,
            expansion: 1,
        }
    }
}

/// `absl::bit_ceil(n)`: the smallest power of two >= n (1 for n == 0).
fn next_power_of_two(n: u64) -> u64 {
    n.next_power_of_two()
}

/// Result is in [1, 255] — 0 is reserved as "empty slot".
fn fingerprint(hash: u64) -> u8 {
    (hash % 255 + 1) as u8
}

/// 0x5bd1e995 is the `MurmurHash2` mixing constant (Austin Appleby), chosen for
/// good bit-avalanche properties.
///
/// `AltIndex` symmetry requires `num_buckets` to be a power of two. Power-of-2
/// modulo is a bitmask, and bitmasks commute with XOR:
///   (a XOR b) & mask == (a & mask) XOR (b & mask)
/// so `alt_index(fp, i) % N == alt_index(fp, i % N) % N` holds, which is what
/// lets KO-insert rollback and deletions find the same alternate bucket.
fn alt_index(fp: u8, index: u64) -> u64 {
    index ^ u64::from(fp).wrapping_mul(0x5bd1_e995)
}

/// A cuckoo filter, mirroring `CuckooFilter`. Owns its sub-filters (the C++
/// uses an external PMR allocator, which the Rust port folds into the `Vec`s).
#[derive(Debug, Clone)]
pub struct CuckooFilter {
    slots_per_bucket: u8,
    max_iterations: u16,
    /// Already rounded up to the next power of two (or 0 if growth disabled).
    expansion: u16,
    /// Base bucket count from construction; never changes as the filter grows
    /// (each new sub-filter scales its own bucket count by expansion instead).
    num_buckets: u64,
    num_items: u64,
    num_deletes: u64,
    num_ko_inserts: u64,
    filters: Vec<Vec<u8>>,
}

/// The per-item lookup parameters derived from a raw hash.
#[derive(Clone, Copy)]
struct LookupParams {
    fp: u8,
    h1: u64, // raw (unmodded) first candidate index
    h2: u64, // raw (unmodded) alternate index
}

impl CuckooFilter {
    #[must_use]
    pub fn new(options: &CuckooFilterOptions) -> Self {
        assert!(options.slots_per_bucket > 0);
        let expansion = if options.expansion != 0 {
            next_power_of_two(u64::from(options.expansion)) as u16
        } else {
            0
        };
        let num_buckets = next_power_of_two(options.capacity / u64::from(options.slots_per_bucket));
        let num_buckets = if num_buckets == 0 { 1 } else { num_buckets };
        let mut cf = CuckooFilter {
            slots_per_bucket: options.slots_per_bucket,
            max_iterations: options.max_iterations,
            expansion,
            num_buckets,
            num_items: 0,
            num_deletes: 0,
            num_ko_inserts: 0,
            filters: Vec::new(),
        };
        cf.add_new_sub_filter();
        cf
    }

    /// Inserts a pre-computed hash. Returns false only if the filter is full
    /// and expansion is disabled (`expansion == 0`). Allows duplicate
    /// insertions — use `insert_unique` to prevent them.
    pub fn insert(&mut self, hash: u64) -> bool {
        let p = Self::lookup_params_from_hash(hash);
        loop {
            for i in (0..self.filters.len()).rev() {
                let (i1, i2) = self.bucket_indices(&self.filters[i], &p);
                for idx in [i1, i2] {
                    let base = (idx * u64::from(self.slots_per_bucket)) as usize;
                    for s in 0..self.slots_per_bucket as usize {
                        if self.filters[i][base + s] == 0 {
                            self.filters[i][base + s] = p.fp;
                            self.num_items += 1;
                            return true;
                        }
                    }
                }
            }

            if self.ko_insert(&p) {
                self.num_items += 1;
                self.num_ko_inserts += 1;
                return true;
            }

            if self.expansion == 0 || !self.add_new_sub_filter() {
                return false;
            }
            // The new sub-filter has empty slots; insert succeeds next iteration.
        }
    }

    /// Inserts only if the hash is not already present. Returns false if the
    /// item already exists or the filter is full.
    pub fn insert_unique(&mut self, hash: u64) -> bool {
        if self.exists(hash) {
            return false;
        }
        self.insert(hash)
    }

    /// Returns true if the hash is present in the filter. May return false
    /// positives but never false negatives.
    #[must_use]
    pub fn exists(&self, hash: u64) -> bool {
        let p = Self::lookup_params_from_hash(hash);
        for sf in &self.filters {
            let (i1, i2) = self.bucket_indices(sf, &p);
            for idx in [i1, i2] {
                let base = (idx * u64::from(self.slots_per_bucket)) as usize;
                for s in 0..self.slots_per_bucket as usize {
                    if sf[base + s] == p.fp {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Returns the number of fingerprint matches across both candidate buckets
    /// and all sub-filters. Each successful `insert` of the same item occupies
    /// its own slot (insert never deduplicates), so this reflects how many
    /// times the item was added minus how many times it was deleted.
    #[must_use]
    pub fn count(&self, hash: u64) -> usize {
        let p = Self::lookup_params_from_hash(hash);
        let mut count = 0usize;
        for sf in &self.filters {
            let (i1, i2) = self.bucket_indices(sf, &p);
            for idx in [i1, i2] {
                let base = (idx * u64::from(self.slots_per_bucket)) as usize;
                for s in 0..self.slots_per_bucket as usize {
                    if sf[base + s] == p.fp {
                        count += 1;
                    }
                }
            }
        }
        count
    }

    /// Removes one occurrence of the hash from the filter. Returns true if
    /// found and removed. This is the key advantage over Bloom filters, which
    /// do not support deletion.
    pub fn delete(&mut self, hash: u64) -> bool {
        let p = Self::lookup_params_from_hash(hash);
        for i in (0..self.filters.len()).rev() {
            let (i1, i2) = self.bucket_indices(&self.filters[i], &p);
            for idx in [i1, i2] {
                let base = (idx * u64::from(self.slots_per_bucket)) as usize;
                for s in 0..self.slots_per_bucket as usize {
                    if self.filters[i][base + s] == p.fp {
                        self.filters[i][base + s] = 0;
                        self.num_items -= 1;
                        self.num_deletes += 1;
                        return true;
                    }
                }
            }
        }
        false
    }

    /// `XXH3_64bits_withSeed(item, 0xc6a4a7935bd1e995ULL)`.
    #[must_use]
    pub fn hash(item: &[u8]) -> u64 {
        xxh3_64_with_seed(item, K_CUCKOO_SEED)
    }

    #[must_use]
    pub fn num_items(&self) -> u64 {
        self.num_items
    }

    /// Number of times an insertion found both candidate buckets full and had
    /// to evict an existing fingerprint to its alternate bucket before the new
    /// fingerprint could be placed (mirrors `NumKOInserts`, used by tests).
    #[must_use]
    pub fn num_ko_inserts(&self) -> u64 {
        self.num_ko_inserts
    }

    #[must_use]
    pub fn num_buckets(&self) -> u64 {
        self.num_buckets
    }

    #[must_use]
    pub fn num_filters(&self) -> usize {
        self.filters.len()
    }

    #[must_use]
    pub fn num_deletes(&self) -> u64 {
        self.num_deletes
    }

    #[must_use]
    pub fn slots_per_bucket(&self) -> u8 {
        self.slots_per_bucket
    }

    #[must_use]
    pub fn max_iterations(&self) -> u16 {
        self.max_iterations
    }

    #[must_use]
    pub fn expansion(&self) -> u16 {
        self.expansion
    }

    /// Approximate heap bytes used by this filter's sub-filter data.
    #[must_use]
    pub fn malloc_used(&self) -> usize {
        std::mem::size_of::<CuckooFilter>()
            + self.filters.capacity() * std::mem::size_of::<Vec<u8>>()
            + self.filters.iter().map(std::vec::Vec::len).sum::<usize>()
    }

    /// Reclaims space by moving items from newer sub-filters back into older
    /// ones, freeing the newest sub-filter once it's been fully emptied. Only
    /// ever frees the last sub-filter, one at a time, working from newest down
    /// to (but not including) the first. If `cont` is false the algorithm
    /// stops at the first sub-filter that can't be fully emptied; if `cont` is
    /// true (CF.COMPACT) it keeps trying older sub-filters regardless.
    pub fn compact(&mut self, cont: bool) {
        for i in (1..self.filters.len()).rev() {
            if !self.compact_single_filter(i) && !cont {
                break;
            }
        }
        self.num_deletes = 0;
    }

    /// Serialize the filter as a single blob: `slots_per_bucket(1)` +
    /// `max_iterations(2` LE) + expansion(2 LE) + `num_buckets(8)` + `num_items(8)` +
    /// `num_deletes(8)` + per sub-filter length-prefixed raw bytes. Port-local
    /// wire format for the RDB save/load paths (the reference serializes
    /// sub-filters individually for a module RDB).
    #[must_use]
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(self.slots_per_bucket);
        out.extend_from_slice(&self.max_iterations.to_le_bytes());
        out.extend_from_slice(&self.expansion.to_le_bytes());
        out.extend_from_slice(&self.num_buckets.to_le_bytes());
        out.extend_from_slice(&self.num_items.to_le_bytes());
        out.extend_from_slice(&self.num_deletes.to_le_bytes());
        for f in &self.filters {
            out.extend_from_slice(&(f.len() as u64).to_le_bytes());
            out.extend_from_slice(f);
        }
        out
    }

    /// Deserialize a blob written by `serialize`. Returns `None` on a
    /// malformed or truncated payload.
    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<CuckooFilter> {
        if bytes.len() < 29 {
            return None;
        }
        let slots_per_bucket = bytes[0];
        if slots_per_bucket == 0 {
            return None;
        }
        let max_iterations = u16::from_le_bytes(bytes[1..3].try_into().ok()?);
        let expansion = u16::from_le_bytes(bytes[3..5].try_into().ok()?);
        let num_buckets = u64::from_le_bytes(bytes[5..13].try_into().ok()?);
        let num_items = u64::from_le_bytes(bytes[13..21].try_into().ok()?);
        let num_deletes = u64::from_le_bytes(bytes[21..29].try_into().ok()?);
        let mut rest = &bytes[29..];
        let mut filters = Vec::new();
        while !rest.is_empty() {
            if rest.len() < 8 {
                return None;
            }
            let len = u64::from_le_bytes(rest[0..8].try_into().ok()?) as usize;
            rest = &rest[8..];
            if rest.len() < len {
                return None;
            }
            filters.push(rest[..len].to_vec());
            rest = &rest[len..];
        }
        if filters.is_empty() || num_buckets == 0 {
            return None;
        }
        for f in &filters {
            if f.len() % slots_per_bucket as usize != 0 {
                return None;
            }
        }
        Some(CuckooFilter {
            slots_per_bucket,
            max_iterations,
            expansion,
            num_buckets,
            num_items,
            num_deletes,
            num_ko_inserts: 0,
            filters,
        })
    }

    fn lookup_params_from_hash(hash: u64) -> LookupParams {
        let fp = fingerprint(hash);
        LookupParams {
            fp,
            h1: hash,
            h2: alt_index(fp, hash),
        }
    }

    /// `{h1 % n, h2 % n}` for the given sub-filter.
    fn bucket_indices(&self, sf: &[u8], p: &LookupParams) -> (u64, u64) {
        let n = self.num_buckets_of(sf);
        (p.h1 % n, p.h2 % n)
    }

    fn num_buckets_of(&self, sf: &[u8]) -> u64 {
        (sf.len() / self.slots_per_bucket as usize) as u64
    }

    /// Appends a new sub-filter sized `num_buckets_ * expansion^filters_.size()`.
    /// This is a Redis engineering choice to avoid rehashing on growth; the
    /// original Fan et al. paper describes a single fixed-size filter only.
    fn add_new_sub_filter(&mut self) -> bool {
        const K_MAX_BUCKETS: u64 = (1 << 56) - 1; // preserve SubFilter numBuckets semantics

        let growth = f64::from(self.expansion).powi(self.filters.len() as i32) as u64;

        if growth > K_MAX_BUCKETS / self.num_buckets {
            return false;
        }

        let bucket_count = self.num_buckets * growth;
        if bucket_count > u64::MAX / u64::from(self.slots_per_bucket) {
            return false;
        }

        self.filters.push(vec![
            0u8;
            (bucket_count * u64::from(self.slots_per_bucket))
                as usize
        ]);
        true
    }

    /// When both candidate buckets of the newest sub-filter are full, evicts a
    /// fingerprint from `h1`, places ours there, then tries to reinsert the
    /// evicted fingerprint into its own alternate bucket. Repeats up to
    /// `max_iterations_` times. On failure, rolls back all swaps.
    fn ko_insert(&mut self, p: &LookupParams) -> bool {
        let sf_idx = self.filters.len() - 1;
        let n = self.num_buckets_of(&self.filters[sf_idx]);
        let mut idx = p.h1 % n;
        let mut fp = p.fp;
        let mut victim_slot = 0u8;

        for _ in 0..self.max_iterations {
            // Evict the fingerprint at victim_slot in bucket idx and take its
            // place. Then jump to the evicted fingerprint's alternate bucket.
            // victim_slot cycles across slots to avoid displacement cycles.
            let pos = (idx * u64::from(self.slots_per_bucket)) as usize + victim_slot as usize;
            std::mem::swap(&mut self.filters[sf_idx][pos], &mut fp);
            idx = alt_index(fp, idx) % n;

            for s in 0..self.slots_per_bucket as usize {
                let pos = (idx * u64::from(self.slots_per_bucket)) as usize + s;
                if self.filters[sf_idx][pos] == 0 {
                    self.filters[sf_idx][pos] = fp;
                    return true;
                }
            }
            victim_slot = (victim_slot + 1) % self.slots_per_bucket;
        }

        // Roll back all swaps to restore the sub-filter to its original state.
        for _ in 0..self.max_iterations {
            victim_slot = (victim_slot + self.slots_per_bucket - 1) % self.slots_per_bucket;
            idx = alt_index(fp, idx) % n;
            let pos = (idx * u64::from(self.slots_per_bucket)) as usize + victim_slot as usize;
            std::mem::swap(&mut self.filters[sf_idx][pos], &mut fp);
        }

        false
    }

    /// Attempts to relocate every occupied slot in `filters[filter_idx]` into
    /// some earlier sub-filter. Returns true if every slot was relocated or
    /// already empty (i.e. this sub-filter is empty and safe to free if it's
    /// the last one).
    fn compact_single_filter(&mut self, filter_idx: usize) -> bool {
        let n = self.num_buckets_of(&self.filters[filter_idx]);
        let mut fully_emptied = true;
        for bucket_idx in 0..n {
            for slot_idx in 0..self.slots_per_bucket {
                if !self.relocate_slot(filter_idx, bucket_idx, slot_idx) {
                    fully_emptied = false;
                }
            }
        }
        // Only the newest sub-filter can ever be freed: freeing a middle one
        // would leave a gap that breaks the bucket-count-growth invariant
        // `relocate_slot` relies on.
        if fully_emptied && filter_idx == self.filters.len() - 1 {
            self.filters.pop();
        }
        fully_emptied
    }

    /// Tries to move the fingerprint located by the parameters into the first
    /// earlier sub-filter (lowest index first) with room for it. Returns true
    /// if the slot was already empty or the fingerprint was relocated; false
    /// if no earlier sub-filter had room.
    fn relocate_slot(&mut self, filter_idx: usize, bucket_idx: u64, slot_idx: u8) -> bool {
        let slot_pos = (bucket_idx * u64::from(self.slots_per_bucket)) as usize + slot_idx as usize;
        let fp = self.filters[filter_idx][slot_pos];
        if fp == 0 {
            return true;
        }

        // bucket_idx is this sub-filter's bucket index, not the raw hash.
        // Reusing it works because each sub-filter's bucket count is a
        // power-of-two multiple of every earlier sub-filter's count, so
        // bucket_idx % earlier_n == raw_hash % earlier_n.
        let alt_bucket_idx = alt_index(fp, bucket_idx);

        for prior in 0..filter_idx {
            let n = self.num_buckets_of(&self.filters[prior]);
            for idx in [bucket_idx % n, alt_bucket_idx % n] {
                let base = (idx * u64::from(self.slots_per_bucket)) as usize;
                for s in 0..self.slots_per_bucket as usize {
                    if self.filters[prior][base + s] == 0 {
                        self.filters[prior][base + s] = fp;
                        self.filters[filter_idx][slot_pos] = 0;
                        return true;
                    }
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(capacity: u64, expansion: u16) -> CuckooFilterOptions {
        CuckooFilterOptions {
            capacity,
            expansion,
            ..Default::default()
        }
    }

    #[test]
    fn insert_exists_delete() {
        let mut cf = CuckooFilter::new(&opts(1000, 1));
        let h = CuckooFilter::hash(b"foo");
        assert!(!cf.exists(h));
        assert!(cf.insert(h));
        assert!(cf.exists(h));
        assert_eq!(cf.count(h), 1);

        // Duplicate insertions occupy their own slot.
        assert!(cf.insert(h));
        assert_eq!(cf.count(h), 2);

        assert!(cf.delete(h));
        assert!(cf.exists(h));
        assert_eq!(cf.count(h), 1);
        assert!(cf.delete(h));
        assert!(!cf.exists(h));
        assert_eq!(cf.count(h), 0);
        assert!(!cf.delete(h));
        assert_eq!(cf.num_items(), 0);
        assert_eq!(cf.num_deletes(), 2);
    }

    #[test]
    fn insert_unique_deduplicates() {
        let mut cf = CuckooFilter::new(&opts(1000, 1));
        let h = CuckooFilter::hash(b"k1");
        assert!(cf.insert_unique(h));
        assert!(!cf.insert_unique(h));
        assert_eq!(cf.count(h), 1);
    }

    #[test]
    fn filter_full_without_expansion() {
        let mut cf = CuckooFilter::new(&opts(4, 0));
        assert_eq!(cf.expansion(), 0);
        for i in 0..4 {
            assert!(cf.insert(CuckooFilter::hash(&[i])), "insert {i}");
        }
        assert!(!cf.insert(CuckooFilter::hash(b"overflow")));
        assert_eq!(cf.num_items(), 4);
    }

    #[test]
    fn growth_adds_sub_filters() {
        let mut cf = CuckooFilter::new(&opts(4, 2));
        assert_eq!(cf.num_filters(), 1);
        assert_eq!(cf.expansion(), 2);
        for i in 0..100 {
            let item = format!("item{i}");
            assert!(cf.insert(CuckooFilter::hash(item.as_bytes())), "insert {i}");
        }
        assert!(cf.num_filters() > 1);
        for i in 0..100 {
            let item = format!("item{i}");
            assert!(cf.exists(CuckooFilter::hash(item.as_bytes())), "exists {i}");
        }
    }

    #[test]
    fn compact_moves_items_to_older_sub_filters() {
        let mut cf = CuckooFilter::new(&opts(4, 2));
        for i in 0..100 {
            let item = format!("item{i}");
            assert!(cf.insert(CuckooFilter::hash(item.as_bytes())));
        }
        let filters_before = cf.num_filters();
        for i in 0..90 {
            let item = format!("item{i}");
            assert!(cf.delete(CuckooFilter::hash(item.as_bytes())), "del {i}");
        }
        assert!(cf.num_deletes() > 0);

        cf.compact(true);
        assert_eq!(cf.num_deletes(), 0);
        assert!(cf.num_filters() <= filters_before);
        for i in 90..100 {
            let item = format!("item{i}");
            assert!(
                cf.exists(CuckooFilter::hash(item.as_bytes())),
                "survivor {i}"
            );
        }
    }

    #[test]
    fn serialize_deserialize_round_trip() {
        let options = CuckooFilterOptions {
            capacity: 1000,
            slots_per_bucket: 4,
            max_iterations: 10,
            expansion: 2,
        };
        let mut cf = CuckooFilter::new(&options);
        for i in 0..200 {
            let item = format!("item{i}");
            assert!(cf.insert(CuckooFilter::hash(item.as_bytes())));
        }
        assert!(cf.delete(CuckooFilter::hash(b"item0")));

        let blob = cf.serialize();
        let restored = CuckooFilter::deserialize(&blob).expect("valid blob");
        assert_eq!(restored.slots_per_bucket(), 4);
        assert_eq!(restored.max_iterations(), 10);
        assert_eq!(restored.expansion(), 2);
        assert_eq!(restored.num_buckets(), cf.num_buckets());
        assert_eq!(restored.num_items(), cf.num_items());
        assert_eq!(restored.num_deletes(), cf.num_deletes());
        assert_eq!(restored.num_filters(), cf.num_filters());
        assert_eq!(restored.serialize(), blob);
        for i in 0..200 {
            let item = format!("item{i}");
            assert_eq!(
                restored.exists(CuckooFilter::hash(item.as_bytes())),
                cf.exists(CuckooFilter::hash(item.as_bytes())),
                "exists {i}"
            );
        }

        assert!(CuckooFilter::deserialize(&blob[..blob.len() - 1]).is_none());
        assert!(CuckooFilter::deserialize(&[0u8; 28]).is_none());
    }

    #[test]
    fn num_buckets_rounds_up_to_power_of_two() {
        let cf = CuckooFilter::new(&opts(1000, 1));
        // ceil(1000 / 2) to a power of two.
        assert_eq!(cf.num_buckets(), 512);
        assert_eq!(cf.num_filters(), 1);
    }
}
