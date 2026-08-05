//! Top-K heavy-hitters sketch, ported from `dragonfly/src/core/topk.{h,cc}`.
//!
//! The sketch is a `depth x width` grid of `u32` counters (Count-Min Sketch
//! style, flattened to one row-major array) coupled with a min-heap of the
//! current Top-K items. Inserts increment every counter selected by one of the
//! `depth` hash rows; with probability `decay^count` an already-positive
//! counter is instead decremented (mutually exclusive with the increment).
//! The estimated frequency of an item is the minimum counter across all rows,
//! and the heap keeps only the `k` items with the highest estimates. This is
//! not a strict `HeavyKeeper` (no per-item fingerprint), so counts may overestimate
//! — which is acceptable for Top-K bounds.

use std::cell::Cell;
use std::sync::OnceLock;

use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::core::compact::CompactString;

pub const DEFAULT_WIDTH: u32 = 8;
pub const DEFAULT_DEPTH: u32 = 7;
pub const DEFAULT_DECAY: f64 = 0.9;
pub const DECAY_EPSILON: f64 = 1e-9;
/// Table size is 4097 so the max index is exactly 4096 (2^12), letting the
/// extrapolation fast-path reuse table values via division/remainder by a
/// power of two.
pub const DECAY_LOOKUP_SIZE: usize = 4097;

/// Shared table for the common `decay == 0.9` case (mirrors the reference's
/// process-wide static table), avoiding a ~32KB table per instance.
static DEFAULT_DECAY_TABLE: OnceLock<Box<[f64; DECAY_LOOKUP_SIZE]>> = OnceLock::new();

fn default_decay_table() -> &'static [f64; DECAY_LOOKUP_SIZE] {
    DEFAULT_DECAY_TABLE.get_or_init(|| {
        let mut t = Box::new([0.0; DECAY_LOOKUP_SIZE]);
        for (i, v) in t.iter_mut().enumerate() {
            *v = DEFAULT_DECAY.powf(i as f64);
        }
        t
    })
}

// Thread-local PRNG producing a uniform double in `[0, 1)`. The reference
// uses a thread-local Xoroshiro128p; the exact stream is irrelevant because
// every TOPK test asserts on bounds, not on specific decay outcomes.
thread_local! {
    static BITGEN: Cell<u64> = const { Cell::new(0x9E37_79B9_7F4A_7C15) };
}

fn uniform() -> f64 {
    BITGEN.with(|c| {
        let mut x = c.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        c.set(x);
        (x >> 11) as f64 * (1.0 / 9_007_199_254_740_992.0)
    })
}

/// An entry in the min-heap of the current Top-K items.
#[derive(Debug, Clone)]
struct HeapItem {
    key: CompactString,
    count: u32,
}

/// A single item in the Top-K list with its estimated count.
#[derive(Debug, Clone)]
pub struct TopkItem {
    pub item: CompactString,
    pub count: u32,
}

/// The Top-K sketch, mirroring `TOPK`.
#[derive(Debug, Clone)]
pub struct Topk {
    k: u32,
    width: u32,
    depth: u32,
    decay: f64,
    /// Custom decay lookup table; `None` for the default decay (shared static).
    decay_table: Option<Box<[f64; DECAY_LOOKUP_SIZE]>>,
    /// Flat `depth x width` counter grid (`counters_`).
    counters: Vec<u32>,
    /// Min-heap of the current Top-K items (`min_heap_`).
    heap: Vec<HeapItem>,
}

impl Topk {
    /// Initialize a sketch (`TOPK(mr, k, width, depth, decay)`). The caller
    /// validates `k`, `width`, `depth` and `decay` bounds.
    #[must_use]
    pub fn new(k: u32, width: u32, depth: u32, decay: f64) -> Self {
        debug_assert!(k > 0 && width > 0 && depth > 0 && (0.0..=1.0).contains(&decay));
        let counters = vec![0u32; (width as usize) * (depth as usize)];
        let decay_table = if (decay - DEFAULT_DECAY).abs() < DECAY_EPSILON {
            None
        } else {
            let mut t = Box::new([0.0; DECAY_LOOKUP_SIZE]);
            for (i, v) in t.iter_mut().enumerate() {
                *v = decay.powf(i as f64);
            }
            Some(t)
        };
        Topk {
            k,
            width,
            depth,
            decay,
            decay_table,
            counters,
            heap: Vec::with_capacity(k as usize),
        }
    }

    fn decay_table_ref(&self) -> &[f64; DECAY_LOOKUP_SIZE] {
        match &self.decay_table {
            Some(t) => t,
            None => default_decay_table(),
        }
    }

    /// The maximum number of items maintained in the min-heap.
    #[must_use]
    pub fn k(&self) -> u32 {
        self.k
    }

    /// The number of items currently tracked in the heap.
    #[must_use]
    pub fn size(&self) -> usize {
        self.heap.len()
    }

    #[must_use]
    pub fn width(&self) -> u32 {
        self.width
    }

    #[must_use]
    pub fn depth(&self) -> u32 {
        self.depth
    }

    #[must_use]
    pub fn decay(&self) -> f64 {
        self.decay
    }

    /// Estimate the frequency of an item: the minimum counter across all rows.
    #[must_use]
    pub fn count(&self, item: &[u8]) -> u32 {
        let mut min_count = u32::MAX;
        for row in 0..self.depth {
            let idx = self.counter_index(item, row);
            min_count = min_count.min(self.counters[idx]);
        }
        min_count
    }

    /// Whether the item currently resides in the Top-K heap.
    #[must_use]
    pub fn query(&self, item: &[u8]) -> bool {
        self.heap.iter().any(|h| h.key.as_bytes() == item)
    }

    /// Insert an item, incrementing its estimated frequency by 1. Returns the
    /// evicted item's key if this insertion displaced a resident item.
    pub fn add(&mut self, item: &[u8]) -> Option<CompactString> {
        self.increment_internal(item, 1)
    }

    /// Increment an item's estimated frequency by a specific amount. An
    /// increment of 0 is a safe no-op that returns `None`.
    pub fn incr_by(&mut self, item: &[u8], increment: u32) -> Option<CompactString> {
        if increment < 1 {
            return None;
        }
        self.increment_internal(item, increment)
    }

    /// The complete list of current Top-K items, sorted descending by count
    /// with lexicographic tie-break (deterministic, Redis-compatible).
    #[must_use]
    pub fn list(&self) -> Vec<TopkItem> {
        let mut result: Vec<TopkItem> = self
            .heap
            .iter()
            .map(|h| TopkItem {
                item: h.key.clone(),
                count: h.count,
            })
            .collect();
        result.sort_by(|a, b| {
            if a.count == b.count {
                a.item.as_bytes().cmp(b.item.as_bytes())
            } else {
                b.count.cmp(&a.count)
            }
        });
        result
    }

    /// Total heap memory dynamically allocated by this instance, including
    /// the custom decay table, counter grid and heap allocations.
    #[must_use]
    pub fn malloc_used(&self) -> usize {
        let mut size = 0;
        if self.decay_table.is_some() {
            size += DECAY_LOOKUP_SIZE * std::mem::size_of::<f64>();
        }
        size += self.counters.capacity() * std::mem::size_of::<u32>();
        size += self.heap.capacity() * std::mem::size_of::<HeapItem>();
        for item in &self.heap {
            size += item.key.as_bytes().len();
        }
        size
    }

    // -----------------------------------------------------------------------
    // Internal machinery
    // -----------------------------------------------------------------------

    /// Hash the item for a row and compute its flattened 1D index: `bucket =
    /// (low32(hash) * width) >> 32` (Lemire's fast range reduction), then
    /// `row * width + bucket`.
    fn counter_index(&self, item: &[u8], row: u32) -> usize {
        debug_assert!(row < self.depth);
        let full_hash = xxh3_64_with_seed(item, u64::from(row));
        let bucket = (u64::from(full_hash as u32) * u64::from(self.width)) >> 32;
        debug_assert!(bucket < u64::from(self.width));
        (row as usize) * (self.width as usize) + bucket as usize
    }

    /// `decay^count`, using the lookup table when possible and extrapolating
    /// via the laws of exponents for counts beyond the table's max index:
    /// `decay^(Q*M+R) = decay^(M*Q) * decay^R = table[M]^Q * table[R]`.
    fn compute_decay_probability(&self, count: u32) -> f64 {
        let table = self.decay_table_ref();
        debug_assert!(count > 0);
        let idx = count as usize;
        if idx < DECAY_LOOKUP_SIZE {
            return table[idx];
        }
        // If the tail probability is below epsilon, decay is statistically
        // impossible; skip the expensive pow extrapolation.
        if table[DECAY_LOOKUP_SIZE - 1] < DECAY_EPSILON {
            return 0.0;
        }
        let m = (DECAY_LOOKUP_SIZE - 1) as u32;
        let base = table[DECAY_LOOKUP_SIZE - 1];
        base.powf(f64::from(count / m)) * table[(count % m) as usize]
    }

    fn should_decay(&self, count: u32) -> bool {
        if count == 0 {
            return false;
        }
        uniform() < self.compute_decay_probability(count)
    }

    fn increment_internal(&mut self, item: &[u8], increment: u32) -> Option<CompactString> {
        let mut min_count = u32::MAX;
        for row in 0..self.depth {
            let idx = self.counter_index(item, row);
            // Decay and increment are mutually exclusive: with probability
            // decay^count the counter is decremented (colliding items suppress
            // each other); otherwise it is incremented.
            if self.counters[idx] > 0 && self.should_decay(self.counters[idx]) {
                self.counters[idx] -= 1;
            } else {
                self.counters[idx] = self.counters[idx].saturating_add(increment);
            }
            min_count = min_count.min(self.counters[idx]);
        }
        self.update_heap(item, min_count)
    }

    /// Restore the min-heap property after an existing item's count changed
    /// or a new item entered the heap. Returns the evicted key when the heap
    /// was full and a stronger item displaced the minimum.
    fn update_heap(&mut self, item: &[u8], new_count: u32) -> Option<CompactString> {
        for i in 0..self.heap.len() {
            if self.heap[i].key.as_bytes() == item {
                let old_count = self.heap[i].count;
                self.heap[i].count = new_count;
                if new_count > old_count {
                    self.heapify_down(i);
                } else if new_count < old_count {
                    self.heapify_up(i);
                }
                return None;
            }
        }

        // Fast reject: the item doesn't qualify for the heap.
        if self.heap.len() >= self.k as usize && new_count <= self.heap[0].count {
            return None;
        }
        debug_assert!(self.heap.len() <= self.k as usize);

        if self.heap.len() < self.k as usize {
            // Heap not full: add the item, no eviction.
            let new_idx = self.heap.len();
            self.heap.push(HeapItem {
                key: CompactString::from_bytes(item),
                count: new_count,
            });
            self.heapify_up(new_idx);
            return None;
        }

        // Heap is full: evict the minimum and add the new item.
        debug_assert_eq!(self.heap.len(), self.k as usize);
        let old_key = self.heap[0].key.clone();
        self.heap[0] = HeapItem {
            key: CompactString::from_bytes(item),
            count: new_count,
        };
        self.heapify_down(0);
        Some(old_key)
    }

    fn heapify_up(&mut self, mut index: usize) {
        debug_assert!(index < self.heap.len());
        while index > 0 {
            let parent = (index - 1) / 2;
            if self.heap[parent].count <= self.heap[index].count {
                break;
            }
            self.heap.swap(parent, index);
            index = parent;
        }
    }

    fn heapify_down(&mut self, mut index: usize) {
        debug_assert!(index < self.heap.len());
        let size = self.heap.len();
        loop {
            let left = 2 * index + 1;
            let right = 2 * index + 2;
            let mut smallest = index;
            if left < size && self.heap[left].count < self.heap[smallest].count {
                smallest = left;
            }
            if right < size && self.heap[right].count < self.heap[smallest].count {
                smallest = right;
            }
            if smallest == index {
                break;
            }
            self.heap.swap(smallest, index);
            index = smallest;
        }
    }

    /// Rebuild the min-heap property from an arbitrary array (bottom-up
    /// sift-down, equivalent to `std::make_heap` with `std::greater`).
    fn make_heap(&mut self) {
        let n = self.heap.len();
        for i in (0..n / 2).rev() {
            self.heapify_down(i);
        }
    }

    // -----------------------------------------------------------------------
    // Serialization (port-local RDB wire format)
    // -----------------------------------------------------------------------

    /// Serialize the sketch as a single blob: k(4) + width(4) + depth(4) +
    /// decay(8) + heap (count(4), then count(4) + item len(4) + item bytes per
    /// entry) + counter count(4) + counter values (4 LE each).
    #[must_use]
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.k.to_le_bytes());
        out.extend_from_slice(&self.width.to_le_bytes());
        out.extend_from_slice(&self.depth.to_le_bytes());
        out.extend_from_slice(&self.decay.to_le_bytes());
        out.extend_from_slice(&(self.heap.len() as u32).to_le_bytes());
        for item in &self.heap {
            out.extend_from_slice(&item.count.to_le_bytes());
            out.extend_from_slice(&(item.key.as_bytes().len() as u32).to_le_bytes());
            out.extend_from_slice(item.key.as_bytes());
        }
        out.extend_from_slice(&(self.counters.len() as u32).to_le_bytes());
        for c in &self.counters {
            out.extend_from_slice(&c.to_le_bytes());
        }
        out
    }

    /// Deserialize a blob written by `serialize`. Returns `None` on a
    /// malformed or truncated payload.
    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<Topk> {
        fn u32_at(b: &[u8], off: &mut usize) -> Option<u32> {
            let end = off.checked_add(4)?;
            let s = b.get(*off..end)?;
            *off = end;
            Some(u32::from_le_bytes(s.try_into().ok()?))
        }
        let mut off = 0usize;
        let k = u32_at(bytes, &mut off)?;
        let width = u32_at(bytes, &mut off)?;
        let depth = u32_at(bytes, &mut off)?;
        let end = off.checked_add(8)?;
        let decay = f64::from_le_bytes(bytes.get(off..end)?.try_into().ok()?);
        off = end;
        if k == 0 || width == 0 || depth == 0 || !(0.0..=1.0).contains(&decay) {
            return None;
        }
        let nheap = u32_at(bytes, &mut off)? as usize;
        if nheap > k as usize {
            return None;
        }
        let mut heap = Vec::with_capacity(nheap);
        for _ in 0..nheap {
            let count = u32_at(bytes, &mut off)?;
            let len = u32_at(bytes, &mut off)? as usize;
            let end = off.checked_add(len)?;
            let key = CompactString::from_bytes(bytes.get(off..end)?);
            off = end;
            heap.push(HeapItem { key, count });
        }
        let ncounters = u32_at(bytes, &mut off)? as usize;
        let expected = (width as usize).checked_mul(depth as usize)?;
        if ncounters != expected {
            return None;
        }
        let mut counters = Vec::with_capacity(ncounters);
        for _ in 0..ncounters {
            counters.push(u32_at(bytes, &mut off)?);
        }
        if off != bytes.len() {
            return None;
        }
        let mut topk = Topk::new(k, width, depth, decay);
        topk.heap = heap;
        topk.counters = counters;
        topk.make_heap();
        Some(topk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_query() {
        let mut topk = Topk::new(5, 50, 7, 0.9);
        assert!(topk.add(b"foo").is_none());
        assert!(topk.query(b"foo"));
        assert!(!topk.query(b"bar"));
        assert_eq!(topk.size(), 1);
    }

    #[test]
    fn count_is_at_least_actual() {
        let mut topk = Topk::new(5, 100, 7, 0.9);
        topk.incr_by(b"foo", 10);
        assert!(topk.count(b"foo") >= 10);
        assert_eq!(topk.count(b"neveradded"), 0);
    }

    #[test]
    fn incr_by_zero_is_noop() {
        let mut topk = Topk::new(5, 50, 7, 0.9);
        assert!(topk.incr_by(b"foo", 0).is_none());
        assert_eq!(topk.count(b"foo"), 0);
        assert!(!topk.query(b"foo"));
    }

    #[test]
    fn heap_capped_at_k() {
        let mut topk = Topk::new(3, 50, 7, 0.9);
        for i in 0..10 {
            topk.incr_by(format!("item{i}").as_bytes(), 100);
        }
        assert_eq!(topk.size(), 3);
        assert_eq!(topk.list().len(), 3);
    }

    #[test]
    fn list_sorted_descending_with_tiebreak() {
        let mut topk = Topk::new(5, 100, 7, 0.9);
        topk.incr_by(b"low", 10);
        topk.incr_by(b"mid", 50);
        topk.incr_by(b"high", 100);
        let list = topk.list();
        let counts: Vec<u32> = list.iter().map(|i| i.count).collect();
        assert!(counts.windows(2).all(|w| w[0] >= w[1]));
        assert_eq!(list[0].item.as_bytes(), b"high");
    }

    #[test]
    fn weak_item_does_not_evict_heavy_items() {
        let mut topk = Topk::new(2, 50, 7, 0.9);
        assert!(topk.incr_by(b"heavy1", 10000).is_none());
        assert!(topk.incr_by(b"heavy2", 5000).is_none());
        // A weak item can't beat the heap minimum (5000).
        assert!(topk.add(b"weak").is_none());
        assert_eq!(topk.size(), 2);
        assert!(topk.query(b"heavy1"));
        assert!(topk.query(b"heavy2"));
        assert!(!topk.query(b"weak"));
        // A strong item evicts the weakest.
        let evicted = topk.incr_by(b"newcomer", 100_000).expect("eviction");
        assert_eq!(evicted.as_bytes(), b"heavy2");
    }

    #[test]
    fn decay_one_always_decays_positive_counters() {
        let mut topk = Topk::new(1, 50, 7, 1.0);
        topk.incr_by(b"heavy", 1000);
        topk.incr_by(b"victim", 5);
        assert!(!topk.query(b"victim"));
        assert!(topk.count(b"victim") >= 5);
        assert!(topk.count(b"heavy") >= 1000);
    }

    #[test]
    fn count_outside_heap_still_reports() {
        let mut topk = Topk::new(1, 50, 7, 1.0);
        topk.incr_by(b"heavy", 1000);
        topk.incr_by(b"victim", 5);
        // victim's min counter is 5 (its own untouched rows).
        assert_eq!(topk.count(b"victim"), 5);
    }

    #[test]
    fn serialize_deserialize() {
        let mut topk = Topk::new(5, 50, 7, 0.9);
        topk.incr_by(b"foo", 10);
        topk.incr_by(b"bar", 3);
        topk.incr_by(b"baz", 7);

        let blob = topk.serialize();
        let restored = Topk::deserialize(&blob).expect("valid blob");
        assert_eq!(restored.k(), topk.k());
        assert_eq!(restored.width(), topk.width());
        assert_eq!(restored.depth(), topk.depth());
        assert_eq!(restored.decay(), topk.decay());
        assert_eq!(restored.size(), topk.size());
        assert_eq!(restored.count(b"foo"), topk.count(b"foo"));
        assert_eq!(restored.count(b"bar"), topk.count(b"bar"));
        assert_eq!(restored.count(b"baz"), topk.count(b"baz"));
        // The heap is rebuilt; query still finds every resident item.
        for item in topk.list() {
            assert!(restored.query(item.item.as_bytes()));
        }

        assert!(Topk::deserialize(&blob[..blob.len() - 1]).is_none());
        assert!(Topk::deserialize(b"\0\0").is_none());
    }

    #[test]
    fn custom_decay_serialize_round_trip() {
        let mut topk = Topk::new(4, 8, 5, 0.75);
        topk.incr_by(b"a", 10);
        let blob = topk.serialize();
        let restored = Topk::deserialize(&blob).unwrap();
        assert_eq!(restored.decay(), 0.75);
        assert_eq!(restored.count(b"a"), topk.count(b"a"));
        assert!(restored.decay_table.is_some());
    }

    #[test]
    fn malloc_used_scales() {
        let topk = Topk::new(10, 100, 7, 0.9);
        assert!(topk.malloc_used() >= 100 * 7 * 4);
        let custom = Topk::new(10, 100, 7, 0.75);
        assert!(custom.malloc_used() > topk.malloc_used());
    }
}
