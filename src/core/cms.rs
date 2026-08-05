//! Count-Min Sketch, ported from `dragonfly/src/core/cms.{h,cc}`.
//!
//! The sketch is a `width x depth` matrix of `i64` counters (depth rows, one
//! per hash function). Both row/column indices are derived from a single
//! `XXH3_128bits` fingerprint: `idx = (h1 + row*h2) % width`. `IncrBy` and
//! `Query` return the minimum counter across all rows, which is an upper bound
//! on the true count with an additive error of `e` and a failure probability of
//! `p` when constructed with `(width, depth) = (ceil(e/e), ceil(ln(1/p)))`.

use xxhash_rust::xxh3::xxh3_128;

/// The offset into the flat counter array for `row`/`col`, mirroring the C++
/// `Offset` helper (unsigned wraparound included).
fn offset(h1: u64, h2: u64, row: u32, width: u32) -> usize {
    let idx = h1.wrapping_add(u64::from(row).wrapping_mul(h2)) % u64::from(width);
    (row as usize) * (width as usize) + idx as usize
}

/// The `XXH3_128bits` fingerprint split into low/high parts.
fn hash_fp(item: &[u8]) -> (u64, u64) {
    let h = xxh3_128(item);
    (h as u64, (h >> 64) as u64)
}

/// A count-min sketch, mirroring `CMS`. Owns its counters (the C++ uses an
/// external PMR allocator, which the Rust port folds into the `Vec`).
#[derive(Debug, Clone)]
pub struct Cms {
    width: u32,
    depth: u32,
    count: i64,
    counters: Vec<i64>,
}

impl Cms {
    /// Create a CMS with the given dimensions (`CMS(width, depth, mr)`).
    #[must_use]
    pub fn new(width: u32, depth: u32) -> Self {
        Cms {
            width,
            depth,
            count: 0,
            counters: vec![0i64; (width as usize) * (depth as usize)],
        }
    }

    /// Create a CMS from an error rate and probability
    /// (`CMS(ErrorRateTag{}, error, probability, mr)`):
    /// `width = ceil(e / error)`, `depth = ceil(ln(1 / probability))`.
    /// The caller validates that `error`/`probability` are in (0, 1) and that
    /// the derived dimensions fit (the command layer mirrors
    /// `ComputeCmsDimensions`).
    #[must_use]
    pub fn new_by_error(error: f64, probability: f64) -> Self {
        let width = (std::f64::consts::E / error).ceil() as u32;
        let depth = (1.0 / probability).ln().ceil() as u32;
        Self::new(width, depth)
    }

    /// Increment the count for an item, returning the new estimated count.
    pub fn incr_by(&mut self, item: &[u8], increment: i64) -> i64 {
        self.count += increment;
        let (h1, h2) = hash_fp(item);
        let mut min_count = i64::MAX;
        for row in 0..self.depth {
            let idx = offset(h1, h2, row, self.width);
            self.counters[idx] += increment;
            min_count = min_count.min(self.counters[idx]);
        }
        min_count
    }

    /// Query the estimated count for an item.
    #[must_use]
    pub fn query(&self, item: &[u8]) -> i64 {
        let (h1, h2) = hash_fp(item);
        let mut min_count = i64::MAX;
        for row in 0..self.depth {
            let idx = offset(h1, h2, row, self.width);
            min_count = min_count.min(self.counters[idx]);
        }
        min_count
    }

    /// Merge another CMS into this one with the given weight. Returns false if
    /// the dimensions don't match.
    pub fn merge_from(&mut self, other: &Cms, weight: i64) -> bool {
        if self.width != other.width || self.depth != other.depth {
            return false;
        }
        for (a, b) in self.counters.iter_mut().zip(other.counters.iter()) {
            *a += b * weight;
        }
        self.count += other.count * weight;
        true
    }

    /// Reset all counters and the total count to zero.
    pub fn reset(&mut self) {
        self.counters.fill(0);
        self.count = 0;
    }

    /// Load serialized counter state (`Load(total_incr_count, data)`). `data`
    /// must have exactly `num_counters()` elements.
    pub fn load(&mut self, total_incr_count: i64, data: &[i64]) {
        debug_assert_eq!(data.len(), self.num_counters());
        self.count = total_incr_count;
        self.counters.copy_from_slice(data);
    }

    #[must_use]
    pub fn width(&self) -> u32 {
        self.width
    }

    #[must_use]
    pub fn depth(&self) -> u32 {
        self.depth
    }

    /// Total count of all `incr_by` operations (used by CMS.INFO).
    #[must_use]
    pub fn total_count(&self) -> i64 {
        self.count
    }

    /// Memory usage in bytes (`MallocUsed`).
    #[must_use]
    pub fn malloc_used(&self) -> usize {
        self.num_counters() * std::mem::size_of::<i64>()
    }

    #[must_use]
    pub fn num_counters(&self) -> usize {
        (self.width as usize) * (self.depth as usize)
    }

    /// The flat counter array (`Data`).
    #[must_use]
    pub fn data(&self) -> &[i64] {
        &self.counters
    }

    /// Serialize the sketch as a single blob: width(4) + depth(4) + count(8) +
    /// counter values (8 LE each). Port-local wire format (the reference does
    /// not persist CMS values in RDB); used by the RDB save/load paths.
    #[must_use]
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.malloc_used() + 16);
        out.extend_from_slice(&self.width.to_le_bytes());
        out.extend_from_slice(&self.depth.to_le_bytes());
        out.extend_from_slice(&self.count.to_le_bytes());
        for c in &self.counters {
            out.extend_from_slice(&c.to_le_bytes());
        }
        out
    }

    /// Deserialize a blob written by `serialize`. Returns `None` on a malformed
    /// or truncated payload.
    #[must_use]
    pub fn deserialize(bytes: &[u8]) -> Option<Cms> {
        if bytes.len() < 16 {
            return None;
        }
        let width = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
        let depth = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
        let count = i64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let num = (width as usize).checked_mul(depth as usize)?;
        let need = 16usize.checked_add(num.checked_mul(8)?)?;
        if bytes.len() < need {
            return None;
        }
        let mut counters = Vec::with_capacity(num);
        for chunk in bytes[16..need].chunks_exact(8) {
            counters.push(i64::from_le_bytes(chunk.try_into().ok()?));
        }
        Some(Cms {
            width,
            depth,
            count,
            counters,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incr_query() {
        let mut cms = Cms::new(100, 5);
        assert_eq!(cms.query(b"foo"), 0);
        assert_eq!(cms.incr_by(b"foo", 3), 3);
        assert_eq!(cms.incr_by(b"foo", 4), 7);
        assert_eq!(cms.incr_by(b"bar", 1), 1);
        assert_eq!(cms.query(b"foo"), 7);
        assert_eq!(cms.query(b"bar"), 1);
        assert_eq!(cms.total_count(), 8);
    }

    #[test]
    fn new_by_error_dimensions() {
        let cms = Cms::new_by_error(0.01, 0.01);
        // ceil(e / 0.01) = 272, ceil(ln(100)) = 5
        assert_eq!(cms.width(), 272);
        assert_eq!(cms.depth(), 5);
    }

    #[test]
    fn merge_weights() {
        let mut a = Cms::new(100, 5);
        a.incr_by(b"foo", 5);
        a.incr_by(b"bar", 3);
        let mut b = Cms::new(100, 5);
        b.incr_by(b"foo", 2);
        b.incr_by(b"bar", 3);
        b.incr_by(b"baz", 1);

        let mut dest = Cms::new(100, 5);
        assert!(dest.merge_from(&a, 2));
        assert!(dest.merge_from(&b, 3));
        assert_eq!(dest.query(b"foo"), 16);
        assert_eq!(dest.query(b"bar"), 15);
        assert_eq!(dest.query(b"baz"), 3);
        assert_eq!(dest.total_count(), 2 * 8 + 3 * 6);

        // dimension mismatch
        assert!(!dest.merge_from(&Cms::new(101, 5), 1));
        assert!(!dest.merge_from(&Cms::new(100, 6), 1));
    }

    #[test]
    fn reset_and_load() {
        let mut cms = Cms::new(100, 5);
        cms.incr_by(b"a", 1);
        cms.reset();
        assert_eq!(cms.total_count(), 0);
        assert_eq!(cms.query(b"a"), 0);

        let mut other = Cms::new(100, 5);
        other.incr_by(b"x", 42);
        cms.load(other.total_count(), other.data());
        assert_eq!(cms.total_count(), 42);
        assert_eq!(cms.query(b"x"), 42);
    }

    #[test]
    fn serialize_deserialize() {
        let mut cms = Cms::new(1000, 5);
        cms.incr_by(b"foo", 5);
        cms.incr_by(b"bar", 3);
        cms.incr_by(b"baz", 9);

        let blob = cms.serialize();
        let restored = Cms::deserialize(&blob).expect("valid blob");
        assert_eq!(restored.width(), cms.width());
        assert_eq!(restored.depth(), cms.depth());
        assert_eq!(restored.total_count(), cms.total_count());
        assert_eq!(restored.data(), cms.data());

        assert!(Cms::deserialize(&blob[..blob.len() - 1]).is_none());
        assert!(Cms::deserialize(&[0u8; 15]).is_none());
    }

    #[test]
    fn malloc_used() {
        let cms = Cms::new(100, 5);
        assert_eq!(cms.malloc_used(), 100 * 5 * 8);
    }
}
