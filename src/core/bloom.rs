//! Scalable bloom filter, ported from `dragonfly/src/core/bloom.{h,cc}`.
//!
//! `Bloom` is a single bloom filter based on the design of
//! https://github.com/jvirkki/libbloom; `SBF` is a scalable bloom filter based
//! on https://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf. The SBF grows a
//! sequence of filters, each doubling the previous capacity, so the false
//! positive rate stays bounded as the key grows.
//!
//! The fingerprint hash is `XXH3_128bits` with seed `0xc6a4a7935bd1e995`
//! (the murmur2 seed), and the first/second 64 bits form the low/high parts of
//! the bit-index pair. All math mirrors the C++ bit-for-bit so capacities and
//! dump bytes match the reference.

use xxhash_rust::xxh3::xxh3_128_with_seed;

/// kSbfDumpVersion: version of the SCANDUMP wire format.
pub const K_SBF_DUMP_VERSION: u32 = 1;
/// Default false-positive probability for keys created implicitly (bloom_family.cc).
pub const K_DEFAULT_FP_PROB: f64 = 0.01;
/// Default expansion (growth) factor for keys created implicitly (bloom_family.cc).
pub const K_DEFAULT_GROW_FACTOR: f64 = 2.0;
/// version(4) + grow_factor(8).
pub const K_DUMP_HEADER_SIZE: usize = 12;
/// hash_cnt(4) + data_length(8) + fp_prob(8) + max_capacity(8) +
/// current_size(8) + prev_size(8).
pub const K_DUMP_FILTER_META_SIZE: usize = 44;
/// Maximum size of one SCANDUMP/LOADCHUNK payload.
pub const K_MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024;

/// Optimal fill ratio for an SBF slice (the paper suggests 50%).
const K_SBF_ERROR_FACTOR: f64 = 0.5;
/// The murmur2 seed used by the reference fingerprint hash.
const K_BLOOM_SEED: u64 = 0xc6a4a7935bd1e995;

fn hash_fp(str: &[u8]) -> (u64, u64) {
    let h = xxh3_128_with_seed(str, K_BLOOM_SEED);
    (h as u64, (h >> 64) as u64)
}

fn bpe(fp_prob: f64) -> f64 {
    let denom = std::f64::consts::LN_2 * std::f64::consts::LN_2;
    -fp_prob.ln() / denom
}

fn bit_index(low: u64, high: u64, i: u64, mask: u64) -> u64 {
    low.wrapping_add(high.wrapping_mul(i)) & mask
}

fn append_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn append_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// A single bloom filter. Unlike the C++ which points into external memory,
/// the Rust `Bloom` owns its bits.
#[derive(Debug, Clone, PartialEq)]
pub struct Bloom {
    hash_cnt: u8,
    bits: Vec<u8>,
}

impl Bloom {
    pub fn new() -> Self {
        Bloom { hash_cnt: 0, bits: Vec::new() }
    }

    /// Initializes a new Bloom object, mirroring `Bloom::Init(entries, fp_prob, heap)`.
    /// `entries` are silently rounded up to the minimum capacity; `fp_prob` must be
    /// in (0, 1).
    pub fn init(&mut self, entries: u64, fp_prob: f64) {
        debug_assert!(fp_prob > 0.0 && fp_prob < 1.0);
        let fp_prob = if fp_prob > 0.5 { 0.5 } else { fp_prob };
        let bpe = bpe(fp_prob);
        self.hash_cnt = (std::f64::consts::LN_2 * bpe).ceil() as u8;
        let mut bits = (entries as f64 * bpe).ceil() as u64;
        if bits < 512 {
            bits = 512;
        }
        bits = bits.next_power_of_two();
        self.bits = vec![0u8; (bits / 8) as usize];
    }

    /// Direct initializer for loading persisted filters. `len * 8` must be a
    /// power of two (enforced by the loaders), mirroring `Bloom::Init(blob, len, hash_cnt)`.
    pub fn init_direct(bits: Vec<u8>, hash_cnt: u8) -> Self {
        debug_assert!((bits.len() * 8).is_power_of_two());
        Bloom { hash_cnt, bits }
    }

    pub fn exists(&self, str: &[u8]) -> bool {
        self.exists_fp(hash_fp(str))
    }

    /// Equivalent to `Exists(str)` but accepts the two fingerprint parts.
    pub fn exists_fp(&self, fp: (u64, u64)) -> bool {
        let mask = self.mask();
        for i in 0..self.hash_cnt as u64 {
            if !self.is_set(bit_index(fp.0, fp.1, i, mask)) {
                return false;
            }
        }
        true
    }

    /// Adds an item to the bloom filter. Returns true if the element was not
    /// present and was added, false if it (or a collision) was already present.
    pub fn add(&mut self, str: &[u8]) -> bool {
        self.add_fp(hash_fp(str))
    }

    /// Equivalent to `Add(str)` but accepts the two fingerprint parts.
    pub fn add_fp(&mut self, fp: (u64, u64)) -> bool {
        let mask = self.mask();
        let mut changes = 0u32;
        for i in 0..self.hash_cnt as u64 {
            if self.set(bit_index(fp.0, fp.1, i, mask)) {
                changes += 1;
            }
        }
        changes != 0
    }

    pub fn bitlen(&self) -> usize {
        self.bits.len() * 8
    }

    /// Max element capacity for this bloom filter: floor(bit_len / bpe).
    pub fn capacity(&self, fp_prob: f64) -> usize {
        let fp_prob = if fp_prob > 0.5 { 0.5 } else { fp_prob };
        let bpe = bpe(fp_prob);
        (self.bitlen() as f64 / bpe).floor() as usize
    }

    pub fn data(&self) -> &[u8] {
        &self.bits
    }

    pub fn hash_cnt(&self) -> u8 {
        self.hash_cnt
    }

    fn mask(&self) -> u64 {
        (1u64 << self.bitlen().trailing_zeros()) - 1
    }

    fn is_set(&self, bit_idx: u64) -> bool {
        let byte_idx = bit_idx / 8;
        let bit = bit_idx % 8;
        self.bits[byte_idx as usize] & (1 << bit) != 0
    }

    /// Returns true if the bit was previously 0 and is now 1.
    fn set(&mut self, bit_idx: u64) -> bool {
        let byte_idx = bit_idx / 8;
        let bit = bit_idx % 8;
        let b = &mut self.bits[byte_idx as usize];
        let old = *b;
        *b |= 1 << bit;
        *b != old
    }
}

impl Default for Bloom {
    fn default() -> Self {
        Bloom::new()
    }
}

/// A scalable bloom filter: a sequence of `Bloom` filters from the smallest to
/// the largest, mirroring the C++ `SBF`.
#[derive(Debug, Clone, PartialEq)]
pub struct SBF {
    filters: Vec<Bloom>,
    grow_factor: f64,
    fp_prob: f64,
    prev_size: usize,
    current_size: usize,
    max_capacity: usize,
}

/// `StateUpdate` applied when a new filter is loaded (`SBF::ApplyStateUpdate`).
pub struct StateUpdate {
    pub fp_prob: f64,
    pub max_capacity: usize,
    pub current_size: usize,
    pub prev_size: usize,
}

impl SBF {
    /// Create a new scalable filter, mirroring the main `SBF` constructor.
    pub fn new(initial_capacity: u64, fp_prob: f64, grow_factor: f64) -> Self {
        let fp_prob = fp_prob * K_SBF_ERROR_FACTOR;
        let mut first = Bloom::new();
        first.init(initial_capacity, fp_prob);
        let max_capacity = first.capacity(fp_prob);
        SBF {
            filters: vec![first],
            grow_factor,
            fp_prob,
            prev_size: 0,
            current_size: 0,
            max_capacity,
        }
    }

    /// Constructor for loading persisted filters; should be followed by
    /// `add_new_filter_to_sbf`.
    pub fn new_loaded(
        grow_factor: f64,
        fp_prob: f64,
        max_capacity: usize,
        prev_size: usize,
        current_size: usize,
    ) -> Self {
        SBF {
            filters: Vec::new(),
            grow_factor,
            fp_prob,
            prev_size,
            current_size,
            max_capacity,
        }
    }

    pub fn add(&mut self, str: &[u8]) -> bool {
        debug_assert!(self.current_size < self.max_capacity);
        let fp = hash_fp(str);

        // Check all the previous filters whether the item exists.
        let last = self.filters.len().saturating_sub(1);
        if self.filters[..last].iter().any(|f| f.exists_fp(fp)) {
            return false;
        }

        if !self.filters[last].add_fp(fp) {
            return false;
        }

        self.current_size += 1;

        // Based on the paper, the optimal fill ratio for SBF is 50%. Add a new
        // slice once we reach it.
        if self.current_size >= self.max_capacity {
            self.prev_size += self.max_capacity;
            self.fp_prob *= K_SBF_ERROR_FACTOR;
            let mut nf = Bloom::new();
            nf.init((self.max_capacity as f64 * self.grow_factor) as u64, self.fp_prob);
            self.filters.push(nf);
            self.current_size = 0;
            self.max_capacity = self.filters[self.filters.len() - 1].capacity(self.fp_prob);
        }

        true
    }

    pub fn exists(&self, str: &[u8]) -> bool {
        let fp = hash_fp(str);
        self.filters.iter().any(|f| f.exists_fp(fp))
    }

    pub fn current_size(&self) -> usize {
        self.current_size
    }

    pub fn prev_size(&self) -> usize {
        self.prev_size
    }

    pub fn grow_factor(&self) -> f64 {
        self.grow_factor
    }

    /// Expected fp probability for the current filter.
    pub fn fp_probability(&self) -> f64 {
        self.fp_prob
    }

    pub fn num_filters(&self) -> usize {
        self.filters.len()
    }

    pub fn data(&self, idx: usize) -> &[u8] {
        self.filters[idx].data()
    }

    pub fn hashfunc_cnt(&self, idx: usize) -> u8 {
        self.filters[idx].hash_cnt()
    }

    /// Max capacity of the current filter.
    pub fn max_capacity(&self) -> usize {
        self.max_capacity
    }

    /// Total design capacity across all filters (completed filters plus the
    /// current one).
    pub fn total_capacity(&self) -> usize {
        self.prev_size + self.max_capacity
    }

    /// Total number of items inserted across all filters.
    pub fn total_items(&self) -> usize {
        self.prev_size + self.current_size
    }

    pub fn malloc_used(&self) -> usize {
        let mut res = std::mem::size_of::<SBF>() + std::mem::size_of::<Bloom>() * self.filters.capacity();
        for f in &self.filters {
            res += f.bitlen() / 8;
        }
        res
    }

    pub fn apply_state_update(&mut self, update: &StateUpdate) {
        self.fp_prob = update.fp_prob;
        self.max_capacity = update.max_capacity;
        self.current_size = update.current_size;
        self.prev_size = update.prev_size;
    }

    /// Serialize the whole filter as a single blob in the SCANDUMP wire format
    /// (header, then every filter's meta + bytes). Used by the RDB DUMP path;
    /// the chunked SCANDUMP view is produced by `SbfDumpIterator`.
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        append_u32(&mut out, K_SBF_DUMP_VERSION);
        append_u64(&mut out, self.grow_factor.to_bits());
        for i in 0..self.num_filters() {
            append_u32(&mut out, self.hashfunc_cnt(i) as u32);
            append_u64(&mut out, self.data(i).len() as u64);
            append_u64(&mut out, self.fp_probability().to_bits());
            append_u64(&mut out, self.max_capacity() as u64);
            append_u64(&mut out, self.current_size() as u64);
            append_u64(&mut out, self.prev_size() as u64);
            out.extend_from_slice(self.data(i));
        }
        out
    }

    /// Deserialize a blob written by `serialize`, applying each filter block in
    /// order (mirroring a LOADCHUNK sequence).
    pub fn deserialize(bytes: &[u8]) -> Result<SBF, SbfLoadError> {
        if bytes.len() < K_DUMP_HEADER_SIZE {
            return Err(SbfLoadError::TruncatedInput);
        }
        let mut sbf = load_sbf_header(&bytes[..K_DUMP_HEADER_SIZE])?;
        let mut rest = &bytes[K_DUMP_HEADER_SIZE..];
        while !rest.is_empty() {
            if rest.len() < K_DUMP_FILTER_META_SIZE {
                return Err(SbfLoadError::TruncatedInput);
            }
            let data_length = read_u64(&rest[4..12]);
            if data_length == 0 || !data_length.is_power_of_two() {
                return Err(SbfLoadError::BadInput);
            }
            let block_len = K_DUMP_FILTER_META_SIZE + data_length as usize;
            if rest.len() < block_len {
                return Err(SbfLoadError::TruncatedInput);
            }
            add_new_filter_to_sbf(&rest[..block_len], &mut sbf)?;
            rest = &rest[block_len..];
        }
        Ok(sbf)
    }
}

/// A pair returned to a client by BF.SCANDUMP, mirroring `SBFChunk`.
pub struct SbfChunk {
    /// 1: `data` is the SBF header; >1: filter data; 0: iteration finished.
    pub cursor: i64,
    pub data: Vec<u8>,
}

/// Allows sending the contents of an SBF in chunks of at most 16MiB, mirroring
/// `SBFDumpIterator`.
pub struct SbfDumpIterator<'a> {
    sbf: &'a SBF,
    cursor: i64,
    filter_index: usize,
    byte_offset: usize,
}

impl<'a> SbfDumpIterator<'a> {
    /// `cursor` is the client-supplied cursor; 0 starts from the beginning.
    pub fn new(sbf: &'a SBF, cursor: i64) -> Self {
        let mut it = SbfDumpIterator { sbf, cursor, filter_index: 0, byte_offset: 0 };
        it.resolve_cursor_to_pos();
        it
    }

    /// Converts a cursor to the specific filter and offset inside it, O(n) in
    /// the number of filters (`ResolveCursorToPos`).
    fn resolve_cursor_to_pos(&mut self) {
        if self.cursor == 0 {
            self.filter_index = 0;
            self.byte_offset = 0;
            return;
        }
        let mut global_offset = (self.cursor - 1) as usize;
        for i in 0..self.sbf.num_filters() {
            let filter_span = K_DUMP_FILTER_META_SIZE + self.sbf.data(i).len();
            if global_offset < filter_span {
                self.filter_index = i;
                self.byte_offset = global_offset;
                return;
            }
            global_offset -= filter_span;
        }
        self.filter_index = self.sbf.num_filters();
        self.byte_offset = 0;
    }

    fn serialize_header(&self) -> Vec<u8> {
        let mut out = Vec::new();
        append_u32(&mut out, K_SBF_DUMP_VERSION);
        append_u64(&mut out, self.sbf.grow_factor().to_bits());
        out
    }

    /// Filter metadata must always be fully contained in one chunk.
    fn build_filter_header(&self, filter_data: &[u8]) -> Vec<u8> {
        let data_chunk_len = (K_MAX_CHUNK_SIZE - K_DUMP_FILTER_META_SIZE).min(filter_data.len());
        let mut chunk = Vec::with_capacity(K_DUMP_FILTER_META_SIZE + data_chunk_len);
        append_u32(&mut chunk, self.sbf.hashfunc_cnt(self.filter_index) as u32);
        append_u64(&mut chunk, filter_data.len() as u64);
        append_u64(&mut chunk, self.sbf.fp_probability().to_bits());
        append_u64(&mut chunk, self.sbf.max_capacity() as u64);
        append_u64(&mut chunk, self.sbf.current_size() as u64);
        append_u64(&mut chunk, self.sbf.prev_size() as u64);
        chunk.extend_from_slice(&filter_data[..data_chunk_len]);
        chunk
    }

    fn build_filter_continuation(&self, filter_data: &[u8]) -> Vec<u8> {
        let data_offset = self.byte_offset - K_DUMP_FILTER_META_SIZE;
        let remaining = filter_data.len() - data_offset;
        let chunk_len = K_MAX_CHUNK_SIZE.min(remaining);
        filter_data[data_offset..data_offset + chunk_len].to_vec()
    }

    /// Returns `(next cursor, data between the current and next cursor)`. Once
    /// the filter is fully read returns `(0, "")`.
    pub fn next_chunk(&mut self) -> SbfChunk {
        if self.cursor == 0 {
            self.cursor = 1;
            return SbfChunk { cursor: 1, data: self.serialize_header() };
        }

        if self.filter_index < self.sbf.num_filters() {
            let filter_data = self.sbf.data(self.filter_index);
            let chunk;
            if self.byte_offset == 0 {
                // First chunk of this filter: metadata followed by filter data.
                chunk = self.build_filter_header(filter_data);
                self.byte_offset = chunk.len();
                self.cursor += chunk.len() as i64;
            } else {
                if self.byte_offset < K_DUMP_FILTER_META_SIZE {
                    return SbfChunk { cursor: 0, data: Vec::new() };
                }
                // Continuing data for the current filter.
                chunk = self.build_filter_continuation(filter_data);
                self.byte_offset += chunk.len();
                self.cursor += chunk.len() as i64;
            }

            // Advance to the next filter if this one is complete.
            debug_assert!(self.byte_offset <= K_DUMP_FILTER_META_SIZE + filter_data.len());
            if self.byte_offset == K_DUMP_FILTER_META_SIZE + filter_data.len() {
                self.filter_index += 1;
                self.byte_offset = 0;
            }

            return SbfChunk { cursor: self.cursor, data: chunk };
        }

        SbfChunk { cursor: 0, data: Vec::new() }
    }
}

/// Load errors, mirroring the C++ `SBFLoadResult`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SbfLoadError {
    BadVersion,
    BadInput,
    TruncatedInput,
    OutOfRange,
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes[..4].try_into().expect("4 bytes"))
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes"))
}

/// Creates an SBF from a dump header chunk (the chunk returned with cursor 1),
/// mirroring `LoadSBFHeader`.
pub fn load_sbf_header(header_data: &[u8]) -> Result<SBF, SbfLoadError> {
    if header_data.len() < K_DUMP_HEADER_SIZE {
        return Err(SbfLoadError::TruncatedInput);
    }
    if header_data.len() > K_DUMP_HEADER_SIZE {
        return Err(SbfLoadError::BadInput);
    }
    let version = read_u32(header_data);
    if version != K_SBF_DUMP_VERSION {
        return Err(SbfLoadError::BadVersion);
    }
    let grow_factor = f64::from_bits(read_u64(&header_data[4..]));
    if !grow_factor.is_finite() || grow_factor < 1.0 {
        return Err(SbfLoadError::BadInput);
    }
    // Initialize everything to 0, later filters overwrite these values.
    Ok(SBF::new_loaded(grow_factor, 0.0, 0, 0, 0))
}

fn add_new_filter_to_sbf(data: &[u8], sbf: &mut SBF) -> Result<(), SbfLoadError> {
    if data.len() < K_DUMP_FILTER_META_SIZE {
        return Err(SbfLoadError::TruncatedInput);
    }

    let hash_cnt = read_u32(data);
    if hash_cnt == 0 || hash_cnt > u8::MAX as u32 {
        return Err(SbfLoadError::BadInput);
    }

    let data_length = read_u64(&data[4..12]);
    if data_length == 0 || !data_length.is_power_of_two() {
        return Err(SbfLoadError::BadInput);
    }

    let fp_prob = f64::from_bits(read_u64(&data[12..20]));
    if !fp_prob.is_finite() || fp_prob <= 0.0 || fp_prob >= 1.0 {
        return Err(SbfLoadError::BadInput);
    }

    let max_capacity = read_u64(&data[20..28]) as usize;
    let current_size = read_u64(&data[28..36]) as usize;
    let prev_size = read_u64(&data[36..44]) as usize;
    if max_capacity == 0 || current_size >= max_capacity {
        return Err(SbfLoadError::BadInput);
    }

    let payload = data.len() - K_DUMP_FILTER_META_SIZE;
    if payload as u64 > data_length {
        return Err(SbfLoadError::OutOfRange);
    }

    sbf.apply_state_update(&StateUpdate { fp_prob, max_capacity, current_size, prev_size });

    let mut bits = vec![0u8; data_length as usize];
    if payload > 0 {
        bits[..payload].copy_from_slice(&data[K_DUMP_FILTER_META_SIZE..]);
    }
    sbf.filters.push(Bloom::init_direct(bits, hash_cnt as u8));

    Ok(())
}

/// Loads a data chunk into an existing SBF, mirroring `LoadSBFChunk`.
pub fn load_sbf_chunk(cursor: i64, data: &[u8], sbf: &mut SBF) -> Result<(), SbfLoadError> {
    let write_pos = cursor - data.len() as i64;
    if write_pos < 1 {
        return Err(SbfLoadError::OutOfRange);
    }

    let mut global_offset = (write_pos - 1) as usize;
    for i in 0..sbf.num_filters() {
        let filter_span = K_DUMP_FILTER_META_SIZE + sbf.data(i).len();
        if global_offset < filter_span {
            // We should never have a write position inside the header; the
            // header is always fully written.
            if global_offset < K_DUMP_FILTER_META_SIZE {
                return Err(SbfLoadError::OutOfRange);
            }
            let data_offset = global_offset - K_DUMP_FILTER_META_SIZE;
            if data_offset + data.len() > sbf.data(i).len() {
                return Err(SbfLoadError::OutOfRange);
            }
            sbf.filters[i].bits[data_offset..data_offset + data.len()].copy_from_slice(data);
            return Ok(());
        }
        global_offset -= filter_span;
    }

    if global_offset != 0 {
        return Err(SbfLoadError::OutOfRange);
    }

    // Global offset is 0, i.e. we ended exactly at the end of the filter; data
    // goes into a new filter.
    add_new_filter_to_sbf(data, sbf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_matches_reference() {
        // reserve b1 0.01 1000 -> capacity 1485 (see Info test in
        // bloom_family_test.cc).
        let sbf = SBF::new(1000, 0.01, 2.0);
        assert_eq!(sbf.max_capacity(), 1485);
        assert_eq!(sbf.num_filters(), 1);
        assert_eq!(sbf.fp_probability(), 0.005);
        assert_eq!(sbf.total_items(), 0);

        // reserve b1 0.1 32.
        let sbf = SBF::new(32, 0.1, 2.0);
        assert_eq!(sbf.max_capacity(), 82);
        assert_eq!(sbf.filters[0].bitlen(), 512);
        assert_eq!(sbf.filters[0].hash_cnt(), 5);
    }

    #[test]
    fn add_exists() {
        let mut sbf = SBF::new(10, 0.01, 2.0);
        assert!(sbf.add(b"a"));
        assert!(sbf.add(b"b"));
        assert!(!sbf.add(b"a"));
        assert!(sbf.exists(b"a"));
        assert!(sbf.exists(b"b"));
        assert!(!sbf.exists(b"c"));
    }

    #[test]
    fn grows_past_capacity() {
        // Capacity 1485; pushing 2000 items must grow a second filter and keep
        // every inserted item reported as present. `add` may return false for
        // items whose fingerprint collides in a previous (full) filter, so only
        // `exists` is asserted.
        let mut sbf = SBF::new(1000, 0.01, 2.0);
        for i in 0..2000u32 {
            sbf.add(format!("item{i}").as_bytes());
        }
        assert_eq!(sbf.num_filters(), 2);
        assert_eq!(sbf.prev_size(), 1485);
        assert!(sbf.total_items() >= 1485);
        for i in 0..2000u32 {
            assert!(sbf.exists(format!("item{i}").as_bytes()));
        }
    }

    #[test]
    fn scandump_round_trip_chunks() {
        let mut sbf = SBF::new(1000, 0.01, 2.0);
        for i in 0..100u32 {
            sbf.add(format!("item{i}").as_bytes());
        }

        // Walk the chunks exactly like the reference ScanDump test.
        let mut cursor = 0i64;
        let mut chunks: Vec<(i64, Vec<u8>)> = Vec::new();
        loop {
            let mut it = SbfDumpIterator::new(&sbf, cursor);
            let chunk = it.next_chunk();
            assert!(chunk.cursor > cursor || chunk.cursor == 0);
            cursor = chunk.cursor;
            if cursor == 0 {
                assert!(chunk.data.is_empty());
                break;
            }
            assert!(!chunk.data.is_empty());
            chunks.push((chunk.cursor, chunk.data));
        }
        assert!(chunks.len() >= 2, "header + filter chunk");

        // Load into a fresh SBF and verify every item exists.
        let mut loaded = SBF::new_loaded(2.0, 0.0, 0, 0, 0);
        for (cursor, data) in chunks {
            if cursor == 1 {
                let fresh = load_sbf_header(&data).expect("valid header");
                loaded = fresh;
            } else {
                load_sbf_chunk(cursor, &data, &mut loaded).expect("valid chunk");
            }
        }
        assert_eq!(loaded.num_filters(), 1);
        for i in 0..100u32 {
            assert!(loaded.exists(format!("item{i}").as_bytes()));
        }
        // State must match the original.
        assert_eq!(loaded.fp_probability(), sbf.fp_probability());
        assert_eq!(loaded.current_size(), sbf.current_size());
        assert_eq!(loaded.max_capacity(), sbf.max_capacity());
        assert_eq!(loaded.prev_size(), sbf.prev_size());
        assert_eq!(loaded.grow_factor(), sbf.grow_factor());
    }

    #[test]
    fn serialize_deserialize() {
        let mut sbf = SBF::new(1000, 0.01, 2.0);
        for i in 0..2000u32 {
            sbf.add(format!("item{i}").as_bytes());
        }
        let bytes = sbf.serialize();
        let loaded = SBF::deserialize(&bytes).expect("valid blob");
        assert_eq!(loaded.num_filters(), sbf.num_filters());
        for i in 0..2000u32 {
            assert!(loaded.exists(format!("item{i}").as_bytes()));
        }
        assert_eq!(loaded.fp_probability(), sbf.fp_probability());
        assert_eq!(loaded.max_capacity(), sbf.max_capacity());
        assert_eq!(loaded.current_size(), sbf.current_size());
        assert_eq!(loaded.prev_size(), sbf.prev_size());
    }

    #[test]
    fn load_chunk_errors() {
        let mut sbf = SBF::new_loaded(2.0, 0.0, 0, 0, 0);
        assert_eq!(load_sbf_header(b"x"), Err(SbfLoadError::TruncatedInput));
        let mut bad = vec![0u8; K_DUMP_HEADER_SIZE];
        assert_eq!(load_sbf_header(&bad), Err(SbfLoadError::BadVersion));
        bad[..4].copy_from_slice(&1u32.to_le_bytes());
        bad[4..].copy_from_slice(&1.0f64.to_bits().to_le_bytes());
        assert!(load_sbf_header(&bad).is_ok());
        assert_eq!(load_sbf_chunk(1, b"data", &mut sbf), Err(SbfLoadError::OutOfRange));
    }
}
