//! `HyperLogLog` probabilistic cardinality estimation.
//!
//! Port of `dragonfly/src/redis/hyperloglog.c` (itself a fork of Valkey's
//! `src/hyperloglog.c`). Dragonfly only stores the dense encoding; sparse
//! values may be imported (replication, foreign imports) and are converted to
//! dense on first use. We keep the sparse encoder (`pfadd_sparse`) as well so
//! newly created keys start sparse, exactly like the reference.
//!
//! Buffer conventions (see `HllBufferPtr` in the reference header):
//! * A sparse HLL is a `Vec<u8>` of exactly its encoded length.
//! * A dense HLL is a `Vec<u8>` of `HLL_DENSE_SIZE + 1` bytes: the extra byte
//!   is the "slack" that `HLL_DENSE_GET/SET_REGISTER` relies on, since register
//!   16383 straddles the last register byte. Stored values are exactly
//!   `HLL_DENSE_SIZE` bytes; the command layer copies them into the slack form
//!   for mutation and strips the slack when writing back.
//! * Read-only kernels (`pfcount_multi`, `pfmerge`) accept stored dense values
//!   directly: the register reader treats a missing final byte as 0, which is
//!   exactly what the C terminator byte provides.

pub const HLL_P: u32 = 14;
pub const HLL_Q: u32 = 64 - HLL_P;
pub const HLL_REGISTERS: usize = 1 << HLL_P;
pub const HLL_P_MASK: u64 = (HLL_REGISTERS - 1) as u64;
pub const HLL_BITS: u32 = 6;
pub const HLL_REGISTER_MAX: u8 = (1 << HLL_BITS) - 1;
pub const HLL_HDR_SIZE: usize = 16;
pub const HLL_DENSE_SIZE: usize = HLL_HDR_SIZE + (HLL_REGISTERS * HLL_BITS as usize).div_ceil(8);
pub const HLL_DENSE: u8 = 0;
pub const HLL_SPARSE: u8 = 1;
pub const HLL_MAX_ENCODING: u8 = 1;

pub const HLL_SPARSE_MAX_BYTES: usize = 3000;
pub const HLL_SPARSE_VAL_MAX_VALUE: u8 = 32;
pub const HLL_SPARSE_VAL_MAX_LEN: usize = 4;
pub const HLL_SPARSE_ZERO_MAX_LEN: usize = 64;
pub const HLL_SPARSE_XZERO_MAX_LEN: usize = 16384;

const HLL_ALPHA_INF: f64 = 0.721_347_520_444_481_7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HllValidness {
    Invalid,
    ValidSparse,
    ValidDense,
}

#[must_use]
pub fn get_dense_hll_size() -> usize {
    HLL_DENSE_SIZE
}

#[must_use]
pub fn get_sparse_hll_init_size() -> usize {
    HLL_HDR_SIZE + HLL_REGISTERS.div_ceil(HLL_SPARSE_XZERO_MAX_LEN) * 2
}

// ---------------------------------------------------------------------------
// Header helpers
// ---------------------------------------------------------------------------

const HLL_MAGIC: &[u8; 4] = b"HYLL";

#[inline]
fn hll_card_valid(buf: &[u8]) -> bool {
    (buf[15] & 0x80) == 0
}

#[inline]
fn hll_read_cached_card(buf: &[u8]) -> u64 {
    let mut card = 0u64;
    for i in 0..8 {
        card |= u64::from(buf[8 + i]) << (8 * i);
    }
    card
}

#[inline]
fn hll_invalidate_cache(buf: &mut [u8]) {
    buf[15] |= 0x80;
}

#[inline]
fn hll_write_cached_card(buf: &mut [u8], card: u64) {
    for i in 0..8 {
        buf[8 + i] = (card >> (8 * i)) as u8;
    }
}

/// Cached cardinality, or `None` when the cache is invalid.
fn hll_cached_card(buf: &[u8]) -> Option<u64> {
    if hll_card_valid(buf) {
        Some(hll_read_cached_card(buf))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Validity
// ---------------------------------------------------------------------------

#[must_use]
pub fn is_valid_hll(buf: &[u8]) -> HllValidness {
    if buf.len() < HLL_HDR_SIZE {
        return HllValidness::Invalid;
    }
    if &buf[0..4] != HLL_MAGIC {
        return HllValidness::Invalid;
    }
    let encoding = buf[4];
    if encoding > HLL_MAX_ENCODING {
        return HllValidness::Invalid;
    }
    match encoding {
        HLL_DENSE => {
            if buf.len() == HLL_DENSE_SIZE {
                HllValidness::ValidDense
            } else {
                HllValidness::Invalid
            }
        }
        HLL_SPARSE => HllValidness::ValidSparse,
        _ => HllValidness::Invalid,
    }
}

// ---------------------------------------------------------------------------
// Dense representation
// ---------------------------------------------------------------------------

/// Read a 6-bit register. `registers` may have either exactly
/// `HLL_REGISTERS * 6 / 8` bytes (a stored value) or one more byte of slack;
/// the final register's second byte is the C terminator / slack, and
/// contributes no bits, so a missing one reads as 0.
#[inline]
fn dense_get_register(registers: &[u8], regnum: usize) -> u8 {
    let byte = regnum * HLL_BITS as usize / 8;
    let fb = (regnum * HLL_BITS as usize) & 7;
    let fb8 = 8 - fb;
    let b0 = u64::from(registers[byte]);
    let b1 = u64::from(registers.get(byte + 1).copied().unwrap_or(0));
    ((b0 >> fb) | (b1 << fb8)) as u8 & HLL_REGISTER_MAX
}

/// Write a 6-bit register. `registers` must have one byte of slack past the
/// register array (i.e. at least `HLL_REGISTERS * 6 / 8 + 1` bytes), since the
/// final register's write touches that byte.
#[inline]
fn dense_set_register(registers: &mut [u8], regnum: usize, val: u8) {
    let byte = regnum * HLL_BITS as usize / 8;
    let fb = (regnum * HLL_BITS as usize) & 7;
    let fb8 = 8 - fb;
    let v = u64::from(val);
    registers[byte] =
        (registers[byte] & !(u64::from(HLL_REGISTER_MAX) << fb) as u8) | ((v << fb) as u8);
    registers[byte + 1] =
        (registers[byte + 1] & !(u64::from(HLL_REGISTER_MAX) >> fb8) as u8) | ((v >> fb8) as u8);
}

/// Set the register at `index` to `count` if `count` is larger.
fn hll_dense_set(registers: &mut [u8], index: usize, count: u8) -> bool {
    let oldcount = dense_get_register(registers, index);
    if count > oldcount {
        dense_set_register(registers, index, count);
        true
    } else {
        false
    }
}

/// Register histogram of a dense register array.
fn hll_dense_reg_histo(registers: &[u8], reghisto: &mut [u32; 64]) {
    for j in 0..HLL_REGISTERS {
        reghisto[dense_get_register(registers, j) as usize] += 1;
    }
}

/// Register histogram of a raw (one byte per register) array.
fn hll_raw_reg_histo(registers: &[u8], reghisto: &mut [u32; 64]) {
    for &r in registers {
        reghisto[r as usize] += 1;
    }
}

/// Merge dense-encoded registers into a raw (max) register array.
fn hll_merge_dense(reg_raw: &mut [u8], reg_dense: &[u8]) {
    for (i, reg) in reg_raw.iter_mut().enumerate() {
        let val = dense_get_register(reg_dense, i);
        if val > *reg {
            *reg = val;
        }
    }
}

/// Compress a raw (max) register array into dense-encoded registers.
fn hll_dense_compress(reg_dense: &mut [u8], reg_raw: &[u8]) {
    for (i, &val) in reg_raw.iter().enumerate() {
        dense_set_register(reg_dense, i, val);
    }
}

// ---------------------------------------------------------------------------
// Hashing
// ---------------------------------------------------------------------------

/// Endian-neutral `MurmurHash64A`, matching `MurmurHash64A` in hyperloglog.c.
#[must_use]
pub fn murmur_hash64_a(key: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;
    let len = key.len();
    let mut h = seed ^ ((len as u64).wrapping_mul(M));
    let end = len - (len & 7);
    let mut data = &key[..end];
    while !data.is_empty() {
        let mut k = u64::from_le_bytes(data[..8].try_into().unwrap());
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
        data = &data[8..];
    }
    // The C code's switch falls through every case to `case 1`, so a length of
    // n appends bytes 0..n as a little-endian value and then multiplies.
    if len & 7 != 0 {
        for (i, &b) in key[end..].iter().enumerate() {
            h ^= u64::from(b) << (8 * i);
        }
        h = h.wrapping_mul(M);
    }
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

/// Pattern length (run of zeros + 1) and register index for an element.
fn hll_pat_len(ele: &[u8], regp: &mut usize) -> u8 {
    let mut hash = murmur_hash64_a(ele, 0xadc8_3b19);
    let index = (hash & HLL_P_MASK) as usize;
    hash >>= HLL_P;
    hash |= 1u64 << HLL_Q; // ensure count <= Q+1, so ctz is defined
    let count = 1 + hash.trailing_zeros() as u8;
    *regp = index;
    count
}

// ---------------------------------------------------------------------------
// Sparse representation
// ---------------------------------------------------------------------------

#[inline]
fn sparse_is_zero(b: u8) -> bool {
    (b & 0xc0) == 0
}

#[inline]
fn sparse_is_xzero(b: u8) -> bool {
    (b & 0xc0) == 0x40
}

#[inline]
fn sparse_is_val(b: u8) -> bool {
    (b & 0x80) != 0
}

#[inline]
fn sparse_zero_len(b: u8) -> usize {
    ((b & 0x3f) + 1) as usize
}

#[inline]
fn sparse_xzero_len(b: u8, b1: u8) -> usize {
    ((((b & 0x3f) as usize) << 8) | b1 as usize) + 1
}

#[inline]
fn sparse_val_value(b: u8) -> u8 {
    ((b >> 2) & 0x1f) + 1
}

#[inline]
fn sparse_val_len(b: u8) -> usize {
    ((b & 0x3) + 1) as usize
}

#[inline]
fn sparse_val_set(val: u8, len: usize) -> u8 {
    ((val - 1) << 2) | ((len - 1) as u8) | 0x80
}

#[inline]
fn sparse_zero_set(len: usize) -> u8 {
    (len - 1) as u8
}

#[inline]
fn sparse_xzero_set(len: usize) -> [u8; 2] {
    let l = (len - 1) as u16;
    [((l >> 8) as u8) | 0x40, (l & 0xff) as u8]
}

/// Decode a sparse HLL into a dense register array. Returns `false` when the
/// sparse encoding is corrupt: any run that overflows the register space is
/// rejected (CVE-2025-32023), and the opcodes must cover exactly
/// `HLL_REGISTERS` registers.
fn sparse_to_dense_registers(in_hll: &[u8], registers: &mut [u8]) -> bool {
    let mut p = HLL_HDR_SIZE;
    let end = in_hll.len();
    let mut idx = 0usize;
    let mut valid = true;
    while p < end {
        let b = in_hll[p];
        if sparse_is_zero(b) {
            let runlen = sparse_zero_len(b);
            if runlen + idx > HLL_REGISTERS {
                valid = false;
                break;
            }
            idx += runlen;
            p += 1;
        } else if sparse_is_xzero(b) {
            let b1 = in_hll.get(p + 1).copied().unwrap_or(0);
            let runlen = sparse_xzero_len(b, b1);
            if runlen + idx > HLL_REGISTERS {
                valid = false;
                break;
            }
            idx += runlen;
            p += 2;
        } else {
            let runlen = sparse_val_len(b);
            let regval = sparse_val_value(b);
            if runlen + idx > HLL_REGISTERS {
                valid = false;
                break;
            }
            for _ in 0..runlen {
                dense_set_register(registers, idx, regval);
                idx += 1;
            }
            p += 1;
        }
    }
    valid && idx == HLL_REGISTERS
}

/// Convert a sparse HLL to a dense HLL buffer (with slack). Returns `None`
/// when the input is not a valid sparse HLL.
#[must_use]
pub fn sparse_to_dense(in_hll: &[u8]) -> Option<Vec<u8>> {
    if in_hll.len() < HLL_HDR_SIZE || in_hll[4] != HLL_SPARSE {
        return None;
    }
    let mut dense = vec![0u8; HLL_DENSE_SIZE + 1];
    dense[0..HLL_HDR_SIZE].copy_from_slice(&in_hll[0..HLL_HDR_SIZE]);
    dense[4] = HLL_DENSE;
    if !sparse_to_dense_registers(in_hll, &mut dense[16..]) {
        return None;
    }
    Some(dense)
}

/// Convert a sparse HLL into the dense representation, writing `out` (exactly
/// `HLL_DENSE_SIZE` bytes). Returns `false` on failure, mirroring
/// `convertSparseToDenseHll`.
pub fn convert_sparse_to_dense_hll(in_hll: &[u8], out: &mut [u8]) -> bool {
    if out.len() != HLL_DENSE_SIZE {
        return false;
    }
    match sparse_to_dense(in_hll) {
        Some(dense) => {
            out.copy_from_slice(&dense[..HLL_DENSE_SIZE]);
            true
        }
        None => false,
    }
}

/// Initialize a sparse HLL buffer of exactly `get_sparse_hll_init_size()`
/// bytes. Returns `false` on wrong size.
pub fn init_sparse_hll(buf: &mut [u8]) -> bool {
    if buf.len() != get_sparse_hll_init_size() {
        return false;
    }
    buf.fill(0);
    buf[0..4].copy_from_slice(HLL_MAGIC);
    buf[4] = HLL_SPARSE;
    let mut aux = HLL_REGISTERS;
    let mut p = HLL_HDR_SIZE;
    while aux > 0 {
        let xzero = HLL_SPARSE_XZERO_MAX_LEN.min(aux);
        let [b0, b1] = sparse_xzero_set(xzero);
        buf[p] = b0;
        buf[p + 1] = b1;
        p += 2;
        aux -= xzero;
    }
    true
}

/// Create a dense HLL buffer (with slack) with all registers zero.
#[must_use]
pub fn create_dense_hll() -> Vec<u8> {
    let mut buf = vec![0u8; HLL_DENSE_SIZE + 1];
    buf[0..4].copy_from_slice(HLL_MAGIC);
    buf[4] = HLL_DENSE;
    buf
}

/// Copy a stored dense HLL into the slack form used for mutation.
#[must_use]
pub fn dense_with_slack(stored: &[u8]) -> Option<Vec<u8>> {
    if stored.len() != HLL_DENSE_SIZE {
        return None;
    }
    let mut v = stored.to_vec();
    v.push(0);
    Some(v)
}

/// Strip the slack byte from a dense HLL buffer for storage.
#[must_use]
pub fn strip_dense_slack(mut v: Vec<u8>) -> Vec<u8> {
    v.truncate(HLL_DENSE_SIZE);
    v
}

/// Low-level sparse set, port of `hllSparseSet` in hyperloglog.c. May replace
/// `hll` with a dense buffer (sets `promoted`); returns 1 if the register was
/// updated, 0 otherwise, and -1 on a corrupt representation.
fn hll_sparse_set(hll: &mut Vec<u8>, index: usize, count: u8, promoted: &mut bool) -> i32 {
    // If the count is too big to be representable by the sparse representation
    // switch to dense representation.
    if count > HLL_SPARSE_VAL_MAX_VALUE {
        return hll_sparse_promote(hll, index, count, promoted);
    }

    // Greedy buffer growth, mirroring the sdsResize step: ensure room for the
    // worst-case +3 byte insert (XZERO split into XZERO-VAL-XZERO) without
    // exceeding HLL_SPARSE_MAX_BYTES.
    if hll.capacity() < HLL_SPARSE_MAX_BYTES && hll.capacity().saturating_sub(hll.len()) < 3 {
        let mut newlen = hll.len() + 3;
        newlen += newlen.min(300);
        if newlen > HLL_SPARSE_MAX_BYTES {
            newlen = HLL_SPARSE_MAX_BYTES;
        }
        hll.reserve(newlen.saturating_sub(hll.len()));
    }

    // Step 1: locate the opcode covering `index`.
    let mut p = HLL_HDR_SIZE;
    let mut first = 0usize;
    let mut prev: Option<usize> = None;
    let mut span = 0usize;
    let len = hll.len();
    while p < len {
        let mut oplen = 1;
        let b = hll[p];
        if sparse_is_zero(b) {
            span = sparse_zero_len(b);
        } else if sparse_is_val(b) {
            span = sparse_val_len(b);
        } else {
            let b1 = hll.get(p + 1).copied().unwrap_or(0);
            span = sparse_xzero_len(b, b1);
            oplen = 2;
        }
        if index < first + span {
            break;
        }
        prev = Some(p);
        p += oplen;
        first += span;
    }
    if span == 0 || p >= len {
        return -1; // invalid format
    }

    let is_zero = sparse_is_zero(hll[p]);
    let is_xzero = sparse_is_xzero(hll[p]);
    let is_val = sparse_is_val(hll[p]);
    let runlen = if is_zero {
        sparse_zero_len(hll[p])
    } else if is_xzero {
        sparse_xzero_len(hll[p], hll.get(p + 1).copied().unwrap_or(0))
    } else {
        sparse_val_len(hll[p])
    };
    let mut next = p + if is_xzero { 2 } else { 1 };
    if next >= len {
        next = usize::MAX;
    }

    // Step 2: the located opcode.
    if is_val {
        let oldcount = sparse_val_value(hll[p]);
        // Case A: already a value >= count, nothing to do.
        if oldcount >= count {
            return 0;
        }
        // Case B: VAL with run length 1, trivial update.
        if runlen == 1 {
            hll[p] = sparse_val_set(count, 1);
            return hll_sparse_merge_adjacent(hll, prev);
        }
    }
    // Case C: ZERO with run length 1, replace with VAL.
    if is_zero && runlen == 1 {
        hll[p] = sparse_val_set(count, 1);
        return hll_sparse_merge_adjacent(hll, prev);
    }

    // Case D: split the opcode into up to three: [ZERO/XZERO|VAL|ZERO/XZERO]
    // or [VAL|VAL|VAL]. Worst case XZERO -> XZERO-VAL-XZERO (5 bytes).
    let mut seq = [0u8; 5];
    let mut n = 0usize;
    let last = first + span - 1;
    if is_zero || is_xzero {
        if index != first {
            let len = index - first;
            if len > HLL_SPARSE_ZERO_MAX_LEN {
                seq[n..n + 2].copy_from_slice(&sparse_xzero_set(len));
                n += 2;
            } else {
                seq[n] = sparse_zero_set(len);
                n += 1;
            }
        }
        seq[n] = sparse_val_set(count, 1);
        n += 1;
        if index != last {
            let len = last - index;
            if len > HLL_SPARSE_ZERO_MAX_LEN {
                seq[n..n + 2].copy_from_slice(&sparse_xzero_set(len));
                n += 2;
            } else {
                seq[n] = sparse_zero_set(len);
                n += 1;
            }
        }
    } else {
        // Split a VAL opcode: keep the surrounding runs of the old value.
        let curval = sparse_val_value(hll[p]);
        if index != first {
            let len = index - first;
            seq[n] = sparse_val_set(curval, len);
            n += 1;
        }
        seq[n] = sparse_val_set(count, 1);
        n += 1;
        if index != last {
            let len = last - index;
            seq[n] = sparse_val_set(curval, len);
            n += 1;
        }
    }

    // Step 3: substitute the new sequence for the old opcode.
    let seqlen = n;
    let oldlen = if is_xzero { 2 } else { 1 };
    let deltalen = seqlen as isize - oldlen as isize;
    if deltalen > 0 && hll.len() + deltalen as usize > HLL_SPARSE_MAX_BYTES {
        return hll_sparse_promote(hll, index, count, promoted);
    }

    if deltalen != 0 && next != usize::MAX {
        // Move the tail [next, len) to [next+deltalen, len+deltalen).
        if deltalen > 0 {
            hll.resize(hll.len() + deltalen as usize, 0);
            for i in (next..len).rev() {
                hll[i + deltalen as usize] = hll[i];
            }
        } else {
            let shift = (-deltalen) as usize;
            for i in next..len {
                hll[i - shift] = hll[i];
            }
            hll.resize(hll.len() + deltalen as usize, 0);
        }
    } else if deltalen != 0 {
        hll.resize(hll.len() + deltalen as usize, 0);
    }
    hll[p..p + seqlen].copy_from_slice(&seq[..seqlen]);

    hll_sparse_merge_adjacent(hll, prev)
}

/// Step 4 of hllSparseSet: merge adjacent equal VAL opcodes.
fn hll_sparse_merge_adjacent(hll: &mut Vec<u8>, prev: Option<usize>) -> i32 {
    let sparse = HLL_HDR_SIZE;
    let mut p = prev.unwrap_or(sparse);
    let mut end = hll.len();
    let mut scanlen = 5;
    while p < end && scanlen > 0 {
        scanlen -= 1;
        let b = hll[p];
        if sparse_is_xzero(b) {
            p += 2;
            continue;
        } else if sparse_is_zero(b) {
            p += 1;
            continue;
        }
        if p + 1 < end && sparse_is_val(hll[p + 1]) {
            let v1 = sparse_val_value(hll[p]);
            let v2 = sparse_val_value(hll[p + 1]);
            if v1 == v2 {
                let len = sparse_val_len(hll[p]) + sparse_val_len(hll[p + 1]);
                if len <= HLL_SPARSE_VAL_MAX_LEN {
                    hll[p + 1] = sparse_val_set(v1, len);
                    hll.copy_within(p + 1..end, p);
                    hll.pop();
                    end -= 1;
                    continue;
                }
            }
        }
        p += 1;
    }

    // Invalidate the cached cardinality.
    hll[15] |= 0x80;
    1
}

/// hllSparseSet's promote path: convert to dense, apply the register update,
/// and report the promotion.
fn hll_sparse_promote(hll: &mut Vec<u8>, index: usize, count: u8, promoted: &mut bool) -> i32 {
    let Some(dense) = sparse_to_dense(hll) else {
        return -1;
    };
    *hll = dense;
    let dense_retval = hll_dense_set(&mut hll[16..], index, count);
    debug_assert!(dense_retval);
    *promoted = true;
    hll[15] |= 0x80;
    1
}

/// Add `value` to a sparse HLL (`hll` is the sparse buffer, exactly sized).
/// Sets `promoted` when the representation became dense, in which case `hll`
/// is a dense buffer with slack.
pub fn pfadd_sparse(hll: &mut Vec<u8>, value: &[u8], promoted: &mut bool) -> i32 {
    let mut index = 0usize;
    let count = hll_pat_len(value, &mut index);
    let retval = hll_sparse_set(hll, index, count, promoted);
    if retval == 1 {
        // hllSparseSet's merge path already invalidated the cache; the promote
        // path sets it too. Keep the reference's double-invalidation.
        hll[15] |= 0x80;
    }
    retval
}

/// Add `value` to a dense HLL (buffer with slack). Returns 1 if the register
/// was updated, 0 otherwise, -1 if the buffer is not a valid dense HLL.
pub fn pfadd_dense(hll: &mut [u8], value: &[u8]) -> i32 {
    if is_valid_hll(&hll[..HLL_DENSE_SIZE]) != HllValidness::ValidDense {
        return -1;
    }
    let mut index = 0usize;
    let count = hll_pat_len(value, &mut index);
    let retval = hll_dense_set(&mut hll[16..], index, count);
    if retval {
        hll_invalidate_cache(hll);
    }
    i32::from(retval)
}

// ---------------------------------------------------------------------------
// Cardinality estimation
// ---------------------------------------------------------------------------

fn hll_sigma(x: f64) -> f64 {
    if x == 1. {
        return f64::INFINITY;
    }
    let mut x = x;
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let z_prime = z;
        z += x * y;
        y += y;
        if z_prime == z {
            break;
        }
    }
    z
}

fn hll_tau(x: f64) -> f64 {
    if x == 0. || x == 1. {
        return 0.;
    }
    let mut x = x;
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let z_prime = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if z_prime == z {
            break;
        }
    }
    z / 3.0
}

/// Estimate the cardinality from a register histogram (Ertl, arXiv:1702.01284).
fn estimate_from_histo(reghisto: &[u32; 64]) -> u64 {
    let m = HLL_REGISTERS as f64;
    let mut z = m * hll_tau((m - f64::from(reghisto[HLL_Q as usize + 1])) / m);
    for j in (1..=HLL_Q as usize).rev() {
        z += f64::from(reghisto[j]);
        z *= 0.5;
    }
    z += m * hll_sigma(f64::from(reghisto[0]) / m);
    (HLL_ALPHA_INF * m * m / z).round() as u64
}

/// Count a dense HLL (buffer with slack). The cache must have been invalidated
/// by the caller's write path; `pfcount_single` handles the cache itself.
fn hll_count_dense(hll: &[u8]) -> u64 {
    let mut reghisto = [0u32; 64];
    hll_dense_reg_histo(&hll[16..], &mut reghisto);
    estimate_from_histo(&reghisto)
}

/// Estimated count for a single dense HLL. Updates the cached cardinality in
/// `hll` (buffer with slack). Returns -1 when the buffer is not a valid dense
/// HLL.
pub fn pfcount_single(hll: &mut [u8]) -> i64 {
    if is_valid_hll(&hll[..HLL_DENSE_SIZE]) != HllValidness::ValidDense {
        return -1;
    }
    if let Some(card) = hll_cached_card(hll) {
        return card as i64;
    }
    let card = hll_count_dense(hll);
    hll_write_cached_card(hll, card);
    card as i64
}

/// Estimated count of the union of dense HLLs. Each element must be exactly
/// `HLL_DENSE_SIZE` bytes (stored form). Returns -1 if any is invalid.
#[must_use]
pub fn pfcount_multi(hlls: &[&[u8]]) -> i64 {
    let mut max = [0u8; HLL_REGISTERS];
    for hll in hlls {
        if is_valid_hll(hll) != HllValidness::ValidDense {
            return -1;
        }
        hll_merge_dense(&mut max, &hll[16..]);
    }
    let mut reghisto = [0u32; 64];
    hll_raw_reg_histo(&max, &mut reghisto);
    estimate_from_histo(&reghisto) as i64
}

/// Merge dense HLLs into `out` (a dense buffer with slack, created by
/// `create_dense_hll`). `out`'s registers are overwritten with the maximum of
/// the inputs. Each input must be exactly `HLL_DENSE_SIZE` bytes. Returns 0 on
/// success, -1 if any input or `out` is invalid.
pub fn pfmerge(in_hlls: &[&[u8]], out: &mut [u8]) -> i32 {
    if is_valid_hll(&out[..HLL_DENSE_SIZE]) != HllValidness::ValidDense {
        return -1;
    }
    let mut max = [0u8; HLL_REGISTERS];
    for hll in in_hlls {
        if is_valid_hll(hll) != HllValidness::ValidDense {
            return -1;
        }
        hll_merge_dense(&mut max, &hll[16..]);
    }
    hll_dense_compress(&mut out[16..], &max);
    hll_invalidate_cache(out);
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dense_stored(hll: &[u8]) -> Vec<u8> {
        hll[..HLL_DENSE_SIZE].to_vec()
    }

    #[test]
    fn murmur_hash_matches_reference() {
        // Reference values produced by hyperloglog.c's MurmurHash64A with
        // seed 0xadc83b19.
        assert_eq!(murmur_hash64_a(b"", 0xadc8_3b19), 0xd8df_ea65_85bc_9732);
        assert_eq!(murmur_hash64_a(b"foo", 0xadc8_3b19), 0xe646_09b8_b014_1cb4);
        assert_eq!(murmur_hash64_a(b"1", 0xadc8_3b19), 0xd68c_fa33_ac86_5d67);
        assert_eq!(murmur_hash64_a(b"2", 0xadc8_3b19), 0x2f1a_a165_b752_3c0b);
    }

    #[test]
    fn sparse_init_and_promote() {
        let mut sparse = vec![0u8; get_sparse_hll_init_size()];
        assert!(init_sparse_hll(&mut sparse));
        assert_eq!(is_valid_hll(&sparse), HllValidness::ValidSparse);

        let mut dense = sparse_to_dense(&sparse).expect("empty sparse converts");
        assert_eq!(
            is_valid_hll(&dense[..HLL_DENSE_SIZE]),
            HllValidness::ValidDense
        );
        assert_eq!(pfcount_single(&mut dense[..]), 0);
    }

    #[test]
    fn pfadd_dense_basic() {
        let mut hll = create_dense_hll();
        assert_eq!(pfadd_dense(&mut hll, b"1"), 1);
        assert_eq!(pfadd_dense(&mut hll, b"1"), 0);
        assert_eq!(pfcount_single(&mut hll[..]), 1);
    }

    #[test]
    fn dense_merge_and_multi_count() {
        let mut a = create_dense_hll();
        let mut b = create_dense_hll();
        pfadd_dense(&mut a, b"1");
        pfadd_dense(&mut a, b"2");
        pfadd_dense(&mut b, b"2");
        pfadd_dense(&mut b, b"3");

        let merged = pfcount_multi(&[&dense_stored(&a), &dense_stored(&b)]);
        assert_eq!(merged, 3);

        let mut out = create_dense_hll();
        assert_eq!(
            pfmerge(&[&dense_stored(&a), &dense_stored(&b)], &mut out),
            0
        );
        assert_eq!(pfcount_single(&mut out[..]), 3);
    }

    #[test]
    fn sparse_overflow_payload_rejected() {
        // CVE-2025-32023: 155486 XZERO ops overflow the register space.
        let mut hll = Vec::new();
        hll.extend_from_slice(b"HYLL");
        hll.push(1); // sparse
        hll.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        for _ in 0..155_486 {
            hll.push(0x7f);
            hll.push(0xff);
        }
        hll.push(0x80); // trailing VAL
        assert_eq!(is_valid_hll(&hll), HllValidness::ValidSparse);
        assert!(sparse_to_dense(&hll).is_none());
    }
}
