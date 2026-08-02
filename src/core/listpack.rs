//! Redis listpack codec, byte-for-byte compatible with Dragonfly's
//! `dragonfly/src/redis/listpack.c`.
//!
//! Format:
//! ```text
//! +--------+---------+-------+------+--------+
//! | total  | num elems | elem | elem | 0xFF   |
//! | bytes  | (u16 LE) | ...  | ...  | (EOF)  |
//! | (u32 LE)|         |      |      |        |
//! +--------+---------+-------+------+--------+
//! ```
//!
//! Every element is followed by a variable-length "backlen" that encodes the
//! total number of bytes the element occupies (excluding the backlen itself),
//! stored most-significant-group first with a continuation bit on every group
//! except the most significant one.

/// Size of the listpack header (`total bytes` + `num elements`).
pub const LP_HDR_SIZE: usize = 6;
/// Terminator byte.
pub const LP_EOF: u8 = 0xff;
/// Header value meaning "number of elements unknown" (used by ziplist
/// conversions); validation skips the count check when this is present.
pub const LP_HDR_NUMELE_UNKNOWN: u16 = 0xffff;

const LP_ENCODING_6BIT_STR: u8 = 0x80;
const LP_ENCODING_13BIT_INT: u8 = 0xc0;
const LP_ENCODING_12BIT_STR: u8 = 0xe0;
const LP_ENCODING_32BIT_STR: u8 = 0xf0;
const LP_ENCODING_16BIT_INT: u8 = 0xf1;
const LP_ENCODING_24BIT_INT: u8 = 0xf2;
const LP_ENCODING_32BIT_INT: u8 = 0xf3;
const LP_ENCODING_64BIT_INT: u8 = 0xf4;

/// Max length of a string that `string_to_i64` will consider (`LONG_STR_SIZE`).
const LONG_STR_SIZE: usize = 21;

pub fn total_bytes(buf: &[u8]) -> u32 {
    u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])
}

pub fn num_elements(buf: &[u8]) -> u32 {
    (buf[4] as u32) | ((buf[5] as u32) << 8)
}

fn set_total(buf: &mut [u8], v: u32) {
    buf[0] = v as u8;
    buf[1] = (v >> 8) as u8;
    buf[2] = (v >> 16) as u8;
    buf[3] = (v >> 24) as u8;
}

fn set_num_elements(buf: &mut [u8], v: u32) {
    buf[4] = v as u8;
    buf[5] = (v >> 8) as u8;
}

/// Number of bytes required to encode the backlen of an element of `l` bytes
/// (`lpEncodeBacklen(NULL, l)`).
fn backlen_size(l: u64) -> usize {
    match l {
        0..=127 => 1,
        128..=16382 => 2,
        16383..=2097150 => 3,
        2097151..=268435454 => 4,
        _ => 5,
    }
}

/// Append the backlen for an element of `l` bytes (`lpEncodeBacklen`).
fn write_backlen(out: &mut Vec<u8>, l: u64) {
    match backlen_size(l) {
        1 => out.push(l as u8),
        2 => {
            out.push((l >> 7) as u8);
            out.push(((l & 127) | 128) as u8);
        }
        3 => {
            out.push((l >> 14) as u8);
            out.push((((l >> 7) & 127) | 128) as u8);
            out.push(((l & 127) | 128) as u8);
        }
        4 => {
            out.push((l >> 21) as u8);
            out.push((((l >> 14) & 127) | 128) as u8);
            out.push((((l >> 7) & 127) | 128) as u8);
            out.push(((l & 127) | 128) as u8);
        }
        _ => {
            out.push((l >> 28) as u8);
            out.push((((l >> 21) & 127) | 128) as u8);
            out.push((((l >> 14) & 127) | 128) as u8);
            out.push((((l >> 7) & 127) | 128) as u8);
            out.push(((l & 127) | 128) as u8);
        }
    }
}

/// Decode the backlen ending at `pos` (the byte immediately before the next
/// element). Walks backwards from `pos` while the continuation bit is set,
/// mirroring `lpDecodeBacklen`. Returns `None` for malformed backlens.
fn decode_backlen(buf: &[u8], mut pos: usize) -> Option<u64> {
    let mut val: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let b = *buf.get(pos)?;
        val |= (u64::from(b & 127)) << shift;
        if b & 128 == 0 {
            break;
        }
        shift += 7;
        if pos == 0 {
            return None;
        }
        pos -= 1;
        if shift > 28 {
            return None;
        }
    }
    Some(val)
}

/// Parse a byte string as an integer, exactly like `lpStringToInt64`. Rejects
/// empty strings, strings >= 21 bytes, strings with leading zeros (except "0"),
/// a bare "-", non-digit bytes and overflowing values.
pub fn string_to_i64(s: &[u8]) -> Option<i64> {
    if s.is_empty() || s.len() >= LONG_STR_SIZE {
        return None;
    }
    if s.len() == 1 && s[0] == b'0' {
        return Some(0);
    }
    let (neg, digits) = match s[0] {
        b'-' => (true, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() || digits[0] == b'0' || !digits[0].is_ascii_digit() {
        return None;
    }
    let mut v: u64 = 0;
    for &d in digits {
        if !d.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?;
        v = v.checked_add(u64::from(d - b'0'))?;
    }
    if neg {
        if v > (1u64 << 63) {
            return None;
        }
        if v == (1u64 << 63) {
            return Some(i64::MIN);
        }
        Some(-(v as i64))
    } else {
        if v > i64::MAX as u64 {
            return None;
        }
        Some(v as i64)
    }
}

/// Encode `v` into `out` (which must be 9 bytes) using `lpEncodeIntegerGetType`.
/// Returns the number of bytes written.
pub fn encode_integer(v: i64, out: &mut [u8; 9]) -> usize {
    if (0..=127).contains(&v) {
        out[0] = v as u8;
        1
    } else if (-4096..=4095).contains(&v) {
        let uv = if v < 0 { (1i64 << 13) + v } else { v };
        out[0] = LP_ENCODING_13BIT_INT | ((uv >> 8) as u8);
        out[1] = uv as u8;
        2
    } else if (-32768..=32767).contains(&v) {
        let uv = if v < 0 { (1i64 << 16) + v } else { v };
        out[0] = LP_ENCODING_16BIT_INT;
        out[1] = uv as u8;
        out[2] = (uv >> 8) as u8;
        3
    } else if (-8388608..=8388607).contains(&v) {
        let uv = if v < 0 { (1i64 << 24) + v } else { v };
        out[0] = LP_ENCODING_24BIT_INT;
        out[1] = uv as u8;
        out[2] = (uv >> 8) as u8;
        out[3] = (uv >> 16) as u8;
        4
    } else if (-2147483648..=2147483647).contains(&v) {
        let uv = if v < 0 { (1i64 << 32) + v } else { v };
        out[0] = LP_ENCODING_32BIT_INT;
        out[1] = uv as u8;
        out[2] = (uv >> 8) as u8;
        out[3] = (uv >> 16) as u8;
        out[4] = (uv >> 24) as u8;
        5
    } else {
        let uv = v as u64;
        out[0] = LP_ENCODING_64BIT_INT;
        out[1..9].copy_from_slice(&uv.to_le_bytes());
        9
    }
}

fn read_le_u16(buf: &[u8], pos: usize) -> Option<u16> {
    let b = buf.get(pos..pos + 2)?;
    Some(u16::from_le_bytes([b[0], b[1]]))
}

fn read_le_u24(buf: &[u8], pos: usize) -> Option<u32> {
    let b = buf.get(pos..pos + 3)?;
    Some(b[0] as u32 | (b[1] as u32) << 8 | (b[2] as u32) << 16)
}

fn read_le_u32(buf: &[u8], pos: usize) -> Option<u32> {
    let b = buf.get(pos..pos + 4)?;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_le_u64(buf: &[u8], pos: usize) -> Option<u64> {
    let b = buf.get(pos..pos + 8)?;
    Some(u64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

/// Number of bytes of the encoded element starting at `buf[pos]` that are
/// needed to hold the length fields themselves (`lpCurrentEncodedSizeBytes`).
/// Returns 0 for invalid encodings.
fn size_bytes(b: u8) -> usize {
    if b & 0x80 == 0 || b & 0xc0 == 0x80 {
        1
    } else if b & 0xe0 == 0xc0 {
        2
    } else if b == LP_ENCODING_16BIT_INT {
        3
    } else if b == LP_ENCODING_24BIT_INT {
        4
    } else if b == LP_ENCODING_32BIT_INT {
        5
    } else if b == LP_ENCODING_64BIT_INT {
        9
    } else if b & 0xf0 == 0xe0 {
        2
    } else if b == LP_ENCODING_32BIT_STR {
        5
    } else if b == LP_EOF {
        1
    } else {
        0
    }
}

/// Full encoded size (header bytes + data) of the element at `buf[pos]`
/// (`lpCurrentEncodedSizeUnsafe`), excluding the backlen. `None` for invalid
/// encodings.
fn encoded_size(buf: &[u8], pos: usize) -> Option<usize> {
    let b = *buf.get(pos)?;
    if b & 0x80 == 0 {
        Some(1)
    } else if b & 0xc0 == 0x80 {
        Some(1 + (b & 0x3f) as usize)
    } else if b & 0xe0 == 0xc0 {
        Some(2)
    } else if b == LP_ENCODING_16BIT_INT {
        Some(3)
    } else if b == LP_ENCODING_24BIT_INT {
        Some(4)
    } else if b == LP_ENCODING_32BIT_INT {
        Some(5)
    } else if b == LP_ENCODING_64BIT_INT {
        Some(9)
    } else if b & 0xf0 == 0xe0 {
        let len = ((b & 0x0f) as usize) << 8 | *buf.get(pos + 1)? as usize;
        Some(2 + len)
    } else if b == LP_ENCODING_32BIT_STR {
        let len = read_le_u32(buf, pos + 1)? as usize;
        Some(5 + len)
    } else if b == LP_EOF {
        Some(1)
    } else {
        None
    }
}

/// Validate a single entry at `pos` and return the position of the next entry,
/// mirroring `lpValidateNext`. `None` means the entry is corrupt or `pos`
/// points at the EOF byte.
fn validate_next(buf: &[u8], p: usize) -> Option<usize> {
    if p < LP_HDR_SIZE || p >= buf.len() {
        return None;
    }
    if buf[p] == LP_EOF {
        return None;
    }
    let lenbytes = size_bytes(buf[p]);
    if lenbytes == 0 {
        return None;
    }
    if p + lenbytes > buf.len() - 1 {
        return None;
    }
    let entrylen = encoded_size(buf, p)?;
    let backlen_bytes = backlen_size(entrylen as u64);
    let entrylen = entrylen + backlen_bytes;
    if p + entrylen > buf.len() - 1 {
        return None;
    }
    let np = p + entrylen;
    let prevlen = decode_backlen(buf, np - 1)?;
    if prevlen.wrapping_add(backlen_bytes as u64) != entrylen as u64 {
        return None;
    }
    Some(np)
}

/// Deep integrity validation, equivalent to `lpValidateIntegrity(lp, size, 1)`.
pub fn validate_deep(buf: &[u8]) -> bool {
    if buf.len() < LP_HDR_SIZE + 1 {
        return false;
    }
    if total_bytes(buf) as usize != buf.len() {
        return false;
    }
    if buf[buf.len() - 1] != LP_EOF {
        return false;
    }
    let mut count: u32 = 0;
    let numele = num_elements(buf);
    let mut p = LP_HDR_SIZE;
    loop {
        if p >= buf.len() {
            return false;
        }
        if buf[p] == LP_EOF {
            break;
        }
        match validate_next(buf, p) {
            Some(np) => p = np,
            None => return false,
        }
        count += 1;
    }
    if p != buf.len() - 1 {
        return false;
    }
    if numele != u32::from(LP_HDR_NUMELE_UNKNOWN) && numele != count {
        return false;
    }
    true
}

/// Position of the first element, if the listpack is non-empty
/// (`lpFirst`/`lpValidateFirst`).
pub fn first(buf: &[u8]) -> Option<usize> {
    if buf.len() < LP_HDR_SIZE + 1 {
        return None;
    }
    let p = LP_HDR_SIZE;
    if buf[p] == LP_EOF { None } else { Some(p) }
}

/// Position of the element after the one at `pos`, or `None` if `pos` was the
/// last element (`lpNext`).
pub fn next(buf: &[u8], pos: usize) -> Option<usize> {
    let np = validate_next(buf, pos)?;
    if buf.get(np)? == &LP_EOF {
        return None;
    }
    Some(np)
}

/// A decoded element. Integer-encoded elements carry their value; string
/// elements borrow their bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry<'a> {
    Int(i64),
    Str(&'a [u8]),
}

/// Decode the element at `pos` (`lpGet` semantics).
pub fn entry_at(buf: &[u8], pos: usize) -> Option<Entry<'_>> {
    let b = *buf.get(pos)?;
    if b & 0x80 == 0 {
        return Some(Entry::Int(b as i64));
    }
    if b & 0xc0 == 0x80 {
        let len = (b & 0x3f) as usize;
        let s = buf.get(pos + 1..pos + 1 + len)?;
        return Some(Entry::Str(s));
    }
    if b & 0xe0 == 0xc0 {
        let u = (((b & 0x1f) as u16) << 8) | *buf.get(pos + 1)? as u16;
        let v = (u & 0x1fff) as i64;
        return Some(Entry::Int(if v >= 4096 { v - 8192 } else { v }));
    }
    match b {
        LP_ENCODING_16BIT_INT => {
            let u = read_le_u16(buf, pos + 1)?;
            Some(Entry::Int(u as i16 as i64))
        }
        LP_ENCODING_24BIT_INT => {
            let u = read_le_u24(buf, pos + 1)?;
            Some(Entry::Int(((u << 8) as i32 >> 8) as i64))
        }
        LP_ENCODING_32BIT_INT => {
            let u = read_le_u32(buf, pos + 1)?;
            Some(Entry::Int(u as i32 as i64))
        }
        LP_ENCODING_64BIT_INT => {
            let u = read_le_u64(buf, pos + 1)?;
            Some(Entry::Int(u as i64))
        }
        b if b & 0xf0 == 0xe0 => {
            let len = (((b & 0x0f) as usize) << 8) | *buf.get(pos + 1)? as usize;
            let s = buf.get(pos + 2..pos + 2 + len)?;
            Some(Entry::Str(s))
        }
        LP_ENCODING_32BIT_STR => {
            let len = read_le_u32(buf, pos + 1)? as usize;
            let s = buf.get(pos + 5..pos + 5 + len)?;
            Some(Entry::Str(s))
        }
        _ => None,
    }
}

/// `lpGetInteger`: decode an integer element. Returns `None` for string
/// encodings, the EOF byte and unknown encodings.
pub fn get_integer(buf: &[u8], pos: usize) -> Option<i64> {
    let encoding = *buf.get(pos)?;
    let (uval, negstart, negmax): (u64, u64, u64) = if encoding < 0x80 {
        (encoding as u64, u64::MAX, 0)
    } else if encoding > LP_ENCODING_32BIT_STR {
        match encoding {
            LP_ENCODING_16BIT_INT => (
                read_le_u16(buf, pos + 1)? as u64,
                1u64 << 15,
                u16::MAX as u64,
            ),
            LP_ENCODING_24BIT_INT => (
                read_le_u24(buf, pos + 1)? as u64,
                1 << 23,
                (u32::MAX >> 8) as u64,
            ),
            LP_ENCODING_32BIT_INT => (
                read_le_u32(buf, pos + 1)? as u64,
                1u64 << 31,
                u32::MAX as u64,
            ),
            LP_ENCODING_64BIT_INT => (read_le_u64(buf, pos + 1)?, 1u64 << 63, u64::MAX),
            _ => return None,
        }
    } else if (LP_ENCODING_13BIT_INT..0xe0).contains(&encoding) {
        let u = (((encoding & 0x1f) as u16) << 8) | *buf.get(pos + 1)? as u16;
        (u as u64, 1u64 << 12, 8191)
    } else {
        return None;
    };
    if uval >= negstart {
        let u = negmax.wrapping_sub(uval);
        let val = u as i64;
        Some(-val - 1)
    } else {
        Some(uval as i64)
    }
}

/// Incremental listpack builder mirroring `lpAppend` / `lpAppendInteger`.
#[derive(Debug, Clone)]
pub struct Listpack {
    buf: Vec<u8>,
}

impl Default for Listpack {
    fn default() -> Self {
        Self::new()
    }
}

impl Listpack {
    /// Create an empty listpack (`lpNew`).
    pub fn new() -> Self {
        let mut buf = vec![0u8; LP_HDR_SIZE + 1];
        set_total(&mut buf, (LP_HDR_SIZE + 1) as u32);
        buf[LP_HDR_SIZE] = LP_EOF;
        Listpack { buf }
    }

    fn append_encoded(&mut self, elem: &[u8]) {
        let enclen = elem.len() as u64;
        let blen = backlen_size(enclen) as u64;
        let old_total = total_bytes(&self.buf) as u64;
        let new_total = old_total + enclen + blen;
        debug_assert!(new_total <= u32::MAX as u64);
        // Drop the EOF, append the element and its backlen, restore the EOF.
        self.buf.truncate(self.buf.len() - 1);
        self.buf.extend_from_slice(elem);
        write_backlen(&mut self.buf, enclen);
        self.buf.push(LP_EOF);
        let cur = num_elements(&self.buf);
        if cur != u32::from(LP_HDR_NUMELE_UNKNOWN) {
            set_num_elements(&mut self.buf, cur + 1);
        }
        set_total(&mut self.buf, new_total as u32);
    }

    /// Append an integer element (`lpAppendInteger`).
    pub fn append_integer(&mut self, v: i64) {
        let mut enc = [0u8; 9];
        let n = encode_integer(v, &mut enc);
        self.append_encoded(&enc[..n]);
    }

    /// Append a byte string element (`lpAppend`). Strings that parse as
    /// integers are stored using the integer encoding.
    pub fn append_bytes(&mut self, s: &[u8]) {
        if let Some(v) = string_to_i64(s) {
            self.append_integer(v);
            return;
        }
        let len = s.len();
        let mut head = [0u8; 5];
        let n = if len < 64 {
            head[0] = LP_ENCODING_6BIT_STR | len as u8;
            1
        } else if len < 4096 {
            head[0] = LP_ENCODING_12BIT_STR | ((len >> 8) as u8 & 0x0f);
            head[1] = len as u8;
            2
        } else {
            head[0] = LP_ENCODING_32BIT_STR;
            head[1..5].copy_from_slice(&(len as u32).to_le_bytes());
            5
        };
        let mut elem = Vec::with_capacity(n + len);
        elem.extend_from_slice(&head[..n]);
        elem.extend_from_slice(s);
        self.append_encoded(&elem);
    }

    /// Number of elements stored.
    pub fn elements(&self) -> u32 {
        num_elements(&self.buf)
    }

    /// Current total size of the listpack in bytes (`lpBytes`).
    pub fn byte_len(&self) -> usize {
        total_bytes(&self.buf) as usize
    }

    /// Finish building and return the raw listpack bytes.
    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lp_bytes(bytes: &[u8]) -> Vec<u8> {
        bytes.to_vec()
    }

    #[test]
    fn empty() {
        let lp = Listpack::new().into_vec();
        assert_eq!(lp, lp_bytes(&[7, 0, 0, 0, 0, 0, 0xff]));
        assert!(validate_deep(&lp));
        assert_eq!(first(&lp), None);
    }

    #[test]
    fn integer_encodings() {
        let mut lp = Listpack::new();
        lp.append_integer(0);
        lp.append_integer(127);
        lp.append_integer(128); // 13-bit
        lp.append_integer(4095); // 13-bit max
        lp.append_integer(-4096); // 13-bit min
        lp.append_integer(1234);
        lp.append_integer(-1234);
        lp.append_integer(70000); // 16-bit
        lp.append_integer(-70000);
        lp.append_integer(10_000_000); // 24-bit
        lp.append_integer(-10_000_000);
        lp.append_integer(2_000_000_000); // 32-bit
        lp.append_integer(-2_000_000_000);
        lp.append_integer(i64::MAX); // 64-bit
        lp.append_integer(i64::MIN);
        let buf = lp.into_vec();
        assert!(validate_deep(&buf));

        let mut values = Vec::new();
        let mut p = first(&buf).unwrap();
        loop {
            values.push(entry_at(&buf, p).unwrap());
            match next(&buf, p) {
                Some(np) => p = np,
                None => break,
            }
        }
        assert_eq!(values.len(), 15);
        assert_eq!(
            values,
            vec![
                Entry::Int(0),
                Entry::Int(127),
                Entry::Int(128),
                Entry::Int(4095),
                Entry::Int(-4096),
                Entry::Int(1234),
                Entry::Int(-1234),
                Entry::Int(70000),
                Entry::Int(-70000),
                Entry::Int(10_000_000),
                Entry::Int(-10_000_000),
                Entry::Int(2_000_000_000),
                Entry::Int(-2_000_000_000),
                Entry::Int(i64::MAX),
                Entry::Int(i64::MIN),
            ]
        );
        // 15 elements total
        assert_eq!(num_elements(&buf), 15);
        // Every integer decodes via get_integer too.
        let mut p = first(&buf).unwrap();
        loop {
            assert!(get_integer(&buf, p).is_some());
            match next(&buf, p) {
                Some(np) => p = np,
                None => break,
            }
        }
    }

    #[test]
    fn string_encodings() {
        let mut lp = Listpack::new();
        lp.append_bytes(b"");
        lp.append_bytes(b"a"); // 6-bit str
        lp.append_bytes(&[0x61; 63]); // 6-bit str max
        lp.append_bytes(&[0x62; 64]); // 12-bit str
        lp.append_bytes(&[0x63; 4095]); // 12-bit str max
        lp.append_bytes(&[0x64; 4096]); // 32-bit str
        let buf = lp.into_vec();
        assert!(validate_deep(&buf));

        let mut lens = Vec::new();
        let mut p = first(&buf).unwrap();
        loop {
            match entry_at(&buf, p).unwrap() {
                Entry::Str(s) => lens.push(s.len()),
                _ => panic!("expected string"),
            }
            match next(&buf, p) {
                Some(np) => p = np,
                None => break,
            }
        }
        assert_eq!(lens, vec![0, 1, 63, 64, 4095, 4096]);
    }

    #[test]
    fn string_to_i64_rules() {
        assert_eq!(string_to_i64(b"0"), Some(0));
        assert_eq!(string_to_i64(b"-0"), None);
        assert_eq!(string_to_i64(b"007"), None);
        assert_eq!(string_to_i64(b"-"), None);
        assert_eq!(string_to_i64(b""), None);
        assert_eq!(string_to_i64(b"12a"), None);
        assert_eq!(string_to_i64(b"19"), Some(19));
        assert_eq!(string_to_i64(b"-100"), Some(-100));
        assert_eq!(string_to_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(string_to_i64(b"9223372036854775808"), None);
        assert_eq!(string_to_i64(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(string_to_i64(b"-9223372036854775809"), None);
        assert_eq!(string_to_i64(b"12345678901234567890"), None); // > i64::MAX
        assert_eq!(string_to_i64(b"123456789012345678901"), None); // >= 21 chars
    }

    #[test]
    fn append_detects_integers() {
        let mut lp = Listpack::new();
        lp.append_bytes(b"19");
        lp.append_bytes(b"20");
        lp.append_bytes(b"acme");
        let buf = lp.into_vec();
        assert!(validate_deep(&buf));
        let mut p = first(&buf).unwrap();
        assert_eq!(entry_at(&buf, p).unwrap(), Entry::Int(19));
        p = next(&buf, p).unwrap();
        assert_eq!(entry_at(&buf, p).unwrap(), Entry::Int(20));
        p = next(&buf, p).unwrap();
        assert_eq!(entry_at(&buf, p).unwrap(), Entry::Str(b"acme"));
    }

    /// Byte-exact reference vectors taken from generic_family_test.cc.
    #[test]
    fn reference_hash_dump_listpack() {
        let mut lp = Listpack::new();
        lp.append_integer(19);
        lp.append_integer(1234);
        assert_eq!(
            lp.into_vec(),
            lp_bytes(&[
                0x0c, 0x00, 0x00, 0x00, 0x02, 0x00, 0x13, 0x01, 0xc4, 0xd2, 0x02, 0xff
            ])
        );
    }

    #[test]
    fn reference_set_listpack() {
        let mut lp = Listpack::new();
        lp.append_bytes(b"acme");
        assert_eq!(
            lp.into_vec(),
            lp_bytes(&[
                0x0d, 0x00, 0x00, 0x00, 0x01, 0x00, 0x84, 0x61, 0x63, 0x6d, 0x65, 0x05, 0xff
            ])
        );
    }

    #[test]
    fn reference_zset_listpack() {
        let mut lp = Listpack::new();
        lp.append_bytes(b"elon");
        lp.append_integer(1);
        assert_eq!(
            lp.into_vec(),
            lp_bytes(&[
                0x0f, 0x00, 0x00, 0x00, 0x02, 0x00, 0x84, 0x65, 0x6c, 0x6f, 0x6e, 0x05, 0x01, 0x01,
                0xff
            ])
        );
    }

    #[test]
    fn reference_list_quicklist_node() {
        let mut lp = Listpack::new();
        lp.append_bytes(b"20");
        assert_eq!(
            lp.into_vec(),
            lp_bytes(&[0x09, 0x00, 0x00, 0x00, 0x01, 0x00, 0x14, 0x01, 0xff])
        );
    }

    /// OOB payload from RestoreOobHashListpack: a 32-bit string declaring
    /// length 0x7fffffff inside a 12-byte listpack must be rejected.
    #[test]
    fn rejects_oob_32bit_string() {
        let bad = lp_bytes(&[
            0x0c, 0x00, 0x00, 0x00, 0x02, 0x00, 0xf0, 0xff, 0xff, 0xff, 0x7f, 0xff,
        ]);
        assert!(!validate_deep(&bad));
    }

    #[test]
    fn rejects_truncated_element() {
        // 6-bit string claims 10 bytes but the listpack ends.
        let bad = lp_bytes(&[12, 0, 0, 0, 1, 0, 0x8a, 0x01, 0xff]);
        assert!(!validate_deep(&bad));
    }

    #[test]
    fn rejects_bad_backlen() {
        // Element with an incorrect backlen value.
        let bad = lp_bytes(&[8, 0, 0, 0, 1, 0, 0x84, 0x61, 0x03, 0xff]);
        assert!(!validate_deep(&bad));
    }

    #[test]
    fn rejects_size_mismatch_header() {
        // total_bytes says 8 but the buffer is 9 bytes.
        let bad = lp_bytes(&[8, 0, 0, 0, 1, 0, 0x14, 0x01, 0xff]);
        assert!(!validate_deep(&bad));
    }

    #[test]
    fn rejects_missing_eof() {
        let bad = lp_bytes(&[8, 0, 0, 0, 1, 0, 0x14, 0x01]);
        assert!(!validate_deep(&bad));
    }

    #[test]
    fn rejects_bad_element_count() {
        // num elements claims 2 but only 1 is stored.
        let bad = lp_bytes(&[9, 0, 0, 0, 2, 0, 0x14, 0x01, 0xff]);
        assert!(!validate_deep(&bad));
    }

    #[test]
    fn num_elements_unknown_is_skipped() {
        let mut lp = Listpack::new();
        lp.append_integer(1);
        let mut buf = lp.into_vec();
        set_num_elements(&mut buf, u32::from(LP_HDR_NUMELE_UNKNOWN));
        assert!(validate_deep(&buf));
    }

    #[test]
    fn backlen_roundtrip_all_sizes() {
        for l in [
            1u64,
            127,
            128,
            16382,
            16383,
            16384,
            2097150,
            2097151,
            2097152,
            268435454,
            268435455,
            268435456,
            u32::MAX as u64,
        ] {
            let mut buf = Vec::new();
            write_backlen(&mut buf, l);
            assert_eq!(buf.len(), backlen_size(l));
            assert_eq!(decode_backlen(&buf, buf.len() - 1), Some(l));
        }
    }

    #[test]
    fn backlen_corrupt_returns_none() {
        // 6 bytes of continuation
        let buf = vec![0xffu8; 6];
        assert_eq!(decode_backlen(&buf, buf.len() - 1), None);
    }
}
