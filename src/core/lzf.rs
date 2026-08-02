//! LZF (FASTLZ) compression used by RDB's `RDB_ENC_LZF` (1) encoding.
//!
//! Config mirrors `dragonfly/src/redis/lzf_c.c` / `lzf_d.c` / `lzfP.h`:
//! `HLOG = 16`, `VERY_FAST = 1`, `ULTRA_FAST = 0`, `INIT_HTAB = 1` (we
//! zero the hash table so compression is deterministic — the reference leaves
//! it uninitialized, but a zeroed table is byte-identical for inputs small
//! enough that the table stays all-zeros, and any stream the reference emits
//! still decodes here), `CHECK_INPUT = 1`, offset-based slots (`u32`).
//!
//! Stream format:
//!   - `000LLLLL <L+1 bytes>`     literal run (1..32 bytes)
//!   - `LLLooooo oooooooo`        backref, len field = copy-3 (0..6 → 3..9 bytes)
//!   - `111ooooo LLLLLLLL oooooooo`  backref, extended len (9..264 bytes)

const HLOG: u32 = 16;
const HSIZE: usize = 1 << HLOG;
const MAX_LIT: usize = 1 << 5;
const MAX_OFF: usize = 1 << 13;
const MAX_REF: usize = (1 << 8) + (1 << 3);

#[inline]
fn frst(p: &[u8]) -> u32 {
    ((p[0] as u32) << 8) | p[1] as u32
}

#[inline]
fn next(hval: u32, p: &[u8]) -> u32 {
    (hval << 8) | p[2] as u32
}

#[inline]
fn idx(h: u32) -> usize {
    (h.wrapping_shr(24 - HLOG).wrapping_sub(h.wrapping_mul(5)) & (HSIZE as u32 - 1)) as usize
}

/// Compress `data`, mirroring `lzf_compress`. Returns `None` when the output
/// would overflow the reference's `in_len + 1` buffer (caller falls back to
/// storing the data verbatim).
pub fn compress(data: &[u8]) -> Option<Vec<u8>> {
    let in_len = data.len();
    if in_len == 0 {
        return None;
    }
    let mut out = vec![0u8; in_len + 1];
    let mut htab = vec![0u32; HSIZE];

    let in_end = in_len;
    let out_end = in_len + 1;
    let mut ip = 0usize;
    let mut op = 1usize; // start literal run: reserve the count byte
    let mut lit = 0usize;
    let mut hval = if in_len >= 2 { frst(data) } else { 0 };

    while ip + 2 < in_end {
        hval = next(hval, &data[ip..]);
        let slot = idx(hval);
        let ref_off = htab[slot] as usize;
        htab[slot] = ip as u32;

        if ref_off != 0
            && ip > ref_off
            && ip - ref_off - 1 < MAX_OFF
            && ref_off > 0
            && data[ref_off + 2] == data[ip + 2]
            && u16::from_le_bytes([data[ref_off], data[ref_off + 1]])
                == u16::from_le_bytes([data[ip], data[ip + 1]])
        {
            let off = ip - ref_off - 1;
            let mut len = 2usize;
            let maxlen = {
                let m = in_end - ip - len;
                if m > MAX_REF { MAX_REF } else { m }
            };

            // Conservative, then exact, output bound checks.
            if op + 4 >= out_end && op - (if lit == 0 { 1 } else { 0 }) + 4 >= out_end {
                return None;
            }

            out[op - lit - 1] = lit.wrapping_sub(1) as u8; // stop run
            if lit == 0 {
                op -= 1; // undo run if length is zero
            }

            let mut mismatched = false;
            if maxlen > 16 {
                for _ in 0..16 {
                    len += 1;
                    if data[ref_off + len] != data[ip + len] {
                        mismatched = true;
                        break;
                    }
                }
            }
            if !mismatched {
                loop {
                    len += 1;
                    if len >= maxlen || data[ref_off + len] != data[ip + len] {
                        break;
                    }
                }
            }

            len -= 2; // len is now #octets - 1
            ip += 1;

            if len < 7 {
                out[op] = ((off >> 8) as u8) + ((len as u8) << 5);
                op += 1;
            } else {
                out[op] = ((off >> 8) as u8) + (7 << 5);
                op += 1;
                out[op] = (len - 7) as u8;
                op += 1;
            }
            out[op] = off as u8;
            op += 1;

            lit = 0;
            op += 1; // start run
            ip += len + 1;

            if ip + 2 >= in_end {
                break;
            }

            // Re-index (VERY_FAST && !ULTRA_FAST path).
            ip -= 2;
            hval = frst(&data[ip..]);
            hval = next(hval, &data[ip..]);
            htab[idx(hval)] = ip as u32;
            ip += 1;
            hval = next(hval, &data[ip..]);
            htab[idx(hval)] = ip as u32;
            ip += 1;
        } else {
            // One more literal byte we must copy.
            if op >= out_end {
                return None;
            }
            lit += 1;
            out[op] = data[ip];
            op += 1;
            ip += 1;

            if lit == MAX_LIT {
                out[op - lit - 1] = lit.wrapping_sub(1) as u8; // stop run
                lit = 0;
                op += 1; // start run
            }
        }
    }

    // Tail: at most 3 bytes can be missing here.
    if op + 3 > out_end {
        return None;
    }
    while ip < in_end {
        lit += 1;
        out[op] = data[ip];
        op += 1;
        ip += 1;
        if lit == MAX_LIT {
            out[op - lit - 1] = lit.wrapping_sub(1) as u8;
            lit = 0;
            op += 1;
        }
    }
    out[op - lit - 1] = lit.wrapping_sub(1) as u8; // end run
    if lit == 0 {
        op -= 1; // undo run if length is zero
    }

    Some(out[..op].to_vec())
}

/// Decompress a LZF stream, mirroring `lzf_decompress` with `CHECK_INPUT`.
/// Returns `None` on any overflow (input or output); the output buffer is
/// always exactly `expected` bytes (the reference tolerates a stream that
/// produces fewer bytes).
pub fn decompress(data: &[u8], expected: usize) -> Option<Vec<u8>> {
    let mut out = vec![0u8; expected];
    let in_end = data.len();
    let out_end = expected;
    let mut ip = 0usize;
    let mut op = 0usize;

    while ip < in_end {
        let ctrl = data[ip] as usize;
        ip += 1;

        if ctrl < (1 << 5) {
            // Literal run.
            let n = ctrl + 1;
            if op + n > out_end {
                return None;
            }
            if ip + n > in_end {
                return None;
            }
            out[op..op + n].copy_from_slice(&data[ip..ip + n]);
            op += n;
            ip += n;
        } else {
            // Back reference.
            let mut len = ctrl >> 5;
            if ip >= in_end {
                return None;
            }
            let mut ref_off = op.checked_sub(((ctrl & 0x1f) << 8) + 1)?;
            if len == 7 {
                len += data[ip] as usize;
                ip += 1;
                if ip >= in_end {
                    return None;
                }
            }
            ref_off = ref_off.checked_sub(data[ip] as usize)?;
            ip += 1;

            if op + len + 2 > out_end {
                return None;
            }
            if ref_off + len + 2 > out.len() {
                return None;
            }

            // Byte-by-byte copy handles overlapping regions.
            len += 2;
            let mut remaining = len;
            while remaining > 0 {
                out[op] = out[ref_off];
                op += 1;
                ref_off += 1;
                remaining -= 1;
            }
        }
    }

    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_run_stream() {
        // 0x04 = literal run of 5 bytes.
        assert_eq!(
            decompress(&[0x04, b'h', b'e', b'l', b'l', b'o'], 5).unwrap(),
            b"hello"
        );
        // Producing fewer bytes than expected is tolerated (mirrors reference).
        assert_eq!(
            decompress(&[0x04, b'h', b'e', b'l', b'l', b'o'], 8).unwrap(),
            b"hello\x00\x00\x00"
        );
    }

    #[test]
    fn backref_stream() {
        // Literal "ab", then backref: ctrl=0x20 (len field 1 -> 3 bytes),
        // offset byte 0x01 (offset 2). Copies "aba".
        let stream = [0x01, b'a', b'b', 0x20, 0x01];
        assert_eq!(decompress(&stream, 5).unwrap(), b"ababa");
    }

    #[test]
    fn extended_backref_stream() {
        // Literal 'x', then backref len 8 (ctrl=0xe0, ext len 1), offset 1.
        // 8+2 = 10 bytes copied -> "xxxxxxxxxxx".
        let stream = [0x00, b'x', 0xe0, 0x01, 0x00];
        assert_eq!(decompress(&stream, 11).unwrap(), b"xxxxxxxxxxx");
    }

    #[test]
    fn rejects_overflow() {
        // Literal run of 32 bytes but only 3 provided in input.
        assert_eq!(decompress(&[0x1f, 1, 2], 32), None);
        // Literal run overflows output.
        assert_eq!(decompress(&[0x1f; 40], 4), None);
        // Truncated backref: ctrl >= 32 but no offset byte.
        assert_eq!(decompress(&[0x20], 10), None);
        // Backref pointing before the start of the output.
        assert_eq!(decompress(&[0x00, b'a', 0x20, 0xff], 4), None);
        // Backref run overflows output.
        assert_eq!(decompress(&[0x00, b'a', 0x20, 0x00], 1), None);
    }

    #[test]
    fn roundtrip_zeros() {
        let data = vec![0u8; 4096];
        let c = compress(&data).unwrap();
        assert_eq!(decompress(&c, data.len()).unwrap(), data);
    }

    #[test]
    fn roundtrip_random() {
        // Deterministic pseudo-random data, mixed with runs of repeats.
        let mut data = Vec::new();
        let mut state = 0x12345678u32;
        for i in 0..8192 {
            state = state.wrapping_mul(1103515245).wrapping_add(12345);
            let b = if i % 17 == 0 {
                0x41
            } else {
                ((state >> 24) & 0xff) as u8
            };
            data.push(b);
        }
        if let Some(c) = compress(&data) {
            assert_eq!(decompress(&c, data.len()).unwrap(), data);
        }
        // A genuinely repetitive payload must compress.
        let mut data = Vec::new();
        let mut state = 0x12345678u32;
        for _ in 0..4096 {
            state = state.wrapping_mul(1103515245).wrapping_add(12345);
            let word = (state & 0xffff) as u8;
            data.extend_from_slice(&[word, word, word, word, word]);
        }
        let c = compress(&data).unwrap();
        assert_eq!(decompress(&c, data.len()).unwrap(), data);
    }

    #[test]
    fn roundtrip_repetitive() {
        let data = b"abababababababababababababababababababab".to_vec();
        let c = compress(&data).unwrap();
        assert_eq!(decompress(&c, data.len()).unwrap(), data);
    }

    #[test]
    fn roundtrip_small() {
        for len in 0..64usize {
            let data: Vec<u8> = (0..len).map(|i| ((i * 7) & 0xff) as u8).collect();
            if let Some(c) = compress(&data) {
                assert_eq!(decompress(&c, len).unwrap(), data, "len={}", len);
            }
        }
    }

    #[test]
    fn literal_only() {
        // Compressing high-entropy short data yields a literal run.
        let data = b"hello world".to_vec();
        // len = 11, too small / incompressible: the reference compressor's
        // output buffer overflows and it returns 0 -> verbatim storage.
        assert_eq!(compress(&data), None);
        // A longer incompressible payload is also rejected.
        let data: Vec<u8> = (0..100).map(|i| ((i * 37) & 0xff) as u8).collect();
        assert_eq!(compress(&data), None);
    }

    #[test]
    fn probe_abab() {
        let data = b"abababababababababababababababababababab".to_vec();
        let c = compress(&data).unwrap();
        assert_eq!(decompress(&c, data.len()).unwrap(), data);
    }
}
