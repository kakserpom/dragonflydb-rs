//! CRC-64/Jones, byte-for-byte compatible with `dragonfly/src/redis/crc64.c`.
//!
//! The reference is pycrc "bit-by-bit-fast" with `Poly = 0xad93d23594c935a9`,
//! `ReflectIn = True`, `ReflectOut = True`, `XorIn = 0`, `XorOut = 0`, and a
//! final reflection step. That is exactly the reflected-normal-form (LSB-first)
//! table algorithm below, using the bit-reversed polynomial. Check values from
//! `crc64.c`:
//! ```text
//! crc64(0, "123456789", 9)  == 0xe9c6d914c4b8d9ca
//! ```

/// Bit-reversed form of the polynomial `0xad93d23594c935a9`.
const POLY_REFLECTED: u64 = 0x95ac9329ac4bc9b5;

static TABLE: [u64; 256] = build_table();

const fn build_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u64;
        let mut b = 0;
        while b < 8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ POLY_REFLECTED;
            } else {
                crc >>= 1;
            }
            b += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// Update a running CRC with one byte (`crcspeed64native` step).
#[inline]
fn update(mut crc: u64, byte: u8) -> u64 {
    crc = TABLE[((crc ^ u64::from(byte)) & 0xff) as usize] ^ (crc >> 8);
    crc
}

/// Compute `crc64(0, data, len)` as used by the RDB DUMP footer.
pub fn crc64(data: &[u8]) -> u64 {
    data.iter().fold(0u64, |crc, &b| update(crc, b))
}

/// Bit-for-bit reference translation of `_crc64` (MSB-first, final reflect),
/// kept for tests to prove the fast table matches the C implementation.
#[cfg(test)]
fn crc64_bitwise(mut crc: u64, data: &[u8]) -> u64 {
    for &byte in data {
        let c = byte;
        let mut i = 0x01u8;
        while i != 0 {
            // `bit = crc & top` then `if (c & i) bit = !bit;` where `!` is C's
            // logical not (0/1): bit ends up set iff exactly one of the top
            // crc bit and the data bit is set.
            let bit = match (crc & 0x8000_0000_0000_0000) != 0 {
                b if c & i != 0 => !b,
                b => b,
            };
            crc <<= 1;
            if bit {
                crc ^= 0xad93d23594c935a9;
            }
            i <<= 1;
        }
    }
    crc &= 0xffff_ffff_ffff_ffff;
    // crc_reflect(crc, 64)
    crc.reverse_bits()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_value_123456789() {
        assert_eq!(crc64(b"123456789"), 0xe9c6d914c4b8d9ca);
    }

    #[test]
    fn matches_reference_bitwise() {
        for len in 0..=64usize {
            let data: Vec<u8> = (0..len)
                .map(|i| (i.wrapping_mul(31).wrapping_add(7)) as u8)
                .collect();
            assert_eq!(crc64(&data), crc64_bitwise(0, &data), "len {len}");
        }
        // `char li[] = "..."` is hashed as `sizeof(li)`, i.e. including the
        // trailing NUL byte.
        let mut lorem = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.".to_vec();
        lorem.push(0);
        assert_eq!(crc64(&lorem), 0xc7794709e69683b3);
        assert_eq!(crc64(&lorem), crc64_bitwise(0, &lorem));
    }

    #[test]
    fn dump_footer_vectors() {
        // From generic_family_test.cc `Dump`: crc covers everything before the
        // crc field, i.e. type + payload + 2-byte version.
        let string = [0x00, 0xc0, 0x13, 0x09, 0x00];
        assert_eq!(crc64(&string), 0x6e35f6684d6f1323);

        let list = [
            0x12, 0x01, 0x02, 0x09, 0x09, 0x00, 0x00, 0x00, 0x01, 0x00, 0x14, 0x01, 0xff, 0x09,
            0x00,
        ];
        assert_eq!(crc64(&list), 0x3b2574b4f836bdfb);

        let hash = [
            0x10, 0x0c, 0x0c, 0x00, 0x00, 0x00, 0x02, 0x00, 0x13, 0x01, 0xc4, 0xd2, 0x02, 0xff,
            0x09, 0x00,
        ];
        assert_eq!(crc64(&hash), 0xc74f230fa4734d68);
    }
}
