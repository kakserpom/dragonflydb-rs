use crate::commands::{
    integer, Command, OpContext, ShardPart, KeyRange, FLAG_DENYOOM, FLAG_FAST, FLAG_MULTI_KEY,
    FLAG_READONLY, FLAG_WRITE,
};
use crate::core::compact::CompactString;
use crate::core::PrimeValue;
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::{parse_i64, parse_u64};

const OFFSET_FACTOR: u32 = 8;
const K_MAX_STR_LEN: u64 = 1 << 28;

// ---------------------------------------------------------------------------
// Bit primitives (port of bitops_family.cc)
// ---------------------------------------------------------------------------

fn get_bit_index(offset: u32) -> u32 {
    offset % OFFSET_FACTOR
}

fn get_normalized_bit_index(offset: u32) -> u32 {
    (OFFSET_FACTOR - 1) - get_bit_index(offset)
}

fn get_byte_index(offset: u32) -> usize {
    (offset / OFFSET_FACTOR) as usize
}

fn check_bit_status(byte: u8, offset: u32) -> bool {
    byte & (1 << offset) != 0
}

/// `str[GetByteIndex(offset)]`; the caller guarantees the index is in range.
fn get_byte_value(str: &[u8], offset: u32) -> u8 {
    str[get_byte_index(offset)]
}

fn turn_bit_on(on: u8, offset: u32) -> u8 {
    on | (1 << offset)
}

fn turn_bit_off(on: u8, offset: u32) -> u8 {
    on & !(1 << offset)
}

fn get_bit_value(entry: &[u8], offset: u32) -> bool {
    check_bit_status(get_byte_value(entry, offset), get_normalized_bit_index(offset))
}

/// Set the bit at `offset` to `bit_value`; returns the old value.
fn set_bit_value(offset: u32, bit_value: bool, entry: &mut [u8]) -> bool {
    let old_value = get_bit_value(entry, offset);
    let byte = get_byte_value(entry, offset);
    let bit_index = get_normalized_bit_index(offset);
    entry[get_byte_index(offset)] = if bit_value {
        turn_bit_on(byte, bit_index)
    } else {
        turn_bit_off(byte, bit_index)
    };
    old_value
}

fn count_bits_range(byte: u8, from: u32, to: u32) -> u32 {
    let mut count = 0;
    for i in from..to {
        if check_bit_status(byte, get_normalized_bit_index(i)) {
            count += 1;
        }
    }
    count
}

fn count_bit_set_by_byte_indices(at: &[u8], start: usize, end: usize) -> usize {
    if start >= end {
        return 0;
    }
    let end = end.min(at.len());
    at[start..end].iter().map(|&b| b.count_ones() as usize).sum()
}

/// Count bits in the inclusive bit range `[front, back]`. Caller guarantees
/// `0 <= front <= back < at.len() * OFFSET_FACTOR`.
fn count_bit_set_by_bit_indices(at: &[u8], front: usize, back: usize) -> usize {
    let front_byte = front / OFFSET_FACTOR as usize;
    let back_byte = back / OFFSET_FACTOR as usize;
    let front_bit = (front % OFFSET_FACTOR as usize) as u32;
    let back_bit_end = (back % OFFSET_FACTOR as usize) as u32 + 1;

    if front_byte == back_byte {
        return count_bits_range(at[front_byte], front_bit, back_bit_end) as usize;
    }
    let mut count = count_bits_range(at[front_byte], front_bit, OFFSET_FACTOR) as usize;
    count += count_bit_set_by_byte_indices(at, front_byte + 1, back_byte);
    count += count_bits_range(at[back_byte], 0, back_bit_end) as usize;
    count
}

/// Normalized offset of `offset` in `size`: negative offsets are from the end,
/// the result is clamped to `[0, size]`.
fn normalized_offset(size: i64, offset: i64) -> i64 {
    let offset = if offset < 0 { size + offset } else { offset };
    offset.clamp(0, size)
}

/// Port of `CountBitSet` (bitops_family.cc): count on bits (`bits`) or bytes.
fn count_bit_set(str: &[u8], start: i64, end: i64, bits: bool) -> usize {
    let strlen = if bits { (str.len() as i64) * 8 } else { str.len() as i64 };
    if strlen == 0 {
        return 0;
    }
    // Both-negative inverted range is empty; without this, clamping pulls both
    // up to 0 on short strings and counts a spurious byte/bit.
    if start < 0 && end < 0 && start > end {
        return 0;
    }
    let mut start = if start < 0 { strlen + start } else { start };
    let mut end = if end < 0 { strlen + end } else { end };
    start = start.max(0);
    end = end.clamp(0, strlen - 1);
    if start > end {
        return 0;
    }
    if bits {
        count_bit_set_by_bit_indices(str, start as usize, end as usize)
    } else {
        count_bit_set_by_byte_indices(str, start as usize, (end + 1) as usize)
    }
}

/// Position (MSB is bit 0) of the leftmost bit equal to `value` in `byte`,
/// or 8 if there is none.
fn get_first_bit_with_value_in_byte(byte: u8, value: bool) -> u32 {
    if value {
        byte.leading_zeros()
    } else {
        byte.leading_ones()
    }
}

fn find_first_bit_with_value_as_bit(value_str: &[u8], bit_value: bool, start: i64, end: i64) -> i64 {
    let mut i = start;
    while i <= end {
        if get_byte_index(i as u32) >= value_str.len() {
            break;
        }
        let current_byte = get_byte_value(value_str, i as u32);
        let current_bit = check_bit_status(current_byte, get_normalized_bit_index(i as u32));
        if current_bit == bit_value {
            return i;
        }
        i += 1;
    }
    -1
}

fn find_first_bit_with_value_as_byte(value_str: &[u8], bit_value: bool, start: i64, end: i64) -> i64 {
    let mut i = start;
    while i <= end {
        if i as usize >= value_str.len() {
            break;
        }
        let current_byte = value_str[i as usize];
        let not_found_byte = if bit_value { 0 } else { u8::MAX };
        if current_byte == not_found_byte {
            i += 1;
            continue;
        }
        return i * 8 + get_first_bit_with_value_in_byte(current_byte, bit_value) as i64;
    }
    -1
}

fn find_first_bit_with_value(value: &[u8], bit_value: bool, start: i64, end: i64, as_bit: bool) -> i64 {
    let mut size = value.len() as i64;
    if as_bit {
        size *= OFFSET_FACTOR as i64;
    }
    let normalized_start = normalized_offset(size, start);
    let normalized_end = normalized_offset(size, end);
    if normalized_start > normalized_end {
        return -1;
    }
    let position = if as_bit {
        find_first_bit_with_value_as_bit(value, bit_value, normalized_start, normalized_end)
    } else {
        find_first_bit_with_value_as_byte(value, bit_value, normalized_start, normalized_end)
    };
    if position == -1 && !bit_value && start >= 0 && (start as usize) < value.len() && end == i64::MAX
    {
        // Returning bit-size of the value, compatible with Redis (but a weird API).
        (value.len() * OFFSET_FACTOR as usize) as i64
    } else {
        position
    }
}

// ---------------------------------------------------------------------------
// BITOP helpers
// ---------------------------------------------------------------------------

/// `at >= s.size() ? 0 : s[at]` for out-of-bounds access during bit ops.
fn get_byte_at(s: &[u8], at: usize) -> u8 {
    if at >= s.len() { 0 } else { s[at] }
}

/// Combine `values` with a single binary op, growing to the longest input.
fn bit_op_string(op: u8, values: &[Vec<u8>], new_value: &mut [u8]) {
    for (i, out) in new_value.iter_mut().enumerate() {
        let mut new_entry = apply_op(op, get_byte_at(&values[0], i), get_byte_at(&values[1], i));
        for v in values.iter().skip(2) {
            new_entry = apply_op(op, new_entry, get_byte_at(v, i));
            if should_skip(op, new_entry) {
                break;
            }
        }
        *out = new_entry;
    }
}

fn apply_op(op: u8, left: u8, right: u8) -> u8 {
    match op {
        b'&' => left & right,
        b'|' => left | right,
        _ => left ^ right,
    }
}

fn should_skip(op: u8, byte: u8) -> bool {
    match op {
        b'&' => byte == 0,
        b'|' => byte == 0xff,
        _ => false,
    }
}

/// Port of `RunBitOperationOnValues` (bitops_family.cc). The combined length is
/// the longest input; a single value is returned unchanged (except for NOT).
fn run_bit_operation_on_values(op: &[u8], values: &[Vec<u8>]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }
    if op == b"NOT" {
        return values[0].iter().map(|b| !*b).collect();
    }
    let mut max_len = values[0].len();
    let mut max_len_index = 0;
    for (i, v) in values.iter().enumerate().skip(1) {
        if v.len() > max_len {
            max_len = v.len();
            max_len_index = i;
        }
    }
    if values.len() == 1 {
        return values[0].clone();
    }
    let mut new_value = match op {
        b"OR" => values[max_len_index].clone(),
        _ => vec![0u8; max_len],
    };
    bit_op_string(match op {
        b"AND" => b'&',
        b"OR" => b'|',
        _ => b'^',
    }, values, &mut new_value);
    new_value
}

// ---------------------------------------------------------------------------
// BITFIELD
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum EncodingType {
    Uint,
    Int,
}

#[derive(Clone, Copy)]
struct CommonAttr {
    etype: EncodingType,
    bits: u32,
    offset: u32,
}

#[derive(Clone, Copy)]
enum Policy {
    Wrap,
    Sat,
    Fail,
}

enum SubCmd {
    Get(CommonAttr),
    Set(CommonAttr, i64),
    IncrBy(CommonAttr, i64),
    Overflow(Policy),
}

fn invalid_bitfield_type() -> RespError {
    RespError::new(
        "ERR invalid bitfield type. use something like i16 u8. note that u64 is not supported but \
         i64 is.",
    )
}

fn bit_offset_out_of_range() -> RespError {
    RespError::new("ERR bit offset is not an integer or out of range")
}

fn bit_arg_error() -> RespError {
    RespError::new("ERR The bit argument must be 1 or 0")
}

fn bitfield_ro_get_only() -> RespError {
    RespError::new("ERR BITFIELD_RO only supports the GET subcommand")
}

fn is_bit_write_in_range(bit_offset: u64, bit_size: u64) -> bool {
    bit_offset < K_MAX_STR_LEN * 8 && (bit_offset + bit_size - 1) / 8 < K_MAX_STR_LEN
}

fn parse_common_attr(args: &[Vec<u8>], i: &mut usize, is_write: bool) -> Result<CommonAttr, RespError> {
    if *i >= args.len() {
        return Err(RespError::syntax());
    }
    let encoding = &args[*i];
    *i += 1;
    if encoding.is_empty() {
        return Err(RespError::syntax());
    }
    let etype = match encoding[0] {
        b'u' => EncodingType::Uint,
        b'i' => EncodingType::Int,
        _ => return Err(invalid_bitfield_type()),
    };
    let bits_s = &encoding[1..];
    if bits_s.is_empty() {
        return Err(RespError::syntax());
    }
    if !bits_s.iter().all(|c| c.is_ascii_digit()) {
        return Err(invalid_bitfield_type());
    }
    let bits: u64 = match std::str::from_utf8(bits_s) {
        Ok(s) => s.parse().map_err(|_| RespError::syntax())?,
        Err(_) => return Err(RespError::syntax()),
    };
    if bits == 0 || bits > 64 {
        return Err(invalid_bitfield_type());
    }
    if bits == 64 && etype == EncodingType::Uint {
        return Err(invalid_bitfield_type());
    }
    if *i >= args.len() {
        return Err(RespError::syntax());
    }
    let offset_arg = &args[*i];
    *i += 1;
    let (is_proxy, num) = match offset_arg.strip_prefix(b"#") {
        Some(rest) => (true, rest),
        None => (false, offset_arg.as_slice()),
    };
    let offset: u64 = match std::str::from_utf8(num) {
        Ok(s) => s.parse().map_err(|_| RespError::syntax())?,
        Err(_) => return Err(RespError::syntax()),
    };
    let offset = if is_proxy {
        if offset > (u32::MAX as u64) / bits {
            return Err(bit_offset_out_of_range());
        }
        offset * bits
    } else {
        offset
    };
    let in_range = if is_write {
        is_bit_write_in_range(offset, bits)
    } else {
        offset <= u32::MAX as u64
    };
    if !in_range {
        return Err(bit_offset_out_of_range());
    }
    Ok(CommonAttr { etype, bits: bits as u32, offset: offset as u32 })
}

fn parse_to_command_list(
    args: &[Vec<u8>],
    start: usize,
    read_only: bool,
) -> Result<Vec<SubCmd>, RespError> {
    let mut result = Vec::new();
    let mut i = start;
    while i < args.len() {
        let cmd = args[i].to_ascii_uppercase();
        i += 1;
        if cmd == b"OVERFLOW" {
            if i >= args.len() {
                return Err(RespError::syntax());
            }
            let policy = match args[i].to_ascii_uppercase().as_slice() {
                b"SAT" => Policy::Sat,
                b"WRAP" => Policy::Wrap,
                b"FAIL" => Policy::Fail,
                _ => return Err(RespError::syntax()),
            };
            i += 1;
            result.push(SubCmd::Overflow(policy));
            continue;
        }
        let is_get = cmd == b"GET";
        if cmd != b"GET" && cmd != b"SET" && cmd != b"INCRBY" {
            return Err(RespError::syntax());
        }
        let is_write = !read_only && !is_get;
        let attr = parse_common_attr(args, &mut i, is_write)?;
        if is_get {
            result.push(SubCmd::Get(attr));
            continue;
        }
        if read_only {
            return Err(bitfield_ro_get_only());
        }
        if i >= args.len() {
            return Err(RespError::syntax());
        }
        let value = parse_i64(&args[i]).ok_or_else(RespError::syntax)?;
        i += 1;
        if cmd == b"SET" {
            result.push(SubCmd::Set(attr, value));
        } else {
            result.push(SubCmd::IncrBy(attr, value));
        }
    }
    Ok(result)
}

fn uint_overflow(incr: i64, total_bits: u32, policy: Policy, value: &mut i64) -> bool {
    let max: u64 = (1u64 << total_bits) - 1;
    let sum = (incr as u64).wrapping_add(*value as u64);
    if sum > max {
        match policy {
            Policy::Wrap => *value = (sum & max) as i64,
            Policy::Sat => *value = max as i64,
            Policy::Fail => {
                *value = 0;
                return false;
            }
        }
        return true;
    }
    *value = sum as i64;
    true
}

fn int_overflow(total_bits: u32, incr: i64, policy: Policy, value: &mut i64) -> bool {
    let int_max = i64::MAX;
    let max: i64 = if total_bits == 64 {
        int_max
    } else {
        (1i64 << (total_bits - 1)) - 1
    };
    let min: i64 = (-max) - 1;

    let switch_overflow = |sat_case: i64, value: &mut i64| -> bool {
        match policy {
            Policy::Wrap => {
                let msb: u64 = 1u64 << (total_bits - 1);
                let mut c = (*value as u64).wrapping_add(incr as u64);
                if total_bits < 64 {
                    let mask = u64::MAX << total_bits;
                    if c & msb != 0 {
                        c |= mask;
                    } else {
                        c &= !mask;
                    }
                }
                *value = c as i64;
            }
            Policy::Sat => *value = sat_case,
            Policy::Fail => {
                *value = 0;
                return false;
            }
        }
        true
    };

    let maxincr: i64 = (max as u64).wrapping_sub(*value as u64) as i64;
    let minincr: i64 = min.wrapping_sub(*value);

    if *value > max
        || (total_bits != 64 && incr > maxincr)
        || (*value >= 0 && incr > 0 && incr > maxincr)
    {
        return switch_overflow(max, value);
    }
    if *value < min
        || (total_bits != 64 && incr < minincr)
        || (*value < 0 && incr < 0 && incr < minincr)
    {
        return switch_overflow(min, value);
    }

    *value = value.wrapping_add(incr);
    true
}

/// Port of `Get::ApplyTo` (bitops_family.cc).
fn bitfield_get(attr: &CommonAttr, bitfield: &[u8]) -> i64 {
    let offset = attr.offset as u64;
    let total_bytes = bitfield.len() as u64;
    let last_byte_offset = (offset + attr.bits as u64 - 1) / 8;
    if offset / 8 >= total_bytes {
        return 0;
    }
    let result_str: Vec<u8>;
    let result_bytes: &[u8] = if last_byte_offset >= total_bytes {
        result_str = {
            let mut b = bitfield.to_vec();
            b.resize(last_byte_offset as usize + 1, 0);
            b
        };
        &result_str
    } else {
        bitfield
    };

    let is_negative = get_bit_value(bitfield, attr.offset);
    let mut result: i64 = 0;
    let mut lsb = offset + attr.bits as u64 - 1;
    for i in 0..attr.bits as u64 {
        let byte = get_byte_value(result_bytes, lsb as u32);
        let index = get_normalized_bit_index(lsb as u32);
        let old_bit = check_bit_status(byte, index);
        result |= (old_bit as i64) << i;
        lsb = lsb.wrapping_sub(1);
    }
    if is_negative && attr.etype == EncodingType::Int && result > 0 && attr.bits < 64 {
        result |= -1i64 ^ ((1i64 << attr.bits) - 1);
    }
    result
}

/// Port of `Set::ApplyTo` (bitops_family.cc). Returns the old field value, or
/// None if the value was rejected by the overflow FAIL policy.
fn bitfield_set(attr: &CommonAttr, set_value: i64, policy: Policy, bitfield: &mut Vec<u8>) -> Option<i64> {
    let offset = attr.offset as u64;
    let total_bytes = bitfield.len() as u64;
    let last_byte_offset = (offset + attr.bits as u64 - 1) / 8 + 1;
    if last_byte_offset > total_bytes {
        bitfield.resize(last_byte_offset as usize, 0);
    }
    let mut set_value = set_value;
    let ok = if attr.etype == EncodingType::Uint {
        uint_overflow(0, attr.bits, policy, &mut set_value)
    } else {
        int_overflow(attr.bits, 0, policy, &mut set_value)
    };
    if !ok {
        return None;
    }

    let mut lsb = offset + attr.bits as u64 - 1;
    let mut old_value: i64 = 0;
    let is_negative = get_bit_value(bitfield, attr.offset);
    for i in 0..attr.bits as u64 {
        let bit_value = (set_value >> i) & 1 != 0;
        let byte = get_byte_value(bitfield, lsb as u32);
        let index = get_normalized_bit_index(lsb as u32);
        let old_bit = check_bit_status(byte, index);
        let byte = if bit_value {
            turn_bit_on(byte, index)
        } else {
            turn_bit_off(byte, index)
        };
        bitfield[get_byte_index(lsb as u32)] = byte;
        old_value |= (old_bit as i64) << i;
        lsb = lsb.wrapping_sub(1);
    }
    if is_negative && attr.etype == EncodingType::Int && old_value > 0 && attr.bits < 64 {
        old_value |= -1i64 ^ ((1i64 << attr.bits) - 1);
    }
    Some(old_value)
}

/// Port of `IncrBy::ApplyTo` (bitops_family.cc).
fn bitfield_incrby(attr: &CommonAttr, incr_value: i64, policy: Policy, bitfield: &mut Vec<u8>) -> Option<i64> {
    let mut res = bitfield_get(attr, bitfield);
    let offset = attr.offset as u64;
    let total_bytes = bitfield.len() as u64;
    let last_byte_offset = (offset + attr.bits as u64 - 1) / 8;
    if last_byte_offset >= total_bytes {
        bitfield.resize(last_byte_offset as usize + 1, 0);
    }
    let ok = if attr.etype == EncodingType::Uint {
        uint_overflow(incr_value, attr.bits, policy, &mut res)
    } else {
        int_overflow(attr.bits, incr_value, policy, &mut res)
    };
    if !ok {
        return None;
    }
    let _ = bitfield_set(attr, res, policy, bitfield);
    Some(res)
}

fn apply_subcmd(
    sub: &SubCmd,
    policy: &mut Policy,
    value: &mut Vec<u8>,
    should_commit: &mut bool,
) -> Option<Option<i64>> {
    match *sub {
        SubCmd::Overflow(p) => {
            *policy = p;
            None
        }
        SubCmd::Get(attr) => Some(Some(bitfield_get(&attr, value))),
        SubCmd::Set(attr, v) => {
            *should_commit = true;
            Some(bitfield_set(&attr, v, *policy, value))
        }
        SubCmd::IncrBy(attr, v) => {
            *should_commit = true;
            Some(bitfield_incrby(&attr, v, *policy, value))
        }
    }
}

fn exec_bitfield_generic(ctx: &mut OpContext, read_only: bool) -> CmdResult {
    if ctx.args.len() < 3 {
        return CmdResult::Ok(RespValue::Array(vec![]));
    }
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let subcmds = match parse_to_command_list(ctx.args, key_idx + 1, read_only) {
        Ok(c) => c,
        Err(e) => return CmdResult::Err(e),
    };

    let mut value: Vec<u8>;
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => value = s.as_bytes().to_vec(),
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => value = Vec::new(),
    }

    let mut results: Vec<Option<i64>> = Vec::new();
    let mut policy = Policy::Wrap;
    let mut should_commit = false;
    for sub in &subcmds {
        if let Some(r) = apply_subcmd(sub, &mut policy, &mut value, &mut should_commit) {
            results.push(r);
        }
    }

    if should_commit {
        if value.is_empty() {
            ctx.db.remove(key);
        } else {
            ctx.db.insert(
                CompactString::from_bytes(key),
                PrimeValue::Str(CompactString::from_bytes(&value)),
            );
        }
    }

    CmdResult::Ok(RespValue::Array(
        results
            .into_iter()
            .map(|r| match r {
                Some(v) => RespValue::Integer(v),
                None => RespValue::Nil,
            })
            .collect(),
    ))
}

fn exec_bitfield(ctx: &mut OpContext) -> CmdResult {
    exec_bitfield_generic(ctx, false)
}

fn exec_bitfield_ro(ctx: &mut OpContext) -> CmdResult {
    exec_bitfield_generic(ctx, true)
}

// ---------------------------------------------------------------------------
// BITOP
// ---------------------------------------------------------------------------

fn exec_bitop(ctx: &mut OpContext) -> CmdResult {
    let op = ctx.args[1].to_ascii_uppercase();
    if !matches!(op.as_slice(), b"AND" | b"OR" | b"XOR" | b"NOT") {
        return CmdResult::Err(RespError::syntax());
    }
    if op == b"NOT" && ctx.args.len() > 4 {
        return CmdResult::Err(RespError::syntax());
    }
    let dest_key_idx = ctx.first_key_idx;
    let total_keys = ctx.args.len() - dest_key_idx;

    let mut values: Vec<Vec<u8>> = Vec::new();
    for &ki in ctx.owned_keys {
        if ki == dest_key_idx {
            continue;
        }
        match ctx.db.find(&ctx.args[ki], ctx.now_ms) {
            Some(PrimeValue::Str(s)) => values.push(s.as_bytes().to_vec()),
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => {}
        }
    }

    if ctx.owned_keys.len() == total_keys {
        let result = run_bit_operation_on_values(&op, &values);
        let len = result.len() as i64;
        let dest = &ctx.args[dest_key_idx];
        if result.is_empty() {
            ctx.db.remove(dest);
        } else {
            ctx.db.clear_expiry(dest);
            ctx.db.insert(
                CompactString::from_bytes(dest),
                PrimeValue::Str(CompactString::from_bytes(&result)),
            );
        }
        return CmdResult::Ok(integer(len));
    }

    // Multi-shard: return this shard's partial result. For NOT the merge inverts,
    // so contribute the raw source value; for AND/OR/XOR contribute the combined
    // value of this shard's sources. Shards without any source key (or a missing
    // NOT source) contribute nothing.
    if values.is_empty() {
        return CmdResult::Ok(RespValue::Nil);
    }
    let partial = if op == b"NOT" {
        values[0].clone()
    } else {
        run_bit_operation_on_values(&op, &values)
    };
    CmdResult::Ok(RespValue::Bulk(partial))
}

fn merge_bitop(parts: &[ShardPart], args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    let op = args[1].to_ascii_uppercase();
    let dest_key = args[keys[0]].clone();
    let mut values: Vec<Vec<u8>> = Vec::new();
    for p in parts {
        match &p.result {
            CmdResult::Ok(RespValue::Bulk(b)) => values.push(b.clone()),
            CmdResult::Ok(RespValue::Nil) => {}
            CmdResult::Err(e) => return CmdResult::Err(e.clone()),
            _ => return CmdResult::Err(RespError::new("ERR internal: bad bitop shard result")),
        }
    }
    let result = run_bit_operation_on_values(&op, &values);
    let len = result.len() as i64;
    let value = if result.is_empty() {
        None
    } else {
        Some(PrimeValue::Str(CompactString::from_bytes(&result)))
    };
    CmdResult::deferred_store(dest_key, value, integer(len))
}

// ---------------------------------------------------------------------------
// GETBIT / SETBIT
// ---------------------------------------------------------------------------

fn parse_bit_offset(args: &[Vec<u8>], i: usize) -> Result<u32, RespError> {
    let offset = parse_u64(&args[i]).ok_or_else(RespError::integer)?;
    if offset > u32::MAX as u64 {
        return Err(RespError::integer());
    }
    Ok(offset as u32)
}

fn exec_getbit(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let offset = match parse_bit_offset(ctx.args, key_idx + 1) {
        Ok(o) => o,
        Err(e) => return CmdResult::Err(e),
    };
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => {
            if get_byte_index(offset) >= s.len() {
                CmdResult::Ok(integer(0))
            } else {
                CmdResult::Ok(integer(get_bit_value(s.as_bytes(), offset) as i64))
            }
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

fn exec_setbit(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let offset = match parse_bit_offset(ctx.args, key_idx + 1) {
        Ok(o) => o,
        Err(e) => return CmdResult::Err(e),
    };
    if !is_bit_write_in_range(offset as u64, 1) {
        return CmdResult::Err(bit_offset_out_of_range());
    }
    let bit_value = match parse_i64(&ctx.args[key_idx + 2]) {
        Some(0) => false,
        Some(1) => true,
        _ => return CmdResult::Err(RespError::integer()),
    };

    let old = match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => s.as_bytes().to_vec(),
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => Vec::new(),
    };
    let mut bytes = old;
    let byte_index = get_byte_index(offset);
    if byte_index >= bytes.len() {
        bytes.resize(byte_index + 1, 0);
    }
    let old_bit = set_bit_value(offset, bit_value, &mut bytes);
    ctx.db.insert(
        CompactString::from_bytes(key),
        PrimeValue::Str(CompactString::from_bytes(&bytes)),
    );
    CmdResult::Ok(integer(old_bit as i64))
}

// ---------------------------------------------------------------------------
// BITCOUNT
// ---------------------------------------------------------------------------

fn exec_bitcount(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let mut i = key_idx + 1;
    let mut start: i64 = 0;
    let mut end: i64 = i64::MAX;
    let mut as_bit = false;
    if i < ctx.args.len() {
        start = match parse_i64(&ctx.args[i]) {
            Some(v) => v,
            None => return CmdResult::Err(RespError::integer()),
        };
        i += 1;
        if i < ctx.args.len() {
            end = match parse_i64(&ctx.args[i]) {
                Some(v) => v,
                None => return CmdResult::Err(RespError::integer()),
            };
            i += 1;
        } else {
            return CmdResult::Err(RespError::syntax());
        }
    }
    if i < ctx.args.len() {
        match ctx.args[i].to_ascii_uppercase().as_slice() {
            b"BYTE" => as_bit = false,
            b"BIT" => as_bit = true,
            _ => return CmdResult::Err(RespError::syntax()),
        }
        i += 1;
    }
    if i < ctx.args.len() {
        return CmdResult::Err(RespError::syntax());
    }

    let count = match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => count_bit_set(s.as_bytes(), start, end, as_bit),
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => 0,
    };
    CmdResult::Ok(integer(count as i64))
}

// ---------------------------------------------------------------------------
// BITPOS
// ---------------------------------------------------------------------------

fn exec_bitpos(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let bit_value = match parse_i64(&ctx.args[key_idx + 1]) {
        Some(0) => false,
        Some(1) => true,
        _ => return CmdResult::Err(bit_arg_error()),
    };
    let mut i = key_idx + 2;
    let mut start: i64 = 0;
    let mut end: i64 = i64::MAX;
    let mut as_bit = false;
    if i < ctx.args.len() {
        start = match parse_i64(&ctx.args[i]) {
            Some(v) => v,
            None => return CmdResult::Err(RespError::integer()),
        };
        i += 1;
    }
    if i < ctx.args.len() {
        end = match parse_i64(&ctx.args[i]) {
            Some(v) => v,
            None => return CmdResult::Err(RespError::integer()),
        };
        i += 1;
    }
    if i < ctx.args.len() {
        match ctx.args[i].to_ascii_uppercase().as_slice() {
            b"BIT" => as_bit = true,
            b"BYTE" => as_bit = false,
            _ => return CmdResult::Err(RespError::syntax()),
        }
        i += 1;
    }
    if i < ctx.args.len() {
        return CmdResult::Err(RespError::syntax());
    }

    let position = match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => {
            find_first_bit_with_value(s.as_bytes(), bit_value, start, end, as_bit)
        }
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => {
            if bit_value {
                -1
            } else {
                0
            }
        }
    };
    CmdResult::Ok(integer(position))
}

// ---------------------------------------------------------------------------
// Command definitions
// ---------------------------------------------------------------------------

pub static CMD_BITCOUNT: Command = Command {
    name: "BITCOUNT",
    arity: -2,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_bitcount,
    merge: None,
};
pub static CMD_BITPOS: Command = Command {
    name: "BITPOS",
    arity: -3,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_bitpos,
    merge: None,
};
pub static CMD_BITFIELD: Command = Command {
    name: "BITFIELD",
    arity: -2,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    key_range: KeyRange::ONE,
    exec: exec_bitfield,
    merge: None,
};
pub static CMD_BITFIELD_RO: Command = Command {
    name: "BITFIELD_RO",
    arity: -2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_bitfield_ro,
    merge: None,
};
pub static CMD_BITOP: Command = Command {
    name: "BITOP",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_MULTI_KEY,
    key_range: KeyRange { first: 2, last: 0, step: 1 },
    exec: exec_bitop,
    merge: Some(merge_bitop),
};
pub static CMD_GETBIT: Command = Command {
    name: "GETBIT",
    arity: 3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_getbit,
    merge: None,
};
pub static CMD_SETBIT: Command = Command {
    name: "SETBIT",
    arity: 4,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    key_range: KeyRange::ONE,
    exec: exec_setbit,
    merge: None,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db::DbSlice;

    fn set(db: &mut DbSlice, key: &str, value: &[u8]) {
        db.insert(
            CompactString::from_bytes(key.as_bytes()),
            PrimeValue::Str(CompactString::from_bytes(value)),
        );
    }

    fn get(db: &mut DbSlice, key: &str) -> Option<Vec<u8>> {
        match db.find(key.as_bytes(), 0) {
            Some(PrimeValue::Str(s)) => Some(s.as_bytes().to_vec()),
            _ => None,
        }
    }

    fn assert_str_store(value: Option<PrimeValue>, expected: &[u8]) {
        match value {
            Some(PrimeValue::Str(s)) => assert_eq!(s.as_bytes(), expected),
            _ => panic!("expected string deferred store, got {:?}", value),
        }
    }

    /// Dispatch a command against a single-shard DbSlice, mirroring `Run(...)`
    /// in the C++ test. `argv[0]` is the command name.
    fn dispatch(db: &mut DbSlice, argv: &[Vec<u8>]) -> CmdResult {
        let (exec, first_key_idx, owned): (fn(&mut OpContext) -> CmdResult, usize, Vec<usize>) =
            match argv[0].as_slice() {
                b"SETBIT" => (exec_setbit, 1, vec![1]),
                b"GETBIT" => (exec_getbit, 1, vec![1]),
                b"BITCOUNT" => (exec_bitcount, 1, vec![1]),
                b"BITPOS" => (exec_bitpos, 1, vec![1]),
                b"BITFIELD" => (exec_bitfield, 1, vec![1]),
                b"BITFIELD_RO" => (exec_bitfield_ro, 1, vec![1]),
                b"BITOP" => (exec_bitop, 2, (2..argv.len()).collect()),
                _ => panic!("unhandled command {:?}", argv[0]),
            };
        let mut ctx = OpContext { db, args: argv, owned_keys: &owned, first_key_idx, now_ms: 0 };
        exec(&mut ctx)
    }

    /// `run!(db, cmd, args...)` builds the argv from heterogeneously-typed
    /// byte-slice expressions (byte-string literals, byte arrays, `&[u8]`).
    macro_rules! run {
        ($db:expr, $($arg:expr),+ $(,)?) => {
            dispatch($db, &[$($arg.to_vec()),+])
        };
    }

    fn int(r: CmdResult) -> i64 {
        match r.into_resp_value() {
            RespValue::Integer(v) => v,
            o => panic!("expected integer, got {o:?}"),
        }
    }

    fn dispatch_slices(db: &mut DbSlice, args: &[&[u8]]) -> CmdResult {
        dispatch(db, &args.iter().map(|a| a.to_vec()).collect::<Vec<_>>())
    }

    fn arr(r: CmdResult) -> Vec<RespValue> {
        match r.into_resp_value() {
            RespValue::Array(v) => v,
            o => panic!("expected array, got {o:?}"),
        }
    }

    fn err(r: CmdResult) -> String {
        match r {
            CmdResult::Err(e) => e.render().to_string(),
            _ => panic!("expected error"),
        }
    }

    const SYNTAX: &str = "ERR syntax error";
    const INVALID_TYPE: &str =
        "ERR invalid bitfield type. use something like i16 u8. note that u64 is not supported but \
         i64 is.";
    const BIT_ARG: &str = "ERR The bit argument must be 1 or 0";
    const BIT_OFFSET: &str = "ERR bit offset is not an integer or out of range";

    /// Old-value sequence for the bits 0..11 of "abc", taken from running on Redis.
    const EXPECTED_SETBIT: [i64; 12] = [0, 1, 1, 0, 0, 0, 0, 1, 0, 1, 1, 0];

    #[test]
    fn get_bit() {
        let mut db = DbSlice::new(0);
        set(&mut db, "foo", b"abc");
        for (i, &expected) in EXPECTED_SETBIT.iter().enumerate() {
            let o = i.to_string();
            assert_eq!(expected, int(run!(&mut db, b"GETBIT", b"foo", o.as_bytes())));
        }
        // Out-of-range bits read as 0.
        assert_eq!(0, int(run!(&mut db, b"GETBIT", b"foo", b"100")));
        // Missing key reads as 0.
        assert_eq!(0, int(run!(&mut db, b"GETBIT", b"nope", b"0")));
    }

    #[test]
    fn set_bit_existing_key() {
        let mut db = DbSlice::new(0);
        set(&mut db, "foo", b"abc");
        for (i, &expected) in EXPECTED_SETBIT.iter().enumerate() {
            let o = i.to_string();
            assert_eq!(expected, int(run!(&mut db, b"SETBIT", b"foo", o.as_bytes(), b"1")));
        }
        for (i, _) in EXPECTED_SETBIT.iter().enumerate() {
            let o = i.to_string();
            assert_eq!(1, int(run!(&mut db, b"GETBIT", b"foo", o.as_bytes())));
        }
    }

    #[test]
    fn set_bit_missing_key() {
        let mut db = DbSlice::new(0);
        for (i, _) in EXPECTED_SETBIT.iter().enumerate() {
            let o = i.to_string();
            assert_eq!(0, int(run!(&mut db, b"SETBIT", b"foo", o.as_bytes(), b"1")));
        }
        for (i, _) in EXPECTED_SETBIT.iter().enumerate() {
            let o = i.to_string();
            assert_eq!(1, int(run!(&mut db, b"GETBIT", b"foo", o.as_bytes())));
        }
    }

    #[test]
    fn set_bit_incorrect_values() {
        let mut db = DbSlice::new(0);
        assert_eq!(0, int(run!(&mut db, b"SETBIT", b"foo", b"0", b"1")));
        for v in [&b"-1"[..], &b"11"[..], &b"a"[..], &b"O"[..]] {
            assert_eq!(
                "ERR value is not an integer or out of range",
                err(run!(&mut db, b"SETBIT", b"foo", b"1", v))
            );
        }
        // Only the bit at offset 0 was changed; the failed writes left the rest untouched.
        assert_eq!(1, int(run!(&mut db, b"GETBIT", b"foo", b"0")));
        for i in 1..=4 {
            let o = i.to_string();
            assert_eq!(0, int(run!(&mut db, b"GETBIT", b"foo", o.as_bytes())));
        }
    }

    #[test]
    fn set_bit_extend_existing_key() {
        let mut db = DbSlice::new(0);
        set(&mut db, "foo", b"abc");
        assert_eq!(0, int(run!(&mut db, b"SETBIT", b"foo", b"100", b"1")));
        // 100 bits -> byte 12 -> 13 bytes.
        assert_eq!(13, get(&mut db, "foo").unwrap().len() as i64);
        assert_eq!(1, int(run!(&mut db, b"GETBIT", b"foo", b"100")));
        assert_eq!(0, int(run!(&mut db, b"GETBIT", b"foo", b"24")));
        assert_eq!(0, int(run!(&mut db, b"GETBIT", b"foo", b"99")));
        assert_eq!(EXPECTED_SETBIT[0], int(run!(&mut db, b"GETBIT", b"foo", b"0")));
        assert_eq!(EXPECTED_SETBIT[1], int(run!(&mut db, b"GETBIT", b"foo", b"1")));
        // Clearing the bit returns its old value.
        assert_eq!(1, int(run!(&mut db, b"SETBIT", b"foo", b"100", b"0")));
        assert_eq!(0, int(run!(&mut db, b"GETBIT", b"foo", b"100")));
    }

    #[test]
    fn set_bit_offset_out_of_range() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            BIT_OFFSET,
            err(run!(&mut db, b"SETBIT", b"sk", b"2200000000", b"1"))
        );
    }

    // Cumulative popcounts of "farbar" byte ranges, from Redis.
    const BYTES_BIT_COUNT: [i64; 9] = [4, 7, 11, 14, 17, 21, 21, 21, 21];

    #[test]
    fn bit_count_byte() {
        let mut db = DbSlice::new(0);
        set(&mut db, "foo", b"farbar");
        assert_eq!(0, int(run!(&mut db, b"BITCOUNT", b"foo2")));
        for (i, &c) in BYTES_BIT_COUNT.iter().enumerate() {
            let e = i.to_string();
            assert_eq!(c, int(run!(&mut db, b"BITCOUNT", b"foo", b"0", e.as_bytes())));
        }
        assert_eq!(21, int(run!(&mut db, b"BITCOUNT", b"foo")));
    }

    #[test]
    fn bit_count_byte_sub_range() {
        let mut db = DbSlice::new(0);
        set(&mut db, "foo", b"farbar");
        assert_eq!(3, int(run!(&mut db, b"BITCOUNT", b"foo", b"1", b"1")));
        assert_eq!(7, int(run!(&mut db, b"BITCOUNT", b"foo", b"1", b"2")));
        assert_eq!(4, int(run!(&mut db, b"BITCOUNT", b"foo", b"2", b"2")));
        assert_eq!(0, int(run!(&mut db, b"BITCOUNT", b"foo", b"3", b"2")));
        assert_eq!(10, int(run!(&mut db, b"BITCOUNT", b"foo", b"-3", b"-1")));
        assert_eq!(13, int(run!(&mut db, b"BITCOUNT", b"foo", b"-5", b"-2")));
        assert_eq!(0, int(run!(&mut db, b"BITCOUNT", b"foo", b"-1", b"-2")));
        assert_eq!(0, int(run!(&mut db, b"BITCOUNT", b"foo", b"1", b"0")));
        // Negative end clamped to 0.
        assert_eq!(4, int(run!(&mut db, b"BITCOUNT", b"foo", b"0", b"-6")));
        assert_eq!(4, int(run!(&mut db, b"BITCOUNT", b"foo", b"0", b"-100")));
        // Both-negative inverted range on a 1-byte key.
        set(&mut db, "A", b"A");
        assert_eq!(2, int(run!(&mut db, b"BITCOUNT", b"A", b"0", b"-2")));
        assert_eq!(0, int(run!(&mut db, b"BITCOUNT", b"A", b"-1", b"-2")));
    }

    #[test]
    fn bit_count_byte_bit_sub_range() {
        let mut db = DbSlice::new(0);
        set(&mut db, "foo", b"abcdef");
        assert_eq!(
            "ERR value is not an integer or out of range",
            err(run!(&mut db, b"BITCOUNT", b"foo", b"bar", b"BIT"))
        );
        assert_eq!(1, int(run!(&mut db, b"BITCOUNT", b"foo", b"1", b"1", b"BIT")));
        assert_eq!(2, int(run!(&mut db, b"BITCOUNT", b"foo", b"1", b"2", b"BIT")));
        assert_eq!(0, int(run!(&mut db, b"BITCOUNT", b"foo", b"3", b"2", b"BIT")));
        assert_eq!(2, int(run!(&mut db, b"BITCOUNT", b"foo", b"-3", b"-1", b"BIT")));
        assert_eq!(4, int(run!(&mut db, b"BITCOUNT", b"foo", b"1", b"9", b"BIT")));
        assert_eq!(0, int(run!(&mut db, b"BITCOUNT", b"foo", b"-1", b"-2", b"BIT")));
        // Both-negative inverted range past the end of a 1-byte key.
        set(&mut db, "x", &[0xff]);
        assert_eq!(0, int(run!(&mut db, b"BITCOUNT", b"x", b"-9", b"-10", b"BIT")));
    }

    #[test]
    fn bit_count_bit_last_bit_regression() {
        let mut db = DbSlice::new(0);
        set(&mut db, "k1", &[0x81]);
        assert_eq!(2, int(run!(&mut db, b"BITCOUNT", b"k1", b"0", b"7", b"BIT")));
        assert_eq!(1, int(run!(&mut db, b"BITCOUNT", b"k1", b"1", b"7", b"BIT")));
        assert_eq!(2, int(run!(&mut db, b"BITCOUNT", b"k1", b"-8", b"-1", b"BIT")));
        assert_eq!(0, int(run!(&mut db, b"BITCOUNT", b"k1", b"8", b"8", b"BIT")));

        set(&mut db, "k2", b"abcdef");
        assert_eq!(
            int(run!(&mut db, b"BITCOUNT", b"k2", b"0", b"-1")),
            int(run!(&mut db, b"BITCOUNT", b"k2", b"0", b"47", b"BIT"))
        );
        assert_eq!(
            int(run!(&mut db, b"BITCOUNT", b"k2", b"5", b"5")),
            int(run!(&mut db, b"BITCOUNT", b"k2", b"40", b"47", b"BIT"))
        );
        assert_eq!(0, int(run!(&mut db, b"BITCOUNT", b"k2", b"48", b"48", b"BIT")));
        assert_eq!(0, int(run!(&mut db, b"BITCOUNT", b"k2", b"100", b"200", b"BIT")));
    }

    const K1: [u8; 3] = [0xff, 0xaa, 0xcc];
    const K2: [u8; 2] = [0x01, 0xbb];
    const K3: [u8; 1] = [0x0f];

    fn bitop(db: &mut DbSlice, args: &[&[u8]]) -> i64 {
        int(dispatch(db, &args.iter().map(|a| a.to_vec()).collect::<Vec<_>>()))
    }

    #[test]
    fn bit_ops_and() {
        let mut db = DbSlice::new(0);
        set(&mut db, "first", &K1);
        set(&mut db, "second", &K2);
        set(&mut db, "third", &K3);
        // Illegal operation: op name isn't AND/OR/XOR/NOT.
        assert_eq!(SYNTAX, err(run!(&mut db, b"BITOP", b"foo", b"bar", b"abc")));
        // Nonexistent keys -> 0 and no dest.
        assert_eq!(0, bitop(&mut db, &[b"BITOP", b"AND", b"dest", b"1", b"2", b"3"]));
        assert_eq!(None, get(&mut db, "dest"));

        // Single source returns the source unchanged.
        assert_eq!(K1.len() as i64, bitop(&mut db, &[b"BITOP", b"AND", b"out", b"first"]));
        assert_eq!(K1.to_vec(), get(&mut db, "out").unwrap());

        // Two sources, result length = longest input.
        assert_eq!(3, bitop(&mut db, &[b"BITOP", b"AND", b"out", b"first", b"second"]));
        assert_eq!(vec![0x01, 0xaa, 0x00], get(&mut db, "out").unwrap());

        // Three sources.
        assert_eq!(3, bitop(&mut db, &[b"BITOP", b"AND", b"out", b"first", b"second", b"third"]));
        assert_eq!(vec![0x01, 0x00, 0x00], get(&mut db, "out").unwrap());
    }

    #[test]
    fn bit_ops_or_xor() {
        let mut db = DbSlice::new(0);
        set(&mut db, "first", &K1);
        set(&mut db, "second", &K2);
        set(&mut db, "third", &K3);
        assert_eq!(0, bitop(&mut db, &[b"BITOP", b"OR", b"dest", b"1", b"2", b"3"]));
        assert_eq!(3, bitop(&mut db, &[b"BITOP", b"OR", b"out", b"first", b"second"]));
        assert_eq!(vec![0xff, 0xbb, 0xcc], get(&mut db, "out").unwrap());
        assert_eq!(3, bitop(&mut db, &[b"BITOP", b"OR", b"out", b"first", b"second", b"third"]));
        assert_eq!(vec![0xff, 0xbb, 0xcc], get(&mut db, "out").unwrap());
        assert_eq!(3, bitop(&mut db, &[b"BITOP", b"XOR", b"out", b"first", b"second"]));
        assert_eq!(vec![0xfe, 0x11, 0xcc], get(&mut db, "out").unwrap());
        assert_eq!(3, bitop(&mut db, &[b"BITOP", b"XOR", b"out", b"first", b"second", b"third"]));
        assert_eq!(vec![0xf1, 0x11, 0xcc], get(&mut db, "out").unwrap());
    }

    #[test]
    fn bit_ops_not() {
        let mut db = DbSlice::new(0);
        // NOT takes exactly one source.
        assert_eq!(SYNTAX, err(run!(&mut db, b"BITOP", b"NOT", b"bar", b"abc", b"efg")));
        // Nonexistent source deletes the destination.
        assert_eq!(0, bitop(&mut db, &[b"BITOP", b"NOT", b"dest", b"missing"]));
        assert_eq!(None, get(&mut db, "dest"));

        set(&mut db, "first", &K1);
        assert_eq!(3, bitop(&mut db, &[b"BITOP", b"NOT", b"out", b"first"]));
        assert_eq!(vec![0x00, 0x55, 0x33], get(&mut db, "out").unwrap());
    }

    #[test]
    fn bit_ops_overwrites_non_string_dest() {
        let mut db = DbSlice::new(0);
        set(&mut db, "src", &[b'a'; 4]);
        db.insert(
            CompactString::from_bytes(b"dest"),
            PrimeValue::List(crate::core::quicklist::QuickList::default()),
        );
        assert_eq!(4, bitop(&mut db, &[b"BITOP", b"OR", b"dest", b"src"]));
        assert_eq!(vec![b'a'; 4], get(&mut db, "dest").unwrap());
    }

    #[test]
    fn bit_ops_wrong_type_source() {
        let mut db = DbSlice::new(0);
        db.insert(
            CompactString::from_bytes(b"lst"),
            PrimeValue::List(crate::core::quicklist::QuickList::default()),
        );
        set(&mut db, "first", &K1);
        assert_eq!(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
            err(run!(&mut db, b"BITOP", b"AND", b"dest", b"lst", b"first"))
        );
    }

    #[test]
    fn bit_ops_multi_shard_merge() {
        // AND across two shards: each contributes its combined partial.
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: vec![3],
                result: CmdResult::Ok(RespValue::Bulk(K1.to_vec())),
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: vec![4],
                result: CmdResult::Ok(RespValue::Bulk(K2.to_vec())),
            },
        ];
        let args = vec![
            b"BITOP".to_vec(),
            b"AND".to_vec(),
            b"dest".to_vec(),
            b"k1".to_vec(),
            b"k2".to_vec(),
        ];
        let keys = [2usize, 3, 4];
        match merge_bitop(&parts, &args, &keys, 0) {
            CmdResult::DeferredStore { key, value, reply } => {
                assert_eq!(key, b"dest");
                assert_str_store(value, &[0x01, 0xaa, 0x00]);
                assert_eq!(reply, integer(3));
            }
            o => panic!("expected DeferredStore, got {:?}", o.into_resp_value()),
        }

        // A shard with no sources contributes nil and is skipped.
        let parts = [
            ShardPart { shard: 0, owned_key_idxs: vec![3], result: CmdResult::Ok(RespValue::Nil) },
            ShardPart {
                shard: 1,
                owned_key_idxs: vec![4],
                result: CmdResult::Ok(RespValue::Bulk(K1.to_vec())),
            },
        ];
        match merge_bitop(&parts, &args, &keys, 0) {
            CmdResult::DeferredStore { value, reply, .. } => {
                assert_str_store(value, &K1);
                assert_eq!(reply, integer(3));
            }
            o => panic!("expected DeferredStore, got {:?}", o.into_resp_value()),
        }

        // NOT: shards contribute the raw source; the merge inverts.
        let args = vec![
            b"BITOP".to_vec(),
            b"NOT".to_vec(),
            b"dest".to_vec(),
            b"k1".to_vec(),
            b"k2".to_vec(),
        ];
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: vec![3],
                result: CmdResult::Ok(RespValue::Bulk(K1.to_vec())),
            },
            ShardPart { shard: 1, owned_key_idxs: vec![4], result: CmdResult::Ok(RespValue::Nil) },
        ];
        match merge_bitop(&parts, &args, &keys, 0) {
            CmdResult::DeferredStore { value, reply, .. } => {
                assert_str_store(value, &[0x00, 0x55, 0x33]);
                assert_eq!(reply, integer(3));
            }
            o => panic!("expected DeferredStore, got {:?}", o.into_resp_value()),
        }
    }

    #[test]
    fn bit_pos() {
        let mut db = DbSlice::new(0);
        set(&mut db, "a", &[0x00, 0x00, 0x06, 0xff, 0xf0]);

        // Clear bits, default BYTE mode.
        assert_eq!(0, int(run!(&mut db, b"BITPOS", b"a", b"0")));
        assert_eq!(8, int(run!(&mut db, b"BITPOS", b"a", b"0", b"1")));
        assert_eq!(16, int(run!(&mut db, b"BITPOS", b"a", b"0", b"2")));
        assert_eq!(-1, int(run!(&mut db, b"BITPOS", b"a", b"0", b"100")));
        assert_eq!(-1, int(run!(&mut db, b"BITPOS", b"a", b"0", b"100", b"103")));
        assert_eq!(0, int(run!(&mut db, b"BITPOS", b"a", b"0", b"0", b"100")));
        assert_eq!(36, int(run!(&mut db, b"BITPOS", b"a", b"0", b"3")));
        assert_eq!(36, int(run!(&mut db, b"BITPOS", b"a", b"0", b"-2")));
        assert_eq!(36, int(run!(&mut db, b"BITPOS", b"a", b"0", b"-2", b"-1")));
        assert_eq!(0, int(run!(&mut db, b"BITPOS", b"a", b"0", b"-100")));

        // Clear bits, BIT mode.
        assert_eq!(0, int(run!(&mut db, b"BITPOS", b"a", b"0", b"0", b"100", b"BIT")));
        assert_eq!(1, int(run!(&mut db, b"BITPOS", b"a", b"0", b"1", b"100", b"BIT")));
        assert_eq!(16, int(run!(&mut db, b"BITPOS", b"a", b"0", b"16", b"100", b"BIT")));
        assert_eq!(23, int(run!(&mut db, b"BITPOS", b"a", b"0", b"21", b"100", b"BIT")));
        assert_eq!(36, int(run!(&mut db, b"BITPOS", b"a", b"0", b"24", b"100", b"BIT")));
        assert_eq!(38, int(run!(&mut db, b"BITPOS", b"a", b"0", b"-2", b"-1", b"BIT")));

        // Set bits.
        assert_eq!(21, int(run!(&mut db, b"BITPOS", b"a", b"1")));
        assert_eq!(21, int(run!(&mut db, b"BITPOS", b"a", b"1", b"2")));
        assert_eq!(24, int(run!(&mut db, b"BITPOS", b"a", b"1", b"3")));
        assert_eq!(32, int(run!(&mut db, b"BITPOS", b"a", b"1", b"4")));
        assert_eq!(32, int(run!(&mut db, b"BITPOS", b"a", b"1", b"-1")));
        assert_eq!(24, int(run!(&mut db, b"BITPOS", b"a", b"1", b"-2")));
        assert_eq!(21, int(run!(&mut db, b"BITPOS", b"a", b"1", b"-100")));
        assert_eq!(-1, int(run!(&mut db, b"BITPOS", b"a", b"1", b"0", b"1")));
        assert_eq!(21, int(run!(&mut db, b"BITPOS", b"a", b"1", b"0", b"3")));
        assert_eq!(21, int(run!(&mut db, b"BITPOS", b"a", b"1", b"0", b"100")));
        assert_eq!(32, int(run!(&mut db, b"BITPOS", b"a", b"1", b"-1", b"-1")));
        assert_eq!(24, int(run!(&mut db, b"BITPOS", b"a", b"1", b"-2", b"-1")));
        assert_eq!(-1, int(run!(&mut db, b"BITPOS", b"a", b"1", b"-1", b"-2")));

        // Set bits, BIT mode.
        assert_eq!(21, int(run!(&mut db, b"BITPOS", b"a", b"1", b"0", b"21", b"BIT")));
        assert_eq!(21, int(run!(&mut db, b"BITPOS", b"a", b"1", b"21", b"21", b"BIT")));
        assert_eq!(21, int(run!(&mut db, b"BITPOS", b"a", b"1", b"0", b"100", b"BIT")));
        assert_eq!(-1, int(run!(&mut db, b"BITPOS", b"a", b"1", b"-1", b"-1", b"BIT")));
        assert_eq!(35, int(run!(&mut db, b"BITPOS", b"a", b"1", b"-5", b"-1", b"BIT")));
        assert_eq!(34, int(run!(&mut db, b"BITPOS", b"a", b"1", b"-6", b"-1", b"BIT")));

        // Clear bit in an all-set string reports the length.
        set(&mut db, "b", &[0xff, 0xff, 0xff]);
        assert_eq!(24, int(run!(&mut db, b"BITPOS", b"b", b"0")));
        assert_eq!(-1, int(run!(&mut db, b"BITPOS", b"b", b"0", b"3")));
        assert_eq!(-1, int(run!(&mut db, b"BITPOS", b"b", b"0", b"0", b"1")));
        assert_eq!(-1, int(run!(&mut db, b"BITPOS", b"b", b"0", b"0", b"1", b"BYTE")));

        // Empty key.
        set(&mut db, "empty", b"");
        assert_eq!(-1, int(run!(&mut db, b"BITPOS", b"empty", b"0")));
        assert_eq!(-1, int(run!(&mut db, b"BITPOS", b"empty", b"0", b"1")));

        // Missing key behaves like a zero-padded string.
        assert_eq!(-1, int(run!(&mut db, b"BITPOS", b"d", b"1")));
        assert_eq!(0, int(run!(&mut db, b"BITPOS", b"d", b"0")));

        // Bit argument must be 0 or 1.
        assert_eq!(BIT_ARG, err(run!(&mut db, b"BITPOS", b"d", b"2")));
        assert_eq!(BIT_ARG, err(run!(&mut db, b"BITPOS", b"d", b"-1")));
    }

    #[test]
    fn bit_field_parsing() {
        let mut db = DbSlice::new(0);
        for args in [
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"SET".as_slice(), b"u1".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"SET".as_slice(), b"u1".as_slice(), b"0".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"SET".as_slice(), b"u1".as_slice(), b"0".as_slice(), b"0".as_slice(), b"55".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"SET".as_slice(), b"u1".as_slice(), b"0".as_slice(), b"0".as_slice(), b"GET".as_slice(), b"u1".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"GET".as_slice(), b"u1".as_slice(), b"0".as_slice(), b"15".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"GET".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"SET".as_slice(), b"u1".as_slice(), b"0".as_slice(), b"0".as_slice(), b"SET".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"OVERFLOW".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"OVERFLOW".as_slice(), b"nonsense".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"GET".as_slice(), b"i16".as_slice(), b"-1".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"SET".as_slice(), b"i16".as_slice(), b"0".as_slice(), b"foo".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"INCRBY".as_slice(), b"i16".as_slice(), b"0".as_slice(), b"bar".as_slice()][..],
        ] {
            assert_eq!(SYNTAX, err(dispatch_slices(&mut db, args)));
        }
        for args in [
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"SET".as_slice(), b"u0".as_slice(), b"0".as_slice(), b"0".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"SET".as_slice(), b"u64".as_slice(), b"0".as_slice(), b"0".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"SET".as_slice(), b"u65".as_slice(), b"0".as_slice(), b"0".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"SET".as_slice(), b"i65".as_slice(), b"0".as_slice(), b"0".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"GET".as_slice(), b"i-42".as_slice(), b"0".as_slice()][..],
            &[b"BITFIELD".as_slice(), b"foo".as_slice(), b"GET".as_slice(), b"I8".as_slice(), b"0".as_slice()][..],
        ] {
            assert_eq!(INVALID_TYPE, err(dispatch_slices(&mut db, args)));
        }
        assert_eq!(
            "ERR BITFIELD_RO only supports the GET subcommand",
            err(run!(&mut db, b"BITFIELD_RO", b"foo", b"SET", b"u1", b"0", b"0"))
        );
        assert_eq!(
            "ERR BITFIELD_RO only supports the GET subcommand",
            err(run!(&mut db, b"BITFIELD_RO", b"foo", b"INCRBY", b"i64", b"0", b"15"))
        );
    }

    #[test]
    fn bit_field_create() {
        let mut db = DbSlice::new(0);
        assert_eq!(vec![RespValue::Integer(0)], arr(run!(&mut db, b"BITFIELD", b"foo", b"SET", b"u1", b"0", b"1")));
        assert_eq!(vec![RespValue::Integer(1)], arr(run!(&mut db, b"BITFIELD", b"foo", b"GET", b"u1", b"0")));
        assert_eq!(vec![RespValue::Integer(1)], arr(run!(&mut db, b"BITFIELD", b"foo", b"INCRBY", b"u1", b"1", b"1")));
        assert_eq!(vec![RespValue::Integer(1)], arr(run!(&mut db, b"BITFIELD", b"foo", b"GET", b"u1", b"1")));
    }

    #[test]
    fn bit_field_overflow_underflow() {
        let mut db = DbSlice::new(0);
        run!(&mut db, b"BITFIELD", b"foo", b"SET", b"u2", b"0", b"2");

        // u1 WRAP.
        assert_eq!(
            vec![RespValue::Integer(1)],
            arr(run!(&mut db, b"BITFIELD", b"foo", b"SET", b"u1", b"0", b"2"))
        );
        assert_eq!(vec![RespValue::Integer(0)], arr(run!(&mut db, b"BITFIELD", b"foo", b"GET", b"u1", b"0")));

        // i64 WRAP: max + 1 -> min.
        run!(&mut db, b"BITFIELD", b"foo", b"SET", b"i64", b"0", b"9223372036854775807");
        assert_eq!(
            vec![RespValue::Integer(i64::MIN)],
            arr(run!(&mut db, b"BITFIELD", b"foo", b"INCRBY", b"i64", b"0", b"1"))
        );
        // i64 WRAP: min - 1 -> max.
        run!(&mut db, b"BITFIELD", b"foo", b"SET", b"i64", b"0", b"-9223372036854775808");
        assert_eq!(
            vec![RespValue::Integer(i64::MAX)],
            arr(run!(&mut db, b"BITFIELD", b"foo", b"INCRBY", b"i64", b"0", b"-1"))
        );

        // i1 WRAP.
        run!(&mut db, b"BITFIELD", b"foo", b"SET", b"i1", b"0", b"-2");
        assert_eq!(vec![RespValue::Integer(0)], arr(run!(&mut db, b"BITFIELD", b"foo", b"GET", b"i1", b"0")));
        assert_eq!(
            vec![RespValue::Integer(-1)],
            arr(run!(&mut db, b"BITFIELD", b"foo", b"INCRBY", b"i1", b"0", b"-1"))
        );

        // SAT on u8.
        run!(&mut db, b"BITFIELD", b"foo", b"SET", b"u1", b"0", b"0");
        assert_eq!(
            vec![RespValue::Integer(255)],
            arr(run!(&mut db, b"BITFIELD", b"foo", b"OVERFLOW", b"SAT", b"INCRBY", b"u8", b"0", b"300"))
        );
        assert_eq!(
            vec![RespValue::Integer(255)],
            arr(run!(&mut db, b"BITFIELD", b"foo", b"GET", b"u8", b"0"))
        );

        // SAT on i8.
        run!(&mut db, b"BITFIELD", b"foo", b"SET", b"u8", b"0", b"0");
        assert_eq!(
            vec![RespValue::Integer(0)],
            arr(run!(&mut db, b"BITFIELD", b"foo", b"OVERFLOW", b"SAT", b"SET", b"i8", b"0", b"300"))
        );
        assert_eq!(
            vec![RespValue::Integer(-128)],
            arr(run!(&mut db, b"BITFIELD", b"foo", b"OVERFLOW", b"SAT", b"INCRBY", b"i8", b"0", b"-255"))
        );

        // FAIL leaves the value unchanged and returns nil.
        run!(&mut db, b"BITFIELD", b"foo", b"SET", b"u8", b"0", b"200");
        assert_eq!(
            vec![RespValue::Nil],
            arr(run!(&mut db, b"BITFIELD", b"foo", b"OVERFLOW", b"FAIL", b"INCRBY", b"u8", b"0", b"100"))
        );
        assert_eq!(vec![RespValue::Integer(200)], arr(run!(&mut db, b"BITFIELD", b"foo", b"GET", b"u8", b"0")));

        // Overflow policy sticks across a chain of subcommands.
        run!(&mut db, b"BITFIELD", b"foo", b"SET", b"u8", b"0", b"0");
        assert_eq!(
            vec![RespValue::Nil, RespValue::Nil],
            arr(run!(
                &mut db,
                b"BITFIELD",
                b"foo",
                b"OVERFLOW",
                b"FAIL",
                b"SET",
                b"u8",
                b"0",
                b"300",
                b"SET",
                b"u1",
                b"0",
                b"400"
            ))
        );
    }

    #[test]
    fn bit_field_operations() {
        let mut db = DbSlice::new(0);
        run!(&mut db, b"BITFIELD", b"foo", b"SET", b"u32", b"0", b"0");
        // Pattern 01111000 00000001 00000001 00001010.
        for (off, val) in [(0u8, 120u8), (8, 1), (16, 1), (24, 10)] {
            let o = off.to_string();
            let v = val.to_string();
            assert_eq!(
                vec![RespValue::Integer(0)],
                arr(run!(&mut db, b"BITFIELD", b"foo", b"SET", b"u8", o.as_bytes(), v.as_bytes()))
            );
        }
        assert_eq!(
            vec![RespValue::Integer(2013331722)],
            arr(run!(&mut db, b"BITFIELD", b"foo", b"GET", b"u32", b"0"))
        );
        assert_eq!(vec![RespValue::Integer(240)], arr(run!(&mut db, b"BITFIELD", b"foo", b"INCRBY", b"u8", b"0", b"120")));

        // Signed aligned.
        run!(&mut db, b"BITFIELD", b"foo", b"SET", b"u32", b"0", b"0");
        for (off, val) in [(0u8, -120i64), (8, -1), (16, -1), (24, -10)] {
            let o = off.to_string();
            let v = val.to_string();
            assert_eq!(
                vec![RespValue::Integer(0)],
                arr(run!(&mut db, b"BITFIELD", b"foo", b"SET", b"i8", o.as_bytes(), v.as_bytes()))
            );
        }
        assert_eq!(
            vec![RespValue::Integer(-1996488714)],
            arr(run!(&mut db, b"BITFIELD", b"foo", b"GET", b"i32", b"0"))
        );

        // Non-aligned unsigned: 00000000 10000000 10000000 10000000 10000000.
        run!(&mut db, b"BITFIELD", b"foo", b"SET", b"i64", b"0", b"0");
        for off in [1u8, 9, 17, 25] {
            let o = off.to_string();
            assert_eq!(
                vec![RespValue::Integer(0)],
                arr(run!(&mut db, b"BITFIELD", b"foo", b"SET", b"u8", o.as_bytes(), b"1"))
            );
        }
        assert_eq!(vec![RespValue::Integer(1)], arr(run!(&mut db, b"BITFIELD", b"foo", b"GET", b"u1", b"8")));
        assert_eq!(vec![RespValue::Integer(1)], arr(run!(&mut db, b"BITFIELD", b"foo", b"GET", b"u1", b"32")));
        assert_eq!(vec![RespValue::Integer(16843009)], arr(run!(&mut db, b"BITFIELD", b"foo", b"GET", b"u33", b"0")));

        // Positional offsets: #0 -> 0, #1 -> 8, #2 -> 16 (u8 fields).
        run!(
            &mut db,
            b"BITFIELD",
            b"foo",
            b"SET",
            b"u8",
            b"#0",
            b"1",
            b"SET",
            b"u8",
            b"#1",
            b"1",
            b"SET",
            b"u8",
            b"#2",
            b"1"
        );
        assert_eq!(vec![RespValue::Integer(1)], arr(run!(&mut db, b"BITFIELD", b"foo", b"GET", b"u1", b"7")));
        assert_eq!(vec![RespValue::Integer(1)], arr(run!(&mut db, b"BITFIELD", b"foo", b"GET", b"u1", b"15")));
    }

    #[test]
    fn bit_field_large_offset() {
        let mut db = DbSlice::new(0);
        set(&mut db, "foo", b"bar");
        // GET works past the end; the FAIL incrby grew the string to 4 bytes.
        assert_eq!(
            vec![RespValue::Integer(1650553344), RespValue::Nil],
            arr(run!(
                &mut db,
                b"BITFIELD",
                b"foo",
                b"GET",
                b"u32",
                b"0",
                b"OVERFLOW",
                b"FAIL",
                b"INCRBY",
                b"u32",
                b"0",
                b"4294967295"
            ))
        );
        assert_eq!(vec![b'b', b'a', b'r', 0], get(&mut db, "foo").unwrap());
        assert_eq!(
            vec![RespValue::Integer(0)],
            arr(run!(&mut db, b"BITFIELD", b"foo", b"GET", b"u32", b"4294967295"))
        );
        // Reads beyond the uint32 bit-index space are rejected; writes are bounded.
        assert_eq!(
            vec![RespValue::Integer(0)],
            arr(run!(&mut db, b"BITFIELD", b"bk", b"GET", b"u8", b"2200000000"))
        );
        assert_eq!(
            BIT_OFFSET,
            err(run!(&mut db, b"BITFIELD", b"bk", b"GET", b"u8", b"5000000000"))
        );
        assert_eq!(
            BIT_OFFSET,
            err(run!(&mut db, b"BITFIELD", b"bk", b"SET", b"u8", b"2200000000", b"1"))
        );
        assert_eq!(
            BIT_OFFSET,
            err(run!(&mut db, b"BITFIELD", b"bk", b"INCRBY", b"u8", b"2200000000", b"1"))
        );
    }

    #[test]
    fn bit_field_issue_5237() {
        let mut db = DbSlice::new(0);
        set(&mut db, "s", &[0xff, 0xf0, 0x00]);
        assert_eq!(
            vec![RespValue::Integer(-1), RespValue::Integer(-1)],
            arr(run!(
                &mut db,
                b"BITFIELD",
                b"s",
                b"OVERFLOW",
                b"SAT",
                b"SET",
                b"i4",
                b"0",
                b"8",
                b"SET",
                b"i4",
                b"4",
                b"7"
            ))
        );
        set(&mut db, "i", &[0xff, 0xf0, 0x00]);
        assert_eq!(
            vec![RespValue::Integer(84), RespValue::Integer(170)],
            arr(run!(
                &mut db,
                b"BITFIELD",
                b"i",
                b"INCRBY",
                b"u8",
                b"0",
                b"85",
                b"INCRBY",
                b"u8",
                b"16",
                b"170"
            ))
        );
    }

    #[test]
    fn bit_field_no_ops() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            Vec::<RespValue>::new(),
            arr(run!(&mut db, b"BITFIELD", b"k", b"OVERFLOW", b"SAT"))
        );
        assert_eq!(Vec::<RespValue>::new(), arr(run!(&mut db, b"BITFIELD", b"k")));
        assert_eq!(
            Vec::<RespValue>::new(),
            arr(run!(&mut db, b"BITFIELD_RO", b"k", b"OVERFLOW", b"SAT"))
        );
        assert_eq!(Vec::<RespValue>::new(), arr(run!(&mut db, b"BITFIELD_RO", b"k")));
    }
}
