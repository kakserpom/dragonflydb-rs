//! The Lua extension libraries the reference loads via `LoadLibrary`
//! (`src/core/interpreter.cc:426-429`): `cjson`, `struct`, `cmsgpack` and
//! `bit`. Pure-Rust ports of the C sources Dragonfly vendors under
//! `src/redis/lua/`, preserving Dragonfly's behavior deltas over stock Redis:
//!
//! - `cjson` is always installed as a global (no `ENABLE_CJSON_GLOBAL` guard)
//!   and `decode` converts integral numbers to Lua integers, so
//!   `tostring(cjson.decode('{"id":42}').id)` is `"42"` not `"42.0"`.
//! - `cmsgpack` uses `int64_t` array/map sizes and drops the buffer
//!   overflow guards.
//!
//! All four are installed in `SandboxedInterpreter::bootstrap` right after the
//! polyfills, mirroring the reference's `LoadLibrary` sequence.

use std::sync::{Arc, Mutex};

use mlua::{Function, IntoLuaMulti, Lua, MultiValue, Table, Value};

use crate::commands::lua::raise_string_error;
use crate::util::format_g;

/// Fetch argument `i` (0-based) or `Value::Nil` when the script passed fewer
/// arguments. The reference reads absent args as nil (`lua_gettop` + index).
fn arg(args: &MultiValue, i: usize) -> Value {
    args.get(i).cloned().unwrap_or(Value::Nil)
}

fn number_arg(lua: &Lua, v: Value) -> mlua::Result<f64> {
    lua.coerce_number(v)?
        .ok_or_else(|| mlua::Error::runtime("number expected"))
}

fn string_arg(lua: &Lua, v: Value) -> mlua::Result<Vec<u8>> {
    lua.coerce_string(v)?
        .map(|s| s.as_bytes().to_vec())
        .ok_or_else(|| mlua::Error::runtime("string expected"))
}

fn integer_arg(lua: &Lua, v: Value) -> mlua::Result<i64> {
    lua.coerce_integer(v)?
        .ok_or_else(|| mlua::Error::runtime("number expected"))
}

/// Register `f` as a Lua function that raises its runtime errors as plain Lua
/// strings. The C libraries call `luaL_error`, so a plain string is what lands
/// in `__redis__err__handler`; returning a typed `mlua::Error` instead would
/// surface as an unconcatenatable userdata to the script's `xpcall` handler.
fn lib_fn<F, R>(lua: &Lua, f: F) -> mlua::Result<Function>
where
    F: Fn(&Lua, MultiValue) -> mlua::Result<R> + Send + Sync + 'static,
    R: IntoLuaMulti,
{
    lua.create_function(move |lua, args| match f(lua, args) {
        Ok(v) => Ok(v),
        Err(mlua::Error::RuntimeError(msg)) => raise_string_error(lua, msg),
        Err(e) => Err(e),
    })
}

// ---------------------------------------------------------------------------
// bit (Lua BitOp 1.0.3, `dfly_lua_bit.c`)
// ---------------------------------------------------------------------------

/// `barg`: convert an argument to its 32-bit unsigned form exactly like Lua
/// BitOp. `luaL_checknumber` (Lua 5.2+) errors on non-coercible values, then
/// the value is rounded to a 54-bit double via `+= 2^52+2^51` and the low 32
/// bits of the mantissa are taken.
fn barg(lua: &Lua, v: Value) -> mlua::Result<u32> {
    let n = number_arg(lua, v)?;
    Ok((n + 6_755_399_441_055_744.0).to_bits() as u32)
}

/// `BRET`: the signed 32-bit result, pushed as a Lua integer.
fn bret(b: u32) -> Value {
    Value::Integer((b as i32) as i64)
}

fn bit_tobit(lua: &Lua, args: &MultiValue) -> mlua::Result<Value> {
    Ok(bret(barg(lua, arg(args, 0))?))
}

fn bit_bnot(lua: &Lua, args: &MultiValue) -> mlua::Result<Value> {
    Ok(bret(!barg(lua, arg(args, 0))?))
}

fn bit_fold(lua: &Lua, args: &MultiValue, f: impl Fn(u32, u32) -> u32) -> mlua::Result<Value> {
    let mut b = barg(lua, arg(args, 0))?;
    for v in args.iter().skip(1) {
        b = f(b, barg(lua, v.clone())?);
    }
    Ok(bret(b))
}

fn bit_shift(lua: &Lua, args: &MultiValue, f: impl Fn(u32, u32) -> u32) -> mlua::Result<Value> {
    let b = barg(lua, arg(args, 0))?;
    let n = barg(lua, arg(args, 1))? & 31;
    Ok(bret(f(b, n)))
}

fn bit_tohex(lua: &Lua, args: &MultiValue) -> mlua::Result<Value> {
    let b = barg(lua, arg(args, 0))?;
    // `lua_isnone(L, 2)` is false for an explicit nil, which then errors in
    // `barg` just like the reference.
    let mut n: i32 = if args.len() < 2 {
        8
    } else {
        barg(lua, arg(args, 1))? as i32
    };
    if n == i32::MIN {
        n += 1;
    }
    let (upper, n) = if n < 0 { (true, -n) } else { (false, n) };
    let n = n.min(8);
    let digits: &[u8] = if upper {
        b"0123456789ABCDEF"
    } else {
        b"0123456789abcdef"
    };
    let mut buf = [0u8; 8];
    let mut bb = b;
    for i in (0..n).rev() {
        buf[i as usize] = digits[(bb & 0xf) as usize];
        bb >>= 4;
    }
    Ok(Value::String(lua.create_string(&buf[..n as usize])?))
}

/// Register the global `bit` table (`luaopen_bit`). The C library's self-test
/// is unnecessary here: the port has no luaconf.h to disagree with.
pub fn install_bit(lua: &Lua) -> mlua::Result<()> {
    let t = lua.create_table()?;
    t.set(
        "tobit",
        lib_fn(lua, |lua, args: MultiValue| bit_tobit(lua, &args))?,
    )?;
    t.set(
        "bnot",
        lib_fn(lua, |lua, args: MultiValue| bit_bnot(lua, &args))?,
    )?;
    t.set(
        "band",
        lib_fn(lua, |lua, args: MultiValue| {
            bit_fold(lua, &args, |a, b| a & b)
        })?,
    )?;
    t.set(
        "bor",
        lib_fn(lua, |lua, args: MultiValue| {
            bit_fold(lua, &args, |a, b| a | b)
        })?,
    )?;
    t.set(
        "bxor",
        lib_fn(lua, |lua, args: MultiValue| {
            bit_fold(lua, &args, |a, b| a ^ b)
        })?,
    )?;
    t.set(
        "lshift",
        lib_fn(lua, |lua, args: MultiValue| {
            bit_shift(lua, &args, |b, n| b << n)
        })?,
    )?;
    t.set(
        "rshift",
        lib_fn(lua, |lua, args: MultiValue| {
            bit_shift(lua, &args, |b, n| b >> n)
        })?,
    )?;
    t.set(
        "arshift",
        lib_fn(lua, |lua, args: MultiValue| {
            bit_shift(lua, &args, |b, n| ((b as i32) >> n) as u32)
        })?,
    )?;
    t.set(
        "rol",
        lib_fn(lua, |lua, args: MultiValue| {
            bit_shift(lua, &args, u32::rotate_left)
        })?,
    )?;
    t.set(
        "ror",
        lib_fn(lua, |lua, args: MultiValue| {
            bit_shift(lua, &args, u32::rotate_right)
        })?,
    )?;
    t.set(
        "bswap",
        lib_fn(lua, |lua, args: MultiValue| {
            let b = barg(lua, arg(&args, 0))?;
            Ok(bret(
                (b >> 24) | ((b >> 8) & 0xff00) | ((b & 0xff00) << 8) | (b << 24),
            ))
        })?,
    )?;
    t.set(
        "tohex",
        lib_fn(lua, |lua, args: MultiValue| bit_tohex(lua, &args))?,
    )?;
    lua.globals().set("bit", t)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// struct (lua-struct v1.7, `dfly_lua_struct.c`)
// ---------------------------------------------------------------------------

/// Maximum size (in bytes) for integral types.
const MAXINTSIZE: usize = 32;
/// `MAXALIGN`: max(`sizeof(struct cD) - sizeof(double)`, `sizeof(int)`) = 8.
const MAXALIGN: usize = 8;
/// The platform is little-endian in practice (`native.endian == LITTLE`).
const NATIVE_LITTLE: bool = cfg!(target_endian = "little");

#[derive(Clone, Copy)]
struct Header {
    little: bool,
    align: usize,
}

impl Default for Header {
    fn default() -> Self {
        Header {
            little: NATIVE_LITTLE,
            align: 1,
        }
    }
}

/// `getnum`: parse a decimal size, advancing the cursor, or return `default`
/// when the next byte is not a digit.
fn getnum(fmt: &[u8], i: &mut usize, default: usize) -> mlua::Result<usize> {
    if *i >= fmt.len() || !fmt[*i].is_ascii_digit() {
        return Ok(default);
    }
    let mut a = 0usize;
    while *i < fmt.len() && fmt[*i].is_ascii_digit() {
        let d = usize::from(fmt[*i] - b'0');
        if a > (i32::MAX as usize) / 10 || a * 10 > (i32::MAX as usize - d) {
            return Err(mlua::Error::runtime("integral size overflow"));
        }
        a = a * 10 + d;
        *i += 1;
    }
    Ok(a)
}

fn optsize(fmt: &[u8], i: &mut usize, opt: u8) -> mlua::Result<usize> {
    match opt {
        b'B' | b'b' | b'x' => Ok(1),        // sizeof(char) / padding
        b'H' | b'h' => Ok(2),               // sizeof(short)
        b'L' | b'l' | b'T' | b'd' => Ok(8), // sizeof(long)/size_t/double
        b'f' => Ok(4),
        b'c' => getnum(fmt, i, 1),
        b'i' | b'I' => {
            let sz = getnum(fmt, i, 4)?; // sizeof(int)
            if sz > MAXINTSIZE {
                return Err(mlua::Error::runtime(format!(
                    "integral size {sz} is larger than limit of {MAXINTSIZE}"
                )));
            }
            Ok(sz)
        }
        _ => Ok(0),
    }
}

fn controloptions(fmt: &[u8], i: &mut usize, opt: u8, h: &mut Header) -> mlua::Result<()> {
    match opt {
        b' ' => Ok(()),
        b'>' => {
            h.little = false;
            Ok(())
        }
        b'<' => {
            h.little = true;
            Ok(())
        }
        b'!' => {
            let a = getnum(fmt, i, MAXALIGN)?;
            if !a.is_power_of_two() {
                return Err(mlua::Error::runtime(format!(
                    "alignment {a} is not a power of 2"
                )));
            }
            h.align = a;
            Ok(())
        }
        _ => Err(mlua::Error::runtime(format!(
            "invalid format option '{}'",
            char::from(opt)
        ))),
    }
}

fn gettoalign(len: usize, align: usize, opt: u8, size: usize) -> usize {
    if size == 0 || opt == b'c' {
        return 0;
    }
    let size = size.min(align);
    (size - (len & (size - 1))) & (size - 1)
}

/// `putinteger`: write `n` as `size` bytes in the requested byte order.
fn putinteger(n: f64, little: bool, size: usize) -> Vec<u8> {
    let value: u64 = if n < 0.0 { n as i64 as u64 } else { n as u64 };
    let mut out = vec![0u8; size];
    let mut v = value;
    if little {
        for b in &mut out {
            *b = v as u8;
            v >>= 8;
        }
    } else {
        for b in out.iter_mut().rev() {
            *b = v as u8;
            v >>= 8;
        }
    }
    out
}

/// `correctbytes`: reverse `size` bytes in place when the format byte order
/// differs from the host.
fn correctbytes(bytes: &mut [u8], size: usize, little: bool) {
    if little != NATIVE_LITTLE {
        bytes[..size].reverse();
    }
}

/// `getinteger`: decode a `size`-byte integer, sign-extending when signed.
fn getinteger(buff: &[u8], little: bool, issigned: bool, size: usize) -> f64 {
    let mut l: u64 = 0;
    if little {
        for b in buff.iter().take(size).rev() {
            l = (l << 8) | u64::from(*b);
        }
    } else {
        for b in buff.iter().take(size) {
            l = (l << 8) | u64::from(*b);
        }
    }
    let bits = size * 8;
    if !issigned {
        l as f64
    } else if bits >= 64 {
        // The C shift degenerates to an i64 interpretation past 64 bits.
        l as i64 as f64
    } else {
        let sign = 1u64 << (bits - 1);
        if l & sign != 0 {
            (l as i64 | !((1i64 << bits) - 1)) as f64
        } else {
            l as f64
        }
    }
}

fn struct_pack(lua: &Lua, args: &MultiValue) -> mlua::Result<Value> {
    let fmt = string_arg(lua, arg(args, 0))?;
    let mut out = Vec::new();
    let mut h = Header::default();
    let mut value = 1usize; // index into args for the next value (arg 2 is first)
    let mut totalsize = 0usize;
    let mut i = 0usize;
    while i < fmt.len() {
        let opt = fmt[i];
        i += 1;
        let mut size = optsize(&fmt, &mut i, opt)?;
        let toalign = gettoalign(totalsize, h.align, opt, size);
        totalsize += toalign;
        out.extend(std::iter::repeat_n(0u8, toalign));
        match opt {
            b'b' | b'B' | b'h' | b'H' | b'l' | b'L' | b'T' | b'i' | b'I' => {
                let n = number_arg(lua, arg(args, value))?;
                value += 1;
                out.extend(putinteger(n, h.little, size));
            }
            b'x' => out.push(0),
            b'f' => {
                let f = number_arg(lua, arg(args, value))? as f32;
                value += 1;
                let mut bytes = f.to_bits().to_ne_bytes();
                correctbytes(&mut bytes, 4, h.little);
                out.extend_from_slice(&bytes);
            }
            b'd' => {
                let d = number_arg(lua, arg(args, value))?;
                value += 1;
                let mut bytes = d.to_bits().to_ne_bytes();
                correctbytes(&mut bytes, 8, h.little);
                out.extend_from_slice(&bytes);
            }
            b'c' | b's' => {
                let s = string_arg(lua, arg(args, value))?;
                value += 1;
                let l = s.len();
                if size == 0 {
                    size = l;
                }
                if l < size {
                    return Err(mlua::Error::runtime("string too short"));
                }
                out.extend_from_slice(&s[..size]);
                if opt == b's' {
                    out.push(0);
                    size += 1;
                }
            }
            _ => controloptions(&fmt, &mut i, opt, &mut h)?,
        }
        totalsize += size;
    }
    Ok(Value::String(lua.create_string(&out)?))
}

fn struct_unpack(lua: &Lua, args: &MultiValue) -> mlua::Result<MultiValue> {
    let fmt = string_arg(lua, arg(args, 0))?;
    let data = string_arg(lua, arg(args, 1))?;
    let pos = match arg(args, 2) {
        Value::Nil => 1,
        v => integer_arg(lua, v)?,
    };
    if pos <= 0 {
        return Err(mlua::Error::runtime("offset must be 1 or greater"));
    }
    let mut pos = (pos - 1) as usize;
    let mut results: Vec<Value> = Vec::new();
    let mut h = Header::default();
    let mut i = 0usize;
    while i < fmt.len() {
        let opt = fmt[i];
        i += 1;
        let mut size = optsize(&fmt, &mut i, opt)?;
        pos += gettoalign(pos, h.align, opt, size);
        if size > data.len() || pos > data.len() - size {
            return Err(mlua::Error::runtime("data string too short"));
        }
        match opt {
            b'b' | b'B' | b'h' | b'H' | b'l' | b'L' | b'T' | b'i' | b'I' => {
                let issigned = opt.is_ascii_lowercase();
                let res = getinteger(&data[pos..], h.little, issigned, size);
                results.push(Value::Number(res));
            }
            b'x' => {}
            b'f' => {
                let mut bytes: [u8; 4] = data[pos..pos + 4].try_into().unwrap();
                correctbytes(&mut bytes, 4, h.little);
                results.push(Value::Number(
                    f32::from_bits(u32::from_ne_bytes(bytes)) as f64
                ));
            }
            b'd' => {
                let mut bytes: [u8; 8] = data[pos..pos + 8].try_into().unwrap();
                correctbytes(&mut bytes, 8, h.little);
                results.push(Value::Number(f64::from_bits(u64::from_ne_bytes(bytes))));
            }
            b'c' => {
                if size == 0 {
                    // `c0` reuses the previous numeric result as the length.
                    let Some(Value::Number(prev)) = results.pop() else {
                        return Err(mlua::Error::runtime("format 'c0' needs a previous size"));
                    };
                    size = prev as usize;
                    if size > data.len() || pos > data.len() - size {
                        return Err(mlua::Error::runtime("data string too short"));
                    }
                }
                results.push(Value::String(lua.create_string(&data[pos..pos + size])?));
            }
            b's' => {
                let Some(off) = data[pos..].iter().position(|b| *b == 0) else {
                    return Err(mlua::Error::runtime("unfinished string in data"));
                };
                size = off + 1;
                results.push(Value::String(lua.create_string(&data[pos..pos + off])?));
            }
            _ => controloptions(&fmt, &mut i, opt, &mut h)?,
        }
        pos += size;
    }
    results.push(Value::Integer((pos + 1) as i64));
    Ok(MultiValue::from(results))
}

fn struct_size(lua: &Lua, args: &MultiValue) -> mlua::Result<Value> {
    let fmt = string_arg(lua, arg(args, 0))?;
    let mut h = Header::default();
    let mut pos = 0usize;
    let mut i = 0usize;
    while i < fmt.len() {
        let opt = fmt[i];
        i += 1;
        let size = optsize(&fmt, &mut i, opt)?;
        pos += gettoalign(pos, h.align, opt, size);
        if opt == b's' {
            return Err(mlua::Error::runtime("option 's' has no fixed size"));
        }
        if opt == b'c' && size == 0 {
            return Err(mlua::Error::runtime("option 'c0' has no fixed size"));
        }
        if !opt.is_ascii_alphanumeric() {
            controloptions(&fmt, &mut i, opt, &mut h)?;
        }
        pos += size;
    }
    Ok(Value::Integer(pos as i64))
}

/// Register the global `struct` table (`luaopen_struct`): `pack`, `unpack`
/// and `size` only, with no callable metatable.
pub fn install_struct(lua: &Lua) -> mlua::Result<()> {
    let t = lua.create_table()?;
    t.set(
        "pack",
        lib_fn(lua, |lua, args: MultiValue| struct_pack(lua, &args))?,
    )?;
    t.set(
        "unpack",
        lib_fn(lua, |lua, args: MultiValue| struct_unpack(lua, &args))?,
    )?;
    t.set(
        "size",
        lib_fn(lua, |lua, args: MultiValue| struct_size(lua, &args))?,
    )?;
    lua.globals().set("struct", t)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// cmsgpack (lua-cmsgpack 0.4.0, `dfly_lua_cmsgpack.c`)
// ---------------------------------------------------------------------------

/// Max tables nesting while encoding (`LUACMSGPACK_MAX_NESTING`).
const MAX_NESTING: usize = 16;

/// `IS_INT64_EQUIVALENT`: a double exactly representable as an i64.
fn is_int64_equivalent(n: f64) -> bool {
    !n.is_infinite()
        && !n.is_nan()
        && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&n)
        && n.fract() == 0.0
}

fn mp_encode_bytes(out: &mut Vec<u8>, s: &[u8]) {
    let len = s.len();
    if len < 32 {
        out.push(0xa0 | (len as u8));
    } else if len <= 0xff {
        out.push(0xd9);
        out.push(len as u8);
    } else if len <= 0xffff {
        out.push(0xda);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xdb);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(s);
}

fn mp_encode_double(out: &mut Vec<u8>, d: f64) {
    let f = d as f32;
    if d == f as f64 {
        out.push(0xca);
        out.extend_from_slice(&f.to_bits().to_be_bytes());
    } else {
        out.push(0xcb);
        out.extend_from_slice(&d.to_bits().to_be_bytes());
    }
}

fn mp_encode_int(out: &mut Vec<u8>, n: i64) {
    if n >= 0 {
        if n <= 127 {
            out.push(n as u8);
        } else if n <= 0xff {
            out.push(0xcc);
            out.push(n as u8);
        } else if n <= 0xffff {
            out.push(0xcd);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        } else if n <= 0xffff_ffff {
            out.push(0xce);
            out.extend_from_slice(&(n as u32).to_be_bytes());
        } else {
            out.push(0xcf);
            out.extend_from_slice(&(n as u64).to_be_bytes());
        }
    } else if n >= -32 {
        out.push(n as u8);
    } else if n >= -128 {
        out.push(0xd0);
        out.push(n as u8);
    } else if n >= -32768 {
        out.push(0xd1);
        out.extend_from_slice(&(n as i16).to_be_bytes());
    } else if n >= -2_147_483_648 {
        out.push(0xd2);
        out.extend_from_slice(&(n as i32).to_be_bytes());
    } else {
        out.push(0xd3);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

fn mp_encode_array(out: &mut Vec<u8>, n: usize) {
    if n <= 15 {
        out.push(0x90 | (n as u8));
    } else if n <= 65535 {
        out.push(0xdc);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    }
}

fn mp_encode_map(out: &mut Vec<u8>, n: usize) {
    if n <= 15 {
        out.push(0x80 | (n as u8));
    } else if n <= 65535 {
        out.push(0xde);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(0xdf);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    }
}

/// `table_is_an_array`: the C version returns true only for tables whose keys
/// are a contiguous integer run. An empty table is a map; `max == count` is a
/// dense array; a sparse table still counts when it has more than `2*count`
/// holes and the raw length equals the max key, or when key `max - count`
/// exists (the keys form a contiguous run ending at `max`).
fn table_is_an_array(t: &Table) -> mlua::Result<bool> {
    let mut count = 0i64;
    let mut max = 0i64;
    for pair in t.pairs::<Value, Value>() {
        let (k, _) = pair?;
        let k = match k {
            Value::Integer(i) => i,
            Value::Number(n) => {
                // Lua normalizes integral float keys to integers, so a float
                // key is out of `lua_Integer` range or non-integral.
                if n.fract() != 0.0
                    || !(-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&n)
                {
                    return Ok(false);
                }
                n as i64
            }
            _ => return Ok(false),
        };
        if k <= 0 || k > i64::from(i32::MAX) {
            return Ok(false);
        }
        max = max.max(k);
        count += 1;
    }
    if count == 0 {
        return Ok(false);
    }
    if max == count {
        return Ok(true);
    }
    if max <= i64::from(i32::MAX) / 2 && count < max / 2 && t.raw_len() as i64 == max {
        return Ok(true);
    }
    for pair in t.pairs::<Value, Value>() {
        if let (Value::Integer(i), _) = pair?
            && i == max - count
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn mp_encode_value(v: Value, level: usize, out: &mut Vec<u8>) -> mlua::Result<()> {
    let v = if matches!(v, Value::Table(_)) && level == MAX_NESTING {
        Value::Nil
    } else {
        v
    };
    match v {
        Value::String(s) => mp_encode_bytes(out, &s.as_bytes()),
        Value::Boolean(b) => out.push(if b { 0xc3 } else { 0xc2 }),
        Value::Integer(i) => mp_encode_int(out, i),
        Value::Number(n) => {
            if is_int64_equivalent(n) {
                mp_encode_int(out, n as i64);
            } else {
                mp_encode_double(out, n);
            }
        }
        Value::Table(t) => {
            if table_is_an_array(&t)? {
                let len = t.raw_len();
                mp_encode_array(out, len);
                for i in 1..=len {
                    let item: Value = t.raw_get(i)?;
                    mp_encode_value(item, level + 1, out)?;
                }
            } else {
                let count = t
                    .pairs::<Value, Value>()
                    .try_fold(0usize, |n, p| p.map(|_| n + 1))?;
                mp_encode_map(out, count);
                for pair in t.pairs::<Value, Value>() {
                    let (k, val) = pair?;
                    mp_encode_value(k, level + 1, out)?;
                    mp_encode_value(val, level + 1, out)?;
                }
            }
        }
        // nil, functions, userdata, etc. encode as msgpack nil.
        _ => out.push(0xc0),
    }
    Ok(())
}

fn mp_pack(lua: &Lua, args: &MultiValue) -> mlua::Result<Value> {
    if args.is_empty() {
        return Err(mlua::Error::runtime("MessagePack pack needs input."));
    }
    let mut concat = Vec::new();
    for v in args {
        let mut out = Vec::new();
        mp_encode_value(v.clone(), 0, &mut out)?;
        concat.extend_from_slice(&out);
    }
    Ok(Value::String(lua.create_string(&concat)?))
}

/// Decode errors: the C library flags the cursor (`MP_CUR_ERROR_*`) and
/// `mp_unpack_full` turns them into these messages; a Lua-side error (e.g. a
/// nil map key) propagates as-is.
enum CurErr {
    Eof,
    BadFmt,
    Lua(mlua::Error),
}

struct MpCur<'a> {
    data: &'a [u8],
    pos: usize,
}

impl MpCur<'_> {
    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }
}

fn mp_need(cur: &MpCur, n: usize) -> Result<(), CurErr> {
    if cur.remaining() < n {
        Err(CurErr::Eof)
    } else {
        Ok(())
    }
}

fn mp_decode_array(lua: &Lua, cur: &mut MpCur, len: usize) -> Result<Value, CurErr> {
    let t = lua.create_table().map_err(CurErr::Lua)?;
    for i in 1..=len {
        let v = mp_decode_value(lua, cur)?;
        t.raw_seti(i, v).map_err(CurErr::Lua)?;
    }
    Ok(Value::Table(t))
}

fn mp_decode_map(lua: &Lua, cur: &mut MpCur, len: usize) -> Result<Value, CurErr> {
    let t = lua.create_table().map_err(CurErr::Lua)?;
    for _ in 0..len {
        let k = mp_decode_value(lua, cur)?;
        let v = mp_decode_value(lua, cur)?;
        if matches!(k, Value::Nil) {
            return Err(CurErr::Lua(mlua::Error::runtime("table index is nil")));
        }
        t.raw_set(k, v).map_err(CurErr::Lua)?;
    }
    Ok(Value::Table(t))
}

fn mp_decode_value(lua: &Lua, cur: &mut MpCur) -> Result<Value, CurErr> {
    mp_need(cur, 1)?;
    let b = cur.data[cur.pos];
    match b {
        0xcc => {
            mp_need(cur, 2)?;
            cur.pos += 2;
            Ok(Value::Integer(cur.data[cur.pos - 1] as i64))
        }
        0xd0 => {
            mp_need(cur, 2)?;
            cur.pos += 2;
            Ok(Value::Integer(cur.data[cur.pos - 1] as i8 as i64))
        }
        0xcd => {
            mp_need(cur, 3)?;
            let v = u16::from_be_bytes([cur.data[cur.pos + 1], cur.data[cur.pos + 2]]);
            cur.pos += 3;
            Ok(Value::Integer(v as i64))
        }
        0xd1 => {
            mp_need(cur, 3)?;
            let v = i16::from_be_bytes([cur.data[cur.pos + 1], cur.data[cur.pos + 2]]);
            cur.pos += 3;
            Ok(Value::Integer(v as i64))
        }
        0xce => {
            mp_need(cur, 5)?;
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&cur.data[cur.pos + 1..cur.pos + 5]);
            cur.pos += 5;
            Ok(Value::Integer(u32::from_be_bytes(bytes) as i64))
        }
        0xd2 => {
            mp_need(cur, 5)?;
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&cur.data[cur.pos + 1..cur.pos + 5]);
            cur.pos += 5;
            Ok(Value::Integer(i32::from_be_bytes(bytes) as i64))
        }
        0xcf => {
            mp_need(cur, 9)?;
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&cur.data[cur.pos + 1..cur.pos + 9]);
            cur.pos += 9;
            // `lua_pushunsigned` on 64-bit is a plain integer push.
            Ok(Value::Integer(u64::from_be_bytes(bytes) as i64))
        }
        0xd3 => {
            mp_need(cur, 9)?;
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&cur.data[cur.pos + 1..cur.pos + 9]);
            cur.pos += 9;
            Ok(Value::Integer(i64::from_be_bytes(bytes)))
        }
        0xc0 => {
            cur.pos += 1;
            Ok(Value::Nil)
        }
        0xc3 => {
            cur.pos += 1;
            Ok(Value::Boolean(true))
        }
        0xc2 => {
            cur.pos += 1;
            Ok(Value::Boolean(false))
        }
        0xca => {
            mp_need(cur, 5)?;
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&cur.data[cur.pos + 1..cur.pos + 5]);
            cur.pos += 5;
            Ok(Value::Number(
                f32::from_bits(u32::from_be_bytes(bytes)) as f64
            ))
        }
        0xcb => {
            mp_need(cur, 9)?;
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&cur.data[cur.pos + 1..cur.pos + 9]);
            cur.pos += 9;
            Ok(Value::Number(f64::from_bits(u64::from_be_bytes(bytes))))
        }
        0xd9 => {
            mp_need(cur, 2)?;
            let l = cur.data[cur.pos + 1] as usize;
            mp_need(cur, 2 + l)?;
            let s = cur.data[cur.pos + 2..cur.pos + 2 + l].to_vec();
            cur.pos += 2 + l;
            Ok(Value::String(lua.create_string(&s).map_err(CurErr::Lua)?))
        }
        0xda => {
            mp_need(cur, 3)?;
            let l = u16::from_be_bytes([cur.data[cur.pos + 1], cur.data[cur.pos + 2]]) as usize;
            mp_need(cur, 3 + l)?;
            let s = cur.data[cur.pos + 3..cur.pos + 3 + l].to_vec();
            cur.pos += 3 + l;
            Ok(Value::String(lua.create_string(&s).map_err(CurErr::Lua)?))
        }
        0xdb => {
            mp_need(cur, 5)?;
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&cur.data[cur.pos + 1..cur.pos + 5]);
            let l = u32::from_be_bytes(bytes) as usize;
            mp_need(cur, l)?;
            let s = cur.data[cur.pos + 5..cur.pos + 5 + l].to_vec();
            cur.pos += 5 + l;
            Ok(Value::String(lua.create_string(&s).map_err(CurErr::Lua)?))
        }
        0xdc => {
            mp_need(cur, 3)?;
            let l = u16::from_be_bytes([cur.data[cur.pos + 1], cur.data[cur.pos + 2]]) as usize;
            cur.pos += 3;
            mp_decode_array(lua, cur, l)
        }
        0xdd => {
            mp_need(cur, 5)?;
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&cur.data[cur.pos + 1..cur.pos + 5]);
            let l = u32::from_be_bytes(bytes) as usize;
            cur.pos += 5;
            mp_decode_array(lua, cur, l)
        }
        0xde => {
            mp_need(cur, 3)?;
            let l = u16::from_be_bytes([cur.data[cur.pos + 1], cur.data[cur.pos + 2]]) as usize;
            cur.pos += 3;
            mp_decode_map(lua, cur, l)
        }
        0xdf => {
            mp_need(cur, 5)?;
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&cur.data[cur.pos + 1..cur.pos + 5]);
            let l = u32::from_be_bytes(bytes) as usize;
            cur.pos += 5;
            mp_decode_map(lua, cur, l)
        }
        _ => {
            if b & 0x80 == 0 {
                cur.pos += 1;
                Ok(Value::Integer(b as i64))
            } else if b & 0xe0 == 0xe0 {
                cur.pos += 1;
                Ok(Value::Integer(b as i8 as i64))
            } else if b & 0xe0 == 0xa0 {
                let l = (b & 0x1f) as usize;
                mp_need(cur, 1 + l)?;
                let s = cur.data[cur.pos + 1..cur.pos + 1 + l].to_vec();
                cur.pos += 1 + l;
                Ok(Value::String(lua.create_string(&s).map_err(CurErr::Lua)?))
            } else if b & 0xf0 == 0x90 {
                let l = (b & 0x0f) as usize;
                cur.pos += 1;
                mp_decode_array(lua, cur, l)
            } else if b & 0xf0 == 0x80 {
                let l = (b & 0x0f) as usize;
                cur.pos += 1;
                mp_decode_map(lua, cur, l)
            } else {
                Err(CurErr::BadFmt)
            }
        }
    }
}

fn mp_unpack_full(lua: &Lua, s: &[u8], limit: i64, offset: i64) -> mlua::Result<MultiValue> {
    let len = s.len() as i64;
    if offset < 0 || limit < 0 {
        // The reference passes `len` for the limit slot (a C bug kept for
        // parity).
        return Err(mlua::Error::runtime(format!(
            "Invalid request to unpack with offset of {offset} and limit of {len}."
        )));
    }
    if offset > len {
        return Err(mlua::Error::runtime(format!(
            "Start offset {offset} greater than input length {len}."
        )));
    }
    let decode_all = limit == 0 && offset == 0;
    let mut cur = MpCur {
        data: s,
        pos: offset as usize,
    };
    let mut results: Vec<Value> = Vec::new();
    let limit = if decode_all { i64::MAX } else { limit };
    while cur.remaining() > 0 && (results.len() as i64) < limit {
        let v = match mp_decode_value(lua, &mut cur) {
            Ok(v) => v,
            Err(CurErr::Eof) => return Err(mlua::Error::runtime("Missing bytes in input.")),
            Err(CurErr::BadFmt) => return Err(mlua::Error::runtime("Bad data format in input.")),
            Err(CurErr::Lua(e)) => return Err(e),
        };
        results.push(v);
    }
    if !decode_all {
        let next_offset = if cur.remaining() == 0 {
            -1
        } else {
            cur.pos as i64
        };
        results.insert(0, Value::Integer(next_offset));
    }
    Ok(MultiValue::from(results))
}

fn mp_unpack(lua: &Lua, args: &MultiValue) -> mlua::Result<MultiValue> {
    let s = string_arg(lua, arg(args, 0))?;
    mp_unpack_full(lua, &s, 0, 0)
}

fn mp_unpack_one(lua: &Lua, args: &MultiValue) -> mlua::Result<MultiValue> {
    let s = string_arg(lua, arg(args, 0))?;
    let offset = match arg(args, 1) {
        Value::Nil => 0,
        v => integer_arg(lua, v)?,
    };
    mp_unpack_full(lua, &s, 1, offset)
}

fn mp_unpack_limit(lua: &Lua, args: &MultiValue) -> mlua::Result<MultiValue> {
    let s = string_arg(lua, arg(args, 0))?;
    let limit = integer_arg(lua, arg(args, 1))?;
    let offset = match arg(args, 2) {
        Value::Nil => 0,
        v => integer_arg(lua, v)?,
    };
    mp_unpack_full(lua, &s, limit, offset)
}

/// Register the global `cmsgpack` table (`luaopen_cmsgpack`): `pack`,
/// `unpack`, `unpack_one`, `unpack_limit` plus the module metadata.
pub fn install_cmsgpack(lua: &Lua) -> mlua::Result<()> {
    let t = lua.create_table()?;
    t.set(
        "pack",
        lib_fn(lua, |lua, args: MultiValue| mp_pack(lua, &args))?,
    )?;
    t.set(
        "unpack",
        lib_fn(lua, |lua, args: MultiValue| mp_unpack(lua, &args))?,
    )?;
    t.set(
        "unpack_one",
        lib_fn(lua, |lua, args: MultiValue| mp_unpack_one(lua, &args))?,
    )?;
    t.set(
        "unpack_limit",
        lib_fn(lua, |lua, args: MultiValue| mp_unpack_limit(lua, &args))?,
    )?;
    t.set("_NAME", "cmsgpack")?;
    t.set("_VERSION", "lua-cmsgpack 0.4.0")?;
    t.set("_COPYRIGHT", "Copyright (C) 2012, Salvatore Sanfilippo")?;
    t.set("_DESCRIPTION", "MessagePack C implementation for Lua")?;
    lua.globals().set("cmsgpack", t)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// cjson (lua-cjson 2.1devel, `dfly_lua_cjson.c`)
// ---------------------------------------------------------------------------

/// Per-module configuration (`json_config_t`), shared by all functions in a
/// cjson table as their common upvalue.
#[derive(Clone)]
struct CjsonConfig {
    encode_sparse_convert: i32,
    encode_sparse_ratio: i32,
    encode_sparse_safe: i32,
    encode_max_depth: i32,
    encode_number_precision: i32,
    encode_keep_buffer: bool,
    encode_invalid_numbers: i32,
    decode_invalid_numbers: bool,
    decode_max_depth: i32,
}

impl Default for CjsonConfig {
    fn default() -> Self {
        CjsonConfig {
            encode_sparse_convert: 0,
            encode_sparse_ratio: 2,
            encode_sparse_safe: 10,
            encode_max_depth: 1000,
            encode_number_precision: 14,
            encode_keep_buffer: true,
            encode_invalid_numbers: 0,
            decode_invalid_numbers: true,
            decode_max_depth: 1000,
        }
    }
}

fn encode_exception(typename: &str, reason: &str) -> mlua::Error {
    mlua::Error::runtime(format!("Cannot serialise {typename}: {reason}"))
}

/// `char2escape`: the escape strings for control characters, `"`, `\`, `/`
/// and DEL; all other bytes pass through untouched.
fn json_append_string(ctx: &mut EncodeCtx, s: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    ctx.out.push(b'"');
    for &b in s {
        match b {
            0x08 => ctx.out.extend_from_slice(b"\\b"),
            0x09 => ctx.out.extend_from_slice(b"\\t"),
            0x0a => ctx.out.extend_from_slice(b"\\n"),
            0x0c => ctx.out.extend_from_slice(b"\\f"),
            0x0d => ctx.out.extend_from_slice(b"\\r"),
            0x22 => ctx.out.extend_from_slice(b"\\\""),
            0x5c => ctx.out.extend_from_slice(b"\\\\"),
            0x2f => ctx.out.extend_from_slice(b"\\/"),
            0x7f => ctx.out.extend_from_slice(b"\\u007f"),
            b if b < 0x20 => {
                ctx.out.extend_from_slice(b"\\u00");
                ctx.out.push(HEX[(b >> 4) as usize]);
                ctx.out.push(HEX[(b & 0x0f) as usize]);
            }
            b => ctx.out.push(b),
        }
    }
    ctx.out.push(b'"');
}

struct EncodeCtx<'a> {
    cfg: &'a CjsonConfig,
    out: Vec<u8>,
}

fn json_append_number(ctx: &mut EncodeCtx, num: f64) -> mlua::Result<()> {
    match ctx.cfg.encode_invalid_numbers {
        0 => {
            if num.is_infinite() || num.is_nan() {
                return Err(encode_exception("number", "must not be NaN or Inf"));
            }
        }
        1 => {
            if num.is_nan() {
                ctx.out.extend_from_slice(b"nan");
                return Ok(());
            }
        }
        _ => {
            if num.is_infinite() || num.is_nan() {
                ctx.out.extend_from_slice(b"null");
                return Ok(());
            }
        }
    }
    if num == 0.0 {
        ctx.out
            .extend_from_slice(if num.is_sign_negative() { b"-0" } else { b"0" });
    } else if num.is_infinite() {
        ctx.out
            .extend_from_slice(if num < 0.0 { b"-inf" } else { b"inf" });
    } else {
        ctx.out
            .extend_from_slice(format_g(num, ctx.cfg.encode_number_precision).as_bytes());
    }
    Ok(())
}

fn json_check_encode_depth(ctx: &mut EncodeCtx, depth: i32) -> mlua::Result<()> {
    if depth <= ctx.cfg.encode_max_depth {
        return Ok(());
    }
    Err(mlua::Error::runtime(format!(
        "Cannot serialise, excessive nesting ({depth})"
    )))
}

/// `lua_array_length`: -1 for a non-array, otherwise the largest index.
fn lua_array_length(lua: &Lua, ctx: &mut EncodeCtx, t: &Table) -> mlua::Result<Option<i32>> {
    let mut max = 0i32;
    let mut items = 0i32;
    for pair in t.pairs::<Value, Value>() {
        let (k, _) = pair?;
        let k = lua.coerce_number(k)?.unwrap_or(0.0);
        if k.fract() == 0.0 && k >= 1.0 {
            max = max.max(k as i32);
            items += 1;
        } else {
            return Ok(None);
        }
    }
    if ctx.cfg.encode_sparse_ratio > 0
        && max > items * ctx.cfg.encode_sparse_ratio
        && max > ctx.cfg.encode_sparse_safe
    {
        if ctx.cfg.encode_sparse_convert == 0 {
            return Err(encode_exception("table", "excessively sparse array"));
        }
        return Ok(None);
    }
    Ok(Some(max))
}

fn json_append_array(
    lua: &Lua,
    ctx: &mut EncodeCtx,
    depth: i32,
    t: &Table,
    len: i32,
) -> mlua::Result<()> {
    ctx.out.push(b'[');
    let mut comma = false;
    for i in 1..=len {
        if comma {
            ctx.out.push(b',');
        } else {
            comma = true;
        }
        let v: Value = t.raw_get(i)?;
        json_append_data(lua, ctx, depth, &v)?;
    }
    ctx.out.push(b']');
    Ok(())
}

fn json_append_object(lua: &Lua, ctx: &mut EncodeCtx, depth: i32, t: &Table) -> mlua::Result<()> {
    ctx.out.push(b'{');
    let mut comma = false;
    for pair in t.pairs::<Value, Value>() {
        let (k, v) = pair?;
        if comma {
            ctx.out.push(b',');
        } else {
            comma = true;
        }
        match &k {
            Value::Integer(i) => {
                ctx.out.push(b'"');
                json_append_number(ctx, *i as f64)?;
                ctx.out.extend_from_slice(b"\":");
            }
            Value::Number(d) => {
                ctx.out.push(b'"');
                json_append_number(ctx, *d)?;
                ctx.out.extend_from_slice(b"\":");
            }
            Value::String(s) => {
                json_append_string(ctx, &s.as_bytes());
                ctx.out.push(b':');
            }
            other => {
                return Err(encode_exception(
                    other.type_name(),
                    "table key must be a number or string",
                ));
            }
        }
        json_append_data(lua, ctx, depth, &v)?;
    }
    ctx.out.push(b'}');
    Ok(())
}

fn json_append_data(lua: &Lua, ctx: &mut EncodeCtx, depth: i32, v: &Value) -> mlua::Result<()> {
    match v {
        Value::String(s) => {
            json_append_string(ctx, &s.as_bytes());
            Ok(())
        }
        Value::Integer(i) => json_append_number(ctx, *i as f64),
        Value::Number(d) => json_append_number(ctx, *d),
        Value::Boolean(b) => {
            ctx.out
                .extend_from_slice(if *b { b"true" } else { b"false" });
            Ok(())
        }
        Value::Table(t) => {
            let depth = depth + 1;
            json_check_encode_depth(ctx, depth)?;
            // `len > 0` decides array vs object, so an empty table is "{}".
            match lua_array_length(lua, ctx, t)? {
                Some(len) if len > 0 => json_append_array(lua, ctx, depth, t, len),
                _ => json_append_object(lua, ctx, depth, t),
            }
        }
        Value::Nil => {
            ctx.out.extend_from_slice(b"null");
            Ok(())
        }
        Value::LightUserData(lud) => {
            if lud.0.is_null() {
                ctx.out.extend_from_slice(b"null");
                Ok(())
            } else {
                Err(encode_exception("lightuserdata", "type not supported"))
            }
        }
        Value::Function(_) => Err(encode_exception("function", "type not supported")),
        Value::Thread(_) => Err(encode_exception("thread", "type not supported")),
        Value::UserData(_) | Value::Error(_) | Value::Other(_) => {
            Err(encode_exception("userdata", "type not supported"))
        }
    }
}

fn json_encode(lua: &Lua, cfg: &Mutex<CjsonConfig>, v: &Value) -> mlua::Result<Value> {
    let cfg = cfg.lock().unwrap();
    let mut ctx = EncodeCtx {
        cfg: &cfg,
        out: Vec::new(),
    };
    json_append_data(lua, &mut ctx, 0, v)?;
    Ok(Value::String(lua.create_string(&ctx.out)?))
}

// ----- Decoding -----

#[derive(Clone, Copy, PartialEq)]
enum TokTy {
    ObjBegin,
    ObjEnd,
    ArrBegin,
    ArrEnd,
    Str,
    Num,
    Bool,
    Null,
    Colon,
    Comma,
    End,
    Err,
    /// A bare number/string/literal start; `next_token` resolves it in-place
    /// and it never reaches [`JsonParser::throw_parse_error`].
    Unknown,
}

impl TokTy {
    fn name(self) -> &'static str {
        match self {
            TokTy::ObjBegin => "T_OBJ_BEGIN",
            TokTy::ObjEnd => "T_OBJ_END",
            TokTy::ArrBegin => "T_ARR_BEGIN",
            TokTy::ArrEnd => "T_ARR_END",
            TokTy::Str => "T_STRING",
            TokTy::Num => "T_NUMBER",
            TokTy::Bool => "T_BOOLEAN",
            TokTy::Null => "T_NULL",
            TokTy::Colon => "T_COLON",
            TokTy::Comma => "T_COMMA",
            TokTy::End => "T_END",
            TokTy::Err => "T_ERROR",
            TokTy::Unknown => "T_UNKNOWN",
        }
    }
}

struct Token {
    ty: TokTy,
    index: usize,
    string: Vec<u8>,
    number: f64,
    boolean: bool,
}

struct JsonParser<'a> {
    data: &'a [u8],
    ptr: usize,
    current_depth: i32,
    tmp: Vec<u8>,
    decode_invalid_numbers: bool,
    decode_max_depth: i32,
}

fn hexdigit2int(hex: u8) -> Option<i32> {
    match hex {
        b'0'..=b'9' => Some(i32::from(hex - b'0')),
        b'a'..=b'f' => Some(10 + i32::from(hex - b'a')),
        b'A'..=b'F' => Some(10 + i32::from(hex - b'A')),
        _ => None,
    }
}

fn decode_hex4(data: &[u8], pos: usize) -> Option<i32> {
    if pos + 4 > data.len() {
        return None;
    }
    let mut v = 0i32;
    for i in 0..4 {
        v = v * 16 + hexdigit2int(data[pos + i])?;
    }
    Some(v)
}

/// Append the UTF-8 encoding of `cp` to `out`; None when out of range.
fn codepoint_to_utf8(cp: i32, out: &mut Vec<u8>) -> Option<usize> {
    if cp <= 0x7F {
        out.push(cp as u8);
        Some(1)
    } else if cp <= 0x7FF {
        out.push(((cp >> 6) | 0xC0) as u8);
        out.push(((cp & 0x3F) | 0x80) as u8);
        Some(2)
    } else if cp <= 0xFFFF {
        out.push(((cp >> 12) | 0xE0) as u8);
        out.push((((cp >> 6) & 0x3F) | 0x80) as u8);
        out.push(((cp & 0x3F) | 0x80) as u8);
        Some(3)
    } else if cp <= 0x001F_FFFF {
        out.push(((cp >> 18) | 0xF0) as u8);
        out.push((((cp >> 12) & 0x3F) | 0x80) as u8);
        out.push((((cp >> 6) & 0x3F) | 0x80) as u8);
        out.push(((cp & 0x3F) | 0x80) as u8);
        Some(4)
    } else {
        None
    }
}

impl JsonParser<'_> {
    fn ch(&self, off: usize) -> u8 {
        self.data.get(self.ptr + off).copied().unwrap_or(0)
    }

    fn err_token(index: usize, msg: &'static str) -> Token {
        Token {
            ty: TokTy::Err,
            index,
            string: msg.as_bytes().to_vec(),
            number: 0.0,
            boolean: false,
        }
    }

    fn is_invalid_number(&self) -> bool {
        let mut p = self.ptr;
        if self.ch(0) == b'+' {
            return true;
        }
        if self.ch(0) == b'-' {
            p += 1;
        }
        let c = self.data.get(p).copied().unwrap_or(0);
        if c == b'0' {
            let c2 = self.data.get(p + 1).copied().unwrap_or(0);
            if (c2 | 0x20) == b'x' || c2.is_ascii_digit() {
                return true;
            }
            return false;
        } else if c <= b'9' {
            return false;
        }
        let tail = &self.data[p.min(self.data.len())..];
        if tail.len() >= 3 {
            let word = &tail[..3];
            if word.eq_ignore_ascii_case(b"inf") || word.eq_ignore_ascii_case(b"nan") {
                return true;
            }
        }
        false
    }

    fn number_token(&mut self, index: usize) -> Token {
        let (num, consumed) = c_strtod(&self.data[self.ptr..]);
        if consumed == 0 {
            return Self::err_token(index, "invalid number");
        }
        self.ptr += consumed;
        Token {
            ty: TokTy::Num,
            index,
            number: num,
            string: Vec::new(),
            boolean: false,
        }
    }

    fn string_token(&mut self, index: usize) -> Token {
        self.ptr += 1; // skip opening quote
        self.tmp.clear();
        loop {
            let ch = self.ch(0);
            if ch == b'"' {
                break;
            }
            if ch == 0 {
                return Self::err_token(index, "unexpected end of string");
            }
            if ch == b'\\' {
                match self.ch(1) {
                    b'u' => {
                        if self.append_unicode_escape().is_err() {
                            return Self::err_token(index, "invalid unicode escape code");
                        }
                        continue;
                    }
                    b'"' | b'\\' | b'/' => self.tmp.push(self.ch(1)),
                    b'b' => self.tmp.push(0x08),
                    b't' => self.tmp.push(0x09),
                    b'n' => self.tmp.push(0x0a),
                    b'f' => self.tmp.push(0x0c),
                    b'r' => self.tmp.push(0x0d),
                    _ => return Self::err_token(index, "invalid escape code"),
                }
                self.ptr += 1;
            }
            self.tmp.push(ch);
            self.ptr += 1;
        }
        self.ptr += 1; // eat closing quote
        Token {
            ty: TokTy::Str,
            index,
            string: std::mem::take(&mut self.tmp),
            number: 0.0,
            boolean: false,
        }
    }

    fn append_unicode_escape(&mut self) -> Result<(), ()> {
        let cp = decode_hex4(self.data, self.ptr + 2).ok_or(())?;
        let mut escape_len = 6;
        let cp = if (cp & 0xF800) == 0xD800 {
            // Error if the first surrogate is not high.
            if cp & 0x400 != 0 {
                return Err(());
            }
            // The next code unit must be a `\u` escape.
            if self.ch(escape_len) != b'\\' || self.ch(escape_len + 1) != b'u' {
                return Err(());
            }
            let low = decode_hex4(self.data, self.ptr + 2 + escape_len).ok_or(())?;
            // Error if the second code is not a low surrogate.
            if (low & 0xFC00) != 0xDC00 {
                return Err(());
            }
            escape_len = 12;
            ((cp & 0x3FF) << 10) | (low & 0x3FF) | 0x10000
        } else {
            cp
        };
        codepoint_to_utf8(cp, &mut self.tmp).ok_or(())?;
        self.ptr += escape_len;
        Ok(())
    }

    fn next_token(&mut self) -> Token {
        // Eat whitespace.
        while matches!(self.ch(0), b' ' | b'\t' | b'\n' | b'\r') {
            self.ptr += 1;
        }
        let index = self.ptr;
        let ch = self.ch(0);
        let ty = match ch {
            b'{' => TokTy::ObjBegin,
            b'}' => TokTy::ObjEnd,
            b'[' => TokTy::ArrBegin,
            b']' => TokTy::ArrEnd,
            b',' => TokTy::Comma,
            b':' => TokTy::Colon,
            0 => TokTy::End,
            b'"' | b'+' | b'-' | b'0'..=b'9' | b'f' | b'i' | b'I' | b'n' | b'N' | b't' => {
                TokTy::Unknown
            }
            _ => TokTy::Err,
        };
        match ty {
            TokTy::Err => Self::err_token(index, "invalid token"),
            TokTy::End => Token {
                ty: TokTy::End,
                index,
                string: Vec::new(),
                number: 0.0,
                boolean: false,
            },
            TokTy::Unknown => {
                if ch == b'"' {
                    return self.string_token(index);
                }
                if ch == b'-' || ch.is_ascii_digit() {
                    if !self.decode_invalid_numbers && self.is_invalid_number() {
                        return Self::err_token(index, "invalid number");
                    }
                    return self.number_token(index);
                }
                if self.data[self.ptr..].starts_with(b"true") {
                    self.ptr += 4;
                    Token {
                        ty: TokTy::Bool,
                        index,
                        boolean: true,
                        string: Vec::new(),
                        number: 0.0,
                    }
                } else if self.data[self.ptr..].starts_with(b"false") {
                    self.ptr += 5;
                    Token {
                        ty: TokTy::Bool,
                        index,
                        boolean: false,
                        string: Vec::new(),
                        number: 0.0,
                    }
                } else if self.data[self.ptr..].starts_with(b"null") {
                    self.ptr += 4;
                    Token {
                        ty: TokTy::Null,
                        index,
                        string: Vec::new(),
                        number: 0.0,
                        boolean: false,
                    }
                } else if self.decode_invalid_numbers && self.is_invalid_number() {
                    self.number_token(index)
                } else {
                    Self::err_token(index, "invalid token")
                }
            }
            _ => {
                self.ptr += 1;
                Token {
                    ty,
                    index,
                    string: Vec::new(),
                    number: 0.0,
                    boolean: false,
                }
            }
        }
    }

    fn throw_parse_error(exp: &str, token: &Token) -> mlua::Error {
        let found = if token.ty == TokTy::Err {
            String::from_utf8_lossy(&token.string).into_owned()
        } else {
            token.ty.name().to_string()
        };
        mlua::Error::runtime(format!(
            "Expected {exp} but found {found} at character {}",
            token.index + 1
        ))
    }

    fn descend(&mut self) -> mlua::Result<()> {
        self.current_depth += 1;
        if self.current_depth <= self.decode_max_depth {
            Ok(())
        } else {
            Err(mlua::Error::runtime(format!(
                "Found too many nested data structures ({}) at character {}",
                self.current_depth, self.ptr
            )))
        }
    }

    fn parse_object(&mut self, lua: &Lua) -> mlua::Result<Value> {
        self.descend()?;
        let t = lua.create_table()?;
        let mut token = self.next_token();
        if token.ty == TokTy::ObjEnd {
            self.current_depth -= 1;
            return Ok(Value::Table(t));
        }
        loop {
            if token.ty != TokTy::Str {
                return Err(Self::throw_parse_error("object key string", &token));
            }
            let key = lua.create_string(&token.string)?;
            token = self.next_token();
            if token.ty != TokTy::Colon {
                return Err(Self::throw_parse_error("colon", &token));
            }
            token = self.next_token();
            let value = self.process_value(lua, &token)?;
            t.raw_set(key, value)?;
            token = self.next_token();
            if token.ty == TokTy::ObjEnd {
                self.current_depth -= 1;
                return Ok(Value::Table(t));
            }
            if token.ty != TokTy::Comma {
                return Err(Self::throw_parse_error("comma or object end", &token));
            }
            token = self.next_token();
        }
    }

    fn parse_array(&mut self, lua: &Lua) -> mlua::Result<Value> {
        self.descend()?;
        let t = lua.create_table()?;
        let mut token = self.next_token();
        if token.ty == TokTy::ArrEnd {
            self.current_depth -= 1;
            return Ok(Value::Table(t));
        }
        let mut i = 1usize;
        loop {
            let value = self.process_value(lua, &token)?;
            t.raw_seti(i, value)?;
            i += 1;
            token = self.next_token();
            if token.ty == TokTy::ArrEnd {
                self.current_depth -= 1;
                return Ok(Value::Table(t));
            }
            if token.ty != TokTy::Comma {
                return Err(Self::throw_parse_error("comma or array end", &token));
            }
            token = self.next_token();
        }
    }

    fn process_value(&mut self, lua: &Lua, token: &Token) -> mlua::Result<Value> {
        match token.ty {
            TokTy::Str => Ok(Value::String(lua.create_string(&token.string)?)),
            TokTy::Num => {
                let d = token.number;
                // Dragonfly: integral numbers within `lua_Integer` decode as
                // integers, so `tostring` produces "42" not "42.0".
                if d.fract() == 0.0
                    && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&d)
                {
                    Ok(Value::Integer(d as i64))
                } else {
                    Ok(Value::Number(d))
                }
            }
            TokTy::Bool => Ok(Value::Boolean(token.boolean)),
            TokTy::ObjBegin => self.parse_object(lua),
            TokTy::ArrBegin => self.parse_array(lua),
            TokTy::Null => Ok(Value::NULL),
            _ => Err(Self::throw_parse_error("value", token)),
        }
    }
}

/// C `strtod()`-compatible prefix parse for the C locale (`.` decimal point):
/// `strtod` reads the longest valid number, including hex floats and
/// `inf`/`infinity`/`nan`, and reports how many bytes it consumed.
fn c_strtod(s: &[u8]) -> (f64, usize) {
    let mut i = 0;
    while i < s.len() && s[i].is_ascii_whitespace() {
        i += 1;
    }
    let start = i;
    let mut neg = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        neg = s[i] == b'-';
        i += 1;
    }
    if s[i..].len() >= 3 && s[i..i + 3].eq_ignore_ascii_case(b"inf") {
        let (val, n) = if s[i..].len() >= 8 && s[i..i + 8].eq_ignore_ascii_case(b"infinity") {
            (f64::INFINITY, 8)
        } else {
            (f64::INFINITY, 3)
        };
        return ((if neg { -val } else { val }), i + n);
    }
    if s[i..].len() >= 3 && s[i..i + 3].eq_ignore_ascii_case(b"nan") {
        let nan = if neg { -f64::NAN } else { f64::NAN };
        return (nan, i + 3);
    }
    if s[i..].len() >= 2 && s[i] == b'0' && (s[i + 1] | 0x20) == b'x' {
        let (val, n) = c_strtod_hex(s, i);
        return ((if neg { -val } else { val }), n);
    }
    let mut j = i;
    let mut int_digits = 0;
    while j < s.len() && s[j].is_ascii_digit() {
        j += 1;
        int_digits += 1;
    }
    let mut frac_digits = 0;
    if j < s.len() && s[j] == b'.' {
        j += 1;
        while j < s.len() && s[j].is_ascii_digit() {
            j += 1;
            frac_digits += 1;
        }
    }
    if int_digits == 0 && frac_digits == 0 {
        // Nothing numeric: `strtod` consumes nothing (endptr == nptr).
        return (0.0, start);
    }
    let mut exp_start = j;
    if j < s.len() && (s[j] == b'e' || s[j] == b'E') {
        let mut k = j + 1;
        if k < s.len() && (s[k] == b'+' || s[k] == b'-') {
            k += 1;
        }
        let mut exp_digits = 0;
        while k < s.len() && s[k].is_ascii_digit() {
            k += 1;
            exp_digits += 1;
        }
        if exp_digits > 0 {
            j = k;
        } else {
            exp_start = j;
        }
    }
    let _ = exp_start;
    let num: f64 = std::str::from_utf8(&s[i..j])
        .unwrap()
        .parse()
        .unwrap_or(0.0);
    (if neg { -num } else { num }, j)
}

/// Hex float form `0x…[.…][p…]`, as accepted by `strtod`.
fn c_strtod_hex(s: &[u8], i: usize) -> (f64, usize) {
    let mut j = i + 2;
    let mut int_digits = 0;
    while j < s.len() && s[j].is_ascii_hexdigit() {
        j += 1;
        int_digits += 1;
    }
    let mut frac_digits = 0;
    if j < s.len() && s[j] == b'.' {
        j += 1;
        while j < s.len() && s[j].is_ascii_hexdigit() {
            j += 1;
            frac_digits += 1;
        }
    }
    if int_digits == 0 && frac_digits == 0 {
        // A bare "0x" is just "0".
        return (0.0, i + 1);
    }
    let mut mantissa = 0.0f64;
    let int_part = &s[i + 2..i + 2 + int_digits];
    for &d in int_part {
        mantissa = mantissa * 16.0 + hexdigit2int(d).unwrap_or(0) as f64;
    }
    let frac_part = &s[i + 2 + int_digits..i + 2 + int_digits + frac_digits];
    let mut weight = 1.0 / 16.0f64;
    for &d in frac_part {
        mantissa += hexdigit2int(d).unwrap_or(0) as f64 * weight;
        weight /= 16.0f64;
    }
    let mut exp = 0i32;
    let mut consumed = j;
    if j < s.len() && (s[j] == b'p' || s[j] == b'P') {
        let mut k = j + 1;
        let mut neg_exp = false;
        if k < s.len() && (s[k] == b'+' || s[k] == b'-') {
            neg_exp = s[k] == b'-';
            k += 1;
        }
        let mut digits = 0;
        let mut e = 0i32;
        while k < s.len() && s[k].is_ascii_digit() {
            e = e * 10 + i32::from(s[k] - b'0');
            k += 1;
            digits += 1;
        }
        if digits > 0 {
            exp = if neg_exp { -e } else { e };
            consumed = k;
        }
    }
    let value = mantissa * 2f64.powi(exp);
    (value, consumed)
}

fn json_decode(lua: &Lua, cfg: &Mutex<CjsonConfig>, data: &[u8]) -> mlua::Result<Value> {
    if data.len() >= 2 && (data[0] == 0 || data[1] == 0) {
        return Err(mlua::Error::runtime(
            "JSON parser does not support UTF-16 or UTF-32",
        ));
    }
    let (decode_invalid_numbers, decode_max_depth) = {
        let c = cfg.lock().unwrap();
        (c.decode_invalid_numbers, c.decode_max_depth)
    };
    let mut p = JsonParser {
        data,
        ptr: 0,
        current_depth: 0,
        tmp: Vec::new(),
        decode_invalid_numbers,
        decode_max_depth,
    };
    let token = p.next_token();
    let v = p.process_value(lua, &token)?;
    let end = p.next_token();
    if end.ty != TokTy::End {
        return Err(JsonParser::throw_parse_error("the end", &end));
    }
    Ok(v)
}

// ----- Configuration functions -----

fn cfg_arg_check(nargs: usize, max: usize) -> mlua::Result<()> {
    if nargs > max {
        Err(mlua::Error::runtime("found too many arguments"))
    } else {
        Ok(())
    }
}

fn integer_option(
    lua: &Lua,
    v: &Value,
    setting: i32,
    min: i32,
    max: i32,
) -> mlua::Result<(i32, Value)> {
    if matches!(v, Value::Nil) {
        return Ok((setting, Value::Integer(setting as i64)));
    }
    let value = integer_arg(lua, v.clone())? as i32;
    if !(min..=max).contains(&value) {
        return Err(mlua::Error::runtime(format!(
            "expected integer between {min} and {max}"
        )));
    }
    Ok((value, Value::Integer(value as i64)))
}

/// `json_enum_option` / `luaL_checkoption` semantics: a boolean when `bool_true`
/// is set, otherwise any value `luaL_checkstring` accepts (strings, and numbers
/// coerced to their text form) matched case-sensitively against `options`.
fn enum_option(
    lua: &Lua,
    v: &Value,
    options: &[&str],
    bool_true: i32,
    setting: i32,
) -> mlua::Result<(i32, Value)> {
    let mut setting = setting;
    if !matches!(v, Value::Nil) {
        if bool_true != 0 && matches!(v, Value::Boolean(_)) {
            setting = if v.as_boolean().unwrap() {
                bool_true
            } else {
                0
            };
        } else {
            let s = string_arg(lua, v.clone())?;
            let idx = options
                .iter()
                .position(|o| o.as_bytes() == s.as_slice())
                .ok_or_else(|| {
                    mlua::Error::runtime(format!(
                        "invalid option '{}'",
                        String::from_utf8_lossy(&s)
                    ))
                })?;
            setting = idx as i32;
        }
    }
    let out = if bool_true != 0 && (setting == 0 || setting == bool_true) {
        Value::Boolean(setting != 0)
    } else {
        Value::String(lua.create_string(options[setting as usize])?)
    };
    Ok((setting, out))
}

fn cfg_encode_sparse_array(
    lua: &Lua,
    cfg: &Mutex<CjsonConfig>,
    args: &MultiValue,
) -> mlua::Result<MultiValue> {
    cfg_arg_check(args.len(), 3)?;
    let mut c = cfg.lock().unwrap();
    let (convert, convert_v) = enum_option(
        lua,
        &arg(args, 0),
        &["off", "on"],
        1,
        c.encode_sparse_convert,
    )?;
    c.encode_sparse_convert = convert;
    let (ratio, ratio_v) = integer_option(lua, &arg(args, 1), c.encode_sparse_ratio, 0, i32::MAX)?;
    c.encode_sparse_ratio = ratio;
    let (safe, safe_v) = integer_option(lua, &arg(args, 2), c.encode_sparse_safe, 0, i32::MAX)?;
    c.encode_sparse_safe = safe;
    Ok(MultiValue::from(vec![convert_v, ratio_v, safe_v]))
}

fn cfg_single_integer(
    lua: &Lua,
    cfg: &Mutex<CjsonConfig>,
    args: &MultiValue,
    field: impl Fn(&mut CjsonConfig) -> &mut i32,
    min: i32,
) -> mlua::Result<Value> {
    cfg_arg_check(args.len(), 1)?;
    let mut c = cfg.lock().unwrap();
    let setting = *field(&mut c);
    let (v, out) = integer_option(lua, &arg(args, 0), setting, min, i32::MAX)?;
    *field(&mut c) = v;
    Ok(out)
}

fn cfg_encode_keep_buffer(
    lua: &Lua,
    cfg: &Mutex<CjsonConfig>,
    args: &MultiValue,
) -> mlua::Result<Value> {
    cfg_arg_check(args.len(), 1)?;
    let mut c = cfg.lock().unwrap();
    let (v, out) = enum_option(
        lua,
        &arg(args, 0),
        &["off", "on"],
        1,
        c.encode_keep_buffer as i32,
    )?;
    c.encode_keep_buffer = v != 0;
    Ok(out)
}

fn cfg_encode_invalid_numbers(
    lua: &Lua,
    cfg: &Mutex<CjsonConfig>,
    args: &MultiValue,
) -> mlua::Result<Value> {
    cfg_arg_check(args.len(), 1)?;
    let mut c = cfg.lock().unwrap();
    let (v, out) = enum_option(
        lua,
        &arg(args, 0),
        &["off", "on", "null"],
        0,
        c.encode_invalid_numbers,
    )?;
    c.encode_invalid_numbers = v;
    Ok(out)
}

fn cfg_decode_invalid_numbers(
    lua: &Lua,
    cfg: &Mutex<CjsonConfig>,
    args: &MultiValue,
) -> mlua::Result<Value> {
    cfg_arg_check(args.len(), 1)?;
    let mut c = cfg.lock().unwrap();
    let (v, out) = enum_option(
        lua,
        &arg(args, 0),
        &["off", "on"],
        0,
        c.decode_invalid_numbers as i32,
    )?;
    c.decode_invalid_numbers = v != 0;
    Ok(out)
}

/// Build one cjson module table with its own config (`lua_cjson_new`).
fn cjson_table(lua: &Lua, cfg: &Arc<Mutex<CjsonConfig>>) -> mlua::Result<Table> {
    let t = lua.create_table()?;

    let c = Arc::clone(cfg);
    t.set(
        "encode",
        lib_fn(lua, move |lua, args: MultiValue| -> mlua::Result<Value> {
            if args.len() != 1 {
                return Err(mlua::Error::runtime("expected 1 argument"));
            }
            json_encode(lua, &c, &arg(&args, 0))
        })?,
    )?;

    let c = Arc::clone(cfg);
    t.set(
        "decode",
        lib_fn(lua, move |lua, args: MultiValue| -> mlua::Result<Value> {
            if args.len() != 1 {
                return Err(mlua::Error::runtime("expected 1 argument"));
            }
            json_decode(lua, &c, &string_arg(lua, arg(&args, 0))?)
        })?,
    )?;

    let c = Arc::clone(cfg);
    t.set(
        "encode_sparse_array",
        lib_fn(lua, move |lua, args: MultiValue| {
            cfg_encode_sparse_array(lua, &c, &args)
        })?,
    )?;

    let c = Arc::clone(cfg);
    t.set(
        "encode_max_depth",
        lib_fn(lua, move |lua, args: MultiValue| {
            cfg_single_integer(lua, &c, &args, |c| &mut c.encode_max_depth, 1)
        })?,
    )?;

    let c = Arc::clone(cfg);
    t.set(
        "decode_max_depth",
        lib_fn(lua, move |lua, args: MultiValue| {
            cfg_single_integer(lua, &c, &args, |c| &mut c.decode_max_depth, 1)
        })?,
    )?;

    let c = Arc::clone(cfg);
    t.set(
        "encode_number_precision",
        lib_fn(lua, move |lua, args: MultiValue| {
            cfg_single_integer(lua, &c, &args, |c| &mut c.encode_number_precision, 1)
        })?,
    )?;

    let c = Arc::clone(cfg);
    t.set(
        "encode_keep_buffer",
        lib_fn(lua, move |lua, args: MultiValue| {
            cfg_encode_keep_buffer(lua, &c, &args)
        })?,
    )?;

    let c = Arc::clone(cfg);
    t.set(
        "encode_invalid_numbers",
        lib_fn(lua, move |lua, args: MultiValue| {
            cfg_encode_invalid_numbers(lua, &c, &args)
        })?,
    )?;

    let c = Arc::clone(cfg);
    t.set(
        "decode_invalid_numbers",
        lib_fn(lua, move |lua, args: MultiValue| {
            cfg_decode_invalid_numbers(lua, &c, &args)
        })?,
    )?;

    t.set("new", lib_fn(lua, |lua, _: MultiValue| cjson_new(lua))?)?;

    t.set("null", Value::NULL)?;
    t.set("_NAME", "cjson")?;
    t.set("_VERSION", "2.1devel")?;
    Ok(t)
}

/// A fresh cjson module with its own config (`lua_cjson_new`).
fn cjson_new(lua: &Lua) -> mlua::Result<Value> {
    let cfg = Arc::new(Mutex::new(CjsonConfig::default()));
    Ok(Value::Table(cjson_table(lua, &cfg)?))
}

/// Register the global `cjson` table (`luaopen_cjson`). Dragonfly always
/// installs it as a global (Redis guards it behind `ENABLE_CJSON_GLOBAL`).
pub fn install_cjson(lua: &Lua) -> mlua::Result<()> {
    let cfg = Arc::new(Mutex::new(CjsonConfig::default()));
    let t = cjson_table(lua, &cfg)?;
    lua.globals().set("cjson", t)?;
    Ok(())
}

/// Install all four extension libraries in the reference's `LoadLibrary`
/// order (`interpreter.cc:426-429`).
pub fn install_all(lua: &Lua) -> mlua::Result<()> {
    install_cjson(lua)?;
    install_struct(lua)?;
    install_cmsgpack(lua)?;
    install_bit(lua)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use crate::commands::lua::{SandboxedInterpreter, ScriptDispatch};
    use crate::error::RespValue;

    struct Noop;

    impl ScriptDispatch for Noop {
        fn dispatch(&mut self, _: Vec<Vec<u8>>) -> Result<RespValue, String> {
            Err("ERR noop".into())
        }
    }

    fn no_kill() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    fn eval(script: &str) -> Result<RespValue, String> {
        let interp = SandboxedInterpreter::new().unwrap();
        interp.define("aaaaaaaaaaaaaaaaaaaa", script.as_bytes())?;
        interp.run("aaaaaaaaaaaaaaaaaaaa", &mut Noop, false, &no_kill())
    }

    fn bulk(s: impl AsRef<[u8]>) -> RespValue {
        RespValue::Bulk(s.as_ref().to_vec())
    }

    fn integer(i: i64) -> RespValue {
        RespValue::Integer(i)
    }

    // ----- bit -----

    #[test]
    fn bit_basic_ops() {
        assert_eq!(eval("return bit.band(1, 2)").unwrap(), integer(0));
        assert_eq!(eval("return bit.band(7, 3)").unwrap(), integer(3));
        assert_eq!(eval("return bit.bor(1, 2, 4)").unwrap(), integer(7));
        assert_eq!(eval("return bit.bxor(255, 1)").unwrap(), integer(254));
        assert_eq!(eval("return bit.bnot(0)").unwrap(), integer(-1));
        assert_eq!(eval("return bit.lshift(1, 4)").unwrap(), integer(16));
        assert_eq!(eval("return bit.rshift(256, 4)").unwrap(), integer(16));
        assert_eq!(eval("return bit.arshift(-16, 1)").unwrap(), integer(-8));
        assert_eq!(eval("return bit.arshift(-1, 1)").unwrap(), integer(-1));
        assert_eq!(eval("return bit.tobit(0xffffffff)").unwrap(), integer(-1));
        assert_eq!(eval("return bit.tobit(-1)").unwrap(), integer(-1));
        assert_eq!(eval("return bit.rol(1, 1)").unwrap(), integer(2));
        assert_eq!(eval("return bit.ror(2, 1)").unwrap(), integer(1));
        assert_eq!(
            eval("return bit.bswap(0x12345678)").unwrap(),
            integer(0x7856_3412)
        );
    }

    #[test]
    fn bit_fractional_and_negative_args() {
        // `barg` rounds via 2^52+2^51 like Lua BitOp: 3.9 rounds to 4.
        assert_eq!(eval("return bit.tobit(3.9)").unwrap(), integer(4));
        assert_eq!(eval("return bit.tobit(-3.9)").unwrap(), integer(-4));
        assert_eq!(eval("return bit.tobit('5')").unwrap(), integer(5));
        assert_eq!(
            eval("return bit.band(0xffffffff, 0xff)").unwrap(),
            integer(255)
        );
    }

    #[test]
    fn bit_tohex() {
        assert_eq!(eval("return bit.tohex(-1)").unwrap(), bulk("ffffffff"));
        assert_eq!(eval("return bit.tohex(0x12)").unwrap(), bulk("00000012"));
        // No width: the default is 8 digits, zero-padded.
        assert_eq!(
            eval("return bit.tohex(0xABCDEF)").unwrap(),
            bulk("00abcdef")
        );
        assert_eq!(eval("return bit.tohex(255, 4)").unwrap(), bulk("00ff"));
        // A negative width selects uppercase digits.
        assert_eq!(
            eval("return bit.tohex(0xdeadbeef, -4)").unwrap(),
            bulk("BEEF")
        );
        assert_eq!(eval("return bit.tohex(0xdeadbeef, 0)").unwrap(), bulk(""));
    }

    #[test]
    fn bit_shift_round_trips() {
        assert_eq!(
            eval("return bit.rol(bit.ror(0x12345678, 4), 4)").unwrap(),
            integer(0x1234_5678)
        );
        assert_eq!(
            eval("return bit.lshift(bit.rshift(0x0fffffff, 3), 3)").unwrap(),
            integer(0x0fff_fff8)
        );
    }

    // ----- struct -----

    #[test]
    fn struct_pack_endianness() {
        assert_eq!(eval("return struct.pack('B', 65)").unwrap(), bulk("\x41"));
        assert_eq!(
            eval("return struct.pack('>I4', 0x01020304)").unwrap(),
            bulk("\x01\x02\x03\x04")
        );
        assert_eq!(
            eval("return struct.pack('<I4', 0x01020304)").unwrap(),
            bulk("\x04\x03\x02\x01")
        );
        assert_eq!(
            eval("return struct.pack('<I2', 258)").unwrap(),
            bulk("\x02\x01")
        );
        assert_eq!(
            eval("return struct.pack('>I2', 258)").unwrap(),
            bulk("\x01\x02")
        );
        assert_eq!(eval("return struct.pack('x')").unwrap(), bulk("\x00"));
    }

    #[test]
    fn struct_pack_floats() {
        // 1.5 as f32 is 0x3fc00000, 2.5 as f64 is 0x4004000000000000.
        assert_eq!(
            eval("return struct.pack('>f', 1.5)").unwrap(),
            bulk(b"\x3f\xc0\x00\x00")
        );
        assert_eq!(
            eval("return struct.pack('<f', 1.5)").unwrap(),
            bulk(b"\x00\x00\xc0\x3f")
        );
        assert_eq!(
            eval("return struct.pack('>d', 2.5)").unwrap(),
            bulk("\x40\x04\x00\x00\x00\x00\x00\x00")
        );
    }

    #[test]
    fn struct_unpack_values() {
        assert_eq!(
            eval("return struct.unpack('>I2', '\\1\\2')").unwrap(),
            RespValue::Double(258.0)
        );
        assert_eq!(
            eval("return struct.unpack('<I2', '\\1\\2')").unwrap(),
            RespValue::Double(513.0)
        );
        assert_eq!(
            eval("return struct.unpack('b', '\\xff')").unwrap(),
            RespValue::Double(-1.0)
        );
        assert_eq!(
            eval("return struct.unpack('B', '\\xff')").unwrap(),
            RespValue::Double(255.0)
        );
    }

    #[test]
    fn struct_string_and_position() {
        assert_eq!(
            eval("local a, p = struct.unpack('c2', 'ab'); return {a, p}").unwrap(),
            RespValue::Array(vec![bulk("ab"), integer(3)])
        );
        // `s` consumes the terminating NUL too, so the next position is 4.
        assert_eq!(
            eval("local a, p = struct.unpack('s', 'hi\\0x'); return {a, p}").unwrap(),
            RespValue::Array(vec![bulk("hi"), integer(4)])
        );
        // `c0` reuses the previous result as the length (consuming it, per the
        // C `lua_pop(L, 1); n--`), so only the string and next position return.
        assert_eq!(
            eval("local a, p = struct.unpack('>B c0', '\\3abc'); return {a, p}").unwrap(),
            RespValue::Array(vec![bulk("abc"), integer(5)])
        );
    }

    #[test]
    fn struct_size() {
        assert_eq!(eval("return struct.size('>I4')").unwrap(), integer(4));
        assert_eq!(eval("return struct.size('c2')").unwrap(), integer(2));
        assert_eq!(eval("return struct.size('>H I4')").unwrap(), integer(6));
        assert!(eval("return struct.size('c0')").is_err());
        assert!(eval("return struct.size('s')").is_err());
    }

    #[test]
    fn struct_errors() {
        assert!(eval("return struct.pack('B')").is_err());
        assert!(
            eval("return struct.unpack('I4', 'ab')").is_err(),
            "data string too short"
        );
        assert!(
            eval("return struct.unpack('>B', 'x', 0)").is_err(),
            "offset must be >= 1"
        );
        assert!(
            eval("return struct.unpack('s', 'no-nul')").is_err(),
            "unfinished string"
        );
    }

    // ----- cmsgpack -----

    #[test]
    fn msgpack_pack_bytes() {
        assert_eq!(
            eval("return cmsgpack.pack(1, 'a', {1, 2})").unwrap(),
            bulk(b"\x01\xa1\x61\x92\x01\x02")
        );
        // Empty tables are maps, exactly like the C library.
        assert_eq!(eval("return cmsgpack.pack({})").unwrap(), bulk(b"\x80"));
        assert_eq!(eval("return cmsgpack.pack(true)").unwrap(), bulk(b"\xc3"));
        assert_eq!(eval("return cmsgpack.pack(false)").unwrap(), bulk(b"\xc2"));
        assert_eq!(eval("return cmsgpack.pack(nil)").unwrap(), bulk(b"\xc0"));
        assert_eq!(
            eval("return cmsgpack.pack(255)").unwrap(),
            bulk(b"\xcc\xff")
        );
        assert_eq!(
            eval("return cmsgpack.pack(256)").unwrap(),
            bulk(b"\xcd\x01\x00")
        );
        // 0.5 and 1.5 are exactly representable in f32, so a float32 marker is
        // used (`mp_encode_double`: `d == (double)(float)d`).
        assert_eq!(
            eval("return cmsgpack.pack(0.5)").unwrap(),
            bulk(b"\xca\x3f\x00\x00\x00")
        );
        assert_eq!(
            eval("return cmsgpack.pack(1.5)").unwrap(),
            bulk(b"\xca\x3f\xc0\x00\x00")
        );
        assert_eq!(
            eval("return cmsgpack.pack({a = 1})").unwrap(),
            bulk(b"\x81\xa1\x61\x01")
        );
    }

    #[test]
    fn msgpack_pack_errors() {
        let err = eval("return cmsgpack.pack()").unwrap_err();
        assert!(err.contains("MessagePack pack needs input."), "{err}");
    }

    #[test]
    fn msgpack_unpack_stream() {
        // `unpack` decodes every top-level value.
        let script = "local a, b, c = cmsgpack.unpack(cmsgpack.pack(1, 'a', {1, 2})); return {a, b, c[1], c[2]}";
        assert_eq!(
            eval(script).unwrap(),
            RespValue::Array(vec![integer(1), bulk("a"), integer(1), integer(2)])
        );
        assert_eq!(
            eval("return cmsgpack.unpack('\\x92\\x01\\x02')").unwrap(),
            RespValue::Array(vec![integer(1), integer(2)])
        );
        assert_eq!(
            eval("local t = cmsgpack.unpack('\\x81\\xa1\\x61\\x01'); return t.a").unwrap(),
            integer(1)
        );
    }

    #[test]
    fn msgpack_unpack_one_and_limit() {
        // unpack_one returns the next offset followed by the single value;
        // -1 means the whole buffer was consumed.
        let script = "local off, v = cmsgpack.unpack_one(cmsgpack.pack(1)); return {off, v}";
        assert_eq!(
            eval(script).unwrap(),
            RespValue::Array(vec![integer(-1), integer(1)])
        );
        let script = "local off, v = cmsgpack.unpack_one(cmsgpack.pack(1, 2)); return {off, v}";
        assert_eq!(
            eval(script).unwrap(),
            RespValue::Array(vec![integer(1), integer(1)])
        );
        // unpack_limit takes a limit then an offset.
        let script = "local off, a, b = cmsgpack.unpack_limit('\\1\\2', 2); return {off, a, b}";
        assert_eq!(
            eval(script).unwrap(),
            RespValue::Array(vec![integer(-1), integer(1), integer(2)])
        );
        let script = "local off, a = cmsgpack.unpack_limit('\\1\\2\\3', 1); return {off, a}";
        assert_eq!(
            eval(script).unwrap(),
            RespValue::Array(vec![integer(1), integer(1)])
        );
    }

    #[test]
    fn msgpack_unpack_errors() {
        // An empty buffer yields nothing (nil), exactly like the C library.
        assert_eq!(eval("return cmsgpack.unpack('')").unwrap(), RespValue::Nil);
        let err = eval("return cmsgpack.unpack('\\xd9')").unwrap_err();
        assert!(err.contains("Missing bytes in input."), "{err}");
        let err = eval("return cmsgpack.unpack('\\xc1')").unwrap_err();
        assert!(err.contains("Bad data format in input."), "{err}");
        let err = eval("return cmsgpack.unpack_one('\\1', -1)").unwrap_err();
        assert!(err.contains("Invalid request to unpack"), "{err}");
        let err = eval("return cmsgpack.unpack_limit('\\1', 1, 5)").unwrap_err();
        assert!(
            err.contains("Start offset 5 greater than input length 1."),
            "{err}"
        );
    }

    // ----- cjson -----

    #[test]
    fn cjson_encode_primitives() {
        assert_eq!(eval("return cjson.encode('hi')").unwrap(), bulk("\"hi\""));
        assert_eq!(eval("return cjson.encode(3.14)").unwrap(), bulk("3.14"));
        assert_eq!(eval("return cjson.encode(42)").unwrap(), bulk("42"));
        assert_eq!(eval("return cjson.encode(-0.0)").unwrap(), bulk("-0"));
        assert_eq!(eval("return cjson.encode(true)").unwrap(), bulk("true"));
        assert_eq!(eval("return cjson.encode(false)").unwrap(), bulk("false"));
        assert_eq!(eval("return cjson.encode(nil)").unwrap(), bulk("null"));
        assert_eq!(
            eval("return cjson.encode(cjson.null)").unwrap(),
            bulk("null")
        );
        assert_eq!(eval("return cjson.encode('\\n')").unwrap(), bulk("\"\\n\""));
        assert_eq!(
            eval("return cjson.encode({['\\1'] = true})").unwrap(),
            bulk("{\"\\u0001\":true}")
        );
    }

    #[test]
    fn cjson_encode_tables() {
        assert_eq!(eval("return cjson.encode({})").unwrap(), bulk("{}"));
        assert_eq!(
            eval("return cjson.encode({1, 2, 3})").unwrap(),
            bulk("[1,2,3]")
        );
        assert_eq!(
            eval("return cjson.encode({a = 1})").unwrap(),
            bulk("{\"a\":1}")
        );
        assert_eq!(
            eval("return cjson.encode({['x y'] = 1})").unwrap(),
            bulk("{\"x y\":1}")
        );
        assert_eq!(
            eval("return cjson.encode({1, {2, {3}}})").unwrap(),
            bulk("[1,[2,[3]]]")
        );
        assert_eq!(
            eval("return cjson.encode({[1] = 'a', [3] = 'c'})").unwrap(),
            bulk("[\"a\",null,\"c\"]")
        );
    }

    #[test]
    fn cjson_sparse_arrays() {
        // Defaults: ratio 2, safe 10 — a sparse table within limits encodes
        // as an array with null holes.
        assert_eq!(
            eval("return cjson.encode({1, [5] = 2})").unwrap(),
            bulk("[1,null,null,null,2]")
        );
        // Beyond the safe limit it errors by default...
        let err = eval("return cjson.encode({1, [100] = 2})").unwrap_err();
        assert!(err.contains("excessively sparse array"), "{err}");
        // ...but with convert enabled it encodes as an object instead.
        assert_eq!(
            eval("cjson.encode_sparse_array('on', 1, 0); return cjson.encode({[100] = 2})")
                .unwrap(),
            bulk("{\"100\":2}")
        );
    }

    #[test]
    fn cjson_decode_types() {
        assert_eq!(eval("return cjson.decode('true')").unwrap(), integer(1));
        assert_eq!(eval("return cjson.decode('null')").unwrap(), RespValue::Nil);
        assert_eq!(eval("return cjson.decode('\"hi\"')").unwrap(), bulk("hi"));
        assert_eq!(
            eval("return cjson.decode('{\"s\": \"\\\\u0041\"}').s").unwrap(),
            bulk("A")
        );
        // Dragonfly converts integral numbers to integers.
        assert_eq!(
            eval("return cjson.decode('{\"id\": 42}').id").unwrap(),
            integer(42)
        );
        assert_eq!(
            eval("return cjson.decode('[1, 2.5]')[2]").unwrap(),
            RespValue::Double(2.5)
        );
        assert_eq!(
            eval("local t = cjson.decode('{\"a\": {\"b\": [1, 2]}}'); return t.a.b[2]").unwrap(),
            integer(2)
        );
        assert_eq!(
            eval("return cjson.encode(cjson.decode('[1, 2]'))").unwrap(),
            bulk("[1,2]")
        );
    }

    #[test]
    fn cjson_decode_errors() {
        let err = eval("return cjson.decode('{bad')").unwrap_err();
        assert!(
            err.contains("Expected object key string but found invalid token at character 2"),
            "{err}"
        );
        let err = eval("return cjson.decode('{\"a\":1} junk')").unwrap_err();
        assert!(
            err.contains("Expected the end but found invalid token at character 9"),
            "{err}"
        );
        assert!(eval("return cjson.decode('[1,]')").is_err());
    }

    #[test]
    fn cjson_decode_invalid_numbers() {
        // Default (on): nan parses as a Lua number.
        let v = eval("return cjson.decode('[nan]')[1]").unwrap();
        let RespValue::Double(d) = v else {
            panic!("expected double, got {v:?}");
        };
        assert!(d.is_nan());
        // Off: invalid numbers are rejected as an unrecognised token.
        let err =
            eval("cjson.decode_invalid_numbers('off'); return cjson.decode('[nan]')").unwrap_err();
        assert!(
            err.contains("Expected value but found invalid token at character 2"),
            "{err}"
        );
    }

    #[test]
    fn cjson_invalid_number_encoding() {
        let err = eval("return cjson.encode(1/0)").unwrap_err();
        assert!(
            err.contains("Cannot serialise number: must not be NaN or Inf"),
            "{err}"
        );
        assert_eq!(
            eval("cjson.encode_invalid_numbers('on'); return cjson.encode(1/0)").unwrap(),
            bulk("inf")
        );
        assert_eq!(
            eval("cjson.encode_invalid_numbers('on'); return cjson.encode(0/0)").unwrap(),
            bulk("nan")
        );
        assert_eq!(
            eval("cjson.encode_invalid_numbers('null'); return cjson.encode(1/0)").unwrap(),
            bulk("null")
        );
    }

    #[test]
    fn cjson_number_precision() {
        assert_eq!(
            eval("return cjson.encode(123.456)").unwrap(),
            bulk("123.456")
        );
        assert_eq!(
            eval("cjson.encode_number_precision(2); return cjson.encode(123.456)").unwrap(),
            bulk("1.2e+02")
        );
        // The getter pushes the old precision as a number.
        assert_eq!(
            eval("return cjson.encode_number_precision()").unwrap(),
            integer(14)
        );
    }

    #[test]
    fn cjson_depth_limits() {
        let err = eval("cjson.encode_max_depth(1); return cjson.encode({1, {2}})").unwrap_err();
        assert!(err.contains("excessive nesting"), "{err}");
        let err = eval("cjson.decode_max_depth(1); return cjson.decode('[1, [2]]')").unwrap_err();
        assert!(
            err.contains("Found too many nested data structures"),
            "{err}"
        );
    }

    #[test]
    fn cjson_new_isolates_config() {
        assert_eq!(
            eval(
                "local m = cjson.new(); m.encode_number_precision(2); \
                 local a = m.encode(123.456); local b = cjson.encode(123.456); \
                 return {a, b}"
            )
            .unwrap(),
            RespValue::Array(vec![bulk("1.2e+02"), bulk("123.456")])
        );
    }

    #[test]
    fn cjson_metadata() {
        assert_eq!(eval("return cjson._VERSION").unwrap(), bulk("2.1devel"));
        assert_eq!(eval("return cjson._NAME").unwrap(), bulk("cjson"));
        assert_eq!(eval("return cjson.null").unwrap(), RespValue::Nil);
    }

    #[test]
    fn all_four_libraries_coexist() {
        assert_eq!(
            eval(
                "local j = cjson.encode({x = 1}); \
                 local s = struct.pack('B', 65); \
                 local m = cmsgpack.unpack(cmsgpack.pack(7)); \
                 return {j, s, m}"
            )
            .unwrap(),
            RespValue::Array(vec![bulk("{\"x\":1}"), bulk("A"), integer(7),])
        );
    }
}
