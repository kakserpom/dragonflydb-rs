//! Bloom filter commands (BF.RESERVE/ADD/MADD/EXISTS/MEXISTS/INFO/SCANDUMP/
//! LOADCHUNK), ported from `dragonfly/src/server/bloom_family.cc`.
//!
//! The BF family is a Rust `PrimeValue::Sbf` backed by `core::bloom::SBF`.

use crate::commands::{
    Command, FLAG_DENYOOM, FLAG_FAST, FLAG_READONLY, FLAG_WRITE, KeyRange, OpContext, integer, ok,
};
use crate::core::PrimeValue;
use crate::core::bloom::{
    K_DEFAULT_FP_PROB, K_DEFAULT_GROW_FACTOR, SBF, SbfDumpIterator, load_sbf_chunk, load_sbf_header,
};
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::{parse_double, parse_i64, parse_u64};

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

fn load_in_progress() -> RespError {
    RespError::new("ERR bloom filter load in progress")
}

fn no_such_key() -> RespError {
    RespError::new("ERR no such key")
}

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

/// Find the SBF for `key`, creating a default filter when the key is missing
/// (port of `OpAdd`'s `AddOrFind` + `SetSBF(0, kDefaultFpProb, kDefaultGrowFactor)`).
fn get_or_create_sbf<'c>(ctx: &'c mut OpContext<'_>, key: &[u8]) -> Result<&'c mut SBF, RespError> {
    let exists = match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Sbf(_)) => true,
        Some(_) => return Err(RespError::wrong_type()),
        None => false,
    };
    if !exists {
        ctx.db.insert(
            key,
            PrimeValue::Sbf(SBF::new(0, K_DEFAULT_FP_PROB, K_DEFAULT_GROW_FACTOR)),
        );
    }
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Sbf(sbf)) => Ok(sbf),
        _ => unreachable!("SBF present or just inserted"),
    }
}

// ---------------------------------------------------------------------------
// BF.RESERVE
// ---------------------------------------------------------------------------

/// Port of `CmdReserve` + `OpReserve`. Reply OK, or "item exists" when the key
/// already holds any value.
fn exec_bf_reserve(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let Some(error) = parse_double(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::syntax());
    };
    let capacity = match parse_u64(&ctx.args[key_idx + 2]) {
        Some(c) if u32::try_from(c).is_ok() => c,
        _ => return CmdResult::Err(RespError::syntax()),
    };
    if !(error > 0.0 && error < 0.5) {
        return CmdResult::Err(RespError::new("ERR error rate is out of range"));
    }
    if ctx.db.contains(key, ctx.now_ms) {
        return CmdResult::Err(RespError::new("ERR item exists"));
    }
    ctx.db.insert(
        key,
        PrimeValue::Sbf(SBF::new(capacity, error, K_DEFAULT_GROW_FACTOR)),
    );
    CmdResult::Ok(ok())
}

// ---------------------------------------------------------------------------
// BF.ADD / BF.MADD
// ---------------------------------------------------------------------------

fn add_or_madd(ctx: &mut OpContext, multi: bool) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let items = &ctx.args[key_idx + 1..];
    let sbf = match get_or_create_sbf(ctx, key) {
        Ok(s) => s,
        Err(e) => return CmdResult::Err(e),
    };
    if sbf.num_filters() == 0 {
        return CmdResult::Err(load_in_progress());
    }
    let mut results = Vec::with_capacity(items.len());
    for item in items {
        results.push(sbf.add(item));
    }
    if multi {
        CmdResult::Ok(RespValue::Array(
            results.into_iter().map(|b| integer(i64::from(b))).collect(),
        ))
    } else {
        CmdResult::Ok(integer(i64::from(results[0])))
    }
}

fn exec_bf_add(ctx: &mut OpContext) -> CmdResult {
    add_or_madd(ctx, false)
}

fn exec_bf_madd(ctx: &mut OpContext) -> CmdResult {
    add_or_madd(ctx, true)
}

// ---------------------------------------------------------------------------
// BF.EXISTS / BF.MEXISTS
// ---------------------------------------------------------------------------

/// Port of `CmdExists`/`CmdMExists` + `OpExists`: a missing or wrong-type key
/// reports 0 (no error); only a loading filter errors.
fn exists_or_mexists(ctx: &mut OpContext, multi: bool) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let items = &ctx.args[key_idx + 1..];
    let zeros = || {
        if multi {
            RespValue::Array(items.iter().map(|_| integer(0)).collect())
        } else {
            integer(0)
        }
    };
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Sbf(sbf)) => {
            if sbf.num_filters() == 0 {
                return CmdResult::Err(load_in_progress());
            }
            let results: Vec<bool> = items.iter().map(|it| sbf.exists(it)).collect();
            if multi {
                CmdResult::Ok(RespValue::Array(
                    results.into_iter().map(|b| integer(i64::from(b))).collect(),
                ))
            } else {
                CmdResult::Ok(integer(i64::from(results[0])))
            }
        }
        Some(_) | None => CmdResult::Ok(zeros()),
    }
}

fn exec_bf_exists(ctx: &mut OpContext) -> CmdResult {
    exists_or_mexists(ctx, false)
}

fn exec_bf_mexists(ctx: &mut OpContext) -> CmdResult {
    exists_or_mexists(ctx, true)
}

// ---------------------------------------------------------------------------
// BF.INFO
// ---------------------------------------------------------------------------

const BF_INFO_NAMES: [&str; 5] = [
    "Capacity",
    "Size",
    "Number of filters",
    "Number of items inserted",
    "Expansion rate",
];
const BF_INFO_SHORT: [&str; 5] = ["CAPACITY", "SIZE", "FILTERS", "ITEMS", "EXPANSION"];

fn exec_bf_info(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let field = ctx.args.get(key_idx + 1).map(|a| a.to_ascii_uppercase());
    if ctx.args.len() > key_idx + 2 {
        return CmdResult::Err(RespError::syntax());
    }
    let sbf = match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Sbf(sbf)) => sbf,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Err(no_such_key()),
    };
    if sbf.num_filters() == 0 {
        return CmdResult::Err(load_in_progress());
    }
    let values = [
        sbf.total_capacity() as i64,
        sbf.malloc_used() as i64,
        sbf.num_filters() as i64,
        sbf.total_items() as i64,
        sbf.grow_factor() as i64,
    ];
    if let Some(f) = field {
        for (i, name) in BF_INFO_SHORT.iter().enumerate() {
            if f.as_slice() == name.as_bytes() {
                return CmdResult::Ok(integer(values[i]));
            }
        }
        return CmdResult::Err(RespError::new("ERR Invalid info arguments"));
    }
    let mut arr = Vec::with_capacity(10);
    for i in 0..5 {
        arr.push(RespValue::Bulk(BF_INFO_NAMES[i].as_bytes().to_vec()));
        arr.push(integer(values[i]));
    }
    CmdResult::Ok(RespValue::Array(arr))
}

// ---------------------------------------------------------------------------
// BF.SCANDUMP
// ---------------------------------------------------------------------------

fn exec_bf_scandump(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let cursor = match parse_i64(&ctx.args[key_idx + 1]) {
        Some(c) if c >= 0 => c,
        _ => return CmdResult::Err(RespError::integer()),
    };
    let sbf = match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Sbf(sbf)) => sbf,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Err(no_such_key()),
    };
    if sbf.num_filters() == 0 {
        return CmdResult::Err(load_in_progress());
    }
    let mut it = SbfDumpIterator::new(sbf, cursor);
    let chunk = it.next_chunk();
    CmdResult::Ok(RespValue::Array(vec![
        integer(chunk.cursor),
        RespValue::Bulk(chunk.data),
    ]))
}

// ---------------------------------------------------------------------------
// BF.LOADCHUNK
// ---------------------------------------------------------------------------

fn exec_bf_loadchunk(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let cursor = match parse_i64(&ctx.args[key_idx + 1]) {
        Some(c) if c >= 1 => c,
        _ => return CmdResult::Err(RespError::integer()),
    };
    let blob = &ctx.args[key_idx + 2];

    if cursor == 1 {
        // Init phase: overwrite the key (of any type) with the parsed SBF.
        let Ok(sbf) = load_sbf_header(blob) else {
            return CmdResult::Err(RespError::new("ERR INVALIDOBJ invalid bloom dump payload"));
        };
        ctx.db.insert(key, PrimeValue::Sbf(sbf));
        ctx.db.clear_expiry(key);
        return CmdResult::Ok(ok());
    }

    // Continue loading chunks into the not-yet-fully-loaded filter.
    let sbf = match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Sbf(sbf)) => sbf,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Err(no_such_key()),
    };
    match load_sbf_chunk(cursor, blob, sbf) {
        Ok(()) => CmdResult::Ok(ok()),
        Err(_) => CmdResult::Err(RespError::out_of_range()),
    }
}

// ---------------------------------------------------------------------------
// Command definitions
// ---------------------------------------------------------------------------

pub static CMD_BF_RESERVE: Command = Command {
    name: "BF.RESERVE",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_bf_reserve,
    merge: None,
};
pub static CMD_BF_ADD: Command = Command {
    name: "BF.ADD",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_bf_add,
    merge: None,
};
pub static CMD_BF_MADD: Command = Command {
    name: "BF.MADD",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_bf_madd,
    merge: None,
};
pub static CMD_BF_EXISTS: Command = Command {
    name: "BF.EXISTS",
    arity: -3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_bf_exists,
    merge: None,
};
pub static CMD_BF_MEXISTS: Command = Command {
    name: "BF.MEXISTS",
    arity: -3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_bf_mexists,
    merge: None,
};
pub static CMD_BF_INFO: Command = Command {
    name: "BF.INFO",
    arity: -2,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_bf_info,
    merge: None,
};
pub static CMD_BF_SCANDUMP: Command = Command {
    name: "BF.SCANDUMP",
    arity: 3,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_bf_scandump,
    merge: None,
};
pub static CMD_BF_LOADCHUNK: Command = Command {
    name: "BF.LOADCHUNK",
    arity: 4,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    key_range: KeyRange::ONE,
    exec: exec_bf_loadchunk,
    merge: None,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::compact::CompactString;
    use crate::core::db::DbSlice;

    fn dispatch(db: &mut DbSlice, argv: &[Vec<u8>]) -> CmdResult {
        let (exec, first_key_idx, owned): (fn(&mut OpContext) -> CmdResult, usize, Vec<usize>) =
            match argv[0].as_slice() {
                b"BF.RESERVE" => (exec_bf_reserve, 1, vec![1]),
                b"BF.ADD" => (exec_bf_add, 1, vec![1]),
                b"BF.MADD" => (exec_bf_madd, 1, vec![1]),
                b"BF.EXISTS" => (exec_bf_exists, 1, vec![1]),
                b"BF.MEXISTS" => (exec_bf_mexists, 1, vec![1]),
                b"BF.INFO" => (exec_bf_info, 1, vec![1]),
                b"BF.SCANDUMP" => (exec_bf_scandump, 1, vec![1]),
                b"BF.LOADCHUNK" => (exec_bf_loadchunk, 1, vec![1]),
                _ => panic!("unhandled command {:?}", argv[0]),
            };
        let mut ctx = OpContext {
            db,
            args: argv,
            owned_keys: &owned,
            first_key_idx,
            conn_id: 0,
            now_ms: 0,
        };
        exec(&mut ctx)
    }

    macro_rules! run {
        ($db:expr, $($arg:expr),+ $(,)?) => {
            dispatch($db, &[$($arg.to_vec()),+])
        };
    }

    fn set(db: &mut DbSlice, key: &str, value: &str) {
        db.insert(key.as_bytes(), PrimeValue::Str(CompactString::from(value)));
    }

    fn int(r: CmdResult) -> i64 {
        match r.into_resp_value() {
            RespValue::Integer(v) => v,
            o => panic!("expected integer, got {o:?}"),
        }
    }

    fn ints(r: CmdResult) -> Vec<i64> {
        match r.into_resp_value() {
            RespValue::Array(a) => a
                .iter()
                .map(|v| match v {
                    RespValue::Integer(i) => *i,
                    o => panic!("expected integer element, got {o:?}"),
                })
                .collect(),
            o => panic!("expected array, got {o:?}"),
        }
    }

    fn err(r: CmdResult) -> String {
        match r {
            CmdResult::Err(e) => e.render().to_string(),
            o => panic!("expected error, got {:?}", o.into_resp_value()),
        }
    }

    fn ok_res(r: CmdResult) {
        match r.into_resp_value() {
            RespValue::Simple(s) => assert_eq!(s, "OK"),
            o => panic!("expected OK, got {o:?}"),
        }
    }

    fn type_of(db: &mut DbSlice, key: &str) -> String {
        match db.find(key.as_bytes(), 0) {
            Some(v) => v.type_name().to_string(),
            None => "none".to_string(),
        }
    }

    fn scandump_raw(db: &mut DbSlice, key: &str, cursor: i64) -> (i64, Vec<u8>) {
        let r = run!(
            db,
            b"BF.SCANDUMP",
            key.as_bytes(),
            cursor.to_string().as_bytes()
        );
        match r.into_resp_value() {
            RespValue::Array(v) => {
                let c = match &v[0] {
                    RespValue::Integer(i) => *i,
                    o => panic!("expected integer cursor, got {o:?}"),
                };
                let data = match &v[1] {
                    RespValue::Bulk(b) => b.clone(),
                    o => panic!("expected bulk data, got {o:?}"),
                };
                (c, data)
            }
            o => panic!("expected [cursor, data], got {o:?}"),
        }
    }

    /// Single-shard fast path of COPY (both keys on this shard).
    fn copy_key(db: &mut DbSlice, src: &str, dst: &str) {
        let val = db.find(src.as_bytes(), 0).expect("source exists").clone();
        db.insert(dst.as_bytes(), val);
    }

    #[test]
    fn basic() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"BF.RESERVE", b"b1", b"0.1", b"32"));
        assert_eq!(type_of(&mut db, "b1"), "MBbloom--");
        assert_eq!(1, int(run!(&mut db, b"BF.ADD", b"b1", b"a")));
        assert_eq!(1, int(run!(&mut db, b"BF.ADD", b"b1", b"b")));
        assert_eq!(0, int(run!(&mut db, b"BF.ADD", b"b1", b"b")));
        assert_eq!(1, int(run!(&mut db, b"BF.ADD", b"b2", b"b")));
        assert_eq!(type_of(&mut db, "b2"), "MBbloom--");
        assert_eq!(0, int(run!(&mut db, b"BF.EXISTS", b"b2", b"c")));
        assert_eq!(0, int(run!(&mut db, b"BF.EXISTS", b"b3", b"c")));
        assert_eq!(1, int(run!(&mut db, b"BF.EXISTS", b"b2", b"b")));
        set(&mut db, "str", "foo");
        assert_eq!(0, int(run!(&mut db, b"BF.EXISTS", b"str", b"b")));
    }

    #[test]
    fn multiple() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            vec![0, 0, 0],
            ints(run!(&mut db, b"BF.MEXISTS", b"bf1", b"a", b"b", b"c"))
        );

        set(&mut db, "str", "foo");
        assert_eq!(
            vec![0, 0, 0],
            ints(run!(&mut db, b"BF.MEXISTS", b"str", b"a", b"b", b"c"))
        );

        assert!(
            err(run!(&mut db, b"BF.MADD", b"str", b"a")).contains("WRONGTYPE"),
            "madd on string key"
        );

        assert_eq!(
            vec![1, 1, 1],
            ints(run!(&mut db, b"BF.MADD", b"bf1", b"a", b"b", b"c"))
        );
        assert_eq!(
            vec![0, 0, 0],
            ints(run!(&mut db, b"BF.MADD", b"bf1", b"a", b"b", b"c"))
        );
        assert_eq!(
            vec![1, 1, 1],
            ints(run!(&mut db, b"BF.MEXISTS", b"bf1", b"a", b"b", b"c"))
        );
    }

    #[test]
    fn scandump() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"BF.RESERVE", b"b1", b"0.01", b"1000"));
        for i in 0..100 {
            run!(&mut db, b"BF.ADD", b"b1", format!("item{i}").as_bytes());
        }

        let (cursor, data) = scandump_raw(&mut db, "b1", 0);
        assert_eq!(cursor, 1);
        assert!(!data.is_empty());

        let mut chunk_count = 1;
        let mut cursor = cursor;
        while cursor != 0 {
            let (next, data) = scandump_raw(&mut db, "b1", cursor);
            assert!(next > cursor || next == 0);
            cursor = next;
            if cursor != 0 {
                chunk_count += 1;
                assert!(!data.is_empty());
            } else {
                assert!(data.is_empty());
            }
        }
        assert!(chunk_count >= 1);
    }

    #[test]
    fn chunk_round_trip() {
        const TOTAL_ITEMS: usize = 100;
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"BF.RESERVE", b"b1", b"0.01", b"1000"));
        for i in 0..TOTAL_ITEMS {
            run!(&mut db, b"BF.ADD", b"b1", format!("item{i}").as_bytes());
        }

        let mut chunks: Vec<(i64, Vec<u8>)> = Vec::new();
        let mut cursor = 0i64;
        loop {
            let (next, data) = scandump_raw(&mut db, "b1", cursor);
            assert!(next > cursor || next == 0);
            cursor = next;
            if cursor != 0 {
                assert!(!data.is_empty());
                chunks.push((cursor, data));
            }
            if cursor == 0 {
                break;
            }
        }
        assert!(chunks.len() >= 2, "header + filter chunks");

        for (crs, data) in chunks {
            let data = data.clone();
            let mut argv = vec![b"BF.LOADCHUNK".to_vec(), b"b2".to_vec()];
            argv.push(crs.to_string().into_bytes());
            argv.push(data);
            ok_res(dispatch(&mut db, &argv));
        }

        for i in 0..TOTAL_ITEMS {
            assert_eq!(
                1,
                int(run!(
                    &mut db,
                    b"BF.EXISTS",
                    b"b2",
                    format!("item{i}").as_bytes()
                ))
            );
        }
    }

    #[test]
    fn scandump_past_end() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"BF.RESERVE", b"b1", b"0.01", b"100"));
        run!(&mut db, b"BF.ADD", b"b1", b"x");

        let (cursor, data) = scandump_raw(&mut db, "b1", 999_999);
        assert_eq!(cursor, 0);
        assert!(data.is_empty());
    }

    #[test]
    fn loadchunk_errors() {
        let mut db = DbSlice::new(0);
        assert!(
            err(run!(&mut db, b"BF.LOADCHUNK", b"b1", b"0", b"data")).contains("not an integer")
        );
        assert!(
            err(run!(&mut db, b"BF.LOADCHUNK", b"b1", b"-1", b"data")).contains("not an integer")
        );
    }

    #[test]
    fn info() {
        let mut db = DbSlice::new(0);
        assert!(
            err(run!(&mut db, b"BF.INFO", b"missing")).contains("no such key"),
            "missing key"
        );

        ok_res(run!(&mut db, b"BF.RESERVE", b"b1", b"0.01", b"1000"));
        match run!(&mut db, b"BF.INFO", b"b1").into_resp_value() {
            RespValue::Array(v) => {
                assert_eq!(v.len(), 10);
                assert_eq!(v[0], RespValue::Bulk(b"Capacity".to_vec()));
                assert_eq!(v[1], RespValue::Integer(1485));
                assert_eq!(v[2], RespValue::Bulk(b"Size".to_vec()));
                match &v[3] {
                    RespValue::Integer(n) => assert!(*n > 0),
                    o => panic!("expected size integer, got {o:?}"),
                }
                assert_eq!(v[4], RespValue::Bulk(b"Number of filters".to_vec()));
                assert_eq!(v[5], RespValue::Integer(1));
                assert_eq!(v[6], RespValue::Bulk(b"Number of items inserted".to_vec()));
                assert_eq!(v[7], RespValue::Integer(0));
                assert_eq!(v[8], RespValue::Bulk(b"Expansion rate".to_vec()));
                assert_eq!(v[9], RespValue::Integer(2));
            }
            o => panic!("expected info array, got {o:?}"),
        }

        for i in 0..10 {
            run!(&mut db, b"BF.ADD", b"b1", format!("item{i}").as_bytes());
        }
        assert_eq!(10, int(run!(&mut db, b"BF.INFO", b"b1", b"items")));
        assert_eq!(1, int(run!(&mut db, b"BF.INFO", b"b1", b"filters")));
        assert!(err(run!(&mut db, b"BF.INFO", b"b1", b"bogus")).contains("Invalid info arguments"));

        set(&mut db, "str", "foo");
        assert!(
            err(run!(&mut db, b"BF.INFO", b"str")).contains("WRONGTYPE"),
            "info on string key"
        );
    }

    #[test]
    fn copy_chunked_round_trip() {
        const TOTAL_ITEMS: usize = 100;
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"BF.RESERVE", b"b1", b"0.01", b"1000"));
        for i in 0..TOTAL_ITEMS {
            run!(&mut db, b"BF.ADD", b"b1", format!("item{i}").as_bytes());
        }

        copy_key(&mut db, "b1", "b2");
        assert_eq!(type_of(&mut db, "b2"), "MBbloom--");

        for i in 0..TOTAL_ITEMS {
            assert_eq!(
                1,
                int(run!(
                    &mut db,
                    b"BF.EXISTS",
                    b"b2",
                    format!("item{i}").as_bytes()
                ))
            );
        }
    }

    #[test]
    fn reserve_errors() {
        let mut db = DbSlice::new(0);
        // Error rate must be in (0, 0.5).
        assert!(
            err(run!(&mut db, b"BF.RESERVE", b"b1", b"0.9", b"32")).contains("error rate"),
            "too high error rate"
        );
        assert!(
            err(run!(&mut db, b"BF.RESERVE", b"b1", b"-0.1", b"32")).contains("error rate"),
            "negative error rate"
        );
        // Existing key (any type) reports "item exists".
        set(&mut db, "str", "foo");
        assert!(
            err(run!(&mut db, b"BF.RESERVE", b"str", b"0.1", b"32")).contains("item exists"),
            "reserve on existing key"
        );
        ok_res(run!(&mut db, b"BF.RESERVE", b"b2", b"0.1", b"32"));
        assert!(
            err(run!(&mut db, b"BF.RESERVE", b"b2", b"0.1", b"32")).contains("item exists"),
            "reserve on existing bloom"
        );
    }
}
