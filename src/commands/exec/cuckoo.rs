//! Cuckoo filter commands (CF.RESERVE/ADD/ADDNX/EXISTS/MEXISTS/INFO/COUNT/
//! DEL/INSERT/INSERTNX/COMPACT), ported from
//! `dragonfly/src/server/cuckoo_filter_family.cc`.
//!
//! The CF family is a Rust `PrimeValue::Cuckoo` backed by `core::cuckoo::CuckooFilter`.

use crate::commands::{
    Command, FLAG_DENYOOM, FLAG_FAST, FLAG_READONLY, FLAG_WRITE, KeyRange, OpContext, integer, ok,
};
use crate::core::PrimeValue;
use crate::core::cuckoo::{CuckooFilter, CuckooFilterOptions};
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::parse_u64;

/// `kDefaultCapacity`: the initial capacity used when CF.ADD/CF.ADDNX
/// auto-create a filter.
const K_DEFAULT_CAPACITY: u64 = 1024;

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

fn cf_capacity() -> RespError {
    RespError::new("ERR CF: capacity must be greater than 0")
}

fn cf_bucket_size() -> RespError {
    RespError::new("ERR CF: bucket size must be between 1 and 255")
}

fn cf_max_iterations() -> RespError {
    RespError::new("ERR CF: max iterations must be between 1 and 65535")
}

fn cf_expansion() -> RespError {
    RespError::new("ERR CF: expansion must be between 0 and 32767")
}

fn cf_filter_full() -> RespError {
    RespError::new("ERR Filter is full")
}

fn no_such_key() -> RespError {
    RespError::new("ERR no such key")
}

fn item_exists() -> RespError {
    RespError::new("ERR item exists")
}

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

/// Find the filter for `key`, creating a default-capacity filter when the key
/// is missing (port of `OpAdd`'s `AddOrFind` + `SetCuckooFilter(kDefaultCapacity)`).
fn get_or_create_cf<'c>(
    ctx: &'c mut OpContext<'_>,
    key: &[u8],
    capacity: u64,
) -> Result<&'c mut CuckooFilter, RespError> {
    let exists = match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Cuckoo(_)) => true,
        Some(_) => return Err(RespError::wrong_type()),
        None => false,
    };
    if !exists {
        ctx.db.insert(
            key,
            PrimeValue::Cuckoo(CuckooFilter::new(&CuckooFilterOptions {
                capacity,
                ..Default::default()
            })),
        );
    }
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Cuckoo(cf)) => Ok(cf),
        _ => unreachable!("cuckoo filter present or just inserted"),
    }
}

// ---------------------------------------------------------------------------
// CF.RESERVE
// ---------------------------------------------------------------------------

/// Parse the value following a CF.RESERVE option keyword.
fn parse_opt_value(args: &[Vec<u8>], i: usize) -> Result<u64, RespError> {
    let arg = args.get(i).ok_or_else(RespError::syntax)?;
    parse_u64(arg).ok_or_else(RespError::integer)
}

/// Port of `CmdReserve` + `OpReserve`. Validates the capacity and the optional
/// BUCKETSIZE/MAXITERATIONS/EXPANSION options, then replies OK (or "item
/// exists" when the key already holds any value).
fn exec_cf_reserve(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let Some(capacity) = parse_u64(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::integer());
    };
    if capacity == 0 {
        return CmdResult::Err(cf_capacity());
    }

    let mut bucket_size: u64 = u64::from(CuckooFilterOptions::default().slots_per_bucket);
    let mut max_iterations: u64 = u64::from(CuckooFilterOptions::default().max_iterations);
    let mut expansion: u64 = u64::from(CuckooFilterOptions::default().expansion);
    let mut i = key_idx + 2;
    while i < ctx.args.len() {
        match ctx.args[i].to_ascii_uppercase().as_slice() {
            b"BUCKETSIZE" => {
                i += 1;
                let v = match parse_opt_value(ctx.args, i) {
                    Ok(v) => v,
                    Err(e) => return CmdResult::Err(e),
                };
                if v > u64::from(u8::MAX) {
                    return CmdResult::Err(RespError::integer());
                }
                if v == 0 {
                    return CmdResult::Err(cf_bucket_size());
                }
                bucket_size = v;
            }
            b"MAXITERATIONS" => {
                i += 1;
                let v = match parse_opt_value(ctx.args, i) {
                    Ok(v) => v,
                    Err(e) => return CmdResult::Err(e),
                };
                if v > u64::from(u16::MAX) {
                    return CmdResult::Err(RespError::integer());
                }
                if v == 0 {
                    return CmdResult::Err(cf_max_iterations());
                }
                max_iterations = v;
            }
            b"EXPANSION" => {
                i += 1;
                let v = match parse_opt_value(ctx.args, i) {
                    Ok(v) => v,
                    Err(e) => return CmdResult::Err(e),
                };
                if v > 32767 {
                    return CmdResult::Err(cf_expansion());
                }
                expansion = v;
            }
            _ => return CmdResult::Err(RespError::syntax()),
        }
        i += 1;
    }

    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Cuckoo(_)) => return CmdResult::Err(item_exists()),
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => {}
    }
    ctx.db.insert(
        key,
        PrimeValue::Cuckoo(CuckooFilter::new(&CuckooFilterOptions {
            capacity,
            slots_per_bucket: bucket_size as u8,
            max_iterations: max_iterations as u16,
            expansion: expansion as u16,
        })),
    );
    CmdResult::Ok(ok())
}

// ---------------------------------------------------------------------------
// CF.ADD / CF.ADDNX
// ---------------------------------------------------------------------------

/// Port of `OpAdd`: auto-create with the default capacity, then insert (always
/// allows duplicates).
fn exec_cf_add(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let item = &ctx.args[key_idx + 1];
    let cf = match get_or_create_cf(ctx, key, K_DEFAULT_CAPACITY) {
        Ok(c) => c,
        Err(e) => return CmdResult::Err(e),
    };
    if !cf.insert(CuckooFilter::hash(item)) {
        return CmdResult::Err(cf_filter_full());
    }
    CmdResult::Ok(integer(1))
}

/// Port of `OpAddNx`: skip the insert when the item already exists.
fn exec_cf_addnx(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let item = &ctx.args[key_idx + 1];
    let cf = match get_or_create_cf(ctx, key, K_DEFAULT_CAPACITY) {
        Ok(c) => c,
        Err(e) => return CmdResult::Err(e),
    };
    let hash = CuckooFilter::hash(item);
    if cf.exists(hash) {
        return CmdResult::Ok(integer(0));
    }
    if !cf.insert(hash) {
        return CmdResult::Err(cf_filter_full());
    }
    CmdResult::Ok(integer(1))
}

// ---------------------------------------------------------------------------
// CF.EXISTS / CF.MEXISTS
// ---------------------------------------------------------------------------

/// Port of `CmdExists`/`CmdMExists` + `OpExists`: a missing or wrong-type key
/// reports 0 (no error).
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
        Some(PrimeValue::Cuckoo(cf)) => {
            let results: Vec<bool> = items
                .iter()
                .map(|it| cf.exists(CuckooFilter::hash(it)))
                .collect();
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

fn exec_cf_exists(ctx: &mut OpContext) -> CmdResult {
    exists_or_mexists(ctx, false)
}

fn exec_cf_mexists(ctx: &mut OpContext) -> CmdResult {
    exists_or_mexists(ctx, true)
}

// ---------------------------------------------------------------------------
// CF.INFO
// ---------------------------------------------------------------------------

/// Port of `OpInfo`/`CmdInfo`: a 16-element array of key/value pairs.
fn exec_cf_info(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let cf = match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Cuckoo(cf)) => cf,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Err(no_such_key()),
    };
    CmdResult::Ok(RespValue::Array(vec![
        RespValue::Bulk(b"Size".to_vec()),
        integer(cf.malloc_used() as i64),
        RespValue::Bulk(b"Number of buckets".to_vec()),
        integer(cf.num_buckets() as i64),
        RespValue::Bulk(b"Number of filters".to_vec()),
        integer(cf.num_filters() as i64),
        RespValue::Bulk(b"Number of items inserted".to_vec()),
        integer(cf.num_items() as i64),
        RespValue::Bulk(b"Number of items deleted".to_vec()),
        integer(cf.num_deletes() as i64),
        RespValue::Bulk(b"Bucket size".to_vec()),
        integer(i64::from(cf.slots_per_bucket())),
        RespValue::Bulk(b"Expansion rate".to_vec()),
        integer(i64::from(cf.expansion())),
        RespValue::Bulk(b"Max iterations".to_vec()),
        integer(i64::from(cf.max_iterations())),
    ]))
}

// ---------------------------------------------------------------------------
// CF.COUNT
// ---------------------------------------------------------------------------

/// Port of `CmdCount` + `OpCount`: a missing or wrong-type key reports 0.
fn exec_cf_count(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let item = &ctx.args[key_idx + 1];
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Cuckoo(cf)) => {
            CmdResult::Ok(integer(cf.count(CuckooFilter::hash(item)) as i64))
        }
        Some(_) | None => CmdResult::Ok(integer(0)),
    }
}

// ---------------------------------------------------------------------------
// CF.DEL
// ---------------------------------------------------------------------------

/// Port of `OpDel`/`CmdDel`: a missing key is an error; a successful delete
/// auto-compacts once deletes exceed 10% of items (mirrors `RedisBloom`).
fn exec_cf_del(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let item = &ctx.args[key_idx + 1];
    let cf = match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Cuckoo(cf)) => cf,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Err(no_such_key()),
    };
    let deleted = cf.delete(CuckooFilter::hash(item));
    if deleted && cf.num_filters() > 1 && cf.num_deletes() > cf.num_items() / 10 {
        cf.compact(false);
    }
    CmdResult::Ok(integer(i64::from(deleted)))
}

// ---------------------------------------------------------------------------
// CF.INSERT / CF.INSERTNX
// ---------------------------------------------------------------------------

/// Parsed `CF.INSERT`/`CF.INSERTNX` options.
struct InsertOptions {
    capacity: u64,
    nocreate: bool,
}

/// Port of the `kInsertGrammar` + `CmdInsertImpl` parsing. Returns the options
/// and the index of the first item argument.
fn parse_insert_options(
    args: &[Vec<u8>],
    key_idx: usize,
) -> Result<(InsertOptions, usize), RespError> {
    let mut opts = InsertOptions {
        capacity: K_DEFAULT_CAPACITY,
        nocreate: false,
    };
    let mut i = key_idx + 1;
    while let Some(tok) = args.get(i) {
        match tok.to_ascii_uppercase().as_slice() {
            b"CAPACITY" => {
                i += 1;
                let v = parse_u64(args.get(i).ok_or_else(RespError::syntax)?)
                    .ok_or_else(RespError::integer)?;
                opts.capacity = v;
            }
            b"NOCREATE" => {
                opts.nocreate = true;
            }
            _ => break,
        }
        i += 1;
    }
    if !opts.nocreate && opts.capacity == 0 {
        return Err(cf_capacity());
    }
    match args.get(i).map(|t| t.to_ascii_uppercase()) {
        Some(t) if t == b"ITEMS" => {}
        _ => return Err(RespError::new("ERR CF.INSERT requires ITEMS keyword")),
    }
    let items_start = i + 1;
    if items_start >= args.len() {
        return Err(RespError::new("ERR CF.INSERT requires at least one item"));
    }
    Ok((opts, items_start))
}

/// Port of `OpInsert`/`CmdInsertImpl`: one integer per item — 1 inserted,
/// 0 already exists (nx only), -1 filter full. `NOCREATE` on a missing key
/// returns "no such key" instead of auto-creating.
fn insert_impl(ctx: &mut OpContext, nx: bool) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let (opts, items_start) = match parse_insert_options(ctx.args, key_idx) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    let items = &ctx.args[items_start..];

    let cf = if opts.nocreate {
        match ctx.db.find_mut(key, ctx.now_ms) {
            Some(PrimeValue::Cuckoo(cf)) => cf,
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => return CmdResult::Err(no_such_key()),
        }
    } else {
        match get_or_create_cf(ctx, key, opts.capacity) {
            Ok(c) => c,
            Err(e) => return CmdResult::Err(e),
        }
    };

    let mut results = Vec::with_capacity(items.len());
    for item in items {
        let hash = CuckooFilter::hash(item);
        if nx {
            if cf.exists(hash) {
                results.push(0);
            } else if cf.insert(hash) {
                results.push(1);
            } else {
                results.push(-1);
            }
        } else if cf.insert(hash) {
            results.push(1);
        } else {
            results.push(-1);
        }
    }
    CmdResult::Ok(RespValue::Array(results.into_iter().map(integer).collect()))
}

fn exec_cf_insert(ctx: &mut OpContext) -> CmdResult {
    insert_impl(ctx, false)
}

fn exec_cf_insertnx(ctx: &mut OpContext) -> CmdResult {
    insert_impl(ctx, true)
}

// ---------------------------------------------------------------------------
// CF.COMPACT
// ---------------------------------------------------------------------------

/// Port of `OpCompact`/`CmdCompact`: `cont=true` keeps trying older
/// sub-filters even if a newer one couldn't be fully emptied.
fn exec_cf_compact(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Cuckoo(cf)) => {
            cf.compact(true);
            CmdResult::Ok(ok())
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Err(no_such_key()),
    }
}

// ---------------------------------------------------------------------------
// Command definitions
// ---------------------------------------------------------------------------

pub static CMD_CF_RESERVE: Command = Command {
    name: "CF.RESERVE",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_cf_reserve,
    merge: None,
};
pub static CMD_CF_ADD: Command = Command {
    name: "CF.ADD",
    arity: 3,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    key_range: KeyRange::ONE,
    exec: exec_cf_add,
    merge: None,
};
pub static CMD_CF_ADDNX: Command = Command {
    name: "CF.ADDNX",
    arity: 3,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    key_range: KeyRange::ONE,
    exec: exec_cf_addnx,
    merge: None,
};
pub static CMD_CF_EXISTS: Command = Command {
    name: "CF.EXISTS",
    arity: -3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_cf_exists,
    merge: None,
};
pub static CMD_CF_MEXISTS: Command = Command {
    name: "CF.MEXISTS",
    arity: -3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_cf_mexists,
    merge: None,
};
pub static CMD_CF_INFO: Command = Command {
    name: "CF.INFO",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_cf_info,
    merge: None,
};
pub static CMD_CF_COUNT: Command = Command {
    name: "CF.COUNT",
    arity: 3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_cf_count,
    merge: None,
};
pub static CMD_CF_DEL: Command = Command {
    name: "CF.DEL",
    arity: 3,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_cf_del,
    merge: None,
};
pub static CMD_CF_INSERT: Command = Command {
    name: "CF.INSERT",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    key_range: KeyRange::ONE,
    exec: exec_cf_insert,
    merge: None,
};
pub static CMD_CF_INSERTNX: Command = Command {
    name: "CF.INSERTNX",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    key_range: KeyRange::ONE,
    exec: exec_cf_insertnx,
    merge: None,
};
pub static CMD_CF_COMPACT: Command = Command {
    name: "CF.COMPACT",
    arity: 2,
    flags: FLAG_WRITE,
    key_range: KeyRange::ONE,
    exec: exec_cf_compact,
    merge: None,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::compact::CompactString;
    use crate::core::db::DbSlice;
    use crate::core::rdb::{RestoreOutcome, dump_value, restore_value};

    fn dispatch(db: &mut DbSlice, argv: &[Vec<u8>]) -> CmdResult {
        let (exec, first_key_idx, owned): (fn(&mut OpContext) -> CmdResult, usize, Vec<usize>) =
            match argv[0].as_slice() {
                b"CF.RESERVE" => (exec_cf_reserve, 1, vec![1]),
                b"CF.ADD" => (exec_cf_add, 1, vec![1]),
                b"CF.ADDNX" => (exec_cf_addnx, 1, vec![1]),
                b"CF.EXISTS" => (exec_cf_exists, 1, vec![1]),
                b"CF.MEXISTS" => (exec_cf_mexists, 1, vec![1]),
                b"CF.INFO" => (exec_cf_info, 1, vec![1]),
                b"CF.COUNT" => (exec_cf_count, 1, vec![1]),
                b"CF.DEL" => (exec_cf_del, 1, vec![1]),
                b"CF.INSERT" => (exec_cf_insert, 1, vec![1]),
                b"CF.INSERTNX" => (exec_cf_insertnx, 1, vec![1]),
                b"CF.COMPACT" => (exec_cf_compact, 1, vec![1]),
                _ => panic!("unhandled command {:?}", argv[0]),
            };
        let mut ctx = OpContext {
            db,
            args: argv,
            owned_keys: &owned,
            first_key_idx,
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

    #[test]
    fn reserve() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"CF.RESERVE", b"cf1", b"1000"));
        assert_eq!(type_of(&mut db, "cf1"), "MBbloomCF");

        assert!(err(run!(&mut db, b"CF.RESERVE", b"cf1", b"1000")).contains("item exists"));

        assert!(
            err(run!(&mut db, b"CF.RESERVE", b"cf2", b"0"))
                .contains("capacity must be greater than 0")
        );
    }

    #[test]
    fn reserve_with_options() {
        let mut db = DbSlice::new(0);
        ok_res(run!(
            &mut db,
            b"CF.RESERVE",
            b"cf1",
            b"1000",
            b"bucketsize",
            b"4",
            b"maxiterations",
            b"10",
            b"expansion",
            b"2"
        ));

        assert!(
            err(run!(
                &mut db,
                b"CF.RESERVE",
                b"cf2",
                b"1000",
                b"BUCKETSIZE",
                b"0"
            ))
            .contains("bucket size must be between 1 and 255")
        );
        assert!(
            err(run!(
                &mut db,
                b"CF.RESERVE",
                b"cf3",
                b"1000",
                b"BUCKETSIZE",
                b"256"
            ))
            .contains("value is not an integer or out of range")
        );
        assert!(
            err(run!(
                &mut db,
                b"CF.RESERVE",
                b"cf4",
                b"1000",
                b"MAXITERATIONS",
                b"0"
            ))
            .contains("max iterations must be between 1 and 65535")
        );
        assert!(
            err(run!(
                &mut db,
                b"CF.RESERVE",
                b"cf5",
                b"1000",
                b"EXPANSION",
                b"32768"
            ))
            .contains("expansion must be between 0 and 32767")
        );
    }

    #[test]
    fn reserve_wrong_type() {
        let mut db = DbSlice::new(0);
        set(&mut db, "str1", "foo");
        assert!(err(run!(&mut db, b"CF.RESERVE", b"str1", b"1000")).contains("WRONGTYPE"));
    }

    #[test]
    fn add_autocreates_and_allows_duplicates() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"f1", b"foo")));
        assert_eq!(type_of(&mut db, "f1"), "MBbloomCF");

        // CF.ADD allows duplicate insertions.
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"f1", b"foo")));
    }

    #[test]
    fn addnx_prevents_duplicates() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run!(&mut db, b"CF.ADDNX", b"cf", b"k1")));
        assert_eq!(0, int(run!(&mut db, b"CF.ADDNX", b"cf", b"k1")));

        // CF.ADD still allows the duplicate CF.ADDNX rejected.
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"cf", b"k1")));
    }

    #[test]
    fn add_wrong_type() {
        let mut db = DbSlice::new(0);
        set(&mut db, "str1", "foo");
        assert!(err(run!(&mut db, b"CF.ADD", b"str1", b"foo")).contains("WRONGTYPE"));
        assert!(err(run!(&mut db, b"CF.ADDNX", b"str1", b"foo")).contains("WRONGTYPE"));
    }

    #[test]
    fn add_filter_full() {
        let mut db = DbSlice::new(0);
        ok_res(run!(
            &mut db,
            b"CF.RESERVE",
            b"cf",
            b"4",
            b"expansion",
            b"0"
        ));
        for i in 0..4 {
            assert_eq!(
                1,
                int(run!(&mut db, b"CF.ADD", b"cf", i.to_string().as_bytes()))
            );
        }
        assert!(err(run!(&mut db, b"CF.ADD", b"cf", b"overflow")).contains("Filter is full"));
    }

    #[test]
    fn insert_filter_full() {
        let mut db = DbSlice::new(0);
        ok_res(run!(
            &mut db,
            b"CF.RESERVE",
            b"cf",
            b"4",
            b"expansion",
            b"0"
        ));
        for i in 0..4 {
            assert_eq!(
                vec![1],
                ints(run!(
                    &mut db,
                    b"CF.INSERT",
                    b"cf",
                    b"ITEMS",
                    i.to_string().as_bytes()
                ))
            );
        }
        assert_eq!(
            vec![-1, -1],
            ints(run!(
                &mut db,
                b"CF.INSERT",
                b"cf",
                b"ITEMS",
                b"overflow1",
                b"overflow2"
            ))
        );

        ok_res(run!(
            &mut db,
            b"CF.RESERVE",
            b"cfnx",
            b"4",
            b"expansion",
            b"0"
        ));
        for i in 0..4 {
            assert_eq!(
                vec![1],
                ints(run!(
                    &mut db,
                    b"CF.INSERTNX",
                    b"cfnx",
                    b"ITEMS",
                    i.to_string().as_bytes()
                ))
            );
        }
        // Item 0 already exists → 0; overflow → -1.
        assert_eq!(
            vec![0, -1],
            ints(run!(
                &mut db,
                b"CF.INSERTNX",
                b"cfnx",
                b"ITEMS",
                b"0",
                b"overflow"
            ))
        );
    }

    #[test]
    fn exists() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"f1", b"foo")));
        assert_eq!(1, int(run!(&mut db, b"CF.EXISTS", b"f1", b"foo")));
        assert_eq!(0, int(run!(&mut db, b"CF.EXISTS", b"f1", b"bar")));

        // Missing key returns 0, not an error.
        assert_eq!(
            0,
            int(run!(&mut db, b"CF.EXISTS", b"nonexist-key", b"blah"))
        );
    }

    #[test]
    fn exists_wrong_type() {
        let mut db = DbSlice::new(0);
        set(&mut db, "str1", "foo");
        assert_eq!(0, int(run!(&mut db, b"CF.EXISTS", b"str1", b"foo")));
    }

    #[test]
    fn mexists() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"f1", b"foo")));
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"f1", b"bar")));
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"f1", b"baz")));

        assert_eq!(
            vec![1, 1, 1],
            ints(run!(&mut db, b"CF.MEXISTS", b"f1", b"foo", b"bar", b"baz"))
        );
        assert_eq!(
            vec![1, 0],
            ints(run!(&mut db, b"CF.MEXISTS", b"f1", b"foo", b"nope"))
        );

        // Missing key returns an all-zero array, not an error.
        assert_eq!(
            vec![0],
            ints(run!(&mut db, b"CF.MEXISTS", b"nonexist-key", b"blah"))
        );
    }

    #[test]
    fn mexists_wrong_type() {
        let mut db = DbSlice::new(0);
        set(&mut db, "str1", "foo");
        assert_eq!(vec![0], ints(run!(&mut db, b"CF.MEXISTS", b"str1", b"foo")));
    }

    #[test]
    fn info() {
        let mut db = DbSlice::new(0);
        ok_res(run!(
            &mut db,
            b"CF.RESERVE",
            b"cf1",
            b"1000",
            b"BUCKETSIZE",
            b"4",
            b"MAXITERATIONS",
            b"10",
            b"EXPANSION",
            b"2"
        ));
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"cf1", b"foo")));

        let arr = match run!(&mut db, b"CF.INFO", b"cf1").into_resp_value() {
            RespValue::Array(a) => a,
            o => panic!("expected array, got {o:?}"),
        };
        assert_eq!(arr.len(), 16);
        assert_eq!(arr[0], RespValue::Bulk(b"Size".to_vec()));
        match &arr[1] {
            RespValue::Integer(n) => assert!(*n > 0),
            o => panic!("expected size integer, got {o:?}"),
        }
        assert_eq!(arr[2], RespValue::Bulk(b"Number of buckets".to_vec()));
        assert_eq!(arr[3], RespValue::Integer(256));
        assert_eq!(arr[4], RespValue::Bulk(b"Number of filters".to_vec()));
        assert_eq!(arr[5], RespValue::Integer(1));
        assert_eq!(
            arr[6],
            RespValue::Bulk(b"Number of items inserted".to_vec())
        );
        assert_eq!(arr[7], RespValue::Integer(1));
        assert_eq!(arr[8], RespValue::Bulk(b"Number of items deleted".to_vec()));
        assert_eq!(arr[9], RespValue::Integer(0));
        assert_eq!(arr[10], RespValue::Bulk(b"Bucket size".to_vec()));
        assert_eq!(arr[11], RespValue::Integer(4));
        assert_eq!(arr[12], RespValue::Bulk(b"Expansion rate".to_vec()));
        assert_eq!(arr[13], RespValue::Integer(2));
        assert_eq!(arr[14], RespValue::Bulk(b"Max iterations".to_vec()));
        assert_eq!(arr[15], RespValue::Integer(10));
    }

    #[test]
    fn info_missing_key() {
        let mut db = DbSlice::new(0);
        assert!(err(run!(&mut db, b"CF.INFO", b"nonexist-key")).contains("no such key"));
    }

    #[test]
    fn count() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"f1", b"foo")));
        assert_eq!(1, int(run!(&mut db, b"CF.COUNT", b"f1", b"foo")));
        assert_eq!(0, int(run!(&mut db, b"CF.COUNT", b"f1", b"bar")));

        // Missing key returns 0, not an error.
        assert_eq!(0, int(run!(&mut db, b"CF.COUNT", b"nonexist-key", b"blah")));
    }

    #[test]
    fn count_after_duplicate_adds() {
        let mut db = DbSlice::new(0);
        // CF.ADD never dedups, so repeated adds should each bump the count.
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"f1", b"foo")));
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"f1", b"foo")));
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"f1", b"foo")));
        assert_eq!(3, int(run!(&mut db, b"CF.COUNT", b"f1", b"foo")));

        assert_eq!(1, int(run!(&mut db, b"CF.DEL", b"f1", b"foo")));
        assert_eq!(2, int(run!(&mut db, b"CF.COUNT", b"f1", b"foo")));
    }

    #[test]
    fn del() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"f1", b"foo")));
        assert_eq!(1, int(run!(&mut db, b"CF.DEL", b"f1", b"foo")));
        assert_eq!(0, int(run!(&mut db, b"CF.EXISTS", b"f1", b"foo")));
    }

    #[test]
    fn del_non_existent_item() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"CF.RESERVE", b"cf1", b"1000"));
        assert_eq!(0, int(run!(&mut db, b"CF.DEL", b"cf1", b"nope")));
    }

    #[test]
    fn del_missing_key() {
        let mut db = DbSlice::new(0);
        assert!(err(run!(&mut db, b"CF.DEL", b"nonexist-key", b"foo")).contains("no such key"));
    }

    #[test]
    fn compact() {
        let mut db = DbSlice::new(0);
        ok_res(run!(&mut db, b"CF.RESERVE", b"cf1", b"4"));
        for i in 0..30 {
            assert_eq!(
                1,
                int(run!(
                    &mut db,
                    b"CF.ADD",
                    b"cf1",
                    format!("item{i}").as_bytes()
                ))
            );
        }
        for i in 0..29 {
            assert_eq!(
                1,
                int(run!(
                    &mut db,
                    b"CF.DEL",
                    b"cf1",
                    format!("item{i}").as_bytes()
                ))
            );
        }

        // Explicit CF.COMPACT should succeed even though CF.DEL's automatic
        // compaction has likely already run — it's a no-op/cheap pass then.
        ok_res(run!(&mut db, b"CF.COMPACT", b"cf1"));
        assert_eq!(1, int(run!(&mut db, b"CF.EXISTS", b"cf1", b"item29")));
    }

    #[test]
    fn compact_missing_key() {
        let mut db = DbSlice::new(0);
        assert!(err(run!(&mut db, b"CF.COMPACT", b"nonexist-key")).contains("no such key"));
    }

    #[test]
    fn insert() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            vec![1, 1, 1],
            ints(run!(
                &mut db,
                b"CF.INSERT",
                b"cf",
                b"ITEMS",
                b"a",
                b"b",
                b"c"
            ))
        );
        assert_eq!(type_of(&mut db, "cf"), "MBbloomCF");

        // Duplicates are allowed (like CF.ADD).
        assert_eq!(
            vec![1, 1],
            ints(run!(&mut db, b"CF.INSERT", b"cf", b"ITEMS", b"a", b"a"))
        );
    }

    #[test]
    fn insert_with_capacity() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            vec![1],
            ints(run!(
                &mut db,
                b"CF.INSERT",
                b"cf",
                b"CAPACITY",
                b"500",
                b"ITEMS",
                b"x"
            ))
        );
    }

    #[test]
    fn insert_zero_capacity() {
        let mut db = DbSlice::new(0);
        assert!(
            err(run!(
                &mut db,
                b"CF.INSERT",
                b"cf",
                b"CAPACITY",
                b"0",
                b"ITEMS",
                b"x"
            ))
            .contains("capacity must be greater than 0")
        );
        assert!(
            err(run!(
                &mut db,
                b"CF.INSERT",
                b"cf",
                b"CAPACITY",
                b"0",
                b"NOCREATE",
                b"ITEMS",
                b"x"
            ))
            .contains("no such key")
        );
    }

    #[test]
    fn insert_nocreate() {
        let mut db = DbSlice::new(0);
        // NOCREATE on missing key returns an error.
        assert!(
            err(run!(
                &mut db,
                b"CF.INSERT",
                b"cf",
                b"NOCREATE",
                b"ITEMS",
                b"a"
            ))
            .contains("no such key")
        );

        // NOCREATE on existing key works fine.
        ok_res(run!(&mut db, b"CF.RESERVE", b"cf", b"1000"));
        assert_eq!(
            vec![1],
            ints(run!(
                &mut db,
                b"CF.INSERT",
                b"cf",
                b"NOCREATE",
                b"ITEMS",
                b"a"
            ))
        );
    }

    #[test]
    fn insert_missing_items_keyword() {
        let mut db = DbSlice::new(0);
        assert!(err(run!(&mut db, b"CF.INSERT", b"cf", b"a", b"b")).contains("ITEMS"));
    }

    #[test]
    fn insert_wrong_type() {
        let mut db = DbSlice::new(0);
        set(&mut db, "str1", "foo");
        assert!(err(run!(&mut db, b"CF.INSERT", b"str1", b"ITEMS", b"a")).contains("WRONGTYPE"));
    }

    #[test]
    fn insertnx() {
        let mut db = DbSlice::new(0);
        assert_eq!(
            vec![1, 1, 1],
            ints(run!(
                &mut db,
                b"CF.INSERTNX",
                b"cf",
                b"ITEMS",
                b"a",
                b"b",
                b"c"
            ))
        );

        // Existing items return 0 (like CF.ADDNX).
        assert_eq!(
            vec![0, 1],
            ints(run!(&mut db, b"CF.INSERTNX", b"cf", b"ITEMS", b"a", b"d"))
        );
    }

    #[test]
    fn insertnx_nocreate() {
        let mut db = DbSlice::new(0);
        assert!(
            err(run!(
                &mut db,
                b"CF.INSERTNX",
                b"cf",
                b"NOCREATE",
                b"ITEMS",
                b"a"
            ))
            .contains("no such key")
        );

        ok_res(run!(&mut db, b"CF.RESERVE", b"cf", b"1000"));
        assert_eq!(
            vec![1],
            ints(run!(
                &mut db,
                b"CF.INSERTNX",
                b"cf",
                b"NOCREATE",
                b"ITEMS",
                b"a"
            ))
        );
    }

    #[test]
    fn insertnx_wrong_type() {
        let mut db = DbSlice::new(0);
        set(&mut db, "str1", "foo");
        assert!(err(run!(&mut db, b"CF.INSERTNX", b"str1", b"ITEMS", b"a")).contains("WRONGTYPE"));
    }

    #[test]
    fn dump_restore_round_trip() {
        let mut db = DbSlice::new(0);
        ok_res(run!(
            &mut db,
            b"CF.RESERVE",
            b"cf1",
            b"1000",
            b"BUCKETSIZE",
            b"4",
            b"MAXITERATIONS",
            b"10",
            b"EXPANSION",
            b"2"
        ));
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"cf1", b"foo")));
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"cf1", b"foo")));
        assert_eq!(1, int(run!(&mut db, b"CF.ADD", b"cf1", b"bar")));

        let cf1 = db.find(b"cf1", 0).expect("cf1 exists").clone();
        let dump = dump_value(&cf1);
        let restored = match restore_value(&dump, 0) {
            Ok(RestoreOutcome::Value(v)) => v,
            other => panic!("expected restored value, got {other:?}"),
        };
        assert_eq!(restored.type_name(), "MBbloomCF");
        db.insert(b"cf2", restored);

        assert_eq!(2, int(run!(&mut db, b"CF.COUNT", b"cf2", b"foo")));
        assert_eq!(1, int(run!(&mut db, b"CF.EXISTS", b"cf2", b"bar")));
        assert_eq!(0, int(run!(&mut db, b"CF.EXISTS", b"cf2", b"nope")));

        // INFO state (filters/items/deletes/options) is preserved.
        let arr = match run!(&mut db, b"CF.INFO", b"cf2").into_resp_value() {
            RespValue::Array(a) => a,
            o => panic!("expected array, got {o:?}"),
        };
        assert_eq!(arr[5], RespValue::Integer(1)); // Number of filters
        assert_eq!(arr[7], RespValue::Integer(3)); // Number of items inserted
        assert_eq!(arr[9], RespValue::Integer(0)); // Number of items deleted
        assert_eq!(arr[11], RespValue::Integer(4)); // Bucket size
        assert_eq!(arr[13], RespValue::Integer(2)); // Expansion rate
        assert_eq!(arr[15], RespValue::Integer(10)); // Max iterations
    }

    #[test]
    fn dump_restore_after_expansion() {
        let mut db = DbSlice::new(0);
        ok_res(run!(
            &mut db,
            b"CF.RESERVE",
            b"cf1",
            b"4",
            b"EXPANSION",
            b"2"
        ));
        for i in 0..100 {
            assert_eq!(
                1,
                int(run!(
                    &mut db,
                    b"CF.ADD",
                    b"cf1",
                    format!("item{i}").as_bytes()
                ))
            );
        }

        let cf1 = db.find(b"cf1", 0).expect("cf1 exists").clone();
        let dump = dump_value(&cf1);
        let restored = match restore_value(&dump, 0) {
            Ok(RestoreOutcome::Value(v)) => v,
            other => panic!("expected restored value, got {other:?}"),
        };
        db.insert(b"cf2", restored);

        for i in 0..100 {
            assert_eq!(
                1,
                int(run!(
                    &mut db,
                    b"CF.EXISTS",
                    b"cf2",
                    format!("item{i}").as_bytes()
                )),
                "exists item{i}"
            );
        }
    }
}
