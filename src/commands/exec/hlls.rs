use crate::commands::{
    integer, ok, Command, OpContext, ShardPart, KeyRange, FLAG_DENYOOM, FLAG_FAST, FLAG_MULTI_KEY,
    FLAG_READONLY, FLAG_WRITE,
};
use crate::core::compact::CompactString;
use crate::core::hll::{
    create_dense_hll, dense_with_slack, get_sparse_hll_init_size, init_sparse_hll, is_valid_hll,
    pfadd_dense, pfadd_sparse, pfcount_multi, pfcount_single, pfmerge, sparse_to_dense,
    strip_dense_slack, HllValidness,
};
use crate::core::PrimeValue;
use crate::error::{CmdResult, RespError, RespValue};

// ---------------------------------------------------------------------------
// Error mapping (hll_family.cc HandleOpValueResult)
// ---------------------------------------------------------------------------

/// `kInvalidHllError` (facade/error.h); sent as `-ERR Key is not a valid
/// HyperLogLog string value`.
fn invalid_hll() -> RespError {
    RespError::new("ERR Key is not a valid HyperLogLog string value")
}

/// `StatusToMsg(OpStatus::CORRUPTED_HLL)` (facade/op_status.cc), sent verbatim.
fn corrupted_hll() -> RespError {
    RespError::new("INVALIDOBJ Corrupted HLL object detected.")
}

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

/// Convert a stored HLL value into a dense stored value (exactly
/// `HLL_DENSE_SIZE` bytes). `None` means the value is not a valid HLL at all
/// (`HLL_INVALID`); `Err` means a sparse value that failed to convert (corrupt).
fn to_dense_stored(stored: &[u8]) -> Result<Option<Vec<u8>>, ()> {
    match is_valid_hll(stored) {
        HllValidness::ValidDense => Ok(Some(stored.to_vec())),
        HllValidness::ValidSparse => {
            match sparse_to_dense(stored) {
                Some(dense) => Ok(Some(strip_dense_slack(dense))),
                None => Err(()),
            }
        }
        HllValidness::Invalid => Ok(None),
    }
}

/// Read and convert the HLL values for `keys`, mirroring `ReadValues` in
/// hll_family.cc: wrong-type keys error, missing keys are skipped, and any value
/// that is not a valid HLL (or corrupt sparse) reports CORRUPTED_HLL.
fn collect_hlls(ctx: &mut OpContext, keys: &[usize]) -> Result<Vec<Vec<u8>>, RespError> {
    let mut out = Vec::new();
    for &ki in keys {
        let key = &ctx.args[ki];
        match ctx.db.find(key, ctx.now_ms) {
            Some(PrimeValue::Str(s)) => match to_dense_stored(s.as_bytes()) {
                Ok(Some(dense)) => out.push(dense),
                Ok(None) => return Err(corrupted_hll()),
                Err(()) => return Err(corrupted_hll()),
            },
            Some(_) => return Err(RespError::wrong_type()),
            None => {}
        }
    }
    Ok(out)
}

fn dense_values_to_resp(values: Vec<Vec<u8>>) -> RespValue {
    RespValue::Array(values.into_iter().map(RespValue::Bulk).collect())
}

fn resp_to_dense_values(p: &ShardPart) -> Result<Vec<Vec<u8>>, RespError> {
    match &p.result {
        CmdResult::Ok(RespValue::Array(arr)) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                if let RespValue::Bulk(b) = v {
                    out.push(b.clone());
                }
            }
            Ok(out)
        }
        CmdResult::Err(e) => Err(e.clone()),
        _ => Err(RespError::new("ERR internal: unexpected HLL shard result")),
    }
}

// ---------------------------------------------------------------------------
// PFADD
// ---------------------------------------------------------------------------

/// Port of `AddToHll` (hll_family.cc): create a sparse HLL when the key is
/// missing, append every value (promoting to dense as needed), and return
/// whether any register changed.
fn exec_pfadd(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];

    let mut is_sparse: bool;
    let mut hll: Vec<u8>;
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => match is_valid_hll(s.as_bytes()) {
            HllValidness::Invalid => return CmdResult::Err(invalid_hll()),
            HllValidness::ValidSparse => {
                is_sparse = true;
                hll = s.as_bytes().to_vec();
            }
            HllValidness::ValidDense => {
                is_sparse = false;
                hll = dense_with_slack(s.as_bytes()).expect("valid dense is exactly sized");
            }
        },
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => {
            is_sparse = true;
            hll = vec![0u8; get_sparse_hll_init_size()];
            if !init_sparse_hll(&mut hll) {
                return CmdResult::Err(RespError::new("ERR internal: failed to init HLL"));
            }
        }
    }

    let mut updated = 0i32;
    for value in &ctx.args[key_idx + 1..] {
        let added = if is_sparse {
            let mut promoted = false;
            let a = pfadd_sparse(&mut hll, value, &mut promoted);
            if promoted {
                is_sparse = false;
            }
            a
        } else {
            pfadd_dense(&mut hll, value)
        };
        if added < 0 {
            return CmdResult::Err(invalid_hll());
        }
        updated += added;
    }

    let stored = if is_sparse { hll } else { strip_dense_slack(hll) };
    ctx.db.insert(
        CompactString::from_bytes(key),
        PrimeValue::Str(CompactString::from_bytes(&stored)),
    );
    CmdResult::Ok(integer(updated.min(1) as i64))
}

// ---------------------------------------------------------------------------
// PFCOUNT
// ---------------------------------------------------------------------------

/// Single-key path, port of `CountHllsSingle`.
fn pfcount_single_key(ctx: &mut OpContext, key: &[u8]) -> CmdResult {
    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Str(s)) => {
            let stored = s.as_bytes();
            match is_valid_hll(stored) {
                HllValidness::ValidDense => {
                    let mut v = stored.to_vec();
                    CmdResult::Ok(integer(pfcount_single(&mut v)))
                }
                HllValidness::ValidSparse => match sparse_to_dense(stored) {
                    Some(mut d) => CmdResult::Ok(integer(pfcount_single(&mut d))),
                    None => CmdResult::Err(corrupted_hll()),
                },
                HllValidness::Invalid => CmdResult::Err(invalid_hll()),
            }
        }
        Some(_) => CmdResult::Err(RespError::wrong_type()),
        None => CmdResult::Ok(integer(0)),
    }
}

fn exec_pfcount(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let total_keys = ctx.args.len() - 1;
    if total_keys == 1 {
        return pfcount_single_key(ctx, &ctx.args[key_idx]);
    }
    if ctx.owned_keys.len() == total_keys {
        // Multi-key, but every key happens to live on this shard.
        let keys: Vec<usize> = (1..=total_keys).collect();
        let hlls = match collect_hlls(ctx, &keys) {
            Ok(h) => h,
            Err(e) => return CmdResult::Err(e),
        };
        let ptrs: Vec<&[u8]> = hlls.iter().map(|h| h.as_slice()).collect();
        let count = pfcount_multi(&ptrs);
        return if count < 0 {
            CmdResult::Err(invalid_hll())
        } else {
            CmdResult::Ok(integer(count))
        };
    }
    // Partial for the coordinator merge.
    let keys: Vec<usize> = ctx.owned_keys.to_vec();
    match collect_hlls(ctx, &keys) {
        Ok(hlls) => CmdResult::Ok(dense_values_to_resp(hlls)),
        Err(e) => CmdResult::Err(e),
    }
}

fn merge_pfcount(parts: &[ShardPart], _args: &[Vec<u8>], _keys: &[usize], _now: u64) -> CmdResult {
    let mut hlls: Vec<Vec<u8>> = Vec::new();
    for p in parts {
        match resp_to_dense_values(p) {
            Ok(values) => hlls.extend(values),
            Err(e) => return CmdResult::Err(e),
        }
    }
    let ptrs: Vec<&[u8]> = hlls.iter().map(|h| h.as_slice()).collect();
    let count = pfcount_multi(&ptrs);
    if count < 0 {
        CmdResult::Err(invalid_hll())
    } else {
        CmdResult::Ok(integer(count))
    }
}

// ---------------------------------------------------------------------------
// PFMERGE
// ---------------------------------------------------------------------------

/// Port of `PFMergeInternal`'s merge + write-back. `collected` holds the dense
/// values read from every key (including the destination when it appears among
/// the sources); the union is written to `dest`.
fn merge_into(_dest: &[u8], collected: &[Vec<u8>]) -> (i32, Vec<u8>) {
    let mut out = create_dense_hll();
    let ptrs: Vec<&[u8]> = collected.iter().map(|h| h.as_slice()).collect();
    let result = pfmerge(&ptrs, &mut out);
    (result, strip_dense_slack(out))
}

fn exec_pfmerge(ctx: &mut OpContext) -> CmdResult {
    let total_keys = ctx.args.len() - 1;
    let single = ctx.owned_keys.len() == total_keys;
    let keys: Vec<usize> = ctx.owned_keys.to_vec();
    let dest = ctx.args[keys[0]].clone();

    let collected = match collect_hlls(ctx, &keys) {
        Ok(h) => h,
        Err(e) => return CmdResult::Err(e),
    };
    let (result, stored) = merge_into(&dest, &collected);

    if single {
        ctx.db.insert(
            CompactString::from_bytes(&dest),
            PrimeValue::Str(CompactString::from_bytes(&stored)),
        );
        if result != 0 {
            return CmdResult::Err(invalid_hll());
        }
        return CmdResult::Ok(ok());
    }

    // Multi-shard: hand the merged value back to the coordinator. Even on a
    // failed merge the reference writes the fresh (empty) dense HLL, so the
    // deferred store always happens.
    let value = Some(PrimeValue::Str(CompactString::from_bytes(&stored)));
    let reply = if result != 0 { RespValue::Error(invalid_hll().message) } else { ok() };
    CmdResult::deferred_store(dest, value, reply)
}

fn merge_pfmerge(parts: &[ShardPart], args: &[Vec<u8>], keys: &[usize], _now: u64) -> CmdResult {
    let dest = args[keys[0]].clone();
    let mut collected: Vec<Vec<u8>> = Vec::new();
    for p in parts {
        match resp_to_dense_values(p) {
            Ok(values) => collected.extend(values),
            Err(e) => return CmdResult::Err(e),
        }
    }
    let (result, stored) = merge_into(&dest, &collected);
    let value = Some(PrimeValue::Str(CompactString::from_bytes(&stored)));
    let reply = if result != 0 { RespValue::Error(invalid_hll().message) } else { ok() };
    CmdResult::deferred_store(dest, value, reply)
}

// ---------------------------------------------------------------------------
// Command definitions
// ---------------------------------------------------------------------------

pub static CMD_PFADD: Command = Command {
    name: "PFADD",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_pfadd,
    merge: None,
};
pub static CMD_PFCOUNT: Command = Command {
    name: "PFCOUNT",
    arity: -2,
    flags: FLAG_READONLY | FLAG_FAST | FLAG_MULTI_KEY,
    key_range: KeyRange::ALL,
    exec: exec_pfcount,
    merge: Some(merge_pfcount),
};
pub static CMD_PFMERGE: Command = Command {
    name: "PFMERGE",
    arity: -2,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_MULTI_KEY,
    key_range: KeyRange::ALL,
    exec: exec_pfmerge,
    merge: Some(merge_pfmerge),
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

    /// Dispatch a command against a single-shard DbSlice, mirroring `Run(...)`
    /// in the C++ test. `argv[0]` is the command name.
    fn dispatch(db: &mut DbSlice, argv: &[Vec<u8>]) -> CmdResult {
        let (exec, first_key_idx, owned): (fn(&mut OpContext) -> CmdResult, usize, Vec<usize>) =
            match argv[0].as_slice() {
                b"PFADD" => (exec_pfadd, 1, vec![1]),
                b"PFCOUNT" => (exec_pfcount, 1, (1..argv.len()).collect()),
                b"PFMERGE" => (exec_pfmerge, 1, (1..argv.len()).collect()),
                _ => panic!("unhandled command {:?}", argv[0]),
            };
        let mut ctx = OpContext { db, args: argv, owned_keys: &owned, first_key_idx, now_ms: 0 };
        exec(&mut ctx)
    }

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

    const INVALID_HLL: &str = "ERR Key is not a valid HyperLogLog string value";
    const CORRUPTED_HLL: &str = "INVALIDOBJ Corrupted HLL object detected.";
    const WRONG_TYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

    fn generate_unique_value(index: i64) -> String {
        format!("Value_{{{}}}", index)
    }

    /// Builds the CVE-2025-32023 payload: a sparse HLL whose XZERO run lengths
    /// sum past INT_MAX so the decoder's `idx` cursor wraps; the trailing VAL
    /// slips past the run-length guards unless every branch checks them.
    fn make_overflowing_sparse_hll() -> Vec<u8> {
        const K_XZERO_OPS: usize = 155486;
        let mut hll = Vec::with_capacity(16 + K_XZERO_OPS * 2 + 1);
        hll.extend_from_slice(b"HYLL");
        hll.push(1); // encoding = HLL_SPARSE
        hll.extend_from_slice(&[0u8; 3]); // notused
        hll.extend_from_slice(&[0u8; 8]); // cached cardinality
        for _ in 0..K_XZERO_OPS {
            hll.push(0x7f); // XZERO, 14-bit length-1 == 16383
            hll.push(0xff);
        }
        hll.push(0x80); // VAL: value 1, run length 1
        hll
    }

    #[test]
    fn simple() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key", b"1")));
        assert_eq!(0, int(run!(&mut db, b"PFADD", b"key", b"1")));
        assert_eq!(1, int(run!(&mut db, b"PFCOUNT", b"key")));
    }

    #[test]
    fn multiple_values() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key", b"1", b"2", b"3")));
        assert_eq!(3, int(run!(&mut db, b"PFCOUNT", b"key")));
        assert_eq!(0, int(run!(&mut db, b"PFADD", b"key", b"1", b"2", b"3")));
        assert_eq!(3, int(run!(&mut db, b"PFCOUNT", b"key")));
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key", b"3", b"4")));
        assert_eq!(4, int(run!(&mut db, b"PFCOUNT", b"key")));
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key", b"5")));
        assert_eq!(5, int(run!(&mut db, b"PFCOUNT", b"key")));
        assert_eq!(0, int(run!(&mut db, b"PFADD", b"key", b"1", b"2", b"3", b"4", b"5")));
        assert_eq!(5, int(run!(&mut db, b"PFCOUNT", b"key")));
    }

    #[test]
    fn promote() {
        let mut db = DbSlice::new(0);
        let promote_i = 1660;
        for i in 0..20000 {
            let v = generate_unique_value(i);
            run!(&mut db, b"PFADD", b"key", v.as_bytes());
            let len = get(&mut db, "key").unwrap().len();
            if i < promote_i {
                assert!(len < 3000 + 1, "len {len} at {i}");
            } else {
                assert_eq!(len, crate::core::hll::get_dense_hll_size(), "at {i}");
            }
        }
        let count = int(run!(&mut db, b"PFCOUNT", b"key"));
        assert!((count as f64 - 20000.0).abs() / 20000.0 < 0.05, "count {count}");
    }

    #[test]
    fn add_invalid() {
        let mut db = DbSlice::new(0);
        set(&mut db, "key", b"...");
        assert_eq!(INVALID_HLL, err(run!(&mut db, b"PFADD", b"key", b"1")));
        assert_eq!(INVALID_HLL, err(run!(&mut db, b"PFCOUNT", b"key")));
    }

    #[test]
    fn other_type() {
        let mut db = DbSlice::new(0);
        db.insert(
            CompactString::from_bytes(b"key"),
            PrimeValue::List(crate::core::quicklist::QuickList::default()),
        );
        assert_eq!(WRONG_TYPE, err(run!(&mut db, b"PFADD", b"key", b"1")));
        assert_eq!(WRONG_TYPE, err(run!(&mut db, b"PFCOUNT", b"key")));
    }

    #[test]
    fn count_empty() {
        let mut db = DbSlice::new(0);
        assert_eq!(0, int(run!(&mut db, b"PFCOUNT", b"nonexisting")));
    }

    #[test]
    fn count_multiple() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key1", b"1", b"2", b"3")));
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key2", b"1", b"2", b"3")));
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key3", b"2", b"3")));
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key4", b"4", b"5")));
        assert_eq!(5, int(run!(&mut db, b"PFCOUNT", b"key1", b"key4")));
        assert_eq!(0, int(run!(&mut db, b"PFCOUNT", b"non-existing-key1", b"non-existing-key2")));
        assert_eq!(3, int(run!(&mut db, b"PFCOUNT", b"key1", b"non-existing-key")));
        assert_eq!(3, int(run!(&mut db, b"PFCOUNT", b"key1", b"key2")));
        assert_eq!(3, int(run!(&mut db, b"PFCOUNT", b"key1", b"key3")));
        assert_eq!(3, int(run!(&mut db, b"PFCOUNT", b"key1", b"key2", b"key3")));
        assert_eq!(5, int(run!(&mut db, b"PFCOUNT", b"key1", b"key2", b"key3", b"key4")));
        assert_eq!(5, int(run!(&mut db, b"PFCOUNT", b"key1", b"key2", b"key3", b"key4", b"non-existing")));
    }

    #[test]
    fn count_multiple_with_wrong_type() {
        let mut db = DbSlice::new(0);
        set(&mut db, "key1", b"value1");
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key", b"value")));
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"list1 element1", b"data")));
        assert_eq!(
            CORRUPTED_HLL,
            err(run!(&mut db, b"PFCOUNT", b"key1", b"key", b"list1 element1"))
        );
    }

    #[test]
    fn merge_to_new() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key1", b"1", b"2", b"3")));
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key2", b"4", b"5")));
        ok_res(run!(&mut db, b"PFMERGE", b"key3", b"key1", b"key2"));
        assert_eq!(5, int(run!(&mut db, b"PFCOUNT", b"key3")));
    }

    #[test]
    fn merge_to_existing() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key1", b"1", b"2", b"3")));
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key2", b"4", b"5")));
        ok_res(run!(&mut db, b"PFMERGE", b"key3", b"key2", b"key1"));
        assert_eq!(5, int(run!(&mut db, b"PFCOUNT", b"key3")));
        ok_res(run!(&mut db, b"PFMERGE", b"key3", b"key3"));
        assert_eq!(5, int(run!(&mut db, b"PFCOUNT", b"key3")));
        ok_res(run!(&mut db, b"PFMERGE", b"key3"));
        assert_eq!(5, int(run!(&mut db, b"PFCOUNT", b"key3")));
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key4", b"4", b"5", b"6")));
        ok_res(run!(&mut db, b"PFMERGE", b"key3", b"key4"));
        assert_eq!(6, int(run!(&mut db, b"PFCOUNT", b"key3")));
    }

    #[test]
    fn merge_non_existing() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key1", b"1", b"2", b"3")));
        ok_res(run!(&mut db, b"PFMERGE", b"key3", b"key1", b"key2"));
        assert_eq!(3, int(run!(&mut db, b"PFCOUNT", b"key3")));
    }

    #[test]
    fn merge_overlapping() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key1", b"1", b"2", b"3")));
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key2", b"2", b"3")));
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key3", b"1", b"3")));
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key4", b"2", b"3")));
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key5", b"3")));
        ok_res(run!(&mut db, b"PFMERGE", b"key6", b"key1", b"key2", b"key3", b"key4", b"key5"));
        assert_eq!(3, int(run!(&mut db, b"PFCOUNT", b"key6")));
    }

    #[test]
    fn merge_invalid() {
        let mut db = DbSlice::new(0);
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key1", b"1", b"2", b"3")));
        set(&mut db, "key4", b"...");
        assert_eq!(CORRUPTED_HLL, err(run!(&mut db, b"PFMERGE", b"key1", b"key4")));
        assert_eq!(3, int(run!(&mut db, b"PFCOUNT", b"key1")));
    }

    #[test]
    fn merge_with_invalid_hll_format() {
        let mut db = DbSlice::new(0);
        let k1 = "complex@key \"weird!field\" \"value\\nwith\\tescape sequences\"";
        let k2 = "\"key with \\\"quotes\\\"\" \"value with \\\\backslashes\\\\\"";
        assert_eq!(1, int(run!(&mut db, b"PFADD", k1.as_bytes(), b"some_element")));
        let appended = {
            let mut v = get(&mut db, k1).unwrap();
            v.extend_from_slice(b"corrupt_data");
            v
        };
        set(&mut db, k1, &appended);
        assert_eq!(1, int(run!(&mut db, b"PFADD", k2.as_bytes(), b"element1")));
        assert_eq!(
            CORRUPTED_HLL,
            err(run!(&mut db, b"PFMERGE", b"result_key", k1.as_bytes(), k2.as_bytes()))
        );
    }

    #[test]
    fn corrupted_sparse_run_length_overflow() {
        let mut db = DbSlice::new(0);
        let payload = make_overflowing_sparse_hll();
        set(&mut db, "overflow", &payload);

        assert_eq!(CORRUPTED_HLL, err(run!(&mut db, b"PFCOUNT", b"overflow")));

        assert_eq!(1, int(run!(&mut db, b"PFADD", b"src", b"hi")));
        assert_eq!(CORRUPTED_HLL, err(run!(&mut db, b"PFMERGE", b"dest", b"overflow", b"src")));

        assert_eq!(INVALID_HLL, err(run!(&mut db, b"PFADD", b"overflow", b"foo")));
    }

    #[test]
    fn corrupted_sparse_truncated_run() {
        let mut db = DbSlice::new(0);
        let mut hll = b"HYLL".to_vec();
        hll.push(1);
        hll.extend_from_slice(&[0u8; 3]);
        hll.extend_from_slice(&[0u8; 8]);
        hll.push(0x7f);
        hll.push(0xff);
        hll.push(0x7f);
        hll.push(0xff);
        set(&mut db, "truncated", &hll);
        assert_eq!(CORRUPTED_HLL, err(run!(&mut db, b"PFCOUNT", b"truncated")));
    }

    #[test]
    fn count_multiple_agrees_with_merge() {
        const K_VALUES_PER_KEY: i64 = 20000;
        let mut db = DbSlice::new(0);
        for i in 0..K_VALUES_PER_KEY {
            run!(&mut db, b"PFADD", b"k1", generate_unique_value(i).as_bytes());
            run!(&mut db, b"PFADD", b"k2", generate_unique_value(K_VALUES_PER_KEY + i).as_bytes());
        }
        ok_res(run!(&mut db, b"PFMERGE", b"merged", b"k1", b"k2"));
        let merged = int(run!(&mut db, b"PFCOUNT", b"merged"));
        assert_eq!(merged, int(run!(&mut db, b"PFCOUNT", b"k1", b"k2")));
        assert!((merged as f64 - 2.0 * K_VALUES_PER_KEY as f64).abs() / (2.0 * K_VALUES_PER_KEY as f64) < 0.05);
    }

    #[test]
    fn sparse_set_promotes_on_large_count() {
        let mut db = DbSlice::new(0);
        let promoting = b".K{bTLLX";
        assert_eq!(1, int(run!(&mut db, b"PFADD", b"key", promoting)));
        assert_eq!(crate::core::hll::get_dense_hll_size(), get(&mut db, "key").unwrap().len());
        assert_eq!(1, int(run!(&mut db, b"PFCOUNT", b"key")));
    }

    #[test]
    fn multi_shard_count_and_merge() {
        // PFCOUNT across two shards: each contributes its dense values.
        let parts = [
            ShardPart {
                shard: 0,
                owned_key_idxs: vec![1],
                result: CmdResult::Ok(RespValue::Array(vec![
                    RespValue::Bulk(pf_stored(&["1", "2", "3"])),
                ])),
            },
            ShardPart {
                shard: 1,
                owned_key_idxs: vec![2],
                result: CmdResult::Ok(RespValue::Array(vec![
                    RespValue::Bulk(pf_stored(&["4", "5"])),
                ])),
            },
        ];
        let args = vec![b"PFCOUNT".to_vec(), b"key1".to_vec(), b"key2".to_vec()];
        let keys = [1usize, 2];
        assert_eq!(5, int(merge_pfcount(&parts, &args, &keys, 0)));

        // PFMERGE: same partials produce a deferred store of the union.
        let args = vec![b"PFMERGE".to_vec(), b"dest".to_vec(), b"key1".to_vec(), b"key2".to_vec()];
        let keys = [1usize, 2, 3];
        match merge_pfmerge(&parts, &args, &keys, 0) {
            CmdResult::DeferredStore { key, value, reply } => {
                assert_eq!(key, b"dest");
                let stored = match value {
                    Some(PrimeValue::Str(s)) => s.as_bytes().to_vec(),
                    _ => panic!("expected string store"),
                };
                assert_eq!(pfcount_multi(&[stored.as_slice()]), 5);
                assert_eq!(reply, RespValue::Simple("OK".into()));
            }
            o => panic!("expected DeferredStore, got {:?}", o.into_resp_value()),
        }
    }

    fn pf_stored(values: &[&str]) -> Vec<u8> {
        let mut hll = create_dense_hll();
        for v in values {
            assert!(pfadd_dense(&mut hll, v.as_bytes()) >= 0);
        }
        strip_dense_slack(hll)
    }
}
