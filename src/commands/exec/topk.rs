//! Top-K commands (TOPK.RESERVE/ADD/INCRBY/QUERY/COUNT/LIST/INFO), ported from
//! `dragonfly/src/server/topk_family.cc`.
//!
//! The TOPK family is a Rust `PrimeValue::Topk` backed by `core::topk::Topk`.
//! TOPK.RESERVE creates the sketch; ADD and INCRBY return the evicted item
//! (or nil) for every argument; QUERY/COUNT answer over a list of items;
//! LIST (optionally WITHCOUNT) and INFO report the current state.

use crate::commands::{
    Command, FLAG_DENYOOM, FLAG_FAST, FLAG_READONLY, FLAG_WRITE, KeyRange, OpContext, bulk, integer,
};
use crate::core::PrimeValue;
use crate::core::topk::{DEFAULT_DECAY, DEFAULT_DEPTH, DEFAULT_WIDTH, Topk};
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::{format_double, parse_double, parse_u64};

/// kMax (k cap), kMaxWidth / kMaxDepth: upper bounds validated by the command
/// layer to prevent excessive memory allocation.
const K_MAX_K: u32 = 100_000;
const K_MAX_WIDTH: u32 = 1_000_000;
const K_MAX_DEPTH: u32 = 100;

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

fn no_such_key() -> RespError {
    RespError::new("ERR no such key")
}

fn item_exists() -> RespError {
    RespError::new("ERR item exists")
}

fn k_greater_than_zero() -> RespError {
    RespError::new("ERR k must be greater than 0")
}

fn k_max_exceeded() -> RespError {
    RespError::new(format!("ERR k exceeds maximum allowed value of {K_MAX_K}"))
}

fn width_depth_zero() -> RespError {
    RespError::new("ERR width and depth must be greater than 0")
}

fn width_depth_caps() -> RespError {
    RespError::new(format!(
        "ERR width must not exceed {K_MAX_WIDTH} and depth must not exceed {K_MAX_DEPTH}"
    ))
}

fn decay_range() -> RespError {
    RespError::new("ERR decay must be between 0 and 1")
}

fn incr_range() -> RespError {
    RespError::new("ERR increment must be between 1 and 100000")
}

fn parse_u32(s: &[u8]) -> Option<u32> {
    parse_u64(s).and_then(|v| u32::try_from(v).ok())
}

// ---------------------------------------------------------------------------
// TOPK.RESERVE
// ---------------------------------------------------------------------------

fn exec_topk_reserve(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let Some(k) = parse_u32(&ctx.args[key_idx + 1]) else {
        return CmdResult::Err(RespError::integer());
    };
    if k == 0 {
        return CmdResult::Err(k_greater_than_zero());
    }
    if k > K_MAX_K {
        return CmdResult::Err(k_max_exceeded());
    }

    let mut width = DEFAULT_WIDTH;
    let mut depth = DEFAULT_DEPTH;
    let mut decay = DEFAULT_DECAY;
    let n = ctx.args.len();
    if key_idx + 2 < n {
        // Optional width/depth/decay are all-or-nothing.
        width = match parse_u32(&ctx.args[key_idx + 2]) {
            Some(w) => w,
            None => return CmdResult::Err(RespError::integer()),
        };
        if key_idx + 3 >= n {
            return CmdResult::Err(RespError::syntax());
        }
        depth = match parse_u32(&ctx.args[key_idx + 3]) {
            Some(d) => d,
            None => return CmdResult::Err(RespError::integer()),
        };
        if key_idx + 4 >= n {
            return CmdResult::Err(RespError::syntax());
        }
        decay = match parse_double(&ctx.args[key_idx + 4]) {
            Some(d) => d,
            None => return CmdResult::Err(RespError::float()),
        };
        if width == 0 || depth == 0 {
            return CmdResult::Err(width_depth_zero());
        }
        if width > K_MAX_WIDTH || depth > K_MAX_DEPTH {
            return CmdResult::Err(width_depth_caps());
        }
        if !(0.0..=1.0).contains(&decay) {
            return CmdResult::Err(decay_range());
        }
        if key_idx + 5 < n {
            return CmdResult::Err(RespError::syntax());
        }
    }

    match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::Topk(_)) => return CmdResult::Err(item_exists()),
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => {}
    }
    ctx.db
        .insert(key, PrimeValue::Topk(Topk::new(k, width, depth, decay)));
    CmdResult::Ok(crate::commands::ok())
}

// ---------------------------------------------------------------------------
// TOPK.ADD / TOPK.INCRBY
// ---------------------------------------------------------------------------

/// Port of `OpAdd` / `OpIncrBy`: return an array with one element per item —
/// the evicted item's key, or nil when nothing was displaced.
fn exec_topk_add(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let items = &ctx.args[key_idx + 1..];
    if items.is_empty() {
        return CmdResult::Err(RespError::syntax());
    }
    let topk = match ctx.db.find_mut(&ctx.args[key_idx], ctx.now_ms) {
        Some(PrimeValue::Topk(t)) => t,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Err(no_such_key()),
    };
    let results: Vec<RespValue> = items
        .iter()
        .map(|it| match topk.add(it) {
            Some(evicted) => bulk(evicted.as_bytes()),
            None => RespValue::Nil,
        })
        .collect();
    CmdResult::Ok(RespValue::Array(results))
}

/// Parse `<item> <increment>` pairs following the key. Mirrors the C++
/// `CmdIncrBy` pair parsing: an increment must parse and fall in
/// [1, 100000]; an odd trailing argument is a syntax error.
fn parse_incrby_pairs(args: &[Vec<u8>], key_idx: usize) -> Result<Vec<(Vec<u8>, u32)>, RespError> {
    let mut items = Vec::new();
    let mut i = key_idx + 1;
    while i < args.len() {
        if i + 1 >= args.len() {
            return Err(RespError::syntax());
        }
        let item = args[i].clone();
        let Some(incr) = crate::util::parse_i64(&args[i + 1]) else {
            return Err(RespError::integer());
        };
        if !(1..=100_000).contains(&incr) {
            return Err(incr_range());
        }
        items.push((item, incr as u32));
        i += 2;
    }
    Ok(items)
}

fn exec_topk_incrby(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let items = match parse_incrby_pairs(ctx.args, key_idx) {
        Ok(items) => items,
        Err(e) => return CmdResult::Err(e),
    };
    if items.is_empty() {
        return CmdResult::Err(RespError::syntax());
    }
    let topk = match ctx.db.find_mut(&ctx.args[key_idx], ctx.now_ms) {
        Some(PrimeValue::Topk(t)) => t,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Err(no_such_key()),
    };
    let results: Vec<RespValue> = items
        .iter()
        .map(|(it, incr)| match topk.incr_by(it, *incr) {
            Some(evicted) => bulk(evicted.as_bytes()),
            None => RespValue::Nil,
        })
        .collect();
    CmdResult::Ok(RespValue::Array(results))
}

// ---------------------------------------------------------------------------
// TOPK.QUERY / TOPK.COUNT
// ---------------------------------------------------------------------------

fn exec_topk_query(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let items = &ctx.args[key_idx + 1..];
    if items.is_empty() {
        return CmdResult::Err(RespError::syntax());
    }
    let topk = match ctx.db.find(&ctx.args[key_idx], ctx.now_ms) {
        Some(PrimeValue::Topk(t)) => t,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Err(no_such_key()),
    };
    let results: Vec<RespValue> = items
        .iter()
        .map(|it| integer(i64::from(topk.query(it))))
        .collect();
    CmdResult::Ok(RespValue::Array(results))
}

fn exec_topk_count(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let items = &ctx.args[key_idx + 1..];
    if items.is_empty() {
        return CmdResult::Err(RespError::syntax());
    }
    let topk = match ctx.db.find(&ctx.args[key_idx], ctx.now_ms) {
        Some(PrimeValue::Topk(t)) => t,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Err(no_such_key()),
    };
    let results: Vec<RespValue> = items
        .iter()
        .map(|it| integer(i64::from(topk.count(it))))
        .collect();
    CmdResult::Ok(RespValue::Array(results))
}

// ---------------------------------------------------------------------------
// TOPK.LIST
// ---------------------------------------------------------------------------

fn exec_topk_list(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let with_count = ctx
        .args
        .get(key_idx + 1)
        .is_some_and(|a| a.eq_ignore_ascii_case(b"WITHCOUNT"));
    if ctx.args.len() > key_idx + 1 + usize::from(with_count) {
        return CmdResult::Err(RespError::syntax());
    }
    let topk = match ctx.db.find(&ctx.args[key_idx], ctx.now_ms) {
        Some(PrimeValue::Topk(t)) => t,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Err(no_such_key()),
    };
    let mut reply = Vec::new();
    for item in topk.list() {
        reply.push(bulk(item.item.as_bytes()));
        if with_count {
            reply.push(integer(i64::from(item.count)));
        }
    }
    CmdResult::Ok(RespValue::Array(reply))
}

// ---------------------------------------------------------------------------
// TOPK.INFO
// ---------------------------------------------------------------------------

fn exec_topk_info(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let topk = match ctx.db.find(&ctx.args[key_idx], ctx.now_ms) {
        Some(PrimeValue::Topk(t)) => t,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Err(no_such_key()),
    };
    CmdResult::Ok(RespValue::Array(vec![
        bulk("k"),
        integer(i64::from(topk.k())),
        bulk("width"),
        integer(i64::from(topk.width())),
        bulk("depth"),
        integer(i64::from(topk.depth())),
        bulk("decay"),
        bulk(format_double(topk.decay())),
    ]))
}

// ---------------------------------------------------------------------------
// Command definitions
// ---------------------------------------------------------------------------

pub static CMD_TOPK_RESERVE: Command = Command {
    name: "TOPK.RESERVE",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_topk_reserve,
    merge: None,
};
pub static CMD_TOPK_ADD: Command = Command {
    name: "TOPK.ADD",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_topk_add,
    merge: None,
};
pub static CMD_TOPK_INCRBY: Command = Command {
    name: "TOPK.INCRBY",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_topk_incrby,
    merge: None,
};
pub static CMD_TOPK_QUERY: Command = Command {
    name: "TOPK.QUERY",
    arity: -3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_topk_query,
    merge: None,
};
pub static CMD_TOPK_COUNT: Command = Command {
    name: "TOPK.COUNT",
    arity: -3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_topk_count,
    merge: None,
};
pub static CMD_TOPK_LIST: Command = Command {
    name: "TOPK.LIST",
    arity: -2,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_topk_list,
    merge: None,
};
pub static CMD_TOPK_INFO: Command = Command {
    name: "TOPK.INFO",
    arity: 2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_topk_info,
    merge: None,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::compact::CompactString;
    use crate::core::db::DbSlice;

    /// Dispatch with the framework-level arity check applied, mirroring
    /// `Command::check_arity` in the server pipeline.
    fn dispatch(db: &mut DbSlice, argv: &[Vec<u8>]) -> CmdResult {
        let cmd: &Command = match argv[0].as_slice() {
            b"TOPK.RESERVE" => &CMD_TOPK_RESERVE,
            b"TOPK.ADD" => &CMD_TOPK_ADD,
            b"TOPK.INCRBY" => &CMD_TOPK_INCRBY,
            b"TOPK.QUERY" => &CMD_TOPK_QUERY,
            b"TOPK.COUNT" => &CMD_TOPK_COUNT,
            b"TOPK.LIST" => &CMD_TOPK_LIST,
            b"TOPK.INFO" => &CMD_TOPK_INFO,
            _ => panic!("unhandled command {:?}", argv[0]),
        };
        if let Some(e) = cmd.check_arity(argv.len()) {
            return CmdResult::Err(RespError::new(e));
        }
        let owned = vec![cmd.key_range.first];
        let mut ctx = OpContext {
            db,
            args: argv,
            owned_keys: &owned,
            first_key_idx: 1,
            conn_id: 0,
            now_ms: 0,
        };
        (cmd.exec)(&mut ctx)
    }

    macro_rules! run {
        ($db:expr, $($arg:expr),+ $(,)?) => {
            dispatch($db, &[$($arg.to_vec()),+])
        };
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

    fn type_of(db: &mut DbSlice, key: &str) -> String {
        match db.find(key.as_bytes(), 0) {
            Some(v) => v.type_name().to_string(),
            None => "none".to_string(),
        }
    }

    fn set(db: &mut DbSlice, key: &str, value: &str) {
        db.insert(key.as_bytes(), PrimeValue::Str(CompactString::from(value)));
    }

    fn reserve_default(db: &mut DbSlice, key: &str, k: u32) {
        ok_res(run!(
            db,
            b"TOPK.RESERVE",
            key.as_bytes(),
            k.to_string().as_bytes()
        ));
    }

    fn reserve_custom(db: &mut DbSlice, key: &str, k: u32, width: u32, depth: u32, decay: f64) {
        ok_res(run!(
            db,
            b"TOPK.RESERVE",
            key.as_bytes(),
            k.to_string().as_bytes(),
            width.to_string().as_bytes(),
            depth.to_string().as_bytes(),
            decay.to_string().as_bytes(),
        ));
    }

    fn add_item(db: &mut DbSlice, key: &str, item: &str) -> CmdResult {
        run!(db, b"TOPK.ADD", key.as_bytes(), item.as_bytes())
    }

    fn incr_by_item(db: &mut DbSlice, key: &str, item: &str, incr: u32) -> CmdResult {
        run!(
            db,
            b"TOPK.INCRBY",
            key.as_bytes(),
            item.as_bytes(),
            incr.to_string().as_bytes()
        )
    }

    /// Assert every element of an array reply is Nil.
    fn assert_all_nil(r: CmdResult) {
        match r.into_resp_value() {
            RespValue::Array(a) => {
                assert!(!a.is_empty());
                for v in &a {
                    assert!(matches!(v, RespValue::Nil), "expected nil, got {v:?}");
                }
            }
            o => panic!("expected array, got {o:?}"),
        }
    }

    #[test]
    fn commands_on_non_existent_key() {
        let mut db = DbSlice::new(0);
        assert!(err(run!(&mut db, b"TOPK.ADD", b"noexist", b"foo")).contains("no such key"));
        assert!(
            err(run!(&mut db, b"TOPK.INCRBY", b"noexist", b"foo", b"1")).contains("no such key")
        );
        assert!(err(run!(&mut db, b"TOPK.QUERY", b"noexist", b"foo")).contains("no such key"));
        assert!(err(run!(&mut db, b"TOPK.COUNT", b"noexist", b"foo")).contains("no such key"));
        assert!(err(run!(&mut db, b"TOPK.LIST", b"noexist")).contains("no such key"));
        assert!(err(run!(&mut db, b"TOPK.INFO", b"noexist")).contains("no such key"));
    }

    #[test]
    fn wrong_type_errors() {
        let mut db = DbSlice::new(0);
        set(&mut db, "mystr", "value");
        assert!(err(run!(&mut db, b"TOPK.ADD", b"mystr", b"foo")).contains("WRONGTYPE"));
        assert!(err(run!(&mut db, b"TOPK.INCRBY", b"mystr", b"foo", b"1")).contains("WRONGTYPE"));
        assert!(err(run!(&mut db, b"TOPK.QUERY", b"mystr", b"foo")).contains("WRONGTYPE"));
        assert!(err(run!(&mut db, b"TOPK.COUNT", b"mystr", b"foo")).contains("WRONGTYPE"));
        assert!(err(run!(&mut db, b"TOPK.LIST", b"mystr")).contains("WRONGTYPE"));
        assert!(err(run!(&mut db, b"TOPK.INFO", b"mystr")).contains("WRONGTYPE"));
    }

    #[test]
    fn type_command() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "myk", 5);
        assert_eq!(type_of(&mut db, "myk"), "TopK-TYPE");
    }

    #[test]
    fn delete_key() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "myk", 5);
        let _ = add_item(&mut db, "myk", "foo");
        assert!(db.remove(b"myk").is_some());
        assert!(err(run!(&mut db, b"TOPK.ADD", b"myk", b"foo")).contains("no such key"));
    }

    #[test]
    fn reserve_on_existing_wrong_type() {
        let mut db = DbSlice::new(0);
        set(&mut db, "mystr", "val");
        assert!(err(run!(&mut db, b"TOPK.RESERVE", b"mystr", b"5")).contains("WRONGTYPE"));
    }

    #[test]
    fn reserve_default_params() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 10);
        let r = run!(&mut db, b"TOPK.INFO", b"tk");
        match r.into_resp_value() {
            RespValue::Array(a) => {
                assert_eq!(a[0], RespValue::Bulk(b"k".to_vec()));
                assert_eq!(a[1], RespValue::Integer(10));
                assert_eq!(a[2], RespValue::Bulk(b"width".to_vec()));
                assert_eq!(a[3], RespValue::Integer(8));
                assert_eq!(a[4], RespValue::Bulk(b"depth".to_vec()));
                assert_eq!(a[5], RespValue::Integer(7));
                assert_eq!(a[6], RespValue::Bulk(b"decay".to_vec()));
                assert_eq!(a[7], RespValue::Bulk(b"0.9".to_vec()));
            }
            o => panic!("expected info array, got {o:?}"),
        }
    }

    #[test]
    fn reserve_all_custom_params() {
        let mut db = DbSlice::new(0);
        reserve_custom(&mut db, "tk", 20, 100, 5, 0.95);
        let r = run!(&mut db, b"TOPK.INFO", b"tk");
        match r.into_resp_value() {
            RespValue::Array(a) => {
                assert_eq!(a[1], RespValue::Integer(20));
                assert_eq!(a[3], RespValue::Integer(100));
                assert_eq!(a[5], RespValue::Integer(5));
                assert_eq!(a[7], RespValue::Bulk(b"0.95".to_vec()));
            }
            o => panic!("expected info array, got {o:?}"),
        }
    }

    #[test]
    fn reserve_min_k() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 1);
        assert_eq!(
            ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"anything")),
            vec![0]
        );
    }

    #[test]
    fn reserve_large_k() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 10000);
        let r = run!(&mut db, b"TOPK.INFO", b"tk");
        match r.into_resp_value() {
            RespValue::Array(a) => assert_eq!(a[1], RespValue::Integer(10000)),
            o => panic!("expected info array, got {o:?}"),
        }
    }

    #[test]
    fn reserve_decay_zero() {
        let mut db = DbSlice::new(0);
        reserve_custom(&mut db, "tk", 5, 8, 7, 0.0);
        let r = run!(&mut db, b"TOPK.INFO", b"tk");
        match r.into_resp_value() {
            RespValue::Array(a) => assert_eq!(a[7], RespValue::Bulk(b"0".to_vec())),
            o => panic!("expected info array, got {o:?}"),
        }
    }

    #[test]
    fn reserve_decay_one() {
        let mut db = DbSlice::new(0);
        reserve_custom(&mut db, "tk", 5, 8, 7, 1.0);
        let r = run!(&mut db, b"TOPK.INFO", b"tk");
        match r.into_resp_value() {
            RespValue::Array(a) => assert_eq!(a[7], RespValue::Bulk(b"1".to_vec())),
            o => panic!("expected info array, got {o:?}"),
        }
    }

    #[test]
    fn reserve_k_zero() {
        let mut db = DbSlice::new(0);
        assert!(
            err(run!(&mut db, b"TOPK.RESERVE", b"tk", b"0")).contains("k must be greater than 0")
        );
    }

    #[test]
    fn reserve_k_negative() {
        let mut db = DbSlice::new(0);
        assert!(err(run!(&mut db, b"TOPK.RESERVE", b"tk", b"-1")).contains("not an integer"));
    }

    #[test]
    fn reserve_k_not_a_number() {
        let mut db = DbSlice::new(0);
        assert!(err(run!(&mut db, b"TOPK.RESERVE", b"tk", b"abc")).contains("not an integer"));
    }

    #[test]
    fn reserve_width_zero() {
        let mut db = DbSlice::new(0);
        let r = run!(&mut db, b"TOPK.RESERVE", b"tk", b"5", b"0", b"7", b"0.9");
        assert!(err(r).contains("width and depth must be greater than 0"));
    }

    #[test]
    fn reserve_depth_zero() {
        let mut db = DbSlice::new(0);
        let r = run!(&mut db, b"TOPK.RESERVE", b"tk", b"5", b"8", b"0", b"0.9");
        assert!(err(r).contains("width and depth must be greater than 0"));
    }

    #[test]
    fn reserve_decay_above_one() {
        let mut db = DbSlice::new(0);
        let r = run!(&mut db, b"TOPK.RESERVE", b"tk", b"5", b"8", b"7", b"1.5");
        assert!(err(r).contains("decay must be between 0 and 1"));
    }

    #[test]
    fn reserve_decay_negative() {
        let mut db = DbSlice::new(0);
        let r = run!(&mut db, b"TOPK.RESERVE", b"tk", b"5", b"8", b"7", b"-0.1");
        assert!(err(r).contains("decay must be between 0 and 1"));
    }

    #[test]
    fn reserve_duplicate_key() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert!(err(run!(&mut db, b"TOPK.RESERVE", b"tk", b"10")).contains("item exists"));
    }

    #[test]
    fn reserve_too_few_args() {
        let mut db = DbSlice::new(0);
        assert!(err(run!(&mut db, b"TOPK.RESERVE", b"tk")).contains("wrong number of arguments"));
    }

    #[test]
    fn reserve_partial_optional_params() {
        let mut db = DbSlice::new(0);
        assert!(err(run!(&mut db, b"TOPK.RESERVE", b"tk", b"5", b"100")).contains("syntax error"));
        assert!(
            err(run!(&mut db, b"TOPK.RESERVE", b"tk", b"5", b"100", b"7")).contains("syntax error")
        );
    }

    #[test]
    fn reserve_trailing_args() {
        let mut db = DbSlice::new(0);
        let r = run!(
            &mut db,
            b"TOPK.RESERVE",
            b"tk",
            b"5",
            b"8",
            b"7",
            b"0.9",
            b"extra"
        );
        assert!(err(r).contains("syntax error"));
    }

    #[test]
    fn reserve_dimensions_exceed_caps() {
        let mut db = DbSlice::new(0);
        assert!(
            err(run!(
                &mut db,
                b"TOPK.RESERVE",
                b"tk1",
                b"50",
                b"1000001",
                b"7",
                b"0.9"
            ))
            .contains("must not exceed")
        );
        assert!(
            err(run!(
                &mut db,
                b"TOPK.RESERVE",
                b"tk2",
                b"50",
                b"100000",
                b"101",
                b"0.9"
            ))
            .contains("must not exceed")
        );
    }

    #[test]
    fn add_single_item() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert_all_nil(add_item(&mut db, "tk", "foo"));
    }

    #[test]
    fn add_multiple_items() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert_all_nil(run!(&mut db, b"TOPK.ADD", b"tk", b"a", b"b", b"c"));
    }

    #[test]
    fn add_duplicate_item() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        let _ = add_item(&mut db, "tk", "foo");
        let _ = add_item(&mut db, "tk", "foo");
        let _ = add_item(&mut db, "tk", "foo");
        let counts = ints(run!(&mut db, b"TOPK.COUNT", b"tk", b"foo"));
        assert!(counts[0] >= 1);
        assert_eq!(ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"foo")), vec![1]);
    }

    #[test]
    fn add_no_items() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert!(err(run!(&mut db, b"TOPK.ADD", b"tk")).contains("wrong number of arguments"));
    }

    #[test]
    fn add_eviction() {
        let mut db = DbSlice::new(0);
        reserve_custom(&mut db, "tk", 2, 50, 7, 0.9);
        let _ = incr_by_item(&mut db, "tk", "heavy1", 10000);
        let _ = incr_by_item(&mut db, "tk", "heavy2", 5000);

        // A weak item can't beat the heap minimum: nil, no eviction.
        assert_all_nil(add_item(&mut db, "tk", "weak"));
        match run!(&mut db, b"TOPK.LIST", b"tk").into_resp_value() {
            RespValue::Array(a) => assert_eq!(a.len(), 2),
            o => panic!("expected array, got {o:?}"),
        }
        assert_eq!(
            ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"heavy1")),
            vec![1]
        );
        assert_eq!(
            ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"heavy2")),
            vec![1]
        );

        // A strong item evicts the weakest: a bulk string, not nil.
        let r = incr_by_item(&mut db, "tk", "newcomer", 100_000);
        match r.into_resp_value() {
            RespValue::Array(a) => {
                assert_eq!(a.len(), 1);
                assert!(
                    matches!(a[0], RespValue::Bulk(_)),
                    "expected evicted string, got {:?}",
                    a[0]
                );
            }
            o => panic!("expected array, got {o:?}"),
        }
    }

    #[test]
    fn add_special_characters() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        let _ = add_item(&mut db, "tk", "hello world");
        let _ = add_item(&mut db, "tk", "foo\tbar");
        let _ = add_item(&mut db, "tk", "");
        assert_eq!(
            ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"hello world")),
            vec![1]
        );
    }

    #[test]
    fn add_large_batch() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 10);
        let mut argv = vec![b"TOPK.ADD".to_vec(), b"tk".to_vec()];
        for i in 0..100 {
            argv.push(format!("item{i}").into_bytes());
        }
        let _ = dispatch(&mut db, &argv);
        match run!(&mut db, b"TOPK.LIST", b"tk").into_resp_value() {
            RespValue::Array(a) => assert!(a.len() <= 10),
            o => panic!("expected array, got {o:?}"),
        }
    }

    #[test]
    fn incr_by_single_item() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert_all_nil(incr_by_item(&mut db, "tk", "foo", 10));
        let counts = ints(run!(&mut db, b"TOPK.COUNT", b"tk", b"foo"));
        assert!(counts[0] >= 1);
    }

    #[test]
    fn incr_by_multiple_items() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert_all_nil(run!(
            &mut db,
            b"TOPK.INCRBY",
            b"tk",
            b"a",
            b"5",
            b"b",
            b"3",
            b"c",
            b"7"
        ));
    }

    #[test]
    fn incr_by_accumulates() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        let _ = incr_by_item(&mut db, "tk", "foo", 10);
        let _ = incr_by_item(&mut db, "tk", "foo", 20);
        let counts = ints(run!(&mut db, b"TOPK.COUNT", b"tk", b"foo"));
        assert!(counts[0] >= 1);
    }

    #[test]
    fn incr_by_min_increment() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert_all_nil(incr_by_item(&mut db, "tk", "foo", 1));
    }

    #[test]
    fn incr_by_max_increment() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert_all_nil(incr_by_item(&mut db, "tk", "foo", 100_000));
    }

    #[test]
    fn incr_by_zero_increment() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        let r = incr_by_item(&mut db, "tk", "foo", 0);
        assert!(err(r).contains("increment must be between 1 and 100000"));
    }

    #[test]
    fn incr_by_exceeds_max_increment() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        let r = run!(&mut db, b"TOPK.INCRBY", b"tk", b"foo", b"100001");
        assert!(err(r).contains("increment must be between 1 and 100000"));
    }

    #[test]
    fn incr_by_non_numeric_increment() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        let r = run!(&mut db, b"TOPK.INCRBY", b"tk", b"foo", b"notanumber");
        assert!(err(r).contains("not an integer"));
    }

    #[test]
    fn incr_by_odd_args() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert!(
            err(run!(&mut db, b"TOPK.INCRBY", b"tk", b"foo")).contains("wrong number of arguments")
        );
        assert!(
            err(run!(&mut db, b"TOPK.INCRBY", b"tk", b"foo", b"1", b"bar"))
                .contains("syntax error")
        );
    }

    #[test]
    fn incr_by_no_items() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert!(err(run!(&mut db, b"TOPK.INCRBY", b"tk")).contains("wrong number of arguments"));
    }

    #[test]
    fn query_present_item() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        let _ = add_item(&mut db, "tk", "foo");
        assert_eq!(ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"foo")), vec![1]);
    }

    #[test]
    fn query_absent_item() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert_eq!(
            ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"neveradded")),
            vec![0]
        );
    }

    #[test]
    fn query_multiple_mixed() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        let _ = add_item(&mut db, "tk", "a");
        let _ = add_item(&mut db, "tk", "b");
        assert_eq!(
            ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"a", b"b", b"c")),
            vec![1, 1, 0]
        );
    }

    #[test]
    fn query_empty_topk() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert_eq!(
            ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"anything")),
            vec![0]
        );
    }

    #[test]
    fn query_no_items() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert!(err(run!(&mut db, b"TOPK.QUERY", b"tk")).contains("wrong number of arguments"));
    }

    #[test]
    fn count_single_item() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        let _ = incr_by_item(&mut db, "tk", "foo", 10);
        let counts = ints(run!(&mut db, b"TOPK.COUNT", b"tk", b"foo"));
        assert!(counts[0] >= 1);
    }

    #[test]
    fn count_absent_item() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert_eq!(
            ints(run!(&mut db, b"TOPK.COUNT", b"tk", b"neveradded")),
            vec![0]
        );
    }

    #[test]
    fn count_multiple_relative_order() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        let _ = incr_by_item(&mut db, "tk", "low", 10);
        let _ = incr_by_item(&mut db, "tk", "high", 100);
        let counts = ints(run!(&mut db, b"TOPK.COUNT", b"tk", b"high", b"low"));
        assert!(counts[0] >= counts[1]);
    }

    #[test]
    fn count_empty_topk() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert_eq!(
            ints(run!(&mut db, b"TOPK.COUNT", b"tk", b"anything")),
            vec![0]
        );
    }

    #[test]
    fn count_item_outside_of_heap() {
        let mut db = DbSlice::new(0);
        reserve_custom(&mut db, "tk", 1, 50, 7, 1.0);
        let _ = incr_by_item(&mut db, "tk", "heavy", 1000);
        let _ = incr_by_item(&mut db, "tk", "victim", 5);

        assert_eq!(
            ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"victim")),
            vec![0]
        );
        let counts = ints(run!(&mut db, b"TOPK.COUNT", b"tk", b"victim"));
        assert!(counts[0] >= 5);
        let counts = ints(run!(&mut db, b"TOPK.COUNT", b"tk", b"heavy"));
        assert!(counts[0] >= 1000);
    }

    #[test]
    fn count_no_items() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert!(err(run!(&mut db, b"TOPK.COUNT", b"tk")).contains("wrong number of arguments"));
    }

    #[test]
    fn list_empty() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        match run!(&mut db, b"TOPK.LIST", b"tk").into_resp_value() {
            RespValue::Array(a) => assert!(a.is_empty()),
            o => panic!("expected empty array, got {o:?}"),
        }
    }

    #[test]
    fn list_after_adds() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        let _ = run!(&mut db, b"TOPK.ADD", b"tk", b"a", b"b", b"c");
        match run!(&mut db, b"TOPK.LIST", b"tk").into_resp_value() {
            RespValue::Array(a) => assert_eq!(a.len(), 3),
            o => panic!("expected array, got {o:?}"),
        }
    }

    #[test]
    fn list_capped_at_k() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 3);
        for i in 0..10 {
            let _ = incr_by_item(&mut db, "tk", &format!("item{i}"), 100);
        }
        match run!(&mut db, b"TOPK.LIST", b"tk").into_resp_value() {
            RespValue::Array(a) => assert_eq!(a.len(), 3),
            o => panic!("expected array, got {o:?}"),
        }
    }

    #[test]
    fn list_with_count() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 3);
        let _ = incr_by_item(&mut db, "tk", "a", 100);
        let _ = incr_by_item(&mut db, "tk", "b", 50);
        let _ = incr_by_item(&mut db, "tk", "c", 10);
        match run!(&mut db, b"TOPK.LIST", b"tk", b"WITHCOUNT").into_resp_value() {
            RespValue::Array(a) => {
                assert_eq!(a.len(), 6);
                for i in (0..a.len()).step_by(2) {
                    assert!(matches!(a[i], RespValue::Bulk(_)));
                    match a[i + 1] {
                        RespValue::Integer(n) => assert!(n >= 1),
                        ref o => panic!("expected count integer, got {o:?}"),
                    }
                }
            }
            o => panic!("expected array, got {o:?}"),
        }
    }

    #[test]
    fn list_with_count_case_insensitive() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 3);
        let _ = add_item(&mut db, "tk", "a");
        match run!(&mut db, b"TOPK.LIST", b"tk", b"withcount").into_resp_value() {
            RespValue::Array(a) => assert_eq!(a.len(), 2),
            o => panic!("expected array, got {o:?}"),
        }
    }

    #[test]
    fn list_descending_order() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 3);
        let _ = incr_by_item(&mut db, "tk", "low", 10);
        let _ = incr_by_item(&mut db, "tk", "mid", 50);
        let _ = incr_by_item(&mut db, "tk", "high", 100);
        match run!(&mut db, b"TOPK.LIST", b"tk", b"WITHCOUNT").into_resp_value() {
            RespValue::Array(a) => {
                assert_eq!(a.len(), 6);
                let mut prev = i64::MAX;
                for i in (1..a.len()).step_by(2) {
                    match a[i] {
                        RespValue::Integer(n) => {
                            assert!(n <= prev);
                            prev = n;
                        }
                        ref o => panic!("expected count integer, got {o:?}"),
                    }
                }
            }
            o => panic!("expected array, got {o:?}"),
        }
    }

    #[test]
    fn list_invalid_flag() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 3);
        assert!(err(run!(&mut db, b"TOPK.LIST", b"tk", b"INVALID")).contains("syntax error"));
    }

    #[test]
    fn list_trailing_args() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 3);
        let r = run!(&mut db, b"TOPK.LIST", b"tk", b"WITHCOUNT", b"extra");
        assert!(err(r).contains("syntax error"));
    }

    #[test]
    fn info_default_params() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        match run!(&mut db, b"TOPK.INFO", b"tk").into_resp_value() {
            RespValue::Array(a) => {
                assert_eq!(a[1], RespValue::Integer(5));
                assert_eq!(a[3], RespValue::Integer(8));
                assert_eq!(a[5], RespValue::Integer(7));
                assert_eq!(a[7], RespValue::Bulk(b"0.9".to_vec()));
            }
            o => panic!("expected info array, got {o:?}"),
        }
    }

    #[test]
    fn info_custom_params() {
        let mut db = DbSlice::new(0);
        reserve_custom(&mut db, "tk", 20, 200, 10, 0.75);
        match run!(&mut db, b"TOPK.INFO", b"tk").into_resp_value() {
            RespValue::Array(a) => {
                assert_eq!(a[1], RespValue::Integer(20));
                assert_eq!(a[3], RespValue::Integer(200));
                assert_eq!(a[5], RespValue::Integer(10));
                assert_eq!(a[7], RespValue::Bulk(b"0.75".to_vec()));
            }
            o => panic!("expected info array, got {o:?}"),
        }
    }

    #[test]
    fn info_trailing_args() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        assert!(
            err(run!(&mut db, b"TOPK.INFO", b"tk", b"extra")).contains("wrong number of arguments")
        );
    }

    #[test]
    fn info_response_format() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        match run!(&mut db, b"TOPK.INFO", b"tk").into_resp_value() {
            RespValue::Array(a) => {
                assert_eq!(a.len(), 8);
                assert_eq!(a[0], RespValue::Bulk(b"k".to_vec()));
                assert_eq!(a[2], RespValue::Bulk(b"width".to_vec()));
                assert_eq!(a[4], RespValue::Bulk(b"depth".to_vec()));
                assert_eq!(a[6], RespValue::Bulk(b"decay".to_vec()));
            }
            o => panic!("expected info array, got {o:?}"),
        }
    }

    #[test]
    fn frequency_accuracy() {
        let mut db = DbSlice::new(0);
        reserve_custom(&mut db, "tk", 3, 50, 7, 0.9);
        let _ = incr_by_item(&mut db, "tk", "alpha", 50000);
        let _ = incr_by_item(&mut db, "tk", "beta", 30000);
        let _ = incr_by_item(&mut db, "tk", "gamma", 20000);
        for i in 0..50 {
            let _ = add_item(&mut db, "tk", &format!("noise{i}"));
        }
        assert_eq!(ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"alpha")), vec![1]);
        assert_eq!(ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"beta")), vec![1]);
        assert_eq!(ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"gamma")), vec![1]);
    }

    #[test]
    fn multiple_keys_isolation() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk1", 3);
        reserve_default(&mut db, "tk2", 5);
        let _ = add_item(&mut db, "tk1", "onlyin1");
        let _ = add_item(&mut db, "tk2", "onlyin2");

        assert_eq!(
            ints(run!(&mut db, b"TOPK.QUERY", b"tk1", b"onlyin1")),
            vec![1]
        );
        assert_eq!(
            ints(run!(&mut db, b"TOPK.QUERY", b"tk1", b"onlyin2")),
            vec![0]
        );
        assert_eq!(
            ints(run!(&mut db, b"TOPK.QUERY", b"tk2", b"onlyin2")),
            vec![1]
        );
        assert_eq!(
            ints(run!(&mut db, b"TOPK.QUERY", b"tk2", b"onlyin1")),
            vec![0]
        );

        match run!(&mut db, b"TOPK.INFO", b"tk1").into_resp_value() {
            RespValue::Array(a) => assert_eq!(a[1], RespValue::Integer(3)),
            o => panic!("expected info array, got {o:?}"),
        }
        match run!(&mut db, b"TOPK.INFO", b"tk2").into_resp_value() {
            RespValue::Array(a) => assert_eq!(a[1], RespValue::Integer(5)),
            o => panic!("expected info array, got {o:?}"),
        }
    }

    #[test]
    fn add_and_incr_by_interaction() {
        let mut db = DbSlice::new(0);
        reserve_custom(&mut db, "tk", 5, 100, 7, 0.9);
        let _ = add_item(&mut db, "tk", "foo");
        assert_eq!(ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"foo")), vec![1]);
        let _ = incr_by_item(&mut db, "tk", "foo", 100);
        assert_eq!(ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"foo")), vec![1]);
        let _ = add_item(&mut db, "tk", "bar");
        let _ = incr_by_item(&mut db, "tk", "bar", 50);
        assert_eq!(ints(run!(&mut db, b"TOPK.QUERY", b"tk", b"bar")), vec![1]);
    }

    #[test]
    fn high_contention_equal_counts() {
        let mut db = DbSlice::new(0);
        reserve_default(&mut db, "tk", 5);
        for i in 0..20 {
            let _ = incr_by_item(&mut db, "tk", &format!("item{i}"), 10);
        }
        match run!(&mut db, b"TOPK.LIST", b"tk").into_resp_value() {
            RespValue::Array(a) => assert_eq!(a.len(), 5),
            o => panic!("expected array, got {o:?}"),
        }
    }
}
