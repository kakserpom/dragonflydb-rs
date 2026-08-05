//! JSON commands (the `ReJSON` family), ported from
//! `dragonfly/src/server/json_family.cc` and
//! `dragonfly/src/server/detail/wrapped_json_path.h`.
//!
//! The wire protocol here is RESP2-only (matching the other command families in
//! this server): every legacy (`V1`) result is a single scalar/array value while
//! every V2 (enhanced, `$...`) result is an array of one element per match.
//!
//! Only the `json::Path` engine (`crate::core::jsonpath`) is used for
//! evaluation; the legacy jsoncons `JsonExpression` evaluator is not ported, but
//! legacy paths are rewritten to `$...` before parsing and share the same
//! traversal engine.

use crate::commands::{
    Command, FLAG_DENYOOM, FLAG_FAST, FLAG_READONLY, FLAG_WRITE, KeyRange, OpContext, bulk,
    integer, ok,
};
use crate::core::PrimeValue;
use crate::core::json::Json;
use crate::core::jsonpath::{self, Segment};
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::parse_double;
use crate::util::parse_i64;

const ERR_NO_SUCH_KEY: &str = "ERR no such key";
const ERR_INVALID_JSON: &str = "ERR failed to parse JSON";
const ERR_INVALID_JSON_PATH: &str = "ERR invalid JSON path";
const ERR_WRONG_JSON_TYPE: &str = "WRONGTYPE wrong JSON type of path value";
const ERR_SYNTAX: &str = "ERR syntax error";
const ERR_OUT_OF_RANGE: &str = "ERR index out of range";
const ERR_RESULT_NOT_NUMBER: &str = "ERR result is not a number";
const ERR_WRONG_TYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

fn e(msg: &str) -> RespError {
    RespError::new(msg)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SavingOrder {
    SaveFirst,
    SaveLast,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OnEmpty {
    SendNil,
    SendWrongType,
}

/// A wrapped, parsed JSON path together with its path mode.
struct WrappedJsonPath {
    path: jsonpath::Path,
    raw: String,
    legacy: bool,
}

impl WrappedJsonPath {
    fn is_legacy(&self) -> bool {
        self.legacy
    }

    fn refers_to_root(&self) -> bool {
        self.raw.is_empty() || self.raw == "." || self.raw == "$"
    }
}

/// Parse a path the way `ParseJsonPath` does: `$...` is V2, everything else is
/// legacy and rewritten to a `$...` path. Returns `None` on a syntax error.
fn parse_json_path(raw: &str) -> Option<WrappedJsonPath> {
    if !raw.is_empty() && raw.starts_with('$') {
        let path = jsonpath::parse_path(raw).ok()?;
        Some(WrappedJsonPath {
            path,
            raw: raw.to_string(),
            legacy: false,
        })
    } else {
        let v2: String = if raw.is_empty() || raw == "." {
            "$".to_string()
        } else {
            let first = raw.bytes().next().unwrap();
            let sep = if first == b'.' || first == b'[' {
                ""
            } else {
                "."
            };
            format!("${sep}{raw}")
        };
        let path = jsonpath::parse_path(&v2).ok()?;
        Some(WrappedJsonPath {
            path,
            raw: raw.to_string(),
            legacy: true,
        })
    }
}

/// The accumulated result of evaluating a path against a document, mirroring the
/// reference `JsonCallbackResult<T>`.
struct JsonCallbackResult<T> {
    result: Vec<Option<T>>,
    legacy: bool,
    on_empty: OnEmpty,
    saving_order: SavingOrder,
    none_capable: bool,
}

impl<T> JsonCallbackResult<T> {
    fn new(legacy: bool, on_empty: OnEmpty, saving_order: SavingOrder, none_capable: bool) -> Self {
        JsonCallbackResult {
            result: Vec::new(),
            legacy,
            on_empty,
            saving_order,
            none_capable,
        }
    }

    /// Add a (possibly undefined) match result.
    fn add_opt(&mut self, value: Option<T>) {
        if !self.legacy {
            self.result.push(value);
            return;
        }
        match self.saving_order {
            SavingOrder::SaveFirst => {
                if self.result.is_empty() {
                    self.result.push(value);
                } else if self.result[0].is_none() {
                    self.result[0] = value;
                }
            }
            SavingOrder::SaveLast => {
                if self.result.is_empty() {
                    self.result.push(value);
                } else {
                    self.result[0] = value;
                }
            }
        }
    }

    fn add_defined(&mut self, value: T) {
        self.add_opt(Some(value));
    }

    /// For mutate operations: a match that produced no value. Only V2 records a
    /// `None` placeholder; legacy skips it entirely (mirrors the reference).
    fn add_empty(&mut self) {
        if !self.legacy {
            self.result.push(None);
        }
    }

    fn should_send_nil(&self) -> bool {
        self.legacy && self.on_empty == OnEmpty::SendNil && self.result.is_empty()
    }

    fn should_send_wrong_type(&self) -> bool {
        if !self.legacy {
            return false;
        }
        if self.result.is_empty() && self.on_empty == OnEmpty::SendWrongType {
            return true;
        }
        if self.none_capable && self.result[0].is_none() {
            return true;
        }
        false
    }
}

/// Render a `JsonCallbackResult` as a reply, using the given element converter.
/// Elements are `(type-checked value or None)`; the converter maps each to a
/// `RespValue`.
fn send_legacy_v2<T>(
    c: &JsonCallbackResult<T>,
    mut elem: impl FnMut(Option<&T>) -> RespValue,
) -> CmdResult {
    if c.should_send_nil() {
        return CmdResult::Ok(RespValue::Nil);
    }
    if c.should_send_wrong_type() {
        return CmdResult::Err(e(ERR_WRONG_JSON_TYPE));
    }
    if c.legacy {
        CmdResult::Ok(elem(c.result[0].as_ref()))
    } else {
        CmdResult::Ok(RespValue::Array(
            c.result.iter().map(Option::as_ref).map(elem).collect(),
        ))
    }
}

fn elem_size(v: Option<&usize>) -> RespValue {
    match v {
        Some(n) => RespValue::Integer(*n as i64),
        None => RespValue::Nil,
    }
}

fn elem_i64(v: Option<&i64>) -> RespValue {
    match v {
        Some(i) => RespValue::Integer(*i),
        None => RespValue::Nil,
    }
}

fn elem_str(v: Option<&String>) -> RespValue {
    match v {
        Some(s) => RespValue::Bulk(s.as_bytes().to_vec()),
        None => RespValue::Nil,
    }
}

/// Fetch the mutable JSON value for `key`, or the appropriate error.
fn json_mut<'a>(
    db: &'a mut crate::core::DbSlice,
    key: &[u8],
    now: u64,
) -> Result<&'a mut Json, RespError> {
    match db.find_mut(key, now) {
        Some(PrimeValue::Json(j)) => Ok(j),
        Some(_) => Err(e(ERR_WRONG_TYPE)),
        None => Err(e(ERR_NO_SUCH_KEY)),
    }
}

/// Normalize a negative array index (mirrors `NormalizeNegativeIndex`).
fn normalize_neg(index: i64, size: usize) -> usize {
    if index >= 0 {
        index as usize
    } else if index.unsigned_abs() > size as u64 {
        0
    } else {
        size - index.unsigned_abs() as usize
    }
}

/// Strict type-aware JSON equality (mirrors `JsonAreEquals`).
fn json_are_equals(a: &Json, b: &Json) -> bool {
    match (a, b) {
        (Json::Null, Json::Null) => true,
        (Json::Bool(x), Json::Bool(y)) => x == y,
        (Json::Int(x), Json::Int(y)) => x == y,
        (Json::Uint(x), Json::Uint(y)) => x == y,
        (Json::Double(x), Json::Double(y)) => x == y,
        (Json::String(x), Json::String(y)) => x == y,
        (Json::Array(x), Json::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(a, b)| json_are_equals(a, b))
        }
        (Json::Object(x), Json::Object(y)) => {
            x.len() == y.len()
                && x.iter().all(|(k, v)| {
                    y.iter()
                        .find(|(kk, _)| kk == k)
                        .is_some_and(|(_, vv)| json_are_equals(v, vv))
                })
        }
        _ => false,
    }
}

/// The `JSON.TYPE` name for a value (distinguishes integer from number).
fn json_type_name(v: &Json) -> &'static str {
    match v {
        Json::Null => "null",
        Json::Bool(_) => "boolean",
        Json::Int(_) | Json::Uint(_) => "integer",
        Json::Double(_) => "number",
        Json::String(_) => "string",
        Json::Array(_) => "array",
        Json::Object(_) => "object",
    }
}

fn count_json_fields(v: &Json) -> usize {
    match v {
        Json::Array(items) => {
            let mut res = items.len();
            for it in items {
                if it.is_array() || it.is_object() {
                    res += count_json_fields(it);
                }
            }
            res
        }
        Json::Object(members) => {
            let mut res = members.len();
            for (_, v) in members {
                if v.is_array() || v.is_object() {
                    res += count_json_fields(v);
                }
            }
            res
        }
        _ => 1,
    }
}

/// Encode a JSON value into the RESP structure used by `JSON.RESP` (RESP2
/// branch: objects/arrays are prefixed with a `{` / `[` marker).
fn json_to_resp(v: &Json) -> RespValue {
    match v {
        Json::Null => RespValue::Nil,
        Json::Bool(b) => RespValue::Simple(if *b { "true" } else { "false" }.to_string()),
        Json::Int(i) => RespValue::Integer(*i),
        Json::Uint(u) => RespValue::Integer(*u as i64),
        Json::Double(d) => RespValue::Double(*d),
        Json::String(s) => RespValue::Bulk(s.as_bytes().to_vec()),
        Json::Array(items) => {
            let mut out = Vec::with_capacity(items.len() + 1);
            out.push(RespValue::Simple("[".to_string()));
            out.extend(items.iter().map(json_to_resp));
            RespValue::Array(out)
        }
        Json::Object(members) => {
            let mut out = Vec::with_capacity(members.len() * 2 + 1);
            out.push(RespValue::Simple("{".to_string()));
            for (k, v) in members {
                out.push(RespValue::Array(vec![
                    RespValue::Bulk(k.as_bytes().to_vec()),
                    json_to_resp(v),
                ]));
            }
            RespValue::Array(out)
        }
    }
}

// ---------------------------------------------------------------------------
// JSON.SET / JSON.MSET
// ---------------------------------------------------------------------------

/// Apply an RFC 6901 JSON Pointer `add` for a path made only of identifiers
/// and single indexes. Mirrors `jsoncons::jsonpointer::add`, creating missing
/// intermediate objects. Returns `Err(())` for unsupported paths.
fn json_pointer_add(root: &mut Json, path: &[Segment], value: Json) -> Result<(), ()> {
    if path.is_empty() {
        *root = value;
        return Ok(());
    }
    let mut cur = root;
    for (i, seg) in path.iter().enumerate() {
        let last = i + 1 == path.len();
        match seg {
            Segment::Identifier(key) => {
                if !cur.is_object() {
                    return Err(());
                }
                if last {
                    cur.object_insert(key.clone(), value.clone());
                } else if !cur.object_contains_key(key) {
                    cur.object_insert(key.clone(), Json::Object(Vec::new()));
                    cur = cur.object_get_mut(key).expect("just inserted");
                } else {
                    cur = cur.object_get_mut(key).expect("exists");
                }
            }
            Segment::Index(expr) => {
                if expr.first != expr.second || expr.first < 0 {
                    return Err(());
                }
                let idx = expr.first as usize;
                if !cur.is_array() {
                    return Err(());
                }
                if last {
                    match cur {
                        Json::Array(items) => {
                            if idx <= items.len() {
                                if idx == items.len() {
                                    items.push(value.clone());
                                } else {
                                    items[idx] = value.clone();
                                }
                            } else {
                                return Err(());
                            }
                        }
                        _ => unreachable!(),
                    }
                } else {
                    match cur {
                        Json::Array(items) => {
                            if idx > items.len() {
                                return Err(());
                            }
                            if idx == items.len() {
                                items.push(Json::Null);
                            }
                        }
                        _ => unreachable!(),
                    }
                    cur = match cur {
                        Json::Array(items) => &mut items[idx],
                        _ => unreachable!(),
                    };
                }
            }
            _ => return Err(()),
        }
    }
    Ok(())
}

/// Set a full JSON value for the (root-referring) path, mirroring `SetFullJson`
/// plus the NX/XX handling done in `OpSet`.
fn op_set_root(
    db: &mut crate::core::DbSlice,
    key: &[u8],
    value: &[u8],
    nx: bool,
    xx: bool,
    now: u64,
) -> CmdResult {
    if nx || xx {
        let exists = db.find(key, now).is_some();
        if (nx && exists) || (xx && !exists) {
            return CmdResult::Ok(RespValue::Nil);
        }
    }
    match db.find(key, now) {
        Some(pv) if !matches!(pv, PrimeValue::Json(_) | PrimeValue::Str(_)) => {
            return CmdResult::Err(e(ERR_WRONG_TYPE));
        }
        _ => {}
    }
    let Ok(parsed) = Json::parse(value) else {
        return CmdResult::Err(e(ERR_INVALID_JSON));
    };
    db.clear_expiry(key);
    db.insert(key, PrimeValue::Json(parsed));
    CmdResult::Ok(ok())
}

/// Set a partial JSON value, mirroring `SetPartialJson`.
fn op_set_partial(
    db: &mut crate::core::DbSlice,
    key: &[u8],
    w: &WrappedJsonPath,
    value: &[u8],
    nx: bool,
    xx: bool,
    now: u64,
) -> CmdResult {
    let Ok(parsed) = Json::parse(value) else {
        return CmdResult::Err(e(ERR_INVALID_JSON));
    };
    let json = match json_mut(db, key, now) {
        Ok(j) => j,
        Err(err) => return CmdResult::Err(err),
    };

    let mut path_exists = false;
    // The callback must return the replacement value for the matched node. We
    // mutate it in place (matching the reference `*val = parsed`) and return a
    // clone; the node is then overwritten with an identical copy.
    jsonpath::mutate_path(&w.path, json, |_, val| {
        path_exists = true;
        if !nx {
            *val = parsed.clone();
        }
        val.clone()
    });

    if !path_exists && !xx {
        if json_pointer_add(json, &w.path, parsed).is_err() {
            return CmdResult::Err(e(ERR_SYNTAX));
        }
        CmdResult::Ok(ok())
    } else if path_exists && !nx {
        CmdResult::Ok(ok())
    } else {
        CmdResult::Ok(RespValue::Nil)
    }
}

fn exec_set(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.owned_keys[0]];
    let path = std::str::from_utf8(&ctx.args[ctx.first_key_idx + 1]).unwrap_or("");
    let value = ctx
        .args
        .get(ctx.first_key_idx + 2)
        .cloned()
        .unwrap_or_default();
    let mut nx = false;
    let mut xx = false;
    let mut i = ctx.first_key_idx + 3;
    while i < ctx.args.len() {
        match ctx.args[i].to_ascii_uppercase().as_slice() {
            b"NX" => nx = true,
            b"XX" => xx = true,
            _ => return CmdResult::Err(e(ERR_SYNTAX)),
        }
        i += 1;
    }
    if nx && xx {
        return CmdResult::Err(e(ERR_SYNTAX));
    }

    let Some(w) = parse_json_path(path) else {
        return CmdResult::Err(e(ERR_SYNTAX));
    };

    if w.refers_to_root() {
        op_set_root(ctx.db, key, &value, nx, xx, ctx.now_ms)
    } else {
        op_set_partial(ctx.db, key, &w, &value, nx, xx, ctx.now_ms)
    }
}

fn exec_mset(ctx: &mut OpContext) -> CmdResult {
    let data = &ctx.args[ctx.first_key_idx..];
    if !data.len().is_multiple_of(3) {
        return CmdResult::Err(e("ERR wrong number of arguments for 'json.mset' command"));
    }
    let mut i = 0;
    while i < data.len() {
        let key = &data[i];
        let path = std::str::from_utf8(&data[i + 1]).unwrap_or("");
        let value = &data[i + 2];
        let Some(w) = parse_json_path(path) else {
            return CmdResult::Err(e(ERR_SYNTAX));
        };
        let r = if w.refers_to_root() {
            op_set_root(ctx.db, key, value, false, false, ctx.now_ms)
        } else {
            op_set_partial(ctx.db, key, &w, value, false, false, ctx.now_ms)
        };
        if r.is_err() {
            return r;
        }
        i += 3;
    }
    CmdResult::Ok(ok())
}

// ---------------------------------------------------------------------------
// JSON.GET / JSON.MGET
// ---------------------------------------------------------------------------

struct JsonGetParams {
    indent: Option<String>,
    newline: Option<String>,
    space: Option<String>,
    paths: Vec<(String, WrappedJsonPath)>,
}

fn parse_get_params(args: &[Vec<u8>], start: usize) -> Result<JsonGetParams, RespError> {
    let mut p = JsonGetParams {
        indent: None,
        newline: None,
        space: None,
        paths: Vec::new(),
    };
    let mut i = start;
    while i < args.len() {
        let a = std::str::from_utf8(&args[i]).unwrap_or("");
        match a.to_ascii_uppercase().as_str() {
            "NOESCAPE" => {}
            "INDENT" | "NEWLINE" | "SPACE" => {
                if i + 1 >= args.len() {
                    return Err(e(ERR_SYNTAX));
                }
                let v = std::str::from_utf8(&args[i + 1]).unwrap_or("").to_string();
                match a.to_ascii_uppercase().as_str() {
                    "INDENT" => p.indent = Some(v),
                    "NEWLINE" => p.newline = Some(v),
                    _ => p.space = Some(v),
                }
                i += 1;
            }
            _ => {
                let w = parse_json_path(a).ok_or_else(|| e(ERR_SYNTAX))?;
                p.paths.push((a.to_string(), w));
            }
        }
        i += 1;
    }
    Ok(p)
}

/// Evaluate a wrapped path against `json`, producing the result Json: for legacy
/// a single value, for V2 an array of all matches.
fn eval_wrapped(w: &WrappedJsonPath, json: &Json) -> Option<Json> {
    let mut c = JsonCallbackResult::new(
        w.legacy,
        OnEmpty::SendWrongType,
        SavingOrder::SaveLast,
        false,
    );
    jsonpath::eval_path(&w.path, json, |_, v| c.add_defined(v.clone()));
    if w.legacy {
        if c.result.is_empty() {
            None
        } else {
            Some(c.result[0].as_ref().expect("legacy defined").clone())
        }
    } else {
        Some(Json::Array(
            c.result.into_iter().map(|v| v.expect("defined")).collect(),
        ))
    }
}

fn get_str_for_key(db: &mut crate::core::DbSlice, key: &[u8], now: u64) -> Result<Json, RespError> {
    match db.find(key, now) {
        Some(PrimeValue::Json(j)) => Ok(j.clone()),
        Some(PrimeValue::Str(s)) => Json::parse(s.as_bytes()).map_err(|_| e(ERR_WRONG_TYPE)),
        Some(_) => Err(e(ERR_WRONG_TYPE)),
        None => Err(e(ERR_NO_SUCH_KEY)),
    }
}

fn exec_get(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let params = match parse_get_params(ctx.args, ctx.first_key_idx + 1) {
        Ok(p) => p,
        Err(err) => return CmdResult::Err(err),
    };

    let json = match get_str_for_key(ctx.db, key, ctx.now_ms) {
        Ok(j) => j,
        Err(e) if e.render() == ERR_NO_SUCH_KEY => return CmdResult::Ok(RespValue::Nil),
        Err(e) => return CmdResult::Err(e),
    };

    if params.paths.is_empty() {
        return CmdResult::Ok(bulk(json.dump()));
    }

    let legacy_all = params.paths.iter().all(|(_, w)| w.is_legacy());
    let indent = params.indent.as_deref().unwrap_or("");
    let newline = params.newline.as_deref().unwrap_or("");
    let space = params.space.as_deref().unwrap_or("");

    if legacy_all {
        // Legacy mode: single path yields one value; multiple paths yield an object.
        if params.paths.len() == 1 {
            match eval_wrapped(&params.paths[0].1, &json) {
                Some(v) => CmdResult::Ok(bulk(v.dump_with_options(indent, newline, space))),
                None => CmdResult::Err(e(ERR_INVALID_JSON_PATH)),
            }
        } else {
            let mut out = Json::Object(Vec::new());
            for (raw, w) in &params.paths {
                match eval_wrapped(w, &json) {
                    Some(v) => {
                        out.object_insert(raw.clone(), v);
                    }
                    None => return CmdResult::Err(e(ERR_INVALID_JSON_PATH)),
                }
            }
            CmdResult::Ok(bulk(out.dump_with_options(indent, newline, space)))
        }
    } else {
        if params.paths.len() == 1 {
            let out = eval_wrapped(&params.paths[0].1, &json).expect("V2 always produces");
            CmdResult::Ok(bulk(out.dump_with_options(indent, newline, space)))
        } else {
            let mut out = Json::Object(Vec::new());
            for (raw, w) in &params.paths {
                out.object_insert(
                    raw.clone(),
                    eval_wrapped(w, &json).expect("V2 always produces"),
                );
            }
            CmdResult::Ok(bulk(out.dump_with_options(indent, newline, space)))
        }
    }
}

fn exec_mget(ctx: &mut OpContext) -> CmdResult {
    let path_str = std::str::from_utf8(ctx.args.last().unwrap()).unwrap_or("");
    let Some(w) = parse_json_path(path_str) else {
        return CmdResult::Err(e(ERR_SYNTAX));
    };

    let mut out = Vec::new();
    for key in &ctx.args[ctx.first_key_idx..ctx.args.len() - 1] {
        let Ok(json) = get_str_for_key(ctx.db, key, ctx.now_ms) else {
            out.push(RespValue::Nil);
            continue;
        };
        match eval_wrapped(&w, &json) {
            Some(v) => out.push(RespValue::Bulk(v.dump().into_bytes())),
            None => out.push(RespValue::Nil),
        }
    }
    CmdResult::Ok(RespValue::Array(out))
}

// ---------------------------------------------------------------------------
// Generic read-only operations (simple values)
// ---------------------------------------------------------------------------

fn exec_type(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = &ctx.args[ctx.first_key_idx + 1];
    let w = parse_json_path(std::str::from_utf8(path).unwrap_or("")).unwrap();
    // TYPE always returns nil on a missing key.
    let Some(pv) = ctx.db.find(key, ctx.now_ms) else {
        return CmdResult::Ok(RespValue::Nil);
    };
    let PrimeValue::Json(json) = pv else {
        return CmdResult::Err(e(ERR_WRONG_TYPE));
    };
    let mut c = JsonCallbackResult::new(w.legacy, OnEmpty::SendNil, SavingOrder::SaveLast, false);
    jsonpath::eval_path(&w.path, json, |_, v| c.add_defined(json_type_name(v)));
    send_legacy_v2(&c, |v| match v {
        Some(s) => RespValue::Bulk(s.as_bytes().to_vec()),
        None => RespValue::Nil,
    })
}

// ---------------------------------------------------------------------------
// JSON.DEL / JSON.FORGET
// ---------------------------------------------------------------------------

fn exec_del(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = ctx
        .args
        .get(ctx.first_key_idx + 1)
        .map_or("", |p| std::str::from_utf8(p).unwrap_or(""));
    let Some(w) = parse_json_path(path) else {
        return CmdResult::Err(e(ERR_SYNTAX));
    };

    if w.refers_to_root() {
        let is_json = matches!(ctx.db.find(key, ctx.now_ms), Some(PrimeValue::Json(_)));
        let exists = ctx.db.find(key, ctx.now_ms).is_some();
        if !exists {
            return CmdResult::Ok(integer(0));
        }
        if !is_json {
            return CmdResult::Err(e(ERR_WRONG_TYPE));
        }
        ctx.db.remove(key);
        return CmdResult::Ok(integer(1));
    }

    let Some(json) = (match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Json(j)) => Some(j),
        _ => None,
    }) else {
        return CmdResult::Ok(integer(0));
    };
    let n = jsonpath::delete_path(&w.path, json);
    CmdResult::Ok(integer(n as i64))
}

// ---------------------------------------------------------------------------
// Read-only size/key accessors
// ---------------------------------------------------------------------------

fn exec_strlen(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = ctx
        .args
        .get(ctx.first_key_idx + 1)
        .map_or("", |p| std::str::from_utf8(p).unwrap_or(""));
    let w = parse_json_path(path).unwrap();
    let Some(pv) = ctx.db.find(key, ctx.now_ms) else {
        return if w.is_legacy() {
            CmdResult::Ok(RespValue::Nil)
        } else {
            CmdResult::Err(e(ERR_NO_SUCH_KEY))
        };
    };
    let PrimeValue::Json(json) = pv else {
        return CmdResult::Err(e(ERR_WRONG_TYPE));
    };
    let mut c = JsonCallbackResult::new(w.legacy, OnEmpty::SendNil, SavingOrder::SaveFirst, true);
    jsonpath::eval_path(&w.path, json, |_, v| {
        c.add_opt(v.as_str().map(str::len));
    });
    send_legacy_v2(&c, elem_size)
}

fn exec_objlen(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = ctx
        .args
        .get(ctx.first_key_idx + 1)
        .map_or("", |p| std::str::from_utf8(p).unwrap_or(""));
    let w = parse_json_path(path).unwrap();
    let Some(pv) = ctx.db.find(key, ctx.now_ms) else {
        return if w.is_legacy() {
            CmdResult::Ok(RespValue::Nil)
        } else {
            CmdResult::Err(e(ERR_NO_SUCH_KEY))
        };
    };
    let PrimeValue::Json(json) = pv else {
        return CmdResult::Err(e(ERR_WRONG_TYPE));
    };
    let mut c = JsonCallbackResult::new(w.legacy, OnEmpty::SendNil, SavingOrder::SaveFirst, true);
    jsonpath::eval_path(&w.path, json, |_, v| {
        c.add_opt(if v.is_object() { Some(v.len()) } else { None });
    });
    send_legacy_v2(&c, elem_size)
}

fn exec_arrlen(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = ctx
        .args
        .get(ctx.first_key_idx + 1)
        .map_or("", |p| std::str::from_utf8(p).unwrap_or(""));
    let w = parse_json_path(path).unwrap();
    let Some(pv) = ctx.db.find(key, ctx.now_ms) else {
        return CmdResult::Ok(RespValue::Nil);
    };
    let PrimeValue::Json(json) = pv else {
        return CmdResult::Err(e(ERR_WRONG_TYPE));
    };
    let mut c = JsonCallbackResult::new(w.legacy, OnEmpty::SendNil, SavingOrder::SaveFirst, true);
    jsonpath::eval_path(&w.path, json, |_, v| {
        c.add_opt(if v.is_array() { Some(v.len()) } else { None });
    });
    send_legacy_v2(&c, elem_size)
}

fn exec_objkeys(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = ctx
        .args
        .get(ctx.first_key_idx + 1)
        .map_or("", |p| std::str::from_utf8(p).unwrap_or(""));
    let w = parse_json_path(path).unwrap();
    let Some(pv) = ctx.db.find(key, ctx.now_ms) else {
        return if w.is_legacy() {
            CmdResult::Ok(RespValue::Nil)
        } else {
            CmdResult::Err(e(ERR_NO_SUCH_KEY))
        };
    };
    let PrimeValue::Json(json) = pv else {
        return CmdResult::Err(e(ERR_WRONG_TYPE));
    };
    let mut c = JsonCallbackResult::new(w.legacy, OnEmpty::SendNil, SavingOrder::SaveFirst, false);
    jsonpath::eval_path(&w.path, json, |_, v| {
        let keys = if v.is_object() {
            v.object_members().iter().map(|(k, _)| k.clone()).collect()
        } else {
            Vec::new()
        };
        c.add_defined(keys);
    });
    send_legacy_v2(&c, |v| match v {
        Some(keys) => RespValue::Array(
            keys.iter()
                .map(|k| RespValue::Bulk(k.as_bytes().to_vec()))
                .collect(),
        ),
        None => RespValue::Nil,
    })
}

// ---------------------------------------------------------------------------
// Mutating string / container operations
// ---------------------------------------------------------------------------

fn exec_strappend(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let rest = &ctx.args[ctx.first_key_idx + 1..];
    let (path_str, value_bytes) = if rest.len() >= 2 {
        (std::str::from_utf8(&rest[0]).unwrap_or(""), rest[1].clone())
    } else if rest.len() == 1 {
        ("", rest[0].clone())
    } else {
        return CmdResult::Err(e(ERR_SYNTAX));
    };
    if ctx.args.len() > ctx.first_key_idx + 1 + rest.len() {
        return CmdResult::Err(e(ERR_SYNTAX));
    }
    let Ok(Json::String(parsed)) = Json::parse(&value_bytes) else {
        return CmdResult::Err(e("expected string value"));
    };

    let w = parse_json_path(path_str).unwrap();
    let json = match json_mut(ctx.db, key, ctx.now_ms) {
        Ok(j) => j,
        Err(err) => return CmdResult::Err(err),
    };
    let mut c = JsonCallbackResult::new(
        w.legacy,
        OnEmpty::SendWrongType,
        SavingOrder::SaveLast,
        true,
    );
    jsonpath::mutate_path(&w.path, json, |_, val| {
        if let Some(s) = val.as_str() {
            let newv = format!("{s}{parsed}");
            *val = Json::String(newv.clone());
            c.add_defined(newv.len());
        } else if !w.legacy {
            c.add_empty();
        }
        val.clone()
    });
    send_legacy_v2(&c, elem_size)
}

fn exec_toggle(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = std::str::from_utf8(&ctx.args[ctx.first_key_idx + 1]).unwrap_or("");
    let Some(w) = parse_json_path(path) else {
        return CmdResult::Err(e(ERR_SYNTAX));
    };
    let json = match json_mut(ctx.db, key, ctx.now_ms) {
        Ok(j) => j,
        Err(err) => return CmdResult::Err(err),
    };
    if w.is_legacy() {
        let mut c =
            JsonCallbackResult::new(true, OnEmpty::SendWrongType, SavingOrder::SaveLast, true);
        let legacy = true;
        jsonpath::mutate_path(&w.path, json, |_, val| {
            if let Some(b) = val.as_bool() {
                let nv = !b;
                *val = Json::Bool(nv);
                c.add_defined(nv);
            } else if !legacy {
                c.add_empty();
            }
            val.clone()
        });
        send_legacy_v2(&c, |v| match v {
            Some(b) => RespValue::Simple(if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }),
            None => RespValue::Nil,
        })
    } else {
        let mut c =
            JsonCallbackResult::new(false, OnEmpty::SendWrongType, SavingOrder::SaveLast, true);
        let legacy = false;
        jsonpath::mutate_path(&w.path, json, |_, val| {
            if let Some(b) = val.as_bool() {
                let nv = !b;
                *val = Json::Bool(nv);
                c.add_defined(i64::from(nv));
            } else if !legacy {
                c.add_empty();
            }
            val.clone()
        });
        send_legacy_v2(&c, |v| match v {
            Some(i) => RespValue::Integer(*i),
            None => RespValue::Nil,
        })
    }
}

fn exec_clear(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = ctx
        .args
        .get(ctx.first_key_idx + 1)
        .map_or("", |p| std::str::from_utf8(p).unwrap_or(""));
    let w = parse_json_path(path).unwrap();
    let json = match json_mut(ctx.db, key, ctx.now_ms) {
        Ok(j) => j,
        Err(err) => return CmdResult::Err(err),
    };
    let mut clear_items = 0usize;
    jsonpath::mutate_path(&w.path, json, |_, val| {
        if val.is_object() || val.is_array() {
            if let Json::Object(m) = val {
                m.clear();
            } else if let Json::Array(a) = val {
                a.clear();
            }
            clear_items += 1;
        } else if val.is_number() {
            *val = Json::Int(0);
            clear_items += 1;
        }
        val.clone()
    });
    CmdResult::Ok(integer(clear_items as i64))
}

// ---------------------------------------------------------------------------
// Array operations
// ---------------------------------------------------------------------------

fn exec_arrpop(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = ctx
        .args
        .get(ctx.first_key_idx + 1)
        .map_or("", |p| std::str::from_utf8(p).unwrap_or(""));
    let index_str = ctx
        .args
        .get(ctx.first_key_idx + 2)
        .map_or("-1", |s| std::str::from_utf8(s).unwrap_or(""));
    let Some(index) = parse_i64(index_str.as_bytes()) else {
        return CmdResult::Err(e("ERR value is not an integer or out of range"));
    };
    let Some(w) = parse_json_path(path) else {
        return CmdResult::Err(e(ERR_SYNTAX));
    };
    let json = match json_mut(ctx.db, key, ctx.now_ms) {
        Ok(j) => j,
        Err(err) => return CmdResult::Err(err),
    };
    let mut c = JsonCallbackResult::new(w.legacy, OnEmpty::SendNil, SavingOrder::SaveLast, true);
    jsonpath::mutate_path(&w.path, json, |_, val| {
        if let Json::Array(items) = val {
            if !items.is_empty() {
                let size = items.len();
                let removal = normalize_neg(index, size).min(size - 1);
                let element = items.remove(removal);
                let dumped = element.dump();
                c.add_defined(dumped);
            } else if !w.legacy {
                c.add_empty();
            }
        } else {
            c.add_empty();
        }
        val.clone()
    });
    send_legacy_v2(&c, elem_str)
}

fn exec_arrtrim(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = std::str::from_utf8(&ctx.args[ctx.first_key_idx + 1]).unwrap_or("");
    let (Some(start), Some(stop)) = (
        parse_i64(&ctx.args[ctx.first_key_idx + 2]),
        parse_i64(&ctx.args[ctx.first_key_idx + 3]),
    ) else {
        return CmdResult::Err(RespError::integer());
    };
    let Some(w) = parse_json_path(path) else {
        return CmdResult::Err(e(ERR_SYNTAX));
    };
    let json = match json_mut(ctx.db, key, ctx.now_ms) {
        Ok(j) => j,
        Err(err) => return CmdResult::Err(err),
    };
    let mut c = JsonCallbackResult::new(
        w.legacy,
        OnEmpty::SendWrongType,
        SavingOrder::SaveLast,
        true,
    );
    jsonpath::mutate_path(&w.path, json, |_, val| {
        if let Json::Array(items) = val {
            if items.is_empty() {
                c.add_defined(0);
            } else {
                let size = items.len();
                let ts = normalize_neg(start, size);
                let te = normalize_neg(stop, size);
                let new_len = if ts >= size || ts > te {
                    items.clear();
                    0
                } else {
                    let te = te.min(size - 1);
                    let keep: Vec<Json> = items[ts..=te].to_vec();
                    let len = keep.len();
                    *items = keep;
                    len
                };
                c.add_defined(new_len);
            }
        } else {
            c.add_empty();
        }
        val.clone()
    });
    send_legacy_v2(&c, elem_size)
}

fn exec_arrinsert(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = std::str::from_utf8(&ctx.args[ctx.first_key_idx + 1]).unwrap_or("");
    let Some(index) = parse_i64(&ctx.args[ctx.first_key_idx + 2]) else {
        return CmdResult::Err(RespError::integer());
    };
    let Some(w) = parse_json_path(path) else {
        return CmdResult::Err(e(ERR_SYNTAX));
    };
    let mut parsed_values = Vec::new();
    for v in &ctx.args[ctx.first_key_idx + 3..] {
        match Json::parse(v) {
            Ok(j) => parsed_values.push(j),
            Err(_) => return CmdResult::Err(e(ERR_SYNTAX)),
        }
    }
    let json = match json_mut(ctx.db, key, ctx.now_ms) {
        Ok(j) => j,
        Err(err) => return CmdResult::Err(err),
    };
    let mut c = JsonCallbackResult::new(
        w.legacy,
        OnEmpty::SendWrongType,
        SavingOrder::SaveLast,
        true,
    );
    let mut oob = false;
    jsonpath::mutate_path(&w.path, json, |_, val| {
        if oob {
            return val.clone();
        }
        if let Json::Array(items) = val {
            let size = items.len();
            let insert_before = if index < 0 {
                if index.unsigned_abs() > size as u64 {
                    oob = true;
                    return val.clone();
                }
                size - index.unsigned_abs() as usize
            } else {
                if index as usize > size {
                    oob = true;
                    return val.clone();
                }
                index as usize
            };
            for (offset, pv) in parsed_values.iter().enumerate() {
                items.insert(insert_before + offset, pv.clone());
            }
            c.add_defined(val.len());
        } else {
            c.add_empty();
        }
        val.clone()
    });
    if oob {
        return CmdResult::Err(e(ERR_OUT_OF_RANGE));
    }
    send_legacy_v2(&c, elem_size)
}

fn exec_arrappend(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = std::str::from_utf8(&ctx.args[ctx.first_key_idx + 1]).unwrap_or("");
    let Some(w) = parse_json_path(path) else {
        return CmdResult::Err(e(ERR_SYNTAX));
    };
    let mut parsed_values = Vec::new();
    for v in &ctx.args[ctx.first_key_idx + 2..] {
        match Json::parse(v) {
            Ok(j) => parsed_values.push(j),
            Err(_) => return CmdResult::Err(e(ERR_SYNTAX)),
        }
    }
    let json = match json_mut(ctx.db, key, ctx.now_ms) {
        Ok(j) => j,
        Err(err) => return CmdResult::Err(err),
    };
    let mut c = JsonCallbackResult::new(
        w.legacy,
        OnEmpty::SendWrongType,
        SavingOrder::SaveLast,
        true,
    );
    jsonpath::mutate_path(&w.path, json, |_, val| {
        if let Json::Array(items) = val {
            for pv in &parsed_values {
                items.push(pv.clone());
            }
            c.add_defined(val.len());
        } else {
            c.add_empty();
        }
        val.clone()
    });
    send_legacy_v2(&c, elem_size)
}

fn exec_arrindex(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = std::str::from_utf8(&ctx.args[ctx.first_key_idx + 1]).unwrap_or("");
    let search = &ctx.args[ctx.first_key_idx + 2];
    let start_idx = match ctx.args.get(ctx.first_key_idx + 3) {
        Some(s) => match parse_i64(s) {
            Some(i) => i,
            None => return CmdResult::Err(RespError::integer()),
        },
        None => 0,
    };
    let end_idx = match ctx.args.get(ctx.first_key_idx + 4) {
        Some(s) => match parse_i64(s) {
            Some(i) => i,
            None => return CmdResult::Err(RespError::integer()),
        },
        None => 0,
    };
    let Some(w) = parse_json_path(path) else {
        return CmdResult::Err(e(ERR_SYNTAX));
    };
    let Ok(search_json) = Json::parse(search) else {
        return CmdResult::Err(e(ERR_SYNTAX));
    };
    let Some(pv) = ctx.db.find(key, ctx.now_ms) else {
        return CmdResult::Err(e(ERR_NO_SUCH_KEY));
    };
    let PrimeValue::Json(json) = pv else {
        return CmdResult::Err(e(ERR_WRONG_TYPE));
    };
    let mut c = JsonCallbackResult::new(
        w.legacy,
        OnEmpty::SendWrongType,
        SavingOrder::SaveLast,
        true,
    );
    jsonpath::eval_path(&w.path, json, |_, v| {
        let idx = if !v.is_array() {
            None
        } else if v.is_empty() {
            Some(-1)
        } else {
            let size = v.len();
            if start_idx < 0 && start_idx.unsigned_abs() > size as u64 {
                Some(-1)
            } else {
                let pos_start = normalize_neg(start_idx, size);
                let pos_end = if end_idx == 0 {
                    size
                } else {
                    normalize_neg(end_idx, size)
                };
                if pos_start >= size && pos_end < size {
                    Some(-1)
                } else {
                    let pos_start = pos_start.min(size - 1);
                    let pos_end = pos_end.min(size - 1);
                    if pos_start > pos_end {
                        Some(-1)
                    } else {
                        let items = v.array_items();
                        let found = items
                            .iter()
                            .enumerate()
                            .skip(pos_start)
                            .take(pos_end + 1 - pos_start)
                            .find(|(_, it)| json_are_equals(&search_json, it))
                            .map_or(-1, |(i, _)| i as i64);
                        Some(found)
                    }
                }
            }
        };
        c.add_opt(idx);
    });
    send_legacy_v2(&c, elem_i64)
}

// ---------------------------------------------------------------------------
// Numerical increment / multiply
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Arith {
    Add,
    Multiply,
}

/// Apply a binary arithmetic operation to `val`, mirroring `BinOpApply`.
fn bin_op_apply(val: &Json, num: f64, num_is_double: bool, op: Arith) -> Result<Json, ()> {
    let cur = val.as_f64().ok_or(())?;
    let result = match op {
        Arith::Add => cur + num,
        Arith::Multiply => cur * num,
    };
    if result.is_infinite() {
        return Err(());
    }
    if val.is_double() || num_is_double {
        Ok(Json::Double(result))
    } else if result >= 0.0 {
        Ok(Json::Uint(result as u64))
    } else if result >= i64::MIN as f64 {
        Ok(Json::Int(result as i64))
    } else {
        Err(())
    }
}

fn op_arith(ctx: &mut OpContext, op: Arith) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = std::str::from_utf8(&ctx.args[ctx.first_key_idx + 1]).unwrap_or("");
    let num = &ctx.args[ctx.first_key_idx + 2];
    let Some(w) = parse_json_path(path) else {
        return CmdResult::Err(e(ERR_SYNTAX));
    };
    let num_str = std::str::from_utf8(num).unwrap_or("");
    let has_fractional_part = num_str.contains('.');
    let Some(double_value) = parse_double(num) else {
        return CmdResult::Err(e(ERR_WRONG_TYPE));
    };
    let json = match json_mut(ctx.db, key, ctx.now_ms) {
        Ok(j) => j,
        Err(err) => return CmdResult::Err(err),
    };

    let mut overflow = false;
    let legacy = w.legacy;
    let mut v2_values: Vec<Option<Json>> = Vec::new();
    let mut legacy_val: Option<Json> = None;
    jsonpath::mutate_path(&w.path, json, |_, val| {
        if val.is_number() {
            match bin_op_apply(val, double_value, has_fractional_part, op) {
                Ok(newv) => {
                    *val = newv.clone();
                    legacy_val = Some(newv.clone());
                    v2_values.push(Some(newv));
                }
                Err(()) => {
                    overflow = true;
                }
            }
        } else {
            v2_values.push(None);
        }
        val.clone()
    });

    if overflow {
        return CmdResult::Err(e(ERR_RESULT_NOT_NUMBER));
    }
    let result = if legacy {
        match legacy_val {
            Some(v) => v.dump(),
            None => return CmdResult::Err(e(ERR_WRONG_JSON_TYPE)),
        }
    } else {
        let arr = v2_values
            .into_iter()
            .map(|v| v.unwrap_or(Json::Null))
            .collect();
        Json::Array(arr).dump()
    };
    CmdResult::Ok(bulk(result))
}

// ---------------------------------------------------------------------------
// JSON.RESP / JSON.DEBUG / JSON.MERGE
// ---------------------------------------------------------------------------

fn exec_resp(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = ctx
        .args
        .get(ctx.first_key_idx + 1)
        .map_or("", |p| std::str::from_utf8(p).unwrap_or(""));
    let Some(w) = parse_json_path(path) else {
        return CmdResult::Err(e(ERR_SYNTAX));
    };
    let Some(pv) = ctx.db.find(key, ctx.now_ms) else {
        return CmdResult::Err(e(ERR_NO_SUCH_KEY));
    };
    let PrimeValue::Json(json) = pv else {
        return CmdResult::Err(e(ERR_WRONG_TYPE));
    };
    let mut c = JsonCallbackResult::new(w.legacy, OnEmpty::SendNil, SavingOrder::SaveLast, false);
    jsonpath::eval_path(&w.path, json, |_, v| c.add_defined(json_to_resp(v)));
    send_legacy_v2(&c, |v| match v {
        Some(r) => r.clone(),
        None => RespValue::Nil,
    })
}

fn exec_debug(ctx: &mut OpContext) -> CmdResult {
    let cmd = ctx
        .args
        .get(ctx.first_key_idx)
        .map(|s| std::str::from_utf8(s).unwrap_or("").to_ascii_lowercase())
        .unwrap_or_default();

    if cmd == "help" {
        return CmdResult::Ok(RespValue::Array(vec![
            RespValue::Bulk(b"JSON.DEBUG MEMORY <key> [path] - report memory size (bytes) of the JSON element. Path defaults to root if not provided.".to_vec()),
            RespValue::Bulk(b"JSON.DEBUG FIELDS <key> [path] - report number of fields in the JSON element. Path defaults to root if not provided.".to_vec()),
            RespValue::Bulk(b"JSON.DEBUG HELP - print help message.".to_vec()),
        ]));
    }

    if cmd == "fields" || cmd == "memory" {
        let Some(key_bytes) = ctx.args.get(ctx.first_key_idx + 1) else {
            return CmdResult::Err(e(ERR_SYNTAX));
        };
        let path = ctx
            .args
            .get(ctx.first_key_idx + 2)
            .map_or("", |p| std::str::from_utf8(p).unwrap_or(""));
        let Some(w) = parse_json_path(path) else {
            return CmdResult::Err(e(ERR_SYNTAX));
        };
        let Some(pv) = ctx.db.find(key_bytes, ctx.now_ms) else {
            return CmdResult::Err(e(ERR_NO_SUCH_KEY));
        };
        let PrimeValue::Json(json) = pv else {
            return CmdResult::Err(e(ERR_WRONG_TYPE));
        };
        let mut c =
            JsonCallbackResult::new(w.legacy, OnEmpty::SendNil, SavingOrder::SaveLast, true);
        jsonpath::eval_path(&w.path, json, |_, v| {
            let n = if cmd == "fields" {
                count_json_fields(v)
            } else {
                v.memory_usage()
            };
            c.add_defined(n);
        });
        return send_legacy_v2(&c, elem_size);
    }

    CmdResult::Err(e("ERR unknown subcommand for 'json.debug'"))
}

fn exec_merge(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[ctx.first_key_idx];
    let path = std::str::from_utf8(&ctx.args[ctx.first_key_idx + 1]).unwrap_or("");
    let patch_str = &ctx.args[ctx.first_key_idx + 2];
    let Some(w) = parse_json_path(path) else {
        return CmdResult::Err(e(ERR_SYNTAX));
    };
    let Ok(patch) = Json::parse(patch_str) else {
        return CmdResult::Err(e(ERR_INVALID_JSON));
    };

    match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::Json(json)) => {
            jsonpath::mutate_path(&w.path, json, |_, val| {
                val.apply_merge_patch(&patch);
                val.clone()
            });
            CmdResult::Ok(ok())
        }
        Some(_) => CmdResult::Err(e(ERR_WRONG_TYPE)),
        None => {
            if w.refers_to_root() {
                ctx.db.clear_expiry(key);
                ctx.db.insert(key, PrimeValue::Json(patch.clone()));
                CmdResult::Ok(ok())
            } else {
                CmdResult::Err(e(ERR_SYNTAX))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

pub static CMD_JSON_GET: Command = Command {
    name: "JSON.GET",
    arity: -2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_get,
    merge: None,
};

pub static CMD_JSON_MGET: Command = Command {
    name: "JSON.MGET",
    arity: -3,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange {
        first: 1,
        last: 0,
        step: 1,
    },
    exec: exec_mget,
    merge: None,
};

pub static CMD_JSON_TYPE: Command = Command {
    name: "JSON.TYPE",
    arity: -2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_type,
    merge: None,
};

pub static CMD_JSON_STRLEN: Command = Command {
    name: "JSON.STRLEN",
    arity: -2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_strlen,
    merge: None,
};

pub static CMD_JSON_OBJLEN: Command = Command {
    name: "JSON.OBJLEN",
    arity: -2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_objlen,
    merge: None,
};

pub static CMD_JSON_ARRLEN: Command = Command {
    name: "JSON.ARRLEN",
    arity: -2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_arrlen,
    merge: None,
};

pub static CMD_JSON_TOGGLE: Command = Command {
    name: "JSON.TOGGLE",
    arity: 3,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_toggle,
    merge: None,
};

pub static CMD_JSON_NUMINCRBY: Command = Command {
    name: "JSON.NUMINCRBY",
    arity: 4,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: op_arith_add,
    merge: None,
};

pub static CMD_JSON_NUMMULTBY: Command = Command {
    name: "JSON.NUMMULTBY",
    arity: 4,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: op_arith_mult,
    merge: None,
};

pub static CMD_JSON_DEL: Command = Command {
    name: "JSON.DEL",
    arity: -2,
    flags: FLAG_WRITE,
    key_range: KeyRange::ONE,
    exec: exec_del,
    merge: None,
};

pub static CMD_JSON_FORGET: Command = Command {
    name: "JSON.FORGET",
    arity: -2,
    flags: FLAG_WRITE,
    key_range: KeyRange::ONE,
    exec: exec_del,
    merge: None,
};

pub static CMD_JSON_OBJKEYS: Command = Command {
    name: "JSON.OBJKEYS",
    arity: -2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_objkeys,
    merge: None,
};

pub static CMD_JSON_STRAPPEND: Command = Command {
    name: "JSON.STRAPPEND",
    arity: -3,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_strappend,
    merge: None,
};

pub static CMD_JSON_CLEAR: Command = Command {
    name: "JSON.CLEAR",
    arity: -2,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_clear,
    merge: None,
};

pub static CMD_JSON_ARRPOP: Command = Command {
    name: "JSON.ARRPOP",
    arity: -2,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_arrpop,
    merge: None,
};

pub static CMD_JSON_ARRTRIM: Command = Command {
    name: "JSON.ARRTRIM",
    arity: 5,
    flags: FLAG_WRITE | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_arrtrim,
    merge: None,
};

pub static CMD_JSON_ARRINSERT: Command = Command {
    name: "JSON.ARRINSERT",
    arity: -5,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_arrinsert,
    merge: None,
};

pub static CMD_JSON_ARRAPPEND: Command = Command {
    name: "JSON.ARRAPPEND",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_arrappend,
    merge: None,
};

pub static CMD_JSON_ARRINDEX: Command = Command {
    name: "JSON.ARRINDEX",
    arity: -4,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_arrindex,
    merge: None,
};

pub static CMD_JSON_DEBUG: Command = Command {
    name: "JSON.DEBUG",
    arity: -2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::NONE,
    exec: exec_debug,
    merge: None,
};

pub static CMD_JSON_RESP: Command = Command {
    name: "JSON.RESP",
    arity: -2,
    flags: FLAG_READONLY | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_resp,
    merge: None,
};

pub static CMD_JSON_SET: Command = Command {
    name: "JSON.SET",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_set,
    merge: None,
};

pub static CMD_JSON_MSET: Command = Command {
    name: "JSON.MSET",
    arity: -4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange {
        first: 1,
        last: 0,
        step: 3,
    },
    exec: exec_mset,
    merge: None,
};

pub static CMD_JSON_MERGE: Command = Command {
    name: "JSON.MERGE",
    arity: 4,
    flags: FLAG_WRITE | FLAG_DENYOOM | FLAG_FAST,
    key_range: KeyRange::ONE,
    exec: exec_merge,
    merge: None,
};

fn op_arith_add(ctx: &mut OpContext) -> CmdResult {
    op_arith(ctx, Arith::Add)
}
fn op_arith_mult(ctx: &mut OpContext) -> CmdResult {
    op_arith(ctx, Arith::Multiply)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DbSlice;
    use crate::util::format_double;

    fn b_args(a: &[&str]) -> Vec<Vec<u8>> {
        a.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn db() -> DbSlice {
        DbSlice::new(0)
    }

    fn dispatch_at(db: &mut DbSlice, now: u64, argv: &[Vec<u8>]) -> CmdResult {
        let cmd = crate::commands::lookup(&argv[0])
            .unwrap_or_else(|| panic!("unknown cmd {:?}", argv[0]));
        if let Some(err) = cmd.check_arity(argv.len()) {
            return CmdResult::err(err);
        }
        let owned = cmd.key_range.keys(argv.len());
        let mut ctx = OpContext {
            db,
            args: argv,
            owned_keys: &owned,
            first_key_idx: 1,
            now_ms: now,
        };
        (cmd.exec)(&mut ctx)
    }

    /// Render a RespValue into a comparable string.
    fn render(v: &RespValue) -> String {
        match v {
            RespValue::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            RespValue::Simple(s) => s.clone(),
            RespValue::Integer(i) => i.to_string(),
            RespValue::Double(f) => format_double(*f),
            RespValue::Nil => "(nil)".into(),
            RespValue::Error(e) => e.clone(),
            RespValue::Bool(b) => b.to_string(),
            RespValue::Array(a) => {
                format!("[{}]", a.iter().map(render).collect::<Vec<_>>().join(", "))
            }
            RespValue::Map(m) => format!("MAP{}", m.len()),
        }
    }

    fn s(db: &mut DbSlice, argv: &[&str]) -> String {
        render(&dispatch_at(db, 0, &b_args(argv)).into_resp_value())
    }

    fn db_set(db: &mut DbSlice, key: &str, json: &str) {
        assert_eq!(s(db, &["JSON.SET", key, ".", json]), "OK");
    }

    #[test]
    fn set_and_get() {
        let mut d = db();
        db_set(&mut d, "json", r#"{"a":{"a":1,"b":2,"c":3}}"#);
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"{"a":{"a":1,"b":2,"c":3}}"#
        );
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "$"]),
            r#"[{"a":{"a":1,"b":2,"c":3}}]"#
        );
        assert_eq!(s(&mut d, &["JSON.GET", "json", "$.a.*"]), "[1,2,3]");
        assert_eq!(s(&mut d, &["JSON.GET", "json", "..a.*"]), "3");
    }

    #[test]
    fn set_partial() {
        let mut d = db();
        db_set(&mut d, "json", r#"{"a":2}"#);
        assert_eq!(s(&mut d, &["JSON.SET", "json", "$.b", "8"]), "OK");
        assert_eq!(s(&mut d, &["JSON.SET", "json", "$.c", "[1,2,3]"]), "OK");
        assert_eq!(s(&mut d, &["JSON.SET", "json", "$.z", "3", "XX"]), "(nil)");
        assert_eq!(s(&mut d, &["JSON.SET", "json", "$.z", "3"]), "OK");
        assert_eq!(s(&mut d, &["JSON.SET", "json", "$.z", "4", "XX"]), "OK");
        assert_eq!(s(&mut d, &["JSON.SET", "json", "$.b", "4", "NX"]), "(nil)");
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"{"a":2,"b":8,"c":[1,2,3],"z":4}"#
        );
    }

    #[test]
    fn set_legacy_partial_add() {
        let mut d = db();
        db_set(&mut d, "json", r#"{"a":{"a":1,"b":2,"c":3}}"#);
        assert_eq!(s(&mut d, &["JSON.SET", "json", ".a.*", "0"]), "OK");
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"{"a":{"a":0,"b":0,"c":0}}"#
        );
        let mut d = db();
        db_set(&mut d, "json", r#"{"a":2}"#);
        assert_eq!(s(&mut d, &["JSON.SET", "json", ".b", "8"]), "OK");
        assert_eq!(s(&mut d, &["JSON.SET", "json", ".c", "[1,2,3]"]), "OK");
        assert_eq!(s(&mut d, &["JSON.SET", "json", ".", "[]", "NX"]), "(nil)");
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"{"a":2,"b":8,"c":[1,2,3]}"#
        );
    }

    #[test]
    fn set_nested_fields_dot_keys() {
        let mut d = db();
        assert_eq!(s(&mut d, &["JSON.SET", "json", "$", "{}"]), "OK");
        assert_eq!(s(&mut d, &["JSON.SET", "json", "$['field1']", "1"]), "OK");
        assert_eq!(s(&mut d, &["JSON.SET", "json", "$['-field2']", "2"]), "OK");
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"{"-field2":2,"field1":1}"#
        );
    }

    #[test]
    fn type_works() {
        let mut d = db();
        db_set(&mut d, "json", r#"[1, 2.3, "foo", true, null, {}, []]"#);
        assert_eq!(
            render(
                &dispatch_at(&mut d, 0, &b_args(&["JSON.TYPE", "json", "$[*]"])).into_resp_value()
            ),
            "[integer, number, string, boolean, null, object, array]"
        );
        assert_eq!(s(&mut d, &["JSON.TYPE", "json", "$[10]"]), "[]");
        assert_eq!(s(&mut d, &["JSON.TYPE", "notequal", "$[10]"]), "(nil)");
        assert_eq!(s(&mut d, &["JSON.TYPE", "json", ".age"]), "(nil)");
        db_set(
            &mut d,
            "o",
            r#"{"age":27,"weight":135.25,"x":"s","isAlive":true,"spouse":null,"a":[],"b":{}}"#,
        );
        assert_eq!(s(&mut d, &["JSON.TYPE", "o", ".age"]), "integer");
        assert_eq!(s(&mut d, &["JSON.TYPE", "o", ".weight"]), "number");
        assert_eq!(s(&mut d, &["JSON.TYPE", "o", ".x"]), "string");
        assert_eq!(s(&mut d, &["JSON.TYPE", "o", ".isAlive"]), "boolean");
        assert_eq!(s(&mut d, &["JSON.TYPE", "o", ".spouse"]), "null");
    }

    #[test]
    fn strlen_works() {
        let mut d = db();
        db_set(
            &mut d,
            "json",
            r#"{"a":{"a":"a"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}}"#,
        );
        assert_eq!(s(&mut d, &["JSON.STRLEN", "json", "$.a.a"]), "[1]");
        assert_eq!(s(&mut d, &["JSON.STRLEN", "json", "$.a"]), "[(nil)]");
        assert_eq!(s(&mut d, &["JSON.STRLEN", "json", "$.c.*"]), "[1, 2]");
        assert_eq!(
            s(&mut d, &["JSON.STRLEN", "json", "$.d.*"]),
            "[(nil), 1, (nil)]"
        );
        assert_eq!(
            render(
                &dispatch_at(&mut d, 0, &b_args(&["JSON.STRLEN", "x", "$.c.b"])).into_resp_value()
            ),
            "ERR no such key"
        );
        // legacy
        assert_eq!(s(&mut d, &["JSON.STRLEN", "json", ".a.a"]), "1");
        assert_eq!(
            s(&mut d, &["JSON.STRLEN", "json", ".a"]),
            "WRONGTYPE wrong JSON type of path value"
        );
        assert_eq!(s(&mut d, &["JSON.STRLEN", "json", ".c.*"]), "1");
        assert_eq!(s(&mut d, &["JSON.STRLEN", "json", ".d.*"]), "1");
        assert_eq!(s(&mut d, &["JSON.STRLEN", "nokey", ".c.b"]), "(nil)");
    }

    #[test]
    fn objlen_works() {
        let mut d = db();
        db_set(
            &mut d,
            "json",
            r#"{"a":{},"b":{"a":"a"},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":{"a":3,"b":4}},"e":1}"#,
        );
        assert_eq!(s(&mut d, &["JSON.OBJLEN", "json", "$.a"]), "[0]");
        assert_eq!(s(&mut d, &["JSON.OBJLEN", "json", "$.a.*"]), "[]");
        assert_eq!(s(&mut d, &["JSON.OBJLEN", "json", "$.c"]), "[2]");
        assert_eq!(
            s(&mut d, &["JSON.OBJLEN", "json", "$.d.*"]),
            "[(nil), (nil), 2]"
        );
        assert_eq!(
            s(&mut d, &["JSON.OBJLEN", "json", "$.*"]),
            "[0, 1, 2, 3, (nil)]"
        );
        // legacy
        assert_eq!(s(&mut d, &["JSON.OBJLEN", "json", ".a"]), "0");
        assert_eq!(s(&mut d, &["JSON.OBJLEN", "json", ".a.*"]), "(nil)");
        assert_eq!(s(&mut d, &["JSON.OBJLEN", "json", ".d.*"]), "2");
        assert_eq!(s(&mut d, &["JSON.OBJLEN", "json", ".*"]), "0");
        assert_eq!(s(&mut d, &["JSON.OBJLEN", "json", ".none"]), "(nil)");
    }

    #[test]
    fn arrlen_works() {
        let mut d = db();
        db_set(&mut d, "json", r#"[[],["a"],["a","b"],["a","b","c"]]"#);
        assert_eq!(s(&mut d, &["JSON.ARRLEN", "json", "$[*]"]), "[0, 1, 2, 3]");
        db_set(&mut d, "json2", r#"[[],"a",["a","b"],["a","b","c"],4]"#);
        assert_eq!(
            s(&mut d, &["JSON.ARRLEN", "json2", "$[*]"]),
            "[0, (nil), 2, 3, (nil)]"
        );
        // legacy
        assert_eq!(s(&mut d, &["JSON.ARRLEN", "json", "[*]"]), "0");
        assert_eq!(s(&mut d, &["JSON.ARRLEN", "json"]), "4");
        assert_eq!(s(&mut d, &["JSON.ARRLEN", "json", "[3]"]), "3");
        assert_eq!(
            s(&mut d, &["JSON.ARRLEN", "json2", "[1]"]),
            "WRONGTYPE wrong JSON type of path value"
        );
        assert_eq!(s(&mut d, &["JSON.ARRLEN", "nokey", "[*]"]), "(nil)");
    }

    #[test]
    fn toggle_works() {
        let mut d = db();
        db_set(
            &mut d,
            "json",
            r#"{"a":true,"b":false,"c":1,"d":null,"e":"foo","f":[],"g":{}}"#,
        );
        assert_eq!(
            render(
                &dispatch_at(&mut d, 0, &b_args(&["JSON.TOGGLE", "json", "$.*"])).into_resp_value()
            ),
            "[0, 1, (nil), (nil), (nil), (nil), (nil)]"
        );
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "$.*"]),
            "[false,true,1,null,\"foo\",[],{}]"
        );
        // legacy
        let mut d = db();
        db_set(&mut d, "json", r#"{"isAvailable": false}"#);
        assert_eq!(s(&mut d, &["JSON.TOGGLE", "json", ".isAvailable"]), "true");
        assert_eq!(s(&mut d, &["JSON.TOGGLE", "json", ".isAvailable"]), "false");
        db_set(&mut d, "j", "true");
        assert_eq!(s(&mut d, &["JSON.TOGGLE", "j", "."]), "false");
        assert_eq!(s(&mut d, &["JSON.TOGGLE", "j", "."]), "true");
        // arity: path required
        assert_eq!(
            s(&mut d, &["JSON.TOGGLE", "json"]),
            "ERR wrong number of arguments for 'json.toggle' command"
        );
    }

    #[test]
    fn numincrby_works() {
        let mut d = db();
        db_set(&mut d, "json", r#"{"e":1.5,"a":1}"#);
        assert_eq!(s(&mut d, &["JSON.NUMINCRBY", "json", "$.a", "-2"]), "[-1]");
        db_set(&mut d, "json", r#"{"a":9223372036854775808}"#);
        assert_eq!(
            s(&mut d, &["JSON.NUMINCRBY", "json", "$.a", "2048"]),
            "[9223372036854777856]"
        );
        db_set(&mut d, "json", r#"{"a":-9223372036854775808}"#);
        assert_eq!(
            s(
                &mut d,
                &["JSON.NUMINCRBY", "json", "$.a", "-9223372036854775808"]
            ),
            "ERR result is not a number"
        );
        db_set(&mut d, "json", r#"{"e":1.5,"a":1}"#);
        assert_eq!(
            s(&mut d, &["JSON.NUMINCRBY", "json", "$.a", "1.1"]),
            "[2.1]"
        );
        assert_eq!(s(&mut d, &["JSON.NUMINCRBY", "json", "$.e", "1"]), "[2.5]");
        assert_eq!(
            s(&mut d, &["JSON.NUMINCRBY", "json", "$.e", "inf"]),
            "ERR result is not a number"
        );
        db_set(&mut d, "json", r#"{"e":1.5,"a":1}"#);
        assert_eq!(
            s(&mut d, &["JSON.NUMINCRBY", "json", "$.e", "1.7e308"]),
            "[1.7e+308]"
        );
        // non-number placeholder
        db_set(
            &mut d,
            "json",
            r#"{"a":{"a":"a"},"d":{"a":1,"b":"b","c":3}}"#,
        );
        assert_eq!(
            s(&mut d, &["JSON.NUMINCRBY", "json", "$.a.*", "1"]),
            "[null]"
        );
        assert_eq!(
            s(&mut d, &["JSON.NUMINCRBY", "json", "$.d.*", "1"]),
            "[2,null,4]"
        );
        // legacy -> last number
        let mut d = db();
        db_set(
            &mut d,
            "json",
            r#"{"a":{"a":"a"},"b":{"a":"a","b":1},"c":{"a":"a","b":"b"},"d":{"a":1,"b":"b","c":3}}"#,
        );
        assert_eq!(
            s(&mut d, &["JSON.NUMINCRBY", "json", ".a.*", "1"]),
            "WRONGTYPE wrong JSON type of path value"
        );
        assert_eq!(s(&mut d, &["JSON.NUMINCRBY", "json", ".b.*", "1"]), "2");
        assert_eq!(
            s(&mut d, &["JSON.NUMINCRBY", "json", ".c.*", "1"]),
            "WRONGTYPE wrong JSON type of path value"
        );
        assert_eq!(s(&mut d, &["JSON.NUMINCRBY", "json", ".d.*", "1"]), "4");
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"{"a":{"a":"a"},"b":{"a":"a","b":2},"c":{"a":"a","b":"b"},"d":{"a":2,"b":"b","c":4}}"#
        );
    }

    #[test]
    fn nummultby_works() {
        let mut d = db();
        db_set(&mut d, "json", r#"{"a":[],"b":[1],"c":[1,2],"d":[1,2,3]}"#);
        assert_eq!(
            s(&mut d, &["JSON.NUMMULTBY", "json", "$.d[*]", "2"]),
            "[2,4,6]"
        );
        assert_eq!(s(&mut d, &["JSON.NUMMULTBY", "json", "$.a[*]", "2"]), "[]");
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"{"a":[],"b":[1],"c":[1,2],"d":[2,4,6]}"#
        );
        assert_eq!(s(&mut d, &["JSON.NUMMULTBY", "json", ".d[*]", "2"]), "12");
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"{"a":[],"b":[1],"c":[1,2],"d":[4,8,12]}"#
        );
    }

    #[test]
    fn numeric_conversions() {
        let mut d = db();
        db_set(&mut d, "json", r#"{"a":2.0}"#);
        assert_eq!(s(&mut d, &["JSON.NUMINCRBY", "json", "$.a", "1"]), "[3.0]");
        db_set(&mut d, "json", r#"{"a":2}"#);
        assert_eq!(s(&mut d, &["JSON.NUMINCRBY", "json", "$.a", "1"]), "[3]");
        assert_eq!(s(&mut d, &["JSON.GET", "json", "."]), r#"{"a":3}"#);
        assert_eq!(
            s(&mut d, &["JSON.NUMINCRBY", "json", "$.a", "1.0"]),
            "[4.0]"
        );
        assert_eq!(s(&mut d, &["JSON.GET", "json", "."]), r#"{"a":4.0}"#);
    }

    #[test]
    fn del_works() {
        let mut d = db();
        db_set(
            &mut d,
            "json",
            r#"{"a":{},"b":{"a":1},"c":{"a":1,"b":2},"d":{"a":1,"b":2,"c":3},"e":[1,2,3,4,5]}"#,
        );
        assert_eq!(s(&mut d, &["JSON.DEL", "json", "$.d.*"]), "3");
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"{"a":{},"b":{"a":1},"c":{"a":1,"b":2},"d":{},"e":[1,2,3,4,5]}"#
        );
        assert_eq!(s(&mut d, &["JSON.DEL", "json", "$.e[*]"]), "5");
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"{"a":{},"b":{"a":1},"c":{"a":1,"b":2},"d":{},"e":[]}"#
        );
        assert_eq!(s(&mut d, &["JSON.DEL", "json", "$..*"]), "5");
        assert_eq!(s(&mut d, &["JSON.GET", "json", "."]), "{}");
        assert_eq!(s(&mut d, &["JSON.DEL", "json"]), "1");
        assert_eq!(s(&mut d, &["JSON.GET", "json", "."]), "(nil)");
        // recursive delete only removes the key "a" at root
        let mut d = db();
        db_set(
            &mut d,
            "doc2",
            r#"{"a":{"a":2,"b":3},"b":["a","b"],"nested":{"b":[true,"a","b"]}}"#,
        );
        assert_eq!(s(&mut d, &["JSON.DEL", "doc2", "$..a"]), "1");
        assert_eq!(
            s(&mut d, &["JSON.GET", "doc2", "."]),
            r#"{"b":["a","b"],"nested":{"b":[true,"a","b"]}}"#
        );
        // non-existing key -> 0
        assert_eq!(s(&mut d, &["JSON.DEL", "nonexisting"]), "0");
        assert_eq!(s(&mut d, &["JSON.DEL", "nonexisting", "$"]), "0");
        assert_eq!(s(&mut d, &["JSON.DEL", "nonexisting", "."]), "0");
    }

    #[test]
    fn objkeys_works() {
        let mut d = db();
        db_set(
            &mut d,
            "json",
            r#"{"a":{},"b":{"a":"a"},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":{"a":3,"b":4}},"e":1}"#,
        );
        assert_eq!(
            s(&mut d, &["JSON.OBJKEYS", "json", "$"]),
            "[[a, b, c, d, e]]"
        );
        assert_eq!(
            s(&mut d, &["JSON.OBJKEYS", "json", "$.*"]),
            "[[], [a], [a, b], [a, b, c], []]"
        );
        assert_eq!(s(&mut d, &["JSON.OBJKEYS", "json", "$.notfound"]), "[]");
        // legacy
        assert_eq!(s(&mut d, &["JSON.OBJKEYS", "json", "."]), "[a, b, c, d, e]");
        assert_eq!(s(&mut d, &["JSON.OBJKEYS", "json", ".a"]), "[]");
        assert_eq!(s(&mut d, &["JSON.OBJKEYS", "json", ".*"]), "[]");
        assert_eq!(s(&mut d, &["JSON.OBJKEYS", "json", ".notfound"]), "(nil)");
    }

    #[test]
    fn strappend_works() {
        let mut d = db();
        db_set(
            &mut d,
            "json",
            r#"{"a":{"a":"a"},"b":{"a":"a","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"b","c":3}}"#,
        );
        assert_eq!(
            s(&mut d, &["JSON.STRAPPEND", "json", "$.a.a", r#""ab""#]),
            "[3]"
        );
        assert_eq!(
            s(&mut d, &["JSON.STRAPPEND", "json", "$.b.*", r#""a""#]),
            "[2, (nil)]"
        );
        assert_eq!(
            s(&mut d, &["JSON.STRAPPEND", "json", "$.d.*", r#""a""#]),
            "[(nil), 2, (nil)]"
        );
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"{"a":{"a":"aab"},"b":{"a":"aa","b":1},"c":{"a":"a","b":"bb"},"d":{"a":1,"b":"ba","c":3}}"#
        );
        // default path (root)
        let mut d = db();
        db_set(&mut d, "json", r#""foo""#);
        assert_eq!(s(&mut d, &["JSON.STRAPPEND", "json", r#""bar""#]), "6");
        assert_eq!(s(&mut d, &["JSON.GET", "json", "."]), r#""foobar""#);
        // legacy: last updated
        let mut d = db();
        db_set(
            &mut d,
            "json",
            r#"{"a":{"a":"a","b":"aa","c":"aaa"},"b":{"a":"aaa","b":"aa","c":"a"}}"#,
        );
        assert_eq!(
            s(&mut d, &["JSON.STRAPPEND", "json", ".a.*", r#""a""#]),
            "4"
        );
        assert_eq!(
            s(&mut d, &["JSON.STRAPPEND", "json", ".b.*", r#""a""#]),
            "2"
        );
    }

    #[test]
    fn clear_works() {
        let mut d = db();
        db_set(&mut d, "json", r#"[[],[0],[0,1],[0,1,2],1,true,null,"d"]"#);
        assert_eq!(s(&mut d, &["JSON.CLEAR", "json", "$[*]"]), "5");
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"[[],[],[],[],0,true,null,"d"]"#
        );
        assert_eq!(s(&mut d, &["JSON.CLEAR", "json", "$"]), "1");
        assert_eq!(s(&mut d, &["JSON.GET", "json", "."]), "[]");
        // legacy
        let mut d = db();
        db_set(&mut d, "json", r#"{"children":["Yossi","Rafi"]}"#);
        assert_eq!(s(&mut d, &["JSON.CLEAR", "json", ".children"]), "1");
        assert_eq!(s(&mut d, &["JSON.GET", "json", "."]), r#"{"children":[]}"#);
    }

    #[test]
    fn arrpop_works() {
        let mut d = db();
        db_set(&mut d, "json", r"[[6,1,6],[7,2,7],[8,3,8]]");
        assert_eq!(
            s(&mut d, &["JSON.ARRPOP", "json", "$[*]", "-2"]),
            "[1, 2, 3]"
        );
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r"[[6,6],[7,7],[8,8]]"
        );
        db_set(&mut d, "json", r#"[[],["a"],["a","b"]]"#);
        assert_eq!(
            s(&mut d, &["JSON.ARRPOP", "json", "$[*]"]),
            "[(nil), \"a\", \"b\"]"
        );
        assert_eq!(s(&mut d, &["JSON.GET", "json", "."]), r#"[[],[],["a"]]"#);
        // legacy: root pop
        let mut d = db();
        db_set(&mut d, "json", r#"[[],["a"],["a","b"]]"#);
        assert_eq!(s(&mut d, &["JSON.ARRPOP", "json", "."]), r#"["a","b"]"#);
        assert_eq!(s(&mut d, &["JSON.GET", "json", "."]), r#"[[],["a"]]"#);
        // object root -> nil
        let mut d = db();
        db_set(&mut d, "json", r#"{"a":"b"}"#);
        assert_eq!(s(&mut d, &["JSON.ARRPOP", "json", "."]), "(nil)");
    }

    #[test]
    fn arrtrim_works() {
        let mut d = db();
        db_set(&mut d, "json", r#"[[],["a"],["a","b"],["a","b","c"]]"#);
        assert_eq!(
            s(&mut d, &["JSON.ARRTRIM", "json", "$[*]", "0", "1"]),
            "[0, 1, 2, 2]"
        );
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"[[],["a"],["a","b"],["a","b"]]"#
        );
        db_set(&mut d, "json", r#"{"a":[1,2,3,2],"nested":{"a":false}}"#);
        assert_eq!(
            s(&mut d, &["JSON.ARRTRIM", "json", "$..a", "1", "2"]),
            "[2, (nil)]"
        );
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"{"a":[2,3],"nested":{"a":false}}"#
        );
        // legacy: last array's new length (doc already trimmed to a=[2,3])
        assert_eq!(s(&mut d, &["JSON.ARRTRIM", "json", "..a", "1", "2"]), "1");
    }

    #[test]
    fn arrinsert_works() {
        let mut d = db();
        db_set(&mut d, "json", r#"[[],["a"],["a","b"]]"#);
        assert_eq!(
            s(&mut d, &["JSON.ARRINSERT", "json", "$[*]", "0", r#""a""#]),
            "[1, 2, 3]"
        );
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"[["a"],["a","a"],["a","a","b"]]"#
        );
        assert_eq!(
            s(&mut d, &["JSON.ARRINSERT", "json", "$[*]", "-1", r#""b""#]),
            "[2, 3, 4]"
        );
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"[["b","a"],["a","b","a"],["a","a","b","b"]]"#
        );
        // out of range
        db_set(&mut d, "arr", r"[0,1,2,3,4,5]");
        assert_eq!(
            s(&mut d, &["JSON.ARRINSERT", "arr", "$", "-55", "6"]),
            "ERR index out of range"
        );
        assert_eq!(
            s(&mut d, &["JSON.ARRINSERT", "arr", "$", "55", "6"]),
            "ERR index out of range"
        );
        db_set(&mut d, "arr", "[]");
        assert_eq!(s(&mut d, &["JSON.ARRINSERT", "arr", "$", "0", "2"]), "[1]");
        assert_eq!(s(&mut d, &["JSON.GET", "arr", "."]), "[2]");
    }

    #[test]
    fn arrappend_works() {
        let mut d = db();
        db_set(&mut d, "json", r#"[[],["a"],["a","b"]]"#);
        assert_eq!(
            s(&mut d, &["JSON.ARRAPPEND", "json", "$[*]", r#""a""#]),
            "[1, 2, 3]"
        );
        assert_eq!(
            s(&mut d, &["JSON.ARRAPPEND", "json", "$[*]", r#""b""#]),
            "[2, 3, 4]"
        );
        db_set(
            &mut d,
            "json",
            r#"{"a":[1],"nested":{"a":[1,2],"nested2":{"a":42}}}"#,
        );
        assert_eq!(
            s(&mut d, &["JSON.ARRAPPEND", "json", "$..a", "3"]),
            "[2, 3, (nil)]"
        );
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "."]),
            r#"{"a":[1,3],"nested":{"a":[1,2,3],"nested2":{"a":42}}}"#
        );
    }

    #[test]
    fn arrindex_works() {
        let mut d = db();
        db_set(&mut d, "json", r#"[[],["a"],["a","b"],["a","b","c"]]"#);
        assert_eq!(
            s(&mut d, &["JSON.ARRINDEX", "json", "$[*]", r#""b""#]),
            "[-1, -1, 1, 1]"
        );
        db_set(
            &mut d,
            "json",
            r#"{"a":["a","b","c","d"],"nested":{"a":["c","d"]}}"#,
        );
        assert_eq!(
            s(&mut d, &["JSON.ARRINDEX", "json", "$..a", r#""b""#]),
            "[1, -1]"
        );
        db_set(
            &mut d,
            "json",
            r#"{"a":["a","b","c","d"],"nested":{"a":false}}"#,
        );
        assert_eq!(
            s(&mut d, &["JSON.ARRINDEX", "json", "$..a", r#""b""#]),
            "[1, (nil)]"
        );
        // legacy
        db_set(
            &mut d,
            "json",
            r#"{"children":["John","Jack","Tom","Bob","Mike"]}"#,
        );
        assert_eq!(
            s(&mut d, &["JSON.ARRINDEX", "json", ".children", r#""Tom""#]),
            "2"
        );
        assert_eq!(
            s(&mut d, &["JSON.ARRINDEX", "json", ".children", r#""Nope""#]),
            "-1"
        );
    }

    #[test]
    fn arrindex_numeric_types() {
        let mut d = db();
        db_set(&mut d, "json", r"[2, 3.0, 3]");
        assert_eq!(s(&mut d, &["JSON.ARRINDEX", "json", "$", "3"]), "[2]");
        assert_eq!(s(&mut d, &["JSON.ARRINDEX", "json", "$", "3.0"]), "[1]");
        db_set(&mut d, "json", r"[[1,2,3],[1.0,2.0,3.0],2.0,[1,2,3]]");
        assert_eq!(s(&mut d, &["JSON.ARRINDEX", "json", "$", "[1,2,3]"]), "[0]");
        db_set(&mut d, "json", r#"[{"a":2},{"a":2.0},2.0]"#);
        assert_eq!(
            s(&mut d, &["JSON.ARRINDEX", "json", "$", r#"{"a":2}"#]),
            "[0]"
        );
    }

    #[test]
    fn mget_works() {
        let mut d = db();
        db_set(&mut d, "json1", r#"{"address":{"country":"Israel"}}"#);
        db_set(&mut d, "json2", r#"{"address":{"country":"Germany"}}"#);
        assert_eq!(
            render(
                &dispatch_at(
                    &mut d,
                    0,
                    &b_args(&["JSON.MGET", "json1", "json2", "json3", "$.address.country"])
                )
                .into_resp_value()
            ),
            "[[\"Israel\"], [\"Germany\"], (nil)]"
        );
        assert_eq!(
            render(
                &dispatch_at(
                    &mut d,
                    0,
                    &b_args(&["JSON.MGET", "json1", "json2", ".address.country"])
                )
                .into_resp_value()
            ),
            "[\"Israel\", \"Germany\"]"
        );
        db_set(&mut d, "json3", r#"{"a":1,"nested":{"a":3}}"#);
        db_set(&mut d, "json4", r#"{"a":4,"nested":{"a":6}}"#);
        assert_eq!(
            render(
                &dispatch_at(&mut d, 0, &b_args(&["JSON.MGET", "json3", "json4", "$..a"]))
                    .into_resp_value()
            ),
            "[[1,3], [4,6]]"
        );
        assert_eq!(
            s(&mut d, &["JSON.MGET", "json1", "??INVALID??"]),
            "ERR syntax error"
        );
    }

    #[test]
    fn mset_works() {
        let mut d = db();
        assert_eq!(
            s(
                &mut d,
                &[
                    "JSON.MSET",
                    "j1",
                    "$",
                    r#"{"a":1}"#,
                    "j2",
                    "$",
                    r#"{"a":2}"#
                ]
            ),
            "OK"
        );
        assert_eq!(s(&mut d, &["JSON.GET", "j1", "$.a"]), "[1]");
        assert_eq!(s(&mut d, &["JSON.GET", "j2", "$.a"]), "[2]");
        assert_eq!(
            s(&mut d, &["JSON.MSET", "j1", "$"]),
            "ERR wrong number of arguments for 'json.mset' command"
        );
        assert_eq!(
            s(&mut d, &["JSON.MSET", "j1", "$", "{}", "j3", "$"]),
            "ERR wrong number of arguments for 'json.mset' command"
        );
    }

    #[test]
    fn merge_works() {
        let mut d = db();
        db_set(&mut d, "j1", r#"{"a":"b","c":{"d":"e","f":"g"}}"#);
        assert_eq!(
            s(
                &mut d,
                &["JSON.MERGE", "j1", "$", r#"{"a":"z","c":{"f":null}}"#]
            ),
            "OK"
        );
        assert_eq!(
            s(&mut d, &["JSON.GET", "j1", "."]),
            r#"{"a":"z","c":{"d":"e"}}"#
        );
        // new key at root -> set
        assert_eq!(
            s(
                &mut d,
                &["JSON.MERGE", "new", "$", r#"{"a":"z","c":{"f":null}}"#]
            ),
            "OK"
        );
        assert_eq!(
            s(&mut d, &["JSON.GET", "new", "."]),
            r#"{"a":"z","c":{"f":null}}"#
        );
        // partial path merge
        db_set(
            &mut d,
            "j2",
            r#"{"ans":{"x":{"y":{"answers":["foo","bar"]}}}}"#,
        );
        assert_eq!(
            s(
                &mut d,
                &[
                    "JSON.MERGE",
                    "j2",
                    "$.ans.x",
                    r#"{"y":{"doubled":true},"z":{"a":1}}"#
                ]
            ),
            "OK"
        );
        assert_eq!(
            s(&mut d, &["JSON.GET", "j2", "."]),
            r#"{"ans":{"x":{"y":{"answers":["foo","bar"],"doubled":true},"z":{"a":1}}}}"#
        );
        // merge a string target replaces it
        db_set(&mut d, "foo", r#""{f1:1, common:2}""#);
        assert_eq!(
            s(
                &mut d,
                &["JSON.MERGE", "foo", "$", r#"{"f2":2,"common":4}"#]
            ),
            "OK"
        );
        assert_eq!(
            s(&mut d, &["JSON.GET", "foo", "."]),
            r#"{"common":4,"f2":2}"#
        );
    }

    #[test]
    fn resp_works() {
        let mut d = db();
        db_set(
            &mut d,
            "json",
            r#"{"isAlive":true,"age":27,"weight":135.25,"s":"hi","c":null}"#,
        );
        // V2
        assert_eq!(s(&mut d, &["JSON.RESP", "json", "$.isAlive"]), "[true]");
        assert_eq!(s(&mut d, &["JSON.RESP", "json", "$.age"]), "[27]");
        assert_eq!(s(&mut d, &["JSON.RESP", "json", "$.weight"]), "[135.25]");
        // legacy scalar
        assert_eq!(s(&mut d, &["JSON.RESP", "json", ".isAlive"]), "true");
        assert_eq!(s(&mut d, &["JSON.RESP", "json", ".age"]), "27");
        assert_eq!(s(&mut d, &["JSON.RESP", "json", ".weight"]), "135.25");
        // object value -> RESP array with `{` marker and [key, value] pairs
        assert_eq!(s(&mut d, &["JSON.RESP", "json", "$.s"]), "[hi]");
    }

    #[test]
    fn debug_fields() {
        let mut d = db();
        db_set(
            &mut d,
            "json1",
            r#"[1,2.3,"foo",true,null,{},[],{"a":1,"b":2},[1,2,3]]"#,
        );
        assert_eq!(
            s(&mut d, &["JSON.DEBUG", "fields", "json1", "$[*]"]),
            "[1, 1, 1, 1, 1, 0, 0, 2, 3]"
        );
        assert_eq!(s(&mut d, &["JSON.DEBUG", "fields", "json1", "$"]), "[14]");
        assert_eq!(s(&mut d, &["JSON.DEBUG", "fields", "json1", "[*]"]), "3");
        assert_eq!(s(&mut d, &["JSON.DEBUG", "fields", "json1", "."]), "14");
        assert_eq!(s(&mut d, &["JSON.DEBUG", "fields", "json1"]), "14");
        // memory: scalars are 0; containers with content are > 0
        assert_eq!(s(&mut d, &["JSON.DEBUG", "memory", "json1", "$[0]"]), "[0]");
        match dispatch_at(
            &mut d,
            0,
            &b_args(&["JSON.DEBUG", "memory", "json1", "$[8]"]),
        )
        .into_resp_value()
        {
            RespValue::Array(items) => assert!(
                matches!(items[0], RespValue::Integer(n) if n > 0),
                "expected memory > 0 for $[8], got {items:?}"
            ),
            o => panic!("expected array, got {o:?}"),
        }
        assert_eq!(s(&mut d, &["JSON.DEBUG", "memory", "json1", "[4]"]), "0");
        // help
        let help = dispatch_at(&mut d, 0, &b_args(&["JSON.DEBUG", "HELP"])).into_resp_value();
        match help {
            RespValue::Array(items) => {
                assert_eq!(items.len(), 3);
                let text = items
                    .iter()
                    .map(|i| match i {
                        RespValue::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
                        o => panic!("unexpected {o:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                assert!(text.contains("MEMORY"));
                assert!(text.contains("FIELDS"));
                assert!(text.contains("HELP"));
            }
            o => panic!("expected array, got {o:?}"),
        }
        // missing key -> syntax error from the subcommand parser
        assert_eq!(s(&mut d, &["JSON.DEBUG", "FIELDS"]), "ERR syntax error");
        assert_eq!(s(&mut d, &["JSON.DEBUG", "MEMORY"]), "ERR syntax error");
    }

    #[test]
    fn get_with_format() {
        let mut d = db();
        db_set(
            &mut d,
            "json",
            r#"{"firstName":"John","age":27,"lastName":"Smith","address":{"city":"New York","state":"NY"}}"#,
        );
        assert_eq!(
            s(
                &mut d,
                &[
                    "JSON.GET",
                    "json",
                    "$.firstName",
                    "INDENT",
                    "i",
                    "NEWLINE",
                    "n"
                ]
            ),
            "[ni\"John\"n]"
        );
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", "$.address", "SPACE", "space"]),
            r#"[{"city":space"New York","state":space"NY"}]"#
        );
        assert_eq!(
            s(
                &mut d,
                &[
                    "JSON.GET",
                    "json",
                    "$.firstName",
                    "$.age",
                    "$.lastName",
                    "INDENT",
                    "indent",
                    "NEWLINE",
                    "newline",
                    "SPACE",
                    "space"
                ]
            ),
            "{newlineindent\"$.age\":space[newlineindentindent27newlineindent],newlineindent\"$.firstName\":space[newlineindentindent\"John\"newlineindent],newlineindent\"$.lastName\":space[newlineindentindent\"Smith\"newlineindent]newline}"
        );
        // NOESCAPE is accepted but has no effect
        db_set(&mut d, "json", r#"{"key":"a\nb"}"#);
        assert_eq!(
            s(&mut d, &["JSON.GET", "json", ".", "NOESCAPE"]),
            r#"{"key":"a\nb"}"#
        );
    }

    #[test]
    fn get_on_string_key() {
        let mut d = db();
        assert_eq!(s(&mut d, &["SET", "json", r#"{"a":"b"}"#]), "OK");
        assert_eq!(s(&mut d, &["JSON.GET", "json", "."]), r#"{"a":"b"}"#);
        assert_eq!(s(&mut d, &["JSON.GET", "json"]), r#"{"a":"b"}"#);
        // invalid JSON string -> wrong type
        assert_eq!(s(&mut d, &["SET", "not_json", "not_json"]), "OK");
        assert_eq!(
            s(&mut d, &["JSON.GET", "not_json", "$.c"]),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
    }

    #[test]
    fn wrong_type_key() {
        let mut d = db();
        assert_eq!(s(&mut d, &["HSET", "k1", "f", "v"]), "1");
        assert_eq!(
            s(&mut d, &["JSON.SET", "k1", "$", r#"{"a":"b"}"#]),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        assert_eq!(s(&mut d, &["HSET", "k2", "f", "v"]), "1");
        assert_eq!(
            s(&mut d, &["JSON.DEL", "k2"]),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
    }
}
