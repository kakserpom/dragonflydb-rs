// ---------------------------------------------------------------------------
// GEO commands: GEOADD, GEOHASH, GEOPOS, GEODIST, GEOSEARCH, GEOSEARCHSTORE,
// GEORADIUS[_RO], GEORADIUSBYMEMBER[_RO].
//
// Port of `dragonfly/src/server/geo_family.cc`. Search semantics follow the
// reference: resolve the search center (FROMMEMBER score or FROMLONLAT),
// derive the covering geohash areas, gather candidate scores in area/zset
// order, filter with `within_shape`, then sort/trim per the requested
// ordering. GEOSEARCHSTORE is not present in the reference and follows
// redis-server semantics instead (the destination is deleted for any empty
// result, including a missing source).
// ---------------------------------------------------------------------------

use crate::commands::{
    Command, FLAG_DENYOOM, FLAG_MOVABLEKEYS, FLAG_NO_REDUCED, FLAG_READONLY, FLAG_WRITE, KeyRange,
    OpContext, ShardPart, bulk, integer,
};
use crate::core::PrimeValue;
use crate::core::compact::CompactString;
use crate::core::geohash::{self, GEO_STEP_MAX, GeoHashBits, GeoHashRange, GeoShape};
use crate::core::zset::ZSet;
use crate::error::{CmdResult, RespError, RespValue};
use crate::util::{format_double, parse_double, parse_u64};

const ERR_COUNT: &str = "ERR COUNT must be > 0";
const ERR_NX_XX: &str = "ERR XX and NX options at the same time are not compatible";
const ERR_SOURCE_CONFLICT: &str =
    "ERR FROMMEMBER and FROMLONLAT options at the same time are not compatible";
const ERR_SHAPE_CONFLICT: &str =
    "ERR BYRADIUS and BYBOX options at the same time are not compatible";
const ERR_ASC_DESC: &str = "ERR ASC and DESC options at the same time are not compatible";
const ERR_STORE_TYPE: &str = "ERR STORE and STOREDIST options at the same time are not compatible";
const ERR_STORE_COMPAT_RADIUS: &str = "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options";
const ERR_STORE_COMPAT_BYMEMBER: &str = "ERR STORE option in GEORADIUSBYMEMBER is not compatible with WITHDIST, WITHHASH and WITHCOORDS options";
const ERR_STORE_COMPAT_SEARCHSTORE: &str =
    "ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options";
const ERR_MEMBER_NOT_FOUND: &str = "ERR could not decode requested zset member";
const ERR_INVALID_UNIT: &str = "ERR unsupported unit provided. please use M, KM, FT, MI";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortType {
    Unsorted,
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeoStoreType {
    NoStore,
    StoreHash,
    StoreDist,
}

#[derive(Debug)]
enum GeoSource {
    Member { member: Vec<u8> },
    LonLat,
}

#[derive(Debug, Clone, Copy)]
struct GeoSearchOpts {
    conversion: f64,
    count: u64,
    sorting: SortType,
    any: bool,
    withdist: bool,
    withcoord: bool,
    withhash: bool,
    store: GeoStoreType,
    store_key: usize,
    store_key_nonzero: bool,
    allow_store: bool,
    /// Message used when a COUNT value is not a number (`"ERR syntax error"`
    /// for the GEORADIUSBYMEMBER family, generic integer error otherwise).
    count_err: Option<&'static str>,
}

impl GeoSearchOpts {
    fn new(allow_store: bool) -> Self {
        GeoSearchOpts {
            conversion: 0.0,
            count: u64::MAX,
            sorting: SortType::Unsorted,
            any: false,
            withdist: false,
            withcoord: false,
            withhash: false,
            store: GeoStoreType::NoStore,
            store_key: 0,
            store_key_nonzero: false,
            allow_store,
            count_err: None,
        }
    }

    fn has_with_statement(&self) -> bool {
        self.withdist || self.withcoord || self.withhash
    }
}

/// Incremental `GeoShape` builder used by the GEOSEARCH grammar, where FROMLONLAT
/// and BYRADIUS/BYBOX mutate the same struct so their order is irrelevant.
#[derive(Debug, Clone, Copy)]
struct ShapeAccum {
    xy: [f64; 2],
    radius: f64,
    width: f64,
    height: f64,
    conversion: f64,
    /// 1 = circle (BYRADIUS), 2 = rect (BYBOX), 0 = none.
    kind: u8,
}

impl ShapeAccum {
    fn new() -> Self {
        ShapeAccum {
            xy: [0.0, 0.0],
            radius: 0.0,
            width: 0.0,
            height: 0.0,
            conversion: 0.0,
            kind: 0,
        }
    }

    fn into_shape(self) -> Option<GeoShape> {
        match self.kind {
            1 => Some(GeoShape::Circle {
                xy: self.xy,
                radius: self.radius,
                conversion: self.conversion,
            }),
            2 => Some(GeoShape::Rect {
                xy: self.xy,
                width: self.width,
                height: self.height,
                conversion: self.conversion,
            }),
            _ => None,
        }
    }
}

struct GeoParse {
    source: Option<GeoSource>,
    acc: ShapeAccum,
    opts: GeoSearchOpts,
}

struct GeoPoint {
    longitude: f64,
    latitude: f64,
    dist: f64,
    score: f64,
    member: CompactString,
}

fn parse_geo_unit(arg: &[u8]) -> Option<f64> {
    match arg.to_ascii_uppercase().as_slice() {
        b"M" => Some(1.0),
        b"KM" => Some(1000.0),
        b"FT" => Some(0.3048),
        b"MI" => Some(1609.34),
        _ => None,
    }
}

fn parse_radius(arg: &[u8]) -> Result<f64, RespError> {
    parse_double(arg).ok_or_else(RespError::float)
}

fn is_valid_lonlat(long: f64, lat: f64) -> bool {
    long.is_finite()
        && lat.is_finite()
        && (geohash::GEO_LONG_MIN..=geohash::GEO_LONG_MAX).contains(&long)
        && (geohash::GEO_LAT_MIN..=geohash::GEO_LAT_MAX).contains(&lat)
}

/// Parse lon/lat for GEOSEARCH/GEORADIUS. Out-of-range / non-finite numerics
/// report `invalid longitude,latitude pair` with the parsed values (mirrors
/// `HandleGeoParserFinalize`); unparseable tokens report `INVALID_FLOAT`.
fn parse_long_lat(lon: &[u8], lat: &[u8]) -> Result<[f64; 2], RespError> {
    let long = parse_double(lon);
    let latv = parse_double(lat);
    match (long, latv) {
        (Some(long), Some(lat)) if is_valid_lonlat(long, lat) => Ok([long, lat]),
        (Some(long), Some(lat)) => Err(RespError::new(format!(
            "ERR invalid longitude,latitude pair {},{}",
            format_double(long),
            format_double(lat)
        ))),
        _ => Err(RespError::float()),
    }
}

/// Parse lon/lat for GEOADD, which reports the raw argument tokens in the error.
fn parse_geoadd_longlat(lon: &[u8], lat: &[u8], member: &[u8]) -> Result<[f64; 2], RespError> {
    let long = parse_double(lon);
    let latv = parse_double(lat);
    match (long, latv) {
        (Some(long), Some(lat)) if is_valid_lonlat(long, lat) => Ok([long, lat]),
        _ => Err(RespError::new(format!(
            "ERR invalid longitude,latitude pair {},{},{}",
            String::from_utf8_lossy(lon),
            String::from_utf8_lossy(lat),
            String::from_utf8_lossy(member),
        ))),
    }
}

fn parse_count(arg: &[u8], custom: Option<&'static str>) -> Result<u64, RespError> {
    match parse_u64(arg) {
        Some(v) => Ok(v),
        None => match custom {
            Some(msg) => Err(RespError::new(msg)),
            None => Err(RespError::integer()),
        },
    }
}

/// Options loop shared by the GEORADIUS family (mirrors `ParseGeoResultOptions`).
fn parse_geo_result_options(
    args: &[Vec<u8>],
    mut i: usize,
    opts: &mut GeoSearchOpts,
) -> Result<(), RespError> {
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"ASC" | b"DESC" => {
                if opts.sorting != SortType::Unsorted {
                    return Err(RespError::new(ERR_ASC_DESC));
                }
                opts.sorting = if args[i].eq_ignore_ascii_case(b"ASC") {
                    SortType::Asc
                } else {
                    SortType::Desc
                };
                i += 1;
            }
            b"COUNT" => {
                let count_arg = args.get(i + 1).ok_or_else(RespError::syntax)?;
                opts.count = parse_count(count_arg, opts.count_err)?;
                i += 2;
                if args.get(i).is_some_and(|a| a.eq_ignore_ascii_case(b"ANY")) {
                    opts.any = true;
                    i += 1;
                }
            }
            b"WITHCOORD" => {
                opts.withcoord = true;
                i += 1;
            }
            b"WITHDIST" => {
                opts.withdist = true;
                i += 1;
            }
            b"WITHHASH" => {
                opts.withhash = true;
                i += 1;
            }
            b"STORE" | b"STOREDIST" if opts.allow_store => {
                if opts.store != GeoStoreType::NoStore {
                    return Err(RespError::new(ERR_STORE_TYPE));
                }
                if i + 1 >= args.len() {
                    return Err(RespError::syntax());
                }
                opts.store = if args[i].eq_ignore_ascii_case(b"STORE") {
                    GeoStoreType::StoreHash
                } else {
                    GeoStoreType::StoreDist
                };
                opts.store_key = i + 1;
                opts.store_key_nonzero = true;
                i += 2;
            }
            _ => return Err(RespError::syntax()),
        }
    }
    Ok(())
}

/// GEOSEARCH / GEOSEARCHSTORE option parser (mirrors `kGeoSearchGrammar`).
fn parse_geosearch_opts(
    args: &[Vec<u8>],
    start: usize,
    is_store: bool,
) -> Result<GeoParse, RespError> {
    let mut p = GeoParse {
        source: None,
        acc: ShapeAccum::new(),
        opts: GeoSearchOpts::new(false),
    };
    let mut i = start;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"FROMMEMBER" => {
                if p.source.is_some() {
                    return Err(RespError::new(ERR_SOURCE_CONFLICT));
                }
                let m = args.get(i + 1).ok_or_else(RespError::syntax)?;
                p.source = Some(GeoSource::Member { member: m.clone() });
                i += 2;
            }
            b"FROMLONLAT" => {
                if p.source.is_some() {
                    return Err(RespError::new(ERR_SOURCE_CONFLICT));
                }
                let lon = args.get(i + 1).ok_or_else(RespError::syntax)?;
                let lat = args.get(i + 2).ok_or_else(RespError::syntax)?;
                let xy = parse_long_lat(lon, lat)?;
                p.acc.xy = xy;
                p.source = Some(GeoSource::LonLat);
                i += 3;
            }
            b"BYRADIUS" => {
                if p.acc.kind != 0 {
                    return Err(RespError::new(ERR_SHAPE_CONFLICT));
                }
                let r = args.get(i + 1).ok_or_else(RespError::syntax)?;
                let u = args.get(i + 2).ok_or_else(RespError::syntax)?;
                p.acc.radius = parse_radius(r)?;
                p.acc.conversion =
                    parse_geo_unit(u).ok_or_else(|| RespError::new(ERR_INVALID_UNIT))?;
                p.acc.kind = 1;
                i += 3;
            }
            b"BYBOX" => {
                if p.acc.kind != 0 {
                    return Err(RespError::new(ERR_SHAPE_CONFLICT));
                }
                let w = args.get(i + 1).ok_or_else(RespError::syntax)?;
                let h = args.get(i + 2).ok_or_else(RespError::syntax)?;
                let u = args.get(i + 3).ok_or_else(RespError::syntax)?;
                p.acc.width = parse_radius(w)?;
                p.acc.height = parse_radius(h)?;
                p.acc.conversion =
                    parse_geo_unit(u).ok_or_else(|| RespError::new(ERR_INVALID_UNIT))?;
                p.acc.kind = 2;
                i += 4;
            }
            b"ASC" | b"DESC" => {
                if p.opts.sorting != SortType::Unsorted {
                    return Err(RespError::new(ERR_ASC_DESC));
                }
                p.opts.sorting = if args[i].eq_ignore_ascii_case(b"ASC") {
                    SortType::Asc
                } else {
                    SortType::Desc
                };
                i += 1;
            }
            b"COUNT" => {
                let c = args.get(i + 1).ok_or_else(RespError::syntax)?;
                p.opts.count = parse_count(c, None)?;
                i += 2;
            }
            b"WITHCOORD" => {
                p.opts.withcoord = true;
                i += 1;
            }
            b"WITHDIST" => {
                p.opts.withdist = true;
                i += 1;
            }
            b"WITHHASH" => {
                p.opts.withhash = true;
                i += 1;
            }
            b"STOREDIST" if is_store => {
                p.opts.store = GeoStoreType::StoreDist;
                i += 1;
            }
            _ => return Err(RespError::syntax()),
        }
    }
    Ok(p)
}

fn to_ascii_geohash(score: f64) -> Option<String> {
    const ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let hash = GeoHashBits {
        bits: score as u64,
        step: GEO_STEP_MAX,
    };
    let xy = geohash::decode_to_long_lat(hash)?;
    // Re-encode at the max step so the string matches Redis's 11-char output.
    // Redis's GEOHASH re-encodes against the legacy ranges (-90..90 latitude),
    // not the WGS84 projection used for the stored score.
    let long_range = GeoHashRange {
        min: -180.0,
        max: 180.0,
    };
    let lat_range = GeoHashRange {
        min: -90.0,
        max: 90.0,
    };
    let hash = geohash::encode(&long_range, &lat_range, xy[0], xy[1], GEO_STEP_MAX)?;
    let bits = hash.bits;
    let mut out = [0u8; 11];
    for (i, slot) in out.iter_mut().enumerate() {
        let idx = if i == 10 {
            0
        } else {
            (bits >> (52 - ((i + 1) * 5))) % 32
        };
        *slot = ALPHABET[idx as usize];
    }
    Some(String::from_utf8_lossy(&out).into_owned())
}

fn sort_if_needed(ga: &mut Vec<GeoPoint>, sorting: SortType, count: u64) {
    if sorting == SortType::Unsorted {
        if count > 0 && (ga.len() as u64) > count {
            ga.truncate(count as usize);
        }
        return;
    }
    let asc = sorting == SortType::Asc;
    if count > 0 {
        let count = (count as usize).min(ga.len());
        ga.sort_by(|a, b| {
            if asc {
                a.dist.total_cmp(&b.dist)
            } else {
                b.dist.total_cmp(&a.dist)
            }
        });
        ga.truncate(count);
    } else {
        ga.sort_by(|a, b| {
            if asc {
                a.dist.total_cmp(&b.dist)
            } else {
                b.dist.total_cmp(&a.dist)
            }
        });
    }
}

/// Shared search + reply for GEOSEARCH/GEOSEARCHSTORE/GEORADIUS family.
///
/// `dest` is the arg index of a STORE destination, or `None` for the read
/// variants. Multi-shard STORE: the shard owning the source resolves and
/// searches; the destination-only shard runs the exec with an unowned source
/// and this function is never reached there.
fn empty_store(ctx: &mut OpContext, dest_idx: usize) -> CmdResult {
    if ctx.owned_keys.contains(&dest_idx) {
        ctx.db.remove(&ctx.args[dest_idx]);
        CmdResult::Ok(integer(0))
    } else {
        CmdResult::deferred_store(ctx.args[dest_idx].clone(), None, integer(0))
    }
}

/// Empty search result: read variants reply an empty array; STORE variants
/// delete the destination and reply `0` (live redis-server semantics, which
/// the reference's `geo_family_test.cc` diverges from for a missing source).
fn empty_or_store(ctx: &mut OpContext, dest: Option<usize>) -> CmdResult {
    match dest {
        Some(dest_idx) => empty_store(ctx, dest_idx),
        None => CmdResult::Ok(RespValue::Array(vec![])),
    }
}

fn geo_search_store_generic(
    ctx: &mut OpContext,
    key: &[u8],
    shape_in: &GeoShape,
    source: &GeoSource,
    dest: Option<usize>,
    opts: &GeoSearchOpts,
) -> CmdResult {
    let mut shape = *shape_in;
    match source {
        GeoSource::Member { member } => {
            let score = match ctx.db.find(key, ctx.now_ms) {
                Some(PrimeValue::ZSet(z)) => z.score(member),
                Some(_) => return CmdResult::Err(RespError::wrong_type()),
                None => return empty_or_store(ctx, dest),
            };
            match score {
                Some(score) => {
                    let xy = geohash::decode_to_long_lat(GeoHashBits {
                        bits: score as u64,
                        step: GEO_STEP_MAX,
                    });
                    match xy {
                        Some(xy) => match &mut shape {
                            GeoShape::Circle { xy: c, .. } | GeoShape::Rect { xy: c, .. } => {
                                *c = xy;
                            }
                        },
                        None => return empty_or_store(ctx, dest),
                    }
                }
                None => return CmdResult::Err(RespError::new(ERR_MEMBER_NOT_FOUND)),
            }
        }
        GeoSource::LonLat => match ctx.db.find(key, ctx.now_ms) {
            Some(PrimeValue::ZSet(_)) => {}
            Some(_) => return CmdResult::Err(RespError::wrong_type()),
            None => return empty_or_store(ctx, dest),
        },
    }
    let Some(PrimeValue::ZSet(z)) = ctx.db.find(key, ctx.now_ms) else {
        return empty_or_store(ctx, dest);
    };

    // Gather candidates from the geohash areas covering the search shape.
    let radius = geohash::calculate_areas_by_shape(&shape);
    let areas = [
        radius.hash,
        radius.neighbors.north,
        radius.neighbors.south,
        radius.neighbors.east,
        radius.neighbors.west,
        radius.neighbors.north_east,
        radius.neighbors.north_west,
        radius.neighbors.south_east,
        radius.neighbors.south_west,
    ];
    let mut ga: Vec<GeoPoint> = Vec::new();
    let limit = if opts.any { opts.count } else { 0 };
    let mut last: Option<(u64, u8)> = None;
    'outer: for h in areas {
        if geohash::hash_is_zero(h) {
            continue;
        }
        if let Some((bits, step)) = last {
            // Adjacent neighbors can be identical for huge radii; skip duplicates.
            if bits == h.bits && step == h.step {
                continue;
            }
        }
        last = Some((h.bits, h.step));
        let (min, max) = geohash::scores_of_geo_hash_box(h);
        let min = min as f64;
        let max = max as f64;
        for (member, score) in z.range_by_score_filtered(|s| s >= min && s < max, false, None) {
            if let Some((xy, dist)) = geohash::within_shape(&shape, score) {
                ga.push(GeoPoint {
                    longitude: xy[0],
                    latitude: xy[1],
                    dist,
                    score,
                    member,
                });
                if limit > 0 && ga.len() >= limit as usize {
                    break 'outer;
                }
            }
        }
    }
    sort_if_needed(&mut ga, opts.sorting, opts.count);

    let conversion = opts.conversion;
    if let Some(dest_idx) = dest {
        let count = ga.len() as i64;
        if ga.is_empty() {
            empty_store(ctx, dest_idx)
        } else {
            let store_dist = opts.store == GeoStoreType::StoreDist;
            let mut zs = ZSet::new();
            for p in &ga {
                let score = if store_dist {
                    p.dist / conversion
                } else {
                    p.score
                };
                zs.insert(p.member.clone(), score);
            }
            if ctx.owned_keys.contains(&dest_idx) {
                ctx.db.insert(&ctx.args[dest_idx], PrimeValue::ZSet(zs));
                CmdResult::Ok(integer(count))
            } else {
                CmdResult::deferred_store(
                    ctx.args[dest_idx].clone(),
                    Some(PrimeValue::ZSet(zs)),
                    integer(count),
                )
            }
        }
    } else {
        let with = opts.has_with_statement();
        let record_size = 1
            + usize::from(opts.withdist)
            + usize::from(opts.withhash)
            + usize::from(opts.withcoord);
        let mut out = Vec::with_capacity(ga.len());
        for p in ga {
            if with {
                let mut rec = Vec::with_capacity(record_size);
                rec.push(RespValue::Bulk(p.member.as_bytes().to_vec()));
                if opts.withdist {
                    rec.push(bulk(format_double(p.dist / conversion).into_bytes()));
                }
                if opts.withhash {
                    rec.push(bulk(format_double(p.score).into_bytes()));
                }
                if opts.withcoord {
                    rec.push(RespValue::Array(vec![
                        bulk(format_double(p.longitude).into_bytes()),
                        bulk(format_double(p.latitude).into_bytes()),
                    ]));
                }
                out.push(RespValue::Array(rec));
            } else {
                out.push(RespValue::Bulk(p.member.as_bytes().to_vec()));
            }
        }
        CmdResult::Ok(RespValue::Array(out))
    }
}

// ---------------------------------------------------------------------------
// Executors
// ---------------------------------------------------------------------------

fn exec_geoadd(ctx: &mut OpContext) -> CmdResult {
    let key_idx = ctx.owned_keys[0];
    let key = &ctx.args[key_idx];
    let mut i = key_idx + 1;
    let (mut xx, mut nx, mut ch) = (false, false, false);
    loop {
        if i >= ctx.args.len() {
            return CmdResult::Err(RespError::syntax());
        }
        match ctx.args[i].to_ascii_uppercase().as_slice() {
            b"XX" => xx = true,
            b"NX" => nx = true,
            b"CH" => ch = true,
            _ => break,
        }
        i += 1;
    }
    let rest = &ctx.args[i..];
    if rest.is_empty() || !rest.len().is_multiple_of(3) {
        return CmdResult::Err(RespError::syntax());
    }
    if nx && xx {
        return CmdResult::Err(RespError::new(ERR_NX_XX));
    }
    let mut members: Vec<(f64, CompactString)> = Vec::with_capacity(rest.len() / 3);
    for c in rest.chunks(3) {
        let xy = match parse_geoadd_longlat(&c[0], &c[1], &c[2]) {
            Ok(v) => v,
            Err(e) => return CmdResult::Err(e),
        };
        let hash = geohash::encode_wgs84(xy[0], xy[1], GEO_STEP_MAX).expect("valid coords");
        members.push((
            geohash::align_52_bits(hash) as f64,
            CompactString::from_bytes(&c[2]),
        ));
    }

    let z = match ctx.db.find_mut(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => z,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => {
            ctx.db.insert(key, PrimeValue::ZSet(ZSet::new()));
            match ctx.db.find_mut(key, ctx.now_ms) {
                Some(PrimeValue::ZSet(z)) => z,
                _ => unreachable!("zset was just inserted"),
            }
        }
    };
    let (mut added, mut changed) = (0i64, 0i64);
    for (score, member) in members {
        let existing = z.score(member.as_bytes());
        let should_add = match existing {
            Some(_) => !nx,
            None => !xx,
        };
        if !should_add {
            continue;
        }
        let was_new = existing.is_none();
        z.insert(member, score);
        if was_new {
            added += 1;
            changed += 1;
        } else if existing != Some(score) {
            changed += 1;
        }
    }
    if z.is_empty() {
        ctx.db.remove(key);
    }
    CmdResult::Ok(integer(if ch { changed } else { added }))
}

fn exec_geohash(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[1];
    let members = &ctx.args[2..];
    let z = match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => Some(z),
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => None,
    };
    let mut out = Vec::with_capacity(members.len());
    for m in members {
        let encoded = z.and_then(|z| z.score(m)).and_then(to_ascii_geohash);
        match encoded {
            Some(s) => out.push(RespValue::Bulk(s.into_bytes())),
            None => out.push(RespValue::Nil),
        }
    }
    CmdResult::Ok(RespValue::Array(out))
}

fn exec_geopos(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[1];
    let members = &ctx.args[2..];
    let z = match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => Some(z),
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => None,
    };
    let mut out = Vec::with_capacity(members.len());
    for m in members {
        let xy = z.and_then(|z| z.score(m)).and_then(|s| {
            geohash::decode_to_long_lat(GeoHashBits {
                bits: s as u64,
                step: GEO_STEP_MAX,
            })
        });
        match xy {
            Some([lon, lat]) => out.push(RespValue::Array(vec![
                bulk(format_double(lon).into_bytes()),
                bulk(format_double(lat).into_bytes()),
            ])),
            None => out.push(RespValue::Nil),
        }
    }
    CmdResult::Ok(RespValue::Array(out))
}

fn exec_geodist(ctx: &mut OpContext) -> CmdResult {
    let key = &ctx.args[1];
    let mut conversion = 1.0;
    if let Some(unit) = ctx.args.get(4) {
        conversion = match parse_geo_unit(unit) {
            Some(c) => c,
            None => return CmdResult::Err(RespError::new(ERR_INVALID_UNIT)),
        };
    }
    if ctx.args.len() > 5 {
        return CmdResult::Err(RespError::syntax());
    }
    let z = match ctx.db.find(key, ctx.now_ms) {
        Some(PrimeValue::ZSet(z)) => z,
        Some(_) => return CmdResult::Err(RespError::wrong_type()),
        None => return CmdResult::Ok(RespValue::Nil),
    };
    let (Some(s1), Some(s2)) = (z.score(&ctx.args[2]), z.score(&ctx.args[3])) else {
        return CmdResult::Ok(RespValue::Nil);
    };
    let Some(xy1) = geohash::decode_to_long_lat(GeoHashBits {
        bits: s1 as u64,
        step: GEO_STEP_MAX,
    }) else {
        return CmdResult::Ok(RespValue::Nil);
    };
    let Some(xy2) = geohash::decode_to_long_lat(GeoHashBits {
        bits: s2 as u64,
        step: GEO_STEP_MAX,
    }) else {
        return CmdResult::Ok(RespValue::Nil);
    };
    let d = geohash::haversine(xy1[0], xy1[1], xy2[0], xy2[1]);
    CmdResult::Ok(bulk(format_double(d / conversion).into_bytes()))
}

fn exec_geosearch(ctx: &mut OpContext) -> CmdResult {
    geosearch_common(ctx, false)
}

fn exec_geosearchstore(ctx: &mut OpContext) -> CmdResult {
    geosearch_common(ctx, true)
}

fn geosearch_common(ctx: &mut OpContext, is_store: bool) -> CmdResult {
    // GEOSEARCHSTORE's first key is the destination; the source is second.
    let source_idx = if is_store { 2 } else { 1 };
    if !ctx.owned_keys.contains(&source_idx) {
        // Destination-only shard of a multi-shard STORE: contributes nothing.
        return CmdResult::Ok(RespValue::Array(vec![]));
    }
    let start = if is_store { 3 } else { 2 };
    let parsed = match parse_geosearch_opts(ctx.args, start, is_store) {
        Ok(p) => p,
        Err(e) => return CmdResult::Err(e),
    };
    if parsed.source.is_none() || parsed.acc.kind == 0 {
        return CmdResult::Err(RespError::syntax());
    }
    let mut opts = parsed.opts;
    opts.conversion = parsed.acc.conversion;
    if opts.count == 0 {
        return CmdResult::Err(RespError::new(ERR_COUNT));
    }
    if is_store && opts.has_with_statement() {
        return CmdResult::Err(RespError::new(ERR_STORE_COMPAT_SEARCHSTORE));
    }
    opts.count = if opts.count == u64::MAX {
        0
    } else {
        opts.count
    };
    let shape = parsed.acc.into_shape().expect("shape kind checked");
    let source = parsed.source.expect("source checked");
    let source_key = ctx.args[source_idx].clone();
    let dest = if is_store { Some(1) } else { None };
    geo_search_store_generic(ctx, &source_key, &shape, &source, dest, &opts)
}

fn exec_georadius(ctx: &mut OpContext) -> CmdResult {
    georadius_common(ctx, false, false)
}

fn exec_georadius_ro(ctx: &mut OpContext) -> CmdResult {
    georadius_common(ctx, false, true)
}

fn exec_georadiusbymember(ctx: &mut OpContext) -> CmdResult {
    georadius_common(ctx, true, false)
}

fn exec_georadiusbymember_ro(ctx: &mut OpContext) -> CmdResult {
    georadius_common(ctx, true, true)
}

fn georadius_common(ctx: &mut OpContext, by_member: bool, read_only: bool) -> CmdResult {
    if !ctx.owned_keys.contains(&1) {
        // Destination-only shard of a multi-shard STORE: contributes nothing.
        return CmdResult::Ok(RespValue::Array(vec![]));
    }
    let (xy, shape_start) = if by_member {
        ([0.0, 0.0], 3)
    } else {
        let xy = match parse_long_lat(&ctx.args[2], &ctx.args[3]) {
            Ok(v) => v,
            Err(e) => return CmdResult::Err(e),
        };
        (xy, 4)
    };
    let radius = match parse_radius(&ctx.args[shape_start]) {
        Ok(v) => v,
        Err(e) => return CmdResult::Err(e),
    };
    let Some(conversion) = parse_geo_unit(&ctx.args[shape_start + 1]) else {
        return CmdResult::Err(RespError::new(ERR_INVALID_UNIT));
    };
    let mut opts = GeoSearchOpts::new(!read_only);
    opts.conversion = conversion;
    opts.count_err = if by_member {
        Some("ERR syntax error")
    } else {
        None
    };
    if let Err(e) = parse_geo_result_options(ctx.args, shape_start + 2, &mut opts) {
        return CmdResult::Err(e);
    }
    if opts.count == 0 {
        return CmdResult::Err(RespError::new(ERR_COUNT));
    }
    if opts.has_with_statement() && opts.store != GeoStoreType::NoStore {
        return CmdResult::Err(RespError::new(if by_member {
            ERR_STORE_COMPAT_BYMEMBER
        } else {
            ERR_STORE_COMPAT_RADIUS
        }));
    }
    opts.count = if opts.count == u64::MAX {
        0
    } else {
        opts.count
    };
    let source = if by_member {
        GeoSource::Member {
            member: ctx.args[2].clone(),
        }
    } else {
        GeoSource::LonLat
    };
    let shape = GeoShape::Circle {
        xy,
        radius,
        conversion,
    };
    let dest = if opts.store_key_nonzero {
        Some(opts.store_key)
    } else {
        None
    };
    geo_search_store_generic(ctx, &ctx.args[1], &shape, &source, dest, &opts)
}

// ---------------------------------------------------------------------------
// Merge functions for multi-shard STORE (mirror merge_sort: forward the part
// that owns the source key; destination-only parts contribute placeholders).
// ---------------------------------------------------------------------------

fn merge_geosearchstore(
    parts: &[ShardPart],
    _args: &[Vec<u8>],
    keys: &[usize],
    _now_ms: u64,
) -> CmdResult {
    for p in parts {
        if let CmdResult::Err(e) = &p.result {
            return CmdResult::Err(e.clone());
        }
        if p.owned_key_idxs.contains(&keys[1]) {
            return p.result.clone();
        }
    }
    parts[0].result.clone()
}

fn merge_geo_radius(
    parts: &[ShardPart],
    _args: &[Vec<u8>],
    keys: &[usize],
    _now_ms: u64,
) -> CmdResult {
    for p in parts {
        if let CmdResult::Err(e) = &p.result {
            return CmdResult::Err(e.clone());
        }
        if p.owned_key_idxs.contains(&keys[0]) {
            return p.result.clone();
        }
    }
    parts[0].result.clone()
}

// ---------------------------------------------------------------------------
// Command registration
// ---------------------------------------------------------------------------

pub static CMD_GEOADD: Command = Command {
    name: "GEOADD",
    arity: -5,
    flags: FLAG_WRITE | FLAG_DENYOOM,
    key_range: KeyRange::ONE,
    exec: exec_geoadd,
    merge: None,
};

pub static CMD_GEOHASH: Command = Command {
    name: "GEOHASH",
    arity: -2,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_geohash,
    merge: None,
};

pub static CMD_GEOPOS: Command = Command {
    name: "GEOPOS",
    arity: -2,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_geopos,
    merge: None,
};

pub static CMD_GEODIST: Command = Command {
    name: "GEODIST",
    arity: -4,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_geodist,
    merge: None,
};

pub static CMD_GEOSEARCH: Command = Command {
    name: "GEOSEARCH",
    arity: -7,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_geosearch,
    merge: None,
};

pub static CMD_GEOSEARCHSTORE: Command = Command {
    name: "GEOSEARCHSTORE",
    arity: -4,
    flags: FLAG_WRITE | FLAG_NO_REDUCED,
    key_range: KeyRange::TWO,
    exec: exec_geosearchstore,
    merge: Some(merge_geosearchstore),
};

pub static CMD_GEORADIUS: Command = Command {
    name: "GEORADIUS",
    arity: -6,
    flags: FLAG_WRITE | FLAG_MOVABLEKEYS | FLAG_NO_REDUCED,
    key_range: KeyRange::ONE,
    exec: exec_georadius,
    merge: Some(merge_geo_radius),
};

pub static CMD_GEORADIUS_RO: Command = Command {
    name: "GEORADIUS_RO",
    arity: -6,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_georadius_ro,
    merge: None,
};

pub static CMD_GEORADIUSBYMEMBER: Command = Command {
    name: "GEORADIUSBYMEMBER",
    arity: -5,
    flags: FLAG_WRITE | FLAG_MOVABLEKEYS | FLAG_NO_REDUCED,
    key_range: KeyRange::ONE,
    exec: exec_georadiusbymember,
    merge: Some(merge_geo_radius),
};

pub static CMD_GEORADIUSBYMEMBER_RO: Command = Command {
    name: "GEORADIUSBYMEMBER_RO",
    arity: -5,
    flags: FLAG_READONLY,
    key_range: KeyRange::ONE,
    exec: exec_georadiusbymember_ro,
    merge: None,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DbSlice;

    fn dispatch_at(db: &mut DbSlice, now_ms: u64, argv: &[Vec<u8>]) -> CmdResult {
        let (exec, first_key_idx, owned): (fn(&mut OpContext) -> CmdResult, usize, Vec<usize>) =
            match argv[0].to_ascii_uppercase().as_slice() {
                b"GEOADD" => (exec_geoadd, 1, vec![1]),
                b"GEOHASH" => (exec_geohash, 1, vec![1]),
                b"GEOPOS" => (exec_geopos, 1, vec![1]),
                b"GEODIST" => (exec_geodist, 1, vec![1]),
                b"GEOSEARCH" => (exec_geosearch, 1, vec![1]),
                b"GEOSEARCHSTORE" => (exec_geosearchstore, 1, vec![1, 2]),
                b"GEORADIUS" => (exec_georadius, 1, vec![1]),
                b"GEORADIUS_RO" => (exec_georadius_ro, 1, vec![1]),
                b"GEORADIUSBYMEMBER" => (exec_georadiusbymember, 1, vec![1]),
                b"GEORADIUSBYMEMBER_RO" => (exec_georadiusbymember_ro, 1, vec![1]),
                _ => panic!("unhandled command {:?}", argv[0]),
            };
        let mut ctx = OpContext {
            db,
            args: argv,
            owned_keys: &owned,
            first_key_idx,
            now_ms,
        };
        let r = (exec)(&mut ctx);
        // Apply deferred stores so STORE results are visible to later commands.
        match r {
            CmdResult::DeferredStore { key, value, reply } => {
                apply_store(db, &key, value);
                CmdResult::Ok(reply)
            }
            CmdResult::DeferredStores { stores, reply } => {
                for (key, value, _exp, _sticky) in stores {
                    apply_store(db, &key, value);
                }
                CmdResult::Ok(reply)
            }
            other => other,
        }
    }

    fn apply_store(db: &mut DbSlice, key: &[u8], value: Option<PrimeValue>) {
        match value {
            Some(v) => db.insert(key, v),
            None => {
                db.remove(key);
            }
        }
    }

    fn int(r: CmdResult) -> i64 {
        match r.into_resp_value() {
            RespValue::Integer(v) => v,
            o => panic!("expected integer, got {o:?}"),
        }
    }

    fn err(r: CmdResult) -> String {
        match r.into_resp_value() {
            RespValue::Error(e) => e,
            o => panic!("expected error, got {o:?}"),
        }
    }

    /// Flat array of Bulk values; nested arrays are rendered as `[a, b]`.
    fn flat(r: CmdResult) -> Vec<String> {
        match r.into_resp_value() {
            RespValue::Array(v) => v
                .into_iter()
                .map(|x| match x {
                    RespValue::Bulk(b) => String::from_utf8_lossy(&b).into_owned(),
                    RespValue::Nil => String::from("(nil)"),
                    RespValue::Array(a) => format!(
                        "[{}]",
                        a.iter()
                            .map(|y| match y {
                                RespValue::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
                                RespValue::Nil => String::from("(nil)"),
                                o => format!("{o:?}"),
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    o => panic!("unexpected element {o:?}"),
                })
                .collect(),
            o => panic!("expected array, got {o:?}"),
        }
    }

    /// Members of the zset at `key` in ascending order.
    fn zmembers(db: &mut DbSlice, key: &str) -> Vec<String> {
        match db.find(key.as_bytes(), 0) {
            Some(PrimeValue::ZSet(z)) => z.iter().map(|(m, _)| m.to_string()).collect(),
            o => panic!("expected zset at {key}, got {o:?}"),
        }
    }

    /// Members and scores of the zset at `key`, `format_double`-formatted.
    fn zscores(db: &mut DbSlice, key: &str) -> Vec<String> {
        match db.find(key.as_bytes(), 0) {
            Some(PrimeValue::ZSet(z)) => z
                .iter()
                .flat_map(|(m, s)| vec![m.to_string(), format_double(s)])
                .collect(),
            o => panic!("expected zset at {key}, got {o:?}"),
        }
    }

    fn exists(db: &mut DbSlice, key: &str) -> bool {
        db.find(key.as_bytes(), 0).is_some()
    }

    fn db() -> DbSlice {
        DbSlice::new(0)
    }

    #[test]
    fn geoadd_geohash() {
        let mut d = db();
        assert_eq!(
            int(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOADD",
                    "Sicily",
                    "13.361389",
                    "38.115556",
                    "Palermo",
                    "15.087269",
                    "37.502669",
                    "Catania"
                ])
            )),
            2
        );
        assert_eq!(
            int(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOADD",
                    "Sicily",
                    "13.361389",
                    "38.115556",
                    "Palermo",
                    "15.087269",
                    "37.502669",
                    "Catania"
                ])
            )),
            0
        );
        assert_eq!(
            flat(dispatch_at(
                &mut d,
                0,
                &b_args(&["GEOHASH", "Sicily", "Palermo", "Catania"])
            )),
            vec!["sqc8b49rny0", "sqdtr74hyu0"]
        );
    }

    #[test]
    fn geoadd_options() {
        let mut d = db();
        dispatch_at(
            &mut d,
            0,
            &b_args(&[
                "GEOADD",
                "Sicily",
                "13.361389",
                "38.115556",
                "Palermo",
                "15.087269",
                "37.502669",
                "Catania",
            ]),
        );
        // XX: update Palermo, skip Messina (new).
        assert_eq!(
            int(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOADD",
                    "Sicily",
                    "XX",
                    "15.361389",
                    "38.115556",
                    "Palermo",
                    "15.554167",
                    "38.193611",
                    "Messina"
                ])
            )),
            0
        );
        let pos = flat(dispatch_at(
            &mut d,
            0,
            &b_args(&["GEOPOS", "Sicily", "Palermo", "Messina"]),
        ));
        assert_eq!(pos[0], "[15.361389219760895, 38.1155563954963]");
        assert_eq!(pos[1], "(nil)");
        // NX: add Syracuse, skip Palermo (existing).
        assert_eq!(
            int(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOADD",
                    "Sicily",
                    "NX",
                    "18.361389",
                    "38.115556",
                    "Palermo",
                    "15.2875",
                    "37.069167",
                    "Syracuse"
                ])
            )),
            1
        );
        // CH: update Palermo, add Marsala -> 2.
        assert_eq!(
            int(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOADD",
                    "Sicily",
                    "CH",
                    "18.361389",
                    "38.115556",
                    "Palermo",
                    "12.434167",
                    "37.798056",
                    "Marsala"
                ])
            )),
            2
        );
        // XX + NX conflict.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOADD",
                    "Sicily",
                    "XX",
                    "NX",
                    "14.75",
                    "36.933333",
                    "Ragusa"
                ])
            )),
            "ERR XX and NX options at the same time are not compatible"
        );
        // Bad arg count.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&["GEOADD", "Sicily", "14.75", "36.933333", "Ragusa", "10.23"])
            )),
            "ERR syntax error"
        );
        // Bad coordinates.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&["GEOADD", "Sicily", "200", "1", "m"])
            )),
            "ERR invalid longitude,latitude pair 200,1,m"
        );
    }

    #[test]
    fn geopos_missing() {
        let mut d = db();
        dispatch_at(
            &mut d,
            0,
            &b_args(&["GEOADD", "Sicily", "13.361389", "38.115556", "Palermo"]),
        );
        let pos = flat(dispatch_at(
            &mut d,
            0,
            &b_args(&["GEOPOS", "Sicily", "Palermo", "NonExisting"]),
        ));
        assert_eq!(pos[0], "[13.361389338970184, 38.1155563954963]");
        assert_eq!(pos[1], "(nil)");
    }

    #[test]
    fn geopos_wrong_type() {
        let mut d = db();
        let r = dispatch_at(&mut d, 0, &b_args(&["GEOPOS", "x", "m"]));
        // Missing key -> all NIL (no WRONGTYPE).
        assert!(matches!(r, CmdResult::Ok(RespValue::Array(_))));
    }

    /// Bulk-string payload as a String (GEODIST replies a single number).
    fn bulk_str(r: CmdResult) -> String {
        match r.into_resp_value() {
            RespValue::Bulk(b) => String::from_utf8_lossy(&b).into_owned(),
            o => panic!("expected bulk, got {o:?}"),
        }
    }

    #[test]
    fn geodist() {
        let mut d = db();
        dispatch_at(
            &mut d,
            0,
            &b_args(&[
                "GEOADD",
                "Sicily",
                "13.361389",
                "38.115556",
                "Palermo",
                "15.087269",
                "37.502669",
                "Catania",
            ]),
        );
        let d1 = bulk_str(dispatch_at(
            &mut d,
            0,
            &b_args(&["GEODIST", "Sicily", "Palermo", "Catania"]),
        ));
        assert!(
            (d1.parse::<f64>().unwrap() - 166_274.151_569_600_33).abs() < 1e-6,
            "got {d1:?}"
        );
        let km = bulk_str(dispatch_at(
            &mut d,
            0,
            &b_args(&["GEODIST", "Sicily", "Palermo", "Catania", "km"]),
        ));
        assert!(
            (km.parse::<f64>().unwrap() - 166.274_151_569_600_32).abs() < 1e-9,
            "got {km:?}"
        );
        // Unit scaling (reference `GeoDist`: MI / FT).
        let mi = bulk_str(dispatch_at(
            &mut d,
            0,
            &b_args(&["GEODIST", "Sicily", "Palermo", "Catania", "MI"]),
        ));
        assert!(
            (mi.parse::<f64>().unwrap() - 103.318_224_594_927_33).abs() < 1e-9,
            "got {mi:?}"
        );
        let ft = bulk_str(dispatch_at(
            &mut d,
            0,
            &b_args(&["GEODIST", "Sicily", "Palermo", "Catania", "FT"]),
        ));
        assert!(
            (ft.parse::<f64>().unwrap() - 545_518.869_979_003_7).abs() < 1e-6,
            "got {ft:?}"
        );
        // Missing members -> nil.
        let r = dispatch_at(&mut d, 0, &b_args(&["GEODIST", "Sicily", "Foo", "Bar"]));
        assert!(matches!(r, CmdResult::Ok(RespValue::Nil)));
        // Bad unit.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&["GEODIST", "Sicily", "Palermo", "Catania", "parsecs"])
            )),
            "ERR unsupported unit provided. please use M, KM, FT, MI"
        );
    }

    fn geoadd_europe(d: &mut DbSlice) {
        dispatch_at(
            d,
            0,
            &b_args(&[
                "GEOADD",
                "Europe",
                "13.4050",
                "52.5200",
                "Berlin",
                "3.7038",
                "40.4168",
                "Madrid",
                "9.1427",
                "38.7369",
                "Lisbon",
                "2.3522",
                "48.8566",
                "Paris",
                "16.3738",
                "48.2082",
                "Vienna",
                "4.8952",
                "52.3702",
                "Amsterdam",
                "10.7522",
                "59.9139",
                "Oslo",
                "23.7275",
                "37.9838",
                "Athens",
                "19.0402",
                "47.4979",
                "Budapest",
                "6.2603",
                "53.3498",
                "Dublin",
            ]),
        );
    }

    #[test]
    fn geosearch_radius() {
        let mut d = db();
        geoadd_europe(&mut d);
        let r = dispatch_at(
            &mut d,
            0,
            &b_args(&[
                "GEOSEARCH",
                "Europe",
                "FROMLONLAT",
                "13.4050",
                "52.5200",
                "BYRADIUS",
                "500",
                "KM",
                "WITHCOORD",
                "WITHDIST",
                "WITHHASH",
            ]),
        );
        let rows = flat(r);
        assert_eq!(rows.len(), 2);
        assert!(
            rows[0].contains("Berlin") && rows[0].contains("3673983950397063"),
            "got {:?}",
            rows[0]
        );
        assert!(
            rows[1].contains("Dublin") && rows[1].contains("3678981558208417"),
            "got {:?}",
            rows[1]
        );
    }

    #[test]
    fn geosearch_missing_source_and_member() {
        let mut d = db();
        geoadd_europe(&mut d);
        // Missing key -> empty array (read variant).
        let r = dispatch_at(
            &mut d,
            0,
            &b_args(&[
                "GEOSEARCH",
                "invalid_key",
                "FROMMEMBER",
                "Madrid",
                "BYRADIUS",
                "700",
                "KM",
            ]),
        );
        assert!(matches!(r, CmdResult::Ok(RespValue::Array(v)) if v.is_empty()));
        // Missing member -> error.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOSEARCH",
                    "Europe",
                    "FROMMEMBER",
                    "invalid_member",
                    "BYRADIUS",
                    "700",
                    "KM"
                ])
            )),
            "ERR could not decode requested zset member"
        );
        // No results within box.
        let r = dispatch_at(
            &mut d,
            0,
            &b_args(&[
                "GEOSEARCH",
                "America",
                "FROMLONLAT",
                "13.4050",
                "52.5200",
                "BYBOX",
                "1000",
                "1000",
                "KM",
            ]),
        );
        assert!(matches!(r, CmdResult::Ok(RespValue::Array(v)) if v.is_empty()));
        // Out-of-range lon -> empty.
        let r = dispatch_at(
            &mut d,
            0,
            &b_args(&[
                "GEOSEARCH",
                "Europe",
                "FROMLONLAT",
                "130.4050",
                "52.5200",
                "BYBOX",
                "10",
                "10",
                "KM",
            ]),
        );
        assert!(matches!(r, CmdResult::Ok(RespValue::Array(v)) if v.is_empty()));
    }

    #[test]
    fn geosearch_nan_coord() {
        let mut d = db();
        dispatch_at(
            &mut d,
            0,
            &b_args(&[
                "GEOADD", "cities", "13.361", "38.115", "Palermo", "15.087", "37.502", "Catania",
            ]),
        );
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOSEARCH",
                    "cities",
                    "FROMLONLAT",
                    "15",
                    "NaN",
                    "BYRADIUS",
                    "200",
                    "km"
                ])
            )),
            "ERR invalid longitude,latitude pair 15,nan"
        );
    }

    #[test]
    fn geosearch_mandatory_and_errors() {
        let mut d = db();
        geoadd_europe(&mut d);
        // COUNT 0.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOSEARCH",
                    "Europe",
                    "FROMLONLAT",
                    "13.4050",
                    "52.5200",
                    "BYRADIUS",
                    "500",
                    "KM",
                    "COUNT",
                    "0"
                ])
            )),
            "ERR COUNT must be > 0"
        );
        // COUNT non-numeric.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOSEARCH",
                    "Europe",
                    "FROMLONLAT",
                    "13.4050",
                    "52.5200",
                    "BYRADIUS",
                    "500",
                    "KM",
                    "COUNT",
                    "abc"
                ])
            )),
            "ERR value is not an integer or out of range"
        );
        // Missing BY*.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&["GEOSEARCH", "Europe", "FROMLONLAT", "13.4050", "52.5200"])
            )),
            "ERR syntax error"
        );
        // Missing FROM*.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&["GEOSEARCH", "Europe", "BYRADIUS", "500", "KM"])
            )),
            "ERR syntax error"
        );
        // Conflicting source / shape.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOSEARCH",
                    "Europe",
                    "FROMMEMBER",
                    "Madrid",
                    "FROMLONLAT",
                    "1",
                    "1",
                    "BYRADIUS",
                    "500",
                    "KM"
                ])
            )),
            "ERR FROMMEMBER and FROMLONLAT options at the same time are not compatible"
        );
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOSEARCH",
                    "Europe",
                    "FROMMEMBER",
                    "Madrid",
                    "BYRADIUS",
                    "500",
                    "KM",
                    "BYBOX",
                    "1",
                    "1",
                    "KM"
                ])
            )),
            "ERR BYRADIUS and BYBOX options at the same time are not compatible"
        );
        // ASC+DESC.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOSEARCH",
                    "Europe",
                    "FROMMEMBER",
                    "Madrid",
                    "BYRADIUS",
                    "500",
                    "KM",
                    "ASC",
                    "DESC"
                ])
            )),
            "ERR ASC and DESC options at the same time are not compatible"
        );
        // Trailing junk.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOSEARCH",
                    "Europe",
                    "FROMMEMBER",
                    "Madrid",
                    "BYRADIUS",
                    "500",
                    "KM",
                    "STORE",
                    "x"
                ])
            )),
            "ERR syntax error"
        );
        // Bad unit.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOSEARCH",
                    "Europe",
                    "FROMMEMBER",
                    "Madrid",
                    "BYRADIUS",
                    "500",
                    "parsecs"
                ])
            )),
            "ERR unsupported unit provided. please use M, KM, FT, MI"
        );
    }

    #[test]
    fn geosearchstore_semantics() {
        let mut d = db();
        geoadd_europe(&mut d);
        // Store two hits (defaults to StoreHash scores).
        assert_eq!(
            int(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOSEARCHSTORE",
                    "dst",
                    "Europe",
                    "FROMLONLAT",
                    "13.4050",
                    "52.5200",
                    "BYRADIUS",
                    "500",
                    "KM"
                ])
            )),
            2
        );
        assert_eq!(
            zmembers(&mut d, "dst"),
            vec!["Berlin".to_string(), "Dublin".to_string()]
        );
        // Missing source: delete dest + reply 0.
        assert_eq!(
            int(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOSEARCHSTORE",
                    "dst",
                    "missing",
                    "FROMLONLAT",
                    "0",
                    "0",
                    "BYRADIUS",
                    "10",
                    "km"
                ])
            )),
            0
        );
        assert!(!exists(&mut d, "dst"), "dest should be deleted");
        // STOREDIST stores distance (km).
        assert_eq!(
            int(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOSEARCHSTORE",
                    "dst",
                    "Europe",
                    "FROMLONLAT",
                    "13.4050",
                    "52.5200",
                    "BYRADIUS",
                    "500",
                    "KM",
                    "STOREDIST"
                ])
            )),
            2
        );
        let scores = zscores(&mut d, "dst");
        assert_eq!(scores.len(), 4);
        // Berlin is the search center: its distance is ~0.17 m, not exactly 0,
        // because the stored score is the cell center (Redis behaves likewise).
        assert!(
            (scores[1].parse::<f64>().unwrap()).abs() < 1e-3,
            "got {scores:?}"
        );
        // With-statement incompatibility (WITHDIST wins over COUNT error only if COUNT is valid).
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOSEARCHSTORE",
                    "dst",
                    "Europe",
                    "FROMLONLAT",
                    "13.4050",
                    "52.5200",
                    "BYRADIUS",
                    "500",
                    "KM",
                    "WITHDIST"
                ])
            )),
            "ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
        );
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEOSEARCHSTORE",
                    "dst",
                    "Europe",
                    "FROMLONLAT",
                    "13.4050",
                    "52.5200",
                    "BYRADIUS",
                    "500",
                    "KM",
                    "COUNT",
                    "0",
                    "WITHDIST"
                ])
            )),
            "ERR COUNT must be > 0"
        );
    }

    #[test]
    fn georadius_family() {
        let mut d = db();
        geoadd_europe(&mut d);
        let r = dispatch_at(
            &mut d,
            0,
            &b_args(&[
                "GEORADIUS",
                "Europe",
                "13.4050",
                "52.5200",
                "500",
                "KM",
                "COUNT",
                "3",
                "WITHCOORD",
                "WITHDIST",
            ]),
        );
        let rows = flat(r);
        assert_eq!(rows.len(), 2);
        assert!(rows[0].contains("Berlin"), "got {:?}", rows[0]);
        assert!(rows[1].contains("Dublin"), "got {:?}", rows[1]);
        // DESC flips the order.
        let r = dispatch_at(
            &mut d,
            0,
            &b_args(&[
                "GEORADIUS",
                "Europe",
                "13.4050",
                "52.5200",
                "500",
                "KM",
                "DESC",
                "WITHCOORD",
                "WITHDIST",
            ]),
        );
        let rows = flat(r);
        assert!(rows[0].contains("Dublin"), "got {:?}", rows[0]);
        assert!(rows[1].contains("Berlin"), "got {:?}", rows[1]);
        // Missing key -> empty array.
        let r = dispatch_at(
            &mut d,
            0,
            &b_args(&[
                "GEORADIUS",
                "invalid_key",
                "16.3738",
                "48.2082",
                "900",
                "KM",
            ]),
        );
        assert!(matches!(r, CmdResult::Ok(RespValue::Array(v)) if v.is_empty()));
        // COUNT 0.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUS",
                    "Europe",
                    "13.4050",
                    "52.5200",
                    "500",
                    "KM",
                    "COUNT",
                    "0"
                ])
            )),
            "ERR COUNT must be > 0"
        );
        // Store incompatibility.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUS",
                    "Europe",
                    "13.4050",
                    "52.5200",
                    "500",
                    "KM",
                    "WITHDIST",
                    "STORE",
                    "result"
                ])
            )),
            "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options"
        );
        // RO variants reject STORE / STOREDIST.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUS_RO",
                    "Europe",
                    "13.4050",
                    "52.5200",
                    "900",
                    "KM",
                    "STORE",
                    "store_key"
                ])
            )),
            "ERR syntax error"
        );
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUS_RO",
                    "Europe",
                    "13.4050",
                    "52.5200",
                    "900",
                    "KM",
                    "STOREDIST",
                    "store_key"
                ])
            )),
            "ERR syntax error"
        );
    }

    #[test]
    fn georadiusbymember_family() {
        let mut d = db();
        geoadd_europe(&mut d);
        let r = dispatch_at(
            &mut d,
            0,
            &b_args(&[
                "GEORADIUSBYMEMBER",
                "Europe",
                "Madrid",
                "700",
                "KM",
                "WITHCOORD",
                "WITHDIST",
            ]),
        );
        let rows = flat(r);
        assert_eq!(rows.len(), 2);
        assert!(rows[0].contains("Madrid"), "got {:?}", rows[0]);
        assert!(rows[1].contains("Lisbon"), "got {:?}", rows[1]);
        // Missing member -> error.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUSBYMEMBER",
                    "Europe",
                    "invalid_mem",
                    "900",
                    "KM",
                    "STORE",
                    "store_key"
                ])
            )),
            "ERR could not decode requested zset member"
        );
        // Missing key -> delete dest + reply 0 (live Redis semantics; the
        // reference's `geo_family_test.cc` diverges and expects an empty array).
        assert_eq!(
            int(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUSBYMEMBER",
                    "invalid_key",
                    "Madrid",
                    "900",
                    "KM",
                    "STORE",
                    "store_key"
                ])
            )),
            0
        );
        assert!(!exists(&mut d, "store_key"), "dest should be deleted");
        // STORE.
        assert_eq!(
            int(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUSBYMEMBER",
                    "Europe",
                    "Madrid",
                    "700",
                    "KM",
                    "STORE",
                    "store_key"
                ])
            )),
            2
        );
        assert_eq!(
            zscores(&mut d, "store_key"),
            vec![
                "Madrid".to_string(),
                "3471766229222696".to_string(),
                "Lisbon".to_string(),
                "3473121093062745".to_string()
            ]
        );
        // STOREDIST stores distance in the query unit.
        assert_eq!(
            int(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUSBYMEMBER",
                    "Europe",
                    "Madrid",
                    "700",
                    "KM",
                    "STOREDIST",
                    "store_dist_key"
                ])
            )),
            2
        );
        let sd = zscores(&mut d, "store_dist_key");
        assert_eq!(sd[0], "Madrid");
        assert_eq!(sd[1], "0");
        assert!(
            (sd[3].parse::<f64>().unwrap() - 502.207_694).abs() < 1e-4,
            "got {sd:?}"
        );
        // WITHCOORD+STORE incompatibility (reference `GeoRadiusByMember`).
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUSBYMEMBER",
                    "Europe",
                    "Madrid",
                    "900",
                    "KM",
                    "STORE",
                    "store_key",
                    "WITHCOORD"
                ])
            )),
            "ERR STORE option in GEORADIUSBYMEMBER is not compatible with WITHDIST, WITHHASH and WITHCOORDS options"
        );
        // WITHHASH before STORE: different arg permutation must also be caught.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUSBYMEMBER",
                    "Sicily",
                    "Agrigento",
                    "100",
                    "km",
                    "WITHHASH",
                    "store",
                    "tmp"
                ])
            )),
            "ERR STORE option in GEORADIUSBYMEMBER is not compatible with WITHDIST, WITHHASH and WITHCOORDS options"
        );
        // COUNT 0.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUSBYMEMBER",
                    "Sicily",
                    "Agrigento",
                    "100",
                    "km",
                    "COUNT",
                    "0"
                ])
            )),
            "ERR COUNT must be > 0"
        );
        // Non-numeric COUNT -> syntax error for the BYMEMBER family.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUSBYMEMBER",
                    "Europe",
                    "Madrid",
                    "700",
                    "KM",
                    "COUNT",
                    "notanumber"
                ])
            )),
            "ERR syntax error"
        );
        // Bad unit.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&["GEORADIUSBYMEMBER", "Europe", "Madrid", "700", "badunit"])
            )),
            "ERR unsupported unit provided. please use M, KM, FT, MI"
        );
        // RO variant rejects STORE and STOREDIST.
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUSBYMEMBER_RO",
                    "Europe",
                    "Madrid",
                    "700",
                    "KM",
                    "STORE",
                    "store_key"
                ])
            )),
            "ERR syntax error"
        );
        assert_eq!(
            err(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUSBYMEMBER_RO",
                    "Europe",
                    "Madrid",
                    "700",
                    "KM",
                    "STOREDIST",
                    "store_dist_key"
                ])
            )),
            "ERR syntax error"
        );
    }

    /// Reference `GeoRadiusByMemberUb`: unit-boundary search in `mi` with
    /// WITHCOORD/WITHDIST must return the matching member and its cell-center
    /// coordinates.
    #[test]
    fn georadius_by_member_ub() {
        let mut d = db();
        dispatch_at(
            &mut d,
            0,
            &b_args(&["GEOADD", "geo", "-118.2437", "34.0522", "972"]),
        );
        dispatch_at(
            &mut d,
            0,
            &b_args(&["GEOADD", "geo", "-73.935242", "40.730610", "973"]),
        );
        dispatch_at(
            &mut d,
            0,
            &b_args(&["GEOADD", "geo", "-122.4194", "37.7749", "971"]),
        );
        let r = dispatch_at(
            &mut d,
            0,
            &b_args(&[
                "GEORADIUSBYMEMBER",
                "geo",
                "971",
                "200",
                "mi",
                "WITHCOORD",
                "WITHDIST",
                "COUNT",
                "40",
                "ASC",
            ]),
        );
        let rows = match r.into_resp_value() {
            RespValue::Array(v) => v,
            o => panic!("expected array, got {o:?}"),
        };
        assert_eq!(rows.len(), 1, "got {rows:?}");
        let row = match &rows[0] {
            RespValue::Array(v) => v,
            o => panic!("expected row array, got {o:?}"),
        };
        assert!(
            matches!(&row[0], RespValue::Bulk(b) if b == b"971"),
            "got {:?}",
            row[0]
        );
        // Member's distance to itself is ~0 (cell-center residual).
        let dist = bulk_str(CmdResult::Ok(row[1].clone()));
        assert!(
            dist.parse::<f64>().unwrap().abs() < 1e-6,
            "got {:?}",
            row[1]
        );
        // Cell-center coordinates (reference: -122.41940170526505, 37.77490001056578).
        let coord = match &row[2] {
            RespValue::Array(v) => v,
            o => panic!("expected coord array, got {o:?}"),
        };
        let lon = bulk_str(CmdResult::Ok(coord[0].clone()));
        let lat = bulk_str(CmdResult::Ok(coord[1].clone()));
        assert!(
            (lon.parse::<f64>().unwrap() - (-122.419_401_705_265_05)).abs() < 1e-9,
            "got {lon:?}"
        );
        assert!(
            (lat.parse::<f64>().unwrap() - 37.774_900_010_565_78).abs() < 1e-9,
            "got {lat:?}"
        );
    }

    #[test]
    fn georadius_store_deletes_on_empty_search() {
        let mut d = db();
        geoadd_europe(&mut d);
        // Existing source, empty search: destination deleted + reply 0.
        assert_eq!(
            int(dispatch_at(
                &mut d,
                0,
                &b_args(&[
                    "GEORADIUS",
                    "Europe",
                    "0",
                    "0",
                    "0.001",
                    "KM",
                    "STORE",
                    "store_key"
                ])
            )),
            0
        );
        assert!(!exists(&mut d, "store_key"), "dest should be deleted");
    }

    fn b_args(a: &[&str]) -> Vec<Vec<u8>> {
        a.iter().map(|s| s.as_bytes().to_vec()).collect()
    }
}
