//! Port of `dragonfly/src/server/geo_family_test.cc`.
//!
//! Adaptations from the C++ original:
//! - `DoubleArg(...)` comparisons become `assert_near` with a relative/absolute
//!   tolerance (the C++ tolerance absorbs CPU FMA variance).
//! - `resp.GetVec().empty()` on an empty reply becomes `t.arr(...).is_empty()`;
//!   on a STORE variant it is the integer `0` (the port follows live redis
//!   semantics for a missing source, which still satisfies the C++ check).
//! - `RespElementsAre` (one-element array) and `RespArray` are both `t.arr`.
//! - The reply field order `[member, dist?, hash?, coords?]` is asserted with
//!   `assert_result`.
#![allow(clippy::unreadable_literal)]

mod common;

use common::*;

/// Tolerance for `DoubleArg`-style comparisons, matching the reference's
/// loosest `DoubleArg(..., 0.001)` usage.
const TOL: f64 = 1e-3;

/// Tolerance for geohash-decoded coordinates (`DoubleArg(..., 0.01)` in the
/// reference), which are quantized on encode and cannot be compared exactly.
const COORD_TOL: f64 = 1e-4;

/// Assert a bulk string parses to a float near `expected`.
fn assert_near(v: &Value, expected: f64) {
    let s = v.text().unwrap_or_else(|| panic!("expected numeric bulk, got {v:?}"));
    let got: f64 = s
        .parse()
        .unwrap_or_else(|_| panic!("expected numeric bulk, got {s:?}"));
    assert!(
        (got - expected).abs() <= TOL,
        "got {got}, expected ~{expected} (tol {TOL})"
    );
}

/// Assert `v` is a `[lon, lat]` pair of bulk strings near `(lon, lat)`.
fn assert_coords(v: &Value, lon: f64, lat: f64) {
    let a = v.arr().unwrap_or_else(|| panic!("expected [lon, lat], got {v:?}"));
    assert_eq!(a.len(), 2, "got {v:?}");
    let lon_s = a[0].text().expect("lon bulk");
    let lat_s = a[1].text().expect("lat bulk");
    let (lon_v, lat_v): (f64, f64) = (lon_s.parse().unwrap(), lat_s.parse().unwrap());
    assert!(
        (lon_v - lon).abs() <= COORD_TOL && (lat_v - lat).abs() <= COORD_TOL,
        "got ({lon_v}, {lat_v}), expected ~({lon}, {lat})"
    );
}

/// Assert `v` is a `[lon, lat]` pair of bulk strings with exact text.
fn assert_coords_text(v: &Value, lon: &str, lat: &str) {
    let a = v.arr().unwrap_or_else(|| panic!("expected [lon, lat], got {v:?}"));
    assert_eq!(a.len(), 2, "got {v:?}");
    assert_eq!(a[0].text().as_deref(), Some(lon));
    assert_eq!(a[1].text().as_deref(), Some(lat));
}

/// Assert one search-result record `[member, dist?, hash?, coords?]`.
fn assert_result(v: &Value, member: &str, fields: (Option<f64>, Option<&str>, Option<(f64, f64)>)) {
    let a = v.arr().unwrap_or_else(|| panic!("expected record, got {v:?}"));
    let want = 1
        + usize::from(fields.0.is_some())
        + usize::from(fields.1.is_some())
        + usize::from(fields.2.is_some());
    assert_eq!(a.len(), want, "record {v:?}");
    assert_eq!(a[0].text().as_deref(), Some(member), "record {v:?}");
    let mut i = 1;
    if let Some(d) = fields.0 {
        assert_near(&a[i], d);
        i += 1;
    }
    if let Some(h) = fields.1 {
        assert_eq!(a[i].text().as_deref(), Some(h), "record {v:?}");
        i += 1;
    }
    if let Some((lon, lat)) = fields.2 {
        assert_coords(&a[i], lon, lat);
    }
}

/// Load the reference's ten-city `Europe` dataset.
fn geoadd_europe(t: &mut Ctx) {
    t.assert_int(
        &[
            "geoadd", "Europe",
            "13.4050", "52.5200", "Berlin",
            "3.7038", "40.4168", "Madrid",
            "9.1427", "38.7369", "Lisbon",
            "2.3522", "48.8566", "Paris",
            "16.3738", "48.2082", "Vienna",
            "4.8952", "52.3702", "Amsterdam",
            "10.7522", "59.9139", "Oslo",
            "23.7275", "37.9838", "Athens",
            "19.0402", "47.4979", "Budapest",
            "6.2603", "53.3498", "Dublin",
        ],
        10,
    );
}

#[test]
fn geo_add() {
    let mut t = Ctx::new();
    t.assert_int(
        &["geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"],
        2,
    );
    t.assert_int(
        &["geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"],
        0,
    );
    let v = t.arr(&["geohash", "Sicily", "Palermo", "Catania"]);
    assert_eq!(
        v.iter().map(Value::text).collect::<Vec<_>>(),
        vec![Some("sqc8b49rny0".into()), Some("sqdtr74hyu0".into())]
    );
}

#[test]
fn geo_add_options() {
    let mut t = Ctx::new();
    t.assert_int(
        &["geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"],
        2,
    );

    // add 1 + update 1 + XX
    t.assert_int(
        &["geoadd", "Sicily", "XX", "15.361389", "38.115556", "Palermo", "15.554167", "38.193611", "Messina"],
        0,
    );
    let v = t.arr(&["geopos", "Sicily", "Palermo", "Messina"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert_coords_text(&v[0], "15.361389219760895", "38.1155563954963");
    assert!(matches!(v[1], Value::Bulk(None)), "expected nil, got {:?}", v[1]);

    // add 1 + update 1 + NX
    t.assert_int(
        &["geoadd", "Sicily", "NX", "18.361389", "38.115556", "Palermo", "15.2875", "37.069167", "Syracuse"],
        1,
    );
    let v = t.arr(&["geopos", "Sicily", "Palermo", "Syracuse"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert_coords_text(&v[0], "15.361389219760895", "38.1155563954963");
    assert_coords_text(&v[1], "15.287499725818634", "37.06916773705567");

    // add 1 + update 1 CH
    t.assert_int(
        &["geoadd", "Sicily", "CH", "18.361389", "38.115556", "Palermo", "12.434167", "37.798056", "Marsala"],
        2,
    );
    let v = t.arr(&["geopos", "Sicily", "Palermo", "Marsala"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert_coords_text(&v[0], "18.361386358737946", "38.1155563954963");
    assert_coords_text(&v[1], "12.43416577577591", "37.7980572230775");

    // update 1 + CH + XX
    t.assert_int(&["geoadd", "Sicily", "CH", "XX", "10.361389", "38.115556", "Palermo"], 1);
    let v = t.arr(&["geopos", "Sicily", "Palermo"]);
    assert_eq!(v.len(), 1, "reply {v:?}");
    assert_coords(&v[0], 10.361389, 38.115556);

    // add 1 + CH + NX
    t.assert_int(&["geoadd", "Sicily", "CH", "NX", "14.25", "37.066667", "Gela"], 1);
    let v = t.arr(&["geopos", "Sicily", "Gela"]);
    assert_eq!(v.len(), 1, "reply {v:?}");
    assert_coords(&v[0], 14.25, 37.066667);

    // add 1 + XX + NX
    t.assert_err(
        &["geoadd", "Sicily", "XX", "NX", "14.75", "36.933333", "Ragusa"],
        "XX and NX options at the same time are not compatible",
    );

    // incorrect number of args
    t.assert_err(
        &["geoadd", "Sicily", "14.75", "36.933333", "Ragusa", "10.23"],
        "syntax error",
    );
}

#[test]
fn geo_pos() {
    let mut t = Ctx::new();
    t.assert_int(&["geoadd", "Sicily", "13.361389", "38.115556", "Palermo"], 1);
    let v = t.arr(&["geopos", "Sicily", "Palermo", "NonExisting"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert_coords_text(&v[0], "13.361389338970184", "38.1155563954963");
    assert!(matches!(v[1], Value::Bulk(None)), "expected nil, got {:?}", v[1]);
}

#[test]
fn geo_pos_wrong_type() {
    let mut t = Ctx::new();
    t.ok(&["set", "x", "value"]);
    t.assert_err(&["geopos", "x", "Sicily", "Palermo"], "WRONGTYPE");
}

#[test]
fn geo_dist() {
    let mut t = Ctx::new();
    t.assert_int(
        &["geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"],
        2,
    );
    let mut d = |args: &[&str]| -> f64 {
        let b = t.bulk_opt(args).expect("expected bulk distance");
        String::from_utf8(b).unwrap().parse().unwrap()
    };
    assert_near(&Value::Bulk(Some(d(&["geodist", "Sicily", "Palermo", "Catania"]).to_string().into_bytes())), 166_274.15156960033);
    assert_near(&Value::Bulk(Some(d(&["geodist", "Sicily", "Palermo", "Catania", "km"]).to_string().into_bytes())), 166.27415156960032);
    assert_near(&Value::Bulk(Some(d(&["geodist", "Sicily", "Palermo", "Catania", "MI"]).to_string().into_bytes())), 103.31822459492733);
    assert_near(&Value::Bulk(Some(d(&["geodist", "Sicily", "Palermo", "Catania", "FT"]).to_string().into_bytes())), 545_518.8699790037);

    // Missing members reply nil.
    assert!(t.bulk_opt(&["geodist", "Sicily", "Foo", "Bar"]).is_none());
}

#[test]
fn geo_search() {
    let mut t = Ctx::new();
    geoadd_europe(&mut t);

    let v = t.arr(&["GEOSEARCH", "Europe", "FROMLONLAT", "13.4050", "52.5200", "BYRADIUS", "500", "KM", "WITHCOORD", "WITHDIST", "WITHHASH"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert_result(&v[0], "Berlin", (Some(0.00017343178521311378), Some("3673983950397063"), Some((13.4050, 52.5200))));
    assert_result(&v[1], "Dublin", (Some(487.5619030644293), Some("3678981558208417"), Some((6.2603, 53.3498))));

    // Missing key -> empty array.
    assert!(t.arr(&["GEOSEARCH", "invalid_key", "FROMMEMBER", "Madrid", "BYRADIUS", "700", "KM", "WITHCOORD", "WITHDIST"]).is_empty());

    // Missing member -> error.
    t.assert_err(
        &["GEOSEARCH", "Europe", "FROMMEMBER", "invalid_member", "BYRADIUS", "700", "KM", "WITHCOORD", "WITHDIST"],
        "could not decode requested zset member",
    );

    // Missing key via FROMLONLAT -> empty array.
    assert!(t.arr(&["GEOSEARCH", "America", "FROMLONLAT", "13.4050", "52.5200", "BYBOX", "1000", "1000", "KM", "WITHCOORD", "WITHDIST"]).is_empty());

    // Box far from every city -> empty array.
    assert!(t.arr(&["GEOSEARCH", "Europe", "FROMLONLAT", "130.4050", "52.5200", "BYBOX", "10", "10", "KM", "WITHCOORD", "WITHDIST"]).is_empty());

    let v = t.arr(&["GEOSEARCH", "Europe", "FROMLONLAT", "13.4050", "52.5200", "BYBOX", "1000", "1000", "KM", "WITHCOORD", "WITHDIST"]);
    assert_eq!(v.len(), 3, "reply {v:?}");
    assert_result(&v[0], "Vienna", (Some(523.6926930553866), None, Some((16.3738, 48.2082))));
    assert_result(&v[1], "Berlin", (Some(0.00017343178521311378), None, Some((13.4050, 52.5200))));
    assert_result(&v[2], "Dublin", (Some(487.5619030644293), None, Some((6.2603, 53.3498))));

    let v = t.arr(&["GEOSEARCH", "Europe", "FROMLONLAT", "13.4050", "52.5200", "BYRADIUS", "500", "KM", "COUNT", "3", "WITHCOORD", "WITHDIST"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert_result(&v[0], "Berlin", (Some(0.00017343178521311378), None, Some((13.4050, 52.5200))));
    assert_result(&v[1], "Dublin", (Some(487.5619030644293), None, Some((6.2603, 53.3498))));

    let v = t.arr(&["GEOSEARCH", "Europe", "FROMLONLAT", "13.4050", "52.5200", "BYRADIUS", "500", "KM", "DESC", "WITHCOORD", "WITHDIST"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert_result(&v[0], "Dublin", (Some(487.5619030644293), None, Some((6.2603, 53.3498))));
    assert_result(&v[1], "Berlin", (Some(0.00017343178521311378), None, Some((13.4050, 52.5200))));

    let v = t.arr(&["GEOSEARCH", "Europe", "FROMMEMBER", "Madrid", "BYRADIUS", "700", "KM", "WITHCOORD", "WITHDIST"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert_result(&v[0], "Madrid", (Some(0.0), None, Some((3.7038, 40.4168))));
    assert_result(&v[1], "Lisbon", (Some(502.20769462704106), None, Some((9.1427, 38.7369))));

    let v = t.arr(&["GEOSEARCH", "Europe", "FROMMEMBER", "Madrid", "BYRADIUS", "700", "KM"]);
    assert_eq!(v.iter().map(Value::text).collect::<Vec<_>>(), vec![Some("Madrid".into()), Some("Lisbon".into())]);
}

#[test]
fn geo_search_nan_coord() {
    let mut t = Ctx::new();
    t.run(&["GEOADD", "cities", "13.361", "38.115", "Palermo", "15.087", "37.502", "Catania"]);
    t.assert_err(
        &["GEOSEARCH", "cities", "FROMLONLAT", "15", "NaN", "BYRADIUS", "200", "km"],
        "invalid longitude,latitude pair",
    );
}

#[test]
fn geo_radius_by_member() {
    let mut t = Ctx::new();
    geoadd_europe(&mut t);

    assert!(t.arr(&["GEORADIUSBYMEMBER", "invalid_key", "Madrid", "900", "KM"]).is_empty());
    // STORE on a missing source replies the destination count (live semantics).
    t.assert_int(&["GEORADIUSBYMEMBER", "invalid_key", "Madrid", "900", "KM", "STORE", "store_key"], 0);
    t.assert_err(
        &["GEORADIUSBYMEMBER", "Europe", "invalid_mem", "900", "KM", "STORE", "store_key"],
        "could not decode requested zset member",
    );

    let v = t.arr(&["GEORADIUSBYMEMBER", "Europe", "Madrid", "700", "KM", "WITHCOORD", "WITHDIST"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert_result(&v[0], "Madrid", (Some(0.0), None, Some((3.703801, 40.416799))));
    assert_result(&v[1], "Lisbon", (Some(502.207695), None, Some((9.142698, 38.736900))));

    t.assert_int(&["GEORADIUSBYMEMBER", "Europe", "Madrid", "700", "KM", "STORE", "store_key"], 2);
    let v = t.arr(&["ZRANGE", "store_key", "0", "-1"]);
    assert_eq!(v.iter().map(Value::text).collect::<Vec<_>>(), vec![Some("Madrid".into()), Some("Lisbon".into())]);
    let v = t.arr(&["ZRANGE", "store_key", "0", "-1", "WITHSCORES"]);
    assert_eq!(
        v.iter().map(Value::text).collect::<Vec<_>>(),
        vec![Some("Madrid".into()), Some("3471766229222696".into()), Some("Lisbon".into()), Some("3473121093062745".into())]
    );

    t.assert_int(&["GEORADIUSBYMEMBER", "Europe", "Madrid", "700", "KM", "STOREDIST", "store_dist_key"], 2);
    let v = t.arr(&["ZRANGE", "store_dist_key", "0", "-1", "WITHSCORES"]);
    assert_eq!(v.len(), 4, "reply {v:?}");
    assert_eq!(v[0].text().as_deref(), Some("Madrid"));
    assert_near(&v[1], 0.0);
    assert_eq!(v[2].text().as_deref(), Some("Lisbon"));
    assert_near(&v[3], 502.207695);

    t.assert_err(
        &["GEORADIUSBYMEMBER", "Europe", "Madrid", "900", "KM", "STORE", "store_key", "WITHCOORD"],
        "STORE option in GEORADIUSBYMEMBER is not compatible",
    );

    // Different argument permutation for the same incompatibility.
    t.assert_err(
        &["GEORADIUSBYMEMBER", "Sicily", "Agrigento", "100", "km", "WITHHASH", "store", "tmp"],
        "STORE option in GEORADIUSBYMEMBER is not compatible",
    );

    t.run(&["GEOADD", "t", "13.361389", "38.115556", "a", "13.3619", "38.1159", "b", "13.3608", "38.1152", "c"]);
    t.assert_err(
        &["GEOSEARCH", "t", "FROMLONLAT", "13.361389", "38.115556", "BYRADIUS", "1", "KM", "COUNT", "0"],
        "COUNT must be > 0",
    );

    // A non-numeric COUNT must report a syntax error, not be misrendered as an
    // invalid lon/lat pair.
    t.assert_err(
        &["GEORADIUSBYMEMBER", "Sicily", "Agrigento", "100", "km", "COUNT", "notanumber"],
        "syntax error",
    );

    t.assert_err(
        &["GEORADIUSBYMEMBER", "Sicily", "Agrigento", "100", "badunit"],
        "unsupported unit provided. please use M, KM, FT, MI",
    );
}

#[test]
fn geo_radius_by_member_ro() {
    let mut t = Ctx::new();
    geoadd_europe(&mut t);

    let v = t.arr(&["GEORADIUSBYMEMBER_RO", "Europe", "Madrid", "700", "KM", "WITHCOORD", "WITHDIST"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert_result(&v[0], "Madrid", (Some(0.0), None, Some((3.703801, 40.416799))));
    assert_result(&v[1], "Lisbon", (Some(502.207695), None, Some((9.142698, 38.736900))));

    // The _RO variant must not accept storing options.
    t.assert_err(
        &["GEORADIUSBYMEMBER_RO", "Europe", "Madrid", "700", "KM", "STOREDIST", "store_dist_key"],
        "syntax error",
    );
    t.assert_err(
        &["GEORADIUSBYMEMBER_RO", "Europe", "Madrid", "700", "KM", "STORE", "store_key"],
        "syntax error",
    );
}

#[test]
fn geo_radius() {
    let mut t = Ctx::new();
    geoadd_europe(&mut t);

    assert!(t.arr(&["GEORADIUS", "invalid_key", "16.3738", "48.2082", "900", "KM"]).is_empty());
    assert!(t.arr(&["GEORADIUS", "America", "13.4050", "52.5200", "500", "KM", "WITHCOORD", "WITHDIST"]).is_empty());
    assert!(t.arr(&["GEORADIUS", "Europe", "130.4050", "52.5200", "10", "KM", "WITHCOORD", "WITHDIST"]).is_empty());

    let v = t.arr(&["GEORADIUS", "Europe", "13.4050", "52.5200", "500", "KM", "COUNT", "3", "WITHCOORD", "WITHDIST"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert_result(&v[0], "Berlin", (Some(0.00017343178521311378), None, Some((13.4050, 52.5200))));
    assert_result(&v[1], "Dublin", (Some(487.5619030644293), None, Some((6.2603, 53.3498))));

    let v = t.arr(&["GEORADIUS", "Europe", "13.4050", "52.5200", "500", "KM", "DESC", "WITHCOORD", "WITHDIST"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert_result(&v[0], "Dublin", (Some(487.5619030644293), None, Some((6.2603, 53.3498))));
    assert_result(&v[1], "Berlin", (Some(0.00017343178521311378), None, Some((13.4050, 52.5200))));

    t.assert_int(&["GEORADIUS", "Europe", "3.7038", "40.4168", "700", "KM", "STORE", "store_key"], 2);
    let v = t.arr(&["ZRANGE", "store_key", "0", "-1"]);
    assert_eq!(v.iter().map(Value::text).collect::<Vec<_>>(), vec![Some("Madrid".into()), Some("Lisbon".into())]);
    let v = t.arr(&["ZRANGE", "store_key", "0", "-1", "WITHSCORES"]);
    assert_eq!(
        v.iter().map(Value::text).collect::<Vec<_>>(),
        vec![Some("Madrid".into()), Some("3471766229222696".into()), Some("Lisbon".into()), Some("3473121093062745".into())]
    );

    t.assert_int(&["GEORADIUS", "Europe", "3.7038", "40.4168", "700", "KM", "STOREDIST", "store_dist_key"], 2);
    let v = t.arr(&["ZRANGE", "store_dist_key", "0", "-1", "WITHSCORES"]);
    assert_eq!(v.len(), 4, "reply {v:?}");
    assert_eq!(v[0].text().as_deref(), Some("Madrid"));
    assert_near(&v[1], 0.0);
    assert_eq!(v[2].text().as_deref(), Some("Lisbon"));
    assert_near(&v[3], 502.207694);

    // STORE mixed with WITH* options and a stray argument must be a syntax error.
    t.assert_err(
        &["GEORADIUS", "key:poq6moq\\r", "111.38360132204588", "-71.17374967857494", "69.77510489600115", "ft", "key", "WITHDIST", "COUNT", "key", "WITHCOORD", "count", "WITHHASH", "STORE"],
        "syntax error",
    );

    t.run(&["GEOADD", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"]);
    t.assert_err(
        &["GEORADIUS", "SICILY", "15", "37", "200", "KM", "COUNT", "0"],
        "COUNT must be > 0",
    );

    t.run(&["GEOADD", "Sicily", "13.583333", "37.316667", "Agrigento"]);
    t.assert_err(
        &["GEORADIUSBYMEMBER", "Sicily", "Agrigento", "100", "km", "COUNT", "0"],
        "COUNT must be > 0",
    );

    let v = t.arr(&["GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT", "1"]);
    assert_eq!(v.len(), 1, "reply {v:?}");
    assert_eq!(v[0].text().as_deref(), Some("Agrigento"));

    t.assert_err(
        &["GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHDIST", "STORE", "result"],
        "STORE option in GEORADIUS is not compatible",
    );
}

#[test]
fn geo_radius_ro() {
    let mut t = Ctx::new();
    geoadd_europe(&mut t);

    // GEORADIUS_RO must not accept storing options.
    t.assert_err(
        &["GEORADIUS_RO", "Europe", "13.4050", "52.5200", "900", "KM", "STORE_DIST", "store_key"],
        "syntax error",
    );
    t.assert_err(
        &["GEORADIUS_RO", "Europe", "13.4050", "52.5200", "900", "KM", "STORE", "store_key"],
        "syntax error",
    );

    let v = t.arr(&["GEORADIUS_RO", "Europe", "13.4050", "52.5200", "500", "KM", "COUNT", "3", "WITHCOORD", "WITHDIST"]);
    assert_eq!(v.len(), 2, "reply {v:?}");
    assert_result(&v[0], "Berlin", (Some(0.00017343178521311378), None, Some((13.4050, 52.5200))));
    assert_result(&v[1], "Dublin", (Some(487.5619030644293), None, Some((6.2603, 53.3498))));
}

#[test]
fn geo_radius_by_member_ub() {
    let mut t = Ctx::new();
    t.run(&["GEOADD", "geo", "-118.2437", "34.0522", "972"]);
    t.run(&["GEOADD", "geo", "-73.935242", "40.730610", "973"]);
    t.run(&["GEOADD", "geo", "-122.4194", "37.7749", "971"]);

    let v = t.arr(&["GEORADIUSBYMEMBER", "geo", "971", "200", "mi", "WITHCOORD", "WITHDIST", "COUNT", "40", "ASC"]);
    assert_eq!(v.len(), 1, "reply {v:?}");
    assert_result(&v[0], "971", (Some(0.0), None, Some((-122.41940170526505, 37.77490001056578))));
}
