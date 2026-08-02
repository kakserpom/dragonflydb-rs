//! Geohash encoding/decoding and geo search math, byte-for-byte compatible with
//! `dragonfly/src/redis/geohash.{c,h}` and `geohash_helper.{c,h}` (yinqiwen /
//! Matt Stancliff / Salvatore Sanfilippo).
//!
//! A geo point is stored as a ZSet score: the 52-bit interleaved geohash at
//! `GEO_STEP_MAX` (26) precision, i.e. `interleave64(lat_offset, long_offset)`
//! scaled to a 26-bit fixed point over the WGS84 (mercator) lat/long ranges.

pub const GEO_LAT_MIN: f64 = -85.05112878;
pub const GEO_LAT_MAX: f64 = 85.05112878;
pub const GEO_LONG_MIN: f64 = -180.0;
pub const GEO_LONG_MAX: f64 = 180.0;

/// 26 * 2 = 52 bits: the precision used for stored scores.
pub const GEO_STEP_MAX: u8 = 26;

pub const EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;
pub const MERCATOR_MAX: f64 = 20037726.37;

const D_R: f64 = std::f64::consts::PI / 180.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GeoHashBits {
    pub bits: u64,
    pub step: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct GeoHashRange {
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct GeoHashArea {
    pub hash: GeoHashBits,
    pub longitude: GeoHashRange,
    pub latitude: GeoHashRange,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoHashNeighbors {
    pub north: GeoHashBits,
    pub east: GeoHashBits,
    pub west: GeoHashBits,
    pub south: GeoHashBits,
    pub north_east: GeoHashBits,
    pub south_east: GeoHashBits,
    pub north_west: GeoHashBits,
    pub south_west: GeoHashBits,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoHashRadius {
    pub hash: GeoHashBits,
    pub area: GeoHashArea,
    pub neighbors: GeoHashNeighbors,
}

pub fn hash_is_zero(h: GeoHashBits) -> bool {
    h.bits == 0 && h.step == 0
}

fn deg_rad(ang: f64) -> f64 {
    ang * D_R
}

fn rad_deg(ang: f64) -> f64 {
    ang / D_R
}

/// Interleave lower bits of x and y, so the bits of x are in the even positions
/// and bits from y in the odd; x and y must initially be less than 2**32.
fn interleave64(xlo: u32, ylo: u32) -> u64 {
    let b = [
        0x5555555555555555u64,
        0x3333333333333333,
        0x0F0F0F0F0F0F0F0F,
        0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
    ];
    let s = [1u32, 2, 4, 8, 16];

    let mut x = xlo as u64;
    let mut y = ylo as u64;

    x = (x | (x << s[4])) & b[4];
    y = (y | (y << s[4])) & b[4];

    x = (x | (x << s[3])) & b[3];
    y = (y | (y << s[3])) & b[3];

    x = (x | (x << s[2])) & b[2];
    y = (y | (y << s[2])) & b[2];

    x = (x | (x << s[1])) & b[1];
    y = (y | (y << s[1])) & b[1];

    x = (x | (x << s[0])) & b[0];
    y = (y | (y << s[0])) & b[0];

    x | (y << 1)
}

/// Reverse of `interleave64`; returns `[lat, long]` (lat in the low 32 bits).
fn deinterleave64(interleaved: u64) -> u64 {
    let b = [
        0x5555555555555555u64,
        0x3333333333333333,
        0x0F0F0F0F0F0F0F0F,
        0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
        0x00000000FFFFFFFF,
    ];
    let s = [0u32, 1, 2, 4, 8, 16];

    let mut x = interleaved;
    let mut y = interleaved >> 1;

    x = (x | (x >> s[0])) & b[0];
    y = (y | (y >> s[0])) & b[0];

    x = (x | (x >> s[1])) & b[1];
    y = (y | (y >> s[1])) & b[1];

    x = (x | (x >> s[2])) & b[2];
    y = (y | (y >> s[2])) & b[2];

    x = (x | (x >> s[3])) & b[3];
    y = (y | (y >> s[3])) & b[3];

    x = (x | (x >> s[4])) & b[4];
    y = (y | (y >> s[4])) & b[4];

    x = (x | (x >> s[5])) & b[5];
    y = (y | (y >> s[5])) & b[5];

    x | (y << 32)
}

/// Constraints from EPSG:900913 / EPSG:3785 / OSGEO:41001.
pub fn coord_range() -> (GeoHashRange, GeoHashRange) {
    (
        GeoHashRange { min: GEO_LONG_MIN, max: GEO_LONG_MAX },
        GeoHashRange { min: GEO_LAT_MIN, max: GEO_LAT_MAX },
    )
}

pub fn encode(
    long_range: &GeoHashRange,
    lat_range: &GeoHashRange,
    longitude: f64,
    latitude: f64,
    step: u8,
) -> Option<GeoHashBits> {
    if step > 32 || step == 0 || (lat_range.min == 0.0 && lat_range.max == 0.0)
        || (long_range.min == 0.0 && long_range.max == 0.0)
    {
        return None;
    }

    // Return an error when trying to index outside the supported constraints.
    if longitude > GEO_LONG_MAX || longitude < GEO_LONG_MIN
        || latitude > GEO_LAT_MAX || latitude < GEO_LAT_MIN
    {
        return None;
    }

    if latitude < lat_range.min || latitude > lat_range.max
        || longitude < long_range.min || longitude > long_range.max
    {
        return None;
    }

    let lat_offset = (latitude - lat_range.min) / (lat_range.max - lat_range.min);
    let long_offset = (longitude - long_range.min) / (long_range.max - long_range.min);

    // Convert to fixed point based on the step size.
    let scale = (1u64 << step) as f64;
    let lat_offset = (lat_offset * scale) as u32;
    let long_offset = (long_offset * scale) as u32;

    Some(GeoHashBits { bits: interleave64(lat_offset, long_offset), step })
}

pub fn encode_wgs84(longitude: f64, latitude: f64, step: u8) -> Option<GeoHashBits> {
    let (long_range, lat_range) = coord_range();
    encode(&long_range, &lat_range, longitude, latitude, step)
}

pub fn decode(long_range: GeoHashRange, lat_range: GeoHashRange, hash: GeoHashBits) -> Option<GeoHashArea> {
    if hash_is_zero(hash) || (lat_range.min == 0.0 && lat_range.max == 0.0)
        || (long_range.min == 0.0 && long_range.max == 0.0)
    {
        return None;
    }

    let step = hash.step as u64;
    let hash_sep = deinterleave64(hash.bits); // hash = [lat][long]

    let lat_scale = lat_range.max - lat_range.min;
    let long_scale = long_range.max - long_range.min;

    let ilato = hash_sep as u32; // get lat part of deinterleaved hash
    let ilono = (hash_sep >> 32) as u32; // shift over to get long part of hash

    let scale = 1u64 << step;
    let area = GeoHashArea {
        hash,
        latitude: GeoHashRange {
            min: lat_range.min + (ilato as f64 / scale as f64) * lat_scale,
            max: lat_range.min + (ilato.wrapping_add(1) as f64 / scale as f64) * lat_scale,
        },
        longitude: GeoHashRange {
            min: long_range.min + (ilono as f64 / scale as f64) * long_scale,
            max: long_range.min + (ilono.wrapping_add(1) as f64 / scale as f64) * long_scale,
        },
    };
    Some(area)
}

pub fn decode_wgs84(hash: GeoHashBits) -> Option<GeoHashArea> {
    let (long_range, lat_range) = coord_range();
    decode(long_range, lat_range, hash)
}

pub fn area_to_long_lat(area: &GeoHashArea) -> [f64; 2] {
    let mut lon = (area.longitude.min + area.longitude.max) / 2.0;
    let mut lat = (area.latitude.min + area.latitude.max) / 2.0;
    if lon > GEO_LONG_MAX {
        lon = GEO_LONG_MAX;
    }
    if lon < GEO_LONG_MIN {
        lon = GEO_LONG_MIN;
    }
    if lat > GEO_LAT_MAX {
        lat = GEO_LAT_MAX;
    }
    if lat < GEO_LAT_MIN {
        lat = GEO_LAT_MIN;
    }
    [lon, lat]
}

pub fn decode_to_long_lat(hash: GeoHashBits) -> Option<[f64; 2]> {
    let area = decode_wgs84(hash)?;
    Some(area_to_long_lat(&area))
}

fn move_x(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }

    let mut x = hash.bits & 0xaaaaaaaaaaaaaaaa;
    let y = hash.bits & 0x5555555555555555;

    let zz = 0x5555555555555555u64 >> (64 - hash.step as u64 * 2);

    if d > 0 {
        x = x.wrapping_add(zz + 1);
    } else {
        x = x | zz;
        x = x.wrapping_sub(zz + 1);
    }

    x &= 0xaaaaaaaaaaaaaaaa >> (64 - hash.step as u64 * 2);
    hash.bits = x | y;
}

fn move_y(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }

    let x = hash.bits & 0xaaaaaaaaaaaaaaaa;
    let mut y = hash.bits & 0x5555555555555555;

    let zz = 0xaaaaaaaaaaaaaaaau64 >> (64 - hash.step as u64 * 2);
    if d > 0 {
        y = y.wrapping_add(zz + 1);
    } else {
        y = y | zz;
        y = y.wrapping_sub(zz + 1);
    }
    y &= 0x5555555555555555 >> (64 - hash.step as u64 * 2);
    hash.bits = x | y;
}

pub fn neighbors(hash: GeoHashBits) -> GeoHashNeighbors {
    let mut n = GeoHashNeighbors {
        east: hash,
        west: hash,
        north: hash,
        south: hash,
        south_east: hash,
        south_west: hash,
        north_east: hash,
        north_west: hash,
    };

    move_x(&mut n.east, 1);
    move_y(&mut n.east, 0);

    move_x(&mut n.west, -1);
    move_y(&mut n.west, 0);

    move_x(&mut n.south, 0);
    move_y(&mut n.south, -1);

    move_x(&mut n.north, 0);
    move_y(&mut n.north, 1);

    move_x(&mut n.north_west, -1);
    move_y(&mut n.north_west, 1);

    move_x(&mut n.north_east, 1);
    move_y(&mut n.north_east, 1);

    move_x(&mut n.south_east, 1);
    move_y(&mut n.south_east, -1);

    move_x(&mut n.south_west, -1);
    move_y(&mut n.south_west, -1);

    n
}

/// Estimate the step (bits precision) of the 9 search area boxes during radius
/// queries.
pub fn estimate_steps_by_radius(range_meters: f64, lat: f64) -> u8 {
    if range_meters == 0.0 {
        return GEO_STEP_MAX;
    }
    let mut step = 1i32;
    let mut r = range_meters;
    while r < MERCATOR_MAX {
        r *= 2.0;
        step += 1;
    }
    step -= 2; // Make sure range is included in most of the base cases.

    // Wider range towards the poles.
    if lat > 66.0 || lat < -66.0 {
        step -= 1;
        if lat > 80.0 || lat < -80.0 {
            step -= 1;
        }
    }

    step.clamp(1, 26) as u8
}

/// A search area: either a circle (center + radius) or an axis-aligned box
/// (center + width x height). All distances are in the raw input unit;
/// `conversion` converts them to meters (KM -> 1000, etc.).
#[derive(Debug, Clone, Copy)]
pub enum GeoShape {
    Circle { xy: [f64; 2], radius: f64, conversion: f64 },
    Rect { xy: [f64; 2], width: f64, height: f64, conversion: f64 },
}

impl GeoShape {
    fn center(&self) -> [f64; 2] {
        match self {
            GeoShape::Circle { xy, .. } => *xy,
            GeoShape::Rect { xy, .. } => *xy,
        }
    }

    fn conversion(&self) -> f64 {
        match self {
            GeoShape::Circle { conversion, .. } => *conversion,
            GeoShape::Rect { conversion, .. } => *conversion,
        }
    }
}

/// Bounding box of the search area. Returns `[min_lon, min_lat, max_lon,
/// max_lat]` in degrees.
pub fn bounding_box(shape: &GeoShape) -> [f64; 4] {
    let (longitude, latitude) = {
        let [lon, lat] = shape.center();
        (lon, lat)
    };
    let (height, width) = match shape {
        GeoShape::Circle { radius, .. } => (*radius, *radius),
        GeoShape::Rect { width, height, .. } => (height / 2.0, width / 2.0),
    };
    let height = shape.conversion() * height;
    let width = shape.conversion() * width;

    let lat_delta = rad_deg(height / EARTH_RADIUS_IN_METERS);
    let long_delta_top = rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(latitude + lat_delta).cos());
    let long_delta_bottom = rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(latitude - lat_delta).cos());

    // The directions of the northern and southern hemispheres are opposite.
    let southern = latitude < 0.0;
    let mut bounds = [0.0; 4];
    bounds[0] = if southern { longitude - long_delta_bottom } else { longitude - long_delta_top };
    bounds[2] = if southern { longitude + long_delta_bottom } else { longitude + long_delta_top };
    bounds[1] = latitude - lat_delta;
    bounds[3] = latitude + lat_delta;
    bounds
}

/// Calculate a set of areas (center + 8) that cover a range query for the
/// specified position and shape.
pub fn calculate_areas_by_shape(shape: &GeoShape) -> GeoHashRadius {
    let (long_range, lat_range) = coord_range();
    let [min_lon, min_lat, max_lon, max_lat] = bounding_box(shape);
    let [longitude, latitude] = shape.center();

    // radius_meters is calculated differently in different search types.
    let radius_meters = match shape {
        GeoShape::Circle { radius, .. } => *radius,
        GeoShape::Rect { width, height, .. } => {
            ((width / 2.0) * (width / 2.0) + (height / 2.0) * (height / 2.0)).sqrt()
        }
    } * shape.conversion();

    let mut steps = estimate_steps_by_radius(radius_meters, latitude);

    let mut hash = encode(&long_range, &lat_range, longitude, latitude, steps).expect("valid coords");
    let mut neigh = neighbors(hash);
    let mut area = decode(long_range, lat_range, hash).expect("non-zero hash");

    // Check if the step is enough at the limits of the covered area.
    let mut decrease_step = false;
    {
        let north = decode(long_range, lat_range, neigh.north).unwrap_or_default();
        let south = decode(long_range, lat_range, neigh.south).unwrap_or_default();
        let east = decode(long_range, lat_range, neigh.east).unwrap_or_default();
        let west = decode(long_range, lat_range, neigh.west).unwrap_or_default();

        if north.latitude.max < max_lat {
            decrease_step = true;
        }
        if south.latitude.min > min_lat {
            decrease_step = true;
        }
        if east.longitude.max < max_lon {
            decrease_step = true;
        }
        if west.longitude.min > min_lon {
            decrease_step = true;
        }
    }

    if steps > 1 && decrease_step {
        steps -= 1;
        hash = encode(&long_range, &lat_range, longitude, latitude, steps).expect("valid coords");
        neigh = neighbors(hash);
        area = decode(long_range, lat_range, hash).expect("non-zero hash");
    }

    let zero = |n: &mut GeoHashBits| {
        n.bits = 0;
        n.step = 0;
    };
    // Exclude the search areas that are useless.
    if steps >= 2 {
        if area.latitude.min < min_lat {
            zero(&mut neigh.south);
            zero(&mut neigh.south_west);
            zero(&mut neigh.south_east);
        }
        if area.latitude.max > max_lat {
            zero(&mut neigh.north);
            zero(&mut neigh.north_east);
            zero(&mut neigh.north_west);
        }
        if area.longitude.min < min_lon {
            zero(&mut neigh.west);
            zero(&mut neigh.south_west);
            zero(&mut neigh.north_west);
        }
        if area.longitude.max > max_lon {
            zero(&mut neigh.east);
            zero(&mut neigh.south_east);
            zero(&mut neigh.north_east);
        }
    }

    GeoHashRadius { hash, area, neighbors: neigh }
}

pub fn align_52_bits(hash: GeoHashBits) -> u64 {
    hash.bits << (52 - hash.step as u64 * 2)
}

/// The sorted-set scores min (inclusive), max (exclusive) to query to retrieve
/// all elements inside the specified area 'hash'.
pub fn scores_of_geo_hash_box(hash: GeoHashBits) -> (u64, u64) {
    let min = align_52_bits(hash);
    let max = align_52_bits(GeoHashBits { bits: hash.bits.wrapping_add(1), step: hash.step });
    (min, max)
}

/// Distance between two lat/lon points along the same meridian (meters).
pub fn lat_distance(lat1: f64, lat2: f64) -> f64 {
    EARTH_RADIUS_IN_METERS * (deg_rad(lat2) - deg_rad(lat1)).abs()
}

/// Simplified haversine great-circle distance in meters.
pub fn haversine(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lon1r = deg_rad(lon1);
    let lon2r = deg_rad(lon2);
    let v = ((lon2r - lon1r) / 2.0).sin();
    // if v == 0 we can avoid doing expensive math when lons are practically the same
    if v == 0.0 {
        return lat_distance(lat1, lat2);
    }
    let lat1r = deg_rad(lat1);
    let lat2r = deg_rad(lat2);
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;
    2.0 * EARTH_RADIUS_IN_METERS * a.sqrt().asin()
}

/// Given a zset score representing a point, check if it's within the search
/// area. Returns `(xy, distance)` on success.
pub fn within_shape(shape: &GeoShape, score: f64) -> Option<([f64; 2], f64)> {
    let hash = GeoHashBits { bits: score as u64, step: GEO_STEP_MAX };
    let xy = decode_to_long_lat(hash)?;
    let distance = match shape {
        GeoShape::Circle { xy: [cx, cy], radius, conversion } => {
            let d = haversine(*cx, *cy, xy[0], xy[1]);
            if d > radius * conversion {
                return None;
            }
            d
        }
        GeoShape::Rect { xy: [cx, cy], width, height, conversion } => {
            // Latitude distance is cheaper to compute than longitude distance.
            let lat_distance = lat_distance(xy[1], *cy);
            if lat_distance > height * conversion / 2.0 {
                return None;
            }
            let lon_distance = haversine(xy[0], xy[1], *cx, xy[1]);
            if lon_distance > width * conversion / 2.0 {
                return None;
            }
            haversine(*cx, *cy, xy[0], xy[1])
        }
    };
    Some((xy, distance))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interleave_roundtrip() {
        let x = 0x1234567u32;
        let y = 0x89abcdeu32;
        assert_eq!(deinterleave64(interleave64(x, y)), (x as u64) | ((y as u64) << 32));
    }

    #[test]
    fn palermo_score_matches_reference() {
        // GEOADD Sicily 13.361389 38.115556 "Palermo" -> score 3479099956230698.
        let hash = encode_wgs84(13.361389, 38.115556, GEO_STEP_MAX).unwrap();
        assert_eq!(align_52_bits(hash), 3479099956230698);
    }

    #[test]
    fn berlin_score_matches_reference() {
        // GEOADD berlin 13.388859 52.517036 -> score 3673983848512388 (live-probed).
        let hash = encode_wgs84(13.388859, 52.517036, GEO_STEP_MAX).unwrap();
        assert_eq!(align_52_bits(hash), 3673983848512388);
    }

    #[test]
    fn decode_roundtrip_berlin() {
        // Live-probed GEOPOS berlin: 13.388860523700714 / 52.51703598153155.
        let xy = decode_to_long_lat(GeoHashBits { bits: 3673983848512388, step: GEO_STEP_MAX }).unwrap();
        assert!((xy[0] - 13.388860523700714).abs() < 1e-15, "got {xy:?}");
        assert!((xy[1] - 52.51703598153155).abs() < 1e-15, "got {xy:?}");
    }

    #[test]
    fn decode_roundtrip_palermo() {
        let xy = decode_to_long_lat(GeoHashBits { bits: 3479099956230698, step: GEO_STEP_MAX }).unwrap();
        // Reference test expects 13.361389338970184 / 38.1155563954963.
        assert!((xy[0] - 13.361389338970184).abs() < 1e-12, "got {xy:?}");
        assert!((xy[1] - 38.1155563954963).abs() < 1e-12, "got {xy:?}");
    }

    #[test]
    fn vienna_budapest_amsterdam_scores() {
        // All live-probed against redis-server 8.8.1 (identical geohash.c).
        let cases = [
            (16.3738, 48.2082, 3673109837845971),    // Vienna
            (19.040236, 47.497913, 3671790573640182), // Budapest
            (4.895168, 52.370216, 3665667490693366),  // Amsterdam
        ];
        for (lon, lat, expected) in cases {
            let hash = encode_wgs84(lon, lat, GEO_STEP_MAX).unwrap();
            assert_eq!(align_52_bits(hash), expected, "lon={lon} lat={lat}");
        }
    }

    #[test]
    fn haversine_berlin_vienna() {
        // GEODIST semantics: decode both members, then haversine.
        let a = decode_to_long_lat(GeoHashBits { bits: 3673983848512388, step: GEO_STEP_MAX }).unwrap();
        let b = decode_to_long_lat(GeoHashBits { bits: 3673109837845971, step: GEO_STEP_MAX }).unwrap();
        let d = haversine(a[0], a[1], b[0], b[1]);
        // Live-probed GEODIST g berlin vienna m -> 523854.2444.
        assert!((d - 523854.244365).abs() < 1e-3, "got {d}");
    }

    #[test]
    fn haversine_same_lon_uses_lat_distance() {
        // Same longitude: haversine falls back to pure latitude distance.
        let d = haversine(13.0, 52.0, 13.0, 53.0);
        let expected = lat_distance(52.0, 53.0);
        assert_eq!(d, expected);
    }

    #[test]
    fn box_scores_berlin_rect() {
        // 1000 m circle around Berlin: step 14, box [3673983845138432, 3673983861915648).
        let shape = GeoShape::Circle { xy: [13.388859, 52.517036], radius: 1000.0, conversion: 1.0 };
        let radius = calculate_areas_by_shape(&shape);
        assert_eq!(radius.hash.step, 14);
        let (min, max) = scores_of_geo_hash_box(radius.hash);
        assert_eq!(min, 3673983845138432);
        assert_eq!(max, 3673983861915648);
        assert!(min <= 3673983848512388 && 3673983848512388 < max);
    }

    #[test]
    fn within_shape_berlin_1000m() {
        let shape = GeoShape::Circle { xy: [13.388859, 52.517036], radius: 1000.0, conversion: 1.0 };
        assert!(within_shape(&shape, 3673983848512388.0).is_some());
        // Vienna is ~524 km away.
        assert!(within_shape(&shape, 3673109837845971.0).is_none());
    }

    #[test]
    fn estimate_steps_by_radius_matches_reference() {
        // 1000 m radius at lat 52.5: log2(20037726.37/1000) doublings -> step 14.
        assert_eq!(estimate_steps_by_radius(1000.0, 52.5), 14);
        assert_eq!(estimate_steps_by_radius(0.0, 52.5), 26);
        // Huge radius clamps to 1.
        assert_eq!(estimate_steps_by_radius(50_000_000.0, 52.5), 1);
    }
}
