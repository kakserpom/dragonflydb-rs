//! Geohash encoding/decoding and geo search math, byte-for-byte compatible with
//! `dragonfly/src/redis/geohash.{c,h}` and `geohash_helper.{c,h}` (yinqiwen /
//! Matt Stancliff / Salvatore Sanfilippo).
//!
//! A geo point is stored as a `ZSet` score: the 52-bit interleaved geohash at
//! `GEO_STEP_MAX` (26) precision, i.e. `interleave64(lat_offset, long_offset)`
//! scaled to a 26-bit fixed point over the WGS84 (mercator) lat/long ranges.

pub const GEO_LAT_MIN: f64 = -85.051_128_78;
pub const GEO_LAT_MAX: f64 = 85.051_128_78;
pub const GEO_LONG_MIN: f64 = -180.0;
pub const GEO_LONG_MAX: f64 = 180.0;

/// 26 * 2 = 52 bits: the precision used for stored scores.
pub const GEO_STEP_MAX: u8 = 26;

pub const EARTH_RADIUS_IN_METERS: f64 = 6_372_797.560_856;
pub const MERCATOR_MAX: f64 = 20_037_726.37;

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

#[must_use]
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
        0x5555_5555_5555_5555_u64,
        0x3333_3333_3333_3333,
        0x0F0F_0F0F_0F0F_0F0F,
        0x00FF_00FF_00FF_00FF,
        0x0000_FFFF_0000_FFFF,
    ];
    let s = [1u32, 2, 4, 8, 16];

    let mut x = u64::from(xlo);
    let mut y = u64::from(ylo);

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
        0x5555_5555_5555_5555_u64,
        0x3333_3333_3333_3333,
        0x0F0F_0F0F_0F0F_0F0F,
        0x00FF_00FF_00FF_00FF,
        0x0000_FFFF_0000_FFFF,
        0x0000_0000_FFFF_FFFF,
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
#[must_use]
pub fn coord_range() -> (GeoHashRange, GeoHashRange) {
    (
        GeoHashRange {
            min: GEO_LONG_MIN,
            max: GEO_LONG_MAX,
        },
        GeoHashRange {
            min: GEO_LAT_MIN,
            max: GEO_LAT_MAX,
        },
    )
}

#[must_use]
pub fn encode(
    long_range: &GeoHashRange,
    lat_range: &GeoHashRange,
    longitude: f64,
    latitude: f64,
    step: u8,
) -> Option<GeoHashBits> {
    if step > 32
        || step == 0
        || (lat_range.min == 0.0 && lat_range.max == 0.0)
        || (long_range.min == 0.0 && long_range.max == 0.0)
    {
        return None;
    }

    // Return an error when trying to index outside the supported constraints.
    if !(GEO_LONG_MIN..=GEO_LONG_MAX).contains(&longitude)
        || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&latitude)
    {
        return None;
    }

    if latitude < lat_range.min
        || latitude > lat_range.max
        || longitude < long_range.min
        || longitude > long_range.max
    {
        return None;
    }

    let lat_offset = (latitude - lat_range.min) / (lat_range.max - lat_range.min);
    let long_offset = (longitude - long_range.min) / (long_range.max - long_range.min);

    // Convert to fixed point based on the step size.
    let scale = (1u64 << step) as f64;
    let lat_offset = (lat_offset * scale) as u32;
    let long_offset = (long_offset * scale) as u32;

    Some(GeoHashBits {
        bits: interleave64(lat_offset, long_offset),
        step,
    })
}

#[must_use]
pub fn encode_wgs84(longitude: f64, latitude: f64, step: u8) -> Option<GeoHashBits> {
    let (long_range, lat_range) = coord_range();
    encode(&long_range, &lat_range, longitude, latitude, step)
}

#[must_use]
pub fn decode(
    long_range: GeoHashRange,
    lat_range: GeoHashRange,
    hash: GeoHashBits,
) -> Option<GeoHashArea> {
    if hash_is_zero(hash)
        || (lat_range.min == 0.0 && lat_range.max == 0.0)
        || (long_range.min == 0.0 && long_range.max == 0.0)
    {
        return None;
    }

    let step = u64::from(hash.step);
    let hash_sep = deinterleave64(hash.bits); // hash = [lat][long]

    let lat_scale = lat_range.max - lat_range.min;
    let long_scale = long_range.max - long_range.min;

    let ilato = hash_sep as u32; // get lat part of deinterleaved hash
    let ilono = (hash_sep >> 32) as u32; // shift over to get long part of hash

    let scale = 1u64 << step;
    let area = GeoHashArea {
        hash,
        latitude: GeoHashRange {
            min: lat_range.min + (f64::from(ilato) / scale as f64) * lat_scale,
            max: lat_range.min + (f64::from(ilato.wrapping_add(1)) / scale as f64) * lat_scale,
        },
        longitude: GeoHashRange {
            min: long_range.min + (f64::from(ilono) / scale as f64) * long_scale,
            max: long_range.min + (f64::from(ilono.wrapping_add(1)) / scale as f64) * long_scale,
        },
    };
    Some(area)
}

#[must_use]
pub fn decode_wgs84(hash: GeoHashBits) -> Option<GeoHashArea> {
    let (long_range, lat_range) = coord_range();
    decode(long_range, lat_range, hash)
}

#[must_use]
pub fn area_to_long_lat(area: &GeoHashArea) -> [f64; 2] {
    let mut lon = f64::midpoint(area.longitude.min, area.longitude.max);
    let mut lat = f64::midpoint(area.latitude.min, area.latitude.max);
    lon = lon.clamp(GEO_LONG_MIN, GEO_LONG_MAX);
    lat = lat.clamp(GEO_LAT_MIN, GEO_LAT_MAX);
    [lon, lat]
}

#[must_use]
pub fn decode_to_long_lat(hash: GeoHashBits) -> Option<[f64; 2]> {
    let area = decode_wgs84(hash)?;
    Some(area_to_long_lat(&area))
}

fn move_x(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }

    let mut x = hash.bits & 0xaaaa_aaaa_aaaa_aaaa;
    let y = hash.bits & 0x5555_5555_5555_5555;

    let zz = 0x5555_5555_5555_5555_u64 >> (64 - u64::from(hash.step) * 2);

    if d > 0 {
        x = x.wrapping_add(zz + 1);
    } else {
        x |= zz;
        x = x.wrapping_sub(zz + 1);
    }

    x &= 0xaaaa_aaaa_aaaa_aaaa >> (64 - u64::from(hash.step) * 2);
    hash.bits = x | y;
}

fn move_y(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }

    let x = hash.bits & 0xaaaa_aaaa_aaaa_aaaa;
    let mut y = hash.bits & 0x5555_5555_5555_5555;

    let zz = 0xaaaa_aaaa_aaaa_aaaa_u64 >> (64 - u64::from(hash.step) * 2);
    if d > 0 {
        y = y.wrapping_add(zz + 1);
    } else {
        y |= zz;
        y = y.wrapping_sub(zz + 1);
    }
    y &= 0x5555_5555_5555_5555 >> (64 - u64::from(hash.step) * 2);
    hash.bits = x | y;
}

#[must_use]
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
#[must_use]
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
    if !(-66.0..=66.0).contains(&lat) {
        step -= 1;
        if !(-80.0..=80.0).contains(&lat) {
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
    Circle {
        xy: [f64; 2],
        radius: f64,
        conversion: f64,
    },
    Rect {
        xy: [f64; 2],
        width: f64,
        height: f64,
        conversion: f64,
    },
}

impl GeoShape {
    fn center(&self) -> [f64; 2] {
        match self {
            GeoShape::Circle { xy, .. } | GeoShape::Rect { xy, .. } => *xy,
        }
    }

    fn conversion(&self) -> f64 {
        match self {
            GeoShape::Circle { conversion, .. } | GeoShape::Rect { conversion, .. } => *conversion,
        }
    }
}

/// Bounding box of the search area. Returns `[min_lon, min_lat, max_lon,
/// max_lat]` in degrees.
#[must_use]
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
    let long_delta_top =
        rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(latitude + lat_delta).cos());
    let long_delta_bottom =
        rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(latitude - lat_delta).cos());

    // The directions of the northern and southern hemispheres are opposite.
    let southern = latitude < 0.0;
    let mut bounds = [0.0; 4];
    bounds[0] = if southern {
        longitude - long_delta_bottom
    } else {
        longitude - long_delta_top
    };
    bounds[2] = if southern {
        longitude + long_delta_bottom
    } else {
        longitude + long_delta_top
    };
    bounds[1] = latitude - lat_delta;
    bounds[3] = latitude + lat_delta;
    bounds
}

/// Calculate a set of areas (center + 8) that cover a range query for the
/// specified position and shape.
#[must_use]
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

    let mut hash =
        encode(&long_range, &lat_range, longitude, latitude, steps).expect("valid coords");
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

    GeoHashRadius {
        hash,
        area,
        neighbors: neigh,
    }
}

#[must_use]
pub fn align_52_bits(hash: GeoHashBits) -> u64 {
    hash.bits << (52 - u64::from(hash.step) * 2)
}

/// The sorted-set scores min (inclusive), max (exclusive) to query to retrieve
/// all elements inside the specified area 'hash'.
#[must_use]
pub fn scores_of_geo_hash_box(hash: GeoHashBits) -> (u64, u64) {
    let min = align_52_bits(hash);
    let max = align_52_bits(GeoHashBits {
        bits: hash.bits.wrapping_add(1),
        step: hash.step,
    });
    (min, max)
}

/// Distance between two lat/lon points along the same meridian (meters).
#[must_use]
pub fn lat_distance(lat1: f64, lat2: f64) -> f64 {
    EARTH_RADIUS_IN_METERS * (deg_rad(lat2) - deg_rad(lat1)).abs()
}

/// Simplified haversine great-circle distance in meters.
#[must_use]
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
#[must_use]
pub fn within_shape(shape: &GeoShape, score: f64) -> Option<([f64; 2], f64)> {
    let hash = GeoHashBits {
        bits: score as u64,
        step: GEO_STEP_MAX,
    };
    let xy = decode_to_long_lat(hash)?;
    let distance = match shape {
        GeoShape::Circle {
            xy: [cx, cy],
            radius,
            conversion,
        } => {
            let d = haversine(*cx, *cy, xy[0], xy[1]);
            if d > radius * conversion {
                return None;
            }
            d
        }
        GeoShape::Rect {
            xy: [cx, cy],
            width,
            height,
            conversion,
        } => {
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
        let x = 0x0123_4567_u32;
        let y = 0x089a_bcde_u32;
        assert_eq!(
            deinterleave64(interleave64(x, y)),
            (x as u64) | ((y as u64) << 32)
        );
    }

    #[test]
    fn palermo_score_matches_reference() {
        // GEOADD Sicily 13.361389 38.115556 "Palermo" -> score 3479099956230698.
        let hash = encode_wgs84(13.361_389, 38.115_556, GEO_STEP_MAX).unwrap();
        assert_eq!(align_52_bits(hash), 3_479_099_956_230_698);
    }

    #[test]
    fn berlin_score_matches_reference() {
        // GEOADD berlin 13.388859 52.517036 -> score 3673983848512388 (live-probed).
        let hash = encode_wgs84(13.388_859, 52.517_036, GEO_STEP_MAX).unwrap();
        assert_eq!(align_52_bits(hash), 3_673_983_848_512_388);
    }

    #[test]
    fn decode_roundtrip_berlin() {
        // Live-probed GEOPOS berlin: 13.388860523700714 / 52.51703598153155.
        let xy = decode_to_long_lat(GeoHashBits {
            bits: 3_673_983_848_512_388,
            step: GEO_STEP_MAX,
        })
        .unwrap();
        assert!((xy[0] - 13.388_860_523_700_714).abs() < 1e-15, "got {xy:?}");
        assert!((xy[1] - 52.517_035_981_531_55).abs() < 1e-15, "got {xy:?}");
    }

    #[test]
    fn decode_roundtrip_palermo() {
        let xy = decode_to_long_lat(GeoHashBits {
            bits: 3_479_099_956_230_698,
            step: GEO_STEP_MAX,
        })
        .unwrap();
        // Reference test expects 13.361389338970184 / 38.1155563954963.
        assert!((xy[0] - 13.361_389_338_970_184).abs() < 1e-12, "got {xy:?}");
        assert!((xy[1] - 38.115_556_395_496_3).abs() < 1e-12, "got {xy:?}");
    }

    #[test]
    fn vienna_budapest_amsterdam_scores() {
        // All live-probed against redis-server 8.8.1 (identical geohash.c).
        let cases = [
            (16.3738, 48.2082, 3_673_109_837_845_971),       // Vienna
            (19.040_236, 47.497_913, 3_671_790_573_640_182), // Budapest
            (4.895_168, 52.370_216, 3_665_667_490_693_366),  // Amsterdam
        ];
        for (lon, lat, expected) in cases {
            let hash = encode_wgs84(lon, lat, GEO_STEP_MAX).unwrap();
            assert_eq!(align_52_bits(hash), expected, "lon={lon} lat={lat}");
        }
    }

    #[test]
    fn haversine_berlin_vienna() {
        // GEODIST semantics: decode both members, then haversine.
        let a = decode_to_long_lat(GeoHashBits {
            bits: 3_673_983_848_512_388,
            step: GEO_STEP_MAX,
        })
        .unwrap();
        let b = decode_to_long_lat(GeoHashBits {
            bits: 3_673_109_837_845_971,
            step: GEO_STEP_MAX,
        })
        .unwrap();
        let d = haversine(a[0], a[1], b[0], b[1]);
        // Live-probed GEODIST g berlin vienna m -> 523854.2444.
        assert!((d - 523_854.244_365).abs() < 1e-3, "got {d}");
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
        let shape = GeoShape::Circle {
            xy: [13.388_859, 52.517_036],
            radius: 1000.0,
            conversion: 1.0,
        };
        let radius = calculate_areas_by_shape(&shape);
        assert_eq!(radius.hash.step, 14);
        let (min, max) = scores_of_geo_hash_box(radius.hash);
        assert_eq!(min, 3_673_983_845_138_432);
        assert_eq!(max, 3_673_983_861_915_648);
        assert!(min <= 3_673_983_848_512_388 && 3_673_983_848_512_388 < max);
    }

    #[test]
    fn within_shape_berlin_1000m() {
        let shape = GeoShape::Circle {
            xy: [13.388_859, 52.517_036],
            radius: 1000.0,
            conversion: 1.0,
        };
        assert!(within_shape(&shape, 3_673_983_848_512_388.0).is_some());
        // Vienna is ~524 km away.
        assert!(within_shape(&shape, 3_673_109_837_845_971.0).is_none());
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
