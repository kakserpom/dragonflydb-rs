//! Fixed-boundary histogram ported from `helio/base/histogram.h` (itself
//! derived from the LevelDB histogram). Used for `SCRIPT LATENCY`, which the
//! reference renders by calling `base::Histogram::ToString()` on the per-SHA
//! histogram (`ScriptMgr::LatencyCmd` in `script_mgr.cc`).
//!
//! The reference keeps one histogram per script SHA per shard and merges them
//! before printing; the port records into a single per-SHA histogram on the
//! coordinator, so no merge is needed.

use std::fmt;

/// Number of buckets (the reference's `kNumBuckets`).
const NUM_BUCKETS: usize = 154;

/// Exclusive bucket upper limits; the last entry doubles as the default
/// `min_` sentinel (`Histogram::Clear()`).
const BUCKET_LIMITS: [f64; NUM_BUCKETS] = [
    1.,
    2.,
    3.,
    4.,
    5.,
    6.,
    7.,
    8.,
    9.,
    10.,
    12.,
    14.,
    16.,
    18.,
    20.,
    25.,
    30.,
    35.,
    40.,
    45.,
    50.,
    60.,
    70.,
    80.,
    90.,
    100.,
    120.,
    140.,
    160.,
    180.,
    200.,
    250.,
    300.,
    350.,
    400.,
    450.,
    500.,
    600.,
    700.,
    800.,
    900.,
    1000.,
    1200.,
    1400.,
    1600.,
    1800.,
    2000.,
    2500.,
    3000.,
    3500.,
    4000.,
    4500.,
    5000.,
    6000.,
    7000.,
    8000.,
    9000.,
    10_000.,
    12_000.,
    14_000.,
    16_000.,
    18_000.,
    20_000.,
    25_000.,
    30_000.,
    35_000.,
    40_000.,
    45_000.,
    50_000.,
    60_000.,
    70_000.,
    80_000.,
    90_000.,
    100_000.,
    120_000.,
    140_000.,
    160_000.,
    180_000.,
    200_000.,
    250_000.,
    300_000.,
    350_000.,
    400_000.,
    450_000.,
    500_000.,
    600_000.,
    700_000.,
    800_000.,
    900_000.,
    1_000_000.,
    1_200_000.,
    1_400_000.,
    1_600_000.,
    1_800_000.,
    2_000_000.,
    2_500_000.,
    3_000_000.,
    3_500_000.,
    4_000_000.,
    4_500_000.,
    5_000_000.,
    6_000_000.,
    7_000_000.,
    8_000_000.,
    9_000_000.,
    10_000_000.,
    12_000_000.,
    14_000_000.,
    16_000_000.,
    18_000_000.,
    20_000_000.,
    25_000_000.,
    30_000_000.,
    35_000_000.,
    40_000_000.,
    45_000_000.,
    50_000_000.,
    60_000_000.,
    70_000_000.,
    80_000_000.,
    90_000_000.,
    100_000_000.,
    120_000_000.,
    140_000_000.,
    160_000_000.,
    180_000_000.,
    200_000_000.,
    250_000_000.,
    300_000_000.,
    350_000_000.,
    400_000_000.,
    450_000_000.,
    500_000_000.,
    600_000_000.,
    700_000_000.,
    800_000_000.,
    900_000_000.,
    1_000_000_000.,
    1_200_000_000.,
    1_400_000_000.,
    1_600_000_000.,
    1_800_000_000.,
    2_000_000_000.,
    2_500_000_000.,
    3_000_000_000.,
    3_500_000_000.,
    4_000_000_000.,
    4_500_000_000.,
    5_000_000_000.,
    6_000_000_000.,
    7_000_000_000.,
    8_000_000_000.,
    9_000_000_000.,
    1e200,
];

/// A LevelDB-style histogram: fixed `[low, high)` buckets over a log-scale
/// boundary table, recording count/sum/min/max. Formatting matches the
/// reference byte-for-byte so `SCRIPT LATENCY` output is identical.
#[derive(Debug, Clone)]
pub struct Histogram {
    min: f64,
    max: f64,
    sum: f64,
    num: u64,
    buckets: Vec<u64>,
}

impl Default for Histogram {
    fn default() -> Self {
        Histogram {
            // `Histogram::Clear()` sets `min_` to the last bucket limit.
            min: BUCKET_LIMITS[NUM_BUCKETS - 1],
            max: 0.0,
            sum: 0.0,
            num: 0,
            buckets: Vec::new(),
        }
    }
}

impl Histogram {
    /// Record one observation (`Histogram::Add(double value, uint64_t count)`).
    pub fn add(&mut self, value: f64) {
        // `upper_bound` over the first `kNumBuckets - 1` limits: values at or
        // above the last limit land in the final (INF) bucket.
        let b = BUCKET_LIMITS[..NUM_BUCKETS - 1].partition_point(|&limit| limit <= value);
        if self.buckets.len() <= b {
            self.buckets.resize(b + 1, 0);
        }
        self.buckets[b] += 1;
        if self.min > value {
            self.min = value;
        }
        if self.max < value {
            self.max = value;
        }
        self.num += 1;
        self.sum += value;
    }

    #[must_use]
    pub fn count(&self) -> u64 {
        self.num
    }

    #[must_use]
    pub fn sum(&self) -> f64 {
        self.sum
    }

    #[must_use]
    pub fn min(&self) -> f64 {
        self.min
    }

    #[must_use]
    pub fn max(&self) -> f64 {
        self.max
    }

    /// Arithmetic mean (`Histogram::Average()`); 0 for an empty histogram.
    #[must_use]
    pub fn average(&self) -> f64 {
        if self.num == 0 {
            0.0
        } else {
            self.sum / self.num as f64
        }
    }

    /// `p`-th percentile with within-bucket interpolation
    /// (`Histogram::Percentile`); 0 for an empty histogram.
    #[must_use]
    pub fn percentile(&self, p: f64) -> f64 {
        if self.num == 0 {
            return 0.0;
        }
        // `uint64_t threshold = num_ * (p / 100.0);` truncates.
        let threshold = (self.num as f64 * (p / 100.0)) as u64;
        let mut sum = 0u64;
        for b in 0..self.buckets.len() {
            sum += self.buckets[b];
            if sum >= threshold {
                let left_sum = sum - self.buckets[b];
                return self.interpolate_val(b, threshold - left_sum);
            }
        }
        self.max
    }

    #[must_use]
    pub fn median(&self) -> f64 {
        self.percentile(50.0)
    }

    /// `(low, high)` bounds of bucket `b` (`BucketLimits`).
    fn bucket_limits(b: usize) -> (f64, f64) {
        let low = if b == 0 { 0.0 } else { BUCKET_LIMITS[b - 1] };
        (low, BUCKET_LIMITS[b])
    }

    /// `InterpolateVal`: assume the value's position inside its bucket follows
    /// an order-statistic distribution, then clamp to the observed range.
    fn interpolate_val(&self, bucket: usize, position: u64) -> f64 {
        let (low, high) = Self::bucket_limits(bucket);
        let pos = position as f64 / (self.buckets[bucket] + 1) as f64;
        let mut r = low + (high - low) * pos;
        if r < self.min {
            r = self.min;
        }
        if r > self.max {
            r = self.max;
        }
        r
    }
}

impl fmt::Display for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Count: {} Average: {:.4}", self.num, self.average())?;
        writeln!(
            f,
            "Min: {:.4}  Median: {:.4}  Max: {:.4}",
            if self.num == 0 { 0.0 } else { self.min },
            self.median(),
            self.max
        )?;
        f.write_str("------------------------------------------------------\n")?;
        let mult = 100.0 / self.num as f64;
        let mut cum = 0.0f64;
        for (b, &count) in self.buckets.iter().enumerate() {
            if (count as f64) <= 0.01 {
                continue;
            }
            cum += count as f64;
            let from = if b == 0 { 0.0 } else { BUCKET_LIMITS[b - 1] };
            let to = if b == NUM_BUCKETS - 1 {
                "INF".to_string()
            } else {
                format!("{:7.0}", BUCKET_LIMITS[b])
            };
            write!(
                f,
                "[ {:7.0}, {} ) {} {:7.3}% {:7.3}% ",
                from,
                to,
                count,
                mult * count as f64,
                mult * cum
            )?;
            // `static_cast<int>(20 * (buckets_[b] / num_) + 0.5)`.
            let marks = (20.0 * (count as f64 / self.num as f64) + 0.5) as usize;
            f.write_str(&"#".repeat(marks))?;
            f.write_str("\n")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Histogram;

    #[test]
    fn aggregates_counts_and_sum() {
        let mut h = Histogram::default();
        assert_eq!(h.count(), 0);
        assert_eq!(h.sum(), 0.0);
        h.add(100.0);
        h.add(300.0);
        assert_eq!(h.count(), 2);
        assert_eq!(h.sum(), 400.0);
        assert_eq!(h.min(), 100.0);
        assert_eq!(h.max(), 300.0);
        assert_eq!(h.average(), 200.0);
    }

    #[test]
    fn percentiles_interpolate_within_buckets() {
        let mut h = Histogram::default();
        h.add(100.0);
        // Threshold 1.0 lands at the second sample inside bucket (100, 120]:
        // pos = 1/2 → 110, clamped to the observed max of 300 → no clamp here.
        h.add(300.0);
        assert_eq!(h.median(), 110.0);
        h.add(100.0);
        // Sorted: 100, 100, 300. Threshold 1.5 → 1 lands at the first sample
        // inside bucket (100, 120], interpolated over its 2 samples: 106.667.
        assert_eq!(h.percentile(50.0), 106.666_666_666_666_67);
        assert_eq!(h.percentile(100.0), 300.0);
    }

    #[test]
    fn to_string_single_sample() {
        let mut h = Histogram::default();
        h.add(100.0);
        assert_eq!(
            h.to_string(),
            "Count: 1 Average: 100.0000\n\
             Min: 100.0000  Median: 100.0000  Max: 100.0000\n\
             ------------------------------------------------------\n\
             [     100,     120 ) 1 100.000% 100.000% ####################\n"
        );
    }

    #[test]
    fn to_string_two_samples() {
        let mut h = Histogram::default();
        h.add(100.0);
        h.add(300.0);
        assert_eq!(
            h.to_string(),
            "Count: 2 Average: 200.0000\n\
             Min: 100.0000  Median: 110.0000  Max: 300.0000\n\
             ------------------------------------------------------\n\
             [     100,     120 ) 1  50.000%  50.000% ##########\n\
             [     300,     350 ) 1  50.000% 100.000% ##########\n"
        );
    }

    #[test]
    fn to_string_empty() {
        let h = Histogram::default();
        assert_eq!(
            h.to_string(),
            "Count: 0 Average: 0.0000\n\
             Min: 0.0000  Median: 0.0000  Max: 0.0000\n\
             ------------------------------------------------------\n"
        );
    }
}
