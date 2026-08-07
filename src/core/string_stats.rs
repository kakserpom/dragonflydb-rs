//! Port of `server/string_stats.{h,cc}`: `DEBUG UNIQ-STRS`'s per-object-type
//! dedup accounting. `UniqueStrings` counts every entry of a container into a
//! dense HyperLogLog plus byte counters, and estimates the savings of
//! deduplicating repeated values.

use std::fmt::Write as _;

use crate::core::hash::Hash;
use crate::core::hll::{HLL_DENSE_SIZE, create_dense_hll, pfadd_dense, pfcount_single, pfmerge};
use crate::core::quicklist::{ListItem, QuickList};
use crate::core::set::Set;
use crate::core::zset::ZSet;
use crate::util::g6_format;

/// `UniqueStrings` (string_stats.h): `total_count`/`total_bytes` counters plus
/// a dense HLL cardinality estimate over the distinct entry values.
#[derive(Debug, Clone)]
pub struct UniqueStrings {
    total_count: u64,
    total_bytes: u64,
    hll: Vec<u8>,
}

impl Default for UniqueStrings {
    fn default() -> Self {
        Self::new()
    }
}

impl UniqueStrings {
    #[must_use]
    pub fn new() -> Self {
        UniqueStrings {
            total_count: 0,
            total_bytes: 0,
            hll: create_dense_hll(),
        }
    }

    /// `AddString` (string_stats.cc:107): count one entry, feeding its bytes —
    /// or the decimal rendering of an int, which benefits from deduplication
    /// just like a string key — into the HLL.
    fn add_bytes(&mut self, bytes: &[u8]) {
        pfadd_dense(&mut self.hll, bytes);
        self.total_count += 1;
        self.total_bytes += bytes.len() as u64;
    }

    /// `AddHMap` (string_stats.cc:74): only the field names of a hash.
    pub fn add_hash(&mut self, h: &Hash) {
        for (field, _) in h.iter() {
            self.add_bytes(field.as_bytes());
        }
    }

    pub fn add_set(&mut self, s: &Set) {
        for member in s.members() {
            self.add_bytes(member.as_bytes());
        }
    }

    pub fn add_list(&mut self, l: &QuickList) {
        for item in l.iter() {
            match item {
                ListItem::Str(s) => self.add_bytes(s.as_bytes()),
                ListItem::Int(i) => self.add_bytes(i.to_string().as_bytes()),
            }
        }
    }

    pub fn add_zset(&mut self, z: &ZSet) {
        for (member, _) in z {
            self.add_bytes(member.as_bytes());
        }
    }

    /// `Add` (string_stats.cc:64): fold another counter's HLL into this one.
    /// `pfmerge` overwrites its target with the max of the *inputs*, so this
    /// counter's own registers must be an input too (the reference passes
    /// `{other.counter_, counter_}` to `pfmerge`). Inputs must be the stored
    /// form (exactly `HLL_DENSE_SIZE` bytes), hence the truncation.
    pub fn merge(&mut self, other: &UniqueStrings) {
        self.total_count += other.total_count;
        self.total_bytes += other.total_bytes;
        let mut merged = create_dense_hll();
        let inputs = [&other.hll[..HLL_DENSE_SIZE], &self.hll[..HLL_DENSE_SIZE]];
        pfmerge(&inputs, &mut merged);
        self.hll = merged;
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// `UniqueCount` (string_stats.h): the HLL cardinality estimate.
    fn unique_count(&mut self) -> u64 {
        pfcount_single(&mut self.hll).max(0) as u64
    }

    fn average_length(&self) -> f64 {
        if self.total_count == 0 {
            0.0
        } else {
            self.total_bytes as f64 / self.total_count as f64
        }
    }

    /// `ByteSavingsOnDedup` (string_stats.cc:138): duplicate entries times the
    /// average entry length.
    fn byte_savings_on_dedup(&mut self) -> u64 {
        let uniques = self.unique_count();
        let duplicates = self.total_count.saturating_sub(uniques);
        (duplicates as f64 * self.average_length()) as u64
    }

    /// `ToString` (string_stats.cc:97): the labeled report block, empty when
    /// nothing was counted.
    pub fn to_string(&mut self, label: &str) -> String {
        if self.total_count == 0 {
            return String::new();
        }
        let total_count = self.total_count;
        let unique_count = self.unique_count();
        let total_bytes = self.total_bytes;
        let average_length = g6_format(self.average_length());
        let savings = self.byte_savings_on_dedup();
        format!(
            "{label}:\n  total strings: {total_count}\n  unique strings: {unique_count}\n  total bytes: {total_bytes}\n  average length: {average_length}\n  estimated savings: {savings} bytes",
        )
    }
}

/// `PerShardStats` (debugcmd.cc:1831): the per-object-type unique-string
/// counters one shard produced for `DEBUG UNIQ-STRS`.
#[derive(Debug, Default, Clone)]
pub struct PerShardStats {
    pub list: Option<UniqueStrings>,
    pub set: Option<UniqueStrings>,
    pub zset: Option<UniqueStrings>,
    pub hash: Option<UniqueStrings>,
}

impl PerShardStats {
    /// Fold one type's counter into the matching slot (`Add`), creating it on
    /// first sight.
    fn merge_into(slot: &mut Option<UniqueStrings>, src: Option<&UniqueStrings>) {
        let Some(src) = src else { return };
        if let Some(dst) = slot {
            dst.merge(src);
        } else {
            *slot = Some(src.clone());
        }
    }

    /// Fold another shard's stats into this summary, in the reference's
    /// `summary[obj_type].Add(*shard_stat[obj_type])` loop (debugcmd.cc:1866).
    pub fn merge(&mut self, other: &PerShardStats) {
        Self::merge_into(&mut self.list, other.list.as_ref());
        Self::merge_into(&mut self.set, other.set.as_ref());
        Self::merge_into(&mut self.zset, other.zset.as_ref());
        Self::merge_into(&mut self.hash, other.hash.as_ref());
    }

    /// `CountUniqueStrings` (debugcmd.cc:1881): render the full report,
    /// visiting OBJECT:list, set, zset, hash in that order (OBJ_LIST..OBJ_HASH).
    pub fn render(&mut self) -> String {
        let mut out = String::from("___begin unique string stats___\n\n");
        let mut emit = |slot: &mut Option<UniqueStrings>, obj_name: &str| {
            let Some(stats) = slot else { return };
            if stats.is_empty() {
                return;
            }
            let _ = writeln!(out, "OBJECT:{obj_name}");
            out.push_str("________________________________________________________________\n");
            out.push_str(&stats.to_string("Strings"));
            out.push('\n');
        };
        emit(&mut self.list, "list");
        emit(&mut self.set, "set");
        emit(&mut self.zset, "zset");
        emit(&mut self.hash, "hash");
        out.push_str("___end unique string stats___\n");
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::compact::CompactString;
    use crate::core::quicklist::QuickList;

    fn count_list(items: &[&str]) -> UniqueStrings {
        let mut list = QuickList::default();
        for s in items {
            list.push_back(ListItem::Str(CompactString::from(*s)));
        }
        let mut stats = UniqueStrings::new();
        stats.add_list(&list);
        stats
    }

    #[test]
    fn counts_and_uniques() {
        let mut stats = UniqueStrings::new();
        for _ in 0..5 {
            let mut h = crate::core::hash::Hash::default();
            h.set("name".into(), "n".into());
            h.set("email".into(), "e".into());
            h.set("age".into(), "a".into());
            stats.add_hash(&h);
        }
        assert_eq!(stats.total_count, 15);
        assert_eq!(stats.total_bytes, 60);
        assert_eq!(stats.unique_count(), 3);
        assert_eq!(stats.byte_savings_on_dedup(), 48);
        assert_eq!(
            stats.to_string("Strings"),
            "Strings:\n  total strings: 15\n  unique strings: 3\n  total bytes: 60\n  average length: 4\n  estimated savings: 48 bytes"
        );
    }

    #[test]
    fn empty_reports_nothing() {
        let mut stats = UniqueStrings::new();
        assert!(stats.is_empty());
        assert_eq!(stats.to_string("Strings"), "");
        let mut per = PerShardStats::default();
        assert_eq!(
            per.render(),
            "___begin unique string stats___\n\n___end unique string stats___\n"
        );
    }

    #[test]
    fn per_shard_merge_folds_across_types() {
        let mut a = PerShardStats {
            set: Some(count_list(&["alpha", "beta"])),
            hash: Some(count_list(&["f1"])),
            ..Default::default()
        };
        let b = PerShardStats {
            set: Some(count_list(&["alpha", "beta"])),
            list: Some(count_list(&["x"])),
            ..Default::default()
        };

        a.merge(&b);
        let out = a.render();
        assert!(out.contains("OBJECT:set\n"));
        assert!(out.contains("OBJECT:hash\n"));
        assert!(out.contains("OBJECT:list\n"));
        assert!(!out.contains("OBJECT:zset\n"));
        assert!(out.contains("total strings: 4\n"), "{out}");
    }
}
