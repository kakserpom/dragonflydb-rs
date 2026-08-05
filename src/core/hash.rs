use hashbrown::HashMap;

use crate::core::compact::CompactString;

const HASH_MAX_SMALL: usize = 128;

/// Dragonfly's HASH type: small hashes stored compactly (listpack), large ones
/// as a hash table. Mirrored as Small(Vec) / Large(HashMap).
///
/// Fields can carry a per-field expiry (HSETEX/HEXPIRE): the optional `expiry`
/// map holds the absolute expiry (in ms) keyed by field. Expired fields are
/// removed lazily via [`Hash::prune_expired`] before an operation accesses them.
#[derive(Debug, Clone)]
pub struct Hash {
    repr: HashRepr,
    expiry: Option<HashMap<CompactString, u64>>,
}

#[derive(Debug, Clone)]
enum HashRepr {
    Small(Vec<(CompactString, CompactString)>),
    Large(HashMap<CompactString, CompactString>),
}

impl Default for Hash {
    fn default() -> Self {
        Self::new()
    }
}

impl Hash {
    #[must_use]
    pub fn new() -> Self {
        Hash {
            repr: HashRepr::Small(Vec::new()),
            expiry: None,
        }
    }

    /// Set a field, optionally attaching an absolute expiry `expire_ms`.
    ///
    /// When the field already exists: with `keepttl` its expiry is preserved
    /// (or the new one applied when it had none, mirroring `StringMap::ComputeTtl`),
    /// otherwise `expire_ms` replaces it (or clears it when `None`, mirroring
    /// plain HSET). Returns true if the field was newly added.
    pub fn add_expirable(
        &mut self,
        field: CompactString,
        value: CompactString,
        expire_ms: Option<u64>,
        keepttl: bool,
    ) -> bool {
        let present = self.contains(field.as_bytes());
        if present {
            match (keepttl, self.field_expire_ms(field.as_bytes()), expire_ms) {
                (false, _, Some(ms)) | (true, None, Some(ms)) => {
                    self.set_expiry(field.clone(), ms);
                }
                (false, _, None) => self.clear_field_expiry(field.as_bytes()),
                (true, _, _) => {}
            }
            self.set(field, value);
            return false;
        }
        if let Some(ms) = expire_ms {
            self.set_expiry(field.clone(), ms);
        }
        self.set(field, value);
        true
    }

    /// Set a field only if absent (HSETNX semantics): existing fields are left
    /// untouched, including their expiry.
    pub fn add_or_skip(
        &mut self,
        field: CompactString,
        value: CompactString,
        expire_ms: Option<u64>,
    ) -> bool {
        if self.contains(field.as_bytes()) {
            return false;
        }
        if let Some(ms) = expire_ms {
            self.set_expiry(field.clone(), ms);
        }
        self.set(field, value);
        true
    }

    fn set_expiry(&mut self, field: CompactString, expire_ms: u64) {
        self.expiry
            .get_or_insert_with(HashMap::new)
            .insert(field, expire_ms);
    }
    fn clear_field_expiry(&mut self, field: &[u8]) {
        if let Some(exp) = &mut self.expiry {
            exp.remove(field);
            if exp.is_empty() {
                self.expiry = None;
            }
        }
    }

    /// Lazily remove all fields expired before `now_ms`.
    pub fn prune_expired(&mut self, now_ms: u64) {
        let Some(exp) = &self.expiry else { return };
        let expired: Vec<CompactString> = exp
            .iter()
            .filter(|(_, at)| **at <= now_ms)
            .map(|(f, _)| f.clone())
            .collect();
        if expired.is_empty() {
            return;
        }
        for f in &expired {
            self.remove(f.as_bytes());
        }
    }

    /// Absolute expiry (in ms) of `field`, if it carries one.
    #[must_use]
    pub fn field_expire_ms(&self, field: &[u8]) -> Option<u64> {
        self.expiry.as_ref().and_then(|exp| exp.get(field).copied())
    }

    /// Whether any field carries an expiry.
    #[must_use]
    pub fn has_expiry(&self) -> bool {
        self.expiry.is_some()
    }

    /// Whether the hash is still in the compact (listpack) representation,
    /// mirroring Dragonfly's `kEncodingListPack` encoding. Used by DUMP to pick
    /// `RDB_TYPE_HASH_LISTPACK`.
    #[must_use]
    pub fn is_small(&self) -> bool {
        matches!(self.repr, HashRepr::Small(_))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match &self.repr {
            HashRepr::Small(v) => v.len(),
            HashRepr::Large(m) => m.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn get(&self, field: &[u8]) -> Option<&CompactString> {
        match &self.repr {
            HashRepr::Small(v) => v
                .iter()
                .find(|(f, _)| f.as_bytes() == field)
                .map(|(_, val)| val),
            HashRepr::Large(m) => m.get(field),
        }
    }

    /// Set a field. Returns the old value if the field existed and was updated,
    /// or None if newly inserted.
    pub fn set(&mut self, field: CompactString, value: CompactString) -> Option<CompactString> {
        match &mut self.repr {
            HashRepr::Small(v) => {
                if let Some(idx) = v.iter().position(|(f, _)| f == &field) {
                    let old = std::mem::replace(&mut v[idx].1, value);
                    Some(old)
                } else {
                    v.push((field, value));
                    if v.len() > HASH_MAX_SMALL {
                        self.promote();
                    }
                    None
                }
            }
            HashRepr::Large(m) => m.insert(field, value),
        }
    }

    pub fn remove(&mut self, field: &[u8]) -> Option<CompactString> {
        if let Some(exp) = &mut self.expiry {
            exp.remove(field);
            if exp.is_empty() {
                self.expiry = None;
            }
        }
        match &mut self.repr {
            HashRepr::Small(v) => {
                if let Some(idx) = v.iter().position(|(f, _)| f.as_bytes() == field) {
                    let (_, val) = v.swap_remove(idx);
                    Some(val)
                } else {
                    None
                }
            }
            HashRepr::Large(m) => m.remove(field),
        }
    }

    #[must_use]
    pub fn contains(&self, field: &[u8]) -> bool {
        match &self.repr {
            HashRepr::Small(v) => v.iter().any(|(f, _)| f.as_bytes() == field),
            HashRepr::Large(m) => m.contains_key(field),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&CompactString, &CompactString)> {
        match &self.repr {
            HashRepr::Small(v) => EitherIter::A(v.iter().map(|(f, val)| (f, val))),
            HashRepr::Large(m) => EitherIter::B(m.iter()),
        }
    }

    #[must_use]
    pub fn len_bytes(&self) -> usize {
        let mut total = 0;
        for (f, v) in self.iter() {
            total += f.len() + v.len();
        }
        total
    }

    fn sample_seed(&self) -> Vec<u8> {
        match &self.repr {
            HashRepr::Small(v) => v
                .first()
                .map(|(f, _)| f.as_bytes().to_vec())
                .unwrap_or_default(),
            HashRepr::Large(m) => m
                .iter()
                .next()
                .map(|(f, _)| f.as_bytes().to_vec())
                .unwrap_or_default(),
        }
    }

    fn pair_at(&self, idx: usize) -> Option<(&CompactString, &CompactString)> {
        match &self.repr {
            HashRepr::Small(v) => v.get(idx).map(|(f, val)| (f, val)),
            HashRepr::Large(m) => m.iter().nth(idx),
        }
    }

    fn rng(&self) -> SplitMix {
        SplitMix(crate::util::shard_hash(&self.sample_seed()))
    }

    /// Return a random (field, value) pair (for HRANDFIELD without COUNT).
    #[must_use]
    pub fn rand_pair(&self) -> Option<(&CompactString, &CompactString)> {
        let len = self.len();
        if len == 0 {
            return None;
        }
        let idx = (self.rng().next() as usize) % len;
        self.pair_at(idx)
    }

    /// Return `count` random (field, value) pairs, duplicates allowed
    /// (HRANDFIELD with a negative COUNT).
    #[must_use]
    pub fn rand_pairs(&self, count: usize) -> Vec<(&CompactString, &CompactString)> {
        let len = self.len();
        if len == 0 || count == 0 {
            return vec![];
        }
        let mut rng = self.rng();
        (0..count)
            .map(|_| {
                self.pair_at((rng.next() as usize) % len)
                    .expect("non-empty hash")
            })
            .collect()
    }

    /// Return `count` distinct random (field, value) pairs, at most the hash
    /// size (HRANDFIELD with a non-negative COUNT).
    #[must_use]
    pub fn rand_pairs_unique(&self, count: usize) -> Vec<(&CompactString, &CompactString)> {
        let len = self.len();
        if len == 0 || count == 0 {
            return vec![];
        }
        let count = count.min(len);
        let mut all: Vec<(&CompactString, &CompactString)> = self.iter().collect();
        let mut rng = self.rng();
        for i in 0..count {
            let j = i + (rng.next() as usize) % (len - i);
            all.swap(i, j);
        }
        all.truncate(count);
        all
    }

    fn promote(&mut self) {
        if let HashRepr::Small(v) = &self.repr {
            let mut m: HashMap<CompactString, CompactString> = HashMap::with_capacity(v.len());
            for (f, val) in v {
                m.insert(f.clone(), val.clone());
            }
            self.repr = HashRepr::Large(m);
        }
    }
}

enum EitherIter<A, B> {
    A(A),
    B(B),
}

impl<'a, T, U> Iterator for EitherIter<T, U>
where
    T: Iterator<Item = (&'a CompactString, &'a CompactString)>,
    U: Iterator<Item = (&'a CompactString, &'a CompactString)>,
{
    type Item = (&'a CompactString, &'a CompactString);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            EitherIter::A(it) => it.next(),
            EitherIter::B(it) => it.next(),
        }
    }
}

/// Deterministic splitmix64 PRNG, seeded from the hash contents so that
/// HRANDFIELD results are stable for a given hash.
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_ops() {
        let mut h = Hash::new();
        assert!(
            h.set(CompactString::from("f1"), CompactString::from("v1"))
                .is_none()
        );
        assert!(
            h.set(CompactString::from("f1"), CompactString::from("v2"))
                .is_some()
        );
        assert_eq!(
            h.get(b"f1")
                .map(super::super::compact::CompactString::as_bytes),
            Some(b"v2".as_slice())
        );
        assert_eq!(h.len(), 1);
        assert_eq!(h.remove(b"f1"), Some(CompactString::from("v2")));
        assert!(h.is_empty());
    }

    #[test]
    fn promotes_to_large() {
        let mut h = Hash::new();
        for i in 0..200 {
            h.set(
                CompactString::from_bytes(format!("f{i}").as_bytes()),
                CompactString::from("v"),
            );
        }
        assert_eq!(h.len(), 200);
        assert!(matches!(h.repr, HashRepr::Large(_)));
        assert_eq!(
            h.get(b"f150")
                .map(super::super::compact::CompactString::as_bytes),
            Some(b"v".as_slice())
        );
        let pairs: Vec<_> = h.iter().map(|(f, v)| (f.clone(), v.clone())).collect();
        assert_eq!(pairs.len(), 200);
    }

    #[test]
    fn field_expiry_prunes() {
        let mut h = Hash::new();
        assert!(h.add_expirable(
            CompactString::from("f1"),
            CompactString::from("v"),
            Some(1000),
            false
        ));
        assert!(!h.add_expirable(
            CompactString::from("f1"),
            CompactString::from("v2"),
            Some(2000),
            false
        ));
        assert_eq!(h.field_expire_ms(b"f1"), Some(2000));
        h.prune_expired(1500);
        assert!(h.contains(b"f1"));
        h.prune_expired(2001);
        assert!(!h.contains(b"f1"));
        assert_eq!(h.field_expire_ms(b"f1"), None);
        assert!(!h.has_expiry());
    }

    #[test]
    fn field_expiry_keepttl() {
        let mut h = Hash::new();
        h.add_expirable(
            CompactString::from("f1"),
            CompactString::from("v"),
            Some(1000),
            false,
        );
        assert!(!h.add_expirable(
            CompactString::from("f1"),
            CompactString::from("v2"),
            Some(2000),
            true
        ));
        assert_eq!(h.field_expire_ms(b"f1"), Some(1000));
        assert!(h.has_expiry());
    }

    #[test]
    fn keepttl_applies_when_no_existing_ttl() {
        let mut h = Hash::new();
        h.set(CompactString::from("f1"), CompactString::from("v"));
        assert!(h.field_expire_ms(b"f1").is_none());
        assert!(!h.add_expirable(
            CompactString::from("f1"),
            CompactString::from("v2"),
            Some(1000),
            true
        ));
        assert_eq!(h.field_expire_ms(b"f1"), Some(1000));
    }

    #[test]
    fn plain_set_clears_expiry() {
        let mut h = Hash::new();
        h.add_expirable(
            CompactString::from("f1"),
            CompactString::from("v"),
            Some(1000),
            false,
        );
        assert_eq!(h.field_expire_ms(b"f1"), Some(1000));
        assert!(!h.add_expirable(
            CompactString::from("f1"),
            CompactString::from("v2"),
            None,
            false
        ));
        assert_eq!(h.field_expire_ms(b"f1"), None);
        assert!(!h.has_expiry());
    }

    #[test]
    fn remove_clears_expiry_entry() {
        let mut h = Hash::new();
        h.add_expirable(
            CompactString::from("f1"),
            CompactString::from("v"),
            Some(1000),
            false,
        );
        h.add_expirable(
            CompactString::from("f2"),
            CompactString::from("v"),
            Some(2000),
            false,
        );
        assert_eq!(h.remove(b"f1"), Some(CompactString::from("v")));
        assert_eq!(h.field_expire_ms(b"f2"), Some(2000));
    }

    #[test]
    fn rand_pairs_are_valid() {
        let mut h = Hash::new();
        for i in 0..10 {
            h.set(
                CompactString::from_bytes(format!("f{i}").as_bytes()),
                CompactString::from("v"),
            );
        }
        let p = h.rand_pair().expect("pair");
        assert!(h.contains(p.0.as_bytes()));
        let pairs = h.rand_pairs(25);
        assert_eq!(pairs.len(), 25);
        assert!(pairs.iter().all(|(f, _)| h.contains(f.as_bytes())));
        let unique = h.rand_pairs_unique(7);
        assert_eq!(unique.len(), 7);
        let names: std::collections::HashSet<_> =
            unique.iter().map(|(f, _)| f.as_bytes()).collect();
        assert_eq!(names.len(), 7);
        assert_eq!(h.rand_pairs_unique(100).len(), 10);
    }
}
