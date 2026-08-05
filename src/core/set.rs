use hashbrown::{HashMap, HashSet};

use crate::core::compact::CompactString;

const SET_MAX_SMALL: usize = 128;

/// Dragonfly's SET type: small sets are stored compactly (listpack), large sets
/// as a hash table. Mirrored as Small(Vec) / Large(HashSet).
///
/// Members can carry a per-member expiry (SADDEX): the optional `expiry` map
/// holds the absolute expiry (in ms) keyed by member. Expired members are
/// removed lazily via [`Set::prune_expired`] before an operation accesses them.
#[derive(Debug, Clone)]
pub struct Set {
    repr: SetRepr,
    expiry: Option<HashMap<CompactString, u64>>,
}

#[derive(Debug, Clone)]
enum SetRepr {
    Small(Vec<CompactString>),
    Large(HashSet<CompactString>),
}

impl Default for Set {
    fn default() -> Self {
        Self::new()
    }
}

impl Set {
    #[must_use]
    pub fn new() -> Self {
        Set {
            repr: SetRepr::Small(Vec::new()),
            expiry: None,
        }
    }

    /// Add a member with an absolute expiry `expire_ms`. When the member is
    /// already present, `keepttl` keeps its existing expiry, otherwise it is
    /// refreshed. Returns true if the member was newly added.
    pub fn add_expirable(&mut self, member: CompactString, expire_ms: u64, keepttl: bool) -> bool {
        let present = self.contains(member.as_bytes());
        if present {
            if !keepttl {
                self.set_expiry(member, expire_ms);
            }
            return false;
        }
        self.add(member.clone());
        self.set_expiry(member, expire_ms);
        true
    }

    fn set_expiry(&mut self, member: CompactString, expire_ms: u64) {
        self.expiry
            .get_or_insert_with(HashMap::new)
            .insert(member, expire_ms);
    }

    /// Lazily remove all members expired before `now_ms`.
    pub fn prune_expired(&mut self, now_ms: u64) {
        let Some(exp) = &self.expiry else { return };
        let expired: Vec<CompactString> = exp
            .iter()
            .filter(|(_, at)| **at <= now_ms)
            .map(|(m, _)| m.clone())
            .collect();
        if expired.is_empty() {
            return;
        }
        for m in &expired {
            self.remove(m.as_bytes());
        }
        let exp = self.expiry.as_mut().unwrap();
        for m in &expired {
            exp.remove(m);
        }
        if exp.is_empty() {
            self.expiry = None;
        }
    }

    /// Remaining time-to-live in ms for `member`, or -1 when the member is
    /// not present or has no expiry.
    #[must_use]
    pub fn member_ttl_ms(&self, member: &[u8], now_ms: u64) -> i64 {
        match &self.expiry {
            Some(exp) => match exp.get(member) {
                Some(at) => (*at as i64).saturating_sub(now_ms as i64),
                None => -1,
            },
            None => -1,
        }
    }

    /// Absolute expiry (in ms) of `member`, if it carries one.
    #[must_use]
    pub fn member_expire_ms(&self, member: &[u8]) -> Option<u64> {
        self.expiry
            .as_ref()
            .and_then(|exp| exp.get(member).copied())
    }

    /// Whether any member carries an expiry (SADDEX sets are never compacted
    /// into the "intset" form in the reference; no impact here).
    #[must_use]
    pub fn has_expiry(&self) -> bool {
        self.expiry.is_some()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match &self.repr {
            SetRepr::Small(v) => v.len(),
            SetRepr::Large(s) => s.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn contains(&self, member: &[u8]) -> bool {
        match &self.repr {
            SetRepr::Small(v) => v.iter().any(|m| m.as_bytes() == member),
            SetRepr::Large(s) => s.contains(member),
        }
    }

    /// Add a member; returns true if newly added.
    pub fn add(&mut self, member: CompactString) -> bool {
        match &mut self.repr {
            SetRepr::Small(v) => {
                if v.iter().any(|m| m == &member) {
                    return false;
                }
                v.push(member);
                if v.len() > SET_MAX_SMALL {
                    self.promote();
                }
                true
            }
            SetRepr::Large(s) => s.insert(member),
        }
    }

    /// Remove a member; returns true if it existed.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        match &mut self.repr {
            SetRepr::Small(v) => {
                if let Some(pos) = v.iter().position(|m| m.as_bytes() == member) {
                    v.swap_remove(pos);
                    true
                } else {
                    false
                }
            }
            SetRepr::Large(s) => s.remove(member),
        }
    }

    #[must_use]
    pub fn members(&self) -> Vec<CompactString> {
        match &self.repr {
            SetRepr::Small(v) => v.clone(),
            SetRepr::Large(s) => s.iter().cloned().collect(),
        }
    }

    /// Return a random member (for SRANDMEMBER).
    #[must_use]
    pub fn rand_member(&self) -> Option<&CompactString> {
        let len = self.len();
        if len == 0 {
            return None;
        }
        let idx = (crate::util::shard_hash(&self.sample_seed()) as usize) % len;
        match &self.repr {
            SetRepr::Small(v) => v.get(idx),
            SetRepr::Large(s) => s.iter().nth(idx),
        }
    }

    fn sample_seed(&self) -> Vec<u8> {
        match &self.repr {
            SetRepr::Small(v) => v.first().map(|m| m.as_bytes().to_vec()).unwrap_or_default(),
            SetRepr::Large(s) => s
                .iter()
                .next()
                .map(|m| m.as_bytes().to_vec())
                .unwrap_or_default(),
        }
    }

    /// Pop a random member (for SPOP).
    pub fn pop_random(&mut self) -> Option<CompactString> {
        let len = self.len();
        if len == 0 {
            return None;
        }
        let seed = self.sample_seed();
        let idx = (crate::util::shard_hash(&seed) as usize) % len;
        match &mut self.repr {
            SetRepr::Small(v) => Some(v.swap_remove(idx)),
            SetRepr::Large(s) => {
                let member = s.iter().nth(idx).cloned()?;
                s.remove(&member);
                Some(member)
            }
        }
    }

    pub fn clear(&mut self) {
        self.repr = SetRepr::Small(Vec::new());
        self.expiry = None;
    }

    fn promote(&mut self) {
        if let SetRepr::Small(v) = &self.repr {
            let set: HashSet<CompactString> = v.iter().cloned().collect();
            self.repr = SetRepr::Large(set);
        }
    }

    /// Add all members from another set's iterator (used by SDIFF/SUNION).
    pub fn extend(&mut self, members: impl Iterator<Item = CompactString>) {
        for m in members {
            self.add(m);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_set_ops() {
        let mut s = Set::new();
        assert!(s.add(CompactString::from("a")));
        assert!(!s.add(CompactString::from("a")));
        assert!(s.add(CompactString::from("b")));
        assert!(s.contains(b"a"));
        assert!(!s.contains(b"c"));
        assert_eq!(s.len(), 2);
        assert!(s.remove(b"a"));
        assert!(!s.remove(b"a"));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn promotes_to_large() {
        let mut s = Set::new();
        for i in 0..200 {
            s.add(CompactString::from_bytes(format!("m{i}").as_bytes()));
        }
        assert_eq!(s.len(), 200);
        assert!(matches!(s.repr, SetRepr::Large(_)));
        assert!(s.contains(b"m150"));
        assert!(s.remove(b"m150"));
        assert!(!s.contains(b"m150"));
    }

    #[test]
    fn member_expiry_prunes() {
        let mut s = Set::new();
        assert!(s.add_expirable(CompactString::from("a"), 1000, false));
        assert!(!s.add_expirable(CompactString::from("a"), 2000, false));
        assert_eq!(s.member_ttl_ms(b"a", 0), 2000);
        s.prune_expired(1500);
        assert!(s.contains(b"a"));
        s.prune_expired(2001);
        assert!(!s.contains(b"a"));
        assert_eq!(s.member_ttl_ms(b"a", 0), -1);
        assert!(!s.has_expiry());
    }

    #[test]
    fn member_expiry_keepttl() {
        let mut s = Set::new();
        s.add_expirable(CompactString::from("a"), 1000, false);
        assert!(!s.add_expirable(CompactString::from("a"), 2000, true));
        assert_eq!(s.member_ttl_ms(b"a", 0), 1000);
        assert!(s.has_expiry());
    }
}
