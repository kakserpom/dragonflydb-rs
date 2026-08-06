use std::cmp::Ordering;

use hashbrown::HashMap;

use crate::core::compact::CompactString;

const ZSKIPLIST_MAXLEVEL: usize = 32;
const ZSKIPLIST_P: u32 = 4; // 1/4 probability

fn cmp_score(a: f64, b: f64) -> std::cmp::Ordering {
    a.total_cmp(&b)
}

#[derive(Clone)]
struct ZNode {
    member: CompactString,
    score: f64,
    next: Vec<Option<usize>>,
    backward: Option<usize>,
}

/// Dragonfly's ZSET: a skiplist keyed by (score, member) plus a hash index from
/// member -> score for O(1) score lookups.
pub struct ZSet {
    nodes: Vec<ZNode>,
    header: usize,
    level: usize,
    len: usize,
    index: HashMap<CompactString, f64>,
    rng_state: u64,
}

impl Default for ZSet {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ZSet {
    fn clone(&self) -> Self {
        ZSet {
            nodes: self.nodes.clone(),
            header: self.header,
            level: self.level,
            len: self.len,
            index: self.index.clone(),
            rng_state: self.rng_state,
        }
    }
}

impl std::fmt::Debug for ZSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ZSet({{")?;
        for (i, (m, s)) in self.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}:{}", String::from_utf8_lossy(m.as_bytes()), s)?;
        }
        write!(f, "}})")
    }
}

impl ZSet {
    #[must_use]
    pub fn new() -> Self {
        let header = ZNode {
            member: CompactString::new(),
            score: 0.0,
            next: vec![None; ZSKIPLIST_MAXLEVEL],
            backward: None,
        };
        let mut s = ZSet {
            nodes: vec![header],
            header: 0,
            level: 1,
            len: 0,
            index: HashMap::new(),
            rng_state: 0x9E37_79B9_7F4A_7C15,
        };
        s.level = 1;
        s
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn next_random_level(&mut self) -> usize {
        self.rng_state ^= self.rng_state << 13;
        self.rng_state ^= self.rng_state >> 7;
        self.rng_state ^= self.rng_state << 17;
        let mut level = 1;
        let mut r = self.rng_state;
        while r.trailing_zeros() >= 2 && level < ZSKIPLIST_MAXLEVEL {
            level += 1;
            r >>= 2;
        }
        let _ = ZSKIPLIST_P;
        level
    }

    /// Insert (member, score). Returns the old score if the member existed.
    pub fn insert(&mut self, member: CompactString, score: f64) -> Option<f64> {
        if let Some(&old) = self.index.get(&member) {
            if old == score {
                return Some(old);
            }
            self.delete(&member);
        }
        let new_level = self.next_random_level();
        if new_level > self.level {
            for _ in self.level..new_level {
                self.nodes[self.header].next.push(None);
            }
            self.level = new_level;
        }
        let mut update = vec![self.header; self.level];
        let mut x = self.header;
        for i in (0..self.level).rev() {
            while let Some(n) = self.nodes[x].next[i] {
                let node = &self.nodes[n];
                let c = cmp_score(score, node.score);
                let advance = if c == std::cmp::Ordering::Greater {
                    true
                } else if c == std::cmp::Ordering::Equal {
                    node.member.as_bytes() < member.as_bytes()
                } else {
                    false
                };
                if !advance {
                    break;
                }
                x = n;
            }
            update[i] = x;
        }
        let node_idx = self.nodes.len();
        let node = ZNode {
            member: member.clone(),
            score,
            next: vec![None; new_level],
            // The first node has no backward node; the header must never appear
            // as a member (ZRevIter walks the backward chain).
            backward: if update[0] == self.header {
                None
            } else {
                Some(update[0])
            },
        };
        self.nodes.push(node);
        for (i, &prev) in update[..new_level].iter().enumerate() {
            let next_idx = self.nodes[prev].next[i];
            self.nodes[prev].next[i] = Some(node_idx);
            self.nodes[node_idx].next[i] = next_idx;
        }
        if let Some(next_idx) = self.nodes[node_idx].next[0] {
            self.nodes[next_idx].backward = Some(node_idx);
        }
        self.len += 1;
        self.index.insert(member, score);
        None
    }

    /// Delete a member. Returns true if it existed.
    pub fn delete(&mut self, member: &[u8]) -> bool {
        let Some(&score) = self.index.get(member) else {
            return false;
        };
        let mut update = vec![self.header; self.level];
        let mut x = self.header;
        for i in (0..self.level).rev() {
            while let Some(n) = self.nodes[x].next[i] {
                let node = &self.nodes[n];
                let c = cmp_score(score, node.score);
                let advance = if c == std::cmp::Ordering::Greater {
                    true
                } else if c == std::cmp::Ordering::Equal {
                    node.member.as_bytes() < member
                } else {
                    false
                };
                if !advance {
                    break;
                }
                x = n;
            }
            update[i] = x;
        }
        let Some(target) = self.nodes[update[0]].next[0] else {
            return false;
        };
        if self.nodes[target].member.as_bytes() != member {
            return false;
        }
        for (i, &prev) in update[..self.level].iter().enumerate() {
            if self.nodes[prev].next[i] == Some(target) {
                self.nodes[prev].next[i] = self.nodes[target].next[i];
            }
        }
        if let Some(next_idx) = self.nodes[target].next[0] {
            self.nodes[next_idx].backward = self.nodes[target].backward;
        }
        self.index.remove(member);
        self.len -= 1;
        while self.level > 1 && self.nodes[self.header].next[self.level - 1].is_none() {
            self.nodes[self.header].next.pop();
            self.level -= 1;
        }
        true
    }

    #[must_use]
    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.index.get(member).copied()
    }

    #[must_use]
    pub fn contains(&self, member: &[u8]) -> bool {
        self.index.contains_key(member)
    }

    /// Rank of member in ascending order (0-based), None if absent.
    #[must_use]
    pub fn rank(&self, member: &[u8]) -> Option<i64> {
        if !self.index.contains_key(member) {
            return None;
        }
        let mut x = self.header;
        let mut rank: i64 = 0;
        let mut i = self.level as i64 - 1;
        loop {
            let level = i as usize;
            while let Some(n) = self.nodes[x].next[level] {
                let node = &self.nodes[n];
                let advance = if node.score < self.index[member] {
                    true
                } else if node.score == self.index[member] {
                    node.member.as_bytes() < member
                } else {
                    false
                };
                if !advance {
                    break;
                }
                // Advance `span` levels worth of nodes; we don't store spans so
                // use a simple walk over the level-0 chain for correctness.
                x = n;
            }
            if i == 0 {
                break;
            }
            i -= 1;
        }
        let mut cur = self.nodes[self.header].next[0];
        while let Some(n) = cur {
            if self.nodes[n].member.as_bytes() == member {
                return Some(rank);
            }
            rank += 1;
            cur = self.nodes[n].next[0];
        }
        None
    }

    /// Fetch (member, score) at a 0-based rank in ascending order.
    #[must_use]
    pub fn by_rank(&self, rank: usize) -> Option<(CompactString, f64)> {
        if rank >= self.len {
            return None;
        }
        let mut cur = self.nodes[self.header].next[0];
        let mut i = 0usize;
        while let Some(n) = cur {
            if i == rank {
                let node = &self.nodes[n];
                return Some((node.member.clone(), node.score));
            }
            i += 1;
            cur = self.nodes[n].next[0];
        }
        None
    }

    /// Iterate members in ascending (score, member) order.
    #[must_use]
    pub fn iter(&self) -> ZIter<'_> {
        ZIter {
            zset: self,
            cur: self.nodes[self.header].next[0],
        }
    }

    /// Iterate members in descending (score, member) order.
    #[must_use]
    pub fn rev_iter(&self) -> ZRevIter<'_> {
        let mut cur = None;
        if self.len > 0 {
            let mut x = self.header;
            for i in (0..self.level).rev() {
                while let Some(n) = self.nodes[x].next[i] {
                    x = n;
                }
            }
            cur = Some(x);
        }
        ZRevIter { zset: self, cur }
    }

    /// Collect an ascending range by Redis semantics: `start`/`stop` may be
    /// negative (from the end). Returns (`start_idx`, count) normalized.
    fn normalized_range(&self, start: i64, stop: i64) -> Option<(usize, usize)> {
        let len = self.len as i64;
        crate::util::redis_range(start, stop, len).map(|(s, c)| (s as usize, c as usize))
    }

    #[must_use]
    pub fn range(&self, start: i64, stop: i64, with_scores: bool) -> Vec<(CompactString, f64)> {
        let Some((s, c)) = self.normalized_range(start, stop) else {
            return Vec::new();
        };
        let mut out = Vec::with_capacity(c);
        let mut it = self.iter();
        for _ in 0..s {
            it.next();
        }
        for (m, sc) in it.take(c) {
            let _ = with_scores;
            out.push((m, sc));
        }
        out
    }

    #[must_use]
    pub fn rev_range(&self, start: i64, stop: i64) -> Vec<(CompactString, f64)> {
        let Some((s, c)) = self.normalized_range(start, stop) else {
            return Vec::new();
        };
        let mut out = Vec::with_capacity(c);
        let mut it = self.rev_iter();
        for _ in 0..s {
            it.next();
        }
        for (m, sc) in it.take(c) {
            out.push((m, sc));
        }
        out
    }

    /// Range by score, inclusive of both bounds. `rev` reverses the output.
    #[must_use]
    pub fn range_by_score(
        &self,
        min: f64,
        max: f64,
        rev: bool,
        limit: Option<(usize, usize)>,
    ) -> Vec<(CompactString, f64)> {
        self.range_by_score_filtered(|s| s >= min && s <= max, rev, limit)
    }

    /// Iterate (score, member) filtered by a score predicate, optionally reversed,
    /// with an optional LIMIT (offset, count).
    pub fn range_by_score_filtered(
        &self,
        pred: impl Fn(f64) -> bool,
        rev: bool,
        limit: Option<(usize, usize)>,
    ) -> Vec<(CompactString, f64)> {
        let it: Box<dyn Iterator<Item = (CompactString, f64)>> = if rev {
            Box::new(self.rev_iter())
        } else {
            Box::new(self.iter())
        };
        let mut out = Vec::new();
        let mut skipped = 0usize;
        for item in it {
            if !pred(item.1) {
                continue;
            }
            if let Some((off, cnt)) = limit {
                if skipped < off {
                    skipped += 1;
                    continue;
                }
                if out.len() >= cnt {
                    break;
                }
            }
            out.push(item);
        }
        out
    }

    /// Iterate (member, score) filtered by a member predicate (lexicographic),
    /// optionally reversed, with an optional LIMIT.
    pub fn range_by_member_filtered(
        &self,
        pred: impl Fn(&CompactString) -> bool,
        rev: bool,
        limit: Option<(usize, usize)>,
    ) -> Vec<(CompactString, f64)> {
        let it: Box<dyn Iterator<Item = (CompactString, f64)>> = if rev {
            Box::new(self.rev_iter())
        } else {
            Box::new(self.iter())
        };
        let mut out = Vec::new();
        let mut skipped = 0usize;
        for item in it {
            if !pred(&item.0) {
                continue;
            }
            if let Some((off, cnt)) = limit {
                if skipped < off {
                    skipped += 1;
                    continue;
                }
                if out.len() >= cnt {
                    break;
                }
            }
            out.push(item);
        }
        out
    }

    /// Lexicographic range mirroring the C++ `SortedMap::GetLexRange` (Redis
    /// `zslFirstInLexRange`): the underlying (score, member) order is scanned,
    /// member bounds are compared member-only, and iteration *breaks* at the far
    /// bound — so a mixed-score set stops early even though later members would
    /// satisfy the predicate. `min`/`max` are `(member, inclusive)`; `None` is
    /// an unbounded low/high bound (`-`/`+`). Equally-scored sets behave like a
    /// plain member-filtered range.
    #[must_use]
    pub fn lex_range(
        &self,
        min: Option<(&[u8], bool)>,
        max: Option<(&[u8], bool)>,
        rev: bool,
        offset: usize,
        limit: usize,
    ) -> Vec<(CompactString, f64)> {
        if limit == 0 || self.len <= offset {
            return Vec::new();
        }
        let near: Option<(&[u8], bool)> = if rev { max } else { min };
        let far: Option<(&[u8], bool)> = if rev { min } else { max };
        let mut it: Box<dyn Iterator<Item = (CompactString, f64)> + '_> = if rev {
            Box::new(self.rev_iter())
        } else {
            Box::new(self.iter())
        };
        // Locate the first element that passes the near bound (member-only
        // compare), then apply the exclusive adjustment and the offset exactly
        // like the C++ rank path: rank = found + (exclusive?1:0) + offset.
        let positioned: Box<dyn Iterator<Item = (CompactString, f64)> + '_> = match near {
            None => Box::new(it.skip(offset)),
            Some((b, incl)) => {
                let mut skip = offset;
                let mut found = None;
                for item in it.by_ref() {
                    let beyond = item.0.as_bytes().cmp(b)
                        == if rev {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        };
                    if beyond {
                        continue;
                    }
                    if !incl && item.0.as_bytes() == b {
                        skip += 1;
                    }
                    found = Some(item);
                    break;
                }
                let Some(f) = found else {
                    return Vec::new();
                };
                Box::new(std::iter::once(f).chain(it).skip(skip))
            }
        };
        // Emit with *break* semantics at the far bound.
        let mut out = Vec::new();
        for (m, s) in positioned {
            if let Some((b, incl)) = far {
                let ord = m.as_bytes().cmp(b);
                let beyond = if rev {
                    ord == Ordering::Less || (ord == Ordering::Equal && !incl)
                } else {
                    ord == Ordering::Greater || (ord == Ordering::Equal && !incl)
                };
                if beyond {
                    break;
                }
            }
            out.push((m, s));
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    /// Count elements with score in [min, max].
    #[must_use]
    pub fn count(&self, min: f64, max: f64) -> usize {
        self.iter().filter(|(_, s)| *s >= min && *s <= max).count()
    }

    pub fn pop_min(&mut self) -> Option<(CompactString, f64)> {
        if self.len == 0 {
            return None;
        }
        let (m, s) = self.by_rank(0)?;
        self.delete(&m);
        Some((m, s))
    }

    pub fn pop_max(&mut self) -> Option<(CompactString, f64)> {
        if self.len == 0 {
            return None;
        }
        let (m, s) = self.by_rank(self.len - 1)?;
        self.delete(&m);
        Some((m, s))
    }
}

pub struct ZIter<'a> {
    zset: &'a ZSet,
    cur: Option<usize>,
}

impl Iterator for ZIter<'_> {
    type Item = (CompactString, f64);
    fn next(&mut self) -> Option<Self::Item> {
        let n = self.cur?;
        let node = &self.zset.nodes[n];
        let item = (node.member.clone(), node.score);
        self.cur = node.next[0];
        Some(item)
    }
}

impl<'a> IntoIterator for &'a ZSet {
    type Item = (CompactString, f64);
    type IntoIter = ZIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct ZRevIter<'a> {
    zset: &'a ZSet,
    cur: Option<usize>,
}

impl Iterator for ZRevIter<'_> {
    type Item = (CompactString, f64);
    fn next(&mut self) -> Option<Self::Item> {
        let n = self.cur?;
        let node = &self.zset.nodes[n];
        let item = (node.member.clone(), node.score);
        self.cur = node.backward;
        Some(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_rank() {
        let mut z = ZSet::new();
        z.insert(CompactString::from("a"), 1.0);
        z.insert(CompactString::from("b"), 3.0);
        z.insert(CompactString::from("c"), 2.0);
        assert_eq!(z.len(), 3);
        assert_eq!(z.score(b"b"), Some(3.0));
        assert_eq!(
            z.by_rank(0).map(|(m, _)| m.as_bytes().to_vec()),
            Some(b"a".to_vec())
        );
        assert_eq!(
            z.by_rank(2).map(|(m, _)| m.as_bytes().to_vec()),
            Some(b"b".to_vec())
        );
        assert!(z.delete(&CompactString::from("c")));
        assert_eq!(z.len(), 2);
    }

    #[test]
    fn update_member() {
        let mut z = ZSet::new();
        z.insert(CompactString::from("a"), 1.0);
        z.insert(CompactString::from("a"), 5.0);
        assert_eq!(z.len(), 1);
        assert_eq!(z.score(b"a"), Some(5.0));
    }

    #[test]
    fn range_by_score() {
        let mut z = ZSet::new();
        for (m, s) in [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)] {
            z.insert(CompactString::from(m), s);
        }
        let r = z.range_by_score(2.0, 3.0, false, None);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].0.as_bytes(), b"b");
        assert_eq!(r[1].0.as_bytes(), b"c");
        let r = z.range_by_score(2.0, 3.0, true, None);
        assert_eq!(r[0].0.as_bytes(), b"c");
        assert_eq!(z.count(2.0, 3.0), 2);
    }

    #[test]
    fn equal_scores_lexicographic() {
        let mut z = ZSet::new();
        z.insert(CompactString::from("banana"), 1.0);
        z.insert(CompactString::from("apple"), 1.0);
        z.insert(CompactString::from("cherry"), 1.0);
        let order: Vec<Vec<u8>> = z.iter().map(|(m, _)| m.as_bytes().to_vec()).collect();
        assert_eq!(
            order,
            vec![b"apple".to_vec(), b"banana".to_vec(), b"cherry".to_vec()]
        );
    }
}
