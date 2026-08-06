use std::collections::VecDeque;

use crate::core::compact::CompactString;
use crate::core::intset;
use crate::util::itoa;

/// A single element stored in a list chunk. Mirrors Dragonfly's listpack entries
/// which can be integers or strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListItem {
    Int(i64),
    Str(CompactString),
}

impl ListItem {
    #[must_use]
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            ListItem::Int(i) => itoa(*i),
            ListItem::Str(s) => s.as_bytes().to_vec(),
        }
    }

    #[must_use]
    pub fn from_bytes(b: &[u8]) -> Self {
        // Mirror Dragonfly's listpack int detection (`lpStringToInt64`): only
        // canonical integers ("0", "-0" and leading zeros are not) are stored
        // as ints, everything else keeps its exact bytes as a string.
        if let Some(i) = intset::string2ll(b)
            && itoa(i) == b
        {
            ListItem::Int(i)
        } else {
            ListItem::Str(CompactString::from_bytes(b))
        }
    }

    #[must_use]
    pub fn byte_len(&self) -> usize {
        match self {
            ListItem::Int(_) => 8,
            ListItem::Str(s) => s.len(),
        }
    }
}

/// A compact chunk of list items, the analogue of Dragonfly's listpack node.
#[derive(Debug, Clone)]
pub struct Chunk {
    items: VecDeque<ListItem>,
    bytes: usize,
}

const CHUNK_MAX_ITEMS: usize = 128;
const CHUNK_MAX_BYTES: usize = 8192;

impl Chunk {
    fn new() -> Self {
        Chunk {
            items: VecDeque::new(),
            bytes: 0,
        }
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    fn push_back(&mut self, item: ListItem) {
        self.bytes += item.byte_len();
        self.items.push_back(item);
    }

    fn push_front(&mut self, item: ListItem) {
        self.bytes += item.byte_len();
        self.items.push_front(item);
    }

    fn pop_back(&mut self) -> Option<ListItem> {
        if let Some(it) = self.items.pop_back() {
            self.bytes -= it.byte_len();
            Some(it)
        } else {
            None
        }
    }

    fn pop_front(&mut self) -> Option<ListItem> {
        if let Some(it) = self.items.pop_front() {
            self.bytes -= it.byte_len();
            Some(it)
        } else {
            None
        }
    }

    /// True if the item would make this chunk exceed size limits.
    fn would_overflow(&self, item: &ListItem) -> bool {
        self.items.len() + 1 > CHUNK_MAX_ITEMS || self.bytes + item.byte_len() > CHUNK_MAX_BYTES
    }
}

/// Dragonfly's list type: a doubly-linked list of compact chunks ("quicklist").
#[derive(Debug, Clone)]
pub struct QuickList {
    chunks: VecDeque<Chunk>,
    count: usize,
}

impl Default for QuickList {
    fn default() -> Self {
        Self::new()
    }
}

impl QuickList {
    #[must_use]
    pub fn new() -> Self {
        QuickList {
            chunks: VecDeque::new(),
            count: 0,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.count
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn tail_mut(&mut self) -> Option<&mut Chunk> {
        self.chunks.back_mut()
    }

    fn head_mut(&mut self) -> Option<&mut Chunk> {
        self.chunks.front_mut()
    }

    pub fn push_back(&mut self, item: ListItem) {
        let overflow = match self.tail_mut() {
            Some(c) => c.would_overflow(&item),
            None => true,
        };
        if overflow {
            self.chunks.push_back(Chunk::new());
        }
        self.tail_mut().unwrap().push_back(item);
        self.count += 1;
    }

    pub fn push_front(&mut self, item: ListItem) {
        let overflow = match self.head_mut() {
            Some(c) => c.would_overflow(&item),
            None => true,
        };
        if overflow {
            self.chunks.push_front(Chunk::new());
        }
        self.head_mut().unwrap().push_front(item);
        self.count += 1;
    }

    pub fn pop_back(&mut self) -> Option<ListItem> {
        let c = self.chunks.back_mut()?;
        let item = c.pop_back()?;
        if c.len() == 0 {
            self.chunks.pop_back();
        }
        self.count -= 1;
        Some(item)
    }

    pub fn pop_front(&mut self) -> Option<ListItem> {
        let c = self.chunks.front_mut()?;
        let item = c.pop_front()?;
        if c.len() == 0 {
            self.chunks.pop_front();
        }
        self.count -= 1;
        Some(item)
    }

    #[must_use]
    pub fn front(&self) -> Option<&ListItem> {
        self.chunks.front()?.items.front()
    }

    #[must_use]
    pub fn back(&self) -> Option<&ListItem> {
        self.chunks.back()?.items.back()
    }

    /// Get an item at a logical index (negative indices wrap from the end).
    #[must_use]
    pub fn get(&self, index: i64) -> Option<&ListItem> {
        let len = self.count as i64;
        let idx = if index < 0 { len + index } else { index };
        if idx < 0 || idx >= len {
            return None;
        }
        let mut rem = idx;
        for c in &self.chunks {
            let n = c.len() as i64;
            if rem < n {
                return c.items.get(rem as usize);
            }
            rem -= n;
        }
        None
    }

    pub fn set(&mut self, index: i64, item: ListItem) -> Option<ListItem> {
        let len = self.count as i64;
        let idx = if index < 0 { len + index } else { index };
        if idx < 0 || idx >= len {
            return None;
        }
        let mut rem = idx;
        for c in &mut self.chunks {
            let n = c.len() as i64;
            if rem < n {
                let new_bytes = item.byte_len();
                let old = std::mem::replace(c.items.get_mut(rem as usize)?, item);
                c.bytes = c.bytes - old.byte_len() + new_bytes;
                return Some(old);
            }
            rem -= n;
        }
        None
    }

    pub fn iter(&self) -> impl Iterator<Item = &ListItem> {
        self.chunks.iter().flat_map(|c| c.items.iter())
    }

    /// Number of chunks (listpack nodes).
    #[must_use]
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Iterate over the chunks, one listpack node each.
    pub fn chunks(&self) -> impl Iterator<Item = &VecDeque<ListItem>> {
        self.chunks.iter().map(|c| &c.items)
    }

    /// Return an iterator over elements [start, stop] inclusive using Redis index
    /// semantics; start/stop may be negative. Returns None if range is empty.
    #[must_use]
    pub fn range(&self, start: i64, stop: i64) -> Option<impl Iterator<Item = &ListItem>> {
        let len = self.count as i64;
        let (s, count) = crate::util::redis_range(start, stop, len)?;
        let rem = s;
        let mut it = self.iter();
        for _ in 0..rem {
            it.next();
        }
        Some(it.take(count as usize))
    }

    /// Remove a range of elements [start, stop] inclusive with Redis semantics.
    /// Returns number of removed elements.
    pub fn remove_range(&mut self, start: i64, stop: i64) -> usize {
        let len = self.count as i64;
        let Some((s, count)) = crate::util::redis_range(start, stop, len) else {
            return 0;
        };
        self.remove_count(s as usize, count as usize)
    }

    /// Remove `count` elements starting at logical index `start` (0-based).
    fn remove_count(&mut self, start: usize, count: usize) -> usize {
        let mut all: Vec<ListItem> = Vec::with_capacity(self.count);
        for c in &self.chunks {
            all.extend(c.items.iter().cloned());
        }
        let end = (start + count).min(all.len());
        all.drain(start..end);
        let removed = end - start;
        *self = Self::from_items(all);
        removed
    }

    /// Rebuild a `QuickList` from a plain vec of items.
    #[must_use]
    pub fn from_items(items: Vec<ListItem>) -> Self {
        let mut ql = QuickList::new();
        for it in items {
            ql.push_back(it);
        }
        ql
    }

    /// Remove up to `count` occurrences of `value` (count == 0 means all).
    /// Returns number removed.
    pub fn remove_value(&mut self, value: &[u8], count: i64) -> usize {
        if count == 0 {
            let before = self.count;
            let keep: Vec<ListItem> = self
                .iter()
                .filter(|it| it.as_bytes() != value)
                .cloned()
                .collect();
            let removed = before - keep.len();
            *self = Self::from_items(keep);
            return removed;
        }
        let mut removed = 0usize;
        let keep: Vec<ListItem> = self
            .iter()
            .filter(|&it| {
                if it.as_bytes() == value && removed < count.unsigned_abs() as usize {
                    removed += 1;
                    return false;
                }
                true
            })
            .cloned()
            .collect();
        *self = Self::from_items(keep);
        removed
    }

    /// Insert `elem` before (or after) the first element equal to `pivot`.
    /// Returns false when no matching pivot was found.
    pub fn insert_relative(&mut self, pivot: &[u8], elem: ListItem, after: bool) -> bool {
        let items: Vec<ListItem> = self.iter().cloned().collect();
        let Some(pos) = items.iter().position(|it| it.as_bytes() == pivot) else {
            return false;
        };
        let mut items = items;
        items.insert(if after { pos + 1 } else { pos }, elem);
        *self = Self::from_items(items);
        true
    }

    /// Insert at logical position `index` (0-based, may be == len for append).
    pub fn insert(&mut self, index: i64, item: ListItem) {
        let len = self.count as i64;
        let idx = if index < 0 { len + index + 1 } else { index };
        if idx <= 0 {
            self.push_front(item);
        } else if idx >= len {
            self.push_back(item);
        } else {
            // find the chunk containing position idx and insert into its middle
            let mut rem = idx;
            let it = self.chunks.iter_mut().enumerate();
            for (ci, c) in it {
                let n = c.len() as i64;
                if rem < n {
                    c.items.insert(rem as usize, item);
                    c.bytes += c.items[rem as usize].byte_len();
                    if c.len() > CHUNK_MAX_ITEMS {
                        // split the chunk to respect the size invariant
                        let half = c.len() / 2;
                        let right: VecDeque<ListItem> = c.items.split_off(half);
                        let right_bytes = right.iter().map(ListItem::byte_len).sum();
                        self.chunks.insert(
                            ci + 1,
                            Chunk {
                                items: right,
                                bytes: right_bytes,
                            },
                        );
                    }
                    self.count += 1;
                    return;
                }
                rem -= n;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn it(i: i64) -> ListItem {
        ListItem::Int(i)
    }

    #[test]
    fn push_pop() {
        let mut q = QuickList::new();
        q.push_back(it(1));
        q.push_back(it(2));
        q.push_front(it(0));
        assert_eq!(q.len(), 3);
        assert_eq!(q.pop_front(), Some(it(0)));
        assert_eq!(q.pop_back(), Some(it(2)));
        assert_eq!(q.pop_back(), Some(it(1)));
        assert_eq!(q.pop_back(), None);
    }

    #[test]
    fn index_and_set() {
        let mut q = QuickList::new();
        for i in 0..1000 {
            q.push_back(it(i));
        }
        assert_eq!(q.len(), 1000);
        assert_eq!(q.get(0), Some(&it(0)));
        assert_eq!(q.get(-1), Some(&it(999)));
        assert_eq!(q.get(500), Some(&it(500)));
        assert_eq!(q.set(0, it(42)), Some(it(0)));
        assert_eq!(q.get(0), Some(&it(42)));
        assert_eq!(q.set(-1, it(43)), Some(it(999)));
        assert_eq!(q.get(999), Some(&it(43)));
    }

    #[test]
    fn range_works() {
        let mut q = QuickList::new();
        for i in 0..300 {
            q.push_back(it(i));
        }
        let r: Vec<&ListItem> = q.range(0, -1).unwrap().collect();
        assert_eq!(r.len(), 300);
        let r: Vec<&ListItem> = q.range(-2, -1).unwrap().collect();
        assert_eq!(r, vec![&it(298), &it(299)]);
        assert!(q.range(5, 3).is_none());
    }

    #[test]
    fn remove_window() {
        let mut q = QuickList::new();
        for i in 0..50 {
            q.push_back(it(i));
        }
        assert_eq!(q.remove_range(10, 19), 10);
        assert_eq!(q.len(), 40);
        assert_eq!(q.get(9), Some(&it(9)));
        assert_eq!(q.get(10), Some(&it(20)));
        assert_eq!(q.remove_range(0, -1), 40);
        assert!(q.is_empty());
    }

    #[test]
    fn remove_value_works() {
        let mut q = QuickList::new();
        for v in [1, 2, 3, 2, 4, 2, 5] {
            q.push_back(it(v));
        }
        assert_eq!(q.remove_value(b"2", 0), 3);
        assert_eq!(q.len(), 4);
    }

    #[test]
    fn insert_middle() {
        let mut q = QuickList::new();
        q.push_back(it(1));
        q.push_back(it(3));
        q.insert(1, it(2));
        assert_eq!(q.len(), 3);
        assert_eq!(q.get(1), Some(&it(2)));
        q.insert(0, it(0));
        q.insert(-1, it(99));
        assert_eq!(q.get(0), Some(&it(0)));
        assert_eq!(q.get(4), Some(&it(99)));
    }
}
