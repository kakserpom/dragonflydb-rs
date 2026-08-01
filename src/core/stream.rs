use std::collections::BTreeMap;

use hashbrown::HashMap;

use crate::core::compact::CompactString;

/// A stream entry ID: `<ms>-<seq>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const MIN: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId { ms: u64::MAX, seq: u64::MAX };

    pub fn is_zero(&self) -> bool {
        self.ms == 0 && self.seq == 0
    }

    pub fn next(self) -> StreamId {
        StreamId { ms: self.ms, seq: self.seq + 1 }
    }

    pub fn render(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }
}

#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub fields: Vec<(CompactString, CompactString)>,
    pub deleted: bool,
}

#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub consumer: CompactString,
    pub delivery_time: u64,
    pub delivery_count: u64,
}

#[derive(Debug, Clone)]
pub struct Consumer {
    pub seen_time: u64,
    pub active_time: u64,
    pub pending: u64,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub last_delivered: StreamId,
    pub entries_read: u64,
    pub consumers: HashMap<CompactString, Consumer>,
    pub pel: BTreeMap<StreamId, PendingEntry>,
}

impl ConsumerGroup {
    fn new(last_delivered: StreamId) -> Self {
        ConsumerGroup {
            last_delivered,
            entries_read: 0,
            consumers: HashMap::new(),
            pel: BTreeMap::new(),
        }
    }

    pub fn consumer_mut(&mut self, name: &CompactString, now_ms: u64) -> &mut Consumer {
        let c = self
            .consumers
            .entry(name.clone())
            .or_insert_with(|| Consumer { seen_time: now_ms, active_time: now_ms, pending: 0 });
        c.active_time = now_ms;
        c
    }
}

/// Dragonfly's STREAM type. Dragonfly stores entries in a rax (radix tree); we
/// use a BTreeMap keyed by (ms, seq) which provides the same ordered semantics.
#[derive(Debug, Clone, Default)]
pub struct Stream {
    pub entries: BTreeMap<StreamId, StreamEntry>,
    pub length: u64,
    pub last_id: StreamId,
    pub max_deleted_id: StreamId,
    pub groups: HashMap<CompactString, ConsumerGroup>,
}

impl Stream {
    pub fn new() -> Self {
        Stream { entries: BTreeMap::new(), length: 0, last_id: StreamId::MIN, max_deleted_id: StreamId::MIN, groups: HashMap::new() }
    }

    pub fn len(&self) -> u64 {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// First non-deleted entry id.
    pub fn first_entry(&self) -> Option<&StreamId> {
        self.entries.iter().find(|(_, e)| !e.deleted).map(|(id, _)| id)
    }

    /// Last non-deleted entry id.
    pub fn last_entry(&self) -> Option<&StreamId> {
        self.entries.iter().rev().find(|(_, e)| !e.deleted).map(|(id, _)| id)
    }

    /// Append an entry; caller guarantees id > last_id.
    pub fn append(&mut self, id: StreamId, fields: Vec<(CompactString, CompactString)>) {
        self.entries.insert(id, StreamEntry { fields, deleted: false });
        self.last_id = id;
        self.length += 1;
    }

    /// Mark an entry deleted. Returns true if it existed and was not deleted.
    pub fn delete(&mut self, id: StreamId) -> bool {
        if let Some(e) = self.entries.get_mut(&id) {
            if !e.deleted {
                e.deleted = true;
                self.length -= 1;
                if id > self.max_deleted_id {
                    self.max_deleted_id = id;
                }
                return true;
            }
        }
        false
    }

    /// Trim entries: with MAXLEN (approximate count) or MINID (keep ids >= min).
    /// Returns number of entries removed (logical deletions).
    pub fn trim(&mut self, maxlen: Option<u64>, minid: Option<StreamId>) -> u64 {
        let mut removed = 0u64;
        // Use a threshold: remove the oldest entries while they exceed limits.
        loop {
            let Some(id) = self.first_entry().copied() else { break };
            let over_len = maxlen.map(|m| self.length > m).unwrap_or(false);
            let below_min = minid.map(|m| id < m).unwrap_or(false);
            if !over_len && !below_min {
                break;
            }
            if self.delete(id) {
                removed += 1;
            }
            // a hard stop to avoid pathological cases
            if removed > self.length.saturating_add(1_000_000) {
                break;
            }
        }
        removed
    }

    pub fn group_mut(&mut self, name: &CompactString) -> Option<&mut ConsumerGroup> {
        self.groups.get_mut(name)
    }

    pub fn group(&self, name: &CompactString) -> Option<&ConsumerGroup> {
        self.groups.get(name)
    }

    pub fn create_group(
        &mut self,
        name: CompactString,
        id: StreamId,
        mkstream: bool,
        id_is_dollar: bool,
    ) -> Result<(), GroupCreateErr> {
        if self.is_empty() && id_is_dollar && !mkstream {
            return Err(GroupCreateErr::Empty);
        }
        if self.groups.contains_key(&name) {
            return Err(GroupCreateErr::Exists);
        }
        if mkstream && self.is_empty() && id_is_dollar {
            self.last_id = id;
        }
        self.groups.insert(name, ConsumerGroup::new(id));
        Ok(())
    }

    pub fn destroy_group(&mut self, name: &CompactString) -> bool {
        self.groups.remove(name).is_some()
    }

    /// XREADGROUP: deliver entries after `group.last_delivered` to the consumer.
    /// Returns (entries, ids_before_trim).
    pub fn read_group(
        &mut self,
        group_name: &CompactString,
        consumer_name: &CompactString,
        id: StreamId,
        count: Option<usize>,
        noack: bool,
        now_ms: u64,
    ) -> Result<Vec<(StreamId, Vec<(CompactString, CompactString)>)>, String> {
        let Some(group) = self.groups.get_mut(group_name) else {
            return Err(format!("NOGROUP No such key or consumer group '{}'", String::from_utf8_lossy(group_name.as_bytes())));
        };
        let mut out = Vec::new();
        if id == (StreamId { ms: 0, seq: 1 }) {
            // Special ">" id: read new entries.
            let last = group.last_delivered;
            let mut iter = self.entries.range((std::ops::Bound::Excluded(last), std::ops::Bound::Unbounded));
            while let Some((eid, entry)) = iter.next() {
                if entry.deleted {
                    continue;
                }
                if let Some(c) = count {
                    if out.len() >= c {
                        break;
                    }
                }
                let eid = *eid;
                let fields = entry.fields.clone();
                if !noack {
                    group.pel.insert(
                        eid,
                        PendingEntry {
                            consumer: consumer_name.clone(),
                            delivery_time: now_ms,
                            delivery_count: 1,
                        },
                    );
                    let cons = group.consumer_mut(consumer_name, now_ms);
                    cons.pending += 1;
                }
                group.last_delivered = eid;
                group.entries_read += 1;
                out.push((eid, fields));
            }
            Ok(out)
        } else {
            // Read from the consumer's own PEL starting at `id`.
            let mut pending: Vec<(StreamId, Vec<(CompactString, CompactString)>)> = Vec::new();
            let mut entries = Vec::new();
            for (eid, pe) in group.pel.range(id..) {
                if pe.consumer != *consumer_name {
                    continue;
                }
                if let Some(c) = count {
                    if pending.len() >= c {
                        break;
                    }
                }
                let fields = match self.entries.get(&eid) {
                    Some(e) if !e.deleted => e.fields.clone(),
                    _ => Vec::new(),
                };
                pending.push((*eid, fields));
                entries.push(*eid);
            }
            for eid in entries {
                if let Some(pe) = group.pel.get_mut(&eid) {
                    pe.delivery_count += 1;
                    pe.delivery_time = now_ms;
                }
            }
            out.extend(pending);
            Ok(out)
        }
    }

    /// XACK: remove entries from the PEL. Returns number removed.
    pub fn ack(&mut self, group_name: &CompactString, ids: &[StreamId]) -> u64 {
        let Some(group) = self.groups.get_mut(group_name) else {
            return 0;
        };
        let mut removed = 0u64;
        for id in ids {
            if let Some(pe) = group.pel.remove(id) {
                if let Some(cons) = group.consumers.get_mut(&pe.consumer) {
                    cons.pending = cons.pending.saturating_sub(1);
                }
                removed += 1;
            }
        }
        removed
    }
}

pub enum GroupCreateErr {
    Empty,
    Exists,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> u64 {
        1_000_000
    }

    #[test]
    fn append_and_delete() {
        let mut s = Stream::new();
        s.append(StreamId { ms: 1, seq: 0 }, vec![(CompactString::from("a"), CompactString::from("b"))]);
        s.append(StreamId { ms: 2, seq: 0 }, vec![(CompactString::from("c"), CompactString::from("d"))]);
        assert_eq!(s.len(), 2);
        assert_eq!(s.first_entry(), Some(&StreamId { ms: 1, seq: 0 }));
        assert!(s.delete(StreamId { ms: 1, seq: 0 }));
        assert_eq!(s.len(), 1);
        assert_eq!(s.first_entry(), Some(&StreamId { ms: 2, seq: 0 }));
        assert!(!s.delete(StreamId { ms: 1, seq: 0 }));
    }

    #[test]
    fn group_read_ack() {
        let mut s = Stream::new();
        for i in 1..=5u64 {
            s.append(StreamId { ms: i, seq: 0 }, vec![(CompactString::from("k"), CompactString::from_bytes(format!("v{}", i).as_bytes()))]);
        }
        let g = CompactString::from("g");
        let c = CompactString::from("c1");
        assert!(s.create_group(g.clone(), StreamId { ms: 0, seq: 0 }, false, false).is_ok());
        let ids = s
            .read_group(&g, &c, StreamId { ms: 0, seq: 1 }, Some(3), false, now())
            .unwrap();
        assert_eq!(ids.len(), 3);
        let ack_ids: Vec<StreamId> = ids.iter().map(|(id, _)| *id).collect();
        assert_eq!(s.ack(&g, &ack_ids), 3);
        let ids = s.read_group(&g, &c, StreamId { ms: 0, seq: 1 }, None, false, now()).unwrap();
        assert_eq!(ids.len(), 2);
    }
}
