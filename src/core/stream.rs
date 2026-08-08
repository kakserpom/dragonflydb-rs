use std::collections::BTreeMap;
use std::ops::Bound;

use hashbrown::HashMap;

use crate::core::compact::CompactString;

/// Sentinel for a consumer group whose read counter was never initialized
/// (`SCG_INVALID_ENTRIES_READ` in Dragonfly). Any non-negative value is valid.
pub const SCG_INVALID_ENTRIES_READ: i64 = -1;
/// Sentinel for a lag value that cannot be computed (`SCG_INVALID_LAG`).
pub const SCG_INVALID_LAG: i64 = -1;

/// A stream entry ID: `<ms>-<seq>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const MIN: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.ms == 0 && self.seq == 0
    }

    #[must_use]
    pub fn next(self) -> StreamId {
        StreamId {
            ms: self.ms,
            seq: self.seq + 1,
        }
    }

    #[must_use]
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
    /// `-1` if the consumer never read any entry.
    pub active_time: i64,
    pub pending: u64,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub last_delivered: StreamId,
    /// Number of entries read by the group, or `SCG_INVALID_ENTRIES_READ`.
    pub entries_read: i64,
    pub consumers: HashMap<CompactString, Consumer>,
    pub pel: BTreeMap<StreamId, PendingEntry>,
}

impl ConsumerGroup {
    fn new(last_delivered: StreamId) -> Self {
        ConsumerGroup {
            last_delivered,
            entries_read: SCG_INVALID_ENTRIES_READ,
            consumers: HashMap::new(),
            pel: BTreeMap::new(),
        }
    }

    /// Ensure the consumer exists and refresh its seen time (FindOrAddConsumer).
    /// New consumers start with `active_time == -1`.
    pub fn consumer_mut(&mut self, name: &CompactString, now_ms: u64) -> &mut Consumer {
        let c = self
            .consumers
            .entry(name.clone())
            .or_insert_with(|| Consumer {
                seen_time: now_ms,
                active_time: SCG_INVALID_ENTRIES_READ,
                pending: 0,
            });
        c.seen_time = now_ms;
        c
    }
}

/// Dragonfly's STREAM type. Dragonfly stores entries in a rax (radix tree); we
/// use a `BTreeMap` keyed by (ms, seq) which provides the same ordered semantics.
#[derive(Debug, Clone, Default)]
pub struct Stream {
    pub entries: BTreeMap<StreamId, StreamEntry>,
    pub length: u64,
    pub last_id: StreamId,
    pub max_deleted_id: StreamId,
    pub groups: HashMap<CompactString, ConsumerGroup>,
}

/// Port of `StreamRangeHasTombstones` (stream_family.cc:99-125): does the
/// half-open range [start, end] contain a deleted (tombstone) entry?
fn stream_range_has_tombstones(
    length: u64,
    max_deleted_id: StreamId,
    start: StreamId,
    end: StreamId,
) -> bool {
    if length == 0 || max_deleted_id.is_zero() {
        return false;
    }
    max_deleted_id >= start && max_deleted_id <= end
}

/// Port of `streamEstimateDistanceFromFirstEverEntry` (t_stream.c:394-436).
/// Returns the number of entries added up to and including `id`, or
/// `SCG_INVALID_ENTRIES_READ` when the counter cannot be computed.
fn stream_estimate_distance(
    entries: &BTreeMap<StreamId, StreamEntry>,
    length: u64,
    last_id: StreamId,
    max_deleted_id: StreamId,
    first_id: Option<StreamId>,
    id: StreamId,
) -> i64 {
    let entries_added = entries.len() as u64;
    if entries_added == 0 {
        return 0;
    }
    if length == 0 && id <= last_id {
        return entries_added as i64;
    }
    if !id.is_zero() && id < max_deleted_id {
        return SCG_INVALID_ENTRIES_READ;
    }
    if id == last_id {
        return entries_added as i64;
    }
    if id > last_id {
        return SCG_INVALID_ENTRIES_READ;
    }
    let Some(first) = first_id else {
        return SCG_INVALID_ENTRIES_READ;
    };
    if max_deleted_id.is_zero() || max_deleted_id < first {
        if id < first {
            return (entries_added - length) as i64;
        }
        if id == first {
            return (entries_added - length + 1) as i64;
        }
    }
    SCG_INVALID_ENTRIES_READ
}

impl Stream {
    #[must_use]
    pub fn new() -> Self {
        Stream {
            entries: BTreeMap::new(),
            length: 0,
            last_id: StreamId::MIN,
            max_deleted_id: StreamId::MIN,
            groups: HashMap::new(),
        }
    }

    #[must_use]
    pub fn len(&self) -> u64 {
        self.length
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// First non-deleted entry id.
    #[must_use]
    pub fn first_entry(&self) -> Option<&StreamId> {
        self.entries
            .iter()
            .find(|(_, e)| !e.deleted)
            .map(|(id, _)| id)
    }

    /// Last non-deleted entry id.
    #[must_use]
    pub fn last_entry(&self) -> Option<&StreamId> {
        self.entries
            .iter()
            .rev()
            .find(|(_, e)| !e.deleted)
            .map(|(id, _)| id)
    }

    /// Append an entry; caller guarantees id > `last_id`.
    pub fn append(&mut self, id: StreamId, fields: Vec<(CompactString, CompactString)>) {
        self.entries.insert(
            id,
            StreamEntry {
                fields,
                deleted: false,
            },
        );
        self.last_id = id;
        self.length += 1;
    }

    /// Mark an entry deleted. Returns true if it existed and was not deleted.
    pub fn delete(&mut self, id: StreamId) -> bool {
        if let Some(e) = self.entries.get_mut(&id)
            && !e.deleted
        {
            e.deleted = true;
            self.length -= 1;
            if id > self.max_deleted_id {
                self.max_deleted_id = id;
            }
            return true;
        }
        false
    }

    /// Like [`delete`] but never updates `max_deleted_id`, mirroring
    /// `StreamTrim` (stream_family.cc:355-473): trimming removes entries
    /// without recording tombstones, so the group lag machinery is unaffected.
    fn trim_delete(&mut self, id: StreamId) -> bool {
        if let Some(e) = self.entries.get_mut(&id)
            && !e.deleted
        {
            e.deleted = true;
            self.length -= 1;
            return true;
        }
        false
    }

    /// Total number of entries ever added (including tombstones).
    #[must_use]
    pub fn entries_added(&self) -> u64 {
        self.entries.len() as u64
    }

    /// Port of `StreamRangeHasTombstones`.
    #[must_use]
    pub fn range_has_tombstones(&self, start: StreamId, end: StreamId) -> bool {
        stream_range_has_tombstones(self.length, self.max_deleted_id, start, end)
    }

    /// Port of `streamEstimateDistanceFromFirstEverEntry`.
    #[must_use]
    pub fn estimate_distance_from_first_ever_entry(&self, id: StreamId) -> i64 {
        stream_estimate_distance(
            &self.entries,
            self.length,
            self.last_id,
            self.max_deleted_id,
            self.first_entry().copied(),
            id,
        )
    }

    /// Port of `StreamCGLag` (stream_family.cc:235-258). Returns
    /// `SCG_INVALID_LAG` when the lag cannot be computed.
    #[must_use]
    pub fn cg_lag(&self, group: &ConsumerGroup) -> i64 {
        let entries_added = self.entries_added();
        if entries_added == 0 {
            return 0;
        }
        if group.entries_read != SCG_INVALID_ENTRIES_READ
            && !self.range_has_tombstones(group.last_delivered, StreamId::MAX)
        {
            return entries_added as i64 - group.entries_read;
        }
        let entries_read = self.estimate_distance_from_first_ever_entry(group.last_delivered);
        if entries_read != SCG_INVALID_ENTRIES_READ {
            return entries_added as i64 - entries_read;
        }
        SCG_INVALID_LAG
    }

    /// Trim entries: with MAXLEN (approximate count) or MINID (keep ids >= min).
    /// Returns number of entries removed (logical deletions).
    ///
    /// Mirrors `StreamTrim` (stream_family.cc:355-473): entries are grouped
    /// into radix-tree nodes of `NODE_MAX_ENTRIES`, whole nodes are removed
    /// when possible, and approximate trimming never partially trims a node.
    pub fn trim(
        &mut self,
        maxlen: Option<u64>,
        minid: Option<StreamId>,
        approx: bool,
        limit: Option<u64>,
    ) -> u64 {
        let mut removed = 0u64;
        let nodes = self.node_chunks();
        for node in nodes {
            if maxlen.is_some_and(|m| self.length <= m) {
                break;
            }
            let entries_in_node = node.len() as u64;
            if let Some(l) = limit
                && removed + entries_in_node > l
            {
                break;
            }
            let last_id = *node.last().unwrap_or(&StreamId::MIN);
            let remove_node = match (maxlen, minid) {
                (Some(m), None) => self.length - entries_in_node >= m,
                (None, Some(min)) => last_id < min,
                _ => false,
            };
            if remove_node {
                for id in &node {
                    if self.trim_delete(*id) {
                        removed += 1;
                    }
                }
                continue;
            }
            if approx {
                break;
            }
            // Exact trimming: partially trim the first non-removable node.
            for id in &node {
                let over_len = maxlen.is_some_and(|m| self.length > m);
                let below_min = minid.is_some_and(|m| *id < m);
                if !over_len && !below_min {
                    break;
                }
                if self.trim_delete(*id) {
                    removed += 1;
                }
            }
            break;
        }
        removed
    }

    /// Chunk the live entry ids into groups of `NODE_MAX_ENTRIES` (the rax node
    /// size). Deleted tombstones are skipped: the reference's node `entries`
    /// field counts live entries only (`StreamTrim`, stream_family.cc:378).
    fn node_chunks(&self) -> Vec<Vec<StreamId>> {
        let mut chunks = Vec::new();
        let mut current = Vec::with_capacity(NODE_MAX_ENTRIES);
        for (id, e) in &self.entries {
            if e.deleted {
                continue;
            }
            current.push(*id);
            if current.len() == NODE_MAX_ENTRIES {
                chunks.push(std::mem::take(&mut current));
            }
        }
        if !current.is_empty() {
            chunks.push(current);
        }
        chunks
    }

    pub fn group_mut(&mut self, name: &CompactString) -> Option<&mut ConsumerGroup> {
        self.groups.get_mut(name)
    }

    #[must_use]
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
    /// Returns (entries, `ids_before_trim`).
    pub fn read_group(
        &mut self,
        group_name: &CompactString,
        consumer_name: &CompactString,
        id: StreamId,
        count: Option<usize>,
        noack: bool,
        now_ms: u64,
    ) -> Result<Vec<(StreamId, Vec<(CompactString, CompactString)>)>, GroupReadErr> {
        let Stream {
            entries,
            length,
            last_id,
            max_deleted_id,
            groups,
        } = self;

        let Some(group) = groups.get_mut(group_name) else {
            return Err(GroupReadErr::NoGroup);
        };
        let mut out = Vec::new();
        if id == (StreamId { ms: 0, seq: 1 }) {
            // Special ">" id: read new entries.
            let has_tombstones = |start: StreamId, end: StreamId| {
                stream_range_has_tombstones(*length, *max_deleted_id, start, end)
            };
            let first_id = entries.iter().find(|(_, e)| !e.deleted).map(|(id, _)| *id);
            group.consumer_mut(consumer_name, now_ms);
            let last = group.last_delivered;
            let iter = entries.range((Bound::Excluded(last), Bound::Unbounded));
            for (&eid, entry) in iter {
                if entry.deleted {
                    continue;
                }
                if let Some(c) = count
                    && out.len() >= c
                {
                    break;
                }
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
                    cons.active_time = now_ms as i64;
                }
                // Update the group read counter, mirroring stream_family.cc:1328-1341.
                if group.entries_read != SCG_INVALID_ENTRIES_READ
                    && first_id.is_some_and(|f| group.last_delivered >= f)
                    && !has_tombstones(group.last_delivered, StreamId::MAX)
                {
                    group.entries_read += 1;
                } else if !entries.is_empty() {
                    group.entries_read = stream_estimate_distance(
                        entries,
                        *length,
                        *last_id,
                        *max_deleted_id,
                        first_id,
                        eid,
                    );
                }
                group.last_delivered = eid;
                out.push((eid, fields));
            }
            Ok(out)
        } else {
            // Read from the consumer's own PEL starting at `id`.
            group.consumer_mut(consumer_name, now_ms);
            let mut pending: Vec<(StreamId, Vec<(CompactString, CompactString)>)> = Vec::new();
            let mut entries_out = Vec::new();
            for (eid, pe) in group.pel.range(id..) {
                if pe.consumer != *consumer_name {
                    continue;
                }
                if let Some(c) = count
                    && pending.len() >= c
                {
                    break;
                }
                let fields = match entries.get(eid) {
                    Some(e) if !e.deleted => e.fields.clone(),
                    _ => Vec::new(),
                };
                pending.push((*eid, fields));
                entries_out.push(*eid);
            }
            for eid in entries_out {
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

/// Number of entries stored per radix-tree node in Dragonfly
/// (`server.stream_node_max_entries`, default 100).
pub const NODE_MAX_ENTRIES: usize = 100;

pub enum GroupCreateErr {
    Empty,
    Exists,
}

#[derive(Debug)]
pub enum GroupReadErr {
    NoGroup,
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
        s.append(
            StreamId { ms: 1, seq: 0 },
            vec![(CompactString::from("a"), CompactString::from("b"))],
        );
        s.append(
            StreamId { ms: 2, seq: 0 },
            vec![(CompactString::from("c"), CompactString::from("d"))],
        );
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
            s.append(
                StreamId { ms: i, seq: 0 },
                vec![(
                    CompactString::from("k"),
                    CompactString::from_bytes(format!("v{i}").as_bytes()),
                )],
            );
        }
        let g = CompactString::from("g");
        let c = CompactString::from("c1");
        assert!(
            s.create_group(g.clone(), StreamId { ms: 0, seq: 0 }, false, false)
                .is_ok()
        );
        let ids = s
            .read_group(&g, &c, StreamId { ms: 0, seq: 1 }, Some(3), false, now())
            .unwrap();
        assert_eq!(ids.len(), 3);
        let ack_ids: Vec<StreamId> = ids.iter().map(|(id, _)| *id).collect();
        assert_eq!(s.ack(&g, &ack_ids), 3);
        let ids = s
            .read_group(&g, &c, StreamId { ms: 0, seq: 1 }, None, false, now())
            .unwrap();
        assert_eq!(ids.len(), 2);
    }
}
