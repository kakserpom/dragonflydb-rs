//! Master-side per-shard replication journal.
//!
//! Mirrors `dflyref`'s `journal_slice.cc` (the per-shard circular LSN buffer and
//! consumer registry) and `serializer.cc` (the `JournalWriter` wire format).
//! Records are serialized into plain bytes with `WritePackedUInt` lengths and
//! the `journal::Op` opcodes, so a replica parses them with the same decoder.

use std::collections::VecDeque;

use crate::core::rdb::write_len;

/// `journal::Op` opcodes (`server/journal/types.h`).
pub const OP_SELECT: u8 = 6;
pub const OP_EXPIRED: u8 = 9;
pub const OP_COMMAND: u8 = 10;
pub const OP_PING: u8 = 13;
pub const OP_LSN: u8 = 15;

/// `--shard_repl_backlog_len`: the per-shard circular log capacity. Kept at the
/// upstream default (8192 records); an entry of any size occupies one slot.
pub const RING_CAPACITY: usize = 8192;

/// One ring-buffered record: the shard-local LSN and the serialized wire bytes
/// (including any auto-emitted SELECT prefix).
#[derive(Debug, Clone)]
pub struct JournalItem {
    pub lsn: u64,
    pub data: Vec<u8>,
}

/// A consumer subscribed to every newly recorded journal entry. Returned ids are
/// used by `JournalSlice::unregister_consumer` (full-sync cancel path).
pub type Consumer = Box<dyn FnMut(&JournalItem) + Send>;

/// Serialize one journal record. A fresh writer is used per record (as the
/// reference's `AddLogRecord` does), so `cur_dbid` always starts unset and a
/// COMMAND/EXPIRED record always carries its own SELECT prefix.
///
/// `lsn` is only meaningful for `OP_LSN` entries; `args` is the reduced
/// per-shard argument list (command name included).
#[must_use]
pub fn serialize_record(
    txid: u64,
    op: u8,
    dbid: u64,
    lsn: u64,
    cmd: &[u8],
    args: &[Vec<u8>],
) -> Vec<u8> {
    let mut w = Writer::new();
    w.write_entry(txid, op, dbid, lsn, cmd, args);
    w.out
}

/// `JournalWriter` (`serializer.cc`): a record is `[opcode]` + per-op payload,
/// prefixed by an auto-emitted `SELECT dbid` entry when `dbid` differs from the
/// writer's current database.
struct Writer {
    out: Vec<u8>,
    cur_dbid: Option<u64>,
}

impl Writer {
    fn new() -> Self {
        Self {
            out: Vec::new(),
            cur_dbid: None,
        }
    }

    fn write_entry(&mut self, txid: u64, op: u8, dbid: u64, lsn: u64, cmd: &[u8], args: &[Vec<u8>]) {
        if op != OP_SELECT
            && op != OP_LSN
            && op != OP_PING
            && self.cur_dbid != Some(dbid)
        {
            self.out.push(OP_SELECT);
            self.write_packed(dbid);
            self.cur_dbid = Some(dbid);
        }
        // `Write(uint8_t(entry.opcode))`: opcodes < 64 encode as one byte.
        self.out.push(op);
        match op {
            OP_SELECT => self.write_packed(dbid),
            OP_LSN => self.write_packed(lsn),
            OP_PING => {}
            OP_COMMAND | OP_EXPIRED => {
                self.write_packed(txid);
                self.write_packed(1); // deprecated `payload` field
                self.write_payload(cmd, args);
            }
            _ => {}
        }
    }

    fn write_payload(&mut self, cmd: &[u8], args: &[Vec<u8>]) {
        let total: usize = cmd.len() + args.iter().map(Vec::len).sum::<usize>();
        self.write_packed(1 + args.len() as u64);
        self.write_packed(total as u64);
        self.write_bytes(cmd);
        for arg in args {
            self.write_bytes(arg);
        }
    }

    fn write_bytes(&mut self, s: &[u8]) {
        self.write_packed(s.len() as u64);
        self.out.extend_from_slice(s);
    }

    fn write_packed(&mut self, v: u64) {
        write_len(&mut self.out, v);
    }
}

/// A decoded journal record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedEntry {
    pub opcode: u8,
    pub dbid: u64,
    pub txid: u64,
    pub lsn: u64,
    /// Reduced command: `[name, key, args...]`.
    pub cmd: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalError {
    Corrupt,
    Truncated,
}

/// Decode a serialized record, transparently applying any SELECT prefix so the
/// returned `dbid` reflects the database the entry applies to.
pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
    dbid: u64,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            dbid: 0,
        }
    }

    pub fn read_entry(&mut self) -> Result<ParsedEntry, JournalError> {
        let op = self.read_byte()?;
        if op == OP_SELECT {
            self.dbid = self.read_packed()?;
            return self.read_entry();
        }
        let mut e = ParsedEntry {
            opcode: op,
            dbid: self.dbid,
            txid: 0,
            lsn: 0,
            cmd: Vec::new(),
        };
        match op {
            OP_PING => {}
            OP_LSN => e.lsn = self.read_packed()?,
            OP_COMMAND | OP_EXPIRED => {
                e.txid = self.read_packed()?;
                self.read_packed()?; // deprecated `payload` field
                let num = self.read_packed()? as usize;
                let mut total = self.read_packed()?;
                for _ in 0..num {
                    let size = self.read_packed()?;
                    if size > total {
                        return Err(JournalError::Corrupt);
                    }
                    let s = self.read_exact(size as usize)?;
                    e.cmd.push(s.to_vec());
                    total -= size;
                }
            }
            _ => return Err(JournalError::Corrupt),
        }
        Ok(e)
    }

    fn read_byte(&mut self) -> Result<u8, JournalError> {
        let b = *self.data.get(self.pos).ok_or(JournalError::Truncated)?;
        self.pos += 1;
        Ok(b)
    }

    fn read_packed(&mut self) -> Result<u64, JournalError> {
        let b = self.read_byte()?;
        match b >> 6 {
            0 => Ok(u64::from(b & 0x3f)),
            1 => {
                let lo = u64::from(self.read_byte()?);
                Ok((u64::from(b & 0x3f) << 8) | lo)
            }
            2 => match b {
                0x80 => {
                    let s: [u8; 4] = self
                        .read_exact(4)?
                        .try_into()
                        .map_err(|_| JournalError::Corrupt)?;
                    Ok(u64::from(u32::from_be_bytes(s)))
                }
                0x81 => {
                    let s: [u8; 8] = self
                        .read_exact(8)?
                        .try_into()
                        .map_err(|_| JournalError::Corrupt)?;
                    Ok(u64::from_be_bytes(s))
                }
                _ => Err(JournalError::Corrupt),
            },
            _ => Err(JournalError::Corrupt),
        }
    }

    fn read_exact(&mut self, n: usize) -> Result<&'a [u8], JournalError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(JournalError::Corrupt)?;
        let s = self
            .data
            .get(self.pos..end)
            .ok_or(JournalError::Truncated)?;
        self.pos = end;
        Ok(s)
    }
}

/// The per-shard journal (`JournalSlice`): a shard-local LSN counter, a
/// circular buffer of serialized records, and the registered consumers.
///
/// The reference uses a single writer fiber guarded by an atomic; the port's
/// shard thread already serializes every mutation, so no lock is needed.
pub struct JournalSlice {
    lsn: u64,
    ring: VecDeque<JournalItem>,
    capacity: usize,
    consumers: Vec<(usize, Consumer)>,
    next_consumer_id: usize,
}

impl JournalSlice {
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(RING_CAPACITY)
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            lsn: 1,
            ring: VecDeque::with_capacity(capacity),
            capacity,
            consumers: Vec::new(),
            next_consumer_id: 0,
        }
    }

    #[must_use]
    pub fn lsn(&self) -> u64 {
        self.lsn
    }

    /// `journal::IsLSNInBuffer`: whether `lsn` is still available for partial
    /// sync. Mirrors `JournalSlice::IsLSNInBuffer` including the single-element
    /// special case.
    #[must_use]
    pub fn is_lsn_in_buffer(&self, lsn: u64) -> bool {
        if self.ring.is_empty() {
            return false;
        }
        let front = self.ring.front().unwrap().lsn;
        let back = self.ring.back().unwrap().lsn;
        if self.ring.len() == 1 {
            return front == lsn;
        }
        front <= lsn && lsn <= back
    }

    /// `JournalSlice::GetEntry(lsn)`: the serialized record for `lsn`.
    #[must_use]
    pub fn get_entry(&self, lsn: u64) -> Option<&[u8]> {
        if !self.is_lsn_in_buffer(lsn) {
            return None;
        }
        let start = self.ring.front().unwrap().lsn;
        self.ring.get((lsn - start) as usize).map(|it| it.data.as_slice())
    }

    /// `JournalSlice::ClearBuffer`: drop the ring and advance the LSN past
    /// every previously issued record so stale replica LSNs are rejected.
    pub fn clear_buffer(&mut self) {
        self.ring.clear();
        self.lsn += 1;
    }

    /// `JournalSlice::SetStartingLSN` / `StartInThreadAtLsn`: reset the ring and
    /// seed the LSN counter.
    pub fn start_at_lsn(&mut self, lsn: u64) {
        self.ring.clear();
        self.lsn = lsn;
    }

    /// Record one entry: assign the next LSN, notify consumers (which forward
    /// to the stable-sync streamers), then push into the ring, evicting the
    /// oldest record at capacity. Returns the assigned LSN.
    pub fn record(&mut self, data: Vec<u8>) -> u64 {
        let lsn = self.lsn;
        self.lsn += 1;
        let item = JournalItem { lsn, data };
        for (_, cb) in &mut self.consumers {
            cb(&item);
        }
        if self.ring.len() == self.capacity {
            self.ring.pop_front();
        }
        self.ring.push_back(item);
        lsn
    }

    pub fn register_consumer(&mut self, cb: Consumer) -> usize {
        let id = self.next_consumer_id;
        self.next_consumer_id += 1;
        self.consumers.push((id, cb));
        id
    }

    pub fn unregister_consumer(&mut self, id: usize) {
        self.consumers.retain(|(i, _)| *i != id);
    }
}

impl Default for JournalSlice {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the reduced per-shard argument list for a write command, mirroring the
/// reference's `ShardArgs` slices: `args[0]` plus, for every key this shard
/// owns, that key and the `step - 1` trailing arguments (only MSET/MSETNX use
/// step 2). Adjacent owned keys merge into contiguous slices.
#[must_use]
pub fn shard_args(cmd: &crate::commands::Command, args: &[Vec<u8>], owned: &[usize]) -> Vec<Vec<u8>> {
    let step = if cmd.key_range.step >= 1 {
        cmd.key_range.step
    } else {
        1
    };
    let mut out = Vec::with_capacity(1 + owned.len() * step);
    out.push(args[0].clone());
    for &ki in owned {
        for arg in args.iter().skip(ki).take(step) {
            out.push(arg.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Command;

    fn cmd(name: &'static str) -> Command {
        Command {
            name,
            arity: -2,
            flags: 0,
            key_range: crate::commands::KeyRange::ONE,
            exec: |_| crate::error::CmdResult::Ok(crate::commands::ok()),
            merge: None,
        }
    }

    #[test]
    fn roundtrip_command_entry() {
        let data = serialize_record(
            7,
            OP_COMMAND,
            2,
            0,
            b"SET",
            &[b"k".to_vec(), b"v".to_vec()],
        );
        let mut r = Reader::new(&data);
        let e = r.read_entry().unwrap();
        assert_eq!(e.opcode, OP_COMMAND);
        assert_eq!(e.dbid, 2);
        assert_eq!(e.txid, 7);
        // The payload carries the command name exactly once, followed by the
        // per-shard args (the reference writes `payload.cmd` separately from
        // `payload.args`).
        assert_eq!(e.cmd, vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]);
    }

    #[test]
    fn select_prefix_emitted_only_on_dbid_change() {
        let data = serialize_record(
            0,
            OP_COMMAND,
            2,
            0,
            b"SET",
            &[b"k".to_vec(), b"v".to_vec()],
        );
        assert_eq!(data[0], OP_SELECT, "fresh writer must emit SELECT");
        let mut r = Reader::new(&data);
        let e = r.read_entry().unwrap();
        assert_eq!(e.dbid, 2);
    }

    #[test]
    fn lsn_and_ping_entries() {
        let d1 = serialize_record(0, OP_LSN, 0, 42, b"", &[]);
        let mut r = Reader::new(&d1);
        let e = r.read_entry().unwrap();
        assert_eq!(e.opcode, OP_LSN);
        assert_eq!(e.lsn, 42);
        assert!(e.cmd.is_empty());

        let d2 = serialize_record(0, OP_PING, 0, 0, b"", &[]);
        let mut r = Reader::new(&d2);
        let e = r.read_entry().unwrap();
        assert_eq!(e.opcode, OP_PING);
        assert!(e.cmd.is_empty());
    }

    #[test]
    fn nested_select_is_transparent() {
        // SELECT 5, SELECT 6, then a command: the reader lands on db 6.
        let mut w = Writer::new();
        w.out.push(OP_SELECT);
        w.write_packed(5);
        w.out.push(OP_SELECT);
        w.write_packed(6);
        w.out.push(OP_COMMAND);
        w.write_packed(0);
        w.write_packed(1);
        w.write_packed(1 + 2);
        w.write_packed((b"SET".len() + b"k".len() + b"v".len()) as u64);
        w.write_bytes(b"SET");
        w.write_bytes(b"k");
        w.write_bytes(b"v");
        let mut r = Reader::new(&w.out);
        let e = r.read_entry().unwrap();
        assert_eq!(e.dbid, 6);
        assert_eq!(e.opcode, OP_COMMAND);
    }

    #[test]
    fn truncated_and_corrupt_data_error() {
        let data = serialize_record(
            0,
            OP_COMMAND,
            1,
            0,
            b"SET",
            &[b"k".to_vec(), b"v".to_vec()],
        );
        let mut r = Reader::new(&data[..data.len() - 1]);
        assert_eq!(r.read_entry(), Err(JournalError::Truncated));

        let mut w = Writer::new();
        w.out.push(OP_COMMAND);
        w.write_packed(0);
        w.write_packed(1);
        w.write_packed(2); // claims 2 args
        w.write_packed(3);
        w.write_bytes(b"SET");
        w.write_bytes(b"k");
        w.write_bytes(b"v");
        // Second arg size (1) exceeds the remaining `total` (0), which is corrupt.
        let mut r = Reader::new(&w.out);
        assert_eq!(r.read_entry(), Err(JournalError::Corrupt));
    }

    #[test]
    fn slice_lsn_and_ring_eviction() {
        let mut j = JournalSlice::with_capacity(2);
        assert!(!j.is_lsn_in_buffer(1));
        let l1 = j.record(b"a".to_vec());
        assert_eq!(l1, 1);
        let l2 = j.record(b"b".to_vec());
        assert_eq!(l2, 2);
        assert!(j.is_lsn_in_buffer(1));
        assert!(j.is_lsn_in_buffer(2));
        assert!(!j.is_lsn_in_buffer(0));
        assert!(!j.is_lsn_in_buffer(3));
        assert_eq!(j.get_entry(1), Some(b"a".as_slice()));
        assert_eq!(j.get_entry(2), Some(b"b".as_slice()));

        let l3 = j.record(b"c".to_vec());
        assert_eq!(l3, 3);
        assert_eq!(j.lsn(), 4);
        assert!(!j.is_lsn_in_buffer(1), "oldest evicted");
        assert!(j.is_lsn_in_buffer(2));
        assert!(j.is_lsn_in_buffer(3));
        assert_eq!(j.get_entry(2), Some(b"b".as_slice()));
        assert_eq!(j.get_entry(3), Some(b"c".as_slice()));
    }

    #[test]
    fn single_element_buffer_special_case() {
        let mut j = JournalSlice::with_capacity(4);
        j.record(b"only".to_vec());
        assert!(j.is_lsn_in_buffer(1));
        assert!(!j.is_lsn_in_buffer(2), "single element: lsn must match exactly");
    }

    #[test]
    fn clear_buffer_advances_lsn() {
        let mut j = JournalSlice::with_capacity(4);
        j.record(b"a".to_vec());
        assert_eq!(j.lsn(), 2);
        j.clear_buffer();
        assert_eq!(j.lsn(), 3);
        assert!(!j.is_lsn_in_buffer(1));
        j.start_at_lsn(100);
        assert_eq!(j.lsn(), 100);
        assert_eq!(j.record(b"b".to_vec()), 100);
    }

    #[test]
    fn consumers_see_every_record_in_order() {
        let mut j = JournalSlice::with_capacity(4);
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let s = seen.clone();
        let id = j.register_consumer(Box::new(move |it| {
            s.lock().unwrap().push((it.lsn, it.data.clone()));
        }));
        j.record(b"x".to_vec());
        j.record(b"y".to_vec());
        j.unregister_consumer(id);
        j.record(b"z".to_vec());
        assert_eq!(seen.lock().unwrap().len(), 2);
        assert_eq!(seen.lock().unwrap()[0].0, 1);
        assert_eq!(seen.lock().unwrap()[1].0, 2);
    }

    #[test]
    fn shard_args_reduces_by_owned_keys() {
        // MSET-style: step 2, owns keys 1 and 3.
        let c = Command {
            key_range: crate::commands::KeyRange::PAIRS,
            ..cmd("MSET")
        };
        let args: Vec<Vec<u8>> = vec![
            b"MSET".to_vec(),
            b"k1".to_vec(),
            b"v1".to_vec(),
            b"k3".to_vec(),
            b"v3".to_vec(),
        ];
        assert_eq!(
            shard_args(&c, &args, &[1, 3]),
            vec![
                b"MSET".to_vec(),
                b"k1".to_vec(),
                b"v1".to_vec(),
                b"k3".to_vec(),
                b"v3".to_vec(),
            ]
        );
        assert_eq!(
            shard_args(&c, &args, &[3]),
            vec![b"MSET".to_vec(), b"k3".to_vec(), b"v3".to_vec()]
        );

        // Single-key command: only the owned key and no trailing args.
        let c = cmd("SET");
        let args: Vec<Vec<u8>> = vec![b"SET".to_vec(), b"k1".to_vec(), b"v1".to_vec()];
        assert_eq!(
            shard_args(&c, &args, &[1]),
            vec![b"SET".to_vec(), b"k1".to_vec()]
        );

        // Movable-key command with no key_range: owns the dest at [1] plus the
        // source keys; each owned key contributes just itself.
        let c = Command {
            key_range: crate::commands::KeyRange::NONE,
            ..cmd("ZUNIONSTORE")
        };
        let args: Vec<Vec<u8>> = vec![
            b"ZUNIONSTORE".to_vec(),
            b"dest".to_vec(),
            b"2".to_vec(),
            b"k1".to_vec(),
            b"k2".to_vec(),
        ];
        assert_eq!(
            shard_args(&c, &args, &[1, 3, 4]),
            vec![
                b"ZUNIONSTORE".to_vec(),
                b"dest".to_vec(),
                b"k1".to_vec(),
                b"k2".to_vec(),
            ]
        );
        assert_eq!(
            shard_args(&c, &args, &[3]),
            vec![b"ZUNIONSTORE".to_vec(), b"k1".to_vec()]
        );
    }

    #[test]
    fn parse_ring_then_reserialize_matches() {
        let mut j = JournalSlice::with_capacity(8);
        j.record(serialize_record(
            1,
            OP_COMMAND,
            0,
            0,
            b"SET",
            &[b"a".to_vec(), b"1".to_vec()],
        ));
        j.record(serialize_record(
            1,
            OP_COMMAND,
            1,
            0,
            b"SET",
            &[b"b".to_vec(), b"2".to_vec()],
        ));
        let data = j.get_entry(1).unwrap();
        let e = Reader::new(data).read_entry().unwrap();
        assert_eq!(e.cmd, vec![b"SET".to_vec(), b"a".to_vec(), b"1".to_vec()]);
        let data = j.get_entry(2).unwrap();
        let e = Reader::new(data).read_entry().unwrap();
        assert_eq!(e.dbid, 1);
        assert_eq!(e.cmd, vec![b"SET".to_vec(), b"b".to_vec(), b"2".to_vec()]);
    }
}
