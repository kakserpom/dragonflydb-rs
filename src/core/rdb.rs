//! RDB DUMP encoder, byte-for-byte compatible with Dragonfly's
//! `RdbSerializer::DumpValue` (`dragonfly/src/server/rdb_save.cc`).
//!
//! A DUMP payload is:
//! ```text
//! +---------+-----------+---------------+-----------------+
//! | rdb type| value     | RDB version   | CRC64           |
//! | (u8)    | (per type)| (u16 LE = 9)  | (u64 LE)        |
//! +---------+-----------+---------------+-----------------+
//! ```
//! The CRC64 covers every byte before the 8 CRC bytes, including the version.
//!
//! Only the object types the Rust port can store are supported (string, list,
//! set, hash, zset, stream, sbf). Module objects (JSON, ...) are absent here.

use std::collections::BTreeMap;

use hashbrown::HashMap;

use crate::core::bloom::SBF;
use crate::core::cms::Cms;
use crate::core::cuckoo::CuckooFilter;
use crate::core::json::Json;
use crate::core::topk::Topk;
use crate::core::compact::CompactString;
use crate::core::crc64;
use crate::core::hash::Hash;
use crate::core::intset;
use crate::core::listpack;
use crate::core::lzf;
use crate::core::quicklist::{ListItem, QuickList};
use crate::core::set::Set;
use crate::core::stream::{Consumer, ConsumerGroup, PendingEntry, Stream, StreamEntry, StreamId};
use crate::core::value::PrimeValue;
use crate::core::zset::ZSet;

/// RDB version accepted by RESTORE (`RDB_VERSION` in `rdb.h`).
pub const RDB_VERSION: u64 = 12;
/// Serialization version written by DUMP (`RDB_SER_VERSION` in `rdb.h`).
pub const RDB_SER_VERSION: u16 = 9;

const RDB_14BITLEN: u8 = 1;
const RDB_32BITLEN: u8 = 0x80;
const RDB_64BITLEN: u8 = 0x81;

const RDB_ENCVAL: u8 = 3;
const RDB_ENC_INT8: u8 = 0;
const RDB_ENC_INT16: u8 = 1;
const RDB_ENC_INT32: u8 = 2;
const RDB_ENC_LZF: u8 = 3;

pub const RDB_TYPE_STRING: u8 = 0;
pub const RDB_TYPE_LIST: u8 = 1;
pub const RDB_TYPE_SET: u8 = 2;
pub const RDB_TYPE_HASH: u8 = 4;
pub const RDB_TYPE_ZSET_2: u8 = 5;
pub const RDB_TYPE_SET_INTSET: u8 = 11;
pub const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
pub const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;
pub const RDB_TYPE_HASH_LISTPACK: u8 = 16;
pub const RDB_TYPE_ZSET_LISTPACK: u8 = 17;
pub const RDB_TYPE_LIST_QUICKLIST_2: u8 = 18;
pub const RDB_TYPE_STREAM_LISTPACKS_2: u8 = 19;
pub const RDB_TYPE_SET_LISTPACK: u8 = 20;
pub const RDB_TYPE_STREAM_LISTPACKS_3: u8 = 21;

/// Dragonfly extension types (`rdb_extensions.h`).
pub const RDB_TYPE_HASH_WITH_EXPIRY: u8 = 31;
pub const RDB_TYPE_SET_WITH_EXPIRY: u8 = 32;

/// Port-local RDB type for the scalable bloom filter. The payload is the SBF
/// SCANDUMP blob (`SBF::serialize`); not part of the reference RDB type table
/// (the reference does not persist SBF values in RDB).
pub const RDB_TYPE_SBF: u8 = 40;

/// Port-local RDB type for the count-min sketch. The payload is the CMS blob
/// (`Cms::serialize`); not part of the reference RDB type table (the reference
/// does not persist CMS values in RDB).
pub const RDB_TYPE_CMS: u8 = 41;

/// Port-local RDB type for the cuckoo filter. The payload is the CF blob
/// (`CuckooFilter::serialize`); not part of the reference RDB type table (the
/// reference serializes cuckoo filters through the module RDB interface).
pub const RDB_TYPE_CUCKOO: u8 = 42;

/// Port-local RDB type for the top-K sketch. The payload is the TOPK blob
/// (`Topk::serialize`); not part of the reference RDB type table.
pub const RDB_TYPE_TOPK: u8 = 43;

/// Port-local RDB type for a JSON document. The payload is the compact JSON
/// dump (`Json::dump`); not part of the reference RDB type table (the reference
/// serializes JSON through the module RDB interface).
pub const RDB_TYPE_JSON: u8 = 44;

const QUICKLIST_NODE_CONTAINER_PACKED: usize = 2;

const STREAM_ITEM_FLAG_DELETED: i64 = 1;
const STREAM_ITEM_FLAG_SAMEFIELDS: i64 = 2;

const K_MAX_INTSET_ENTRIES: usize = 256;
const K_MAX_LISTPACK_MAP_BYTES: usize = 1024;
const K_STREAM_NODE_MAX_BYTES: usize = 4096;
const K_STREAM_NODE_MAX_ENTRIES: i64 = 100;

const K_MEMBER_EXPIRY_BASE: u64 = 1_675_209_600;

/// Encode a length with `WritePackedUInt` semantics (6/14/32/64-bit).
fn write_len(out: &mut Vec<u8>, len: u64) {
    if len < 1 << 6 {
        out.push(len as u8);
    } else if len < 1 << 14 {
        out.push(((len >> 8) as u8 & 0x3f) | (RDB_14BITLEN << 6));
        out.push(len as u8);
    } else if len <= u32::MAX as u64 {
        out.push(RDB_32BITLEN);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        out.push(RDB_64BITLEN);
        out.extend_from_slice(&len.to_be_bytes());
    }
}

/// `EncodeInteger`: encode a value as a `RDB_ENCVAL` integer string
/// (`0xc0`/`0xc1`/`0xc2`). Returns `None` when it does not fit 32 bits.
fn encode_integer(value: i64) -> Option<Vec<u8>> {
    if (-(1 << 7)..=(1 << 7) - 1).contains(&value) {
        return Some(vec![(RDB_ENCVAL << 6) | RDB_ENC_INT8, value as u8]);
    }
    if (-(1 << 15)..=(1 << 15) - 1).contains(&value) {
        let mut v = vec![(RDB_ENCVAL << 6) | RDB_ENC_INT16];
        v.extend_from_slice(&(value as i16).to_le_bytes());
        return Some(v);
    }
    let k31: i64 = 1 << 31;
    if (-k31..k31).contains(&value) {
        let mut v = vec![(RDB_ENCVAL << 6) | RDB_ENC_INT32];
        v.extend_from_slice(&(value as i32).to_le_bytes());
        return Some(v);
    }
    None
}

/// `TryIntegerEncoding`: encode a canonical decimal string as an integer string
/// when it round-trips through a signed 32-bit value.
fn try_integer_encoding(s: &[u8]) -> Option<Vec<u8>> {
    if s.len() > 11 {
        return None;
    }
    let value = intset::string2ll(s)?;
    if crate::util::itoa(value) != s {
        return None;
    }
    encode_integer(value)
}

/// `SaveLongLongAsString`: an integer-encoded string, falling back to a length
/// plus decimal bytes (never an int64 encoding).
fn save_long_long_as_string(out: &mut Vec<u8>, value: i64) {
    match encode_integer(value) {
        Some(enc) => out.extend_from_slice(&enc),
        None => {
            let bytes = crate::util::itoa(value);
            write_len(out, bytes.len() as u64);
            out.extend_from_slice(&bytes);
        }
    }
}

/// `SaveBinaryDouble`: a raw little-endian binary64 (RDB_VERSION >= 8).
fn save_binary_double(out: &mut Vec<u8>, val: f64) {
    out.extend_from_slice(&val.to_bits().to_le_bytes());
}

/// `SaveString`: integer encoding for short canonical ints, LZF compression in
/// SINGLE_ENTRY mode when it saves enough bytes, otherwise length + verbatim.
fn save_string(out: &mut Vec<u8>, s: &[u8]) {
    if s.len() <= 11
        && let Some(enc) = try_integer_encoding(s)
    {
        out.extend_from_slice(&enc);
        return;
    }
    if s.len() > 20
        && let Some(compressed) = lzf::compress(s)
    {
        let worth_it =
            compressed.len() + 8 < s.len() && compressed.len() < (s.len() as f64 * 0.85) as usize;
        if worth_it {
            out.push((RDB_ENCVAL << 6) | RDB_ENC_LZF);
            write_len(out, compressed.len() as u64);
            write_len(out, s.len() as u64);
            out.extend_from_slice(&compressed);
            return;
        }
    }
    write_len(out, s.len() as u64);
    out.extend_from_slice(s);
}

fn build_hash_listpack(h: &Hash) -> Vec<u8> {
    let mut lp = listpack::Listpack::new();
    for (f, v) in h.iter() {
        lp.append_bytes(f.as_bytes());
        lp.append_bytes(v.as_bytes());
    }
    lp.into_vec()
}

/// Decide the RDB type, mirroring `RdbObjectType` (`rdb_save.cc:166`).
fn rdb_object_type(pv: &PrimeValue) -> u8 {
    match pv {
        PrimeValue::Str(_) => RDB_TYPE_STRING,
        PrimeValue::List(_) => RDB_TYPE_LIST_QUICKLIST_2,
        PrimeValue::Set(s) => {
            if s.has_expiry() {
                RDB_TYPE_SET_WITH_EXPIRY
            } else if s.len() <= K_MAX_INTSET_ENTRIES
                && s.members()
                    .iter()
                    .all(|m| intset::string2ll(m.as_bytes()).is_some())
            {
                RDB_TYPE_SET_INTSET
            } else {
                RDB_TYPE_SET
            }
        }
        PrimeValue::Hash(h) => {
            if h.has_expiry() {
                RDB_TYPE_HASH_WITH_EXPIRY
            } else if h.is_small() && build_hash_listpack(h).len() <= K_MAX_LISTPACK_MAP_BYTES {
                RDB_TYPE_HASH_LISTPACK
            } else {
                RDB_TYPE_HASH
            }
        }
        PrimeValue::ZSet(_) => RDB_TYPE_ZSET_2,
        PrimeValue::Stream(_) => RDB_TYPE_STREAM_LISTPACKS_3,
        PrimeValue::Sbf(_) => RDB_TYPE_SBF,
        PrimeValue::Cms(_) => RDB_TYPE_CMS,
        PrimeValue::Cuckoo(_) => RDB_TYPE_CUCKOO,
        PrimeValue::Topk(_) => RDB_TYPE_TOPK,
        PrimeValue::Json(_) => RDB_TYPE_JSON,
    }
}

fn save_list(out: &mut Vec<u8>, ql: &QuickList) {
    write_len(out, ql.chunk_count() as u64);
    for chunk in ql.chunks() {
        write_len(out, QUICKLIST_NODE_CONTAINER_PACKED as u64);
        let mut lp = listpack::Listpack::new();
        for item in chunk {
            match item {
                ListItem::Int(i) => lp.append_integer(*i),
                ListItem::Str(s) => lp.append_bytes(s.as_bytes()),
            }
        }
        save_string(out, &lp.into_vec());
    }
}

fn save_set(out: &mut Vec<u8>, s: &Set, typ: u8) {
    if typ == RDB_TYPE_SET_INTSET {
        let members: Vec<i64> = s
            .members()
            .iter()
            .filter_map(|m| intset::string2ll(m.as_bytes()))
            .collect();
        let blob = intset::build(members);
        save_string(out, &blob);
        return;
    }
    let has_expiry = typ == RDB_TYPE_SET_WITH_EXPIRY;
    write_len(out, s.len() as u64);
    for member in s.members() {
        save_string(out, member.as_bytes());
        if has_expiry {
            let expiry = match s.member_expire_ms(member.as_bytes()) {
                Some(ms) => (ms / 1000).saturating_sub(K_MEMBER_EXPIRY_BASE) as i64,
                None => -1,
            };
            save_long_long_as_string(out, expiry);
        }
    }
}

fn save_hash(out: &mut Vec<u8>, h: &Hash, typ: u8) {
    if typ == RDB_TYPE_HASH_LISTPACK {
        save_string(out, &build_hash_listpack(h));
        return;
    }
    let has_expiry = typ == RDB_TYPE_HASH_WITH_EXPIRY;
    write_len(out, h.len() as u64);
    for (f, v) in h.iter() {
        save_string(out, f.as_bytes());
        save_string(out, v.as_bytes());
        if has_expiry {
            let expiry = match h.field_expire_ms(f.as_bytes()) {
                Some(ms) => (ms / 1000).saturating_sub(K_MEMBER_EXPIRY_BASE) as i64,
                None => -1,
            };
            save_long_long_as_string(out, expiry);
        }
    }
}

fn save_zset(out: &mut Vec<u8>, z: &ZSet) {
    write_len(out, z.len() as u64);
    for (member, score) in z.iter() {
        save_string(out, member.as_bytes());
        save_binary_double(out, score);
    }
}

/// Encode a stream ID as the 16-byte big-endian radix key (`StreamEncodeID`).
fn encode_stream_id(id: &StreamId) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&id.ms.to_be_bytes());
    b[8..].copy_from_slice(&id.seq.to_be_bytes());
    b
}

/// Append one entry record to a stream listpack, mirroring
/// `StreamAppendItem` (`stream_family.cc:1089`).
fn append_stream_entry(
    lp: &mut listpack::Listpack,
    entry: &StreamEntry,
    id: StreamId,
    master_id: StreamId,
    master_fields: &[CompactString],
) {
    let numfields = entry.fields.len() as i64;
    let same = entry.fields.len() == master_fields.len()
        && entry
            .fields
            .iter()
            .zip(master_fields)
            .all(|((f, _), mf)| f == mf);
    let mut flags = 0i64;
    if entry.deleted {
        flags |= STREAM_ITEM_FLAG_DELETED;
    }
    if same {
        flags |= STREAM_ITEM_FLAG_SAMEFIELDS;
    }
    lp.append_integer(flags);
    lp.append_integer((id.ms - master_id.ms) as i64);
    lp.append_integer((id.seq - master_id.seq) as i64);
    if !same {
        lp.append_integer(numfields);
    }
    for (f, v) in &entry.fields {
        if !same {
            lp.append_bytes(f.as_bytes());
        }
        lp.append_bytes(v.as_bytes());
    }
    let mut lp_count = numfields + 3;
    if !same {
        lp_count += numfields + 1;
    }
    lp.append_integer(lp_count);
}

/// A stream listpack node being accumulated before it is flushed.
struct PendingNode<'a> {
    master_id: StreamId,
    master_fields: Vec<CompactString>,
    entries: Vec<(StreamId, &'a StreamEntry)>,
}

impl PendingNode<'_> {
    /// Build the final listpack: the master entry (with live/deleted counts)
    /// followed by every entry record (`stream_family.cc:1015`).
    fn build(&self) -> Vec<u8> {
        let live = self.entries.iter().filter(|(_, e)| !e.deleted).count() as i64;
        let deleted = self.entries.len() as i64 - live;
        let mut lp = listpack::Listpack::new();
        lp.append_integer(live);
        lp.append_integer(deleted);
        lp.append_integer(self.master_fields.len() as i64);
        for f in &self.master_fields {
            lp.append_bytes(f.as_bytes());
        }
        lp.append_integer(0);
        for (id, entry) in &self.entries {
            append_stream_entry(&mut lp, entry, *id, self.master_id, &self.master_fields);
        }
        lp.into_vec()
    }
}

fn save_stream(out: &mut Vec<u8>, s: &Stream) {
    // Rebuild the radix-tree listpack nodes the same way `StreamAppendItem`
    // would have: reuse the tail node while it stays under the byte/entry
    // limits, otherwise start a new node keyed by the entry's full ID.
    let mut nodes: Vec<(StreamId, Vec<u8>)> = Vec::new();
    let mut cur: Option<PendingNode> = None;
    for (id, entry) in s.entries.iter() {
        let id = *id;
        let totelelen: usize = entry.fields.iter().map(|(f, v)| f.len() + v.len()).sum();
        let make_new = match &cur {
            Some(node) => {
                node.build().len() + totelelen >= K_STREAM_NODE_MAX_BYTES
                    || node.entries.len() as i64 >= K_STREAM_NODE_MAX_ENTRIES
            }
            None => true,
        };
        if make_new {
            if let Some(node) = cur.take() {
                nodes.push((node.master_id, node.build()));
            }
            let master_fields: Vec<CompactString> =
                entry.fields.iter().map(|(f, _)| f.clone()).collect();
            cur = Some(PendingNode {
                master_id: id,
                master_fields,
                entries: vec![(id, entry)],
            });
        } else {
            cur.as_mut().unwrap().entries.push((id, entry));
        }
    }
    if let Some(node) = cur.take() {
        nodes.push((node.master_id, node.build()));
    }

    write_len(out, nodes.len() as u64);
    for (mid, lp) in &nodes {
        let key = encode_stream_id(mid);
        save_string(out, &key);
        save_string(out, lp);
    }

    write_len(out, s.length);
    write_len(out, s.last_id.ms);
    write_len(out, s.last_id.seq);
    let first_id = s.first_entry().copied().unwrap_or(StreamId::MIN);
    write_len(out, first_id.ms);
    write_len(out, first_id.seq);
    write_len(out, s.max_deleted_id.ms);
    write_len(out, s.max_deleted_id.seq);
    write_len(out, s.entries.len() as u64); // entries_added

    let mut groups: Vec<(&CompactString, &ConsumerGroup)> = s.groups.iter().collect();
    groups.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    write_len(out, groups.len() as u64);
    for (name, cg) in groups {
        save_string(out, name.as_bytes());
        write_len(out, cg.last_delivered.ms);
        write_len(out, cg.last_delivered.seq);
        write_len(out, cg.entries_read);
        write_len(out, cg.pel.len() as u64);
        for (eid, pe) in &cg.pel {
            out.extend_from_slice(&encode_stream_id(eid));
            out.extend_from_slice(&pe.delivery_time.to_le_bytes());
            write_len(out, pe.delivery_count);
        }
        let mut consumers: Vec<&CompactString> = cg.consumers.keys().collect();
        consumers.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        write_len(out, consumers.len() as u64);
        for cname in consumers {
            let c = &cg.consumers[cname];
            save_string(out, cname.as_bytes());
            out.extend_from_slice(&c.seen_time.to_le_bytes());
            out.extend_from_slice(&c.active_time.to_le_bytes());
            let consumer_pel: Vec<&StreamId> = cg
                .pel
                .iter()
                .filter(|(_, pe)| pe.consumer == *cname)
                .map(|(eid, _)| eid)
                .collect();
            write_len(out, consumer_pel.len() as u64);
            for eid in &consumer_pel {
                out.extend_from_slice(&encode_stream_id(eid));
            }
        }
    }
}

fn save_value(out: &mut Vec<u8>, pv: &PrimeValue) {
    match pv {
        PrimeValue::Str(s) => save_string(out, s.as_bytes()),
        PrimeValue::List(ql) => save_list(out, ql),
        PrimeValue::Set(s) => save_set(out, s, rdb_object_type(pv)),
        PrimeValue::Hash(h) => save_hash(out, h, rdb_object_type(pv)),
        PrimeValue::ZSet(z) => save_zset(out, z),
        PrimeValue::Stream(s) => save_stream(out, s),
        PrimeValue::Sbf(s) => save_string(out, &s.serialize()),
        PrimeValue::Cms(c) => save_string(out, &c.serialize()),
        PrimeValue::Cuckoo(c) => save_string(out, &c.serialize()),
        PrimeValue::Topk(t) => save_string(out, &t.serialize()),
        PrimeValue::Json(j) => save_string(out, j.dump().as_bytes()),
    }
}

/// Serialize a value as a full DUMP payload (type byte + value + version +
/// CRC64), mirroring `RdbSerializer::DumpValue` with `ignore_crc=false`.
pub fn dump_value(pv: &PrimeValue) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(rdb_object_type(pv));
    save_value(&mut out, pv);
    out.extend_from_slice(&RDB_SER_VERSION.to_le_bytes());
    let crc = crc64::crc64(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

// ---------------------------------------------------------------------------
// RESTORE decoder
// ---------------------------------------------------------------------------

/// Error signalling a failed RESTORE decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreError {
    /// Malformed / corrupt payload -> "ERR Bad data format".
    BadDataFormat,
    /// Every member/field was already expired during deserialization
    /// (the reference maps this to `SKIPPED`, i.e. OK without storing).
    Expired,
}

/// Result of decoding a RESTORE payload.
#[derive(Debug, Clone)]
pub enum RestoreOutcome {
    Value(PrimeValue),
    Expired,
}

/// A bounds-checked cursor over the value bytes of a DUMP payload.
struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Reader { data, pos: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, RestoreError> {
        let b = *self.data.get(self.pos).ok_or(RestoreError::BadDataFormat)?;
        self.pos += 1;
        Ok(b)
    }

    fn read_exact(&mut self, n: usize) -> Result<&'a [u8], RestoreError> {
        let end = self.pos.checked_add(n).ok_or(RestoreError::BadDataFormat)?;
        let s = self
            .data
            .get(self.pos..end)
            .ok_or(RestoreError::BadDataFormat)?;
        self.pos = end;
        Ok(s)
    }

    /// `rdbLoadLen` semantics: returns `(value, is_encoded)` where
    /// `is_encoded` is set for `RDB_ENCVAL` (0xc0..0xff) markers. 32/64-bit
    /// lengths are big-endian; the reserved 0x82..0xbf values are corrupt.
    fn read_len(&mut self) -> Result<(u64, bool), RestoreError> {
        let b = self.read_u8()?;
        match b >> 6 {
            0 => Ok(((b & 0x3f) as u64, false)),
            1 => {
                let lo = self.read_u8()? as u64;
                Ok(((((b & 0x3f) as u64) << 8) | lo, false))
            }
            2 => match b {
                0x80 => {
                    let s = self.read_exact(4)?;
                    Ok((u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as u64, false))
                }
                0x81 => {
                    let s = self.read_exact(8)?;
                    Ok((
                        u64::from_be_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]),
                        false,
                    ))
                }
                _ => Err(RestoreError::BadDataFormat),
            },
            _ => Ok(((b & 0x3f) as u64, true)),
        }
    }

    /// `FetchGenericString`: an RDB string, decoding integer and LZF
    /// encodings. Integer encodings are rendered as canonical decimals.
    fn read_string(&mut self) -> Result<Vec<u8>, RestoreError> {
        let (len, encoded) = self.read_len()?;
        if !encoded {
            return Ok(self.read_exact(len as usize)?.to_vec());
        }
        match len as u8 {
            RDB_ENC_INT8 => {
                let b = self.read_exact(1)?[0] as i8 as i64;
                Ok(crate::util::itoa(b))
            }
            RDB_ENC_INT16 => {
                let s = self.read_exact(2)?;
                let v = i16::from_le_bytes([s[0], s[1]]) as i64;
                Ok(crate::util::itoa(v))
            }
            RDB_ENC_INT32 => {
                let s = self.read_exact(4)?;
                let v = i32::from_le_bytes([s[0], s[1], s[2], s[3]]) as i64;
                Ok(crate::util::itoa(v))
            }
            RDB_ENC_LZF => {
                let (clen, _) = self.read_len()?;
                let (ulen, _) = self.read_len()?;
                if clen == 0 || ulen <= clen || ulen > 1 << 29 {
                    return Err(RestoreError::BadDataFormat);
                }
                let comp = self.read_exact(clen as usize)?;
                lzf::decompress(comp, ulen as usize).ok_or(RestoreError::BadDataFormat)
            }
            _ => Err(RestoreError::BadDataFormat),
        }
    }

    fn read_u64_le(&mut self) -> Result<u64, RestoreError> {
        let s = self.read_exact(8)?;
        Ok(u64::from_le_bytes([
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
        ]))
    }

    fn read_binary_double(&mut self) -> Result<f64, RestoreError> {
        Ok(f64::from_bits(self.read_u64_le()?))
    }
}

/// `MemberTimeSeconds` (`common.h`): the reference's relative member-seconds
/// clock.
fn member_time_seconds(now_ms: u64) -> i64 {
    (now_ms / 1000) as i64 - K_MEMBER_EXPIRY_BASE as i64
}

/// Decode one listpack element into its string bytes (`lpGet` semantics).
fn entry_to_bytes(lp: &[u8], pos: usize) -> Result<Vec<u8>, RestoreError> {
    match listpack::entry_at(lp, pos).ok_or(RestoreError::BadDataFormat)? {
        listpack::Entry::Int(v) => Ok(crate::util::itoa(v)),
        listpack::Entry::Str(s) => Ok(s.to_vec()),
    }
}

fn load_set(r: &mut Reader, with_expiry: bool, now_ms: u64) -> Result<Option<Set>, RestoreError> {
    let len = r.read_len()?.0;
    let mut s = Set::new();
    let mut values_expired = false;
    for _ in 0..len {
        let member = CompactString::from_bytes(&r.read_string()?);
        if with_expiry {
            let ttl_str = r.read_string()?;
            let ttl_time = crate::util::parse_i64(&ttl_str).ok_or(RestoreError::BadDataFormat)?;
            if ttl_time == -1 {
                if !s.add(member) {
                    return Err(RestoreError::BadDataFormat);
                }
            } else if ttl_time <= member_time_seconds(now_ms) {
                values_expired = true;
            } else {
                let expiry_ms = ((K_MEMBER_EXPIRY_BASE as i64 + ttl_time) * 1000) as u64;
                if !s.add_expirable(member, expiry_ms, false) {
                    return Err(RestoreError::BadDataFormat);
                }
            }
        } else if !s.add(member) {
            return Err(RestoreError::BadDataFormat);
        }
    }
    if s.is_empty() && values_expired {
        return Ok(None);
    }
    Ok(Some(s))
}

fn load_intset(r: &mut Reader) -> Result<Set, RestoreError> {
    let blob = r.read_string()?;
    let mut s = Set::new();
    for v in intset::values(&blob).ok_or(RestoreError::BadDataFormat)? {
        s.add(CompactString::from_bytes(&crate::util::itoa(v)));
    }
    Ok(s)
}

fn load_lp_set(r: &mut Reader) -> Result<Set, RestoreError> {
    let lp = r.read_string()?;
    if !listpack::validate_deep(&lp) {
        return Err(RestoreError::BadDataFormat);
    }
    let mut s = Set::new();
    let mut p = listpack::first(&lp);
    while let Some(pos) = p {
        let member = CompactString::from_bytes(&entry_to_bytes(&lp, pos)?);
        if !s.add(member) {
            return Err(RestoreError::BadDataFormat);
        }
        p = listpack::next(&lp, pos);
    }
    Ok(s)
}

fn load_hash(r: &mut Reader, with_expiry: bool, now_ms: u64) -> Result<Option<Hash>, RestoreError> {
    let len = r.read_len()?.0;
    let mut h = Hash::new();
    let mut values_expired = false;
    for _ in 0..len {
        let field = CompactString::from_bytes(&r.read_string()?);
        let value = CompactString::from_bytes(&r.read_string()?);
        let mut expire_ms = None;
        if with_expiry {
            let ttl_str = r.read_string()?;
            let ttl_time = crate::util::parse_i64(&ttl_str).ok_or(RestoreError::BadDataFormat)?;
            if ttl_time != -1 {
                if ttl_time <= member_time_seconds(now_ms) {
                    values_expired = true;
                    continue;
                }
                expire_ms = Some(((K_MEMBER_EXPIRY_BASE as i64 + ttl_time) * 1000) as u64);
            }
        }
        if !h.add_or_skip(field, value, expire_ms) {
            return Err(RestoreError::BadDataFormat);
        }
    }
    if h.is_empty() && values_expired {
        return Ok(None);
    }
    Ok(Some(h))
}

fn load_hash_listpack(r: &mut Reader) -> Result<Hash, RestoreError> {
    let lp = r.read_string()?;
    if !listpack::validate_deep(&lp) {
        return Err(RestoreError::BadDataFormat);
    }
    let mut h = Hash::new();
    let mut p = listpack::first(&lp);
    while let Some(fpos) = p {
        let vpos = listpack::next(&lp, fpos).ok_or(RestoreError::BadDataFormat)?;
        let field = CompactString::from_bytes(&entry_to_bytes(&lp, fpos)?);
        let value = CompactString::from_bytes(&entry_to_bytes(&lp, vpos)?);
        if !h.add_or_skip(field, value, None) {
            return Err(RestoreError::BadDataFormat);
        }
        p = listpack::next(&lp, vpos);
    }
    Ok(h)
}

fn load_zset(r: &mut Reader) -> Result<ZSet, RestoreError> {
    let len = r.read_len()?.0;
    let mut z = ZSet::new();
    for _ in 0..len {
        let member = CompactString::from_bytes(&r.read_string()?);
        let score = r.read_binary_double()?;
        if z.contains(member.as_bytes()) {
            return Err(RestoreError::BadDataFormat);
        }
        z.insert(member, score);
    }
    Ok(z)
}

fn load_zset_listpack(r: &mut Reader) -> Result<ZSet, RestoreError> {
    let lp = r.read_string()?;
    if !listpack::validate_deep(&lp) {
        return Err(RestoreError::BadDataFormat);
    }
    let mut z = ZSet::new();
    let mut p = listpack::first(&lp);
    while let Some(mpos) = p {
        let spos = listpack::next(&lp, mpos).ok_or(RestoreError::BadDataFormat)?;
        let member = CompactString::from_bytes(&entry_to_bytes(&lp, mpos)?);
        let score = match listpack::entry_at(&lp, spos).ok_or(RestoreError::BadDataFormat)? {
            listpack::Entry::Int(v) => v as f64,
            listpack::Entry::Str(s) => {
                crate::util::parse_double(s).ok_or(RestoreError::BadDataFormat)?
            }
        };
        if z.contains(member.as_bytes()) {
            return Err(RestoreError::BadDataFormat);
        }
        z.insert(member, score);
        p = listpack::next(&lp, spos);
    }
    Ok(z)
}

fn load_old_list(r: &mut Reader) -> Result<QuickList, RestoreError> {
    let len = r.read_len()?.0;
    let mut ql = QuickList::new();
    for _ in 0..len {
        ql.push_back(ListItem::from_bytes(&r.read_string()?));
    }
    Ok(ql)
}

fn load_quicklist(r: &mut Reader) -> Result<QuickList, RestoreError> {
    let len = r.read_len()?.0;
    let mut ql = QuickList::new();
    for _ in 0..len {
        let container = r.read_len()?.0;
        let blob = r.read_string()?;
        match container {
            c if c == QUICKLIST_NODE_CONTAINER_PACKED as u64 => {
                if blob.is_empty() || !listpack::validate_deep(&blob) {
                    return Err(RestoreError::BadDataFormat);
                }
                let mut p = listpack::first(&blob);
                while let Some(pos) = p {
                    match listpack::entry_at(&blob, pos).ok_or(RestoreError::BadDataFormat)? {
                        listpack::Entry::Int(v) => ql.push_back(ListItem::Int(v)),
                        listpack::Entry::Str(s) => ql.push_back(ListItem::from_bytes(s)),
                    }
                    p = listpack::next(&blob, pos);
                }
            }
            1 => ql.push_back(ListItem::from_bytes(&blob)),
            _ => return Err(RestoreError::BadDataFormat),
        }
    }
    Ok(ql)
}

/// Decode a 16-byte big-endian stream radix key into a `StreamId`.
fn decode_stream_id(raw: &[u8]) -> StreamId {
    let mut ms = [0u8; 8];
    let mut seq = [0u8; 8];
    ms.copy_from_slice(&raw[..8]);
    seq.copy_from_slice(&raw[8..]);
    StreamId {
        ms: u64::from_be_bytes(ms),
        seq: u64::from_be_bytes(seq),
    }
}

/// Parse one stream listpack node and insert its entries. Mirrors
/// `StreamValidateListpackIntegrity` (`rdb_load.cc:100`) and the entry-record
/// walk, verifying the master live/deleted counts, the `0` terminator, the
/// per-record lp-counts and that the walk consumes the whole listpack.
fn parse_stream_node(
    lp: &[u8],
    master_id: StreamId,
    entries: &mut BTreeMap<StreamId, StreamEntry>,
) -> Result<(), RestoreError> {
    let mut p = listpack::first(lp).ok_or(RestoreError::BadDataFormat)?;
    let live = listpack::get_integer(lp, p).ok_or(RestoreError::BadDataFormat)?;
    p = listpack::next(lp, p).ok_or(RestoreError::BadDataFormat)?;
    let deleted = listpack::get_integer(lp, p).ok_or(RestoreError::BadDataFormat)?;
    p = listpack::next(lp, p).ok_or(RestoreError::BadDataFormat)?;
    let numfields = listpack::get_integer(lp, p).ok_or(RestoreError::BadDataFormat)?;
    p = listpack::next(lp, p).ok_or(RestoreError::BadDataFormat)?;

    if live < 0 || deleted < 0 || numfields < 0 {
        return Err(RestoreError::BadDataFormat);
    }

    let mut master_fields: Vec<CompactString> = Vec::with_capacity(numfields as usize);
    for _ in 0..numfields {
        master_fields.push(CompactString::from_bytes(&entry_to_bytes(lp, p)?));
        p = listpack::next(lp, p).ok_or(RestoreError::BadDataFormat)?;
    }
    if listpack::get_integer(lp, p).ok_or(RestoreError::BadDataFormat)? != 0 {
        return Err(RestoreError::BadDataFormat);
    }
    p = listpack::next(lp, p).ok_or(RestoreError::BadDataFormat)?;

    let mut live_count = 0i64;
    let mut deleted_count = 0i64;
    let mut cur: Option<usize> = Some(p);
    while let Some(flags_pos) = cur {
        let flags = listpack::get_integer(lp, flags_pos).ok_or(RestoreError::BadDataFormat)?;
        let mut nxt = listpack::next(lp, flags_pos).ok_or(RestoreError::BadDataFormat)?;
        let ms_delta = listpack::get_integer(lp, nxt).ok_or(RestoreError::BadDataFormat)?;
        nxt = listpack::next(lp, nxt).ok_or(RestoreError::BadDataFormat)?;
        let seq_delta = listpack::get_integer(lp, nxt).ok_or(RestoreError::BadDataFormat)?;
        nxt = listpack::next(lp, nxt).ok_or(RestoreError::BadDataFormat)?;

        let samefields = flags & STREAM_ITEM_FLAG_SAMEFIELDS != 0;
        let entry_numfields = if samefields {
            master_fields.len() as i64
        } else {
            let n = listpack::get_integer(lp, nxt).ok_or(RestoreError::BadDataFormat)?;
            if n < 0 {
                return Err(RestoreError::BadDataFormat);
            }
            nxt = listpack::next(lp, nxt).ok_or(RestoreError::BadDataFormat)?;
            n
        };

        let mut fields: Vec<(CompactString, CompactString)> = Vec::new();
        if samefields {
            for i in 0..entry_numfields as usize {
                let f = master_fields
                    .get(i)
                    .ok_or(RestoreError::BadDataFormat)?
                    .clone();
                let v = CompactString::from_bytes(&entry_to_bytes(lp, nxt)?);
                nxt = listpack::next(lp, nxt).ok_or(RestoreError::BadDataFormat)?;
                fields.push((f, v));
            }
        } else {
            for _ in 0..entry_numfields {
                let f = CompactString::from_bytes(&entry_to_bytes(lp, nxt)?);
                nxt = listpack::next(lp, nxt).ok_or(RestoreError::BadDataFormat)?;
                let v = CompactString::from_bytes(&entry_to_bytes(lp, nxt)?);
                nxt = listpack::next(lp, nxt).ok_or(RestoreError::BadDataFormat)?;
                fields.push((f, v));
            }
        }

        let lp_count = listpack::get_integer(lp, nxt).ok_or(RestoreError::BadDataFormat)?;
        let expected_count = entry_numfields + 3 + if samefields { 0 } else { entry_numfields + 1 };
        if lp_count != expected_count {
            return Err(RestoreError::BadDataFormat);
        }
        cur = listpack::next(lp, nxt);

        let id = StreamId {
            ms: master_id.ms.wrapping_add(ms_delta as u64),
            seq: master_id.seq.wrapping_add(seq_delta as u64),
        };
        if flags & STREAM_ITEM_FLAG_DELETED != 0 {
            deleted_count += 1;
        } else {
            live_count += 1;
        }
        entries.insert(
            id,
            StreamEntry {
                fields,
                deleted: flags & STREAM_ITEM_FLAG_DELETED != 0,
            },
        );
    }

    if live != live_count || deleted != deleted_count {
        return Err(RestoreError::BadDataFormat);
    }
    Ok(())
}

fn load_stream(r: &mut Reader, typ: u8) -> Result<Stream, RestoreError> {
    let listpacks = r.read_len()?.0;
    let mut entries: BTreeMap<StreamId, StreamEntry> = BTreeMap::new();
    for _ in 0..listpacks {
        let key = r.read_string()?;
        if key.len() != 16 {
            return Err(RestoreError::BadDataFormat);
        }
        let lp = r.read_string()?;
        if lp.is_empty() || !listpack::validate_deep(&lp) {
            return Err(RestoreError::BadDataFormat);
        }
        let master_id = decode_stream_id(&key);
        parse_stream_node(&lp, master_id, &mut entries)?;
    }

    let stream_len = r.read_len()?.0;
    let last_id = StreamId {
        ms: r.read_len()?.0,
        seq: r.read_len()?.0,
    };
    let (max_deleted_id, _entries_added) = if typ >= RDB_TYPE_STREAM_LISTPACKS_2 {
        // first_id is not stored in the Rust Stream shape; skip it.
        let _first_id = StreamId {
            ms: r.read_len()?.0,
            seq: r.read_len()?.0,
        };
        (
            StreamId {
                ms: r.read_len()?.0,
                seq: r.read_len()?.0,
            },
            r.read_len()?.0,
        )
    } else {
        (StreamId::MIN, stream_len)
    };

    let mut groups: HashMap<CompactString, ConsumerGroup> = HashMap::new();
    let cgroups = r.read_len()?.0;
    for _ in 0..cgroups {
        let name = CompactString::from_bytes(&r.read_string()?);
        let last_delivered = StreamId {
            ms: r.read_len()?.0,
            seq: r.read_len()?.0,
        };
        let mut entries_read = 0;
        if typ >= RDB_TYPE_STREAM_LISTPACKS_2 {
            entries_read = r.read_len()?.0;
        }

        let pel_size = r.read_len()?.0;
        let mut pel: BTreeMap<StreamId, PendingEntry> = BTreeMap::new();
        for _ in 0..pel_size {
            let raw = r.read_exact(16)?;
            let eid = decode_stream_id(raw);
            let delivery_time = r.read_u64_le()?;
            let delivery_count = r.read_len()?.0;
            pel.insert(
                eid,
                PendingEntry {
                    consumer: CompactString::new(),
                    delivery_time,
                    delivery_count,
                },
            );
        }

        let consumers_num = r.read_len()?.0;
        let mut consumers: HashMap<CompactString, Consumer> = HashMap::new();
        for _ in 0..consumers_num {
            let cname = CompactString::from_bytes(&r.read_string()?);
            let seen_time = r.read_u64_le()?;
            let active_time = if typ >= RDB_TYPE_STREAM_LISTPACKS_3 {
                r.read_u64_le()?
            } else {
                seen_time
            };
            let cpel_size = r.read_len()?.0;
            for _ in 0..cpel_size {
                let raw = r.read_exact(16)?;
                let eid = decode_stream_id(raw);
                match pel.get_mut(&eid) {
                    Some(pe) => pe.consumer = cname.clone(),
                    None => {
                        // Lenient: an ID outside the global PEL becomes an
                        // orphan pending entry owned by this consumer.
                        pel.insert(
                            eid,
                            PendingEntry {
                                consumer: cname.clone(),
                                delivery_time: 0,
                                delivery_count: 0,
                            },
                        );
                    }
                }
            }
            consumers.insert(
                cname,
                Consumer {
                    seen_time,
                    active_time,
                    pending: cpel_size,
                },
            );
        }

        groups.insert(
            name,
            ConsumerGroup {
                last_delivered,
                entries_read,
                consumers,
                pel,
            },
        );
    }

    let mut s = Stream::new();
    s.entries = entries;
    s.length = stream_len;
    s.last_id = last_id;
    s.max_deleted_id = max_deleted_id;
    s.groups = groups;
    Ok(s)
}

fn load_value(r: &mut Reader, typ: u8, now_ms: u64) -> Result<RestoreOutcome, RestoreError> {
    match typ {
        RDB_TYPE_STRING => {
            let s = r.read_string()?;
            Ok(RestoreOutcome::Value(PrimeValue::Str(
                CompactString::from_bytes(&s),
            )))
        }
        RDB_TYPE_LIST => Ok(RestoreOutcome::Value(PrimeValue::List(load_old_list(r)?))),
        RDB_TYPE_SET => match load_set(r, false, now_ms)? {
            Some(s) => Ok(RestoreOutcome::Value(PrimeValue::Set(s))),
            None => Ok(RestoreOutcome::Expired),
        },
        RDB_TYPE_SET_INTSET => Ok(RestoreOutcome::Value(PrimeValue::Set(load_intset(r)?))),
        RDB_TYPE_SET_LISTPACK => Ok(RestoreOutcome::Value(PrimeValue::Set(load_lp_set(r)?))),
        RDB_TYPE_SET_WITH_EXPIRY => match load_set(r, true, now_ms)? {
            Some(s) => Ok(RestoreOutcome::Value(PrimeValue::Set(s))),
            None => Ok(RestoreOutcome::Expired),
        },
        RDB_TYPE_HASH => match load_hash(r, false, now_ms)? {
            Some(h) => Ok(RestoreOutcome::Value(PrimeValue::Hash(h))),
            None => Ok(RestoreOutcome::Expired),
        },
        RDB_TYPE_HASH_LISTPACK => Ok(RestoreOutcome::Value(PrimeValue::Hash(load_hash_listpack(
            r,
        )?))),
        RDB_TYPE_HASH_WITH_EXPIRY => match load_hash(r, true, now_ms)? {
            Some(h) => Ok(RestoreOutcome::Value(PrimeValue::Hash(h))),
            None => Ok(RestoreOutcome::Expired),
        },
        RDB_TYPE_ZSET_2 => Ok(RestoreOutcome::Value(PrimeValue::ZSet(load_zset(r)?))),
        RDB_TYPE_ZSET_LISTPACK => Ok(RestoreOutcome::Value(PrimeValue::ZSet(load_zset_listpack(
            r,
        )?))),
        RDB_TYPE_LIST_QUICKLIST | RDB_TYPE_LIST_QUICKLIST_2 => {
            Ok(RestoreOutcome::Value(PrimeValue::List(load_quicklist(r)?)))
        }
        RDB_TYPE_STREAM_LISTPACKS | RDB_TYPE_STREAM_LISTPACKS_2 | RDB_TYPE_STREAM_LISTPACKS_3 => {
            Ok(RestoreOutcome::Value(PrimeValue::Stream(load_stream(
                r, typ,
            )?)))
        }
        RDB_TYPE_SBF => {
            let blob = r.read_string()?;
            match SBF::deserialize(&blob) {
                Ok(sbf) => Ok(RestoreOutcome::Value(PrimeValue::Sbf(sbf))),
                Err(_) => Err(RestoreError::BadDataFormat),
            }
        }
        RDB_TYPE_CMS => {
            let blob = r.read_string()?;
            match Cms::deserialize(&blob) {
                Some(cms) => Ok(RestoreOutcome::Value(PrimeValue::Cms(cms))),
                None => Err(RestoreError::BadDataFormat),
            }
        }
        RDB_TYPE_CUCKOO => {
            let blob = r.read_string()?;
            match CuckooFilter::deserialize(&blob) {
                Some(cf) => Ok(RestoreOutcome::Value(PrimeValue::Cuckoo(cf))),
                None => Err(RestoreError::BadDataFormat),
            }
        }
        RDB_TYPE_TOPK => {
            let blob = r.read_string()?;
            match Topk::deserialize(&blob) {
                Some(tk) => Ok(RestoreOutcome::Value(PrimeValue::Topk(tk))),
                None => Err(RestoreError::BadDataFormat),
            }
        }
        RDB_TYPE_JSON => {
            let blob = r.read_string()?;
            match Json::parse(&blob) {
                Ok(j) => Ok(RestoreOutcome::Value(PrimeValue::Json(j))),
                Err(_) => Err(RestoreError::BadDataFormat),
            }
        }
        _ => Err(RestoreError::BadDataFormat),
    }
}

/// Restore a DUMP payload (`[type][value][version u16 LE][crc64 u64 LE]`) back
/// into a `PrimeValue`, mirroring `GetRdbVersion` + `RdbRestoreValue::Add`.
///
/// The footer is validated first: the payload must exceed 10 bytes, the LE
/// version must not exceed [`RDB_VERSION`], and the CRC64 (computed over every
/// byte except the final 8) must match.
pub fn restore_value(payload: &[u8], now_ms: u64) -> Result<RestoreOutcome, RestoreError> {
    const FOOTER: usize = size_of::<u16>() + size_of::<u64>();
    if payload.len() <= FOOTER {
        return Err(RestoreError::BadDataFormat);
    }
    let footer = &payload[payload.len() - FOOTER..];
    let version = u16::from_le_bytes([footer[0], footer[1]]);
    if version as u64 > RDB_VERSION {
        return Err(RestoreError::BadDataFormat);
    }
    let expected_crc = u64::from_le_bytes([
        footer[2], footer[3], footer[4], footer[5], footer[6], footer[7], footer[8], footer[9],
    ]);
    let actual_crc = crc64::crc64(&payload[..payload.len() - size_of::<u64>()]);
    if actual_crc != expected_crc {
        return Err(RestoreError::BadDataFormat);
    }

    let mut r = Reader::new(payload);
    let typ = r.read_u8()?;
    load_value(&mut r, typ, now_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::compact::CompactString;

    fn cs(s: &str) -> CompactString {
        CompactString::from(s)
    }

    /// Byte-exact vectors taken from `GenericFamilyTest::Dump` and
    /// `Restore` (`generic_family_test.cc`).
    #[test]
    fn string_vector() {
        let dump = dump_value(&PrimeValue::Str(cs("19")));
        assert_eq!(
            dump,
            vec![
                0x00, 0xc0, 0x13, 0x09, 0x00, 0x23, 0x13, 0x6f, 0x4d, 0x68, 0xf6, 0x35, 0x6e
            ]
        );
    }

    #[test]
    fn list_vector() {
        let mut ql = QuickList::new();
        ql.push_back(ListItem::Str(cs("20")));
        let dump = dump_value(&PrimeValue::List(ql));
        assert_eq!(
            dump,
            vec![
                0x12, 0x01, 0x02, 0x09, 0x09, 0x00, 0x00, 0x00, 0x01, 0x00, 0x14, 0x01, 0xff, 0x09,
                0x00, 0xfb, 0xbd, 0x36, 0xf8, 0xb4, 0x74, 0x25, 0x3b,
            ]
        );
    }

    #[test]
    fn hash_listpack_vector() {
        let mut h = Hash::new();
        h.set(cs("19"), cs("1234"));
        let dump = dump_value(&PrimeValue::Hash(h));
        assert_eq!(
            dump,
            vec![
                0x10, 0x0c, 0x0c, 0x00, 0x00, 0x00, 0x02, 0x00, 0x13, 0x01, 0xc4, 0xd2, 0x02, 0xff,
                0x09, 0x00, 0x68, 0x4d, 0x73, 0xa4, 0x0f, 0x23, 0x4f, 0xc7,
            ]
        );
    }

    #[test]
    fn intset_set() {
        let mut s = Set::new();
        s.add(cs("1"));
        s.add(cs("2"));
        s.add(cs("3"));
        let dump = dump_value(&PrimeValue::Set(s));
        let mut expected = vec![0x0b, 0x0e];
        expected.extend_from_slice(&[2, 0, 0, 0, 3, 0, 0, 0, 1, 0, 2, 0, 3, 0]);
        expected.extend_from_slice(&9u16.to_le_bytes());
        expected.extend_from_slice(&crc64::crc64(&expected).to_le_bytes());
        assert_eq!(dump, expected);
    }

    #[test]
    fn strmap_set() {
        let mut s = Set::new();
        s.add(cs("a"));
        s.add(cs("b"));
        let dump = dump_value(&PrimeValue::Set(s));
        assert_eq!(dump[0], RDB_TYPE_SET);
        assert_eq!(dump[1], 2); // SaveLen(2)
        let mut p = vec![RDB_TYPE_SET, 2];
        save_string(&mut p, b"a");
        save_string(&mut p, b"b");
        let mut expected = p.clone();
        expected.extend_from_slice(&9u16.to_le_bytes());
        expected.extend_from_slice(&crc64::crc64(&expected).to_le_bytes());
        assert_eq!(dump, expected);
    }

    #[test]
    fn set_with_expiry_member_seconds() {
        let mut s = Set::new();
        // expire at base + 100s (member-second time 100).
        s.add_expirable(cs("m"), (K_MEMBER_EXPIRY_BASE + 100) * 1000, false);
        let dump = dump_value(&PrimeValue::Set(s));
        assert_eq!(dump[0], RDB_TYPE_SET_WITH_EXPIRY);
        assert_eq!(dump[1], 1); // one member
        assert_eq!(dump[2], 1); // SaveLen(1) for "m"
        assert_eq!(dump[3], 0x6d); // 'm'
        // value '100' -> SaveLongLongAsString(100) = 0xc0 0x64
        assert_eq!(&dump[4..6], &[0xc0, 0x64]);
    }

    #[test]
    fn hash_strmap_with_expiry() {
        let mut h = Hash::new();
        h.set(cs("f"), cs("v"));
        h.add_expirable(
            cs("e"),
            cs("w"),
            Some((K_MEMBER_EXPIRY_BASE + 42) * 1000),
            false,
        );
        let dump = dump_value(&PrimeValue::Hash(h));
        assert_eq!(dump[0], RDB_TYPE_HASH_WITH_EXPIRY);
        // count = 2
        assert_eq!(dump[1], 2);
    }

    #[test]
    fn zset_skiplist() {
        let mut z = ZSet::new();
        z.insert(cs("member"), 2.75);
        let dump = dump_value(&PrimeValue::ZSet(z));
        assert_eq!(dump[0], RDB_TYPE_ZSET_2);
        assert_eq!(dump[1], 1); // size
        // "member" (6 bytes, not int) -> SaveLen(6) + bytes
        assert_eq!(&dump[2..2 + 7], &[6, b'm', b'e', b'm', b'b', b'e', b'r']);
        // 2.75 as LE binary64
        let score = &dump[2 + 7..2 + 7 + 8];
        assert_eq!(score, &2.75f64.to_bits().to_le_bytes());
    }

    #[test]
    fn stream_single_node() {
        let mut s = Stream::new();
        let id = StreamId { ms: 1000, seq: 0 };
        s.append(id, vec![(cs("f"), cs("v"))]);
        let dump = dump_value(&PrimeValue::Stream(s));

        let mut expected = vec![RDB_TYPE_STREAM_LISTPACKS_3];
        write_len(&mut expected, 1); // node count
        let mut key = [0u8; 16];
        key[..8].copy_from_slice(&1000u64.to_be_bytes());
        save_string(&mut expected, &key);
        let mut lp = listpack::Listpack::new();
        lp.append_integer(1); // valid entries
        lp.append_integer(0); // deleted
        lp.append_integer(1); // master num fields
        lp.append_bytes(b"f");
        lp.append_integer(0); // master terminator
        lp.append_integer(STREAM_ITEM_FLAG_SAMEFIELDS);
        lp.append_integer(0); // ms delta
        lp.append_integer(0); // seq delta
        lp.append_bytes(b"v");
        lp.append_integer(4); // lp_count = 1 field + 3 fixed
        let lp = lp.into_vec();
        save_string(&mut expected, &lp);
        write_len(&mut expected, 1); // length
        write_len(&mut expected, 1000); // last_id.ms
        write_len(&mut expected, 0); // last_id.seq
        write_len(&mut expected, 1000); // first_id.ms
        write_len(&mut expected, 0); // first_id.seq
        write_len(&mut expected, 0); // max_deleted ms
        write_len(&mut expected, 0); // max_deleted seq
        write_len(&mut expected, 1); // entries_added
        write_len(&mut expected, 0); // consumer groups
        expected.extend_from_slice(&9u16.to_le_bytes());
        expected.extend_from_slice(&crc64::crc64(&expected).to_le_bytes());
        assert_eq!(dump, expected);
    }

    #[test]
    fn stream_two_entries_shared_node() {
        // Two entries sharing fields pack into a single listpack node keyed by
        // the first entry's ID, with the second delta-encoded.
        let mut s = Stream::new();
        let id1 = StreamId { ms: 1000, seq: 0 };
        let id2 = StreamId { ms: 1000, seq: 1 };
        s.append(id1, vec![(cs("f"), cs("v"))]);
        s.append(id2, vec![(cs("f"), cs("w"))]);
        let dump = dump_value(&PrimeValue::Stream(s));

        let mut expected = vec![RDB_TYPE_STREAM_LISTPACKS_3];
        write_len(&mut expected, 1); // single node
        let mut key = [0u8; 16];
        key[..8].copy_from_slice(&1000u64.to_be_bytes());
        save_string(&mut expected, &key);
        let mut lp = listpack::Listpack::new();
        lp.append_integer(2); // valid entries
        lp.append_integer(0); // deleted
        lp.append_integer(1); // master num fields
        lp.append_bytes(b"f");
        lp.append_integer(0);
        lp.append_integer(STREAM_ITEM_FLAG_SAMEFIELDS); // entry 1
        lp.append_integer(0);
        lp.append_integer(0);
        lp.append_bytes(b"v");
        lp.append_integer(4);
        lp.append_integer(STREAM_ITEM_FLAG_SAMEFIELDS); // entry 2
        lp.append_integer(0); // ms delta
        lp.append_integer(1); // seq delta
        lp.append_bytes(b"w");
        lp.append_integer(4);
        let lp = lp.into_vec();
        save_string(&mut expected, &lp);
        write_len(&mut expected, 2); // length
        write_len(&mut expected, 1000); // last_id.ms
        write_len(&mut expected, 1); // last_id.seq
        write_len(&mut expected, 1000); // first_id.ms
        write_len(&mut expected, 0); // first_id.seq
        write_len(&mut expected, 0);
        write_len(&mut expected, 0);
        write_len(&mut expected, 2); // entries_added
        write_len(&mut expected, 0); // groups
        expected.extend_from_slice(&9u16.to_le_bytes());
        expected.extend_from_slice(&crc64::crc64(&expected).to_le_bytes());
        assert_eq!(dump, expected);
    }

    #[test]
    fn stream_with_consumer_group() {
        let mut s = Stream::new();
        let id = StreamId { ms: 1, seq: 0 };
        s.append(id, vec![(cs("f"), cs("v"))]);
        let gname = CompactString::from("g1");
        let cname = CompactString::from("c1");
        assert!(
            s.create_group(gname.clone(), StreamId::MIN, false, false)
                .is_ok()
        );
        let now_ms = 10_000_000u64;
        let ids = s
            .read_group(
                &gname,
                &cname,
                StreamId { ms: 0, seq: 1 },
                None,
                false,
                now_ms,
            )
            .unwrap();
        assert_eq!(ids.len(), 1);
        let dump = dump_value(&PrimeValue::Stream(s));
        // Group section: name, last_id, entries_read, PEL with delivery info,
        // then the consumer.
        assert_eq!(dump[0], RDB_TYPE_STREAM_LISTPACKS_3);
        let now_ms = 10_000_000u64;
        let id_bytes = {
            let mut b = [0u8; 16];
            b[..8].copy_from_slice(&1u64.to_be_bytes());
            b
        };
        let mut expected = vec![RDB_TYPE_STREAM_LISTPACKS_3];
        write_len(&mut expected, 1); // node count
        save_string(&mut expected, &id_bytes); // node key: 1.0
        let mut lp = listpack::Listpack::new();
        lp.append_integer(1); // valid entries
        lp.append_integer(0); // deleted
        lp.append_integer(1); // master num fields
        lp.append_bytes(b"f");
        lp.append_integer(0); // master terminator
        lp.append_integer(STREAM_ITEM_FLAG_SAMEFIELDS);
        lp.append_integer(0); // ms delta
        lp.append_integer(0); // seq delta
        lp.append_bytes(b"v");
        lp.append_integer(4); // lp_count
        save_string(&mut expected, &lp.into_vec());
        write_len(&mut expected, 1); // length
        write_len(&mut expected, 1); // last_id.ms
        write_len(&mut expected, 0); // last_id.seq
        write_len(&mut expected, 1); // first_id.ms
        write_len(&mut expected, 0); // first_id.seq
        write_len(&mut expected, 0); // max_deleted ms
        write_len(&mut expected, 0); // max_deleted seq
        write_len(&mut expected, 1); // entries_added
        // --- consumer group section ---
        write_len(&mut expected, 1); // num groups
        save_string(&mut expected, b"g1");
        write_len(&mut expected, 1); // last_delivered.ms
        write_len(&mut expected, 0); // last_delivered.seq
        write_len(&mut expected, 1); // entries_read
        write_len(&mut expected, 1); // global PEL count
        expected.extend_from_slice(&id_bytes); // PEL entry ID 1.0
        expected.extend_from_slice(&now_ms.to_le_bytes()); // delivery_time
        write_len(&mut expected, 1); // delivery_count
        write_len(&mut expected, 1); // consumers
        save_string(&mut expected, b"c1");
        expected.extend_from_slice(&now_ms.to_le_bytes()); // seen_time
        expected.extend_from_slice(&now_ms.to_le_bytes()); // active_time
        write_len(&mut expected, 1); // consumer PEL count
        expected.extend_from_slice(&id_bytes); // consumer PEL entry ID 1.0
        expected.extend_from_slice(&9u16.to_le_bytes());
        expected.extend_from_slice(&crc64::crc64(&expected).to_le_bytes());
        assert_eq!(dump, expected);
    }

    #[test]
    fn dump_roundtrip_consistency() {
        // DUMP output is stable for a fixed value: re-dumping yields identical bytes.
        let mut h = Hash::new();
        h.set(cs("k1"), cs("v1"));
        h.set(cs("k2"), cs("v2"));
        let a = dump_value(&PrimeValue::Hash(h.clone()));
        let b = dump_value(&PrimeValue::Hash(h));
        assert_eq!(a, b);
    }

    fn now_ms() -> u64 {
        1_800_000_000_000
    }

    fn restore_value_ok(payload: &[u8]) -> PrimeValue {
        match restore_value(payload, now_ms()) {
            Ok(RestoreOutcome::Value(v)) => v,
            other => panic!("expected Value, got {:?}", other),
        }
    }

    /// Hand-craft a payload with a given version (used for version rejection).
    fn with_version(payload: &[u8], version: u16) -> Vec<u8> {
        let mut p = payload[..payload.len() - 10].to_vec();
        p.extend_from_slice(&version.to_le_bytes());
        p.extend_from_slice(&crc64::crc64(&p).to_le_bytes());
        p
    }

    // ---- Reference vectors from `GenericFamilyTest::Restore` ----

    #[test]
    fn restore_redis_string_vector() {
        let payload: Vec<u8> = vec![
            0x00, 0xc1, 0xd2, 0x04, 0x09, 0x00, 0xd0, 0x75, 0x59, 0x6d, 0x10, 0x04, 0x3f, 0x5c,
        ];
        match restore_value(&payload, now_ms()) {
            Ok(RestoreOutcome::Value(PrimeValue::Str(s))) => assert_eq!(s.as_bytes(), b"1234"),
            other => panic!("unexpected {:?}", other),
        }
        // Re-dumping yields the exact same bytes (int16 encoding round-trips).
        let v = restore_value_ok(&payload);
        assert_eq!(dump_value(&v), payload);
    }

    #[test]
    fn restore_set_listpack_vector() {
        let payload: Vec<u8> = vec![
            0x14, 0x0d, 0x0d, 0x00, 0x00, 0x00, 0x01, 0x00, 0x84, 0x61, 0x63, 0x6d, 0x65, 0x05,
            0xff, 0x0b, 0x00, 0xc1, 0x37, 0x5c, 0xe5, 0xe2, 0xc0, 0xdd, 0x27,
        ];
        match restore_value(&payload, now_ms()) {
            Ok(RestoreOutcome::Value(PrimeValue::Set(s))) => {
                assert_eq!(s.len(), 1);
                assert!(s.contains(b"acme"));
            }
            other => panic!("unexpected {:?}", other),
        }
    }

    #[test]
    fn restore_zset_listpack_vector() {
        let payload: Vec<u8> = vec![
            0x11, 0x0f, 0x0f, 0x00, 0x00, 0x00, 0x02, 0x00, 0x84, 0x65, 0x6c, 0x6f, 0x6e, 0x05,
            0x01, 0x01, 0xff, 0x0b, 0x00, 0xc8, 0x01, 0x2c, 0xad, 0xd9, 0xa3, 0x99, 0x5e,
        ];
        match restore_value(&payload, now_ms()) {
            Ok(RestoreOutcome::Value(PrimeValue::ZSet(z))) => {
                assert_eq!(z.len(), 1);
                assert_eq!(z.iter().collect::<Vec<_>>(), vec![(cs("elon"), 1.0)]);
            }
            other => panic!("unexpected {:?}", other),
        }
    }

    // ---- Corrupt / OOB reference payloads (`RestoreOob*`) ----

    #[test]
    fn restore_rejects_corrupt_zset_type() {
        // Valid ZSET_LISTPACK payload whose type byte was flipped to 0x12 and
        // CRC recomputed: must be rejected.
        let payload: Vec<u8> = vec![
            0x12, 0x0f, 0x0f, 0x00, 0x00, 0x00, 0x02, 0x00, 0x84, 0x65, 0x6c, 0x6f, 0x6e, 0x05,
            0x01, 0x01, 0xff, 0x0b, 0x00, 0x4e, 0xa3, 0x4c, 0x89, 0xc4, 0x8b, 0xd9, 0xe4,
        ];
        assert!(matches!(
            restore_value(&payload, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_oob_set_listpack() {
        let payload: Vec<u8> = vec![
            0x14, 0x0c, 0x0c, 0x00, 0x00, 0x00, 0x01, 0x00, 0xf0, 0xff, 0xff, 0xff, 0x7f, 0xff,
            0x0b, 0x00, 0xdf, 0x34, 0x52, 0xe8, 0xed, 0x1f, 0xfe, 0x61,
        ];
        assert!(matches!(
            restore_value(&payload, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_oob_hash_listpack() {
        let payload: Vec<u8> = vec![
            0x10, 0x0c, 0x0c, 0x00, 0x00, 0x00, 0x02, 0x00, 0xf0, 0xff, 0xff, 0xff, 0x7f, 0xff,
            0x0b, 0x00, 0xed, 0x55, 0x88, 0x24, 0xae, 0xbc, 0xbd, 0xa0,
        ];
        assert!(matches!(
            restore_value(&payload, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_oob_zset_listpack() {
        let payload: Vec<u8> = vec![
            0x11, 0x0c, 0x0c, 0x00, 0x00, 0x00, 0x02, 0x00, 0xf0, 0xff, 0xff, 0xff, 0x7f, 0xff,
            0x0b, 0x00, 0xc1, 0xf6, 0xd5, 0x74, 0xd3, 0x02, 0x6a, 0x79,
        ];
        assert!(matches!(
            restore_value(&payload, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_oob_list_quicklist() {
        let payload: Vec<u8> = vec![
            0x12, 0x01, 0x02, 0x0c, 0x0c, 0x00, 0x00, 0x00, 0x01, 0x00, 0xf0, 0xff, 0xff, 0xff,
            0x7f, 0xff, 0x0b, 0x00, 0xc2, 0x3d, 0x24, 0x8b, 0xeb, 0x8c, 0x05, 0x25,
        ];
        assert!(matches!(
            restore_value(&payload, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_oob_stream_listpack() {
        let payload: Vec<u8> = vec![
            0x0f, 0x01, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x1d, 0x1d, 0x00, 0x00, 0x00, 0x09, 0x00, 0x01, 0x01,
            0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x01, 0x01,
            0xf0, 0xff, 0xff, 0xff, 0x7f, 0x05, 0xff, 0x01, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x22,
            0x31, 0x24, 0xa4, 0x6a, 0x6d, 0x9d, 0x7f,
        ];
        assert!(matches!(
            restore_value(&payload, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    // ---- Footer / length / encoding corruption ----

    #[test]
    fn restore_rejects_short_payload() {
        assert!(matches!(
            restore_value(&[0x00, 0x01], now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
        assert!(matches!(
            restore_value(&[0x00; 10], now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_rejects_bad_version() {
        let payload = dump_value(&PrimeValue::Str(cs("19")));
        assert!(matches!(
            restore_value(&with_version(&payload, 13), now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_rejects_bad_crc() {
        let mut payload = dump_value(&PrimeValue::Str(cs("19")));
        payload[1] ^= 0xff; // corrupt the value byte; footer now mismatches
        assert!(matches!(
            restore_value(&payload, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_rejects_unknown_type() {
        let mut p = vec![0x7e];
        p.extend_from_slice(&9u16.to_le_bytes());
        p.extend_from_slice(&crc64::crc64(&p).to_le_bytes());
        assert!(matches!(
            restore_value(&p, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));

        // Redis 8's `RDB_TYPE_STREAM_LISTPACKS_5` (27) on a real stream payload
        // must be rejected after the CRC validates, not silently mis-decoded.
        let mut s = Stream::new();
        s.append(StreamId { ms: 1, seq: 0 }, vec![(cs("f"), cs("v"))]);
        let mut payload = dump_value(&PrimeValue::Stream(s));
        payload[0] = 0x1b;
        let len = payload.len();
        let crc = crc64::crc64(&payload[..len - 8]);
        payload[len - 8..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            restore_value(&payload, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_rejects_reserved_length() {
        // 0x82 is a reserved length byte -> illegal byte sequence.
        let mut p = vec![0x00, 0x82];
        p.extend_from_slice(&9u16.to_le_bytes());
        p.extend_from_slice(&crc64::crc64(&p).to_le_bytes());
        assert!(matches!(
            restore_value(&p, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_rejects_huge_string_length() {
        // RDB_32BITLEN string length 0x7fffffff cannot be satisfied.
        let mut p = vec![0x00, 0x80, 0x7f, 0xff, 0xff, 0xff];
        p.extend_from_slice(&9u16.to_le_bytes());
        p.extend_from_slice(&crc64::crc64(&p).to_le_bytes());
        assert!(matches!(
            restore_value(&p, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_rejects_unknown_string_encoding() {
        // 0xc4 is an encoded value outside int8/16/32/LZF.
        let mut p = vec![0x00, 0xc4];
        p.extend_from_slice(&9u16.to_le_bytes());
        p.extend_from_slice(&crc64::crc64(&p).to_le_bytes());
        assert!(matches!(
            restore_value(&p, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_rejects_invalid_lzf() {
        // ulen (0) <= clen (1) is rejected.
        let mut p = vec![0x00, 0xc3, 0x01, 0x00, 0x05];
        p.extend_from_slice(&9u16.to_le_bytes());
        p.extend_from_slice(&crc64::crc64(&p).to_le_bytes());
        assert!(matches!(
            restore_value(&p, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    // ---- Integer / LZF string decoding ----

    #[test]
    fn restore_integer_encodings() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"\xc0\x05", b"5"),
            (b"\xc0\xff", b"-1"), // int8 -1
            (b"\xc1\xd2\x04", b"1234"),
            (b"\xc1\x00\x80", b"-32768"),
            (b"\xc2\x78\x56\x34\x12", b"305419896"),
        ];
        for (body, expected) in cases {
            let mut p = vec![0x00];
            p.extend_from_slice(body);
            p.extend_from_slice(&9u16.to_le_bytes());
            p.extend_from_slice(&crc64::crc64(&p).to_le_bytes());
            match restore_value(&p, now_ms()) {
                Ok(RestoreOutcome::Value(PrimeValue::Str(s))) => {
                    assert_eq!(s.as_bytes(), *expected)
                }
                other => panic!(
                    "expected Str({}), got {:?}",
                    String::from_utf8_lossy(expected),
                    other
                ),
            }
        }
    }

    #[test]
    fn restore_lzf_string() {
        let data = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_vec();
        let comp = lzf::compress(&data).unwrap();
        let mut p = vec![0x00, 0xc3];
        write_len(&mut p, comp.len() as u64);
        write_len(&mut p, data.len() as u64);
        p.extend_from_slice(&comp);
        p.extend_from_slice(&9u16.to_le_bytes());
        p.extend_from_slice(&crc64::crc64(&p).to_le_bytes());
        match restore_value(&p, now_ms()) {
            Ok(RestoreOutcome::Value(PrimeValue::Str(s))) => assert_eq!(s.as_bytes(), data),
            other => panic!("unexpected {:?}", other),
        }
    }

    // ---- Duplicate / integrity rejection ----

    #[test]
    fn restore_rejects_duplicate_set_member() {
        let mut p = vec![RDB_TYPE_SET, 2];
        save_string(&mut p, b"a");
        save_string(&mut p, b"a");
        p.extend_from_slice(&9u16.to_le_bytes());
        p.extend_from_slice(&crc64::crc64(&p).to_le_bytes());
        assert!(matches!(
            restore_value(&p, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_rejects_duplicate_hash_field() {
        let mut p = vec![RDB_TYPE_HASH, 2];
        save_string(&mut p, b"f");
        save_string(&mut p, b"v1");
        save_string(&mut p, b"f");
        save_string(&mut p, b"v2");
        p.extend_from_slice(&9u16.to_le_bytes());
        p.extend_from_slice(&crc64::crc64(&p).to_le_bytes());
        assert!(matches!(
            restore_value(&p, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_rejects_duplicate_zset_member() {
        let mut p = vec![RDB_TYPE_ZSET_2, 2];
        save_string(&mut p, b"m");
        p.extend_from_slice(&1.0f64.to_bits().to_le_bytes());
        save_string(&mut p, b"m");
        p.extend_from_slice(&2.0f64.to_bits().to_le_bytes());
        p.extend_from_slice(&9u16.to_le_bytes());
        p.extend_from_slice(&crc64::crc64(&p).to_le_bytes());
        assert!(matches!(
            restore_value(&p, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_rejects_duplicate_listpack_set_member() {
        let mut lp = listpack::Listpack::new();
        lp.append_bytes(b"a");
        lp.append_bytes(b"a");
        let mut p = vec![RDB_TYPE_SET_LISTPACK];
        save_string(&mut p, &lp.into_vec());
        p.extend_from_slice(&9u16.to_le_bytes());
        p.extend_from_slice(&crc64::crc64(&p).to_le_bytes());
        assert!(matches!(
            restore_value(&p, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    #[test]
    fn restore_rejects_bad_intset() {
        // An intset blob whose declared count doesn't match its size.
        let mut blob = intset::build([1i64, 2, 3]);
        blob.truncate(blob.len() - 1);
        let mut p = vec![RDB_TYPE_SET_INTSET];
        save_string(&mut p, &blob);
        p.extend_from_slice(&9u16.to_le_bytes());
        p.extend_from_slice(&crc64::crc64(&p).to_le_bytes());
        assert!(matches!(
            restore_value(&p, now_ms()),
            Err(RestoreError::BadDataFormat)
        ));
    }

    // ---- Expiry handling ----

    #[test]
    fn restore_set_with_expiry_roundtrip() {
        let mut s = Set::new();
        let expire = now_ms() + 100_000;
        s.add_expirable(cs("m1"), expire, false);
        s.add(cs("m2"));
        let dump = dump_value(&PrimeValue::Set(s));
        assert_eq!(dump[0], RDB_TYPE_SET_WITH_EXPIRY);
        match restore_value(&dump, now_ms()) {
            Ok(RestoreOutcome::Value(PrimeValue::Set(restored))) => {
                assert_eq!(restored.len(), 2);
                assert!(restored.contains(b"m1"));
                assert!(restored.contains(b"m2"));
                assert_eq!(restored.member_expire_ms(b"m1"), Some(expire));
                assert_eq!(restored.member_expire_ms(b"m2"), None);
                // Re-dump yields identical bytes.
                assert_eq!(dump_value(&PrimeValue::Set(restored)), dump);
            }
            other => panic!("unexpected {:?}", other),
        }
    }

    #[test]
    fn restore_all_members_expired_is_expired() {
        let mut s = Set::new();
        s.add_expirable(cs("gone"), now_ms() - 1000, false);
        let dump = dump_value(&PrimeValue::Set(s));
        assert!(matches!(
            restore_value(&dump, now_ms()),
            Ok(RestoreOutcome::Expired)
        ));
    }

    #[test]
    fn restore_partially_expired_set_keeps_survivors() {
        let mut s = Set::new();
        s.add_expirable(cs("gone"), now_ms() - 1000, false);
        s.add_expirable(cs("alive"), now_ms() + 100_000, false);
        let dump = dump_value(&PrimeValue::Set(s));
        match restore_value(&dump, now_ms()) {
            Ok(RestoreOutcome::Value(PrimeValue::Set(restored))) => {
                assert_eq!(restored.len(), 1);
                assert!(restored.contains(b"alive"));
            }
            other => panic!("unexpected {:?}", other),
        }
    }

    #[test]
    fn restore_hash_with_expiry_roundtrip() {
        let mut h = Hash::new();
        let expire = now_ms() + 100_000;
        h.add_expirable(cs("f1"), cs("v1"), Some(expire), false);
        h.set(cs("f2"), cs("v2"));
        let dump = dump_value(&PrimeValue::Hash(h));
        assert_eq!(dump[0], RDB_TYPE_HASH_WITH_EXPIRY);
        match restore_value(&dump, now_ms()) {
            Ok(RestoreOutcome::Value(PrimeValue::Hash(restored))) => {
                assert_eq!(restored.len(), 2);
                assert_eq!(
                    restored.get(b"f1").map(|v| v.as_bytes()),
                    Some(b"v1".as_slice())
                );
                assert_eq!(restored.field_expire_ms(b"f1"), Some(expire));
                assert_eq!(restored.field_expire_ms(b"f2"), None);
                assert_eq!(dump_value(&PrimeValue::Hash(restored)), dump);
            }
            other => panic!("unexpected {:?}", other),
        }
    }

    #[test]
    fn restore_all_hash_fields_expired_is_expired() {
        let mut h = Hash::new();
        h.add_expirable(cs("gone"), cs("v"), Some(now_ms() - 1000), false);
        let dump = dump_value(&PrimeValue::Hash(h));
        assert!(matches!(
            restore_value(&dump, now_ms()),
            Ok(RestoreOutcome::Expired)
        ));
    }

    // ---- Round trips ----

    #[test]
    fn restore_roundtrip_string() {
        for s in ["19", "1234", "hello world", "0", "-9223372036854775808"] {
            let dump = dump_value(&PrimeValue::Str(cs(s)));
            assert_eq!(dump_value(&restore_value_ok(&dump)), dump, "string {:?}", s);
        }
    }

    #[test]
    fn restore_roundtrip_intset_set() {
        let mut s = Set::new();
        for m in ["1", "2", "3", "-100", "70000"] {
            s.add(cs(m));
        }
        let dump = dump_value(&PrimeValue::Set(s));
        assert_eq!(dump[0], RDB_TYPE_SET_INTSET);
        match restore_value_ok(&dump) {
            PrimeValue::Set(restored) => {
                assert_eq!(restored.len(), 5);
                assert!(restored.contains(b"-100"));
                assert!(restored.contains(b"70000"));
            }
            other => panic!("unexpected {:?}", other),
        }
        assert_eq!(dump_value(&restore_value_ok(&dump)), dump);
    }

    #[test]
    fn restore_roundtrip_strmap_set() {
        let mut s = Set::new();
        s.add(cs("a"));
        s.add(cs("bb"));
        s.add(cs("ccc"));
        let dump = dump_value(&PrimeValue::Set(s));
        assert_eq!(dump[0], RDB_TYPE_SET);
        assert_eq!(dump_value(&restore_value_ok(&dump)), dump);
    }

    #[test]
    fn restore_roundtrip_hash_listpack() {
        let mut h = Hash::new();
        h.set(cs("19"), cs("1234"));
        let dump = dump_value(&PrimeValue::Hash(h));
        assert_eq!(dump[0], RDB_TYPE_HASH_LISTPACK);
        assert_eq!(dump_value(&restore_value_ok(&dump)), dump);
    }

    #[test]
    fn restore_roundtrip_large_hash() {
        // Large hashes use an open-addressing table whose iteration order
        // depends on insertion order, so re-dumping a restored hash is not
        // byte-identical to the original DUMP (same as the reference server).
        // Verify that every field/value round-trips instead.
        let mut h = Hash::new();
        for i in 0..200 {
            h.set(
                CompactString::from_bytes(format!("f{}", i).as_bytes()),
                CompactString::from_bytes(format!("v{}", i).as_bytes()),
            );
        }
        let dump = dump_value(&PrimeValue::Hash(h.clone()));
        assert_eq!(dump[0], RDB_TYPE_HASH);
        let restored = match restore_value(&dump, now_ms()) {
            Ok(RestoreOutcome::Value(PrimeValue::Hash(h))) => h,
            other => panic!("unexpected {:?}", other),
        };
        assert_eq!(restored.len(), 200);
        for i in 0..200 {
            let f = CompactString::from_bytes(format!("f{}", i).as_bytes());
            assert_eq!(
                restored.get(f.as_bytes()).map(|v| v.as_bytes()),
                h.get(f.as_bytes()).map(|v| v.as_bytes())
            );
        }
    }

    #[test]
    fn restore_roundtrip_zset() {
        let mut z = ZSet::new();
        z.insert(cs("a"), 1.5);
        z.insert(cs("b"), -2.0);
        z.insert(cs("c"), f64::INFINITY);
        let dump = dump_value(&PrimeValue::ZSet(z));
        assert_eq!(dump[0], RDB_TYPE_ZSET_2);
        assert_eq!(dump_value(&restore_value_ok(&dump)), dump);
    }

    #[test]
    fn restore_roundtrip_list() {
        let mut ql = QuickList::new();
        ql.push_back(ListItem::Int(20));
        ql.push_back(ListItem::Str(cs("hello")));
        ql.push_back(ListItem::Int(-5));
        let dump = dump_value(&PrimeValue::List(ql));
        assert_eq!(dump[0], RDB_TYPE_LIST_QUICKLIST_2);
        assert_eq!(dump_value(&restore_value_ok(&dump)), dump);
    }

    #[test]
    fn restore_roundtrip_stream() {
        let mut s = Stream::new();
        let id = StreamId { ms: 1000, seq: 0 };
        s.append(id, vec![(cs("f"), cs("v"))]);
        let gname = CompactString::from("g1");
        let cname = CompactString::from("c1");
        assert!(
            s.create_group(gname.clone(), StreamId::MIN, false, false)
                .is_ok()
        );
        s.read_group(
            &gname,
            &cname,
            StreamId { ms: 0, seq: 1 },
            None,
            false,
            now_ms(),
        )
        .unwrap();
        let dump = dump_value(&PrimeValue::Stream(s));
        let restored = restore_value_ok(&dump);
        assert_eq!(dump_value(&restored), dump);
        if let PrimeValue::Stream(st) = &restored {
            assert_eq!(st.len(), 1);
            assert_eq!(st.last_id, StreamId { ms: 1000, seq: 0 });
            assert_eq!(st.groups.len(), 1);
            let g = st.groups.get(&gname).unwrap();
            assert_eq!(g.pel.len(), 1);
            assert_eq!(g.consumers.len(), 1);
            assert_eq!(g.consumers.get(&cname).unwrap().pending, 1);
        }
    }

    #[test]
    fn restore_roundtrip_stream_two_nodes() {
        // Many entries force multiple listpack nodes.
        let mut s = Stream::new();
        for i in 0..200u64 {
            s.append(
                StreamId { ms: 1, seq: i },
                vec![(
                    cs("k"),
                    CompactString::from_bytes(format!("v{}", i).as_bytes()),
                )],
            );
        }
        let dump = dump_value(&PrimeValue::Stream(s));
        assert_eq!(dump_value(&restore_value_ok(&dump)), dump);
    }

    #[test]
    fn restore_roundtrip_all_types_are_deterministic() {
        // Re-restoring an already-restored value is stable.
        let mut s = Set::new();
        s.add(cs("a"));
        s.add(cs("b"));
        let payloads = vec![
            dump_value(&PrimeValue::Str(cs("hello"))),
            dump_value(&PrimeValue::List(QuickList::new())),
            dump_value(&PrimeValue::Set(s)),
            dump_value(&PrimeValue::Stream(Stream::new())),
        ];
        for p in payloads {
            let v = restore_value_ok(&p);
            let re = dump_value(&v);
            let v2 = restore_value_ok(&re);
            let re2 = dump_value(&v2);
            assert_eq!(re, re2);
        }
    }

    #[test]
    fn module_types_dump_restore_round_trip() {
        use crate::core::bloom::SBF;
        use crate::core::cms::Cms;
        use crate::core::cuckoo::{CuckooFilter, CuckooFilterOptions};
        use crate::core::topk::Topk;

        let mut sbf = SBF::new(32, 0.01, 2.0);
        sbf.add(b"a");
        sbf.add(b"b");
        let mut cms = Cms::new(100, 5);
        cms.incr_by(b"foo", 5);
        cms.incr_by(b"bar", 3);
        let mut cf = CuckooFilter::new(&CuckooFilterOptions {
            capacity: 1000,
            slots_per_bucket: 4,
            max_iterations: 10,
            expansion: 2,
        });
        cf.insert(CuckooFilter::hash(b"foo"));
        cf.insert(CuckooFilter::hash(b"foo"));
        cf.insert(CuckooFilter::hash(b"bar"));
        cf.delete(CuckooFilter::hash(b"bar"));
        let mut tk = Topk::new(5, 50, 7, 0.9);
        tk.incr_by(b"foo", 10);
        tk.incr_by(b"bar", 3);

        for pv in [
            PrimeValue::Sbf(sbf),
            PrimeValue::Cms(cms),
            PrimeValue::Cuckoo(cf),
            PrimeValue::Topk(tk),
        ] {
            let dump = dump_value(&pv);
            match restore_value(&dump, now_ms()) {
                Ok(RestoreOutcome::Value(v)) => {
                    assert_eq!(v.type_name(), pv.type_name());
                    assert_eq!(dump_value(&v), dump);
                }
                other => panic!("expected Value, got {:?}", other),
            }
        }

        // Truncated / garbage payloads are rejected as BadDataFormat.
        let cms = Cms::new(100, 5);
        let mut bad = dump_value(&PrimeValue::Cms(cms));
        bad.truncate(bad.len() - 1);
        assert!(matches!(restore_value(&bad, now_ms()), Err(RestoreError::BadDataFormat)));
    }
}
