//! Master-side replication: `ReplicationManager` (the port's analogue of
//! `dflycmd.cc`'s `DflyCmd`), the sync-session bookkeeping behind
//! `REPLCONF CAPA dragonfly` / `DFLY FLOW` / `DFLY SYNC` / `DFLY STARTSTABLE`,
//! and the full-sync RDB stream construction.
//!
//! The journal itself lives per shard (`crate::server::journal`); this module
//! owns the replica sessions and each flow's connection ids.

use std::collections::HashMap;
use std::sync::mpsc;

use crate::error::RespValue;
use crate::server::journal::JournalItem;

/// `DflyVersion::CURRENT_VER`. The protocol version reported in the
/// `REPLCONF CAPA dragonfly` reply; used by the replica to negotiate
/// `DFLY FLOW` partial-sync arguments.
pub const CURRENT_VER: u32 = 8;

/// A replica session state machine (`ReplicaInfo::SyncState`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    Preparation,
    FullSync,
    StableSync,
    Cancelled,
}

/// One flow connection of a replica session: the socket that carries the RDB
/// stream and the stable-sync journal records for a single shard.
#[derive(Debug)]
pub struct Flow {
    pub flow_id: usize,
    /// The event-loop connection id of this flow.
    pub conn_id: u64,
    /// Random hex token that closes the full-sync RDB stream (`GetRandomHex`).
    pub eof_token: String,
    /// The shard's LSN at the full-sync cut: the next record the replica needs
    /// from stable sync. Populated by `DFLY SYNC`.
    pub start_lsn: u64,
    /// Set when partial sync was negotiated by `DFLY FLOW`.
    pub start_partial_sync_at: Option<u64>,
    /// Last `REPLCONF ACK`ed LSN, for lag accounting.
    pub last_acked_lsn: u64,
}

/// A replica session (`ReplicaInfo`), created by `REPLCONF CAPA dragonfly`.
#[derive(Debug)]
pub struct Replica {
    pub sync_id: u32,
    pub address: String,
    pub port: u32,
    pub state: SyncState,
    pub flows: Vec<Flow>,
}

/// The master's replica registry and the shared master identity.
pub struct ReplicationManager {
    next_sync_id: u32,
    pub replicas: HashMap<u32, Replica>,
    /// The master replication id, reported to replicas (`master_replid_`).
    pub master_replid: String,
    /// The lineage id in the `capa dragonfly` reply (fixed, like a fresh master).
    pub lineage_id: String,
}

impl ReplicationManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_sync_id: 1,
            replicas: HashMap::new(),
            master_replid: random_hex(40),
            lineage_id: "0".repeat(40),
        }
    }

    #[must_use]
    pub fn get(&self, sync_id: u32) -> Option<&Replica> {
        self.replicas.get(&sync_id)
    }

    #[must_use]
    pub fn get_mut(&mut self, sync_id: u32) -> Option<&mut Replica> {
        self.replicas.get_mut(&sync_id)
    }

    /// `DflyCmd::CreateSyncSession`: allocate a sync id and one `Flow` per
    /// shard, in `PREPARATION`.
    pub fn create_sync_session(&mut self, address: String, port: u32, num_shards: usize) -> u32 {
        let sync_id = self.next_sync_id;
        self.next_sync_id += 1;
        let flows = (0..num_shards)
            .map(|flow_id| Flow {
                flow_id,
                conn_id: 0,
                eof_token: String::new(),
                start_lsn: 0,
                start_partial_sync_at: None,
                last_acked_lsn: 0,
            })
            .collect();
        self.replicas.insert(
            sync_id,
            Replica {
                sync_id,
                address,
                port,
                state: SyncState::Preparation,
                flows,
            },
        );
        sync_id
    }

    /// Parse `"SYNC<digits>"` into a sync id (`ToSyncId`).
    #[must_use]
    pub fn parse_sync_id(id: &str) -> Option<u32> {
        id.strip_prefix("SYNC")?.parse().ok()
    }
}

impl Default for ReplicationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// What a [`ReplChunk`] carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkKind {
    /// A stable-sync journal record (post-`DFLY STARTSTABLE`).
    StableSync,
    /// A full-sync snapshot chunk. `journal_lsn` is `Some` only on the final
    /// chunk, carrying the snapshot cut LSN (the `JOURNAL_OFFSET` written into
    /// the stream tail); interim chunks are `None`.
    FullSync { journal_lsn: Option<u64> },
}

/// A journal record or full-sync chunk routed from a shard thread to the flow
/// connection that owns it, through the shared `repl_tx` bus drained by the IO
/// thread (`drain_repl`).
pub struct ReplChunk {
    pub sync_id: u32,
    pub flow_id: usize,
    pub bytes: Vec<u8>,
    pub kind: ChunkKind,
}

/// A full-sync chunk bus: the shared `repl_tx` plus a kqueue wakeup pipe, so
/// the IO thread wakes as soon as a chunk is ready without polling. Mirrors
/// `ReplyBus`; stable-sync records keep flowing through the raw `repl_tx` the
/// way they did before.
#[derive(Debug, Clone)]
pub struct FullSyncBus {
    tx: mpsc::Sender<ReplChunk>,
    wake_w: libc::c_int,
}

impl FullSyncBus {
    #[must_use]
    pub fn new(tx: mpsc::Sender<ReplChunk>, wake_w: libc::c_int) -> Self {
        FullSyncBus { tx, wake_w }
    }

    pub fn send(&self, chunk: ReplChunk) {
        if self.tx.send(chunk).is_err() {
            return;
        }
        let one = [1u8];
        unsafe {
            libc::write(self.wake_w, one.as_ptr().cast::<libc::c_void>(), 1);
        }
    }
}

/// Route a stable-sync journal item to the flow connection of `(sync_id,
/// flow_id)`, through the shared `repl_tx` bus drained by the IO thread.
#[must_use]
pub fn flow_consumer(
    repl_tx: mpsc::Sender<ReplChunk>,
    sync_id: u32,
    flow_id: usize,
) -> crate::server::journal::Consumer {
    Box::new(move |item: &JournalItem| {
        let chunk = ReplChunk {
            sync_id,
            flow_id,
            bytes: item.data.clone(),
            kind: ChunkKind::StableSync,
        };
        let _ = repl_tx.send(chunk);
    })
}

/// The `REPLCONF CAPA dragonfly` reply: `[master_replid, sync_id, flow_count,
/// version, lineage_id]`.
#[must_use]
pub fn capa_dragonfly_reply(
    repl: &ReplicationManager,
    sync_id: u32,
    flow_count: usize,
) -> RespValue {
    RespValue::Array(vec![
        RespValue::bulk(repl.master_replid.as_str()),
        RespValue::bulk(format!("SYNC{sync_id}")),
        RespValue::Integer(flow_count as i64),
        RespValue::Integer(i64::from(CURRENT_VER)),
        RespValue::bulk(repl.lineage_id.as_str()),
    ])
}

/// A fresh random lowercase hex string (like `GetRandomHex`): `bytes` hex chars.
#[must_use]
pub fn random_hex(bytes: usize) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEED: AtomicU64 = AtomicU64::new(0x9e3779b97f4a7c15);
    let mut seed = SEED.fetch_add(0x100000001b3, Ordering::Relaxed) as u64;
    let mut out = String::with_capacity(bytes);
    for _ in 0..(bytes / 2) {
        // xorshift64star
        seed ^= seed >> 12;
        seed ^= seed << 25;
        seed ^= seed >> 27;
        let w = (seed.wrapping_mul(0x2545f4914f6cdd1d)) & 0xff;
        out.push_str(&format!("{w:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_id_parsing() {
        assert_eq!(ReplicationManager::parse_sync_id("SYNC7"), Some(7));
        assert_eq!(ReplicationManager::parse_sync_id("SYNC0"), Some(0));
        assert_eq!(ReplicationManager::parse_sync_id("SYNC"), None);
        assert_eq!(ReplicationManager::parse_sync_id("SYNCx"), None);
        assert_eq!(ReplicationManager::parse_sync_id("foo"), None);
    }

    #[test]
    fn create_session_allocates_flows() {
        let mut r = ReplicationManager::new();
        let sid = r.create_sync_session("127.0.0.1".into(), 6380, 3);
        let rep = r.get(sid).unwrap();
        assert_eq!(rep.state, SyncState::Preparation);
        assert_eq!(rep.flows.len(), 3);
        assert_eq!(rep.flows[0].flow_id, 0);
        let sid2 = r.create_sync_session("127.0.0.1".into(), 6380, 3);
        assert_ne!(sid, sid2);
    }

    #[test]
    fn capa_reply_shape() {
        let mut r = ReplicationManager::new();
        let sid = r.create_sync_session("127.0.0.1".into(), 6380, 2);
        let v = capa_dragonfly_reply(&r, sid, 2);
        match v {
            RespValue::Array(items) => {
                assert_eq!(items.len(), 5);
                assert_eq!(items[1], RespValue::bulk("SYNC1"));
                assert_eq!(items[2], RespValue::Integer(2));
                assert_eq!(items[3], RespValue::Integer(i64::from(CURRENT_VER)));
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn replid_is_40_hex() {
        let mut r1 = ReplicationManager::new();
        let mut r2 = ReplicationManager::new();
        assert_eq!(r1.master_replid.len(), 40);
        assert_eq!(r1.lineage_id.len(), 40);
        assert_ne!(r1.master_replid, r2.master_replid);
        assert!(r1.master_replid.bytes().all(|b| b.is_ascii_hexdigit()));
    }
}
