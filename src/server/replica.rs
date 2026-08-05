//! Replica side of replication (`replica.cc`).
//!
//! The replica runs on its own dedicated thread(s), completely outside the
//! event loop: one control connection owns the handshake and session, and one
//! flow connection per master shard streams the shard's full-sync RDB snapshot
//! and then its journal records. Applied records are tracked with a shard-local
//! LSN and acked to the master with periodic `REPLCONF ACK <lsn>`.
//!
//! A disconnect restarts the whole session with a partial sync: each flow
//! resumes from the last applied LSN (`DFLY FLOW ... <lsn>`), which the master
//! resolves to FULL or PARTIAL against its journal ring.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::core::rdb::{self, Rd, RestoreError};
use crate::error::RespValue;
use crate::server::journal::{OP_COMMAND, OP_EXPIRED, OP_LSN, OP_PING, OP_SELECT};
use crate::server::ShardMsg;

/// Read-poll granularity: blocking reads wake up this often to re-check the
/// stop/abort flags, and ACK threads poll at this rate.
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Write timeout on replica sockets (master stalls are treated as fatal).
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// Upper bound for a single handshake round-trip (`DFLY FLOW` reply etc).
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
/// Delay between reconnect attempts after a failed session.
const CONNECT_BACKOFF: Duration = Duration::from_millis(500);
/// ACK cadence (`replication_acks_interval`).
const ACK_INTERVAL: Duration = Duration::from_secs(1);

/// The replica's lifecycle, surfaced through `ROLE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaPhase {
    /// Not a replica (a standalone master, or after `REPLICAOF NO ONE`).
    Master,
    /// Trying to connect / negotiating the handshake.
    Connecting,
    /// Flows are loading the full-sync RDB snapshot.
    FullSync,
    /// Journal records are being applied and acked.
    StableSync,
}

/// Status shared between the replica thread and the event loop (`ROLE`,
/// read-only gating).
#[derive(Debug, Clone)]
pub struct ReplicaStatus {
    pub phase: ReplicaPhase,
    pub master_host: String,
    pub master_port: u16,
    /// The session id assigned by the master (`SYNC<n>`).
    pub sync_id: String,
    /// Number of journal records applied this session.
    pub journal_rec_executed: u64,
    /// The most recent session error, if any.
    pub error: Option<String>,
}

impl Default for ReplicaStatus {
    fn default() -> Self {
        Self {
            phase: ReplicaPhase::Master,
            master_host: String::new(),
            master_port: 0,
            sync_id: String::new(),
            journal_rec_executed: 0,
            error: None,
        }
    }
}

/// Configuration handed to the replica thread (`ReplicaOf` in the reference).
#[derive(Clone)]
pub struct ReplicaConfig {
    pub host: String,
    pub port: u16,
    /// The replica's own listening port, reported via `REPLCONF listening-port`.
    pub listen_port: u16,
    pub num_shards: usize,
    pub shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    pub status: Arc<Mutex<ReplicaStatus>>,
    pub stop: Arc<AtomicBool>,
    /// Per-flow resume LSNs, kept across reconnect attempts so a session can
    /// continue with a partial sync after a dropped connection.
    pub lsn_cells: Arc<Vec<Arc<AtomicU64>>>,
}

/// The `ROLE` reply. A master reports `["master", []]`; a replica reports
/// `["slave", host, port, state, offset]` like Redis.
pub fn role_reply(status: &ReplicaStatus) -> RespValue {
    match status.phase {
        ReplicaPhase::Master => RespValue::Array(vec![
            RespValue::bulk("master"),
            RespValue::Array(vec![]),
        ]),
        _ => RespValue::Array(vec![
            RespValue::bulk("slave"),
            RespValue::bulk(status.master_host.as_str()),
            RespValue::Integer(i64::from(status.master_port)),
            RespValue::bulk(phase_string(status.phase)),
            RespValue::Integer(status.journal_rec_executed as i64),
        ]),
    }
}

/// `Replica::StateToString`: CONNECTING -> "connect", FULL_SYNC -> "sync",
/// STABLE_SYNC -> "connected".
fn phase_string(phase: ReplicaPhase) -> &'static str {
    match phase {
        ReplicaPhase::Master => "master",
        ReplicaPhase::Connecting => "connect",
        ReplicaPhase::FullSync => "sync",
        ReplicaPhase::StableSync => "connected",
    }
}

/// Replica-side errors.
#[derive(Debug)]
enum ReplicaError {
    Io(io::Error),
    Eof,
    Protocol(String),
    Timeout,
    /// The session was aborted (a peer flow failed or REPLICAOF NO ONE).
    Stopped,
    /// A shard channel send failed.
    Shard,
}

impl ReplicaError {
    fn into_string(self) -> String {
        match self {
            ReplicaError::Io(e) => format!("replica io error: {e}"),
            ReplicaError::Eof => "replica connection closed".into(),
            ReplicaError::Protocol(m) => m,
            ReplicaError::Timeout => "replica handshake timed out".into(),
            ReplicaError::Stopped => "replica stopped".into(),
            ReplicaError::Shard => "replica shard channel closed".into(),
        }
    }
}

fn proto(msg: impl Into<String>) -> ReplicaError {
    ReplicaError::Protocol(msg.into())
}

impl From<crate::core::rdb::RestoreError> for ReplicaError {
    fn from(_: crate::core::rdb::RestoreError) -> Self {
        proto("corrupt full-sync stream")
    }
}

/// A buffered reader over the replica's sockets. Reads block until data
/// arrives but wake up every `POLL_INTERVAL` to observe the `halt` flag, and
/// respect an optional deadline (handshake round-trips).
struct StreamReader {
    stream: TcpStream,
    buf: Vec<u8>,
    pos: usize,
    halt: Arc<AtomicBool>,
    deadline: Option<Instant>,
}

impl StreamReader {
    fn new(stream: TcpStream, halt: Arc<AtomicBool>) -> Self {
        Self {
            stream,
            buf: Vec::new(),
            pos: 0,
            halt,
            deadline: None,
        }
    }

    fn set_deadline(&mut self, d: Option<Duration>) {
        self.deadline = d.map(|d| Instant::now() + d);
    }

    fn fill(&mut self) -> Result<(), ReplicaError> {
        let mut tmp = [0u8; 65536];
        loop {
            match self.stream.read(&mut tmp) {
                Ok(0) => return Err(ReplicaError::Eof),
                Ok(n) => {
                    self.buf.extend_from_slice(&tmp[..n]);
                    return Ok(());
                }
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::TimedOut =>
                {
                    if self.halt.load(Ordering::Relaxed) {
                        return Err(ReplicaError::Stopped);
                    }
                    if self.deadline.is_some_and(|d| Instant::now() >= d) {
                        return Err(ReplicaError::Timeout);
                    }
                }
                Err(e) => return Err(ReplicaError::Io(e)),
            }
        }
    }

    fn ensure(&mut self, n: usize) -> Result<(), ReplicaError> {
        while self.buf.len() - self.pos < n {
            self.fill()?;
        }
        Ok(())
    }

    fn next_u8(&mut self) -> Result<u8, ReplicaError> {
        self.ensure(1)?;
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn take(&mut self, n: usize) -> Result<Vec<u8>, ReplicaError> {
        self.ensure(n)?;
        let s = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n;
        Ok(s)
    }

    fn skip(&mut self, n: usize) -> Result<(), ReplicaError> {
        self.ensure(n)?;
        self.pos += n;
        Ok(())
    }

    /// Consume bytes up to and including the next CRLF, as a string.
    fn read_line(&mut self) -> Result<String, ReplicaError> {
        loop {
            if let Some(rel) = self.buf[self.pos..]
                .windows(2)
                .position(|w| w == b"\r\n")
            {
                let line =
                    String::from_utf8_lossy(&self.buf[self.pos..self.pos + rel]).into_owned();
                self.pos += rel + 2;
                return Ok(line);
            }
            self.fill()?;
        }
    }

    /// Parse one RESP value (`ParseResult<RespValue>`).
    fn read_resp(&mut self) -> Result<RespValue, ReplicaError> {
        match self.next_u8()? {
            b'+' => Ok(RespValue::Simple(self.read_line()?)),
            b'-' => Ok(RespValue::Error(self.read_line()?)),
            b':' => {
                let line = self.read_line()?;
                let n = line
                    .parse::<i64>()
                    .map_err(|_| proto("bad RESP integer"))?;
                Ok(RespValue::Integer(n))
            }
            b'$' => {
                let line = self.read_line()?;
                let n = line
                    .parse::<i64>()
                    .map_err(|_| proto("bad RESP bulk length"))?;
                if n < 0 {
                    return Ok(RespValue::Nil);
                }
                let data = self.take(n as usize)?;
                self.skip(2)?; // trailing CRLF
                Ok(RespValue::Bulk(data))
            }
            b'*' => {
                let line = self.read_line()?;
                let n = line
                    .parse::<i64>()
                    .map_err(|_| proto("bad RESP array length"))?;
                if n < 0 {
                    return Ok(RespValue::Nil);
                }
                let mut items = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    items.push(self.read_resp()?);
                }
                Ok(RespValue::Array(items))
            }
            tag => Err(proto(format!("unexpected RESP tag {tag:#x}"))),
        }
    }
}

impl Rd for StreamReader {
    fn read_u8(&mut self) -> Result<u8, RestoreError> {
        self.next_u8().map_err(|_| RestoreError::BadDataFormat)
    }

    fn read_exact(&mut self, n: usize) -> Result<Vec<u8>, RestoreError> {
        self.take(n).map_err(|_| RestoreError::BadDataFormat)
    }
}

/// A control connection: a reader for replies and a writer for commands.
struct Tcp {
    r: StreamReader,
    w: TcpStream,
}

impl Tcp {
    fn connect(cfg: &ReplicaConfig, halt: Arc<AtomicBool>) -> Result<Self, ReplicaError> {
        let stream = TcpStream::connect((cfg.host.as_str(), cfg.port))
            .map_err(ReplicaError::Io)?;
        configure_socket(&stream)?;
        let w = stream.try_clone().map_err(ReplicaError::Io)?;
        Ok(Tcp {
            r: StreamReader::new(stream, halt),
            w,
        })
    }

    fn cmd(&mut self, args: &[Vec<u8>]) -> Result<(), ReplicaError> {
        write_cmd(&mut self.w, args)
    }

    fn cmd_str(&mut self, args: &[&str]) -> Result<(), ReplicaError> {
        let args: Vec<Vec<u8>> = args.iter().map(|a| a.as_bytes().to_vec()).collect();
        self.cmd(&args)
    }

    fn reply(&mut self) -> Result<RespValue, ReplicaError> {
        self.r.set_deadline(Some(HANDSHAKE_TIMEOUT));
        let r = self.r.read_resp();
        self.r.set_deadline(None);
        r
    }
}

fn configure_socket(stream: &TcpStream) -> Result<(), ReplicaError> {
    stream.set_nodelay(true).map_err(ReplicaError::Io)?;
    stream
        .set_read_timeout(Some(POLL_INTERVAL))
        .map_err(ReplicaError::Io)?;
    stream
        .set_write_timeout(Some(WRITE_TIMEOUT))
        .map_err(ReplicaError::Io)
}

fn write_cmd(w: &mut TcpStream, args: &[Vec<u8>]) -> Result<(), ReplicaError> {
    let mut out = Vec::with_capacity(16 + args.iter().map(|a| a.len() + 16).sum::<usize>());
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    w.write_all(&out).map_err(ReplicaError::Io)
}

fn expect_simple(v: RespValue, want: &str) -> Result<(), ReplicaError> {
    match v {
        RespValue::Simple(s) if s == want => Ok(()),
        RespValue::Error(e) => Err(proto(format!("master replied: {e}"))),
        other => Err(proto(format!("expected +{want}, got {other:?}"))),
    }
}

fn expect_ok(v: RespValue) -> Result<(), ReplicaError> {
    expect_simple(v, "OK")
}

/// The handshake result (`REPLCONF CAPA dragonfly` reply).
struct HandshakeInfo {
    master_replid: String,
    sync_id: String,
    num_flows: u64,
}

fn simple_str(v: &RespValue) -> Result<&str, ReplicaError> {
    match v {
        RespValue::Bulk(b) => Ok(std::str::from_utf8(b).map_err(|_| proto("non-utf8 bulk"))?),
        RespValue::Simple(s) => Ok(s),
        other => Err(proto(format!("expected bulk, got {other:?}"))),
    }
}

fn int(v: &RespValue) -> Result<i64, ReplicaError> {
    match v {
        RespValue::Integer(n) => Ok(*n),
        other => Err(proto(format!("expected integer, got {other:?}"))),
    }
}

/// The `Greet` handshake: PING, listening-port, capabilities, the dragonfly
/// session, then client identity. Mirrors `Replica::Greet` (`replica.cc`); the
/// `PSYNC ? -1` step is skipped because the master side of this port does not
/// implement Redis PSYNC (the dragonfly handshake does not need it).
fn greet(tcp: &mut Tcp, cfg: &ReplicaConfig) -> Result<HandshakeInfo, ReplicaError> {
    tcp.cmd_str(&["PING"])?;
    expect_simple(tcp.reply()?, "PONG")?;

    tcp.cmd_str(&["REPLCONF", "listening-port", &cfg.listen_port.to_string()])?;
    expect_ok(tcp.reply()?)?;

    tcp.cmd_str(&["REPLCONF", "capa", "eof", "capa", "psync2"])?;
    expect_ok(tcp.reply()?)?;

    tcp.cmd_str(&["REPLCONF", "capa", "dragonfly"])?;
    let info = match tcp.reply()? {
        RespValue::Array(items) if items.len() == 5 => {
            let master_replid = simple_str(&items[0])?.to_string();
            let sync_id = simple_str(&items[1])?.to_string();
            let num_flows = int(&items[2])?;
            if num_flows <= 0 || num_flows > 1024 {
                return Err(proto(format!("bad flow count {num_flows}")));
            }
            HandshakeInfo {
                master_replid,
                sync_id,
                num_flows: num_flows as u64,
            }
        }
        RespValue::Error(e) => return Err(proto(format!("master replied: {e}"))),
        other => return Err(proto(format!("unexpected capa reply {other:?}"))),
    };
    if info.num_flows != cfg.num_shards as u64 {
        return Err(proto(format!(
            "master flow count {} != shard count {}",
            info.num_flows, cfg.num_shards
        )));
    }

    tcp.cmd_str(&["REPLCONF", "CLIENT-ID", "0"])?;
    expect_ok(tcp.reply()?)?;
    tcp.cmd_str(&["REPLCONF", "CLIENT-VERSION", "8"])?;
    expect_ok(tcp.reply()?)?;

    Ok(info)
}

/// A decoded journal entry read from a flow socket.
struct JournalEntry {
    opcode: u8,
    dbid: u64,
    txid: u64,
    cmd: Vec<Vec<u8>>,
}

/// Streaming decoder for the journal wire format (`JournalReader`): each record
/// is self-delimiting, starting with an opcode; a COMMAND record begins with a
/// `SELECT dbid` prefix emitted by the fresh writer on the master.
struct JournalReader<'a> {
    stream: &'a mut StreamReader,
    dbid: u64,
}

impl<'a> JournalReader<'a> {
    fn read_entry(&mut self) -> Result<JournalEntry, ReplicaError> {
        loop {
            let op = self.stream.next_u8()?;
            if op == OP_SELECT {
                self.dbid = self.read_packed()?;
                continue;
            }
            return match op {
                OP_PING => Ok(JournalEntry {
                    opcode: OP_PING,
                    dbid: 0,
                    txid: 0,
                    cmd: Vec::new(),
                }),
                OP_LSN => {
                    let _lsn = self.read_packed()?;
                    Ok(JournalEntry {
                        opcode: OP_LSN,
                        dbid: 0,
                        txid: 0,
                        cmd: Vec::new(),
                    })
                }
                OP_COMMAND | OP_EXPIRED => {
                    let txid = self.read_packed()?;
                    self.read_packed()?; // deprecated `payload` field
                    let num = self.read_packed()? as usize;
                    let mut total = self.read_packed()?;
                    let mut cmd = Vec::with_capacity(num);
                    for _ in 0..num {
                        let size = self.read_packed()?;
                        if size > total {
                            return Err(proto("corrupt journal record"));
                        }
                        let s = self.stream.take(size as usize)?;
                        cmd.push(s);
                        total -= size;
                    }
                    Ok(JournalEntry {
                        opcode: op,
                        dbid: self.dbid,
                        txid,
                        cmd,
                    })
                }
                other => Err(proto(format!("unexpected journal opcode {other:#x}"))),
            };
        }
    }

    fn read_packed(&mut self) -> Result<u64, ReplicaError> {
        let b = self.stream.next_u8()?;
        match b >> 6 {
            0 => Ok(u64::from(b & 0x3f)),
            1 => {
                let lo = u64::from(self.stream.next_u8()?);
                Ok((u64::from(b & 0x3f) << 8) | lo)
            }
            2 => match b {
                0x80 => {
                    let s = self.stream.take(4)?;
                    let a: [u8; 4] = s.try_into().map_err(|_| proto("corrupt length"))?;
                    Ok(u64::from(u32::from_be_bytes(a)))
                }
                0x81 => {
                    let s = self.stream.take(8)?;
                    let a: [u8; 8] = s.try_into().map_err(|_| proto("corrupt length"))?;
                    Ok(u64::from_be_bytes(a))
                }
                _ => Err(proto("corrupt length")),
            },
            _ => Err(proto("corrupt length")),
        }
    }
}

/// `IsGlobalCmd`: only the commands the master executes as a global transaction
/// on every shard. Each flow applies them barrier-synchronized so the shards
/// cannot diverge on the position of a FLUSH.
fn is_global_cmd(cmd: &[Vec<u8>]) -> bool {
    let Some(name) = cmd.first() else {
        return false;
    };
    name.eq_ignore_ascii_case(b"FLUSHDB") || name.eq_ignore_ascii_case(b"FLUSHALL")
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Load the full-sync RDB stream for one shard, forwarding each key to the
/// shard thread (`RdbLoader::LoadRdbStream`). Returns the `JOURNAL_OFFSET`:
/// the LSN of the first journal record the stable-sync stream will carry.
fn load_rdb(r: &mut StreamReader, cfg: &ReplicaConfig, flow_id: usize) -> Result<u64, ReplicaError> {
    let magic = r.take(9)?;
    if &magic != b"REDIS0009" {
        return Err(proto("bad full-sync magic"));
    }
    let mut dbid = 0u64;
    let mut pending_expiry: Option<u64> = None;
    let mut journal_lsn = 0u64;
    loop {
        let op = r.next_u8()?;
        match op {
            // AUX: skip key + value.
            0xfa => {
                let _ = r.read_string()?;
                let _ = r.read_string()?;
            }
            // SELECTDB <id>
            0xfe => {
                let (d, _) = r.read_len()?;
                dbid = d;
            }
            // RESIZEDB <keys> <expires>
            0xfb => {
                let _ = r.read_len()?;
                let _ = r.read_len()?;
            }
            // EXPIRETIME_MS <u64 le>
            0xfc => {
                let s = r.take(8)?;
                let a: [u8; 8] = s.try_into().map_err(|_| proto("corrupt expiry"))?;
                pending_expiry = Some(u64::from_le_bytes(a));
            }
            // FULLSYNC_END + 8 zero bytes.
            0xc8 => {
                r.skip(8)?;
            }
            // JOURNAL_OFFSET + u64 le.
            0xd3 => {
                let s = r.take(8)?;
                let a: [u8; 8] = s.try_into().map_err(|_| proto("corrupt journal offset"))?;
                journal_lsn = u64::from_le_bytes(a);
            }
            // EOF + 8 zero bytes: end of the snapshot.
            0xff => {
                r.skip(8)?;
                return Ok(journal_lsn);
            }
            // Otherwise a value type: <type> <key> <value>.
            typ => {
                let key = r.read_string()?;
                match rdb::load_value(r, typ, now_ms())? {
                    rdb::RestoreOutcome::Value(value) => {
                        let msg = ShardMsg::ReplicaLoadValue {
                            db_idx: dbid as usize,
                            key,
                            value,
                            expire_at: pending_expiry,
                        };
                        let _ = cfg.shard_txs[flow_id].send(msg);
                    }
                    rdb::RestoreOutcome::Expired => {}
                }
                pending_expiry = None;
            }
        }
    }
}

/// Apply a journal command on this flow's shard, waiting for the shard to
/// finish (like the reference's per-shard `executor_->Execute`).
fn apply_ops(
    cfg: &ReplicaConfig,
    flow_id: usize,
    args: &[Vec<u8>],
    db_idx: usize,
) -> Result<(), ReplicaError> {
    let (ack_tx, ack_rx) = mpsc::channel();
    let msg = ShardMsg::ReplicaOp {
        args: args.to_vec(),
        db_idx,
        ack: ack_tx,
    };
    cfg.shard_txs[flow_id]
        .send(msg)
        .map_err(|_| ReplicaError::Shard)?;
    ack_rx.recv().map_err(|_| ReplicaError::Shard)
}

/// Flush every shard before a full sync (`Replica::FlushAll`).
fn flush_all_shards(cfg: &ReplicaConfig) -> Result<(), ReplicaError> {
    let mut rxs = Vec::with_capacity(cfg.num_shards);
    for (i, tx) in cfg.shard_txs.iter().enumerate() {
        let (ack_tx, ack_rx) = mpsc::channel();
        tx.send(ShardMsg::ReplicaFlushAll { ack: ack_tx })
            .map_err(|_| ReplicaError::Shard)?;
        rxs.push((i, ack_rx));
    }
    for (_, rx) in rxs {
        rx.recv().map_err(|_| ReplicaError::Shard)?;
    }
    Ok(())
}

/// A global transaction entry: counts how many flows reached (and completed)
/// the txid, so FLUSH commands apply on every shard at the same logical point.
struct GlobalTx {
    num: usize,
    arrived: usize,
    barrier: usize,
    done: usize,
}

/// The shared rendezvous for global commands (`MultiShardExecution`): the first
/// flow to reach a txid creates its entry; every flow waits for all `num_flows`
/// arrivals before applying, and the last flow to finish removes the entry.
struct GlobalBarrier {
    map: Mutex<HashMap<u64, GlobalTx>>,
    cond: Condvar,
}

impl GlobalBarrier {
    fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            cond: Condvar::new(),
        }
    }
}

/// Wait until `pred` holds for `txid`, polling the abort flag so a failed peer
/// flow cannot deadlock the session.
fn wait_until<'g>(
    barrier: &GlobalBarrier,
    mut guard: MutexGuard<'g, HashMap<u64, GlobalTx>>,
    txid: u64,
    pred: impl Fn(&GlobalTx) -> bool,
    abort: &AtomicBool,
) -> Result<MutexGuard<'g, HashMap<u64, GlobalTx>>, ReplicaError> {
    loop {
        if pred(&guard[&txid]) {
            return Ok(guard);
        }
        if abort.load(Ordering::Relaxed) {
            return Err(ReplicaError::Stopped);
        }
        let (g, _) = barrier
            .cond
            .wait_timeout(guard, POLL_INTERVAL)
            .map_err(|e| proto(format!("barrier poisoned: {e}")))?;
        guard = g;
    }
}

fn global_apply(
    barrier: &GlobalBarrier,
    txid: u64,
    args: &[Vec<u8>],
    db_idx: usize,
    cfg: &ReplicaConfig,
    flow_id: usize,
    abort: &AtomicBool,
) -> Result<(), ReplicaError> {
    let mut guard = barrier.map.lock().unwrap();
    let g = guard.entry(txid).or_insert(GlobalTx {
        num: cfg.num_shards,
        arrived: 0,
        barrier: 0,
        done: 0,
    });
    g.arrived += 1;
    if g.arrived == g.num {
        barrier.cond.notify_all();
    }
    guard = wait_until(barrier, guard, txid, |g| g.arrived == g.num, abort)?;
    let g = guard.get_mut(&txid).unwrap();
    g.barrier += 1;
    if g.barrier == g.num {
        barrier.cond.notify_all();
    }
    guard = wait_until(barrier, guard, txid, |g| g.barrier == g.num, abort)?;
    drop(guard);

    apply_ops(cfg, flow_id, args, db_idx)?;

    let mut guard = barrier.map.lock().unwrap();
    let g = guard.get_mut(&txid).unwrap();
    g.done += 1;
    if g.done == g.num {
        guard.remove(&txid);
        barrier.cond.notify_all();
    }
    Ok(())
}

/// The per-flow ACK fiber (`StableSyncDflyAcksFb`): periodically writes
/// `REPLCONF ACK <next-lsn>` on the flow socket, immediately after a PING.
fn ack_thread(
    mut w: TcpStream,
    lsn_cell: Arc<AtomicU64>,
    force_ack: Arc<AtomicBool>,
    ack_stop: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
) {
    let mut last = Instant::now();
    while !ack_stop.load(Ordering::Relaxed)
        && !abort.load(Ordering::Relaxed)
        && !stop.load(Ordering::Relaxed)
    {
        if force_ack.swap(false, Ordering::SeqCst) || last.elapsed() >= ACK_INTERVAL {
            let lsn = lsn_cell.load(Ordering::SeqCst);
            let args: Vec<Vec<u8>> = vec![
                b"REPLCONF".to_vec(),
                b"ACK".to_vec(),
                lsn.to_string().into_bytes(),
            ];
            if write_cmd(&mut w, &args).is_err() {
                return;
            }
            last = Instant::now();
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// One flow: connect, negotiate FULL vs PARTIAL, load the RDB snapshot when
/// FULL, then apply journal records forever (or until the session stops).
fn flow_thread(sess: &Arc<Session>, flow_id: usize) -> Result<(), ReplicaError> {
    let lsn = sess.cfg.lsn_cells[flow_id].load(Ordering::SeqCst);
    let stream = TcpStream::connect((sess.cfg.host.as_str(), sess.cfg.port))
        .map_err(ReplicaError::Io)?;
    configure_socket(&stream)?;
    let read = stream.try_clone().map_err(ReplicaError::Io)?;
    let mut write = stream.try_clone().map_err(ReplicaError::Io)?;

    let mut sr = StreamReader::new(read, sess.abort.clone());
    let flow_lsn = lsn.to_string();
    let flow_arg = flow_id.to_string();
    write_cmd(
        &mut write,
        &[
            b"DFLY".to_vec(),
            b"FLOW".to_vec(),
            sess.master_replid.clone().into_bytes(),
            sess.session_id.clone().into_bytes(),
            flow_arg.clone().into_bytes(),
            flow_lsn.into_bytes(),
        ],
    )?;

    sr.set_deadline(Some(HANDSHAKE_TIMEOUT));
    let reply = sr.read_resp()?;
    sr.set_deadline(None);
    let (is_full, eof_token) = match &reply {
        RespValue::Array(items) if items.len() == 2 => {
            let kind = simple_str(&items[0])?;
            let token = simple_str(&items[1])?.to_string();
            if kind != "FULL" && kind != "PARTIAL" {
                return Err(proto(format!("bad sync type {kind}")));
            }
            (kind == "FULL", token)
        }
        RespValue::Error(e) => return Err(proto(format!("master replied: {e}"))),
        other => return Err(proto(format!("unexpected DFLY FLOW reply {other:?}"))),
    };

    let _ = sess.started_tx.send((flow_id, Ok(is_full)));

    let mut next_lsn = lsn;
    if is_full {
        next_lsn = load_rdb(&mut sr, &sess.cfg, flow_id)?;
        sess.cfg.lsn_cells[flow_id].store(next_lsn, Ordering::SeqCst);
    }
    let _ = sess.full_tx.send((flow_id, Ok(())));

    // The eof token closes the RDB snapshot; journal records follow it.
    let token = sr.take(eof_token.len())?;
    if String::from_utf8_lossy(&token) != eof_token {
        return Err(proto("eof token mismatch"));
    }

    // Stable sync: apply records, track the next-needed LSN, ack periodically.
    let lsn_cell = sess.cfg.lsn_cells[flow_id].clone();
    let force_ack = Arc::new(AtomicBool::new(false));
    let ack_stop = Arc::new(AtomicBool::new(false));
    {
        let w = write.try_clone().map_err(ReplicaError::Io)?;
        let fa = force_ack.clone();
        let as_ = ack_stop.clone();
        let abort = sess.abort.clone();
        let stop = sess.stop.clone();
        let cell = lsn_cell.clone();
        std::thread::spawn(move || ack_thread(w, cell, fa, as_, abort, stop));
    }

    let mut reader = JournalReader { stream: &mut sr, dbid: 0 };
    let result = (|| -> Result<(), ReplicaError> {
        loop {
            let entry = reader.read_entry()?;
            match entry.opcode {
                OP_PING => {
                    force_ack.store(true, Ordering::SeqCst);
                    next_lsn += 1;
                }
                OP_LSN => {}
                OP_COMMAND | OP_EXPIRED => {
                    if is_global_cmd(&entry.cmd) {
                        global_apply(
                            &sess.global,
                            entry.txid,
                            &entry.cmd,
                            entry.dbid as usize,
                            &sess.cfg,
                            flow_id,
                            &sess.abort,
                        )?;
                    } else {
                        apply_ops(&sess.cfg, flow_id, &entry.cmd, entry.dbid as usize)?;
                    }
                    let mut status = sess.cfg.status.lock().unwrap();
                    status.journal_rec_executed += 1;
                    drop(status);
                    next_lsn += 1;
                }
                other => return Err(proto(format!("unexpected journal opcode {other:#x}"))),
            }
            lsn_cell.store(next_lsn, Ordering::SeqCst);
            if sess.abort.load(Ordering::Relaxed) || sess.stop.load(Ordering::Relaxed) {
                return Ok(());
            }
        }
    })();
    ack_stop.store(true, Ordering::SeqCst);
    result
}

/// Shared state for one replication session (one control + `num_shards` flows).
struct Session {
    cfg: ReplicaConfig,
    master_replid: String,
    session_id: String,
    global: Arc<GlobalBarrier>,
    abort: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    started_tx: mpsc::Sender<(usize, Result<bool, String>)>,
    full_tx: mpsc::Sender<(usize, Result<(), String>)>,
    err_tx: mpsc::Sender<String>,
}

/// Receive on `rx`, waking up to observe the abort/stop flags. `None` means the
/// session was asked to stop.
fn recv_running<T>(
    rx: &mpsc::Receiver<T>,
    abort: &AtomicBool,
    stop: &AtomicBool,
) -> Result<Option<T>, ReplicaError> {
    loop {
        match rx.recv_timeout(POLL_INTERVAL) {
            Ok(v) => return Ok(Some(v)),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if abort.load(Ordering::Relaxed) {
                    return Err(ReplicaError::Stopped);
                }
                if stop.load(Ordering::Relaxed) {
                    return Ok(None);
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(proto("flow channel closed"));
            }
        }
    }
}

/// One full session: handshake, spawn the flows, drive full/partial sync to
/// STABLE_SYNC, then wait for a flow failure or `REPLICAOF NO ONE`.
fn connect_and_replicate(cfg: &ReplicaConfig) -> Result<(), ReplicaError> {
    {
        let mut status = cfg.status.lock().unwrap();
        status.phase = ReplicaPhase::Connecting;
        status.error = None;
    }

    let abort = Arc::new(AtomicBool::new(false));
    let mut tcp = Tcp::connect(cfg, cfg.stop.clone())?;
    let handshake = greet(&mut tcp, cfg)?;
    {
        let mut status = cfg.status.lock().unwrap();
        status.phase = ReplicaPhase::FullSync;
        status.sync_id.clone_from(&handshake.sync_id);
        status.journal_rec_executed = 0;
    }

    let (started_tx, started_rx) = mpsc::channel();
    let (full_tx, full_rx) = mpsc::channel();
    let (err_tx, err_rx) = mpsc::channel();
    let session = Arc::new(Session {
        cfg: cfg.clone(),
        master_replid: handshake.master_replid,
        session_id: handshake.sync_id.clone(),
        global: Arc::new(GlobalBarrier::new()),
        abort: abort.clone(),
        stop: cfg.stop.clone(),
        started_tx,
        full_tx,
        err_tx,
    });

    let mut handles = Vec::with_capacity(cfg.num_shards);
    for flow_id in 0..cfg.num_shards {
        let s = session.clone();
        let err_tx = session.err_tx.clone();
        handles.push(std::thread::Builder::new()
            .name(format!("replica-flow-{flow_id}"))
            .spawn(move || {
                if let Err(e) = flow_thread(&s, flow_id) {
                    // Surface the failure to the stable-sync waiter unless the
                    // session is already shutting down.
                    if !s.abort.load(Ordering::Relaxed) {
                        let _ = err_tx.send(e.into_string());
                    }
                }
            })
            .expect("failed to spawn replica flow thread"));
    }
    // The flows exit on their own once `abort`/`stop` is set; the handles only
    // keep the threads referenced. Late shard messages from a dying flow are
    // FIFO-ordered before the next session's flush, so they cannot corrupt it.
    let _ = handles;

    // Wait for every flow's DFLY FLOW handshake.
    let mut fulls = vec![false; cfg.num_shards];
    for _ in 0..cfg.num_shards {
        match recv_running(&started_rx, &abort, &cfg.stop)? {
            Some((f, Ok(is_full))) => fulls[f] = is_full,
            Some((_, Err(e))) => {
                abort.store(true, Ordering::SeqCst);
                return Err(proto(e));
            }
            None => {
                abort.store(true, Ordering::SeqCst);
                return Ok(());
            }
        }
    }
    let all_full = fulls.iter().all(|f| *f);
    let all_partial = fulls.iter().all(|f| !*f);
    if !all_full && !all_partial {
        // The master cannot resume a mixed session (DFLY SYNC requires every
        // flow to be FULL); force a full re-sync on the next attempt.
        for cell in cfg.lsn_cells.iter() {
            cell.store(0, Ordering::SeqCst);
        }
        abort.store(true, Ordering::SeqCst);
        return Err(proto("mixed full/partial sync"));
    }

    if all_full {
        flush_all_shards(cfg)?;
        tcp.cmd_str(&["DFLY", "SYNC", &session.session_id])?;
        expect_ok(tcp.reply()?)?;
    }

    // Wait for every flow to finish loading (FULL) or skip it (PARTIAL).
    for _ in 0..cfg.num_shards {
        match recv_running(&full_rx, &abort, &cfg.stop)? {
            Some((_, Ok(()))) => {}
            Some((_, Err(e))) => {
                abort.store(true, Ordering::SeqCst);
                return Err(proto(e));
            }
            None => {
                abort.store(true, Ordering::SeqCst);
                return Ok(());
            }
        }
    }

    tcp.cmd_str(&["DFLY", "STARTSTABLE", &session.session_id])?;
    expect_ok(tcp.reply()?)?;

    {
        let mut status = cfg.status.lock().unwrap();
        status.phase = ReplicaPhase::StableSync;
        status.error = None;
    }

    // Stable sync: the flows run; this thread waits for a failure or a stop.
    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            abort.store(true, Ordering::SeqCst);
            return Ok(());
        }
        match err_rx.recv_timeout(POLL_INTERVAL) {
            Ok(e) => {
                abort.store(true, Ordering::SeqCst);
                return Err(proto(e));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                abort.store(true, Ordering::SeqCst);
                return Err(proto("all flows exited"));
            }
        }
    }
}

/// The replica thread entry point: repeatedly try to replicate until stopped.
/// Takes the config by value because the caller moves it into the thread.
#[allow(clippy::needless_pass_by_value)]
pub fn run(cfg: ReplicaConfig) {
    while !cfg.stop.load(Ordering::Relaxed) {
        match connect_and_replicate(&cfg) {
            Ok(()) => {
                // Clean stop (REPLICAOF NO ONE).
                let mut status = cfg.status.lock().unwrap();
                *status = ReplicaStatus::default();
                return;
            }
            Err(e) => {
                let msg = e.into_string();
                let mut status = cfg.status.lock().unwrap();
                status.error = Some(msg);
                if status.phase != ReplicaPhase::StableSync {
                    status.phase = ReplicaPhase::Connecting;
                }
            }
        }
        // Back off between attempts, but stay responsive to REPLICAOF NO ONE.
        for _ in 0..(CONNECT_BACKOFF.as_millis() / 100) {
            if cfg.stop.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    let mut status = cfg.status.lock().unwrap();
    *status = ReplicaStatus::default();
}
