use std::collections::{BTreeMap, HashMap};
use std::net::TcpListener;
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use std::fmt::Write as _;

use crate::commands::exec::keys::glob_match;
use crate::commands::exec::server;
use crate::commands::{Command, FLAG_ADMIN, FLAG_BLOCKING, FLAG_GLOBAL, FLAG_LOCAL, FLAG_WRITE};
use crate::error::RespValue;
use crate::protocol::resp::RespParser;
use crate::server::coordinator::format_exec_slowlog;
use crate::server::pubsub::{self, ChannelStore, SubscribeInfo};
use crate::server::replica::{self, ReplicaPhase, ReplicaStatus};
use crate::server::replication::{self, ChunkKind, ReplChunk, ReplicationManager, SyncState};
use crate::server::slowlog::{SlowLog, SlowLogEntry};
use crate::server::{
    CoordMsg, GcRequest, Reply, ServerEnv, ShardMsg, SingleOp, TrackingMode, WatchState,
    command_for, encode_value, extract_keys, is_eval_cmd, is_function_cmd, keys_per_shard,
    local_function, local_script,
};

const EV_READ: i16 = libc::EVFILT_READ;
const EV_WRITE: i16 = libc::EVFILT_WRITE;
const EV_ADD_ENABLE: u16 = libc::EV_ADD | libc::EV_ENABLE;
const EV_DELETE: u16 = libc::EV_DELETE;

/// `gettimeofday`-style `sec.usec` timestamp used in MONITOR lines.
fn monitor_timestamp() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}.{:06}", now.as_secs(), now.subsec_micros())
}

/// Epoch microseconds (wall clock): SLOWLOG entries' `unix_ts_usec`.
fn now_usec() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// `ERR Unknown subcommand or wrong number of arguments for '<sub>'. Try
/// SLOWLOG HELP.` (`facade::UnknownSubCmd`).
fn slowlog_unknown_subcmd(sub: &[u8]) -> String {
    format!(
        "ERR Unknown subcommand or wrong number of arguments for '{}'. Try SLOWLOG HELP.",
        String::from_utf8_lossy(sub)
    )
}

/// `ParseClientListFilter`'s result: an optional `TYPE` class plus an
/// allow-list of connection ids (`CLIENT LIST ID ...`).
#[derive(Default)]
struct ClientListFilter {
    kind: Option<ClientListType>,
    ids: Vec<u32>,
}

/// Connection classes from `ServerFamily::ClassifyConnection` used by the
/// `CLIENT LIST TYPE <normal|master|replica|pubsub>` filter.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ClientListType {
    Normal,
    Master,
    Replica,
    Pubsub,
}

/// `facade::kSyntaxErr`.
fn syntax_err() -> RespValue {
    RespValue::Error("ERR syntax error".into())
}

/// One `CLIENT LIST` / `CLIENT INFO` info line (`facade::Connection::
/// GetClientInfo`). Field values reflect the connection's live state; the
/// reference emits the same fields for a real connection.
fn client_info_line(conn: &Conn) -> String {
    let flags = if conn.monitor {
        'O'
    } else if !conn.sub.is_empty() {
        'P'
    } else {
        'N'
    };
    let multi = match conn.multi.phase {
        MultiPhase::Collect => conn.multi.queue.len() as i64,
        _ => -1,
    };
    format!(
        "id={} addr={} fd={} name= age=0 idle=0 flags={} db={} sub={} psub=0 multi={} \
         watch={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 \
         omem=0 tot-mem=0 events=r cmd=client user=default redir=-1 resp=2",
        conn.id,
        conn.remote,
        conn.fd,
        flags,
        conn.db_idx,
        conn.sub.channels.len() + conn.sub.sharded.len(),
        multi,
        conn.watched.len(),
    )
}

/// `ParseClientListFilter` (server_family.cc:408): `TYPE <type>` or
/// `ID <id>...`, rejecting trailing/unknown tokens.
fn parse_client_list_filter(args: &[Vec<u8>]) -> Result<ClientListFilter, RespValue> {
    let rest = &args[2..];
    let mut filter = ClientListFilter {
        kind: None,
        ids: Vec::new(),
    };
    if rest.is_empty() {
        return Ok(filter);
    }
    match rest[0].to_ascii_uppercase().as_slice() {
        b"TYPE" => {
            if rest.len() < 2 {
                return Err(syntax_err());
            }
            let raw = String::from_utf8_lossy(&rest[1]);
            let kind = match raw.to_ascii_uppercase().as_str() {
                "NORMAL" => ClientListType::Normal,
                "MASTER" => ClientListType::Master,
                "REPLICA" | "SLAVE" => ClientListType::Replica,
                "PUBSUB" => ClientListType::Pubsub,
                _ => {
                    return Err(RespValue::Error(format!("ERR Unknown client type '{raw}'")));
                }
            };
            if rest.len() > 2 {
                return Err(syntax_err());
            }
            filter.kind = Some(kind);
        }
        b"ID" => {
            if rest.len() < 2 {
                return Err(syntax_err());
            }
            for id in &rest[1..] {
                match crate::util::parse_u64(id).and_then(|n| u32::try_from(n).ok()) {
                    Some(n) => filter.ids.push(n),
                    None => {
                        return Err(RespValue::Error("ERR Invalid client ID".into()));
                    }
                }
            }
        }
        _ => return Err(syntax_err()),
    }
    Ok(filter)
}

/// `ClassifyConnection` (server_family.cc:448): replica takes priority over
/// pubsub; everything else is normal.
fn classify_conn(conn: &Conn) -> ClientListType {
    if conn.repl.is_some() {
        return ClientListType::Replica;
    }
    if !conn.sub.is_empty() {
        return ClientListType::Pubsub;
    }
    ClientListType::Normal
}

/// Escape a command argument for MONITOR output the way upstream's
/// `CmdEntryToMonitorFormat` does: backslash and quote are backslash-escaped,
/// control characters use their C short forms, and every other non-printable
/// byte is emitted as `\xNN`.
fn monitor_escape(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len());
    for &b in bytes {
        match b {
            b'\\' => s.push_str("\\\\"),
            b'"' => s.push_str("\\\""),
            b'\n' => s.push_str("\\n"),
            b'\r' => s.push_str("\\r"),
            b'\t' => s.push_str("\\t"),
            7 => s.push_str("\\a"),
            8 => s.push_str("\\b"),
            0..=31 | 127..=255 => write!(s, "\\x{b:02x}").unwrap(),
            _ => s.push(b as char),
        }
    }
    s
}

/// Normalize a CONFIG parameter name or GET pattern: lowercased with `-`
/// replaced by `_`, so `replica-priority` and `replica_priority` are the same
/// parameter (`ConfigNormalization`).
fn normalize_config_name(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw)
        .to_ascii_lowercase()
        .replace('-', "_")
}

/// Parse a human-readable memory size: an integer with an optional binary
/// unit suffix (`b`, `k`/`kb`, `m`/`mb`, `g`/`gb`, `t`/`tb`), so `1GB` is
/// 2^30 bytes (`ConfigGetMemoryBytes`).
fn parse_human_size(raw: &[u8]) -> Option<u64> {
    let s = String::from_utf8_lossy(raw);
    let s = s.trim();
    let mut digits = 0;
    for b in s.as_bytes() {
        if b.is_ascii_digit() {
            digits += 1;
        } else {
            break;
        }
    }
    let (num, unit) = s.split_at(digits);
    let value = num.parse::<u64>().ok()?;
    let mult: u64 = match unit.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" => 1 << 10,
        "m" | "mb" => 1 << 20,
        "g" | "gb" => 1 << 30,
        "t" | "tb" => 1 << 40,
        _ => return None,
    };
    value.checked_mul(mult)
}

/// Phases of a connection-scoped MULTI/EXEC block (mirrors
/// `ConnectionState::ExecInfo::ExecState`). `Collect` accepts queued commands;
/// `Error` marks a block that will fail at EXEC (EXECABORT); commands arriving
/// while in `Error` execute immediately, like upstream.
#[derive(Default, Clone, Copy, PartialEq, Eq)]
enum MultiPhase {
    #[default]
    Inactive,
    Collect,
    Error,
}

/// A key watched by a connection via WATCH, with the state snapshot taken at
/// watch time. At EXEC the live state is compared and any difference (or a
/// FLUSHDB epoch bump) aborts the transaction.
struct WatchedKey {
    db: usize,
    key: Vec<u8>,
    state: WatchState,
}

/// Connection-scoped MULTI/EXEC state.
#[derive(Default)]
struct MultiState {
    phase: MultiPhase,
    /// Commands queued while the block is collecting (raw argument vectors).
    queue: Vec<Vec<Vec<u8>>>,
}

struct Conn {
    id: u64,
    fd: RawFd,
    parser: RespParser,
    out: Vec<u8>,
    write_registered: bool,
    /// Next seq to assign to an incoming request.
    dispatch_seq: u64,
    /// Next seq whose reply can be appended to `out`.
    deliver_seq: u64,
    /// Replies received out of order (seq -> encoded bytes).
    buffered: BTreeMap<u64, Vec<u8>>,
    /// The connection's currently selected DB index.
    db_idx: usize,
    /// Whether the connection negotiated RESP3 (`HELLO 3`). CLIENT TRACKING and
    /// CACHING reject RESP2 connections, and invalidation pushes are only ever
    /// sent to RESP3 connections.
    resp3: bool,
    /// MULTI/EXEC transaction state.
    multi: MultiState,
    /// True while EXEC runs the queued commands: their blocking commands must
    /// reply nil instead of waiting (upstream `IsMulti` during exec). The
    /// `multi` state is reset before the queue runs, so this carries the flag.
    exec_multi: bool,
    /// Keys this connection is watching (WATCH/UNWATCH).
    watched: Vec<WatchedKey>,
    /// Latch mirroring upstream `ExecInfo::watched_dirty`: set when any
    /// watched key is observed modified, making the next EXEC abort even if a
    /// later WATCH re-registers keys. Cleared by EXEC/DISCARD/RESET/UNWATCH.
    watched_dirty: bool,
    /// Pub/sub channels and patterns this connection is subscribed to.
    sub: SubscribeInfo,
    /// Peer address rendered as `ip:port`, shown in MONITOR output.
    remote: String,
    /// Whether this connection is registered as a MONITOR. Monitor
    /// connections reject every command except RESET/QUIT.
    monitor: bool,
    /// This connection's role in a replication session, if any.
    repl: Option<ConnRepl>,
    /// Set by QUIT: close the socket once pending output has flushed.
    closing: bool,
    /// In-flight requests awaiting their reply (seq -> start info), used to
    /// measure SLOWLOG latency from dispatch to reply delivery.
    pending_slowlog: HashMap<u64, PendingSlowlog>,
}

/// A dispatched command whose reply has not arrived yet. The SLOWLOG entry is
/// produced when the reply for `seq` is flushed.
struct PendingSlowlog {
    name: String,
    args: Vec<Vec<u8>>,
    started: Instant,
    /// `FormatExecSlowlog`/`FormatEvalSlowlog` arguments attached by the
    /// coordinator for EXEC/EVAL replies; other commands send `None` and the
    /// raw tail is used.
    slowlog_args: Option<Vec<Vec<u8>>>,
}

/// The role a connection plays inside a replica session. The control connection
/// carries `REPLCONF`/`DFLY`; each flow connection carries the RDB stream and
/// the stable-sync journal for one shard.
#[derive(Debug, Clone, Copy)]
enum ConnRepl {
    /// The session's control connection (`REPLCONF CAPA dragonfly`).
    Control { sync_id: u32 },
    /// A `DFLY FLOW` connection for one shard of the session.
    Flow { sync_id: u32, flow_id: usize },
}

/// A single-threaded kqueue-based IO event loop. Owns all client sockets, the
/// reply bus receiver, the shared `ServerEnv` handle, and the master-side
/// replication state.
pub struct IoLoop {
    env: ServerEnv,
    reply_bus_rx: mpsc::Receiver<Reply>,
    /// Stable-sync journal chunks from the shard threads, routed to their
    /// flow connections.
    repl_rx: mpsc::Receiver<ReplChunk>,
    listener: TcpListener,
    wake_r: RawFd,
    kq: RawFd,
    conns: HashMap<u64, Conn>,
    fd_to_id: HashMap<RawFd, u64>,
    next_conn_id: u64,
    /// Subscriber index shared by all connections.
    pubsub: ChannelStore,
    /// Connection ids currently in MONITOR mode (fed by the broadcast below).
    monitors: Vec<u64>,
    /// Replica sessions (`dflycmd.cc` `replica_infos_`).
    repl: ReplicationManager,
    /// Set by SHUTDOWN; the run loop stops once pending replies are flushed.
    shutting_down: bool,
    /// Replica-side state: the status shared with the replica thread (surfaced
    /// through ROLE), the stop flag for `REPLICAOF NO ONE`, and the running
    /// replica thread, if any.
    replica_status: Arc<Mutex<ReplicaStatus>>,
    replica_stop: Arc<AtomicBool>,
    replica_handle: Option<std::thread::JoinHandle<()>>,
    /// The IO thread's SLOWLOG ring (`ServerState::tlocal()->GetSlowLog()`).
    slow_log: SlowLog,
    /// `replica_priority` config (`RegisterMutable`): dash/underscore names
    /// are interchangeable (`ConfigNormalization`).
    replica_priority: i64,
    /// `maxmemory` config in bytes; human-readable sizes are parsed on set.
    maxmemory: u64,
}

impl IoLoop {
    pub fn new(
        env: ServerEnv,
        reply_bus_rx: mpsc::Receiver<Reply>,
        repl_rx: mpsc::Receiver<ReplChunk>,
        listener: TcpListener,
        wake_r: RawFd,
    ) -> std::io::Result<Self> {
        let kq = unsafe { libc::kqueue() };
        if kq < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(IoLoop {
            env,
            reply_bus_rx,
            repl_rx,
            listener,
            wake_r,
            kq,
            conns: HashMap::new(),
            fd_to_id: HashMap::new(),
            next_conn_id: 1,
            pubsub: ChannelStore::new(),
            monitors: Vec::new(),
            repl: ReplicationManager::new(),
            shutting_down: false,
            replica_status: Arc::new(Mutex::new(ReplicaStatus::default())),
            replica_stop: Arc::new(AtomicBool::new(false)),
            replica_handle: None,
            slow_log: SlowLog::new(),
            replica_priority: 100,
            maxmemory: 0,
        })
    }

    pub fn run(&mut self) -> std::io::Result<()> {
        self.listener.set_nonblocking(true)?;
        set_nonblocking(self.wake_r);
        let listen_fd = self.listener.as_raw_fd();
        let setup = [
            kev(listen_fd as usize, EV_READ, EV_ADD_ENABLE),
            kev(self.wake_r as usize, EV_READ, EV_ADD_ENABLE),
        ];
        self.kevent_change(&setup)?;

        let mut out_events = Vec::with_capacity(256);
        out_events.resize_with(256, zero_kev);
        loop {
            let n = unsafe {
                libc::kevent(
                    self.kq,
                    std::ptr::null(),
                    0,
                    out_events.as_mut_ptr(),
                    out_events.len() as i32,
                    std::ptr::null(),
                )
            };
            if n < 0 {
                let e = std::io::Error::last_os_error();
                if e.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(e);
            }
            for ev in out_events.iter().take(n as usize) {
                self.handle_event(*ev);
            }
            self.drain_bus();
            self.drain_repl();
            self.flush_all();
            if self.shutting_down {
                break;
            }
        }
        Ok(())
    }

    fn handle_event(&mut self, ev: libc::kevent) {
        if ev.filter == libc::EVFILT_READ {
            if ev.ident == self.wake_r as usize {
                self.drain_wake();
            } else if ev.ident == self.listener.as_raw_fd() as usize {
                self.accept_conns();
            } else {
                self.handle_read(ev.ident as RawFd);
            }
        } else if ev.filter == libc::EVFILT_WRITE {
            let fd = ev.ident as RawFd;
            if let Some(&cid) = self.fd_to_id.get(&fd) {
                self.flush_conn(cid);
            }
        }
    }

    // ------------------------------------------------------------------
    // Connections
    // ------------------------------------------------------------------

    fn accept_conns(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    let remote = stream
                        .peer_addr()
                        .map_or_else(|_| "0.0.0.0:0".into(), |a| a.to_string());
                    let _ = stream.set_nonblocking(true);
                    let fd = stream.into_raw_fd();
                    self.register_conn(fd, remote);
                }
                Err(e) if is_again(&e) => break,
                Err(_) => break,
            }
        }
    }

    fn register_conn(&mut self, fd: RawFd, remote: String) {
        let conn_id = self.next_conn_id;
        self.next_conn_id += 1;
        self.conns.insert(
            conn_id,
            Conn {
                id: conn_id,
                fd,
                parser: RespParser::new(),
                out: Vec::new(),
                write_registered: false,
                dispatch_seq: 0,
                deliver_seq: 0,
                buffered: BTreeMap::new(),
                db_idx: 0,
                resp3: false,
                multi: MultiState::default(),
                exec_multi: false,
                watched: Vec::new(),
                watched_dirty: false,
                sub: SubscribeInfo::default(),
                remote,
                monitor: false,
                repl: None,
                closing: false,
                pending_slowlog: HashMap::new(),
            },
        );
        self.fd_to_id.insert(fd, conn_id);
        let ev = kev(fd as usize, EV_READ, EV_ADD_ENABLE);
        let _ = self.kevent_change(&[ev]);
    }

    fn close_conn(&mut self, conn_id: u64) {
        // A closed flow/control connection tears down the whole replica session
        // (`DflyCmd::StopReplication`); the shard consumers are unregistered.
        if let Some(role) = self.conns.get(&conn_id).and_then(|c| c.repl) {
            let sync_id = match role {
                ConnRepl::Control { sync_id } => sync_id,
                ConnRepl::Flow { sync_id, .. } => sync_id,
            };
            self.stop_replication(sync_id);
        }
        if let Some(conn) = self.conns.remove(&conn_id) {
            self.fd_to_id.remove(&conn.fd);
            self.pubsub.remove_conn(conn_id);
            self.monitors.retain(|&c| c != conn_id);
            // Drop the connection's tracking state (`SetClientTracking(false)`
            // on close, main_service.cc:1955); its registered keys go too.
            self.env.tracking.lock().unwrap().remove_conn(conn_id);
            unsafe { libc::close(conn.fd) };
        }
    }

    fn handle_read(&mut self, fd: RawFd) {
        let Some(&conn_id) = self.fd_to_id.get(&fd) else {
            return;
        };
        let mut buf = [0u8; 16384];
        let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast::<libc::c_void>(), buf.len()) };
        if n <= 0 {
            if n < 0 && is_again(&std::io::Error::last_os_error()) {
                return;
            }
            self.close_conn(conn_id);
            return;
        }
        {
            let conn = self.conns.get_mut(&conn_id).unwrap();
            conn.parser.feed(&buf[..n as usize]);
        }
        let mut protocol_error = None;
        loop {
            let parsed = self.conns.get_mut(&conn_id).unwrap().parser.next_request();
            match parsed {
                Ok(Some(args)) => self.dispatch(conn_id, args),
                Ok(None) => break,
                Err(e) => {
                    protocol_error = Some(e);
                    break;
                }
            }
        }
        if let Some(e) = protocol_error {
            let bytes = encode_value(&RespValue::Error(format!("ERR {e}")));
            if let Some(conn) = self.conns.get_mut(&conn_id) {
                conn.out.extend_from_slice(&bytes);
            }
            self.flush_conn(conn_id);
            self.close_conn(conn_id);
        }
    }

    // ------------------------------------------------------------------
    // Request dispatch
    // ------------------------------------------------------------------

    fn dispatch(&mut self, conn_id: u64, args: Vec<Vec<u8>>) {
        let seq = self.next_seq(conn_id);

        let Some(cmd) = command_for(&args) else {
            let msg = format!(
                "ERR unknown command '{}'",
                String::from_utf8_lossy(&args[0])
            );
            self.deliver(conn_id, seq, encode_value(&RespValue::Error(msg)));
            return;
        };
        // `XGROUP HELP` resolves to the hidden `_XGROUP_HELP` command (arity 2,
        // NOSCRIPT) before the arity check (command_registry.cc:347-352), so the
        // 2-arg HELP form bypasses XGROUP's -3 arity.
        let xgroup_help =
            cmd.name == "XGROUP" && args.len() == 2 && args[1].eq_ignore_ascii_case(b"HELP");
        if !xgroup_help && let Some(e) = cmd.check_arity(args.len()) {
            // A validation failure while collecting poisons the transaction so
            // EXEC will abort (EXECABORT). Unknown commands do not poison it.
            if let Some(conn) = self.conns.get_mut(&conn_id)
                && conn.multi.phase == MultiPhase::Collect
            {
                conn.multi.phase = MultiPhase::Error;
            }
            self.deliver(conn_id, seq, encode_value(&RespValue::Error(e)));
            return;
        }

        // A replica rejects writes from client connections (the replica's own
        // journal application bypasses `dispatch` entirely). Matches
        // `main_service.cc` `IsWriteCmd && !is_master && !is_replicating`.
        if self.is_replica() && cmd.has_flag(FLAG_WRITE) {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error(
                    "READONLY You can't write against a read only replica.".into(),
                )),
            );
            return;
        }

        // A MONITOR connection may only run RESET or QUIT
        // (`main_service.cc:1413-1414`).
        if self.conns.get(&conn_id).is_some_and(|c| c.monitor)
            && !matches!(cmd.name, "RESET" | "QUIT")
        {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error(
                    "Replica can't interact with the keyspace".into(),
                )),
            );
            return;
        }

        // Inside an open MULTI block everything except the exec-group commands
        // and RESET is queued (upstream `StoreInMultiBlock`).
        if self.in_multi(conn_id) && !matches!(cmd.name, "MULTI" | "EXEC" | "DISCARD" | "RESET") {
            self.queue_cmd(conn_id, seq, cmd, args);
            return;
        }

        // Register the slowlog start: the entry is produced when the reply for
        // this seq is flushed (`deliver`). Blocking commands are excluded
        // (`CO::BLOCKING` skips logging, conn_context.cc:341).
        if !cmd.has_flag(FLAG_BLOCKING)
            && let Some(conn) = self.conns.get_mut(&conn_id)
        {
            conn.pending_slowlog.insert(
                seq,
                PendingSlowlog {
                    name: cmd.name.to_string(),
                    args: args.clone(),
                    started: Instant::now(),
                    slowlog_args: None,
                },
            );
        }
        // Count the execution for INFO COMMANDSTATS (`UpdateCmdStatsMap`).
        server::bump_cmd_stat(&self.env.command_stats, cmd.name);

        // Feed live MONITOR connections; admin commands and EXEC are excluded
        // (`command_registry.cc` CAN_MONITOR). Queued MULTI commands are logged
        // when EXEC runs them (see `run_queued`).
        if !cmd.has_flag(FLAG_ADMIN) && cmd.name != "EXEC" {
            self.broadcast_monitor(conn_id, &args);
        }

        match cmd.name {
            "MULTI" => return self.local_multi(conn_id, seq),
            "EXEC" => return self.local_exec(conn_id, seq),
            "DISCARD" => return self.local_discard(conn_id, seq),
            "RESET" => return self.local_reset(conn_id, seq),
            "WATCH" => return self.local_watch(conn_id, seq, &args),
            "UNWATCH" => return self.local_unwatch(conn_id, seq),
            "SUBSCRIBE" => return self.local_subscribe(conn_id, seq, &args),
            "UNSUBSCRIBE" => return self.local_unsubscribe(conn_id, seq, &args),
            "PSUBSCRIBE" => return self.local_psubscribe(conn_id, seq, &args),
            "PUNSUBSCRIBE" => return self.local_punsubscribe(conn_id, seq, &args),
            "SSUBSCRIBE" => return self.local_ssubscribe(conn_id, seq, &args),
            "SUNSUBSCRIBE" => return self.local_sunsubscribe(conn_id, seq, &args),
            "QUIT" => return self.local_quit(conn_id, seq),
            _ => {}
        }
        if cmd.has_flag(FLAG_LOCAL) {
            // REPLCONF ACK is answered with silence: the reply is dropped but
            // the seq still advances so later replies drain in order.
            if cmd.name == "REPLCONF" {
                match self.replconf(conn_id, &args) {
                    Some(v) => self.deliver(conn_id, seq, encode_value(&v)),
                    None => self.deliver(conn_id, seq, Vec::new()),
                }
                self.tracking_seq_advance(conn_id);
                return;
            }
            if cmd.name == "DFLY" {
                self.dfly(conn_id, seq, &args);
                self.tracking_seq_advance(conn_id);
                return;
            }
            let v = self.handle_local(conn_id, cmd, &args);
            self.deliver(conn_id, seq, encode_value(&v));
            // The tracking sequence is bumped after the command's state changes:
            // `CLIENT TRACKING ON` (the increment sees tracking now enabled) and
            // `OFF` (no increment) behave exactly like the reference's
            // post-dispatch `IncrementSequenceNumber`.
            self.tracking_seq_advance(conn_id);
            return;
        }
        self.dispatch_keyed(conn_id, seq, cmd, &args);
        self.tracking_seq_advance(conn_id);
    }

    /// `IncrementSequenceNumber` (main_service.cc:1707): bump the tracking
    /// sequence of a connection that just ran a top-level (non-MULTI) command.
    /// The increment is gated on tracking being enabled, so it runs after the
    /// command's own `CLIENT TRACKING` state change. Queued MULTI commands run
    /// through `run_queued` and never reach this path; MULTI/EXEC/DISCARD
    /// return before it (`cid->IsMulti()`).
    fn tracking_seq_advance(&self, conn_id: u64) {
        self.env.tracking.lock().unwrap().inc_seq(conn_id);
    }

    /// Split a command by its keys and send it to a shard or the coordinator.
    fn dispatch_keyed(&self, conn_id: u64, seq: u64, cmd: &'static Command, args: &[Vec<u8>]) {
        // The EVAL/FCALL families run entirely on the coordinator (it owns the
        // Lua interpreter); the coordinator derives the declared keys itself.
        if is_eval_cmd(cmd.name) || is_function_cmd(cmd.name) {
            self.send_coord(conn_id, seq, args.to_vec(), vec![], vec![], 0);
            return;
        }
        if cmd.has_flag(FLAG_GLOBAL) {
            let shards: Vec<usize> = (0..self.env.num_shards).collect();
            self.send_coord(
                conn_id,
                seq,
                args.to_vec(),
                vec![],
                shards,
                cmd.key_range.first,
            );
            return;
        }

        let keys = extract_keys(cmd, args);
        if keys.is_empty() {
            // Malformed/movable-key command without keys: let the executor
            // validate and reply with an error from shard 0.
            self.send_single(conn_id, seq, 0, args.to_vec(), vec![]);
            return;
        }
        let per = keys_per_shard(args, &keys, self.env.num_shards);
        if per.len() == 1 && !cmd.has_flag(FLAG_BLOCKING) {
            self.send_single(conn_id, seq, per[0].0, args.to_vec(), per[0].1.clone());
        } else {
            let shards: Vec<usize> = per.iter().map(|(s, _)| *s).collect();
            self.send_coord(
                conn_id,
                seq,
                args.to_vec(),
                keys,
                shards,
                cmd.key_range.first,
            );
        }
    }

    /// Run a command queued inside a MULTI block. Its reply is delivered with a
    /// fresh seq so the EXEC array header and all replies stay in order.
    fn run_queued(&mut self, conn_id: u64, args: &[Vec<u8>]) {
        let seq = self.next_seq(conn_id);
        let Some(cmd) = command_for(args) else {
            let bytes = encode_value(&RespValue::Error("ERR unknown command".into()));
            self.deliver(conn_id, seq, bytes);
            return;
        };
        if !cmd.has_flag(FLAG_BLOCKING)
            && let Some(conn) = self.conns.get_mut(&conn_id)
        {
            conn.pending_slowlog.insert(
                seq,
                PendingSlowlog {
                    name: cmd.name.to_string(),
                    args: args.to_vec(),
                    started: Instant::now(),
                    slowlog_args: None,
                },
            );
        }
        if cmd.name == "UNWATCH" {
            self.local_unwatch(conn_id, seq);
            return;
        }
        // Commands executed through EXEC are monitored as they run.
        if !cmd.has_flag(FLAG_ADMIN) && cmd.name != "EXEC" {
            self.broadcast_monitor(conn_id, args);
        }
        server::bump_cmd_stat(&self.env.command_stats, cmd.name);
        if cmd.has_flag(FLAG_LOCAL) {
            if cmd.name == "REPLCONF" {
                match self.replconf(conn_id, args) {
                    Some(v) => self.deliver(conn_id, seq, encode_value(&v)),
                    None => self.deliver(conn_id, seq, Vec::new()),
                }
                return;
            }
            if cmd.name == "DFLY" {
                self.dfly(conn_id, seq, args);
                return;
            }
            let v = self.handle_local(conn_id, cmd, args);
            self.deliver(conn_id, seq, encode_value(&v));
            return;
        }
        self.dispatch_keyed(conn_id, seq, cmd, args);
    }

    fn next_seq(&mut self, conn_id: u64) -> u64 {
        let conn = self.conns.get_mut(&conn_id).unwrap();
        let seq = conn.dispatch_seq;
        conn.dispatch_seq += 1;
        seq
    }

    fn in_multi(&self, conn_id: u64) -> bool {
        self.conns
            .get(&conn_id)
            .is_some_and(|c| c.multi.phase == MultiPhase::Collect)
    }

    /// Whether the connection is inside a MULTI block or currently executing
    /// its queued commands, i.e. blocking commands must reply nil.
    fn no_block(&self, conn_id: u64) -> bool {
        self.conns
            .get(&conn_id)
            .is_some_and(|c| c.multi.phase == MultiPhase::Collect || c.exec_multi)
    }

    fn handle_local(&mut self, conn_id: u64, cmd: &Command, args: &[Vec<u8>]) -> RespValue {
        // Single-reply pub/sub commands reach this path both from `dispatch`
        // (FLAG_LOCAL) and from `run_queued` when executed inside EXEC.
        match cmd.name {
            "ROLE" => return replica::role_reply(&self.replica_status.lock().unwrap()),
            "REPLICAOF" | "SLAVEOF" => {
                return match server::parse_replicaof(args) {
                    Ok(server::ReplicaOf::NoOne) => {
                        self.stop_replica();
                        RespValue::Simple("OK".into())
                    }
                    Ok(server::ReplicaOf::Start { host, port }) => {
                        self.start_replica(&host, port);
                        RespValue::Simple("OK".into())
                    }
                    Err(e) => e,
                };
            }
            "PUBLISH" => return self.local_publish(args),
            "SPUBLISH" => return self.local_spublish(args),
            "PUBSUB" => return self.local_pubsub(args),
            "MONITOR" => {
                // Register the connection and reply +OK (`ChangeMonitor(true)`).
                if let Some(conn) = self.conns.get_mut(&conn_id) {
                    conn.monitor = true;
                }
                if !self.monitors.contains(&conn_id) {
                    self.monitors.push(conn_id);
                }
                return RespValue::Simple("OK".into());
            }
            "SHUTDOWN" => {
                // Grammar validation happens first; a valid SHUTDOWN stops the
                // run loop once this +OK reply has been flushed.
                return match server::local_shutdown(args) {
                    Ok(()) => {
                        self.shutting_down = true;
                        RespValue::Simple("OK".into())
                    }
                    Err(e) => e,
                };
            }
            "SLOWLOG" => return self.local_slowlog(args),
            "CONFIG" => return self.local_config(args),
            "CLIENT" => return self.local_client(args, conn_id),
            _ => {}
        }
        if cmd.name == "SELECT" {
            let v = server::local_select(args);
            if matches!(&v, RespValue::Simple(_))
                && let (Some(db), Some(conn)) = (
                    args.get(1).and_then(|a| crate::util::parse_i64(a)),
                    self.conns.get_mut(&conn_id),
                )
            {
                conn.db_idx = db as usize;
            }
            return v;
        }
        if cmd.name == "HELLO" {
            // The negotiated protocol version is captured for RESP3 gating
            // (client tracking). HELLO 3 returns a map response.
            let v = server::local_hello(args);
            let resp3 = matches!(args.get(1).and_then(|a| crate::util::parse_i64(a)), Some(3));
            if let Some(conn) = self.conns.get_mut(&conn_id) {
                conn.resp3 = resp3;
            }
            return v;
        }
        // While subscribed in RESP2, PING echoes the message inside a
        // `["pong", msg]` array instead of a plain bulk reply
        // (`GenericFamily::Ping`).
        if cmd.name == "PING" && self.conns.get(&conn_id).is_some_and(|c| !c.sub.is_empty()) {
            let msg = args.get(1).map_or(&b""[..], |a| a.as_slice());
            return pubsub::ping_pubsub(msg);
        }
        self.run_local(cmd, args)
    }

    // ------------------------------------------------------------------
    // MULTI / EXEC / DISCARD / RESET
    // ------------------------------------------------------------------

    fn local_multi(&mut self, conn_id: u64, seq: u64) {
        let phase = self
            .conns
            .get(&conn_id)
            .map_or(MultiPhase::Inactive, |c| c.multi.phase);
        if phase == MultiPhase::Collect {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error(
                    "ERR MULTI calls can not be nested".into(),
                )),
            );
            return;
        }
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.multi = MultiState::default();
            conn.multi.phase = MultiPhase::Collect;
        }
        self.deliver(conn_id, seq, encode_value(&RespValue::Simple("OK".into())));
    }

    fn local_discard(&mut self, conn_id: u64, seq: u64) {
        let phase = self
            .conns
            .get(&conn_id)
            .map_or(MultiPhase::Inactive, |c| c.multi.phase);
        // Upstream `MultiCleanup` runs before the IsInMulti check, so DISCARD
        // outside a MULTI block still unwatches all keys and drains the queue.
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.multi = MultiState::default();
            conn.watched.clear();
            conn.watched_dirty = false;
        }
        if phase == MultiPhase::Inactive {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("ERR DISCARD without MULTI".into())),
            );
            return;
        }
        self.deliver(conn_id, seq, encode_value(&RespValue::Simple("OK".into())));
    }

    /// `WATCH key...`: snapshot each key's state on its shard and record it on
    /// the connection. Replies OK. Mirrors upstream `Service::Watch`: if the
    /// connection is already marked dirty (a watched key changed since the
    /// last clear) the command is a no-op that will make EXEC abort.
    fn local_watch(&mut self, conn_id: u64, seq: u64, args: &[Vec<u8>]) {
        let (db_idx, already_dirty) = {
            let Some(conn) = self.conns.get(&conn_id) else {
                return;
            };
            (conn.db_idx, conn.watched_dirty)
        };
        if already_dirty {
            self.deliver(conn_id, seq, encode_value(&RespValue::Simple("OK".into())));
            return;
        }
        let new_keys: Vec<Vec<u8>> = args[1..].to_vec();
        // Re-verify every previously watched key against its stored snapshot:
        // if any changed, latch the dirty flag so EXEC aborts (upstream marks
        // the key dirty on every update and refuses to re-register afterwards).
        let mut dirty = false;
        let existing: Vec<Vec<u8>> = {
            let Some(conn) = self.conns.get(&conn_id) else {
                return;
            };
            conn.watched.iter().map(|w| w.key.clone()).collect()
        };
        if !existing.is_empty() {
            let states = self.watch_snapshot(&existing, db_idx);
            let by_key: HashMap<&[u8], &WatchState> =
                states.iter().map(|(k, s)| (k.as_slice(), s)).collect();
            let conn = self.conns.get(&conn_id).unwrap();
            dirty = conn.watched.iter().any(|w| {
                by_key.get(w.key.as_slice()).is_none_or(|s| {
                    s.version != w.state.version
                        || s.existed != w.state.existed
                        || s.db_epoch != w.state.db_epoch
                })
            });
        }
        let states = self.watch_snapshot(&new_keys, db_idx);
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.watched_dirty |= dirty;
            if !dirty {
                for (key, state) in states {
                    conn.watched.push(WatchedKey {
                        db: db_idx,
                        key,
                        state,
                    });
                }
            }
        }
        self.deliver(conn_id, seq, encode_value(&RespValue::Simple("OK".into())));
    }

    fn local_unwatch(&mut self, conn_id: u64, seq: u64) {
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.watched.clear();
            conn.watched_dirty = false;
        }
        self.deliver(conn_id, seq, encode_value(&RespValue::Simple("OK".into())));
    }

    fn local_exec(&mut self, conn_id: u64, seq: u64) {
        let (phase, queue, watched, dirty) = {
            let Some(conn) = self.conns.get_mut(&conn_id) else {
                return;
            };
            (
                conn.multi.phase,
                std::mem::take(&mut conn.multi.queue),
                std::mem::take(&mut conn.watched),
                std::mem::replace(&mut conn.watched_dirty, false),
            )
        };
        if phase == MultiPhase::Inactive {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("EXEC without MULTI".into())),
            );
            return;
        }
        if phase == MultiPhase::Error {
            if let Some(conn) = self.conns.get_mut(&conn_id) {
                conn.multi = MultiState::default();
            }
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error(
                    "EXECABORT Transaction discarded because of previous errors".into(),
                )),
            );
            return;
        }

        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.multi = MultiState::default();
        }
        // Watch guards: a nil reply aborts the transaction (upstream
        // `watched_dirty` / `CheckWatchedKeyExpiry`).
        if !watched.is_empty() || dirty {
            let db_idx = self.conns.get(&conn_id).map_or(0, |c| c.db_idx);
            if watched.iter().any(|w| w.db != db_idx) {
                self.deliver(
                    conn_id,
                    seq,
                    encode_value(&RespValue::Error(
                        "Dragonfly does not allow WATCH and EXEC on different databases".into(),
                    )),
                );
                return;
            }
            if dirty {
                self.deliver(conn_id, seq, encode_value(&RespValue::Nil));
                return;
            }
            let keys: Vec<Vec<u8>> = watched.iter().map(|w| w.key.clone()).collect();
            let states = self.watch_snapshot(&keys, db_idx);
            let by_key: HashMap<&[u8], &WatchState> =
                states.iter().map(|(k, s)| (k.as_slice(), s)).collect();
            let is_dirty = watched.iter().any(|w| {
                by_key.get(w.key.as_slice()).is_none_or(|s| {
                    s.version != w.state.version
                        || s.existed != w.state.existed
                        || s.db_epoch != w.state.db_epoch
                })
            });
            if is_dirty {
                self.deliver(conn_id, seq, encode_value(&RespValue::Nil));
                return;
            }
        }
        // The header plus each queued command's reply, delivered in seq order,
        // concatenate into the EXEC RESP array. The header reply carries the
        // EXEC slowlog metadata (`FormatExecSlowlog`).
        let is_write = queue
            .iter()
            .any(|q| command_for(q).is_some_and(|c| c.has_flag(FLAG_WRITE)));
        let exec_slowlog_args = format_exec_slowlog(queue.len(), is_write);
        let header = format!("*{}\r\n", queue.len()).into_bytes();
        self.deliver_with_args(conn_id, seq, header, Some(exec_slowlog_args));
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.exec_multi = true;
        }
        for args in queue {
            self.run_queued(conn_id, &args);
        }
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.exec_multi = false;
        }
    }

    fn local_reset(&mut self, conn_id: u64, seq: u64) {
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.multi = MultiState::default();
            conn.watched.clear();
            conn.watched_dirty = false;
            conn.db_idx = 0;
            conn.sub = SubscribeInfo::default();
            conn.monitor = false;
            // RESET drops client tracking and re-negotiates the protocol
            // (`ConnectionContext::Reset`).
            conn.resp3 = false;
        }
        self.env
            .tracking
            .lock()
            .unwrap()
            .set_enabled(conn_id, false);
        self.pubsub.remove_conn(conn_id);
        self.monitors.retain(|&c| c != conn_id);
        self.deliver(
            conn_id,
            seq,
            encode_value(&RespValue::Simple("RESET".into())),
        );
    }

    /// QUIT: reply `+OK`, then close once the reply is flushed (upstream
    /// `Service::Quit`).
    fn local_quit(&mut self, conn_id: u64, seq: u64) {
        self.deliver(conn_id, seq, encode_value(&RespValue::Simple("OK".into())));
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.closing = true;
        }
    }

    // ------------------------------------------------------------------
    // Pub/sub
    // ------------------------------------------------------------------

    /// SUBSCRIBE ch...: register each channel and reply with one
    /// `[subscribe, ch, count]` array per channel. Each channel gets its own
    /// top-level reply, mirroring upstream's pipeline-breaking behavior.
    fn local_subscribe(&mut self, conn_id: u64, seq: u64, args: &[Vec<u8>]) {
        for (seq, ch) in (seq..).zip(args[1..].iter()) {
            self.pubsub.subscribe(ch, conn_id);
            if let Some(conn) = self.conns.get_mut(&conn_id) {
                conn.sub.channels.insert(ch.clone());
            }
            let count = self.conns.get(&conn_id).map_or(0, |c| c.sub.count());
            let reply = encode_value(&pubsub::sub_change("subscribe", Some(ch), count));
            self.deliver(conn_id, seq, reply);
        }
    }

    /// UNSUBSCRIBE [ch...]: the no-arg form unsubscribes from every channel the
    /// connection owns; with nothing subscribed it still emits
    /// `[unsubscribe, nil, 0]`.
    fn local_unsubscribe(&mut self, conn_id: u64, seq: u64, args: &[Vec<u8>]) {
        let channels: Vec<Vec<u8>> = if args.len() > 1 {
            args[1..].to_vec()
        } else {
            self.conns
                .get(&conn_id)
                .map(|c| c.sub.channels.iter().cloned().collect())
                .unwrap_or_default()
        };
        if channels.is_empty() {
            let reply = encode_value(&pubsub::sub_change("unsubscribe", None, 0));
            self.deliver(conn_id, seq, reply);
            return;
        }
        for (seq, ch) in (seq..).zip(channels) {
            self.pubsub.unsubscribe(&ch, conn_id);
            if let Some(conn) = self.conns.get_mut(&conn_id) {
                conn.sub.channels.remove(&ch);
            }
            let count = self.conns.get(&conn_id).map_or(0, |c| c.sub.count());
            let reply = encode_value(&pubsub::sub_change("unsubscribe", Some(&ch), count));
            self.deliver(conn_id, seq, reply);
        }
    }

    fn local_psubscribe(&mut self, conn_id: u64, seq: u64, args: &[Vec<u8>]) {
        for (seq, pat) in (seq..).zip(args[1..].iter()) {
            self.pubsub.psubscribe(pat, conn_id);
            if let Some(conn) = self.conns.get_mut(&conn_id) {
                conn.sub.patterns.insert(pat.clone());
            }
            let count = self.conns.get(&conn_id).map_or(0, |c| c.sub.count());
            let reply = encode_value(&pubsub::sub_change("psubscribe", Some(pat), count));
            self.deliver(conn_id, seq, reply);
        }
    }

    fn local_punsubscribe(&mut self, conn_id: u64, seq: u64, args: &[Vec<u8>]) {
        let patterns: Vec<Vec<u8>> = if args.len() > 1 {
            args[1..].to_vec()
        } else {
            self.conns
                .get(&conn_id)
                .map(|c| c.sub.patterns.iter().cloned().collect())
                .unwrap_or_default()
        };
        if patterns.is_empty() {
            let reply = encode_value(&pubsub::sub_change("punsubscribe", None, 0));
            self.deliver(conn_id, seq, reply);
            return;
        }
        for (seq, pat) in (seq..).zip(patterns) {
            self.pubsub.punsubscribe(&pat, conn_id);
            if let Some(conn) = self.conns.get_mut(&conn_id) {
                conn.sub.patterns.remove(&pat);
            }
            let count = self.conns.get(&conn_id).map_or(0, |c| c.sub.count());
            let reply = encode_value(&pubsub::sub_change("punsubscribe", Some(&pat), count));
            self.deliver(conn_id, seq, reply);
        }
    }

    /// Shard pub/sub (SSUBSCRIBE/SUNSUBSCRIBE/SPUBLISH). In non-cluster mode
    /// these behave like their regular counterparts with "ssubscribe" /
    /// "sunsubscribe" / "smessage" reply types (upstream only gates them on
    /// `IsClusterEnabled()`).
    fn local_ssubscribe(&mut self, conn_id: u64, seq: u64, args: &[Vec<u8>]) {
        for (seq, ch) in (seq..).zip(args[1..].iter()) {
            self.pubsub.ssubscribe(ch, conn_id);
            if let Some(conn) = self.conns.get_mut(&conn_id) {
                conn.sub.sharded.insert(ch.clone());
            }
            let count = self.conns.get(&conn_id).map_or(0, |c| c.sub.count());
            let reply = encode_value(&pubsub::sub_change("ssubscribe", Some(ch), count));
            self.deliver(conn_id, seq, reply);
        }
    }

    fn local_sunsubscribe(&mut self, conn_id: u64, seq: u64, args: &[Vec<u8>]) {
        let channels: Vec<Vec<u8>> = if args.len() > 1 {
            args[1..].to_vec()
        } else {
            self.conns
                .get(&conn_id)
                .map(|c| c.sub.sharded.iter().cloned().collect())
                .unwrap_or_default()
        };
        if channels.is_empty() {
            let reply = encode_value(&pubsub::sub_change("sunsubscribe", None, 0));
            self.deliver(conn_id, seq, reply);
            return;
        }
        for (seq, ch) in (seq..).zip(channels) {
            self.pubsub.sunsubscribe(&ch, conn_id);
            if let Some(conn) = self.conns.get_mut(&conn_id) {
                conn.sub.sharded.remove(&ch);
            }
            let count = self.conns.get(&conn_id).map_or(0, |c| c.sub.count());
            let reply = encode_value(&pubsub::sub_change("sunsubscribe", Some(&ch), count));
            self.deliver(conn_id, seq, reply);
        }
    }

    /// PUBLISH ch msg: append a `["message", ch, msg]` frame to every
    /// subscriber's output (patterns emit `["pmessage", pat, ch, msg]`) and
    /// return the number of subscriber connections that were notified.
    fn local_publish(&mut self, args: &[Vec<u8>]) -> RespValue {
        let channel = args[1].clone();
        let message = args[2].clone();
        let targets = self.pubsub.subscribers(&channel);
        for (target, pattern) in &targets {
            let frame = encode_value(&pubsub::push_message(
                pattern.as_deref(),
                &channel,
                &message,
                false,
            ));
            if let Some(conn) = self.conns.get_mut(target) {
                conn.out.extend_from_slice(&frame);
            }
        }
        RespValue::Integer(targets.len() as i64)
    }

    /// SPUBLISH ch msg: like PUBLISH but only notifies shard-channel
    /// subscribers, emitting `["smessage", ch, msg]` frames. In non-cluster
    /// mode the command is fully supported (upstream only gates it on
    /// `IsClusterEnabled()`).
    fn local_spublish(&mut self, args: &[Vec<u8>]) -> RespValue {
        let channel = args[1].clone();
        let message = args[2].clone();
        let targets = self.pubsub.sharded_subscribers(&channel);
        for &target in &targets {
            let frame = encode_value(&pubsub::push_message(None, &channel, &message, true));
            if let Some(conn) = self.conns.get_mut(&target) {
                conn.out.extend_from_slice(&frame);
            }
        }
        RespValue::Integer(targets.len() as i64)
    }

    /// PUBSUB introspection (CHANNELS/NUMSUB/NUMPAT/SHARD*/HELP).
    fn local_pubsub(&mut self, args: &[Vec<u8>]) -> RespValue {
        match pubsub::pubsub_command(args, &self.pubsub) {
            Ok(v) => v,
            Err(e) => RespValue::Error(e),
        }
    }

    /// Blocking read of every watched key's state across its shards, under the
    /// shard lock (queued behind an active transaction like a single op).
    fn watch_snapshot(&self, keys: &[Vec<u8>], db_idx: usize) -> Vec<(Vec<u8>, WatchState)> {
        let mut by_shard: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for (i, k) in keys.iter().enumerate() {
            by_shard
                .entry(crate::util::shard_for_key(k, self.env.num_shards))
                .or_default()
                .push(i);
        }
        let mut out: Vec<(Vec<u8>, WatchState)> = keys
            .iter()
            .map(|k| {
                (
                    k.clone(),
                    WatchState {
                        version: 0,
                        existed: false,
                        db_epoch: 0,
                    },
                )
            })
            .collect();
        for (shard, idxs) in by_shard {
            let ks: Vec<Vec<u8>> = idxs.iter().map(|&i| keys[i].clone()).collect();
            let (tx, rx) = mpsc::channel();
            if self.env.shard_txs[shard]
                .send(ShardMsg::WatchQuery {
                    keys: ks,
                    db_idx,
                    result_tx: tx,
                })
                .is_ok()
                && let Ok(states) = rx.recv()
            {
                for (&i, (_, state)) in idxs.iter().zip(states.iter()) {
                    out[i].1 = state.clone();
                }
            }
        }
        out
    }

    /// Validate and queue a command arriving inside a MULTI block.
    fn queue_cmd(&mut self, conn_id: u64, seq: u64, cmd: &Command, args: Vec<Vec<u8>>) {
        // These are forbidden inside a transaction and poison it
        // (upstream `VerifyCommandState`).
        if matches!(
            cmd.name,
            "WATCH"
                | "FLUSHALL"
                | "FLUSHDB"
                | "SUBSCRIBE"
                | "UNSUBSCRIBE"
                | "PSUBSCRIBE"
                | "PUNSUBSCRIBE"
                | "SSUBSCRIBE"
                | "SUNSUBSCRIBE"
        ) {
            let msg = format!("ERR '{}' not allowed inside a transaction", cmd.name);
            if let Some(conn) = self.conns.get_mut(&conn_id) {
                conn.multi.phase = MultiPhase::Error;
            }
            self.deliver(conn_id, seq, encode_value(&RespValue::Error(msg)));
            return;
        }
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.multi.queue.push(args);
        }
        self.deliver(
            conn_id,
            seq,
            encode_value(&RespValue::Simple("QUEUED".into())),
        );
    }

    /// SCRIPT subcommands against the shared script cache (see
    /// `server::local_script`). GC runs a real collection on the coordinator's
    /// interpreter (`ScriptMgr::GCCmd`), so it is routed there; the ack comes
    /// back over a one-shot channel.
    fn local_script(&self, args: &[Vec<u8>]) -> RespValue {
        if args.get(1).is_some_and(|a| a.eq_ignore_ascii_case(b"GC")) {
            let (ack_tx, ack_rx) = mpsc::channel();
            if self.env.gc_tx.send(GcRequest { ack: ack_tx }).is_ok() {
                // Block until the coordinator finished collecting; the reply is
                // `+OK` either way (`SendOk`).
                let _ = ack_rx.recv_timeout(Duration::from_secs(10));
            }
            return RespValue::Simple("OK".into());
        }
        local_script(&mut self.env.script_mgr.lock().unwrap(), args)
    }

    /// FUNCTION subcommands against the shared library registry (see
    /// `server::local_function`).
    fn local_function(&self, args: &[Vec<u8>]) -> RespValue {
        local_function(&mut self.env.script_mgr.lock().unwrap(), args)
    }

    /// SLOWLOG subcommands against the IO thread's ring (`ServerFamily::SlowLog`).
    fn local_slowlog(&mut self, args: &[Vec<u8>]) -> RespValue {
        if args.len() < 2 {
            return RespValue::Error("ERR wrong number of arguments for 'slowlog' command".into());
        }
        match args[1].to_ascii_uppercase().as_slice() {
            b"HELP" => RespValue::Array(vec![
                RespValue::Simple(
                    "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".into(),
                ),
                RespValue::Simple("GET [<count>]".into()),
                RespValue::Simple(
                    "    Return top <count> entries from the slowlog (default: 10, -1 mean all)."
                        .into(),
                ),
                RespValue::Simple("    Entries are made of:".into()),
                RespValue::Simple(
                    "    id, timestamp, time in microseconds, arguments array, client IP and port,"
                        .into(),
                ),
                RespValue::Simple("    client name".into()),
                RespValue::Simple("LEN".into()),
                RespValue::Simple("    Return the length of the slowlog.".into()),
                RespValue::Simple("RESET".into()),
                RespValue::Simple("    Reset the slowlog.".into()),
                RespValue::Simple("HELP".into()),
                RespValue::Simple("    Prints this help.".into()),
            ]),
            b"LEN" => RespValue::Integer(self.slow_log.len() as i64),
            b"RESET" => {
                self.slow_log.reset();
                RespValue::Simple("OK".into())
            }
            b"GET" => self.slowlog_get(args),
            other => RespValue::Error(slowlog_unknown_subcmd(other)),
        }
    }

    /// `SlowLogGet` (server_family.cc:1027): parse the optional count, then
    /// snapshot the ring (newest first, limited to the count).
    fn slowlog_get(&self, args: &[Vec<u8>]) -> RespValue {
        // args = ["SLOWLOG", "GET"[, count]]: 4+ arguments is a parse error.
        if args.len() > 3 {
            return RespValue::Error(slowlog_unknown_subcmd(b"GET"));
        }
        let mut requested = u32::MAX as u64;
        if args.len() == 3 {
            match crate::util::parse_i64(&args[2]) {
                Some(n) if n >= -1 => {
                    if n >= 0 {
                        requested = n as u64;
                    }
                }
                _ => {
                    return RespValue::Error(
                        "ERR count should be greater than or equal to -1".into(),
                    );
                }
            }
        }
        RespValue::Array(
            self.slow_log
                .snapshot(requested)
                .iter()
                .map(SlowLogEntry::into_resp)
                .collect(),
        )
    }

    /// CONFIG subcommands against the IO thread's runtime knobs
    /// (`ServerFamily::ConfigSet` / `ConfigGet`). The slowlog parameters map
    /// onto the thread-local ring.
    fn local_config(&mut self, args: &[Vec<u8>]) -> RespValue {
        if args.len() < 2 {
            return RespValue::Error("ERR wrong number of arguments for 'config' command".into());
        }
        match args[1].to_ascii_uppercase().as_slice() {
            b"SET" => self.config_set(args),
            b"GET" => self.config_get(args),
            b"RESETSTAT" => {
                server::reset_cmd_stats(&self.env.command_stats);
                RespValue::Simple("OK".into())
            }
            _ => RespValue::Error(
                "ERR Unknown CONFIG subcommand or wrong number of arguments for 'config' command"
                    .into(),
            ),
        }
    }

    fn config_set(&mut self, args: &[Vec<u8>]) -> RespValue {
        if args.len() != 4 {
            return RespValue::Error(
                "ERR wrong number of arguments for 'config|set' command".into(),
            );
        }
        // Config names accept dashes and underscores interchangeably
        // (`ConfigNormalization`): normalize to the canonical underscore form.
        let name = normalize_config_name(&args[2]);
        match name.as_str() {
            // `RegisterMutable("slowlog_max_len")` with the setter below.
            "slowlog_max_len" => match crate::util::parse_i64(&args[3]) {
                Some(n) if n >= 0 => {
                    self.slow_log.change_length(n as usize);
                    RespValue::Simple("OK".into())
                }
                _ => RespValue::Error(format!("ERR Invalid config parameter '{name}'")),
            },
            "slowlog_log_slower_than" => match crate::util::parse_i64(&args[3]) {
                Some(n) => {
                    self.slow_log.set_threshold(n);
                    RespValue::Simple("OK".into())
                }
                None => RespValue::Error(format!("ERR Invalid config parameter '{name}'")),
            },
            "replica_priority" => match crate::util::parse_i64(&args[3]) {
                Some(n) if n >= 0 => {
                    self.replica_priority = n;
                    RespValue::Simple("OK".into())
                }
                _ => RespValue::Error(format!("ERR Invalid config parameter '{name}'")),
            },
            "maxmemory" => match parse_human_size(&args[3]) {
                Some(n) => {
                    self.maxmemory = n;
                    RespValue::Simple("OK".into())
                }
                None => RespValue::Error(format!("ERR Invalid config parameter '{name}'")),
            },
            _ => RespValue::Simple("OK".into()),
        }
    }

    fn config_get(&self, args: &[Vec<u8>]) -> RespValue {
        if args.len() < 3 {
            return RespValue::Error(
                "ERR wrong number of arguments for 'config|get' command".into(),
            );
        }
        // The pattern is normalized the same way as parameter names.
        let pattern = normalize_config_name(&args[2]);
        let mut out = Vec::new();
        let push = |name: &str, value: String, out: &mut Vec<RespValue>| {
            if glob_match(pattern.as_bytes(), name.as_bytes()) {
                out.push(RespValue::Bulk(name.as_bytes().to_vec()));
                out.push(RespValue::Bulk(value.into_bytes()));
            }
        };
        push(
            "slowlog_max_len",
            self.slow_log.max_len().to_string(),
            &mut out,
        );
        push(
            "slowlog_log_slower_than",
            self.slow_log.log_slower_than().to_string(),
            &mut out,
        );
        push(
            "replica_priority",
            self.replica_priority.to_string(),
            &mut out,
        );
        push("maxmemory", self.maxmemory.to_string(), &mut out);
        RespValue::Array(out)
    }

    /// CLIENT subcommands against the IO thread's connection table
    /// (`ServerFamily::Client`): `LIST` filters connections, `INFO` reports the
    /// calling connection.
    fn local_client(&self, args: &[Vec<u8>], conn_id: u64) -> RespValue {
        if args.len() < 2 {
            return RespValue::Error("ERR wrong number of arguments for 'client' command".into());
        }
        match args[1].to_ascii_uppercase().as_slice() {
            b"LIST" => self.client_list(args),
            // `ClientInfo` (server_family.cc:391): extra arguments are rejected.
            b"INFO" if args.len() == 2 => self
                .conns
                .get(&conn_id)
                .map_or(RespValue::Bulk(Vec::new()), |conn| {
                    RespValue::Bulk(client_info_line(conn).into_bytes())
                }),
            b"INFO" => RespValue::Error("ERR syntax error".into()),
            b"SETNAME" | b"SETINFO" | b"NO-EVICT" | b"NO-TOUCH" => RespValue::Simple("OK".into()),
            b"GETNAME" => RespValue::Bulk(Vec::new()),
            b"TRACKING" => self.local_tracking(args, conn_id),
            b"CACHING" => self.local_caching(args, conn_id),
            b"ID" => self
                .conns
                .get(&conn_id)
                .map_or(RespValue::Integer(0), |c| RespValue::Integer(c.id as i64)),
            _ => RespValue::Error(
                "ERR Unknown CLIENT subcommand or wrong number of arguments for 'client' command"
                    .into(),
            ),
        }
    }

    /// `ClientTracking` (server_family.cc:513). RESP2 is rejected; the only
    /// subcommand options are OPTIN/OPTOUT/NOLOOP. The tracking state lives in
    /// the shared `Tracking` table.
    fn local_tracking(&self, args: &[Vec<u8>], conn_id: u64) -> RespValue {
        if !self.conns.get(&conn_id).is_some_and(|c| c.resp3) {
            return RespValue::Error(
                "ERR Client tracking is currently not supported for RESP2. Please use RESP3."
                    .into(),
            );
        }
        let sub = &args[2..];
        if sub.is_empty() || sub.len() > 3 {
            return RespValue::Error("ERR syntax error".into());
        }
        let is_on = if sub[0].eq_ignore_ascii_case(b"ON") {
            true
        } else if sub[0].eq_ignore_ascii_case(b"OFF") {
            false
        } else {
            return RespValue::Error("ERR syntax error".into());
        };
        let mut mode = TrackingMode::None;
        let mut noloop = false;
        if sub.len() >= 2 {
            if sub[1].eq_ignore_ascii_case(b"OPTIN") {
                mode = TrackingMode::OptIn;
            } else if sub[1].eq_ignore_ascii_case(b"OPTOUT") {
                mode = TrackingMode::OptOut;
            } else if sub[1].eq_ignore_ascii_case(b"NOLOOP") {
                noloop = true;
            } else {
                return RespValue::Error("ERR syntax error".into());
            }
        }
        if sub.len() == 3 {
            if !noloop && sub[2].eq_ignore_ascii_case(b"NOLOOP") {
                noloop = true;
            } else {
                return RespValue::Error("ERR syntax error".into());
            }
        }
        let mut tracking = self.env.tracking.lock().unwrap();
        tracking.set_enabled(conn_id, is_on);
        tracking.set_mode(conn_id, mode);
        tracking.set_noloop(conn_id, noloop);
        RespValue::Simple("OK".into())
    }

    /// `ClientCaching` (server_family.cc:564). The captured `caching_seq_num`
    /// is `seq - 1` inside a MULTI/EXEC block so the seq advance of the block's
    /// first read makes it tracked.
    fn local_caching(&self, args: &[Vec<u8>], conn_id: u64) -> RespValue {
        if !self.conns.get(&conn_id).is_some_and(|c| c.resp3) {
            return RespValue::Error(
                "ERR Client caching is currently not supported for RESP2. Please use RESP3.".into(),
            );
        }
        if args.len() != 3 {
            return RespValue::Error("ERR syntax error".into());
        }
        let mut tracking = self.env.tracking.lock().unwrap();
        let Some(c) = tracking.conn(conn_id) else {
            return RespValue::Error(
                "ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled".into(),
            );
        };
        if !c.enabled {
            return RespValue::Error(
                "ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled".into(),
            );
        }
        let yes = args[2].eq_ignore_ascii_case(b"YES");
        if yes {
            if c.mode != TrackingMode::OptIn {
                return RespValue::Error(
                    "ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode"
                        .into(),
                );
            }
        } else if args[2].eq_ignore_ascii_case(b"NO") {
            if c.mode != TrackingMode::OptOut {
                return RespValue::Error(
                    "ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode"
                        .into(),
                );
            }
        } else {
            return RespValue::Error("ERR syntax error".into());
        }
        // Queued CACHING commands run inside EXEC, where `exec_multi` carries
        // the flag after the `multi` state was reset (`tx->IsMulti()`).
        let is_multi = self.conns.get(&conn_id).is_some_and(|c| c.exec_multi);
        tracking.set_caching(conn_id, is_multi);
        RespValue::Simple("OK".into())
    }

    /// `ClientList` (server_family.cc:466): one info line per matching
    /// connection, joined with newlines and sent as a (verbatim) bulk string.
    /// The `TYPE` filter is checked first against the `normal`/`replica`/
    /// `pubsub` classes; `master` only matches replication link entries, which
    /// a standalone server has none of.
    fn client_list(&self, args: &[Vec<u8>]) -> RespValue {
        let filter = match parse_client_list_filter(args) {
            Ok(f) => f,
            Err(e) => return e,
        };
        let mut lines: Vec<String> = Vec::new();
        if filter.kind != Some(ClientListType::Master) {
            for conn in self.conns.values() {
                if !filter.ids.is_empty() && !filter.ids.contains(&(conn.id as u32)) {
                    continue;
                }
                if let Some(kind) = &filter.kind
                    && classify_conn(conn) != *kind
                {
                    continue;
                }
                lines.push(client_info_line(conn));
            }
        }
        let mut result = lines.join("\n");
        if !result.is_empty() {
            result.push('\n');
        }
        RespValue::Bulk(result.into_bytes())
    }

    fn run_local(&self, cmd: &Command, args: &[Vec<u8>]) -> RespValue {
        match cmd.name {
            "PING" => server::local_ping(args),
            "ECHO" => server::local_echo(args),
            "SELECT" => server::local_select(args),
            "AUTH" => server::local_auth(args),
            "COMMAND" => server::local_command(args),
            "HELLO" => server::local_hello(args),
            "CONFIG" => server::local_config(args),
            "TIME" => server::local_time(args),
            "LASTSAVE" => server::local_lastsave(args),
            "LATENCY" => server::local_latency(args),
            "WAIT" => server::local_wait(args),
            "ADDREPLICAOF" => server::local_addreplicaof(args),
            "REPLTAKEOVER" => server::local_repltakeover(args),
            "MODULE" => server::local_module(args),
            "FUNCTION" => self.local_function(args),
            "SCRIPT" => self.local_script(args),
            _ => RespValue::Error("ERR internal: unhandled local command".into()),
        }
    }

    // ------------------------------------------------------------------
    // Replication (replica side)
    // ------------------------------------------------------------------

    /// Whether this server currently plays the replica role (any phase other
    /// than Master), which gates writes from client connections.
    fn is_replica(&self) -> bool {
        let status = self.replica_status.lock().unwrap();
        status.phase != ReplicaPhase::Master
    }

    /// `REPLICAOF <host> <port>`: stop any previous session, then start a new
    /// replica thread (the reply is `+OK` immediately; the handshake happens
    /// asynchronously). Mirrors `ServerFamily::ReplicaOf`.
    fn start_replica(&mut self, host: &str, port: u16) {
        self.stop_replica();
        self.replica_stop.store(false, Ordering::Relaxed);
        let cfg = replica::ReplicaConfig {
            host: host.to_string(),
            port,
            listen_port: self.env.listen_port,
            num_shards: self.env.num_shards,
            shard_txs: self.env.shard_txs.clone(),
            status: self.replica_status.clone(),
            stop: self.replica_stop.clone(),
            lsn_cells: Arc::new(
                (0..self.env.num_shards)
                    .map(|_| Arc::new(AtomicU64::new(0)))
                    .collect(),
            ),
        };
        {
            let mut status = self.replica_status.lock().unwrap();
            status.master_host = host.to_string();
            status.master_port = port;
            status.error = None;
        }
        let handle = std::thread::Builder::new()
            .name("replica".into())
            .spawn(move || replica::run(cfg))
            .expect("failed to spawn replica thread");
        self.replica_handle = Some(handle);
    }

    /// `REPLICAOF NO ONE`: stop the replica thread and reset to master mode.
    fn stop_replica(&mut self) {
        self.replica_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.replica_handle.take() {
            let _ = handle.join();
        }
        let mut status = self.replica_status.lock().unwrap();
        *status = ReplicaStatus::default();
    }

    // ------------------------------------------------------------------
    // Replication (master side)
    // ------------------------------------------------------------------

    /// REPLCONF handling. `REPLCONF CAPA dragonfly` (a single pair) opens a
    /// replica session and replies with the `[replid, "SYNC<n>", flow_count,
    /// version, lineage]` handshake (`server_family.cc`). A single
    /// `REPLCONF ACK <lsn>` on a flow connection records the acknowledged LSN
    /// and is answered with silence; everything else defers to
    /// `local_replconf`.
    fn replconf(&mut self, conn_id: u64, args: &[Vec<u8>]) -> Option<RespValue> {
        let rest = &args[1..];
        if rest.len() == 2 {
            if rest[0].eq_ignore_ascii_case(b"CAPA") && rest[1].eq_ignore_ascii_case(b"dragonfly") {
                let (address, port) = {
                    let remote = self
                        .conns
                        .get(&conn_id)
                        .map_or_else(|| "0.0.0.0:0".to_string(), |c| c.remote.clone());
                    match remote.rsplit_once(':') {
                        Some((a, p)) => (a.to_string(), p.parse::<u32>().unwrap_or(0)),
                        None => (remote, 0),
                    }
                };
                let sync_id = self
                    .repl
                    .create_sync_session(address, port, self.env.num_shards);
                if let Some(conn) = self.conns.get_mut(&conn_id) {
                    conn.repl = Some(ConnRepl::Control { sync_id });
                }
                return Some(replication::capa_dragonfly_reply(
                    &self.repl,
                    sync_id,
                    self.env.num_shards,
                ));
            }
            if rest[0].eq_ignore_ascii_case(b"ACK") {
                if let Some(ConnRepl::Flow { sync_id, flow_id }) =
                    self.conns.get(&conn_id).and_then(|c| c.repl)
                {
                    if let Some(n) = std::str::from_utf8(&rest[1])
                        .ok()
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        if let Some(replica) = self.repl.get_mut(sync_id)
                            && let Some(flow) = replica.flows.get_mut(flow_id)
                        {
                            flow.last_acked_lsn = n;
                        }
                    }
                }
                return None;
            }
        }
        server::local_replconf(args)
    }

    /// `DFLY` subcommand dispatch (`dflycmd.cc` `DflyCmd`).
    fn dfly(&mut self, conn_id: u64, seq: u64, args: &[Vec<u8>]) {
        let sub = args
            .get(1)
            .map(|a| a.to_ascii_uppercase())
            .unwrap_or_default();
        match sub.as_slice() {
            b"FLOW" => self.dfly_flow(conn_id, seq, args),
            b"SYNC" => self.dfly_sync(conn_id, seq, args),
            b"STARTSTABLE" => self.dfly_startstable(conn_id, seq, args),
            _ => self.deliver(conn_id, seq, encode_value(&server::local_dfly(args))),
        }
    }

    /// `DFLY FLOW <master_id> <sync_id> <flow_id> [<lsn>]` or the failover form
    /// `<last_master_id> <lsn-vec>`. Registers the flow connection, enables the
    /// flow's shard journal, and negotiates FULL vs PARTIAL sync against the
    /// journal ring; replies `[FULL|PARTIAL, eof_token]`.
    fn dfly_flow(&mut self, conn_id: u64, seq: u64, args: &[Vec<u8>]) {
        if args.len() < 5 {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error(
                    "ERR wrong number of arguments for 'DFLY FLOW' command".into(),
                )),
            );
            return;
        }
        if String::from_utf8_lossy(&args[2]) != self.repl.master_replid {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("bad master id".into())),
            );
            return;
        }
        let Some(sync_id) = ReplicationManager::parse_sync_id(&String::from_utf8_lossy(&args[3]))
        else {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("bad sync id".into())),
            );
            return;
        };
        let Some(flow_id) = std::str::from_utf8(&args[4])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        else {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error(
                    "ERR value is not an integer or out of range".into(),
                )),
            );
            return;
        };
        if flow_id >= self.env.num_shards {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error(
                    "ERR value is not an integer or out of range".into(),
                )),
            );
            return;
        }
        let Some(replica) = self.repl.get(sync_id) else {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("bad sync id".into())),
            );
            return;
        };
        if replica.state != SyncState::Preparation {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("invalid state".into())),
            );
            return;
        }

        // The trailing LSN forms: `[<lsn>]` or `<last_master_id> <lsn-vec>`
        // (the lsn-vec has one entry per shard; only the failover/cascaded
        // partial-sync path, which v1 does not negotiate).
        let mut flow_lsn = None;
        match args.len() {
            6 => flow_lsn = parse_u64(&args[5]),
            7 => {
                if let Some(lsns) = parse_lsn_vec(&args[6])
                    && lsns.len() == self.env.num_shards
                {
                    flow_lsn = lsns.get(flow_id).copied();
                }
            }
            _ => {}
        }

        // `journal::IsLSNInPartialSyncBuffer` decides the sync type.
        let mut sync_type = "FULL";
        if let Some(lsn) = flow_lsn {
            let (tx, rx) = mpsc::channel();
            let asked = self.env.shard_txs[flow_id]
                .send(ShardMsg::IsLsnInBuffer { lsn, result_tx: tx })
                .is_ok()
                && rx.recv_timeout(Duration::from_secs(10)).unwrap_or(false);
            if asked {
                sync_type = "PARTIAL";
            }
        }

        let eof_token = replication::random_hex(40);
        {
            let replica = self.repl.get_mut(sync_id).unwrap();
            let flow = &mut replica.flows[flow_id];
            flow.conn_id = conn_id;
            flow.eof_token.clone_from(&eof_token);
            if sync_type == "PARTIAL" {
                flow.start_partial_sync_at = flow_lsn;
            }
        }
        // `journal::StartInThread`: the flow's shard starts recording writes.
        let _ = self.env.shard_txs[flow_id].send(ShardMsg::EnableJournal { enabled: true });
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.repl = Some(ConnRepl::Flow { sync_id, flow_id });
        }
        let reply = RespValue::Array(vec![
            RespValue::Simple(sync_type.into()),
            RespValue::Simple(eof_token.into()),
        ]);
        self.deliver(conn_id, seq, encode_value(&reply));
    }

    /// `DFLY SYNC <sync_id>`: kick off the chunked full-sync snapshot on every
    /// shard and move the session to FULL_SYNC. Each shard serializes its RDB
    /// baseline one chunk at a time (draining writes between chunks) and pushes
    /// the chunks through `repl_tx`; `drain_repl` writes them to the flow
    /// sockets and drives the next step. Partial-sync sessions reject SYNC (the
    /// replica must not send it after a PARTIAL reply).
    fn dfly_sync(&mut self, conn_id: u64, seq: u64, args: &[Vec<u8>]) {
        let Some(sync_id) = args
            .get(2)
            .and_then(|a| ReplicationManager::parse_sync_id(&String::from_utf8_lossy(a)))
        else {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("bad sync id".into())),
            );
            return;
        };
        let Some(replica) = self.repl.get(sync_id) else {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("bad sync id".into())),
            );
            return;
        };
        if replica.state != SyncState::Preparation {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("invalid state".into())),
            );
            return;
        }
        if replica
            .flows
            .iter()
            .any(|f| f.start_partial_sync_at.is_some())
        {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("invalid state".into())),
            );
            return;
        }

        for shard in 0..self.env.num_shards {
            let _ = self.env.shard_txs[shard].send(ShardMsg::FullSyncSnapshot {
                sync_id,
                flow_id: shard,
                bus: self.env.full_sync_bus.clone(),
            });
        }
        let replica = self.repl.get_mut(sync_id).unwrap();
        replica.state = SyncState::FullSync;
        self.deliver(conn_id, seq, encode_value(&RespValue::Simple("OK".into())));
    }

    /// `DFLY STARTSTABLE <sync_id>`: start the stable-sync streamers on every
    /// shard (ring catch-up then live consumer), write the eof tokens that
    /// close the full-sync RDB streams, and move the session to STABLE_SYNC.
    fn dfly_startstable(&mut self, conn_id: u64, seq: u64, args: &[Vec<u8>]) {
        let Some(sync_id) = args
            .get(2)
            .and_then(|a| ReplicationManager::parse_sync_id(&String::from_utf8_lossy(a)))
        else {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("bad sync id".into())),
            );
            return;
        };
        let Some(replica) = self.repl.get(sync_id) else {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("bad sync id".into())),
            );
            return;
        };
        let state = replica.state;
        if state != SyncState::FullSync && state != SyncState::Preparation {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("invalid state".into())),
            );
            return;
        }
        if replica.flows.iter().any(|f| f.conn_id == 0) {
            // `AllFlowsConnected`: a flow disconnected before STARTSTABLE.
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("invalid state".into())),
            );
            return;
        }

        for shard in 0..self.env.num_shards {
            let from_lsn = {
                let flow = &self.repl.get(sync_id).unwrap().flows[shard];
                flow.start_partial_sync_at.unwrap_or(flow.start_lsn)
            };
            let (tx, rx) = mpsc::channel();
            let _ = self.env.shard_txs[shard].send(ShardMsg::StartStableSync {
                sync_id,
                flow_id: shard,
                from_lsn,
                repl_tx: self.env.repl_tx.clone(),
                result_tx: tx,
            });
            if rx
                .recv_timeout(Duration::from_secs(120))
                .map_or(true, |r| r.is_err())
            {
                self.deliver(
                    conn_id,
                    seq,
                    encode_value(&RespValue::Error("invalid state".into())),
                );
                return;
            }
        }

        let replica = self.repl.get_mut(sync_id).unwrap();
        replica.state = SyncState::StableSync;
        // The eof token closes each flow's RDB stream; stable-sync journal
        // bytes follow it on the socket (they are drained after this reply).
        let tokens: Vec<(u64, String)> = replica
            .flows
            .iter()
            .map(|f| (f.conn_id, f.eof_token.clone()))
            .collect();
        for (fid, token) in tokens {
            if let Some(conn) = self.conns.get_mut(&fid) {
                conn.out.extend_from_slice(token.as_bytes());
            }
        }
        self.deliver(conn_id, seq, encode_value(&RespValue::Simple("OK".into())));
    }

    /// `DflyCmd::StopReplication`: unregister every flow's journal consumer,
    /// abort any in-flight full-sync snapshot, and drop the session. Triggered
    /// when a flow or control connection closes.
    fn stop_replication(&mut self, sync_id: u32) {
        let Some(replica) = self.repl.get(sync_id) else {
            return;
        };
        let flows: Vec<usize> = replica.flows.iter().map(|f| f.flow_id).collect();
        for flow_id in flows {
            let _ =
                self.env.shard_txs[flow_id].send(ShardMsg::StopReplication { sync_id, flow_id });
            let _ = self.env.shard_txs[flow_id].send(ShardMsg::CancelFullSync { sync_id, flow_id });
        }
        if let Some(replica) = self.repl.get_mut(sync_id) {
            replica.state = SyncState::Cancelled;
        }
        self.repl.replicas.remove(&sync_id);
    }

    fn drain_repl(&mut self) {
        while let Ok(chunk) = self.repl_rx.try_recv() {
            // A full-sync chunk for a vanished session: tell the shard to abort
            // its snapshot so no serialization state or consumer leaks.
            if self.repl.get(chunk.sync_id).is_none() {
                let _ = self.env.shard_txs[chunk.flow_id].send(ShardMsg::CancelFullSync {
                    sync_id: chunk.sync_id,
                    flow_id: chunk.flow_id,
                });
                continue;
            }
            if let ChunkKind::FullSync { journal_lsn } = chunk.kind {
                if let Some(cut) = journal_lsn {
                    // Final chunk: the snapshot cut LSN, matching the
                    // `JOURNAL_OFFSET` written into the stream tail.
                    let flow = &mut self.repl.get_mut(chunk.sync_id).unwrap().flows[chunk.flow_id];
                    flow.start_lsn = cut;
                } else {
                    // Interim chunk: serialize the next baseline chunk. The
                    // replica can only send STARTSTABLE after reading the
                    // final chunk, so `start_lsn` is always set by then.
                    let _ = self.env.shard_txs[chunk.flow_id].send(ShardMsg::SnapshotStep {
                        sync_id: chunk.sync_id,
                        flow_id: chunk.flow_id,
                    });
                }
            }
            let conn_id = self
                .repl
                .get(chunk.sync_id)
                .and_then(|r| r.flows.get(chunk.flow_id))
                .map(|f| f.conn_id);
            if let Some(conn_id) = conn_id
                && let Some(conn) = self.conns.get_mut(&conn_id)
            {
                conn.out.extend_from_slice(&chunk.bytes);
            }
        }
    }

    /// Feed a command to the live MONITOR connections as one
    /// `"<ts> [<db> <src>] \"CMD\" \"arg\" ..."` bulk line per command,
    /// mirroring upstream `DispatchMonitor` (`main_service.cc`). Admin
    /// commands never reach here (they are excluded by `dispatch`), and the
    /// issuing connection is skipped so a monitor never echoes its own
    /// RESET/QUIT.
    fn broadcast_monitor(&mut self, conn_id: u64, args: &[Vec<u8>]) {
        if self.monitors.is_empty() {
            return;
        }
        let ts = monitor_timestamp();
        let db = self.conns.get(&conn_id).map_or(0, |c| c.db_idx);
        let src = self
            .conns
            .get(&conn_id)
            .map(|c| c.remote.clone())
            .unwrap_or_default();
        let mut line = format!("{ts} [{db} {src}] ");
        for (i, a) in args.iter().enumerate() {
            if i > 0 {
                line.push(' ');
            }
            line.push('"');
            line.push_str(&monitor_escape(a));
            line.push('"');
        }
        let frame = encode_value(&RespValue::Bulk(line.into_bytes()));
        let targets: Vec<u64> = self
            .monitors
            .iter()
            .copied()
            .filter(|&m| m != conn_id)
            .collect();
        for mon in targets {
            if let Some(c) = self.conns.get_mut(&mon) {
                c.out.extend_from_slice(&frame);
            }
        }
    }

    fn send_single(
        &self,
        conn_id: u64,
        seq: u64,
        shard: usize,
        args: Vec<Vec<u8>>,
        owned: Vec<usize>,
    ) {
        let db_idx = self.conns.get(&conn_id).map_or(0, |c| c.db_idx);
        // `IncrementSequenceNumber` has not run for this command yet, so
        // `should_track` sees the pre-dispatch sequence (OPTIN: first command
        // after CACHING YES, OPTOUT: any but the first, NONE: always).
        let track_keys = self.should_track(conn_id);
        let op = SingleOp {
            conn_id,
            seq,
            args,
            owned_key_idxs: owned,
            db_idx,
            owns_all_keys: true,
            reply: self.env.reply_bus_tx.clone(),
            track_keys,
        };
        let _ = self.env.shard_txs[shard].send(ShardMsg::Single(op));
    }

    fn send_coord(
        &self,
        conn_id: u64,
        seq: u64,
        args: Vec<Vec<u8>>,
        keys: Vec<usize>,
        shards: Vec<usize>,
        first_key_idx: usize,
    ) {
        let db_idx = self.conns.get(&conn_id).map_or(0, |c| c.db_idx);
        let track_keys = self.should_track(conn_id);
        let msg = CoordMsg {
            conn_id,
            seq,
            args,
            keys,
            shards,
            first_key_idx,
            db_idx,
            no_block: self.no_block(conn_id),
            slowlog_threshold_usec: self.slow_log.threshold(),
            track_keys,
        };
        let _ = self.env.coord_tx.send(msg);
    }

    /// `TrackingConn::ShouldTrackKeys` (conn_context.h:232): enabled, not
    /// noloop, and the seq matches the mode's requirement.
    fn should_track(&self, conn_id: u64) -> bool {
        self.env.tracking.lock().unwrap().should_track(conn_id)
    }

    // ------------------------------------------------------------------
    // Replies
    // ------------------------------------------------------------------

    fn deliver(&mut self, conn_id: u64, seq: u64, bytes: Vec<u8>) {
        let completed = {
            let Some(conn) = self.conns.get_mut(&conn_id) else {
                return;
            };
            if seq == conn.deliver_seq {
                conn.out.extend_from_slice(&bytes);
                conn.deliver_seq += 1;
                while let Some(next) = conn.buffered.remove(&conn.deliver_seq) {
                    conn.out.extend_from_slice(&next);
                    conn.deliver_seq += 1;
                }
                conn.pending_slowlog.remove(&seq)
            } else if seq > conn.deliver_seq {
                conn.buffered.insert(seq, bytes);
                None
            } else {
                None
            }
        };
        if let Some(pending) = completed {
            self.record_slowlog(conn_id, pending);
        }
    }

    /// Like `deliver`, but attaches the augmented slowlog arguments produced by
    /// the coordinator for EXEC/EVAL replies (`FormatExecSlowlog` /
    /// `FormatEvalSlowlog`).
    fn deliver_with_args(
        &mut self,
        conn_id: u64,
        seq: u64,
        bytes: Vec<u8>,
        slowlog_args: Option<Vec<Vec<u8>>>,
    ) {
        if let Some(conn) = self.conns.get_mut(&conn_id)
            && let Some(pending) = conn.pending_slowlog.get_mut(&seq)
        {
            pending.slowlog_args = slowlog_args;
        }
        self.deliver(conn_id, seq, bytes);
    }

    /// Produce the SLOWLOG entry for a completed request (`RecordLatency`): the
    /// execution time is the dispatch-to-reply latency; the tail is the raw
    /// arguments, or the augmented stats arguments for EXEC/EVAL.
    fn record_slowlog(&mut self, conn_id: u64, pending: PendingSlowlog) {
        let exec_time_usec = pending.started.elapsed().as_micros() as u64;
        if !self.slow_log.should_log(exec_time_usec) {
            return;
        }
        let tail = pending
            .slowlog_args
            .unwrap_or_else(|| pending.args[1..].to_vec());
        let client_ip = self
            .conns
            .get(&conn_id)
            .map_or_else(String::new, |c| c.remote.clone());
        self.slow_log.add(
            &pending.name,
            tail,
            &client_ip,
            "",
            exec_time_usec,
            now_usec(),
        );
    }

    fn drain_bus(&mut self) {
        while let Ok(reply) = self.reply_bus_rx.try_recv() {
            if reply.is_push {
                // An unsequenced push (e.g. a CLIENT TRACKING invalidation).
                // It is appended straight to the connection's output buffer,
                // bypassing the reply ordering machinery and slowlog.
                if let Some(conn) = self.conns.get_mut(&reply.conn_id) {
                    conn.out.extend_from_slice(&reply.bytes);
                }
                continue;
            }
            self.deliver_with_args(reply.conn_id, reply.seq, reply.bytes, reply.slowlog_args);
        }
    }

    fn drain_wake(&mut self) {
        let mut buf = [0u8; 4096];
        loop {
            let n = unsafe {
                libc::read(
                    self.wake_r,
                    buf.as_mut_ptr().cast::<libc::c_void>(),
                    buf.len(),
                )
            };
            if n <= 0 || (n as usize) < buf.len() {
                break;
            }
        }
    }

    // ------------------------------------------------------------------
    // Output flushing
    // ------------------------------------------------------------------

    fn flush_all(&mut self) {
        let ids: Vec<u64> = self
            .conns
            .values()
            .filter(|c| !c.out.is_empty() || c.closing)
            .map(|c| c.id)
            .collect();
        for id in ids {
            self.flush_conn(id);
            // QUIT closes the socket right after its +OK has been written.
            if self
                .conns
                .get(&id)
                .is_some_and(|c| c.closing && c.out.is_empty())
            {
                self.close_conn(id);
            }
        }
    }

    fn flush_conn(&mut self, conn_id: u64) {
        let fd = match self.conns.get(&conn_id) {
            Some(c) => c.fd,
            None => return,
        };
        loop {
            let n = {
                let Some(conn) = self.conns.get_mut(&conn_id) else {
                    return;
                };
                if conn.out.is_empty() {
                    break;
                }
                unsafe { libc::write(fd, conn.out.as_ptr().cast::<libc::c_void>(), conn.out.len()) }
            };
            if n < 0 {
                let e = std::io::Error::last_os_error();
                if is_again(&e) {
                    break;
                }
                self.close_conn(conn_id);
                return;
            }
            if n == 0 {
                break;
            }
            let conn = self.conns.get_mut(&conn_id).unwrap();
            conn.out.drain(..n as usize);
        }

        let (needs_del, needs_add) = match self.conns.get(&conn_id) {
            Some(c) => (
                c.out.is_empty() && c.write_registered,
                !c.out.is_empty() && !c.write_registered,
            ),
            None => return,
        };
        if needs_del {
            let ev = kev(fd as usize, EV_WRITE, EV_DELETE);
            let _ = self.kevent_change(&[ev]);
            if let Some(c) = self.conns.get_mut(&conn_id) {
                c.write_registered = false;
            }
        } else if needs_add {
            let ev = kev(fd as usize, EV_WRITE, EV_ADD_ENABLE);
            let _ = self.kevent_change(&[ev]);
            if let Some(c) = self.conns.get_mut(&conn_id) {
                c.write_registered = true;
            }
        }
    }

    fn kevent_change(&self, events: &[libc::kevent]) -> std::io::Result<()> {
        let n = unsafe {
            libc::kevent(
                self.kq,
                events.as_ptr(),
                events.len() as i32,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if n < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

fn kev(ident: usize, filter: i16, flags: u16) -> libc::kevent {
    libc::kevent {
        ident,
        filter,
        flags,
        fflags: 0,
        data: 0,
        udata: std::ptr::null_mut(),
    }
}

fn zero_kev() -> libc::kevent {
    unsafe { std::mem::zeroed() }
}

fn set_nonblocking(fd: RawFd) {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags >= 0 {
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
    }
}

fn is_again(e: &std::io::Error) -> bool {
    matches!(e.raw_os_error(), Some(libc::EAGAIN))
        || matches!(e.raw_os_error(), Some(libc::EWOULDBLOCK))
}

fn parse_u64(bytes: &[u8]) -> Option<u64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// `DFLY FLOW`'s trailing `last_journal_LSNs` argument: a space-separated list
/// of LSNs, one per shard (`ParseLsnVec`).
fn parse_lsn_vec(bytes: &[u8]) -> Option<Vec<u64>> {
    let s = std::str::from_utf8(bytes).ok()?;
    if s.trim().is_empty() {
        return Some(vec![]);
    }
    s.split_ascii_whitespace()
        .map(|tok| tok.parse::<u64>().ok())
        .collect()
}
