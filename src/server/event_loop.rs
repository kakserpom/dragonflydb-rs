use std::collections::{BTreeMap, HashMap};
use std::net::TcpListener;
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
use std::sync::mpsc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::commands::exec::server;
use crate::commands::{Command, FLAG_ADMIN, FLAG_BLOCKING, FLAG_GLOBAL, FLAG_LOCAL};
use crate::error::RespValue;
use crate::protocol::resp::RespParser;
use crate::server::pubsub::{self, ChannelStore, SubscribeInfo};
use crate::server::{
    command_for, encode_value, keys_per_shard, CoordMsg, Reply, ServerEnv, ShardMsg, SingleOp,
    WatchState,
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
            0..=31 | 127..=255 => s.push_str(&format!("\\x{:02x}", b)),
            _ => s.push(b as char),
        }
    }
    s
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
    conn_id: u64,
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
    /// MULTI/EXEC transaction state.
    multi: MultiState,
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
    /// Set by QUIT: close the socket once pending output has flushed.
    closing: bool,
}

/// A single-threaded kqueue-based IO event loop. Owns all client sockets, the
/// reply bus receiver, and the shared `ServerEnv` handle.
pub struct IoLoop {
    env: ServerEnv,
    reply_bus_rx: mpsc::Receiver<Reply>,
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
    /// Set by SHUTDOWN; the run loop stops once pending replies are flushed.
    shutting_down: bool,
}

impl IoLoop {
    pub fn new(
        env: ServerEnv,
        reply_bus_rx: mpsc::Receiver<Reply>,
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
            listener,
            wake_r,
            kq,
            conns: HashMap::new(),
            fd_to_id: HashMap::new(),
            next_conn_id: 1,
            pubsub: ChannelStore::new(),
            monitors: Vec::new(),
            shutting_down: false,
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
                        .map(|a| a.to_string())
                        .unwrap_or_else(|_| "0.0.0.0:0".into());
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
                conn_id,
                fd,
                parser: RespParser::new(),
                out: Vec::new(),
                write_registered: false,
                dispatch_seq: 0,
                deliver_seq: 0,
                buffered: BTreeMap::new(),
                db_idx: 0,
                multi: MultiState::default(),
                watched: Vec::new(),
                watched_dirty: false,
                sub: SubscribeInfo::default(),
                remote,
                monitor: false,
                closing: false,
            },
        );
        self.fd_to_id.insert(fd, conn_id);
        let ev = kev(fd as usize, EV_READ, EV_ADD_ENABLE);
        let _ = self.kevent_change(&[ev]);
    }

    fn close_conn(&mut self, conn_id: u64) {
        if let Some(conn) = self.conns.remove(&conn_id) {
            self.fd_to_id.remove(&conn.fd);
            self.pubsub.remove_conn(conn_id);
            self.monitors.retain(|&c| c != conn_id);
            unsafe { libc::close(conn.fd) };
        }
    }

    fn handle_read(&mut self, fd: RawFd) {
        let Some(&conn_id) = self.fd_to_id.get(&fd) else { return };
        let mut buf = [0u8; 16384];
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
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
            let bytes = encode_value(&RespValue::Error(format!("ERR {}", e)));
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
            let msg = format!("ERR unknown command '{}'", String::from_utf8_lossy(&args[0]));
            self.deliver(conn_id, seq, encode_value(&RespValue::Error(msg)));
            return;
        };
        if let Some(e) = cmd.check_arity(args.len()) {
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

        // A MONITOR connection may only run RESET or QUIT
        // (`main_service.cc:1413-1414`).
        if self
            .conns
            .get(&conn_id)
            .map(|c| c.monitor)
            .unwrap_or(false)
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
                match server::local_replconf(&args) {
                    Some(v) => self.deliver(conn_id, seq, encode_value(&v)),
                    None => self.deliver(conn_id, seq, Vec::new()),
                }
                return;
            }
            let v = self.handle_local(conn_id, cmd, &args);
            self.deliver(conn_id, seq, encode_value(&v));
            return;
        }
        self.dispatch_keyed(conn_id, seq, cmd, &args);
    }

    /// Split a command by its keys and send it to a shard or the coordinator.
    fn dispatch_keyed(&self, conn_id: u64, seq: u64, cmd: &'static Command, args: &[Vec<u8>]) {
        if cmd.has_flag(FLAG_GLOBAL) {
            let shards: Vec<usize> = (0..self.env.num_shards).collect();
            self.send_coord(conn_id, seq, args.to_vec(), vec![], shards, cmd.key_range.first);
            return;
        }

        let keys = self.env.extract_keys(cmd, args);
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
            self.send_coord(conn_id, seq, args.to_vec(), keys, shards, cmd.key_range.first);
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
        if cmd.name == "UNWATCH" {
            self.local_unwatch(conn_id, seq);
            return;
        }
        // Commands executed through EXEC are monitored as they run.
        if !cmd.has_flag(FLAG_ADMIN) && cmd.name != "EXEC" {
            self.broadcast_monitor(conn_id, args);
        }
        if cmd.has_flag(FLAG_LOCAL) {
            if cmd.name == "REPLCONF" {
                match server::local_replconf(args) {
                    Some(v) => self.deliver(conn_id, seq, encode_value(&v)),
                    None => self.deliver(conn_id, seq, Vec::new()),
                }
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
            .map(|c| c.multi.phase == MultiPhase::Collect)
            .unwrap_or(false)
    }

    fn handle_local(&mut self, conn_id: u64, cmd: &Command, args: &[Vec<u8>]) -> RespValue {
        // Single-reply pub/sub commands reach this path both from `dispatch`
        // (FLAG_LOCAL) and from `run_queued` when executed inside EXEC.
        match cmd.name {
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
        // While subscribed in RESP2, PING echoes the message inside a
        // `["pong", msg]` array instead of a plain bulk reply
        // (`GenericFamily::Ping`).
        if cmd.name == "PING"
            && self
                .conns
                .get(&conn_id)
                .map(|c| !c.sub.is_empty())
                .unwrap_or(false)
        {
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
            .map(|c| c.multi.phase)
            .unwrap_or(MultiPhase::Inactive);
        if phase == MultiPhase::Collect {
            self.deliver(
                conn_id,
                seq,
                encode_value(&RespValue::Error("ERR MULTI calls can not be nested".into())),
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
            .map(|c| c.multi.phase)
            .unwrap_or(MultiPhase::Inactive);
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
            let conn = match self.conns.get(&conn_id) {
                Some(c) => c,
                None => return,
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
            let conn = match self.conns.get(&conn_id) {
                Some(c) => c,
                None => return,
            };
            conn.watched.iter().map(|w| w.key.clone()).collect()
        };
        if !existing.is_empty() {
            let states = self.watch_snapshot(&existing, db_idx);
            let by_key: HashMap<&[u8], &WatchState> =
                states.iter().map(|(k, s)| (k.as_slice(), s)).collect();
            let conn = self.conns.get(&conn_id).unwrap();
            dirty = conn.watched.iter().any(|w| {
                by_key
                    .get(w.key.as_slice())
                    .map(|s| {
                        s.version != w.state.version
                            || s.existed != w.state.existed
                            || s.db_epoch != w.state.db_epoch
                    })
                    .unwrap_or(true)
            });
        }
        let states = self.watch_snapshot(&new_keys, db_idx);
        if let Some(conn) = self.conns.get_mut(&conn_id) {
            conn.watched_dirty |= dirty;
            if !dirty {
                for (key, state) in states {
                    conn.watched.push(WatchedKey { db: db_idx, key, state });
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
            let conn = match self.conns.get_mut(&conn_id) {
                Some(c) => c,
                None => return,
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
            let db_idx = self.conns.get(&conn_id).map(|c| c.db_idx).unwrap_or(0);
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
                by_key
                    .get(w.key.as_slice())
                    .map(|s| {
                        s.version != w.state.version
                            || s.existed != w.state.existed
                            || s.db_epoch != w.state.db_epoch
                    })
                    .unwrap_or(true)
            });
            if is_dirty {
                self.deliver(conn_id, seq, encode_value(&RespValue::Nil));
                return;
            }
        }
        // The header plus each queued command's reply, delivered in seq order,
        // concatenate into the EXEC RESP array.
        let header = format!("*{}\r\n", queue.len()).into_bytes();
        self.deliver(conn_id, seq, header);
        for args in queue {
            self.run_queued(conn_id, &args);
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
        }
        self.pubsub.remove_conn(conn_id);
        self.monitors.retain(|&c| c != conn_id);
        self.deliver(conn_id, seq, encode_value(&RespValue::Simple("RESET".into())));
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
                conn.sub.channels.insert(ch.to_vec());
            }
            let count = self
                .conns
                .get(&conn_id)
                .map(|c| c.sub.count())
                .unwrap_or(0);
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
            let count = self
                .conns
                .get(&conn_id)
                .map(|c| c.sub.count())
                .unwrap_or(0);
            let reply = encode_value(&pubsub::sub_change("unsubscribe", Some(&ch), count));
            self.deliver(conn_id, seq, reply);
        }
    }

    fn local_psubscribe(&mut self, conn_id: u64, seq: u64, args: &[Vec<u8>]) {
        for (seq, pat) in (seq..).zip(args[1..].iter()) {
            self.pubsub.psubscribe(pat, conn_id);
            if let Some(conn) = self.conns.get_mut(&conn_id) {
                conn.sub.patterns.insert(pat.to_vec());
            }
            let count = self
                .conns
                .get(&conn_id)
                .map(|c| c.sub.count())
                .unwrap_or(0);
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
            let count = self
                .conns
                .get(&conn_id)
                .map(|c| c.sub.count())
                .unwrap_or(0);
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
                conn.sub.sharded.insert(ch.to_vec());
            }
            let count = self
                .conns
                .get(&conn_id)
                .map(|c| c.sub.count())
                .unwrap_or(0);
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
            let count = self
                .conns
                .get(&conn_id)
                .map(|c| c.sub.count())
                .unwrap_or(0);
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
            let frame =
                encode_value(&pubsub::push_message(pattern.as_deref(), &channel, &message, false));
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
                    WatchState { version: 0, existed: false, db_epoch: 0 },
                )
            })
            .collect();
        for (shard, idxs) in by_shard {
            let ks: Vec<Vec<u8>> = idxs.iter().map(|&i| keys[i].clone()).collect();
            let (tx, rx) = mpsc::channel();
            if self.env.shard_txs[shard]
                .send(ShardMsg::WatchQuery { keys: ks, db_idx, result_tx: tx })
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
        self.deliver(conn_id, seq, encode_value(&RespValue::Simple("QUEUED".into())));
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
            "CLIENT" => server::local_client(args),
            "TIME" => server::local_time(args),
            "ROLE" => server::local_role(args),
            "LASTSAVE" => server::local_lastsave(args),
            "LATENCY" => server::local_latency(args),
            "SLOWLOG" => server::local_slowlog(args),
            "WAIT" => server::local_wait(args),
            "REPLICAOF" | "SLAVEOF" => server::local_replicaof(args),
            "ADDREPLICAOF" => server::local_addreplicaof(args),
            "REPLTAKEOVER" => server::local_repltakeover(args),
            "MODULE" => server::local_module(args),
            "FUNCTION" => server::local_function(args),
            "SCRIPT" => server::local_script(args),
            "EVAL" | "EVALSHA" => server::local_lua(args),
            "DFLY" => server::local_dfly(args),
            _ => RespValue::Error("ERR internal: unhandled local command".into()),
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
        let db = self.conns.get(&conn_id).map(|c| c.db_idx).unwrap_or(0);
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
        let targets: Vec<u64> = self.monitors.iter().copied().filter(|&m| m != conn_id).collect();
        for mon in targets {
            if let Some(c) = self.conns.get_mut(&mon) {
                c.out.extend_from_slice(&frame);
            }
        }
    }

    fn send_single(&self, conn_id: u64, seq: u64, shard: usize, args: Vec<Vec<u8>>, owned: Vec<usize>) {
        let db_idx = self.conns.get(&conn_id).map(|c| c.db_idx).unwrap_or(0);
        let op = SingleOp {
            conn_id,
            seq,
            args,
            owned_key_idxs: owned,
            db_idx,
            reply: self.env.reply_bus_tx.clone(),
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
        let db_idx = self.conns.get(&conn_id).map(|c| c.db_idx).unwrap_or(0);
        let msg = CoordMsg {
            conn_id,
            seq,
            args,
            keys,
            shards,
            first_key_idx,
            db_idx,
            no_block: self.in_multi(conn_id),
        };
        let _ = self.env.coord_tx.send(msg);
    }

    // ------------------------------------------------------------------
    // Replies
    // ------------------------------------------------------------------

    fn deliver(&mut self, conn_id: u64, seq: u64, bytes: Vec<u8>) {
        let Some(conn) = self.conns.get_mut(&conn_id) else { return };
        if seq == conn.deliver_seq {
            conn.out.extend_from_slice(&bytes);
            conn.deliver_seq += 1;
            while let Some(next) = conn.buffered.remove(&conn.deliver_seq) {
                conn.out.extend_from_slice(&next);
                conn.deliver_seq += 1;
            }
        } else if seq > conn.deliver_seq {
            conn.buffered.insert(seq, bytes);
        }
    }

    fn drain_bus(&mut self) {
        while let Ok(reply) = self.reply_bus_rx.try_recv() {
            self.deliver(reply.conn_id, reply.seq, reply.bytes);
        }
    }

    fn drain_wake(&mut self) {
        let mut buf = [0u8; 4096];
        loop {
            let n = unsafe { libc::read(self.wake_r, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
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
            .map(|c| c.conn_id)
            .collect();
        for id in ids {
            self.flush_conn(id);
            // QUIT closes the socket right after its +OK has been written.
            if self.conns.get(&id).map(|c| c.closing && c.out.is_empty()).unwrap_or(false) {
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
                let conn = match self.conns.get_mut(&conn_id) {
                    Some(c) => c,
                    None => return,
                };
                if conn.out.is_empty() {
                    break;
                }
                unsafe { libc::write(fd, conn.out.as_ptr() as *const libc::c_void, conn.out.len()) }
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
            Some(c) => (c.out.is_empty() && c.write_registered, !c.out.is_empty() && !c.write_registered),
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
    libc::kevent { ident, filter, flags, fflags: 0, data: 0, udata: std::ptr::null_mut() }
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
