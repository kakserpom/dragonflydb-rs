use std::collections::{BTreeMap, HashMap};
use std::net::TcpListener;
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
use std::sync::mpsc;

use crate::commands::exec::server;
use crate::commands::{Command, FLAG_BLOCKING, FLAG_GLOBAL, FLAG_LOCAL};
use crate::error::RespValue;
use crate::protocol::resp::RespParser;
use crate::server::{
    command_for, encode_value, keys_per_shard, CoordMsg, Reply, ServerEnv, ShardMsg, SingleOp,
};

const EV_READ: i16 = libc::EVFILT_READ;
const EV_WRITE: i16 = libc::EVFILT_WRITE;
const EV_ADD_ENABLE: u16 = libc::EV_ADD | libc::EV_ENABLE;
const EV_DELETE: u16 = libc::EV_DELETE;

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
        }
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
                    let _ = stream.set_nonblocking(true);
                    let fd = stream.into_raw_fd();
                    self.register_conn(fd);
                }
                Err(e) if is_again(&e) => break,
                Err(_) => break,
            }
        }
    }

    fn register_conn(&mut self, fd: RawFd) {
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
            },
        );
        self.fd_to_id.insert(fd, conn_id);
        let ev = kev(fd as usize, EV_READ, EV_ADD_ENABLE);
        let _ = self.kevent_change(&[ev]);
    }

    fn close_conn(&mut self, conn_id: u64) {
        if let Some(conn) = self.conns.remove(&conn_id) {
            self.fd_to_id.remove(&conn.fd);
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
        let seq = {
            let conn = self.conns.get_mut(&conn_id).unwrap();
            let seq = conn.dispatch_seq;
            conn.dispatch_seq += 1;
            seq
        };

        let Some(cmd) = command_for(&args) else {
            let msg = format!("ERR unknown command '{}'", String::from_utf8_lossy(&args[0]));
            self.deliver(conn_id, seq, encode_value(&RespValue::Error(msg)));
            return;
        };
        if let Some(e) = cmd.check_arity(args.len()) {
            self.deliver(conn_id, seq, encode_value(&RespValue::Error(e)));
            return;
        }
        if cmd.has_flag(FLAG_LOCAL) {
            if cmd.name == "SELECT" {
                let v = server::local_select(&args);
                if matches!(&v, RespValue::Simple(_)) {
                    if let (Some(db), Some(conn)) = (
                        args.get(1).and_then(|a| crate::util::parse_i64(a)),
                        self.conns.get_mut(&conn_id),
                    ) {
                        conn.db_idx = db as usize;
                    }
                }
                self.deliver(conn_id, seq, encode_value(&v));
                return;
            }
            let v = self.run_local(cmd, &args);
            self.deliver(conn_id, seq, encode_value(&v));
            return;
        }
        if cmd.has_flag(FLAG_GLOBAL) {
            let shards: Vec<usize> = (0..self.env.num_shards).collect();
            self.send_coord(conn_id, seq, args, vec![], shards, cmd.key_range.first);
            return;
        }

        let keys = self.env.extract_keys(cmd, &args);
        if keys.is_empty() {
            // Malformed/movable-key command without keys: let the executor
            // validate and reply with an error from shard 0.
            self.send_single(conn_id, seq, 0, args, vec![]);
            return;
        }
        let per = keys_per_shard(&args, &keys, self.env.num_shards);
        if per.len() == 1 && !cmd.has_flag(FLAG_BLOCKING) {
            self.send_single(conn_id, seq, per[0].0, args, per[0].1.clone());
        } else {
            let shards: Vec<usize> = per.iter().map(|(s, _)| *s).collect();
            self.send_coord(conn_id, seq, args, keys, shards, cmd.key_range.first);
        }
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
            _ => RespValue::Error("ERR internal: unhandled local command".into()),
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
        let msg = CoordMsg { conn_id, seq, args, keys, shards, first_key_idx, db_idx };
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
        loop {
            match self.reply_bus_rx.try_recv() {
                Ok(reply) => self.deliver(reply.conn_id, reply.seq, reply.bytes),
                Err(mpsc::TryRecvError::Empty) | Err(mpsc::TryRecvError::Disconnected) => break,
            }
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
            .filter(|c| !c.out.is_empty())
            .map(|c| c.conn_id)
            .collect();
        for id in ids {
            self.flush_conn(id);
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
