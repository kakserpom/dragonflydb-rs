use std::net::TcpListener;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::mpsc;

use dragonflydb::commands::lua::ScriptMgr;
use dragonflydb::server::event_loop::IoLoop;
use dragonflydb::server::{Reply, ReplyBus, ServerEnv};
use dragonflydb::server::{coordinator, shard};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut port: u16 = 6379;
    let mut num_shards = std::thread::available_parallelism()
        .map_or(4, std::num::NonZero::get)
        .max(1);
    let mut lua_auto_async = false;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                i += 1;
                if i >= args.len() {
                    usage();
                }
                port = args[i].parse().expect("invalid --port value");
            }
            "--num-shards" | "--num_shards" => {
                i += 1;
                if i >= args.len() {
                    usage();
                }
                num_shards = args[i].parse().expect("invalid --num-shards value");
            }
            "--lua_auto_async" => lua_auto_async = true,
            "--lua_auto_async=false" => lua_auto_async = false,
            "--help" | "-h" => {
                usage();
                return;
            }
            other => {
                eprintln!("unknown argument: {other}");
                usage();
                return;
            }
        }
        i += 1;
    }
    if num_shards == 0 {
        eprintln!("--num-shards must be >= 1");
        std::process::exit(1);
    }

    // Writing to a closed socket must not kill the process.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }

    // Reply bus + kqueue wakeup pipe.
    let (reply_tx, reply_rx) = mpsc::channel::<Reply>();
    let mut pipefds = [0i32; 2];
    let rc = unsafe { libc::pipe(pipefds.as_mut_ptr()) };
    assert!(rc == 0, "pipe failed");
    set_nonblocking(pipefds[0]);
    set_nonblocking(pipefds[1]);
    let reply_bus = ReplyBus::new(reply_tx, pipefds[1]);

    // Shard threads.
    let mut shard_txs = Vec::with_capacity(num_shards);
    for s in 0..num_shards {
        let (tx, rx) = mpsc::channel();
        let _ = shard::spawn(s, rx);
        shard_txs.push(tx);
    }

    // Transaction coordinator thread.
    let (coord_tx, coord_rx) = mpsc::channel();
    let (gc_tx, gc_rx) = mpsc::channel();
    let mut mgr = ScriptMgr::new();
    mgr.lua_auto_async = lua_auto_async;
    let script_mgr = Arc::new(std::sync::Mutex::new(mgr));
    coordinator::spawn(
        num_shards,
        coord_rx,
        gc_rx,
        shard_txs.clone(),
        reply_bus.clone(),
        script_mgr.clone(),
    );

    let listener = match TcpListener::bind(("0.0.0.0", port)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("failed to bind 0.0.0.0:{port}: {e}");
            std::process::exit(1);
        }
    };

    let env = ServerEnv {
        num_shards,
        shard_txs,
        coord_tx,
        gc_tx,
        reply_bus_tx: reply_bus,
        script_mgr,
    };

    println!("dragonflydb-rs listening on 0.0.0.0:{port} with {num_shards} shards");

    let mut loop_ = match IoLoop::new(env, reply_rx, listener, pipefds[0]) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("failed to create event loop: {e}");
            std::process::exit(1);
        }
    };
    if let Err(e) = loop_.run() {
        eprintln!("event loop error: {e}");
        std::process::exit(1);
    }
}

fn usage() {
    eprintln!(
        "usage: dragonflydb [--port PORT] [--num-shards N] [--lua_auto_async[=false]] [--help]"
    );
}

fn set_nonblocking(fd: RawFd) {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags >= 0 {
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
    }
}
