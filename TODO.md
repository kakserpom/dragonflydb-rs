# DragonflyDB Rust Port — Parity Tracking

Goal: 100% parity with DragonflyDB — every command in the reference
`dragonfly/src/server/*.cc` registries ported to `src/commands/exec/*.rs`,
with reference tests (`*_test.cc`) ported to Rust unit tests.

Command counts: reference **295**, registered **296** (all reference
commands are now in the registry; the extra is GEOSEARCHSTORE, a Redis
extension Dragonfly does not implement). **Full command-name parity reached.**

Legend:
- [x] ported
- [~] partial (registered, some subcommands blocked)
- [ ] missing

## String family (`string_family.cc`) — 25 cmds
- [x] APPEND, DECR, DECRBY, DIGEST, GAT, GET, GETDEL, GETEX, GETRANGE, GETSET,
  INCR, INCRBY, INCRBYFLOAT, MGET, MSET, MSETNX, PREPEND, PSETEX, SET, SETEX,
  SETNX, SETRANGE, STRLEN, SUBSTR, CL.THROTTLE

## Key/generic family (`generic_family.cc`) — 33 cmds
- [x] COPY, DELEX, DEL, DUMP, ECHO, EXISTS, EXPIRE, EXPIREAT, EXPIRETIME,
  FIELDEXPIRE, FIELDTTL, KEYS, MOVE, PERSIST, PEXPIRETIME, PEXPIRE, PEXPIREAT,
  PING, PTTL, RANDOMKEY, RENAME, RENAMENX, RESTORE, RM, SCAN, SELECT, SORT,
  SORT_RO, STICK, TIME, TOUCH, TTL, TYPE, UNLINK
- [ ] (none — family complete)

## Bit ops (`bitops_family.cc`) — 6 cmds
- [x] BITCOUNT, BITFIELD, BITFIELD_RO, BITOP, BITPOS, GETBIT, SETBIT

## List family (`list_family.cc`) — 22 cmds
- [x] LINDEX, LLEN, LPOP, LPOS, LPUSH, LPUSHX, LRANGE, LREM, LSET, LTRIM, RPOP,
  RPUSH, RPUSHX
- [x] BLMOVE, BLMPOP, BLPOP, BRPOP, BRPOPLPUSH, LINSERT, LMOVE, LMPOP, RPOPLPUSH

## Hash family (`hset_family.cc`) — 21 cmds
- [x] HDEL, HEXISTS, HGET, HGETALL, HGETEX, HEXPIRE, HINCRBY, HINCRBYFLOAT,
  HKEYS, HLEN, HMGET, HMSET, HPEXPIRETIME, HRANDFIELD, HSCAN, HSET, HSETEX,
  HSETNX, HSTRLEN, HTTL, HVALS

## Set family (`set_family.cc`) — 18 cmds
- [x] SADD, SADDEX, SCARD, SDIFF, SDIFFSTORE, SINTER, SINTERCARD,
  SINTERSTORE, SISMEMBER, SMEMBERS, SMISMEMBER, SMOVE, SPOP, SRANDMEMBER,
  SREM, SSCAN, SUNION, SUNIONSTORE

## Zset family (`zset_family.cc`) — 35 cmds
- [x] BZMPOP, BZPOPMAX, BZPOPMIN, ZADD, ZCARD, ZCOUNT, ZDIFF, ZDIFFSTORE,
  ZINCRBY, ZINTER, ZINTERCARD, ZINTERSTORE, ZLEXCOUNT, ZMSCORE, ZMPOP, ZPOPMAX,
  ZPOPMIN, ZRANDMEMBER, ZRANGE, ZRANGEBYLEX, ZRANGEBYSCORE, ZRANGESTORE, ZRANK,
  ZREM, ZREMRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREVRANGE,
  ZREVRANGEBYLEX, ZREVRANGEBYSCORE, ZREVRANK, ZSCAN, ZSCORE, ZUNION, ZUNIONSTORE
- [ ] (none — family complete)

## Stream family (`stream_family.cc`) — 15 cmds
- [x] XACK, XADD, XAUTOCLAIM, XCLAIM, XDEL, XGROUP, XINFO, XLEN, XPENDING,
  XRANGE, XREAD, XREADGROUP, XREVRANGE, XSETID, XTRIM

## HyperLogLog (`hll_family.cc`) — 3 cmds
- [x] PFADD, PFCOUNT, PFMERGE

## Geo (`geo_family.cc`) — 9 cmds
- [x] GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEORADIUS_RO, GEORADIUSBYMEMBER,
  GEORADIUSBYMEMBER_RO, GEOSEARCH, GEOSEARCHSTORE

## Server / admin (`server_family.cc` + `main_service.cc`)
- [x] ADDREPLICAOF, AUTH, BGSAVE, CLIENT, COMMAND, CONFIG, DBSIZE, DEBUG,
  DISCARD, EXEC, FLUSHALL, FLUSHDB, HELLO, INFO, LASTSAVE, LATENCY,
  MEMORY, MODULE, MONITOR, MULTI, PING, PSUBSCRIBE, PUBLISH, PUBSUB,
  PUNSUBSCRIBE, QUIT, REPLCONF, REPLICAOF, REPLTAKEOVER, RESET, ROLE, SAVE,
  SHRINK, SHUTDOWN, SLAVEOF, SLOWLOG, SPUBLISH, SSUBSCRIBE, SUBSCRIBE,
  SUNSUBSCRIBE, UNSUBSCRIBE, UNWATCH, WAIT, WATCH
- [x] CLIENT TRACKING/CACHING (RESP3-gated client-side caching: per-connection
  `seq_num` with OPTIN/OPTOUT/NOLOOP, keyed invalidation pushes delivered FIFO
  before the triggering write's reply, a null-keyed broadcast push on
  FLUSHDB/FLUSHALL, and CACHING stickiness through MULTI/EXEC/DISCARD; full
  `ClientTracking*` test suite ported)
- [x] FUNCTION (LOAD [REPLACE], DELETE, FLUSH, LIST [LIBRARYNAME] [WITHCODE],
  STATS, DUMP, RESTORE [FLUSH|APPEND|REPLACE], KILL, HELP backed by a shared
  library registry with per-library `#!lua name=...` headers, `redis.register_function`
  collection, and duplicate library/function-name enforcement)
- [x] FCALL, FCALL_RO (run registered functions on the shard owning the first
  key: lazy library load into its interpreter, `(keys, args)` tables, per-key
  transaction locking, `no-writes`/`allow-undeclared-keys` flag enforcement)
- [x] SCRIPT (EXISTS, LIST, FLUSH, LATENCY, GC, FLAGS, LOAD, HELP backed by a
  shared `ScriptMgr` cache of compiled scripts; `SCRIPT FLAGS` and `--!df`
  comment flags set per-script params)
- [x] EVAL, EVALSHA, EVAL_RO, EVALSHA_RO (sandboxed Lua interpreter per shard,
  created on the shard thread: compile-once per shard, `KEYS`/`ARGV` globals,
  per-key transaction locking, cross-shard `redis.call`/`redis.pcall` dispatch,
  script error wrapping `ERR Error running script (call to <sha>): ...`, NOSCRIPT
  and read-only write rejection)
- [x] `redis.acall`/`redis.apcall` (async command batching flushed as one
  squashed phase on sync calls / budget overflow / end of run, with the
  `--multi_eval_squash_buffer` 8096-byte budget and the reference's
  `error_abort` + `ONLY_ERR` semantics: acall aborts on runtime errors, apcall
  suppresses them)
- [x] `--lua_auto_async` (load-time `DetectPossibleAsyncCalls` byte scanner
  rewriting statement-context `redis.call`/`redis.pcall` into `acall`/`apcall`
  for atomic scripts, applied at SCRIPT LOAD and first EVAL while keeping the
  SHA over the original body)
- [x] DFLY (FLOW, SYNC, STARTSTABLE, SHELLO, SYNCID behind the master-side
  `ReplicationManager` in `src/server/replication.rs`; see Replication section)
- [x] Lua extension libraries loaded at interpreter bootstrap (`LoadLibrary`
  order): `cjson` (2.1devel), `struct` (v1.7), `cmsgpack`, `bit` (BitOp
  1.0.3) — pure-Rust ports in `src/commands/lua_libs.rs` mirroring Dragonfly's
  vendored C sources, including the Dragonfly deltas (always-global `cjson`,
  integer-returning `decode`, `int64_t` msgpack sizes) and the C error strings
  (raised as plain Lua strings so `__redis__err__handler` can format them).
  Parity audit complete (29 `lua_libs` tests): cjson array detection counts
  only `LUA_TNUMBER` keys (string keys force objects), config defaults/bounds
  match `json_enum_option`/`json_integer_option` (precision cap 14), cmsgpack
  `table_is_an_array` is exactly `max == count` (empty `{}` packs as `\x90`,
  sparse as a map), struct `c0` coerces the previous result via
  `lua_isnumber` (numeric strings accepted)

### Scripting deviations
- Scripts run on the shard owning their first key (shard 0 for keyless GLOBAL
  scripts), not on the coordinator: each shard owns one interpreter, created on
  the shard thread (mlua is not `Send`), and compiles each script/library once —
  the reference's per-thread `InterpreterManager` model. The run-shard locks the
  shards its transaction needs before executing (`LOCK_AHEAD`, or every shard in
  GLOBAL mode) and holds them for the whole body; subcommands dispatch to peer
  shards from the interpreter (`ShardMsg::ScriptOp`/`ScriptBatch`).
- Script runs are serialized by the coordinator as a deliberate design choice:
  `execute_script`/`execute_function` send `ShardMsg::RunScript` to the resident
  shard and block on the single `ScriptRunResult` — one blocking wait per run,
  not per subcommand as in the pre-refactor coordinator. While a script runs the
  coordinator dispatches no new work, so the run-shard's waits on peer lock acks
  and cross-shard op acks can never deadlock against a concurrent script on
  another shard — the port's analog of the reference's connection thread
  awaiting the multi transaction, where shards suspend per-fiber instead. It
  also keeps `active_tx` on the run-shard owned exclusively by the script for
  its whole body.
- `SCRIPT LATENCY` prints the reference's `base::Histogram::ToString()` text
  dump per SHA (a 154-bucket fixed-boundary histogram, ported in
  `src/core/histogram.rs`, sent as a bulk string — exactly
  `SendVerbatimString`'s RESP2 encoding). The reference merges per-shard
  histograms before printing; the coordinator records a single histogram per
  SHA, so no merge is needed and the output format is identical.
- The async batch is flushed as a `MultiCommandSquasher`-style squashed phase:
  per-shard accumulation runs in a single parallel hop per shard
  (`ShardMsg::ScriptBatch`) when a shard's batch reaches `max_squash_cmd_num`
  (32) or at flush time; keyless and multi-shard commands flush the hop then
  run standalone. An `acall` error aborts the remaining batch
  (`error_abort`), `apcall` errors are suppressed. The byte budget uses the
  reference's per-command heap formula (`BackedArguments` inline caps +
  `sizeof(StoredCmd)`), so the mid-script flush point matches the reference.
- Function library callbacks live in a `__dfly_functions__` table in the Lua
  registry (hidden from scripts, which would otherwise bypass FCALL's locking
  and flag enforcement) and are recreated on first FCALL or when a library's
  sha changes, purging names the replacement dropped;
  `FUNCTION DUMP`/`RESTORE` use an opaque local binary format.
- The whole FUNCTION family (LOAD [REPLACE], DELETE, LIST [WITHCODE], STATS,
  DUMP, RESTORE [FLUSH|APPEND|REPLACE], KILL, HELP) plus FCALL/FCALL_RO is a
  deliberate superset: the reference's `Service::Function` is a stub
  (`main_service.cc:2708`) that only accepts `FUNCTION FLUSH` (returns OK) and
  rejects every other subcommand with
  `Unknown subcommand or wrong number of arguments for '<subcmd>'. Try FUNCTION HELP.`
  The port keeps the full Redis-compatible implementation (for real clients)
  rather than mirroring the stub; `FUNCTION FLUSH` behavior matches.
- `redis.register_function` outside a `FUNCTION LOAD` body errors with Redis's
  exact `redis.register_function can only be called on FUNCTION LOAD command`;
  library and function names are validated (`[A-Za-z0-9_.-]`, like
  `functionVerifyName`).
- Blocking commands in scripts: NOSCRIPT ones (BLPOP, BRPOP, BRPOPLPUSH,
  BZPOPMIN, BZPOPMAX) are rejected with `This Redis command is not allowed from
  script` (mirrors the reference's `CO::NOSCRIPT` mask); BLMOVE, BLMPOP and
  BZMPOP are not NOSCRIPT and run with blocking disabled — a script transaction
  is a multi (`tx->IsMulti()` in the reference), so an empty source replies
  null immediately instead of suspending. The coordinator maps
  `CmdResult::Blocked` to `Ok(RespValue::Nil)` in `execute_script_cmd`.

## Replication (`replication.cc`/`replica.cc`/`dflycmd.cc`/`journal_slice.cc`)
- [x] Master side: `ReplicationManager` (`src/server/replication.rs`) with
  `DFLY FLOW`/`SYNC`/`STARTSTABLE`/`SHELLO`/`SYNCID` — replica sessions, flow
  registration with LSN continuity checks, per-shard full-sync RDB streamers
  (`save_shard_full_sync`: `REDIS0009` + AUX + per-db `SELECTDB` + `FULLSYNC_END`
  + `JOURNAL_OFFSET` + EOF) and stable-sync streamers replaying the per-shard
  journal from a consumer callback.
- [x] Per-shard journal (`src/server/journal.rs`): circular LSN ring with
  eviction and consumer registry, Dragonfly wire format (opcodes `SELECT=6`,
  `EXPIRED=9`, `COMMAND=10`, `PING=13`, `LSN=15`; fresh writer per record so
  COMMAND always carries its own SELECT prefix). Multi-key stores journal
  `FLAG_NO_REDUCED` via coordinator deferred stores (DEL/SET/RESTORE); reduced
  `ShardArgs` records are replay-safe for every other write command.
- [x] Replica side (`src/server/replica.rs`): dedicated threads per flow,
  `PING`/`REPLCONF`/`DFLY` handshake, full-sync `load_rdb` restore through the
  shard message queue, per-record journal apply (single-shard ops acked via
  `ShardMsg::ReplicaOp`, global FLUSHDB/FLUSHALL through a `GlobalBarrier` with
  abort polling), periodic `REPLCONF ACK <lsn>` threads, partial reconnect from
  the last applied LSN (`DFLY FLOW ... <lsn>`).
- [x] Read-only gating on the replica (`-READONLY You can't write against a read
  only replica.`), `ROLE`/`REPLICAOF NO ONE` detach, and a two-instance
  integration test (`tests/replication.rs`) covering full sync, stable sync,
  multi-key/global commands, TTL/expiry propagation, and the read-only gate.

## Module / probabilistic (`bloom_family.cc`, `cms_family.cc`, `cuckoo_filter_family.cc`, `topk_family.cc`)
- [x] BF.ADD, BF.EXISTS, BF.INFO, BF.LOADCHUNK, BF.MADD, BF.MEXISTS, BF.RESERVE,
  BF.SCANDUMP
  (BF.INSERT is not in Dragonfly's `bloom_family.cc` — marked Unsupported; excluded)
- [x] CMS.INCRBY, CMS.INFO, CMS.INITBYDIM, CMS.INITBYPROB, CMS.MERGE, CMS.QUERY
- [x] CF.ADD, CF.ADDNX, CF.COMPACT, CF.COUNT, CF.DEL, CF.EXISTS, CF.INFO,
  CF.INSERT, CF.INSERTNX, CF.MEXISTS, CF.RESERVE
- [x] TOPK.ADD, TOPK.COUNT, TOPK.INCRBY, TOPK.INFO, TOPK.LIST, TOPK.QUERY,
  TOPK.RESERVE

## JSON module (`json_family.cc`) — 24 cmds
- [x] JSON.ARRAPPEND, JSON.ARRINDEX, JSON.ARRINSERT, JSON.ARRLEN, JSON.ARRPOP,
  JSON.ARRTRIM, JSON.CLEAR, JSON.DEBUG, JSON.DEL, JSON.FORGET, JSON.GET,
  JSON.MERGE, JSON.MGET, JSON.MSET, JSON.NUMINCRBY, JSON.NUMMULTBY, JSON.OBJKEYS,
  JSON.OBJLEN, JSON.RESP, JSON.SET, JSON.STRAPPEND, JSON.STRLEN, JSON.TOGGLE,
  JSON.TYPE
  (built on `src/core/json.rs` JSON model, `src/core/jsonpath.rs` JSONPath v2
  engine and `PrimeValue::Json` storage; RESP2-only replies, legacy + enhanced
  paths)

## Test porting backlog
Reference C++ tests in `dragonfly/src/server/*_test.cc` are ported as Rust
integration tests (`tests/*.rs`) that run against the in-process server
(spawned shards/coordinator/IO loop over a real 127.0.0.1 socket, RESP2
`Client` + `Ctx` helpers in `tests/common/mod.rs`).
- [x] `string_family_test.cc` → `tests/string_family.rs` (34 tests). Fixes to
  the port's `SET` (STICK/GET, expiry validation + kMaxExpireDeadlineMs cap,
  past-absolute → NegativeExpire), SETEX/PSETEX (TTL clamp + sec→ms overflow),
  DECRBY i64::MIN overflow, INCRBYFLOAT strict float parse, MSET/MSETNX
  odd-args (`interleave_step_ = 2` parity now enforced in `Command::check_arity`
  like `command_registry.cc:232`), and SETRANGE empty-value no-op; shared
  `redis_range` now clamps negative stops to 0 and rejects `start<0 && stop<start`
  (matches `OpGetRange`).
- [x] `bitops_family_test.cc` → `tests/bitops_family.rs` (27 tests: BITCOUNT,
  BITPOS, GETBIT/SETBIT, BITOP, BITFIELD/BITFIELD_RO). Port needed no source
  fixes; only the test harness's binary helper (`set_b`) was added.
- [x] `generic_family_test.cc` → `tests/generic_family.rs` (47 tests). Source
  fixes: `expiretime_common` rounding (`(at+500)/1000` for EXPIRETIME, raw ms
  for PEXPIRETIME), `parse_scan_opts` accepting `MINMSZ` (reference
  `common.cc:196`), `exec_restore` deleting the old key before insert on
  REPLACE (clears stale expiry), integer-returning commands left unasserted
  (del/lpush/rpush/sadd/hset/zadd/unlink/stick/rm), TIME inside MULTI replies
  `QUEUED`, multi-shard RM drains to cursor "0", and `QuickList::from_bytes`
  int-encoding strict Redis integers only (matches `lpStringToInt64`; keeps
  `-0`/leading zeros as raw strings).
- [x] `list_family_test.cc` → `tests/list_family.rs` (46 tests). Adaptations:
  real wall clock, background-thread connections for blocking commands
  (`Ctx::spawn`/`spawn_b`/`spawn_fn`), controller-internals assertions dropped,
  scheduler-stress tests skipped. Source fixes: `pop` replies nil (not an empty
  array) for a missing key even with a count, `exec_lpos` rejects negative
  MAXLEN ("ERR MAXLEN can't be negative"), reports real (descending) indices
  for RANK < 0, and treats COUNT 0 as unlimited; MULTI-queued blocking commands
  reply nil via a new `exec_multi` flag on the connection (EXEC resets `multi`
  before running the queue, which previously made queued BLPOP with timeout 0
  block forever); a blocked command that wakes to a wrong-type key stays
  blocked instead of erroring (`WrongTypeDoesNotWake`).
- [x] `set_family_test.cc` → `tests/set_family.rs` (31 tests). Source fixes:
  deterministic SRANDMEMBER and SPOP/SRANDMEMBER trailing-argument parsing.
- [x] `zset_family_test.cc` → `tests/zset_family.rs` (47 tests). Source
  fixes: BYLEX range parity and the blocking-timeout wire shape.
- [x] `hll_family_test.cc` → `tests/hll_family.rs` (20 tests).
- [x] `geo_family_test.cc` → `tests/geo_family.rs` (12 tests).
- [x] `bloom_family_test.cc` → `tests/bloom_family.rs` (8 tests).
- [x] `cms_family_test.cc` → `tests/cms_family.rs` (11 tests).
- [x] `cuckoo_filter_family_test.cc` → `tests/cuckoo_family.rs` (35 tests).
- [x] `topk_family_test.cc` → `tests/topk_family.rs` (67 tests).
- [x] `hset_family_test.cc` → `tests/hset_family.rs` (44 tests). Adaptations:
  real wall clock (field-TTL assertions use ranges where a second boundary can
  land between commands; `hexpire_no_expire_early` widens the TTL to 10s with a
  1.2s sleep), RESP3 parameterized cases (Get/HRandFieldRespFormat) assert only
  the RESP2 replies, DEBUG OBJECT encoding assertions dropped, SHRINK is a stub
  replying 0. Source fixes: HINCRBY replies "hash value is not an integer" for
  non-integer stored values and "increment or decrement would overflow" on
  i64 overflow; HINCRBYFLOAT rejects non-finite deltas ("increment would
  produce NaN or Infinity"), rejects stored NaN/out-of-range values ("hash
  value is not a float") while routing stored ±inf through the finite check;
  HSCAN scans small (listpack) hashes whole, ignoring COUNT; shared
  `parse_double` now rejects f64 overflow ("1e999", "1.8E+308") like the
  reference `ParseDouble`/`TryParseNum`.
- [x] `stream_family_test.cc` → the in-file `streams.rs` tests module (12
  tests: Add, AddExtended, Xclaim, XAutoClaim,
  AutoClaimPelItemsFromAnotherConsumer, AutoClaimDelCount,
  XClaimWithNonExistentGroup, XsetIdSmallerMaxDeleted, XAutoClaimEmptyConsumer,
  XInfoGroups, XInfoConsumers, XInfoStream) plus a RESP2 wire-format probe in
  `tests/stream_family.rs`. Unlike the other families the stream tests run
  directly against `exec_*` on a `DbSlice` (no socket), because the reply
  shape and per-connection watermark logic is asserted at the unit level.
  Source fixes: XREAD/XREADGROUP reply with one nested `[key, [entries]]`
  pair per stream and empty reads send a null *array*, not a null bulk (new
  `RespValue::NilArray`; MULTI no-block and woken-XREADGROUP replies use it);
  blocked `$` watermarks are resolved per connection (conn_id threaded through
  `OpContext`/`TxCtx`/`run_exec`), so concurrent readers keep their own
  watermark; XADD accepts ms-only ids (`xadd key 5` -> `5-0`) with the
  sequence auto-completed like Redis; stream trim chunks live entries only
  (counting tombstones underflowed the node-length subtraction after a MAXLEN
  trim); XGROUP HELP bypasses the -3 arity and is NOSCRIPT from scripts;
  `extract_movable_keys` scans past a stray STREAMS marker (a consumer
  literally named STREAMS).
- [x] The remaining `stream_family_test.cc` cases land in two places: the
  `streams.rs` unit module (46 tests) and the blocking cluster in
  `tests/stream_family.rs` (13 tests: XReadBlock, XReadGroupBlock,
  XReadGroupBlockDelconsumer, XReadBlockOnEmptiedStream,
  XReadBlockIgnoresEntriesBelowRequestedId, XReadBlockOnMaxMsId,
  XReadGroupBlockWakeOnDeletedStream, XReadBlockStaysBlockedOnDeletedStream,
  XReadGroupBlockWakeOnRetypedStream, XReadGroupBlockWakeOnFlushDb,
  XReadGroupBlockHonorsCount, Issue854, probe). Blocked readers are re-run on
  the coordinator's 20ms poll, so wakes assert delivered data instead of
  `IsConnBlocked`. Source fixes: a retyped key wakes a blocked XREADGROUP with
  WRONGTYPE (list blocking keeps WrongTypeDoesNotWake); `XGROUP HELP` from a
  script is rejected before the arity check (the rewritten `_XGROUP_HELP` has
  arity 2). `XReadGroupBlockIgnoresWakeFromRemovedEntry` is skipped: EXEC
  dispatches queued commands as separate transactions with a pending-retry
  between them, so MULTI is not atomic with respect to the woken reader.
- [x] `server_family_test.cc` (in progress) → `tests/server_family.rs`. The
  `ClientTracking*` suite is ported (16 tests: ClientTrackingOnAndOff,
  ToggleTrackingOnAndOff, ClientTrackingReadKey, ClientTrackingOptIn,
  ClientTrackingMulti, ClientTrackingCompatibilityMulti, ClientTrackingMultiOptIn,
  ClientTrackingOptOut, ClientTrackingMultiOptOut, ClientTrackingUpdateKey,
  ClientTrackingDeleteKey, ClientTrackingRenameKey, ClientTrackingExpireKey,
  ClientTrackingSelectDb, ClientTrackingNonTransactionalBug,
  ClientTrackingLuaBug), alongside the slowlog/config/client-list/debug/
  info/memory tests from earlier sessions. Feature work behind the tracking
  suite: the RESP3 push-message path (`drain_bus` appends `is_push` frames
  straight to the connection, FIFO before the triggering write's reply);
  per-connection tracking state (`seq_num`, `should_track` computed at dispatch,
  OPTIN/OPTOUT/NOLOOP, CACHING stickiness through MULTI/EXEC/DISCARD); the
  shard-global tracking map keyed by value (a write invalidates every tracking
  connection; FLUSHDB/FLUSHALL broadcast one null-keyed push from shard 0);
  `TrackIfNeeded` recorded after the command runs — like the reference's
  post-run tracking callback — so a lazy-expiry delete a read triggers
  invalidates pre-read trackers but not the read's own freshly tracked key.
  Adaptations: `ClientTrackingNonTransactionalBug` only asserts the port errors
  (no `CLUSTER SLOTS`), `ClientTrackingExpireKey` drives the fake clock with
  `advance`, and the suite gates on `HELLO 3`.
- [x] CLIENT PAUSE ported (reference `ClientPauseCmd`, server_family.cc:3953):
  `PauseMode::{All,Write}` + a `ClientPause` struct (`begin`/`end`/
  `wait_until_clear`, `Mutex`+`Condvar`) on `ServerEnv`. The single IO thread
  mirrors the reference's per-connection `Pause()` fiber gates with a
  `pause_check` at dispatch (before XGROUP-HELP/arity): `is_write` from
  `FLAG_WRITE`/PUBLISH/eval/function-minus-`FLAG_READONLY`/EXEC-write like
  `main_service.cc:843`. `CLIENT PAUSE <ms> [WRITE|ALL]` spawns a detached
  timer thread (non-blocking `+OK` like the reference's pause fiber); ALL
  blocks everything, WRITE only writes; `timeout` ms minimum validated
  (`ERR Invalid timeout`). `client_pause` test (server_family_test.cc:271)
  ports with real `Instant` timings.
- [x] `ReadTcpInfo`/`GetTcpSocketInfoIPv6` (server_family_test.cc:31, 68):
  `GetSocketInfo` ported in `src/server/socket_utils.rs` (mirrors
  `facade/socket_utils.cc` + the TCP half of helio's `io/proc_reader`). On
  Linux it `fstat`s the fd for the inode, resolves the family via
  `getsockname`, scans `/proc/net/tcp` (`AF_INET`) or `/proc/net/tcp6`
  (`AF_INET6`) and renders `State: ..., Local: ..., Remote: ..., Inode: ...`
  (`TcpStateToString` states, IPv4 from the little-endian hex dump, IPv6 via
  RFC 5952 `Ipv6Addr`, exactly the reference's `inet_ntop` output); non-Linux
  platforms return the reference's fixed
  `"socket info not available on this platform"`. The proc parser is a pure
  fixture-tested function (9 unit tests run everywhere), and the reference's
  Linux-only socket tests are ported as `#[cfg(target_os = "linux")]` cases
  in `tests/server_family.rs` (`read_tcp_info`, `get_tcp_socket_info_ipv6`)
  plus a portable `socket_info_invalid_fd`. The port has no TLS, so (like the
  reference's only consumers, TLS accept error logs) nothing calls it from
  the server itself.
- [x] `multi_test.cc` → `tests/multi.rs` (25 tests): MULTI/EXEC queueing
  (`multi_and_flush`/`multi_with_error`/`multi_empty`/`multi_seq`/
  `multi_without_tx`/`multi_global_commands`/`multi_rename`/`multi_types`),
  EVAL/SCRIPT LOAD in transactions (`multi_and_eval`), WATCH validation
  (`watch`), RESET semantics (`reset_returns_reset_string`,
  `reset_clears_multi_block`, `reset_clears_watch_state`, `reset_selects_db0`),
  the script-flag suite (`script_flags_invalid_sha`/`script_flags_embedded`,
  `cjson_decode_integer_behavior`, `script_bad_command`), and the EVAL family
  (`eval_ro`/`eval_sha_ro`, `eval_expiration`, `eval_select`, `general_acall`,
  `acall_undeclared_keys`, `multi_eval_mode_conflict`). Source fixes:
  FLAG_LOCAL subcommands (PING/ECHO/TIME/LASTSAVE/SELECT) now run inline in the
  script dispatch (`script_local` in `shard.rs`) instead of hitting a shard's
  `local_stub`; `script_select` rejects LOCK_AHEAD scripts ("SELECT is not
  allowed in regular EXEC/EVAL"), range-checks the DB ("ERR DB index is out of
  range") and switches the script DB for GLOBAL/NON_ATOMIC scripts;
  script-local errors are raised from `redis.call` (not returned as replies);
  a MULTI-queued `allow-undeclared-keys` EVAL reports "Multi mode conflict when
  running eval in multi transaction. Multi mode is: LOCK_AHEAD, eval mode is:
  GLOBAL" (`no_block && undeclared_keys` in `execute_script`). Adaptations:
  EVAL/EVALSHA error strings are matched by substring (wrapped as "ERR Error
  running script (call to <sha>): ..."), "OK" replies by content (status vs
  bulk), EXEC-abort matchers use the port's nil reply, EXPIRE-based checks run
  under `clock_guard`, and the `MultiEvalTest`-fixture / CONFIG-gated /
  FT-gated / INFO-keyspace cases are skipped (documented in the test header).
- [x] Remaining `server_family_test.cc` audit: all 42 `ServerFamilyTest` cases
  are ported. The reference has no `DFLY_USE_CLUSTER`/cluster-only gates (the
  earlier "cluster-only paths remain" note was stale; there is no cluster mode
  in the reference test file).
- [x] `DEBUG UNIQ-STRS` + `string_stats_test.cc` → `tests/string_stats.rs`
  (6 tests, 1:1: HashWithDuplicateFields, SetWithUniqueMembers,
  SetWithDuplicateMembers, MultipleTypes, EmptyDatabase, NumberKeys).
  `UniqueStrings`/`PerShardStats` live in `src/core/string_stats.rs`: per-entry
  `total_count`/`total_bytes` plus a dense HLL (`create_dense_hll`), merging
  per-object-type across shards with `pfmerge` (the register-wise max must
  include both operands, exactly the reference's `{other.counter_, counter_}`
  input pair). Hash/Set/ZSet count member bytes; list ints are counted by
  their decimal rendering (reference `AddString` FastIntToBuffer), so `007`
  dedups across keys. Average length renders via `%.6g` (`absl::AlphaNum`),
  now a shared `util::g6_format`. Dispatch: `DEBUG UNIQ-STRS` is not
  `FLAG_GLOBAL`, so `dispatch_keyed` special-cases it to all shards;
  `CmdResult::UniqueStrings(PerShardStats)` carries the per-shard counters to
  `merge_uniq_strs` (rendering `___begin unique string stats___`, per-type
  `OBJECT:<type>` + 64-dash blocks, `___end unique string stats___`).
- [x] `json_family_test.cc` → `tests/json_family.rs` (86 tests, 1:1 with the
  reference: `SetGetBasic`..`SetFullJsonInvalidOnNewKey`; the RESP3-parameterized
  `*NestedArrayBug` cases are ported as RESP2-only `*_flat` variants and
  `Type`/`NumericOperationsResp2Resp3` as the RESP2-only `type_v2`/
  `numeric_operations_resp2`).
- [x] Scripting end-to-end coverage in `tests/functions.rs` (11 tests). There is
  no upstream `function_family_test.cc` (and no FUNCTION tests in dragonfly's
  tree at all), so these are authored from the port's own documented semantics
  (`local_function` in `src/server/mod.rs` + `execute_function` in
  `src/server/coordinator.rs`). They run over the socket through the shared
  `ScriptMgr`, exercising both dispatch halves: FUNCTION admin on the IO thread
  (`handle_local`) and FCALL/FCALL_RO dispatched from the coordinator
  (`is_function_cmd`) to the function's resident shard. Coverage:
  LOAD/REPLACE/DELETE/FLUSH round-trips, duplicate library/function-name
  enforcement, the REPLACE purge path for dropped names (per-shard `loaded_libs`
  + `purge_functions`), LIST
  LIBRARYNAME/WITHCODE (maps flatten to RESP2 arrays), STATS fields, HELP,
  NOTBUSY KILL, bad payloads (missing metadata / no functions / invalid engine /
  missing name / non-function callback / top-level redis.call), FCALL errors
  (unknown function, bad numkeys, numkeys>args, wrapped runtime error),
  multi-key writes over declared keys, undeclared-key rejection in atomic mode,
  no-writes + FCALL_RO write rejection, FUNCTION/FCALL NOSCRIPT from EVAL and
  from inside a function, and DFLY replication-control rejection
  ("ERR DFLY replication control is not supported"; FLOW/SYNC/STARTSTABLE live
  in `tests/replication.rs`).

## Priority order
1. Core data types: bitops, keys/generic, string, list, hash, set, zset, stream
   (biggest day-to-day surface, fits existing architecture).
2. GEO (self-contained, well-specified).
3. Scripting (EVAL/FUNCTION/MULTI) + pub/sub (need connection/runtime hooks).
   - Done: EVAL family + SCRIPT + pub/sub are ported, FUNCTION/FCALL have
     end-to-end coverage in `tests/functions.rs`, and the replication-related
     DFLY rejection is asserted; only the live-replication paths remain
     (covered by `tests/replication.rs`).
4. Server/admin + SORT.
5. Probabilistic structures (BF/CF/CMS/TOPK) and JSON (large; need new value
   types in `src/core/value.rs`).
   - Done: BF/CF/CMS/TOPK and JSON families are ported.
