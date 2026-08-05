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
- [x] FUNCTION (LOAD [REPLACE], DELETE, FLUSH, LIST [LIBRARYNAME] [WITHCODE],
  STATS, DUMP, RESTORE [FLUSH|APPEND|REPLACE], KILL, HELP backed by a shared
  library registry with per-library `#!lua name=...` headers, `redis.register_function`
  collection, and duplicate library/function-name enforcement)
- [x] FCALL, FCALL_RO (run registered functions on the coordinator: lazy library
  load into its interpreter, `(keys, args)` tables, per-key transaction locking,
  `no-writes`/`allow-undeclared-keys` flag enforcement)
- [x] SCRIPT (EXISTS, LIST, FLUSH, LATENCY, GC, FLAGS, LOAD, HELP backed by a
  shared `ScriptMgr` cache of compiled scripts; `SCRIPT FLAGS` and `--!df`
  comment flags set per-script params)
- [x] EVAL, EVALSHA, EVAL_RO, EVALSHA_RO (sandboxed Lua interpreter on the
  coordinator thread: compile-before-cache, `KEYS`/`ARGV` globals, per-key
  transaction locking, cross-shard `redis.call`/`redis.pcall` dispatch, script
  error wrapping `ERR Error running script (call to <sha>): ...`, NOSCRIPT and
  read-only write rejection)
- [x] `redis.acall`/`redis.apcall` (async command batching flushed as one
  squashed phase on sync calls / budget overflow / end of run, with the
  `--multi_eval_squash_buffer` 8096-byte budget and the reference's
  `error_abort` + `ONLY_ERR` semantics: acall aborts on runtime errors, apcall
  suppresses them)
- [x] `--lua_auto_async` (load-time `DetectPossibleAsyncCalls` byte scanner
  rewriting statement-context `redis.call`/`redis.pcall` into `acall`/`apcall`
  for atomic scripts, applied at SCRIPT LOAD and first EVAL while keeping the
  SHA over the original body)
- [~] DFLY (registered, rejects: no replication stack in `Cargo.toml`)

### Scripting deviations
- Scripts run on a single coordinator-side interpreter (taken from the sandbox
  pool), not one per shard; EVAL is serialized by the coordinator and holds
  the script's locks for its whole body.
- `SCRIPT LATENCY` aggregates per-SHA usec runs on the coordinator into a
  single summary per script (the reference merges per-shard
  `base::Histogram` dumps).
- The async batch is squashed sequentially through the shards rather than
  through a real `MultiCommandSquasher` single-hop; the byte budget uses an
  approximate per-command heap cost (arg bytes + 64), so the mid-script flush
  point may differ slightly from the reference.
- Function library callbacks live in a `__dfly_functions__` table in the Lua
  registry (hidden from scripts, which would otherwise bypass FCALL's locking
  and flag enforcement) and are recreated on first FCALL or when a library's
  sha changes, purging names the replacement dropped;
  `FUNCTION DUMP`/`RESTORE` use an opaque local binary format.
- `redis.register_function` outside a `FUNCTION LOAD` body errors with Redis's
  exact `redis.register_function can only be called on FUNCTION LOAD command`;
  library and function names are validated (`[A-Za-z0-9_.-]`, like
  `functionVerifyName`).
- `FUNCTION KILL` is soft: it sets a flag the coordinator polls between
  `redis.call` dispatches (idle reply is Redis's `NOTBUSY`), so a CPU-bound
  tight loop that never calls out cannot be interrupted.

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
Reference C++ tests in `dragonfly/src/server/*_test.cc` should be ported to
Rust unit tests (`#[cfg(test)]` modules in the matching `src/commands/exec/*.rs`),
mirroring how `StringFamilyTest.ClThrottle` was ported (virtual clock, pure core
function + thin exec wrapper).

## Priority order
1. Core data types: bitops, keys/generic, string, list, hash, set, zset, stream
   (biggest day-to-day surface, fits existing architecture).
2. GEO (self-contained, well-specified).
3. Scripting (EVAL/FUNCTION/MULTI) + pub/sub (need connection/runtime hooks).
   - Done: EVAL family + SCRIPT + pub/sub are ported; FUNCTION and the
     replication-related DFLY remain.
4. Server/admin + SORT.
5. Probabilistic structures (BF/CF/CMS/TOPK) and JSON (large; need new value
   types in `src/core/value.rs`).
   - Done: BF/CF/CMS/TOPK and JSON families are ported.
