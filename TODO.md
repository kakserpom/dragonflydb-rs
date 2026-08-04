# DragonflyDB Rust Port — Parity Tracking

Goal: 100% parity with DragonflyDB — every command in the reference
`dragonfly/src/server/*.cc` registries ported to `src/commands/exec/*.rs`,
with reference tests (`*_test.cc`) ported to Rust unit tests.

Command counts: reference **289**, ported **235**, missing **54**.

Legend:
- [x] ported
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
- [x] AUTH, BGSAVE, CLIENT, COMMAND, CONFIG, DBSIZE, DEBUG, DISCARD, EXEC,
  FLUSHALL, FLUSHDB, HELLO, INFO, LASTSAVE, LATENCY, MEMORY, MULTI, PING, QUIT,
  RESET, ROLE, SAVE, SLOWLOG, UNWATCH, WATCH
- [ ] ADDREPLICAOF, DFLY, EVAL, EVALSHA, FUNCTION,
  MODULE, MONITOR, PSUBSCRIBE, PUBLISH, PUBSUB,
  PUNSUBSCRIBE, REPLCONF, REPLICAOF, REPLTAKEOVER, SCRIPT, SHRINK,
  SHUTDOWN, SLAVEOF, SPUBLISH, SSUBSCRIBE, SUBSCRIBE, SUNSUBSCRIBE,
  UNSUBSCRIBE, WAIT

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
4. Server/admin + SORT.
5. Probabilistic structures (BF/CF/CMS/TOPK) and JSON (large; need new value
   types in `src/core/value.rs`).
   - Done: BF/CF/CMS/TOPK and JSON families are ported.
