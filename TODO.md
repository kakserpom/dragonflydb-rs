# DragonflyDB Rust Port — Parity Tracking

Goal: 100% parity with DragonflyDB — every command in the reference
`dragonfly/src/server/*.cc` registries ported to `src/commands/exec/*.rs`,
with reference tests (`*_test.cc`) ported to Rust unit tests.

Command counts: reference **289**, ported **129**, missing **160**.

Legend:
- [x] ported
- [ ] missing

## String family (`string_family.cc`) — 25 cmds
- [x] APPEND, DECR, DECRBY, DIGEST, GAT, GET, GETDEL, GETEX, GETRANGE, GETSET,
  INCR, INCRBY, INCRBYFLOAT, MGET, MSET, MSETNX, PREPEND, PSETEX, SET, SETEX,
  SETNX, SETRANGE, STRLEN, SUBSTR, CL.THROTTLE

## Key/generic family (`generic_family.cc`) — 33 cmds
- [x] DEL, ECHO, EXISTS, EXPIRE, EXPIREAT, KEYS, PERSIST, PEXPIRE, PEXPIREAT,
  PING, PTTL, SELECT, TIME, TTL, TYPE
- [ ] COPY, DELEX, DUMP, EXPIRETIME, FIELDEXPIRE, FIELDTTL, MOVE, PEXPIRETIME,
  RANDOMKEY, RENAME, RENAMENX, RESTORE, RM, SCAN, SORT, STICK, TOUCH, UNLINK

## Bit ops (`bitops_family.cc`) — 6 cmds
- [x] BITCOUNT, BITFIELD, BITFIELD_RO, BITOP, BITPOS, GETBIT, SETBIT

## List family (`list_family.cc`) — 22 cmds
- [x] LINDEX, LLEN, LPOP, LPOS, LPUSH, LPUSHX, LRANGE, LREM, LSET, LTRIM, RPOP,
  RPUSH, RPUSHX
- [ ] BLMOVE, BLMPOP, BLPOP, BRPOP, BRPOPLPUSH, LINSERT, LMOVE, LMPOP, RPOPLPUSH

## Hash family (`hset_family.cc`) — 21 cmds
- [x] HDEL, HEXISTS, HGET, HGETALL, HGETEX, HEXPIRE, HINCRBY, HINCRBYFLOAT,
  HKEYS, HLEN, HMGET, HMSET, HPEXPIRETIME, HRANDFIELD, HSCAN, HSET, HSETEX,
  HSETNX, HSTRLEN, HTTL, HVALS

## Set family (`set_family.cc`) — 18 cmds
- [x] SADD, SADDEX, SCARD, SDIFF, SDIFFSTORE, SINTER, SINTERCARD,
  SINTERSTORE, SISMEMBER, SMEMBERS, SMISMEMBER, SMOVE, SPOP, SRANDMEMBER,
  SREM, SSCAN, SUNION, SUNIONSTORE

## Zset family (`zset_family.cc`) — 35 cmds
- [x] ZADD, ZCARD, ZCOUNT, ZINCRBY, ZMSCORE, ZPOPMAX, ZPOPMIN, ZRANGE,
  ZRANGEBYLEX, ZRANGEBYSCORE, ZRANK, ZREM, ZREMRANGEBYRANK, ZREMRANGEBYSCORE,
  ZREVRANGEBYSCORE, ZREVRANK, ZSCORE
- [ ] BZMPOP, BZPOPMAX, BZPOPMIN, ZDIFF, ZDIFFSTORE, ZINTER, ZINTERCARD,
  ZINTERSTORE, ZLEXCOUNT, ZMPOP, ZRANDMEMBER, ZRANGESTORE, ZREMRANGEBYLEX,
  ZREVRANGE, ZREVRANGEBYLEX, ZSCAN, ZUNION, ZUNIONSTORE

## Stream family (`stream_family.cc`) — 15 cmds
- [x] XACK, XADD, XAUTOCLAIM, XCLAIM, XDEL, XGROUP, XINFO, XLEN, XPENDING,
  XRANGE, XREAD, XREADGROUP, XREVRANGE, XSETID, XTRIM

## HyperLogLog (`hll_family.cc`) — 3 cmds
- [ ] PFADD, PFCOUNT, PFMERGE

## Geo (`geo_family.cc`) — 7 cmds
- [ ] GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEORADIUSBYMEMBER, GEOSEARCH

## Server / admin (`server_family.cc` + `main_service.cc`)
- [x] AUTH, CLIENT, COMMAND, CONFIG, DBSIZE, FLUSHALL, FLUSHDB, HELLO, INFO,
  PING, QUIT, RESET
- [ ] ADDREPLICAOF, BGSAVE, DEBUG, DFLY, DISCARD, EVAL, EVALSHA, EXEC, FUNCTION,
  LASTSAVE, LATENCY, MEMORY, MODULE, MONITOR, MULTI, PSUBSCRIBE, PUBLISH, PUBSUB,
  PUNSUBSCRIBE, REPLCONF, REPLICAOF, REPLTAKEOVER, ROLE, SAVE, SCRIPT, SHRINK,
  SHUTDOWN, SLAVEOF, SLOWLOG, SPUBLISH, SSUBSCRIBE, SUBSCRIBE, SUNSUBSCRIBE,
  UNSUBSCRIBE, UNWATCH, WAIT, WATCH

## Module / probabilistic (`bloom_family.cc`, `cms_family.cc`, `cuckoo_filter_family.cc`, `topk_family.cc`)
- [ ] BF.ADD, BF.EXISTS, BF.INFO, BF.LOADCHUNK, BF.MADD, BF.MEXISTS, BF.RESERVE,
  BF.SCANDUMP
- [ ] CMS.INCRBY, CMS.INFO, CMS.INITBYDIM, CMS.INITBYPROB, CMS.MERGE, CMS.QUERY
- [ ] CF.ADD, CF.ADDNX, CF.COMPACT, CF.COUNT, CF.DEL, CF.EXISTS, CF.INFO,
  CF.INSERT, CF.INSERTNX, CF.MEXISTS, CF.RESERVE
- [ ] TOPK.ADD, TOPK.COUNT, TOPK.INCRBY, TOPK.INFO, TOPK.LIST, TOPK.QUERY,
  TOPK.RESERVE

## JSON module (`json_family.cc`) — 24 cmds
- [ ] JSON.ARRAPPEND, JSON.ARRINDEX, JSON.ARRINSERT, JSON.ARRLEN, JSON.ARRPOP,
  JSON.ARRTRIM, JSON.CLEAR, JSON.DEBUG, JSON.DEL, JSON.FORGET, JSON.GET,
  JSON.MERGE, JSON.MGET, JSON.MSET, JSON.NUMINCRBY, JSON.NUMMULTBY, JSON.OBJKEYS,
  JSON.OBJLEN, JSON.RESP, JSON.SET, JSON.STRAPPEND, JSON.STRLEN, JSON.TOGGLE,
  JSON.TYPE

## Test porting backlog
Reference C++ tests in `dragonfly/src/server/*_test.cc` should be ported to
Rust unit tests (`#[cfg(test)]` modules in the matching `src/commands/exec/*.rs`),
mirroring how `StringFamilyTest.ClThrottle` was ported (virtual clock, pure core
function + thin exec wrapper).

## Priority order
1. Core data types: bitops, keys/generic, string, list, hash, set, zset, stream
   (biggest day-to-day surface, fits existing architecture).
2. HLL, GEO (self-contained, well-specified).
3. Scripting (EVAL/FUNCTION/MULTI) + pub/sub (need connection/runtime hooks).
4. Server/admin + SORT.
5. Probabilistic structures (BF/CF/CMS/TOPK) and JSON (large; need new value
   types in `src/core/value.rs`).
