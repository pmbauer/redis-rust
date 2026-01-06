# Delancie Redis Compatibility Plan

## Overview

Implementation plan to support all Redis commands used by Delancie job queue system.

## Command Analysis

### Currently Implemented (Working)

| Category | Commands | Status |
|----------|----------|--------|
| Strings | GET, SET, SETEX, SETNX, APPEND, STRLEN, MGET, MSET | Working |
| Hashes | HGET, HSET, HDEL, HGETALL, HKEYS, HVALS, HLEN, HEXISTS | Working |
| Lists | LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX, LRANGE | Working |
| Sets | SADD, SREM, SMEMBERS, SISMEMBER, SCARD | Working |
| Sorted Sets | ZADD, ZREM, ZSCORE, ZRANK, ZRANGE, ZREVRANGE, ZCARD | Working |
| Keys | EXISTS, EXPIRE, TTL, PTTL, PERSIST, KEYS, TYPE | Working |
| Numeric | INCR, DECR, INCRBY, DECRBY | Working |
| Server | PING, INFO, FLUSHDB, FLUSHALL | Working |

### Needs Implementation/Fix

| Priority | Command | Description | Complexity |
|----------|---------|-------------|------------|
| P0 | SET options | NX/XX/EX/PX/GET flags | Medium |
| P0 | DEL multi-key | DEL key1 key2 key3 | Easy |
| P0 | HINCRBY | Fix existing bug | Easy |
| P1 | LSET | Set element at index | Easy |
| P1 | LTRIM | Trim list to range | Easy |
| P1 | RPOPLPUSH | Atomic pop-push | Medium |
| P1 | LMOVE | Move between lists | Medium |
| P1 | ZCOUNT | Count in score range | Easy |
| P1 | ZRANGEBYSCORE | Get by score range | Medium |
| P2 | SCAN | Iterate keys | Medium |
| P2 | HSCAN | Iterate hash fields | Medium |
| P2 | ZSCAN | Iterate sorted set | Medium |
| P3 | EVAL | Lua script execution | Hard |
| P3 | MULTI/EXEC | Transactions | Hard |
| P3 | WATCH/UNWATCH | Optimistic locking | Hard |

---

## Phase 1: Critical Fixes (P0)

### 1.1 SET Command Options

**Current:** `SET key value` only
**Required:** `SET key value [NX|XX] [EX seconds|PX ms] [GET]`

```rust
// New Command variant
SetWithOptions {
    key: String,
    value: SDS,
    nx: bool,        // Only set if not exists
    xx: bool,        // Only set if exists
    ex: Option<i64>, // Expire in seconds
    px: Option<i64>, // Expire in milliseconds
    get: bool,       // Return old value
}
```

**Files:** `src/redis/commands.rs`, `src/redis/mod.rs`

### 1.2 Multi-key DEL

**Current:** `DEL key` (single key)
**Required:** `DEL key1 key2 ... keyN`

```rust
// Change from:
Del(String)
// To:
Del(Vec<String>)
```

**Files:** `src/redis/commands.rs`

### 1.3 Fix HINCRBY

**Issue:** Returns error instead of incremented value
**Fix:** Check type coercion and error handling

**Files:** `src/redis/commands.rs`

---

## Phase 2: List Operations (P1)

### 2.1 LSET - Set element at index

```rust
Command::LSet(key, index, value) => {
    // Set list[index] = value
    // Return OK or error if out of range
}
```

### 2.2 LTRIM - Trim list to range

```rust
Command::LTrim(key, start, stop) => {
    // Keep only elements in range [start, stop]
    // Return OK
}
```

### 2.3 RPOPLPUSH - Atomic pop and push

```rust
Command::RPopLPush(source, destination) => {
    // Atomically: RPOP source, LPUSH destination
    // Return the moved element
}
```

### 2.4 LMOVE - Move element between lists

```rust
Command::LMove(source, dest, where_from, where_to) => {
    // where_from/to: LEFT or RIGHT
    // Atomically move element
}
```

---

## Phase 3: Sorted Set Operations (P1)

### 3.1 ZCOUNT - Count in score range

```rust
Command::ZCount(key, min, max) => {
    // Count members with score between min and max
    // Support -inf, +inf, (exclusive
}
```

### 3.2 ZRANGEBYSCORE - Get by score range

```rust
Command::ZRangeByScore(key, min, max, with_scores, limit) => {
    // Return members with score in range
    // Optional WITHSCORES
    // Optional LIMIT offset count
}
```

---

## Phase 4: Iteration Commands (P2)

### 4.1 SCAN - Iterate keys

```rust
Command::Scan(cursor, pattern, count) => {
    // Return [next_cursor, [keys...]]
    // Cursor-based iteration
}
```

### 4.2 HSCAN - Iterate hash fields

```rust
Command::HScan(key, cursor, pattern, count) => {
    // Return [next_cursor, [field, value, ...]]
}
```

### 4.3 ZSCAN - Iterate sorted set

```rust
Command::ZScan(key, cursor, pattern, count) => {
    // Return [next_cursor, [member, score, ...]]
}
```

---

## Phase 5: Transactions & Scripting (P3)

### 5.1 Basic Transactions

```rust
// Per-connection transaction state
struct TransactionState {
    in_multi: bool,
    queued_commands: Vec<Command>,
    watched_keys: HashMap<String, u64>, // key -> version
}

Command::Multi => start_transaction()
Command::Exec => execute_queued()
Command::Discard => abort_transaction()
Command::Watch(keys) => watch_keys()
Command::Unwatch => clear_watches()
```

**Note:** CRDT architecture complicates transactions. May need:
- Single-shard transactions only
- Or transaction coordinator actor

### 5.2 Lua Scripting (EVAL)

```rust
Command::Eval(script, numkeys, keys, args) => {
    // Embed Lua interpreter (mlua crate)
    // Provide redis.call() function
}
```

**Dependencies:** `mlua` or `rlua` crate

**Complexity:** High - need to:
1. Parse Lua scripts
2. Provide redis.call() binding
3. Handle atomicity guarantees
4. Key extraction for sharding

---

## Implementation Order

```
Week 1: Phase 1 (P0 - Critical)
├── SET options (NX/XX/EX/PX/GET)
├── Multi-key DEL
└── Fix HINCRBY

Week 2: Phase 2 (P1 - List ops)
├── LSET
├── LTRIM
├── RPOPLPUSH
└── LMOVE

Week 3: Phase 3 (P1 - Sorted set ops)
├── ZCOUNT
└── ZRANGEBYSCORE

Week 4: Phase 4 (P2 - Iteration)
├── SCAN
├── HSCAN
└── ZSCAN

Week 5-6: Phase 5 (P3 - Transactions)
├── MULTI/EXEC/DISCARD
├── WATCH/UNWATCH
└── Basic EVAL support
```

---

## Testing Strategy

### Differential Tests
Each new command gets added to `tests/redis_equivalence_test.rs`:
- Compare against real Redis
- Test edge cases
- Test error conditions

### Delancie Integration Tests
```bash
# After each phase, run Delancie's test suite against redis-rust
cd delancie
REDIS_URL=redis://localhost:3000 go test ./...
```

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/redis/mod.rs` | Add Command variants |
| `src/redis/commands.rs` | Parser + executor |
| `src/redis/data/list.rs` | LSET, LTRIM helpers |
| `src/redis/data/sorted_set.rs` | ZCOUNT, ZRANGEBYSCORE |
| `tests/redis_equivalence_test.rs` | New test cases |
| `Cargo.toml` | Add mlua (for EVAL) |

---

## Success Criteria

- [ ] All P0 commands pass equivalence tests
- [ ] All P1 commands pass equivalence tests
- [ ] All P2 commands pass equivalence tests
- [ ] Delancie Lua scripts execute correctly
- [ ] Delancie integration tests pass

---

## Risk Assessment

| Risk | Mitigation |
|------|------------|
| EVAL complexity | Start with subset of Lua, add as needed |
| Transaction + CRDT conflict | Document limitations, single-shard only |
| Performance regression | Benchmark after each phase |
| Breaking existing commands | Run full test suite after each change |
