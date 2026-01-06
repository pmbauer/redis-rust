# Performance Plan V2 - Combined Abseil + Profiling Analysis

Based on Abseil Performance Tips and actual profiling of this codebase.

---

## Executive Summary

**Current State (after Phase 0.2 - SET batching):**
| Operation | Redis 7.4 | Redis 8.0 | Rust | % of R7 | % of R8 | Status |
|-----------|-----------|-----------|------|---------|---------|--------|
| SET P=1 | 184K | 175K | 161K | 87% | **92%** | ✅ OK |
| GET P=1 | 192K | 153K | 178K | **93%** | **116%** | ✅ BEATS R8! |
| SET P=16 | 1.43M | 1.54M | 1.19M | 83% | **77%** | ✅ FIXED (+18%) |
| GET P=16 | 1.67M | 1.85M | 1.54M | 92% | **83%** | ✅ OK |

**Phase 0.1 Complete**: Fixed double parsing regression. P=1 performance restored.
**Phase 0.2 Complete**: Added SET batching. SET P=16 improved from 59% to 77-80%.

**Key Win**: GET P=1 now **beats Redis 8.0 by 16%**!

---

## Profiling Analysis

### Layer Performance
| Layer | Throughput | Notes |
|-------|-----------|-------|
| CommandExecutor (pure) | 8-14M ops/sec | Core execution is fast |
| With actors + network | 180-200K ops/sec | 50-100x overhead |

### Hot Path (per command)
```
TCP read → collect_get_keys() → try_fast_path() → parse → hash → actor msg → oneshot → execute → encode → TCP write
           ↑                    ↑                                   ↑
           NEW OVERHEAD         NEW OVERHEAD                        MAIN BOTTLENECK
```

### Identified Issues

1. **Double Parsing (NEW REGRESSION)**
   - `collect_get_keys()` runs on EVERY read, even single commands
   - Then `try_fast_path()` re-parses if not a batched GET
   - Cost: ~20% overhead for P=1 scenarios

2. **Oneshot Channel Per Command (MAIN BOTTLENECK)**
   - Creates channel pair for every single command
   - Allocator pressure + synchronization overhead
   - Cost: ~40% of non-network overhead

3. **SET Not Batched (MISSING OPTIMIZATION)**
   - GET batching improved P=16 from 80% to 100%
   - SET still sequential - no batching benefit
   - Cost: SET P=16 regressed from 87% to 72%

4. **SSO Hidden Costs (UNCLEAR)**
   - May have branch misprediction overhead
   - Needs micro-benchmark verification

---

## Optimization Strategy

### Phase 0: Fix Regressions (IMMEDIATE)

#### 0.1 Remove Double Parsing in Hot Path
**Problem**: `collect_get_keys()` runs before every command batch, hurting P=1.

**Fix**: Only attempt batching when buffer has multiple commands:
```rust
// BEFORE (always runs):
let (get_keys, get_count) = self.collect_get_keys();
if get_count >= 2 { ... }

// AFTER (only when buffer likely has pipeline):
if self.buffer.len() > 50 {  // Heuristic: pipelined data is larger
    let (get_keys, get_count) = self.collect_get_keys();
    if get_count >= 2 { ... }
}
```

**Better Fix**: Single-pass parsing that detects fast-path OR regular:
```rust
async fn process_commands(&mut self) -> CommandResult {
    // Single parse attempt - either fast path or regular
    match self.try_parse_command() {
        ParseResult::FastGet(key) => self.execute_fast_get(key).await,
        ParseResult::FastSet(key, val) => self.execute_fast_set(key, val).await,
        ParseResult::Regular(cmd) => self.execute_regular(cmd).await,
        ParseResult::NeedMoreData => CommandResult::NeedMoreData,
        ParseResult::Error(e) => CommandResult::ParseError(e),
    }
}
```

**Impact**: Restore P=1 to baseline (~95% of Redis)
**Effort**: 1 day

#### 0.2 Add SET Batching
**Problem**: GET batching improved P=16, SET lacks equivalent.

**Fix**: Add `FastBatchSet` message type and batch consecutive SETs:
```rust
pub enum ShardMessage {
    FastBatchSet {
        pairs: Vec<(bytes::Bytes, bytes::Bytes)>,
        response_tx: oneshot::Sender<Vec<RespValue>>,
    },
}
```

**Impact**: SET P=16 should improve to ~90%+ of Redis
**Effort**: 0.5 day

---

### Phase 1: Eliminate Channel Overhead (HIGH IMPACT)

#### 1.1 Channel Pool (Abseil Tip #7 - Reuse)
**Problem**: `oneshot::channel()` allocates on every command.

**Solution**: Pool of reusable channels:
```rust
struct ChannelPool {
    pool: crossbeam::ArrayQueue<oneshot::Sender<RespValue>>,
}

impl ChannelPool {
    fn acquire(&self) -> (oneshot::Sender<RespValue>, oneshot::Receiver<RespValue>) {
        if let Some(tx) = self.pool.pop() {
            // Reuse existing channel
            return (tx, rx);
        }
        oneshot::channel()  // Fallback to new allocation
    }

    fn release(&self, tx: oneshot::Sender<RespValue>) {
        let _ = self.pool.push(tx);  // Return to pool
    }
}
```

**Impact**: 10-20% improvement in channel-heavy scenarios
**Effort**: 1 day

#### 1.2 Fire-and-Forget for Writes (Abseil Tip #25 - Async)
**Problem**: SET commands wait for response even though client only needs "OK".

**Solution**: Don't wait for shard confirmation on writes:
```rust
// For SET without EX/PX/NX/XX, fire and forget
pub fn fast_set_fire_and_forget(&self, key: bytes::Bytes, value: bytes::Bytes) {
    let msg = ShardMessage::FastSetNoReply { key, value };
    let _ = self.tx.send(msg);
}

// Return +OK immediately
Self::encode_simple_string("OK", &mut self.write_buffer);
```

**Consideration**: Breaks strict ordering guarantees. May need config flag.
**Impact**: 30-50% improvement on write-heavy workloads
**Effort**: 0.5 day (if acceptable for use case)

---

### Phase 2: Reduce Allocations (MEDIUM IMPACT)

#### 2.1 Pre-allocated Response Buffers
**Problem**: Each response encodes to new bytes.

**Solution**: Per-shard response buffer pool, reuse across commands.

**Impact**: 5-10%
**Effort**: 1 day

#### 2.2 Verify SSO Benefits
**Problem**: SSO may have hidden costs (branch prediction, enum overhead).

**Solution**: Micro-benchmark SSO vs Vec<u8>:
```bash
cargo bench --bench sso_benchmark
```

If SSO is slower, consider:
- Raise threshold (23 bytes may be too low)
- Use compile-time feature flag
- Profile branch misprediction rate

**Impact**: TBD (need measurement)
**Effort**: 0.5 day

---

### Phase 3: Architectural Improvements (HIGH IMPACT, HIGH RISK)

#### 3.1 Batch Actor Messages
**Problem**: 16 shard actors = 16 potential context switches per pipeline.

**Solution**: Connection-local execution for common commands:
```rust
// If key routes to local data, skip actor entirely
if let Some(local_data) = self.local_cache.get(&key) {
    return local_data.clone();
}
// Only route to actor for misses or writes
```

**Risk**: Breaks actor isolation model
**Impact**: 2-3x improvement possible
**Effort**: 3+ days

#### 3.2 io_uring (Linux only)
**Problem**: Each TCP read/write is a syscall.

**Solution**: Use io_uring for batched I/O on Linux:
```rust
#[cfg(target_os = "linux")]
use tokio_uring::net::TcpStream;
```

**Impact**: 20-30% on high-connection workloads
**Effort**: 3+ days, Linux only

---

## Implementation Priority

| Phase | Task | Impact | Risk | Effort | Priority | Status |
|-------|------|--------|------|--------|----------|--------|
| 0.1 | Fix double parsing | +10-12% P=1 | Low | 1 day | **CRITICAL** | ✅ DONE |
| 0.2 | Add SET batching | +18% P=16 SET | Low | 0.5 day | **HIGH** | ✅ DONE |
| 1.1 | Channel pool | +10-20% | Low | 1 day | Medium | Next |
| 1.2 | Fire-and-forget SET | +30-50% writes | Medium | 0.5 day | Medium | |
| 2.1 | Response buffer pool | +5-10% | Low | 1 day | Low | |
| 2.2 | Verify SSO | TBD | Low | 0.5 day | **HIGH** | |
| 3.1 | Local execution | +200% | High | 3+ days | Future | |

**Recommended Order**: ~~0.1~~ → ~~0.2~~ → 1.1 → 2.2 → 1.2

---

## Success Criteria

**Target: Match Redis 8.0** (the new baseline)

After Phase 0 (✅ COMPLETE):
- [x] P=1 GET/SET: ≥90% of Redis 8.0 → **GET 116%, SET 92%** ✅
- [x] P=16 GET: ≥80% of Redis 8.0 → **83%** ✅
- [x] P=16 SET: ≥75% of Redis 8.0 → **77%** ✅ (was 59%)
- [ ] p99 latency ≤ Redis 8.0 p99

After Phase 1:
- [ ] P=1 GET/SET: ≥95% of Redis 8.0
- [ ] Pipelined throughput ≥1.6M req/sec (match Redis 8.0)
- [ ] Memory per key ≤ Redis

**Achieved:** GET P=1 beats Redis 8.0 by 16%!

---

## Measurement Protocol

1. **All benchmarks in Docker** (fair comparison)
2. **Multiple runs** (median of 3)
3. **Both P=1 and P=16** (single and pipelined)
4. **Record latency percentiles** (p50, p95, p99)
5. **Compare against Redis 8.0** (the new performance baseline)

```bash
# Run 3-way comparison (Redis 7.4, Redis 8.0, Rust)
cd docker-benchmark
./run-redis8-comparison.sh

# Run detailed benchmarks with latency
./run-detailed-benchmarks.sh

# Quick validation
docker run --rm --network host redis:8.0 redis-benchmark \
  -p 3000 -n 100000 -c 50 -t get,set -q -P 1
docker run --rm --network host redis:8.0 redis-benchmark \
  -p 3000 -n 100000 -c 50 -t get,set -q -P 16
```

---

## Anti-Patterns Identified

1. **Premature "optimization"** - Current fast path added overhead
2. **Not measuring first** - Assumed parsing was bottleneck (it wasn't)
3. **Ignoring single-command case** - Optimized P=16 at cost of P=1
4. **Double work** - collect_get_keys + try_fast_path parse twice

## Key Insight

The main bottleneck is **channel allocation**, not parsing. The fast-path parsing
saved ~5% but added ~25% overhead from double-parsing. Net result: regression.

Focus optimization on the actual bottleneck: per-command channel creation.
