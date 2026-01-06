# RedisEvolve - Automatic Performance Tuning

RedisEvolve is an evolutionary optimization harness that automatically discovers optimal configuration parameters by running benchmarks and evolving towards Redis 8.0 performance.

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                    Python Evolution Engine                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ Generate │→ │ Evaluate │→ │  Select  │→ │  Evolve  │ → loop │
│  │ Population│  │ Fitness  │  │   Best   │  │ (mutate) │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

1. **Generate** population of config candidates (shards, buffer sizes, batch thresholds)
2. **Evaluate** each candidate: write TOML → rebuild Docker → run redis-benchmark → compute fitness
3. **Select** best performers, **evolve** via crossover + mutation
4. **Repeat** for N generations until convergence

## Running Evolution

```bash
# Quick test with mock evaluator (no Docker)
python -m evolve.harness --mock --generations 3 --population 6

# Full evolution with real benchmarks (~8-10 hours)
python -m evolve.harness --generations 10 --population 8
```

## Results

After 10 generations (62 candidates evaluated), RedisEvolve discovered:

| Parameter | Default | Optimized | Impact |
|-----------|---------|-----------|--------|
| num_shards | 16 | **4** | Less contention |
| response_pool.capacity | 256 | **512** | More pooled responses |
| response_pool.prewarm | 64 | **96** | Better warm start |
| buffers.read_size | 8192 | 8192 | Already optimal |
| batching.batch_threshold | 2 | **6** | More aggressive batching |

**Result: 99.1% of Redis 8.0 performance** (up from ~88% with defaults)

Key insight: Fewer shards (4 vs 16) reduces contention and improves throughput.

## Configuration

The optimized config is saved to `evolve/results/best_config.toml`. To use it:

```bash
# Copy to production config
cp evolve/results/best_config.toml perf_config.toml

# Server will automatically load it on startup
cargo run --bin redis-server-optimized --release
```
