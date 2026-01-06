//! CPU-intensive benchmark for profiling
//! Run with: cargo build --release --bin profile-benchmark
//! Profile with: perf record -g ./target/release/profile-benchmark

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use redis_sim::redis::{Command, CommandExecutor, SDS};
use std::time::Instant;

fn main() {
    println!("Redis Command Execution Profiler");
    println!("=================================\n");

    let mut executor = CommandExecutor::new();
    let iterations = 1_000_000;

    // Warm up
    for i in 0..10000 {
        let key = format!("warmup:{}", i);
        let value = SDS::from_str("warmup_value");
        executor.execute(&Command::set(key, value));
    }

    println!("Running {} iterations of each command type...\n", iterations);

    // Profile SET operations
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("key:{}", i % 10000);
        let value = SDS::from_str("value_data_here_for_testing");
        executor.execute(&Command::set(key, value));
    }
    let set_duration = start.elapsed();
    println!("SET: {} ops in {:?} ({:.0} ops/sec)",
             iterations, set_duration,
             iterations as f64 / set_duration.as_secs_f64());

    // Profile GET operations
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("key:{}", i % 10000);
        executor.execute(&Command::Get(key));
    }
    let get_duration = start.elapsed();
    println!("GET: {} ops in {:?} ({:.0} ops/sec)",
             iterations, get_duration,
             iterations as f64 / get_duration.as_secs_f64());

    // Profile INCR operations
    for i in 0..1000 {
        let key = format!("counter:{}", i);
        let value = SDS::from_str("0");
        executor.execute(&Command::set(key, value));
    }
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("counter:{}", i % 1000);
        executor.execute(&Command::Incr(key));
    }
    let incr_duration = start.elapsed();
    println!("INCR: {} ops in {:?} ({:.0} ops/sec)",
             iterations, incr_duration,
             iterations as f64 / incr_duration.as_secs_f64());

    // Profile HSET operations
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("hash:{}", i % 1000);
        let field = SDS::from_str(&format!("field:{}", i % 100));
        let value = SDS::from_str("hash_value_data");
        executor.execute(&Command::HSet(key, vec![(field, value)]));
    }
    let hset_duration = start.elapsed();
    println!("HSET: {} ops in {:?} ({:.0} ops/sec)",
             iterations, hset_duration,
             iterations as f64 / hset_duration.as_secs_f64());

    // Profile HGET operations
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("hash:{}", i % 1000);
        let field = SDS::from_str(&format!("field:{}", i % 100));
        executor.execute(&Command::HGet(key, field));
    }
    let hget_duration = start.elapsed();
    println!("HGET: {} ops in {:?} ({:.0} ops/sec)",
             iterations, hget_duration,
             iterations as f64 / hget_duration.as_secs_f64());

    // Profile LPUSH operations
    for i in 0..100 {
        let key = format!("list:{}", i);
        executor.execute(&Command::LPush(key, vec![SDS::from_str("init")]));
    }
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("list:{}", i % 100);
        let value = SDS::from_str("list_item_value");
        executor.execute(&Command::LPush(key, vec![value]));
    }
    let lpush_duration = start.elapsed();
    println!("LPUSH: {} ops in {:?} ({:.0} ops/sec)",
             iterations, lpush_duration,
             iterations as f64 / lpush_duration.as_secs_f64());

    // Profile ZADD operations
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("zset:{}", i % 100);
        let member = SDS::from_str(&format!("member:{}", i % 1000));
        let score = (i % 10000) as f64;
        executor.execute(&Command::ZAdd(key, vec![(score, member)]));
    }
    let zadd_duration = start.elapsed();
    println!("ZADD: {} ops in {:?} ({:.0} ops/sec)",
             iterations, zadd_duration,
             iterations as f64 / zadd_duration.as_secs_f64());

    println!("\n=================================");
    println!("Profiling complete!");

    // Summary
    let total_ops = iterations * 7;
    let total_time = set_duration + get_duration + incr_duration +
                     hset_duration + hget_duration + lpush_duration + zadd_duration;
    println!("Total: {} ops in {:?} ({:.0} ops/sec average)",
             total_ops, total_time,
             total_ops as f64 / total_time.as_secs_f64());
}
