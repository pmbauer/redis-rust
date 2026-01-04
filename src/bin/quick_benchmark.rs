//! Quick Benchmark - Performance testing utility
//!
//! TigerStyle: Graceful error handling instead of panics

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Instant;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔥 Redis Server Performance Benchmark\n");
    println!("Connecting to 127.0.0.1:3000...\n");

    let num_requests = 10_000;
    let num_clients = 50;

    println!("Configuration:");
    println!("  Total requests per test: {}", num_requests);
    println!("  Concurrent clients: {}\n", num_clients);

    println!("====================================\n");

    benchmark_ping(num_requests, num_clients).await?;
    benchmark_set(num_requests, num_clients).await?;
    benchmark_get_existing(num_requests, num_clients).await?;
    benchmark_incr(num_requests, num_clients).await?;
    benchmark_mixed(num_requests, num_clients).await?;

    println!("====================================\n");
    println!("✅ Benchmark complete!\n");

    Ok(())
}

/// Run a single client's benchmark operations
/// Returns (completed_count, error_count)
async fn run_client_benchmark<F>(
    requests_per_client: usize,
    completed: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    make_cmd: F,
) where
    F: Fn(usize) -> Vec<u8>,
{
    let stream = match TcpStream::connect("127.0.0.1:3000").await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Connection failed: {}", e);
            errors.fetch_add(requests_per_client as u64, Ordering::Relaxed);
            return;
        }
    };

    let mut stream = stream;
    let mut buf = vec![0u8; 256];

    for i in 0..requests_per_client {
        let cmd = make_cmd(i);

        if let Err(e) = stream.write_all(&cmd).await {
            // Connection broken - count remaining as errors and exit
            let remaining = (requests_per_client - i) as u64;
            errors.fetch_add(remaining, Ordering::Relaxed);
            if errors.load(Ordering::Relaxed) == remaining {
                // Only log once per client
                eprintln!("Write error: {}", e);
            }
            return;
        }

        if let Err(e) = stream.read(&mut buf).await {
            let remaining = (requests_per_client - i) as u64;
            errors.fetch_add(remaining, Ordering::Relaxed);
            if errors.load(Ordering::Relaxed) == remaining {
                eprintln!("Read error: {}", e);
            }
            return;
        }

        completed.fetch_add(1, Ordering::Relaxed);
    }
}

async fn benchmark_ping(num_requests: usize, num_clients: usize) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let completed = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];
    let requests_per_client = num_requests / num_clients;

    for _ in 0..num_clients {
        let completed = completed.clone();
        let errors = errors.clone();
        let handle = tokio::spawn(async move {
            run_client_benchmark(
                requests_per_client,
                completed,
                errors,
                |_| b"*1\r\n$4\r\nPING\r\n".to_vec(),
            ).await;
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    let total = completed.load(Ordering::Relaxed);
    let error_count = errors.load(Ordering::Relaxed);
    print_results("PING", total, error_count, elapsed);

    Ok(())
}

async fn benchmark_set(num_requests: usize, num_clients: usize) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let completed = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];
    let requests_per_client = num_requests / num_clients;

    for client_id in 0..num_clients {
        let completed = completed.clone();
        let errors = errors.clone();
        let handle = tokio::spawn(async move {
            run_client_benchmark(
                requests_per_client,
                completed,
                errors,
                move |i| format!(
                    "*3\r\n$3\r\nSET\r\n$6\r\nbm:{}:{}\r\n$5\r\nvalue\r\n",
                    client_id, i
                ).into_bytes(),
            ).await;
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    let total = completed.load(Ordering::Relaxed);
    let error_count = errors.load(Ordering::Relaxed);
    print_results("SET", total, error_count, elapsed);

    Ok(())
}

async fn benchmark_get_existing(num_requests: usize, num_clients: usize) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let completed = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];
    let requests_per_client = num_requests / num_clients;

    for client_id in 0..num_clients {
        let completed = completed.clone();
        let errors = errors.clone();
        let handle = tokio::spawn(async move {
            run_client_benchmark(
                requests_per_client,
                completed,
                errors,
                move |i| {
                    let key_id = i % 200;
                    format!(
                        "*2\r\n$3\r\nGET\r\n$6\r\nbm:{}:{}\r\n",
                        client_id, key_id
                    ).into_bytes()
                },
            ).await;
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    let total = completed.load(Ordering::Relaxed);
    let error_count = errors.load(Ordering::Relaxed);
    print_results("GET", total, error_count, elapsed);

    Ok(())
}

async fn benchmark_incr(num_requests: usize, num_clients: usize) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let completed = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];
    let requests_per_client = num_requests / num_clients;

    for client_id in 0..num_clients {
        let completed = completed.clone();
        let errors = errors.clone();
        let handle = tokio::spawn(async move {
            let cmd = format!("*2\r\n$4\r\nINCR\r\n$6\r\nctr:{}\r\n", client_id).into_bytes();
            run_client_benchmark(
                requests_per_client,
                completed,
                errors,
                move |_| cmd.clone(),
            ).await;
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    let total = completed.load(Ordering::Relaxed);
    let error_count = errors.load(Ordering::Relaxed);
    print_results("INCR", total, error_count, elapsed);

    Ok(())
}

async fn benchmark_mixed(num_requests: usize, num_clients: usize) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let completed = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];
    let requests_per_client = num_requests / num_clients;

    for client_id in 0..num_clients {
        let completed = completed.clone();
        let errors = errors.clone();
        let handle = tokio::spawn(async move {
            run_client_benchmark(
                requests_per_client,
                completed,
                errors,
                move |i| {
                    match i % 4 {
                        0 => format!(
                            "*3\r\n$3\r\nSET\r\n$6\r\nmx:{}:{}\r\n$5\r\nvalue\r\n",
                            client_id, i
                        ),
                        1 => format!(
                            "*2\r\n$3\r\nGET\r\n$6\r\nmx:{}:{}\r\n",
                            client_id, i.saturating_sub(1)
                        ),
                        2 => format!("*2\r\n$4\r\nINCR\r\n$7\r\nmxc:{}\r\n", client_id),
                        _ => "*1\r\n$4\r\nPING\r\n".to_string(),
                    }.into_bytes()
                },
            ).await;
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    let total = completed.load(Ordering::Relaxed);
    let error_count = errors.load(Ordering::Relaxed);
    print_results("MIXED (SET/GET/INCR/PING)", total, error_count, elapsed);

    Ok(())
}

fn print_results(test_name: &str, total: u64, errors: u64, elapsed: std::time::Duration) {
    let ops_per_sec = if elapsed.as_secs_f64() > 0.0 {
        total as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    let latency_ms = if total > 0 {
        elapsed.as_secs_f64() * 1000.0 / total as f64
    } else {
        0.0
    };

    println!("{}", test_name);
    println!("  {} requests in {:.2}s", total, elapsed.as_secs_f64());
    println!("  {:.0} req/sec", ops_per_sec);
    println!("  {:.3} ms avg latency", latency_ms);
    if errors > 0 {
        println!("  ⚠️  {} errors", errors);
    }
    println!();
}
