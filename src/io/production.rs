//! Production Runtime Implementation
//!
//! Wraps tokio for real-world I/O operations.
//! Zero overhead abstraction - all calls compile down to direct tokio calls.

use super::{Clock, Duration, Network, NetworkListener, NetworkStream, Rng, Runtime, Ticker, Timestamp};
use std::future::Future;
use std::io::Result as IoResult;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time;

/// Production runtime wrapping tokio
#[derive(Debug)]
pub struct ProductionRuntime {
    clock: ProductionClock,
    network: ProductionNetwork,
}

impl ProductionRuntime {
    pub fn new() -> Self {
        ProductionRuntime {
            clock: ProductionClock,
            network: ProductionNetwork,
        }
    }
}

impl Default for ProductionRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime for ProductionRuntime {
    type Clock = ProductionClock;
    type Network = ProductionNetwork;

    fn clock(&self) -> &Self::Clock {
        &self.clock
    }

    fn network(&self) -> &Self::Network {
        &self.network
    }

    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future);
    }
}

/// Production clock using system time
#[derive(Debug, Clone, Copy)]
pub struct ProductionClock;

impl Clock for ProductionClock {
    fn now(&self) -> Timestamp {
        // TigerStyle: Handle system clock edge cases gracefully
        // Clock may go backwards during NTP sync or VM migration
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Timestamp(duration.as_millis() as u64)
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            time::sleep(duration.as_std()).await;
        })
    }

    fn interval(&self, period: Duration) -> Box<dyn Ticker + Send> {
        Box::new(ProductionTicker {
            inner: time::interval(period.as_std()),
        })
    }
}

/// Production ticker wrapping tokio::time::Interval
pub struct ProductionTicker {
    inner: time::Interval,
}

impl Ticker for ProductionTicker {
    fn tick(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.inner.tick().await;
        })
    }
}

/// Production network using tokio TCP
#[derive(Debug, Clone, Copy)]
pub struct ProductionNetwork;

impl Network for ProductionNetwork {
    type Listener = ProductionListener;
    type Stream = ProductionStream;

    fn bind<'a>(
        &'a self,
        addr: &'a str,
    ) -> Pin<Box<dyn Future<Output = IoResult<Self::Listener>> + Send + 'a>> {
        Box::pin(async move {
            let listener = TcpListener::bind(addr).await?;
            Ok(ProductionListener { inner: listener })
        })
    }

    fn connect<'a>(
        &'a self,
        addr: &'a str,
    ) -> Pin<Box<dyn Future<Output = IoResult<Self::Stream>> + Send + 'a>> {
        Box::pin(async move {
            let stream = TcpStream::connect(addr).await?;
            Ok(ProductionStream { inner: stream })
        })
    }
}

/// Production listener wrapping TcpListener
#[derive(Debug)]
pub struct ProductionListener {
    inner: TcpListener,
}

impl NetworkListener for ProductionListener {
    type Stream = ProductionStream;

    fn accept(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = IoResult<(Self::Stream, String)>> + Send + '_>> {
        Box::pin(async move {
            let (stream, addr) = self.inner.accept().await?;
            Ok((ProductionStream { inner: stream }, addr.to_string()))
        })
    }

    fn local_addr(&self) -> IoResult<String> {
        self.inner.local_addr().map(|a| a.to_string())
    }
}

/// Production stream wrapping TcpStream
#[derive(Debug)]
pub struct ProductionStream {
    inner: TcpStream,
}

impl NetworkStream for ProductionStream {
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = IoResult<usize>> + Send + 'a>> {
        Box::pin(async move { self.inner.read(buf).await })
    }

    fn read_exact<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = IoResult<()>> + Send + 'a>> {
        Box::pin(async move {
            self.inner.read_exact(buf).await?;
            Ok(())
        })
    }

    fn write_all<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = IoResult<()>> + Send + 'a>> {
        Box::pin(async move { self.inner.write_all(buf).await })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = IoResult<()>> + Send + '_>> {
        Box::pin(async move { self.inner.flush().await })
    }

    fn peer_addr(&self) -> IoResult<String> {
        self.inner.peer_addr().map(|a| a.to_string())
    }
}

/// Production RNG using StdRng (Send-safe)
pub struct ProductionRng {
    inner: Mutex<rand::rngs::StdRng>,
}

impl ProductionRng {
    pub fn new() -> Self {
        use rand::SeedableRng;
        ProductionRng {
            inner: Mutex::new(rand::rngs::StdRng::from_entropy()),
        }
    }
}

impl Default for ProductionRng {
    fn default() -> Self {
        Self::new()
    }
}

impl ProductionRng {
    /// TigerStyle: Acquire lock with graceful poison recovery
    ///
    /// If a previous holder panicked, we still return the RNG since
    /// the internal state is still valid for generating random numbers.
    #[inline]
    fn lock_inner(&self) -> std::sync::MutexGuard<'_, rand::rngs::StdRng> {
        self.inner.lock().unwrap_or_else(|poisoned| {
            // Log the poison but recover - RNG state is still usable
            eprintln!("ProductionRng: recovering from poisoned lock");
            poisoned.into_inner()
        })
    }
}

impl Rng for ProductionRng {
    fn next_u64(&mut self) -> u64 {
        use rand::RngCore;
        self.lock_inner().next_u64()
    }

    fn gen_bool(&mut self, probability: f64) -> bool {
        use rand::Rng;
        self.lock_inner().gen_bool(probability.clamp(0.0, 1.0))
    }

    fn gen_range(&mut self, min: u64, max: u64) -> u64 {
        use rand::Rng;
        if min >= max {
            return min;
        }
        self.lock_inner().gen_range(min..max)
    }

    fn shuffle<T>(&mut self, slice: &mut [T]) {
        use rand::seq::SliceRandom;
        slice.shuffle(&mut *self.lock_inner());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_production_clock_now() {
        let clock = ProductionClock;
        let t1 = clock.now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t2 = clock.now();
        assert!(t2 > t1);
    }

    #[test]
    fn test_production_rng() {
        let mut rng = ProductionRng::new();

        // Test gen_range
        for _ in 0..100 {
            let val = rng.gen_range(10, 20);
            assert!(val >= 10 && val < 20);
        }

        // Test gen_bool
        let mut true_count = 0;
        for _ in 0..1000 {
            if rng.gen_bool(0.5) {
                true_count += 1;
            }
        }
        // Should be roughly 50%, allow wide margin
        assert!(true_count > 300 && true_count < 700);
    }

    #[tokio::test]
    async fn test_production_clock_sleep() {
        let clock = ProductionClock;
        let start = std::time::Instant::now();
        clock.sleep(Duration::from_millis(50)).await;
        let elapsed = start.elapsed();
        assert!(elapsed.as_millis() >= 45); // Allow some tolerance
    }
}
