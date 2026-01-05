use super::ShardedActorState;
use super::connection_pool::BufferPoolAsync;
use crate::redis::{Command, RespValue, RespCodec};
use crate::observability::{Metrics, spans};
use bytes::{BytesMut, BufMut};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{info, warn, error, debug, Instrument};

const MAX_BUFFER_SIZE: usize = 1024 * 1024;

pub struct OptimizedConnectionHandler {
    stream: TcpStream,
    state: ShardedActorState,
    buffer: BytesMut,
    write_buffer: BytesMut,
    client_addr: String,
    buffer_pool: Arc<BufferPoolAsync>,
    metrics: Arc<Metrics>,
}

impl OptimizedConnectionHandler {
    #[inline]
    pub fn new(
        stream: TcpStream,
        state: ShardedActorState,
        client_addr: String,
        buffer_pool: Arc<BufferPoolAsync>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let buffer = buffer_pool.acquire();
        let write_buffer = buffer_pool.acquire();
        debug_assert!(buffer.capacity() > 0, "Buffer pool returned zero-capacity buffer");
        OptimizedConnectionHandler {
            stream,
            state,
            buffer,
            write_buffer,
            client_addr,
            buffer_pool,
            metrics,
        }
    }

    pub async fn run(mut self) {
        // Create connection span for distributed tracing
        let connection_span = spans::connection_span(&self.client_addr);

        async {
            info!("Client connected: {}", self.client_addr);
            self.metrics.record_connection("established");

            // Enable TCP_NODELAY for lower latency (disable Nagle's algorithm)
            if let Err(e) = self.stream.set_nodelay(true) {
                warn!("Failed to set TCP_NODELAY: {}", e);
            }

            let mut read_buf = [0u8; 8192];

            loop {
                match self.stream.read(&mut read_buf).await {
                    Ok(0) => {
                        info!("Client disconnected: {}", self.client_addr);
                        break;
                    }
                    Ok(n) => {
                        if self.buffer.len() + n > MAX_BUFFER_SIZE {
                            error!("Buffer overflow from {}, closing connection", self.client_addr);
                            Self::encode_error_into("buffer overflow", &mut self.write_buffer);
                            let _ = self.stream.write_all(&self.write_buffer).await;
                            break;
                        }

                        self.buffer.extend_from_slice(&read_buf[..n]);

                        // Process ALL available commands (pipelining support)
                        let mut commands_executed = 0;
                        let mut had_parse_error = false;

                        loop {
                            match self.try_execute_command().await {
                                CommandResult::Executed => {
                                    commands_executed += 1;
                                    // Don't flush yet - continue processing pipeline
                                }
                                CommandResult::NeedMoreData => break,
                                CommandResult::ParseError(e) => {
                                    warn!("Parse error from {}: {}, draining buffer", self.client_addr, e);
                                    self.buffer.clear();
                                    Self::encode_error_into("protocol error", &mut self.write_buffer);
                                    had_parse_error = true;
                                    break;
                                }
                            }
                        }

                        // Flush ALL responses at once (critical for pipelining performance)
                        if !self.write_buffer.is_empty() {
                            if let Err(e) = self.stream.write_all(&self.write_buffer).await {
                                error!("Write failed to {}: {}", self.client_addr, e);
                                break;
                            }
                            // Ensure data is sent immediately
                            if let Err(e) = self.stream.flush().await {
                                error!("Flush failed to {}: {}", self.client_addr, e);
                                break;
                            }
                            self.write_buffer.clear();
                        }

                        if had_parse_error {
                            // Continue to next read after parse error
                        }

                        debug!("Processed {} commands in pipeline batch", commands_executed);
                    }
                    Err(e) => {
                        debug!("Read error from {}: {}", self.client_addr, e);
                        break;
                    }
                }
            }

            self.metrics.record_connection("closed");
            self.buffer_pool.release(self.buffer);
            self.buffer_pool.release(self.write_buffer);
        }
        .instrument(connection_span)
        .await
    }

    #[inline]
    async fn try_execute_command(&mut self) -> CommandResult {
        // Try fast path first for GET/SET commands (80%+ of traffic)
        match self.try_fast_path().await {
            FastPathResult::Handled => return CommandResult::Executed,
            FastPathResult::NeedMoreData => return CommandResult::NeedMoreData,
            FastPathResult::NotFastPath => {} // Fall through to regular parsing
        }

        match RespCodec::parse(&mut self.buffer) {
            Ok(Some(resp_value)) => {
                match Command::from_resp_zero_copy(&resp_value) {
                    Ok(cmd) => {
                        let cmd_name = cmd.name();
                        let start = Instant::now();

                        let response = self.state.execute(&cmd).await;

                        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
                        let success = !matches!(&response, RespValue::Error(_));
                        self.metrics.record_command(cmd_name, duration_ms, success);

                        Self::encode_resp_into(&response, &mut self.write_buffer);
                        CommandResult::Executed
                    }
                    Err(e) => {
                        self.metrics.record_command("PARSE_ERROR", 0.0, false);
                        Self::encode_error_into(&e, &mut self.write_buffer);
                        CommandResult::Executed
                    }
                }
            }
            Ok(None) => CommandResult::NeedMoreData,
            Err(e) => CommandResult::ParseError(e),
        }
    }

    /// Fast path for GET/SET commands - bypasses full RESP parsing
    ///
    /// RESP format for GET: *2\r\n$3\r\nGET\r\n$<keylen>\r\n<key>\r\n
    /// RESP format for SET: *3\r\n$3\r\nSET\r\n$<keylen>\r\n<key>\r\n$<vallen>\r\n<value>\r\n
    #[inline]
    async fn try_fast_path(&mut self) -> FastPathResult {
        let buf = &self.buffer[..];

        // Need at least "*2\r\n$3\r\nGET" (12 bytes) to detect GET
        if buf.len() < 12 {
            return FastPathResult::NotFastPath;
        }

        // Check for GET: *2\r\n$3\r\nGET\r\n
        if buf.starts_with(b"*2\r\n$3\r\nGET\r\n") || buf.starts_with(b"*2\r\n$3\r\nget\r\n") {
            return self.try_fast_get().await;
        }

        // Check for SET: *3\r\n$3\r\nSET\r\n
        if buf.starts_with(b"*3\r\n$3\r\nSET\r\n") || buf.starts_with(b"*3\r\n$3\r\nset\r\n") {
            return self.try_fast_set().await;
        }

        FastPathResult::NotFastPath
    }

    /// Parse and execute GET command via fast path
    #[inline]
    async fn try_fast_get(&mut self) -> FastPathResult {
        // Format: *2\r\n$3\r\nGET\r\n$<keylen>\r\n<key>\r\n
        // Header is 14 bytes: "*2\r\n$3\r\nGET\r\n"
        const HEADER_LEN: usize = 14;

        let buf = &self.buffer[..];
        if buf.len() < HEADER_LEN + 1 {
            return FastPathResult::NeedMoreData;
        }

        // Parse key length: $<len>\r\n
        let after_header = &buf[HEADER_LEN..];
        if after_header[0] != b'$' {
            return FastPathResult::NotFastPath; // Malformed, fall back
        }

        // Find \r\n after key length
        let Some(crlf_pos) = memchr::memchr(b'\r', &after_header[1..]) else {
            return FastPathResult::NeedMoreData;
        };
        let len_end = crlf_pos + 1; // Position of \r relative to after_header[1..]

        // Parse key length
        let len_str = &after_header[1..len_end];
        let Ok(key_len) = std::str::from_utf8(len_str).ok().and_then(|s| s.parse::<usize>().ok()).ok_or(()) else {
            return FastPathResult::NotFastPath; // Invalid length
        };

        // Check we have complete key + trailing \r\n
        let key_start = HEADER_LEN + 1 + len_end + 1; // After $<len>\r\n
        let total_needed = key_start + key_len + 2; // key + \r\n

        if buf.len() < total_needed {
            return FastPathResult::NeedMoreData;
        }

        // Extract key as bytes::Bytes (zero-copy from buffer)
        let key = bytes::Bytes::copy_from_slice(&buf[key_start..key_start + key_len]);

        // Consume the parsed bytes from buffer
        let _ = self.buffer.split_to(total_needed);

        // Execute fast GET
        let start = Instant::now();
        let response = self.state.fast_get(key).await;
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        let success = !matches!(&response, RespValue::Error(_));
        self.metrics.record_command("GET", duration_ms, success);

        Self::encode_resp_into(&response, &mut self.write_buffer);
        FastPathResult::Handled
    }

    /// Parse and execute SET command via fast path
    #[inline]
    async fn try_fast_set(&mut self) -> FastPathResult {
        // Format: *3\r\n$3\r\nSET\r\n$<keylen>\r\n<key>\r\n$<vallen>\r\n<value>\r\n
        // Header is 14 bytes: "*3\r\n$3\r\nSET\r\n"
        const HEADER_LEN: usize = 14;

        let buf = &self.buffer[..];
        if buf.len() < HEADER_LEN + 1 {
            return FastPathResult::NeedMoreData;
        }

        // Parse key length: $<len>\r\n
        let after_header = &buf[HEADER_LEN..];
        if after_header[0] != b'$' {
            return FastPathResult::NotFastPath;
        }

        let Some(key_len_crlf) = memchr::memchr(b'\r', &after_header[1..]) else {
            return FastPathResult::NeedMoreData;
        };

        let key_len_str = &after_header[1..key_len_crlf + 1];
        let Ok(key_len) = std::str::from_utf8(key_len_str).ok().and_then(|s| s.parse::<usize>().ok()).ok_or(()) else {
            return FastPathResult::NotFastPath;
        };

        // Calculate key position
        let key_start = HEADER_LEN + 1 + key_len_crlf + 2; // After $<keylen>\r\n
        let key_end = key_start + key_len;
        let val_len_start = key_end + 2; // After key\r\n

        if buf.len() < val_len_start + 1 {
            return FastPathResult::NeedMoreData;
        }

        // Parse value length: $<len>\r\n
        if buf[val_len_start] != b'$' {
            return FastPathResult::NotFastPath;
        }

        let after_key = &buf[val_len_start + 1..];
        let Some(val_len_crlf) = memchr::memchr(b'\r', after_key) else {
            return FastPathResult::NeedMoreData;
        };

        let val_len_str = &after_key[..val_len_crlf];
        let Ok(val_len) = std::str::from_utf8(val_len_str).ok().and_then(|s| s.parse::<usize>().ok()).ok_or(()) else {
            return FastPathResult::NotFastPath;
        };

        // Calculate value position and total length
        let val_start = val_len_start + 1 + val_len_crlf + 2; // After $<vallen>\r\n
        let total_needed = val_start + val_len + 2; // value + \r\n

        if buf.len() < total_needed {
            return FastPathResult::NeedMoreData;
        }

        // Extract key and value as bytes::Bytes
        let key = bytes::Bytes::copy_from_slice(&buf[key_start..key_end]);
        let value = bytes::Bytes::copy_from_slice(&buf[val_start..val_start + val_len]);

        // Consume the parsed bytes
        let _ = self.buffer.split_to(total_needed);

        // Execute fast SET
        let start = Instant::now();
        let response = self.state.fast_set(key, value).await;
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
        let success = !matches!(&response, RespValue::Error(_));
        self.metrics.record_command("SET", duration_ms, success);

        Self::encode_resp_into(&response, &mut self.write_buffer);
        FastPathResult::Handled
    }

    #[inline]
    fn encode_resp_into(value: &RespValue, buf: &mut BytesMut) {
        match value {
            RespValue::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Error(s) => {
                buf.put_u8(b'-');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(n) => {
                buf.put_u8(b':');
                buf.extend_from_slice(n.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(None) => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            RespValue::BulkString(Some(data)) => {
                buf.put_u8(b'$');
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Array(None) => {
                buf.extend_from_slice(b"*-1\r\n");
            }
            RespValue::Array(Some(elements)) => {
                buf.put_u8(b'*');
                buf.extend_from_slice(elements.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for elem in elements {
                    Self::encode_resp_into(elem, buf);
                }
            }
        }
    }

    #[inline]
    fn encode_error_into(msg: &str, buf: &mut BytesMut) {
        buf.put_u8(b'-');
        buf.extend_from_slice(b"ERR ");
        buf.extend_from_slice(msg.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
}

enum CommandResult {
    Executed,
    NeedMoreData,
    ParseError(String),
}

/// Result of attempting fast path execution
enum FastPathResult {
    /// Command handled via fast path
    Handled,
    /// Need more data to complete parsing
    NeedMoreData,
    /// Not a fast-path command, fall back to regular parsing
    NotFastPath,
}
