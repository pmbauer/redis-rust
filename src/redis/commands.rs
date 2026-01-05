use super::data::*;
use super::resp::RespValue;
use super::resp_optimized::RespValueZeroCopy;
use ahash::AHashMap;
use crate::simulator::VirtualTime;

#[derive(Debug, Clone)]
pub enum Command {
    // String commands
    Get(String),
    Set(String, SDS),
    SetEx(String, i64, SDS),
    SetNx(String, SDS),
    Append(String, SDS),
    GetSet(String, SDS),
    StrLen(String),
    MGet(Vec<String>),
    MSet(Vec<(String, SDS)>),
    /// Internal command for batched SET within a single shard (not exposed via RESP)
    BatchSet(Vec<(String, SDS)>),
    /// Internal command for batched GET within a single shard (not exposed via RESP)
    BatchGet(Vec<String>),
    // Counter commands
    Incr(String),
    Decr(String),
    IncrBy(String, i64),
    DecrBy(String, i64),
    // Key commands
    Del(String),
    Exists(Vec<String>),
    TypeOf(String),
    Keys(String),
    FlushDb,
    FlushAll,
    // Expiration commands
    Expire(String, i64),
    ExpireAt(String, i64),
    PExpireAt(String, i64),
    Ttl(String),
    Pttl(String),
    Persist(String),
    // List commands
    LPush(String, Vec<SDS>),
    RPush(String, Vec<SDS>),
    LPop(String),
    RPop(String),
    LLen(String),
    LIndex(String, isize),
    LRange(String, isize, isize),
    // Set commands
    SAdd(String, Vec<SDS>),
    SRem(String, Vec<SDS>),
    SMembers(String),
    SIsMember(String, SDS),
    SCard(String),
    // Hash commands
    HSet(String, Vec<(SDS, SDS)>),
    HGet(String, SDS),
    HDel(String, Vec<SDS>),
    HGetAll(String),
    HKeys(String),
    HVals(String),
    HLen(String),
    HExists(String, SDS),
    HIncrBy(String, SDS, i64),
    // Sorted set commands
    ZAdd(String, Vec<(f64, SDS)>),
    ZRem(String, Vec<SDS>),
    ZRange(String, isize, isize),
    ZRevRange(String, isize, isize, bool),  // bool = WITHSCORES
    ZScore(String, SDS),
    ZRank(String, SDS),
    ZCard(String),
    // Server commands
    Info,
    Ping,
    Unknown(String),
}

impl Command {
    pub fn from_resp(value: &RespValue) -> Result<Command, String> {
        match value {
            RespValue::Array(Some(elements)) if !elements.is_empty() => {
                let cmd_name = match &elements[0] {
                    RespValue::BulkString(Some(data)) => {
                        String::from_utf8_lossy(data).to_uppercase()
                    }
                    _ => return Err("Invalid command format".to_string()),
                };

                match cmd_name.as_str() {
                    "PING" => Ok(Command::Ping),
                    "INFO" => Ok(Command::Info),
                    "FLUSHDB" => Ok(Command::FlushDb),
                    "FLUSHALL" => Ok(Command::FlushAll),
                    "GET" => {
                        if elements.len() != 2 {
                            return Err("GET requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::Get(key))
                    }
                    "SET" => {
                        if elements.len() != 3 {
                            return Err("SET requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let value = Self::extract_sds(&elements[2])?;
                        Ok(Command::Set(key, value))
                    }
                    "SETEX" => {
                        if elements.len() != 4 {
                            return Err("SETEX requires 3 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let seconds = Self::extract_integer(&elements[2])? as i64;
                        let value = Self::extract_sds(&elements[3])?;
                        Ok(Command::SetEx(key, seconds, value))
                    }
                    "SETNX" => {
                        if elements.len() != 3 {
                            return Err("SETNX requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let value = Self::extract_sds(&elements[2])?;
                        Ok(Command::SetNx(key, value))
                    }
                    "DEL" => {
                        if elements.len() != 2 {
                            return Err("DEL requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::Del(key))
                    }
                    "EXISTS" => {
                        if elements.len() < 2 {
                            return Err("EXISTS requires at least 1 argument".to_string());
                        }
                        let keys = elements[1..].iter().map(Self::extract_string).collect::<Result<Vec<_>, _>>()?;
                        Ok(Command::Exists(keys))
                    }
                    "TYPE" => {
                        if elements.len() != 2 {
                            return Err("TYPE requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::TypeOf(key))
                    }
                    "KEYS" => {
                        if elements.len() != 2 {
                            return Err("KEYS requires 1 argument".to_string());
                        }
                        let pattern = Self::extract_string(&elements[1])?;
                        Ok(Command::Keys(pattern))
                    }
                    "EXPIRE" => {
                        if elements.len() != 3 {
                            return Err("EXPIRE requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let seconds = Self::extract_integer(&elements[2])? as i64;
                        Ok(Command::Expire(key, seconds))
                    }
                    "EXPIREAT" => {
                        if elements.len() != 3 {
                            return Err("EXPIREAT requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let timestamp = Self::extract_integer(&elements[2])? as i64;
                        Ok(Command::ExpireAt(key, timestamp))
                    }
                    "PEXPIREAT" => {
                        if elements.len() != 3 {
                            return Err("PEXPIREAT requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let timestamp_millis = Self::extract_integer(&elements[2])? as i64;
                        Ok(Command::PExpireAt(key, timestamp_millis))
                    }
                    "TTL" => {
                        if elements.len() != 2 {
                            return Err("TTL requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::Ttl(key))
                    }
                    "PTTL" => {
                        if elements.len() != 2 {
                            return Err("PTTL requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::Pttl(key))
                    }
                    "PERSIST" => {
                        if elements.len() != 2 {
                            return Err("PERSIST requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::Persist(key))
                    }
                    "INCR" => {
                        if elements.len() != 2 {
                            return Err("INCR requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::Incr(key))
                    }
                    "DECR" => {
                        if elements.len() != 2 {
                            return Err("DECR requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::Decr(key))
                    }
                    "INCRBY" => {
                        if elements.len() != 3 {
                            return Err("INCRBY requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let increment = Self::extract_integer(&elements[2])? as i64;
                        Ok(Command::IncrBy(key, increment))
                    }
                    "DECRBY" => {
                        if elements.len() != 3 {
                            return Err("DECRBY requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let decrement = Self::extract_integer(&elements[2])? as i64;
                        Ok(Command::DecrBy(key, decrement))
                    }
                    "APPEND" => {
                        if elements.len() != 3 {
                            return Err("APPEND requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let value = Self::extract_sds(&elements[2])?;
                        Ok(Command::Append(key, value))
                    }
                    "GETSET" => {
                        if elements.len() != 3 {
                            return Err("GETSET requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let value = Self::extract_sds(&elements[2])?;
                        Ok(Command::GetSet(key, value))
                    }
                    "STRLEN" => {
                        if elements.len() != 2 {
                            return Err("STRLEN requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::StrLen(key))
                    }
                    "MGET" => {
                        if elements.len() < 2 {
                            return Err("MGET requires at least 1 argument".to_string());
                        }
                        let keys = elements[1..].iter().map(Self::extract_string).collect::<Result<Vec<_>, _>>()?;
                        Ok(Command::MGet(keys))
                    }
                    "MSET" => {
                        if elements.len() < 3 || (elements.len() - 1) % 2 != 0 {
                            return Err("MSET requires key-value pairs".to_string());
                        }
                        // Pre-allocate capacity (Abseil Tip #19)
                        let mut pairs = Vec::with_capacity((elements.len() - 1) / 2);
                        for i in (1..elements.len()).step_by(2) {
                            let key = Self::extract_string(&elements[i])?;
                            let value = Self::extract_sds(&elements[i + 1])?;
                            pairs.push((key, value));
                        }
                        Ok(Command::MSet(pairs))
                    }
                    "LPUSH" => {
                        if elements.len() < 3 {
                            return Err("LPUSH requires at least 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let values = elements[2..].iter().map(Self::extract_sds).collect::<Result<Vec<_>, _>>()?;
                        Ok(Command::LPush(key, values))
                    }
                    "RPUSH" => {
                        if elements.len() < 3 {
                            return Err("RPUSH requires at least 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let values = elements[2..].iter().map(Self::extract_sds).collect::<Result<Vec<_>, _>>()?;
                        Ok(Command::RPush(key, values))
                    }
                    "LPOP" => {
                        if elements.len() != 2 {
                            return Err("LPOP requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::LPop(key))
                    }
                    "RPOP" => {
                        if elements.len() != 2 {
                            return Err("RPOP requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::RPop(key))
                    }
                    "LRANGE" => {
                        if elements.len() != 4 {
                            return Err("LRANGE requires 3 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let start = Self::extract_integer(&elements[2])?;
                        let stop = Self::extract_integer(&elements[3])?;
                        Ok(Command::LRange(key, start, stop))
                    }
                    "LLEN" => {
                        if elements.len() != 2 {
                            return Err("LLEN requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::LLen(key))
                    }
                    "LINDEX" => {
                        if elements.len() != 3 {
                            return Err("LINDEX requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let index = Self::extract_integer(&elements[2])?;
                        Ok(Command::LIndex(key, index))
                    }
                    "SADD" => {
                        if elements.len() < 3 {
                            return Err("SADD requires at least 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let members = elements[2..].iter().map(Self::extract_sds).collect::<Result<Vec<_>, _>>()?;
                        Ok(Command::SAdd(key, members))
                    }
                    "SMEMBERS" => {
                        if elements.len() != 2 {
                            return Err("SMEMBERS requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::SMembers(key))
                    }
                    "SISMEMBER" => {
                        if elements.len() != 3 {
                            return Err("SISMEMBER requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let member = Self::extract_sds(&elements[2])?;
                        Ok(Command::SIsMember(key, member))
                    }
                    "SREM" => {
                        if elements.len() < 3 {
                            return Err("SREM requires at least 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let members = elements[2..].iter().map(Self::extract_sds).collect::<Result<Vec<_>, _>>()?;
                        Ok(Command::SRem(key, members))
                    }
                    "SCARD" => {
                        if elements.len() != 2 {
                            return Err("SCARD requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::SCard(key))
                    }
                    "HSET" => {
                        // HSET key field value [field value ...]
                        if elements.len() < 4 || (elements.len() - 2) % 2 != 0 {
                            return Err("HSET requires key and field-value pairs".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let mut pairs = Vec::with_capacity((elements.len() - 2) / 2);
                        for i in (2..elements.len()).step_by(2) {
                            let field = Self::extract_sds(&elements[i])?;
                            let value = Self::extract_sds(&elements[i + 1])?;
                            pairs.push((field, value));
                        }
                        Ok(Command::HSet(key, pairs))
                    }
                    "HGET" => {
                        if elements.len() != 3 {
                            return Err("HGET requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let field = Self::extract_sds(&elements[2])?;
                        Ok(Command::HGet(key, field))
                    }
                    "HGETALL" => {
                        if elements.len() != 2 {
                            return Err("HGETALL requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::HGetAll(key))
                    }
                    "HINCRBY" => {
                        if elements.len() != 4 {
                            return Err("HINCRBY requires 3 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let field = Self::extract_sds(&elements[2])?;
                        let increment = Self::extract_i64(&elements[3])?;
                        Ok(Command::HIncrBy(key, field, increment))
                    }
                    "HDEL" => {
                        if elements.len() < 3 {
                            return Err("HDEL requires at least 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let fields = elements[2..].iter().map(Self::extract_sds).collect::<Result<Vec<_>, _>>()?;
                        Ok(Command::HDel(key, fields))
                    }
                    "HKEYS" => {
                        if elements.len() != 2 {
                            return Err("HKEYS requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::HKeys(key))
                    }
                    "HVALS" => {
                        if elements.len() != 2 {
                            return Err("HVALS requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::HVals(key))
                    }
                    "HLEN" => {
                        if elements.len() != 2 {
                            return Err("HLEN requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::HLen(key))
                    }
                    "HEXISTS" => {
                        if elements.len() != 3 {
                            return Err("HEXISTS requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let field = Self::extract_sds(&elements[2])?;
                        Ok(Command::HExists(key, field))
                    }
                    "ZADD" => {
                        if elements.len() < 4 || (elements.len() - 2) % 2 != 0 {
                            return Err("ZADD requires key and score-member pairs".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        // Pre-allocate capacity (Abseil Tip #19)
                        let mut pairs = Vec::with_capacity((elements.len() - 2) / 2);
                        for i in (2..elements.len()).step_by(2) {
                            let score = Self::extract_float(&elements[i])?;
                            let member = Self::extract_sds(&elements[i + 1])?;
                            pairs.push((score, member));
                        }
                        Ok(Command::ZAdd(key, pairs))
                    }
                    "ZRANGE" => {
                        if elements.len() != 4 {
                            return Err("ZRANGE requires 3 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let start = Self::extract_integer(&elements[2])?;
                        let stop = Self::extract_integer(&elements[3])?;
                        Ok(Command::ZRange(key, start, stop))
                    }
                    "ZREVRANGE" => {
                        if elements.len() < 4 || elements.len() > 5 {
                            return Err("ZREVRANGE requires 3 or 4 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let start = Self::extract_integer(&elements[2])?;
                        let stop = Self::extract_integer(&elements[3])?;
                        let with_scores = if elements.len() == 5 {
                            let opt = Self::extract_string(&elements[4])?.to_uppercase();
                            opt == "WITHSCORES"
                        } else {
                            false
                        };
                        Ok(Command::ZRevRange(key, start, stop, with_scores))
                    }
                    "ZSCORE" => {
                        if elements.len() != 3 {
                            return Err("ZSCORE requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let member = Self::extract_sds(&elements[2])?;
                        Ok(Command::ZScore(key, member))
                    }
                    "ZREM" => {
                        if elements.len() < 3 {
                            return Err("ZREM requires at least 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let members = elements[2..].iter().map(Self::extract_sds).collect::<Result<Vec<_>, _>>()?;
                        Ok(Command::ZRem(key, members))
                    }
                    "ZRANK" => {
                        if elements.len() != 3 {
                            return Err("ZRANK requires 2 arguments".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        let member = Self::extract_sds(&elements[2])?;
                        Ok(Command::ZRank(key, member))
                    }
                    "ZCARD" => {
                        if elements.len() != 2 {
                            return Err("ZCARD requires 1 argument".to_string());
                        }
                        let key = Self::extract_string(&elements[1])?;
                        Ok(Command::ZCard(key))
                    }
                    _ => Ok(Command::Unknown(cmd_name)),
                }
            }
            _ => Err("Invalid command format".to_string()),
        }
    }

    fn extract_string(value: &RespValue) -> Result<String, String> {
        match value {
            RespValue::BulkString(Some(data)) => {
                Ok(String::from_utf8_lossy(data).to_string())
            }
            _ => Err("Expected bulk string".to_string()),
        }
    }

    fn extract_sds(value: &RespValue) -> Result<SDS, String> {
        match value {
            RespValue::BulkString(Some(data)) => Ok(SDS::new(data.clone())),
            _ => Err("Expected bulk string".to_string()),
        }
    }

    fn extract_integer(value: &RespValue) -> Result<isize, String> {
        match value {
            RespValue::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                s.parse::<isize>().map_err(|e| e.to_string())
            }
            RespValue::Integer(n) => Ok(*n as isize),
            _ => Err("Expected integer".to_string()),
        }
    }

    fn extract_float(value: &RespValue) -> Result<f64, String> {
        match value {
            RespValue::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                s.parse::<f64>().map_err(|e| e.to_string())
            }
            _ => Err("Expected float".to_string()),
        }
    }

    fn extract_i64(value: &RespValue) -> Result<i64, String> {
        match value {
            RespValue::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                s.parse::<i64>().map_err(|e| e.to_string())
            }
            RespValue::Integer(n) => Ok(*n),
            _ => Err("Expected integer".to_string()),
        }
    }

    pub fn from_resp_zero_copy(value: &RespValueZeroCopy) -> Result<Command, String> {
        match value {
            RespValueZeroCopy::Array(Some(elements)) if !elements.is_empty() => {
                let cmd_name = match &elements[0] {
                    RespValueZeroCopy::BulkString(Some(data)) => {
                        String::from_utf8_lossy(data).to_uppercase()
                    }
                    _ => return Err("Invalid command format".to_string()),
                };

                match cmd_name.as_str() {
                    "PING" => Ok(Command::Ping),
                    "INFO" => Ok(Command::Info),
                    "FLUSHDB" => Ok(Command::FlushDb),
                    "FLUSHALL" => Ok(Command::FlushAll),
                    "GET" => {
                        if elements.len() != 2 { return Err("GET requires 1 argument".to_string()); }
                        Ok(Command::Get(Self::extract_string_zc(&elements[1])?))
                    }
                    "SET" => {
                        if elements.len() != 3 { return Err("SET requires 2 arguments".to_string()); }
                        Ok(Command::Set(Self::extract_string_zc(&elements[1])?, Self::extract_sds_zc(&elements[2])?))
                    }
                    "SETEX" => {
                        if elements.len() != 4 { return Err("SETEX requires 3 arguments".to_string()); }
                        Ok(Command::SetEx(Self::extract_string_zc(&elements[1])?, Self::extract_integer_zc(&elements[2])? as i64, Self::extract_sds_zc(&elements[3])?))
                    }
                    "SETNX" => {
                        if elements.len() != 3 { return Err("SETNX requires 2 arguments".to_string()); }
                        Ok(Command::SetNx(Self::extract_string_zc(&elements[1])?, Self::extract_sds_zc(&elements[2])?))
                    }
                    "DEL" => {
                        if elements.len() != 2 { return Err("DEL requires 1 argument".to_string()); }
                        Ok(Command::Del(Self::extract_string_zc(&elements[1])?))
                    }
                    "EXISTS" => {
                        if elements.len() < 2 { return Err("EXISTS requires at least 1 argument".to_string()); }
                        Ok(Command::Exists(elements[1..].iter().map(Self::extract_string_zc).collect::<Result<Vec<_>, _>>()?))
                    }
                    "TYPE" => {
                        if elements.len() != 2 { return Err("TYPE requires 1 argument".to_string()); }
                        Ok(Command::TypeOf(Self::extract_string_zc(&elements[1])?))
                    }
                    "KEYS" => {
                        if elements.len() != 2 { return Err("KEYS requires 1 argument".to_string()); }
                        Ok(Command::Keys(Self::extract_string_zc(&elements[1])?))
                    }
                    "EXPIRE" => {
                        if elements.len() != 3 { return Err("EXPIRE requires 2 arguments".to_string()); }
                        Ok(Command::Expire(Self::extract_string_zc(&elements[1])?, Self::extract_integer_zc(&elements[2])? as i64))
                    }
                    "EXPIREAT" => {
                        if elements.len() != 3 { return Err("EXPIREAT requires 2 arguments".to_string()); }
                        Ok(Command::ExpireAt(Self::extract_string_zc(&elements[1])?, Self::extract_integer_zc(&elements[2])? as i64))
                    }
                    "PEXPIREAT" => {
                        if elements.len() != 3 { return Err("PEXPIREAT requires 2 arguments".to_string()); }
                        Ok(Command::PExpireAt(Self::extract_string_zc(&elements[1])?, Self::extract_integer_zc(&elements[2])? as i64))
                    }
                    "TTL" => {
                        if elements.len() != 2 { return Err("TTL requires 1 argument".to_string()); }
                        Ok(Command::Ttl(Self::extract_string_zc(&elements[1])?))
                    }
                    "PTTL" => {
                        if elements.len() != 2 { return Err("PTTL requires 1 argument".to_string()); }
                        Ok(Command::Pttl(Self::extract_string_zc(&elements[1])?))
                    }
                    "PERSIST" => {
                        if elements.len() != 2 { return Err("PERSIST requires 1 argument".to_string()); }
                        Ok(Command::Persist(Self::extract_string_zc(&elements[1])?))
                    }
                    "INCR" => {
                        if elements.len() != 2 { return Err("INCR requires 1 argument".to_string()); }
                        Ok(Command::Incr(Self::extract_string_zc(&elements[1])?))
                    }
                    "DECR" => {
                        if elements.len() != 2 { return Err("DECR requires 1 argument".to_string()); }
                        Ok(Command::Decr(Self::extract_string_zc(&elements[1])?))
                    }
                    "INCRBY" => {
                        if elements.len() != 3 { return Err("INCRBY requires 2 arguments".to_string()); }
                        Ok(Command::IncrBy(Self::extract_string_zc(&elements[1])?, Self::extract_integer_zc(&elements[2])? as i64))
                    }
                    "DECRBY" => {
                        if elements.len() != 3 { return Err("DECRBY requires 2 arguments".to_string()); }
                        Ok(Command::DecrBy(Self::extract_string_zc(&elements[1])?, Self::extract_integer_zc(&elements[2])? as i64))
                    }
                    "APPEND" => {
                        if elements.len() != 3 { return Err("APPEND requires 2 arguments".to_string()); }
                        Ok(Command::Append(Self::extract_string_zc(&elements[1])?, Self::extract_sds_zc(&elements[2])?))
                    }
                    "GETSET" => {
                        if elements.len() != 3 { return Err("GETSET requires 2 arguments".to_string()); }
                        Ok(Command::GetSet(Self::extract_string_zc(&elements[1])?, Self::extract_sds_zc(&elements[2])?))
                    }
                    "STRLEN" => {
                        if elements.len() != 2 { return Err("STRLEN requires 1 argument".to_string()); }
                        Ok(Command::StrLen(Self::extract_string_zc(&elements[1])?))
                    }
                    "MGET" => {
                        if elements.len() < 2 { return Err("MGET requires at least 1 argument".to_string()); }
                        Ok(Command::MGet(elements[1..].iter().map(Self::extract_string_zc).collect::<Result<Vec<_>, _>>()?))
                    }
                    "MSET" => {
                        if elements.len() < 3 || (elements.len() - 1) % 2 != 0 { return Err("MSET requires key-value pairs".to_string()); }
                        // Pre-allocate capacity (Abseil Tip #19)
                        let mut pairs = Vec::with_capacity((elements.len() - 1) / 2);
                        for i in (1..elements.len()).step_by(2) {
                            pairs.push((Self::extract_string_zc(&elements[i])?, Self::extract_sds_zc(&elements[i + 1])?));
                        }
                        Ok(Command::MSet(pairs))
                    }
                    "LPUSH" => {
                        if elements.len() < 3 { return Err("LPUSH requires key and values".to_string()); }
                        let key = Self::extract_string_zc(&elements[1])?;
                        let values = elements[2..].iter().map(Self::extract_sds_zc).collect::<Result<Vec<_>, _>>()?;
                        Ok(Command::LPush(key, values))
                    }
                    "RPUSH" => {
                        if elements.len() < 3 { return Err("RPUSH requires key and values".to_string()); }
                        let key = Self::extract_string_zc(&elements[1])?;
                        let values = elements[2..].iter().map(Self::extract_sds_zc).collect::<Result<Vec<_>, _>>()?;
                        Ok(Command::RPush(key, values))
                    }
                    "LPOP" => {
                        if elements.len() != 2 { return Err("LPOP requires 1 argument".to_string()); }
                        Ok(Command::LPop(Self::extract_string_zc(&elements[1])?))
                    }
                    "RPOP" => {
                        if elements.len() != 2 { return Err("RPOP requires 1 argument".to_string()); }
                        Ok(Command::RPop(Self::extract_string_zc(&elements[1])?))
                    }
                    "LRANGE" => {
                        if elements.len() != 4 { return Err("LRANGE requires 3 arguments".to_string()); }
                        Ok(Command::LRange(Self::extract_string_zc(&elements[1])?, Self::extract_integer_zc(&elements[2])?, Self::extract_integer_zc(&elements[3])?))
                    }
                    "LLEN" => {
                        if elements.len() != 2 { return Err("LLEN requires 1 argument".to_string()); }
                        Ok(Command::LLen(Self::extract_string_zc(&elements[1])?))
                    }
                    "LINDEX" => {
                        if elements.len() != 3 { return Err("LINDEX requires 2 arguments".to_string()); }
                        Ok(Command::LIndex(Self::extract_string_zc(&elements[1])?, Self::extract_integer_zc(&elements[2])?))
                    }
                    "SADD" => {
                        if elements.len() < 3 { return Err("SADD requires key and members".to_string()); }
                        let key = Self::extract_string_zc(&elements[1])?;
                        let members = elements[2..].iter().map(Self::extract_sds_zc).collect::<Result<Vec<_>, _>>()?;
                        Ok(Command::SAdd(key, members))
                    }
                    "SMEMBERS" => {
                        if elements.len() != 2 { return Err("SMEMBERS requires 1 argument".to_string()); }
                        Ok(Command::SMembers(Self::extract_string_zc(&elements[1])?))
                    }
                    "SISMEMBER" => {
                        if elements.len() != 3 { return Err("SISMEMBER requires 2 arguments".to_string()); }
                        Ok(Command::SIsMember(Self::extract_string_zc(&elements[1])?, Self::extract_sds_zc(&elements[2])?))
                    }
                    "SREM" => {
                        if elements.len() < 3 { return Err("SREM requires at least 2 arguments".to_string()); }
                        let key = Self::extract_string_zc(&elements[1])?;
                        let members = elements[2..].iter().map(Self::extract_sds_zc).collect::<Result<Vec<_>, _>>()?;
                        Ok(Command::SRem(key, members))
                    }
                    "SCARD" => {
                        if elements.len() != 2 { return Err("SCARD requires 1 argument".to_string()); }
                        Ok(Command::SCard(Self::extract_string_zc(&elements[1])?))
                    }
                    "HSET" => {
                        // HSET key field value [field value ...]
                        if elements.len() < 4 || (elements.len() - 2) % 2 != 0 {
                            return Err("HSET requires key and field-value pairs".to_string());
                        }
                        let key = Self::extract_string_zc(&elements[1])?;
                        let mut pairs = Vec::with_capacity((elements.len() - 2) / 2);
                        for i in (2..elements.len()).step_by(2) {
                            let field = Self::extract_sds_zc(&elements[i])?;
                            let value = Self::extract_sds_zc(&elements[i + 1])?;
                            pairs.push((field, value));
                        }
                        Ok(Command::HSet(key, pairs))
                    }
                    "HGET" => {
                        if elements.len() != 3 { return Err("HGET requires 2 arguments".to_string()); }
                        Ok(Command::HGet(Self::extract_string_zc(&elements[1])?, Self::extract_sds_zc(&elements[2])?))
                    }
                    "HGETALL" => {
                        if elements.len() != 2 { return Err("HGETALL requires 1 argument".to_string()); }
                        Ok(Command::HGetAll(Self::extract_string_zc(&elements[1])?))
                    }
                    "HINCRBY" => {
                        if elements.len() != 4 { return Err("HINCRBY requires 3 arguments".to_string()); }
                        Ok(Command::HIncrBy(Self::extract_string_zc(&elements[1])?, Self::extract_sds_zc(&elements[2])?, Self::extract_i64_zc(&elements[3])?))
                    }
                    "HDEL" => {
                        if elements.len() < 3 { return Err("HDEL requires at least 2 arguments".to_string()); }
                        let key = Self::extract_string_zc(&elements[1])?;
                        let fields = elements[2..].iter().map(Self::extract_sds_zc).collect::<Result<Vec<_>, _>>()?;
                        Ok(Command::HDel(key, fields))
                    }
                    "HKEYS" => {
                        if elements.len() != 2 { return Err("HKEYS requires 1 argument".to_string()); }
                        Ok(Command::HKeys(Self::extract_string_zc(&elements[1])?))
                    }
                    "HVALS" => {
                        if elements.len() != 2 { return Err("HVALS requires 1 argument".to_string()); }
                        Ok(Command::HVals(Self::extract_string_zc(&elements[1])?))
                    }
                    "HLEN" => {
                        if elements.len() != 2 { return Err("HLEN requires 1 argument".to_string()); }
                        Ok(Command::HLen(Self::extract_string_zc(&elements[1])?))
                    }
                    "HEXISTS" => {
                        if elements.len() != 3 { return Err("HEXISTS requires 2 arguments".to_string()); }
                        Ok(Command::HExists(Self::extract_string_zc(&elements[1])?, Self::extract_sds_zc(&elements[2])?))
                    }
                    "ZADD" => {
                        if elements.len() < 4 || (elements.len() - 2) % 2 != 0 { return Err("ZADD requires key and score-member pairs".to_string()); }
                        let key = Self::extract_string_zc(&elements[1])?;
                        // Pre-allocate capacity (Abseil Tip #19)
                        let mut pairs = Vec::with_capacity((elements.len() - 2) / 2);
                        for i in (2..elements.len()).step_by(2) {
                            pairs.push((Self::extract_float_zc(&elements[i])?, Self::extract_sds_zc(&elements[i + 1])?));
                        }
                        Ok(Command::ZAdd(key, pairs))
                    }
                    "ZRANGE" => {
                        if elements.len() != 4 { return Err("ZRANGE requires 3 arguments".to_string()); }
                        Ok(Command::ZRange(Self::extract_string_zc(&elements[1])?, Self::extract_integer_zc(&elements[2])?, Self::extract_integer_zc(&elements[3])?))
                    }
                    "ZREVRANGE" => {
                        if elements.len() < 4 || elements.len() > 5 { return Err("ZREVRANGE requires 3 or 4 arguments".to_string()); }
                        let key = Self::extract_string_zc(&elements[1])?;
                        let start = Self::extract_integer_zc(&elements[2])?;
                        let stop = Self::extract_integer_zc(&elements[3])?;
                        let with_scores = if elements.len() == 5 {
                            Self::extract_string_zc(&elements[4])?.to_uppercase() == "WITHSCORES"
                        } else {
                            false
                        };
                        Ok(Command::ZRevRange(key, start, stop, with_scores))
                    }
                    "ZSCORE" => {
                        if elements.len() != 3 { return Err("ZSCORE requires 2 arguments".to_string()); }
                        Ok(Command::ZScore(Self::extract_string_zc(&elements[1])?, Self::extract_sds_zc(&elements[2])?))
                    }
                    "ZREM" => {
                        if elements.len() < 3 { return Err("ZREM requires at least 2 arguments".to_string()); }
                        let key = Self::extract_string_zc(&elements[1])?;
                        let members = elements[2..].iter().map(Self::extract_sds_zc).collect::<Result<Vec<_>, _>>()?;
                        Ok(Command::ZRem(key, members))
                    }
                    "ZRANK" => {
                        if elements.len() != 3 { return Err("ZRANK requires 2 arguments".to_string()); }
                        Ok(Command::ZRank(Self::extract_string_zc(&elements[1])?, Self::extract_sds_zc(&elements[2])?))
                    }
                    "ZCARD" => {
                        if elements.len() != 2 { return Err("ZCARD requires 1 argument".to_string()); }
                        Ok(Command::ZCard(Self::extract_string_zc(&elements[1])?))
                    }
                    _ => Ok(Command::Unknown(cmd_name)),
                }
            }
            _ => Err("Invalid command format".to_string()),
        }
    }

    fn extract_string_zc(value: &RespValueZeroCopy) -> Result<String, String> {
        match value {
            RespValueZeroCopy::BulkString(Some(data)) => Ok(String::from_utf8_lossy(data).to_string()),
            _ => Err("Expected bulk string".to_string()),
        }
    }

    fn extract_sds_zc(value: &RespValueZeroCopy) -> Result<SDS, String> {
        match value {
            RespValueZeroCopy::BulkString(Some(data)) => Ok(SDS::new(data.to_vec())),
            _ => Err("Expected bulk string".to_string()),
        }
    }

    fn extract_integer_zc(value: &RespValueZeroCopy) -> Result<isize, String> {
        match value {
            RespValueZeroCopy::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                s.parse::<isize>().map_err(|e| e.to_string())
            }
            RespValueZeroCopy::Integer(n) => Ok(*n as isize),
            _ => Err("Expected integer".to_string()),
        }
    }

    fn extract_float_zc(value: &RespValueZeroCopy) -> Result<f64, String> {
        match value {
            RespValueZeroCopy::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                s.parse::<f64>().map_err(|e| e.to_string())
            }
            _ => Err("Expected float".to_string()),
        }
    }

    fn extract_i64_zc(value: &RespValueZeroCopy) -> Result<i64, String> {
        match value {
            RespValueZeroCopy::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                s.parse::<i64>().map_err(|e| e.to_string())
            }
            RespValueZeroCopy::Integer(n) => Ok(*n),
            _ => Err("Expected integer".to_string()),
        }
    }
}

pub struct CommandExecutor {
    data: AHashMap<String, Value>,
    expirations: AHashMap<String, VirtualTime>,
    current_time: VirtualTime,
    access_times: AHashMap<String, VirtualTime>,
    #[allow(dead_code)]
    key_count: usize,
    commands_processed: usize,
    simulation_start_epoch: i64,
}

impl Command {
    /// Returns true if this command only reads data (no mutations)
    pub fn is_read_only(&self) -> bool {
        matches!(self,
            Command::Get(_) |
            Command::StrLen(_) |
            Command::MGet(_) |
            Command::Exists(_) |
            Command::TypeOf(_) |
            Command::Keys(_) |
            Command::Ttl(_) |
            Command::Pttl(_) |
            Command::LLen(_) |
            Command::LIndex(_, _) |
            Command::LRange(_, _, _) |
            Command::SMembers(_) |
            Command::SIsMember(_, _) |
            Command::SCard(_) |
            Command::HGet(_, _) |
            Command::HGetAll(_) |
            Command::HKeys(_) |
            Command::HVals(_) |
            Command::HLen(_) |
            Command::HExists(_, _) |
            Command::ZRange(_, _, _) |
            Command::ZRevRange(_, _, _, _) |
            Command::ZScore(_, _) |
            Command::ZRank(_, _) |
            Command::ZCard(_) |
            Command::Info |
            Command::Ping
        )
    }
    
    /// Returns the key(s) this command operates on (for sharding)
    pub fn get_primary_key(&self) -> Option<&str> {
        match self {
            Command::Get(k) | Command::Set(k, _) | Command::SetEx(k, _, _) |
            Command::SetNx(k, _) | Command::Del(k) | Command::TypeOf(k) |
            Command::Expire(k, _) | Command::ExpireAt(k, _) | Command::PExpireAt(k, _) |
            Command::Ttl(k) | Command::Pttl(k) | Command::Persist(k) |
            Command::Incr(k) | Command::Decr(k) | Command::IncrBy(k, _) |
            Command::DecrBy(k, _) | Command::Append(k, _) | Command::GetSet(k, _) |
            Command::StrLen(k) |
            Command::LPush(k, _) | Command::RPush(k, _) | Command::LPop(k) |
            Command::RPop(k) | Command::LLen(k) | Command::LIndex(k, _) | Command::LRange(k, _, _) |
            Command::SAdd(k, _) | Command::SRem(k, _) | Command::SMembers(k) |
            Command::SIsMember(k, _) | Command::SCard(k) |
            Command::HSet(k, _) | Command::HGet(k, _) | Command::HDel(k, _) |
            Command::HGetAll(k) | Command::HKeys(k) | Command::HVals(k) |
            Command::HLen(k) | Command::HExists(k, _) | Command::HIncrBy(k, _, _) |
            Command::ZAdd(k, _) | Command::ZRem(k, _) | Command::ZRange(k, _, _) |
            Command::ZRevRange(k, _, _, _) | Command::ZScore(k, _) |
            Command::ZRank(k, _) | Command::ZCard(k) => Some(k.as_str()),
            Command::Exists(keys) => keys.first().map(|s| s.as_str()),
            Command::MGet(keys) => keys.first().map(|s| s.as_str()),
            Command::MSet(pairs) => pairs.first().map(|(k, _)| k.as_str()),
            Command::BatchSet(pairs) => pairs.first().map(|(k, _)| k.as_str()),
            Command::BatchGet(keys) => keys.first().map(|s| s.as_str()),
            Command::Keys(_) | Command::FlushDb | Command::FlushAll |
            Command::Info | Command::Ping | Command::Unknown(_) => None,
        }
    }

    /// Returns the command name as a string (for metrics/tracing)
    #[inline]
    pub fn name(&self) -> &'static str {
        match self {
            Command::Get(_) => "GET",
            Command::Set(_, _) => "SET",
            Command::SetEx(_, _, _) => "SETEX",
            Command::SetNx(_, _) => "SETNX",
            Command::Append(_, _) => "APPEND",
            Command::GetSet(_, _) => "GETSET",
            Command::StrLen(_) => "STRLEN",
            Command::MGet(_) => "MGET",
            Command::MSet(_) => "MSET",
            Command::BatchSet(_) => "BATCHSET",
            Command::BatchGet(_) => "BATCHGET",
            Command::Incr(_) => "INCR",
            Command::Decr(_) => "DECR",
            Command::IncrBy(_, _) => "INCRBY",
            Command::DecrBy(_, _) => "DECRBY",
            Command::Del(_) => "DEL",
            Command::Exists(_) => "EXISTS",
            Command::TypeOf(_) => "TYPE",
            Command::Keys(_) => "KEYS",
            Command::FlushDb => "FLUSHDB",
            Command::FlushAll => "FLUSHALL",
            Command::Expire(_, _) => "EXPIRE",
            Command::ExpireAt(_, _) => "EXPIREAT",
            Command::PExpireAt(_, _) => "PEXPIREAT",
            Command::Ttl(_) => "TTL",
            Command::Pttl(_) => "PTTL",
            Command::Persist(_) => "PERSIST",
            Command::LPush(_, _) => "LPUSH",
            Command::RPush(_, _) => "RPUSH",
            Command::LPop(_) => "LPOP",
            Command::RPop(_) => "RPOP",
            Command::LLen(_) => "LLEN",
            Command::LIndex(_, _) => "LINDEX",
            Command::LRange(_, _, _) => "LRANGE",
            Command::SAdd(_, _) => "SADD",
            Command::SRem(_, _) => "SREM",
            Command::SMembers(_) => "SMEMBERS",
            Command::SIsMember(_, _) => "SISMEMBER",
            Command::SCard(_) => "SCARD",
            Command::HSet(_, _) => "HSET",
            Command::HGet(_, _) => "HGET",
            Command::HDel(_, _) => "HDEL",
            Command::HGetAll(_) => "HGETALL",
            Command::HKeys(_) => "HKEYS",
            Command::HVals(_) => "HVALS",
            Command::HLen(_) => "HLEN",
            Command::HExists(_, _) => "HEXISTS",
            Command::HIncrBy(_, _, _) => "HINCRBY",
            Command::ZAdd(_, _) => "ZADD",
            Command::ZRem(_, _) => "ZREM",
            Command::ZRange(_, _, _) => "ZRANGE",
            Command::ZRevRange(_, _, _, _) => "ZREVRANGE",
            Command::ZScore(_, _) => "ZSCORE",
            Command::ZRank(_, _) => "ZRANK",
            Command::ZCard(_) => "ZCARD",
            Command::Info => "INFO",
            Command::Ping => "PING",
            Command::Unknown(_) => "UNKNOWN",
        }
    }
}

impl CommandExecutor {
    pub fn new() -> Self {
        CommandExecutor {
            data: AHashMap::new(),
            expirations: AHashMap::new(),
            current_time: VirtualTime::from_millis(0),
            access_times: AHashMap::new(),
            key_count: 0,
            commands_processed: 0,
            simulation_start_epoch: 0,
        }
    }
    
    pub fn set_simulation_start_epoch(&mut self, epoch: i64) {
        self.simulation_start_epoch = epoch;
    }

    pub fn set_time(&mut self, time: VirtualTime) {
        self.current_time = time;
        self.evict_expired_keys();
    }
    
    pub fn get_current_time(&self) -> VirtualTime {
        self.current_time
    }
    
    /// Update time without evicting keys (for read-only operations)
    pub fn update_time_readonly(&mut self, time: VirtualTime) {
        self.current_time = time;
    }

    fn is_expired(&self, key: &str) -> bool {
        if let Some(expiration) = self.expirations.get(key) {
            *expiration <= self.current_time
        } else {
            false
        }
    }

    /// Fast path GET - avoids Command enum overhead
    /// Used by the fast path in connection handler for ~10-15% improvement
    #[inline]
    pub fn get_direct(&mut self, key: &str) -> RespValue {
        self.commands_processed += 1;
        match self.get_value(key) {
            Some(Value::String(s)) => RespValue::BulkString(Some(s.as_bytes().to_vec())),
            Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => RespValue::BulkString(None),
        }
    }

    /// Fast path SET - avoids Command enum overhead
    /// Used by the fast path in connection handler for ~10-15% improvement
    #[inline]
    pub fn set_direct(&mut self, key: &str, value: &[u8]) -> RespValue {
        self.commands_processed += 1;
        self.data.insert(key.to_string(), Value::String(SDS::new(value.to_vec())));
        self.expirations.remove(key);
        self.access_times.insert(key.to_string(), self.current_time);
        RespValue::SimpleString("OK".to_string())
    }

    /// Direct expiration eviction - call this from TTL manager
    /// Returns the number of keys evicted
    pub fn evict_expired_direct(&mut self, current_time: VirtualTime) -> usize {
        // TigerStyle: Capture pre-state for postcondition verification
        #[cfg(debug_assertions)]
        let pre_data_len = self.data.len();
        #[cfg(debug_assertions)]
        let pre_exp_len = self.expirations.len();

        self.current_time = current_time;

        let expired_keys: Vec<String> = self.expirations
            .iter()
            .filter(|(_, &exp_time)| exp_time <= self.current_time)
            .map(|(k, _)| k.clone())
            .collect();

        let count = expired_keys.len();
        for key in expired_keys {
            self.data.remove(&key);
            self.expirations.remove(&key);
            self.access_times.remove(&key);
        }

        // TigerStyle: Postconditions
        #[cfg(debug_assertions)]
        {
            debug_assert_eq!(
                self.data.len(),
                pre_data_len.saturating_sub(count),
                "Postcondition: data size must decrease by evicted count"
            );
            debug_assert_eq!(
                self.expirations.len(),
                pre_exp_len.saturating_sub(count),
                "Postcondition: expirations size must decrease by evicted count"
            );
            // No expired keys should remain
            for (_, &exp_time) in &self.expirations {
                debug_assert!(
                    exp_time > self.current_time,
                    "Postcondition: no expired keys should remain after eviction"
                );
            }
        }

        count
    }

    fn evict_expired_keys(&mut self) {
        // TigerStyle: Capture pre-state for postcondition verification
        #[cfg(debug_assertions)]
        let pre_data_len = self.data.len();

        let expired_keys: Vec<String> = self.expirations
            .iter()
            .filter(|(_, &exp_time)| exp_time <= self.current_time)
            .map(|(k, _)| k.clone())
            .collect();

        #[cfg(debug_assertions)]
        let evicted_count = expired_keys.len();

        for key in expired_keys {
            self.data.remove(&key);
            self.expirations.remove(&key);
            self.access_times.remove(&key);
        }

        // TigerStyle: Postconditions
        #[cfg(debug_assertions)]
        {
            debug_assert_eq!(
                self.data.len(),
                pre_data_len.saturating_sub(evicted_count),
                "Postcondition: data size must decrease by evicted count"
            );
            // No expired keys should remain
            for (_, &exp_time) in &self.expirations {
                debug_assert!(
                    exp_time > self.current_time,
                    "Postcondition: no expired keys should remain after eviction"
                );
            }
        }
    }
    
    /// Execute a read-only command (can be called with just &self for some operations)
    /// Note: This still requires &mut self due to access_times updates, but is semantically read-only
    pub fn execute_read(&mut self, cmd: &Command) -> RespValue {
        debug_assert!(cmd.is_read_only(), "execute_read called with write command");
        self.execute(cmd)
    }

    /// Execute a read-only command without updating access times
    /// This can be used when we only have immutable access to the executor
    pub fn execute_readonly(&self, cmd: &Command) -> RespValue {
        match cmd {
            Command::Get(key) => {
                if self.is_expired(key) {
                    return RespValue::BulkString(None);
                }
                match self.data.get(key) {
                    Some(Value::String(s)) => RespValue::BulkString(Some(s.as_bytes().to_vec())),
                    Some(_) => RespValue::Error("WRONGTYPE".to_string()),
                    None => RespValue::BulkString(None),
                }
            }
            Command::Exists(keys) => {
                let count = keys.iter().filter(|k| {
                    !self.is_expired(k) && self.data.contains_key(*k)
                }).count();
                RespValue::Integer(count as i64)
            }
            Command::Keys(pattern) => {
                let matching: Vec<RespValue> = self.data.keys()
                    .filter(|k| !self.is_expired(k) && self.matches_glob_pattern(k, pattern))
                    .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
                    .collect();
                RespValue::Array(Some(matching))
            }
            Command::Ping => RespValue::SimpleString("PONG".to_string()),
            _ => RespValue::Error("ERR command not supported in readonly mode".to_string()),
        }
    }

    /// Get read-only access to the data store
    pub fn get_data(&self) -> &AHashMap<String, Value> {
        &self.data
    }

    fn get_value(&mut self, key: &str) -> Option<&Value> {
        if self.is_expired(key) {
            self.data.remove(key);
            self.expirations.remove(key);
            self.access_times.remove(key);
            None
        } else {
            self.access_times.insert(key.to_string(), self.current_time);
            self.data.get(key)
        }
    }

    fn get_value_mut(&mut self, key: &str) -> Option<&mut Value> {
        if self.is_expired(key) {
            self.data.remove(key);
            self.expirations.remove(key);
            self.access_times.remove(key);
            None
        } else {
            self.access_times.insert(key.to_string(), self.current_time);
            self.data.get_mut(key)
        }
    }

    pub fn execute(&mut self, cmd: &Command) -> RespValue {
        self.commands_processed += 1;
        match cmd {
            Command::Ping => RespValue::SimpleString("PONG".to_string()),
            
            Command::Info => {
                let info = format!(
                    "# Server\r\n\
                     redis_mode:simulator\r\n\
                     \r\n\
                     # Stats\r\n\
                     total_commands_processed:{}\r\n\
                     total_keys:{}\r\n\
                     keys_with_expiration:{}\r\n\
                     current_time_ms:{}\r\n",
                    self.commands_processed,
                    self.data.len(),
                    self.expirations.len(),
                    self.current_time.as_millis()
                );
                RespValue::BulkString(Some(info.into_bytes()))
            }
            
            Command::Get(key) => {
                match self.get_value(key) {
                    Some(Value::String(s)) => RespValue::BulkString(Some(s.as_bytes().to_vec())),
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::BulkString(None),
                }
            }
            
            Command::Set(key, value) => {
                self.data.insert(key.clone(), Value::String(value.clone()));
                self.expirations.remove(key);
                self.access_times.insert(key.clone(), self.current_time);
                RespValue::SimpleString("OK".to_string())
            }
            
            Command::SetEx(key, seconds, value) => {
                if *seconds <= 0 {
                    return RespValue::Error("ERR invalid expire time in setex".to_string());
                }
                self.data.insert(key.clone(), Value::String(value.clone()));
                let expiration = self.current_time + crate::simulator::Duration::from_secs(*seconds as u64);
                self.expirations.insert(key.clone(), expiration);
                self.access_times.insert(key.clone(), self.current_time);
                RespValue::SimpleString("OK".to_string())
            }
            
            Command::SetNx(key, value) => {
                if !self.is_expired(key) && self.data.contains_key(key) {
                    RespValue::Integer(0)
                } else {
                    self.data.insert(key.clone(), Value::String(value.clone()));
                    self.expirations.remove(key);
                    self.access_times.insert(key.clone(), self.current_time);
                    RespValue::Integer(1)
                }
            }
            
            Command::Del(key) => {
                let removed = self.data.remove(key).is_some();
                self.expirations.remove(key);
                self.access_times.remove(key);
                RespValue::Integer(if removed { 1 } else { 0 })
            }
            
            Command::Exists(keys) => {
                let count = keys.iter().filter(|k| {
                    !self.is_expired(k) && self.data.contains_key(*k)
                }).count();
                RespValue::Integer(count as i64)
            }
            
            Command::TypeOf(key) => {
                match self.get_value(key) {
                    Some(Value::String(_)) => RespValue::SimpleString("string".to_string()),
                    Some(Value::List(_)) => RespValue::SimpleString("list".to_string()),
                    Some(Value::Set(_)) => RespValue::SimpleString("set".to_string()),
                    Some(Value::Hash(_)) => RespValue::SimpleString("hash".to_string()),
                    Some(Value::SortedSet(_)) => RespValue::SimpleString("zset".to_string()),
                    _ => RespValue::SimpleString("none".to_string()),
                }
            }
            
            Command::Keys(pattern) => {
                let keys: Vec<RespValue> = self.data.keys()
                    .filter(|k| !self.is_expired(k) && self.matches_glob_pattern(k, pattern))
                    .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
                    .collect();
                RespValue::Array(Some(keys))
            }
            
            Command::FlushDb | Command::FlushAll => {
                self.data.clear();
                self.expirations.clear();
                self.access_times.clear();
                RespValue::SimpleString("OK".to_string())
            }
            
            Command::Expire(key, seconds) => {
                if self.is_expired(key) || !self.data.contains_key(key) {
                    RespValue::Integer(0)
                } else {
                    if *seconds <= 0 {
                        self.data.remove(key);
                        self.expirations.remove(key);
                        self.access_times.remove(key);
                        RespValue::Integer(1)
                    } else {
                        let expiration = self.current_time + crate::simulator::Duration::from_secs(*seconds as u64);
                        self.expirations.insert(key.clone(), expiration);
                        RespValue::Integer(1)
                    }
                }
            }
            
            Command::ExpireAt(key, timestamp) => {
                if self.is_expired(key) || !self.data.contains_key(key) {
                    RespValue::Integer(0)
                } else {
                    let simulation_relative_secs = *timestamp - self.simulation_start_epoch;
                    if simulation_relative_secs <= 0 {
                        self.data.remove(key);
                        self.expirations.remove(key);
                        self.access_times.remove(key);
                        RespValue::Integer(1)
                    } else {
                        let expiration_millis = (simulation_relative_secs as u64).saturating_mul(1000);
                        if expiration_millis <= self.current_time.as_millis() {
                            self.data.remove(key);
                            self.expirations.remove(key);
                            self.access_times.remove(key);
                            RespValue::Integer(1)
                        } else {
                            let expiration = VirtualTime::from_millis(expiration_millis);
                            self.expirations.insert(key.clone(), expiration);
                            RespValue::Integer(1)
                        }
                    }
                }
            }
            
            Command::PExpireAt(key, timestamp_millis) => {
                if self.is_expired(key) || !self.data.contains_key(key) {
                    RespValue::Integer(0)
                } else {
                    let simulation_relative_millis = *timestamp_millis - (self.simulation_start_epoch * 1000);
                    if simulation_relative_millis <= 0 {
                        self.data.remove(key);
                        self.expirations.remove(key);
                        self.access_times.remove(key);
                        RespValue::Integer(1)
                    } else if (simulation_relative_millis as u64) <= self.current_time.as_millis() {
                        self.data.remove(key);
                        self.expirations.remove(key);
                        self.access_times.remove(key);
                        RespValue::Integer(1)
                    } else {
                        let expiration = VirtualTime::from_millis(simulation_relative_millis as u64);
                        self.expirations.insert(key.clone(), expiration);
                        RespValue::Integer(1)
                    }
                }
            }
            
            Command::Ttl(key) => {
                if self.is_expired(key) || !self.data.contains_key(key) {
                    RespValue::Integer(-2)
                } else if let Some(expiration) = self.expirations.get(key) {
                    let remaining_ms = expiration.as_millis() as i64 - self.current_time.as_millis() as i64;
                    let remaining_secs = (remaining_ms / 1000).max(0);
                    RespValue::Integer(remaining_secs)
                } else {
                    RespValue::Integer(-1)
                }
            }
            
            Command::Pttl(key) => {
                if self.is_expired(key) || !self.data.contains_key(key) {
                    RespValue::Integer(-2)
                } else if let Some(expiration) = self.expirations.get(key) {
                    let remaining = expiration.as_millis() as i64 - self.current_time.as_millis() as i64;
                    RespValue::Integer(remaining.max(0))
                } else {
                    RespValue::Integer(-1)
                }
            }
            
            Command::Persist(key) => {
                if self.is_expired(key) || !self.data.contains_key(key) {
                    RespValue::Integer(0)
                } else if self.expirations.remove(key).is_some() {
                    RespValue::Integer(1)
                } else {
                    RespValue::Integer(0)
                }
            }
            
            Command::Incr(key) => {
                self.incr_by_impl(key, 1)
            }
            
            Command::Decr(key) => {
                self.incr_by_impl(key, -1)
            }
            
            Command::IncrBy(key, increment) => {
                self.incr_by_impl(key, *increment)
            }
            
            Command::DecrBy(key, decrement) => {
                // TigerStyle: Use checked_neg() to prevent overflow when decrement == i64::MIN
                match decrement.checked_neg() {
                    Some(negated) => self.incr_by_impl(key, negated),
                    None => RespValue::Error("ERR value is out of range".to_string()),
                }
            }
            
            Command::Append(key, value) => {
                match self.get_value_mut(key) {
                    Some(Value::String(s)) => {
                        s.append(value);
                        RespValue::Integer(s.len() as i64)
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => {
                        let len = value.len();
                        self.data.insert(key.clone(), Value::String(value.clone()));
                        self.access_times.insert(key.clone(), self.current_time);
                        RespValue::Integer(len as i64)
                    }
                }
            }
            
            Command::GetSet(key, value) => {
                let old_value = match self.get_value(key) {
                    Some(Value::String(s)) => RespValue::BulkString(Some(s.as_bytes().to_vec())),
                    Some(_) => return RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::BulkString(None),
                };
                self.data.insert(key.clone(), Value::String(value.clone()));
                self.access_times.insert(key.clone(), self.current_time);
                old_value
            }
            
            Command::MGet(keys) => {
                let values: Vec<RespValue> = keys.iter().map(|k| {
                    match self.get_value(k) {
                        Some(Value::String(s)) => RespValue::BulkString(Some(s.as_bytes().to_vec())),
                        _ => RespValue::BulkString(None),
                    }
                }).collect();
                RespValue::Array(Some(values))
            }
            
            Command::MSet(pairs) => {
                for (key, value) in pairs {
                    self.data.insert(key.clone(), Value::String(value.clone()));
                    self.access_times.insert(key.clone(), self.current_time);
                }
                RespValue::SimpleString("OK".to_string())
            }

            Command::BatchSet(pairs) => {
                // Optimized batch set - all keys are guaranteed to be on this shard
                for (key, value) in pairs {
                    self.data.insert(key.clone(), Value::String(value.clone()));
                    self.access_times.insert(key.clone(), self.current_time);
                }
                RespValue::SimpleString("OK".to_string())
            }

            Command::BatchGet(keys) => {
                // Optimized batch get - all keys are guaranteed to be on this shard
                // Pre-allocate result vector with capacity
                let mut results = Vec::with_capacity(keys.len());
                for key in keys {
                    let value = self.get_value(key);
                    results.push(match value {
                        Some(Value::String(s)) => RespValue::BulkString(Some(s.as_bytes().to_vec())),
                        Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                        None => RespValue::BulkString(None),
                    });
                }
                RespValue::Array(Some(results))
            }

            Command::LPush(key, values) => {
                if self.is_expired(key) {
                    self.data.remove(key);
                    self.expirations.remove(key);
                }
                let list = self.data.entry(key.clone()).or_insert_with(|| Value::List(RedisList::new()));
                self.access_times.insert(key.clone(), self.current_time);
                match list {
                    Value::List(l) => {
                        for value in values {
                            l.lpush(value.clone());
                        }
                        RespValue::Integer(l.len() as i64)
                    }
                    _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            
            Command::RPush(key, values) => {
                if self.is_expired(key) {
                    self.data.remove(key);
                    self.expirations.remove(key);
                }
                let list = self.data.entry(key.clone()).or_insert_with(|| Value::List(RedisList::new()));
                self.access_times.insert(key.clone(), self.current_time);
                match list {
                    Value::List(l) => {
                        for value in values {
                            l.rpush(value.clone());
                        }
                        RespValue::Integer(l.len() as i64)
                    }
                    _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            
            Command::LPop(key) => {
                match self.get_value_mut(key) {
                    Some(Value::List(l)) => {
                        match l.lpop() {
                            Some(v) => RespValue::BulkString(Some(v.as_bytes().to_vec())),
                            None => RespValue::BulkString(None),
                        }
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::BulkString(None),
                }
            }
            
            Command::RPop(key) => {
                match self.get_value_mut(key) {
                    Some(Value::List(l)) => {
                        match l.rpop() {
                            Some(v) => RespValue::BulkString(Some(v.as_bytes().to_vec())),
                            None => RespValue::BulkString(None),
                        }
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::BulkString(None),
                }
            }
            
            Command::LRange(key, start, stop) => {
                match self.get_value(key) {
                    Some(Value::List(l)) => {
                        let range = l.range(*start, *stop);
                        let elements: Vec<RespValue> = range
                            .iter()
                            .map(|s| RespValue::BulkString(Some(s.as_bytes().to_vec())))
                            .collect();
                        RespValue::Array(Some(elements))
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Array(Some(Vec::new())),
                }
            }
            
            Command::SAdd(key, members) => {
                if self.is_expired(key) {
                    self.data.remove(key);
                    self.expirations.remove(key);
                }
                let set = self.data.entry(key.clone()).or_insert_with(|| Value::Set(RedisSet::new()));
                self.access_times.insert(key.clone(), self.current_time);
                match set {
                    Value::Set(s) => {
                        let mut added = 0;
                        for member in members {
                            if s.add(member.clone()) {
                                added += 1;
                            }
                        }
                        RespValue::Integer(added)
                    }
                    _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            
            Command::SMembers(key) => {
                match self.get_value(key) {
                    Some(Value::Set(s)) => {
                        let members: Vec<RespValue> = s.members()
                            .iter()
                            .map(|m| RespValue::BulkString(Some(m.as_bytes().to_vec())))
                            .collect();
                        RespValue::Array(Some(members))
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Array(Some(Vec::new())),
                }
            }
            
            Command::SIsMember(key, member) => {
                match self.get_value(key) {
                    Some(Value::Set(s)) => {
                        RespValue::Integer(if s.contains(member) { 1 } else { 0 })
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Integer(0),
                }
            }
            
            Command::HSet(key, pairs) => {
                if self.is_expired(key) {
                    self.data.remove(key);
                    self.expirations.remove(key);
                }
                let hash = self.data.entry(key.clone()).or_insert_with(|| Value::Hash(RedisHash::new()));
                self.access_times.insert(key.clone(), self.current_time);
                match hash {
                    Value::Hash(h) => {
                        let mut new_fields = 0i64;
                        for (field, value) in pairs {
                            if !h.exists(field) {
                                new_fields += 1;
                            }
                            h.set(field.clone(), value.clone());
                        }
                        RespValue::Integer(new_fields)
                    }
                    _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            
            Command::HGet(key, field) => {
                match self.get_value(key) {
                    Some(Value::Hash(h)) => {
                        match h.get(field) {
                            Some(v) => RespValue::BulkString(Some(v.as_bytes().to_vec())),
                            None => RespValue::BulkString(None),
                        }
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::BulkString(None),
                }
            }
            
            Command::HGetAll(key) => {
                match self.get_value(key) {
                    Some(Value::Hash(h)) => {
                        // Pre-allocate capacity: each field has key and value (Abseil Tip #19)
                        let mut elements = Vec::with_capacity(h.len() * 2);
                        for (k, v) in h.get_all() {
                            elements.push(RespValue::BulkString(Some(k.as_bytes().to_vec())));
                            elements.push(RespValue::BulkString(Some(v.as_bytes().to_vec())));
                        }
                        RespValue::Array(Some(elements))
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Array(Some(Vec::new())),
                }
            }

            Command::HIncrBy(key, field, increment) => {
                let hash = self.data.entry(key.clone()).or_insert_with(|| {
                    Value::Hash(RedisHash::new())
                });

                match hash {
                    Value::Hash(h) => {
                        let current = h.get(field).map(|v| v.to_string().parse::<i64>().ok()).flatten().unwrap_or(0);

                        // TigerStyle: Use checked arithmetic to detect overflow
                        let new_value = match current.checked_add(*increment) {
                            Some(v) => v,
                            None => return RespValue::Error("ERR increment would produce overflow".to_string()),
                        };

                        h.set(field.clone(), SDS::from_str(&new_value.to_string()));

                        // TigerStyle: Assert invariants after mutation
                        debug_assert!(
                            h.get(field).is_some(),
                            "Invariant violated: field must exist after HINCRBY"
                        );
                        debug_assert_eq!(
                            h.get(field).map(|v| v.to_string().parse::<i64>().ok()).flatten(),
                            Some(new_value),
                            "Invariant violated: field value must equal computed value after HINCRBY"
                        );

                        RespValue::Integer(new_value)
                    }
                    _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }

            Command::ZAdd(key, pairs) => {
                if self.is_expired(key) {
                    self.data.remove(key);
                    self.expirations.remove(key);
                }
                let zset = self.data.entry(key.clone()).or_insert_with(|| Value::SortedSet(RedisSortedSet::new()));
                self.access_times.insert(key.clone(), self.current_time);
                match zset {
                    Value::SortedSet(zs) => {
                        let mut added = 0;
                        for (score, member) in pairs {
                            if zs.add(member.clone(), *score) {
                                added += 1;
                            }
                        }
                        RespValue::Integer(added)
                    }
                    _ => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            
            Command::ZRange(key, start, stop) => {
                match self.get_value(key) {
                    Some(Value::SortedSet(zs)) => {
                        let range = zs.range(*start, *stop);
                        let elements: Vec<RespValue> = range
                            .iter()
                            .map(|(m, _)| RespValue::BulkString(Some(m.as_bytes().to_vec())))
                            .collect();
                        RespValue::Array(Some(elements))
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Array(Some(Vec::new())),
                }
            }

            Command::ZRevRange(key, start, stop, with_scores) => {
                match self.get_value(key) {
                    Some(Value::SortedSet(zs)) => {
                        let range = zs.rev_range(*start, *stop);
                        if *with_scores {
                            let mut elements = Vec::with_capacity(range.len() * 2);
                            for (m, s) in range {
                                elements.push(RespValue::BulkString(Some(m.as_bytes().to_vec())));
                                elements.push(RespValue::BulkString(Some(s.to_string().into_bytes())));
                            }
                            RespValue::Array(Some(elements))
                        } else {
                            let elements: Vec<RespValue> = range
                                .iter()
                                .map(|(m, _)| RespValue::BulkString(Some(m.as_bytes().to_vec())))
                                .collect();
                            RespValue::Array(Some(elements))
                        }
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Array(Some(Vec::new())),
                }
            }

            Command::ZScore(key, member) => {
                match self.get_value(key) {
                    Some(Value::SortedSet(zs)) => {
                        match zs.score(member) {
                            Some(score) => {
                                let score_str = score.to_string();
                                RespValue::BulkString(Some(score_str.into_bytes()))
                            }
                            None => RespValue::BulkString(None),
                        }
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::BulkString(None),
                }
            }

            // === NEW COMMANDS (TigerStyle + VOPR) ===

            Command::StrLen(key) => {
                match self.get_value(key) {
                    Some(Value::String(s)) => {
                        let len = s.len() as i64;
                        // TigerStyle: Postcondition - length must be non-negative
                        debug_assert!(len >= 0, "Invariant violated: STRLEN must return non-negative");
                        RespValue::Integer(len)
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Integer(0),
                }
            }

            Command::LLen(key) => {
                match self.get_value(key) {
                    Some(Value::List(l)) => {
                        let len = l.len() as i64;
                        // TigerStyle: Postcondition - length must be non-negative
                        debug_assert!(len >= 0, "Invariant violated: LLEN must return non-negative");
                        RespValue::Integer(len)
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Integer(0),
                }
            }

            Command::LIndex(key, index) => {
                match self.get_value(key) {
                    Some(Value::List(l)) => {
                        let len = l.len() as isize;
                        // TigerStyle: Handle negative indices (Redis convention)
                        let actual_index = if *index < 0 {
                            let normalized = len + *index;
                            if normalized < 0 {
                                return RespValue::BulkString(None);
                            }
                            normalized as usize
                        } else if *index >= len {
                            return RespValue::BulkString(None);
                        } else {
                            *index as usize
                        };

                        // TigerStyle: Precondition verified
                        debug_assert!(actual_index < l.len(), "Invariant violated: index must be in bounds");

                        let range = l.range(actual_index as isize, actual_index as isize);
                        if let Some(item) = range.first() {
                            RespValue::BulkString(Some(item.as_bytes().to_vec()))
                        } else {
                            RespValue::BulkString(None)
                        }
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::BulkString(None),
                }
            }

            Command::SRem(key, members) => {
                match self.get_value_mut(key) {
                    Some(Value::Set(s)) => {
                        // TigerStyle: Capture pre-state for postcondition
                        #[cfg(debug_assertions)]
                        let pre_len = s.len();

                        let mut removed = 0i64;
                        for member in members {
                            if s.remove(member) {
                                removed += 1;
                            }
                        }

                        // TigerStyle: Postconditions
                        #[cfg(debug_assertions)]
                        {
                            debug_assert!(removed >= 0, "Invariant violated: removed count must be non-negative");
                            debug_assert!(removed <= members.len() as i64, "Invariant violated: can't remove more than requested");
                            debug_assert_eq!(s.len(), pre_len - removed as usize, "Invariant violated: len must decrease by removed count");
                        }

                        RespValue::Integer(removed)
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Integer(0),
                }
            }

            Command::SCard(key) => {
                match self.get_value(key) {
                    Some(Value::Set(s)) => {
                        let card = s.len() as i64;
                        // TigerStyle: Postcondition
                        debug_assert!(card >= 0, "Invariant violated: SCARD must return non-negative");
                        RespValue::Integer(card)
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Integer(0),
                }
            }

            Command::HDel(key, fields) => {
                match self.get_value_mut(key) {
                    Some(Value::Hash(h)) => {
                        // TigerStyle: Capture pre-state for postcondition
                        #[cfg(debug_assertions)]
                        let pre_len = h.len();

                        let mut deleted = 0i64;
                        for field in fields {
                            if h.delete(field) {
                                deleted += 1;
                            }
                        }

                        // TigerStyle: Postconditions
                        #[cfg(debug_assertions)]
                        {
                            debug_assert!(deleted >= 0, "Invariant violated: deleted count must be non-negative");
                            debug_assert!(deleted <= fields.len() as i64, "Invariant violated: can't delete more than requested");
                            debug_assert_eq!(h.len(), pre_len - deleted as usize, "Invariant violated: len must decrease by deleted count");
                        }

                        RespValue::Integer(deleted)
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Integer(0),
                }
            }

            Command::HKeys(key) => {
                match self.get_value(key) {
                    Some(Value::Hash(h)) => {
                        let hash_keys = h.keys();
                        // TigerStyle: Postcondition - keys count must equal len
                        debug_assert_eq!(hash_keys.len(), h.len(), "Invariant violated: HKEYS count must equal HLEN");

                        let keys: Vec<RespValue> = hash_keys
                            .iter()
                            .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
                            .collect();
                        RespValue::Array(Some(keys))
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Array(Some(Vec::new())),
                }
            }

            Command::HVals(key) => {
                match self.get_value(key) {
                    Some(Value::Hash(h)) => {
                        let hash_vals = h.values();
                        // TigerStyle: Postcondition - values count must equal len
                        debug_assert_eq!(hash_vals.len(), h.len(), "Invariant violated: HVALS count must equal HLEN");

                        let vals: Vec<RespValue> = hash_vals
                            .iter()
                            .map(|v| RespValue::BulkString(Some(v.as_bytes().to_vec())))
                            .collect();
                        RespValue::Array(Some(vals))
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Array(Some(Vec::new())),
                }
            }

            Command::HLen(key) => {
                match self.get_value(key) {
                    Some(Value::Hash(h)) => {
                        let len = h.len() as i64;
                        // TigerStyle: Postcondition
                        debug_assert!(len >= 0, "Invariant violated: HLEN must return non-negative");
                        RespValue::Integer(len)
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Integer(0),
                }
            }

            Command::HExists(key, field) => {
                match self.get_value(key) {
                    Some(Value::Hash(h)) => {
                        let exists = h.exists(field);
                        // TigerStyle: Postcondition - result must be 0 or 1
                        let result = if exists { 1i64 } else { 0i64 };
                        debug_assert!(result == 0 || result == 1, "Invariant violated: HEXISTS must return 0 or 1");
                        RespValue::Integer(result)
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Integer(0),
                }
            }

            Command::ZRem(key, members) => {
                match self.get_value_mut(key) {
                    Some(Value::SortedSet(zs)) => {
                        // TigerStyle: Capture pre-state for postcondition
                        #[cfg(debug_assertions)]
                        let pre_len = zs.len();

                        let mut removed = 0i64;
                        for member in members {
                            if zs.remove(member) {
                                removed += 1;
                            }
                        }

                        // TigerStyle: Postconditions
                        #[cfg(debug_assertions)]
                        {
                            debug_assert!(removed >= 0, "Invariant violated: removed count must be non-negative");
                            debug_assert!(removed <= members.len() as i64, "Invariant violated: can't remove more than requested");
                            debug_assert_eq!(zs.len(), pre_len - removed as usize, "Invariant violated: len must decrease by removed count");
                        }

                        RespValue::Integer(removed)
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Integer(0),
                }
            }

            Command::ZRank(key, member) => {
                match self.get_value(key) {
                    Some(Value::SortedSet(zs)) => {
                        match zs.rank(member) {
                            Some(rank) => {
                                // TigerStyle: Postcondition - rank must be valid index
                                debug_assert!(rank < zs.len(), "Invariant violated: rank must be less than zset length");
                                RespValue::Integer(rank as i64)
                            }
                            None => RespValue::BulkString(None),
                        }
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::BulkString(None),
                }
            }

            Command::ZCard(key) => {
                match self.get_value(key) {
                    Some(Value::SortedSet(zs)) => {
                        let card = zs.len() as i64;
                        // TigerStyle: Postcondition
                        debug_assert!(card >= 0, "Invariant violated: ZCARD must return non-negative");
                        RespValue::Integer(card)
                    }
                    Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                    None => RespValue::Integer(0),
                }
            }

            Command::Unknown(cmd) => {
                RespValue::Error(format!("ERR unknown command '{}'", cmd))
            }
        }
    }

    fn incr_by_impl(&mut self, key: &str, increment: i64) -> RespValue {
        // TigerStyle: Precondition
        debug_assert!(!key.is_empty(), "Precondition: key must not be empty");

        let response = match self.get_value_mut(key) {
            Some(Value::String(s)) => {
                let current = match s.to_string().parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => return RespValue::Error("ERR value is not an integer or out of range".to_string()),
                };
                let new_value = match current.checked_add(increment) {
                    Some(n) => n,
                    None => return RespValue::Error("ERR increment or decrement would overflow".to_string()),
                };
                let new_str = SDS::from_str(&new_value.to_string());
                self.data.insert(key.to_string(), Value::String(new_str));
                RespValue::Integer(new_value)
            }
            Some(_) => RespValue::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => {
                self.data.insert(key.to_string(), Value::String(SDS::from_str(&increment.to_string())));
                self.access_times.insert(key.to_string(), self.current_time);
                RespValue::Integer(increment)
            }
        };

        // TigerStyle: Postcondition - verify stored value matches returned value
        #[cfg(debug_assertions)]
        if let RespValue::Integer(result) = &response {
            if let Some(Value::String(s)) = self.data.get(key) {
                debug_assert_eq!(
                    s.to_string().parse::<i64>().ok(),
                    Some(*result),
                    "Postcondition: stored value must equal returned value"
                );
            }
        }

        response
    }

    fn matches_glob_pattern(&self, key: &str, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        
        let key_chars: Vec<char> = key.chars().collect();
        let pattern_chars: Vec<char> = pattern.chars().collect();
        
        self.glob_match(&key_chars, &pattern_chars, 0, 0)
    }

    fn glob_match(&self, key: &[char], pattern: &[char], k_idx: usize, p_idx: usize) -> bool {
        if p_idx == pattern.len() {
            return k_idx == key.len();
        }

        let p_char = pattern[p_idx];

        if p_char == '*' {
            for i in k_idx..=key.len() {
                if self.glob_match(key, pattern, i, p_idx + 1) {
                    return true;
                }
            }
            false
        } else if p_char == '?' {
            if k_idx >= key.len() {
                false
            } else {
                self.glob_match(key, pattern, k_idx + 1, p_idx + 1)
            }
        } else if p_char == '[' {
            if k_idx >= key.len() {
                return false;
            }
            
            let mut bracket_end = p_idx + 1;
            while bracket_end < pattern.len() && pattern[bracket_end] != ']' {
                bracket_end += 1;
            }
            
            if bracket_end >= pattern.len() {
                return p_char == key[k_idx] && self.glob_match(key, pattern, k_idx + 1, p_idx + 1);
            }
            
            let char_set: Vec<char> = pattern[p_idx + 1..bracket_end].to_vec();
            let negate = !char_set.is_empty() && char_set[0] == '^';
            let chars_to_check = if negate { &char_set[1..] } else { &char_set[..] };
            
            let mut matched = false;
            for &c in chars_to_check {
                if c == key[k_idx] {
                    matched = true;
                    break;
                }
            }
            
            if negate {
                matched = !matched;
            }
            
            if matched {
                self.glob_match(key, pattern, k_idx + 1, bracket_end + 1)
            } else {
                false
            }
        } else {
            if k_idx >= key.len() || key[k_idx] != p_char {
                false
            } else {
                self.glob_match(key, pattern, k_idx + 1, p_idx + 1)
            }
        }
    }
}
