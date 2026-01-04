#[cfg(test)]
mod resp_parser_tests {
    use super::super::{RespParser, RespValue, RespCodec, RespValueZeroCopy};
    use bytes::BytesMut;

    fn test_parse_equivalence(input: &[u8]) {
        let old_result = RespParser::parse(input);
        let mut buf = BytesMut::from(input);
        let new_result = RespCodec::parse(&mut buf);

        match (old_result, new_result) {
            (Ok((old_val, _)), Ok(Some(new_val))) => {
                assert!(values_equivalent(&old_val, &new_val), 
                    "Parsed values differ for input: {:?}", input);
            }
            (Err(_), Ok(None)) | (Err(_), Err(_)) => {}
            (Ok(_), Ok(None)) => panic!("New parser incomplete where old succeeded"),
            (Ok(_), Err(e)) => panic!("New parser error where old succeeded: {}", e),
            (Err(e), Ok(Some(_))) => panic!("Old parser error where new succeeded: {}", e),
        }
    }

    fn values_equivalent(old: &RespValue, new: &RespValueZeroCopy) -> bool {
        match (old, new) {
            (RespValue::SimpleString(s1), RespValueZeroCopy::SimpleString(s2)) => {
                s1.as_bytes() == s2.as_ref()
            }
            (RespValue::Error(s1), RespValueZeroCopy::Error(s2)) => {
                s1.as_bytes() == s2.as_ref()
            }
            (RespValue::Integer(n1), RespValueZeroCopy::Integer(n2)) => n1 == n2,
            (RespValue::BulkString(None), RespValueZeroCopy::BulkString(None)) => true,
            (RespValue::BulkString(Some(d1)), RespValueZeroCopy::BulkString(Some(d2))) => {
                d1.as_slice() == d2.as_ref()
            }
            (RespValue::Array(None), RespValueZeroCopy::Array(None)) => true,
            (RespValue::Array(Some(a1)), RespValueZeroCopy::Array(Some(a2))) => {
                a1.len() == a2.len() && 
                a1.iter().zip(a2.iter()).all(|(v1, v2)| values_equivalent(v1, v2))
            }
            _ => false,
        }
    }

    #[test]
    fn test_simple_string() {
        test_parse_equivalence(b"+OK\r\n");
        test_parse_equivalence(b"+PONG\r\n");
    }

    #[test]
    fn test_error() {
        test_parse_equivalence(b"-ERR unknown command\r\n");
    }

    #[test]
    fn test_integer() {
        test_parse_equivalence(b":0\r\n");
        test_parse_equivalence(b":1000\r\n");
        test_parse_equivalence(b":-1\r\n");
    }

    #[test]
    fn test_bulk_string() {
        test_parse_equivalence(b"$5\r\nhello\r\n");
        test_parse_equivalence(b"$0\r\n\r\n");
        test_parse_equivalence(b"$-1\r\n");
    }

    #[test]
    fn test_array() {
        test_parse_equivalence(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        test_parse_equivalence(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
        test_parse_equivalence(b"*0\r\n");
        test_parse_equivalence(b"*-1\r\n");
    }

    #[test]
    fn test_ping_command() {
        test_parse_equivalence(b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn test_set_get_commands() {
        test_parse_equivalence(b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n");
        test_parse_equivalence(b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");
    }
}

#[cfg(test)]
mod command_parser_tests {
    use super::super::{Command, RespValue, RespValueZeroCopy};
    use bytes::Bytes;

    #[test]
    fn test_zrevrange_without_scores() {
        let old_resp = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"ZREVRANGE".to_vec())),
            RespValue::BulkString(Some(b"myzset".to_vec())),
            RespValue::BulkString(Some(b"0".to_vec())),
            RespValue::BulkString(Some(b"-1".to_vec())),
        ]));
        let new_resp = RespValueZeroCopy::Array(Some(vec![
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"ZREVRANGE"))),
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"myzset"))),
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"0"))),
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"-1"))),
        ]));

        let old_cmd = Command::from_resp(&old_resp).unwrap();
        let new_cmd = Command::from_resp_zero_copy(&new_resp).unwrap();

        match (old_cmd, new_cmd) {
            (Command::ZRevRange(k1, s1, e1, ws1), Command::ZRevRange(k2, s2, e2, ws2)) => {
                assert_eq!(k1, k2);
                assert_eq!(k1, "myzset");
                assert_eq!(s1, s2);
                assert_eq!(s1, 0);
                assert_eq!(e1, e2);
                assert_eq!(e1, -1);
                assert!(!ws1);
                assert!(!ws2);
            }
            _ => panic!("Commands don't match"),
        }
    }

    #[test]
    fn test_zrevrange_with_scores() {
        let old_resp = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"ZREVRANGE".to_vec())),
            RespValue::BulkString(Some(b"leaderboard".to_vec())),
            RespValue::BulkString(Some(b"0".to_vec())),
            RespValue::BulkString(Some(b"9".to_vec())),
            RespValue::BulkString(Some(b"WITHSCORES".to_vec())),
        ]));
        let new_resp = RespValueZeroCopy::Array(Some(vec![
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"ZREVRANGE"))),
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"leaderboard"))),
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"0"))),
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"9"))),
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"WITHSCORES"))),
        ]));

        let old_cmd = Command::from_resp(&old_resp).unwrap();
        let new_cmd = Command::from_resp_zero_copy(&new_resp).unwrap();

        match (old_cmd, new_cmd) {
            (Command::ZRevRange(k1, s1, e1, ws1), Command::ZRevRange(k2, s2, e2, ws2)) => {
                assert_eq!(k1, k2);
                assert_eq!(k1, "leaderboard");
                assert_eq!(s1, 0);
                assert_eq!(e1, 9);
                assert!(ws1);
                assert!(ws2);
            }
            _ => panic!("Commands don't match"),
        }
    }

    #[test]
    fn test_zrevrange_case_insensitive_withscores() {
        // Test lowercase withscores
        let resp = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"ZREVRANGE".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"0".to_vec())),
            RespValue::BulkString(Some(b"-1".to_vec())),
            RespValue::BulkString(Some(b"withscores".to_vec())),
        ]));

        let cmd = Command::from_resp(&resp).unwrap();
        match cmd {
            Command::ZRevRange(_, _, _, with_scores) => {
                assert!(with_scores);
            }
            _ => panic!("Expected ZRevRange"),
        }
    }

    #[test]
    fn test_zrevrange_negative_indices() {
        let resp = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"ZREVRANGE".to_vec())),
            RespValue::BulkString(Some(b"mykey".to_vec())),
            RespValue::BulkString(Some(b"-3".to_vec())),
            RespValue::BulkString(Some(b"-1".to_vec())),
        ]));

        let cmd = Command::from_resp(&resp).unwrap();
        match cmd {
            Command::ZRevRange(key, start, stop, _) => {
                assert_eq!(key, "mykey");
                assert_eq!(start, -3);
                assert_eq!(stop, -1);
            }
            _ => panic!("Expected ZRevRange"),
        }
    }

    #[test]
    fn test_hset_multi_field() {
        let resp = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"HSET".to_vec())),
            RespValue::BulkString(Some(b"myhash".to_vec())),
            RespValue::BulkString(Some(b"field1".to_vec())),
            RespValue::BulkString(Some(b"value1".to_vec())),
            RespValue::BulkString(Some(b"field2".to_vec())),
            RespValue::BulkString(Some(b"value2".to_vec())),
        ]));

        let cmd = Command::from_resp(&resp).unwrap();
        match cmd {
            Command::HSet(key, pairs) => {
                assert_eq!(key, "myhash");
                assert_eq!(pairs.len(), 2);
                assert_eq!(pairs[0].0.to_string(), "field1");
                assert_eq!(pairs[0].1.to_string(), "value1");
                assert_eq!(pairs[1].0.to_string(), "field2");
                assert_eq!(pairs[1].1.to_string(), "value2");
            }
            _ => panic!("Expected HSet"),
        }
    }

    #[test]
    fn test_hincrby_parsing() {
        let resp = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"HINCRBY".to_vec())),
            RespValue::BulkString(Some(b"myhash".to_vec())),
            RespValue::BulkString(Some(b"field1".to_vec())),
            RespValue::BulkString(Some(b"5".to_vec())),
        ]));

        let cmd = Command::from_resp(&resp).unwrap();
        match cmd {
            Command::HIncrBy(key, field, increment) => {
                assert_eq!(key, "myhash");
                assert_eq!(field.to_string(), "field1");
                assert_eq!(increment, 5);
            }
            _ => panic!("Expected HIncrBy"),
        }
    }

    #[test]
    fn test_hincrby_negative() {
        let resp = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"HINCRBY".to_vec())),
            RespValue::BulkString(Some(b"counter".to_vec())),
            RespValue::BulkString(Some(b"hits".to_vec())),
            RespValue::BulkString(Some(b"-3".to_vec())),
        ]));

        let cmd = Command::from_resp(&resp).unwrap();
        match cmd {
            Command::HIncrBy(key, field, increment) => {
                assert_eq!(key, "counter");
                assert_eq!(field.to_string(), "hits");
                assert_eq!(increment, -3);
            }
            _ => panic!("Expected HIncrBy"),
        }
    }

    #[test]
    fn test_hincrby_execution() {
        use super::super::CommandExecutor;
        use super::super::data::SDS;

        let mut executor = CommandExecutor::new();

        // First HINCRBY creates the field with the increment value
        let cmd = Command::HIncrBy("myhash".to_string(), SDS::from_str("counter"), 5);
        let result = executor.execute(&cmd);
        assert_eq!(result, RespValue::Integer(5));

        // Second HINCRBY increments existing value
        let cmd = Command::HIncrBy("myhash".to_string(), SDS::from_str("counter"), 3);
        let result = executor.execute(&cmd);
        assert_eq!(result, RespValue::Integer(8));

        // Negative increment (decrement)
        let cmd = Command::HIncrBy("myhash".to_string(), SDS::from_str("counter"), -2);
        let result = executor.execute(&cmd);
        assert_eq!(result, RespValue::Integer(6));
    }

    #[test]
    fn test_ping_from_both_parsers() {
        let old_resp = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"PING".to_vec()))
        ]));
        let new_resp = RespValueZeroCopy::Array(Some(vec![
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"PING")))
        ]));

        let old_cmd = Command::from_resp(&old_resp).unwrap();
        let new_cmd = Command::from_resp_zero_copy(&new_resp).unwrap();

        assert!(matches!(old_cmd, Command::Ping));
        assert!(matches!(new_cmd, Command::Ping));
    }

    #[test]
    fn test_set_from_both_parsers() {
        let old_resp = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"SET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
            RespValue::BulkString(Some(b"value".to_vec())),
        ]));
        let new_resp = RespValueZeroCopy::Array(Some(vec![
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"SET"))),
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"key"))),
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"value"))),
        ]));

        let old_cmd = Command::from_resp(&old_resp).unwrap();
        let new_cmd = Command::from_resp_zero_copy(&new_resp).unwrap();

        match (old_cmd, new_cmd) {
            (Command::Set(k1, v1), Command::Set(k2, v2)) => {
                assert_eq!(k1, k2);
                assert_eq!(v1.as_bytes(), v2.as_bytes());
            }
            _ => panic!("Commands don't match"),
        }
    }

    #[test]
    fn test_get_from_both_parsers() {
        let old_resp = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"GET".to_vec())),
            RespValue::BulkString(Some(b"mykey".to_vec())),
        ]));
        let new_resp = RespValueZeroCopy::Array(Some(vec![
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"GET"))),
            RespValueZeroCopy::BulkString(Some(Bytes::from_static(b"mykey"))),
        ]));

        let old_cmd = Command::from_resp(&old_resp).unwrap();
        let new_cmd = Command::from_resp_zero_copy(&new_resp).unwrap();

        match (old_cmd, new_cmd) {
            (Command::Get(k1), Command::Get(k2)) => {
                assert_eq!(k1, k2);
            }
            _ => panic!("Commands don't match"),
        }
    }
}
