use redis_sim::redis::{Command, CommandExecutor, RespValue, SDS};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, BufRead, Write};

#[derive(Debug, Deserialize)]
struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Debug, Deserialize)]
struct Body {
    #[serde(rename = "type")]
    msg_type: String,
    msg_id: Option<u64>,
    key: Option<Value>,
    value: Option<Value>,
    from: Option<Value>,
    to: Option<Value>,
    #[allow(dead_code)]
    node_id: Option<String>,
    #[allow(dead_code)]
    node_ids: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
struct Response {
    src: String,
    dest: String,
    body: ResponseBody,
}

#[derive(Debug, Serialize)]
struct ResponseBody {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
    in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    
    let mut executor = CommandExecutor::new();
    let mut msg_counter: u64 = 0;
    
    for line in stdin.lock().lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        
        let msg: Message = match serde_json::from_str(&line) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Failed to parse message: {} - {}", line, e);
                continue;
            }
        };
        
        msg_counter += 1;
        
        let response = match msg.body.msg_type.as_str() {
            "init" => {
                Response {
                    src: msg.dest.clone(),
                    dest: msg.src.clone(),
                    body: ResponseBody {
                        msg_type: "init_ok".to_string(),
                        msg_id: Some(msg_counter),
                        in_reply_to: msg.body.msg_id,
                        value: None,
                        code: None,
                        text: None,
                    },
                }
            }
            "read" => {
                let key = value_to_string(&msg.body.key);
                let cmd = Command::Get(key);
                let result = executor.execute(&cmd);
                
                match result {
                    RespValue::BulkString(Some(data)) => {
                        let value_str = String::from_utf8_lossy(&data);
                        let value: Value = serde_json::from_str(&value_str)
                            .unwrap_or(Value::String(value_str.to_string()));
                        Response {
                            src: msg.dest.clone(),
                            dest: msg.src.clone(),
                            body: ResponseBody {
                                msg_type: "read_ok".to_string(),
                                msg_id: Some(msg_counter),
                                in_reply_to: msg.body.msg_id,
                                value: Some(value),
                                code: None,
                                text: None,
                            },
                        }
                    }
                    RespValue::BulkString(None) => {
                        Response {
                            src: msg.dest.clone(),
                            dest: msg.src.clone(),
                            body: ResponseBody {
                                msg_type: "error".to_string(),
                                msg_id: Some(msg_counter),
                                in_reply_to: msg.body.msg_id,
                                value: None,
                                code: Some(20),
                                text: Some("key does not exist".to_string()),
                            },
                        }
                    }
                    _ => {
                        Response {
                            src: msg.dest.clone(),
                            dest: msg.src.clone(),
                            body: ResponseBody {
                                msg_type: "error".to_string(),
                                msg_id: Some(msg_counter),
                                in_reply_to: msg.body.msg_id,
                                value: None,
                                code: Some(13),
                                text: Some("internal error".to_string()),
                            },
                        }
                    }
                }
            }
            "write" => {
                let key = value_to_string(&msg.body.key);
                let value = value_to_string(&msg.body.value);
                let sds = SDS::from_str(&value);
                let cmd = Command::set(key, sds);
                let _ = executor.execute(&cmd);
                
                Response {
                    src: msg.dest.clone(),
                    dest: msg.src.clone(),
                    body: ResponseBody {
                        msg_type: "write_ok".to_string(),
                        msg_id: Some(msg_counter),
                        in_reply_to: msg.body.msg_id,
                        value: None,
                        code: None,
                        text: None,
                    },
                }
            }
            "cas" => {
                let key = value_to_string(&msg.body.key);
                let from_value = value_to_string(&msg.body.from);
                let to_value = value_to_string(&msg.body.to);
                
                let get_cmd = Command::Get(key.clone());
                let current = executor.execute(&get_cmd);
                
                match current {
                    RespValue::BulkString(Some(data)) => {
                        let current_str = String::from_utf8_lossy(&data);
                        if current_str == from_value {
                            let sds = SDS::from_str(&to_value);
                            let set_cmd = Command::set(key, sds);
                            let _ = executor.execute(&set_cmd);
                            Response {
                                src: msg.dest.clone(),
                                dest: msg.src.clone(),
                                body: ResponseBody {
                                    msg_type: "cas_ok".to_string(),
                                    msg_id: Some(msg_counter),
                                    in_reply_to: msg.body.msg_id,
                                    value: None,
                                    code: None,
                                    text: None,
                                },
                            }
                        } else {
                            Response {
                                src: msg.dest.clone(),
                                dest: msg.src.clone(),
                                body: ResponseBody {
                                    msg_type: "error".to_string(),
                                    msg_id: Some(msg_counter),
                                    in_reply_to: msg.body.msg_id,
                                    value: None,
                                    code: Some(22),
                                    text: Some(format!(
                                        "expected {}, but had {}",
                                        from_value, current_str
                                    )),
                                },
                            }
                        }
                    }
                    RespValue::BulkString(None) => {
                        Response {
                            src: msg.dest.clone(),
                            dest: msg.src.clone(),
                            body: ResponseBody {
                                msg_type: "error".to_string(),
                                msg_id: Some(msg_counter),
                                in_reply_to: msg.body.msg_id,
                                value: None,
                                code: Some(20),
                                text: Some("key does not exist".to_string()),
                            },
                        }
                    }
                    _ => {
                        Response {
                            src: msg.dest.clone(),
                            dest: msg.src.clone(),
                            body: ResponseBody {
                                msg_type: "error".to_string(),
                                msg_id: Some(msg_counter),
                                in_reply_to: msg.body.msg_id,
                                value: None,
                                code: Some(13),
                                text: Some("internal error".to_string()),
                            },
                        }
                    }
                }
            }
            _ => {
                Response {
                    src: msg.dest.clone(),
                    dest: msg.src.clone(),
                    body: ResponseBody {
                        msg_type: "error".to_string(),
                        msg_id: Some(msg_counter),
                        in_reply_to: msg.body.msg_id,
                        value: None,
                        code: Some(10),
                        text: Some(format!("unsupported message type: {}", msg.body.msg_type)),
                    },
                }
            }
        };
        
        let response_str = serde_json::to_string(&response)?;
        writeln!(stdout, "{}", response_str)?;
        stdout.flush()?;
    }
    
    Ok(())
}

fn value_to_string(value: &Option<Value>) -> String {
    match value {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        Some(v) => serde_json::to_string(v).unwrap_or_default(),
        None => String::new(),
    }
}
