use log::error;

use std::{
    collections::VecDeque,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    db::Db,
    proto::{ParseError, Value},
};

pub enum SetBehaviour {
    Force,
    OnlyIfNotExists,
    OnlyIfExists,
}

pub enum ExpireBehaviour {
    Force,
    OnlyIfNoExpiry,
    OnlyIfExpiry,
    OnlyIfGreater,
    OnlyIfLess,
}

pub enum RedisCommand {
    /// https://redis.io/commands/command/ - no arguments
    Command,
    /// https://redis.io/commands/command-docs/ - array of command names
    CommandDocs(Vec<String>),
    /// https://redis.io/commands/config-get/ - array of config parameters
    ConfigGet(Vec<String>),
    /// https://redis.io/commands/get/ - string of key name
    Get(String),
    /// https://redis.io/commands/set/ - set key to value with options
    Set {
        key: String,
        value: Value,
        expiry: Option<Duration>,
        behaviour: SetBehaviour,
        return_old: bool,
        keep_ttl: bool,
    },
    /// https://redis.io/commands/del/ - delete keys
    Del(Vec<String>),
    /// https://redis.io/commands/ttl/ - TTL for key
    Ttl(String),
    /// https://redis.io/commands/pttl/ - TTL in ms for key
    Pttl(String),
    /// https://redis.io/commands/expire/ - set TTL for key
    Expire {
        key: String,
        seconds: u64,
        behaviour: ExpireBehaviour,
    },
    /// https://redis.io/commands/keys/ - get all keys for pattern
    Keys(String),
}

impl RedisCommand {
    pub async fn apply(self, db: &Db) -> Value {
        match self {
            RedisCommand::Command => {
                // This is mainly for redis-cli compatibility
                Value::Array(Vec::new())
            }
            RedisCommand::CommandDocs(_) => {
                // This is mainly for redis-cli compatibility
                Value::Array(Vec::new())
            }
            RedisCommand::ConfigGet(_) => {
                // TODO: This needs to be at least partially supported
                // Mainly for redis-benchmark compatibility
                Value::Array(Vec::new())
            }
            RedisCommand::Get(key) => {
                // Technically GET can only work with strings
                // but who cares for now?
                if let Some(value) = db.get(&key) {
                    value
                } else {
                    Value::NullString
                }
            }
            RedisCommand::Set {
                key,
                value,
                expiry,
                behaviour,
                return_old,
                keep_ttl,
            } => {
                let old = db.set(key, value, expiry, behaviour, keep_ttl).await;

                if return_old {
                    if let Some(value) = old {
                        value
                    } else {
                        Value::NullString
                    }
                } else {
                    if old.is_some() {
                        Value::SimpleString(String::from("OK"))
                    } else {
                        Value::NullString
                    }
                }
            }
            RedisCommand::Del(keys) => {
                let count = db.remove(keys);

                Value::Integer(count as i64)
            }
            RedisCommand::Ttl(key) => {
                let ttl = db.ttl(&key);

                Value::Integer(ttl)
            }
            RedisCommand::Pttl(key) => {
                let pttl = db.pttl(&key);

                Value::Integer(pttl)
            }
            RedisCommand::Expire {
                key,
                seconds,
                behaviour,
            } => todo!(),
            RedisCommand::Keys(_) => todo!(),
        }
    }
}

pub struct CommandParser {
    buffer: VecDeque<Value>,
}

impl CommandParser {
    pub fn new(buffer: Vec<Value>) -> Self {
        Self {
            buffer: VecDeque::from(buffer),
        }
    }

    fn peek(&self) -> Option<&Value> {
        self.buffer.get(0)
    }

    fn skip(&mut self) {
        self.buffer.pop_front();
    }

    fn expect_string(&mut self) -> Result<String, ParseError> {
        match self.buffer.pop_front() {
            Some(Value::BulkString(string)) | Some(Value::SimpleString(string)) => Ok(string),
            _ => Err(ParseError::ExpectedString),
        }
    }

    fn expect_integer(&mut self) -> Result<i64, ParseError> {
        match self.buffer.pop_front() {
            Some(Value::Integer(integer)) => Ok(integer),
            _ => Err(ParseError::ExpectedInteger),
        }
    }

    fn expect_any(&mut self) -> Result<Value, ParseError> {
        match self.buffer.pop_front() {
            Some(value) => Ok(value),
            _ => Err(ParseError::ExpectedAny),
        }
    }

    pub fn parse(mut self) -> Result<RedisCommand, ParseError> {
        let mut command_name = self.expect_string()?;
        command_name.make_ascii_uppercase();

        // Some commands might have a subcommand
        if command_name == "COMMAND" {
            if let Ok(mut subcommand) = self.expect_string() {
                subcommand.make_ascii_uppercase();

                command_name.push(' ');
                command_name.push_str(&subcommand);
            }
        } else if command_name == "CONFIG" {
            let mut subcommand = self.expect_string()?;
            subcommand.make_ascii_uppercase();
            command_name.push(' ');
            command_name.push_str(&subcommand);
        }

        // Now parse the arguments
        match command_name.as_str() {
            "COMMAND" => Ok(RedisCommand::Command),
            "COMMAND DOCS" => {
                let mut command_names = Vec::with_capacity(self.buffer.len());

                while let Ok(command_name) = self.expect_string() {
                    command_names.push(command_name);
                }

                Ok(RedisCommand::CommandDocs(command_names))
            }
            "CONFIG GET" => {
                let mut parameter_globs = Vec::with_capacity(self.buffer.len());

                while let Ok(glob) = self.expect_string() {
                    parameter_globs.push(glob);
                }

                Ok(RedisCommand::ConfigGet(parameter_globs))
            }
            "GET" => {
                let key = self.expect_string()?;

                Ok(RedisCommand::Get(key))
            }
            "SET" => {
                let key = self.expect_string()?;
                let value = self.expect_any()?;

                let behaviour = match self.peek().and_then(Value::try_as_string).as_deref() {
                    Some("NX") => {
                        self.skip();
                        SetBehaviour::OnlyIfNotExists
                    }
                    Some("XX") => {
                        self.skip();
                        SetBehaviour::OnlyIfExists
                    }
                    _ => SetBehaviour::Force,
                };

                let return_old = if matches!(
                    self.peek().and_then(Value::try_as_string).as_deref(),
                    Some("GET")
                ) {
                    self.skip();
                    true
                } else {
                    false
                };

                let (expiry, keep_ttl) = match self.peek().and_then(Value::try_as_string).as_deref()
                {
                    Some("EX") => {
                        println!("{:?}", self.buffer);
                        self.skip();
                        let seconds = self.expect_integer()?;
                        let duration = Duration::from_secs(seconds as u64);

                        (Some(duration), false)
                    }
                    Some("PX") => {
                        self.skip();
                        let millis = self.expect_integer()?;
                        let duration = Duration::from_millis(millis as u64);

                        (Some(duration), false)
                    }
                    Some("EXAT") => {
                        self.skip();
                        let seconds = self.expect_integer()?;
                        let since_unix = Duration::from_secs(seconds as u64);
                        let system_time = UNIX_EPOCH + since_unix;
                        let duration = system_time.duration_since(SystemTime::now()).ok();

                        (duration, false)
                    }
                    Some("PXAT") => {
                        self.skip();
                        let millis = self.expect_integer()?;
                        let since_unix = Duration::from_millis(millis as u64);
                        let system_time = UNIX_EPOCH + since_unix;
                        let duration = system_time.duration_since(SystemTime::now()).ok();

                        (duration, false)
                    }
                    Some("KEEPTTL") => {
                        self.skip();

                        (None, true)
                    }
                    _ => (None, false),
                };

                Ok(RedisCommand::Set {
                    key,
                    value,
                    expiry,
                    behaviour,
                    return_old,
                    keep_ttl,
                })
            }
            "DEL" => {
                let mut keys = Vec::with_capacity(self.buffer.len());

                while let Ok(key) = self.expect_string() {
                    keys.push(key);
                }

                Ok(RedisCommand::Del(keys))
            }
            "TTL" => {
                let key = self.expect_string()?;

                Ok(RedisCommand::Ttl(key))
            }
            "PTTL" => {
                let key = self.expect_string()?;

                Ok(RedisCommand::Pttl(key))
            }
            "EXPIRE" => {
                let key = self.expect_string()?;
                let seconds = self.expect_integer()? as u64;

                let behaviour = match self.peek().and_then(Value::try_as_string).as_deref() {
                    Some("NX") => {
                        self.skip();
                        ExpireBehaviour::OnlyIfNoExpiry
                    }
                    Some("XX") => {
                        self.skip();
                        ExpireBehaviour::OnlyIfExpiry
                    }
                    Some("GT") => {
                        self.skip();
                        ExpireBehaviour::OnlyIfGreater
                    }
                    Some("LT") => {
                        self.skip();
                        ExpireBehaviour::OnlyIfLess
                    }
                    _ => ExpireBehaviour::Force,
                };

                Ok(RedisCommand::Expire {
                    key,
                    seconds,
                    behaviour,
                })
            }
            "KEYS" => {
                let glob = self.expect_string()?;

                Ok(RedisCommand::Keys(glob))
            }
            cmd => {
                error!("Unimplemented command: {cmd}");
                unimplemented!()
            }
        }
    }
}
