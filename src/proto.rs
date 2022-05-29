/// A tokio-util based implementation of the RESP protocol.
///
/// TODO:
/// - UTF8 validation (with SIMD)
use bytes::{Buf, BufMut, BytesMut};
use memchr::memchr_iter;
use tokio_util::codec::{Decoder, Encoder};

use std::io;

#[derive(Clone, Debug)]
pub enum Value {
    SimpleString(String),
    Error(RedisError),
    Integer(i64),
    BulkString(String),
    Array(Vec<Value>),
    NullArray,
    NullString,
}

impl Value {
    pub fn try_as_string(&self) -> Option<String> {
        match self {
            Self::SimpleString(string) | Self::BulkString(string) => {
                Some(string.as_str().to_ascii_uppercase())
            }
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RedisError {
    pub message: String,
}

#[derive(Debug)]
pub enum ProtocolError {
    UnknownType,
    NotAnInteger,
    ExpectedCrlf,
}

#[derive(Debug)]
pub enum ParseError {
    ExpectedString,
    ExpectedInteger,
    ExpectedAny,
}

#[derive(Debug)]
pub enum Error {
    ProtocolError(ProtocolError),
    Io(io::Error),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

/// Find the next CRLF start byte.
fn find_next_crlf(input: &[u8]) -> Option<usize> {
    for cr in memchr_iter(b'\r', input) {
        if matches!(input.get(cr + 1), Some(b'\n')) {
            return Some(cr);
        }
    }

    None
}

enum OptionalWithMissingHint<T> {
    Some(T),
    Missing(usize),
    NoClue,
}

struct ParsedValue {
    value: Value,
    offset: usize,
}

impl Value {
    fn parse(src: &[u8]) -> Result<OptionalWithMissingHint<ParsedValue>, Error> {
        if src.len() < 1 {
            return Ok(OptionalWithMissingHint::Missing(1));
        }

        match unsafe { src.get_unchecked(0) } {
            b'+' => {
                // Simple string is terminated by CRLF
                match find_next_crlf(unsafe { src.get_unchecked(1..) }) {
                    Some(crlf_start) => {
                        let bytes = unsafe { src.get_unchecked(1..crlf_start + 1) }.to_vec();
                        let string = unsafe { String::from_utf8_unchecked(bytes) };

                        let value = Value::SimpleString(string);
                        let offset = crlf_start + 3;

                        Ok(OptionalWithMissingHint::Some(ParsedValue { value, offset }))
                    }
                    None => Ok(OptionalWithMissingHint::NoClue),
                }
            }
            b'-' => {
                // Error is terminated by CRLF
                match find_next_crlf(unsafe { src.get_unchecked(1..) }) {
                    Some(crlf_start) => {
                        let bytes = unsafe { src.get_unchecked(1..crlf_start + 1) }.to_vec();
                        let string = unsafe { String::from_utf8_unchecked(bytes) };

                        let value = Value::Error(RedisError { message: string });
                        let offset = crlf_start + 3;

                        Ok(OptionalWithMissingHint::Some(ParsedValue { value, offset }))
                    }
                    None => Ok(OptionalWithMissingHint::NoClue),
                }
            }
            b':' => {
                // Integers are terminated by CRLF
                match find_next_crlf(unsafe { src.get_unchecked(1..) }) {
                    Some(crlf_start) => {
                        let bytes = unsafe { src.get_unchecked(1..crlf_start + 1) };
                        let integer = atoi::atoi(bytes)
                            .ok_or(Error::ProtocolError(ProtocolError::NotAnInteger))?;

                        let value = Value::Integer(integer);
                        let offset = crlf_start + 3;

                        Ok(OptionalWithMissingHint::Some(ParsedValue { value, offset }))
                    }
                    None => Ok(OptionalWithMissingHint::NoClue),
                }
            }
            b'$' => {
                let mut offset;

                let length: i64 = match find_next_crlf(unsafe { src.get_unchecked(1..) }) {
                    Some(crlf_start) => {
                        let bytes = unsafe { src.get_unchecked(1..crlf_start + 1) };
                        offset = crlf_start + 3;
                        let integer = atoi::atoi(bytes)
                            .ok_or(Error::ProtocolError(ProtocolError::NotAnInteger))?;

                        integer
                    }
                    None => return Ok(OptionalWithMissingHint::NoClue),
                };

                if length != -1 {
                    let length = length as usize;
                    let rest = unsafe { src.get_unchecked(offset..) };

                    // Needs to have a CRLF
                    if rest.len() < length + 2 {
                        return Ok(OptionalWithMissingHint::Missing(length + 2 - rest.len()));
                    }

                    if unsafe { rest.get_unchecked(length..length + 2) } != b"\r\n" {
                        return Err(Error::ProtocolError(ProtocolError::ExpectedCrlf));
                    }

                    let bytes = unsafe { rest.get_unchecked(..length) }.to_vec();
                    let string = unsafe { String::from_utf8_unchecked(bytes) };

                    offset += length;
                    offset += 2;

                    let value = Value::BulkString(string);

                    Ok(OptionalWithMissingHint::Some(ParsedValue { value, offset }))
                } else {
                    let value = Value::NullString;

                    Ok(OptionalWithMissingHint::Some(ParsedValue { value, offset }))
                }
            }
            b'*' => {
                let mut offset;

                let length: i64 = match find_next_crlf(unsafe { src.get_unchecked(1..) }) {
                    Some(crlf_start) => {
                        let bytes = unsafe { src.get_unchecked(1..crlf_start + 1) };
                        offset = crlf_start + 3;
                        let integer = atoi::atoi(bytes)
                            .ok_or(Error::ProtocolError(ProtocolError::NotAnInteger))?;

                        integer
                    }
                    None => return Ok(OptionalWithMissingHint::NoClue),
                };

                if length != -1 {
                    let length = length as usize;

                    let mut items = Vec::with_capacity(length);

                    for _ in 0..length {
                        match Value::parse(unsafe { src.get_unchecked(offset..) })? {
                            OptionalWithMissingHint::Some(value) => {
                                offset += value.offset;
                                items.push(value.value);
                            }
                            other => return Ok(other),
                        };
                    }

                    let value = Value::Array(items);

                    Ok(OptionalWithMissingHint::Some(ParsedValue { offset, value }))
                } else {
                    let value = Value::NullArray;

                    Ok(OptionalWithMissingHint::Some(ParsedValue { value, offset }))
                }
            }
            _ => Err(Error::ProtocolError(ProtocolError::UnknownType)),
        }
    }
}

pub struct RedisProtocol;

impl Decoder for RedisProtocol {
    type Item = Value;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match Value::parse(src) {
            Ok(OptionalWithMissingHint::Some(ParsedValue { value, offset })) => {
                src.advance(offset);

                Ok(Some(value))
            }
            Ok(OptionalWithMissingHint::Missing(amount)) => {
                src.reserve(amount);

                Ok(None)
            }
            Ok(OptionalWithMissingHint::NoClue) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl Encoder<Value> for RedisProtocol {
    type Error = Error;

    fn encode(&mut self, item: Value, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Value::SimpleString(string) => {
                dst.reserve(string.len() + 3);
                dst.put_u8(b'+');
                dst.extend_from_slice(string.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Value::Error(RedisError { message }) => {
                dst.reserve(message.len() + 3);
                dst.put_u8(b'+');
                dst.extend_from_slice(message.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Value::Integer(integer) => {
                let mut buffer = itoa::Buffer::new();
                let printed = buffer.format(integer);
                dst.reserve(printed.len() + 3);
                dst.put_u8(b':');
                dst.extend_from_slice(printed.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Value::BulkString(string) => {
                let mut buffer = itoa::Buffer::new();
                let printed = buffer.format(string.len());
                dst.reserve(printed.len() + string.len() + 5);
                dst.put_u8(b'$');
                dst.extend_from_slice(printed.as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(string.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Value::Array(array) => {
                let mut buffer = itoa::Buffer::new();
                let printed = buffer.format(array.len());
                dst.reserve(printed.len() + 3);
                dst.put_u8(b'*');
                dst.extend_from_slice(printed.as_bytes());
                dst.extend_from_slice(b"\r\n");

                for value in array {
                    self.encode(value, dst)?;
                }
            }
            Value::NullString => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            Value::NullArray => {
                dst.extend_from_slice(b"*-1\r\n");
            }
        }

        Ok(())
    }
}

#[test]
fn decode_works() {
    use bytes::BufMut;

    let test_data: &[&[u8]] = &[
        b"+OK\r\n",
        b"-Error message\r\n",
        b"-ERR unknown command 'helloworld'\r\n",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
        b":0\r\n",
        b":1000\r\n",
        b"$5\r\nhello\r\n",
        b"$0\r\n\r\n",
        b"$-1\r\n",
        b"*0\r\n",
        b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
        b"*3\r\n:1\r\n:2\r\n:3\r\n",
        b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n", // TODO: Bulk string is incorrectly not being parsed, everything is None
        b"*-1\r\n",
        b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n",
        b"*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n",
        b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n",
    ];

    for data in test_data {
        let mut input = BytesMut::new();
        input.put_slice(data);

        assert!(matches!(RedisProtocol {}.decode(&mut input), Ok(Some(_))));
    }
}
