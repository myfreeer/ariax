//! Serde JSON construction with reservations before owned container allocation.

use crate::rpc_budget::{RpcBudgetError, RpcRequestLease};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::cell::Cell;
use std::fmt;

const NODE_BYTES: usize = 256;
const MAX_DEPTH: usize = 128;

pub(crate) fn owned_value_bytes(value: &Value) -> usize {
    NODE_BYTES.saturating_add(match value {
        Value::String(value) => value.capacity(),
        Value::Array(values) => values
            .iter()
            .map(owned_value_bytes)
            .fold(0, usize::saturating_add),
        Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                NODE_BYTES
                    .saturating_add(key.capacity())
                    .saturating_add(owned_value_bytes(value))
            })
            .fold(0, usize::saturating_add),
        _ => 0,
    })
}

pub(crate) fn command_value_bytes(value: &Value) -> usize {
    1024_usize.saturating_add(match value {
        Value::String(text) => text.len().saturating_mul(12),
        Value::Array(values) => values
            .iter()
            .map(command_value_bytes)
            .fold(0, usize::saturating_add),
        Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                1024_usize
                    .saturating_add(key.len().saturating_mul(12))
                    .saturating_add(command_value_bytes(value))
            })
            .fold(0, usize::saturating_add),
        _ => 0,
    })
}

#[derive(Debug)]
pub(crate) enum RpcJsonError {
    Parse,
    Budget(RpcBudgetError),
}

pub(crate) fn parse(bytes: &[u8], lease: &RpcRequestLease) -> Result<Value, RpcJsonError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let exhausted = Cell::new(None);
    let value = Seed {
        lease,
        depth: 0,
        exhausted: &exhausted,
    }
    .deserialize(&mut deserializer)
    .map_err(|_| {
        exhausted
            .get()
            .map_or(RpcJsonError::Parse, RpcJsonError::Budget)
    })?;
    deserializer.end().map_err(|_| RpcJsonError::Parse)?;
    Ok(value)
}

struct Seed<'a> {
    lease: &'a RpcRequestLease,
    depth: usize,
    exhausted: &'a Cell<Option<RpcBudgetError>>,
}

impl Seed<'_> {
    fn reserve<E: de::Error>(&self, bytes: usize) -> Result<(), E> {
        self.lease.reserve(bytes).map_err(|error| {
            self.exhausted.set(Some(error));
            E::custom(error)
        })
    }

    fn child<E: de::Error>(&self) -> Result<Seed<'_>, E> {
        if self.depth == MAX_DEPTH {
            return Err(E::custom("RPC JSON nesting limit exceeded"));
        }
        Ok(Seed {
            lease: self.lease,
            depth: self.depth + 1,
            exhausted: self.exhausted,
        })
    }
}

impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = Value;

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        self.reserve(NODE_BYTES)?;
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Seed<'_> {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }
    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }
    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Value, E> {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("invalid JSON number"))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Value, E> {
        self.reserve(value.len().saturating_mul(2))?;
        Ok(Value::String(value.to_owned()))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(self.child()?)? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut values = Map::new();
        while let Some(Value::String(key)) = map.next_key_seed(KeySeed(self.child()?))? {
            let value = map.next_value_seed(self.child()?)?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

struct KeySeed<'a>(Seed<'a>);

impl<'de> DeserializeSeed<'de> for KeySeed<'_> {
    type Value = Value;

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        self.0.reserve(NODE_BYTES)?;
        deserializer.deserialize_str(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RpcBudgets;

    #[test]
    fn bounded_parser_matches_json_and_rejects_expansion_before_backend_admission() {
        let process = RpcBudgets::process_default();
        let client = process.client().expect("client");
        for bytes in [
            br#"{"jsonrpc":"2.0","params":[null,true,-1,2,1.5,"a\u1234"],"id":1}"#.as_slice(),
            br#"{"duplicate":1,"duplicate":2}"#,
        ] {
            let request = client.try_request(bytes.len()).expect("request");
            assert_eq!(
                parse(bytes, &request).expect("bounded value"),
                serde_json::from_slice::<Value>(bytes).expect("reference")
            );
        }
        let bytes = format!("[{}null]", "null,".repeat(40_000));
        let request = client.try_request(bytes.len()).expect("small input");
        assert!(parse(bytes.as_bytes(), &request).is_err());
        assert!(client.request_bytes() <= crate::rpc_budget::MAX_RPC_CLIENT_REQUEST_BYTES);
        drop(request);
        assert_eq!(client.request_bytes(), 0);
        let request = client.try_request(10).expect("later request");
        assert!(parse(b"{\"bad\":}", &request).is_err());
        drop(request);
        assert_eq!(client.request_bytes(), 0);
    }
}
