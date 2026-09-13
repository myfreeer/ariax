//! Preflight owned JSON layout from borrowed Serde views before materialization.

use serde::Serialize;
use serde::ser::{self, SerializeMap, SerializeSeq, SerializeStruct};
use serde_json::Value;
use std::fmt;

const NODE_BYTES: usize = 256;
// The envelope, a maximum-sized request id, and one bounded status row coexist with results.
pub(crate) const RESULT_VALUE_BYTES: usize =
    crate::rpc_budget::RPC_RESULT_WORKSPACE_BYTES - crate::MAX_HTTP_RPC_REQUEST_BYTES - 64 * 1024;

pub(crate) struct DisplayValue<T>(pub(crate) T);

impl<T: fmt::Display> Serialize for DisplayValue<T> {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

pub(crate) struct SourceUris<'a> {
    pub(crate) sources: &'a [crate::HttpSourceSpec],
    pub(crate) status: bool,
}

impl Serialize for SourceUris<'_> {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Uri<'a> {
            uri: &'a str,
            status: &'static str,
        }
        let mut sequence = serializer.serialize_seq(Some(self.sources.len()))?;
        for source in self.sources {
            if self.status {
                sequence.serialize_element(&Uri {
                    uri: source.uri().or(source.persistence_safe_uri()).unwrap_or(""),
                    status: if source.uri().is_some() {
                        "used"
                    } else {
                        "waiting"
                    },
                })?;
            } else {
                sequence.serialize_element(&source.uri().unwrap_or(""))?;
            }
        }
        sequence.end()
    }
}

pub(crate) struct SourceServers<'a>(pub(crate) &'a [crate::HttpSourceSpec]);

pub(crate) struct PersistedUris<'a>(pub(crate) &'a [crate::HttpSourceSpec]);

impl Serialize for PersistedUris<'_> {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(
            self.0
                .iter()
                .filter_map(crate::HttpSourceSpec::persistence_safe_uri),
        )
    }
}

pub(crate) struct PersistedSources<'a>(pub(crate) &'a [crate::HttpSourceSpec]);

struct SourceFingerprint<'a>(&'a [u8; 32]);

impl fmt::Display for SourceFingerprint<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Serialize for PersistedSources<'_> {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Source<'a> {
            uri_id: DisplayValue<u32>,
            uri: Option<&'a str>,
            fingerprint: DisplayValue<SourceFingerprint<'a>>,
            needs_credentials: bool,
            priority: DisplayValue<i64>,
        }
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for source in self.0 {
            sequence.serialize_element(&Source {
                uri_id: DisplayValue(source.id().get()),
                uri: source.persistence_safe_uri(),
                fingerprint: DisplayValue(SourceFingerprint(source.redacted_fingerprint())),
                needs_credentials: source.needs_credentials(),
                priority: DisplayValue(source.priority()),
            })?;
        }
        sequence.end()
    }
}

impl Serialize for SourceServers<'_> {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Server<'a> {
            uri: &'a str,
            current_uri: &'a str,
            download_speed: &'static str,
        }
        #[derive(Serialize)]
        struct Row<'a> {
            index: DisplayValue<u64>,
            servers: [Server<'a>; 1],
        }
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for source in self.0 {
            sequence.serialize_element(&Row {
                index: DisplayValue(u64::from(source.id().get()) + 1),
                servers: [Server {
                    uri: source.uri().or(source.persistence_safe_uri()).unwrap_or(""),
                    current_uri: source.uri().unwrap_or(""),
                    download_speed: "0",
                }],
            })?;
        }
        sequence.end()
    }
}

pub(crate) struct OptionMap<'a>(pub(crate) &'a ariax_storage::SanitizedOptionMap);

impl Serialize for OptionMap<'_> {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.0.entries())
    }
}

pub(crate) struct StringMap<I>(pub(crate) I);

impl<'a, I> Serialize for StringMap<I>
where
    I: Clone + Iterator<Item = (&'a str, &'a str)>,
{
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.0.clone())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResultTooLarge;

impl fmt::Display for ResultTooLarge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RPC result exceeds the materialization budget")
    }
}

impl std::error::Error for ResultTooLarge {}

impl ser::Error for ResultTooLarge {
    fn custom<T: fmt::Display>(_message: T) -> Self {
        Self
    }
}

pub(crate) fn measure(value: &impl Serialize, limit: usize) -> Result<usize, ResultTooLarge> {
    let mut meter = Meter { used: 0, limit };
    value.serialize(&mut meter)?;
    Ok(meter.used)
}

pub(crate) fn to_value(value: &impl Serialize, limit: usize) -> Result<Value, ResultTooLarge> {
    measure(value, limit)?;
    serde_json::to_value(value).map_err(|_| ResultTooLarge)
}

pub(crate) struct ResultList {
    values: Vec<Value>,
    used: usize,
    limit: usize,
}

impl ResultList {
    pub(crate) fn new() -> Self {
        Self {
            values: Vec::new(),
            used: NODE_BYTES,
            limit: RESULT_VALUE_BYTES,
        }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.limit.saturating_sub(self.used)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub(crate) fn push(&mut self, value: &impl Serialize) -> Result<(), ResultTooLarge> {
        let bytes = measure(value, self.remaining())?;
        let value = serde_json::to_value(value).map_err(|_| ResultTooLarge)?;
        self.used += bytes;
        self.values.push(value);
        Ok(())
    }

    pub(crate) fn push_scratch(&mut self, value: Value) -> Result<(), ResultTooLarge> {
        let bytes = crate::rpc_json::owned_value_bytes(&value);
        if bytes > self.remaining() {
            return Err(ResultTooLarge);
        }
        self.used += bytes;
        self.values.push(value);
        Ok(())
    }

    pub(crate) fn finish(self) -> Value {
        Value::Array(self.values)
    }
}

struct Meter {
    used: usize,
    limit: usize,
}

impl Meter {
    fn add(&mut self, bytes: usize) -> Result<(), ResultTooLarge> {
        self.used = self
            .used
            .checked_add(bytes)
            .filter(|used| *used <= self.limit)
            .ok_or(ResultTooLarge)?;
        Ok(())
    }

    fn container(&mut self, length: Option<usize>) -> Result<&mut Self, ResultTooLarge> {
        self.add(NODE_BYTES)?;
        if length.is_some_and(|length| length > (self.limit - self.used) / NODE_BYTES) {
            return Err(ResultTooLarge);
        }
        Ok(self)
    }
}

macro_rules! scalar_methods {
    ($($name:ident: $ty:ty),* $(,)?) => { $(
        fn $name(self, _value: $ty) -> Result<(), ResultTooLarge> { self.add(NODE_BYTES) }
    )* };
}

impl ser::Serializer for &mut Meter {
    type Ok = ();
    type Error = ResultTooLarge;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    scalar_methods! {
        serialize_bool: bool, serialize_i8: i8, serialize_i16: i16,
        serialize_i32: i32, serialize_i64: i64, serialize_i128: i128,
        serialize_u8: u8, serialize_u16: u16, serialize_u32: u32,
        serialize_u64: u64, serialize_u128: u128, serialize_f32: f32,
        serialize_f64: f64, serialize_char: char,
    }

    fn serialize_str(self, value: &str) -> Result<(), ResultTooLarge> {
        self.add(NODE_BYTES.saturating_add(value.len().saturating_mul(2)))
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<(), ResultTooLarge> {
        self.add(NODE_BYTES.saturating_add(value.len().saturating_mul(NODE_BYTES)))
    }

    fn serialize_none(self) -> Result<(), ResultTooLarge> {
        self.serialize_unit()
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<(), ResultTooLarge> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<(), ResultTooLarge> {
        self.add(NODE_BYTES)
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), ResultTooLarge> {
        self.serialize_unit()
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<(), ResultTooLarge> {
        self.serialize_str(variant)
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), ResultTooLarge> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<(), ResultTooLarge> {
        self.container(Some(1))?;
        self.serialize_str(variant)?;
        value.serialize(self)
    }
    fn serialize_seq(self, length: Option<usize>) -> Result<Self::SerializeSeq, ResultTooLarge> {
        self.container(length)
    }
    fn serialize_tuple(self, length: usize) -> Result<Self::SerializeTuple, ResultTooLarge> {
        self.container(Some(length))
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        length: usize,
    ) -> Result<Self::SerializeTupleStruct, ResultTooLarge> {
        self.container(Some(length))
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        length: usize,
    ) -> Result<Self::SerializeTupleVariant, ResultTooLarge> {
        self.add(NODE_BYTES)?;
        self.serialize_str(variant)?;
        self.container(Some(length))
    }
    fn serialize_map(self, length: Option<usize>) -> Result<Self::SerializeMap, ResultTooLarge> {
        self.container(length)
    }
    fn serialize_struct(
        self,
        _name: &'static str,
        length: usize,
    ) -> Result<Self::SerializeStruct, ResultTooLarge> {
        self.container(Some(length))
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        length: usize,
    ) -> Result<Self::SerializeStructVariant, ResultTooLarge> {
        self.add(NODE_BYTES)?;
        self.serialize_str(variant)?;
        self.container(Some(length))
    }
    fn collect_str<T: ?Sized + fmt::Display>(self, value: &T) -> Result<(), ResultTooLarge> {
        self.add(NODE_BYTES)?;
        struct TextMeter<'a>(&'a mut Meter);
        impl fmt::Write for TextMeter<'_> {
            fn write_str(&mut self, value: &str) -> fmt::Result {
                self.0
                    .add(value.len().saturating_mul(2))
                    .map_err(|_| fmt::Error)
            }
        }
        fmt::write(&mut TextMeter(self), format_args!("{value}")).map_err(|_| ResultTooLarge)
    }
}

impl SerializeSeq for &mut Meter {
    type Ok = ();
    type Error = ResultTooLarge;
    fn serialize_element<T: ?Sized + Serialize>(
        &mut self,
        value: &T,
    ) -> Result<(), ResultTooLarge> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), ResultTooLarge> {
        Ok(())
    }
}

macro_rules! tuple_impl {
    ($trait:ident, $element:ident) => {
        impl ser::$trait for &mut Meter {
            type Ok = ();
            type Error = ResultTooLarge;
            fn $element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), ResultTooLarge> {
                value.serialize(&mut **self)
            }
            fn end(self) -> Result<(), ResultTooLarge> {
                Ok(())
            }
        }
    };
}
tuple_impl!(SerializeTuple, serialize_element);
tuple_impl!(SerializeTupleStruct, serialize_field);
tuple_impl!(SerializeTupleVariant, serialize_field);

impl SerializeMap for &mut Meter {
    type Ok = ();
    type Error = ResultTooLarge;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), ResultTooLarge> {
        key.serialize(&mut **self)
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), ResultTooLarge> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), ResultTooLarge> {
        Ok(())
    }
}

impl SerializeStruct for &mut Meter {
    type Ok = ();
    type Error = ResultTooLarge;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), ResultTooLarge> {
        key.serialize(&mut **self)?;
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), ResultTooLarge> {
        Ok(())
    }
}

impl ser::SerializeStructVariant for &mut Meter {
    type Ok = ();
    type Error = ResultTooLarge;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), ResultTooLarge> {
        SerializeStruct::serialize_field(self, key, value)
    }
    fn end(self) -> Result<(), ResultTooLarge> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn measured_borrowed_results_match_json_and_bound_owned_layout() {
        #[derive(Serialize)]
        struct ResultView<'a> {
            uri: &'a str,
            values: &'a [u64],
            optional: Option<bool>,
        }
        let view = ResultView {
            uri: "https://example.test/file",
            values: &[1, 2, 3],
            optional: Some(true),
        };
        let bytes = measure(&view, RESULT_VALUE_BYTES).expect("measure");
        let value = to_value(&view, bytes).expect("exact budget");
        assert_eq!(value, serde_json::to_value(&view).expect("reference"));
        assert!(crate::rpc_json::owned_value_bytes(&value) <= bytes);
        assert!(to_value(&view, bytes - 1).is_err());
    }

    #[test]
    fn excessive_sequence_rejects_before_materialization_pass() {
        struct Large<'a>(&'a Cell<usize>);
        impl Serialize for Large<'_> {
            fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.0.set(self.0.get() + 1);
                let mut sequence = serializer.serialize_seq(Some(usize::MAX))?;
                sequence.serialize_element(&"never allocated")?;
                sequence.end()
            }
        }
        let passes = Cell::new(0);
        assert!(to_value(&Large(&passes), 1024).is_err());
        assert_eq!(passes.get(), 1);
        let mut list = ResultList {
            values: Vec::new(),
            used: NODE_BYTES,
            limit: 1024,
        };
        assert!(list.push(&"short").is_ok());
        assert!(list.push(&"x".repeat(1024)).is_err());
        assert_eq!(list.finish(), serde_json::json!(["short"]));
    }

    #[test]
    fn measured_layout_dominates_materialized_json_across_nested_shapes() {
        for width in [0, 1, 2, 7, 16, 64] {
            for string_bytes in [0, 1, 31, 256, 4096] {
                let mut value = Value::String("x".repeat(string_bytes));
                for depth in 0..4 {
                    value = if depth % 2 == 0 {
                        Value::Array(vec![value; width])
                    } else {
                        serde_json::json!({"nested": value, "bool": true, "number": u64::MAX, "optional": null})
                    };
                    let projected = measure(&value, RESULT_VALUE_BYTES);
                    if let Ok(projected) = projected {
                        assert!(crate::rpc_json::owned_value_bytes(&value) <= projected);
                        assert_eq!(
                            to_value(&value, projected).expect("exact measured allowance"),
                            value
                        );
                        assert!(to_value(&value, projected - 1).is_err());
                    }
                }
            }
        }
    }
}
