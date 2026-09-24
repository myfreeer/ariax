use crate::{BtError, MetadataLimits};
use std::ops::Range;

#[derive(Debug)]
pub(crate) struct Node<'a> {
    pub range: Range<usize>,
    pub value: Value<'a>,
}

#[derive(Debug)]
pub(crate) enum Value<'a> {
    Integer(i64),
    Bytes(&'a [u8]),
    List(Vec<Node<'a>>),
    Dictionary(Vec<(&'a [u8], Node<'a>)>),
}

impl<'a> Node<'a> {
    pub fn get(&self, key: &[u8]) -> Option<&Node<'a>> {
        let Value::Dictionary(values) = &self.value else {
            return None;
        };
        values
            .binary_search_by_key(&key, |(name, _)| *name)
            .ok()
            .map(|index| &values[index].1)
    }

    pub fn required(&self, key: &[u8]) -> Result<&Node<'a>, BtError> {
        self.get(key).ok_or(BtError::InvalidMetadata)
    }

    pub fn bytes(&self) -> Result<&'a [u8], BtError> {
        match self.value {
            Value::Bytes(value) => Ok(value),
            _ => Err(BtError::InvalidMetadata),
        }
    }

    pub fn text(&self) -> Result<&'a str, BtError> {
        std::str::from_utf8(self.bytes()?).map_err(|_| BtError::InvalidMetadata)
    }

    pub fn integer(&self) -> Result<u64, BtError> {
        match self.value {
            Value::Integer(value) => u64::try_from(value).map_err(|_| BtError::InvalidMetadata),
            _ => Err(BtError::InvalidMetadata),
        }
    }

    pub fn list(&self) -> Result<&[Node<'a>], BtError> {
        match &self.value {
            Value::List(value) => Ok(value),
            _ => Err(BtError::InvalidMetadata),
        }
    }

    pub fn dictionary(&self) -> Result<&[(&'a [u8], Node<'a>)], BtError> {
        match &self.value {
            Value::Dictionary(value) => Ok(value),
            _ => Err(BtError::InvalidMetadata),
        }
    }
}

struct Parser<'a> {
    bytes: &'a [u8],
    offset: usize,
    tokens: usize,
    limits: MetadataLimits,
}

impl<'a> Parser<'a> {
    fn byte(&self) -> Result<u8, BtError> {
        self.bytes
            .get(self.offset)
            .copied()
            .ok_or(BtError::InvalidMetadata)
    }

    fn token(&mut self) -> Result<(), BtError> {
        self.tokens = self.tokens.checked_add(1).ok_or(BtError::MetadataLimit)?;
        if self.tokens > self.limits.tokens {
            return Err(BtError::MetadataLimit);
        }
        Ok(())
    }

    fn string(&mut self) -> Result<&'a [u8], BtError> {
        self.token()?;
        let start = self.offset;
        let mut length = 0usize;
        while self.byte()?.is_ascii_digit() {
            if self.offset > start && self.bytes[start] == b'0' {
                return Err(BtError::InvalidMetadata);
            }
            length = length
                .checked_mul(10)
                .and_then(|value| value.checked_add(usize::from(self.bytes[self.offset] - b'0')))
                .ok_or(BtError::MetadataLimit)?;
            if length > self.limits.bytes {
                return Err(BtError::MetadataLimit);
            }
            self.offset += 1;
        }
        if self.offset == start || self.byte()? != b':' {
            return Err(BtError::InvalidMetadata);
        }
        self.offset += 1;
        let end = self
            .offset
            .checked_add(length)
            .ok_or(BtError::MetadataLimit)?;
        let result = self
            .bytes
            .get(self.offset..end)
            .ok_or(BtError::InvalidMetadata)?;
        self.offset = end;
        Ok(result)
    }

    fn node(&mut self, depth: usize) -> Result<Node<'a>, BtError> {
        if depth > self.limits.depth {
            return Err(BtError::MetadataLimit);
        }
        let start = self.offset;
        let value = match self.byte()? {
            b'0'..=b'9' => Value::Bytes(self.string()?),
            b'i' => {
                self.token()?;
                self.offset += 1;
                let number_start = self.offset;
                if self.byte()? == b'-' {
                    self.offset += 1;
                }
                let digits = self.offset;
                while self.byte()?.is_ascii_digit() {
                    self.offset += 1;
                }
                if self.offset == digits
                    || self.byte()? != b'e'
                    || self.offset - digits > 1 && self.bytes[digits] == b'0'
                    || number_start != digits && self.bytes[digits] == b'0'
                {
                    return Err(BtError::InvalidMetadata);
                }
                let integer = std::str::from_utf8(&self.bytes[number_start..self.offset])
                    .map_err(|_| BtError::InvalidMetadata)?
                    .parse()
                    .map_err(|_| BtError::InvalidMetadata)?;
                self.offset += 1;
                Value::Integer(integer)
            }
            b'l' => {
                self.token()?;
                self.offset += 1;
                let mut values = Vec::new();
                while self.byte()? != b'e' {
                    values.push(self.node(depth + 1)?);
                }
                self.offset += 1;
                Value::List(values)
            }
            b'd' => {
                self.token()?;
                self.offset += 1;
                let mut values: Vec<(&[u8], Node<'_>)> = Vec::new();
                while self.byte()? != b'e' {
                    let key = self.string()?;
                    if values.last().is_some_and(|(previous, _)| *previous >= key) {
                        return Err(BtError::InvalidMetadata);
                    }
                    values.push((key, self.node(depth + 1)?));
                }
                self.offset += 1;
                Value::Dictionary(values)
            }
            _ => return Err(BtError::InvalidMetadata),
        };
        Ok(Node {
            range: start..self.offset,
            value,
        })
    }
}

pub(crate) fn decode(bytes: &[u8], limits: MetadataLimits) -> Result<Node<'_>, BtError> {
    limits.validate()?;
    if bytes.len() > limits.bytes {
        return Err(BtError::MetadataLimit);
    }
    let mut parser = Parser {
        bytes,
        offset: 0,
        tokens: 0,
        limits,
    };
    let node = parser.node(0)?;
    if parser.offset != bytes.len() {
        return Err(BtError::InvalidMetadata);
    }
    Ok(node)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_values_reject_duplicate_keys_overflow_depth_and_every_truncation() {
        let encoded = b"d1:ali0e3:abce1:zi9223372036854775807ee";
        assert!(decode(encoded, MetadataLimits::default()).is_ok());
        for end in 0..encoded.len() {
            assert!(decode(&encoded[..end], MetadataLimits::default()).is_err());
        }
        for invalid in [
            b"i-0e".as_slice(),
            b"i01e",
            b"03:abc",
            b"d1:ai1e1:ai2ee",
            b"d1:bi1e1:ai2ee",
            b"i9223372036854775808e",
            b"1:aextra",
        ] {
            assert!(decode(invalid, MetadataLimits::default()).is_err());
        }
        let limits = MetadataLimits {
            depth: 2,
            ..MetadataLimits::default()
        };
        assert_eq!(
            decode(b"lllleeee", limits).unwrap_err(),
            BtError::MetadataLimit
        );
        let limits = MetadataLimits {
            tokens: 2,
            ..MetadataLimits::default()
        };
        assert_eq!(
            decode(b"li1ei2ee", limits).unwrap_err(),
            BtError::MetadataLimit
        );
    }
}
