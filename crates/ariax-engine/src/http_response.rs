//! Exact response-head validation for closed HTTP range attempts.

use ariax_core::UriId;
use ariax_storage::{
    GlobalSpan, JournalDigest, JournalDigestAlgorithm, JournalHash,
    calculate_http_strong_validator_fingerprint,
};
use hyper::header::{
    CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG, HeaderName, HeaderValue, LAST_MODIFIED,
    TRANSFER_ENCODING,
};
use hyper::{HeaderMap, StatusCode, Uri};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fmt;
use std::sync::Arc;

const HTTP_RANGE_VALIDATOR_HASH_DOMAIN: &str = "ariax/http-range-validator/v1\0";
const HTTP_RESOURCE_HASH_DOMAIN: &str = "ariax/http-resource/v1\0";
const MAX_HTTP_LAST_MODIFIED_BYTES: usize = 128;
/// A response head is bounded before any structured-field parsing. This is
/// deliberately smaller than the general HTTP header budget because one
/// digest dictionary only needs a handful of algorithm members.
pub const MAX_HTTP_REPR_DIGEST_BYTES: usize = 4096;
const MAX_HTTP_REPR_DIGEST_MEMBERS: usize = 16;
const MAX_HTTP_REPR_DIGEST_KEY_BYTES: usize = 64;
const REPR_DIGEST_HEADER: HeaderName = HeaderName::from_static("repr-digest");
type ParsedEtag = (Option<Box<[u8]>>, bool);

/// A validated SHA-256 representation digest advertised by RFC 9530's
/// `Repr-Digest` field. The raw bytes are retained so the value can be
/// compared across mirrors without re-encoding or trusting textual spelling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpRepresentationDigest([u8; 32]);

impl HttpRepresentationDigest {
    #[must_use]
    pub const fn sha256(value: [u8; 32]) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> [u8; 32] {
        self.0
    }

    #[must_use]
    pub fn journal_digest(self) -> JournalDigest {
        JournalDigest::new(JournalDigestAlgorithm::Sha256, self.0.to_vec())
            .expect("SHA-256 representation digest has the canonical length")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRangeResponseValidator {
    source: UriId,
    final_uri: Arc<str>,
    total_length: u64,
    etag: Option<Box<[u8]>>,
    strong_etag: bool,
    last_modified: Option<Box<[u8]>>,
    fingerprint: JournalHash,
    resource_fingerprint: JournalHash,
    strong_validator_fingerprint: Option<JournalHash>,
    representation_digest: Option<HttpRepresentationDigest>,
}

impl HttpRangeResponseValidator {
    pub fn from_probe(
        source: UriId,
        final_uri: &str,
        status: StatusCode,
        headers: &HeaderMap,
    ) -> Result<Self, HttpRangeResponseError> {
        let (_, _, total_length) =
            validate_exact_range_head(status, headers, GlobalSpan { offset: 0, len: 1 }, None)?;
        let (etag, strong_etag) = parse_etag(headers)?;
        let last_modified = parse_last_modified(headers)?;
        let representation_digest = parse_repr_digest(headers)?;
        let fingerprint = validator_fingerprint(
            source,
            final_uri,
            total_length,
            etag.as_deref(),
            last_modified.as_deref(),
            representation_digest,
        );
        let resource_fingerprint = http_resource_fingerprint(
            &final_uri
                .parse()
                .map_err(|_| HttpRangeResponseError::ResourceChanged)?,
        );
        let strong_validator_fingerprint = strong_etag
            .then(|| {
                calculate_http_strong_validator_fingerprint(
                    etag.as_deref().expect("strong ETag has bytes"),
                    total_length,
                )
            })
            .transpose()
            .map_err(|_| HttpRangeResponseError::InvalidValidator)?;
        Ok(Self {
            source,
            final_uri: final_uri.to_owned().into(),
            total_length,
            etag,
            strong_etag,
            last_modified,
            fingerprint,
            resource_fingerprint,
            strong_validator_fingerprint,
            representation_digest,
        })
    }

    pub fn validate_range(
        &self,
        final_uri: &str,
        status: StatusCode,
        headers: &HeaderMap,
        span: GlobalSpan,
    ) -> Result<Option<HttpRepresentationDigest>, HttpRangeResponseError> {
        if final_uri != self.final_uri.as_ref() {
            return Err(HttpRangeResponseError::ResourceChanged);
        }
        validate_exact_range_head(status, headers, span, Some(self.total_length))?;
        let (etag, _) = parse_etag(headers)?;
        let last_modified = parse_last_modified(headers)?;
        let representation_digest = parse_repr_digest(headers)?;
        if self.etag.is_some() {
            if etag.as_deref() != self.etag.as_deref() {
                return Err(HttpRangeResponseError::ValidatorChanged);
            }
        } else if self.last_modified.is_some()
            && last_modified.as_deref() != self.last_modified.as_deref()
        {
            return Err(HttpRangeResponseError::ValidatorChanged);
        }
        if self.representation_digest.is_some() && representation_digest.is_none() {
            return Err(HttpRangeResponseError::RepresentationDigestChanged);
        }
        Ok(representation_digest)
    }

    #[must_use]
    pub const fn source(&self) -> UriId {
        self.source
    }

    #[must_use]
    pub fn final_uri(&self) -> &str {
        &self.final_uri
    }

    #[must_use]
    pub const fn total_length(&self) -> u64 {
        self.total_length
    }

    #[must_use]
    pub fn if_range(&self) -> Option<&[u8]> {
        self.strong_etag.then_some(self.etag.as_deref()).flatten()
    }

    #[must_use]
    pub const fn fingerprint(&self) -> JournalHash {
        self.fingerprint
    }

    #[must_use]
    pub const fn resource_fingerprint(&self) -> JournalHash {
        self.resource_fingerprint
    }

    #[must_use]
    pub const fn strong_validator_fingerprint(&self) -> Option<JournalHash> {
        self.strong_validator_fingerprint
    }

    #[must_use]
    pub const fn representation_digest(&self) -> Option<HttpRepresentationDigest> {
        self.representation_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HttpRangeResponseError {
    UnexpectedStatus(StatusCode),
    RangeIgnored,
    RangeNotSatisfiable,
    TransferEncoding,
    ContentEncoding,
    MissingContentLength,
    DuplicateContentLength,
    InvalidContentLength,
    MissingContentRange,
    DuplicateContentRange,
    InvalidContentRange,
    RangeMismatch,
    InvalidValidator,
    DuplicateValidator,
    ValidatorChanged,
    InvalidRepresentationDigest,
    RepresentationDigestChanged,
    RepresentationDigestMismatch,
    ResourceChanged,
}

impl HttpRangeResponseError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::UnexpectedStatus(_) => "unexpected_http_status",
            Self::RangeIgnored => "range_ignored",
            Self::RangeNotSatisfiable => "range_not_satisfiable",
            Self::TransferEncoding => "transfer_encoding_forbidden",
            Self::ContentEncoding => "content_encoding_forbidden",
            Self::MissingContentLength => "missing_content_length",
            Self::DuplicateContentLength => "duplicate_content_length",
            Self::InvalidContentLength => "invalid_content_length",
            Self::MissingContentRange => "missing_content_range",
            Self::DuplicateContentRange => "duplicate_content_range",
            Self::InvalidContentRange => "invalid_content_range",
            Self::RangeMismatch => "range_length_mismatch",
            Self::InvalidValidator => "invalid_http_validator",
            Self::DuplicateValidator => "duplicate_http_validator",
            Self::ValidatorChanged => "stale_validator",
            Self::InvalidRepresentationDigest => "invalid_repr_digest",
            Self::RepresentationDigestChanged => "repr_digest_changed",
            Self::RepresentationDigestMismatch => "repr_digest_mismatch",
            Self::ResourceChanged => "redirect_resource_changed",
        }
    }

    #[must_use]
    pub const fn retryable_status(&self) -> bool {
        matches!(
            self,
            Self::UnexpectedStatus(status)
                if matches!(status.as_u16(), 408 | 425 | 429 | 500 | 502 | 503 | 504)
        )
    }
}

impl fmt::Display for HttpRangeResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Self::UnexpectedStatus(status) = self {
            write!(formatter, "unexpected HTTP status {status}")
        } else {
            formatter.write_str(self.code())
        }
    }
}

impl Error for HttpRangeResponseError {}

fn validate_exact_range_head(
    status: StatusCode,
    headers: &HeaderMap,
    span: GlobalSpan,
    expected_total: Option<u64>,
) -> Result<(u64, u64, u64), HttpRangeResponseError> {
    match status {
        StatusCode::PARTIAL_CONTENT => {}
        StatusCode::OK => return Err(HttpRangeResponseError::RangeIgnored),
        StatusCode::RANGE_NOT_SATISFIABLE => {
            return Err(HttpRangeResponseError::RangeNotSatisfiable);
        }
        status => return Err(HttpRangeResponseError::UnexpectedStatus(status)),
    }
    if headers.contains_key(TRANSFER_ENCODING) {
        return Err(HttpRangeResponseError::TransferEncoding);
    }
    let mut encodings = headers.get_all(CONTENT_ENCODING).iter();
    if let Some(encoding) = encodings.next()
        && (encodings.next().is_some()
            || !encoding
                .to_str()
                .is_ok_and(|value| value.eq_ignore_ascii_case("identity")))
    {
        return Err(HttpRangeResponseError::ContentEncoding);
    }
    let content_length = single_decimal_header(
        headers,
        CONTENT_LENGTH,
        HttpRangeResponseError::MissingContentLength,
        HttpRangeResponseError::DuplicateContentLength,
        HttpRangeResponseError::InvalidContentLength,
    )?;
    let content_range = single_header(
        headers,
        CONTENT_RANGE,
        HttpRangeResponseError::MissingContentRange,
        HttpRangeResponseError::DuplicateContentRange,
    )?
    .to_str()
    .map_err(|_| HttpRangeResponseError::InvalidContentRange)?;
    let (start, end, total) = parse_content_range(content_range)?;
    let expected_len =
        u64::try_from(span.len).map_err(|_| HttpRangeResponseError::RangeMismatch)?;
    let expected_end = span
        .offset
        .checked_add(expected_len)
        .and_then(|end| end.checked_sub(1))
        .ok_or(HttpRangeResponseError::RangeMismatch)?;
    if start != span.offset
        || end != expected_end
        || content_length != expected_len
        || expected_total.is_some_and(|expected| expected != total)
    {
        return Err(HttpRangeResponseError::RangeMismatch);
    }
    Ok((start, end, total))
}

fn single_header(
    headers: &HeaderMap,
    name: hyper::header::HeaderName,
    missing: HttpRangeResponseError,
    duplicate: HttpRangeResponseError,
) -> Result<&HeaderValue, HttpRangeResponseError> {
    let mut values = headers.get_all(name).iter();
    let first = values.next().ok_or(missing)?;
    if values.next().is_some() {
        return Err(duplicate);
    }
    Ok(first)
}

fn single_decimal_header(
    headers: &HeaderMap,
    name: hyper::header::HeaderName,
    missing: HttpRangeResponseError,
    duplicate: HttpRangeResponseError,
    invalid: HttpRangeResponseError,
) -> Result<u64, HttpRangeResponseError> {
    let value = single_header(headers, name, missing, duplicate)?
        .to_str()
        .map_err(|_| invalid.clone())?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid);
    }
    value.parse().map_err(|_| invalid)
}

fn parse_content_range(value: &str) -> Result<(u64, u64, u64), HttpRangeResponseError> {
    let value = value
        .strip_prefix("bytes ")
        .ok_or(HttpRangeResponseError::InvalidContentRange)?;
    let (range, total) = value
        .split_once('/')
        .ok_or(HttpRangeResponseError::InvalidContentRange)?;
    if total.contains('/') {
        return Err(HttpRangeResponseError::InvalidContentRange);
    }
    let (start, end) = range
        .split_once('-')
        .ok_or(HttpRangeResponseError::InvalidContentRange)?;
    if end.contains('-') {
        return Err(HttpRangeResponseError::InvalidContentRange);
    }
    let parse = |value: &str| {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(HttpRangeResponseError::InvalidContentRange);
        }
        value
            .parse::<u64>()
            .map_err(|_| HttpRangeResponseError::InvalidContentRange)
    };
    let start = parse(start)?;
    let end = parse(end)?;
    let total = parse(total)?;
    if start > end || end >= total {
        return Err(HttpRangeResponseError::InvalidContentRange);
    }
    Ok((start, end, total))
}

fn parse_etag(headers: &HeaderMap) -> Result<ParsedEtag, HttpRangeResponseError> {
    let mut values = headers.get_all(ETAG).iter();
    let Some(value) = values.next() else {
        return Ok((None, false));
    };
    if values.next().is_some() {
        return Err(HttpRangeResponseError::DuplicateValidator);
    }
    let bytes = value.as_bytes();
    let (validated, strong) = bytes
        .strip_prefix(b"W/")
        .map_or((bytes, true), |strong| (strong, false));
    calculate_http_strong_validator_fingerprint(validated, 0)
        .map_err(|_| HttpRangeResponseError::InvalidValidator)?;
    Ok((Some(bytes.to_vec().into_boxed_slice()), strong))
}

fn parse_last_modified(headers: &HeaderMap) -> Result<Option<Box<[u8]>>, HttpRangeResponseError> {
    let mut values = headers.get_all(LAST_MODIFIED).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(HttpRangeResponseError::DuplicateValidator);
    }
    let text = value
        .to_str()
        .map_err(|_| HttpRangeResponseError::InvalidValidator)?;
    if text.len() > MAX_HTTP_LAST_MODIFIED_BYTES || httpdate::parse_http_date(text).is_err() {
        return Err(HttpRangeResponseError::InvalidValidator);
    }
    Ok(Some(value.as_bytes().to_vec().into_boxed_slice()))
}

fn validator_fingerprint(
    source: UriId,
    final_uri: &str,
    total_length: u64,
    etag: Option<&[u8]>,
    last_modified: Option<&[u8]>,
    representation_digest: Option<HttpRepresentationDigest>,
) -> JournalHash {
    let mut digest = Sha256::new();
    digest.update(HTTP_RANGE_VALIDATOR_HASH_DOMAIN.as_bytes());
    digest.update(source.get().to_le_bytes());
    digest.update(total_length.to_le_bytes());
    for value in [Some(final_uri.as_bytes()), etag, last_modified] {
        let value = value.unwrap_or_default();
        digest.update((value.len() as u64).to_le_bytes());
        digest.update(value);
    }
    if let Some(representation_digest) = representation_digest {
        digest.update([1]);
        digest.update(representation_digest.value());
    } else {
        digest.update([0]);
    }
    JournalHash::new(digest.finalize().into()).expect("SHA-256 output is nonzero")
}

fn parse_repr_digest(
    headers: &HeaderMap,
) -> Result<Option<HttpRepresentationDigest>, HttpRangeResponseError> {
    let values = headers.get_all(REPR_DIGEST_HEADER);
    let mut field = Vec::new();
    let mut saw_value = false;
    for value in values.iter() {
        saw_value = true;
        if !value.as_bytes().is_ascii() {
            return Err(HttpRangeResponseError::InvalidRepresentationDigest);
        }
        let required = value
            .as_bytes()
            .len()
            .checked_add(usize::from(!field.is_empty()))
            .ok_or(HttpRangeResponseError::InvalidRepresentationDigest)?;
        if field
            .len()
            .checked_add(required)
            .is_none_or(|len| len > MAX_HTTP_REPR_DIGEST_BYTES)
        {
            return Err(HttpRangeResponseError::InvalidRepresentationDigest);
        }
        if !field.is_empty() {
            field.push(b',');
        }
        field.extend_from_slice(value.as_bytes());
    }
    if field.is_empty() {
        if saw_value {
            return Err(HttpRangeResponseError::InvalidRepresentationDigest);
        }
        return Ok(None);
    }
    parse_repr_digest_dictionary(&field)
}

fn parse_repr_digest_dictionary(
    field: &[u8],
) -> Result<Option<HttpRepresentationDigest>, HttpRangeResponseError> {
    let mut cursor = 0;
    let mut members = 0_usize;
    let mut sha256 = None;
    let mut keys: Vec<Box<[u8]>> = Vec::new();
    loop {
        skip_ows(field, &mut cursor);
        if cursor == field.len() {
            break;
        }
        members = members
            .checked_add(1)
            .ok_or(HttpRangeResponseError::InvalidRepresentationDigest)?;
        if members > MAX_HTTP_REPR_DIGEST_MEMBERS {
            return Err(HttpRangeResponseError::InvalidRepresentationDigest);
        }
        let key_start = cursor;
        while cursor < field.len() && is_structured_key_continue(field[cursor]) {
            cursor += 1;
            if cursor - key_start > MAX_HTTP_REPR_DIGEST_KEY_BYTES {
                return Err(HttpRangeResponseError::InvalidRepresentationDigest);
            }
        }
        if cursor == key_start {
            return Err(HttpRangeResponseError::InvalidRepresentationDigest);
        }
        let key = &field[key_start..cursor];
        if !is_structured_key_start(key[0]) || keys.iter().any(|seen| seen.as_ref() == key) {
            return Err(HttpRangeResponseError::InvalidRepresentationDigest);
        }
        keys.push(key.to_vec().into_boxed_slice());
        skip_ows(field, &mut cursor);
        if field.get(cursor) != Some(&b'=') {
            return Err(HttpRangeResponseError::InvalidRepresentationDigest);
        }
        cursor += 1;
        skip_ows(field, &mut cursor);
        if field.get(cursor) != Some(&b':') {
            return Err(HttpRangeResponseError::InvalidRepresentationDigest);
        }
        cursor += 1;
        let value_start = cursor;
        while cursor < field.len() && field[cursor] != b':' {
            if !field[cursor].is_ascii() || field[cursor].is_ascii_whitespace() {
                return Err(HttpRangeResponseError::InvalidRepresentationDigest);
            }
            cursor += 1;
        }
        if field.get(cursor) != Some(&b':') {
            return Err(HttpRangeResponseError::InvalidRepresentationDigest);
        }
        let decoded = decode_digest_base64(&field[value_start..cursor])?;
        cursor += 1;
        parse_repr_digest_parameters(field, &mut cursor)?;
        if key == b"sha-256" {
            if decoded.len() != 32 || sha256.is_some() {
                return Err(HttpRangeResponseError::InvalidRepresentationDigest);
            }
            let mut value = [0_u8; 32];
            value.copy_from_slice(&decoded);
            sha256 = Some(HttpRepresentationDigest::sha256(value));
        }
        skip_ows(field, &mut cursor);
        if cursor == field.len() {
            break;
        }
        if field[cursor] != b',' {
            return Err(HttpRangeResponseError::InvalidRepresentationDigest);
        }
        cursor += 1;
        skip_ows(field, &mut cursor);
        if cursor == field.len() {
            return Err(HttpRangeResponseError::InvalidRepresentationDigest);
        }
    }
    Ok(sha256)
}

fn parse_repr_digest_parameters(
    field: &[u8],
    cursor: &mut usize,
) -> Result<(), HttpRangeResponseError> {
    skip_ows(field, cursor);
    if field.get(*cursor) == Some(&b';') {
        // Parameters are intentionally rejected in this bounded milestone.
        // Their semantics are not part of the supported identity tuple, so
        // accepting them could treat an unrecognized coverage or
        // representation parameter as equivalent to the canonical digest.
        return Err(HttpRangeResponseError::InvalidRepresentationDigest);
    }
    Ok(())
}

fn decode_digest_base64(value: &[u8]) -> Result<Vec<u8>, HttpRangeResponseError> {
    if value.len() > 128 || value.is_empty() || !value.len().is_multiple_of(4) {
        return Err(HttpRangeResponseError::InvalidRepresentationDigest);
    }
    let mut decoded = Vec::with_capacity(value.len() / 4 * 3);
    for (index, chunk) in value.chunks_exact(4).enumerate() {
        let last = index + 1 == value.len() / 4;
        let a =
            base64_value(chunk[0]).ok_or(HttpRangeResponseError::InvalidRepresentationDigest)?;
        let b =
            base64_value(chunk[1]).ok_or(HttpRangeResponseError::InvalidRepresentationDigest)?;
        let c = if chunk[2] == b'=' {
            if !last || chunk[3] != b'=' {
                return Err(HttpRangeResponseError::InvalidRepresentationDigest);
            }
            0
        } else {
            base64_value(chunk[2]).ok_or(HttpRangeResponseError::InvalidRepresentationDigest)?
        };
        let d = if chunk[3] == b'=' {
            if !last {
                return Err(HttpRangeResponseError::InvalidRepresentationDigest);
            }
            if chunk[2] != b'=' && c & 0x03 != 0 {
                return Err(HttpRangeResponseError::InvalidRepresentationDigest);
            }
            0
        } else {
            base64_value(chunk[3]).ok_or(HttpRangeResponseError::InvalidRepresentationDigest)?
        };
        decoded.push((a << 2) | (b >> 4));
        if chunk[2] != b'=' {
            decoded.push((b << 4) | (c >> 2));
        } else if b & 0x0f != 0 {
            return Err(HttpRangeResponseError::InvalidRepresentationDigest);
        }
        if chunk[3] != b'=' {
            decoded.push((c << 6) | d);
        }
    }
    if decoded.len() > 64 {
        return Err(HttpRangeResponseError::InvalidRepresentationDigest);
    }
    Ok(decoded)
}

const fn base64_value(value: u8) -> Option<u8> {
    match value {
        b'A'..=b'Z' => Some(value - b'A'),
        b'a'..=b'z' => Some(value - b'a' + 26),
        b'0'..=b'9' => Some(value - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

const fn is_structured_key_start(value: u8) -> bool {
    value.is_ascii_lowercase() || value == b'*'
}

const fn is_structured_key_continue(value: u8) -> bool {
    value.is_ascii_lowercase()
        || value.is_ascii_digit()
        || matches!(value, b'*' | b'-' | b'.' | b'_')
}

fn skip_ows(field: &[u8], cursor: &mut usize) {
    while matches!(field.get(*cursor), Some(b' ' | b'\t')) {
        *cursor += 1;
    }
}

pub(crate) fn http_resource_fingerprint(uri: &Uri) -> JournalHash {
    let mut digest = Sha256::new();
    digest.update(HTTP_RESOURCE_HASH_DOMAIN.as_bytes());
    for component in [
        uri.scheme_str().unwrap_or_default().as_bytes(),
        uri.authority()
            .map_or(&[][..], |value| value.as_str().as_bytes()),
        uri.path_and_query()
            .map_or(b"/".as_slice(), |value| value.as_str().as_bytes()),
    ] {
        digest.update(
            u32::try_from(component.len())
                .expect("URI component length fits u32")
                .to_le_bytes(),
        );
        digest.update(component);
    }
    JournalHash::new(digest.finalize().into()).expect("SHA-256 output is nonzero")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(range: &str, length: &str, etag: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_RANGE, range.parse().expect("range"));
        headers.insert(CONTENT_LENGTH, length.parse().expect("length"));
        headers.insert(ETAG, etag.parse().expect("etag"));
        headers
    }

    #[test]
    fn parses_rfc9530_sha256_representation_digest_and_ignores_other_algorithms() {
        let mut response = headers("bytes 0-0/4", "1", "\"v1\"");
        response.insert(
            REPR_DIGEST_HEADER,
            "sha-512=:AQIDBA==:, sha-256=:ERERERERERERERERERERERERERERERERERERERERERE=:"
                .parse()
                .expect("digest"),
        );
        let validator = HttpRangeResponseValidator::from_probe(
            UriId::new(0),
            "https://example.test/file",
            StatusCode::PARTIAL_CONTENT,
            &response,
        )
        .expect("probe");
        assert_eq!(
            validator
                .representation_digest()
                .map(|digest| digest.value()),
            Some([0x11; 32])
        );
    }

    #[test]
    fn rejects_duplicate_or_malformed_representation_digest_members() {
        for value in [
            "sha-256=:ERERERERERERERERERERERERERERERERERERERERERE=:, sha-256=:ERERERERERERERERERERERERERERERERERERERERERE=:",
            "sha-256=\"not-a-byte-sequence\"",
            "sha-256=:not-base64:",
            "sha-256=:ERERERERERERERERERERERERERERERERERERERERERE=:;bad=",
            "sha-256=:ERERERERERERERERERERERERERERERERERERERERERF=:",
            "sha-256=:ERERERERERERERERERERERERERERERERERERERERERE=:,",
            "SHA-256=:ERERERERERERERERERERERERERERERERERERERERERE=:",
        ] {
            let mut response = headers("bytes 0-0/4", "1", "\"v1\"");
            response.insert(REPR_DIGEST_HEADER, value.parse().expect("header"));
            assert!(
                matches!(
                    HttpRangeResponseValidator::from_probe(
                        UriId::new(0),
                        "https://example.test/file",
                        StatusCode::PARTIAL_CONTENT,
                        &response,
                    ),
                    Err(HttpRangeResponseError::InvalidRepresentationDigest)
                ),
                "{value}"
            );
        }
    }

    #[test]
    fn repeated_representation_digest_fields_merge_with_bounded_dictionary_rules() {
        let mut response = headers("bytes 0-0/4", "1", "\"v1\"");
        response.append(
            REPR_DIGEST_HEADER,
            "sha-512=:AQIDBA==:".parse().expect("digest"),
        );
        response.append(
            REPR_DIGEST_HEADER,
            "sha-256=:ERERERERERERERERERERERERERERERERERERERERERE=:"
                .parse()
                .expect("digest"),
        );
        let validator = HttpRangeResponseValidator::from_probe(
            UriId::new(0),
            "https://example.test/file",
            StatusCode::PARTIAL_CONTENT,
            &response,
        )
        .expect("merged digest fields");
        assert_eq!(
            validator
                .representation_digest()
                .map(|digest| digest.value()),
            Some([0x11; 32])
        );

        let mut empty = headers("bytes 0-0/4", "1", "\"v1\"");
        empty.insert(REPR_DIGEST_HEADER, HeaderValue::from_static(""));
        assert!(matches!(
            HttpRangeResponseValidator::from_probe(
                UriId::new(0),
                "https://example.test/file",
                StatusCode::PARTIAL_CONTENT,
                &empty,
            ),
            Err(HttpRangeResponseError::InvalidRepresentationDigest)
        ));
    }

    #[test]
    fn representation_digest_rejects_member_and_byte_caps_before_decode() {
        let members = (0..=MAX_HTTP_REPR_DIGEST_MEMBERS)
            .map(|index| format!("a{index}=:AQIDBA==:"))
            .collect::<Vec<_>>()
            .join(",");
        let mut too_many = headers("bytes 0-0/4", "1", "\"v1\"");
        too_many.insert(REPR_DIGEST_HEADER, members.parse().expect("members"));
        assert!(matches!(
            HttpRangeResponseValidator::from_probe(
                UriId::new(0),
                "https://example.test/file",
                StatusCode::PARTIAL_CONTENT,
                &too_many,
            ),
            Err(HttpRangeResponseError::InvalidRepresentationDigest)
        ));

        let oversized = format!("sha-256=:{}:", "A".repeat(MAX_HTTP_REPR_DIGEST_BYTES));
        let mut too_large = headers("bytes 0-0/4", "1", "\"v1\"");
        too_large.insert(REPR_DIGEST_HEADER, oversized.parse().expect("oversized"));
        assert!(matches!(
            HttpRangeResponseValidator::from_probe(
                UriId::new(0),
                "https://example.test/file",
                StatusCode::PARTIAL_CONTENT,
                &too_large,
            ),
            Err(HttpRangeResponseError::InvalidRepresentationDigest)
        ));
    }

    #[test]
    fn representation_digest_must_stay_present_and_can_vary_by_exact_range() {
        let digest = "sha-256=:ERERERERERERERERERERERERERERERERERERERERERE=:";
        let mut probe = headers("bytes 0-0/10", "1", "\"v1\"");
        probe.insert(REPR_DIGEST_HEADER, digest.parse().expect("digest"));
        let validator = HttpRangeResponseValidator::from_probe(
            UriId::new(0),
            "https://example.test/file",
            StatusCode::PARTIAL_CONTENT,
            &probe,
        )
        .expect("probe");
        assert_eq!(
            validator.validate_range(
                "https://example.test/file",
                StatusCode::PARTIAL_CONTENT,
                &headers("bytes 4-7/10", "4", "\"v1\""),
                GlobalSpan { offset: 4, len: 4 },
            ),
            Err(HttpRangeResponseError::RepresentationDigestChanged)
        );
        let mut changed = headers("bytes 4-7/10", "4", "\"v1\"");
        changed.insert(
            REPR_DIGEST_HEADER,
            "sha-256=:IiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiI=:"
                .parse()
                .expect("digest"),
        );
        assert!(matches!(
            validator.validate_range(
                "https://example.test/file",
                StatusCode::PARTIAL_CONTENT,
                &changed,
                GlobalSpan { offset: 4, len: 4 },
            ),
            Ok(Some(_))
        ));
    }

    #[test]
    fn probe_settles_total_and_exact_range_reuses_strong_validator() {
        let probe = headers("bytes 0-0/10", "1", "\"v1\"");
        let validator = HttpRangeResponseValidator::from_probe(
            UriId::new(0),
            "https://example.test/file",
            StatusCode::PARTIAL_CONTENT,
            &probe,
        )
        .expect("probe");
        assert_eq!(validator.total_length(), 10);
        assert_eq!(validator.if_range(), Some(b"\"v1\"".as_slice()));
        assert_eq!(
            validator.strong_validator_fingerprint(),
            Some(
                calculate_http_strong_validator_fingerprint(b"\"v1\"", 10)
                    .expect("strong validator")
            )
        );
        assert_eq!(
            validator.resource_fingerprint(),
            http_resource_fingerprint(&"https://example.test/file".parse().expect("resource URI"))
        );
        validator
            .validate_range(
                "https://example.test/file",
                StatusCode::PARTIAL_CONTENT,
                &headers("bytes 4-7/10", "4", "\"v1\""),
                GlobalSpan { offset: 4, len: 4 },
            )
            .expect("exact range");
    }

    #[test]
    fn rejects_ignored_mismatched_coded_and_stale_ranges() {
        let probe = headers("bytes 0-0/10", "1", "\"v1\"");
        let validator = HttpRangeResponseValidator::from_probe(
            UriId::new(0),
            "https://example.test/file",
            StatusCode::PARTIAL_CONTENT,
            &probe,
        )
        .expect("probe");
        assert_eq!(
            validator.validate_range(
                "https://example.test/file",
                StatusCode::OK,
                &HeaderMap::new(),
                GlobalSpan { offset: 4, len: 4 },
            ),
            Err(HttpRangeResponseError::RangeIgnored)
        );
        assert_eq!(
            validator.validate_range(
                "https://example.test/file",
                StatusCode::PARTIAL_CONTENT,
                &headers("bytes 5-8/10", "4", "\"v1\""),
                GlobalSpan { offset: 4, len: 4 },
            ),
            Err(HttpRangeResponseError::RangeMismatch)
        );
        let mut coded = headers("bytes 4-7/10", "4", "\"v1\"");
        coded.insert(CONTENT_ENCODING, "gzip".parse().expect("encoding"));
        assert_eq!(
            validator.validate_range(
                "https://example.test/file",
                StatusCode::PARTIAL_CONTENT,
                &coded,
                GlobalSpan { offset: 4, len: 4 },
            ),
            Err(HttpRangeResponseError::ContentEncoding)
        );
        assert_eq!(
            validator.validate_range(
                "https://example.test/file",
                StatusCode::PARTIAL_CONTENT,
                &headers("bytes 4-7/10", "4", "\"v2\""),
                GlobalSpan { offset: 4, len: 4 },
            ),
            Err(HttpRangeResponseError::ValidatorChanged)
        );
    }
}
