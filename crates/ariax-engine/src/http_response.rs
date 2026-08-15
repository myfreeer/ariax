//! Exact response-head validation for closed HTTP range attempts.

use ariax_core::UriId;
use ariax_storage::{GlobalSpan, JournalHash, calculate_http_strong_validator_fingerprint};
use hyper::header::{
    CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG, HeaderValue, LAST_MODIFIED,
    TRANSFER_ENCODING,
};
use hyper::{HeaderMap, StatusCode};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fmt;
use std::sync::Arc;

const HTTP_RANGE_VALIDATOR_HASH_DOMAIN: &str = "ariax/http-range-validator/v1\0";
const MAX_HTTP_LAST_MODIFIED_BYTES: usize = 128;
type ParsedEtag = (Option<Box<[u8]>>, bool);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRangeResponseValidator {
    source: UriId,
    final_uri: Arc<str>,
    total_length: u64,
    etag: Option<Box<[u8]>>,
    strong_etag: bool,
    last_modified: Option<Box<[u8]>>,
    fingerprint: JournalHash,
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
        let fingerprint = validator_fingerprint(
            source,
            final_uri,
            total_length,
            etag.as_deref(),
            last_modified.as_deref(),
        );
        Ok(Self {
            source,
            final_uri: final_uri.to_owned().into(),
            total_length,
            etag,
            strong_etag,
            last_modified,
            fingerprint,
        })
    }

    pub fn validate_range(
        &self,
        final_uri: &str,
        status: StatusCode,
        headers: &HeaderMap,
        span: GlobalSpan,
    ) -> Result<(), HttpRangeResponseError> {
        if final_uri != self.final_uri.as_ref() {
            return Err(HttpRangeResponseError::ResourceChanged);
        }
        validate_exact_range_head(status, headers, span, Some(self.total_length))?;
        let (etag, _) = parse_etag(headers)?;
        let last_modified = parse_last_modified(headers)?;
        if self.etag.is_some() {
            if etag.as_deref() != self.etag.as_deref() {
                return Err(HttpRangeResponseError::ValidatorChanged);
            }
        } else if self.last_modified.is_some()
            && last_modified.as_deref() != self.last_modified.as_deref()
        {
            return Err(HttpRangeResponseError::ValidatorChanged);
        }
        Ok(())
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
