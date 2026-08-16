#![no_main]

use ariax_core::UriId;
use ariax_engine::HttpRangeResponseValidator;
use ariax_storage::GlobalSpan;
use hyper::header::{
    CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG, HeaderValue, LAST_MODIFIED,
    TRANSFER_ENCODING,
};
use hyper::{HeaderMap, StatusCode};
use libfuzzer_sys::fuzz_target;

const MAX_INPUT_BYTES: usize = 65_536;

fn bounded_u64(data: &[u8], offset: usize) -> u64 {
    let mut bytes = [0_u8; 8];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = data.get(offset + index).copied().unwrap_or_default();
    }
    u64::from_le_bytes(bytes)
}

fn header_value(value: &str) -> Option<HeaderValue> {
    HeaderValue::from_str(value).ok()
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    let requested_len = bounded_u64(data, 0) % 4096 + 1;
    let requested_offset = bounded_u64(data, 8) % 1_048_576;
    let total = requested_offset
        .saturating_add(requested_len)
        .saturating_add(bounded_u64(data, 16) % 4096);
    let requested_end = requested_offset
        .saturating_add(requested_len)
        .saturating_sub(1);
    let status = match data.first().copied().unwrap_or_default() % 5 {
        0 => StatusCode::PARTIAL_CONTENT,
        1 => StatusCode::OK,
        2 => StatusCode::RANGE_NOT_SATISFIABLE,
        3 => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::from_u16(600).unwrap_or(StatusCode::BAD_GATEWAY),
    };
    let mut headers = HeaderMap::new();
    if let Some(value) = header_value(&requested_len.to_string()) {
        headers.insert(CONTENT_LENGTH, value);
    }
    let range = if data.get(1).is_some_and(|byte| byte & 1 == 0) {
        format!("bytes {requested_offset}-{requested_end}/{total}")
    } else {
        String::from_utf8_lossy(
            &data.get(24..).unwrap_or_default()[..data.len().saturating_sub(24).min(128)],
        )
        .into_owned()
    };
    if let Some(value) = header_value(&range) {
        headers.insert(CONTENT_RANGE, value);
    }
    if data.get(2).is_some_and(|byte| byte & 1 != 0) {
        headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    } else if data.get(3).is_some_and(|byte| byte & 1 != 0) {
        headers.insert(CONTENT_ENCODING, HeaderValue::from_static("identity"));
    }
    if data.get(4).is_some_and(|byte| byte & 1 != 0) {
        headers.insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
    }
    if data.get(5).is_some_and(|byte| byte & 1 != 0) {
        headers.insert(ETAG, HeaderValue::from_static("\"fuzz-etag\""));
    }
    if data.get(6).is_some_and(|byte| byte & 1 != 0) {
        headers.insert(
            LAST_MODIFIED,
            HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT"),
        );
    }

    let final_uri = if data.get(7).is_some_and(|byte| byte & 1 != 0) {
        "https://mirror.example.invalid/file"
    } else {
        "not-a-uri"
    };
    let span = GlobalSpan {
        offset: requested_offset,
        len: usize::try_from(requested_len).unwrap_or(1),
    };
    if let Ok(validator) = HttpRangeResponseValidator::from_probe(
        UriId::new(u32::from(data.get(8).copied().unwrap_or_default())),
        final_uri,
        status,
        &headers,
    ) {
        let _ = validator.validate_range(final_uri, status, &headers, span);
        let _ =
            validator.validate_range("https://other.example.invalid/file", status, &headers, span);
    }
});
