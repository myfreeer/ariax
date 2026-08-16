#![no_main]

use ariax_engine::{HttpCustomHeaders, HttpRequestPolicy, build_http_request};
use ariax_storage::GlobalSpan;
use hyper::Method;
use libfuzzer_sys::fuzz_target;

const MAX_INPUT_BYTES: usize = 65_536;

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).chars().take(512).collect()
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }
    let mut headers = Vec::new();
    for (index, chunk) in data.chunks(16).take(64).enumerate() {
        let name = if index == 0 && data.first().is_some_and(|byte| byte & 0x0f == 0) {
            "range".to_owned()
        } else {
            format!("x-fuzz-{index}")
        };
        let value = text(chunk);
        headers.push((name, value));
    }
    let custom = HttpCustomHeaders::new(headers);
    let Ok(custom) = custom else {
        return;
    };
    let span = if data.first().copied().unwrap_or_default() & 1 == 0 {
        Some(GlobalSpan {
            offset: u64::from(data.get(1).copied().unwrap_or_default()),
            len: usize::from(data.get(2).copied().unwrap_or(1)).max(1),
        })
    } else {
        None
    };
    let _ = build_http_request(HttpRequestPolicy {
        method: if data.get(3).copied().unwrap_or_default() & 1 == 0 {
            Method::GET
        } else {
            Method::HEAD
        },
        uri: if data.get(4).copied().unwrap_or_default() & 1 == 0 {
            "https://mirror.example.invalid/file"
        } else {
            "http://[invalid"
        },
        route: None,
        range: span,
        if_range: span.map(|_| b"\"fuzz-etag\"".as_slice()),
        want_repr_digest: data.get(5).copied().unwrap_or_default() & 1 != 0,
        authorization: None,
        proxy_authorization: None,
        cookie: None,
        custom_headers: &custom,
    });
});
