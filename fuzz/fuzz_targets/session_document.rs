#![no_main]

use ariax_engine::{MAX_SESSION_DOCUMENT_BYTES, SessionFormat, validate_session_syntax};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_SESSION_DOCUMENT_BYTES { return; }
    if let Ok(text) = std::str::from_utf8(data) {
        for format in [SessionFormat::Aria2, SessionFormat::Json] {
            if let Ok(count) = validate_session_syntax(text, format) {
                assert!(count <= ariax_storage::SESSION_MAX_IMPORT_TASKS);
            }
        }
    }
});
