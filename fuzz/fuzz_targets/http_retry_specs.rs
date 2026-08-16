#![no_main]

use ariax_engine::{
    HttpRetryAfterPolicy, HttpRetryBackoff, HttpRetryProfile, HttpRetryStatusSet,
    HttpRetryTriggerSet, HttpStaleValidatorPolicy,
};
use libfuzzer_sys::fuzz_target;

const MAX_INPUT_BYTES: usize = 8_192;

fn bounded_text(data: &[u8]) -> String {
    String::from_utf8_lossy(&data[..data.len().min(MAX_INPUT_BYTES)]).into_owned()
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }
    let input = bounded_text(data);
    let _ = HttpRetryProfile::parse(&input);
    let _ = HttpRetryBackoff::parse(&input);
    let _ = HttpRetryAfterPolicy::parse(&input);
    let _ = HttpStaleValidatorPolicy::parse(&input);
    let _ = HttpRetryTriggerSet::parse(&input);
    let _ = HttpRetryStatusSet::parse(&input);
    for delimiter in [',', '-', ' ', '\n', '\0'] {
        let mutated = input.replace(',', &delimiter.to_string());
        let _ = HttpRetryStatusSet::parse(&mutated);
        let _ = HttpRetryTriggerSet::parse(&mutated);
    }
});
