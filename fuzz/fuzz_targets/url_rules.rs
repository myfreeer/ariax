#![no_main]

use ariax_config::{MAX_URL_RULE_DOCUMENT_BYTES, MAX_URL_RULES, UrlRules};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_URL_RULE_DOCUMENT_BYTES {
        return;
    }
    if let Ok(text) = std::str::from_utf8(data)
        && let Ok(rules) = UrlRules::parse(text)
    {
        assert!(rules.rules().len() <= MAX_URL_RULES);
        let canonical = rules.to_toml().expect("serializable rules");
        let Ok(roundtrip) = UrlRules::parse(&canonical) else {
            return;
        };
        for uri in [
            "http://example.test/file",
            "https://u:secret@example.test/file?token=x#y",
            text,
        ] {
            assert_eq!(rules.apply(uri), roundtrip.apply(uri));
        }
    }
});
