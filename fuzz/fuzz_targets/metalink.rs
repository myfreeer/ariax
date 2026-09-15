#![no_main]
use ariax_engine::{MetalinkOptions, parse_metalink};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > 16 * 1024 {
        return;
    }
    let options = MetalinkOptions {
        max_document_bytes: 16 * 1024,
        max_files: 8,
        max_sources: 8,
        metadata_bytes: 256 * 1024,
        base_uri: Some("https://example.test/metadata".into()),
        ..Default::default()
    };
    if let Ok(document) = parse_metalink(data, &options) {
        assert!(document.files.len() <= 8);
        assert!(document.retained_bytes <= options.metadata_bytes);
        for file in document.files {
            assert!(!file.sources.is_empty() && file.sources.len() <= 8);
            let encoded = file.verification.encode();
            assert_eq!(
                &ariax_storage::VerificationManifest::decode(&encoded).unwrap(),
                file.verification.as_ref()
            );
        }
    }
});
