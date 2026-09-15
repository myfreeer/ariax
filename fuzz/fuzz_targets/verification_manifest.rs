#![no_main]
use ariax_storage::VerificationManifest;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > 64 * 1024 {
        return;
    }
    if let Ok(manifest) = VerificationManifest::decode(data) {
        assert_eq!(manifest.encode(), data);
        assert_eq!(
            VerificationManifest::decode(&manifest.encode())
                .unwrap()
                .fingerprint(),
            manifest.fingerprint()
        );
        if !manifest.chunks().is_empty() {
            let first = manifest.chunk_span(0).unwrap();
            assert_eq!(first.offset(), 0);
            assert!(first.len() <= manifest.chunk_length());
        }
    }
});
