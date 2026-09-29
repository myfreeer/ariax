#![no_main]

use ariax_bt_metadata::*;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let limits = MetadataLimits {
        bytes: 1024 * 1024,
        depth: 16,
        tokens: 16_384,
        files: 256,
        pieces: 32_768,
    };
    if data.len() > limits.bytes {
        return;
    }
    if let Ok(metadata) = parse_torrent(data, limits) {
        assert!(metadata.files.len() <= limits.files);
        assert!(metadata.pieces <= limits.pieces);
        let info = info_section(data, limits).unwrap();
        assert_eq!(
            parse_info(info, limits).unwrap().identity,
            metadata.identity
        );
        let rebuilt = torrent_from_info(info, limits).unwrap();
        assert_eq!(
            parse_torrent(&rebuilt, limits).unwrap().identity,
            metadata.identity
        );
        if let Ok(bytes) = with_trackers(data, &[], &["*".into()], limits) {
            let without = parse_torrent(&bytes, limits).unwrap();
            assert_eq!(without.identity, metadata.identity);
            assert!(without.trackers.is_empty());
            assert_eq!(info_section(&bytes, limits).unwrap(), info);
        }
    }
    let _ = parse_info(data, limits);
    let identity = BtIdentity {
        v1: Some("1".repeat(40)),
        v2: None,
    };
    let _ = validate_resume(data, &identity);
    if let Ok(text) = std::str::from_utf8(data)
        && let Ok(magnet) = parse_magnet(text)
        && let Ok(uri) = magnet_with_trackers(text, &[], &["*".into()])
    {
        let clean = parse_magnet(&uri).unwrap();
        assert_eq!(clean.identity, magnet.identity);
        assert!(clean.trackers.is_empty());
    }
});
