use ariax_bt_metadata::{
    BtError, MetadataLimits, info_section, parse_info, parse_torrent, torrent_from_info,
};

#[test]
fn transfer_fixtures_preserve_identity_without_outer_piece_layers() {
    for (bytes, pieces, files) in [
        (
            include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/v1.torrent").as_slice(),
            1,
            1,
        ),
        (
            include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/v2.torrent").as_slice(),
            1,
            1,
        ),
        (
            include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/hybrid.torrent")
                .as_slice(),
            1,
            1,
        ),
        (
            include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/multi-piece-v2.torrent")
                .as_slice(),
            5,
            1,
        ),
        (
            include_bytes!(
                "../../ariax-bt-libtorrent-sys/tests/fixtures/multi-piece-hybrid.torrent"
            )
            .as_slice(),
            5,
            1,
        ),
        (
            include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/selection-v1.torrent")
                .as_slice(),
            2,
            2,
        ),
    ] {
        let limits = MetadataLimits::default();
        let metadata = parse_torrent(bytes, limits).unwrap();
        assert_eq!(metadata.pieces, pieces);
        assert_eq!(
            metadata.files.iter().filter(|file| !file.padding).count(),
            files
        );
        let info = info_section(bytes, limits).unwrap();
        let restored = torrent_from_info(info, limits).unwrap();
        assert_eq!(parse_torrent(&restored, limits).unwrap(), metadata);
        assert_eq!(info_section(&restored, limits).unwrap(), info);
    }
}

#[test]
fn malicious_peer_fixtures_have_typed_path_and_symlink_rejections() {
    for (bytes, expected) in [
        (
            include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/unsafe-path-v1.torrent")
                .as_slice(),
            BtError::UnsafePath,
        ),
        (
            include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/symlink-v1.torrent")
                .as_slice(),
            BtError::Symlink,
        ),
    ] {
        let limits = MetadataLimits::default();
        let info = info_section(bytes, limits).unwrap();
        assert_eq!(parse_torrent(bytes, limits), Err(expected));
        assert_eq!(parse_info(info, limits), Err(expected));
    }
}
