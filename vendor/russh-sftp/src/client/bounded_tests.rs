use super::*;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn over_cap_prefix_does_not_wait_for_payload() {
    let (client, mut server) = tokio::io::duplex(16);
    let raw = RawSftpSession::new_with_config(client, Config { max_packet_len: 64, ..Config::default() });
    let response = tokio::spawn(async move { raw.init().await });
    // Only the prefix is sent. A payload read would stall until the test deadline.
    server.write_u32(65).await.unwrap();
    let error = tokio::time::timeout(std::time::Duration::from_secs(1), response)
        .await.unwrap().unwrap().unwrap_err();
    assert!(matches!(error, error::Error::PacketTooLarge));
}

#[tokio::test]
async fn trailing_or_malformed_frames_close_and_drain_every_request() {
    for bytes in [vec![0xff], vec![2, 0, 0, 0, 3, 1], vec![]] {
        let (client, mut server) = tokio::io::duplex(128);
        let raw = RawSftpSession::new(client);
        let response = tokio::spawn(async move { raw.init().await });
        server.write_u32(bytes.len() as u32).await.unwrap();
        server.write_all(&bytes).await.unwrap();
        assert!(tokio::time::timeout(std::time::Duration::from_secs(1), response)
            .await.unwrap().unwrap().is_err());
    }
}

#[tokio::test]
async fn request_admission_is_bounded_and_close_drains() {
    let (client, mut server) = tokio::io::duplex(8192);
    let raw = Arc::new(RawSftpSession::new(client));
    let init = { let raw = raw.clone(); tokio::spawn(async move { raw.init().await }) };
    let length = server.read_u32().await.unwrap();
    let mut request = vec![0; length as usize];
    server.read_exact(&mut request).await.unwrap();
    server.write_all(&[0, 0, 0, 5, 2, 0, 0, 0, 3]).await.unwrap();
    init.await.unwrap().unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..MAX_PENDING_REQUESTS + 1 {
        let raw = raw.clone();
        tasks.spawn(async move { raw.fstat("handle").await });
    }
    let error = tasks.join_next().await.unwrap().unwrap().unwrap_err();
    assert!(matches!(error, error::Error::RequestLimit));
    assert_eq!(raw.pending_requests(), MAX_PENDING_REQUESTS);
    raw.close_session().unwrap();
    while let Some(result) = tasks.join_next().await {
        assert!(matches!(result.unwrap(), Err(error::Error::SessionClosed)));
    }
    assert_eq!(raw.pending_requests(), 0);
    assert!(matches!(raw.fstat("handle").await, Err(error::Error::SessionClosed)));
}

#[tokio::test]
async fn request_timeout_closes_session_and_negotiation_only_lowers_cap() {
    let (client, _server) = tokio::io::duplex(64);
    let mut raw = RawSftpSession::new_with_config(client, Config { request_timeout_secs: 0, ..Config::default() });
    raw.set_limits(rawsession::Limits { packet_len: Some(64), ..Default::default() });
    raw.set_limits(rawsession::Limits { packet_len: Some(u64::MAX), ..Default::default() });
    assert_eq!(raw.packet_cap(), 64);
    assert!(matches!(raw.init().await, Err(error::Error::Timeout)));
    assert_eq!(raw.pending_requests(), 0);
    assert!(raw.init().await.is_err());
}

#[test]
fn decoder_rejects_hostile_counts_trailing_data_and_invalid_utf8() {
    // NAME request id=1, count=u32::MAX and no entries.
    assert!(Packet::try_from(&mut Bytes::from_static(&[104, 0, 0, 0, 1, 255, 255, 255, 255])).is_err());
    assert!(Packet::try_from(&mut Bytes::from_static(&[103, 0, 0, 0, 1, 0, 0, 0, 1, 42, 42])).is_err());
    use crate::buf::TryBuf;
    assert!(Bytes::from_static(&[0, 0, 0, 1, 255]).try_get_string().is_err());
}
