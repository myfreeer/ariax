#![forbid(unsafe_code)]
#![cfg(target_os = "linux")]

#[path = "../benches/rpc_active_profile/peer_startup.rs"]
mod peer_startup;

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::timeout;

async fn accept_handshake(listener: &TcpListener) -> (TcpStream, [u8; 68]) {
    timeout(Duration::from_secs(3), async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 68];
        stream.read_exact(&mut request).await.unwrap();
        (stream, request)
    })
    .await
    .expect("fixture did not receive handshake")
}

#[tokio::test]
async fn pending_handshakes_are_bounded_but_established_peers_are_not() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let gate = Arc::new(Semaphore::new(peer_startup::HANDSHAKES));
    let mut clients = tokio::task::JoinSet::new();
    for index in 0..=peer_startup::HANDSHAKES {
        let gate = gate.clone();
        clients.spawn(async move { peer_startup::connect(index, port, [7; 20], &gate).await });
    }
    let mut server_peers = Vec::new();
    for _ in 0..peer_startup::HANDSHAKES {
        server_peers.push(accept_handshake(&listener).await);
    }
    assert!(
        timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err(),
        "another connection started before a handshake completed"
    );
    for (stream, request) in &mut server_peers {
        stream.write_all(request).await.unwrap();
    }
    let (mut stream, request) = accept_handshake(&listener).await;
    stream.write_all(&request).await.unwrap();
    server_peers.push((stream, request));
    let mut established = Vec::new();
    while let Some(result) = timeout(Duration::from_secs(3), clients.join_next())
        .await
        .unwrap()
    {
        established.push(result.unwrap().unwrap());
    }
    assert_eq!(established.len(), peer_startup::HANDSHAKES + 1);
    // Every connection stays usable after its startup permit is released.
    for (stream, _) in &mut server_peers {
        stream.write_all(&[42]).await.unwrap();
    }
    for stream in &mut established {
        assert_eq!(
            timeout(Duration::from_secs(3), stream.read_u8())
                .await
                .unwrap()
                .unwrap(),
            42
        );
    }
}

#[tokio::test]
async fn rejected_identity_and_early_eof_report_handshake_failure() {
    for wrong_identity in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let client = tokio::spawn(async move {
            peer_startup::connect(0, port, [7; 20], &Semaphore::new(1)).await
        });
        let (mut stream, mut request) = accept_handshake(&listener).await;
        if wrong_identity {
            request[28] ^= 1;
            stream.write_all(&request).await.unwrap();
        }
        drop(stream);
        let error = timeout(Duration::from_secs(3), client)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .to_string();
        assert!(error.contains(if wrong_identity {
            "handshake identity mismatch"
        } else {
            "handshake reply"
        }));
        assert!(
            timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "failed handshake was retried"
        );
    }
}

#[tokio::test]
async fn stalled_handshake_has_a_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let client =
        tokio::spawn(
            async move { peer_startup::connect(0, port, [7; 20], &Semaphore::new(1)).await },
        );
    let (_stream, _) = accept_handshake(&listener).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(15)).await;
    assert!(
        client
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("handshake exceeded 15 seconds")
    );
}
