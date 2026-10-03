#![forbid(unsafe_code)]

// Exercise the same fixture helper imported by the custom benchmark harness.
mod origin_metrics {
    include!("../benches/rpc_active_profile/origin_metrics.rs");

    #[cfg(test)]
    mod tests {
        use super::*;
        use serde_json::json;
        use tokio::net::TcpListener;
        use tokio::sync::oneshot;
        use tokio::time::timeout;

        #[tokio::test]
        async fn metrics_and_pulses_complete_before_fragmented_response_connection_closes() {
            for pulse in [false, true] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let (release, keep_open) = oneshot::channel();
                let server = tokio::spawn(async move {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        request.push(stream.read_u8().await.unwrap());
                    }
                    let path = if pulse { "/pulse" } else { "/metrics" };
                    assert_eq!(
                        request,
                        format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes()
                    );
                    let response = b"HTTP/1.1 200 OK\r\ncOnTeNt-LeNgTh: 21\r\nConnection: close\r\n\r\n{\"acks\":0,\"active\":0}";
                    for byte in response {
                        stream.write_all(&[*byte]).await.unwrap();
                        tokio::task::yield_now().await;
                    }
                    keep_open.await.unwrap();
                    drop(stream);
                });
                let value = timeout(Duration::from_secs(2), query(address, pulse))
                    .await
                    .expect("metrics must complete while the server socket remains open")
                    .unwrap();
                assert_eq!(value, json!({"acks": 0, "active": 0}));
                release.send(()).unwrap();
                server.await.unwrap();
            }
        }

        #[tokio::test]
        async fn invalid_status_length_encoding_and_json_are_rejected() {
            for response in [
                "HTTP/1.1 500 Error\r\nContent-Length: 2\r\n\r\n{}",
                "HTTP/1.1 200 OK\r\n\r\n{}",
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\n{}",
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\n{}",
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n{}",
                "HTTP/1.1 200 OK\r\nContent-Length: +2\r\n\r\n{}",
                "HTTP/1.1 200 OK\r\nContent-Length: -1\r\n\r\n{}",
                "HTTP/1.1 200 OK\r\nContent-Length: \r\n\r\n{}",
                "HTTP/1.1 200 OK\r\nContent-Length: 2, 2\r\n\r\n{}",
                "HTTP/1.1 200 OK\r\nContent-Length: 99999999999999999999999999999\r\n\r\n",
                "HTTP/1.1 200 OK\r\nMalformed\r\nContent-Length: 2\r\n\r\n{}",
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nxx",
            ] {
                assert!(
                    read_response(&mut response.as_bytes(), &mut "read")
                        .await
                        .is_err(),
                    "accepted {response:?}"
                );
            }
        }

        #[tokio::test]
        async fn truncated_headers_and_bodies_fail_at_every_boundary() {
            let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
            for boundary in 0..response.len() {
                assert!(
                    read_response(&mut &response[..boundary], &mut "read")
                        .await
                        .is_err(),
                    "accepted truncated response at {boundary}"
                );
            }
            assert_eq!(
                read_response(&mut response.as_slice(), &mut "read")
                    .await
                    .unwrap(),
                json!({})
            );
        }

        #[tokio::test]
        async fn combined_header_and_body_limit_is_checked_before_body_allocation() {
            let header = "HTTP/1.1 200 OK\r\nContent-Length: 16384\r\n\r\n";
            let length = MAX_RESPONSE_BYTES - header.len();
            let header = format!("HTTP/1.1 200 OK\r\nContent-Length: {length}\r\n\r\n");
            let response = header + "{}" + &" ".repeat(length - 2);
            assert_eq!(response.len(), MAX_RESPONSE_BYTES);
            assert_eq!(
                read_response(&mut response.as_bytes(), &mut "read")
                    .await
                    .unwrap(),
                json!({})
            );
            for response in [
                "H".repeat(MAX_RESPONSE_BYTES + 1),
                format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", length + 1),
                format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", usize::MAX),
            ] {
                assert_eq!(
                    read_response(&mut response.as_bytes(), &mut "read")
                        .await
                        .unwrap_err()
                        .to_string(),
                    "oversized origin metrics response"
                );
            }
        }
    }
}
