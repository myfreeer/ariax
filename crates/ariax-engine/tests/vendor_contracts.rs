//! Exercise the patched dependency APIs through the resolved production graph.
#![forbid(unsafe_code)]

#[cfg(feature = "sftp")]
mod sftp {
    use russh_sftp::client::{Config, MAX_PENDING_REQUESTS, RawSftpSession, error::Error};
    use std::{sync::Arc, time::Duration};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn framing_rejects_before_payload_and_closes_all_outstanding_requests() {
        assert_eq!(russh_sftp::client::ARIAX_BOUNDED_PROTOCOL_REVISION, 1);
        for frame in [
            &[0, 0, 0, 65][..],
            &[0, 0, 0, 1, 255],
            &[0, 0, 0, 6, 2, 0, 0, 0, 3, 1],
        ] {
            let (client, mut peer) = tokio::io::duplex(128);
            let raw = Arc::new(RawSftpSession::new_with_config(
                client,
                Config {
                    max_packet_len: 64,
                    ..Default::default()
                },
            ));
            let initialized = {
                let raw = raw.clone();
                tokio::spawn(async move { raw.init().await })
            };
            peer.write_all(frame).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_secs(1), initialized)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_err()
            );
            assert!(raw.is_closed());
            assert_eq!(raw.pending_requests(), 0);
        }
        let (client, mut peer) = tokio::io::duplex(8192);
        let raw = Arc::new(RawSftpSession::new(client));
        let init = {
            let raw = raw.clone();
            tokio::spawn(async move { raw.init().await })
        };
        let length = peer.read_u32().await.unwrap();
        peer.read_exact(&mut vec![0; length as usize])
            .await
            .unwrap();
        peer.write_all(&[0, 0, 0, 5, 2, 0, 0, 0, 3]).await.unwrap();
        init.await.unwrap().unwrap();
        let mut pending = tokio::task::JoinSet::new();
        for _ in 0..=MAX_PENDING_REQUESTS {
            let raw = raw.clone();
            pending.spawn(async move { raw.fstat(b"\xff\0handle".as_slice()).await });
        }
        let result = tokio::time::timeout(Duration::from_secs(1), pending.join_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(Error::RequestLimit)));
        assert_eq!(raw.pending_requests(), MAX_PENDING_REQUESTS);
        raw.close_session().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(result) = pending.join_next().await {
                assert!(matches!(result.unwrap(), Err(Error::SessionClosed)));
            }
        })
        .await
        .unwrap();
        assert_eq!(raw.pending_requests(), 0);
    }
}

#[cfg(feature = "ftp")]
mod ftp {
    use std::{sync::Mutex, time::Duration};
    use suppaftp::{FtpError, Status, tokio::AsyncFtpStream};
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
    };

    struct Capture(Mutex<Vec<String>>);
    static LOG: Capture = Capture(Mutex::new(Vec::new()));
    impl log::Log for Capture {
        fn enabled(&self, _: &log::Metadata<'_>) -> bool {
            true
        }
        fn log(&self, record: &log::Record<'_>) {
            self.0.lock().unwrap().push(record.args().to_string());
        }
        fn flush(&self) {}
    }

    #[tokio::test]
    async fn ftp_reply_limits_close_poisoned_channels_and_logs_redact_all_text() {
        assert_eq!(suppaftp::ARIAX_BOUNDED_PROTOCOL_REVISION, 1);
        log::set_logger(&LOG).unwrap();
        for level in [
            log::LevelFilter::Error,
            log::LevelFilter::Warn,
            log::LevelFilter::Info,
            log::LevelFilter::Debug,
            log::LevelFilter::Trace,
        ] {
            log::set_max_level(level);
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut tcp, _) = listener.accept().await.unwrap();
                tcp.write_all(b"220 greeting-CANARY\r\n").await.unwrap();
                let mut stream = BufReader::new(tcp);
                let mut passive = None;
                loop {
                    let mut line = String::new();
                    if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
                        break;
                    }
                    let response = match line.split_whitespace().next().unwrap() {
                        "USER" => "331 user-CANARY\r\n".to_owned(),
                        "PASS" => "230 pass-CANARY\r\n".into(),
                        "ACCT" | "SITE" | "OP" => "200 reply-CANARY\r\n".into(),
                        "CWD" => "250 path-CANARY\r\n".into(),
                        "FEAT" => "211-feat-CANARY\r\n CANARY\r\n211 done-CANARY\r\n".into(),
                        "EPSV" => {
                            let data = TcpListener::bind("127.0.0.1:0").await.unwrap();
                            let response = format!(
                                "229 passive (|||{}|)\r\n",
                                data.local_addr().unwrap().port()
                            );
                            passive = Some(data);
                            response
                        }
                        "NLST" => {
                            stream
                                .get_mut()
                                .write_all(b"150 data-CANARY\r\n")
                                .await
                                .unwrap();
                            let (mut data, _) = passive.take().unwrap().accept().await.unwrap();
                            data.write_all(b"listing-CANARY\r\n").await.unwrap();
                            drop(data);
                            "226 done-CANARY\r\n".into()
                        }
                        _ => "500 error-CANARY\r\n".into(),
                    };
                    if stream
                        .get_mut()
                        .write_all(response.as_bytes())
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let tcp = TcpStream::connect(address).await.unwrap();
            let mut ftp = AsyncFtpStream::connect_with_stream(tcp).await.unwrap();
            ftp.login("USER-CANARY", "PASS-CANARY").await.unwrap();
            ftp.custom_command("ACCT account-CANARY", &[Status::CommandOk])
                .await
                .unwrap();
            ftp.site("site-CANARY").await.unwrap();
            ftp.custom_command("OP custom-CANARY", &[Status::CommandOk])
                .await
                .unwrap();
            ftp.cwd("path-CANARY").await.unwrap();
            assert!(ftp.feat().await.unwrap().contains_key("CANARY"));
            ftp.set_mode(suppaftp::Mode::ExtendedPassive);
            assert_eq!(ftp.nlst(None).await.unwrap(), vec!["listing-CANARY"]);
            drop(ftp);
            server.await.unwrap();
            assert!(
                LOG.0
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|line| !line.contains("CANARY"))
            );
        }
        for case in 0..5 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let reply = match case {
                0 => vec![b'x'; suppaftp::MAX_CONTROL_LINE + 1],
                1 => [
                    b"220-many\r\n".to_vec(),
                    b" x\r\n".repeat(suppaftp::MAX_CONTROL_LINES),
                ]
                .concat(),
                2 => [
                    b"220-long\r\n".to_vec(),
                    [vec![b' '; suppaftp::MAX_CONTROL_LINE - 2], b"\r\n".to_vec()]
                        .concat()
                        .repeat(17),
                ]
                .concat(),
                3 => b"220-first\r\n221 wrong\r\n".to_vec(),
                _ => b"220 invalid-LF\n".to_vec(),
            };
            let server = tokio::spawn(async move {
                let (mut peer, _) = listener.accept().await.unwrap();
                let _ = peer.write_all(&reply).await;
                // Over-limit frames must close the peer, without waiting for more bytes.
                let _ = tokio::time::timeout(Duration::from_secs(1), peer.read_u8())
                    .await
                    .unwrap();
            });
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                AsyncFtpStream::connect_with_stream(TcpStream::connect(address).await.unwrap()),
            )
            .await
            .unwrap();
            assert!(
                matches!(result, Err(FtpError::ControlLimit | FtpError::BadResponse)),
                "case {case}"
            );
            server.await.unwrap();
        }
    }
}
