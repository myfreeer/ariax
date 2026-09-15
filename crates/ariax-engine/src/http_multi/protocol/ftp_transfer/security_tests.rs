use super::super::tests::Directory;
use super::*;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpSocket};

trait ControlIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ControlIo for T {}

// These are real TLS control/data channels. A peer from 127.0.0.2 sends a
// canary before the approved active peer connects; it must never reach TLS.
async fn server(
    scenario: u8,
    commands: Arc<Mutex<Vec<String>>>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let implicit = matches!(scenario, 0 | 2 | 5 | 6);
    let acceptor =
        tokio_rustls::TlsAcceptor::from(crate::http_transport::tests::test_server_config());
    let server = tokio::spawn(async move {
        loop {
            let (tcp, _) = listener.accept().await.unwrap();
            let commands = commands.clone();
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let mut stream: Box<dyn ControlIo> = if implicit {
                    match acceptor.accept(tcp).await {
                        Ok(tls) => Box::new(tls),
                        Err(_) => return,
                    }
                } else {
                    Box::new(tcp)
                };
                if stream.write_all(b"220 ready\r\n").await.is_err() {
                    return;
                }
                let mut control = BufReader::new(stream);
                let mut passive = None;
                let mut active = None;
                let mut protected = false;
                loop {
                    let mut line = String::new();
                    if control.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    let (verb, arg) = line
                        .trim_end()
                        .split_once(' ')
                        .unwrap_or((line.trim_end(), ""));
                    commands.lock().unwrap().push(verb.to_owned());
                    let response = match verb {
                        "AUTH" => {
                            if control.get_mut().write_all(b"234 TLS\r\n").await.is_err() {
                                return;
                            }
                            match acceptor.accept(control.into_inner()).await {
                                Ok(tls) => control = BufReader::new(Box::new(tls)),
                                Err(_) => return,
                            }
                            continue;
                        }
                        "PBSZ" => "200 buffer\r\n".to_owned(),
                        "PROT" if scenario == 3 => "534 protection required\r\n".to_owned(),
                        "PROT" => {
                            assert_eq!(arg, "P");
                            protected = true;
                            "200 private\r\n".into()
                        }
                        "USER" => "331 password\r\n".into(),
                        "PASS" => "230 accepted\r\n".into(),
                        "TYPE" => {
                            assert_eq!(arg, "I");
                            "200 binary\r\n".into()
                        }
                        "SIZE" => "213 12\r\n".into(),
                        "MDTM" => "213 20260915000000\r\n".into(),
                        "EPSV" => {
                            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                            let port = listener.local_addr().unwrap().port();
                            passive = Some(listener);
                            format!("229 passive (|||{port}|)\r\n")
                        }
                        "EPRT" => {
                            let parts: Vec<_> = arg.split('|').collect();
                            active = Some(
                                format!("{}:{}", parts[2], parts[3])
                                    .parse::<std::net::SocketAddr>()
                                    .unwrap(),
                            );
                            "200 active\r\n".into()
                        }
                        "PORT" => {
                            let parts: Vec<u16> =
                                arg.split(',').map(|part| part.parse().unwrap()).collect();
                            active = Some(
                                format!(
                                    "{}.{}.{}.{}:{}",
                                    parts[0],
                                    parts[1],
                                    parts[2],
                                    parts[3],
                                    parts[4] * 256 + parts[5]
                                )
                                .parse()
                                .unwrap(),
                            );
                            "200 active\r\n".into()
                        }
                        "RETR" => {
                            assert!(protected, "FTPS must negotiate private data protection");
                            if control
                                .get_mut()
                                .write_all(b"150 opening\r\n")
                                .await
                                .is_err()
                            {
                                return;
                            }
                            let data = if let Some(address) = active {
                                for _ in 0..if scenario == 6 { 32 } else { 1 } {
                                    let socket = TcpSocket::new_v4().unwrap();
                                    socket.bind("127.0.0.2:0".parse().unwrap()).unwrap();
                                    let mut rejected = socket.connect(address).await.unwrap();
                                    let _ =
                                        rejected.write_all(b"unapproved active peer canary").await;
                                }
                                if scenario == 6 {
                                    return;
                                }
                                tokio::net::TcpStream::connect(address).await.unwrap()
                            } else {
                                passive.take().unwrap().accept().await.unwrap().0
                            };
                            if scenario == 4 {
                                let mut data = data;
                                let _ = data.write_all(b"plaintext must never be accepted").await;
                            } else {
                                match acceptor.accept(data).await {
                                    Ok(mut tls) => {
                                        let _ = tls.write_all(b"abcdefghijkl").await;
                                        let _ = tls.shutdown().await;
                                    }
                                    Err(_) => return,
                                }
                            }
                            "226 complete\r\n".into()
                        }
                        _ => "500 unsupported\r\n".into(),
                    };
                    if control
                        .get_mut()
                        .write_all(response.as_bytes())
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });
    (
        format!(
            "{}://user:secret@localhost:{}/file",
            if implicit { "ftps" } else { "ftp" },
            address.port()
        ),
        server,
    )
}

#[tokio::test]
async fn ftps_requires_private_data_trust_and_pre_tls_active_peer_authorization() {
    for scenario in 0..7 {
        let directory = Directory::new();
        let commands = Arc::new(Mutex::new(Vec::new()));
        let (uri, server) = server(scenario, commands.clone()).await;
        let ca = directory.0.join("root.pem");
        std::fs::write(&ca, crate::http_transport::tests::TEST_ROOT_CERTIFICATE_PEM).unwrap();
        let mut options = crate::HttpTaskOptions {
            connect_timeout: Duration::from_secs(2),
            response_body_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        options.transfer.ftp_tls = true;
        options.transfer.ftp_passive = !matches!(scenario, 2 | 6);
        let manifest = Arc::new(
            VerificationManifest::new(
                12,
                3,
                b"abcdefghijkl"
                    .chunks(3)
                    .map(|bytes| {
                        let mut hash =
                            ContentHasher::new(ariax_storage::JournalDigestAlgorithm::Sha256);
                        hash.update(bytes);
                        hash.finalize().journal_digest()
                    })
                    .collect(),
                vec![],
            )
            .unwrap(),
        );
        let spec = HttpTaskSpec::new(
            TaskId::new(1).unwrap(),
            Gid::new(1).unwrap(),
            [uri],
            directory.0.clone(),
            ariax_storage::SafePathBuilder::from_user_path(
                "result",
                ariax_storage::PathPlatform::current(),
            )
            .unwrap(),
            options,
            false,
        )
        .unwrap()
        .with_verification(manifest, None)
        .unwrap();
        let mut tls = crate::HttpTlsPolicy::default();
        if scenario != 5 {
            tls.trust = crate::HttpTrustSource::CustomPem(ca);
        }
        let config = HttpMultiRangeWorkerConfig {
            journal_root: directory.0.join("journals"),
            ..Default::default()
        };
        let metadata = config.protocol_metadata.clone();
        let client = HttpPolicyClient::new(
            crate::HttpResolver::new(Default::default()).unwrap(),
            crate::HttpPolicyClientConfig {
                destination: crate::HttpDestinationPolicy {
                    allow_loopback: true,
                    ..Default::default()
                },
                direct: crate::HttpDirectTransportConfig {
                    tls,
                    budgets: crate::HttpTransportBudgets::new(
                        8,
                        8 * crate::HTTP_CONNECTION_RESERVATION_BYTES,
                    )
                    .unwrap(),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let worker = HttpMultiRangeWorker::new(
            client,
            config,
            SharedHttpTransferStats::new(NonZeroUsize::new(4).unwrap()),
        )
        .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            worker.run_task(Arc::new(spec), Generation::INITIAL, HttpCancellation::new()),
        )
        .await
        .unwrap_or_else(|error| panic!("scenario {scenario}: {error}"));
        server.abort();
        assert_eq!(metadata.used(), 0);
        if scenario <= 2 {
            result.unwrap_or_else(|error| panic!("scenario {scenario}: {error}"));
            assert_eq!(
                std::fs::read(directory.0.join("result")).unwrap(),
                b"abcdefghijkl"
            );
        } else {
            assert!(result.is_err(), "scenario {scenario}: {result:?}");
            if matches!(scenario, 3 | 5) {
                assert!(
                    !commands
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|command| command == "RETR")
                );
            }
        }
    }
}
