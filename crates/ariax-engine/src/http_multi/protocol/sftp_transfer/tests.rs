use super::super::tests::{Directory, server as http_server};
use super::*;
use crate::{HttpPolicyClientConfig, HttpTaskOptions};
use russh::{
    Channel, ChannelId,
    server::{Auth, Msg, Session},
};
use russh_sftp::protocol::{
    Attrs, Data, FileAttributes, Handle, Packet, Status, StatusCode, Version,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

struct Server {
    channels: BTreeMap<ChannelId, Channel<Msg>>,
    auth: Arc<AtomicU64>,
    reads: Arc<AtomicU64>,
    scenario: u8,
    close_once: Arc<AtomicBool>,
}
impl russh::server::Handler for Server {
    type Error = russh::Error;
    async fn auth_keyboard_interactive<'a>(
        &'a mut self,
        _user: &str,
        _submethods: &str,
        response: Option<russh::server::Response<'a>>,
    ) -> Result<Auth, Self::Error> {
        use std::borrow::Cow;
        if let Some(mut response) = response {
            self.auth.fetch_add(1, Ordering::SeqCst);
            if self.scenario != 12 && response.next().as_deref() == Some(b"secret") {
                return Ok(Auth::Accept);
            }
        }
        let mut prompts = vec![(Cow::Borrowed("Password:"), self.scenario == 10)];
        if self.scenario == 11 {
            prompts.push((Cow::Borrowed("OTP:"), false));
        }
        Ok(Auth::Partial {
            name: Cow::Borrowed(""),
            instructions: Cow::Borrowed(""),
            prompts: Cow::Owned(prompts),
        })
    }
    async fn auth_password(&mut self, _user: &str, password: &str) -> Result<Auth, Self::Error> {
        self.auth.fetch_add(1, Ordering::SeqCst);
        Ok(if password == "secret" {
            Auth::Accept
        } else {
            Auth::Reject {
                proceed_with_methods: None,
                partial_success: false,
            }
        })
    }
    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: russh::server::ChannelOpenHandle,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.channels.insert(channel.id(), channel);
        reply.accept().await;
        Ok(())
    }
    async fn subsystem_request(
        &mut self,
        id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if name != "sftp" {
            session.channel_failure(id)?;
            return Ok(());
        }
        let channel = self.channels.remove(&id).unwrap();
        session.channel_success(id)?;
        let reads = self.reads.clone();
        let scenario = self.scenario;
        let close_once = self.close_once.clone();
        tokio::spawn(async move {
            let (mut input, output) = tokio::io::split(channel.into_stream());
            let output = Arc::new(tokio::sync::Mutex::new(output));
            let mut fstats = 0;
            while let Ok(length) = input.read_u32().await {
                if length > 4096 {
                    break;
                }
                let mut bytes = vec![0; length as usize];
                if input.read_exact(&mut bytes).await.is_err() {
                    break;
                }
                let mut bytes = bytes::Bytes::from(bytes);
                let request = Packet::try_from(&mut bytes).unwrap();
                let mut delay = false;
                let response: Packet = match request {
                    Packet::Init(_) => Version::new().into(),
                    Packet::Open(request) => Handle {
                        id: request.id,
                        handle: vec![0xff, 0, 0x81],
                    }
                    .into(),
                    Packet::Fstat(request) => {
                        fstats += 1;
                        assert_eq!(request.handle, [0xff, 0, 0x81]);
                        Attrs {
                            id: request.id,
                            attrs: FileAttributes {
                                size: Some(if scenario == 7 {
                                    0
                                } else if scenario == 8 && fstats > 1 {
                                    13
                                } else {
                                    12
                                }),
                                mtime: Some(42),
                                ..Default::default()
                            },
                        }
                        .into()
                    }
                    Packet::Read(request) => {
                        assert_eq!(request.handle, [0xff, 0, 0x81]);
                        reads.fetch_add(1, Ordering::SeqCst);
                        if scenario == 3 {
                            let _ = output.lock().await.write_u32(1025).await;
                            break;
                        }
                        if scenario == 5 && close_once.swap(false, Ordering::SeqCst) {
                            break;
                        }
                        if scenario == 6 {
                            let packet: Packet = Data {
                                id: request.id,
                                data: vec![42; request.len as usize + 1],
                            }
                            .into();
                            let bytes = bytes::Bytes::try_from(packet).unwrap();
                            let _ = output.lock().await.write_all(&bytes).await;
                            break;
                        }
                        let start = request.offset as usize;
                        let end = (start + request.len.min(1) as usize).min(12);
                        delay = start.is_multiple_of(3);
                        Data {
                            id: request.id,
                            data: b"abcdefghijkl"[start..end].to_vec(),
                        }
                        .into()
                    }
                    Packet::Close(request) => Status {
                        id: request.id,
                        status_code: StatusCode::Ok,
                        error_message: String::new(),
                        language_tag: String::new(),
                    }
                    .into(),
                    _ => panic!("unexpected SFTP test request"),
                };
                let output = output.clone();
                tokio::spawn(async move {
                    if delay {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    let bytes = bytes::Bytes::try_from(response).unwrap();
                    let _ = output.lock().await.write_all(&bytes).await;
                });
            }
        });
        Ok(())
    }
}

async fn server(
    scenario: u8,
) -> (
    String,
    String,
    Arc<AtomicU64>,
    Arc<AtomicU64>,
    tokio::task::JoinHandle<()>,
) {
    let key = russh::keys::decode_secret_key(
        include_str!("../../../../tests/fixtures/sftp-test-host"),
        None,
    )
    .unwrap();
    let blob = key.public_key().to_bytes().unwrap();
    let pin = ariax_storage::session_host_key_pin_value(
        ariax_core::HostKeyFingerprint::for_presented_key(&blob),
    );
    let config = Arc::new(russh::server::Config {
        keys: vec![key],
        methods: if (9..=12).contains(&scenario) {
            [russh::MethodKind::KeyboardInteractive].as_slice().into()
        } else {
            [russh::MethodKind::Password].as_slice().into()
        },
        auth_rejection_time: Duration::ZERO,
        auth_rejection_time_initial: Some(Duration::ZERO),
        ..Default::default()
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let auth = Arc::new(AtomicU64::new(0));
    let counter = auth.clone();
    let reads = Arc::new(AtomicU64::new(0));
    let reads_counter = reads.clone();
    let close_once = Arc::new(AtomicBool::new(true));
    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let config = config.clone();
            let auth = counter.clone();
            let reads = reads_counter.clone();
            let close_once = close_once.clone();
            tokio::spawn(async move {
                if let Ok(session) = russh::server::run_stream(
                    config,
                    stream,
                    Server {
                        channels: BTreeMap::new(),
                        auth,
                        reads,
                        scenario,
                        close_once,
                    },
                )
                .await
                {
                    let _ = session.await;
                }
            });
        }
    });
    (
        format!("sftp://user:secret@{address}/file"),
        pin,
        auth,
        reads,
        server,
    )
}

#[tokio::test]
async fn sftp_trust_precedes_authentication_and_bounded_reads_share_http_ranges() {
    for scenario in 0..14 {
        let known_hosts = crate::sftp_trust::TestKnownHosts::new();
        let directory = Directory::new();
        let (uri, pin, auth, reads, server) = server(scenario).await;
        let (http, http_server) = http_server(false).await;
        let mut options = HttpTaskOptions::default();
        options.transfer.sftp_known_hosts = Some(known_hosts.0.clone());
        options.transfer.sftp_max_read_size = 2;
        options.transfer.sftp_max_packet_size = 1024;
        options.retry = Some(HttpRetryPolicy {
            base_wait: Duration::from_millis(1),
            ..Default::default()
        });
        if scenario != 0 {
            options.transfer.sftp_host_key_sha256 =
                Some(if scenario == 1 { "00".repeat(32) } else { pin });
        }
        if scenario == 13 {
            options.transfer.sftp_private_key =
                Some(directory.0.join("must-not-open-unoffered-key"));
        }
        let payload: &[u8] = if scenario == 7 { b"" } else { b"abcdefghijkl" };
        let uris = if scenario == 4 {
            vec![http, uri]
        } else {
            vec![uri]
        };
        let manifest = Arc::new(
            VerificationManifest::new(
                payload.len() as u64,
                3,
                payload
                    .chunks(3)
                    .map(|bytes| {
                        let mut hash = ContentHasher::new(JournalDigestAlgorithm::Sha512);
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
            uris,
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
        let client = HttpPolicyClient::new(
            crate::HttpResolver::new(Default::default()).unwrap(),
            HttpPolicyClientConfig {
                destination: crate::HttpDestinationPolicy {
                    allow_loopback: true,
                    ..Default::default()
                },
                direct: crate::HttpDirectTransportConfig {
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
        let stats = SharedHttpTransferStats::new(NonZeroUsize::new(4).unwrap());
        let config = HttpMultiRangeWorkerConfig {
            journal_root: directory.0.join("journals"),
            ..Default::default()
        };
        let metadata = config.protocol_metadata.clone();
        let ingress = config.sftp_ingress.clone();
        let worker = HttpMultiRangeWorker::new(client, config, stats.clone()).unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            worker.run_task(Arc::new(spec), Generation::INITIAL, HttpCancellation::new()),
        )
        .await
        .unwrap_or_else(|error| panic!("scenario {scenario}: {error}"));
        server.abort();
        http_server.abort();
        assert_eq!(metadata.used(), 0);
        assert_eq!(ingress.used(), 0);
        match scenario {
            0 => {
                assert!(
                    matches!(result, Err(HttpMultiRangeError::HostKeyChallenge(_))),
                    "{result:?}"
                );
                assert_eq!(auth.load(Ordering::SeqCst), 0);
            }
            1 => {
                assert!(
                    matches!(
                        result,
                        Err(HttpMultiRangeError::Transfer(
                            ProtocolFailure::HostKeyMismatch
                        ))
                    ),
                    "{result:?}"
                );
                assert_eq!(auth.load(Ordering::SeqCst), 0);
            }
            3 => assert!(
                matches!(
                    result,
                    Err(HttpMultiRangeError::Transfer(
                        ProtocolFailure::ResourceLimit
                    ))
                ),
                "{result:?}"
            ),
            6 => assert!(
                matches!(
                    result,
                    Err(HttpMultiRangeError::Transfer(ProtocolFailure::Malformed))
                ),
                "{result:?}"
            ),
            7 => {
                result.unwrap();
                assert!(
                    std::fs::read(directory.0.join("result"))
                        .unwrap()
                        .is_empty()
                );
                assert_eq!(reads.load(Ordering::SeqCst), 0);
            }
            8 => assert!(
                matches!(
                    result,
                    Err(HttpMultiRangeError::Transfer(
                        ProtocolFailure::StaleValidator
                    ))
                ),
                "{result:?}"
            ),
            10..=12 => {
                assert!(
                    matches!(
                        result,
                        Err(HttpMultiRangeError::Transfer(ProtocolFailure::AuthFailure))
                    ),
                    "{result:?}"
                );
                assert_eq!(reads.load(Ordering::SeqCst), 0);
                assert_eq!(auth.load(Ordering::SeqCst), u64::from(scenario == 12));
            }
            5 => {
                result.unwrap();
                assert_eq!(
                    std::fs::read(directory.0.join("result")).unwrap(),
                    b"abcdefghijkl"
                );
                assert!(auth.load(Ordering::SeqCst) >= 2);
            }
            _ => {
                result.unwrap();
                assert_eq!(
                    std::fs::read(directory.0.join("result")).unwrap(),
                    b"abcdefghijkl"
                );
                assert!(reads.load(Ordering::SeqCst) > 0);
                assert_eq!(auth.load(Ordering::SeqCst), 1);
            }
        }
    }
}
