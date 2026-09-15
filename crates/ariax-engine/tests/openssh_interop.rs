//! Opt-in interoperability with a separately provisioned loopback OpenSSH.
#![forbid(unsafe_code)]
#![cfg(feature = "sftp")]
use ariax_core::{Generation, Gid, TaskId};
use ariax_engine::*;
use ariax_storage::{JournalDigestAlgorithm, PathPlatform, SafePathBuilder};
use std::{num::NonZeroUsize, path::PathBuf, sync::Arc, time::Duration};

#[tokio::test]
#[ignore = "run scripts/run-openssh-interop.py with a private loopback sshd"]
async fn openssh_public_key_offsets_and_final_attributes_interoperate() {
    let uri = std::env::var("ARIAX_OPENSSH_URI").expect("provision a private OpenSSH fixture");
    let root = PathBuf::from(std::env::var_os("ARIAX_OPENSSH_OUTPUT").expect("fixture output"));
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sftp-test-host");
    let mut options = HttpTaskOptions {
        connect_timeout: Duration::from_secs(3),
        response_body_timeout: Duration::from_secs(3),
        ..Default::default()
    };
    options.transfer.sftp_private_key = Some(PathBuf::from(
        std::env::var_os("ARIAX_OPENSSH_KEY").expect("private client key copy"),
    ));
    options.transfer.sftp_known_hosts = Some(PathBuf::from(
        std::env::var_os("ARIAX_OPENSSH_KNOWN_HOSTS").expect("private fixture known-hosts file"),
    ));
    options.transfer.sftp_host_key = Some(
        std::fs::read_to_string(fixture.with_extension("pub"))
            .unwrap()
            .trim()
            .to_owned(),
    );
    let manifest = Arc::new(
        VerificationManifest::new(
            12,
            3,
            b"abcdefghijkl"
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
    let task = TaskId::new(1).unwrap();
    let spec = HttpTaskSpec::new(
        task,
        Gid::new(1).unwrap(),
        [uri],
        root.clone(),
        SafePathBuilder::from_user_path("result", PathPlatform::current()).unwrap(),
        options,
        false,
    )
    .unwrap()
    .with_verification(manifest, None)
    .unwrap();
    let config = HttpMultiRangeWorkerConfig {
        journal_root: root.join("journals"),
        ..Default::default()
    };
    let metadata = config.protocol_metadata.clone();
    let ingress = config.sftp_ingress.clone();
    let stats = SharedHttpTransferStats::new(NonZeroUsize::new(2).unwrap());
    let client = HttpPolicyClient::new(
        HttpResolver::new(Default::default()).unwrap(),
        HttpPolicyClientConfig {
            destination: HttpDestinationPolicy {
                allow_loopback: true,
                ..Default::default()
            },
            direct: HttpDirectTransportConfig {
                budgets: HttpTransportBudgets::new(8, 8 * HTTP_CONNECTION_RESERVATION_BYTES)
                    .unwrap(),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let worker = HttpMultiRangeWorker::new(client, config, stats.clone()).unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        worker.run_task(Arc::new(spec), Generation::INITIAL, HttpCancellation::new()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(std::fs::read(root.join("result")).unwrap(), b"abcdefghijkl");
    let status = stats.get(task).unwrap().snapshot();
    assert_eq!(status.durable_bytes, 12);
    assert_eq!(status.ssh_connection.unwrap().host_key, "ssh-ed25519");
    assert_eq!(metadata.used(), 0);
    assert_eq!(ingress.used(), 0);
}
