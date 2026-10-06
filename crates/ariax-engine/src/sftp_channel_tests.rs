use russh::keys::ssh_key::encoding::Decode;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct Peer;

impl russh::server::Handler for Peer {
    type Error = russh::Error;

    async fn auth_none(&mut self, _: &str) -> Result<russh::server::Auth, Self::Error> {
        Ok(russh::server::Auth::Accept)
    }

    async fn auth_succeeded(
        &mut self,
        session: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        let unknown = russh::ChannelId::decode(&mut &u32::MAX.to_be_bytes()[..]).unwrap();
        // A fixed, small probe, not an unbounded flooding test.
        for _ in 0..256 {
            session.channel_open_failure(
                unknown,
                russh::ChannelOpenFailure::AdministrativelyProhibited,
                "unknown channel",
                "en",
            )?;
        }
        Ok(())
    }

    async fn channel_open_session(
        &mut self,
        _: russh::Channel<russh::server::Msg>,
        reply: russh::server::ChannelOpenHandle,
        _: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        reply
            .reject(russh::ChannelOpenFailure::AdministrativelyProhibited)
            .await;
        Ok(())
    }
}

struct Client {
    unknown: Arc<AtomicUsize>,
    known: Arc<AtomicUsize>,
}

impl russh::client::Handler for Client {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _: &russh::keys::ssh_key::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }

    async fn channel_open_failure(
        &mut self,
        channel: russh::ChannelId,
        _: russh::ChannelOpenFailure,
        _: &str,
        _: &str,
        _: &mut russh::client::Session,
    ) -> Result<(), Self::Error> {
        if channel.number() == u32::MAX {
            self.unknown.fetch_add(1, Ordering::SeqCst);
        } else {
            self.known.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[tokio::test]
async fn unknown_channel_open_failures_are_dropped_and_pending_open_rejections_are_delivered() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let config = russh::server::Config {
                keys: vec![
                    russh::keys::decode_secret_key(
                        include_str!("../tests/fixtures/sftp-test-host"),
                        None,
                    )
                    .unwrap(),
                ],
                auth_rejection_time: Duration::ZERO,
                auth_rejection_time_initial: Some(Duration::ZERO),
                ..Default::default()
            };
            russh::server::run_stream(Arc::new(config), socket, Peer)
                .await
                .unwrap()
                .await
        });
        let unknown = Arc::new(AtomicUsize::new(0));
        let known = Arc::new(AtomicUsize::new(0));
        let mut client = russh::client::connect(
            Arc::new(russh::client::Config::default()),
            address,
            Client {
                unknown: unknown.clone(),
                known: known.clone(),
            },
        )
        .await
        .unwrap();
        assert!(client.authenticate_none("probe").await.unwrap().success());
        let rejected = client.channel_open_session().await;
        while known.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        client
            .disconnect(russh::Disconnect::ByApplication, "done", "en")
            .await
            .unwrap();
        drop(client);
        server.await.unwrap().unwrap();
        assert!(matches!(rejected, Err(russh::Error::ChannelOpenFailure(_))));
        assert_eq!(known.load(Ordering::SeqCst), 1);
        assert_eq!(unknown.load(Ordering::SeqCst), 0);
    })
    .await
    .expect("bounded SSH channel probe");
}
