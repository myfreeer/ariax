//! Bounded two-racer TCP connection establishment for admitted HTTP peers.

use std::error::Error;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep_until, timeout_at};

pub const DEFAULT_HTTP_HAPPY_EYEBALLS_DELAY: Duration = Duration::from_millis(250);
pub const MAX_HTTP_HAPPY_EYEBALLS_ADDRESSES: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpHappyEyeballsConfig {
    pub connect_timeout: Duration,
    pub fallback_delay: Duration,
}

impl Default for HttpHappyEyeballsConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(30),
            fallback_delay: DEFAULT_HTTP_HAPPY_EYEBALLS_DELAY,
        }
    }
}

#[derive(Debug)]
pub struct HttpConnectedPeer {
    pub stream: TcpStream,
    pub peer: SocketAddr,
}

#[derive(Debug)]
pub enum HttpHappyEyeballsError {
    InvalidConfig,
    NoAddresses,
    TooManyAddresses,
    Timeout,
    Connect(io::Error),
}

impl HttpHappyEyeballsError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_happy_eyeballs_config",
            Self::NoAddresses => "no_destination_addresses",
            Self::TooManyAddresses => "too_many_destination_addresses",
            Self::Timeout => "connect_timeout",
            Self::Connect(_) => "connect",
        }
    }
}

impl fmt::Display for HttpHappyEyeballsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(error) => write!(formatter, "HTTP connect failed: {error}"),
            _ => formatter.write_str(self.code()),
        }
    }
}

impl Error for HttpHappyEyeballsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Connect(error) => Some(error),
            _ => None,
        }
    }
}

pub async fn connect_http_happy_eyeballs(
    addresses: &[SocketAddr],
    config: HttpHappyEyeballsConfig,
) -> Result<HttpConnectedPeer, HttpHappyEyeballsError> {
    if config.connect_timeout.is_zero() || config.fallback_delay.is_zero() {
        return Err(HttpHappyEyeballsError::InvalidConfig);
    }
    if addresses.is_empty() {
        return Err(HttpHappyEyeballsError::NoAddresses);
    }
    if addresses.len() > MAX_HTTP_HAPPY_EYEBALLS_ADDRESSES {
        return Err(HttpHappyEyeballsError::TooManyAddresses);
    }
    let deadline = Instant::now() + config.connect_timeout;
    let mut last_error = None;
    for pair in addresses.chunks(2) {
        match connect_pair(pair, deadline, config.fallback_delay).await {
            Ok(connected) => return Ok(connected),
            Err(PairError::Timeout) => return Err(HttpHappyEyeballsError::Timeout),
            Err(PairError::Connect(error)) => last_error = Some(error),
        }
    }
    Err(HttpHappyEyeballsError::Connect(last_error.unwrap_or_else(
        || {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "no connection attempts completed",
            )
        },
    )))
}

enum PairError {
    Timeout,
    Connect(io::Error),
}

async fn connect_pair(
    pair: &[SocketAddr],
    deadline: Instant,
    fallback_delay: Duration,
) -> Result<HttpConnectedPeer, PairError> {
    let first_address = pair[0];
    if pair.len() == 1 {
        return timeout_at(deadline, TcpStream::connect(first_address))
            .await
            .map_err(|_| PairError::Timeout)?
            .map(|stream| HttpConnectedPeer {
                stream,
                peer: first_address,
            })
            .map_err(PairError::Connect);
    }
    let second_address = pair[1];
    let first = TcpStream::connect(first_address);
    tokio::pin!(first);
    let delay = sleep_until((Instant::now() + fallback_delay).min(deadline));
    tokio::pin!(delay);
    tokio::select! {
        result = &mut first => match result {
            Ok(stream) => Ok(HttpConnectedPeer { stream, peer: first_address }),
            Err(first_error) => connect_after_first_failure(second_address, deadline, first_error).await,
        },
        () = &mut delay => race_started_pair(first, first_address, second_address, deadline).await,
    }
}

async fn connect_after_first_failure(
    second_address: SocketAddr,
    deadline: Instant,
    first_error: io::Error,
) -> Result<HttpConnectedPeer, PairError> {
    match timeout_at(deadline, TcpStream::connect(second_address)).await {
        Err(_) => Err(PairError::Timeout),
        Ok(Ok(stream)) => Ok(HttpConnectedPeer {
            stream,
            peer: second_address,
        }),
        Ok(Err(_second_error)) => Err(PairError::Connect(first_error)),
    }
}

async fn race_started_pair(
    mut first: std::pin::Pin<&mut impl std::future::Future<Output = io::Result<TcpStream>>>,
    first_address: SocketAddr,
    second_address: SocketAddr,
    deadline: Instant,
) -> Result<HttpConnectedPeer, PairError> {
    let second = TcpStream::connect(second_address);
    tokio::pin!(second);
    let first_result = tokio::select! {
        result = timeout_at(deadline, &mut first) => match result {
            Err(_) => return Err(PairError::Timeout),
            Ok(Ok(stream)) => return Ok(HttpConnectedPeer { stream, peer: first_address }),
            Ok(Err(error)) => error,
        },
        result = timeout_at(deadline, &mut second) => match result {
            Err(_) => return Err(PairError::Timeout),
            Ok(Ok(stream)) => return Ok(HttpConnectedPeer { stream, peer: second_address }),
            Ok(Err(_error)) => {
                return timeout_at(deadline, &mut first)
                    .await
                    .map_err(|_| PairError::Timeout)?
                    .map(|stream| HttpConnectedPeer { stream, peer: first_address })
                    .map_err(PairError::Connect);
            }
        },
    };
    timeout_at(deadline, &mut second)
        .await
        .map_err(|_| PairError::Timeout)?
        .map(|stream| HttpConnectedPeer {
            stream,
            peer: second_address,
        })
        .map_err(|_| PairError::Connect(first_result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn falls_through_a_refused_peer_to_the_second_racer() {
        let refused = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind refused");
        let refused_address = refused.local_addr().expect("address");
        drop(refused);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let accepted_address = listener.local_addr().expect("address");
        let accept = tokio::spawn(async move { listener.accept().await.expect("accept").0 });
        let connected = connect_http_happy_eyeballs(
            &[refused_address, accepted_address],
            HttpHappyEyeballsConfig {
                connect_timeout: Duration::from_secs(2),
                fallback_delay: Duration::from_millis(50),
            },
        )
        .await
        .expect("connection");
        assert_eq!(connected.peer, accepted_address);
        drop(connected.stream);
        drop(accept.await.expect("accept task"));
    }

    #[tokio::test]
    async fn rejects_empty_oversized_and_zero_duration_policies() {
        assert!(matches!(
            connect_http_happy_eyeballs(&[], HttpHappyEyeballsConfig::default()).await,
            Err(HttpHappyEyeballsError::NoAddresses)
        ));
        let addresses = vec!["127.0.0.1:1".parse().expect("address"); 33];
        assert!(matches!(
            connect_http_happy_eyeballs(&addresses, HttpHappyEyeballsConfig::default()).await,
            Err(HttpHappyEyeballsError::TooManyAddresses)
        ));
        assert!(matches!(
            connect_http_happy_eyeballs(
                &["127.0.0.1:1".parse().expect("address")],
                HttpHappyEyeballsConfig {
                    connect_timeout: Duration::ZERO,
                    fallback_delay: Duration::from_millis(250),
                }
            )
            .await,
            Err(HttpHappyEyeballsError::InvalidConfig)
        ));
    }
}
