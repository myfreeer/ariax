//! A single owned REST/RETR stream per file, with bounded control metadata.
use crate::{
    HttpCancellation, HttpIngressBudgets, HttpIngressPermit, HttpPolicyClient, HttpSourceSpec,
    HttpTaskOptions, HttpTransportCapacityPermit, ProtocolFailure, ProtocolValidator,
    TransferProtocol,
};
use ariax_storage::JournalHash;
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use suppaftp::{
    FtpError, Mode,
    tokio::{AsyncDataStream, AsyncRustlsConnector, AsyncRustlsFtpStream, AsyncRustlsStream},
    types::FileType,
};
use tokio::io::AsyncReadExt;

pub(crate) struct FtpData {
    stream: AsyncDataStream<AsyncRustlsStream>,
    _capacity: HttpTransportCapacityPermit,
}
pub(crate) struct FtpSession {
    ftp: AsyncRustlsFtpStream,
    data_capacity: Arc<Mutex<Option<HttpTransportCapacityPermit>>>,
    _control_capacity: HttpTransportCapacityPermit,
    _metadata: HttpIngressPermit,
    _listener_capacity: Option<HttpTransportCapacityPermit>,
    pub validator: ProtocolValidator,
    path: String,
    timeout: Duration,
    passive: bool,
}
impl FtpSession {
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        client: &HttpPolicyClient,
        source: &HttpSourceSpec,
        options: &HttpTaskOptions,
        metadata: &HttpIngressBudgets,
        tls: Option<Arc<rustls::ClientConfig>>,
        credentials: Option<crate::TransferCredentials>,
        cancellation: &HttpCancellation,
    ) -> Result<Self, ProtocolFailure> {
        let metadata = metadata
            .try_acquire(2 * 1024 * 1024)
            .map_err(|_| ProtocolFailure::ResourceLimit)?;
        let uri = source.uri().ok_or(ProtocolFailure::AuthFailure)?;
        let url = url::Url::parse(uri).map_err(|_| ProtocolFailure::UnsafeDestination)?;
        let host = url
            .host_str()
            .ok_or(ProtocolFailure::UnsafeDestination)?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned();
        let path = crate::transfer_task::decode_uri_component(url.path())
            .map_err(|_| ProtocolFailure::UnsafeDestination)?;
        let connection = tokio::select! {
            biased; _ = cancellation.cancelled() => return Err(ProtocolFailure::Cancelled),
            result = client.connect_protocol(uri,None,options.connect_timeout) => result?,
        };
        let peer = connection.peer;
        let proxied = connection.proxied;
        if !options.transfer.ftp_passive && proxied {
            return Err(ProtocolFailure::UnsafeDestination);
        }
        let local = connection
            .stream
            .local_addr()
            .map_err(|_| ProtocolFailure::Connect)?;
        let future = async {
            let mut ftp = if source.protocol() == TransferProtocol::Ftps {
                let connector = AsyncRustlsConnector::from(tokio_rustls::TlsConnector::from(
                    tls.clone().ok_or(ProtocolFailure::Tls)?,
                ));
                AsyncRustlsFtpStream::connect_secure_implicit_with_stream(
                    connection.stream,
                    connector,
                    &host,
                )
                .await
                .map_err(ftp_error)?
            } else {
                let ftp = AsyncRustlsFtpStream::connect_with_stream(connection.stream)
                    .await
                    .map_err(ftp_error)?;
                if options.transfer.ftp_tls {
                    let connector = AsyncRustlsConnector::from(tokio_rustls::TlsConnector::from(
                        tls.ok_or(ProtocolFailure::Tls)?,
                    ));
                    ftp.into_secure(connector, &host).await.map_err(ftp_error)?
                } else {
                    ftp
                }
            };
            let credentials = credentials.as_ref();
            let user = credentials.map_or("anonymous", |credentials| credentials.username.as_ref());
            let password = credentials
                .and_then(|credentials| credentials.password.as_deref())
                .unwrap_or("anonymous@");
            ftp.login(user, password).await.map_err(ftp_error)?;
            ftp.transfer_type(FileType::Binary)
                .await
                .map_err(ftp_error)?;
            let total = ftp.size(&path).await.map_err(|error| match error {
                FtpError::UnexpectedResponse(response)
                    if matches!(response.status as u32, 500 | 502 | 504 | 550) =>
                {
                    ProtocolFailure::SizeUnsupported
                }
                error => ftp_error(error),
            })? as u64;
            let modified = match ftp.mdtm(&path).await {
                Ok(value) => u64::try_from(value.and_utc().timestamp()).ok(),
                Err(FtpError::UnexpectedResponse(response))
                    if matches!(response.status as u32, 500 | 502 | 504 | 550) =>
                {
                    None
                }
                Err(error) => return Err(ftp_error(error)),
            };
            let validator = ProtocolValidator {
                protocol: if source.protocol() == TransferProtocol::Ftps || options.transfer.ftp_tls
                {
                    2
                } else {
                    1
                },
                source: JournalHash::new(*source.redacted_fingerprint())
                    .ok_or(ProtocolFailure::Malformed)?,
                total_length: total,
                modified_unix_seconds: modified,
                host_key: None,
            };
            Ok::<_, ProtocolFailure>((ftp, validator))
        };
        let (ftp, validator) = tokio::select! {
            biased; _ = cancellation.cancelled() => return Err(ProtocolFailure::Cancelled),
            result = tokio::time::timeout(options.connect_timeout,future) => result.map_err(|_| ProtocolFailure::Timeout)??,
        };
        let data_capacity = Arc::new(Mutex::new(None));
        let data_slot = data_capacity.clone();
        let data_client = client.clone();
        let data_uri = uri.to_owned();
        let timeout = options.connect_timeout;
        let server_address = options.transfer.ftp_pasv_server_address;
        let mut ftp = ftp.passive_stream_builder(move |advertised: SocketAddr| {
            let slot = data_slot.clone();
            let client = data_client.clone();
            let uri = data_uri.clone();
            Box::pin(async move {
                if advertised.port() == 0 {
                    return Err(FtpError::UnsafeActivePeer);
                }
                let mut url = url::Url::parse(&uri).map_err(|_| FtpError::BadResponse)?;
                url.set_port(Some(advertised.port()))
                    .map_err(|_| FtpError::BadResponse)?;
                let pinned = SocketAddr::new(
                    if server_address {
                        advertised.ip()
                    } else {
                        peer.ip()
                    },
                    advertised.port(),
                );
                if server_address {
                    url.set_host(Some(&pinned.ip().to_string()))
                        .map_err(|_| FtpError::BadResponse)?;
                }
                let owned = client
                    .connect_protocol(url.as_str(), Some(pinned), timeout)
                    .await
                    .map_err(|error| match error {
                        ProtocolFailure::ResourceLimit => FtpError::ControlLimit,
                        ProtocolFailure::UnsafeDestination => FtpError::UnsafeActivePeer,
                        _ => FtpError::ConnectionError(std::io::Error::from(
                            std::io::ErrorKind::ConnectionAborted,
                        )),
                    })?;
                let mut slot = slot.lock().map_err(|_| FtpError::ControlClosed)?;
                if slot.is_some() {
                    return Err(FtpError::DataConnectionAlreadyOpen);
                }
                *slot = Some(owned.capacity);
                Ok(owned.stream)
            })
        });
        let mut listener_capacity = None;
        if options.transfer.ftp_passive {
            ftp.set_mode(Mode::ExtendedPassive);
        } else {
            let (_, config) = client.protocol_policy();
            listener_capacity = Some(
                config
                    .direct
                    .budgets
                    .try_acquire_connection()
                    .map_err(|_| ProtocolFailure::ResourceLimit)?,
            );
            *data_capacity.lock().map_err(|_| ProtocolFailure::Control)? = Some(
                config
                    .direct
                    .budgets
                    .try_acquire_connection()
                    .map_err(|_| ProtocolFailure::ResourceLimit)?,
            );
            let listener = tokio::net::TcpListener::bind(SocketAddr::new(local.ip(), 0))
                .await
                .map_err(|_| ProtocolFailure::Connect)?;
            ftp.active_listener(
                listener,
                move |candidate| candidate.ip() == peer.ip(),
                options.connect_timeout,
            );
        }
        Ok(Self {
            ftp,
            data_capacity,
            _control_capacity: connection.capacity,
            _metadata: metadata,
            _listener_capacity: listener_capacity,
            validator,
            path,
            timeout: options.response_body_timeout,
            passive: options.transfer.ftp_passive,
        })
    }
    pub async fn retrieve(&mut self, offset: u64) -> Result<FtpData, ProtocolFailure> {
        let path = self.path.clone();
        let future = async {
            if offset != 0 {
                self.ftp
                    .resume_transfer(
                        usize::try_from(offset).map_err(|_| ProtocolFailure::ResumeUnsupported)?,
                    )
                    .await
                    .map_err(|_| ProtocolFailure::ResumeUnsupported)?;
            }
            match self.ftp.retr_as_stream(&path).await {
                Ok(stream) => Ok(stream),
                Err(FtpError::UnexpectedResponse(response))
                    if self.passive && matches!(response.status as u32, 500 | 502 | 504 | 522) =>
                {
                    self.ftp.set_mode(Mode::Passive);
                    self.ftp.retr_as_stream(&path).await.map_err(ftp_error)
                }
                Err(error) => Err(ftp_error(error)),
            }
        };
        let stream = tokio::time::timeout(self.timeout, future)
            .await
            .map_err(|_| ProtocolFailure::Timeout)??;
        let capacity = self
            .data_capacity
            .lock()
            .map_err(|_| ProtocolFailure::Control)?
            .take()
            .ok_or(ProtocolFailure::ResourceLimit)?;
        Ok(FtpData {
            stream,
            _capacity: capacity,
        })
    }
    pub async fn read(
        stream: &mut FtpData,
        bytes: &mut [u8],
        timeout: Duration,
        cancellation: &HttpCancellation,
    ) -> Result<usize, ProtocolFailure> {
        tokio::select! { biased; _ = cancellation.cancelled() => Err(ProtocolFailure::Cancelled),
        result = tokio::time::timeout(timeout,stream.stream.read(bytes)) => result.map_err(|_| ProtocolFailure::Timeout)?.map_err(|_| ProtocolFailure::Data) }
    }
    pub async fn finish(&mut self, stream: FtpData) -> Result<(), ProtocolFailure> {
        let FtpData { stream, _capacity } = stream;
        let result = tokio::time::timeout(self.timeout, self.ftp.finalize_retr_stream(stream))
            .await
            .map_err(|_| ProtocolFailure::Timeout)?
            .map_err(ftp_error);
        drop(_capacity);
        result
    }
}
fn ftp_error(error: FtpError) -> ProtocolFailure {
    match error {
        FtpError::ControlLimit => ProtocolFailure::ResourceLimit,
        FtpError::ControlClosed | FtpError::BadResponse => ProtocolFailure::Malformed,
        FtpError::UnsafeActivePeer => ProtocolFailure::UnsafeDestination,
        FtpError::SecureError(_) => ProtocolFailure::Tls,
        FtpError::ConnectionError(_) => ProtocolFailure::Control,
        FtpError::UnexpectedResponse(response)
            if matches!(response.status as u32, 430 | 530 | 532) =>
        {
            ProtocolFailure::AuthFailure
        }
        FtpError::UnexpectedResponse(response) if response.status as u32 >= 500 => {
            ProtocolFailure::Malformed
        }
        _ => ProtocolFailure::Control,
    }
}
