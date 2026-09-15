//! Owned SSH transport and bounded raw SFTP sessions.
use crate::protocol_transport::ProtocolStreamGuard;
use crate::sftp_trust::HostTrust;
use crate::{
    HttpCancellation, HttpIngressBudgets, HttpIngressPermit, HttpMultiRangeError, HttpPolicyClient,
    HttpSourceSpec, HttpTaskSpec, ProtocolFailure, ProtocolValidator,
};
use ariax_core::{Generation, PresentedHostKeyChallenge};
use ariax_runtime::CpuPool;
use ariax_storage::JournalHash;
use russh::{client, keys::ssh_key::PublicKey};
use russh_sftp::{
    client::{RawSftpSession, error::Error as SftpError},
    protocol::{FileAttributes, OpenFlags},
};
use std::{
    borrow::Cow,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::Semaphore,
};

struct TrustState {
    key: Option<Vec<u8>>,
    challenge: Option<PresentedHostKeyChallenge>,
    failure: Option<ProtocolFailure>,
}
pub(crate) struct Handler {
    trust: HostTrust,
    state: Arc<Mutex<TrustState>>,
    task: Arc<HttpTaskSpec>,
    generation: Generation,
    host: String,
    port: u16,
    source: u32,
    stats: crate::HttpTransferStats,
}
impl client::Handler for Handler {
    type Error = russh::Error;
    async fn kex_done(
        &mut self,
        _shared_secret: Option<&[u8]>,
        names: &russh::Names,
        _session: &mut client::Session,
    ) -> Result<(), Self::Error> {
        let known = |name: &str, allowed: &'static [&'static str]| {
            allowed
                .iter()
                .copied()
                .find(|entry| *entry == name)
                .ok_or(russh::Error::Inconsistent)
        };
        let key = names.key.to_string();
        self.stats
            .set_ssh_connection(crate::SshConnectionDiagnostic {
                source: self.source,
                kex: known(names.kex.as_ref(), KEX)?,
                host_key: known(&key, HOST_KEYS)?,
                cipher: known(names.cipher.as_ref(), CIPHER)?,
                client_mac: known(names.client_mac.as_ref(), DIAGNOSTIC_MACS)?,
                server_mac: known(names.server_mac.as_ref(), DIAGNOSTIC_MACS)?,
                insecure_host_key: !self.task.options().transfer.sftp_check_host_key,
                legacy_host_key_digest: self.task.options().transfer.ssh_host_key_md.is_some(),
            });
        Ok(())
    }
    async fn check_server_key(&mut self, key: &PublicKey) -> Result<bool, Self::Error> {
        let Ok(blob) = key.to_bytes() else {
            self.state.lock().expect("host trust").failure = Some(ProtocolFailure::Malformed);
            return Ok(false);
        };
        if blob.len() > ariax_core::MAX_PRESENTED_HOST_KEY_BYTES {
            self.state.lock().expect("host trust").failure = Some(ProtocolFailure::ResourceLimit);
            return Ok(false);
        }
        let mut state = self.state.lock().expect("host trust");
        if let Some(previous) = &state.key {
            if previous != &blob {
                state.failure = Some(ProtocolFailure::HostKeyMismatch);
                return Ok(false);
            }
            return Ok(true);
        }
        match self.trust.accepts(&blob) {
            Ok(true) => {
                state.key = Some(blob);
                Ok(true)
            }
            Ok(false) => {
                match crate::sftp_trust::challenge(
                    self.task.gid(),
                    self.generation,
                    self.host.clone(),
                    self.port,
                    key,
                    blob,
                ) {
                    Ok(challenge) => state.challenge = Some(challenge),
                    Err(error) => state.failure = Some(error),
                }
                Ok(false)
            }
            Err(error) => {
                state.failure = Some(error);
                Ok(false)
            }
        }
    }
}

pub(crate) struct SftpSession {
    pub raw: Arc<RawSftpSession>,
    pub handle: Vec<u8>,
    pub validator: ProtocolValidator,
    pub max_read: usize,
    pub slots: Arc<Semaphore>,
    guard: tokio::sync::Mutex<ProtocolStreamGuard>,
    _ssh: client::Handle<Handler>,
}

async fn cpu<T: Send + 'static>(
    pool: &CpuPool,
    bytes: usize,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, ProtocolFailure> {
    let reservation = pool
        .reserve(bytes)
        .map_err(|_| ProtocolFailure::ResourceLimit)?;
    reservation
        .spawn(work)
        .join()
        .await
        .map(|output| output.into_inner())
        .map_err(|_| ProtocolFailure::Malformed)
}

impl SftpSession {
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        client: &HttpPolicyClient,
        task: Arc<HttpTaskSpec>,
        source: &HttpSourceSpec,
        generation: Generation,
        metadata: &HttpIngressBudgets,
        ingress: &HttpIngressBudgets,
        pool: &CpuPool,
        cancellation: &HttpCancellation,
        stats: crate::HttpTransferStats,
    ) -> Result<Self, HttpMultiRangeError> {
        let uri = source.uri().ok_or(ProtocolFailure::AuthFailure)?;
        let url = url::Url::parse(uri).map_err(|_| ProtocolFailure::UnsafeDestination)?;
        let host = url
            .host_str()
            .ok_or(ProtocolFailure::UnsafeDestination)?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_ascii_lowercase();
        let port = url.port().unwrap_or(22);
        let path = crate::transfer_task::decode_uri_component(url.path())
            .map_err(|_| ProtocolFailure::UnsafeDestination)?;
        let options = &task.options().transfer;
        // The SSH window and fixed packet/channel queues fit this reservation;
        // it moves into the socket and survives detached handshake cancellation.
        let metadata = metadata
            .try_acquire(4 * 1024 * 1024)
            .map_err(|_| ProtocolFailure::ResourceLimit)?;
        let trust_options = options.clone();
        let trust_host = host.clone();
        let trust = cpu(
            pool,
            crate::sftp_trust::MAX_RETAINED_KNOWN_HOSTS_BYTES + 256 * 1024,
            move || HostTrust::load(&trust_options, &trust_host, port),
        )
        .await??;
        let state = Arc::new(Mutex::new(TrustState {
            key: None,
            challenge: None,
            failure: None,
        }));
        let handler = Handler {
            trust,
            state: state.clone(),
            task: task.clone(),
            generation,
            host,
            port,
            source: source.id().get(),
            stats,
        };
        let connection = tokio::select! {biased;_=cancellation.cancelled()=>return Err(HttpMultiRangeError::Cancelled),
        result=client.connect_protocol(uri,None,task.options().connect_timeout)=>result?};
        let (stream, mut guard) = connection.into_guarded(metadata);
        let config = Arc::new(client::Config {
            window_size: 256 * 1024,
            maximum_packet_size: 32 * 1024,
            channel_buffer_size: 8,
            inactivity_timeout: Some(task.options().response_body_timeout),
            preferred: preferred(),
            ..Default::default()
        });
        let connected = tokio::select! {biased;_=cancellation.cancelled()=>Err(ProtocolFailure::Cancelled),
        result=tokio::time::timeout(task.options().connect_timeout,client::connect_stream(config,stream,handler))=>result.map_err(|_|ProtocolFailure::Timeout).and_then(|result|result.map_err(|_|ProtocolFailure::Connect))};
        let mut ssh = match connected {
            Ok(ssh) => ssh,
            Err(error) => {
                guard.drain().await;
                let mut state = state.lock().expect("host trust");
                if let Some(challenge) = state.challenge.take() {
                    return Err(HttpMultiRangeError::HostKeyChallenge(Box::new(challenge)));
                }
                return Err(state.failure.unwrap_or(error).into());
            }
        };
        let authentication = async {
            let source = source.clone();
            let options = options.clone();
            let credentials = cpu(pool, crate::MAX_HTTP_NETRC_BYTES, move || {
                crate::transfer_task::load_protocol_credentials(&options, &source)
            })
            .await??
            .ok_or(ProtocolFailure::AuthFailure)?;
            let user = credentials.username.as_ref();
            let options = &task.options().transfer;
            let mut result = ssh
                .authenticate_none(user)
                .await
                .map_err(|_| ProtocolFailure::AuthFailure)?;
            if offered(&result, russh::MethodKind::PublicKey)
                && let Some(path) = &options.sftp_private_key
            {
                let path = path.clone();
                let passphrase = options.sftp_private_key_passphrase.clone();
                let key = cpu(pool, 1024 * 1024 + 64 * 1024, move || {
                    use std::io::Read;
                    let file = crate::http_auth::open_private_netrc(&path)
                        .map_err(|_| ProtocolFailure::AuthFailure)?;
                    let mut bytes = zeroize::Zeroizing::new(Vec::new());
                    file.take(1024 * 1024 + 1)
                        .read_to_end(&mut bytes)
                        .map_err(|_| ProtocolFailure::AuthFailure)?;
                    if bytes.len() > 1024 * 1024 {
                        return Err(ProtocolFailure::ResourceLimit);
                    }
                    std::str::from_utf8(&bytes)
                        .map_err(|_| ProtocolFailure::AuthFailure)
                        .and_then(|text| {
                            russh::keys::decode_secret_key(
                                text,
                                passphrase.as_ref().map(crate::ProtocolSecret::expose),
                            )
                            .map_err(|_| ProtocolFailure::AuthFailure)
                        })
                })
                .await??;
                result = ssh
                    .authenticate_publickey(
                        user,
                        russh::keys::PrivateKeyWithHashAlg::new(
                            Arc::new(key),
                            Some(russh::keys::ssh_key::HashAlg::Sha256),
                        ),
                    )
                    .await
                    .map_err(|_| ProtocolFailure::AuthFailure)?;
            }
            if offered(&result, russh::MethodKind::PublicKey) && options.sftp_use_agent {
                let _agent_handle = client
                    .protocol_policy()
                    .1
                    .direct
                    .budgets
                    .try_acquire_connection()
                    .map_err(|_| ProtocolFailure::ResourceLimit)?;
                result = agent_auth(&mut ssh, user, result).await?;
            }
            if offered(&result, russh::MethodKind::Password)
                && let Some(password) = credentials.password.as_deref()
            {
                result = ssh
                    .authenticate_password(user, password)
                    .await
                    .map_err(|_| ProtocolFailure::AuthFailure)?;
            }
            let mut authenticated = result.success();
            if offered(&result, russh::MethodKind::KeyboardInteractive)
                && let Some(password) = credentials.password.as_deref()
            {
                use client::KeyboardInteractiveAuthResponse;
                let response = ssh
                    .authenticate_keyboard_interactive_start(user, None)
                    .await
                    .map_err(|_| ProtocolFailure::AuthFailure)?;
                authenticated = match response {
                    KeyboardInteractiveAuthResponse::Success => true,
                    KeyboardInteractiveAuthResponse::InfoRequest { prompts, .. }
                        if password_prompt(&prompts) =>
                    {
                        matches!(
                            ssh.authenticate_keyboard_interactive_respond(vec![
                                password.to_owned()
                            ])
                            .await
                            .map_err(|_| ProtocolFailure::AuthFailure)?,
                            KeyboardInteractiveAuthResponse::Success
                        )
                    }
                    _ => false,
                };
            }
            if !authenticated {
                return Err(ProtocolFailure::AuthFailure);
            }
            let mut channel = ssh
                .channel_open_session()
                .await
                .map_err(|_| ProtocolFailure::Control)?;
            channel
                .request_subsystem(true, "sftp")
                .await
                .map_err(|_| ProtocolFailure::Control)?;
            let mut remaining = 64;
            while !subsystem_approved(channel.wait().await, &mut remaining)? {}
            Ok(channel)
        };
        let channel = tokio::select! {biased;_=cancellation.cancelled()=>Err(ProtocolFailure::Cancelled),
        result=tokio::time::timeout(task.options().connect_timeout,authentication)=>result.map_err(|_|ProtocolFailure::Timeout).and_then(|result|result)};
        let channel = match channel {
            Ok(channel) => channel,
            Err(error) => {
                // Russh can be waiting for an authentication response rather
                // than polling the socket. Closing its command sender wakes
                // that wait before the transport drain acknowledgement.
                drop(ssh);
                guard.drain().await;
                return Err(error.into());
            }
        };
        // This base charge protects the temporary decoder allocation even if a
        // request receiver is dropped. Request-specific double charges are extra.
        let base = match ingress.try_acquire(options.sftp_max_packet_size * 2) {
            Ok(permit) => permit,
            Err(_) => {
                drop(channel);
                drop(ssh);
                guard.drain().await;
                return Err(ProtocolFailure::ResourceLimit.into());
            }
        };
        let mut raw = RawSftpSession::new_with_config(
            IngressStream {
                stream: channel.into_stream(),
                _permit: base,
            },
            russh_sftp::client::Config {
                max_packet_len: options.sftp_max_packet_size as u32,
                request_timeout_secs: task.options().response_body_timeout.as_secs().max(1),
                ..Default::default()
            },
        );
        let opened = async {
            let version = raw.init().await.map_err(sftp_error)?;
            if version.version != 3 {
                return Err(ProtocolFailure::Malformed);
            }
            let mut read_cap = options.sftp_max_read_size;
            if version
                .extensions
                .contains_key(russh_sftp::extensions::LIMITS)
            {
                let limits = raw.limits().await.map_err(sftp_error)?;
                if limits.max_read_len > 0 {
                    read_cap =
                        read_cap.min(usize::try_from(limits.max_read_len).unwrap_or(usize::MAX));
                }
                raw.set_limits(limits.into());
            }
            read_cap = read_cap.min((raw.packet_cap() as usize).saturating_sub(13));
            if read_cap == 0 {
                return Err(ProtocolFailure::ResourceLimit);
            }
            let handle = raw
                .open(path, OpenFlags::READ, FileAttributes::default())
                .await
                .map_err(sftp_error)?
                .handle;
            if handle.is_empty() || handle.len() > 16 * 1024 {
                return Err(ProtocolFailure::Malformed);
            }
            let attrs = raw
                .fstat(handle.as_slice())
                .await
                .map_err(sftp_error)?
                .attrs;
            let total = attrs.size.ok_or(ProtocolFailure::SizeUnsupported)?;
            let key = state
                .lock()
                .expect("host trust")
                .key
                .clone()
                .ok_or(ProtocolFailure::HostKeyMismatch)?;
            let fingerprint = ariax_core::HostKeyFingerprint::for_presented_key(&key);
            let validator = ProtocolValidator {
                protocol: 3,
                source: JournalHash::new(*source.redacted_fingerprint())
                    .ok_or(ProtocolFailure::Malformed)?,
                total_length: total,
                modified_unix_seconds: attrs.mtime.map(u64::from),
                host_key: Some(
                    JournalHash::new(*fingerprint.as_bytes()).ok_or(ProtocolFailure::Malformed)?,
                ),
            };
            Ok((handle, validator, read_cap))
        };
        let result = tokio::select! {biased;_=cancellation.cancelled()=>Err(ProtocolFailure::Cancelled),result=opened=>result};
        let (handle, validator, max_read) = match result {
            Ok(opened) => opened,
            Err(error) => {
                raw.drain().await;
                guard.drain().await;
                return Err(error.into());
            }
        };
        Ok(Self {
            raw: Arc::new(raw),
            handle,
            validator,
            max_read,
            slots: Arc::new(Semaphore::new(options.sftp_max_outstanding_reads)),
            guard: tokio::sync::Mutex::new(guard),
            _ssh: ssh,
        })
    }

    pub async fn drain(&self) {
        self.raw.drain().await;
        self.guard.lock().await.drain().await;
    }
    pub async fn finish(&self) -> Result<(), ProtocolFailure> {
        let result = async {
            let attrs = self
                .raw
                .fstat(self.handle.as_slice())
                .await
                .map_err(sftp_error)?
                .attrs;
            if attrs.size != Some(self.validator.total_length)
                || attrs.mtime.map(u64::from) != self.validator.modified_unix_seconds
            {
                return Err(ProtocolFailure::StaleValidator);
            }
            self.raw
                .close(self.handle.as_slice())
                .await
                .map_err(sftp_error)?;
            Ok(())
        }
        .await;
        self.drain().await;
        result
    }
}

fn subsystem_approved(
    message: Option<russh::ChannelMsg>,
    remaining: &mut usize,
) -> Result<bool, ProtocolFailure> {
    *remaining = remaining.checked_sub(1).ok_or(ProtocolFailure::Control)?;
    match message {
        Some(russh::ChannelMsg::Success) => Ok(true),
        Some(russh::ChannelMsg::WindowAdjusted { .. }) => Ok(false),
        _ => Err(ProtocolFailure::Control),
    }
}

pub(crate) fn sftp_error(error: SftpError) -> ProtocolFailure {
    match error {
        SftpError::PacketTooLarge | SftpError::RequestLimit | SftpError::Limited(_) => {
            ProtocolFailure::ResourceLimit
        }
        SftpError::Timeout => ProtocolFailure::Timeout,
        SftpError::SessionClosed | SftpError::IO(_) => ProtocolFailure::Data,
        SftpError::Status(status)
            if status.status_code == russh_sftp::protocol::StatusCode::PermissionDenied =>
        {
            ProtocolFailure::AuthFailure
        }
        SftpError::Status(_) => ProtocolFailure::Data,
        _ => ProtocolFailure::Malformed,
    }
}

const KEX: &[&str] = &[
    "curve25519-sha256",
    "curve25519-sha256@libssh.org",
    "diffie-hellman-group-exchange-sha256",
    "diffie-hellman-group14-sha256",
    "ext-info-c",
    "kex-strict-c-v00@openssh.com",
];
const CIPHER: &[&str] = &[
    "chacha20-poly1305@openssh.com",
    "aes256-gcm@openssh.com",
    "aes128-gcm@openssh.com",
    "aes256-ctr",
    "aes192-ctr",
    "aes128-ctr",
];
const MAC: &[&str] = &[
    "hmac-sha2-512-etm@openssh.com",
    "hmac-sha2-256-etm@openssh.com",
    "hmac-sha2-512",
    "hmac-sha2-256",
];
const HOST_KEYS: &[&str] = &[
    "ssh-ed25519",
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
    "rsa-sha2-512",
    "rsa-sha2-256",
    "ssh-ed25519-cert-v01@openssh.com",
    "ecdsa-sha2-nistp256-cert-v01@openssh.com",
    "ecdsa-sha2-nistp384-cert-v01@openssh.com",
    "ecdsa-sha2-nistp521-cert-v01@openssh.com",
    "rsa-sha2-512-cert-v01@openssh.com",
    "rsa-sha2-256-cert-v01@openssh.com",
];
const DIAGNOSTIC_MACS: &[&str] = &[
    "none",
    "hmac-sha2-512-etm@openssh.com",
    "hmac-sha2-256-etm@openssh.com",
    "hmac-sha2-512",
    "hmac-sha2-256",
];
fn preferred() -> russh::Preferred {
    let defaults = russh::Preferred::default();
    russh::Preferred {
        kex: Cow::Owned(
            defaults
                .kex
                .iter()
                .copied()
                .filter(|name| KEX.contains(&name.as_ref()))
                .collect(),
        ),
        key: Cow::Owned(
            defaults
                .key
                .iter()
                .filter(|algorithm| HOST_KEYS.contains(&algorithm.to_string().as_str()))
                .cloned()
                .collect(),
        ),
        cipher: Cow::Owned(
            defaults
                .cipher
                .iter()
                .copied()
                .filter(|name| CIPHER.contains(&name.as_ref()))
                .collect(),
        ),
        mac: Cow::Owned(
            defaults
                .mac
                .iter()
                .copied()
                .filter(|name| MAC.contains(&name.as_ref()))
                .collect(),
        ),
        compression: Cow::Owned(vec![russh::compression::NONE]),
    }
}

fn offered(result: &client::AuthResult, method: russh::MethodKind) -> bool {
    matches!(result, client::AuthResult::Failure { remaining_methods, .. } if remaining_methods.contains(&method))
}
fn password_prompt(prompts: &[client::Prompt]) -> bool {
    matches!(prompts, [prompt] if !prompt.echo && prompt.prompt.trim().eq_ignore_ascii_case("password:"))
}

async fn agent_auth(
    ssh: &mut client::Handle<Handler>,
    user: &str,
    mut result: client::AuthResult,
) -> Result<client::AuthResult, ProtocolFailure> {
    #[cfg(unix)]
    let agent = russh::keys::agent::client::AgentClient::connect_env().await;
    #[cfg(windows)]
    let agent =
        russh::keys::agent::client::AgentClient::connect_named_pipe(r"\\.\pipe\openssh-ssh-agent")
            .await;
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (ssh, user);
        return Err(ProtocolFailure::AuthFailure);
    }
    #[cfg(any(unix, windows))]
    {
        let Ok(mut agent) = agent else {
            return Ok(result);
        };
        let identities = agent
            .request_identities()
            .await
            .map_err(|_| ProtocolFailure::AuthFailure)?;
        if identities.len() > 64 {
            return Err(ProtocolFailure::ResourceLimit);
        }
        for identity in identities {
            if !offered(&result, russh::MethodKind::PublicKey) {
                break;
            }
            if let russh::keys::agent::AgentIdentity::PublicKey { key, .. } = identity {
                result = ssh
                    .authenticate_publickey_with(
                        user,
                        key,
                        Some(russh::keys::ssh_key::HashAlg::Sha256),
                        &mut agent,
                    )
                    .await
                    .map_err(|_| ProtocolFailure::AuthFailure)?;
            }
        }
        Ok(result)
    }
}

struct IngressStream<S> {
    stream: S,
    _permit: HttpIngressPermit,
}
impl<S: AsyncRead + Unpin> AsyncRead for IngressStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for IngressStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod subsystem_tests {
    use super::*;
    #[test]
    fn subsystem_accepts_window_updates_but_requires_bounded_success() {
        let mut remaining = 64;
        assert_eq!(
            subsystem_approved(
                Some(russh::ChannelMsg::WindowAdjusted { new_size: 1024 }),
                &mut remaining
            ),
            Ok(false)
        );
        assert_eq!(
            subsystem_approved(Some(russh::ChannelMsg::Success), &mut remaining),
            Ok(true)
        );
        for message in [
            Some(russh::ChannelMsg::Failure),
            Some(russh::ChannelMsg::Eof),
            None,
        ] {
            assert!(subsystem_approved(message, &mut 64).is_err());
        }
        let mut remaining = 64;
        for _ in 0..64 {
            assert_eq!(
                subsystem_approved(
                    Some(russh::ChannelMsg::WindowAdjusted { new_size: 1 }),
                    &mut remaining
                ),
                Ok(false)
            );
        }
        assert!(subsystem_approved(Some(russh::ChannelMsg::Success), &mut remaining).is_err());
    }
}
