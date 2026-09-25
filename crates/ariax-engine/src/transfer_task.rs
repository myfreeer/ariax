//! Protocol selection and immutable protocol options shared by every surface.
use crate::{
    ChunkAlignment, ContentChecksum, HttpContentChecksum, HttpTaskOptions, HttpTaskSpecError,
};
use ariax_storage::JournalHash;
use std::{fmt, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum TransferProtocol {
    Http,
    Https,
    Ftp,
    Ftps,
    Sftp,
}
impl TransferProtocol {
    pub fn parse(scheme: &str) -> Result<Self, HttpTaskSpecError> {
        match scheme {
            "http" => Ok(Self::Http),
            "https" => Ok(Self::Https),
            "ftp" => Ok(Self::Ftp),
            "ftps" => Ok(Self::Ftps),
            "sftp" => Ok(Self::Sftp),
            _ => Err(HttpTaskSpecError::UnsupportedScheme),
        }
    }
    pub const fn enabled(self) -> bool {
        match self {
            Self::Http | Self::Https => true,
            Self::Ftp | Self::Ftps => cfg!(feature = "ftp"),
            Self::Sftp => cfg!(feature = "sftp"),
        }
    }
    pub const fn code(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
            Self::Ftp => "ftp",
            Self::Ftps => "ftps",
            Self::Sftp => "sftp",
        }
    }
    pub const fn default_port(self) -> u16 {
        match self {
            Self::Http => 80,
            Self::Https => 443,
            Self::Ftp => 21,
            Self::Ftps => 990,
            Self::Sftp => 22,
        }
    }
    pub const fn random_access(self) -> bool {
        matches!(self, Self::Http | Self::Https | Self::Sftp)
    }
    pub const fn is_http(self) -> bool {
        matches!(self, Self::Http | Self::Https)
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct TransferCredentials {
    pub(crate) username: Arc<str>,
    pub(crate) password: Option<Arc<str>>,
}
impl TransferCredentials {
    pub fn new(username: String, password: Option<String>) -> Result<Self, HttpTaskSpecError> {
        if username.is_empty()
            || username.len() > 1024
            || username.contains(['\r', '\n', '\0'])
            || password
                .as_ref()
                .is_some_and(|value| value.len() > 4096 || value.contains(['\r', '\n', '\0']))
        {
            return Err(HttpTaskSpecError::InvalidOptions);
        }
        Ok(Self {
            username: username.into(),
            password: password.map(Into::into),
        })
    }
}
impl fmt::Debug for TransferCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TransferCredentials([redacted])")
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UriSelector {
    InOrder,
    #[default]
    Feedback,
    Adaptive,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FollowMetadata {
    Never,
    #[default]
    Follow,
    Memory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferOptions {
    pub credentials: Option<TransferCredentials>,
    pub sftp_known_hosts: Option<std::path::PathBuf>,
    pub sftp_private_key: Option<std::path::PathBuf>,
    pub sftp_private_key_passphrase: Option<ProtocolSecret>,
    pub sftp_use_agent: bool,
    pub netrc_path: Option<std::path::PathBuf>,
    pub no_netrc: bool,
    pub checksum: Option<ContentChecksum>,
    pub alignment: ChunkAlignment,
    pub realtime_checksum: bool,
    pub follow_metalink: FollowMetadata,
    pub follow_torrent: FollowMetadata,
    pub bittorrent_options: crate::BitTorrentOptions,
    pub uri_selector: UriSelector,
    pub server_stat_timeout: std::time::Duration,
    pub metalink_filters: std::collections::BTreeMap<String, String>,
    pub ftp_passive: bool,
    pub ftp_pasv_server_address: bool,
    pub ftp_reuse_connection: bool,
    pub ftp_tls: bool,
    pub sftp_max_outstanding_reads: usize,
    pub sftp_max_read_size: usize,
    pub sftp_max_packet_size: usize,
    pub sftp_check_host_key: bool,
    pub sftp_host_key: Option<String>,
    pub sftp_host_key_sha256: Option<String>,
    pub ssh_host_key_md: Option<String>,
    /// Internal, persisted binding; admission never accepts this from RPC options.
    pub(crate) verification_fingerprint: Option<JournalHash>,
    pub(crate) metadata_expansion: Option<ariax_storage::MetadataExpansion>,
}
impl Default for TransferOptions {
    fn default() -> Self {
        Self {
            credentials: None,
            sftp_known_hosts: None,
            sftp_private_key: None,
            sftp_private_key_passphrase: None,
            sftp_use_agent: false,
            netrc_path: None,
            no_netrc: false,
            checksum: None,
            alignment: ChunkAlignment::Auto,
            realtime_checksum: true,
            follow_metalink: FollowMetadata::Follow,
            follow_torrent: FollowMetadata::Follow,
            bittorrent_options: Default::default(),
            uri_selector: UriSelector::Feedback,
            server_stat_timeout: std::time::Duration::from_secs(86400),
            metalink_filters: Default::default(),
            ftp_passive: true,
            ftp_pasv_server_address: false,
            ftp_reuse_connection: true,
            ftp_tls: false,
            sftp_max_outstanding_reads: 8,
            sftp_max_read_size: 64 * 1024,
            sftp_max_packet_size: 128 * 1024,
            sftp_check_host_key: true,
            sftp_host_key: None,
            sftp_host_key_sha256: None,
            ssh_host_key_md: None,
            verification_fingerprint: None,
            metadata_expansion: None,
        }
    }
}
impl TransferOptions {
    pub(crate) fn is_bittorrent_option(name: &str) -> bool {
        cfg!(feature = "bt")
            && ariax_config::builtin_registry()
                .find(name)
                .is_some_and(|definition| {
                    definition.owner == "bt"
                        && definition.scopes.contains(ariax_config::Scope::PerDownload)
                        && name != "follow-torrent"
                })
    }
    pub(crate) fn is_metalink_filter(name: &str) -> bool {
        matches!(
            name,
            "select-file"
                | "metalink-language"
                | "metalink-location"
                | "metalink-os"
                | "metalink-version"
                | "metalink-preferred-protocol"
                | "metalink-enable-unique-protocol"
                | "metadata-max-document-size"
                | "metadata-max-files"
                | "metadata-max-sources"
        )
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        let credentials = |value: &TransferCredentials| {
            value
                .username
                .len()
                .saturating_add(value.password.as_ref().map_or(0, |value| value.len()))
                .saturating_add(64)
        };
        let mut bytes = self.credentials.as_ref().map_or(0, credentials);
        for path in [
            &self.sftp_known_hosts,
            &self.sftp_private_key,
            &self.netrc_path,
        ]
        .into_iter()
        .flatten()
        {
            bytes = bytes
                .saturating_add(path.as_os_str().len().saturating_mul(2))
                .saturating_add(64);
        }
        for text in [
            &self.sftp_host_key,
            &self.sftp_host_key_sha256,
            &self.ssh_host_key_md,
        ]
        .into_iter()
        .flatten()
        {
            bytes = bytes.saturating_add(text.capacity()).saturating_add(32);
        }
        bytes = bytes.saturating_add(
            self.sftp_private_key_passphrase
                .as_ref()
                .map_or(0, |value| value.expose().len().saturating_add(32)),
        );
        bytes = bytes.saturating_add(self.metadata_expansion.as_ref().map_or(0, |value| {
            value
                .children
                .capacity()
                .saturating_mul(std::mem::size_of::<ariax_core::Gid>())
                .saturating_add(256)
        }));
        bytes = self
            .bittorrent_options
            .pairs()
            .fold(bytes, |bytes, (name, value)| {
                bytes
                    .saturating_add(name.len())
                    .saturating_add(value.len())
                    .saturating_add(512)
            });
        self.metalink_filters
            .iter()
            .fold(bytes, |sum, (key, value)| {
                sum.saturating_add(key.capacity())
                    .saturating_add(value.capacity())
                    .saturating_add(128)
            })
    }

    pub(crate) fn validate(&self) -> Result<(), HttpTaskSpecError> {
        if self.server_stat_timeout.as_secs() > 31_536_000
            || self.metalink_filters.iter().any(|(name, value)| {
                !Self::is_metalink_filter(name)
                    || value.len() > 65536
                    || value.contains(['\0', '\r', '\n'])
            })
            || self.sftp_max_outstanding_reads == 0
            || self.sftp_max_outstanding_reads > 64
            || self.sftp_max_read_size == 0
            || self.sftp_max_read_size > 1024 * 1024
            || !(1024..=1024 * 1024).contains(&self.sftp_max_packet_size)
            || self.sftp_max_read_size.saturating_add(13) > self.sftp_max_packet_size
            || self
                .sftp_host_key
                .as_ref()
                .is_some_and(|v| v.len() > 16 * 1024 || v.contains(['\r', '\n', '\0']))
            || self
                .sftp_host_key_sha256
                .as_ref()
                .is_some_and(|v| crate::transfer_task::parse_host_key_fingerprint(v).is_err())
        {
            return Err(HttpTaskSpecError::InvalidOptions);
        }
        Ok(())
    }
    pub(crate) fn handles(name: &str) -> bool {
        Self::is_metalink_filter(name)
            || Self::is_bittorrent_option(name)
            || cfg!(feature = "bt") && name == "follow-torrent"
            || name == "server-stat-timeout"
            || matches!(
                name,
                "realtime-chunk-checksum"
                    | "metalink-chunk-alignment"
                    | "follow-metalink"
                    | "uri-selector"
            )
            || ((cfg!(feature = "ftp") || cfg!(feature = "sftp"))
                && matches!(name, "ftp-user" | "ftp-passwd" | "netrc-path" | "no-netrc"))
            || (cfg!(feature = "ftp")
                && matches!(
                    name,
                    "ftp-pasv"
                        | "ftp-pasv-address"
                        | "ftp-reuse-connection"
                        | "ftp-type"
                        | "ftp-ssl"
                ))
            || (cfg!(feature = "sftp")
                && matches!(
                    name,
                    "sftp-known-hosts"
                        | "sftp-private-key"
                        | "sftp-private-key-passphrase"
                        | "sftp-use-agent"
                        | "sftp-max-outstanding-reads"
                        | "sftp-max-read-size"
                        | "sftp-max-packet-size"
                        | "sftp-check-host-key"
                        | "sftp-host-key"
                        | "sftp-host-key-sha256"
                        | "ssh-host-key-md"
                ))
    }
    pub(crate) fn set(&mut self, name: &str, value: &str) -> Result<(), HttpTaskSpecError> {
        let invalid = || HttpTaskSpecError::InvalidOptions;
        match name {
            "follow-torrent" => {
                self.follow_torrent = match value {
                    "true" => FollowMetadata::Follow,
                    "false" => FollowMetadata::Never,
                    "mem" => FollowMetadata::Memory,
                    _ => return Err(invalid()),
                };
            }
            name if Self::is_bittorrent_option(name) => {
                self.bittorrent_options = crate::BitTorrentOptions::from_pairs(
                    self.bittorrent_options
                        .pairs()
                        .filter(|(key, _)| *key != name)
                        .map(|(name, value)| (name.to_owned(), value.to_owned()))
                        .chain([(name.to_owned(), value.to_owned())]),
                )
                .map_err(|_| invalid())?;
            }
            name if Self::is_metalink_filter(name) => {
                self.metalink_filters
                    .insert(name.to_owned(), value.to_owned());
            }
            "server-stat-timeout" => {
                self.server_stat_timeout =
                    std::time::Duration::from_secs(value.parse().map_err(|_| invalid())?)
            }
            "netrc-path" => self.netrc_path = Some(value.into()),
            "no-netrc" => self.no_netrc = value.parse().map_err(|_| invalid())?,
            "sftp-known-hosts" => self.sftp_known_hosts = Some(value.into()),
            "sftp-private-key" => self.sftp_private_key = Some(value.into()),
            "sftp-private-key-passphrase" => {
                self.sftp_private_key_passphrase = Some(ProtocolSecret::new(value.to_owned())?)
            }
            "sftp-use-agent" => self.sftp_use_agent = value.parse().map_err(|_| invalid())?,
            "realtime-chunk-checksum" => {
                self.realtime_checksum = value.parse().map_err(|_| invalid())?
            }
            "metalink-chunk-alignment" => {
                self.alignment = match value {
                    "auto" => ChunkAlignment::Auto,
                    "strict" => ChunkAlignment::Strict,
                    "relaxed" => ChunkAlignment::Relaxed,
                    _ => return Err(invalid()),
                }
            }
            "follow-metalink" => {
                self.follow_metalink = match value {
                    "true" => FollowMetadata::Follow,
                    "false" => FollowMetadata::Never,
                    "mem" => FollowMetadata::Memory,
                    _ => return Err(invalid()),
                }
            }
            "uri-selector" => {
                self.uri_selector = match value {
                    "inorder" => UriSelector::InOrder,
                    "feedback" => UriSelector::Feedback,
                    "adaptive" => UriSelector::Adaptive,
                    _ => return Err(invalid()),
                }
            }
            "ftp-pasv" => self.ftp_passive = value.parse().map_err(|_| invalid())?,
            "ftp-pasv-address" => {
                self.ftp_pasv_server_address = match value {
                    "control-peer" => false,
                    "server" => true,
                    _ => return Err(invalid()),
                }
            }
            "ftp-reuse-connection" => {
                self.ftp_reuse_connection = value.parse().map_err(|_| invalid())?
            }
            "ftp-ssl" => self.ftp_tls = value.parse().map_err(|_| invalid())?,
            "ftp-type" if value == "binary" => {}
            "sftp-max-outstanding-reads" => {
                self.sftp_max_outstanding_reads = value.parse().map_err(|_| invalid())?
            }
            "sftp-max-read-size" => {
                self.sftp_max_read_size = value.parse().map_err(|_| invalid())?
            }
            "sftp-max-packet-size" => {
                self.sftp_max_packet_size = value.parse().map_err(|_| invalid())?
            }
            "sftp-check-host-key" => {
                self.sftp_check_host_key = value.parse().map_err(|_| invalid())?
            }
            "sftp-host-key" => self.sftp_host_key = Some(value.to_owned()),
            "sftp-host-key-sha256" => self.sftp_host_key_sha256 = Some(value.to_owned()),
            "ssh-host-key-md" => self.ssh_host_key_md = Some(value.to_owned()),
            "metadata-expansion" => {
                self.metadata_expansion =
                    Some(ariax_storage::MetadataExpansion::parse(value).ok_or_else(invalid)?)
            }
            "verification-manifest" => {
                let digest =
                    ContentChecksum::parse(&format!("sha-256={value}")).map_err(|_| invalid())?;
                self.verification_fingerprint = Some(
                    JournalHash::new(digest.value().try_into().map_err(|_| invalid())?)
                        .ok_or_else(invalid)?,
                );
            }
            _ => return Err(invalid()),
        }
        Ok(())
    }
    pub(crate) fn persisted(&self) -> Vec<(String, String)> {
        // Default-only fields do not change historical HTTP option snapshots.
        let default = Self::default();
        let mut entries: Vec<_> = self
            .metalink_filters
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        if self.server_stat_timeout != default.server_stat_timeout {
            entries.push((
                "server-stat-timeout".into(),
                self.server_stat_timeout.as_secs().to_string(),
            ));
        }
        entries.extend(
            self.bittorrent_options
                .pairs()
                .map(|(name, value)| (name.to_owned(), value.to_owned())),
        );
        for (name, changed, value) in [
            (
                "follow-torrent",
                self.follow_torrent != default.follow_torrent,
                match self.follow_torrent {
                    FollowMetadata::Follow => "true",
                    FollowMetadata::Never => "false",
                    FollowMetadata::Memory => "mem",
                }
                .to_owned(),
            ),
            (
                "no-netrc",
                self.no_netrc != default.no_netrc,
                self.no_netrc.to_string(),
            ),
            (
                "realtime-chunk-checksum",
                self.realtime_checksum != default.realtime_checksum,
                self.realtime_checksum.to_string(),
            ),
            (
                "metalink-chunk-alignment",
                self.alignment != default.alignment,
                match self.alignment {
                    ChunkAlignment::Auto => "auto",
                    ChunkAlignment::Strict => "strict",
                    ChunkAlignment::Relaxed => "relaxed",
                }
                .to_owned(),
            ),
            (
                "follow-metalink",
                self.follow_metalink != default.follow_metalink,
                match self.follow_metalink {
                    FollowMetadata::Follow => "true",
                    FollowMetadata::Never => "false",
                    FollowMetadata::Memory => "mem",
                }
                .to_owned(),
            ),
            (
                "uri-selector",
                self.uri_selector != default.uri_selector,
                match self.uri_selector {
                    UriSelector::InOrder => "inorder",
                    UriSelector::Feedback => "feedback",
                    UriSelector::Adaptive => "adaptive",
                }
                .to_owned(),
            ),
            (
                "ftp-pasv",
                self.ftp_passive != default.ftp_passive,
                self.ftp_passive.to_string(),
            ),
            (
                "ftp-reuse-connection",
                self.ftp_reuse_connection != default.ftp_reuse_connection,
                self.ftp_reuse_connection.to_string(),
            ),
            (
                "ftp-ssl",
                self.ftp_tls != default.ftp_tls,
                self.ftp_tls.to_string(),
            ),
            (
                "sftp-max-outstanding-reads",
                self.sftp_max_outstanding_reads != default.sftp_max_outstanding_reads,
                self.sftp_max_outstanding_reads.to_string(),
            ),
            (
                "sftp-max-read-size",
                self.sftp_max_read_size != default.sftp_max_read_size,
                self.sftp_max_read_size.to_string(),
            ),
            (
                "sftp-max-packet-size",
                self.sftp_max_packet_size != default.sftp_max_packet_size,
                self.sftp_max_packet_size.to_string(),
            ),
        ] {
            if changed {
                entries.push((name.to_owned(), value));
            }
        }
        for (name, value) in [
            ("sftp-host-key", &self.sftp_host_key),
            ("sftp-host-key-sha256", &self.sftp_host_key_sha256),
            ("ssh-host-key-md", &self.ssh_host_key_md),
        ] {
            if let Some(value) = value {
                entries.push((name.to_owned(), value.clone()));
            }
        }
        if let Some(expansion) = &self.metadata_expansion {
            entries.push((
                ariax_storage::METADATA_EXPANSION_OPTION.to_owned(),
                expansion.canonical(),
            ));
        }
        if let Some(fingerprint) = self.verification_fingerprint {
            entries.push(("verification-manifest".to_owned(), fingerprint.to_string()));
        }
        entries
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ProtocolSecret(Arc<str>);
impl ProtocolSecret {
    pub fn new(value: String) -> Result<Self, HttpTaskSpecError> {
        if value.len() > 4096 || value.contains(['\0', '\r', '\n']) {
            return Err(HttpTaskSpecError::InvalidOptions);
        }
        Ok(Self(value.into()))
    }
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for ProtocolSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProtocolSecret([redacted])")
    }
}

pub(crate) fn parse_host_key_fingerprint(text: &str) -> Result<[u8; 32], HttpTaskSpecError> {
    use base64ct::Encoding;
    if let Some(value) = text.strip_prefix("SHA256:") {
        base64ct::Base64Unpadded::decode_vec(value)
            .map_err(|_| HttpTaskSpecError::InvalidOptions)?
            .try_into()
            .map_err(|_| HttpTaskSpecError::InvalidOptions)
    } else {
        let digest = ContentChecksum::parse(&format!("sha-256={text}"))
            .map_err(|_| HttpTaskSpecError::InvalidOptions)?;
        digest
            .value()
            .try_into()
            .map_err(|_| HttpTaskSpecError::InvalidOptions)
    }
}

pub(crate) fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(HEX[(byte >> 4) as usize] as char);
        text.push(HEX[(byte & 15) as usize] as char);
    }
    text
}

pub(crate) fn parse_hex_bytes<const N: usize>(text: &str) -> Result<[u8; N], HttpTaskSpecError> {
    if text.len() != N * 2 || !text.is_ascii() {
        return Err(HttpTaskSpecError::InvalidOptions);
    }
    let mut bytes = [0; N];
    for (byte, input) in bytes.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        let high = (input[0] as char)
            .to_digit(16)
            .ok_or(HttpTaskSpecError::InvalidOptions)?;
        let low = (input[1] as char)
            .to_digit(16)
            .ok_or(HttpTaskSpecError::InvalidOptions)?;
        *byte = (high * 16 + low) as u8;
    }
    Ok(bytes)
}
impl HttpTaskOptions {
    pub(crate) fn without_live_authority(&self) -> Self {
        let mut options = self.clone();
        let transfer = &mut options.transfer;
        transfer.credentials = None;
        transfer.sftp_known_hosts = None;
        transfer.sftp_private_key = None;
        transfer.sftp_private_key_passphrase = None;
        transfer.sftp_use_agent = false;
        transfer.netrc_path = None;
        transfer.ftp_pasv_server_address = false;
        transfer.sftp_check_host_key = true;
        options
    }
    pub fn content_checksum(&self) -> Option<ContentChecksum> {
        self.transfer
            .checksum
            .or_else(|| self.checksum.map(ContentChecksum::from))
    }
    pub(crate) fn set_content_checksum(&mut self, value: &str) -> Result<(), HttpTaskSpecError> {
        let checksum =
            ContentChecksum::parse(value).map_err(|_| HttpTaskSpecError::InvalidOptions)?;
        match HttpContentChecksum::try_from(checksum) {
            Ok(http) => {
                self.checksum = Some(http);
                self.transfer.checksum = None;
            }
            Err(_) => {
                self.checksum = None;
                self.transfer.checksum = Some(checksum);
            }
        }
        Ok(())
    }
}

pub(crate) fn decode_uri_component(text: &str) -> Result<String, HttpTaskSpecError> {
    if text.len() > 16 * 1024 {
        return Err(HttpTaskSpecError::InvalidUri);
    }
    let mut bytes = Vec::with_capacity(text.len());
    let mut input = text.as_bytes().iter().copied();
    while let Some(byte) = input.next() {
        let byte = if byte == b'%' {
            let high = input
                .next()
                .and_then(|v| (v as char).to_digit(16))
                .ok_or(HttpTaskSpecError::InvalidUri)?;
            let low = input
                .next()
                .and_then(|v| (v as char).to_digit(16))
                .ok_or(HttpTaskSpecError::InvalidUri)?;
            (high * 16 + low) as u8
        } else {
            byte
        };
        if matches!(byte, 0 | b'\r' | b'\n') {
            return Err(HttpTaskSpecError::InvalidUri);
        }
        bytes.push(byte);
    }
    String::from_utf8(bytes).map_err(|_| HttpTaskSpecError::InvalidUri)
}

#[cfg(any(feature = "ftp", feature = "sftp"))]
pub(crate) fn load_protocol_credentials(
    options: &TransferOptions,
    source: &crate::HttpSourceSpec,
) -> Result<Option<TransferCredentials>, crate::ProtocolFailure> {
    if let Some(credentials) = source.credentials().or(options.credentials.as_ref()) {
        return Ok(Some(credentials.clone()));
    }
    if !options.no_netrc
        && let Some(path) = &options.netrc_path
    {
        let uri = url::Url::parse(source.uri().ok_or(crate::ProtocolFailure::AuthFailure)?)
            .map_err(|_| crate::ProtocolFailure::UnsafeDestination)?;
        let netrc =
            crate::HttpNetrc::load(path).map_err(|_| crate::ProtocolFailure::AuthFailure)?;
        return Ok(netrc
            .credentials_for(
                uri.host_str()
                    .ok_or(crate::ProtocolFailure::UnsafeDestination)?,
            )
            .map(crate::HttpBasicCredentials::protocol_credentials));
    }
    Ok(None)
}
