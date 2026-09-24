use crate::BtError;
use crate::bencode::{Node, decode};
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Digest as _, Sha256};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataLimits {
    pub bytes: usize,
    pub depth: usize,
    pub tokens: usize,
    pub files: usize,
    pub pieces: u64,
}

impl Default for MetadataLimits {
    fn default() -> Self {
        Self {
            bytes: 16 * 1024 * 1024,
            depth: 32,
            tokens: 200_000,
            files: 10_000,
            pieces: 1_000_000,
        }
    }
}

impl MetadataLimits {
    pub fn validate(self) -> Result<(), BtError> {
        if self.bytes == 0
            || self.bytes > 64 * 1024 * 1024
            || self.depth == 0
            || self.depth > 100
            || self.tokens == 0
            || self.tokens > 1_000_000
            || self.files == 0
            || self.files > 100_000
            || self.pieces == 0
            || self.pieces > 2 * 1024 * 1024
        {
            return Err(BtError::MetadataLimit);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BtIdentity {
    pub v1: Option<String>,
    pub v2: Option<String>,
}

impl BtIdentity {
    pub fn matches(&self, discovered: &Self) -> bool {
        (self.v1.is_some() || self.v2.is_some())
            && self
                .v1
                .as_ref()
                .is_none_or(|hash| discovered.v1.as_ref() == Some(hash))
            && self
                .v2
                .as_ref()
                .is_none_or(|hash| discovered.v2.as_ref() == Some(hash))
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.v1
            .as_ref()
            .is_some_and(|hash| other.v1.as_ref() == Some(hash))
            || self
                .v2
                .as_ref()
                .is_some_and(|hash| other.v2.as_ref() == Some(hash))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataFile {
    pub index: u32,
    pub components: Vec<String>,
    pub length: u64,
    pub offset: u64,
    pub padding: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TorrentMetadata {
    pub identity: BtIdentity,
    pub name: String,
    pub files: Vec<MetadataFile>,
    pub piece_length: u64,
    pub pieces: u64,
    pub total_length: u64,
    pub private: bool,
    pub trackers: Vec<String>,
    pub web_seeds: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Magnet {
    pub identity: BtIdentity,
    pub name: Option<String>,
    pub trackers: Vec<String>,
    pub web_seeds: Vec<String>,
    pub peers: Vec<std::net::SocketAddr>,
}

fn component(value: &[u8]) -> Result<String, BtError> {
    let value = std::str::from_utf8(value).map_err(|_| BtError::UnsafePath)?;
    if value.is_empty()
        || value.len() > 255
        || value == "."
        || value == ".."
        || value.contains(['/', '\\'])
        || value.chars().any(char::is_control)
    {
        return Err(BtError::UnsafePath);
    }
    Ok(value.to_owned())
}

fn attributes(node: &Node<'_>) -> Result<bool, BtError> {
    let attrs = node
        .get(b"attr")
        .map(Node::bytes)
        .transpose()?
        .unwrap_or_default();
    if attrs.contains(&b'l') || node.get(b"symlink path").is_some() {
        return Err(BtError::Symlink);
    }
    Ok(attrs.contains(&b'p'))
}

fn push_file(
    files: &mut Vec<MetadataFile>,
    components: Vec<String>,
    length: u64,
    padding: bool,
    total: &mut u64,
    limits: MetadataLimits,
) -> Result<(), BtError> {
    if files.len() >= limits.files {
        return Err(BtError::MetadataLimit);
    }
    let index = u32::try_from(files.len()).map_err(|_| BtError::MetadataLimit)?;
    files.push(MetadataFile {
        index,
        components,
        length,
        offset: *total,
        padding,
    });
    *total = total
        .checked_add(length)
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or(BtError::MetadataLimit)?;
    Ok(())
}

fn v2_files(
    node: &Node<'_>,
    path: &mut Vec<String>,
    files: &mut Vec<MetadataFile>,
    total: &mut u64,
    piece_length: u64,
    limits: MetadataLimits,
) -> Result<(), BtError> {
    if path.len() > limits.depth {
        return Err(BtError::MetadataLimit);
    }
    let entries = node.dictionary()?;
    if let Some(leaf) = node.get(b"") {
        if entries.len() != 1 || path.len() < 2 {
            return Err(BtError::InvalidMetadata);
        }
        let padding = attributes(leaf)?;
        let length = leaf.required(b"length")?.integer()?;
        if length > 0 && leaf.required(b"pieces root")?.bytes()?.len() != 32 {
            return Err(BtError::InvalidMetadata);
        }
        if !total.is_multiple_of(piece_length) {
            let gap = piece_length - *total % piece_length;
            push_file(
                files,
                vec![".ariax-padding".into(), files.len().to_string()],
                gap,
                true,
                total,
                limits,
            )?;
        }
        return push_file(files, path.clone(), length, padding, total, limits);
    }
    if entries.is_empty() {
        return Err(BtError::InvalidMetadata);
    }
    for (name, child) in entries {
        path.push(component(name)?);
        v2_files(child, path, files, total, piece_length, limits)?;
        path.pop();
    }
    Ok(())
}

pub(crate) fn endpoint(value: &str, tracker: bool) -> Result<String, BtError> {
    if value.len() > 4096 || value.chars().any(char::is_control) {
        return Err(BtError::Destination);
    }
    let url = url::Url::parse(value).map_err(|_| BtError::Destination)?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(BtError::Credentials);
    }
    // Authenticated tracker URLs require an explicit secret provider. They are
    // not silently serialized inside opaque metainfo/resume blobs.
    if url.query_pairs().any(|(key, _)| {
        matches!(
            key.to_ascii_lowercase().as_str(),
            "token"
                | "key"
                | "passkey"
                | "auth"
                | "password"
                | "secret"
                | "signature"
                | "access_token"
        )
    }) {
        return Err(BtError::Credentials);
    }
    if !(matches!(url.scheme(), "http" | "https") || tracker && url.scheme() == "udp")
        || url.host_str().is_none()
        || url.fragment().is_some()
    {
        return Err(BtError::Destination);
    }
    Ok(url.to_string())
}

fn parse_metadata(
    info: &Node<'_>,
    bytes: &[u8],
    limits: MetadataLimits,
) -> Result<TorrentMetadata, BtError> {
    info.dictionary()?;
    attributes(info)?;
    let name = component(
        info.get(b"name.utf-8")
            .or_else(|| info.get(b"name"))
            .ok_or(BtError::InvalidMetadata)?
            .bytes()?,
    )?;
    let piece_length = info.required(b"piece length")?.integer()?;
    if piece_length == 0 || piece_length > 16 * 1024 * 1024 {
        return Err(BtError::InvalidMetadata);
    }
    let v2 = match info.get(b"meta version").map(Node::integer).transpose()? {
        None => false,
        Some(2) => true,
        Some(_) => return Err(BtError::InvalidMetadata),
    };
    if v2 && (piece_length < 16384 || !piece_length.is_power_of_two()) {
        return Err(BtError::InvalidMetadata);
    }
    let v1_hashes = info.get(b"pieces").map(Node::bytes).transpose()?;
    if !v2 && v1_hashes.is_none() {
        return Err(BtError::InvalidMetadata);
    }
    let mut files = Vec::new();
    let mut total = 0;
    if v1_hashes.is_some() {
        if let Some(entries) = info.get(b"files") {
            if info.get(b"length").is_some() {
                return Err(BtError::InvalidMetadata);
            }
            for entry in entries.list()? {
                let padding = attributes(entry)?;
                let mut path = vec![name.clone()];
                let components = entry
                    .get(b"path.utf-8")
                    .or_else(|| entry.get(b"path"))
                    .ok_or(BtError::InvalidMetadata)?
                    .list()?;
                if components.is_empty() || components.len() >= limits.depth {
                    return Err(BtError::InvalidMetadata);
                }
                for part in components {
                    path.push(component(part.bytes()?)?);
                }
                push_file(
                    &mut files,
                    path,
                    entry.required(b"length")?.integer()?,
                    padding,
                    &mut total,
                    limits,
                )?;
            }
        } else {
            push_file(
                &mut files,
                vec![name.clone()],
                info.required(b"length")?.integer()?,
                false,
                &mut total,
                limits,
            )?;
        }
    }
    if v2 {
        let mut tree_files = Vec::new();
        let mut tree_total = 0;
        v2_files(
            info.required(b"file tree")?,
            &mut vec![name.clone()],
            &mut tree_files,
            &mut tree_total,
            piece_length,
            limits,
        )?;
        // A v2 single-file torrent uses the file name as its root.
        if tree_files.len() == 1 && tree_files[0].components == [name.clone(), name.clone()] {
            tree_files[0].components = vec![name.clone()];
        }
        if !tree_total.is_multiple_of(piece_length) {
            let gap = piece_length - tree_total % piece_length;
            let path = vec![".ariax-padding".into(), tree_files.len().to_string()];
            push_file(&mut tree_files, path, gap, true, &mut tree_total, limits)?;
        }
        if v1_hashes.is_some() {
            let real = |files: &[MetadataFile]| {
                files
                    .iter()
                    .filter(|file| !file.padding)
                    .map(|file| (file.components.clone(), file.length, file.offset))
                    .collect::<Vec<_>>()
            };
            if real(&files) != real(&tree_files) {
                return Err(BtError::IdentityMismatch);
            }
        }
        files = tree_files;
        total = tree_total;
    }
    if files.is_empty() || total == 0 {
        return Err(BtError::InvalidMetadata);
    }
    let pieces = total.div_ceil(piece_length);
    if pieces > limits.pieces {
        return Err(BtError::MetadataLimit);
    }
    if v1_hashes.is_some_and(|hashes| hashes.len() as u64 != pieces.saturating_mul(20)) {
        return Err(BtError::InvalidMetadata);
    }
    let raw = &bytes[info.range.clone()];
    let identity = BtIdentity {
        v1: v1_hashes.map(|_| {
            Sha1::digest(raw)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect()
        }),
        v2: v2.then(|| {
            Sha256::digest(raw)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect()
        }),
    };
    let private = info
        .get(b"private")
        .map(Node::integer)
        .transpose()?
        .unwrap_or(0);
    if private > 1 {
        return Err(BtError::InvalidMetadata);
    }
    Ok(TorrentMetadata {
        identity,
        name,
        files,
        piece_length,
        pieces,
        total_length: total,
        private: private != 0,
        trackers: Vec::new(),
        web_seeds: Vec::new(),
    })
}

pub fn parse_torrent(bytes: &[u8], limits: MetadataLimits) -> Result<TorrentMetadata, BtError> {
    let root = decode(bytes, limits)?;
    let mut metadata = parse_metadata(root.required(b"info")?, bytes, limits)?;
    if let Some(value) = root.get(b"announce") {
        metadata.trackers.push(endpoint(value.text()?, true)?);
    }
    if let Some(tiers) = root.get(b"announce-list") {
        for tier in tiers.list()? {
            for value in tier.list()? {
                if metadata.trackers.len() >= 64 {
                    return Err(BtError::MetadataLimit);
                }
                let url = endpoint(value.text()?, true)?;
                if !metadata.trackers.contains(&url) {
                    metadata.trackers.push(url);
                }
            }
        }
    }
    for name in [b"url-list".as_slice(), b"httpseeds"] {
        if let Some(values) = root.get(name) {
            if let Ok(value) = values.text() {
                metadata.web_seeds.push(endpoint(value, false)?);
            } else {
                for value in values.list()? {
                    if metadata.web_seeds.len() >= 64 {
                        return Err(BtError::MetadataLimit);
                    }
                    metadata.web_seeds.push(endpoint(value.text()?, false)?);
                }
            }
        }
    }
    Ok(metadata)
}

pub fn parse_info(bytes: &[u8], limits: MetadataLimits) -> Result<TorrentMetadata, BtError> {
    let info = decode(bytes, limits)?;
    parse_metadata(&info, bytes, limits)
}

fn hash(value: &str, length: usize) -> Result<String, BtError> {
    if value.len() != length || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(BtError::InvalidMagnet);
    }
    Ok(value.to_ascii_lowercase())
}

fn btih(value: &str) -> Result<String, BtError> {
    if value.len() != 32 {
        return hash(value, 40);
    }
    let mut data = Vec::with_capacity(20);
    let mut buffer = 0u32;
    let mut bits = 0;
    for byte in value.bytes() {
        let digit = match byte.to_ascii_uppercase() {
            b'A'..=b'Z' => byte.to_ascii_uppercase() - b'A',
            b'2'..=b'7' => byte - b'2' + 26,
            _ => return Err(BtError::InvalidMagnet),
        };
        buffer = (buffer << 5) | u32::from(digit);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            data.push((buffer >> bits) as u8);
        }
    }
    Ok(data.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub fn parse_magnet(value: &str) -> Result<Magnet, BtError> {
    if value.len() > 65536 || value.chars().any(char::is_control) {
        return Err(BtError::InvalidMagnet);
    }
    let url = url::Url::parse(value).map_err(|_| BtError::InvalidMagnet)?;
    if url.scheme() != "magnet"
        || url.host().is_some()
        || !url.path().is_empty()
        || url.fragment().is_some()
    {
        return Err(BtError::InvalidMagnet);
    }
    let mut magnet = Magnet {
        identity: BtIdentity::default(),
        name: None,
        trackers: Vec::new(),
        web_seeds: Vec::new(),
        peers: Vec::new(),
    };
    for (index, (key, value)) in url.query_pairs().enumerate() {
        if index >= 128 {
            return Err(BtError::MetadataLimit);
        }
        match key.as_ref() {
            "xt" => {
                let (slot, value) = if let Some(value) = value.strip_prefix("urn:btih:") {
                    (&mut magnet.identity.v1, btih(value)?)
                } else if let Some(value) = value.strip_prefix("urn:btmh:1220") {
                    (&mut magnet.identity.v2, hash(value, 64)?)
                } else {
                    return Err(BtError::InvalidMagnet);
                };
                if slot.as_ref().is_some_and(|previous| previous != &value) {
                    return Err(BtError::IdentityMismatch);
                }
                *slot = Some(value);
            }
            "dn" => {
                if magnet.name.is_some() {
                    return Err(BtError::InvalidMagnet);
                }
                magnet.name = Some(component(value.as_bytes())?);
            }
            "tr" => {
                if magnet.trackers.len() >= 64 {
                    return Err(BtError::MetadataLimit);
                }
                magnet.trackers.push(endpoint(&value, true)?);
            }
            "ws" => {
                if magnet.web_seeds.len() >= 64 {
                    return Err(BtError::MetadataLimit);
                }
                magnet.web_seeds.push(endpoint(&value, false)?);
            }
            "x.pe" => {
                if magnet.peers.len() >= 32 {
                    return Err(BtError::MetadataLimit);
                }
                magnet
                    .peers
                    .push(value.parse().map_err(|_| BtError::Destination)?);
            }
            // Unknown extensions are not forwarded into native URL/path options.
            _ => return Err(BtError::InvalidMagnet),
        }
    }
    if magnet.identity.v1.is_none() && magnet.identity.v2.is_none() {
        return Err(BtError::InvalidMagnet);
    }
    Ok(magnet)
}

#[cfg(test)]
mod tests {
    use super::*;

    const V1: &[u8] = include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/v1.torrent");
    const V2: &[u8] = include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/v2.torrent");
    const HYBRID: &[u8] =
        include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/hybrid.torrent");

    #[test]
    fn v1_v2_hybrid_identity_and_limits_cover_complete_and_rejected_inputs() {
        for (bytes, v1, v2) in [(V1, true, false), (V2, false, true), (HYBRID, true, true)] {
            let metadata = parse_torrent(bytes, MetadataLimits::default()).unwrap();
            assert_eq!(metadata.identity.v1.is_some(), v1);
            assert_eq!(metadata.identity.v2.is_some(), v2);
            assert_eq!(
                metadata.files.iter().filter(|file| !file.padding).count(),
                1
            );
            assert_eq!(metadata.files[0].components, ["payload.bin"]);
            assert_eq!(metadata.files[0].length, 5000);
            for length in 0..bytes.len() {
                assert!(parse_torrent(&bytes[..length], MetadataLimits::default()).is_err());
            }
            let limit = MetadataLimits {
                bytes: bytes.len() - 1,
                ..MetadataLimits::default()
            };
            assert_eq!(parse_torrent(bytes, limit), Err(BtError::MetadataLimit));
        }
        let mut altered = V1.to_vec();
        let offset = altered
            .windows(4)
            .position(|window| window == b"5000")
            .unwrap();
        altered[offset..offset + 4].copy_from_slice(b"9999");
        let first = parse_torrent(V1, MetadataLimits::default()).unwrap();
        let changed = parse_torrent(&altered, MetadataLimits::default()).unwrap();
        assert!(!first.identity.matches(&changed.identity));
    }

    #[test]
    fn symlinks_unsafe_names_and_secret_endpoints_fail_before_native_admission() {
        let mut symlink = V1.to_vec();
        let offset = symlink
            .windows(6)
            .position(|bytes| bytes == b"4:info")
            .unwrap()
            + 6;
        assert_eq!(symlink[offset], b'd');
        symlink.splice(offset + 1..offset + 1, b"4:attr1:l".iter().copied());
        assert_eq!(
            parse_torrent(&symlink, MetadataLimits::default()),
            Err(BtError::Symlink)
        );
        for url in [
            "https://user:secret@example.test/announce",
            "https://example.test/announce?passkey=canary",
        ] {
            assert_eq!(endpoint(url, true), Err(BtError::Credentials));
        }
        assert_eq!(
            endpoint("file:///payload", false),
            Err(BtError::Destination)
        );
        assert_eq!(component(b".."), Err(BtError::UnsafePath));
        assert_eq!(component(b"path/name"), Err(BtError::UnsafePath));
    }

    #[test]
    fn magnets_accept_exact_hashes_and_reject_ambiguous_or_secret_inputs() {
        let v1 = "0123456789abcdef0123456789abcdef01234567";
        let v2 = "ab".repeat(32);
        let magnet = parse_magnet(&format!("magnet:?xt=urn:btih:{v1}&xt=urn:btmh:1220{v2}&tr=udp%3A%2F%2Ftracker.example%3A6969&x.pe=127.0.0.1%3A6881")).unwrap();
        assert_eq!(magnet.identity.v1.as_deref(), Some(v1));
        assert_eq!(magnet.identity.v2.as_deref(), Some(v2.as_str()));
        assert_eq!(magnet.peers.len(), 1);
        assert_eq!(
            parse_magnet("magnet:?xt=urn:btih:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
                .unwrap()
                .identity
                .v1,
            Some("0".repeat(40))
        );
        for invalid in [
            "magnet:",
            "magnet:?xt=urn:btih:x",
            "magnet://host?xt=urn:btih:x",
            "magnet:?xt=urn:btmh:1220ff",
        ] {
            assert!(parse_magnet(invalid).is_err());
        }
        assert_eq!(
            parse_magnet(&format!(
                "magnet:?xt=urn:btih:{v1}&xt=urn:btih:{}",
                "0".repeat(40)
            )),
            Err(BtError::IdentityMismatch)
        );
        assert_eq!(
            parse_magnet(&format!(
                "magnet:?xt=urn:btih:{v1}&tr=https%3A%2F%2Fsecret%40example.test"
            )),
            Err(BtError::Credentials)
        );
    }
}
