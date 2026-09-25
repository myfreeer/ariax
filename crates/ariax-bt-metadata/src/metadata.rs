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

/// Validate a tracker before it can enter a task's persisted option snapshot.
pub fn validate_tracker(value: &str) -> Result<String, BtError> {
    endpoint(value, true)
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
        } else {
            files = tree_files;
            total = tree_total;
        }
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
    // Libtorrent 2.1 does not implement BEP 17 HTTP seeds.
    if root.get(b"httpseeds").is_some() {
        return Err(BtError::UnsupportedOption);
    }
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
    for name in [b"url-list".as_slice()] {
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

/// Extract the exact hashed info dictionary; never re-encode its identity.
pub fn info_section(bytes: &[u8], limits: MetadataLimits) -> Result<&[u8], BtError> {
    let root = decode(bytes, limits)?;
    let info = root.required(b"info")?;
    Ok(&bytes[info.range.clone()])
}

/// Add BEP 19 web seeds while preserving all existing hashed metadata bytes.
pub fn with_web_seeds(
    bytes: &[u8],
    seeds: &[String],
    limits: MetadataLimits,
) -> Result<Vec<u8>, BtError> {
    let metadata = parse_torrent(bytes, limits)?;
    if seeds.len().saturating_add(metadata.web_seeds.len()) > 64 {
        return Err(BtError::MetadataLimit);
    }
    let mut urls = metadata.web_seeds;
    for seed in seeds {
        urls.push(endpoint(seed, false)?);
    }
    let maximum = bytes
        .len()
        .saturating_add(urls.iter().map(|url| url.len() + 16).sum::<usize>())
        .saturating_add(32);
    if maximum > limits.bytes {
        return Err(BtError::MetadataLimit);
    }
    let root = decode(bytes, limits)?;
    let mut entries = root
        .dictionary()?
        .iter()
        .filter(|(key, _)| *key != b"url-list")
        .map(|(key, node)| (*key, &bytes[node.range.clone()]))
        .collect::<Vec<_>>();
    let mut list = Vec::new();
    list.push(b'l');
    for url in urls {
        list.extend_from_slice(format!("{}:", url.len()).as_bytes());
        list.extend_from_slice(url.as_bytes());
    }
    list.push(b'e');
    entries.push((b"url-list", &list));
    entries.sort_by_key(|(key, _)| *key);
    let mut output = Vec::with_capacity(maximum);
    output.push(b'd');
    for (key, value) in entries {
        output.extend_from_slice(format!("{}:", key.len()).as_bytes());
        output.extend_from_slice(key);
        output.extend_from_slice(value);
    }
    output.push(b'e');
    Ok(output)
}

/// A metadata-only magnet export has no cached endpoint or credential state.
pub fn torrent_from_info(info: &[u8], limits: MetadataLimits) -> Result<Vec<u8>, BtError> {
    parse_info(info, limits)?;
    if info.len().saturating_add(8) > limits.bytes {
        return Err(BtError::MetadataLimit);
    }
    let mut output = Vec::with_capacity(info.len() + 8);
    output.extend_from_slice(b"d4:info");
    output.extend_from_slice(info);
    output.push(b'e');
    Ok(output)
}

/// Apply explicit tracker additions/exclusions without touching the info hash.
pub fn with_trackers(
    bytes: &[u8],
    add: &[String],
    exclude: &[String],
    limits: MetadataLimits,
) -> Result<Vec<u8>, BtError> {
    let mut trackers = parse_torrent(bytes, limits)?.trackers;
    trackers.retain(|url| {
        !exclude
            .iter()
            .any(|pattern| pattern == "*" || pattern == url)
    });
    for url in add {
        let url = endpoint(url, true)?;
        if !trackers.contains(&url) {
            trackers.push(url);
        }
    }
    if trackers.len() > 64 {
        return Err(BtError::MetadataLimit);
    }
    let maximum = bytes
        .len()
        .saturating_add(trackers.iter().map(|url| url.len() + 32).sum::<usize>())
        .saturating_add(64);
    if maximum > limits.bytes {
        return Err(BtError::MetadataLimit);
    }
    let root = decode(bytes, limits)?;
    let mut entries = root
        .dictionary()?
        .iter()
        .filter(|(key, _)| *key != b"announce" && *key != b"announce-list")
        .map(|(key, node)| (*key, &bytes[node.range.clone()]))
        .collect::<Vec<_>>();
    let mut list = vec![b'l'];
    for url in &trackers {
        list.push(b'l');
        list.extend_from_slice(format!("{}:", url.len()).as_bytes());
        list.extend_from_slice(url.as_bytes());
        list.push(b'e');
    }
    list.push(b'e');
    if !trackers.is_empty() {
        entries.push((b"announce-list", &list));
    }
    entries.sort_by_key(|(key, _)| *key);
    let mut output = Vec::with_capacity(maximum);
    output.push(b'd');
    for (key, value) in entries {
        output.extend_from_slice(format!("{}:", key.len()).as_bytes());
        output.extend_from_slice(key);
        output.extend_from_slice(value);
    }
    output.push(b'e');
    Ok(output)
}

pub fn magnet_with_trackers(
    value: &str,
    add: &[String],
    exclude: &[String],
) -> Result<String, BtError> {
    let mut magnet = parse_magnet(value)?;
    magnet.trackers.retain(|url| {
        !exclude
            .iter()
            .any(|pattern| pattern == "*" || pattern == url)
    });
    for url in add {
        let url = endpoint(url, true)?;
        if !magnet.trackers.contains(&url) {
            magnet.trackers.push(url);
        }
    }
    if magnet.trackers.len() > 64 {
        return Err(BtError::MetadataLimit);
    }
    let mut url = url::Url::parse(value).map_err(|_| BtError::InvalidMagnet)?;
    let pairs = url
        .query_pairs()
        .filter(|(name, _)| name != "tr")
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    url.set_query(None);
    url.query_pairs_mut()
        .extend_pairs(pairs)
        .extend_pairs(magnet.trackers.iter().map(|value| ("tr", value)));
    let output = url.to_string();
    parse_magnet(&output)?;
    Ok(output)
}

/// Validates the pinned native resume format without trusting paths or endpoints.
pub fn validate_resume(bytes: &[u8], expected: &BtIdentity) -> Result<(), BtError> {
    use crate::bencode::Value;
    let limits = MetadataLimits {
        bytes: 64 * 1024 * 1024,
        tokens: 1_000_000,
        ..MetadataLimits::default()
    };
    let root = decode(bytes, limits)?;
    if root.required(b"file-format")?.bytes()? != b"libtorrent resume file"
        || root.required(b"file-version")?.integer()? != 2
    {
        return Err(BtError::InvalidMetadata);
    }
    fn empty(value: &Node<'_>) -> bool {
        match &value.value {
            Value::Bytes(value) => value.is_empty(),
            Value::List(values) => values.iter().all(empty),
            Value::Dictionary(values) => values.is_empty(),
            Value::Integer(_) => false,
        }
    }
    for (name, value) in root.dictionary()? {
        match *name {
            b"save_path" | b"part_file_dir" | b"root_certificate" | b"mapped_files"
            | b"trackers" | b"url-list" | b"httpseeds" | b"peers" | b"peers6" | b"banned_peers"
            | b"banned_peers6" | b"url" | b"comment" | b"created by" => {
                if !empty(value) {
                    return Err(BtError::Credentials);
                }
            }
            b"file-format" | b"file-version" | b"info-hash" | b"info-hash2" | b"info"
            | b"pieces" | b"verified" | b"trees" | b"unfinished" | b"file_priority"
            | b"piece_priority" => {}
            b"libtorrent-version" => {
                if value.bytes()?.len() > 32 || !value.text()?.starts_with("2.1.1") {
                    return Err(BtError::InvalidMetadata);
                }
            }
            b"allocation" => {
                if !matches!(value.bytes()?, b"full" | b"sparse") {
                    return Err(BtError::InvalidMetadata);
                }
            }
            b"name" => {
                component(value.bytes()?)?;
            }
            b"total_uploaded"
            | b"total_downloaded"
            | b"active_time"
            | b"finished_time"
            | b"seeding_time"
            | b"last_seen_complete"
            | b"last_download"
            | b"last_upload"
            | b"num_complete"
            | b"num_incomplete"
            | b"num_downloaded"
            | b"seed_mode"
            | b"upload_mode"
            | b"share_mode"
            | b"apply_ip_filter"
            | b"paused"
            | b"auto_managed"
            | b"super_seeding"
            | b"sequential_download"
            | b"stop_when_ready"
            | b"disable_dht"
            | b"disable_lsd"
            | b"disable_pex"
            | b"disable_v1_hashes"
            | b"added_time"
            | b"completed_time"
            | b"creation date"
            | b"upload_rate_limit"
            | b"download_rate_limit"
            | b"max_connections"
            | b"max_uploads" => {
                if !matches!(value.value, Value::Integer(_)) {
                    return Err(BtError::InvalidMetadata);
                }
            }
            _ => return Err(BtError::InvalidMetadata),
        }
    }
    let read_hash = |name: &[u8], length: usize| -> Result<Option<String>, BtError> {
        let Some(value) = root.get(name) else {
            return Ok(None);
        };
        let bytes = value.bytes()?;
        if bytes.len() != length {
            return Err(BtError::IdentityMismatch);
        }
        Ok(bytes
            .iter()
            .any(|byte| *byte != 0)
            .then(|| bytes.iter().map(|byte| format!("{byte:02x}")).collect()))
    };
    let identity = BtIdentity {
        v1: read_hash(b"info-hash", 20)?,
        v2: read_hash(b"info-hash2", 32)?,
    };
    if !expected.matches(&identity) {
        return Err(BtError::IdentityMismatch);
    }
    if let Some(info) = root.get(b"info") {
        let metadata = parse_info(&bytes[info.range.clone()], MetadataLimits::default())?;
        if metadata.identity != identity {
            return Err(BtError::IdentityMismatch);
        }
    }
    Ok(())
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

    fn resume_fixture(identity: &BtIdentity, extra: Option<(&str, &[u8])>) -> Vec<u8> {
        let mut fields = std::collections::BTreeMap::from([
            ("file-format", b"22:libtorrent resume file".to_vec()),
            ("file-version", b"i2e".to_vec()),
        ]);
        for (key, hash) in [("info-hash", &identity.v1), ("info-hash2", &identity.v2)] {
            if let Some(hash) = hash {
                let mut bytes = format!("{}:", hash.len() / 2).into_bytes();
                bytes.extend(
                    (0..hash.len())
                        .step_by(2)
                        .map(|index| u8::from_str_radix(&hash[index..index + 2], 16).unwrap()),
                );
                fields.insert(key, bytes);
            }
        }
        if let Some((key, value)) = extra {
            fields.insert(key, value.to_vec());
        }
        let mut bytes = vec![b'd'];
        for (key, value) in fields {
            bytes.extend(format!("{}:{key}", key.len()).bytes());
            bytes.extend(value);
        }
        bytes.push(b'e');
        bytes
    }

    #[test]
    fn resume_data_accepts_current_progress_and_rejects_authority_or_changed_identity() {
        for torrent in [V1, V2, HYBRID] {
            let identity = parse_torrent(torrent, MetadataLimits::default())
                .unwrap()
                .identity;
            let bytes = resume_fixture(&identity, Some(("trackers", b"llee")));
            validate_resume(&bytes, &identity).unwrap();
            for length in 0..bytes.len() {
                assert!(validate_resume(&bytes[..length], &identity).is_err());
            }
            for (name, value) in [
                ("save_path", b"7:/secret".as_slice()),
                ("mapped_files", b"l10:../outsidee".as_slice()),
                ("peers", b"6:secret".as_slice()),
                ("url-list", b"l6:secrete".as_slice()),
                ("unknown", b"i1e".as_slice()),
                ("file-version", b"i1e".as_slice()),
            ] {
                assert!(
                    validate_resume(&resume_fixture(&identity, Some((name, value))), &identity)
                        .is_err()
                );
            }
            let changed = BtIdentity {
                v1: Some("ff".repeat(20)),
                v2: Some("ff".repeat(32)),
            };
            assert_eq!(
                validate_resume(&bytes, &changed),
                Err(BtError::IdentityMismatch)
            );
        }
    }

    #[test]
    fn tracker_and_web_seed_overlays_preserve_all_torrent_identities() {
        for torrent in [V1, V2, HYBRID] {
            let identity = parse_torrent(torrent, MetadataLimits::default())
                .unwrap()
                .identity;
            let with_seed = with_web_seeds(
                torrent,
                &["https://seed.example/payload".into()],
                MetadataLimits::default(),
            )
            .unwrap();
            let updated = with_trackers(
                &with_seed,
                &["udp://tracker.example:6969".into()],
                &[],
                MetadataLimits::default(),
            )
            .unwrap();
            let metadata = parse_torrent(&updated, MetadataLimits::default()).unwrap();
            assert_eq!(metadata.identity, identity);
            assert_eq!(metadata.web_seeds, ["https://seed.example/payload"]);
            assert_eq!(metadata.trackers, ["udp://tracker.example:6969"]);
            assert!(
                with_web_seeds(
                    torrent,
                    &["https://user:secret@seed.example/".into()],
                    MetadataLimits::default()
                )
                .is_err()
            );
        }
    }

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
            assert_eq!(metadata.files.len(), if v2 && !v1 { 2 } else { 1 });
            assert_eq!(metadata.total_length, if v2 && !v1 { 16384 } else { 5000 });
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
