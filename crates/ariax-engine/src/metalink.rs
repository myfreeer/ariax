//! Bounded Metalink v3/v4 pull parsing. Only the current element/file and
//! charged canonical results are retained; no DOM or external entity loader.
use crate::{ContentChecksum, TransferProtocol, VerificationManifest};
use ariax_storage::{
    JournalDigest, JournalDigestAlgorithm, PathPlatform, SafePathBuilder, SafeRelativePath,
};
use quick_xml::{
    Reader,
    events::{BytesStart, Event},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

pub const METALINK_DOCUMENT_BYTES: usize = 64 * 1024 * 1024;
pub use crate::MAX_METALINK_DOCUMENT_BYTES;
pub const MAX_METALINK_FILES: usize = 262_144;
pub const MAX_METALINK_SOURCES: usize = 1024;
pub const MAX_XML_DEPTH: usize = 64;
pub const MAX_XML_ATTRIBUTES: usize = 128;
pub const MAX_XML_ATTRIBUTE_BYTES: usize = 64 * 1024;
pub const MAX_XML_TEXT_BYTES: usize = 8 * 1024 * 1024;
const V3: &str = "http://www.metalinker.org/";
const V4: &str = "urn:ietf:params:xml:ns:metalink";

#[derive(Clone, Debug)]
pub struct MetalinkOptions {
    pub max_document_bytes: usize,
    pub max_files: usize,
    pub max_sources: usize,
    pub metadata_bytes: usize,
    pub base_uri: Option<String>,
    pub select_file: Option<String>,
    pub language: Option<String>,
    pub os: Option<String>,
    pub version: Option<String>,
    pub location: Option<String>,
    pub preferred_protocol: Option<TransferProtocol>,
    pub unique_protocol: bool,
    pub user_checksum: Option<ContentChecksum>,
    pub default_chunk_length: u64,
}
impl Default for MetalinkOptions {
    fn default() -> Self {
        Self {
            max_document_bytes: METALINK_DOCUMENT_BYTES,
            max_files: MAX_METALINK_FILES,
            max_sources: MAX_METALINK_SOURCES,
            metadata_bytes: METALINK_DOCUMENT_BYTES,
            base_uri: None,
            select_file: None,
            language: None,
            os: None,
            version: None,
            location: None,
            preferred_protocol: None,
            unique_protocol: true,
            user_checksum: None,
            default_chunk_length: crate::DEFAULT_HTTP_PIECE_LENGTH,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetalinkError {
    Limit,
    Xml,
    Encoding,
    Entity,
    Schema,
    UnsafePath,
    PathCollision,
    InvalidSize,
    InvalidChecksum,
    UnsupportedChecksum,
    InvalidSource,
    NoUsableSource,
    Selection,
    UnsupportedMetaurl,
}
impl fmt::Display for MetalinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Metalink {self:?}")
    }
}
impl std::error::Error for MetalinkError {}

#[derive(Clone, Eq, PartialEq)]
pub struct MetalinkSource {
    pub uri: String,
    pub protocol: TransferProtocol,
    pub priority: i64,
    pub location: Option<String>,
}
impl fmt::Debug for MetalinkSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetalinkSource")
            .field("protocol", &self.protocol)
            .field("priority", &self.priority)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetalinkFile {
    pub index: u32,
    pub name: SafeRelativePath,
    pub sources: Vec<MetalinkSource>,
    pub verification: Arc<VerificationManifest>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetalinkDocument {
    pub version: u8,
    pub files: Vec<MetalinkFile>,
    pub retained_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Root,
    Files,
    File,
    Resources,
    Verification,
    Url,
    Size,
    Hash,
    Pieces,
    Language,
    Os,
    Version,
    Metaurl,
    Extension,
}
impl Kind {
    fn text(self) -> bool {
        matches!(
            self,
            Self::Url
                | Self::Size
                | Self::Hash
                | Self::Language
                | Self::Os
                | Self::Version
                | Self::Metaurl
        )
    }
}
struct Frame {
    attrs: BTreeMap<String, String>,
    kind: Kind,
    text: String,
}
struct Pieces {
    algorithm: Option<JournalDigestAlgorithm>,
    length: u64,
    hashes: BTreeMap<u64, JournalDigest>,
}
struct FileBuilder {
    index: u32,
    name: SafeRelativePath,
    size: Option<u64>,
    sources: Vec<MetalinkSource>,
    source_count: usize,
    whole: BTreeMap<u8, JournalDigest>,
    pieces: Vec<Pieces>,
    current_pieces: Option<Pieces>,
    had_checksum: bool,
    had_pieces: bool,
    had_metaurl: bool,
    language: Vec<String>,
    os: Vec<String>,
    version: Option<String>,
}
struct Parser<'a> {
    options: &'a MetalinkOptions,
    stack: Vec<Frame>,
    version: u8,
    closed: bool,
    declared: bool,
    current: Option<FileBuilder>,
    files: Vec<MetalinkFile>,
    count: u32,
    retained: usize,
    paths: BTreeSet<String>,
    selection: Vec<(u32, u32)>,
    base: Option<url::Url>,
}

pub fn parse_metalink(
    bytes: &[u8],
    options: &MetalinkOptions,
) -> Result<MetalinkDocument, MetalinkError> {
    if options.max_document_bytes == 0
        || options.max_document_bytes > MAX_METALINK_DOCUMENT_BYTES
        || options.max_files == 0
        || options.max_files > MAX_METALINK_FILES
        || options.max_sources == 0
        || options.max_sources > MAX_METALINK_SOURCES
        || options.metadata_bytes == 0
        || options.metadata_bytes > METALINK_DOCUMENT_BYTES
        || options.default_chunk_length == 0
        || options.default_chunk_length > crate::MAX_HTTP_PIECE_LENGTH
        || bytes.len() > options.max_document_bytes
    {
        return Err(MetalinkError::Limit);
    }
    let text = std::str::from_utf8(bytes).map_err(|_| MetalinkError::Encoding)?;
    // XML 1.0 rejects control characters, including in ignored extension data.
    if text.chars().any(|c| {
        c == '\0'
            || (c < '\u{20}' && !matches!(c, '\t' | '\r' | '\n'))
            || matches!(c, '\u{fffe}' | '\u{ffff}')
    }) {
        return Err(MetalinkError::Xml);
    }
    let base = options
        .base_uri
        .as_deref()
        .map(|base| url::Url::parse(base).map_err(|_| MetalinkError::InvalidSource))
        .transpose()?;
    if let Some(base) = &base
        && !TransferProtocol::parse(base.scheme()).is_ok_and(TransferProtocol::is_http)
    {
        return Err(MetalinkError::InvalidSource);
    }
    let mut parser = Parser {
        options,
        stack: Vec::with_capacity(MAX_XML_DEPTH),
        version: 0,
        closed: false,
        declared: false,
        current: None,
        files: Vec::new(),
        count: 0,
        retained: 0,
        paths: BTreeSet::new(),
        selection: parse_selection(options.select_file.as_deref())?,
        base,
    };
    let mut reader = Reader::from_str(text);
    reader.config_mut().check_end_names = true;
    loop {
        let event = reader.read_event().map_err(|_| MetalinkError::Xml)?;
        match event {
            Event::Start(start) => parser.start(start, &reader)?,
            Event::Empty(start) => {
                parser.start(start, &reader)?;
                parser.end()?;
            }
            Event::End(_) => parser.end()?,
            Event::Text(text) => {
                if text.len() > MAX_XML_TEXT_BYTES {
                    return Err(MetalinkError::Limit);
                }
                parser.text(&text.decode().map_err(|_| MetalinkError::Encoding)?)?;
            }
            Event::CData(text) => {
                if text.len() > MAX_XML_TEXT_BYTES {
                    return Err(MetalinkError::Limit);
                }
                parser.text(&text.decode().map_err(|_| MetalinkError::Encoding)?)?;
            }
            Event::GeneralRef(reference) => {
                let name = reference.decode().map_err(|_| MetalinkError::Encoding)?;
                if name.len() > 32 {
                    return Err(MetalinkError::Entity);
                }
                let escaped = format!("&{name};");
                let value =
                    quick_xml::escape::unescape(&escaped).map_err(|_| MetalinkError::Entity)?;
                parser.text(&value)?;
            }
            Event::DocType(_) => return Err(MetalinkError::Entity),
            Event::Decl(decl) => {
                if parser.declared || parser.version != 0 || !parser.stack.is_empty() {
                    return Err(MetalinkError::Xml);
                }
                parser.declared = true;
                if decl.version().map_err(|_| MetalinkError::Xml)?.as_ref() != b"1.0" {
                    return Err(MetalinkError::Encoding);
                }
                if let Some(encoding) = decl.encoding() {
                    let encoding = encoding.map_err(|_| MetalinkError::Encoding)?;
                    if !encoding.eq_ignore_ascii_case(b"UTF-8") {
                        return Err(MetalinkError::Encoding);
                    }
                }
            }
            Event::PI(_) => return Err(MetalinkError::Xml),
            Event::Comment(comment) => {
                if comment.len() > MAX_XML_TEXT_BYTES {
                    return Err(MetalinkError::Limit);
                }
            }
            Event::Eof => break,
        }
    }
    if !parser.closed || !parser.stack.is_empty() || parser.current.is_some() {
        return Err(MetalinkError::Xml);
    }
    if parser.selection.iter().any(|(_, end)| *end > parser.count) {
        return Err(MetalinkError::Selection);
    }
    if parser.files.is_empty() {
        return Err(MetalinkError::Selection);
    }
    Ok(MetalinkDocument {
        version: parser.version,
        files: parser.files,
        retained_bytes: parser.retained,
    })
}

impl Parser<'_> {
    fn charge(&mut self, bytes: usize) -> Result<(), MetalinkError> {
        self.retained = self
            .retained
            .checked_add(bytes)
            .ok_or(MetalinkError::Limit)?;
        if self.retained > self.options.metadata_bytes {
            return Err(MetalinkError::Limit);
        }
        Ok(())
    }
    fn start(
        &mut self,
        start: BytesStart<'_>,
        reader: &Reader<&[u8]>,
    ) -> Result<(), MetalinkError> {
        if self.closed
            || self.stack.len() == MAX_XML_DEPTH
            || start.name().as_ref().len() > MAX_XML_ATTRIBUTE_BYTES
        {
            return Err(MetalinkError::Limit);
        }
        let mut attrs = BTreeMap::new();
        // Attribute iteration disables the library's growing duplicate set;
        // our bounded map checks duplicates only after enforcing the count.
        let mut iterator = start.attributes();
        iterator.with_checks(false);
        for (index, attr) in iterator.enumerate() {
            if index == MAX_XML_ATTRIBUTES {
                return Err(MetalinkError::Limit);
            }
            let attr = attr.map_err(|_| MetalinkError::Xml)?;
            if attr.value.len() > MAX_XML_ATTRIBUTE_BYTES
                || attr.key.as_ref().len() > MAX_XML_ATTRIBUTE_BYTES
            {
                return Err(MetalinkError::Limit);
            }
            let key =
                std::str::from_utf8(attr.key.as_ref()).map_err(|_| MetalinkError::Encoding)?;
            let value = attr
                .decoded_and_normalized_value(quick_xml::XmlVersion::Implicit1_0, reader.decoder())
                .map_err(|_| MetalinkError::Entity)?;
            if attrs.insert(key.to_owned(), value.into_owned()).is_some() {
                return Err(MetalinkError::Xml);
            }
        }
        let name = std::str::from_utf8(start.name().as_ref())
            .map_err(|_| MetalinkError::Encoding)?
            .to_owned();
        let (prefix, local) = name
            .split_once(':')
            .map_or((None, name.as_str()), |(p, n)| (Some(p), n));
        if local.contains(':') {
            return Err(MetalinkError::Xml);
        }
        let namespace_key =
            prefix.map_or_else(|| "xmlns".to_owned(), |prefix| format!("xmlns:{prefix}"));
        let namespace = attrs.get(&namespace_key).or_else(|| {
            self.stack
                .iter()
                .rev()
                .find_map(|frame| frame.attrs.get(&namespace_key))
        });
        if prefix.is_some() && namespace.is_none() {
            return Err(MetalinkError::Schema);
        }
        let namespace = namespace.map(String::as_str).unwrap_or("");
        let parent = self.stack.last().map(|frame| frame.kind);
        let kind = if parent.is_none() {
            if local != "metalink" {
                return Err(MetalinkError::Schema);
            }
            self.version = match namespace {
                V3 => 3,
                V4 => 4,
                _ => return Err(MetalinkError::Schema),
            };
            if self.version == 3 && attrs.get("version").is_some_and(|v| v != "3.0") {
                return Err(MetalinkError::Schema);
            }
            Kind::Root
        } else if namespace != if self.version == 3 { V3 } else { V4 } {
            Kind::Extension
        } else {
            match (parent, local) {
                (Some(Kind::Root), "files") if self.version == 3 => Kind::Files,
                (Some(Kind::Root), "file") if self.version == 4 => Kind::File,
                (Some(Kind::Files), "file") if self.version == 3 => Kind::File,
                (Some(Kind::File), "resources") if self.version == 3 => Kind::Resources,
                (Some(Kind::File), "verification") if self.version == 3 => Kind::Verification,
                (Some(Kind::File), "size") => Kind::Size,
                (Some(Kind::File), "language") => Kind::Language,
                (Some(Kind::File), "os") => Kind::Os,
                (Some(Kind::File), "version") => Kind::Version,
                (Some(Kind::File), "url") if self.version == 4 => Kind::Url,
                (Some(Kind::Resources), "url") => Kind::Url,
                (Some(Kind::File), "hash" | "pieces") if self.version == 4 => {
                    if local == "hash" {
                        Kind::Hash
                    } else {
                        Kind::Pieces
                    }
                }
                (Some(Kind::Verification), "hash" | "pieces") => {
                    if local == "hash" {
                        Kind::Hash
                    } else {
                        Kind::Pieces
                    }
                }
                (Some(Kind::Pieces), "hash") => Kind::Hash,
                (Some(Kind::File), "metaurl") => Kind::Metaurl,
                _ => Kind::Extension,
            }
        };
        if self.stack.last().is_some_and(|frame| frame.kind.text()) {
            return Err(MetalinkError::Schema);
        }
        if kind == Kind::File {
            if self.current.is_some() {
                return Err(MetalinkError::Schema);
            }
            self.count += 1;
            if self.count as usize > self.options.max_files {
                return Err(MetalinkError::Limit);
            }
            let text = attrs.get("name").ok_or(MetalinkError::UnsafePath)?;
            let name =
                SafePathBuilder::from_metadata_components(text.split('/'), PathPlatform::Windows)
                    .map_err(|_| MetalinkError::UnsafePath)?;
            let key = name.canonical_string().to_lowercase();
            if self.paths.contains(&key)
                || self
                    .paths
                    .range(key.clone()..)
                    .next()
                    .is_some_and(|existing| existing.starts_with(&format!("{key}/")))
                || key
                    .match_indices('/')
                    .any(|(i, _)| self.paths.contains(&key[..i]))
            {
                return Err(MetalinkError::PathCollision);
            }
            self.charge(name.encoded_len().saturating_mul(3).saturating_add(512))?;
            self.paths.insert(key);
            self.current = Some(FileBuilder {
                index: self.count,
                name,
                size: None,
                sources: Vec::new(),
                source_count: 0,
                whole: BTreeMap::new(),
                pieces: Vec::new(),
                current_pieces: None,
                had_checksum: false,
                had_pieces: false,
                had_metaurl: false,
                language: Vec::new(),
                os: Vec::new(),
                version: None,
            });
        }
        if kind == Kind::Pieces {
            let file = self.current.as_mut().ok_or(MetalinkError::Schema)?;
            file.had_pieces = true;
            file.had_checksum = true;
            if file.current_pieces.is_some() || file.pieces.len() == 16 {
                return Err(MetalinkError::Limit);
            }
            let length = attrs
                .get("length")
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0 && *v <= crate::MAX_HTTP_PIECE_LENGTH)
                .ok_or(MetalinkError::InvalidChecksum)?;
            let algorithm = algorithm(attrs.get("type").ok_or(MetalinkError::InvalidChecksum)?);
            file.current_pieces = Some(Pieces {
                algorithm,
                length,
                hashes: BTreeMap::new(),
            });
        }
        self.stack.push(Frame {
            attrs,
            kind,
            text: String::new(),
        });
        Ok(())
    }
    fn text(&mut self, value: &str) -> Result<(), MetalinkError> {
        let Some(frame) = self.stack.last_mut() else {
            return if value.trim().is_empty() {
                Ok(())
            } else {
                Err(MetalinkError::Xml)
            };
        };
        if frame.text.len().saturating_add(value.len()) > MAX_XML_TEXT_BYTES {
            return Err(MetalinkError::Limit);
        }
        if frame.kind.text() {
            frame.text.push_str(value);
        } else if frame.kind != Kind::Extension && !value.trim().is_empty() {
            return Err(MetalinkError::Schema);
        }
        Ok(())
    }
    fn end(&mut self) -> Result<(), MetalinkError> {
        let frame = self.stack.pop().ok_or(MetalinkError::Xml)?;
        let text = frame.text.trim();
        match frame.kind {
            Kind::Root => self.closed = true,
            Kind::Size => {
                let size = text
                    .parse::<u64>()
                    .map_err(|_| MetalinkError::InvalidSize)?;
                let file = self.current.as_mut().ok_or(MetalinkError::Schema)?;
                if file.size.replace(size).is_some() {
                    return Err(MetalinkError::InvalidSize);
                }
            }
            Kind::Language | Kind::Os | Kind::Version => {
                if text.is_empty() || text.len() > 1024 {
                    return Err(MetalinkError::Limit);
                }
                self.charge(text.len() + 64)?;
                let file = self.current.as_mut().ok_or(MetalinkError::Schema)?;
                match frame.kind {
                    Kind::Language => file.language.push(text.to_owned()),
                    Kind::Os => file.os.push(text.to_owned()),
                    _ => {
                        if file.version.replace(text.to_owned()).is_some() {
                            return Err(MetalinkError::Schema);
                        }
                    }
                }
            }
            Kind::Hash => {
                self.charge(128)?;
                let file = self.current.as_mut().ok_or(MetalinkError::Schema)?;
                file.had_checksum = true;
                let pieces = file.current_pieces.as_mut();
                let alg = if let Some(pieces) = pieces.as_ref() {
                    pieces.algorithm
                } else {
                    algorithm(
                        frame
                            .attrs
                            .get("type")
                            .ok_or(MetalinkError::InvalidChecksum)?,
                    )
                };
                if let Some(alg) = alg {
                    let digest = ContentChecksum::parse(&format!("{}={text}", alg.code()))
                        .map_err(|_| MetalinkError::InvalidChecksum)?
                        .journal_digest();
                    if let Some(pieces) = pieces {
                        if pieces.hashes.len() == ariax_storage::MAX_VERIFICATION_CHUNKS {
                            return Err(MetalinkError::Limit);
                        }
                        let index = if self.version == 3 {
                            frame
                                .attrs
                                .get("piece")
                                .and_then(|value| value.parse::<u64>().ok())
                                .ok_or(MetalinkError::InvalidChecksum)?
                        } else {
                            pieces.hashes.len() as u64
                        };
                        if index >= ariax_storage::MAX_VERIFICATION_CHUNKS as u64
                            || pieces.hashes.insert(index, digest).is_some()
                        {
                            return Err(MetalinkError::InvalidChecksum);
                        }
                    } else if file
                        .whole
                        .insert(rank(alg), digest.clone())
                        .is_some_and(|previous| previous != digest)
                    {
                        return Err(MetalinkError::InvalidChecksum);
                    }
                }
            }
            Kind::Pieces => {
                let file = self.current.as_mut().ok_or(MetalinkError::Schema)?;
                file.pieces
                    .push(file.current_pieces.take().ok_or(MetalinkError::Schema)?);
            }
            Kind::Url => self.source(text, &frame.attrs)?,
            Kind::Metaurl => {
                self.current
                    .as_mut()
                    .ok_or(MetalinkError::Schema)?
                    .had_metaurl = true
            }
            Kind::File => self.finish_file()?,
            _ => {}
        }
        Ok(())
    }
    fn source(
        &mut self,
        text: &str,
        attrs: &BTreeMap<String, String>,
    ) -> Result<(), MetalinkError> {
        if text.is_empty() || text.len() > 16 * 1024 {
            return Err(MetalinkError::InvalidSource);
        }
        let file = self.current.as_mut().ok_or(MetalinkError::Schema)?;
        file.source_count += 1;
        if file.source_count > self.options.max_sources {
            return Err(MetalinkError::Limit);
        }
        let url = match url::Url::parse(text) {
            Ok(url) => url,
            Err(url::ParseError::RelativeUrlWithoutBase) => self
                .base
                .as_ref()
                .ok_or(MetalinkError::InvalidSource)?
                .join(text)
                .map_err(|_| MetalinkError::InvalidSource)?,
            Err(_) => return Err(MetalinkError::InvalidSource),
        };
        let Ok(protocol) = TransferProtocol::parse(url.scheme()) else {
            return Ok(());
        };
        if !protocol.enabled() {
            return Ok(());
        }
        if url.host_str().is_none()
            || url.fragment().is_some()
            || (protocol.is_http() && (!url.username().is_empty() || url.password().is_some()))
        {
            return Err(MetalinkError::InvalidSource);
        }
        crate::transfer_task::decode_uri_component(url.path())
            .map_err(|_| MetalinkError::InvalidSource)?;
        if let Some(kind) = attrs.get("type")
            && kind != protocol.code()
        {
            return Err(MetalinkError::InvalidSource);
        }
        let priority = if self.version == 3 {
            100 - attrs
                .get("preference")
                .map(|v| {
                    v.parse::<i64>()
                        .ok()
                        .filter(|v| (1..=100).contains(v))
                        .ok_or(MetalinkError::InvalidSource)
                })
                .transpose()?
                .unwrap_or(50)
        } else {
            attrs
                .get("priority")
                .map(|v| {
                    v.parse::<i64>()
                        .ok()
                        .filter(|v| (1..=999999).contains(v))
                        .ok_or(MetalinkError::InvalidSource)
                })
                .transpose()?
                .unwrap_or(999999)
        };
        let location = attrs.get("location").cloned();
        if location.as_ref().is_some_and(|value| value.len() > 64) {
            return Err(MetalinkError::InvalidSource);
        }
        let uri = url.to_string();
        self.charge(uri.len().saturating_mul(2).saturating_add(256))?;
        let file = self.current.as_mut().ok_or(MetalinkError::Schema)?;
        if file.sources.iter().any(|source| source.uri == uri) {
            return Err(MetalinkError::InvalidSource);
        }
        file.sources.push(MetalinkSource {
            uri,
            protocol,
            priority,
            location,
        });
        Ok(())
    }
    fn finish_file(&mut self) -> Result<(), MetalinkError> {
        let mut file = self.current.take().ok_or(MetalinkError::Schema)?;
        let total = file.size.ok_or(MetalinkError::InvalidSize)?;
        let mut strongest: Option<(u8, u64, Vec<JournalDigest>)> = None;
        let mut seen = BTreeSet::new();
        for pieces in file.pieces {
            let Some(alg) = pieces.algorithm else {
                continue;
            };
            if !seen.insert(rank(alg)) {
                return Err(MetalinkError::InvalidChecksum);
            }
            if total.div_ceil(pieces.length) != pieces.hashes.len() as u64
                || pieces
                    .hashes
                    .keys()
                    .enumerate()
                    .any(|(i, index)| i as u64 != *index)
            {
                continue;
            }
            if strongest
                .as_ref()
                .is_none_or(|(best, _, _)| rank(alg) > *best)
            {
                strongest = Some((
                    rank(alg),
                    pieces.length,
                    pieces.hashes.into_values().collect(),
                ));
            }
        }
        if file.had_pieces && strongest.is_none() {
            return Err(MetalinkError::InvalidChecksum);
        }
        let mut whole = file
            .whole
            .pop_last()
            .map(|(_, digest)| vec![digest])
            .unwrap_or_default();
        if file.had_checksum && whole.is_empty() && strongest.is_none() {
            return Err(MetalinkError::UnsupportedChecksum);
        }
        if let Some(user) = &self.options.user_checksum
            && !whole.contains(&user.journal_digest())
        {
            whole.push(user.journal_digest());
        }
        let (_, length, chunks) =
            strongest.unwrap_or((0, self.options.default_chunk_length, Vec::new()));
        let manifest = VerificationManifest::new(total, length, chunks, whole)
            .map_err(|_| MetalinkError::InvalidChecksum)?;
        if !self.selection.is_empty()
            && !self
                .selection
                .iter()
                .any(|(start, end)| (*start..=*end).contains(&file.index))
        {
            return Ok(());
        }
        let filter = |wanted: &Option<String>, values: &[String]| {
            wanted.as_ref().is_none_or(|wanted| {
                values.is_empty()
                    || values
                        .iter()
                        .any(|value| value.eq_ignore_ascii_case(wanted))
            })
        };
        if !filter(&self.options.language, &file.language)
            || !filter(&self.options.os, &file.os)
            || self
                .options
                .version
                .as_ref()
                .is_some_and(|wanted| file.version.as_ref().is_some_and(|value| value != wanted))
        {
            return Ok(());
        }
        if file.sources.is_empty() {
            return Err(if file.had_metaurl {
                MetalinkError::UnsupportedMetaurl
            } else {
                MetalinkError::NoUsableSource
            });
        }
        let preferred = self.options.preferred_protocol;
        let locations = self
            .options
            .location
            .as_deref()
            .unwrap_or("")
            .split(',')
            .collect::<Vec<_>>();
        file.sources.sort_by_key(|source| {
            (
                preferred.is_some_and(|preferred| preferred != source.protocol),
                !source.location.as_ref().is_some_and(|location| {
                    locations
                        .iter()
                        .any(|wanted| wanted.eq_ignore_ascii_case(location))
                }),
                source.priority,
            )
        });
        if self.options.unique_protocol {
            let selected = file.sources[0].protocol;
            file.sources.retain(|source| source.protocol == selected);
        }
        for (index, source) in file.sources.iter_mut().enumerate() {
            source.priority = index as i64;
        }
        self.files.push(MetalinkFile {
            index: file.index,
            name: file.name,
            sources: file.sources,
            verification: Arc::new(manifest),
        });
        Ok(())
    }
}
fn rank(algorithm: JournalDigestAlgorithm) -> u8 {
    match algorithm {
        JournalDigestAlgorithm::Md5 => 1,
        JournalDigestAlgorithm::Sha1 => 2,
        JournalDigestAlgorithm::Sha256 => 3,
        JournalDigestAlgorithm::Sha512 => 4,
    }
}
fn algorithm(value: &str) -> Option<JournalDigestAlgorithm> {
    match value {
        "sha-512" | "sha512" => Some(JournalDigestAlgorithm::Sha512),
        "sha-256" | "sha256" => Some(JournalDigestAlgorithm::Sha256),
        "sha-1" | "sha1" => Some(JournalDigestAlgorithm::Sha1),
        "md5" => Some(JournalDigestAlgorithm::Md5),
        _ => None,
    }
}
fn parse_selection(value: Option<&str>) -> Result<Vec<(u32, u32)>, MetalinkError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.is_empty() || value.len() > 64 * 1024 {
        return Err(MetalinkError::Selection);
    }
    let mut ranges = Vec::new();
    for value in value.split(',') {
        if ranges.len() == 1024 {
            return Err(MetalinkError::Limit);
        }
        let (start, end) = value.split_once('-').unwrap_or((value, value));
        let start = start.parse::<u32>().map_err(|_| MetalinkError::Selection)?;
        let end = end.parse::<u32>().map_err(|_| MetalinkError::Selection)?;
        if start == 0 || end < start || end as usize > MAX_METALINK_FILES {
            return Err(MetalinkError::Selection);
        }
        ranges.push((start, end));
    }
    ranges.sort_unstable();
    if ranges.windows(2).any(|pair| pair[0].1 >= pair[1].0) {
        return Err(MetalinkError::Selection);
    }
    Ok(ranges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContentHasher;
    fn digest(bytes: &[u8], algorithm: JournalDigestAlgorithm) -> String {
        let mut hash = ContentHasher::new(algorithm);
        hash.update(bytes);
        hash.finalize()
            .canonical()
            .split_once('=')
            .unwrap()
            .1
            .to_owned()
    }
    fn v4(file: &str) -> String {
        format!("<metalink xmlns='{V4}'>{file}</metalink>")
    }
    #[test]
    fn v3_v4_select_strongest_and_resolve_relative_mirrors() {
        let sha = digest(b"abc", JournalDigestAlgorithm::Sha512);
        let md5 = digest(b"abc", JournalDigestAlgorithm::Md5);
        for (document, version) in [
            (
                v4(&format!(
                    "<file name='dir/a'><size>3</size><hash type='md5'>{md5}</hash><pieces type='sha-512' length='3'><hash>{sha}</hash></pieces><url priority='2'>a</url><url priority='1'>b</url></file>"
                )),
                4,
            ),
            (
                format!(
                    "<m:metalink xmlns:m='{V3}' version='3.0'><m:files><m:file name='dir/a'><m:size>3</m:size><m:verification><m:hash type='md5'>{md5}</m:hash><m:pieces type='sha512' length='3'><m:hash piece='0'>{sha}</m:hash></m:pieces></m:verification><m:resources><m:url preference='1'>a</m:url><m:url preference='100'>b</m:url></m:resources></m:file></m:files></m:metalink>"
                ),
                3,
            ),
        ] {
            let parsed = parse_metalink(
                document.as_bytes(),
                &MetalinkOptions {
                    base_uri: Some("https://example.test/files/index.meta4".into()),
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(parsed.version, version);
            assert_eq!(
                parsed.files[0].sources[0].uri,
                "https://example.test/files/b"
            );
            assert_eq!(
                parsed.files[0].verification.chunks()[0].algorithm(),
                JournalDigestAlgorithm::Sha512
            );
            assert_eq!(
                parsed.files[0].verification.whole()[0].algorithm(),
                JournalDigestAlgorithm::Md5
            );
        }
    }
    #[test]
    fn xml_paths_algorithms_sources_and_selection_fail_closed() {
        let good = "<file name='a'><size>3</size><url>https://example.test/a</url></file>";
        for document in [
            format!(
                "<!DOCTYPE metalink [<!ENTITY x SYSTEM 'file:///secret'>]>{}",
                v4(good)
            ),
            format!("<?xml version='1.0' encoding='utf-16'?>{}", v4(good)),
            v4(&good.replace("name='a'", "name='../a'")),
            v4(&good.replace("name='a'", "name='CON'")),
            v4(&good.replace("name='a'", "name='a\\b'")),
            v4(&format!("{good}{good}")),
            v4(&good.replace(
                "<size>3</size>",
                "<size>3</size><hash type='sha-256'>bad</hash>",
            )),
            v4(&good.replace(
                "<size>3</size>",
                "<size>3</size><hash type='unknown'>abc</hash>",
            )),
            v4(&good.replace("https://example.test/a", "file:///private")),
            v4(good).replace("</file>", "</wrong>"),
        ] {
            assert!(
                parse_metalink(document.as_bytes(), &Default::default()).is_err(),
                "accepted invalid metadata"
            );
        }
        assert!(
            parse_metalink(
                v4(good).as_bytes(),
                &MetalinkOptions {
                    select_file: Some("2".into()),
                    ..Default::default()
                }
            )
            .is_err()
        );
        let parsed = parse_metalink(
            v4(&format!("{good}{}", good.replace("name='a'", "name='b'"))).as_bytes(),
            &MetalinkOptions {
                select_file: Some("2".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(parsed.files[0].index, 2);
    }
    #[test]
    fn resource_bounds_apply_to_ignored_extensions_and_emitted_results() {
        let attrs = (0..129).map(|i| format!(" a{i}='x'")).collect::<String>();
        let nested = format!("{}{}", "<x>".repeat(65), "</x>".repeat(65));
        for file in [format!("<extension{attrs}/>"), nested] {
            assert_eq!(
                parse_metalink(v4(&file).as_bytes(), &Default::default()),
                Err(MetalinkError::Limit)
            );
        }
        let file = "<file name='a'><size>0</size><url>https://example.test/a</url></file>";
        assert_eq!(
            parse_metalink(
                v4(file).as_bytes(),
                &MetalinkOptions {
                    metadata_bytes: 1,
                    ..Default::default()
                }
            ),
            Err(MetalinkError::Limit)
        );
        let document = v4(file);
        for cut in 0..document.len() {
            assert!(parse_metalink(&document.as_bytes()[..cut], &Default::default()).is_err());
        }
        assert!(parse_metalink(document.as_bytes(), &Default::default()).is_ok());
    }
}
