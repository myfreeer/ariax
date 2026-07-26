use crate::journal::{encode_record, validate_record};
use crate::journal_payload::{JournalPayload, PayloadCodecError};
use crate::{
    JournalEncodeError, JournalId, RecordType, SEGMENT_HASH_DOMAIN, SEGMENT_HEADER_LEN,
    SegmentHash, SegmentHeader,
};
use ariax_core::{Generation, Gid};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const JOURNAL_SEGMENT_FILE_PREFIX: &str = "segment-";
pub const JOURNAL_SEGMENT_FILE_SUFFIX: &str = ".arxj";
pub const JOURNAL_TEMP_FILE_SUFFIX: &str = ".tmp";

const STREAM_BUFFER_BYTES: usize = 64 * 1024;
const TAIL_RECORD_HASH_DOMAIN: &str = "ariax/journal-tail-record/v1\0";

/// A record whose complete bytes reached the active segment's file handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Appended {
    sequence: u64,
}

impl Appended {
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence
    }
}

/// The highest record covered by a completed journal `sync_all` barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Flushed {
    through_sequence: u64,
}

impl Flushed {
    #[must_use]
    pub const fn through_sequence(self) -> u64 {
        self.through_sequence
    }
}

/// Evidence returned after installing a successor segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalRotation {
    previous_segment_index: u32,
    new_segment_index: u32,
    first_sequence: u64,
    previous_segment_hash: SegmentHash,
}

impl JournalRotation {
    #[must_use]
    pub const fn previous_segment_index(self) -> u32 {
        self.previous_segment_index
    }

    #[must_use]
    pub const fn new_segment_index(self) -> u32 {
        self.new_segment_index
    }

    #[must_use]
    pub const fn first_sequence(self) -> u64 {
        self.first_sequence
    }

    #[must_use]
    pub const fn previous_segment_hash(self) -> SegmentHash {
        self.previous_segment_hash
    }
}

/// File operation associated with a durable appender error.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum JournalIoOperation {
    CreateDirectory,
    InspectSegmentPath,
    CreateTemporarySegment,
    WriteSegmentHeader,
    SyncSegment,
    InstallSegment,
    SyncDirectory,
    OpenActiveSegment,
    InspectActiveSegment,
    ReadActiveSegment,
    WriteRecord,
}

impl JournalIoOperation {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::CreateDirectory => "create_directory",
            Self::InspectSegmentPath => "inspect_segment_path",
            Self::CreateTemporarySegment => "create_temporary_segment",
            Self::WriteSegmentHeader => "write_segment_header",
            Self::SyncSegment => "sync_segment",
            Self::InstallSegment => "install_segment",
            Self::SyncDirectory => "sync_directory",
            Self::OpenActiveSegment => "open_active_segment",
            Self::InspectActiveSegment => "inspect_active_segment",
            Self::ReadActiveSegment => "read_active_segment",
            Self::WriteRecord => "write_record",
        }
    }
}

pub const ALL_JOURNAL_IO_OPERATIONS: [JournalIoOperation; 11] = [
    JournalIoOperation::CreateDirectory,
    JournalIoOperation::InspectSegmentPath,
    JournalIoOperation::CreateTemporarySegment,
    JournalIoOperation::WriteSegmentHeader,
    JournalIoOperation::SyncSegment,
    JournalIoOperation::InstallSegment,
    JournalIoOperation::SyncDirectory,
    JournalIoOperation::OpenActiveSegment,
    JournalIoOperation::InspectActiveSegment,
    JournalIoOperation::ReadActiveSegment,
    JournalIoOperation::WriteRecord,
];

/// First failure latched by an active appender.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum JournalAppenderFault {
    WriteRecord,
    Flush,
    Rotation,
    Reopen,
}

impl JournalAppenderFault {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::WriteRecord => "write_record",
            Self::Flush => "flush",
            Self::Rotation => "rotation",
            Self::Reopen => "reopen",
        }
    }
}

pub const ALL_JOURNAL_APPENDER_FAULTS: [JournalAppenderFault; 4] = [
    JournalAppenderFault::WriteRecord,
    JournalAppenderFault::Flush,
    JournalAppenderFault::Rotation,
    JournalAppenderFault::Reopen,
];

/// Why a closed descriptor could not be trusted for the next append.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum JournalTailMismatch {
    Length,
    Header,
    InvalidTailRecord,
    TailFingerprint,
}

impl JournalTailMismatch {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Length => "length",
            Self::Header => "header",
            Self::InvalidTailRecord => "invalid_tail_record",
            Self::TailFingerprint => "tail_fingerprint",
        }
    }
}

pub const ALL_JOURNAL_TAIL_MISMATCHES: [JournalTailMismatch; 4] = [
    JournalTailMismatch::Length,
    JournalTailMismatch::Header,
    JournalTailMismatch::InvalidTailRecord,
    JournalTailMismatch::TailFingerprint,
];

/// Why the durable single-owner appender rejected an operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalAppenderError {
    Payload(PayloadCodecError),
    Journal(JournalEncodeError),
    Io {
        operation: JournalIoOperation,
        kind: io::ErrorKind,
    },
    Faulted(JournalAppenderFault),
    FlushBeyondAppended {
        requested: u64,
        appended: u64,
    },
    UnflushedRecords {
        appended: u64,
        flushed: u64,
    },
    SegmentPathExists {
        temporary: bool,
    },
    TailMismatch(JournalTailMismatch),
}

impl JournalAppenderError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Payload(_) => "payload",
            Self::Journal(_) => "journal",
            Self::Io { .. } => "io",
            Self::Faulted(_) => "faulted",
            Self::FlushBeyondAppended { .. } => "flush_beyond_appended",
            Self::UnflushedRecords { .. } => "unflushed_records",
            Self::SegmentPathExists { .. } => "segment_path_exists",
            Self::TailMismatch(_) => "tail_mismatch",
        }
    }
}

pub const ALL_JOURNAL_APPENDER_ERROR_CODES: [&str; 8] = [
    "payload",
    "journal",
    "io",
    "faulted",
    "flush_beyond_appended",
    "unflushed_records",
    "segment_path_exists",
    "tail_mismatch",
];

impl fmt::Display for JournalAppenderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Payload(error) => error.fmt(formatter),
            Self::Journal(error) => error.fmt(formatter),
            Self::Io { operation, kind } => {
                write!(formatter, "journal {} failed: {kind}", operation.code())
            }
            Self::Faulted(fault) => write!(formatter, "journal appender faulted: {}", fault.code()),
            Self::FlushBeyondAppended {
                requested,
                appended,
            } => write!(
                formatter,
                "cannot flush through sequence {requested}; only {appended} is appended"
            ),
            Self::UnflushedRecords { appended, flushed } => write!(
                formatter,
                "journal has unflushed records: appended {appended}, flushed {flushed}"
            ),
            Self::SegmentPathExists { temporary } => {
                if *temporary {
                    formatter.write_str("journal temporary segment path already exists")
                } else {
                    formatter.write_str("journal segment path already exists")
                }
            }
            Self::TailMismatch(mismatch) => {
                write!(formatter, "journal tail mismatch: {}", mismatch.code())
            }
        }
    }
}

impl Error for JournalAppenderError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Payload(error) => Some(error),
            Self::Journal(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TailRecord {
    offset: u64,
    length: usize,
    fingerprint: [u8; 32],
}

/// File-backed, serialized owner of one task's active journal segment.
#[derive(Debug)]
pub struct ControlJournalAppender {
    directory: PathBuf,
    segment_paths: Vec<PathBuf>,
    active_path: PathBuf,
    active_file: Option<File>,
    header: SegmentHeader,
    valid_length: u64,
    next_sequence: u64,
    appended_sequence: u64,
    flushed_sequence: u64,
    tail_record: Option<TailRecord>,
    fault: Option<JournalAppenderFault>,
}

impl ControlJournalAppender {
    pub fn create(
        directory: impl AsRef<Path>,
        task_gid: Gid,
        journal_id: JournalId,
        starting_generation: Generation,
        created_at_unix_ms: u64,
    ) -> Result<Self, JournalAppenderError> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)
            .map_err(|error| io_error(JournalIoOperation::CreateDirectory, error))?;
        let header = SegmentHeader::first(
            task_gid,
            journal_id,
            starting_generation,
            created_at_unix_ms,
        );
        let active_path = install_segment(&directory, header)?;
        let active_file = open_active_segment(&active_path)?;
        let valid_length =
            u64::try_from(SEGMENT_HEADER_LEN).expect("the fixed segment header length fits u64");
        Ok(Self {
            directory,
            segment_paths: vec![active_path.clone()],
            active_path,
            active_file: Some(active_file),
            header,
            valid_length,
            next_sequence: header.first_sequence(),
            appended_sequence: header.first_sequence() - 1,
            flushed_sequence: header.first_sequence() - 1,
            tail_record: None,
            fault: None,
        })
    }

    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    #[must_use]
    pub fn active_path(&self) -> &Path {
        &self.active_path
    }

    #[must_use]
    pub fn segment_paths(&self) -> &[PathBuf] {
        &self.segment_paths
    }

    #[must_use]
    pub const fn active_header(&self) -> SegmentHeader {
        self.header
    }

    #[must_use]
    pub const fn active_length(&self) -> u64 {
        self.valid_length
    }

    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    #[must_use]
    pub const fn appended_sequence(&self) -> u64 {
        self.appended_sequence
    }

    #[must_use]
    pub const fn flushed_sequence(&self) -> u64 {
        self.flushed_sequence
    }

    #[must_use]
    pub const fn fault(&self) -> Option<JournalAppenderFault> {
        self.fault
    }

    #[must_use]
    pub const fn is_open(&self) -> bool {
        self.active_file.is_some()
    }

    pub fn append_payload(
        &mut self,
        generation: Generation,
        payload: &JournalPayload,
    ) -> Result<Appended, JournalAppenderError> {
        self.ensure_healthy()?;
        let encoded = payload.encode().map_err(JournalAppenderError::Payload)?;
        self.append(payload.record_type(), generation, &encoded)
    }

    fn append(
        &mut self,
        record_type: RecordType,
        generation: Generation,
        payload: &[u8],
    ) -> Result<Appended, JournalAppenderError> {
        self.ensure_healthy()?;
        if generation < self.header.starting_generation() {
            return Err(JournalAppenderError::Journal(
                JournalEncodeError::GenerationBeforeSegment,
            ));
        }
        let sequence = self.next_sequence;
        let next_sequence = sequence
            .checked_add(1)
            .ok_or(JournalAppenderError::Journal(
                JournalEncodeError::SequenceExhausted,
            ))?;
        let record = encode_record(record_type, generation, sequence, payload)
            .map_err(JournalAppenderError::Journal)?;
        let record_length = u64::try_from(record.len())
            .map_err(|_| JournalAppenderError::Journal(JournalEncodeError::AllocationFailed))?;
        let next_length =
            self.valid_length
                .checked_add(record_length)
                .ok_or(JournalAppenderError::Journal(
                    JournalEncodeError::AllocationFailed,
                ))?;
        let fingerprint = tail_record_fingerprint(&record);
        self.ensure_open()?;
        let write_result = self
            .active_file
            .as_mut()
            .expect("ensure_open installs a file")
            .write_all(&record);
        if let Err(error) = write_result {
            self.fault = Some(JournalAppenderFault::WriteRecord);
            return Err(io_error(JournalIoOperation::WriteRecord, error));
        }
        self.tail_record = Some(TailRecord {
            offset: self.valid_length,
            length: record.len(),
            fingerprint,
        });
        self.valid_length = next_length;
        self.next_sequence = next_sequence;
        self.appended_sequence = sequence;
        Ok(Appended { sequence })
    }

    pub fn flush(&mut self, up_to_sequence: u64) -> Result<Flushed, JournalAppenderError> {
        self.ensure_healthy()?;
        if up_to_sequence > self.appended_sequence {
            return Err(JournalAppenderError::FlushBeyondAppended {
                requested: up_to_sequence,
                appended: self.appended_sequence,
            });
        }
        if up_to_sequence <= self.flushed_sequence {
            return Ok(Flushed {
                through_sequence: self.flushed_sequence,
            });
        }
        self.ensure_open()?;
        let sync_result = self
            .active_file
            .as_ref()
            .expect("ensure_open installs a file")
            .sync_all();
        if let Err(error) = sync_result {
            self.fault = Some(JournalAppenderFault::Flush);
            return Err(io_error(JournalIoOperation::SyncSegment, error));
        }
        self.flushed_sequence = self.appended_sequence;
        Ok(Flushed {
            through_sequence: self.flushed_sequence,
        })
    }

    pub fn close_flushed(&mut self) -> Result<(), JournalAppenderError> {
        self.ensure_healthy()?;
        if self.appended_sequence != self.flushed_sequence {
            return Err(JournalAppenderError::UnflushedRecords {
                appended: self.appended_sequence,
                flushed: self.flushed_sequence,
            });
        }
        self.active_file.take();
        Ok(())
    }

    pub fn rotate(
        &mut self,
        starting_generation: Generation,
        created_at_unix_ms: u64,
    ) -> Result<JournalRotation, JournalAppenderError> {
        self.ensure_healthy()?;
        if self.appended_sequence < self.header.first_sequence() {
            return Err(JournalAppenderError::Journal(
                JournalEncodeError::CannotRotateEmptySegment,
            ));
        }
        if self.appended_sequence != self.flushed_sequence {
            return Err(JournalAppenderError::UnflushedRecords {
                appended: self.appended_sequence,
                flushed: self.flushed_sequence,
            });
        }
        self.ensure_open()?;
        if let Err(error) = self.validate_open_file() {
            self.fault = Some(JournalAppenderFault::Rotation);
            return Err(error);
        }
        let sync_result = self
            .active_file
            .as_ref()
            .expect("ensure_open installs a file")
            .sync_all();
        if let Err(error) = sync_result {
            self.fault = Some(JournalAppenderFault::Rotation);
            return Err(io_error(JournalIoOperation::SyncSegment, error));
        }
        let previous_segment_hash = match hash_open_segment(
            self.active_file
                .as_mut()
                .expect("ensure_open installs a file"),
            self.valid_length,
        ) {
            Ok(hash) => hash,
            Err(error) => {
                self.fault = Some(JournalAppenderFault::Rotation);
                return Err(error);
            }
        };
        let next_header = self
            .header
            .successor(
                self.appended_sequence,
                previous_segment_hash,
                starting_generation,
                created_at_unix_ms,
            )
            .map_err(JournalAppenderError::Journal)?;
        self.active_file.take();
        let new_path = match install_segment(&self.directory, next_header) {
            Ok(path) => path,
            Err(error) => {
                self.fault = Some(JournalAppenderFault::Rotation);
                return Err(error);
            }
        };
        let new_file = match open_active_segment(&new_path) {
            Ok(file) => file,
            Err(error) => {
                self.fault = Some(JournalAppenderFault::Rotation);
                return Err(error);
            }
        };
        let previous_segment_index = self.header.segment_index();
        self.segment_paths.push(new_path.clone());
        self.active_path = new_path;
        self.active_file = Some(new_file);
        self.header = next_header;
        self.valid_length =
            u64::try_from(SEGMENT_HEADER_LEN).expect("the fixed segment header length fits u64");
        self.next_sequence = next_header.first_sequence();
        self.tail_record = None;
        Ok(JournalRotation {
            previous_segment_index,
            new_segment_index: next_header.segment_index(),
            first_sequence: next_header.first_sequence(),
            previous_segment_hash,
        })
    }

    fn ensure_healthy(&self) -> Result<(), JournalAppenderError> {
        if let Some(fault) = self.fault {
            Err(JournalAppenderError::Faulted(fault))
        } else {
            Ok(())
        }
    }

    fn ensure_open(&mut self) -> Result<(), JournalAppenderError> {
        if self.active_file.is_some() {
            return Ok(());
        }
        let mut file = match open_active_segment(&self.active_path) {
            Ok(file) => file,
            Err(error) => {
                self.fault = Some(JournalAppenderFault::Reopen);
                return Err(error);
            }
        };
        if let Err(error) = validate_active_file(
            &mut file,
            self.header,
            self.valid_length,
            self.appended_sequence,
            self.tail_record,
        ) {
            self.fault = Some(JournalAppenderFault::Reopen);
            return Err(error);
        }
        self.active_file = Some(file);
        Ok(())
    }

    fn validate_open_file(&mut self) -> Result<(), JournalAppenderError> {
        validate_active_file(
            self.active_file
                .as_mut()
                .expect("the caller ensured the active descriptor is open"),
            self.header,
            self.valid_length,
            self.appended_sequence,
            self.tail_record,
        )
    }
}

#[must_use]
pub fn journal_segment_file_name(segment_index: u32) -> String {
    format!("{JOURNAL_SEGMENT_FILE_PREFIX}{segment_index:010}{JOURNAL_SEGMENT_FILE_SUFFIX}")
}

#[must_use]
pub fn journal_segment_path(directory: impl AsRef<Path>, segment_index: u32) -> PathBuf {
    directory
        .as_ref()
        .join(journal_segment_file_name(segment_index))
}

fn journal_temporary_segment_path(directory: &Path, segment_index: u32) -> PathBuf {
    let mut file_name = journal_segment_file_name(segment_index);
    file_name.push_str(JOURNAL_TEMP_FILE_SUFFIX);
    directory.join(file_name)
}

fn install_segment(
    directory: &Path,
    header: SegmentHeader,
) -> Result<PathBuf, JournalAppenderError> {
    let final_path = journal_segment_path(directory, header.segment_index());
    let temporary_path = journal_temporary_segment_path(directory, header.segment_index());
    if path_exists(&final_path)? {
        return Err(JournalAppenderError::SegmentPathExists { temporary: false });
    }
    if path_exists(&temporary_path)? {
        return Err(JournalAppenderError::SegmentPathExists { temporary: true });
    }
    let mut temporary = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary_path)
        .map_err(|error| io_error(JournalIoOperation::CreateTemporarySegment, error))?;
    if let Err(error) = temporary.write_all(&header.encode()) {
        drop(temporary);
        let _ = fs::remove_file(&temporary_path);
        return Err(io_error(JournalIoOperation::WriteSegmentHeader, error));
    }
    if let Err(error) = temporary.sync_all() {
        drop(temporary);
        let _ = fs::remove_file(&temporary_path);
        return Err(io_error(JournalIoOperation::SyncSegment, error));
    }
    drop(temporary);
    fs::rename(&temporary_path, &final_path)
        .map_err(|error| io_error(JournalIoOperation::InstallSegment, error))?;
    sync_directory(directory)
        .map_err(|error| io_error(JournalIoOperation::SyncDirectory, error))?;
    Ok(final_path)
}

fn path_exists(path: &Path) -> Result<bool, JournalAppenderError> {
    path.try_exists()
        .map_err(|error| io_error(JournalIoOperation::InspectSegmentPath, error))
}

fn open_active_segment(path: &Path) -> Result<File, JournalAppenderError> {
    OpenOptions::new()
        .read(true)
        .append(true)
        .open(path)
        .map_err(|error| io_error(JournalIoOperation::OpenActiveSegment, error))
}

fn validate_active_file(
    file: &mut File,
    expected_header: SegmentHeader,
    expected_length: u64,
    appended_sequence: u64,
    tail_record: Option<TailRecord>,
) -> Result<(), JournalAppenderError> {
    let actual_length = file
        .metadata()
        .map_err(|error| io_error(JournalIoOperation::InspectActiveSegment, error))?
        .len();
    if actual_length != expected_length {
        return Err(JournalAppenderError::TailMismatch(
            JournalTailMismatch::Length,
        ));
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| io_error(JournalIoOperation::ReadActiveSegment, error))?;
    let mut header_bytes = [0_u8; SEGMENT_HEADER_LEN];
    file.read_exact(&mut header_bytes)
        .map_err(|error| io_error(JournalIoOperation::ReadActiveSegment, error))?;
    if SegmentHeader::decode(&header_bytes) != Ok(expected_header) {
        return Err(JournalAppenderError::TailMismatch(
            JournalTailMismatch::Header,
        ));
    }
    match tail_record {
        Some(tail) => {
            let tail_length = u64::try_from(tail.length)
                .map_err(|_| JournalAppenderError::Journal(JournalEncodeError::AllocationFailed))?;
            if tail.offset.checked_add(tail_length) != Some(expected_length) {
                return Err(JournalAppenderError::TailMismatch(
                    JournalTailMismatch::Length,
                ));
            }
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(tail.length)
                .map_err(|_| JournalAppenderError::Journal(JournalEncodeError::AllocationFailed))?;
            bytes.resize(tail.length, 0);
            file.seek(SeekFrom::Start(tail.offset))
                .map_err(|error| io_error(JournalIoOperation::ReadActiveSegment, error))?;
            file.read_exact(&mut bytes)
                .map_err(|error| io_error(JournalIoOperation::ReadActiveSegment, error))?;
            if validate_record(
                &bytes,
                appended_sequence,
                expected_header.starting_generation(),
            ) != Ok(tail.length)
            {
                return Err(JournalAppenderError::TailMismatch(
                    JournalTailMismatch::InvalidTailRecord,
                ));
            }
            if tail_record_fingerprint(&bytes) != tail.fingerprint {
                return Err(JournalAppenderError::TailMismatch(
                    JournalTailMismatch::TailFingerprint,
                ));
            }
        }
        None => {
            let header_length = u64::try_from(SEGMENT_HEADER_LEN)
                .expect("the fixed segment header length fits u64");
            if expected_length != header_length
                || appended_sequence.checked_add(1) != Some(expected_header.first_sequence())
            {
                return Err(JournalAppenderError::TailMismatch(
                    JournalTailMismatch::Length,
                ));
            }
        }
    }
    Ok(())
}

fn hash_open_segment(
    file: &mut File,
    valid_length: u64,
) -> Result<SegmentHash, JournalAppenderError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|error| io_error(JournalIoOperation::ReadActiveSegment, error))?;
    let mut digest = Sha256::new();
    digest.update(SEGMENT_HASH_DOMAIN.as_bytes());
    digest.update(valid_length.to_le_bytes());
    let mut buffer = [0_u8; STREAM_BUFFER_BYTES];
    let mut remaining = valid_length;
    while remaining > 0 {
        let chunk_length = usize::try_from(remaining.min(STREAM_BUFFER_BYTES as u64))
            .expect("bounded stream chunk fits usize");
        file.read_exact(&mut buffer[..chunk_length])
            .map_err(|error| io_error(JournalIoOperation::ReadActiveSegment, error))?;
        digest.update(&buffer[..chunk_length]);
        remaining -= chunk_length as u64;
    }
    Ok(SegmentHash::from_bytes(digest.finalize().into()))
}

fn tail_record_fingerprint(record: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(TAIL_RECORD_HASH_DOMAIN.as_bytes());
    digest.update((record.len() as u64).to_le_bytes());
    digest.update(record);
    digest.finalize().into()
}

fn io_error(operation: JournalIoOperation, error: io::Error) -> JournalAppenderError {
    JournalAppenderError::Io {
        operation,
        kind: error.kind(),
    }
}

#[cfg(unix)]
fn sync_directory(directory: &Path) -> io::Result<()> {
    let file = File::open(directory)?;
    match file.sync_all() {
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::InvalidInput | io::ErrorKind::Unsupported
            ) =>
        {
            Ok(())
        }
        result => result,
    }
}

#[cfg(not(unix))]
fn sync_directory(_directory: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ALL_JOURNAL_APPENDER_ERROR_CODES, ALL_JOURNAL_APPENDER_FAULTS, ALL_JOURNAL_IO_OPERATIONS,
        ALL_JOURNAL_TAIL_MISMATCHES, ControlJournalAppender, JournalAppenderError,
        JournalAppenderFault, JournalTailMismatch, journal_segment_file_name, journal_segment_path,
    };
    use crate::{
        DurabilityMode, JournalId, JournalPayload, ReplayLimits, ReplayStop, TaskPauseReason,
        replay_ordered_segments,
    };
    use ariax_core::{Generation, Gid};
    use std::collections::HashSet;
    use std::fs::{self, OpenOptions};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            let id = TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "ariax-journal-appender-{}-{id}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create test directory");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn gid() -> Gid {
        Gid::new(0x1234).expect("gid")
    }

    fn journal_id() -> JournalId {
        JournalId::new([9; 16]).expect("journal id")
    }

    fn task_created() -> JournalPayload {
        JournalPayload::TaskCreated {
            durability: DurabilityMode::Balanced,
            creator_version: 1,
        }
    }

    #[test]
    fn typed_append_flush_close_reopen_and_replay() {
        let directory = TestDirectory::new();
        let mut appender = ControlJournalAppender::create(
            directory.path(),
            gid(),
            journal_id(),
            Generation::INITIAL,
            100,
        )
        .expect("create appender");
        assert_eq!(
            appender.active_path(),
            journal_segment_path(directory.path(), 0)
        );
        assert_eq!(journal_segment_file_name(0), "segment-0000000000.arxj");

        let first = appender
            .append_payload(Generation::INITIAL, &task_created())
            .expect("append task");
        assert_eq!(first.sequence(), 1);
        assert_eq!(appender.appended_sequence(), 1);
        assert_eq!(appender.flushed_sequence(), 0);
        assert_eq!(
            appender.close_flushed(),
            Err(JournalAppenderError::UnflushedRecords {
                appended: 1,
                flushed: 0
            })
        );
        assert_eq!(appender.flush(1).expect("flush").through_sequence(), 1);
        appender.close_flushed().expect("close");
        assert!(!appender.is_open());

        let paused = JournalPayload::TaskPaused {
            reason: TaskPauseReason::User,
        };
        assert_eq!(
            appender
                .append_payload(Generation::INITIAL, &paused)
                .expect("append after validated reopen")
                .sequence(),
            2
        );
        appender.flush(2).expect("flush second");

        let bytes = fs::read(appender.active_path()).expect("read segment");
        let replay = replay_ordered_segments(&[&bytes], ReplayLimits::default());
        assert_eq!(replay.stop, ReplayStop::CleanEnd);
        assert_eq!(replay.last_sequence, 2);
        assert_eq!(replay.records[0].decode_payload(), Ok(task_created()));
        assert_eq!(replay.records[1].decode_payload(), Ok(paused));
    }

    #[test]
    fn codec_and_flush_range_failures_do_not_consume_sequence_or_fault() {
        let directory = TestDirectory::new();
        let mut appender = ControlJournalAppender::create(
            directory.path(),
            gid(),
            journal_id(),
            Generation::INITIAL,
            0,
        )
        .expect("create appender");
        let invalid = JournalPayload::TaskCreated {
            durability: DurabilityMode::Balanced,
            creator_version: 0,
        };
        assert!(matches!(
            appender.append_payload(Generation::INITIAL, &invalid),
            Err(JournalAppenderError::Payload(_))
        ));
        assert_eq!(appender.next_sequence(), 1);
        assert_eq!(
            appender.flush(1),
            Err(JournalAppenderError::FlushBeyondAppended {
                requested: 1,
                appended: 0
            })
        );
        assert_eq!(appender.fault(), None);
        assert_eq!(
            appender
                .append_payload(Generation::INITIAL, &task_created())
                .expect("valid append")
                .sequence(),
            1
        );
    }

    #[test]
    fn rotation_requires_a_flushed_boundary_and_links_immutable_segments() {
        let directory = TestDirectory::new();
        let mut appender = ControlJournalAppender::create(
            directory.path(),
            gid(),
            journal_id(),
            Generation::INITIAL,
            0,
        )
        .expect("create appender");
        appender
            .append_payload(Generation::INITIAL, &task_created())
            .expect("append task");
        assert!(matches!(
            appender.rotate(Generation::INITIAL, 1),
            Err(JournalAppenderError::UnflushedRecords { .. })
        ));
        appender.flush(1).expect("flush first");
        let first_path = appender.active_path().to_path_buf();
        let first_before = fs::read(&first_path).expect("read first before rotation");
        let rotation = appender.rotate(Generation::INITIAL, 1).expect("rotate");
        assert_eq!(rotation.previous_segment_index(), 0);
        assert_eq!(rotation.new_segment_index(), 1);
        assert_eq!(rotation.first_sequence(), 2);
        assert_eq!(appender.segment_paths().len(), 2);
        assert_eq!(
            fs::read(&first_path).expect("read immutable first"),
            first_before
        );
        assert_eq!(
            appender.rotate(Generation::INITIAL, 2),
            Err(JournalAppenderError::Journal(
                crate::JournalEncodeError::CannotRotateEmptySegment
            ))
        );

        appender
            .append_payload(
                Generation::INITIAL,
                &JournalPayload::TaskPaused {
                    reason: TaskPauseReason::User,
                },
            )
            .expect("append second segment");
        appender.flush(2).expect("flush second");
        let first = fs::read(&appender.segment_paths()[0]).expect("read first");
        let second = fs::read(&appender.segment_paths()[1]).expect("read second");
        let replay = replay_ordered_segments(&[&first, &second], ReplayLimits::default());
        assert_eq!(replay.stop, ReplayStop::CleanEnd);
        assert_eq!(replay.last_sequence, 2);
    }

    #[test]
    fn changed_closed_tail_latches_reopen_fault_and_blocks_later_sequences() {
        let directory = TestDirectory::new();
        let mut appender = ControlJournalAppender::create(
            directory.path(),
            gid(),
            journal_id(),
            Generation::INITIAL,
            0,
        )
        .expect("create appender");
        appender
            .append_payload(Generation::INITIAL, &task_created())
            .expect("append task");
        appender.flush(1).expect("flush");
        appender.close_flushed().expect("close");
        let file = OpenOptions::new()
            .write(true)
            .open(appender.active_path())
            .expect("open for truncation");
        file.set_len(appender.active_length() - 1)
            .expect("truncate tail");
        drop(file);

        assert_eq!(
            appender.append_payload(Generation::INITIAL, &task_created()),
            Err(JournalAppenderError::TailMismatch(
                JournalTailMismatch::Length
            ))
        );
        assert_eq!(appender.fault(), Some(JournalAppenderFault::Reopen));
        assert_eq!(appender.next_sequence(), 2);
        assert_eq!(
            appender.append_payload(Generation::INITIAL, &task_created()),
            Err(JournalAppenderError::Faulted(JournalAppenderFault::Reopen))
        );
    }

    #[test]
    fn creation_never_overwrites_an_existing_segment() {
        let directory = TestDirectory::new();
        let path = journal_segment_path(directory.path(), 0);
        fs::write(&path, b"owned-by-another-journal").expect("seed path");
        assert!(matches!(
            ControlJournalAppender::create(
                directory.path(),
                gid(),
                journal_id(),
                Generation::INITIAL,
                0,
            ),
            Err(JournalAppenderError::SegmentPathExists { temporary: false })
        ));
        assert_eq!(
            fs::read(path).expect("read preserved path"),
            b"owned-by-another-journal"
        );
    }

    #[test]
    fn appender_vocabularies_are_closed_and_unique() {
        let error_codes = ALL_JOURNAL_APPENDER_ERROR_CODES
            .into_iter()
            .collect::<HashSet<_>>();
        assert_eq!(error_codes.len(), ALL_JOURNAL_APPENDER_ERROR_CODES.len());
        let operations = ALL_JOURNAL_IO_OPERATIONS
            .into_iter()
            .map(super::JournalIoOperation::code)
            .collect::<HashSet<_>>();
        assert_eq!(operations.len(), ALL_JOURNAL_IO_OPERATIONS.len());
        let faults = ALL_JOURNAL_APPENDER_FAULTS
            .into_iter()
            .map(super::JournalAppenderFault::code)
            .collect::<HashSet<_>>();
        assert_eq!(faults.len(), ALL_JOURNAL_APPENDER_FAULTS.len());
        let mismatches = ALL_JOURNAL_TAIL_MISMATCHES
            .into_iter()
            .map(super::JournalTailMismatch::code)
            .collect::<HashSet<_>>();
        assert_eq!(mismatches.len(), ALL_JOURNAL_TAIL_MISMATCHES.len());
    }
}
