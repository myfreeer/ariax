use crate::journal::{encode_record, hash_segment, replay_ordered_segments, validate_record};
use crate::journal_payload::{JournalPayload, PayloadCodecError};
use crate::{
    JournalEncodeError, JournalId, JournalReplay, RECORD_OVERHEAD, RecordStopReason, RecordType,
    ReplayLimits, ReplayResource, ReplayStop, SEGMENT_HASH_DOMAIN, SEGMENT_HEADER_LEN, SegmentHash,
    SegmentHeader,
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
    InspectRecoverySegment,
    OpenRecoverySegment,
    ReadRecoverySegment,
    RepairRecoverySegment,
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
            Self::InspectRecoverySegment => "inspect_recovery_segment",
            Self::OpenRecoverySegment => "open_recovery_segment",
            Self::ReadRecoverySegment => "read_recovery_segment",
            Self::RepairRecoverySegment => "repair_recovery_segment",
            Self::WriteRecord => "write_record",
        }
    }
}

pub const ALL_JOURNAL_IO_OPERATIONS: [JournalIoOperation; 15] = [
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
    JournalIoOperation::InspectRecoverySegment,
    JournalIoOperation::OpenRecoverySegment,
    JournalIoOperation::ReadRecoverySegment,
    JournalIoOperation::RepairRecoverySegment,
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
    RecoverySegmentPath {
        input_index: usize,
    },
    RecoveryInputBytesExceeded {
        limit: usize,
    },
    RecoveryTaskMismatch {
        expected: Gid,
        actual: Gid,
    },
    RecoveryJournalMismatch {
        expected: JournalId,
        actual: JournalId,
    },
    RecoveryStopped(ReplayStop),
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
            Self::RecoverySegmentPath { .. } => "recovery_segment_path",
            Self::RecoveryInputBytesExceeded { .. } => "recovery_input_bytes_exceeded",
            Self::RecoveryTaskMismatch { .. } => "recovery_task_mismatch",
            Self::RecoveryJournalMismatch { .. } => "recovery_journal_mismatch",
            Self::RecoveryStopped(_) => "recovery_stopped",
        }
    }
}

pub const ALL_JOURNAL_APPENDER_ERROR_CODES: [&str; 13] = [
    "payload",
    "journal",
    "io",
    "faulted",
    "flush_beyond_appended",
    "unflushed_records",
    "segment_path_exists",
    "tail_mismatch",
    "recovery_segment_path",
    "recovery_input_bytes_exceeded",
    "recovery_task_mismatch",
    "recovery_journal_mismatch",
    "recovery_stopped",
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
            Self::RecoverySegmentPath { input_index } => write!(
                formatter,
                "journal recovery segment {input_index} is not its exact named regular path"
            ),
            Self::RecoveryInputBytesExceeded { limit } => write!(
                formatter,
                "journal recovery input exceeds the encoded-byte budget of {limit}"
            ),
            Self::RecoveryTaskMismatch { .. } => {
                formatter.write_str("journal recovery task identity does not match")
            }
            Self::RecoveryJournalMismatch { .. } => {
                formatter.write_str("journal recovery journal identity does not match")
            }
            Self::RecoveryStopped(stop) => {
                write!(formatter, "journal recovery rejected replay stop: {stop:?}")
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

    /// Opens an installed segment set at its exact replay-valid boundary.
    ///
    /// The caller must hold exclusive persistence ownership for the task while
    /// this method validates and, for a recoverable final torn record, repairs
    /// the active segment. Its path checks are a portable lexical/metadata
    /// preflight, not descriptor-safe authority: native startup must exclude
    /// namespace mutation for the whole call.
    pub fn open_recovered(
        directory: impl AsRef<Path>,
        installed_segment_paths: &[PathBuf],
        expected_task_gid: Gid,
        expected_journal_id: JournalId,
        limits: ReplayLimits,
        recovery_starting_generation: Generation,
        recovery_created_at_unix_ms: u64,
    ) -> Result<(Self, JournalReplay), JournalAppenderError> {
        let directory = directory.as_ref().to_path_buf();
        if installed_segment_paths.is_empty() {
            return Err(JournalAppenderError::RecoveryStopped(
                ReplayStop::NoSegments,
            ));
        }
        if installed_segment_paths.len() > limits.max_segments {
            return Err(JournalAppenderError::RecoveryStopped(
                ReplayStop::ResourceLimit(ReplayResource::Segments),
            ));
        }

        let encoded_byte_budget = recovery_encoded_byte_budget(limits)?;
        let mut remaining_byte_budget = encoded_byte_budget;
        let mut segment_bytes = Vec::new();
        segment_bytes
            .try_reserve_exact(installed_segment_paths.len())
            .map_err(|_| JournalAppenderError::Journal(JournalEncodeError::AllocationFailed))?;
        for (input_index, path) in installed_segment_paths.iter().enumerate() {
            let segment_index = u32::try_from(input_index).map_err(|_| {
                JournalAppenderError::RecoveryStopped(ReplayStop::ResourceLimit(
                    ReplayResource::Segments,
                ))
            })?;
            if *path != journal_segment_path(&directory, segment_index)
                || !recovery_path_is_regular(path)?
            {
                return Err(JournalAppenderError::RecoverySegmentPath { input_index });
            }
            segment_bytes.push(read_recovery_segment(
                path,
                encoded_byte_budget,
                &mut remaining_byte_budget,
            )?);
        }
        validate_recovery_directory(&directory, installed_segment_paths)?;

        let first_header = SegmentHeader::decode(&segment_bytes[0]).map_err(|error| {
            JournalAppenderError::RecoveryStopped(ReplayStop::Header {
                input_index: 0,
                error,
            })
        })?;
        if first_header.task_gid() != expected_task_gid {
            return Err(JournalAppenderError::RecoveryTaskMismatch {
                expected: expected_task_gid,
                actual: first_header.task_gid(),
            });
        }
        if first_header.journal_id() != expected_journal_id {
            return Err(JournalAppenderError::RecoveryJournalMismatch {
                expected: expected_journal_id,
                actual: first_header.journal_id(),
            });
        }

        let mut replay_inputs = Vec::new();
        replay_inputs
            .try_reserve_exact(segment_bytes.len())
            .map_err(|_| JournalAppenderError::Journal(JournalEncodeError::AllocationFailed))?;
        replay_inputs.extend(segment_bytes.iter().map(Vec::as_slice));
        let replay = replay_ordered_segments(&replay_inputs, limits);
        let appender = match replay.stop {
            ReplayStop::CleanEnd => open_clean_recovered_appender(
                directory,
                installed_segment_paths,
                &segment_bytes,
                &replay,
            )?,
            ReplayStop::Record {
                segment_index,
                offset,
                reason,
            } if usize::try_from(segment_index).ok()
                == installed_segment_paths.len().checked_sub(1)
                && replay.valid_segment_prefixes.len() == installed_segment_paths.len()
                && repairable_recovery_tail(reason) =>
            {
                open_repaired_recovered_appender(
                    directory,
                    installed_segment_paths,
                    &mut segment_bytes,
                    &replay,
                    offset,
                    recovery_starting_generation,
                    recovery_created_at_unix_ms,
                )?
            }
            stop => return Err(JournalAppenderError::RecoveryStopped(stop)),
        };
        Ok((appender, replay))
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

fn recovery_encoded_byte_budget(limits: ReplayLimits) -> Result<usize, JournalAppenderError> {
    limits
        .max_segments
        .checked_mul(SEGMENT_HEADER_LEN)
        .and_then(|headers| {
            limits
                .max_records
                .checked_mul(RECORD_OVERHEAD)
                .and_then(|records| headers.checked_add(records))
        })
        .and_then(|framing| framing.checked_add(limits.max_payload_bytes))
        .ok_or(JournalAppenderError::Journal(
            JournalEncodeError::AllocationFailed,
        ))
}

fn recovery_path_is_regular(path: &Path) -> Result<bool, JournalAppenderError> {
    path.symlink_metadata()
        .map(|metadata| metadata.file_type().is_file())
        .map_err(|error| io_error(JournalIoOperation::InspectRecoverySegment, error))
}

fn validate_recovery_directory(
    directory: &Path,
    installed_segment_paths: &[PathBuf],
) -> Result<(), JournalAppenderError> {
    let immediate_successor = u32::try_from(installed_segment_paths.len())
        .ok()
        .map(|index| journal_segment_path(directory, index));
    let entries = fs::read_dir(directory)
        .map_err(|error| io_error(JournalIoOperation::InspectRecoverySegment, error))?;
    for entry in entries {
        let entry =
            entry.map_err(|error| io_error(JournalIoOperation::InspectRecoverySegment, error))?;
        let path = entry.path();
        if installed_segment_paths.contains(&path) {
            continue;
        }
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        if file_name.starts_with(JOURNAL_SEGMENT_FILE_PREFIX)
            && file_name.ends_with(JOURNAL_TEMP_FILE_SUFFIX)
        {
            return Err(JournalAppenderError::SegmentPathExists { temporary: true });
        }
        if file_name.starts_with(JOURNAL_SEGMENT_FILE_PREFIX)
            && file_name.ends_with(JOURNAL_SEGMENT_FILE_SUFFIX)
        {
            if immediate_successor.as_ref() == Some(&path) {
                return Err(JournalAppenderError::SegmentPathExists { temporary: false });
            }
            return Err(JournalAppenderError::RecoverySegmentPath {
                input_index: installed_segment_paths.len(),
            });
        }
    }
    Ok(())
}

fn read_recovery_segment(
    path: &Path,
    encoded_byte_budget: usize,
    remaining_byte_budget: &mut usize,
) -> Result<Vec<u8>, JournalAppenderError> {
    let mut file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|error| io_error(JournalIoOperation::OpenRecoverySegment, error))?;
    let length = file
        .metadata()
        .map_err(|error| io_error(JournalIoOperation::InspectRecoverySegment, error))?
        .len();
    let length =
        usize::try_from(length).map_err(|_| JournalAppenderError::RecoveryInputBytesExceeded {
            limit: encoded_byte_budget,
        })?;
    if length > *remaining_byte_budget {
        return Err(JournalAppenderError::RecoveryInputBytesExceeded {
            limit: encoded_byte_budget,
        });
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| JournalAppenderError::Journal(JournalEncodeError::AllocationFailed))?;
    bytes.resize(length, 0);
    file.read_exact(&mut bytes)
        .map_err(|error| io_error(JournalIoOperation::ReadRecoverySegment, error))?;
    let mut extra = [0_u8; 1];
    if file
        .read(&mut extra)
        .map_err(|error| io_error(JournalIoOperation::ReadRecoverySegment, error))?
        != 0
    {
        return Err(JournalAppenderError::TailMismatch(
            JournalTailMismatch::Length,
        ));
    }
    *remaining_byte_budget -= length;
    Ok(bytes)
}

fn repairable_recovery_tail(reason: RecordStopReason) -> bool {
    matches!(
        reason,
        RecordStopReason::TruncatedFraming
            | RecordStopReason::InvalidMagic
            | RecordStopReason::PayloadTooLarge
            | RecordStopReason::UnknownRecordType
            | RecordStopReason::UnknownFlags
            | RecordStopReason::LengthOverflow
            | RecordStopReason::TruncatedRecord
            | RecordStopReason::BadCrc
            | RecordStopReason::MissingCommit
    )
}

fn open_clean_recovered_appender(
    directory: PathBuf,
    installed_segment_paths: &[PathBuf],
    segment_bytes: &[Vec<u8>],
    replay: &JournalReplay,
) -> Result<ControlJournalAppender, JournalAppenderError> {
    let active_path = installed_segment_paths
        .last()
        .expect("recovered open rejects empty segment sets")
        .clone();
    let active_bytes = segment_bytes
        .last()
        .expect("recovered open reads every installed segment");
    let header = SegmentHeader::decode(active_bytes).map_err(|error| {
        JournalAppenderError::RecoveryStopped(ReplayStop::Header {
            input_index: installed_segment_paths.len() - 1,
            error,
        })
    })?;
    let valid_length = u64::try_from(active_bytes.len())
        .map_err(|_| JournalAppenderError::Journal(JournalEncodeError::AllocationFailed))?;
    let tail_record = recovered_tail_record(active_bytes, header, replay)?;
    let next_sequence =
        replay
            .last_sequence
            .checked_add(1)
            .ok_or(JournalAppenderError::Journal(
                JournalEncodeError::SequenceExhausted,
            ))?;
    let mut active_file = open_active_segment(&active_path)?;
    validate_recovery_file_bytes(&mut active_file, active_bytes)?;
    validate_active_file(
        &mut active_file,
        header,
        valid_length,
        replay.last_sequence,
        tail_record,
    )?;
    active_file
        .sync_all()
        .map_err(|error| io_error(JournalIoOperation::SyncSegment, error))?;
    Ok(ControlJournalAppender {
        directory,
        segment_paths: clone_segment_paths(installed_segment_paths)?,
        active_path,
        active_file: Some(active_file),
        header,
        valid_length,
        next_sequence,
        appended_sequence: replay.last_sequence,
        flushed_sequence: replay.last_sequence,
        tail_record,
        fault: None,
    })
}

fn open_repaired_recovered_appender(
    directory: PathBuf,
    installed_segment_paths: &[PathBuf],
    segment_bytes: &mut [Vec<u8>],
    replay: &JournalReplay,
    valid_prefix: usize,
    recovery_starting_generation: Generation,
    recovery_created_at_unix_ms: u64,
) -> Result<ControlJournalAppender, JournalAppenderError> {
    let active_path = installed_segment_paths
        .last()
        .expect("recovered open rejects empty segment sets");
    let active_bytes = segment_bytes
        .last_mut()
        .expect("recovered open reads every installed segment");
    if replay.valid_segment_prefixes.last().copied() != Some(valid_prefix)
        || valid_prefix < SEGMENT_HEADER_LEN
        || valid_prefix >= active_bytes.len()
    {
        return Err(JournalAppenderError::RecoveryStopped(replay.stop));
    }
    let header = SegmentHeader::decode(active_bytes).map_err(|error| {
        JournalAppenderError::RecoveryStopped(ReplayStop::Header {
            input_index: installed_segment_paths.len() - 1,
            error,
        })
    })?;
    let reopen_first_empty = header.segment_index() == 0 && replay.last_sequence == 0;
    let next_header = if reopen_first_empty {
        None
    } else {
        let previous_segment_hash = hash_segment(&active_bytes[..valid_prefix]);
        let next_header = header
            .recovery_successor(
                replay.last_sequence,
                previous_segment_hash,
                recovery_starting_generation,
                recovery_created_at_unix_ms,
            )
            .map_err(JournalAppenderError::Journal)?;
        let final_path = journal_segment_path(&directory, next_header.segment_index());
        let temporary_path =
            journal_temporary_segment_path(&directory, next_header.segment_index());
        if path_exists(&final_path)? {
            return Err(JournalAppenderError::SegmentPathExists { temporary: false });
        }
        if path_exists(&temporary_path)? {
            return Err(JournalAppenderError::SegmentPathExists { temporary: true });
        }
        Some(next_header)
    };

    repair_recovery_segment(active_path, active_bytes, valid_prefix)?;
    active_bytes.truncate(valid_prefix);

    let Some(next_header) = next_header else {
        return open_clean_recovered_appender(
            directory,
            installed_segment_paths,
            segment_bytes,
            replay,
        );
    };
    let new_path = install_segment(&directory, next_header)?;
    let mut new_file = open_active_segment(&new_path)?;
    let header_length =
        u64::try_from(SEGMENT_HEADER_LEN).expect("the fixed segment header length fits u64");
    validate_active_file(
        &mut new_file,
        next_header,
        header_length,
        replay.last_sequence,
        None,
    )?;
    let mut recovered_paths = clone_segment_paths(installed_segment_paths)?;
    recovered_paths.push(new_path.clone());
    Ok(ControlJournalAppender {
        directory,
        segment_paths: recovered_paths,
        active_path: new_path,
        active_file: Some(new_file),
        header: next_header,
        valid_length: header_length,
        next_sequence: next_header.first_sequence(),
        appended_sequence: replay.last_sequence,
        flushed_sequence: replay.last_sequence,
        tail_record: None,
        fault: None,
    })
}

fn recovered_tail_record(
    active_bytes: &[u8],
    header: SegmentHeader,
    replay: &JournalReplay,
) -> Result<Option<TailRecord>, JournalAppenderError> {
    if replay.last_sequence < header.first_sequence() {
        return Ok(None);
    }
    let record = replay
        .records
        .last()
        .ok_or(JournalAppenderError::TailMismatch(
            JournalTailMismatch::InvalidTailRecord,
        ))?;
    if record.sequence != replay.last_sequence {
        return Err(JournalAppenderError::TailMismatch(
            JournalTailMismatch::InvalidTailRecord,
        ));
    }
    let length =
        RECORD_OVERHEAD
            .checked_add(record.payload.len())
            .ok_or(JournalAppenderError::Journal(
                JournalEncodeError::AllocationFailed,
            ))?;
    let offset =
        active_bytes
            .len()
            .checked_sub(length)
            .ok_or(JournalAppenderError::TailMismatch(
                JournalTailMismatch::Length,
            ))?;
    if offset < SEGMENT_HEADER_LEN
        || validate_record(
            &active_bytes[offset..],
            replay.last_sequence,
            header.starting_generation(),
        ) != Ok(length)
    {
        return Err(JournalAppenderError::TailMismatch(
            JournalTailMismatch::InvalidTailRecord,
        ));
    }
    Ok(Some(TailRecord {
        offset: u64::try_from(offset)
            .map_err(|_| JournalAppenderError::Journal(JournalEncodeError::AllocationFailed))?,
        length,
        fingerprint: tail_record_fingerprint(&active_bytes[offset..]),
    }))
}

fn clone_segment_paths(paths: &[PathBuf]) -> Result<Vec<PathBuf>, JournalAppenderError> {
    let mut cloned = Vec::new();
    cloned
        .try_reserve_exact(paths.len())
        .map_err(|_| JournalAppenderError::Journal(JournalEncodeError::AllocationFailed))?;
    cloned.extend(paths.iter().cloned());
    Ok(cloned)
}

fn validate_recovery_file_bytes(
    file: &mut File,
    expected: &[u8],
) -> Result<(), JournalAppenderError> {
    let expected_length = u64::try_from(expected.len())
        .map_err(|_| JournalAppenderError::Journal(JournalEncodeError::AllocationFailed))?;
    let actual_length = file
        .metadata()
        .map_err(|error| io_error(JournalIoOperation::InspectRecoverySegment, error))?
        .len();
    if actual_length != expected_length {
        return Err(JournalAppenderError::TailMismatch(
            JournalTailMismatch::Length,
        ));
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| io_error(JournalIoOperation::ReadRecoverySegment, error))?;
    let mut offset = 0_usize;
    let mut buffer = [0_u8; STREAM_BUFFER_BYTES];
    while offset < expected.len() {
        let chunk_length = (expected.len() - offset).min(STREAM_BUFFER_BYTES);
        file.read_exact(&mut buffer[..chunk_length])
            .map_err(|error| io_error(JournalIoOperation::ReadRecoverySegment, error))?;
        if buffer[..chunk_length] != expected[offset..offset + chunk_length] {
            return Err(JournalAppenderError::TailMismatch(
                JournalTailMismatch::TailFingerprint,
            ));
        }
        offset += chunk_length;
    }
    Ok(())
}

fn repair_recovery_segment(
    path: &Path,
    expected: &[u8],
    valid_prefix: usize,
) -> Result<(), JournalAppenderError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| io_error(JournalIoOperation::OpenRecoverySegment, error))?;
    validate_recovery_file_bytes(&mut file, expected)?;
    let valid_prefix = u64::try_from(valid_prefix)
        .map_err(|_| JournalAppenderError::Journal(JournalEncodeError::AllocationFailed))?;
    file.set_len(valid_prefix)
        .map_err(|error| io_error(JournalIoOperation::RepairRecoverySegment, error))?;
    file.sync_all()
        .map_err(|error| io_error(JournalIoOperation::SyncSegment, error))
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
        .map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                JournalAppenderError::SegmentPathExists { temporary: true }
            } else {
                io_error(JournalIoOperation::CreateTemporarySegment, error)
            }
        })?;
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
    if let Err(error) = link_segment_no_clobber(&temporary_path, &final_path) {
        drop(temporary);
        let _ = fs::remove_file(&temporary_path);
        return Err(error);
    }
    // The installed hard link and candidate name reference the same inode, so
    // this second barrier covers the installed file before its directory entry
    // is declared durable.
    if let Err(error) = temporary.sync_all() {
        drop(temporary);
        return Err(io_error(JournalIoOperation::SyncSegment, error));
    }
    if let Err(error) = sync_directory(directory) {
        drop(temporary);
        return Err(io_error(JournalIoOperation::SyncDirectory, error));
    }
    drop(temporary);
    fs::remove_file(&temporary_path)
        .map_err(|error| io_error(JournalIoOperation::InstallSegment, error))?;
    sync_directory(directory)
        .map_err(|error| io_error(JournalIoOperation::SyncDirectory, error))?;
    Ok(final_path)
}

fn link_segment_no_clobber(
    temporary_path: &Path,
    final_path: &Path,
) -> Result<(), JournalAppenderError> {
    fs::hard_link(temporary_path, final_path).map_err(|error| {
        if error.kind() == io::ErrorKind::AlreadyExists {
            JournalAppenderError::SegmentPathExists { temporary: false }
        } else {
            io_error(JournalIoOperation::InstallSegment, error)
        }
    })
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
        journal_temporary_segment_path, link_segment_no_clobber,
    };
    use crate::{
        DurabilityMode, JournalId, JournalPayload, JournalReplay, RecordStopReason, ReplayLimits,
        ReplayStop, TaskPauseReason, replay_ordered_segments,
    };
    use ariax_core::{Generation, Gid};
    use std::collections::HashSet;
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

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

    fn other_gid() -> Gid {
        Gid::new(0x5678).expect("other gid")
    }

    fn other_journal_id() -> JournalId {
        JournalId::new([7; 16]).expect("other journal id")
    }

    fn task_created() -> JournalPayload {
        JournalPayload::TaskCreated {
            durability: DurabilityMode::Balanced,
            creator_version: 1,
        }
    }

    fn task_paused() -> JournalPayload {
        JournalPayload::TaskPaused {
            reason: TaskPauseReason::User,
        }
    }

    fn recover(
        directory: &Path,
        paths: &[PathBuf],
        expected_gid: Gid,
        expected_journal_id: JournalId,
    ) -> Result<(ControlJournalAppender, JournalReplay), JournalAppenderError> {
        ControlJournalAppender::open_recovered(
            directory,
            paths,
            expected_gid,
            expected_journal_id,
            ReplayLimits::default(),
            Generation::INITIAL,
            500,
        )
    }

    fn create_rotated_journal(directory: &Path, task_gid: Gid, id: JournalId) -> Vec<PathBuf> {
        let mut appender =
            ControlJournalAppender::create(directory, task_gid, id, Generation::INITIAL, 100)
                .expect("create rotated journal");
        appender
            .append_payload(Generation::INITIAL, &task_created())
            .expect("append first record");
        appender.flush(1).expect("flush first record");
        appender
            .rotate(Generation::INITIAL, 200)
            .expect("rotate journal");
        appender.segment_paths().to_vec()
    }

    fn replace_header_gid(path: &Path, replacement: Gid) {
        let mut bytes = fs::read(path).expect("read segment for gid replacement");
        bytes[8..16].copy_from_slice(&replacement.get().to_le_bytes());
        replace_header_crc(&mut bytes);
        fs::write(path, bytes).expect("write segment with replaced gid");
    }

    fn replace_header_journal_id(path: &Path, replacement: JournalId) {
        let mut bytes = fs::read(path).expect("read segment for journal replacement");
        bytes[16..32].copy_from_slice(replacement.as_bytes());
        replace_header_crc(&mut bytes);
        fs::write(path, bytes).expect("write segment with replaced journal id");
    }

    fn replace_header_previous_hash(path: &Path, replacement: [u8; 32]) {
        let mut bytes = fs::read(path).expect("read segment for link replacement");
        bytes[60..92].copy_from_slice(&replacement);
        replace_header_crc(&mut bytes);
        fs::write(path, bytes).expect("write segment with replaced link");
    }

    fn replace_header_crc(bytes: &mut [u8]) {
        let crc_offset = crate::SEGMENT_HEADER_LEN - 4;
        let crc = crc32c::crc32c(&bytes[..crc_offset]);
        bytes[crc_offset..crate::SEGMENT_HEADER_LEN].copy_from_slice(&crc.to_le_bytes());
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
    fn segment_publication_rejects_a_raced_destination_without_overwriting_it() {
        let directory = TestDirectory::new();
        let temporary = directory.path().join("owned-candidate.tmp");
        let destination = journal_segment_path(directory.path(), 0);
        fs::write(&temporary, b"candidate-header").expect("seed owned candidate");
        fs::write(&destination, b"raced-destination").expect("seed raced destination");

        assert_eq!(
            link_segment_no_clobber(&temporary, &destination),
            Err(JournalAppenderError::SegmentPathExists { temporary: false })
        );
        assert_eq!(
            fs::read(&destination).expect("read preserved destination"),
            b"raced-destination"
        );
        assert_eq!(
            fs::read(&temporary).expect("read still-owned candidate"),
            b"candidate-header"
        );
    }

    #[test]
    fn successful_segment_publication_removes_only_its_owned_candidate_name() {
        let directory = TestDirectory::new();
        let unrelated = directory.path().join("unrelated.tmp");
        fs::write(&unrelated, b"unrelated").expect("seed unrelated file");

        let appender = ControlJournalAppender::create(
            directory.path(),
            gid(),
            journal_id(),
            Generation::INITIAL,
            0,
        )
        .expect("create appender through no-clobber publication");

        assert!(appender.active_path().is_file());
        assert!(!journal_temporary_segment_path(directory.path(), 0).exists());
        assert_eq!(
            fs::read(unrelated).expect("read preserved unrelated file"),
            b"unrelated"
        );
    }

    #[test]
    fn concurrent_segment_creators_publish_exactly_one_no_clobber_winner() {
        const CONTENDERS: usize = 8;

        let directory = TestDirectory::new();
        let barrier = Arc::new(Barrier::new(CONTENDERS));
        let mut threads = Vec::new();
        for contender in 0..CONTENDERS {
            let directory = directory.path().to_path_buf();
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                ControlJournalAppender::create(
                    directory,
                    gid(),
                    journal_id(),
                    Generation::INITIAL,
                    contender as u64,
                )
            }));
        }

        let mut winner = None;
        for thread in threads {
            match thread.join().expect("creator thread") {
                Ok(appender) => {
                    assert!(winner.replace(appender).is_none(), "multiple creators won");
                }
                Err(JournalAppenderError::SegmentPathExists { .. }) => {}
                Err(error) => panic!("unexpected concurrent creation error: {error}"),
            }
        }

        let winner = winner.expect("one creator must publish the segment");
        assert_eq!(winner.active_header().task_gid(), gid());
        assert_eq!(winner.active_header().journal_id(), journal_id());
        assert_eq!(winner.segment_paths().len(), 1);
        assert_eq!(winner.segment_paths()[0], winner.active_path());
        assert!(!journal_temporary_segment_path(directory.path(), 0).exists());
    }

    #[test]
    fn recovered_open_resumes_a_clean_tail_at_the_next_sequence() {
        let directory = TestDirectory::new();
        let mut original = ControlJournalAppender::create(
            directory.path(),
            gid(),
            journal_id(),
            Generation::INITIAL,
            100,
        )
        .expect("create appender");
        original
            .append_payload(Generation::INITIAL, &task_created())
            .expect("append task");
        original.flush(1).expect("flush task");
        let paths = original.segment_paths().to_vec();
        drop(original);

        let (mut recovered, replay) =
            recover(directory.path(), &paths, gid(), journal_id()).expect("recover clean tail");
        assert_eq!(replay.stop, ReplayStop::CleanEnd);
        assert_eq!(recovered.appended_sequence(), 1);
        assert_eq!(recovered.flushed_sequence(), 1);
        assert_eq!(recovered.next_sequence(), 2);
        assert_eq!(recovered.segment_paths(), paths);
        assert_eq!(
            recovered
                .append_payload(Generation::INITIAL, &task_paused())
                .expect("append after recovery")
                .sequence(),
            2
        );
        recovered.flush(2).expect("flush recovered append");

        let bytes = fs::read(recovered.active_path()).expect("read recovered segment");
        let replay = replay_ordered_segments(&[&bytes], ReplayLimits::default());
        assert_eq!(replay.stop, ReplayStop::CleanEnd);
        assert_eq!(replay.last_sequence, 2);
    }

    #[test]
    fn recovered_open_resumes_a_clean_empty_rotated_tail() {
        let directory = TestDirectory::new();
        let paths = create_rotated_journal(directory.path(), gid(), journal_id());

        let (mut recovered, replay) =
            recover(directory.path(), &paths, gid(), journal_id()).expect("recover rotated tail");
        assert_eq!(replay.stop, ReplayStop::CleanEnd);
        assert_eq!(recovered.active_header().segment_index(), 1);
        assert_eq!(recovered.next_sequence(), 2);
        recovered
            .append_payload(Generation::INITIAL, &task_paused())
            .expect("append first record to rotated tail");
        recovered.flush(2).expect("flush rotated tail");

        let first = fs::read(&paths[0]).expect("read first segment");
        let second = fs::read(&paths[1]).expect("read second segment");
        let replay = replay_ordered_segments(&[&first, &second], ReplayLimits::default());
        assert_eq!(replay.stop, ReplayStop::CleanEnd);
        assert_eq!(replay.last_sequence, 2);
    }

    #[test]
    fn recovered_open_trims_a_torn_final_record_and_installs_a_linked_successor() {
        let directory = TestDirectory::new();
        let mut original = ControlJournalAppender::create(
            directory.path(),
            gid(),
            journal_id(),
            Generation::INITIAL,
            100,
        )
        .expect("create appender");
        original
            .append_payload(Generation::INITIAL, &task_created())
            .expect("append first");
        original
            .append_payload(Generation::INITIAL, &task_paused())
            .expect("append second");
        original.flush(2).expect("flush both");
        let first_path = original.active_path().to_path_buf();
        drop(original);
        let file = OpenOptions::new()
            .write(true)
            .open(&first_path)
            .expect("open for torn-tail simulation");
        let torn_length = file.metadata().expect("torn metadata").len() - 3;
        file.set_len(torn_length).expect("tear final commit");
        drop(file);
        let torn_bytes = fs::read(&first_path).expect("read torn segment");
        let torn_replay = replay_ordered_segments(&[&torn_bytes], ReplayLimits::default());
        let valid_prefix = torn_replay.valid_segment_prefixes[0];
        assert!(matches!(
            torn_replay.stop,
            ReplayStop::Record {
                reason: RecordStopReason::MissingCommit,
                ..
            }
        ));

        let (mut recovered, replay) = recover(
            directory.path(),
            std::slice::from_ref(&first_path),
            gid(),
            journal_id(),
        )
        .expect("repair torn tail");
        assert_eq!(replay.last_sequence, 1);
        assert_eq!(
            fs::metadata(&first_path).expect("trimmed metadata").len(),
            valid_prefix as u64
        );
        assert_eq!(recovered.active_header().segment_index(), 1);
        assert_eq!(recovered.segment_paths().len(), 2);
        assert_eq!(recovered.next_sequence(), 2);
        recovered
            .append_payload(Generation::INITIAL, &task_paused())
            .expect("replace torn fact");
        recovered.flush(2).expect("flush replacement");

        let first = fs::read(&recovered.segment_paths()[0]).expect("read repaired first");
        let second = fs::read(&recovered.segment_paths()[1]).expect("read successor");
        let replay = replay_ordered_segments(&[&first, &second], ReplayLimits::default());
        assert_eq!(replay.stop, ReplayStop::CleanEnd);
        assert_eq!(replay.last_sequence, 2);
    }

    #[test]
    fn recovered_open_trims_a_torn_first_record_without_inventing_sequence_zero_linkage() {
        let directory = TestDirectory::new();
        let original = ControlJournalAppender::create(
            directory.path(),
            gid(),
            journal_id(),
            Generation::INITIAL,
            100,
        )
        .expect("create empty appender");
        let path = original.active_path().to_path_buf();
        drop(original);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open for torn first record")
            .write_all(b"ARX")
            .expect("write torn framing");

        let (mut recovered, replay) = recover(
            directory.path(),
            std::slice::from_ref(&path),
            gid(),
            journal_id(),
        )
        .expect("repair torn first record");
        assert!(matches!(
            replay.stop,
            ReplayStop::Record {
                reason: RecordStopReason::TruncatedFraming,
                ..
            }
        ));
        assert_eq!(recovered.segment_paths(), std::slice::from_ref(&path));
        assert_eq!(recovered.active_length(), crate::SEGMENT_HEADER_LEN as u64);
        assert_eq!(recovered.next_sequence(), 1);
        recovered
            .append_payload(Generation::INITIAL, &task_created())
            .expect("append first committed fact");
        recovered.flush(1).expect("flush first fact");
        let bytes = fs::read(path).expect("read repaired journal");
        assert_eq!(
            replay_ordered_segments(&[&bytes], ReplayLimits::default()).stop,
            ReplayStop::CleanEnd
        );
    }

    #[test]
    fn recovered_open_rotates_a_torn_header_only_nonfirst_segment() {
        let directory = TestDirectory::new();
        let paths = create_rotated_journal(directory.path(), gid(), journal_id());
        OpenOptions::new()
            .append(true)
            .open(&paths[1])
            .expect("open empty rotated tail")
            .write_all(b"ARX")
            .expect("write torn first record in rotated tail");

        let (mut recovered, replay) =
            recover(directory.path(), &paths, gid(), journal_id()).expect("repair rotated tail");
        assert!(matches!(
            replay.stop,
            ReplayStop::Record {
                segment_index: 1,
                reason: RecordStopReason::TruncatedFraming,
                ..
            }
        ));
        assert_eq!(recovered.active_header().segment_index(), 2);
        assert_eq!(recovered.segment_paths().len(), 3);
        assert_eq!(recovered.next_sequence(), 2);
        recovered
            .append_payload(Generation::INITIAL, &task_paused())
            .expect("append after repaired empty rotation");
        recovered.flush(2).expect("flush repaired rotation");

        let bytes = recovered
            .segment_paths()
            .iter()
            .map(|path| fs::read(path).expect("read recovered segment"))
            .collect::<Vec<_>>();
        let slices = bytes.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let replay = replay_ordered_segments(&slices, ReplayLimits::default());
        assert_eq!(replay.stop, ReplayStop::CleanEnd);
        assert_eq!(replay.last_sequence, 2);
    }

    #[test]
    fn recovered_open_rejects_reordered_paths_and_reordered_contents() {
        let directory = TestDirectory::new();
        let paths = create_rotated_journal(directory.path(), gid(), journal_id());
        let first_before = fs::read(&paths[0]).expect("read first before reorder");
        let second_before = fs::read(&paths[1]).expect("read second before reorder");
        let reordered_paths = [paths[1].clone(), paths[0].clone()];
        assert!(matches!(
            recover(directory.path(), &reordered_paths, gid(), journal_id()),
            Err(JournalAppenderError::RecoverySegmentPath { input_index: 0 })
        ));
        assert_eq!(fs::read(&paths[0]).expect("preserved first"), first_before);
        assert_eq!(
            fs::read(&paths[1]).expect("preserved second"),
            second_before
        );

        fs::write(&paths[0], &second_before).expect("install index-one bytes first");
        fs::write(&paths[1], &first_before).expect("install index-zero bytes second");
        assert!(matches!(
            recover(directory.path(), &paths, gid(), journal_id()),
            Err(JournalAppenderError::RecoveryStopped(
                ReplayStop::SegmentIndex {
                    expected: 0,
                    actual: 1
                }
            ))
        ));
    }

    #[test]
    fn recovered_open_rejects_expected_and_cross_segment_identity_mismatches() {
        let directory = TestDirectory::new();
        let paths = create_rotated_journal(directory.path(), gid(), journal_id());
        let expected_other_gid = other_gid();
        assert!(matches!(
            recover(directory.path(), &paths, expected_other_gid, journal_id()),
            Err(JournalAppenderError::RecoveryTaskMismatch {
                expected,
                actual,
            }) if expected == expected_other_gid && actual == gid()
        ));
        let expected_other_journal = other_journal_id();
        assert!(matches!(
            recover(directory.path(), &paths, gid(), expected_other_journal),
            Err(JournalAppenderError::RecoveryJournalMismatch {
                expected,
                actual,
            }) if expected == expected_other_journal && actual == journal_id()
        ));

        replace_header_gid(&paths[1], other_gid());
        let changed = fs::read(&paths[1]).expect("read task-mismatched segment");
        assert!(matches!(
            recover(directory.path(), &paths, gid(), journal_id()),
            Err(JournalAppenderError::RecoveryStopped(
                ReplayStop::TaskMismatch
            ))
        ));
        assert_eq!(fs::read(&paths[1]).expect("preserved mismatch"), changed);

        let second_directory = TestDirectory::new();
        let second_paths = create_rotated_journal(second_directory.path(), gid(), journal_id());
        replace_header_journal_id(&second_paths[1], other_journal_id());
        assert!(matches!(
            recover(second_directory.path(), &second_paths, gid(), journal_id()),
            Err(JournalAppenderError::RecoveryStopped(
                ReplayStop::JournalMismatch
            ))
        ));
    }

    #[test]
    fn recovered_open_rejects_a_mismatched_previous_segment_link() {
        let directory = TestDirectory::new();
        let paths = create_rotated_journal(directory.path(), gid(), journal_id());
        replace_header_previous_hash(&paths[1], [5; 32]);
        let changed = fs::read(&paths[1]).expect("read link-mismatched segment");
        assert!(matches!(
            recover(directory.path(), &paths, gid(), journal_id()),
            Err(JournalAppenderError::RecoveryStopped(
                ReplayStop::PreviousHashMismatch
            ))
        ));
        assert_eq!(
            fs::read(&paths[1]).expect("preserved link mismatch"),
            changed
        );
        assert!(!journal_segment_path(directory.path(), 2).exists());
    }

    #[test]
    fn recovered_open_rejects_a_nonfinal_corrupt_suffix_without_repairing_it() {
        let directory = TestDirectory::new();
        let paths = create_rotated_journal(directory.path(), gid(), journal_id());
        OpenOptions::new()
            .append(true)
            .open(&paths[0])
            .expect("open nonfinal segment")
            .write_all(b"ARX")
            .expect("append corrupt nonfinal suffix");
        let first_before = fs::read(&paths[0]).expect("read corrupt nonfinal segment");
        assert!(matches!(
            recover(directory.path(), &paths, gid(), journal_id()),
            Err(JournalAppenderError::RecoveryStopped(ReplayStop::Record {
                segment_index: 0,
                reason: RecordStopReason::TruncatedFraming,
                ..
            }))
        ));
        assert_eq!(
            fs::read(&paths[0]).expect("read preserved nonfinal segment"),
            first_before
        );
    }

    #[test]
    fn recovered_open_rejects_a_sequence_gap_instead_of_discarding_it() {
        let directory = TestDirectory::new();
        let mut original = ControlJournalAppender::create(
            directory.path(),
            gid(),
            journal_id(),
            Generation::INITIAL,
            100,
        )
        .expect("create appender");
        original
            .append_payload(Generation::INITIAL, &task_created())
            .expect("append task");
        original.flush(1).expect("flush task");
        let path = original.active_path().to_path_buf();
        drop(original);
        let mut bytes = fs::read(&path).expect("read segment for sequence mutation");
        let record_offset = crate::SEGMENT_HEADER_LEN;
        bytes[record_offset + 20..record_offset + 28].copy_from_slice(&2_u64.to_le_bytes());
        let payload_length = u32::from_le_bytes(
            bytes[record_offset + 4..record_offset + 8]
                .try_into()
                .expect("payload length bytes"),
        ) as usize;
        let crc_offset = record_offset + crate::RECORD_PREFIX_LEN + payload_length;
        let crc = crc32c::crc32c(&bytes[record_offset..crc_offset]);
        bytes[crc_offset..crc_offset + 4].copy_from_slice(&crc.to_le_bytes());
        fs::write(&path, &bytes).expect("write sequence gap");

        assert!(matches!(
            recover(
                directory.path(),
                std::slice::from_ref(&path),
                gid(),
                journal_id()
            ),
            Err(JournalAppenderError::RecoveryStopped(ReplayStop::Record {
                reason: RecordStopReason::SequenceGap {
                    expected: 1,
                    actual: 2
                },
                ..
            }))
        ));
        assert_eq!(fs::read(path).expect("preserved sequence gap"), bytes);
        assert!(!journal_segment_path(directory.path(), 1).exists());
    }

    #[test]
    fn recovered_open_preserves_a_preexisting_successor_and_torn_tail() {
        let directory = TestDirectory::new();
        let mut original = ControlJournalAppender::create(
            directory.path(),
            gid(),
            journal_id(),
            Generation::INITIAL,
            100,
        )
        .expect("create appender");
        original
            .append_payload(Generation::INITIAL, &task_created())
            .expect("append first");
        original
            .append_payload(Generation::INITIAL, &task_paused())
            .expect("append second");
        original.flush(2).expect("flush records");
        let path = original.active_path().to_path_buf();
        drop(original);
        let file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open for tear");
        file.set_len(file.metadata().expect("metadata").len() - 3)
            .expect("tear tail");
        drop(file);
        let torn_before = fs::read(&path).expect("read torn input");
        let successor = journal_segment_path(directory.path(), 1);
        fs::write(&successor, b"preexisting-successor").expect("seed successor");

        assert!(matches!(
            recover(
                directory.path(),
                std::slice::from_ref(&path),
                gid(),
                journal_id()
            ),
            Err(JournalAppenderError::SegmentPathExists { temporary: false })
        ));
        assert_eq!(fs::read(&path).expect("preserved torn tail"), torn_before);
        assert_eq!(
            fs::read(successor).expect("preserved successor"),
            b"preexisting-successor"
        );
    }

    #[test]
    fn recovered_open_preserves_a_preexisting_candidate_and_torn_tail() {
        let directory = TestDirectory::new();
        let mut original = ControlJournalAppender::create(
            directory.path(),
            gid(),
            journal_id(),
            Generation::INITIAL,
            100,
        )
        .expect("create appender");
        original
            .append_payload(Generation::INITIAL, &task_created())
            .expect("append first");
        original
            .append_payload(Generation::INITIAL, &task_paused())
            .expect("append second");
        original.flush(2).expect("flush records");
        let path = original.active_path().to_path_buf();
        drop(original);
        let file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open for tear");
        file.set_len(file.metadata().expect("metadata").len() - 3)
            .expect("tear tail");
        drop(file);
        let torn_before = fs::read(&path).expect("read torn input");
        let candidate = journal_temporary_segment_path(directory.path(), 1);
        fs::write(&candidate, b"preexisting-candidate").expect("seed candidate");

        assert!(matches!(
            recover(
                directory.path(),
                std::slice::from_ref(&path),
                gid(),
                journal_id()
            ),
            Err(JournalAppenderError::SegmentPathExists { temporary: true })
        ));
        assert_eq!(fs::read(&path).expect("preserved torn tail"), torn_before);
        assert_eq!(
            fs::read(candidate).expect("preserved candidate"),
            b"preexisting-candidate"
        );
    }

    #[cfg(unix)]
    #[test]
    fn recovered_open_rejects_an_exact_named_symlink_during_portable_preflight() {
        use std::os::unix::fs::symlink;

        let directory = TestDirectory::new();
        let mut original = ControlJournalAppender::create(
            directory.path(),
            gid(),
            journal_id(),
            Generation::INITIAL,
            100,
        )
        .expect("create appender");
        original
            .append_payload(Generation::INITIAL, &task_created())
            .expect("append task");
        original.flush(1).expect("flush task");
        let named_path = original.active_path().to_path_buf();
        drop(original);
        let backing_path = directory.path().join("segment-backing.arxj");
        fs::rename(&named_path, &backing_path).expect("move segment behind a symlink");
        symlink(&backing_path, &named_path).expect("install exact-name symlink");
        let backing_before = fs::read(&backing_path).expect("read backing segment");

        assert!(matches!(
            recover(
                directory.path(),
                std::slice::from_ref(&named_path),
                gid(),
                journal_id()
            ),
            Err(JournalAppenderError::RecoverySegmentPath { input_index: 0 })
        ));
        assert!(
            named_path
                .symlink_metadata()
                .expect("symlink metadata")
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            fs::read(backing_path).expect("preserved backing segment"),
            backing_before
        );
    }

    #[test]
    fn recovered_open_rejects_empty_noncanonical_and_over_budget_sets() {
        let directory = TestDirectory::new();
        assert!(matches!(
            recover(directory.path(), &[], gid(), journal_id()),
            Err(JournalAppenderError::RecoveryStopped(
                ReplayStop::NoSegments
            ))
        ));

        let mut original = ControlJournalAppender::create(
            directory.path(),
            gid(),
            journal_id(),
            Generation::INITIAL,
            100,
        )
        .expect("create appender");
        original
            .append_payload(Generation::INITIAL, &task_created())
            .expect("append task");
        original.flush(1).expect("flush task");
        let canonical = original.active_path().to_path_buf();
        drop(original);
        let noncanonical = directory.path().join("renamed.arxj");
        fs::copy(&canonical, &noncanonical).expect("copy noncanonical segment");
        assert!(matches!(
            recover(
                directory.path(),
                std::slice::from_ref(&noncanonical),
                gid(),
                journal_id()
            ),
            Err(JournalAppenderError::RecoverySegmentPath { input_index: 0 })
        ));

        let limits = ReplayLimits {
            max_segments: 1,
            max_records: 0,
            max_payload_bytes: 0,
        };
        assert!(matches!(
            ControlJournalAppender::open_recovered(
                directory.path(),
                std::slice::from_ref(&canonical),
                gid(),
                journal_id(),
                limits,
                Generation::INITIAL,
                500,
            ),
            Err(JournalAppenderError::RecoveryInputBytesExceeded { .. })
        ));
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
