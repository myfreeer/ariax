//! Runtime-independent ownership of active-transfer replay reservations.
use super::*;
use std::ops::Deref;
use std::sync::Arc;

/// The caller must retain a domain and global memory charge in the returned guard.
pub trait JournalReplayBudget: fmt::Debug + Send + Sync {
    fn reserve(&self, bytes: usize) -> Option<JournalReplayReservation>;
}

trait ReplayCharge: fmt::Debug + Send + Sync {}
impl<T: fmt::Debug + Send + Sync> ReplayCharge for T {}

/// Opaque, move-only charge supplied by the runtime without a storage dependency on it.
#[derive(Debug)]
pub struct JournalReplayReservation {
    _charge: Box<dyn ReplayCharge>,
}

impl JournalReplayReservation {
    pub fn new(charge: impl fmt::Debug + Send + Sync + 'static) -> Self {
        Self {
            _charge: Box::new(charge),
        }
    }
}

/// Clones share both immutable replay data and its reservation.
#[derive(Clone, Debug)]
pub struct ReservedJournalReplay(Arc<ReservedReplayInner>);

#[derive(Debug)]
struct ReservedReplayInner {
    replay: JournalReplay,
    // Field order releases the allocation before its charge.
    _reservation: JournalReplayReservation,
}

impl ReservedJournalReplay {
    fn new(replay: JournalReplay, reservation: JournalReplayReservation) -> Self {
        Self(Arc::new(ReservedReplayInner {
            replay,
            _reservation: reservation,
        }))
    }
}

impl Deref for ReservedJournalReplay {
    type Target = JournalReplay;
    fn deref(&self) -> &Self::Target {
        &self.0.replay
    }
}

impl PartialEq for ReservedJournalReplay {
    fn eq(&self, other: &Self) -> bool {
        self.0.replay == other.0.replay
    }
}
impl Eq for ReservedJournalReplay {}

impl ControlJournalAppender {
    /// Reserves before proportional input/replay allocation, retaining the charge
    /// in the result even if its completion receiver or original owner disappears.
    pub fn snapshot_with_budget(
        &mut self,
        limits: ReplayLimits,
        budget: &dyn JournalReplayBudget,
    ) -> Result<ReservedJournalReplay, JournalAppenderError> {
        let (limits, bytes) =
            replay_requirements(&self.directory_capability, &self.segment_paths, limits)?;
        let reservation = budget
            .reserve(bytes)
            .ok_or(JournalAppenderError::RecoveryMemoryLimit { requested: bytes })?;
        self.snapshot(limits)
            .map(|replay| ReservedJournalReplay::new(replay, reservation))
    }

    /// Descriptor-based size admission for the standalone HTTP recovery path.
    #[allow(clippy::too_many_arguments)]
    pub fn open_recovered_with_budget(
        directory: impl AsRef<Path>,
        installed_segment_paths: &[PathBuf],
        expected_task_gid: Gid,
        expected_journal_id: JournalId,
        limits: ReplayLimits,
        recovery_starting_generation: Generation,
        recovery_created_at_unix_ms: u64,
        budget: &dyn JournalReplayBudget,
    ) -> Result<(Self, ReservedJournalReplay), JournalAppenderError> {
        let capability = JournalDirectoryCapability::open_trusted(directory.as_ref())
            .map_err(|error| capability_error(JournalIoOperation::OpenRecoverySegment, error))?;
        let (limits, bytes) = replay_requirements(&capability, installed_segment_paths, limits)?;
        let reservation = budget
            .reserve(bytes)
            .ok_or(JournalAppenderError::RecoveryMemoryLimit { requested: bytes })?;
        let prepared = Self::prepare_recovered_in(
            capability,
            installed_segment_paths,
            expected_task_gid,
            expected_journal_id,
            limits,
        )?;
        let (appender, replay) = Self::open_prepared(
            prepared,
            recovery_starting_generation,
            recovery_created_at_unix_ms,
        )?;
        Ok((appender, ReservedJournalReplay::new(replay, reservation)))
    }
}

fn replay_requirements(
    directory: &JournalDirectoryCapability,
    paths: &[PathBuf],
    limits: ReplayLimits,
) -> Result<(ReplayLimits, usize), JournalAppenderError> {
    let overflow = || JournalAppenderError::Journal(JournalEncodeError::AllocationFailed);
    if paths.is_empty() {
        return Err(JournalAppenderError::RecoveryStopped(
            ReplayStop::NoSegments,
        ));
    }
    if paths.len() > limits.max_segments {
        return Err(JournalAppenderError::RecoveryStopped(
            ReplayStop::ResourceLimit(ReplayResource::Segments),
        ));
    }
    let maximum_encoded = recovery_encoded_byte_budget(limits)?;
    let mut encoded = 0_usize;
    let mut path_bytes = 0_usize;
    for (index, path) in paths.iter().enumerate() {
        let name = journal_segment_file_name(u32::try_from(index).map_err(|_| overflow())?);
        if *path != directory.display().join(&name) {
            return Err(JournalAppenderError::RecoverySegmentPath { input_index: index });
        }
        // Publication aliases are checked by prepare_recovered_in before use.
        let file = directory
            .open_regular_file_for_publication_validation(OsStr::new(&name))
            .map_err(|_| JournalAppenderError::RecoverySegmentPath { input_index: index })?;
        let len = file
            .metadata()
            .map_err(|e| io_error(JournalIoOperation::InspectRecoverySegment, e))?
            .len();
        encoded = encoded
            .checked_add(usize::try_from(len).map_err(|_| overflow())?)
            .ok_or_else(overflow)?;
        if encoded > maximum_encoded {
            return Err(JournalAppenderError::RecoveryInputBytesExceeded {
                limit: maximum_encoded,
            });
        }
        path_bytes = path_bytes
            .checked_add(path.as_os_str().as_encoded_bytes().len())
            .ok_or_else(overflow)?;
    }
    let bounded = ReplayLimits {
        max_segments: paths.len(),
        max_records: limits.max_records.min(encoded / RECORD_OVERHEAD),
        max_payload_bytes: limits.max_payload_bytes.min(encoded),
    };
    // Re-read lengths may change; charge the enforced narrowed limit, not just
    // the preceding observation. Vec growth stays below twice its record cap
    // (plus its minimum capacity); payload copies have exact boxed lengths.
    let bytes = recovery_encoded_byte_budget(bounded)?
        .checked_add(bounded.max_payload_bytes)
        .and_then(|n| {
            bounded
                .max_records
                .checked_mul(2)?
                .checked_add(4)?
                .checked_mul(std::mem::size_of::<crate::JournalRecord>())?
                .checked_add(n)
        })
        .and_then(|n| path_bytes.checked_mul(8)?.checked_add(n))
        .and_then(|n| bounded.max_segments.checked_mul(1024)?.checked_add(n))
        .and_then(|n| n.checked_add(64 * 1024))
        .ok_or_else(overflow)?;
    Ok((bounded, bytes))
}
