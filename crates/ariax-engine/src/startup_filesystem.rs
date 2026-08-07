use crate::{
    DeferredAppenderRecovery, DeferredJournalInstallRecovery, NativeInstallOutcome,
    NativeStartupBackend, RecoveredEngineTask,
};
use ariax_storage::{
    ControlJournalAppender, JournalAppenderError, JournalDirectoryCapability, JournalId,
    JournalInstallPhase, JournalStateLimits, JournalStateStop, NativeCapabilityError,
    PersistedOptionPolicy, PlatformPath, PreparedJournalSet, ReplayLimits, RootDirectoryCapability,
    recover_journal_state,
};
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug)]
pub enum NativeFilesystemError {
    Capability(NativeCapabilityError),
    Journal(JournalAppenderError),
    MissingRootBinding,
    UnsupportedJournalLocation,
    DirectoryIdentityChanged,
    SequenceMismatch { expected: u64, actual: u64 },
    CheckpointMismatch,
    SemanticReplay(JournalStateStop),
}

impl NativeFilesystemError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Capability(_) => "capability",
            Self::Journal(_) => "journal",
            Self::MissingRootBinding => "missing_root_binding",
            Self::UnsupportedJournalLocation => "unsupported_journal_location",
            Self::DirectoryIdentityChanged => "directory_identity_changed",
            Self::SequenceMismatch { .. } => "sequence_mismatch",
            Self::CheckpointMismatch => "checkpoint_mismatch",
            Self::SemanticReplay(_) => "semantic_replay",
        }
    }
}

impl fmt::Display for NativeFilesystemError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capability(error) => error.fmt(formatter),
            Self::Journal(error) => error.fmt(formatter),
            Self::MissingRootBinding => {
                formatter.write_str("recovered layout is missing its native root binding")
            }
            Self::UnsupportedJournalLocation => {
                formatter.write_str("native startup supports only central journal sets")
            }
            Self::DirectoryIdentityChanged => {
                formatter.write_str("journal directory identity changed during recovery")
            }
            Self::SequenceMismatch { expected, actual } => write!(
                formatter,
                "journal sequence {actual} does not match expected sequence {expected}"
            ),
            Self::CheckpointMismatch => {
                formatter.write_str("journal checkpoint does not match its install intent")
            }
            Self::SemanticReplay(stop) => {
                write!(formatter, "journal semantic replay stopped at {stop:?}")
            }
        }
    }
}

impl Error for NativeFilesystemError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Capability(error) => Some(error),
            Self::Journal(error) => Some(error),
            _ => None,
        }
    }
}

impl From<NativeCapabilityError> for NativeFilesystemError {
    fn from(error: NativeCapabilityError) -> Self {
        Self::Capability(error)
    }
}

impl From<JournalAppenderError> for NativeFilesystemError {
    fn from(error: JournalAppenderError) -> Self {
        Self::Journal(error)
    }
}

/// Explicit internal policy for the central-journal native startup stage.
pub struct NativeFilesystemPolicy {
    control_root: JournalDirectoryCapability,
    allowed_output_roots: Vec<RootDirectoryCapability>,
    replay_limits: ReplayLimits,
    state_limits: JournalStateLimits,
    option_policy: Arc<dyn PersistedOptionPolicy + Send + Sync>,
}

impl NativeFilesystemPolicy {
    pub fn new<P>(
        control_directory: impl AsRef<Path>,
        allowed_output_roots: impl IntoIterator<Item = PathBuf>,
        replay_limits: ReplayLimits,
        state_limits: JournalStateLimits,
        option_policy: P,
    ) -> Result<Self, NativeFilesystemError>
    where
        P: PersistedOptionPolicy + Send + Sync + 'static,
    {
        let control_root = JournalDirectoryCapability::open_trusted(control_directory)?;
        let allowed_output_roots = allowed_output_roots
            .into_iter()
            .map(RootDirectoryCapability::open_trusted)
            .collect::<Result<Vec<_>, _>>()?;
        if allowed_output_roots.len() > ariax_storage::MAX_NATIVE_ALLOWED_ROOTS {
            return Err(NativeCapabilityError::TooManyAllowedRoots.into());
        }
        Ok(Self {
            control_root,
            allowed_output_roots,
            replay_limits,
            state_limits,
            option_policy: Arc::new(option_policy),
        })
    }

    #[must_use]
    pub fn control_root(&self) -> &JournalDirectoryCapability {
        &self.control_root
    }
}

/// Concrete central-journal descriptor-safe startup backend.
pub struct NativeFilesystemBackend {
    policy: NativeFilesystemPolicy,
}

impl NativeFilesystemBackend {
    #[must_use]
    pub const fn new(policy: NativeFilesystemPolicy) -> Self {
        Self { policy }
    }

    fn prepare_set(
        &self,
        path: &PlatformPath,
        task_id: ariax_core::TaskId,
        gid: ariax_core::Gid,
        journal_id: JournalId,
        expected_last_sequence: Option<u64>,
    ) -> Result<PreparedJournalSet, NativeFilesystemError> {
        let directory =
            JournalDirectoryCapability::open_persisted_under(&self.policy.control_root, path)?;
        let paths = ControlJournalAppender::discover_segment_paths(
            &directory,
            self.policy.replay_limits.max_segments,
        )?;
        let expected_directory_identity = directory.identity();
        let prepared = ControlJournalAppender::prepare_recovered_in(
            directory,
            &paths,
            gid,
            journal_id,
            self.policy.replay_limits,
        )?;
        if prepared.directory_identity() != expected_directory_identity {
            return Err(NativeFilesystemError::DirectoryIdentityChanged);
        }
        if let Some(expected) = expected_last_sequence {
            let actual = prepared.replay().last_sequence;
            if actual != expected {
                return Err(NativeFilesystemError::SequenceMismatch { expected, actual });
            }
        }
        let semantic = recover_journal_state(
            &prepared.replay().records,
            task_id,
            self.policy.option_policy.as_ref(),
            self.policy.state_limits,
        );
        if semantic.accepted_records != prepared.replay().records.len()
            || !matches!(semantic.stop, JournalStateStop::CleanEnd)
        {
            return Err(NativeFilesystemError::SemanticReplay(semantic.stop));
        }
        Ok(prepared)
    }

    fn candidate_is_acceptable(
        &self,
        request: &DeferredJournalInstallRecovery,
        prepared: &PreparedJournalSet,
    ) -> Result<(), NativeFilesystemError> {
        if (request.intent.phase == JournalInstallPhase::Installing
            && (request.authoritative_journal_id != request.intent.old_journal_id
                || request.authoritative_last_sequence > request.intent.source_last_sequence))
            || (request.intent.phase == JournalInstallPhase::Installed
                && request.authoritative_journal_id != request.intent.new_journal_id)
        {
            return Err(NativeFilesystemError::CheckpointMismatch);
        }
        let semantic = recover_journal_state(
            &prepared.replay().records,
            request.task_id,
            self.policy.option_policy.as_ref(),
            self.policy.state_limits,
        );
        if semantic.accepted_records != prepared.replay().records.len()
            || !matches!(semantic.stop, JournalStateStop::CleanEnd)
        {
            return Err(NativeFilesystemError::SemanticReplay(semantic.stop));
        }
        let checkpoint = semantic
            .state
            .as_ref()
            .and_then(|state| state.checkpoint())
            .ok_or(NativeFilesystemError::CheckpointMismatch)?;
        if checkpoint.checkpoint_id != request.intent.checkpoint_id
            || checkpoint.source_last_sequence != request.intent.source_last_sequence
            || checkpoint.end_sequence > prepared.replay().last_sequence
            || (request.intent.phase == JournalInstallPhase::Installed
                && request.authoritative_checkpoint.as_ref() != Some(checkpoint))
        {
            return Err(NativeFilesystemError::CheckpointMismatch);
        }
        Ok(())
    }

    fn cleanup_candidate(&self, path: &PlatformPath) -> Result<(), NativeFilesystemError> {
        let directory =
            JournalDirectoryCapability::open_persisted_under(&self.policy.control_root, path)?;
        let max_entries = self.policy.replay_limits.max_segments.saturating_mul(2);
        ControlJournalAppender::retire_owned_segment_artifacts(&directory, max_entries)?;
        Ok(())
    }

    fn prepare_install_path(
        &self,
        request: &DeferredJournalInstallRecovery,
        path: &PlatformPath,
        journal_id: JournalId,
        expected_last_sequence: Option<u64>,
    ) -> Result<PreparedJournalSet, NativeFilesystemError> {
        self.prepare_set(
            path,
            request.task_id,
            request.intent.gid,
            journal_id,
            expected_last_sequence,
        )
    }
}

impl NativeStartupBackend for NativeFilesystemBackend {
    type RootCapability = Option<RootDirectoryCapability>;
    type Error = NativeFilesystemError;

    fn acquire_root(
        &mut self,
        task: &RecoveredEngineTask,
    ) -> Result<Self::RootCapability, Self::Error> {
        let Some(recovered_layout) = task.journal.layout() else {
            return Ok(None);
        };
        let layout = recovered_layout.layout();
        let binding = layout.root_binding();
        let root = RootDirectoryCapability::open_bound(
            binding.path(),
            binding.root_identity(),
            &self.policy.allowed_output_roots,
        )?;
        for file in layout.files() {
            if let Some(identity) = file.identity() {
                root.verify_file(file.safe_path(), identity)?;
            }
        }
        Ok(Some(root))
    }

    fn recover_journal_install(
        &mut self,
        request: &DeferredJournalInstallRecovery,
        _root: &Self::RootCapability,
    ) -> Result<NativeInstallOutcome, Self::Error> {
        match request.intent.phase {
            JournalInstallPhase::Installing => {
                let candidate = self.prepare_install_path(
                    request,
                    &request.intent.new_path,
                    request.intent.new_journal_id,
                    None,
                );
                match candidate {
                    Ok(prepared) if self.candidate_is_acceptable(request, &prepared).is_ok() => {
                        Ok(NativeInstallOutcome::AcceptedCandidate(prepared))
                    }
                    Ok(_) | Err(_) => {
                        self.cleanup_candidate(&request.intent.new_path)?;
                        let old = self.prepare_install_path(
                            request,
                            &request.intent.old_path,
                            request.intent.old_journal_id,
                            Some(request.authoritative_last_sequence),
                        )?;
                        Ok(NativeInstallOutcome::RejectedCandidate(old))
                    }
                }
            }
            JournalInstallPhase::Installed => {
                let prepared = self.prepare_install_path(
                    request,
                    &request.intent.new_path,
                    request.intent.new_journal_id,
                    Some(request.authoritative_last_sequence),
                )?;
                self.candidate_is_acceptable(request, &prepared)?;
                Ok(NativeInstallOutcome::ValidatedInstalled(prepared))
            }
        }
    }

    fn retire_journal_install(
        &mut self,
        request: &DeferredJournalInstallRecovery,
        _root: &Self::RootCapability,
    ) -> Result<(), Self::Error> {
        let directory = JournalDirectoryCapability::open_persisted_under(
            &self.policy.control_root,
            &request.intent.old_path,
        )?;
        ControlJournalAppender::retire_owned_segment_artifacts(
            &directory,
            self.policy.replay_limits.max_segments.saturating_mul(2),
        )?;
        Ok(())
    }

    fn prepare_recovered_journal(
        &mut self,
        request: &DeferredAppenderRecovery,
        _root: &Self::RootCapability,
    ) -> Result<PreparedJournalSet, Self::Error> {
        if request.replica.is_some() {
            return Err(NativeFilesystemError::UnsupportedJournalLocation);
        }
        self.prepare_set(
            &request.primary_path,
            request.task_id,
            request.gid,
            request.journal_id,
            Some(request.expected_last_sequence),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{NativeFilesystemBackend, NativeFilesystemError, NativeFilesystemPolicy};
    use crate::{
        DeferredAppenderRecovery, DeferredJournalInstallRecovery, DeferredReplicaRecovery,
        NativeInstallOutcome, NativeStartupBackend,
    };
    use ariax_core::{Generation, Gid, TaskId};
    use ariax_storage::{
        CheckpointId, ControlJournalAppender, DurabilityMode, JournalHash, JournalId,
        JournalInstallIntent, JournalInstallPhase, JournalPayload, JournalRecord,
        JournalStateLimits, JournalStateStop, OptionsSnapshotScope, PlatformPath,
        RecoveredCheckpoint, ReplayLimits, SanitizedOptionMap, calculate_checkpoint_state_hash,
        recover_journal_state,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "ariax-native-filesystem-{label}-{}-{}",
                std::process::id(),
                TEST_ID.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).expect("create test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn allow_all_options(_name: &str) -> bool {
        true
    }

    fn gid() -> Gid {
        Gid::new(1).expect("gid")
    }

    fn task_id() -> TaskId {
        TaskId::new(1).expect("task id")
    }

    fn journal(marker: u8) -> JournalId {
        JournalId::new([marker; 16]).expect("journal id")
    }

    fn hash(marker: u8) -> JournalHash {
        JournalHash::new([marker; 32]).expect("journal hash")
    }

    fn path(value: &Path) -> PlatformPath {
        PlatformPath::from_current(value).expect("platform path")
    }

    fn backend(control: &TestDirectory) -> NativeFilesystemBackend {
        NativeFilesystemBackend::new(
            NativeFilesystemPolicy::new(
                &control.0,
                Vec::<PathBuf>::new(),
                ReplayLimits::default(),
                JournalStateLimits::default(),
                allow_all_options,
            )
            .expect("native policy"),
        )
    }

    fn create_live_journal(control: &TestDirectory, name: &str, id: JournalId) -> PathBuf {
        let directory = control.0.join(name);
        let mut appender =
            ControlJournalAppender::create(&directory, gid(), id, Generation::INITIAL, 100)
                .expect("create live journal");
        let appended = appender
            .append_payload(
                Generation::INITIAL,
                &JournalPayload::TaskCreated {
                    durability: DurabilityMode::Balanced,
                    creator_version: 1,
                },
            )
            .expect("append live state");
        appender
            .flush(appended.sequence())
            .expect("flush live state");
        drop(appender);
        directory
    }

    fn create_checkpoint_journal(
        control: &TestDirectory,
        name: &str,
        id: JournalId,
        checkpoint_id: CheckpointId,
        source_last_sequence: u64,
    ) -> (PathBuf, RecoveredCheckpoint) {
        let directory = control.0.join(name);
        let task_created = JournalPayload::TaskCreated {
            durability: DurabilityMode::Balanced,
            creator_version: 1,
        };
        let options =
            SanitizedOptionMap::new(Vec::<(String, String)>::new()).expect("empty options");
        let current_options = JournalPayload::OptionsSnapshot {
            scope: OptionsSnapshotScope::CurrentGeneration,
            patch_id: None,
            snapshot_hash: options.snapshot_hash(),
            options,
        };
        let state_payloads = [task_created, current_options];
        let state_records = state_payloads
            .iter()
            .enumerate()
            .map(|(index, payload)| JournalRecord {
                record_type: payload.record_type(),
                generation: Generation::INITIAL,
                sequence: index as u64 + 2,
                payload: payload.encode().expect("encode checkpoint state"),
            })
            .collect::<Vec<_>>();
        let state_hash =
            calculate_checkpoint_state_hash(&state_records).expect("checkpoint state hash");
        let mut appender =
            ControlJournalAppender::create(&directory, gid(), id, Generation::INITIAL, 100)
                .expect("create checkpoint journal");
        appender
            .append_payload(
                Generation::INITIAL,
                &JournalPayload::CheckpointStart {
                    checkpoint_id,
                    source_last_sequence,
                    source_segment_hash: hash(7),
                    state_record_count: 2,
                    created_at_unix_ms: 100,
                },
            )
            .expect("append checkpoint start");
        for payload in &state_payloads {
            appender
                .append_payload(Generation::INITIAL, payload)
                .expect("append checkpoint state");
        }
        let appended = appender
            .append_payload(
                Generation::INITIAL,
                &JournalPayload::CheckpointEnd {
                    checkpoint_id,
                    state_record_count: 2,
                    state_hash,
                },
            )
            .expect("append checkpoint end");
        appender
            .flush(appended.sequence())
            .expect("flush checkpoint");
        let records = appender
            .segment_paths()
            .iter()
            .map(fs::read)
            .collect::<Result<Vec<_>, _>>()
            .expect("read checkpoint segments");
        drop(appender);
        let slices = records.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let replay = ariax_storage::replay_ordered_segments(&slices, ReplayLimits::default());
        let semantic = recover_journal_state(
            &replay.records,
            task_id(),
            &allow_all_options,
            JournalStateLimits::default(),
        );
        assert_eq!(semantic.stop, JournalStateStop::CleanEnd);
        let checkpoint = semantic
            .state
            .expect("checkpoint state")
            .checkpoint()
            .expect("checkpoint envelope")
            .clone();
        (directory, checkpoint)
    }

    fn appender_request(
        directory: &Path,
        id: JournalId,
        expected: u64,
    ) -> DeferredAppenderRecovery {
        DeferredAppenderRecovery {
            task_id: task_id(),
            gid: gid(),
            journal_id: id,
            primary_path: path(directory),
            replica: None,
            expected_last_sequence: expected,
        }
    }

    #[test]
    fn central_journal_preparation_is_descriptor_relative_and_sequence_bound() {
        let control = TestDirectory::new("ordinary");
        let directory = create_live_journal(&control, "journal", journal(1));
        let mut backend = backend(&control);
        let prepared = backend
            .prepare_recovered_journal(&appender_request(&directory, journal(1), 1), &None)
            .expect("prepare central journal");
        assert_eq!(prepared.replay().last_sequence, 1);
        drop(prepared);

        assert!(matches!(
            backend.prepare_recovered_journal(&appender_request(&directory, journal(1), 2), &None,),
            Err(NativeFilesystemError::SequenceMismatch {
                expected: 2,
                actual: 1,
            })
        ));
    }

    #[test]
    fn central_backend_rejects_replica_and_outside_control_paths() {
        let control = TestDirectory::new("path-policy");
        let outside = TestDirectory::new("outside");
        let directory = create_live_journal(&control, "journal", journal(1));
        let outside_directory = create_live_journal(&outside, "journal", journal(1));
        let mut backend = backend(&control);
        let mut request = appender_request(&directory, journal(1), 1);
        request.replica = Some(DeferredReplicaRecovery {
            path: path(&directory),
            copied_through_sequence: 1,
        });
        assert!(matches!(
            backend.prepare_recovered_journal(&request, &None),
            Err(NativeFilesystemError::UnsupportedJournalLocation)
        ));
        assert!(matches!(
            backend.prepare_recovered_journal(
                &appender_request(&outside_directory, journal(1), 1),
                &None,
            ),
            Err(NativeFilesystemError::Capability(
                ariax_storage::NativeCapabilityError::OutsideAllowedRoot
            ))
        ));
    }

    #[test]
    fn invalid_installing_candidate_is_cleaned_before_old_set_fallback() {
        let control = TestDirectory::new("install-reject");
        let old_directory = create_live_journal(&control, "old", journal(1));
        let candidate_directory = create_live_journal(&control, "candidate", journal(2));
        let candidate_segment = ariax_storage::journal_segment_path(&candidate_directory, 0);
        let request = DeferredJournalInstallRecovery {
            task_id: task_id(),
            intent: JournalInstallIntent {
                gid: gid(),
                checkpoint_id: CheckpointId::new([3; 16]).expect("checkpoint id"),
                old_journal_id: journal(1),
                old_path: path(&old_directory),
                new_journal_id: journal(2),
                new_path: path(&candidate_directory),
                source_last_sequence: 1,
                phase: JournalInstallPhase::Installing,
                created_ms: 100,
            },
            authoritative_journal_id: journal(1),
            authoritative_last_sequence: 1,
            authoritative_checkpoint: None,
        };
        let outcome = backend(&control)
            .recover_journal_install(&request, &None)
            .expect("reject candidate and prepare old set");
        assert!(matches!(
            outcome,
            NativeInstallOutcome::RejectedCandidate(ref prepared)
                if prepared.replay().last_sequence == 1
        ));
        assert!(!candidate_segment.exists());
    }

    #[test]
    fn valid_checkpoint_is_accepted_in_installing_and_installed_phases() {
        let control = TestDirectory::new("install-accept");
        let old_directory = create_live_journal(&control, "old", journal(1));
        let checkpoint_id = CheckpointId::new([4; 16]).expect("checkpoint id");
        let (candidate_directory, recovered_checkpoint) =
            create_checkpoint_journal(&control, "candidate", journal(2), checkpoint_id, 1);
        let intent = JournalInstallIntent {
            gid: gid(),
            checkpoint_id,
            old_journal_id: journal(1),
            old_path: path(&old_directory),
            new_journal_id: journal(2),
            new_path: path(&candidate_directory),
            source_last_sequence: 1,
            phase: JournalInstallPhase::Installing,
            created_ms: 100,
        };
        let installing = DeferredJournalInstallRecovery {
            task_id: task_id(),
            intent: intent.clone(),
            authoritative_journal_id: journal(1),
            authoritative_last_sequence: 1,
            authoritative_checkpoint: None,
        };
        let outcome = backend(&control)
            .recover_journal_install(&installing, &None)
            .expect("accept installing checkpoint");
        assert!(matches!(
            outcome,
            NativeInstallOutcome::AcceptedCandidate(ref prepared)
                if prepared.replay().last_sequence == 4
        ));
        drop(outcome);

        let installed = DeferredJournalInstallRecovery {
            task_id: task_id(),
            intent: JournalInstallIntent {
                phase: JournalInstallPhase::Installed,
                ..intent
            },
            authoritative_journal_id: journal(2),
            authoritative_last_sequence: 4,
            authoritative_checkpoint: Some(recovered_checkpoint),
        };
        assert!(matches!(
            backend(&control).recover_journal_install(&installed, &None),
            Ok(NativeInstallOutcome::ValidatedInstalled(ref prepared))
                if prepared.replay().last_sequence == 4
        ));
    }
}
