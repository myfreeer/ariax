use crate::command::{NoSpaceProbeOrigin, SchedulerCommandKind, TaskEventKind};
use crate::{Generation, Gid, MonotonicInstant, TaskConditionsSnapshot, TaskId, TaskState};
use std::error::Error;
use std::fmt;

/// One immutable state change published by the scheduler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateTransition {
    pub task: TaskId,
    pub gid: Gid,
    pub generation: Generation,
    pub from: TaskState,
    pub to: TaskState,
    pub reason: StateReason,
    pub at: MonotonicInstant,
}

/// Removal of a retained task after its persisted metadata deletion is
/// acknowledged. A deletion has no destination `TaskState`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskDeletion {
    pub task: TaskId,
    pub gid: Gid,
    pub generation: Generation,
    pub from: TaskState,
    pub reason: StateReason,
    pub at: MonotonicInstant,
}

/// Why the scheduler accepted, rejected, or ignored one semantic action.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum StateReason {
    ValidationSucceeded,
    ValidationFailed,
    UserPause,
    UserResume,
    UserRemove,
    SchedulerAdmission,
    AdmissionBlocked,
    PlanningFailed,
    OptionPatch,
    OptionPatchApplicationFailed,
    CredentialsSatisfied,
    CredentialUpdateFailed,
    NoSpaceProbeRequested,
    NoSpaceProbeSucceeded,
    NoSpaceProbeFailed,
    GenerationPersistenceSucceeded,
    AllocationSucceeded,
    AllocationRetryableFailure,
    HostKeyChallenge,
    HostKeyApprovalRequired,
    AllocationTerminalFailure,
    LeaseRetry,
    RepresentationRestart,
    PayloadReceived,
    BitTorrentSeeding,
    SlowSlotDemotion,
    SlowSlotPause,
    NoSpace,
    TerminalWorkFailure,
    RetryReadmission,
    RetryExhausted,
    SlowReadmission,
    HostKeyApproved,
    HostKeyResolutionFailed,
    OptionPatchPersistenceSucceeded,
    OptionPatchPersistenceFailed,
    CancellationDrained,
    RestartQuiesced,
    SourceReplacement,
    RestartApplicationSucceeded,
    RestartApplicationFailed,
    VerificationSucceeded,
    VerificationRecoverableFailure,
    VerificationTerminalFailure,
    SeedingStopped,
    BitTorrentFailure,
    TerminalPersistenceSucceeded,
    StoppedResultRemovalRequested,
    StoppedResultRemovalFailed,
    StoppedResultRemoved,
    OrderlyShutdown,
    DuplicateEventIgnored,
    StaleEventIgnored,
    Idempotent,
    Conflict,
}

/// Every state-transition reason in canonical contract order.
pub const ALL_STATE_REASONS: &[StateReason] = &[
    StateReason::ValidationSucceeded,
    StateReason::ValidationFailed,
    StateReason::UserPause,
    StateReason::UserResume,
    StateReason::UserRemove,
    StateReason::SchedulerAdmission,
    StateReason::AdmissionBlocked,
    StateReason::PlanningFailed,
    StateReason::OptionPatch,
    StateReason::OptionPatchApplicationFailed,
    StateReason::CredentialsSatisfied,
    StateReason::CredentialUpdateFailed,
    StateReason::NoSpaceProbeRequested,
    StateReason::NoSpaceProbeSucceeded,
    StateReason::NoSpaceProbeFailed,
    StateReason::GenerationPersistenceSucceeded,
    StateReason::AllocationSucceeded,
    StateReason::AllocationRetryableFailure,
    StateReason::HostKeyChallenge,
    StateReason::HostKeyApprovalRequired,
    StateReason::AllocationTerminalFailure,
    StateReason::LeaseRetry,
    StateReason::RepresentationRestart,
    StateReason::PayloadReceived,
    StateReason::BitTorrentSeeding,
    StateReason::SlowSlotDemotion,
    StateReason::SlowSlotPause,
    StateReason::NoSpace,
    StateReason::TerminalWorkFailure,
    StateReason::RetryReadmission,
    StateReason::RetryExhausted,
    StateReason::SlowReadmission,
    StateReason::HostKeyApproved,
    StateReason::HostKeyResolutionFailed,
    StateReason::OptionPatchPersistenceSucceeded,
    StateReason::OptionPatchPersistenceFailed,
    StateReason::CancellationDrained,
    StateReason::RestartQuiesced,
    StateReason::SourceReplacement,
    StateReason::RestartApplicationSucceeded,
    StateReason::RestartApplicationFailed,
    StateReason::VerificationSucceeded,
    StateReason::VerificationRecoverableFailure,
    StateReason::VerificationTerminalFailure,
    StateReason::SeedingStopped,
    StateReason::BitTorrentFailure,
    StateReason::TerminalPersistenceSucceeded,
    StateReason::StoppedResultRemovalRequested,
    StateReason::StoppedResultRemovalFailed,
    StateReason::StoppedResultRemoved,
    StateReason::OrderlyShutdown,
    StateReason::DuplicateEventIgnored,
    StateReason::StaleEventIgnored,
    StateReason::Idempotent,
    StateReason::Conflict,
];

impl StateReason {
    /// Returns the stable diagnostic and generated-matrix code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::ValidationSucceeded => "validation_succeeded",
            Self::ValidationFailed => "validation_failed",
            Self::UserPause => "user_pause",
            Self::UserResume => "user_resume",
            Self::UserRemove => "user_remove",
            Self::SchedulerAdmission => "scheduler_admission",
            Self::AdmissionBlocked => "admission_blocked",
            Self::PlanningFailed => "planning_failed",
            Self::OptionPatch => "option_patch",
            Self::OptionPatchApplicationFailed => "option_patch_application_failed",
            Self::CredentialsSatisfied => "credentials_satisfied",
            Self::CredentialUpdateFailed => "credential_update_failed",
            Self::NoSpaceProbeRequested => "no_space_probe_requested",
            Self::NoSpaceProbeSucceeded => "no_space_probe_succeeded",
            Self::NoSpaceProbeFailed => "no_space_probe_failed",
            Self::GenerationPersistenceSucceeded => "generation_persistence_succeeded",
            Self::AllocationSucceeded => "allocation_succeeded",
            Self::AllocationRetryableFailure => "allocation_retryable_failure",
            Self::HostKeyChallenge => "host_key_challenge",
            Self::HostKeyApprovalRequired => "host_key_approval_required",
            Self::AllocationTerminalFailure => "allocation_terminal_failure",
            Self::LeaseRetry => "lease_retry",
            Self::RepresentationRestart => "representation_restart",
            Self::PayloadReceived => "payload_received",
            Self::BitTorrentSeeding => "bit_torrent_seeding",
            Self::SlowSlotDemotion => "slow_slot_demotion",
            Self::SlowSlotPause => "slow_slot_pause",
            Self::NoSpace => "no_space",
            Self::TerminalWorkFailure => "terminal_work_failure",
            Self::RetryReadmission => "retry_readmission",
            Self::RetryExhausted => "retry_exhausted",
            Self::SlowReadmission => "slow_readmission",
            Self::HostKeyApproved => "host_key_approved",
            Self::HostKeyResolutionFailed => "host_key_resolution_failed",
            Self::OptionPatchPersistenceSucceeded => "option_patch_persistence_succeeded",
            Self::OptionPatchPersistenceFailed => "option_patch_persistence_failed",
            Self::CancellationDrained => "cancellation_drained",
            Self::RestartQuiesced => "restart_quiesced",
            Self::SourceReplacement => "source_replacement",
            Self::RestartApplicationSucceeded => "restart_application_succeeded",
            Self::RestartApplicationFailed => "restart_application_failed",
            Self::VerificationSucceeded => "verification_succeeded",
            Self::VerificationRecoverableFailure => "verification_recoverable_failure",
            Self::VerificationTerminalFailure => "verification_terminal_failure",
            Self::SeedingStopped => "seeding_stopped",
            Self::BitTorrentFailure => "bit_torrent_failure",
            Self::TerminalPersistenceSucceeded => "terminal_persistence_succeeded",
            Self::StoppedResultRemovalRequested => "stopped_result_removal_requested",
            Self::StoppedResultRemovalFailed => "stopped_result_removal_failed",
            Self::StoppedResultRemoved => "stopped_result_removed",
            Self::OrderlyShutdown => "orderly_shutdown",
            Self::DuplicateEventIgnored => "duplicate_event_ignored",
            Self::StaleEventIgnored => "stale_event_ignored",
            Self::Idempotent => "idempotent",
            Self::Conflict => "conflict",
        }
    }
}

/// A validated command or subsystem result presented to the task state model.
///
/// Conditional inputs are split into explicit actions so the state/action
/// matrix remains total without consulting mutable scheduler context.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SchedulerAction {
    ValidationSucceeded,
    ValidationSucceededPaused,
    ValidationFailed,
    Pause,
    PauseDeferred,
    Resume,
    ResumeDeferred,
    Remove,
    RemoveDeferred,
    SchedulerAdmission,
    AdmissionBlocked,
    PlanningFailed,
    InPlaceOptionPatchAccepted,
    InPlaceOptionPatchApplied,
    InPlaceOptionPatchApplicationFailed,
    CredentialsSatisfied,
    CredentialSatisfactionFailed,
    ExplicitNoSpaceProbeRequested,
    WaitingNoSpaceProbeSucceeded,
    WaitingNoSpaceProbeFailed,
    PausedNoSpaceResumeProbeSucceeded,
    PausedNoSpaceResumeProbeFailed,
    PausedNoSpacePausePreservingProbeSucceeded,
    PausedNoSpacePausePreservingProbeFailed,
    GenerationPersistenceSucceeded,
    AllocationSucceeded,
    AllocationRetryableFailure,
    ActiveHostKeyChallengeRequired,
    HostKeyChallengeRequired,
    AllocationTerminalFailure,
    ActiveRestartOptionPatchAccepted,
    SourceReplacementRequested,
    SourceReplacementCommitted,
    ActiveRestartOptionPatchPersisted,
    ActiveRestartOptionPatchPersistenceFailed,
    LeaseRetryableWithRunnableWork,
    LeaseRetryableWithoutRunnableWork,
    RepresentationRestart,
    AllRequiredDataReceived,
    BitTorrentPayloadComplete,
    SlowSlotDemote,
    SlowSlotPause,
    MidTransferNoSpace,
    TerminalWorkFailure,
    RetryReadmissionSucceeded,
    RetryReadmissionBlocked,
    RetryExhausted,
    SlowReadmissionSucceeded,
    SlowReadmissionBlocked,
    ApproveHostKey,
    ApplyMatchingHostKeyOption,
    HostKeyResolutionSucceeded,
    HostKeyResolutionSucceededPreservingPause,
    HostKeyResolutionFailed,
    CancellationDrainSucceeded,
    RestartQuiesced,
    RestartApplicationScheduled,
    RestartApplicationSucceeded,
    RestartApplicationFailed,
    DeferredPauseCompleted,
    DeferredResumeCompleted,
    DeferredRemoveCompleted,
    VerificationSucceeded,
    VerificationRecoverableFailure,
    VerificationTerminalFailure,
    SeedingStopped,
    BitTorrentFailure,
    TerminalPersistenceSucceeded,
    RemoveStoppedResult,
    StoppedResultDeletionSucceeded,
    StoppedResultDeletionFailed,
    OrderlyShutdown,
    DuplicateEventIgnored,
    StaleEventIgnored,
}

/// Every scheduler action in canonical matrix order.
pub const ALL_SCHEDULER_ACTIONS: &[SchedulerAction] = &[
    SchedulerAction::ValidationSucceeded,
    SchedulerAction::ValidationSucceededPaused,
    SchedulerAction::ValidationFailed,
    SchedulerAction::Pause,
    SchedulerAction::PauseDeferred,
    SchedulerAction::Resume,
    SchedulerAction::ResumeDeferred,
    SchedulerAction::Remove,
    SchedulerAction::RemoveDeferred,
    SchedulerAction::SchedulerAdmission,
    SchedulerAction::AdmissionBlocked,
    SchedulerAction::PlanningFailed,
    SchedulerAction::InPlaceOptionPatchAccepted,
    SchedulerAction::InPlaceOptionPatchApplied,
    SchedulerAction::InPlaceOptionPatchApplicationFailed,
    SchedulerAction::CredentialsSatisfied,
    SchedulerAction::CredentialSatisfactionFailed,
    SchedulerAction::ExplicitNoSpaceProbeRequested,
    SchedulerAction::WaitingNoSpaceProbeSucceeded,
    SchedulerAction::WaitingNoSpaceProbeFailed,
    SchedulerAction::PausedNoSpaceResumeProbeSucceeded,
    SchedulerAction::PausedNoSpaceResumeProbeFailed,
    SchedulerAction::PausedNoSpacePausePreservingProbeSucceeded,
    SchedulerAction::PausedNoSpacePausePreservingProbeFailed,
    SchedulerAction::GenerationPersistenceSucceeded,
    SchedulerAction::AllocationSucceeded,
    SchedulerAction::AllocationRetryableFailure,
    SchedulerAction::ActiveHostKeyChallengeRequired,
    SchedulerAction::HostKeyChallengeRequired,
    SchedulerAction::AllocationTerminalFailure,
    SchedulerAction::ActiveRestartOptionPatchAccepted,
    SchedulerAction::SourceReplacementRequested,
    SchedulerAction::SourceReplacementCommitted,
    SchedulerAction::ActiveRestartOptionPatchPersisted,
    SchedulerAction::ActiveRestartOptionPatchPersistenceFailed,
    SchedulerAction::LeaseRetryableWithRunnableWork,
    SchedulerAction::LeaseRetryableWithoutRunnableWork,
    SchedulerAction::RepresentationRestart,
    SchedulerAction::AllRequiredDataReceived,
    SchedulerAction::BitTorrentPayloadComplete,
    SchedulerAction::SlowSlotDemote,
    SchedulerAction::SlowSlotPause,
    SchedulerAction::MidTransferNoSpace,
    SchedulerAction::TerminalWorkFailure,
    SchedulerAction::RetryReadmissionSucceeded,
    SchedulerAction::RetryReadmissionBlocked,
    SchedulerAction::RetryExhausted,
    SchedulerAction::SlowReadmissionSucceeded,
    SchedulerAction::SlowReadmissionBlocked,
    SchedulerAction::ApproveHostKey,
    SchedulerAction::ApplyMatchingHostKeyOption,
    SchedulerAction::HostKeyResolutionSucceeded,
    SchedulerAction::HostKeyResolutionSucceededPreservingPause,
    SchedulerAction::HostKeyResolutionFailed,
    SchedulerAction::CancellationDrainSucceeded,
    SchedulerAction::RestartQuiesced,
    SchedulerAction::RestartApplicationScheduled,
    SchedulerAction::RestartApplicationSucceeded,
    SchedulerAction::RestartApplicationFailed,
    SchedulerAction::DeferredPauseCompleted,
    SchedulerAction::DeferredResumeCompleted,
    SchedulerAction::DeferredRemoveCompleted,
    SchedulerAction::VerificationSucceeded,
    SchedulerAction::VerificationRecoverableFailure,
    SchedulerAction::VerificationTerminalFailure,
    SchedulerAction::SeedingStopped,
    SchedulerAction::BitTorrentFailure,
    SchedulerAction::TerminalPersistenceSucceeded,
    SchedulerAction::RemoveStoppedResult,
    SchedulerAction::StoppedResultDeletionSucceeded,
    SchedulerAction::StoppedResultDeletionFailed,
    SchedulerAction::OrderlyShutdown,
    SchedulerAction::DuplicateEventIgnored,
    SchedulerAction::StaleEventIgnored,
];

impl SchedulerAction {
    /// Returns the stable generated-matrix code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::ValidationSucceeded => "validation_succeeded",
            Self::ValidationSucceededPaused => "validation_succeeded_paused",
            Self::ValidationFailed => "validation_failed",
            Self::Pause => "pause",
            Self::PauseDeferred => "pause_deferred",
            Self::Resume => "resume",
            Self::ResumeDeferred => "resume_deferred",
            Self::Remove => "remove",
            Self::RemoveDeferred => "remove_deferred",
            Self::SchedulerAdmission => "scheduler_admission",
            Self::AdmissionBlocked => "admission_blocked",
            Self::PlanningFailed => "planning_failed",
            Self::InPlaceOptionPatchAccepted => "in_place_option_patch_accepted",
            Self::InPlaceOptionPatchApplied => "in_place_option_patch_applied",
            Self::InPlaceOptionPatchApplicationFailed => "in_place_option_patch_application_failed",
            Self::CredentialsSatisfied => "credentials_satisfied",
            Self::CredentialSatisfactionFailed => "credential_satisfaction_failed",
            Self::ExplicitNoSpaceProbeRequested => "explicit_no_space_probe_requested",
            Self::WaitingNoSpaceProbeSucceeded => "waiting_no_space_probe_succeeded",
            Self::WaitingNoSpaceProbeFailed => "waiting_no_space_probe_failed",
            Self::PausedNoSpaceResumeProbeSucceeded => "paused_no_space_resume_probe_succeeded",
            Self::PausedNoSpaceResumeProbeFailed => "paused_no_space_resume_probe_failed",
            Self::PausedNoSpacePausePreservingProbeSucceeded => {
                "paused_no_space_pause_preserving_probe_succeeded"
            }
            Self::PausedNoSpacePausePreservingProbeFailed => {
                "paused_no_space_pause_preserving_probe_failed"
            }
            Self::GenerationPersistenceSucceeded => "generation_persistence_succeeded",
            Self::AllocationSucceeded => "allocation_succeeded",
            Self::AllocationRetryableFailure => "allocation_retryable_failure",
            Self::ActiveHostKeyChallengeRequired => "active_host_key_challenge_required",
            Self::HostKeyChallengeRequired => "host_key_challenge_required",
            Self::AllocationTerminalFailure => "allocation_terminal_failure",
            Self::ActiveRestartOptionPatchAccepted => "active_restart_option_patch_accepted",
            Self::SourceReplacementRequested => "source_replacement_requested",
            Self::SourceReplacementCommitted => "source_replacement_committed",
            Self::ActiveRestartOptionPatchPersisted => "active_restart_option_patch_persisted",
            Self::ActiveRestartOptionPatchPersistenceFailed => {
                "active_restart_option_patch_persistence_failed"
            }
            Self::LeaseRetryableWithRunnableWork => "lease_retryable_with_runnable_work",
            Self::LeaseRetryableWithoutRunnableWork => "lease_retryable_without_runnable_work",
            Self::RepresentationRestart => "representation_restart",
            Self::AllRequiredDataReceived => "all_required_data_received",
            Self::BitTorrentPayloadComplete => "bit_torrent_payload_complete",
            Self::SlowSlotDemote => "slow_slot_demote",
            Self::SlowSlotPause => "slow_slot_pause",
            Self::MidTransferNoSpace => "mid_transfer_no_space",
            Self::TerminalWorkFailure => "terminal_work_failure",
            Self::RetryReadmissionSucceeded => "retry_readmission_succeeded",
            Self::RetryReadmissionBlocked => "retry_readmission_blocked",
            Self::RetryExhausted => "retry_exhausted",
            Self::SlowReadmissionSucceeded => "slow_readmission_succeeded",
            Self::SlowReadmissionBlocked => "slow_readmission_blocked",
            Self::ApproveHostKey => "approve_host_key",
            Self::ApplyMatchingHostKeyOption => "apply_matching_host_key_option",
            Self::HostKeyResolutionSucceeded => "host_key_resolution_succeeded",
            Self::HostKeyResolutionSucceededPreservingPause => {
                "host_key_resolution_succeeded_preserving_pause"
            }
            Self::HostKeyResolutionFailed => "host_key_resolution_failed",
            Self::CancellationDrainSucceeded => "cancellation_drain_succeeded",
            Self::RestartQuiesced => "restart_quiesced",
            Self::RestartApplicationScheduled => "restart_application_scheduled",
            Self::RestartApplicationSucceeded => "restart_application_succeeded",
            Self::RestartApplicationFailed => "restart_application_failed",
            Self::DeferredPauseCompleted => "deferred_pause_completed",
            Self::DeferredResumeCompleted => "deferred_resume_completed",
            Self::DeferredRemoveCompleted => "deferred_remove_completed",
            Self::VerificationSucceeded => "verification_succeeded",
            Self::VerificationRecoverableFailure => "verification_recoverable_failure",
            Self::VerificationTerminalFailure => "verification_terminal_failure",
            Self::SeedingStopped => "seeding_stopped",
            Self::BitTorrentFailure => "bit_torrent_failure",
            Self::TerminalPersistenceSucceeded => "terminal_persistence_succeeded",
            Self::RemoveStoppedResult => "remove_stopped_result",
            Self::StoppedResultDeletionSucceeded => "stopped_result_deletion_succeeded",
            Self::StoppedResultDeletionFailed => "stopped_result_deletion_failed",
            Self::OrderlyShutdown => "orderly_shutdown",
            Self::DuplicateEventIgnored => "duplicate_event_ignored",
            Self::StaleEventIgnored => "stale_event_ignored",
        }
    }
}

/// The validated source category for one semantic state-machine action.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SchedulerActionSource {
    Command(SchedulerCommandKind),
    TaskEvent(TaskEventKind),
    AnyTaskEvent,
    Internal,
}

impl SchedulerActionSource {
    #[must_use]
    pub const fn kind_code(self) -> &'static str {
        match self {
            Self::Command(_) => "command",
            Self::TaskEvent(_) => "task_event",
            Self::AnyTaskEvent => "any_task_event",
            Self::Internal => "internal",
        }
    }

    #[must_use]
    pub const fn input_code(self) -> Option<&'static str> {
        match self {
            Self::Command(kind) => Some(kind.code()),
            Self::TaskEvent(kind) => Some(kind.code()),
            Self::AnyTaskEvent | Self::Internal => None,
        }
    }

    #[must_use]
    pub fn matches_command(self, command: SchedulerCommandKind) -> bool {
        matches!(self, Self::Command(actual) if actual == command)
    }

    #[must_use]
    pub fn matches_task_event(self, event: TaskEventKind) -> bool {
        matches!(self, Self::TaskEvent(actual) if actual == event)
    }
}

impl SchedulerAction {
    /// Returns the payload-bearing command or event normalized into this action.
    #[must_use]
    pub const fn source(self) -> SchedulerActionSource {
        match self {
            Self::ValidationSucceeded | Self::ValidationSucceededPaused => {
                SchedulerActionSource::Command(SchedulerCommandKind::AddValidatedTask)
            }
            Self::Pause | Self::PauseDeferred => {
                SchedulerActionSource::Command(SchedulerCommandKind::Pause)
            }
            Self::Resume | Self::ResumeDeferred | Self::ExplicitNoSpaceProbeRequested => {
                SchedulerActionSource::Command(SchedulerCommandKind::Resume)
            }
            Self::Remove | Self::RemoveDeferred => {
                SchedulerActionSource::Command(SchedulerCommandKind::Remove)
            }
            Self::InPlaceOptionPatchAccepted
            | Self::ActiveRestartOptionPatchAccepted
            | Self::ApplyMatchingHostKeyOption => {
                SchedulerActionSource::Command(SchedulerCommandKind::ApplyOptionPatch)
            }
            Self::SourceReplacementRequested => {
                SchedulerActionSource::Command(SchedulerCommandKind::BeginSourceReplacement)
            }
            Self::SourceReplacementCommitted => {
                SchedulerActionSource::Command(SchedulerCommandKind::CommitSourceReplacement)
            }
            Self::InPlaceOptionPatchApplied => {
                SchedulerActionSource::TaskEvent(TaskEventKind::OptionPatchApplied)
            }
            Self::InPlaceOptionPatchApplicationFailed => {
                SchedulerActionSource::TaskEvent(TaskEventKind::OptionPatchApplicationFailed)
            }
            Self::CredentialsSatisfied => {
                SchedulerActionSource::TaskEvent(TaskEventKind::OptionPatchApplied)
            }
            Self::CredentialSatisfactionFailed => {
                SchedulerActionSource::TaskEvent(TaskEventKind::OptionPatchApplicationFailed)
            }
            Self::ApproveHostKey => {
                SchedulerActionSource::Command(SchedulerCommandKind::ApproveHostKey)
            }
            Self::RemoveStoppedResult => {
                SchedulerActionSource::Command(SchedulerCommandKind::RemoveStoppedResult)
            }
            Self::OrderlyShutdown => {
                SchedulerActionSource::Command(SchedulerCommandKind::OrderlyShutdown)
            }
            Self::WaitingNoSpaceProbeSucceeded
            | Self::WaitingNoSpaceProbeFailed
            | Self::PausedNoSpaceResumeProbeSucceeded
            | Self::PausedNoSpaceResumeProbeFailed
            | Self::PausedNoSpacePausePreservingProbeSucceeded
            | Self::PausedNoSpacePausePreservingProbeFailed => {
                SchedulerActionSource::TaskEvent(TaskEventKind::NoSpaceProbeCompleted)
            }
            Self::GenerationPersistenceSucceeded => {
                SchedulerActionSource::TaskEvent(TaskEventKind::GenerationPersisted)
            }
            Self::ActiveRestartOptionPatchPersisted => {
                SchedulerActionSource::TaskEvent(TaskEventKind::OptionPatchPersisted)
            }
            Self::ActiveRestartOptionPatchPersistenceFailed => {
                SchedulerActionSource::TaskEvent(TaskEventKind::OptionPatchPersistenceFailed)
            }
            Self::RestartApplicationSucceeded => {
                SchedulerActionSource::TaskEvent(TaskEventKind::OptionPatchApplied)
            }
            Self::RestartApplicationFailed => {
                SchedulerActionSource::TaskEvent(TaskEventKind::OptionPatchApplicationFailed)
            }
            Self::AllocationSucceeded => {
                SchedulerActionSource::TaskEvent(TaskEventKind::AllocationSucceeded)
            }
            Self::AllocationRetryableFailure => {
                SchedulerActionSource::TaskEvent(TaskEventKind::AllocationRetryable)
            }
            Self::ActiveHostKeyChallengeRequired => {
                SchedulerActionSource::TaskEvent(TaskEventKind::ActiveHostKeyChallenge)
            }
            Self::HostKeyChallengeRequired => {
                SchedulerActionSource::TaskEvent(TaskEventKind::AllocationHostKeyChallenge)
            }
            Self::AllocationTerminalFailure => {
                SchedulerActionSource::TaskEvent(TaskEventKind::AllocationFailed)
            }
            Self::LeaseRetryableWithoutRunnableWork => {
                SchedulerActionSource::TaskEvent(TaskEventKind::ActiveRetryIdle)
            }
            Self::RepresentationRestart => {
                SchedulerActionSource::TaskEvent(TaskEventKind::ActiveRepresentationRestart)
            }
            Self::AllRequiredDataReceived => {
                SchedulerActionSource::TaskEvent(TaskEventKind::DataComplete)
            }
            Self::BitTorrentPayloadComplete => {
                SchedulerActionSource::TaskEvent(TaskEventKind::BitTorrentPayloadComplete)
            }
            Self::SlowSlotDemote => SchedulerActionSource::TaskEvent(TaskEventKind::SlowDemoted),
            Self::SlowSlotPause => SchedulerActionSource::TaskEvent(TaskEventKind::SlowPaused),
            Self::MidTransferNoSpace => SchedulerActionSource::TaskEvent(TaskEventKind::NoSpace),
            Self::TerminalWorkFailure => {
                SchedulerActionSource::TaskEvent(TaskEventKind::TerminalFailure)
            }
            Self::RetryReadmissionSucceeded | Self::RetryReadmissionBlocked => {
                SchedulerActionSource::TaskEvent(TaskEventKind::RetryReady)
            }
            Self::SlowReadmissionSucceeded | Self::SlowReadmissionBlocked => {
                SchedulerActionSource::TaskEvent(TaskEventKind::SlowReadmit)
            }
            Self::CancellationDrainSucceeded | Self::RestartQuiesced => {
                SchedulerActionSource::TaskEvent(TaskEventKind::CancellationDrained)
            }
            Self::VerificationSucceeded => {
                SchedulerActionSource::TaskEvent(TaskEventKind::VerificationSucceeded)
            }
            Self::VerificationRecoverableFailure => {
                SchedulerActionSource::TaskEvent(TaskEventKind::VerificationRecoverable)
            }
            Self::VerificationTerminalFailure => {
                SchedulerActionSource::TaskEvent(TaskEventKind::VerificationFailed)
            }
            Self::SeedingStopped => {
                SchedulerActionSource::TaskEvent(TaskEventKind::SeedingComplete)
            }
            Self::BitTorrentFailure => {
                SchedulerActionSource::TaskEvent(TaskEventKind::SeedingFailed)
            }
            Self::TerminalPersistenceSucceeded => {
                SchedulerActionSource::TaskEvent(TaskEventKind::TerminalPersisted)
            }
            Self::HostKeyResolutionSucceeded | Self::HostKeyResolutionSucceededPreservingPause => {
                SchedulerActionSource::TaskEvent(TaskEventKind::HostKeyResolutionPersisted)
            }
            Self::HostKeyResolutionFailed => {
                SchedulerActionSource::TaskEvent(TaskEventKind::HostKeyResolutionFailed)
            }
            Self::StoppedResultDeletionSucceeded => {
                SchedulerActionSource::TaskEvent(TaskEventKind::StoppedResultDeleted)
            }
            Self::StoppedResultDeletionFailed => {
                SchedulerActionSource::TaskEvent(TaskEventKind::StoppedResultDeletionFailed)
            }
            Self::DuplicateEventIgnored | Self::StaleEventIgnored => {
                SchedulerActionSource::AnyTaskEvent
            }
            Self::ValidationFailed
            | Self::SchedulerAdmission
            | Self::AdmissionBlocked
            | Self::PlanningFailed
            | Self::LeaseRetryableWithRunnableWork
            | Self::RetryExhausted
            | Self::RestartApplicationScheduled
            | Self::DeferredPauseCompleted
            | Self::DeferredResumeCompleted
            | Self::DeferredRemoveCompleted => SchedulerActionSource::Internal,
        }
    }

    /// Selects the validation-success row from the validated add payload.
    #[must_use]
    pub const fn for_validated_add(desired_paused: bool) -> Self {
        if desired_paused {
            Self::ValidationSucceededPaused
        } else {
            Self::ValidationSucceeded
        }
    }

    /// Selects generic resume or the required explicit no-space probe request.
    #[must_use]
    pub const fn for_resume(state: TaskState, conditions: TaskConditionsSnapshot) -> Self {
        if conditions.no_space
            && matches!(
                state,
                TaskState::Waiting
                    | TaskState::WaitingSlow
                    | TaskState::RetryWait
                    | TaskState::Paused
                    | TaskState::PausedSlow
            )
        {
            Self::ExplicitNoSpaceProbeRequested
        } else {
            Self::Resume
        }
    }

    /// Selects the host-key resolution result after durable persistence.
    #[must_use]
    pub const fn for_host_key_resolution(desired_paused: bool) -> Self {
        if desired_paused {
            Self::HostKeyResolutionSucceededPreservingPause
        } else {
            Self::HostKeyResolutionSucceeded
        }
    }

    /// Selects the state-, current-pause-intent-, and result-specific no-space row.
    #[must_use]
    pub const fn for_no_space_probe_result(
        state: TaskState,
        origin: NoSpaceProbeOrigin,
        desired_paused: bool,
        ready: bool,
    ) -> Option<Self> {
        match (state, origin, desired_paused, ready) {
            (TaskState::Waiting | TaskState::WaitingSlow | TaskState::RetryWait, _, _, true) => {
                Some(Self::WaitingNoSpaceProbeSucceeded)
            }
            (TaskState::Waiting | TaskState::WaitingSlow | TaskState::RetryWait, _, _, false) => {
                Some(Self::WaitingNoSpaceProbeFailed)
            }
            (TaskState::Paused, _, false, true) => Some(Self::PausedNoSpaceResumeProbeSucceeded),
            (TaskState::Paused, _, false, false) => Some(Self::PausedNoSpaceResumeProbeFailed),
            (TaskState::Paused | TaskState::PausedSlow, _, true, true) => {
                Some(Self::PausedNoSpacePausePreservingProbeSucceeded)
            }
            (TaskState::Paused | TaskState::PausedSlow, _, true, false) => {
                Some(Self::PausedNoSpacePausePreservingProbeFailed)
            }
            _ => None,
        }
    }
}

/// A fresh asynchronous result was paired with an action from the wrong source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerActionSourceError {
    EventActionMismatch {
        event: TaskEventKind,
        action: SchedulerAction,
    },
}

impl fmt::Display for SchedulerActionSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EventActionMismatch { event, action } => write!(
                formatter,
                "task event {} cannot produce scheduler action {}",
                event.code(),
                action.code()
            ),
        }
    }
}

impl Error for SchedulerActionSourceError {}

/// Identity disposition computed before a task event enters the state matrix.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum EventDisposition {
    Fresh,
    Duplicate,
    Stale,
}

pub const ALL_EVENT_DISPOSITIONS: &[EventDisposition] = &[
    EventDisposition::Fresh,
    EventDisposition::Duplicate,
    EventDisposition::Stale,
];

impl EventDisposition {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Duplicate => "duplicate",
            Self::Stale => "stale",
        }
    }

    /// Validates the fresh event/action pairing, then applies identity disposition.
    pub fn resolve(
        self,
        event: TaskEventKind,
        fresh: SchedulerAction,
    ) -> Result<SchedulerAction, SchedulerActionSourceError> {
        if !fresh.source().matches_task_event(event) {
            return Err(SchedulerActionSourceError::EventActionMismatch {
                event,
                action: fresh,
            });
        }
        Ok(match self {
            Self::Fresh => fresh,
            Self::Duplicate => SchedulerAction::DuplicateEventIgnored,
            Self::Stale => SchedulerAction::StaleEventIgnored,
        })
    }
}

/// How a scheduler must interpret one state/action matrix cell.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TransitionContractKind {
    /// The action deterministically enters the sole target state.
    Transition,
    /// The action is accepted and mutates orthogonal state while remaining in place.
    Stay,
    /// Recovery context selects one of several permitted target states.
    Conditional,
    /// The retained stopped result is deleted, leaving no task state.
    Delete,
    /// The requested postcondition already holds and no effect is required.
    NoOp,
    /// A validated duplicate or stale asynchronous event is ignored without effects.
    Ignore,
    /// The action is invalid in the current state and must have no side effects.
    Conflict,
}

/// Every transition-contract kind in stable code order.
pub const ALL_TRANSITION_CONTRACT_KINDS: &[TransitionContractKind] = &[
    TransitionContractKind::Transition,
    TransitionContractKind::Stay,
    TransitionContractKind::Conditional,
    TransitionContractKind::Delete,
    TransitionContractKind::NoOp,
    TransitionContractKind::Ignore,
    TransitionContractKind::Conflict,
];

impl TransitionContractKind {
    /// Returns the stable generated-matrix code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Transition => "transition",
            Self::Stay => "stay",
            Self::Conditional => "conditional",
            Self::Delete => "delete",
            Self::NoOp => "no_op",
            Self::Ignore => "ignore",
            Self::Conflict => "conflict",
        }
    }

    /// Returns whether this cell accepts the action.
    #[must_use]
    pub const fn accepts(self) -> bool {
        !matches!(self, Self::Conflict)
    }
}

/// Typed failure returned for a rejected state/action cell.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TransitionRejection {
    Conflict,
    HostKeyApprovalRequired,
}

pub const ALL_TRANSITION_REJECTIONS: &[TransitionRejection] = &[
    TransitionRejection::Conflict,
    TransitionRejection::HostKeyApprovalRequired,
];

impl TransitionRejection {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Conflict => "conflict",
            Self::HostKeyApprovalRequired => "host_key_approval_required",
        }
    }
}

/// One executable cell in the complete task-state transition matrix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransitionContract {
    kind: TransitionContractKind,
    reason: StateReason,
    targets: &'static [TaskState],
    rejection: Option<TransitionRejection>,
}

impl TransitionContract {
    #[must_use]
    pub const fn kind(self) -> TransitionContractKind {
        self.kind
    }

    #[must_use]
    pub const fn reason(self) -> StateReason {
        self.reason
    }

    /// Returns every state the action may produce. Deletion and conflicts have no target.
    #[must_use]
    pub const fn targets(self) -> &'static [TaskState] {
        self.targets
    }

    #[must_use]
    pub const fn accepts(self) -> bool {
        self.kind.accepts()
    }

    #[must_use]
    pub const fn rejection(self) -> Option<TransitionRejection> {
        self.rejection
    }
}

const NO_TARGETS: &[TaskState] = &[];
const ACCEPTED_TARGET: &[TaskState] = &[TaskState::Accepted];
const WAITING_TARGET: &[TaskState] = &[TaskState::Waiting];
const WAITING_SLOW_TARGET: &[TaskState] = &[TaskState::WaitingSlow];
const ALLOCATING_TARGET: &[TaskState] = &[TaskState::Allocating];
const ACTIVE_TARGET: &[TaskState] = &[TaskState::Active];
const RETRY_WAIT_TARGET: &[TaskState] = &[TaskState::RetryWait];
const PAUSED_TARGET: &[TaskState] = &[TaskState::Paused];
const PAUSED_SLOW_TARGET: &[TaskState] = &[TaskState::PausedSlow];
const PAUSED_HOST_KEY_TARGET: &[TaskState] = &[TaskState::PausedHostKey];
const PAUSED_RESTARTING_TARGET: &[TaskState] = &[TaskState::PausedRestarting];
const VERIFYING_TARGET: &[TaskState] = &[TaskState::Verifying];
const SEEDING_TARGET: &[TaskState] = &[TaskState::Seeding];
const COMPLETE_TARGET: &[TaskState] = &[TaskState::Complete];
const ERROR_TARGET: &[TaskState] = &[TaskState::Error];
const REMOVED_TARGET: &[TaskState] = &[TaskState::Removed];
const STOPPED_RESULT_TARGET: &[TaskState] = &[TaskState::StoppedResult];
const SHUTDOWN_RECOVERY_TARGETS: &[TaskState] = &[
    TaskState::Waiting,
    TaskState::Paused,
    TaskState::StoppedResult,
];

const fn state_target(state: TaskState) -> &'static [TaskState] {
    match state {
        TaskState::Accepted => ACCEPTED_TARGET,
        TaskState::Waiting => WAITING_TARGET,
        TaskState::WaitingSlow => WAITING_SLOW_TARGET,
        TaskState::Allocating => ALLOCATING_TARGET,
        TaskState::Active => ACTIVE_TARGET,
        TaskState::RetryWait => RETRY_WAIT_TARGET,
        TaskState::Paused => PAUSED_TARGET,
        TaskState::PausedSlow => PAUSED_SLOW_TARGET,
        TaskState::PausedHostKey => PAUSED_HOST_KEY_TARGET,
        TaskState::PausedRestarting => PAUSED_RESTARTING_TARGET,
        TaskState::Verifying => VERIFYING_TARGET,
        TaskState::Seeding => SEEDING_TARGET,
        TaskState::Complete => COMPLETE_TARGET,
        TaskState::Error => ERROR_TARGET,
        TaskState::Removed => REMOVED_TARGET,
        TaskState::StoppedResult => STOPPED_RESULT_TARGET,
    }
}

const fn transition(reason: StateReason, targets: &'static [TaskState]) -> TransitionContract {
    TransitionContract {
        kind: TransitionContractKind::Transition,
        reason,
        targets,
        rejection: None,
    }
}

const fn stay(state: TaskState, reason: StateReason) -> TransitionContract {
    TransitionContract {
        kind: TransitionContractKind::Stay,
        reason,
        targets: state_target(state),
        rejection: None,
    }
}

const fn conditional(reason: StateReason, targets: &'static [TaskState]) -> TransitionContract {
    TransitionContract {
        kind: TransitionContractKind::Conditional,
        reason,
        targets,
        rejection: None,
    }
}

const fn delete(reason: StateReason) -> TransitionContract {
    TransitionContract {
        kind: TransitionContractKind::Delete,
        reason,
        targets: NO_TARGETS,
        rejection: None,
    }
}

const fn no_op(state: TaskState) -> TransitionContract {
    TransitionContract {
        kind: TransitionContractKind::NoOp,
        reason: StateReason::Idempotent,
        targets: state_target(state),
        rejection: None,
    }
}

const fn ignore(state: TaskState, reason: StateReason) -> TransitionContract {
    TransitionContract {
        kind: TransitionContractKind::Ignore,
        reason,
        targets: state_target(state),
        rejection: None,
    }
}

const fn conflict() -> TransitionContract {
    TransitionContract {
        kind: TransitionContractKind::Conflict,
        reason: StateReason::Conflict,
        targets: NO_TARGETS,
        rejection: Some(TransitionRejection::Conflict),
    }
}

const fn rejected(reason: StateReason, rejection: TransitionRejection) -> TransitionContract {
    TransitionContract {
        kind: TransitionContractKind::Conflict,
        reason,
        targets: NO_TARGETS,
        rejection: Some(rejection),
    }
}

/// Returns the normative contract for every task-state and semantic-action pair.
///
/// The caller must select actions whose names encode already-validated context,
/// such as whether admission was blocked or a disk-space probe succeeded.
#[must_use]
pub const fn transition_contract(state: TaskState, action: SchedulerAction) -> TransitionContract {
    match action {
        SchedulerAction::ValidationSucceeded => match state {
            TaskState::Accepted => transition(StateReason::ValidationSucceeded, WAITING_TARGET),
            _ => conflict(),
        },
        SchedulerAction::ValidationSucceededPaused => match state {
            TaskState::Accepted => transition(StateReason::ValidationSucceeded, PAUSED_TARGET),
            _ => conflict(),
        },
        SchedulerAction::ValidationFailed => match state {
            TaskState::Accepted => transition(StateReason::ValidationFailed, ERROR_TARGET),
            _ => conflict(),
        },
        SchedulerAction::Pause => match state {
            TaskState::Accepted
            | TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::Allocating
            | TaskState::Active
            | TaskState::RetryWait
            | TaskState::PausedRestarting
            | TaskState::Verifying
            | TaskState::Seeding => transition(StateReason::UserPause, PAUSED_TARGET),
            TaskState::Paused | TaskState::PausedSlow | TaskState::PausedHostKey => {
                stay(state, StateReason::UserPause)
            }
            TaskState::Complete
            | TaskState::Error
            | TaskState::Removed
            | TaskState::StoppedResult => conflict(),
        },
        SchedulerAction::PauseDeferred => match state {
            TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::Allocating
            | TaskState::Active
            | TaskState::RetryWait
            | TaskState::Paused
            | TaskState::PausedSlow
            | TaskState::PausedHostKey
            | TaskState::PausedRestarting
            | TaskState::Verifying
            | TaskState::Seeding => stay(state, StateReason::UserPause),
            TaskState::Accepted
            | TaskState::Complete
            | TaskState::Error
            | TaskState::Removed
            | TaskState::StoppedResult => conflict(),
        },
        SchedulerAction::Resume => match state {
            TaskState::Accepted
            | TaskState::Waiting
            | TaskState::Allocating
            | TaskState::Active
            | TaskState::RetryWait
            | TaskState::PausedRestarting
            | TaskState::Verifying
            | TaskState::Seeding => no_op(state),
            TaskState::WaitingSlow | TaskState::Paused | TaskState::PausedSlow => {
                transition(StateReason::UserResume, WAITING_TARGET)
            }
            TaskState::PausedHostKey => rejected(
                StateReason::HostKeyApprovalRequired,
                TransitionRejection::HostKeyApprovalRequired,
            ),
            TaskState::Complete
            | TaskState::Error
            | TaskState::Removed
            | TaskState::StoppedResult => conflict(),
        },
        SchedulerAction::ResumeDeferred => match state {
            TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::Allocating
            | TaskState::Active
            | TaskState::RetryWait
            | TaskState::Paused
            | TaskState::PausedSlow
            | TaskState::PausedHostKey
            | TaskState::PausedRestarting
            | TaskState::Verifying
            | TaskState::Seeding => stay(state, StateReason::UserResume),
            TaskState::Accepted
            | TaskState::Complete
            | TaskState::Error
            | TaskState::Removed
            | TaskState::StoppedResult => conflict(),
        },
        SchedulerAction::Remove => match state {
            TaskState::Accepted
            | TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::Allocating
            | TaskState::Active
            | TaskState::RetryWait
            | TaskState::Paused
            | TaskState::PausedSlow
            | TaskState::PausedHostKey
            | TaskState::PausedRestarting
            | TaskState::Verifying
            | TaskState::Seeding => transition(StateReason::UserRemove, REMOVED_TARGET),
            TaskState::Removed => no_op(state),
            TaskState::Complete | TaskState::Error | TaskState::StoppedResult => conflict(),
        },
        SchedulerAction::RemoveDeferred => match state {
            TaskState::Accepted
            | TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::Allocating
            | TaskState::Active
            | TaskState::RetryWait
            | TaskState::Paused
            | TaskState::PausedSlow
            | TaskState::PausedHostKey
            | TaskState::PausedRestarting
            | TaskState::Verifying
            | TaskState::Seeding => stay(state, StateReason::UserRemove),
            TaskState::Complete
            | TaskState::Error
            | TaskState::Removed
            | TaskState::StoppedResult => conflict(),
        },
        SchedulerAction::SchedulerAdmission => match state {
            TaskState::Waiting => transition(StateReason::SchedulerAdmission, ALLOCATING_TARGET),
            _ => conflict(),
        },
        SchedulerAction::AdmissionBlocked => match state {
            TaskState::Waiting => stay(state, StateReason::AdmissionBlocked),
            _ => conflict(),
        },
        SchedulerAction::PlanningFailed => match state {
            TaskState::Waiting => transition(StateReason::PlanningFailed, ERROR_TARGET),
            _ => conflict(),
        },
        SchedulerAction::InPlaceOptionPatchAccepted => match state {
            TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::Allocating
            | TaskState::Active
            | TaskState::RetryWait
            | TaskState::Paused
            | TaskState::PausedSlow
            | TaskState::PausedHostKey
            | TaskState::PausedRestarting
            | TaskState::Verifying
            | TaskState::Seeding => stay(state, StateReason::OptionPatch),
            _ => conflict(),
        },
        SchedulerAction::InPlaceOptionPatchApplied => match state {
            TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::Allocating
            | TaskState::Active
            | TaskState::RetryWait
            | TaskState::Paused
            | TaskState::PausedSlow
            | TaskState::PausedHostKey
            | TaskState::PausedRestarting
            | TaskState::Verifying
            | TaskState::Seeding => stay(state, StateReason::OptionPatch),
            _ => conflict(),
        },
        SchedulerAction::InPlaceOptionPatchApplicationFailed => match state {
            TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::Allocating
            | TaskState::Active
            | TaskState::RetryWait
            | TaskState::Paused
            | TaskState::PausedSlow
            | TaskState::PausedHostKey
            | TaskState::PausedRestarting
            | TaskState::Verifying
            | TaskState::Seeding => stay(state, StateReason::OptionPatchApplicationFailed),
            _ => conflict(),
        },
        SchedulerAction::CredentialsSatisfied => match state {
            TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::RetryWait
            | TaskState::Paused
            | TaskState::PausedSlow => stay(state, StateReason::CredentialsSatisfied),
            _ => conflict(),
        },
        SchedulerAction::CredentialSatisfactionFailed => match state {
            TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::RetryWait
            | TaskState::Paused
            | TaskState::PausedSlow => stay(state, StateReason::CredentialUpdateFailed),
            _ => conflict(),
        },
        SchedulerAction::ExplicitNoSpaceProbeRequested => match state {
            TaskState::Waiting | TaskState::RetryWait | TaskState::Paused => {
                stay(state, StateReason::NoSpaceProbeRequested)
            }
            TaskState::WaitingSlow | TaskState::PausedSlow => {
                transition(StateReason::NoSpaceProbeRequested, WAITING_TARGET)
            }
            _ => conflict(),
        },
        SchedulerAction::WaitingNoSpaceProbeSucceeded => match state {
            TaskState::Waiting | TaskState::WaitingSlow | TaskState::RetryWait => {
                stay(state, StateReason::NoSpaceProbeSucceeded)
            }
            _ => conflict(),
        },
        SchedulerAction::WaitingNoSpaceProbeFailed => match state {
            TaskState::Waiting | TaskState::WaitingSlow | TaskState::RetryWait => {
                stay(state, StateReason::NoSpaceProbeFailed)
            }
            _ => conflict(),
        },
        SchedulerAction::PausedNoSpaceResumeProbeSucceeded => match state {
            TaskState::Paused => transition(StateReason::NoSpaceProbeSucceeded, WAITING_TARGET),
            _ => conflict(),
        },
        SchedulerAction::PausedNoSpaceResumeProbeFailed => match state {
            TaskState::Paused => transition(StateReason::NoSpaceProbeFailed, WAITING_TARGET),
            _ => conflict(),
        },
        SchedulerAction::PausedNoSpacePausePreservingProbeSucceeded => match state {
            TaskState::Paused | TaskState::PausedSlow => {
                stay(state, StateReason::NoSpaceProbeSucceeded)
            }
            _ => conflict(),
        },
        SchedulerAction::PausedNoSpacePausePreservingProbeFailed => match state {
            TaskState::Paused | TaskState::PausedSlow => {
                stay(state, StateReason::NoSpaceProbeFailed)
            }
            _ => conflict(),
        },
        SchedulerAction::GenerationPersistenceSucceeded => match state {
            TaskState::Allocating => stay(state, StateReason::GenerationPersistenceSucceeded),
            _ => conflict(),
        },
        SchedulerAction::AllocationSucceeded => match state {
            TaskState::Allocating => transition(StateReason::AllocationSucceeded, ACTIVE_TARGET),
            _ => conflict(),
        },
        SchedulerAction::AllocationRetryableFailure => match state {
            TaskState::Allocating => {
                transition(StateReason::AllocationRetryableFailure, RETRY_WAIT_TARGET)
            }
            _ => conflict(),
        },
        SchedulerAction::ActiveHostKeyChallengeRequired => match state {
            TaskState::Active => transition(StateReason::HostKeyChallenge, PAUSED_HOST_KEY_TARGET),
            _ => conflict(),
        },
        SchedulerAction::HostKeyChallengeRequired => match state {
            TaskState::Allocating => {
                transition(StateReason::HostKeyChallenge, PAUSED_HOST_KEY_TARGET)
            }
            _ => conflict(),
        },
        SchedulerAction::AllocationTerminalFailure => match state {
            TaskState::Allocating => {
                transition(StateReason::AllocationTerminalFailure, ERROR_TARGET)
            }
            _ => conflict(),
        },
        SchedulerAction::ActiveRestartOptionPatchAccepted => match state {
            TaskState::Allocating | TaskState::Active | TaskState::Verifying => {
                stay(state, StateReason::OptionPatch)
            }
            _ => conflict(),
        },
        SchedulerAction::SourceReplacementRequested => match state {
            TaskState::Allocating
            | TaskState::Active
            | TaskState::Verifying
            | TaskState::RetryWait => {
                transition(StateReason::SourceReplacement, PAUSED_RESTARTING_TARGET)
            }
            TaskState::Waiting | TaskState::Paused => stay(state, StateReason::SourceReplacement),
            _ => conflict(),
        },
        SchedulerAction::SourceReplacementCommitted => match state {
            TaskState::PausedRestarting => {
                transition(StateReason::SourceReplacement, WAITING_TARGET)
            }
            TaskState::Waiting | TaskState::Paused => stay(state, StateReason::SourceReplacement),
            _ => conflict(),
        },
        SchedulerAction::ActiveRestartOptionPatchPersisted => match state {
            TaskState::Allocating | TaskState::Active | TaskState::Verifying => transition(
                StateReason::OptionPatchPersistenceSucceeded,
                PAUSED_RESTARTING_TARGET,
            ),
            _ => conflict(),
        },
        SchedulerAction::ActiveRestartOptionPatchPersistenceFailed => match state {
            TaskState::Allocating | TaskState::Active | TaskState::Verifying => {
                stay(state, StateReason::OptionPatchPersistenceFailed)
            }
            _ => conflict(),
        },
        SchedulerAction::LeaseRetryableWithRunnableWork => match state {
            TaskState::Active => stay(state, StateReason::LeaseRetry),
            _ => conflict(),
        },
        SchedulerAction::LeaseRetryableWithoutRunnableWork => match state {
            TaskState::Active => transition(StateReason::LeaseRetry, RETRY_WAIT_TARGET),
            _ => conflict(),
        },
        SchedulerAction::RepresentationRestart => match state {
            TaskState::Active => transition(StateReason::RepresentationRestart, ALLOCATING_TARGET),
            _ => conflict(),
        },
        SchedulerAction::AllRequiredDataReceived => match state {
            TaskState::Active => transition(StateReason::PayloadReceived, VERIFYING_TARGET),
            _ => conflict(),
        },
        SchedulerAction::BitTorrentPayloadComplete => match state {
            TaskState::Active => transition(StateReason::BitTorrentSeeding, SEEDING_TARGET),
            _ => conflict(),
        },
        SchedulerAction::SlowSlotDemote => match state {
            TaskState::Active => transition(StateReason::SlowSlotDemotion, WAITING_SLOW_TARGET),
            TaskState::WaitingSlow => no_op(state),
            _ => conflict(),
        },
        SchedulerAction::SlowSlotPause => match state {
            TaskState::Active => transition(StateReason::SlowSlotPause, PAUSED_SLOW_TARGET),
            TaskState::PausedSlow => no_op(state),
            _ => conflict(),
        },
        SchedulerAction::MidTransferNoSpace => match state {
            TaskState::Active => transition(StateReason::NoSpace, WAITING_TARGET),
            _ => conflict(),
        },
        SchedulerAction::TerminalWorkFailure => match state {
            TaskState::Active => transition(StateReason::TerminalWorkFailure, ERROR_TARGET),
            _ => conflict(),
        },
        SchedulerAction::RetryReadmissionSucceeded => match state {
            TaskState::RetryWait => transition(StateReason::RetryReadmission, ALLOCATING_TARGET),
            _ => conflict(),
        },
        SchedulerAction::RetryReadmissionBlocked => match state {
            TaskState::RetryWait => stay(state, StateReason::AdmissionBlocked),
            _ => conflict(),
        },
        SchedulerAction::RetryExhausted => match state {
            TaskState::RetryWait => transition(StateReason::RetryExhausted, ERROR_TARGET),
            _ => conflict(),
        },
        SchedulerAction::SlowReadmissionSucceeded => match state {
            TaskState::WaitingSlow => transition(StateReason::SlowReadmission, ALLOCATING_TARGET),
            _ => conflict(),
        },
        SchedulerAction::SlowReadmissionBlocked => match state {
            TaskState::WaitingSlow => stay(state, StateReason::AdmissionBlocked),
            _ => conflict(),
        },
        SchedulerAction::ApproveHostKey => match state {
            TaskState::PausedHostKey => stay(state, StateReason::HostKeyApproved),
            _ => conflict(),
        },
        SchedulerAction::ApplyMatchingHostKeyOption => match state {
            TaskState::PausedHostKey => stay(state, StateReason::OptionPatch),
            _ => conflict(),
        },
        SchedulerAction::HostKeyResolutionSucceeded => match state {
            TaskState::PausedHostKey => transition(StateReason::HostKeyApproved, WAITING_TARGET),
            _ => conflict(),
        },
        SchedulerAction::HostKeyResolutionSucceededPreservingPause => match state {
            TaskState::PausedHostKey => transition(StateReason::HostKeyApproved, PAUSED_TARGET),
            _ => conflict(),
        },
        SchedulerAction::HostKeyResolutionFailed => match state {
            TaskState::PausedHostKey => stay(state, StateReason::HostKeyResolutionFailed),
            _ => conflict(),
        },
        SchedulerAction::CancellationDrainSucceeded => match state {
            TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::Paused
            | TaskState::PausedSlow
            | TaskState::Error
            | TaskState::Removed => stay(state, StateReason::CancellationDrained),
            _ => conflict(),
        },
        SchedulerAction::RestartQuiesced => match state {
            TaskState::PausedRestarting => stay(state, StateReason::RestartQuiesced),
            _ => conflict(),
        },
        SchedulerAction::RestartApplicationScheduled => match state {
            TaskState::Waiting => stay(state, StateReason::OptionPatch),
            _ => conflict(),
        },
        SchedulerAction::RestartApplicationSucceeded => match state {
            TaskState::PausedRestarting => {
                transition(StateReason::RestartApplicationSucceeded, WAITING_TARGET)
            }
            TaskState::Waiting => stay(state, StateReason::RestartApplicationSucceeded),
            _ => conflict(),
        },
        SchedulerAction::RestartApplicationFailed => match state {
            TaskState::PausedRestarting | TaskState::Waiting => {
                transition(StateReason::RestartApplicationFailed, ERROR_TARGET)
            }
            _ => conflict(),
        },
        SchedulerAction::DeferredPauseCompleted => match state {
            TaskState::Accepted
            | TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::Allocating
            | TaskState::Active
            | TaskState::RetryWait
            | TaskState::PausedRestarting
            | TaskState::Verifying
            | TaskState::Seeding => transition(StateReason::UserPause, PAUSED_TARGET),
            TaskState::Paused | TaskState::PausedSlow => stay(state, StateReason::UserPause),
            TaskState::PausedHostKey
            | TaskState::Complete
            | TaskState::Error
            | TaskState::Removed
            | TaskState::StoppedResult => conflict(),
        },
        SchedulerAction::DeferredResumeCompleted => match state {
            TaskState::Paused | TaskState::PausedSlow => {
                transition(StateReason::UserResume, WAITING_TARGET)
            }
            _ => conflict(),
        },
        SchedulerAction::DeferredRemoveCompleted => match state {
            TaskState::Accepted
            | TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::Allocating
            | TaskState::Active
            | TaskState::RetryWait
            | TaskState::Paused
            | TaskState::PausedSlow
            | TaskState::PausedHostKey
            | TaskState::PausedRestarting
            | TaskState::Verifying
            | TaskState::Seeding => transition(StateReason::UserRemove, REMOVED_TARGET),
            TaskState::Complete
            | TaskState::Error
            | TaskState::Removed
            | TaskState::StoppedResult => conflict(),
        },
        SchedulerAction::VerificationSucceeded => match state {
            TaskState::Verifying => transition(StateReason::VerificationSucceeded, COMPLETE_TARGET),
            _ => conflict(),
        },
        SchedulerAction::VerificationRecoverableFailure => match state {
            TaskState::Verifying => {
                transition(StateReason::VerificationRecoverableFailure, WAITING_TARGET)
            }
            _ => conflict(),
        },
        SchedulerAction::VerificationTerminalFailure => match state {
            TaskState::Verifying => {
                transition(StateReason::VerificationTerminalFailure, ERROR_TARGET)
            }
            _ => conflict(),
        },
        SchedulerAction::SeedingStopped => match state {
            TaskState::Seeding => transition(StateReason::SeedingStopped, COMPLETE_TARGET),
            _ => conflict(),
        },
        SchedulerAction::BitTorrentFailure => match state {
            TaskState::Seeding => transition(StateReason::BitTorrentFailure, ERROR_TARGET),
            _ => conflict(),
        },
        SchedulerAction::TerminalPersistenceSucceeded => match state {
            TaskState::Complete | TaskState::Error | TaskState::Removed => transition(
                StateReason::TerminalPersistenceSucceeded,
                STOPPED_RESULT_TARGET,
            ),
            TaskState::StoppedResult => no_op(state),
            _ => conflict(),
        },
        SchedulerAction::RemoveStoppedResult => match state {
            TaskState::StoppedResult => stay(state, StateReason::StoppedResultRemovalRequested),
            _ => conflict(),
        },
        SchedulerAction::StoppedResultDeletionSucceeded => match state {
            TaskState::StoppedResult => delete(StateReason::StoppedResultRemoved),
            _ => conflict(),
        },
        SchedulerAction::StoppedResultDeletionFailed => match state {
            TaskState::StoppedResult => stay(state, StateReason::StoppedResultRemovalFailed),
            _ => conflict(),
        },
        SchedulerAction::OrderlyShutdown => match state {
            TaskState::Accepted
            | TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::Allocating
            | TaskState::Active
            | TaskState::RetryWait
            | TaskState::Paused
            | TaskState::PausedSlow
            | TaskState::PausedHostKey
            | TaskState::PausedRestarting
            | TaskState::Verifying
            | TaskState::Seeding => {
                conditional(StateReason::OrderlyShutdown, SHUTDOWN_RECOVERY_TARGETS)
            }
            TaskState::Complete
            | TaskState::Error
            | TaskState::Removed
            | TaskState::StoppedResult => no_op(state),
        },
        SchedulerAction::DuplicateEventIgnored => ignore(state, StateReason::DuplicateEventIgnored),
        SchedulerAction::StaleEventIgnored => ignore(state, StateReason::StaleEventIgnored),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ALL_EVENT_DISPOSITIONS, ALL_SCHEDULER_ACTIONS, ALL_STATE_REASONS,
        ALL_TRANSITION_CONTRACT_KINDS, ALL_TRANSITION_REJECTIONS, EventDisposition,
        SHUTDOWN_RECOVERY_TARGETS, SchedulerAction, SchedulerActionSource,
        SchedulerActionSourceError, StateReason, TransitionContractKind, TransitionRejection,
        transition_contract,
    };
    use crate::{
        ALL_ARIA2_STATUSES, ALL_DRAIN_TARGETS, ALL_SCHEDULER_COMMAND_KINDS, ALL_TASK_EVENT_KINDS,
        ALL_TASK_STATES, Aria2Status, DrainTarget, NoSpaceProbeOrigin, SchedulerCommandHandling,
        SchedulerCommandKind, TaskConditionsSnapshot, TaskEventKind, TaskState, WireProjection,
    };
    use std::collections::BTreeSet;

    #[test]
    fn stable_vocabularies_are_unique() {
        let action_codes: BTreeSet<_> = ALL_SCHEDULER_ACTIONS
            .iter()
            .map(|action| action.code())
            .collect();
        let reason_codes: BTreeSet<_> = ALL_STATE_REASONS
            .iter()
            .map(|reason| reason.code())
            .collect();
        let kind_codes: BTreeSet<_> = ALL_TRANSITION_CONTRACT_KINDS
            .iter()
            .map(|kind| kind.code())
            .collect();
        let disposition_codes: BTreeSet<_> = ALL_EVENT_DISPOSITIONS
            .iter()
            .map(|disposition| disposition.code())
            .collect();
        let rejection_codes: BTreeSet<_> = ALL_TRANSITION_REJECTIONS
            .iter()
            .map(|rejection| rejection.code())
            .collect();
        assert_eq!(action_codes.len(), ALL_SCHEDULER_ACTIONS.len());
        assert_eq!(reason_codes.len(), ALL_STATE_REASONS.len());
        assert_eq!(kind_codes.len(), ALL_TRANSITION_CONTRACT_KINDS.len());
        assert_eq!(disposition_codes.len(), ALL_EVENT_DISPOSITIONS.len());
        assert_eq!(rejection_codes.len(), ALL_TRANSITION_REJECTIONS.len());
    }

    #[test]
    fn every_transition_matrix_cell_has_a_canonical_shape() {
        let mut cells = 0_usize;
        for state in ALL_TASK_STATES.iter().copied() {
            for action in ALL_SCHEDULER_ACTIONS.iter().copied() {
                let contract = transition_contract(state, action);
                let targets: BTreeSet<_> = contract.targets().iter().copied().collect();
                assert_eq!(
                    targets.len(),
                    contract.targets().len(),
                    "{state:?} + {action:?}"
                );
                match contract.kind() {
                    TransitionContractKind::Transition => {
                        assert_eq!(contract.targets().len(), 1, "{state:?} + {action:?}");
                        assert_ne!(contract.targets()[0], state, "{state:?} + {action:?}");
                        assert!(!matches!(
                            contract.reason(),
                            StateReason::Idempotent | StateReason::Conflict
                        ));
                        assert_eq!(contract.rejection(), None);
                    }
                    TransitionContractKind::Stay => {
                        assert_eq!(contract.targets(), &[state], "{state:?} + {action:?}");
                        assert!(!matches!(
                            contract.reason(),
                            StateReason::Idempotent | StateReason::Conflict
                        ));
                        assert_eq!(contract.rejection(), None);
                    }
                    TransitionContractKind::Conditional => {
                        assert!(contract.targets().len() > 1, "{state:?} + {action:?}");
                        assert!(!matches!(
                            contract.reason(),
                            StateReason::Idempotent | StateReason::Conflict
                        ));
                        assert_eq!(contract.rejection(), None);
                    }
                    TransitionContractKind::Delete => {
                        assert!(contract.targets().is_empty(), "{state:?} + {action:?}");
                        assert_eq!(contract.reason(), StateReason::StoppedResultRemoved);
                        assert_eq!(contract.rejection(), None);
                    }
                    TransitionContractKind::NoOp => {
                        assert_eq!(contract.targets(), &[state], "{state:?} + {action:?}");
                        assert_eq!(contract.reason(), StateReason::Idempotent);
                        assert_eq!(contract.rejection(), None);
                    }
                    TransitionContractKind::Ignore => {
                        assert_eq!(contract.targets(), &[state], "{state:?} + {action:?}");
                        assert!(matches!(
                            contract.reason(),
                            StateReason::DuplicateEventIgnored | StateReason::StaleEventIgnored
                        ));
                        assert_eq!(contract.rejection(), None);
                    }
                    TransitionContractKind::Conflict => {
                        assert!(contract.targets().is_empty(), "{state:?} + {action:?}");
                        assert!(
                            matches!(
                                contract.reason(),
                                StateReason::Conflict | StateReason::HostKeyApprovalRequired
                            ),
                            "{state:?} + {action:?} used {:?}",
                            contract.reason()
                        );
                        let expected = match contract.reason() {
                            StateReason::HostKeyApprovalRequired => {
                                TransitionRejection::HostKeyApprovalRequired
                            }
                            StateReason::Conflict => TransitionRejection::Conflict,
                            reason => panic!("unexpected rejection reason {reason:?}"),
                        };
                        assert_eq!(contract.rejection(), Some(expected));
                    }
                }
                assert_eq!(contract.accepts(), contract.kind().accepts());
                cells += 1;
            }
        }
        assert_eq!(cells, ALL_TASK_STATES.len() * ALL_SCHEDULER_ACTIONS.len());
        assert_eq!(cells, 16 * 74);
    }

    #[test]
    fn pause_and_remove_paths_are_total_and_terminal_safe() {
        for state in [
            TaskState::Accepted,
            TaskState::Waiting,
            TaskState::WaitingSlow,
            TaskState::Allocating,
            TaskState::Active,
            TaskState::RetryWait,
            TaskState::PausedRestarting,
            TaskState::Verifying,
            TaskState::Seeding,
        ] {
            assert_contract(
                state,
                SchedulerAction::Pause,
                TransitionContractKind::Transition,
                StateReason::UserPause,
                &[TaskState::Paused],
            );
        }
        for state in [
            TaskState::Paused,
            TaskState::PausedSlow,
            TaskState::PausedHostKey,
        ] {
            assert_contract(
                state,
                SchedulerAction::Pause,
                TransitionContractKind::Stay,
                StateReason::UserPause,
                &[state],
            );
        }
        for state in ALL_TASK_STATES.iter().copied().filter(|state| {
            !matches!(
                state,
                TaskState::Complete | TaskState::Error | TaskState::StoppedResult
            )
        }) {
            let contract = transition_contract(state, SchedulerAction::Remove);
            assert!(contract.accepts(), "remove rejected from {state:?}");
            assert!(
                contract
                    .targets()
                    .iter()
                    .all(|target| *target == TaskState::Removed),
                "remove from {state:?} targeted {:?}",
                contract.targets()
            );
        }
    }

    #[test]
    fn payload_and_condition_sensitive_commands_select_executable_rows() {
        assert_eq!(
            SchedulerAction::for_validated_add(false),
            SchedulerAction::ValidationSucceeded
        );
        assert_eq!(
            SchedulerAction::for_validated_add(true),
            SchedulerAction::ValidationSucceededPaused
        );
        assert_contract(
            TaskState::Accepted,
            SchedulerAction::ValidationSucceededPaused,
            TransitionContractKind::Transition,
            StateReason::ValidationSucceeded,
            &[TaskState::Paused],
        );

        let no_space = TaskConditionsSnapshot {
            needs_credentials: false,
            no_space: true,
        };
        for (state, kind, targets) in [
            (
                TaskState::Waiting,
                TransitionContractKind::Stay,
                &[TaskState::Waiting][..],
            ),
            (
                TaskState::RetryWait,
                TransitionContractKind::Stay,
                &[TaskState::RetryWait][..],
            ),
            (
                TaskState::Paused,
                TransitionContractKind::Stay,
                &[TaskState::Paused][..],
            ),
            (
                TaskState::WaitingSlow,
                TransitionContractKind::Transition,
                &[TaskState::Waiting][..],
            ),
            (
                TaskState::PausedSlow,
                TransitionContractKind::Transition,
                &[TaskState::Waiting][..],
            ),
        ] {
            assert_eq!(
                SchedulerAction::for_resume(state, no_space),
                SchedulerAction::ExplicitNoSpaceProbeRequested
            );
            assert_contract(
                state,
                SchedulerAction::ExplicitNoSpaceProbeRequested,
                kind,
                StateReason::NoSpaceProbeRequested,
                targets,
            );
        }
        assert_eq!(
            SchedulerAction::for_resume(TaskState::Paused, TaskConditionsSnapshot::default()),
            SchedulerAction::Resume
        );

        for (state, origin, desired_paused, ready, expected) in [
            (
                TaskState::Waiting,
                NoSpaceProbeOrigin::ExplicitResume,
                false,
                true,
                SchedulerAction::WaitingNoSpaceProbeSucceeded,
            ),
            (
                TaskState::Waiting,
                NoSpaceProbeOrigin::ExplicitResume,
                false,
                false,
                SchedulerAction::WaitingNoSpaceProbeFailed,
            ),
            (
                TaskState::Waiting,
                NoSpaceProbeOrigin::AutomaticRetry,
                true,
                true,
                SchedulerAction::WaitingNoSpaceProbeSucceeded,
            ),
            (
                TaskState::Waiting,
                NoSpaceProbeOrigin::AutomaticRetry,
                true,
                false,
                SchedulerAction::WaitingNoSpaceProbeFailed,
            ),
            (
                TaskState::RetryWait,
                NoSpaceProbeOrigin::AutomaticRetry,
                false,
                true,
                SchedulerAction::WaitingNoSpaceProbeSucceeded,
            ),
            (
                TaskState::WaitingSlow,
                NoSpaceProbeOrigin::AutomaticRetry,
                false,
                false,
                SchedulerAction::WaitingNoSpaceProbeFailed,
            ),
            (
                TaskState::Paused,
                NoSpaceProbeOrigin::ExplicitResume,
                false,
                true,
                SchedulerAction::PausedNoSpaceResumeProbeSucceeded,
            ),
            (
                TaskState::Paused,
                NoSpaceProbeOrigin::ExplicitResume,
                false,
                false,
                SchedulerAction::PausedNoSpaceResumeProbeFailed,
            ),
            (
                TaskState::Paused,
                NoSpaceProbeOrigin::AutomaticRetry,
                true,
                true,
                SchedulerAction::PausedNoSpacePausePreservingProbeSucceeded,
            ),
            (
                TaskState::Paused,
                NoSpaceProbeOrigin::AutomaticRetry,
                true,
                false,
                SchedulerAction::PausedNoSpacePausePreservingProbeFailed,
            ),
            (
                TaskState::PausedSlow,
                NoSpaceProbeOrigin::AutomaticRetry,
                true,
                true,
                SchedulerAction::PausedNoSpacePausePreservingProbeSucceeded,
            ),
        ] {
            assert_eq!(
                SchedulerAction::for_no_space_probe_result(state, origin, desired_paused, ready,),
                Some(expected)
            );
            assert!(transition_contract(state, expected).accepts());
        }
        assert_eq!(
            SchedulerAction::for_no_space_probe_result(
                TaskState::Active,
                NoSpaceProbeOrigin::AutomaticRetry,
                false,
                true,
            ),
            None
        );
    }

    #[test]
    fn re_pause_during_explicit_no_space_probe_wins_over_the_earlier_resume() {
        let no_space = TaskConditionsSnapshot {
            needs_credentials: false,
            no_space: true,
        };
        assert_eq!(
            SchedulerAction::for_resume(TaskState::Paused, no_space),
            SchedulerAction::ExplicitNoSpaceProbeRequested
        );
        assert_contract(
            TaskState::Paused,
            SchedulerAction::Pause,
            TransitionContractKind::Stay,
            StateReason::UserPause,
            &[TaskState::Paused],
        );

        for (ready, expected, reason) in [
            (
                true,
                SchedulerAction::PausedNoSpacePausePreservingProbeSucceeded,
                StateReason::NoSpaceProbeSucceeded,
            ),
            (
                false,
                SchedulerAction::PausedNoSpacePausePreservingProbeFailed,
                StateReason::NoSpaceProbeFailed,
            ),
        ] {
            let action = SchedulerAction::for_no_space_probe_result(
                TaskState::Paused,
                NoSpaceProbeOrigin::ExplicitResume,
                true,
                ready,
            );
            assert_eq!(action, Some(expected));
            assert_contract(
                TaskState::Paused,
                expected,
                TransitionContractKind::Stay,
                reason,
                &[TaskState::Paused],
            );
        }
    }

    #[test]
    fn resume_is_idempotent_for_states_that_are_already_unpaused() {
        for state in [
            TaskState::Accepted,
            TaskState::Waiting,
            TaskState::Allocating,
            TaskState::Active,
            TaskState::RetryWait,
            TaskState::PausedRestarting,
            TaskState::Verifying,
            TaskState::Seeding,
        ] {
            assert_contract(
                state,
                SchedulerAction::Resume,
                TransitionContractKind::NoOp,
                StateReason::Idempotent,
                &[state],
            );
        }
    }

    #[test]
    fn retry_paths_preserve_active_work_and_gate_task_readmission() {
        assert_contract(
            TaskState::Active,
            SchedulerAction::LeaseRetryableWithRunnableWork,
            TransitionContractKind::Stay,
            StateReason::LeaseRetry,
            &[TaskState::Active],
        );
        assert_contract(
            TaskState::Active,
            SchedulerAction::LeaseRetryableWithoutRunnableWork,
            TransitionContractKind::Transition,
            StateReason::LeaseRetry,
            &[TaskState::RetryWait],
        );
        assert_contract(
            TaskState::RetryWait,
            SchedulerAction::RetryReadmissionSucceeded,
            TransitionContractKind::Transition,
            StateReason::RetryReadmission,
            &[TaskState::Allocating],
        );
        assert_contract(
            TaskState::RetryWait,
            SchedulerAction::RetryReadmissionBlocked,
            TransitionContractKind::Stay,
            StateReason::AdmissionBlocked,
            &[TaskState::RetryWait],
        );
        assert_contract(
            TaskState::RetryWait,
            SchedulerAction::RetryExhausted,
            TransitionContractKind::Transition,
            StateReason::RetryExhausted,
            &[TaskState::Error],
        );
        assert_contract(
            TaskState::WaitingSlow,
            SchedulerAction::SlowReadmissionSucceeded,
            TransitionContractKind::Transition,
            StateReason::SlowReadmission,
            &[TaskState::Allocating],
        );
        assert_contract(
            TaskState::WaitingSlow,
            SchedulerAction::SlowReadmissionBlocked,
            TransitionContractKind::Stay,
            StateReason::AdmissionBlocked,
            &[TaskState::WaitingSlow],
        );
    }

    #[test]
    fn persistence_and_cancellation_acknowledgements_are_modeled_as_fresh_actions() {
        assert_contract(
            TaskState::Allocating,
            SchedulerAction::GenerationPersistenceSucceeded,
            TransitionContractKind::Stay,
            StateReason::GenerationPersistenceSucceeded,
            &[TaskState::Allocating],
        );

        for state in [
            TaskState::Allocating,
            TaskState::Active,
            TaskState::Verifying,
        ] {
            assert_contract(
                state,
                SchedulerAction::ActiveRestartOptionPatchAccepted,
                TransitionContractKind::Stay,
                StateReason::OptionPatch,
                &[state],
            );
            assert_contract(
                state,
                SchedulerAction::ActiveRestartOptionPatchPersisted,
                TransitionContractKind::Transition,
                StateReason::OptionPatchPersistenceSucceeded,
                &[TaskState::PausedRestarting],
            );
            assert_contract(
                state,
                SchedulerAction::ActiveRestartOptionPatchPersistenceFailed,
                TransitionContractKind::Stay,
                StateReason::OptionPatchPersistenceFailed,
                &[state],
            );
        }

        for target in ALL_DRAIN_TARGETS {
            let state = target.state();
            if *target == DrainTarget::PausedRestarting {
                assert_contract(
                    state,
                    SchedulerAction::RestartQuiesced,
                    TransitionContractKind::Stay,
                    StateReason::RestartQuiesced,
                    &[TaskState::PausedRestarting],
                );
            } else {
                assert_contract(
                    state,
                    SchedulerAction::CancellationDrainSucceeded,
                    TransitionContractKind::Stay,
                    StateReason::CancellationDrained,
                    &[state],
                );
            }
        }

        assert_contract(
            TaskState::PausedRestarting,
            SchedulerAction::RestartApplicationSucceeded,
            TransitionContractKind::Transition,
            StateReason::RestartApplicationSucceeded,
            &[TaskState::Waiting],
        );
        assert_contract(
            TaskState::PausedRestarting,
            SchedulerAction::RestartApplicationFailed,
            TransitionContractKind::Transition,
            StateReason::RestartApplicationFailed,
            &[TaskState::Error],
        );
    }

    #[test]
    fn host_key_resolution_waits_for_persistence_and_honors_current_pause_intent() {
        for action in [
            SchedulerAction::ApproveHostKey,
            SchedulerAction::ApplyMatchingHostKeyOption,
        ] {
            assert_contract(
                TaskState::PausedHostKey,
                action,
                TransitionContractKind::Stay,
                if action == SchedulerAction::ApproveHostKey {
                    StateReason::HostKeyApproved
                } else {
                    StateReason::OptionPatch
                },
                &[TaskState::PausedHostKey],
            );
        }

        assert_eq!(
            SchedulerAction::for_host_key_resolution(false),
            SchedulerAction::HostKeyResolutionSucceeded
        );
        assert_contract(
            TaskState::PausedHostKey,
            SchedulerAction::HostKeyResolutionSucceeded,
            TransitionContractKind::Transition,
            StateReason::HostKeyApproved,
            &[TaskState::Waiting],
        );

        assert_eq!(
            SchedulerAction::for_host_key_resolution(true),
            SchedulerAction::HostKeyResolutionSucceededPreservingPause
        );
        assert_contract(
            TaskState::PausedHostKey,
            SchedulerAction::HostKeyResolutionSucceededPreservingPause,
            TransitionContractKind::Transition,
            StateReason::HostKeyApproved,
            &[TaskState::Paused],
        );

        assert_contract(
            TaskState::PausedHostKey,
            SchedulerAction::HostKeyResolutionFailed,
            TransitionContractKind::Stay,
            StateReason::HostKeyResolutionFailed,
            &[TaskState::PausedHostKey],
        );
    }

    #[test]
    fn terminal_paths_require_persistence_before_retention_and_deletion() {
        for state in [TaskState::Complete, TaskState::Error, TaskState::Removed] {
            assert!(state.is_terminal_pending());
            assert_contract(
                state,
                SchedulerAction::TerminalPersistenceSucceeded,
                TransitionContractKind::Transition,
                StateReason::TerminalPersistenceSucceeded,
                &[TaskState::StoppedResult],
            );
        }
        assert_contract(
            TaskState::StoppedResult,
            SchedulerAction::TerminalPersistenceSucceeded,
            TransitionContractKind::NoOp,
            StateReason::Idempotent,
            &[TaskState::StoppedResult],
        );
        assert_contract(
            TaskState::StoppedResult,
            SchedulerAction::RemoveStoppedResult,
            TransitionContractKind::Stay,
            StateReason::StoppedResultRemovalRequested,
            &[TaskState::StoppedResult],
        );
        assert_contract(
            TaskState::StoppedResult,
            SchedulerAction::StoppedResultDeletionSucceeded,
            TransitionContractKind::Delete,
            StateReason::StoppedResultRemoved,
            &[],
        );
        assert!(TaskState::StoppedResult.is_retained_result());
    }

    #[test]
    fn every_matrix_target_projects_only_to_the_closed_wire_vocabulary() {
        for state in ALL_TASK_STATES.iter().copied() {
            for action in ALL_SCHEDULER_ACTIONS.iter().copied() {
                for target in transition_contract(state, action).targets() {
                    if *target == TaskState::StoppedResult {
                        for status in [
                            Aria2Status::Error,
                            Aria2Status::Complete,
                            Aria2Status::Removed,
                        ] {
                            assert_eq!(
                                WireProjection {
                                    stopped_status: Some(status),
                                    terminal_persisted: true,
                                    ..WireProjection::default()
                                }
                                .project(*target),
                                Ok(status)
                            );
                        }
                    } else {
                        let status = WireProjection {
                            terminal_persisted: target.is_terminal(),
                            ..WireProjection::default()
                        }
                        .project(*target)
                        .expect("non-stopped state projects");
                        assert!(ALL_ARIA2_STATUSES.contains(&status));
                    }
                }
            }
        }

        assert_eq!(
            WireProjection {
                conditions: TaskConditionsSnapshot {
                    needs_credentials: false,
                    no_space: true,
                },
                ..WireProjection::default()
            }
            .project(TaskState::Waiting),
            Ok(Aria2Status::Paused)
        );
    }

    #[test]
    fn key_normative_rows_match_the_design() {
        assert_contract(
            TaskState::Accepted,
            SchedulerAction::ValidationSucceeded,
            TransitionContractKind::Transition,
            StateReason::ValidationSucceeded,
            &[TaskState::Waiting],
        );
        assert_contract(
            TaskState::Waiting,
            SchedulerAction::AdmissionBlocked,
            TransitionContractKind::Stay,
            StateReason::AdmissionBlocked,
            &[TaskState::Waiting],
        );
        assert_contract(
            TaskState::Active,
            SchedulerAction::LeaseRetryableWithRunnableWork,
            TransitionContractKind::Stay,
            StateReason::LeaseRetry,
            &[TaskState::Active],
        );
        assert_contract(
            TaskState::Active,
            SchedulerAction::LeaseRetryableWithoutRunnableWork,
            TransitionContractKind::Transition,
            StateReason::LeaseRetry,
            &[TaskState::RetryWait],
        );
        assert_contract(
            TaskState::Active,
            SchedulerAction::SlowSlotDemote,
            TransitionContractKind::Transition,
            StateReason::SlowSlotDemotion,
            &[TaskState::WaitingSlow],
        );
        assert_contract(
            TaskState::Active,
            SchedulerAction::SlowSlotPause,
            TransitionContractKind::Transition,
            StateReason::SlowSlotPause,
            &[TaskState::PausedSlow],
        );
        assert_contract(
            TaskState::PausedHostKey,
            SchedulerAction::Resume,
            TransitionContractKind::Conflict,
            StateReason::HostKeyApprovalRequired,
            &[],
        );
        assert_eq!(
            transition_contract(TaskState::PausedHostKey, SchedulerAction::Resume).rejection(),
            Some(TransitionRejection::HostKeyApprovalRequired)
        );
        assert_contract(
            TaskState::PausedHostKey,
            SchedulerAction::ApproveHostKey,
            TransitionContractKind::Stay,
            StateReason::HostKeyApproved,
            &[TaskState::PausedHostKey],
        );
        assert_contract(
            TaskState::PausedHostKey,
            SchedulerAction::ApplyMatchingHostKeyOption,
            TransitionContractKind::Stay,
            StateReason::OptionPatch,
            &[TaskState::PausedHostKey],
        );
        assert_contract(
            TaskState::PausedHostKey,
            SchedulerAction::HostKeyResolutionSucceeded,
            TransitionContractKind::Transition,
            StateReason::HostKeyApproved,
            &[TaskState::Waiting],
        );
        assert_contract(
            TaskState::PausedRestarting,
            SchedulerAction::RestartQuiesced,
            TransitionContractKind::Stay,
            StateReason::RestartQuiesced,
            &[TaskState::PausedRestarting],
        );
        assert_contract(
            TaskState::PausedRestarting,
            SchedulerAction::RestartApplicationSucceeded,
            TransitionContractKind::Transition,
            StateReason::RestartApplicationSucceeded,
            &[TaskState::Waiting],
        );
        assert_contract(
            TaskState::Verifying,
            SchedulerAction::VerificationSucceeded,
            TransitionContractKind::Transition,
            StateReason::VerificationSucceeded,
            &[TaskState::Complete],
        );
        assert_contract(
            TaskState::Complete,
            SchedulerAction::TerminalPersistenceSucceeded,
            TransitionContractKind::Transition,
            StateReason::TerminalPersistenceSucceeded,
            &[TaskState::StoppedResult],
        );
        assert_contract(
            TaskState::StoppedResult,
            SchedulerAction::RemoveStoppedResult,
            TransitionContractKind::Stay,
            StateReason::StoppedResultRemovalRequested,
            &[TaskState::StoppedResult],
        );
        assert_contract(
            TaskState::StoppedResult,
            SchedulerAction::StoppedResultDeletionSucceeded,
            TransitionContractKind::Delete,
            StateReason::StoppedResultRemoved,
            &[],
        );
    }

    #[test]
    fn no_space_actions_preserve_the_condition_specific_rows() {
        assert_contract(
            TaskState::Waiting,
            SchedulerAction::WaitingNoSpaceProbeFailed,
            TransitionContractKind::Stay,
            StateReason::NoSpaceProbeFailed,
            &[TaskState::Waiting],
        );
        assert_contract(
            TaskState::Paused,
            SchedulerAction::PausedNoSpaceResumeProbeFailed,
            TransitionContractKind::Transition,
            StateReason::NoSpaceProbeFailed,
            &[TaskState::Waiting],
        );
        assert_contract(
            TaskState::Paused,
            SchedulerAction::PausedNoSpacePausePreservingProbeSucceeded,
            TransitionContractKind::Stay,
            StateReason::NoSpaceProbeSucceeded,
            &[TaskState::Paused],
        );
        for state in [TaskState::WaitingSlow, TaskState::RetryWait] {
            assert_contract(
                state,
                SchedulerAction::WaitingNoSpaceProbeSucceeded,
                TransitionContractKind::Stay,
                StateReason::NoSpaceProbeSucceeded,
                &[state],
            );
        }
        assert_contract(
            TaskState::PausedSlow,
            SchedulerAction::PausedNoSpacePausePreservingProbeFailed,
            TransitionContractKind::Stay,
            StateReason::NoSpaceProbeFailed,
            &[TaskState::PausedSlow],
        );
        for state in [
            TaskState::Waiting,
            TaskState::WaitingSlow,
            TaskState::RetryWait,
            TaskState::Paused,
            TaskState::PausedSlow,
        ] {
            assert_contract(
                state,
                SchedulerAction::CredentialsSatisfied,
                TransitionContractKind::Stay,
                StateReason::CredentialsSatisfied,
                &[state],
            );
        }
    }

    #[test]
    fn event_identity_disposition_is_explicit_and_state_independent() {
        for event_kind in ALL_TASK_EVENT_KINDS {
            let fresh_actions: Vec<_> = ALL_SCHEDULER_ACTIONS
                .iter()
                .copied()
                .filter(|action| action.source().matches_task_event(*event_kind))
                .collect();
            assert!(!fresh_actions.is_empty(), "{event_kind:?}");
            for fresh_action in fresh_actions {
                assert_eq!(
                    EventDisposition::Fresh.resolve(*event_kind, fresh_action),
                    Ok(fresh_action)
                );
                for state in ALL_TASK_STATES.iter().copied() {
                    for (disposition, expected_action, expected_reason) in [
                        (
                            EventDisposition::Duplicate,
                            SchedulerAction::DuplicateEventIgnored,
                            StateReason::DuplicateEventIgnored,
                        ),
                        (
                            EventDisposition::Stale,
                            SchedulerAction::StaleEventIgnored,
                            StateReason::StaleEventIgnored,
                        ),
                    ] {
                        let action = disposition
                            .resolve(*event_kind, fresh_action)
                            .expect("matching event action");
                        assert_eq!(action, expected_action, "{event_kind:?} + {state:?}");
                        let contract = transition_contract(state, action);
                        assert_eq!(
                            contract.kind(),
                            TransitionContractKind::Ignore,
                            "{event_kind:?} + {state:?}"
                        );
                        assert_eq!(contract.reason(), expected_reason);
                        assert_eq!(contract.targets(), &[state]);
                        assert_eq!(contract.rejection(), None);
                    }
                }
            }
        }
        assert_eq!(
            EventDisposition::Fresh.resolve(
                TaskEventKind::GenerationPersisted,
                SchedulerAction::AllocationSucceeded,
            ),
            Err(SchedulerActionSourceError::EventActionMismatch {
                event: TaskEventKind::GenerationPersisted,
                action: SchedulerAction::AllocationSucceeded,
            })
        );
    }

    #[test]
    fn semantic_actions_declare_closed_input_sources() {
        for action in ALL_SCHEDULER_ACTIONS.iter().copied() {
            match action.source() {
                SchedulerActionSource::Command(kind) => {
                    assert!(ALL_SCHEDULER_COMMAND_KINDS.contains(&kind), "{action:?}");
                }
                SchedulerActionSource::TaskEvent(kind) => {
                    assert!(ALL_TASK_EVENT_KINDS.contains(&kind), "{action:?}");
                }
                SchedulerActionSource::AnyTaskEvent | SchedulerActionSource::Internal => {}
            }
        }

        assert_eq!(
            SchedulerAction::Pause.source(),
            SchedulerActionSource::Command(SchedulerCommandKind::Pause)
        );
        assert_eq!(
            SchedulerAction::RetryReadmissionSucceeded.source(),
            SchedulerActionSource::TaskEvent(TaskEventKind::RetryReady)
        );
        assert_eq!(
            SchedulerAction::DuplicateEventIgnored.source(),
            SchedulerActionSource::AnyTaskEvent
        );

        for command in ALL_SCHEDULER_COMMAND_KINDS {
            let action_count = ALL_SCHEDULER_ACTIONS
                .iter()
                .copied()
                .filter(|action| action.source().matches_command(*command))
                .count();
            match command.handling() {
                SchedulerCommandHandling::StateMatrix => {
                    assert!(action_count > 0, "{command:?}");
                }
                SchedulerCommandHandling::QueueOperation => {
                    assert_eq!(action_count, 0, "{command:?}");
                }
                SchedulerCommandHandling::BatchOperation => {
                    assert!(action_count > 0, "{command:?}");
                }
            }
        }
    }

    #[test]
    fn shutdown_has_only_the_normative_recovery_targets() {
        for state in ALL_TASK_STATES
            .iter()
            .copied()
            .filter(|state| !state.is_terminal())
        {
            assert_contract(
                state,
                SchedulerAction::OrderlyShutdown,
                TransitionContractKind::Conditional,
                StateReason::OrderlyShutdown,
                SHUTDOWN_RECOVERY_TARGETS,
            );
        }
    }

    #[test]
    fn terminal_states_cannot_reactivate() {
        for state in [
            TaskState::Complete,
            TaskState::Error,
            TaskState::Removed,
            TaskState::StoppedResult,
        ] {
            for action in ALL_SCHEDULER_ACTIONS.iter().copied() {
                let contract = transition_contract(state, action);
                assert!(
                    contract.targets().iter().all(|target| target.is_terminal()),
                    "{state:?} + {action:?} could reactivate as {:?}",
                    contract.targets()
                );
            }
        }
    }

    fn assert_contract(
        state: TaskState,
        action: SchedulerAction,
        kind: TransitionContractKind,
        reason: StateReason,
        targets: &[TaskState],
    ) {
        let contract = transition_contract(state, action);
        assert_eq!(contract.kind(), kind, "{state:?} + {action:?}");
        assert_eq!(contract.reason(), reason, "{state:?} + {action:?}");
        assert_eq!(contract.targets(), targets, "{state:?} + {action:?}");
        assert_eq!(contract.accepts(), kind.accepts());
    }
}
