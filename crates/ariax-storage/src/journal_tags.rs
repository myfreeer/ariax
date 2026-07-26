use std::error::Error;
use std::fmt;

/// An unknown numeric value in a closed version-1 journal tag vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnknownJournalTag {
    vocabulary: &'static str,
    value: u8,
}

impl UnknownJournalTag {
    pub(crate) const fn new_for_contract() -> Self {
        Self {
            vocabulary: "contract",
            value: 0,
        }
    }

    #[must_use]
    pub const fn vocabulary(self) -> &'static str {
        self.vocabulary
    }

    #[must_use]
    pub const fn value(self) -> u8 {
        self.value
    }
}

impl fmt::Display for UnknownJournalTag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unknown {} journal tag {}",
            self.vocabulary, self.value
        )
    }
}

impl Error for UnknownJournalTag {}

macro_rules! journal_tag_enum {
    (
        $(#[$meta:meta])*
        $name:ident, $all:ident, $vocabulary:literal,
        $(($variant:ident, $number:literal, $code:literal)),+ $(,)?
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        #[repr(u8)]
        pub enum $name {
            $($variant = $number),+
        }

        impl $name {
            #[must_use]
            pub const fn number(self) -> u8 {
                self as u8
            }

            #[must_use]
            pub const fn code(self) -> &'static str {
                match self {
                    $(Self::$variant => $code),+
                }
            }

            #[must_use]
            pub const fn vocabulary() -> &'static str {
                $vocabulary
            }
        }

        impl TryFrom<u8> for $name {
            type Error = UnknownJournalTag;

            fn try_from(value: u8) -> Result<Self, Self::Error> {
                match value {
                    $($number => Ok(Self::$variant)),+,
                    _ => Err(UnknownJournalTag {
                        vocabulary: $vocabulary,
                        value,
                    }),
                }
            }
        }

        pub const $all: &[$name] = &[$($name::$variant),+];
    };
}

journal_tag_enum!(
    /// Data and journal flush policy selected when the task was created.
    DurabilityMode,
    ALL_DURABILITY_MODES,
    "durability",
    (Fast, 1, "fast"),
    (Balanced, 2, "balanced"),
    (Strict, 3, "strict"),
);

journal_tag_enum!(
    /// Which generation slot an option snapshot occupies.
    OptionsSnapshotScope,
    ALL_OPTIONS_SNAPSHOT_SCOPES,
    "options_snapshot_scope",
    (CurrentGeneration, 1, "current_generation"),
    (NextAdmission, 2, "next_admission"),
);

journal_tag_enum!(
    /// Why one admission advanced the task generation.
    GenerationStartReason,
    ALL_GENERATION_START_REASONS,
    "generation_start_reason",
    (OptionPatch, 1, "option_patch"),
    (RetryReadmission, 2, "retry_readmission"),
    (RepresentationRestart, 3, "representation_restart"),
    (BackendFailover, 4, "backend_failover"),
    (RootRebind, 5, "root_rebind"),
    (RecoveryRepair, 6, "recovery_repair"),
    (ExplicitRestart, 7, "explicit_restart"),
);

journal_tag_enum!(
    /// Why provisional bytes for one lease were made untrusted.
    LeaseAbortReason,
    ALL_LEASE_ABORT_REASONS,
    "lease_abort_reason",
    (Cancelled, 1, "cancelled"),
    (Redirect, 2, "redirect"),
    (ShortBody, 3, "short_body"),
    (OversizedBody, 4, "oversized_body"),
    (InvalidRange, 5, "invalid_range"),
    (StaleValidator, 6, "stale_validator"),
    (DigestMismatch, 7, "digest_mismatch"),
    (StorageRejected, 8, "storage_rejected"),
    (OverlapLost, 9, "overlap_lost"),
    (OverlapUncertain, 10, "overlap_uncertain"),
    (Retry, 11, "retry"),
    (GenerationDrain, 12, "generation_drain"),
);

journal_tag_enum!(
    /// Evidence for the data-before-journal barrier of a durable piece.
    DataBarrierKind,
    ALL_DATA_BARRIER_KINDS,
    "data_barrier",
    (BalancedGroup, 1, "balanced_group"),
    (StrictPiece, 2, "strict_piece"),
    (FastFinalization, 3, "fast_finalization"),
    (RecoveryReadback, 4, "recovery_readback"),
);

journal_tag_enum!(
    /// Stable owner of a persisted retry decision.
    RetryScope,
    ALL_RETRY_SCOPES,
    "retry_scope",
    (Task, 1, "task"),
    (Uri, 2, "uri"),
    (Span, 3, "span"),
    (Piece, 4, "piece"),
);

journal_tag_enum!(
    /// How the persisted retry delay was selected.
    RetryReason,
    ALL_RETRY_REASONS,
    "retry_reason",
    (Backoff, 1, "backoff"),
    (RetryAfter, 2, "retry_after"),
    (PolicyClamp, 3, "policy_clamp"),
);

journal_tag_enum!(
    /// Why a task-local pause checkpoint was written.
    TaskPauseReason,
    ALL_TASK_PAUSE_REASONS,
    "task_pause_reason",
    (User, 1, "user"),
    (NoSpace, 2, "no_space"),
    (SlowSlot, 3, "slow_slot"),
    (HostKeyApproval, 4, "host_key_approval"),
    (Restarting, 5, "restarting"),
    (RecoveryHold, 6, "recovery_hold"),
);

journal_tag_enum!(
    /// Why the task was terminally removed from the scheduler.
    TaskRemoveReason,
    ALL_TASK_REMOVE_REASONS,
    "task_remove_reason",
    (User, 1, "user"),
    (SessionCleanup, 2, "session_cleanup"),
    (Replaced, 3, "replaced"),
);

#[cfg(test)]
mod tests {
    use super::{
        ALL_DATA_BARRIER_KINDS, ALL_DURABILITY_MODES, ALL_GENERATION_START_REASONS,
        ALL_LEASE_ABORT_REASONS, ALL_OPTIONS_SNAPSHOT_SCOPES, ALL_RETRY_REASONS, ALL_RETRY_SCOPES,
        ALL_TASK_PAUSE_REASONS, ALL_TASK_REMOVE_REASONS, DataBarrierKind, DurabilityMode,
        GenerationStartReason, LeaseAbortReason, OptionsSnapshotScope, RetryReason, RetryScope,
        TaskPauseReason, TaskRemoveReason,
    };
    use std::collections::BTreeSet;

    macro_rules! assert_closed {
        ($type:ty, $all:expr) => {{
            let values = $all;
            let numbers: BTreeSet<_> = values.iter().map(|value| value.number()).collect();
            let codes: BTreeSet<_> = values.iter().map(|value| value.code()).collect();
            assert_eq!(numbers.len(), values.len());
            assert_eq!(codes.len(), values.len());
            for (index, value) in values.iter().copied().enumerate() {
                assert_eq!(value.number(), index as u8 + 1);
                assert_eq!(<$type>::try_from(value.number()), Ok(value));
            }
            let zero = <$type>::try_from(0).expect_err("zero is reserved");
            assert_eq!(zero.value(), 0);
            assert_eq!(zero.vocabulary(), <$type>::vocabulary());
            assert!(<$type>::try_from(values.len() as u8 + 1).is_err());
        }};
    }

    #[test]
    fn every_journal_tag_vocabulary_is_closed_and_contiguous() {
        assert_closed!(DurabilityMode, ALL_DURABILITY_MODES);
        assert_closed!(OptionsSnapshotScope, ALL_OPTIONS_SNAPSHOT_SCOPES);
        assert_closed!(GenerationStartReason, ALL_GENERATION_START_REASONS);
        assert_closed!(LeaseAbortReason, ALL_LEASE_ABORT_REASONS);
        assert_closed!(DataBarrierKind, ALL_DATA_BARRIER_KINDS);
        assert_closed!(RetryScope, ALL_RETRY_SCOPES);
        assert_closed!(RetryReason, ALL_RETRY_REASONS);
        assert_closed!(TaskPauseReason, ALL_TASK_PAUSE_REASONS);
        assert_closed!(TaskRemoveReason, ALL_TASK_REMOVE_REASONS);
    }
}
