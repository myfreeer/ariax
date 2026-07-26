use crate::{ErrorKind, LeaseId, UriId};
use std::error::Error;
use std::fmt;
use std::time::{Duration, Instant};

/// A process-local monotonic deadline. It is never serialized directly.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MonotonicInstant(Instant);

impl MonotonicInstant {
    #[must_use]
    pub fn now() -> Self {
        Self(Instant::now())
    }

    #[must_use]
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        self.0.checked_add(duration).map(Self)
    }

    #[must_use]
    pub fn duration_since(self, earlier: Self) -> Duration {
        self.0.saturating_duration_since(earlier.0)
    }
}

/// Internal scheduler task states. These names never appear as RPC statuses.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TaskState {
    Accepted,
    Waiting,
    WaitingSlow,
    Allocating,
    Active,
    RetryWait,
    Paused,
    PausedSlow,
    PausedHostKey,
    PausedRestarting,
    Verifying,
    Seeding,
    Complete,
    Error,
    Removed,
    StoppedResult,
}

/// Every internal task state in canonical matrix order.
pub const ALL_TASK_STATES: &[TaskState] = &[
    TaskState::Accepted,
    TaskState::Waiting,
    TaskState::WaitingSlow,
    TaskState::Allocating,
    TaskState::Active,
    TaskState::RetryWait,
    TaskState::Paused,
    TaskState::PausedSlow,
    TaskState::PausedHostKey,
    TaskState::PausedRestarting,
    TaskState::Verifying,
    TaskState::Seeding,
    TaskState::Complete,
    TaskState::Error,
    TaskState::Removed,
    TaskState::StoppedResult,
];

impl TaskState {
    /// Returns the stable internal state name used by diagnostics and matrices.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Waiting => "waiting",
            Self::WaitingSlow => "waiting_slow",
            Self::Allocating => "allocating",
            Self::Active => "active",
            Self::RetryWait => "retry_wait",
            Self::Paused => "paused",
            Self::PausedSlow => "paused_slow",
            Self::PausedHostKey => "paused_host_key",
            Self::PausedRestarting => "paused_restarting",
            Self::Verifying => "verifying",
            Self::Seeding => "seeding",
            Self::Complete => "complete",
            Self::Error => "error",
            Self::Removed => "removed",
            Self::StoppedResult => "stopped_result",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Complete | Self::Error | Self::Removed | Self::StoppedResult
        )
    }
}

/// The closed aria2-compatible status vocabulary.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Aria2Status {
    Active,
    Waiting,
    Paused,
    Error,
    Complete,
    Removed,
}

/// Every permitted aria2 wire status in canonical matrix order.
pub const ALL_ARIA2_STATUSES: &[Aria2Status] = &[
    Aria2Status::Active,
    Aria2Status::Waiting,
    Aria2Status::Paused,
    Aria2Status::Error,
    Aria2Status::Complete,
    Aria2Status::Removed,
];

impl Aria2Status {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Waiting => "waiting",
            Self::Paused => "paused",
            Self::Error => "error",
            Self::Complete => "complete",
            Self::Removed => "removed",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Error | Self::Complete | Self::Removed)
    }
}

impl fmt::Display for Aria2Status {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A non-secret description of credentials required before readmission.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CredentialKind {
    SourceUri,
    HttpAuthentication,
    ProxyAuthentication,
    FtpAuthentication,
    SftpAuthentication,
    PrivateKeyPassphrase,
}

/// One scheduler-owned credential admission blocker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialRequirement {
    pub kind: CredentialKind,
    pub source: Option<UriId>,
    pub safe_description: String,
}

/// One scheduler-owned disk-space admission blocker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NoSpaceCondition {
    pub redacted_path: String,
    pub retry_at: Option<MonotonicInstant>,
}

/// Recoverable admission blockers orthogonal to task state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TaskConditions {
    pub needs_credentials: Option<CredentialRequirement>,
    pub no_space: Option<NoSpaceCondition>,
}

impl TaskConditions {
    #[must_use]
    pub const fn blocks_admission(&self) -> bool {
        self.needs_credentials.is_some() || self.no_space.is_some()
    }

    #[must_use]
    pub const fn snapshot(&self) -> TaskConditionsSnapshot {
        TaskConditionsSnapshot {
            needs_credentials: self.needs_credentials.is_some(),
            no_space: self.no_space.is_some(),
        }
    }
}

/// Bounded condition flags included in immutable status snapshots.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TaskConditionsSnapshot {
    pub needs_credentials: bool,
    pub no_space: bool,
}

/// Context required to project internal state onto aria2's closed wire status.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WireProjection {
    pub conditions: TaskConditionsSnapshot,
    pub desired_paused: bool,
    pub retry_wait_holds_slot: bool,
    pub stopped_status: Option<Aria2Status>,
}

impl WireProjection {
    /// Projects one internal state without exposing internal state names.
    pub fn project(self, state: TaskState) -> Result<Aria2Status, WireProjectionError> {
        let terminal = match state {
            TaskState::Complete => Some(Aria2Status::Complete),
            TaskState::Error => Some(Aria2Status::Error),
            TaskState::Removed => Some(Aria2Status::Removed),
            TaskState::StoppedResult => {
                let status = self
                    .stopped_status
                    .ok_or(WireProjectionError::MissingStoppedStatus)?;
                if !status.is_terminal() {
                    return Err(WireProjectionError::InvalidStoppedStatus);
                }
                Some(status)
            }
            _ => None,
        };
        if let Some(status) = terminal {
            return Ok(status);
        }

        if self.conditions.no_space {
            return Ok(Aria2Status::Paused);
        }
        if self.conditions.needs_credentials {
            return Ok(if self.desired_paused {
                Aria2Status::Paused
            } else {
                Aria2Status::Waiting
            });
        }

        Ok(match state {
            TaskState::Accepted
            | TaskState::Waiting
            | TaskState::WaitingSlow
            | TaskState::Allocating
            | TaskState::PausedRestarting => Aria2Status::Waiting,
            TaskState::Active | TaskState::Verifying | TaskState::Seeding => Aria2Status::Active,
            TaskState::RetryWait if self.retry_wait_holds_slot => Aria2Status::Active,
            TaskState::RetryWait => Aria2Status::Waiting,
            TaskState::Paused | TaskState::PausedSlow | TaskState::PausedHostKey => {
                Aria2Status::Paused
            }
            TaskState::Complete
            | TaskState::Error
            | TaskState::Removed
            | TaskState::StoppedResult => unreachable!("terminal states returned above"),
        })
    }
}

/// An invalid attempt to project a retained stopped result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireProjectionError {
    MissingStoppedStatus,
    InvalidStoppedStatus,
}

impl fmt::Display for WireProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MissingStoppedStatus => "stopped result has no retained terminal status",
            Self::InvalidStoppedStatus => "stopped result retained a nonterminal status",
        })
    }
}

impl Error for WireProjectionError {}

/// The execution state of one planned transfer span inside an active task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlannedSpanState {
    Pending,
    Leased(LeaseId),
    RetryWait {
        until: MonotonicInstant,
        attempt: u32,
        error: ErrorKind,
    },
    Done,
}

#[cfg(test)]
mod tests {
    use super::{
        ALL_TASK_STATES, Aria2Status, TaskConditionsSnapshot, TaskState, WireProjection,
        WireProjectionError,
    };
    use std::collections::BTreeSet;

    #[test]
    fn task_state_codes_are_unique() {
        let codes: BTreeSet<_> = ALL_TASK_STATES.iter().map(|state| state.code()).collect();
        assert_eq!(codes.len(), ALL_TASK_STATES.len());
    }

    #[test]
    fn every_non_stopped_state_has_the_normative_wire_projection() {
        let projection = WireProjection::default();
        let cases = [
            (TaskState::Accepted, Aria2Status::Waiting),
            (TaskState::Waiting, Aria2Status::Waiting),
            (TaskState::WaitingSlow, Aria2Status::Waiting),
            (TaskState::Allocating, Aria2Status::Waiting),
            (TaskState::Active, Aria2Status::Active),
            (TaskState::RetryWait, Aria2Status::Waiting),
            (TaskState::Paused, Aria2Status::Paused),
            (TaskState::PausedSlow, Aria2Status::Paused),
            (TaskState::PausedHostKey, Aria2Status::Paused),
            (TaskState::PausedRestarting, Aria2Status::Waiting),
            (TaskState::Verifying, Aria2Status::Active),
            (TaskState::Seeding, Aria2Status::Active),
            (TaskState::Complete, Aria2Status::Complete),
            (TaskState::Error, Aria2Status::Error),
            (TaskState::Removed, Aria2Status::Removed),
        ];
        for (state, expected) in cases {
            assert_eq!(projection.project(state), Ok(expected), "{state:?}");
        }
    }

    #[test]
    fn retry_slot_and_conditions_control_projection_without_new_statuses() {
        assert_eq!(
            WireProjection {
                retry_wait_holds_slot: true,
                ..WireProjection::default()
            }
            .project(TaskState::RetryWait),
            Ok(Aria2Status::Active)
        );
        assert_eq!(
            WireProjection {
                conditions: TaskConditionsSnapshot {
                    no_space: true,
                    needs_credentials: false,
                },
                ..WireProjection::default()
            }
            .project(TaskState::Waiting),
            Ok(Aria2Status::Paused)
        );
        assert_eq!(
            WireProjection {
                conditions: TaskConditionsSnapshot {
                    no_space: false,
                    needs_credentials: true,
                },
                desired_paused: true,
                ..WireProjection::default()
            }
            .project(TaskState::Waiting),
            Ok(Aria2Status::Paused)
        );
    }

    #[test]
    fn stopped_results_require_a_terminal_retained_status() {
        assert_eq!(
            WireProjection::default().project(TaskState::StoppedResult),
            Err(WireProjectionError::MissingStoppedStatus)
        );
        assert_eq!(
            WireProjection {
                stopped_status: Some(Aria2Status::Waiting),
                ..WireProjection::default()
            }
            .project(TaskState::StoppedResult),
            Err(WireProjectionError::InvalidStoppedStatus)
        );
        assert_eq!(
            WireProjection {
                stopped_status: Some(Aria2Status::Complete),
                ..WireProjection::default()
            }
            .project(TaskState::StoppedResult),
            Ok(Aria2Status::Complete)
        );
    }
}
