use crate::{
    Aria2Status, Generation, Gid, HostKeyChallengeId, HostKeyFingerprint, MonotonicInstant,
    PublicError, TaskConditionsSnapshot, TaskState, WireProjection, WireProjectionError,
};
use std::error::Error;
use std::fmt;

/// Hard bound for one presented SSH host key retained until explicit approval.
pub const MAX_PRESENTED_HOST_KEY_BYTES: usize = 16 * 1024;
pub const MAX_HOST_KEY_CANONICAL_HOST_BYTES: usize = 253;
pub const MAX_HOST_KEY_ALGORITHM_BYTES: usize = 64;

/// A bounded, non-secret SFTP host-key challenge exposed for explicit approval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostKeyChallenge {
    pub id: HostKeyChallengeId,
    pub canonical_host: String,
    pub port: u16,
    pub algorithm: String,
    pub fingerprint_sha256: HostKeyFingerprint,
}

/// Exact bounded key material paired with the non-secret challenge summary.
/// Snapshots expose only `summary`; the raw key stays inside scheduler and
/// persistence effects so approval pins the key that was actually displayed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PresentedHostKeyChallenge {
    summary: HostKeyChallenge,
    presented_public_key: Vec<u8>,
}

impl PresentedHostKeyChallenge {
    pub fn new(
        summary: HostKeyChallenge,
        presented_public_key: Vec<u8>,
    ) -> Result<Self, PresentedHostKeyChallengeError> {
        if presented_public_key.is_empty() {
            return Err(PresentedHostKeyChallengeError::EmptyKey);
        }
        if presented_public_key.len() > MAX_PRESENTED_HOST_KEY_BYTES {
            return Err(PresentedHostKeyChallengeError::KeyTooLarge);
        }
        if summary.canonical_host.is_empty() {
            return Err(PresentedHostKeyChallengeError::EmptyCanonicalHost);
        }
        if summary.canonical_host.len() > MAX_HOST_KEY_CANONICAL_HOST_BYTES {
            return Err(PresentedHostKeyChallengeError::CanonicalHostTooLong);
        }
        if summary.port == 0 {
            return Err(PresentedHostKeyChallengeError::InvalidPort);
        }
        if summary.algorithm.is_empty() {
            return Err(PresentedHostKeyChallengeError::EmptyAlgorithm);
        }
        if summary.algorithm.len() > MAX_HOST_KEY_ALGORITHM_BYTES {
            return Err(PresentedHostKeyChallengeError::AlgorithmTooLong);
        }
        if summary.fingerprint_sha256
            != HostKeyFingerprint::for_presented_key(&presented_public_key)
        {
            return Err(PresentedHostKeyChallengeError::FingerprintMismatch);
        }
        Ok(Self {
            summary,
            presented_public_key,
        })
    }

    #[must_use]
    pub const fn summary(&self) -> &HostKeyChallenge {
        &self.summary
    }

    #[must_use]
    pub fn presented_public_key(&self) -> &[u8] {
        &self.presented_public_key
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PresentedHostKeyChallengeError {
    EmptyKey,
    KeyTooLarge,
    EmptyCanonicalHost,
    CanonicalHostTooLong,
    InvalidPort,
    EmptyAlgorithm,
    AlgorithmTooLong,
    FingerprintMismatch,
}

impl fmt::Display for PresentedHostKeyChallengeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyKey => "presented host key is empty",
            Self::KeyTooLarge => "presented host key exceeds the scheduler bound",
            Self::EmptyCanonicalHost => "host-key canonical host is empty",
            Self::CanonicalHostTooLong => "host-key canonical host exceeds the scheduler bound",
            Self::InvalidPort => "host-key port zero is invalid",
            Self::EmptyAlgorithm => "host-key algorithm is empty",
            Self::AlgorithmTooLong => "host-key algorithm exceeds the scheduler bound",
            Self::FingerprintMismatch => "host-key fingerprint does not match the presented key",
        })
    }
}

impl Error for PresentedHostKeyChallengeError {}

/// One immutable status snapshot consumed by CLI, RPC, and library APIs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskSnapshot {
    pub gid: Gid,
    pub state: TaskState,
    pub generation: Generation,
    pub total_length: Option<u64>,
    pub completed_length: u64,
    pub durable_length: u64,
    pub current_speed: u64,
    pub average_speed: u64,
    pub active_leases: u32,
    pub retry_wait_leases: u32,
    pub retry_wait_until: Option<MonotonicInstant>,
    pub last_progress_at: Option<MonotonicInstant>,
    pub conditions: TaskConditionsSnapshot,
    pub desired_paused: bool,
    pub retry_wait_holds_slot: bool,
    pub stopped_status: Option<Aria2Status>,
    pub host_key_challenge: Option<HostKeyChallenge>,
    pub error: Option<PublicError>,
    pub terminal_persisted: bool,
}

impl TaskSnapshot {
    /// Derives the public aria2 status from scheduler-owned state and context.
    pub fn wire_status(&self) -> Result<Aria2Status, WireProjectionError> {
        let status = WireProjection {
            conditions: self.conditions,
            desired_paused: self.desired_paused,
            retry_wait_holds_slot: self.retry_wait_holds_slot,
            stopped_status: self.stopped_status,
            terminal_persisted: self.terminal_persisted,
        }
        .project(self.state)?;
        if self.state.is_terminal_pending() {
            return Err(WireProjectionError::TerminalPendingRetention);
        }
        Ok(status)
    }

    /// Validates invariants required before a snapshot can be published.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.durable_length > self.completed_length {
            return Err("durable length exceeds completed length");
        }
        if let Some(total_length) = self.total_length
            && self.completed_length > total_length
        {
            return Err("completed length exceeds total length");
        }
        if self.state == TaskState::PausedHostKey && self.host_key_challenge.is_none() {
            return Err("paused host-key state has no challenge");
        }
        if self.state != TaskState::PausedHostKey && self.host_key_challenge.is_some() {
            return Err("host-key challenge is visible outside paused host-key state");
        }
        if (self.state == TaskState::Error
            || (self.state == TaskState::StoppedResult
                && self.stopped_status == Some(Aria2Status::Error)))
            && self.error.is_none()
        {
            return Err("error terminal result has no public error");
        }
        if self.state != TaskState::StoppedResult && self.stopped_status.is_some() {
            return Err("retained terminal status is visible outside stopped-result state");
        }
        if self.state.is_terminal_pending() {
            return Err(if self.terminal_persisted {
                "terminal-pending snapshot must be retained before publication"
            } else {
                "terminal snapshot is not persistence-acknowledged"
            });
        }
        if self.state.is_retained_result() != self.terminal_persisted {
            return Err(if self.state.is_retained_result() {
                "stopped-result snapshot is not persistence-acknowledged"
            } else {
                "nonterminal snapshot claims terminal persistence"
            });
        }
        self.wire_status()
            .map_err(|_| "snapshot state cannot be projected to a wire status")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_HOST_KEY_ALGORITHM_BYTES, MAX_HOST_KEY_CANONICAL_HOST_BYTES,
        MAX_PRESENTED_HOST_KEY_BYTES, PresentedHostKeyChallenge, PresentedHostKeyChallengeError,
        TaskSnapshot,
    };
    use crate::{
        Aria2Status, Generation, Gid, HostKeyChallenge, HostKeyChallengeId, HostKeyFingerprint,
        TaskConditionsSnapshot, TaskState, WireProjectionError,
    };

    fn snapshot() -> TaskSnapshot {
        TaskSnapshot {
            gid: Gid::new(1).expect("GID"),
            state: TaskState::Active,
            generation: Generation::INITIAL,
            total_length: Some(100),
            completed_length: 50,
            durable_length: 40,
            current_speed: 10,
            average_speed: 8,
            active_leases: 1,
            retry_wait_leases: 0,
            retry_wait_until: None,
            last_progress_at: None,
            conditions: TaskConditionsSnapshot::default(),
            desired_paused: false,
            retry_wait_holds_slot: false,
            stopped_status: None,
            host_key_challenge: None,
            error: None,
            terminal_persisted: false,
        }
    }

    #[test]
    fn valid_snapshot_passes_contract_checks() {
        let value = snapshot();
        assert_eq!(value.validate(), Ok(()));
        assert_eq!(value.wire_status(), Ok(Aria2Status::Active));
    }

    #[test]
    fn snapshot_rejects_impossible_progress() {
        let mut value = snapshot();
        value.durable_length = 51;
        assert_eq!(
            value.validate(),
            Err("durable length exceeds completed length")
        );
        value.durable_length = 40;
        value.completed_length = 101;
        assert_eq!(
            value.validate(),
            Err("completed length exceeds total length")
        );
    }

    #[test]
    fn snapshot_derives_wire_projection_and_gates_terminal_visibility() {
        let mut value = snapshot();
        value.state = TaskState::PausedRestarting;
        value.conditions.no_space = true;
        assert_eq!(
            value.validate(),
            Err("snapshot state cannot be projected to a wire status")
        );

        value = snapshot();
        value.state = TaskState::Complete;
        assert_eq!(
            value.validate(),
            Err("terminal snapshot is not persistence-acknowledged")
        );
        value.terminal_persisted = true;
        assert_eq!(
            value.validate(),
            Err("terminal-pending snapshot must be retained before publication")
        );
        assert_eq!(
            value.wire_status(),
            Err(WireProjectionError::TerminalPendingRetention)
        );

        value.state = TaskState::StoppedResult;
        value.stopped_status = Some(Aria2Status::Complete);
        assert_eq!(value.validate(), Ok(()));
        assert_eq!(value.wire_status(), Ok(Aria2Status::Complete));

        value = snapshot();
        value.state = TaskState::RetryWait;
        value.retry_wait_holds_slot = true;
        assert_eq!(value.wire_status(), Ok(Aria2Status::Active));
    }

    #[test]
    fn retained_error_requires_the_public_error_payload() {
        let mut value = snapshot();
        value.state = TaskState::StoppedResult;
        value.stopped_status = Some(Aria2Status::Error);
        value.terminal_persisted = true;
        assert_eq!(
            value.validate(),
            Err("error terminal result has no public error")
        );
    }

    #[test]
    fn presented_host_keys_enforce_exact_bounded_identity() {
        let key = vec![7_u8; MAX_PRESENTED_HOST_KEY_BYTES];
        let valid = HostKeyChallenge {
            id: HostKeyChallengeId::new([1; 16]),
            canonical_host: "h".repeat(MAX_HOST_KEY_CANONICAL_HOST_BYTES),
            port: u16::MAX,
            algorithm: "a".repeat(MAX_HOST_KEY_ALGORITHM_BYTES),
            fingerprint_sha256: HostKeyFingerprint::for_presented_key(&key),
        };
        assert!(PresentedHostKeyChallenge::new(valid.clone(), key.clone()).is_ok());

        let cases = [
            (
                valid.clone(),
                Vec::new(),
                PresentedHostKeyChallengeError::EmptyKey,
            ),
            (
                valid.clone(),
                vec![0; MAX_PRESENTED_HOST_KEY_BYTES + 1],
                PresentedHostKeyChallengeError::KeyTooLarge,
            ),
            (
                HostKeyChallenge {
                    canonical_host: String::new(),
                    ..valid.clone()
                },
                key.clone(),
                PresentedHostKeyChallengeError::EmptyCanonicalHost,
            ),
            (
                HostKeyChallenge {
                    canonical_host: "h".repeat(MAX_HOST_KEY_CANONICAL_HOST_BYTES + 1),
                    ..valid.clone()
                },
                key.clone(),
                PresentedHostKeyChallengeError::CanonicalHostTooLong,
            ),
            (
                HostKeyChallenge {
                    port: 0,
                    ..valid.clone()
                },
                key.clone(),
                PresentedHostKeyChallengeError::InvalidPort,
            ),
            (
                HostKeyChallenge {
                    algorithm: String::new(),
                    ..valid.clone()
                },
                key.clone(),
                PresentedHostKeyChallengeError::EmptyAlgorithm,
            ),
            (
                HostKeyChallenge {
                    algorithm: "a".repeat(MAX_HOST_KEY_ALGORITHM_BYTES + 1),
                    ..valid.clone()
                },
                key.clone(),
                PresentedHostKeyChallengeError::AlgorithmTooLong,
            ),
            (
                HostKeyChallenge {
                    fingerprint_sha256: HostKeyFingerprint::new([9; 32]),
                    ..valid
                },
                key,
                PresentedHostKeyChallengeError::FingerprintMismatch,
            ),
        ];
        for (summary, presented_public_key, expected) in cases {
            assert_eq!(
                PresentedHostKeyChallenge::new(summary, presented_public_key),
                Err(expected)
            );
        }
    }
}
