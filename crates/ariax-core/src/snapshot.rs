use crate::{
    Aria2Status, Generation, Gid, HostKeyChallengeId, HostKeyFingerprint, MonotonicInstant,
    PublicError, TaskConditionsSnapshot, TaskState,
};

/// A bounded, non-secret SFTP host-key challenge exposed for explicit approval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostKeyChallenge {
    pub id: HostKeyChallengeId,
    pub canonical_host: String,
    pub port: u16,
    pub algorithm: String,
    pub fingerprint_sha256: HostKeyFingerprint,
}

/// One immutable status snapshot consumed by CLI, RPC, and library APIs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskSnapshot {
    pub gid: Gid,
    pub state: TaskState,
    pub wire_status: Aria2Status,
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
    pub host_key_challenge: Option<HostKeyChallenge>,
    pub error: Option<PublicError>,
}

impl TaskSnapshot {
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
        if self.state == TaskState::Error && self.error.is_none() {
            return Err("error state has no public error");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::TaskSnapshot;
    use crate::{Aria2Status, Generation, Gid, TaskConditionsSnapshot, TaskState};

    fn snapshot() -> TaskSnapshot {
        TaskSnapshot {
            gid: Gid::new(1).expect("GID"),
            state: TaskState::Active,
            wire_status: Aria2Status::Active,
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
            host_key_challenge: None,
            error: None,
        }
    }

    #[test]
    fn valid_snapshot_passes_contract_checks() {
        assert_eq!(snapshot().validate(), Ok(()));
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
}
