use crate::BtError;
use ariax_runtime::{ByteBudget, BytePermit};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BridgeLimits {
    pub commands: usize,
    pub command_bytes: usize,
    pub events: usize,
    pub event_bytes: usize,
    pub completions: usize,
    pub completion_bytes: usize,
    pub blob_bytes: usize,
}

impl Default for BridgeLimits {
    fn default() -> Self {
        Self {
            commands: 256,
            command_bytes: 8 * 1024 * 1024,
            events: 1024,
            event_bytes: 16 * 1024 * 1024,
            completions: 64,
            completion_bytes: 4 * 1024 * 1024,
            blob_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BridgeBudget {
    commands: ByteBudget,
    command_bytes: ByteBudget,
    completions: ByteBudget,
    completion_bytes: ByteBudget,
    blobs: ByteBudget,
    resident: ByteBudget,
}

/// A command keeps all of its reservations until execution and delivery settle.
#[derive(Debug)]
pub struct BridgeLease {
    _command: BytePermit,
    _command_bytes: BytePermit,
    _completion: BytePermit,
    _completion_bytes: BytePermit,
    _resident: BytePermit,
}

#[derive(Debug)]
pub struct OwnedBlob {
    data: Vec<u8>,
    _domain: BytePermit,
    _resident: BytePermit,
}

impl OwnedBlob {
    pub fn bytes(&self) -> &[u8] {
        &self.data
    }
}

impl BridgeBudget {
    pub fn new(limits: BridgeLimits, resident: ByteBudget) -> Result<Self, BtError> {
        let ceiling = BridgeLimits::default();
        for (actual, maximum) in [
            (limits.commands, ceiling.commands),
            (limits.command_bytes, ceiling.command_bytes),
            (limits.completions, ceiling.completions),
            (limits.completion_bytes, ceiling.completion_bytes),
            (limits.events, ceiling.events),
            (limits.event_bytes, ceiling.event_bytes),
        ] {
            if actual == 0 || actual > maximum {
                return Err(BtError::Overloaded);
            }
        }
        if limits.blob_bytes == 0 || limits.blob_bytes > 256 * 1024 * 1024 {
            return Err(BtError::Overloaded);
        }
        Ok(Self {
            commands: ByteBudget::new(limits.commands),
            command_bytes: ByteBudget::new(limits.command_bytes),
            completions: ByteBudget::new(limits.completions),
            completion_bytes: ByteBudget::new(limits.completion_bytes),
            blobs: ByteBudget::new(limits.blob_bytes),
            resident,
        })
    }

    pub fn reserve(
        &self,
        command_bytes: usize,
        completion_bytes: usize,
    ) -> Result<BridgeLease, BtError> {
        let command_bytes = command_bytes.max(1);
        let completion_bytes = completion_bytes.max(1);
        Ok(BridgeLease {
            _command: self
                .commands
                .try_acquire(1)
                .map_err(|_| BtError::Overloaded)?,
            _command_bytes: self
                .command_bytes
                .try_acquire(command_bytes)
                .map_err(|_| BtError::Overloaded)?,
            _completion: self
                .completions
                .try_acquire(1)
                .map_err(|_| BtError::Overloaded)?,
            _completion_bytes: self
                .completion_bytes
                .try_acquire(completion_bytes)
                .map_err(|_| BtError::Overloaded)?,
            _resident: self
                .resident
                .try_acquire(
                    command_bytes
                        .checked_add(completion_bytes)
                        .ok_or(BtError::Overloaded)?,
                )
                .map_err(|_| BtError::Overloaded)?,
        })
    }

    pub fn blob(&self, data: Vec<u8>) -> Result<OwnedBlob, BtError> {
        // Includes the original buffer, FFI copies and bounded parser nodes.
        let capacity = data.capacity();
        let bytes = capacity
            .checked_mul(3)
            .and_then(|value| value.checked_add((data.len() / 2).min(200_000) * 64))
            .ok_or(BtError::Overloaded)?
            .max(4096);
        Ok(OwnedBlob {
            _domain: self
                .blobs
                .try_acquire(bytes)
                .map_err(|_| BtError::Overloaded)?,
            _resident: self
                .resident
                .try_acquire(bytes)
                .map_err(|_| BtError::Overloaded)?,
            data,
        })
    }

    #[cfg(any(feature = "libtorrent", test))]
    pub(crate) fn reserve_blob_output(
        &self,
        capacity: usize,
    ) -> Result<(BytePermit, BytePermit), BtError> {
        let bytes = capacity
            .checked_mul(3)
            .and_then(|value| value.checked_add((capacity / 2).min(200_000) * 64))
            .ok_or(BtError::Overloaded)?
            .max(4096);
        Ok((
            self.blobs
                .try_acquire(bytes)
                .map_err(|_| BtError::Overloaded)?,
            self.resident
                .try_acquire(bytes)
                .map_err(|_| BtError::Overloaded)?,
        ))
    }

    #[cfg(any(feature = "libtorrent", test))]
    pub(crate) fn finish_blob(data: Vec<u8>, mut permits: (BytePermit, BytePermit)) -> OwnedBlob {
        let retained =
            (data.capacity().saturating_mul(3) + (data.len() / 2).min(200_000) * 64).max(4096);
        // The native output writer must honor the reserved cap before growing.
        assert!(retained <= permits.0.bytes() && retained <= permits.1.bytes());
        permits
            .0
            .shrink_to(retained)
            .expect("output fits its reservation");
        permits
            .1
            .shrink_to(retained)
            .expect("output fits its reservation");
        OwnedBlob {
            data,
            _domain: permits.0,
            _resident: permits.1,
        }
    }

    pub fn pending(&self) -> usize {
        self.commands.used()
    }
    pub fn blob_reservations(&self) -> usize {
        self.blobs.used()
    }
}

/// `None` is unlimited. `Some(0)` means that group's payload admission stops.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BandwidthAllocation {
    pub bt: Option<u64>,
    pub transfer: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BandwidthGroup {
    Bt,
    Transfer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BandwidthUpdate {
    pub group: BandwidthGroup,
    pub limit: Option<u64>,
}

pub fn split_bandwidth(
    total: Option<u64>,
    bt_active: bool,
    transfer_active: bool,
) -> BandwidthAllocation {
    let allocation = match total {
        None => (None, None),
        Some(limit) => match (bt_active, transfer_active) {
            (true, true) => (Some(limit / 2), Some(limit - limit / 2)),
            (true, false) => (Some(limit), Some(0)),
            (false, true) => (Some(0), Some(limit)),
            (false, false) => (Some(0), Some(0)),
        },
    };
    BandwidthAllocation {
        bt: allocation.0,
        transfer: allocation.1,
    }
}

/// Apply every reduction and wait for acknowledgement before any increase.
pub fn bandwidth_updates(
    previous: BandwidthAllocation,
    next: BandwidthAllocation,
) -> Vec<BandwidthUpdate> {
    let mut reductions = Vec::new();
    let mut increases = Vec::new();
    for (group, before, after) in [
        (BandwidthGroup::Bt, previous.bt, next.bt),
        (BandwidthGroup::Transfer, previous.transfer, next.transfer),
    ] {
        if before == after {
            continue;
        }
        let update = BandwidthUpdate {
            group,
            limit: after,
        };
        if after.unwrap_or(u64::MAX) < before.unwrap_or(u64::MAX) {
            reductions.push(update);
        } else {
            increases.push(update);
        }
    }
    reductions.extend(increases);
    reductions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_work_retains_credit_after_caller_disconnect_until_completion_settles() {
        let resident = ByteBudget::new(1024 * 1024);
        let budget = BridgeBudget::new(
            BridgeLimits {
                commands: 1,
                ..BridgeLimits::default()
            },
            resident.clone(),
        )
        .unwrap();
        let lease = budget.reserve(1024, 128).unwrap();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        drop(receiver);
        assert_eq!(budget.pending(), 1);
        assert!(budget.reserve(1, 1).is_err());
        let failed = sender.send(lease).unwrap_err();
        assert_eq!(resident.used(), 1152);
        drop(failed);
        assert_eq!(budget.pending(), 0);
        assert_eq!(resident.used(), 0);
        assert!(budget.reserve(1, 1).is_ok());
    }

    #[test]
    fn byte_and_completion_limits_reject_atomically_and_blobs_have_separate_ownership() {
        let resident = ByteBudget::new(1024 * 1024);
        let budget = BridgeBudget::new(
            BridgeLimits {
                completion_bytes: 128,
                ..BridgeLimits::default()
            },
            resident.clone(),
        )
        .unwrap();
        assert!(budget.reserve(1024, 129).is_err());
        assert_eq!(resident.used(), 0);
        assert_eq!(budget.pending(), 0);
        let blob = budget.blob(vec![0; 1024]).unwrap();
        assert_eq!(blob.bytes().len(), 1024);
        assert!(budget.blob_reservations() > 1024);
        drop(blob);
        assert_eq!(resident.used(), 0);
        let permits = budget.reserve_blob_output(4096).unwrap();
        let output = BridgeBudget::finish_blob(vec![1; 2048], permits);
        assert_eq!(output.bytes().len(), 2048);
        drop(output);
        assert_eq!(resident.used(), 0);
    }

    #[test]
    fn every_finite_share_and_transition_preserves_the_total_cap() {
        for total in 1..4096 {
            let both = split_bandwidth(Some(total), true, true);
            assert_eq!(both.bt.unwrap() + both.transfer.unwrap(), total);
            assert!(both.bt.unwrap().abs_diff(both.transfer.unwrap()) <= 1);
            for (bt, transfer) in [(true, false), (false, true), (true, true), (false, false)] {
                let next = split_bandwidth(Some(total), bt, transfer);
                let mut current = both;
                for update in bandwidth_updates(current, next) {
                    match update.group {
                        BandwidthGroup::Bt => current.bt = update.limit,
                        BandwidthGroup::Transfer => current.transfer = update.limit,
                    }
                    assert!(current.bt.unwrap() + current.transfer.unwrap() <= total);
                }
                assert_eq!(current, next);
            }
        }
    }
}
