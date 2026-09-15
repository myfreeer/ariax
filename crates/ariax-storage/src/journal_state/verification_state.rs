use super::*;
use crate::{
    MAX_VERIFICATION_MANIFEST_BYTES, VERIFICATION_MANIFEST_PART_BYTES, VerificationManifest,
};

pub(super) struct PendingManifest {
    fingerprint: JournalHash,
    total: usize,
    pub(super) count: u32,
    pub(super) next: u32,
    bytes: Vec<u8>,
}

impl<P: PersistedOptionPolicy + ?Sized> SemanticMachine<'_, P> {
    pub(super) fn apply_manifest(
        &mut self,
        record: &JournalRecord,
        fingerprint: JournalHash,
        total_bytes: u32,
        count: u32,
        bytes: Box<[u8]>,
    ) -> Result<(), JournalStateError> {
        self.require_current_generation(record)?;
        if self.network_started
            || self
                .state
                .as_ref()
                .is_none_or(|state| state.verification_manifest.is_some())
        {
            return Err(JournalStateError::InvalidVerificationManifest);
        }
        let total = total_bytes as usize;
        if total > MAX_VERIFICATION_MANIFEST_BYTES
            || total < bytes.len()
            || count as usize != total.div_ceil(VERIFICATION_MANIFEST_PART_BYTES)
        {
            return Err(JournalStateError::InvalidVerificationManifest);
        }
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(total)
            .map_err(|_| JournalStateError::AllocationFailed)?;
        buffer.extend_from_slice(&bytes);
        self.pending_manifest = Some(PendingManifest {
            fingerprint,
            total,
            count,
            next: 1,
            bytes: buffer,
        });
        self.finish_manifest()
    }

    pub(super) fn apply_manifest_chunk(
        &mut self,
        record: &JournalRecord,
        fingerprint: JournalHash,
        index: u32,
        count: u32,
        bytes: Box<[u8]>,
    ) -> Result<(), JournalStateError> {
        self.require_current_generation(record)?;
        let pending = self
            .pending_manifest
            .as_mut()
            .ok_or(JournalStateError::InvalidVerificationManifest)?;
        if pending.fingerprint != fingerprint
            || pending.next != index
            || pending.count != count
            || bytes.len()
                != (pending.total - pending.bytes.len()).min(VERIFICATION_MANIFEST_PART_BYTES)
        {
            return Err(JournalStateError::InvalidVerificationManifest);
        }
        pending.bytes.extend_from_slice(&bytes);
        pending.next += 1;
        self.finish_manifest()
    }

    fn finish_manifest(&mut self) -> Result<(), JournalStateError> {
        let pending = self
            .pending_manifest
            .as_ref()
            .ok_or(JournalStateError::InvalidVerificationManifest)?;
        if pending.next != pending.count {
            return Ok(());
        }
        if pending.bytes.len() != pending.total
            || VerificationManifest::hash_bytes(&pending.bytes) != pending.fingerprint
        {
            return Err(JournalStateError::InvalidVerificationManifest);
        }
        let manifest = VerificationManifest::decode(&pending.bytes)
            .map_err(|_| JournalStateError::InvalidVerificationManifest)?;
        if manifest.chunks().len() > self.limits.max_durable_pieces {
            return Err(JournalStateError::ResourceLimit(
                JournalStateResource::DurablePieces,
            ));
        }
        let state = self
            .state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        if let Some(expected) = state.current_options.as_ref().and_then(|snapshot| {
            snapshot
                .options
                .entries()
                .find_map(|(key, value)| (key == "verification-manifest").then_some(value))
        }) && expected != manifest.fingerprint().to_string()
        {
            return Err(JournalStateError::InvalidVerificationManifest);
        }
        if let Some(layout) = &state.layout
            && (layout.layout().total_length() != Some(manifest.total_length())
                || layout.layout().piece_length() != manifest.chunk_length())
        {
            return Err(JournalStateError::InvalidVerificationManifest);
        }
        state.verification_manifest = Some(std::sync::Arc::new(manifest));
        self.pending_manifest = None;
        Ok(())
    }

    pub(super) fn require_manifest_ready(&self) -> Result<(), JournalStateError> {
        let state = self
            .state
            .as_ref()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        let required = state.current_options.as_ref().and_then(|snapshot| {
            snapshot
                .options
                .entries()
                .find_map(|(key, value)| (key == "verification-manifest").then_some(value))
        });
        if self.pending_manifest.is_some()
            || required.is_some_and(|expected| {
                state
                    .verification_manifest
                    .as_ref()
                    .is_none_or(|manifest| expected != manifest.fingerprint().to_string())
            })
        {
            return Err(JournalStateError::InvalidVerificationManifest);
        }
        if let Some(manifest) = &state.verification_manifest {
            let layout = self.require_layout()?;
            if layout.layout().total_length() != Some(manifest.total_length())
                || layout.layout().piece_length() != manifest.chunk_length()
            {
                return Err(JournalStateError::InvalidVerificationManifest);
            }
        }
        Ok(())
    }

    pub(super) fn apply_protocol_validator(
        &mut self,
        record: &JournalRecord,
        validator: crate::ProtocolValidator,
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        let state = self
            .state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        if !validator.validate()
            || state
                .layout
                .as_ref()
                .and_then(|layout| layout.layout().total_length())
                != Some(validator.total_length)
            || state
                .protocol_validators
                .get(&validator.source)
                .is_some_and(|old| old != &validator)
            || (state.protocol_validators.len() >= 1024
                && !state.protocol_validators.contains_key(&validator.source))
        {
            return Err(JournalStateError::InvalidProtocolValidator);
        }
        state
            .protocol_validators
            .insert(validator.source, validator);
        Ok(())
    }

    pub(super) fn apply_whole_verified(
        &mut self,
        record: &JournalRecord,
        fingerprint: JournalHash,
        digests: &[JournalDigest],
    ) -> Result<(), JournalStateError> {
        self.require_ready_nonterminal(record)?;
        self.require_manifest_ready()?;
        let state = self
            .state
            .as_mut()
            .ok_or(JournalStateError::TaskCreatedMissing)?;
        let manifest = state
            .verification_manifest
            .as_ref()
            .ok_or(JournalStateError::InvalidVerificationManifest)?;
        let pieces = manifest.total_length().div_ceil(manifest.chunk_length());
        if fingerprint != manifest.fingerprint()
            || digests != manifest.whole()
            || state.durable_pieces.len() as u64 != pieces
            || !self.active_leases.is_empty()
        {
            return Err(JournalStateError::VerificationMismatch);
        }
        state.whole_file_verified = true;
        Ok(())
    }
}
