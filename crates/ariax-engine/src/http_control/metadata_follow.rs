use super::*;
pub(super) struct PendingFollow {
    request: crate::metadata_follow::FollowRequest,
    result: oneshot::Receiver<Result<Value, HttpControlError>>,
}
impl HttpControlPlane {
    pub(super) fn poll_metadata_follow(&mut self) -> Result<(), HttpControlError> {
        if let Some(mut pending) = self.pending_follow.take() {
            let result = match pending.result.try_recv() {
                Ok(result) => result,
                Err(oneshot::error::TryRecvError::Empty) => {
                    self.pending_follow = Some(pending);
                    return Ok(());
                }
                Err(_) => Err(HttpControlError::Persistence(
                    "metadata admission stopped".into(),
                )),
            };
            let result = result.and_then(|value| {
                let children = value
                    .as_array()
                    .ok_or(HttpControlError::InvalidConfig)?
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .and_then(|text| text.parse().ok())
                            .ok_or(HttpControlError::InvalidConfig)
                    })
                    .collect::<Result<Vec<Gid>, _>>()?;
                let expansion = ariax_storage::MetadataExpansion {
                    parent: pending.request.parent,
                    children,
                };
                if !expansion.validate() {
                    return Err(HttpControlError::InvalidConfig);
                }
                let spec = self
                    .tasks
                    .get_gid(expansion.parent.gid)
                    .ok_or(HttpControlError::NotFound)?;
                if spec.options().transfer.metadata_expansion.as_ref() != Some(&expansion) {
                    return Err(HttpControlError::InvalidConfig);
                }
                Ok(expansion)
            });
            let _ = pending.request.reply.send(result);
            self.turn.mark_progress();
            return Ok(());
        }
        if self.pending_admission.is_some()
            || self.pending_configuration.is_some()
            || self.pending_mutation.is_some()
            || !self.engine_idle()
            || !self.pending_source_replacements.is_empty()
        {
            return Ok(());
        }
        let Some(request) = self
            .metadata_follow
            .as_ref()
            .and_then(crate::MetadataFollowQueue::pop)
        else {
            return Ok(());
        };
        if self.shutdown_requested
            || self
                .engine
                .scheduler()
                .task(request.parent.gid)
                .is_none_or(|task| {
                    task.generation != request.parent.generation
                        || task.state != ariax_core::TaskState::Active
                        || task.pending_barrier.is_some()
                })
        {
            let _ = request.reply.send(Err(HttpControlError::Busy));
            return Ok(());
        }
        let admission = self
            .direct_client
            .try_request(0)
            .map_err(|_| HttpControlError::Busy);
        let mut request = request;
        let result = admission.and_then(|admission| {
            self.begin_admission(
                std::mem::take(&mut request.params),
                admission,
                match request.kind {
                    crate::metadata_follow::MetadataKind::Metalink => {
                        admission::AdmissionKind::Follow(request.parent)
                    }
                    crate::metadata_follow::MetadataKind::BitTorrent => {
                        admission::AdmissionKind::FollowTorrent(request.parent)
                    }
                },
                false,
            )
        });
        match result {
            Ok(ControlReply::Deferred(result)) => {
                self.pending_follow = Some(PendingFollow { request, result })
            }
            Err(error) => {
                let _ = request.reply.send(Err(error));
            }
            Ok(ControlReply::Ready(_)) => return Err(HttpControlError::InvalidConfig),
        }
        self.turn.mark_progress();
        Ok(())
    }
}
