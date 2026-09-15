//! Bounded worker-to-control handoff for atomic metadata expansion.
use crate::{HttpControlError, HttpIngressPermit};
use ariax_storage::{MetalinkExpansion, MetalinkParent};
use serde_json::Value;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot;
#[derive(Clone, Default)]
pub struct MetalinkFollowQueue(Arc<Mutex<FollowQueueState>>);
#[derive(Default)]
struct FollowQueueState {
    closed: bool,
    requests: VecDeque<FollowRequest>,
}
impl std::fmt::Debug for MetalinkFollowQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetalinkFollowQueue")
            .finish_non_exhaustive()
    }
}
pub(crate) struct FollowRequest {
    pub parent: MetalinkParent,
    pub params: Value,
    pub reply: oneshot::Sender<Result<MetalinkExpansion, HttpControlError>>,
    pub _metadata: HttpIngressPermit,
}
impl MetalinkFollowQueue {
    pub(crate) fn push(&self, request: FollowRequest) -> Result<(), HttpControlError> {
        let mut queue = self.0.lock().expect("Metalink handoff");
        if queue.closed || queue.requests.len() >= 64 {
            return Err(HttpControlError::Busy);
        }
        queue.requests.push_back(request);
        Ok(())
    }
    pub(crate) fn pop(&self) -> Option<FollowRequest> {
        self.0
            .lock()
            .expect("Metalink handoff")
            .requests
            .pop_front()
    }
    pub(crate) fn close(&self) {
        let mut state = self.0.lock().expect("Metalink handoff");
        state.closed = true;
        for request in state.requests.drain(..) {
            let _ = request.reply.send(Err(HttpControlError::Busy));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_handoff_refunds_rejected_and_closed_requests() {
        let queue = MetalinkFollowQueue::default();
        let budget = crate::HttpIngressBudgets::new(65);
        let request = || {
            let (reply, receive) = oneshot::channel();
            (
                FollowRequest {
                    parent: MetalinkParent {
                        gid: ariax_core::Gid::new(1).unwrap(),
                        generation: ariax_core::Generation::INITIAL,
                        snapshot_hash: ariax_storage::JournalHash::new([1; 32]).unwrap(),
                        document_hash: ariax_storage::JournalHash::new([2; 32]).unwrap(),
                        document_bytes: 1,
                        retained: false,
                    },
                    params: Value::Null,
                    reply,
                    _metadata: budget.try_acquire(1).unwrap(),
                },
                receive,
            )
        };
        let mut replies = Vec::new();
        for _ in 0..64 {
            let (request, reply) = request();
            queue.push(request).unwrap();
            replies.push(reply);
        }
        assert_eq!(budget.used(), 64);
        assert!(queue.push(request().0).is_err());
        assert_eq!(budget.used(), 64);
        let accepted = queue.pop().unwrap();
        queue.close();
        assert_eq!(budget.used(), 1);
        assert!(queue.pop().is_none());
        assert!(queue.push(request().0).is_err());
        assert!(replies[1].try_recv().unwrap().is_err());
        drop(accepted);
        assert_eq!(budget.used(), 0);
    }
}
