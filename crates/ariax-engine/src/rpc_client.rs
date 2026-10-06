//! Connection-local authorization and ownership of pushed event subscriptions.

use crate::{RpcEventBroker, RpcEventDelivery, RpcEventError, RpcEventLimits, RpcEventSubscriber};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct RpcClientContext {
    state: Arc<Mutex<ClientState>>,
    request: Option<crate::rpc_budget::RpcRequestLease>,
    retained_results: Option<Arc<Mutex<crate::rpc_budget::RpcAllocation>>>,
    local_admin: bool,
    #[cfg(feature = "control-diagnostics")]
    pub(crate) source_trace: Option<crate::SourceMutationTrace>,
}

#[derive(Default)]
struct ClientState {
    authenticated: bool,
    events: Option<RpcEventBroker>,
    subscriber: Option<RpcEventSubscriber>,
    client: Option<crate::RpcClientBudget>,
}

impl RpcClientContext {
    /// Attaches bounded local timing to this request, without changing authorization.
    #[cfg(feature = "control-diagnostics")]
    #[must_use]
    pub fn with_source_trace(mut self, trace: crate::SourceMutationTrace) -> Self {
        self.source_trace = Some(trace);
        self
    }

    pub(crate) fn local() -> Self {
        Self {
            local_admin: true,
            ..Self::default()
        }
    }

    pub(crate) fn is_local_admin(&self) -> bool {
        self.local_admin
    }

    #[cfg(test)]
    pub(crate) fn with_events(
        events: RpcEventBroker,
        authentication_required: bool,
    ) -> Result<Self, RpcEventError> {
        let client = events.client_budget()?;
        Self::with_events_and_budget(events, authentication_required, client)
    }

    pub(crate) fn with_events_and_budget(
        events: RpcEventBroker,
        authentication_required: bool,
        client: crate::RpcClientBudget,
    ) -> Result<Self, RpcEventError> {
        let context = Self {
            state: Arc::new(Mutex::new(ClientState {
                events: Some(events),
                client: Some(client),
                ..ClientState::default()
            })),
            request: None,
            retained_results: None,
            local_admin: false,
            #[cfg(feature = "control-diagnostics")]
            source_trace: None,
        };
        if !authentication_required {
            context.authorize()?;
        }
        Ok(context)
    }

    pub(crate) fn with_request(&self, request: crate::rpc_budget::RpcRequestLease) -> Self {
        Self {
            state: self.state.clone(),
            retained_results: Some(Arc::new(Mutex::new(crate::rpc_budget::RpcAllocation::new(
                request.client(),
                false,
            )))),
            request: Some(request),
            local_admin: self.local_admin,
            #[cfg(feature = "control-diagnostics")]
            source_trace: self.source_trace.clone(),
        }
    }

    pub(crate) fn retain_result(&self, bytes: usize) -> Result<(), crate::RpcBudgetError> {
        if let Some(results) = &self.retained_results {
            results
                .lock()
                .expect("RPC result accounting")
                .reserve(bytes)?;
        }
        Ok(())
    }

    pub(crate) fn request_lease(&self) -> Option<crate::rpc_budget::RpcRequestLease> {
        self.request.clone()
    }

    pub(crate) fn client_budget(&self) -> Option<crate::RpcClientBudget> {
        self.request
            .as_ref()
            .map(crate::rpc_budget::RpcRequestLease::client)
            .or_else(|| self.state.lock().expect("RPC client state").client.clone())
    }

    /// Observes this connection without retaining its requests, results or credit.
    /// Contexts without a transport budget return `None`.
    #[must_use]
    pub fn budget_observer(&self) -> Option<crate::RpcClientBudgetObserver> {
        self.client_budget().map(|client| client.observer())
    }

    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        self.state.lock().expect("RPC client state").authenticated
    }

    pub(crate) fn authorize(&self) -> Result<(), RpcEventError> {
        let mut state = self.state.lock().expect("RPC client state");
        if !state.authenticated {
            if let Some(events) = &state.events {
                state.subscriber = Some(events.subscribe_with_client(
                    RpcEventLimits::default(),
                    state.client.clone().expect("event client budget"),
                )?);
            }
            state.authenticated = true;
        }
        Ok(())
    }

    pub(crate) fn try_next_event(&self) -> Result<Option<RpcEventDelivery>, RpcEventError> {
        let mut state = self.state.lock().expect("RPC client state");
        match &mut state.subscriber {
            Some(subscriber) => subscriber.try_next_bounded(crate::rpc_result::RESULT_VALUE_BYTES),
            None => Ok(None),
        }
    }

    pub(crate) fn set_event_filter(
        &self,
        filter: crate::RpcEventFilter,
    ) -> Result<(), RpcEventError> {
        let mut state = self.state.lock().expect("RPC client state");
        state
            .subscriber
            .as_mut()
            .ok_or(RpcEventError::InvalidFilter)?
            .set_filter(filter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_observer_requires_a_transport_budget_and_does_not_retain_context() {
        assert!(RpcClientContext::default().budget_observer().is_none());
        let budgets = crate::RpcBudgets::process_default();
        let client = budgets.client().expect("client");
        let context =
            RpcClientContext::default().with_request(client.try_request(1024).expect("request"));
        context.retain_result(512 * 1024).expect("result");
        let observer = context.budget_observer().expect("request budget");
        drop(client);
        assert_eq!(observer.snapshot().unwrap().outstanding_requests, 1);
        drop(context);
        assert_eq!(observer.snapshot(), None);
    }

    #[test]
    fn budget_observer_uses_event_connection_before_a_request() {
        let broker = crate::RpcEventBroker::with_budgets(crate::RpcBudgets::process_default());
        let context = RpcClientContext::with_events(broker, false).expect("event context");
        let observer = context.budget_observer().expect("connection budget");
        assert_eq!(observer.snapshot().unwrap().outstanding_requests, 0);
        drop(context);
        assert_eq!(observer.snapshot(), None);
    }
}
