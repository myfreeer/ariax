//! Connection-local authorization and ownership of pushed event subscriptions.

use crate::{RpcEventBroker, RpcEventDelivery, RpcEventError, RpcEventLimits, RpcEventSubscriber};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct RpcClientContext {
    state: Arc<Mutex<ClientState>>,
    request: Option<crate::rpc_budget::RpcRequestLease>,
    retained_results: Option<Arc<Mutex<crate::rpc_budget::RpcAllocation>>>,
}

#[derive(Default)]
struct ClientState {
    authenticated: bool,
    events: Option<RpcEventBroker>,
    subscriber: Option<RpcEventSubscriber>,
    client: Option<crate::RpcClientBudget>,
}

impl RpcClientContext {
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
