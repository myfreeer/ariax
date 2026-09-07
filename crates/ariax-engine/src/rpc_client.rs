//! Connection-local authorization and ownership of pushed event subscriptions.

use crate::{RpcEventBroker, RpcEventDelivery, RpcEventError, RpcEventLimits, RpcEventSubscriber};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct RpcClientContext {
    state: Arc<Mutex<ClientState>>,
}

#[derive(Default)]
struct ClientState {
    authenticated: bool,
    events: Option<RpcEventBroker>,
    subscriber: Option<RpcEventSubscriber>,
}

impl RpcClientContext {
    pub(crate) fn with_events(
        events: RpcEventBroker,
        authentication_required: bool,
    ) -> Result<Self, RpcEventError> {
        let context = Self {
            state: Arc::new(Mutex::new(ClientState {
                events: Some(events),
                ..ClientState::default()
            })),
        };
        if !authentication_required {
            context.authorize()?;
        }
        Ok(context)
    }

    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        self.state.lock().expect("RPC client state").authenticated
    }

    pub(crate) fn authorize(&self) -> Result<(), RpcEventError> {
        let mut state = self.state.lock().expect("RPC client state");
        if !state.authenticated {
            if let Some(events) = &state.events {
                state.subscriber = Some(events.subscribe(RpcEventLimits::default())?);
            }
            state.authenticated = true;
        }
        Ok(())
    }

    pub(crate) fn try_next_event(&self) -> Result<Option<RpcEventDelivery>, RpcEventError> {
        let mut state = self.state.lock().expect("RPC client state");
        match &mut state.subscriber {
            Some(subscriber) => subscriber.try_next(),
            None => Ok(None),
        }
    }
}
