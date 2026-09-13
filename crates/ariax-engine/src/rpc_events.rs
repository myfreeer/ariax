use crate::rpc_budget::{RpcByteCharge, RpcEventCharge};
use crate::{RpcBudgets, RpcClientBudget};
use ariax_core::Gid;
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};

pub const DEFAULT_RPC_EVENT_CAPACITY: usize = 256;
pub const DEFAULT_RPC_EVENT_BYTE_CAPACITY: usize = 4 * 1024 * 1024;
pub const MAX_RPC_EVENT_CAPACITY: usize = 4096;
pub const MAX_RPC_EVENT_BYTE_CAPACITY: usize = 64 * 1024 * 1024;
pub const MAX_RPC_EVENT_SUBSCRIBERS: usize = 1024;
pub const MAX_RPC_EVENT_FILTER_METHODS: usize = 64;
pub const MAX_RPC_EVENT_FILTER_GIDS: usize = 256;

/// A bounded conjunction of method and task selections. Empty selections match all.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RpcEventFilter {
    methods: Vec<String>,
    gids: Vec<Gid>,
}

impl RpcEventFilter {
    pub(crate) fn from_rpc(value: &Value) -> Result<Self, RpcEventError> {
        let object = value.as_object().ok_or(RpcEventError::InvalidFilter)?;
        if object
            .keys()
            .any(|key| !matches!(key.as_str(), "methods" | "gids"))
        {
            return Err(RpcEventError::InvalidFilter);
        }
        let array = |name: &str, limit: usize| -> Result<&[Value], RpcEventError> {
            match object.get(name) {
                None => Ok(&[]),
                Some(Value::Array(values)) if values.len() <= limit => Ok(values),
                _ => Err(RpcEventError::InvalidFilter),
            }
        };
        let methods = array("methods", MAX_RPC_EVENT_FILTER_METHODS)?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|text| text.len() <= 128)
                    .map(str::to_owned)
                    .ok_or(RpcEventError::InvalidFilter)
            })
            .collect::<Result<_, _>>()?;
        let gids = array("gids", MAX_RPC_EVENT_FILTER_GIDS)?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .and_then(|text| text.parse().ok())
                    .ok_or(RpcEventError::InvalidFilter)
            })
            .collect::<Result<_, _>>()?;
        Self::new(methods, gids)
    }

    pub fn new(mut methods: Vec<String>, mut gids: Vec<Gid>) -> Result<Self, RpcEventError> {
        if methods.len() > MAX_RPC_EVENT_FILTER_METHODS
            || gids.len() > MAX_RPC_EVENT_FILTER_GIDS
            || methods.iter().any(|method| {
                method.is_empty()
                    || method.len() > 128
                    || !method
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_'))
            })
        {
            return Err(RpcEventError::InvalidFilter);
        }
        methods.sort();
        methods.dedup();
        gids.sort();
        gids.dedup();
        Ok(Self { methods, gids })
    }

    fn owned_bytes(&self) -> usize {
        self.methods.capacity() * std::mem::size_of::<String>()
            + self.methods.iter().map(String::capacity).sum::<usize>()
            + self.gids.capacity() * std::mem::size_of::<Gid>()
            + 128
    }

    fn matches(&self, event: &RpcEvent) -> bool {
        let method = event
            .value
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("");
        let gid = event.key.as_ref().and_then(|key| key.gid).or_else(|| {
            let params = event.value.get("params")?;
            params
                .get("gid")
                .or_else(|| params.get(0)?.get("gid"))?
                .as_str()?
                .parse()
                .ok()
        });
        (self.methods.is_empty()
            || self
                .methods
                .binary_search_by(|candidate| candidate.as_str().cmp(method))
                .is_ok())
            && (self.gids.is_empty() || gid.is_none_or(|gid| self.gids.binary_search(&gid).is_ok()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpcEventClass {
    Reliable,
    Coalesced,
    Informational,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RpcEventKey {
    gid: Option<Gid>,
    kind: Arc<str>,
}

impl RpcEventKey {
    #[must_use]
    pub fn new(gid: Option<Gid>, kind: impl Into<Arc<str>>) -> Self {
        Self {
            gid,
            kind: kind.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RpcEvent {
    value: Arc<Value>,
    serialized_bytes: usize,
    owned_bytes: usize,
    class: RpcEventClass,
    key: Option<RpcEventKey>,
}

impl RpcEvent {
    pub fn notification(
        method: impl Into<String>,
        params: Value,
        class: RpcEventClass,
        key: Option<RpcEventKey>,
    ) -> Result<Self, RpcEventError> {
        if class == RpcEventClass::Coalesced && key.is_none() {
            return Err(RpcEventError::MissingCoalesceKey);
        }
        let value = json!({"jsonrpc":"2.0", "method":method.into(), "params":params});
        let serialized_bytes =
            crate::http_rpc::serialized_value_size(&value, MAX_RPC_EVENT_BYTE_CAPACITY)
                .ok_or(RpcEventError::EventTooLarge)?;
        let owned_bytes = crate::rpc_json::owned_value_bytes(&value);
        Ok(Self {
            value: Arc::new(value),
            serialized_bytes,
            owned_bytes,
            class,
            key,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcEventLimits {
    pub events: NonZeroUsize,
    pub bytes: NonZeroUsize,
}

impl Default for RpcEventLimits {
    fn default() -> Self {
        Self {
            events: NonZeroUsize::new(DEFAULT_RPC_EVENT_CAPACITY)
                .expect("default event capacity is nonzero"),
            bytes: NonZeroUsize::new(DEFAULT_RPC_EVENT_BYTE_CAPACITY)
                .expect("default event byte capacity is nonzero"),
        }
    }
}

impl RpcEventLimits {
    pub fn validate(self) -> Result<Self, RpcEventError> {
        if self.events.get() > MAX_RPC_EVENT_CAPACITY
            || self.bytes.get() > MAX_RPC_EVENT_BYTE_CAPACITY
        {
            return Err(RpcEventError::InvalidLimits);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpcEventDisconnect {
    SlowConsumer,
    Unsubscribed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpcEventError {
    InvalidLimits,
    InvalidFilter,
    TooManySubscribers,
    MissingCoalesceKey,
    EventTooLarge,
    Serialization,
    BudgetExhausted,
    Disconnected(RpcEventDisconnect),
}

impl fmt::Display for RpcEventError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLimits => "invalid RPC event queue limits",
            Self::InvalidFilter => "invalid RPC event filter",
            Self::TooManySubscribers => "RPC event subscriber limit reached",
            Self::MissingCoalesceKey => "coalesced RPC event requires a key",
            Self::EventTooLarge => "RPC event exceeds the byte bound",
            Self::Serialization => "RPC event serialization failed",
            Self::BudgetExhausted => "RPC event budget is busy",
            Self::Disconnected(RpcEventDisconnect::SlowConsumer) => {
                "RPC event subscriber is a slow consumer"
            }
            Self::Disconnected(RpcEventDisconnect::Unsubscribed) => {
                "RPC event subscriber was removed"
            }
        })
    }
}

impl Error for RpcEventError {}

#[derive(Clone, Debug)]
struct QueuedEvent {
    event: RpcEvent,
    charge: Arc<RpcEventCharge>,
}

#[derive(Debug)]
struct SubscriberState {
    limits: RpcEventLimits,
    queue: VecDeque<QueuedEvent>,
    queued_bytes: usize,
    dropped: u64,
    coalesced: u64,
    disconnect: Option<RpcEventDisconnect>,
    client: RpcClientBudget,
    filter: RpcEventFilter,
    _filter_charge: RpcByteCharge,
    _queue_charge: RpcByteCharge,
}

#[derive(Debug)]
struct BrokerState {
    next_id: u64,
    subscribers: BTreeMap<u64, SubscriberState>,
}

#[derive(Clone, Debug)]
pub struct RpcEventBroker {
    state: Arc<Mutex<BrokerState>>,
    budgets: RpcBudgets,
}

impl Default for RpcEventBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcEventBroker {
    #[must_use]
    pub fn new() -> Self {
        Self::with_budgets(RpcBudgets::process_default())
    }

    #[must_use]
    pub fn with_budgets(budgets: RpcBudgets) -> Self {
        Self {
            budgets,
            state: Arc::new(Mutex::new(BrokerState {
                next_id: 1,
                subscribers: BTreeMap::new(),
            })),
        }
    }

    pub fn subscribe(&self, limits: RpcEventLimits) -> Result<RpcEventSubscriber, RpcEventError> {
        self.subscribe_with_client(limits, self.client_budget()?)
    }

    pub fn subscribe_filtered(
        &self,
        limits: RpcEventLimits,
        filter: RpcEventFilter,
    ) -> Result<RpcEventSubscriber, RpcEventError> {
        self.subscribe_filtered_with_client(limits, filter, self.client_budget()?)
    }

    pub(crate) fn client_budget(&self) -> Result<RpcClientBudget, RpcEventError> {
        self.budgets
            .client()
            .map_err(|_| RpcEventError::BudgetExhausted)
    }

    pub(crate) fn subscribe_with_client(
        &self,
        limits: RpcEventLimits,
        client: RpcClientBudget,
    ) -> Result<RpcEventSubscriber, RpcEventError> {
        self.subscribe_filtered_with_client(limits, RpcEventFilter::default(), client)
    }

    pub(crate) fn subscribe_filtered_with_client(
        &self,
        limits: RpcEventLimits,
        filter: RpcEventFilter,
        client: RpcClientBudget,
    ) -> Result<RpcEventSubscriber, RpcEventError> {
        let limits = limits.validate()?;
        let filter_charge = client
            .charge(filter.owned_bytes())
            .map_err(|_| RpcEventError::BudgetExhausted)?;
        let mut state = lock_unpoisoned(&self.state);
        if state.subscribers.len() == MAX_RPC_EVENT_SUBSCRIBERS {
            return Err(RpcEventError::TooManySubscribers);
        }
        let queue_charge = client
            .charge(limits.events.get() * std::mem::size_of::<QueuedEvent>() + 1024)
            .map_err(|_| RpcEventError::BudgetExhausted)?;
        let id = state.next_id;
        state.next_id = state.next_id.checked_add(1).unwrap_or(1);
        if state.next_id == 0 {
            state.next_id = 1;
        }
        state.subscribers.insert(
            id,
            SubscriberState {
                limits,
                queue: VecDeque::with_capacity(limits.events.get()),
                queued_bytes: 0,
                dropped: 0,
                coalesced: 0,
                disconnect: None,
                client,
                filter,
                _filter_charge: filter_charge,
                _queue_charge: queue_charge,
            },
        );
        Ok(RpcEventSubscriber {
            id,
            broker: self.clone(),
            active: true,
        })
    }

    pub fn publish(&self, event: RpcEvent) {
        let mut state = lock_unpoisoned(&self.state);
        for subscriber in state.subscribers.values_mut() {
            enqueue(subscriber, event.clone());
        }
    }

    pub fn unsubscribe(&self, id: u64) -> bool {
        lock_unpoisoned(&self.state)
            .subscribers
            .remove(&id)
            .is_some()
    }

    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        lock_unpoisoned(&self.state).subscribers.len()
    }
}

#[derive(Debug)]
pub struct RpcEventSubscriber {
    id: u64,
    broker: RpcEventBroker,
    active: bool,
}

impl RpcEventSubscriber {
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    pub fn set_filter(&mut self, filter: RpcEventFilter) -> Result<(), RpcEventError> {
        let mut state = lock_unpoisoned(&self.broker.state);
        let subscriber = state
            .subscribers
            .get_mut(&self.id)
            .ok_or(RpcEventError::Disconnected(
                RpcEventDisconnect::Unsubscribed,
            ))?;
        let charge = subscriber
            .client
            .charge(filter.owned_bytes())
            .map_err(|_| RpcEventError::BudgetExhausted)?;
        subscriber
            .queue
            .retain(|queued| filter.matches(&queued.event));
        subscriber.queued_bytes = subscriber
            .queue
            .iter()
            .map(|queued| queued.event.serialized_bytes)
            .sum();
        subscriber.filter = filter;
        subscriber._filter_charge = charge;
        Ok(())
    }

    pub fn try_next(&mut self) -> Result<Option<RpcEventDelivery>, RpcEventError> {
        self.try_next_bounded(usize::MAX)
    }

    pub(crate) fn try_next_bounded(
        &mut self,
        bytes: usize,
    ) -> Result<Option<RpcEventDelivery>, RpcEventError> {
        let mut state = lock_unpoisoned(&self.broker.state);
        let subscriber = state
            .subscribers
            .get_mut(&self.id)
            .ok_or(RpcEventError::Disconnected(
                RpcEventDisconnect::Unsubscribed,
            ))?;
        if let Some(reason) = subscriber.disconnect {
            return Err(RpcEventError::Disconnected(reason));
        }
        if subscriber
            .queue
            .front()
            .is_some_and(|queued| queued.event.owned_bytes.saturating_add(1024) > bytes)
        {
            return Err(RpcEventError::EventTooLarge);
        }
        let Some(queued) = subscriber.queue.pop_front() else {
            return Ok(None);
        };
        subscriber.queued_bytes = subscriber
            .queued_bytes
            .saturating_sub(queued.event.serialized_bytes);
        let dropped = std::mem::take(&mut subscriber.dropped);
        Ok(Some(RpcEventDelivery {
            value: queued.event.value,
            dropped,
            _charge: queued.charge,
        }))
    }
}

impl Drop for RpcEventSubscriber {
    fn drop(&mut self) {
        if self.active {
            self.broker.unsubscribe(self.id);
            self.active = false;
        }
    }
}

#[derive(Clone, Debug)]
pub struct RpcEventDelivery {
    value: Arc<Value>,
    dropped: u64,
    _charge: Arc<RpcEventCharge>,
}

impl RpcEventDelivery {
    #[must_use]
    pub fn into_value(self) -> Value {
        let mut value = (*self.value).clone();
        if self.dropped != 0
            && let Some(params) = value.get_mut("params").and_then(Value::as_object_mut)
        {
            params.insert("ariaxDropped".to_owned(), Value::from(self.dropped));
        }
        value
    }
}

fn enqueue(subscriber: &mut SubscriberState, event: RpcEvent) {
    if subscriber.disconnect.is_some() || !subscriber.filter.matches(&event) {
        return;
    }
    if event.class == RpcEventClass::Coalesced
        && let Some(index) = subscriber
            .queue
            .iter()
            .position(|queued| queued.event.key == event.key)
    {
        let previous = subscriber
            .queue
            .get_mut(index)
            .expect("coalesced event index remains present");
        let replaced_bytes = previous.event.serialized_bytes;
        let next_bytes = subscriber
            .queued_bytes
            .saturating_sub(replaced_bytes)
            .saturating_add(event.serialized_bytes);
        let replaced = next_bytes <= subscriber.limits.bytes.get()
            && Arc::get_mut(&mut previous.charge).is_some_and(|charge| {
                charge
                    .replace_bytes(
                        &subscriber.client,
                        event.owned_bytes.saturating_mul(2).saturating_add(256),
                    )
                    .is_ok()
            });
        if replaced {
            previous.event = event;
            subscriber.queued_bytes = next_bytes;
            subscriber.coalesced = subscriber.coalesced.saturating_add(1);
        } else {
            subscriber.dropped = subscriber.dropped.saturating_add(1);
        }
        return;
    }
    let fits = subscriber.queue.len() < subscriber.limits.events.get()
        && subscriber
            .queued_bytes
            .saturating_add(event.serialized_bytes)
            <= subscriber.limits.bytes.get();
    let charge = fits
        .then(|| {
            subscriber
                .client
                .event_charge(event.owned_bytes.saturating_mul(2).saturating_add(256))
                .ok()
        })
        .flatten();
    if let Some(charge) = charge {
        subscriber.queued_bytes = subscriber
            .queued_bytes
            .saturating_add(event.serialized_bytes);
        subscriber.queue.push_back(QueuedEvent {
            event,
            charge: Arc::new(charge),
        });
    } else if event.class == RpcEventClass::Reliable {
        subscriber.disconnect = Some(RpcEventDisconnect::SlowConsumer);
        subscriber.queue.clear();
        subscriber.queued_bytes = 0;
    } else {
        subscriber.dropped = subscriber.dropped.saturating_add(1);
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_prevent_unrelated_overflow_and_atomically_remove_queued_events() {
        let broker = RpcEventBroker::new();
        let gid: Gid = "0000000000000001".parse().expect("gid");
        let other: Gid = "0000000000000002".parse().expect("gid");
        let filter = RpcEventFilter::new(vec!["aria2.onDownloadComplete".to_owned()], vec![gid])
            .expect("filter");
        let mut subscriber = broker
            .subscribe_filtered(
                RpcEventLimits {
                    events: NonZeroUsize::new(1).expect("one"),
                    bytes: NonZeroUsize::new(4096).expect("bytes"),
                },
                filter,
            )
            .expect("subscribe");
        for _ in 0..8 {
            broker.publish(
                RpcEvent::notification(
                    "aria2.onDownloadComplete",
                    json!([{"gid":other.to_string()}]),
                    RpcEventClass::Reliable,
                    None,
                )
                .expect("event"),
            );
        }
        assert!(
            subscriber
                .try_next()
                .expect("no unrelated overflow")
                .is_none()
        );
        broker.publish(
            RpcEvent::notification(
                "aria2.onDownloadComplete",
                json!([{"gid":gid.to_string()}]),
                RpcEventClass::Reliable,
                None,
            )
            .expect("event"),
        );
        subscriber
            .set_filter(
                RpcEventFilter::new(vec!["ariax.onShutdown".to_owned()], vec![other])
                    .expect("replacement"),
            )
            .expect("replace");
        assert!(subscriber.try_next().expect("discard old queue").is_none());
        broker.publish(
            RpcEvent::notification("ariax.onShutdown", json!({}), RpcEventClass::Reliable, None)
                .expect("global"),
        );
        assert_eq!(
            subscriber
                .try_next()
                .expect("global bypasses GID")
                .expect("event")
                .into_value()["method"],
            "ariax.onShutdown"
        );
        for value in [
            json!({"extra":[]}),
            json!({"gids":["1"]}),
            json!({"methods":["bad\nmethod"]}),
            json!({"methods":vec!["x";65]}),
        ] {
            assert_eq!(
                RpcEventFilter::from_rpc(&value),
                Err(RpcEventError::InvalidFilter)
            );
        }
    }

    #[test]
    fn bounded_poll_keeps_an_oversized_event_and_its_permits_queued() {
        let broker = RpcEventBroker::new();
        let mut subscriber = broker
            .subscribe(RpcEventLimits::default())
            .expect("subscriber");
        broker.publish(event(RpcEventClass::Reliable, None, 1));
        assert!(matches!(
            subscriber.try_next_bounded(1),
            Err(RpcEventError::EventTooLarge)
        ));
        let delivered = subscriber
            .try_next_bounded(64 * 1024)
            .expect("budget fits")
            .expect("event remains queued")
            .into_value();
        assert_eq!(delivered["params"]["sequence"], 1);
        assert!(subscriber.try_next().expect("empty").is_none());
    }

    #[test]
    fn event_credit_follows_delivery_and_preserves_command_capacity() {
        let profile = ariax_runtime::ResolvedRuntimeProfile::resolve(
            ariax_runtime::RuntimeProfile::Compact,
            None,
        );
        let budgets = RpcBudgets::with_shared_resident(
            profile,
            ariax_runtime::ByteBudget::new(64 * 1024 * 1024),
        );
        let broker = RpcEventBroker::with_budgets(budgets.clone());
        let client = budgets.client().expect("client");
        let mut subscriber = broker
            .subscribe_with_client(RpcEventLimits::default(), client.clone())
            .expect("subscriber");
        let baseline = budgets.snapshot().bytes;
        let limit = budgets.snapshot().item_limit / 2;
        for sequence in 0..limit {
            broker.publish(event(RpcEventClass::Informational, None, sequence as u64));
        }
        assert_eq!(budgets.snapshot().items, limit);
        assert!(budgets.snapshot().bytes > baseline);
        broker.publish(event(RpcEventClass::Informational, None, 100));
        assert_eq!(budgets.snapshot().items, limit);
        let request = client.try_request(128).expect("polling remains possible");
        let response = client
            .response(Some(request))
            .expect("reply remains possible");
        drop(response);
        let delivery = subscriber.try_next().expect("queue").expect("delivery");
        let retained = delivery.clone();
        drop(delivery);
        assert_eq!(budgets.snapshot().items, limit);
        drop(retained);
        assert_eq!(budgets.snapshot().items, limit - 1);
        broker.publish(event(RpcEventClass::Reliable, None, 101));
        broker.publish(event(RpcEventClass::Reliable, None, 102));
        assert!(matches!(
            subscriber.try_next(),
            Err(RpcEventError::Disconnected(
                RpcEventDisconnect::SlowConsumer
            ))
        ));
        assert_eq!(budgets.snapshot().items, 0);
        drop(subscriber);
        drop(client);
        assert_eq!(budgets.snapshot().bytes, 0);
        assert_eq!(budgets.snapshot().resident_bytes, 0);
    }

    fn event(class: RpcEventClass, key: Option<RpcEventKey>, sequence: u64) -> RpcEvent {
        RpcEvent::notification("ariax.test", json!({"sequence": sequence}), class, key)
            .expect("event")
    }

    #[test]
    fn coalescing_is_per_subscriber_and_reports_information_loss() {
        let broker = RpcEventBroker::new();
        let mut subscriber = broker
            .subscribe(RpcEventLimits {
                events: NonZeroUsize::new(1).expect("events"),
                bytes: NonZeroUsize::new(1024).expect("bytes"),
            })
            .expect("subscribe");
        let key = RpcEventKey::new(None, "status");
        broker.publish(event(RpcEventClass::Coalesced, Some(key.clone()), 1));
        broker.publish(event(RpcEventClass::Coalesced, Some(key), 2));
        broker.publish(event(RpcEventClass::Informational, None, 3));

        let delivered = subscriber
            .try_next()
            .expect("delivery")
            .expect("queued event")
            .into_value();
        assert_eq!(delivered["params"]["sequence"], 2);
        assert_eq!(delivered["params"]["ariaxDropped"], 1);
        assert!(subscriber.try_next().expect("empty queue").is_none());
    }

    #[test]
    fn reliable_overflow_disconnects_only_the_slow_subscriber() {
        let broker = RpcEventBroker::new();
        let limits = RpcEventLimits {
            events: NonZeroUsize::new(1).expect("events"),
            bytes: NonZeroUsize::new(1024).expect("bytes"),
        };
        let mut slow = broker.subscribe(limits).expect("slow subscriber");
        let mut fast = broker.subscribe(limits).expect("fast subscriber");
        broker.publish(event(RpcEventClass::Reliable, None, 1));
        assert!(fast.try_next().expect("fast delivery").is_some());
        broker.publish(event(RpcEventClass::Reliable, None, 2));
        assert_eq!(
            slow.try_next().expect_err("slow consumer disconnected"),
            RpcEventError::Disconnected(RpcEventDisconnect::SlowConsumer)
        );
        assert_eq!(
            fast.try_next()
                .expect("second fast delivery")
                .expect("fast event")
                .into_value()["params"]["sequence"],
            2
        );
    }
}
