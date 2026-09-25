//! Process and connection accounting retained by request and response owners.

use ariax_runtime::{ByteBudget, BytePermit, ResolvedRuntimeProfile, RuntimeProfile};
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::Notify;

pub const MAX_RPC_CLIENT_REQUESTS: usize = 4;
pub const MAX_RPC_CLIENT_REQUEST_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_RPC_CLIENT_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const RPC_RESULT_WORKSPACE_BYTES: usize = 8 * 1024 * 1024;
const ALLOCATION_PAGE: usize = 4096;
const PAGE_ACCOUNTING_BYTES: usize = 256;
const CLIENT_OVERHEAD_BYTES: usize = 64 * 1024;
const REQUEST_OVERHEAD_BYTES: usize = 1024;

#[derive(Clone, Debug)]
pub struct RpcBudgets {
    inner: Arc<ProcessBudget>,
}

#[derive(Debug)]
struct ProcessBudget {
    items: ByteBudget,
    event_items: ByteBudget,
    bytes: ByteBudget,
    resident: ByteBudget,
    changed: Notify,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcBudgetSnapshot {
    pub items: usize,
    pub item_limit: usize,
    pub bytes: usize,
    pub byte_limit: usize,
    pub resident_bytes: usize,
    pub resident_limit: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpcBudgetError {
    Busy,
    TooLarge,
}

impl fmt::Display for RpcBudgetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Busy => "RPC budget is busy",
            Self::TooLarge => "RPC allocation exceeds its budget",
        })
    }
}

impl std::error::Error for RpcBudgetError {}

impl RpcBudgets {
    #[cfg(feature = "bt")]
    pub(crate) fn resident_budget(&self) -> ByteBudget {
        self.inner.resident.clone()
    }
    #[must_use]
    pub fn with_shared_resident(profile: ResolvedRuntimeProfile, resident: ByteBudget) -> Self {
        let limits = profile.limits();
        Self::new(limits.rpc_pending_items, limits.rpc_pending_bytes, resident)
    }

    fn new(items: usize, bytes: usize, resident: ByteBudget) -> Self {
        Self {
            inner: Arc::new(ProcessBudget {
                items: ByteBudget::new(items),
                event_items: ByteBudget::new(items / 2),
                bytes: ByteBudget::new(bytes),
                resident,
                changed: Notify::new(),
            }),
        }
    }

    /// Fallback for standalone backends; every listener in the process shares it.
    #[must_use]
    pub fn process_default() -> Self {
        static DEFAULT: OnceLock<RpcBudgets> = OnceLock::new();
        DEFAULT
            .get_or_init(|| {
                let profile = ResolvedRuntimeProfile::resolve(RuntimeProfile::Auto, None);
                Self::with_shared_resident(
                    profile,
                    ByteBudget::new(profile.limits().accounted_resident_limit_bytes),
                )
            })
            .clone()
    }

    #[must_use]
    pub fn snapshot(&self) -> RpcBudgetSnapshot {
        RpcBudgetSnapshot {
            items: self.inner.items.used(),
            item_limit: self.inner.items.limit(),
            bytes: self.inner.bytes.used(),
            byte_limit: self.inner.bytes.limit(),
            resident_bytes: self.inner.resident.used(),
            resident_limit: self.inner.resident.limit(),
        }
    }

    pub fn client(&self) -> Result<RpcClientBudget, RpcBudgetError> {
        let limit = (self.inner.bytes.limit() / 4 * 3).min(MAX_RPC_CLIENT_BYTES);
        let total = ByteBudget::new(limit);
        let overhead = self.charge(&total, None, CLIENT_OVERHEAD_BYTES)?;
        Ok(RpcClientBudget {
            inner: Arc::new(ClientBudget {
                process: self.clone(),
                total,
                requests: ByteBudget::new(MAX_RPC_CLIENT_REQUESTS),
                request_bytes: ByteBudget::new(MAX_RPC_CLIENT_REQUEST_BYTES),
                responses: ByteBudget::new(1),
                _overhead: overhead,
            }),
        })
    }

    fn charge(
        &self,
        client: &ByteBudget,
        local: Option<&ByteBudget>,
        bytes: usize,
    ) -> Result<RpcByteCharge, RpcBudgetError> {
        if bytes > client.limit()
            || bytes > self.inner.bytes.limit()
            || bytes > self.inner.resident.limit()
            || local.is_some_and(|local| bytes > local.limit())
        {
            return Err(RpcBudgetError::TooLarge);
        }
        let local = local
            .map(|budget| budget.try_acquire(bytes))
            .transpose()
            .map_err(|_| RpcBudgetError::Busy)?;
        let client = client
            .try_acquire(bytes)
            .map_err(|_| RpcBudgetError::Busy)?;
        let process = self
            .inner
            .bytes
            .try_acquire(bytes)
            .map_err(|_| RpcBudgetError::Busy)?;
        let resident = self
            .inner
            .resident
            .try_acquire(bytes)
            .map_err(|_| RpcBudgetError::Busy)?;
        Ok(RpcByteCharge {
            permits: Some((local, client, process, resident)),
            process: self.clone(),
        })
    }
}

#[derive(Debug)]
struct ClientBudget {
    process: RpcBudgets,
    total: ByteBudget,
    requests: ByteBudget,
    request_bytes: ByteBudget,
    responses: ByteBudget,
    _overhead: RpcByteCharge,
}

#[derive(Clone, Debug)]
pub struct RpcClientBudget {
    inner: Arc<ClientBudget>,
}

impl RpcClientBudget {
    #[must_use]
    pub fn outstanding_requests(&self) -> usize {
        self.inner.requests.used()
    }

    #[must_use]
    pub fn request_bytes(&self) -> usize {
        self.inner.request_bytes.used()
    }

    #[must_use]
    pub fn bytes(&self) -> usize {
        self.inner.total.used()
    }

    pub(crate) fn try_request(&self, body_bytes: usize) -> Result<RpcRequestLease, RpcBudgetError> {
        let bytes = body_bytes
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(REQUEST_OVERHEAD_BYTES))
            .filter(|bytes| *bytes <= MAX_RPC_CLIENT_REQUEST_BYTES)
            .ok_or(RpcBudgetError::TooLarge)?;
        let slot = self.item(&self.inner.requests)?;
        let mut allocation = RpcAllocation::new(self.clone(), true);
        allocation.reserve(bytes)?;
        Ok(RpcRequestLease {
            inner: Arc::new(RequestLease {
                allocation: Mutex::new(allocation),
                _slot: slot,
            }),
            _command: None,
        })
    }

    pub(crate) async fn request(
        &self,
        body_bytes: usize,
    ) -> Result<RpcRequestLease, RpcBudgetError> {
        loop {
            let changed = self.inner.process.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.try_request(body_bytes) {
                Ok(lease) => return Ok(lease),
                Err(RpcBudgetError::TooLarge) => return Err(RpcBudgetError::TooLarge),
                Err(RpcBudgetError::Busy) => {}
            }
            // Download/storage resident permits can be released without an RPC notification.
            tokio::select! {
                () = changed => {},
                () = tokio::time::sleep(Duration::from_millis(10)) => {},
            }
        }
    }

    pub(crate) fn response(
        &self,
        request: Option<RpcRequestLease>,
    ) -> Result<RpcResponseLease, RpcBudgetError> {
        Ok(RpcResponseLease {
            client: self.clone(),
            _slot: self.item(&self.inner.responses)?,
            _request: request,
        })
    }

    fn item(&self, local: &ByteBudget) -> Result<RpcItemCharge, RpcBudgetError> {
        let local = local.try_acquire(1).map_err(|_| RpcBudgetError::Busy)?;
        let process = self
            .inner
            .process
            .inner
            .items
            .try_acquire(1)
            .map_err(|_| RpcBudgetError::Busy)?;
        Ok(RpcItemCharge {
            permits: Some((local, process)),
            process: self.inner.process.clone(),
        })
    }

    pub(crate) fn charge(&self, bytes: usize) -> Result<RpcByteCharge, RpcBudgetError> {
        self.inner.process.charge(&self.inner.total, None, bytes)
    }

    pub(crate) fn event_charge(&self, bytes: usize) -> Result<RpcEventCharge, RpcBudgetError> {
        let items = self.item(&self.inner.process.inner.event_items)?;
        Ok(RpcEventCharge {
            _bytes: self.charge(bytes)?,
            _items: items,
        })
    }
}

#[derive(Debug)]
pub(crate) struct RpcEventCharge {
    _bytes: RpcByteCharge,
    _items: RpcItemCharge,
}

impl RpcEventCharge {
    pub(crate) fn replace_bytes(
        &mut self,
        client: &RpcClientBudget,
        bytes: usize,
    ) -> Result<(), RpcBudgetError> {
        self._bytes = client.charge(bytes)?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct RpcByteCharge {
    permits: Option<(Option<BytePermit>, BytePermit, BytePermit, BytePermit)>,
    process: RpcBudgets,
}

impl Drop for RpcByteCharge {
    fn drop(&mut self) {
        drop(self.permits.take());
        self.process.inner.changed.notify_waiters();
    }
}

#[derive(Debug)]
struct RpcItemCharge {
    permits: Option<(BytePermit, BytePermit)>,
    process: RpcBudgets,
}

impl Drop for RpcItemCharge {
    fn drop(&mut self) {
        drop(self.permits.take());
        self.process.inner.changed.notify_waiters();
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RpcRequestLease {
    inner: Arc<RequestLease>,
    _command: Option<Arc<RpcAllocation>>,
}

#[derive(Debug)]
struct RequestLease {
    allocation: Mutex<RpcAllocation>,
    _slot: RpcItemCharge,
}

impl RpcRequestLease {
    pub(crate) fn reserve_command(&self, bytes: usize) -> Result<Self, RpcBudgetError> {
        let mut command = RpcAllocation::new(self.client(), true);
        command.reserve(bytes)?;
        Ok(Self {
            inner: self.inner.clone(),
            _command: Some(Arc::new(command)),
        })
    }

    pub(crate) fn client(&self) -> RpcClientBudget {
        self.inner
            .allocation
            .lock()
            .expect("RPC request accounting")
            .client
            .clone()
    }

    pub(crate) fn reserve(&self, bytes: usize) -> Result<(), RpcBudgetError> {
        self.inner
            .allocation
            .lock()
            .expect("RPC request accounting")
            .reserve(bytes)
    }
}

#[derive(Debug)]
pub(crate) struct RpcResponseLease {
    client: RpcClientBudget,
    _slot: RpcItemCharge,
    _request: Option<RpcRequestLease>,
}

impl RpcResponseLease {
    pub(crate) fn workspace(&self) -> Result<RpcByteCharge, RpcBudgetError> {
        self.client.charge(RPC_RESULT_WORKSPACE_BYTES)
    }

    pub(crate) fn allocation(&self) -> RpcAllocation {
        RpcAllocation::new(self.client.clone(), false)
    }
}

#[derive(Debug)]
pub(crate) struct RpcAllocation {
    client: RpcClientBudget,
    request: bool,
    used: usize,
    reserved: usize,
    charges: Vec<RpcByteCharge>,
}

impl RpcAllocation {
    pub(crate) fn new(client: RpcClientBudget, request: bool) -> Self {
        Self {
            client,
            request,
            used: 0,
            reserved: 0,
            charges: Vec::new(),
        }
    }

    pub(crate) fn reserve(&mut self, bytes: usize) -> Result<(), RpcBudgetError> {
        let next = self
            .used
            .checked_add(bytes)
            .ok_or(RpcBudgetError::TooLarge)?;
        let local = self.request.then_some(&self.client.inner.request_bytes);
        if local.is_some_and(|local| next > local.limit()) || next > self.client.inner.total.limit()
        {
            return Err(RpcBudgetError::TooLarge);
        }
        if next > self.reserved {
            let additional = (next - self.reserved).div_ceil(ALLOCATION_PAGE) * ALLOCATION_PAGE;
            let charge = self.client.inner.process.charge(
                &self.client.inner.total,
                local,
                additional + PAGE_ACCOUNTING_BYTES,
            )?;
            self.charges.push(charge);
            self.reserved += additional;
        }
        self.used = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budgets() -> RpcBudgets {
        RpcBudgets::new(32, 32 * 1024 * 1024, ByteBudget::new(40 * 1024 * 1024))
    }

    #[test]
    fn sequential_commands_refund_temporary_state_but_deferred_commands_retain_it() {
        let process = budgets();
        let client = process.client().expect("client");
        let request = client.try_request(1024).expect("batch request");
        let baseline = client.request_bytes();
        for _ in 0..256 {
            let command = request
                .reserve_command(128 * 1024)
                .expect("one batch member");
            assert!(client.request_bytes() > baseline);
            drop(command);
            assert_eq!(client.request_bytes(), baseline);
        }
        let pending = request
            .reserve_command(128 * 1024)
            .expect("deferred mutation");
        drop(request);
        assert_eq!(client.outstanding_requests(), 1);
        assert!(client.request_bytes() > baseline);
        assert!(
            pending
                .reserve_command(MAX_RPC_CLIENT_REQUEST_BYTES)
                .is_err()
        );
        drop(pending);
        assert_eq!(client.outstanding_requests(), 0);
        assert_eq!(client.request_bytes(), 0);
        drop(client);
        assert_eq!(process.snapshot().bytes, 0);
    }

    #[test]
    fn request_slots_include_retained_commands_and_responses() {
        let process = budgets();
        let client = process.client().expect("client");
        let mut requests = (0..4)
            .map(|_| client.try_request(1024).expect("request"))
            .collect::<Vec<_>>();
        assert_eq!(client.outstanding_requests(), 4);
        assert!(matches!(client.try_request(1), Err(RpcBudgetError::Busy)));
        let response = client.response(requests.pop()).expect("response");
        assert!(client.response(None).is_err());
        assert!(client.try_request(1).is_err());
        drop(response);
        assert!(client.try_request(1).is_ok());
        let deferred = requests[0].clone();
        drop(requests);
        assert_eq!(client.outstanding_requests(), 1);
        drop(deferred);
        assert_eq!(client.request_bytes(), 0);
        drop(client);
        assert_eq!(process.snapshot().bytes, 0);
        assert_eq!(process.snapshot().resident_bytes, 0);
        assert_eq!(process.snapshot().items, 0);
    }

    #[test]
    fn process_and_resident_exhaustion_roll_back_every_partial_charge() {
        let resident = ByteBudget::new(40 * 1024 * 1024);
        let process = RpcBudgets::new(32, 32 * 1024 * 1024, resident.clone());
        let a = process.client().expect("a");
        let b = process.clone().client().expect("b");
        let held = a.charge(20 * 1024 * 1024).expect("client share");
        assert!(a.charge(5 * 1024 * 1024).is_err());
        let baseline = process.snapshot();
        assert!(b.charge(20 * 1024 * 1024).is_err());
        assert_eq!(process.snapshot(), baseline);
        drop(held);
        let download = resident.try_acquire(39 * 1024 * 1024).expect("download");
        let baseline = process.snapshot();
        assert!(b.try_request(1024 * 1024).is_err());
        assert_eq!(process.snapshot(), baseline);
        assert_eq!(b.outstanding_requests(), 0);
        drop(download);
        assert!(b.try_request(1024 * 1024).is_ok());
    }

    #[tokio::test]
    async fn released_credit_wakes_a_backpressured_reader() {
        let process = budgets();
        let client = process.client().expect("client");
        let requests = (0..4)
            .map(|_| client.try_request(1).expect("request"))
            .collect::<Vec<_>>();
        let waiter = client.request(1);
        tokio::pin!(waiter);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut waiter)
                .await
                .is_err()
        );
        drop(requests);
        let lease = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("woken")
            .expect("lease");
        drop(lease);
        assert_eq!(client.outstanding_requests(), 0);
    }
}
