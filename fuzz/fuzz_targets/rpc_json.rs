#![no_main]

use ariax_engine::{
    HttpRpcBackend, HttpRpcBackendError, MAX_HTTP_RPC_REQUEST_BYTES, MAX_HTTP_RPC_RESPONSE_BYTES,
    RpcAuthPolicy, RpcDispatcher, RpcFuture, dispatch_json,
};
use libfuzzer_sys::fuzz_target;
use serde_json::Value;
use std::sync::{Arc, LazyLock};

struct RejectingBackend;

impl HttpRpcBackend for RejectingBackend {
    fn call(&self, _method: &str, _params: Value) -> RpcFuture {
        Box::pin(async { Err(HttpRpcBackendError::new(-32601, "Method not found")) })
    }
}

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("fuzz runtime")
});

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_HTTP_RPC_REQUEST_BYTES {
        return;
    }
    for auth in [
        RpcAuthPolicy::default(),
        RpcAuthPolicy::with_secret("fuzz-token"),
    ] {
        let dispatcher = RpcDispatcher::new(Arc::new(RejectingBackend), auth);
        let budgets = dispatcher.rpc_budgets();
        let baseline = budgets.snapshot();
        let response = RUNTIME.block_on(dispatch_json(&dispatcher, data));
        assert!(response.len() <= MAX_HTTP_RPC_RESPONSE_BYTES);
        if !response.is_empty() {
            serde_json::from_slice::<Value>(&response).expect("dispatcher emits complete JSON");
        }
        assert!(budgets.snapshot().bytes <= budgets.snapshot().byte_limit);
        assert!(budgets.snapshot().items <= budgets.snapshot().item_limit);
        drop(response);
        assert_eq!(
            budgets.snapshot(),
            baseline,
            "dispatch refunds all temporary and response credit"
        );
    }
});
