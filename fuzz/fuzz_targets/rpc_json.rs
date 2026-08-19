#![no_main]

use ariax_engine::{
    HttpRpcBackend, HttpRpcBackendError, MAX_HTTP_RPC_REQUEST_BYTES,
    MAX_HTTP_RPC_RESPONSE_BYTES, RpcFuture, dispatch_json,
};
use libfuzzer_sys::fuzz_target;
use serde_json::Value;
use std::sync::LazyLock;

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
    let response = RUNTIME.block_on(dispatch_json(&RejectingBackend, data));
    assert!(response.len() <= MAX_HTTP_RPC_RESPONSE_BYTES);
    if !response.is_empty() {
        serde_json::from_slice::<Value>(&response).expect("dispatcher emits complete JSON");
    }
});
