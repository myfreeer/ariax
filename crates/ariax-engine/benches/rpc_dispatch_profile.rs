use ariax_engine::{
    HttpRpcBackend, HttpRpcBackendError, RpcAuthPolicy, RpcDispatcher, RpcFuture, dispatch_json,
};
use serde_json::{Value, json};
use std::env;
use std::sync::Arc;
use std::time::Instant;

const SAMPLES: usize = 20_000;

struct SnapshotBackend;

impl HttpRpcBackend for SnapshotBackend {
    fn call(&self, method: &str, _params: Value) -> RpcFuture {
        let method = method.to_owned();
        Box::pin(async move {
            if method == "aria2.tellStatus" {
                Ok(json!({
                    "gid":"0000000000000001",
                    "status":"active",
                    "totalLength":"1048576",
                    "completedLength":"524288",
                    "downloadSpeed":"1048576"
                }))
            } else {
                Err(HttpRpcBackendError::new(-32601, "Method not found"))
            }
        })
    }
}

fn main() {
    if env::var_os("ARIAX_RUN_RPC_BENCH").is_none() {
        println!(
            "RPC dispatch profile benchmark compiled; set ARIAX_RUN_RPC_BENCH=1 to measure {SAMPLES} bounded status calls"
        );
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("RPC benchmark runtime");
    runtime.block_on(async {
        let backend = RpcDispatcher::new(Arc::new(SnapshotBackend), RpcAuthPolicy::default());
        let request = br#"{"jsonrpc":"2.0","id":1,"method":"aria2.tellStatus","params":["0000000000000001"]}"#;
        let mut samples = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let started = Instant::now();
            let response = dispatch_json(&backend, request).await;
            samples.push(started.elapsed());
            let value: Value = serde_json::from_slice(&response).expect("bounded response JSON");
            assert_eq!(value["result"]["status"], "active");
        }
        samples.sort_unstable();
        let p50 = samples[SAMPLES / 2];
        let p99 = samples[(SAMPLES * 99 / 100).min(SAMPLES - 1)];
        println!(
            "rpc_dispatch samples={SAMPLES} p50_us={} p99_us={}",
            p50.as_micros(),
            p99.as_micros()
        );
    });
}
