#![forbid(unsafe_code)]

#[path = "../benches/rpc_active_profile/stalled_credit.rs"]
mod stalled_credit;

use serde_json::json;
use stalled_credit::{MIN_RETAINED_BYTES, Sample, consumer_index, sample};

#[test]
fn another_consumers_credit_cannot_satisfy_retention_or_release() {
    let baseline = 64 * 1024;
    let mut metrics = json!({"rpc":20_000_000,"stalledConsumers":{
        "events":{"bytes":2_000_000,"responses":1},
        "response":{"bytes":baseline,"responses":0}}});
    assert!(
        !sample(&metrics, "response")
            .unwrap()
            .unwrap()
            .retains(baseline)
    );
    metrics["stalledConsumers"]["response"] =
        json!({"bytes":baseline + MIN_RETAINED_BYTES,"responses":1});
    metrics["rpc"] = json!(19_000_000);
    metrics["stalledConsumers"]["events"] = json!(null);
    assert!(
        sample(&metrics, "response")
            .unwrap()
            .unwrap()
            .retains(baseline)
    );
    assert!(sample(&metrics, "response").unwrap().is_some());
    assert!(sample(&metrics, "events").unwrap().is_none());
}

#[test]
fn cached_bytes_without_a_response_owner_do_not_prove_stalling() {
    let bytes = MIN_RETAINED_BYTES + 64 * 1024;
    assert!(
        !Sample {
            bytes,
            responses: 0
        }
        .retains(64 * 1024)
    );
    assert!(
        !Sample {
            bytes,
            responses: 1
        }
        .retains(bytes + 1)
    );
    assert!(
        !Sample {
            bytes,
            responses: 1
        }
        .retains(64 * 1024 + 1)
    );
    assert!(
        Sample {
            bytes,
            responses: 1
        }
        .retains(64 * 1024)
    );
}

#[test]
fn missing_or_malformed_evidence_does_not_count_as_release() {
    for metrics in [
        json!({}),
        json!({"stalledConsumers":{}}),
        json!({"stalledConsumers":{"response":{}}}),
        json!({"stalledConsumers":{"response":{"bytes":-1,"responses":1}}}),
        json!({"stalledConsumers":{"response":{"bytes":123,"responses":"1"}}}),
    ] {
        assert!(sample(&metrics, "response").is_err());
    }
    assert!(consumer_index("other").is_err());
    assert!(sample(&json!({"stalledConsumers":{"other":null}}), "other").is_err());
    assert_eq!(
        sample(&json!({"stalledConsumers":{"response":null}}), "response"),
        Ok(None)
    );
}

mod socket_fixture {
    use super::stalled_credit::Sample;
    use ariax_engine::*;
    use ariax_runtime::{ByteBudget, ResolvedRuntimeProfile, RuntimeProfile};
    use futures_util::{SinkExt as _, StreamExt as _};
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

    type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

    struct Backend {
        budgets: RpcBudgets,
        events: RpcEventBroker,
        observers: Mutex<[Option<RpcClientBudgetObserver>; 2]>,
    }

    impl Backend {
        fn new() -> Self {
            let profile = ResolvedRuntimeProfile::resolve(RuntimeProfile::Concurrency, None);
            let budgets =
                RpcBudgets::with_shared_resident(profile, ByteBudget::new(128 * 1024 * 1024));
            Self {
                events: RpcEventBroker::with_budgets(budgets.clone()),
                budgets,
                observers: Mutex::new([None, None]),
            }
        }

        fn observer(&self, index: usize) -> RpcClientBudgetObserver {
            self.observers.lock().unwrap()[index]
                .clone()
                .expect("registered")
        }
    }

    impl HttpRpcBackend for Backend {
        fn rpc_budgets(&self) -> RpcBudgets {
            self.budgets.clone()
        }

        fn call(&self, method: &str, params: Value) -> RpcFuture {
            self.call_with_context(method, params, RpcClientContext::default())
        }

        fn call_with_context(
            &self,
            method: &str,
            params: Value,
            context: RpcClientContext,
        ) -> RpcFuture {
            if method == "observe" {
                let index = params[0].as_u64().expect("index") as usize;
                self.observers.lock().unwrap()[index] = context.budget_observer();
            }
            let large = method == "large";
            Box::pin(async move {
                Ok(if large {
                    json!("x".repeat(512 * 1024))
                } else {
                    json!("OK")
                })
            })
        }
    }

    impl RpcWebSocketBackend for Backend {
        fn event_broker(&self) -> RpcEventBroker {
            self.events.clone()
        }
    }

    async fn call(socket: &mut Socket, method: &str, params: Value) {
        tokio::time::timeout(Duration::from_secs(5), async {
            send(socket, method, params).await;
            loop {
                let message = socket.next().await.expect("reply").expect("frame");
                let value: Value = serde_json::from_slice(&message.into_data()).expect("JSON");
                if value.get("id").is_some() {
                    assert_eq!(value["result"], "OK", "{value}");
                    break;
                }
            }
        })
        .await
        .expect("bounded call");
    }

    async fn send(socket: &mut Socket, method: &str, params: Value) {
        socket
            .send(Message::Text(
                json!({"jsonrpc":"2.0","id":1,"method":method,"params":params})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send");
    }

    async fn settles(mut condition: impl FnMut() -> bool) -> bool {
        tokio::time::timeout(Duration::from_secs(6), async {
            let mut consecutive = 0;
            loop {
                consecutive = if condition() { consecutive + 1 } else { 0 };
                if consecutive == 3 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .is_ok()
    }

    fn retains(observer: &RpcClientBudgetObserver, baseline: u64) -> bool {
        observer.snapshot().is_some_and(|snapshot| {
            Sample {
                bytes: snapshot.bytes as u64,
                responses: snapshot.outstanding_responses as u64,
            }
            .retains(baseline)
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn socket_stalls_retain_each_consumers_credit_and_release_on_disconnect() {
        let backend = Arc::new(Backend::new());
        let dispatcher = Arc::new(
            RpcDispatcher::new(backend.clone(), RpcAuthPolicy::default())
                .with_compatibility(RpcCompatibility::Extended),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/jsonrpc", listener.local_addr().unwrap());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve_loopback_websocket_listener_until(
            listener,
            dispatcher,
            async {
                let _ = stopped.await;
                Ok(())
            },
        ));
        let (mut event_socket, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        call(
            &mut event_socket,
            "ariax.setEventFilter",
            json!([{"methods":["bench.onSample"]}]),
        )
        .await;
        call(&mut event_socket, "observe", json!([0])).await;
        let event_observer = backend.observer(0);
        assert!(settles(|| event_observer.snapshot().unwrap().outstanding_responses == 0).await);
        let event_baseline = event_observer.snapshot().unwrap().bytes as u64;
        let event = RpcEvent::notification(
            "bench.onSample",
            json!({"padding":"x".repeat(512 * 1024)}),
            RpcEventClass::Coalesced,
            Some(RpcEventKey::new(None, "benchmark")),
        )
        .unwrap();
        for _ in 0..32 {
            backend.events.publish(event.clone());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            settles(|| retains(&event_observer, event_baseline)).await,
            "events: {:?}",
            event_observer.snapshot()
        );
        let (mut response_socket, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        call(
            &mut response_socket,
            "ariax.setEventFilter",
            json!([{"methods":["aria2.onDownloadError"]}]),
        )
        .await;
        call(&mut response_socket, "observe", json!([1])).await;
        let response_observer = backend.observer(1);
        assert!(settles(|| response_observer.snapshot().unwrap().outstanding_responses == 0).await);
        let response_baseline = response_observer.snapshot().unwrap().bytes as u64;
        tokio::time::timeout(Duration::from_secs(5), async {
            for _ in 0..64 {
                send(&mut response_socket, "large", json!([])).await;
            }
        })
        .await
        .expect("bounded request burst");
        assert!(
            settles(|| retains(&response_observer, response_baseline)).await,
            "responses: {:?}",
            response_observer.snapshot()
        );
        let (mut ordinary, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        call(&mut ordinary, "ping", json!([])).await;
        drop(ordinary);
        assert!(retains(&event_observer, event_baseline));
        drop(response_socket);
        assert!(
            settles(|| response_observer.snapshot().is_none()).await,
            "response credit after disconnect: {:?}",
            response_observer.snapshot()
        );
        assert!(retains(&event_observer, event_baseline));
        drop(event_socket);
        assert!(
            settles(|| event_observer.snapshot().is_none()).await,
            "event credit after disconnect: {:?}",
            event_observer.snapshot()
        );
        stop.send(()).unwrap();
        server.await.unwrap().unwrap();
        assert_eq!(backend.budgets.snapshot().bytes, 0);
    }
}
