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
