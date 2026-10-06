#![forbid(unsafe_code)]

#[path = "../benches/rpc_active_profile/source_timing.rs"]
mod source_timing;

use serde_json::json;
use source_timing::{BackendLog, CAPACITY, ClientSample, correlate};
use std::time::Duration;

fn client(ordinal: usize) -> ClientSample {
    ClientSample {
        sample_index: 99 + ordinal * 160,
        burst: 1,
        started_unix_ns: 1_791_000_000_000_000_000,
        elapsed: Duration::from_nanos(100_250_123),
    }
}

fn completed(log: &mut BackendLog) {
    let ordinal = log.begin().unwrap();
    log.finish(
        ordinal,
        1_791_000_000_000_100_000,
        Duration::from_nanos(90_000_001),
        true,
    );
}

#[test]
fn source_calls_correlate_exact_ordinals_and_preserve_nanoseconds() {
    let mut log = BackendLog::default();
    completed(&mut log);
    completed(&mut log);
    let report = correlate(&[client(0), client(1)], &log.report().unwrap(), 320).unwrap();
    assert_eq!(report["calls"], 2);
    assert_eq!(report["samples"][1]["sampleIndex"], 259);
    assert_eq!(report["samples"][0]["roundTripNs"], 100_250_123);
    assert_eq!(report["samples"][0]["backendNs"], 90_000_001);
    assert_eq!(report["samples"][0]["outsideBackendNs"], 10_250_122);
    println!("sourceTimingFixture={report}");
}

#[test]
fn log_is_bounded_and_overflow_is_not_silently_truncated() {
    let mut log = BackendLog::default();
    for _ in 0..CAPACITY {
        completed(&mut log);
    }
    assert_eq!(log.report().unwrap().as_array().unwrap().len(), CAPACITY);
    assert!(log.begin().is_none());
    assert!(log.report().is_err());
}

#[test]
fn incomplete_duplicate_and_invalid_completions_reject() {
    let mut log = BackendLog::default();
    let ordinal = log.begin().unwrap();
    assert!(log.report().is_err());
    log.finish(ordinal, 1, Duration::ZERO, true);
    assert!(log.report().is_ok());
    log.finish(ordinal, 1, Duration::ZERO, true);
    assert!(log.report().is_err());
    let mut log = BackendLog::default();
    log.finish(0, 1, Duration::ZERO, true);
    assert!(log.report().is_err());
}

#[test]
fn missing_extra_reordered_and_failed_backend_records_reject() {
    let mut log = BackendLog::default();
    completed(&mut log);
    let good = log.report().unwrap();
    for bad in [json!(null), json!([]), json!([good[0], good[0]])] {
        assert!(correlate(&[client(0)], &bad, 160).is_err());
    }
    for (field, value) in [
        ("ordinal", json!(1)),
        ("succeeded", json!(false)),
        ("elapsedNs", json!(100_250_124)),
        ("elapsedNs", json!(-1)),
        ("startedUnixNs", json!(0)),
    ] {
        let mut bad = good.clone();
        bad[0][field] = value;
        assert!(correlate(&[client(0)], &bad, 160).is_err(), "{field}");
    }
}

#[test]
fn invalid_client_positions_geometry_and_anchors_reject() {
    let mut log = BackendLog::default();
    completed(&mut log);
    let backend = log.report().unwrap();
    for total in [0, 159, 161, 20_160] {
        assert!(correlate(&[client(0)], &backend, total).is_err());
    }
    assert!(correlate(&[], &backend, 160).is_err());
    assert!(correlate(&[client(1)], &backend, 160).is_err());
    let mut bad = client(0);
    bad.started_unix_ns = 0;
    assert!(correlate(&[bad], &backend, 160).is_err());
    let mut bad = client(0);
    bad.burst = 0;
    assert!(correlate(&[bad], &backend, 160).is_err());
}

#[test]
fn stages_require_complete_monotonic_offsets_within_backend_interval() {
    let mut log = BackendLog::default();
    completed(&mut log);
    log.attach_stages(0, Ok([1, 2, 3, 4, 5, 90_000_001]));
    let report = correlate(&[client(0)], &log.report().unwrap(), 160).unwrap();
    assert_eq!(report["samples"][0]["stageOffsetsNs"][5], 90_000_001);
    for stages in [
        json!([]),
        json!([1, 2, 3, 4, 5, 90_000_002]),
        json!([2, 1, 3, 4, 5, 6]),
        json!([1, 2, 3, 4, 5, true]),
    ] {
        let mut bad = log.report().unwrap();
        bad[0]["stageOffsetsNs"] = stages;
        assert!(correlate(&[client(0)], &bad, 160).is_err());
    }
    log.attach_stages(0, Ok([1, 2, 3, 4, 5, 6]));
    assert!(log.report().is_err());
    for stages in [
        Err("incomplete"),
        Ok([2, 1, 3, 4, 5, 6]),
        Ok([1, 2, 3, 4, 5, 90_000_002]),
    ] {
        let mut log = BackendLog::default();
        completed(&mut log);
        log.attach_stages(0, stages);
        assert!(log.report().is_err());
    }
    let mut log = BackendLog::default();
    completed(&mut log);
    completed(&mut log);
    log.attach_stages(0, Ok([1, 2, 3, 4, 5, 6]));
    assert!(correlate(&[client(0), client(1)], &log.report().unwrap(), 320).is_err());
}
