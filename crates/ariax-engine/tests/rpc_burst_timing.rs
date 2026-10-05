#![forbid(unsafe_code)]

#[path = "../benches/rpc_active_profile/burst_timing.rs"]
mod burst_timing;

use burst_timing::{BurstTiming, Phase};
use std::time::Duration;

#[test]
fn bursts_at_or_below_the_limit_pass_but_one_nanosecond_over_fails() {
    let timing = BurstTiming::new(0);
    for elapsed in [Duration::ZERO, Duration::from_millis(500)] {
        assert!(timing.check_limit("http", 1, elapsed).is_ok());
    }
    assert!(
        timing
            .check_limit(
                "http",
                1,
                Duration::from_millis(500) + Duration::from_nanos(1)
            )
            .is_err()
    );
}

#[test]
fn overrun_attributes_verification_and_preserves_sample_position() {
    let mut timing = BurstTiming::new(9517);
    timing.record(
        "bt.changeOption",
        Phase::Primary,
        9517,
        Duration::from_millis(1),
        Duration::from_millis(10),
    );
    timing.record(
        "bt.changeOption",
        Phase::Verification,
        9517,
        Duration::from_millis(12),
        Duration::from_millis(515),
    );
    let error = timing
        .check_limit("mixed-bt", 26, Duration::from_micros(530_097))
        .unwrap_err();
    assert!(error.starts_with("mixed-bt burst exceeded 500 ms: 530097 us;"));
    assert!(error.contains("burst=26 firstSampleIndex=9517 primaryCalls=1 verificationCalls=1"));
    assert!(error.contains("timedStepUs=525000 otherUs=5097"));
    let verification = "operation=bt.changeOption,phase=verification,sampleIndex=9517,startUs=12000,durationUs=515000";
    assert!(error.contains(&format!("last=[{verification}] slowest=[{verification}]")));
}

#[test]
fn slow_primary_call_remains_visible_after_a_faster_verification() {
    let mut timing = BurstTiming::new(19);
    timing.record(
        "changeUri",
        Phase::Primary,
        19,
        Duration::ZERO,
        Duration::from_millis(490),
    );
    timing.record(
        "changeUri",
        Phase::Verification,
        19,
        Duration::from_millis(491),
        Duration::from_millis(20),
    );
    let error = timing
        .check_limit("http", 2, Duration::from_millis(512))
        .unwrap_err();
    assert!(error.contains("last=[operation=changeUri,phase=verification,sampleIndex=19"));
    assert!(error.contains("slowest=[operation=changeUri,phase=primary,sampleIndex=19"));
    assert!(error.contains("timedStepUs=510000 otherUs=2000"));
}

#[test]
fn untimed_overrun_reports_no_completed_steps() {
    let timing = BurstTiming::new(20);
    let error = timing
        .check_limit("ndjson", 3, Duration::from_millis(501))
        .unwrap_err();
    assert!(error.contains("firstSampleIndex=20 primaryCalls=0 verificationCalls=0"));
    assert!(error.contains("timedStepUs=0 otherUs=501000 last=[none] slowest=[none]"));
}
