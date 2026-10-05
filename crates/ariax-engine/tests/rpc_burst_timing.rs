#![forbid(unsafe_code)]

#[path = "../benches/rpc_active_profile/burst_timing.rs"]
mod burst_timing;

use burst_timing::{BurstTiming, CompletedBursts, Phase};
use serde_json::json;
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
    assert!(error.contains("primaryStepUs=10000 verificationStepUs=515000"));
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

#[test]
fn completed_bursts_keep_the_longest_snapshot_and_separate_all_totals() {
    let mut completed = CompletedBursts::default();
    let mut first = BurstTiming::new(0);
    first.record(
        "pause",
        Phase::Primary,
        0,
        Duration::ZERO,
        Duration::from_millis(20),
    );
    completed
        .record(
            "mixed-bt",
            1,
            1_791_187_200_000_000_000,
            Duration::from_millis(30),
            first,
        )
        .unwrap();
    let mut longest = BurstTiming::new(1);
    longest.record(
        "bt.changeOption",
        Phase::Primary,
        1,
        Duration::from_millis(1),
        Duration::from_millis(100),
    );
    longest.record(
        "bt.changeOption",
        Phase::Verification,
        1,
        Duration::from_millis(110),
        Duration::from_millis(350),
    );
    completed
        .record(
            "mixed-bt",
            2,
            1_791_187_200_000_000_000,
            Duration::from_millis(486),
            longest,
        )
        .unwrap();
    let mut last = BurstTiming::new(2);
    last.record(
        "pause",
        Phase::Primary,
        2,
        Duration::ZERO,
        Duration::from_millis(10),
    );
    completed
        .record(
            "mixed-bt",
            3,
            1_791_187_200_000_000_000,
            Duration::from_millis(15),
            last,
        )
        .unwrap();
    let report = completed.report();
    assert_eq!(report["primaryUs"], 130_000);
    assert_eq!(report["verificationUs"], 350_000);
    assert_eq!(report["otherUs"], 51_000);
    assert_eq!(
        report["worstCompletedBurst"],
        json!({
            "startedUnixNs": 1_791_187_200_000_000_000_u64,
            "burst": 2, "elapsedUs": 486_000, "elapsedNs": 486_000_000,
            "firstSampleIndex": 1, "primaryCalls": 1, "verificationCalls": 1,
            "primaryUs": 100_000, "verificationUs": 350_000, "otherUs": 36_000,
            "last": {"operation": "bt.changeOption", "phase": "verification", "sampleIndex": 1,
                     "startUs": 110_000, "durationUs": 350_000},
            "slowest": {"operation": "bt.changeOption", "phase": "verification", "sampleIndex": 1,
                        "startUs": 110_000, "durationUs": 350_000}
        })
    );
    // Also consumed by the short local cross-language report validation.
    println!("burstTimingFixture={report}");
}

#[test]
fn exact_ties_keep_the_first_burst_and_rejected_bursts_leave_totals_unchanged() {
    let mut completed = CompletedBursts::default();
    for burst in 1..=2 {
        completed
            .record(
                "http",
                burst,
                1_791_187_200_000_000_000,
                Duration::from_millis(500),
                BurstTiming::new(burst - 1),
            )
            .unwrap();
    }
    let before = completed.report();
    assert_eq!(before["worstCompletedBurst"]["burst"], 1);
    assert!(before["worstCompletedBurst"]["last"].is_null());
    assert!(
        completed
            .record(
                "http",
                3,
                1_791_187_200_000_000_000,
                Duration::from_millis(500) + Duration::from_nanos(1),
                BurstTiming::new(2)
            )
            .is_err()
    );
    assert_eq!(completed.report(), before);
    let mut invalid = BurstTiming::new(2);
    invalid.record(
        "pause",
        Phase::Primary,
        2,
        Duration::ZERO,
        Duration::from_millis(2),
    );
    assert!(
        completed
            .record(
                "http",
                3,
                1_791_187_200_000_000_000,
                Duration::from_millis(1),
                invalid
            )
            .is_err()
    );
    assert_eq!(completed.report(), before);
}

#[test]
fn completed_totals_preserve_submicrosecond_work_before_final_rounding() {
    let mut completed = CompletedBursts::default();
    for burst in 1..=2 {
        let mut timing = BurstTiming::new(burst - 1);
        timing.record(
            "pause",
            Phase::Primary,
            burst - 1,
            Duration::ZERO,
            Duration::from_nanos(900),
        );
        timing.record(
            "pause",
            Phase::Verification,
            burst - 1,
            Duration::from_nanos(900),
            Duration::from_nanos(900),
        );
        completed
            .record(
                "http",
                burst,
                1_791_187_200_000_000_000,
                Duration::from_nanos(2_700),
                timing,
            )
            .unwrap();
    }
    let report = completed.report();
    assert_eq!(report["primaryUs"], 1);
    assert_eq!(report["verificationUs"], 1);
    assert_eq!(report["otherUs"], 1);
    let worst = &report["worstCompletedBurst"];
    assert_eq!(worst["elapsedUs"], 2);
    assert_eq!(worst["elapsedNs"], 2_700);
    assert_eq!(worst["primaryUs"], 0);
    assert_eq!(worst["verificationUs"], 0);
    assert_eq!(worst["otherUs"], 0);
}

#[test]
fn failed_burst_retains_the_wall_clock_correlation_anchor() {
    let error = CompletedBursts::default()
        .record(
            "mixed-bt",
            26,
            1_791_187_200_000_000_000,
            Duration::from_micros(530_097),
            BurstTiming::new(9517),
        )
        .unwrap_err();
    assert!(error.contains("burst=26 firstSampleIndex=9517"));
    assert!(error.ends_with("burstStartedUnixNs=1791187200000000000"));
}
