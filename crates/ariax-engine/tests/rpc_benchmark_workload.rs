#![forbid(unsafe_code)]

#[path = "../benches/rpc_active_profile/workload.rs"]
mod workload;

use std::ffi::OsStr;
use workload::Workload;

#[test]
fn default_geometry_and_small_diagnostics_preserve_complete_mutation_cycles() {
    assert_eq!(workload::SETTING, "ARIAX_BENCH_DIAGNOSTIC_SMALL");
    let full = Workload::from_setting(None).unwrap();
    assert_eq!(full, Workload::from_setting(Some(OsStr::new("0"))).unwrap());
    assert_eq!((full.ranges, full.samples), (1_000, 20_000));
    assert_eq!(full.scenario_seconds(), 90);
    assert_eq!(full.measurement_kind(), "full");
    assert!(!full.diagnostic_small);
    let small = Workload::from_setting(Some(OsStr::new("1"))).unwrap();
    assert_eq!((small.ranges, small.samples), (16, 1_600));
    assert_eq!(small.total_bytes(), 32 * 1024 * 1024);
    assert_eq!(small.scenario_seconds(), 30);
    assert_eq!(small.measurement_kind(), "diagnostic-small");
    assert!(small.diagnostic_small);
    for preset in [full, small] {
        assert_eq!(preset.ranges % 8, 0);
        assert_eq!(preset.samples % (20 * 8), 0);
        for scenario in ["http", "websocket", "content-length", "ndjson"] {
            assert!(preset.validate_scenario(scenario).is_ok());
        }
        assert!(preset.validate_scenario("unknown").is_err());
    }
    for scenario in ["mixed-bt", "administrative"] {
        assert!(full.validate_scenario(scenario).is_ok());
        assert!(small.validate_scenario(scenario).is_err());
    }
}

#[test]
fn invalid_or_ambiguous_presets_are_rejected() {
    for value in ["", "true", "false", "2", "01", "1 ", " 0"] {
        assert!(Workload::from_setting(Some(OsStr::new(value))).is_err());
    }
}
