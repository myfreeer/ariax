#![no_main]

use ariax_core::TaskId;
use ariax_engine::{
    HttpDiscardBudget, HttpDiscardBudgetLimits, HttpDiscardScope, HttpDiscardScopeLimits,
};
use libfuzzer_sys::fuzz_target;

const MAX_INPUT_BYTES: usize = 16_384;

fn value(data: &[u8], offset: usize) -> u64 {
    u64::from(data.get(offset).copied().unwrap_or_default()) + 1
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }
    let limits = HttpDiscardBudgetLimits {
        process_bytes: value(data, 0).min(4096),
        host_bytes: value(data, 1).min(2048),
        task_bytes: value(data, 2).min(2048),
        attempt_bytes: value(data, 3).min(1024),
    };
    let budget = HttpDiscardBudget::new(limits).expect("bounded nonzero limits");
    let task_limits = HttpDiscardScopeLimits {
        host_bytes: limits.host_bytes,
        task_bytes: limits.task_bytes,
        attempt_bytes: limits.attempt_bytes,
    };
    let task = budget
        .begin_task(
            TaskId::new(u64::from(data.get(4).copied().unwrap_or_default()) + 1)
                .expect("nonzero task id"),
            task_limits,
        )
        .expect("task");
    for chunk in data.chunks(8).take(256) {
        let host = format!(
            "https://host-{}.example.invalid",
            chunk.first().copied().unwrap_or_default() % 8
        );
        let attempt = task.begin_attempt(host).expect("attempt");
        let requested = u64::try_from(chunk.len()).unwrap_or(u64::MAX)
            + u64::from(chunk.get(1).copied().unwrap_or_default());
        let charge = attempt.charge_u64(requested);
        let snapshot = attempt.snapshot();
        assert!(snapshot.process_consumed <= limits.process_bytes);
        assert!(snapshot.host_consumed <= limits.host_bytes);
        assert!(snapshot.task_consumed <= limits.task_bytes);
        assert!(snapshot.attempt_consumed <= limits.attempt_bytes);
        assert!(charge.charged <= charge.requested);
        if charge.exhausted.is_some() {
            assert_eq!(attempt.available(), 0);
            let _ = attempt
                .exhausted_scope()
                .unwrap_or(HttpDiscardScope::Attempt);
        }
    }
});
