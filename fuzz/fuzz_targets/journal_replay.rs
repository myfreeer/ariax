#![no_main]

use ariax_storage::{ReplayLimits, ReplayResource, ReplayStop, replay_ordered_segments};
use libfuzzer_sys::fuzz_target;

const MAX_INPUT_BYTES: usize = 128 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }
    let segments: Vec<&[u8]> = data.chunks(16 * 1024).take(8).collect();
    let limits = ReplayLimits {
        max_segments: 8,
        max_records: 256,
        max_payload_bytes: 64 * 1024,
    };
    let replay = replay_ordered_segments(&segments, limits);
    assert!(replay.records.len() <= limits.max_records);
    assert!(replay.payload_bytes <= limits.max_payload_bytes);
    assert!(replay.valid_segment_prefixes.len() <= segments.len());
    if let ReplayStop::ResourceLimit(resource) = replay.stop {
        assert!(matches!(
            resource,
            ReplayResource::Segments | ReplayResource::Records | ReplayResource::PayloadBytes
        ));
    }
});
