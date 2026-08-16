use std::error::Error;
use std::fmt;
use std::time::Duration;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
const GIB: usize = 1024 * MIB;

pub const PROFILE_CONTROL_HANDLE_RESERVE: usize = 64;
pub const C10K_LOW_ACTIVITY_SOCKET_TARGET: usize = 10_000;
pub const HTTP_IDLE_CONNECTION_RESERVATION_BYTES: usize = 32 * KIB;
pub const HTTP_ACTIVE_TLS_CONNECTION_CEILING_BYTES: usize = 96 * KIB;

/// The single coordinated user-facing runtime profile.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum RuntimeProfile {
    #[default]
    Auto,
    Concurrency,
    Throughput,
    Latency,
    Compact,
}

impl RuntimeProfile {
    pub const ALL: [Self; 5] = [
        Self::Auto,
        Self::Concurrency,
        Self::Throughput,
        Self::Latency,
        Self::Compact,
    ];

    pub const CONCRETE: [Self; 4] = [
        Self::Concurrency,
        Self::Throughput,
        Self::Latency,
        Self::Compact,
    ];

    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Concurrency => "concurrency",
            Self::Throughput => "throughput",
            Self::Latency => "latency",
            Self::Compact => "compact",
        }
    }

    pub fn parse(value: &str) -> Result<Self, ProfileCapacityError> {
        match value {
            "auto" => Ok(Self::Auto),
            "concurrency" => Ok(Self::Concurrency),
            "throughput" => Ok(Self::Throughput),
            "latency" => Ok(Self::Latency),
            "compact" => Ok(Self::Compact),
            _ => Err(ProfileCapacityError::InvalidProfile),
        }
    }

    /// `auto` begins at concurrency-safe limits and may adapt only inside the
    /// documented concurrency-to-throughput guardrails.
    #[must_use]
    pub const fn baseline(self) -> Self {
        match self {
            Self::Auto => Self::Concurrency,
            concrete => concrete,
        }
    }

    #[must_use]
    pub const fn claims_c10k(self) -> bool {
        matches!(self, Self::Auto | Self::Concurrency)
    }
}

/// Exact registry-owned defaults for one concrete profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProfileLimits {
    pub resident_target_bytes: usize,
    pub accounted_resident_limit_bytes: usize,
    pub process_handle_target: usize,
    pub logical_socket_cap: usize,
    pub logical_file_cap: usize,
    pub control_urgent_capacity: usize,
    pub control_bulk_capacity: usize,
    pub urgent_burst: usize,
    pub write_lane_items: usize,
    pub write_lane_bytes: usize,
    pub disk_queue_ops: usize,
    pub disk_queue_bytes: usize,
    pub hash_lane_jobs: usize,
    pub hash_lane_bytes: usize,
    pub journal_inbox_capacity: usize,
    pub durability_group_bytes: usize,
    pub durability_group_age: Duration,
    pub durability_group_pieces: usize,
    pub buffer_budget_bytes: usize,
    pub quarantine_budget_bytes: usize,
    pub http_ingress_budget_bytes: usize,
    pub sftp_ingress_budget_bytes: usize,
    pub piece_metadata_budget_bytes: usize,
    pub task_metadata_budget_bytes: usize,
    pub transform_budget_bytes: usize,
    pub rpc_pending_items: usize,
    pub rpc_pending_bytes: usize,
    pub journal_state_budget_bytes: usize,
    pub sqlite_cache_budget_bytes: usize,
    pub metadata_cache_budget_bytes: usize,
    pub cpu_scratch_budget_bytes: usize,
    pub worker_stack_bytes: usize,
    pub event_queue_events_per_client: usize,
    pub event_queue_bytes_per_client: usize,
    pub stopped_result_retention: usize,
    pub http_idle_connections_global: usize,
    pub http_idle_connections_per_origin: usize,
    pub http_idle_pool_budget_bytes: usize,
    pub http_idle_timeout: Duration,
    pub dns_positive_entries: usize,
    pub dns_negative_entries: usize,
    pub cookie_entries: usize,
    pub server_stat_entries: usize,
    pub exported_metric_top_n: usize,
}

impl ProfileLimits {
    #[must_use]
    pub const fn for_profile(profile: RuntimeProfile) -> Self {
        match profile.baseline() {
            RuntimeProfile::Auto => unreachable!(),
            RuntimeProfile::Concurrency => Self {
                resident_target_bytes: GIB,
                accounted_resident_limit_bytes: 896 * MIB,
                process_handle_target: 16_384,
                logical_socket_cap: 12_288,
                logical_file_cap: 4_096,
                control_urgent_capacity: 256,
                control_bulk_capacity: 1_024,
                urgent_burst: 32,
                write_lane_items: 512,
                write_lane_bytes: 64 * MIB,
                disk_queue_ops: 128,
                disk_queue_bytes: 64 * MIB,
                hash_lane_jobs: 64,
                hash_lane_bytes: 32 * MIB,
                journal_inbox_capacity: 1_024,
                durability_group_bytes: 16 * MIB,
                durability_group_age: Duration::from_secs(1),
                durability_group_pieces: 1_024,
                buffer_budget_bytes: 256 * MIB,
                quarantine_budget_bytes: 32 * MIB,
                http_ingress_budget_bytes: 64 * MIB,
                sftp_ingress_budget_bytes: 32 * MIB,
                piece_metadata_budget_bytes: 128 * MIB,
                task_metadata_budget_bytes: 64 * MIB,
                transform_budget_bytes: 0,
                rpc_pending_items: 128,
                rpc_pending_bytes: 64 * MIB,
                journal_state_budget_bytes: 32 * MIB,
                sqlite_cache_budget_bytes: 16 * MIB,
                metadata_cache_budget_bytes: 32 * MIB,
                cpu_scratch_budget_bytes: 32 * MIB,
                worker_stack_bytes: 2 * MIB,
                event_queue_events_per_client: 256,
                event_queue_bytes_per_client: 4 * MIB,
                stopped_result_retention: 1_000,
                http_idle_connections_global: 512,
                http_idle_connections_per_origin: 2,
                http_idle_pool_budget_bytes: 32 * MIB,
                http_idle_timeout: Duration::from_secs(60),
                dns_positive_entries: 4_096,
                dns_negative_entries: 512,
                cookie_entries: 3_000,
                server_stat_entries: 4_096,
                exported_metric_top_n: 100,
            },
            RuntimeProfile::Throughput => Self {
                resident_target_bytes: 2 * GIB,
                accounted_resident_limit_bytes: 1_792 * MIB,
                process_handle_target: 8_192,
                logical_socket_cap: 4_096,
                logical_file_cap: 4_096,
                control_urgent_capacity: 256,
                control_bulk_capacity: 1_024,
                urgent_burst: 32,
                write_lane_items: 1_024,
                write_lane_bytes: 256 * MIB,
                disk_queue_ops: 256,
                disk_queue_bytes: 256 * MIB,
                hash_lane_jobs: 128,
                hash_lane_bytes: 128 * MIB,
                journal_inbox_capacity: 2_048,
                durability_group_bytes: 64 * MIB,
                durability_group_age: Duration::from_secs(2),
                durability_group_pieces: 4_096,
                buffer_budget_bytes: GIB,
                quarantine_budget_bytes: 64 * MIB,
                http_ingress_budget_bytes: 256 * MIB,
                sftp_ingress_budget_bytes: 128 * MIB,
                piece_metadata_budget_bytes: 256 * MIB,
                task_metadata_budget_bytes: 256 * MIB,
                transform_budget_bytes: 0,
                rpc_pending_items: 128,
                rpc_pending_bytes: 64 * MIB,
                journal_state_budget_bytes: 64 * MIB,
                sqlite_cache_budget_bytes: 32 * MIB,
                metadata_cache_budget_bytes: 64 * MIB,
                cpu_scratch_budget_bytes: 128 * MIB,
                worker_stack_bytes: 2 * MIB,
                event_queue_events_per_client: 256,
                event_queue_bytes_per_client: 4 * MIB,
                stopped_result_retention: 1_000,
                http_idle_connections_global: 256,
                http_idle_connections_per_origin: 8,
                http_idle_pool_budget_bytes: 32 * MIB,
                http_idle_timeout: Duration::from_secs(60),
                dns_positive_entries: 4_096,
                dns_negative_entries: 512,
                cookie_entries: 3_000,
                server_stat_entries: 4_096,
                exported_metric_top_n: 100,
            },
            RuntimeProfile::Latency => Self {
                resident_target_bytes: 768 * MIB,
                accounted_resident_limit_bytes: 672 * MIB,
                process_handle_target: 8_192,
                logical_socket_cap: 4_096,
                logical_file_cap: 2_048,
                control_urgent_capacity: 256,
                control_bulk_capacity: 512,
                urgent_burst: 16,
                write_lane_items: 256,
                write_lane_bytes: 32 * MIB,
                disk_queue_ops: 64,
                disk_queue_bytes: 32 * MIB,
                hash_lane_jobs: 32,
                hash_lane_bytes: 16 * MIB,
                journal_inbox_capacity: 512,
                durability_group_bytes: 4 * MIB,
                durability_group_age: Duration::from_millis(250),
                durability_group_pieces: 256,
                buffer_budget_bytes: 128 * MIB,
                quarantine_budget_bytes: 16 * MIB,
                http_ingress_budget_bytes: 32 * MIB,
                sftp_ingress_budget_bytes: 16 * MIB,
                piece_metadata_budget_bytes: 64 * MIB,
                task_metadata_budget_bytes: 32 * MIB,
                transform_budget_bytes: 0,
                rpc_pending_items: 256,
                rpc_pending_bytes: 128 * MIB,
                journal_state_budget_bytes: 32 * MIB,
                sqlite_cache_budget_bytes: 8 * MIB,
                metadata_cache_budget_bytes: 16 * MIB,
                cpu_scratch_budget_bytes: 16 * MIB,
                worker_stack_bytes: 2 * MIB,
                event_queue_events_per_client: 512,
                event_queue_bytes_per_client: 8 * MIB,
                stopped_result_retention: 1_000,
                http_idle_connections_global: 128,
                http_idle_connections_per_origin: 2,
                http_idle_pool_budget_bytes: 16 * MIB,
                http_idle_timeout: Duration::from_secs(30),
                dns_positive_entries: 2_048,
                dns_negative_entries: 256,
                cookie_entries: 3_000,
                server_stat_entries: 2_048,
                exported_metric_top_n: 100,
            },
            RuntimeProfile::Compact => Self {
                resident_target_bytes: 128 * MIB,
                accounted_resident_limit_bytes: 112 * MIB,
                process_handle_target: 1_024,
                logical_socket_cap: 512,
                logical_file_cap: 512,
                control_urgent_capacity: 64,
                control_bulk_capacity: 128,
                urgent_burst: 8,
                write_lane_items: 128,
                write_lane_bytes: 8 * MIB,
                disk_queue_ops: 32,
                disk_queue_bytes: 8 * MIB,
                hash_lane_jobs: 16,
                hash_lane_bytes: 4 * MIB,
                journal_inbox_capacity: 256,
                durability_group_bytes: 8 * MIB,
                durability_group_age: Duration::from_secs(2),
                durability_group_pieces: 512,
                buffer_budget_bytes: 32 * MIB,
                quarantine_budget_bytes: 8 * MIB,
                http_ingress_budget_bytes: 8 * MIB,
                sftp_ingress_budget_bytes: 4 * MIB,
                piece_metadata_budget_bytes: 16 * MIB,
                task_metadata_budget_bytes: 8 * MIB,
                transform_budget_bytes: 0,
                rpc_pending_items: 32,
                rpc_pending_bytes: 32 * MIB,
                journal_state_budget_bytes: 20 * MIB,
                sqlite_cache_budget_bytes: 4 * MIB,
                metadata_cache_budget_bytes: 4 * MIB,
                cpu_scratch_budget_bytes: 4 * MIB,
                worker_stack_bytes: MIB,
                event_queue_events_per_client: 64,
                event_queue_bytes_per_client: MIB,
                stopped_result_retention: 250,
                http_idle_connections_global: 32,
                http_idle_connections_per_origin: 1,
                http_idle_pool_budget_bytes: 4 * MIB,
                http_idle_timeout: Duration::from_secs(30),
                dns_positive_entries: 512,
                dns_negative_entries: 64,
                cookie_entries: 512,
                server_stat_entries: 512,
                exported_metric_top_n: 32,
            },
        }
    }

    #[must_use]
    pub const fn max_connections_per_origin(self) -> usize {
        self.http_idle_connections_per_origin
    }
}

/// Profile defaults after native process-handle capability is applied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedRuntimeProfile {
    requested: RuntimeProfile,
    baseline: RuntimeProfile,
    limits: ProfileLimits,
    native_handle_limit: Option<usize>,
    process_handle_capacity: usize,
    logical_socket_capacity: usize,
    logical_file_capacity: usize,
    connection_overhead_budget_bytes: usize,
}

impl ResolvedRuntimeProfile {
    #[must_use]
    pub fn resolve(profile: RuntimeProfile, native_handle_limit: Option<usize>) -> Self {
        let baseline = profile.baseline();
        let limits = ProfileLimits::for_profile(baseline);
        let native_capacity = native_handle_limit.map_or(limits.process_handle_target, |limit| {
            limit.saturating_sub(PROFILE_CONTROL_HANDLE_RESERVE)
        });
        let process_handle_capacity = limits.process_handle_target.min(native_capacity);
        let logical_socket_capacity = limits.logical_socket_cap.min(process_handle_capacity);
        let logical_file_capacity = limits.logical_file_cap.min(process_handle_capacity);
        let connection_overhead_budget_bytes = logical_socket_capacity
            .saturating_mul(HTTP_IDLE_CONNECTION_RESERVATION_BYTES)
            .min(limits.accounted_resident_limit_bytes);
        Self {
            requested: profile,
            baseline,
            limits,
            native_handle_limit,
            process_handle_capacity,
            logical_socket_capacity,
            logical_file_capacity,
            connection_overhead_budget_bytes,
        }
    }

    #[must_use]
    pub fn resolve_native(profile: RuntimeProfile) -> Self {
        Self::resolve(profile, native_process_handle_limit())
    }

    #[must_use]
    pub const fn requested(self) -> RuntimeProfile {
        self.requested
    }

    #[must_use]
    pub const fn baseline(self) -> RuntimeProfile {
        self.baseline
    }

    #[must_use]
    pub const fn limits(self) -> ProfileLimits {
        self.limits
    }

    #[must_use]
    pub const fn native_handle_limit(self) -> Option<usize> {
        self.native_handle_limit
    }

    #[must_use]
    pub const fn process_handle_capacity(self) -> usize {
        self.process_handle_capacity
    }

    #[must_use]
    pub const fn logical_socket_capacity(self) -> usize {
        self.logical_socket_capacity
    }

    #[must_use]
    pub const fn logical_file_capacity(self) -> usize {
        self.logical_file_capacity
    }

    #[must_use]
    pub const fn connection_overhead_budget_bytes(self) -> usize {
        self.connection_overhead_budget_bytes
    }

    #[must_use]
    pub fn low_activity_socket_capacity(self) -> usize {
        self.logical_socket_capacity
            .min(self.connection_overhead_budget_bytes / HTTP_IDLE_CONNECTION_RESERVATION_BYTES)
    }

    pub fn require_c10k(self) -> Result<(), ProfileCapacityError> {
        if !self.requested.claims_c10k() {
            return Err(ProfileCapacityError::ProfileDoesNotClaimC10k {
                profile: self.requested,
            });
        }
        let available = self.low_activity_socket_capacity();
        if available < C10K_LOW_ACTIVITY_SOCKET_TARGET {
            return Err(ProfileCapacityError::InsufficientC10kCapacity {
                required: C10K_LOW_ACTIVITY_SOCKET_TARGET,
                available,
                native_handle_limit: self.native_handle_limit,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProfileCapacityError {
    InvalidProfile,
    ProfileDoesNotClaimC10k {
        profile: RuntimeProfile,
    },
    InsufficientC10kCapacity {
        required: usize,
        available: usize,
        native_handle_limit: Option<usize>,
    },
}

impl fmt::Display for ProfileCapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProfile => formatter.write_str("invalid runtime profile"),
            Self::ProfileDoesNotClaimC10k { profile } => {
                write!(
                    formatter,
                    "runtime profile {} does not claim C10k",
                    profile.code()
                )
            }
            Self::InsufficientC10kCapacity {
                required,
                available,
                native_handle_limit,
            } => write!(
                formatter,
                "C10k capacity unavailable: required {required} low-activity sockets, resolved {available}, native handle limit {native_handle_limit:?}"
            ),
        }
    }
}

impl Error for ProfileCapacityError {}

#[cfg(unix)]
#[must_use]
pub fn native_process_handle_limit() -> Option<usize> {
    rustix::process::getrlimit(rustix::process::Resource::Nofile)
        .current
        .and_then(|value| usize::try_from(value).ok())
}

#[cfg(not(unix))]
#[must_use]
pub const fn native_process_handle_limit() -> Option<usize> {
    None
}

#[cfg(test)]
mod tests {
    use super::{
        C10K_LOW_ACTIVITY_SOCKET_TARGET, HTTP_IDLE_CONNECTION_RESERVATION_BYTES,
        PROFILE_CONTROL_HANDLE_RESERVE, ProfileCapacityError, ProfileLimits,
        ResolvedRuntimeProfile, RuntimeProfile,
    };

    #[test]
    fn profile_defaults_are_exact_and_auto_starts_concurrency_safe() {
        assert_eq!(RuntimeProfile::parse("auto"), Ok(RuntimeProfile::Auto));
        assert_eq!(RuntimeProfile::Auto.baseline(), RuntimeProfile::Concurrency);
        assert_eq!(RuntimeProfile::ALL.len(), 5);
        assert!(RuntimeProfile::parse("CONCURRENCY").is_err());

        let concurrency = ProfileLimits::for_profile(RuntimeProfile::Concurrency);
        assert_eq!(concurrency.resident_target_bytes, 1024 * 1024 * 1024);
        assert_eq!(
            concurrency.accounted_resident_limit_bytes,
            896 * 1024 * 1024
        );
        assert_eq!(concurrency.process_handle_target, 16_384);
        assert_eq!(concurrency.logical_socket_cap, 12_288);
        assert_eq!(concurrency.logical_file_cap, 4_096);
        assert_eq!(concurrency.http_idle_connections_global, 512);
        assert_eq!(concurrency.http_idle_connections_per_origin, 2);
        assert_eq!(concurrency.http_ingress_budget_bytes, 64 * 1024 * 1024);

        for profile in RuntimeProfile::CONCRETE {
            let limits = ProfileLimits::for_profile(profile);
            assert_eq!(
                limits.accounted_resident_limit_bytes,
                limits.resident_target_bytes / 8 * 7
            );
            assert!(limits.logical_socket_cap <= limits.process_handle_target);
            assert!(limits.logical_file_cap <= limits.process_handle_target);
            assert!(limits.http_idle_connections_per_origin > 0);
            assert_eq!(
                limits.http_idle_connections_global % limits.http_idle_connections_per_origin,
                0
            );
        }
    }

    #[test]
    fn native_handle_limit_scales_caps_and_c10k_failure_is_explicit() {
        let low_native = C10K_LOW_ACTIVITY_SOCKET_TARGET
            .saturating_sub(1)
            .saturating_add(PROFILE_CONTROL_HANDLE_RESERVE);
        let resolved =
            ResolvedRuntimeProfile::resolve(RuntimeProfile::Concurrency, Some(low_native));
        assert_eq!(
            resolved.process_handle_capacity(),
            C10K_LOW_ACTIVITY_SOCKET_TARGET - 1
        );
        assert!(matches!(
            resolved.require_c10k(),
            Err(ProfileCapacityError::InsufficientC10kCapacity {
                required: C10K_LOW_ACTIVITY_SOCKET_TARGET,
                available,
                native_handle_limit: Some(limit),
            }) if available == C10K_LOW_ACTIVITY_SOCKET_TARGET - 1 && limit == low_native
        ));

        let exact_native = C10K_LOW_ACTIVITY_SOCKET_TARGET + PROFILE_CONTROL_HANDLE_RESERVE;
        let exact =
            ResolvedRuntimeProfile::resolve(RuntimeProfile::Concurrency, Some(exact_native));
        assert_eq!(
            exact.connection_overhead_budget_bytes(),
            C10K_LOW_ACTIVITY_SOCKET_TARGET * HTTP_IDLE_CONNECTION_RESERVATION_BYTES
        );
        assert_eq!(
            exact.low_activity_socket_capacity(),
            C10K_LOW_ACTIVITY_SOCKET_TARGET
        );
        assert_eq!(exact.require_c10k(), Ok(()));
    }

    #[test]
    fn only_auto_and_concurrency_claim_c10k() {
        for profile in [
            RuntimeProfile::Throughput,
            RuntimeProfile::Latency,
            RuntimeProfile::Compact,
        ] {
            let resolved = ResolvedRuntimeProfile::resolve(profile, None);
            assert!(matches!(
                resolved.require_c10k(),
                Err(ProfileCapacityError::ProfileDoesNotClaimC10k { profile: rejected })
                    if rejected == profile
            ));
        }
    }
}
