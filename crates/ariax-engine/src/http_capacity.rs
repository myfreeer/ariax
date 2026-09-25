use crate::{
    HttpDirectTransportConfig, HttpDiscardBudget, HttpIngressBudgets, HttpMultiRangeWorkerConfig,
    HttpPolicyClientConfig, HttpProxyConnectConfig, HttpProxyRequestConfig, HttpTransportBudgets,
    HttpTransportError, RpcBudgets, StorageEngineConfig,
};
use ariax_runtime::{
    ByteBudget, HTTP_IDLE_CONNECTION_RESERVATION_BYTES, HandleBudgetError, HandleBudgetLimits,
    HandleBudgets, ProfileCapacityError, ResolvedRuntimeProfile, RuntimeProfile,
    native_process_handle_limit,
};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;

/// One resolved process-wide direct-HTTP resource set. Direct client/worker
/// clones made from this object share socket-handle, connection-overhead,
/// ingress, buffer, and global resident admission. Proxy sockets and storage
/// file descriptors consume the same process-owned handle domains.
#[derive(Clone, Debug)]
pub struct HttpProcessResources {
    profile: ResolvedRuntimeProfile,
    resident: ByteBudget,
    #[cfg(feature = "bt")]
    native_threads: ByteBudget,
    transport: HttpTransportBudgets,
    ingress: HttpIngressBudgets,
    protocol_metadata: HttpIngressBudgets,
    sftp_ingress: HttpIngressBudgets,
    discard: HttpDiscardBudget,
    rpc: RpcBudgets,
    scheduling: crate::HttpSchedulingPolicy,
    cpu: ariax_runtime::CpuPool,
    server_stats: crate::ServerStatistics,
}

impl HttpProcessResources {
    pub fn for_profile(profile: RuntimeProfile) -> Result<Self, HttpCapacityError> {
        Self::with_native_handle_limit(profile, native_process_handle_limit())
    }

    pub fn with_native_handle_limit(
        profile: RuntimeProfile,
        native_handle_limit: Option<usize>,
    ) -> Result<Self, HttpCapacityError> {
        let resolved = ResolvedRuntimeProfile::resolve(profile, native_handle_limit);
        if resolved.process_handle_capacity() == 0
            || resolved.logical_socket_capacity() == 0
            || resolved.logical_file_capacity() == 0
        {
            return Err(HttpCapacityError::NoUsableHandles);
        }
        let handles = HandleBudgets::new(HandleBudgetLimits {
            process: resolved.process_handle_capacity(),
            sockets: resolved.logical_socket_capacity(),
            files: resolved.logical_file_capacity(),
        })
        .map_err(HttpCapacityError::Handles)?;
        let limits = resolved.limits();
        let resident = ByteBudget::new(limits.accounted_resident_limit_bytes);
        let transport = HttpTransportBudgets::with_shared_resident(
            handles,
            resolved.connection_overhead_budget_bytes(),
            resident.clone(),
            HTTP_IDLE_CONNECTION_RESERVATION_BYTES,
        )
        .map_err(HttpCapacityError::Transport)?;
        let ingress = HttpIngressBudgets::with_shared_resident(
            limits.http_ingress_budget_bytes,
            resident.clone(),
        );
        let cpu = ariax_runtime::CpuPool::new(ariax_runtime::CpuPoolConfig {
            workers: if profile == RuntimeProfile::Compact {
                1
            } else {
                2
            },
            jobs: 512,
            bytes: limits.cpu_scratch_budget_bytes,
            resident: resident.clone(),
            shared_disk: profile == RuntimeProfile::Compact,
        })
        .map_err(HttpCapacityError::Cpu)?;
        Ok(Self {
            #[cfg(feature = "bt")]
            native_threads: ByteBudget::new(if profile == RuntimeProfile::Compact {
                4
            } else {
                8
            }),
            server_stats: crate::ServerStatistics::new(
                limits.server_stat_entries,
                std::time::Duration::from_secs(86400),
                HttpIngressBudgets::with_shared_resident(
                    limits.metadata_cache_budget_bytes,
                    resident.clone(),
                ),
            ),
            protocol_metadata: HttpIngressBudgets::with_shared_resident(
                limits.task_metadata_budget_bytes,
                resident.clone(),
            ),
            sftp_ingress: HttpIngressBudgets::with_shared_resident(
                limits.sftp_ingress_budget_bytes,
                resident.clone(),
            ),
            cpu,
            profile: resolved,
            rpc: RpcBudgets::with_shared_resident(resolved, resident.clone()),
            scheduling: crate::HttpSchedulingPolicy::default(),
            resident,
            transport,
            ingress,
            discard: HttpDiscardBudget::default(),
        })
    }

    #[must_use]
    pub const fn profile(&self) -> ResolvedRuntimeProfile {
        self.profile
    }

    pub fn require_c10k(&self) -> Result<(), HttpCapacityError> {
        self.profile
            .require_c10k()
            .map_err(HttpCapacityError::Profile)
    }

    #[must_use]
    pub fn resident_budget(&self) -> ByteBudget {
        self.resident.clone()
    }

    #[cfg(feature = "bt")]
    pub fn bt_resources(&self) -> ariax_bt::BtResources {
        ariax_bt::BtResources {
            resident: self.resident.clone(),
            handles: self.transport.handle_budgets(),
            threads: self.native_threads.clone(),
        }
    }

    pub(crate) fn metadata_budget(&self) -> HttpIngressBudgets {
        self.protocol_metadata.clone()
    }

    pub fn cpu_pool(&self) -> ariax_runtime::CpuPool {
        self.cpu.clone()
    }

    #[must_use]
    pub fn rpc_budgets(&self) -> RpcBudgets {
        self.rpc.clone()
    }

    pub fn scheduling_policy(&self) -> crate::HttpSchedulingPolicy {
        self.scheduling.clone()
    }

    #[must_use]
    pub fn transport_budgets(&self) -> HttpTransportBudgets {
        self.transport.clone()
    }

    #[must_use]
    pub fn ingress_budgets(&self) -> HttpIngressBudgets {
        self.ingress.clone()
    }

    #[must_use]
    pub fn discard_budget(&self) -> HttpDiscardBudget {
        self.discard.clone()
    }

    #[must_use]
    pub fn policy_client_config(&self) -> HttpPolicyClientConfig {
        let limits = self.profile.limits();
        let per_origin = limits.max_connections_per_origin();
        let proxy_request = HttpProxyRequestConfig {
            connect: HttpProxyConnectConfig {
                budgets: self.transport.clone(),
                ..HttpProxyConnectConfig::default()
            },
            ..HttpProxyRequestConfig::default()
        };
        HttpPolicyClientConfig {
            direct: HttpDirectTransportConfig {
                max_connections_per_origin: per_origin,
                max_idle_connections_per_origin: per_origin,
                idle_timeout: limits.http_idle_timeout,
                budgets: self.transport.clone(),
                ..HttpDirectTransportConfig::default()
            },
            direct_transport_cache_capacity: limits
                .http_idle_connections_global
                .checked_div(per_origin)
                .unwrap_or(0),
            proxy_request,
            ..HttpPolicyClientConfig::default()
        }
    }

    #[must_use]
    pub fn worker_config(&self, journal_root: PathBuf) -> HttpMultiRangeWorkerConfig {
        let limits = self.profile.limits();
        let storage = StorageEngineConfig {
            disk_queue_capacity: limits.disk_queue_ops,
            disk_completion_capacity: limits.disk_queue_ops,
            max_in_flight_bytes: limits.disk_queue_bytes,
            buffer_pool_bytes: limits.buffer_budget_bytes,
            resident_budget: self.resident.clone(),
            handle_budgets: Some(self.transport.handle_budgets()),
            cpu_pool: Some(self.cpu.clone()),
            ..StorageEngineConfig::default()
        };
        HttpMultiRangeWorkerConfig {
            journal_root,
            storage,
            ingress_budget: self.ingress.clone(),
            protocol_metadata: self.protocol_metadata.clone(),
            server_stats: self.server_stats.clone(),
            sftp_ingress: self.sftp_ingress.clone(),
            discard_budget: self.discard.clone(),
            scheduling: self.scheduling.clone(),
            ..HttpMultiRangeWorkerConfig::default()
        }
    }
}

#[derive(Debug)]
pub enum HttpCapacityError {
    Cpu(ariax_runtime::CpuError),
    Profile(ProfileCapacityError),
    Handles(HandleBudgetError),
    Transport(HttpTransportError),
    NoUsableHandles,
}

impl fmt::Display for HttpCapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cpu(error) => error.fmt(formatter),
            Self::Profile(error) => error.fmt(formatter),
            Self::Handles(error) => error.fmt(formatter),
            Self::Transport(error) => error.fmt(formatter),
            Self::NoUsableHandles => {
                formatter.write_str("native handle capability leaves no usable HTTP capacity")
            }
        }
    }
}

impl Error for HttpCapacityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Cpu(error) => Some(error),
            Self::Profile(error) => Some(error),
            Self::Handles(error) => Some(error),
            Self::Transport(error) => Some(error),
            Self::NoUsableHandles => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::HttpProcessResources;
    use crate::HttpDiscardScopeLimits;
    use ariax_core::TaskId;
    use ariax_runtime::{
        C10K_LOW_ACTIVITY_SOCKET_TARGET, HTTP_IDLE_CONNECTION_RESERVATION_BYTES,
        PROFILE_CONTROL_HANDLE_RESERVE, ProfileCapacityError, RuntimeProfile,
    };
    use std::path::PathBuf;

    #[test]
    fn concurrency_profile_resolves_exact_http_process_defaults() {
        let resources = HttpProcessResources::with_native_handle_limit(
            RuntimeProfile::Concurrency,
            Some(16_384 + PROFILE_CONTROL_HANDLE_RESERVE),
        )
        .expect("resources");
        resources.require_c10k().expect("C10k capacity");
        let client = resources.policy_client_config();
        assert_eq!(client.direct.max_connections_per_origin, 2);
        assert_eq!(client.direct.max_idle_connections_per_origin, 2);
        assert_eq!(client.direct_transport_cache_capacity, 256);
        assert_eq!(client.direct.idle_timeout.as_secs(), 60);
        assert_eq!(client.direct.budgets.socket_limit(), 12_288);
        assert_eq!(
            client.direct.budgets.connection_reservation_bytes(),
            HTTP_IDLE_CONNECTION_RESERVATION_BYTES
        );
        assert_eq!(client.proxy_request.connect.budgets.socket_limit(), 12_288);

        let worker = resources.worker_config(PathBuf::from("/tmp/ariax-profile-journals"));
        assert_eq!(worker.ingress_budget.limit(), 64 * 1024 * 1024);
        assert_eq!(worker.storage.disk_queue_capacity, 128);
        assert_eq!(worker.storage.disk_completion_capacity, 128);
        assert_eq!(worker.storage.max_in_flight_bytes, 64 * 1024 * 1024);
        assert_eq!(worker.storage.buffer_pool_bytes, 256 * 1024 * 1024);
        assert!(worker.storage.handle_budgets.is_some());
        let limits = worker.discard_budget.configured_limits();
        let guard = worker
            .discard_budget
            .begin_task(
                TaskId::new(1).expect("task"),
                HttpDiscardScopeLimits {
                    host_bytes: limits.host_bytes,
                    task_bytes: limits.task_bytes,
                    attempt_bytes: limits.attempt_bytes,
                },
            )
            .expect("discard task");
        let attempt = guard
            .begin_attempt("https://capacity.test")
            .expect("attempt");
        assert_eq!(attempt.charge(1).exhausted, None);
        assert_eq!(
            resources
                .discard_budget()
                .process_snapshot()
                .process_consumed,
            1
        );
    }

    #[test]
    fn c10k_and_one_thousand_active_ingress_reservations_share_resident_limit() {
        let resources = HttpProcessResources::with_native_handle_limit(
            RuntimeProfile::Concurrency,
            Some(16_384 + PROFILE_CONTROL_HANDLE_RESERVE),
        )
        .expect("resources");
        let transport = resources.transport_budgets();
        let ingress = resources.ingress_budgets();
        let mut sockets = Vec::with_capacity(C10K_LOW_ACTIVITY_SOCKET_TARGET);
        for _ in 0..C10K_LOW_ACTIVITY_SOCKET_TARGET {
            sockets.push(transport.try_acquire_connection().expect("socket capacity"));
        }
        assert_eq!(
            transport.connection_memory_used(),
            C10K_LOW_ACTIVITY_SOCKET_TARGET * HTTP_IDLE_CONNECTION_RESERVATION_BYTES
        );
        let mut active = Vec::with_capacity(1_000);
        for _ in 0..1_000 {
            active.push(ingress.try_acquire(64 * 1024).expect("ingress capacity"));
        }
        assert_eq!(ingress.used(), 1_000 * 64 * 1024);
        assert_eq!(
            resources.resident_budget().used(),
            C10K_LOW_ACTIVITY_SOCKET_TARGET * HTTP_IDLE_CONNECTION_RESERVATION_BYTES
                + 1_000 * 64 * 1024
        );
        assert!(
            resources.resident_budget().used()
                < resources.profile().limits().accounted_resident_limit_bytes
        );
        drop((active, sockets));
        assert_eq!(resources.resident_budget().used(), 0);
    }

    #[test]
    fn low_native_limit_scales_transport_and_reports_c10k_rejection() {
        let native = 8_192 + PROFILE_CONTROL_HANDLE_RESERVE;
        let resources = HttpProcessResources::with_native_handle_limit(
            RuntimeProfile::Concurrency,
            Some(native),
        )
        .expect("scaled resources");
        assert_eq!(resources.transport_budgets().socket_limit(), 8_192);
        assert!(matches!(
            resources.require_c10k(),
            Err(super::HttpCapacityError::Profile(
                ProfileCapacityError::InsufficientC10kCapacity {
                    available: 8_192,
                    native_handle_limit: Some(limit),
                    ..
                }
            )) if limit == native
        ));
    }
}
