use super::*;
use crate::slow_slots::{SLOW_SAMPLE_INTERVAL, SlowObservation};
use ariax_core::{SlowReadmissionDecision, TaskState};
use ariax_runtime::ConnectionCondition;

impl HttpControlPlane {
    pub(super) fn poll_slow_slots_at(
        &mut self,
        now: MonotonicInstant,
    ) -> Result<(), HttpControlError> {
        let policy = self.scheduling.config();
        if policy.policy == crate::SlowSlotPolicy::Off
            && policy.retry_wait != crate::RetryWaitSlotPolicy::Auto
        {
            return Ok(());
        }
        if self.next_slow_sample.is_some_and(|next| now < next) {
            return Ok(());
        }
        let late = self
            .next_slow_sample
            .is_some_and(|next| now.duration_since(next) > Duration::from_millis(500));
        self.next_slow_sample = now.checked_add(SLOW_SAMPLE_INTERVAL);
        let root = self.engine.snapshot_reader().load();
        let runnable_waiter = root.queue(QueueClass::Waiting).iter().any(|gid| {
            self.engine.scheduler().task(*gid).is_some_and(|task| {
                task.state == TaskState::Waiting
                    && task.pending_barrier.is_none()
                    && !task.desired_paused
                    && !task.conditions.needs_credentials
                    && !task.conditions.no_space
            })
        });
        let active: Vec<_> = root
            .queue(QueueClass::Active)
            .iter()
            .filter_map(|gid| {
                let task = self.engine.scheduler().task(*gid)?;
                let stats = self.stats.get(task.task_id)?.snapshot_at(now);
                Some((task, stats))
            })
            .collect();
        drop(root);
        self.scheduling.set_all_active_idle(
            !active.is_empty()
                && active.iter().all(|(task, stats)| {
                    !stats.local_pressure
                        && !matches!(
                            stats.connection_condition,
                            ConnectionCondition::Backpressured | ConnectionCondition::RateLimited
                        )
                        && (task.state == TaskState::RetryWait
                            || stats.retry_wait_until.is_some()
                            || stats.connection_condition == ConnectionCondition::Stalled)
                }),
        );
        if policy.policy == crate::SlowSlotPolicy::Off {
            return Ok(());
        }
        let current: std::collections::BTreeSet<_> =
            active.iter().map(|(task, _)| task.gid).collect();
        Arc::make_mut(&mut self.slow_observations).retain(|gid, _| current.contains(gid));
        let global_limited = self
            .global_options
            .get("max-overall-download-limit")
            .is_some_and(|limit| limit != "0");
        let mut selected = None;
        for (task, stats) in active {
            let Some(spec) = self.tasks.get_gid(task.gid) else {
                continue;
            };
            let observation = Arc::make_mut(&mut self.slow_observations)
                .entry(task.gid)
                .or_insert(SlowObservation {
                    generation: task.generation,
                    active_since: now,
                    slow_since: None,
                });
            if observation.generation != task.generation {
                *observation = SlowObservation {
                    generation: task.generation,
                    active_since: now,
                    slow_since: None,
                };
            }
            let threshold = if policy.speed_limit != 0 {
                policy.speed_limit
            } else if spec.options().lowest_speed_limit != 0 {
                spec.options().lowest_speed_limit
            } else {
                64 * 1024
            };
            let eligible = runnable_waiter
                && !late
                && !global_limited
                && spec.options().max_download_limit == 0
                && task.state == TaskState::Active
                && !task.desired_paused
                && task.pending_barrier.is_none()
                && task.slow_demotion_count < policy.max_demotions;
            if observe(observation, policy, stats, threshold, eligible, now) {
                selected = Some(task);
                break;
            }
        }
        let Some(task) = selected else {
            return Ok(());
        };
        let event = match policy.policy {
            crate::SlowSlotPolicy::Off => return Ok(()),
            crate::SlowSlotPolicy::Demote => TaskEvent::SlowDemoted {
                gid: task.gid,
                generation: task.generation,
                decision: SlowReadmissionDecision {
                    readmit_at: now
                        .checked_add(policy.readmit_after)
                        .ok_or(HttpControlError::InvalidConfig)?,
                    scheduled_at_ms: now_unix_ms(),
                    delay_ms: u64::try_from(policy.readmit_after.as_millis())
                        .map_err(|_| HttpControlError::InvalidConfig)?,
                },
            },
            crate::SlowSlotPolicy::Pause => TaskEvent::SlowPaused {
                gid: task.gid,
                generation: task.generation,
            },
        }
        .for_task(task.task_id);
        self.prepare_and_begin_event(event, now)
    }
}

fn observe(
    observation: &mut SlowObservation,
    policy: crate::SlowSlotConfig,
    stats: HttpTransferStatsSnapshot,
    threshold: u64,
    eligible: bool,
    now: MonotonicInstant,
) -> bool {
    if !eligible
        || !stats.network_phase
        || (stats.active_connections == 0 && stats.retry_wait_until.is_none())
        || stats.local_pressure
        || matches!(
            stats.connection_condition,
            ConnectionCondition::Backpressured | ConnectionCondition::RateLimited
        )
        || stats.useful_speed >= threshold
    {
        observation.slow_since = None;
        return false;
    }
    let since = *observation.slow_since.get_or_insert(now);
    now.duration_since(observation.active_since) >= policy.min_active_time
        && now.duration_since(since) >= policy.grace_period
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slow_classifier_excludes_local_pressure_and_honors_all_thresholds() {
        let start = MonotonicInstant::now();
        let policy = crate::SlowSlotConfig {
            policy: crate::SlowSlotPolicy::Demote,
            grace_period: Duration::from_secs(1),
            min_active_time: Duration::from_secs(2),
            ..crate::SlowSlotConfig::default()
        };
        let stats = HttpTransferStatsSnapshot {
            network_phase: true,
            active_connections: 1,
            useful_speed: 10,
            ..HttpTransferStatsSnapshot::default()
        };
        let mut observation = SlowObservation {
            generation: Generation::INITIAL,
            active_since: start,
            slow_since: None,
        };
        assert!(!observe(&mut observation, policy, stats, 100, true, start));
        assert!(!observe(
            &mut observation,
            policy,
            stats,
            100,
            true,
            start.checked_add(Duration::from_secs(1)).expect("time")
        ));
        assert!(observe(
            &mut observation,
            policy,
            stats,
            100,
            true,
            start.checked_add(Duration::from_secs(2)).expect("time")
        ));
        for excluded in [
            HttpTransferStatsSnapshot {
                local_pressure: true,
                ..stats
            },
            HttpTransferStatsSnapshot {
                connection_condition: ConnectionCondition::Backpressured,
                ..stats
            },
            HttpTransferStatsSnapshot {
                connection_condition: ConnectionCondition::RateLimited,
                ..stats
            },
            HttpTransferStatsSnapshot {
                network_phase: false,
                ..stats
            },
            HttpTransferStatsSnapshot {
                active_connections: 0,
                ..stats
            },
            HttpTransferStatsSnapshot {
                useful_speed: 100,
                ..stats
            },
        ] {
            assert!(!observe(
                &mut observation,
                policy,
                excluded,
                100,
                true,
                start.checked_add(Duration::from_secs(3)).expect("time")
            ));
            assert!(observation.slow_since.is_none());
        }
        assert!(!observe(
            &mut observation,
            policy,
            stats,
            100,
            false,
            start.checked_add(Duration::from_secs(3)).expect("time")
        ));
        assert!(!observe(
            &mut observation,
            policy,
            stats,
            100,
            true,
            start.checked_add(Duration::from_secs(3)).expect("time")
        ));
        assert!(observe(
            &mut observation,
            policy,
            stats,
            100,
            true,
            start.checked_add(Duration::from_secs(4)).expect("time")
        ));
    }
}
