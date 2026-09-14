//! Nonblocking persistence barriers before an exact, prepared scheduler input.

use super::*;
use ariax_storage::{SessionCompletion, SessionOwnerError};
use std::task::Poll;

pub(super) enum PreparedInput {
    Command(SchedulerCommand, MonotonicInstant),
    Event(TaskEventEnvelope, MonotonicInstant),
    Admit(MonotonicInstant),
}

#[derive(Clone, Copy)]
enum Expected {
    Unit,
    Appended(Gid),
    Flushed(u64),
}

struct Write {
    command: SessionCommand,
    expected: Expected,
}

#[derive(Default)]
pub(super) struct SessionWrites {
    pending: VecDeque<Write>,
    completion: Option<(SessionCompletion, Expected)>,
}

impl SessionWrites {
    pub(super) fn unit(&mut self, command: SessionCommand) {
        self.pending.push_back(Write {
            command,
            expected: Expected::Unit,
        });
    }

    pub(super) fn journal(&mut self, gid: Gid, generation: Generation, payload: JournalPayload) {
        self.pending.push_back(Write {
            command: SessionCommand::AppendJournal {
                gid,
                generation,
                payload,
            },
            expected: Expected::Appended(gid),
        });
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.completion.is_none()
    }

    /// Consumes at most one completion or submits one owned command per poll.
    pub(super) fn poll(
        &mut self,
        session: &SessionHandle,
        turn: &mut OwnerTurn,
    ) -> Poll<Result<(), HttpControlError>> {
        if let Some((completion, expected)) = self.completion.take() {
            let result = match completion.try_wait() {
                Ok(None) => {
                    self.completion = Some((completion, expected));
                    return Poll::Pending;
                }
                Ok(Some(result)) => {
                    turn.mark_progress();
                    result
                }
                Err(error) => {
                    return Poll::Ready(Err(HttpControlError::Persistence(error.to_string())));
                }
            };
            match (expected, result) {
                (Expected::Unit, SessionCommandResult::Unit) => {}
                (Expected::Appended(gid), SessionCommandResult::JournalAppended(appended)) => {
                    let sequence = appended.sequence();
                    self.pending.push_front(Write {
                        command: SessionCommand::FlushJournal {
                            gid,
                            through_sequence: sequence,
                        },
                        expected: Expected::Flushed(sequence),
                    });
                }
                (Expected::Flushed(sequence), SessionCommandResult::JournalFlushed(flushed))
                    if flushed.through_sequence() >= sequence => {}
                _ => {
                    return Poll::Ready(Err(HttpControlError::Persistence(
                        "unexpected control persistence result".to_owned(),
                    )));
                }
            }
        } else if let Some(write) = self.pending.pop_front() {
            match session.try_submit_owned(write.command) {
                Ok(completion) => {
                    self.completion = Some((completion, write.expected));
                    turn.mark_progress();
                }
                Err(rejection) => {
                    let (command, error) = rejection.into_boxed_parts();
                    if matches!(error, SessionOwnerError::QueueFull) {
                        self.pending.push_front(Write {
                            command: *command,
                            expected: write.expected,
                        });
                    } else {
                        return Poll::Ready(Err(HttpControlError::Persistence(error.to_string())));
                    }
                }
            }
        }
        if self.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

pub(super) struct PendingInput {
    input: PreparedInput,
    writes: SessionWrites,
}

impl HttpControlPlane {
    pub(super) fn engine_idle(&self) -> bool {
        self.engine.is_idle() && self.pending_input.is_none()
    }

    pub(super) fn begin_prepared_input(
        &mut self,
        input: PreparedInput,
        writes: SessionWrites,
    ) -> Result<(), HttpControlError> {
        if !self.engine_idle() {
            return Err(HttpControlError::Busy);
        }
        if writes.is_empty() {
            self.apply_prepared_input(input)
        } else {
            self.pending_input = Some(PendingInput { input, writes });
            Ok(())
        }
    }

    fn apply_prepared_input(&mut self, input: PreparedInput) -> Result<(), HttpControlError> {
        let result = match input {
            PreparedInput::Command(command, at) => self.engine.execute_command_at(command, at),
            PreparedInput::Event(event, at) => self.engine.handle_event_at(&event, at),
            PreparedInput::Admit(at) => self.engine.admit_next_at(at),
        };
        result.map_err(|error| HttpControlError::Scheduler(format!("{error:?}")))
    }

    pub(super) fn poll_prepared_input(&mut self) -> Result<(), HttpControlError> {
        let Some(mut pending) = self.pending_input.take() else {
            return Ok(());
        };
        match pending.writes.poll(&self.session, &mut self.turn) {
            Poll::Pending => self.pending_input = Some(pending),
            Poll::Ready(result) => {
                if let Err(error) = result.and_then(|()| self.apply_prepared_input(pending.input)) {
                    // Once a prelude is admitted its durable outcome cannot be
                    // rolled back or reported as ordinary backpressure.
                    self.engine.fail_control_publication();
                    return Err(error);
                }
            }
        }
        Ok(())
    }
}
