//! A single journal writer, either local or retained by the native owner.

use ariax_core::{Generation, Gid};
use ariax_storage::{
    Appended, ControlJournalAppender, Flushed, JournalAppenderError, JournalAppenderFault,
    JournalPayload, SessionCommand, SessionCommandResult, SessionHandle, SessionOwnerError,
    SessionPersistenceError,
};

pub(crate) trait JournalWrite {
    fn append_payload(
        &mut self,
        generation: Generation,
        payload: &JournalPayload,
    ) -> Result<Appended, JournalAppenderError>;
    fn flush(&mut self, sequence: u64) -> Result<Flushed, JournalAppenderError>;
}

impl JournalWrite for ControlJournalAppender {
    fn append_payload(
        &mut self,
        generation: Generation,
        payload: &JournalPayload,
    ) -> Result<Appended, JournalAppenderError> {
        Self::append_payload(self, generation, payload)
    }
    fn flush(&mut self, sequence: u64) -> Result<Flushed, JournalAppenderError> {
        Self::flush(self, sequence)
    }
}

pub(crate) enum StorageJournal {
    Local(Box<ControlJournalAppender>),
    Managed {
        session: SessionHandle,
        gid: Gid,
        sequence: u64,
    },
}

impl From<ControlJournalAppender> for StorageJournal {
    fn from(value: ControlJournalAppender) -> Self {
        Self::Local(Box::new(value))
    }
}

impl StorageJournal {
    pub(crate) fn last_sequence(&self) -> u64 {
        match self {
            Self::Local(journal) => journal.appended_sequence(),
            Self::Managed { sequence, .. } => *sequence,
        }
    }
    pub(crate) fn attached(session: SessionHandle, gid: Gid, sequence: u64) -> Self {
        Self::Managed {
            session,
            gid,
            sequence,
        }
    }
    pub(crate) fn manage(
        self,
        session: Option<&SessionHandle>,
        gid: Gid,
    ) -> Result<Self, JournalAppenderError> {
        let (session, journal) = match (session, self) {
            (Some(session), Self::Local(journal)) => (session, journal),
            (_, journal) => return Ok(journal),
        };
        let sequence = journal.appended_sequence();
        let result = session
            .execute(SessionCommand::InstallJournalAppender {
                gid,
                appender: *journal,
            })
            .map_err(owner_error)?;
        if result != SessionCommandResult::Unit {
            return Err(unavailable());
        }
        Ok(Self::Managed {
            session: session.clone(),
            gid,
            sequence,
        })
    }

    pub(crate) fn close_flushed(&mut self) -> Result<(), JournalAppenderError> {
        match self {
            Self::Local(journal) => journal.close_flushed(),
            Self::Managed { session, gid, .. } => session
                .execute(SessionCommand::FlushJournalHead { gid: *gid })
                .map_err(owner_error)
                .and_then(|result| {
                    if matches!(result, SessionCommandResult::JournalFlushed(_)) {
                        Ok(())
                    } else {
                        Err(unavailable())
                    }
                }),
        }
    }

    pub(crate) fn into_local(self) -> Result<ControlJournalAppender, JournalAppenderError> {
        match self {
            Self::Local(journal) => Ok(*journal),
            Self::Managed { .. } => Err(unavailable()),
        }
    }
}

impl JournalWrite for StorageJournal {
    fn append_payload(
        &mut self,
        generation: Generation,
        payload: &JournalPayload,
    ) -> Result<Appended, JournalAppenderError> {
        match self {
            Self::Local(journal) => journal.append_payload(generation, payload),
            Self::Managed {
                session,
                gid,
                sequence,
            } => {
                match session
                    .execute(SessionCommand::AppendJournal {
                        gid: *gid,
                        generation,
                        payload: payload.clone(),
                    })
                    .map_err(owner_error)?
                {
                    SessionCommandResult::JournalAppended(appended) => {
                        *sequence = appended.sequence();
                        Ok(appended)
                    }
                    _ => Err(unavailable()),
                }
            }
        }
    }
    fn flush(&mut self, sequence: u64) -> Result<Flushed, JournalAppenderError> {
        match self {
            Self::Local(journal) => journal.flush(sequence),
            Self::Managed { session, gid, .. } => {
                match session
                    .execute(SessionCommand::FlushJournal {
                        gid: *gid,
                        through_sequence: sequence,
                    })
                    .map_err(owner_error)?
                {
                    SessionCommandResult::JournalFlushed(flushed) => Ok(flushed),
                    _ => Err(unavailable()),
                }
            }
        }
    }
}

fn unavailable() -> JournalAppenderError {
    JournalAppenderError::Faulted(JournalAppenderFault::WriteRecord)
}

fn owner_error(error: SessionOwnerError) -> JournalAppenderError {
    match error {
        SessionOwnerError::Persistence(SessionPersistenceError::Journal { error, .. }) => error,
        _ => unavailable(),
    }
}
