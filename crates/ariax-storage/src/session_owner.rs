use crate::{
    Appended, ControlJournalAppender, Flushed, JournalAppenderError, JournalId,
    JournalInstallIntent, JournalInstallToken, JournalPayload, OptionsSnapshotScope,
    PersistedOptionPolicy, PlatformPath, PreparedJournalSet, SanitizedOptionMap,
    SessionHostKeyChallengeRecord, SessionHostKeyResolution, SessionJournalCache,
    SessionNoSpaceCondition, SessionQueueState, SessionQueueTransition, SessionRecord,
    SessionStoppedResultRecord, SessionStore, SessionStoreConfig, SessionStoreError,
    SessionStoreSettings, SessionTaskMetadata, SessionTaskRecord, SessionTaskSourceRecord,
    SessionTaskSourceSet,
};
use ariax_core::{Generation, Gid, HostKeyChallengeId};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::mpsc::{
    Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError, sync_channel,
};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::thread::{self, JoinHandle, Thread};
use std::time::{Duration, Instant};

/// Default number of session commands that may wait behind the current command.
pub const SESSION_OWNER_DEFAULT_CAPACITY: usize = 64;
/// Hard maximum for queued session commands, including owned persistence payloads.
pub const SESSION_OWNER_MAX_CAPACITY: usize = 64;
pub const SESSION_OWNER_DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
pub const SESSION_OWNER_DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
pub const SESSION_OWNER_MAX_WAIT: Duration = Duration::from_secs(300);
const SESSION_OWNER_IDLE_POLL: Duration = Duration::from_millis(10);
const SESSION_OWNER_JOIN_POLL: Duration = Duration::from_millis(10);

/// Fixed startup and bounded-admission policy for the dedicated session thread.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionOwnerConfig {
    pub database_path: PathBuf,
    pub store: SessionStoreConfig,
    pub request_capacity: NonZeroUsize,
    pub startup_timeout: Duration,
    pub shutdown_timeout: Duration,
}

impl SessionOwnerConfig {
    #[must_use]
    pub fn new(database_path: PathBuf) -> Self {
        Self {
            database_path,
            store: SessionStoreConfig::default(),
            request_capacity: NonZeroUsize::new(SESSION_OWNER_DEFAULT_CAPACITY)
                .expect("the default session-owner capacity is nonzero"),
            startup_timeout: SESSION_OWNER_DEFAULT_STARTUP_TIMEOUT,
            shutdown_timeout: SESSION_OWNER_DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

/// Bounded semantic state read before the owner publishes a usable handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionStartupSnapshot {
    pub settings: SessionStoreSettings,
    pub session: Option<SessionRecord>,
    pub tasks: Vec<SessionTaskRecord>,
    pub task_sources: Vec<SessionTaskSourceSet>,
    pub stopped_results: Vec<SessionStoppedResultRecord>,
    pub host_key_challenges: Vec<SessionHostKeyChallengeRecord>,
    pub journal_installs: Vec<JournalInstallIntent>,
}

/// Owned commands accepted by the bounded session-owner queue.
#[derive(Debug)]
pub enum SessionCommand {
    PutSession(SessionRecord),
    PutTask(SessionTaskRecord),
    CreateTaskWithMetadata {
        task: SessionTaskRecord,
        sources: Vec<SessionTaskSourceRecord>,
        options: SanitizedOptionMap,
    },
    CreateTaskBatch(Arc<[SessionTaskMetadata]>),
    CreateFollowedMetalink {
        tasks: Arc<[SessionTaskMetadata]>,
        parent: crate::MetalinkParent,
    },
    ConfirmTaskMetadata(Arc<SessionTaskMetadata>),
    ReadTasks,
    ReadStoppedResults,
    TransitionTaskQueue(SessionQueueTransition),
    SetNoSpaceCondition {
        gid: Gid,
        condition: Option<SessionNoSpaceCondition>,
        updated_ms: u64,
    },
    ReconcileJournalAuthority {
        gid: Gid,
        expected_journal_id: JournalId,
        cache: Option<SessionJournalCache>,
        root_display: Option<PlatformPath>,
        updated_ms: u64,
    },
    ReplaceTaskSources {
        gid: Gid,
        sources: Vec<SessionTaskSourceRecord>,
    },
    ReplaceTaskSourcesAndQueue {
        transition: SessionQueueTransition,
        sources: Vec<SessionTaskSourceRecord>,
    },
    ReadTaskSources {
        gid: Gid,
    },
    ReplaceTaskOptions {
        gid: Gid,
        scope: OptionsSnapshotScope,
        options: SanitizedOptionMap,
    },
    PromoteTaskOptions {
        gid: Gid,
        options: SanitizedOptionMap,
    },
    ReadTaskOptions {
        gid: Gid,
        scope: OptionsSnapshotScope,
    },
    PutHostKeyChallenge(SessionHostKeyChallengeRecord),
    ReadHostKeyChallenge {
        gid: Gid,
    },
    ReadHostKeyChallenges,
    RejectHostKeyChallenge {
        gid: Gid,
        challenge_id: HostKeyChallengeId,
    },
    ResolveHostKeyChallenge(SessionHostKeyResolution),
    PersistStoppedResult {
        result: SessionStoppedResultRecord,
        transition: SessionQueueTransition,
    },
    DeleteStoppedTaskMetadata {
        gid: Gid,
        remaining_order: Vec<Gid>,
        updated_ms: u64,
    },
    ReadQueueOrder {
        state: SessionQueueState,
    },
    AbortJournalInstall {
        token: JournalInstallToken,
    },
    CompleteJournalInstall {
        token: JournalInstallToken,
        updated_ms: u64,
    },
    ClearInstalledJournal {
        token: JournalInstallToken,
    },
    InstallJournalAppender {
        gid: Gid,
        appender: ControlJournalAppender,
    },
    InstallPreparedJournal {
        gid: Gid,
        prepared: PreparedJournalSet,
        recovery_starting_generation: Generation,
        recovery_created_at_unix_ms: u64,
    },
    AppendJournal {
        gid: Gid,
        generation: Generation,
        payload: JournalPayload,
    },
    FlushJournal {
        gid: Gid,
        through_sequence: u64,
    },
    FlushJournalHead {
        gid: Gid,
    },
    SnapshotJournal {
        gid: Gid,
    },
    FlushAllJournals,
    CloseJournal {
        gid: Gid,
    },
    CloseAllFlushedJournals,
    IntegrityCheck,
    CheckpointWalTruncate,
    #[cfg(test)]
    InstallJournalAppenderWithDropNotice {
        gid: Gid,
        appender: ControlJournalAppender,
        dropped: SyncSender<(thread::ThreadId, thread::ThreadId)>,
    },
    #[cfg(test)]
    HoldForTest {
        entered: SyncSender<()>,
        release: Arc<(Mutex<bool>, std::sync::Condvar)>,
    },
    #[cfg(test)]
    PanicForTest,
}

/// One typed result delivered through the request's reserved completion slot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionCommandResult {
    Unit,
    Tasks(Vec<SessionTaskRecord>),
    StoppedResults(Vec<SessionStoppedResultRecord>),
    TaskSources(Vec<SessionTaskSourceRecord>),
    TaskOptions(SanitizedOptionMap),
    HostKeyChallenge(Option<SessionHostKeyChallengeRecord>),
    HostKeyChallenges(Vec<SessionHostKeyChallengeRecord>),
    QueueOrder(Vec<Gid>),
    JournalAppended(Appended),
    JournalFlushed(Flushed),
    JournalSnapshot(Box<crate::JournalReplay>),
    JournalsFlushed(usize),
    JournalsClosed(usize),
}

/// A persistence command failure with its concrete storage cause preserved.
#[derive(Debug)]
pub enum SessionPersistenceError {
    Store(SessionStoreError),
    Journal {
        gid: Gid,
        error: JournalAppenderError,
    },
    MissingJournal {
        gid: Gid,
    },
    DuplicateJournal {
        gid: Gid,
    },
    JournalGidMismatch {
        expected: Gid,
        actual: Gid,
    },
}

pub const ALL_SESSION_PERSISTENCE_ERROR_CODES: [&str; 5] = [
    "store",
    "journal",
    "missing_journal",
    "duplicate_journal",
    "journal_gid_mismatch",
];

impl SessionPersistenceError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Store(_) => "store",
            Self::Journal { .. } => "journal",
            Self::MissingJournal { .. } => "missing_journal",
            Self::DuplicateJournal { .. } => "duplicate_journal",
            Self::JournalGidMismatch { .. } => "journal_gid_mismatch",
        }
    }
}

impl fmt::Display for SessionPersistenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => error.fmt(formatter),
            Self::Journal { gid, error } => {
                write!(formatter, "journal command for {gid} failed: {error}")
            }
            Self::MissingJournal { gid } => {
                write!(formatter, "no journal appender is installed for {gid}")
            }
            Self::DuplicateJournal { gid } => {
                write!(
                    formatter,
                    "a journal appender is already installed for {gid}"
                )
            }
            Self::JournalGidMismatch { expected, actual } => write!(
                formatter,
                "journal appender task identity {actual} does not match requested GID {expected}"
            ),
        }
    }
}

impl Error for SessionPersistenceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::Journal { error, .. } => Some(error),
            _ => None,
        }
    }
}

impl From<SessionStoreError> for SessionPersistenceError {
    fn from(error: SessionStoreError) -> Self {
        Self::Store(error)
    }
}

/// Which owner lifecycle wait failed validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionOwnerWait {
    Startup,
    Shutdown,
}

impl SessionOwnerWait {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Shutdown => "shutdown",
        }
    }
}

/// Result of a bounded out-of-band owner shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionOwnerShutdown {
    Joined,
    DetachedUncertain { timeout: Duration },
}

#[derive(Debug)]
pub enum SessionOwnerError {
    Persistence(SessionPersistenceError),
    ThreadSpawn(io::ErrorKind),
    InvalidRequestCapacity { requested: usize, maximum: usize },
    InvalidWaitTimeout(SessionOwnerWait),
    StartupTimedOut { timeout: Duration },
    ShutdownTimedOut { timeout: Duration },
    QueueFull,
    ShuttingDown,
    Unavailable,
    OwnerPanicked,
}

pub const ALL_SESSION_OWNER_ERROR_CODES: [&str; 10] = [
    "persistence",
    "thread_spawn",
    "invalid_request_capacity",
    "invalid_wait_timeout",
    "startup_timed_out",
    "shutdown_timed_out",
    "queue_full",
    "shutting_down",
    "unavailable",
    "owner_panicked",
];

impl SessionOwnerError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Persistence(_) => "persistence",
            Self::ThreadSpawn(_) => "thread_spawn",
            Self::InvalidRequestCapacity { .. } => "invalid_request_capacity",
            Self::InvalidWaitTimeout(_) => "invalid_wait_timeout",
            Self::StartupTimedOut { .. } => "startup_timed_out",
            Self::ShutdownTimedOut { .. } => "shutdown_timed_out",
            Self::QueueFull => "queue_full",
            Self::ShuttingDown => "shutting_down",
            Self::Unavailable => "unavailable",
            Self::OwnerPanicked => "owner_panicked",
        }
    }
}

impl fmt::Display for SessionOwnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Persistence(error) => error.fmt(formatter),
            Self::ThreadSpawn(kind) => {
                write!(formatter, "session owner thread spawn failed: {kind}")
            }
            Self::InvalidRequestCapacity { requested, maximum } => write!(
                formatter,
                "session owner request capacity {requested} exceeds the hard maximum {maximum}"
            ),
            Self::InvalidWaitTimeout(wait) => write!(
                formatter,
                "session owner {} timeout must be nonzero and at most {} seconds",
                wait.code(),
                SESSION_OWNER_MAX_WAIT.as_secs()
            ),
            Self::StartupTimedOut { timeout } => write!(
                formatter,
                "session owner startup timed out after {} ms; owner detached with persistence state uncertain",
                timeout.as_millis()
            ),
            Self::ShutdownTimedOut { timeout } => write!(
                formatter,
                "session owner shutdown timed out after {} ms; owner detached with persistence state uncertain",
                timeout.as_millis()
            ),
            Self::QueueFull => formatter.write_str("session owner request queue is full"),
            Self::ShuttingDown => formatter.write_str("session owner is shutting down"),
            Self::Unavailable => formatter.write_str("session owner is unavailable"),
            Self::OwnerPanicked => formatter.write_str("session owner thread panicked"),
        }
    }
}

impl Error for SessionOwnerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Persistence(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SessionPersistenceError> for SessionOwnerError {
    fn from(error: SessionPersistenceError) -> Self {
        Self::Persistence(error)
    }
}

impl From<SessionStoreError> for SessionOwnerError {
    fn from(error: SessionStoreError) -> Self {
        Self::Persistence(SessionPersistenceError::Store(error))
    }
}

/// One accepted command whose completion capacity was reserved at admission.
pub struct SessionCompletion {
    receiver: Receiver<Result<SessionCommandResult, SessionPersistenceError>>,
}

impl SessionCompletion {
    pub fn wait(self) -> Result<SessionCommandResult, SessionOwnerError> {
        self.receiver
            .recv()
            .map_err(|_| SessionOwnerError::Unavailable)?
            .map_err(SessionOwnerError::Persistence)
    }

    pub fn try_wait(&self) -> Result<Option<SessionCommandResult>, SessionOwnerError> {
        match self.receiver.try_recv() {
            Ok(result) => result.map(Some).map_err(SessionOwnerError::Persistence),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(SessionOwnerError::Unavailable),
        }
    }
}

/// One command rejected before admission, retained exactly for a caller retry.
#[derive(Debug)]
pub struct SessionSubmitError {
    command: Box<SessionCommand>,
    error: SessionOwnerError,
}

impl SessionSubmitError {
    fn new(command: SessionCommand, error: SessionOwnerError) -> Self {
        Self {
            command: Box::new(command),
            error,
        }
    }

    #[must_use]
    pub const fn error(&self) -> &SessionOwnerError {
        &self.error
    }

    #[must_use]
    pub const fn command(&self) -> &SessionCommand {
        &self.command
    }

    #[must_use]
    pub fn into_parts(self) -> (SessionCommand, SessionOwnerError) {
        (*self.command, self.error)
    }

    #[must_use]
    pub fn into_boxed_parts(self) -> (Box<SessionCommand>, SessionOwnerError) {
        (self.command, self.error)
    }

    #[must_use]
    pub fn into_error(self) -> SessionOwnerError {
        self.error
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OwnerState {
    Running,
    ShuttingDown,
    Closed,
}

struct OwnerRequest {
    command: SessionCommand,
    completion: SyncSender<Result<SessionCommandResult, SessionPersistenceError>>,
}

struct OwnedJournalAppender {
    appender: ControlJournalAppender,
    #[cfg(test)]
    drop_notice: Option<(
        thread::ThreadId,
        SyncSender<(thread::ThreadId, thread::ThreadId)>,
    )>,
}

impl OwnedJournalAppender {
    fn new(appender: ControlJournalAppender) -> Self {
        Self {
            appender,
            #[cfg(test)]
            drop_notice: None,
        }
    }

    #[cfg(test)]
    fn with_drop_notice(
        appender: ControlJournalAppender,
        drop_notice: SyncSender<(thread::ThreadId, thread::ThreadId)>,
    ) -> Self {
        Self {
            appender,
            drop_notice: Some((thread::current().id(), drop_notice)),
        }
    }
}

impl Drop for OwnedJournalAppender {
    fn drop(&mut self) {
        #[cfg(test)]
        if let Some((owner_thread, drop_notice)) = self.drop_notice.take() {
            let _ = drop_notice.send((owner_thread, thread::current().id()));
        }
    }
}

struct Admission {
    state: OwnerState,
    sender: Option<SyncSender<OwnerRequest>>,
}

struct SessionShared {
    admission: Mutex<Admission>,
    owner_thread: Mutex<Option<Thread>>,
    join: Mutex<Option<JoinHandle<()>>>,
    shutdown_timeout: Duration,
}

impl Drop for SessionShared {
    fn drop(&mut self) {
        if let Some(owner_thread) = lock_unpoisoned(&self.owner_thread).as_ref() {
            owner_thread.unpark();
        }
    }
}

/// Cloneable bounded command handle. It never exposes the synchronous store.
#[derive(Clone)]
pub struct SessionHandle {
    shared: Arc<SessionShared>,
}

impl SessionHandle {
    pub fn try_submit(
        &self,
        command: SessionCommand,
    ) -> Result<SessionCompletion, SessionOwnerError> {
        self.try_submit_owned(command)
            .map_err(SessionSubmitError::into_error)
    }

    /// Attempts bounded admission while preserving an unaccepted owned command.
    pub fn try_submit_owned(
        &self,
        command: SessionCommand,
    ) -> Result<SessionCompletion, SessionSubmitError> {
        let (completion, receiver) = sync_channel(1);
        let mut admission = lock_unpoisoned(&self.shared.admission);
        let sender = match admission.state {
            OwnerState::Running => match admission.sender.as_ref() {
                Some(sender) => sender.clone(),
                None => {
                    return Err(SessionSubmitError::new(
                        command,
                        SessionOwnerError::Unavailable,
                    ));
                }
            },
            OwnerState::ShuttingDown => {
                return Err(SessionSubmitError::new(
                    command,
                    SessionOwnerError::ShuttingDown,
                ));
            }
            OwnerState::Closed => {
                return Err(SessionSubmitError::new(
                    command,
                    SessionOwnerError::Unavailable,
                ));
            }
        };
        let request = OwnerRequest {
            command,
            completion,
        };
        match sender.try_send(request) {
            Ok(()) => {}
            Err(TrySendError::Full(request)) => {
                return Err(SessionSubmitError::new(
                    request.command,
                    SessionOwnerError::QueueFull,
                ));
            }
            Err(TrySendError::Disconnected(request)) => {
                admission.state = OwnerState::Closed;
                admission.sender = None;
                return Err(SessionSubmitError::new(
                    request.command,
                    SessionOwnerError::Unavailable,
                ));
            }
        }
        drop(admission);
        if let Some(owner_thread) = lock_unpoisoned(&self.shared.owner_thread).as_ref() {
            owner_thread.unpark();
        }
        Ok(SessionCompletion { receiver })
    }

    pub fn execute(
        &self,
        command: SessionCommand,
    ) -> Result<SessionCommandResult, SessionOwnerError> {
        self.try_submit(command)?.wait()
    }

    /// Closes admission out of band and applies the configured bounded join wait.
    pub fn shutdown(&self) -> Result<(), SessionOwnerError> {
        match self.shutdown_with_timeout(self.shared.shutdown_timeout)? {
            SessionOwnerShutdown::Joined => Ok(()),
            SessionOwnerShutdown::DetachedUncertain { timeout } => {
                Err(SessionOwnerError::ShutdownTimedOut { timeout })
            }
        }
    }

    /// Closes admission, drains accepted commands, and waits only to the deadline.
    pub fn shutdown_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<SessionOwnerShutdown, SessionOwnerError> {
        validate_wait_timeout(SessionOwnerWait::Shutdown, timeout)?;
        begin_shutdown(&self.shared);
        let handle = {
            let mut join = lock_unpoisoned(&self.shared.join);
            join.take()
        };
        let Some(handle) = handle else {
            return if lock_unpoisoned(&self.shared.admission).state == OwnerState::Closed {
                Ok(SessionOwnerShutdown::Joined)
            } else {
                Err(SessionOwnerError::ShuttingDown)
            };
        };
        let deadline =
            Instant::now()
                .checked_add(timeout)
                .ok_or(SessionOwnerError::InvalidWaitTimeout(
                    SessionOwnerWait::Shutdown,
                ))?;
        while !handle.is_finished() {
            let now = Instant::now();
            if now >= deadline {
                drop(handle);
                return Ok(SessionOwnerShutdown::DetachedUncertain { timeout });
            }
            thread::park_timeout((deadline - now).min(SESSION_OWNER_JOIN_POLL));
        }
        if handle.join().is_err() {
            mark_closed(&self.shared);
            return Err(SessionOwnerError::OwnerPanicked);
        }
        mark_closed(&self.shared);
        Ok(SessionOwnerShutdown::Joined)
    }
}

/// Namespace for creating the dedicated synchronous-store owner.
pub struct SessionOwner;

impl SessionOwner {
    pub fn spawn<P>(
        config: SessionOwnerConfig,
        policy: P,
    ) -> Result<(SessionHandle, SessionStartupSnapshot), SessionOwnerError>
    where
        P: PersistedOptionPolicy + Send + 'static,
    {
        if config.request_capacity.get() > SESSION_OWNER_MAX_CAPACITY {
            return Err(SessionOwnerError::InvalidRequestCapacity {
                requested: config.request_capacity.get(),
                maximum: SESSION_OWNER_MAX_CAPACITY,
            });
        }
        validate_wait_timeout(SessionOwnerWait::Startup, config.startup_timeout)?;
        validate_wait_timeout(SessionOwnerWait::Shutdown, config.shutdown_timeout)?;
        let startup_timeout = config.startup_timeout;
        let shutdown_timeout = config.shutdown_timeout;
        let (sender, receiver) = sync_channel(config.request_capacity.get());
        let shared = Arc::new(SessionShared {
            admission: Mutex::new(Admission {
                state: OwnerState::Running,
                sender: Some(sender),
            }),
            owner_thread: Mutex::new(None),
            join: Mutex::new(None),
            shutdown_timeout,
        });
        let weak = Arc::downgrade(&shared);
        let (startup_sender, startup_receiver) = sync_channel(1);
        let thread = thread::Builder::new()
            .name("ariax-session-owner".to_owned())
            .spawn(move || owner_main(weak, receiver, config, policy, startup_sender))
            .map_err(|error| SessionOwnerError::ThreadSpawn(error.kind()))?;
        *lock_unpoisoned(&shared.owner_thread) = Some(thread.thread().clone());
        *lock_unpoisoned(&shared.join) = Some(thread);
        let handle = SessionHandle { shared };
        match startup_receiver.recv_timeout(startup_timeout) {
            Ok(Ok(snapshot)) => Ok((handle, snapshot)),
            Ok(Err(error)) => {
                let _ = handle.shutdown();
                Err(SessionOwnerError::Persistence(
                    SessionPersistenceError::Store(error),
                ))
            }
            Err(RecvTimeoutError::Disconnected) => match handle.shutdown() {
                Err(SessionOwnerError::OwnerPanicked) => Err(SessionOwnerError::OwnerPanicked),
                _ => Err(SessionOwnerError::Unavailable),
            },
            Err(RecvTimeoutError::Timeout) => {
                begin_shutdown(&handle.shared);
                drop(lock_unpoisoned(&handle.shared.join).take());
                Err(SessionOwnerError::StartupTimedOut {
                    timeout: startup_timeout,
                })
            }
        }
    }
}

fn owner_main<P>(
    shared: Weak<SessionShared>,
    receiver: Receiver<OwnerRequest>,
    config: SessionOwnerConfig,
    policy: P,
    startup: SyncSender<Result<SessionStartupSnapshot, SessionStoreError>>,
) where
    P: PersistedOptionPolicy,
{
    let mut store = match SessionStore::open(config.database_path, config.store) {
        Ok(store) => store,
        Err(error) => {
            let _ = startup.send(Err(error));
            if let Some(shared) = shared.upgrade() {
                mark_closed(&shared);
            }
            return;
        }
    };
    let snapshot = startup_snapshot(&store);
    if snapshot.is_err() {
        let _ = startup.send(snapshot);
        if let Some(shared) = shared.upgrade() {
            mark_closed(&shared);
        }
        return;
    }
    if startup.send(snapshot).is_err() {
        if let Some(shared) = shared.upgrade() {
            mark_closed(&shared);
        }
        return;
    }

    let mut journals = BTreeMap::new();

    loop {
        match receiver.try_recv() {
            Ok(request) => {
                let result = execute_command(&mut store, &mut journals, &policy, request.command);
                let _ = request.completion.send(result);
            }
            Err(TryRecvError::Empty) => {
                let Some(shared) = shared.upgrade() else {
                    break;
                };
                let state = lock_unpoisoned(&shared.admission).state;
                drop(shared);
                if state == OwnerState::Running {
                    thread::park_timeout(SESSION_OWNER_IDLE_POLL);
                } else {
                    break;
                }
            }
            Err(TryRecvError::Disconnected) => break,
        }
    }
    drop(journals);
    drop(store);
    if let Some(shared) = shared.upgrade() {
        mark_closed(&shared);
    }
}

fn startup_snapshot(store: &SessionStore) -> Result<SessionStartupSnapshot, SessionStoreError> {
    Ok(SessionStartupSnapshot {
        settings: store.settings()?,
        session: store.session()?,
        tasks: store.tasks()?,
        task_sources: store.task_source_sets()?,
        stopped_results: store.stopped_results()?,
        host_key_challenges: store.host_key_challenges()?,
        journal_installs: store.journal_installs()?,
    })
}

fn execute_command(
    store: &mut SessionStore,
    journals: &mut BTreeMap<Gid, OwnedJournalAppender>,
    policy: &impl PersistedOptionPolicy,
    command: SessionCommand,
) -> Result<SessionCommandResult, SessionPersistenceError> {
    match command {
        SessionCommand::PutSession(record) => {
            store.put_session(&record)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::PutTask(record) => {
            store.put_task(&record)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::CreateTaskWithMetadata {
            task,
            sources,
            options,
        } => {
            store.create_task_with_metadata(&task, &sources, &options, policy)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::ReadTasks => store
            .tasks()
            .map(SessionCommandResult::Tasks)
            .map_err(SessionPersistenceError::Store),
        SessionCommand::CreateFollowedMetalink { tasks, parent } => {
            store.create_task_batch_following(&tasks, Some(parent), policy)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::CreateTaskBatch(tasks) => {
            store.create_task_batch(&tasks, policy)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::ConfirmTaskMetadata(task) => {
            store.confirm_task_metadata(&task, policy)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::ReadStoppedResults => store
            .stopped_results()
            .map(SessionCommandResult::StoppedResults)
            .map_err(SessionPersistenceError::Store),
        SessionCommand::TransitionTaskQueue(transition) => {
            store.transition_task_queue_exact(&transition)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::SetNoSpaceCondition {
            gid,
            condition,
            updated_ms,
        } => {
            store.set_task_no_space_condition(gid, condition.as_ref(), updated_ms)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::ReconcileJournalAuthority {
            gid,
            expected_journal_id,
            cache,
            root_display,
            updated_ms,
        } => {
            store.reconcile_journal_authority(
                gid,
                expected_journal_id,
                cache,
                root_display.as_ref(),
                updated_ms,
            )?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::ReplaceTaskSources { gid, sources } => {
            store.replace_task_sources(gid, &sources)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::ReplaceTaskSourcesAndQueue {
            transition,
            sources,
        } => {
            store.replace_task_sources_and_queue(&transition, &sources)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::ReadTaskSources { gid } => store
            .task_sources(gid)
            .map(SessionCommandResult::TaskSources)
            .map_err(SessionPersistenceError::Store),
        SessionCommand::ReplaceTaskOptions {
            gid,
            scope,
            options,
        } => {
            store.replace_task_options(gid, scope, &options, policy)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::ReadTaskOptions { gid, scope } => store
            .task_options(gid, scope, policy)
            .map(SessionCommandResult::TaskOptions)
            .map_err(SessionPersistenceError::Store),
        SessionCommand::PromoteTaskOptions { gid, options } => {
            store.promote_task_options(gid, &options, policy)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::PutHostKeyChallenge(challenge) => {
            store.put_host_key_challenge(&challenge)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::ReadHostKeyChallenge { gid } => store
            .host_key_challenge(gid)
            .map(SessionCommandResult::HostKeyChallenge)
            .map_err(SessionPersistenceError::Store),
        SessionCommand::ReadHostKeyChallenges => store
            .host_key_challenges()
            .map(SessionCommandResult::HostKeyChallenges)
            .map_err(SessionPersistenceError::Store),
        SessionCommand::RejectHostKeyChallenge { gid, challenge_id } => {
            store.reject_host_key_challenge(gid, challenge_id)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::ResolveHostKeyChallenge(resolution) => {
            store.resolve_host_key_challenge(&resolution, policy)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::PersistStoppedResult { result, transition } => {
            store.persist_stopped_result_exact(&result, &transition)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::DeleteStoppedTaskMetadata {
            gid,
            remaining_order,
            updated_ms,
        } => {
            store.delete_stopped_task_metadata(gid, &remaining_order, updated_ms)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::ReadQueueOrder { state } => store
            .queue_order(state)
            .map(SessionCommandResult::QueueOrder)
            .map_err(SessionPersistenceError::Store),
        SessionCommand::AbortJournalInstall { token } => {
            store.abort_journal_install(token)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::CompleteJournalInstall { token, updated_ms } => {
            store.complete_journal_install(token, updated_ms)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::ClearInstalledJournal { token } => {
            store.clear_installed_journal(token)?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::InstallJournalAppender { gid, appender } => {
            install_journal_appender(journals, gid, OwnedJournalAppender::new(appender))?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::InstallPreparedJournal {
            gid,
            prepared,
            recovery_starting_generation,
            recovery_created_at_unix_ms,
        } => {
            let actual = prepared.task_gid();
            if actual != gid {
                return Err(SessionPersistenceError::JournalGidMismatch {
                    expected: gid,
                    actual,
                });
            }
            if journals.contains_key(&gid) {
                return Err(SessionPersistenceError::DuplicateJournal { gid });
            }
            let (appender, _) = ControlJournalAppender::open_prepared(
                prepared,
                recovery_starting_generation,
                recovery_created_at_unix_ms,
            )
            .map_err(|error| journal_error(gid, error))?;
            install_journal_appender(journals, gid, OwnedJournalAppender::new(appender))?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::AppendJournal {
            gid,
            generation,
            payload,
        } => {
            let journal = journal_mut(journals, gid)?;
            journal
                .appender
                .append_payload(generation, &payload)
                .map(SessionCommandResult::JournalAppended)
                .map_err(|error| journal_error(gid, error))
        }
        SessionCommand::FlushJournal {
            gid,
            through_sequence,
        } => {
            let journal = journal_mut(journals, gid)?;
            journal
                .appender
                .flush(through_sequence)
                .map(SessionCommandResult::JournalFlushed)
                .map_err(|error| journal_error(gid, error))
        }
        SessionCommand::FlushAllJournals => flush_all_journals(journals),
        SessionCommand::FlushJournalHead { gid } => {
            let journal = journal_mut(journals, gid)?;
            let sequence = journal.appender.appended_sequence();
            journal
                .appender
                .flush(sequence)
                .map(SessionCommandResult::JournalFlushed)
                .map_err(|error| journal_error(gid, error))
        }
        SessionCommand::SnapshotJournal { gid } => journal_mut(journals, gid)?
            .appender
            .snapshot(crate::ReplayLimits::default())
            .map(|replay| SessionCommandResult::JournalSnapshot(Box::new(replay)))
            .map_err(|error| journal_error(gid, error)),
        SessionCommand::CloseJournal { gid } => {
            let journal = journal_mut(journals, gid)?;
            journal
                .appender
                .close_flushed()
                .map_err(|error| journal_error(gid, error))?;
            let removed = journals
                .remove(&gid)
                .expect("the journal remains installed until close succeeds");
            drop(removed);
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::CloseAllFlushedJournals => close_all_flushed_journals(journals),
        SessionCommand::IntegrityCheck => {
            store.integrity_check()?;
            Ok(SessionCommandResult::Unit)
        }
        SessionCommand::CheckpointWalTruncate => {
            store.checkpoint_wal_truncate()?;
            Ok(SessionCommandResult::Unit)
        }
        #[cfg(test)]
        SessionCommand::InstallJournalAppenderWithDropNotice {
            gid,
            appender,
            dropped,
        } => {
            install_journal_appender(
                journals,
                gid,
                OwnedJournalAppender::with_drop_notice(appender, dropped),
            )?;
            Ok(SessionCommandResult::Unit)
        }
        #[cfg(test)]
        SessionCommand::HoldForTest { entered, release } => {
            let _ = entered.send(());
            let (lock, wake) = &*release;
            let mut released = lock_unpoisoned(lock);
            while !*released {
                released = wake
                    .wait(released)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            Ok(SessionCommandResult::Unit)
        }
        #[cfg(test)]
        SessionCommand::PanicForTest => panic!("session owner panic injection"),
    }
}

fn install_journal_appender(
    journals: &mut BTreeMap<Gid, OwnedJournalAppender>,
    gid: Gid,
    journal: OwnedJournalAppender,
) -> Result<(), SessionPersistenceError> {
    let actual = journal.appender.active_header().task_gid();
    if actual != gid {
        return Err(SessionPersistenceError::JournalGidMismatch {
            expected: gid,
            actual,
        });
    }
    if journals.contains_key(&gid) {
        return Err(SessionPersistenceError::DuplicateJournal { gid });
    }
    let replaced = journals.insert(gid, journal);
    debug_assert!(replaced.is_none(), "duplicate journals are rejected above");
    Ok(())
}

fn journal_mut(
    journals: &mut BTreeMap<Gid, OwnedJournalAppender>,
    gid: Gid,
) -> Result<&mut OwnedJournalAppender, SessionPersistenceError> {
    journals
        .get_mut(&gid)
        .ok_or(SessionPersistenceError::MissingJournal { gid })
}

fn journal_error(gid: Gid, error: JournalAppenderError) -> SessionPersistenceError {
    SessionPersistenceError::Journal { gid, error }
}

fn close_all_flushed_journals(
    journals: &mut BTreeMap<Gid, OwnedJournalAppender>,
) -> Result<SessionCommandResult, SessionPersistenceError> {
    for (gid, journal) in journals.iter() {
        if let Some(fault) = journal.appender.fault() {
            return Err(journal_error(*gid, JournalAppenderError::Faulted(fault)));
        }
        let appended = journal.appender.appended_sequence();
        let flushed = journal.appender.flushed_sequence();
        if appended != flushed {
            return Err(journal_error(
                *gid,
                JournalAppenderError::UnflushedRecords { appended, flushed },
            ));
        }
    }

    for (gid, journal) in journals.iter_mut() {
        journal
            .appender
            .close_flushed()
            .map_err(|error| journal_error(*gid, error))?;
    }
    let count = journals.len();
    journals.clear();
    Ok(SessionCommandResult::JournalsClosed(count))
}

fn flush_all_journals(
    journals: &mut BTreeMap<Gid, OwnedJournalAppender>,
) -> Result<SessionCommandResult, SessionPersistenceError> {
    let mut first_error = None;
    let mut flushed = 0_usize;
    for (gid, journal) in journals.iter_mut() {
        let through_sequence = journal.appender.appended_sequence();
        match journal.appender.flush(through_sequence) {
            Ok(_) => flushed += 1,
            Err(error) if first_error.is_none() => {
                first_error = Some(journal_error(*gid, error));
            }
            Err(_) => {}
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(SessionCommandResult::JournalsFlushed(flushed)),
    }
}

fn validate_wait_timeout(
    wait: SessionOwnerWait,
    timeout: Duration,
) -> Result<(), SessionOwnerError> {
    if timeout.is_zero() || timeout > SESSION_OWNER_MAX_WAIT {
        Err(SessionOwnerError::InvalidWaitTimeout(wait))
    } else {
        Ok(())
    }
}

fn begin_shutdown(shared: &SessionShared) {
    {
        let mut admission = lock_unpoisoned(&shared.admission);
        if admission.state == OwnerState::Running {
            admission.state = OwnerState::ShuttingDown;
            admission.sender = None;
        }
    }
    if let Some(owner_thread) = lock_unpoisoned(&shared.owner_thread).as_ref() {
        owner_thread.unpark();
    }
}

fn mark_closed(shared: &SessionShared) {
    let mut admission = lock_unpoisoned(&shared.admission);
    admission.state = OwnerState::Closed;
    admission.sender = None;
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{
        ALL_SESSION_OWNER_ERROR_CODES, ALL_SESSION_PERSISTENCE_ERROR_CODES, OwnerState,
        SESSION_OWNER_DEFAULT_SHUTDOWN_TIMEOUT, SESSION_OWNER_DEFAULT_STARTUP_TIMEOUT,
        SESSION_OWNER_MAX_CAPACITY, SESSION_OWNER_MAX_WAIT, SessionCommand, SessionCommandResult,
        SessionOwner, SessionOwnerConfig, SessionOwnerError, SessionOwnerShutdown,
        SessionOwnerWait, SessionPersistenceError, lock_unpoisoned,
    };
    use crate::{
        ControlJournalAppender, JournalAppenderError, JournalHash, JournalId, JournalPayload,
        PathPlatform, PlatformPath, PreparedJournalSet, ReplayLimits, SessionId,
        SessionIoOperation, SessionNoSpaceCondition, SessionQueueState, SessionRecord,
        SessionStore, SessionStoreConfig, SessionStoreError, SessionTaskRecord, TaskPauseReason,
        journal_segment_path,
    };
    use ariax_core::{Generation, Gid};
    use std::collections::HashSet;
    use std::fs::{self, OpenOptions};
    use std::num::NonZeroUsize;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};

    static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(1);

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            let id = TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("ariax-session-owner-{}-{id}", std::process::id()));
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o700);
                builder
                    .create(&path)
                    .expect("create private test directory");
            }
            #[cfg(windows)]
            ariax_windows_security::create_private_directory(&path)
                .expect("create private test directory");
            Self { path }
        }

        fn database(&self) -> PathBuf {
            self.path.join("session.db")
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn gid(value: u64) -> Gid {
        Gid::new(value).expect("nonzero gid")
    }

    fn hash(value: u8) -> JournalHash {
        JournalHash::new([value; 32]).expect("nonzero hash")
    }

    fn path(value: &str) -> PlatformPath {
        PlatformPath::from_native_bytes(PathPlatform::Unix, value.as_bytes()).expect("path")
    }

    fn session() -> SessionRecord {
        SessionRecord {
            session_id: SessionId::new([1; 16]),
            created_ms: 100,
            updated_ms: 100,
            clean_shutdown: false,
        }
    }

    fn task(value: u64, position: u32) -> SessionTaskRecord {
        let gid = gid(value);
        SessionTaskRecord {
            gid,
            session_id: SessionId::new([1; 16]),
            queue_state: SessionQueueState::Waiting,
            queue_position: position,
            desired_paused: false,
            slow_demotion_count: 0,
            slow_slot: None,
            primary_journal_id: JournalId::new([2; 16]).expect("journal id"),
            primary_journal_path: path(&format!("/journal/{gid}")),
            replica_journal_path: None,
            replica_sequence: None,
            root_display: path(&format!("/output/{gid}")),
            cached_layout_hash: Some(hash(3)),
            cached_root_binding_hash: Some(hash(4)),
            cached_snapshot_hash: hash(5),
            no_space: None::<SessionNoSpaceCondition>,
            created_ms: 100,
            updated_ms: 200,
        }
    }

    fn owner_config(directory: &TestDirectory, capacity: usize) -> SessionOwnerConfig {
        SessionOwnerConfig {
            database_path: directory.database(),
            store: SessionStoreConfig::default(),
            request_capacity: NonZeroUsize::new(capacity).expect("nonzero capacity"),
            startup_timeout: SESSION_OWNER_DEFAULT_STARTUP_TIMEOUT,
            shutdown_timeout: SESSION_OWNER_DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }

    fn wait_for_store_available(directory: &TestDirectory, config: SessionStoreConfig) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match SessionStore::open(directory.database(), config) {
                Ok(store) => {
                    drop(store);
                    return;
                }
                Err(
                    SessionStoreError::OwnerLockBusy
                    | SessionStoreError::Io {
                        operation: SessionIoOperation::InspectPath,
                        kind: std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied,
                    },
                ) if Instant::now() < deadline => {
                    std::thread::park_timeout(Duration::from_millis(5));
                }
                Err(error) => panic!("session store did not become available: {error}"),
            }
        }
    }

    fn appender(
        directory: &TestDirectory,
        task_gid: Gid,
        journal_marker: u8,
    ) -> ControlJournalAppender {
        ControlJournalAppender::create(
            directory
                .path
                .join(format!("journal-{}-{journal_marker}", task_gid.get())),
            task_gid,
            JournalId::new([journal_marker; 16]).expect("nonzero journal id"),
            Generation::INITIAL,
            100,
        )
        .expect("create journal appender")
    }

    fn prepared_appender(
        directory: &TestDirectory,
        task_gid: Gid,
        journal_marker: u8,
    ) -> PreparedJournalSet {
        let appender = appender(directory, task_gid, journal_marker);
        let journal_directory = appender.directory().to_path_buf();
        drop(appender);
        ControlJournalAppender::prepare_recovered(
            &journal_directory,
            &[journal_segment_path(&journal_directory, 0)],
            task_gid,
            JournalId::new([journal_marker; 16]).expect("nonzero journal id"),
            ReplayLimits::default(),
        )
        .expect("prepare journal appender")
    }

    fn prepared_torn_appender(
        directory: &TestDirectory,
        task_gid: Gid,
        journal_marker: u8,
    ) -> (PreparedJournalSet, PathBuf, Vec<u8>) {
        let mut appender = appender(directory, task_gid, journal_marker);
        appender
            .append_payload(
                Generation::INITIAL,
                &JournalPayload::TaskCreated {
                    durability: crate::DurabilityMode::Balanced,
                    creator_version: 1,
                },
            )
            .expect("append created");
        appender
            .append_payload(Generation::INITIAL, &paused_payload())
            .expect("append paused");
        appender.flush(2).expect("flush journal");
        let journal_directory = appender.directory().to_path_buf();
        let segment = appender.active_path().to_path_buf();
        drop(appender);
        let file = OpenOptions::new()
            .write(true)
            .open(&segment)
            .expect("open torn journal");
        let torn_length = file.metadata().expect("journal metadata").len() - 3;
        file.set_len(torn_length).expect("tear final record");
        drop(file);
        let torn_bytes = fs::read(&segment).expect("read torn journal");
        let prepared = ControlJournalAppender::prepare_recovered(
            &journal_directory,
            std::slice::from_ref(&segment),
            task_gid,
            JournalId::new([journal_marker; 16]).expect("nonzero journal id"),
            ReplayLimits::default(),
        )
        .expect("prepare torn journal");
        (prepared, segment, torn_bytes)
    }

    fn paused_payload() -> JournalPayload {
        JournalPayload::TaskPaused {
            reason: TaskPauseReason::User,
        }
    }

    #[test]
    fn owner_error_codes_are_closed_and_unique() {
        assert_eq!(
            ALL_SESSION_OWNER_ERROR_CODES
                .into_iter()
                .collect::<HashSet<_>>()
                .len(),
            ALL_SESSION_OWNER_ERROR_CODES.len()
        );
        assert_eq!(
            ALL_SESSION_PERSISTENCE_ERROR_CODES
                .into_iter()
                .collect::<HashSet<_>>()
                .len(),
            ALL_SESSION_PERSISTENCE_ERROR_CODES.len()
        );
    }

    #[test]
    fn owner_rejects_invalid_capacity_and_waits_before_spawning() {
        let directory = TestDirectory::new();
        let mut config = owner_config(&directory, 1);
        config.request_capacity =
            NonZeroUsize::new(SESSION_OWNER_MAX_CAPACITY + 1).expect("over-cap capacity");
        assert!(matches!(
            SessionOwner::spawn(config, |_: &str| true),
            Err(SessionOwnerError::InvalidRequestCapacity {
                requested,
                maximum: SESSION_OWNER_MAX_CAPACITY,
            }) if requested == SESSION_OWNER_MAX_CAPACITY + 1
        ));
        assert!(!directory.database().exists());

        let mut config = owner_config(&directory, 1);
        config.startup_timeout = Duration::ZERO;
        assert!(matches!(
            SessionOwner::spawn(config, |_: &str| true),
            Err(SessionOwnerError::InvalidWaitTimeout(
                SessionOwnerWait::Startup
            ))
        ));
        assert!(!directory.database().exists());

        let mut config = owner_config(&directory, 1);
        config.startup_timeout = SESSION_OWNER_MAX_WAIT + Duration::from_nanos(1);
        assert!(matches!(
            SessionOwner::spawn(config, |_: &str| true),
            Err(SessionOwnerError::InvalidWaitTimeout(
                SessionOwnerWait::Startup
            ))
        ));
        assert!(!directory.database().exists());

        let mut config = owner_config(&directory, 1);
        config.shutdown_timeout = Duration::ZERO;
        assert!(matches!(
            SessionOwner::spawn(config, |_: &str| true),
            Err(SessionOwnerError::InvalidWaitTimeout(
                SessionOwnerWait::Shutdown
            ))
        ));
        assert!(!directory.database().exists());

        let mut config = owner_config(&directory, 1);
        config.shutdown_timeout = SESSION_OWNER_MAX_WAIT + Duration::from_nanos(1);
        assert!(matches!(
            SessionOwner::spawn(config, |_: &str| true),
            Err(SessionOwnerError::InvalidWaitTimeout(
                SessionOwnerWait::Shutdown
            ))
        ));
        assert!(!directory.database().exists());
    }

    #[test]
    fn invalid_explicit_shutdown_wait_does_not_close_admission() {
        let directory = TestDirectory::new();
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 1), |_: &str| true).expect("spawn owner");

        for timeout in [
            Duration::ZERO,
            SESSION_OWNER_MAX_WAIT + Duration::from_nanos(1),
        ] {
            assert!(matches!(
                handle.shutdown_with_timeout(timeout),
                Err(SessionOwnerError::InvalidWaitTimeout(
                    SessionOwnerWait::Shutdown
                ))
            ));
            assert_eq!(
                handle
                    .execute(SessionCommand::IntegrityCheck)
                    .expect("invalid wait leaves admission open"),
                SessionCommandResult::Unit
            );
        }

        handle.shutdown().expect("shutdown owner");
    }

    #[test]
    fn owner_exclusively_owns_store_and_round_trips_commands() {
        let directory = TestDirectory::new();
        let (handle, startup) =
            SessionOwner::spawn(owner_config(&directory, 4), |_: &str| true).expect("spawn owner");
        assert!(startup.session.is_none());
        assert!(startup.tasks.is_empty());
        assert!(matches!(
            SessionStore::open(directory.database(), SessionStoreConfig::default()),
            Err(SessionStoreError::OwnerLockBusy)
        ));

        assert_eq!(
            handle
                .execute(SessionCommand::PutSession(session()))
                .expect("put session"),
            SessionCommandResult::Unit
        );
        handle
            .execute(SessionCommand::PutTask(task(1, 0)))
            .expect("put task");
        assert!(matches!(
            handle.execute(SessionCommand::ReadTasks),
            Ok(SessionCommandResult::Tasks(tasks)) if tasks == vec![task(1, 0)]
        ));
        handle.shutdown().expect("shutdown owner");

        let reopened = SessionStore::open(directory.database(), SessionStoreConfig::default())
            .expect("owner lock released on owner thread");
        assert_eq!(reopened.tasks().expect("reopened tasks"), vec![task(1, 0)]);
    }

    #[test]
    fn bounded_admission_rejects_full_queue_without_blocking_completion_delivery() {
        let directory = TestDirectory::new();
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 1), |_: &str| true).expect("spawn owner");
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let held = handle
            .try_submit(SessionCommand::HoldForTest {
                entered: entered_sender,
                release: Arc::clone(&release),
            })
            .expect("accept held command");
        entered_receiver.recv().expect("owner entered hold");
        let (next_entered_sender, next_entered_receiver) = std::sync::mpsc::sync_channel(1);
        let next_release = Arc::new((Mutex::new(false), Condvar::new()));
        let dropped_completion = handle
            .try_submit(SessionCommand::HoldForTest {
                entered: next_entered_sender,
                release: Arc::clone(&next_release),
            })
            .expect("fill request queue");
        assert!(matches!(
            handle.try_submit(SessionCommand::ReadTasks),
            Err(SessionOwnerError::QueueFull)
        ));
        drop(dropped_completion);

        let (released, wake) = &*release;
        *released.lock().expect("release lock") = true;
        wake.notify_one();
        assert_eq!(
            held.wait().expect("held completion"),
            SessionCommandResult::Unit
        );
        next_entered_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("owner dequeued the command with a dropped completion");
        let last = handle
            .try_submit(SessionCommand::IntegrityCheck)
            .expect("next admission slot is available");
        let (released, wake) = &*next_release;
        *released.lock().expect("release next lock") = true;
        wake.notify_one();
        assert_eq!(
            last.wait()
                .expect("owner remains live after dropped completion"),
            SessionCommandResult::Unit
        );
        handle.shutdown().expect("shutdown owner");
    }

    #[test]
    fn owned_submission_preserves_the_exact_command_across_admission_rejection() {
        let directory = TestDirectory::new();
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 1), |_: &str| true).expect("spawn owner");
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let held = handle
            .try_submit(SessionCommand::HoldForTest {
                entered: entered_sender,
                release: Arc::clone(&release),
            })
            .expect("accept held command");
        entered_receiver.recv().expect("owner entered hold");
        let queued = handle
            .try_submit(SessionCommand::IntegrityCheck)
            .expect("fill request queue");

        let rejection = match handle.try_submit_owned(SessionCommand::ReadQueueOrder {
            state: SessionQueueState::Paused,
        }) {
            Err(rejection) => rejection,
            Ok(_) => panic!("full queue unexpectedly accepted a command"),
        };
        let (command, error) = rejection.into_parts();
        assert!(matches!(error, SessionOwnerError::QueueFull));
        assert!(matches!(
            command,
            SessionCommand::ReadQueueOrder {
                state: SessionQueueState::Paused
            }
        ));

        let (released, wake) = &*release;
        *released.lock().expect("release lock") = true;
        wake.notify_one();
        assert_eq!(
            held.wait().expect("held completion"),
            SessionCommandResult::Unit
        );
        assert_eq!(
            queued.wait().expect("queued completion"),
            SessionCommandResult::Unit
        );
        handle.shutdown().expect("shutdown owner");

        let rejection = match handle.try_submit_owned(SessionCommand::ReadQueueOrder {
            state: SessionQueueState::Stopped,
        }) {
            Err(rejection) => rejection,
            Ok(_) => panic!("closed owner unexpectedly accepted a command"),
        };
        let (command, error) = rejection.into_parts();
        assert!(matches!(error, SessionOwnerError::Unavailable));
        assert!(matches!(
            command,
            SessionCommand::ReadQueueOrder {
                state: SessionQueueState::Stopped
            }
        ));
    }

    #[test]
    fn owned_install_rejection_retries_the_same_appender_on_the_owner_thread() {
        let directory = TestDirectory::new();
        let caller_thread = std::thread::current().id();
        let task_gid = gid(1);
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 1), |_: &str| true).expect("spawn owner");
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let held = handle
            .try_submit(SessionCommand::HoldForTest {
                entered: entered_sender,
                release: Arc::clone(&release),
            })
            .expect("accept held command");
        entered_receiver.recv().expect("owner entered hold");
        let queued = handle
            .try_submit(SessionCommand::IntegrityCheck)
            .expect("fill request queue");
        let (dropped_sender, dropped_receiver) = std::sync::mpsc::sync_channel(1);

        let rejection =
            match handle.try_submit_owned(SessionCommand::InstallJournalAppenderWithDropNotice {
                gid: task_gid,
                appender: appender(&directory, task_gid, 1),
                dropped: dropped_sender,
            }) {
                Err(rejection) => rejection,
                Ok(_) => panic!("full queue unexpectedly accepted the appender"),
            };
        let (command, error) = rejection.into_parts();
        assert!(matches!(error, SessionOwnerError::QueueFull));

        let (released, wake) = &*release;
        *released.lock().expect("release lock") = true;
        wake.notify_one();
        assert_eq!(
            held.wait().expect("held completion"),
            SessionCommandResult::Unit
        );
        assert_eq!(
            queued.wait().expect("queued completion"),
            SessionCommandResult::Unit
        );
        assert_eq!(
            handle
                .try_submit_owned(command)
                .expect("retry exact rejected install")
                .wait()
                .expect("install completion"),
            SessionCommandResult::Unit
        );
        handle.shutdown().expect("shutdown owner");

        let (owner_thread, drop_thread) = dropped_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("retried appender dropped on shutdown");
        assert_eq!(drop_thread, owner_thread);
        assert_ne!(drop_thread, caller_thread);
    }

    #[test]
    fn owned_prepared_install_rejection_retries_the_exact_open_authority() {
        let directory = TestDirectory::new();
        let task_gid = gid(1);
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 1), |_: &str| true).expect("spawn owner");
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let held = handle
            .try_submit(SessionCommand::HoldForTest {
                entered: entered_sender,
                release: Arc::clone(&release),
            })
            .expect("accept held command");
        entered_receiver.recv().expect("owner entered hold");
        let queued = handle
            .try_submit(SessionCommand::IntegrityCheck)
            .expect("fill request queue");

        let rejection = match handle.try_submit_owned(SessionCommand::InstallPreparedJournal {
            gid: task_gid,
            prepared: prepared_appender(&directory, task_gid, 1),
            recovery_starting_generation: Generation::INITIAL,
            recovery_created_at_unix_ms: 200,
        }) {
            Err(rejection) => rejection,
            Ok(_) => panic!("full queue unexpectedly accepted prepared authority"),
        };
        let (command, error) = rejection.into_parts();
        assert!(matches!(error, SessionOwnerError::QueueFull));
        assert!(matches!(
            command,
            SessionCommand::InstallPreparedJournal { gid, .. } if gid == task_gid
        ));

        let (released, wake) = &*release;
        *released.lock().expect("release lock") = true;
        wake.notify_one();
        assert_eq!(
            held.wait().expect("held completion"),
            SessionCommandResult::Unit
        );
        assert_eq!(
            queued.wait().expect("queued completion"),
            SessionCommandResult::Unit
        );
        assert_eq!(
            handle
                .try_submit_owned(command)
                .expect("retry exact prepared install")
                .wait()
                .expect("prepared install completion"),
            SessionCommandResult::Unit
        );
        assert!(matches!(
            handle
                .execute(SessionCommand::AppendJournal {
                    gid: task_gid,
                    generation: Generation::INITIAL,
                    payload: JournalPayload::TaskCreated {
                        durability: crate::DurabilityMode::Balanced,
                        creator_version: 1,
                    },
                })
                .expect("append through prepared appender"),
            SessionCommandResult::JournalAppended(appended) if appended.sequence() == 1
        ));
        handle.shutdown().expect("shutdown owner");
    }

    #[test]
    fn prepared_install_identity_rejections_do_not_repair_the_torn_set() {
        let directory = TestDirectory::new();
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 4), |_: &str| true).expect("spawn owner");
        let task_gid = gid(1);
        handle
            .execute(SessionCommand::InstallJournalAppender {
                gid: task_gid,
                appender: appender(&directory, task_gid, 1),
            })
            .expect("install existing appender");

        let (duplicate, duplicate_segment, duplicate_bytes) =
            prepared_torn_appender(&directory, task_gid, 2);
        assert!(matches!(
            handle.execute(SessionCommand::InstallPreparedJournal {
                gid: task_gid,
                prepared: duplicate,
                recovery_starting_generation: Generation::INITIAL,
                recovery_created_at_unix_ms: 200,
            }),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::DuplicateJournal { gid }
            )) if gid == task_gid
        ));
        assert_eq!(
            fs::read(&duplicate_segment).expect("read rejected duplicate"),
            duplicate_bytes
        );
        assert!(
            !journal_segment_path(duplicate_segment.parent().expect("journal directory"), 1,)
                .exists()
        );

        let (mismatched, mismatched_segment, mismatched_bytes) =
            prepared_torn_appender(&directory, gid(2), 3);
        assert!(matches!(
            handle.execute(SessionCommand::InstallPreparedJournal {
                gid: gid(3),
                prepared: mismatched,
                recovery_starting_generation: Generation::INITIAL,
                recovery_created_at_unix_ms: 200,
            }),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::JournalGidMismatch { expected, actual }
            )) if expected == gid(3) && actual == gid(2)
        ));
        assert_eq!(
            fs::read(&mismatched_segment).expect("read rejected mismatch"),
            mismatched_bytes
        );
        assert!(
            !journal_segment_path(mismatched_segment.parent().expect("journal directory"), 1,)
                .exists()
        );
        handle.shutdown().expect("shutdown owner");
    }

    #[test]
    fn owned_submission_preserves_the_exact_command_while_shutting_down() {
        let directory = TestDirectory::new();
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 1), |_: &str| true).expect("spawn owner");
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let held = handle
            .try_submit(SessionCommand::HoldForTest {
                entered: entered_sender,
                release: Arc::clone(&release),
            })
            .expect("accept held command");
        entered_receiver.recv().expect("owner entered hold");

        let shutdown_handle = handle.clone();
        let shutdown = std::thread::spawn(move || {
            shutdown_handle.shutdown_with_timeout(Duration::from_secs(1))
        });
        while lock_unpoisoned(&handle.shared.admission).state != OwnerState::ShuttingDown {
            std::thread::yield_now();
        }
        let rejection = match handle.try_submit_owned(SessionCommand::ReadQueueOrder {
            state: SessionQueueState::Demoted,
        }) {
            Err(rejection) => rejection,
            Ok(_) => panic!("shutting-down owner unexpectedly accepted a command"),
        };
        let (command, error) = rejection.into_parts();
        assert!(matches!(error, SessionOwnerError::ShuttingDown));
        assert!(matches!(
            command,
            SessionCommand::ReadQueueOrder {
                state: SessionQueueState::Demoted
            }
        ));

        let (released, wake) = &*release;
        *released.lock().expect("release lock") = true;
        wake.notify_one();
        assert_eq!(
            held.wait().expect("held completion"),
            SessionCommandResult::Unit
        );
        assert_eq!(
            shutdown
                .join()
                .expect("join shutdown caller")
                .expect("shutdown"),
            SessionOwnerShutdown::Joined
        );
    }

    #[test]
    fn shutdown_closes_admission_and_drains_already_accepted_commands() {
        let directory = TestDirectory::new();
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 2), |_: &str| true).expect("spawn owner");
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let held = handle
            .try_submit(SessionCommand::HoldForTest {
                entered: entered_sender,
                release: Arc::clone(&release),
            })
            .expect("held command");
        entered_receiver.recv().expect("entered hold");
        let accepted = handle
            .try_submit(SessionCommand::PutSession(session()))
            .expect("accepted before shutdown");
        let shutdown_handle = handle.clone();
        let shutdown = std::thread::spawn(move || {
            shutdown_handle.shutdown_with_timeout(Duration::from_secs(1))
        });
        while !matches!(
            handle.try_submit(SessionCommand::ReadTasks),
            Err(SessionOwnerError::ShuttingDown | SessionOwnerError::Unavailable)
        ) {
            std::thread::yield_now();
        }
        let (released, wake) = &*release;
        *released.lock().expect("release lock") = true;
        wake.notify_one();
        assert_eq!(
            held.wait().expect("held result"),
            SessionCommandResult::Unit
        );
        assert_eq!(
            accepted.wait().expect("accepted command drained"),
            SessionCommandResult::Unit
        );
        assert_eq!(
            shutdown
                .join()
                .expect("join shutdown caller")
                .expect("shutdown"),
            SessionOwnerShutdown::Joined
        );
        assert!(matches!(
            handle.try_submit(SessionCommand::ReadTasks),
            Err(SessionOwnerError::Unavailable)
        ));
    }

    #[test]
    fn shutdown_timeout_detaches_without_fake_closing_and_later_drains() {
        let directory = TestDirectory::new();
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 2), |_: &str| true).expect("spawn owner");
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let held = handle
            .try_submit(SessionCommand::HoldForTest {
                entered: entered_sender,
                release: Arc::clone(&release),
            })
            .expect("held command");
        entered_receiver.recv().expect("entered hold");
        let accepted = handle
            .try_submit(SessionCommand::PutSession(session()))
            .expect("accepted before shutdown");

        let timeout = Duration::from_millis(20);
        let started = Instant::now();
        assert_eq!(
            handle
                .shutdown_with_timeout(timeout)
                .expect("bounded shutdown result"),
            SessionOwnerShutdown::DetachedUncertain { timeout }
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(matches!(
            handle.try_submit(SessionCommand::ReadTasks),
            Err(SessionOwnerError::ShuttingDown)
        ));
        assert!(matches!(
            SessionStore::open(directory.database(), SessionStoreConfig::default()),
            Err(SessionStoreError::OwnerLockBusy)
        ));

        let (released, wake) = &*release;
        *released.lock().expect("release lock") = true;
        wake.notify_one();
        assert_eq!(
            held.wait().expect("held result"),
            SessionCommandResult::Unit
        );
        assert_eq!(
            accepted.wait().expect("accepted command drained"),
            SessionCommandResult::Unit
        );
        wait_for_store_available(&directory, SessionStoreConfig::default());
        assert_eq!(
            handle
                .shutdown_with_timeout(Duration::from_millis(20))
                .expect("closed owner result"),
            SessionOwnerShutdown::Joined
        );
    }

    #[test]
    fn configured_shutdown_timeout_reports_typed_detached_uncertainty() {
        let directory = TestDirectory::new();
        let timeout = Duration::from_millis(20);
        let mut config = owner_config(&directory, 1);
        config.shutdown_timeout = timeout;
        let (handle, _) = SessionOwner::spawn(config, |_: &str| true).expect("spawn owner");
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let held = handle
            .try_submit(SessionCommand::HoldForTest {
                entered: entered_sender,
                release: Arc::clone(&release),
            })
            .expect("held command");
        entered_receiver.recv().expect("entered hold");

        let started = Instant::now();
        assert!(matches!(
            handle.shutdown(),
            Err(SessionOwnerError::ShutdownTimedOut { timeout: actual })
                if actual == timeout
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(matches!(
            handle.try_submit(SessionCommand::ReadTasks),
            Err(SessionOwnerError::ShuttingDown)
        ));
        assert!(matches!(
            SessionStore::open(directory.database(), SessionStoreConfig::default()),
            Err(SessionStoreError::OwnerLockBusy)
        ));

        let (released, wake) = &*release;
        *released.lock().expect("release lock") = true;
        wake.notify_one();
        assert_eq!(
            held.wait().expect("held result"),
            SessionCommandResult::Unit
        );
        wait_for_store_available(&directory, SessionStoreConfig::default());
    }

    #[test]
    fn last_handle_drop_is_zero_wait_and_drains_accepted_commands() {
        let directory = TestDirectory::new();
        let store_config = SessionStoreConfig::default();
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 2), |_: &str| true).expect("spawn owner");
        let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let held = handle
            .try_submit(SessionCommand::HoldForTest {
                entered: entered_sender,
                release: Arc::clone(&release),
            })
            .expect("held command");
        entered_receiver.recv().expect("entered hold");
        let accepted = handle
            .try_submit(SessionCommand::PutSession(session()))
            .expect("accepted before last handle drop");

        let started = Instant::now();
        drop(handle);
        assert!(started.elapsed() < Duration::from_secs(1));

        let (released, wake) = &*release;
        *released.lock().expect("release lock") = true;
        wake.notify_one();
        assert_eq!(
            held.wait().expect("held result"),
            SessionCommandResult::Unit
        );
        assert_eq!(
            accepted.wait().expect("accepted command drained"),
            SessionCommandResult::Unit
        );
        wait_for_store_available(&directory, store_config);
        let reopened = SessionStore::open(directory.database(), store_config)
            .expect("owner lock released after detached drain");
        assert_eq!(
            reopened.session().expect("reopened session"),
            Some(session())
        );
    }

    #[test]
    fn startup_timeout_detaches_a_blocked_unpublished_owner() {
        let directory = TestDirectory::new();
        let store_config = SessionStoreConfig {
            busy_timeout_ms: 5_000,
            prefer_wal: false,
            ..SessionStoreConfig::default()
        };
        drop(
            SessionStore::open(directory.database(), store_config)
                .expect("create session database"),
        );
        let blocker = rusqlite::Connection::open(directory.database()).expect("open blocker");
        blocker
            .execute_batch("PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE;")
            .expect("hold SQLite exclusive lock");

        let timeout = Duration::from_millis(20);
        let mut config = owner_config(&directory, 2);
        config.store = store_config;
        config.startup_timeout = timeout;
        let started = Instant::now();
        assert!(matches!(
            SessionOwner::spawn(config, |_: &str| true),
            Err(SessionOwnerError::StartupTimedOut { timeout: actual }) if actual == timeout
        ));
        assert!(started.elapsed() < Duration::from_secs(1));

        blocker.execute_batch("ROLLBACK").expect("release blocker");
        drop(blocker);
        wait_for_store_available(&directory, store_config);
    }

    #[test]
    fn command_rejection_is_typed_and_does_not_fault_owner() {
        let directory = TestDirectory::new();
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 2), |_: &str| true).expect("spawn owner");
        assert!(matches!(
            handle.execute(SessionCommand::PutTask(task(1, 0))),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::Store(SessionStoreError::Sqlite(_))
            ))
        ));
        assert_eq!(
            handle
                .execute(SessionCommand::IntegrityCheck)
                .expect("owner remains available"),
            SessionCommandResult::Unit
        );
        handle.shutdown().expect("shutdown owner");
    }

    #[test]
    fn journal_snapshots_keep_native_ownership_and_preserve_command_order() {
        let directory = TestDirectory::new();
        let task_gid = gid(1);
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 4), |_: &str| true).expect("owner");
        assert!(matches!(
            handle.execute(SessionCommand::SnapshotJournal { gid: task_gid }),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::MissingJournal { .. }
            ))
        ));
        handle
            .execute(SessionCommand::InstallJournalAppender {
                gid: task_gid,
                appender: appender(&directory, task_gid, 1),
            })
            .expect("install");
        for sequence in 1..=2 {
            let result = handle
                .execute(SessionCommand::AppendJournal {
                    gid: task_gid,
                    generation: Generation::INITIAL,
                    payload: JournalPayload::TaskCreated {
                        durability: crate::DurabilityMode::Balanced,
                        creator_version: 1,
                    },
                })
                .expect("append");
            assert!(
                matches!(result, SessionCommandResult::JournalAppended(value) if value.sequence() == sequence)
            );
            let SessionCommandResult::JournalSnapshot(snapshot) = handle
                .execute(SessionCommand::SnapshotJournal { gid: task_gid })
                .expect("snapshot")
            else {
                panic!("wrong result");
            };
            assert_eq!(snapshot.last_sequence, sequence);
            assert_eq!(snapshot.records.len(), sequence as usize);
        }
        assert!(
            matches!(handle.execute(SessionCommand::FlushJournalHead { gid: task_gid }).expect("flush"), SessionCommandResult::JournalFlushed(value) if value.through_sequence() == 2)
        );
        handle
            .execute(SessionCommand::CloseJournal { gid: task_gid })
            .expect("close");
        assert!(matches!(
            handle.execute(SessionCommand::FlushJournalHead { gid: task_gid }),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::MissingJournal { .. }
            ))
        ));
        handle.shutdown().expect("shutdown");
    }

    #[test]
    fn owner_installs_appends_flushes_and_closes_one_journal() {
        let directory = TestDirectory::new();
        let task_gid = gid(1);
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 4), |_: &str| true).expect("spawn owner");

        assert_eq!(
            handle
                .execute(SessionCommand::InstallJournalAppender {
                    gid: task_gid,
                    appender: appender(&directory, task_gid, 1),
                })
                .expect("install journal"),
            SessionCommandResult::Unit
        );
        let appended = handle
            .execute(SessionCommand::AppendJournal {
                gid: task_gid,
                generation: Generation::INITIAL,
                payload: paused_payload(),
            })
            .expect("append payload");
        assert!(matches!(
            appended,
            SessionCommandResult::JournalAppended(evidence) if evidence.sequence() == 1
        ));
        let flushed = handle
            .execute(SessionCommand::FlushJournal {
                gid: task_gid,
                through_sequence: 1,
            })
            .expect("flush payload");
        assert!(matches!(
            flushed,
            SessionCommandResult::JournalFlushed(evidence)
                if evidence.through_sequence() == 1
        ));
        assert_eq!(
            handle
                .execute(SessionCommand::CloseJournal { gid: task_gid })
                .expect("close flushed journal"),
            SessionCommandResult::Unit
        );
        assert!(matches!(
            handle.execute(SessionCommand::AppendJournal {
                gid: task_gid,
                generation: Generation::INITIAL,
                payload: paused_payload(),
            }),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::MissingJournal { gid }
            )) if gid == task_gid
        ));
        handle.shutdown().expect("shutdown owner");
    }

    #[test]
    fn journal_install_rejects_missing_duplicate_and_mismatched_identity() {
        let directory = TestDirectory::new();
        let task_gid = gid(1);
        let other_gid = gid(2);
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 4), |_: &str| true).expect("spawn owner");

        assert!(matches!(
            handle.execute(SessionCommand::FlushJournal {
                gid: task_gid,
                through_sequence: 0,
            }),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::MissingJournal { gid }
            )) if gid == task_gid
        ));
        assert!(matches!(
            handle.execute(SessionCommand::InstallJournalAppender {
                gid: task_gid,
                appender: appender(&directory, other_gid, 2),
            }),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::JournalGidMismatch { expected, actual }
            )) if expected == task_gid && actual == other_gid
        ));
        handle
            .execute(SessionCommand::InstallJournalAppender {
                gid: task_gid,
                appender: appender(&directory, task_gid, 3),
            })
            .expect("install matching journal");
        assert!(matches!(
            handle.execute(SessionCommand::InstallJournalAppender {
                gid: task_gid,
                appender: appender(&directory, task_gid, 4),
            }),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::DuplicateJournal { gid }
            )) if gid == task_gid
        ));
        assert_eq!(
            handle
                .execute(SessionCommand::CloseAllFlushedJournals)
                .expect("close installed empty journal"),
            SessionCommandResult::JournalsClosed(1)
        );
        handle.shutdown().expect("shutdown owner");
    }

    #[test]
    fn accepted_install_validation_rejections_drop_appenders_on_the_owner_thread() {
        let directory = TestDirectory::new();
        let caller_thread = std::thread::current().id();
        let task_gid = gid(1);
        let other_gid = gid(2);
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 4), |_: &str| true).expect("spawn owner");

        let (mismatch_sender, mismatch_receiver) = std::sync::mpsc::sync_channel(1);
        assert!(matches!(
            handle.execute(SessionCommand::InstallJournalAppenderWithDropNotice {
                gid: task_gid,
                appender: appender(&directory, other_gid, 1),
                dropped: mismatch_sender,
            }),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::JournalGidMismatch { expected, actual }
            )) if expected == task_gid && actual == other_gid
        ));
        let (mismatch_owner, mismatch_drop) = mismatch_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("mismatched appender dropped");
        assert_eq!(mismatch_drop, mismatch_owner);
        assert_ne!(mismatch_drop, caller_thread);

        handle
            .execute(SessionCommand::InstallJournalAppender {
                gid: task_gid,
                appender: appender(&directory, task_gid, 2),
            })
            .expect("install first matching appender");
        let (duplicate_sender, duplicate_receiver) = std::sync::mpsc::sync_channel(1);
        assert!(matches!(
            handle.execute(SessionCommand::InstallJournalAppenderWithDropNotice {
                gid: task_gid,
                appender: appender(&directory, task_gid, 3),
                dropped: duplicate_sender,
            }),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::DuplicateJournal { gid }
            )) if gid == task_gid
        ));
        let (duplicate_owner, duplicate_drop) = duplicate_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("duplicate appender dropped");
        assert_eq!(duplicate_drop, duplicate_owner);
        assert_ne!(duplicate_drop, caller_thread);

        handle
            .execute(SessionCommand::CloseAllFlushedJournals)
            .expect("close retained appender");
        handle.shutdown().expect("shutdown owner");
    }

    #[test]
    fn close_rejects_unflushed_data_without_removing_the_journal() {
        let directory = TestDirectory::new();
        let task_gid = gid(1);
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 4), |_: &str| true).expect("spawn owner");
        handle
            .execute(SessionCommand::InstallJournalAppender {
                gid: task_gid,
                appender: appender(&directory, task_gid, 1),
            })
            .expect("install journal");
        handle
            .execute(SessionCommand::AppendJournal {
                gid: task_gid,
                generation: Generation::INITIAL,
                payload: paused_payload(),
            })
            .expect("append payload");

        assert!(matches!(
            handle.execute(SessionCommand::CloseJournal { gid: task_gid }),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::Journal {
                    gid,
                    error: JournalAppenderError::UnflushedRecords {
                        appended: 1,
                        flushed: 0,
                    },
                }
            )) if gid == task_gid
        ));
        handle
            .execute(SessionCommand::FlushJournal {
                gid: task_gid,
                through_sequence: 1,
            })
            .expect("journal remains installed for flush");
        handle
            .execute(SessionCommand::CloseJournal { gid: task_gid })
            .expect("close after flush");
        handle.shutdown().expect("shutdown owner");
    }

    #[test]
    fn flush_all_flushes_every_installed_journal() {
        let directory = TestDirectory::new();
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 8), |_: &str| true).expect("spawn owner");
        for (task_gid, marker) in [(gid(1), 1), (gid(2), 2)] {
            handle
                .execute(SessionCommand::InstallJournalAppender {
                    gid: task_gid,
                    appender: appender(&directory, task_gid, marker),
                })
                .expect("install journal");
            handle
                .execute(SessionCommand::AppendJournal {
                    gid: task_gid,
                    generation: Generation::INITIAL,
                    payload: paused_payload(),
                })
                .expect("append payload");
        }

        assert_eq!(
            handle
                .execute(SessionCommand::FlushAllJournals)
                .expect("flush every journal"),
            SessionCommandResult::JournalsFlushed(2)
        );
        assert_eq!(
            handle
                .execute(SessionCommand::CloseAllFlushedJournals)
                .expect("close every flushed journal"),
            SessionCommandResult::JournalsClosed(2)
        );
        handle.shutdown().expect("shutdown owner");
    }

    #[test]
    fn flush_all_reports_the_first_fault_but_attempts_later_journals() {
        let directory = TestDirectory::new();
        let first_gid = gid(1);
        let second_gid = gid(2);
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 8), |_: &str| true).expect("spawn owner");
        let mut first = appender(&directory, first_gid, 1);
        first.fail_next_flush_for_test();
        for (task_gid, appender) in [
            (first_gid, first),
            (second_gid, appender(&directory, second_gid, 2)),
        ] {
            handle
                .execute(SessionCommand::InstallJournalAppender {
                    gid: task_gid,
                    appender,
                })
                .expect("install journal");
            handle
                .execute(SessionCommand::AppendJournal {
                    gid: task_gid,
                    generation: Generation::INITIAL,
                    payload: paused_payload(),
                })
                .expect("append payload");
        }

        assert!(matches!(
            handle.execute(SessionCommand::FlushAllJournals),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::Journal {
                    gid,
                    error: JournalAppenderError::Io { .. },
                }
            )) if gid == first_gid
        ));
        handle
            .execute(SessionCommand::CloseJournal { gid: second_gid })
            .expect("later journal was still flushed");
        handle.shutdown().expect("shutdown owner");
    }

    #[test]
    fn close_all_preflights_every_journal_before_closing_any() {
        let directory = TestDirectory::new();
        let first_gid = gid(1);
        let second_gid = gid(2);
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 8), |_: &str| true).expect("spawn owner");
        for (task_gid, marker) in [(first_gid, 1), (second_gid, 2)] {
            handle
                .execute(SessionCommand::InstallJournalAppender {
                    gid: task_gid,
                    appender: appender(&directory, task_gid, marker),
                })
                .expect("install journal");
            handle
                .execute(SessionCommand::AppendJournal {
                    gid: task_gid,
                    generation: Generation::INITIAL,
                    payload: paused_payload(),
                })
                .expect("append payload");
        }
        handle
            .execute(SessionCommand::FlushJournal {
                gid: first_gid,
                through_sequence: 1,
            })
            .expect("flush first journal");

        assert!(matches!(
            handle.execute(SessionCommand::CloseAllFlushedJournals),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::Journal {
                    gid,
                    error: JournalAppenderError::UnflushedRecords { .. },
                }
            )) if gid == second_gid
        ));
        let appended = handle
            .execute(SessionCommand::AppendJournal {
                gid: first_gid,
                generation: Generation::INITIAL,
                payload: paused_payload(),
            })
            .expect("preflight failure left the first journal installed");
        assert!(matches!(
            appended,
            SessionCommandResult::JournalAppended(evidence) if evidence.sequence() == 2
        ));
        for (task_gid, through_sequence) in [(first_gid, 2), (second_gid, 1)] {
            handle
                .execute(SessionCommand::FlushJournal {
                    gid: task_gid,
                    through_sequence,
                })
                .expect("flush journal");
        }
        assert_eq!(
            handle
                .execute(SessionCommand::CloseAllFlushedJournals)
                .expect("close all journals"),
            SessionCommandResult::JournalsClosed(2)
        );
        handle.shutdown().expect("shutdown owner");
    }

    #[test]
    fn shutdown_and_last_handle_drop_destroy_journals_on_the_owner_thread() {
        let directory = TestDirectory::new();
        let caller_thread = std::thread::current().id();
        let task_gid = gid(1);
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 2), |_: &str| true).expect("spawn owner");
        let (dropped_sender, dropped_receiver) = std::sync::mpsc::sync_channel(1);
        handle
            .execute(SessionCommand::InstallJournalAppenderWithDropNotice {
                gid: task_gid,
                appender: appender(&directory, task_gid, 1),
                dropped: dropped_sender,
            })
            .expect("install observed journal");
        handle.shutdown().expect("shutdown owner");
        let (shutdown_owner_thread, shutdown_drop_thread) = dropped_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("shutdown dropped journal");
        assert_eq!(shutdown_drop_thread, shutdown_owner_thread);
        assert_ne!(shutdown_drop_thread, caller_thread);

        let (handle, _) = SessionOwner::spawn(owner_config(&directory, 2), |_: &str| true)
            .expect("respawn owner");
        let (dropped_sender, dropped_receiver) = std::sync::mpsc::sync_channel(1);
        handle
            .execute(SessionCommand::InstallJournalAppenderWithDropNotice {
                gid: task_gid,
                appender: appender(&directory, task_gid, 2),
                dropped: dropped_sender,
            })
            .expect("install second observed journal");
        drop(handle);
        let (last_handle_owner_thread, last_handle_drop_thread) = dropped_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("last handle drop released journal");
        assert_eq!(last_handle_drop_thread, last_handle_owner_thread);
        assert_ne!(last_handle_drop_thread, caller_thread);
    }

    #[test]
    fn owner_panic_closes_completions_and_is_reported_on_join() {
        let directory = TestDirectory::new();
        let (handle, _) =
            SessionOwner::spawn(owner_config(&directory, 2), |_: &str| true).expect("spawn owner");
        let completion = handle
            .try_submit(SessionCommand::PanicForTest)
            .expect("accept panic injection");
        assert!(matches!(
            completion.wait(),
            Err(SessionOwnerError::Unavailable)
        ));
        match handle.try_submit(SessionCommand::ReadTasks) {
            Err(SessionOwnerError::Unavailable) => {}
            Ok(completion) => {
                assert!(matches!(
                    completion.wait(),
                    Err(SessionOwnerError::Unavailable)
                ));
            }
            Err(error) => panic!("unexpected post-panic admission result: {error}"),
        }
        assert!(matches!(
            handle.shutdown(),
            Err(SessionOwnerError::OwnerPanicked)
        ));
    }

    #[test]
    fn startup_store_failure_never_publishes_a_handle() {
        let directory = TestDirectory::new();
        let first = SessionStore::open(directory.database(), SessionStoreConfig::default())
            .expect("hold direct owner lock");
        assert!(matches!(
            SessionOwner::spawn(owner_config(&directory, 1), |_: &str| true),
            Err(SessionOwnerError::Persistence(
                SessionPersistenceError::Store(SessionStoreError::OwnerLockBusy)
            ))
        ));
        drop(first);
        let (handle, _) = SessionOwner::spawn(owner_config(&directory, 1), |_: &str| true)
            .expect("spawn after lock release");
        handle.shutdown().expect("shutdown owner");
    }
}
