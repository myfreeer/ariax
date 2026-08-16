use std::error::Error;
use std::fmt;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// One logical process-handle accounting domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandleDomain {
    Socket,
    File,
}

impl HandleDomain {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Socket => "socket",
            Self::File => "file",
        }
    }
}

/// Exact shared and per-domain handle limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HandleBudgetLimits {
    pub process: usize,
    pub sockets: usize,
    pub files: usize,
}

/// Process-owned handle admission. Socket and file permits each consume their
/// domain subcap and the same process-wide cap.
#[derive(Clone, Debug)]
pub struct HandleBudgets {
    limits: HandleBudgetLimits,
    process: Arc<Semaphore>,
    sockets: Arc<Semaphore>,
    files: Arc<Semaphore>,
}

impl HandleBudgets {
    pub fn new(limits: HandleBudgetLimits) -> Result<Self, HandleBudgetError> {
        if limits.process == 0
            || limits.sockets == 0
            || limits.files == 0
            || limits.sockets > limits.process
            || limits.files > limits.process
        {
            return Err(HandleBudgetError::InvalidLimits);
        }
        Ok(Self {
            limits,
            process: Arc::new(Semaphore::new(limits.process)),
            sockets: Arc::new(Semaphore::new(limits.sockets)),
            files: Arc::new(Semaphore::new(limits.files)),
        })
    }

    #[must_use]
    pub const fn limits(&self) -> HandleBudgetLimits {
        self.limits
    }

    #[must_use]
    pub fn available_process(&self) -> usize {
        self.process.available_permits()
    }

    #[must_use]
    pub fn available_sockets(&self) -> usize {
        self.sockets.available_permits()
    }

    #[must_use]
    pub fn available_files(&self) -> usize {
        self.files.available_permits()
    }

    pub fn try_acquire_socket(&self) -> Result<HandlePermit, HandleBudgetError> {
        self.try_acquire(HandleDomain::Socket)
    }

    pub fn try_acquire_file(&self) -> Result<HandlePermit, HandleBudgetError> {
        self.try_acquire(HandleDomain::File)
    }

    fn try_acquire(&self, domain: HandleDomain) -> Result<HandlePermit, HandleBudgetError> {
        let process =
            self.process
                .clone()
                .try_acquire_owned()
                .map_err(|_| HandleBudgetError::Exhausted {
                    domain,
                    process_available: self.available_process(),
                    domain_available: match domain {
                        HandleDomain::Socket => self.available_sockets(),
                        HandleDomain::File => self.available_files(),
                    },
                })?;
        let domain_permit = match domain {
            HandleDomain::Socket => self.sockets.clone().try_acquire_owned(),
            HandleDomain::File => self.files.clone().try_acquire_owned(),
        }
        .map_err(|_| HandleBudgetError::Exhausted {
            domain,
            process_available: self.available_process().saturating_add(1),
            domain_available: 0,
        })?;
        Ok(HandlePermit {
            domain,
            _process: process,
            _domain: domain_permit,
        })
    }
}

/// Move-only authority for one admitted native handle.
pub struct HandlePermit {
    domain: HandleDomain,
    _process: OwnedSemaphorePermit,
    _domain: OwnedSemaphorePermit,
}

impl HandlePermit {
    #[must_use]
    pub const fn domain(&self) -> HandleDomain {
        self.domain
    }
}

impl fmt::Debug for HandlePermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HandlePermit")
            .field("domain", &self.domain)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandleBudgetError {
    InvalidLimits,
    Exhausted {
        domain: HandleDomain,
        process_available: usize,
        domain_available: usize,
    },
}

impl fmt::Display for HandleBudgetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits => formatter.write_str("invalid process handle limits"),
            Self::Exhausted {
                domain,
                process_available,
                domain_available,
            } => write!(
                formatter,
                "{} handle budget exhausted: process available {process_available}, domain available {domain_available}",
                domain.code()
            ),
        }
    }
}

impl Error for HandleBudgetError {}

#[cfg(test)]
mod tests {
    use super::{HandleBudgetError, HandleBudgetLimits, HandleBudgets, HandleDomain};

    #[test]
    fn shared_process_cap_applies_across_socket_and_file_domains() {
        let budgets = HandleBudgets::new(HandleBudgetLimits {
            process: 2,
            sockets: 2,
            files: 2,
        })
        .expect("budgets");
        let socket = budgets.try_acquire_socket().expect("socket");
        let file = budgets.try_acquire_file().expect("file");
        assert_eq!(socket.domain(), HandleDomain::Socket);
        assert_eq!(file.domain(), HandleDomain::File);
        assert!(matches!(
            budgets.try_acquire_socket(),
            Err(HandleBudgetError::Exhausted {
                domain: HandleDomain::Socket,
                process_available: 0,
                ..
            })
        ));
        drop(socket);
        assert!(budgets.try_acquire_socket().is_ok());
    }

    #[test]
    fn domain_subcaps_reject_without_leaking_process_capacity() {
        let budgets = HandleBudgets::new(HandleBudgetLimits {
            process: 3,
            sockets: 1,
            files: 2,
        })
        .expect("budgets");
        let socket = budgets.try_acquire_socket().expect("socket");
        assert!(matches!(
            budgets.try_acquire_socket(),
            Err(HandleBudgetError::Exhausted {
                domain: HandleDomain::Socket,
                domain_available: 0,
                ..
            })
        ));
        assert_eq!(budgets.available_process(), 2);
        let first_file = budgets.try_acquire_file().expect("first file");
        let second_file = budgets.try_acquire_file().expect("second file");
        assert_eq!(budgets.available_process(), 0);
        drop((socket, first_file, second_file));
        assert_eq!(budgets.available_process(), 3);
    }

    #[test]
    fn invalid_zero_and_overwide_subcaps_are_rejected() {
        for limits in [
            HandleBudgetLimits {
                process: 0,
                sockets: 1,
                files: 1,
            },
            HandleBudgetLimits {
                process: 1,
                sockets: 2,
                files: 1,
            },
            HandleBudgetLimits {
                process: 1,
                sockets: 1,
                files: 0,
            },
        ] {
            assert!(matches!(
                HandleBudgets::new(limits),
                Err(HandleBudgetError::InvalidLimits)
            ));
        }
    }
}
