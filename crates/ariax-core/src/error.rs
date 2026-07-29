use std::error::Error;
use std::fmt;

/// Stable error categories shared by CLI, RPC, the native API, and persistence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ErrorKind {
    Config = 1,
    UnsupportedOption = 2,
    OptionPatchRejected = 3,
    InvalidGid = 4,
    GidAmbiguous = 5,
    GidNotFound = 6,
    GidCollision = 7,
    InvalidPath = 8,
    PathEscape = 9,
    Network = 10,
    Timeout = 11,
    Retryable = 12,
    InvalidRange = 13,
    StaleValidator = 14,
    ChecksumMismatch = 15,
    HostKeyApprovalRequired = 16,
    StaleChallenge = 17,
    Disk = 18,
    NoSpace = 19,
    Permission = 20,
    JournalCorrupt = 21,
    DirtyCheckpoint = 22,
    NeedsCredentials = 23,
    SlowConsumer = 24,
    ResponseTooLarge = 25,
    ResourceLimit = 26,
    BackendUnavailable = 27,
    Cancelled = 28,
    InternalInvariant = 29,
}

/// Every stable error kind in canonical matrix order.
pub const ALL_ERROR_KINDS: &[ErrorKind] = &[
    ErrorKind::Config,
    ErrorKind::UnsupportedOption,
    ErrorKind::OptionPatchRejected,
    ErrorKind::InvalidGid,
    ErrorKind::GidAmbiguous,
    ErrorKind::GidNotFound,
    ErrorKind::GidCollision,
    ErrorKind::InvalidPath,
    ErrorKind::PathEscape,
    ErrorKind::Network,
    ErrorKind::Timeout,
    ErrorKind::Retryable,
    ErrorKind::InvalidRange,
    ErrorKind::StaleValidator,
    ErrorKind::ChecksumMismatch,
    ErrorKind::HostKeyApprovalRequired,
    ErrorKind::StaleChallenge,
    ErrorKind::Disk,
    ErrorKind::NoSpace,
    ErrorKind::Permission,
    ErrorKind::JournalCorrupt,
    ErrorKind::DirtyCheckpoint,
    ErrorKind::NeedsCredentials,
    ErrorKind::SlowConsumer,
    ErrorKind::ResponseTooLarge,
    ErrorKind::ResourceLimit,
    ErrorKind::BackendUnavailable,
    ErrorKind::Cancelled,
    ErrorKind::InternalInvariant,
];

impl ErrorKind {
    /// Returns the stable version-1 journal numeric value.
    #[must_use]
    pub const fn number(self) -> u8 {
        self as u8
    }

    /// Returns the stable transport-independent error code name.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Config => "Config",
            Self::UnsupportedOption => "UnsupportedOption",
            Self::OptionPatchRejected => "OptionPatchRejected",
            Self::InvalidGid => "InvalidGid",
            Self::GidAmbiguous => "GidAmbiguous",
            Self::GidNotFound => "GidNotFound",
            Self::GidCollision => "GidCollision",
            Self::InvalidPath => "InvalidPath",
            Self::PathEscape => "PathEscape",
            Self::Network => "Network",
            Self::Timeout => "Timeout",
            Self::Retryable => "Retryable",
            Self::InvalidRange => "InvalidRange",
            Self::StaleValidator => "StaleValidator",
            Self::ChecksumMismatch => "ChecksumMismatch",
            Self::HostKeyApprovalRequired => "HostKeyApprovalRequired",
            Self::StaleChallenge => "StaleChallenge",
            Self::Disk => "Disk",
            Self::NoSpace => "NoSpace",
            Self::Permission => "Permission",
            Self::JournalCorrupt => "JournalCorrupt",
            Self::DirtyCheckpoint => "DirtyCheckpoint",
            Self::NeedsCredentials => "NeedsCredentials",
            Self::SlowConsumer => "SlowConsumer",
            Self::ResponseTooLarge => "ResponseTooLarge",
            Self::ResourceLimit => "ResourceLimit",
            Self::BackendUnavailable => "BackendUnavailable",
            Self::Cancelled => "Cancelled",
            Self::InternalInvariant => "InternalInvariant",
        }
    }
}

impl TryFrom<u8> for ErrorKind {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        ALL_ERROR_KINDS
            .iter()
            .copied()
            .find(|kind| kind.number() == value)
            .ok_or(())
    }
}

/// Retry guidance attached to a safe public error.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RetryClass {
    Never,
    SameSource,
    AnotherSource,
    RestartGeneration,
    UserAction,
}

/// One safe, redacted error suitable for a snapshot or API response.
pub const MAX_PUBLIC_ERROR_MESSAGE_BYTES: usize = 4096;

/// One safe, redacted error suitable for a snapshot or API response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicError {
    kind: ErrorKind,
    safe_message: String,
    diagnostic_id: Option<u64>,
    retry: RetryClass,
}

impl PublicError {
    /// Creates a public error. Callers must supply an already-redacted message.
    #[must_use]
    pub fn new(kind: ErrorKind, safe_message: impl Into<String>, retry: RetryClass) -> Self {
        let mut safe_message = safe_message.into();
        if safe_message.len() > MAX_PUBLIC_ERROR_MESSAGE_BYTES {
            let mut end = MAX_PUBLIC_ERROR_MESSAGE_BYTES;
            while !safe_message.is_char_boundary(end) {
                end -= 1;
            }
            safe_message.truncate(end);
        }
        Self {
            kind,
            safe_message,
            diagnostic_id: None,
            retry,
        }
    }

    /// Attaches a non-secret internal diagnostic correlation identifier.
    #[must_use]
    pub const fn with_diagnostic_id(mut self, diagnostic_id: u64) -> Self {
        self.diagnostic_id = Some(diagnostic_id);
        self
    }

    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    #[must_use]
    pub fn safe_message(&self) -> &str {
        &self.safe_message
    }

    #[must_use]
    pub const fn diagnostic_id(&self) -> Option<u64> {
        self.diagnostic_id
    }

    #[must_use]
    pub const fn retry_class(&self) -> RetryClass {
        self.retry
    }
}

impl fmt::Display for PublicError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.kind.code(), self.safe_message)
    }
}

impl Error for PublicError {}

/// Stable reasons inside the single public `OptionPatchRejected` vocabulary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OptionPatchRejectReason {
    InvalidValue,
    Unsupported,
    NotRuntimeMutable,
    RequiresNewGeneration,
    RequiresExplicitBtRestart,
    UnsafeCompatRequired,
}

/// Every option-patch rejection reason in canonical matrix order.
pub const ALL_OPTION_PATCH_REJECT_REASONS: &[OptionPatchRejectReason] = &[
    OptionPatchRejectReason::InvalidValue,
    OptionPatchRejectReason::Unsupported,
    OptionPatchRejectReason::NotRuntimeMutable,
    OptionPatchRejectReason::RequiresNewGeneration,
    OptionPatchRejectReason::RequiresExplicitBtRestart,
    OptionPatchRejectReason::UnsafeCompatRequired,
];

impl OptionPatchRejectReason {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidValue => "invalid_value",
            Self::Unsupported => "unsupported",
            Self::NotRuntimeMutable => "not_runtime_mutable",
            Self::RequiresNewGeneration => "requires_new_generation",
            Self::RequiresExplicitBtRestart => "requires_explicit_bt_restart",
            Self::UnsafeCompatRequired => "unsafe_compat_required",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ALL_ERROR_KINDS, ALL_OPTION_PATCH_REJECT_REASONS, ErrorKind,
        MAX_PUBLIC_ERROR_MESSAGE_BYTES, OptionPatchRejectReason, PublicError, RetryClass,
    };
    use std::collections::BTreeSet;

    #[test]
    fn stable_error_codes_are_unique_and_complete() {
        let codes: BTreeSet<_> = ALL_ERROR_KINDS.iter().map(|kind| kind.code()).collect();
        let numbers: BTreeSet<_> = ALL_ERROR_KINDS.iter().map(|kind| kind.number()).collect();
        assert_eq!(codes.len(), ALL_ERROR_KINDS.len());
        assert_eq!(numbers.len(), ALL_ERROR_KINDS.len());
        for (index, kind) in ALL_ERROR_KINDS.iter().copied().enumerate() {
            assert_eq!(kind.number(), index as u8 + 1);
            assert_eq!(ErrorKind::try_from(kind.number()), Ok(kind));
        }
        assert!(ErrorKind::try_from(0).is_err());
        assert!(ErrorKind::try_from(30).is_err());
        assert!(codes.contains(ErrorKind::JournalCorrupt.code()));
        assert!(codes.contains(ErrorKind::InternalInvariant.code()));
    }

    #[test]
    fn public_error_exposes_only_safe_fields() {
        let error = PublicError::new(
            ErrorKind::Timeout,
            "request timed out",
            RetryClass::SameSource,
        )
        .with_diagnostic_id(42);
        assert_eq!(error.kind(), ErrorKind::Timeout);
        assert_eq!(error.safe_message(), "request timed out");
        assert_eq!(error.diagnostic_id(), Some(42));
        assert_eq!(error.retry_class(), RetryClass::SameSource);
        assert_eq!(error.to_string(), "Timeout: request timed out");
    }

    #[test]
    fn public_error_truncates_at_the_storage_utf8_boundary() {
        let error = PublicError::new(
            ErrorKind::InternalInvariant,
            format!("{}é", "x".repeat(MAX_PUBLIC_ERROR_MESSAGE_BYTES - 1)),
            RetryClass::Never,
        );
        assert_eq!(
            error.safe_message().len(),
            MAX_PUBLIC_ERROR_MESSAGE_BYTES - 1
        );
        assert!(
            error
                .safe_message()
                .is_char_boundary(error.safe_message().len())
        );
    }

    #[test]
    fn option_patch_rejection_codes_match_the_contract() {
        let codes: BTreeSet<_> = ALL_OPTION_PATCH_REJECT_REASONS
            .iter()
            .map(|reason| reason.code())
            .collect();
        assert_eq!(codes.len(), ALL_OPTION_PATCH_REJECT_REASONS.len());
        assert_eq!(
            OptionPatchRejectReason::RequiresExplicitBtRestart.code(),
            "requires_explicit_bt_restart"
        );
        assert_eq!(
            OptionPatchRejectReason::UnsafeCompatRequired.code(),
            "unsafe_compat_required"
        );
    }
}
