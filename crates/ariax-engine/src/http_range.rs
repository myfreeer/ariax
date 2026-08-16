//! Deterministic, bounded coordination for parallel HTTP range workers.

use crate::http_task::MAX_HTTP_ENDGAME_MAX_DUPLICATES;
use ariax_core::{LeaseId, OverlapGroupId, PieceId, UriId};
use ariax_storage::GlobalSpan;
use hyper::Uri;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

pub const MAX_HTTP_RANGE_PIECES: usize = 1_048_576;
pub const DEFAULT_HTTP_MAX_TOTAL_ATTEMPTS: u32 = 5;
pub const DEFAULT_HTTP_MAX_ATTEMPTS_PER_SOURCE: u32 = 3;
pub const HTTP_ENDGAME_MIN_TAIL_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpRangeCoordinatorConfig {
    pub total_length: u64,
    pub piece_length: u64,
    pub split: NonZeroUsize,
    pub max_connections_per_origin: NonZeroUsize,
    pub max_total_attempts: u32,
    pub max_attempts_per_source: u32,
    pub endgame_max_duplicates: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRangeSource {
    id: UriId,
    origin: Arc<str>,
    ordinary_assignments: bool,
    same_source_endgame: bool,
    shared_identity_endgame: bool,
}

impl HttpRangeSource {
    pub fn from_uri(id: UriId, uri_text: &str) -> Result<Self, HttpRangeCoordinatorError> {
        let uri: Uri = uri_text
            .parse()
            .map_err(|_| HttpRangeCoordinatorError::InvalidSource)?;
        let scheme = uri
            .scheme_str()
            .filter(|scheme| matches!(*scheme, "http" | "https"))
            .ok_or(HttpRangeCoordinatorError::InvalidSource)?;
        let authority = uri
            .authority()
            .ok_or(HttpRangeCoordinatorError::InvalidSource)?;
        if authority.as_str().contains('@') {
            return Err(HttpRangeCoordinatorError::InvalidSource);
        }
        let host = authority.host();
        if host.is_empty() {
            return Err(HttpRangeCoordinatorError::InvalidSource);
        }
        let port = authority
            .port_u16()
            .unwrap_or(if scheme == "https" { 443 } else { 80 });
        if port == 0 {
            return Err(HttpRangeCoordinatorError::InvalidSource);
        }
        let host = host.to_ascii_lowercase();
        let origin = if host.contains(':') {
            format!("{scheme}://[{host}]:{port}")
        } else {
            format!("{scheme}://{host}:{port}")
        };
        Ok(Self {
            id,
            origin: origin.into(),
            ordinary_assignments: true,
            same_source_endgame: false,
            shared_identity_endgame: false,
        })
    }

    /// Controls whether this source may receive ordinary non-overlapping
    /// pieces. A strict range-digest mirror can be retained as an endgame-only
    /// peer without widening ordinary multi-mirror trust.
    #[must_use]
    pub const fn with_ordinary_assignments(mut self, allowed: bool) -> Self {
        self.ordinary_assignments = allowed;
        self
    }

    /// Marks this source as safe for one same-source, same-validator endgame
    /// duplicate. The HTTP worker enables this only for a strong ETag-pinned
    /// source; the coordinator never authorizes a cross-source race.
    #[must_use]
    pub const fn with_same_source_endgame(mut self, allowed: bool) -> Self {
        self.same_source_endgame = allowed;
        self
    }

    /// Marks this source as eligible to race an active lease from another
    /// origin after the bounded range-digest probe gate. The worker, not the
    /// coordinator, compares the exact response digests before opening the
    /// duplicate and verifies each response body before commit.
    #[must_use]
    pub const fn with_shared_identity_endgame(mut self, allowed: bool) -> Self {
        self.shared_identity_endgame = allowed;
        self
    }

    #[must_use]
    pub const fn id(&self) -> UriId {
        self.id
    }

    #[must_use]
    pub fn origin(&self) -> &str {
        &self.origin
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpRangeAssignment {
    pub lease: LeaseId,
    pub source: UriId,
    pub piece: PieceId,
    pub span: GlobalSpan,
    pub overlap_group: Option<OverlapGroupId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRangePoll {
    Assignment(HttpRangeAssignment),
    Endgame {
        assignment: HttpRangeAssignment,
        original: LeaseId,
    },
    RetryAt(u64),
    Saturated,
    Complete,
    Exhausted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpRangeFailure {
    RetryAt(u64),
    DisableSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpOverlapFence {
    pub group: OverlapGroupId,
    pub candidate: LeaseId,
    pub competitor: LeaseId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpOverlapSettlement {
    CandidateCommitted,
    RolledBack,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpRangeStats {
    pub total_length: u64,
    pub completed_length: u64,
    pub active_connections: usize,
    pub active_endgame_duplicates: usize,
    pub completed_pieces: usize,
    pub total_pieces: usize,
    pub retry_count: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PieceState {
    Pending,
    Active(ActivePiece),
    Durable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActivePiece {
    original: LeaseId,
    duplicate: Option<LeaseId>,
    overlap_group: Option<OverlapGroupId>,
    candidate: Option<LeaseId>,
}

#[derive(Clone, Debug)]
struct SourceState {
    source: HttpRangeSource,
    disabled: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActiveRange {
    source_index: usize,
    piece_index: usize,
    overlap_group: Option<OverlapGroupId>,
    duplicate: bool,
}

#[derive(Debug)]
pub enum HttpRangeCoordinatorError {
    InvalidConfig,
    InvalidSource,
    NoSources,
    TooManyPieces,
    DuplicateSource,
    InvalidOverlap,
    IdentifierExhausted,
    UnknownLease,
}

impl HttpRangeCoordinatorError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_range_config",
            Self::InvalidSource => "invalid_range_source",
            Self::NoSources => "no_range_sources",
            Self::TooManyPieces => "too_many_range_pieces",
            Self::DuplicateSource => "duplicate_range_source",
            Self::InvalidOverlap => "invalid_range_overlap",
            Self::IdentifierExhausted => "range_identifier_exhausted",
            Self::UnknownLease => "unknown_range_lease",
        }
    }
}

impl fmt::Display for HttpRangeCoordinatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for HttpRangeCoordinatorError {}

#[derive(Debug)]
pub struct HttpRangeCoordinator {
    config: HttpRangeCoordinatorConfig,
    sources: Vec<SourceState>,
    pieces: Vec<PieceState>,
    piece_attempts: Vec<u32>,
    source_piece_attempts: BTreeMap<(usize, usize), u32>,
    source_piece_retry_at: BTreeMap<(usize, usize), u64>,
    active: BTreeMap<LeaseId, ActiveRange>,
    active_by_origin: BTreeMap<Arc<str>, usize>,
    next_lease: u64,
    next_overlap_group: u64,
    next_piece: usize,
    next_source: usize,
    completed_length: u64,
    completed_pieces: usize,
    retry_count: u32,
    active_endgame_duplicates: usize,
}

impl HttpRangeCoordinator {
    pub fn new(
        config: HttpRangeCoordinatorConfig,
        sources: impl IntoIterator<Item = HttpRangeSource>,
    ) -> Result<Self, HttpRangeCoordinatorError> {
        if config.total_length == 0
            || config.piece_length == 0
            || !config.piece_length.is_power_of_two()
            || config.split.get() > 1024
            || config.max_connections_per_origin.get() > 1024
            || config.max_total_attempts == 0
            || config.max_attempts_per_source == 0
            || config.endgame_max_duplicates > MAX_HTTP_ENDGAME_MAX_DUPLICATES
        {
            return Err(HttpRangeCoordinatorError::InvalidConfig);
        }
        let piece_count = config
            .total_length
            .checked_add(config.piece_length - 1)
            .ok_or(HttpRangeCoordinatorError::TooManyPieces)?
            / config.piece_length;
        let piece_count =
            usize::try_from(piece_count).map_err(|_| HttpRangeCoordinatorError::TooManyPieces)?;
        if piece_count == 0 || piece_count > MAX_HTTP_RANGE_PIECES {
            return Err(HttpRangeCoordinatorError::TooManyPieces);
        }
        let mut source_states = Vec::new();
        let mut seen = BTreeMap::new();
        for source in sources {
            if seen.insert(source.id, ()).is_some() {
                return Err(HttpRangeCoordinatorError::DuplicateSource);
            }
            source_states.push(SourceState {
                source,
                disabled: false,
            });
        }
        if source_states.is_empty() {
            return Err(HttpRangeCoordinatorError::NoSources);
        }
        Ok(Self {
            config,
            sources: source_states,
            pieces: vec![PieceState::Pending; piece_count],
            piece_attempts: vec![0; piece_count],
            source_piece_attempts: BTreeMap::new(),
            source_piece_retry_at: BTreeMap::new(),
            active: BTreeMap::new(),
            active_by_origin: BTreeMap::new(),
            next_lease: 1,
            next_overlap_group: 1,
            next_piece: 0,
            next_source: 0,
            completed_length: 0,
            completed_pieces: 0,
            retry_count: 0,
            active_endgame_duplicates: 0,
        })
    }

    pub fn poll(&mut self, now_ms: u64) -> Result<HttpRangePoll, HttpRangeCoordinatorError> {
        self.poll_with_endgame(now_ms, &[])
    }

    /// Polls ordinary work first, then admits at most one same-source
    /// duplicate for an explicitly slow original lease. The caller supplies
    /// the slow/near-deadline lease ids; the coordinator still enforces the
    /// tail threshold, source validator capability, attempt caps, and the
    /// task-wide duplicate budget.
    pub fn poll_with_endgame(
        &mut self,
        now_ms: u64,
        eligible_originals: &[LeaseId],
    ) -> Result<HttpRangePoll, HttpRangeCoordinatorError> {
        if self.completed_pieces == self.pieces.len() {
            return Ok(HttpRangePoll::Complete);
        }
        if self.active.len() < self.config.split.get()
            && let Some((piece_index, source_index)) = self.find_assignment(now_ms)
        {
            return self.issue_normal_assignment(piece_index, source_index);
        }
        if let Some((original, piece_index, source_index)) =
            self.find_endgame_assignment(now_ms, eligible_originals)
        {
            return self.issue_endgame_assignment(original, piece_index, source_index);
        }
        if !self.active.is_empty() {
            return Ok(HttpRangePoll::Saturated);
        }
        Ok(self
            .next_retry_deadline(now_ms)
            .map_or(HttpRangePoll::Exhausted, HttpRangePoll::RetryAt))
    }

    fn issue_normal_assignment(
        &mut self,
        piece_index: usize,
        source_index: usize,
    ) -> Result<HttpRangePoll, HttpRangeCoordinatorError> {
        let lease = self.next_lease_id()?;
        let span = self.piece_span_global(piece_index)?;
        let source_id = self.sources[source_index].source.id;
        let source_origin = Arc::clone(&self.sources[source_index].source.origin);
        self.record_attempt(piece_index, source_index, false);
        *self.active_by_origin.entry(source_origin).or_default() += 1;
        self.pieces[piece_index] = PieceState::Active(ActivePiece {
            original: lease,
            duplicate: None,
            overlap_group: None,
            candidate: None,
        });
        self.active.insert(
            lease,
            ActiveRange {
                source_index,
                piece_index,
                overlap_group: None,
                duplicate: false,
            },
        );
        self.advance_cursor(piece_index, source_index);
        Ok(HttpRangePoll::Assignment(HttpRangeAssignment {
            lease,
            source: source_id,
            piece: PieceId::new(u64::try_from(piece_index).expect("piece index fits u64")),
            span,
            overlap_group: None,
        }))
    }

    fn issue_endgame_assignment(
        &mut self,
        original: LeaseId,
        piece_index: usize,
        source_index: usize,
    ) -> Result<HttpRangePoll, HttpRangeCoordinatorError> {
        let group = OverlapGroupId::new(self.next_overlap_group)
            .ok_or(HttpRangeCoordinatorError::IdentifierExhausted)?;
        self.next_overlap_group = self
            .next_overlap_group
            .checked_add(1)
            .ok_or(HttpRangeCoordinatorError::IdentifierExhausted)?;
        let lease = self.next_lease_id()?;
        let span = self.piece_span_global(piece_index)?;
        let source_id = self.sources[source_index].source.id;
        let source_origin = Arc::clone(&self.sources[source_index].source.origin);
        self.record_attempt(piece_index, source_index, true);
        *self.active_by_origin.entry(source_origin).or_default() += 1;
        let PieceState::Active(mut state) = self.pieces[piece_index] else {
            return Err(HttpRangeCoordinatorError::InvalidOverlap);
        };
        if state.original != original
            || state.duplicate.is_some()
            || state.overlap_group.is_some()
            || state.candidate.is_some()
        {
            return Err(HttpRangeCoordinatorError::InvalidOverlap);
        }
        state.duplicate = Some(lease);
        state.overlap_group = Some(group);
        self.pieces[piece_index] = PieceState::Active(state);
        if let Some(active) = self.active.get_mut(&original) {
            active.overlap_group = Some(group);
        } else {
            return Err(HttpRangeCoordinatorError::UnknownLease);
        }
        self.active.insert(
            lease,
            ActiveRange {
                source_index,
                piece_index,
                overlap_group: Some(group),
                duplicate: true,
            },
        );
        self.active_endgame_duplicates += 1;
        self.advance_cursor(piece_index, source_index);
        Ok(HttpRangePoll::Endgame {
            assignment: HttpRangeAssignment {
                lease,
                source: source_id,
                piece: PieceId::new(u64::try_from(piece_index).expect("piece index fits u64")),
                span,
                overlap_group: Some(group),
            },
            original,
        })
    }

    fn next_lease_id(&mut self) -> Result<LeaseId, HttpRangeCoordinatorError> {
        let lease =
            LeaseId::new(self.next_lease).ok_or(HttpRangeCoordinatorError::IdentifierExhausted)?;
        self.next_lease = self
            .next_lease
            .checked_add(1)
            .ok_or(HttpRangeCoordinatorError::IdentifierExhausted)?;
        Ok(lease)
    }

    fn piece_span_global(
        &self,
        piece_index: usize,
    ) -> Result<GlobalSpan, HttpRangeCoordinatorError> {
        let offset = u64::try_from(piece_index)
            .ok()
            .and_then(|piece| piece.checked_mul(self.config.piece_length))
            .ok_or(HttpRangeCoordinatorError::InvalidConfig)?;
        let length = self
            .config
            .piece_length
            .min(self.config.total_length - offset);
        Ok(GlobalSpan {
            offset,
            len: usize::try_from(length).map_err(|_| HttpRangeCoordinatorError::InvalidConfig)?,
        })
    }

    fn record_attempt(&mut self, piece_index: usize, source_index: usize, duplicate: bool) {
        self.piece_attempts[piece_index] += 1;
        *self
            .source_piece_attempts
            .entry((piece_index, source_index))
            .or_default() += 1;
        if !duplicate && self.piece_attempts[piece_index] > 1 {
            self.retry_count = self.retry_count.saturating_add(1);
        }
    }

    fn advance_cursor(&mut self, piece_index: usize, source_index: usize) {
        self.next_piece = (piece_index + 1) % self.pieces.len();
        self.next_source = (source_index + 1) % self.sources.len();
    }

    /// Restores scheduler-independent durable pieces after journal recovery.
    /// Duplicate or out-of-layout identifiers are rejected before any new
    /// range can be leased.
    pub fn restore_durable(
        &mut self,
        pieces: impl IntoIterator<Item = PieceId>,
    ) -> Result<(), HttpRangeCoordinatorError> {
        for piece in pieces {
            let index = usize::try_from(piece.get())
                .map_err(|_| HttpRangeCoordinatorError::InvalidConfig)?;
            let state = self
                .pieces
                .get_mut(index)
                .ok_or(HttpRangeCoordinatorError::InvalidConfig)?;
            if *state != PieceState::Pending {
                return Err(HttpRangeCoordinatorError::InvalidConfig);
            }
            *state = PieceState::Durable;
            self.completed_pieces += 1;
            self.completed_length = self.completed_length.saturating_add(
                self.piece_length(index)
                    .ok_or(HttpRangeCoordinatorError::InvalidConfig)?,
            );
        }
        Ok(())
    }

    /// Restores the completed number of attempts for a pending piece before
    /// replayed retry work is admitted. The stored value includes the initial
    /// attempt and is therefore never zero.
    pub fn restore_piece_attempts(
        &mut self,
        piece: PieceId,
        attempts: u32,
    ) -> Result<(), HttpRangeCoordinatorError> {
        let index =
            usize::try_from(piece.get()).map_err(|_| HttpRangeCoordinatorError::InvalidConfig)?;
        if attempts == 0
            || attempts > self.config.max_total_attempts
            || self.pieces.get(index) != Some(&PieceState::Pending)
            || self.piece_attempts.get(index).copied() != Some(0)
        {
            return Err(HttpRangeCoordinatorError::InvalidConfig);
        }
        self.piece_attempts[index] = attempts;
        self.retry_count = self.retry_count.saturating_add(attempts.saturating_sub(1));
        Ok(())
    }

    /// Restores one source-specific retry delay and accounting row. Delays are
    /// relative to the new process monotonic origin and may be zero when the
    /// persisted wait has already elapsed.
    pub fn restore_source_piece_retry(
        &mut self,
        piece: PieceId,
        source: UriId,
        attempts: u32,
        retry_at_ms: u64,
    ) -> Result<(), HttpRangeCoordinatorError> {
        let piece_index =
            usize::try_from(piece.get()).map_err(|_| HttpRangeCoordinatorError::InvalidConfig)?;
        let source_index = self
            .sources
            .iter()
            .position(|candidate| candidate.source.id() == source)
            .ok_or(HttpRangeCoordinatorError::InvalidSource)?;
        let key = (piece_index, source_index);
        if attempts == 0
            || attempts > self.config.max_attempts_per_source
            || self.pieces.get(piece_index) != Some(&PieceState::Pending)
            || self.piece_attempts.get(piece_index).copied().unwrap_or(0) < attempts
            || self.source_piece_attempts.contains_key(&key)
        {
            return Err(HttpRangeCoordinatorError::InvalidConfig);
        }
        self.source_piece_attempts.insert(key, attempts);
        self.source_piece_retry_at.insert(key, retry_at_ms);
        Ok(())
    }

    pub fn complete(&mut self, lease: LeaseId) -> Result<(), HttpRangeCoordinatorError> {
        let active = self
            .active
            .get(&lease)
            .copied()
            .ok_or(HttpRangeCoordinatorError::UnknownLease)?;
        let PieceState::Active(state) = self.pieces[active.piece_index] else {
            return Err(HttpRangeCoordinatorError::UnknownLease);
        };
        if state.original != lease
            || state.duplicate.is_some()
            || state.overlap_group.is_some()
            || state.candidate.is_some()
        {
            return Err(HttpRangeCoordinatorError::UnknownLease);
        }
        self.release_active(lease)?;
        self.mark_piece_durable(active.piece_index);
        Ok(())
    }

    /// Freezes one two-member overlap group after storage accepts the first
    /// exact-length commit candidate. The returned competitor is the only
    /// network attempt that may still need cancellation confirmation.
    pub fn begin_overlap_commit(
        &mut self,
        lease: LeaseId,
    ) -> Result<HttpOverlapFence, HttpRangeCoordinatorError> {
        let active = self
            .active
            .get(&lease)
            .copied()
            .ok_or(HttpRangeCoordinatorError::UnknownLease)?;
        let group = active
            .overlap_group
            .ok_or(HttpRangeCoordinatorError::InvalidOverlap)?;
        let PieceState::Active(mut state) = self.pieces[active.piece_index] else {
            return Err(HttpRangeCoordinatorError::InvalidOverlap);
        };
        if state.overlap_group != Some(group) || state.candidate.is_some() {
            return Err(HttpRangeCoordinatorError::InvalidOverlap);
        }
        let competitor = if state.original == lease {
            state
                .duplicate
                .ok_or(HttpRangeCoordinatorError::InvalidOverlap)?
        } else if state.duplicate == Some(lease) {
            state.original
        } else {
            return Err(HttpRangeCoordinatorError::InvalidOverlap);
        };
        state.candidate = Some(lease);
        self.pieces[active.piece_index] = PieceState::Active(state);
        Ok(HttpOverlapFence {
            group,
            candidate: lease,
            competitor,
        })
    }

    /// Applies storage's fully fenced overlap result. Both active lease slots
    /// are released together so a rolled-back span cannot be observed as
    /// durable between member completions.
    pub fn settle_overlap(
        &mut self,
        group: OverlapGroupId,
        candidate: LeaseId,
        settlement: HttpOverlapSettlement,
    ) -> Result<(), HttpRangeCoordinatorError> {
        let candidate_active = self
            .active
            .get(&candidate)
            .copied()
            .ok_or(HttpRangeCoordinatorError::UnknownLease)?;
        let PieceState::Active(state) = self.pieces[candidate_active.piece_index] else {
            return Err(HttpRangeCoordinatorError::InvalidOverlap);
        };
        if state.overlap_group != Some(group) || state.candidate != Some(candidate) {
            return Err(HttpRangeCoordinatorError::InvalidOverlap);
        }
        let competitor = if state.original == candidate {
            state
                .duplicate
                .ok_or(HttpRangeCoordinatorError::InvalidOverlap)?
        } else if state.duplicate == Some(candidate) {
            state.original
        } else {
            return Err(HttpRangeCoordinatorError::InvalidOverlap);
        };
        let competitor_active = self
            .active
            .get(&competitor)
            .copied()
            .ok_or(HttpRangeCoordinatorError::UnknownLease)?;
        if competitor_active.piece_index != candidate_active.piece_index
            || competitor_active.overlap_group != Some(group)
        {
            return Err(HttpRangeCoordinatorError::InvalidOverlap);
        }
        self.release_active(candidate)?;
        self.release_active(competitor)?;
        match settlement {
            HttpOverlapSettlement::CandidateCommitted => {
                self.mark_piece_durable(candidate_active.piece_index)
            }
            HttpOverlapSettlement::RolledBack => {
                self.pieces[candidate_active.piece_index] = PieceState::Pending;
            }
        }
        Ok(())
    }

    /// Rolls back a group before a commit candidate exists, such as when one
    /// duplicate fails after already writing provisional bytes. Both leases
    /// are removed atomically from coordinator visibility and the piece
    /// becomes ordinary pending work again.
    pub fn rollback_overlap(
        &mut self,
        group: OverlapGroupId,
    ) -> Result<[LeaseId; 2], HttpRangeCoordinatorError> {
        let (piece_index, state) = self
            .pieces
            .iter()
            .copied()
            .enumerate()
            .find_map(|(piece, state)| match state {
                PieceState::Active(active) if active.overlap_group == Some(group) => {
                    Some((piece, active))
                }
                _ => None,
            })
            .ok_or(HttpRangeCoordinatorError::InvalidOverlap)?;
        let duplicate = state
            .duplicate
            .ok_or(HttpRangeCoordinatorError::InvalidOverlap)?;
        self.release_active(state.original)?;
        self.release_active(duplicate)?;
        self.pieces[piece_index] = PieceState::Pending;
        Ok([state.original, duplicate])
    }

    pub fn fail(
        &mut self,
        lease: LeaseId,
        failure: HttpRangeFailure,
    ) -> Result<(), HttpRangeCoordinatorError> {
        let active = self
            .active
            .get(&lease)
            .copied()
            .ok_or(HttpRangeCoordinatorError::UnknownLease)?;
        let PieceState::Active(state) = self.pieces[active.piece_index] else {
            return Err(HttpRangeCoordinatorError::UnknownLease);
        };
        if state.candidate.is_some() {
            return Err(HttpRangeCoordinatorError::InvalidOverlap);
        }
        let owns_piece = state.original == lease || state.duplicate == Some(lease);
        if !owns_piece {
            return Err(HttpRangeCoordinatorError::UnknownLease);
        }
        self.release_active(lease)?;
        if let (Some(group), Some(duplicate)) = (state.overlap_group, state.duplicate) {
            let remaining = if state.original == lease {
                duplicate
            } else {
                state.original
            };
            let remaining_active = self
                .active
                .get_mut(&remaining)
                .ok_or(HttpRangeCoordinatorError::UnknownLease)?;
            if remaining_active.overlap_group != Some(group) {
                return Err(HttpRangeCoordinatorError::InvalidOverlap);
            }
            if remaining_active.duplicate {
                remaining_active.duplicate = false;
                self.active_endgame_duplicates = self.active_endgame_duplicates.saturating_sub(1);
            }
            remaining_active.overlap_group = None;
            self.pieces[active.piece_index] = PieceState::Active(ActivePiece {
                original: remaining,
                duplicate: None,
                overlap_group: None,
                candidate: None,
            });
        } else {
            self.pieces[active.piece_index] = PieceState::Pending;
        }
        let source = &mut self.sources[active.source_index];
        match failure {
            HttpRangeFailure::DisableSource => source.disabled = true,
            HttpRangeFailure::RetryAt(retry_at_ms) => {
                let key = (active.piece_index, active.source_index);
                let retry_at = self.source_piece_retry_at.entry(key).or_default();
                *retry_at = (*retry_at).max(retry_at_ms);
            }
        }
        Ok(())
    }

    /// Applies a retry/source decision after a whole overlap group was already
    /// removed and returned to pending. This preserves the same bounded source
    /// accounting without requiring a stale lease id to remain active.
    pub fn record_released_failure(
        &mut self,
        piece: PieceId,
        source: UriId,
        failure: HttpRangeFailure,
    ) -> Result<(), HttpRangeCoordinatorError> {
        let piece_index =
            usize::try_from(piece.get()).map_err(|_| HttpRangeCoordinatorError::InvalidConfig)?;
        if self.pieces.get(piece_index) != Some(&PieceState::Pending) {
            return Err(HttpRangeCoordinatorError::InvalidOverlap);
        }
        let source_index = self
            .sources
            .iter()
            .position(|candidate| candidate.source.id == source)
            .ok_or(HttpRangeCoordinatorError::InvalidSource)?;
        let source = &mut self.sources[source_index];
        match failure {
            HttpRangeFailure::DisableSource => source.disabled = true,
            HttpRangeFailure::RetryAt(retry_at_ms) => {
                let key = (piece_index, source_index);
                let retry_at = self.source_piece_retry_at.entry(key).or_default();
                *retry_at = (*retry_at).max(retry_at_ms);
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn stats(&self) -> HttpRangeStats {
        HttpRangeStats {
            total_length: self.config.total_length,
            completed_length: self.completed_length,
            active_connections: self.active.len(),
            active_endgame_duplicates: self.active_endgame_duplicates,
            completed_pieces: self.completed_pieces,
            total_pieces: self.pieces.len(),
            retry_count: self.retry_count,
        }
    }

    fn mark_piece_durable(&mut self, piece_index: usize) {
        self.pieces[piece_index] = PieceState::Durable;
        self.source_piece_attempts
            .retain(|(piece, _), _| *piece != piece_index);
        self.source_piece_retry_at
            .retain(|(piece, _), _| *piece != piece_index);
        self.completed_pieces += 1;
        self.completed_length = self.completed_length.saturating_add(
            self.piece_length(piece_index)
                .expect("active piece has a valid length"),
        );
    }

    fn find_assignment(&self, now_ms: u64) -> Option<(usize, usize)> {
        (0..self.pieces.len())
            .map(|offset| (self.next_piece + offset) % self.pieces.len())
            .filter(|piece| {
                self.pieces[*piece] == PieceState::Pending
                    && self.piece_attempts[*piece] < self.config.max_total_attempts
            })
            .find_map(|piece| {
                (0..self.sources.len())
                    .map(|offset| (self.next_source + offset) % self.sources.len())
                    .find(|source_index| {
                        self.sources[*source_index].source.ordinary_assignments
                            && self.source_available(piece, *source_index, now_ms)
                    })
                    .map(|source| (piece, source))
            })
    }

    fn find_endgame_assignment(
        &self,
        now_ms: u64,
        eligible_originals: &[LeaseId],
    ) -> Option<(LeaseId, usize, usize)> {
        if self.config.endgame_max_duplicates == 0
            || self.active_endgame_duplicates >= self.config.endgame_max_duplicates
            || self.active.len()
                >= self
                    .config
                    .split
                    .get()
                    .saturating_add(self.config.endgame_max_duplicates)
            || self
                .config
                .total_length
                .saturating_sub(self.completed_length)
                > HTTP_ENDGAME_MIN_TAIL_BYTES.max(self.config.piece_length.saturating_mul(2))
        {
            return None;
        }
        eligible_originals.iter().find_map(|&original| {
            let active = self.active.get(&original)?;
            if active.duplicate || active.overlap_group.is_some() {
                return None;
            }
            let PieceState::Active(state) = self.pieces[active.piece_index] else {
                return None;
            };
            let source = &self.sources[active.source_index].source;
            if state.original != original {
                return None;
            }
            if state.duplicate.is_some()
                || state.overlap_group.is_some()
                || state.candidate.is_some()
                || self.piece_attempts[active.piece_index] >= self.config.max_total_attempts
            {
                return None;
            }
            let shared_source = if source.shared_identity_endgame {
                (1..self.sources.len()).find_map(|offset| {
                    let source_index = (active.source_index + offset) % self.sources.len();
                    (self.sources[source_index].source.shared_identity_endgame
                        && self.source_available(active.piece_index, source_index, now_ms))
                    .then_some(source_index)
                })
            } else {
                None
            };
            let duplicate_source = shared_source.or_else(|| {
                (source.same_source_endgame
                    && self.source_available(active.piece_index, active.source_index, now_ms))
                .then_some(active.source_index)
            })?;
            Some((original, active.piece_index, duplicate_source))
        })
    }

    fn next_retry_deadline(&self, now_ms: u64) -> Option<u64> {
        self.pieces
            .iter()
            .enumerate()
            .filter(|(piece, state)| {
                **state == PieceState::Pending
                    && self.piece_attempts[*piece] < self.config.max_total_attempts
            })
            .flat_map(|(piece, _)| {
                self.sources
                    .iter()
                    .enumerate()
                    .filter(move |(source_index, source)| {
                        let retry_at_ms = self
                            .source_piece_retry_at
                            .get(&(piece, *source_index))
                            .copied()
                            .unwrap_or(0);
                        !source.disabled
                            && retry_at_ms > now_ms
                            && self
                                .source_piece_attempts
                                .get(&(piece, *source_index))
                                .copied()
                                .unwrap_or(0)
                                < self.config.max_attempts_per_source
                    })
                    .filter_map(move |(source_index, _)| {
                        self.source_piece_retry_at
                            .get(&(piece, source_index))
                            .copied()
                    })
            })
            .min()
    }

    fn source_available(&self, piece: usize, source_index: usize, now_ms: u64) -> bool {
        let source = &self.sources[source_index];
        let retry_at_ms = self
            .source_piece_retry_at
            .get(&(piece, source_index))
            .copied()
            .unwrap_or(0);
        !source.disabled
            && retry_at_ms <= now_ms
            && self
                .source_piece_attempts
                .get(&(piece, source_index))
                .copied()
                .unwrap_or(0)
                < self.config.max_attempts_per_source
            && self
                .active_by_origin
                .get(&source.source.origin)
                .copied()
                .unwrap_or(0)
                < self.config.max_connections_per_origin.get()
    }

    fn release_active(&mut self, lease: LeaseId) -> Result<ActiveRange, HttpRangeCoordinatorError> {
        let active = self
            .active
            .remove(&lease)
            .ok_or(HttpRangeCoordinatorError::UnknownLease)?;
        if active.duplicate {
            self.active_endgame_duplicates = self.active_endgame_duplicates.saturating_sub(1);
        }
        let origin = Arc::clone(&self.sources[active.source_index].source.origin);
        let count = self
            .active_by_origin
            .get_mut(&origin)
            .expect("active origin count exists");
        *count -= 1;
        if *count == 0 {
            self.active_by_origin.remove(&origin);
        }
        Ok(active)
    }

    fn piece_length(&self, piece_index: usize) -> Option<u64> {
        let offset = u64::try_from(piece_index)
            .ok()?
            .checked_mul(self.config.piece_length)?;
        Some(
            self.config
                .piece_length
                .min(self.config.total_length.checked_sub(offset)?),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(id: u32, uri: &str) -> HttpRangeSource {
        HttpRangeSource::from_uri(UriId::new(id), uri).expect("source is valid")
    }

    fn endgame_source(id: u32, uri: &str) -> HttpRangeSource {
        source(id, uri).with_same_source_endgame(true)
    }

    fn shared_endgame_source(id: u32, uri: &str) -> HttpRangeSource {
        source(id, uri).with_shared_identity_endgame(true)
    }

    fn digest_endgame_only_source(id: u32, uri: &str) -> HttpRangeSource {
        shared_endgame_source(id, uri).with_ordinary_assignments(false)
    }

    fn config() -> HttpRangeCoordinatorConfig {
        HttpRangeCoordinatorConfig {
            total_length: 10,
            piece_length: 4,
            split: NonZeroUsize::new(3).expect("nonzero"),
            max_connections_per_origin: NonZeroUsize::new(1).expect("nonzero"),
            max_total_attempts: 5,
            max_attempts_per_source: 3,
            endgame_max_duplicates: 0,
        }
    }

    #[test]
    fn schedules_non_overlapping_piece_aligned_ranges_across_origins() {
        let mut coordinator = HttpRangeCoordinator::new(
            config(),
            [
                source(0, "https://one.example/a"),
                source(1, "https://two.example/b"),
                source(2, "https://one.example/c"),
            ],
        )
        .expect("coordinator starts");

        let HttpRangePoll::Assignment(first) = coordinator.poll(0).expect("first poll") else {
            panic!("first assignment expected");
        };
        let HttpRangePoll::Assignment(second) = coordinator.poll(0).expect("second poll") else {
            panic!("second assignment expected");
        };
        assert_eq!(first.span, GlobalSpan { offset: 0, len: 4 });
        assert_eq!(second.span, GlobalSpan { offset: 4, len: 4 });
        assert_ne!(first.source, second.source);
        assert_eq!(
            coordinator.poll(0).expect("origin caps apply"),
            HttpRangePoll::Saturated
        );

        coordinator.complete(first.lease).expect("first completes");
        let HttpRangePoll::Assignment(last) = coordinator.poll(0).expect("last poll") else {
            panic!("last assignment expected");
        };
        assert_eq!(last.span, GlobalSpan { offset: 8, len: 2 });
        coordinator
            .complete(second.lease)
            .expect("second completes");
        coordinator.complete(last.lease).expect("last completes");
        assert_eq!(
            coordinator.poll(0).expect("complete poll"),
            HttpRangePoll::Complete
        );
        assert_eq!(
            coordinator.stats(),
            HttpRangeStats {
                total_length: 10,
                completed_length: 10,
                active_connections: 0,
                active_endgame_duplicates: 0,
                completed_pieces: 3,
                total_pieces: 3,
                retry_count: 0,
            }
        );
    }

    #[test]
    fn retry_releases_piece_waits_and_exhausts_bounded_sources() {
        let mut retry_config = config();
        retry_config.total_length = 4;
        retry_config.split = NonZeroUsize::new(1).expect("nonzero");
        retry_config.max_total_attempts = 2;
        retry_config.max_attempts_per_source = 2;
        let mut coordinator =
            HttpRangeCoordinator::new(retry_config, [source(0, "https://one.example/a")])
                .expect("coordinator starts");

        let HttpRangePoll::Assignment(first) = coordinator.poll(0).expect("first poll") else {
            panic!("first assignment expected");
        };
        coordinator
            .fail(first.lease, HttpRangeFailure::RetryAt(100))
            .expect("retry recorded");
        assert_eq!(
            coordinator.poll(99).expect("wait poll"),
            HttpRangePoll::RetryAt(100)
        );
        let HttpRangePoll::Assignment(second) = coordinator.poll(100).expect("retry poll") else {
            panic!("retry assignment expected");
        };
        assert_eq!(second.span, first.span);
        assert_ne!(second.lease, first.lease);
        coordinator
            .fail(second.lease, HttpRangeFailure::RetryAt(200))
            .expect("second retry recorded");
        assert_eq!(
            coordinator.poll(200).expect("exhausted poll"),
            HttpRangePoll::Exhausted
        );
        assert_eq!(coordinator.stats().retry_count, 1);
    }

    #[test]
    fn restored_piece_retry_wait_preserves_caps_without_stalling_other_pieces() {
        let mut retry_config = config();
        retry_config.total_length = 8;
        retry_config.split = NonZeroUsize::new(1).expect("nonzero");
        let mut coordinator =
            HttpRangeCoordinator::new(retry_config, [source(0, "https://one.example/a")])
                .expect("coordinator starts");
        let delayed = PieceId::new(0);
        coordinator
            .restore_piece_attempts(delayed, 1)
            .expect("piece attempts restore");
        coordinator
            .restore_source_piece_retry(delayed, UriId::new(0), 1, 100)
            .expect("source retry restores");

        let HttpRangePoll::Assignment(available) = coordinator.poll(99).expect("other piece poll")
        else {
            panic!("the other piece remains assignable");
        };
        assert_eq!(available.piece, PieceId::new(1));
        coordinator
            .complete(available.lease)
            .expect("piece completes");
        assert_eq!(
            coordinator.poll(99).expect("restored wait poll"),
            HttpRangePoll::RetryAt(100)
        );
        let HttpRangePoll::Assignment(retry) = coordinator.poll(100).expect("retry poll") else {
            panic!("restored retry becomes assignable");
        };
        assert_eq!(retry.piece, delayed);
        assert_eq!(coordinator.stats().retry_count, 1);

        let mut invalid =
            HttpRangeCoordinator::new(retry_config, [source(0, "https://one.example/a")])
                .expect("coordinator starts");
        invalid
            .restore_piece_attempts(delayed, 1)
            .expect("piece attempts restore");
        assert!(matches!(
            invalid.restore_source_piece_retry(delayed, UriId::new(0), 2, 100),
            Err(HttpRangeCoordinatorError::InvalidConfig)
        ));
    }

    #[test]
    fn rejects_invalid_layout_sources_and_stale_completions() {
        let mut invalid = config();
        invalid.total_length = 0;
        assert!(matches!(
            HttpRangeCoordinator::new(invalid, [source(0, "https://one.example/a")]),
            Err(HttpRangeCoordinatorError::InvalidConfig)
        ));
        assert!(matches!(
            HttpRangeSource::from_uri(UriId::new(0), "ftp://example.test/file"),
            Err(HttpRangeCoordinatorError::InvalidSource)
        ));
        assert!(matches!(
            HttpRangeCoordinator::new(config(), []),
            Err(HttpRangeCoordinatorError::NoSources)
        ));

        let mut coordinator =
            HttpRangeCoordinator::new(config(), [source(0, "https://one.example/a")])
                .expect("coordinator starts");
        let HttpRangePoll::Assignment(assignment) = coordinator.poll(0).expect("assignment") else {
            panic!("assignment expected");
        };
        coordinator
            .complete(assignment.lease)
            .expect("completion succeeds");
        assert!(matches!(
            coordinator.complete(assignment.lease),
            Err(HttpRangeCoordinatorError::UnknownLease)
        ));
    }

    #[test]
    fn same_source_endgame_is_bounded_and_clean_settlement_wins_once() {
        let mut endgame_config = config();
        endgame_config.total_length = 4;
        endgame_config.split = NonZeroUsize::new(1).expect("split");
        endgame_config.max_connections_per_origin = NonZeroUsize::new(2).expect("origin cap");
        endgame_config.endgame_max_duplicates = 2;
        let mut coordinator = HttpRangeCoordinator::new(
            endgame_config,
            [endgame_source(0, "https://one.example/file")],
        )
        .expect("coordinator");
        let HttpRangePoll::Assignment(original) = coordinator.poll(0).expect("original") else {
            panic!("original assignment expected");
        };
        let HttpRangePoll::Endgame {
            assignment: duplicate,
            original: original_id,
        } = coordinator
            .poll_with_endgame(0, &[original.lease])
            .expect("endgame poll")
        else {
            panic!("same-source duplicate expected");
        };
        assert_eq!(original_id, original.lease);
        assert_eq!(duplicate.span, original.span);
        assert_ne!(duplicate.lease, original.lease);
        assert!(duplicate.overlap_group.is_some());
        assert_eq!(
            coordinator
                .poll_with_endgame(0, &[original.lease])
                .expect("cap poll"),
            HttpRangePoll::Saturated
        );
        let fence = coordinator
            .begin_overlap_commit(original.lease)
            .expect("candidate fence");
        assert_eq!(fence.competitor, duplicate.lease);
        coordinator
            .settle_overlap(
                fence.group,
                fence.candidate,
                HttpOverlapSettlement::CandidateCommitted,
            )
            .expect("clean settlement");
        assert_eq!(coordinator.stats().completed_length, 4);
        assert_eq!(coordinator.stats().active_endgame_duplicates, 0);
    }

    #[test]
    fn shared_identity_endgame_uses_a_different_origin() {
        let mut endgame_config = config();
        endgame_config.total_length = 4;
        endgame_config.split = NonZeroUsize::new(1).expect("split");
        endgame_config.max_connections_per_origin = NonZeroUsize::new(1).expect("origin cap");
        endgame_config.endgame_max_duplicates = 1;
        let mut coordinator = HttpRangeCoordinator::new(
            endgame_config,
            [
                shared_endgame_source(0, "https://one.example/file"),
                shared_endgame_source(1, "https://two.example/file"),
            ],
        )
        .expect("coordinator");
        let HttpRangePoll::Assignment(original) = coordinator.poll(0).expect("original") else {
            panic!("original assignment expected");
        };
        let HttpRangePoll::Endgame {
            assignment: duplicate,
            original: original_id,
        } = coordinator
            .poll_with_endgame(0, &[original.lease])
            .expect("endgame poll")
        else {
            panic!("cross-source duplicate expected");
        };
        assert_eq!(original_id, original.lease);
        assert_ne!(duplicate.source, original.source);
        assert_eq!(duplicate.span, original.span);
    }

    #[test]
    fn range_digest_peer_is_endgame_only_for_ordinary_scheduling() {
        let mut endgame_config = config();
        endgame_config.total_length = 4;
        endgame_config.split = NonZeroUsize::new(2).expect("split");
        endgame_config.max_connections_per_origin = NonZeroUsize::new(1).expect("origin cap");
        endgame_config.endgame_max_duplicates = 1;
        let mut coordinator = HttpRangeCoordinator::new(
            endgame_config,
            [
                shared_endgame_source(0, "https://one.example/file"),
                digest_endgame_only_source(1, "https://two.example/file"),
            ],
        )
        .expect("coordinator");
        let HttpRangePoll::Assignment(original) = coordinator.poll(0).expect("original") else {
            panic!("ordinary assignment expected");
        };
        assert_eq!(original.source, UriId::new(0));
        assert_eq!(
            coordinator.poll(0).expect("ordinary saturation"),
            HttpRangePoll::Saturated
        );
        let HttpRangePoll::Endgame {
            assignment: duplicate,
            ..
        } = coordinator
            .poll_with_endgame(0, &[original.lease])
            .expect("endgame poll")
        else {
            panic!("endgame assignment expected");
        };
        assert_eq!(duplicate.source, UriId::new(1));
    }

    #[test]
    fn shared_identity_endgame_falls_back_to_the_validator_pinned_source() {
        let mut endgame_config = config();
        endgame_config.total_length = 4;
        endgame_config.split = NonZeroUsize::new(1).expect("split");
        endgame_config.max_connections_per_origin = NonZeroUsize::new(2).expect("origin cap");
        endgame_config.endgame_max_duplicates = 1;
        let mut coordinator = HttpRangeCoordinator::new(
            endgame_config,
            [
                shared_endgame_source(0, "https://one.example/file").with_same_source_endgame(true),
                digest_endgame_only_source(1, "https://two.example/file"),
            ],
        )
        .expect("coordinator");
        let piece = PieceId::new(0);
        coordinator
            .restore_piece_attempts(piece, 1)
            .expect("piece retry count");
        coordinator
            .restore_source_piece_retry(piece, UriId::new(1), 1, 100)
            .expect("secondary wait");
        let HttpRangePoll::Assignment(original) = coordinator.poll(0).expect("original") else {
            panic!("ordinary assignment expected");
        };
        let HttpRangePoll::Endgame {
            assignment: duplicate,
            ..
        } = coordinator
            .poll_with_endgame(0, &[original.lease])
            .expect("endgame poll")
        else {
            panic!("same-source fallback expected");
        };
        assert_eq!(duplicate.source, original.source);
    }

    #[test]
    fn endgame_requires_strong_same_source_identity_and_dirty_rollback_returns_pending() {
        let mut endgame_config = config();
        endgame_config.total_length = 4;
        endgame_config.split = NonZeroUsize::new(1).expect("split");
        endgame_config.max_connections_per_origin = NonZeroUsize::new(2).expect("origin cap");
        endgame_config.endgame_max_duplicates = 8;
        let mut without_identity =
            HttpRangeCoordinator::new(endgame_config, [source(0, "https://one.example/file")])
                .expect("coordinator");
        let HttpRangePoll::Assignment(original) = without_identity.poll(0).expect("original")
        else {
            panic!("original assignment expected");
        };
        assert_eq!(
            without_identity
                .poll_with_endgame(0, &[original.lease])
                .expect("identity gate"),
            HttpRangePoll::Saturated
        );

        let mut coordinator = HttpRangeCoordinator::new(
            endgame_config,
            [endgame_source(0, "https://one.example/file")],
        )
        .expect("coordinator");
        let HttpRangePoll::Assignment(original) = coordinator.poll(0).expect("original") else {
            panic!("original assignment expected");
        };
        let HttpRangePoll::Endgame { assignment, .. } = coordinator
            .poll_with_endgame(0, &[original.lease])
            .expect("duplicate")
        else {
            panic!("duplicate expected");
        };
        let fence = coordinator
            .begin_overlap_commit(original.lease)
            .expect("candidate fence");
        assert_eq!(fence.competitor, assignment.lease);
        coordinator
            .settle_overlap(
                fence.group,
                fence.candidate,
                HttpOverlapSettlement::RolledBack,
            )
            .expect("dirty rollback");
        assert_eq!(coordinator.stats().completed_length, 0);
        assert_eq!(coordinator.stats().active_connections, 0);
        let HttpRangePoll::Assignment(retry) = coordinator.poll(0).expect("pending retry") else {
            panic!("rolled-back piece must be pending");
        };
        assert_eq!(retry.piece, original.piece);
    }
}
