//! BT persistence is independent of project-owned transfer journals.
use super::*;
use ariax_bt_metadata::{BtIdentity, MetadataLimits, parse_info, parse_magnet, parse_torrent};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;

pub(super) const BT_METADATA_TABLE_SQL: &str = r#"CREATE TABLE bt_metadata (
    gid TEXT PRIMARY KEY NOT NULL,
    generation BLOB NOT NULL CHECK(typeof(generation) = 'blob' AND length(generation) = 8),
    v1 TEXT UNIQUE CHECK(v1 IS NULL OR (length(v1) = 40 AND v1 NOT GLOB '*[^0-9a-f]*')),
    v2 TEXT UNIQUE CHECK(v2 IS NULL OR (length(v2) = 64 AND v2 NOT GLOB '*[^0-9a-f]*')),
    root_identity BLOB NOT NULL CHECK(typeof(root_identity) = 'blob' AND length(root_identity) BETWEEN 1 AND 4096),
    metainfo BLOB NOT NULL CHECK(typeof(metainfo) = 'blob' AND length(metainfo) <= 16777216),
    info BLOB NOT NULL CHECK(typeof(info) = 'blob' AND length(info) <= 16777216),
    magnet TEXT CHECK(magnet IS NULL OR length(CAST(magnet AS BLOB)) <= 65536),
    files BLOB NOT NULL CHECK(typeof(files) = 'blob' AND length(files) <= 16777216),
    downloaded BLOB NOT NULL CHECK(typeof(downloaded) = 'blob' AND length(downloaded) = 8),
    uploaded BLOB NOT NULL CHECK(typeof(uploaded) = 'blob' AND length(uploaded) = 8),
    seed_millis BLOB NOT NULL CHECK(typeof(seed_millis) = 'blob' AND length(seed_millis) = 8),
    CHECK(v1 IS NOT NULL OR v2 IS NOT NULL),
    FOREIGN KEY(gid) REFERENCES task(gid) ON UPDATE CASCADE ON DELETE CASCADE
) STRICT"#;

const METADATA_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionBtFile {
    pub index: u32,
    pub path: String,
    pub length: u64,
    pub offset: u64,
    pub selected: bool,
    pub padding: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionBtBinding {
    pub identity: BtIdentity,
    pub root_identity: Vec<u8>,
    pub metainfo: Vec<u8>,
    pub info: Vec<u8>,
    pub magnet: Option<String>,
    pub files: Vec<SessionBtFile>,
}

impl SessionBtBinding {
    /// Domain-separated evidence for the exact identity, mapping and selection.
    /// This is independent of a transfer journal and of the local root spelling.
    pub fn layout_hash(&self) -> JournalHash {
        use sha2::{Digest as _, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"ariax/bt-layout/v3\0");
        for identity in [&self.identity.v1, &self.identity.v2] {
            hash.update([u8::from(identity.is_some())]);
            if let Some(identity) = identity {
                hash.update(identity.as_bytes());
            }
        }
        for file in &self.files {
            hash.update(file.index.to_le_bytes());
            hash.update((file.path.len() as u64).to_le_bytes());
            hash.update(file.path.as_bytes());
            hash.update(file.length.to_le_bytes());
            hash.update(file.offset.to_le_bytes());
            hash.update([u8::from(file.selected), u8::from(file.padding)]);
        }
        JournalHash::new(hash.finalize().into()).expect("SHA-256 layout digest is nonzero")
    }

    pub fn validate(&self) -> Result<(), SessionStoreError> {
        let invalid = || SessionStoreError::InvalidRecord("bt.binding");
        if self.root_identity.is_empty()
            || self.root_identity.len() > 4096
            || self.metainfo.len() > METADATA_BYTES
            || self.info.len() > METADATA_BYTES
            || self.files.len() > 10_000
        {
            return Err(invalid());
        }
        let mut identity_uri = String::from("magnet:?");
        for (hash, prefix, size) in [
            (&self.identity.v1, "urn:btih:", 40),
            (&self.identity.v2, "urn:btmh:1220", 64),
        ] {
            if let Some(hash) = hash {
                if hash.len() != size
                    || !hash
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                {
                    return Err(invalid());
                }
                identity_uri.push_str(&format!("xt={prefix}{hash}&"));
            }
        }
        parse_magnet(&identity_uri).map_err(|_| invalid())?;
        if let Some(magnet) = &self.magnet {
            let magnet = parse_magnet(magnet).map_err(|_| invalid())?;
            if !magnet.identity.matches(&self.identity) {
                return Err(invalid());
            }
        }
        let mut metadata = None;
        for (bytes, torrent) in [(&self.metainfo, true), (&self.info, false)] {
            if !bytes.is_empty() {
                let parsed = if torrent {
                    parse_torrent(bytes, MetadataLimits::default())
                } else {
                    parse_info(bytes, MetadataLimits::default())
                }
                .map_err(|_| invalid())?;
                if parsed.identity != self.identity {
                    return Err(invalid());
                }
                metadata = Some(parsed);
            }
        }
        let Some(metadata) = metadata else {
            return if self.magnet.is_some() && self.files.is_empty() {
                Ok(())
            } else {
                Err(invalid())
            };
        };
        if metadata.files.len() != self.files.len() {
            return Err(invalid());
        }
        let mut paths = BTreeSet::new();
        let mut directories = BTreeSet::new();
        for (file, native) in self.files.iter().zip(&metadata.files) {
            if file.index != native.index
                || file.length != native.length
                || file.offset != native.offset
                || file.padding != native.padding
                || file.padding && file.selected
                || file.path.len() > 4096
            {
                return Err(invalid());
            }
            let safe = crate::SafePathBuilder::from_user_path(&file.path, PathPlatform::Windows)
                .map_err(|_| invalid())?;
            if safe.canonical_string() != file.path {
                return Err(invalid());
            }
            let components = safe.components().collect::<Vec<_>>();
            let key = file.path.to_lowercase();
            if directories.contains(&key) || !paths.insert(key) {
                return Err(invalid());
            }
            for end in 1..components.len() {
                let parent = components[..end].join("/").to_lowercase();
                if paths.contains(&parent) {
                    return Err(invalid());
                }
                directories.insert(parent);
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn owned_bytes(&self) -> usize {
        self.metainfo
            .capacity()
            .saturating_add(self.info.capacity())
            .saturating_add(self.root_identity.capacity())
            .saturating_add(self.magnet.as_ref().map_or(0, String::capacity))
            .saturating_add(
                self.files
                    .capacity()
                    .saturating_mul(std::mem::size_of::<SessionBtFile>()),
            )
            .saturating_add(
                self.files
                    .iter()
                    .map(|file| file.path.capacity())
                    .sum::<usize>(),
            )
            .saturating_add(256)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionBtTaskRecord {
    pub gid: Gid,
    pub session_id: SessionId,
    pub queue_state: SessionQueueState,
    pub queue_position: u32,
    pub desired_paused: bool,
    pub root_display: PlatformPath,
    pub generation: u64,
    pub binding: SessionBtBinding,
    pub downloaded: u64,
    pub uploaded: u64,
    pub seed_millis: u64,
    pub created_ms: u64,
    pub updated_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionBtResumeRecord {
    pub gid: Gid,
    pub generation: u64,
    pub request: u64,
    pub resume_blob: Arc<[u8]>,
    pub dirty: bool,
    pub saved_ms: u64,
}

/// Explicit replacement at a drained pause, distinct from native metadata binding.
#[derive(Clone, Debug)]
pub struct SessionBtOptionPatch {
    pub previous: Arc<SessionBtTaskRecord>,
    pub replacement: Arc<SessionBtTaskRecord>,
    pub generation: u64,
    pub expected_options: JournalHash,
    pub options: SanitizedOptionMap,
}

/// `None` records a failed boundary and retains the previous safe resume blob.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionBtCheckpoint {
    pub gid: Gid,
    pub generation: u64,
    pub request: u64,
    pub resume_blob: Option<Arc<[u8]>>,
    pub downloaded: u64,
    pub uploaded: u64,
    pub seed_millis: u64,
    pub saved_ms: u64,
}

fn files_blob(binding: &SessionBtBinding) -> Result<Vec<u8>, SessionStoreError> {
    // Bound all variable data before serializing, including JSON escaping.
    if binding
        .files
        .iter()
        .try_fold(2usize, |sum, file| {
            sum.checked_add(file.path.len().saturating_mul(6).saturating_add(256))
        })
        .is_none_or(|size| size > METADATA_BYTES)
    {
        return Err(SessionStoreError::InvalidRecord("bt.files_size"));
    }
    serde_json::to_vec(&binding.files).map_err(|_| SessionStoreError::InvalidRecord("bt.files"))
}

pub(super) fn validate_admission<P: PersistedOptionPolicy>(
    task: &SessionBtTaskRecord,
    options: &SanitizedOptionMap,
    policy: &P,
) -> Result<(), SessionStoreError> {
    task.binding.validate()?;
    validate_time_order(task.created_ms, task.updated_ms)?;
    validate_options_for_persistence(options, policy)?;
    if !matches!(
        task.queue_state,
        SessionQueueState::Waiting | SessionQueueState::Paused
    ) {
        return Err(SessionStoreError::InvalidRecord("bt.initial_state"));
    }
    Ok(())
}

pub(super) fn insert_admission(
    transaction: &rusqlite::Transaction<'_>,
    task: &SessionBtTaskRecord,
    options: &SanitizedOptionMap,
) -> Result<(), SessionStoreError> {
    let files = files_blob(&task.binding)?;
    let root = encode_platform_path(&task.root_display)?;
    if task_exists(transaction, task.gid)? {
        return Err(SessionStoreError::InvalidRecord("task.gid_exists"));
    }
    let count: i64 = transaction.query_row("SELECT COUNT(*) FROM task", [], |row| row.get(0))?;
    if count >= SESSION_MAX_TASKS as i64 {
        return Err(SessionStoreError::InvalidRecord("task.count"));
    }
    let queue_len: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM task WHERE queue_state=?1",
        [task.queue_state as i64],
        |row| row.get(0),
    )?;
    if i64::from(task.queue_position) > queue_len {
        return Err(SessionStoreError::QueueInvariant);
    }
    transaction.execute("UPDATE task SET queue_position=queue_position+1 WHERE queue_state=?1 AND queue_position>=?2", params![task.queue_state as i64, task.queue_position])?;
    transaction.execute("INSERT INTO task(gid,session_id,task_kind,queue_state,queue_position,desired_paused,slow_demotion_count,root_display,created_ms,updated_ms) VALUES(?1,?2,2,?3,?4,?5,0,?6,?7,?8)",
            params![task.gid.to_string(), task.session_id.as_bytes().as_slice(), task.queue_state as i64, task.queue_position, bool_to_i64(task.desired_paused), root, time_to_i64(task.created_ms,"bt.created_ms")?, time_to_i64(task.updated_ms,"bt.updated_ms")?])?;
    transaction.execute("INSERT INTO bt_metadata(gid,generation,v1,v2,root_identity,metainfo,info,magnet,files,downloaded,uploaded,seed_millis) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![task.gid.to_string(), encode_u64(task.generation), task.binding.identity.v1, task.binding.identity.v2, task.binding.root_identity, task.binding.metainfo, task.binding.info, task.binding.magnet, files, encode_u64(task.downloaded), encode_u64(task.uploaded), encode_u64(task.seed_millis)])?;
    transaction.execute("INSERT INTO bt_resume(gid,resume_blob,dirty,request,generation,saved_ms) VALUES(?1,X'',1,?2,?3,?4)", params![task.gid.to_string(), encode_u64(0), encode_u64(task.generation), time_to_i64(task.created_ms,"bt.created_ms")?])?;
    replace_task_options_in_transaction(
        transaction,
        task.gid,
        OptionsSnapshotScope::CurrentGeneration,
        options,
    )?;
    Ok(())
}

impl SessionStore {
    pub fn create_bt_task<P: PersistedOptionPolicy>(
        &mut self,
        task: &SessionBtTaskRecord,
        options: &SanitizedOptionMap,
        policy: &P,
    ) -> Result<(), SessionStoreError> {
        validate_admission(task, options, policy)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_admission(&transaction, task, options)?;
        validate_dense_queues(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn confirm_bt_task<P: PersistedOptionPolicy>(
        &self,
        task: &SessionBtTaskRecord,
        options: &SanitizedOptionMap,
        policy: &P,
    ) -> Result<(), SessionStoreError> {
        validate_admission(task, options, policy)?;
        if read_task(&self.connection, task.gid)? != *task
            || self.task_options(task.gid, OptionsSnapshotScope::CurrentGeneration, policy)?
                != *options
        {
            return Err(SessionStoreError::InvalidRecord("import.metadata_mismatch"));
        }
        Ok(())
    }

    pub fn bt_tasks(&self) -> Result<Vec<SessionBtTaskRecord>, SessionStoreError> {
        read_tasks(&self.connection)
    }

    pub fn bt_resume(
        &self,
        gid: Gid,
        limit: usize,
    ) -> Result<SessionBtResumeRecord, SessionStoreError> {
        if limit == 0 || limit > SESSION_MAX_BT_RESUME_BYTES {
            return Err(SessionStoreError::InvalidRecord("bt.resume_limit"));
        }
        let size: i64 = self
            .connection
            .query_row(
                "SELECT length(resume_blob) FROM bt_resume WHERE gid=?1",
                [gid.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or(SessionStoreError::NotFound)?;
        bounded_count(size, limit, "bt.resume_size")?;
        self.connection
            .query_row(
                "SELECT generation,request,resume_blob,dirty,saved_ms FROM bt_resume WHERE gid=?1",
                [gid.to_string()],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )?
            .pipe_resume(gid)
    }

    pub fn bind_bt_metadata(
        &mut self,
        gid: Gid,
        generation: u64,
        binding: &SessionBtBinding,
    ) -> Result<(), SessionStoreError> {
        binding.validate()?;
        let files = files_blob(binding)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old = read_task(&transaction, gid)?;
        if old.generation != generation
            || old.binding.root_identity != binding.root_identity
            || !old.binding.identity.matches(&binding.identity)
            || old.binding.magnet != binding.magnet
            || !old.binding.files.is_empty()
                && (old.binding.files != binding.files || old.binding.identity != binding.identity)
            || !old.binding.info.is_empty() && old.binding.info != binding.info
            || !old.binding.metainfo.is_empty() && old.binding.metainfo != binding.metainfo
        {
            return Err(SessionStoreError::InvalidRecord("bt.binding_changed"));
        }
        transaction.execute(
            "UPDATE bt_metadata SET v1=?2,v2=?3,metainfo=?4,info=?5,files=?6 WHERE gid=?1",
            params![
                gid.to_string(),
                binding.identity.v1,
                binding.identity.v2,
                binding.metainfo,
                binding.info,
                files
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn replace_paused_bt_options(
        &mut self,
        patch: &SessionBtOptionPatch,
        policy: &impl PersistedOptionPolicy,
    ) -> Result<(), SessionStoreError> {
        let before = &patch.previous;
        let after = &patch.replacement;
        after.binding.validate()?;
        validate_options_for_persistence(&patch.options, policy)?;
        if before.gid != after.gid
            || before.session_id != after.session_id
            || before.root_display != after.root_display
            || before.binding.identity != after.binding.identity
            || before.binding.root_identity != after.binding.root_identity
            || before.binding.info != after.binding.info
        {
            return Err(SessionStoreError::InvalidRecord("bt.option_identity"));
        }
        let files = files_blob(&after.binding)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = read_task(&transaction, before.gid)?;
        if current.queue_state != SessionQueueState::Paused
            || !current.desired_paused
            || current.generation != patch.generation
            || current.binding != before.binding
            || read_task_options(
                &transaction,
                before.gid,
                OptionsSnapshotScope::CurrentGeneration,
                policy,
            )?
            .snapshot_hash()
                != patch.expected_options
        {
            return Err(SessionStoreError::InvalidRecord("bt.option_changed"));
        }
        transaction.execute(
            "UPDATE bt_metadata SET metainfo=?2,magnet=?3,files=?4 WHERE gid=?1",
            params![
                before.gid.to_string(),
                after.binding.metainfo,
                after.binding.magnet,
                files
            ],
        )?;
        replace_task_options_in_transaction(
            &transaction,
            before.gid,
            OptionsSnapshotScope::CurrentGeneration,
            &patch.options,
        )?;
        // All old-generation callbacks are obsolete. Only BeginBtGeneration
        // can reopen its request sequence; paused removal can use this token.
        transaction.execute(
            "UPDATE bt_resume SET resume_blob=X'',dirty=1,request=?2 WHERE gid=?1",
            params![before.gid.to_string(), encode_u64(u64::MAX)],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Mark dirty before the adapter can perform any new payload I/O.
    pub fn begin_bt_generation(
        &mut self,
        gid: Gid,
        expected: u64,
        generation: u64,
    ) -> Result<(), SessionStoreError> {
        if generation < expected || generation == expected && generation != 0 {
            return Err(SessionStoreError::InvalidRecord("bt.generation"));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if transaction.execute(
            "UPDATE bt_metadata SET generation=?3 WHERE gid=?1 AND generation=?2",
            params![
                gid.to_string(),
                encode_u64(expected),
                encode_u64(generation)
            ],
        )? != 1
        {
            return Err(SessionStoreError::InvalidRecord("bt.stale_generation"));
        }
        transaction.execute(
            "UPDATE bt_resume SET dirty=1,generation=?2,request=?3 WHERE gid=?1",
            params![gid.to_string(), encode_u64(generation), encode_u64(0)],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn mark_bt_dirty(&mut self, gid: Gid, generation: u64) -> Result<(), SessionStoreError> {
        if self.connection.execute(
            "UPDATE bt_resume SET dirty=1 WHERE gid=?1 AND generation=?2",
            params![gid.to_string(), encode_u64(generation)],
        )? != 1
        {
            return Err(SessionStoreError::InvalidRecord("bt.stale_generation"));
        }
        Ok(())
    }

    pub fn persist_bt_terminal(
        &mut self,
        result: &SessionStoppedResultRecord,
        transition: &SessionQueueTransition,
        generation: u64,
        request: u64,
    ) -> Result<(), SessionStoreError> {
        validate_stopped_result(result)?;
        if result.gid != transition.gid {
            return Err(SessionStoreError::InvalidRecord("bt.terminal_identity"));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (stored_generation, stored_request, dirty): (Vec<u8>, Vec<u8>, i64) = transaction
            .query_row(
                "SELECT generation,request,dirty FROM bt_resume WHERE gid=?1",
                [result.gid.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
        if decode_u64(&stored_generation, "bt.generation")? != generation
            || decode_u64(&stored_request, "bt.request")? != request
            || result.status == SessionTerminalStatus::Complete && dirty != 0
        {
            return Err(SessionStoreError::InvalidRecord("bt.terminal_checkpoint"));
        }
        apply_exact_queue_transition_in_transaction(&transaction, transition)?;
        transaction.execute("INSERT INTO stopped_result(gid,terminal_status,error_code,safe_message,total_length,layout_hash,completed_ms) VALUES(?1,?2,?3,?4,?5,?6,?7)", params![result.gid.to_string(),result.status as i64,result.error_kind.map_or(0,|kind|i64::from(kind.number())),result.safe_message,result.total_length.map(encode_u64),result.layout_hash.map(|hash|hash.as_bytes().to_vec()),time_to_i64(result.completed_ms,"bt.completed_ms")?])?;
        validate_dense_queues(&transaction)?;
        validate_stopped_result_pairing(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn checkpoint_bt(
        &mut self,
        checkpoint: &SessionBtCheckpoint,
    ) -> Result<(), SessionStoreError> {
        if checkpoint.request == 0
            || checkpoint
                .resume_blob
                .as_ref()
                .is_some_and(|blob| blob.is_empty() || blob.len() > SESSION_MAX_BT_RESUME_BYTES)
        {
            return Err(SessionStoreError::InvalidRecord("bt.checkpoint"));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction.query_row("SELECT b.generation,r.request,b.downloaded,b.uploaded,b.seed_millis,r.saved_ms FROM bt_metadata b JOIN bt_resume r ON r.gid=b.gid WHERE b.gid=?1", [checkpoint.gid.to_string()], |row| Ok((row.get::<_,Vec<u8>>(0)?,row.get::<_,Vec<u8>>(1)?,row.get::<_,Vec<u8>>(2)?,row.get::<_,Vec<u8>>(3)?,row.get::<_,Vec<u8>>(4)?,row.get::<_,i64>(5)?))).optional()?.ok_or(SessionStoreError::NotFound)?;
        if decode_u64(&current.0, "bt.generation")? != checkpoint.generation
            || decode_u64(&current.1, "bt.request")? >= checkpoint.request
            || decode_u64(&current.2, "bt.downloaded")? > checkpoint.downloaded
            || decode_u64(&current.3, "bt.uploaded")? > checkpoint.uploaded
            || decode_u64(&current.4, "bt.seed_millis")? > checkpoint.seed_millis
            || nonnegative_i64(current.5, "bt.saved_ms")? > checkpoint.saved_ms
        {
            return Err(SessionStoreError::InvalidRecord("bt.stale_checkpoint"));
        }
        transaction.execute(
            "UPDATE bt_metadata SET downloaded=?2,uploaded=?3,seed_millis=?4 WHERE gid=?1",
            params![
                checkpoint.gid.to_string(),
                encode_u64(checkpoint.downloaded),
                encode_u64(checkpoint.uploaded),
                encode_u64(checkpoint.seed_millis)
            ],
        )?;
        transaction.execute("UPDATE bt_resume SET resume_blob=COALESCE(?2,resume_blob),dirty=?3,request=?4,generation=?5,saved_ms=CASE WHEN ?2 IS NULL THEN saved_ms ELSE ?6 END WHERE gid=?1", params![checkpoint.gid.to_string(),checkpoint.resume_blob.as_deref(),bool_to_i64(checkpoint.resume_blob.is_none()),encode_u64(checkpoint.request),encode_u64(checkpoint.generation),time_to_i64(checkpoint.saved_ms,"bt.saved_ms")?])?;
        transaction.commit()?;
        Ok(())
    }
}

// Keep the SQL row conversion separate from untrusted blob validation.
trait ResumeRow {
    fn pipe_resume(self, gid: Gid) -> Result<SessionBtResumeRecord, SessionStoreError>;
}
impl ResumeRow for (Vec<u8>, Vec<u8>, Vec<u8>, i64, i64) {
    fn pipe_resume(self, gid: Gid) -> Result<SessionBtResumeRecord, SessionStoreError> {
        Ok(SessionBtResumeRecord {
            gid,
            generation: decode_u64(&self.0, "bt.generation")?,
            request: decode_u64(&self.1, "bt.request")?,
            resume_blob: self.2.into(),
            dirty: decode_bool(self.3, "bt.dirty")?,
            saved_ms: nonnegative_i64(self.4, "bt.saved_ms")?,
        })
    }
}

fn read_task(connection: &Connection, gid: Gid) -> Result<SessionBtTaskRecord, SessionStoreError> {
    read_tasks_filtered(connection, Some(gid))?
        .pop()
        .ok_or(SessionStoreError::NotFound)
}

fn read_tasks(connection: &Connection) -> Result<Vec<SessionBtTaskRecord>, SessionStoreError> {
    read_tasks_filtered(connection, None)
}

fn read_tasks_filtered(
    connection: &Connection,
    gid: Option<Gid>,
) -> Result<Vec<SessionBtTaskRecord>, SessionStoreError> {
    let filter = gid.map(|gid| gid.to_string());
    let (count, bytes): (i64,i64) = connection.query_row("SELECT COUNT(*),COALESCE(SUM(length(b.metainfo)+length(b.info)+length(b.files)+COALESCE(length(b.magnet),0)+length(b.root_identity)+length(t.root_display)+1024),0) FROM bt_metadata b JOIN task t ON t.gid=b.gid WHERE ?1 IS NULL OR b.gid=?1", [&filter], |row| Ok((row.get(0)?,row.get(1)?)))?;
    let count = bounded_count(count, SESSION_MAX_TASKS, "bt.count")?;
    bounded_count(bytes, SESSION_TASK_READ_BUDGET_BYTES, "bt.read_budget")?;
    let mut statement = connection.prepare("SELECT t.gid,t.session_id,t.queue_state,t.queue_position,t.desired_paused,t.root_display,t.created_ms,t.updated_ms,b.generation,b.v1,b.v2,b.root_identity,b.metainfo,b.info,b.magnet,b.files,b.downloaded,b.uploaded,b.seed_millis FROM bt_metadata b JOIN task t ON t.gid=b.gid WHERE ?1 IS NULL OR b.gid=?1 ORDER BY t.queue_state,t.queue_position,t.gid")?;
    let mut rows = statement.query([&filter])?;
    let mut tasks = Vec::with_capacity(count);
    let mut owned = 0usize;
    while let Some(row) = rows.next()? {
        let file_bytes: Vec<u8> = row.get(15)?;
        let task = SessionBtTaskRecord {
            gid: decode_gid(&row.get::<_, String>(0)?)?,
            session_id: decode_session_id(&row.get::<_, Vec<u8>>(1)?)?,
            queue_state: SessionQueueState::try_from(row.get::<_, i64>(2)?)?,
            queue_position: u32::try_from(row.get::<_, i64>(3)?)
                .map_err(|_| SessionStoreError::InvalidPersistedValue("bt.queue_position"))?,
            desired_paused: decode_bool(row.get(4)?, "bt.desired_paused")?,
            root_display: decode_platform_path(&row.get::<_, Vec<u8>>(5)?, "bt.root")?,
            created_ms: nonnegative_i64(row.get(6)?, "bt.created_ms")?,
            updated_ms: nonnegative_i64(row.get(7)?, "bt.updated_ms")?,
            generation: decode_u64(&row.get::<_, Vec<u8>>(8)?, "bt.generation")?,
            binding: SessionBtBinding {
                identity: BtIdentity {
                    v1: row.get(9)?,
                    v2: row.get(10)?,
                },
                root_identity: row.get(11)?,
                metainfo: row.get(12)?,
                info: row.get(13)?,
                magnet: row.get(14)?,
                files: serde_json::from_slice(&file_bytes)
                    .map_err(|_| SessionStoreError::InvalidPersistedValue("bt.files"))?,
            },
            downloaded: decode_u64(&row.get::<_, Vec<u8>>(16)?, "bt.downloaded")?,
            uploaded: decode_u64(&row.get::<_, Vec<u8>>(17)?, "bt.uploaded")?,
            seed_millis: decode_u64(&row.get::<_, Vec<u8>>(18)?, "bt.seed_millis")?,
        };
        task.binding.validate()?;
        validate_time_order(task.created_ms, task.updated_ms)?;
        if task.queue_state == SessionQueueState::Demoted {
            return Err(SessionStoreError::InvalidPersistedValue("bt.state"));
        }
        owned = owned
            .saturating_add(task.binding.owned_bytes())
            .saturating_add(std::mem::size_of::<SessionBtTaskRecord>());
        if owned > SESSION_TASK_READ_BUDGET_BYTES {
            return Err(SessionStoreError::InvalidPersistedValue("bt.read_budget"));
        }
        tasks.push(task);
    }
    Ok(tasks)
}

pub(super) fn validate_rows(connection: &Connection) -> Result<(), SessionStoreError> {
    let invalid: i64 = connection.query_row("SELECT (SELECT COUNT(*) FROM task t LEFT JOIN bt_metadata b ON b.gid=t.gid WHERE (t.task_kind=2)!=(b.gid IS NOT NULL)) + (SELECT COUNT(*) FROM bt_metadata b LEFT JOIN bt_resume r ON r.gid=b.gid WHERE r.gid IS NULL OR r.generation!=b.generation OR (r.dirty=0 AND length(r.resume_blob)=0))", [], |row| row.get(0))?;
    if invalid != 0 {
        return Err(SessionStoreError::InvalidPersistedValue("bt.task_pair"));
    }
    read_tasks(connection)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let id = BACKUP_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("ariax-bt-store-{}-{id}", std::process::id()));
            create_private_directory(&path).unwrap();
            Self(path)
        }
        fn database(&self) -> PathBuf {
            self.0.join("session.db")
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn policy(_: &str) -> bool {
        false
    }
    fn record() -> SessionBtTaskRecord {
        let metainfo =
            include_bytes!("../../ariax-bt-libtorrent-sys/tests/fixtures/v1.torrent").to_vec();
        let metadata = parse_torrent(&metainfo, MetadataLimits::default()).unwrap();
        SessionBtTaskRecord {
            gid: Gid::new(1).unwrap(),
            session_id: SessionId::new([1; 16]),
            queue_state: SessionQueueState::Waiting,
            queue_position: 0,
            desired_paused: false,
            root_display: PlatformPath::from_native_bytes(PathPlatform::Unix, b"/output").unwrap(),
            generation: 1,
            downloaded: 0,
            uploaded: 0,
            seed_millis: 0,
            created_ms: 1,
            updated_ms: 1,
            binding: SessionBtBinding {
                identity: metadata.identity,
                root_identity: vec![1; 16],
                metainfo,
                info: Vec::new(),
                magnet: None,
                files: metadata
                    .files
                    .into_iter()
                    .map(|file| SessionBtFile {
                        index: file.index,
                        path: file.components.join("/"),
                        length: file.length,
                        offset: file.offset,
                        selected: !file.padding,
                        padding: file.padding,
                    })
                    .collect(),
            },
        }
    }
    fn store(directory: &Directory) -> SessionStore {
        let mut store =
            SessionStore::open(directory.database(), SessionStoreConfig::default()).unwrap();
        store
            .put_session(&SessionRecord {
                session_id: SessionId::new([1; 16]),
                created_ms: 1,
                updated_ms: 1,
                clean_shutdown: false,
            })
            .unwrap();
        store
    }
    #[test]
    fn checkpoints_retain_safe_data_after_failure_and_reject_stale_completion() {
        let directory = Directory::new();
        let mut store = store(&directory);
        let task = record();
        store
            .create_bt_task(&task, &SanitizedOptionMap::new([]).unwrap(), &policy)
            .unwrap();
        assert!(store.tasks().unwrap().is_empty());
        assert_eq!(store.bt_tasks().unwrap(), [task.clone()]);
        let journal: Option<Vec<u8>> = store
            .connection
            .query_row("SELECT primary_journal_id FROM task", [], |row| row.get(0))
            .unwrap();
        assert!(journal.is_none());
        let mut checkpoint = SessionBtCheckpoint {
            gid: task.gid,
            generation: 1,
            request: 1,
            resume_blob: Some(Arc::from(b"de".as_slice())),
            downloaded: 3,
            uploaded: 2,
            seed_millis: 1,
            saved_ms: 2,
        };
        store.checkpoint_bt(&checkpoint).unwrap();
        assert!(!store.bt_resume(task.gid, 16).unwrap().dirty);
        assert!(store.bt_resume(task.gid, 1).is_err());
        checkpoint.request = 2;
        checkpoint.resume_blob = None;
        store.checkpoint_bt(&checkpoint).unwrap();
        let dirty = store.bt_resume(task.gid, 16).unwrap();
        assert!(dirty.dirty);
        assert_eq!(&*dirty.resume_blob, b"de");
        checkpoint.request = 1;
        checkpoint.resume_blob = Some(Arc::from(b"d1:xi1ee".as_slice()));
        assert!(store.checkpoint_bt(&checkpoint).is_err());
        assert_eq!(store.bt_resume(task.gid, 16).unwrap(), dirty);
        store.begin_bt_generation(task.gid, 1, 2).unwrap();
        checkpoint.request = 3;
        assert!(store.checkpoint_bt(&checkpoint).is_err());
        drop(store);
        let store =
            SessionStore::open(directory.database(), SessionStoreConfig::default()).unwrap();
        let restored = store.bt_resume(task.gid, 16).unwrap();
        assert!(restored.dirty);
        assert_eq!(restored.generation, 2);
        assert_eq!(&*restored.resume_blob, b"de");
    }
    #[test]
    fn paused_option_replacement_is_atomic_and_retires_old_checkpoint_tokens() {
        let directory = Directory::new();
        let mut store = store(&directory);
        let mut task = record();
        task.queue_state = SessionQueueState::Paused;
        task.desired_paused = true;
        let options = SanitizedOptionMap::new([]).unwrap();
        store.create_bt_task(&task, &options, &policy).unwrap();
        let mut checkpoint = SessionBtCheckpoint {
            gid: task.gid,
            generation: task.generation,
            request: 1,
            resume_blob: Some(Arc::from(b"de".as_slice())),
            downloaded: 7,
            uploaded: 3,
            seed_millis: 2,
            saved_ms: 2,
        };
        store.checkpoint_bt(&checkpoint).unwrap();
        let original = store.bt_tasks().unwrap().remove(0);
        let resume = store.bt_resume(task.gid, 64).unwrap();
        let mut replacement = original.clone();
        replacement.binding.files[0].path = "renamed.bin".into();
        let patch = SessionBtOptionPatch {
            previous: Arc::new(original.clone()),
            replacement: Arc::new(replacement),
            generation: task.generation,
            expected_options: options.snapshot_hash(),
            options: SanitizedOptionMap::new([("out".into(), "renamed.bin".into())]).unwrap(),
        };
        store.connection.execute_batch("CREATE TEMP TRIGGER reject_restart BEFORE UPDATE ON bt_resume BEGIN SELECT RAISE(ABORT, 'injected restart failure'); END;").unwrap();
        assert!(store.replace_paused_bt_options(&patch, &policy).is_err());
        assert_eq!(store.bt_tasks().unwrap(), [original.clone()]);
        assert_eq!(
            store
                .task_options(task.gid, OptionsSnapshotScope::CurrentGeneration, &policy)
                .unwrap(),
            options
        );
        assert_eq!(store.bt_resume(task.gid, 64).unwrap(), resume);
        store
            .connection
            .execute_batch("DROP TRIGGER reject_restart")
            .unwrap();
        let mut stale = patch.clone();
        stale.generation += 1;
        assert!(store.replace_paused_bt_options(&stale, &policy).is_err());
        stale = patch.clone();
        stale.expected_options = JournalHash::new([99; 32]).unwrap();
        assert!(store.replace_paused_bt_options(&stale, &policy).is_err());
        store.replace_paused_bt_options(&patch, &policy).unwrap();
        let changed = store.bt_tasks().unwrap().remove(0);
        assert_eq!(changed.binding.files[0].path, "renamed.bin");
        assert_eq!(
            (changed.downloaded, changed.uploaded, changed.seed_millis),
            (7, 3, 2)
        );
        assert_eq!(changed.queue_state, SessionQueueState::Paused);
        assert!(store.replace_paused_bt_options(&patch, &policy).is_err());
        let retired = store.bt_resume(task.gid, 64).unwrap();
        assert!(retired.dirty && retired.resume_blob.is_empty());
        assert_eq!(retired.request, u64::MAX);
        checkpoint.request = 2;
        assert!(store.checkpoint_bt(&checkpoint).is_err());
        store
            .begin_bt_generation(task.gid, task.generation, task.generation + 1)
            .unwrap();
        checkpoint.generation += 1;
        checkpoint.request = 1;
        store.checkpoint_bt(&checkpoint).unwrap();
        drop(store);
        let store =
            SessionStore::open(directory.database(), SessionStoreConfig::default()).unwrap();
        assert_eq!(
            store.bt_tasks().unwrap()[0].binding,
            patch.replacement.binding
        );
        assert!(!store.bt_resume(task.gid, 64).unwrap().dirty);
    }

    #[test]
    fn metadata_binding_rejects_changed_paths_identity_and_duplicate_torrents() {
        let directory = Directory::new();
        let mut store = store(&directory);
        let task = record();
        store
            .create_bt_task(&task, &SanitizedOptionMap::new([]).unwrap(), &policy)
            .unwrap();
        store.bind_bt_metadata(task.gid, 1, &task.binding).unwrap();
        let mut changed = task.binding.clone();
        changed.files[0].path = "../outside".into();
        assert!(store.bind_bt_metadata(task.gid, 1, &changed).is_err());
        changed.files[0].path = "different".into();
        assert!(store.bind_bt_metadata(task.gid, 1, &changed).is_err());
        changed = task.binding.clone();
        changed.identity.v1 = Some("0".repeat(40));
        assert!(store.bind_bt_metadata(task.gid, 1, &changed).is_err());
        let mut duplicate = task.clone();
        duplicate.gid = Gid::new(2).unwrap();
        assert!(
            store
                .create_bt_task(&duplicate, &SanitizedOptionMap::new([]).unwrap(), &policy)
                .is_err()
        );
        assert_eq!(store.bt_tasks().unwrap(), [task]);
    }
    #[test]
    fn older_development_formats_are_rejected_without_mutating_artifacts() {
        for version in [1, 2] {
            let directory = Directory::new();
            let connection = Connection::open(directory.database()).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE sentinel(value TEXT); INSERT INTO sentinel VALUES('preserve');",
                )
                .unwrap();
            connection
                .pragma_update(None, "user_version", version)
                .unwrap();
            drop(connection);
            let bytes = fs::read(directory.database()).unwrap();
            let names = fs::read_dir(&directory.0)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>();
            assert!(
                matches!(SessionStore::open(directory.database(),SessionStoreConfig::default()),Err(SessionStoreError::UnsupportedSchema {found,..}) if found==version)
            );
            assert_eq!(fs::read(directory.database()).unwrap(), bytes);
            assert_eq!(
                fs::read_dir(&directory.0)
                    .unwrap()
                    .map(|entry| entry.unwrap().file_name())
                    .collect::<Vec<_>>(),
                names
            );
        }
    }
}
