use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use uuid::Uuid;

use crate::error::{AppError, AppResult};
use crate::pipeline::state_machine::RoomState;
use crate::state::model::{
    LiveSession, PersistedRoomState, Segment, SessionStatus, Submission, SubmissionPlan,
    UploadedPart,
};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const SESSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("sessions");
const SEGMENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("segments");
const UPLOADED_PARTS: TableDefinition<&str, &[u8]> = TableDefinition::new("uploaded_parts");
const SUBMISSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("submissions");
const SUBMISSION_PLANS: TableDefinition<&str, &[u8]> = TableDefinition::new("submission_plans");
const ROOM_STATES: TableDefinition<u64, &[u8]> = TableDefinition::new("room_states");

const SCHEMA_VERSION: u32 = 2;

#[derive(Debug)]
pub struct StateSummary {
    pub session_count: usize,
    pub segment_count: usize,
    pub uploaded_parts_count: usize,
    pub submission_count: usize,
    pub submission_plan_count: usize,
}

pub struct StateStore {
    db: Database,
}

impl StateStore {
    pub fn open(path: impl AsRef<Path>) -> AppResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| AppError::Io {
                path: parent.to_path_buf(),
                source: e,
            })?;
        }
        let db = Database::create(path)?;
        let store = Self { db };
        store.init_schema()?;
        Ok(store)
    }

    pub fn init_schema(&self) -> AppResult<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut meta = write_txn.open_table(META)?;
            let existing_version = {
                let existing = meta.get("schema_version")?;
                existing
                    .map(|v| decode_schema_version(v.value()))
                    .transpose()?
            };
            match existing_version {
                Some(existing) => {
                    if existing != SCHEMA_VERSION {
                        return Err(AppError::State(format!(
                            "unsupported state schema version {existing}; expected {SCHEMA_VERSION}"
                        )));
                    }
                }
                None => {
                    meta.insert("schema_version", SCHEMA_VERSION.to_be_bytes().as_slice())?;
                }
            }
            write_txn.open_table(SESSIONS)?;
            write_txn.open_table(SEGMENTS)?;
            write_txn.open_table(UPLOADED_PARTS)?;
            write_txn.open_table(SUBMISSIONS)?;
            write_txn.open_table(SUBMISSION_PLANS)?;
            write_txn.open_table(ROOM_STATES)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn schema_version(&self) -> AppResult<u32> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(META)?;
        match table.get("schema_version")? {
            Some(v) => decode_schema_version(v.value()),
            None => Ok(0),
        }
    }

    /// Run `f` inside a single write transaction. Every row write performed via
    /// the provided [`StoreTxn`] commits atomically; if `f` returns `Err` the
    /// transaction is aborted and nothing is persisted. This is how callers
    /// express "change these N rows together or not at all".
    pub fn write<R>(&self, f: impl FnOnce(&StoreTxn<'_>) -> AppResult<R>) -> AppResult<R> {
        let write_txn = self.db.begin_write()?;
        let result = {
            let store_txn = StoreTxn { txn: &write_txn };
            f(&store_txn)
        };
        match result {
            Ok(value) => {
                write_txn.commit()?;
                Ok(value)
            }
            Err(e) => {
                // Abort explicitly so the partial writes never reach disk; the
                // original error is what the caller needs to see.
                let _ = write_txn.abort();
                Err(e)
            }
        }
    }

    pub fn put_session(&self, session: &LiveSession) -> AppResult<()> {
        self.write(|txn| txn.put_session(session))
    }

    pub fn create_recording_session(
        &self,
        session: &LiveSession,
        submission_plan: &SubmissionPlan,
        room_id: u64,
    ) -> AppResult<()> {
        let room_key = room_id.to_string();
        if session.id != submission_plan.session_id {
            return Err(AppError::State(format!(
                "SubmissionPlan session_id {} does not match LiveSession {}",
                submission_plan.session_id, session.id
            )));
        }
        if session.status != SessionStatus::Recording {
            return Err(AppError::State(format!(
                "LiveSession {} is {:?}, not Recording; refusing to create recording session",
                session.id, session.status
            )));
        }
        if session.room_key != room_key {
            return Err(AppError::State(format!(
                "LiveSession {} belongs to room {}, not room {room_id}",
                session.id, session.room_key
            )));
        }
        if session.upload_credential.as_ref() != Some(&submission_plan.upload_credential) {
            return Err(AppError::State(format!(
                "LiveSession {} upload credential {:?} does not match SubmissionPlan credential {:?}",
                session.id, session.upload_credential, submission_plan.upload_credential
            )));
        }
        self.write(|txn| {
            txn.put_session(session)?;
            txn.put_submission_plan(submission_plan)?;
            txn.put_room_state(room_id, RoomState::Recording, Some(session.id), None)
        })
    }

    pub fn finalize_session_and_release_room(
        &self,
        room_id: u64,
        session_id: Uuid,
    ) -> AppResult<()> {
        let room_key = room_id.to_string();
        self.write(|txn| {
            let mut session = txn.get_session(session_id)?.ok_or_else(|| {
                AppError::State(format!("Session {session_id} not found when finalizing"))
            })?;
            if session.status != SessionStatus::Recording {
                return Err(AppError::State(format!(
                    "Session {session_id} is {:?}, not Recording; refusing to finalize",
                    session.status
                )));
            }
            if session.room_key != room_key {
                return Err(AppError::State(format!(
                    "Session {session_id} belongs to room {}, not room {room_id}; refusing to release room",
                    session.room_key
                )));
            }
            session.status = SessionStatus::Finalized;
            txn.put_session(&session)?;
            txn.put_room_state(room_id, RoomState::Idle, None, None)
        })
    }

    pub fn put_room_state_value(&self, room_id: u64, state: RoomState) -> AppResult<()> {
        self.write(|txn| txn.put_room_state(room_id, state, None, None))
    }

    pub fn put_room_state(
        &self,
        room_id: u64,
        state: RoomState,
        active_session_id: Option<Uuid>,
    ) -> AppResult<()> {
        self.write(|txn| txn.put_room_state(room_id, state, active_session_id, None))
    }

    pub fn put_room_state_with_error(
        &self,
        room_id: u64,
        state: RoomState,
        active_session_id: Option<Uuid>,
        last_error: String,
    ) -> AppResult<()> {
        self.write(|txn| txn.put_room_state(room_id, state, active_session_id, Some(last_error)))
    }

    pub fn get_room_state_value(&self, room_id: u64) -> AppResult<Option<RoomState>> {
        Ok(self
            .get_room_state(room_id)?
            .map(|room_state| room_state.state))
    }

    pub fn get_room_state(&self, room_id: u64) -> AppResult<Option<PersistedRoomState>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(ROOM_STATES)?;
        match table.get(room_id)? {
            Some(v) => {
                let state = decode_room_state(v.value())?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    pub fn get_session(&self, id: Uuid) -> AppResult<Option<LiveSession>> {
        let key = id.to_string();
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SESSIONS)?;
        match table.get(key.as_str())? {
            Some(v) => {
                let session: LiveSession = serde_json::from_slice(v.value())
                    .map_err(|e| AppError::State(format!("deserialize session: {e}")))?;
                Ok(Some(session))
            }
            None => Ok(None),
        }
    }

    pub fn put_segment(&self, segment: &Segment) -> AppResult<()> {
        self.write(|txn| txn.put_segment(segment))
    }

    pub fn get_segment(&self, session_id: Uuid, index: u32) -> AppResult<Option<Segment>> {
        let key = format!("{}:{:010}", session_id, index);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SEGMENTS)?;
        match table.get(key.as_str())? {
            Some(v) => {
                let segment: Segment = serde_json::from_slice(v.value())
                    .map_err(|e| AppError::State(format!("deserialize segment: {e}")))?;
                Ok(Some(segment))
            }
            None => Ok(None),
        }
    }

    pub fn list_segments(&self, session_id: Uuid) -> AppResult<Vec<Segment>> {
        let prefix = format!("{session_id}:");
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SEGMENTS)?;
        let mut segments = Vec::new();
        let iter = table.range(prefix.as_str()..)?;
        for entry in iter {
            let (key_guard, value_guard) = entry?;
            let key = key_guard.value();
            if !key.starts_with(&prefix) {
                break;
            }
            let segment: Segment = serde_json::from_slice(value_guard.value())
                .map_err(|e| AppError::State(format!("deserialize segment: {e}")))?;
            segments.push(segment);
        }
        Ok(segments)
    }

    pub fn list_all_sessions(&self) -> AppResult<Vec<LiveSession>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SESSIONS)?;
        let mut sessions = Vec::new();
        for entry in table.iter()? {
            let (_, v) = entry?;
            let session: LiveSession = serde_json::from_slice(v.value())
                .map_err(|e| AppError::State(format!("deserialize session: {e}")))?;
            sessions.push(session);
        }
        Ok(sessions)
    }

    pub fn list_all_segments(&self) -> AppResult<Vec<Segment>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SEGMENTS)?;
        let mut segments = Vec::new();
        for entry in table.iter()? {
            let (_, v) = entry?;
            let segment: Segment = serde_json::from_slice(v.value())
                .map_err(|e| AppError::State(format!("deserialize segment: {e}")))?;
            segments.push(segment);
        }
        Ok(segments)
    }

    pub fn list_all_uploaded_parts(&self) -> AppResult<Vec<UploadedPart>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(UPLOADED_PARTS)?;
        let mut parts = Vec::new();
        for entry in table.iter()? {
            let (_, v) = entry?;
            let part: UploadedPart = serde_json::from_slice(v.value())
                .map_err(|e| AppError::State(format!("deserialize uploaded part: {e}")))?;
            parts.push(part);
        }
        Ok(parts)
    }

    pub fn list_all_submissions(&self) -> AppResult<Vec<Submission>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SUBMISSIONS)?;
        let mut submissions = Vec::new();
        for entry in table.iter()? {
            let (_, v) = entry?;
            let submission: Submission = serde_json::from_slice(v.value())
                .map_err(|e| AppError::State(format!("deserialize submission: {e}")))?;
            submissions.push(submission);
        }
        Ok(submissions)
    }

    pub fn list_all_submission_plans(&self) -> AppResult<Vec<SubmissionPlan>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SUBMISSION_PLANS)?;
        let mut plans = Vec::new();
        for entry in table.iter()? {
            let (_, v) = entry?;
            let plan: SubmissionPlan = serde_json::from_slice(v.value())
                .map_err(|e| AppError::State(format!("deserialize submission plan: {e}")))?;
            plans.push(plan);
        }
        Ok(plans)
    }

    pub fn list_all_room_state_values(&self) -> AppResult<Vec<(u64, RoomState)>> {
        Ok(self
            .list_all_room_states()?
            .into_iter()
            .map(|(room_id, state)| (room_id, state.state))
            .collect())
    }

    pub fn list_all_room_states(&self) -> AppResult<Vec<(u64, PersistedRoomState)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(ROOM_STATES)?;
        let mut states = Vec::new();
        for entry in table.iter()? {
            let (k, v) = entry?;
            let room_id = k.value();
            let state = decode_room_state(v.value())?;
            states.push((room_id, state));
        }
        Ok(states)
    }

    pub fn summary(&self) -> AppResult<StateSummary> {
        let read_txn = self.db.begin_read()?;
        let session_count = {
            let table = read_txn.open_table(SESSIONS)?;
            table.len()? as usize
        };
        let segment_count = {
            let table = read_txn.open_table(SEGMENTS)?;
            table.len()? as usize
        };
        let uploaded_parts_count = {
            let table = read_txn.open_table(UPLOADED_PARTS)?;
            table.len()? as usize
        };
        let submission_count = {
            let table = read_txn.open_table(SUBMISSIONS)?;
            table.len()? as usize
        };
        let submission_plan_count = {
            let table = read_txn.open_table(SUBMISSION_PLANS)?;
            table.len()? as usize
        };
        Ok(StateSummary {
            session_count,
            segment_count,
            uploaded_parts_count,
            submission_count,
            submission_plan_count,
        })
    }

    pub fn put_uploaded_part(&self, part: &UploadedPart) -> AppResult<()> {
        self.write(|txn| txn.put_uploaded_part(part))
    }

    pub fn put_submission(&self, submission: &Submission) -> AppResult<()> {
        self.write(|txn| txn.put_submission(submission))
    }

    pub fn begin_submission(&self, submission: &Submission) -> AppResult<bool> {
        self.write(|txn| {
            if txn.get_submission(submission.session_id)?.is_some() {
                return Ok(false);
            }
            txn.put_submission(submission)?;
            Ok(true)
        })
    }

    pub fn put_submission_plan(&self, plan: &SubmissionPlan) -> AppResult<()> {
        self.write(|txn| txn.put_submission_plan(plan))
    }

    pub fn list_uploaded_parts(&self, session_id: Uuid) -> AppResult<Vec<UploadedPart>> {
        let prefix = format!("{session_id}:");
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(UPLOADED_PARTS)?;
        let mut parts = Vec::new();
        let iter = table.range(prefix.as_str()..)?;
        for entry in iter {
            let (key_guard, value_guard) = entry?;
            let key = key_guard.value();
            if !key.starts_with(&prefix) {
                break;
            }
            let part: UploadedPart = serde_json::from_slice(value_guard.value())
                .map_err(|e| AppError::State(format!("deserialize uploaded part: {e}")))?;
            parts.push(part);
        }
        Ok(parts)
    }

    pub fn get_submission(&self, session_id: Uuid) -> AppResult<Option<Submission>> {
        let key = session_id.to_string();
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SUBMISSIONS)?;
        match table.get(key.as_str())? {
            Some(v) => {
                let submission: Submission = serde_json::from_slice(v.value())
                    .map_err(|e| AppError::State(format!("deserialize submission: {e}")))?;
                Ok(Some(submission))
            }
            None => Ok(None),
        }
    }

    pub fn get_submission_plan(&self, session_id: Uuid) -> AppResult<Option<SubmissionPlan>> {
        let key = session_id.to_string();
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SUBMISSION_PLANS)?;
        match table.get(key.as_str())? {
            Some(v) => {
                let plan: SubmissionPlan = serde_json::from_slice(v.value())
                    .map_err(|e| AppError::State(format!("deserialize submission plan: {e}")))?;
                Ok(Some(plan))
            }
            None => Ok(None),
        }
    }
}

/// A handle to an in-progress write transaction, handed to the closure passed
/// to [`StateStore::write`]. Each method opens its table, inserts, and releases
/// it, so multiple calls compose into one atomic commit. Reads still go through
/// `StateStore` directly.
pub struct StoreTxn<'a> {
    txn: &'a redb::WriteTransaction,
}

impl StoreTxn<'_> {
    pub fn put_session(&self, session: &LiveSession) -> AppResult<()> {
        let key = session.id.to_string();
        let value = serde_json::to_vec(session)
            .map_err(|e| AppError::State(format!("serialize session: {e}")))?;
        let mut table = self.txn.open_table(SESSIONS)?;
        table.insert(key.as_str(), value.as_slice())?;
        Ok(())
    }

    pub fn put_segment(&self, segment: &Segment) -> AppResult<()> {
        let key = format!("{}:{:010}", segment.session_id, segment.index);
        let value = serde_json::to_vec(segment)
            .map_err(|e| AppError::State(format!("serialize segment: {e}")))?;
        let mut table = self.txn.open_table(SEGMENTS)?;
        table.insert(key.as_str(), value.as_slice())?;
        Ok(())
    }

    pub fn put_uploaded_part(&self, part: &UploadedPart) -> AppResult<()> {
        let key = format!("{}:{:010}", part.session_id, part.segment_index);
        let value = serde_json::to_vec(part)
            .map_err(|e| AppError::State(format!("serialize uploaded part: {e}")))?;
        let mut table = self.txn.open_table(UPLOADED_PARTS)?;
        table.insert(key.as_str(), value.as_slice())?;
        Ok(())
    }

    pub fn put_submission(&self, submission: &Submission) -> AppResult<()> {
        let key = submission.session_id.to_string();
        let value = serde_json::to_vec(submission)
            .map_err(|e| AppError::State(format!("serialize submission: {e}")))?;
        let mut table = self.txn.open_table(SUBMISSIONS)?;
        table.insert(key.as_str(), value.as_slice())?;
        Ok(())
    }

    pub fn put_submission_plan(&self, plan: &SubmissionPlan) -> AppResult<()> {
        let key = plan.session_id.to_string();
        let value = serde_json::to_vec(plan)
            .map_err(|e| AppError::State(format!("serialize submission plan: {e}")))?;
        let mut table = self.txn.open_table(SUBMISSION_PLANS)?;
        table.insert(key.as_str(), value.as_slice())?;
        Ok(())
    }

    pub fn put_room_state(
        &self,
        room_id: u64,
        state: RoomState,
        active_session_id: Option<Uuid>,
        last_error: Option<String>,
    ) -> AppResult<()> {
        validate_room_state_shape(state, active_session_id)?;
        let room_state = PersistedRoomState {
            state,
            active_session_id,
            last_error_at: last_error.as_ref().map(|_| jiff::Timestamp::now()),
            last_error,
        };
        let value = serde_json::to_vec(&room_state).map_err(|e| AppError::State(e.to_string()))?;
        let mut table = self.txn.open_table(ROOM_STATES)?;
        table.insert(room_id, value.as_slice())?;
        Ok(())
    }

    pub fn get_session(&self, id: Uuid) -> AppResult<Option<LiveSession>> {
        let key = id.to_string();
        let table = self.txn.open_table(SESSIONS)?;
        match table.get(key.as_str())? {
            Some(v) => {
                let session: LiveSession = serde_json::from_slice(v.value())
                    .map_err(|e| AppError::State(format!("deserialize session: {e}")))?;
                Ok(Some(session))
            }
            None => Ok(None),
        }
    }

    pub fn get_submission(&self, session_id: Uuid) -> AppResult<Option<Submission>> {
        let key = session_id.to_string();
        let table = self.txn.open_table(SUBMISSIONS)?;
        match table.get(key.as_str())? {
            Some(v) => {
                let submission: Submission = serde_json::from_slice(v.value())
                    .map_err(|e| AppError::State(format!("deserialize submission: {e}")))?;
                Ok(Some(submission))
            }
            None => Ok(None),
        }
    }

    pub fn get_segment(&self, session_id: Uuid, index: u32) -> AppResult<Option<Segment>> {
        let key = format!("{}:{:010}", session_id, index);
        let table = self.txn.open_table(SEGMENTS)?;
        match table.get(key.as_str())? {
            Some(v) => {
                let segment: Segment = serde_json::from_slice(v.value())
                    .map_err(|e| AppError::State(format!("deserialize segment: {e}")))?;
                Ok(Some(segment))
            }
            None => Ok(None),
        }
    }
}

fn decode_room_state(bytes: &[u8]) -> AppResult<PersistedRoomState> {
    let room_state = serde_json::from_slice::<PersistedRoomState>(bytes)
        .map_err(|e| AppError::State(format!("deserialize room state: {e}")))?;
    validate_room_state_shape(room_state.state, room_state.active_session_id)?;
    Ok(room_state)
}

fn decode_schema_version(bytes: &[u8]) -> AppResult<u32> {
    let arr: [u8; 4] = bytes
        .try_into()
        .map_err(|_| AppError::State("invalid schema version bytes".to_string()))?;
    Ok(u32::from_be_bytes(arr))
}

fn validate_room_state_shape(state: RoomState, active_session_id: Option<Uuid>) -> AppResult<()> {
    match (state.requires_active_session(), active_session_id) {
        (true, None) => Err(AppError::State(format!(
            "room state {state:?} requires active_session_id"
        ))),
        (false, Some(session_id)) => Err(AppError::State(format!(
            "room state {state:?} must not carry active_session_id {session_id}"
        ))),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Copyright, SubmitApi};
    use crate::credential::CredentialIdentity;
    use crate::state::model::{
        LiveSession, SessionStatus, SubmissionPlan,
        fixtures::{finalized_segment, recording_segment},
    };
    use jiff::Timestamp;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn test_store() -> (StateStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();
        (store, dir)
    }

    fn test_credential(name: &str) -> CredentialIdentity {
        CredentialIdentity::new(name, format!("data/{name}.json"))
    }

    fn recording_session(room_id: u64, upload_credential: CredentialIdentity) -> LiveSession {
        LiveSession {
            id: Uuid::new_v4(),
            room_key: room_id.to_string(),
            title: "t".to_string(),
            started_at: Timestamp::now(),
            status: SessionStatus::Recording,
            record_credential: None,
            upload_credential: Some(upload_credential),
        }
    }

    fn submission_plan(session_id: Uuid, upload_credential: CredentialIdentity) -> SubmissionPlan {
        SubmissionPlan {
            session_id,
            upload_credential,
            submit_api: SubmitApi::App,
            title: "t".to_string(),
            description: String::new(),
            category_id: 171,
            copyright: Copyright::Reprint,
            source: "直播录像".to_string(),
            tags: vec!["直播录像".to_string()],
            private: false,
            dynamic: String::new(),
            forbid_reprint: false,
            charging_panel: false,
            close_reply: false,
            close_danmu: false,
            featured_reply: false,
            delete_after_submit: false,
        }
    }

    #[test]
    fn open_creates_database() {
        let (_store, dir) = test_store();
        assert!(dir.path().join("state.redb").exists());
    }

    #[test]
    fn schema_version_is_2() {
        let (store, _dir) = test_store();
        assert_eq!(store.schema_version().unwrap(), 2);
    }

    #[test]
    fn open_rejects_unsupported_schema_version() {
        let (store, dir) = test_store();
        let write_txn = store.db.begin_write().unwrap();
        {
            let mut meta = write_txn.open_table(META).unwrap();
            meta.insert("schema_version", 1_u32.to_be_bytes().as_slice())
                .unwrap();
        }
        write_txn.commit().unwrap();
        drop(store);

        let err = match StateStore::open(dir.path().join("state.redb")) {
            Ok(_) => panic!("expected unsupported schema error"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("unsupported state schema version"));
    }

    #[test]
    fn put_and_get_session() {
        let (store, _dir) = test_store();
        let session = LiveSession {
            id: Uuid::new_v4(),
            room_key: "12345".to_string(),
            title: "Test Stream".to_string(),
            started_at: Timestamp::now(),
            status: SessionStatus::Recording,
            record_credential: None,
            upload_credential: None,
        };
        store.put_session(&session).unwrap();
        let loaded = store.get_session(session.id).unwrap().unwrap();
        assert_eq!(loaded.id, session.id);
        assert_eq!(loaded.room_key, session.room_key);
        assert_eq!(loaded.title, session.title);
        assert_eq!(loaded.status, SessionStatus::Recording);
    }

    #[test]
    fn get_nonexistent_session_returns_none() {
        let (store, _dir) = test_store();
        assert!(store.get_session(Uuid::new_v4()).unwrap().is_none());
    }

    #[test]
    fn put_and_list_segments() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();
        let seg0 = finalized_segment(session_id, 0, PathBuf::from("/tmp/seg0.flv"));
        let seg1 = recording_segment(session_id, 1, PathBuf::from("/tmp/seg1.flv"));
        store.put_segment(&seg0).unwrap();
        store.put_segment(&seg1).unwrap();

        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].index, 0);
        assert_eq!(segments[1].index, 1);
    }

    #[test]
    fn list_segments_for_empty_session() {
        let (store, _dir) = test_store();
        let segments = store.list_segments(Uuid::new_v4()).unwrap();
        assert!(segments.is_empty());
    }

    #[test]
    fn summary_counts() {
        let (store, _dir) = test_store();
        let s = store.summary().unwrap();
        assert_eq!(s.session_count, 0);
        assert_eq!(s.segment_count, 0);

        let session = LiveSession {
            id: Uuid::new_v4(),
            room_key: "1".to_string(),
            title: "t".to_string(),
            started_at: Timestamp::now(),
            status: SessionStatus::Recording,
            record_credential: None,
            upload_credential: None,
        };
        store.put_session(&session).unwrap();

        let seg = finalized_segment(session.id, 0, PathBuf::from("/tmp/a.flv"));
        store.put_segment(&seg).unwrap();

        let s = store.summary().unwrap();
        assert_eq!(s.session_count, 1);
        assert_eq!(s.segment_count, 1);
    }

    #[test]
    fn list_segments_returns_ascending_numeric_order() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        // Insert in reverse numeric order; lexicographic "10" < "2" would break naive sorting.
        for index in [10u32, 2] {
            let seg = finalized_segment(
                session_id,
                index,
                PathBuf::from(format!("/tmp/seg{index}.flv")),
            );
            store.put_segment(&seg).unwrap();
        }

        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].index, 2);
        assert_eq!(segments[1].index, 10);
    }

    #[test]
    fn reopen_preserves_state() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("state.redb");

        let session = LiveSession {
            id: Uuid::new_v4(),
            room_key: "reopen".to_string(),
            title: "persist".to_string(),
            started_at: Timestamp::now(),
            status: SessionStatus::Finalized,
            record_credential: None,
            upload_credential: None,
        };

        {
            let store = StateStore::open(&db_path).unwrap();
            store.put_session(&session).unwrap();
        }

        let store = StateStore::open(&db_path).unwrap();
        let loaded = store.get_session(session.id).unwrap().unwrap();
        assert_eq!(loaded.room_key, "reopen");
        assert_eq!(loaded.status, SessionStatus::Finalized);
    }

    #[test]
    fn put_and_list_uploaded_parts() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();
        let part = UploadedPart {
            session_id,
            segment_index: 0,
            bili_filename: "test.flv".to_string(),
            part_title: "Test Part".to_string(),
        };
        store.put_uploaded_part(&part).unwrap();
        let parts = store.list_uploaded_parts(session_id).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].bili_filename, "test.flv");
    }

    #[test]
    fn put_and_get_submission() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();
        let sub = crate::state::model::Submission {
            session_id,
            upload_credential: crate::credential::CredentialIdentity::new("test", "cookies.json"),
            status: crate::state::model::SubmissionStatus::Submitted,
            aid: Some(123),
            bvid: Some("BV123".to_string()),
            error: None,
        };
        store.put_submission(&sub).unwrap();
        let loaded = store.get_submission(session_id).unwrap().unwrap();
        assert_eq!(loaded.aid, Some(123));
    }

    #[test]
    fn room_state_preserves_active_session_id() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_room_state(42, RoomState::Recording, Some(session_id))
            .unwrap();

        let loaded = store.get_room_state(42).unwrap().unwrap();
        assert_eq!(loaded.state, RoomState::Recording);
        assert_eq!(loaded.active_session_id, Some(session_id));
    }

    #[test]
    fn room_state_requires_active_session_shape() {
        let (store, _dir) = test_store();

        let err = store
            .put_room_state(42, RoomState::Recording, None)
            .unwrap_err();
        assert!(err.to_string().contains("requires active_session_id"));

        let err = store
            .put_room_state(42, RoomState::Idle, Some(Uuid::new_v4()))
            .unwrap_err();
        assert!(err.to_string().contains("must not carry active_session_id"));
    }

    #[test]
    fn create_recording_session_requires_recording_status() {
        let (store, _dir) = test_store();
        let credential = test_credential("main");
        let mut session = recording_session(42, credential.clone());
        session.status = SessionStatus::Finalized;
        let plan = submission_plan(session.id, credential);

        let err = store
            .create_recording_session(&session, &plan, 42)
            .unwrap_err();

        assert!(err.to_string().contains("not Recording"));
        assert!(store.get_session(session.id).unwrap().is_none());
        assert!(store.get_submission_plan(session.id).unwrap().is_none());
        assert!(store.get_room_state(42).unwrap().is_none());
    }

    #[test]
    fn create_recording_session_requires_room_match() {
        let (store, _dir) = test_store();
        let credential = test_credential("main");
        let session = recording_session(43, credential.clone());
        let plan = submission_plan(session.id, credential);

        let err = store
            .create_recording_session(&session, &plan, 42)
            .unwrap_err();

        assert!(err.to_string().contains("belongs to room 43"));
        assert!(store.get_session(session.id).unwrap().is_none());
        assert!(store.get_submission_plan(session.id).unwrap().is_none());
        assert!(store.get_room_state(42).unwrap().is_none());
    }

    #[test]
    fn create_recording_session_requires_upload_credential_match() {
        let (store, _dir) = test_store();
        let session = recording_session(42, test_credential("session"));
        let plan = submission_plan(session.id, test_credential("plan"));

        let err = store
            .create_recording_session(&session, &plan, 42)
            .unwrap_err();

        assert!(err.to_string().contains("upload credential"));
        assert!(store.get_session(session.id).unwrap().is_none());
        assert!(store.get_submission_plan(session.id).unwrap().is_none());
        assert!(store.get_room_state(42).unwrap().is_none());
    }

    #[test]
    fn finalize_session_and_release_room_requires_room_match() {
        let (store, _dir) = test_store();
        let session = recording_session(43, test_credential("main"));
        store.put_session(&session).unwrap();
        store
            .put_room_state(42, RoomState::Recording, Some(session.id))
            .unwrap();

        let err = store
            .finalize_session_and_release_room(42, session.id)
            .unwrap_err();

        assert!(err.to_string().contains("belongs to room 43"));
        let loaded = store.get_session(session.id).unwrap().unwrap();
        assert_eq!(loaded.status, SessionStatus::Recording);
        let room_state = store.get_room_state(42).unwrap().unwrap();
        assert_eq!(room_state.state, RoomState::Recording);
        assert_eq!(room_state.active_session_id, Some(session.id));
    }

    #[test]
    fn write_commits_all_rows_atomically() {
        let (store, _dir) = test_store();
        let session = LiveSession {
            id: Uuid::new_v4(),
            room_key: "atomic".to_string(),
            title: "t".to_string(),
            started_at: Timestamp::now(),
            status: SessionStatus::Recording,
            record_credential: None,
            upload_credential: None,
        };
        let seg = finalized_segment(session.id, 0, PathBuf::from("/tmp/a.flv"));

        store
            .write(|txn| {
                txn.put_session(&session)?;
                txn.put_segment(&seg)?;
                txn.put_room_state(7, RoomState::Recording, Some(session.id), None)
            })
            .unwrap();

        assert!(store.get_session(session.id).unwrap().is_some());
        assert_eq!(store.list_segments(session.id).unwrap().len(), 1);
        assert_eq!(
            store.get_room_state_value(7).unwrap(),
            Some(RoomState::Recording)
        );
    }

    #[test]
    fn write_rolls_back_every_row_when_closure_errors() {
        let (store, _dir) = test_store();
        let session = LiveSession {
            id: Uuid::new_v4(),
            room_key: "rollback".to_string(),
            title: "t".to_string(),
            started_at: Timestamp::now(),
            status: SessionStatus::Recording,
            record_credential: None,
            upload_credential: None,
        };
        let seg = finalized_segment(session.id, 0, PathBuf::from("/tmp/a.flv"));

        let result = store.write(|txn| {
            // First two writes succeed within the transaction, but the closure
            // then refuses: nothing it wrote should survive the abort.
            txn.put_session(&session)?;
            txn.put_segment(&seg)?;
            Err::<(), _>(AppError::State("deliberate failure".to_string()))
        });

        assert!(result.is_err());
        assert!(store.get_session(session.id).unwrap().is_none());
        assert!(store.list_segments(session.id).unwrap().is_empty());
    }
}
