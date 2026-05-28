use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use uuid::Uuid;

use crate::error::{AppError, AppResult};
use crate::pipeline::state_machine::PipelineState;
use crate::state::model::{LiveSession, RoomPipelineState, Segment, Submission, UploadedPart};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const SESSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("sessions");
const SEGMENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("segments");
const UPLOADED_PARTS: TableDefinition<&str, &[u8]> = TableDefinition::new("uploaded_parts");
const SUBMISSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("submissions");
const PIPELINE_STATES: TableDefinition<u64, &[u8]> = TableDefinition::new("pipeline_states");

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug)]
pub struct StateSummary {
    pub session_count: usize,
    pub segment_count: usize,
    pub uploaded_parts_count: usize,
    pub submission_count: usize,
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
            if meta.get("schema_version")?.is_none() {
                meta.insert("schema_version", SCHEMA_VERSION.to_be_bytes().as_slice())?;
            }
            write_txn.open_table(SESSIONS)?;
            write_txn.open_table(SEGMENTS)?;
            write_txn.open_table(UPLOADED_PARTS)?;
            write_txn.open_table(SUBMISSIONS)?;
            write_txn.open_table(PIPELINE_STATES)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn schema_version(&self) -> AppResult<u32> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(META)?;
        match table.get("schema_version")? {
            Some(v) => {
                let bytes = v.value();
                let arr: [u8; 4] = bytes
                    .try_into()
                    .map_err(|_| AppError::State("invalid schema version bytes".to_string()))?;
                Ok(u32::from_be_bytes(arr))
            }
            None => Ok(0),
        }
    }

    pub fn put_session(&self, session: &LiveSession) -> AppResult<()> {
        let key = session.id.to_string();
        let value = serde_json::to_vec(session)
            .map_err(|e| AppError::State(format!("serialize session: {e}")))?;
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(SESSIONS)?;
            table.insert(key.as_str(), value.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn put_session_and_pipeline_state(
        &self,
        session: &LiveSession,
        room_id: u64,
        state: PipelineState,
    ) -> AppResult<()> {
        let key = session.id.to_string();
        let session_value = serde_json::to_vec(session)
            .map_err(|e| AppError::State(format!("serialize session: {e}")))?;
        let room_state = RoomPipelineState {
            state,
            active_session_id: Some(session.id),
        };
        let state_value =
            serde_json::to_vec(&room_state).map_err(|e| AppError::State(e.to_string()))?;

        let write_txn = self.db.begin_write()?;
        {
            let mut sessions_table = write_txn.open_table(SESSIONS)?;
            sessions_table.insert(key.as_str(), session_value.as_slice())?;

            let mut states_table = write_txn.open_table(PIPELINE_STATES)?;
            states_table.insert(room_id, state_value.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn put_pipeline_state(&self, room_id: u64, state: PipelineState) -> AppResult<()> {
        self.put_room_pipeline_state(room_id, state, None)
    }

    pub fn put_room_pipeline_state(
        &self,
        room_id: u64,
        state: PipelineState,
        active_session_id: Option<Uuid>,
    ) -> AppResult<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(PIPELINE_STATES)?;
            let room_state = RoomPipelineState {
                state,
                active_session_id,
            };
            let value =
                serde_json::to_vec(&room_state).map_err(|e| AppError::State(e.to_string()))?;
            table.insert(room_id, value.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_pipeline_state(&self, room_id: u64) -> AppResult<Option<PipelineState>> {
        Ok(self
            .get_room_pipeline_state(room_id)?
            .map(|room_state| room_state.state))
    }

    pub fn get_room_pipeline_state(&self, room_id: u64) -> AppResult<Option<RoomPipelineState>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PIPELINE_STATES)?;
        match table.get(room_id)? {
            Some(v) => {
                let state = decode_room_pipeline_state(v.value())?;
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
        let key = format!("{}:{:010}", segment.session_id, segment.index);
        let value = serde_json::to_vec(segment)
            .map_err(|e| AppError::State(format!("serialize segment: {e}")))?;
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(SEGMENTS)?;
            table.insert(key.as_str(), value.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
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

    pub fn list_all_pipeline_states(&self) -> AppResult<Vec<(u64, PipelineState)>> {
        Ok(self
            .list_all_room_pipeline_states()?
            .into_iter()
            .map(|(room_id, state)| (room_id, state.state))
            .collect())
    }

    pub fn list_all_room_pipeline_states(&self) -> AppResult<Vec<(u64, RoomPipelineState)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PIPELINE_STATES)?;
        let mut states = Vec::new();
        for entry in table.iter()? {
            let (k, v) = entry?;
            let room_id = k.value();
            let state = decode_room_pipeline_state(v.value())?;
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
        Ok(StateSummary {
            session_count,
            segment_count,
            uploaded_parts_count,
            submission_count,
        })
    }

    pub fn put_uploaded_part(&self, part: &UploadedPart) -> AppResult<()> {
        let key = format!("{}:{:010}", part.session_id, part.segment_index);
        let value = serde_json::to_vec(part)
            .map_err(|e| AppError::State(format!("serialize uploaded part: {e}")))?;
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(UPLOADED_PARTS)?;
            table.insert(key.as_str(), value.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn put_submission(&self, submission: &Submission) -> AppResult<()> {
        let key = submission.session_id.to_string();
        let value = serde_json::to_vec(submission)
            .map_err(|e| AppError::State(format!("serialize submission: {e}")))?;
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(SUBMISSIONS)?;
            table.insert(key.as_str(), value.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
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
}

fn decode_room_pipeline_state(bytes: &[u8]) -> AppResult<RoomPipelineState> {
    if let Ok(state) = serde_json::from_slice::<RoomPipelineState>(bytes) {
        return Ok(state);
    }

    let legacy_state: PipelineState = serde_json::from_slice(bytes)
        .map_err(|e| AppError::State(format!("deserialize pipeline state: {e}")))?;
    Ok(RoomPipelineState {
        state: legacy_state,
        active_session_id: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::model::{
        LiveSession, SessionStatus,
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

    #[test]
    fn open_creates_database() {
        let (_store, dir) = test_store();
        assert!(dir.path().join("state.redb").exists());
    }

    #[test]
    fn schema_version_is_1() {
        let (store, _dir) = test_store();
        assert_eq!(store.schema_version().unwrap(), 1);
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
    fn room_pipeline_state_preserves_active_session_id() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_room_pipeline_state(42, PipelineState::Recording, Some(session_id))
            .unwrap();

        let loaded = store.get_room_pipeline_state(42).unwrap().unwrap();
        assert_eq!(loaded.state, PipelineState::Recording);
        assert_eq!(loaded.active_session_id, Some(session_id));
    }
}
