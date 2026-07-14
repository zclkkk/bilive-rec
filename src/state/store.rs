use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use uuid::Uuid;

use crate::error::{AppError, AppResult};
use crate::state::model::{
    LiveSession, RoomState, Segment, Submission, UploadTarget, UploadTargetState,
};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const SESSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("sessions");
const SEGMENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("segments");
const SUBMISSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("submissions");
const ROOM_STATES: TableDefinition<u64, &[u8]> = TableDefinition::new("room_states");
const UPLOAD_TARGET_STATES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("upload_target_states");

pub const STATE_FORMAT_ID: &str = "bilive-rec-state";
const SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateSummary {
    pub session_count: usize,
    pub segment_count: usize,
    pub submission_count: usize,
    pub room_count: usize,
    pub upload_target_count: usize,
}

pub(super) struct StoreSnapshot {
    pub(super) sessions: Vec<LiveSession>,
    pub(super) segments: Vec<Segment>,
    pub(super) submissions: Vec<Submission>,
    pub(super) room_states: Vec<(u64, RoomState)>,
    pub(super) upload_targets: Vec<UploadTargetState>,
}

pub struct StateStore {
    db: Database,
}

impl StateStore {
    /// Open the runtime state database, creating the directory and database on
    /// the first run. This is intentionally the only constructor that creates
    /// durable state.
    pub fn create_or_open(path: impl AsRef<Path>) -> AppResult<Self> {
        let path = path.as_ref();
        let is_new = !path.exists();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| AppError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let store = Self {
            db: Database::create(path)?,
        };
        if is_new {
            store.initialize_state_format()?;
        } else {
            store.validate_state_format()?;
        }
        Ok(store)
    }

    /// Open an existing database for inspection or operator recovery. Missing
    /// or mistyped paths are errors and never create a directory or database.
    pub fn open_existing(path: impl AsRef<Path>) -> AppResult<Self> {
        let path = path.as_ref();
        if !path.is_file() {
            return Err(AppError::State(format!(
                "state database does not exist or is not a regular file: {}",
                path.display()
            )));
        }
        let store = Self {
            db: Database::open(path)?,
        };
        store.validate_state_format()?;
        Ok(store)
    }

    fn initialize_state_format(&self) -> AppResult<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut meta = write_txn.open_table(META)?;
            meta.insert("format_id", STATE_FORMAT_ID.as_bytes())?;
            meta.insert("schema_version", SCHEMA_VERSION.to_be_bytes().as_slice())?;
            write_txn.open_table(SESSIONS)?;
            write_txn.open_table(SEGMENTS)?;
            write_txn.open_table(SUBMISSIONS)?;
            write_txn.open_table(ROOM_STATES)?;
            write_txn.open_table(UPLOAD_TARGET_STATES)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn validate_state_format(&self) -> AppResult<()> {
        let read_txn = self.db.begin_read()?;
        let meta = read_txn.open_table(META)?;
        let format_matches = meta
            .get("format_id")?
            .is_some_and(|value| value.value() == STATE_FORMAT_ID.as_bytes());
        if !format_matches {
            return Err(AppError::State(
                "unsupported state database format; remove old data before running 0.2.x".into(),
            ));
        }
        let version = meta
            .get("schema_version")?
            .map(|value| decode_schema_version(value.value()))
            .transpose()?
            .ok_or_else(|| AppError::State("state database has no schema version".into()))?;
        if version != SCHEMA_VERSION {
            return Err(AppError::State(format!(
                "unsupported state schema version {version}; expected {SCHEMA_VERSION}; remove old data before running 0.2.x"
            )));
        }
        read_txn.open_table(SESSIONS)?;
        read_txn.open_table(SEGMENTS)?;
        read_txn.open_table(SUBMISSIONS)?;
        read_txn.open_table(ROOM_STATES)?;
        read_txn.open_table(UPLOAD_TARGET_STATES)?;
        Ok(())
    }

    pub fn schema_version(&self) -> AppResult<u32> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(META)?;
        table
            .get("schema_version")?
            .map(|value| decode_schema_version(value.value()))
            .transpose()?
            .ok_or_else(|| AppError::State("state database has no schema version".into()))
    }

    pub(super) fn write<R>(
        &self,
        operation: impl FnOnce(&StoreTxn<'_>) -> AppResult<R>,
    ) -> AppResult<R> {
        let write_txn = self.db.begin_write()?;
        let result = operation(&StoreTxn { txn: &write_txn });
        match result {
            Ok(value) => {
                write_txn.commit()?;
                Ok(value)
            }
            Err(error) => {
                let _ = write_txn.abort();
                Err(error)
            }
        }
    }

    pub fn get_session(&self, id: Uuid) -> AppResult<Option<LiveSession>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SESSIONS)?;
        decode_optional(table.get(id.to_string().as_str())?, "session")
    }

    pub fn list_sessions(&self) -> AppResult<Vec<LiveSession>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SESSIONS)?;
        decode_all(&table, "session")
    }

    pub fn get_segment(&self, session_id: Uuid, index: u32) -> AppResult<Option<Segment>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SEGMENTS)?;
        decode_optional(
            table.get(segment_key(session_id, index).as_str())?,
            "segment",
        )
    }

    pub fn list_segments(&self, session_id: Uuid) -> AppResult<Vec<Segment>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SEGMENTS)?;
        decode_segments(&table, session_id)
    }

    pub fn list_all_segments(&self) -> AppResult<Vec<Segment>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SEGMENTS)?;
        decode_all(&table, "segment")
    }

    pub fn get_submission(&self, session_id: Uuid) -> AppResult<Option<Submission>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SUBMISSIONS)?;
        decode_optional(table.get(session_id.to_string().as_str())?, "submission")
    }

    pub fn list_submissions(&self) -> AppResult<Vec<Submission>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SUBMISSIONS)?;
        decode_all(&table, "submission")
    }

    pub fn get_room_state(&self, room_id: u64) -> AppResult<Option<RoomState>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(ROOM_STATES)?;
        decode_optional(table.get(room_id)?, "room state")
    }

    pub fn list_room_states(&self) -> AppResult<Vec<(u64, RoomState)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(ROOM_STATES)?;
        let mut states = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            states.push((key.value(), decode(value.value(), "room state")?));
        }
        Ok(states)
    }

    pub fn get_upload_target_state(
        &self,
        target: &UploadTarget,
    ) -> AppResult<Option<UploadTargetState>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(UPLOAD_TARGET_STATES)?;
        decode_optional(
            table.get(upload_target_key(target)?.as_str())?,
            "upload target state",
        )
    }

    pub fn list_upload_target_states(&self) -> AppResult<Vec<UploadTargetState>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(UPLOAD_TARGET_STATES)?;
        decode_all(&table, "upload target state")
    }

    pub fn summary(&self) -> AppResult<StateSummary> {
        let read_txn = self.db.begin_read()?;
        let session_count = read_txn.open_table(SESSIONS)?.len()? as usize;
        let segment_count = read_txn.open_table(SEGMENTS)?.len()? as usize;
        let submission_count = read_txn.open_table(SUBMISSIONS)?.len()? as usize;
        let room_count = read_txn.open_table(ROOM_STATES)?.len()? as usize;
        let upload_target_count = read_txn.open_table(UPLOAD_TARGET_STATES)?.len()? as usize;
        Ok(StateSummary {
            session_count,
            segment_count,
            submission_count,
            room_count,
            upload_target_count,
        })
    }

    /// Capture every business table from one redb read transaction. Status and
    /// anomaly detection must not combine rows read from different snapshots.
    pub(super) fn read_snapshot(&self) -> AppResult<StoreSnapshot> {
        let read_txn = self.db.begin_read()?;
        let sessions = decode_all(&read_txn.open_table(SESSIONS)?, "session")?;
        let segments = decode_all(&read_txn.open_table(SEGMENTS)?, "segment")?;
        let submissions = decode_all(&read_txn.open_table(SUBMISSIONS)?, "submission")?;
        let upload_targets = decode_all(
            &read_txn.open_table(UPLOAD_TARGET_STATES)?,
            "upload target state",
        )?;
        let room_table = read_txn.open_table(ROOM_STATES)?;
        let mut room_states = Vec::new();
        for entry in room_table.iter()? {
            let (key, value) = entry?;
            room_states.push((key.value(), decode(value.value(), "room state")?));
        }
        Ok(StoreSnapshot {
            sessions,
            segments,
            submissions,
            room_states,
            upload_targets,
        })
    }
}

pub(super) struct StoreTxn<'a> {
    txn: &'a redb::WriteTransaction,
}

impl StoreTxn<'_> {
    pub(super) fn put_session(&self, session: &LiveSession) -> AppResult<()> {
        put_json(
            &mut self.txn.open_table(SESSIONS)?,
            session.id.to_string().as_str(),
            session,
            "session",
        )
    }

    pub(super) fn get_session(&self, id: Uuid) -> AppResult<Option<LiveSession>> {
        let table = self.txn.open_table(SESSIONS)?;
        decode_optional(table.get(id.to_string().as_str())?, "session")
    }

    pub(super) fn list_sessions(&self) -> AppResult<Vec<LiveSession>> {
        decode_all(&self.txn.open_table(SESSIONS)?, "session")
    }

    pub(super) fn put_segment(&self, segment: &Segment) -> AppResult<()> {
        put_json(
            &mut self.txn.open_table(SEGMENTS)?,
            segment_key(segment.session_id, segment.index).as_str(),
            segment,
            "segment",
        )
    }

    pub(super) fn get_segment(&self, session_id: Uuid, index: u32) -> AppResult<Option<Segment>> {
        let table = self.txn.open_table(SEGMENTS)?;
        decode_optional(
            table.get(segment_key(session_id, index).as_str())?,
            "segment",
        )
    }

    pub(super) fn list_segments(&self, session_id: Uuid) -> AppResult<Vec<Segment>> {
        let table = self.txn.open_table(SEGMENTS)?;
        decode_segments(&table, session_id)
    }

    pub(super) fn put_submission(&self, submission: &Submission) -> AppResult<()> {
        put_json(
            &mut self.txn.open_table(SUBMISSIONS)?,
            submission.session_id.to_string().as_str(),
            submission,
            "submission",
        )
    }

    pub(super) fn get_submission(&self, session_id: Uuid) -> AppResult<Option<Submission>> {
        let table = self.txn.open_table(SUBMISSIONS)?;
        decode_optional(table.get(session_id.to_string().as_str())?, "submission")
    }

    pub(super) fn put_room_state(&self, room_id: u64, state: &RoomState) -> AppResult<()> {
        let value = serde_json::to_vec(state)
            .map_err(|error| AppError::State(format!("serialize room state: {error}")))?;
        self.txn
            .open_table(ROOM_STATES)?
            .insert(room_id, value.as_slice())?;
        Ok(())
    }

    pub(super) fn get_room_state(&self, room_id: u64) -> AppResult<Option<RoomState>> {
        let table = self.txn.open_table(ROOM_STATES)?;
        decode_optional(table.get(room_id)?, "room state")
    }

    pub(super) fn put_upload_target_state(&self, state: &UploadTargetState) -> AppResult<()> {
        put_json(
            &mut self.txn.open_table(UPLOAD_TARGET_STATES)?,
            upload_target_key(&state.target)?.as_str(),
            state,
            "upload target state",
        )
    }

    pub(super) fn get_upload_target_state(
        &self,
        target: &UploadTarget,
    ) -> AppResult<Option<UploadTargetState>> {
        let table = self.txn.open_table(UPLOAD_TARGET_STATES)?;
        decode_optional(
            table.get(upload_target_key(target)?.as_str())?,
            "upload target state",
        )
    }
}

fn segment_key(session_id: Uuid, index: u32) -> String {
    format!("{session_id}:{index:010}")
}

fn upload_target_key(target: &UploadTarget) -> AppResult<String> {
    serde_json::to_string(target)
        .map_err(|error| AppError::State(format!("serialize upload target key: {error}")))
}

fn put_json<T: serde::Serialize>(
    table: &mut redb::Table<'_, &str, &[u8]>,
    key: &str,
    value: &T,
    label: &str,
) -> AppResult<()> {
    let value = serde_json::to_vec(value)
        .map_err(|error| AppError::State(format!("serialize {label}: {error}")))?;
    table.insert(key, value.as_slice())?;
    Ok(())
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8], label: &str) -> AppResult<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| AppError::State(format!("deserialize {label}: {error}")))
}

fn decode_optional<T: serde::de::DeserializeOwned>(
    value: Option<redb::AccessGuard<'_, &[u8]>>,
    label: &str,
) -> AppResult<Option<T>> {
    value.map(|value| decode(value.value(), label)).transpose()
}

fn decode_all<T: serde::de::DeserializeOwned>(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    label: &str,
) -> AppResult<Vec<T>> {
    let mut values = Vec::new();
    for entry in table.iter()? {
        let (_, value) = entry?;
        values.push(decode(value.value(), label)?);
    }
    Ok(values)
}

fn decode_segments(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    session_id: Uuid,
) -> AppResult<Vec<Segment>> {
    let prefix = format!("{session_id}:");
    let mut segments = Vec::new();
    for entry in table.range(prefix.as_str()..)? {
        let (key, value) = entry?;
        if !key.value().starts_with(&prefix) {
            break;
        }
        segments.push(decode(value.value(), "segment")?);
    }
    Ok(segments)
}

fn decode_schema_version(bytes: &[u8]) -> AppResult<u32> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| AppError::State("invalid schema version bytes".into()))?;
    Ok(u32::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn create_or_open_creates_new_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nested/state.redb");
        let store = StateStore::create_or_open(&path).unwrap();
        assert!(path.is_file());
        assert_eq!(store.schema_version().unwrap(), 2);
        assert_eq!(
            store.summary().unwrap(),
            StateSummary {
                session_count: 0,
                segment_count: 0,
                submission_count: 0,
                room_count: 0,
                upload_target_count: 0,
            }
        );
    }

    #[test]
    fn open_existing_never_creates_missing_state() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("missing/state.redb");
        let error = match StateStore::open_existing(&path) {
            Ok(_) => panic!("missing database unexpectedly opened"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("does not exist"));
        assert!(!path.exists());
        assert!(!path.parent().unwrap().exists());
    }

    #[test]
    fn open_existing_reads_created_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        drop(StateStore::create_or_open(&path).unwrap());
        assert_eq!(
            StateStore::open_existing(path)
                .unwrap()
                .schema_version()
                .unwrap(),
            2
        );
    }
}
