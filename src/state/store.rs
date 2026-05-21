use std::path::Path;

use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use uuid::Uuid;

use crate::error::{AppError, AppResult};
use crate::state::model::{LiveSession, Segment};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const SESSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("sessions");
const SEGMENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("segments");

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug)]
pub struct StateSummary {
    pub session_count: usize,
    pub segment_count: usize,
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
        Ok(StateSummary {
            session_count,
            segment_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::model::{LiveSession, Segment, SegmentStatus, SessionStatus};
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
        let seg0 = Segment {
            session_id,
            index: 0,
            path: PathBuf::from("/tmp/seg0.flv"),
            status: SegmentStatus::Finalized,
            error: None,
        };
        let seg1 = Segment {
            session_id,
            index: 1,
            path: PathBuf::from("/tmp/seg1.flv"),
            status: SegmentStatus::Recording,
            error: None,
        };
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
        };
        store.put_session(&session).unwrap();

        let seg = Segment {
            session_id: session.id,
            index: 0,
            path: PathBuf::from("/tmp/a.flv"),
            status: SegmentStatus::Finalized,
            error: None,
        };
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
            let seg = Segment {
                session_id,
                index,
                path: PathBuf::from(format!("/tmp/seg{index}.flv")),
                status: SegmentStatus::Finalized,
                error: None,
            };
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
}
