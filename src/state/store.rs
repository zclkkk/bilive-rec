use std::path::Path;

use uuid::Uuid;

use crate::error::AppResult;
use crate::state::model::{LiveSession, Segment};

/// Summary of persisted state, returned by `StateStore::summary`.
#[derive(Debug)]
pub struct StateSummary {
    pub session_count: usize,
    pub segment_count: usize,
}

/// Durable state store backed by redb.
///
/// Placeholder for Phase 1. All methods return `not implemented` errors.
pub struct StateStore;

impl StateStore {
    pub fn open(_path: impl AsRef<Path>) -> AppResult<Self> {
        Err(crate::error::AppError::NotImplemented(
            "StateStore::open".to_string(),
        ))
    }

    pub fn init_schema(&self) -> AppResult<()> {
        Err(crate::error::AppError::NotImplemented(
            "StateStore::init_schema".to_string(),
        ))
    }

    pub fn schema_version(&self) -> AppResult<u32> {
        Err(crate::error::AppError::NotImplemented(
            "StateStore::schema_version".to_string(),
        ))
    }

    pub fn put_session(&self, _session: &LiveSession) -> AppResult<()> {
        Err(crate::error::AppError::NotImplemented(
            "StateStore::put_session".to_string(),
        ))
    }

    pub fn get_session(&self, _id: Uuid) -> AppResult<Option<LiveSession>> {
        Err(crate::error::AppError::NotImplemented(
            "StateStore::get_session".to_string(),
        ))
    }

    pub fn put_segment(&self, _segment: &Segment) -> AppResult<()> {
        Err(crate::error::AppError::NotImplemented(
            "StateStore::put_segment".to_string(),
        ))
    }

    pub fn list_segments(&self, _session_id: Uuid) -> AppResult<Vec<Segment>> {
        Err(crate::error::AppError::NotImplemented(
            "StateStore::list_segments".to_string(),
        ))
    }

    pub fn summary(&self) -> AppResult<StateSummary> {
        Err(crate::error::AppError::NotImplemented(
            "StateStore::summary".to_string(),
        ))
    }
}
