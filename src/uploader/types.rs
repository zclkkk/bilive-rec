use crate::error::AppResult;
use crate::state::model::UploadedPart;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct UploadRequest {
    pub session_id: Uuid,
    pub segment_index: u32,
    pub path: PathBuf,
    pub part_title: String,
}

#[derive(Debug, Clone)]
pub struct SubmissionRequest {
    pub title: String,
    pub description: String,
    pub tid: u16,
    pub copyright: u8,
    pub tags: Vec<String>,
    pub source: String,
    pub parts: Vec<UploadedPart>,
}

/// Outcome of a remote submission call.
///
/// Bilibili's submit API can answer in three ways:
///   - `Confirmed`: code=0 and at least one of aid/bvid is returned.
///   - `Ambiguous`: code=0 but neither aid nor bvid is returned. The remote
///     may have accepted the submission, but we cannot prove it locally
///     without a follow-up query — operators must verify on Bilibili and
///     resolve via `state resolve-submission`.
///   - `Err(...)`: a locally known failure or explicit Bilibili rejection.
///     Transport/response errors after the submit boundary must be mapped to
///     `Ambiguous`, because the remote side may already have accepted it.
///
/// Folding Ambiguous into Err would lie about the remote state; folding it
/// into Confirmed would silently lose the aid/bvid we need to navigate back
/// to the upload. So it gets its own arm.
#[derive(Debug, Clone)]
pub enum SubmissionOutcome {
    Confirmed {
        aid: Option<u64>,
        bvid: Option<String>,
    },
    Ambiguous {
        reason: String,
    },
}

pub trait Uploader: Send + Sync {
    fn check_login(&self) -> impl std::future::Future<Output = AppResult<()>> + Send;

    fn upload_segment(
        &self,
        req: UploadRequest,
    ) -> impl std::future::Future<Output = AppResult<UploadedPart>> + Send;

    fn submit(
        &self,
        req: SubmissionRequest,
    ) -> impl std::future::Future<Output = AppResult<SubmissionOutcome>> + Send;
}
