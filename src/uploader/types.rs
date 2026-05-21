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

#[derive(Debug, Clone)]
pub struct SubmissionResult {
    pub aid: Option<u64>,
    pub bvid: Option<String>,
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
    ) -> impl std::future::Future<Output = AppResult<SubmissionResult>> + Send;
}
