use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSession {
    pub id: Uuid,
    pub room_key: String,
    pub title: String,
    pub started_at: jiff::Timestamp,
    pub status: SessionStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub session_id: Uuid,
    pub index: u32,
    pub path: std::path::PathBuf,
    pub status: SegmentStatus,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadedPart {
    pub session_id: Uuid,
    pub segment_index: u32,
    pub bili_filename: String,
    pub part_title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Submission {
    pub session_id: Uuid,
    pub status: SubmissionStatus,
    #[serde(default)]
    pub aid: Option<u64>,
    #[serde(default)]
    pub bvid: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionStatus {
    Recording,
    Finalized,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SegmentStatus {
    Recording,
    Finalized,
    Filtered,
    Uploading,
    Uploaded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SubmissionStatus {
    Pending,
    Submitted,
    Failed,
}
