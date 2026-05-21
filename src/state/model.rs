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

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::Timestamp;
    use std::path::PathBuf;

    #[test]
    fn session_status_serde_roundtrip() {
        for status in [
            SessionStatus::Recording,
            SessionStatus::Finalized,
            SessionStatus::Failed,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let decoded: SessionStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, status);
        }
    }

    #[test]
    fn segment_status_serde_roundtrip() {
        let variants = [
            SegmentStatus::Recording,
            SegmentStatus::Finalized,
            SegmentStatus::Filtered,
            SegmentStatus::Uploading,
            SegmentStatus::Uploaded,
            SegmentStatus::Failed,
        ];
        for status in variants {
            let json = serde_json::to_string(&status).unwrap();
            let decoded: SegmentStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, status);
        }
    }

    #[test]
    fn submission_status_serde_roundtrip() {
        for status in [
            SubmissionStatus::Pending,
            SubmissionStatus::Submitted,
            SubmissionStatus::Failed,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let decoded: SubmissionStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, status);
        }
    }

    #[test]
    fn live_session_serde_roundtrip() {
        let session = LiveSession {
            id: Uuid::new_v4(),
            room_key: "12345".to_string(),
            title: "Test".to_string(),
            started_at: Timestamp::now(),
            status: SessionStatus::Recording,
        };
        let json = serde_json::to_string(&session).unwrap();
        let decoded: LiveSession = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.id, session.id);
        assert_eq!(decoded.status, SessionStatus::Recording);
    }

    #[test]
    fn segment_serde_roundtrip() {
        let seg = Segment {
            session_id: Uuid::new_v4(),
            index: 0,
            path: PathBuf::from("/tmp/test.flv"),
            status: SegmentStatus::Finalized,
            error: Some("test error".to_string()),
        };
        let json = serde_json::to_string(&seg).unwrap();
        let decoded: Segment = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.index, 0);
        assert_eq!(decoded.error.as_deref(), Some("test error"));
    }
}
