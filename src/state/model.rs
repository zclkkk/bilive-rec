use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

use crate::credential::CredentialIdentity;
use crate::pipeline::state_machine::PipelineState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSession {
    pub id: Uuid,
    pub room_key: String,
    pub title: String,
    pub started_at: jiff::Timestamp,
    pub status: SessionStatus,
    #[serde(default)]
    pub record_credential: Option<CredentialIdentity>,
    #[serde(default)]
    pub upload_credential: Option<CredentialIdentity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub session_id: Uuid,
    pub index: u32,
    pub path: std::path::PathBuf,
    pub status: SegmentStatus,
    #[serde(default)]
    pub close_reason: Option<SegmentCloseReason>,
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
    pub upload_credential: CredentialIdentity,
    pub status: SubmissionStatus,
    #[serde(default)]
    pub aid: Option<u64>,
    #[serde(default)]
    pub bvid: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomPipelineState {
    pub state: PipelineState,
    #[serde(default)]
    pub active_session_id: Option<Uuid>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub last_error_at: Option<jiff::Timestamp>,
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
    Cleaned,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SegmentCloseReason {
    Rotation {
        triggers: Vec<SegmentRotationTrigger>,
    },
    StreamEnded,
    ConnectionDropped,
    IdleTimeout {
        seconds: u64,
    },
    GracefulShutdown,
    RepeatedMediaData,
}

impl fmt::Display for SegmentCloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rotation { triggers } => {
                write!(f, "rotation(")?;
                for (idx, trigger) in triggers.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{trigger}")?;
                }
                write!(f, ")")
            }
            Self::StreamEnded => write!(f, "stream_ended"),
            Self::ConnectionDropped => write!(f, "connection_dropped"),
            Self::IdleTimeout { seconds } => write!(f, "idle_timeout({seconds}s)"),
            Self::GracefulShutdown => write!(f, "graceful_shutdown"),
            Self::RepeatedMediaData => write!(f, "repeated_media_data"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SegmentRotationTrigger {
    HeaderChanged,
    SizeLimit { current_size: u64, limit: u64 },
    TimeLimit { elapsed_ms: u64, limit_ms: u64 },
}

impl fmt::Display for SegmentRotationTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HeaderChanged => write!(f, "header_changed"),
            Self::SizeLimit {
                current_size,
                limit,
            } => {
                write!(f, "size_limit(current={current_size}, limit={limit})")
            }
            Self::TimeLimit {
                elapsed_ms,
                limit_ms,
            } => {
                write!(
                    f,
                    "time_limit(elapsed={}ms, limit={}ms)",
                    elapsed_ms, limit_ms
                )
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SubmissionStatus {
    /// Submission has been initiated but not yet confirmed.
    Pending,
    /// Submission was confirmed by Bilibili (aid and/or bvid returned).
    Submitted,
    /// Bilibili accepted the submission (code=0) but did not return aid/bvid;
    /// the remote outcome is unknown and must be verified manually.
    Ambiguous,
    /// Submission failed and no remote artifact is expected.
    Failed,
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;
    use std::path::PathBuf;

    // -- LiveSession --

    pub(crate) fn recording_session(room_id: u64) -> LiveSession {
        session_with_status(room_id, SessionStatus::Recording)
    }

    pub(crate) fn session_with_status(room_id: u64, status: SessionStatus) -> LiveSession {
        LiveSession {
            id: Uuid::new_v4(),
            room_key: room_id.to_string(),
            title: format!("Test Room {room_id}"),
            started_at: jiff::Timestamp::now(),
            status,
            record_credential: None,
            upload_credential: None,
        }
    }

    // -- Submission --

    pub(crate) fn submission_with_status(session_id: Uuid, status: SubmissionStatus) -> Submission {
        Submission {
            session_id,
            upload_credential: crate::credential::CredentialIdentity::new("test", "cookies.json"),
            status,
            aid: None,
            bvid: None,
            error: None,
        }
    }

    pub(crate) fn pending_submission(session_id: Uuid) -> Submission {
        submission_with_status(session_id, SubmissionStatus::Pending)
    }

    pub(crate) fn submitted_submission(session_id: Uuid, aid: u64, bvid: &str) -> Submission {
        Submission {
            session_id,
            upload_credential: crate::credential::CredentialIdentity::new("test", "cookies.json"),
            status: SubmissionStatus::Submitted,
            aid: Some(aid),
            bvid: Some(bvid.to_string()),
            error: None,
        }
    }

    // -- Segment --

    fn segment(
        session_id: Uuid,
        index: u32,
        path: impl Into<PathBuf>,
        status: SegmentStatus,
    ) -> Segment {
        Segment {
            session_id,
            index,
            path: path.into(),
            status,
            close_reason: None,
            error: None,
        }
    }

    pub(crate) fn recording_segment(
        session_id: Uuid,
        index: u32,
        path: impl Into<PathBuf>,
    ) -> Segment {
        segment(session_id, index, path, SegmentStatus::Recording)
    }

    pub(crate) fn finalized_segment(
        session_id: Uuid,
        index: u32,
        path: impl Into<PathBuf>,
    ) -> Segment {
        segment(session_id, index, path, SegmentStatus::Finalized)
    }

    pub(crate) fn uploading_segment(
        session_id: Uuid,
        index: u32,
        path: impl Into<PathBuf>,
    ) -> Segment {
        segment(session_id, index, path, SegmentStatus::Uploading)
    }

    pub(crate) fn uploaded_segment(
        session_id: Uuid,
        index: u32,
        path: impl Into<PathBuf>,
    ) -> Segment {
        segment(session_id, index, path, SegmentStatus::Uploaded)
    }

    pub(crate) fn failed_segment(
        session_id: Uuid,
        index: u32,
        path: impl Into<PathBuf>,
        error: impl Into<String>,
    ) -> Segment {
        Segment {
            error: Some(error.into()),
            ..segment(session_id, index, path, SegmentStatus::Failed)
        }
    }
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
            SegmentStatus::Cleaned,
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
            SubmissionStatus::Ambiguous,
            SubmissionStatus::Failed,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let decoded: SubmissionStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, status);
        }
    }

    #[test]
    fn segment_close_reason_serde_roundtrip() {
        let reason = SegmentCloseReason::Rotation {
            triggers: vec![
                SegmentRotationTrigger::HeaderChanged,
                SegmentRotationTrigger::SizeLimit {
                    current_size: 2048,
                    limit: 1024,
                },
                SegmentRotationTrigger::TimeLimit {
                    elapsed_ms: 61_000,
                    limit_ms: 60_000,
                },
            ],
        };

        let json = serde_json::to_string(&reason).unwrap();
        let decoded: SegmentCloseReason = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, reason);
        assert_eq!(
            decoded.to_string(),
            "rotation(header_changed, size_limit(current=2048, limit=1024), time_limit(elapsed=61000ms, limit=60000ms))"
        );
    }

    #[test]
    fn live_session_serde_roundtrip() {
        let session = LiveSession {
            id: Uuid::new_v4(),
            room_key: "12345".to_string(),
            title: "Test".to_string(),
            started_at: Timestamp::now(),
            status: SessionStatus::Recording,
            record_credential: Some(CredentialIdentity::new("record", "record.json")),
            upload_credential: Some(CredentialIdentity::new("upload", "upload.json")),
        };
        let json = serde_json::to_string(&session).unwrap();
        let decoded: LiveSession = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.id, session.id);
        assert_eq!(decoded.status, SessionStatus::Recording);
        assert_eq!(decoded.record_credential, session.record_credential);
        assert_eq!(decoded.upload_credential, session.upload_credential);
    }

    #[test]
    fn segment_serde_roundtrip() {
        let seg = Segment {
            session_id: Uuid::new_v4(),
            index: 0,
            path: PathBuf::from("/tmp/test.flv"),
            status: SegmentStatus::Finalized,
            close_reason: Some(SegmentCloseReason::StreamEnded),
            error: Some("test error".to_string()),
        };
        let json = serde_json::to_string(&seg).unwrap();
        let decoded: Segment = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.index, 0);
        assert_eq!(decoded.close_reason, Some(SegmentCloseReason::StreamEnded));
        assert_eq!(decoded.error.as_deref(), Some("test error"));
    }
}
