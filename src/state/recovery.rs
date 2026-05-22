use std::collections::{HashMap, HashSet};

use crate::error::AppResult;
use crate::pipeline::state_machine::PipelineState;
use crate::state::model::{SegmentStatus, SessionStatus, SubmissionStatus};
use crate::state::store::StateStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnomalyKind {
    /// A segment is stuck in Recording status (crash interrupted it)
    InterruptedSegment,
    /// A Finalized segment has no corresponding UploadedPart
    MissingUpload,
    /// A submission is stuck in Pending status (unknown outcome)
    PendingSubmission,
    /// A submission has Failed
    FailedSubmission,
    /// A room's pipeline is stuck in Failed state
    FailedPipeline,
    /// A segment references a file path that does not exist on disk
    MissingSegmentFile,
    /// A session is stuck in Recording status (crash interrupted it)
    InterruptedSession,
}

#[derive(Debug, Clone)]
pub struct Anomaly {
    pub kind: AnomalyKind,
    pub description: String,
}

/// Returns true if the pipeline state indicates the room is actively processing
/// (recording, reconnecting, uploading, or submitting).
fn is_active_pipeline_state(state: &PipelineState) -> bool {
    matches!(
        state,
        PipelineState::Recording
            | PipelineState::ReResolving
            | PipelineState::WaitingReconnect
            | PipelineState::Uploading
            | PipelineState::Submitting
    )
}

/// Returns true if the pipeline state supports having an active recording segment.
fn is_recording_pipeline_state(state: &PipelineState) -> bool {
    matches!(
        state,
        PipelineState::Recording | PipelineState::ReResolving | PipelineState::WaitingReconnect
    )
}

/// Detect anomalies in the persisted state.
///
/// This function is read-only and does not mutate any state.
/// It scans redb for inconsistent or stuck states and checks
/// whether segment file paths referenced in the database exist on disk.
pub fn detect_anomalies(store: &StateStore) -> AppResult<Vec<Anomaly>> {
    let mut anomalies = Vec::new();

    let sessions = store.list_all_sessions()?;
    let segments = store.list_all_segments()?;
    let uploaded_parts = store.list_all_uploaded_parts()?;
    let submissions = store.list_all_submissions()?;
    let pipeline_states = store.list_all_pipeline_states()?;

    // Build lookup: room_id -> PipelineState
    let pipeline_by_room: HashMap<u64, PipelineState> = pipeline_states.into_iter().collect();

    // Build lookup: session_id -> set of uploaded segment indices
    let mut uploaded_by_session: HashMap<uuid::Uuid, HashSet<u32>> = HashMap::new();
    for part in &uploaded_parts {
        uploaded_by_session
            .entry(part.session_id)
            .or_default()
            .insert(part.segment_index);
    }

    // Build lookup: session_id -> room_key for segment -> session mapping
    let session_room_key: HashMap<uuid::Uuid, &str> = sessions
        .iter()
        .map(|s| (s.id, s.room_key.as_str()))
        .collect();

    // Check sessions stuck in Recording
    for session in &sessions {
        if session.status == SessionStatus::Recording {
            // Check if the room's pipeline is actively processing
            let room_active = session
                .room_key
                .parse::<u64>()
                .ok()
                .and_then(|room_id| pipeline_by_room.get(&room_id))
                .is_some_and(is_active_pipeline_state);

            if !room_active {
                anomalies.push(Anomaly {
                    kind: AnomalyKind::InterruptedSession,
                    description: format!(
                        "Session {} (room {}) is stuck in Recording status — likely interrupted by crash",
                        session.id, session.room_key
                    ),
                });
            }
        }
    }

    // Check segments
    for segment in &segments {
        match segment.status {
            SegmentStatus::Recording => {
                // Check if the session's room pipeline is actively recording
                let room_recording = session_room_key
                    .get(&segment.session_id)
                    .and_then(|room_key| room_key.parse::<u64>().ok())
                    .and_then(|room_id| pipeline_by_room.get(&room_id))
                    .is_some_and(is_recording_pipeline_state);

                if !room_recording {
                    anomalies.push(Anomaly {
                        kind: AnomalyKind::InterruptedSegment,
                        description: format!(
                            "Segment {}/{} is stuck in Recording status — interrupted by crash, path: {}",
                            segment.session_id, segment.index, segment.path.display()
                        ),
                    });
                }
            }
            SegmentStatus::Finalized => {
                // Check if file exists on disk
                if !segment.path.exists() {
                    anomalies.push(Anomaly {
                        kind: AnomalyKind::MissingSegmentFile,
                        description: format!(
                            "Segment {}/{} references missing file: {}",
                            segment.session_id,
                            segment.index,
                            segment.path.display()
                        ),
                    });
                }

                // Check if UploadedPart exists
                let has_upload = uploaded_by_session
                    .get(&segment.session_id)
                    .map(|indices| indices.contains(&segment.index))
                    .unwrap_or(false);

                if !has_upload {
                    anomalies.push(Anomaly {
                        kind: AnomalyKind::MissingUpload,
                        description: format!(
                            "Segment {}/{} is Finalized but has no UploadedPart — needs upload reconciliation",
                            segment.session_id, segment.index
                        ),
                    });
                }
            }
            SegmentStatus::Failed => {
                // Failed segments are not anomalies — they're expected outcomes
            }
            SegmentStatus::Filtered => {
                // Filtered segments are not anomalies — they were intentionally filtered
            }
            SegmentStatus::Uploading | SegmentStatus::Uploaded => {
                // These are in-progress or completed states, not anomalies
            }
        }
    }

    // Check submissions
    for submission in &submissions {
        match submission.status {
            SubmissionStatus::Pending => {
                anomalies.push(Anomaly {
                    kind: AnomalyKind::PendingSubmission,
                    description: format!(
                        "Submission for session {} is Pending — outcome unknown, requires manual verification",
                        submission.session_id
                    ),
                });
            }
            SubmissionStatus::Failed => {
                let error_detail = submission.error.as_deref().unwrap_or("no error message");
                anomalies.push(Anomaly {
                    kind: AnomalyKind::FailedSubmission,
                    description: format!(
                        "Submission for session {} Failed: {}",
                        submission.session_id, error_detail
                    ),
                });
            }
            SubmissionStatus::Submitted => {
                // Not an anomaly
            }
        }
    }

    // Check pipeline states
    for (room_id, state) in &pipeline_by_room {
        if *state == PipelineState::Failed {
            anomalies.push(Anomaly {
                kind: AnomalyKind::FailedPipeline,
                description: format!(
                    "Room {} pipeline is stuck in Failed state — requires explicit reset",
                    room_id
                ),
            });
        }
    }

    Ok(anomalies)
}

pub fn recover(_store: &StateStore) -> AppResult<Vec<String>> {
    Ok(vec!["no recovery actions implemented yet".to_string()])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::model::{
        LiveSession, Segment, SegmentStatus, SessionStatus, Submission, SubmissionStatus,
        UploadedPart,
    };
    use jiff::Timestamp;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use uuid::Uuid;

    fn test_store() -> (StateStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();
        (store, dir)
    }

    #[test]
    fn detect_no_anomalies_in_empty_state() {
        let (store, _dir) = test_store();
        let anomalies = detect_anomalies(&store).unwrap();
        assert!(anomalies.is_empty());
    }

    #[test]
    fn detect_interrupted_session_no_pipeline() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();
        store
            .put_session(&LiveSession {
                id: session_id,
                room_key: "123".to_string(),
                title: "Test".to_string(),
                started_at: Timestamp::now(),
                status: SessionStatus::Recording,
            })
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert_eq!(anomalies.len(), 1);
        assert_eq!(anomalies[0].kind, AnomalyKind::InterruptedSession);
    }

    #[test]
    fn recording_session_with_active_pipeline_is_not_anomaly() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_session(&LiveSession {
                id: session_id,
                room_key: "456".to_string(),
                title: "Live".to_string(),
                started_at: Timestamp::now(),
                status: SessionStatus::Recording,
            })
            .unwrap();

        store
            .put_pipeline_state(456, PipelineState::Recording)
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            !anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::InterruptedSession)
        );
    }

    #[test]
    fn recording_session_with_re_resolving_pipeline_is_not_anomaly() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_session(&LiveSession {
                id: session_id,
                room_key: "789".to_string(),
                title: "Live".to_string(),
                started_at: Timestamp::now(),
                status: SessionStatus::Recording,
            })
            .unwrap();

        store
            .put_pipeline_state(789, PipelineState::ReResolving)
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            !anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::InterruptedSession)
        );
    }

    #[test]
    fn recording_session_with_failed_pipeline_is_anomaly() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_session(&LiveSession {
                id: session_id,
                room_key: "111".to_string(),
                title: "Live".to_string(),
                started_at: Timestamp::now(),
                status: SessionStatus::Recording,
            })
            .unwrap();

        store
            .put_pipeline_state(111, PipelineState::Failed)
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::InterruptedSession)
        );
    }

    #[test]
    fn recording_session_with_idle_pipeline_is_anomaly() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_session(&LiveSession {
                id: session_id,
                room_key: "222".to_string(),
                title: "Live".to_string(),
                started_at: Timestamp::now(),
                status: SessionStatus::Recording,
            })
            .unwrap();

        store.put_pipeline_state(222, PipelineState::Idle).unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::InterruptedSession)
        );
    }

    #[test]
    fn detect_interrupted_segment_no_pipeline() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let part_path = dir.path().join("test.part");

        store
            .put_session(&LiveSession {
                id: session_id,
                room_key: "999".to_string(),
                title: "Test".to_string(),
                started_at: Timestamp::now(),
                status: SessionStatus::Recording,
            })
            .unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: part_path,
                status: SegmentStatus::Recording,
                error: None,
            })
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::InterruptedSegment)
        );
    }

    #[test]
    fn recording_segment_with_active_pipeline_is_not_anomaly() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let part_path = dir.path().join("test.part");

        store
            .put_session(&LiveSession {
                id: session_id,
                room_key: "333".to_string(),
                title: "Live".to_string(),
                started_at: Timestamp::now(),
                status: SessionStatus::Recording,
            })
            .unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: part_path,
                status: SegmentStatus::Recording,
                error: None,
            })
            .unwrap();

        store
            .put_pipeline_state(333, PipelineState::Recording)
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            !anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::InterruptedSegment)
        );
    }

    #[test]
    fn recording_segment_with_failed_pipeline_is_anomaly() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let part_path = dir.path().join("test.part");

        store
            .put_session(&LiveSession {
                id: session_id,
                room_key: "444".to_string(),
                title: "Live".to_string(),
                started_at: Timestamp::now(),
                status: SessionStatus::Recording,
            })
            .unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: part_path,
                status: SegmentStatus::Recording,
                error: None,
            })
            .unwrap();

        store
            .put_pipeline_state(444, PipelineState::Failed)
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::InterruptedSegment)
        );
    }

    #[test]
    fn recording_segment_with_idle_pipeline_is_anomaly() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let part_path = dir.path().join("test.part");

        store
            .put_session(&LiveSession {
                id: session_id,
                room_key: "555".to_string(),
                title: "Live".to_string(),
                started_at: Timestamp::now(),
                status: SessionStatus::Recording,
            })
            .unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: part_path,
                status: SegmentStatus::Recording,
                error: None,
            })
            .unwrap();

        store.put_pipeline_state(555, PipelineState::Idle).unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::InterruptedSegment)
        );
    }

    #[test]
    fn detect_finalized_missing_upload() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let flv_path = dir.path().join("test.flv");
        std::fs::write(&flv_path, b"fake").unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: flv_path,
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::MissingUpload)
        );
    }

    #[test]
    fn detect_finalized_with_upload_is_not_anomaly() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let flv_path = dir.path().join("test.flv");
        std::fs::write(&flv_path, b"fake").unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: flv_path,
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        store
            .put_uploaded_part(&UploadedPart {
                session_id,
                segment_index: 0,
                bili_filename: "test.flv".to_string(),
                part_title: "Part 0".to_string(),
            })
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            !anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::MissingUpload)
        );
    }

    #[test]
    fn detect_missing_segment_file() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: PathBuf::from("/nonexistent/path/test.flv"),
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        store
            .put_uploaded_part(&UploadedPart {
                session_id,
                segment_index: 0,
                bili_filename: "test.flv".to_string(),
                part_title: "Part 0".to_string(),
            })
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::MissingSegmentFile)
        );
    }

    #[test]
    fn detect_pending_submission() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_submission(&Submission {
                session_id,
                status: SubmissionStatus::Pending,
                aid: None,
                bvid: None,
                error: None,
            })
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::PendingSubmission)
        );
    }

    #[test]
    fn detect_failed_submission() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_submission(&Submission {
                session_id,
                status: SubmissionStatus::Failed,
                aid: None,
                bvid: None,
                error: Some("network error".to_string()),
            })
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::FailedSubmission)
        );
    }

    #[test]
    fn detect_failed_pipeline() {
        let (store, _dir) = test_store();

        store
            .put_pipeline_state(12345, PipelineState::Failed)
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::FailedPipeline)
        );
    }

    #[test]
    fn submitted_submission_is_not_anomaly() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_submission(&Submission {
                session_id,
                status: SubmissionStatus::Submitted,
                aid: Some(123),
                bvid: Some("BV123".to_string()),
                error: None,
            })
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(anomalies.is_empty());
    }
}
