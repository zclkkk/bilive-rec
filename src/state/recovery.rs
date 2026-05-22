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

/// A safe recovery action that can be applied to fix persisted state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryAction {
    /// Mark a SegmentStatus::Recording segment as SegmentStatus::Failed
    /// with error "Interrupted by hard crash". Leaves .part file on disk.
    MarkInterruptedSegment { session_id: uuid::Uuid, index: u32 },
    /// Reset a room's pipeline from PipelineState::Failed to PipelineState::Idle.
    /// Only through explicit --reset-room flag.
    ResetRoomPipeline { room_id: u64 },
    /// Re-upload a Finalized segment that has no UploadedPart.
    /// Only for Finalized segments with existing .flv paths.
    ScheduleUploadReconciliation {
        session_id: uuid::Uuid,
        segment_index: u32,
        path: std::path::PathBuf,
    },
    /// Refused or ambiguous action with explanation.
    LeaveAsIs { reason: String },
}

impl std::fmt::Display for RecoveryAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecoveryAction::MarkInterruptedSegment { session_id, index } => write!(
                f,
                "Would mark segment {}/{} as Failed: Interrupted by hard crash",
                session_id, index
            ),
            RecoveryAction::ResetRoomPipeline { room_id } => {
                write!(
                    f,
                    "Would reset room {} pipeline from Failed to Idle",
                    room_id
                )
            }
            RecoveryAction::ScheduleUploadReconciliation {
                session_id,
                segment_index,
                path,
            } => write!(
                f,
                "Would re-upload finalized segment {}/{}: {}",
                session_id,
                segment_index,
                path.display()
            ),
            RecoveryAction::LeaveAsIs { reason } => {
                write!(f, "Would leave unchanged: {}", reason)
            }
        }
    }
}

/// A plan of recovery actions derived from persisted state.
#[derive(Debug, Clone)]
pub struct RecoveryPlan {
    pub actions: Vec<RecoveryAction>,
}

impl RecoveryPlan {
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }
}

impl std::fmt::Display for RecoveryPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.actions.is_empty() {
            return write!(f, "No recovery actions needed.");
        }
        for (i, action) in self.actions.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{}", action)?;
        }
        Ok(())
    }
}

/// Build a recovery plan from persisted state.
///
/// This function is read-only and does not mutate any state.
/// It produces a plan of safe, idempotent recovery actions that can be
/// applied with `apply_recovery` when the user passes `--apply`.
pub fn plan_recovery(store: &StateStore) -> AppResult<RecoveryPlan> {
    let mut actions = Vec::new();

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

    // 1. Interrupted segments: Recording status with no active pipeline
    for segment in &segments {
        if segment.status == SegmentStatus::Recording {
            let room_recording = session_room_key
                .get(&segment.session_id)
                .and_then(|room_key| room_key.parse::<u64>().ok())
                .and_then(|room_id| pipeline_by_room.get(&room_id))
                .is_some_and(is_recording_pipeline_state);

            if !room_recording {
                actions.push(RecoveryAction::MarkInterruptedSegment {
                    session_id: segment.session_id,
                    index: segment.index,
                });
            }
        }
    }

    // 2. Finalized segments missing upload (only if file exists and is .flv)
    for segment in &segments {
        if segment.status == SegmentStatus::Finalized {
            let has_upload = uploaded_by_session
                .get(&segment.session_id)
                .map(|indices| indices.contains(&segment.index))
                .unwrap_or(false);

            if !has_upload {
                // Only schedule upload if the file exists on disk and is not a .part
                if segment.path.exists()
                    && segment
                        .path
                        .extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("flv"))
                {
                    actions.push(RecoveryAction::ScheduleUploadReconciliation {
                        session_id: segment.session_id,
                        segment_index: segment.index,
                        path: segment.path.clone(),
                    });
                } else if !segment.path.exists() {
                    actions.push(RecoveryAction::LeaveAsIs {
                        reason: format!(
                            "Finalized segment {}/{} missing upload, but file does not exist: {}",
                            segment.session_id,
                            segment.index,
                            segment.path.display()
                        ),
                    });
                } else {
                    actions.push(RecoveryAction::LeaveAsIs {
                        reason: format!(
                            "Finalized segment {}/{} missing upload, but file is not a .flv: {}",
                            segment.session_id,
                            segment.index,
                            segment.path.display()
                        ),
                    });
                }
            }
        }
    }

    // 3. Pending submissions — ambiguous, must not auto-retry
    for submission in &submissions {
        if submission.status == SubmissionStatus::Pending {
            actions.push(RecoveryAction::LeaveAsIs {
                reason: format!(
                    "Pending submission {} — outcome unknown, requires manual verification",
                    submission.session_id
                ),
            });
        }
    }

    // 4. Failed submissions — must not auto-retry
    for submission in &submissions {
        if submission.status == SubmissionStatus::Failed {
            actions.push(RecoveryAction::LeaveAsIs {
                reason: format!(
                    "Failed submission {} — requires manual verification on Bilibili before retry",
                    submission.session_id
                ),
            });
        }
    }

    // 5. Failed pipeline states — require explicit --reset-room
    for (room_id, state) in &pipeline_by_room {
        if *state == PipelineState::Failed {
            actions.push(RecoveryAction::LeaveAsIs {
                reason: format!(
                    "Room {} pipeline stuck in Failed — use --reset-room {} to reset",
                    room_id, room_id
                ),
            });
        }
    }

    Ok(RecoveryPlan { actions })
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

    // --- plan_recovery tests ---

    #[test]
    fn plan_empty_state() {
        let (store, _dir) = test_store();
        let plan = plan_recovery(&store).unwrap();
        assert!(plan.is_empty());
    }

    #[test]
    fn plan_interrupted_segment() {
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
                index: 3,
                path: part_path,
                status: SegmentStatus::Recording,
                error: None,
            })
            .unwrap();

        let plan = plan_recovery(&store).unwrap();
        assert_eq!(plan.actions.len(), 1);
        assert_eq!(
            plan.actions[0],
            RecoveryAction::MarkInterruptedSegment {
                session_id,
                index: 3,
            }
        );
    }

    #[test]
    fn plan_active_recording_segment_skipped() {
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

        let plan = plan_recovery(&store).unwrap();
        assert!(plan.actions.is_empty());
    }

    #[test]
    fn plan_finalized_missing_upload_file_exists() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let flv_path = dir.path().join("seg.flv");
        std::fs::write(&flv_path, b"fake flv data").unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: flv_path.clone(),
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        let plan = plan_recovery(&store).unwrap();
        assert_eq!(plan.actions.len(), 1);
        assert_eq!(
            plan.actions[0],
            RecoveryAction::ScheduleUploadReconciliation {
                session_id,
                segment_index: 1,
                path: flv_path,
            }
        );
    }

    #[test]
    fn plan_finalized_missing_upload_file_missing() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_segment(&Segment {
                session_id,
                index: 2,
                path: PathBuf::from("/nonexistent/seg.flv"),
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        let plan = plan_recovery(&store).unwrap();
        assert_eq!(plan.actions.len(), 1);
        assert!(matches!(plan.actions[0], RecoveryAction::LeaveAsIs { .. }));
        if let RecoveryAction::LeaveAsIs { reason } = &plan.actions[0] {
            assert!(reason.contains("file does not exist"));
        }
    }

    #[test]
    fn plan_finalized_missing_upload_part_file() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let part_path = dir.path().join("seg.part");
        std::fs::write(&part_path, b"fake part data").unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 3,
                path: part_path,
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        let plan = plan_recovery(&store).unwrap();
        assert_eq!(plan.actions.len(), 1);
        assert!(matches!(plan.actions[0], RecoveryAction::LeaveAsIs { .. }));
        if let RecoveryAction::LeaveAsIs { reason } = &plan.actions[0] {
            assert!(reason.contains("not a .flv"));
        }
    }

    #[test]
    fn plan_finalized_with_upload_no_action() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let flv_path = dir.path().join("seg.flv");
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
                bili_filename: "seg.flv".to_string(),
                part_title: "Part 0".to_string(),
            })
            .unwrap();

        let plan = plan_recovery(&store).unwrap();
        assert!(plan.actions.is_empty());
    }

    #[test]
    fn plan_pending_submission_leave_as_is() {
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

        let plan = plan_recovery(&store).unwrap();
        assert_eq!(plan.actions.len(), 1);
        assert!(matches!(plan.actions[0], RecoveryAction::LeaveAsIs { .. }));
        if let RecoveryAction::LeaveAsIs { reason } = &plan.actions[0] {
            assert!(reason.contains("Pending submission"));
        }
    }

    #[test]
    fn plan_failed_submission_leave_as_is() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_submission(&Submission {
                session_id,
                status: SubmissionStatus::Failed,
                aid: None,
                bvid: None,
                error: Some("timeout".to_string()),
            })
            .unwrap();

        let plan = plan_recovery(&store).unwrap();
        assert_eq!(plan.actions.len(), 1);
        assert!(matches!(plan.actions[0], RecoveryAction::LeaveAsIs { .. }));
        if let RecoveryAction::LeaveAsIs { reason } = &plan.actions[0] {
            assert!(reason.contains("manual verification"));
        }
    }

    #[test]
    fn plan_failed_pipeline_leave_as_is() {
        let (store, _dir) = test_store();

        store.put_pipeline_state(42, PipelineState::Failed).unwrap();

        let plan = plan_recovery(&store).unwrap();
        assert_eq!(plan.actions.len(), 1);
        assert!(matches!(plan.actions[0], RecoveryAction::LeaveAsIs { .. }));
        if let RecoveryAction::LeaveAsIs { reason } = &plan.actions[0] {
            assert!(reason.contains("--reset-room 42"));
        }
    }

    #[test]
    fn plan_display_format() {
        let plan = RecoveryPlan {
            actions: vec![
                RecoveryAction::MarkInterruptedSegment {
                    session_id: Uuid::nil(),
                    index: 1,
                },
                RecoveryAction::LeaveAsIs {
                    reason: "test reason".to_string(),
                },
            ],
        };
        let display = format!("{}", plan);
        assert!(display.contains("Would mark segment"));
        assert!(display.contains("Would leave unchanged: test reason"));
    }

    #[test]
    fn plan_display_empty() {
        let plan = RecoveryPlan { actions: vec![] };
        assert_eq!(format!("{}", plan), "No recovery actions needed.");
    }
}
