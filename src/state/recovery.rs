use std::collections::{HashMap, HashSet};

use crate::error::AppResult;
use crate::pipeline::state_machine::PipelineState;
use crate::state::model::{SegmentStatus, SessionStatus, SubmissionStatus};
use crate::state::store::StateStore;
use crate::uploader::types::{UploadRequest, Uploader};

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
    /// A pipeline is persisted in an active state after process exit
    ActivePipeline,
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
        match state {
            PipelineState::Failed => {
                anomalies.push(Anomaly {
                    kind: AnomalyKind::FailedPipeline,
                    description: format!(
                        "Room {} pipeline is stuck in Failed state — requires explicit reset",
                        room_id
                    ),
                });
            }
            PipelineState::Recording
            | PipelineState::Uploading
            | PipelineState::Submitting
            | PipelineState::ReResolving
            | PipelineState::WaitingReconnect => {
                anomalies.push(Anomaly {
                    kind: AnomalyKind::ActivePipeline,
                    description: format!(
                        "Room {} pipeline persisted in {:?} state — likely interrupted by process exit, resumable on restart",
                        room_id, state
                    ),
                });
            }
            PipelineState::Idle
            | PipelineState::Resolving
            | PipelineState::Offline
            | PipelineState::Submitted => {
                // Normal or terminal states — not anomalies
            }
        }
    }

    Ok(anomalies)
}

/// A safe recovery action that can be applied to fix persisted state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryAction {
    /// Mark a LiveSession stuck in Recording as SessionStatus::Failed
    /// with error "Interrupted by hard crash". Preserves existing data.
    MarkInterruptedSession { session_id: uuid::Uuid },
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
            RecoveryAction::MarkInterruptedSession { session_id } => write!(
                f,
                "Would mark session {} as Failed: Interrupted by hard crash",
                session_id
            ),
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

/// Check if the plan contains any ScheduleUploadReconciliation actions.
pub fn plan_has_upload_actions(plan: &RecoveryPlan) -> bool {
    plan.actions
        .iter()
        .any(|a| matches!(a, RecoveryAction::ScheduleUploadReconciliation { .. }))
}

/// Build a recovery plan from persisted state.
///
/// This function is read-only and does not mutate any state.
/// It produces a plan of safe, idempotent recovery actions that can be
/// applied with `apply_recovery` when the user passes `--apply`.
///
/// `reset_rooms` is the set of room IDs the user explicitly requested
/// to reset from Failed to Idle via `--reset-room`.
///
/// `retry_upload_sessions` is the set of session IDs the user explicitly
/// requested to re-upload finalized segments for via `--retry-upload`.
pub fn plan_recovery(
    store: &StateStore,
    reset_rooms: &HashSet<u64>,
    retry_upload_sessions: &HashSet<uuid::Uuid>,
) -> AppResult<RecoveryPlan> {
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

    // Build lookup: session_id -> Submission
    let submission_by_session: HashMap<uuid::Uuid, &crate::state::model::Submission> =
        submissions.iter().map(|s| (s.session_id, s)).collect();

    // 0. Interrupted sessions: Recording status with no active pipeline
    for session in &sessions {
        if session.status == SessionStatus::Recording {
            let room_active = session
                .room_key
                .parse::<u64>()
                .ok()
                .and_then(|room_id| pipeline_by_room.get(&room_id))
                .is_some_and(is_active_pipeline_state);

            if !room_active {
                actions.push(RecoveryAction::MarkInterruptedSession {
                    session_id: session.id,
                });
            }
        }
    }

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

    // 2. Finalized segments missing upload
    for segment in &segments {
        if segment.status == SegmentStatus::Finalized {
            let has_upload = uploaded_by_session
                .get(&segment.session_id)
                .map(|indices| indices.contains(&segment.index))
                .unwrap_or(false);

            if !has_upload {
                let session_requested = retry_upload_sessions.contains(&segment.session_id);

                if session_requested {
                    // Check submission boundary: refuse if session has a submission
                    if let Some(sub) = submission_by_session.get(&segment.session_id) {
                        let status_word = match sub.status {
                            SubmissionStatus::Submitted => "Submitted",
                            SubmissionStatus::Pending => "Pending",
                            SubmissionStatus::Failed => "Failed",
                        };
                        actions.push(RecoveryAction::LeaveAsIs {
                            reason: format!(
                                "Finalized segment {}/{} missing upload, but session has a {} submission — cannot retry upload",
                                segment.session_id, segment.index, status_word
                            ),
                        });
                        continue;
                    }

                    // Only schedule upload if the file exists on disk and is .flv
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
                } else {
                    actions.push(RecoveryAction::LeaveAsIs {
                        reason: format!(
                            "Finalized segment {}/{} missing upload — use --retry-upload {} --apply to upload",
                            segment.session_id, segment.index, segment.session_id
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
            if reset_rooms.contains(room_id) {
                actions.push(RecoveryAction::ResetRoomPipeline { room_id: *room_id });
            } else {
                actions.push(RecoveryAction::LeaveAsIs {
                    reason: format!(
                        "Room {} pipeline stuck in Failed — use --reset-room {} to reset",
                        room_id, room_id
                    ),
                });
            }
        }
    }

    // 6. Active pipeline states — informational, may be resumed by run
    for (room_id, state) in &pipeline_by_room {
        if is_active_pipeline_state(state) {
            actions.push(RecoveryAction::LeaveAsIs {
                reason: format!(
                    "Room {} pipeline persisted in {:?} — may be resumed by `bilive-rec run`, no automatic recovery performed",
                    room_id, state
                ),
            });
        }
    }

    Ok(RecoveryPlan { actions })
}

/// Result of applying a single recovery action.
#[derive(Debug)]
pub enum ApplyResult {
    /// Action was applied successfully.
    Applied(String),
    /// Action was skipped (LeaveAsIs or preconditions not met).
    Skipped(String),
}

/// Apply a recovery plan by executing each action against the store.
///
/// Safety rules:
/// - MarkInterruptedSegment: only if segment is still Recording.
/// - ResetRoomPipeline: only if pipeline is still Failed.
/// - ScheduleUploadReconciliation: only if segment is Finalized, path exists,
///   path is .flv, and no UploadedPart exists.
/// - LeaveAsIs: always skipped.
///
/// Returns a list of results for each action in the plan.
pub async fn apply_recovery<U: Uploader>(
    store: &StateStore,
    plan: &RecoveryPlan,
    uploader: Option<&U>,
) -> AppResult<Vec<ApplyResult>> {
    let mut results = Vec::new();

    for action in &plan.actions {
        let result = match action {
            RecoveryAction::MarkInterruptedSession { session_id } => {
                // Verify session is still Recording (idempotency)
                match store.get_session(*session_id)? {
                    Some(session) if session.status == SessionStatus::Recording => {
                        let mut updated = session;
                        updated.status = SessionStatus::Failed;
                        store.put_session(&updated)?;
                        ApplyResult::Applied(format!("Marked session {} as Failed", session_id))
                    }
                    Some(session) => ApplyResult::Skipped(format!(
                        "Session {} is {:?}, not Recording — skipping",
                        session_id, session.status
                    )),
                    None => {
                        ApplyResult::Skipped(format!("Session {} not found — skipping", session_id))
                    }
                }
            }
            RecoveryAction::MarkInterruptedSegment { session_id, index } => {
                // Verify segment is still Recording (idempotency)
                let segments = store.list_segments(*session_id)?;
                let segment = segments.iter().find(|s| s.index == *index);

                match segment {
                    Some(seg) if seg.status == SegmentStatus::Recording => {
                        let mut updated = seg.clone();
                        updated.status = SegmentStatus::Failed;
                        updated.error = Some("Interrupted by hard crash".to_string());
                        store.put_segment(&updated)?;
                        ApplyResult::Applied(format!(
                            "Marked segment {}/{} as Failed",
                            session_id, index
                        ))
                    }
                    Some(seg) => ApplyResult::Skipped(format!(
                        "Segment {}/{} is {:?}, not Recording — skipping",
                        session_id, index, seg.status
                    )),
                    None => ApplyResult::Skipped(format!(
                        "Segment {}/{} not found — skipping",
                        session_id, index
                    )),
                }
            }
            RecoveryAction::ResetRoomPipeline { room_id } => {
                // Verify pipeline is still Failed (idempotency)
                let current = store.get_pipeline_state(*room_id)?;
                match current {
                    Some(PipelineState::Failed) => {
                        store.put_pipeline_state(*room_id, PipelineState::Idle)?;
                        ApplyResult::Applied(format!(
                            "Reset room {} pipeline from Failed to Idle",
                            room_id
                        ))
                    }
                    Some(other) => ApplyResult::Skipped(format!(
                        "Room {} pipeline is {:?}, not Failed — skipping",
                        room_id, other
                    )),
                    None => ApplyResult::Skipped(format!(
                        "Room {} has no pipeline state — skipping",
                        room_id
                    )),
                }
            }
            RecoveryAction::ScheduleUploadReconciliation {
                session_id,
                segment_index,
                path,
            } => {
                // Re-verify submission boundary at apply time
                if let Some(sub) = store.get_submission(*session_id)? {
                    let status_word = match sub.status {
                        SubmissionStatus::Submitted => "Submitted",
                        SubmissionStatus::Pending => "Pending",
                        SubmissionStatus::Failed => "Failed",
                    };
                    ApplyResult::Skipped(format!(
                        "Segment {}/{}: session has a {} submission — skipping upload",
                        session_id, segment_index, status_word
                    ))
                } else {
                    // Re-verify preconditions
                    let segments = store.list_segments(*session_id)?;
                    let segment = segments.iter().find(|s| s.index == *segment_index);

                    let skip_reason = if let Some(seg) = segment {
                        if seg.status != SegmentStatus::Finalized {
                            Some(format!(
                                "Segment {}/{} is {:?}, not Finalized",
                                session_id, segment_index, seg.status
                            ))
                        } else if seg.path != *path {
                            // Fix 3: action path doesn't match persisted path
                            Some(format!(
                                "Segment {}/{} path mismatch: action has {}, store has {}",
                                session_id,
                                segment_index,
                                path.display(),
                                seg.path.display()
                            ))
                        } else if !path.exists() {
                            Some(format!("File does not exist: {}", path.display()))
                        } else if !path
                            .extension()
                            .is_some_and(|ext| ext.eq_ignore_ascii_case("flv"))
                        {
                            Some(format!("File is not a .flv: {}", path.display()))
                        } else {
                            // Check if UploadedPart already exists
                            let parts = store.list_uploaded_parts(*session_id)?;
                            if parts.iter().any(|p| p.segment_index == *segment_index) {
                                Some(format!(
                                    "Segment {}/{} already has an UploadedPart",
                                    session_id, segment_index
                                ))
                            } else {
                                None
                            }
                        }
                    } else {
                        Some(format!(
                            "Segment {}/{} not found",
                            session_id, segment_index
                        ))
                    };

                    if let Some(reason) = skip_reason {
                        ApplyResult::Skipped(reason)
                    } else if let Some(uploader) = uploader {
                        let req = UploadRequest {
                            session_id: *session_id,
                            segment_index: *segment_index,
                            path: path.clone(),
                            part_title: format!("Part {}", segment_index),
                        };
                        match uploader.upload_segment(req).await {
                            Ok(part) => {
                                store.put_uploaded_part(&part)?;
                                ApplyResult::Applied(format!(
                                    "Uploaded segment {}/{}: {}",
                                    session_id, segment_index, part.bili_filename
                                ))
                            }
                            Err(e) => ApplyResult::Skipped(format!(
                                "Upload failed for segment {}/{}: {}",
                                session_id, segment_index, e
                            )),
                        }
                    } else {
                        ApplyResult::Skipped(format!(
                            "Upload skipped for segment {}/{}: no uploader configured",
                            session_id, segment_index
                        ))
                    }
                } // end else (no submission)
            }
            RecoveryAction::LeaveAsIs { reason } => ApplyResult::Skipped(reason.clone()),
        };
        results.push(result);
    }

    Ok(results)
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

    fn empty_reset_rooms() -> HashSet<u64> {
        HashSet::new()
    }

    fn empty_retry_uploads() -> HashSet<Uuid> {
        HashSet::new()
    }

    #[test]
    fn plan_empty_state() {
        let (store, _dir) = test_store();
        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
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

        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
        // Both session and segment are interrupted
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            RecoveryAction::MarkInterruptedSession { session_id: sid } if *sid == session_id
        )));
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            RecoveryAction::MarkInterruptedSegment { session_id: sid, index: 3 } if *sid == session_id
        )));
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

        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
        // No MarkInterruptedSegment — pipeline is active
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, RecoveryAction::MarkInterruptedSegment { .. }))
        );
        // No MarkInterruptedSession — pipeline is active
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, RecoveryAction::MarkInterruptedSession { .. }))
        );
        // But there IS active pipeline guidance
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            RecoveryAction::LeaveAsIs { reason } if reason.contains("resumed by")
        )));
    }

    #[test]
    fn plan_finalized_missing_upload_without_retry_is_leave_as_is() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let flv_path = dir.path().join("seg.flv");
        std::fs::write(&flv_path, b"fake flv data").unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: flv_path,
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
        assert_eq!(plan.actions.len(), 1);
        assert!(matches!(plan.actions[0], RecoveryAction::LeaveAsIs { .. }));
        if let RecoveryAction::LeaveAsIs { reason } = &plan.actions[0] {
            assert!(reason.contains("--retry-upload"));
        }
    }

    #[test]
    fn plan_finalized_missing_upload_with_retry_schedules_upload() {
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

        let mut retry_uploads = HashSet::new();
        retry_uploads.insert(session_id);
        let plan = plan_recovery(&store, &empty_reset_rooms(), &retry_uploads).unwrap();
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
    fn plan_finalized_missing_upload_file_missing_with_retry() {
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

        let mut retry_uploads = HashSet::new();
        retry_uploads.insert(session_id);
        let plan = plan_recovery(&store, &empty_reset_rooms(), &retry_uploads).unwrap();
        assert_eq!(plan.actions.len(), 1);
        assert!(matches!(plan.actions[0], RecoveryAction::LeaveAsIs { .. }));
        if let RecoveryAction::LeaveAsIs { reason } = &plan.actions[0] {
            assert!(reason.contains("file does not exist"));
        }
    }

    #[test]
    fn plan_finalized_missing_upload_part_file_with_retry() {
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

        let mut retry_uploads = HashSet::new();
        retry_uploads.insert(session_id);
        let plan = plan_recovery(&store, &empty_reset_rooms(), &retry_uploads).unwrap();
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

        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
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

        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
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

        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
        assert_eq!(plan.actions.len(), 1);
        assert!(matches!(plan.actions[0], RecoveryAction::LeaveAsIs { .. }));
        if let RecoveryAction::LeaveAsIs { reason } = &plan.actions[0] {
            assert!(reason.contains("manual verification"));
        }
    }

    #[test]
    fn plan_failed_pipeline_leave_as_is_without_reset() {
        let (store, _dir) = test_store();

        store.put_pipeline_state(42, PipelineState::Failed).unwrap();

        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
        assert_eq!(plan.actions.len(), 1);
        assert!(matches!(plan.actions[0], RecoveryAction::LeaveAsIs { .. }));
        if let RecoveryAction::LeaveAsIs { reason } = &plan.actions[0] {
            assert!(reason.contains("--reset-room 42"));
        }
    }

    #[test]
    fn plan_failed_pipeline_with_reset_room() {
        let (store, _dir) = test_store();

        store.put_pipeline_state(42, PipelineState::Failed).unwrap();

        let mut reset_rooms = HashSet::new();
        reset_rooms.insert(42);
        let plan = plan_recovery(&store, &reset_rooms, &empty_retry_uploads()).unwrap();
        assert_eq!(plan.actions.len(), 1);
        assert_eq!(
            plan.actions[0],
            RecoveryAction::ResetRoomPipeline { room_id: 42 }
        );
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

    // --- apply_recovery tests ---

    struct FakeUploader;

    impl Uploader for FakeUploader {
        async fn check_login(&self) -> AppResult<()> {
            Ok(())
        }
        async fn upload_segment(&self, req: UploadRequest) -> AppResult<UploadedPart> {
            Ok(UploadedPart {
                session_id: req.session_id,
                segment_index: req.segment_index,
                bili_filename: format!("uploaded_{}.flv", req.segment_index),
                part_title: req.part_title,
            })
        }
        async fn submit(
            &self,
            _req: crate::uploader::types::SubmissionRequest,
        ) -> AppResult<crate::uploader::types::SubmissionResult> {
            Ok(crate::uploader::types::SubmissionResult {
                aid: None,
                bvid: None,
            })
        }
    }

    #[tokio::test]
    async fn apply_mark_interrupted_segment() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let part_path = dir.path().join("test.part");

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: part_path,
                status: SegmentStatus::Recording,
                error: None,
            })
            .unwrap();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::MarkInterruptedSegment {
                session_id,
                index: 0,
            }],
        };

        let results = apply_recovery(&store, &plan, Some(&FakeUploader))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Applied(_)));

        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments[0].status, SegmentStatus::Failed);
        assert_eq!(
            segments[0].error.as_deref(),
            Some("Interrupted by hard crash")
        );
    }

    #[tokio::test]
    async fn apply_mark_interrupted_segment_idempotent_already_failed() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let part_path = dir.path().join("test.part");

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: part_path,
                status: SegmentStatus::Failed,
                error: Some("already failed".to_string()),
            })
            .unwrap();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::MarkInterruptedSegment {
                session_id,
                index: 0,
            }],
        };

        let results = apply_recovery(&store, &plan, Some(&FakeUploader))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Skipped(_)));

        // Segment unchanged
        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments[0].error.as_deref(), Some("already failed"));
    }

    #[tokio::test]
    async fn apply_reset_room_pipeline() {
        let (store, _dir) = test_store();

        store.put_pipeline_state(42, PipelineState::Failed).unwrap();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::ResetRoomPipeline { room_id: 42 }],
        };

        let results = apply_recovery(&store, &plan, Some(&FakeUploader))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Applied(_)));

        let state = store.get_pipeline_state(42).unwrap();
        assert_eq!(state, Some(PipelineState::Idle));
    }

    #[tokio::test]
    async fn apply_reset_room_pipeline_idempotent_already_idle() {
        let (store, _dir) = test_store();

        store.put_pipeline_state(42, PipelineState::Idle).unwrap();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::ResetRoomPipeline { room_id: 42 }],
        };

        let results = apply_recovery(&store, &plan, Some(&FakeUploader))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Skipped(_)));
    }

    #[tokio::test]
    async fn apply_schedule_upload_reconciliation() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let flv_path = dir.path().join("seg.flv");
        std::fs::write(&flv_path, b"fake flv").unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: flv_path.clone(),
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::ScheduleUploadReconciliation {
                session_id,
                segment_index: 1,
                path: flv_path,
            }],
        };

        let results = apply_recovery(&store, &plan, Some(&FakeUploader))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Applied(_)));

        let parts = store.list_uploaded_parts(session_id).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].segment_index, 1);
        assert_eq!(parts[0].bili_filename, "uploaded_1.flv");
    }

    #[tokio::test]
    async fn apply_upload_reconciliation_idempotent_already_uploaded() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let flv_path = dir.path().join("seg.flv");
        std::fs::write(&flv_path, b"fake flv").unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: flv_path.clone(),
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        store
            .put_uploaded_part(&UploadedPart {
                session_id,
                segment_index: 1,
                bili_filename: "existing.flv".to_string(),
                part_title: "Part 1".to_string(),
            })
            .unwrap();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::ScheduleUploadReconciliation {
                session_id,
                segment_index: 1,
                path: flv_path,
            }],
        };

        let results = apply_recovery(&store, &plan, Some(&FakeUploader))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Skipped(_)));

        // UploadedPart unchanged
        let parts = store.list_uploaded_parts(session_id).unwrap();
        assert_eq!(parts[0].bili_filename, "existing.flv");
    }

    #[tokio::test]
    async fn apply_upload_reconciliation_skips_non_finalized() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let part_path = dir.path().join("seg.part");

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: part_path,
                status: SegmentStatus::Recording,
                error: None,
            })
            .unwrap();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::ScheduleUploadReconciliation {
                session_id,
                segment_index: 0,
                path: PathBuf::from("/fake/seg.flv"),
            }],
        };

        let results = apply_recovery(&store, &plan, Some(&FakeUploader))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Skipped(_)));
    }

    #[tokio::test]
    async fn apply_leave_as_is_always_skipped() {
        let (store, _dir) = test_store();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::LeaveAsIs {
                reason: "test reason".to_string(),
            }],
        };

        let results = apply_recovery(&store, &plan, Some(&FakeUploader))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Skipped(_)));
    }

    #[tokio::test]
    async fn apply_empty_plan() {
        let (store, _dir) = test_store();

        let plan = RecoveryPlan { actions: vec![] };
        let results = apply_recovery(&store, &plan, Some(&FakeUploader))
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn apply_mark_interrupted_without_uploader() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let part_path = dir.path().join("test.part");

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: part_path,
                status: SegmentStatus::Recording,
                error: None,
            })
            .unwrap();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::MarkInterruptedSegment {
                session_id,
                index: 0,
            }],
        };

        let results = apply_recovery::<FakeUploader>(&store, &plan, None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Applied(_)));

        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments[0].status, SegmentStatus::Failed);
    }

    #[tokio::test]
    async fn apply_reset_room_without_uploader() {
        let (store, _dir) = test_store();

        store.put_pipeline_state(42, PipelineState::Failed).unwrap();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::ResetRoomPipeline { room_id: 42 }],
        };

        let results = apply_recovery::<FakeUploader>(&store, &plan, None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Applied(_)));

        let state = store.get_pipeline_state(42).unwrap();
        assert_eq!(state, Some(PipelineState::Idle));
    }

    #[tokio::test]
    async fn apply_upload_without_uploader_skips() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let flv_path = dir.path().join("seg.flv");
        std::fs::write(&flv_path, b"fake flv").unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: flv_path.clone(),
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::ScheduleUploadReconciliation {
                session_id,
                segment_index: 1,
                path: flv_path,
            }],
        };

        let results = apply_recovery::<FakeUploader>(&store, &plan, None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Skipped(_)));
    }

    // --- Fix 1: InterruptedSession tests ---

    #[test]
    fn plan_interrupted_session_produces_action() {
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

        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            RecoveryAction::MarkInterruptedSession { session_id: sid } if *sid == session_id
        )));
    }

    #[test]
    fn plan_active_session_no_action() {
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

        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, RecoveryAction::MarkInterruptedSession { .. }))
        );
    }

    #[tokio::test]
    async fn apply_mark_interrupted_session() {
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

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::MarkInterruptedSession { session_id }],
        };

        let results = apply_recovery::<FakeUploader>(&store, &plan, None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Applied(_)));

        let session = store.get_session(session_id).unwrap().unwrap();
        assert_eq!(session.status, SessionStatus::Failed);
    }

    #[tokio::test]
    async fn apply_mark_interrupted_session_idempotent() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();
        store
            .put_session(&LiveSession {
                id: session_id,
                room_key: "123".to_string(),
                title: "Test".to_string(),
                started_at: Timestamp::now(),
                status: SessionStatus::Failed,
            })
            .unwrap();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::MarkInterruptedSession { session_id }],
        };

        let results = apply_recovery::<FakeUploader>(&store, &plan, None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Skipped(_)));
    }

    // --- Fix 2: ActivePipeline anomaly tests ---

    #[test]
    fn detect_active_pipeline_recording() {
        let (store, _dir) = test_store();
        store
            .put_pipeline_state(1, PipelineState::Recording)
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::ActivePipeline)
        );
    }

    #[test]
    fn detect_active_pipeline_uploading() {
        let (store, _dir) = test_store();
        store
            .put_pipeline_state(1, PipelineState::Uploading)
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::ActivePipeline)
        );
    }

    #[test]
    fn detect_active_pipeline_submitting() {
        let (store, _dir) = test_store();
        store
            .put_pipeline_state(1, PipelineState::Submitting)
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::ActivePipeline)
        );
    }

    #[test]
    fn detect_active_pipeline_re_resolving() {
        let (store, _dir) = test_store();
        store
            .put_pipeline_state(1, PipelineState::ReResolving)
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::ActivePipeline)
        );
    }

    #[test]
    fn detect_active_pipeline_waiting_reconnect() {
        let (store, _dir) = test_store();
        store
            .put_pipeline_state(1, PipelineState::WaitingReconnect)
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::ActivePipeline)
        );
    }

    #[test]
    fn detect_idle_pipeline_not_active_anomaly() {
        let (store, _dir) = test_store();
        store.put_pipeline_state(1, PipelineState::Idle).unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            !anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::ActivePipeline)
        );
    }

    // --- Fix 3: Submission boundary tests ---

    #[test]
    fn plan_retry_upload_refused_when_submitted() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let flv_path = dir.path().join("seg.flv");
        std::fs::write(&flv_path, b"fake").unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: flv_path,
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        store
            .put_submission(&Submission {
                session_id,
                status: SubmissionStatus::Submitted,
                aid: Some(1),
                bvid: Some("BV1".into()),
                error: None,
            })
            .unwrap();

        let mut retry_uploads = HashSet::new();
        retry_uploads.insert(session_id);
        let plan = plan_recovery(&store, &empty_reset_rooms(), &retry_uploads).unwrap();
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            RecoveryAction::LeaveAsIs { reason } if reason.contains("Submitted submission")
        )));
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, RecoveryAction::ScheduleUploadReconciliation { .. }))
        );
    }

    #[test]
    fn plan_retry_upload_refused_when_pending() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let flv_path = dir.path().join("seg.flv");
        std::fs::write(&flv_path, b"fake").unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: flv_path,
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        store
            .put_submission(&Submission {
                session_id,
                status: SubmissionStatus::Pending,
                aid: None,
                bvid: None,
                error: None,
            })
            .unwrap();

        let mut retry_uploads = HashSet::new();
        retry_uploads.insert(session_id);
        let plan = plan_recovery(&store, &empty_reset_rooms(), &retry_uploads).unwrap();
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            RecoveryAction::LeaveAsIs { reason } if reason.contains("Pending submission")
        )));
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, RecoveryAction::ScheduleUploadReconciliation { .. }))
        );
    }

    #[test]
    fn plan_retry_upload_refused_when_failed_submission() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let flv_path = dir.path().join("seg.flv");
        std::fs::write(&flv_path, b"fake").unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: flv_path,
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        store
            .put_submission(&Submission {
                session_id,
                status: SubmissionStatus::Failed,
                aid: None,
                bvid: None,
                error: Some("timeout".into()),
            })
            .unwrap();

        let mut retry_uploads = HashSet::new();
        retry_uploads.insert(session_id);
        let plan = plan_recovery(&store, &empty_reset_rooms(), &retry_uploads).unwrap();
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            RecoveryAction::LeaveAsIs { reason } if reason.contains("Failed submission")
        )));
        assert!(
            !plan
                .actions
                .iter()
                .any(|a| matches!(a, RecoveryAction::ScheduleUploadReconciliation { .. }))
        );
    }

    // --- Fix 1: Active pipeline guidance in plan_recovery ---

    #[test]
    fn plan_active_pipeline_recording_produces_guidance() {
        let (store, _dir) = test_store();
        store
            .put_pipeline_state(1, PipelineState::Recording)
            .unwrap();
        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            RecoveryAction::LeaveAsIs { reason } if reason.contains("Recording") && reason.contains("resumed by")
        )));
    }

    #[test]
    fn plan_active_pipeline_uploading_produces_guidance() {
        let (store, _dir) = test_store();
        store
            .put_pipeline_state(2, PipelineState::Uploading)
            .unwrap();
        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            RecoveryAction::LeaveAsIs { reason } if reason.contains("Uploading") && reason.contains("resumed by")
        )));
    }

    #[test]
    fn plan_active_pipeline_submitting_produces_guidance() {
        let (store, _dir) = test_store();
        store
            .put_pipeline_state(3, PipelineState::Submitting)
            .unwrap();
        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            RecoveryAction::LeaveAsIs { reason } if reason.contains("Submitting") && reason.contains("resumed by")
        )));
    }

    #[test]
    fn plan_active_pipeline_re_resolving_produces_guidance() {
        let (store, _dir) = test_store();
        store
            .put_pipeline_state(4, PipelineState::ReResolving)
            .unwrap();
        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            RecoveryAction::LeaveAsIs { reason } if reason.contains("ReResolving") && reason.contains("resumed by")
        )));
    }

    #[test]
    fn plan_active_pipeline_waiting_reconnect_produces_guidance() {
        let (store, _dir) = test_store();
        store
            .put_pipeline_state(5, PipelineState::WaitingReconnect)
            .unwrap();
        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
        assert!(plan.actions.iter().any(|a| matches!(
            a,
            RecoveryAction::LeaveAsIs { reason } if reason.contains("WaitingReconnect") && reason.contains("resumed by")
        )));
    }

    #[test]
    fn plan_idle_pipeline_produces_no_active_guidance() {
        let (store, _dir) = test_store();
        store.put_pipeline_state(1, PipelineState::Idle).unwrap();
        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
        assert!(!plan.actions.iter().any(|a| matches!(
            a,
            RecoveryAction::LeaveAsIs { reason } if reason.contains("resumed by")
        )));
    }

    // --- Fix 2: Apply-time submission boundary refusal ---

    #[tokio::test]
    async fn apply_upload_refused_when_submitted() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let flv_path = dir.path().join("seg.flv");
        std::fs::write(&flv_path, b"fake flv").unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: flv_path.clone(),
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        store
            .put_submission(&Submission {
                session_id,
                status: SubmissionStatus::Submitted,
                aid: Some(1),
                bvid: Some("BV1".into()),
                error: None,
            })
            .unwrap();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::ScheduleUploadReconciliation {
                session_id,
                segment_index: 1,
                path: flv_path,
            }],
        };

        let results = apply_recovery(&store, &plan, Some(&FakeUploader))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Skipped(_)));
        if let ApplyResult::Skipped(msg) = &results[0] {
            assert!(msg.contains("Submitted submission"));
        }
    }

    #[tokio::test]
    async fn apply_upload_refused_when_pending() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let flv_path = dir.path().join("seg.flv");
        std::fs::write(&flv_path, b"fake flv").unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: flv_path.clone(),
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        store
            .put_submission(&Submission {
                session_id,
                status: SubmissionStatus::Pending,
                aid: None,
                bvid: None,
                error: None,
            })
            .unwrap();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::ScheduleUploadReconciliation {
                session_id,
                segment_index: 1,
                path: flv_path,
            }],
        };

        let results = apply_recovery(&store, &plan, Some(&FakeUploader))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Skipped(_)));
        if let ApplyResult::Skipped(msg) = &results[0] {
            assert!(msg.contains("Pending submission"));
        }
    }

    #[tokio::test]
    async fn apply_upload_refused_when_failed_submission() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let flv_path = dir.path().join("seg.flv");
        std::fs::write(&flv_path, b"fake flv").unwrap();

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: flv_path.clone(),
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        store
            .put_submission(&Submission {
                session_id,
                status: SubmissionStatus::Failed,
                aid: None,
                bvid: None,
                error: Some("timeout".into()),
            })
            .unwrap();

        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::ScheduleUploadReconciliation {
                session_id,
                segment_index: 1,
                path: flv_path,
            }],
        };

        let results = apply_recovery(&store, &plan, Some(&FakeUploader))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Skipped(_)));
        if let ApplyResult::Skipped(msg) = &results[0] {
            assert!(msg.contains("Failed submission"));
        }
    }

    // --- Fix 3: Stale path mismatch ---

    #[tokio::test]
    async fn apply_upload_skips_when_path_mismatch() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        let real_path = dir.path().join("real.flv");
        let stale_path = dir.path().join("stale.flv");
        std::fs::write(&real_path, b"real data").unwrap();
        std::fs::write(&stale_path, b"stale data").unwrap();

        // Store has real_path
        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: real_path,
                status: SegmentStatus::Finalized,
                error: None,
            })
            .unwrap();

        // Plan has stale_path
        let plan = RecoveryPlan {
            actions: vec![RecoveryAction::ScheduleUploadReconciliation {
                session_id,
                segment_index: 1,
                path: stale_path,
            }],
        };

        let results = apply_recovery(&store, &plan, Some(&FakeUploader))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], ApplyResult::Skipped(_)));
        if let ApplyResult::Skipped(msg) = &results[0] {
            assert!(msg.contains("path mismatch"));
        }

        // Verify no upload happened
        let parts = store.list_uploaded_parts(session_id).unwrap();
        assert!(parts.is_empty());
    }
}
