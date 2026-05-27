use std::collections::{HashMap, HashSet};

use crate::error::AppResult;
use crate::pipeline::state_machine::PipelineState;
use crate::state::model::{RoomPipelineState, SegmentStatus, SessionStatus, SubmissionStatus};
use crate::state::store::StateStore;
use crate::uploader::types::Uploader;
use crate::uploader::validation::{
    PersistedUploadFailure, upload_and_persist_segment, validate_finalized_segment_for_upload,
};

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
    /// A submission is Ambiguous (Bilibili accepted but did not return aid/bvid)
    AmbiguousSubmission,
    /// A room's pipeline is stuck in Failed state
    FailedPipeline,
    /// A segment references a file path that does not exist on disk
    MissingSegmentFile,
    /// A session is stuck in Recording status (crash interrupted it)
    InterruptedSession,
    /// A pipeline is persisted in an active state after process exit
    ActivePipeline,
    /// A segment was marked Uploading, so a remote upload may have started
    AmbiguousUpload,
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

fn pipeline_marks_session_active(
    room_pipeline: Option<&RoomPipelineState>,
    session_id: uuid::Uuid,
) -> bool {
    room_pipeline.is_some_and(|room_pipeline| {
        is_active_pipeline_state(&room_pipeline.state)
            && room_pipeline.active_session_id == Some(session_id)
    })
}

fn pipeline_marks_session_recording(
    room_pipeline: Option<&RoomPipelineState>,
    session_id: uuid::Uuid,
) -> bool {
    room_pipeline.is_some_and(|room_pipeline| {
        is_recording_pipeline_state(&room_pipeline.state)
            && room_pipeline.active_session_id == Some(session_id)
    })
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
    let pipeline_states = store.list_all_room_pipeline_states()?;

    // Build lookup: room_id -> PipelineState
    let pipeline_by_room: HashMap<u64, RoomPipelineState> = pipeline_states.into_iter().collect();

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
                .is_some_and(|state| pipeline_marks_session_active(Some(state), session.id));

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
                    .is_some_and(|state| {
                        pipeline_marks_session_recording(Some(state), segment.session_id)
                    });

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
            SegmentStatus::Uploading => {
                anomalies.push(Anomaly {
                    kind: AnomalyKind::AmbiguousUpload,
                    description: format!(
                        "Segment {}/{} is Uploading — remote upload outcome is unknown, refusing automatic retry",
                        segment.session_id, segment.index
                    ),
                });
            }
            SegmentStatus::Uploaded | SegmentStatus::Cleaned => {
                let has_upload = uploaded_by_session
                    .get(&segment.session_id)
                    .map(|indices| indices.contains(&segment.index))
                    .unwrap_or(false);

                if !has_upload {
                    anomalies.push(Anomaly {
                        kind: AnomalyKind::AmbiguousUpload,
                        description: format!(
                            "Segment {}/{} is {:?} but has no UploadedPart — remote filename is missing",
                            segment.session_id, segment.index, segment.status
                        ),
                    });
                }
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
            SubmissionStatus::Ambiguous => {
                let error_detail = submission.error.as_deref().unwrap_or("no detail");
                anomalies.push(Anomaly {
                    kind: AnomalyKind::AmbiguousSubmission,
                    description: format!(
                        "Submission for session {} is Ambiguous (Bilibili accepted but did not return aid/bvid): {}",
                        submission.session_id, error_detail
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
    for (room_id, room_pipeline) in &pipeline_by_room {
        match room_pipeline.state {
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
                        room_id, room_pipeline.state
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
    let pipeline_states = store.list_all_room_pipeline_states()?;

    // Build lookup: room_id -> PipelineState
    let pipeline_by_room: HashMap<u64, RoomPipelineState> = pipeline_states.into_iter().collect();

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
                .is_some_and(|state| pipeline_marks_session_active(Some(state), session.id));

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
                .is_some_and(|state| {
                    pipeline_marks_session_recording(Some(state), segment.session_id)
                });

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
                            SubmissionStatus::Ambiguous => "Ambiguous",
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
    for segment in &segments {
        if matches!(
            segment.status,
            SegmentStatus::Uploading | SegmentStatus::Uploaded | SegmentStatus::Cleaned
        ) {
            let has_upload = uploaded_by_session
                .get(&segment.session_id)
                .map(|indices| indices.contains(&segment.index))
                .unwrap_or(false);
            if segment.status == SegmentStatus::Uploading
                || (matches!(
                    segment.status,
                    SegmentStatus::Uploaded | SegmentStatus::Cleaned
                ) && !has_upload)
            {
                actions.push(RecoveryAction::LeaveAsIs {
                    reason: format!(
                        "Segment {}/{} is {:?} with ambiguous upload state — requires manual verification",
                        segment.session_id, segment.index, segment.status
                    ),
                });
            }
        }
    }

    // 4. Pending submissions — outcome unknown, must not auto-retry
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

    // 4b. Ambiguous submissions — Bilibili accepted but did not return aid/bvid;
    // we cannot prove whether the video was created. Manual verification required.
    for submission in &submissions {
        if submission.status == SubmissionStatus::Ambiguous {
            actions.push(RecoveryAction::LeaveAsIs {
                reason: format!(
                    "Ambiguous submission {} — Bilibili accepted but did not return aid/bvid; verify on Bilibili and use `state resolve-submission`",
                    submission.session_id
                ),
            });
        }
    }

    // 5. Failed submissions — must not auto-retry
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

    // 6. Failed pipeline states — require explicit --reset-room
    for (room_id, room_pipeline) in &pipeline_by_room {
        if room_pipeline.state == PipelineState::Failed {
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

    // 7. Active pipeline states — informational, may be resumed by run
    for (room_id, room_pipeline) in &pipeline_by_room {
        if is_active_pipeline_state(&room_pipeline.state) {
            actions.push(RecoveryAction::LeaveAsIs {
                reason: format!(
                    "Room {} pipeline persisted in {:?} — may be resumed by `bilive-rec run`, no automatic recovery performed",
                    room_id, room_pipeline.state
                ),
            });
        }
    }

    Ok(RecoveryPlan { actions })
}

/// Outcome of a successful manual submission resolve.
#[derive(Debug, Clone)]
pub struct SubmissionResolved {
    pub session_id: uuid::Uuid,
    pub from: SubmissionStatus,
    pub to: SubmissionStatus,
    pub aid: Option<u64>,
    pub bvid: Option<String>,
}

/// Manually resolve a Pending or Ambiguous submission to a definitive
/// outcome the operator confirmed on Bilibili.
///
/// Refuses to:
///   - touch a submission that is already Submitted or Failed (those are
///     definitive; if they need editing, that's an operator data-fix, not
///     recovery);
///   - flip to Submitted without at least one of aid/bvid (otherwise the
///     row would claim a confirmed identity it does not have);
///   - resolve a session that has no submission row.
///
/// On success the existing aid/bvid are overwritten when resolving to
/// Submitted, and preserved (but the row marked Failed) when resolving to
/// Failed — the original error string is replaced with a "Manually
/// resolved as Failed" annotation including the prior status.
pub fn resolve_submission(
    store: &StateStore,
    session_id: uuid::Uuid,
    target: SubmissionStatus,
    aid: Option<u64>,
    bvid: Option<String>,
) -> AppResult<SubmissionResolved> {
    use crate::error::AppError;
    use crate::state::model::Submission;

    if !matches!(
        target,
        SubmissionStatus::Submitted | SubmissionStatus::Failed
    ) {
        return Err(AppError::Config(format!(
            "resolve target must be Submitted or Failed, got {target:?}"
        )));
    }

    let current = store
        .get_submission(session_id)?
        .ok_or_else(|| AppError::State(format!("Submission for session {session_id} not found")))?;

    match current.status {
        SubmissionStatus::Pending | SubmissionStatus::Ambiguous => {}
        other => {
            return Err(AppError::State(format!(
                "Submission for session {session_id} is {other:?}, not Pending or Ambiguous — refusing manual resolve"
            )));
        }
    }

    let updated = match target {
        SubmissionStatus::Submitted => {
            if aid.is_none() && bvid.is_none() {
                return Err(AppError::Config(
                    "resolving to Submitted requires at least one of --aid or --bvid".into(),
                ));
            }
            Submission {
                session_id,
                upload_credential: current.upload_credential.clone(),
                status: SubmissionStatus::Submitted,
                aid,
                bvid: bvid.clone(),
                error: None,
            }
        }
        SubmissionStatus::Failed => Submission {
            session_id,
            upload_credential: current.upload_credential.clone(),
            status: SubmissionStatus::Failed,
            aid: current.aid,
            bvid: current.bvid.clone(),
            error: Some(format!(
                "Manually resolved as Failed (was {:?})",
                current.status
            )),
        },
        _ => unreachable!("guarded above"),
    };

    let from = current.status;
    let to = updated.status;
    let aid_out = updated.aid;
    let bvid_out = updated.bvid.clone();
    store.put_submission(&updated)?;

    Ok(SubmissionResolved {
        session_id,
        from,
        to,
        aid: aid_out,
        bvid: bvid_out,
    })
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
/// - ScheduleUploadReconciliation: only if no submission boundary has been crossed,
///   the persisted segment still matches the action, the file is a regular `.flv`,
///   and no UploadedPart exists.
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
                match validate_finalized_segment_for_upload(
                    store,
                    *session_id,
                    *segment_index,
                    Some(path),
                )? {
                    Err(reason) => ApplyResult::Skipped(format!(
                        "Segment {}/{}: {}",
                        session_id, segment_index, reason
                    )),
                    Ok(segment) => {
                        if let Some(uploader) = uploader {
                            match upload_and_persist_segment(
                                uploader,
                                store,
                                segment,
                                format!("Part {}", segment_index),
                            )
                            .await
                            {
                                Ok(part) => ApplyResult::Applied(format!(
                                    "Uploaded segment {}/{}: {}",
                                    session_id, segment_index, part.bili_filename
                                )),
                                Err(PersistedUploadFailure::Remote { error, .. }) => {
                                    ApplyResult::Skipped(format!(
                                        "Upload failed for segment {}/{}: {}",
                                        session_id, segment_index, error
                                    ))
                                }
                                Err(PersistedUploadFailure::StateBeforeRemote {
                                    error, ..
                                }) => {
                                    return Err(crate::error::AppError::State(format!(
                                        "Failed to persist pre-upload state for segment {}/{}: {}",
                                        session_id, segment_index, error
                                    )));
                                }
                                Err(PersistedUploadFailure::StateAfterRemote { error, .. }) => {
                                    return Err(crate::error::AppError::State(format!(
                                        "Remote upload for segment {}/{} may have succeeded, but state persistence failed: {}",
                                        session_id, segment_index, error
                                    )));
                                }
                            }
                        } else {
                            ApplyResult::Skipped(format!(
                                "Upload skipped for segment {}/{}: no uploader configured",
                                session_id, segment_index
                            ))
                        }
                    }
                }
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
        LiveSession, SegmentStatus, SessionStatus, Submission, SubmissionStatus, UploadedPart,
        fixtures::{failed_segment, finalized_segment, recording_segment, uploading_segment},
    };
    use crate::uploader::types::UploadRequest;
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
                record_credential: None,
                upload_credential: None,
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
                record_credential: None,
                upload_credential: None,
            })
            .unwrap();

        store
            .put_room_pipeline_state(456, PipelineState::Recording, Some(session_id))
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
                record_credential: None,
                upload_credential: None,
            })
            .unwrap();

        store
            .put_room_pipeline_state(789, PipelineState::ReResolving, Some(session_id))
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
                record_credential: None,
                upload_credential: None,
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
                record_credential: None,
                upload_credential: None,
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
                record_credential: None,
                upload_credential: None,
            })
            .unwrap();

        store
            .put_segment(&recording_segment(session_id, 0, part_path))
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
                record_credential: None,
                upload_credential: None,
            })
            .unwrap();

        store
            .put_segment(&recording_segment(session_id, 0, part_path))
            .unwrap();

        store
            .put_room_pipeline_state(333, PipelineState::Recording, Some(session_id))
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
                record_credential: None,
                upload_credential: None,
            })
            .unwrap();

        store
            .put_segment(&recording_segment(session_id, 0, part_path))
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
                record_credential: None,
                upload_credential: None,
            })
            .unwrap();

        store
            .put_segment(&recording_segment(session_id, 0, part_path))
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
            .put_segment(&finalized_segment(session_id, 0, flv_path))
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
            .put_segment(&finalized_segment(session_id, 0, flv_path))
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
            .put_segment(&finalized_segment(
                session_id,
                0,
                PathBuf::from("/nonexistent/path/test.flv"),
            ))
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
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
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
    fn detect_ambiguous_submission() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
                status: SubmissionStatus::Ambiguous,
                aid: None,
                bvid: None,
                error: Some("Bilibili returned code=0 but no aid/bvid".to_string()),
            })
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        let ambig = anomalies
            .iter()
            .find(|a| a.kind == AnomalyKind::AmbiguousSubmission)
            .expect("expected an AmbiguousSubmission anomaly");
        assert!(ambig.description.contains("Ambiguous"));
        assert!(ambig.description.contains("aid/bvid") || ambig.description.contains("code=0"));
    }

    #[test]
    fn plan_recovery_leaves_ambiguous_submission_alone() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
                status: SubmissionStatus::Ambiguous,
                aid: None,
                bvid: None,
                error: Some("ambiguous".into()),
            })
            .unwrap();

        let plan = plan_recovery(&store, &HashSet::new(), &HashSet::new()).unwrap();

        let leave_msgs: Vec<&String> = plan
            .actions
            .iter()
            .filter_map(|a| match a {
                RecoveryAction::LeaveAsIs { reason } => Some(reason),
                _ => None,
            })
            .collect();
        assert!(
            leave_msgs
                .iter()
                .any(|msg| msg.contains("Ambiguous submission")
                    && msg.contains("resolve-submission"))
        );
    }

    #[test]
    fn detect_failed_submission() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
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
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
                status: SubmissionStatus::Submitted,
                aid: Some(123),
                bvid: Some("BV123".to_string()),
                error: None,
            })
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(anomalies.is_empty());
    }

    #[test]
    fn detect_uploading_segment_is_ambiguous_upload() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        store
            .put_segment(&uploading_segment(
                session_id,
                0,
                dir.path().join("segment.flv"),
            ))
            .unwrap();

        let anomalies = detect_anomalies(&store).unwrap();
        assert!(
            anomalies
                .iter()
                .any(|a| a.kind == AnomalyKind::AmbiguousUpload)
        );
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
                record_credential: None,
                upload_credential: None,
            })
            .unwrap();

        store
            .put_segment(&recording_segment(session_id, 3, part_path))
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
                record_credential: None,
                upload_credential: None,
            })
            .unwrap();

        store
            .put_segment(&recording_segment(session_id, 0, part_path))
            .unwrap();

        store
            .put_room_pipeline_state(333, PipelineState::Recording, Some(session_id))
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
            .put_segment(&finalized_segment(session_id, 1, flv_path))
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
            .put_segment(&finalized_segment(session_id, 1, flv_path.clone()))
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
            .put_segment(&finalized_segment(
                session_id,
                2,
                PathBuf::from("/nonexistent/seg.flv"),
            ))
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
            .put_segment(&finalized_segment(session_id, 3, part_path))
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
            .put_segment(&finalized_segment(session_id, 0, flv_path))
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
    fn plan_uploading_segment_is_left_for_manual_verification() {
        let (store, dir) = test_store();
        let session_id = Uuid::new_v4();
        store
            .put_segment(&uploading_segment(
                session_id,
                0,
                dir.path().join("segment.flv"),
            ))
            .unwrap();

        let plan = plan_recovery(&store, &empty_reset_rooms(), &empty_retry_uploads()).unwrap();
        assert!(plan.actions.iter().any(|action| {
            matches!(
                action,
                RecoveryAction::LeaveAsIs { reason }
                    if reason.contains("ambiguous upload state")
            )
        }));
    }

    #[test]
    fn plan_pending_submission_leave_as_is() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();

        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
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
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
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
        ) -> AppResult<crate::uploader::types::SubmissionOutcome> {
            Ok(crate::uploader::types::SubmissionOutcome::Confirmed {
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
            .put_segment(&recording_segment(session_id, 0, part_path))
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
            .put_segment(&failed_segment(session_id, 0, part_path, "already failed"))
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
            .put_segment(&finalized_segment(session_id, 1, flv_path.clone()))
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
            .put_segment(&finalized_segment(session_id, 1, flv_path.clone()))
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
            .put_segment(&recording_segment(session_id, 0, part_path))
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
            .put_segment(&recording_segment(session_id, 0, part_path))
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
            .put_segment(&finalized_segment(session_id, 1, flv_path.clone()))
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
                record_credential: None,
                upload_credential: None,
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
                record_credential: None,
                upload_credential: None,
            })
            .unwrap();
        store
            .put_room_pipeline_state(456, PipelineState::Recording, Some(session_id))
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
                record_credential: None,
                upload_credential: None,
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
                record_credential: None,
                upload_credential: None,
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
            .put_segment(&finalized_segment(session_id, 1, flv_path))
            .unwrap();

        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
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
            .put_segment(&finalized_segment(session_id, 1, flv_path))
            .unwrap();

        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
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
            .put_segment(&finalized_segment(session_id, 1, flv_path))
            .unwrap();

        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
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
            .put_segment(&finalized_segment(session_id, 1, flv_path.clone()))
            .unwrap();

        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
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
            .put_segment(&finalized_segment(session_id, 1, flv_path.clone()))
            .unwrap();

        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
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
            .put_segment(&finalized_segment(session_id, 1, flv_path.clone()))
            .unwrap();

        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
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
            .put_segment(&finalized_segment(session_id, 1, real_path))
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

    // --- resolve_submission tests ---

    #[test]
    fn resolve_pending_to_submitted_with_aid() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();
        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
                status: SubmissionStatus::Pending,
                aid: None,
                bvid: None,
                error: None,
            })
            .unwrap();

        let resolved = resolve_submission(
            &store,
            session_id,
            SubmissionStatus::Submitted,
            Some(12345),
            None,
        )
        .unwrap();
        assert_eq!(resolved.from, SubmissionStatus::Pending);
        assert_eq!(resolved.to, SubmissionStatus::Submitted);
        assert_eq!(resolved.aid, Some(12345));

        let sub = store.get_submission(session_id).unwrap().unwrap();
        assert_eq!(sub.status, SubmissionStatus::Submitted);
        assert_eq!(sub.aid, Some(12345));
        assert!(sub.error.is_none());
    }

    #[test]
    fn resolve_ambiguous_to_submitted_with_bvid() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();
        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
                status: SubmissionStatus::Ambiguous,
                aid: None,
                bvid: None,
                error: Some("no aid/bvid in response".into()),
            })
            .unwrap();

        let resolved = resolve_submission(
            &store,
            session_id,
            SubmissionStatus::Submitted,
            None,
            Some("BV1xxx".into()),
        )
        .unwrap();
        assert_eq!(resolved.from, SubmissionStatus::Ambiguous);
        assert_eq!(resolved.to, SubmissionStatus::Submitted);
        assert_eq!(resolved.bvid.as_deref(), Some("BV1xxx"));

        let sub = store.get_submission(session_id).unwrap().unwrap();
        assert_eq!(sub.status, SubmissionStatus::Submitted);
        // Original error string cleared on Submitted resolve.
        assert!(sub.error.is_none());
    }

    #[test]
    fn resolve_pending_to_failed_records_origin() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();
        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
                status: SubmissionStatus::Pending,
                aid: None,
                bvid: None,
                error: None,
            })
            .unwrap();

        resolve_submission(&store, session_id, SubmissionStatus::Failed, None, None).unwrap();

        let sub = store.get_submission(session_id).unwrap().unwrap();
        assert_eq!(sub.status, SubmissionStatus::Failed);
        assert!(sub.error.as_deref().unwrap().contains("Manually resolved"));
        assert!(sub.error.as_deref().unwrap().contains("Pending"));
    }

    #[test]
    fn resolve_to_submitted_without_aid_or_bvid_is_refused() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();
        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
                status: SubmissionStatus::Pending,
                aid: None,
                bvid: None,
                error: None,
            })
            .unwrap();

        let err = resolve_submission(&store, session_id, SubmissionStatus::Submitted, None, None)
            .unwrap_err();
        assert!(err.to_string().contains("--aid"));

        // Submission is untouched.
        let sub = store.get_submission(session_id).unwrap().unwrap();
        assert_eq!(sub.status, SubmissionStatus::Pending);
    }

    #[test]
    fn resolve_refuses_already_submitted() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();
        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
                status: SubmissionStatus::Submitted,
                aid: Some(1),
                bvid: Some("BV1".into()),
                error: None,
            })
            .unwrap();

        let err = resolve_submission(
            &store,
            session_id,
            SubmissionStatus::Submitted,
            Some(2),
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("not Pending or Ambiguous"));

        // Submission preserved verbatim.
        let sub = store.get_submission(session_id).unwrap().unwrap();
        assert_eq!(sub.aid, Some(1));
    }

    #[test]
    fn resolve_refuses_already_failed() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();
        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
                status: SubmissionStatus::Failed,
                aid: None,
                bvid: None,
                error: Some("real failure".into()),
            })
            .unwrap();

        let err = resolve_submission(&store, session_id, SubmissionStatus::Failed, None, None)
            .unwrap_err();
        assert!(err.to_string().contains("not Pending or Ambiguous"));

        let sub = store.get_submission(session_id).unwrap().unwrap();
        assert_eq!(sub.error.as_deref(), Some("real failure"));
    }

    #[test]
    fn resolve_with_unknown_session_errors() {
        let (store, _dir) = test_store();
        let err = resolve_submission(&store, Uuid::new_v4(), SubmissionStatus::Failed, None, None)
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn resolve_to_invalid_target_is_refused() {
        let (store, _dir) = test_store();
        let session_id = Uuid::new_v4();
        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
                status: SubmissionStatus::Pending,
                aid: None,
                bvid: None,
                error: None,
            })
            .unwrap();

        // Resolving to Pending or Ambiguous makes no sense — those are the
        // states we're resolving *from*.
        for bad in [SubmissionStatus::Pending, SubmissionStatus::Ambiguous] {
            let err = resolve_submission(&store, session_id, bad, None, None).unwrap_err();
            assert!(err.to_string().contains("target must be"));
        }
    }
}
