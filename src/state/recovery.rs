use std::collections::{HashMap, HashSet};

use crate::error::{AppError, AppResult};
use crate::pipeline::state_machine::RoomState;
use crate::state::model::{
    PersistedRoomState, SegmentStatus, SessionStatus, Submission, SubmissionStatus,
};
use crate::state::store::StateStore;
use crate::uploader::types::Uploader;
use crate::uploader::validation::{
    PersistedUploadFailure, upload_and_persist_segment, validate_finalized_segment_for_upload,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnomalyKind {
    InterruptedSession,
    InterruptedSegment,
    FailedRoom,
    MissingSegmentFile,
    AmbiguousUpload,
    PendingSubmission,
    AmbiguousSubmission,
    FailedSubmission,
    ActiveRoom,
}

#[derive(Debug, Clone)]
pub struct Anomaly {
    pub kind: AnomalyKind,
    pub description: String,
}

fn is_active_room_state(state: RoomState) -> bool {
    state.requires_active_session()
}

fn is_recording_room_state(state: RoomState) -> bool {
    is_active_room_state(state)
}

fn room_marks_session_active(
    room_state: Option<&PersistedRoomState>,
    session_id: uuid::Uuid,
) -> bool {
    room_state.is_some_and(|room_state| {
        is_active_room_state(room_state.state) && room_state.active_session_id == Some(session_id)
    })
}

fn room_marks_session_recording(
    room_state: Option<&PersistedRoomState>,
    session_id: uuid::Uuid,
) -> bool {
    room_state.is_some_and(|room_state| {
        is_recording_room_state(room_state.state)
            && room_state.active_session_id == Some(session_id)
    })
}

struct RecoveryContext {
    sessions: Vec<crate::state::model::LiveSession>,
    segments: Vec<crate::state::model::Segment>,
    submissions: Vec<Submission>,
    room_by_id: HashMap<u64, PersistedRoomState>,
    uploaded_by_session: HashMap<uuid::Uuid, HashSet<u32>>,
    session_room_key: HashMap<uuid::Uuid, String>,
}

impl RecoveryContext {
    fn load(store: &StateStore) -> AppResult<Self> {
        let sessions = store.list_all_sessions()?;
        let segments = store.list_all_segments()?;
        let submissions = store.list_all_submissions()?;
        let room_by_id: HashMap<u64, PersistedRoomState> =
            store.list_all_room_states()?.into_iter().collect();

        let mut uploaded_by_session: HashMap<uuid::Uuid, HashSet<u32>> = HashMap::new();
        for part in store.list_all_uploaded_parts()? {
            uploaded_by_session
                .entry(part.session_id)
                .or_default()
                .insert(part.segment_index);
        }

        let session_room_key = sessions
            .iter()
            .map(|session| (session.id, session.room_key.clone()))
            .collect();

        Ok(Self {
            sessions,
            segments,
            submissions,
            room_by_id,
            uploaded_by_session,
            session_room_key,
        })
    }

    fn session_room_state(
        &self,
        session: &crate::state::model::LiveSession,
    ) -> Option<&PersistedRoomState> {
        session
            .room_key
            .parse::<u64>()
            .ok()
            .and_then(|room_id| self.room_by_id.get(&room_id))
    }

    fn segment_room_state(
        &self,
        segment: &crate::state::model::Segment,
    ) -> Option<&PersistedRoomState> {
        self.session_room_key
            .get(&segment.session_id)
            .and_then(|room_key| room_key.parse::<u64>().ok())
            .and_then(|room_id| self.room_by_id.get(&room_id))
    }

    fn is_session_interrupted(&self, session: &crate::state::model::LiveSession) -> bool {
        session.status == SessionStatus::Recording
            && !room_marks_session_active(self.session_room_state(session), session.id)
    }

    fn is_segment_interrupted(&self, segment: &crate::state::model::Segment) -> bool {
        segment.status == SegmentStatus::Recording
            && !room_marks_session_recording(self.segment_room_state(segment), segment.session_id)
    }

    fn has_uploaded_part(&self, session_id: uuid::Uuid, index: u32) -> bool {
        self.uploaded_by_session
            .get(&session_id)
            .is_some_and(|indices| indices.contains(&index))
    }
}

pub fn detect_anomalies(store: &StateStore) -> AppResult<Vec<Anomaly>> {
    let ctx = RecoveryContext::load(store)?;
    let mut anomalies = Vec::new();

    for session in &ctx.sessions {
        if ctx.is_session_interrupted(session) {
            anomalies.push(Anomaly {
                kind: AnomalyKind::InterruptedSession,
                description: format!(
                    "Session {} (room {}) is stuck in Recording without an active room state",
                    session.id, session.room_key
                ),
            });
        }
    }

    for segment in &ctx.segments {
        match segment.status {
            SegmentStatus::Recording if ctx.is_segment_interrupted(segment) => {
                anomalies.push(Anomaly {
                    kind: AnomalyKind::InterruptedSegment,
                    description: format!(
                        "Segment {}/{} is stuck in Recording: {}",
                        segment.session_id,
                        segment.index,
                        segment.path.display()
                    ),
                });
            }
            SegmentStatus::Finalized => {
                if !segment.path.exists() {
                    anomalies.push(Anomaly {
                        kind: AnomalyKind::MissingSegmentFile,
                        description: format!(
                            "Finalized segment {}/{} references missing file: {}",
                            segment.session_id,
                            segment.index,
                            segment.path.display()
                        ),
                    });
                }
            }
            SegmentStatus::Uploading => {
                anomalies.push(Anomaly {
                    kind: AnomalyKind::AmbiguousUpload,
                    description: format!(
                        "Segment {}/{} is Uploading; remote outcome is unknown",
                        segment.session_id, segment.index
                    ),
                });
            }
            SegmentStatus::Uploaded | SegmentStatus::Cleaned
                if !ctx.has_uploaded_part(segment.session_id, segment.index) =>
            {
                anomalies.push(Anomaly {
                    kind: AnomalyKind::AmbiguousUpload,
                    description: format!(
                        "Segment {}/{} is {:?} but has no UploadedPart",
                        segment.session_id, segment.index, segment.status
                    ),
                });
            }
            SegmentStatus::Recording
            | SegmentStatus::Filtered
            | SegmentStatus::Uploaded
            | SegmentStatus::Cleaned
            | SegmentStatus::Failed => {}
        }
    }

    for submission in &ctx.submissions {
        match submission.status {
            SubmissionStatus::Pending => anomalies.push(Anomaly {
                kind: AnomalyKind::PendingSubmission,
                description: format!(
                    "Submission for session {} is Pending; outcome unknown",
                    submission.session_id
                ),
            }),
            SubmissionStatus::Ambiguous => anomalies.push(Anomaly {
                kind: AnomalyKind::AmbiguousSubmission,
                description: format!(
                    "Submission for session {} is Ambiguous; manual verification required",
                    submission.session_id
                ),
            }),
            SubmissionStatus::Failed => anomalies.push(Anomaly {
                kind: AnomalyKind::FailedSubmission,
                description: format!(
                    "Submission for session {} Failed: {}",
                    submission.session_id,
                    submission.error.as_deref().unwrap_or("no error message")
                ),
            }),
            SubmissionStatus::Submitted => {}
        }
    }

    for (room_id, room_state) in &ctx.room_by_id {
        match room_state.state {
            RoomState::Failed => anomalies.push(Anomaly {
                kind: AnomalyKind::FailedRoom,
                description: format!("Room {room_id} is Failed and requires explicit reset"),
            }),
            RoomState::Recording | RoomState::ReResolving | RoomState::WaitingReconnect => {
                anomalies.push(Anomaly {
                    kind: AnomalyKind::ActiveRoom,
                    description: format!(
                        "Room {room_id} persisted in {:?}; may be resumed by run",
                        room_state.state
                    ),
                })
            }
            RoomState::Idle | RoomState::Resolving | RoomState::Offline => {}
        }
    }

    Ok(anomalies)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryAction {
    MarkInterruptedSession {
        session_id: uuid::Uuid,
    },
    MarkInterruptedSegment {
        session_id: uuid::Uuid,
        index: u32,
    },
    ResetRoom {
        room_id: u64,
    },
    ScheduleUploadReconciliation {
        session_id: uuid::Uuid,
        segment_index: u32,
        path: std::path::PathBuf,
    },
    LeaveAsIs {
        reason: String,
    },
}

impl std::fmt::Display for RecoveryAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecoveryAction::MarkInterruptedSession { session_id } => write!(
                f,
                "Would mark session {} as Failed: interrupted recording",
                session_id
            ),
            RecoveryAction::MarkInterruptedSegment { session_id, index } => write!(
                f,
                "Would mark segment {}/{} as Failed: interrupted recording",
                session_id, index
            ),
            RecoveryAction::ResetRoom { room_id } => {
                write!(f, "Would reset room {} from Failed to Idle", room_id)
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
            RecoveryAction::LeaveAsIs { reason } => write!(f, "Would leave unchanged: {reason}"),
        }
    }
}

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
        for (idx, action) in self.actions.iter().enumerate() {
            if idx > 0 {
                writeln!(f)?;
            }
            write!(f, "{action}")?;
        }
        Ok(())
    }
}

pub fn plan_has_upload_actions(plan: &RecoveryPlan) -> bool {
    plan.actions
        .iter()
        .any(|action| matches!(action, RecoveryAction::ScheduleUploadReconciliation { .. }))
}

pub fn plan_recovery(
    store: &StateStore,
    reset_rooms: &HashSet<u64>,
    retry_upload_sessions: &HashSet<uuid::Uuid>,
) -> AppResult<RecoveryPlan> {
    let ctx = RecoveryContext::load(store)?;
    let mut actions = Vec::new();
    let submissions_by_session: HashMap<uuid::Uuid, &Submission> =
        ctx.submissions.iter().map(|s| (s.session_id, s)).collect();

    for session in &ctx.sessions {
        if ctx.is_session_interrupted(session) {
            actions.push(RecoveryAction::MarkInterruptedSession {
                session_id: session.id,
            });
        }
    }

    for segment in &ctx.segments {
        if ctx.is_segment_interrupted(segment) {
            actions.push(RecoveryAction::MarkInterruptedSegment {
                session_id: segment.session_id,
                index: segment.index,
            });
        }
    }

    for segment in &ctx.segments {
        if segment.status != SegmentStatus::Finalized
            || ctx.has_uploaded_part(segment.session_id, segment.index)
            || !retry_upload_sessions.contains(&segment.session_id)
        {
            continue;
        }

        if let Some(submission) = submissions_by_session.get(&segment.session_id) {
            actions.push(RecoveryAction::LeaveAsIs {
                reason: format!(
                    "Finalized segment {}/{} missing upload, but session has a {:?} submission",
                    segment.session_id, segment.index, submission.status
                ),
            });
            continue;
        }

        if !segment.path.exists() {
            actions.push(RecoveryAction::LeaveAsIs {
                reason: format!(
                    "Finalized segment {}/{} missing upload, but file does not exist: {}",
                    segment.session_id,
                    segment.index,
                    segment.path.display()
                ),
            });
            continue;
        }
        if !segment
            .path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("flv"))
        {
            actions.push(RecoveryAction::LeaveAsIs {
                reason: format!(
                    "Finalized segment {}/{} missing upload, but file is not .flv: {}",
                    segment.session_id,
                    segment.index,
                    segment.path.display()
                ),
            });
            continue;
        }

        actions.push(RecoveryAction::ScheduleUploadReconciliation {
            session_id: segment.session_id,
            segment_index: segment.index,
            path: segment.path.clone(),
        });
    }

    for segment in &ctx.segments {
        if segment.status == SegmentStatus::Uploading
            || matches!(
                segment.status,
                SegmentStatus::Uploaded | SegmentStatus::Cleaned
            ) && !ctx.has_uploaded_part(segment.session_id, segment.index)
        {
            actions.push(RecoveryAction::LeaveAsIs {
                reason: format!(
                    "Segment {}/{} is {:?}; upload outcome requires manual verification",
                    segment.session_id, segment.index, segment.status
                ),
            });
        }
    }

    for submission in &ctx.submissions {
        match submission.status {
            SubmissionStatus::Pending => actions.push(RecoveryAction::LeaveAsIs {
                reason: format!(
                    "Pending submission {} requires manual verification",
                    submission.session_id
                ),
            }),
            SubmissionStatus::Ambiguous => actions.push(RecoveryAction::LeaveAsIs {
                reason: format!(
                    "Ambiguous submission {} requires manual verification and state resolve-submission",
                    submission.session_id
                ),
            }),
            SubmissionStatus::Failed => actions.push(RecoveryAction::LeaveAsIs {
                reason: format!(
                    "Failed submission {} requires manual verification before retry",
                    submission.session_id
                ),
            }),
            SubmissionStatus::Submitted => {}
        }
    }

    for (room_id, room_state) in &ctx.room_by_id {
        if room_state.state == RoomState::Failed {
            if reset_rooms.contains(room_id) {
                actions.push(RecoveryAction::ResetRoom { room_id: *room_id });
            } else {
                actions.push(RecoveryAction::LeaveAsIs {
                    reason: format!(
                        "Room {room_id} is Failed; use --reset-room {room_id} to reset"
                    ),
                });
            }
        } else if is_active_room_state(room_state.state) {
            actions.push(RecoveryAction::LeaveAsIs {
                reason: format!(
                    "Room {room_id} persisted in {:?}; may be resumed by bilive-rec run",
                    room_state.state
                ),
            });
        }
    }

    Ok(RecoveryPlan { actions })
}

#[derive(Debug, Clone)]
pub struct SubmissionResolved {
    pub session_id: uuid::Uuid,
    pub from: SubmissionStatus,
    pub to: SubmissionStatus,
    pub aid: Option<u64>,
    pub bvid: Option<String>,
}

pub fn resolve_submission(
    store: &StateStore,
    session_id: uuid::Uuid,
    target: SubmissionStatus,
    aid: Option<u64>,
    bvid: Option<String>,
) -> AppResult<SubmissionResolved> {
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
                "Submission for session {session_id} is {other:?}, not Pending or Ambiguous"
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

#[derive(Debug)]
pub enum ApplyResult {
    Applied(String),
    Skipped(String),
}

pub async fn apply_recovery<U: Uploader>(
    store: &StateStore,
    plan: &RecoveryPlan,
    uploader: Option<&U>,
) -> AppResult<Vec<ApplyResult>> {
    let mut results = Vec::new();

    for action in &plan.actions {
        let result = match action {
            RecoveryAction::MarkInterruptedSession { session_id } => {
                match store.get_session(*session_id)? {
                    Some(session) if session.status == SessionStatus::Recording => {
                        let mut updated = session;
                        updated.status = SessionStatus::Failed;
                        store.put_session(&updated)?;
                        ApplyResult::Applied(format!("Marked session {session_id} as Failed"))
                    }
                    Some(session) => ApplyResult::Skipped(format!(
                        "Session {} is {:?}, not Recording",
                        session_id, session.status
                    )),
                    None => ApplyResult::Skipped(format!("Session {session_id} not found")),
                }
            }
            RecoveryAction::MarkInterruptedSegment { session_id, index } => {
                match store.get_segment(*session_id, *index)? {
                    Some(segment) if segment.status == SegmentStatus::Recording => {
                        let mut updated = segment;
                        updated.status = SegmentStatus::Failed;
                        updated.error = Some("Interrupted by hard crash".into());
                        store.put_segment(&updated)?;
                        ApplyResult::Applied(format!(
                            "Marked segment {session_id}/{index} as Failed"
                        ))
                    }
                    Some(segment) => ApplyResult::Skipped(format!(
                        "Segment {}/{} is {:?}, not Recording",
                        session_id, index, segment.status
                    )),
                    None => ApplyResult::Skipped(format!("Segment {session_id}/{index} not found")),
                }
            }
            RecoveryAction::ResetRoom { room_id } => match store.get_room_state_value(*room_id)? {
                Some(RoomState::Failed) => {
                    store.put_room_state_value(*room_id, RoomState::Idle)?;
                    ApplyResult::Applied(format!("Reset room {room_id} from Failed to Idle"))
                }
                Some(other) => {
                    ApplyResult::Skipped(format!("Room {room_id} is {other:?}, not Failed"))
                }
                None => ApplyResult::Skipped(format!("Room {room_id} has no state")),
            },
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
                                    return Err(AppError::State(format!(
                                        "Failed to persist pre-upload state for segment {}/{}: {}",
                                        session_id, segment_index, error
                                    )));
                                }
                                Err(PersistedUploadFailure::StateAfterRemote { error, .. }) => {
                                    return Err(AppError::State(format!(
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
    use crate::credential::CredentialIdentity;
    use crate::state::model::{LiveSession, Segment};
    use tempfile::TempDir;

    fn store() -> (StateStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(dir.path().join("state.redb")).unwrap();
        (store, dir)
    }

    fn session(status: SessionStatus) -> LiveSession {
        LiveSession {
            id: uuid::Uuid::new_v4(),
            room_key: "1".into(),
            title: "title".into(),
            started_at: jiff::Timestamp::now(),
            status,
            record_credential: None,
            upload_credential: Some(CredentialIdentity::new("main", "cookies.json")),
        }
    }

    #[test]
    fn finalized_missing_upload_is_worker_backlog_not_anomaly() {
        let (store, dir) = store();
        let s = session(SessionStatus::Finalized);
        store.put_session(&s).unwrap();
        let path = dir.path().join("0.flv");
        std::fs::write(&path, b"flv").unwrap();
        store
            .put_segment(&Segment {
                session_id: s.id,
                index: 0,
                path,
                status: SegmentStatus::Finalized,
                close_reason: None,
                error: None,
            })
            .unwrap();

        assert!(detect_anomalies(&store).unwrap().is_empty());
        assert!(
            plan_recovery(&store, &HashSet::new(), &HashSet::new())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn failed_room_requires_explicit_reset() {
        let (store, _dir) = store();
        store.put_room_state_value(1, RoomState::Failed).unwrap();
        let plan = plan_recovery(&store, &HashSet::new(), &HashSet::new()).unwrap();
        assert!(matches!(plan.actions[0], RecoveryAction::LeaveAsIs { .. }));

        let plan = plan_recovery(&store, &HashSet::from([1]), &HashSet::new()).unwrap();
        assert!(matches!(
            plan.actions[0],
            RecoveryAction::ResetRoom { room_id: 1 }
        ));
    }
}
