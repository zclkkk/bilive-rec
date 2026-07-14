use uuid::Uuid;

use crate::error::{AppError, AppResult};
use crate::state::model::{
    ArtifactResolution, ArtifactResolutionDecision, ArtifactState, LiveSession, OutputPlan,
    RecordingDecision, RecordingEvent, RemoteAttempt, RemoteOperationRef, RoomLifecycle, RoomState,
    Segment, SegmentCloseReason, SessionClosure, SessionLifecycle, Submission, SubmissionAttempt,
    SubmissionAttemptOutcome, SubmissionResolution, SubmissionResolutionDecision, SubmissionState,
    UploadAttempt, UploadAttemptOutcome, UploadResolution, UploadResolutionDecision, UploadState,
    UploadTarget, UploadTargetGate, UploadTargetState, UploadedPart,
};
use crate::state::store::{StateStore, StoreTxn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseSessionRequest {
    Natural {
        note: Option<String>,
    },
    Recover {
        exclude_failed: bool,
        note: Option<String>,
    },
    Abandon {
        note: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseSessionResult {
    pub session_id: Uuid,
    pub lifecycle: SessionLifecycle,
    pub excluded_segments: Vec<u32>,
}

pub fn create_session(store: &StateStore, session: &LiveSession) -> AppResult<()> {
    if session.lifecycle != SessionLifecycle::Open {
        return Err(state_error(format!(
            "session {} must be Open when created",
            session.id
        )));
    }
    if !session.recording_events.is_empty() {
        return Err(state_error(format!(
            "session {} must not have recording events when created",
            session.id
        )));
    }
    if !session.recording_plan.output_dir.is_absolute() {
        return Err(state_error(format!(
            "session {} recording output locator must be absolute",
            session.id
        )));
    }
    if session
        .recording_plan
        .credential
        .as_ref()
        .is_some_and(|credential| !credential.cookie_file.is_absolute())
    {
        return Err(state_error(format!(
            "session {} recording credential locator must be absolute",
            session.id
        )));
    }
    if let Some(upload) = session.output_plan.upload_plan() {
        if upload.threads == 0 || upload.line.trim().is_empty() {
            return Err(state_error(
                "Bilibili output requires a non-empty upload line and at least one upload thread",
            ));
        }
        if upload.principal.expected_mid == 0 {
            return Err(state_error(
                "Bilibili upload principal requires a non-zero expected mid",
            ));
        }
        if !upload.principal.cookie_file().is_absolute() {
            return Err(state_error(
                "Bilibili upload credential locator must be absolute",
            ));
        }
    }
    store.write(|txn| {
        if txn.get_session(session.id)?.is_some() {
            return Err(state_error(format!(
                "session {} already exists",
                session.id
            )));
        }
        if let Some(existing) = txn.list_sessions()?.into_iter().find(|existing| {
            existing.room_id == session.room_id
                && !matches!(existing.lifecycle, SessionLifecycle::Closed { .. })
        }) {
            return Err(state_error(format!(
                "room {} already has non-closed session {}",
                session.room_id, existing.id
            )));
        }
        if let Some(room) = txn.get_room_state(session.room_id)?
            && room.lifecycle != RoomLifecycle::Ready
        {
            return Err(state_error(format!(
                "room {} is already {:?}",
                session.room_id, room.lifecycle
            )));
        }
        txn.put_session(session)?;
        txn.put_room_state(
            session.room_id,
            &room_state(
                RoomLifecycle::Owned {
                    session_id: session.id,
                },
                None,
            ),
        )?;
        if let Some(plan) = session.output_plan.upload_plan() {
            let target = UploadTarget::from(plan);
            if txn.get_upload_target_state(&target)?.is_none() {
                txn.put_upload_target_state(&UploadTargetState {
                    target,
                    gate: UploadTargetGate::Ready,
                })?;
            }
        }
        Ok(())
    })
}

/// Persist a new Writing segment. The caller-provided upload state is ignored:
/// upload intent is derived from the frozen session output plan in this same
/// transaction.
pub fn open_segment(store: &StateStore, mut segment: Segment) -> AppResult<Segment> {
    if segment.part_path == segment.final_path {
        return Err(state_error(
            "segment part_path and final_path must be different",
        ));
    }
    if !segment.part_path.is_absolute() || !segment.final_path.is_absolute() {
        return Err(state_error(
            "segment part_path and final_path locators must be absolute",
        ));
    }
    store.write(|txn| {
        let session = require_session(txn, segment.session_id)?;
        require_open_owned_session(txn, &session)?;
        if txn
            .get_segment(segment.session_id, segment.index)?
            .is_some()
        {
            return Err(state_error(format!(
                "segment {}/{} already exists",
                segment.session_id, segment.index
            )));
        }
        let next_index = txn
            .list_segments(segment.session_id)?
            .last()
            .map_or(1, |last| last.index.saturating_add(1));
        if segment.index != next_index {
            return Err(state_error(format!(
                "segment {}/{} is out of sequence; expected index {next_index}",
                segment.session_id, segment.index
            )));
        }
        if segment.artifact != ArtifactState::Writing {
            return Err(state_error(format!(
                "new segment {}/{} must be Writing",
                segment.session_id, segment.index
            )));
        }
        segment.upload = match session.output_plan {
            OutputPlan::LocalOnly => UploadState::NotPlanned,
            OutputPlan::Bilibili { .. } => UploadState::pending(),
        };
        segment.artifact_resolutions.clear();
        segment.upload_attempts.clear();
        segment.upload_resolutions.clear();
        txn.put_segment(&segment)?;
        Ok(segment)
    })
}

pub fn begin_artifact_finalization(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    close_reason: SegmentCloseReason,
) -> AppResult<Segment> {
    update_recording_artifact(store, session_id, index, true, |segment| {
        if segment.artifact != ArtifactState::Writing {
            return Err(unexpected_artifact(segment, "Writing"));
        }
        segment.artifact = ArtifactState::Finalizing { close_reason };
        Ok(())
    })
}

pub fn complete_artifact_finalization(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
) -> AppResult<Segment> {
    update_recording_artifact(store, session_id, index, false, |segment| {
        let ArtifactState::Finalizing { close_reason } = &segment.artifact else {
            return Err(unexpected_artifact(segment, "Finalizing"));
        };
        segment.artifact = ArtifactState::Ready {
            close_reason: close_reason.clone(),
        };
        Ok(())
    })
}

pub fn begin_artifact_discard(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    close_reason: SegmentCloseReason,
) -> AppResult<Segment> {
    update_recording_artifact(store, session_id, index, true, |segment| {
        if segment.artifact != ArtifactState::Writing {
            return Err(unexpected_artifact(segment, "Writing"));
        }
        segment.artifact = ArtifactState::Discarding { close_reason };
        Ok(())
    })
}

pub fn complete_artifact_discard(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
) -> AppResult<Segment> {
    update_recording_artifact(store, session_id, index, false, |segment| {
        let ArtifactState::Discarding { close_reason } = &segment.artifact else {
            return Err(unexpected_artifact(segment, "Discarding"));
        };
        segment.artifact = ArtifactState::Filtered {
            close_reason: close_reason.clone(),
        };
        segment.upload = UploadState::NotPlanned;
        Ok(())
    })
}

/// Persist an operator's decision before touching either side of a
/// finalization conflict. The file committer can replay this intent after a
/// crash without asking the operator to choose again.
pub fn begin_artifact_conflict_resolution(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    decision: ArtifactResolutionDecision,
    note: Option<String>,
) -> AppResult<Segment> {
    store.write(|txn| {
        let session = require_session(txn, session_id)?;
        if !matches!(session.lifecycle, SessionLifecycle::RecoveryRequired { .. }) {
            return Err(state_error(format!(
                "session {session_id} must be RecoveryRequired before resolving an artifact conflict"
            )));
        }
        require_or_claim_recovery_owner(txn, &session)?;
        let mut segment = require_segment(txn, session_id, index)?;
        let close_reason = match &segment.artifact {
            ArtifactState::Finalizing { close_reason }
            | ArtifactState::Discarding { close_reason } => close_reason.clone(),
            _ => return Err(unexpected_artifact(&segment, "Finalizing or Discarding")),
        };
        segment.artifact_resolutions.push(ArtifactResolution {
            decided_at: jiff::Timestamp::now(),
            decision,
            note,
        });
        segment.artifact = ArtifactState::ResolvingConflict {
            close_reason,
            decision,
        };
        txn.put_segment(&segment)?;
        Ok(segment)
    })
}

pub fn complete_artifact_conflict_resolution(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
) -> AppResult<Segment> {
    update_recording_artifact(store, session_id, index, false, |segment| {
        let ArtifactState::ResolvingConflict {
            close_reason,
            decision,
        } = &segment.artifact
        else {
            return Err(unexpected_artifact(segment, "ResolvingConflict"));
        };
        if *decision == ArtifactResolutionDecision::Exclude {
            return Err(state_error(format!(
                "segment {session_id}/{index} conflict decision is Exclude, not a keep decision"
            )));
        }
        segment.artifact = ArtifactState::Ready {
            close_reason: close_reason.clone(),
        };
        Ok(())
    })
}

pub fn exclude_artifact_conflict(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
) -> AppResult<Segment> {
    update_recording_artifact(store, session_id, index, false, |segment| {
        let ArtifactState::ResolvingConflict { decision, .. } = &segment.artifact else {
            return Err(unexpected_artifact(segment, "ResolvingConflict"));
        };
        if *decision != ArtifactResolutionDecision::Exclude {
            return Err(state_error(format!(
                "segment {session_id}/{index} conflict decision is {decision:?}, not Exclude"
            )));
        }
        segment.artifact = ArtifactState::Excluded {
            reason: "operator excluded a conflicting part/final artifact pair".into(),
        };
        segment.upload = UploadState::NotPlanned;
        Ok(())
    })
}

pub fn fail_artifact(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    reason: String,
    close_reason: Option<SegmentCloseReason>,
) -> AppResult<Segment> {
    if reason.trim().is_empty() {
        return Err(state_error("artifact failure reason must not be empty"));
    }
    store.write(|txn| {
        let mut session = require_session(txn, session_id)?;
        if matches!(session.lifecycle, SessionLifecycle::Closed { .. }) {
            return Err(state_error(format!(
                "session {session_id} is already closed"
            )));
        }
        require_or_claim_recovery_owner(txn, &session)?;
        let mut segment = require_segment(txn, session_id, index)?;
        if !matches!(
            segment.artifact,
            ArtifactState::Writing
                | ArtifactState::Finalizing { .. }
                | ArtifactState::Discarding { .. }
                | ArtifactState::ResolvingConflict { .. }
        ) {
            return Err(unexpected_artifact(
                &segment,
                "Writing, Finalizing, Discarding, or ResolvingConflict",
            ));
        }
        let close_reason = close_reason.or_else(|| segment.artifact.close_reason().cloned());
        segment.artifact = ArtifactState::Failed {
            reason,
            close_reason,
        };
        txn.put_segment(&segment)?;

        let recovery_reason = format!(
            "segment {session_id}/{index} failed: {}",
            match &segment.artifact {
                ArtifactState::Failed { reason, .. } => reason.as_str(),
                _ => unreachable!(),
            }
        );
        let detected_at = jiff::Timestamp::now();
        let primary_reason = record_recovery_reason(&mut session, recovery_reason, detected_at);
        txn.put_session(&session)?;
        txn.put_room_state(
            session.room_id,
            &room_state(RoomLifecycle::Blocked { session_id }, Some(primary_reason)),
        )?;
        Ok(segment)
    })
}

pub fn begin_artifact_delete(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
) -> AppResult<Segment> {
    store.write(|txn| {
        let session = require_session(txn, session_id)?;
        if !session.lifecycle.permits_submission() {
            return Err(state_error(format!(
                "session {session_id} is not Completed; refusing artifact deletion"
            )));
        }
        if !session
            .output_plan
            .upload_plan()
            .is_some_and(|plan| plan.delete_after_submit)
        {
            return Err(state_error(format!(
                "session {session_id} does not permit delete_after_submit"
            )));
        }
        let Some(submission) = txn.get_submission(session_id)? else {
            return Err(state_error(format!(
                "session {session_id} has no submission; refusing artifact deletion"
            )));
        };
        if !matches!(submission.state, SubmissionState::Submitted { .. }) {
            return Err(state_error(format!(
                "session {session_id} submission is not Submitted"
            )));
        }
        let mut segment = require_segment(txn, session_id, index)?;
        if !matches!(segment.artifact, ArtifactState::Ready { .. })
            || !matches!(segment.upload, UploadState::Uploaded { .. })
        {
            return Err(state_error(format!(
                "segment {session_id}/{index} must be Ready and Uploaded before deletion"
            )));
        }
        segment.artifact = ArtifactState::Deleting;
        txn.put_segment(&segment)?;
        Ok(segment)
    })
}

pub fn complete_artifact_delete(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
) -> AppResult<Segment> {
    update_artifact(store, session_id, index, |segment| {
        if segment.artifact != ArtifactState::Deleting {
            return Err(unexpected_artifact(segment, "Deleting"));
        }
        segment.artifact = ArtifactState::Deleted;
        Ok(())
    })
}

pub fn require_recovery(
    store: &StateStore,
    session_id: Uuid,
    reason: String,
) -> AppResult<SessionLifecycle> {
    if reason.trim().is_empty() {
        return Err(state_error("recovery reason must not be empty"));
    }
    store.write(|txn| {
        let mut session = require_session(txn, session_id)?;
        match &session.lifecycle {
            SessionLifecycle::Open => {}
            SessionLifecycle::RecoveryRequired {
                reason: current_reason,
                ..
            } => {
                if current_reason != &reason
                    && !session.recording_events.iter().any(|event| {
                        matches!(event, RecordingEvent::RecoveryRequired { reason: recorded, .. } if recorded == &reason)
                    })
                {
                    session.recording_events.push(RecordingEvent::RecoveryRequired {
                        detected_at: jiff::Timestamp::now(),
                        reason: reason.clone(),
                    });
                    txn.put_session(&session)?;
                }
                require_or_claim_recovery_owner(txn, &session)?;
                txn.put_room_state(
                    session.room_id,
                    &room_state(
                        RoomLifecycle::Blocked { session_id },
                        Some(current_reason.clone()),
                    ),
                )?;
                return Ok(session.lifecycle);
            }
            SessionLifecycle::Closed { .. } => {
                return Err(state_error(format!(
                    "session {session_id} is already closed"
                )));
            }
        }
        require_or_claim_recovery_owner(txn, &session)?;
        let detected_at = jiff::Timestamp::now();
        session.lifecycle = SessionLifecycle::RecoveryRequired {
            reason: reason.clone(),
            detected_at,
        };
        session
            .recording_events
            .push(RecordingEvent::RecoveryRequired {
                detected_at,
                reason: reason.clone(),
            });
        txn.put_session(&session)?;
        txn.put_room_state(
            session.room_id,
            &room_state(RoomLifecycle::Blocked { session_id }, Some(reason)),
        )?;
        Ok(session.lifecycle)
    })
}

pub fn close_session(
    store: &StateStore,
    session_id: Uuid,
    request: CloseSessionRequest,
) -> AppResult<CloseSessionResult> {
    store.write(|txn| close_session_txn(txn, session_id, request))
}

fn close_session_txn(
    txn: &StoreTxn<'_>,
    session_id: Uuid,
    request: CloseSessionRequest,
) -> AppResult<CloseSessionResult> {
    let mut session = require_session(txn, session_id)?;
    let mut segments = txn.list_segments(session_id)?;
    let mut excluded_segments = Vec::new();

    match &request {
        CloseSessionRequest::Natural { .. } if session.lifecycle != SessionLifecycle::Open => {
            return Err(state_error(format!(
                "session {session_id} must be Open for natural close"
            )));
        }
        CloseSessionRequest::Recover { .. }
            if !matches!(session.lifecycle, SessionLifecycle::RecoveryRequired { .. }) =>
        {
            return Err(state_error(format!(
                "session {session_id} does not require recovery"
            )));
        }
        CloseSessionRequest::Abandon { .. }
            if matches!(session.lifecycle, SessionLifecycle::Closed { .. }) =>
        {
            return Err(state_error(format!(
                "session {session_id} is already closed"
            )));
        }
        _ => {}
    }
    match &request {
        CloseSessionRequest::Natural { .. } => {
            require_room_owner(txn, &session)?;
        }
        CloseSessionRequest::Recover { .. } | CloseSessionRequest::Abandon { .. } => {
            require_or_claim_recovery_owner(txn, &session)?;
        }
    }
    let is_recover_request = matches!(&request, CloseSessionRequest::Recover { .. });

    if let CloseSessionRequest::Abandon { note } = request {
        if txn.get_submission(session_id)?.is_some() {
            return Err(state_error(format!(
                "session {session_id} already has a submission and cannot be abandoned"
            )));
        }
        if let Some(target) = session.output_plan.upload_plan().map(UploadTarget::from)
            && let Some(state) = txn.get_upload_target_state(&target)?
            && matches!(state.gate, UploadTargetGate::Blocked { ref owner, .. } if owner.session_id() == session_id)
        {
            return Err(state_error(format!(
                "session {session_id} owns the blocked upload target; resolve that target failure before abandoning the session"
            )));
        }
        for segment in &mut segments {
            if matches!(
                segment.artifact,
                ArtifactState::Writing
                    | ArtifactState::Finalizing { .. }
                    | ArtifactState::Discarding { .. }
                    | ArtifactState::ResolvingConflict { .. }
                    | ArtifactState::Deleting
            ) {
                return Err(state_error(format!(
                    "segment {session_id}/{} has unresolved artifact intent {:?}",
                    segment.index, segment.artifact
                )));
            }
            abandon_upload(segment)?;
            txn.put_segment(segment)?;
        }
        let now = jiff::Timestamp::now();
        session.lifecycle = SessionLifecycle::Closed {
            closure: SessionClosure::Abandoned {
                closed_at: now,
                note: note.clone(),
            },
        };
        session
            .recording_events
            .push(RecordingEvent::OperatorResolved {
                resolved_at: now,
                decision: RecordingDecision::Abandoned,
                note,
            });
        txn.put_session(&session)?;
        txn.put_room_state(session.room_id, &room_state(RoomLifecycle::Ready, None))?;
        return Ok(CloseSessionResult {
            session_id,
            lifecycle: session.lifecycle,
            excluded_segments,
        });
    }

    if let Some(segment) = segments.iter().find(|segment| {
        matches!(
            segment.artifact,
            ArtifactState::Writing
                | ArtifactState::Finalizing { .. }
                | ArtifactState::Discarding { .. }
                | ArtifactState::ResolvingConflict { .. }
                | ArtifactState::Deleting
        )
    }) {
        let reason = format!(
            "segment {session_id}/{} has unresolved artifact intent {:?}",
            segment.index, segment.artifact
        );
        if is_recover_request {
            return Err(state_error(reason));
        }
        return mark_recovery_required_txn(txn, session, reason);
    }

    let exclude_failed = matches!(
        request,
        CloseSessionRequest::Recover {
            exclude_failed: true,
            ..
        }
    );
    if let Some(segment) = segments
        .iter()
        .find(|segment| matches!(segment.artifact, ArtifactState::Failed { .. }))
        && !exclude_failed
    {
        let reason = format!(
            "segment {session_id}/{} is Failed and requires exclusion or abandonment",
            segment.index
        );
        if is_recover_request {
            return Err(state_error(reason));
        }
        return mark_recovery_required_txn(txn, session, reason);
    }
    if exclude_failed {
        for segment in &mut segments {
            if let ArtifactState::Failed { reason, .. } = &segment.artifact {
                segment.artifact = ArtifactState::Excluded {
                    reason: format!("operator excluded failed segment: {reason}"),
                };
                segment.upload = UploadState::NotPlanned;
                excluded_segments.push(segment.index);
                txn.put_segment(segment)?;
            }
        }
    }

    let now = jiff::Timestamp::now();
    let note = match request {
        CloseSessionRequest::Natural { note } | CloseSessionRequest::Recover { note, .. } => note,
        CloseSessionRequest::Abandon { .. } => unreachable!(),
    };
    let has_usable_recording = segments.iter().any(|segment| segment.artifact.is_usable());
    session.lifecycle = if has_usable_recording {
        SessionLifecycle::Closed {
            closure: SessionClosure::Completed {
                closed_at: now,
                note: note.clone(),
            },
        }
    } else {
        SessionLifecycle::Closed {
            closure: SessionClosure::NoUsableRecording {
                closed_at: now,
                reason: "session closed without a usable recording segment".into(),
            },
        }
    };
    if is_recover_request {
        session
            .recording_events
            .push(RecordingEvent::OperatorResolved {
                resolved_at: now,
                decision: RecordingDecision::Finalized,
                note,
            });
    }
    txn.put_session(&session)?;
    txn.put_room_state(session.room_id, &room_state(RoomLifecycle::Ready, None))?;
    Ok(CloseSessionResult {
        session_id,
        lifecycle: session.lifecycle,
        excluded_segments,
    })
}

fn mark_recovery_required_txn(
    txn: &StoreTxn<'_>,
    mut session: LiveSession,
    reason: String,
) -> AppResult<CloseSessionResult> {
    let detected_at = jiff::Timestamp::now();
    let primary_reason = record_recovery_reason(&mut session, reason, detected_at);
    txn.put_session(&session)?;
    txn.put_room_state(
        session.room_id,
        &room_state(
            RoomLifecycle::Blocked {
                session_id: session.id,
            },
            Some(primary_reason),
        ),
    )?;
    Ok(CloseSessionResult {
        session_id: session.id,
        lifecycle: session.lifecycle,
        excluded_segments: Vec::new(),
    })
}

fn record_recovery_reason(
    session: &mut LiveSession,
    reason: String,
    detected_at: jiff::Timestamp,
) -> String {
    if !session.recording_events.iter().any(|event| {
        matches!(event, RecordingEvent::RecoveryRequired { reason: recorded, .. } if recorded == &reason)
    }) {
        session.recording_events.push(RecordingEvent::RecoveryRequired {
            detected_at,
            reason: reason.clone(),
        });
    }
    if let SessionLifecycle::RecoveryRequired {
        reason: primary, ..
    } = &session.lifecycle
    {
        return primary.clone();
    }
    session.lifecycle = SessionLifecycle::RecoveryRequired {
        reason: reason.clone(),
        detected_at,
    };
    reason
}

pub fn begin_upload(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    attempt: RemoteAttempt,
) -> AppResult<Segment> {
    store.write(|txn| {
        let session = require_session(txn, session_id)?;
        if !session.lifecycle.permits_upload() {
            return Err(state_error(format!(
                "session {session_id} lifecycle does not permit upload"
            )));
        }
        let upload_plan = session
            .output_plan
            .upload_plan()
            .ok_or_else(|| state_error(format!("session {session_id} is configured LocalOnly")))?;
        consume_target_ready(txn, &UploadTarget::from(upload_plan), attempt.started_at)?;
        if txn.get_submission(session_id)?.is_some() {
            return Err(state_error(format!(
                "session {session_id} already has a submission"
            )));
        }
        let mut segment = require_segment(txn, session_id, index)?;
        if !matches!(segment.artifact, ArtifactState::Ready { .. })
            || !matches!(segment.upload, UploadState::Pending { .. })
            || !segment.upload.is_due(attempt.started_at)
        {
            return Err(state_error(format!(
                "segment {session_id}/{index} is not a due Ready/Pending upload"
            )));
        }
        if segment
            .upload_attempts
            .iter()
            .any(|record| record.attempt.id == attempt.id)
        {
            return Err(state_error(format!(
                "upload attempt {} already exists",
                attempt.id
            )));
        }
        segment.upload_attempts.push(UploadAttempt {
            attempt: attempt.clone(),
            finished_at: None,
            outcome: None,
        });
        segment.upload = UploadState::Attempting { attempt };
        txn.put_segment(&segment)?;
        Ok(segment)
    })
}

pub fn complete_upload(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    attempt_id: Uuid,
    proof: UploadedPart,
) -> AppResult<Segment> {
    validate_uploaded_part(&proof)?;
    store.write(|txn| {
        let mut segment = require_segment(txn, session_id, index)?;
        require_current_upload_attempt(&segment, attempt_id)?;
        let now = jiff::Timestamp::now();
        finish_upload_history(
            &mut segment,
            attempt_id,
            now,
            UploadAttemptOutcome::Confirmed {
                proof: proof.clone(),
            },
        )?;
        segment.upload = UploadState::Uploaded { proof };
        txn.put_segment(&segment)?;
        Ok(segment)
    })
}

pub fn schedule_upload_retry(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    attempt_id: Uuid,
    reason: String,
    retry_at: jiff::Timestamp,
) -> AppResult<Segment> {
    schedule_upload_retry_with_target_gate(
        store,
        session_id,
        index,
        attempt_id,
        reason,
        retry_at,
        AttemptTargetGate::Unchanged,
    )
}

pub fn schedule_upload_target_retry(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    attempt_id: Uuid,
    reason: String,
    retry_at: jiff::Timestamp,
    failures: u32,
) -> AppResult<Segment> {
    if failures == 0 {
        return Err(state_error(
            "target failure count must be greater than zero",
        ));
    }
    schedule_upload_retry_with_target_gate(
        store,
        session_id,
        index,
        attempt_id,
        reason.clone(),
        retry_at,
        AttemptTargetGate::Backoff {
            owner: RemoteOperationRef::Upload {
                session_id,
                segment_index: index,
                attempt_id,
            },
            retry_at,
            last_error: reason,
            failures,
        },
    )
}

fn schedule_upload_retry_with_target_gate(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    attempt_id: Uuid,
    reason: String,
    retry_at: jiff::Timestamp,
    target_gate: AttemptTargetGate,
) -> AppResult<Segment> {
    store.write(|txn| {
        let target = session_upload_target(txn, session_id)?;
        let mut segment = require_segment(txn, session_id, index)?;
        require_current_upload_attempt(&segment, attempt_id)?;
        let now = jiff::Timestamp::now();
        finish_upload_history(
            &mut segment,
            attempt_id,
            now,
            UploadAttemptOutcome::RetryScheduled {
                reason: reason.clone(),
            },
        )?;
        let failures = segment
            .upload_attempts
            .iter()
            .filter(|record| {
                matches!(
                    record.outcome,
                    Some(UploadAttemptOutcome::RetryScheduled { .. })
                )
            })
            .count() as u32;
        segment.upload = UploadState::Pending {
            failures,
            retry_at: Some(retry_at),
            last_error: Some(reason.clone()),
        };
        txn.put_segment(&segment)?;
        put_attempt_target_gate_txn(txn, target, target_gate, now)?;
        Ok(segment)
    })
}

pub fn block_upload(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    attempt_id: Uuid,
    reason: String,
) -> AppResult<Segment> {
    finish_upload_attempt(
        store,
        session_id,
        index,
        attempt_id,
        AttemptTargetGate::Unchanged,
        |segment, now| {
            finish_upload_history(
                segment,
                attempt_id,
                now,
                UploadAttemptOutcome::Blocked {
                    reason: reason.clone(),
                },
            )?;
            segment.upload = UploadState::Blocked {
                attempt_id: Some(attempt_id),
                reason,
            };
            Ok(())
        },
    )
}

pub fn block_upload_for_target(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    attempt_id: Uuid,
    reason: String,
) -> AppResult<Segment> {
    finish_upload_attempt(
        store,
        session_id,
        index,
        attempt_id,
        AttemptTargetGate::Blocked {
            owner: RemoteOperationRef::Upload {
                session_id,
                segment_index: index,
                attempt_id,
            },
            reason: reason.clone(),
        },
        |segment, now| {
            finish_upload_history(
                segment,
                attempt_id,
                now,
                UploadAttemptOutcome::Blocked {
                    reason: reason.clone(),
                },
            )?;
            segment.upload = UploadState::Blocked {
                attempt_id: Some(attempt_id),
                reason,
            };
            Ok(())
        },
    )
}

pub fn mark_upload_ambiguous(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    attempt_id: Uuid,
    reason: String,
) -> AppResult<Segment> {
    finish_upload_attempt(
        store,
        session_id,
        index,
        attempt_id,
        AttemptTargetGate::Unchanged,
        |segment, now| {
            let attempt = current_upload_attempt(segment, attempt_id)?.clone();
            finish_upload_history(
                segment,
                attempt_id,
                now,
                UploadAttemptOutcome::Ambiguous {
                    reason: reason.clone(),
                },
            )?;
            segment.upload = UploadState::Ambiguous { attempt, reason };
            Ok(())
        },
    )
}

pub fn resolve_upload_not_uploaded(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    note: Option<String>,
) -> AppResult<Segment> {
    store.write(|txn| {
        let session = require_session(txn, session_id)?;
        let target = session
            .output_plan
            .upload_plan()
            .map(UploadTarget::from)
            .ok_or_else(|| state_error(format!("session {session_id} is LocalOnly")))?;
        if txn.get_submission(session_id)?.is_some() {
            return Err(state_error(format!(
                "session {session_id} already has a submission; refusing upload resolution"
            )));
        }
        let mut segment = require_segment(txn, session_id, index)?;
        let attempt_id = recoverable_upload_attempt_id(&segment.upload)?;
        let now = jiff::Timestamp::now();
        if let Some(attempt_id) = attempt_id {
            finish_unfinished_upload_as_ambiguous(&mut segment, attempt_id, now)?;
        }
        segment.upload_resolutions.push(UploadResolution {
            attempt_id,
            resolved_at: now,
            decision: UploadResolutionDecision::ConfirmedNotUploaded,
            note,
        });
        segment.upload = match session.lifecycle {
            SessionLifecycle::Closed {
                closure:
                    SessionClosure::Abandoned { .. }
                    | SessionClosure::NoUsableRecording { .. },
            } => UploadState::Cancelled {
                cancelled_at: now,
                reason: "operator confirmed no remote upload for a closed session".into(),
            },
            SessionLifecycle::Open
            | SessionLifecycle::RecoveryRequired { .. }
            | SessionLifecycle::Closed {
                closure: SessionClosure::Completed { .. },
            } => UploadState::pending(),
        };
        txn.put_segment(&segment)?;
        clear_target_gate_if_owner(txn, target, upload_operation_ref(session_id, index, attempt_id))?;
        Ok(segment)
    })
}

pub fn resolve_upload_uploaded(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    proof: UploadedPart,
    note: Option<String>,
) -> AppResult<Segment> {
    validate_uploaded_part(&proof)?;
    store.write(|txn| {
        let session = require_session(txn, session_id)?;
        let target = session
            .output_plan
            .upload_plan()
            .map(UploadTarget::from)
            .ok_or_else(|| state_error(format!("session {session_id} is LocalOnly")))?;
        if txn.get_submission(session_id)?.is_some() {
            return Err(state_error(format!(
                "session {session_id} already has a submission; refusing upload resolution"
            )));
        }
        let mut segment = require_segment(txn, session_id, index)?;
        let attempt_id = recoverable_upload_attempt_id(&segment.upload)?;
        let now = jiff::Timestamp::now();
        if let Some(attempt_id) = attempt_id {
            finish_unfinished_upload_as_ambiguous(&mut segment, attempt_id, now)?;
        }
        segment.upload_resolutions.push(UploadResolution {
            attempt_id,
            resolved_at: now,
            decision: UploadResolutionDecision::ConfirmedUploaded {
                proof: proof.clone(),
            },
            note,
        });
        segment.upload = UploadState::Uploaded { proof };
        txn.put_segment(&segment)?;
        clear_target_gate_if_owner(
            txn,
            target,
            upload_operation_ref(session_id, index, attempt_id),
        )?;
        Ok(segment)
    })
}

pub fn begin_submission(
    store: &StateStore,
    session_id: Uuid,
    attempt: RemoteAttempt,
) -> AppResult<Submission> {
    store.write(|txn| {
        let session = require_session(txn, session_id)?;
        if !session.lifecycle.permits_submission() {
            return Err(state_error(format!(
                "session {session_id} is not Completed"
            )));
        }
        let upload_plan = session.output_plan.upload_plan().ok_or_else(|| {
            state_error(format!("session {session_id} is configured LocalOnly"))
        })?;
        consume_target_ready(txn, &UploadTarget::from(upload_plan), attempt.started_at)?;
        require_submission_parts(txn, session_id)?;

        let mut submission = match txn.get_submission(session_id)? {
            None => Submission {
                session_id,
                state: SubmissionState::Attempting {
                    attempt: attempt.clone(),
                },
                attempts: Vec::new(),
                resolutions: Vec::new(),
            },
            Some(mut submission) => {
                let due = matches!(submission.state, SubmissionState::RetryAuthorized { .. })
                    || matches!(
                        submission.state,
                        SubmissionState::RetryScheduled { retry_at, .. } if retry_at <= attempt.started_at
                    );
                if !due {
                    return Err(state_error(format!(
                        "submission for session {session_id} is not ready for another attempt"
                    )));
                }
                submission.state = SubmissionState::Attempting {
                    attempt: attempt.clone(),
                };
                submission
            }
        };
        if submission
            .attempts
            .iter()
            .any(|record| record.attempt.id == attempt.id)
        {
            return Err(state_error(format!(
                "submission attempt {} already exists",
                attempt.id
            )));
        }
        submission.attempts.push(SubmissionAttempt {
            attempt,
            finished_at: None,
            outcome: None,
        });
        txn.put_submission(&submission)?;
        Ok(submission)
    })
}

pub fn complete_submission(
    store: &StateStore,
    session_id: Uuid,
    attempt_id: Uuid,
    aid: Option<u64>,
    bvid: Option<String>,
) -> AppResult<Submission> {
    validate_submission_proof(aid, bvid.as_deref())?;
    store.write(|txn| {
        let mut submission = txn.get_submission(session_id)?.ok_or_else(|| {
            state_error(format!(
                "submission for session {session_id} does not exist"
            ))
        })?;
        require_current_submission_attempt(&submission, attempt_id)?;
        let now = jiff::Timestamp::now();
        let outcome = SubmissionAttemptOutcome::Submitted {
            aid,
            bvid: bvid.clone(),
        };
        finish_submission_history(&mut submission, attempt_id, now, outcome)?;
        submission.state = SubmissionState::Submitted { aid, bvid };
        txn.put_submission(&submission)?;
        Ok(submission)
    })
}

pub fn schedule_submission_retry(
    store: &StateStore,
    session_id: Uuid,
    attempt_id: Uuid,
    reason: String,
    retry_at: jiff::Timestamp,
) -> AppResult<Submission> {
    schedule_submission_retry_with_target_gate(
        store,
        session_id,
        attempt_id,
        reason,
        retry_at,
        AttemptTargetGate::Unchanged,
    )
}

pub fn schedule_submission_target_retry(
    store: &StateStore,
    session_id: Uuid,
    attempt_id: Uuid,
    reason: String,
    retry_at: jiff::Timestamp,
    failures: u32,
) -> AppResult<Submission> {
    if failures == 0 {
        return Err(state_error(
            "target failure count must be greater than zero",
        ));
    }
    schedule_submission_retry_with_target_gate(
        store,
        session_id,
        attempt_id,
        reason.clone(),
        retry_at,
        AttemptTargetGate::Backoff {
            owner: RemoteOperationRef::Submission {
                session_id,
                attempt_id,
            },
            retry_at,
            last_error: reason,
            failures,
        },
    )
}

fn schedule_submission_retry_with_target_gate(
    store: &StateStore,
    session_id: Uuid,
    attempt_id: Uuid,
    reason: String,
    retry_at: jiff::Timestamp,
    target_gate: AttemptTargetGate,
) -> AppResult<Submission> {
    store.write(|txn| {
        let target = session_upload_target(txn, session_id)?;
        let mut submission = txn.get_submission(session_id)?.ok_or_else(|| {
            state_error(format!(
                "submission for session {session_id} does not exist"
            ))
        })?;
        require_current_submission_attempt(&submission, attempt_id)?;
        let now = jiff::Timestamp::now();
        finish_submission_history(
            &mut submission,
            attempt_id,
            now,
            SubmissionAttemptOutcome::RetryScheduled {
                reason: reason.clone(),
            },
        )?;
        let failures = submission
            .attempts
            .iter()
            .filter(|record| {
                matches!(
                    record.outcome,
                    Some(SubmissionAttemptOutcome::RetryScheduled { .. })
                )
            })
            .count() as u32;
        submission.state = SubmissionState::RetryScheduled {
            failures,
            retry_at,
            last_error: reason.clone(),
        };
        txn.put_submission(&submission)?;
        put_attempt_target_gate_txn(txn, target, target_gate, now)?;
        Ok(submission)
    })
}

pub fn block_submission(
    store: &StateStore,
    session_id: Uuid,
    attempt_id: Uuid,
    reason: String,
) -> AppResult<Submission> {
    finish_submission_attempt(
        store,
        session_id,
        attempt_id,
        AttemptTargetGate::Unchanged,
        |submission, now| {
            finish_submission_history(
                submission,
                attempt_id,
                now,
                SubmissionAttemptOutcome::Blocked {
                    reason: reason.clone(),
                },
            )?;
            submission.state = SubmissionState::Blocked {
                attempt_id: Some(attempt_id),
                reason,
            };
            Ok(())
        },
    )
}

pub fn block_submission_for_target(
    store: &StateStore,
    session_id: Uuid,
    attempt_id: Uuid,
    reason: String,
) -> AppResult<Submission> {
    finish_submission_attempt(
        store,
        session_id,
        attempt_id,
        AttemptTargetGate::Blocked {
            owner: RemoteOperationRef::Submission {
                session_id,
                attempt_id,
            },
            reason: reason.clone(),
        },
        |submission, now| {
            finish_submission_history(
                submission,
                attempt_id,
                now,
                SubmissionAttemptOutcome::Blocked {
                    reason: reason.clone(),
                },
            )?;
            submission.state = SubmissionState::Blocked {
                attempt_id: Some(attempt_id),
                reason,
            };
            Ok(())
        },
    )
}

pub fn mark_submission_ambiguous(
    store: &StateStore,
    session_id: Uuid,
    attempt_id: Uuid,
    reason: String,
) -> AppResult<Submission> {
    finish_submission_attempt(
        store,
        session_id,
        attempt_id,
        AttemptTargetGate::Unchanged,
        |submission, now| {
            let attempt = current_submission_attempt(submission, attempt_id)?.clone();
            finish_submission_history(
                submission,
                attempt_id,
                now,
                SubmissionAttemptOutcome::Ambiguous {
                    reason: reason.clone(),
                },
            )?;
            submission.state = SubmissionState::Ambiguous { attempt, reason };
            Ok(())
        },
    )
}

pub fn resolve_submission_not_submitted(
    store: &StateStore,
    session_id: Uuid,
    note: Option<String>,
) -> AppResult<Submission> {
    store.write(|txn| {
        let session = require_session(txn, session_id)?;
        if !session.lifecycle.permits_submission() {
            return Err(state_error(format!(
                "session {session_id} does not permit submission recovery"
            )));
        }
        let target = session
            .output_plan
            .upload_plan()
            .map(UploadTarget::from)
            .ok_or_else(|| state_error(format!("session {session_id} is LocalOnly")))?;
        let mut submission = txn.get_submission(session_id)?.ok_or_else(|| {
            state_error(format!(
                "submission for session {session_id} does not exist"
            ))
        })?;
        let attempt_id = recoverable_submission_attempt_id(&submission.state)?;
        let now = jiff::Timestamp::now();
        if let Some(attempt_id) = attempt_id {
            finish_unfinished_submission_as_ambiguous(&mut submission, attempt_id, now)?;
        }
        submission.resolutions.push(SubmissionResolution {
            attempt_id,
            resolved_at: now,
            decision: SubmissionResolutionDecision::ConfirmedNotSubmitted,
            note,
        });
        submission.state = SubmissionState::RetryAuthorized { authorized_at: now };
        txn.put_submission(&submission)?;
        clear_target_gate_if_owner(
            txn,
            target,
            submission_operation_ref(session_id, attempt_id),
        )?;
        Ok(submission)
    })
}

pub fn resolve_submission_submitted(
    store: &StateStore,
    session_id: Uuid,
    aid: Option<u64>,
    bvid: Option<String>,
    note: Option<String>,
) -> AppResult<Submission> {
    validate_submission_proof(aid, bvid.as_deref())?;
    store.write(|txn| {
        let session = require_session(txn, session_id)?;
        if !session.lifecycle.permits_submission() {
            return Err(state_error(format!(
                "session {session_id} does not permit submission recovery"
            )));
        }
        let target = session
            .output_plan
            .upload_plan()
            .map(UploadTarget::from)
            .ok_or_else(|| state_error(format!("session {session_id} is LocalOnly")))?;
        let mut submission = txn.get_submission(session_id)?.ok_or_else(|| {
            state_error(format!(
                "submission for session {session_id} does not exist"
            ))
        })?;
        let attempt_id = recoverable_submission_attempt_id(&submission.state)?;
        let now = jiff::Timestamp::now();
        if let Some(attempt_id) = attempt_id {
            finish_unfinished_submission_as_ambiguous(&mut submission, attempt_id, now)?;
        }
        submission.resolutions.push(SubmissionResolution {
            attempt_id,
            resolved_at: now,
            decision: SubmissionResolutionDecision::ConfirmedSubmitted {
                aid,
                bvid: bvid.clone(),
            },
            note,
        });
        submission.state = SubmissionState::Submitted { aid, bvid };
        txn.put_submission(&submission)?;
        clear_target_gate_if_owner(
            txn,
            target,
            submission_operation_ref(session_id, attempt_id),
        )?;
        Ok(submission)
    })
}

pub fn reconcile_interrupted_remote_attempts(store: &StateStore) -> AppResult<Vec<String>> {
    let mut messages = Vec::new();
    for segment in store.list_all_segments()? {
        let UploadState::Attempting { attempt } = segment.upload else {
            continue;
        };
        mark_upload_ambiguous(
            store,
            segment.session_id,
            segment.index,
            attempt.id,
            "process ended before the remote upload result was durably confirmed".into(),
        )?;
        messages.push(format!(
            "marked upload {}/{} Ambiguous",
            segment.session_id, segment.index
        ));
    }
    for submission in store.list_submissions()? {
        let SubmissionState::Attempting { attempt } = submission.state else {
            continue;
        };
        mark_submission_ambiguous(
            store,
            submission.session_id,
            attempt.id,
            "process ended before the remote submission result was durably confirmed".into(),
        )?;
        messages.push(format!(
            "marked submission {} Ambiguous",
            submission.session_id
        ));
    }
    Ok(messages)
}

fn update_artifact(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    update: impl FnOnce(&mut Segment) -> AppResult<()>,
) -> AppResult<Segment> {
    store.write(|txn| {
        let mut segment = require_segment(txn, session_id, index)?;
        update(&mut segment)?;
        txn.put_segment(&segment)?;
        Ok(segment)
    })
}

fn update_recording_artifact(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    require_open: bool,
    update: impl FnOnce(&mut Segment) -> AppResult<()>,
) -> AppResult<Segment> {
    store.write(|txn| {
        let session = require_session(txn, session_id)?;
        if require_open {
            require_open_owned_session(txn, &session)?;
        } else {
            if matches!(session.lifecycle, SessionLifecycle::Closed { .. }) {
                return Err(state_error(format!(
                    "session {session_id} is already closed"
                )));
            }
            require_or_claim_recovery_owner(txn, &session)?;
        }
        let mut segment = require_segment(txn, session_id, index)?;
        update(&mut segment)?;
        txn.put_segment(&segment)?;
        Ok(segment)
    })
}

fn finish_upload_attempt(
    store: &StateStore,
    session_id: Uuid,
    index: u32,
    attempt_id: Uuid,
    target_gate: AttemptTargetGate,
    update: impl FnOnce(&mut Segment, jiff::Timestamp) -> AppResult<()>,
) -> AppResult<Segment> {
    store.write(|txn| {
        let target = session_upload_target(txn, session_id)?;
        let mut segment = require_segment(txn, session_id, index)?;
        require_current_upload_attempt(&segment, attempt_id)?;
        let now = jiff::Timestamp::now();
        update(&mut segment, now)?;
        txn.put_segment(&segment)?;
        put_attempt_target_gate_txn(txn, target, target_gate, now)?;
        Ok(segment)
    })
}

fn finish_submission_attempt(
    store: &StateStore,
    session_id: Uuid,
    attempt_id: Uuid,
    target_gate: AttemptTargetGate,
    update: impl FnOnce(&mut Submission, jiff::Timestamp) -> AppResult<()>,
) -> AppResult<Submission> {
    store.write(|txn| {
        let target = session_upload_target(txn, session_id)?;
        let mut submission = txn.get_submission(session_id)?.ok_or_else(|| {
            state_error(format!(
                "submission for session {session_id} does not exist"
            ))
        })?;
        require_current_submission_attempt(&submission, attempt_id)?;
        let now = jiff::Timestamp::now();
        update(&mut submission, now)?;
        txn.put_submission(&submission)?;
        put_attempt_target_gate_txn(txn, target, target_gate, now)?;
        Ok(submission)
    })
}

fn require_session(txn: &StoreTxn<'_>, session_id: Uuid) -> AppResult<LiveSession> {
    txn.get_session(session_id)?
        .ok_or_else(|| state_error(format!("session {session_id} does not exist")))
}

fn require_segment(txn: &StoreTxn<'_>, session_id: Uuid, index: u32) -> AppResult<Segment> {
    txn.get_segment(session_id, index)?
        .ok_or_else(|| state_error(format!("segment {session_id}/{index} does not exist")))
}

fn require_open_owned_session(txn: &StoreTxn<'_>, session: &LiveSession) -> AppResult<()> {
    if session.lifecycle != SessionLifecycle::Open {
        return Err(state_error(format!("session {} is not Open", session.id)));
    }
    require_room_owner(txn, session)
}

fn require_room_owner(txn: &StoreTxn<'_>, session: &LiveSession) -> AppResult<()> {
    let room = txn.get_room_state(session.room_id)?.ok_or_else(|| {
        state_error(format!(
            "room {} has no durable state for session {}",
            session.room_id, session.id
        ))
    })?;
    let owns = matches!(
        room.lifecycle,
        RoomLifecycle::Owned { session_id } if session_id == session.id
    );
    if !owns {
        return Err(state_error(format!(
            "room {} is {:?}, not owned by session {}",
            session.room_id, room.lifecycle, session.id
        )));
    }
    Ok(())
}

/// Recovery repairs a missing/Ready/mismatched-phase ownership row in the same
/// transaction, but it never steals a room from a different session.
fn require_or_claim_recovery_owner(txn: &StoreTxn<'_>, session: &LiveSession) -> AppResult<()> {
    let desired = match &session.lifecycle {
        SessionLifecycle::Open => RoomLifecycle::Owned {
            session_id: session.id,
        },
        SessionLifecycle::RecoveryRequired { .. } => RoomLifecycle::Blocked {
            session_id: session.id,
        },
        SessionLifecycle::Closed { .. } => {
            return Err(state_error(format!(
                "closed session {} cannot claim room ownership",
                session.id
            )));
        }
    };

    let current = txn.get_room_state(session.room_id)?;
    let claimable = current.as_ref().is_none_or(|room| match room.lifecycle {
        RoomLifecycle::Ready => true,
        RoomLifecycle::Owned { session_id } | RoomLifecycle::Blocked { session_id } => {
            session_id == session.id
        }
    });
    if !claimable {
        return Err(state_error(format!(
            "room {} is owned by another session; refusing recovery claim for {}",
            session.room_id, session.id
        )));
    }

    if !current.is_some_and(|room| room.lifecycle == desired) {
        let message = match &session.lifecycle {
            SessionLifecycle::RecoveryRequired { reason, .. } => Some(reason.clone()),
            SessionLifecycle::Open => None,
            SessionLifecycle::Closed { .. } => unreachable!(),
        };
        txn.put_room_state(session.room_id, &room_state(desired, message))?;
    }
    Ok(())
}

fn require_submission_parts(txn: &StoreTxn<'_>, session_id: Uuid) -> AppResult<()> {
    let segments = txn.list_segments(session_id)?;
    let mut uploaded = 0usize;
    for segment in segments {
        match (&segment.artifact, &segment.upload) {
            (ArtifactState::Filtered { .. } | ArtifactState::Excluded { .. }, _) => {}
            (
                ArtifactState::Ready { .. } | ArtifactState::Deleting | ArtifactState::Deleted,
                UploadState::Uploaded { .. },
            ) => uploaded += 1,
            _ => {
                return Err(state_error(format!(
                    "segment {session_id}/{} is not ready for submission: artifact={:?}, upload={:?}",
                    segment.index, segment.artifact, segment.upload
                )));
            }
        }
    }
    if uploaded == 0 {
        return Err(state_error(format!(
            "session {session_id} has no uploaded parts; NoUsableRecording must not be submitted"
        )));
    }
    Ok(())
}

fn session_upload_target(txn: &StoreTxn<'_>, session_id: Uuid) -> AppResult<UploadTarget> {
    let session = require_session(txn, session_id)?;
    session
        .output_plan
        .upload_plan()
        .map(UploadTarget::from)
        .ok_or_else(|| state_error(format!("session {session_id} is configured LocalOnly")))
}

fn put_target_gate_txn(
    txn: &StoreTxn<'_>,
    target: UploadTarget,
    gate: UploadTargetGate,
) -> AppResult<()> {
    txn.put_upload_target_state(&UploadTargetState { target, gate })
}

enum AttemptTargetGate {
    Unchanged,
    Backoff {
        owner: RemoteOperationRef,
        failures: u32,
        retry_at: jiff::Timestamp,
        last_error: String,
    },
    Blocked {
        owner: RemoteOperationRef,
        reason: String,
    },
}

fn put_attempt_target_gate_txn(
    txn: &StoreTxn<'_>,
    target: UploadTarget,
    gate: AttemptTargetGate,
    now: jiff::Timestamp,
) -> AppResult<()> {
    match gate {
        AttemptTargetGate::Unchanged => Ok(()),
        AttemptTargetGate::Backoff {
            owner,
            failures,
            retry_at,
            last_error,
        } => backoff_target_txn(txn, target, owner, failures, retry_at, last_error),
        AttemptTargetGate::Blocked { owner, reason } => put_target_gate_txn(
            txn,
            target,
            UploadTargetGate::Blocked {
                owner,
                since: now,
                reason,
            },
        ),
    }
}

fn backoff_target_txn(
    txn: &StoreTxn<'_>,
    target: UploadTarget,
    owner: RemoteOperationRef,
    failures: u32,
    retry_at: jiff::Timestamp,
    last_error: String,
) -> AppResult<()> {
    put_target_gate_txn(
        txn,
        target,
        UploadTargetGate::Backoff {
            owner,
            failures,
            retry_at,
            last_error,
        },
    )
}

fn consume_target_ready(
    txn: &StoreTxn<'_>,
    target: &UploadTarget,
    now: jiff::Timestamp,
) -> AppResult<()> {
    let state = txn
        .get_upload_target_state(target)?
        .ok_or_else(|| state_error("upload target has no durable gate"))?;
    match state.gate {
        UploadTargetGate::Ready => Ok(()),
        UploadTargetGate::Backoff { retry_at, .. } if retry_at <= now => {
            put_target_gate_txn(txn, target.clone(), UploadTargetGate::Ready)
        }
        UploadTargetGate::Backoff { retry_at, .. } => Err(state_error(format!(
            "upload target is in backoff until {retry_at}"
        ))),
        UploadTargetGate::Blocked { reason, .. } => {
            Err(state_error(format!("upload target is blocked: {reason}")))
        }
    }
}

fn clear_target_gate_if_owner(
    txn: &StoreTxn<'_>,
    target: UploadTarget,
    expected_owner: Option<RemoteOperationRef>,
) -> AppResult<()> {
    let Some(expected_owner) = expected_owner else {
        return Ok(());
    };
    let Some(state) = txn.get_upload_target_state(&target)? else {
        return Ok(());
    };
    let matches = match state.gate {
        UploadTargetGate::Backoff { owner, .. } | UploadTargetGate::Blocked { owner, .. } => {
            owner == expected_owner
        }
        UploadTargetGate::Ready => false,
    };
    if matches {
        put_target_gate_txn(txn, target, UploadTargetGate::Ready)?;
    }
    Ok(())
}

fn upload_operation_ref(
    session_id: Uuid,
    segment_index: u32,
    attempt_id: Option<Uuid>,
) -> Option<RemoteOperationRef> {
    attempt_id.map(|attempt_id| RemoteOperationRef::Upload {
        session_id,
        segment_index,
        attempt_id,
    })
}

fn submission_operation_ref(
    session_id: Uuid,
    attempt_id: Option<Uuid>,
) -> Option<RemoteOperationRef> {
    attempt_id.map(|attempt_id| RemoteOperationRef::Submission {
        session_id,
        attempt_id,
    })
}

fn current_upload_attempt(segment: &Segment, attempt_id: Uuid) -> AppResult<&RemoteAttempt> {
    match &segment.upload {
        UploadState::Attempting { attempt } if attempt.id == attempt_id => Ok(attempt),
        _ => Err(state_error(format!(
            "segment {}/{} is not owned by upload attempt {attempt_id}",
            segment.session_id, segment.index
        ))),
    }
}

fn require_current_upload_attempt(segment: &Segment, attempt_id: Uuid) -> AppResult<()> {
    current_upload_attempt(segment, attempt_id).map(|_| ())
}

fn finish_upload_history(
    segment: &mut Segment,
    attempt_id: Uuid,
    finished_at: jiff::Timestamp,
    outcome: UploadAttemptOutcome,
) -> AppResult<()> {
    let record = segment
        .upload_attempts
        .iter_mut()
        .find(|record| record.attempt.id == attempt_id)
        .ok_or_else(|| state_error(format!("upload attempt {attempt_id} has no history record")))?;
    if record.finished_at.is_some() || record.outcome.is_some() {
        return Err(state_error(format!(
            "upload attempt {attempt_id} is already finished"
        )));
    }
    record.finished_at = Some(finished_at);
    record.outcome = Some(outcome);
    Ok(())
}

fn current_submission_attempt(
    submission: &Submission,
    attempt_id: Uuid,
) -> AppResult<&RemoteAttempt> {
    match &submission.state {
        SubmissionState::Attempting { attempt } if attempt.id == attempt_id => Ok(attempt),
        _ => Err(state_error(format!(
            "submission {} is not owned by attempt {attempt_id}",
            submission.session_id
        ))),
    }
}

fn require_current_submission_attempt(submission: &Submission, attempt_id: Uuid) -> AppResult<()> {
    current_submission_attempt(submission, attempt_id).map(|_| ())
}

fn finish_submission_history(
    submission: &mut Submission,
    attempt_id: Uuid,
    finished_at: jiff::Timestamp,
    outcome: SubmissionAttemptOutcome,
) -> AppResult<()> {
    let record = submission
        .attempts
        .iter_mut()
        .find(|record| record.attempt.id == attempt_id)
        .ok_or_else(|| {
            state_error(format!(
                "submission attempt {attempt_id} has no history record"
            ))
        })?;
    if record.finished_at.is_some() || record.outcome.is_some() {
        return Err(state_error(format!(
            "submission attempt {attempt_id} is already finished"
        )));
    }
    record.finished_at = Some(finished_at);
    record.outcome = Some(outcome);
    Ok(())
}

fn recoverable_upload_attempt_id(upload: &UploadState) -> AppResult<Option<Uuid>> {
    match upload {
        UploadState::Attempting { attempt } | UploadState::Ambiguous { attempt, .. } => {
            Ok(Some(attempt.id))
        }
        UploadState::Blocked { attempt_id, .. } => Ok(*attempt_id),
        _ => Err(state_error(format!(
            "upload state {upload:?} does not require operator resolution"
        ))),
    }
}

fn recoverable_submission_attempt_id(state: &SubmissionState) -> AppResult<Option<Uuid>> {
    match state {
        SubmissionState::Attempting { attempt } | SubmissionState::Ambiguous { attempt, .. } => {
            Ok(Some(attempt.id))
        }
        SubmissionState::Blocked { attempt_id, .. } => Ok(*attempt_id),
        _ => Err(state_error(format!(
            "submission state {state:?} does not require operator resolution"
        ))),
    }
}

fn finish_unfinished_upload_as_ambiguous(
    segment: &mut Segment,
    attempt_id: Uuid,
    now: jiff::Timestamp,
) -> AppResult<()> {
    let Some(record) = segment
        .upload_attempts
        .iter_mut()
        .find(|record| record.attempt.id == attempt_id)
    else {
        return Err(state_error(format!(
            "upload attempt {attempt_id} has no history record"
        )));
    };
    if record.outcome.is_none() {
        record.finished_at = Some(now);
        record.outcome = Some(UploadAttemptOutcome::Ambiguous {
            reason: "operator resolved an interrupted attempt".into(),
        });
    }
    Ok(())
}

fn finish_unfinished_submission_as_ambiguous(
    submission: &mut Submission,
    attempt_id: Uuid,
    now: jiff::Timestamp,
) -> AppResult<()> {
    let Some(record) = submission
        .attempts
        .iter_mut()
        .find(|record| record.attempt.id == attempt_id)
    else {
        return Err(state_error(format!(
            "submission attempt {attempt_id} has no history record"
        )));
    };
    if record.outcome.is_none() {
        record.finished_at = Some(now);
        record.outcome = Some(SubmissionAttemptOutcome::Ambiguous {
            reason: "operator resolved an interrupted attempt".into(),
        });
    }
    Ok(())
}

fn abandon_upload(segment: &mut Segment) -> AppResult<()> {
    let now = jiff::Timestamp::now();
    match &segment.upload {
        UploadState::Attempting { attempt } => {
            let attempt = attempt.clone();
            finish_unfinished_upload_as_ambiguous(segment, attempt.id, now)?;
            segment.upload = UploadState::Ambiguous {
                attempt,
                reason: "session was abandoned while upload outcome was unknown".into(),
            };
        }
        UploadState::Pending { .. } | UploadState::Blocked { .. } => {
            segment.upload = UploadState::Cancelled {
                cancelled_at: now,
                reason: "session was abandoned".into(),
            };
        }
        UploadState::NotPlanned
        | UploadState::Ambiguous { .. }
        | UploadState::Uploaded { .. }
        | UploadState::Cancelled { .. } => {}
    }
    Ok(())
}

fn validate_submission_proof(aid: Option<u64>, bvid: Option<&str>) -> AppResult<()> {
    if aid == Some(0) {
        return Err(state_error("aid must be greater than zero"));
    }
    if aid.is_none() && bvid.is_none() {
        return Err(state_error(
            "confirmed submission requires at least one of aid or bvid",
        ));
    }
    if bvid.is_some_and(|value| value.trim().is_empty() || value.chars().any(char::is_control)) {
        return Err(state_error(
            "bvid must not be empty or contain control characters",
        ));
    }
    Ok(())
}

fn validate_uploaded_part(proof: &UploadedPart) -> AppResult<()> {
    for (label, value) in [
        ("bili filename", proof.bili_filename.as_str()),
        ("part title", proof.part_title.as_str()),
    ] {
        if value.trim().is_empty() || value.chars().any(char::is_control) {
            return Err(state_error(format!(
                "{label} must not be empty or contain control characters"
            )));
        }
    }
    Ok(())
}

fn room_state(lifecycle: RoomLifecycle, message: Option<String>) -> RoomState {
    RoomState {
        lifecycle,
        changed_at: jiff::Timestamp::now(),
        message,
    }
}

fn unexpected_artifact(segment: &Segment, expected: &str) -> AppError {
    state_error(format!(
        "segment {}/{} is {:?}, expected {expected}",
        segment.session_id, segment.index, segment.artifact
    ))
}

fn state_error(message: impl Into<String>) -> AppError {
    AppError::State(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::config::{Copyright, SubmitApi};
    use crate::credential::CredentialRef;
    use crate::state::model::{
        RecordingPlan, SubmissionSpec, UploadPlan, UploadResolutionDecision,
    };

    fn store() -> (StateStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = StateStore::create_or_open(dir.path().join("state.redb")).unwrap();
        (store, dir)
    }

    fn session(room_id: u64, output_plan: OutputPlan) -> LiveSession {
        LiveSession {
            id: Uuid::new_v4(),
            room_id,
            room_name: format!("room-{room_id}"),
            title: "test".into(),
            started_at: jiff::Timestamp::now(),
            lifecycle: SessionLifecycle::Open,
            recording_plan: RecordingPlan {
                credential: None,
                output_dir: std::env::temp_dir().join("bilive-rec-test-recordings"),
                segment_time_ms: None,
                segment_size: None,
                min_segment_size: 0,
                qn: 10_000,
                cdn: Vec::new(),
            },
            output_plan,
            recording_events: Vec::new(),
        }
    }

    fn bilibili_output() -> OutputPlan {
        OutputPlan::Bilibili {
            upload: UploadPlan {
                principal: crate::credential::UploadPrincipal::new(
                    CredentialRef::new("main", std::env::temp_dir().join("cookies.json")),
                    1,
                ),
                line: "auto".into(),
                threads: 3,
                submit_api: SubmitApi::App,
                delete_after_submit: false,
            },
            submission: Box::new(SubmissionSpec {
                title: "title".into(),
                description: String::new(),
                category_id: 171,
                copyright: Copyright::Reprint,
                source: "source".into(),
                tags: vec!["live".into()],
                private: false,
                dynamic: String::new(),
                forbid_reprint: false,
                charging_panel: false,
                close_reply: false,
                close_danmu: false,
                featured_reply: false,
            }),
        }
    }

    fn raw_segment(session_id: Uuid, index: u32, dir: &std::path::Path) -> Segment {
        Segment {
            session_id,
            index,
            part_path: dir.join(format!("{index}.part")),
            final_path: dir.join(format!("{index}.flv")),
            artifact: ArtifactState::Writing,
            artifact_resolutions: Vec::new(),
            upload: UploadState::NotPlanned,
            upload_attempts: Vec::new(),
            upload_resolutions: Vec::new(),
        }
    }

    fn ready_segment(store: &StateStore, session_id: Uuid, dir: &std::path::Path) -> Segment {
        let segment = open_segment(store, raw_segment(session_id, 1, dir)).unwrap();
        begin_artifact_finalization(
            store,
            session_id,
            segment.index,
            SegmentCloseReason::StreamEnded,
        )
        .unwrap();
        complete_artifact_finalization(store, session_id, segment.index).unwrap()
    }

    #[test]
    fn open_segment_derives_upload_intent_from_frozen_session() {
        let (store, dir) = store();
        let local = session(1, OutputPlan::LocalOnly);
        create_session(&store, &local).unwrap();
        assert_eq!(
            open_segment(&store, raw_segment(local.id, 1, dir.path()))
                .unwrap()
                .upload,
            UploadState::NotPlanned
        );

        let remote = session(2, bilibili_output());
        create_session(&store, &remote).unwrap();
        assert!(matches!(
            open_segment(&store, raw_segment(remote.id, 1, dir.path()))
                .unwrap()
                .upload,
            UploadState::Pending { failures: 0, .. }
        ));
    }

    #[test]
    fn failed_segment_atomically_requires_recovery_and_blocks_room() {
        let (store, dir) = store();
        let session = session(1, OutputPlan::LocalOnly);
        create_session(&store, &session).unwrap();
        open_segment(&store, raw_segment(session.id, 1, dir.path())).unwrap();
        fail_artifact(&store, session.id, 1, "disk full".into(), None).unwrap();

        assert!(matches!(
            store.get_session(session.id).unwrap().unwrap().lifecycle,
            SessionLifecycle::RecoveryRequired { .. }
        ));
        assert_eq!(
            store.get_room_state(1).unwrap().unwrap().lifecycle,
            RoomLifecycle::Blocked {
                session_id: session.id
            }
        );
    }

    #[test]
    fn artifact_completion_repairs_a_claimable_open_room_owner() {
        let (store, dir) = store();
        let session = session(1, OutputPlan::LocalOnly);
        create_session(&store, &session).unwrap();
        open_segment(&store, raw_segment(session.id, 1, dir.path())).unwrap();
        begin_artifact_finalization(&store, session.id, 1, SegmentCloseReason::StreamEnded)
            .unwrap();

        store
            .write(|txn| txn.put_room_state(1, &room_state(RoomLifecycle::Ready, None)))
            .unwrap();

        complete_artifact_finalization(&store, session.id, 1).unwrap();

        assert_eq!(
            store.get_room_state(1).unwrap().unwrap().lifecycle,
            RoomLifecycle::Owned {
                session_id: session.id
            }
        );
    }

    #[test]
    fn recording_recovery_repairs_a_claimable_room_owner() {
        let (store, dir) = store();
        let session = session(1, OutputPlan::LocalOnly);
        create_session(&store, &session).unwrap();
        open_segment(&store, raw_segment(session.id, 1, dir.path())).unwrap();
        fail_artifact(&store, session.id, 1, "disk full".into(), None).unwrap();

        store
            .write(|txn| txn.put_room_state(1, &room_state(RoomLifecycle::Ready, None)))
            .unwrap();

        close_session(
            &store,
            session.id,
            CloseSessionRequest::Recover {
                exclude_failed: true,
                note: None,
            },
        )
        .unwrap();

        assert_eq!(
            store.get_room_state(1).unwrap().unwrap().lifecycle,
            RoomLifecycle::Ready
        );
    }

    #[test]
    fn recover_excludes_failed_and_completes_session() {
        let (store, dir) = store();
        let session = session(1, OutputPlan::LocalOnly);
        create_session(&store, &session).unwrap();
        open_segment(&store, raw_segment(session.id, 1, dir.path())).unwrap();
        fail_artifact(&store, session.id, 1, "disk full".into(), None).unwrap();

        let result = close_session(
            &store,
            session.id,
            CloseSessionRequest::Recover {
                exclude_failed: true,
                note: Some("keep what survived".into()),
            },
        )
        .unwrap();
        assert!(matches!(
            result.lifecycle,
            SessionLifecycle::Closed {
                closure: SessionClosure::NoUsableRecording { .. },
            }
        ));
        assert_eq!(result.excluded_segments, vec![1]);
        assert_eq!(
            store.get_room_state(1).unwrap().unwrap().lifecycle,
            RoomLifecycle::Ready
        );
    }

    #[test]
    fn all_filtered_closes_as_no_usable_recording() {
        let (store, dir) = store();
        let session = session(1, bilibili_output());
        create_session(&store, &session).unwrap();
        open_segment(&store, raw_segment(session.id, 1, dir.path())).unwrap();
        begin_artifact_discard(&store, session.id, 1, SegmentCloseReason::StreamEnded).unwrap();
        complete_artifact_discard(&store, session.id, 1).unwrap();
        let result = close_session(
            &store,
            session.id,
            CloseSessionRequest::Natural { note: None },
        )
        .unwrap();
        assert!(matches!(
            result.lifecycle,
            SessionLifecycle::Closed {
                closure: SessionClosure::NoUsableRecording { .. },
            }
        ));
        assert!(store.get_submission(session.id).unwrap().is_none());
    }

    #[test]
    fn abandon_preserves_ambiguous_remote_truth() {
        let (store, dir) = store();
        let session = session(1, bilibili_output());
        create_session(&store, &session).unwrap();
        ready_segment(&store, session.id, dir.path());
        let attempt = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: jiff::Timestamp::now(),
        };
        begin_upload(&store, session.id, 1, attempt.clone()).unwrap();
        require_recovery(&store, session.id, "operator requested stop".into()).unwrap();
        close_session(
            &store,
            session.id,
            CloseSessionRequest::Abandon { note: None },
        )
        .unwrap();
        assert!(matches!(
            store.get_segment(session.id, 1).unwrap().unwrap().upload,
            UploadState::Ambiguous { .. }
        ));
    }

    #[test]
    fn not_uploaded_resolution_on_abandoned_session_cancels_instead_of_retries() {
        let (store, dir) = store();
        let session = session(1, bilibili_output());
        create_session(&store, &session).unwrap();
        ready_segment(&store, session.id, dir.path());
        let attempt = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: jiff::Timestamp::now(),
        };
        begin_upload(&store, session.id, 1, attempt).unwrap();
        require_recovery(&store, session.id, "stop".into()).unwrap();
        close_session(
            &store,
            session.id,
            CloseSessionRequest::Abandon { note: None },
        )
        .unwrap();

        let segment = resolve_upload_not_uploaded(&store, session.id, 1, None).unwrap();
        assert!(matches!(segment.upload, UploadState::Cancelled { .. }));
        assert_eq!(
            segment.upload_resolutions.last().unwrap().decision,
            UploadResolutionDecision::ConfirmedNotUploaded
        );
    }

    #[test]
    fn target_backoff_and_block_are_durable() {
        let (store, dir) = store();
        let session = session(1, bilibili_output());
        create_session(&store, &session).unwrap();
        ready_segment(&store, session.id, dir.path());
        let target = UploadTarget::from(session.output_plan.upload_plan().unwrap());
        let retry_at = jiff::Timestamp::now();
        let first = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: retry_at,
        };
        begin_upload(&store, session.id, 1, first.clone()).unwrap();
        schedule_upload_target_retry(
            &store,
            session.id,
            1,
            first.id,
            "timeout".into(),
            retry_at,
            1,
        )
        .unwrap();
        assert!(matches!(
            store
                .get_upload_target_state(&target)
                .unwrap()
                .unwrap()
                .gate,
            UploadTargetGate::Backoff { failures: 1, .. }
        ));
        drop(store);

        let reopened = StateStore::open_existing(dir.path().join("state.redb")).unwrap();
        assert!(matches!(
            reopened
                .get_upload_target_state(&target)
                .unwrap()
                .unwrap()
                .gate,
            UploadTargetGate::Backoff { failures: 1, .. }
        ));
        let second = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: jiff::Timestamp::now(),
        };
        begin_upload(&reopened, session.id, 1, second.clone()).unwrap();
        schedule_upload_target_retry(
            &reopened,
            session.id,
            1,
            second.id,
            "timeout again".into(),
            jiff::Timestamp::now(),
            2,
        )
        .unwrap();
        assert!(matches!(
            reopened
                .get_upload_target_state(&target)
                .unwrap()
                .unwrap()
                .gate,
            UploadTargetGate::Backoff { failures: 2, .. }
        ));
        let third = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: jiff::Timestamp::now(),
        };
        begin_upload(&reopened, session.id, 1, third.clone()).unwrap();
        block_upload_for_target(
            &reopened,
            session.id,
            1,
            third.id,
            "invalid credential".into(),
        )
        .unwrap();
        assert!(matches!(
            reopened
                .get_upload_target_state(&target)
                .unwrap()
                .unwrap()
                .gate,
            UploadTargetGate::Blocked { .. }
        ));
        resolve_upload_not_uploaded(&reopened, session.id, 1, None).unwrap();
        assert_eq!(
            reopened
                .get_upload_target_state(&target)
                .unwrap()
                .unwrap()
                .gate,
            UploadTargetGate::Ready
        );
    }

    #[test]
    fn unrelated_item_recovery_cannot_clear_another_sessions_target_gate() {
        let (store, dir) = store();
        let first = session(1, bilibili_output());
        let second = session(2, bilibili_output());
        create_session(&store, &first).unwrap();
        create_session(&store, &second).unwrap();
        ready_segment(&store, first.id, dir.path());
        ready_segment(&store, second.id, dir.path());
        let target = UploadTarget::from(first.output_plan.upload_plan().unwrap());

        let first_attempt = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: jiff::Timestamp::now(),
        };
        begin_upload(&store, first.id, 1, first_attempt.clone()).unwrap();
        block_upload(&store, first.id, 1, first_attempt.id, "bad item".into()).unwrap();

        let second_attempt = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: jiff::Timestamp::now(),
        };
        begin_upload(&store, second.id, 1, second_attempt.clone()).unwrap();
        block_upload_for_target(
            &store,
            second.id,
            1,
            second_attempt.id,
            "credential rejected".into(),
        )
        .unwrap();

        resolve_upload_not_uploaded(&store, first.id, 1, None).unwrap();
        assert!(matches!(
            store.get_upload_target_state(&target).unwrap().unwrap().gate,
            UploadTargetGate::Blocked {
                owner: RemoteOperationRef::Upload { session_id, .. },
                ..
            } if session_id == second.id
        ));
    }

    #[test]
    fn abandoning_the_owner_of_a_blocked_target_is_refused() {
        let (store, dir) = store();
        let session = session(1, bilibili_output());
        create_session(&store, &session).unwrap();
        ready_segment(&store, session.id, dir.path());
        let attempt = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: jiff::Timestamp::now(),
        };
        begin_upload(&store, session.id, 1, attempt.clone()).unwrap();
        block_upload_for_target(
            &store,
            session.id,
            1,
            attempt.id,
            "credential rejected".into(),
        )
        .unwrap();
        require_recovery(&store, session.id, "operator requested stop".into()).unwrap();

        let error = close_session(
            &store,
            session.id,
            CloseSessionRequest::Abandon { note: None },
        )
        .unwrap_err();
        assert!(error.to_string().contains("owns the blocked upload target"));
    }

    #[test]
    fn stale_upload_completion_cannot_replace_durable_proof() {
        let (store, dir) = store();
        let session = session(1, bilibili_output());
        create_session(&store, &session).unwrap();
        ready_segment(&store, session.id, dir.path());
        let attempt = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: jiff::Timestamp::now(),
        };
        begin_upload(&store, session.id, 1, attempt.clone()).unwrap();
        let proof = UploadedPart {
            bili_filename: "first".into(),
            part_title: "Part 1".into(),
        };
        complete_upload(&store, session.id, 1, attempt.id, proof.clone()).unwrap();

        assert!(
            complete_upload(
                &store,
                session.id,
                1,
                attempt.id,
                UploadedPart {
                    bili_filename: "replacement".into(),
                    part_title: "Part 1".into(),
                },
            )
            .is_err()
        );
        assert_eq!(
            store
                .get_segment(session.id, 1)
                .unwrap()
                .unwrap()
                .uploaded_part(),
            Some(&proof)
        );
    }

    #[test]
    fn submission_resolution_history_survives_the_next_attempt() {
        let (store, dir) = store();
        let session = session(1, bilibili_output());
        create_session(&store, &session).unwrap();
        ready_segment(&store, session.id, dir.path());
        close_session(
            &store,
            session.id,
            CloseSessionRequest::Natural { note: None },
        )
        .unwrap();

        let upload_attempt = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: jiff::Timestamp::now(),
        };
        begin_upload(&store, session.id, 1, upload_attempt.clone()).unwrap();
        complete_upload(
            &store,
            session.id,
            1,
            upload_attempt.id,
            UploadedPart {
                bili_filename: "remote".into(),
                part_title: "Part 1".into(),
            },
        )
        .unwrap();

        let first = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: jiff::Timestamp::now(),
        };
        begin_submission(&store, session.id, first.clone()).unwrap();
        block_submission(
            &store,
            session.id,
            first.id,
            "explicit API rejection".into(),
        )
        .unwrap();
        assert!(
            begin_submission(
                &store,
                session.id,
                RemoteAttempt {
                    id: Uuid::new_v4(),
                    started_at: jiff::Timestamp::now(),
                },
            )
            .is_err()
        );

        resolve_submission_not_submitted(
            &store,
            session.id,
            Some("verified no archive exists".into()),
        )
        .unwrap();
        let second = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: jiff::Timestamp::now(),
        };
        let submission = begin_submission(&store, session.id, second).unwrap();
        assert_eq!(submission.attempts.len(), 2);
        assert_eq!(submission.resolutions.len(), 1);
        assert_eq!(
            submission.resolutions[0].decision,
            SubmissionResolutionDecision::ConfirmedNotSubmitted
        );
    }

    #[test]
    fn artifact_conflict_decision_is_durable_and_blocks_session_close_until_completed() {
        let (store, dir) = store();
        let session = session(1, OutputPlan::LocalOnly);
        create_session(&store, &session).unwrap();
        open_segment(&store, raw_segment(session.id, 1, dir.path())).unwrap();
        begin_artifact_finalization(&store, session.id, 1, SegmentCloseReason::StreamEnded)
            .unwrap();
        require_recovery(&store, session.id, "both part and final files exist".into()).unwrap();

        let resolving = begin_artifact_conflict_resolution(
            &store,
            session.id,
            1,
            ArtifactResolutionDecision::KeepFinal,
            Some("final file inspected".into()),
        )
        .unwrap();
        assert!(matches!(
            resolving.artifact,
            ArtifactState::ResolvingConflict {
                decision: ArtifactResolutionDecision::KeepFinal,
                ..
            }
        ));
        assert_eq!(resolving.artifact_resolutions.len(), 1);
        assert!(
            close_session(
                &store,
                session.id,
                CloseSessionRequest::Recover {
                    exclude_failed: false,
                    note: None,
                },
            )
            .is_err()
        );

        let completed = complete_artifact_conflict_resolution(&store, session.id, 1).unwrap();
        assert!(matches!(completed.artifact, ArtifactState::Ready { .. }));
        assert_eq!(completed.artifact_resolutions.len(), 1);
    }
}
