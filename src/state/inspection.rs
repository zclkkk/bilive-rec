use std::collections::HashMap;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use uuid::Uuid;

use crate::error::AppResult;
use crate::state::model::{
    ArtifactState, LiveSession, OutputPlan, RoomLifecycle, RoomState, Segment, SessionLifecycle,
    Submission, SubmissionState, UploadState, UploadTargetGate, UploadTargetState,
};
use crate::state::store::{StateStore, StateSummary};

/// A read-only projection used by status output and operator diagnostics. It
/// deliberately includes the durable rows as well as observations about the
/// filesystem; an observation never mutates state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateInspection {
    pub summary: StateSummary,
    pub sessions: Vec<LiveSession>,
    pub segments: Vec<Segment>,
    pub submissions: Vec<Submission>,
    pub room_states: Vec<(u64, RoomState)>,
    pub upload_targets: Vec<UploadTargetState>,
    pub file_presence: Vec<SegmentFilePresence>,
    pub anomalies: Vec<Anomaly>,
}

impl StateInspection {
    pub fn load(store: &StateStore) -> AppResult<Self> {
        let snapshot = store.read_snapshot()?;
        let mut sessions = snapshot.sessions;
        let mut segments = snapshot.segments;
        let mut submissions = snapshot.submissions;
        let mut room_states = snapshot.room_states;
        let mut upload_targets = snapshot.upload_targets;

        sessions.sort_by_key(|session| session.id);
        segments.sort_by_key(|segment| (segment.session_id, segment.index));
        submissions.sort_by_key(|submission| submission.session_id);
        room_states.sort_by_key(|(room_id, _)| *room_id);
        upload_targets.sort_by(|left, right| {
            left.target
                .principal
                .credential
                .name
                .cmp(&right.target.principal.credential.name)
                .then_with(|| left.target.line.cmp(&right.target.line))
                .then_with(|| left.target.threads.cmp(&right.target.threads))
                .then_with(|| {
                    left.target
                        .submit_api
                        .as_config_value()
                        .cmp(right.target.submit_api.as_config_value())
                })
        });

        let summary = StateSummary {
            session_count: sessions.len(),
            segment_count: segments.len(),
            submission_count: submissions.len(),
            room_count: room_states.len(),
            upload_target_count: upload_targets.len(),
        };
        let file_presence: Vec<_> = segments.iter().map(SegmentFilePresence::inspect).collect();
        let anomalies = detect_anomalies(
            &sessions,
            &segments,
            &submissions,
            &room_states,
            &upload_targets,
            &file_presence,
        );

        Ok(Self {
            summary,
            sessions,
            segments,
            submissions,
            room_states,
            upload_targets,
            file_presence,
            anomalies,
        })
    }
}

/// What inspection could establish about a path without changing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilePresence {
    Missing,
    RegularFile,
    Other,
    Unreadable { reason: String },
}

impl FilePresence {
    fn inspect(path: &Path) -> Self {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_file() => Self::RegularFile,
            Ok(_) => Self::Other,
            Err(error) if error.kind() == ErrorKind::NotFound => Self::Missing,
            Err(error) => Self::Unreadable {
                reason: error.to_string(),
            },
        }
    }

    fn is_present(&self) -> bool {
        matches!(self, Self::RegularFile | Self::Other)
    }

    fn label(&self) -> String {
        match self {
            Self::Missing => "missing".into(),
            Self::RegularFile => "regular_file".into(),
            Self::Other => "non_file".into(),
            Self::Unreadable { reason } => format!("unreadable({reason})"),
        }
    }
}

/// The part/final existence matrix for one segment. Keeping both observations
/// is essential: either path alone is insufficient to recover rename windows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFilePresence {
    pub session_id: Uuid,
    pub segment_index: u32,
    pub part: FilePresence,
    pub final_file: FilePresence,
}

impl SegmentFilePresence {
    fn inspect(segment: &Segment) -> Self {
        Self {
            session_id: segment.session_id,
            segment_index: segment.index,
            part: FilePresence::inspect(&segment.part_path),
            final_file: FilePresence::inspect(&segment.final_path),
        }
    }

    fn matrix(&self) -> String {
        format!(
            "part={}, final={}",
            self.part.label(),
            self.final_file.label()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnomalyKind {
    OpenSession,
    SessionRecoveryRequired,
    RoomOwnershipMismatch,
    OrphanSegment,
    OrphanSubmission,
    InvalidSegmentPaths,
    InterruptedSegment,
    FinalizationConflict,
    ArtifactResolutionPending,
    DiscardConflict,
    DeletionConflict,
    FailedSegment,
    MissingSegmentFile,
    UnexpectedSegmentFile,
    InaccessibleSegmentPath,
    InvalidUploadState,
    InterruptedUpload,
    AmbiguousUpload,
    BlockedUpload,
    InvalidSubmissionState,
    InterruptedSubmission,
    AmbiguousSubmission,
    BlockedSubmission,
    UploadTargetBackoff,
    UploadTargetBlocked,
}

impl AnomalyKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OpenSession => "open_session",
            Self::SessionRecoveryRequired => "session_recovery_required",
            Self::RoomOwnershipMismatch => "room_ownership_mismatch",
            Self::OrphanSegment => "orphan_segment",
            Self::OrphanSubmission => "orphan_submission",
            Self::InvalidSegmentPaths => "invalid_segment_paths",
            Self::InterruptedSegment => "interrupted_segment",
            Self::FinalizationConflict => "finalization_conflict",
            Self::ArtifactResolutionPending => "artifact_resolution_pending",
            Self::DiscardConflict => "discard_conflict",
            Self::DeletionConflict => "deletion_conflict",
            Self::FailedSegment => "failed_segment",
            Self::MissingSegmentFile => "missing_segment_file",
            Self::UnexpectedSegmentFile => "unexpected_segment_file",
            Self::InaccessibleSegmentPath => "inaccessible_segment_path",
            Self::InvalidUploadState => "invalid_upload_state",
            Self::InterruptedUpload => "interrupted_upload",
            Self::AmbiguousUpload => "ambiguous_upload",
            Self::BlockedUpload => "blocked_upload",
            Self::InvalidSubmissionState => "invalid_submission_state",
            Self::InterruptedSubmission => "interrupted_submission",
            Self::AmbiguousSubmission => "ambiguous_submission",
            Self::BlockedSubmission => "blocked_submission",
            Self::UploadTargetBackoff => "upload_target_backoff",
            Self::UploadTargetBlocked => "upload_target_blocked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Anomaly {
    pub kind: AnomalyKind,
    pub description: String,
    pub next_action: String,
}

fn detect_anomalies(
    sessions: &[LiveSession],
    segments: &[Segment],
    submissions: &[Submission],
    room_states: &[(u64, RoomState)],
    upload_targets: &[UploadTargetState],
    file_presence: &[SegmentFilePresence],
) -> Vec<Anomaly> {
    let sessions_by_id: HashMap<_, _> = sessions
        .iter()
        .map(|session| (session.id, session))
        .collect();
    let rooms_by_id: HashMap<_, _> = room_states
        .iter()
        .map(|(room_id, state)| (*room_id, state))
        .collect();
    let files_by_segment: HashMap<_, _> = file_presence
        .iter()
        .map(|files| ((files.session_id, files.segment_index), files))
        .collect();
    let mut anomalies = Vec::new();

    for session in sessions {
        inspect_session(
            session,
            rooms_by_id.get(&session.room_id).copied(),
            &mut anomalies,
        );
    }

    for (room_id, room) in room_states {
        inspect_room(*room_id, room, &sessions_by_id, &mut anomalies);
    }

    for segment in segments {
        let Some(files) = files_by_segment.get(&(segment.session_id, segment.index)) else {
            // StateInspection always constructs this matrix itself. Keeping the
            // fallback makes the pure projection logic honest if reused.
            continue;
        };
        let session = sessions_by_id.get(&segment.session_id).copied();
        if session.is_none() {
            anomalies.push(Anomaly {
                kind: AnomalyKind::OrphanSegment,
                description: format!(
                    "Segment {}/{} references a session that does not exist",
                    segment.session_id, segment.index
                ),
                next_action: "Preserve both files and repair or explicitly retire the orphaned durable row before retrying any upload".into(),
            });
        }
        inspect_segment_files(segment, files, &mut anomalies);
        inspect_upload(segment, session, &mut anomalies);
    }

    for submission in submissions {
        inspect_submission(
            submission,
            sessions_by_id.get(&submission.session_id).copied(),
            &mut anomalies,
        );
    }

    for target in upload_targets {
        inspect_upload_target(target, &mut anomalies);
    }

    anomalies
}

fn inspect_session(session: &LiveSession, room: Option<&RoomState>, anomalies: &mut Vec<Anomaly>) {
    match &session.lifecycle {
        SessionLifecycle::Open => {
            if room.is_some_and(|room| {
                room.lifecycle
                    == RoomLifecycle::Owned {
                        session_id: session.id,
                    }
            }) {
                anomalies.push(Anomaly {
                    kind: AnomalyKind::OpenSession,
                    description: format!(
                        "Session {} for room {} remains open and durably owns the room",
                        session.id, session.room_id
                    ),
                    next_action: "Restart `bilive-rec run` so it can resume or deterministically reconcile the interrupted recording".into(),
                });
            } else {
                anomalies.push(Anomaly {
                    kind: AnomalyKind::RoomOwnershipMismatch,
                    description: format!(
                        "Open session {} for room {} is not matched by an Owned room state",
                        session.id, session.room_id
                    ),
                    next_action: "Restart `bilive-rec run` to persist recovery-required state, then make an explicit recording recovery decision".into(),
                });
            }
        }
        SessionLifecycle::RecoveryRequired { reason, .. } => {
            anomalies.push(Anomaly {
                kind: AnomalyKind::SessionRecoveryRequired,
                description: format!(
                    "Session {} for room {} requires an explicit recording decision: {reason}",
                    session.id, session.room_id
                ),
                next_action: recording_recovery_action(session.id),
            });
            if !room.is_some_and(|room| {
                room.lifecycle
                    == RoomLifecycle::Blocked {
                        session_id: session.id,
                    }
            }) {
                anomalies.push(Anomaly {
                    kind: AnomalyKind::RoomOwnershipMismatch,
                    description: format!(
                        "Recovery-required session {} is not matched by a Blocked state for room {}",
                        session.id, session.room_id
                    ),
                    next_action: "Do not resume this room until its session and room rows are repaired atomically".into(),
                });
            }
        }
        SessionLifecycle::Closed { .. } => {}
    }
}

fn inspect_room(
    room_id: u64,
    room: &RoomState,
    sessions: &HashMap<Uuid, &LiveSession>,
    anomalies: &mut Vec<Anomaly>,
) {
    let (session_id, expected_lifecycle) = match &room.lifecycle {
        RoomLifecycle::Ready => return,
        RoomLifecycle::Owned { session_id } => (*session_id, "open"),
        RoomLifecycle::Blocked { session_id } => (*session_id, "recovery_required"),
    };
    let valid = sessions.get(&session_id).is_some_and(|session| {
        session.room_id == room_id
            && matches!(
                (&room.lifecycle, &session.lifecycle),
                (RoomLifecycle::Owned { .. }, SessionLifecycle::Open)
                    | (
                        RoomLifecycle::Blocked { .. },
                        SessionLifecycle::RecoveryRequired { .. }
                    )
            )
    });
    if !valid {
        anomalies.push(Anomaly {
            kind: AnomalyKind::RoomOwnershipMismatch,
            description: format!(
                "Room {room_id} references session {session_id}, but that session is missing, belongs to another room, or is not {expected_lifecycle}"
            ),
            next_action: "Keep the room stopped and repair the room/session ownership pair in one state transition".into(),
        });
    }
}

fn inspect_segment_files(
    segment: &Segment,
    files: &SegmentFilePresence,
    anomalies: &mut Vec<Anomaly>,
) {
    let identity = format!("Segment {}/{}", segment.session_id, segment.index);
    let matrix = files.matrix();

    if segment.part_path == segment.final_path {
        anomalies.push(Anomaly {
            kind: AnomalyKind::InvalidSegmentPaths,
            description: format!(
                "{identity} uses the same path for part and final files: {}",
                segment.part_path.display()
            ),
            next_action: "Stop processing this segment and repair its path layout before any rename or deletion".into(),
        });
    }
    for (label, path, presence) in [
        ("part", segment.part_path.as_path(), &files.part),
        ("final", segment.final_path.as_path(), &files.final_file),
    ] {
        if let FilePresence::Unreadable { reason } = presence {
            anomalies.push(Anomaly {
                kind: AnomalyKind::InaccessibleSegmentPath,
                description: format!(
                    "{identity} {label} path could not be inspected at {}: {reason}",
                    path.display()
                ),
                next_action: "Restore filesystem access before deciding whether the artifact is missing or safe to recover".into(),
            });
        }
    }

    match &segment.artifact {
        ArtifactState::Writing => {
            anomalies.push(interrupted_segment(
                &identity,
                &matrix,
                "recording stopped while the part file was being written",
            ));
            if files.final_file.is_present() {
                anomalies.push(file_anomaly(
                    AnomalyKind::FinalizationConflict,
                    &identity,
                    &matrix,
                    "a final file exists even though no finalization intent was persisted",
                    "Preserve both files and inspect them before making the recording recovery decision",
                ));
            }
        }
        ArtifactState::Finalizing { .. } => match (&files.part, &files.final_file) {
            (FilePresence::RegularFile, FilePresence::Missing)
            | (FilePresence::Missing, FilePresence::RegularFile) => anomalies.push(
                interrupted_segment(&identity, &matrix, "finalization was interrupted"),
            ),
            (FilePresence::RegularFile, FilePresence::RegularFile) => anomalies.push(file_anomaly(
                AnomalyKind::FinalizationConflict,
                &identity,
                &matrix,
                "both rename endpoints exist, so recovery cannot choose one without losing truth",
                &segment_conflict_recovery_action(segment.session_id, segment.index),
            )),
            (FilePresence::Missing, FilePresence::Missing) => anomalies.push(file_anomaly(
                AnomalyKind::MissingSegmentFile,
                &identity,
                &matrix,
                "finalization intent has neither a part nor a final file",
                &recording_recovery_action(segment.session_id),
            )),
            (FilePresence::Other, _) | (_, FilePresence::Other) => anomalies.push(file_anomaly(
                AnomalyKind::FinalizationConflict,
                &identity,
                &matrix,
                "a rename endpoint exists but is not a regular file",
                "Repair the filesystem object type before attempting deterministic recovery",
            )),
            _ => {}
        },
        ArtifactState::ResolvingConflict { decision, .. } => anomalies.push(Anomaly {
            kind: AnomalyKind::ArtifactResolutionPending,
            description: format!(
                "{identity} has durable conflict decision {decision:?} awaiting filesystem commit ({matrix})"
            ),
            next_action: "Restart `bilive-rec run` to replay the persisted conflict decision; do not choose or edit the files again".into(),
        }),
        ArtifactState::Discarding { .. } => {
            if files.final_file.is_present() {
                let next_action = match (&files.part, &files.final_file) {
                    (FilePresence::RegularFile, FilePresence::RegularFile) => {
                        segment_conflict_recovery_action(segment.session_id, segment.index)
                    }
                    (FilePresence::Missing, FilePresence::RegularFile) => format!(
                        "Inspect the final file, then run `bilive-rec recover segment {} {} --keep-final` or `--exclude`",
                        segment.session_id, segment.index
                    ),
                    _ => "Repair the unexpected filesystem object type before resolving the discard conflict".into(),
                };
                anomalies.push(file_anomaly(
                    AnomalyKind::DiscardConflict,
                    &identity,
                    &matrix,
                    "a final file exists despite persisted discard intent",
                    &next_action,
                ));
            } else if matches!(&files.final_file, FilePresence::Missing) {
                if matches!(
                    &files.part,
                    FilePresence::RegularFile | FilePresence::Missing
                ) {
                    anomalies.push(interrupted_segment(
                        &identity,
                        &matrix,
                        "discard was interrupted and can be replayed from durable intent",
                    ));
                } else if matches!(&files.part, FilePresence::Other) {
                    anomalies.push(file_anomaly(
                        AnomalyKind::DiscardConflict,
                        &identity,
                        &matrix,
                        "the part path is not a regular file",
                        "Repair or preserve the unexpected filesystem object before replaying deletion",
                    ));
                }
            }
        }
        ArtifactState::Ready { .. } => {
            match &files.final_file {
                FilePresence::Missing | FilePresence::Other => anomalies.push(file_anomaly(
                    AnomalyKind::MissingSegmentFile,
                    &identity,
                    &matrix,
                    "a Ready artifact has no regular final file",
                    &recording_recovery_action(segment.session_id),
                )),
                FilePresence::RegularFile | FilePresence::Unreadable { .. } => {}
            }
            if files.part.is_present() {
                anomalies.push(file_anomaly(
                    AnomalyKind::UnexpectedSegmentFile,
                    &identity,
                    &matrix,
                    "a Ready artifact still has an unexpected part path",
                    "Preserve and inspect the part file; do not discard it merely because the final file is Ready",
                ));
            }
        }
        ArtifactState::Filtered { .. } => {
            if files.part.is_present() || files.final_file.is_present() {
                anomalies.push(file_anomaly(
                    AnomalyKind::UnexpectedSegmentFile,
                    &identity,
                    &matrix,
                    "a Filtered artifact still has a filesystem object",
                    "Inspect the retained object before deciding whether it can be removed",
                ));
            }
        }
        ArtifactState::Failed { reason, .. } => anomalies.push(Anomaly {
            kind: AnomalyKind::FailedSegment,
            description: format!("{identity} failed: {reason} ({matrix})"),
            next_action: recording_recovery_action(segment.session_id),
        }),
        ArtifactState::Excluded { .. } => {}
        ArtifactState::Deleting => {
            anomalies.push(interrupted_segment(
                &identity,
                &matrix,
                "final-file deletion was interrupted",
            ));
            if matches!(&files.final_file, FilePresence::Other) {
                anomalies.push(file_anomaly(
                    AnomalyKind::DeletionConflict,
                    &identity,
                    &matrix,
                    "the final path targeted for deletion is not a regular file",
                    "Repair or preserve the unexpected filesystem object before replaying deletion",
                ));
            }
            if files.part.is_present() {
                anomalies.push(file_anomaly(
                    AnomalyKind::UnexpectedSegmentFile,
                    &identity,
                    &matrix,
                    "a deleting artifact also has an unexpected part path",
                    "Preserve and inspect the part path before replaying final-file deletion",
                ));
            }
        }
        ArtifactState::Deleted => {
            if files.part.is_present() || files.final_file.is_present() {
                anomalies.push(file_anomaly(
                    AnomalyKind::UnexpectedSegmentFile,
                    &identity,
                    &matrix,
                    "a Deleted artifact still has a filesystem object",
                    "Inspect the retained object and repair state or cleanup explicitly; do not silently delete it",
                ));
            }
        }
    }
}

fn inspect_upload(segment: &Segment, session: Option<&LiveSession>, anomalies: &mut Vec<Anomaly>) {
    let identity = format!("Segment {}/{}", segment.session_id, segment.index);

    if let Some(session) = session {
        let upload_planned = matches!(&session.output_plan, OutputPlan::Bilibili { .. });
        let state_is_valid = if !upload_planned {
            matches!(&segment.upload, UploadState::NotPlanned)
        } else {
            match (&segment.artifact, &segment.upload) {
                (
                    ArtifactState::Discarding { .. }
                    | ArtifactState::Filtered { .. }
                    | ArtifactState::Excluded { .. },
                    UploadState::NotPlanned,
                ) => true,
                (
                    ArtifactState::Deleting | ArtifactState::Deleted,
                    UploadState::Uploaded { .. },
                ) => true,
                (_, UploadState::Uploaded { .. }) => segment.artifact.is_usable(),
                (_, UploadState::NotPlanned) => false,
                (ArtifactState::Deleting | ArtifactState::Deleted, _) => false,
                (
                    ArtifactState::Discarding { .. }
                    | ArtifactState::Filtered { .. }
                    | ArtifactState::Excluded { .. },
                    _,
                ) => false,
                _ => true,
            }
        };
        if !state_is_valid {
            anomalies.push(Anomaly {
                kind: AnomalyKind::InvalidUploadState,
                description: format!(
                    "{identity} has upload state {:?}, which is incompatible with artifact {:?} and session output {:?}",
                    segment.upload, segment.artifact, session.output_plan
                ),
                next_action: "Stop the uploader and repair this combination through a checked state transition".into(),
            });
        }
    }

    match &segment.upload {
        UploadState::Attempting { attempt } => anomalies.push(Anomaly {
            kind: AnomalyKind::InterruptedUpload,
            description: format!(
                "{identity} upload attempt {} started at {} has no durable outcome",
                attempt.id, attempt.started_at
            ),
            next_action: "Restart `bilive-rec run` to mark the attempt ambiguous, verify the remote result, then resolve it explicitly".into(),
        }),
        UploadState::Ambiguous { attempt, reason } => anomalies.push(Anomaly {
            kind: AnomalyKind::AmbiguousUpload,
            description: format!(
                "{identity} upload attempt {} has an unknown remote outcome: {reason}",
                attempt.id
            ),
            next_action: upload_recovery_action(segment.session_id, segment.index),
        }),
        UploadState::Blocked { attempt_id, reason } => anomalies.push(Anomaly {
            kind: AnomalyKind::BlockedUpload,
            description: format!(
                "{identity} upload is blocked{}: {reason}",
                attempt_id.map_or_else(String::new, |id| format!(" after attempt {id}"))
            ),
            next_action: format!(
                "Correct the reported cause, then {}",
                upload_recovery_action(segment.session_id, segment.index)
            ),
        }),
        UploadState::NotPlanned
        | UploadState::Pending { .. }
        | UploadState::Uploaded { .. }
        | UploadState::Cancelled { .. } => {}
    }
}

fn inspect_submission(
    submission: &Submission,
    session: Option<&LiveSession>,
    anomalies: &mut Vec<Anomaly>,
) {
    match session {
        None => anomalies.push(Anomaly {
            kind: AnomalyKind::OrphanSubmission,
            description: format!(
                "Submission references session {}, which does not exist",
                submission.session_id
            ),
            next_action: "Preserve the submission history and repair its owning session before any remote retry".into(),
        }),
        Some(session)
            if !matches!(&session.output_plan, OutputPlan::Bilibili { .. })
                || !session.lifecycle.permits_submission() =>
        {
            anomalies.push(Anomaly {
                kind: AnomalyKind::InvalidSubmissionState,
                description: format!(
                    "Submission for session {} exists while the session output or lifecycle does not permit submission",
                    submission.session_id
                ),
                next_action: "Stop submission work and repair the owning session through a checked transition".into(),
            });
        }
        Some(_) => {}
    }

    match &submission.state {
        SubmissionState::Attempting { attempt } => anomalies.push(Anomaly {
            kind: AnomalyKind::InterruptedSubmission,
            description: format!(
                "Submission attempt {} for session {} started at {} has no durable outcome",
                attempt.id, submission.session_id, attempt.started_at
            ),
            next_action: "Restart `bilive-rec run` to mark the attempt ambiguous, verify Bilibili, then resolve it explicitly".into(),
        }),
        SubmissionState::Ambiguous { attempt, reason } => anomalies.push(Anomaly {
            kind: AnomalyKind::AmbiguousSubmission,
            description: format!(
                "Submission attempt {} for session {} has an unknown remote outcome: {reason}",
                attempt.id, submission.session_id
            ),
            next_action: submission_recovery_action(submission.session_id),
        }),
        SubmissionState::Blocked { attempt_id, reason } => anomalies.push(Anomaly {
            kind: AnomalyKind::BlockedSubmission,
            description: format!(
                "Submission for session {} is blocked{}: {reason}",
                submission.session_id,
                attempt_id.map_or_else(String::new, |id| format!(" after attempt {id}"))
            ),
            next_action: format!(
                "Correct the reported cause, then {}",
                submission_recovery_action(submission.session_id)
            ),
        }),
        SubmissionState::RetryScheduled { .. }
        | SubmissionState::RetryAuthorized { .. }
        | SubmissionState::Submitted { .. } => {}
    }
}

fn inspect_upload_target(target: &UploadTargetState, anomalies: &mut Vec<Anomaly>) {
    let label = format!(
        "upload target credential={} mid={} line={} threads={} submit_api={}",
        target.target.principal.credential.name,
        target.target.principal.expected_mid,
        target.target.line,
        target.target.threads,
        target.target.submit_api.as_config_value()
    );
    match &target.gate {
        UploadTargetGate::Ready => {}
        UploadTargetGate::Backoff {
            owner,
            failures,
            retry_at,
            last_error,
            ..
        } => anomalies.push(Anomaly {
            kind: AnomalyKind::UploadTargetBackoff,
            description: format!(
                "{label} is backing off after {failures} failure(s) until {retry_at}; owner={}: {last_error}",
                operation_label(owner)
            ),
            next_action: "`bilive-rec run` will consume this gate when the retry time arrives; correct the credential or network cause if failures continue".into(),
        }),
        UploadTargetGate::Blocked { owner, since, reason } => anomalies.push(Anomaly {
            kind: AnomalyKind::UploadTargetBlocked,
            description: format!("{label} has been blocked since {since}: {reason}"),
            next_action: format!("Repair or refresh the named credential. {}; restarting alone does not clear the durable gate", target_recovery_action(owner)),
        }),
    }
}

fn target_recovery_action(owner: &crate::state::model::RemoteOperationRef) -> String {
    match owner {
        crate::state::model::RemoteOperationRef::Upload {
            session_id,
            segment_index,
            ..
        } => upload_recovery_action(*session_id, *segment_index),
        crate::state::model::RemoteOperationRef::Submission { session_id, .. } => {
            submission_recovery_action(*session_id)
        }
    }
}

fn operation_label(owner: &crate::state::model::RemoteOperationRef) -> String {
    match owner {
        crate::state::model::RemoteOperationRef::Upload {
            session_id,
            segment_index,
            attempt_id,
        } => format!("upload {session_id}/{segment_index} attempt {attempt_id}"),
        crate::state::model::RemoteOperationRef::Submission {
            session_id,
            attempt_id,
        } => format!("submission {session_id} attempt {attempt_id}"),
    }
}

fn interrupted_segment(identity: &str, matrix: &str, detail: &str) -> Anomaly {
    file_anomaly(
        AnomalyKind::InterruptedSegment,
        identity,
        matrix,
        detail,
        "Restart `bilive-rec run` to replay the deterministic local recovery from durable intent",
    )
}

fn file_anomaly(
    kind: AnomalyKind,
    identity: &str,
    matrix: &str,
    detail: &str,
    next_action: &str,
) -> Anomaly {
    Anomaly {
        kind,
        description: format!("{identity}: {detail} ({matrix})"),
        next_action: next_action.into(),
    }
}

fn recording_recovery_action(session_id: Uuid) -> String {
    format!(
        "Inspect retained files, then run `bilive-rec recover recording {session_id} --finalize [--exclude-failed]` or `bilive-rec recover recording {session_id} --abandon`"
    )
}

fn upload_recovery_action(session_id: Uuid, segment_index: u32) -> String {
    format!(
        "Verify Bilibili first; if absent run `bilive-rec recover upload {session_id} {segment_index} --not-uploaded`, or if present use `--uploaded <bili_filename>`"
    )
}

fn segment_conflict_recovery_action(session_id: Uuid, segment_index: u32) -> String {
    format!(
        "Preserve and inspect both files, then run `bilive-rec recover segment {session_id} {segment_index} --keep-part`, `--keep-final`, or `--exclude`"
    )
}

fn submission_recovery_action(session_id: Uuid) -> String {
    format!(
        "Verify Bilibili first; if absent run `bilive-rec recover submission {session_id} --not-submitted`, or if present use `--submitted --bvid <BVID>` (or `--aid <AID>`)"
    )
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::state::model::{SegmentCloseReason, UploadState};

    #[test]
    fn file_presence_distinguishes_missing_file_and_non_file() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("segment.flv");
        fs::write(&file, b"data").unwrap();

        assert_eq!(FilePresence::inspect(&file), FilePresence::RegularFile);
        assert_eq!(FilePresence::inspect(dir.path()), FilePresence::Other);
        assert_eq!(
            FilePresence::inspect(&dir.path().join("missing.flv")),
            FilePresence::Missing
        );
    }

    #[test]
    fn finalizing_with_both_files_is_reported_as_a_conflict() {
        let dir = TempDir::new().unwrap();
        let part_path = dir.path().join("segment.flv.part");
        let final_path = dir.path().join("segment.flv");
        fs::write(&part_path, b"part").unwrap();
        fs::write(&final_path, b"final").unwrap();
        let segment = Segment {
            session_id: Uuid::new_v4(),
            index: 7,
            part_path,
            final_path,
            artifact: ArtifactState::Finalizing {
                close_reason: SegmentCloseReason::StreamEnded,
            },
            artifact_resolutions: Vec::new(),
            upload: UploadState::NotPlanned,
            upload_attempts: Vec::new(),
            upload_resolutions: Vec::new(),
        };
        let files = SegmentFilePresence::inspect(&segment);
        let mut anomalies = Vec::new();

        inspect_segment_files(&segment, &files, &mut anomalies);

        let conflict = anomalies
            .iter()
            .find(|anomaly| anomaly.kind == AnomalyKind::FinalizationConflict)
            .unwrap();
        assert!(conflict.next_action.contains(&format!(
            "recover segment {} 7 --keep-part",
            segment.session_id
        )));
    }

    #[test]
    fn target_gate_recovery_actions_show_complete_cli_alternatives() {
        let session_id = Uuid::new_v4();
        let upload = target_recovery_action(&crate::state::model::RemoteOperationRef::Upload {
            session_id,
            segment_index: 7,
            attempt_id: Uuid::new_v4(),
        });
        let submission =
            target_recovery_action(&crate::state::model::RemoteOperationRef::Submission {
                session_id,
                attempt_id: Uuid::new_v4(),
            });

        assert!(upload.contains(&format!(
            "`bilive-rec recover upload {session_id} 7 --not-uploaded`"
        )));
        assert!(upload.contains("`--uploaded <bili_filename>`"));
        assert!(submission.contains(&format!(
            "`bilive-rec recover submission {session_id} --not-submitted`"
        )));
        assert!(submission.contains("`--submitted --bvid <BVID>`"));
        assert!(!upload.contains("..."));
        assert!(!submission.contains("..."));
        assert!(!upload.contains("state recover"));
        assert!(!submission.contains("state recover"));
    }
}
