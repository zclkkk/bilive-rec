use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::{Copyright, SubmitApi};
use crate::credential::{CredentialRef, UploadPrincipal};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveSession {
    pub id: Uuid,
    pub room_id: u64,
    pub room_name: String,
    pub title: String,
    pub started_at: jiff::Timestamp,
    pub lifecycle: SessionLifecycle,
    pub recording_plan: RecordingPlan,
    pub output_plan: OutputPlan,
    pub recording_events: Vec<RecordingEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionLifecycle {
    Open,
    RecoveryRequired {
        reason: String,
        detected_at: jiff::Timestamp,
    },
    Closed {
        closure: SessionClosure,
    },
}

impl SessionLifecycle {
    pub fn is_open(&self) -> bool {
        matches!(self, Self::Open)
    }

    pub fn permits_upload(&self) -> bool {
        matches!(
            self,
            Self::Open
                | Self::Closed {
                    closure: SessionClosure::Completed { .. },
                }
        )
    }

    pub fn permits_submission(&self) -> bool {
        matches!(
            self,
            Self::Closed {
                closure: SessionClosure::Completed { .. },
            }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionClosure {
    Completed {
        closed_at: jiff::Timestamp,
        note: Option<String>,
    },
    NoUsableRecording {
        closed_at: jiff::Timestamp,
        reason: String,
    },
    Abandoned {
        closed_at: jiff::Timestamp,
        note: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecordingEvent {
    RecoveryRequired {
        detected_at: jiff::Timestamp,
        reason: String,
    },
    OperatorResolved {
        resolved_at: jiff::Timestamp,
        decision: RecordingDecision,
        note: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordingDecision {
    Finalized,
    Abandoned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingPlan {
    pub credential: Option<CredentialRef>,
    pub output_dir: PathBuf,
    pub segment_time_ms: Option<u64>,
    pub segment_size: Option<u64>,
    pub min_segment_size: u64,
    pub qn: u32,
    pub cdn: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutputPlan {
    LocalOnly,
    Bilibili {
        upload: UploadPlan,
        submission: Box<SubmissionSpec>,
    },
}

impl OutputPlan {
    pub fn upload_plan(&self) -> Option<&UploadPlan> {
        match self {
            Self::LocalOnly => None,
            Self::Bilibili { upload, .. } => Some(upload),
        }
    }

    pub fn submission_spec(&self) -> Option<&SubmissionSpec> {
        match self {
            Self::LocalOnly => None,
            Self::Bilibili { submission, .. } => Some(submission),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadPlan {
    pub principal: UploadPrincipal,
    pub line: String,
    pub threads: usize,
    pub submit_api: SubmitApi,
    pub delete_after_submit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadTarget {
    pub principal: UploadPrincipal,
    pub line: String,
    pub threads: usize,
    pub submit_api: SubmitApi,
}

impl From<&UploadPlan> for UploadTarget {
    fn from(plan: &UploadPlan) -> Self {
        Self {
            principal: plan.principal.clone(),
            line: plan.line.clone(),
            threads: plan.threads,
            submit_api: plan.submit_api.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadTargetState {
    pub target: UploadTarget,
    pub gate: UploadTargetGate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UploadTargetGate {
    Ready,
    Backoff {
        owner: RemoteOperationRef,
        failures: u32,
        retry_at: jiff::Timestamp,
        last_error: String,
    },
    Blocked {
        owner: RemoteOperationRef,
        since: jiff::Timestamp,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RemoteOperationRef {
    Upload {
        session_id: Uuid,
        segment_index: u32,
        attempt_id: Uuid,
    },
    Submission {
        session_id: Uuid,
        attempt_id: Uuid,
    },
}

impl RemoteOperationRef {
    pub fn session_id(&self) -> Uuid {
        match self {
            Self::Upload { session_id, .. } | Self::Submission { session_id, .. } => *session_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionSpec {
    pub title: String,
    pub description: String,
    pub category_id: u16,
    pub copyright: Copyright,
    pub source: String,
    pub tags: Vec<String>,
    pub private: bool,
    pub dynamic: String,
    pub forbid_reprint: bool,
    pub charging_panel: bool,
    pub close_reply: bool,
    pub close_danmu: bool,
    pub featured_reply: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoomState {
    pub lifecycle: RoomLifecycle,
    pub changed_at: jiff::Timestamp,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RoomLifecycle {
    Ready,
    Owned { session_id: Uuid },
    Blocked { session_id: Uuid },
}

impl RoomLifecycle {
    pub fn session_id(&self) -> Option<Uuid> {
        match self {
            Self::Ready => None,
            Self::Owned { session_id } | Self::Blocked { session_id } => Some(*session_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Segment {
    pub session_id: Uuid,
    pub index: u32,
    pub part_path: PathBuf,
    pub final_path: PathBuf,
    pub artifact: ArtifactState,
    pub artifact_resolutions: Vec<ArtifactResolution>,
    pub upload: UploadState,
    pub upload_attempts: Vec<UploadAttempt>,
    pub upload_resolutions: Vec<UploadResolution>,
}

impl Segment {
    pub fn local_path(&self) -> &Path {
        match self.artifact {
            ArtifactState::Ready { .. }
            | ArtifactState::Filtered { .. }
            | ArtifactState::Deleting
            | ArtifactState::Deleted => &self.final_path,
            ArtifactState::Writing
            | ArtifactState::Finalizing { .. }
            | ArtifactState::Discarding { .. }
            | ArtifactState::Failed { .. }
            | ArtifactState::Excluded { .. }
            | ArtifactState::ResolvingConflict { .. } => &self.part_path,
        }
    }

    pub fn close_reason(&self) -> Option<&SegmentCloseReason> {
        self.artifact.close_reason()
    }

    pub fn uploaded_part(&self) -> Option<&UploadedPart> {
        match &self.upload {
            UploadState::Uploaded { proof } => Some(proof),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactState {
    Writing,
    Finalizing {
        close_reason: SegmentCloseReason,
    },
    Ready {
        close_reason: SegmentCloseReason,
    },
    Discarding {
        close_reason: SegmentCloseReason,
    },
    ResolvingConflict {
        close_reason: SegmentCloseReason,
        decision: ArtifactResolutionDecision,
    },
    Filtered {
        close_reason: SegmentCloseReason,
    },
    Failed {
        reason: String,
        close_reason: Option<SegmentCloseReason>,
    },
    Excluded {
        reason: String,
    },
    Deleting,
    Deleted,
}

impl ArtifactState {
    pub fn close_reason(&self) -> Option<&SegmentCloseReason> {
        match self {
            Self::Finalizing { close_reason }
            | Self::Ready { close_reason }
            | Self::Discarding { close_reason }
            | Self::Filtered { close_reason }
            | Self::ResolvingConflict { close_reason, .. } => Some(close_reason),
            Self::Failed { close_reason, .. } => close_reason.as_ref(),
            Self::Writing | Self::Excluded { .. } | Self::Deleting | Self::Deleted => None,
        }
    }

    pub fn is_usable(&self) -> bool {
        matches!(self, Self::Ready { .. } | Self::Deleting | Self::Deleted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactResolution {
    pub decided_at: jiff::Timestamp,
    pub decision: ArtifactResolutionDecision,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactResolutionDecision {
    KeepPart,
    KeepFinal,
    Exclude,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UploadState {
    NotPlanned,
    Pending {
        failures: u32,
        retry_at: Option<jiff::Timestamp>,
        last_error: Option<String>,
    },
    Attempting {
        attempt: RemoteAttempt,
    },
    Blocked {
        attempt_id: Option<Uuid>,
        reason: String,
    },
    Ambiguous {
        attempt: RemoteAttempt,
        reason: String,
    },
    Uploaded {
        proof: UploadedPart,
    },
    Cancelled {
        cancelled_at: jiff::Timestamp,
        reason: String,
    },
}

impl UploadState {
    pub fn pending() -> Self {
        Self::Pending {
            failures: 0,
            retry_at: None,
            last_error: None,
        }
    }

    pub fn is_due(&self, now: jiff::Timestamp) -> bool {
        match self {
            Self::Pending { retry_at, .. } => retry_at.is_none_or(|at| at <= now),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteAttempt {
    pub id: Uuid,
    pub started_at: jiff::Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadAttempt {
    pub attempt: RemoteAttempt,
    pub finished_at: Option<jiff::Timestamp>,
    pub outcome: Option<UploadAttemptOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UploadAttemptOutcome {
    Confirmed { proof: UploadedPart },
    RetryScheduled { reason: String },
    Blocked { reason: String },
    Ambiguous { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadedPart {
    pub bili_filename: String,
    pub part_title: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadResolution {
    pub attempt_id: Option<Uuid>,
    pub resolved_at: jiff::Timestamp,
    pub decision: UploadResolutionDecision,
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UploadResolutionDecision {
    ConfirmedNotUploaded,
    ConfirmedUploaded { proof: UploadedPart },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Submission {
    pub session_id: Uuid,
    pub state: SubmissionState,
    pub attempts: Vec<SubmissionAttempt>,
    pub resolutions: Vec<SubmissionResolution>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubmissionState {
    RetryScheduled {
        failures: u32,
        retry_at: jiff::Timestamp,
        last_error: String,
    },
    Attempting {
        attempt: RemoteAttempt,
    },
    Blocked {
        attempt_id: Option<Uuid>,
        reason: String,
    },
    Ambiguous {
        attempt: RemoteAttempt,
        reason: String,
    },
    RetryAuthorized {
        authorized_at: jiff::Timestamp,
    },
    Submitted {
        aid: Option<u64>,
        bvid: Option<String>,
    },
}

impl SubmissionState {
    pub fn is_due(&self, now: jiff::Timestamp) -> bool {
        match self {
            Self::RetryScheduled { retry_at, .. } => *retry_at <= now,
            Self::RetryAuthorized { .. } => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionAttempt {
    pub attempt: RemoteAttempt,
    pub finished_at: Option<jiff::Timestamp>,
    pub outcome: Option<SubmissionAttemptOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubmissionAttemptOutcome {
    Submitted {
        aid: Option<u64>,
        bvid: Option<String>,
    },
    RetryScheduled {
        reason: String,
    },
    Blocked {
        reason: String,
    },
    Ambiguous {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionResolution {
    pub attempt_id: Option<Uuid>,
    pub resolved_at: jiff::Timestamp,
    pub decision: SubmissionResolutionDecision,
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubmissionResolutionDecision {
    ConfirmedNotSubmitted,
    ConfirmedSubmitted {
        aid: Option<u64>,
        bvid: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
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
                for (index, trigger) in triggers.iter().enumerate() {
                    if index > 0 {
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
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
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
                write!(f, "time_limit(elapsed={elapsed_ms}ms, limit={limit_ms}ms)")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uploaded_proof_is_part_of_upload_state() {
        let state = UploadState::Uploaded {
            proof: UploadedPart {
                bili_filename: "remote".into(),
                part_title: "Part 1".into(),
            },
        };
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(serde_json::from_str::<UploadState>(&json).unwrap(), state);
    }

    #[test]
    fn artifact_state_owns_close_reason_and_failure_reason() {
        let artifact = ArtifactState::Failed {
            reason: "disk full".into(),
            close_reason: Some(SegmentCloseReason::StreamEnded),
        };
        assert_eq!(
            artifact.close_reason(),
            Some(&SegmentCloseReason::StreamEnded)
        );
        let json = serde_json::to_string(&artifact).unwrap();
        assert_eq!(
            serde_json::from_str::<ArtifactState>(&json).unwrap(),
            artifact
        );
    }

    #[test]
    fn closed_session_lifecycle_has_a_nested_serialization_boundary() {
        let lifecycle = SessionLifecycle::Closed {
            closure: SessionClosure::Completed {
                closed_at: jiff::Timestamp::now(),
                note: Some("done".into()),
            },
        };
        let json = serde_json::to_string(&lifecycle).unwrap();

        assert!(json.contains("\"closure\""));
        assert_eq!(
            serde_json::from_str::<SessionLifecycle>(&json).unwrap(),
            lifecycle
        );
    }
}
