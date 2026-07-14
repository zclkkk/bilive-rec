use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::error::AppResult;
use crate::state::model::{
    ArtifactState, Segment, UploadState, UploadTargetGate, UploadTargetState, UploadedPart,
};
use crate::state::store::StateStore;
use crate::uploader::types::UploadRequest;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadySegmentForUpload {
    pub session_id: Uuid,
    pub segment_index: u32,
    pub path: PathBuf,
}

impl ReadySegmentForUpload {
    pub(crate) fn into_request(self, part_title: String) -> UploadRequest {
        UploadRequest {
            session_id: self.session_id,
            segment_index: self.segment_index,
            path: self.path,
            part_title,
        }
    }
}

/// Re-check local facts before the transition layer claims an upload attempt.
/// This function is deliberately read-only: the transition layer remains the
/// sole authority that can perform the final compare-and-set.
pub fn validate_ready_segment_for_upload(
    store: &StateStore,
    session_id: Uuid,
    segment_index: u32,
    expected_path: Option<&Path>,
    now: jiff::Timestamp,
) -> AppResult<Result<ReadySegmentForUpload, String>> {
    let Some(session) = store.get_session(session_id)? else {
        return Ok(Err(format!("session {session_id} not found")));
    };
    if !session.lifecycle.permits_upload() {
        return Ok(Err(format!(
            "session {session_id} lifecycle does not permit upload"
        )));
    }
    if session.output_plan.upload_plan().is_none() {
        return Ok(Err(format!(
            "session {session_id} is LocalOnly; upload was not planned"
        )));
    }
    if let Some(submission) = store.get_submission(session_id)? {
        return Ok(Err(format!(
            "session already has a {:?} submission; refusing another upload",
            submission.state
        )));
    }

    let Some(segment) = store.get_segment(session_id, segment_index)? else {
        return Ok(Err(format!(
            "segment {session_id}/{segment_index} not found"
        )));
    };
    if !matches!(segment.artifact, ArtifactState::Ready { .. })
        || !matches!(segment.upload, UploadState::Pending { .. })
        || !segment.upload.is_due(now)
    {
        return Ok(Err(format!(
            "segment {session_id}/{segment_index} is not a due Ready/Pending upload: artifact={:?}, upload={:?}",
            segment.artifact, segment.upload
        )));
    }
    if let Some(expected_path) = expected_path
        && segment.final_path != expected_path
    {
        return Ok(Err(format!(
            "segment path mismatch: expected {}, store has {}",
            expected_path.display(),
            segment.final_path.display()
        )));
    }
    if !segment.final_path.is_file() {
        return Ok(Err(format!(
            "path is not a regular file: {}",
            segment.final_path.display()
        )));
    }
    if !segment
        .final_path
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("flv"))
    {
        return Ok(Err(format!(
            "file is not a .flv: {}",
            segment.final_path.display()
        )));
    }

    Ok(Ok(ReadySegmentForUpload {
        session_id,
        segment_index,
        path: segment.final_path,
    }))
}

pub fn upload_target_is_ready(state: &UploadTargetState, now: jiff::Timestamp) -> bool {
    match state.gate {
        UploadTargetGate::Ready => true,
        UploadTargetGate::Backoff { retry_at, .. } => retry_at <= now,
        UploadTargetGate::Blocked { .. } => false,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentUploadState {
    Satisfied,
    NeedsUpload,
    Blocked { reason: String },
}

pub fn classify_segment_upload(segment: &Segment) -> SegmentUploadState {
    if matches!(
        segment.artifact,
        ArtifactState::Filtered { .. } | ArtifactState::Excluded { .. }
    ) {
        return SegmentUploadState::Satisfied;
    }

    match (&segment.artifact, &segment.upload) {
        (
            ArtifactState::Ready { .. } | ArtifactState::Deleting | ArtifactState::Deleted,
            UploadState::Uploaded { .. },
        ) => SegmentUploadState::Satisfied,
        (ArtifactState::Ready { .. }, UploadState::Pending { .. }) => {
            SegmentUploadState::NeedsUpload
        }
        (artifact, upload) => SegmentUploadState::Blocked {
            reason: format!(
                "segment {} is artifact={artifact:?}, upload={upload:?}",
                segment.index
            ),
        },
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct UploadReconciliation {
    pub needs_upload: Vec<u32>,
    pub blocked: Vec<(u32, String)>,
    pub uploaded_parts: Vec<(u32, UploadedPart)>,
}

impl UploadReconciliation {
    pub fn is_ready_for_submission(&self) -> bool {
        self.needs_upload.is_empty() && self.blocked.is_empty() && !self.uploaded_parts.is_empty()
    }
}

pub fn reconcile_session_uploads(segments: &[Segment]) -> UploadReconciliation {
    let mut report = UploadReconciliation::default();
    for segment in segments {
        match classify_segment_upload(segment) {
            SegmentUploadState::Satisfied => {
                if let UploadState::Uploaded { proof } = &segment.upload {
                    report.uploaded_parts.push((segment.index, proof.clone()));
                }
            }
            SegmentUploadState::NeedsUpload => report.needs_upload.push(segment.index),
            SegmentUploadState::Blocked { reason } => {
                report.blocked.push((segment.index, reason));
            }
        }
    }
    report.needs_upload.sort_unstable();
    report.uploaded_parts.sort_by_key(|(index, _)| *index);
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::model::SegmentCloseReason;

    fn segment(artifact: ArtifactState, upload: UploadState) -> Segment {
        Segment {
            session_id: Uuid::new_v4(),
            index: 1,
            part_path: "/tmp/1.part".into(),
            final_path: "/tmp/1.flv".into(),
            artifact,
            artifact_resolutions: Vec::new(),
            upload,
            upload_attempts: Vec::new(),
            upload_resolutions: Vec::new(),
        }
    }

    #[test]
    fn ready_pending_needs_upload() {
        assert_eq!(
            classify_segment_upload(&segment(
                ArtifactState::Ready {
                    close_reason: SegmentCloseReason::StreamEnded,
                },
                UploadState::pending(),
            )),
            SegmentUploadState::NeedsUpload
        );
    }

    #[test]
    fn uploaded_proof_is_sufficient_without_a_parallel_table() {
        let proof = UploadedPart {
            bili_filename: "remote".into(),
            part_title: "Part 1".into(),
        };
        let report = reconcile_session_uploads(&[segment(
            ArtifactState::Deleted,
            UploadState::Uploaded {
                proof: proof.clone(),
            },
        )]);

        assert!(report.is_ready_for_submission());
        assert_eq!(report.uploaded_parts, vec![(1, proof)]);
    }

    #[test]
    fn ambiguous_and_blocked_uploads_block_submission() {
        let blocked = segment(
            ArtifactState::Ready {
                close_reason: SegmentCloseReason::StreamEnded,
            },
            UploadState::Blocked {
                attempt_id: None,
                reason: "invalid cookie".into(),
            },
        );

        let report = reconcile_session_uploads(&[blocked]);
        assert!(!report.is_ready_for_submission());
        assert_eq!(report.blocked.len(), 1);
    }
}
