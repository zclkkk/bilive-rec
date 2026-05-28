use std::collections::HashSet;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::error::AppResult;
use crate::state::model::{Segment, SegmentStatus, SubmissionStatus, UploadedPart};
use crate::state::store::{StateStore, StoreTxn};
use crate::uploader::types::{UploadRequest, Uploader};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedSegmentForUpload {
    pub session_id: Uuid,
    pub segment_index: u32,
    pub path: PathBuf,
}

impl FinalizedSegmentForUpload {
    pub fn into_request(self, part_title: String) -> UploadRequest {
        UploadRequest {
            session_id: self.session_id,
            segment_index: self.segment_index,
            path: self.path,
            part_title,
        }
    }
}

#[derive(Debug)]
pub enum PersistedUploadFailure {
    StateBeforeRemote { index: u32, error: String },
    Remote { index: u32, error: String },
    StateAfterRemote { index: u32, error: String },
}

pub async fn upload_and_persist_segment<U: Uploader + ?Sized>(
    uploader: &U,
    store: &StateStore,
    segment: FinalizedSegmentForUpload,
    part_title: String,
) -> Result<UploadedPart, PersistedUploadFailure> {
    let index = segment.segment_index;

    // Atomic: flip to Uploading before the remote call.
    store
        .write(|txn| set_segment_status_txn(txn, &segment, SegmentStatus::Uploading, None))
        .map_err(|error| PersistedUploadFailure::StateBeforeRemote {
            index,
            error: error.to_string(),
        })?;

    let req = segment.clone().into_request(part_title);
    let uploaded_part = uploader.upload_segment(req).await.map_err(|error| {
        // Best-effort rollback; if the store is broken too, report both.
        let reset_result = store.write(|txn| {
            set_segment_status_txn(txn, &segment, SegmentStatus::Finalized, None)
        });
        match reset_result {
            Ok(()) => PersistedUploadFailure::Remote {
                index,
                error: error.to_string(),
            },
            Err(reset_error) => PersistedUploadFailure::StateAfterRemote {
                index,
                error: format!(
                    "remote upload failed: {error}; additionally failed to reset segment status to Finalized: {reset_error}"
                ),
            },
        }
    })?;

    // Atomic: persist the uploaded part and advance the segment status in one
    // transaction so recovery always sees a consistent pair.
    store
        .write(|txn| {
            txn.put_uploaded_part(&uploaded_part)?;
            set_segment_status_txn(txn, &segment, SegmentStatus::Uploaded, None)
        })
        .map_err(|error| PersistedUploadFailure::StateAfterRemote {
            index,
            error: format!(
                "remote filename {}, persistence error: {}",
                uploaded_part.bili_filename, error
            ),
        })?;

    Ok(uploaded_part)
}

fn set_segment_status_txn(
    txn: &StoreTxn<'_>,
    segment: &FinalizedSegmentForUpload,
    status: SegmentStatus,
    error: Option<String>,
) -> AppResult<()> {
    let mut stored = txn
        .get_segment(segment.session_id, segment.segment_index)?
        .ok_or_else(|| {
            crate::error::AppError::State(format!(
                "segment {}/{} not found",
                segment.session_id, segment.segment_index
            ))
        })?;
    stored.status = status;
    stored.error = error;
    txn.put_segment(&stored)
}

pub fn validate_finalized_segment_for_upload(
    store: &StateStore,
    session_id: Uuid,
    segment_index: u32,
    expected_path: Option<&Path>,
) -> AppResult<Result<FinalizedSegmentForUpload, String>> {
    if let Some(submission) = store.get_submission(session_id)? {
        let status = match submission.status {
            SubmissionStatus::Submitted => "Submitted",
            SubmissionStatus::Pending => "Pending",
            SubmissionStatus::Ambiguous => "Ambiguous",
            SubmissionStatus::Failed => "Failed",
        };
        return Ok(Err(format!(
            "session has a {status} submission — refusing upload"
        )));
    }

    let Some(segment) = store.get_segment(session_id, segment_index)? else {
        return Ok(Err(format!(
            "segment {session_id}/{segment_index} not found"
        )));
    };

    if segment.status != SegmentStatus::Finalized {
        return Ok(Err(format!(
            "segment {session_id}/{segment_index} is {:?}, not Finalized",
            segment.status
        )));
    }

    if let Some(expected_path) = expected_path
        && segment.path != expected_path
    {
        return Ok(Err(format!(
            "segment {session_id}/{segment_index} path mismatch: expected {}, store has {}",
            expected_path.display(),
            segment.path.display()
        )));
    }

    if !segment.path.exists() {
        return Ok(Err(format!(
            "file does not exist: {}",
            segment.path.display()
        )));
    }

    if !segment.path.is_file() {
        return Ok(Err(format!(
            "path is not a regular file: {}",
            segment.path.display()
        )));
    }

    if !segment
        .path
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("flv"))
    {
        return Ok(Err(format!(
            "file is not a .flv: {}",
            segment.path.display()
        )));
    }

    let uploaded_parts = store.list_uploaded_parts(session_id)?;
    if uploaded_parts
        .iter()
        .any(|part| part.segment_index == segment_index)
    {
        return Ok(Err(format!(
            "segment {session_id}/{segment_index} already has an UploadedPart"
        )));
    }

    Ok(Ok(FinalizedSegmentForUpload {
        session_id,
        segment_index,
        path: segment.path.clone(),
    }))
}

/// How one segment of a finalizing session relates to the upload requirement,
/// derived purely from persisted state: the segment's status and whether a
/// durable [`UploadedPart`] already exists for it.
///
/// This is the single rule consulted by both the pre-submission readiness gate
/// and the upload reconciliation loop, so the two can never drift apart about
/// what "every part is uploaded" means. The durable part — not the segment's
/// own status row — is the proof of upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentUploadState {
    /// The requirement is met: a durable part exists, or the segment was
    /// intentionally excluded from upload (`Filtered`).
    Satisfied,
    /// `Finalized` with no part yet — uploadable, must be uploaded before submit.
    NeedsUpload,
    /// Cannot be made ready automatically; submission and reconciliation must
    /// refuse and surface `reason`.
    Blocked { reason: String },
}

/// Classify a single segment against the upload requirement. `has_part` must be
/// true iff a durable [`UploadedPart`] exists for this segment.
pub fn classify_segment_upload(segment: &Segment, has_part: bool) -> SegmentUploadState {
    match segment.status {
        // Intentionally excluded from upload; nothing to submit for it.
        SegmentStatus::Filtered => SegmentUploadState::Satisfied,
        // A durable part proves the upload regardless of how the segment's own
        // status row was later advanced (`Uploaded`) or cleaned post-submit.
        SegmentStatus::Finalized | SegmentStatus::Uploaded | SegmentStatus::Cleaned if has_part => {
            SegmentUploadState::Satisfied
        }
        // Finalized but never uploaded.
        SegmentStatus::Finalized => SegmentUploadState::NeedsUpload,
        // Status claims (or implies) an upload, but the part that proves it is
        // gone — the remote filename is unrecoverable, so we cannot submit it.
        SegmentStatus::Uploaded | SegmentStatus::Cleaned => SegmentUploadState::Blocked {
            reason: format!(
                "segment {} is {:?} but has no UploadedPart",
                segment.index, segment.status
            ),
        },
        // A remote upload may be in flight; its outcome is unknown.
        SegmentStatus::Uploading => SegmentUploadState::Blocked {
            reason: format!(
                "segment {} is Uploading; upload outcome is ambiguous",
                segment.index
            ),
        },
        SegmentStatus::Recording => SegmentUploadState::Blocked {
            reason: format!("segment {} is still Recording", segment.index),
        },
        SegmentStatus::Failed => SegmentUploadState::Blocked {
            reason: format!("segment {} is Failed", segment.index),
        },
    }
}

/// A read-only verdict on a session's upload completeness: what still needs
/// uploading and what blocks submission outright.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct UploadReconciliation {
    /// Indices of `Finalized` segments with no part yet, ascending — uploadable.
    pub needs_upload: Vec<u32>,
    /// `(index, reason)` for segments that block submission and cannot be
    /// auto-resolved.
    pub blocked: Vec<(u32, String)>,
}

impl UploadReconciliation {
    /// True iff nothing is outstanding: no uploads needed and nothing blocked.
    pub fn is_ready(&self) -> bool {
        self.needs_upload.is_empty() && self.blocked.is_empty()
    }
}

/// Classify every segment of a session against the upload requirement. Pure
/// over `(segments, uploaded_indices)`; performs no IO and no mutation.
///
/// Also flags orphan uploaded parts — parts whose segment index has no
/// corresponding segment row — as blocked, since the segment that produced
/// them is missing and the operator must decide what to do.
pub fn reconcile_session_uploads(
    segments: &[Segment],
    uploaded_indices: &HashSet<u32>,
) -> UploadReconciliation {
    let segment_indices: HashSet<u32> = segments.iter().map(|s| s.index).collect();
    let mut report = UploadReconciliation::default();
    for segment in segments {
        match classify_segment_upload(segment, uploaded_indices.contains(&segment.index)) {
            SegmentUploadState::Satisfied => {}
            SegmentUploadState::NeedsUpload => report.needs_upload.push(segment.index),
            SegmentUploadState::Blocked { reason } => report.blocked.push((segment.index, reason)),
        }
    }
    for &part_index in uploaded_indices {
        if !segment_indices.contains(&part_index) {
            report.blocked.push((
                part_index,
                format!("orphan UploadedPart for segment {part_index}: no Segment row exists"),
            ));
        }
    }
    report.needs_upload.sort_unstable();
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn seg(index: u32, status: SegmentStatus) -> Segment {
        Segment {
            session_id: Uuid::new_v4(),
            index,
            path: PathBuf::from(format!("/tmp/{index}.flv")),
            status,
            close_reason: None,
            error: None,
        }
    }

    #[test]
    fn classify_uses_part_not_status_as_proof_of_upload() {
        // A durable part satisfies the requirement for any "uploaded-ish" status.
        for status in [
            SegmentStatus::Finalized,
            SegmentStatus::Uploaded,
            SegmentStatus::Cleaned,
        ] {
            assert_eq!(
                classify_segment_upload(&seg(0, status), true),
                SegmentUploadState::Satisfied,
                "{status:?} with a part must be Satisfied"
            );
        }
    }

    #[test]
    fn classify_finalized_without_part_needs_upload() {
        assert_eq!(
            classify_segment_upload(&seg(0, SegmentStatus::Finalized), false),
            SegmentUploadState::NeedsUpload
        );
    }

    #[test]
    fn classify_filtered_is_always_satisfied() {
        assert_eq!(
            classify_segment_upload(&seg(0, SegmentStatus::Filtered), false),
            SegmentUploadState::Satisfied
        );
    }

    #[test]
    fn classify_blocks_statuses_that_cannot_auto_resolve() {
        for status in [
            SegmentStatus::Uploading,
            SegmentStatus::Recording,
            SegmentStatus::Failed,
            // Claims an upload but the proving part is gone.
            SegmentStatus::Uploaded,
            SegmentStatus::Cleaned,
        ] {
            assert!(
                matches!(
                    classify_segment_upload(&seg(0, status), false),
                    SegmentUploadState::Blocked { .. }
                ),
                "{status:?} without a part must be Blocked"
            );
        }
    }

    #[test]
    fn reconcile_partitions_session_and_reports_ready() {
        let segments = vec![
            seg(0, SegmentStatus::Uploaded),  // has part -> satisfied
            seg(1, SegmentStatus::Finalized), // no part -> needs upload
            seg(2, SegmentStatus::Filtered),  // excluded -> satisfied
            seg(3, SegmentStatus::Uploading), // blocked
        ];
        let uploaded: HashSet<u32> = [0u32].into_iter().collect();

        let report = reconcile_session_uploads(&segments, &uploaded);
        assert_eq!(report.needs_upload, vec![1]);
        assert_eq!(report.blocked.len(), 1);
        assert_eq!(report.blocked[0].0, 3);
        assert!(!report.is_ready());
    }

    #[test]
    fn reconcile_is_ready_when_every_segment_satisfied() {
        let segments = vec![
            seg(0, SegmentStatus::Uploaded),
            seg(1, SegmentStatus::Filtered),
        ];
        let uploaded: HashSet<u32> = [0u32].into_iter().collect();

        assert!(reconcile_session_uploads(&segments, &uploaded).is_ready());
    }

    #[test]
    fn reconcile_blocks_orphan_uploaded_parts() {
        let segments = vec![seg(0, SegmentStatus::Uploaded)];
        // Part index 1 has no matching segment — orphan.
        let uploaded: HashSet<u32> = [0u32, 1u32].into_iter().collect();

        let report = reconcile_session_uploads(&segments, &uploaded);
        assert!(!report.is_ready());
        assert_eq!(report.blocked.len(), 1);
        assert_eq!(report.blocked[0].0, 1);
        assert!(report.blocked[0].1.contains("orphan"));
    }
}
