use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::error::AppResult;
use crate::state::model::{SegmentStatus, SubmissionStatus, UploadedPart};
use crate::state::store::StateStore;
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
    set_segment_status(store, &segment, SegmentStatus::Uploading, None).map_err(|error| {
        PersistedUploadFailure::StateBeforeRemote {
            index,
            error: error.to_string(),
        }
    })?;

    let req = segment.clone().into_request(part_title);
    let uploaded_part = uploader.upload_segment(req).await.map_err(|error| {
        let reset_result = set_segment_status(store, &segment, SegmentStatus::Finalized, None);
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

    store.put_uploaded_part(&uploaded_part).map_err(|error| {
        PersistedUploadFailure::StateAfterRemote {
            index,
            error: format!(
                "remote filename {}, persistence error: {}",
                uploaded_part.bili_filename, error
            ),
        }
    })?;

    set_segment_status(store, &segment, SegmentStatus::Uploaded, None).map_err(|error| {
        PersistedUploadFailure::StateAfterRemote {
            index,
            error: format!(
                "remote filename {} persisted, but segment status update failed: {}",
                uploaded_part.bili_filename, error
            ),
        }
    })?;

    Ok(uploaded_part)
}

fn set_segment_status(
    store: &StateStore,
    segment: &FinalizedSegmentForUpload,
    status: SegmentStatus,
    error: Option<String>,
) -> AppResult<()> {
    let segments = store.list_segments(segment.session_id)?;
    let mut stored = segments
        .into_iter()
        .find(|stored| stored.index == segment.segment_index)
        .ok_or_else(|| {
            crate::error::AppError::State(format!(
                "segment {}/{} not found",
                segment.session_id, segment.segment_index
            ))
        })?;
    stored.status = status;
    stored.error = error;
    store.put_segment(&stored)
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
            SubmissionStatus::Failed => "Failed",
        };
        return Ok(Err(format!(
            "session has a {status} submission — refusing upload"
        )));
    }

    let segments = store.list_segments(session_id)?;
    let Some(segment) = segments
        .iter()
        .find(|segment| segment.index == segment_index)
    else {
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
