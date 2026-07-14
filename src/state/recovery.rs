//! Explicit operator recovery commands.
//!
//! Deterministic filesystem reconciliation lives with the artifact commit
//! protocol in `recorder`. This module records only decisions that require
//! knowledge supplied by an operator; all writes still pass through the domain
//! transitions.

use uuid::Uuid;

use crate::error::AppResult;
use crate::state::model::{Segment, Submission, UploadedPart};
use crate::state::store::StateStore;
use crate::state::transitions::{
    CloseSessionRequest, CloseSessionResult, close_session, resolve_submission_not_submitted,
    resolve_submission_submitted, resolve_upload_not_uploaded, resolve_upload_uploaded,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordingResolutionTarget {
    Finalize { exclude_failed: bool },
    Abandon,
}

pub fn resolve_recording(
    store: &StateStore,
    session_id: Uuid,
    target: RecordingResolutionTarget,
    note: Option<String>,
) -> AppResult<CloseSessionResult> {
    let request = match target {
        RecordingResolutionTarget::Finalize { exclude_failed } => CloseSessionRequest::Recover {
            exclude_failed,
            note,
        },
        RecordingResolutionTarget::Abandon => CloseSessionRequest::Abandon { note },
    };
    close_session(store, session_id, request)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadResolutionTarget {
    NotUploaded,
    Uploaded { proof: UploadedPart },
}

pub fn resolve_upload(
    store: &StateStore,
    session_id: Uuid,
    segment_index: u32,
    target: UploadResolutionTarget,
    note: Option<String>,
) -> AppResult<Segment> {
    match target {
        UploadResolutionTarget::NotUploaded => {
            resolve_upload_not_uploaded(store, session_id, segment_index, note)
        }
        UploadResolutionTarget::Uploaded { proof } => {
            resolve_upload_uploaded(store, session_id, segment_index, proof, note)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmissionResolutionTarget {
    NotSubmitted,
    Submitted {
        aid: Option<u64>,
        bvid: Option<String>,
    },
}

pub fn resolve_submission(
    store: &StateStore,
    session_id: Uuid,
    target: SubmissionResolutionTarget,
    note: Option<String>,
) -> AppResult<Submission> {
    match target {
        SubmissionResolutionTarget::NotSubmitted => {
            resolve_submission_not_submitted(store, session_id, note)
        }
        SubmissionResolutionTarget::Submitted { aid, bvid } => {
            resolve_submission_submitted(store, session_id, aid, bvid, note)
        }
    }
}
