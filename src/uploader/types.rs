use crate::config::Copyright;
use crate::state::model::UploadedPart;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct UploadRequest {
    pub session_id: Uuid,
    pub segment_index: u32,
    pub path: PathBuf,
    pub part_title: String,
}

#[derive(Debug, Clone)]
pub struct SubmissionRequest {
    pub title: String,
    pub description: String,
    pub category_id: u16,
    pub copyright: Copyright,
    pub tags: Vec<String>,
    pub source: String,
    pub private: bool,
    pub dynamic: String,
    pub forbid_reprint: bool,
    pub charging_panel: bool,
    pub close_reply: bool,
    pub close_danmu: bool,
    pub featured_reply: bool,
    pub parts: Vec<UploadedPart>,
}

/// Whether a known failure only applies to one artifact/submission or to the
/// shared credential/upload target. Workers use this to avoid multiplying a
/// broken target across every pending segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureScope {
    Item,
    Target,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownFailure {
    pub reason: String,
    pub scope: FailureScope,
}

/// Result of crossing the Bilibili submission boundary.
///
/// `RetryableKnownFailure` is safe to retry automatically because the request
/// is known not to have reached an accepting endpoint. `BlockedKnownFailure`
/// is also a known outcome, but requires an external/configuration correction.
/// Once Bilibili may have accepted the request, failures must be `Ambiguous`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmissionOutcome {
    Confirmed {
        aid: Option<u64>,
        bvid: Option<String>,
    },
    Ambiguous {
        reason: String,
    },
    RetryableKnownFailure(KnownFailure),
    BlockedKnownFailure(KnownFailure),
}

/// Result of crossing the Bilibili upload boundary, with the same known versus
/// ambiguous distinction as [`SubmissionOutcome`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadOutcome {
    Confirmed(UploadedPart),
    RetryableKnownFailure(KnownFailure),
    BlockedKnownFailure(KnownFailure),
    Ambiguous { reason: String },
}

pub trait Uploader: Send + Sync {
    fn upload_segment(
        &self,
        req: UploadRequest,
    ) -> impl std::future::Future<Output = UploadOutcome> + Send;

    fn submit(
        &self,
        req: SubmissionRequest,
    ) -> impl std::future::Future<Output = SubmissionOutcome> + Send;
}
