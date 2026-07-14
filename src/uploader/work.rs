use uuid::Uuid;

use crate::error::AppResult;
use crate::state::model::{
    ArtifactState, OutputPlan, SubmissionState, UploadState, UploadTarget, UploadTargetGate,
};
use crate::state::store::StateStore;
use crate::uploader::validation::reconcile_session_uploads;

/// Durable operations that remain meaningful without any current room task.
///
/// Cleanup is intentionally not associated with an upload target: once a
/// submission is confirmed, deleting its local recording is executable even
/// when another session has blocked the shared remote target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableWork {
    Cleanup {
        session_id: Uuid,
    },
    Upload {
        session_id: Uuid,
        segment_index: u32,
        target: UploadTarget,
    },
    Submission {
        session_id: Uuid,
        target: UploadTarget,
    },
}

pub fn pending_durable_work(store: &StateStore) -> AppResult<Vec<DurableWork>> {
    let mut work = Vec::new();

    for segment in store.list_all_segments()? {
        if !matches!(segment.artifact, ArtifactState::Ready { .. })
            || !matches!(segment.upload, UploadState::Pending { .. })
        {
            continue;
        }
        let Some(session) = store.get_session(segment.session_id)? else {
            continue;
        };
        if !session.lifecycle.permits_upload() || store.get_submission(session.id)?.is_some() {
            continue;
        }
        let Some(plan) = session.output_plan.upload_plan() else {
            continue;
        };
        work.push(DurableWork::Upload {
            session_id: session.id,
            segment_index: segment.index,
            target: UploadTarget::from(plan),
        });
    }

    for session in store.list_sessions()? {
        if !session.lifecycle.permits_submission() {
            continue;
        }
        let OutputPlan::Bilibili { upload, .. } = &session.output_plan else {
            continue;
        };
        let target = UploadTarget::from(upload);
        match store.get_submission(session.id)? {
            None => {
                if reconcile_session_uploads(&store.list_segments(session.id)?)
                    .is_ready_for_submission()
                {
                    work.push(DurableWork::Submission {
                        session_id: session.id,
                        target,
                    });
                }
            }
            Some(submission)
                if matches!(
                    submission.state,
                    SubmissionState::RetryScheduled { .. }
                        | SubmissionState::RetryAuthorized { .. }
                ) =>
            {
                work.push(DurableWork::Submission {
                    session_id: session.id,
                    target,
                });
            }
            Some(submission)
                if matches!(submission.state, SubmissionState::Submitted { .. })
                    && upload.delete_after_submit
                    && store.list_segments(session.id)?.iter().any(|segment| {
                        matches!(segment.artifact, ArtifactState::Ready { .. })
                            && matches!(segment.upload, UploadState::Uploaded { .. })
                    }) =>
            {
                work.push(DurableWork::Cleanup {
                    session_id: session.id,
                });
            }
            _ => {}
        }
    }

    Ok(work)
}

pub fn has_executable_durable_work(
    store: &StateStore,
    has_uploader: impl Fn(&UploadTarget) -> bool,
) -> AppResult<bool> {
    for work in pending_durable_work(store)? {
        let target = match &work {
            DurableWork::Cleanup { .. } => return Ok(true),
            DurableWork::Upload { target, .. } | DurableWork::Submission { target, .. } => target,
        };
        if has_uploader(target)
            && store
                .get_upload_target_state(target)?
                .is_some_and(|state| !matches!(state.gate, UploadTargetGate::Blocked { .. }))
        {
            return Ok(true);
        }
    }
    Ok(false)
}
