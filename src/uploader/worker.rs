use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::SubmitApi;
use crate::credential::CredentialIdentity;
use crate::error::{AppError, AppResult};
use crate::state::model::{
    SegmentStatus, SessionStatus, Submission, SubmissionPlan, SubmissionStatus,
};
use crate::state::store::StateStore;
use crate::uploader::types::{SubmissionOutcome, SubmissionRequest, Uploader};
use crate::uploader::validation::{
    PersistedUploadFailure, reconcile_session_uploads, upload_and_persist_segment,
    validate_finalized_segment_for_upload,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UploadTarget {
    pub credential: CredentialIdentity,
    pub submit_api: SubmitApi,
}

impl UploadTarget {
    pub fn new(credential: CredentialIdentity, submit_api: SubmitApi) -> Self {
        Self {
            credential,
            submit_api,
        }
    }
}

pub struct UploadWorker<U: Uploader + Send + Sync + 'static> {
    store: Arc<StateStore>,
    uploaders: HashMap<UploadTarget, Arc<U>>,
    poll_interval: std::time::Duration,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl<U: Uploader + Send + Sync + 'static> UploadWorker<U> {
    pub fn new(
        store: Arc<StateStore>,
        uploaders: HashMap<UploadTarget, Arc<U>>,
        poll_interval: std::time::Duration,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self {
            store,
            uploaders,
            poll_interval,
            shutdown_rx,
        }
    }

    pub async fn run(mut self) -> AppResult<()> {
        loop {
            if *self.shutdown_rx.borrow() {
                tracing::info!("Upload worker shutting down");
                return Ok(());
            }

            self.run_once().await?;

            tokio::select! {
                _ = tokio::time::sleep(self.poll_interval) => {}
                _ = self.shutdown_rx.changed() => {
                    tracing::info!("Upload worker shutting down");
                    return Ok(());
                }
            }
        }
    }

    pub async fn run_once(&self) -> AppResult<()> {
        self.upload_finalized_segments().await?;
        self.submit_finalized_sessions().await?;
        Ok(())
    }

    async fn upload_finalized_segments(&self) -> AppResult<()> {
        let uploaded: HashSet<(Uuid, u32)> = self
            .store
            .list_all_uploaded_parts()?
            .into_iter()
            .map(|part| (part.session_id, part.segment_index))
            .collect();

        let mut segments = self.store.list_all_segments()?;
        segments.sort_by_key(|segment| (segment.session_id, segment.index));

        for segment in segments {
            if segment.status != SegmentStatus::Finalized {
                continue;
            }
            if uploaded.contains(&(segment.session_id, segment.index)) {
                continue;
            }
            if self.store.get_submission(segment.session_id)?.is_some() {
                continue;
            }

            let Some(plan) = self.store.get_submission_plan(segment.session_id)? else {
                warn!(
                    session_id = %segment.session_id,
                    segment_index = segment.index,
                    "Finalized segment has no submission plan; upload worker cannot choose uploader"
                );
                continue;
            };
            let target = UploadTarget::new(plan.upload_credential.clone(), plan.submit_api.clone());
            let Some(uploader) = self.uploaders.get(&target) else {
                warn!(
                    session_id = %segment.session_id,
                    credential = %plan.upload_credential.name,
                    submit_api = %plan.submit_api.as_config_value(),
                    "No uploader configured for persisted submission plan"
                );
                continue;
            };

            match validate_finalized_segment_for_upload(
                &self.store,
                segment.session_id,
                segment.index,
                Some(&segment.path),
            )? {
                Ok(uploadable) => match upload_and_persist_segment(
                    uploader.as_ref(),
                    &self.store,
                    uploadable,
                    format!("Part {}", segment.index),
                )
                .await
                {
                    Ok(part) => {
                        info!(
                            session_id = %part.session_id,
                            segment_index = part.segment_index,
                            bili_filename = %part.bili_filename,
                            "Uploaded finalized segment"
                        );
                    }
                    Err(PersistedUploadFailure::Remote { index, error }) => {
                        warn!(
                            session_id = %segment.session_id,
                            segment_index = index,
                            "Remote upload failed; segment was left Finalized for retry: {}",
                            error
                        );
                    }
                    Err(PersistedUploadFailure::StateBeforeRemote { index, error }) => {
                        return Err(AppError::State(format!(
                            "Failed to persist pre-upload state for segment {}/{}: {}",
                            segment.session_id, index, error
                        )));
                    }
                    Err(PersistedUploadFailure::StateAfterRemote { index, error }) => {
                        return Err(AppError::State(format!(
                            "Remote upload for segment {}/{} may have succeeded, but UploadedPart persistence failed: {}. Refusing automatic reconciliation.",
                            segment.session_id, index, error
                        )));
                    }
                },
                Err(reason) => {
                    warn!(
                        session_id = %segment.session_id,
                        segment_index = segment.index,
                        "Upload precondition failed: {}",
                        reason
                    );
                }
            }
        }

        Ok(())
    }

    async fn submit_finalized_sessions(&self) -> AppResult<()> {
        let mut sessions = self.store.list_all_sessions()?;
        sessions.sort_by_key(|session| session.started_at);

        for session in sessions {
            if session.status != SessionStatus::Finalized {
                continue;
            }

            let Some(plan) = self.store.get_submission_plan(session.id)? else {
                warn!(
                    session_id = %session.id,
                    "Finalized session has no submission plan; upload worker cannot submit"
                );
                continue;
            };

            if self.handle_existing_submission(&plan).await? {
                continue;
            }

            let uploaded_indices: HashSet<u32> = self
                .store
                .list_uploaded_parts(session.id)?
                .into_iter()
                .map(|part| part.segment_index)
                .collect();
            let segments = self.store.list_segments(session.id)?;
            let report = reconcile_session_uploads(&segments, &uploaded_indices);
            if !report.is_ready() {
                for (index, reason) in report.blocked {
                    warn!(
                        session_id = %session.id,
                        segment_index = index,
                        "Session blocks submission: {}",
                        reason
                    );
                }
                continue;
            }

            let target = UploadTarget::new(plan.upload_credential.clone(), plan.submit_api.clone());
            let Some(uploader) = self.uploaders.get(&target) else {
                warn!(
                    session_id = %session.id,
                    credential = %plan.upload_credential.name,
                    submit_api = %plan.submit_api.as_config_value(),
                    "No uploader configured for persisted submission plan"
                );
                continue;
            };

            let mut parts = self.store.list_uploaded_parts(session.id)?;
            parts.sort_by_key(|part| part.segment_index);
            if parts.is_empty() {
                let sub = Submission {
                    session_id: session.id,
                    upload_credential: plan.upload_credential.clone(),
                    status: SubmissionStatus::Failed,
                    aid: None,
                    bvid: None,
                    error: Some("No parts to submit".into()),
                };
                self.store.put_submission(&sub)?;
                continue;
            }

            let req = SubmissionRequest::from_plan(&plan, parts);
            let mut sub = Submission {
                session_id: session.id,
                upload_credential: plan.upload_credential.clone(),
                status: SubmissionStatus::Pending,
                aid: None,
                bvid: None,
                error: None,
            };
            if !self.store.begin_submission(&sub)? {
                continue;
            }

            match uploader.submit(req).await {
                Ok(SubmissionOutcome::Confirmed { aid, bvid }) => {
                    sub.status = SubmissionStatus::Submitted;
                    sub.aid = aid;
                    sub.bvid = bvid;
                    self.store.put_submission(&sub)?;
                    info!(
                        session_id = %session.id,
                        credential = %plan.upload_credential.name,
                        submit_api = %plan.submit_api.as_config_value(),
                        aid = ?sub.aid,
                        bvid = ?sub.bvid,
                        "Submission confirmed"
                    );
                    if plan.delete_after_submit {
                        let cleaned =
                            cleanup_submitted_session_recordings(&self.store, session.id).await?;
                        if cleaned > 0 {
                            info!(
                                session_id = %session.id,
                                credential = %plan.upload_credential.name,
                                cleaned,
                                "Deleted local recordings after confirmed submission"
                            );
                        }
                    }
                }
                Ok(SubmissionOutcome::Ambiguous { reason }) => {
                    warn!(
                        session_id = %session.id,
                        credential = %plan.upload_credential.name,
                        submit_api = %plan.submit_api.as_config_value(),
                        "Submission outcome is ambiguous: {}",
                        reason
                    );
                    sub.status = SubmissionStatus::Ambiguous;
                    sub.error = Some(reason);
                    self.store.put_submission(&sub)?;
                }
                Err(error) => {
                    error!(
                        session_id = %session.id,
                        credential = %plan.upload_credential.name,
                        submit_api = %plan.submit_api.as_config_value(),
                        "Submission failed: {}",
                        error
                    );
                    sub.status = SubmissionStatus::Failed;
                    sub.error = Some(error.to_string());
                    self.store.put_submission(&sub)?;
                }
            }
        }

        Ok(())
    }

    async fn handle_existing_submission(&self, plan: &SubmissionPlan) -> AppResult<bool> {
        let Some(existing) = self.store.get_submission(plan.session_id)? else {
            return Ok(false);
        };

        match existing.status {
            SubmissionStatus::Submitted => {
                if plan.delete_after_submit {
                    cleanup_submitted_session_recordings(&self.store, plan.session_id).await?;
                }
            }
            SubmissionStatus::Pending | SubmissionStatus::Ambiguous | SubmissionStatus::Failed => {}
        }

        Ok(true)
    }
}

impl SubmissionRequest {
    fn from_plan(plan: &SubmissionPlan, parts: Vec<crate::state::model::UploadedPart>) -> Self {
        Self {
            title: plan.title.clone(),
            description: plan.description.clone(),
            category_id: plan.category_id,
            copyright: plan.copyright,
            tags: plan.tags.clone(),
            source: plan.source.clone(),
            private: plan.private,
            dynamic: plan.dynamic.clone(),
            forbid_reprint: plan.forbid_reprint,
            charging_panel: plan.charging_panel,
            close_reply: plan.close_reply,
            close_danmu: plan.close_danmu,
            featured_reply: plan.featured_reply,
            parts,
        }
    }
}

async fn cleanup_submitted_session_recordings(
    store: &StateStore,
    session_id: Uuid,
) -> AppResult<usize> {
    let session = store
        .get_session(session_id)?
        .ok_or_else(|| AppError::State(format!("Session {session_id} not found")))?;
    if session.status != SessionStatus::Finalized {
        return Err(AppError::State(format!(
            "Session {session_id} is {:?}, not Finalized; refusing recording cleanup",
            session.status
        )));
    }

    let submission = store
        .get_submission(session_id)?
        .ok_or_else(|| AppError::State(format!("Session {session_id} has no submission")))?;
    if submission.status != SubmissionStatus::Submitted {
        return Err(AppError::State(format!(
            "Session {session_id} submission is {:?}, not Submitted; refusing recording cleanup",
            submission.status
        )));
    }

    let uploaded_indices: HashSet<u32> = store
        .list_uploaded_parts(session_id)?
        .into_iter()
        .map(|part| part.segment_index)
        .collect();
    let segments = store.list_segments(session_id)?;
    let mut cleaned = 0;

    for segment in segments {
        match segment.status {
            SegmentStatus::Cleaned | SegmentStatus::Filtered => {}
            SegmentStatus::Uploaded | SegmentStatus::Finalized => {
                if !uploaded_indices.contains(&segment.index) {
                    return Err(AppError::State(format!(
                        "Segment {}/{} is {:?} but has no UploadedPart; refusing recording cleanup",
                        segment.session_id, segment.index, segment.status
                    )));
                }

                match tokio::fs::metadata(&segment.path).await {
                    Ok(metadata) => {
                        if !metadata.is_file() {
                            return Err(AppError::State(format!(
                                "Segment {}/{} path is not a regular file: {}",
                                segment.session_id,
                                segment.index,
                                segment.path.display()
                            )));
                        }
                        match tokio::fs::remove_file(&segment.path).await {
                            Ok(()) => {}
                            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                            Err(source) => {
                                return Err(AppError::Io {
                                    path: segment.path.clone(),
                                    source,
                                });
                            }
                        }
                    }
                    Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                    Err(source) => {
                        return Err(AppError::Io {
                            path: segment.path.clone(),
                            source,
                        });
                    }
                }

                let mut cleaned_segment = segment;
                cleaned_segment.status = SegmentStatus::Cleaned;
                cleaned_segment.error = None;
                store.put_segment(&cleaned_segment)?;
                cleaned += 1;
            }
            SegmentStatus::Recording | SegmentStatus::Uploading | SegmentStatus::Failed => {
                return Err(AppError::State(format!(
                    "Segment {}/{} is {:?}; refusing recording cleanup",
                    segment.session_id, segment.index, segment.status
                )));
            }
        }
    }

    Ok(cleaned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Copyright, SubmitApi};
    use crate::state::model::{LiveSession, Segment, UploadedPart};
    use crate::uploader::types::UploadRequest;
    use std::sync::Mutex;
    use tempfile::TempDir;

    #[derive(Default)]
    struct FakeUploader {
        uploaded: Mutex<Vec<UploadRequest>>,
        submitted: Mutex<Vec<SubmissionRequest>>,
    }

    impl Uploader for FakeUploader {
        async fn check_login(&self) -> AppResult<()> {
            Ok(())
        }

        async fn upload_segment(&self, req: UploadRequest) -> AppResult<UploadedPart> {
            self.uploaded.lock().unwrap().push(req.clone());
            Ok(UploadedPart {
                session_id: req.session_id,
                segment_index: req.segment_index,
                bili_filename: format!("remote-{}", req.segment_index),
                part_title: req.part_title,
            })
        }

        async fn submit(&self, req: SubmissionRequest) -> AppResult<SubmissionOutcome> {
            self.submitted.lock().unwrap().push(req);
            Ok(SubmissionOutcome::Confirmed {
                aid: Some(1),
                bvid: Some("BV1".into()),
            })
        }
    }

    fn credential(dir: &std::path::Path) -> CredentialIdentity {
        CredentialIdentity::new("main", dir.join("cookies.json"))
    }

    fn plan(session_id: Uuid, credential: CredentialIdentity) -> SubmissionPlan {
        SubmissionPlan {
            session_id,
            upload_credential: credential,
            submit_api: SubmitApi::App,
            title: "title".into(),
            description: "desc".into(),
            category_id: 171,
            copyright: Copyright::Reprint,
            source: "source".into(),
            tags: vec!["tag".into()],
            private: false,
            dynamic: String::new(),
            forbid_reprint: false,
            charging_panel: false,
            close_reply: false,
            close_danmu: false,
            featured_reply: false,
            delete_after_submit: false,
        }
    }

    fn session(session_id: Uuid) -> LiveSession {
        LiveSession {
            id: session_id,
            room_key: "1".into(),
            title: "live".into(),
            started_at: jiff::Timestamp::now(),
            status: SessionStatus::Finalized,
            record_credential: None,
            upload_credential: None,
        }
    }

    #[tokio::test]
    async fn worker_uploads_finalized_segment_before_session_finalized_submit() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::open(dir.path().join("state.redb")).unwrap());
        let session_id = Uuid::new_v4();
        let credential = credential(dir.path());
        let mut live_session = session(session_id);
        live_session.status = SessionStatus::Recording;
        store.put_session(&live_session).unwrap();
        store
            .put_submission_plan(&plan(session_id, credential.clone()))
            .unwrap();
        let path = dir.path().join("0.flv");
        std::fs::write(&path, b"flv").unwrap();
        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path,
                status: SegmentStatus::Finalized,
                close_reason: None,
                error: None,
            })
            .unwrap();

        let mut uploaders = HashMap::new();
        let uploader = Arc::new(FakeUploader::default());
        uploaders.insert(
            UploadTarget::new(credential, SubmitApi::App),
            uploader.clone(),
        );
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let worker = UploadWorker::new(
            store.clone(),
            uploaders,
            std::time::Duration::from_secs(60),
            rx,
        );

        worker.run_once().await.unwrap();

        assert_eq!(uploader.uploaded.lock().unwrap().len(), 1);
        assert!(store.get_submission(session_id).unwrap().is_none());
        assert_eq!(
            store.get_segment(session_id, 0).unwrap().unwrap().status,
            SegmentStatus::Uploaded
        );
    }

    #[tokio::test]
    async fn worker_requires_matching_submit_api_for_persisted_plan() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::open(dir.path().join("state.redb")).unwrap());
        let session_id = Uuid::new_v4();
        let credential = credential(dir.path());
        let mut submission_plan = plan(session_id, credential.clone());
        submission_plan.submit_api = SubmitApi::Web;
        let mut live_session = session(session_id);
        live_session.status = SessionStatus::Recording;
        store.put_session(&live_session).unwrap();
        store.put_submission_plan(&submission_plan).unwrap();
        let path = dir.path().join("0.flv");
        std::fs::write(&path, b"flv").unwrap();
        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path,
                status: SegmentStatus::Finalized,
                close_reason: None,
                error: None,
            })
            .unwrap();

        let mut uploaders = HashMap::new();
        let uploader = Arc::new(FakeUploader::default());
        uploaders.insert(
            UploadTarget::new(credential, SubmitApi::App),
            uploader.clone(),
        );
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let worker = UploadWorker::new(
            store.clone(),
            uploaders,
            std::time::Duration::from_secs(60),
            rx,
        );

        worker.run_once().await.unwrap();

        assert!(uploader.uploaded.lock().unwrap().is_empty());
        assert!(store.list_uploaded_parts(session_id).unwrap().is_empty());
        assert_eq!(
            store.get_segment(session_id, 0).unwrap().unwrap().status,
            SegmentStatus::Finalized
        );
    }

    #[tokio::test]
    async fn worker_submits_finalized_session_after_parts_are_ready() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::open(dir.path().join("state.redb")).unwrap());
        let session_id = Uuid::new_v4();
        let credential = credential(dir.path());
        store.put_session(&session(session_id)).unwrap();
        store
            .put_submission_plan(&plan(session_id, credential.clone()))
            .unwrap();
        let path = dir.path().join("0.flv");
        std::fs::write(&path, b"flv").unwrap();
        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path,
                status: SegmentStatus::Uploaded,
                close_reason: None,
                error: None,
            })
            .unwrap();
        store
            .put_uploaded_part(&UploadedPart {
                session_id,
                segment_index: 0,
                bili_filename: "remote".into(),
                part_title: "Part 0".into(),
            })
            .unwrap();

        let mut uploaders = HashMap::new();
        let uploader = Arc::new(FakeUploader::default());
        uploaders.insert(
            UploadTarget::new(credential, SubmitApi::App),
            uploader.clone(),
        );
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let worker = UploadWorker::new(
            store.clone(),
            uploaders,
            std::time::Duration::from_secs(60),
            rx,
        );

        worker.run_once().await.unwrap();

        assert_eq!(uploader.submitted.lock().unwrap().len(), 1);
        assert_eq!(
            store.get_submission(session_id).unwrap().unwrap().status,
            SubmissionStatus::Submitted
        );
    }
}
