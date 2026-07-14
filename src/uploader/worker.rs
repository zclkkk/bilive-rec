use std::collections::HashMap;
use std::sync::Arc;

use tracing::{info, warn};
use uuid::Uuid;

use crate::error::{AppError, AppResult};
use crate::recorder::artifact_commit;
use crate::state::model::{
    ArtifactState, LiveSession, OutputPlan, RemoteAttempt, SubmissionSpec, SubmissionState,
    UploadAttemptOutcome, UploadState, UploadTargetGate, UploadedPart,
};
use crate::state::store::StateStore;
use crate::state::transitions;
use crate::uploader::types::{
    FailureScope, SubmissionOutcome, SubmissionRequest, UploadOutcome, Uploader,
};
use crate::uploader::validation::{
    reconcile_session_uploads, upload_target_is_ready, validate_ready_segment_for_upload,
};

pub use crate::state::model::UploadTarget;

const RETRY_BASE_SECONDS: u64 = 30;
const RETRY_MAX_SECONDS: u64 = 30 * 60;

pub struct UploadWorker<U: Uploader + Send + Sync + 'static> {
    store: Arc<StateStore>,
    uploaders: HashMap<UploadTarget, Arc<U>>,
    poll_interval: std::time::Duration,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    open_sessions_ready_rx: Option<tokio::sync::watch::Receiver<bool>>,
    stop_when_idle_rx: Option<tokio::sync::watch::Receiver<bool>>,
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
            open_sessions_ready_rx: None,
            stop_when_idle_rx: None,
        }
    }

    pub fn with_stop_when_idle_signal(
        mut self,
        stop_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        self.stop_when_idle_rx = Some(stop_rx);
        self
    }

    pub fn with_open_session_barrier(
        mut self,
        ready_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        self.open_sessions_ready_rx = Some(ready_rx);
        self
    }

    pub async fn run(mut self) -> AppResult<()> {
        loop {
            if *self.shutdown_rx.borrow() {
                info!("Upload worker shutting down");
                return Ok(());
            }

            self.run_once().await?;

            let should_stop_when_idle = self
                .stop_when_idle_rx
                .as_ref()
                .is_some_and(|stop| *stop.borrow());
            if should_stop_when_idle && !self.has_pending_work()? {
                info!("Upload worker finished all durable work");
                return Ok(());
            }

            tokio::select! {
                _ = tokio::time::sleep(self.poll_interval) => {}
                _ = self.shutdown_rx.changed() => {
                    info!("Upload worker shutting down");
                    return Ok(());
                }
            }
        }
    }

    pub async fn run_once(&self) -> AppResult<()> {
        self.upload_ready_segments().await?;
        if self.shutdown_requested() {
            return Ok(());
        }
        self.submit_completed_sessions().await?;
        Ok(())
    }

    async fn upload_ready_segments(&self) -> AppResult<()> {
        let mut segments = self.store.list_all_segments()?;
        segments.sort_by_key(|segment| (segment.session_id, segment.index));

        for segment in segments {
            if !matches!(segment.artifact, ArtifactState::Ready { .. })
                || !matches!(segment.upload, UploadState::Pending { .. })
            {
                continue;
            }

            let Some(session) = self.store.get_session(segment.session_id)? else {
                warn!(
                    session_id = %segment.session_id,
                    segment_index = segment.index,
                    "Ready segment has no owning session"
                );
                continue;
            };
            if !session.lifecycle.permits_upload() {
                continue;
            }
            if matches!(
                session.lifecycle,
                crate::state::model::SessionLifecycle::Open
            ) && !self.open_sessions_ready()
            {
                continue;
            }
            let Some(upload_plan) = session.output_plan.upload_plan() else {
                continue;
            };
            if self.store.get_submission(session.id)?.is_some() {
                continue;
            }

            let target = UploadTarget::from(upload_plan);
            let Some(target_state) = self.store.get_upload_target_state(&target)? else {
                warn!(
                    session_id = %session.id,
                    segment_index = segment.index,
                    "Frozen upload target has no durable gate"
                );
                continue;
            };
            let now = jiff::Timestamp::now();
            if !upload_target_is_ready(&target_state, now) || !segment.upload.is_due(now) {
                continue;
            }
            let target_failures_before = match target_state.gate {
                UploadTargetGate::Backoff { failures, .. } => failures,
                UploadTargetGate::Ready | UploadTargetGate::Blocked { .. } => 0,
            };
            let Some(uploader) = self.uploaders.get(&target) else {
                warn!(
                    session_id = %session.id,
                    segment_index = segment.index,
                    credential = %upload_plan.principal.credential.name,
                    "No uploader is configured for the frozen upload target"
                );
                continue;
            };

            let uploadable = match validate_ready_segment_for_upload(
                &self.store,
                session.id,
                segment.index,
                Some(&segment.final_path),
                now,
            )? {
                Ok(uploadable) => uploadable,
                Err(reason) => {
                    warn!(
                        session_id = %session.id,
                        segment_index = segment.index,
                        "Upload precondition failed: {reason}"
                    );
                    continue;
                }
            };

            let attempt = RemoteAttempt {
                id: Uuid::new_v4(),
                started_at: now,
            };
            if self.shutdown_requested() {
                return Ok(());
            }
            let claimed =
                transitions::begin_upload(&self.store, session.id, segment.index, attempt.clone())?;
            let outcome = uploader
                .upload_segment(uploadable.into_request(format!("Part {}", segment.index)))
                .await;
            self.persist_upload_outcome(
                &session,
                &claimed,
                attempt.id,
                target_failures_before,
                outcome,
            )?;
        }

        Ok(())
    }

    fn persist_upload_outcome(
        &self,
        session: &LiveSession,
        segment: &crate::state::model::Segment,
        attempt_id: Uuid,
        target_failures_before: u32,
        outcome: UploadOutcome,
    ) -> AppResult<()> {
        match outcome {
            UploadOutcome::Confirmed(proof) => {
                transitions::complete_upload(
                    &self.store,
                    session.id,
                    segment.index,
                    attempt_id,
                    proof.clone(),
                )?;
                info!(
                    session_id = %session.id,
                    segment_index = segment.index,
                    bili_filename = %proof.bili_filename,
                    "Uploaded finalized segment"
                );
            }
            UploadOutcome::RetryableKnownFailure(failure) => {
                let failures = match failure.scope {
                    FailureScope::Item => next_upload_failure_count(segment),
                    FailureScope::Target => target_failures_before.saturating_add(1),
                };
                let retry_at = next_retry_at(failures)?;
                match failure.scope {
                    FailureScope::Item => transitions::schedule_upload_retry(
                        &self.store,
                        session.id,
                        segment.index,
                        attempt_id,
                        failure.reason.clone(),
                        retry_at,
                    )?,
                    FailureScope::Target => transitions::schedule_upload_target_retry(
                        &self.store,
                        session.id,
                        segment.index,
                        attempt_id,
                        failure.reason.clone(),
                        retry_at,
                        failures,
                    )?,
                };
                warn!(
                    session_id = %session.id,
                    segment_index = segment.index,
                    retry_at = %retry_at,
                    "Upload failed safely and was scheduled for retry: {}",
                    failure.reason
                );
            }
            UploadOutcome::BlockedKnownFailure(failure) => {
                match failure.scope {
                    FailureScope::Item => transitions::block_upload(
                        &self.store,
                        session.id,
                        segment.index,
                        attempt_id,
                        failure.reason.clone(),
                    )?,
                    FailureScope::Target => transitions::block_upload_for_target(
                        &self.store,
                        session.id,
                        segment.index,
                        attempt_id,
                        failure.reason.clone(),
                    )?,
                };
                warn!(
                    session_id = %session.id,
                    segment_index = segment.index,
                    "Upload is blocked and requires correction: {}",
                    failure.reason
                );
            }
            UploadOutcome::Ambiguous { reason } => {
                transitions::mark_upload_ambiguous(
                    &self.store,
                    session.id,
                    segment.index,
                    attempt_id,
                    reason.clone(),
                )?;
                warn!(
                    session_id = %session.id,
                    segment_index = segment.index,
                    "Upload outcome is ambiguous and requires operator resolution: {reason}"
                );
            }
        }
        Ok(())
    }

    async fn submit_completed_sessions(&self) -> AppResult<()> {
        let mut sessions = self.store.list_sessions()?;
        sessions.sort_by_key(|session| session.started_at);

        for session in sessions {
            if !session.lifecycle.permits_submission() {
                continue;
            }
            let OutputPlan::Bilibili { upload, submission } = &session.output_plan else {
                continue;
            };

            if let Some(existing) = self.store.get_submission(session.id)? {
                match existing.state {
                    SubmissionState::Submitted { .. } => {
                        if upload.delete_after_submit {
                            if self.shutdown_requested() {
                                return Ok(());
                            }
                            self.cleanup_submitted_session(&session).await?;
                        }
                        continue;
                    }
                    SubmissionState::RetryScheduled { retry_at, .. }
                        if retry_at <= jiff::Timestamp::now() => {}
                    SubmissionState::RetryAuthorized { .. } => {}
                    SubmissionState::Attempting { .. }
                    | SubmissionState::RetryScheduled { .. }
                    | SubmissionState::Blocked { .. }
                    | SubmissionState::Ambiguous { .. } => continue,
                }
            }

            let segments = self.store.list_segments(session.id)?;
            let report = reconcile_session_uploads(&segments);
            if !report.is_ready_for_submission() {
                for (index, reason) in report.blocked {
                    warn!(
                        session_id = %session.id,
                        segment_index = index,
                        "Session blocks submission: {reason}"
                    );
                }
                continue;
            }

            let target = UploadTarget::from(upload);
            let Some(target_state) = self.store.get_upload_target_state(&target)? else {
                warn!(session_id = %session.id, "Submission target has no durable gate");
                continue;
            };
            let now = jiff::Timestamp::now();
            if !upload_target_is_ready(&target_state, now) {
                continue;
            }
            let target_failures_before = match target_state.gate {
                UploadTargetGate::Backoff { failures, .. } => failures,
                UploadTargetGate::Ready | UploadTargetGate::Blocked { .. } => 0,
            };
            let Some(uploader) = self.uploaders.get(&target) else {
                warn!(
                    session_id = %session.id,
                    credential = %upload.principal.credential.name,
                    "No uploader is configured for the frozen submission target"
                );
                continue;
            };

            let parts = report
                .uploaded_parts
                .into_iter()
                .map(|(_, proof)| proof)
                .collect();
            let request = SubmissionRequest::from_spec(submission, parts);
            let attempt = RemoteAttempt {
                id: Uuid::new_v4(),
                started_at: now,
            };
            if self.shutdown_requested() {
                return Ok(());
            }
            let claimed = transitions::begin_submission(&self.store, session.id, attempt.clone())?;
            let outcome = uploader.submit(request).await;
            self.persist_submission_outcome(
                &session,
                &claimed,
                attempt.id,
                target_failures_before,
                outcome,
            )?;

            if matches!(
                self.store
                    .get_submission(session.id)?
                    .map(|submission| submission.state),
                Some(SubmissionState::Submitted { .. })
            ) && upload.delete_after_submit
            {
                if self.shutdown_requested() {
                    return Ok(());
                }
                self.cleanup_submitted_session(&session).await?;
            }
        }

        Ok(())
    }

    fn persist_submission_outcome(
        &self,
        session: &LiveSession,
        submission: &crate::state::model::Submission,
        attempt_id: Uuid,
        target_failures_before: u32,
        outcome: SubmissionOutcome,
    ) -> AppResult<()> {
        match outcome {
            SubmissionOutcome::Confirmed { aid, bvid } => {
                transitions::complete_submission(
                    &self.store,
                    session.id,
                    attempt_id,
                    aid,
                    bvid.clone(),
                )?;
                info!(
                    session_id = %session.id,
                    aid = ?aid,
                    bvid = ?bvid,
                    "Submission confirmed"
                );
            }
            SubmissionOutcome::RetryableKnownFailure(failure) => {
                let failures = match failure.scope {
                    FailureScope::Item => next_submission_failure_count(submission),
                    FailureScope::Target => target_failures_before.saturating_add(1),
                };
                let retry_at = next_retry_at(failures)?;
                match failure.scope {
                    FailureScope::Item => transitions::schedule_submission_retry(
                        &self.store,
                        session.id,
                        attempt_id,
                        failure.reason.clone(),
                        retry_at,
                    )?,
                    FailureScope::Target => transitions::schedule_submission_target_retry(
                        &self.store,
                        session.id,
                        attempt_id,
                        failure.reason.clone(),
                        retry_at,
                        failures,
                    )?,
                };
                warn!(
                    session_id = %session.id,
                    retry_at = %retry_at,
                    "Submission failed safely and was scheduled for retry: {}",
                    failure.reason
                );
            }
            SubmissionOutcome::BlockedKnownFailure(failure) => {
                match failure.scope {
                    FailureScope::Item => transitions::block_submission(
                        &self.store,
                        session.id,
                        attempt_id,
                        failure.reason.clone(),
                    )?,
                    FailureScope::Target => transitions::block_submission_for_target(
                        &self.store,
                        session.id,
                        attempt_id,
                        failure.reason.clone(),
                    )?,
                };
                warn!(
                    session_id = %session.id,
                    "Submission is blocked and requires correction: {}",
                    failure.reason
                );
            }
            SubmissionOutcome::Ambiguous { reason } => {
                transitions::mark_submission_ambiguous(
                    &self.store,
                    session.id,
                    attempt_id,
                    reason.clone(),
                )?;
                warn!(
                    session_id = %session.id,
                    "Submission outcome is ambiguous and requires operator resolution: {reason}"
                );
            }
        }
        Ok(())
    }

    async fn cleanup_submitted_session(&self, session: &LiveSession) -> AppResult<usize> {
        let mut cleaned = 0;
        for segment in self.store.list_segments(session.id)? {
            match (&segment.artifact, &segment.upload) {
                (ArtifactState::Ready { .. }, UploadState::Uploaded { .. }) => {
                    if self.shutdown_requested() {
                        return Ok(cleaned);
                    }
                    match artifact_commit::delete(&self.store, session.id, segment.index).await {
                        Ok(()) => cleaned += 1,
                        Err(error) if !is_store_failure(&error) => {
                            warn!(
                                session_id = %session.id,
                                segment_index = segment.index,
                                "Recording deletion is incomplete and will be reconciled at startup: {error}"
                            );
                        }
                        Err(error) => return Err(error),
                    }
                }
                (
                    ArtifactState::Filtered { .. }
                    | ArtifactState::Excluded { .. }
                    | ArtifactState::Deleting
                    | ArtifactState::Deleted,
                    _,
                ) => {}
                (artifact, upload) => {
                    return Err(AppError::State(format!(
                        "segment {}/{} is artifact={artifact:?}, upload={upload:?}; refusing submitted recording cleanup",
                        session.id, segment.index
                    )));
                }
            }
        }
        if cleaned > 0 {
            info!(session_id = %session.id, cleaned, "Deleted submitted recordings");
        }
        Ok(cleaned)
    }

    fn shutdown_requested(&self) -> bool {
        *self.shutdown_rx.borrow()
    }

    fn open_sessions_ready(&self) -> bool {
        self.open_sessions_ready_rx
            .as_ref()
            .is_none_or(|ready| *ready.borrow())
    }

    fn has_pending_work(&self) -> AppResult<bool> {
        crate::uploader::work::has_executable_durable_work(&self.store, |target| {
            self.uploaders.contains_key(target)
        })
    }
}

impl SubmissionRequest {
    fn from_spec(spec: &SubmissionSpec, parts: Vec<UploadedPart>) -> Self {
        Self {
            title: spec.title.clone(),
            description: spec.description.clone(),
            category_id: spec.category_id,
            copyright: spec.copyright,
            tags: spec.tags.clone(),
            source: spec.source.clone(),
            private: spec.private,
            dynamic: spec.dynamic.clone(),
            forbid_reprint: spec.forbid_reprint,
            charging_panel: spec.charging_panel,
            close_reply: spec.close_reply,
            close_danmu: spec.close_danmu,
            featured_reply: spec.featured_reply,
            parts,
        }
    }
}

fn next_upload_failure_count(segment: &crate::state::model::Segment) -> u32 {
    segment
        .upload_attempts
        .iter()
        .filter(|attempt| {
            matches!(
                attempt.outcome,
                Some(UploadAttemptOutcome::RetryScheduled { .. })
            )
        })
        .count()
        .saturating_add(1) as u32
}

fn next_submission_failure_count(submission: &crate::state::model::Submission) -> u32 {
    submission
        .attempts
        .iter()
        .filter(|attempt| {
            matches!(
                attempt.outcome,
                Some(crate::state::model::SubmissionAttemptOutcome::RetryScheduled { .. })
            )
        })
        .count()
        .saturating_add(1) as u32
}

fn next_retry_at(failures: u32) -> AppResult<jiff::Timestamp> {
    let seconds = retry_delay_seconds(failures);
    jiff::Timestamp::now()
        .checked_add(jiff::SignedDuration::from_secs(seconds as i64))
        .map_err(|error| AppError::State(format!("failed to calculate retry timestamp: {error}")))
}

fn retry_delay_seconds(failures: u32) -> u64 {
    let exponent = failures.saturating_sub(1).min(16);
    RETRY_BASE_SECONDS
        .saturating_mul(1_u64 << exponent)
        .min(RETRY_MAX_SECONDS)
}

fn is_store_failure(error: &AppError) -> bool {
    matches!(
        error,
        AppError::Database(_)
            | AppError::Table(_)
            | AppError::Transaction(_)
            | AppError::Storage(_)
            | AppError::Commit(_)
            | AppError::State(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::config::{Copyright, SubmitApi};
    use crate::credential::CredentialRef;
    use crate::state::model::{
        RecordingPlan, Segment, SegmentCloseReason, SessionLifecycle, SubmissionAttemptOutcome,
        UploadPlan,
    };
    use crate::uploader::types::KnownFailure;
    use tempfile::TempDir;

    struct FakeUploader {
        upload_outcomes: Mutex<VecDeque<UploadOutcome>>,
        submission_outcomes: Mutex<VecDeque<SubmissionOutcome>>,
        upload_calls: AtomicUsize,
        submission_calls: AtomicUsize,
    }

    struct ShutdownAfterUpload {
        shutdown_tx: tokio::sync::watch::Sender<bool>,
        calls: AtomicUsize,
    }

    impl Uploader for ShutdownAfterUpload {
        async fn upload_segment(
            &self,
            request: crate::uploader::types::UploadRequest,
        ) -> UploadOutcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let _ = self.shutdown_tx.send(true);
            UploadOutcome::Confirmed(UploadedPart {
                bili_filename: format!("remote-{}", request.segment_index),
                part_title: request.part_title,
            })
        }

        async fn submit(&self, _request: SubmissionRequest) -> SubmissionOutcome {
            panic!("shutdown must prevent a new submission")
        }
    }

    impl FakeUploader {
        fn new(
            upload_outcomes: impl IntoIterator<Item = UploadOutcome>,
            submission_outcomes: impl IntoIterator<Item = SubmissionOutcome>,
        ) -> Self {
            Self {
                upload_outcomes: Mutex::new(upload_outcomes.into_iter().collect()),
                submission_outcomes: Mutex::new(submission_outcomes.into_iter().collect()),
                upload_calls: AtomicUsize::new(0),
                submission_calls: AtomicUsize::new(0),
            }
        }
    }

    impl Uploader for FakeUploader {
        async fn upload_segment(
            &self,
            _request: crate::uploader::types::UploadRequest,
        ) -> UploadOutcome {
            self.upload_calls.fetch_add(1, Ordering::SeqCst);
            self.upload_outcomes
                .lock()
                .unwrap()
                .pop_front()
                .expect("test upload outcome")
        }

        async fn submit(&self, _request: SubmissionRequest) -> SubmissionOutcome {
            self.submission_calls.fetch_add(1, Ordering::SeqCst);
            self.submission_outcomes
                .lock()
                .unwrap()
                .pop_front()
                .expect("test submission outcome")
        }
    }

    fn session(dir: &TempDir, delete_after_submit: bool) -> LiveSession {
        LiveSession {
            id: Uuid::new_v4(),
            room_id: 1,
            room_name: "room".into(),
            title: "live".into(),
            started_at: jiff::Timestamp::now(),
            lifecycle: SessionLifecycle::Open,
            recording_plan: RecordingPlan {
                credential: None,
                output_dir: dir.path().into(),
                segment_time_ms: None,
                segment_size: None,
                min_segment_size: 0,
                qn: 10_000,
                cdn: Vec::new(),
            },
            output_plan: OutputPlan::Bilibili {
                upload: UploadPlan {
                    principal: crate::credential::UploadPrincipal::new(
                        CredentialRef::new("main", dir.path().join("cookies.json")),
                        1,
                    ),
                    line: "auto".into(),
                    threads: 3,
                    submit_api: SubmitApi::App,
                    delete_after_submit,
                },
                submission: Box::new(SubmissionSpec {
                    title: "title".into(),
                    description: "description".into(),
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
                }),
            },
            recording_events: Vec::new(),
        }
    }

    fn ready_segment(store: &StateStore, dir: &TempDir, session_id: Uuid, index: u32) {
        let final_path = dir.path().join(format!("{index}.flv"));
        transitions::open_segment(
            store,
            Segment {
                session_id,
                index,
                part_path: dir.path().join(format!("{index}.part")),
                final_path: final_path.clone(),
                artifact: ArtifactState::Writing,
                artifact_resolutions: Vec::new(),
                upload: UploadState::NotPlanned,
                upload_attempts: Vec::new(),
                upload_resolutions: Vec::new(),
            },
        )
        .unwrap();
        transitions::begin_artifact_finalization(
            store,
            session_id,
            index,
            SegmentCloseReason::StreamEnded,
        )
        .unwrap();
        std::fs::write(final_path, b"flv").unwrap();
        transitions::complete_artifact_finalization(store, session_id, index).unwrap();
    }

    fn worker(
        store: Arc<StateStore>,
        target: UploadTarget,
        uploader: Arc<FakeUploader>,
    ) -> UploadWorker<FakeUploader> {
        let mut uploaders = HashMap::new();
        uploaders.insert(target, uploader);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        UploadWorker::new(
            store,
            uploaders,
            std::time::Duration::from_secs(60),
            shutdown_rx,
        )
    }

    #[test]
    fn retry_backoff_is_exponential_and_capped() {
        assert_eq!(retry_delay_seconds(1), 30);
        assert_eq!(retry_delay_seconds(2), 60);
        assert_eq!(retry_delay_seconds(6), 960);
        assert_eq!(retry_delay_seconds(7), 1_800);
        assert_eq!(retry_delay_seconds(u32::MAX), 1_800);
    }

    #[tokio::test]
    async fn open_session_uploads_but_submits_only_after_completion() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::create_or_open(dir.path().join("state.redb")).unwrap());
        let session = session(&dir, false);
        transitions::create_session(&store, &session).unwrap();
        ready_segment(&store, &dir, session.id, 1);
        let target = UploadTarget::from(session.output_plan.upload_plan().unwrap());
        let uploader = Arc::new(FakeUploader::new(
            [UploadOutcome::Confirmed(UploadedPart {
                bili_filename: "remote".into(),
                part_title: "Part 1".into(),
            })],
            [SubmissionOutcome::Confirmed {
                aid: Some(1),
                bvid: Some("BV1".into()),
            }],
        ));
        let worker = worker(store.clone(), target, uploader.clone());

        worker.run_once().await.unwrap();

        assert_eq!(uploader.upload_calls.load(Ordering::SeqCst), 1);
        assert_eq!(uploader.submission_calls.load(Ordering::SeqCst), 0);
        assert!(store.get_submission(session.id).unwrap().is_none());

        transitions::close_session(
            &store,
            session.id,
            transitions::CloseSessionRequest::Natural { note: None },
        )
        .unwrap();
        worker.run_once().await.unwrap();

        assert_eq!(uploader.upload_calls.load(Ordering::SeqCst), 1);
        assert_eq!(uploader.submission_calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            store.get_submission(session.id).unwrap().unwrap().state,
            SubmissionState::Submitted { .. }
        ));
    }

    #[tokio::test]
    async fn shutdown_after_one_remote_boundary_prevents_the_next_operation() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::create_or_open(dir.path().join("state.redb")).unwrap());
        let session = session(&dir, false);
        transitions::create_session(&store, &session).unwrap();
        ready_segment(&store, &dir, session.id, 1);
        ready_segment(&store, &dir, session.id, 2);
        let target = UploadTarget::from(session.output_plan.upload_plan().unwrap());
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let uploader = Arc::new(ShutdownAfterUpload {
            shutdown_tx,
            calls: AtomicUsize::new(0),
        });
        let mut uploaders = HashMap::new();
        uploaders.insert(target, uploader.clone());
        let worker = UploadWorker::new(
            store.clone(),
            uploaders,
            std::time::Duration::from_secs(60),
            shutdown_rx,
        );

        worker.run_once().await.unwrap();

        assert_eq!(uploader.calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            store.get_segment(session.id, 2).unwrap().unwrap().upload,
            UploadState::Pending { .. }
        ));
    }

    #[tokio::test]
    async fn ambiguous_upload_is_durable_and_not_polled_again() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::create_or_open(dir.path().join("state.redb")).unwrap());
        let session = session(&dir, false);
        transitions::create_session(&store, &session).unwrap();
        ready_segment(&store, &dir, session.id, 1);
        let target = UploadTarget::from(session.output_plan.upload_plan().unwrap());
        let uploader = Arc::new(FakeUploader::new(
            [UploadOutcome::Ambiguous {
                reason: "multipart completion response was lost".into(),
            }],
            [],
        ));
        let worker = worker(store.clone(), target.clone(), uploader.clone());

        worker.run_once().await.unwrap();
        worker.run_once().await.unwrap();

        assert_eq!(uploader.upload_calls.load(Ordering::SeqCst), 1);
        let segment = store.get_segment(session.id, 1).unwrap().unwrap();
        assert!(matches!(segment.upload, UploadState::Ambiguous { .. }));
        assert!(matches!(
            segment.upload_attempts[0].outcome,
            Some(UploadAttemptOutcome::Ambiguous { .. })
        ));
        assert_eq!(
            store
                .get_upload_target_state(&target)
                .unwrap()
                .unwrap()
                .gate,
            UploadTargetGate::Ready
        );
    }

    #[tokio::test]
    async fn target_failure_blocks_later_segments_without_multiplying_calls() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::create_or_open(dir.path().join("state.redb")).unwrap());
        let session = session(&dir, false);
        transitions::create_session(&store, &session).unwrap();
        ready_segment(&store, &dir, session.id, 1);
        ready_segment(&store, &dir, session.id, 2);
        let target = UploadTarget::from(session.output_plan.upload_plan().unwrap());
        let uploader = Arc::new(FakeUploader::new(
            [UploadOutcome::BlockedKnownFailure(KnownFailure {
                reason: "invalid cookie".into(),
                scope: FailureScope::Target,
            })],
            [],
        ));
        let worker = worker(store.clone(), target.clone(), uploader.clone());

        worker.run_once().await.unwrap();

        assert_eq!(uploader.upload_calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            store.get_segment(session.id, 1).unwrap().unwrap().upload,
            UploadState::Blocked { .. }
        ));
        assert!(matches!(
            store.get_segment(session.id, 2).unwrap().unwrap().upload,
            UploadState::Pending { .. }
        ));
        assert!(matches!(
            store
                .get_upload_target_state(&target)
                .unwrap()
                .unwrap()
                .gate,
            UploadTargetGate::Blocked { .. }
        ));
    }

    #[tokio::test]
    async fn safe_target_failure_persists_backoff_and_does_not_poll_retry() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::create_or_open(dir.path().join("state.redb")).unwrap());
        let session = session(&dir, false);
        transitions::create_session(&store, &session).unwrap();
        ready_segment(&store, &dir, session.id, 1);
        let target = UploadTarget::from(session.output_plan.upload_plan().unwrap());
        let uploader = Arc::new(FakeUploader::new(
            [UploadOutcome::RetryableKnownFailure(KnownFailure {
                reason: "connect failed".into(),
                scope: FailureScope::Target,
            })],
            [],
        ));
        let worker = worker(store.clone(), target.clone(), uploader.clone());

        worker.run_once().await.unwrap();
        worker.run_once().await.unwrap();

        assert_eq!(uploader.upload_calls.load(Ordering::SeqCst), 1);
        let segment = store.get_segment(session.id, 1).unwrap().unwrap();
        assert!(matches!(
            segment.upload,
            UploadState::Pending {
                failures: 1,
                retry_at: Some(_),
                ..
            }
        ));
        assert!(matches!(
            segment.upload_attempts[0].outcome,
            Some(UploadAttemptOutcome::RetryScheduled { .. })
        ));
        assert!(matches!(
            store
                .get_upload_target_state(&target)
                .unwrap()
                .unwrap()
                .gate,
            UploadTargetGate::Backoff { failures: 1, .. }
        ));
    }

    #[tokio::test]
    async fn item_retry_does_not_backoff_the_shared_target() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::create_or_open(dir.path().join("state.redb")).unwrap());
        let session = session(&dir, false);
        transitions::create_session(&store, &session).unwrap();
        ready_segment(&store, &dir, session.id, 1);
        let target = UploadTarget::from(session.output_plan.upload_plan().unwrap());
        let uploader = Arc::new(FakeUploader::new(
            [UploadOutcome::RetryableKnownFailure(KnownFailure {
                reason: "item-specific preflight failure".into(),
                scope: FailureScope::Item,
            })],
            [],
        ));
        let worker = worker(store.clone(), target.clone(), uploader.clone());

        worker.run_once().await.unwrap();
        worker.run_once().await.unwrap();

        assert_eq!(uploader.upload_calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            store.get_segment(session.id, 1).unwrap().unwrap().upload,
            UploadState::Pending {
                failures: 1,
                retry_at: Some(_),
                ..
            }
        ));
        assert_eq!(
            store
                .get_upload_target_state(&target)
                .unwrap()
                .unwrap()
                .gate,
            UploadTargetGate::Ready
        );
    }

    #[tokio::test]
    async fn completed_session_submits_and_deletes_through_artifact_protocol() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::create_or_open(dir.path().join("state.redb")).unwrap());
        let session = session(&dir, true);
        transitions::create_session(&store, &session).unwrap();
        ready_segment(&store, &dir, session.id, 1);
        transitions::close_session(
            &store,
            session.id,
            transitions::CloseSessionRequest::Natural { note: None },
        )
        .unwrap();
        let target = UploadTarget::from(session.output_plan.upload_plan().unwrap());
        let uploader = Arc::new(FakeUploader::new(
            [UploadOutcome::Confirmed(UploadedPart {
                bili_filename: "remote".into(),
                part_title: "Part 1".into(),
            })],
            [SubmissionOutcome::Confirmed {
                aid: Some(1),
                bvid: Some("BV1".into()),
            }],
        ));
        let worker = worker(store.clone(), target, uploader.clone());

        worker.run_once().await.unwrap();

        assert_eq!(uploader.upload_calls.load(Ordering::SeqCst), 1);
        assert_eq!(uploader.submission_calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            store.get_segment(session.id, 1).unwrap().unwrap().artifact,
            ArtifactState::Deleted
        ));
        let submission = store.get_submission(session.id).unwrap().unwrap();
        assert!(matches!(
            submission.state,
            SubmissionState::Submitted { .. }
        ));
        assert!(matches!(
            submission.attempts[0].outcome,
            Some(SubmissionAttemptOutcome::Submitted { .. })
        ));
        assert!(!dir.path().join("1.flv").exists());
    }

    #[tokio::test]
    async fn submitted_cleanup_ignores_another_sessions_blocked_target() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::create_or_open(dir.path().join("state.redb")).unwrap());

        let submitted = session(&dir, true);
        transitions::create_session(&store, &submitted).unwrap();
        ready_segment(&store, &dir, submitted.id, 1);
        transitions::close_session(
            &store,
            submitted.id,
            transitions::CloseSessionRequest::Natural { note: None },
        )
        .unwrap();
        let upload_attempt = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: jiff::Timestamp::now(),
        };
        transitions::begin_upload(&store, submitted.id, 1, upload_attempt.clone()).unwrap();
        transitions::complete_upload(
            &store,
            submitted.id,
            1,
            upload_attempt.id,
            UploadedPart {
                bili_filename: "remote-submitted".into(),
                part_title: "Part 1".into(),
            },
        )
        .unwrap();
        let submission_attempt = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: jiff::Timestamp::now(),
        };
        transitions::begin_submission(&store, submitted.id, submission_attempt.clone()).unwrap();
        transitions::complete_submission(
            &store,
            submitted.id,
            submission_attempt.id,
            Some(1),
            None,
        )
        .unwrap();

        let blocked_dir = TempDir::new().unwrap();
        let mut blocked = session(&blocked_dir, false);
        blocked.room_id = 2;
        let submitted_principal = submitted
            .output_plan
            .upload_plan()
            .unwrap()
            .principal
            .clone();
        let OutputPlan::Bilibili { upload, .. } = &mut blocked.output_plan else {
            unreachable!();
        };
        upload.principal = submitted_principal;
        transitions::create_session(&store, &blocked).unwrap();
        ready_segment(&store, &blocked_dir, blocked.id, 1);
        let blocked_attempt = RemoteAttempt {
            id: Uuid::new_v4(),
            started_at: jiff::Timestamp::now(),
        };
        transitions::begin_upload(&store, blocked.id, 1, blocked_attempt.clone()).unwrap();
        transitions::block_upload_for_target(
            &store,
            blocked.id,
            1,
            blocked_attempt.id,
            "credential rejected".into(),
        )
        .unwrap();

        let target = UploadTarget::from(submitted.output_plan.upload_plan().unwrap());
        assert!(matches!(
            store
                .get_upload_target_state(&target)
                .unwrap()
                .unwrap()
                .gate,
            UploadTargetGate::Blocked { .. }
        ));
        let uploader = Arc::new(FakeUploader::new([], []));
        let worker = worker(store.clone(), target, uploader.clone());

        assert!(worker.has_pending_work().unwrap());
        worker.run_once().await.unwrap();

        assert_eq!(uploader.upload_calls.load(Ordering::SeqCst), 0);
        assert_eq!(uploader.submission_calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            store
                .get_segment(submitted.id, 1)
                .unwrap()
                .unwrap()
                .artifact,
            ArtifactState::Deleted
        ));
    }
}
