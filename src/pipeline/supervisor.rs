use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::bilibili::client::BiliClient;
use crate::bilibili::room::fetch_room_info;
use crate::bilibili::stream::{
    fetch_play_info, parse_stream_candidates, select_healthy_stream_candidate,
};
use crate::bilibili::types::LiveStatus;
use crate::config::{PipelineConfig, ResolvedRoomConfig, RoomCredentials};
use crate::error::{AppError, AppResult};
use crate::pipeline::session::PipelineSession;
use crate::pipeline::state_machine::PipelineState;
use crate::recorder::segment::{RecorderPolicy, SegmentFilter, SegmentLayout, SegmentPolicy};
use crate::recorder::{record_flv, segment::SegmentEvent};
use crate::state::model::{
    LiveSession, SegmentStatus, SessionStatus, Submission, SubmissionStatus,
};
use crate::state::store::StateStore;
use crate::submission_template::render_room_template;
use crate::uploader::types::{SubmissionOutcome, SubmissionRequest, Uploader};
use crate::uploader::validation::{
    PersistedUploadFailure, upload_and_persist_segment, validate_finalized_segment_for_upload,
};

#[derive(Debug)]
enum BackgroundUploadFailure {
    Reconcileable { index: u32, error: String },
    FatalState { index: u32, error: String },
    Ambiguous { index: u32, error: String },
}

type BackgroundUploadResult = Result<(), BackgroundUploadFailure>;

pub struct RoomSupervisorDeps<U: Uploader + Send + Sync + 'static> {
    pub store: Arc<StateStore>,
    pub client: Arc<BiliClient>,
    pub uploader: Arc<U>,
}

fn pipeline_state_requires_active_session(state: PipelineState) -> bool {
    matches!(
        state,
        PipelineState::Recording
            | PipelineState::ReResolving
            | PipelineState::WaitingReconnect
            | PipelineState::Uploading
            | PipelineState::Submitting
    )
}

fn ensure_session_ready_to_submit(store: &StateStore, session_id: Uuid) -> AppResult<()> {
    let segments = store.list_segments(session_id)?;
    let uploaded_parts = store.list_uploaded_parts(session_id)?;
    let uploaded_indices: std::collections::HashSet<u32> = uploaded_parts
        .into_iter()
        .map(|part| part.segment_index)
        .collect();

    for segment in &segments {
        match segment.status {
            SegmentStatus::Finalized | SegmentStatus::Uploaded => {
                if !uploaded_indices.contains(&segment.index) {
                    return Err(AppError::State(format!(
                        "Segment {} is {:?} but has no UploadedPart; refusing submission",
                        segment.index, segment.status
                    )));
                }
            }
            SegmentStatus::Uploading => {
                return Err(AppError::State(format!(
                    "Segment {} is Uploading; upload outcome is ambiguous, refusing submission",
                    segment.index
                )));
            }
            SegmentStatus::Recording => {
                return Err(AppError::State(format!(
                    "Segment {} is still Recording; refusing submission",
                    segment.index
                )));
            }
            SegmentStatus::Failed => {
                return Err(AppError::State(format!(
                    "Segment {} is Failed; refusing submission",
                    segment.index
                )));
            }
            SegmentStatus::Cleaned => {
                return Err(AppError::State(format!(
                    "Segment {} was cleaned before submission; refusing submission",
                    segment.index
                )));
            }
            SegmentStatus::Filtered => {}
        }
    }

    Ok(())
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

    let uploaded_indices: std::collections::HashSet<u32> = store
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
                            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                                // Crash or concurrent cleanup after metadata but before remove:
                                // deletion is already true, so the state can still be advanced.
                            }
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

fn validate_session_credentials(
    session: &LiveSession,
    expected: &RoomCredentials,
) -> AppResult<()> {
    if session.record_credential != expected.record {
        return Err(AppError::State(format!(
            "Persisted session {} record credential does not match current room config",
            session.id
        )));
    }
    if session.upload_credential.as_ref() != Some(&expected.upload) {
        let actual = session
            .upload_credential
            .as_ref()
            .map(|credential| {
                format!(
                    "'{}' at {}",
                    credential.name,
                    credential.cookie_file.display()
                )
            })
            .unwrap_or_else(|| "no upload credential".to_string());
        return Err(AppError::State(format!(
            "Persisted session {} upload credential {} does not match current room config credential '{}' at {}",
            session.id,
            actual,
            expected.upload.name,
            expected.upload.cookie_file.display()
        )));
    }
    Ok(())
}

pub struct RoomSupervisor<U: Uploader + Send + Sync + 'static> {
    pub room_id: u64,
    pub session: PipelineSession,
    pub config: PipelineConfig,
    pub room_config: ResolvedRoomConfig,
    pub store: Arc<StateStore>,
    pub client: Arc<BiliClient>,
    pub uploader: Arc<U>,
    pub active_session_id: Option<Uuid>,
    upload_tasks: Vec<JoinHandle<BackgroundUploadResult>>,
    pub offline_since: Option<Instant>,
    reconnect_attempt: u32,
    pub shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl<U: Uploader + Send + Sync + 'static> RoomSupervisor<U> {
    pub fn new(
        room_id: u64,
        config: PipelineConfig,
        room_config: ResolvedRoomConfig,
        deps: RoomSupervisorDeps<U>,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> AppResult<Self> {
        let expected_credentials = room_config.credentials();
        let mut supervisor = Self {
            room_id,
            session: PipelineSession::new(room_id),
            config,
            room_config,
            store: deps.store.clone(),
            client: deps.client,
            uploader: deps.uploader,
            active_session_id: None,
            upload_tasks: Vec::new(),
            offline_since: None,
            reconnect_attempt: 0,
            shutdown_rx,
        };

        if let Some(room_state) = deps.store.get_room_pipeline_state(room_id)? {
            supervisor.session.state = room_state.state;

            if pipeline_state_requires_active_session(room_state.state) {
                let session_id = room_state.active_session_id.ok_or_else(|| {
                    AppError::State(format!(
                        "Persisted pipeline state {:?} requires active_session_id",
                        room_state.state
                    ))
                })?;
                let session = deps.store.get_session(session_id)?.ok_or_else(|| {
                    AppError::State(format!(
                        "Persisted active session {} for room {} does not exist",
                        session_id, room_id
                    ))
                })?;
                if session.room_key != room_id.to_string() {
                    return Err(AppError::State(format!(
                        "Persisted active session {} belongs to room {}, not {}",
                        session_id, session.room_key, room_id
                    )));
                }
                if session.status == SessionStatus::Failed {
                    return Err(AppError::State(format!(
                        "Persisted active session {} is Failed",
                        session_id
                    )));
                }
                validate_session_credentials(&session, &expected_credentials)?;
                supervisor.active_session_id = Some(session_id);
            }
        }

        Ok(supervisor)
    }

    /// Validate the transition `prev -> next` against the state machine
    /// table and the active-session invariant. Used as a precondition by
    /// both `transition` (which persists first) and the atomic-write paths
    /// that bundle the pipeline state with other rows (which must validate
    /// *before* persisting so an invalid transition cannot poison disk
    /// state).
    fn check_transition(&self, next: PipelineState) -> AppResult<()> {
        self.check_transition_with_active_session(next, self.active_session_id)
    }

    fn check_transition_with_active_session(
        &self,
        next: PipelineState,
        active_session_id: Option<Uuid>,
    ) -> AppResult<()> {
        if !self.session.state.can_transition_to(next) {
            return Err(AppError::State(format!(
                "Invalid pipeline state transition from {:?} to {:?}",
                self.session.state, next
            )));
        }
        if pipeline_state_requires_active_session(next) && active_session_id.is_none() {
            return Err(AppError::State(format!(
                "Pipeline state {:?} requires an active session",
                next
            )));
        }
        Ok(())
    }

    /// Apply an in-memory state transition that has already been persisted.
    /// Callers must have validated with `check_transition` and persisted
    /// the new pipeline state to redb before invoking this. The re-check
    /// here is a safety net — if it ever fires it means a caller skipped
    /// the contract and we should refuse to silently corrupt in-memory
    /// state to match a bad on-disk write.
    fn apply_transition(&mut self, next: PipelineState) -> AppResult<()> {
        self.check_transition(next)?;
        let prev = self.session.state;
        if !pipeline_state_requires_active_session(next) {
            self.active_session_id = None;
        }
        if next == PipelineState::WaitingReconnect && prev != PipelineState::WaitingReconnect {
            self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
        }
        if matches!(
            next,
            PipelineState::Recording
                | PipelineState::Uploading
                | PipelineState::Submitted
                | PipelineState::Idle
        ) {
            self.reconnect_attempt = 0;
        }
        self.session.state = next;
        info!(room_id = self.room_id, from = ?prev, to = ?next, "Pipeline state transition");
        Ok(())
    }

    /// Validate, persist, and apply a state transition. The common path
    /// where the pipeline state is the only thing changing on disk.
    pub fn transition(&mut self, next: PipelineState) -> AppResult<()> {
        self.check_transition(next)?;
        let active_session_id = if pipeline_state_requires_active_session(next) {
            self.active_session_id
        } else {
            None
        };
        self.store
            .put_room_pipeline_state(self.room_id, next, active_session_id)?;
        self.apply_transition(next)
    }

    pub fn reconnect_delay(&self) -> std::time::Duration {
        let base = std::time::Duration::from_secs(self.config.backoff_s);
        let max = std::time::Duration::from_secs(self.config.max_backoff_s);
        let exponent = self.reconnect_attempt.saturating_sub(1).min(31);
        let factor = 1_u32 << exponent;
        base.saturating_mul(factor).min(max)
    }

    /// Main state machine pump. Blocks when performing long tasks (e.g. recording, uploading).
    pub async fn run_step(&mut self) -> AppResult<()> {
        match self.session.state {
            PipelineState::Idle => {
                self.transition(PipelineState::Resolving)?;
            }
            PipelineState::Resolving | PipelineState::ReResolving => {
                match fetch_room_info(&self.client, self.room_id).await {
                    Ok(info) => {
                        if info.live_status == LiveStatus::Live {
                            if self.session.state == PipelineState::Resolving {
                                let session_id = Uuid::new_v4();
                                let room_credentials = self.room_config.credentials();

                                let live_session = LiveSession {
                                    id: session_id,
                                    room_key: self.room_id.to_string(),
                                    title: info.title.clone(),
                                    started_at: jiff::Timestamp::now(),
                                    status: SessionStatus::Recording,
                                    record_credential: room_credentials.record,
                                    upload_credential: Some(room_credentials.upload),
                                };

                                // Validate before the atomic write: an invalid
                                // transition must not be persisted, even via
                                // bundled writes. (Resolving -> Recording is in
                                // the table, but we're not going to trust that
                                // by hand — every caller of the state machine
                                // goes through check_transition.)
                                self.check_transition_with_active_session(
                                    PipelineState::Recording,
                                    Some(session_id),
                                )?;
                                self.store.put_session_and_pipeline_state(
                                    &live_session,
                                    self.room_id,
                                    PipelineState::Recording,
                                )?;
                                self.active_session_id = Some(session_id);
                                self.apply_transition(PipelineState::Recording)?;
                                self.offline_since = None;
                            } else {
                                self.offline_since = None;
                                self.transition(PipelineState::Recording)?;
                            }
                        } else if self.session.state == PipelineState::Resolving {
                            self.transition(PipelineState::Offline)?;
                        } else {
                            self.transition(PipelineState::WaitingReconnect)?;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to fetch room info for {}: {}", self.room_id, e);
                        if self.session.state == PipelineState::Resolving {
                            self.transition(PipelineState::Failed)?;
                        } else {
                            self.transition(PipelineState::WaitingReconnect)?;
                        }
                    }
                }
            }
            PipelineState::Offline => {
                self.transition(PipelineState::Idle)?;
            }
            PipelineState::Recording => {
                let active_session = self.active_session_id.ok_or_else(|| {
                    AppError::State("Recording state requires active_session_id".into())
                })?;

                let policy = RecorderPolicy {
                    layout: SegmentLayout {
                        output_dir: self.room_config.record.output_dir.clone(),
                    },
                    segment: SegmentPolicy {
                        segment_time: self.room_config.record.segment_time,
                        segment_size: self.room_config.record.segment_size,
                    },
                    filter: SegmentFilter {
                        min_segment_size: self.room_config.record.min_segment_size,
                    },
                };

                let play_info =
                    match fetch_play_info(&self.client, self.room_id, self.room_config.record.qn)
                        .await
                    {
                        Ok(info) => info,
                        Err(e) => {
                            warn!("fetch_play_info failed: {}", e);
                            self.transition(PipelineState::WaitingReconnect)?;
                            return Ok(());
                        }
                    };

                let candidates = match parse_stream_candidates(&play_info) {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("Failed to parse stream candidates: {}", e);
                        self.transition(PipelineState::WaitingReconnect)?;
                        return Ok(());
                    }
                };

                let cand_opt = match select_healthy_stream_candidate(
                    &candidates,
                    &self.room_config.record,
                    &self.client,
                )
                .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("No healthy stream candidates: {}", e);
                        self.transition(PipelineState::WaitingReconnect)?;
                        return Ok(());
                    }
                };

                let cand = match cand_opt {
                    Some(c) => c,
                    None => {
                        warn!("No healthy stream candidates available");
                        self.transition(PipelineState::WaitingReconnect)?;
                        return Ok(());
                    }
                };

                let req = self
                    .client
                    .stream_client()
                    .get(&cand.url)
                    .header("User-Agent", "Mozilla/5.0")
                    .header("Referer", "https://live.bilibili.com/");
                let resp = match req.send().await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("Failed to connect to stream: {}", e);
                        self.transition(PipelineState::WaitingReconnect)?;
                        return Ok(());
                    }
                };

                // Compute start_index across all segments
                let mut start_index = 1;
                let segments = self.store.list_segments(active_session)?;
                if let Some(max_idx) = segments.iter().map(|s| s.index).max() {
                    start_index = max_idx + 1;
                }

                let (event_tx, mut event_rx) = mpsc::unbounded_channel::<SegmentEvent>();
                let uploader_clone = self.uploader.clone();
                let store_clone = self.store.clone();

                let handle = tokio::spawn(async move {
                    while let Some(event) = event_rx.recv().await {
                        match event {
                            SegmentEvent::Finalized {
                                session_id,
                                index,
                                path,
                                size: _,
                                close_reason,
                            } => {
                                info!(
                                    "Segment finalized: idx={}, path={:?}, reason={}",
                                    index, path, close_reason
                                );
                                let segment = match validate_finalized_segment_for_upload(
                                    &store_clone,
                                    session_id,
                                    index,
                                    Some(&path),
                                ) {
                                    Ok(Ok(segment)) => segment,
                                    Ok(Err(reason)) => {
                                        error!(
                                            "Upload precondition failed for finalized segment {}: {}",
                                            index, reason
                                        );
                                        return Err(BackgroundUploadFailure::Reconcileable {
                                            index,
                                            error: reason,
                                        });
                                    }
                                    Err(error) => {
                                        error!(
                                            "Failed to validate finalized segment {}: {}",
                                            index, error
                                        );
                                        return Err(BackgroundUploadFailure::Reconcileable {
                                            index,
                                            error: error.to_string(),
                                        });
                                    }
                                };
                                match upload_and_persist_segment(
                                    uploader_clone.as_ref(),
                                    &store_clone,
                                    segment,
                                    format!("Part {}", index),
                                )
                                .await
                                {
                                    Ok(_uploaded_part) => {
                                        info!("Upload success for idx={}", index);
                                    }
                                    Err(PersistedUploadFailure::Remote { index, error }) => {
                                        error!(
                                            "Upload segment failed for idx={}: {}",
                                            index, error
                                        );
                                        return Err(BackgroundUploadFailure::Reconcileable {
                                            index,
                                            error,
                                        });
                                    }
                                    Err(PersistedUploadFailure::StateBeforeRemote {
                                        index,
                                        error,
                                    }) => {
                                        error!(
                                            "Failed to mark segment {} as Uploading before remote upload: {}",
                                            index, error
                                        );
                                        return Err(BackgroundUploadFailure::FatalState {
                                            index,
                                            error,
                                        });
                                    }
                                    Err(PersistedUploadFailure::StateAfterRemote {
                                        index,
                                        error,
                                    }) => {
                                        error!(
                                            "Upload segment persisted remotely but not locally for idx={}: {}",
                                            index, error
                                        );
                                        return Err(BackgroundUploadFailure::Ambiguous {
                                            index,
                                            error,
                                        });
                                    }
                                }
                            }
                            _ => {
                                // ignore Started, Filtered
                            }
                        }
                    }
                    Ok(())
                });

                self.upload_tasks.push(handle);

                info!("Starting record_flv from index {}", start_index);
                match record_flv(
                    resp,
                    active_session,
                    policy,
                    &self.store,
                    event_tx,
                    start_index,
                    self.shutdown_rx.clone(),
                )
                .await
                {
                    Ok(_) => {
                        info!("record_flv completed gracefully");
                        self.transition(PipelineState::WaitingReconnect)?;
                    }
                    Err(AppError::GracefulShutdown) => {
                        info!("record_flv interrupted by graceful shutdown");
                        // event_tx was moved into record_flv and dropped on return,
                        // closing the channel. Await the upload task to let it finish
                        // any in-flight upload before we exit.
                        let tasks = std::mem::take(&mut self.upload_tasks);
                        for task in tasks {
                            match task.await {
                                Ok(Ok(())) => {}
                                Ok(Err(BackgroundUploadFailure::Reconcileable {
                                    index,
                                    error,
                                })) => {
                                    warn!(
                                        "Upload task failed during shutdown for segment {} (recoverable): {}",
                                        index, error
                                    );
                                }
                                Ok(Err(BackgroundUploadFailure::FatalState { index, error })) => {
                                    self.transition(PipelineState::Failed)?;
                                    return Err(AppError::State(format!(
                                        "Failed to persist pre-upload state for segment {index}: {error}"
                                    )));
                                }
                                Ok(Err(BackgroundUploadFailure::Ambiguous { index, error })) => {
                                    self.transition(PipelineState::Failed)?;
                                    return Err(AppError::State(format!(
                                        "Remote upload for segment {index} may have succeeded, but state persistence failed during shutdown: {error}"
                                    )));
                                }
                                Err(e) => {
                                    self.transition(PipelineState::Failed)?;
                                    return Err(AppError::State(format!(
                                        "Background upload task panicked during shutdown; refusing automatic reconciliation: {e}"
                                    )));
                                }
                            }
                        }
                        // Do not transition — leave persisted state as-is
                        return Err(AppError::GracefulShutdown);
                    }
                    Err(e) => match e {
                        AppError::Io { .. }
                        | AppError::State(_)
                        | AppError::Config(_)
                        | AppError::Database(_)
                        | AppError::Table(_)
                        | AppError::Transaction(_)
                        | AppError::Storage(_)
                        | AppError::Commit(_) => {
                            error!("record_flv fatal error: {}", e);
                            return Err(e);
                        }
                        _ => {
                            warn!("record_flv transient error: {}", e);
                            self.transition(PipelineState::WaitingReconnect)?;
                        }
                    },
                }
            }
            PipelineState::WaitingReconnect => {
                if self.offline_since.is_none() {
                    self.offline_since = Some(Instant::now());
                }

                if let Some(since) = self.offline_since {
                    if since.elapsed().as_secs() > self.config.offline_grace_s {
                        info!("Offline grace period expired. Transitioning to Uploading.");
                        self.transition(PipelineState::Uploading)?;
                    } else {
                        // Not expired yet, try to resolve again
                        self.transition(PipelineState::ReResolving)?;
                    }
                } else {
                    self.transition(PipelineState::Uploading)?;
                }
            }
            PipelineState::Uploading => {
                let active_session = self.active_session_id.ok_or_else(|| {
                    AppError::State("Uploading state requires active_session_id".into())
                })?;

                let tasks = std::mem::take(&mut self.upload_tasks);

                for task in tasks {
                    match task.await {
                        Ok(Ok(())) => {}
                        Ok(Err(BackgroundUploadFailure::Reconcileable { index, error })) => {
                            warn!(
                                "Background upload task failed for segment {} (will reconcile): {}",
                                index, error
                            );
                        }
                        Ok(Err(BackgroundUploadFailure::FatalState { index, error })) => {
                            self.transition(PipelineState::Failed)?;
                            return Err(AppError::State(format!(
                                "Failed to persist pre-upload state for segment {index}: {error}"
                            )));
                        }
                        Ok(Err(BackgroundUploadFailure::Ambiguous { index, error })) => {
                            self.transition(PipelineState::Failed)?;
                            return Err(AppError::State(format!(
                                "Remote upload for segment {index} may have succeeded, but UploadedPart persistence failed: {error}. Refusing automatic reconciliation."
                            )));
                        }
                        Err(e) => {
                            self.transition(PipelineState::Failed)?;
                            return Err(AppError::State(format!(
                                "Background upload task panicked; refusing automatic reconciliation: {e}"
                            )));
                        }
                    }
                }

                let mut reconciliation_failed = false;
                let segments = self.store.list_segments(active_session)?;
                let uploaded_parts = self.store.list_uploaded_parts(active_session)?;

                let mut uploaded_indices: std::collections::HashSet<u32> = uploaded_parts
                    .into_iter()
                    .map(|p| p.segment_index)
                    .collect();

                for seg in &segments {
                    match seg.status {
                        SegmentStatus::Finalized if !uploaded_indices.contains(&seg.index) => {
                            info!("Reconciling upload for segment index {}", seg.index);
                            match validate_finalized_segment_for_upload(
                                &self.store,
                                active_session,
                                seg.index,
                                Some(&seg.path),
                            )? {
                                Ok(segment) => {
                                    match upload_and_persist_segment(
                                        self.uploader.as_ref(),
                                        &self.store,
                                        segment,
                                        format!("Part {}", seg.index),
                                    )
                                    .await
                                    {
                                        Ok(part) => {
                                            uploaded_indices.insert(part.segment_index);
                                        }
                                        Err(PersistedUploadFailure::Remote { index, error }) => {
                                            error!(
                                                "Reconciled upload failed for index {}: {}",
                                                index, error
                                            );
                                            reconciliation_failed = true;
                                        }
                                        Err(PersistedUploadFailure::StateBeforeRemote {
                                            index,
                                            error,
                                        }) => {
                                            self.transition(PipelineState::Failed)?;
                                            return Err(AppError::State(format!(
                                                "Failed to mark segment {index} as Uploading before reconciliation: {error}. Refusing remote upload."
                                            )));
                                        }
                                        Err(PersistedUploadFailure::StateAfterRemote {
                                            index,
                                            error,
                                        }) => {
                                            self.transition(PipelineState::Failed)?;
                                            return Err(AppError::State(format!(
                                                "Remote upload for segment {index} may have succeeded during reconciliation, but state persistence failed: {error}. Refusing automatic reconciliation."
                                            )));
                                        }
                                    }
                                }
                                Err(reason) => {
                                    error!(
                                        "Upload precondition failed for segment {}: {}",
                                        seg.index, reason
                                    );
                                    reconciliation_failed = true;
                                }
                            }
                        }
                        SegmentStatus::Uploading => {
                            error!(
                                "Segment {} is Uploading; upload outcome is ambiguous",
                                seg.index
                            );
                            reconciliation_failed = true;
                        }
                        SegmentStatus::Uploaded | SegmentStatus::Cleaned
                            if !uploaded_indices.contains(&seg.index) =>
                        {
                            error!(
                                "Segment {} is {:?} but lacks UploadedPart",
                                seg.index, seg.status
                            );
                            reconciliation_failed = true;
                        }
                        SegmentStatus::Recording | SegmentStatus::Failed => {
                            error!(
                                "Segment {} is {:?}; refusing upload reconciliation",
                                seg.index, seg.status
                            );
                            reconciliation_failed = true;
                        }
                        SegmentStatus::Finalized
                        | SegmentStatus::Uploaded
                        | SegmentStatus::Cleaned
                        | SegmentStatus::Filtered => {}
                    }
                }

                let final_parts = self.store.list_uploaded_parts(active_session)?;
                let final_indices: std::collections::HashSet<u32> =
                    final_parts.into_iter().map(|p| p.segment_index).collect();

                for seg in &segments {
                    match seg.status {
                        SegmentStatus::Finalized
                        | SegmentStatus::Uploaded
                        | SegmentStatus::Cleaned => {
                            if !final_indices.contains(&seg.index) {
                                error!(
                                    "Segment {} is {:?} but still lacks UploadedPart after reconciliation",
                                    seg.index, seg.status
                                );
                                reconciliation_failed = true;
                            }
                        }
                        SegmentStatus::Uploading
                        | SegmentStatus::Recording
                        | SegmentStatus::Failed => {
                            reconciliation_failed = true;
                        }
                        SegmentStatus::Filtered => {}
                    }
                }

                if reconciliation_failed {
                    self.transition(PipelineState::Failed)?;
                } else {
                    self.transition(PipelineState::Submitting)?;
                }
            }
            PipelineState::Submitting => {
                let active_session = self.active_session_id.ok_or_else(|| {
                    AppError::State("Submitting state requires active_session_id".into())
                })?;
                let session = self.store.get_session(active_session)?.ok_or_else(|| {
                    AppError::State(format!("Session {active_session} not found"))
                })?;

                // Check if already submitted or failed to avoid duplicate submissions
                if let Some(existing_sub) = self.store.get_submission(active_session)? {
                    match existing_sub.status {
                        SubmissionStatus::Submitted => {
                            if self.room_config.record.delete_after_submit {
                                let cleaned = cleanup_submitted_session_recordings(
                                    &self.store,
                                    active_session,
                                )
                                .await?;
                                if cleaned > 0 {
                                    info!(
                                        session_id = %active_session,
                                        cleaned,
                                        "Deleted local recordings after confirmed submission"
                                    );
                                }
                            }
                            self.transition(PipelineState::Submitted)?;
                            return Ok(());
                        }
                        SubmissionStatus::Failed => {
                            self.transition(PipelineState::Failed)?;
                            return Err(AppError::State("Submission previously failed. Requires Phase 6 recovery or manual intervention.".into()));
                        }
                        SubmissionStatus::Pending => {
                            return Err(AppError::State("Pending submission is unknown/in-flight and requires Phase 6 recovery/manual verification.".into()));
                        }
                        SubmissionStatus::Ambiguous => {
                            return Err(AppError::State("Ambiguous submission — Bilibili accepted but did not return aid/bvid; resolve via `state resolve-submission <session_id> --as submitted|failed`.".into()));
                        }
                    }
                } else {
                    // Mark LiveSession as finalized
                    if session.status == SessionStatus::Recording {
                        let mut finalized_session = session.clone();
                        finalized_session.status = SessionStatus::Finalized;
                        self.store.put_session(&finalized_session)?;
                    }
                }

                if let Err(e) = ensure_session_ready_to_submit(&self.store, active_session) {
                    self.transition(PipelineState::Failed)?;
                    return Err(e);
                }

                let mut parts = self.store.list_uploaded_parts(active_session)?;
                parts.sort_by_key(|p| p.segment_index);

                if parts.is_empty() {
                    let sub = Submission {
                        session_id: active_session,
                        upload_credential: session.upload_credential.clone().ok_or_else(|| {
                            AppError::State(format!(
                                "Session {active_session} has no upload credential"
                            ))
                        })?,
                        status: SubmissionStatus::Failed,
                        aid: None,
                        bvid: None,
                        error: Some("No parts to submit".into()),
                    };
                    self.store.put_submission(&sub)?;
                    self.transition(PipelineState::Failed)?;
                    return Err(AppError::State("No parts to submit".into()));
                }

                let title = self
                    .room_config
                    .submit
                    .title
                    .as_deref()
                    .map(|template| {
                        render_room_template(
                            template,
                            &self.room_config.name,
                            &self.room_config.url,
                            Some(&session),
                            self.room_id,
                        )
                    })
                    .transpose()?
                    .unwrap_or_else(|| session.title.clone());
                let description = self
                    .room_config
                    .submit
                    .description
                    .as_deref()
                    .map(|template| {
                        render_room_template(
                            template,
                            &self.room_config.name,
                            &self.room_config.url,
                            Some(&session),
                            self.room_id,
                        )
                    })
                    .transpose()?
                    .unwrap_or_default();
                let submit = &self.room_config.submit;

                let req = SubmissionRequest {
                    title,
                    description,
                    category_id: submit.category_id,
                    copyright: submit.copyright,
                    tags: submit.tags.clone(),
                    source: submit.source.clone(),
                    private: submit.private,
                    dynamic: submit.dynamic.clone(),
                    forbid_reprint: submit.forbid_reprint,
                    charging_panel: submit.charging_panel,
                    close_reply: submit.close_reply,
                    close_danmu: submit.close_danmu,
                    featured_reply: submit.featured_reply,
                    parts,
                };

                let mut sub = self
                    .store
                    .get_submission(active_session)?
                    .unwrap_or(Submission {
                        session_id: active_session,
                        upload_credential: session.upload_credential.clone().ok_or_else(|| {
                            AppError::State(format!(
                                "Session {active_session} has no upload credential"
                            ))
                        })?,
                        status: SubmissionStatus::Pending,
                        aid: None,
                        bvid: None,
                        error: None,
                    });

                if sub.status != SubmissionStatus::Pending {
                    sub.status = SubmissionStatus::Pending;
                    sub.error = None;
                }
                self.store.put_submission(&sub)?;

                match self.uploader.submit(req).await {
                    Ok(SubmissionOutcome::Confirmed { aid, bvid }) => {
                        sub.status = SubmissionStatus::Submitted;
                        sub.aid = aid;
                        sub.bvid = bvid;
                        self.store.put_submission(&sub)?;
                        if self.room_config.record.delete_after_submit {
                            let cleaned =
                                cleanup_submitted_session_recordings(&self.store, active_session)
                                    .await?;
                            if cleaned > 0 {
                                info!(
                                    session_id = %active_session,
                                    cleaned,
                                    "Deleted local recordings after confirmed submission"
                                );
                            }
                        }
                        self.transition(PipelineState::Submitted)?;
                    }
                    Ok(SubmissionOutcome::Ambiguous { reason }) => {
                        // The submit call completed without error, so the pipeline
                        // *action* succeeded — transition to Submitted. But the
                        // submission status records that we don't actually know
                        // whether Bilibili created the video. Operators must
                        // verify on Bilibili and use
                        // `state resolve-submission`.
                        warn!(
                            session_id = %active_session,
                            "Submission outcome is ambiguous: {}",
                            reason
                        );
                        sub.status = SubmissionStatus::Ambiguous;
                        sub.error = Some(reason);
                        self.store.put_submission(&sub)?;
                        self.transition(PipelineState::Submitted)?;
                    }
                    Err(e) => {
                        sub.status = SubmissionStatus::Failed;
                        sub.error = Some(e.to_string());
                        self.store.put_submission(&sub)?;
                        self.transition(PipelineState::Failed)?;
                    }
                }
            }
            PipelineState::Submitted => {
                self.active_session_id = None;
                self.offline_since = None;
                self.transition(PipelineState::Idle)?;
            }
            PipelineState::Failed => {
                return Err(AppError::State(
                    "Room is in Failed state and requires Phase 6 recovery or manual intervention."
                        .into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::model::UploadedPart;
    use crate::uploader::types::UploadRequest;

    /// What the FakeUploader should do on the next `submit` call.
    #[derive(Debug, Clone)]
    enum SubmitBehavior {
        Confirmed {
            aid: Option<u64>,
            bvid: Option<String>,
        },
        Ambiguous {
            reason: String,
        },
        Err(String),
    }

    struct FakeUploader {
        submit_count: std::sync::atomic::AtomicUsize,
        upload_count: std::sync::atomic::AtomicUsize,
        last_submission: std::sync::Mutex<Option<SubmissionRequest>>,
        submit_behavior: std::sync::Mutex<SubmitBehavior>,
    }

    impl FakeUploader {
        fn new() -> Self {
            Self {
                submit_count: std::sync::atomic::AtomicUsize::new(0),
                upload_count: std::sync::atomic::AtomicUsize::new(0),
                last_submission: std::sync::Mutex::new(None),
                submit_behavior: std::sync::Mutex::new(SubmitBehavior::Confirmed {
                    aid: Some(1),
                    bvid: Some("bv1".to_string()),
                }),
            }
        }

        fn get_submit_count(&self) -> usize {
            self.submit_count.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn get_upload_count(&self) -> usize {
            self.upload_count.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn last_submission(&self) -> Option<SubmissionRequest> {
            self.last_submission.lock().unwrap().clone()
        }

        fn set_submit_behavior(&self, behavior: SubmitBehavior) {
            *self.submit_behavior.lock().unwrap() = behavior;
        }
    }

    impl Uploader for FakeUploader {
        async fn check_login(&self) -> AppResult<()> {
            Ok(())
        }
        async fn upload_segment(&self, req: UploadRequest) -> AppResult<UploadedPart> {
            self.upload_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(UploadedPart {
                session_id: req.session_id,
                segment_index: req.segment_index,
                bili_filename: "fake_file".to_string(),
                part_title: req.part_title,
            })
        }
        async fn submit(&self, _req: SubmissionRequest) -> AppResult<SubmissionOutcome> {
            self.submit_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            *self.last_submission.lock().unwrap() = Some(_req);
            match self.submit_behavior.lock().unwrap().clone() {
                SubmitBehavior::Confirmed { aid, bvid } => {
                    Ok(SubmissionOutcome::Confirmed { aid, bvid })
                }
                SubmitBehavior::Ambiguous { reason } => Ok(SubmissionOutcome::Ambiguous { reason }),
                SubmitBehavior::Err(msg) => Err(AppError::Bilibili(msg)),
            }
        }
    }

    fn test_upload_credential() -> crate::credential::CredentialIdentity {
        crate::credential::CredentialIdentity::new("main", "Cargo.toml")
    }

    fn test_record_config() -> crate::config::ResolvedRecordConfig {
        crate::config::ResolvedRecordConfig {
            credential: None,
            output_dir: "./data/recordings".into(),
            segment_time: None,
            segment_size: None,
            min_segment_size: 20 * 1024 * 1024,
            qn: 10000,
            cdn: Vec::new(),
            delete_after_submit: false,
        }
    }

    fn test_submit_config() -> crate::config::ResolvedSubmitConfig {
        crate::config::ResolvedSubmitConfig {
            title: None,
            description: None,
            category_id: 171,
            copyright: crate::config::Copyright::Reprint,
            source: "source".into(),
            tags: Vec::new(),
            private: false,
            dynamic: String::new(),
            forbid_reprint: false,
            charging_panel: false,
            close_reply: false,
            close_danmu: false,
            featured_reply: false,
        }
    }

    fn test_room_config() -> ResolvedRoomConfig {
        ResolvedRoomConfig {
            name: "test-room".into(),
            url: "https://live.bilibili.com/1".into(),
            record: test_record_config(),
            upload: crate::config::ResolvedRoomUploadConfig {
                credential: test_upload_credential(),
            },
            submit: test_submit_config(),
        }
    }

    fn put_recording_session(store: &StateStore, room_id: u64) -> Uuid {
        let session_id = Uuid::new_v4();
        store
            .put_session(&LiveSession {
                id: session_id,
                room_key: room_id.to_string(),
                title: "Test Stream".into(),
                started_at: jiff::Timestamp::now(),
                status: SessionStatus::Recording,
                record_credential: None,
                upload_credential: Some(test_upload_credential()),
            })
            .unwrap();
        session_id
    }

    fn mock_supervisor() -> RoomSupervisor<FakeUploader> {
        let (_, rx) = tokio::sync::watch::channel(false);
        let store =
            Arc::new(StateStore::open(tempfile::tempdir().unwrap().path().join("db")).unwrap());
        RoomSupervisor::new(
            1,
            PipelineConfig::default(),
            test_room_config(),
            RoomSupervisorDeps {
                store,
                client: Arc::new(BiliClient::new(None).unwrap()),
                uploader: Arc::new(FakeUploader::new()),
            },
            rx,
        )
        .unwrap()
    }

    fn supervisor_with(
        store: Arc<StateStore>,
        uploader: Arc<FakeUploader>,
    ) -> RoomSupervisor<FakeUploader> {
        supervisor_with_room(store, uploader, test_room_config())
    }

    fn supervisor_with_room(
        store: Arc<StateStore>,
        uploader: Arc<FakeUploader>,
        room_config: ResolvedRoomConfig,
    ) -> RoomSupervisor<FakeUploader> {
        let (_, shutdown_rx) = tokio::sync::watch::channel(false);
        RoomSupervisor::new(
            1,
            PipelineConfig::default(),
            room_config,
            RoomSupervisorDeps {
                store,
                client: Arc::new(BiliClient::new(None).unwrap()),
                uploader,
            },
            shutdown_rx,
        )
        .unwrap()
    }

    #[test]
    fn test_supervisor_skeleton_offline() {
        let mut supervisor = mock_supervisor();

        assert_eq!(supervisor.session.state, PipelineState::Idle);
        supervisor.transition(PipelineState::Resolving).unwrap();
        assert_eq!(supervisor.session.state, PipelineState::Resolving);

        // Room is offline
        supervisor.transition(PipelineState::Offline).unwrap();

        // Go back to idle
        supervisor.transition(PipelineState::Idle).unwrap();
        assert_eq!(supervisor.session.state, PipelineState::Idle);
    }

    #[test]
    fn test_transition_rejects_invalid_jump_and_leaves_disk_clean() {
        let mut supervisor = mock_supervisor();
        let room_id = supervisor.room_id;
        let store = supervisor.store.clone();

        // Idle -> Recording is not in the state-machine table.
        let err = supervisor
            .transition(PipelineState::Recording)
            .expect_err("invalid transition must be rejected");
        assert!(matches!(err, AppError::State(_)));
        assert_eq!(supervisor.session.state, PipelineState::Idle);
        // No pipeline state should have been written to redb.
        assert!(store.get_pipeline_state(room_id).unwrap().is_none());
    }

    #[test]
    fn test_transition_rejects_target_without_active_session() {
        let mut supervisor = mock_supervisor();
        let room_id = supervisor.room_id;
        let store = supervisor.store.clone();

        // Drive to Resolving legitimately so the transition table allows
        // Resolving -> Recording. But active_session_id is still None, so
        // Recording (which requires a session) must be refused.
        supervisor.transition(PipelineState::Resolving).unwrap();
        let err = supervisor
            .transition(PipelineState::Recording)
            .expect_err("Recording without active_session_id must be rejected");
        assert!(
            matches!(err, AppError::State(ref msg) if msg.contains("requires an active session"))
        );
        // Disk state still Resolving — no half-written Recording row.
        assert_eq!(
            store.get_pipeline_state(room_id).unwrap(),
            Some(PipelineState::Resolving)
        );
    }

    #[test]
    fn test_reconnect_delay_exponential_and_clamped() {
        let mut supervisor = mock_supervisor();
        supervisor.config.backoff_s = 5;
        supervisor.config.max_backoff_s = 20;
        supervisor.session.state = PipelineState::Recording;
        supervisor.active_session_id = Some(Uuid::new_v4());

        supervisor
            .transition(PipelineState::WaitingReconnect)
            .unwrap();
        assert_eq!(
            supervisor.reconnect_delay(),
            std::time::Duration::from_secs(5)
        );

        supervisor.transition(PipelineState::ReResolving).unwrap();
        supervisor
            .transition(PipelineState::WaitingReconnect)
            .unwrap();
        assert_eq!(
            supervisor.reconnect_delay(),
            std::time::Duration::from_secs(10)
        );

        supervisor.transition(PipelineState::ReResolving).unwrap();
        supervisor
            .transition(PipelineState::WaitingReconnect)
            .unwrap();
        assert_eq!(
            supervisor.reconnect_delay(),
            std::time::Duration::from_secs(20)
        );

        supervisor.transition(PipelineState::ReResolving).unwrap();
        supervisor
            .transition(PipelineState::WaitingReconnect)
            .unwrap();
        assert_eq!(
            supervisor.reconnect_delay(),
            std::time::Duration::from_secs(20)
        );

        supervisor.transition(PipelineState::Recording).unwrap();
        assert_eq!(
            supervisor.reconnect_delay(),
            std::time::Duration::from_secs(5)
        );
    }

    #[test]
    fn test_apply_transition_is_safety_net_for_atomic_write_callers() {
        // Simulates a caller that bundled the pipeline state with another
        // write but skipped check_transition. apply_transition's internal
        // re-check must catch the invalid transition and refuse to update
        // the in-memory state — at that point on-disk state is already
        // corrupt, but at least the in-memory state machine refuses to
        // pretend it's fine.
        let mut supervisor = mock_supervisor();
        let err = supervisor
            .apply_transition(PipelineState::Recording)
            .expect_err("apply_transition must re-check");
        assert!(matches!(err, AppError::State(_)));
        assert_eq!(supervisor.session.state, PipelineState::Idle);
    }

    #[test]
    fn test_parse_duration_variations() {
        use crate::config::parse_hms_duration;
        assert_eq!(
            parse_hms_duration("01:30:00").unwrap(),
            std::time::Duration::from_secs(90 * 60)
        );
        assert_eq!(
            parse_hms_duration("00:15:30").unwrap(),
            std::time::Duration::from_secs(15 * 60 + 30)
        );
        assert!(parse_hms_duration("invalid").is_err());
        assert!(parse_hms_duration("01:aa:bb").is_err());
    }
    #[test]
    fn test_parse_size_variations() {
        use crate::config::parse_size_bytes;
        assert_eq!(parse_size_bytes("20MiB"), Some(20 * 1024 * 1024));
        assert_eq!(parse_size_bytes("2GiB"), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_size_bytes("10MB"), Some(10 * 1024 * 1024));
        assert_eq!(parse_size_bytes("15KB"), Some(15 * 1024));
        assert_eq!(parse_size_bytes("invalid"), None);
    }

    #[tokio::test]
    async fn test_submitting_with_empty_parts() {
        use crate::state::store::StateStore;

        let store = std::sync::Arc::new(
            StateStore::open(tempfile::tempdir().unwrap().path().join("db")).unwrap(),
        );
        let mut supervisor = supervisor_with(store.clone(), Arc::new(FakeUploader::new()));

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        let err = supervisor.run_step().await.unwrap_err();
        assert!(matches!(err, AppError::State(_)));
        assert_eq!(supervisor.session.state, PipelineState::Failed);

        let sub = store.get_submission(session_id).unwrap().unwrap();
        assert_eq!(sub.status, crate::state::model::SubmissionStatus::Failed);
    }

    #[tokio::test]
    async fn test_uploading_reconciles_missing_parts() {
        use crate::state::model::{Segment, SegmentStatus};
        use crate::state::store::StateStore;

        let file_dir = tempfile::tempdir().unwrap();
        let file_path = file_dir.path().join("test.flv");
        std::fs::write(&file_path, b"FLV").unwrap();

        let store = std::sync::Arc::new(
            StateStore::open(tempfile::tempdir().unwrap().path().join("db")).unwrap(),
        );
        let mut supervisor = supervisor_with(store.clone(), Arc::new(FakeUploader::new()));

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Uploading;

        // Add a finalized segment with no uploaded part
        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: file_path,
                status: SegmentStatus::Finalized,
                close_reason: None,
                error: None,
            })
            .unwrap();

        supervisor.run_step().await.unwrap();

        // Should have transitioned to Submitting and added uploaded part
        assert_eq!(supervisor.session.state, PipelineState::Submitting);
        let parts = store.list_uploaded_parts(session_id).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].segment_index, 1);
        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments[0].status, SegmentStatus::Uploaded);
    }

    #[tokio::test]
    async fn test_uploading_refuses_missing_finalized_file() {
        use crate::state::model::{Segment, SegmentStatus};
        use crate::state::store::StateStore;

        let store =
            Arc::new(StateStore::open(tempfile::tempdir().unwrap().path().join("db")).unwrap());
        let uploader = Arc::new(FakeUploader::new());
        let mut supervisor = supervisor_with(store.clone(), uploader.clone());

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Uploading;

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: std::path::PathBuf::from("missing.flv"),
                status: SegmentStatus::Finalized,
                close_reason: None,
                error: None,
            })
            .unwrap();

        supervisor.run_step().await.unwrap();

        assert_eq!(supervisor.session.state, PipelineState::Failed);
        assert_eq!(uploader.get_upload_count(), 0);
        assert!(store.list_uploaded_parts(session_id).unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_uploading_refuses_ambiguous_uploading_segment() {
        use crate::state::model::{Segment, SegmentStatus};
        use crate::state::store::StateStore;

        let store =
            Arc::new(StateStore::open(tempfile::tempdir().unwrap().path().join("db")).unwrap());
        let uploader = Arc::new(FakeUploader::new());
        let mut supervisor = supervisor_with(store.clone(), uploader.clone());

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Uploading;

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: std::path::PathBuf::from("ambiguous.flv"),
                status: SegmentStatus::Uploading,
                close_reason: None,
                error: None,
            })
            .unwrap();

        supervisor.run_step().await.unwrap();

        assert_eq!(supervisor.session.state, PipelineState::Failed);
        assert_eq!(uploader.get_upload_count(), 0);
    }

    #[tokio::test]
    async fn test_recording_missing_components() {
        let mut supervisor = mock_supervisor();
        supervisor.session.state = PipelineState::Recording;

        let err = supervisor.run_step().await.unwrap_err();
        assert!(matches!(err, AppError::State(_)));
        assert_eq!(supervisor.session.state, PipelineState::Recording);
    }

    #[test]
    fn test_resume_requires_persisted_active_session_id() {
        let store =
            Arc::new(StateStore::open(tempfile::tempdir().unwrap().path().join("db")).unwrap());
        store
            .put_pipeline_state(1, PipelineState::Recording)
            .unwrap();
        let (_, rx) = tokio::sync::watch::channel(false);

        let res = RoomSupervisor::new(
            1,
            PipelineConfig::default(),
            test_room_config(),
            RoomSupervisorDeps {
                store,
                client: Arc::new(BiliClient::new(None).unwrap()),
                uploader: Arc::new(FakeUploader::new()),
            },
            rx,
        );

        assert!(matches!(res, Err(AppError::State(_))));
    }

    #[test]
    fn test_resume_uses_persisted_active_session_id_not_latest_session() {
        let store =
            Arc::new(StateStore::open(tempfile::tempdir().unwrap().path().join("db")).unwrap());
        let active_session_id = put_recording_session(&store, 1);
        let newer_session_id = Uuid::new_v4();
        store
            .put_session(&LiveSession {
                id: newer_session_id,
                room_key: "1".into(),
                title: "newer terminal session".into(),
                started_at: jiff::Timestamp::now() + jiff::SignedDuration::from_secs(1),
                status: SessionStatus::Finalized,
                record_credential: None,
                upload_credential: Some(test_upload_credential()),
            })
            .unwrap();
        store
            .put_room_pipeline_state(1, PipelineState::Recording, Some(active_session_id))
            .unwrap();

        let (_, rx) = tokio::sync::watch::channel(false);
        let supervisor = RoomSupervisor::new(
            1,
            PipelineConfig::default(),
            test_room_config(),
            RoomSupervisorDeps {
                store,
                client: Arc::new(BiliClient::new(None).unwrap()),
                uploader: Arc::new(FakeUploader::new()),
            },
            rx,
        )
        .unwrap();

        assert_eq!(supervisor.active_session_id, Some(active_session_id));
    }

    #[test]
    fn test_resume_refuses_upload_credential_mismatch() {
        let store =
            Arc::new(StateStore::open(tempfile::tempdir().unwrap().path().join("db")).unwrap());
        let session_id = Uuid::new_v4();
        store
            .put_session(&LiveSession {
                id: session_id,
                room_key: "1".into(),
                title: "mismatched credential".into(),
                started_at: jiff::Timestamp::now(),
                status: SessionStatus::Recording,
                record_credential: None,
                upload_credential: Some(crate::credential::CredentialIdentity::new(
                    "main",
                    "Cargo.lock",
                )),
            })
            .unwrap();
        store
            .put_room_pipeline_state(1, PipelineState::Recording, Some(session_id))
            .unwrap();

        let (_, rx) = tokio::sync::watch::channel(false);
        let res = RoomSupervisor::new(
            1,
            PipelineConfig::default(),
            test_room_config(),
            RoomSupervisorDeps {
                store,
                client: Arc::new(BiliClient::new(None).unwrap()),
                uploader: Arc::new(FakeUploader::new()),
            },
            rx,
        );

        assert!(matches!(res, Err(AppError::State(ref msg)) if msg.contains("upload credential")));
    }

    #[tokio::test]
    async fn test_submitting_idempotent_submitted() {
        let db_dir = tempfile::tempdir().unwrap();
        let store =
            std::sync::Arc::new(StateStore::open(db_dir.path().join("state.redb")).unwrap());
        let uploader = std::sync::Arc::new(FakeUploader::new());
        let mut room_config = test_room_config();
        room_config.submit.category_id = 123;
        let mut supervisor = supervisor_with_room(store.clone(), uploader.clone(), room_config);

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
                status: SubmissionStatus::Submitted,
                aid: Some(1),
                bvid: Some("bv1".into()),
                error: None,
            })
            .unwrap();

        supervisor.run_step().await.unwrap();

        assert_eq!(supervisor.session.state, PipelineState::Submitted);
        assert_eq!(uploader.get_submit_count(), 0);
    }

    #[tokio::test]
    async fn test_submitting_deletes_uploaded_recordings_after_confirmed_submission() {
        use crate::state::model::{Segment, SegmentStatus};

        let db_dir = tempfile::tempdir().unwrap();
        let file_dir = tempfile::tempdir().unwrap();
        let file_path = file_dir.path().join("segment.flv");
        std::fs::write(&file_path, b"FLV").unwrap();

        let store = Arc::new(StateStore::open(db_dir.path().join("state.redb")).unwrap());
        let uploader = Arc::new(FakeUploader::new());
        let mut room_config = test_room_config();
        room_config.record.delete_after_submit = true;
        let mut supervisor = supervisor_with_room(store.clone(), uploader.clone(), room_config);

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: file_path.clone(),
                status: SegmentStatus::Uploaded,
                close_reason: None,
                error: None,
            })
            .unwrap();
        store
            .put_uploaded_part(&UploadedPart {
                session_id,
                segment_index: 0,
                bili_filename: "fake_file".into(),
                part_title: "part 0".into(),
            })
            .unwrap();

        supervisor.run_step().await.unwrap();

        assert_eq!(supervisor.session.state, PipelineState::Submitted);
        assert!(!file_path.exists());
        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments[0].status, SegmentStatus::Cleaned);
        let sub = store.get_submission(session_id).unwrap().unwrap();
        assert_eq!(sub.status, SubmissionStatus::Submitted);
    }

    #[tokio::test]
    async fn test_submitting_existing_submitted_retries_recording_cleanup() {
        use crate::state::model::{Segment, SegmentStatus};

        let db_dir = tempfile::tempdir().unwrap();
        let file_dir = tempfile::tempdir().unwrap();
        let file_path = file_dir.path().join("segment.flv");
        std::fs::write(&file_path, b"FLV").unwrap();

        let store = Arc::new(StateStore::open(db_dir.path().join("state.redb")).unwrap());
        let uploader = Arc::new(FakeUploader::new());
        let mut room_config = test_room_config();
        room_config.record.delete_after_submit = true;
        let mut supervisor = supervisor_with_room(store.clone(), uploader.clone(), room_config);

        let session_id = put_recording_session(&store, 1);
        let mut session = store.get_session(session_id).unwrap().unwrap();
        session.status = SessionStatus::Finalized;
        store.put_session(&session).unwrap();

        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: file_path.clone(),
                status: SegmentStatus::Uploaded,
                close_reason: None,
                error: None,
            })
            .unwrap();
        store
            .put_uploaded_part(&UploadedPart {
                session_id,
                segment_index: 0,
                bili_filename: "fake_file".into(),
                part_title: "part 0".into(),
            })
            .unwrap();
        store
            .put_submission(&Submission {
                session_id,
                upload_credential: test_upload_credential(),
                status: SubmissionStatus::Submitted,
                aid: Some(1),
                bvid: Some("bv1".into()),
                error: None,
            })
            .unwrap();

        supervisor.run_step().await.unwrap();

        assert_eq!(supervisor.session.state, PipelineState::Submitted);
        assert_eq!(uploader.get_submit_count(), 0);
        assert!(!file_path.exists());
        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments[0].status, SegmentStatus::Cleaned);
    }

    #[tokio::test]
    async fn test_submitting_uses_room_title_and_description_templates() {
        let db_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(StateStore::open(db_dir.path().join("state.redb")).unwrap());
        let uploader = Arc::new(FakeUploader::new());
        let mut room_config = test_room_config();
        room_config.name = "room-name".into();
        room_config.submit.title = Some("Archive {title} #{room_id}".into());
        room_config.submit.description = Some("From {name}: {url}".into());
        let mut supervisor = supervisor_with_room(store.clone(), uploader.clone(), room_config);

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        store
            .put_uploaded_part(&UploadedPart {
                session_id,
                segment_index: 0,
                bili_filename: "fake_file".into(),
                part_title: "part 0".into(),
            })
            .unwrap();

        supervisor.run_step().await.unwrap();

        let req = uploader.last_submission().unwrap();
        assert_eq!(req.title, "Archive Test Stream #1");
        assert_eq!(
            req.description,
            "From room-name: https://live.bilibili.com/1"
        );
    }

    #[tokio::test]
    async fn test_submitting_refuses_ambiguous_uploading_segment() {
        use crate::state::model::{Segment, SegmentStatus};

        let db_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(StateStore::open(db_dir.path().join("state.redb")).unwrap());
        let uploader = Arc::new(FakeUploader::new());
        let mut supervisor = supervisor_with(store.clone(), uploader.clone());

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: std::path::PathBuf::from("ambiguous.flv"),
                status: SegmentStatus::Uploading,
                close_reason: None,
                error: None,
            })
            .unwrap();

        let err = supervisor.run_step().await.unwrap_err();

        assert!(matches!(err, AppError::State(_)));
        assert_eq!(supervisor.session.state, PipelineState::Failed);
        assert_eq!(uploader.get_submit_count(), 0);
    }

    #[tokio::test]
    async fn test_submitting_idempotent_failed() {
        let db_dir = tempfile::tempdir().unwrap();
        let store =
            std::sync::Arc::new(StateStore::open(db_dir.path().join("state.redb")).unwrap());
        let uploader = std::sync::Arc::new(FakeUploader::new());
        let mut room_config = test_room_config();
        room_config.submit.category_id = 123;
        let mut supervisor = supervisor_with_room(store.clone(), uploader.clone(), room_config);

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
                status: SubmissionStatus::Failed,
                aid: None,
                bvid: None,
                error: Some("mock err".into()),
            })
            .unwrap();

        let err = supervisor.run_step().await.unwrap_err();
        assert!(matches!(err, AppError::State(_)));

        assert_eq!(supervisor.session.state, PipelineState::Failed);
        assert_eq!(uploader.get_submit_count(), 0);
    }

    #[tokio::test]
    async fn test_submitting_idempotent_pending() {
        let db_dir = tempfile::tempdir().unwrap();
        let store =
            std::sync::Arc::new(StateStore::open(db_dir.path().join("state.redb")).unwrap());
        let uploader = std::sync::Arc::new(FakeUploader::new());
        let mut room_config = test_room_config();
        room_config.submit.category_id = 123;
        let mut supervisor = supervisor_with_room(store.clone(), uploader.clone(), room_config);

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
                status: SubmissionStatus::Pending,
                aid: None,
                bvid: None,
                error: None,
            })
            .unwrap();

        store
            .put_uploaded_part(&UploadedPart {
                session_id,
                segment_index: 0,
                bili_filename: "fake_file".into(),
                part_title: "part 0".into(),
            })
            .unwrap();

        let err = supervisor.run_step().await.unwrap_err();
        assert!(matches!(err, AppError::State(_)));

        assert_eq!(supervisor.session.state, PipelineState::Submitting);
        assert_eq!(uploader.get_submit_count(), 0);
    }

    #[tokio::test]
    async fn test_submitting_records_ambiguous_outcome() {
        use crate::state::model::{Segment, SegmentStatus};

        let db_dir = tempfile::tempdir().unwrap();
        let file_dir = tempfile::tempdir().unwrap();
        let file_path = file_dir.path().join("segment.flv");
        std::fs::write(&file_path, b"FLV").unwrap();
        let store = Arc::new(StateStore::open(db_dir.path().join("state.redb")).unwrap());
        let uploader = Arc::new(FakeUploader::new());
        uploader.set_submit_behavior(SubmitBehavior::Ambiguous {
            reason: "code=0 but no aid/bvid".into(),
        });

        let mut room_config = test_room_config();
        room_config.record.delete_after_submit = true;
        let mut supervisor = supervisor_with_room(store.clone(), uploader.clone(), room_config);

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: file_path.clone(),
                status: SegmentStatus::Uploaded,
                close_reason: None,
                error: None,
            })
            .unwrap();
        store
            .put_uploaded_part(&UploadedPart {
                session_id,
                segment_index: 0,
                bili_filename: "fake_file".into(),
                part_title: "part 0".into(),
            })
            .unwrap();

        // submit succeeds (the pipeline action completed), but the submission
        // status records the outcome as Ambiguous.
        supervisor.run_step().await.unwrap();

        assert_eq!(supervisor.session.state, PipelineState::Submitted);
        assert_eq!(uploader.get_submit_count(), 1);

        let sub = store.get_submission(session_id).unwrap().unwrap();
        assert_eq!(sub.status, SubmissionStatus::Ambiguous);
        assert!(sub.aid.is_none());
        assert!(sub.bvid.is_none());
        assert!(sub.error.as_deref().unwrap().contains("no aid/bvid"));
        assert!(file_path.exists());
        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments[0].status, SegmentStatus::Uploaded);
    }

    #[tokio::test]
    async fn test_submitting_refuses_existing_ambiguous_submission() {
        let db_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(StateStore::open(db_dir.path().join("state.redb")).unwrap());
        let uploader = Arc::new(FakeUploader::new());

        let mut supervisor = supervisor_with(store.clone(), uploader.clone());

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        // Pre-existing Ambiguous submission from a prior run.
        store
            .put_submission(&Submission {
                session_id,
                upload_credential: crate::credential::CredentialIdentity::new(
                    "test",
                    "cookies.json",
                ),
                status: SubmissionStatus::Ambiguous,
                aid: None,
                bvid: None,
                error: Some("prior ambiguous outcome".into()),
            })
            .unwrap();

        let err = supervisor.run_step().await.unwrap_err();
        assert!(matches!(err, AppError::State(ref msg) if msg.contains("Ambiguous")));
        // Refused — no re-submission.
        assert_eq!(uploader.get_submit_count(), 0);
        // Status stayed Ambiguous; no automatic flip.
        let sub = store.get_submission(session_id).unwrap().unwrap();
        assert_eq!(sub.status, SubmissionStatus::Ambiguous);
    }

    #[tokio::test]
    async fn test_submitting_records_failed_on_remote_error() {
        let db_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(StateStore::open(db_dir.path().join("state.redb")).unwrap());
        let uploader = Arc::new(FakeUploader::new());
        uploader.set_submit_behavior(SubmitBehavior::Err("network reset".into()));

        let mut supervisor = supervisor_with(store.clone(), uploader.clone());

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        store
            .put_uploaded_part(&UploadedPart {
                session_id,
                segment_index: 0,
                bili_filename: "fake_file".into(),
                part_title: "part 0".into(),
            })
            .unwrap();

        // submit Err → SubmissionStatus::Failed, pipeline → Failed.
        let _ = supervisor.run_step().await;

        assert_eq!(supervisor.session.state, PipelineState::Failed);
        let sub = store.get_submission(session_id).unwrap().unwrap();
        assert_eq!(sub.status, SubmissionStatus::Failed);
        assert!(sub.error.as_deref().unwrap().contains("network reset"));
    }
}
