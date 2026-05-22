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
use crate::config::{AppConfig, PipelineConfig, RoomConfig};
use crate::error::{AppError, AppResult};
use crate::pipeline::session::PipelineSession;
use crate::pipeline::state_machine::PipelineState;
use crate::recorder::segment::SegmentPolicy;
use crate::recorder::{record_flv, segment::SegmentEvent};
use crate::state::model::{
    LiveSession, SegmentStatus, SessionStatus, Submission, SubmissionStatus,
};
use crate::state::store::StateStore;
use crate::uploader::types::{SubmissionRequest, Uploader};
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
    pub app_config: Arc<AppConfig>,
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

fn render_room_template(
    template: &str,
    room_config: &RoomConfig,
    session: Option<&LiveSession>,
    room_id: u64,
) -> String {
    let title = session
        .map(|s| s.title.as_str())
        .unwrap_or(room_config.name.as_str());
    template
        .replace("{title}", title)
        .replace("{room_title}", title)
        .replace("{room_name}", &room_config.name)
        .replace("{name}", &room_config.name)
        .replace("{room_id}", &room_id.to_string())
        .replace("{url}", &room_config.url)
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
            SegmentStatus::Filtered => {}
        }
    }

    Ok(())
}

pub struct RoomSupervisor<U: Uploader + Send + Sync + 'static> {
    pub room_id: u64,
    pub session: PipelineSession,
    pub config: PipelineConfig,
    pub room_config: RoomConfig,
    pub store: Arc<StateStore>,
    pub client: Arc<BiliClient>,
    pub uploader: Arc<U>,
    pub active_session_id: Option<Uuid>,
    upload_tasks: Vec<JoinHandle<BackgroundUploadResult>>,
    pub offline_since: Option<Instant>,
    pub app_config: Arc<AppConfig>,
    pub shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl<U: Uploader + Send + Sync + 'static> RoomSupervisor<U> {
    pub fn new(
        room_id: u64,
        config: PipelineConfig,
        room_config: RoomConfig,
        deps: RoomSupervisorDeps<U>,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> AppResult<Self> {
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
            app_config: deps.app_config,
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
                supervisor.active_session_id = Some(session_id);
            }
        }

        Ok(supervisor)
    }

    /// Perform a single state transition, updating internal state and persisting it.
    pub fn transition(&mut self, next: PipelineState) -> AppResult<()> {
        let prev = self.session.state;

        if !prev.can_transition_to(next) {
            return Err(AppError::State(format!(
                "Invalid pipeline state transition from {:?} to {:?}",
                prev, next
            )));
        }

        let active_session_id = if pipeline_state_requires_active_session(next) {
            Some(self.active_session_id.ok_or_else(|| {
                AppError::State(format!(
                    "Pipeline state {:?} requires an active session",
                    next
                ))
            })?)
        } else {
            None
        };

        self.store
            .put_room_pipeline_state(self.room_id, next, active_session_id)?;

        if !pipeline_state_requires_active_session(next) {
            self.active_session_id = None;
        }

        self.session.state = next;

        info!(room_id = self.room_id, from = ?prev, to = ?next, "Pipeline state transition");
        Ok(())
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

                                let live_session = LiveSession {
                                    id: session_id,
                                    room_key: self.room_id.to_string(),
                                    title: info.title.clone(),
                                    started_at: jiff::Timestamp::now(),
                                    status: SessionStatus::Recording,
                                };

                                self.store.put_session_and_pipeline_state(
                                    &live_session,
                                    self.room_id,
                                    PipelineState::Recording,
                                )?;

                                self.active_session_id = Some(session_id);

                                let prev = self.session.state;
                                self.session.state = PipelineState::Recording;
                                info!(room_id = self.room_id, from = ?prev, to = ?PipelineState::Recording, "Pipeline state transition");
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

                let policy = SegmentPolicy {
                    output_dir: self.app_config.record.output_dir.clone(),
                    segment_time: self.app_config.record.segment_time_duration()?,
                    segment_size: self.app_config.record.segment_size_bytes()?,
                    min_segment_size: self.app_config.record.min_segment_size_bytes()?,
                };

                let play_info =
                    match fetch_play_info(&self.client, self.room_id, self.app_config.record.qn)
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
                    &self.app_config.record,
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
                            } => {
                                info!("Segment finalized: idx={}, path={:?}", index, path);
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
                        SegmentStatus::Uploaded if !uploaded_indices.contains(&seg.index) => {
                            error!("Segment {} is Uploaded but lacks UploadedPart", seg.index);
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
                        | SegmentStatus::Filtered => {}
                    }
                }

                let final_parts = self.store.list_uploaded_parts(active_session)?;
                let final_indices: std::collections::HashSet<u32> =
                    final_parts.into_iter().map(|p| p.segment_index).collect();

                for seg in &segments {
                    match seg.status {
                        SegmentStatus::Finalized | SegmentStatus::Uploaded => {
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
                    .title
                    .as_deref()
                    .map(|template| {
                        render_room_template(
                            template,
                            &self.room_config,
                            Some(&session),
                            self.room_id,
                        )
                    })
                    .unwrap_or_else(|| session.title.clone());
                let description = self
                    .room_config
                    .description
                    .as_deref()
                    .map(|template| {
                        render_room_template(
                            template,
                            &self.room_config,
                            Some(&session),
                            self.room_id,
                        )
                    })
                    .unwrap_or_default();
                let upload_config = self.app_config.upload_config()?;

                let req = SubmissionRequest {
                    title,
                    description,
                    tid: upload_config.tid,
                    copyright: upload_config.copyright,
                    tags: upload_config.tags.clone(),
                    source: upload_config.source.clone(),
                    parts,
                };

                let mut sub = self
                    .store
                    .get_submission(active_session)?
                    .unwrap_or(Submission {
                        session_id: active_session,
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
                    Ok(res) => {
                        sub.status = SubmissionStatus::Submitted;
                        sub.aid = res.aid;
                        sub.bvid = res.bvid;
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
    use crate::uploader::types::{SubmissionResult, UploadRequest};

    struct FakeUploader {
        submit_count: std::sync::atomic::AtomicUsize,
        upload_count: std::sync::atomic::AtomicUsize,
        last_submission: std::sync::Mutex<Option<SubmissionRequest>>,
    }

    impl FakeUploader {
        fn new() -> Self {
            Self {
                submit_count: std::sync::atomic::AtomicUsize::new(0),
                upload_count: std::sync::atomic::AtomicUsize::new(0),
                last_submission: std::sync::Mutex::new(None),
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
        async fn submit(&self, _req: SubmissionRequest) -> AppResult<SubmissionResult> {
            self.submit_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            *self.last_submission.lock().unwrap() = Some(_req);
            Ok(SubmissionResult {
                aid: Some(1),
                bvid: Some("bv1".to_string()),
            })
        }
    }

    fn test_app_config() -> AppConfig {
        AppConfig::parse("[upload]\ncookie_file=\"cookies.json\"\ntid=1\nline=\"auto\"").unwrap()
    }

    fn test_room_config() -> RoomConfig {
        RoomConfig {
            name: "test-room".into(),
            url: "https://live.bilibili.com/1".into(),
            title: None,
            description: None,
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
                app_config: Arc::new(test_app_config()),
            },
            rx,
        )
        .unwrap()
    }

    fn supervisor_with(
        store: Arc<StateStore>,
        uploader: Arc<FakeUploader>,
        config: AppConfig,
    ) -> RoomSupervisor<FakeUploader> {
        supervisor_with_room(store, uploader, config, test_room_config())
    }

    fn supervisor_with_room(
        store: Arc<StateStore>,
        uploader: Arc<FakeUploader>,
        config: AppConfig,
        room_config: RoomConfig,
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
                app_config: Arc::new(config),
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
        let config = crate::config::AppConfig {
            data: Default::default(),
            record: Default::default(),
            upload: Some(crate::config::UploadConfig {
                cookie_file: "test".into(),
                line: "auto".into(),
                threads: 1,
                submit_api: Default::default(),
                tid: 171,
                copyright: 2,
                source: "source".into(),
                tags: vec![],
            }),
            pipeline: Default::default(),
            rooms: vec![],
        };
        let mut supervisor = supervisor_with(store.clone(), Arc::new(FakeUploader::new()), config);

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
        let config = crate::config::AppConfig {
            data: Default::default(),
            record: Default::default(),
            upload: Some(crate::config::UploadConfig {
                cookie_file: "test".into(),
                line: "auto".into(),
                threads: 1,
                submit_api: Default::default(),
                tid: 171,
                copyright: 2,
                source: "source".into(),
                tags: vec![],
            }),
            pipeline: Default::default(),
            rooms: vec![],
        };
        let mut supervisor = supervisor_with(store.clone(), Arc::new(FakeUploader::new()), config);

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
        let mut supervisor = supervisor_with(store.clone(), uploader.clone(), test_app_config());

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Uploading;

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: std::path::PathBuf::from("missing.flv"),
                status: SegmentStatus::Finalized,
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
        let mut supervisor = supervisor_with(store.clone(), uploader.clone(), test_app_config());

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Uploading;

        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: std::path::PathBuf::from("ambiguous.flv"),
                status: SegmentStatus::Uploading,
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
                app_config: Arc::new(test_app_config()),
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
                app_config: Arc::new(test_app_config()),
            },
            rx,
        )
        .unwrap();

        assert_eq!(supervisor.active_session_id, Some(active_session_id));
    }

    #[tokio::test]
    async fn test_recording_invalid_segment_config() {
        use crate::state::store::StateStore;

        let store = std::sync::Arc::new(
            StateStore::open(tempfile::tempdir().unwrap().path().join("db")).unwrap(),
        );
        let mut config = crate::config::AppConfig {
            data: Default::default(),
            record: Default::default(),
            upload: Some(crate::config::UploadConfig {
                cookie_file: "test".into(),
                line: "auto".into(),
                threads: 1,
                submit_api: Default::default(),
                tid: 171,
                copyright: 2,
                source: "source".into(),
                tags: vec![],
            }),
            pipeline: Default::default(),
            rooms: vec![],
        };

        config.record.segment_time = Some("invalid_time".into());

        let mut supervisor =
            supervisor_with(store.clone(), Arc::new(FakeUploader::new()), config.clone());

        supervisor.session.state = PipelineState::Recording;
        supervisor.active_session_id = Some(put_recording_session(&store, 1));

        let err = supervisor.run_step().await.unwrap_err();
        assert!(matches!(err, AppError::Config(_)));

        config.record.segment_time = None;
        config.record.segment_size = Some("invalid_size".into());
        let mut supervisor2 = supervisor_with(store.clone(), Arc::new(FakeUploader::new()), config);
        supervisor2.session.state = PipelineState::Recording;
        supervisor2.active_session_id = Some(put_recording_session(&store, 1));

        let err2 = supervisor2.run_step().await.unwrap_err();
        assert!(matches!(err2, AppError::Config(_)));
    }
    #[tokio::test]
    async fn test_submitting_idempotent_submitted() {
        let db_dir = tempfile::tempdir().unwrap();
        let store =
            std::sync::Arc::new(StateStore::open(db_dir.path().join("state.redb")).unwrap());
        let uploader = std::sync::Arc::new(FakeUploader::new());
        let mut config =
            AppConfig::parse("[upload]\ncookie_file=\"cookies.json\"\ntid=1\nline=\"auto\"")
                .unwrap();
        config.upload.as_mut().unwrap().tid = 123;

        let mut supervisor = supervisor_with(store.clone(), uploader.clone(), config);

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        store
            .put_submission(&Submission {
                session_id,
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
    async fn test_submitting_uses_room_title_and_description_templates() {
        let db_dir = tempfile::tempdir().unwrap();
        let store = Arc::new(StateStore::open(db_dir.path().join("state.redb")).unwrap());
        let uploader = Arc::new(FakeUploader::new());
        let room_config = RoomConfig {
            name: "room-name".into(),
            url: "https://live.bilibili.com/1".into(),
            title: Some("Archive {title} #{room_id}".into()),
            description: Some("From {name}: {url}".into()),
        };
        let mut supervisor = supervisor_with_room(
            store.clone(),
            uploader.clone(),
            test_app_config(),
            room_config,
        );

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
        let mut supervisor = supervisor_with(store.clone(), uploader.clone(), test_app_config());

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        store
            .put_segment(&Segment {
                session_id,
                index: 0,
                path: std::path::PathBuf::from("ambiguous.flv"),
                status: SegmentStatus::Uploading,
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
        let mut config =
            AppConfig::parse("[upload]\ncookie_file=\"cookies.json\"\ntid=1\nline=\"auto\"")
                .unwrap();
        config.upload.as_mut().unwrap().tid = 123;

        let mut supervisor = supervisor_with(store.clone(), uploader.clone(), config);

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        store
            .put_submission(&Submission {
                session_id,
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
        let mut config =
            AppConfig::parse("[upload]\ncookie_file=\"cookies.json\"\ntid=1\nline=\"auto\"")
                .unwrap();
        config.upload.as_mut().unwrap().tid = 123;

        let mut supervisor = supervisor_with(store.clone(), uploader.clone(), config);

        let session_id = put_recording_session(&store, 1);
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Submitting;

        store
            .put_submission(&Submission {
                session_id,
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
}
