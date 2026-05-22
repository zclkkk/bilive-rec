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
use crate::config::{AppConfig, PipelineConfig};
use crate::error::{AppError, AppResult};
use crate::pipeline::session::PipelineSession;
use crate::pipeline::state_machine::PipelineState;
use crate::recorder::segment::SegmentPolicy;
use crate::recorder::{record_flv, segment::SegmentEvent};
use crate::state::model::{
    LiveSession, SegmentStatus, SessionStatus, Submission, SubmissionStatus,
};
use crate::state::store::StateStore;
use crate::uploader::types::{SubmissionRequest, UploadRequest, Uploader};

fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim().to_uppercase();
    let mut num_str = s.clone();
    let mut multiplier = 1;
    if s.ends_with("GIB") {
        num_str = s.trim_end_matches("GIB").to_string();
        multiplier = 1024 * 1024 * 1024;
    } else if s.ends_with("GB") {
        num_str = s.trim_end_matches("GB").to_string();
        multiplier = 1024 * 1024 * 1024;
    } else if s.ends_with("MIB") {
        num_str = s.trim_end_matches("MIB").to_string();
        multiplier = 1024 * 1024;
    } else if s.ends_with("MB") {
        num_str = s.trim_end_matches("MB").to_string();
        multiplier = 1024 * 1024;
    } else if s.ends_with("KIB") {
        num_str = s.trim_end_matches("KIB").to_string();
        multiplier = 1024;
    } else if s.ends_with("KB") {
        num_str = s.trim_end_matches("KB").to_string();
        multiplier = 1024;
    } else if s.ends_with('B') {
        num_str = s.trim_end_matches('B').to_string();
    }
    num_str.trim().parse::<u64>().ok().map(|n| n * multiplier)
}

fn parse_duration(s: &str) -> Option<std::time::Duration> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() == 3 {
        let h: u64 = parts[0].parse().ok()?;
        let m: u64 = parts[1].parse().ok()?;
        let sec: u64 = parts[2].parse().ok()?;
        Some(std::time::Duration::from_secs(h * 3600 + m * 60 + sec))
    } else {
        None
    }
}

pub struct RoomSupervisor<U: Uploader + Send + Sync + 'static> {
    pub room_id: u64,
    pub session: PipelineSession,
    pub config: PipelineConfig,
    pub store: Option<Arc<StateStore>>,
    pub client: Option<Arc<BiliClient>>,
    pub uploader: Option<Arc<U>>,
    pub active_session_id: Option<Uuid>,
    pub upload_tasks: Vec<JoinHandle<AppResult<()>>>,
    pub offline_since: Option<Instant>,
    pub app_config: Option<Arc<AppConfig>>,
}

impl<U: Uploader + Send + Sync + 'static> RoomSupervisor<U> {
    pub fn new(
        room_id: u64,
        config: PipelineConfig,
        store: Option<Arc<StateStore>>,
        client: Option<Arc<BiliClient>>,
        uploader: Option<Arc<U>>,
        app_config: Option<Arc<AppConfig>>,
    ) -> Self {
        Self {
            room_id,
            session: PipelineSession::new(room_id),
            config,
            store,
            client,
            uploader,
            active_session_id: None,
            upload_tasks: Vec::new(),
            offline_since: None,
            app_config,
        }
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

        if let Some(store) = &self.store {
            store.put_pipeline_state(self.room_id, next)?;
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
                if let Some(client) = &self.client {
                    match fetch_room_info(client, self.room_id).await {
                        Ok(info) => {
                            if info.live_status == LiveStatus::Live {
                                // Stream is live
                                if self.session.state == PipelineState::Resolving {
                                    // Start a brand new session
                                    let session_id = Uuid::new_v4();
                                    self.active_session_id = Some(session_id);

                                    if let Some(store) = &self.store {
                                        let live_session = LiveSession {
                                            id: session_id,
                                            room_key: self.room_id.to_string(),
                                            title: info.title.clone(),
                                            started_at: jiff::Timestamp::now(),
                                            status: SessionStatus::Recording,
                                        };
                                        store.put_session(&live_session)?;
                                    }
                                }
                                self.offline_since = None;
                                self.transition(PipelineState::Recording)?;
                            } else {
                                // Room is not live
                                if self.session.state == PipelineState::Resolving {
                                    self.transition(PipelineState::Offline)?;
                                } else {
                                    // It was ReResolving (reconnecting) and found offline
                                    self.transition(PipelineState::WaitingReconnect)?;
                                }
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
                } else {
                    // Test stubs
                    if self.session.state == PipelineState::Resolving {
                        self.transition(PipelineState::Recording)?;
                    } else {
                        self.transition(PipelineState::WaitingReconnect)?;
                    }
                }
            }
            PipelineState::Offline => {
                self.transition(PipelineState::Idle)?;
            }
            PipelineState::Recording => {
                if let (
                    Some(client),
                    Some(store),
                    Some(active_session),
                    Some(app_config),
                    Some(uploader),
                ) = (
                    &self.client,
                    &self.store,
                    self.active_session_id,
                    &self.app_config,
                    &self.uploader,
                ) {
                    let play_info =
                        match fetch_play_info(client, self.room_id, app_config.record.qn).await {
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
                        &app_config.record,
                        client,
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

                    let req = client
                        .client()
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
                    let segments = store.list_segments(active_session)?;
                    if let Some(max_idx) = segments.iter().map(|s| s.index).max() {
                        start_index = max_idx + 1;
                    }

                    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<SegmentEvent>();
                    let uploader_clone = uploader.clone();
                    let store_clone = store.clone();

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
                                    let req = UploadRequest {
                                        session_id,
                                        segment_index: index,
                                        path: path.clone(),
                                        part_title: format!("Part {}", index),
                                    };
                                    match uploader_clone.upload_segment(req).await {
                                        Ok(uploaded_part) => {
                                            info!("Upload success for idx={}", index);
                                            if let Err(e) =
                                                store_clone.put_uploaded_part(&uploaded_part)
                                            {
                                                error!("Failed to persist UploadedPart: {}", e);
                                                return Err(e);
                                            }
                                        }
                                        Err(e) => {
                                            error!(
                                                "Upload segment failed for idx={}: {}",
                                                index, e
                                            );
                                            return Err(e);
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

                    let min_segment_size = parse_size(&app_config.record.min_segment_size)
                        .ok_or_else(|| {
                            AppError::Config(format!(
                                "Invalid min_segment_size: {}",
                                app_config.record.min_segment_size
                            ))
                        })?;

                    let policy = SegmentPolicy {
                        output_dir: app_config.record.output_dir.clone(),
                        segment_time: app_config
                            .record
                            .segment_time
                            .as_ref()
                            .and_then(|s| parse_duration(s)),
                        segment_size: app_config
                            .record
                            .segment_size
                            .as_ref()
                            .and_then(|s| parse_size(s)),
                        min_segment_size,
                    };

                    info!("Starting record_flv from index {}", start_index);
                    match record_flv(resp, active_session, policy, store, event_tx, start_index)
                        .await
                    {
                        Ok(_) => {
                            info!("record_flv completed gracefully");
                            self.transition(PipelineState::WaitingReconnect)?;
                        }
                        Err(e) => {
                            warn!("record_flv failed: {}", e);
                            self.transition(PipelineState::WaitingReconnect)?;
                        }
                    }
                } else {
                    return Err(AppError::State(
                        "Missing required components for Recording".into(),
                    ));
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
                if self.store.is_none()
                    || self.active_session_id.is_none()
                    || self.uploader.is_none()
                {
                    return Err(AppError::State(
                        "Missing required components for Uploading".into(),
                    ));
                }

                // Join all tasks and verify success
                let tasks = std::mem::take(&mut self.upload_tasks);
                let mut has_upload_errors = false;

                for task in tasks {
                    match task.await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            error!("Background upload task failed: {}", e);
                            has_upload_errors = true;
                        }
                        Err(e) => {
                            error!("Background upload task panicked: {}", e);
                            has_upload_errors = true;
                        }
                    }
                }

                // Reconcile missing uploads from store
                if let (Some(store), Some(active_session), Some(uploader)) =
                    (&self.store, self.active_session_id, &self.uploader)
                {
                    let segments = store.list_segments(active_session)?;
                    let uploaded_parts = store.list_uploaded_parts(active_session)?;

                    let uploaded_indices: std::collections::HashSet<u32> = uploaded_parts
                        .into_iter()
                        .map(|p| p.segment_index)
                        .collect();

                    for seg in segments {
                        if seg.status == SegmentStatus::Finalized
                            && !uploaded_indices.contains(&seg.index)
                        {
                            info!("Reconciling upload for segment index {}", seg.index);
                            let req = UploadRequest {
                                session_id: active_session,
                                segment_index: seg.index,
                                path: seg.path.clone(),
                                part_title: format!("Part {}", seg.index),
                            };
                            match uploader.upload_segment(req).await {
                                Ok(part) => {
                                    if let Err(e) = store.put_uploaded_part(&part) {
                                        error!("Failed to persist reconciled UploadedPart: {}", e);
                                        has_upload_errors = true;
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Reconciled upload failed for index {}: {}",
                                        seg.index, e
                                    );
                                    has_upload_errors = true;
                                }
                            }
                        }
                    }
                }

                if has_upload_errors {
                    self.transition(PipelineState::Failed)?;
                } else {
                    self.transition(PipelineState::Submitting)?;
                }
            }
            PipelineState::Submitting => {
                if self.store.is_none()
                    || self.active_session_id.is_none()
                    || self.app_config.is_none()
                    || self.uploader.is_none()
                {
                    return Err(AppError::State(
                        "Missing required components for Submitting".into(),
                    ));
                }

                if let (Some(store), Some(active_session), Some(app_config), Some(uploader)) = (
                    &self.store,
                    self.active_session_id,
                    &self.app_config,
                    &self.uploader,
                ) {
                    // Mark LiveSession as finalized
                    if let Some(mut session) = store.get_session(active_session)? {
                        session.status = SessionStatus::Finalized;
                        store.put_session(&session)?;
                    }

                    let mut parts = store.list_uploaded_parts(active_session)?;
                    parts.sort_by_key(|p| p.segment_index);

                    if parts.is_empty() {
                        let sub = Submission {
                            session_id: active_session,
                            status: SubmissionStatus::Failed,
                            aid: None,
                            bvid: None,
                            error: Some("No parts to submit".into()),
                        };
                        store.put_submission(&sub)?;
                        self.transition(PipelineState::Failed)?;
                        return Err(AppError::State("No parts to submit".into()));
                    }

                    let req = SubmissionRequest {
                        title: "直播录像".to_string(), // Need template in Phase 5C or later
                        description: "".to_string(),
                        tid: app_config.upload.tid,
                        copyright: app_config.upload.copyright,
                        tags: app_config.upload.tags.clone(),
                        source: app_config.upload.source.clone(),
                        parts,
                    };

                    let mut sub = Submission {
                        session_id: active_session,
                        status: SubmissionStatus::Pending,
                        aid: None,
                        bvid: None,
                        error: None,
                    };
                    store.put_submission(&sub)?;

                    match uploader.submit(req).await {
                        Ok(res) => {
                            sub.status = SubmissionStatus::Submitted;
                            sub.aid = res.aid;
                            sub.bvid = res.bvid;
                            store.put_submission(&sub)?;
                            self.transition(PipelineState::Submitted)?;
                        }
                        Err(e) => {
                            sub.status = SubmissionStatus::Failed;
                            sub.error = Some(e.to_string());
                            store.put_submission(&sub)?;
                            self.transition(PipelineState::Failed)?;
                        }
                    }
                }
            }
            PipelineState::Submitted => {
                self.active_session_id = None;
                self.offline_since = None;
                self.transition(PipelineState::Idle)?;
            }
            PipelineState::Failed => {
                self.active_session_id = None;
                self.offline_since = None;
                self.transition(PipelineState::Idle)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::model::UploadedPart;
    use crate::uploader::types::SubmissionResult;

    struct FakeUploader;
    impl Uploader for FakeUploader {
        async fn check_login(&self) -> AppResult<()> {
            Ok(())
        }
        async fn upload_segment(&self, req: UploadRequest) -> AppResult<UploadedPart> {
            Ok(UploadedPart {
                session_id: req.session_id,
                segment_index: req.segment_index,
                bili_filename: "fake_file".to_string(),
                part_title: req.part_title,
            })
        }
        async fn submit(&self, _req: SubmissionRequest) -> AppResult<SubmissionResult> {
            Ok(SubmissionResult {
                aid: Some(1),
                bvid: Some("bv1".to_string()),
            })
        }
    }

    fn mock_supervisor() -> RoomSupervisor<FakeUploader> {
        RoomSupervisor::new(1, PipelineConfig::default(), None, None, None, None)
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
    fn test_parse_size_variations() {
        assert_eq!(parse_size("20MiB"), Some(20 * 1024 * 1024));
        assert_eq!(parse_size("2GiB"), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("10MB"), Some(10 * 1024 * 1024));
        assert_eq!(parse_size("15KB"), Some(15 * 1024));
        assert_eq!(parse_size("invalid"), None);
    }

    #[tokio::test]
    async fn test_submitting_with_empty_parts() {
        use crate::config::PipelineConfig;
        use crate::state::store::StateStore;

        let store = std::sync::Arc::new(
            StateStore::open(tempfile::tempdir().unwrap().path().join("db")).unwrap(),
        );
        let config = crate::config::AppConfig {
            data: Default::default(),
            record: Default::default(),
            upload: crate::config::UploadConfig {
                cookie_file: "test".into(),
                line: "auto".into(),
                threads: 1,
                submit_api: Default::default(),
                tid: 171,
                copyright: 2,
                source: "source".into(),
                tags: vec![],
            },
            pipeline: Default::default(),
            rooms: vec![],
        };
        let mut supervisor = RoomSupervisor::new(
            1,
            PipelineConfig::default(),
            Some(store.clone()),
            None,
            Some(std::sync::Arc::new(FakeUploader)),
            Some(std::sync::Arc::new(config)),
        );

        // Setup session
        let session_id = uuid::Uuid::new_v4();
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
        use crate::config::PipelineConfig;
        use crate::state::model::{Segment, SegmentStatus};
        use crate::state::store::StateStore;

        let store = std::sync::Arc::new(
            StateStore::open(tempfile::tempdir().unwrap().path().join("db")).unwrap(),
        );
        let config = crate::config::AppConfig {
            data: Default::default(),
            record: Default::default(),
            upload: crate::config::UploadConfig {
                cookie_file: "test".into(),
                line: "auto".into(),
                threads: 1,
                submit_api: Default::default(),
                tid: 171,
                copyright: 2,
                source: "source".into(),
                tags: vec![],
            },
            pipeline: Default::default(),
            rooms: vec![],
        };
        let mut supervisor = RoomSupervisor::new(
            1,
            PipelineConfig::default(),
            Some(store.clone()),
            None,
            Some(std::sync::Arc::new(FakeUploader)),
            Some(std::sync::Arc::new(config)),
        );

        let session_id = uuid::Uuid::new_v4();
        supervisor.active_session_id = Some(session_id);
        supervisor.session.state = PipelineState::Uploading;

        // Add a finalized segment with no uploaded part
        store
            .put_segment(&Segment {
                session_id,
                index: 1,
                path: std::path::PathBuf::from("test.flv"),
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
    }

    #[tokio::test]
    async fn test_recording_missing_components() {
        let mut supervisor = mock_supervisor();
        supervisor.session.state = PipelineState::Recording;

        let err = supervisor.run_step().await.unwrap_err();
        assert!(matches!(err, AppError::State(_)));
        assert_eq!(supervisor.session.state, PipelineState::Recording);
    }
}
