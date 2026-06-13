use std::sync::Arc;
use std::time::Instant;

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
use crate::pipeline::session::RoomStateMachine;
use crate::pipeline::state_machine::RoomState;
use crate::recorder::record_flv;
use crate::recorder::segment::{RecorderPolicy, SegmentFilter, SegmentLayout, SegmentPolicy};
use crate::state::model::{LiveSession, SessionStatus, SubmissionPlan};
use crate::state::store::StateStore;
use crate::submission_template::render_room_template;

pub struct RoomSupervisorDeps {
    pub store: Arc<StateStore>,
    pub client: Arc<BiliClient>,
}

fn recording_retry_reason(error: AppError) -> Result<String, AppError> {
    match error {
        AppError::Network(_)
        | AppError::Bilibili(_)
        | AppError::StreamProtocol(_)
        | AppError::StreamRepeatedData(_) => Ok(error.to_string()),
        AppError::Io { .. }
        | AppError::Config(_)
        | AppError::Database(_)
        | AppError::Table(_)
        | AppError::Transaction(_)
        | AppError::Storage(_)
        | AppError::Commit(_)
        | AppError::State(_)
        | AppError::GracefulShutdown => Err(error),
    }
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

pub struct RoomSupervisor {
    pub room_id: u64,
    pub session: RoomStateMachine,
    pub config: PipelineConfig,
    pub room_config: ResolvedRoomConfig,
    pub store: Arc<StateStore>,
    pub client: Arc<BiliClient>,
    pub active_session_id: Option<Uuid>,
    pub offline_since: Option<Instant>,
    reconnect_attempt: u32,
    pub shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl RoomSupervisor {
    pub fn new(
        room_id: u64,
        config: PipelineConfig,
        room_config: ResolvedRoomConfig,
        deps: RoomSupervisorDeps,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> AppResult<Self> {
        let expected_credentials = room_config.credentials();
        let mut supervisor = Self {
            room_id,
            session: RoomStateMachine::new(room_id),
            config,
            room_config,
            store: deps.store.clone(),
            client: deps.client,
            active_session_id: None,
            offline_since: None,
            reconnect_attempt: 0,
            shutdown_rx,
        };

        if let Some(room_state) = deps.store.get_room_state(room_id)? {
            supervisor.session.state = room_state.state;

            if room_state.state.requires_active_session() {
                let session_id = room_state.active_session_id.ok_or_else(|| {
                    AppError::State(format!(
                        "Persisted room state {:?} requires active_session_id",
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
                if session.status != SessionStatus::Recording {
                    return Err(AppError::State(format!(
                        "Persisted active session {} is {:?}, not Recording",
                        session_id, session.status
                    )));
                }
                validate_session_credentials(&session, &expected_credentials)?;
                supervisor.active_session_id = Some(session_id);
            }
            if room_state.state == RoomState::WaitingReconnect {
                supervisor.offline_since = Some(Instant::now());
            }
        }

        Ok(supervisor)
    }

    fn check_transition(&self, next: RoomState) -> AppResult<()> {
        self.check_transition_with_active_session(next, self.active_session_id)
    }

    fn check_transition_with_active_session(
        &self,
        next: RoomState,
        active_session_id: Option<Uuid>,
    ) -> AppResult<()> {
        if !self.session.state.can_transition_to(next) {
            return Err(AppError::State(format!(
                "Invalid room state transition from {:?} to {:?}",
                self.session.state, next
            )));
        }
        if next.requires_active_session() && active_session_id.is_none() {
            return Err(AppError::State(format!(
                "Room state {:?} requires an active session",
                next
            )));
        }
        Ok(())
    }

    fn apply_transition(&mut self, next: RoomState) -> AppResult<()> {
        self.check_transition(next)?;
        let prev = self.session.state;
        if !next.requires_active_session() {
            self.active_session_id = None;
        }
        if next == RoomState::WaitingReconnect && prev != RoomState::WaitingReconnect {
            self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
            self.offline_since.get_or_insert_with(Instant::now);
        }
        if matches!(
            next,
            RoomState::Recording
                | RoomState::Resolving
                | RoomState::Offline
                | RoomState::Failed
                | RoomState::Idle
        ) {
            self.reconnect_attempt = 0;
            self.offline_since = None;
        }
        self.session.state = next;
        info!(
            room_name = %self.room_config.name,
            room_id = self.room_id,
            from = ?prev,
            to = ?next,
            "Room state transition"
        );
        Ok(())
    }

    pub fn transition(&mut self, next: RoomState) -> AppResult<()> {
        self.check_transition(next)?;
        let active_session_id = if next.requires_active_session() {
            self.active_session_id
        } else {
            None
        };
        self.store
            .put_room_state(self.room_id, next, active_session_id)?;
        self.apply_transition(next)
    }

    fn transition_with_error(&mut self, next: RoomState, error: String) -> AppResult<()> {
        self.check_transition(next)?;
        let active_session_id = if next.requires_active_session() {
            self.active_session_id
        } else {
            None
        };
        self.store
            .put_room_state_with_error(self.room_id, next, active_session_id, error)?;
        self.apply_transition(next)
    }

    fn wait_reconnect_with_error(&mut self, reason: impl Into<String>) -> AppResult<()> {
        self.transition_with_error(RoomState::WaitingReconnect, reason.into())
    }

    fn handle_room_info_error(&mut self, error: AppError) -> AppResult<()> {
        let reason = format!("fetch_room_info failed: {error}");
        warn!(
            room_name = %self.room_config.name,
            room_id = self.room_id,
            "Failed to fetch room info: {}",
            error
        );
        if self.session.state == RoomState::Resolving {
            self.transition_with_error(RoomState::Offline, reason)
        } else {
            self.wait_reconnect_with_error(reason)
        }
    }

    pub fn reconnect_delay(&self) -> std::time::Duration {
        let base = std::time::Duration::from_secs(self.config.backoff_s);
        let max = std::time::Duration::from_secs(self.config.max_backoff_s);
        let exponent = self.reconnect_attempt.saturating_sub(1).min(31);
        let factor = 1_u32 << exponent;
        base.saturating_mul(factor).min(max)
    }

    fn submission_plan_for_session(&self, session: &LiveSession) -> AppResult<SubmissionPlan> {
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
                    Some(session),
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
                    Some(session),
                    self.room_id,
                )
            })
            .transpose()?
            .unwrap_or_default();
        let submit = &self.room_config.submit;

        Ok(SubmissionPlan {
            session_id: session.id,
            upload_credential: self.room_config.upload.credential.clone(),
            submit_api: self.room_config.upload.submit_api.clone(),
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
            delete_after_submit: self.room_config.record.delete_after_submit,
        })
    }

    async fn step_resolving(&mut self) -> AppResult<()> {
        match fetch_room_info(&self.client, self.room_id).await {
            Ok(info) => {
                if info.live_status == LiveStatus::Live {
                    if self.session.state == RoomState::Resolving {
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
                        let submission_plan = self.submission_plan_for_session(&live_session)?;

                        self.check_transition_with_active_session(
                            RoomState::Recording,
                            Some(session_id),
                        )?;
                        self.store.create_recording_session(
                            &live_session,
                            &submission_plan,
                            self.room_id,
                        )?;
                        self.active_session_id = Some(session_id);
                        self.apply_transition(RoomState::Recording)?;
                    } else {
                        self.transition(RoomState::Recording)?;
                    }
                } else if self.session.state == RoomState::Resolving {
                    self.transition(RoomState::Offline)?;
                } else {
                    self.transition(RoomState::WaitingReconnect)?;
                }
            }
            Err(e) => {
                self.handle_room_info_error(e)?;
            }
        }
        Ok(())
    }

    async fn step_recording(&mut self) -> AppResult<()> {
        let active_session = self
            .active_session_id
            .ok_or_else(|| AppError::State("Recording state requires active_session_id".into()))?;

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
            match fetch_play_info(&self.client, self.room_id, self.room_config.record.qn).await {
                Ok(info) => info,
                Err(e) => {
                    let reason = format!("fetch_play_info failed: {e}");
                    warn!(
                        room_name = %self.room_config.name,
                        room_id = self.room_id,
                        session_id = %active_session,
                        "fetch_play_info failed: {}",
                        e
                    );
                    self.wait_reconnect_with_error(reason)?;
                    return Ok(());
                }
            };

        let candidates = match parse_stream_candidates(&play_info) {
            Ok(c) => c,
            Err(e) => {
                let reason = format!("parse_stream_candidates failed: {e}");
                warn!(
                    room_name = %self.room_config.name,
                    room_id = self.room_id,
                    session_id = %active_session,
                    "parse_stream_candidates failed: {}",
                    e
                );
                self.wait_reconnect_with_error(reason)?;
                return Ok(());
            }
        };

        let cand = match select_healthy_stream_candidate(
            &candidates,
            &self.room_config.record,
            &self.client,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                let reason = format!("select_healthy_stream_candidate failed: {e}");
                warn!(
                    room_name = %self.room_config.name,
                    room_id = self.room_id,
                    session_id = %active_session,
                    "select_healthy_stream_candidate failed: {}",
                    e
                );
                self.wait_reconnect_with_error(reason)?;
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
                let reason = format!("stream connect failed: {e}");
                warn!(
                    room_name = %self.room_config.name,
                    room_id = self.room_id,
                    session_id = %active_session,
                    "stream connect failed: {}",
                    e
                );
                self.wait_reconnect_with_error(reason)?;
                return Ok(());
            }
        };

        let start_index = self
            .store
            .list_segments(active_session)?
            .iter()
            .map(|s| s.index)
            .max()
            .map_or(1, |idx| idx + 1);

        info!(
            room_name = %self.room_config.name,
            room_id = self.room_id,
            session_id = %active_session,
            start_index,
            "Starting record_flv"
        );
        match record_flv(
            resp,
            active_session,
            policy,
            &self.store,
            None,
            start_index,
            self.shutdown_rx.clone(),
        )
        .await
        {
            Ok(_) => {
                info!(
                    room_name = %self.room_config.name,
                    room_id = self.room_id,
                    session_id = %active_session,
                    "record_flv completed gracefully"
                );
                self.transition(RoomState::WaitingReconnect)?;
            }
            Err(AppError::GracefulShutdown) => {
                info!(
                    room_name = %self.room_config.name,
                    room_id = self.room_id,
                    session_id = %active_session,
                    "record_flv interrupted by graceful shutdown"
                );
                return Err(AppError::GracefulShutdown);
            }
            Err(e) => match recording_retry_reason(e) {
                Ok(reason) => {
                    warn!(
                        room_name = %self.room_config.name,
                        room_id = self.room_id,
                        session_id = %active_session,
                        "record_flv retryable error: {}",
                        reason
                    );
                    self.wait_reconnect_with_error(reason)?;
                }
                Err(e) => {
                    error!(
                        room_name = %self.room_config.name,
                        room_id = self.room_id,
                        session_id = %active_session,
                        "record_flv fatal error: {}",
                        e
                    );
                    self.transition(RoomState::Failed)?;
                    return Err(e);
                }
            },
        }
        Ok(())
    }

    fn step_waiting_reconnect(&mut self) -> AppResult<()> {
        let since = self.offline_since.get_or_insert_with(Instant::now);

        if since.elapsed().as_secs() > self.config.offline_grace_s {
            let active_session = self.active_session_id.ok_or_else(|| {
                AppError::State("WaitingReconnect state requires active_session_id".into())
            })?;
            info!(
                room_name = %self.room_config.name,
                room_id = self.room_id,
                session_id = %active_session,
                "Offline grace period expired; finalizing session and releasing room"
            );
            self.check_transition(RoomState::Idle)?;
            self.store
                .finalize_session_and_release_room(self.room_id, active_session)?;
            self.apply_transition(RoomState::Idle)?;
        } else {
            self.transition(RoomState::ReResolving)?;
        }
        Ok(())
    }

    pub async fn run_step(&mut self) -> AppResult<()> {
        match self.session.state {
            RoomState::Idle => {
                self.transition(RoomState::Resolving)?;
            }
            RoomState::Resolving | RoomState::ReResolving => {
                self.step_resolving().await?;
            }
            RoomState::Offline => {
                self.transition(RoomState::Idle)?;
            }
            RoomState::Recording => {
                self.step_recording().await?;
            }
            RoomState::WaitingReconnect => {
                self.step_waiting_reconnect()?;
            }
            RoomState::Failed => {
                return Err(AppError::State(
                    "Room is in Failed state and requires recovery or manual intervention.".into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        Copyright, ResolvedRecordConfig, ResolvedRoomUploadConfig, ResolvedSubmitConfig,
    };
    use crate::credential::CredentialIdentity;
    use tempfile::TempDir;

    fn test_room_config(dir: &std::path::Path) -> ResolvedRoomConfig {
        let upload_credential = CredentialIdentity::new("main", dir.join("cookies.json"));
        ResolvedRoomConfig {
            name: "room".into(),
            url: "https://live.bilibili.com/1".into(),
            record: ResolvedRecordConfig {
                credential: None,
                output_dir: dir.join("recordings"),
                segment_time: None,
                segment_size: None,
                min_segment_size: 0,
                qn: 10_000,
                cdn: Vec::new(),
                delete_after_submit: false,
            },
            upload: ResolvedRoomUploadConfig {
                credential: upload_credential,
                line: "bda2".into(),
                threads: 3,
                submit_api: crate::config::SubmitApi::App,
            },
            submit: ResolvedSubmitConfig {
                title: None,
                description: None,
                category_id: 171,
                copyright: Copyright::Reprint,
                source: "live recording".into(),
                tags: vec!["直播录像".into()],
                private: false,
                dynamic: String::new(),
                forbid_reprint: false,
                charging_panel: false,
                close_reply: false,
                close_danmu: false,
                featured_reply: false,
            },
        }
    }

    fn test_supervisor(store: Arc<StateStore>, dir: &std::path::Path) -> RoomSupervisor {
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        RoomSupervisor::new(
            1,
            PipelineConfig {
                poll_interval_s: 1,
                offline_grace_s: 0,
                backoff_s: 1,
                max_backoff_s: 1,
            },
            test_room_config(dir),
            RoomSupervisorDeps {
                store,
                client: Arc::new(BiliClient::new(None).unwrap()),
            },
            shutdown_rx,
        )
        .unwrap()
    }

    #[test]
    fn waiting_reconnect_grace_expiry_finalizes_session_and_releases_room() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::open(dir.path().join("state.redb")).unwrap());
        let mut supervisor = test_supervisor(store.clone(), dir.path());
        let session_id = Uuid::new_v4();
        let session = LiveSession {
            id: session_id,
            room_key: "1".into(),
            title: "title".into(),
            started_at: jiff::Timestamp::now(),
            status: SessionStatus::Recording,
            record_credential: None,
            upload_credential: Some(supervisor.room_config.upload.credential.clone()),
        };
        let plan = supervisor.submission_plan_for_session(&session).unwrap();
        store.create_recording_session(&session, &plan, 1).unwrap();
        supervisor.session.state = RoomState::WaitingReconnect;
        supervisor.active_session_id = Some(session_id);
        supervisor.offline_since = Some(Instant::now() - std::time::Duration::from_secs(2));

        supervisor.step_waiting_reconnect().unwrap();

        assert_eq!(supervisor.session.state, RoomState::Idle);
        assert_eq!(supervisor.active_session_id, None);
        assert_eq!(
            store.get_session(session_id).unwrap().unwrap().status,
            SessionStatus::Finalized
        );
        let room_state = store.get_room_state(1).unwrap().unwrap();
        assert_eq!(room_state.state, RoomState::Idle);
        assert_eq!(room_state.active_session_id, None);
    }

    #[test]
    fn active_room_state_requires_persisted_session() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::open(dir.path().join("state.redb")).unwrap());
        store
            .put_room_state(1, RoomState::Recording, Some(Uuid::new_v4()))
            .unwrap();
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let err = match RoomSupervisor::new(
            1,
            PipelineConfig::default(),
            test_room_config(dir.path()),
            RoomSupervisorDeps {
                store,
                client: Arc::new(BiliClient::new(None).unwrap()),
            },
            shutdown_rx,
        ) {
            Ok(_) => panic!("expected missing active session error"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn active_room_state_requires_recording_session() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::open(dir.path().join("state.redb")).unwrap());
        let upload_credential = CredentialIdentity::new("main", dir.path().join("cookies.json"));
        let session_id = Uuid::new_v4();
        store
            .put_session(&LiveSession {
                id: session_id,
                room_key: "1".into(),
                title: "title".into(),
                started_at: jiff::Timestamp::now(),
                status: SessionStatus::Finalized,
                record_credential: None,
                upload_credential: Some(upload_credential),
            })
            .unwrap();
        store
            .put_room_state(1, RoomState::Recording, Some(session_id))
            .unwrap();
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let err = match RoomSupervisor::new(
            1,
            PipelineConfig::default(),
            test_room_config(dir.path()),
            RoomSupervisorDeps {
                store,
                client: Arc::new(BiliClient::new(None).unwrap()),
            },
            shutdown_rx,
        ) {
            Ok(_) => panic!("expected finalized active session error"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("not Recording"));
    }
}
