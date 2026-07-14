use std::sync::Arc;
use tokio::time::Instant;

use tracing::{error, info, warn};
use uuid::Uuid;

use crate::bilibili::client::BiliClient;
use crate::bilibili::room::fetch_room_info;
use crate::bilibili::stream::{
    fetch_play_info, parse_stream_candidates, select_healthy_stream_candidate,
};
use crate::bilibili::types::LiveStatus;
use crate::config::{PipelineConfig, ResolvedRecordConfig, ResolvedRoomConfig, ResolvedRoomOutput};
use crate::error::{AppError, AppResult};
use crate::pipeline::state_machine::RoomState;
use crate::recorder::record_flv;
use crate::recorder::segment::{RecorderPolicy, SegmentFilter, SegmentLayout, SegmentPolicy};
use crate::state::model::{
    LiveSession, OutputPlan, RecordingPlan, RoomLifecycle, SessionLifecycle, SubmissionSpec,
    UploadPlan,
};
use crate::state::store::StateStore;
use crate::state::transitions::{self, CloseSessionRequest};
use crate::submission_template::render_room_template;

pub struct RoomSupervisorDeps {
    pub store: Arc<StateStore>,
    pub client: Arc<BiliClient>,
}

fn recording_retry_reason(error: AppError) -> Result<String, AppError> {
    match error {
        AppError::Network(_)
        | AppError::Bilibili(_)
        | AppError::BilibiliResponse(_)
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
        | AppError::RecoveryRequired(_)
        | AppError::GracefulShutdown => Err(error),
    }
}

pub struct RoomSupervisor {
    pub room_id: u64,
    pub state: RoomState,
    pub config: PipelineConfig,
    pub room_config: ResolvedRoomConfig,
    pub store: Arc<StateStore>,
    configured_client: Arc<BiliClient>,
    active_session: Option<ActiveSessionContext>,
    pub offline_since: Option<Instant>,
    reconnect_attempt: u32,
    next_reconnect_probe: Option<Instant>,
    pub shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

struct ActiveSessionContext {
    id: Uuid,
    recording_plan: RecordingPlan,
    client: Arc<BiliClient>,
}

impl RoomSupervisor {
    pub fn new(
        room_id: u64,
        config: PipelineConfig,
        room_config: ResolvedRoomConfig,
        deps: RoomSupervisorDeps,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> AppResult<Self> {
        let mut supervisor = Self {
            room_id,
            state: RoomState::Idle,
            config,
            room_config,
            store: deps.store.clone(),
            configured_client: deps.client,
            active_session: None,
            offline_since: None,
            reconnect_attempt: 0,
            next_reconnect_probe: None,
            shutdown_rx,
        };

        if let Some(room_state) = deps.store.get_room_state(room_id)? {
            match room_state.lifecycle {
                RoomLifecycle::Ready => {
                    supervisor.state = RoomState::Idle;
                }
                RoomLifecycle::Owned { session_id } => {
                    let session = deps.store.get_session(session_id)?.ok_or_else(|| {
                        AppError::State(format!(
                            "Persisted active session {} for room {} does not exist",
                            session_id, room_id
                        ))
                    })?;
                    if session.room_id != room_id {
                        return Err(AppError::State(format!(
                            "Persisted active session {} belongs to room {}, not {}",
                            session_id, session.room_id, room_id
                        )));
                    }
                    if session.lifecycle != SessionLifecycle::Open {
                        return Err(AppError::State(format!(
                            "Persisted active session {} is {:?}, not Open",
                            session_id, session.lifecycle
                        )));
                    }
                    let client = Arc::new(BiliClient::from_optional_cookie_file(
                        session
                            .recording_plan
                            .credential
                            .as_ref()
                            .map(crate::credential::CredentialRef::cookie_file),
                    )?);
                    supervisor.active_session = Some(ActiveSessionContext {
                        id: session_id,
                        recording_plan: session.recording_plan,
                        client,
                    });
                    supervisor.state = RoomState::Recording;
                }
                RoomLifecycle::Blocked { session_id } => {
                    let session = deps.store.get_session(session_id)?.ok_or_else(|| {
                        AppError::State(format!(
                            "Persisted blocked session {session_id} for room {room_id} does not exist"
                        ))
                    })?;
                    if session.room_id != room_id
                        || !matches!(session.lifecycle, SessionLifecycle::RecoveryRequired { .. })
                    {
                        return Err(AppError::State(format!(
                            "Persisted blocked room {room_id} points to inconsistent session {session_id} ({:?})",
                            session.lifecycle
                        )));
                    }
                    supervisor.state = RoomState::Failed;
                }
            }
        }

        Ok(supervisor)
    }

    fn check_transition(&self, next: RoomState) -> AppResult<()> {
        self.check_transition_with_active_session(
            next,
            self.active_session.as_ref().map(|active| active.id),
        )
    }

    fn check_transition_with_active_session(
        &self,
        next: RoomState,
        active_session_id: Option<Uuid>,
    ) -> AppResult<()> {
        if !self.state.can_transition_to(next) {
            return Err(AppError::State(format!(
                "Invalid room state transition from {:?} to {:?}",
                self.state, next
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
        let prev = self.state;
        if !next.requires_active_session() {
            self.active_session = None;
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
            self.next_reconnect_probe = None;
        }
        self.state = next;
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
        self.apply_transition(next)
    }

    fn transition_with_error(&mut self, next: RoomState, error: String) -> AppResult<()> {
        self.check_transition(next)?;
        warn!(
            room_name = %self.room_config.name,
            room_id = self.room_id,
            "transient room error: {error}"
        );
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
        if self.state == RoomState::Resolving {
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

    fn reconnect_wait(&self, now: Instant) -> std::time::Duration {
        let elapsed = self
            .offline_since
            .map_or(std::time::Duration::ZERO, |since| {
                now.saturating_duration_since(since)
            });
        let remaining =
            std::time::Duration::from_secs(self.config.offline_grace_s).saturating_sub(elapsed);
        let retry_delay = self
            .next_reconnect_probe
            .map_or(std::time::Duration::ZERO, |probe_at| {
                probe_at.saturating_duration_since(now)
            });
        if remaining.is_zero() {
            retry_delay
        } else {
            let scheduled = if retry_delay.is_zero() {
                self.reconnect_delay()
            } else {
                retry_delay
            };
            scheduled.min(remaining)
        }
    }

    fn output_plan_for_session(&self, session: &LiveSession) -> AppResult<OutputPlan> {
        let ResolvedRoomOutput::Bilibili { upload, submit } = &self.room_config.output else {
            return Ok(OutputPlan::LocalOnly);
        };
        let title = submit
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
        let description = submit
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
        let source = if submit.copyright == crate::config::Copyright::Reprint {
            render_room_template(
                &submit.source,
                &self.room_config.name,
                &self.room_config.url,
                Some(session),
                self.room_id,
            )?
        } else {
            String::new()
        };

        Ok(OutputPlan::Bilibili {
            upload: UploadPlan {
                principal: upload.principal.clone(),
                line: upload.line.clone(),
                threads: upload.threads,
                submit_api: upload.submit_api.clone(),
                delete_after_submit: upload.delete_after_submit,
            },
            submission: Box::new(SubmissionSpec {
                title,
                description,
                category_id: submit.category_id,
                copyright: submit.copyright,
                tags: submit.tags.clone(),
                source,
                private: submit.private,
                dynamic: submit.dynamic.clone(),
                forbid_reprint: submit.forbid_reprint,
                charging_panel: submit.charging_panel,
                close_reply: submit.close_reply,
                close_danmu: submit.close_danmu,
                featured_reply: submit.featured_reply,
            }),
        })
    }

    async fn step_resolving(&mut self) -> AppResult<()> {
        match fetch_room_info(&self.configured_client, self.room_id).await {
            Ok(info) => {
                if info.live_status == LiveStatus::Live {
                    if self.state == RoomState::Resolving {
                        let session_id = Uuid::new_v4();
                        let mut live_session = LiveSession {
                            id: session_id,
                            room_id: self.room_id,
                            room_name: self.room_config.name.clone(),
                            title: info.title.clone(),
                            started_at: jiff::Timestamp::now(),
                            lifecycle: SessionLifecycle::Open,
                            recording_plan: RecordingPlan {
                                credential: self.room_config.record.credential.clone(),
                                output_dir: self.room_config.record.output_dir.clone(),
                                segment_time_ms: self.room_config.record.segment_time.map(
                                    |value| value.as_millis().min(u128::from(u64::MAX)) as u64,
                                ),
                                segment_size: self.room_config.record.segment_size,
                                min_segment_size: self.room_config.record.min_segment_size,
                                qn: self.room_config.record.qn,
                                cdn: self.room_config.record.cdn.clone(),
                            },
                            output_plan: OutputPlan::LocalOnly,
                            recording_events: Vec::new(),
                        };
                        live_session.output_plan = self.output_plan_for_session(&live_session)?;

                        self.check_transition_with_active_session(
                            RoomState::Recording,
                            Some(session_id),
                        )?;
                        transitions::create_session(&self.store, &live_session)?;
                        self.active_session = Some(ActiveSessionContext {
                            id: session_id,
                            recording_plan: live_session.recording_plan.clone(),
                            client: self.configured_client.clone(),
                        });
                        self.apply_transition(RoomState::Recording)?;
                    } else {
                        self.transition(RoomState::Recording)?;
                    }
                } else {
                    match info.live_status {
                        LiveStatus::Offline | LiveStatus::RoundPlay => {
                            if self.state == RoomState::Resolving {
                                self.transition(RoomState::Offline)?;
                            } else {
                                self.transition(RoomState::WaitingReconnect)?;
                            }
                        }
                        LiveStatus::Unknown(code) => {
                            self.handle_room_info_error(AppError::Bilibili(format!(
                                "room {} returned unknown live status {code}",
                                self.room_id
                            )))?;
                        }
                        LiveStatus::Live => unreachable!(),
                    }
                }
            }
            Err(e) => {
                self.handle_room_info_error(e.into_app_error())?;
            }
        }
        Ok(())
    }

    async fn step_recording(&mut self) -> AppResult<()> {
        let active = self.active_session.as_ref().ok_or_else(|| {
            AppError::State("Recording state requires an active session context".into())
        })?;
        let active_session = active.id;
        let recording_plan = active.recording_plan.clone();
        let client = active.client.clone();
        let resolved_record = ResolvedRecordConfig {
            credential: recording_plan.credential.clone(),
            output_dir: recording_plan.output_dir.clone(),
            segment_time: recording_plan
                .segment_time_ms
                .map(std::time::Duration::from_millis),
            segment_size: recording_plan.segment_size,
            min_segment_size: recording_plan.min_segment_size,
            qn: recording_plan.qn,
            cdn: recording_plan.cdn.clone(),
        };
        let policy = RecorderPolicy {
            layout: SegmentLayout {
                output_dir: recording_plan.output_dir,
            },
            segment: SegmentPolicy {
                segment_time: resolved_record.segment_time,
                segment_size: resolved_record.segment_size,
            },
            filter: SegmentFilter {
                min_segment_size: resolved_record.min_segment_size,
            },
        };

        let play_info = match fetch_play_info(&client, self.room_id, resolved_record.qn).await {
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

        let cand =
            match select_healthy_stream_candidate(&candidates, &resolved_record, &client).await {
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

        let req = client
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
            Err(e)
                if self
                    .store
                    .list_segments(active_session)?
                    .iter()
                    .any(|segment| {
                        matches!(
                            segment.artifact,
                            crate::state::model::ArtifactState::Failed { .. }
                        )
                    }) =>
            {
                let reason = format!("recording produced a failed artifact: {e}");
                transitions::require_recovery(&self.store, active_session, reason)?;
                self.apply_transition(RoomState::Failed)?;
                return Err(e);
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
                    transitions::require_recovery(&self.store, active_session, e.to_string())?;
                    self.apply_transition(RoomState::Failed)?;
                    return Err(e);
                }
            },
        }
        Ok(())
    }

    async fn step_waiting_reconnect(&mut self) -> AppResult<()> {
        let since = self.offline_since.get_or_insert_with(Instant::now);
        let deadline_reached =
            since.elapsed() >= std::time::Duration::from_secs(self.config.offline_grace_s);
        let client = self
            .active_session
            .as_ref()
            .map(|active| active.client.clone())
            .ok_or_else(|| {
                AppError::State("WaitingReconnect state requires an active session context".into())
            })?;
        let status = match fetch_room_info(&client, self.room_id).await {
            Ok(info) => info.live_status,
            Err(error) => {
                warn!(
                    room_name = %self.room_config.name,
                    room_id = self.room_id,
                    "Reconnect probe failed: {error}"
                );
                self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
                self.next_reconnect_probe = Some(Instant::now() + self.reconnect_delay());
                return Ok(());
            }
        };
        if status == LiveStatus::Live {
            self.apply_transition(RoomState::Recording)?;
            return Ok(());
        }
        match status {
            LiveStatus::Offline | LiveStatus::RoundPlay if deadline_reached => {
                self.close_after_confirmed_offline()?;
            }
            LiveStatus::Offline | LiveStatus::RoundPlay => {
                self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
            }
            LiveStatus::Unknown(code) => {
                warn!(
                    room_name = %self.room_config.name,
                    room_id = self.room_id,
                    "Reconnect probe returned unknown live status {code}"
                );
                self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
                self.next_reconnect_probe = Some(Instant::now() + self.reconnect_delay());
            }
            LiveStatus::Live => unreachable!(),
        }
        Ok(())
    }

    fn close_after_confirmed_offline(&mut self) -> AppResult<()> {
        let active_session = self
            .active_session
            .as_ref()
            .map(|active| active.id)
            .ok_or_else(|| {
                AppError::State("WaitingReconnect state requires an active session context".into())
            })?;
        info!(
            room_name = %self.room_config.name,
            room_id = self.room_id,
            session_id = %active_session,
            "Offline grace period expired; finalizing session and releasing room"
        );
        self.check_transition(RoomState::Idle)?;
        let result = transitions::close_session(
            &self.store,
            active_session,
            CloseSessionRequest::Natural { note: None },
        )?;
        if matches!(result.lifecycle, SessionLifecycle::RecoveryRequired { .. }) {
            self.apply_transition(RoomState::Failed)?;
            return Err(AppError::RecoveryRequired(format!(
                "session {active_session} requires recovery before room monitoring can continue"
            )));
        }
        self.apply_transition(RoomState::Idle)?;
        Ok(())
    }

    pub async fn run_step(&mut self) -> AppResult<()> {
        match self.state {
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
                self.step_waiting_reconnect().await?;
            }
            RoomState::Failed => {
                return Err(AppError::RecoveryRequired(
                    "Room is in Failed state and requires recovery or manual intervention.".into(),
                ));
            }
        }
        Ok(())
    }

    pub async fn run(mut self) -> AppResult<()> {
        loop {
            if *self.shutdown_rx.borrow() {
                return Ok(());
            }

            if self.state == RoomState::WaitingReconnect {
                let delay = self.reconnect_wait(Instant::now());
                self.next_reconnect_probe = None;
                if !delay.is_zero() && self.sleep_or_shutdown(delay).await? {
                    return Ok(());
                }
            }

            self.run_step().await?;

            if self.state == RoomState::Offline
                && self
                    .sleep_or_shutdown(std::time::Duration::from_secs(self.config.poll_interval_s))
                    .await?
            {
                return Ok(());
            }
        }
    }

    async fn sleep_or_shutdown(&mut self, duration: std::time::Duration) -> AppResult<bool> {
        tokio::select! {
            _ = tokio::time::sleep(duration) => Ok(false),
            changed = self.shutdown_rx.changed() => {
                changed.map_err(|_| AppError::State("shutdown channel closed".into()))?;
                Ok(*self.shutdown_rx.borrow())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        Copyright, ResolvedRecordConfig, ResolvedRoomOutput, ResolvedRoomUploadConfig,
        ResolvedSubmitConfig,
    };
    use crate::credential::CredentialRef;
    use tempfile::TempDir;

    fn test_room_config(dir: &std::path::Path) -> ResolvedRoomConfig {
        let upload_credential = CredentialRef::new("main", dir.join("cookies.json"));
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
            },
            output: ResolvedRoomOutput::Bilibili {
                upload: ResolvedRoomUploadConfig {
                    principal: crate::credential::UploadPrincipal::new(upload_credential, 1),
                    line: "bda2".into(),
                    threads: 3,
                    submit_api: crate::config::SubmitApi::App,
                    delete_after_submit: false,
                },
                submit: Box::new(ResolvedSubmitConfig {
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
                }),
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

    fn test_session(supervisor: &RoomSupervisor) -> LiveSession {
        let record = &supervisor.room_config.record;
        let mut session = LiveSession {
            id: Uuid::new_v4(),
            room_id: supervisor.room_id,
            room_name: supervisor.room_config.name.clone(),
            title: "title".into(),
            started_at: jiff::Timestamp::now(),
            lifecycle: SessionLifecycle::Open,
            recording_plan: RecordingPlan {
                credential: record.credential.clone(),
                output_dir: record.output_dir.clone(),
                segment_time_ms: record
                    .segment_time
                    .map(|duration| duration.as_millis() as u64),
                segment_size: record.segment_size,
                min_segment_size: record.min_segment_size,
                qn: record.qn,
                cdn: record.cdn.clone(),
            },
            output_plan: OutputPlan::LocalOnly,
            recording_events: Vec::new(),
        };
        session.output_plan = supervisor.output_plan_for_session(&session).unwrap();
        session
    }

    #[test]
    fn waiting_reconnect_grace_expiry_finalizes_session_and_releases_room() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::create_or_open(dir.path().join("state.redb")).unwrap());
        let mut supervisor = test_supervisor(store.clone(), dir.path());
        let session = test_session(&supervisor);
        let session_id = session.id;
        transitions::create_session(&store, &session).unwrap();
        supervisor.state = RoomState::WaitingReconnect;
        supervisor.active_session = Some(ActiveSessionContext {
            id: session_id,
            recording_plan: session.recording_plan,
            client: supervisor.configured_client.clone(),
        });
        supervisor.offline_since = Some(Instant::now() - std::time::Duration::from_secs(2));

        supervisor.close_after_confirmed_offline().unwrap();

        assert_eq!(supervisor.state, RoomState::Idle);
        assert!(supervisor.active_session.is_none());
        assert!(matches!(
            store.get_session(session_id).unwrap().unwrap().lifecycle,
            SessionLifecycle::Closed {
                closure: crate::state::model::SessionClosure::NoUsableRecording { .. },
            }
        ));
        let room_state = store.get_room_state(1).unwrap().unwrap();
        assert_eq!(room_state.lifecycle, RoomLifecycle::Ready);
        assert_eq!(room_state.lifecycle.session_id(), None);
    }

    #[test]
    fn output_plan_freezes_rendered_source_template() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::create_or_open(dir.path().join("state.redb")).unwrap());
        let mut supervisor = test_supervisor(store, dir.path());
        let ResolvedRoomOutput::Bilibili { submit, .. } = &mut supervisor.room_config.output else {
            panic!("expected Bilibili output");
        };
        submit.source = "{url}".into();
        let session = test_session(&supervisor);
        let OutputPlan::Bilibili { submission, .. } = session.output_plan else {
            panic!("expected Bilibili output plan");
        };

        assert_eq!(submission.source, "https://live.bilibili.com/1");
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_sleep_never_crosses_the_offline_grace_deadline() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::create_or_open(dir.path().join("state.redb")).unwrap());
        let mut supervisor = test_supervisor(store, dir.path());
        supervisor.config.offline_grace_s = 60;
        supervisor.config.backoff_s = 60;
        supervisor.config.max_backoff_s = 60;
        supervisor.reconnect_attempt = 3;
        supervisor.offline_since = Some(Instant::now());

        tokio::time::advance(std::time::Duration::from_secs(45)).await;

        assert_eq!(
            supervisor.reconnect_wait(Instant::now()),
            std::time::Duration::from_secs(15)
        );
        tokio::time::advance(std::time::Duration::from_secs(15)).await;
        assert!(supervisor.reconnect_wait(Instant::now()).is_zero());
    }

    #[test]
    fn resumed_session_client_is_dropped_when_that_session_closes() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(StateStore::create_or_open(dir.path().join("state.redb")).unwrap());
        let old_cookie = dir.path().join("old-cookie.txt");
        std::fs::write(&old_cookie, "SESSDATA=old").unwrap();
        let old_session = LiveSession {
            id: Uuid::new_v4(),
            room_id: 1,
            room_name: "room".into(),
            title: "old".into(),
            started_at: jiff::Timestamp::now(),
            lifecycle: SessionLifecycle::Open,
            recording_plan: RecordingPlan {
                credential: Some(CredentialRef::new("old", old_cookie)),
                output_dir: dir.path().join("recordings"),
                segment_time_ms: None,
                segment_size: None,
                min_segment_size: 0,
                qn: 10_000,
                cdn: Vec::new(),
            },
            output_plan: OutputPlan::LocalOnly,
            recording_events: Vec::new(),
        };
        transitions::create_session(&store, &old_session).unwrap();
        let configured_client = Arc::new(BiliClient::new(None).unwrap());
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut supervisor = RoomSupervisor::new(
            1,
            PipelineConfig {
                poll_interval_s: 1,
                offline_grace_s: 0,
                backoff_s: 1,
                max_backoff_s: 1,
            },
            test_room_config(dir.path()),
            RoomSupervisorDeps {
                store,
                client: configured_client.clone(),
            },
            shutdown_rx,
        )
        .unwrap();

        assert!(!Arc::ptr_eq(
            &supervisor.active_session.as_ref().unwrap().client,
            &configured_client
        ));
        supervisor.state = RoomState::WaitingReconnect;
        supervisor.offline_since = Some(Instant::now());
        supervisor.close_after_confirmed_offline().unwrap();
        assert!(supervisor.active_session.is_none());
        assert!(Arc::ptr_eq(
            &supervisor.configured_client,
            &configured_client
        ));
    }
}
