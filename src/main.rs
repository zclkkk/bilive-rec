use std::process;

use bilive_rec::bilibili;
use bilive_rec::bilibili::client::BiliClient;
use bilive_rec::cli::{Cli, Command, ResolveOutcome, StateAction};
use bilive_rec::config::AppConfig;
use bilive_rec::error::AppResult;
use bilive_rec::state;
use bilive_rec::state::store::StateStore;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    // Initialize tracing once, before dispatch. Every subcommand's
    // info!/warn!/error! output should reach the operator on stderr,
    // regardless of which command they invoked. stdout stays reserved
    // for machine-readable command output (e.g. `state inspect`,
    // `state recover` dry-run plans).
    init_tracing();

    let cli = Cli::parse();

    let result = match cli.command {
        Command::Check { room_url, config } => check_cmd(&room_url, config.as_deref()).await,
        Command::Record { room_url, config } => record_cmd(&room_url, config.as_deref()).await,
        Command::Upload {
            files,
            title,
            config,
        } => upload_cmd(files, title, config.as_deref()).await,
        Command::Run { config } => run_cmd(&config).await,
        Command::State { config, action } => match action {
            StateAction::Inspect => state_inspect_cmd(&config),
            StateAction::Recover {
                apply,
                reset_room,
                retry_upload,
            } => state_recover_cmd(&config, apply, reset_room, retry_upload).await,
            StateAction::ResolveSubmission {
                session_id,
                outcome,
                aid,
                bvid,
            } => state_resolve_submission_cmd(&config, session_id, outcome, aid, bvid),
        },
    };

    if let Err(e) = result {
        eprintln!("error: {e}");
        process::exit(1);
    }
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .ok();
}

enum RunTaskResult {
    Room(AppResult<()>),
    UploadWorker(AppResult<()>),
}

#[derive(Debug, Default)]
struct RunTaskOutcome {
    failed: bool,
    global_failure: bool,
    room_finished: bool,
    upload_worker_finished: bool,
}

async fn run_cmd(config_path: &std::path::Path) -> AppResult<()> {
    let config = AppConfig::load(config_path)?;
    let run_config = config.resolve_for_run()?;
    tracing::info!("config loaded from {}", config_path.display());

    use bilive_rec::pipeline::state_machine::RoomState;
    use bilive_rec::pipeline::supervisor::{RoomSupervisor, RoomSupervisorDeps};
    use bilive_rec::uploader::biliup_adapter::BiliupUploader;
    use bilive_rec::uploader::worker::{UploadTarget, UploadWorker};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::time::Duration;

    let db_path = run_config.data.dir.join("state.redb");
    let store = Arc::new(StateStore::open(&db_path)?);
    let store_clone = store.clone();
    use bilive_rec::uploader::types::Uploader;

    let pipeline_config = run_config.pipeline.clone();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let mut handles = futures::stream::FuturesUnordered::new();
    let prepared_rooms = run_config.rooms;

    let mut uploaders = HashMap::new();
    for room_config in &prepared_rooms {
        let target = UploadTarget::new(
            room_config.upload.credential.clone(),
            room_config.upload.submit_api.clone(),
        );
        if uploaders.contains_key(&target) {
            continue;
        }
        tracing::info!(
            "Checking upload credential '{}'...",
            room_config.upload.credential.name
        );
        let uploader = Arc::new(BiliupUploader::new(
            room_config.upload.credential.cookie_file.clone(),
            room_config.upload.line.clone(),
            room_config.upload.threads,
            room_config.upload.submit_api.clone(),
        ));
        uploader.check_login().await?;
        uploaders.insert(target, uploader);
    }

    let mut clients: HashMap<Option<bilive_rec::credential::CredentialIdentity>, Arc<BiliClient>> =
        HashMap::new();

    let mut active_room_tasks = prepared_rooms.len();
    let mut upload_worker_running = false;
    if active_room_tasks > 0 {
        upload_worker_running = true;
        handles.push(tokio::spawn({
            let store = store_clone.clone();
            let uploaders = uploaders.clone();
            let shutdown_rx = shutdown_rx.clone();
            let poll_interval = Duration::from_secs(pipeline_config.poll_interval_s);
            async move {
                RunTaskResult::UploadWorker(
                    UploadWorker::new(store, uploaders, poll_interval, shutdown_rx)
                        .run()
                        .await,
                )
            }
        }));
    }

    for room_config in prepared_rooms {
        let room_url = room_config.url.clone();
        let store = store_clone.clone();
        let room_credentials = room_config.credentials();

        let record_credential = room_credentials.record.clone();
        let client = if let Some(client) = clients.get(&record_credential) {
            client.clone()
        } else {
            if let Some(credential) = &record_credential {
                tracing::info!(
                    "Room '{}' uses record credential '{}'",
                    room_config.name,
                    credential.name
                );
            }
            let client = Arc::new(BiliClient::from_optional_cookie_file(
                room_credentials.record_cookie_file(),
            )?);
            clients.insert(record_credential, client.clone());
            client
        };

        let shutdown_rx = shutdown_rx.clone();
        let pipeline_config = pipeline_config.clone();

        let handle = tokio::spawn(async move {
            let result = async move {
                let room_id =
                    match bilive_rec::bilibili::room::resolve_room_id(&client, &room_url).await {
                        Ok(id) => id,
                        Err(e) => {
                            return Err(bilive_rec::error::AppError::Bilibili(format!(
                                "Failed to resolve room URL {}: {}",
                                room_url, e
                            )));
                        }
                    };

                let mut loop_shutdown_rx = shutdown_rx.clone();
                let mut supervisor = RoomSupervisor::new(
                    room_id,
                    pipeline_config.clone(),
                    room_config,
                    RoomSupervisorDeps { store, client },
                    shutdown_rx,
                )?;

                tracing::info!("Started supervisor for room {}", room_id);

                loop {
                    // Check shutdown before each step
                    if *loop_shutdown_rx.borrow() {
                        tracing::info!("Room {} shutting down (signal received)", room_id);
                        return Ok::<(), bilive_rec::error::AppError>(());
                    }

                    match supervisor.run_step().await {
                        Ok(()) => {}
                        Err(bilive_rec::error::AppError::GracefulShutdown) => {
                            tracing::info!("Room {} interrupted by graceful shutdown", room_id);
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::error!("Fatal supervisor error for room {}: {}", room_id, e);
                            return Err(e);
                        }
                    }

                    let state = supervisor.session.state;
                    let sleep_duration = match state {
                        RoomState::Idle => {
                            Some(Duration::from_secs(pipeline_config.poll_interval_s))
                        }
                        RoomState::Failed | RoomState::Offline => {
                            Some(Duration::from_secs(pipeline_config.poll_interval_s))
                        }
                        RoomState::WaitingReconnect => Some(supervisor.reconnect_delay()),
                        _ => None, // Recording blocks/pumps immediately
                    };

                    if let Some(d) = sleep_duration {
                        tokio::select! {
                            _ = tokio::time::sleep(d) => {}
                            _ = loop_shutdown_rx.changed() => {
                                tracing::info!("Room {} shutting down (signal during sleep)", room_id);
                                return Ok(());
                            }
                        }
                    }
                }
            }
            .await;

            RunTaskResult::Room(result)
        });

        handles.push(handle);
    }

    use futures::StreamExt;

    // Each room runs independently. A failure in one room is logged and
    // recorded in redb but must not terminate sibling rooms — operators
    // can inspect failures via `bilive-rec state inspect` after the run.
    //
    // Shutdown contract:
    //   - First Ctrl-C: broadcast shutdown, drain handles cleanly.
    //   - Second Ctrl-C: forced exit; in-flight uploads may be lost.
    let mut shutdown_initiated = false;
    let mut any_failed = false;

    while !handles.is_empty() {
        if !shutdown_initiated {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Ctrl-C received, signaling graceful shutdown...");
                    let _ = shutdown_tx.send(true);
                    shutdown_initiated = true;
                }
                Some(res) = handles.next() => {
                    let outcome = process_run_task_result(res);
                    if outcome.failed {
                        any_failed = true;
                    }
                    if outcome.room_finished {
                        active_room_tasks = active_room_tasks.saturating_sub(1);
                    }
                    if outcome.upload_worker_finished {
                        upload_worker_running = false;
                    }
                    if outcome.global_failure && !shutdown_initiated {
                        let _ = shutdown_tx.send(true);
                        shutdown_initiated = true;
                    }
                    if active_room_tasks == 0 && upload_worker_running && !shutdown_initiated {
                        tracing::info!("All room tasks finished; signaling upload worker shutdown...");
                        let _ = shutdown_tx.send(true);
                        shutdown_initiated = true;
                    }
                }
            }
        } else {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::warn!("Second Ctrl-C received; forcing exit. In-flight uploads may be lost.");
                    return Err(bilive_rec::error::AppError::State(
                        "Forced exit by second Ctrl-C".into(),
                    ));
                }
                Some(res) = handles.next() => {
                    let outcome = process_run_task_result(res);
                    if outcome.failed {
                        any_failed = true;
                    }
                    if outcome.room_finished {
                        active_room_tasks = active_room_tasks.saturating_sub(1);
                    }
                    if outcome.upload_worker_finished {
                        upload_worker_running = false;
                    }
                    if outcome.global_failure {
                        let _ = shutdown_tx.send(true);
                    }
                    if active_room_tasks == 0 && upload_worker_running {
                        tracing::info!("All room tasks finished; signaling upload worker shutdown...");
                        let _ = shutdown_tx.send(true);
                    }
                }
            }
        }
    }

    tracing::info!("All run tasks finished.");

    if any_failed {
        Err(bilive_rec::error::AppError::State(
            "One or more run tasks failed; run `state inspect` for details".into(),
        ))
    } else {
        Ok(())
    }
}

fn process_room_outcome(result: bilive_rec::error::AppResult<()>) -> bool {
    match result {
        Ok(()) => {
            tracing::info!("Room task shut down cleanly");
            false
        }
        Err(bilive_rec::error::AppError::GracefulShutdown) => {
            tracing::info!("Room task interrupted by shutdown");
            false
        }
        Err(e) => {
            tracing::error!("Room task error: {}", e);
            true
        }
    }
}

fn process_run_task_result(res: Result<RunTaskResult, tokio::task::JoinError>) -> RunTaskOutcome {
    match res {
        Ok(RunTaskResult::Room(result)) => RunTaskOutcome {
            failed: process_room_outcome(result),
            global_failure: false,
            room_finished: true,
            upload_worker_finished: false,
        },
        Ok(RunTaskResult::UploadWorker(Ok(()))) => {
            tracing::info!("Upload worker shut down cleanly");
            RunTaskOutcome {
                failed: false,
                global_failure: false,
                room_finished: false,
                upload_worker_finished: true,
            }
        }
        Ok(RunTaskResult::UploadWorker(Err(e))) => {
            tracing::error!("Upload worker error: {}", e);
            RunTaskOutcome {
                failed: true,
                global_failure: true,
                room_finished: false,
                upload_worker_finished: true,
            }
        }
        Err(join_err) => {
            tracing::warn!("Run task panicked: {}", join_err);
            RunTaskOutcome {
                failed: true,
                global_failure: true,
                room_finished: false,
                upload_worker_finished: false,
            }
        }
    }
}

fn state_inspect_cmd(config_path: &std::path::Path) -> AppResult<()> {
    let config = AppConfig::load(config_path)?;
    let db_path = config.data.dir.join("state.redb");
    let store = StateStore::open(&db_path)?;

    // Schema version
    let schema_version = store.schema_version()?;
    println!("=== State Inspection ===");
    println!("Schema version: {}", schema_version);
    println!();

    // Summary counts
    let summary = store.summary()?;
    println!("Summary:");
    println!("  sessions: {}", summary.session_count);
    println!("  segments: {}", summary.segment_count);
    println!("  uploaded_parts: {}", summary.uploaded_parts_count);
    println!("  submissions: {}", summary.submission_count);
    println!("  submission_plans: {}", summary.submission_plan_count);
    println!();

    // Room states
    let room_states = store.list_all_room_states()?;
    if !room_states.is_empty() {
        println!("Room states:");
        for (room_id, room_state) in &room_states {
            print!("  room {}: {:?}", room_id, room_state.state);
            if let Some(session_id) = room_state.active_session_id {
                print!("  session={session_id}");
            }
            if let (Some(err), Some(ts)) = (&room_state.last_error, &room_state.last_error_at) {
                print!("  last_error=[{ts}] {err}");
            }
            println!();
        }
        println!();
    }

    // Sessions
    let sessions = store.list_all_sessions()?;
    if !sessions.is_empty() {
        println!("Sessions:");
        for session in &sessions {
            println!("  id: {}", session.id);
            println!("    room_key: {}", session.room_key);
            println!("    title: {}", session.title);
            println!("    started_at: {}", session.started_at);
            println!("    status: {:?}", session.status);
        }
        println!();
    }

    // Segments grouped by session
    let all_segments = store.list_all_segments()?;
    if !all_segments.is_empty() {
        // Group by session_id
        let mut segments_by_session: std::collections::HashMap<uuid::Uuid, Vec<_>> =
            std::collections::HashMap::new();
        for seg in &all_segments {
            segments_by_session
                .entry(seg.session_id)
                .or_default()
                .push(seg);
        }

        println!("Segments:");
        for (session_id, segs) in &segments_by_session {
            println!("  session {}:", session_id);
            // Sort by index
            let mut sorted_segs = segs.clone();
            sorted_segs.sort_by_key(|s| s.index);
            for seg in &sorted_segs {
                print!(
                    "    index={}: status={:?}, path={}",
                    seg.index,
                    seg.status,
                    seg.path.display()
                );
                if let Some(ref err) = seg.error {
                    print!(", error={}", err);
                }
                println!();
            }
        }
        println!();
    }

    // Uploaded parts grouped by session
    let all_parts = store.list_all_uploaded_parts()?;
    if !all_parts.is_empty() {
        let mut parts_by_session: std::collections::HashMap<uuid::Uuid, Vec<_>> =
            std::collections::HashMap::new();
        for part in &all_parts {
            parts_by_session
                .entry(part.session_id)
                .or_default()
                .push(part);
        }

        println!("Uploaded parts:");
        for (session_id, parts) in &parts_by_session {
            println!("  session {}:", session_id);
            let mut sorted_parts = parts.clone();
            sorted_parts.sort_by_key(|p| p.segment_index);
            for part in &sorted_parts {
                println!(
                    "    segment_index={}, bili_filename={}, part_title={}",
                    part.segment_index, part.bili_filename, part.part_title
                );
            }
        }
        println!();
    }

    // Submissions
    let all_submissions = store.list_all_submissions()?;
    if !all_submissions.is_empty() {
        println!("Submissions:");
        for sub in &all_submissions {
            print!("  session {}: status={:?}", sub.session_id, sub.status);
            if let Some(aid) = sub.aid {
                print!(", aid={}", aid);
            }
            if let Some(ref bvid) = sub.bvid {
                print!(", bvid={}", bvid);
            }
            if let Some(ref err) = sub.error {
                print!(", error={}", err);
            }
            println!();
        }
        println!();
    }

    // Anomalies
    let anomalies = bilive_rec::state::recovery::detect_anomalies(&store)?;
    if anomalies.is_empty() {
        println!("No anomalies detected.");
    } else {
        println!("Anomalies ({}):", anomalies.len());
        for anomaly in &anomalies {
            println!();
            println!(
                "  [{}] {}",
                format!("{:?}", anomaly.kind).to_lowercase(),
                anomaly.description
            );
        }
    }

    Ok(())
}

struct RecoveryUploader {
    by_session: std::collections::HashMap<
        uuid::Uuid,
        std::sync::Arc<bilive_rec::uploader::biliup_adapter::BiliupUploader>,
    >,
}

impl bilive_rec::uploader::types::Uploader for RecoveryUploader {
    async fn check_login(&self) -> AppResult<()> {
        for uploader in self.by_session.values() {
            uploader.check_login().await?;
        }
        Ok(())
    }

    async fn upload_segment(
        &self,
        req: bilive_rec::uploader::types::UploadRequest,
    ) -> AppResult<bilive_rec::state::model::UploadedPart> {
        let uploader = self.by_session.get(&req.session_id).ok_or_else(|| {
            bilive_rec::error::AppError::State(format!(
                "no recovery uploader initialized for session {}",
                req.session_id
            ))
        })?;
        uploader.upload_segment(req).await
    }

    async fn submit(
        &self,
        _req: bilive_rec::uploader::types::SubmissionRequest,
    ) -> AppResult<bilive_rec::uploader::types::SubmissionOutcome> {
        Err(bilive_rec::error::AppError::State(
            "recovery uploader does not submit videos".into(),
        ))
    }
}

async fn state_recover_cmd(
    config_path: &std::path::Path,
    apply: bool,
    reset_room: Option<u64>,
    retry_upload: Option<uuid::Uuid>,
) -> AppResult<()> {
    let config = AppConfig::load(config_path)?;
    let db_path = config.data.dir.join("state.redb");
    let store = StateStore::open(&db_path)?;

    let mut reset_rooms = std::collections::HashSet::new();
    if let Some(room) = reset_room {
        reset_rooms.insert(room);
    }

    let mut retry_upload_sessions = std::collections::HashSet::new();
    if let Some(session) = retry_upload {
        retry_upload_sessions.insert(session);
    }

    let plan = state::recovery::plan_recovery(&store, &reset_rooms, &retry_upload_sessions)?;

    if apply {
        if state::recovery::plan_has_upload_actions(&plan) {
            let upload_recovery = config.resolve_for_upload_recovery()?;
            let mut uploaders_by_credential: std::collections::HashMap<
                bilive_rec::credential::CredentialIdentity,
                std::sync::Arc<bilive_rec::uploader::biliup_adapter::BiliupUploader>,
            > = std::collections::HashMap::new();
            let mut uploaders_by_session = std::collections::HashMap::new();

            for action in &plan.actions {
                let state::recovery::RecoveryAction::ScheduleUploadReconciliation {
                    session_id,
                    ..
                } = action
                else {
                    continue;
                };

                let session = store.get_session(*session_id)?.ok_or_else(|| {
                    bilive_rec::error::AppError::State(format!(
                        "session {} not found for upload recovery",
                        session_id
                    ))
                })?;
                let session_upload_credential =
                    session.upload_credential.clone().ok_or_else(|| {
                        bilive_rec::error::AppError::State(format!(
                            "session {} has no upload credential; cannot retry upload automatically",
                            session.id
                        ))
                    })?;
                let current = config.credential_identity(
                    &session_upload_credential.name,
                    &format!("session {} upload_credential", session.id),
                )?;
                if current != session_upload_credential {
                    return Err(bilive_rec::error::AppError::Config(format!(
                        "session {} was recorded with upload credential '{}' at {}, but current config resolves it to {}",
                        session.id,
                        session_upload_credential.name,
                        session_upload_credential.cookie_file.display(),
                        current.cookie_file.display()
                    )));
                }

                let uploader = if let Some(uploader) =
                    uploaders_by_credential.get(&session_upload_credential)
                {
                    uploader.clone()
                } else {
                    let uploader = std::sync::Arc::new(
                        bilive_rec::uploader::biliup_adapter::BiliupUploader::new(
                            session_upload_credential.cookie_file.clone(),
                            upload_recovery.upload.line.clone(),
                            upload_recovery.upload.threads,
                            upload_recovery.upload.submit_api.clone(),
                        ),
                    );
                    uploaders_by_credential.insert(session_upload_credential, uploader.clone());
                    uploader
                };
                uploaders_by_session.insert(*session_id, uploader);
            }

            let uploader = RecoveryUploader {
                by_session: uploaders_by_session,
            };
            use bilive_rec::uploader::types::Uploader;
            uploader.check_login().await?;

            let results = state::recovery::apply_recovery(&store, &plan, Some(&uploader)).await?;
            for result in &results {
                match result {
                    state::recovery::ApplyResult::Applied(msg) => println!("[applied] {}", msg),
                    state::recovery::ApplyResult::Skipped(msg) => println!("[skipped] {}", msg),
                }
            }
        } else {
            // Local-only apply — no uploader needed
            use bilive_rec::uploader::biliup_adapter::BiliupUploader;
            let results =
                state::recovery::apply_recovery::<BiliupUploader>(&store, &plan, None).await?;
            for result in &results {
                match result {
                    state::recovery::ApplyResult::Applied(msg) => println!("[applied] {}", msg),
                    state::recovery::ApplyResult::Skipped(msg) => println!("[skipped] {}", msg),
                }
            }
        }
    } else {
        if plan.is_empty() {
            println!("No recovery actions needed.");
        } else {
            println!("{}", plan);
        }
    }

    Ok(())
}

fn state_resolve_submission_cmd(
    config_path: &std::path::Path,
    session_id: uuid::Uuid,
    outcome: ResolveOutcome,
    aid: Option<u64>,
    bvid: Option<String>,
) -> AppResult<()> {
    let config = AppConfig::load(config_path)?;
    let db_path = config.data.dir.join("state.redb");
    let store = StateStore::open(&db_path)?;

    let target = match outcome {
        ResolveOutcome::Submitted => bilive_rec::state::model::SubmissionStatus::Submitted,
        ResolveOutcome::Failed => bilive_rec::state::model::SubmissionStatus::Failed,
    };

    let resolved = state::recovery::resolve_submission(&store, session_id, target, aid, bvid)?;
    println!(
        "session {}: {:?} -> {:?}",
        resolved.session_id, resolved.from, resolved.to
    );
    if let Some(a) = resolved.aid {
        println!("aid = {a}");
    }
    if let Some(b) = resolved.bvid {
        println!("bvid = {b}");
    }
    Ok(())
}

async fn check_cmd(room_url: &str, config_path: Option<&std::path::Path>) -> AppResult<()> {
    let config = match config_path {
        None => {
            let default_path = std::path::Path::new("config.toml");
            if default_path.exists() {
                AppConfig::load(default_path)?
            } else {
                AppConfig::parse("")?
            }
        }
        Some(path) => AppConfig::load(path)?,
    };

    let check_config = config.resolve_for_check()?;
    let client = BiliClient::from_optional_cookie_file(
        check_config
            .record
            .credential
            .as_ref()
            .map(|credential| credential.cookie_file()),
    )?;

    let room_id = bilibili::room::resolve_room_id(&client, room_url).await?;
    let room_info = bilibili::room::fetch_room_info(&client, room_id).await?;

    if room_info.live_status.is_live() {
        println!("live");
        println!("title = {}", room_info.title);
        println!("room_id = {}", room_info.room_id);

        let play_info_resp =
            bilibili::stream::fetch_play_info(&client, room_info.room_id, check_config.record.qn)
                .await?;
        let candidates = bilibili::stream::parse_stream_candidates(&play_info_resp)?;

        println!("candidates = {}", candidates.len());
        for candidate in &candidates {
            println!(
                "  - codec={}, qn={}, cdn={}, url={}",
                candidate.codec.as_str(),
                candidate.qn,
                candidate.cdn_name,
                candidate.url
            );
        }

        let selected = bilibili::stream::select_healthy_stream_candidate(
            &candidates,
            &check_config.record,
            &client,
        )
        .await?;
        println!("selected = {}", selected.url);
    } else {
        println!("offline");
        println!("room_id = {}", room_info.room_id);
        println!("title = {}", room_info.title);
    }

    Ok(())
}

/// One-shot recording. Resolves the room, captures the stream into the
/// configured output dir, and persists a LiveSession + segment rows. No
/// upload, no persisted room state. Stops on Ctrl-C or when the stream
/// ends. For long-running multi-room operation with auto-upload, use `run`.
async fn record_cmd(room_url: &str, config_path: Option<&std::path::Path>) -> AppResult<()> {
    use bilive_rec::recorder::record_flv;
    use bilive_rec::recorder::segment::{
        RecorderPolicy, SegmentEvent, SegmentFilter, SegmentLayout, SegmentPolicy,
    };
    use bilive_rec::state::model::{LiveSession, SessionStatus};
    use std::sync::Arc;
    use uuid::Uuid;

    let config = match config_path {
        None => {
            let default_path = std::path::Path::new("config.toml");
            if default_path.exists() {
                AppConfig::load(default_path)?
            } else {
                AppConfig::parse("")?
            }
        }
        Some(path) => AppConfig::load(path)?,
    };
    let check_config = config.resolve_for_check()?;

    let record_credential = check_config.record.credential.clone();
    let client = BiliClient::from_optional_cookie_file(
        record_credential
            .as_ref()
            .map(|credential| credential.cookie_file()),
    )?;
    let room_id = bilibili::room::resolve_room_id(&client, room_url).await?;

    let room_info = bilibili::room::fetch_room_info(&client, room_id).await?;
    if !room_info.live_status.is_live() {
        println!("offline");
        println!("room_id = {}", room_info.room_id);
        println!("title = {}", room_info.title);
        return Ok(());
    }

    let play_info_resp =
        bilibili::stream::fetch_play_info(&client, room_info.room_id, check_config.record.qn)
            .await?;
    let candidates = bilibili::stream::parse_stream_candidates(&play_info_resp)?;
    let candidate = bilibili::stream::select_healthy_stream_candidate(
        &candidates,
        &check_config.record,
        &client,
    )
    .await?;

    tracing::info!(
        room_id = room_info.room_id,
        url = candidate.url.as_str(),
        "selected stream candidate"
    );

    // Persist truth before risk: create the LiveSession row before we open
    // the network stream. If the process dies mid-record, `state inspect`
    // can still see the session and its segments.
    let db_path = check_config.data.dir.join("state.redb");
    let store = Arc::new(bilive_rec::state::store::StateStore::open(&db_path)?);

    let session_id = Uuid::new_v4();
    let live_session = LiveSession {
        id: session_id,
        room_key: room_info.room_id.to_string(),
        title: room_info.title.clone(),
        started_at: jiff::Timestamp::now(),
        status: SessionStatus::Recording,
        record_credential,
        upload_credential: None,
    };
    store.put_session(&live_session)?;
    println!("session_id = {session_id}");

    let policy = RecorderPolicy {
        layout: SegmentLayout {
            output_dir: check_config.record.output_dir.clone(),
        },
        segment: SegmentPolicy {
            segment_time: check_config.record.segment_time,
            segment_size: check_config.record.segment_size,
        },
        filter: SegmentFilter {
            min_segment_size: check_config.record.min_segment_size,
        },
    };

    let resp = client
        .stream_client()
        .get(&candidate.url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Referer", "https://live.bilibili.com/")
        .send()
        .await?;

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<SegmentEvent>();
    let event_drain = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                SegmentEvent::Started {
                    index, part_path, ..
                } => {
                    println!("started segment {index} at {}", part_path.display());
                }
                SegmentEvent::Finalized {
                    index,
                    path,
                    size,
                    close_reason,
                    ..
                } => {
                    println!(
                        "finalized segment {index} ({size} bytes, reason={close_reason}) at {}",
                        path.display()
                    );
                }
                SegmentEvent::Filtered {
                    index,
                    size,
                    close_reason,
                    ..
                } => {
                    println!(
                        "filtered segment {index} (too small: {size} bytes, reason={close_reason})"
                    );
                }
            }
        }
    });

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Forward the first Ctrl-C into the shared shutdown channel. We don't
    // spawn the recorder itself in a task because record_flv takes
    // &StateStore (no Arc), which can't live on a 'static future.
    let shutdown_tx_clone = shutdown_tx.clone();
    let signal_task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("Ctrl-C received; signaling graceful shutdown");
            let _ = shutdown_tx_clone.send(true);
        }
    });

    let record_result = record_flv(
        resp,
        session_id,
        policy,
        store.as_ref(),
        Some(event_tx),
        1,
        shutdown_rx,
    )
    .await;

    signal_task.abort();
    let _ = event_drain.await;

    // Persist session status reflecting the outcome.
    let session_status = match &record_result {
        Ok(()) | Err(bilive_rec::error::AppError::GracefulShutdown) => SessionStatus::Finalized,
        Err(_) => SessionStatus::Failed,
    };
    let mut updated = live_session;
    updated.status = session_status;
    store.put_session(&updated)?;

    match record_result {
        Ok(()) => {
            println!("recording complete");
            Ok(())
        }
        Err(bilive_rec::error::AppError::GracefulShutdown) => {
            println!("recording interrupted by Ctrl-C; session marked Finalized");
            Ok(())
        }
        Err(e) => Err(e),
    }
}
async fn upload_cmd(
    files: Vec<std::path::PathBuf>,
    title: Option<String>,
    config_path: Option<&std::path::Path>,
) -> AppResult<()> {
    use bilive_rec::state::model::{Submission, SubmissionStatus};
    use bilive_rec::state::store::StateStore;
    use bilive_rec::uploader::biliup_adapter::BiliupUploader;
    use bilive_rec::uploader::types::{
        SubmissionOutcome, SubmissionRequest, UploadRequest, Uploader,
    };
    use uuid::Uuid;

    let config = match config_path {
        None => {
            let default_path = std::path::Path::new("config.toml");
            if default_path.exists() {
                AppConfig::load(default_path)?
            } else {
                return Err(bilive_rec::error::AppError::Config(
                    "No config file provided for upload command".into(),
                ));
            }
        }
        Some(path) => AppConfig::load(path)?,
    };
    let upload_command = config.resolve_for_upload()?;

    if files.is_empty() {
        return Err(bilive_rec::error::AppError::Config(
            "No files provided for upload.".into(),
        ));
    }

    for file in &files {
        if !file.exists() {
            return Err(bilive_rec::error::AppError::Config(format!(
                "Upload file does not exist: {}",
                file.display()
            )));
        }
        if !file.is_file() {
            return Err(bilive_rec::error::AppError::Config(format!(
                "Upload path is not a regular file: {}",
                file.display()
            )));
        }
        if !file
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("flv"))
        {
            return Err(bilive_rec::error::AppError::Config(format!(
                "Upload file is not a .flv: {}",
                file.display()
            )));
        }
    }

    println!("Checking login...");
    let uploader = BiliupUploader::new(
        upload_command.upload.credential.cookie_file.clone(),
        upload_command.upload.line.clone(),
        upload_command.upload.threads,
        upload_command.upload.submit_api.clone(),
    );
    uploader.check_login().await?;

    // Open store
    let db_path = upload_command.data.dir.join("state.redb");
    let store = StateStore::open(&db_path)?;

    let session_id = Uuid::new_v4();
    println!("Session ID: {}", session_id);
    let mut uploaded_parts = Vec::new();

    let display_title = title.unwrap_or_else(|| {
        files
            .first()
            .and_then(|p| p.file_stem())
            .and_then(|s| s.to_str())
            .unwrap_or("Untitled")
            .to_string()
    });

    for (index, file) in files.into_iter().enumerate() {
        println!("Uploading: {}", file.display());
        let part_title = if index == 0 {
            display_title.clone()
        } else {
            format!("{} P{}", display_title, index + 1)
        };

        let req = UploadRequest {
            session_id,
            segment_index: index as u32,
            path: file,
            part_title,
        };

        let part = uploader.upload_segment(req).await?;
        println!("Uploaded part: {}", part.bili_filename);
        store.put_uploaded_part(&part)?;
        uploaded_parts.push(part);
    }

    println!("Submitting...");
    let submit = upload_command.submit;
    let submit_req = SubmissionRequest {
        title: display_title,
        description: String::new(),
        category_id: submit.category_id,
        copyright: submit.copyright,
        tags: submit.tags,
        source: submit.source,
        private: submit.private,
        dynamic: submit.dynamic,
        forbid_reprint: submit.forbid_reprint,
        charging_panel: submit.charging_panel,
        close_reply: submit.close_reply,
        close_danmu: submit.close_danmu,
        featured_reply: submit.featured_reply,
        parts: uploaded_parts,
    };

    let mut submission = Submission {
        session_id,
        upload_credential: upload_command.upload.credential,
        status: SubmissionStatus::Pending,
        aid: None,
        bvid: None,
        error: None,
    };
    store.put_submission(&submission)?;

    let res = uploader.submit(submit_req).await;
    match res {
        Ok(SubmissionOutcome::Confirmed { aid, bvid }) => {
            submission.status = SubmissionStatus::Submitted;
            submission.aid = aid;
            submission.bvid = bvid.clone();
            store.put_submission(&submission)?;

            println!("Submission complete!");
            if let Some(ref b) = bvid {
                println!("BVID: {}", b);
            }
            if let Some(a) = aid {
                println!("AID: {}", a);
            }
        }
        Ok(SubmissionOutcome::Ambiguous { reason }) => {
            submission.status = SubmissionStatus::Ambiguous;
            submission.error = Some(reason.clone());
            store.put_submission(&submission)?;

            println!("Submission accepted but outcome is AMBIGUOUS.");
            println!("Bilibili did not return aid/bvid; verify on Bilibili and");
            println!(
                "resolve via: bilive-rec state resolve-submission {} --as submitted|failed",
                session_id
            );
            println!("Reason: {}", reason);
            return Err(bilive_rec::error::AppError::Bilibili(format!(
                "submission ambiguous: {reason}"
            )));
        }
        Err(e) => {
            submission.status = SubmissionStatus::Failed;
            submission.error = Some(e.to_string());
            store.put_submission(&submission)?;
            return Err(e);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_run_cmd_empty_rooms() {
        use std::io::Write;
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        let toml_str = "[upload]\ncredential=\"main\"\nline=\"auto\"\n";
        temp_file.write_all(toml_str.as_bytes()).unwrap();

        let res = run_cmd(temp_file.path()).await;
        assert!(res.is_err());
        if let Err(e) = res {
            assert!(matches!(e, bilive_rec::error::AppError::Config(_)));
        }
    }

    #[tokio::test]
    async fn test_run_cmd_zero_poll_interval() {
        use std::io::Write;
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        let toml_str = "[upload]\ncredential=\"main\"\nline=\"auto\"\n\n[pipeline]\npoll_interval_s = 0\n\n[rooms.test]\nurl = \"http://bilibili.com/123\"\n";
        temp_file.write_all(toml_str.as_bytes()).unwrap();

        let res = run_cmd(temp_file.path()).await;
        assert!(res.is_err());
        if let Err(e) = res {
            assert!(matches!(e, bilive_rec::error::AppError::Config(_)));
            assert!(
                e.to_string()
                    .contains("pipeline.poll_interval_s must be greater than 0")
            );
        }
    }

    #[tokio::test]
    async fn test_run_cmd_zero_backoff() {
        use std::io::Write;
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        let toml_str = "[upload]\ncredential=\"main\"\nline=\"auto\"\n\n[pipeline]\nbackoff_s = 0\n\n[rooms.test]\nurl = \"http://bilibili.com/123\"\n";
        temp_file.write_all(toml_str.as_bytes()).unwrap();

        let res = run_cmd(temp_file.path()).await;
        assert!(res.is_err());
        if let Err(e) = res {
            assert!(matches!(e, bilive_rec::error::AppError::Config(_)));
            assert!(
                e.to_string()
                    .contains("pipeline.backoff_s must be greater than 0")
            );
        }
    }

    #[tokio::test]
    async fn test_process_room_outcome_clean_exit() {
        assert!(!process_room_outcome(Ok(())));
    }

    #[tokio::test]
    async fn test_process_room_outcome_graceful_shutdown() {
        assert!(!process_room_outcome(Err(
            bilive_rec::error::AppError::GracefulShutdown
        )));
    }

    #[tokio::test]
    async fn test_process_room_outcome_fatal_error() {
        assert!(process_room_outcome(Err(
            bilive_rec::error::AppError::State("room is in Failed".into(),)
        )));
    }

    #[tokio::test]
    async fn test_process_run_task_result_panic_is_global_failure() {
        let handle: tokio::task::JoinHandle<RunTaskResult> =
            tokio::spawn(async { panic!("simulated panic") });

        let outcome = process_run_task_result(handle.await);

        assert!(outcome.failed);
        assert!(outcome.global_failure);
        assert!(!outcome.room_finished);
        assert!(!outcome.upload_worker_finished);
    }

    #[tokio::test]
    async fn test_process_run_task_result_room_error_is_not_global_failure() {
        let handle = tokio::spawn(async {
            RunTaskResult::Room(Err(bilive_rec::error::AppError::State(
                "room failed".into(),
            )))
        });

        let outcome = process_run_task_result(handle.await);

        assert!(outcome.failed);
        assert!(!outcome.global_failure);
        assert!(outcome.room_finished);
        assert!(!outcome.upload_worker_finished);
    }

    #[tokio::test]
    async fn test_process_run_task_result_worker_error_is_global_failure() {
        let handle = tokio::spawn(async {
            RunTaskResult::UploadWorker(Err(bilive_rec::error::AppError::State(
                "worker failed".into(),
            )))
        });

        let outcome = process_run_task_result(handle.await);

        assert!(outcome.failed);
        assert!(outcome.global_failure);
        assert!(!outcome.room_finished);
        assert!(outcome.upload_worker_finished);
    }

    /// Smoke test: drain loop sees room task outcomes through the same enum the
    /// run loop uses, instead of preserving the pre-worker task shape.
    #[tokio::test]
    async fn test_run_cmd_drains_mixed_room_outcomes() {
        use futures::stream::FuturesUnordered;

        let handles: FuturesUnordered<tokio::task::JoinHandle<RunTaskResult>> =
            FuturesUnordered::new();
        handles.push(tokio::spawn(async { RunTaskResult::Room(Ok(())) }));
        handles.push(tokio::spawn(async {
            RunTaskResult::Room(Err(bilive_rec::error::AppError::State(
                "simulated room failure".into(),
            )))
        }));
        handles.push(tokio::spawn(async {
            RunTaskResult::Room(Err(bilive_rec::error::AppError::GracefulShutdown))
        }));

        // Drain all handles using the same classifier the main loop uses.
        let mut any_failed = false;
        let mut handles = handles;
        while let Some(res) = futures::StreamExt::next(&mut handles).await {
            let outcome = process_run_task_result(res);
            assert!(!outcome.global_failure);
            assert!(outcome.room_finished);
            if outcome.failed {
                any_failed = true;
            }
        }

        assert!(any_failed, "expected at least one failure to be recorded");
    }
}
