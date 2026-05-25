use std::process;

use bilive_rec::bilibili;
use bilive_rec::bilibili::client::BiliClient;
use bilive_rec::cli::{Cli, Command, ResolveOutcome, StateAction};
use bilive_rec::config::{AppConfig, RecordConfig};
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

async fn run_cmd(config_path: &std::path::Path) -> AppResult<()> {
    let config = AppConfig::load(config_path)?;
    config.validate_for_run()?;
    tracing::info!("config loaded from {}", config_path.display());

    use bilive_rec::credential::CredentialIdentity;
    use bilive_rec::pipeline::state_machine::PipelineState;
    use bilive_rec::pipeline::supervisor::{RoomSupervisor, RoomSupervisorDeps};
    use bilive_rec::uploader::biliup_adapter::BiliupUploader;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::time::Duration;

    let db_path = config.data.dir.join("state.redb");
    let store = Arc::new(StateStore::open(&db_path)?);
    let store_clone = store.clone();
    let upload_config = config.upload_config()?.clone();
    use bilive_rec::uploader::types::Uploader;

    let app_config = Arc::new(config.clone());

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let mut handles = futures::stream::FuturesUnordered::new();
    let prepared_rooms: Vec<_> = config
        .rooms
        .iter()
        .cloned()
        .map(|room_config| {
            let credentials = config.room_credentials(&room_config)?;
            Ok((room_config, credentials))
        })
        .collect::<AppResult<_>>()?;

    let mut uploaders = HashMap::new();
    for (_, room_credentials) in &prepared_rooms {
        if uploaders.contains_key(&room_credentials.upload) {
            continue;
        }
        tracing::info!(
            "Checking upload credential '{}'...",
            room_credentials.upload.name
        );
        let uploader = Arc::new(BiliupUploader::new(
            room_credentials.upload.cookie_file.clone(),
            upload_config.line.clone(),
            upload_config.threads,
            upload_config.submit_api.clone(),
        ));
        uploader.check_login().await?;
        uploaders.insert(room_credentials.upload.clone(), uploader);
    }

    let mut clients: HashMap<Option<CredentialIdentity>, Arc<BiliClient>> = HashMap::new();

    for (room_config, room_credentials) in prepared_rooms {
        let room_url = room_config.url.clone();
        let store = store_clone.clone();

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

        let uploader = uploaders
            .get(&room_credentials.upload)
            .ok_or_else(|| {
                bilive_rec::error::AppError::State(format!(
                    "upload credential '{}' was not initialized",
                    room_credentials.upload.name
                ))
            })?
            .clone();

        let app_config = app_config.clone();
        let shutdown_rx = shutdown_rx.clone();

        let handle = tokio::spawn(async move {
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
                app_config.pipeline.clone(),
                room_config,
                RoomSupervisorDeps {
                    store,
                    client,
                    uploader,
                    app_config: app_config.clone(),
                },
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
                    PipelineState::Idle => {
                        Some(Duration::from_secs(app_config.pipeline.poll_interval_s))
                    }
                    PipelineState::Failed | PipelineState::Offline => {
                        Some(Duration::from_secs(app_config.pipeline.poll_interval_s))
                    }
                    PipelineState::WaitingReconnect | PipelineState::ReResolving => {
                        Some(supervisor.reconnect_delay())
                    }
                    _ => None, // Recording, Uploading blocks/pumps immediately
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
            #[allow(unreachable_code)]
            Ok::<(), bilive_rec::error::AppError>(())
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
                    if process_room_result(res) {
                        any_failed = true;
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
                    if process_room_result(res) {
                        any_failed = true;
                    }
                }
            }
        }
    }

    tracing::info!("All room tasks finished.");

    if any_failed {
        Err(bilive_rec::error::AppError::State(
            "One or more room tasks failed; run `state inspect` for details".into(),
        ))
    } else {
        Ok(())
    }
}

/// Classify a single room task result. Returns true if the task failed
/// (non-graceful error or panic), false otherwise.
fn process_room_result(
    res: Result<bilive_rec::error::AppResult<()>, tokio::task::JoinError>,
) -> bool {
    match res {
        Ok(Ok(())) => {
            tracing::info!("Room task shut down cleanly");
            false
        }
        Ok(Err(bilive_rec::error::AppError::GracefulShutdown)) => {
            tracing::info!("Room task interrupted by shutdown");
            false
        }
        Ok(Err(e)) => {
            tracing::error!("Room task error: {}", e);
            true
        }
        Err(join_err) => {
            tracing::warn!("Room task panicked: {}", join_err);
            true
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
    println!();

    // Pipeline states
    let pipeline_states = store.list_all_pipeline_states()?;
    if !pipeline_states.is_empty() {
        println!("Pipeline states:");
        for (room_id, state) in &pipeline_states {
            println!("  room {}: {:?}", room_id, state);
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
            config.validate_for_upload_actions()?;
            let upload_config = config.upload_config()?.clone();
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
                            upload_config.line.clone(),
                            upload_config.threads,
                            upload_config.submit_api.clone(),
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
                Some(AppConfig::load(default_path)?)
            } else {
                None
            }
        }
        Some(path) => Some(AppConfig::load(path)?),
    };

    let (record_config, client) = if let Some(config) = config {
        config.validate_for_check()?;
        let record_cookie_file = config.record_cookie_file()?;
        let client = BiliClient::from_optional_cookie_file(record_cookie_file.as_deref())?;
        (config.record, client)
    } else {
        (RecordConfig::default(), BiliClient::new(None)?)
    };

    let room_id = bilibili::room::resolve_room_id(&client, room_url).await?;
    let room_info = bilibili::room::fetch_room_info(&client, room_id).await?;

    if room_info.live_status.is_live() {
        println!("live");
        println!("title = {}", room_info.title);
        println!("room_id = {}", room_info.room_id);

        let play_info_resp =
            bilibili::stream::fetch_play_info(&client, room_info.room_id, record_config.qn).await?;
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

        let selected =
            bilibili::stream::select_healthy_stream_candidate(&candidates, &record_config, &client)
                .await?;
        if let Some(ref sel) = selected {
            println!("selected = {}", sel.url);
        } else {
            println!("selected = none");
        }
    } else {
        println!("offline");
        println!("room_id = {}", room_info.room_id);
        println!("title = {}", room_info.title);
    }

    Ok(())
}

/// One-shot recording. Resolves the room, captures the stream into the
/// configured output dir, and persists a LiveSession + segment rows. No
/// upload, no pipeline state machine. Stops on Ctrl-C or when the stream
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
    config.validate_for_check()?;

    let record_credential = config.record_credential_identity()?;
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
        bilibili::stream::fetch_play_info(&client, room_info.room_id, config.record.qn).await?;
    let candidates = bilibili::stream::parse_stream_candidates(&play_info_resp)?;
    let candidate =
        bilibili::stream::select_healthy_stream_candidate(&candidates, &config.record, &client)
            .await?
            .ok_or_else(|| {
                bilive_rec::error::AppError::Bilibili("no healthy stream candidates".into())
            })?;

    tracing::info!(
        room_id = room_info.room_id,
        url = candidate.url.as_str(),
        "selected stream candidate"
    );

    // Persist truth before risk: create the LiveSession row before we open
    // the network stream. If the process dies mid-record, `state inspect`
    // can still see the session and its segments.
    let db_path = config.data.dir.join("state.redb");
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
            output_dir: config.record.output_dir.clone(),
        },
        segment: SegmentPolicy {
            segment_time: config.record.segment_time_duration()?,
            segment_size: config.record.segment_size_bytes()?,
        },
        filter: SegmentFilter {
            min_segment_size: config.record.min_segment_size_bytes()?,
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
                    index, path, size, ..
                } => {
                    println!(
                        "finalized segment {index} ({size} bytes) at {}",
                        path.display()
                    );
                }
                SegmentEvent::Filtered { index, size, .. } => {
                    println!("filtered segment {index} (too small: {size} bytes)");
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
        event_tx,
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
    config.validate_for_upload()?;
    let upload_config = config.upload_config()?.clone();
    let upload_credential = config.upload_credential_identity()?;

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
        upload_credential.cookie_file.clone(),
        upload_config.line.clone(),
        upload_config.threads,
        upload_config.submit_api.clone(),
    );
    uploader.check_login().await?;

    // Open store
    let db_path = config.data.dir.join("state.redb");
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
    let submit_req = SubmissionRequest {
        title: display_title,
        description: String::new(),
        tid: upload_config.tid,
        copyright: upload_config.copyright,
        tags: upload_config.tags,
        source: upload_config.source.clone(),
        parts: uploaded_parts,
    };

    let mut submission = Submission {
        session_id,
        upload_credential,
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
        let toml_str = "[upload]\ncredential=\"main\"\ntid=1\nline=\"auto\"\n";
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
        let toml_str = "[upload]\ncredential=\"main\"\ntid=1\nline=\"auto\"\n\n[pipeline]\npoll_interval_s = 0\n\n[[rooms]]\nname = \"test\"\nurl = \"http://bilibili.com/123\"\n";
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
        let toml_str = "[upload]\ncredential=\"main\"\ntid=1\nline=\"auto\"\n\n[pipeline]\nbackoff_s = 0\n\n[[rooms]]\nname = \"test\"\nurl = \"http://bilibili.com/123\"\n";
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
    async fn test_process_room_result_clean_exit() {
        let handle = tokio::spawn(async { Ok(()) });
        let res = handle.await;
        assert!(!process_room_result(res));
    }

    #[tokio::test]
    async fn test_process_room_result_graceful_shutdown() {
        let handle = tokio::spawn(async { Err(bilive_rec::error::AppError::GracefulShutdown) });
        let res = handle.await;
        assert!(!process_room_result(res));
    }

    #[tokio::test]
    async fn test_process_room_result_fatal_error() {
        let handle = tokio::spawn(async {
            Err(bilive_rec::error::AppError::State(
                "room is in Failed".into(),
            ))
        });
        let res = handle.await;
        assert!(process_room_result(res));
    }

    #[tokio::test]
    async fn test_process_room_result_panic() {
        let handle: tokio::task::JoinHandle<bilive_rec::error::AppResult<()>> =
            tokio::spawn(async { panic!("simulated panic") });
        let res = handle.await;
        assert!(process_room_result(res));
    }

    /// Smoke test: drain loop terminates when all handles complete, regardless
    /// of whether they succeed, fail, or panic, and reports any_failed correctly.
    #[tokio::test]
    async fn test_run_cmd_drains_mixed_room_outcomes() {
        use futures::stream::FuturesUnordered;

        let handles: FuturesUnordered<tokio::task::JoinHandle<bilive_rec::error::AppResult<()>>> =
            FuturesUnordered::new();
        handles.push(tokio::spawn(async { Ok(()) }));
        handles.push(tokio::spawn(async {
            Err(bilive_rec::error::AppError::State(
                "simulated room failure".into(),
            ))
        }));
        handles.push(tokio::spawn(async {
            Err(bilive_rec::error::AppError::GracefulShutdown)
        }));

        // Drain all handles using the same classifier the main loop uses.
        let mut any_failed = false;
        let mut handles = handles;
        while let Some(res) = futures::StreamExt::next(&mut handles).await {
            if process_room_result(res) {
                any_failed = true;
            }
        }

        assert!(any_failed, "expected at least one failure to be recorded");
    }
}
