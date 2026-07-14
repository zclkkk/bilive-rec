use std::process;

use bilive_rec::bilibili;
use bilive_rec::bilibili::client::BiliClient;
use bilive_rec::cli::{Cli, Command, RecoverAction};
use bilive_rec::config::AppConfig;
use bilive_rec::credential::CredentialRef;
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
    // for command output such as `status` and `recover` results.
    init_tracing();

    let cli = Cli::parse();
    let config = cli.config;
    let persistent_config = config
        .clone()
        .unwrap_or_else(|| std::path::PathBuf::from("config.toml"));

    let result = match cli.command {
        Command::Check { room_url } => check_cmd(&room_url, config.as_deref()).await,
        Command::Run => run_cmd(&persistent_config).await,
        Command::Status { verbose } => status_cmd(&persistent_config, verbose),
        Command::Recover { action } => match action {
            RecoverAction::Recording {
                session_id,
                finalize,
                abandon,
                exclude_failed,
                note,
            } => recover_recording_cmd(
                &persistent_config,
                session_id,
                finalize,
                abandon,
                exclude_failed,
                note,
            ),
            RecoverAction::Upload {
                session_id,
                segment_index,
                not_uploaded,
                uploaded,
                part_title,
                note,
            } => recover_upload_cmd(
                &persistent_config,
                session_id,
                segment_index,
                not_uploaded,
                uploaded,
                part_title,
                note,
            ),
            RecoverAction::Submission {
                session_id,
                not_submitted,
                submitted,
                aid,
                bvid,
                note,
            } => recover_submission_cmd(
                &persistent_config,
                session_id,
                not_submitted,
                submitted,
                aid,
                bvid,
                note,
            ),
            RecoverAction::Segment {
                session_id,
                segment_index,
                keep_part,
                keep_final,
                exclude,
                note,
            } => {
                recover_segment_cmd(
                    &persistent_config,
                    session_id,
                    segment_index,
                    keep_part,
                    keep_final,
                    exclude,
                    note,
                )
                .await
            }
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

struct RunTaskCoordinator {
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    handles: futures::stream::FuturesUnordered<tokio::task::JoinHandle<RunTaskResult>>,
}

impl RunTaskCoordinator {
    fn new(shutdown_tx: tokio::sync::watch::Sender<bool>) -> Self {
        Self {
            shutdown_tx,
            handles: futures::stream::FuturesUnordered::new(),
        }
    }

    fn push(&self, handle: tokio::task::JoinHandle<RunTaskResult>) {
        self.handles.push(handle);
    }

    fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    fn signal_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    fn subscribe_shutdown(&self) -> tokio::sync::watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    fn abort_all(&self) {
        for handle in self.handles.iter() {
            handle.abort();
        }
    }

    async fn next(&mut self) -> Option<Result<RunTaskResult, tokio::task::JoinError>> {
        futures::StreamExt::next(&mut self.handles).await
    }

    /// Every exit after the first task starts passes here. Fatal bootstrap and
    /// state errors stop new work, then wait for the current remote/file
    /// boundary to persist its result before the top-level process may exit.
    async fn finish(&mut self, result: AppResult<()>) -> AppResult<()> {
        if self.handles.is_empty() {
            return result;
        }

        self.signal_shutdown();
        let mut drain_failed = false;
        let mut forced = false;
        while !self.handles.is_empty() {
            tokio::select! {
                task = self.next() => {
                    if process_run_task_result(task.expect("guarded non-empty run task set")).failed {
                        drain_failed = true;
                    }
                }
                _ = tokio::signal::ctrl_c(), if !forced => {
                    tracing::warn!("Second Ctrl-C received while draining; forcing task cancellation. In-flight remote operations may become ambiguous.");
                    self.abort_all();
                    forced = true;
                }
            }
        }
        if forced {
            return Err(bilive_rec::error::AppError::State(
                "Forced exit by second Ctrl-C".into(),
            ));
        }
        if result.is_ok() && drain_failed {
            Err(bilive_rec::error::AppError::State(
                "One or more run tasks failed while shutdown was draining; run `bilive-rec status --verbose` for details"
                    .into(),
            ))
        } else {
            result
        }
    }
}

fn record_credential_name(credential: Option<&CredentialRef>) -> &str {
    credential.map_or("anonymous", |credential| credential.name.as_str())
}

async fn run_cmd(config_path: &std::path::Path) -> AppResult<()> {
    let config = AppConfig::load(config_path)?;
    let run_config = config.resolve_for_run()?;
    tracing::info!("config loaded from {}", config_path.display());

    use bilive_rec::pipeline::bootstrap::prepare_rooms;
    use bilive_rec::state::model::{OutputPlan, RoomLifecycle, UploadTarget};
    use bilive_rec::state::transitions;
    use bilive_rec::uploader::biliup_adapter::BiliupUploader;
    use bilive_rec::uploader::worker::UploadWorker;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::time::Duration;

    let db_path = run_config.data.dir.join("state.redb");
    let store = if db_path.exists() {
        Arc::new(StateStore::open_existing(&db_path)?)
    } else if run_config.rooms.is_empty() {
        return Err(bilive_rec::error::AppError::State(format!(
            "no configured rooms and no state database at {}; refusing to create empty state",
            db_path.display()
        )));
    } else {
        Arc::new(StateStore::create_or_open(&db_path)?)
    };
    let store_clone = store.clone();

    for message in bilive_rec::state::ownership::audit(store.as_ref())? {
        tracing::warn!("startup ownership recovery: {message}");
    }
    for message in bilive_rec::recorder::artifact_commit::reconcile(store.as_ref()).await? {
        tracing::info!("startup recovery: {message}");
    }
    for message in transitions::reconcile_interrupted_remote_attempts(store.as_ref())? {
        tracing::info!("startup recovery: {message}");
    }

    // With no current rooms the canonical registry is already known to be
    // empty, so persist removed-room recovery before deciding whether any
    // executable historical work remains.
    if run_config.rooms.is_empty() {
        for (room_id, room_state) in store.list_room_states()? {
            if let RoomLifecycle::Owned { session_id } = room_state.lifecycle {
                transitions::require_recovery(
                    &store,
                    session_id,
                    format!("room {room_id} was removed from the current configuration"),
                )?;
            }
        }
    }

    let pipeline_config = run_config.pipeline.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (open_sessions_ready_tx, open_sessions_ready_rx) = tokio::sync::watch::channel(false);
    let (worker_stop_when_idle_tx, worker_stop_when_idle_rx) = tokio::sync::watch::channel(false);

    let mut uploaders = HashMap::new();
    let has_bilibili_room = run_config.rooms.iter().any(|room| {
        matches!(
            room.output,
            bilive_rec::config::ResolvedRoomOutput::Bilibili { .. }
        )
    });
    for room_config in &run_config.rooms {
        let bilive_rec::config::ResolvedRoomOutput::Bilibili { upload, .. } = &room_config.output
        else {
            continue;
        };
        let target = UploadTarget {
            principal: upload.principal.clone(),
            line: upload.line.clone(),
            threads: upload.threads,
            submit_api: upload.submit_api.clone(),
        };
        if uploaders.contains_key(&target) {
            continue;
        }
        tracing::info!(
            credential = %upload.principal.credential.name,
            submit_api = %upload.submit_api.as_config_value(),
            line = %upload.line,
            "Preparing lazy upload target"
        );
        let uploader = Arc::new(BiliupUploader::new(
            upload.principal.clone(),
            upload.line.clone(),
            upload.threads,
            upload.submit_api.clone(),
        ));
        uploaders.insert(target, uploader);
    }

    for session in store.list_sessions()? {
        let OutputPlan::Bilibili { upload, .. } = &session.output_plan else {
            continue;
        };
        let target = UploadTarget::from(upload);
        if uploaders.contains_key(&target) {
            continue;
        }
        tracing::info!(
            session_id = %session.id,
            credential = %upload.principal.credential.name,
            submit_api = %upload.submit_api.as_config_value(),
            line = %upload.line,
            "Preparing lazy persisted-session upload target"
        );
        let uploader = Arc::new(BiliupUploader::new(
            upload.principal.clone(),
            upload.line.clone(),
            upload.threads,
            upload.submit_api.clone(),
        ));
        uploaders.insert(target, uploader);
    }

    let has_durable_work =
        bilive_rec::uploader::work::has_executable_durable_work(&store, |target| {
            uploaders.contains_key(target)
        })?;
    if run_config.rooms.is_empty() && !has_durable_work {
        return Err(bilive_rec::error::AppError::State(
            "no configured rooms or executable durable work; inspect blocked state with `bilive-rec status --verbose`"
                .into(),
        ));
    }

    let mut tasks = RunTaskCoordinator::new(shutdown_tx);
    let run_result: AppResult<()> = async {
        let mut upload_worker_running = false;
        if has_bilibili_room || has_durable_work {
            upload_worker_running = true;
            let worker_handle = tokio::spawn({
                let store = store_clone.clone();
                let uploaders = uploaders.clone();
                let shutdown_rx = shutdown_rx.clone();
                let poll_interval = Duration::from_secs(pipeline_config.poll_interval_s);
                async move {
                    let worker = UploadWorker::new(store, uploaders, poll_interval, shutdown_rx)
                        .with_open_session_barrier(open_sessions_ready_rx)
                        .with_stop_when_idle_signal(worker_stop_when_idle_rx);
                    RunTaskResult::UploadWorker(worker.run().await)
                }
            });
            tasks.push(worker_handle);
        }

        let prepared_rooms = if run_config.rooms.is_empty() {
            Vec::new()
        } else {
            loop {
                let preparation = tokio::select! {
                    result = prepare_rooms(run_config.rooms.clone()) => result,
                    task = tasks.next(), if upload_worker_running => {
                        let outcome = process_run_task_result(task.expect("guarded upload worker task"));
                        upload_worker_running = !outcome.upload_worker_finished;
                        if outcome.failed {
                            return Err(bilive_rec::error::AppError::State(
                                "upload worker failed while the room registry was being prepared".into(),
                            ));
                        }
                        continue;
                    }
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("Ctrl-C received while preparing room registry");
                        return Ok(());
                    }
                };
                match preparation {
                    Ok(rooms) => break rooms,
                    Err(error) if error.is_retryable() => {
                        tracing::warn!("Room registry preparation failed; retrying: {error}");
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(pipeline_config.backoff_s)) => {}
                            _ = tokio::signal::ctrl_c() => {
                                tracing::info!("Ctrl-C received during room registry retry backoff");
                                return Ok(());
                            }
                        }
                    }
                    Err(error) => return Err(error.into_app_error()),
                }
            }
        };

        run_prepared_rooms(
            &mut tasks,
            prepared_rooms,
            store_clone.clone(),
            pipeline_config.clone(),
            open_sessions_ready_tx,
            worker_stop_when_idle_tx,
            upload_worker_running,
        )
        .await
    }
    .await;

    tasks.finish(run_result).await
}

async fn run_prepared_rooms(
    tasks: &mut RunTaskCoordinator,
    prepared_rooms: Vec<bilive_rec::pipeline::bootstrap::PreparedRoom>,
    store: std::sync::Arc<StateStore>,
    pipeline_config: bilive_rec::config::PipelineConfig,
    open_sessions_ready_tx: tokio::sync::watch::Sender<bool>,
    worker_stop_when_idle_tx: tokio::sync::watch::Sender<bool>,
    mut upload_worker_running: bool,
) -> AppResult<()> {
    use bilive_rec::pipeline::supervisor::{RoomSupervisor, RoomSupervisorDeps};
    use bilive_rec::state::model::RoomLifecycle;
    use bilive_rec::state::transitions;
    use std::collections::HashSet;

    let shutdown_rx = tasks.subscribe_shutdown();
    let configured_room_ids: HashSet<u64> =
        prepared_rooms.iter().map(|room| room.room_id).collect();
    for (room_id, room_state) in store.list_room_states()? {
        if let RoomLifecycle::Owned { session_id } = room_state.lifecycle
            && !configured_room_ids.contains(&room_id)
        {
            transitions::require_recovery(
                &store,
                session_id,
                format!("room {room_id} was removed from the current configuration"),
            )?;
        }
    }
    let _ = open_sessions_ready_tx.send(true);

    for prepared in &prepared_rooms {
        let room_config = &prepared.room_config;
        tracing::info!(
            room_name = %room_config.name,
            room_url = %room_config.url,
            room_id = prepared.room_id,
            record_credential = %record_credential_name(room_config.record.credential.as_ref()),
            "Room registry ready"
        );
    }

    let mut runnable_rooms = Vec::new();
    for room in prepared_rooms {
        if store
            .get_room_state(room.room_id)?
            .is_some_and(|state| matches!(state.lifecycle, RoomLifecycle::Blocked { .. }))
        {
            tracing::warn!(
                room_id = room.room_id,
                room_name = %room.room_config.name,
                "Room is blocked by durable recovery state; skipping supervisor"
            );
        } else {
            runnable_rooms.push(room);
        }
    }

    let has_future_upload_producer = runnable_rooms.iter().any(|room| {
        matches!(
            room.room_config.output,
            bilive_rec::config::ResolvedRoomOutput::Bilibili { .. }
        )
    });
    let _ = worker_stop_when_idle_tx.send(!has_future_upload_producer);

    let mut active_room_tasks = runnable_rooms.len();
    for prepared in runnable_rooms {
        let room_id = prepared.room_id;
        let room_config = prepared.room_config;
        let client = prepared.client;
        let store = store.clone();
        let shutdown_rx = shutdown_rx.clone();
        let pipeline_config = pipeline_config.clone();

        let handle = tokio::spawn(async move {
            let result = async move {
                let supervisor = RoomSupervisor::new(
                    room_id,
                    pipeline_config,
                    room_config,
                    RoomSupervisorDeps { store, client },
                    shutdown_rx,
                )?;

                tracing::info!(
                    room_name = %supervisor.room_config.name,
                    room_id,
                    "Started supervisor"
                );
                supervisor.run().await
            }
            .await;

            RunTaskResult::Room(result)
        });
        tasks.push(handle);
    }

    if tasks.is_empty() {
        return Err(bilive_rec::error::AppError::State(
            "no runnable rooms or upload work; inspect blocked sessions with `bilive-rec status --verbose`"
                .into(),
        ));
    }

    // First Ctrl-C drains cleanly. A forced second exit can leave Attempting;
    // startup turns those durable intents into Ambiguous, never Pending.
    let mut shutdown_initiated = false;
    let mut any_failed = false;

    while !tasks.is_empty() {
        if !shutdown_initiated {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Ctrl-C received, signaling graceful shutdown...");
                    tasks.signal_shutdown();
                    shutdown_initiated = true;
                }
                Some(res) = tasks.next() => {
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
                        tasks.signal_shutdown();
                        shutdown_initiated = true;
                    }
                    if active_room_tasks == 0 && upload_worker_running && !shutdown_initiated {
                        tracing::info!("All room tasks finished; upload worker will drain durable work and stop when idle");
                        let _ = worker_stop_when_idle_tx.send(true);
                    }
                }
            }
        } else {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::warn!("Second Ctrl-C received; forcing exit. In-flight uploads may be lost.");
                    tasks.abort_all();
                    return Err(bilive_rec::error::AppError::State(
                        "Forced exit by second Ctrl-C".into(),
                    ));
                }
                Some(res) = tasks.next() => {
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
                        tasks.signal_shutdown();
                    }
                    if active_room_tasks == 0 && upload_worker_running {
                        let _ = worker_stop_when_idle_tx.send(true);
                    }
                }
            }
        }
    }

    tracing::info!("All run tasks finished.");
    if any_failed {
        Err(bilive_rec::error::AppError::State(
            "One or more run tasks failed; run `bilive-rec status --verbose` for details".into(),
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

fn is_global_run_error(error: &bilive_rec::error::AppError) -> bool {
    matches!(
        error,
        bilive_rec::error::AppError::Database(_)
            | bilive_rec::error::AppError::Table(_)
            | bilive_rec::error::AppError::Transaction(_)
            | bilive_rec::error::AppError::Storage(_)
            | bilive_rec::error::AppError::Commit(_)
            | bilive_rec::error::AppError::State(_)
    )
}

fn process_run_task_result(res: Result<RunTaskResult, tokio::task::JoinError>) -> RunTaskOutcome {
    match res {
        Ok(RunTaskResult::Room(result)) => {
            let global_failure = result.as_ref().err().is_some_and(is_global_run_error);
            RunTaskOutcome {
                failed: process_room_outcome(result),
                global_failure,
                room_finished: true,
                upload_worker_finished: false,
            }
        }
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

fn status_cmd(config_path: &std::path::Path, verbose: bool) -> AppResult<()> {
    let config = AppConfig::load(config_path)?;
    let db_path = config.data.dir.join("state.redb");
    let store = StateStore::open_existing(&db_path)?;
    let inspection = state::inspection::StateInspection::load(&store)?;

    println!("=== bilive-rec status ===");
    println!("State format: {}", state::store::STATE_FORMAT_ID);
    println!("Schema version: {}", store.schema_version()?);
    println!();

    println!("Summary:");
    println!("  sessions: {}", inspection.summary.session_count);
    println!("  segments: {}", inspection.summary.segment_count);
    println!("  submissions: {}", inspection.summary.submission_count);
    println!("  rooms: {}", inspection.summary.room_count);
    println!(
        "  upload_targets: {}",
        inspection.summary.upload_target_count
    );
    println!();

    if !inspection.room_states.is_empty() {
        println!("Room states:");
        for (room_id, room) in &inspection.room_states {
            print!(
                "  room {room_id}: {:?}, changed_at={}",
                room.lifecycle, room.changed_at
            );
            if let Some(message) = &room.message {
                print!(", message={message:?}");
            }
            println!();
        }
        println!();
    }

    if verbose {
        if !inspection.sessions.is_empty() {
            println!("Sessions:");
            for session in &inspection.sessions {
                println!("  id: {}", session.id);
                println!("    room_id: {}", session.room_id);
                println!("    room_name: {}", session.room_name);
                println!("    title: {}", session.title);
                println!("    started_at: {}", session.started_at);
                println!("    lifecycle: {:?}", session.lifecycle);
                println!("    recording_plan: {:?}", session.recording_plan);
                println!("    output_plan: {:?}", session.output_plan);
                for event in &session.recording_events {
                    println!("    recording_event: {event:?}");
                }
            }
            println!();
        }

        if !inspection.segments.is_empty() {
            println!("Segments:");
            for segment in &inspection.segments {
                let files = inspection.file_presence.iter().find(|files| {
                    files.session_id == segment.session_id && files.segment_index == segment.index
                });
                println!(
                    "  {}/{}: artifact={:?}, upload={:?}",
                    segment.session_id, segment.index, segment.artifact, segment.upload
                );
                println!("    part_path: {}", segment.part_path.display());
                println!("    final_path: {}", segment.final_path.display());
                if let Some(files) = files {
                    println!(
                        "    files: part={:?}, final={:?}",
                        files.part, files.final_file
                    );
                }
                for attempt in &segment.upload_attempts {
                    println!("    upload_attempt: {attempt:?}");
                }
                for resolution in &segment.upload_resolutions {
                    println!("    upload_resolution: {resolution:?}");
                }
            }
            println!();
        }

        if !inspection.submissions.is_empty() {
            println!("Submissions:");
            for submission in &inspection.submissions {
                println!(
                    "  session {}: state={:?}",
                    submission.session_id, submission.state
                );
                for attempt in &submission.attempts {
                    println!("    attempt: {attempt:?}");
                }
                for resolution in &submission.resolutions {
                    println!("    resolution: {resolution:?}");
                }
            }
            println!();
        }

        if !inspection.upload_targets.is_empty() {
            println!("Upload targets:");
            for target in &inspection.upload_targets {
                println!("  {:?}: {:?}", target.target, target.gate);
            }
            println!();
        }
    }

    if inspection.anomalies.is_empty() {
        println!("No anomalies detected.");
    } else {
        println!("Anomalies ({}):", inspection.anomalies.len());
        for anomaly in &inspection.anomalies {
            println!();
            println!("  [{}] {}", anomaly.kind.as_str(), anomaly.description);
            println!("    next: {}", anomaly.next_action);
        }
    }

    Ok(())
}

fn recover_upload_cmd(
    config_path: &std::path::Path,
    session_id: uuid::Uuid,
    segment_index: u32,
    not_uploaded: bool,
    uploaded: Option<String>,
    part_title: Option<String>,
    note: Option<String>,
) -> AppResult<()> {
    let config = AppConfig::load(config_path)?;
    let db_path = config.data.dir.join("state.redb");
    let store = StateStore::open_existing(&db_path)?;

    let target = match (not_uploaded, uploaded) {
        (true, None) => state::recovery::UploadResolutionTarget::NotUploaded,
        (false, Some(bili_filename)) => state::recovery::UploadResolutionTarget::Uploaded {
            proof: state::model::UploadedPart {
                bili_filename,
                part_title: part_title.unwrap_or_else(|| format!("Part {segment_index}")),
            },
        },
        _ => {
            return Err(bilive_rec::error::AppError::Config(
                "choose exactly one of --not-uploaded or --uploaded".into(),
            ));
        }
    };

    let resolved =
        state::recovery::resolve_upload(&store, session_id, segment_index, target, note)?;
    println!(
        "segment {}/{}: upload={:?}",
        resolved.session_id, resolved.index, resolved.upload
    );
    Ok(())
}

fn recover_recording_cmd(
    config_path: &std::path::Path,
    session_id: uuid::Uuid,
    finalize: bool,
    abandon: bool,
    exclude_failed: bool,
    note: Option<String>,
) -> AppResult<()> {
    if finalize == abandon {
        return Err(bilive_rec::error::AppError::Config(
            "choose exactly one of --finalize or --abandon".into(),
        ));
    }
    if abandon && exclude_failed {
        return Err(bilive_rec::error::AppError::Config(
            "--exclude-failed is only valid with --finalize".into(),
        ));
    }
    let config = AppConfig::load(config_path)?;
    let store = StateStore::open_existing(config.data.dir.join("state.redb"))?;
    let target = if finalize {
        state::recovery::RecordingResolutionTarget::Finalize { exclude_failed }
    } else {
        state::recovery::RecordingResolutionTarget::Abandon
    };
    let resolved = state::recovery::resolve_recording(&store, session_id, target, note)?;
    println!(
        "recording {}: lifecycle={:?}",
        resolved.session_id, resolved.lifecycle
    );
    if !resolved.excluded_segments.is_empty() {
        println!(
            "excluded failed segments: {}",
            resolved
                .excluded_segments
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    println!("room ownership released; `run` may monitor it again");
    Ok(())
}

fn recover_submission_cmd(
    config_path: &std::path::Path,
    session_id: uuid::Uuid,
    not_submitted: bool,
    submitted: bool,
    aid: Option<u64>,
    bvid: Option<String>,
    note: Option<String>,
) -> AppResult<()> {
    if not_submitted == submitted {
        return Err(bilive_rec::error::AppError::Config(
            "choose exactly one of --not-submitted or --submitted".into(),
        ));
    }
    if not_submitted && (aid.is_some() || bvid.is_some()) {
        return Err(bilive_rec::error::AppError::Config(
            "--aid and --bvid are only valid with --submitted".into(),
        ));
    }
    if submitted && aid.is_none() && bvid.is_none() {
        return Err(bilive_rec::error::AppError::Config(
            "--submitted requires at least one of --aid or --bvid".into(),
        ));
    }

    let config = AppConfig::load(config_path)?;
    let db_path = config.data.dir.join("state.redb");
    let store = StateStore::open_existing(&db_path)?;
    let target = if submitted {
        state::recovery::SubmissionResolutionTarget::Submitted { aid, bvid }
    } else {
        state::recovery::SubmissionResolutionTarget::NotSubmitted
    };
    let resolved = state::recovery::resolve_submission(&store, session_id, target, note)?;
    println!(
        "submission {}: state={:?}",
        resolved.session_id, resolved.state
    );
    Ok(())
}

async fn recover_segment_cmd(
    config_path: &std::path::Path,
    session_id: uuid::Uuid,
    segment_index: u32,
    keep_part: bool,
    keep_final: bool,
    exclude: bool,
    note: Option<String>,
) -> AppResult<()> {
    let selected = u8::from(keep_part) + u8::from(keep_final) + u8::from(exclude);
    if selected != 1 {
        return Err(bilive_rec::error::AppError::Config(
            "choose exactly one of --keep-part, --keep-final, or --exclude".into(),
        ));
    }
    let decision = if keep_part {
        state::model::ArtifactResolutionDecision::KeepPart
    } else if keep_final {
        state::model::ArtifactResolutionDecision::KeepFinal
    } else {
        state::model::ArtifactResolutionDecision::Exclude
    };
    let config = AppConfig::load(config_path)?;
    let store = StateStore::open_existing(config.data.dir.join("state.redb"))?;
    let segment = bilive_rec::recorder::artifact_commit::resolve_conflict(
        &store,
        session_id,
        segment_index,
        decision,
        note,
    )
    .await?;
    println!(
        "segment {}/{}: artifact={:?}",
        segment.session_id, segment.index, segment.artifact
    );
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
            assert!(matches!(e, bilive_rec::error::AppError::State(_)));
        }
    }

    #[tokio::test]
    async fn zero_room_run_marks_an_owned_session_for_recovery_before_reporting_idle() {
        use bilive_rec::state::model::{LiveSession, OutputPlan, RecordingPlan, SessionLifecycle};
        use std::io::Write;

        let dir = tempfile::TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");
        let mut config = std::fs::File::create(&config_path).unwrap();
        writeln!(config, "[data]\ndir = 'data'").unwrap();
        let state_path = dir.path().join("data/state.redb");
        let store = StateStore::create_or_open(&state_path).unwrap();
        let session = LiveSession {
            id: uuid::Uuid::new_v4(),
            room_id: 1,
            room_name: "removed".into(),
            title: "live".into(),
            started_at: jiff::Timestamp::now(),
            lifecycle: SessionLifecycle::Open,
            recording_plan: RecordingPlan {
                credential: None,
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
        bilive_rec::state::transitions::create_session(&store, &session).unwrap();
        drop(store);

        let error = run_cmd(&config_path).await.unwrap_err();
        assert!(error.to_string().contains("no configured rooms"));
        let store = StateStore::open_existing(&state_path).unwrap();
        assert!(matches!(
            store.get_session(session.id).unwrap().unwrap().lifecycle,
            SessionLifecycle::RecoveryRequired { .. }
        ));
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
    async fn test_process_run_task_result_state_error_is_global_failure() {
        let handle = tokio::spawn(async {
            RunTaskResult::Room(Err(bilive_rec::error::AppError::State(
                "room failed".into(),
            )))
        });

        let outcome = process_run_task_result(handle.await);

        assert!(outcome.failed);
        assert!(outcome.global_failure);
        assert!(outcome.room_finished);
        assert!(!outcome.upload_worker_finished);
    }

    #[tokio::test]
    async fn test_process_run_task_result_room_io_error_is_scoped() {
        let handle = tokio::spawn(async {
            RunTaskResult::Room(Err(bilive_rec::error::AppError::Io {
                path: "/recordings/room.flv".into(),
                source: std::io::Error::other("disk failure"),
            }))
        });

        let outcome = process_run_task_result(handle.await);

        assert!(outcome.failed);
        assert!(!outcome.global_failure);
        assert!(outcome.room_finished);
    }

    #[tokio::test]
    async fn test_process_run_task_result_recovery_required_is_room_scoped() {
        let handle = tokio::spawn(async {
            RunTaskResult::Room(Err(bilive_rec::error::AppError::RecoveryRequired(
                "operator decision needed".into(),
            )))
        });

        let outcome = process_run_task_result(handle.await);

        assert!(outcome.failed);
        assert!(!outcome.global_failure);
        assert!(outcome.room_finished);
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

    #[tokio::test]
    async fn coordinator_drains_started_tasks_before_returning_a_fatal_error() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let completed = Arc::new(AtomicBool::new(false));
        let task_completed = completed.clone();
        let mut coordinator = RunTaskCoordinator::new(shutdown_tx);
        coordinator.push(tokio::spawn(async move {
            while !*shutdown_rx.borrow() {
                shutdown_rx.changed().await.unwrap();
            }
            tokio::task::yield_now().await;
            task_completed.store(true, Ordering::SeqCst);
            RunTaskResult::UploadWorker(Ok(()))
        }));

        let result = coordinator
            .finish(Err(bilive_rec::error::AppError::Config(
                "fatal registry error".into(),
            )))
            .await;

        assert!(matches!(
            result,
            Err(bilive_rec::error::AppError::Config(_))
        ));
        assert!(completed.load(Ordering::SeqCst));
        assert!(coordinator.is_empty());
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
            RunTaskResult::Room(Err(bilive_rec::error::AppError::Io {
                path: "/recordings/room.flv".into(),
                source: std::io::Error::other("simulated room failure"),
            }))
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
