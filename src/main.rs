use std::process;

use bilive_rec::bilibili;
use bilive_rec::bilibili::client::BiliClient;
use bilive_rec::cli::{Cli, Command, StateAction};
use bilive_rec::config::{AppConfig, RecordConfig};
use bilive_rec::error::AppResult;
use bilive_rec::state;
use bilive_rec::state::store::StateStore;
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Command::Login => {
            println!("not implemented: login");
            Ok(())
        }
        Command::Check { room_url, config } => check_cmd(&room_url, config.as_deref()).await,
        Command::Record { room_url } => {
            println!("not implemented: record {room_url}");
            Ok(())
        }
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
    init_tracing();
    let config = AppConfig::load(config_path)?;
    tracing::info!("config loaded from {}", config_path.display());

    use bilive_rec::config::SubmitApi;
    if !matches!(config.upload.submit_api, SubmitApi::App) {
        return Err(bilive_rec::error::AppError::Config(
            "Only 'app' submit API is supported for now.".into(),
        ));
    }

    if config.upload.line != "auto" && config.upload.line != "bda2" {
        return Err(bilive_rec::error::AppError::Config(format!(
            "Unsupported upload line '{}'. Only 'auto' and 'bda2' are supported for now.",
            config.upload.line
        )));
    }

    if config.upload.threads == 0 {
        return Err(bilive_rec::error::AppError::Config(
            "upload.threads must be greater than 0".into(),
        ));
    }

    if config.rooms.is_empty() {
        return Err(bilive_rec::error::AppError::Config(
            "run requires at least one room".into(),
        ));
    }

    if config.pipeline.poll_interval_s == 0 {
        return Err(bilive_rec::error::AppError::Config(
            "pipeline.poll_interval_s must be greater than 0".into(),
        ));
    }

    if config.pipeline.backoff_s == 0 {
        return Err(bilive_rec::error::AppError::Config(
            "pipeline.backoff_s must be greater than 0".into(),
        ));
    }

    use bilive_rec::pipeline::state_machine::PipelineState;
    use bilive_rec::pipeline::supervisor::RoomSupervisor;
    use bilive_rec::uploader::biliup_adapter::BiliupUploader;
    use std::sync::Arc;
    use tokio::time::Duration;

    let db_path = config.data.dir.join("state.redb");
    let store = Arc::new(StateStore::open(&db_path)?);
    let store_clone = store.clone();

    // Check login right at the start
    let uploader = Arc::new(BiliupUploader::new(
        config.upload.cookie_file.clone(),
        config.upload.line.clone(),
        config.upload.threads,
    ));
    use bilive_rec::uploader::types::Uploader;
    tracing::info!("Checking uploader login...");
    uploader.check_login().await?;

    let client = Arc::new(BiliClient::new(None)?);
    let app_config = Arc::new(config.clone());

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let mut handles = futures::stream::FuturesUnordered::new();

    for room in &config.rooms {
        let room_url = room.url.clone();
        let store = store_clone.clone();
        let uploader = uploader.clone();
        let client = client.clone();
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
                Some(store),
                Some(client),
                Some(uploader),
                Some(app_config.clone()),
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
                        Some(Duration::from_secs(app_config.pipeline.backoff_s))
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
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl-C received, signaling graceful shutdown...");
            let _ = shutdown_tx.send(true);
            // Wait for all room tasks to finish their current operation
            while let Some(res) = handles.next().await {
                match res {
                    Ok(Ok(())) => {
                        tracing::info!("Room task shut down cleanly");
                    }
                    Ok(Err(bilive_rec::error::AppError::GracefulShutdown)) => {
                        tracing::info!("Room task interrupted by shutdown");
                    }
                    Ok(Err(e)) => {
                        tracing::warn!("Room task error during shutdown: {}", e);
                    }
                    Err(join_err) => {
                        tracing::warn!("Room task panicked during shutdown: {}", join_err);
                    }
                }
            }
            tracing::info!("All room tasks stopped.");
        }
        res = handles.next() => {
            match res {
                Some(Ok(Ok(()))) => {
                    return Err(bilive_rec::error::AppError::State("Room task exited unexpectedly with Ok(())".into()));
                }
                Some(Ok(Err(e))) => {
                    return Err(e);
                }
                Some(Err(join_err)) => {
                    return Err(bilive_rec::error::AppError::State(format!("Room task panicked or joined with error: {}", join_err)));
                }
                None => {
                    tracing::info!("All room tasks finished.");
                }
            }
        }
    }

    Ok(())
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
            // Validate upload config before constructing uploader
            use bilive_rec::config::SubmitApi;
            if !matches!(config.upload.submit_api, SubmitApi::App) {
                return Err(bilive_rec::error::AppError::Config(
                    "Only 'app' submit API is supported for upload recovery.".into(),
                ));
            }
            if config.upload.threads == 0 {
                return Err(bilive_rec::error::AppError::Config(
                    "upload.threads must be greater than 0".into(),
                ));
            }
            if config.upload.line != "auto" && config.upload.line != "bda2" {
                return Err(bilive_rec::error::AppError::Config(format!(
                    "Unsupported upload line '{}'. Only 'auto' and 'bda2' are supported.",
                    config.upload.line
                )));
            }
            if !config.upload.cookie_file.exists() {
                return Err(bilive_rec::error::AppError::Config(format!(
                    "Cookie file does not exist: {}",
                    config.upload.cookie_file.display()
                )));
            }
            if !config.upload.cookie_file.is_file() {
                return Err(bilive_rec::error::AppError::Config(format!(
                    "Cookie file path is not a regular file: {}",
                    config.upload.cookie_file.display()
                )));
            }

            let uploader = bilive_rec::uploader::biliup_adapter::BiliupUploader::new(
                config.upload.cookie_file.clone(),
                config.upload.line.clone(),
                config.upload.threads,
            );
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

async fn check_cmd(room_url: &str, config_path: Option<&std::path::Path>) -> AppResult<()> {
    let record_config = match config_path {
        None => {
            let default_path = std::path::Path::new("config.toml");
            if default_path.exists() {
                AppConfig::load(default_path)?.record
            } else {
                RecordConfig::default()
            }
        }
        Some(path) => AppConfig::load(path)?.record,
    };

    let client = BiliClient::new(None)?;
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
                "  - protocol={}, format={}, codec={}, qn={}, cdn={}, url={}",
                candidate.protocol.as_str(),
                candidate.format,
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

async fn upload_cmd(
    files: Vec<std::path::PathBuf>,
    title: Option<String>,
    config_path: Option<&std::path::Path>,
) -> AppResult<()> {
    use bilive_rec::state::model::{Submission, SubmissionStatus};
    use bilive_rec::state::store::StateStore;
    use bilive_rec::uploader::biliup_adapter::BiliupUploader;
    use bilive_rec::uploader::types::{SubmissionRequest, UploadRequest, Uploader};
    use uuid::Uuid;

    let upload_config = match config_path {
        None => {
            let default_path = std::path::Path::new("config.toml");
            if default_path.exists() {
                AppConfig::load(default_path)?.upload
            } else {
                return Err(bilive_rec::error::AppError::Config(
                    "No config file provided for upload command".into(),
                ));
            }
        }
        Some(path) => AppConfig::load(path)?.upload,
    };

    use bilive_rec::config::SubmitApi;
    if !matches!(upload_config.submit_api, SubmitApi::App) {
        return Err(bilive_rec::error::AppError::Config(
            "Only 'app' submit API is supported for now.".into(),
        ));
    }

    if upload_config.line != "auto" && upload_config.line != "bda2" {
        return Err(bilive_rec::error::AppError::Config(format!(
            "Unsupported upload line '{}'. Only 'auto' and 'bda2' are supported for now.",
            upload_config.line
        )));
    }

    if upload_config.threads == 0 {
        return Err(bilive_rec::error::AppError::Config(
            "upload.threads must be greater than 0".into(),
        ));
    }

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
    }

    println!("Checking login...");
    let uploader = BiliupUploader::new(
        upload_config.cookie_file.clone(),
        upload_config.line.clone(),
        upload_config.threads,
    );
    uploader.check_login().await?;

    // Open store
    let config = AppConfig::load(config_path.unwrap_or(std::path::Path::new("config.toml")))?;
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
        status: SubmissionStatus::Pending,
        aid: None,
        bvid: None,
        error: None,
    };
    store.put_submission(&submission)?;

    let res = uploader.submit(submit_req).await;
    match res {
        Ok(sres) => {
            submission.status = SubmissionStatus::Submitted;
            submission.aid = sres.aid;
            submission.bvid = sres.bvid.clone();
            store.put_submission(&submission)?;

            println!("Submission complete!");
            if let Some(ref bvid) = sres.bvid {
                println!("BVID: {}", bvid);
            }
            if let Some(aid) = sres.aid {
                println!("AID: {}", aid);
            }
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
        let toml_str = "[upload]\ncookie_file=\"cookies.json\"\ntid=1\nline=\"auto\"\n";
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
        let toml_str = "[upload]\ncookie_file=\"cookies.json\"\ntid=1\nline=\"auto\"\n\n[pipeline]\npoll_interval_s = 0\n\n[[rooms]]\nname = \"test\"\nurl = \"http://bilibili.com/123\"\n";
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
        let toml_str = "[upload]\ncookie_file=\"cookies.json\"\ntid=1\nline=\"auto\"\n\n[pipeline]\nbackoff_s = 0\n\n[[rooms]]\nname = \"test\"\nurl = \"http://bilibili.com/123\"\n";
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
}
