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
        Command::Run { config } => run_cmd(&config),
        Command::State { config, action } => match action {
            StateAction::Inspect => state_inspect_cmd(&config),
            StateAction::Recover => state_recover_cmd(&config),
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
        .init();
}

fn run_cmd(config_path: &std::path::Path) -> AppResult<()> {
    init_tracing();
    let _config = AppConfig::load(config_path)?;
    tracing::info!("config loaded from {}", config_path.display());
    println!("not implemented: run");
    Ok(())
}

fn state_inspect_cmd(config_path: &std::path::Path) -> AppResult<()> {
    let config = AppConfig::load(config_path)?;
    let db_path = config.data.dir.join("state.redb");
    let store = StateStore::open(&db_path)?;
    let summary = store.summary()?;
    println!("sessions: {}", summary.session_count);
    println!("segments: {}", summary.segment_count);
    println!("uploaded_parts: {}", summary.uploaded_parts_count);
    println!("submissions: {}", summary.submission_count);
    Ok(())
}

fn state_recover_cmd(config_path: &std::path::Path) -> AppResult<()> {
    let config = AppConfig::load(config_path)?;
    let db_path = config.data.dir.join("state.redb");
    let store = StateStore::open(&db_path)?;
    let actions = state::recovery::recover(&store)?;
    for action in &actions {
        println!("{action}");
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
        source: String::new(),
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
