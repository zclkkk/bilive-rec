use std::process;

use bilive_rec::bilibili;
use bilive_rec::bilibili::client::BiliClient;
use bilive_rec::cli::{Cli, Command, StateAction};
use bilive_rec::config::{AppConfig, RecordConfig};
use bilive_rec::error::{AppError, AppResult};
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
        Command::Upload { files } => {
            println!("not implemented: upload {:?}", files);
            Ok(())
        }
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

    let room_id = bilibili::room::extract_room_id(room_url).ok_or_else(|| {
        AppError::Config(format!("Failed to extract room ID from '{}'", room_url))
    })?;

    let client = BiliClient::new(None)?;
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

        let selected = bilibili::stream::select_stream_candidate(&candidates, &record_config);
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
