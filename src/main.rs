#[allow(dead_code)]
mod bilibili;
mod cli;
#[allow(dead_code)]
mod config;
#[allow(dead_code)]
mod error;
#[allow(dead_code)]
mod pipeline;
#[allow(dead_code)]
mod recorder;
#[allow(dead_code)]
mod state;
#[allow(dead_code)]
mod uploader;

use std::process;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use cli::{Cli, Command, StateAction};
use config::AppConfig;
use error::AppResult;
use state::store::StateStore;

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Command::Login => {
            println!("not implemented: login");
            Ok(())
        }
        Command::Check { room_url } => {
            println!("not implemented: check {room_url}");
            Ok(())
        }
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
