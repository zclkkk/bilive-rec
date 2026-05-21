use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "bilive-rec", version, about = "Bilibili live stream recorder")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Log in to Bilibili
    Login,

    /// Check live status of a room
    Check {
        /// Room URL (e.g. https://live.bilibili.com/123456)
        room_url: String,

        /// Path to config file
        #[arg(short, long)]
        config: Option<PathBuf>,
    },

    /// Record a live stream
    Record {
        /// Room URL
        room_url: String,
    },

    /// Upload recorded files
    Upload {
        /// Files to upload
        files: Vec<PathBuf>,
    },

    /// Run the full pipeline
    Run {
        /// Path to config file
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
    },

    /// Inspect or recover persisted state
    State {
        /// Path to config file
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,

        #[command(subcommand)]
        action: StateAction,
    },
}

#[derive(Subcommand)]
pub enum StateAction {
    /// Print a summary of persisted state
    Inspect,

    /// Run crash recovery
    Recover,
}
