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
        #[arg(num_args = 1..)]
        files: Vec<PathBuf>,

        /// Title of the video
        #[arg(long)]
        title: Option<String>,

        /// Path to config file
        #[arg(short, long)]
        config: Option<PathBuf>,
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

    /// Run crash recovery (dry-run by default)
    Recover {
        /// Apply safe recovery mutations instead of just printing the plan
        #[arg(long)]
        apply: bool,

        /// Reset a specific room's pipeline from Failed to Idle
        #[arg(long)]
        reset_room: Option<u64>,
    },
}
