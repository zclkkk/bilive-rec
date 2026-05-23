use std::path::PathBuf;

use clap::{Parser, Subcommand};
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "bilive-rec", version, about = "Bilibili live stream recorder")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Check live status of a room
    Check {
        /// Room URL (e.g. https://live.bilibili.com/123456)
        room_url: String,

        /// Path to config file
        #[arg(short, long)]
        config: Option<PathBuf>,
    },

    /// Record a live stream once (no upload, no pipeline state machine).
    ///
    /// Use this for ad-hoc recording. Stops on Ctrl-C or when the stream
    /// ends. For long-running multi-room operation with auto-upload, use
    /// `run` with a config.toml.
    Record {
        /// Room URL (e.g. https://live.bilibili.com/123456)
        room_url: String,

        /// Path to config file (defaults to ./config.toml if it exists)
        #[arg(short, long)]
        config: Option<PathBuf>,
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

        /// Re-upload finalized segments missing UploadedPart for a specific session
        #[arg(long)]
        retry_upload: Option<Uuid>,
    },
}
