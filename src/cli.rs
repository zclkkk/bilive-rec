use std::path::PathBuf;

use clap::{Parser, Subcommand};
use uuid::Uuid;

#[derive(Parser)]
#[command(
    name = "bilive-rec",
    version,
    about = "Bilibili live stream recorder and uploader"
)]
pub struct Cli {
    /// Path to config file. Commands that require persistent state default to
    /// ./config.toml; `check` can run with built-in recording defaults.
    #[arg(short, long, global = true)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Check live status of a room
    Check {
        /// Room URL (e.g. https://live.bilibili.com/123456)
        room_url: String,
    },

    /// Run the automatic recording and upload daemon
    Run,

    /// Show persisted recording, upload, and submission status
    Status {
        /// Include full session, segment, part, and submission details
        #[arg(long)]
        verbose: bool,
    },

    /// Resolve an exceptional state after operator verification
    Recover {
        #[command(subcommand)]
        action: RecoverAction,
    },
}

#[derive(Subcommand)]
pub enum RecoverAction {
    /// Resolve a recording session that stopped without a trustworthy completion
    Recording {
        /// Session shown as RecoveryRequired by status
        session_id: Uuid,

        /// Finalize the usable recording artifacts
        #[arg(long, conflicts_with = "abandon", required_unless_present = "abandon")]
        finalize: bool,

        /// Abandon upload and submission for this recording session
        #[arg(
            long,
            conflicts_with = "finalize",
            required_unless_present = "finalize"
        )]
        abandon: bool,

        /// When finalizing, retain failed partial files but exclude them from upload
        #[arg(long, requires = "finalize")]
        exclude_failed: bool,

        /// Optional operator note persisted with the decision
        #[arg(long)]
        note: Option<String>,
    },

    /// Resolve an upload whose remote outcome is unknown
    Upload {
        /// Session containing the segment
        session_id: Uuid,

        /// Segment index within the session
        segment_index: u32,

        /// Confirm that Bilibili did not create the remote upload
        #[arg(
            long,
            conflicts_with = "uploaded",
            required_unless_present = "uploaded"
        )]
        not_uploaded: bool,

        /// Confirm upload success using the exact remote filename
        #[arg(
            long,
            value_name = "BILI_FILENAME",
            conflicts_with = "not_uploaded",
            required_unless_present = "not_uploaded"
        )]
        uploaded: Option<String>,

        /// Part title used when confirming an uploaded remote file
        #[arg(long, conflicts_with = "not_uploaded")]
        part_title: Option<String>,

        /// Optional operator note persisted with the resolution
        #[arg(long)]
        note: Option<String>,
    },

    /// Resolve a failed or ambiguous submission after checking Bilibili
    Submission {
        /// Session whose submission outcome needs resolution
        session_id: Uuid,

        /// Confirm that Bilibili did not create a submission
        #[arg(
            long,
            conflicts_with = "submitted",
            required_unless_present = "submitted"
        )]
        not_submitted: bool,

        /// Confirm that Bilibili created the submission
        #[arg(
            long,
            conflicts_with = "not_submitted",
            required_unless_present = "not_submitted"
        )]
        submitted: bool,

        /// Confirmed Bilibili aid
        #[arg(long, conflicts_with = "not_submitted")]
        aid: Option<u64>,

        /// Confirmed Bilibili bvid
        #[arg(long, conflicts_with = "not_submitted")]
        bvid: Option<String>,

        /// Optional operator note persisted with the resolution
        #[arg(long)]
        note: Option<String>,
    },

    /// Resolve a local part/final file conflict after inspecting both files
    Segment {
        /// Session containing the segment
        session_id: Uuid,

        /// Segment index within the session
        segment_index: u32,

        /// Keep the .part file and remove the conflicting final file
        #[arg(
            long,
            conflicts_with_all = ["keep_final", "exclude"],
            required_unless_present_any = ["keep_final", "exclude"]
        )]
        keep_part: bool,

        /// Keep the final file and remove the conflicting .part file
        #[arg(
            long,
            conflicts_with_all = ["keep_part", "exclude"],
            required_unless_present_any = ["keep_part", "exclude"]
        )]
        keep_final: bool,

        /// Exclude the segment from upload without deleting either file
        #[arg(
            long,
            conflicts_with_all = ["keep_part", "keep_final"],
            required_unless_present_any = ["keep_part", "keep_final"]
        )]
        exclude: bool,

        /// Optional operator note persisted with the decision
        #[arg(long)]
        note: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn primary_commands_parse() {
        assert!(Cli::try_parse_from(["bilive-rec", "run"]).is_ok());
        assert!(Cli::try_parse_from(["bilive-rec", "status"]).is_ok());
        assert!(
            Cli::try_parse_from(["bilive-rec", "check", "https://live.bilibili.com/123456",])
                .is_ok()
        );
    }

    #[test]
    fn config_is_a_global_option() {
        let before = Cli::try_parse_from(["bilive-rec", "--config", "prod.toml", "run"]).unwrap();
        assert_eq!(before.config, Some(PathBuf::from("prod.toml")));

        let after = Cli::try_parse_from(["bilive-rec", "run", "--config", "prod.toml"]).unwrap();
        assert_eq!(after.config, Some(PathBuf::from("prod.toml")));
    }

    #[test]
    fn recover_upload_requires_exactly_one_resolution() {
        let session = Uuid::new_v4().to_string();
        assert!(
            Cli::try_parse_from([
                "bilive-rec",
                "recover",
                "upload",
                &session,
                "1",
                "--not-uploaded",
            ])
            .is_ok()
        );
        assert!(Cli::try_parse_from(["bilive-rec", "recover", "upload", &session, "1",]).is_err());
        assert!(
            Cli::try_parse_from([
                "bilive-rec",
                "recover",
                "upload",
                &session,
                "1",
                "--not-uploaded",
                "--part-title",
                "Part 1",
            ])
            .is_err()
        );
    }

    #[test]
    fn recover_submission_rejects_ambiguous_or_irrelevant_options() {
        let session = Uuid::new_v4().to_string();
        assert!(
            Cli::try_parse_from([
                "bilive-rec",
                "recover",
                "submission",
                &session,
                "--not-submitted",
            ])
            .is_ok()
        );
        assert!(Cli::try_parse_from(["bilive-rec", "recover", "submission", &session,]).is_err());
        assert!(
            Cli::try_parse_from([
                "bilive-rec",
                "recover",
                "submission",
                &session,
                "--not-submitted",
                "--aid",
                "1",
            ])
            .is_err()
        );
    }

    #[test]
    fn recover_recording_requires_one_terminal_decision() {
        let session = Uuid::new_v4().to_string();
        assert!(
            Cli::try_parse_from([
                "bilive-rec",
                "recover",
                "recording",
                &session,
                "--finalize",
                "--exclude-failed",
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from(["bilive-rec", "recover", "recording", &session, "--abandon",])
                .is_ok()
        );
        assert!(Cli::try_parse_from(["bilive-rec", "recover", "recording", &session,]).is_err());
    }

    #[test]
    fn recover_segment_requires_exactly_one_local_fact() {
        let session = Uuid::new_v4().to_string();
        assert!(
            Cli::try_parse_from([
                "bilive-rec",
                "recover",
                "segment",
                &session,
                "1",
                "--keep-final",
            ])
            .is_ok()
        );
        assert!(Cli::try_parse_from(["bilive-rec", "recover", "segment", &session, "1"]).is_err());
        assert!(
            Cli::try_parse_from([
                "bilive-rec",
                "recover",
                "segment",
                &session,
                "1",
                "--keep-part",
                "--exclude",
            ])
            .is_err()
        );
    }
}
