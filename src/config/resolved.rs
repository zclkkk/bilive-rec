use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::credential::{CredentialRef, UploadPrincipal};

use super::raw::{Copyright, DataConfig, PipelineConfig, SubmitApi};

#[derive(Debug, Clone)]
pub struct RunConfig {
    pub data: DataConfig,
    pub pipeline: PipelineConfig,
    pub rooms: Vec<ResolvedRoomConfig>,
}

#[derive(Debug, Clone)]
pub struct CheckConfig {
    pub data: DataConfig,
    pub record: ResolvedRecordConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRoomConfig {
    pub name: String,
    pub url: String,
    pub record: ResolvedRecordConfig,
    pub output: ResolvedRoomOutput,
}

impl ResolvedRoomConfig {
    pub fn credentials(&self) -> RoomCredentials {
        RoomCredentials {
            record: self.record.credential.clone(),
            upload: match &self.output {
                ResolvedRoomOutput::LocalOnly => None,
                ResolvedRoomOutput::Bilibili { upload, .. } => {
                    Some(upload.principal.credential.clone())
                }
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedRoomOutput {
    LocalOnly,
    Bilibili {
        upload: ResolvedRoomUploadConfig,
        submit: Box<ResolvedSubmitConfig>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRecordConfig {
    pub credential: Option<CredentialRef>,
    pub output_dir: PathBuf,
    pub segment_time: Option<Duration>,
    pub segment_size: Option<u64>,
    pub min_segment_size: u64,
    pub qn: u32,
    pub cdn: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRoomUploadConfig {
    pub principal: UploadPrincipal,
    pub line: String,
    pub threads: usize,
    pub submit_api: SubmitApi,
    pub delete_after_submit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSubmitConfig {
    pub title: Option<String>,
    pub description: Option<String>,
    pub category_id: u16,
    pub copyright: Copyright,
    pub source: String,
    pub tags: Vec<String>,
    pub private: bool,
    pub dynamic: String,
    pub forbid_reprint: bool,
    pub charging_panel: bool,
    pub close_reply: bool,
    pub close_danmu: bool,
    pub featured_reply: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomCredentials {
    pub record: Option<CredentialRef>,
    pub upload: Option<CredentialRef>,
}

impl RoomCredentials {
    pub fn record_cookie_file(&self) -> Option<&Path> {
        self.record.as_ref().map(CredentialRef::cookie_file)
    }
}
