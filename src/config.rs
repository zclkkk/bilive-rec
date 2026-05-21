use std::path::PathBuf;

use serde::Deserialize;

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub data: DataConfig,
    pub record: RecordConfig,
    pub upload: UploadConfig,
    pub rooms: Vec<RoomConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataConfig {
    pub dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RecordConfig {
    pub output_dir: PathBuf,
    #[serde(default)]
    pub segment_time: Option<String>,
    #[serde(default)]
    pub segment_size: Option<String>,
    #[serde(default = "default_min_segment_size")]
    pub min_segment_size: String,
    #[serde(default)]
    pub prefer_protocol: PreferredProtocol,
    #[serde(default = "default_qn")]
    pub qn: u32,
    #[serde(default)]
    pub cdn: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UploadConfig {
    pub cookie_file: PathBuf,
    #[serde(default = "default_line")]
    pub line: String,
    #[serde(default = "default_threads")]
    pub threads: usize,
    #[serde(default)]
    pub submit_api: SubmitApi,
    #[serde(default = "default_tid")]
    pub tid: u16,
    #[serde(default = "default_copyright")]
    pub copyright: u8,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RoomConfig {
    pub name: String,
    pub url: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PreferredProtocol {
    #[default]
    Flv,
    HlsTs,
    HlsFmp4,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SubmitApi {
    #[default]
    App,
    Web,
}

fn default_min_segment_size() -> String {
    "20MiB".to_string()
}

fn default_qn() -> u32 {
    10000
}

fn default_line() -> String {
    "auto".to_string()
}

fn default_threads() -> usize {
    3
}

fn default_tid() -> u16 {
    171
}

fn default_copyright() -> u8 {
    2
}

impl AppConfig {
    pub fn load(path: &std::path::Path) -> AppResult<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| AppError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        toml::from_str(&content).map_err(|e| AppError::Config(e.to_string()))
    }
}
