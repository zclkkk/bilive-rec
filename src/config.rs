use std::path::PathBuf;

use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PreferredProtocol {
    #[default]
    Flv,
    HlsTs,
    HlsFmp4,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
        Self::parse(&content)
    }

    pub fn parse(content: &str) -> AppResult<Self> {
        toml::from_str(content).map_err(|e| AppError::Config(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_TOML: &str = r#"
[data]
dir = "./data"

[record]
output_dir = "./recordings"
segment_time = "01:00:00"
segment_size = "2GiB"
min_segment_size = "20MiB"
prefer_protocol = "flv"
qn = 10000
cdn = []

[upload]
cookie_file = "./data/cookies.json"
line = "auto"
threads = 3
submit_api = "app"
tid = 171
copyright = 2
tags = ["直播录像"]

[[rooms]]
name = "example"
url = "https://live.bilibili.com/123456"
title = "{streamer} {title} {date}"
description = "{streamer} 直播录像\n原直播间：{url}"
"#;

    #[test]
    fn parse_sample_config() {
        let config = AppConfig::parse(SAMPLE_TOML).unwrap();
        assert_eq!(config.data.dir, std::path::PathBuf::from("./data"));
        assert_eq!(
            config.record.output_dir,
            std::path::PathBuf::from("./recordings")
        );
        assert_eq!(config.record.segment_time.as_deref(), Some("01:00:00"));
        assert_eq!(config.record.segment_size.as_deref(), Some("2GiB"));
        assert_eq!(config.record.min_segment_size, "20MiB");
        assert_eq!(config.record.qn, 10000);
        assert!(config.record.cdn.is_empty());
        assert_eq!(config.upload.threads, 3);
        assert_eq!(config.upload.tid, 171);
        assert_eq!(config.upload.copyright, 2);
        assert_eq!(config.upload.tags, vec!["直播录像"]);
        assert_eq!(config.rooms.len(), 1);
        assert_eq!(config.rooms[0].name, "example");
    }

    #[test]
    fn parse_config_with_defaults() {
        let toml = r#"
[data]
dir = "./data"

[record]
output_dir = "./rec"

[upload]
cookie_file = "./cookies.json"

[[rooms]]
name = "test"
url = "https://live.bilibili.com/1"
"#;
        let config = AppConfig::parse(toml).unwrap();
        assert_eq!(config.record.min_segment_size, "20MiB");
        assert_eq!(config.record.qn, 10000);
        assert_eq!(config.upload.line, "auto");
        assert_eq!(config.upload.threads, 3);
        assert_eq!(config.upload.tid, 171);
        assert_eq!(config.upload.copyright, 2);
    }

    #[test]
    fn preferred_protocol_serde_roundtrip() {
        let json = serde_json::to_string(&PreferredProtocol::Flv).unwrap();
        assert_eq!(json, "\"flv\"");
        let p: PreferredProtocol = serde_json::from_str("\"hlsts\"").unwrap();
        assert!(matches!(p, PreferredProtocol::HlsTs));
    }

    #[test]
    fn submit_api_serde_roundtrip() {
        let json = serde_json::to_string(&SubmitApi::App).unwrap();
        assert_eq!(json, "\"app\"");
        let s: SubmitApi = serde_json::from_str("\"web\"").unwrap();
        assert!(matches!(s, SubmitApi::Web));
    }
}
