use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub data: DataConfig,
    #[serde(default)]
    pub record: RecordConfig,
    pub upload: UploadConfig,
    #[serde(default)]
    pub pipeline: PipelineConfig,
    #[serde(default)]
    pub rooms: Vec<RoomConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataConfig {
    #[serde(default = "default_data_dir")]
    pub dir: PathBuf,
}

impl Default for DataConfig {
    fn default() -> Self {
        Self {
            dir: default_data_dir(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RecordConfig {
    #[serde(default = "default_output_dir")]
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

impl Default for RecordConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("./recordings"),
            segment_time: None,
            segment_size: None,
            min_segment_size: default_min_segment_size(),
            prefer_protocol: PreferredProtocol::default(),
            qn: default_qn(),
            cdn: Vec::new(),
        }
    }
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
    #[serde(default = "default_source")]
    pub source: String,
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
pub enum PreferredProtocol {
    #[serde(rename = "flv")]
    #[default]
    Flv,
    #[serde(rename = "hls_ts", alias = "hlsts", alias = "ts")]
    HlsTs,
    #[serde(rename = "hls_fmp4", alias = "hlsfmp4", alias = "fmp4")]
    HlsFmp4,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SubmitApi {
    #[default]
    App,
    Web,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

fn default_output_dir() -> PathBuf {
    PathBuf::from("./recordings")
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

fn default_source() -> String {
    "直播录像".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    #[serde(default = "default_poll_interval_s")]
    pub poll_interval_s: u64,
    #[serde(default = "default_offline_grace_s")]
    pub offline_grace_s: u64,
    #[serde(default = "default_backoff_s")]
    pub backoff_s: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            poll_interval_s: default_poll_interval_s(),
            offline_grace_s: default_offline_grace_s(),
            backoff_s: default_backoff_s(),
        }
    }
}

fn default_poll_interval_s() -> u64 {
    60
}
fn default_offline_grace_s() -> u64 {
    60
}
fn default_backoff_s() -> u64 {
    15
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

    pub fn validate_for_run(&self) -> AppResult<()> {
        if self.rooms.is_empty() {
            return Err(AppError::Config("run requires at least one room".into()));
        }
        self.pipeline.validate()?;
        self.record.validate()?;
        self.upload.validate()?;
        self.upload.validate_cookie_file()
    }

    pub fn validate_for_upload(&self) -> AppResult<()> {
        self.upload.validate()?;
        self.upload.validate_cookie_file()
    }

    pub fn validate_for_upload_recovery(&self) -> AppResult<()> {
        self.upload.validate()?;
        self.upload.validate_cookie_file()
    }

    pub fn validate_for_check(&self) -> AppResult<()> {
        self.record.validate()?;
        self.upload.validate_cookie_file()
    }
}

impl RecordConfig {
    pub fn validate(&self) -> AppResult<()> {
        self.min_segment_size_bytes()?;
        self.segment_time_duration()?;
        self.segment_size_bytes()?;
        Ok(())
    }

    pub fn min_segment_size_bytes(&self) -> AppResult<u64> {
        parse_size_bytes(&self.min_segment_size).ok_or_else(|| {
            AppError::Config(format!(
                "Invalid min_segment_size: {}",
                self.min_segment_size
            ))
        })
    }

    pub fn segment_time_duration(&self) -> AppResult<Option<Duration>> {
        self.segment_time
            .as_deref()
            .map(parse_hms_duration)
            .transpose()
    }

    pub fn segment_size_bytes(&self) -> AppResult<Option<u64>> {
        self.segment_size
            .as_deref()
            .map(|value| {
                parse_size_bytes(value)
                    .ok_or_else(|| AppError::Config(format!("Invalid segment_size: {value}")))
            })
            .transpose()
    }
}

impl UploadConfig {
    pub fn validate(&self) -> AppResult<()> {
        if !matches!(self.submit_api, SubmitApi::App) {
            return Err(AppError::Config(
                "Only 'app' submit API is supported for now.".into(),
            ));
        }
        if self.line != "auto" && self.line != "bda2" {
            return Err(AppError::Config(format!(
                "Unsupported upload line '{}'. Only 'auto' and 'bda2' are supported for now.",
                self.line
            )));
        }
        if self.threads == 0 {
            return Err(AppError::Config(
                "upload.threads must be greater than 0".into(),
            ));
        }
        Ok(())
    }

    pub fn validate_cookie_file(&self) -> AppResult<()> {
        if !self.cookie_file.exists() {
            return Err(AppError::Config(format!(
                "Cookie file does not exist: {}",
                self.cookie_file.display()
            )));
        }
        if !self.cookie_file.is_file() {
            return Err(AppError::Config(format!(
                "Cookie file path is not a regular file: {}",
                self.cookie_file.display()
            )));
        }
        Ok(())
    }
}

impl PipelineConfig {
    pub fn validate(&self) -> AppResult<()> {
        if self.poll_interval_s == 0 {
            return Err(AppError::Config(
                "pipeline.poll_interval_s must be greater than 0".into(),
            ));
        }
        if self.backoff_s == 0 {
            return Err(AppError::Config(
                "pipeline.backoff_s must be greater than 0".into(),
            ));
        }
        Ok(())
    }
}

pub fn parse_size_bytes(value: &str) -> Option<u64> {
    let s = value.trim().to_uppercase();
    let mut num_str = s.clone();
    let mut multiplier = 1;
    if s.ends_with("GIB") {
        num_str = s.trim_end_matches("GIB").to_string();
        multiplier = 1024 * 1024 * 1024;
    } else if s.ends_with("GB") {
        num_str = s.trim_end_matches("GB").to_string();
        multiplier = 1024 * 1024 * 1024;
    } else if s.ends_with("MIB") {
        num_str = s.trim_end_matches("MIB").to_string();
        multiplier = 1024 * 1024;
    } else if s.ends_with("MB") {
        num_str = s.trim_end_matches("MB").to_string();
        multiplier = 1024 * 1024;
    } else if s.ends_with("KIB") {
        num_str = s.trim_end_matches("KIB").to_string();
        multiplier = 1024;
    } else if s.ends_with("KB") {
        num_str = s.trim_end_matches("KB").to_string();
        multiplier = 1024;
    } else if s.ends_with('B') {
        num_str = s.trim_end_matches('B').to_string();
    }
    num_str.trim().parse::<u64>().ok().map(|n| n * multiplier)
}

pub fn parse_hms_duration(value: &str) -> AppResult<Duration> {
    let parts: Vec<&str> = value.split(':').collect();
    if parts.len() != 3 {
        return Err(AppError::Config(format!("Invalid segment_time: {value}")));
    }
    let h: u64 = parts[0]
        .parse()
        .map_err(|_| AppError::Config(format!("Invalid segment_time: {value}")))?;
    let m: u64 = parts[1]
        .parse()
        .map_err(|_| AppError::Config(format!("Invalid segment_time: {value}")))?;
    let sec: u64 = parts[2]
        .parse()
        .map_err(|_| AppError::Config(format!("Invalid segment_time: {value}")))?;
    Ok(Duration::from_secs(h * 3600 + m * 60 + sec))
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
    fn parse_upload_only_config() {
        let toml = r#"
[upload]
cookie_file = "./data/cookies.json"
"#;
        let config = AppConfig::parse(toml).unwrap();

        // Check defaults for omitted sections
        assert_eq!(config.data.dir, std::path::PathBuf::from("./data"));
        assert_eq!(
            config.record.output_dir,
            std::path::PathBuf::from("./recordings")
        );
        assert!(config.rooms.is_empty());

        assert_eq!(config.upload.source, "直播录像");
    }

    #[test]
    fn record_config_default_matches_schema_defaults() {
        let config = RecordConfig::default();
        assert_eq!(config.output_dir, std::path::PathBuf::from("./recordings"));
        assert_eq!(config.segment_time, None);
        assert_eq!(config.segment_size, None);
        assert_eq!(config.min_segment_size, "20MiB");
        assert!(matches!(config.prefer_protocol, PreferredProtocol::Flv));
        assert_eq!(config.qn, 10000);
        assert!(config.cdn.is_empty());
    }

    #[test]
    fn preferred_protocol_serde_roundtrip() {
        let json = serde_json::to_string(&PreferredProtocol::Flv).unwrap();
        assert_eq!(json, "\"flv\"");

        let hls_ts = serde_json::to_string(&PreferredProtocol::HlsTs).unwrap();
        assert_eq!(hls_ts, "\"hls_ts\"");
        let p: PreferredProtocol = serde_json::from_str("\"hls_ts\"").unwrap();
        assert!(matches!(p, PreferredProtocol::HlsTs));

        let legacy_p: PreferredProtocol = serde_json::from_str("\"hlsts\"").unwrap();
        assert!(matches!(legacy_p, PreferredProtocol::HlsTs));

        let hls_fmp4 = serde_json::to_string(&PreferredProtocol::HlsFmp4).unwrap();
        assert_eq!(hls_fmp4, "\"hls_fmp4\"");
        let f: PreferredProtocol = serde_json::from_str("\"fmp4\"").unwrap();
        assert!(matches!(f, PreferredProtocol::HlsFmp4));
    }

    #[test]
    fn submit_api_serde_roundtrip() {
        let json = serde_json::to_string(&SubmitApi::App).unwrap();
        assert_eq!(json, "\"app\"");
        let s: SubmitApi = serde_json::from_str("\"web\"").unwrap();
        assert!(matches!(s, SubmitApi::Web));
    }

    #[test]
    fn record_validation_rejects_invalid_segment_limits() {
        let mut record = RecordConfig {
            segment_time: Some("bad".into()),
            ..RecordConfig::default()
        };
        assert!(record.validate().is_err());

        record.segment_time = None;
        record.segment_size = Some("bad".into());
        assert!(record.validate().is_err());
    }

    #[test]
    fn upload_validation_rejects_zero_threads() {
        let upload = UploadConfig {
            cookie_file: "cookies.json".into(),
            line: "auto".into(),
            threads: 0,
            submit_api: SubmitApi::App,
            tid: 171,
            copyright: 2,
            source: "source".into(),
            tags: vec![],
        };

        let err = upload.validate().unwrap_err();
        assert!(err.to_string().contains("upload.threads"));
    }
}
