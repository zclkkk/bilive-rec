//! Configuration policy: credential paths are explicit, operational knobs get
//! conservative defaults, and command-specific validation decides which
//! boundaries are required for each action.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::credential::CredentialIdentity;
use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    #[serde(default)]
    pub data: DataConfig,
    #[serde(default)]
    pub credentials: HashMap<String, CredentialConfig>,
    #[serde(default)]
    pub record: RecordConfig,
    #[serde(default)]
    pub upload: Option<UploadConfig>,
    #[serde(default)]
    pub pipeline: PipelineConfig,
    #[serde(default)]
    pub rooms: Vec<RoomConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct CredentialConfig {
    pub cookie_file: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordConfig {
    #[serde(default)]
    pub credential: Option<String>,
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
            credential: None,
            output_dir: PathBuf::from("./data/recordings"),
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
#[serde(deny_unknown_fields)]
pub struct UploadConfig {
    #[serde(default)]
    pub credential: Option<String>,
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
#[serde(deny_unknown_fields)]
pub struct RoomConfig {
    pub name: String,
    pub url: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub record_credential: Option<String>,
    #[serde(default)]
    pub upload_credential: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomCredentials {
    pub record: Option<CredentialIdentity>,
    pub upload: CredentialIdentity,
}

impl RoomCredentials {
    pub fn record_cookie_file(&self) -> Option<&Path> {
        self.record.as_ref().map(CredentialIdentity::cookie_file)
    }

    pub fn upload_cookie_file(&self) -> &Path {
        self.upload.cookie_file()
    }
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
    PathBuf::from("./data/recordings")
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
#[serde(deny_unknown_fields)]
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
        let upload = self.upload_config()?;
        upload.validate()?;
        for room in &self.rooms {
            self.room_credentials(room)?;
        }
        Ok(())
    }

    pub fn validate_for_upload(&self) -> AppResult<()> {
        let upload = self.upload_config()?;
        upload.validate()?;
        self.upload_cookie_file()?;
        Ok(())
    }

    pub fn validate_for_upload_actions(&self) -> AppResult<()> {
        let upload = self.upload_config()?;
        upload.validate()?;
        self.upload_cookie_file()?;
        Ok(())
    }

    pub fn validate_for_check(&self) -> AppResult<()> {
        self.record.validate()?;
        self.record_cookie_file()?;
        Ok(())
    }

    pub fn upload_config(&self) -> AppResult<&UploadConfig> {
        self.upload
            .as_ref()
            .ok_or_else(|| AppError::Config("[upload] config is required for this command".into()))
    }

    pub fn record_cookie_file(&self) -> AppResult<Option<PathBuf>> {
        self.record_credential_identity()
            .map(|credential| credential.map(|credential| credential.cookie_file))
    }

    pub fn record_credential_identity(&self) -> AppResult<Option<CredentialIdentity>> {
        self.record
            .credential
            .as_deref()
            .map(|name| self.credential_identity(name, "record.credential"))
            .transpose()
    }

    pub fn upload_cookie_file(&self) -> AppResult<PathBuf> {
        self.upload_credential_identity()
            .map(|credential| credential.cookie_file)
    }

    pub fn upload_credential_identity(&self) -> AppResult<CredentialIdentity> {
        let upload = self.upload_config()?;
        let name = upload.credential.as_deref().ok_or_else(|| {
            AppError::Config("upload.credential is required for this command".into())
        })?;
        self.credential_identity(name, "upload.credential")
    }

    pub fn room_credentials(&self, room: &RoomConfig) -> AppResult<RoomCredentials> {
        let record = if let Some(name) = room.record_credential.as_deref() {
            Some(
                self.credential_identity(name, &format!("rooms[{}].record_credential", room.name))?,
            )
        } else if let Some(name) = self.record.credential.as_deref() {
            Some(self.credential_identity(name, "record.credential")?)
        } else {
            None
        };

        let upload = self.upload_config()?;
        let upload = if let Some(name) = room.upload_credential.as_deref() {
            self.credential_identity(name, &format!("rooms[{}].upload_credential", room.name))?
        } else if let Some(name) = upload.credential.as_deref() {
            self.credential_identity(name, "upload.credential")?
        } else {
            return Err(AppError::Config(format!(
                "rooms[{}] requires upload_credential or upload.credential",
                room.name
            )));
        };

        Ok(RoomCredentials { record, upload })
    }

    pub fn credential_identity(&self, name: &str, label: &str) -> AppResult<CredentialIdentity> {
        let credential = self.credentials.get(name).ok_or_else(|| {
            AppError::Config(format!("{label} references unknown credential '{name}'"))
        })?;
        validate_cookie_file_path(
            &credential.cookie_file,
            &format!("credentials.{name}.cookie_file"),
        )?;
        Ok(CredentialIdentity::new(
            name,
            credential.cookie_file.clone(),
        ))
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
        // Both SubmitApi::App and SubmitApi::Web are supported; biliup
        // provides submit_by_app and submit_by_web. No restriction here.
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
}

fn validate_cookie_file_path(path: &Path, label: &str) -> AppResult<()> {
    if !path.exists() {
        return Err(AppError::Config(format!(
            "{label} does not exist: {}",
            path.display()
        )));
    }
    if !path.is_file() {
        return Err(AppError::Config(format!(
            "{label} is not a regular file: {}",
            path.display()
        )));
    }
    Ok(())
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

    const SAMPLE_TOML: &str = include_str!("../config.example.toml");

    #[test]
    fn parse_sample_config() {
        let config = AppConfig::parse(SAMPLE_TOML).unwrap();
        assert_eq!(config.data.dir, std::path::PathBuf::from("./data"));
        assert_eq!(
            config.record.output_dir,
            std::path::PathBuf::from("./data/recordings")
        );
        assert_eq!(
            config.credentials["main"].cookie_file,
            std::path::PathBuf::from("./data/cookies.json")
        );
        assert_eq!(config.record.credential.as_deref(), Some("main"));
        assert_eq!(config.record.segment_time.as_deref(), Some("01:00:00"));
        assert_eq!(config.record.segment_size.as_deref(), Some("2GiB"));
        assert_eq!(config.record.min_segment_size, "20MiB");
        assert_eq!(config.record.qn, 10000);
        assert!(config.record.cdn.is_empty());
        let upload = config.upload_config().unwrap();
        assert_eq!(upload.credential.as_deref(), Some("main"));
        assert_eq!(upload.threads, 3);
        assert_eq!(upload.tid, 171);
        assert_eq!(upload.copyright, 2);
        assert_eq!(upload.source, "直播录像");
        assert_eq!(upload.tags, vec!["直播录像"]);
        assert_eq!(config.pipeline.poll_interval_s, 60);
        assert_eq!(config.pipeline.offline_grace_s, 60);
        assert_eq!(config.pipeline.backoff_s, 15);
        assert_eq!(config.rooms.len(), 1);
        assert_eq!(config.rooms[0].name, "example");
        assert_eq!(config.rooms[0].title.as_deref(), Some("{title}"));
        assert_eq!(
            config.rooms[0].description.as_deref(),
            Some("{name} 直播录像\n原直播间：{url}")
        );
    }

    #[test]
    fn parse_config_with_defaults() {
        let toml = r#"
[data]
dir = "./data"

[record]
output_dir = "./rec"

[credentials.main]
cookie_file = "./data/cookies.json"

[upload]
credential = "main"

[[rooms]]
name = "test"
url = "https://live.bilibili.com/1"
"#;
        let config = AppConfig::parse(toml).unwrap();
        assert!(config.record.credential.is_none());
        assert_eq!(config.record.min_segment_size, "20MiB");
        assert_eq!(config.record.qn, 10000);
        let upload = config.upload_config().unwrap();
        assert_eq!(upload.credential.as_deref(), Some("main"));
        assert_eq!(upload.line, "auto");
        assert_eq!(upload.threads, 3);
        assert_eq!(upload.tid, 171);
        assert_eq!(upload.copyright, 2);
    }

    #[test]
    fn parse_upload_only_config() {
        let toml = r#"
[upload]
credential = "main"
"#;
        let config = AppConfig::parse(toml).unwrap();

        // Check defaults for omitted sections
        assert_eq!(config.data.dir, std::path::PathBuf::from("./data"));
        assert_eq!(
            config.record.output_dir,
            std::path::PathBuf::from("./data/recordings")
        );
        assert!(config.rooms.is_empty());
        assert!(config.record.credential.is_none());

        assert_eq!(
            config.upload_config().unwrap().credential.as_deref(),
            Some("main")
        );
        assert_eq!(config.upload_config().unwrap().source, "直播录像");
    }

    #[test]
    fn parse_record_only_config() {
        let toml = r#"
[record]
"#;
        let config = AppConfig::parse(toml).unwrap();

        assert!(config.record.credential.is_none());
        assert!(config.upload.is_none());
    }

    #[test]
    fn parse_rejects_unknown_top_level_and_section_fields() {
        let top = AppConfig::parse(
            r#"
[pipline]
backoff_s = 1
"#,
        )
        .unwrap_err();
        assert!(top.to_string().contains("unknown field"));

        let data = AppConfig::parse(
            r#"
[data]
directory = "./data"
"#,
        )
        .unwrap_err();
        assert!(data.to_string().contains("unknown field"));

        let pipeline = AppConfig::parse(
            r#"
[pipeline]
retry_forever = true
"#,
        )
        .unwrap_err();
        assert!(pipeline.to_string().contains("unknown field"));
    }

    #[test]
    fn check_validation_does_not_require_upload_config() {
        let config = AppConfig::parse("").unwrap();
        config.validate_for_check().unwrap();
    }

    #[test]
    fn credential_paths_are_explicit_not_defaulted() {
        let config = AppConfig::parse("").unwrap();
        assert!(config.record.credential.is_none());
        assert!(config.upload.is_none());

        let config = AppConfig::parse("[upload]\n").unwrap();
        let err = config.validate_for_upload().unwrap_err();
        assert!(err.to_string().contains("upload.credential"));
    }

    #[test]
    fn run_validation_requires_upload_config_when_rooms_exist() {
        let cookie = tempfile::NamedTempFile::new().unwrap();
        let toml = format!(
            r#"
[record]
credential = "main"

[credentials.main]
cookie_file = "{}"

[[rooms]]
name = "test"
url = "https://live.bilibili.com/1"
"#,
            cookie.path().display()
        );
        let config = AppConfig::parse(&toml).unwrap();

        let err = config.validate_for_run().unwrap_err();
        assert!(err.to_string().contains("[upload]"));
    }

    #[test]
    fn room_credentials_resolve_named_overrides() {
        let main_cookie = tempfile::NamedTempFile::new().unwrap();
        let record_cookie = tempfile::NamedTempFile::new().unwrap();
        let upload_cookie = tempfile::NamedTempFile::new().unwrap();
        let toml = format!(
            r#"
[credentials.main]
cookie_file = "{}"

[credentials.record_alt]
cookie_file = "{}"

[credentials.upload_alt]
cookie_file = "{}"

[record]
credential = "main"

[upload]
credential = "main"

[[rooms]]
name = "test"
url = "https://live.bilibili.com/1"
record_credential = "record_alt"
upload_credential = "upload_alt"
"#,
            main_cookie.path().display(),
            record_cookie.path().display(),
            upload_cookie.path().display()
        );
        let config = AppConfig::parse(&toml).unwrap();
        let credentials = config.room_credentials(&config.rooms[0]).unwrap();

        assert_eq!(
            credentials
                .record
                .as_ref()
                .map(|credential| credential.cookie_file.as_path()),
            Some(record_cookie.path())
        );
        assert_eq!(credentials.upload.name, "upload_alt");
        assert_eq!(credentials.upload.cookie_file, upload_cookie.path());
    }

    #[test]
    fn record_validation_rejects_missing_cookie_file() {
        let config = AppConfig::parse(
            r#"
[credentials.main]
cookie_file = "./definitely-missing-live-cookie.json"

[record]
credential = "main"
"#,
        )
        .unwrap();

        let err = config.validate_for_check().unwrap_err();
        assert!(err.to_string().contains("credentials.main.cookie_file"));
    }

    #[test]
    fn record_config_default_matches_schema_defaults() {
        let config = RecordConfig::default();
        assert!(config.credential.is_none());
        assert_eq!(
            config.output_dir,
            std::path::PathBuf::from("./data/recordings")
        );
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
            credential: Some("main".into()),
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

    #[test]
    fn upload_validation_accepts_both_submit_apis() {
        // SubmitApi::Web was previously declared but rejected — the config
        // schema exposed a variant the code refused to honor. With biliup's
        // submit_by_web wired up (Finding 8), both variants must validate.
        for api in [SubmitApi::App, SubmitApi::Web] {
            let upload = UploadConfig {
                credential: Some("main".into()),
                line: "auto".into(),
                threads: 3,
                submit_api: api,
                tid: 171,
                copyright: 2,
                source: "source".into(),
                tags: vec![],
            };
            upload
                .validate()
                .expect("both App and Web submit APIs must validate");
        }
    }
}
