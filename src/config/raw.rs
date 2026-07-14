use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::credential::{CredentialRef, UploadPrincipal};
use crate::error::{AppError, AppResult};
use crate::submission_template::validate_room_template;

use super::defaults;
use super::resolved::{
    CheckConfig, ResolvedRecordConfig, ResolvedRoomConfig, ResolvedRoomOutput,
    ResolvedRoomUploadConfig, ResolvedSubmitConfig, RunConfig,
};
use super::validation::{
    ConfigValueError, parse_hms_duration, parse_size_bytes, validate_cookie_file_path,
    validate_upload_cookie_file,
};

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
    pub submit: SubmitConfig,
    #[serde(default)]
    pub pipeline: PipelineConfig,
    #[serde(default)]
    pub rooms: BTreeMap<String, RoomConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataConfig {
    #[serde(default = "defaults::data_dir")]
    pub dir: PathBuf,
}

impl Default for DataConfig {
    fn default() -> Self {
        Self {
            dir: defaults::data_dir(),
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
    #[serde(default = "defaults::output_dir")]
    pub output_dir: PathBuf,
    #[serde(default)]
    pub segment_time: Option<String>,
    #[serde(default)]
    pub segment_size: Option<String>,
    #[serde(default = "defaults::min_segment_size")]
    pub min_segment_size: String,
    #[serde(default = "defaults::qn")]
    pub qn: u32,
    #[serde(default)]
    pub cdn: Vec<String>,
}

impl Default for RecordConfig {
    fn default() -> Self {
        Self {
            credential: None,
            output_dir: defaults::output_dir(),
            segment_time: None,
            segment_size: None,
            min_segment_size: defaults::min_segment_size(),
            qn: defaults::qn(),
            cdn: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadConfig {
    #[serde(default)]
    pub credential: Option<String>,
    #[serde(default = "defaults::line")]
    pub line: String,
    #[serde(default = "defaults::threads")]
    pub threads: usize,
    #[serde(default)]
    pub submit_api: SubmitApi,
    #[serde(default)]
    pub delete_after_submit: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitConfig {
    #[serde(default = "defaults::title_template")]
    pub title: Option<String>,
    #[serde(default = "defaults::description_template")]
    pub description: Option<String>,
    #[serde(default = "defaults::category_id")]
    pub category_id: u16,
    #[serde(default = "defaults::copyright")]
    pub copyright: Copyright,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub private: bool,
    #[serde(default)]
    pub dynamic: String,
    #[serde(default)]
    pub forbid_reprint: bool,
    #[serde(default)]
    pub charging_panel: bool,
    #[serde(default)]
    pub close_reply: bool,
    #[serde(default)]
    pub close_danmu: bool,
    #[serde(default)]
    pub featured_reply: bool,
}

impl Default for SubmitConfig {
    fn default() -> Self {
        Self {
            title: None,
            description: None,
            category_id: defaults::category_id(),
            copyright: defaults::copyright(),
            source: None,
            tags: Vec::new(),
            private: false,
            dynamic: String::new(),
            forbid_reprint: false,
            charging_panel: false,
            close_reply: false,
            close_danmu: false,
            featured_reply: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoomConfig {
    pub url: String,
    #[serde(default)]
    pub record: RoomRecordConfig,
    #[serde(default)]
    pub upload: RoomUploadConfig,
    #[serde(default)]
    pub submit: RoomSubmitConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoomRecordConfig {
    #[serde(default)]
    pub credential: Option<String>,
    #[serde(default)]
    pub qn: Option<u32>,
    #[serde(default)]
    pub cdn: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoomUploadConfig {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub credential: Option<String>,
    #[serde(default)]
    pub delete_after_submit: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoomSubmitConfig {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub category_id: Option<u16>,
    #[serde(default)]
    pub copyright: Option<Copyright>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub private: Option<bool>,
    #[serde(default)]
    pub dynamic: Option<String>,
    #[serde(default)]
    pub forbid_reprint: Option<bool>,
    #[serde(default)]
    pub charging_panel: Option<bool>,
    #[serde(default)]
    pub close_reply: Option<bool>,
    #[serde(default)]
    pub close_danmu: Option<bool>,
    #[serde(default)]
    pub featured_reply: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Copyright {
    Original,
    Reprint,
}

impl Copyright {
    pub fn as_biliup_code(self) -> u8 {
        match self {
            Self::Original => 1,
            Self::Reprint => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SubmitApi {
    #[default]
    App,
    Web,
    #[serde(rename = "bcut_android")]
    BCutAndroid,
}

impl SubmitApi {
    pub fn as_config_value(&self) -> &'static str {
        match self {
            Self::App => "app",
            Self::Web => "web",
            Self::BCutAndroid => "bcut_android",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    #[serde(default = "defaults::poll_interval_s")]
    pub poll_interval_s: u64,
    #[serde(default = "defaults::offline_grace_s")]
    pub offline_grace_s: u64,
    #[serde(default = "defaults::backoff_s")]
    pub backoff_s: u64,
    #[serde(default = "defaults::max_backoff_s")]
    pub max_backoff_s: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            poll_interval_s: defaults::poll_interval_s(),
            offline_grace_s: defaults::offline_grace_s(),
            backoff_s: defaults::backoff_s(),
            max_backoff_s: defaults::max_backoff_s(),
        }
    }
}

impl AppConfig {
    pub fn load(path: &std::path::Path) -> AppResult<Self> {
        let locator = absolute_locator(path)?;
        let content = std::fs::read_to_string(&locator).map_err(|e| AppError::Io {
            path: locator.clone(),
            source: e,
        })?;
        let mut config = Self::parse(&content)?;
        let base = locator.parent().ok_or_else(|| {
            AppError::Config(format!(
                "configuration locator has no parent: {}",
                locator.display()
            ))
        })?;
        config.resolve_path_locators(base);
        Ok(config)
    }

    pub fn parse(content: &str) -> AppResult<Self> {
        toml::from_str(content).map_err(|e| AppError::Config(e.to_string()))
    }

    pub fn resolve_for_run(&self) -> AppResult<RunConfig> {
        self.pipeline.validate()?;
        self.record.validate()?;
        if let Some(upload) = &self.upload {
            upload.validate()?;
        }

        let mut rooms = Vec::with_capacity(self.rooms.len());
        for (name, room) in &self.rooms {
            rooms.push(self.resolve_room(name, room, self.upload.as_ref())?);
        }

        Ok(RunConfig {
            data: self.data.clone(),
            pipeline: self.pipeline.clone(),
            rooms,
        })
    }

    pub fn resolve_for_check(&self) -> AppResult<CheckConfig> {
        self.record.validate()?;
        let record = self.resolve_record_config(None, "record")?;
        Ok(CheckConfig {
            data: self.data.clone(),
            record,
        })
    }

    pub fn credential_identity(&self, name: &str, label: &str) -> AppResult<CredentialRef> {
        let credential = self.credentials.get(name).ok_or_else(|| {
            AppError::Config(format!("{label} references unknown credential '{name}'"))
        })?;
        validate_cookie_file_path(
            &credential.cookie_file,
            &format!("credentials.{name}.cookie_file"),
        )?;
        Ok(CredentialRef::new(name, credential.cookie_file.clone()))
    }

    fn resolve_room(
        &self,
        name: &str,
        room: &RoomConfig,
        upload: Option<&UploadConfig>,
    ) -> AppResult<ResolvedRoomConfig> {
        validate_name(name, &format!("rooms.{name}"))?;
        let record =
            self.resolve_record_config(Some(&room.record), &format!("rooms.{name}.record"))?;
        let upload_enabled = room.upload.enabled.unwrap_or(upload.is_some());
        let output = if upload_enabled {
            let upload = upload.ok_or_else(|| {
                AppError::Config(format!(
                    "rooms.{name}.upload.enabled is true, but [upload] is not configured"
                ))
            })?;
            let upload = self.resolve_room_upload_config(name, room, upload)?;
            let submit = self.resolve_submit_config(
                Some(&room.submit),
                &format!("rooms.{name}.submit"),
                true,
                Some("{url}"),
            )?;
            ResolvedRoomOutput::Bilibili {
                upload,
                submit: Box::new(submit),
            }
        } else {
            if room.upload.credential.is_some() || room.upload.delete_after_submit.is_some() {
                return Err(AppError::Config(format!(
                    "rooms.{name}.upload is disabled; credential and delete_after_submit must not be set"
                )));
            }
            ResolvedRoomOutput::LocalOnly
        };

        Ok(ResolvedRoomConfig {
            name: name.to_string(),
            url: room.url.clone(),
            record,
            output,
        })
    }

    fn resolve_record_config(
        &self,
        room: Option<&RoomRecordConfig>,
        label: &str,
    ) -> AppResult<ResolvedRecordConfig> {
        let credential_name = room
            .and_then(|room| room.credential.as_deref())
            .or(self.record.credential.as_deref());
        let credential = credential_name
            .map(|name| self.credential_identity(name, &format!("{label}.credential")))
            .transpose()?;
        let segment_time = self.record.segment_time_duration()?;
        let segment_size = self.record.segment_size_bytes()?;
        let min_segment_size = self.record.min_segment_size_bytes()?;
        let qn = room.and_then(|room| room.qn).unwrap_or(self.record.qn);
        let cdn = room
            .and_then(|room| room.cdn.clone())
            .unwrap_or_else(|| self.record.cdn.clone());
        Ok(ResolvedRecordConfig {
            credential,
            output_dir: self.record.output_dir.clone(),
            segment_time,
            segment_size,
            min_segment_size,
            qn,
            cdn,
        })
    }

    fn resolve_room_upload_config(
        &self,
        room_name: &str,
        room: &RoomConfig,
        upload: &UploadConfig,
    ) -> AppResult<ResolvedRoomUploadConfig> {
        let credential_name = room
            .upload
            .credential
            .as_deref()
            .or(upload.credential.as_deref())
            .ok_or_else(|| {
                AppError::Config(format!(
                    "rooms.{room_name}.upload.credential or upload.credential is required"
                ))
            })?;

        let credential = self.credential_identity(
            credential_name,
            &format!("rooms.{room_name}.upload.credential"),
        )?;
        let expected_mid = validate_upload_cookie_file(
            credential.cookie_file(),
            &format!("credentials.{credential_name}.cookie_file"),
        )?;

        Ok(ResolvedRoomUploadConfig {
            principal: UploadPrincipal::new(credential, expected_mid),
            line: upload.line.clone(),
            threads: upload.threads,
            submit_api: upload.submit_api.clone(),
            delete_after_submit: room
                .upload
                .delete_after_submit
                .unwrap_or(upload.delete_after_submit),
        })
    }

    fn resolve_submit_config(
        &self,
        room: Option<&RoomSubmitConfig>,
        label: &str,
        validate_templates: bool,
        default_reprint_source: Option<&str>,
    ) -> AppResult<ResolvedSubmitConfig> {
        let submit = &self.submit;
        let title = room
            .and_then(|room| room.title.clone())
            .or_else(|| submit.title.clone());
        let description = room
            .and_then(|room| room.description.clone())
            .or_else(|| submit.description.clone());
        let category_id = room
            .and_then(|room| room.category_id)
            .unwrap_or(submit.category_id);
        let copyright = room
            .and_then(|room| room.copyright)
            .unwrap_or(submit.copyright);
        let source = room
            .and_then(|room| room.source.clone())
            .or_else(|| submit.source.clone());
        let tags = room
            .and_then(|room| room.tags.clone())
            .unwrap_or_else(|| submit.tags.clone());

        if category_id == 0 {
            return Err(AppError::Config(format!(
                "{label}.category_id must be greater than 0"
            )));
        }
        if validate_templates {
            if let Some(template) = &title {
                validate_room_template(template)
                    .map_err(|err| label_config_error(label, "title", err))?;
            }
            if let Some(template) = &description {
                validate_room_template(template)
                    .map_err(|err| label_config_error(label, "description", err))?;
            }
        }
        let source = if copyright == Copyright::Reprint {
            let source = source
                .or_else(|| default_reprint_source.map(str::to_owned))
                .ok_or_else(|| {
                    AppError::Config(format!(
                        "{label}.source is required when copyright = \"reprint\""
                    ))
                })?;
            if source.trim().is_empty() {
                return Err(AppError::Config(format!(
                    "{label}.source must not be empty when copyright = \"reprint\""
                )));
            }
            if validate_templates {
                validate_room_template(&source)
                    .map_err(|err| label_config_error(label, "source", err))?;
            } else if source.contains('{') || source.contains('}') {
                return Err(AppError::Config(format!(
                    "{label}.source templates are only supported for room submissions"
                )));
            }
            source
        } else {
            String::new()
        };

        Ok(ResolvedSubmitConfig {
            title,
            description,
            category_id,
            copyright,
            source,
            tags,
            private: room.and_then(|room| room.private).unwrap_or(submit.private),
            dynamic: room
                .and_then(|room| room.dynamic.clone())
                .unwrap_or_else(|| submit.dynamic.clone()),
            forbid_reprint: room
                .and_then(|room| room.forbid_reprint)
                .unwrap_or(submit.forbid_reprint),
            charging_panel: room
                .and_then(|room| room.charging_panel)
                .unwrap_or(submit.charging_panel),
            close_reply: room
                .and_then(|room| room.close_reply)
                .unwrap_or(submit.close_reply),
            close_danmu: room
                .and_then(|room| room.close_danmu)
                .unwrap_or(submit.close_danmu),
            featured_reply: room
                .and_then(|room| room.featured_reply)
                .unwrap_or(submit.featured_reply),
        })
    }

    fn resolve_path_locators(&mut self, base: &Path) {
        absolutize(&mut self.data.dir, base);
        absolutize(&mut self.record.output_dir, base);
        for credential in self.credentials.values_mut() {
            absolutize(&mut credential.cookie_file, base);
        }
    }
}

fn absolute_locator(path: &Path) -> AppResult<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .map_err(|source| AppError::Io {
            path: PathBuf::from("."),
            source,
        })
}

fn absolutize(path: &mut PathBuf, base: &Path) {
    if path.is_relative() {
        *path = base.join(&*path);
    }
}

impl RecordConfig {
    pub fn validate(&self) -> AppResult<()> {
        let min_segment_size = self.min_segment_size_bytes()?;
        self.segment_time_duration()?;
        if let Some(segment_size) = self.segment_size_bytes()?
            && min_segment_size > segment_size
        {
            return Err(AppError::Config(format!(
                "record.min_segment_size ({min_segment_size} bytes) must not exceed record.segment_size ({segment_size} bytes)"
            )));
        }
        Ok(())
    }

    pub fn min_segment_size_bytes(&self) -> AppResult<u64> {
        parse_size_bytes(&self.min_segment_size)
            .map_err(|err| value_config_error("record.min_segment_size", err))
    }

    pub fn segment_time_duration(&self) -> AppResult<Option<std::time::Duration>> {
        self.segment_time
            .as_deref()
            .map(|value| {
                let duration = parse_hms_duration(value)
                    .map_err(|err| value_config_error("record.segment_time", err))?;
                if duration.is_zero() {
                    return Err(value_config_error(
                        "record.segment_time",
                        ConfigValueError::ZeroNotAllowed,
                    ));
                }
                Ok(duration)
            })
            .transpose()
    }

    pub fn segment_size_bytes(&self) -> AppResult<Option<u64>> {
        self.segment_size
            .as_deref()
            .map(|value| {
                let size = parse_size_bytes(value)
                    .map_err(|err| value_config_error("record.segment_size", err))?;
                if size == 0 {
                    return Err(value_config_error(
                        "record.segment_size",
                        ConfigValueError::ZeroNotAllowed,
                    ));
                }
                Ok(size)
            })
            .transpose()
    }
}

impl UploadConfig {
    pub fn validate(&self) -> AppResult<()> {
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
        if self.max_backoff_s == 0 {
            return Err(AppError::Config(
                "pipeline.max_backoff_s must be greater than 0".into(),
            ));
        }
        if self.max_backoff_s < self.backoff_s {
            return Err(AppError::Config(
                "pipeline.max_backoff_s must be greater than or equal to pipeline.backoff_s".into(),
            ));
        }
        Ok(())
    }
}

fn validate_name(name: &str, label: &str) -> AppResult<()> {
    if name.trim().is_empty() {
        return Err(AppError::Config(format!("{label} name must not be empty")));
    }
    Ok(())
}

fn label_config_error(label: &str, field: &str, err: AppError) -> AppError {
    match err {
        AppError::Config(message) => AppError::Config(format!("{label}.{field}: {message}")),
        err => err,
    }
}

fn value_config_error(label: &str, err: ConfigValueError) -> AppError {
    AppError::Config(format!("{label}: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ResolvedRoomOutput;

    const SAMPLE_TOML: &str = include_str!("../../config.example.toml");

    fn upload_cookie() -> tempfile::NamedTempFile {
        use std::io::Write;

        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(
            br#"{
                "cookie_info": {
                    "cookies": [{"name": "SESSDATA", "value": "test"}]
                },
                "sso": [],
                "token_info": {
                    "access_token": "test",
                    "expires_in": 3600,
                    "mid": 1,
                    "refresh_token": "test"
                },
                "platform": null
            }"#,
        )
        .unwrap();
        file.flush().unwrap();
        file
    }

    fn bilibili_output(
        room: &ResolvedRoomConfig,
    ) -> (&ResolvedRoomUploadConfig, &ResolvedSubmitConfig) {
        match &room.output {
            ResolvedRoomOutput::Bilibili { upload, submit } => (upload, submit),
            ResolvedRoomOutput::LocalOnly => panic!("expected Bilibili room output"),
        }
    }

    #[test]
    fn parse_sample_config_and_resolve_run() {
        let cookie = upload_cookie();
        let toml = SAMPLE_TOML.replace("./data/cookies.json", &cookie.path().display().to_string());
        let config = AppConfig::parse(&toml).unwrap();
        let run = config.resolve_for_run().unwrap();

        assert_eq!(run.data.dir, std::path::PathBuf::from("./data"));
        assert_eq!(run.rooms.len(), 1);

        let room = &run.rooms[0];
        let (upload, submit) = bilibili_output(room);
        assert_eq!(room.name, "example");
        assert_eq!(room.url, "https://live.bilibili.com/123456");
        assert_eq!(upload.line, "auto");
        assert_eq!(upload.threads, 3);
        assert_eq!(room.record.qn, 10000);
        assert!(room.record.cdn.is_empty());
        assert_eq!(
            submit.title.as_deref(),
            Some("{title} {started_at:%Y-%m-%d}")
        );
        assert_eq!(submit.category_id, 171);
        assert_eq!(submit.copyright, Copyright::Reprint);
        assert_eq!(submit.source, "{url}");
        assert_eq!(submit.tags, vec!["直播录像"]);
        assert!(!submit.private);
        assert!(!upload.delete_after_submit);
    }

    #[test]
    fn resolve_defaults_for_check_without_upload_config() {
        let config = AppConfig::parse("").unwrap();
        let check = config.resolve_for_check().unwrap();

        assert_eq!(
            check.record.output_dir,
            std::path::PathBuf::from("./data/recordings")
        );
        assert_eq!(check.record.min_segment_size, 20 * 1024 * 1024);
        assert_eq!(check.record.qn, 10000);
        assert!(check.record.credential.is_none());
    }

    #[test]
    fn resolve_room_overrides_record_upload_and_submit() {
        let main_cookie = upload_cookie();
        let record_cookie = tempfile::NamedTempFile::new().unwrap();
        let upload_cookie = upload_cookie();
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
cdn = ["global"]

[upload]
credential = "main"
delete_after_submit = true

[submit]
title = "{{title}}"
category_id = 171
copyright = "reprint"
source = "global source"
tags = ["global"]

[rooms.test]
url = "https://live.bilibili.com/1"

[rooms.test.record]
credential = "record_alt"
qn = 400
cdn = []

[rooms.test.upload]
credential = "upload_alt"
delete_after_submit = false

[rooms.test.submit]
category_id = 65
copyright = "original"
source = "ignored for original"
tags = []
private = true
dynamic = "room dynamic"
forbid_reprint = true
charging_panel = true
close_reply = true
close_danmu = true
featured_reply = true
"#,
            main_cookie.path().display(),
            record_cookie.path().display(),
            upload_cookie.path().display()
        );
        let config = AppConfig::parse(&toml).unwrap();
        let run = config.resolve_for_run().unwrap();
        let room = &run.rooms[0];
        let (upload, submit) = bilibili_output(room);

        assert_eq!(
            room.record
                .credential
                .as_ref()
                .map(|credential| credential.cookie_file.as_path()),
            Some(record_cookie.path())
        );
        assert_eq!(room.record.qn, 400);
        assert!(room.record.cdn.is_empty());
        assert!(!upload.delete_after_submit);
        assert_eq!(upload.principal.credential.name, "upload_alt");
        assert_eq!(upload.principal.expected_mid, 1);
        assert_eq!(
            upload.principal.credential.cookie_file,
            upload_cookie.path()
        );
        assert_eq!(submit.category_id, 65);
        assert_eq!(submit.copyright, Copyright::Original);
        assert_eq!(submit.source, "");
        assert!(submit.tags.is_empty());
        assert!(submit.private);
        assert_eq!(submit.dynamic, "room dynamic");
        assert!(submit.forbid_reprint);
        assert!(submit.charging_panel);
        assert!(submit.close_reply);
        assert!(submit.close_danmu);
        assert!(submit.featured_reply);
    }

    #[test]
    fn run_without_upload_config_resolves_rooms_as_local_only() {
        let toml = r#"
[rooms.test]
url = "https://live.bilibili.com/1"
"#;
        let config = AppConfig::parse(toml).unwrap();
        let run = config.resolve_for_run().unwrap();
        assert_eq!(run.rooms[0].output, ResolvedRoomOutput::LocalOnly);
    }

    #[test]
    fn run_resolution_allows_zero_current_rooms() {
        let run = AppConfig::parse("").unwrap().resolve_for_run().unwrap();
        assert!(run.rooms.is_empty());
    }

    #[test]
    fn record_rejects_retention_floor_above_rotation_limit() {
        let config = AppConfig::parse(
            r#"
[record]
segment_size = "10MiB"
min_segment_size = "20MiB"
"#,
        )
        .unwrap();
        let error = config.resolve_for_run().unwrap_err();
        assert!(error.to_string().contains("must not exceed"));
    }

    #[test]
    fn load_makes_all_runtime_paths_absolute_from_config_locator() {
        use std::io::Write;

        let dir = tempfile::TempDir::new().unwrap();
        let config_dir = dir.path().join("configuration");
        std::fs::create_dir_all(&config_dir).unwrap();
        let cookie_path = config_dir.join("cookie.json");
        std::fs::write(&cookie_path, b"cookie").unwrap();
        let config_path = config_dir.join("config.toml");
        let mut file = std::fs::File::create(&config_path).unwrap();
        writeln!(file, "[data]\ndir = '../state'").unwrap();
        writeln!(file, "[record]\noutput_dir = './recordings'").unwrap();
        writeln!(file, "[credentials.main]\ncookie_file = 'cookie.json'").unwrap();

        let config = AppConfig::load(&config_path).unwrap();
        assert_eq!(config.data.dir, config_dir.join("../state"));
        assert_eq!(config.record.output_dir, config_dir.join("./recordings"));
        assert_eq!(config.credentials["main"].cookie_file, cookie_path);
        assert!(config.data.dir.is_absolute());
        assert!(config.record.output_dir.is_absolute());
    }

    #[cfg(unix)]
    #[test]
    fn config_symlink_uses_the_symlink_locator_directory_as_base() {
        let dir = tempfile::TempDir::new().unwrap();
        let target_dir = dir.path().join("target");
        let locator_dir = dir.path().join("locator");
        std::fs::create_dir_all(&target_dir).unwrap();
        std::fs::create_dir_all(&locator_dir).unwrap();
        let target = target_dir.join("config.toml");
        std::fs::write(&target, "[data]\ndir = 'state'\n").unwrap();
        let locator = locator_dir.join("config.toml");
        std::os::unix::fs::symlink(&target, &locator).unwrap();

        let config = AppConfig::load(&locator).unwrap();
        assert_eq!(config.data.dir, locator_dir.join("state"));
    }

    #[test]
    fn room_upload_is_enabled_by_default_when_global_upload_exists() {
        let cookie = upload_cookie();
        let toml = format!(
            r#"
[credentials.main]
cookie_file = "{}"

[upload]
credential = "main"

[rooms.test]
url = "https://live.bilibili.com/1"
"#,
            cookie.path().display()
        );
        let run = AppConfig::parse(&toml).unwrap().resolve_for_run().unwrap();
        assert!(matches!(
            run.rooms[0].output,
            ResolvedRoomOutput::Bilibili { .. }
        ));
    }

    #[test]
    fn room_can_disable_upload_in_mixed_run() {
        let cookie = upload_cookie();
        let toml = format!(
            r#"
[credentials.main]
cookie_file = "{}"

[upload]
credential = "main"

[rooms.uploaded]
url = "https://live.bilibili.com/1"

[rooms.local]
url = "https://live.bilibili.com/2"

[rooms.local.upload]
enabled = false
"#,
            cookie.path().display()
        );
        let run = AppConfig::parse(&toml).unwrap().resolve_for_run().unwrap();
        let local = run.rooms.iter().find(|room| room.name == "local").unwrap();
        let uploaded = run
            .rooms
            .iter()
            .find(|room| room.name == "uploaded")
            .unwrap();
        assert_eq!(local.output, ResolvedRoomOutput::LocalOnly);
        assert!(matches!(
            uploaded.output,
            ResolvedRoomOutput::Bilibili { .. }
        ));
    }

    #[test]
    fn room_cannot_enable_upload_without_global_upload_config() {
        let config = AppConfig::parse(
            r#"
[rooms.test]
url = "https://live.bilibili.com/1"

[rooms.test.upload]
enabled = true
"#,
        )
        .unwrap();
        let err = config.resolve_for_run().unwrap_err();
        assert!(err.to_string().contains("[upload] is not configured"));
    }

    #[test]
    fn disabled_room_rejects_unused_upload_overrides() {
        let config = AppConfig::parse(
            r#"
[rooms.test]
url = "https://live.bilibili.com/1"

[rooms.test.upload]
enabled = false
credential = "unused"
"#,
        )
        .unwrap();
        let err = config.resolve_for_run().unwrap_err();
        assert!(err.to_string().contains("upload is disabled"));
    }

    #[test]
    fn rejects_unknown_fields_at_boundaries() {
        let top = AppConfig::parse("[pipline]\nbackoff_s = 1\n").unwrap_err();
        assert!(top.to_string().contains("unknown field"));

        let upload = AppConfig::parse("[upload]\nextraneous = true\n").unwrap_err();
        assert!(upload.to_string().contains("unknown field"));

        let room = AppConfig::parse(
            r#"
[rooms.test]
url = "https://live.bilibili.com/1"

[rooms.test.submit]
mystery = true
"#,
        )
        .unwrap_err();
        assert!(room.to_string().contains("unknown field"));
    }

    #[test]
    fn reprint_requires_source() {
        let cookie = upload_cookie();
        let toml = format!(
            r#"
[credentials.main]
cookie_file = "{}"

[upload]
credential = "main"

[submit]
copyright = "reprint"
source = ""

[rooms.test]
url = "https://live.bilibili.com/1"
"#,
            cookie.path().display()
        );
        let config = AppConfig::parse(&toml).unwrap();
        let err = config.resolve_for_run().unwrap_err();
        assert!(err.to_string().contains("source"));
    }

    #[test]
    fn run_defaults_reprint_source_to_room_url_template() {
        let cookie = upload_cookie();
        let toml = format!(
            r#"
[credentials.main]
cookie_file = "{}"

[upload]
credential = "main"

[rooms.test]
url = "https://live.bilibili.com/1"
"#,
            cookie.path().display()
        );
        let config = AppConfig::parse(&toml).unwrap();
        let run = config.resolve_for_run().unwrap();
        let (_, submit) = bilibili_output(&run.rooms[0]);
        assert_eq!(submit.source, "{url}");
    }

    #[test]
    fn run_rejects_invalid_submit_template() {
        let cookie = upload_cookie();
        let toml = format!(
            r#"
[credentials.main]
cookie_file = "{}"

[upload]
credential = "main"

[submit]
title = "{{started_at:%}}"

[rooms.test]
url = "https://live.bilibili.com/1"
"#,
            cookie.path().display()
        );
        let config = AppConfig::parse(&toml).unwrap();
        let err = config.resolve_for_run().unwrap_err();
        assert!(err.to_string().contains("invalid started_at format"));
    }

    #[test]
    fn run_rejects_invalid_source_template() {
        let cookie = upload_cookie();
        let toml = format!(
            r#"
[credentials.main]
cookie_file = "{}"

[upload]
credential = "main"

[submit]
source = "{{unknown}}"

[rooms.test]
url = "https://live.bilibili.com/1"
"#,
            cookie.path().display()
        );
        let config = AppConfig::parse(&toml).unwrap();
        let err = config.resolve_for_run().unwrap_err();
        assert!(err.to_string().contains("submit.source"));
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
    fn record_validation_rejects_zero_rotation_limits() {
        let mut record = RecordConfig {
            segment_time: Some("00:00:00".into()),
            ..RecordConfig::default()
        };
        let err = record.validate().unwrap_err();
        assert!(err.to_string().contains("record.segment_time"));
        assert!(err.to_string().contains("greater than zero"));

        record.segment_time = None;
        record.segment_size = Some("0".into());
        let err = record.validate().unwrap_err();
        assert!(err.to_string().contains("record.segment_size"));
        assert!(err.to_string().contains("greater than zero"));
    }

    #[test]
    fn record_validation_allows_zero_min_segment_size() {
        let record = RecordConfig {
            min_segment_size: "0".into(),
            ..RecordConfig::default()
        };

        assert_eq!(record.min_segment_size_bytes().unwrap(), 0);
    }

    #[test]
    fn upload_validation_rejects_zero_threads() {
        let upload = UploadConfig {
            credential: Some("main".into()),
            line: "auto".into(),
            threads: 0,
            submit_api: SubmitApi::App,
            delete_after_submit: false,
        };

        let err = upload.validate().unwrap_err();
        assert!(err.to_string().contains("upload.threads"));
    }

    #[test]
    fn upload_validation_accepts_all_submit_apis() {
        for api in [SubmitApi::App, SubmitApi::Web, SubmitApi::BCutAndroid] {
            let upload = UploadConfig {
                credential: Some("main".into()),
                line: "auto".into(),
                threads: 3,
                submit_api: api,
                delete_after_submit: false,
            };
            upload.validate().expect("all submit APIs must validate");
        }
    }

    #[test]
    fn pipeline_validation_rejects_invalid_backoff_bounds() {
        let mut pipeline = PipelineConfig {
            max_backoff_s: 0,
            ..PipelineConfig::default()
        };
        assert!(pipeline.validate().is_err());

        pipeline.max_backoff_s = 1;
        pipeline.backoff_s = 15;
        assert!(pipeline.validate().is_err());
    }

    #[test]
    fn submit_api_serde_roundtrip() {
        let json = serde_json::to_string(&SubmitApi::App).unwrap();
        assert_eq!(json, "\"app\"");
        let s: SubmitApi = serde_json::from_str("\"web\"").unwrap();
        assert!(matches!(s, SubmitApi::Web));
        let json = serde_json::to_string(&SubmitApi::BCutAndroid).unwrap();
        assert_eq!(json, "\"bcut_android\"");
        let s: SubmitApi = serde_json::from_str("\"bcut_android\"").unwrap();
        assert!(matches!(s, SubmitApi::BCutAndroid));

        assert!(serde_json::from_str::<SubmitApi>("\"bcutandroid\"").is_err());
        assert!(serde_json::from_str::<SubmitApi>("\"b-cut-android\"").is_err());
    }
}
