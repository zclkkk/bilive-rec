use std::fs::{File, OpenOptions};
use std::io::{BufReader, Seek, Write};
use std::path::{Path, PathBuf};

use biliup::uploader::credential::LoginInfo;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialRef {
    pub name: String,
    pub cookie_file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadPrincipal {
    pub credential: CredentialRef,
    pub expected_mid: u64,
}

impl UploadPrincipal {
    pub fn new(credential: CredentialRef, expected_mid: u64) -> Self {
        Self {
            credential,
            expected_mid,
        }
    }

    pub fn cookie_file(&self) -> &Path {
        self.credential.cookie_file()
    }
}

impl CredentialRef {
    pub fn new(name: impl Into<String>, cookie_file: impl Into<PathBuf>) -> Self {
        Self {
            name: name.into(),
            cookie_file: cookie_file.into(),
        }
    }

    pub fn cookie_file(&self) -> &Path {
        &self.cookie_file
    }
}

/// An upload credential opened once for one remote boundary.
///
/// biliup's `LoginInfo` deliberately keeps the upstream wire shape, including
/// an untyped `cookie_info`. Validate that shape here before biliup can reach
/// its infallible cookie-store assumptions. Keeping the file handle also means
/// a token refresh is written back to the same document that was validated.
pub struct UploadCredentialFile {
    path: PathBuf,
    file: File,
    login_info: LoginInfo,
}

impl UploadCredentialFile {
    pub fn open(path: &Path) -> Result<Self, UploadCredentialError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|source| UploadCredentialError::Open {
                path: path.to_path_buf(),
                source,
            })?;
        let login_info = serde_json::from_reader(BufReader::new(&file)).map_err(|source| {
            UploadCredentialError::InvalidJson {
                path: path.to_path_buf(),
                source,
            }
        })?;
        validate_login_info(&login_info).map_err(|problem| {
            UploadCredentialError::InvalidShape {
                path: path.to_path_buf(),
                problem,
            }
        })?;

        Ok(Self {
            path: path.to_path_buf(),
            file,
            login_info,
        })
    }

    pub fn login_info(&self) -> &LoginInfo {
        &self.login_info
    }

    pub fn replace(&mut self, login_info: LoginInfo) -> Result<(), UploadCredentialError> {
        validate_login_info(&login_info).map_err(|problem| {
            UploadCredentialError::InvalidShape {
                path: self.path.clone(),
                problem,
            }
        })?;

        // Serialize before truncating so a serialization failure cannot damage
        // the credential that is still valid on disk.
        let document = serde_json::to_vec_pretty(&login_info).map_err(|source| {
            UploadCredentialError::Serialize {
                path: self.path.clone(),
                source,
            }
        })?;
        self.file
            .rewind()
            .and_then(|_| self.file.set_len(0))
            .and_then(|_| self.file.write_all(&document))
            .and_then(|_| self.file.flush())
            .and_then(|_| self.file.sync_data())
            .map_err(|source| UploadCredentialError::Persist {
                path: self.path.clone(),
                source,
            })?;
        self.login_info = login_info;
        Ok(())
    }
}

pub fn validate_login_info(login_info: &LoginInfo) -> Result<(), String> {
    if login_info.token_info.mid == 0 {
        return Err("token_info.mid must be a non-zero Bilibili account mid".into());
    }
    if login_info.token_info.access_token.trim().is_empty() {
        return Err("token_info.access_token must not be empty".into());
    }

    let cookies = login_info
        .cookie_info
        .get("cookies")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "cookie_info.cookies must be an array".to_string())?;
    if cookies.is_empty() {
        return Err("cookie_info.cookies must contain at least one cookie".into());
    }
    for (index, cookie) in cookies.iter().enumerate() {
        let name = cookie
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("cookie_info.cookies[{index}].name must be a string"))?;
        if name.is_empty() {
            return Err(format!(
                "cookie_info.cookies[{index}].name must not be empty"
            ));
        }
        if cookie
            .get("value")
            .and_then(serde_json::Value::as_str)
            .is_none()
        {
            return Err(format!(
                "cookie_info.cookies[{index}].value must be a string"
            ));
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum UploadCredentialError {
    #[error("failed to open biliup credential {path}: {source}")]
    Open {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("credential {path} is not a biliup LoginInfo JSON file: {source}")]
    InvalidJson {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("credential {path} has invalid biliup LoginInfo data: {problem}")]
    InvalidShape { path: PathBuf, problem: String },
    #[error("failed to serialize refreshed biliup credential {path}: {source}")]
    Serialize {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("failed to persist refreshed biliup credential {path}: {source}")]
    Persist {
        path: PathBuf,
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn login_info(cookie_info: serde_json::Value) -> LoginInfo {
        serde_json::from_value(serde_json::json!({
            "cookie_info": cookie_info,
            "sso": [],
            "token_info": {
                "access_token": "token",
                "expires_in": 3600,
                "mid": 1,
                "refresh_token": "refresh"
            },
            "platform": null
        }))
        .unwrap()
    }

    #[test]
    fn malformed_cookie_info_is_rejected_before_biliup_sees_it() {
        let error = validate_login_info(&login_info(serde_json::json!({}))).unwrap_err();
        assert!(error.contains("cookie_info.cookies must be an array"));

        let error = validate_login_info(&login_info(serde_json::json!({
            "cookies": [{"name": "SESSDATA"}]
        })))
        .unwrap_err();
        assert!(error.contains("value must be a string"));
    }

    #[test]
    fn credential_file_revalidates_replacements() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let valid = login_info(serde_json::json!({
            "cookies": [{"name": "SESSDATA", "value": "cookie"}]
        }));
        std::fs::write(file.path(), serde_json::to_vec(&valid).unwrap()).unwrap();

        let mut opened = UploadCredentialFile::open(file.path()).unwrap();
        let invalid = login_info(serde_json::json!({}));
        assert!(opened.replace(invalid).is_err());

        let reopened = UploadCredentialFile::open(file.path()).unwrap();
        assert_eq!(reopened.login_info().token_info.mid, 1);
    }
}
