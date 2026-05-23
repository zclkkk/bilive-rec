use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CredentialIdentity {
    pub name: String,
    pub cookie_file: PathBuf,
}

impl CredentialIdentity {
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
