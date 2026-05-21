use std::path::PathBuf;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("config error: {0}")]
    Config(String),

    #[error("state store error: {0}")]
    State(String),

    #[error("not implemented: {0}")]
    NotImplemented(String),
}
