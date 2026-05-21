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

    #[error("database error: {0}")]
    Database(#[from] redb::DatabaseError),

    #[error("table error: {0}")]
    Table(#[from] redb::TableError),

    #[error("transaction error: {0}")]
    Transaction(Box<redb::TransactionError>),

    #[error("storage error: {0}")]
    Storage(#[from] redb::StorageError),

    #[error("commit error: {0}")]
    Commit(#[from] redb::CommitError),

    #[error("state store error: {0}")]
    State(String),

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("bilibili error: {0}")]
    Bilibili(String),
}

impl From<redb::TransactionError> for AppError {
    fn from(e: redb::TransactionError) -> Self {
        AppError::Transaction(Box::new(e))
    }
}
