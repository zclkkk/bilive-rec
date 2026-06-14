//! Configuration boundary.
//!
//! Raw TOML structs live only at the edge. Runtime code should use resolved
//! config types so inheritance, credentials, defaults, and validation are paid
//! once before the run loop starts.

mod defaults;
mod raw;
mod resolved;
mod validation;

pub use raw::{
    AppConfig, Copyright, CredentialConfig, DataConfig, PipelineConfig, RecordConfig, RoomConfig,
    RoomRecordConfig, RoomSubmitConfig, RoomUploadConfig, SubmitApi, SubmitConfig, UploadConfig,
};
pub use resolved::{
    CheckConfig, ResolvedRecordConfig, ResolvedRoomConfig, ResolvedRoomUploadConfig,
    ResolvedSubmitConfig, ResolvedUploadConfig, RoomCredentials, RunConfig, UploadCommandConfig,
    UploadRecoveryConfig, UploadTransportConfig,
};
pub use validation::{ConfigValueError, parse_hms_duration, parse_size_bytes};
