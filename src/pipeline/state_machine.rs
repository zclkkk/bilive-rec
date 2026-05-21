use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum PipelineState {
    Idle,
    Resolving,
    Offline,
    Recording,
    ReResolving,
    Uploading,
    Submitting,
    Submitted,
    Failed,
}
