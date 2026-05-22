use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum PipelineState {
    Idle,
    Resolving,
    Offline,
    Recording,
    ReResolving,
    WaitingReconnect,
    Uploading,
    Submitting,
    Submitted,
    Failed,
}

impl PipelineState {
    /// Validates if a state transition is allowed in the pipeline.
    pub fn can_transition_to(&self, next: PipelineState) -> bool {
        if *self == next {
            return true;
        }

        matches!(
            (self, next),
            (Self::Idle, Self::Resolving)
                | (Self::Resolving, Self::Offline)
                | (Self::Resolving, Self::Recording)
                | (Self::Resolving, Self::Failed)
                | (Self::Offline, Self::Idle)
                | (Self::Recording, Self::ReResolving)
                | (Self::Recording, Self::WaitingReconnect)
                | (Self::Recording, Self::Uploading)
                | (Self::Recording, Self::Failed)
                | (Self::ReResolving, Self::Recording)
                | (Self::ReResolving, Self::WaitingReconnect)
                | (Self::ReResolving, Self::Failed)
                | (Self::WaitingReconnect, Self::ReResolving)
                | (Self::WaitingReconnect, Self::Recording)
                | (Self::WaitingReconnect, Self::Uploading)
                | (Self::WaitingReconnect, Self::Failed)
                | (Self::Uploading, Self::Submitting)
                | (Self::Uploading, Self::Failed)
                | (Self::Submitting, Self::Submitted)
                | (Self::Submitting, Self::Failed)
                | (Self::Submitted, Self::Idle)
                | (Self::Failed, Self::Idle)
        )
    }
}
