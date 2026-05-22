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

impl PipelineState {
    /// Validates if a state transition is allowed in the pipeline.
    pub fn can_transition_to(&self, next: PipelineState) -> bool {
        // Self-transitions are allowed
        if *self == next {
            return true;
        }

        match (self, next) {
            // Normal flow starts from Idle
            (Self::Idle, Self::Resolving) => true,

            // Resolving checks if the room is live
            (Self::Resolving, Self::Recording) => true,
            (Self::Resolving, Self::Offline) => true,
            (Self::Resolving, Self::Failed) => true,

            // Offline can go back to Idle for the next polling interval
            (Self::Offline, Self::Idle) => true,

            // Recording can end naturally (Offline), encounter an issue (ReResolving),
            // or finish and proceed to Uploading
            (Self::Recording, Self::Offline) => true,
            (Self::Recording, Self::ReResolving) => true,
            (Self::Recording, Self::Uploading) => true,
            (Self::Recording, Self::Failed) => true,

            // ReResolving attempts to recover a dropped stream
            (Self::ReResolving, Self::Recording) => true,
            (Self::ReResolving, Self::Offline) => true,
            (Self::ReResolving, Self::Failed) => true,

            // Uploading completes or fails
            (Self::Uploading, Self::Submitting) => true,
            (Self::Uploading, Self::Failed) => true,

            // Submitting completes or fails
            (Self::Submitting, Self::Submitted) => true,
            (Self::Submitting, Self::Failed) => true,

            // Submitted can cycle back to Idle
            (Self::Submitted, Self::Idle) => true,

            // Failed can cycle back to Idle for retry
            (Self::Failed, Self::Idle) => true,

            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowed_transitions() {
        assert!(PipelineState::Idle.can_transition_to(PipelineState::Resolving));
        assert!(PipelineState::Resolving.can_transition_to(PipelineState::Recording));
        assert!(PipelineState::Recording.can_transition_to(PipelineState::Uploading));
        assert!(PipelineState::Uploading.can_transition_to(PipelineState::Submitting));
        assert!(PipelineState::Submitting.can_transition_to(PipelineState::Submitted));
        assert!(PipelineState::Submitted.can_transition_to(PipelineState::Idle));

        // Error handling paths
        assert!(PipelineState::Recording.can_transition_to(PipelineState::ReResolving));
        assert!(PipelineState::ReResolving.can_transition_to(PipelineState::Recording));

        // Failure paths
        assert!(PipelineState::Resolving.can_transition_to(PipelineState::Failed));
        assert!(PipelineState::Recording.can_transition_to(PipelineState::Failed));
        assert!(PipelineState::Uploading.can_transition_to(PipelineState::Failed));
        assert!(PipelineState::Submitting.can_transition_to(PipelineState::Failed));

        // Reset
        assert!(PipelineState::Failed.can_transition_to(PipelineState::Idle));
        assert!(PipelineState::Offline.can_transition_to(PipelineState::Idle));
    }

    #[test]
    fn disallowed_transitions() {
        assert!(!PipelineState::Idle.can_transition_to(PipelineState::Recording));
        assert!(!PipelineState::Idle.can_transition_to(PipelineState::Uploading));
        assert!(!PipelineState::Offline.can_transition_to(PipelineState::Recording));
        assert!(!PipelineState::Resolving.can_transition_to(PipelineState::Uploading));
        assert!(!PipelineState::Uploading.can_transition_to(PipelineState::Recording));
        assert!(!PipelineState::Submitted.can_transition_to(PipelineState::Uploading));
    }
}
