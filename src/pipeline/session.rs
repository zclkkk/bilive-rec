use super::state_machine::PipelineState;

#[derive(Debug, Clone)]
pub struct PipelineSession {
    pub room_id: u64,
    pub state: PipelineState,
}

impl PipelineSession {
    pub fn new(room_id: u64) -> Self {
        Self {
            room_id,
            state: PipelineState::Idle,
        }
    }

    /// Attempts to transition the pipeline state.
    /// Returns true if the transition was allowed and applied, false otherwise.
    pub fn transition_to(&mut self, next: PipelineState) -> bool {
        if self.state.can_transition_to(next) {
            self.state = next;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_session_transition() {
        let mut session = PipelineSession::new(123);
        assert_eq!(session.state, PipelineState::Idle);

        // Allowed transition
        assert!(session.transition_to(PipelineState::Resolving));
        assert_eq!(session.state, PipelineState::Resolving);

        // Disallowed transition
        assert!(!session.transition_to(PipelineState::Uploading));
        assert_eq!(session.state, PipelineState::Resolving); // Unchanged
    }
}
