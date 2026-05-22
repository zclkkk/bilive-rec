use super::state_machine::PipelineState;
use crate::error::{AppError, AppResult};

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
    /// Returns Ok(()) if the transition was allowed and applied.
    /// Returns AppError::State if the transition is disallowed, leaving state unchanged.
    pub fn transition_to(&mut self, next: PipelineState) -> AppResult<()> {
        if self.state.can_transition_to(next) {
            self.state = next;
            Ok(())
        } else {
            Err(AppError::State(format!(
                "Invalid pipeline state transition from {:?} to {:?}",
                self.state, next
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normal_full_flow() {
        let mut session = PipelineSession::new(1);
        session.transition_to(PipelineState::Resolving).unwrap();
        session.transition_to(PipelineState::Recording).unwrap();
        session.transition_to(PipelineState::Uploading).unwrap();
        session.transition_to(PipelineState::Submitting).unwrap();
        session.transition_to(PipelineState::Submitted).unwrap();
        session.transition_to(PipelineState::Idle).unwrap();
    }

    #[test]
    fn test_pre_live_offline_polling() {
        let mut session = PipelineSession::new(1);
        session.transition_to(PipelineState::Resolving).unwrap();
        session.transition_to(PipelineState::Offline).unwrap();
        session.transition_to(PipelineState::Idle).unwrap();
    }

    #[test]
    fn test_cdn_lease_recovery() {
        let mut session = PipelineSession::new(1);
        session.transition_to(PipelineState::Resolving).unwrap();
        session.transition_to(PipelineState::Recording).unwrap();
        // Drop stream
        session.transition_to(PipelineState::ReResolving).unwrap();
        // Recovered
        session.transition_to(PipelineState::Recording).unwrap();
    }

    #[test]
    fn test_temporary_streamer_disconnect() {
        let mut session = PipelineSession::new(1);
        session.transition_to(PipelineState::Resolving).unwrap();
        session.transition_to(PipelineState::Recording).unwrap();
        // Stream drops
        session
            .transition_to(PipelineState::WaitingReconnect)
            .unwrap();
        // Try resolving
        session.transition_to(PipelineState::ReResolving).unwrap();
        // Stream comes back
        session.transition_to(PipelineState::Recording).unwrap();
    }

    #[test]
    fn test_confirmed_session_end_after_grace() {
        let mut session = PipelineSession::new(1);
        session.transition_to(PipelineState::Resolving).unwrap();
        session.transition_to(PipelineState::Recording).unwrap();
        // Stream drops
        session
            .transition_to(PipelineState::WaitingReconnect)
            .unwrap();
        // Grace expires
        session.transition_to(PipelineState::Uploading).unwrap();
    }

    #[test]
    fn test_disallowed_transitions() {
        let mut session = PipelineSession::new(1);

        // Idle -> Recording is disallowed
        let err = session.transition_to(PipelineState::Recording).unwrap_err();
        match err {
            AppError::State(msg) => assert!(msg.contains("Idle to Recording")),
            _ => panic!("Expected AppError::State"),
        }
        assert_eq!(session.state, PipelineState::Idle);

        // Move to Offline
        session.transition_to(PipelineState::Resolving).unwrap();
        session.transition_to(PipelineState::Offline).unwrap();

        // Offline -> Recording is disallowed
        let err = session.transition_to(PipelineState::Recording).unwrap_err();
        match err {
            AppError::State(msg) => assert!(msg.contains("Offline to Recording")),
            _ => panic!("Expected AppError::State"),
        }
        assert_eq!(session.state, PipelineState::Offline);
    }

    #[test]
    fn test_self_transition() {
        let mut session = PipelineSession::new(1);
        assert!(session.transition_to(PipelineState::Idle).is_ok());
        assert_eq!(session.state, PipelineState::Idle);
    }
}
