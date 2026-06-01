use super::state_machine::RoomState;
use crate::error::{AppError, AppResult};

#[derive(Debug, Clone)]
pub struct RoomStateMachine {
    pub room_id: u64,
    pub state: RoomState,
}

impl RoomStateMachine {
    pub fn new(room_id: u64) -> Self {
        Self {
            room_id,
            state: RoomState::Idle,
        }
    }

    /// Attempts to transition the room lifecycle state.
    /// Returns Ok(()) if the transition was allowed and applied.
    /// Returns AppError::State if the transition is disallowed, leaving state unchanged.
    pub fn transition_to(&mut self, next: RoomState) -> AppResult<()> {
        if self.state.can_transition_to(next) {
            self.state = next;
            Ok(())
        } else {
            Err(AppError::State(format!(
                "Invalid room state transition from {:?} to {:?}",
                self.state, next
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_room_lifecycle_returns_to_listening_after_session_end() {
        let mut session = RoomStateMachine::new(1);
        session.transition_to(RoomState::Resolving).unwrap();
        session.transition_to(RoomState::Recording).unwrap();
        session.transition_to(RoomState::WaitingReconnect).unwrap();
        session.transition_to(RoomState::Idle).unwrap();
        session.transition_to(RoomState::Resolving).unwrap();
        session.transition_to(RoomState::Offline).unwrap();
        session.transition_to(RoomState::Idle).unwrap();
    }

    #[test]
    fn test_pre_live_offline_polling() {
        let mut session = RoomStateMachine::new(1);
        session.transition_to(RoomState::Resolving).unwrap();
        session.transition_to(RoomState::Offline).unwrap();
        session.transition_to(RoomState::Idle).unwrap();
    }

    #[test]
    fn test_stream_re_resolve_returns_to_recording() {
        let mut session = RoomStateMachine::new(1);
        session.transition_to(RoomState::Resolving).unwrap();
        session.transition_to(RoomState::Recording).unwrap();
        // Drop stream
        session.transition_to(RoomState::ReResolving).unwrap();
        // Recovered
        session.transition_to(RoomState::Recording).unwrap();
    }

    #[test]
    fn test_temporary_streamer_disconnect() {
        let mut session = RoomStateMachine::new(1);
        session.transition_to(RoomState::Resolving).unwrap();
        session.transition_to(RoomState::Recording).unwrap();
        // Stream drops
        session.transition_to(RoomState::WaitingReconnect).unwrap();
        // Try resolving
        session.transition_to(RoomState::ReResolving).unwrap();
        // Stream comes back
        session.transition_to(RoomState::Recording).unwrap();
    }

    #[test]
    fn test_confirmed_session_end_after_grace_returns_to_idle() {
        let mut session = RoomStateMachine::new(1);
        session.transition_to(RoomState::Resolving).unwrap();
        session.transition_to(RoomState::Recording).unwrap();
        // Stream drops
        session.transition_to(RoomState::WaitingReconnect).unwrap();
        // Grace expires
        session.transition_to(RoomState::Idle).unwrap();
    }

    #[test]
    fn test_disallowed_transitions() {
        let mut session = RoomStateMachine::new(1);

        // Idle -> Recording is disallowed
        let err = session.transition_to(RoomState::Recording).unwrap_err();
        match err {
            AppError::State(msg) => assert!(msg.contains("Idle to Recording")),
            _ => panic!("Expected AppError::State"),
        }
        assert_eq!(session.state, RoomState::Idle);

        // Move to Offline
        session.transition_to(RoomState::Resolving).unwrap();
        session.transition_to(RoomState::Offline).unwrap();

        // Offline -> Recording is disallowed
        let err = session.transition_to(RoomState::Recording).unwrap_err();
        match err {
            AppError::State(msg) => assert!(msg.contains("Offline to Recording")),
            _ => panic!("Expected AppError::State"),
        }
        assert_eq!(session.state, RoomState::Offline);
    }

    #[test]
    fn test_self_transition() {
        let mut session = RoomStateMachine::new(1);
        assert!(session.transition_to(RoomState::Idle).is_ok());
        assert_eq!(session.state, RoomState::Idle);
    }
}
