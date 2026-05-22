use std::sync::Arc;
use tracing::info;

use crate::config::PipelineConfig;
use crate::error::AppResult;
use crate::pipeline::session::PipelineSession;
use crate::pipeline::state_machine::PipelineState;
use crate::state::store::StateStore;

pub struct RoomSupervisor {
    pub room_id: u64,
    pub session: PipelineSession,
    pub config: PipelineConfig,
    pub store: Option<Arc<StateStore>>,
}

impl RoomSupervisor {
    pub fn new(room_id: u64, config: PipelineConfig, store: Option<Arc<StateStore>>) -> Self {
        Self {
            room_id,
            session: PipelineSession::new(room_id),
            config,
            store,
        }
    }

    /// Perform a single state transition, updating internal state and persisting it.
    pub fn transition(&mut self, next: PipelineState) -> AppResult<()> {
        let prev = self.session.state;
        self.session.transition_to(next)?;

        info!(room_id = self.room_id, from = ?prev, to = ?next, "Pipeline state transition");

        if let Some(store) = &self.store {
            store.put_pipeline_state(self.room_id, next)?;
        }

        Ok(())
    }

    /// Stub to represent the main state machine loop for a single room.
    pub async fn step(&mut self) -> AppResult<()> {
        // Just a stub for tests
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_supervisor() -> RoomSupervisor {
        RoomSupervisor::new(1, PipelineConfig::default(), None)
    }

    #[test]
    fn test_supervisor_skeleton_offline() {
        let mut supervisor = mock_supervisor();

        assert_eq!(supervisor.session.state, PipelineState::Idle);
        supervisor.transition(PipelineState::Resolving).unwrap();
        assert_eq!(supervisor.session.state, PipelineState::Resolving);

        // Room is offline
        supervisor.transition(PipelineState::Offline).unwrap();

        // Go back to idle
        supervisor.transition(PipelineState::Idle).unwrap();
        assert_eq!(supervisor.session.state, PipelineState::Idle);
    }

    #[test]
    fn test_supervisor_skeleton_live_flow() {
        let mut supervisor = mock_supervisor();

        supervisor.transition(PipelineState::Resolving).unwrap();
        // Stream live!
        supervisor.transition(PipelineState::Recording).unwrap();

        assert_eq!(supervisor.session.state, PipelineState::Recording);
    }

    #[test]
    fn test_supervisor_skeleton_temp_disconnect() {
        let mut supervisor = mock_supervisor();

        supervisor.transition(PipelineState::Resolving).unwrap();
        supervisor.transition(PipelineState::Recording).unwrap();

        // Stream drops mid-session
        supervisor
            .transition(PipelineState::WaitingReconnect)
            .unwrap();
        assert_eq!(supervisor.session.state, PipelineState::WaitingReconnect);

        // We wait... then resolve again
        supervisor.transition(PipelineState::ReResolving).unwrap();

        // It comes back
        supervisor.transition(PipelineState::Recording).unwrap();
        assert_eq!(supervisor.session.state, PipelineState::Recording);
    }

    #[test]
    fn test_supervisor_skeleton_invalid_transition() {
        let mut supervisor = mock_supervisor();

        let res = supervisor.transition(PipelineState::Recording);
        assert!(res.is_err());
        assert_eq!(supervisor.session.state, PipelineState::Idle); // unchanged
    }

    #[test]
    fn test_supervisor_skeleton_persists_state() {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        let store = Arc::new(StateStore::open(dir.path().join("test.redb")).unwrap());

        let mut supervisor =
            RoomSupervisor::new(100, PipelineConfig::default(), Some(store.clone()));

        supervisor.transition(PipelineState::Resolving).unwrap();

        // Verify it was written to DB
        let persisted = store.get_pipeline_state(100).unwrap();
        assert_eq!(persisted, Some(PipelineState::Resolving));

        supervisor.transition(PipelineState::Offline).unwrap();
        let persisted2 = store.get_pipeline_state(100).unwrap();
        assert_eq!(persisted2, Some(PipelineState::Offline));
    }
}
