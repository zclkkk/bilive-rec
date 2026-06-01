use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum RoomState {
    Idle,
    Resolving,
    Offline,
    Recording,
    ReResolving,
    WaitingReconnect,
    Failed,
}

impl RoomState {
    pub fn requires_active_session(self) -> bool {
        matches!(
            self,
            Self::Recording | Self::ReResolving | Self::WaitingReconnect
        )
    }

    /// Validates if a room lifecycle transition is allowed.
    pub fn can_transition_to(&self, next: RoomState) -> bool {
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
                | (Self::Recording, Self::Failed)
                | (Self::ReResolving, Self::Recording)
                | (Self::ReResolving, Self::WaitingReconnect)
                | (Self::ReResolving, Self::Failed)
                | (Self::WaitingReconnect, Self::ReResolving)
                | (Self::WaitingReconnect, Self::Recording)
                | (Self::WaitingReconnect, Self::Idle)
                | (Self::WaitingReconnect, Self::Failed)
                | (Self::Failed, Self::Idle)
        )
    }
}
