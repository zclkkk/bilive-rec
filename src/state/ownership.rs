use std::collections::{BTreeMap, HashMap};

use uuid::Uuid;

use crate::error::{AppError, AppResult};
use crate::state::model::{LiveSession, RoomLifecycle, SessionLifecycle};
use crate::state::store::StateStore;
use crate::state::transitions;

/// Validate the complete durable Session/room ownership graph from one read
/// snapshot. Hard conflicts abort before any repair is written. Unique,
/// claimable mismatches are then normalized into explicit recovery state.
pub fn audit(store: &StateStore) -> AppResult<Vec<String>> {
    let snapshot = store.read_snapshot()?;
    let non_closed: Vec<_> = snapshot
        .sessions
        .into_iter()
        .filter(|session| !matches!(session.lifecycle, SessionLifecycle::Closed { .. }))
        .collect();
    let rooms: HashMap<_, _> = snapshot.room_states.into_iter().collect();

    detect_hard_conflicts(&non_closed, &rooms)?;

    let mut repairs = Vec::new();
    let mut ordered = non_closed;
    ordered.sort_by_key(|session| (session.room_id, session.id));
    for session in ordered {
        let exact = match (
            &session.lifecycle,
            rooms.get(&session.room_id).map(|room| &room.lifecycle),
        ) {
            (SessionLifecycle::Open, Some(RoomLifecycle::Owned { session_id })) => {
                *session_id == session.id
            }
            (
                SessionLifecycle::RecoveryRequired { .. },
                Some(RoomLifecycle::Blocked { session_id }),
            ) => *session_id == session.id,
            _ => false,
        };
        if exact {
            continue;
        }

        let reason = format!(
            "durable ownership mismatch for room {} and session {}",
            session.room_id, session.id
        );
        transitions::require_recovery(store, session.id, reason)?;
        repairs.push(format!(
            "normalized room {} and session {} to RecoveryRequired/Blocked",
            session.room_id, session.id
        ));
    }
    Ok(repairs)
}

fn detect_hard_conflicts(
    sessions: &[LiveSession],
    rooms: &HashMap<u64, crate::state::model::RoomState>,
) -> AppResult<()> {
    let mut by_room: BTreeMap<u64, Vec<Uuid>> = BTreeMap::new();
    let by_id: HashMap<_, _> = sessions
        .iter()
        .map(|session| (session.id, session))
        .collect();
    for session in sessions {
        by_room.entry(session.room_id).or_default().push(session.id);
    }
    for (room_id, ids) in &by_room {
        if ids.len() > 1 {
            return Err(AppError::State(format!(
                "room {room_id} has multiple non-closed sessions: {}",
                ids.iter()
                    .map(Uuid::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    }

    let mut ordered_rooms: Vec<_> = rooms.iter().collect();
    ordered_rooms.sort_by_key(|(room_id, _)| **room_id);
    for (room_id, room) in ordered_rooms {
        let Some(session_id) = room.lifecycle.session_id() else {
            continue;
        };
        let Some(session) = by_id.get(&session_id) else {
            return Err(AppError::State(format!(
                "room {room_id} points to missing or closed session {session_id}"
            )));
        };
        if session.room_id != *room_id {
            return Err(AppError::State(format!(
                "room {room_id} points to session {session_id}, but that session belongs to room {}",
                session.room_id
            )));
        }
    }

    for session in sessions {
        if let Some(room) = rooms.get(&session.room_id)
            && let Some(owner) = room.lifecycle.session_id()
            && owner != session.id
        {
            return Err(AppError::State(format!(
                "room {} points to session {owner}, not non-closed session {}",
                session.room_id, session.id
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::model::{
        LiveSession, OutputPlan, RecordingPlan, RoomLifecycle, RoomState, SessionLifecycle,
    };

    fn session(room_id: u64) -> LiveSession {
        LiveSession {
            id: Uuid::new_v4(),
            room_id,
            room_name: "room".into(),
            title: "live".into(),
            started_at: jiff::Timestamp::now(),
            lifecycle: SessionLifecycle::Open,
            recording_plan: RecordingPlan {
                credential: None,
                output_dir: std::env::temp_dir().join("bilive-rec-test-recordings"),
                segment_time_ms: None,
                segment_size: None,
                min_segment_size: 0,
                qn: 10_000,
                cdn: Vec::new(),
            },
            output_plan: OutputPlan::LocalOnly,
            recording_events: Vec::new(),
        }
    }

    #[test]
    fn claimable_open_mismatch_becomes_explicit_recovery() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = StateStore::create_or_open(dir.path().join("state.redb")).unwrap();
        let session = session(1);
        transitions::create_session(&store, &session).unwrap();
        store
            .write(|txn| {
                txn.put_room_state(
                    1,
                    &RoomState {
                        lifecycle: RoomLifecycle::Ready,
                        changed_at: jiff::Timestamp::now(),
                        message: None,
                    },
                )
            })
            .unwrap();

        let repairs = audit(&store).unwrap();
        assert_eq!(repairs.len(), 1);
        assert!(matches!(
            store.get_session(session.id).unwrap().unwrap().lifecycle,
            SessionLifecycle::RecoveryRequired { .. }
        ));
        assert!(matches!(
            store.get_room_state(1).unwrap().unwrap().lifecycle,
            RoomLifecycle::Blocked { session_id } if session_id == session.id
        ));
    }

    #[test]
    fn hard_conflict_is_detected_before_any_normalization_write() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = StateStore::create_or_open(dir.path().join("state.redb")).unwrap();
        let first = session(1);
        let second = session(1);
        transitions::create_session(&store, &first).unwrap();
        store.write(|txn| txn.put_session(&second)).unwrap();

        let error = audit(&store).unwrap_err();
        assert!(error.to_string().contains("multiple non-closed sessions"));
        assert_eq!(
            store.get_session(first.id).unwrap().unwrap().lifecycle,
            SessionLifecycle::Open
        );
        assert_eq!(
            store.get_session(second.id).unwrap().unwrap().lifecycle,
            SessionLifecycle::Open
        );
    }
}
