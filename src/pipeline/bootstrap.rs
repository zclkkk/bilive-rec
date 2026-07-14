use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::bilibili::client::BiliClient;
use crate::bilibili::room::{fetch_room_info, resolve_room_id};
use crate::config::ResolvedRoomConfig;
use crate::credential::CredentialRef;
use crate::error::{AppError, AppResult};

/// A room whose configured URL has already been resolved to its canonical
/// Bilibili identity. Durable startup recovery may already be active, but
/// `run` must prepare every current room before it writes current ownership or
/// spawns a supervisor, so duplicate aliases fail as one configuration error.
pub struct PreparedRoom {
    pub room_id: u64,
    pub room_config: ResolvedRoomConfig,
    pub client: Arc<BiliClient>,
}

#[derive(Debug, thiserror::Error)]
pub enum PrepareRoomsError {
    #[error("{context}: {source}")]
    Retryable {
        context: String,
        #[source]
        source: AppError,
    },
    #[error("{context}: {source}")]
    Fatal {
        context: String,
        #[source]
        source: AppError,
    },
}

impl PrepareRoomsError {
    fn fatal(context: impl Into<String>, source: AppError) -> Self {
        Self::Fatal {
            context: context.into(),
            source,
        }
    }

    fn from_lookup(
        context: impl Into<String>,
        error: crate::bilibili::room::RoomLookupError,
    ) -> Self {
        let context = context.into();
        match error {
            crate::bilibili::room::RoomLookupError::Retryable { source } => {
                Self::Retryable { context, source }
            }
            crate::bilibili::room::RoomLookupError::Fatal { source } => {
                Self::Fatal { context, source }
            }
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable { .. })
    }

    pub fn into_app_error(self) -> AppError {
        let (context, source) = match self {
            Self::Retryable { context, source } | Self::Fatal { context, source } => {
                (context, source)
            }
        };
        match source {
            AppError::Config(message) => AppError::Config(format!("{context}: {message}")),
            AppError::Bilibili(message) => AppError::Bilibili(format!("{context}: {message}")),
            AppError::BilibiliResponse(message) => {
                AppError::BilibiliResponse(format!("{context}: {message}"))
            }
            AppError::State(message) => AppError::State(format!("{context}: {message}")),
            other => other,
        }
    }
}

pub async fn prepare_rooms(
    mut rooms: Vec<ResolvedRoomConfig>,
) -> Result<Vec<PreparedRoom>, PrepareRoomsError> {
    // AppConfig stores rooms in a HashMap, so its iteration order is not a
    // stable bootstrap order. Sorting here makes both network preparation and
    // any reported duplicate pair deterministic.
    rooms.sort_by(|left, right| left.name.cmp(&right.name));

    let mut clients: HashMap<Option<CredentialRef>, Arc<BiliClient>> = HashMap::new();
    let mut resolved = Vec::with_capacity(rooms.len());

    for room_config in rooms {
        let credential = room_config.record.credential.clone();
        let client = if let Some(client) = clients.get(&credential) {
            client.clone()
        } else {
            let client = Arc::new(
                BiliClient::from_optional_cookie_file(
                    credential.as_ref().map(CredentialRef::cookie_file),
                )
                .map_err(|source| {
                    PrepareRoomsError::fatal(
                        format!("failed to build client for rooms.{}", room_config.name),
                        source,
                    )
                })?,
            );
            clients.insert(credential, client.clone());
            client
        };

        let configured_id = resolve_room_id(&client, &room_config.url)
            .await
            .map_err(|source| {
                PrepareRoomsError::from_lookup(
                    format!(
                        "failed to resolve rooms.{} URL {}",
                        room_config.name, room_config.url
                    ),
                    source,
                )
            })?;
        let room_info = fetch_room_info(&client, configured_id)
            .await
            .map_err(|source| {
                PrepareRoomsError::from_lookup(
                    format!(
                        "failed to fetch canonical identity for rooms.{} ({})",
                        room_config.name, room_config.url
                    ),
                    source,
                )
            })?;
        resolved.push(PreparedRoom {
            room_id: room_info.room_id,
            room_config,
            client,
        });
    }

    // Do this only after every configured room has resolved successfully. The
    // caller receives either a complete, unique registry or no prepared run at
    // all; no supervisor or durable state mutation belongs before this point.
    canonical_room_registry(
        resolved
            .iter()
            .map(|room| (room.room_id, room.room_config.name.as_str())),
    )
    .map_err(|source| PrepareRoomsError::fatal("current room registry is invalid", source))?;
    Ok(resolved)
}

/// Builds the immutable canonical ownership registry used to validate a run.
///
/// This function is deliberately pure: bootstrap can validate aliases without
/// starting tasks or touching durable state. It sorts by configured room name
/// internally so callers cannot accidentally make the selected duplicate pair
/// or its diagnostic depend on HashMap/network completion order.
pub fn canonical_room_registry<'a>(
    rooms: impl IntoIterator<Item = (u64, &'a str)>,
) -> AppResult<BTreeMap<u64, &'a str>> {
    let mut entries: Vec<_> = rooms.into_iter().collect();
    entries.sort_by(|(left_id, left_name), (right_id, right_name)| {
        left_name
            .cmp(right_name)
            .then_with(|| left_id.cmp(right_id))
    });

    let mut owners = BTreeMap::new();
    for (room_id, room_name) in entries {
        if let Some(previous) = owners.get(&room_id) {
            return Err(AppError::Config(format!(
                "rooms.{previous} and rooms.{room_name} resolve to the same canonical Bilibili room {room_id}"
            )));
        }
        owners.insert(room_id, room_name);
    }
    Ok(owners)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_registry_contains_each_unique_owner() {
        let registry =
            canonical_room_registry([(42, "beta"), (7, "alpha"), (99, "gamma")]).unwrap();

        assert_eq!(registry.len(), 3);
        assert_eq!(registry.get(&7), Some(&"alpha"));
        assert_eq!(registry.get(&42), Some(&"beta"));
        assert_eq!(registry.get(&99), Some(&"gamma"));
    }

    #[test]
    fn canonical_registry_rejects_aliases_deterministically() {
        let first =
            canonical_room_registry([(42, "beta"), (7, "other"), (42, "alpha"), (42, "gamma")])
                .unwrap_err()
                .to_string();
        let second =
            canonical_room_registry([(42, "gamma"), (42, "alpha"), (42, "beta"), (7, "other")])
                .unwrap_err()
                .to_string();

        assert_eq!(first, second);
        let message = first;
        assert!(message.contains("rooms.alpha"));
        assert!(message.contains("rooms.beta"));
        assert!(message.contains("canonical Bilibili room 42"));
        assert!(!message.contains("rooms.gamma"));
    }

    #[test]
    fn permanent_room_errors_are_fatal_and_preserve_config_category() {
        let error = PrepareRoomsError::from_lookup(
            "failed to resolve rooms.bad",
            crate::bilibili::room::RoomLookupError::Fatal {
                source: AppError::Config("not a room URL".into()),
            },
        );

        assert!(!error.is_retryable());
        let error = error.into_app_error();
        assert!(matches!(error, AppError::Config(_)));
        assert!(error.to_string().contains("rooms.bad"));
        assert!(error.to_string().contains("not a room URL"));
    }

    #[test]
    fn explicit_fatal_room_errors_are_not_retried() {
        let error = PrepareRoomsError::from_lookup(
            "failed to fetch canonical identity",
            crate::bilibili::room::RoomLookupError::Fatal {
                source: AppError::Bilibili("unsupported live status".into()),
            },
        );

        assert!(!error.is_retryable());
        assert!(matches!(error.into_app_error(), AppError::Bilibili(_)));
    }

    #[test]
    fn safe_response_decode_errors_remain_retryable() {
        let error = PrepareRoomsError::from_lookup(
            "failed to decode canonical identity",
            crate::bilibili::room::RoomLookupError::Retryable {
                source: AppError::BilibiliResponse("truncated JSON".into()),
            },
        );

        assert!(error.is_retryable());
        assert!(matches!(
            error.into_app_error(),
            AppError::BilibiliResponse(_)
        ));
    }
}
