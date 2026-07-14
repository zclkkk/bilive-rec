use std::path::Path;

use uuid::Uuid;

use crate::error::{AppError, AppResult};
use crate::state::model::{ArtifactResolutionDecision, ArtifactState, Segment, SegmentCloseReason};
use crate::state::store::StateStore;
use crate::state::transitions;

/// Commit an already flushed and synced `.part` file as the final recording.
/// The durable Finalizing intent is deliberately retained if rename or parent
/// directory sync fails; startup recovery can then inspect the file matrix.
pub async fn finalize(
    store: &StateStore,
    session_id: Uuid,
    segment_index: u32,
    close_reason: SegmentCloseReason,
) -> AppResult<()> {
    let segment =
        transitions::begin_artifact_finalization(store, session_id, segment_index, close_reason)?;
    let commit: AppResult<()> = async {
        require_regular_file(&segment.part_path)?;
        rename_noreplace(&segment.part_path, &segment.final_path).await?;
        sync_parent(&segment.final_path).await
    }
    .await;
    if let Err(error) = commit {
        return recording_commit_failed(store, session_id, error);
    }
    transitions::complete_artifact_finalization(store, session_id, segment_index)?;
    Ok(())
}

/// Commit a size-filter decision. The Discarding intent remains durable on a
/// deletion failure and is replayed by the same recovery matrix at startup.
pub async fn discard(
    store: &StateStore,
    session_id: Uuid,
    segment_index: u32,
    close_reason: SegmentCloseReason,
) -> AppResult<()> {
    let segment =
        transitions::begin_artifact_discard(store, session_id, segment_index, close_reason)?;
    let commit: AppResult<()> = async {
        require_missing_path(&segment.final_path)?;
        remove_regular_file_if_exists(&segment.part_path).await?;
        sync_parent(&segment.part_path).await
    }
    .await;
    if let Err(error) = commit {
        return recording_commit_failed(store, session_id, error);
    }
    transitions::complete_artifact_discard(store, session_id, segment_index)?;
    Ok(())
}

/// Delete a submitted recording with a durable Deleting intent. A crash before
/// the final transition is deterministic: recovery repeats the idempotent file
/// removal and records Deleted.
pub async fn delete(store: &StateStore, session_id: Uuid, segment_index: u32) -> AppResult<()> {
    let segment = transitions::begin_artifact_delete(store, session_id, segment_index)?;
    remove_regular_file_if_exists(&segment.final_path).await?;
    sync_parent(&segment.final_path).await?;
    transitions::complete_artifact_delete(store, session_id, segment_index)?;
    Ok(())
}

/// Resolve the only local file matrix that has no deterministic winner. The
/// operator's decision is persisted as ResolvingConflict before either file is
/// removed, so a crash replays the chosen action instead of asking again.
pub async fn resolve_conflict(
    store: &StateStore,
    session_id: Uuid,
    segment_index: u32,
    decision: ArtifactResolutionDecision,
    note: Option<String>,
) -> AppResult<Segment> {
    let current = store
        .get_segment(session_id, segment_index)?
        .ok_or_else(|| {
            AppError::State(format!("segment {session_id}/{segment_index} not found"))
        })?;
    let part = inspect_path(&current.part_path);
    let final_file = inspect_path(&current.final_path);
    let valid = match current.artifact {
        ArtifactState::Finalizing { .. } => {
            matches!(&part, PathKind::RegularFile) && matches!(&final_file, PathKind::RegularFile)
        }
        ArtifactState::Discarding { .. } => {
            matches!(&part, PathKind::RegularFile | PathKind::Missing)
                && matches!(&final_file, PathKind::RegularFile)
                && !(decision == ArtifactResolutionDecision::KeepPart
                    && matches!(&part, PathKind::Missing))
        }
        _ => false,
    };
    if !valid {
        return Err(AppError::State(format!(
            "segment {session_id}/{segment_index} is not a resolvable Finalizing/Discarding file conflict for decision {decision:?} (part={part:?}, final={final_file:?})"
        )));
    }
    let segment = transitions::begin_artifact_conflict_resolution(
        store,
        session_id,
        segment_index,
        decision,
        note,
    )?;
    replay_conflict(store, segment).await
}

pub async fn reconcile(store: &StateStore) -> AppResult<Vec<String>> {
    let mut messages = Vec::new();
    let segments = store.list_all_segments()?;

    for segment in segments {
        match &segment.artifact {
            ArtifactState::Writing => {
                transitions::fail_artifact(
                    store,
                    segment.session_id,
                    segment.index,
                    "recording process ended before segment close".into(),
                    None,
                )?;
                messages.push(format!(
                    "marked interrupted segment {}/{} Failed",
                    segment.session_id, segment.index
                ));
            }
            ArtifactState::Finalizing { close_reason } => {
                let part = inspect_path(&segment.part_path);
                let final_file = inspect_path(&segment.final_path);
                match (&part, &final_file) {
                    (PathKind::RegularFile, PathKind::Missing) => {
                        let replay: AppResult<()> = async {
                            rename_noreplace(&segment.part_path, &segment.final_path).await?;
                            sync_parent(&segment.final_path).await
                        }
                        .await;
                        match replay {
                            Ok(()) => {
                                transitions::complete_artifact_finalization(
                                    store,
                                    segment.session_id,
                                    segment.index,
                                )?;
                                messages.push(format!(
                                    "completed finalization for segment {}/{}",
                                    segment.session_id, segment.index
                                ));
                            }
                            Err(error) => {
                                persist_recovery_reason(store, &segment, &error.to_string())?;
                                messages.push(format!(
                                    "left segment {}/{} Finalizing after recovery error: {error}",
                                    segment.session_id, segment.index
                                ));
                            }
                        }
                    }
                    (PathKind::Missing, PathKind::RegularFile) => {
                        match sync_parent(&segment.final_path).await {
                            Ok(()) => {
                                transitions::complete_artifact_finalization(
                                    store,
                                    segment.session_id,
                                    segment.index,
                                )?;
                                messages.push(format!(
                                    "confirmed finalization for segment {}/{}",
                                    segment.session_id, segment.index
                                ));
                            }
                            Err(error) => {
                                persist_recovery_reason(store, &segment, &error.to_string())?;
                                messages.push(format!(
                                    "left segment {}/{} Finalizing after directory sync error: {error}",
                                    segment.session_id, segment.index
                                ));
                            }
                        }
                    }
                    (PathKind::Missing, PathKind::Missing) => {
                        transitions::fail_artifact(
                            store,
                            segment.session_id,
                            segment.index,
                            "finalization intent has neither part nor final file".into(),
                            Some(close_reason.clone()),
                        )?;
                        messages.push(format!(
                            "marked missing finalization segment {}/{} Failed",
                            segment.session_id, segment.index
                        ));
                    }
                    (PathKind::RegularFile, PathKind::RegularFile) => {
                        persist_recovery_reason(
                            store,
                            &segment,
                            "both part and final files exist during finalization",
                        )?;
                        messages.push(format!(
                            "left segment {}/{} Finalizing because both part and final files exist",
                            segment.session_id, segment.index
                        ));
                    }
                    _ => {
                        persist_recovery_reason(
                            store,
                            &segment,
                            &format!(
                                "finalization paths are not a recoverable regular-file matrix: part={part:?}, final={final_file:?}"
                            ),
                        )?;
                        messages.push(format!(
                            "left segment {}/{} Finalizing because a path is not a regular file",
                            segment.session_id, segment.index
                        ));
                    }
                }
            }
            ArtifactState::Discarding { .. } => {
                let part = inspect_path(&segment.part_path);
                let final_file = inspect_path(&segment.final_path);
                if !matches!(final_file, PathKind::Missing) {
                    persist_recovery_reason(
                        store,
                        &segment,
                        &format!("discard intent conflicts with final path state {final_file:?}"),
                    )?;
                    messages.push(format!(
                        "left segment {}/{} Discarding because the final path is not missing",
                        segment.session_id, segment.index
                    ));
                    continue;
                }
                if !matches!(part, PathKind::RegularFile | PathKind::Missing) {
                    persist_recovery_reason(
                        store,
                        &segment,
                        &format!("discard part path is not removable: {part:?}"),
                    )?;
                    messages.push(format!(
                        "left segment {}/{} Discarding because the part path is not a regular file",
                        segment.session_id, segment.index
                    ));
                    continue;
                }
                let replay: AppResult<()> = async {
                    remove_regular_file_if_exists(&segment.part_path).await?;
                    sync_parent(&segment.part_path).await
                }
                .await;
                match replay {
                    Ok(()) => {
                        transitions::complete_artifact_discard(
                            store,
                            segment.session_id,
                            segment.index,
                        )?;
                        messages.push(format!(
                            "completed discard for segment {}/{}",
                            segment.session_id, segment.index
                        ));
                    }
                    Err(error) => {
                        persist_recovery_reason(store, &segment, &error.to_string())?;
                        messages.push(format!(
                            "left segment {}/{} Discarding after recovery error: {error}",
                            segment.session_id, segment.index
                        ));
                    }
                }
            }
            ArtifactState::ResolvingConflict { .. } => {
                match replay_conflict(store, segment.clone()).await {
                    Ok(resolved) => messages.push(format!(
                        "completed {:?} resolution for segment {}/{}",
                        resolved.artifact, resolved.session_id, resolved.index
                    )),
                    Err(error) => {
                        persist_recovery_reason(store, &segment, &error.to_string())?;
                        messages.push(format!(
                            "left segment {}/{} ResolvingConflict after recovery error: {error}",
                            segment.session_id, segment.index
                        ));
                    }
                }
            }
            ArtifactState::Deleting => {
                if !matches!(
                    inspect_path(&segment.final_path),
                    PathKind::RegularFile | PathKind::Missing
                ) {
                    messages.push(format!(
                        "left segment {}/{} Deleting because the final path is not a regular file",
                        segment.session_id, segment.index
                    ));
                    continue;
                }
                let replay: AppResult<()> = async {
                    remove_regular_file_if_exists(&segment.final_path).await?;
                    sync_parent(&segment.final_path).await
                }
                .await;
                match replay {
                    Ok(()) => {
                        transitions::complete_artifact_delete(
                            store,
                            segment.session_id,
                            segment.index,
                        )?;
                        messages.push(format!(
                            "completed deletion for segment {}/{}",
                            segment.session_id, segment.index
                        ));
                    }
                    Err(error) => messages.push(format!(
                        "left segment {}/{} Deleting after recovery error: {error}",
                        segment.session_id, segment.index
                    )),
                }
            }
            ArtifactState::Ready { .. }
            | ArtifactState::Filtered { .. }
            | ArtifactState::Failed { .. }
            | ArtifactState::Excluded { .. }
            | ArtifactState::Deleted => {}
        }
    }

    Ok(messages)
}

async fn replay_conflict(store: &StateStore, segment: Segment) -> AppResult<Segment> {
    let ArtifactState::ResolvingConflict { decision, .. } = segment.artifact else {
        return Err(AppError::State(format!(
            "segment {}/{} is not ResolvingConflict",
            segment.session_id, segment.index
        )));
    };
    match decision {
        ArtifactResolutionDecision::KeepPart => {
            match (
                inspect_path(&segment.part_path),
                inspect_path(&segment.final_path),
            ) {
                (PathKind::RegularFile, PathKind::RegularFile | PathKind::Missing) => {
                    remove_regular_file_if_exists(&segment.final_path).await?;
                    sync_parent(&segment.final_path).await?;
                    rename_noreplace(&segment.part_path, &segment.final_path).await?;
                    sync_parent(&segment.final_path).await?;
                }
                (PathKind::Missing, PathKind::RegularFile) => {
                    // A previous replay completed the rename but crashed before
                    // the directory sync or durable Ready transition. Repeat
                    // the sync before claiming that the rename is durable.
                    sync_parent(&segment.final_path).await?;
                }
                (PathKind::Missing, PathKind::Missing) => {
                    return transitions::fail_artifact(
                        store,
                        segment.session_id,
                        segment.index,
                        "KeepPart conflict resolution has neither part nor final file".into(),
                        segment.close_reason().cloned(),
                    );
                }
                (part, final_file) => {
                    return Err(invalid_path_matrix_error(
                        &segment, "KeepPart", part, final_file,
                    ));
                }
            }
            transitions::complete_artifact_conflict_resolution(
                store,
                segment.session_id,
                segment.index,
            )
        }
        ArtifactResolutionDecision::KeepFinal => {
            match (
                inspect_path(&segment.part_path),
                inspect_path(&segment.final_path),
            ) {
                (PathKind::RegularFile | PathKind::Missing, PathKind::RegularFile) => {
                    remove_regular_file_if_exists(&segment.part_path).await?;
                    sync_parent(&segment.part_path).await?;
                }
                (PathKind::Missing, PathKind::Missing) => {
                    return transitions::fail_artifact(
                        store,
                        segment.session_id,
                        segment.index,
                        "KeepFinal conflict resolution has no final file".into(),
                        segment.close_reason().cloned(),
                    );
                }
                (part, final_file) => {
                    return Err(invalid_path_matrix_error(
                        &segment,
                        "KeepFinal",
                        part,
                        final_file,
                    ));
                }
            }
            transitions::complete_artifact_conflict_resolution(
                store,
                segment.session_id,
                segment.index,
            )
        }
        ArtifactResolutionDecision::Exclude => {
            transitions::exclude_artifact_conflict(store, segment.session_id, segment.index)
        }
    }
}

#[derive(Debug)]
enum PathKind {
    Missing,
    RegularFile,
    Other,
    Inaccessible(String),
}

fn inspect_path(path: &Path) -> PathKind {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => PathKind::RegularFile,
        Ok(_) => PathKind::Other,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => PathKind::Missing,
        Err(error) => PathKind::Inaccessible(error.to_string()),
    }
}

fn require_regular_file(path: &Path) -> AppResult<()> {
    match inspect_path(path) {
        PathKind::RegularFile => Ok(()),
        kind => Err(path_kind_error(path, kind)),
    }
}

fn require_missing_path(path: &Path) -> AppResult<()> {
    match inspect_path(path) {
        PathKind::Missing => Ok(()),
        PathKind::RegularFile => Err(AppError::Io {
            path: path.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "refusing to discard while a final recording already exists",
            ),
        }),
        kind => Err(path_kind_error(path, kind)),
    }
}

async fn remove_regular_file_if_exists(path: &Path) -> AppResult<()> {
    match inspect_path(path) {
        PathKind::Missing => Ok(()),
        PathKind::RegularFile => {
            tokio::fs::remove_file(path)
                .await
                .map_err(|source| AppError::Io {
                    path: path.to_path_buf(),
                    source,
                })
        }
        kind => Err(path_kind_error(path, kind)),
    }
}

fn path_kind_error(path: &Path, kind: PathKind) -> AppError {
    let detail = match kind {
        PathKind::Missing => "path is missing".into(),
        PathKind::RegularFile => "path unexpectedly changed after inspection".into(),
        PathKind::Other => "path exists but is not a regular file".into(),
        PathKind::Inaccessible(reason) => format!("path is inaccessible: {reason}"),
    };
    AppError::Io {
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, detail),
    }
}

fn invalid_path_matrix_error(
    segment: &Segment,
    operation: &str,
    part: PathKind,
    final_file: PathKind,
) -> AppError {
    AppError::State(format!(
        "{operation} cannot continue for segment {}/{}: part={part:?}, final={final_file:?}",
        segment.session_id, segment.index
    ))
}

async fn rename_noreplace(from: &Path, to: &Path) -> AppResult<()> {
    let from = from.to_path_buf();
    let to = to.to_path_buf();
    let error_path = to.clone();
    tokio::task::spawn_blocking(move || rename_noreplace_blocking(&from, &to))
        .await
        .map_err(|error| AppError::State(format!("artifact rename task failed: {error}")))?
        .map_err(|source| AppError::Io {
            path: error_path,
            source,
        })
}

#[cfg(any(target_os = "android", target_vendor = "apple", target_os = "linux"))]
fn rename_noreplace_blocking(from: &Path, to: &Path) -> std::io::Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        from,
        rustix::fs::CWD,
        to,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(Into::into)
}

#[cfg(windows)]
fn rename_noreplace_blocking(from: &Path, to: &Path) -> std::io::Result<()> {
    // MoveFileEx without REPLACE_EXISTING, which backs std::fs::rename on
    // Windows, refuses an existing destination.
    std::fs::rename(from, to)
}

#[cfg(not(any(
    target_os = "android",
    target_vendor = "apple",
    target_os = "linux",
    windows
)))]
fn rename_noreplace_blocking(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::hard_link(from, to)?;
    std::fs::remove_file(from)
}

fn recording_commit_failed<T>(
    store: &StateStore,
    session_id: Uuid,
    error: AppError,
) -> AppResult<T> {
    let reason = error.to_string();
    transitions::require_recovery(store, session_id, reason.clone()).map_err(|persist_error| {
        AppError::State(format!(
            "{reason}; additionally failed to persist recovery-required state: {persist_error}"
        ))
    })?;
    Err(error)
}

fn persist_recovery_reason(store: &StateStore, segment: &Segment, detail: &str) -> AppResult<()> {
    transitions::require_recovery(
        store,
        segment.session_id,
        format!(
            "artifact recovery for segment {}/{} stopped: {detail}",
            segment.session_id, segment.index
        ),
    )?;
    Ok(())
}

async fn sync_parent(path: &Path) -> AppResult<()> {
    let parent = path.parent().ok_or_else(|| {
        AppError::State(format!("artifact path has no parent: {}", path.display()))
    })?;
    let parent = parent.to_path_buf();
    tokio::task::spawn_blocking(move || sync_directory(&parent))
        .await
        .map_err(|error| AppError::State(format!("parent directory sync task failed: {error}")))?
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> AppResult<()> {
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| AppError::Io {
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(windows)]
fn sync_directory(path: &Path) -> AppResult<()> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| AppError::Io {
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(not(any(unix, windows)))]
fn sync_directory(path: &Path) -> AppResult<()> {
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::state::model::{
        LiveSession, OutputPlan, RecordingPlan, RoomLifecycle, SegmentCloseReason,
        SessionLifecycle, UploadState,
    };

    fn setup() -> (TempDir, StateStore, LiveSession, Segment) {
        let dir = TempDir::new().unwrap();
        let store = StateStore::create_or_open(dir.path().join("state.redb")).unwrap();
        let session = LiveSession {
            id: Uuid::new_v4(),
            room_id: 1,
            room_name: "room".into(),
            title: "live".into(),
            started_at: jiff::Timestamp::now(),
            lifecycle: SessionLifecycle::Open,
            recording_plan: RecordingPlan {
                credential: None,
                output_dir: dir.path().into(),
                segment_time_ms: None,
                segment_size: None,
                min_segment_size: 0,
                qn: 10_000,
                cdn: Vec::new(),
            },
            output_plan: OutputPlan::LocalOnly,
            recording_events: Vec::new(),
        };
        transitions::create_session(&store, &session).unwrap();
        let segment = transitions::open_segment(
            &store,
            Segment {
                session_id: session.id,
                index: 1,
                part_path: dir.path().join("segment.flv.part"),
                final_path: dir.path().join("segment.flv"),
                artifact: ArtifactState::Writing,
                artifact_resolutions: Vec::new(),
                upload: UploadState::NotPlanned,
                upload_attempts: Vec::new(),
                upload_resolutions: Vec::new(),
            },
        )
        .unwrap();
        (dir, store, session, segment)
    }

    #[tokio::test]
    async fn finalize_never_replaces_an_existing_final_file() {
        let (_dir, store, session, segment) = setup();
        std::fs::write(&segment.part_path, b"new recording").unwrap();
        std::fs::write(&segment.final_path, b"existing recording").unwrap();

        finalize(
            &store,
            session.id,
            segment.index,
            SegmentCloseReason::StreamEnded,
        )
        .await
        .unwrap_err();

        assert_eq!(std::fs::read(&segment.part_path).unwrap(), b"new recording");
        assert_eq!(
            std::fs::read(&segment.final_path).unwrap(),
            b"existing recording"
        );
        assert!(matches!(
            store.get_segment(session.id, 1).unwrap().unwrap().artifact,
            ArtifactState::Finalizing { .. }
        ));
        assert!(matches!(
            store.get_session(session.id).unwrap().unwrap().lifecycle,
            SessionLifecycle::RecoveryRequired { .. }
        ));
        assert_eq!(
            store
                .get_room_state(session.room_id)
                .unwrap()
                .unwrap()
                .lifecycle,
            RoomLifecycle::Blocked {
                session_id: session.id
            }
        );
    }

    #[tokio::test]
    async fn discard_never_removes_part_when_a_final_file_exists() {
        let (_dir, store, session, segment) = setup();
        std::fs::write(&segment.part_path, b"small but unexplained").unwrap();
        std::fs::write(&segment.final_path, b"existing recording").unwrap();

        discard(
            &store,
            session.id,
            segment.index,
            SegmentCloseReason::StreamEnded,
        )
        .await
        .unwrap_err();

        assert_eq!(
            std::fs::read(&segment.part_path).unwrap(),
            b"small but unexplained"
        );
        assert_eq!(
            std::fs::read(&segment.final_path).unwrap(),
            b"existing recording"
        );
        assert!(matches!(
            store.get_segment(session.id, 1).unwrap().unwrap().artifact,
            ArtifactState::Discarding { .. }
        ));
        assert!(matches!(
            store.get_session(session.id).unwrap().unwrap().lifecycle,
            SessionLifecycle::RecoveryRequired { .. }
        ));

        let resolved = resolve_conflict(
            &store,
            session.id,
            segment.index,
            ArtifactResolutionDecision::KeepFinal,
            Some("final inspected".into()),
        )
        .await
        .unwrap();
        assert!(!segment.part_path.exists());
        assert_eq!(
            std::fs::read(&segment.final_path).unwrap(),
            b"existing recording"
        );
        assert!(matches!(resolved.artifact, ArtifactState::Ready { .. }));
    }

    #[tokio::test]
    async fn reconcile_completes_a_durable_finalization_intent() {
        let (_dir, store, session, segment) = setup();
        std::fs::write(&segment.part_path, b"recording").unwrap();
        transitions::begin_artifact_finalization(
            &store,
            session.id,
            segment.index,
            SegmentCloseReason::StreamEnded,
        )
        .unwrap();

        reconcile(&store).await.unwrap();

        assert!(!segment.part_path.exists());
        assert_eq!(std::fs::read(&segment.final_path).unwrap(), b"recording");
        assert!(matches!(
            store.get_segment(session.id, 1).unwrap().unwrap().artifact,
            ArtifactState::Ready { .. }
        ));
        assert_eq!(
            store.get_session(session.id).unwrap().unwrap().lifecycle,
            SessionLifecycle::Open
        );
    }

    #[tokio::test]
    async fn persisted_conflict_decision_is_replayed_without_asking_again() {
        let (_dir, store, session, segment) = setup();
        std::fs::write(&segment.part_path, b"chosen recording").unwrap();
        std::fs::write(&segment.final_path, b"discarded recording").unwrap();
        transitions::begin_artifact_finalization(
            &store,
            session.id,
            segment.index,
            SegmentCloseReason::StreamEnded,
        )
        .unwrap();
        reconcile(&store).await.unwrap();
        transitions::begin_artifact_conflict_resolution(
            &store,
            session.id,
            segment.index,
            ArtifactResolutionDecision::KeepPart,
            Some("part inspected".into()),
        )
        .unwrap();

        reconcile(&store).await.unwrap();

        assert!(!segment.part_path.exists());
        assert_eq!(
            std::fs::read(&segment.final_path).unwrap(),
            b"chosen recording"
        );
        let resolved = store.get_segment(session.id, 1).unwrap().unwrap();
        assert!(matches!(resolved.artifact, ArtifactState::Ready { .. }));
        assert_eq!(resolved.artifact_resolutions.len(), 1);
        assert_eq!(
            resolved.artifact_resolutions[0].decision,
            ArtifactResolutionDecision::KeepPart
        );
    }

    #[tokio::test]
    async fn keep_part_replay_finishes_after_the_rename_window() {
        let (_dir, store, session, segment) = setup();
        std::fs::write(&segment.part_path, b"chosen recording").unwrap();
        std::fs::write(&segment.final_path, b"discarded recording").unwrap();
        transitions::begin_artifact_finalization(
            &store,
            session.id,
            segment.index,
            SegmentCloseReason::StreamEnded,
        )
        .unwrap();
        transitions::require_recovery(&store, session.id, "both part and final files exist".into())
            .unwrap();
        transitions::begin_artifact_conflict_resolution(
            &store,
            session.id,
            segment.index,
            ArtifactResolutionDecision::KeepPart,
            None,
        )
        .unwrap();

        // Model a crash after the no-replace rename but before directory sync
        // and the final Ready transition.
        std::fs::remove_file(&segment.final_path).unwrap();
        std::fs::rename(&segment.part_path, &segment.final_path).unwrap();

        reconcile(&store).await.unwrap();

        assert!(!segment.part_path.exists());
        assert_eq!(
            std::fs::read(&segment.final_path).unwrap(),
            b"chosen recording"
        );
        assert!(matches!(
            store.get_segment(session.id, 1).unwrap().unwrap().artifact,
            ArtifactState::Ready { .. }
        ));
    }
}
