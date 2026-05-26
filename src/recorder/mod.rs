pub mod flv;
mod flv_metadata;
mod flv_pipeline;
pub mod segment;

use crate::error::{AppError, AppResult};
use crate::recorder::flv::{FlvHeader, FlvTag, FlvTagHeader, FlvTagType, read_previous_tag_size};
use crate::recorder::flv_metadata::{KeyframeIndex, build_metadata_body};
use crate::recorder::flv_pipeline::{
    FlvNormalizer, MediaGroupDecision, MediaGroupDeduplicator, NormalizedAction,
};
use crate::recorder::segment::{
    RecorderPolicy, SegmentEvent, final_path, part_path, should_filter_by_size,
    should_rotate_by_elapsed, should_rotate_by_size,
};
use crate::state::model::{Segment, SegmentStatus};
use crate::state::store::StateStore;

use reqwest::Response;
use std::path::PathBuf;
use std::time::Instant;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Debug)]
struct ActiveSegment {
    file: tokio::fs::File,
    index: u32,
    part_path: std::path::PathBuf,
    size: u64,
    start_time: Instant,
    metadata_source: Vec<u8>,
    metadata_len: usize,
    duration_ms: u32,
    keyframes: Vec<KeyframeIndex>,
}

#[derive(Debug)]
enum RecordPhase {
    WaitSync,
    Recording(Box<ActiveSegment>),
}

#[derive(Debug)]
struct PendingMediaTag {
    tag: FlvTag,
}

#[derive(Debug, Clone, Copy)]
enum PendingMediaFlush {
    Normal,
    Final,
}

struct InitialSegmentWrite {
    size: u64,
    metadata_source: Vec<u8>,
    metadata_len: usize,
}

fn is_video(tag: &FlvTag) -> bool {
    tag.header.tag_type == FlvTagType::Video
}

const METADATA_BODY_OFFSET: u64 = 13 + 11;
const KEYFRAME_INDEX_MIN_INTERVAL_MS: u32 = 1_900;

pub struct FlvRecorder<'a> {
    session_id: Uuid,
    policy: RecorderPolicy,
    store: &'a StateStore,
    event_tx: mpsc::UnboundedSender<SegmentEvent>,

    buffer: Vec<u8>,
    header: Option<FlvHeader>,
    normalizer: FlvNormalizer,
    pending_media_group: Vec<PendingMediaTag>,
    deduplicator: MediaGroupDeduplicator,

    next_index: u32,
    phase: RecordPhase,
}

impl<'a> FlvRecorder<'a> {
    pub async fn new(
        session_id: Uuid,
        policy: RecorderPolicy,
        store: &'a StateStore,
        event_tx: mpsc::UnboundedSender<SegmentEvent>,
        start_index: u32,
    ) -> AppResult<Self> {
        // Ensure output dir exists
        tokio::fs::create_dir_all(&policy.layout.output_dir)
            .await
            .map_err(|e| AppError::Io {
                path: policy.layout.output_dir.clone(),
                source: e,
            })?;

        Ok(Self {
            session_id,
            policy,
            store,
            event_tx,
            buffer: Vec::new(),
            header: None,
            normalizer: FlvNormalizer::new(),
            pending_media_group: Vec::new(),
            deduplicator: MediaGroupDeduplicator::new(),
            next_index: start_index,
            phase: RecordPhase::WaitSync,
        })
    }

    pub async fn push_chunk(&mut self, chunk: &[u8]) -> AppResult<()> {
        self.buffer.extend_from_slice(chunk);

        if self.header.is_none() {
            if self.buffer.len() >= 13 {
                let mut cursor = std::io::Cursor::new(&self.buffer);
                let h = FlvHeader::read(&mut cursor)
                    .map_err(|e| AppError::Bilibili(format!("FLV header error: {}", e)))?;
                let prev_size = read_previous_tag_size(&mut cursor)
                    .map_err(|e| AppError::Bilibili(format!("FLV prev tag size error: {}", e)))?;
                if prev_size != 0 {
                    return Err(AppError::Bilibili(format!(
                        "Invalid initial previous tag size: {}",
                        prev_size
                    )));
                }
                self.header = Some(h);
                self.buffer.drain(..13);
            } else {
                return Ok(());
            }
        }

        while self.buffer.len() >= 11 {
            let mut cursor = std::io::Cursor::new(&self.buffer);
            let tag_header = match FlvTagHeader::read(&mut cursor) {
                Ok(th) => th,
                Err(e) => return Err(AppError::Bilibili(format!("FLV tag header error: {}", e))),
            };

            let total_needed = 11 + (tag_header.data_size as usize) + 4;
            if self.buffer.len() < total_needed {
                break;
            }

            cursor.set_position(0);
            let tag = FlvTag::read(&mut cursor)
                .map_err(|e| AppError::Bilibili(format!("FLV tag read error: {}", e)))?;

            if self.normalizer.is_cache_tag(&tag) {
                self.flush_pending_media_group(PendingMediaFlush::Normal)
                    .await?;
            }

            let recording = matches!(self.phase, RecordPhase::Recording(_));
            let action = self.normalizer.observe_tag(tag, recording)?;
            let NormalizedAction::Write { tag, is_keyframe } = action else {
                self.buffer.drain(..total_needed);
                continue;
            };

            let mut allow_keyframe_rotation = true;
            if matches!(self.phase, RecordPhase::WaitSync) {
                if self.normalizer.is_synced() && is_keyframe {
                    self.open_new_segment().await?;
                    allow_keyframe_rotation = false;
                } else {
                    // Drop tags until we sync on a keyframe with all headers.
                    self.buffer.drain(..total_needed);
                    continue;
                }
            }

            if self.normalizer.has_pending_header_change() && !is_keyframe {
                self.buffer.drain(..total_needed);
                continue;
            }

            self.queue_media_tag(tag, is_keyframe, allow_keyframe_rotation)
                .await?;
            self.buffer.drain(..total_needed);
        }

        Ok(())
    }

    async fn queue_media_tag(
        &mut self,
        tag: FlvTag,
        is_keyframe: bool,
        allow_keyframe_rotation: bool,
    ) -> AppResult<()> {
        if self.starts_new_media_group(&tag, is_keyframe) {
            self.flush_pending_media_group(PendingMediaFlush::Normal)
                .await?;
        }

        if is_keyframe && allow_keyframe_rotation {
            self.rotate_before_keyframe_if_needed().await?;
        }

        self.pending_media_group.push(PendingMediaTag { tag });
        Ok(())
    }

    fn starts_new_media_group(&self, tag: &FlvTag, is_keyframe: bool) -> bool {
        let Some(last) = self.pending_media_group.last() else {
            return false;
        };

        if is_keyframe
            && self
                .pending_media_group
                .iter()
                .any(|item| is_video(&item.tag))
        {
            return true;
        }

        let diff = i64::from(tag.header.timestamp) - i64::from(last.tag.header.timestamp);
        !(-24_999..24_999).contains(&diff)
    }

    async fn rotate_before_keyframe_if_needed(&mut self) -> AppResult<()> {
        let mut needs_rotation = false;
        let header_change = self.normalizer.has_pending_header_change();
        if let RecordPhase::Recording(seg) = &self.phase
            && (header_change
                || should_rotate_by_size(seg.size, &self.policy.segment)
                || should_rotate_by_elapsed(seg.start_time.elapsed(), &self.policy.segment))
        {
            needs_rotation = true;
        }

        if needs_rotation {
            self.finalize_current_segment().await?;
            if header_change {
                self.deduplicator.reset();
            }
            self.open_new_segment().await?;
        }

        Ok(())
    }

    async fn flush_pending_media_group(&mut self, mode: PendingMediaFlush) -> AppResult<()> {
        if self.pending_media_group.is_empty() {
            return Ok(());
        }

        match self
            .deduplicator
            .observe(self.pending_media_group.iter().map(|item| &item.tag))
        {
            MediaGroupDecision::Unique => {}
            MediaGroupDecision::Duplicate => {
                tracing::warn!(
                    media_tags = self.pending_media_group.len(),
                    "dropping duplicated FLV media group"
                );
                self.pending_media_group.clear();
                return Ok(());
            }
            MediaGroupDecision::Reconnect => {
                tracing::warn!(
                    media_tags = self.pending_media_group.len(),
                    "dropping duplicated FLV media group after reconnect threshold"
                );
                self.pending_media_group.clear();
                if matches!(mode, PendingMediaFlush::Final) {
                    return Ok(());
                }
                return Err(AppError::StreamRepeatedData(
                    "received duplicated FLV media groups repeatedly".into(),
                ));
            }
        }

        let group = std::mem::take(&mut self.pending_media_group);
        for PendingMediaTag { tag } in group {
            self.write_media_tag(tag).await?;
        }

        Ok(())
    }

    async fn write_media_tag(&mut self, mut tag: FlvTag) -> AppResult<()> {
        // Write current tag
        if let RecordPhase::Recording(seg) = &mut self.phase {
            self.normalizer.normalize_media_timestamp(&mut tag);
            let mut buf = Vec::new();
            let file_position = seg.size;
            let timestamp = tag.header.timestamp;
            let is_keyframe = crate::recorder::flv::is_avc_keyframe(&tag.data);
            tag.write(&mut buf)
                .map_err(|e| AppError::Bilibili(format!("FLV tag write error: {}", e)))?;
            seg.file.write_all(&buf).await.map_err(|e| AppError::Io {
                path: seg.part_path.clone(),
                source: e,
            })?;
            seg.size += buf.len() as u64;
            seg.duration_ms = seg.duration_ms.max(timestamp);
            if is_keyframe
                && seg.keyframes.last().is_none_or(|last| {
                    timestamp.saturating_sub(last.time_ms) > KEYFRAME_INDEX_MIN_INTERVAL_MS
                })
            {
                seg.keyframes.push(KeyframeIndex {
                    time_ms: timestamp,
                    file_position,
                });
            }
        }

        Ok(())
    }

    async fn finalize_current_segment(&mut self) -> AppResult<()> {
        let mut seg = match std::mem::replace(&mut self.phase, RecordPhase::WaitSync) {
            RecordPhase::Recording(s) => s,
            RecordPhase::WaitSync => return Ok(()),
        };

        let final_p = final_path(&self.policy.layout, &self.session_id, seg.index);

        if should_filter_by_size(seg.size, &self.policy.filter) {
            if let Err(e) = seg.file.flush().await.map_err(|e| AppError::Io {
                path: seg.part_path.clone(),
                source: e,
            }) {
                return self.persist_failed_segment(seg.index, seg.part_path.clone(), e);
            }
            drop(seg.file);

            let db_seg = Segment {
                session_id: self.session_id,
                index: seg.index,
                path: final_p.clone(),
                status: SegmentStatus::Filtered,
                error: None,
            };
            if let Err(e) = self.store.put_segment(&db_seg) {
                return self.persist_failed_segment(seg.index, seg.part_path.clone(), e);
            }

            if let Err(e) = tokio::fs::remove_file(&seg.part_path)
                .await
                .map_err(|e| AppError::Io {
                    path: seg.part_path.clone(),
                    source: e,
                })
            {
                return self.persist_failed_segment(seg.index, seg.part_path.clone(), e);
            }

            let _ = self.event_tx.send(SegmentEvent::Filtered {
                session_id: self.session_id,
                index: seg.index,
                path: final_p,
                size: seg.size,
            });
            return Ok(());
        }

        if let Err(e) = rewrite_segment_metadata(&mut seg).await {
            return self.persist_failed_segment(seg.index, seg.part_path.clone(), e);
        }

        if let Err(e) = seg.file.flush().await.map_err(|e| AppError::Io {
            path: seg.part_path.clone(),
            source: e,
        }) {
            return self.persist_failed_segment(seg.index, seg.part_path.clone(), e);
        }
        drop(seg.file);

        if let Err(e) = tokio::fs::rename(&seg.part_path, &final_p)
            .await
            .map_err(|e| AppError::Io {
                path: final_p.clone(),
                source: e,
            })
        {
            return self.persist_failed_segment(seg.index, seg.part_path.clone(), e);
        }

        let db_seg = Segment {
            session_id: self.session_id,
            index: seg.index,
            path: final_p.clone(),
            status: SegmentStatus::Finalized,
            error: None,
        };
        if let Err(e) = self.store.put_segment(&db_seg) {
            let rollback_res =
                tokio::fs::rename(&final_p, &seg.part_path)
                    .await
                    .map_err(|rollback_err| AppError::Io {
                        path: seg.part_path.clone(),
                        source: rollback_err,
                    });

            let failure_path = if rollback_res.is_ok() {
                seg.part_path.clone()
            } else {
                final_p.clone()
            };
            let mut error = e.to_string();
            if let Err(rollback_err) = rollback_res {
                error = format!(
                    "{error}; additionally failed to roll back finalized file from {} to {}: {rollback_err}",
                    final_p.display(),
                    seg.part_path.display()
                );
            }
            return self.persist_failed_segment(seg.index, failure_path, AppError::State(error));
        }

        let _ = self.event_tx.send(SegmentEvent::Finalized {
            session_id: self.session_id,
            index: seg.index,
            path: final_p,
            size: seg.size,
        });

        Ok(())
    }

    fn persist_failed_segment(
        &self,
        index: u32,
        path: PathBuf,
        original: AppError,
    ) -> AppResult<()> {
        let original_msg = original.to_string();
        let db_seg = Segment {
            session_id: self.session_id,
            index,
            path,
            status: SegmentStatus::Failed,
            error: Some(original_msg.clone()),
        };
        self.store.put_segment(&db_seg).map_err(|persist_err| {
            AppError::State(format!(
                "{original_msg}; additionally failed to persist failed segment state: {persist_err}"
            ))
        })?;
        Err(original)
    }

    async fn open_new_segment(&mut self) -> AppResult<()> {
        let idx = self.next_index;
        let p_path = part_path(&self.policy.layout, &self.session_id, idx);

        // Persist truth before risk: the segment row goes into redb *before*
        // we create a file on disk. If we crashed between File::create and
        // put_segment, recovery would never see the orphan .part file because
        // it scans the DB, not the filesystem. With DB-first, an orphan can
        // only mean "DB row exists, file missing" — which recovery handles by
        // marking the segment Failed (or MissingSegmentFile) on the next scan.
        let db_seg = Segment {
            session_id: self.session_id,
            index: idx,
            path: p_path.clone(),
            status: SegmentStatus::Recording,
            error: None,
        };
        self.store.put_segment(&db_seg)?;

        let mut file = match tokio::fs::File::create(&p_path)
            .await
            .map_err(|e| AppError::Io {
                path: p_path.clone(),
                source: e,
            }) {
            Ok(f) => f,
            Err(create_err) => return self.persist_failed_segment(idx, p_path.clone(), create_err),
        };

        let initial = match self.write_initial_segment_headers(&mut file, &p_path).await {
            Ok(initial) => initial,
            Err(write_err) => return self.persist_failed_segment(idx, p_path.clone(), write_err),
        };

        // Both DB and initial file writes are in place — advance next_index now
        // so a retry does not reuse this index.
        self.next_index = idx + 1;

        let _ = self.event_tx.send(SegmentEvent::Started {
            session_id: self.session_id,
            index: idx,
            part_path: p_path.clone(),
        });

        self.normalizer.start_new_file();

        self.phase = RecordPhase::Recording(Box::new(ActiveSegment {
            file,
            index: idx,
            part_path: p_path,
            size: initial.size,
            start_time: Instant::now(),
            metadata_source: initial.metadata_source,
            metadata_len: initial.metadata_len,
            duration_ms: 0,
            keyframes: Vec::new(),
        }));

        Ok(())
    }

    async fn write_initial_segment_headers(
        &self,
        file: &mut tokio::fs::File,
        path: &std::path::Path,
    ) -> AppResult<InitialSegmentWrite> {
        let mut size = 0;
        let mut buf = Vec::new();

        let h = self
            .header
            .as_ref()
            .expect("Invariant violated: FLV header must exist when opening segment");
        h.write(&mut buf)
            .map_err(|e| AppError::Bilibili(format!("FLV header write error: {}", e)))?;
        crate::recorder::flv::write_previous_tag_size(&mut buf, 0)
            .map_err(|e| AppError::Bilibili(format!("FLV prev tag size write error: {}", e)))?;
        file.write_all(&buf).await.map_err(|e| AppError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        size += buf.len() as u64;
        buf.clear();

        // Always inject cached headers. Since we wait for headers in WaitSync,
        // we will always have them before creating any segment.
        let meta = self
            .normalizer
            .metadata_tag
            .as_ref()
            .expect("Invariant violated: Metadata must exist when opening segment");
        let mut meta = meta.clone();
        meta.header.timestamp = 0;
        meta.data = build_metadata_body(&meta.data, 0, &[]);
        meta.header.data_size = meta.data.len() as u32;
        let metadata_source = meta.data.clone();
        let metadata_len = meta.data.len();
        meta.write(&mut buf)
            .map_err(|e| AppError::Bilibili(format!("FLV metadata write error: {}", e)))?;

        let avc = self
            .normalizer
            .avc_seq_tag
            .as_ref()
            .expect("Invariant violated: AVC sequence header must exist when opening segment");
        let mut avc = avc.clone();
        avc.header.timestamp = 0;
        avc.write(&mut buf)
            .map_err(|e| AppError::Bilibili(format!("FLV AVC seq write error: {}", e)))?;

        let aac = self
            .normalizer
            .aac_seq_tag
            .as_ref()
            .expect("Invariant violated: AAC sequence header must exist when opening segment");
        let mut aac = aac.clone();
        aac.header.timestamp = 0;
        aac.write(&mut buf)
            .map_err(|e| AppError::Bilibili(format!("FLV AAC seq write error: {}", e)))?;

        if !buf.is_empty() {
            file.write_all(&buf).await.map_err(|e| AppError::Io {
                path: path.to_path_buf(),
                source: e,
            })?;
            size += buf.len() as u64;
        }

        Ok(InitialSegmentWrite {
            size,
            metadata_source,
            metadata_len,
        })
    }

    pub async fn finalize(&mut self) -> AppResult<()> {
        if !self.buffer.is_empty() {
            tracing::warn!(
                leftover_bytes = self.buffer.len(),
                "discarding incomplete trailing FLV tag before finalizing segment"
            );
            self.buffer.clear();
        }
        self.flush_pending_media_group(PendingMediaFlush::Final)
            .await?;
        self.finalize_current_segment().await?;
        Ok(())
    }

    pub fn mark_failed(&mut self, err_msg: &str) -> AppResult<()> {
        if let RecordPhase::Recording(seg) =
            std::mem::replace(&mut self.phase, RecordPhase::WaitSync)
        {
            let db_seg = Segment {
                session_id: self.session_id,
                index: seg.index,
                path: seg.part_path.clone(),
                status: SegmentStatus::Failed,
                error: Some(err_msg.to_string()),
            };
            self.store.put_segment(&db_seg)?;
        }
        Ok(())
    }
}

async fn rewrite_segment_metadata(seg: &mut ActiveSegment) -> AppResult<()> {
    let body = build_metadata_body(&seg.metadata_source, seg.duration_ms, &seg.keyframes);
    if body.len() != seg.metadata_len {
        return Err(AppError::Bilibili(format!(
            "FLV metadata rewrite size changed: expected {}, got {}",
            seg.metadata_len,
            body.len()
        )));
    }

    seg.file
        .seek(std::io::SeekFrom::Start(METADATA_BODY_OFFSET))
        .await
        .map_err(|e| AppError::Io {
            path: seg.part_path.clone(),
            source: e,
        })?;
    seg.file.write_all(&body).await.map_err(|e| AppError::Io {
        path: seg.part_path.clone(),
        source: e,
    })?;
    seg.file
        .seek(std::io::SeekFrom::End(0))
        .await
        .map_err(|e| AppError::Io {
            path: seg.part_path.clone(),
            source: e,
        })?;

    Ok(())
}

/// A thin wrapper that reads from an HTTP response and drives the FlvRecorder.
pub async fn record_flv(
    mut resp: Response,
    session_id: Uuid,
    policy: RecorderPolicy,
    store: &StateStore,
    event_tx: mpsc::UnboundedSender<SegmentEvent>,
    start_index: u32,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> AppResult<()> {
    if !resp.status().is_success() {
        return Err(AppError::Bilibili(format!(
            "Non-success HTTP status: {}",
            resp.status()
        )));
    }

    let mut recorder = FlvRecorder::new(session_id, policy, store, event_tx, start_index).await?;

    let mut graceful_shutdown = false;
    let result: AppResult<()> = async {
        loop {
            tokio::select! {
                chunk_res = tokio::time::timeout(std::time::Duration::from_secs(30), resp.chunk()) => {
                    match chunk_res {
                        Ok(Ok(Some(data))) => {
                            if let Err(error) = recorder.push_chunk(&data).await {
                                if matches!(error, AppError::StreamRepeatedData(_)) {
                                    tracing::warn!("{}", error);
                                    break;
                                }
                                return Err(error);
                            }
                        }
                        Ok(Ok(None)) => break,
                        Ok(Err(e)) => {
                            tracing::warn!("Stream connection dropped: {}", e);
                            break;
                        }
                        Err(_) => {
                            tracing::warn!("Stream idle timeout: no data received for 30s");
                            break;
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    tracing::info!("Graceful shutdown requested, finalizing segment");
                    graceful_shutdown = true;
                    break;
                }
            }
        }
        recorder.finalize().await?;
        if graceful_shutdown {
            return Err(AppError::GracefulShutdown);
        }
        Ok(())
    }
    .await;

    if let Err(e) = result {
        if matches!(e, AppError::GracefulShutdown) {
            return Err(e);
        }
        let err_msg = e.to_string();
        recorder.mark_failed(&err_msg).map_err(|persist_err| {
            AppError::State(format!(
                "{err_msg}; additionally failed to persist failed segment state: {persist_err}"
            ))
        })?;
        return Err(e);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recorder::flv::{
        FlvTagType, is_aac_sequence_header, is_avc_sequence_header, read_previous_tag_size,
    };
    use tempfile::tempdir;

    fn test_policy(
        output_dir: PathBuf,
        segment_size: Option<u64>,
        segment_time: Option<std::time::Duration>,
        min_segment_size: u64,
    ) -> RecorderPolicy {
        RecorderPolicy {
            layout: crate::recorder::segment::SegmentLayout { output_dir },
            segment: crate::recorder::segment::SegmentPolicy {
                segment_time,
                segment_size,
            },
            filter: crate::recorder::segment::SegmentFilter { min_segment_size },
        }
    }

    fn write_tag(tag: FlvTag) -> Vec<u8> {
        let mut buf = Vec::new();
        tag.write(&mut buf).unwrap();
        buf
    }

    fn script_tag(timestamp: u32, data: Vec<u8>) -> FlvTag {
        FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::ScriptData,
                data_size: data.len() as u32,
                timestamp,
                stream_id: 0,
            },
            data,
        }
    }

    fn video_tag(timestamp: u32, data: Vec<u8>) -> FlvTag {
        FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: data.len() as u32,
                timestamp,
                stream_id: 0,
            },
            data,
        }
    }

    fn audio_tag(timestamp: u32, data: Vec<u8>) -> FlvTag {
        FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Audio,
                data_size: data.len() as u32,
                timestamp,
                stream_id: 0,
            },
            data,
        }
    }

    async fn start_recording_with_headers(recorder: &mut FlvRecorder<'_>) {
        recorder
            .push_chunk(b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00")
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(script_tag(0, vec![0, 1, 2, 3, 4])))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(0, vec![0x17, 0x00, 0, 0, 0])))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(audio_tag(0, vec![0xAF, 0x00])))
            .await
            .unwrap();
    }

    fn read_recorded_tags(path: &std::path::Path) -> Vec<FlvTag> {
        let mut file = std::fs::File::open(path).unwrap();
        FlvHeader::read(&mut file).unwrap();
        assert_eq!(read_previous_tag_size(&mut file).unwrap(), 0);

        let mut tags = Vec::new();
        loop {
            match FlvTag::read(&mut file) {
                Ok(tag) => tags.push(tag),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => panic!("unexpected FLV read error: {e}"),
            }
        }
        tags
    }

    #[tokio::test]
    async fn test_flv_recorder_push_chunk() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();

        let policy = test_policy(dir.path().to_path_buf(), Some(1024), None, 0);

        let session_id = Uuid::new_v4();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx, 1)
            .await
            .unwrap();

        // Push valid FLV header + prev tag size (0)
        let header_bytes = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00";
        recorder.push_chunk(header_bytes).await.unwrap();

        // Helper to push an AVC tag
        let push_avc_tag = |is_keyframe: bool| {
            let mut tag_buf = Vec::new();
            let frame_type = if is_keyframe { 1 } else { 2 };
            let codec_id = 7;
            let packet_type = 1; // NALU
            let first_byte = (frame_type << 4) | codec_id;

            let tag = FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Video,
                    data_size: 5,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![first_byte, packet_type, 0, 0, 0],
            };
            tag.write(&mut tag_buf).unwrap(); // 11 + 5 + 4 = 20 bytes
            tag_buf
        };

        let write_tag = |tag: FlvTag| {
            let mut b = Vec::new();
            tag.write(&mut b).unwrap();
            b
        };

        // Push script, avc seq, aac seq, then keyframe to enter Recording
        let metadata_tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::ScriptData,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![0, 1, 2, 3, 4],
        };
        let avc_seq_tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![(1 << 4) | 7, 0, 0, 0, 0], // AVC seq
        };
        let aac_seq_tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Audio,
                data_size: 2,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![0xAF, 0], // AAC seq
        };

        // Push in pieces to test chunk boundaries
        let meta_buf = write_tag(metadata_tag.clone());
        recorder.push_chunk(&meta_buf[..5]).await.unwrap();
        recorder.push_chunk(&meta_buf[5..]).await.unwrap();

        recorder.push_chunk(&write_tag(avc_seq_tag)).await.unwrap();
        recorder.push_chunk(&write_tag(aac_seq_tag)).await.unwrap();

        assert!(matches!(recorder.phase, RecordPhase::WaitSync)); // Still WaitSync

        // Push keyframe to start segment
        recorder.push_chunk(&push_avc_tag(true)).await.unwrap();

        assert_eq!(recorder.buffer.len(), 0);
        assert!(matches!(recorder.phase, RecordPhase::Recording(_)));
        assert_eq!(recorder.normalizer.metadata_tag, Some(metadata_tag));

        // Finish
        recorder.finalize().await.unwrap();

        // Verify events
        let ev1 = rx.recv().await.unwrap();
        match ev1 {
            SegmentEvent::Started { index, .. } => assert_eq!(index, 1),
            _ => panic!("Expected Started event"),
        }

        let ev2 = rx.recv().await.unwrap();
        match ev2 {
            SegmentEvent::Finalized { index, size, .. } => {
                assert_eq!(index, 1);
                assert!(size > 0);
            }
            _ => panic!("Expected Finalized event"),
        }

        // Verify `.part -> .flv` lifecycle
        let part_p = part_path(&policy.layout, &session_id, 1);
        let final_p = final_path(&policy.layout, &session_id, 1);
        assert!(!part_p.exists(), ".part file should be gone");
        assert!(final_p.exists(), ".flv file should exist");
        assert!(
            std::fs::metadata(&final_p).unwrap().len() > 0,
            "file should not be empty"
        );

        // Verify redb persistence
        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].index, 1);
        assert_eq!(segments[0].path, final_p);
        assert_eq!(segments[0].status, SegmentStatus::Finalized);
    }

    #[tokio::test]
    async fn test_flv_recorder_filtering() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();

        let policy = test_policy(dir.path().to_path_buf(), None, None, 99999);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx, 1)
            .await
            .unwrap();

        // Push valid FLV header + prev tag size (0)
        let header_bytes = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00";
        recorder.push_chunk(header_bytes).await.unwrap();

        // Helper
        let write_tag = |tag: FlvTag| {
            let mut b = Vec::new();
            tag.write(&mut b).unwrap();
            b
        };

        let metadata_tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::ScriptData,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![0, 1, 2, 3, 4],
        };
        let avc_seq_tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![(1 << 4) | 7, 0, 0, 0, 0], // AVC seq
        };
        let aac_seq_tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Audio,
                data_size: 2,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![0xAF, 0], // AAC seq
        };

        recorder.push_chunk(&write_tag(metadata_tag)).await.unwrap();
        recorder.push_chunk(&write_tag(avc_seq_tag)).await.unwrap();
        recorder.push_chunk(&write_tag(aac_seq_tag)).await.unwrap();

        // Push a small keyframe tag to open a segment
        let mut tag_buf = Vec::new();
        let tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![(1 << 4) | 7, 1, 0, 0, 0], // NALU keyframe
        };
        tag.write(&mut tag_buf).unwrap();
        recorder.push_chunk(&tag_buf).await.unwrap();

        recorder.finalize().await.unwrap();

        let part_p = part_path(&policy.layout, &session_id, 1);
        let final_p = final_path(&policy.layout, &session_id, 1);
        assert!(!part_p.exists(), ".part file should be removed");
        assert!(!final_p.exists(), ".flv file should not be created");

        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].status, SegmentStatus::Filtered);
    }

    #[tokio::test]
    async fn test_flv_recorder_rotation() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();

        let policy = test_policy(dir.path().to_path_buf(), Some(50), None, 0);

        let session_id = Uuid::new_v4();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx, 1)
            .await
            .unwrap();

        let header_bytes = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00";
        recorder.push_chunk(header_bytes).await.unwrap(); // ~13 bytes

        // Helper to push an AVC tag
        let push_avc_tag = |is_keyframe: bool| {
            let mut tag_buf = Vec::new();
            let frame_type = if is_keyframe { 1 } else { 2 };
            let codec_id = 7;
            let packet_type = 1; // NALU
            let first_byte = (frame_type << 4) | codec_id;

            let tag = FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Video,
                    data_size: 5,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![first_byte, packet_type, 0, 0, 0],
            };
            tag.write(&mut tag_buf).unwrap(); // 11 + 5 + 4 = 20 bytes
            tag_buf
        };
        let write_tag = |tag: FlvTag| {
            let mut b = Vec::new();
            tag.write(&mut b).unwrap();
            b
        };

        let metadata_tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::ScriptData,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![0, 1, 2, 3, 4],
        };
        let avc_seq_tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![(1 << 4) | 7, 0, 0, 0, 0],
        };
        let aac_seq_tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Audio,
                data_size: 2,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![0xAF, 0],
        };

        recorder.push_chunk(&write_tag(metadata_tag)).await.unwrap();
        recorder.push_chunk(&write_tag(avc_seq_tag)).await.unwrap();
        recorder.push_chunk(&write_tag(aac_seq_tag)).await.unwrap(); // 13 header + (20+20+17) = 70. Wait, limits apply AFTER seg creation.

        // Tag 1: Keyframe (size 20) -> starts segment. Size starts with header+seq headers injected!
        // header (13) + headers (20+20+17=57) = 70 bytes. Plus the tag itself (20 bytes) = 90 bytes.
        // Wait, 90 >= 50! So the FIRST tag will already put size at 90.
        recorder.push_chunk(&push_avc_tag(true)).await.unwrap();

        // Tag 2: Interframe (size 20) -> segment 1 size = 90 + 20 = 110
        // Limit is 50. But this is an interframe, so no rotation yet.
        recorder.push_chunk(&push_avc_tag(false)).await.unwrap();

        // Tag 3: Keyframe (size 20). Limit exceeded (110 >= 50) and it's a keyframe!
        // Should rotate before writing Tag 3.
        recorder.push_chunk(&push_avc_tag(true)).await.unwrap();

        recorder.finalize().await.unwrap();

        // Expect events:
        // Started 1, Finalized 1, Started 2, Finalized 2
        let mut finalized_count = 0;
        while let Ok(ev) = rx.try_recv() {
            if let SegmentEvent::Finalized { .. } = ev {
                finalized_count += 1;
            }
        }
        assert_eq!(finalized_count, 2);

        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments.len(), 2);
    }

    #[tokio::test]
    async fn test_flv_recorder_incomplete_eof() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();

        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy, &store, tx, 1)
            .await
            .unwrap();

        // Push valid FLV header + prev tag size (0)
        let header_bytes = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00";
        recorder.push_chunk(header_bytes).await.unwrap();

        // Push headers to enter Recording
        let write_tag = |tag: FlvTag| {
            let mut b = Vec::new();
            tag.write(&mut b).unwrap();
            b
        };
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::ScriptData,
                    data_size: 5,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![0, 1, 2, 3, 4],
            }))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Video,
                    data_size: 5,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![(1 << 4) | 7, 0, 0, 0, 0],
            }))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Audio,
                    data_size: 2,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![0xAF, 0],
            }))
            .await
            .unwrap();

        // Push a complete keyframe tag to open a segment
        let mut valid_tag_buf = Vec::new();
        let valid_tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![(1 << 4) | 7, 1, 0, 0, 0],
        };
        valid_tag.write(&mut valid_tag_buf).unwrap();
        recorder.push_chunk(&valid_tag_buf).await.unwrap();

        // Push an incomplete tag (missing data)
        let mut tag_buf = Vec::new();
        let tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::ScriptData,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![0, 1, 2, 3, 4],
        };
        tag.write(&mut tag_buf).unwrap();

        recorder.push_chunk(&tag_buf[..5]).await.unwrap();

        recorder.finalize().await.unwrap();

        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].status, SegmentStatus::Finalized);
    }

    #[tokio::test]
    async fn test_flv_recorder_drops_redundant_headers_without_rotating() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();
        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx, 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        recorder
            .push_chunk(&write_tag(video_tag(1_000, vec![0x17, 0x01, 0, 0, 0])))
            .await
            .unwrap();

        // These are common stream-side refreshes. They update the cached
        // boundary truth but should not be written into the current file and
        // should not create a new segment.
        recorder
            .push_chunk(&write_tag(script_tag(1_010, vec![9, 9, 9, 9, 9])))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(1_020, vec![0x17, 0x00, 0, 0, 0])))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(audio_tag(1_020, vec![0xAF, 0x00])))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(1_033, vec![0x17, 0x01, 0, 0, 0])))
            .await
            .unwrap();

        recorder.finalize().await.unwrap();

        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].status, SegmentStatus::Finalized);

        let tags = read_recorded_tags(&final_path(&policy.layout, &session_id, 1));
        assert_eq!(
            tags.iter()
                .filter(|tag| tag.header.tag_type == FlvTagType::ScriptData)
                .count(),
            1
        );
        assert_eq!(
            tags.iter()
                .filter(|tag| {
                    tag.header.tag_type == FlvTagType::Video && is_avc_sequence_header(&tag.data)
                })
                .count(),
            1
        );
        assert_eq!(
            tags.iter()
                .filter(|tag| {
                    tag.header.tag_type == FlvTagType::Audio && is_aac_sequence_header(&tag.data)
                })
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn test_flv_recorder_rebases_timestamp_jump_without_rotating() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();
        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx, 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        recorder
            .push_chunk(&write_tag(video_tag(10_000, vec![0x17, 0x01, 0, 0, 0])))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(10_033, vec![0x27, 0x01, 0, 0, 0])))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(50_000, vec![0x27, 0x01, 0, 0, 0])))
            .await
            .unwrap();

        recorder.finalize().await.unwrap();

        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments.len(), 1);

        let tags = read_recorded_tags(&final_path(&policy.layout, &session_id, 1));
        let media_video_timestamps: Vec<_> = tags
            .iter()
            .filter(|tag| {
                tag.header.tag_type == FlvTagType::Video && !is_avc_sequence_header(&tag.data)
            })
            .map(|tag| tag.header.timestamp)
            .collect();

        assert_eq!(media_video_timestamps, vec![0, 33, 66]);
    }

    #[tokio::test]
    async fn test_flv_recorder_drops_duplicated_media_group() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();
        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx, 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        let keyframe = video_tag(1_000, vec![0x17, 0x01, 1, 2, 3]);
        recorder
            .push_chunk(&write_tag(keyframe.clone()))
            .await
            .unwrap();
        recorder.push_chunk(&write_tag(keyframe)).await.unwrap();

        recorder.finalize().await.unwrap();

        let tags = read_recorded_tags(&final_path(&policy.layout, &session_id, 1));
        let media_video_timestamps: Vec<_> = tags
            .iter()
            .filter(|tag| {
                tag.header.tag_type == FlvTagType::Video && !is_avc_sequence_header(&tag.data)
            })
            .map(|tag| tag.header.timestamp)
            .collect();
        assert_eq!(media_video_timestamps, vec![0]);
    }

    #[tokio::test]
    async fn test_flv_recorder_finalizes_when_duplicate_threshold_hits_on_final_flush() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();
        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx, 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        let keyframe = video_tag(1_000, vec![0x17, 0x01, 1, 2, 3]);
        for _ in 0..12 {
            recorder
                .push_chunk(&write_tag(keyframe.clone()))
                .await
                .unwrap();
        }

        recorder.finalize().await.unwrap();

        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].status, SegmentStatus::Finalized);

        let tags = read_recorded_tags(&final_path(&policy.layout, &session_id, 1));
        let media_video_timestamps: Vec<_> = tags
            .iter()
            .filter(|tag| {
                tag.header.tag_type == FlvTagType::Video && !is_avc_sequence_header(&tag.data)
            })
            .map(|tag| tag.header.timestamp)
            .collect();
        assert_eq!(media_video_timestamps, vec![0]);
    }

    #[tokio::test]
    async fn test_flv_recorder_keeps_same_media_data_with_new_timestamp() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();
        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx, 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        recorder
            .push_chunk(&write_tag(video_tag(1_000, vec![0x17, 0x01, 1, 2, 3])))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(1_033, vec![0x17, 0x01, 1, 2, 3])))
            .await
            .unwrap();

        recorder.finalize().await.unwrap();

        let tags = read_recorded_tags(&final_path(&policy.layout, &session_id, 1));
        let media_video_timestamps: Vec<_> = tags
            .iter()
            .filter(|tag| {
                tag.header.tag_type == FlvTagType::Video && !is_avc_sequence_header(&tag.data)
            })
            .map(|tag| tag.header.timestamp)
            .collect();
        assert_eq!(media_video_timestamps, vec![0, 33]);
    }

    #[tokio::test]
    async fn test_flv_recorder_rewrites_metadata_duration_and_keyframes() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();
        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx, 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        recorder
            .push_chunk(&write_tag(video_tag(1_000, vec![0x17, 0x01, 1, 2, 3])))
            .await
            .unwrap();
        for timestamp in [1_400, 1_800, 2_200, 2_600] {
            recorder
                .push_chunk(&write_tag(video_tag(timestamp, vec![0x27, 0x01, 1, 2, 3])))
                .await
                .unwrap();
        }
        recorder
            .push_chunk(&write_tag(video_tag(3_033, vec![0x17, 0x01, 4, 5, 6])))
            .await
            .unwrap();

        recorder.finalize().await.unwrap();

        let tags = read_recorded_tags(&final_path(&policy.layout, &session_id, 1));
        let metadata = tags
            .iter()
            .find(|tag| tag.header.tag_type == FlvTagType::ScriptData)
            .expect("recorded FLV should have metadata");
        let duration =
            crate::recorder::flv_metadata::debug_number_property(&metadata.data, "duration")
                .expect("metadata should contain duration");
        let keyframe_times = crate::recorder::flv_metadata::debug_keyframe_times(&metadata.data)
            .expect("metadata should contain keyframes.times");

        assert_eq!(duration, 2.033);
        assert_eq!(keyframe_times, vec![0.0, 2.033]);
    }

    #[tokio::test]
    async fn test_flv_recorder_drops_media_until_keyframe_after_header_change() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();
        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx, 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        recorder
            .push_chunk(&write_tag(video_tag(1_000, vec![0x17, 0x01, 0, 0, 0])))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(1_010, vec![0x17, 0x00, 9, 9, 9])))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(audio_tag(1_020, vec![0xAF, 0x01])))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(1_033, vec![0x27, 0x01, 0, 0, 0])))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(1_066, vec![0x17, 0x01, 0, 0, 0])))
            .await
            .unwrap();

        recorder.finalize().await.unwrap();

        let first_tags = read_recorded_tags(&final_path(&policy.layout, &session_id, 1));
        let first_media_count = first_tags
            .iter()
            .filter(|tag| {
                (tag.header.tag_type == FlvTagType::Video && !is_avc_sequence_header(&tag.data))
                    || (tag.header.tag_type == FlvTagType::Audio
                        && !is_aac_sequence_header(&tag.data))
            })
            .count();
        assert_eq!(first_media_count, 1);

        let second_tags = read_recorded_tags(&final_path(&policy.layout, &session_id, 2));
        let second_media_timestamps: Vec<_> = second_tags
            .iter()
            .filter(|tag| {
                tag.header.tag_type == FlvTagType::Video && !is_avc_sequence_header(&tag.data)
            })
            .map(|tag| tag.header.timestamp)
            .collect();
        assert_eq!(second_media_timestamps, vec![0]);
    }

    #[tokio::test]
    async fn test_flv_recorder_marks_failed_when_final_rename_fails() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();

        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx, 1)
            .await
            .unwrap();

        let header_bytes = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00";
        recorder.push_chunk(header_bytes).await.unwrap();

        let write_tag = |tag: FlvTag| {
            let mut b = Vec::new();
            tag.write(&mut b).unwrap();
            b
        };
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::ScriptData,
                    data_size: 5,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![0, 1, 2, 3, 4],
            }))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Video,
                    data_size: 5,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![(1 << 4) | 7, 0, 0, 0, 0],
            }))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Audio,
                    data_size: 2,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![0xAF, 0],
            }))
            .await
            .unwrap();

        let mut tag_buf = Vec::new();
        let tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![0x17, 0x01, 0, 0, 0],
        };
        tag.write(&mut tag_buf).unwrap();
        recorder.push_chunk(&tag_buf).await.unwrap();

        let part_p = part_path(&policy.layout, &session_id, 1);
        let final_p = final_path(&policy.layout, &session_id, 1);
        std::fs::create_dir(&final_p).unwrap();

        let err = recorder.finalize().await.unwrap_err();
        assert!(err.to_string().contains(&final_p.display().to_string()));

        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].status, SegmentStatus::Failed);
        assert_eq!(segments[0].path, part_p);
        assert!(segments[0].error.as_ref().unwrap().contains("io error"));
        assert!(part_p.exists(), ".part file should remain recoverable");
    }

    #[tokio::test]
    async fn test_flv_recorder_drop_orphan_frames() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();

        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy, &store, tx, 1)
            .await
            .unwrap();

        let header_bytes = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00";
        recorder.push_chunk(header_bytes).await.unwrap();

        let write_tag = |tag: FlvTag| {
            let mut b = Vec::new();
            tag.write(&mut b).unwrap();
            b
        };

        // Push some P-frames (orphan frames) without headers
        for _ in 0..5 {
            let p_frame = FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Video,
                    data_size: 5,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![(2 << 4) | 7, 1, 0, 0, 0], // Interframe NALU
            };
            recorder.push_chunk(&write_tag(p_frame)).await.unwrap();
        }

        // Recorder should still be in WaitSync and no segment created
        assert!(matches!(recorder.phase, RecordPhase::WaitSync));
        assert!(matches!(recorder.phase, RecordPhase::WaitSync));

        // Now push headers
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::ScriptData,
                    data_size: 5,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![0, 1, 2, 3, 4],
            }))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Video,
                    data_size: 5,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![(1 << 4) | 7, 0, 0, 0, 0],
            }))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Audio,
                    data_size: 2,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![0xAF, 0],
            }))
            .await
            .unwrap();

        assert!(matches!(recorder.phase, RecordPhase::WaitSync));
        assert!(matches!(recorder.phase, RecordPhase::WaitSync));

        // Push a keyframe, it should transition to Recording and open a segment
        let keyframe = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![(1 << 4) | 7, 1, 0, 0, 0],
        };
        recorder.push_chunk(&write_tag(keyframe)).await.unwrap();

        assert!(matches!(recorder.phase, RecordPhase::Recording(_)));
        assert!(matches!(recorder.phase, RecordPhase::Recording(_)));
    }

    #[tokio::test]
    async fn test_flv_recorder_sequence_change() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();

        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy, &store, tx, 1)
            .await
            .unwrap();

        let header_bytes = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00";
        recorder.push_chunk(header_bytes).await.unwrap();

        let write_tag = |tag: FlvTag| {
            let mut b = Vec::new();
            tag.write(&mut b).unwrap();
            b
        };

        // 1. Send initial headers
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::ScriptData,
                    data_size: 5,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![0, 1, 2, 3, 4],
            }))
            .await
            .unwrap();
        let initial_avc_seq = vec![(1 << 4) | 7, 0, 0, 0, 0];
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Video,
                    data_size: 5,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: initial_avc_seq.clone(),
            }))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Audio,
                    data_size: 2,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![0xAF, 0],
            }))
            .await
            .unwrap();

        // 2. Send keyframe to start first segment
        let keyframe = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![(1 << 4) | 7, 1, 0, 0, 0],
        };
        recorder
            .push_chunk(&write_tag(keyframe.clone()))
            .await
            .unwrap();

        assert!(matches!(recorder.phase, RecordPhase::Recording(_)));
        assert!(!recorder.normalizer.has_pending_header_change());

        // 3. Send a new AVC seq with different data
        let new_avc_seq = vec![(1 << 4) | 7, 0, 9, 9, 9];
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Video,
                    data_size: 5,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: new_avc_seq.clone(),
            }))
            .await
            .unwrap();

        // It should mark dirty, but not rotate immediately since it's not a keyframe
        assert!(recorder.normalizer.has_pending_header_change());
        let idx = match &recorder.phase {
            RecordPhase::Recording(s) => s.index,
            _ => panic!("Not recording"),
        };
        assert_eq!(idx, 1);

        // 4. Send the next keyframe, it should rotate!
        recorder.push_chunk(&write_tag(keyframe)).await.unwrap();

        let idx = match &recorder.phase {
            RecordPhase::Recording(s) => s.index,
            _ => panic!("Not recording"),
        };
        assert_eq!(idx, 2);
        assert!(!recorder.normalizer.has_pending_header_change());
        assert_eq!(
            recorder.normalizer.avc_seq_tag.as_ref().unwrap().data,
            new_avc_seq
        );

        recorder.finalize().await.unwrap();

        // Verify rotation event
        let mut segments = 0;
        while let Ok(ev) = rx.try_recv() {
            if let SegmentEvent::Finalized { .. } = ev {
                segments += 1;
            }
        }
        assert_eq!(segments, 2);
    }

    /// If File::create fails while opening a new segment, the segment row
    /// must already be in redb so recovery can see the orphan. After the
    /// failure the row should be flipped to Failed with the IO error.
    #[tokio::test]
    async fn test_flv_recorder_db_record_survives_file_create_failure() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();

        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        // Pre-place a *directory* at the .part path the recorder will try
        // to use for index 1. File::create against a directory returns
        // EISDIR (or equivalent) — exactly the post-DB-write failure we
        // want to exercise.
        let blocking_path = part_path(&policy.layout, &session_id, 1);
        std::fs::create_dir(&blocking_path).unwrap();

        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx, 1)
            .await
            .unwrap();

        // Stream enough to drive WaitSync → open_new_segment.
        let header_bytes = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00";
        recorder.push_chunk(header_bytes).await.unwrap();

        let write_tag = |tag: FlvTag| {
            let mut b = Vec::new();
            tag.write(&mut b).unwrap();
            b
        };

        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::ScriptData,
                    data_size: 5,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![0, 1, 2, 3, 4],
            }))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Video,
                    data_size: 5,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![(1 << 4) | 7, 0, 0, 0, 0],
            }))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Audio,
                    data_size: 2,
                    timestamp: 0,
                    stream_id: 0,
                },
                data: vec![0xAF, 0],
            }))
            .await
            .unwrap();

        // Keyframe → triggers open_new_segment → File::create on the
        // blocking directory → Err.
        let keyframe = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![(1 << 4) | 7, 1, 0, 0, 0],
        };
        let err = recorder
            .push_chunk(&write_tag(keyframe))
            .await
            .expect_err("File::create should fail against a directory");
        assert!(matches!(err, AppError::Io { .. }));

        // The orphan must be visible in redb: segment 1, Failed status,
        // with the IO error captured.
        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].index, 1);
        assert_eq!(segments[0].status, SegmentStatus::Failed);
        assert!(segments[0].error.is_some());
    }

    #[tokio::test]
    async fn test_flv_recorder_marks_failed_when_initial_header_write_fails() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();

        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy, &store, tx, 1)
            .await
            .unwrap();

        recorder.header = Some(FlvHeader {
            has_video: true,
            has_audio: true,
        });
        recorder.normalizer.metadata_tag = Some(FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::ScriptData,
                data_size: 2,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![0],
        });
        recorder.normalizer.avc_seq_tag = Some(FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![(1 << 4) | 7, 0, 0, 0],
        });
        recorder.normalizer.aac_seq_tag = Some(FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Audio,
                data_size: 2,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![0xAF, 0],
        });

        let err = recorder
            .open_new_segment()
            .await
            .expect_err("AVC sequence size mismatch should fail initial header writes");
        assert!(matches!(err, AppError::Bilibili(_)));

        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].index, 1);
        assert_eq!(segments[0].status, SegmentStatus::Failed);
        assert!(
            segments[0]
                .error
                .as_deref()
                .is_some_and(|error| error.contains("data size mismatch"))
        );
    }
}
