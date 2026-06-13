pub mod flv;
mod flv_metadata;
mod flv_pipeline;
pub mod segment;

use crate::error::{AppError, AppResult};
use crate::recorder::flv::{FlvHeader, FlvTag, FlvTagHeader, read_previous_tag_size};
use crate::recorder::flv_metadata::{KeyframeIndex, build_metadata_body};
use crate::recorder::flv_pipeline::{
    FlvNormalizer, MediaGroupBuffer, MediaGroupFlush, NormalizedAction,
};
use crate::recorder::segment::{
    RecorderPolicy, SegmentEvent, final_path, part_path, should_filter_by_size,
    should_rotate_by_elapsed, should_rotate_by_size,
};
use crate::state::model::{Segment, SegmentCloseReason, SegmentRotationTrigger, SegmentStatus};
use crate::state::store::StateStore;

use reqwest::Response;
use std::path::PathBuf;
use std::time::Instant;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Debug)]
struct ActiveSegment {
    writer: BufWriter<tokio::fs::File>,
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

/// Controls how duplicate media groups are handled during a flush.
///
/// During normal streaming, hitting the duplicate-threshold is a hard
/// error — the stream is stuck and must reconnect. During segment finalization
/// the same condition is benign: the segment is being closed regardless, so the
/// duplicate group is simply dropped without triggering a reconnect.
#[derive(Debug, Clone, Copy)]
enum PendingMediaFlush {
    /// Stream is live; threshold-exceeded duplicates trigger reconnect.
    DuringStream,
    /// Segment is finalizing; threshold-exceeded duplicates are silently dropped.
    FinalizeSegment,
}

struct InitialSegmentWrite {
    size: u64,
    metadata_source: Vec<u8>,
    metadata_len: usize,
}

const METADATA_BODY_OFFSET: u64 = 13 + 11;
const KEYFRAME_INDEX_MIN_INTERVAL_MS: u32 = 1_900;
const STREAM_IDLE_TIMEOUT_SECS: u64 = 30;
const SEGMENT_WRITE_BUFFER_CAPACITY: usize = 128 * 1024;
const TAG_WRITE_BUFFER_INITIAL_CAPACITY: usize = 16 * 1024;

fn duration_millis_u64(duration: std::time::Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

pub struct FlvRecorder<'a> {
    session_id: Uuid,
    policy: RecorderPolicy,
    store: &'a StateStore,
    event_tx: Option<mpsc::UnboundedSender<SegmentEvent>>,

    buffer: Vec<u8>,
    /// Number of leading bytes in `buffer` already parsed into tags. Advancing
    /// this is O(1); the consumed prefix is compacted away in one drain at the
    /// end of `push_chunk` instead of draining from the front per tag (which is
    /// O(n) each and O(n²) across a chunk).
    read_pos: usize,
    header: Option<FlvHeader>,
    normalizer: FlvNormalizer,
    media_group: MediaGroupBuffer,
    tag_write_buffer: Vec<u8>,

    next_index: u32,
    phase: RecordPhase,
}

impl<'a> FlvRecorder<'a> {
    pub async fn new(
        session_id: Uuid,
        policy: RecorderPolicy,
        store: &'a StateStore,
        event_tx: Option<mpsc::UnboundedSender<SegmentEvent>>,
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
            read_pos: 0,
            header: None,
            normalizer: FlvNormalizer::new(),
            media_group: MediaGroupBuffer::new(),
            tag_write_buffer: Vec::with_capacity(TAG_WRITE_BUFFER_INITIAL_CAPACITY),
            next_index: start_index,
            phase: RecordPhase::WaitSync,
        })
    }

    fn emit_event(&self, event: SegmentEvent) {
        if let Some(event_tx) = &self.event_tx {
            let _ = event_tx.send(event);
        }
    }

    pub async fn push_chunk(&mut self, chunk: &[u8]) -> AppResult<()> {
        self.buffer.extend_from_slice(chunk);

        if self.header.is_none() {
            if self.buffer.len() - self.read_pos >= 13 {
                let mut cursor = std::io::Cursor::new(&self.buffer[self.read_pos..]);
                let h = FlvHeader::read(&mut cursor)
                    .map_err(|e| AppError::StreamProtocol(format!("FLV header error: {}", e)))?;
                let prev_size = read_previous_tag_size(&mut cursor).map_err(|e| {
                    AppError::StreamProtocol(format!("FLV prev tag size error: {}", e))
                })?;
                if prev_size != 0 {
                    return Err(AppError::StreamProtocol(format!(
                        "Invalid initial previous tag size: {}",
                        prev_size
                    )));
                }
                self.header = Some(h);
                self.read_pos += 13;
            } else {
                return Ok(());
            }
        }

        while self.buffer.len() - self.read_pos >= 11 {
            let mut cursor = std::io::Cursor::new(&self.buffer[self.read_pos..]);
            let tag_header = match FlvTagHeader::read(&mut cursor) {
                Ok(th) => th,
                Err(e) => {
                    return Err(AppError::StreamProtocol(format!(
                        "FLV tag header error: {}",
                        e
                    )));
                }
            };

            let total_needed = 11 + (tag_header.data_size as usize) + 4;
            if self.buffer.len() - self.read_pos < total_needed {
                break;
            }

            cursor.set_position(0);
            let tag = FlvTag::read(&mut cursor)
                .map_err(|e| AppError::StreamProtocol(format!("FLV tag read error: {}", e)))?;
            // The tag is fully parsed; advance past it before any fallible work
            // so all paths (including `continue` and `?`) leave the cursor
            // consistent. The consumed prefix is compacted once below.
            self.read_pos += total_needed;

            if self.normalizer.is_cache_tag(&tag) {
                self.flush_pending_media_group(PendingMediaFlush::DuringStream)
                    .await?;
            }

            let recording = matches!(self.phase, RecordPhase::Recording(_));
            let action = self.normalizer.observe_tag(tag, recording)?;
            let NormalizedAction::Write { tag, is_keyframe } = action else {
                continue;
            };

            let mut allow_keyframe_rotation = true;
            if matches!(self.phase, RecordPhase::WaitSync) {
                if self.normalizer.is_synced() && is_keyframe {
                    self.open_new_segment().await?;
                    allow_keyframe_rotation = false;
                } else {
                    // Drop tags until we sync on a keyframe with all headers.
                    continue;
                }
            }

            if self.normalizer.has_pending_header_change() && !is_keyframe {
                continue;
            }

            self.queue_media_tag(tag, is_keyframe, allow_keyframe_rotation)
                .await?;
        }

        self.compact_buffer();
        Ok(())
    }

    /// Drop the already-parsed prefix of `buffer`, keeping only the bytes of an
    /// incomplete trailing tag. Runs in one O(remaining) move per chunk.
    fn compact_buffer(&mut self) {
        if self.read_pos > 0 {
            self.buffer.drain(..self.read_pos);
            self.read_pos = 0;
        }
    }

    async fn queue_media_tag(
        &mut self,
        tag: FlvTag,
        is_keyframe: bool,
        allow_keyframe_rotation: bool,
    ) -> AppResult<()> {
        if self.media_group.should_start_new_group(&tag, is_keyframe) {
            self.flush_pending_media_group(PendingMediaFlush::DuringStream)
                .await?;
        }

        if is_keyframe && allow_keyframe_rotation {
            self.rotate_before_keyframe_if_needed().await?;
        }

        self.media_group.push(tag);
        Ok(())
    }

    async fn rotate_before_keyframe_if_needed(&mut self) -> AppResult<()> {
        let rotation_reason = if let RecordPhase::Recording(seg) = &self.phase {
            let mut triggers = Vec::new();
            if self.normalizer.has_pending_header_change() {
                triggers.push(SegmentRotationTrigger::HeaderChanged);
            }
            if let Some(limit) = self.policy.segment.segment_size
                && should_rotate_by_size(seg.size, &self.policy.segment)
            {
                triggers.push(SegmentRotationTrigger::SizeLimit {
                    current_size: seg.size,
                    limit,
                });
            }
            if let Some(limit) = self.policy.segment.segment_time {
                let elapsed = seg.start_time.elapsed();
                if should_rotate_by_elapsed(elapsed, &self.policy.segment) {
                    triggers.push(SegmentRotationTrigger::TimeLimit {
                        elapsed_ms: duration_millis_u64(elapsed),
                        limit_ms: duration_millis_u64(limit),
                    });
                }
            }

            if triggers.is_empty() {
                None
            } else {
                Some(SegmentCloseReason::Rotation { triggers })
            }
        } else {
            None
        };

        if let Some(reason) = rotation_reason {
            let reset_deduplicator = matches!(
                &reason,
                SegmentCloseReason::Rotation { triggers }
                    if triggers
                        .iter()
                        .any(|trigger| matches!(trigger, SegmentRotationTrigger::HeaderChanged))
            );
            self.finalize_current_segment(reason).await?;
            if reset_deduplicator {
                self.media_group.reset_deduplicator();
            }
            self.open_new_segment().await?;
        }

        Ok(())
    }

    async fn flush_pending_media_group(&mut self, mode: PendingMediaFlush) -> AppResult<()> {
        match self.media_group.flush() {
            MediaGroupFlush::Empty => return Ok(()),
            MediaGroupFlush::Unique(group) => {
                for tag in group {
                    self.write_media_tag(tag).await?;
                }
            }
            MediaGroupFlush::Duplicate {
                threshold_exceeded: false,
                media_tags,
            } => {
                tracing::warn!(
                    session_id = %self.session_id,
                    media_tags,
                    "dropping duplicated FLV media group"
                );
            }
            MediaGroupFlush::Duplicate {
                threshold_exceeded: true,
                media_tags,
            } => {
                tracing::warn!(
                    session_id = %self.session_id,
                    media_tags,
                    "dropping duplicated FLV media group — reconnect threshold exceeded"
                );
                if matches!(mode, PendingMediaFlush::FinalizeSegment) {
                    return Ok(());
                }
                return Err(AppError::StreamRepeatedData(
                    "received duplicated FLV media groups repeatedly".into(),
                ));
            }
        }

        Ok(())
    }

    async fn write_media_tag(&mut self, mut tag: FlvTag) -> AppResult<()> {
        // Write current tag
        if let RecordPhase::Recording(seg) = &mut self.phase {
            self.normalizer.normalize_media_timestamp(&mut tag);
            self.tag_write_buffer.clear();
            let file_position = seg.size;
            let timestamp = tag.header.timestamp;
            let is_keyframe = crate::recorder::flv::is_avc_keyframe(&tag.data);
            tag.write(&mut self.tag_write_buffer)
                .map_err(|e| AppError::StreamProtocol(format!("FLV tag write error: {}", e)))?;
            seg.writer
                .write_all(&self.tag_write_buffer)
                .await
                .map_err(|e| AppError::Io {
                    path: seg.part_path.clone(),
                    source: e,
                })?;
            seg.size += self.tag_write_buffer.len() as u64;
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

    async fn finalize_current_segment(
        &mut self,
        close_reason: SegmentCloseReason,
    ) -> AppResult<()> {
        let mut seg = match std::mem::replace(&mut self.phase, RecordPhase::WaitSync) {
            RecordPhase::Recording(s) => s,
            RecordPhase::WaitSync => return Ok(()),
        };

        let final_p = final_path(&self.policy.layout, &self.session_id, seg.index);

        if should_filter_by_size(seg.size, &self.policy.filter) {
            if let Err(e) = seg.writer.flush().await.map_err(|e| AppError::Io {
                path: seg.part_path.clone(),
                source: e,
            }) {
                return self.persist_failed_segment(seg.index, seg.part_path.clone(), e);
            }
            drop(seg.writer);

            let db_seg = Segment {
                session_id: self.session_id,
                index: seg.index,
                path: final_p.clone(),
                status: SegmentStatus::Filtered,
                close_reason: Some(close_reason.clone()),
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

            self.emit_event(SegmentEvent::Filtered {
                session_id: self.session_id,
                index: seg.index,
                path: final_p,
                size: seg.size,
                close_reason,
            });
            return Ok(());
        }

        if let Err(e) = rewrite_segment_metadata(&mut seg).await {
            return self.persist_failed_segment(seg.index, seg.part_path.clone(), e);
        }

        if let Err(e) = seg.writer.flush().await.map_err(|e| AppError::Io {
            path: seg.part_path.clone(),
            source: e,
        }) {
            return self.persist_failed_segment(seg.index, seg.part_path.clone(), e);
        }
        drop(seg.writer);

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
            close_reason: Some(close_reason.clone()),
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

        self.emit_event(SegmentEvent::Finalized {
            session_id: self.session_id,
            index: seg.index,
            path: final_p,
            size: seg.size,
            close_reason,
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
            close_reason: None,
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
            close_reason: None,
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

        self.emit_event(SegmentEvent::Started {
            session_id: self.session_id,
            index: idx,
            part_path: p_path.clone(),
        });

        self.normalizer.start_new_file();

        let writer = BufWriter::with_capacity(SEGMENT_WRITE_BUFFER_CAPACITY, file);

        self.phase = RecordPhase::Recording(Box::new(ActiveSegment {
            writer,
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
            .ok_or_else(|| AppError::State("FLV header missing when opening segment".into()))?;
        h.write(&mut buf)
            .map_err(|e| AppError::StreamProtocol(format!("FLV header write error: {}", e)))?;
        crate::recorder::flv::write_previous_tag_size(&mut buf, 0).map_err(|e| {
            AppError::StreamProtocol(format!("FLV prev tag size write error: {}", e))
        })?;
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
            .ok_or_else(|| AppError::State("Metadata missing when opening segment".into()))?;
        let mut meta = meta.clone();
        meta.header.timestamp = 0;
        meta.data = build_metadata_body(&meta.data, 0, &[]);
        meta.header.data_size = meta.data.len() as u32;
        let metadata_source = meta.data.clone();
        let metadata_len = meta.data.len();
        meta.write(&mut buf)
            .map_err(|e| AppError::StreamProtocol(format!("FLV metadata write error: {}", e)))?;

        let avc = self.normalizer.avc_seq_tag.as_ref().ok_or_else(|| {
            AppError::State("AVC sequence header missing when opening segment".into())
        })?;
        let mut avc = avc.clone();
        avc.header.timestamp = 0;
        avc.write(&mut buf)
            .map_err(|e| AppError::StreamProtocol(format!("FLV AVC seq write error: {}", e)))?;

        let aac = self.normalizer.aac_seq_tag.as_ref().ok_or_else(|| {
            AppError::State("AAC sequence header missing when opening segment".into())
        })?;
        let mut aac = aac.clone();
        aac.header.timestamp = 0;
        aac.write(&mut buf)
            .map_err(|e| AppError::StreamProtocol(format!("FLV AAC seq write error: {}", e)))?;

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

    pub async fn finalize(&mut self, close_reason: SegmentCloseReason) -> AppResult<()> {
        // If `push_chunk` returned mid-loop (e.g. repeated-data break), the
        // parsed prefix may not have been compacted yet. Drop it first so the
        // warning reflects only the incomplete trailing tag.
        self.compact_buffer();
        if !self.buffer.is_empty() {
            tracing::warn!(
                leftover_bytes = self.buffer.len(),
                "discarding incomplete trailing FLV tag before finalizing segment"
            );
            self.buffer.clear();
        }
        self.flush_pending_media_group(PendingMediaFlush::FinalizeSegment)
            .await?;
        self.finalize_current_segment(close_reason).await?;
        Ok(())
    }

    pub async fn mark_failed(&mut self, err_msg: &str) -> AppResult<()> {
        if let RecordPhase::Recording(seg) =
            std::mem::replace(&mut self.phase, RecordPhase::WaitSync)
        {
            let mut seg = seg;
            let error = match seg.writer.flush().await {
                Ok(()) => err_msg.to_string(),
                Err(flush_err) => format!(
                    "{err_msg}; additionally failed to flush buffered segment data: {flush_err}"
                ),
            };
            drop(seg.writer);

            let db_seg = Segment {
                session_id: self.session_id,
                index: seg.index,
                path: seg.part_path.clone(),
                status: SegmentStatus::Failed,
                close_reason: None,
                error: Some(error),
            };
            self.store.put_segment(&db_seg)?;
        }
        Ok(())
    }
}

async fn rewrite_segment_metadata(seg: &mut ActiveSegment) -> AppResult<()> {
    seg.writer.flush().await.map_err(|e| AppError::Io {
        path: seg.part_path.clone(),
        source: e,
    })?;

    let body = build_metadata_body(&seg.metadata_source, seg.duration_ms, &seg.keyframes);
    if body.len() != seg.metadata_len {
        return Err(AppError::StreamProtocol(format!(
            "FLV metadata rewrite size changed: expected {}, got {}",
            seg.metadata_len,
            body.len()
        )));
    }

    seg.writer
        .seek(std::io::SeekFrom::Start(METADATA_BODY_OFFSET))
        .await
        .map_err(|e| AppError::Io {
            path: seg.part_path.clone(),
            source: e,
        })?;
    seg.writer
        .write_all(&body)
        .await
        .map_err(|e| AppError::Io {
            path: seg.part_path.clone(),
            source: e,
        })?;
    seg.writer.flush().await.map_err(|e| AppError::Io {
        path: seg.part_path.clone(),
        source: e,
    })?;
    seg.writer
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
    event_tx: Option<mpsc::UnboundedSender<SegmentEvent>>,
    start_index: u32,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> AppResult<()> {
    if !resp.status().is_success() {
        return Err(AppError::StreamProtocol(format!(
            "Non-success HTTP status: {}",
            resp.status()
        )));
    }

    let mut recorder = FlvRecorder::new(session_id, policy, store, event_tx, start_index).await?;

    let mut graceful_shutdown = false;
    let result: AppResult<()> = async {
        let close_reason = loop {
            tokio::select! {
                chunk_res = tokio::time::timeout(std::time::Duration::from_secs(STREAM_IDLE_TIMEOUT_SECS), resp.chunk()) => {
                    match chunk_res {
                        Ok(Ok(Some(data))) => {
                            if let Err(error) = recorder.push_chunk(&data).await {
                                if matches!(error, AppError::StreamRepeatedData(_)) {
                                    tracing::warn!(
                                        session_id = %session_id,
                                        "stream repeated data: {}",
                                        error
                                    );
                                    break SegmentCloseReason::RepeatedMediaData;
                                }
                                return Err(error);
                            }
                        }
                        Ok(Ok(None)) => {
                            break SegmentCloseReason::StreamEnded;
                        }
                        Ok(Err(e)) => {
                            tracing::warn!(
                                session_id = %session_id,
                                "Stream connection dropped: {}",
                                e
                            );
                            break SegmentCloseReason::ConnectionDropped;
                        }
                        Err(_) => {
                            tracing::warn!(
                                session_id = %session_id,
                                "Stream idle timeout: no data received for {}s",
                                STREAM_IDLE_TIMEOUT_SECS
                            );
                            break SegmentCloseReason::IdleTimeout {
                                seconds: STREAM_IDLE_TIMEOUT_SECS,
                            };
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    tracing::info!(
                        session_id = %session_id,
                        "Graceful shutdown requested, finalizing segment"
                    );
                    graceful_shutdown = true;
                    break SegmentCloseReason::GracefulShutdown;
                }
            }
        };
        recorder.finalize(close_reason).await?;
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
        recorder
            .mark_failed(&err_msg)
            .await
            .map_err(|persist_err| {
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

    fn avc_sequence_header(marker: u8) -> Vec<u8> {
        vec![
            0x17, 0x00, 0, 0, 0, // FLV AVC sequence header prefix
            1, 0x64, 0, 0x1f, 0xff, // AVCDecoderConfigurationRecord, 4-byte NALU lengths
            0xe1, 0, 1, marker, // one-byte SPS placeholder
            1, 0, 1, marker, // one-byte PPS placeholder
        ]
    }

    fn avc_sequence_header_tag(timestamp: u32, marker: u8) -> FlvTag {
        video_tag(timestamp, avc_sequence_header(marker))
    }

    fn avc_keyframe_data(marker: u8) -> Vec<u8> {
        vec![
            0x17, 0x01, 0, 0, 0, // FLV AVC NALU packet prefix
            0, 0, 0, 2, 0x65, marker, // IDR slice
        ]
    }

    fn avc_keyframe_with_parameter_sets_data(marker: u8) -> Vec<u8> {
        vec![
            0x17, 0x01, 0, 0, 0, // FLV AVC NALU packet prefix
            0, 0, 0, 2, 0x67, marker, // SPS
            0, 0, 0, 2, 0x68, marker, // PPS
            0, 0, 0, 1, 0x65, // IDR slice
        ]
    }

    fn avc_interframe_data(marker: u8) -> Vec<u8> {
        vec![
            0x27, 0x01, 0, 0, 0, // FLV AVC NALU packet prefix
            0, 0, 0, 2, 0x41, marker, // non-IDR slice
        ]
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
            .push_chunk(&write_tag(video_tag(0, avc_sequence_header(0))))
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
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, Some(tx), 1)
            .await
            .unwrap();

        // Push valid FLV header + prev tag size (0)
        let header_bytes = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00";
        recorder.push_chunk(header_bytes).await.unwrap();

        // Helper to push an AVC tag
        let push_avc_tag = |is_keyframe: bool| {
            let mut tag_buf = Vec::new();
            let data = if is_keyframe {
                avc_keyframe_data(0)
            } else {
                avc_interframe_data(0)
            };

            let tag = FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Video,
                    data_size: data.len() as u32,
                    timestamp: 0,
                    stream_id: 0,
                },
                data,
            };
            tag.write(&mut tag_buf).unwrap();
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
        let avc_seq_tag = avc_sequence_header_tag(0, 0);
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
        recorder
            .finalize(SegmentCloseReason::StreamEnded)
            .await
            .unwrap();

        // Verify events
        let ev1 = rx.recv().await.unwrap();
        match ev1 {
            SegmentEvent::Started { index, .. } => assert_eq!(index, 1),
            _ => panic!("Expected Started event"),
        }

        let ev2 = rx.recv().await.unwrap();
        match ev2 {
            SegmentEvent::Finalized {
                index,
                size,
                close_reason,
                ..
            } => {
                assert_eq!(index, 1);
                assert!(size > 0);
                assert_eq!(close_reason, SegmentCloseReason::StreamEnded);
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
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, Some(tx), 1)
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
        let avc_seq_tag = avc_sequence_header_tag(0, 0);
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
        let tag = video_tag(0, avc_keyframe_data(0));
        tag.write(&mut tag_buf).unwrap();
        recorder.push_chunk(&tag_buf).await.unwrap();

        recorder
            .finalize(SegmentCloseReason::StreamEnded)
            .await
            .unwrap();

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
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, Some(tx), 1)
            .await
            .unwrap();

        let header_bytes = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00";
        recorder.push_chunk(header_bytes).await.unwrap(); // ~13 bytes

        // Helper to push an AVC tag
        let push_avc_tag = |is_keyframe: bool| {
            let mut tag_buf = Vec::new();
            let data = if is_keyframe {
                avc_keyframe_data(0)
            } else {
                avc_interframe_data(0)
            };

            let tag = FlvTag {
                header: FlvTagHeader {
                    tag_type: FlvTagType::Video,
                    data_size: data.len() as u32,
                    timestamp: 0,
                    stream_id: 0,
                },
                data,
            };
            tag.write(&mut tag_buf).unwrap();
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
        let avc_seq_tag = avc_sequence_header_tag(0, 0);
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

        recorder
            .finalize(SegmentCloseReason::StreamEnded)
            .await
            .unwrap();

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
        assert!(matches!(
            segments[0].close_reason.as_ref(),
            Some(SegmentCloseReason::Rotation { triggers })
                if triggers
                    .iter()
                    .any(|trigger| matches!(trigger, SegmentRotationTrigger::SizeLimit { .. }))
        ));
        assert_eq!(
            segments[1].close_reason,
            Some(SegmentCloseReason::StreamEnded)
        );
    }

    #[tokio::test]
    async fn test_flv_recorder_incomplete_eof() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();

        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy, &store, Some(tx), 1)
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
            .push_chunk(&write_tag(avc_sequence_header_tag(0, 0)))
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
        let valid_tag = video_tag(0, avc_keyframe_data(0));
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

        recorder
            .finalize(SegmentCloseReason::StreamEnded)
            .await
            .unwrap();

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
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, Some(tx), 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        recorder
            .push_chunk(&write_tag(video_tag(1_000, avc_keyframe_data(0))))
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
            .push_chunk(&write_tag(avc_sequence_header_tag(1_020, 0)))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(audio_tag(1_020, vec![0xAF, 0x00])))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(1_033, avc_keyframe_data(0))))
            .await
            .unwrap();

        recorder
            .finalize(SegmentCloseReason::StreamEnded)
            .await
            .unwrap();

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
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, Some(tx), 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        recorder
            .push_chunk(&write_tag(video_tag(10_000, avc_keyframe_data(0))))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(10_033, avc_interframe_data(0))))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(50_000, avc_interframe_data(0))))
            .await
            .unwrap();

        recorder
            .finalize(SegmentCloseReason::StreamEnded)
            .await
            .unwrap();

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
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, Some(tx), 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        let keyframe = video_tag(1_000, avc_keyframe_data(1));
        recorder
            .push_chunk(&write_tag(keyframe.clone()))
            .await
            .unwrap();
        recorder.push_chunk(&write_tag(keyframe)).await.unwrap();

        recorder
            .finalize(SegmentCloseReason::StreamEnded)
            .await
            .unwrap();

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
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, Some(tx), 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        let keyframe = video_tag(1_000, avc_keyframe_data(1));
        for _ in 0..12 {
            recorder
                .push_chunk(&write_tag(keyframe.clone()))
                .await
                .unwrap();
        }

        recorder
            .finalize(SegmentCloseReason::StreamEnded)
            .await
            .unwrap();

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
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, Some(tx), 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        recorder
            .push_chunk(&write_tag(video_tag(1_000, avc_keyframe_data(1))))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(1_033, avc_keyframe_data(1))))
            .await
            .unwrap();

        recorder
            .finalize(SegmentCloseReason::StreamEnded)
            .await
            .unwrap();

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
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, Some(tx), 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        recorder
            .push_chunk(&write_tag(video_tag(1_000, avc_keyframe_data(1))))
            .await
            .unwrap();
        for timestamp in [1_400, 1_800, 2_200, 2_600] {
            recorder
                .push_chunk(&write_tag(video_tag(timestamp, avc_interframe_data(1))))
                .await
                .unwrap();
        }
        recorder
            .push_chunk(&write_tag(video_tag(3_033, avc_keyframe_data(4))))
            .await
            .unwrap();

        recorder
            .finalize(SegmentCloseReason::StreamEnded)
            .await
            .unwrap();

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
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, Some(tx), 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        recorder
            .push_chunk(&write_tag(video_tag(1_000, avc_keyframe_data(0))))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(avc_sequence_header_tag(1_010, 9)))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(audio_tag(1_020, vec![0xAF, 0x01])))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(1_033, avc_interframe_data(0))))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(1_066, avc_keyframe_data(0))))
            .await
            .unwrap();

        recorder
            .finalize(SegmentCloseReason::StreamEnded)
            .await
            .unwrap();

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
    async fn test_flv_recorder_does_not_rotate_on_in_band_parameter_set_refresh() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();
        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, Some(tx), 1)
            .await
            .unwrap();

        start_recording_with_headers(&mut recorder).await;
        recorder
            .push_chunk(&write_tag(video_tag(
                1_000,
                avc_keyframe_with_parameter_sets_data(1),
            )))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(
                1_033,
                avc_keyframe_with_parameter_sets_data(2),
            )))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(avc_sequence_header_tag(1_040, 9)))
            .await
            .unwrap();
        recorder
            .push_chunk(&write_tag(video_tag(1_066, avc_keyframe_data(3))))
            .await
            .unwrap();

        recorder
            .finalize(SegmentCloseReason::StreamEnded)
            .await
            .unwrap();

        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].status, SegmentStatus::Finalized);
    }

    #[tokio::test]
    async fn test_flv_recorder_marks_failed_when_final_rename_fails() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();

        let policy = test_policy(dir.path().to_path_buf(), None, None, 0);

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, Some(tx), 1)
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
            .push_chunk(&write_tag(avc_sequence_header_tag(0, 0)))
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
        let tag = video_tag(0, avc_keyframe_data(0));
        tag.write(&mut tag_buf).unwrap();
        recorder.push_chunk(&tag_buf).await.unwrap();

        let part_p = part_path(&policy.layout, &session_id, 1);
        let final_p = final_path(&policy.layout, &session_id, 1);
        std::fs::create_dir(&final_p).unwrap();

        let err = recorder
            .finalize(SegmentCloseReason::StreamEnded)
            .await
            .unwrap_err();
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
        let mut recorder = FlvRecorder::new(session_id, policy, &store, Some(tx), 1)
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
            let p_frame = video_tag(0, avc_interframe_data(0));
            recorder.push_chunk(&write_tag(p_frame)).await.unwrap();
        }

        // Recorder should still be in WaitSync and no segment created
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
            .push_chunk(&write_tag(avc_sequence_header_tag(0, 0)))
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

        // Push a keyframe, it should transition to Recording and open a segment
        let keyframe = video_tag(0, avc_keyframe_data(0));
        recorder.push_chunk(&write_tag(keyframe)).await.unwrap();

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
        let mut recorder = FlvRecorder::new(session_id, policy, &store, Some(tx), 1)
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
        let initial_avc_seq = avc_sequence_header(0);
        recorder
            .push_chunk(&write_tag(video_tag(0, initial_avc_seq.clone())))
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
        let keyframe = video_tag(0, avc_keyframe_data(0));
        recorder
            .push_chunk(&write_tag(keyframe.clone()))
            .await
            .unwrap();

        assert!(matches!(recorder.phase, RecordPhase::Recording(_)));
        assert!(!recorder.normalizer.has_pending_header_change());

        // 3. Send a new AVC seq with different data
        let new_avc_seq = avc_sequence_header(9);
        recorder
            .push_chunk(&write_tag(video_tag(0, new_avc_seq.clone())))
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

        recorder
            .finalize(SegmentCloseReason::StreamEnded)
            .await
            .unwrap();

        // Verify rotation event
        let mut segments = 0;
        while let Ok(ev) = rx.try_recv() {
            if let SegmentEvent::Finalized { .. } = ev {
                segments += 1;
            }
        }
        assert_eq!(segments, 2);
        let stored = store.list_segments(session_id).unwrap();
        assert!(matches!(
            stored[0].close_reason.as_ref(),
            Some(SegmentCloseReason::Rotation { triggers })
                if triggers
                    .iter()
                    .any(|trigger| matches!(trigger, SegmentRotationTrigger::HeaderChanged))
        ));
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
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, Some(tx), 1)
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
            .push_chunk(&write_tag(avc_sequence_header_tag(0, 0)))
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
        let keyframe = video_tag(0, avc_keyframe_data(0));
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
        let mut recorder = FlvRecorder::new(session_id, policy, &store, Some(tx), 1)
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
        assert!(matches!(err, AppError::StreamProtocol(_)));

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
