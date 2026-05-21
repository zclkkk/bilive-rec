pub mod flv;
pub mod segment;

use crate::error::{AppError, AppResult};
use crate::recorder::flv::{
    FlvHeader, FlvTag, FlvTagHeader, FlvTagType, is_aac_sequence_header, is_avc_keyframe,
    is_avc_sequence_header, read_previous_tag_size,
};
use crate::recorder::segment::{
    SegmentEvent, SegmentPolicy, final_path, part_path, should_filter_by_size,
    should_rotate_by_elapsed, should_rotate_by_size,
};
use crate::state::model::{Segment, SegmentStatus};
use crate::state::store::StateStore;

use reqwest::Response;
use std::path::PathBuf;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use uuid::Uuid;

struct ActiveSegment {
    file: tokio::fs::File,
    index: u32,
    part_path: std::path::PathBuf,
    size: u64,
    start_time: Instant,
}

pub struct FlvRecorder<'a> {
    session_id: Uuid,
    policy: SegmentPolicy,
    store: &'a StateStore,
    event_tx: mpsc::UnboundedSender<SegmentEvent>,

    buffer: Vec<u8>,
    header: Option<FlvHeader>,
    metadata_tag: Option<FlvTag>,
    avc_seq_tag: Option<FlvTag>,
    aac_seq_tag: Option<FlvTag>,

    current_segment: Option<ActiveSegment>,
    next_index: u32,
    is_first_segment: bool,
}

impl<'a> FlvRecorder<'a> {
    pub async fn new(
        session_id: Uuid,
        policy: SegmentPolicy,
        store: &'a StateStore,
        event_tx: mpsc::UnboundedSender<SegmentEvent>,
    ) -> AppResult<Self> {
        // Ensure output dir exists
        tokio::fs::create_dir_all(&policy.output_dir)
            .await
            .map_err(|e| AppError::Io {
                path: policy.output_dir.clone(),
                source: e,
            })?;

        Ok(Self {
            session_id,
            policy,
            store,
            event_tx,
            buffer: Vec::new(),
            header: None,
            metadata_tag: None,
            avc_seq_tag: None,
            aac_seq_tag: None,
            current_segment: None,
            next_index: 1,
            is_first_segment: true,
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

            let is_script = tag.header.tag_type == FlvTagType::ScriptData;
            let is_avc_seq =
                tag.header.tag_type == FlvTagType::Video && is_avc_sequence_header(&tag.data);
            let is_aac_seq =
                tag.header.tag_type == FlvTagType::Audio && is_aac_sequence_header(&tag.data);

            if is_script {
                self.metadata_tag = Some(tag.clone());
            } else if is_avc_seq {
                self.avc_seq_tag = Some(tag.clone());
            } else if is_aac_seq {
                self.aac_seq_tag = Some(tag.clone());
            }

            let is_keyframe =
                tag.header.tag_type == FlvTagType::Video && is_avc_keyframe(&tag.data);

            let mut needs_rotation = false;
            if let Some(seg) = &self.current_segment {
                if is_keyframe
                    && (should_rotate_by_size(seg.size, &self.policy)
                        || should_rotate_by_elapsed(seg.start_time.elapsed(), &self.policy))
                {
                    needs_rotation = true;
                }
            } else {
                // No active segment, we need to open one
                needs_rotation = true;
            }

            if needs_rotation {
                self.finalize_current_segment().await?;
                self.open_new_segment().await?;
            }

            // Write current tag
            if let Some(seg) = &mut self.current_segment {
                let mut buf = Vec::new();
                tag.write(&mut buf)
                    .map_err(|e| AppError::Bilibili(format!("FLV tag write error: {}", e)))?;
                seg.file.write_all(&buf).await.map_err(|e| AppError::Io {
                    path: seg.part_path.clone(),
                    source: e,
                })?;
                seg.size += buf.len() as u64;
            }

            self.buffer.drain(..total_needed);
        }

        Ok(())
    }

    async fn finalize_current_segment(&mut self) -> AppResult<()> {
        let mut seg = match self.current_segment.take() {
            Some(s) => s,
            None => return Ok(()),
        };

        if let Err(e) = seg.file.flush().await.map_err(|e| AppError::Io {
            path: seg.part_path.clone(),
            source: e,
        }) {
            return self.persist_failed_segment(seg.index, seg.part_path.clone(), e);
        }
        drop(seg.file);

        let final_p = final_path(&self.policy, &self.session_id, seg.index);

        if should_filter_by_size(seg.size, &self.policy) {
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
        self.next_index += 1;
        let p_path = part_path(&self.policy, &self.session_id, idx);
        let mut file = tokio::fs::File::create(&p_path)
            .await
            .map_err(|e| AppError::Io {
                path: p_path.clone(),
                source: e,
            })?;

        let db_seg = Segment {
            session_id: self.session_id,
            index: idx,
            path: p_path.clone(),
            status: SegmentStatus::Recording,
            error: None,
        };
        self.store.put_segment(&db_seg)?;

        let _ = self.event_tx.send(SegmentEvent::Started {
            session_id: self.session_id,
            index: idx,
            part_path: p_path.clone(),
        });

        let mut size = 0;
        let mut buf = Vec::new();

        if let Some(h) = &self.header {
            h.write(&mut buf)
                .map_err(|e| AppError::Bilibili(format!("FLV header write error: {}", e)))?;
            crate::recorder::flv::write_previous_tag_size(&mut buf, 0)
                .map_err(|e| AppError::Bilibili(format!("FLV prev tag size write error: {}", e)))?;
            file.write_all(&buf).await.map_err(|e| AppError::Io {
                path: p_path.clone(),
                source: e,
            })?;
            size += buf.len() as u64;
            buf.clear();
        }

        // Only inject cached headers if this is NOT the very first segment.
        // For the first segment, the headers are already in the incoming stream and will be written normally.
        if !self.is_first_segment {
            if let Some(t) = &self.metadata_tag {
                t.write(&mut buf)
                    .map_err(|e| AppError::Bilibili(format!("FLV metadata write error: {}", e)))?;
            }
            if let Some(t) = &self.avc_seq_tag {
                t.write(&mut buf)
                    .map_err(|e| AppError::Bilibili(format!("FLV AVC seq write error: {}", e)))?;
            }
            if let Some(t) = &self.aac_seq_tag {
                t.write(&mut buf)
                    .map_err(|e| AppError::Bilibili(format!("FLV AAC seq write error: {}", e)))?;
            }
            if !buf.is_empty() {
                file.write_all(&buf).await.map_err(|e| AppError::Io {
                    path: p_path.clone(),
                    source: e,
                })?;
                size += buf.len() as u64;
                buf.clear();
            }
        }

        self.is_first_segment = false;

        self.current_segment = Some(ActiveSegment {
            file,
            index: idx,
            part_path: p_path,
            size,
            start_time: Instant::now(),
        });

        Ok(())
    }

    pub async fn finalize(&mut self) -> AppResult<()> {
        if !self.buffer.is_empty() {
            return Err(AppError::Bilibili(format!(
                "Stream ended with {} leftover bytes (incomplete tag)",
                self.buffer.len()
            )));
        }
        self.finalize_current_segment().await?;
        Ok(())
    }

    pub fn mark_failed(&mut self, err_msg: &str) -> AppResult<()> {
        if let Some(seg) = self.current_segment.take() {
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

/// A thin wrapper that reads from an HTTP response and drives the FlvRecorder.
pub async fn record_flv(
    mut resp: Response,
    session_id: Uuid,
    policy: SegmentPolicy,
    store: &StateStore,
    event_tx: mpsc::UnboundedSender<SegmentEvent>,
) -> AppResult<()> {
    if !resp.status().is_success() {
        return Err(AppError::Bilibili(format!(
            "Non-success HTTP status: {}",
            resp.status()
        )));
    }

    let mut recorder = FlvRecorder::new(session_id, policy, store, event_tx).await?;

    let result: AppResult<()> = async {
        while let Some(chunk) = resp.chunk().await? {
            recorder.push_chunk(&chunk).await?;
        }
        recorder.finalize().await?;
        Ok(())
    }
    .await;

    if let Err(e) = result {
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
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_flv_recorder_push_chunk() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();

        let policy = SegmentPolicy {
            output_dir: dir.path().to_path_buf(),
            segment_size: Some(1024),
            segment_time: None,
            min_segment_size: 0,
        };

        let session_id = Uuid::new_v4();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx)
            .await
            .unwrap();

        // Push valid FLV header + prev tag size (0)
        let header_bytes = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00";
        recorder.push_chunk(header_bytes).await.unwrap();

        // Now push a script tag (simulating metadata)
        let mut tag_buf = Vec::new();
        let metadata_tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::ScriptData,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![0, 1, 2, 3, 4],
        };
        metadata_tag.write(&mut tag_buf).unwrap();

        // Push in two pieces to test chunk boundaries
        recorder.push_chunk(&tag_buf[..5]).await.unwrap();
        recorder.push_chunk(&tag_buf[5..]).await.unwrap();

        assert_eq!(recorder.buffer.len(), 0);
        assert!(recorder.current_segment.is_some());
        assert_eq!(recorder.metadata_tag, Some(metadata_tag));

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
        let part_p = part_path(&policy, &session_id, 1);
        let final_p = final_path(&policy, &session_id, 1);
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

        let policy = SegmentPolicy {
            output_dir: dir.path().to_path_buf(),
            segment_size: None,
            segment_time: None,
            min_segment_size: 99999, // very large minimum size
        };

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx)
            .await
            .unwrap();

        // Push valid FLV header + prev tag size (0)
        let header_bytes = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00";
        recorder.push_chunk(header_bytes).await.unwrap();

        // Push a small tag to open a segment
        let mut tag_buf = Vec::new();
        let tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![0, 0, 0, 0, 0],
        };
        tag.write(&mut tag_buf).unwrap();
        recorder.push_chunk(&tag_buf).await.unwrap();

        recorder.finalize().await.unwrap();

        let part_p = part_path(&policy, &session_id, 1);
        let final_p = final_path(&policy, &session_id, 1);
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

        let policy = SegmentPolicy {
            output_dir: dir.path().to_path_buf(),
            segment_size: Some(50), // very small limit to trigger rotation
            segment_time: None,
            min_segment_size: 0,
        };

        let session_id = Uuid::new_v4();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx)
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

        // Tag 1: Keyframe (size 20) -> segment 1 size = 13 + 20 = 33
        recorder.push_chunk(&push_avc_tag(true)).await.unwrap();

        // Tag 2: Interframe (size 20) -> segment 1 size = 33 + 20 = 53
        // Limit is 50. But this is an interframe, so no rotation yet.
        recorder.push_chunk(&push_avc_tag(false)).await.unwrap();

        // Tag 3: Keyframe (size 20). Limit exceeded (53 >= 50) and it's a keyframe!
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

        let policy = SegmentPolicy {
            output_dir: dir.path().to_path_buf(),
            segment_size: None,
            segment_time: None,
            min_segment_size: 0,
        };

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy, &store, tx)
            .await
            .unwrap();

        // Push valid FLV header + prev tag size (0)
        let header_bytes = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00";
        recorder.push_chunk(header_bytes).await.unwrap();

        // Push a complete tag to open a segment
        let mut valid_tag_buf = Vec::new();
        let valid_tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: 5,
                timestamp: 0,
                stream_id: 0,
            },
            data: vec![0, 0, 0, 0, 0],
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

        let err = recorder.finalize().await.unwrap_err();
        assert!(err.to_string().contains("incomplete tag"));

        recorder.mark_failed(&err.to_string()).unwrap();

        let segments = store.list_segments(session_id).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].status, SegmentStatus::Failed);
        assert!(
            segments[0]
                .error
                .as_ref()
                .unwrap()
                .contains("incomplete tag")
        );
    }

    #[tokio::test]
    async fn test_flv_recorder_marks_failed_when_final_rename_fails() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let store = StateStore::open(&db_path).unwrap();

        let policy = SegmentPolicy {
            output_dir: dir.path().to_path_buf(),
            segment_size: None,
            segment_time: None,
            min_segment_size: 0,
        };

        let session_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut recorder = FlvRecorder::new(session_id, policy.clone(), &store, tx)
            .await
            .unwrap();

        let header_bytes = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00";
        recorder.push_chunk(header_bytes).await.unwrap();

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

        let part_p = part_path(&policy, &session_id, 1);
        let final_p = final_path(&policy, &session_id, 1);
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
}
