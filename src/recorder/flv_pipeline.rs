use crate::error::{AppError, AppResult};
use crate::recorder::flv::{
    FlvTag, FlvTagType, avc_nalu_length_size_from_sequence_header, inspect_avc_nalu_packet,
    is_aac_sequence_header, is_avc_keyframe, is_avc_nalu_packet, is_avc_sequence_header,
    is_avc_video_tag,
};
use std::collections::VecDeque;

const TIMESTAMP_JUMP_THRESHOLD_MS: i64 = 500;
const VIDEO_FRAME_DURATION_FALLBACK_MS: u32 = 33;
const AUDIO_FRAME_DURATION_FALLBACK_MS: u32 = 22;
const DUPLICATE_HISTORY_LIMIT: usize = 16;
const DUPLICATE_RECONNECT_THRESHOLD: u32 = 10;
const IN_BAND_PARAMETER_SET_CONFIRMATIONS: u8 = 2;

#[derive(Debug)]
pub(super) struct FlvNormalizer {
    pub(super) metadata_tag: Option<FlvTag>,
    pub(super) avc_seq_tag: Option<FlvTag>,
    pub(super) aac_seq_tag: Option<FlvTag>,
    avc_nalu_length_size: Option<usize>,
    in_band_parameter_sets_seen: u8,
    avc_in_band_parameter_sets: bool,
    pending_header_change: bool,
    timestamp: TimestampNormalizer,
}

#[derive(Debug)]
pub(super) enum NormalizedAction {
    Drop,
    Write { tag: FlvTag, is_keyframe: bool },
}

impl FlvNormalizer {
    pub(super) fn new() -> Self {
        Self {
            metadata_tag: None,
            avc_seq_tag: None,
            aac_seq_tag: None,
            avc_nalu_length_size: None,
            in_band_parameter_sets_seen: 0,
            avc_in_band_parameter_sets: false,
            pending_header_change: false,
            timestamp: TimestampNormalizer::new(),
        }
    }

    pub(super) fn is_synced(&self) -> bool {
        self.metadata_tag.is_some() && self.avc_seq_tag.is_some() && self.aac_seq_tag.is_some()
    }

    pub(super) fn has_pending_header_change(&self) -> bool {
        self.pending_header_change
    }

    pub(super) fn is_cache_tag(&self, tag: &FlvTag) -> bool {
        match tag.header.tag_type {
            FlvTagType::ScriptData => true,
            FlvTagType::Video => is_avc_sequence_header(&tag.data),
            FlvTagType::Audio => is_aac_sequence_header(&tag.data),
        }
    }

    pub(super) fn start_new_file(&mut self) {
        self.pending_header_change = false;
        self.timestamp.reset();
    }

    pub(super) fn observe_tag(
        &mut self,
        tag: FlvTag,
        recording: bool,
    ) -> AppResult<NormalizedAction> {
        match tag.header.tag_type {
            FlvTagType::ScriptData => {
                self.metadata_tag = Some(tag);
                Ok(NormalizedAction::Drop)
            }
            FlvTagType::Video => {
                if !is_avc_video_tag(&tag.data) {
                    return Err(AppError::Bilibili(
                        "Unsupported FLV video codec; only AVC is supported".into(),
                    ));
                }

                if is_avc_sequence_header(&tag.data) {
                    let length_size = avc_nalu_length_size_from_sequence_header(&tag.data)
                        .map_err(|err| {
                            AppError::Bilibili(format!("Invalid AVC sequence header: {err}"))
                        })?;
                    let header_changed = recording
                        && self
                            .avc_seq_tag
                            .as_ref()
                            .is_some_and(|old| old.data != tag.data);
                    let length_size_changed = self
                        .avc_nalu_length_size
                        .is_some_and(|old| old != length_size);

                    if header_changed && (!self.avc_in_band_parameter_sets || length_size_changed) {
                        self.pending_header_change = true;
                    } else if header_changed {
                        tracing::warn!(
                            "AVC sequence header changed while keyframes carry in-band SPS/PPS; updating cached header without segment rotation"
                        );
                    }
                    self.avc_nalu_length_size = Some(length_size);
                    self.avc_seq_tag = Some(tag);
                    return Ok(NormalizedAction::Drop);
                }

                let is_keyframe = is_avc_keyframe(&tag.data);
                if let Some(length_size) = self.avc_nalu_length_size
                    && is_avc_nalu_packet(&tag.data)
                {
                    let inspection =
                        inspect_avc_nalu_packet(&tag.data, length_size).map_err(|err| {
                            AppError::Bilibili(format!("Invalid AVC NALU packet: {err}"))
                        })?;
                    if is_keyframe && inspection.has_sps && inspection.has_pps {
                        self.note_in_band_parameter_sets();
                    }
                }

                Ok(NormalizedAction::Write { tag, is_keyframe })
            }
            FlvTagType::Audio => {
                if is_aac_sequence_header(&tag.data) {
                    if recording
                        && self
                            .aac_seq_tag
                            .as_ref()
                            .is_some_and(|old| old.data != tag.data)
                    {
                        self.pending_header_change = true;
                    }
                    self.aac_seq_tag = Some(tag);
                    return Ok(NormalizedAction::Drop);
                }

                Ok(NormalizedAction::Write {
                    tag,
                    is_keyframe: false,
                })
            }
        }
    }

    pub(super) fn normalize_media_timestamp(&mut self, tag: &mut FlvTag) {
        self.timestamp.normalize(tag);
    }

    fn note_in_band_parameter_sets(&mut self) {
        if self.avc_in_band_parameter_sets {
            return;
        }

        self.in_band_parameter_sets_seen = self.in_band_parameter_sets_seen.saturating_add(1);
        if self.in_band_parameter_sets_seen >= IN_BAND_PARAMETER_SET_CONFIRMATIONS {
            self.avc_in_band_parameter_sets = true;
            tracing::warn!(
                "detected repeated AVC keyframes carrying in-band SPS/PPS; sequence header refreshes will not force segment rotation"
            );
        }
    }
}

#[derive(Debug)]
struct TimestampNormalizer {
    initialized: bool,
    current_offset: i64,
    last_original: u32,
    last_output: u32,
}

impl TimestampNormalizer {
    fn new() -> Self {
        Self {
            initialized: false,
            current_offset: 0,
            last_original: 0,
            last_output: 0,
        }
    }

    fn reset(&mut self) {
        *self = Self::new();
    }

    fn normalize(&mut self, tag: &mut FlvTag) {
        let original = tag.header.timestamp;

        if !self.initialized {
            self.initialized = true;
            self.current_offset = i64::from(original);
            self.last_original = original;
            self.last_output = 0;
            tag.header.timestamp = 0;
            return;
        }

        let diff = i64::from(original) - i64::from(self.last_original);
        let fallback_delta = fallback_delta_for_tag(tag);
        let mut output = i64::from(original) - self.current_offset;

        if !(-TIMESTAMP_JUMP_THRESHOLD_MS..=TIMESTAMP_JUMP_THRESHOLD_MS).contains(&diff) {
            let target = self.last_output.saturating_add(fallback_delta);
            self.current_offset = i64::from(original) - i64::from(target);
            output = i64::from(target);
        }

        if output < 0 {
            output = 0;
            self.current_offset = i64::from(original);
        }

        let output = output.min(i64::from(u32::MAX)) as u32;
        tag.header.timestamp = output;
        self.last_original = original;
        self.last_output = self.last_output.max(output);
    }
}

fn fallback_delta_for_tag(tag: &FlvTag) -> u32 {
    match tag.header.tag_type {
        FlvTagType::Audio => AUDIO_FRAME_DURATION_FALLBACK_MS,
        FlvTagType::Video => VIDEO_FRAME_DURATION_FALLBACK_MS,
        FlvTagType::ScriptData => 0,
    }
}

#[derive(Debug)]
pub(super) struct MediaGroupBuffer {
    pending: Vec<FlvTag>,
    deduplicator: MediaGroupDeduplicator,
}

#[derive(Debug)]
pub(super) enum MediaGroupFlush {
    Empty,
    Unique(Vec<FlvTag>),
    Duplicate { reconnect: bool, media_tags: usize },
}

impl MediaGroupBuffer {
    pub(super) fn new() -> Self {
        Self {
            pending: Vec::new(),
            deduplicator: MediaGroupDeduplicator::new(),
        }
    }

    pub(super) fn should_start_new_group(&self, tag: &FlvTag, is_keyframe: bool) -> bool {
        let Some(last) = self.pending.last() else {
            return false;
        };

        if is_keyframe && self.pending.iter().any(is_video_tag) {
            return true;
        }

        let diff = i64::from(tag.header.timestamp) - i64::from(last.header.timestamp);
        !(-24_999..24_999).contains(&diff)
    }

    pub(super) fn push(&mut self, tag: FlvTag) {
        self.pending.push(tag);
    }

    pub(super) fn flush(&mut self) -> MediaGroupFlush {
        if self.pending.is_empty() {
            return MediaGroupFlush::Empty;
        }

        let media_tags = self.pending.len();
        match self.deduplicator.observe(&self.pending) {
            MediaGroupDecision::Unique => {
                MediaGroupFlush::Unique(std::mem::take(&mut self.pending))
            }
            MediaGroupDecision::Duplicate => {
                self.pending.clear();
                MediaGroupFlush::Duplicate {
                    reconnect: false,
                    media_tags,
                }
            }
            MediaGroupDecision::Reconnect => {
                self.pending.clear();
                MediaGroupFlush::Duplicate {
                    reconnect: true,
                    media_tags,
                }
            }
        }
    }

    pub(super) fn reset_deduplicator(&mut self) {
        self.deduplicator.reset();
    }
}

fn is_video_tag(tag: &FlvTag) -> bool {
    tag.header.tag_type == FlvTagType::Video
}

#[derive(Debug)]
struct MediaGroupDeduplicator {
    history: VecDeque<u64>,
    duplicate_run: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MediaGroupDecision {
    Unique,
    Duplicate,
    Reconnect,
}

impl MediaGroupDeduplicator {
    fn new() -> Self {
        Self {
            history: VecDeque::with_capacity(DUPLICATE_HISTORY_LIMIT),
            duplicate_run: 0,
        }
    }

    fn reset(&mut self) {
        self.history.clear();
        self.duplicate_run = 0;
    }

    fn observe<'a, I>(&mut self, tags: I) -> MediaGroupDecision
    where
        I: IntoIterator<Item = &'a FlvTag>,
    {
        let fingerprint = fingerprint_media_group(tags);
        if self.history.contains(&fingerprint) {
            self.duplicate_run = self.duplicate_run.saturating_add(1);
            if self.duplicate_run > DUPLICATE_RECONNECT_THRESHOLD {
                return MediaGroupDecision::Reconnect;
            }
            return MediaGroupDecision::Duplicate;
        }

        self.duplicate_run = 0;
        self.history.push_back(fingerprint);
        while self.history.len() > DUPLICATE_HISTORY_LIMIT {
            self.history.pop_front();
        }
        MediaGroupDecision::Unique
    }
}

fn fingerprint_media_group<'a, I>(tags: I) -> u64
where
    I: IntoIterator<Item = &'a FlvTag>,
{
    let mut hash = StableHasher::new();
    let mut count = 0u64;

    for tag in tags {
        count += 1;
        hash.write_u8(tag.header.tag_type as u8);
        hash.write_u32(tag.header.data_size);
        hash.write_u32(tag.header.timestamp);
        hash.write_bytes(&tag.data);
    }

    hash.write_u64(count);
    hash.finish()
}

struct StableHasher(u64);

impl StableHasher {
    fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    fn write_u8(&mut self, value: u8) {
        self.write_bytes(&[value]);
    }

    fn write_u32(&mut self, value: u32) {
        self.write_bytes(&value.to_be_bytes());
    }

    fn write_u64(&mut self, value: u64) {
        self.write_bytes(&value.to_be_bytes());
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn finish(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recorder::flv::{FlvTagHeader, FlvTagType};

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

    fn avc_keyframe_with_parameter_sets(marker: u8) -> Vec<u8> {
        vec![
            0x17, 0x01, 0, 0, 0, // FLV AVC NALU packet prefix
            0, 0, 0, 2, 0x67, marker, // SPS
            0, 0, 0, 2, 0x68, marker, // PPS
            0, 0, 0, 1, 0x65, // IDR
        ]
    }

    #[test]
    fn same_sequence_header_does_not_mark_pending_change() {
        let mut normalizer = FlvNormalizer::new();
        let seq = video_tag(0, avc_sequence_header(0));

        assert!(matches!(
            normalizer.observe_tag(seq.clone(), false).unwrap(),
            NormalizedAction::Drop
        ));
        assert!(matches!(
            normalizer.observe_tag(seq, true).unwrap(),
            NormalizedAction::Drop
        ));

        assert!(!normalizer.has_pending_header_change());
    }

    #[test]
    fn changed_sequence_header_marks_pending_change() {
        let mut normalizer = FlvNormalizer::new();
        normalizer
            .observe_tag(video_tag(0, avc_sequence_header(0)), false)
            .unwrap();
        normalizer
            .observe_tag(video_tag(0, avc_sequence_header(9)), true)
            .unwrap();

        assert!(normalizer.has_pending_header_change());
    }

    #[test]
    fn in_band_parameter_sets_need_two_keyframe_confirmations() {
        let mut normalizer = FlvNormalizer::new();
        normalizer
            .observe_tag(video_tag(0, avc_sequence_header(0)), false)
            .unwrap();

        normalizer
            .observe_tag(video_tag(33, avc_keyframe_with_parameter_sets(1)), true)
            .unwrap();
        assert!(!normalizer.avc_in_band_parameter_sets);

        normalizer
            .observe_tag(video_tag(66, avc_keyframe_with_parameter_sets(2)), true)
            .unwrap();
        assert!(normalizer.avc_in_band_parameter_sets);
    }

    #[test]
    fn in_band_parameter_sets_suppress_sequence_header_rotation() {
        let mut normalizer = FlvNormalizer::new();
        normalizer
            .observe_tag(video_tag(0, avc_sequence_header(0)), false)
            .unwrap();
        normalizer
            .observe_tag(video_tag(33, avc_keyframe_with_parameter_sets(1)), true)
            .unwrap();
        normalizer
            .observe_tag(video_tag(66, avc_keyframe_with_parameter_sets(2)), true)
            .unwrap();

        normalizer
            .observe_tag(video_tag(70, avc_sequence_header(9)), true)
            .unwrap();

        assert!(!normalizer.has_pending_header_change());
        assert_eq!(
            normalizer.avc_seq_tag.as_ref().unwrap().data,
            avc_sequence_header(9)
        );
    }

    #[test]
    fn in_band_parameter_sets_do_not_suppress_length_size_changes() {
        let mut normalizer = FlvNormalizer::new();
        normalizer
            .observe_tag(video_tag(0, avc_sequence_header(0)), false)
            .unwrap();
        normalizer
            .observe_tag(video_tag(33, avc_keyframe_with_parameter_sets(1)), true)
            .unwrap();
        normalizer
            .observe_tag(video_tag(66, avc_keyframe_with_parameter_sets(2)), true)
            .unwrap();

        let mut changed = avc_sequence_header(9);
        changed[9] = 0xfd; // 2-byte NALU lengths.
        normalizer
            .observe_tag(video_tag(70, changed), true)
            .unwrap();

        assert!(normalizer.has_pending_header_change());
    }

    #[test]
    fn start_code_framing_is_rejected() {
        let mut normalizer = FlvNormalizer::new();
        normalizer
            .observe_tag(video_tag(0, avc_sequence_header(0)), false)
            .unwrap();

        let err = normalizer
            .observe_tag(
                video_tag(
                    33,
                    vec![
                        0x17, 0x01, 0, 0, 0, // FLV AVC NALU packet prefix
                        0, 0, 0, 1, 0x67, 0x64, 0, 0x1f, 0, 0, 0, 1, 0x68, 0xee,
                    ],
                ),
                true,
            )
            .unwrap_err();

        assert!(err.to_string().contains("Annex-B start-code"));
    }

    #[test]
    fn timestamp_jump_is_rebased_to_boring_continuity() {
        let mut normalizer = FlvNormalizer::new();
        let mut first = video_tag(10_000, vec![0x17, 0x01, 0, 0, 0]);
        let mut second = audio_tag(10_021, vec![0xAF, 0x01]);
        let mut jumped = video_tag(50_000, vec![0x27, 0x01, 0, 0, 0]);

        normalizer.normalize_media_timestamp(&mut first);
        normalizer.normalize_media_timestamp(&mut second);
        normalizer.normalize_media_timestamp(&mut jumped);

        assert_eq!(first.header.timestamp, 0);
        assert_eq!(second.header.timestamp, 21);
        assert_eq!(jumped.header.timestamp, 54);
    }

    #[test]
    fn unsupported_video_codec_is_rejected() {
        let mut normalizer = FlvNormalizer::new();
        let err = normalizer
            .observe_tag(video_tag(0, vec![0x1c, 0x01, 0, 0, 0]), false)
            .unwrap_err();

        assert!(err.to_string().contains("Unsupported FLV video codec"));
    }
}
