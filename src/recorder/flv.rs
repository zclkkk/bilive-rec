use std::fmt;
use std::io::{Read, Write};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlvHeader {
    pub has_video: bool,
    pub has_audio: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlvTagType {
    Audio = 8,
    Video = 9,
    ScriptData = 18,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlvTagHeader {
    pub tag_type: FlvTagType,
    pub data_size: u32,
    pub timestamp: u32,
    pub stream_id: u32,
}

impl FlvHeader {
    pub fn read<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut buf = [0u8; 9];
        reader.read_exact(&mut buf)?;
        if &buf[0..3] != b"FLV" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid FLV signature",
            ));
        }
        let version = buf[3];
        if version != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported FLV version: {}", version),
            ));
        }

        let offset = u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]);
        if offset != 9 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported FLV offset: {}", offset),
            ));
        }

        let flags = buf[4];
        Ok(Self {
            has_video: (flags & 1) != 0,
            has_audio: (flags & 4) != 0,
        })
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let mut buf = [0u8; 9];
        buf[0..3].copy_from_slice(b"FLV");
        buf[3] = 1; // Version 1
        if self.has_video {
            buf[4] |= 1;
        }
        if self.has_audio {
            buf[4] |= 4;
        }
        // Offset is always 9
        buf[5..9].copy_from_slice(&9u32.to_be_bytes());
        writer.write_all(&buf)
    }
}

impl FlvTagHeader {
    pub fn read<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut buf = [0u8; 11];
        reader.read_exact(&mut buf)?;

        let tag_type = match buf[0] {
            8 => FlvTagType::Audio,
            9 => FlvTagType::Video,
            18 => FlvTagType::ScriptData,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid FLV tag type",
                ));
            }
        };

        let data_size = u32::from_be_bytes([0, buf[1], buf[2], buf[3]]);
        let timestamp = u32::from_be_bytes([buf[7], buf[4], buf[5], buf[6]]); // Extended timestamp is at buf[7]
        let stream_id = u32::from_be_bytes([0, buf[8], buf[9], buf[10]]);

        Ok(Self {
            tag_type,
            data_size,
            timestamp,
            stream_id,
        })
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        if self.data_size > 0x00FF_FFFF {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "FLV tag data size exceeds 24 bits",
            ));
        }
        if self.stream_id > 0x00FF_FFFF {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "FLV tag stream ID exceeds 24 bits",
            ));
        }

        let mut buf = [0u8; 11];
        buf[0] = self.tag_type as u8;
        let size_bytes = self.data_size.to_be_bytes();
        buf[1..4].copy_from_slice(&size_bytes[1..4]);

        let ts_bytes = self.timestamp.to_be_bytes();
        buf[4..7].copy_from_slice(&ts_bytes[1..4]);
        buf[7] = ts_bytes[0]; // Extended timestamp

        let stream_id_bytes = self.stream_id.to_be_bytes();
        buf[8..11].copy_from_slice(&stream_id_bytes[1..4]);

        writer.write_all(&buf)
    }
}

pub fn read_previous_tag_size<R: Read>(reader: &mut R) -> std::io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

pub fn write_previous_tag_size<W: Write>(writer: &mut W, size: u32) -> std::io::Result<()> {
    writer.write_all(&size.to_be_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlvTag {
    pub header: FlvTagHeader,
    pub data: Vec<u8>,
}

impl FlvTag {
    pub fn read<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let header = FlvTagHeader::read(reader)?;
        let mut data = vec![0u8; header.data_size as usize];
        reader.read_exact(&mut data)?;
        let previous_tag_size = read_previous_tag_size(reader)?;
        let expected_size = 11 + header.data_size;
        if previous_tag_size != expected_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Invalid PreviousTagSize: expected {}, got {}",
                    expected_size, previous_tag_size
                ),
            ));
        }
        Ok(Self { header, data })
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        if self.data.len() as u32 != self.header.data_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "FLV tag data size mismatch",
            ));
        }
        self.header.write(writer)?;
        writer.write_all(&self.data)?;
        let previous_tag_size = 11 + self.header.data_size;
        write_previous_tag_size(writer, previous_tag_size)?;
        Ok(())
    }
}

pub fn is_avc_video_tag(data: &[u8]) -> bool {
    if data.is_empty() {
        return false;
    }
    let codec_id = data[0] & 0x0F;
    codec_id == 7 // AVC
}

pub fn is_avc_keyframe(data: &[u8]) -> bool {
    if data.len() < 2 {
        return false;
    }
    let frame_type = (data[0] >> 4) & 0x0F;
    let codec_id = data[0] & 0x0F;
    let packet_type = data[1];

    frame_type == 1 && codec_id == 7 && packet_type == 1
}

pub fn is_avc_nalu_packet(data: &[u8]) -> bool {
    if data.len() < 2 {
        return false;
    }
    let codec_id = data[0] & 0x0F;
    let packet_type = data[1];
    codec_id == 7 && packet_type == 1
}

pub fn is_avc_sequence_header(data: &[u8]) -> bool {
    if data.len() < 2 {
        return false;
    }
    let codec_id = data[0] & 0x0F;
    let packet_type = data[1];
    codec_id == 7 && packet_type == 0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AvcNaluInspection {
    pub has_sps: bool,
    pub has_pps: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvcPayloadError {
    NotSequenceHeader,
    SequenceHeaderTooShort,
    InvalidSequenceHeader,
    NotNaluPacket,
    InvalidNaluLengthSize,
    EmptyNaluPayload,
    TruncatedNaluLength,
    ZeroNaluLength,
    TruncatedNalu,
    InvalidNaluHeader,
    UnsupportedAnnexBStartCode,
}

impl fmt::Display for AvcPayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotSequenceHeader => write!(f, "not an AVC sequence header"),
            Self::SequenceHeaderTooShort => write!(f, "AVC sequence header is too short"),
            Self::InvalidSequenceHeader => write!(f, "invalid AVC sequence header"),
            Self::NotNaluPacket => write!(f, "not an AVC NALU packet"),
            Self::InvalidNaluLengthSize => write!(f, "invalid AVC NALU length size"),
            Self::EmptyNaluPayload => write!(f, "empty AVC NALU payload"),
            Self::TruncatedNaluLength => write!(f, "truncated AVC NALU length"),
            Self::ZeroNaluLength => write!(f, "zero-sized AVC NALU"),
            Self::TruncatedNalu => write!(f, "truncated AVC NALU payload"),
            Self::InvalidNaluHeader => write!(f, "invalid H.264 NALU header"),
            Self::UnsupportedAnnexBStartCode => write!(
                f,
                "unsupported H.264 Annex-B start-code framing inside FLV AVC payload"
            ),
        }
    }
}

pub fn avc_nalu_length_size_from_sequence_header(data: &[u8]) -> Result<usize, AvcPayloadError> {
    if !is_avc_sequence_header(data) {
        return Err(AvcPayloadError::NotSequenceHeader);
    }
    if data.len() < 10 {
        return Err(AvcPayloadError::SequenceHeaderTooShort);
    }
    if data[5] != 1 {
        return Err(AvcPayloadError::InvalidSequenceHeader);
    }

    let length_size = usize::from(data[9] & 0x03) + 1;
    if length_size == 3 {
        return Err(AvcPayloadError::InvalidNaluLengthSize);
    }

    Ok(length_size)
}

pub fn inspect_avc_nalu_packet(
    data: &[u8],
    length_size: usize,
) -> Result<AvcNaluInspection, AvcPayloadError> {
    if !is_avc_nalu_packet(data) {
        return Err(AvcPayloadError::NotNaluPacket);
    }
    if !matches!(length_size, 1 | 2 | 4) {
        return Err(AvcPayloadError::InvalidNaluLengthSize);
    }

    let payload = &data[5..];
    let parsed = inspect_length_prefixed_nalus(payload, length_size);
    if parsed.is_err() && starts_with_annex_b_start_code(payload) {
        return Err(AvcPayloadError::UnsupportedAnnexBStartCode);
    }
    parsed
}

fn inspect_length_prefixed_nalus(
    payload: &[u8],
    length_size: usize,
) -> Result<AvcNaluInspection, AvcPayloadError> {
    if payload.is_empty() {
        return Err(AvcPayloadError::EmptyNaluPayload);
    }

    let mut offset = 0;
    let mut inspection = AvcNaluInspection {
        has_sps: false,
        has_pps: false,
    };

    while offset < payload.len() {
        if payload.len() - offset < length_size {
            return Err(AvcPayloadError::TruncatedNaluLength);
        }

        let mut nalu_len = 0usize;
        for byte in &payload[offset..offset + length_size] {
            nalu_len = (nalu_len << 8) | usize::from(*byte);
        }
        offset += length_size;

        if nalu_len == 0 {
            return Err(AvcPayloadError::ZeroNaluLength);
        }
        if payload.len() - offset < nalu_len {
            return Err(AvcPayloadError::TruncatedNalu);
        }

        let first = payload[offset];
        if (first & 0x80) != 0 {
            return Err(AvcPayloadError::InvalidNaluHeader);
        }

        match first & 0x1f {
            7 => inspection.has_sps = true,
            8 => inspection.has_pps = true,
            _ => {}
        }
        offset += nalu_len;
    }

    Ok(inspection)
}

fn starts_with_annex_b_start_code(payload: &[u8]) -> bool {
    payload.starts_with(&[0, 0, 1]) || payload.starts_with(&[0, 0, 0, 1])
}

pub fn is_aac_sequence_header(data: &[u8]) -> bool {
    if data.len() < 2 {
        return false;
    }
    let sound_format = (data[0] >> 4) & 0x0F;
    let packet_type = data[1];
    sound_format == 10 && packet_type == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flv_header_read_write() {
        let header = FlvHeader {
            has_video: true,
            has_audio: true,
        };
        let mut buf = Vec::new();
        header.write(&mut buf).unwrap();
        assert_eq!(buf.len(), 9);

        let read_header = FlvHeader::read(&mut buf.as_slice()).unwrap();
        assert_eq!(read_header, header);
    }

    #[test]
    fn test_flv_header_read_validation() {
        // Invalid signature
        let mut slice = &b"FLX\x01\x05\x00\x00\x00\x09"[..];
        assert!(FlvHeader::read(&mut slice).is_err());

        // Invalid version
        let mut slice = &b"FLV\x02\x05\x00\x00\x00\x09"[..];
        assert!(FlvHeader::read(&mut slice).is_err());

        // Invalid offset
        let mut slice = &b"FLV\x01\x05\x00\x00\x00\x0A"[..];
        assert!(FlvHeader::read(&mut slice).is_err());
    }

    #[test]
    fn test_flv_tag_header_write_validation() {
        let mut buf = Vec::new();

        let tag_header_size = FlvTagHeader {
            tag_type: FlvTagType::Video,
            data_size: 0x01_00_00_00, // 24 bits max
            timestamp: 0,
            stream_id: 0,
        };
        assert!(tag_header_size.write(&mut buf).is_err());

        let tag_header_stream = FlvTagHeader {
            tag_type: FlvTagType::Video,
            data_size: 100,
            timestamp: 0,
            stream_id: 0x01_00_00_00, // 24 bits max
        };
        assert!(tag_header_stream.write(&mut buf).is_err());
    }

    #[test]
    fn test_flv_tag_roundtrip() {
        let tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: 5,
                timestamp: 12345,
                stream_id: 0,
            },
            data: vec![0x17, 0x01, 0x00, 0x00, 0x00],
        };

        let mut buf = Vec::new();
        tag.write(&mut buf).unwrap();

        // 11 bytes header + 5 bytes data + 4 bytes previous tag size = 20 bytes
        assert_eq!(buf.len(), 20);

        let mut slice = buf.as_slice();
        let read_tag = FlvTag::read(&mut slice).unwrap();
        assert_eq!(read_tag, tag);
    }

    #[test]
    fn test_flv_tag_read_invalid_previous_tag_size() {
        let tag = FlvTag {
            header: FlvTagHeader {
                tag_type: FlvTagType::Video,
                data_size: 5,
                timestamp: 12345,
                stream_id: 0,
            },
            data: vec![0x17, 0x01, 0x00, 0x00, 0x00],
        };

        let mut buf = Vec::new();
        tag.write(&mut buf).unwrap();

        // Corrupt the PreviousTagSize (last 4 bytes)
        let len = buf.len();
        buf[len - 1] = 0xFF; // Was 16 (11 + 5), now wrong

        let mut slice = buf.as_slice();
        assert!(FlvTag::read(&mut slice).is_err());
    }

    #[test]
    fn test_flv_tag_header_read_write() {
        let tag_header = FlvTagHeader {
            tag_type: FlvTagType::Video,
            data_size: 1024,
            timestamp: 0x01_02_03_04, // Using extended timestamp
            stream_id: 0,
        };
        let mut buf = Vec::new();
        tag_header.write(&mut buf).unwrap();
        assert_eq!(buf.len(), 11);

        // Check timestamp byte order in buf
        // buf[4..7] should be 02_03_04
        // buf[7] should be 01
        assert_eq!(buf[4], 0x02);
        assert_eq!(buf[5], 0x03);
        assert_eq!(buf[6], 0x04);
        assert_eq!(buf[7], 0x01);

        let read_tag_header = FlvTagHeader::read(&mut buf.as_slice()).unwrap();
        assert_eq!(read_tag_header, tag_header);
    }

    #[test]
    fn test_avc_keyframe() {
        // FrameType: 1 (Keyframe) << 4 | CodecID: 7 (AVC) -> 0x17
        // AVCPacketType: 1 (NALU) -> 0x01
        let data = [0x17, 0x01, 0x00, 0x00, 0x00];
        assert!(is_avc_keyframe(&data));
        assert!(is_avc_video_tag(&data));
        assert!(!is_avc_sequence_header(&data));

        // AVCPacketType: 0 (Sequence header) -> 0x00
        let data_seq = [0x17, 0x00, 0x00, 0x00, 0x00];
        assert!(!is_avc_keyframe(&data_seq));
        assert!(is_avc_video_tag(&data_seq));
        assert!(is_avc_sequence_header(&data_seq));

        // FrameType: 2 (Interframe) << 4 | CodecID: 7 (AVC) -> 0x27
        // AVCPacketType: 1 (NALU) -> 0x01
        let data_inter = [0x27, 0x01, 0x00, 0x00, 0x00];
        assert!(!is_avc_keyframe(&data_inter));
        assert!(is_avc_video_tag(&data_inter));
        assert!(!is_avc_sequence_header(&data_inter));
    }

    #[test]
    fn avc_sequence_header_exposes_nalu_length_size() {
        let mut data = vec![0x17, 0x00, 0, 0, 0, 1, 0x64, 0, 0x1f, 0xff];
        assert_eq!(avc_nalu_length_size_from_sequence_header(&data), Ok(4));

        data[9] = 0xfc;
        assert_eq!(avc_nalu_length_size_from_sequence_header(&data), Ok(1));

        data[9] = 0xfe;
        assert_eq!(
            avc_nalu_length_size_from_sequence_header(&data),
            Err(AvcPayloadError::InvalidNaluLengthSize)
        );

        data[5] = 0;
        assert_eq!(
            avc_nalu_length_size_from_sequence_header(&data),
            Err(AvcPayloadError::InvalidSequenceHeader)
        );

        let short = [0x17, 0x00, 0, 0, 0];
        assert_eq!(
            avc_nalu_length_size_from_sequence_header(&short),
            Err(AvcPayloadError::SequenceHeaderTooShort)
        );
    }

    #[test]
    fn avc_nalu_packet_inspection_detects_sps_and_pps() {
        let data = [
            0x17, 0x01, 0, 0, 0, // FLV AVC video packet prefix
            0, 0, 0, 2, 0x67, 0x64, // SPS
            0, 0, 0, 2, 0x68, 0xee, // PPS
            0, 0, 0, 1, 0x65, // IDR
        ];

        let inspection = inspect_avc_nalu_packet(&data, 4).unwrap();
        assert!(inspection.has_sps);
        assert!(inspection.has_pps);
    }

    #[test]
    fn avc_nalu_packet_rejects_malformed_lengths() {
        let data = [
            0x17, 0x01, 0, 0, 0, // FLV AVC video packet prefix
            0, 0, 0, 8, 0x65, 0x88,
        ];

        assert_eq!(
            inspect_avc_nalu_packet(&data, 4),
            Err(AvcPayloadError::TruncatedNalu)
        );
    }

    #[test]
    fn avc_nalu_packet_rejects_start_code_framing() {
        let data = [
            0x17, 0x01, 0, 0, 0, // FLV AVC video packet prefix
            0, 0, 0, 1, 0x67, 0x64, 0, 0x1f, 0, 0, 0, 1, 0x68, 0xee,
        ];

        assert_eq!(
            inspect_avc_nalu_packet(&data, 4),
            Err(AvcPayloadError::UnsupportedAnnexBStartCode)
        );
    }

    #[test]
    fn test_aac_sequence_header() {
        // SoundFormat: 10 (AAC) << 4 | SoundRate: 3 (44kHz) << 2 | SoundSize: 1 (16bit) << 1 | SoundType: 1 (Stereo) -> 0xAF
        // AACPacketType: 0 (Sequence header) -> 0x00
        let data_seq = [0xAF, 0x00];
        assert!(is_aac_sequence_header(&data_seq));

        // AACPacketType: 1 (Raw) -> 0x01
        let data_raw = [0xAF, 0x01];
        assert!(!is_aac_sequence_header(&data_raw));
    }
}
