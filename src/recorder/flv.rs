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

pub fn is_avc_sequence_header(data: &[u8]) -> bool {
    if data.len() < 2 {
        return false;
    }
    let codec_id = data[0] & 0x0F;
    let packet_type = data[1];
    codec_id == 7 && packet_type == 0
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
