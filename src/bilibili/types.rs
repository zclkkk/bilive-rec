use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WbiKeys {
    pub img_key: String,
    pub sub_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NavResponse {
    pub code: i32,
    pub message: String,
    pub data: Option<NavData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NavData {
    #[serde(rename = "isLogin")]
    pub is_login: bool,
    pub uname: Option<String>,
    pub mid: Option<u64>,
    pub wbi_img: Option<WbiImgInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WbiImgInfo {
    pub img_url: String,
    pub sub_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveStatus {
    Offline,
    Live,
    RoundPlay,
    Unknown(i32),
}

impl LiveStatus {
    pub fn from_i32(val: i32) -> Self {
        match val {
            0 => LiveStatus::Offline,
            1 => LiveStatus::Live,
            2 => LiveStatus::RoundPlay,
            other => LiveStatus::Unknown(other),
        }
    }

    pub fn to_i32(self) -> i32 {
        match self {
            LiveStatus::Offline => 0,
            LiveStatus::Live => 1,
            LiveStatus::RoundPlay => 2,
            LiveStatus::Unknown(other) => other,
        }
    }

    pub fn is_live(&self) -> bool {
        matches!(self, LiveStatus::Live)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomInfoResponse {
    pub code: i32,
    pub message: String,
    pub data: Option<RoomInfoData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomInfoData {
    pub room_info: RoomInfoDetail,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomInfoDetail {
    pub room_id: u64,
    #[serde(default)]
    pub short_id: u64,
    pub uid: u64,
    pub live_status: i32,
    pub title: String,
    #[serde(default)]
    pub cover: String,
    #[serde(default)]
    pub live_start_time: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BiliRoomInfo {
    pub room_id: u64,
    pub short_id: u64,
    pub uid: u64,
    pub live_status: LiveStatus,
    pub title: String,
    pub cover_url: String,
    pub live_start_time: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayInfoResponse {
    pub code: i32,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub msg: String,
    pub data: Option<PlayInfoData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayInfoData {
    pub playurl_info: Option<PlayurlInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayurlInfo {
    pub playurl: PlayurlDetail,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayurlDetail {
    pub stream: Vec<StreamInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfo {
    pub protocol_name: String,
    pub format: Vec<FormatInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatInfo {
    pub format_name: String,
    pub codec: Vec<CodecInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodecInfo {
    pub codec_name: String,
    pub current_qn: u32,
    #[serde(default)]
    pub accept_qn: Vec<u32>,
    pub base_url: String,
    pub url_info: Vec<UrlInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlInfo {
    pub host: String,
    pub extra: String,
    #[serde(default)]
    pub stream_ttl: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Flv,
    HlsFmp4,
    HlsTs,
    Unknown,
}

impl Protocol {
    pub fn from_api_name(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "flv" => Protocol::Flv,
            "fmp4" => Protocol::HlsFmp4,
            "ts" => Protocol::HlsTs,
            _ => Protocol::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Protocol::Flv => "flv",
            Protocol::HlsFmp4 => "hls_fmp4",
            Protocol::HlsTs => "hls_ts",
            Protocol::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    Avc,
    Hevc,
    Av1,
    Unknown,
}

impl Codec {
    pub fn from_api_name(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "avc" => Codec::Avc,
            "hevc" => Codec::Hevc,
            "av1" => Codec::Av1,
            _ => Codec::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Codec::Avc => "avc",
            Codec::Hevc => "hevc",
            Codec::Av1 => "av1",
            Codec::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamCandidate {
    pub url: String,
    pub protocol: Protocol,
    pub format: String,
    pub codec: Codec,
    pub qn: u32,
    pub cdn_name: String,
    pub host: String,
}
