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
