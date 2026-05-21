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
