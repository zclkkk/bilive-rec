use std::collections::HashMap;

use crate::bilibili::client::BiliClient;
use crate::bilibili::types::{BiliRoomInfo, LiveStatus, RoomInfoResponse};
use crate::bilibili::wbi::{mix_wbi_keys, sign_wbi_query};
use crate::error::{AppError, AppResult};

/// Extracts the numerical room ID from a raw input string or Bilibili URL.
///
/// Supported formats:
/// - Pure digits (e.g., "123456")
/// - Standard live URLs (e.g., "https://live.bilibili.com/123456")
/// - Mobile live URLs (e.g., "https://live.bilibili.com/h5/123456")
/// - Blanc live URLs (e.g., "https://live.bilibili.com/blanc/123456")
pub fn extract_room_id(input: &str) -> Option<u64> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }

    // 1. If it's a pure numeric room ID, parse it directly
    if input.chars().all(|c| c.is_ascii_digit()) {
        return input.parse::<u64>().ok();
    }

    // 2. Try parsing as a URL
    let url_str = if input.starts_with("http://") || input.starts_with("https://") {
        input.to_string()
    } else {
        format!("https://{}", input)
    };

    let parsed = reqwest::Url::parse(&url_str).ok()?;
    let host = parsed.host_str()?;

    if host != "live.bilibili.com" {
        return None;
    }

    // Get the last non-empty path segment
    let segments = parsed.path_segments()?;
    let last_segment = segments.rev().find(|s| !s.is_empty())?;

    last_segment.parse::<u64>().ok()
}

/// Checks if the input URL belongs to b23.tv shortlinks.
pub fn is_b23_url(input: &str) -> bool {
    let url_str = if input.starts_with("http://") || input.starts_with("https://") {
        input.to_string()
    } else {
        format!("https://{}", input)
    };
    if let Ok(url) = reqwest::Url::parse(&url_str) {
        return url
            .host_str()
            .is_some_and(|host| host == "b23.tv" || host.ends_with(".b23.tv"));
    }
    false
}

/// Resolves a room ID from an input string, following b23.tv redirects if necessary.
pub async fn resolve_room_id(_client: &BiliClient, input: &str) -> AppResult<u64> {
    if let Some(id) = extract_room_id(input) {
        return Ok(id);
    }

    if is_b23_url(input) {
        let url_str = if input.starts_with("http://") || input.starts_with("https://") {
            input.to_string()
        } else {
            format!("https://{}", input)
        };

        // We use a local transient client to avoid altering the global BiliClient's behavior.
        // It is configured to follow up to 5 redirects automatically.
        let transient_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()
            .map_err(|e| {
                AppError::Bilibili(format!(
                    "Failed to build transient client for b23.tv: {}",
                    e
                ))
            })?;

        let resp =
            transient_client.get(&url_str).send().await.map_err(|e| {
                AppError::Bilibili(format!("Failed to resolve b23.tv redirect: {}", e))
            })?;

        let final_url = resp.url().as_str();
        if let Some(id) = extract_room_id(final_url) {
            return Ok(id);
        } else {
            return Err(AppError::Bilibili(format!(
                "b23.tv link resolved to non-live URL: {}",
                final_url
            )));
        }
    }

    Err(AppError::Config(format!(
        "Failed to extract room ID from '{}'. Not a valid live.bilibili.com or b23.tv URL.",
        input
    )))
}

/// Fetches room details using `/xlive/web-room/v1/index/getInfoByRoom`.
pub async fn fetch_room_info(client: &BiliClient, room_id: u64) -> AppResult<BiliRoomInfo> {
    let keys = client.fetch_wbi_keys().await?;
    let mixed_key = mix_wbi_keys(&keys.img_key, &keys.sub_key)?;
    let params = build_room_info_params(room_id, &mixed_key, current_unix_timestamp());

    let resp: RoomInfoResponse = client
        .client()
        .get("https://api.live.bilibili.com/xlive/web-room/v1/index/getInfoByRoom")
        .query(&params)
        .header("Referer", "https://live.bilibili.com")
        .send()
        .await?
        .json()
        .await?;

    parse_room_info(&resp)
}

fn current_unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn build_room_info_params(
    room_id: u64,
    mixed_key: &str,
    timestamp: i64,
) -> HashMap<String, String> {
    let mut params = HashMap::new();
    params.insert("room_id".to_string(), room_id.to_string());
    params.insert("web_location".to_string(), "444.8".to_string());
    sign_wbi_query(&params, mixed_key, timestamp)
}

/// Converts a RoomInfoResponse to BiliRoomInfo domain object and handles error cases.
fn parse_room_info(resp: &RoomInfoResponse) -> AppResult<BiliRoomInfo> {
    if resp.code != 0 {
        return Err(AppError::Bilibili(format!(
            "getInfoByRoom API returned code {}: {}",
            resp.code, resp.message
        )));
    }

    let data = resp
        .data
        .as_ref()
        .ok_or_else(|| AppError::Bilibili("getInfoByRoom API returned empty data".to_string()))?;

    let detail = &data.room_info;
    let live_start_time = if detail.live_start_time <= 0 {
        None
    } else {
        Some(detail.live_start_time)
    };

    Ok(BiliRoomInfo {
        room_id: detail.room_id,
        short_id: detail.short_id,
        uid: detail.uid,
        live_status: LiveStatus::from_i32(detail.live_status),
        title: detail.title.clone(),
        cover_url: detail.cover.clone(),
        live_start_time,
        special_type: detail.special_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_room_id() {
        assert_eq!(extract_room_id("123456"), Some(123456));
        assert_eq!(extract_room_id("  123456  "), Some(123456));

        assert_eq!(
            extract_room_id("https://live.bilibili.com/123456"),
            Some(123456)
        );
        assert_eq!(
            extract_room_id("http://live.bilibili.com/123456"),
            Some(123456)
        );
        assert_eq!(extract_room_id("live.bilibili.com/123456"), Some(123456));

        assert_eq!(
            extract_room_id("https://live.bilibili.com/h5/123456"),
            Some(123456)
        );
        assert_eq!(
            extract_room_id("https://live.bilibili.com/blanc/123456?q=1"),
            Some(123456)
        );

        // Invalid formats and non-live.bilibili.com domains
        assert_eq!(extract_room_id("b23.tv/abc"), None);
        assert_eq!(extract_room_id("https://b23.tv/123456"), None);
        assert_eq!(extract_room_id("https://google.com/123456"), None);
        assert_eq!(extract_room_id(""), None);
        assert_eq!(extract_room_id("   "), None);

        assert_eq!(
            extract_room_id("https://www.bilibili.com/video/123456"),
            None
        );
        assert_eq!(extract_room_id("https://notbilibili.com/123456"), None);
        assert_eq!(
            extract_room_id("https://live.bilibili.com.evil.test/123456"),
            None
        );
    }

    #[test]
    fn test_is_b23_url() {
        assert!(is_b23_url("b23.tv/abc"));
        assert!(is_b23_url("https://b23.tv/123456"));
        assert!(is_b23_url("http://b23.tv/xyz"));
        assert!(is_b23_url("https://www.b23.tv/abc"));
        assert!(!is_b23_url("https://live.bilibili.com/123456"));
        assert!(!is_b23_url("google.com/b23.tv"));
        assert!(!is_b23_url("123456"));
    }

    #[test]
    fn test_build_room_info_params_signs_wbi_query() {
        let params = build_room_info_params(123, "ea1db124c0beaec8d8d73b06385d38a0", 114514);

        assert_eq!(params.get("room_id").map(String::as_str), Some("123"));
        assert_eq!(
            params.get("web_location").map(String::as_str),
            Some("444.8")
        );
        assert_eq!(params.get("wts").map(String::as_str), Some("114514"));
        assert_eq!(
            params.get("w_rid").map(String::as_str),
            Some("5f385b31068c44413a179c5334108a07")
        );
    }

    #[test]
    fn test_parse_mocked_room_info_success_live() {
        let json_data = r#"{
            "code": 0,
            "message": "0",
            "data": {
                "room_info": {
                    "room_id": 456,
                    "short_id": 123,
                    "uid": 9999,
                    "live_status": 1,
                    "title": "测试直播间",
                    "cover": "https://example.com/cover.png",
                    "live_start_time": 1716300000,
                    "special_type": 1
                }
            }
        }"#;

        let resp: RoomInfoResponse = serde_json::from_str(json_data).unwrap();
        let info = parse_room_info(&resp).unwrap();

        assert_eq!(info.room_id, 456);
        assert_eq!(info.short_id, 123);
        assert_eq!(info.uid, 9999);
        assert_eq!(info.live_status, LiveStatus::Live);
        assert!(info.live_status.is_live());
        assert_eq!(info.title, "测试直播间");
        assert_eq!(info.cover_url, "https://example.com/cover.png");
        assert_eq!(info.live_start_time, Some(1716300000));
        assert_eq!(info.special_type, 1);
    }

    #[test]
    fn test_parse_mocked_room_info_offline() {
        let json_data = r#"{
            "code": 0,
            "message": "0",
            "data": {
                "room_info": {
                    "room_id": 456,
                    "short_id": 0,
                    "uid": 9999,
                    "live_status": 0,
                    "title": "测试直播间",
                    "cover": "https://example.com/cover.png"
                }
            }
        }"#;

        let resp: RoomInfoResponse = serde_json::from_str(json_data).unwrap();
        let info = parse_room_info(&resp).unwrap();

        assert_eq!(info.room_id, 456);
        assert_eq!(info.short_id, 0);
        assert_eq!(info.live_status, LiveStatus::Offline);
        assert!(!info.live_status.is_live());
        assert_eq!(info.live_start_time, None);
        assert_eq!(info.special_type, 0);
    }

    #[test]
    fn test_parse_mocked_room_info_error() {
        let json_data = r#"{
            "code": -400,
            "message": "房间不存在",
            "data": null
        }"#;

        let resp: RoomInfoResponse = serde_json::from_str(json_data).unwrap();
        let res = parse_room_info(&resp);
        assert!(res.is_err());
        let err_msg = res.unwrap_err().to_string();
        assert!(err_msg.contains("getInfoByRoom API returned code -400"));
    }
}
