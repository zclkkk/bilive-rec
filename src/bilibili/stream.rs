use std::collections::HashMap;

use crate::bilibili::cdn::check_stream_health;
use crate::bilibili::client::BiliClient;
use crate::bilibili::types::{Codec, PlayInfoResponse, StreamCandidate};
use crate::bilibili::wbi::{mix_wbi_keys, sign_wbi_query};
use crate::config::ResolvedRecordConfig;
use crate::error::{AppError, AppResult};

/// Fetches play info using `/xlive/web-room/v2/index/getRoomPlayInfo`.
///
/// Uses the WBI signature query signing based on mixed keys and current timestamp.
pub async fn fetch_play_info(
    client: &BiliClient,
    room_id: u64,
    qn: u32,
) -> AppResult<PlayInfoResponse> {
    // 1. Fetch WBI keys
    let keys = client.fetch_wbi_keys().await?;

    // 2. Mix keys
    let mixed_key = mix_wbi_keys(&keys.img_key, &keys.sub_key)?;

    // 3. Prepare parameters
    let params = build_play_info_params(room_id, qn);

    // 4. Sign params with current unix timestamp
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let signed_params = sign_wbi_query(&params, &mixed_key, now_secs);

    // 5. Send GET request and deserialize
    let url = "https://api.live.bilibili.com/xlive/web-room/v2/index/getRoomPlayInfo";
    let resp: PlayInfoResponse = client
        .client()
        .get(url)
        .query(&signed_params)
        .header("Referer", "https://live.bilibili.com")
        .send()
        .await?
        .json()
        .await
        .map_err(|e| {
            AppError::Bilibili(format!("Failed to parse getRoomPlayInfo response: {e}"))
        })?;

    Ok(resp)
}

fn build_play_info_params(room_id: u64, qn: u32) -> HashMap<String, String> {
    let mut params = HashMap::new();
    params.insert("room_id".to_string(), room_id.to_string());
    params.insert("qn".to_string(), qn.to_string());
    params.insert("platform".to_string(), "web".to_string());
    params.insert("protocol".to_string(), "0".to_string());
    params.insert("format".to_string(), "0".to_string());
    params.insert("codec".to_string(), "0".to_string());
    params.insert("ptype".to_string(), "8".to_string());
    params.insert("dolby".to_string(), "5".to_string());
    params.insert("web_location".to_string(), "444.8".to_string());
    params
}

/// Parses the PlayInfoResponse into domain stream candidates.
pub fn parse_stream_candidates(resp: &PlayInfoResponse) -> AppResult<Vec<StreamCandidate>> {
    if resp.code != 0 {
        let msg = if !resp.message.is_empty() {
            &resp.message
        } else if !resp.msg.is_empty() {
            &resp.msg
        } else {
            "Unknown error"
        };
        return Err(AppError::Bilibili(format!(
            "getRoomPlayInfo API returned code {}: {}",
            resp.code, msg
        )));
    }

    let data = resp
        .data
        .as_ref()
        .ok_or_else(|| AppError::Bilibili("getRoomPlayInfo API returned empty data".to_string()))?;

    let playurl_info = data
        .playurl_info
        .as_ref()
        .ok_or_else(|| AppError::Bilibili("playurl_info is missing in response".to_string()))?;

    let mut candidates = Vec::new();

    for stream_info in &playurl_info.playurl.stream {
        for format_info in &stream_info.format {
            if !format_info.format_name.eq_ignore_ascii_case("flv") {
                continue;
            }
            for codec_info in &format_info.codec {
                let codec = Codec::from_api_name(&codec_info.codec_name);
                if !is_supported_codec(codec) {
                    continue;
                }
                for url_info in &codec_info.url_info {
                    let url = format!("{}{}{}", url_info.host, codec_info.base_url, url_info.extra);
                    let cdn_name = extract_cdn_name(&url_info.extra);
                    candidates.push(StreamCandidate {
                        url,
                        codec,
                        qn: codec_info.current_qn,
                        cdn_name,
                        host: url_info.host.clone(),
                    });
                }
            }
        }
    }

    Ok(candidates)
}

/// Extracts the CDN name from the `extra` query string parameter.
/// Returns `"unknown"` if not present.
pub fn extract_cdn_name(extra: &str) -> String {
    let clean = extra.strip_prefix('?').unwrap_or(extra);
    for pair in clean.split('&') {
        let mut parts = pair.splitn(2, '=');
        let next_k = parts.next();
        let next_v = parts.next();
        if let (Some("cdn"), Some(v)) = (next_k, next_v) {
            return v.to_string();
        }
    }
    "unknown".to_string()
}

/// Selects the best stream candidate from a list based on configured and fallback policies.
pub fn select_stream_candidate(
    candidates: &[StreamCandidate],
    config: &ResolvedRecordConfig,
) -> Option<StreamCandidate> {
    if candidates.is_empty() {
        return None;
    }
    let mut sorted: Vec<_> = candidates
        .iter()
        .filter(|candidate| is_supported_codec(candidate.codec))
        .cloned()
        .collect();
    if sorted.is_empty() {
        return None;
    }
    sorted.sort_by(|a, b| compare_candidates(a, b, config));
    sorted.first().cloned()
}

/// Selects the best stream candidate from a list based on configured and fallback policies,
/// and verifies its health. Returns the first healthy candidate.
pub async fn select_healthy_stream_candidate(
    candidates: &[StreamCandidate],
    config: &ResolvedRecordConfig,
    client: &BiliClient,
) -> AppResult<StreamCandidate> {
    if candidates.is_empty() {
        return Err(AppError::Bilibili("no stream candidates returned".into()));
    }
    let mut sorted: Vec<_> = candidates
        .iter()
        .filter(|candidate| is_supported_codec(candidate.codec))
        .cloned()
        .collect();
    if sorted.is_empty() {
        return Err(AppError::Bilibili(
            "no supported AVC FLV stream candidates returned".into(),
        ));
    }
    sorted.sort_by(|a, b| compare_candidates(a, b, config));

    let mut checked = 0usize;
    let mut unhealthy_statuses = 0usize;
    let mut request_errors = 0usize;
    let mut last_request_error = None;

    for candidate in sorted {
        checked += 1;
        match check_stream_health(client.client(), &candidate.url).await {
            Ok(true) => return Ok(candidate),
            Ok(false) => {
                unhealthy_statuses += 1;
                tracing::debug!(
                    qn = candidate.qn,
                    cdn = candidate.cdn_name.as_str(),
                    url = candidate.url.as_str(),
                    "Candidate failed health check (unhealthy status)"
                );
            }
            Err(e) => {
                request_errors += 1;
                last_request_error = Some(e.to_string());
                tracing::debug!(
                    qn = candidate.qn,
                    cdn = candidate.cdn_name.as_str(),
                    url = candidate.url.as_str(),
                    error = %e,
                    "Candidate failed health check (request error)"
                );
            }
        }
    }

    let mut reason = format!(
        "no healthy stream candidates among {checked} supported candidates ({unhealthy_statuses} unhealthy statuses, {request_errors} request errors)"
    );
    if let Some(error) = last_request_error {
        reason.push_str(&format!("; last request error: {error}"));
    }
    Err(AppError::Bilibili(reason))
}

/// Compares two candidates. Returns `Ordering::Less` if `a` is better than `b` (sorting priority).
fn compare_candidates(
    a: &StreamCandidate,
    b: &StreamCandidate,
    config: &ResolvedRecordConfig,
) -> std::cmp::Ordering {
    // 1. QN ranking logic
    let qn_ord = compare_qn(a.qn, b.qn, config.qn);
    if qn_ord != std::cmp::Ordering::Equal {
        return qn_ord.reverse();
    }

    // 2. Configured CDN order
    let cdn_ord = compare_cdn(&a.cdn_name, &b.cdn_name, &config.cdn);
    if cdn_ord != std::cmp::Ordering::Equal {
        return cdn_ord.reverse();
    }

    // 3. Non-MCDN before MCDN
    let mcdn_ord = compare_mcdn(&a.host, &b.host);
    if mcdn_ord != std::cmp::Ordering::Equal {
        return mcdn_ord.reverse();
    }

    std::cmp::Ordering::Equal
}

fn is_supported_codec(codec: Codec) -> bool {
    matches!(codec, Codec::Avc)
}

fn compare_qn(a: u32, b: u32, conf_qn: u32) -> std::cmp::Ordering {
    // 4. Exact configured qn first
    if a == conf_qn && b != conf_qn {
        return std::cmp::Ordering::Greater;
    }
    if b == conf_qn && a != conf_qn {
        return std::cmp::Ordering::Less;
    }
    if a == conf_qn && b == conf_qn {
        return std::cmp::Ordering::Equal;
    }

    // 5. Otherwise highest qn <= configured qn
    let a_le = a <= conf_qn;
    let b_le = b <= conf_qn;
    if a_le && !b_le {
        return std::cmp::Ordering::Greater;
    }
    if b_le && !a_le {
        return std::cmp::Ordering::Less;
    }

    if a_le && b_le {
        return a.cmp(&b);
    }

    // 6. Otherwise highest available qn (both are > conf_qn)
    a.cmp(&b)
}

fn compare_cdn(a: &str, b: &str, cdn_list: &[String]) -> std::cmp::Ordering {
    let pos_a = cdn_list.iter().position(|c| c == a);
    let pos_b = cdn_list.iter().position(|c| c == b);

    match (pos_a, pos_b) {
        (Some(idx_a), Some(idx_b)) => idx_b.cmp(&idx_a), // lower index is better (so returns Greater)
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn compare_mcdn(a_host: &str, b_host: &str) -> std::cmp::Ordering {
    let a_is_mcdn = a_host.contains(".mcdn.");
    let b_is_mcdn = b_host.contains(".mcdn.");

    match (a_is_mcdn, b_is_mcdn) {
        (false, true) => std::cmp::Ordering::Greater, // non-MCDN is better
        (true, false) => std::cmp::Ordering::Less,
        _ => std::cmp::Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_test_candidate(
        url: &str,
        codec: Codec,
        qn: u32,
        cdn_name: &str,
        host: &str,
    ) -> StreamCandidate {
        StreamCandidate {
            url: url.to_string(),
            codec,
            qn,
            cdn_name: cdn_name.to_string(),
            host: host.to_string(),
        }
    }

    fn default_config() -> ResolvedRecordConfig {
        ResolvedRecordConfig {
            credential: None,
            output_dir: PathBuf::from("rec"),
            segment_time: None,
            segment_size: None,
            min_segment_size: 20 * 1024 * 1024,
            qn: 10000,
            cdn: vec![],
            delete_after_submit: false,
        }
    }

    #[test]
    fn test_extract_cdn_name() {
        assert_eq!(extract_cdn_name("cdn=ws&apikey=123"), "ws");
        assert_eq!(extract_cdn_name("?cdn=hws&foo=bar"), "hws");
        assert_eq!(extract_cdn_name("foo=bar&cdn=tx&baz=1"), "tx");
        assert_eq!(extract_cdn_name("foo=bar&baz=1"), "unknown");
        assert_eq!(extract_cdn_name(""), "unknown");
    }

    #[test]
    fn test_unsupported_codec_is_not_selected() {
        let config = default_config();

        let unsupported =
            make_test_candidate("unsupported_url", Codec::Unknown, 10000, "ws", "host");
        let avc = make_test_candidate("avc_url", Codec::Avc, 10000, "ws", "host");

        let selected = select_stream_candidate(&[unsupported.clone(), avc], &config).unwrap();
        assert_eq!(selected.url, "avc_url");
        assert!(select_stream_candidate(&[unsupported], &config).is_none());
    }

    #[tokio::test]
    async fn test_healthy_selection_rejects_empty_candidates_without_network() {
        let client = BiliClient::new(None).unwrap();
        let err = select_healthy_stream_candidate(&[], &default_config(), &client)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("no stream candidates"));
    }

    #[tokio::test]
    async fn test_healthy_selection_rejects_unsupported_candidates_without_network() {
        let client = BiliClient::new(None).unwrap();
        let unsupported =
            make_test_candidate("unsupported_url", Codec::Unknown, 10000, "ws", "host");

        let err = select_healthy_stream_candidate(&[unsupported], &default_config(), &client)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("no supported AVC FLV"));
    }

    #[test]
    fn test_build_play_info_params_requests_flv_avc_only() {
        let params = build_play_info_params(123, 10000);

        assert_eq!(params.get("room_id").map(String::as_str), Some("123"));
        assert_eq!(params.get("qn").map(String::as_str), Some("10000"));
        assert_eq!(params.get("protocol").map(String::as_str), Some("0"));
        assert_eq!(params.get("format").map(String::as_str), Some("0"));
        assert_eq!(params.get("codec").map(String::as_str), Some("0"));
    }

    #[test]
    fn test_ranking_qn_exact() {
        let mut config = default_config();
        config.qn = 10000;

        let c1 = make_test_candidate("qn4000", Codec::Avc, 4000, "ws", "host");
        let c2 = make_test_candidate("qn15000", Codec::Avc, 15000, "ws", "host");
        let c3 = make_test_candidate("qn10000", Codec::Avc, 10000, "ws", "host");

        // Exact qn (10000) first
        let selected = select_stream_candidate(&[c1, c2, c3], &config).unwrap();
        assert_eq!(selected.url, "qn10000");
    }

    #[test]
    fn test_ranking_qn_highest_under() {
        let mut config = default_config();
        config.qn = 10000;

        let c1 = make_test_candidate("qn4000", Codec::Avc, 4000, "ws", "host");
        let c2 = make_test_candidate("qn80", Codec::Avc, 80, "ws", "host");
        let c3 = make_test_candidate("qn15000", Codec::Avc, 15000, "ws", "host");

        // No exact 10000. Under 10000, we have 4000 and 80. Highest of those is 4000.
        let selected = select_stream_candidate(&[c2, c1, c3], &config).unwrap();
        assert_eq!(selected.url, "qn4000");
    }

    #[test]
    fn test_ranking_qn_highest_available() {
        let mut config = default_config();
        config.qn = 10000;

        let c1 = make_test_candidate("qn15000", Codec::Avc, 15000, "ws", "host");
        let c2 = make_test_candidate("qn20000", Codec::Avc, 20000, "ws", "host");

        // Both are > 10000. Highest available wins (20000).
        let selected = select_stream_candidate(&[c1, c2], &config).unwrap();
        assert_eq!(selected.url, "qn20000");
    }

    #[test]
    fn test_ranking_cdn_order() {
        let mut config = default_config();
        config.cdn = vec!["wscdn".to_string(), "txcdn".to_string()];

        let c1 = make_test_candidate("other_cdn", Codec::Avc, 10000, "other", "host");
        let c2 = make_test_candidate("tx_cdn", Codec::Avc, 10000, "txcdn", "host");
        let c3 = make_test_candidate("ws_cdn", Codec::Avc, 10000, "wscdn", "host");

        // configured CDN order: wscdn (c3) > txcdn (c2) > other (c1)
        let selected = select_stream_candidate(&[c1, c2, c3], &config).unwrap();
        assert_eq!(selected.url, "ws_cdn");
    }

    #[test]
    fn test_ranking_mcdn() {
        let config = default_config();

        let c1 = make_test_candidate("mcdn_url", Codec::Avc, 10000, "ws", "host.mcdn.bili.com");
        let c2 = make_test_candidate("non_mcdn_url", Codec::Avc, 10000, "ws", "host.bili.com");

        // Non-MCDN before MCDN
        let selected = select_stream_candidate(&[c1, c2], &config).unwrap();
        assert_eq!(selected.url, "non_mcdn_url");
    }

    #[test]
    fn test_parse_play_info_success() {
        let json_data = r#"{
            "code": 0,
            "message": "0",
            "data": {
                "playurl_info": {
                    "playurl": {
                        "stream": [
                            {
                                "protocol_name": "http_stream",
                                "format": [
                                    {
                                        "format_name": "flv",
                                        "codec": [
                                            {
                                                "codec_name": "avc",
                                                "current_qn": 10000,
                                                "base_url": "/live-bili/test.flv",
                                                "url_info": [
                                                    {
                                                        "host": "https://hw.bili.com",
                                                        "extra": "?cdn=wscdn&key=1"
                                                    },
                                                    {
                                                        "host": "https://tx.bili.com",
                                                        "extra": "?cdn=txcdn&key=2"
                                                    }
                                                ]
                                            },
                                            {
                                                "codec_name": "hevc",
                                                "current_qn": 10000,
                                                "base_url": "/live-bili/hevc.flv",
                                                "url_info": [
                                                    {
                                                        "host": "https://hevc.bili.com",
                                                        "extra": "?cdn=hevc&key=3"
                                                    }
                                                ]
                                            }
                                        ]
                                    },
                                    {
                                        "format_name": "fmp4",
                                        "codec": [
                                            {
                                                "codec_name": "avc",
                                                "current_qn": 10000,
                                                "base_url": "/live-bili/test.m4s",
                                                "url_info": [
                                                    {
                                                        "host": "https://fmp4.bili.com",
                                                        "extra": "?cdn=fmp4&key=4"
                                                    }
                                                ]
                                            }
                                        ]
                                    },
                                    {
                                        "format_name": "ts",
                                        "codec": [
                                            {
                                                "codec_name": "avc",
                                                "current_qn": 10000,
                                                "base_url": "/live-bili/test.ts",
                                                "url_info": [
                                                    {
                                                        "host": "https://ts.bili.com",
                                                        "extra": "?cdn=ts&key=5"
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
        }"#;

        let resp: PlayInfoResponse = serde_json::from_str(json_data).unwrap();
        let candidates = parse_stream_candidates(&resp).unwrap();

        assert_eq!(candidates.len(), 2);
        assert_eq!(
            candidates[0].url,
            "https://hw.bili.com/live-bili/test.flv?cdn=wscdn&key=1"
        );
        assert_eq!(candidates[0].codec, Codec::Avc);
        assert_eq!(candidates[0].qn, 10000);
        assert_eq!(candidates[0].cdn_name, "wscdn");
        assert_eq!(candidates[0].host, "https://hw.bili.com");

        assert_eq!(
            candidates[1].url,
            "https://tx.bili.com/live-bili/test.flv?cdn=txcdn&key=2"
        );
        assert_eq!(candidates[1].cdn_name, "txcdn");
    }

    #[test]
    fn test_parse_play_info_error() {
        let json_data = r#"{
            "code": -400,
            "msg": "invalid room",
            "data": null
        }"#;

        let resp: PlayInfoResponse = serde_json::from_str(json_data).unwrap();
        let res = parse_stream_candidates(&resp);
        assert!(res.is_err());
        let err_msg = res.unwrap_err().to_string();
        assert!(err_msg.contains("getRoomPlayInfo API returned code -400: invalid room"));
    }
}
