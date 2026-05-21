use std::time::Duration;

use crate::bilibili::types::{NavResponse, WbiKeys};
use crate::bilibili::wbi::extract_key;
use crate::error::{AppError, AppResult};
use reqwest::header::{COOKIE, HeaderMap, HeaderValue, USER_AGENT};

const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(15);

pub struct BiliClient {
    client: reqwest::Client,
}

impl BiliClient {
    pub fn new(cookie: Option<String>) -> AppResult<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            ),
        );
        if let Some(cookie_str) = cookie {
            let mut val = HeaderValue::from_str(&cookie_str)
                .map_err(|e| AppError::Config(format!("invalid cookie header value: {e}")))?;
            val.set_sensitive(true);
            headers.insert(COOKIE, val);
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(DEFAULT_HTTP_TIMEOUT)
            .build()?;

        Ok(Self { client })
    }

    pub async fn fetch_wbi_keys(&self) -> AppResult<WbiKeys> {
        let resp: NavResponse = self
            .client
            .get("https://api.bilibili.com/x/web-interface/nav")
            .send()
            .await?
            .json()
            .await?;

        parse_wbi_keys(&resp)
    }

    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }
}

fn parse_wbi_keys(resp: &NavResponse) -> AppResult<WbiKeys> {
    let is_ok = resp.code == 0 || (resp.code == -101 && resp.message == "账号未登录");
    if !is_ok {
        return Err(AppError::Bilibili(format!(
            "nav API returned code {}: {}",
            resp.code, resp.message
        )));
    }

    let data = resp
        .data
        .as_ref()
        .ok_or_else(|| AppError::Bilibili("nav API returned empty data".to_string()))?;
    let wbi_img = data
        .wbi_img
        .as_ref()
        .ok_or_else(|| AppError::Bilibili("wbi_img is missing in nav response".to_string()))?;

    let img_key = extract_key(&wbi_img.img_url).ok_or_else(|| {
        AppError::Bilibili(format!(
            "failed to extract img_key from {}",
            wbi_img.img_url
        ))
    })?;
    let sub_key = extract_key(&wbi_img.sub_url).ok_or_else(|| {
        AppError::Bilibili(format!(
            "failed to extract sub_key from {}",
            wbi_img.sub_url
        ))
    })?;

    Ok(WbiKeys { img_key, sub_key })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bilibili::types::{NavData, WbiImgInfo};

    #[test]
    fn test_client_construction() {
        let client_res = BiliClient::new(None);
        assert!(client_res.is_ok());

        let client_with_cookie = BiliClient::new(Some("SESSDATA=mock_sessdata".to_string()));
        assert!(client_with_cookie.is_ok());
    }

    #[test]
    fn test_client_invalid_cookie() {
        // cookies with invalid ASCII values or control characters
        let client_res = BiliClient::new(Some("SESSDATA=\n".to_string()));
        assert!(client_res.is_err());
    }

    #[test]
    fn test_parse_wbi_keys_not_logged_in() {
        let resp = NavResponse {
            code: -101,
            message: "账号未登录".to_string(),
            data: Some(NavData {
                is_login: false,
                uname: None,
                mid: None,
                wbi_img: Some(WbiImgInfo {
                    img_url: "https://i0.hdslb.com/bfs/wbi/7250cfc818b84d69a693f7333a281d30.png"
                        .to_string(),
                    sub_url: "https://i0.hdslb.com/bfs/wbi/c6b4c407cc5d4b52a792f3957245a4a5.png"
                        .to_string(),
                }),
            }),
        };

        let keys = parse_wbi_keys(&resp).unwrap();
        assert_eq!(keys.img_key, "7250cfc818b84d69a693f7333a281d30");
        assert_eq!(keys.sub_key, "c6b4c407cc5d4b52a792f3957245a4a5");
    }

    #[test]
    fn test_parse_wbi_keys_logged_in() {
        let resp = NavResponse {
            code: 0,
            message: "0".to_string(),
            data: Some(NavData {
                is_login: true,
                uname: Some("test_user".to_string()),
                mid: Some(123456),
                wbi_img: Some(WbiImgInfo {
                    img_url: "https://i0.hdslb.com/bfs/wbi/7250cfc818b84d69a693f7333a281d30.png"
                        .to_string(),
                    sub_url: "https://i0.hdslb.com/bfs/wbi/c6b4c407cc5d4b52a792f3957245a4a5.png"
                        .to_string(),
                }),
            }),
        };

        let keys = parse_wbi_keys(&resp).unwrap();
        assert_eq!(keys.img_key, "7250cfc818b84d69a693f7333a281d30");
        assert_eq!(keys.sub_key, "c6b4c407cc5d4b52a792f3957245a4a5");
    }

    #[test]
    fn test_parse_wbi_keys_other_error() {
        let resp = NavResponse {
            code: -999,
            message: "Some unknown error".to_string(),
            data: None,
        };

        let res = parse_wbi_keys(&resp);
        assert!(res.is_err());
    }
}
