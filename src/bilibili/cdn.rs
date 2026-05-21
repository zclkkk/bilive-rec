use crate::error::AppResult;
use std::time::Duration;

/// Checks the health of a stream candidate URL.
///
/// Sends a GET request with `Range: bytes=0-1023`, `Referer: https://live.bilibili.com`,
/// and a 5-second timeout. Returns `Ok(true)` if the response is successful (2xx).
pub async fn check_stream_health(client: &reqwest::Client, url: &str) -> AppResult<bool> {
    let r = client
        .get(url)
        .header("Range", "bytes=0-1023")
        .header("Referer", "https://live.bilibili.com")
        .timeout(Duration::from_secs(5))
        .send()
        .await?;

    Ok(r.status().is_success())
}
