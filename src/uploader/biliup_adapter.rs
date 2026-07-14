use crate::bilibili::types::NavResponse;
use crate::config::SubmitApi;
use crate::credential::{UploadCredentialFile, UploadPrincipal, validate_login_info};
use crate::state::model::UploadedPart;
use crate::uploader::types::{
    FailureScope, KnownFailure, SubmissionOutcome, SubmissionRequest, UploadOutcome, UploadRequest,
    Uploader,
};
use biliup::error::Kind as BiliupError;
use biliup::uploader::VideoFile;
use biliup::uploader::bilibili::{BiliBili, Video};
use biliup::uploader::credential::{Credential, LoginInfo, bilibili_from_info};
use biliup::uploader::line;
use futures::{FutureExt, StreamExt};
use serde::Deserialize;
use std::panic::AssertUnwindSafe;

const OAUTH_INFO_URL: &str = "https://passport.bilibili.com/x/passport-login/oauth2/info";
const ANDROID_APP_KEY: &str = "783bbb7264451d82";
const ANDROID_APP_SECRET: &str = "2653583c8873dea268ab9386918b1d65";
const NOT_LOGGED_IN_CODE: i32 = -101;
const INVALID_REQUEST_CODE: i32 = -400;

pub struct BiliupUploader {
    principal: UploadPrincipal,
    line: String,
    threads: usize,
    submit_api: SubmitApi,
}

impl BiliupUploader {
    pub fn new(
        principal: UploadPrincipal,
        line: String,
        threads: usize,
        submit_api: SubmitApi,
    ) -> Self {
        Self {
            principal,
            line,
            threads,
            submit_api,
        }
    }

    async fn authenticate(&self) -> Result<AuthenticatedBiliup, AuthFailure> {
        let mut credential_file = UploadCredentialFile::open(self.principal.cookie_file())
            .map_err(|error| {
                AuthFailure::blocked(format!("Invalid upload credential document: {error}"))
            })?;
        let mut login_info = credential_file.login_info().clone();
        verify_declared_principal(&login_info, self.principal.expected_mid)?;

        let oauth = verify_oauth_principal(&login_info, self.principal.expected_mid).await?;
        let refreshed = oauth.refresh;
        if refreshed {
            let refresh_client = Credential::new(None);
            login_info = AssertUnwindSafe(refresh_client.renew_tokens(login_info))
                .catch_unwind()
                .await
                .map_err(|_| {
                    AuthFailure::retryable(
                        "Biliup panicked while decoding a credential refresh response",
                    )
                })?
                .map_err(AuthFailure::from_login)?;
            validate_login_info(&login_info).map_err(|problem| {
                AuthFailure::retryable(format!(
                    "Bilibili returned an invalid refreshed credential: {problem}"
                ))
            })?;
            verify_declared_principal(&login_info, self.principal.expected_mid)?;
            verify_oauth_principal(&login_info, self.principal.expected_mid).await?;
        }

        // The upstream constructor assumes a validated cookie_info and uses
        // unwrap internally. The strict document check above is the ordinary
        // error path; catch_unwind keeps a dependency invariant violation from
        // escaping across this external boundary.
        let bili = std::panic::catch_unwind(AssertUnwindSafe(|| {
            bilibili_from_info(login_info.clone(), None)
        }))
        .map_err(|_| {
            AuthFailure::blocked("Biliup rejected a validated credential document internally")
        })?
        .map_err(AuthFailure::from_login)?;

        let response = bili
            .client
            .get("https://api.bilibili.com/x/web-interface/nav")
            .send()
            .await
            .map_err(|error| {
                AuthFailure::from_http("Bilibili cookie account verification", error)
            })?;
        let status = response.status();
        verify_auth_http_status("Bilibili cookie account verification", status)?;
        let nav: NavResponse = response.json().await.map_err(|error| {
            AuthFailure::response_decode("Bilibili cookie account verification", error)
        })?;
        verify_authenticated_principal(nav, self.principal.expected_mid)?;

        if refreshed {
            credential_file.replace(login_info).map_err(|error| {
                AuthFailure::blocked(format!(
                    "Failed to persist the verified refreshed credential: {error}"
                ))
            })?;
        }

        Ok(AuthenticatedBiliup { bili })
    }
}

#[derive(Debug)]
struct AuthenticatedBiliup {
    bili: BiliBili,
}

impl AuthenticatedBiliup {
    fn into_inner(self) -> BiliBili {
        self.bili
    }
}

impl Uploader for BiliupUploader {
    async fn upload_segment(&self, req: UploadRequest) -> UploadOutcome {
        let bili = match self.authenticate().await {
            Ok(authenticated) => authenticated.into_inner(),
            Err(error) => return error.upload_outcome(),
        };

        let video_file = match VideoFile::new(&req.path) {
            Ok(file) => file,
            Err(error) => {
                return UploadOutcome::BlockedKnownFailure(KnownFailure {
                    reason: format!("Failed to read video file {}: {error}", req.path.display()),
                    scope: FailureScope::Item,
                });
            }
        };

        let upos_line = if self.line == "auto" {
            match line::Probe::probe(&bili.client).await {
                Ok(line) => line,
                Err(error) => return probe_error_to_outcome(error),
            }
        } else if self.line == "bda2" {
            line::bda2()
        } else {
            return UploadOutcome::BlockedKnownFailure(KnownFailure {
                reason: format!("Unsupported upload line: {}", self.line),
                scope: FailureScope::Target,
            });
        };

        let uploader = match upos_line.pre_upload(&bili, video_file).await {
            Ok(uploader) => uploader,
            Err(error) => return pre_upload_error_to_outcome(error),
        };

        let client = biliup::client::StatelessClient::default();
        let video = match uploader
            .upload(client, self.threads, |vs| {
                vs.map(|chunk_res| {
                    let chunk =
                        chunk_res.map_err(|e| biliup::error::Kind::Custom(e.to_string()))?;
                    let len = chunk.len();
                    Ok((chunk, len))
                })
            })
            .await
        {
            Ok(video) => video,
            Err(error) => {
                return UploadOutcome::Ambiguous {
                    reason: format!(
                        "Upload outcome unknown after multipart transfer error: {error}"
                    ),
                };
            }
        };

        UploadOutcome::Confirmed(UploadedPart {
            bili_filename: video.filename,
            part_title: req.part_title,
        })
    }

    async fn submit(&self, req: SubmissionRequest) -> SubmissionOutcome {
        let bili = match self.authenticate().await {
            Ok(authenticated) => authenticated.into_inner(),
            Err(error) => return error.submission_outcome(),
        };

        if self.submit_api == SubmitApi::Web && !has_bili_jct(&bili) {
            return SubmissionOutcome::BlockedKnownFailure(KnownFailure {
                reason: "Web submission requires a non-empty bili_jct cookie".into(),
                scope: FailureScope::Target,
            });
        }

        let studio = studio_from_submission(req);

        let res = match self.submit_api {
            SubmitApi::App => match bili.submit_by_app(&studio, None).await {
                Ok(res) => res,
                Err(error) => return submit_error_to_outcome("app", error),
            },
            SubmitApi::Web => match bili.submit_by_web(&studio, None).await {
                Ok(res) => res,
                Err(error) => return submit_error_to_outcome("web", error),
            },
            SubmitApi::BCutAndroid => match bili.submit_by_bcut_android(&studio, None).await {
                Ok(res) => res,
                Err(error) => return submit_error_to_outcome("bcut_android", error),
            },
        };

        if res.code != 0 {
            return SubmissionOutcome::BlockedKnownFailure(KnownFailure {
                reason: format!("Bilibili API error: {res}"),
                scope: FailureScope::Item,
            });
        }

        let mut aid = None;
        let mut bvid = None;

        if let Some(ref data) = res.data {
            if let Some(a) = data.get("aid").and_then(|v| v.as_u64()) {
                aid = Some(a);
            }
            if let Some(b) = data.get("bvid").and_then(|v| v.as_str()) {
                bvid = Some(b.to_string());
            }
        }

        if aid.is_none() && bvid.is_none() {
            // Bilibili accepted the submission (code=0) but did not return any
            // identifier — we cannot prove locally whether the video was
            // actually created. Surface it as Ambiguous so the operator can
            // verify on Bilibili and resolve via `recover submission`.
            SubmissionOutcome::Ambiguous {
                reason: format!(
                    "Bilibili API returned code=0 but no aid/bvid; response: {}",
                    res
                ),
            }
        } else {
            SubmissionOutcome::Confirmed { aid, bvid }
        }
    }
}

#[derive(Debug)]
struct AuthFailure {
    reason: String,
    retryable: bool,
}

impl AuthFailure {
    fn retryable(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            retryable: true,
        }
    }

    fn blocked(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            retryable: false,
        }
    }

    fn from_login(error: BiliupError) -> Self {
        let reason = format!("Biliup credential refresh failed: {error}");
        if is_safe_login_retry(&error) {
            Self::retryable(reason)
        } else {
            Self::blocked(reason)
        }
    }

    fn from_http(context: &str, error: reqwest::Error) -> Self {
        let reason = format!("{context} failed: {error}");
        if error.is_builder() {
            Self::blocked(reason)
        } else {
            // Authentication has not crossed an upload/submission boundary.
            // Transport, body, and protocol failures are therefore safe to
            // retry; only a locally unbuildable request is deterministic.
            Self::retryable(reason)
        }
    }

    fn response_decode(context: &str, error: impl std::fmt::Display) -> Self {
        Self::retryable(format!("Invalid {context} response: {error}"))
    }

    fn upload_outcome(self) -> UploadOutcome {
        let failure = KnownFailure {
            reason: self.reason,
            scope: FailureScope::Target,
        };
        if self.retryable {
            UploadOutcome::RetryableKnownFailure(failure)
        } else {
            UploadOutcome::BlockedKnownFailure(failure)
        }
    }

    fn submission_outcome(self) -> SubmissionOutcome {
        let failure = KnownFailure {
            reason: self.reason,
            scope: FailureScope::Target,
        };
        if self.retryable {
            SubmissionOutcome::RetryableKnownFailure(failure)
        } else {
            SubmissionOutcome::BlockedKnownFailure(failure)
        }
    }
}

#[derive(Debug, Deserialize)]
struct OAuthInfoResponse {
    code: i32,
    #[serde(default, alias = "msg")]
    message: String,
    data: Option<OAuthPrincipal>,
}

#[derive(Debug, Deserialize)]
struct OAuthPrincipal {
    mid: u64,
    #[serde(default)]
    refresh: bool,
}

async fn verify_oauth_principal(
    login_info: &LoginInfo,
    expected_mid: u64,
) -> Result<OAuthPrincipal, AuthFailure> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| {
            AuthFailure::blocked(format!("System clock is before Unix epoch: {error}"))
        })?
        .as_secs();
    let params = oauth_info_params(&login_info.token_info.access_token, timestamp)?;
    let response = reqwest::Client::new()
        .get(OAUTH_INFO_URL)
        .query(&params)
        .send()
        .await
        .map_err(|error| AuthFailure::from_http("Bilibili OAuth account verification", error))?;
    let status = response.status();
    verify_auth_http_status("Bilibili OAuth account verification", status)?;
    let response = response.json().await.map_err(|error| {
        AuthFailure::response_decode("Bilibili OAuth account verification", error)
    })?;
    verify_oauth_response(response, expected_mid)
}

fn verify_auth_http_status(context: &str, status: reqwest::StatusCode) -> Result<(), AuthFailure> {
    if status.is_success() {
        return Ok(());
    }
    if status.is_server_error() || matches!(status.as_u16(), 408 | 412 | 425 | 429) {
        return Err(AuthFailure::retryable(format!(
            "{context} returned transient HTTP {status}"
        )));
    }
    Err(AuthFailure::blocked(format!(
        "{context} returned permanent HTTP {status}"
    )))
}

fn oauth_info_params(
    access_token: &str,
    timestamp: u64,
) -> Result<Vec<(String, String)>, AuthFailure> {
    let mut params = vec![
        ("access_key".to_string(), access_token.to_string()),
        ("actionKey".to_string(), "appkey".to_string()),
        ("appkey".to_string(), ANDROID_APP_KEY.to_string()),
        ("ts".to_string(), timestamp.to_string()),
    ];
    let unsigned_query = serde_urlencoded::to_string(&params).map_err(|error| {
        AuthFailure::blocked(format!(
            "Failed to encode OAuth verification request parameters: {error}"
        ))
    })?;
    let sign = Credential::sign(&unsigned_query, ANDROID_APP_SECRET);
    params.push(("sign".to_string(), sign));
    Ok(params)
}

fn verify_oauth_response(
    response: OAuthInfoResponse,
    expected_mid: u64,
) -> Result<OAuthPrincipal, AuthFailure> {
    if matches!(response.code, NOT_LOGGED_IN_CODE | INVALID_REQUEST_CODE) {
        return Err(AuthFailure::blocked(format!(
            "Bilibili OAuth credential was explicitly rejected (code {}): {}",
            response.code, response.message
        )));
    }
    if response.code != 0 {
        return Err(AuthFailure::retryable(format!(
            "Bilibili OAuth account verification returned an unknown API failure (code {}): {}",
            response.code, response.message
        )));
    }
    let principal = response.data.ok_or_else(|| {
        AuthFailure::retryable("Bilibili OAuth account verification returned no account data")
    })?;
    if principal.mid == 0 {
        return Err(AuthFailure::retryable(
            "Bilibili OAuth account verification returned no non-zero mid",
        ));
    }
    if principal.mid != expected_mid {
        return Err(AuthFailure::blocked(format!(
            "Bilibili OAuth account mismatch: expected mid {expected_mid}, authenticated mid {}",
            principal.mid
        )));
    }
    Ok(principal)
}

fn verify_declared_principal(login_info: &LoginInfo, expected_mid: u64) -> Result<(), AuthFailure> {
    let declared_mid = login_info.token_info.mid;
    if declared_mid != expected_mid {
        return Err(AuthFailure::blocked(format!(
            "Credential document account mismatch: expected mid {expected_mid}, token_info.mid is {declared_mid}"
        )));
    }
    Ok(())
}

fn has_bili_jct(bili: &BiliBili) -> bool {
    has_bili_jct_cookie_info(&bili.login_info.cookie_info)
}

fn has_bili_jct_cookie_info(cookie_info: &serde_json::Value) -> bool {
    cookie_info
        .get("cookies")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|cookies| {
            cookies.iter().any(|cookie| {
                cookie.get("name").and_then(serde_json::Value::as_str) == Some("bili_jct")
                    && cookie
                        .get("value")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|value| !value.is_empty())
            })
        })
}

fn verify_authenticated_principal(nav: NavResponse, expected_mid: u64) -> Result<(), AuthFailure> {
    if matches!(nav.code, NOT_LOGGED_IN_CODE | INVALID_REQUEST_CODE) {
        return Err(AuthFailure::blocked(format!(
            "Bilibili credential was explicitly rejected (code {}): {}",
            nav.code, nav.message
        )));
    }
    if nav.code != 0 {
        return Err(AuthFailure::retryable(format!(
            "Bilibili cookie account verification returned an unknown API failure (code {}): {}",
            nav.code, nav.message
        )));
    }
    let data = nav.data.ok_or_else(|| {
        AuthFailure::retryable("Bilibili account verification returned no account data")
    })?;
    if !data.is_login {
        return Err(AuthFailure::blocked(
            "Bilibili credential is explicitly reported as not logged in",
        ));
    }
    let actual_mid = data.mid.filter(|mid| *mid != 0).ok_or_else(|| {
        AuthFailure::retryable("Bilibili account verification returned no non-zero mid")
    })?;
    if actual_mid != expected_mid {
        return Err(AuthFailure::blocked(format!(
            "Bilibili account mismatch: expected mid {expected_mid}, authenticated mid {actual_mid}"
        )));
    }
    Ok(())
}

#[cfg(test)]
fn login_error_to_upload_outcome(error: BiliupError) -> UploadOutcome {
    let retryable = is_safe_login_retry(&error);
    let failure = KnownFailure {
        reason: format!("Biliup login failed: {error}"),
        scope: FailureScope::Target,
    };
    if retryable {
        UploadOutcome::RetryableKnownFailure(failure)
    } else {
        UploadOutcome::BlockedKnownFailure(failure)
    }
}

#[cfg(test)]
fn login_error_to_submission_outcome(error: BiliupError) -> SubmissionOutcome {
    let retryable = is_safe_login_retry(&error);
    let failure = KnownFailure {
        reason: format!("Biliup login failed: {error}"),
        scope: FailureScope::Target,
    };
    if retryable {
        SubmissionOutcome::RetryableKnownFailure(failure)
    } else {
        SubmissionOutcome::BlockedKnownFailure(failure)
    }
}

fn is_safe_login_retry(error: &BiliupError) -> bool {
    // Login/token validation cannot create an upload or submission artifact,
    // so transport failures and rate limits are safe to retry. Local cookie
    // I/O/JSON and credential-shape failures stay blocked.
    matches!(
        error,
        BiliupError::Reqwest(_)
            | BiliupError::ReqwestMiddleware(_)
            | BiliupError::SerdeJson(_)
            | BiliupError::RateLimit { .. }
    )
}

fn probe_error_to_outcome(error: BiliupError) -> UploadOutcome {
    let retryable = matches!(
        &error,
        BiliupError::Reqwest(_)
            | BiliupError::ReqwestMiddleware(_)
            | BiliupError::SerdeJson(_)
            | BiliupError::RateLimit { .. }
    );
    let failure = KnownFailure {
        reason: format!("Failed to probe auto line: {error}"),
        scope: FailureScope::Target,
    };
    if retryable {
        UploadOutcome::RetryableKnownFailure(failure)
    } else {
        UploadOutcome::BlockedKnownFailure(failure)
    }
}

fn pre_upload_error_to_outcome(error: BiliupError) -> UploadOutcome {
    match error {
        BiliupError::Reqwest(error) if error.is_builder() => {
            UploadOutcome::BlockedKnownFailure(KnownFailure {
                reason: format!("Pre-upload request could not be built: {error}"),
                scope: FailureScope::Target,
            })
        }
        BiliupError::Reqwest(error) if error.is_connect() => {
            UploadOutcome::RetryableKnownFailure(KnownFailure {
                reason: format!("Pre-upload connection was not established: {error}"),
                scope: FailureScope::Target,
            })
        }
        BiliupError::Reqwest(error) => UploadOutcome::Ambiguous {
            reason: format!(
                "Pre-upload outcome unknown after HTTP error; a remote upload session may exist: {error}"
            ),
        },
        BiliupError::ReqwestMiddleware(error) if error.is_builder() => {
            UploadOutcome::BlockedKnownFailure(KnownFailure {
                reason: format!("Pre-upload request could not be built: {error}"),
                scope: FailureScope::Target,
            })
        }
        BiliupError::ReqwestMiddleware(error) if error.is_connect() => {
            UploadOutcome::RetryableKnownFailure(KnownFailure {
                reason: format!("Pre-upload connection was not established: {error}"),
                scope: FailureScope::Target,
            })
        }
        BiliupError::ReqwestMiddleware(error) => UploadOutcome::Ambiguous {
            reason: format!(
                "Pre-upload outcome unknown after middleware HTTP error; a remote upload session may exist: {error}"
            ),
        },
        BiliupError::SerdeJson(error) => UploadOutcome::Ambiguous {
            reason: format!(
                "Pre-upload outcome unknown after response parse error; a remote upload session may exist: {error}"
            ),
        },
        BiliupError::RateLimit { code, message } => {
            UploadOutcome::RetryableKnownFailure(KnownFailure {
                reason: format!("Pre-upload rate limited (code {code}): {message}"),
                scope: FailureScope::Target,
            })
        }
        BiliupError::NeedRecaptcha(reason) => UploadOutcome::BlockedKnownFailure(KnownFailure {
            reason: format!("Pre-upload requires recaptcha: {reason}"),
            scope: FailureScope::Target,
        }),
        other => UploadOutcome::BlockedKnownFailure(KnownFailure {
            reason: format!("Pre-upload was explicitly rejected before transfer: {other}"),
            scope: FailureScope::Item,
        }),
    }
}

fn studio_from_submission(req: SubmissionRequest) -> biliup::uploader::bilibili::Studio {
    let mut videos = Vec::new();
    for part in req.parts {
        let mut video = Video::new(&part.bili_filename);
        video.title = Some(part.part_title);
        videos.push(video);
    }

    // Named fields keep the upstream Studio boundary typed: schema changes
    // fail at compile time instead of leaking through untyped JSON.
    biliup::uploader::bilibili::Studio {
        copyright: req.copyright.as_biliup_code(),
        source: req.source,
        tid: req.category_id,
        cover: String::new(),
        title: req.title,
        desc_format_id: 0,
        desc: req.description,
        desc_v2: None,
        dynamic: req.dynamic,
        subtitle: biliup::uploader::bilibili::Subtitle::default(),
        tag: req.tags.join(","),
        videos,
        dtime: None,
        open_subtitle: false,
        interactive: 0,
        mission_id: None,
        dolby: 0,
        lossless_music: 0,
        no_reprint: req.forbid_reprint as u8,
        is_only_self: req.private.then_some(1),
        charging_pay: req.charging_panel as u8,
        aid: None,
        up_selection_reply: req.featured_reply,
        up_close_reply: req.close_reply,
        up_close_danmu: req.close_danmu,
        extra_fields: None,
    }
}

fn submit_error_to_outcome(api: &str, error: BiliupError) -> SubmissionOutcome {
    match error {
        BiliupError::Reqwest(error) if error.is_builder() => {
            SubmissionOutcome::BlockedKnownFailure(KnownFailure {
                reason: format!("Submission ({api}) request could not be built: {error}"),
                scope: FailureScope::Target,
            })
        }
        BiliupError::Reqwest(error) if error.is_connect() => {
            SubmissionOutcome::RetryableKnownFailure(KnownFailure {
                reason: format!("Submission ({api}) connection was not established: {error}"),
                scope: FailureScope::Target,
            })
        }
        BiliupError::Reqwest(error) => SubmissionOutcome::Ambiguous {
            reason: format!("Submission ({api}) outcome unknown after HTTP error: {error}"),
        },
        BiliupError::ReqwestMiddleware(error) if error.is_builder() => {
            SubmissionOutcome::BlockedKnownFailure(KnownFailure {
                reason: format!("Submission ({api}) request could not be built: {error}"),
                scope: FailureScope::Target,
            })
        }
        BiliupError::ReqwestMiddleware(error) if error.is_connect() => {
            SubmissionOutcome::RetryableKnownFailure(KnownFailure {
                reason: format!("Submission ({api}) connection was not established: {error}"),
                scope: FailureScope::Target,
            })
        }
        BiliupError::ReqwestMiddleware(error) => SubmissionOutcome::Ambiguous {
            reason: format!(
                "Submission ({api}) outcome unknown after middleware HTTP error: {error}"
            ),
        },
        BiliupError::SerdeJson(error) => SubmissionOutcome::Ambiguous {
            reason: format!(
                "Submission ({api}) outcome unknown after response parse error: {error}"
            ),
        },
        BiliupError::RateLimit { code, message } => {
            SubmissionOutcome::RetryableKnownFailure(KnownFailure {
                reason: format!("Submission ({api}) rate limited (code {code}): {message}"),
                scope: FailureScope::Target,
            })
        }
        BiliupError::NeedRecaptcha(reason) => {
            SubmissionOutcome::BlockedKnownFailure(KnownFailure {
                reason: format!("Submission ({api}) requires recaptcha: {reason}"),
                scope: FailureScope::Target,
            })
        }
        other => SubmissionOutcome::BlockedKnownFailure(KnownFailure {
            reason: format!("Submission ({api}) was explicitly rejected: {other}"),
            scope: FailureScope::Item,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Copyright;
    use crate::state::model::UploadedPart;

    fn submission_request() -> SubmissionRequest {
        SubmissionRequest {
            title: "title".into(),
            description: "description".into(),
            category_id: 171,
            copyright: Copyright::Reprint,
            tags: vec!["tag-a".into(), "tag-b".into()],
            source: "source".into(),
            private: true,
            dynamic: "dynamic".into(),
            forbid_reprint: true,
            charging_panel: true,
            close_reply: true,
            close_danmu: true,
            featured_reply: true,
            parts: vec![UploadedPart {
                bili_filename: "bili-file".into(),
                part_title: "part-title".into(),
            }],
        }
    }

    #[test]
    fn submit_response_parse_error_is_ambiguous() {
        let parse_error = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let outcome = submit_error_to_outcome("app", BiliupError::SerdeJson(parse_error));

        match outcome {
            SubmissionOutcome::Ambiguous { reason } => {
                assert!(reason.contains("outcome unknown"));
                assert!(reason.contains("response parse"));
            }
            SubmissionOutcome::Confirmed { .. } => panic!("parse error must not be confirmed"),
            other => panic!("response parse error is ambiguous, got {other:?}"),
        }
    }

    #[test]
    fn submit_explicit_biliup_custom_error_is_item_blocked() {
        let outcome = submit_error_to_outcome("app", BiliupError::Custom("code=-1".into()));

        match outcome {
            SubmissionOutcome::BlockedKnownFailure(failure) => {
                assert_eq!(failure.scope, FailureScope::Item);
                assert!(failure.reason.contains("explicitly rejected"));
                assert!(failure.reason.contains("code=-1"));
            }
            other => panic!("expected item-blocked failure, got {other:?}"),
        }
    }

    #[test]
    fn pre_upload_rate_limit_is_target_retryable() {
        let outcome = pre_upload_error_to_outcome(BiliupError::RateLimit {
            code: 601,
            message: "too fast".into(),
        });

        match outcome {
            UploadOutcome::RetryableKnownFailure(failure) => {
                assert_eq!(failure.scope, FailureScope::Target);
                assert!(failure.reason.contains("601"));
            }
            other => panic!("expected target retry, got {other:?}"),
        }
    }

    #[test]
    fn probe_response_parse_failure_is_target_retryable() {
        let parse_error = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let outcome = probe_error_to_outcome(BiliupError::SerdeJson(parse_error));

        match outcome {
            UploadOutcome::RetryableKnownFailure(failure) => {
                assert_eq!(failure.scope, FailureScope::Target);
                assert!(failure.reason.contains("probe auto line"));
            }
            other => panic!("expected target retry, got {other:?}"),
        }
    }

    #[test]
    fn pre_upload_explicit_rejection_is_item_blocked() {
        let outcome = pre_upload_error_to_outcome(BiliupError::Custom("file rejected".into()));

        match outcome {
            UploadOutcome::BlockedKnownFailure(failure) => {
                assert_eq!(failure.scope, FailureScope::Item);
                assert!(failure.reason.contains("explicitly rejected"));
            }
            other => panic!("expected blocked item, got {other:?}"),
        }
    }

    #[test]
    fn login_rate_limit_is_target_retryable() {
        let outcome = login_error_to_submission_outcome(BiliupError::RateLimit {
            code: 601,
            message: "too fast".into(),
        });

        match outcome {
            SubmissionOutcome::RetryableKnownFailure(failure) => {
                assert_eq!(failure.scope, FailureScope::Target);
                assert!(failure.reason.contains("too fast"));
            }
            other => panic!("expected target retry, got {other:?}"),
        }
    }

    #[test]
    fn invalid_local_cookie_is_target_blocked() {
        let outcome = login_error_to_upload_outcome(BiliupError::IO(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "missing cookies",
        )));

        match outcome {
            UploadOutcome::BlockedKnownFailure(failure) => {
                assert_eq!(failure.scope, FailureScope::Target);
                assert!(failure.reason.contains("missing cookies"));
            }
            other => panic!("expected blocked target, got {other:?}"),
        }
    }

    #[test]
    fn submission_request_maps_to_biliup_studio_fields() {
        let studio = studio_from_submission(submission_request());

        assert_eq!(studio.tid, 171);
        assert_eq!(studio.copyright, 2);
        assert_eq!(studio.source, "source");
        assert_eq!(studio.tag, "tag-a,tag-b");
        assert_eq!(studio.dynamic, "dynamic");
        assert_eq!(studio.no_reprint, 1);
        assert_eq!(studio.is_only_self, Some(1));
        assert_eq!(studio.charging_pay, 1);
        assert!(studio.up_close_reply);
        assert!(studio.up_close_danmu);
        assert!(studio.up_selection_reply);
        assert_eq!(studio.videos.len(), 1);
        assert_eq!(studio.videos[0].title.as_deref(), Some("part-title"));
    }

    #[test]
    fn authenticated_mid_must_match_the_frozen_principal() {
        let error = verify_authenticated_principal(
            NavResponse {
                code: 0,
                message: "ok".into(),
                data: Some(crate::bilibili::types::NavData {
                    is_login: true,
                    uname: Some("other".into()),
                    mid: Some(2),
                    wbi_img: None,
                }),
            },
            1,
        )
        .unwrap_err();
        assert!(!error.retryable);
        assert!(error.reason.contains("expected mid 1"));
        assert!(error.reason.contains("authenticated mid 2"));
    }

    #[test]
    fn oauth_mid_must_match_the_frozen_principal() {
        let error = verify_oauth_response(
            OAuthInfoResponse {
                code: 0,
                message: "ok".into(),
                data: Some(OAuthPrincipal {
                    mid: 2,
                    refresh: false,
                }),
            },
            1,
        )
        .unwrap_err();

        assert!(!error.retryable);
        assert!(error.reason.contains("OAuth account mismatch"));
        assert!(error.reason.contains("authenticated mid 2"));
    }

    #[test]
    fn authentication_protocol_anomalies_are_retryable() {
        let missing_oauth_data = verify_oauth_response(
            OAuthInfoResponse {
                code: 0,
                message: "ok".into(),
                data: None,
            },
            1,
        )
        .unwrap_err();
        assert!(missing_oauth_data.retryable);

        let unknown_oauth_failure = verify_oauth_response(
            OAuthInfoResponse {
                code: -500,
                message: "temporary service failure".into(),
                data: None,
            },
            1,
        )
        .unwrap_err();
        assert!(unknown_oauth_failure.retryable);

        let missing_cookie_data = verify_authenticated_principal(
            NavResponse {
                code: 0,
                message: "ok".into(),
                data: None,
            },
            1,
        )
        .unwrap_err();
        assert!(missing_cookie_data.retryable);
    }

    #[test]
    fn explicit_not_logged_in_responses_block_the_target() {
        let oauth = verify_oauth_response(
            OAuthInfoResponse {
                code: NOT_LOGGED_IN_CODE,
                message: "not logged in".into(),
                data: None,
            },
            1,
        )
        .unwrap_err();
        assert!(!oauth.retryable);

        let cookie = verify_authenticated_principal(
            NavResponse {
                code: 0,
                message: "ok".into(),
                data: Some(crate::bilibili::types::NavData {
                    is_login: false,
                    uname: None,
                    mid: None,
                    wbi_img: None,
                }),
            },
            1,
        )
        .unwrap_err();
        assert!(!cookie.retryable);
    }

    #[test]
    fn oauth_verification_uses_the_pinned_biliup_signing_shape() {
        let params = oauth_info_params("a b/+", 1).unwrap();
        let sign = params
            .iter()
            .find_map(|(key, value)| (key == "sign").then_some(value))
            .unwrap();
        let unsigned =
            format!("access_key=a+b%2F%2B&actionKey=appkey&appkey={ANDROID_APP_KEY}&ts=1");
        let expected = format!(
            "{:x}",
            md5::compute(format!("{unsigned}{ANDROID_APP_SECRET}"))
        );

        assert_eq!(sign, &expected);
    }

    #[test]
    fn authentication_response_decode_failures_are_retryable() {
        let error = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let failure = AuthFailure::response_decode("account verification", error);

        assert!(failure.retryable);
        assert!(
            failure
                .reason
                .contains("Invalid account verification response")
        );
    }

    #[test]
    fn authentication_http_statuses_distinguish_transient_and_permanent_failures() {
        for status in [
            reqwest::StatusCode::REQUEST_TIMEOUT,
            reqwest::StatusCode::PRECONDITION_FAILED,
            reqwest::StatusCode::TOO_EARLY,
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            reqwest::StatusCode::BAD_GATEWAY,
        ] {
            assert!(
                verify_auth_http_status("account verification", status)
                    .unwrap_err()
                    .retryable
            );
        }

        assert!(
            !verify_auth_http_status("account verification", reqwest::StatusCode::UNAUTHORIZED)
                .unwrap_err()
                .retryable
        );
    }

    #[test]
    fn web_submission_requires_non_empty_bili_jct() {
        assert!(!has_bili_jct_cookie_info(
            &serde_json::json!({"cookies": []})
        ));
        assert!(!has_bili_jct_cookie_info(&serde_json::json!({
            "cookies": [{"name": "bili_jct", "value": ""}]
        })));
        assert!(has_bili_jct_cookie_info(&serde_json::json!({
            "cookies": [{"name": "bili_jct", "value": "csrf"}]
        })));
    }

    /// Authentication is deliberately repeated at every remote boundary.
    #[tokio::test]
    async fn login_failure_is_rechecked_on_each_operation() {
        let uploader = BiliupUploader::new(
            UploadPrincipal::new(
                crate::credential::CredentialRef::new(
                    "main",
                    "/nonexistent/path/that/will/never/exist.json",
                ),
                1,
            ),
            "auto".into(),
            1,
            SubmitApi::App,
        );

        let err1 = uploader.authenticate().await.unwrap_err();
        let err2 = uploader.authenticate().await.unwrap_err();
        assert!(err1.reason.contains("Invalid upload credential document"));
        assert_eq!(err1.reason, err2.reason);
    }

    #[tokio::test]
    async fn malformed_cookie_document_returns_an_error_without_entering_biliup() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            br#"{
                "cookie_info": {},
                "sso": [],
                "token_info": {
                    "access_token": "token",
                    "expires_in": 3600,
                    "mid": 1,
                    "refresh_token": "refresh"
                },
                "platform": null
            }"#,
        )
        .unwrap();
        let uploader = BiliupUploader::new(
            UploadPrincipal::new(
                crate::credential::CredentialRef::new("main", file.path()),
                1,
            ),
            "auto".into(),
            1,
            SubmitApi::App,
        );

        let error = uploader.authenticate().await.unwrap_err();
        assert!(!error.retryable);
        assert!(
            error
                .reason
                .contains("cookie_info.cookies must be an array")
        );
    }
}
