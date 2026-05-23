use crate::error::{AppError, AppResult};
use crate::state::model::UploadedPart;
use crate::uploader::types::{SubmissionOutcome, SubmissionRequest, UploadRequest, Uploader};
use biliup::error::Kind as BiliupError;
use biliup::uploader::VideoFile;
use biliup::uploader::bilibili::{BiliBili, Video};
use biliup::uploader::credential::login_by_cookies;
use biliup::uploader::line;
use futures::StreamExt;
use std::path::PathBuf;

pub struct BiliupUploader {
    cookie_path: PathBuf,
    line: String,
    threads: usize,
}

impl BiliupUploader {
    pub fn new(cookie_path: PathBuf, line: String, threads: usize) -> Self {
        Self {
            cookie_path,
            line,
            threads,
        }
    }

    async fn get_bilibili(&self) -> AppResult<BiliBili> {
        login_by_cookies(&self.cookie_path, None)
            .await
            .map_err(|e| AppError::Bilibili(format!("Biliup login failed: {}", e)))
    }
}

impl Uploader for BiliupUploader {
    async fn check_login(&self) -> AppResult<()> {
        let _bili = self.get_bilibili().await?;
        Ok(())
    }

    async fn upload_segment(&self, req: UploadRequest) -> AppResult<UploadedPart> {
        let bili = self.get_bilibili().await?;

        let video_file = VideoFile::new(&req.path).map_err(|e| {
            AppError::Bilibili(format!(
                "Failed to read video file {}: {}",
                req.path.display(),
                e
            ))
        })?;

        let upos_line = if self.line == "auto" {
            line::Probe::probe(&bili.client)
                .await
                .map_err(|e| AppError::Bilibili(format!("Failed to probe auto line: {}", e)))?
        } else if self.line == "bda2" {
            line::bda2()
        } else {
            return Err(AppError::Config(format!(
                "Unsupported upload line: {}",
                self.line
            )));
        };

        let uploader = upos_line
            .pre_upload(&bili, video_file)
            .await
            .map_err(|e| AppError::Bilibili(format!("Pre-upload failed: {}", e)))?;

        let client = biliup::client::StatelessClient::default();
        let video = uploader
            .upload(client, self.threads, |vs| {
                vs.map(|chunk_res| {
                    let chunk =
                        chunk_res.map_err(|e| biliup::error::Kind::Custom(e.to_string()))?;
                    let len = chunk.len();
                    Ok((chunk, len))
                })
            })
            .await
            .map_err(|e| AppError::Bilibili(format!("Upload failed: {}", e)))?;

        Ok(UploadedPart {
            session_id: req.session_id,
            segment_index: req.segment_index,
            bili_filename: video.filename,
            part_title: req.part_title,
        })
    }

    async fn submit(&self, req: SubmissionRequest) -> AppResult<SubmissionOutcome> {
        let bili = self.get_bilibili().await?;

        let mut videos = Vec::new();
        for part in req.parts {
            let mut video = Video::new(&part.bili_filename);
            video.title = Some(part.part_title);
            videos.push(video);
        }

        // Construct Studio by explicit field assignment. The previous
        // implementation hopped through serde_json::from_value(json!({...}))
        // which let biliup's Studio shape leak in via untyped JSON — if
        // biliup renamed a field or changed a type we'd silently send the
        // wrong thing. With named-field construction the compiler breaks
        // loudly when the upstream schema moves, and the boundary stays
        // explicit at this one site.
        let studio = biliup::uploader::bilibili::Studio {
            copyright: req.copyright,
            source: req.source,
            tid: req.tid,
            cover: String::new(),
            title: req.title,
            desc_format_id: 0,
            desc: req.description,
            desc_v2: None,
            dynamic: String::new(),
            subtitle: biliup::uploader::bilibili::Subtitle::default(),
            tag: req.tags.join(","),
            videos,
            dtime: None,
            open_subtitle: false,
            interactive: 0,
            mission_id: None,
            dolby: 0,
            lossless_music: 0,
            no_reprint: 0,
            is_only_self: None,
            charging_pay: 0,
            aid: None,
            up_selection_reply: false,
            up_close_reply: false,
            up_close_danmu: false,
            extra_fields: None,
        };

        let res = match bili.submit_by_app(&studio, None).await {
            Ok(res) => res,
            Err(error) => return submit_error_to_outcome("app", error),
        };

        if res.code != 0 {
            return Err(AppError::Bilibili(format!("Bilibili API error: {}", res)));
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
            // verify on Bilibili and resolve via `state resolve-submission`.
            Ok(SubmissionOutcome::Ambiguous {
                reason: format!(
                    "Bilibili API returned code=0 but no aid/bvid; response: {}",
                    res
                ),
            })
        } else {
            Ok(SubmissionOutcome::Confirmed { aid, bvid })
        }
    }
}

fn submit_error_to_outcome(api: &str, error: BiliupError) -> AppResult<SubmissionOutcome> {
    match error {
        BiliupError::Reqwest(error) => Ok(SubmissionOutcome::Ambiguous {
            reason: format!("Submission ({api}) outcome unknown after HTTP error: {error}"),
        }),
        BiliupError::SerdeJson(error) => Ok(SubmissionOutcome::Ambiguous {
            reason: format!(
                "Submission ({api}) outcome unknown after response parse error: {error}"
            ),
        }),
        other => Err(AppError::Bilibili(format!(
            "Submission ({api}) failed: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_response_parse_error_is_ambiguous() {
        let parse_error = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let outcome = submit_error_to_outcome("app", BiliupError::SerdeJson(parse_error)).unwrap();

        match outcome {
            SubmissionOutcome::Ambiguous { reason } => {
                assert!(reason.contains("outcome unknown"));
                assert!(reason.contains("response parse"));
            }
            SubmissionOutcome::Confirmed { .. } => panic!("parse error must not be confirmed"),
        }
    }

    #[test]
    fn submit_explicit_biliup_custom_error_is_failed() {
        let err = submit_error_to_outcome("app", BiliupError::Custom("code=-1".into()))
            .expect_err("explicit remote rejection should stay an error");

        assert!(err.to_string().contains("Submission (app) failed"));
        assert!(err.to_string().contains("code=-1"));
    }
}
