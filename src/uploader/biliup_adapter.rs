use crate::error::{AppError, AppResult};
use crate::state::model::UploadedPart;
use crate::uploader::types::{SubmissionRequest, SubmissionResult, UploadRequest, Uploader};
use biliup::uploader::VideoFile;
use biliup::uploader::bilibili::{BiliBili, Studio, Video};
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

    async fn submit(&self, req: SubmissionRequest) -> AppResult<SubmissionResult> {
        let bili = self.get_bilibili().await?;

        let mut videos = Vec::new();
        for part in req.parts {
            let mut video = Video::new(&part.bili_filename);
            video.title = Some(part.part_title);
            videos.push(video);
        }

        // We use serde_json to populate Studio since it has many fields and builder patterns might change
        let mut studio: Studio = serde_json::from_value(serde_json::json!({
            "copyright": req.copyright,
            "source": req.source,
            "tid": req.tid,
            "title": req.title,
            "desc": req.description,
            "tag": req.tags.join(","),
        }))
        .map_err(|e| AppError::Bilibili(format!("Failed to build Studio: {}", e)))?;

        studio.videos = videos;

        let res = bili
            .submit_by_app(&studio, None)
            .await
            .map_err(|e| AppError::Bilibili(format!("Submission failed: {}", e)))?;

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
            return Err(AppError::Bilibili(format!(
                "Submission succeeded but no aid/bvid returned. Response context: {}",
                res
            )));
        }

        Ok(SubmissionResult { aid, bvid })
    }
}
