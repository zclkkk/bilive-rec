use std::path::PathBuf;

use super::raw::Copyright;

pub(super) fn data_dir() -> PathBuf {
    PathBuf::from("./data")
}

pub(super) fn output_dir() -> PathBuf {
    PathBuf::from("./data/recordings")
}

pub(super) fn min_segment_size() -> String {
    "20MiB".to_string()
}

pub(super) fn qn() -> u32 {
    10000
}

pub(super) fn line() -> String {
    "auto".to_string()
}

pub(super) fn threads() -> usize {
    3
}

pub(super) fn title_template() -> Option<String> {
    None
}

pub(super) fn description_template() -> Option<String> {
    None
}

pub(super) fn category_id() -> u16 {
    171
}

pub(super) fn copyright() -> Copyright {
    Copyright::Reprint
}

pub(super) fn source() -> String {
    "直播录像".to_string()
}

pub(super) fn poll_interval_s() -> u64 {
    60
}

pub(super) fn offline_grace_s() -> u64 {
    60
}

pub(super) fn backoff_s() -> u64 {
    15
}

pub(super) fn max_backoff_s() -> u64 {
    300
}
