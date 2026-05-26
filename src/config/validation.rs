use std::path::Path;
use std::time::Duration;

use crate::error::{AppError, AppResult};

pub(super) fn validate_cookie_file_path(path: &Path, label: &str) -> AppResult<()> {
    if !path.exists() {
        return Err(AppError::Config(format!(
            "{label} does not exist: {}",
            path.display()
        )));
    }
    if !path.is_file() {
        return Err(AppError::Config(format!(
            "{label} is not a regular file: {}",
            path.display()
        )));
    }
    Ok(())
}

pub fn parse_size_bytes(value: &str) -> Option<u64> {
    let s = value.trim().to_uppercase();
    let mut num_str = s.clone();
    let mut multiplier = 1;
    if s.ends_with("GIB") {
        num_str = s.trim_end_matches("GIB").to_string();
        multiplier = 1024 * 1024 * 1024;
    } else if s.ends_with("GB") {
        num_str = s.trim_end_matches("GB").to_string();
        multiplier = 1024 * 1024 * 1024;
    } else if s.ends_with("MIB") {
        num_str = s.trim_end_matches("MIB").to_string();
        multiplier = 1024 * 1024;
    } else if s.ends_with("MB") {
        num_str = s.trim_end_matches("MB").to_string();
        multiplier = 1024 * 1024;
    } else if s.ends_with("KIB") {
        num_str = s.trim_end_matches("KIB").to_string();
        multiplier = 1024;
    } else if s.ends_with("KB") {
        num_str = s.trim_end_matches("KB").to_string();
        multiplier = 1024;
    } else if s.ends_with('B') {
        num_str = s.trim_end_matches('B').to_string();
    }
    num_str.trim().parse::<u64>().ok().map(|n| n * multiplier)
}

pub fn parse_hms_duration(value: &str) -> AppResult<Duration> {
    let parts: Vec<&str> = value.split(':').collect();
    if parts.len() != 3 {
        return Err(AppError::Config(format!("Invalid segment_time: {value}")));
    }
    let h: u64 = parts[0]
        .parse()
        .map_err(|_| AppError::Config(format!("Invalid segment_time: {value}")))?;
    let m: u64 = parts[1]
        .parse()
        .map_err(|_| AppError::Config(format!("Invalid segment_time: {value}")))?;
    let sec: u64 = parts[2]
        .parse()
        .map_err(|_| AppError::Config(format!("Invalid segment_time: {value}")))?;
    Ok(Duration::from_secs(h * 3600 + m * 60 + sec))
}
