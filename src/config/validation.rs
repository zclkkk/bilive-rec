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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigValueError {
    Empty,
    InvalidSizeFormat,
    UnknownSizeUnit(String),
    InvalidDurationFormat,
    InvalidNumber,
    MinuteOutOfRange(u64),
    SecondOutOfRange(u64),
    Overflow,
    ZeroNotAllowed,
}

impl std::fmt::Display for ConfigValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "value must not be empty"),
            Self::InvalidSizeFormat => write!(f, "expected <bytes>[B|KiB|MiB|GiB]"),
            Self::UnknownSizeUnit(unit) => write!(
                f,
                "unknown size unit '{unit}'; expected B, KiB, MiB, or GiB"
            ),
            Self::InvalidDurationFormat => write!(f, "expected HH:MM:SS"),
            Self::InvalidNumber => write!(f, "expected ASCII digits"),
            Self::MinuteOutOfRange(value) => write!(f, "minutes must be less than 60, got {value}"),
            Self::SecondOutOfRange(value) => write!(f, "seconds must be less than 60, got {value}"),
            Self::Overflow => write!(f, "value is too large"),
            Self::ZeroNotAllowed => write!(f, "value must be greater than zero"),
        }
    }
}

pub fn parse_size_bytes(value: &str) -> Result<u64, ConfigValueError> {
    let s = value.trim();
    if s.is_empty() {
        return Err(ConfigValueError::Empty);
    }

    let digit_len = s
        .char_indices()
        .find_map(|(idx, ch)| (!ch.is_ascii_digit()).then_some(idx))
        .unwrap_or(s.len());
    if digit_len == 0 {
        return Err(ConfigValueError::InvalidSizeFormat);
    }

    let number = parse_u64_digits(&s[..digit_len])?;
    let unit = s[digit_len..].trim();
    let multiplier = match unit {
        "" | "B" => 1,
        "KiB" => 1024,
        "MiB" => 1024 * 1024,
        "GiB" => 1024 * 1024 * 1024,
        unit => return Err(ConfigValueError::UnknownSizeUnit(unit.to_string())),
    };

    number
        .checked_mul(multiplier)
        .ok_or(ConfigValueError::Overflow)
}

pub fn parse_hms_duration(value: &str) -> Result<Duration, ConfigValueError> {
    let s = value.trim();
    if s.is_empty() {
        return Err(ConfigValueError::Empty);
    }

    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 3 {
        return Err(ConfigValueError::InvalidDurationFormat);
    }

    let hours = parse_u64_digits(parts[0])?;
    let minutes = parse_u64_digits(parts[1])?;
    let seconds = parse_u64_digits(parts[2])?;

    if minutes >= 60 {
        return Err(ConfigValueError::MinuteOutOfRange(minutes));
    }
    if seconds >= 60 {
        return Err(ConfigValueError::SecondOutOfRange(seconds));
    }

    let hour_seconds = hours.checked_mul(3600).ok_or(ConfigValueError::Overflow)?;
    let minute_seconds = minutes.checked_mul(60).ok_or(ConfigValueError::Overflow)?;
    let total = hour_seconds
        .checked_add(minute_seconds)
        .and_then(|value| value.checked_add(seconds))
        .ok_or(ConfigValueError::Overflow)?;

    Ok(Duration::from_secs(total))
}

fn parse_u64_digits(value: &str) -> Result<u64, ConfigValueError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ConfigValueError::InvalidNumber);
    }
    value.parse::<u64>().map_err(|_| ConfigValueError::Overflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_size_accepts_binary_units_and_bytes() {
        assert_eq!(parse_size_bytes("20MiB"), Ok(20 * 1024 * 1024));
        assert_eq!(parse_size_bytes("2GiB"), Ok(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_size_bytes("1024"), Ok(1024));
        assert_eq!(parse_size_bytes("1024B"), Ok(1024));
        assert_eq!(parse_size_bytes("15 KiB"), Ok(15 * 1024));
    }

    #[test]
    fn parse_size_rejects_ambiguous_or_invalid_units() {
        assert_eq!(
            parse_size_bytes("10MB"),
            Err(ConfigValueError::UnknownSizeUnit("MB".into()))
        );
        assert_eq!(
            parse_size_bytes("15KB"),
            Err(ConfigValueError::UnknownSizeUnit("KB".into()))
        );
        assert_eq!(
            parse_size_bytes("invalid"),
            Err(ConfigValueError::InvalidSizeFormat)
        );
    }

    #[test]
    fn parse_size_rejects_overflow() {
        assert_eq!(
            parse_size_bytes("18446744073709551615GiB"),
            Err(ConfigValueError::Overflow)
        );
    }

    #[test]
    fn parse_duration_accepts_hms() {
        assert_eq!(
            parse_hms_duration("01:30:00"),
            Ok(Duration::from_secs(90 * 60))
        );
        assert_eq!(
            parse_hms_duration("00:15:30"),
            Ok(Duration::from_secs(15 * 60 + 30))
        );
        assert_eq!(parse_hms_duration("1:02:03"), Ok(Duration::from_secs(3723)));
    }

    #[test]
    fn parse_duration_rejects_invalid_components() {
        assert_eq!(
            parse_hms_duration("invalid"),
            Err(ConfigValueError::InvalidDurationFormat)
        );
        assert_eq!(
            parse_hms_duration("01:aa:bb"),
            Err(ConfigValueError::InvalidNumber)
        );
        assert_eq!(
            parse_hms_duration("01:60:00"),
            Err(ConfigValueError::MinuteOutOfRange(60))
        );
        assert_eq!(
            parse_hms_duration("01:00:60"),
            Err(ConfigValueError::SecondOutOfRange(60))
        );
    }

    #[test]
    fn parse_duration_rejects_overflow() {
        assert_eq!(
            parse_hms_duration("18446744073709551615:00:00"),
            Err(ConfigValueError::Overflow)
        );
    }
}
