use jiff::{fmt::strtime, tz::TimeZone};

use crate::error::{AppError, AppResult};
use crate::state::model::LiveSession;

pub(crate) fn validate_room_template(template: &str) -> AppResult<()> {
    parse_room_template(template, |placeholder| {
        parse_placeholder(placeholder)?;
        Ok(String::new())
    })
    .map(|_| ())
}

pub(crate) fn render_room_template(
    template: &str,
    room_name: &str,
    room_url: &str,
    session: Option<&LiveSession>,
    room_id: u64,
) -> AppResult<String> {
    parse_room_template(template, |placeholder| {
        render_room_template_placeholder(placeholder, room_name, room_url, session, room_id)
    })
}

fn parse_room_template<F>(template: &str, mut render_placeholder: F) -> AppResult<String>
where
    F: FnMut(&str) -> AppResult<String>,
{
    let mut rendered = String::with_capacity(template.len());
    let mut rest = template;

    while let Some(open) = rest.find('{') {
        let literal = &rest[..open];
        if literal.contains('}') {
            return Err(AppError::Config(
                "unmatched '}' in room submission template".into(),
            ));
        }
        rendered.push_str(literal);

        let after_open = &rest[open + 1..];
        let close = after_open
            .find('}')
            .ok_or_else(|| AppError::Config("unclosed '{' in room submission template".into()))?;
        let placeholder = &after_open[..close];
        rendered.push_str(&render_placeholder(placeholder)?);
        rest = &after_open[close + 1..];
    }

    if rest.contains('}') {
        return Err(AppError::Config(
            "unmatched '}' in room submission template".into(),
        ));
    }
    rendered.push_str(rest);
    Ok(rendered)
}

enum TemplatePlaceholder<'a> {
    Title,
    RoomName,
    RoomId,
    Url,
    StartedAt(&'a str),
}

fn parse_placeholder(placeholder: &str) -> AppResult<TemplatePlaceholder<'_>> {
    validate_placeholder_shape(placeholder)?;
    match placeholder {
        "title" | "room_title" => Ok(TemplatePlaceholder::Title),
        "room_name" | "name" => Ok(TemplatePlaceholder::RoomName),
        "room_id" => Ok(TemplatePlaceholder::RoomId),
        "url" => Ok(TemplatePlaceholder::Url),
        "started_at" => Err(AppError::Config(
            "started_at placeholder requires a format: {started_at:%Y-%m-%d %H:%M:%S}".into(),
        )),
        _ => {
            let Some(format) = placeholder.strip_prefix("started_at:") else {
                return Err(AppError::Config(format!(
                    "unknown placeholder '{{{placeholder}}}' in room submission template"
                )));
            };
            validate_started_at_format(format)?;
            Ok(TemplatePlaceholder::StartedAt(format))
        }
    }
}

fn render_room_template_placeholder(
    placeholder: &str,
    room_name: &str,
    room_url: &str,
    session: Option<&LiveSession>,
    room_id: u64,
) -> AppResult<String> {
    let title = session.map(|s| s.title.as_str()).unwrap_or(room_name);
    match parse_placeholder(placeholder)? {
        TemplatePlaceholder::Title => Ok(title.to_string()),
        TemplatePlaceholder::RoomName => Ok(room_name.to_string()),
        TemplatePlaceholder::RoomId => Ok(room_id.to_string()),
        TemplatePlaceholder::Url => Ok(room_url.to_string()),
        TemplatePlaceholder::StartedAt(format) => {
            let session = session.ok_or_else(|| {
                AppError::Config(
                    "started_at placeholder requires a persisted recording session".into(),
                )
            })?;
            format_started_at(session.started_at, format)
        }
    }
}

fn validate_placeholder_shape(placeholder: &str) -> AppResult<()> {
    if placeholder.is_empty() {
        return Err(AppError::Config(
            "empty placeholder in room submission template".into(),
        ));
    }
    if placeholder.contains('{') {
        return Err(AppError::Config(format!(
            "invalid nested placeholder '{{{placeholder}}}' in room submission template"
        )));
    }
    Ok(())
}

fn validate_started_at_format(format: &str) -> AppResult<()> {
    if format.is_empty() {
        return Err(AppError::Config(
            "started_at placeholder format must not be empty".into(),
        ));
    }
    format_started_at(jiff::Timestamp::UNIX_EPOCH, format).map(|_| ())
}

fn format_started_at(started_at: jiff::Timestamp, format: &str) -> AppResult<String> {
    let started_at = started_at.to_zoned(TimeZone::system());
    strtime::format(format, &started_at).map_err(|err| {
        AppError::Config(format!(
            "invalid started_at format in room submission template: {err}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::model::{OutputPlan, RecordingPlan, SessionLifecycle};
    use uuid::Uuid;

    fn test_live_session_with_started_at(started_at: jiff::Timestamp) -> LiveSession {
        LiveSession {
            id: Uuid::new_v4(),
            room_id: 1,
            room_name: "test-room".into(),
            title: "Test Stream".into(),
            started_at,
            lifecycle: SessionLifecycle::Open,
            recording_plan: RecordingPlan {
                credential: None,
                output_dir: "recordings".into(),
                segment_time_ms: None,
                segment_size: None,
                min_segment_size: 0,
                qn: 10_000,
                cdn: Vec::new(),
            },
            output_plan: OutputPlan::LocalOnly,
            recording_events: Vec::new(),
        }
    }

    #[test]
    fn render_room_template_formats_recording_started_at() {
        let started_at = jiff::Timestamp::from_second(1_714_998_896).unwrap();
        let session = test_live_session_with_started_at(started_at);
        let rendered = render_room_template(
            "Archive {title} at {started_at:%s}",
            "test-room",
            "https://live.bilibili.com/1",
            Some(&session),
            1,
        )
        .unwrap();

        assert_eq!(rendered, "Archive Test Stream at 1714998896");
    }

    #[test]
    fn validate_room_template_rejects_unknown_placeholder() {
        let err = validate_room_template("Archive {live_started_at:%Y-%m-%d}").unwrap_err();

        assert!(matches!(err, AppError::Config(ref msg) if msg.contains("unknown placeholder")));
    }

    #[test]
    fn validate_room_template_rejects_bad_started_at_placeholder() {
        let err = validate_room_template("Archive {started_at}").unwrap_err();

        assert!(matches!(err, AppError::Config(ref msg) if msg.contains("requires a format")));
    }

    #[test]
    fn validate_room_template_rejects_invalid_started_at_format() {
        let err = validate_room_template("Archive {started_at:%}").unwrap_err();

        assert!(
            matches!(err, AppError::Config(ref msg) if msg.contains("invalid started_at format"))
        );
    }

    #[test]
    fn validate_room_template_rejects_malformed_placeholders() {
        let unclosed = validate_room_template("Archive {title").unwrap_err();
        let unmatched = validate_room_template("Archive }").unwrap_err();

        assert!(matches!(unclosed, AppError::Config(ref msg) if msg.contains("unclosed")));
        assert!(matches!(unmatched, AppError::Config(ref msg) if msg.contains("unmatched")));
    }
}
