use std::path::PathBuf;
use std::time::Duration;
use uuid::Uuid;

use crate::state::model::SegmentCloseReason;

/// Defines where segment files are written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentLayout {
    pub output_dir: PathBuf,
}

/// Defines when an active segment should rotate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentPolicy {
    pub segment_time: Option<Duration>,
    pub segment_size: Option<u64>,
}

/// Defines which finalized segments should be kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFilter {
    pub min_segment_size: u64,
}

/// Complete recorder policy assembled from independent segment concerns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecorderPolicy {
    pub layout: SegmentLayout,
    pub segment: SegmentPolicy,
    pub filter: SegmentFilter,
}

/// Represents lifecycle events for a segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentEvent {
    /// A new segment started recording.
    Started {
        session_id: Uuid,
        index: u32,
        part_path: PathBuf,
    },
    /// A segment finished recording and meets size requirements.
    Finalized {
        session_id: Uuid,
        index: u32,
        path: PathBuf,
        size: u64,
        close_reason: SegmentCloseReason,
    },
    /// A segment finished but was too small (e.g. < min_segment_size).
    Filtered {
        session_id: Uuid,
        index: u32,
        path: PathBuf,
        size: u64,
        close_reason: SegmentCloseReason,
    },
}

/// Generates the `.part` path for an actively recording segment.
/// Example: `output_dir/12345678123412341234123456789abc-0001.part`
pub fn part_path(layout: &SegmentLayout, session_id: &Uuid, index: u32) -> PathBuf {
    layout
        .output_dir
        .join(format!("{}-{:04}.part", session_id.simple(), index))
}

/// Generates the final path for a completed segment.
/// Example: `output_dir/12345678123412341234123456789abc-0001.flv`
pub fn final_path(layout: &SegmentLayout, session_id: &Uuid, index: u32) -> PathBuf {
    layout
        .output_dir
        .join(format!("{}-{:04}.flv", session_id.simple(), index))
}

/// Determines whether a segment should be rotated due to reaching the maximum size threshold.
pub fn should_rotate_by_size(current_size: u64, policy: &SegmentPolicy) -> bool {
    if let Some(max_size) = policy.segment_size {
        current_size >= max_size
    } else {
        false
    }
}

/// Determines whether a segment should be rotated due to reaching the maximum elapsed time.
pub fn should_rotate_by_elapsed(elapsed: Duration, policy: &SegmentPolicy) -> bool {
    if let Some(max_time) = policy.segment_time {
        elapsed >= max_time
    } else {
        false
    }
}

/// Determines whether a finalized segment is too small and should be filtered.
pub fn should_filter_by_size(final_size: u64, filter: &SegmentFilter) -> bool {
    final_size < filter.min_segment_size
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn test_uuid() -> Uuid {
        Uuid::from_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
    }

    #[test]
    fn test_part_path() {
        let layout = SegmentLayout {
            output_dir: PathBuf::from("/tmp"),
        };
        let path = part_path(&layout, &test_uuid(), 42);
        assert_eq!(
            path.to_str().unwrap(),
            "/tmp/550e8400e29b41d4a716446655440000-0042.part"
        );
    }

    #[test]
    fn test_final_path() {
        let layout = SegmentLayout {
            output_dir: PathBuf::from("/tmp"),
        };
        let path = final_path(&layout, &test_uuid(), 42);
        assert_eq!(
            path.to_str().unwrap(),
            "/tmp/550e8400e29b41d4a716446655440000-0042.flv"
        );
    }

    #[test]
    fn test_segment_event_identity() {
        let session_id = test_uuid();
        let index = 42;
        let event = SegmentEvent::Started {
            session_id,
            index,
            part_path: PathBuf::from("test.part"),
        };
        match event {
            SegmentEvent::Started {
                session_id: sid,
                index: idx,
                ..
            } => {
                assert_eq!(sid, session_id);
                assert_eq!(idx, index);
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_should_rotate_by_size() {
        let policy = SegmentPolicy {
            segment_time: None,
            segment_size: Some(1024),
        };

        assert!(!should_rotate_by_size(1023, &policy));
        assert!(should_rotate_by_size(1024, &policy));
        assert!(should_rotate_by_size(2048, &policy));

        let policy_no_limit = SegmentPolicy {
            segment_size: None,
            ..policy
        };
        assert!(!should_rotate_by_size(9999999, &policy_no_limit));
    }

    #[test]
    fn test_should_rotate_by_elapsed() {
        let policy = SegmentPolicy {
            segment_time: Some(Duration::from_secs(60)),
            segment_size: None,
        };

        assert!(!should_rotate_by_elapsed(Duration::from_secs(59), &policy));
        assert!(should_rotate_by_elapsed(Duration::from_secs(60), &policy));
        assert!(should_rotate_by_elapsed(Duration::from_secs(61), &policy));

        let policy_no_limit = SegmentPolicy {
            segment_time: None,
            ..policy
        };
        assert!(!should_rotate_by_elapsed(
            Duration::from_secs(999999),
            &policy_no_limit
        ));
    }

    #[test]
    fn test_should_filter_by_size() {
        let filter = SegmentFilter {
            min_segment_size: 1024,
        };

        assert!(should_filter_by_size(0, &filter));
        assert!(should_filter_by_size(1023, &filter));
        assert!(!should_filter_by_size(1024, &filter));
        assert!(!should_filter_by_size(2048, &filter));
    }
}
