use crate::error::{Error, Result};

/// Normalize a topic string into a clean, canonical absolute path starting with `/`.
pub fn normalize_topic(topic: &str) -> Result<String> {
    let trimmed = topic.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidTopic {
            topic: topic.to_string(),
            reason: "topic path cannot be empty".to_string(),
        });
    }

    let mut path = if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    };

    // Collapse multiple consecutive slashes into single slashes.
    while path.contains("//") {
        path = path.replace("//", "/");
    }

    if path.len() > 1 && path.ends_with('/') {
        path.pop();
    }

    Ok(path)
}

/// Qualify a topic for a specific participant module.
/// Relative topics (e.g. `scan`) become `/{participant}/{topic}`.
/// Absolute topics (e.g. `/global/clock`) remain unchanged as `/global/clock`.
pub fn qualify_participant_topic(participant: &str, topic: &str) -> Result<String> {
    let trimmed_topic = topic.trim();
    if trimmed_topic.starts_with('/') {
        normalize_topic(trimmed_topic)
    } else {
        let trimmed_part = participant.trim().trim_matches('/');
        if trimmed_part.is_empty() {
            normalize_topic(trimmed_topic)
        } else {
            normalize_topic(&format!("{trimmed_part}/{trimmed_topic}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_normalization() {
        assert_eq!(normalize_topic("pose").unwrap(), "/pose");
        assert_eq!(normalize_topic("/perception/pose").unwrap(), "/perception/pose");
        assert_eq!(normalize_topic("//perception///pose/").unwrap(), "/perception/pose");
        assert!(normalize_topic("  ").is_err());
    }

    #[test]
    fn test_qualify_participant_topic() {
        assert_eq!(
            qualify_participant_topic("lidar", "scan").unwrap(),
            "/lidar/scan"
        );
        assert_eq!(
            qualify_participant_topic("lidar", "/clock").unwrap(),
            "/clock"
        );
    }
}
