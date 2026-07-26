use super::reset_time;
use crate::harness::{LimitState, LimitUpdate, ParseOutcome, TranscriptParser};
use chrono::{DateTime, Local};
use serde_json::Value;

pub struct ClaudeTranscriptParser;

pub(super) fn limit_update_from_message(
    event_time: DateTime<Local>,
    content_text: &str,
    now: DateTime<Local>,
) -> Option<LimitUpdate> {
    let (target_time, display) = reset_time::parse_reset_time(event_time, content_text)?;
    let state = if target_time > now {
        LimitState::Locked {
            target_time,
            display,
        }
    } else {
        LimitState::Clear
    };
    Some(LimitUpdate { event_time, state })
}

fn parse_rate_limit_line(line: &str, now: DateTime<Local>) -> Option<LimitUpdate> {
    if !line.contains("rate_limit") {
        return None;
    }
    let value = serde_json::from_str::<Value>(line).ok()?;
    if value["error"].as_str() != Some("rate_limit") {
        return None;
    }
    let event_time = DateTime::parse_from_rfc3339(value["timestamp"].as_str()?)
        .ok()?
        .with_timezone(&Local);
    let message = value["message"]["content"][0]["text"].as_str()?;
    limit_update_from_message(event_time, message, now)
}

impl TranscriptParser for ClaudeTranscriptParser {
    fn parse_line(&self, line: &str, now: DateTime<Local>) -> ParseOutcome {
        parse_rate_limit_line(line, now)
            .map(ParseOutcome::Update)
            .unwrap_or(ParseOutcome::Ignored)
    }
}

#[cfg(test)]
mod tests {
    use super::ClaudeTranscriptParser;
    use crate::harness::{LimitState, ParseOutcome, TranscriptParser};
    use chrono::{DateTime, Datelike, Duration, Local, TimeZone};

    const ROW: &str = r#"{"timestamp":"2026-07-12T23:00:00-04:00","error":"rate_limit","message":{"content":[{"type":"text","text":"Claude limit reached; resets 2am"}]}}"#;

    fn event_time() -> DateTime<Local> {
        DateTime::parse_from_rfc3339("2026-07-12T23:00:00-04:00")
            .unwrap()
            .with_timezone(&Local)
    }

    #[test]
    fn future_limit_retains_event_and_target_times() {
        let event_time = event_time();
        let now = event_time + Duration::minutes(30);
        let ParseOutcome::Update(limit) = ClaudeTranscriptParser.parse_line(ROW, now) else {
            panic!("expected parsed rate-limit event");
        };
        assert_eq!(limit.event_time, event_time);
        let LimitState::Locked { target_time, .. } = limit.state else {
            panic!("expected locked state");
        };
        let mut expected_target = Local
            .with_ymd_and_hms(
                event_time.year(),
                event_time.month(),
                event_time.day(),
                2,
                0,
                5,
            )
            .unwrap();
        if expected_target <= event_time {
            expected_target += Duration::days(1);
        }
        assert_eq!(target_time, expected_target);
    }

    #[test]
    fn expired_limit_emits_clear_at_injected_now() {
        let now = event_time() + Duration::days(1);
        let ParseOutcome::Update(limit) = ClaudeTranscriptParser.parse_line(ROW, now) else {
            panic!("expected parsed rate-limit event");
        };
        assert_eq!(limit.state, LimitState::Clear);
    }

    #[test]
    fn newest_valid_row_wins_by_event_time() {
        let now = event_time() - Duration::hours(3);
        let content = concat!(
            "{\"timestamp\":\"2026-07-12T20:00:00-04:00\",\"error\":\"rate_limit\",\"message\":{\"content\":[{\"text\":\"Claude limit reached; resets Jul 14 at 10am\"}]}}\n",
            "{\"timestamp\":\"2026-07-12T23:00:00-04:00\",\"error\":\"rate_limit\",\"message\":{\"content\":[{\"text\":\"Claude limit reached; resets 11pm\"}]}}\n"
        );
        let update = content
            .lines()
            .filter_map(|line| match ClaudeTranscriptParser.parse_line(line, now) {
                ParseOutcome::Update(update) => Some(update),
                ParseOutcome::Ignored | ParseOutcome::Diagnostic(_) => None,
            })
            .max_by_key(|update| update.event_time)
            .expect("expected newest event");
        assert_eq!(update.event_time, event_time());
    }
}
