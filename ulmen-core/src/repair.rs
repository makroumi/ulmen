//! LLM output repair: parse and fix malformed ULMEN-AGENT output.

use crate::agent::*;
use crate::chunk::make_error_payload;

/// Parse raw LLM output and return a valid AgentPayload.
/// Applies auto-repair: strips fences, finds magic, fixes record count,
/// removes blank lines, skips unknown types.
pub fn parse_llm_output(
    raw_text: &str,
    thread_id: Option<&str>,
    strict: bool,
) -> Result<AgentPayload, String> {
    let repair_tid = thread_id.unwrap_or("REPAIR");

    // Strip markdown fences
    let cleaned: String = raw_text
        .lines()
        .filter(|l| !l.trim().starts_with("```"))
        .collect::<Vec<_>>()
        .join("\n");
    let cleaned = cleaned.trim();

    // Find magic line
    let all_lines: Vec<&str> = cleaned.lines().collect();
    let magic_idx = all_lines.iter().position(|l| l.trim() == AGENT_MAGIC);

    let mi = match magic_idx {
        Some(i) => i,
        None => {
            let msg = format!("No '{}' magic line found in LLM output", AGENT_MAGIC);
            if strict {
                return Err(msg);
            }
            return Ok(make_error_payload(&msg, repair_tid));
        }
    };

    let work_lines = &all_lines[mi..];

    // Separate header from data
    let mut header_lines: Vec<String> = Vec::new();
    let mut data_lines: Vec<String> = Vec::new();
    let mut past_records = false;

    let header_prefixes = [
        "thread: ",
        "context_window: ",
        "context_used: ",
        "payload_id: ",
        "parent_payload_id: ",
        "agent_id: ",
        "session_id: ",
        "schema_version: ",
        "meta: ",
        "records: ",
    ];

    for &line in &work_lines[1..] {
        let stripped = line.trim();
        if stripped.is_empty() {
            continue;
        }

        if !past_records && header_prefixes.iter().any(|p| stripped.starts_with(p)) {
            header_lines.push(stripped.to_string());
            if stripped.starts_with("records: ") {
                past_records = true;
            }
        } else if is_data_line(stripped) {
            data_lines.push(stripped.to_string());
        } else if !past_records {
            header_lines.push(stripped.to_string());
        }
    }

    // Fix record count
    let actual_count = data_lines.len();
    let mut found_records = false;
    for h in header_lines.iter_mut() {
        if h.starts_with("records: ") {
            *h = format!("records: {}", actual_count);
            found_records = true;
        }
    }
    if !found_records {
        header_lines.push(format!("records: {}", actual_count));
    }

    // Reassemble and try to decode
    let mut repaired = String::with_capacity(256);
    repaired.push_str(AGENT_MAGIC);
    repaired.push('\n');
    for h in &header_lines {
        repaired.push_str(h);
        repaired.push('\n');
    }
    for d in &data_lines {
        repaired.push_str(d);
        repaired.push('\n');
    }

    // Try decoding the repaired payload
    match AgentPayload::decode(&repaired) {
        Ok(payload) => return Ok(payload),
        Err(_) => {}
    }

    // Last resort: decode individual rows, keep good ones
    let meta_fields = extract_meta_fields(&header_lines);
    let meta_refs: Vec<&str> = meta_fields.iter().map(|s| s.as_str()).collect();

    let mut good_records: Vec<AgentRecord> = Vec::new();
    for row in &data_lines {
        if let Ok(rec) = decode_record_inner(row, &meta_refs) {
            good_records.push(rec);
        }
    }

    if good_records.is_empty() {
        let msg = "Could not repair LLM output";
        if strict {
            return Err(msg.to_string());
        }
        return Ok(make_error_payload(msg, repair_tid));
    }

    Ok(AgentPayload {
        header: AgentHeader {
            thread_id: thread_id.map(String::from),
            meta_fields,
            record_count: good_records.len(),
            ..Default::default()
        },
        records: good_records,
    })
}

fn is_data_line(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let first = s.split('|').next().unwrap_or("").trim();
    RecordType::from_str(first).is_some()
}

fn extract_meta_fields(header_lines: &[String]) -> Vec<String> {
    for h in header_lines {
        if let Some(v) = h.strip_prefix("meta: ") {
            return v
                .split(',')
                .map(|f| f.trim().to_string())
                .filter(|f| !f.is_empty())
                .collect();
        }
    }
    Vec::new()
}

/// Internal record decoder (duplicates logic from agent.rs to avoid circular dep).
fn decode_record_inner(line: &str, meta_field_names: &[&str]) -> Result<AgentRecord, AgentError> {
    // Delegate to the public decode in agent module
    crate::agent::decode_record_public(line, meta_field_names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repair_valid_passthrough() {
        let text = "ULMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|1|user|1|hi|1|F\n";
        let result = parse_llm_output(text, None, false).unwrap();
        assert_eq!(result.records.len(), 1);
    }

    #[test]
    fn repair_strips_fences() {
        let text = "```\nULMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|1|user|1|hi|1|F\n```\n";
        let result = parse_llm_output(text, None, false).unwrap();
        assert_eq!(result.records.len(), 1);
    }

    #[test]
    fn repair_fixes_count() {
        let text = "ULMEN-AGENT v1\nrecords: 99\nmsg|m1|t1|1|user|1|hi|1|F\n";
        let result = parse_llm_output(text, None, false).unwrap();
        assert_eq!(result.records.len(), 1);
    }

    #[test]
    fn repair_no_magic_returns_error() {
        let result = parse_llm_output("just some text", None, false).unwrap();
        assert_eq!(result.records[0].record_type, RecordType::Err);
    }

    #[test]
    fn repair_strict_no_magic_errors() {
        let result = parse_llm_output("just some text", None, true);
        assert!(result.is_err());
    }

    #[test]
    fn repair_skips_blank_lines() {
        let text = "ULMEN-AGENT v1\nrecords: 1\n\nmsg|m1|t1|1|user|1|hi|1|F\n\n";
        let result = parse_llm_output(text, None, false).unwrap();
        assert_eq!(result.records.len(), 1);
    }
}
