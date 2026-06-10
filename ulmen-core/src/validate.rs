//! ULMEN-AGENT v1 validation.
//!
//! Validates semantic correctness of agent payloads:
//! - thread_id and id must be non-empty
//! - step must be positive and non-decreasing within a thread
//! - res records must have matching tool records
//! - enum fields must contain valid values
//! - record counts must match

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::agent::{AgentPayload, AgentRecord, FieldValue, RecordType};

// ---------------------------------------------------------------------------
// Validation error
// ---------------------------------------------------------------------------

/// Structured validation error with diagnostic context.
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub message: String,
    pub row: Option<usize>,
    pub field: Option<String>,
    pub expected: Option<String>,
    pub got: Option<String>,
    pub suggestion: Option<String>,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)?;
        if let Some(row) = self.row {
            write!(f, " | row={}", row)?;
        }
        if let Some(ref field) = self.field {
            write!(f, " | field={:?}", field)?;
        }
        if let Some(ref expected) = self.expected {
            write!(f, " | expected={:?}", expected)?;
        }
        if let Some(ref got) = self.got {
            write!(f, " | got={:?}", got)?;
        }
        if let Some(ref hint) = self.suggestion {
            write!(f, " | hint={:?}", hint)?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationError {}

impl ValidationError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            row: None,
            field: None,
            expected: None,
            got: None,
            suggestion: None,
        }
    }

    fn with_row(mut self, row: usize) -> Self {
        self.row = Some(row);
        self
    }
    fn with_field(mut self, f: impl Into<String>) -> Self {
        self.field = Some(f.into());
        self
    }
    fn with_expected(mut self, e: impl Into<String>) -> Self {
        self.expected = Some(e.into());
        self
    }
    fn with_got(mut self, g: impl Into<String>) -> Self {
        self.got = Some(g.into());
        self
    }
    fn with_suggestion(mut self, s: impl Into<String>) -> Self {
        self.suggestion = Some(s.into());
        self
    }
}

// ---------------------------------------------------------------------------
// Enum validation sets (sorted for binary search)
// ---------------------------------------------------------------------------

static ENUM_MSG_ROLE: &[&str] = &["assistant", "system", "user"];
static ENUM_TOOL_STATUS: &[&str] = &["done", "error", "pending", "running"];
static ENUM_RES_STATUS: &[&str] = &["done", "error", "timeout"];
static ENUM_PLAN_STATUS: &[&str] = &["active", "done", "pending", "skipped"];
static ENUM_COT_TYPE: &[&str] = &["compute", "conclude", "observe", "plan", "verify"];

#[inline]
fn enum_valid(set: &[&str], val: &str) -> bool {
    set.binary_search(&val).is_ok()
}

// ---------------------------------------------------------------------------
// Validate payload
// ---------------------------------------------------------------------------

/// Validate a decoded AgentPayload for semantic correctness.
///
/// Returns Ok(()) on success, Err(ValidationError) on first failure.
pub fn validate_payload(payload: &AgentPayload) -> Result<(), ValidationError> {
    let mut thread_steps: HashMap<&str, i64> = HashMap::new();
    let mut tool_ids: HashSet<&str> = HashSet::new();
    let mut res_refs: Vec<(&str, usize)> = Vec::new(); // (id, 1-based row)

    for (i, rec) in payload.records.iter().enumerate() {
        let row = i + 1;

        // thread_id must be non-empty
        if rec.thread_id.is_empty() {
            return Err(
                ValidationError::new(format!("Row {}: thread_id is empty", row))
                    .with_row(row)
                    .with_field("thread_id")
                    .with_expected("non-empty string")
                    .with_got("empty string")
                    .with_suggestion("Set thread_id to a unique thread identifier"),
            );
        }

        // id must be non-empty
        if rec.id.is_empty() {
            return Err(ValidationError::new(format!("Row {}: id is empty", row))
                .with_row(row)
                .with_field("id")
                .with_expected("non-empty string")
                .with_got("empty string")
                .with_suggestion("Set id to a unique record identifier"));
        }

        // step must be >= 1
        if rec.step < 1 {
            return Err(ValidationError::new(format!(
                "Row {}: step must be positive integer, got {}",
                row, rec.step
            ))
            .with_row(row)
            .with_field("step")
            .with_expected("positive integer >= 1")
            .with_got(format!("{}", rec.step))
            .with_suggestion("step starts at 1 and increments monotonically"));
        }

        // step must be non-decreasing within thread
        let prev = *thread_steps.get(rec.thread_id.as_str()).unwrap_or(&0);
        if rec.step < prev {
            return Err(ValidationError::new(format!(
                "Row {}: step {} is less than previous step {} in thread {:?}",
                row, rec.step, prev, rec.thread_id
            ))
            .with_row(row)
            .with_field("step")
            .with_expected(format!(">= {}", prev))
            .with_got(format!("{}", rec.step))
            .with_suggestion("steps must be non-decreasing within a thread"));
        }
        thread_steps.insert(&rec.thread_id, rec.step);

        // Track tool/res for cross-reference
        if rec.record_type == RecordType::Tool {
            tool_ids.insert(&rec.id);
        }
        if rec.record_type == RecordType::Res {
            res_refs.push((&rec.id, row));
        }

        // Enum validation
        validate_enums(rec, row)?;
    }

    // Every res must have a matching tool
    for (res_id, row) in &res_refs {
        if !tool_ids.contains(res_id) {
            return Err(ValidationError::new(format!(
                "row {}: res id {:?} has no matching tool row",
                row, res_id
            ))
            .with_field("id")
            .with_expected(format!("tool row with id={:?}", res_id))
            .with_got("no matching tool row")
            .with_suggestion(format!(
                "Add a tool row with id={:?} before the res row",
                res_id
            )));
        }
    }

    Ok(())
}

/// Validate a raw payload string. Returns (true, None) or (false, Some(error)).
pub fn validate_payload_str(text: &str) -> (bool, Option<ValidationError>) {
    match AgentPayload::decode(text) {
        Err(e) => (
            false,
            Some(
                ValidationError::new(e.to_string())
                    .with_suggestion("Check payload structure and header lines"),
            ),
        ),
        Ok(payload) => match validate_payload(&payload) {
            Ok(()) => (true, None),
            Err(e) => (false, Some(e)),
        },
    }
}

// ---------------------------------------------------------------------------
// Enum validation per record type
// ---------------------------------------------------------------------------

fn validate_enums(rec: &AgentRecord, row: usize) -> Result<(), ValidationError> {
    match rec.record_type {
        RecordType::Msg => check_str_enum(rec, 0, "role", ENUM_MSG_ROLE, row)?,
        RecordType::Tool => check_str_enum(rec, 2, "status", ENUM_TOOL_STATUS, row)?,
        RecordType::Res => check_str_enum(rec, 2, "status", ENUM_RES_STATUS, row)?,
        RecordType::Plan => check_str_enum(rec, 2, "status", ENUM_PLAN_STATUS, row)?,
        RecordType::Cot => check_str_enum(rec, 1, "cot_type", ENUM_COT_TYPE, row)?,
        _ => {}
    }
    Ok(())
}

fn check_str_enum(
    rec: &AgentRecord,
    field_idx: usize,
    field_name: &str,
    valid: &[&str],
    row: usize,
) -> Result<(), ValidationError> {
    if field_idx >= rec.fields.len() {
        return Ok(());
    }
    match &rec.fields[field_idx] {
        FieldValue::Null => Ok(()), // null is acceptable
        FieldValue::Str(val) => {
            if !enum_valid(valid, val) {
                Err(ValidationError::new(format!(
                    "Row {}: field {:?} value {:?} not in {:?}",
                    row, field_name, val, valid
                ))
                .with_row(row)
                .with_field(field_name)
                .with_expected(valid.join(" / "))
                .with_got(val.clone())
                .with_suggestion(format!("Use one of: {}", valid.join(", "))))
            } else {
                Ok(())
            }
        }
        FieldValue::Bool(b) => {
            let tok = if *b { "T" } else { "F" };
            if !enum_valid(valid, tok) {
                Err(ValidationError::new(format!(
                    "Row {}: field {:?} value {:?} not in {:?}",
                    row, field_name, tok, valid
                ))
                .with_row(row)
                .with_field(field_name))
            } else {
                Ok(())
            }
        }
        _ => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::*;

    fn msg(id: &str, tid: &str, step: i64, role: &str) -> AgentRecord {
        AgentRecord {
            record_type: RecordType::Msg,
            id: id.into(),
            thread_id: tid.into(),
            step,
            fields: vec![
                FieldValue::Str(role.into()),
                FieldValue::Int(1),
                FieldValue::Str("hi".into()),
                FieldValue::Int(1),
                FieldValue::Bool(false),
            ],
            meta: MetaFields::default(),
        }
    }

    fn tool(id: &str, step: i64) -> AgentRecord {
        AgentRecord {
            record_type: RecordType::Tool,
            id: id.into(),
            thread_id: "t1".into(),
            step,
            fields: vec![
                FieldValue::Str("search".into()),
                FieldValue::Str("{}".into()),
                FieldValue::Str("done".into()),
            ],
            meta: MetaFields::default(),
        }
    }

    fn res(id: &str, step: i64) -> AgentRecord {
        AgentRecord {
            record_type: RecordType::Res,
            id: id.into(),
            thread_id: "t1".into(),
            step,
            fields: vec![
                FieldValue::Str("search".into()),
                FieldValue::Str("result".into()),
                FieldValue::Str("done".into()),
                FieldValue::Int(100),
            ],
            meta: MetaFields::default(),
        }
    }

    fn payload(records: Vec<AgentRecord>) -> AgentPayload {
        AgentPayload {
            header: AgentHeader {
                record_count: records.len(),
                ..Default::default()
            },
            records,
        }
    }

    #[test]
    fn valid_payload() {
        let p = payload(vec![msg("m1", "t1", 1, "user")]);
        assert!(validate_payload(&p).is_ok());
    }

    #[test]
    fn empty_thread_id() {
        let p = payload(vec![msg("m1", "", 1, "user")]);
        let err = validate_payload(&p).unwrap_err();
        assert!(err.message.contains("thread_id"));
        assert_eq!(err.field.as_deref(), Some("thread_id"));
    }

    #[test]
    fn empty_id() {
        let p = payload(vec![msg("", "t1", 1, "user")]);
        let err = validate_payload(&p).unwrap_err();
        assert!(err.message.contains("id"));
    }

    #[test]
    fn step_zero() {
        let p = payload(vec![msg("m1", "t1", 0, "user")]);
        let err = validate_payload(&p).unwrap_err();
        assert_eq!(err.field.as_deref(), Some("step"));
    }

    #[test]
    fn step_backwards() {
        let p = payload(vec![
            msg("m1", "t1", 5, "user"),
            msg("m2", "t1", 3, "assistant"),
        ]);
        let err = validate_payload(&p).unwrap_err();
        assert!(err.message.contains("less than previous"));
    }

    #[test]
    fn step_same_is_ok() {
        let p = payload(vec![
            msg("m1", "t1", 1, "user"),
            msg("m2", "t1", 1, "assistant"),
        ]);
        assert!(validate_payload(&p).is_ok());
    }

    #[test]
    fn res_without_tool() {
        let p = payload(vec![res("ghost", 1)]);
        let err = validate_payload(&p).unwrap_err();
        assert!(err.message.contains("no matching tool"));
    }

    #[test]
    fn res_with_tool() {
        let p = payload(vec![tool("t1", 1), res("t1", 2)]);
        assert!(validate_payload(&p).is_ok());
    }

    #[test]
    fn bad_msg_role() {
        let p = payload(vec![msg("m1", "t1", 1, "robot")]);
        let err = validate_payload(&p).unwrap_err();
        assert!(err.message.contains("robot"));
        assert_eq!(err.field.as_deref(), Some("role"));
    }

    #[test]
    fn valid_msg_roles() {
        for role in &["user", "assistant", "system"] {
            let p = payload(vec![msg("m1", "t1", 1, role)]);
            assert!(
                validate_payload(&p).is_ok(),
                "role {:?} should be valid",
                role
            );
        }
    }

    #[test]
    fn validate_str_valid() {
        let text = "ULMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|1|user|1|hi|1|F\n";
        let (ok, err) = validate_payload_str(text);
        assert!(ok, "err: {:?}", err);
    }

    #[test]
    fn validate_str_bad_magic() {
        let (ok, err) = validate_payload_str("GARBAGE\nrecords: 0\n");
        assert!(!ok);
        assert!(err.unwrap().message.contains("bad magic"));
    }

    #[test]
    fn validate_str_bad_step() {
        let text = "ULMEN-AGENT v1\nrecords: 1\nmsg|m1|t1|0|user|1|hi|1|F\n";
        let (ok, _) = validate_payload_str(text);
        assert!(!ok);
    }

    #[test]
    fn multiple_threads_independent() {
        let p = payload(vec![
            msg("m1", "t1", 5, "user"),
            msg("m2", "t2", 1, "user"), // different thread, lower step is ok
        ]);
        assert!(validate_payload(&p).is_ok());
    }
}
