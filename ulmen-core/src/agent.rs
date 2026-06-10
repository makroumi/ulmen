//! ULMEN-AGENT v1 protocol: native Rust types, encode, decode.
//!
//! All types are plain Rust structs with no PyO3 dependency.
//! The encoder produces byte-identical output to the Python reference.

use std::fmt;
use std::fmt::Write;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const AGENT_MAGIC: &str = "ULMEN-AGENT v1";
pub const AGENT_VERSION: &str = "1.0.0";

// ---------------------------------------------------------------------------
// Field value -- the fundamental cell type
// ---------------------------------------------------------------------------

/// A single field value in an agent record.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

impl FieldValue {
    /// Encode to ULMEN-AGENT wire format, appending to the buffer.
    #[inline]
    pub fn encode_into(&self, out: &mut String) {
        match self {
            FieldValue::Null => out.push('N'),
            FieldValue::Bool(true) => out.push('T'),
            FieldValue::Bool(false) => out.push('F'),
            FieldValue::Int(i) => {
                let _ = write!(out, "{}", i);
            }
            FieldValue::Float(f) => {
                if f.is_nan() {
                    out.push_str("nan");
                } else if f.is_infinite() {
                    out.push_str(if *f > 0.0 { "inf" } else { "-inf" });
                } else {
                    let _ = write!(out, "{:?}", f);
                }
            }
            FieldValue::Str(s) => encode_str_into(s, out),
        }
    }

    /// Decode from a ULMEN-AGENT token with known type hint.
    pub fn decode(tok: &str, type_char: u8) -> Result<Self, AgentError> {
        match tok {
            "N" => return Ok(FieldValue::Null),
            "T" => return Ok(FieldValue::Bool(true)),
            "F" => return Ok(FieldValue::Bool(false)),
            "$0=" => return Ok(FieldValue::Str(String::new())),
            _ => {}
        }

        // Quoted string
        if tok.len() >= 2 && tok.starts_with('"') && tok.ends_with('"') {
            let inner = &tok[1..tok.len() - 1];
            return Ok(FieldValue::Str(unescape(inner)));
        }

        match type_char {
            b'd' => {
                let i: i64 = tok
                    .parse()
                    .map_err(|_| AgentError::FieldDecode(format!("invalid int: {:?}", tok)))?;
                Ok(FieldValue::Int(i))
            }
            b'f' => {
                let f: f64 = match tok {
                    "nan" => f64::NAN,
                    "inf" => f64::INFINITY,
                    "-inf" => f64::NEG_INFINITY,
                    _ => tok.parse().map_err(|_| {
                        AgentError::FieldDecode(format!("invalid float: {:?}", tok))
                    })?,
                };
                Ok(FieldValue::Float(f))
            }
            b'b' => match tok {
                "T" => Ok(FieldValue::Bool(true)),
                "F" => Ok(FieldValue::Bool(false)),
                _ => Err(AgentError::FieldDecode(format!("invalid bool: {:?}", tok))),
            },
            _ => Ok(FieldValue::Str(tok.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// Record types
// ---------------------------------------------------------------------------

/// The 10 ULMEN-AGENT record types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecordType {
    Msg,
    Tool,
    Res,
    Plan,
    Obs,
    Err,
    Mem,
    Rag,
    Hyp,
    Cot,
}

impl RecordType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Msg => "msg",
            Self::Tool => "tool",
            Self::Res => "res",
            Self::Plan => "plan",
            Self::Obs => "obs",
            Self::Err => "err",
            Self::Mem => "mem",
            Self::Rag => "rag",
            Self::Hyp => "hyp",
            Self::Cot => "cot",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "msg" => Some(Self::Msg),
            "tool" => Some(Self::Tool),
            "res" => Some(Self::Res),
            "plan" => Some(Self::Plan),
            "obs" => Some(Self::Obs),
            "err" => Some(Self::Err),
            "mem" => Some(Self::Mem),
            "rag" => Some(Self::Rag),
            "hyp" => Some(Self::Hyp),
            "cot" => Some(Self::Cot),
            _ => None,
        }
    }

    /// Schema: list of (field_name, type_char, required).
    /// Schema: list of (field_name, type_char, required).
    pub fn schema(&self) -> &'static [FieldDef] {
        static MSG: [FieldDef; 5] = [
            fd("role", b's', true),
            fd("turn", b'd', true),
            fd("content", b's', true),
            fd("tokens", b'd', true),
            fd("flagged", b'b', true),
        ];
        static TOOL: [FieldDef; 3] = [
            fd("name", b's', true),
            fd("args", b's', true),
            fd("status", b's', true),
        ];
        static RES: [FieldDef; 4] = [
            fd("name", b's', true),
            fd("data", b's', false),
            fd("status", b's', true),
            fd("latency_ms", b'd', true),
        ];
        static PLAN: [FieldDef; 3] = [
            fd("index", b'd', true),
            fd("description", b's', true),
            fd("status", b's', true),
        ];
        static OBS: [FieldDef; 3] = [
            fd("source", b's', true),
            fd("content", b's', true),
            fd("confidence", b'f', true),
        ];
        static ERR: [FieldDef; 4] = [
            fd("code", b's', true),
            fd("message", b's', true),
            fd("source", b's', true),
            fd("recoverable", b'b', true),
        ];
        static MEM: [FieldDef; 4] = [
            fd("key", b's', true),
            fd("value", b's', true),
            fd("confidence", b'f', true),
            fd("ttl", b'd', false),
        ];
        static RAG: [FieldDef; 5] = [
            fd("rank", b'd', true),
            fd("score", b'f', true),
            fd("source", b's', true),
            fd("chunk", b's', true),
            fd("used", b'b', true),
        ];
        static HYP: [FieldDef; 4] = [
            fd("statement", b's', true),
            fd("evidence", b's', true),
            fd("score", b'f', true),
            fd("accepted", b'b', true),
        ];
        static COT: [FieldDef; 4] = [
            fd("index", b'd', true),
            fd("cot_type", b's', true),
            fd("text", b's', true),
            fd("confidence", b'f', true),
        ];
        match self {
            Self::Msg => &MSG,
            Self::Tool => &TOOL,
            Self::Res => &RES,
            Self::Plan => &PLAN,
            Self::Obs => &OBS,
            Self::Err => &ERR,
            Self::Mem => &MEM,
            Self::Rag => &RAG,
            Self::Hyp => &HYP,
            Self::Cot => &COT,
        }
    }

    /// Total field count: 4 common + type-specific.
    pub fn field_count(&self) -> usize {
        4 + self.schema().len()
    }
}

// ---------------------------------------------------------------------------
// Schema field definition
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct FieldDef {
    pub name: &'static str,
    pub type_char: u8,
    pub required: bool,
}

const fn fd(name: &'static str, type_char: u8, required: bool) -> FieldDef {
    FieldDef {
        name,
        type_char,
        required,
    }
}

// ---------------------------------------------------------------------------
// Meta fields
// ---------------------------------------------------------------------------

/// Optional meta fields appended after type-specific fields.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MetaFields {
    pub parent_id: Option<String>,
    pub from_agent: Option<String>,
    pub to_agent: Option<String>,
    pub priority: Option<i64>,
}

pub const META_FIELD_NAMES: &[&str] = &["parent_id", "from_agent", "to_agent", "priority"];

// ---------------------------------------------------------------------------
// Agent record
// ---------------------------------------------------------------------------

/// A single ULMEN-AGENT record with native Rust types.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentRecord {
    pub record_type: RecordType,
    pub id: String,
    pub thread_id: String,
    pub step: i64,
    /// Type-specific fields in schema order.
    pub fields: Vec<FieldValue>,
    pub meta: MetaFields,
}

impl AgentRecord {
    /// Encode this record to a pipe-delimited row.
    /// Writes directly into the provided buffer for zero-allocation encoding.
    pub fn encode_into(&self, out: &mut String, meta_field_names: &[&str]) {
        out.push_str(self.record_type.as_str());
        out.push('|');
        encode_str_into(&self.id, out);
        out.push('|');
        encode_str_into(&self.thread_id, out);
        out.push('|');
        let _ = write!(out, "{}", self.step);

        for fv in &self.fields {
            out.push('|');
            fv.encode_into(out);
        }

        for mf_name in meta_field_names {
            out.push('|');
            let val = match *mf_name {
                "parent_id" => self
                    .meta
                    .parent_id
                    .as_deref()
                    .map(|s| FieldValue::Str(s.to_string()))
                    .unwrap_or(FieldValue::Null),
                "from_agent" => self
                    .meta
                    .from_agent
                    .as_deref()
                    .map(|s| FieldValue::Str(s.to_string()))
                    .unwrap_or(FieldValue::Null),
                "to_agent" => self
                    .meta
                    .to_agent
                    .as_deref()
                    .map(|s| FieldValue::Str(s.to_string()))
                    .unwrap_or(FieldValue::Null),
                "priority" => self
                    .meta
                    .priority
                    .map(FieldValue::Int)
                    .unwrap_or(FieldValue::Null),
                _ => FieldValue::Null,
            };
            val.encode_into(out);
        }
    }

    /// Encode to a standalone string.
    pub fn encode(&self, meta_field_names: &[&str]) -> String {
        let mut out = String::with_capacity(128);
        self.encode_into(&mut out, meta_field_names);
        out
    }
}

// ---------------------------------------------------------------------------
// Agent header
// ---------------------------------------------------------------------------

/// Parsed ULMEN-AGENT payload header.
#[derive(Debug, Clone, Default)]
pub struct AgentHeader {
    pub thread_id: Option<String>,
    pub context_window: Option<i64>,
    pub context_used: Option<i64>,
    pub payload_id: Option<String>,
    pub parent_payload_id: Option<String>,
    pub agent_id: Option<String>,
    pub session_id: Option<String>,
    pub schema_version: Option<String>,
    pub meta_fields: Vec<String>,
    pub record_count: usize,
}

impl AgentHeader {
    /// Encode header lines (excluding magic, including records:).
    pub fn encode_lines(&self) -> Vec<String> {
        let mut lines = Vec::with_capacity(10);
        if let Some(ref t) = self.thread_id {
            lines.push(format!("thread: {}", t));
        }
        if let Some(cw) = self.context_window {
            lines.push(format!("context_window: {}", cw));
        }
        if let Some(cu) = self.context_used {
            lines.push(format!("context_used: {}", cu));
        }
        if let Some(ref p) = self.payload_id {
            lines.push(format!("payload_id: {}", p));
        }
        if let Some(ref p) = self.parent_payload_id {
            lines.push(format!("parent_payload_id: {}", p));
        }
        if let Some(ref a) = self.agent_id {
            lines.push(format!("agent_id: {}", a));
        }
        if let Some(ref s) = self.session_id {
            lines.push(format!("session_id: {}", s));
        }
        if let Some(ref v) = self.schema_version {
            lines.push(format!("schema_version: {}", v));
        }
        if !self.meta_fields.is_empty() {
            lines.push(format!("meta: {}", self.meta_fields.join(",")));
        }
        lines.push(format!("records: {}", self.record_count));
        lines
    }
}

// ---------------------------------------------------------------------------
// Agent payload
// ---------------------------------------------------------------------------

/// A complete ULMEN-AGENT v1 payload: header + records.
#[derive(Debug, Clone)]
pub struct AgentPayload {
    pub header: AgentHeader,
    pub records: Vec<AgentRecord>,
}

impl AgentPayload {
    /// Encode to a complete ULMEN-AGENT v1 payload string.
    pub fn encode(&self) -> String {
        let meta_refs: Vec<&str> = self.header.meta_fields.iter().map(|s| s.as_str()).collect();

        // Pre-encode records to estimate capacity.
        let mut data_lines: Vec<String> = Vec::with_capacity(self.records.len());
        for rec in &self.records {
            data_lines.push(rec.encode(&meta_refs));
        }

        let header_lines = self.header.encode_lines();

        let total_cap = AGENT_MAGIC.len()
            + 1
            + header_lines.iter().map(|l| l.len() + 1).sum::<usize>()
            + data_lines.iter().map(|l| l.len() + 1).sum::<usize>();

        let mut out = String::with_capacity(total_cap);
        out.push_str(AGENT_MAGIC);
        out.push('\n');
        for line in &header_lines {
            out.push_str(line);
            out.push('\n');
        }
        for line in &data_lines {
            out.push_str(line);
            out.push('\n');
        }
        out
    }

    /// Decode a ULMEN-AGENT v1 payload string.
    pub fn decode(text: &str) -> Result<Self, AgentError> {
        let trimmed = text.trim_end_matches('\n');
        let all_lines: Vec<&str> = trimmed.split('\n').collect();

        if all_lines.len() < 2 {
            return Err(AgentError::Parse("payload too short".into()));
        }
        if all_lines[0] != AGENT_MAGIC {
            return Err(AgentError::Parse(format!(
                "bad magic: expected {:?}, got {:?}",
                AGENT_MAGIC, all_lines[0]
            )));
        }

        let header = parse_header(&all_lines[1..])?;
        let data_start = 1 + header.lines_consumed;

        let data_lines = &all_lines[data_start..];
        if data_lines.len() != header.record_count {
            return Err(AgentError::Parse(format!(
                "record count mismatch: declared {}, found {}",
                header.record_count,
                data_lines.len()
            )));
        }

        let meta_refs: Vec<&str> = header.meta_fields.iter().map(|s| s.as_str()).collect();

        let mut records = Vec::with_capacity(header.record_count);
        for (i, line) in data_lines.iter().enumerate() {
            if line.is_empty() {
                return Err(AgentError::Parse(format!("blank line at row {}", i + 1)));
            }
            let rec = decode_record(line, &meta_refs)
                .map_err(|e| AgentError::Parse(format!("row {}: {}", i + 1, e)))?;
            records.push(rec);
        }

        Ok(AgentPayload {
            header: AgentHeader {
                thread_id: header.thread_id,
                context_window: header.context_window,
                context_used: header.context_used,
                payload_id: header.payload_id,
                parent_payload_id: header.parent_payload_id,
                agent_id: header.agent_id,
                session_id: header.session_id,
                schema_version: header.schema_version,
                meta_fields: header.meta_fields,
                record_count: header.record_count,
            },
            records,
        })
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum AgentError {
    Parse(String),
    FieldDecode(String),
    Validation(String),
}

impl fmt::Display for AgentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AgentError::Parse(m) => write!(f, "parse error: {}", m),
            AgentError::FieldDecode(m) => write!(f, "field decode error: {}", m),
            AgentError::Validation(m) => write!(f, "validation error: {}", m),
        }
    }
}

impl std::error::Error for AgentError {}

// ---------------------------------------------------------------------------
// String encoding helpers
// ---------------------------------------------------------------------------

/// Encode a string value into the buffer with ULMEN-AGENT escaping.
#[inline]
pub fn encode_str_into(s: &str, out: &mut String) {
    if s.is_empty() {
        out.push_str("$0=");
        return;
    }
    if needs_quoting(s) {
        out.push('"');
        for c in s.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\"\""),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                _ => out.push(c),
            }
        }
        out.push('"');
    } else {
        out.push_str(s);
    }
}

#[inline]
fn needs_quoting(s: &str) -> bool {
    s.bytes()
        .any(|b| matches!(b, b'|' | b'"' | b'\\' | b'\n' | b'\r'))
}

/// Unescape a quoted ULMEN-AGENT string (inner content, without outer quotes).
fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => out.push('\\'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some(c2) => {
                    out.push('\\');
                    out.push(c2);
                }
                None => out.push('\\'),
            }
        } else if c == '"' && chars.peek() == Some(&'"') {
            out.push('"');
            chars.next();
        } else {
            out.push(c);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Row splitter -- pipe-aware, handles quoted fields
// ---------------------------------------------------------------------------

fn split_row(line: &str) -> Vec<&str> {
    if !line.contains('"') {
        return line.split('|').collect();
    }

    let bytes = line.as_bytes();
    let n = bytes.len();
    let mut fields: Vec<&str> = Vec::with_capacity(16);
    let mut i = 0;

    while i < n {
        if bytes[i] == b'"' {
            let start = i;
            i += 1;
            while i < n {
                if bytes[i] == b'"' {
                    if i + 1 < n && bytes[i + 1] == b'"' {
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            fields.push(&line[start..i]);
            if i < n && bytes[i] == b'|' {
                i += 1;
                if i == n {
                    fields.push("");
                }
            }
        } else {
            let start = i;
            while i < n && bytes[i] != b'|' {
                i += 1;
            }
            fields.push(&line[start..i]);
            if i < n && bytes[i] == b'|' {
                i += 1;
                if i == n {
                    fields.push("");
                }
            }
        }
    }
    fields
}

// ---------------------------------------------------------------------------
// Record decoder
// ---------------------------------------------------------------------------

fn decode_record(line: &str, meta_field_names: &[&str]) -> Result<AgentRecord, AgentError> {
    let tokens = split_row(line);
    if tokens.is_empty() {
        return Err(AgentError::Parse("empty row".into()));
    }

    let rtype = RecordType::parse(tokens[0])
        .ok_or_else(|| AgentError::Parse(format!("unknown type: {:?}", tokens[0])))?;

    let schema = rtype.schema();
    let expected = rtype.field_count() + meta_field_names.len();

    if tokens.len() != expected {
        return Err(AgentError::Parse(format!(
            "type {:?} expects {} fields, got {}",
            tokens[0],
            expected,
            tokens.len()
        )));
    }

    let id = FieldValue::decode(tokens[1], b's')?;
    let thread_id = FieldValue::decode(tokens[2], b's')?;
    let step = FieldValue::decode(tokens[3], b'd')?;

    let id_str = match id {
        FieldValue::Str(s) => s,
        FieldValue::Null => String::new(),
        _ => return Err(AgentError::FieldDecode("id must be string".into())),
    };
    let tid_str = match thread_id {
        FieldValue::Str(s) => s,
        FieldValue::Null => String::new(),
        _ => return Err(AgentError::FieldDecode("thread_id must be string".into())),
    };
    let step_val = match step {
        FieldValue::Int(i) => i,
        FieldValue::Null => 0,
        _ => return Err(AgentError::FieldDecode("step must be int".into())),
    };

    let mut fields = Vec::with_capacity(schema.len());
    for (i, fd) in schema.iter().enumerate() {
        let tok = tokens[4 + i];
        if fd.required && tok == "N" {
            return Err(AgentError::Parse(format!(
                "required field {:?} is null",
                fd.name
            )));
        }
        fields.push(FieldValue::decode(tok, fd.type_char)?);
    }

    let mut meta = MetaFields::default();
    let meta_offset = rtype.field_count();
    for (i, mf_name) in meta_field_names.iter().enumerate() {
        let tok = tokens[meta_offset + i];
        match *mf_name {
            "parent_id" => {
                if tok != "N" {
                    meta.parent_id = Some(FieldValue::decode(tok, b's')?.into_string());
                }
            }
            "from_agent" => {
                if tok != "N" {
                    meta.from_agent = Some(FieldValue::decode(tok, b's')?.into_string());
                }
            }
            "to_agent" => {
                if tok != "N" {
                    meta.to_agent = Some(FieldValue::decode(tok, b's')?.into_string());
                }
            }
            "priority" => {
                if tok != "N" {
                    if let FieldValue::Int(i) = FieldValue::decode(tok, b'd')? {
                        meta.priority = Some(i);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(AgentRecord {
        record_type: rtype,
        id: id_str,
        thread_id: tid_str,
        step: step_val,
        fields,
        meta,
    })
}

/// Public record decoder for use by repair module.
pub fn decode_record_public(
    line: &str,
    meta_field_names: &[&str],
) -> Result<AgentRecord, AgentError> {
    decode_record(line, meta_field_names)
}

// ---------------------------------------------------------------------------
// Header parser
// ---------------------------------------------------------------------------

struct ParsedHeaderInner {
    thread_id: Option<String>,
    context_window: Option<i64>,
    context_used: Option<i64>,
    payload_id: Option<String>,
    parent_payload_id: Option<String>,
    agent_id: Option<String>,
    session_id: Option<String>,
    schema_version: Option<String>,
    meta_fields: Vec<String>,
    record_count: usize,
    lines_consumed: usize,
}

fn parse_header(lines: &[&str]) -> Result<ParsedHeaderInner, AgentError> {
    let mut h = ParsedHeaderInner {
        thread_id: None,
        context_window: None,
        context_used: None,
        payload_id: None,
        parent_payload_id: None,
        agent_id: None,
        session_id: None,
        schema_version: None,
        meta_fields: Vec::new(),
        record_count: 0,
        lines_consumed: 0,
    };

    let mut idx = 0;
    while idx < lines.len() {
        let line = lines[idx];

        if let Some(v) = line.strip_prefix("thread: ") {
            h.thread_id = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("context_window: ") {
            h.context_window = Some(
                v.trim()
                    .parse()
                    .map_err(|_| AgentError::Parse(format!("bad context_window: {:?}", line)))?,
            );
        } else if let Some(v) = line.strip_prefix("context_used: ") {
            h.context_used = Some(
                v.trim()
                    .parse()
                    .map_err(|_| AgentError::Parse(format!("bad context_used: {:?}", line)))?,
            );
        } else if let Some(v) = line.strip_prefix("payload_id: ") {
            h.payload_id = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("parent_payload_id: ") {
            h.parent_payload_id = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("agent_id: ") {
            h.agent_id = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("session_id: ") {
            h.session_id = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("schema_version: ") {
            h.schema_version = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("meta: ") {
            h.meta_fields = v
                .split(',')
                .map(|f| f.trim().to_string())
                .filter(|f| !f.is_empty())
                .collect();
            let valid = ["parent_id", "from_agent", "to_agent", "priority"];
            for f in &h.meta_fields {
                if !valid.contains(&f.as_str()) {
                    return Err(AgentError::Parse(format!("unknown meta field: {:?}", f)));
                }
            }
        } else if let Some(v) = line.strip_prefix("records: ") {
            h.record_count = v
                .trim()
                .parse()
                .map_err(|_| AgentError::Parse(format!("bad record count: {:?}", line)))?;
            idx += 1;
            h.lines_consumed = idx;
            return Ok(h);
        }
        // Forward compatibility: unknown lines silently ignored

        idx += 1;
    }

    Err(AgentError::Parse("records: not found".into()))
}

// ---------------------------------------------------------------------------
// FieldValue helper
// ---------------------------------------------------------------------------

impl FieldValue {
    fn into_string(self) -> String {
        match self {
            FieldValue::Str(s) => s,
            FieldValue::Null => String::new(),
            FieldValue::Bool(b) => (if b { "T" } else { "F" }).to_string(),
            FieldValue::Int(i) => i.to_string(),
            FieldValue::Float(f) => format!("{:?}", f),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_msg(id: &str, step: i64, content: &str) -> AgentRecord {
        AgentRecord {
            record_type: RecordType::Msg,
            id: id.to_string(),
            thread_id: "t1".to_string(),
            step,
            fields: vec![
                FieldValue::Str("user".into()),  // role
                FieldValue::Int(1),              // turn
                FieldValue::Str(content.into()), // content
                FieldValue::Int(5),              // tokens
                FieldValue::Bool(false),         // flagged
            ],
            meta: MetaFields::default(),
        }
    }

    #[test]
    fn record_type_roundtrip() {
        for rt in &[
            RecordType::Msg,
            RecordType::Tool,
            RecordType::Res,
            RecordType::Plan,
            RecordType::Obs,
            RecordType::Err,
            RecordType::Mem,
            RecordType::Rag,
            RecordType::Hyp,
            RecordType::Cot,
        ] {
            assert_eq!(RecordType::parse(rt.as_str()), Some(*rt));
        }
    }

    #[test]
    fn record_type_unknown() {
        assert_eq!(RecordType::parse("xyz"), None);
    }

    #[test]
    fn encode_simple_record() {
        let rec = make_msg("m1", 1, "hello");
        let encoded = rec.encode(&[]);
        assert_eq!(encoded, "msg|m1|t1|1|user|1|hello|5|F");
    }

    #[test]
    fn encode_special_chars() {
        let rec = make_msg("m1", 1, "hello|world");
        let encoded = rec.encode(&[]);
        assert!(encoded.contains("\"hello|world\""));
    }

    #[test]
    fn encode_newline() {
        let rec = make_msg("m1", 1, "line1\nline2");
        let encoded = rec.encode(&[]);
        assert!(encoded.contains("\"line1\\nline2\""));
    }

    #[test]
    fn encode_empty_string() {
        let rec = make_msg("m1", 1, "");
        let encoded = rec.encode(&[]);
        assert!(encoded.contains("$0="));
    }

    #[test]
    fn encode_null_field() {
        let mut rec = make_msg("m1", 1, "hi");
        rec.fields[3] = FieldValue::Null; // tokens
        let encoded = rec.encode(&[]);
        // Should have N in the tokens position
        let parts: Vec<&str> = encoded.split('|').collect();
        assert_eq!(parts[7], "N");
    }

    #[test]
    fn payload_encode_decode_roundtrip() {
        let payload = AgentPayload {
            header: AgentHeader {
                thread_id: Some("t1".into()),
                record_count: 2,
                ..Default::default()
            },
            records: vec![make_msg("m1", 1, "hello"), make_msg("m2", 2, "world")],
        };

        let encoded = payload.encode();
        assert!(encoded.starts_with("ULMEN-AGENT v1\n"));
        assert!(encoded.contains("records: 2\n"));

        let decoded = AgentPayload::decode(&encoded).unwrap();
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].id, "m1");
        assert_eq!(decoded.records[1].id, "m2");
        assert_eq!(decoded.records[0].step, 1);
        assert_eq!(decoded.records[1].step, 2);
    }

    #[test]
    fn payload_with_meta_fields() {
        let mut rec = make_msg("m1", 1, "hi");
        rec.meta.priority = Some(1);
        rec.meta.from_agent = Some("agent_a".into());

        let payload = AgentPayload {
            header: AgentHeader {
                meta_fields: vec!["from_agent".into(), "priority".into()],
                record_count: 1,
                ..Default::default()
            },
            records: vec![rec],
        };

        let encoded = payload.encode();
        assert!(encoded.contains("meta: from_agent,priority\n"));

        let decoded = AgentPayload::decode(&encoded).unwrap();
        assert_eq!(
            decoded.records[0].meta.from_agent.as_deref(),
            Some("agent_a")
        );
        assert_eq!(decoded.records[0].meta.priority, Some(1));
    }

    #[test]
    fn decode_bad_magic() {
        let result = AgentPayload::decode("NOT-MAGIC\nrecords: 0\n");
        assert!(result.is_err());
    }

    #[test]
    fn decode_record_count_mismatch() {
        let result =
            AgentPayload::decode("ULMEN-AGENT v1\nrecords: 5\nmsg|m1|t1|1|user|1|hi|5|F\n");
        assert!(result.is_err());
    }

    #[test]
    fn field_value_decode_int() {
        assert_eq!(FieldValue::decode("42", b'd').unwrap(), FieldValue::Int(42));
        assert_eq!(FieldValue::decode("-7", b'd').unwrap(), FieldValue::Int(-7));
    }

    #[test]
    fn field_value_decode_float() {
        if let FieldValue::Float(f) = FieldValue::decode("1.23", b'f').unwrap() {
            assert!((f - 1.23).abs() < 0.001);
        } else {
            panic!("expected float");
        }
    }

    #[test]
    fn field_value_decode_bool() {
        assert_eq!(
            FieldValue::decode("T", b'b').unwrap(),
            FieldValue::Bool(true)
        );
        assert_eq!(
            FieldValue::decode("F", b'b').unwrap(),
            FieldValue::Bool(false)
        );
    }

    #[test]
    fn field_value_decode_null() {
        assert_eq!(FieldValue::decode("N", b's').unwrap(), FieldValue::Null);
    }

    #[test]
    fn field_value_decode_empty_string() {
        assert_eq!(
            FieldValue::decode("$0=", b's').unwrap(),
            FieldValue::Str(String::new())
        );
    }

    #[test]
    fn field_value_decode_quoted() {
        assert_eq!(
            FieldValue::decode("\"hello|world\"", b's').unwrap(),
            FieldValue::Str("hello|world".into())
        );
    }

    #[test]
    fn field_value_nan() {
        if let FieldValue::Float(f) = FieldValue::decode("nan", b'f').unwrap() {
            assert!(f.is_nan());
        }
    }

    #[test]
    fn encode_1000_records_benchmark() {
        // Verify correctness at scale, not a timing test
        let mut records = Vec::with_capacity(1000);
        for i in 0..1000 {
            records.push(AgentRecord {
                record_type: RecordType::Msg,
                id: format!("m{}", i),
                thread_id: "t1".into(),
                step: i + 1,
                fields: vec![
                    FieldValue::Str(if i % 2 == 0 { "user" } else { "assistant" }.into()),
                    FieldValue::Int(i + 1),
                    FieldValue::Str(format!("Message {} with content.", i)),
                    FieldValue::Int(12),
                    FieldValue::Bool(false),
                ],
                meta: MetaFields::default(),
            });
        }

        let payload = AgentPayload {
            header: AgentHeader {
                thread_id: Some("t1".into()),
                record_count: 1000,
                ..Default::default()
            },
            records,
        };

        let encoded = payload.encode();
        assert!(encoded.starts_with("ULMEN-AGENT v1\n"));
        assert!(encoded.contains("records: 1000\n"));

        let decoded = AgentPayload::decode(&encoded).unwrap();
        assert_eq!(decoded.records.len(), 1000);
        assert_eq!(decoded.records[999].id, "m999");
    }

    #[test]
    fn split_row_simple() {
        assert_eq!(split_row("a|b|c"), vec!["a", "b", "c"]);
    }

    #[test]
    fn split_row_quoted() {
        let row = "msg|\"hello|world\"|t1";
        let parts = split_row(row);
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[1], "\"hello|world\"");
    }

    #[test]
    fn unescape_backslash_n() {
        assert_eq!(unescape("hello\\nworld"), "hello\nworld");
    }

    #[test]
    fn unescape_double_quote() {
        assert_eq!(unescape("say \"\"hi\"\""), "say \"hi\"");
    }

    #[test]
    fn header_all_fields() {
        let h = AgentHeader {
            thread_id: Some("t1".into()),
            context_window: Some(4096),
            context_used: Some(100),
            payload_id: Some("p1".into()),
            parent_payload_id: Some("p0".into()),
            agent_id: Some("a1".into()),
            session_id: Some("s1".into()),
            schema_version: Some("1.0.0".into()),
            meta_fields: vec!["priority".into()],
            record_count: 5,
        };
        let lines = h.encode_lines();
        assert!(lines.contains(&"thread: t1".to_string()));
        assert!(lines.contains(&"records: 5".to_string()));
        assert!(lines.contains(&"meta: priority".to_string()));
    }

    #[test]
    fn forward_compat_unknown_header() {
        let text = "ULMEN-AGENT v1\nfuture_field: xyz\nrecords: 0\n";
        let payload = AgentPayload::decode(text).unwrap();
        assert_eq!(payload.records.len(), 0);
    }
}
