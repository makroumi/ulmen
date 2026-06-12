//! ulmen-core Python bindings.
//!
//! Thin PyO3 wrapper over ulmen-core. Zero reimplementation.
//! Every function delegates directly to the Rust crate.
//!
//! Python gets Rust speed. Rust does the work.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use ulmen_core::*;

// ========================================================================
// FieldValue conversion (PyO3 0.21 GIL-bound API)
// ========================================================================

fn py_to_field(obj: &Bound<'_, pyo3::PyAny>) -> PyResult<FieldValue> {
    if obj.is_none() {
        Ok(FieldValue::Null)
    } else if let Ok(b) = obj.extract::<bool>() {
        Ok(FieldValue::Bool(b))
    } else if let Ok(i) = obj.extract::<i64>() {
        Ok(FieldValue::Int(i))
    } else if let Ok(f) = obj.extract::<f64>() {
        Ok(FieldValue::Float(f))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(FieldValue::Str(s))
    } else {
        Err(PyValueError::new_err("unsupported field type"))
    }
}

fn field_to_py(py: Python<'_>, fv: &FieldValue) -> PyObject {
    match fv {
        FieldValue::Null => py.None(),
        FieldValue::Bool(b) => b.to_object(py),
        FieldValue::Int(i) => i.to_object(py),
        FieldValue::Float(f) => f.to_object(py),
        FieldValue::Str(s) => s.to_object(py),
    }
}

// ========================================================================
// AgentRecord -> dict, dict -> AgentRecord
// ========================================================================

fn record_to_py(py: Python<'_>, rec: &AgentRecord) -> PyObject {
    let d = PyDict::new_bound(py);
    d.set_item("type", rec.record_type.as_str()).unwrap();
    d.set_item("id", &rec.id).unwrap();
    d.set_item("thread_id", &rec.thread_id).unwrap();
    d.set_item("step", rec.step).unwrap();

    let fields: Vec<PyObject> = rec.fields.iter().map(|f| field_to_py(py, f)).collect();
    d.set_item("fields", PyList::new_bound(py, &fields)).unwrap();

    let meta = PyDict::new_bound(py);
    if let Some(ref v) = rec.meta.parent_id {
        meta.set_item("parent_id", v).unwrap();
    }
    if let Some(ref v) = rec.meta.from_agent {
        meta.set_item("from_agent", v).unwrap();
    }
    if let Some(ref v) = rec.meta.to_agent {
        meta.set_item("to_agent", v).unwrap();
    }
    if let Some(v) = rec.meta.priority {
        meta.set_item("priority", v).unwrap();
    }
    d.set_item("meta", meta).unwrap();

    d.into()
}

fn dict_to_record(d: &Bound<'_, PyDict>) -> PyResult<AgentRecord> {
    let type_str: String = d
        .get_item("type")?
        .ok_or_else(|| PyValueError::new_err("missing 'type'"))?
        .extract()?;
    let record_type = RecordType::parse(&type_str)
        .ok_or_else(|| PyValueError::new_err(format!("unknown record type: {}", type_str)))?;
    let id: String = d
        .get_item("id")?
        .ok_or_else(|| PyValueError::new_err("missing 'id'"))?
        .extract()?;
    let thread_id: String = d
        .get_item("thread_id")?
        .ok_or_else(|| PyValueError::new_err("missing 'thread_id'"))?
        .extract()?;
    let step: i64 = d
        .get_item("step")?
        .ok_or_else(|| PyValueError::new_err("missing 'step'"))?
        .extract()?;

    let fields_obj = d
        .get_item("fields")?
        .ok_or_else(|| PyValueError::new_err("missing 'fields'"))?;
    let fields_list = fields_obj
        .downcast::<PyList>()
        .map_err(|_| PyValueError::new_err("'fields' must be a list"))?;

    let fields: Vec<FieldValue> = fields_list
        .iter()
        .map(|item| py_to_field(&item))
        .collect::<PyResult<_>>()?;

    let mut meta = MetaFields::default();
    if let Ok(Some(meta_obj)) = d.get_item("meta") {
        if let Ok(md) = meta_obj.downcast::<PyDict>() {
            if let Ok(Some(v)) = md.get_item("parent_id") {
                meta.parent_id = v.extract().ok();
            }
            if let Ok(Some(v)) = md.get_item("from_agent") {
                meta.from_agent = v.extract().ok();
            }
            if let Ok(Some(v)) = md.get_item("to_agent") {
                meta.to_agent = v.extract().ok();
            }
            if let Ok(Some(v)) = md.get_item("priority") {
                meta.priority = v.extract().ok();
            }
        }
    }

    Ok(AgentRecord {
        record_type,
        id,
        thread_id,
        step,
        fields,
        meta,
    })
}

fn records_from_pylist(list: &Bound<'_, PyList>) -> PyResult<Vec<AgentRecord>> {
    list.iter()
        .map(|item| {
            let d = item
                .downcast::<PyDict>()
                .map_err(|_| PyValueError::new_err("each record must be a dict"))?;
            dict_to_record(d)
        })
        .collect()
}

fn records_to_pylist(py: Python<'_>, recs: &[AgentRecord]) -> PyObject {
    let items: Vec<PyObject> = recs.iter().map(|r| record_to_py(py, r)).collect();
    PyList::new_bound(py, &items).into()
}

// ========================================================================
// Header conversion
// ========================================================================

fn header_to_py(py: Python<'_>, h: &AgentHeader) -> PyObject {
    let d = PyDict::new_bound(py);
    if let Some(ref v) = h.thread_id {
        d.set_item("thread_id", v).unwrap();
    }
    if let Some(v) = h.context_window {
        d.set_item("context_window", v).unwrap();
    }
    if let Some(v) = h.context_used {
        d.set_item("context_used", v).unwrap();
    }
    if let Some(ref v) = h.payload_id {
        d.set_item("payload_id", v).unwrap();
    }
    if let Some(ref v) = h.parent_payload_id {
        d.set_item("parent_payload_id", v).unwrap();
    }
    if let Some(ref v) = h.agent_id {
        d.set_item("agent_id", v).unwrap();
    }
    if let Some(ref v) = h.session_id {
        d.set_item("session_id", v).unwrap();
    }
    if let Some(ref v) = h.schema_version {
        d.set_item("schema_version", v).unwrap();
    }
    if !h.meta_fields.is_empty() {
        d.set_item("meta_fields", PyList::new_bound(py, &h.meta_fields)).unwrap();
    }
    d.set_item("record_count", h.record_count).unwrap();
    d.into()
}

fn dict_to_header(d: &Bound<'_, PyDict>) -> PyResult<AgentHeader> {
    let mut h = AgentHeader::default();
    if let Ok(Some(v)) = d.get_item("thread_id") {
        h.thread_id = v.extract().ok();
    }
    if let Ok(Some(v)) = d.get_item("context_window") {
        h.context_window = v.extract().ok();
    }
    if let Ok(Some(v)) = d.get_item("context_used") {
        h.context_used = v.extract().ok();
    }
    if let Ok(Some(v)) = d.get_item("payload_id") {
        h.payload_id = v.extract().ok();
    }
    if let Ok(Some(v)) = d.get_item("parent_payload_id") {
        h.parent_payload_id = v.extract().ok();
    }
    if let Ok(Some(v)) = d.get_item("agent_id") {
        h.agent_id = v.extract().ok();
    }
    if let Ok(Some(v)) = d.get_item("session_id") {
        h.session_id = v.extract().ok();
    }
    if let Ok(Some(v)) = d.get_item("schema_version") {
        h.schema_version = v.extract().ok();
    }
    if let Ok(Some(v)) = d.get_item("meta_fields") {
        h.meta_fields = v.extract().unwrap_or_default();
    }
    if let Ok(Some(v)) = d.get_item("record_count") {
        h.record_count = v.extract().unwrap_or(0);
    }
    Ok(h)
}

fn payload_to_py(py: Python<'_>, payload: &AgentPayload) -> PyObject {
    let d = PyDict::new_bound(py);
    d.set_item("header", header_to_py(py, &payload.header)).unwrap();
    d.set_item("records", records_to_pylist(py, &payload.records)).unwrap();
    d.into()
}

fn pydict_to_payload(d: &Bound<'_, PyDict>) -> PyResult<AgentPayload> {
    let header_obj = d
        .get_item("header")?
        .ok_or_else(|| PyValueError::new_err("missing 'header'"))?;
    let header_dict = header_obj
        .downcast::<PyDict>()
        .map_err(|_| PyValueError::new_err("'header' must be a dict"))?;
    let h = dict_to_header(header_dict)?;

    let records_obj = d
        .get_item("records")?
        .ok_or_else(|| PyValueError::new_err("missing 'records'"))?;
    let records_list = records_obj
        .downcast::<PyList>()
        .map_err(|_| PyValueError::new_err("'records' must be a list"))?;
    let recs = records_from_pylist(records_list)?;

    Ok(AgentPayload {
        header: AgentHeader {
            record_count: recs.len(),
            ..h
        },
        records: recs,
    })
}

// ========================================================================
// Tokens
// ========================================================================

#[pyfunction]
fn count_tokens_py(text: &str) -> usize {
    count_tokens(text)
}

#[pyfunction]
#[pyo3(signature = (text, per_record_overhead = 3))]
fn count_tokens_with_overhead_py(text: &str, per_record_overhead: usize) -> usize {
    count_tokens_with_overhead(text, per_record_overhead)
}

#[pyfunction]
fn estimate_tokens_py(text: &str) -> usize {
    estimate_tokens(text)
}

// ========================================================================
// Agent record encode/decode
// ========================================================================

#[pyfunction]
fn encode_field_py(value: &Bound<'_, pyo3::PyAny>) -> PyResult<String> {
    let fv = py_to_field(value)?;
    let mut out = String::new();
    fv.encode_into(&mut out);
    Ok(out)
}

#[pyfunction]
#[pyo3(signature = (record, meta_fields = None))]
fn encode_record_py(_py: Python<'_>, record: &Bound<'_, PyDict>, meta_fields: Option<Vec<String>>) -> PyResult<String> {
    let rec = dict_to_record(record)?;
    let mf_refs: Vec<&str> = meta_fields
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_str()).collect())
        .unwrap_or_default();
    Ok(rec.encode(&mf_refs))
}

#[pyfunction]
#[pyo3(signature = (line, meta_fields = None))]
fn decode_record_py(py: Python<'_>, line: &str, meta_fields: Option<Vec<String>>) -> PyResult<PyObject> {
    let mf_refs: Vec<&str> = meta_fields
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_str()).collect())
        .unwrap_or_default();
    let rec = decode_record_public(line, &mf_refs)
        .map_err(|e| PyRuntimeError::new_err(format!("{}", e)))?;
    Ok(record_to_py(py, &rec))
}

// ========================================================================
// Agent payload encode/decode
// ========================================================================

#[pyfunction]
fn encode_payload_py(_py: Python<'_>, header: &Bound<'_, PyDict>, records: &Bound<'_, PyList>) -> PyResult<String> {
    let h = dict_to_header(header)?;
    let recs = records_from_pylist(records)?;
    let payload = AgentPayload {
        header: AgentHeader {
            record_count: recs.len(),
            ..h
        },
        records: recs,
    };
    Ok(payload.encode())
}

#[pyfunction]
fn decode_payload_py(py: Python<'_>, text: &str) -> PyResult<PyObject> {
    let payload = AgentPayload::decode(text)
        .map_err(|e| PyRuntimeError::new_err(format!("{}", e)))?;
    Ok(payload_to_py(py, &payload))
}

// ========================================================================
// Validation
// ========================================================================

#[pyfunction]
fn validate_payload_from_parts(_py: Python<'_>, header: &Bound<'_, PyDict>, records: &Bound<'_, PyList>) -> PyResult<()> {
    let h = dict_to_header(header)?;
    let recs = records_from_pylist(records)?;
    let payload = AgentPayload {
        header: AgentHeader {
            record_count: recs.len(),
            ..h
        },
        records: recs,
    };
    validate_payload(&payload).map_err(|e| PyValueError::new_err(format!("{}", e)))
}

#[pyfunction]
fn validate_payload_str_py(text: &str) -> (bool, Option<String>) {
    let (valid, err) = validate_payload_str(text);
    (valid, err.map(|e| format!("{}", e)))
}

// ========================================================================
// Chunking
// ========================================================================

#[pyfunction]
#[pyo3(signature = (records, budget, thread_id = None, meta_fields = None, overlap = 1, parent_payload_id = None, session_id = None))]
fn chunk_payload_py(
    py: Python<'_>,
    records: &Bound<'_, PyList>,
    budget: usize,
    thread_id: Option<&str>,
    meta_fields: Option<Vec<String>>,
    overlap: usize,
    parent_payload_id: Option<&str>,
    session_id: Option<&str>,
) -> PyResult<PyObject> {
    let recs = records_from_pylist(records)?;
    let mf_refs: Vec<&str> = meta_fields
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_str()).collect())
        .unwrap_or_default();
    let chunks = chunk_payload(&recs, budget, thread_id, &mf_refs, overlap, parent_payload_id, session_id);
    let items: Vec<PyObject> = chunks.iter().map(|c| payload_to_py(py, c)).collect();
    Ok(PyList::new_bound(py, &items).into())
}

#[pyfunction]
fn merge_chunks_py(py: Python<'_>, chunks: &Bound<'_, PyList>) -> PyResult<PyObject> {
    let payloads: Vec<AgentPayload> = chunks
        .iter()
        .map(|item| {
            let d = item
                .downcast::<PyDict>()
                .map_err(|_| PyValueError::new_err("each chunk must be a dict"))?;
            pydict_to_payload(d)
        })
        .collect::<PyResult<_>>()?;
    let merged = merge_chunks(&payloads);
    Ok(records_to_pylist(py, &merged))
}

#[pyfunction]
fn make_error_payload_py(py: Python<'_>, msg: &str, thread_id: &str) -> PyObject {
    let payload = make_error_payload(msg, thread_id);
    payload_to_py(py, &payload)
}

// ========================================================================
// Compression
// ========================================================================

#[pyfunction]
#[pyo3(signature = (records, strategy = "completed_sequences", keep_priority = 3, keep_types = None, window_size = None, preserve_cot = false))]
fn compress_context_py(
    py: Python<'_>,
    records: &Bound<'_, PyList>,
    strategy: &str,
    keep_priority: i64,
    keep_types: Option<Vec<String>>,
    window_size: Option<usize>,
    preserve_cot: bool,
) -> PyResult<PyObject> {
    let recs = records_from_pylist(records)?;
    let strat = CompressStrategy::parse(strategy)
        .ok_or_else(|| PyValueError::new_err(format!("unknown strategy: {}", strategy)))?;
    let kt_refs: Option<Vec<&str>> = keep_types
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_str()).collect());
    let compressed = compress_context(&recs, strat, keep_priority, kt_refs.as_deref(), window_size, preserve_cot);
    Ok(records_to_pylist(py, &compressed))
}

#[pyfunction]
fn summarize_as_mem_py(py: Python<'_>, records: &Bound<'_, PyList>) -> PyResult<PyObject> {
    let recs = records_from_pylist(records)?;
    let result = summarize_as_mem(&recs);
    Ok(records_to_pylist(py, &result))
}

#[pyfunction]
fn dedup_mem_py(py: Python<'_>, records: &Bound<'_, PyList>) -> PyResult<PyObject> {
    let recs = records_from_pylist(records)?;
    let result = dedup_mem(&recs);
    Ok(records_to_pylist(py, &result))
}

// ========================================================================
// Repair
// ========================================================================

#[pyfunction]
#[pyo3(signature = (raw_text, thread_id = None, strict = false))]
fn parse_llm_output_py(py: Python<'_>, raw_text: &str, thread_id: Option<&str>, strict: bool) -> PyResult<PyObject> {
    let payload = parse_llm_output(raw_text, thread_id, strict)
        .map_err(|e| PyRuntimeError::new_err(format!("{}", e)))?;
    Ok(payload_to_py(py, &payload))
}

// ========================================================================
// Constants
// ========================================================================

#[pyfunction]
fn agent_magic() -> &'static str {
    AGENT_MAGIC
}

#[pyfunction]
fn agent_version() -> &'static str {
    AGENT_VERSION
}

#[pyfunction]
fn meta_field_names_py() -> Vec<&'static str> {
    META_FIELD_NAMES.to_vec()
}

#[pyfunction]
fn record_types_py() -> Vec<&'static str> {
    vec!["msg", "tool", "res", "plan", "obs", "err", "mem", "rag", "hyp", "cot"]
}

#[pyfunction]
fn field_count_py(record_type: &str) -> PyResult<usize> {
    let rt = RecordType::parse(record_type)
        .ok_or_else(|| PyValueError::new_err(format!("unknown type: {}", record_type)))?;
    Ok(rt.field_count())
}

// ========================================================================
// Module registration
// ========================================================================

#[pymodule]
fn _ulmen_rust(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Constants
    m.add("AGENT_MAGIC", AGENT_MAGIC)?;
    m.add("AGENT_VERSION", AGENT_VERSION)?;
    m.add("VERSION", "1.0.3")?;
    m.add("EDITION", "ULMEN V1")?;

    // Tokens
    m.add_function(wrap_pyfunction!(count_tokens_py, m)?)?;
    m.add_function(wrap_pyfunction!(count_tokens_with_overhead_py, m)?)?;
    m.add_function(wrap_pyfunction!(estimate_tokens_py, m)?)?;

    // Agent field
    m.add_function(wrap_pyfunction!(encode_field_py, m)?)?;

    // Agent record
    m.add_function(wrap_pyfunction!(encode_record_py, m)?)?;
    m.add_function(wrap_pyfunction!(decode_record_py, m)?)?;

    // Agent payload
    m.add_function(wrap_pyfunction!(encode_payload_py, m)?)?;
    m.add_function(wrap_pyfunction!(decode_payload_py, m)?)?;

    // Validation
    m.add_function(wrap_pyfunction!(validate_payload_from_parts, m)?)?;
    m.add_function(wrap_pyfunction!(validate_payload_str_py, m)?)?;

    // Chunking
    m.add_function(wrap_pyfunction!(chunk_payload_py, m)?)?;
    m.add_function(wrap_pyfunction!(merge_chunks_py, m)?)?;
    m.add_function(wrap_pyfunction!(make_error_payload_py, m)?)?;

    // Compression
    m.add_function(wrap_pyfunction!(compress_context_py, m)?)?;
    m.add_function(wrap_pyfunction!(summarize_as_mem_py, m)?)?;
    m.add_function(wrap_pyfunction!(dedup_mem_py, m)?)?;

    // Repair
    m.add_function(wrap_pyfunction!(parse_llm_output_py, m)?)?;

    // Constants
    m.add_function(wrap_pyfunction!(agent_magic, m)?)?;
    m.add_function(wrap_pyfunction!(agent_version, m)?)?;
    m.add_function(wrap_pyfunction!(meta_field_names_py, m)?)?;
    m.add_function(wrap_pyfunction!(record_types_py, m)?)?;
    m.add_function(wrap_pyfunction!(field_count_py, m)?)?;

    Ok(())
}
