//! Context compression for ULMEN-AGENT payloads.
//!
//! Three strategies:
//! - completed_sequences: replace finished tool+res pairs with mem summaries
//! - keep_types: keep only specified record types
//! - sliding_window: keep recent N records, summarize older ones

use crate::agent::*;

/// Compression strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressStrategy {
    CompletedSequences,
    KeepTypes,
    SlidingWindow,
}

impl CompressStrategy {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "completed_sequences" => Some(Self::CompletedSequences),
            "keep_types" => Some(Self::KeepTypes),
            "sliding_window" => Some(Self::SlidingWindow),
            _ => None,
        }
    }
}

/// Priority levels for records.
pub const PRIORITY_MUST_KEEP: i64 = 1;
pub const PRIORITY_KEEP_IF_ROOM: i64 = 2;
pub const PRIORITY_COMPRESSIBLE: i64 = 3;

/// Compress agent records using the specified strategy.
pub fn compress_context(
    records: &[AgentRecord],
    strategy: CompressStrategy,
    keep_priority: i64,
    keep_types: Option<&[&str]>,
    window_size: Option<usize>,
    preserve_cot: bool,
) -> Vec<AgentRecord> {
    if records.is_empty() {
        return Vec::new();
    }

    match strategy {
        CompressStrategy::CompletedSequences => {
            compress_completed_sequences(records, keep_priority, preserve_cot)
        }
        CompressStrategy::KeepTypes => {
            let kt: Vec<&str> = keep_types.unwrap_or(&["msg", "err", "mem"]).to_vec();
            records
                .iter()
                .filter(|r| {
                    kt.contains(&r.record_type.as_str()) || rec_priority(r) <= keep_priority
                })
                .cloned()
                .collect()
        }
        CompressStrategy::SlidingWindow => {
            let ws = window_size.unwrap_or_else(|| std::cmp::max(10, records.len() / 4));
            if records.len() <= ws {
                return records.to_vec();
            }
            let cutoff = records.len() - ws;
            let mut result = summarize_as_mem(&records[..cutoff]);
            result.extend_from_slice(&records[cutoff..]);
            result
        }
    }
}

fn rec_priority(rec: &AgentRecord) -> i64 {
    rec.meta.priority.unwrap_or(PRIORITY_COMPRESSIBLE)
}

fn compress_completed_sequences(
    records: &[AgentRecord],
    keep_priority: i64,
    preserve_cot: bool,
) -> Vec<AgentRecord> {
    // Find completed tool+res pairs
    let mut tool_ids: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut res_ids: std::collections::HashSet<&str> = std::collections::HashSet::new();

    for rec in records {
        match rec.record_type {
            RecordType::Tool => {
                tool_ids.insert(&rec.id);
            }
            RecordType::Res => {
                res_ids.insert(&rec.id);
            }
            _ => {}
        }
    }

    let completed: std::collections::HashSet<&str> =
        tool_ids.intersection(&res_ids).copied().collect();

    let mut result = Vec::with_capacity(records.len());
    let mut compressed_tools: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut seq_counter: usize = 0;
    let mut cot_counter: usize = 0;

    for rec in records {
        let pri = rec_priority(rec);

        if pri <= keep_priority {
            result.push(rec.clone());
            continue;
        }

        match rec.record_type {
            RecordType::Msg
            | RecordType::Plan
            | RecordType::Obs
            | RecordType::Err
            | RecordType::Mem
            | RecordType::Hyp
            | RecordType::Rag => {
                result.push(rec.clone());
            }
            RecordType::Tool if completed.contains(rec.id.as_str()) => {
                if compressed_tools.contains(rec.id.as_str()) {
                    continue;
                }
                compressed_tools.insert(&rec.id);
                seq_counter += 1;

                let name = match rec.fields.first() {
                    Some(FieldValue::Str(s)) if !s.is_empty() => s.as_str(),
                    _ => rec.id.as_str(),
                };

                result.push(AgentRecord {
                    record_type: RecordType::Mem,
                    id: format!("mem_cmp_{:03}", seq_counter),
                    thread_id: rec.thread_id.clone(),
                    step: rec.step,
                    fields: vec![
                        FieldValue::Str(format!("tool_result_{}", name)),
                        FieldValue::Str(String::new()),
                        FieldValue::Float(1.0),
                        FieldValue::Null,
                    ],
                    meta: rec.meta.clone(),
                });
            }
            RecordType::Res if completed.contains(rec.id.as_str()) => {
                continue; // drop completed res
            }
            RecordType::Cot => {
                if preserve_cot {
                    cot_counter += 1;
                    let cot_type = match rec.fields.get(1) {
                        Some(FieldValue::Str(s)) => s.as_str(),
                        _ => "step",
                    };
                    let index = match rec.fields.first() {
                        Some(FieldValue::Int(i)) => *i,
                        _ => cot_counter as i64,
                    };
                    let text = match rec.fields.get(2) {
                        Some(FieldValue::Str(s)) => {
                            if s.len() > 500 {
                                &s[..500]
                            } else {
                                s.as_str()
                            }
                        }
                        _ => "",
                    };
                    let confidence = match rec.fields.get(3) {
                        Some(FieldValue::Float(f)) => *f,
                        _ => 1.0,
                    };

                    result.push(AgentRecord {
                        record_type: RecordType::Mem,
                        id: format!("mem_cot_{:03}", cot_counter),
                        thread_id: rec.thread_id.clone(),
                        step: rec.step,
                        fields: vec![
                            FieldValue::Str(format!("cot_{}_{}", cot_type, index)),
                            FieldValue::Str(text.to_string()),
                            FieldValue::Float(confidence),
                            FieldValue::Null,
                        ],
                        meta: rec.meta.clone(),
                    });
                }
                // else: drop cot
            }
            _ => {
                result.push(rec.clone());
            }
        }
    }

    result
}

/// Summarize a slice of records into mem summary records (one per thread).
pub fn summarize_as_mem(records: &[AgentRecord]) -> Vec<AgentRecord> {
    let mut by_thread: std::collections::HashMap<
        &str,
        (usize, std::collections::HashSet<&str>, i64),
    > = std::collections::HashMap::new();

    for rec in records {
        let entry =
            by_thread
                .entry(&rec.thread_id)
                .or_insert((0, std::collections::HashSet::new(), 0));
        entry.0 += 1;
        entry.1.insert(rec.record_type.as_str());
        if rec.step > entry.2 {
            entry.2 = rec.step;
        }
    }

    let mut result = Vec::with_capacity(by_thread.len());
    for (tid, (count, types, max_step)) in &by_thread {
        let mut sorted_types: Vec<&&str> = types.iter().collect();
        sorted_types.sort();
        let types_str = sorted_types
            .iter()
            .map(|s| **s)
            .collect::<Vec<_>>()
            .join(",");

        result.push(AgentRecord {
            record_type: RecordType::Mem,
            id: format!("mem_summary_{}", tid),
            thread_id: tid.to_string(),
            step: *max_step,
            fields: vec![
                FieldValue::Str(format!("context_summary_{}", tid)),
                FieldValue::Str(format!(
                    "Compressed {} records (types: {}) up to step {}",
                    count, types_str, max_step
                )),
                FieldValue::Float(0.9),
                FieldValue::Null,
            ],
            meta: MetaFields::default(),
        });
    }
    result
}

/// Deduplicate mem records by key: keep only the most recent per (thread_id, key).
pub fn dedup_mem(records: &[AgentRecord]) -> Vec<AgentRecord> {
    // Find winning mem per (thread_id, key)
    let mut best: std::collections::HashMap<(&str, &str), (i64, usize)> =
        std::collections::HashMap::new();

    for (i, rec) in records.iter().enumerate() {
        if rec.record_type == RecordType::Mem {
            if let Some(FieldValue::Str(key)) = rec.fields.first() {
                let k = (rec.thread_id.as_str(), key.as_str());
                match best.get(&k) {
                    Some((prev_step, _)) if rec.step <= *prev_step => {}
                    _ => {
                        best.insert(k, (rec.step, i));
                    }
                }
            }
        }
    }

    let winning_indices: std::collections::HashSet<usize> =
        best.values().map(|(_, i)| *i).collect();

    records
        .iter()
        .enumerate()
        .filter(|(i, rec)| {
            if rec.record_type == RecordType::Mem {
                winning_indices.contains(i)
            } else {
                true
            }
        })
        .map(|(_, rec)| rec.clone())
        .collect()
}

/// Get the most recent mem record with the given key.
pub fn get_latest_mem<'a>(records: &'a [AgentRecord], key: &str) -> Option<&'a AgentRecord> {
    let mut best: Option<&AgentRecord> = None;
    let mut best_step: i64 = -1;

    for rec in records {
        if rec.record_type == RecordType::Mem {
            if let Some(FieldValue::Str(k)) = rec.fields.first() {
                if k == key && rec.step > best_step {
                    best_step = rec.step;
                    best = Some(rec);
                }
            }
        }
    }
    best
}

/// Extract a subgraph of records by thread, step range, and/or types.
pub fn extract_subgraph(
    records: &[AgentRecord],
    thread_id: Option<&str>,
    step_min: Option<i64>,
    step_max: Option<i64>,
    types: Option<&[RecordType]>,
) -> Vec<AgentRecord> {
    records
        .iter()
        .filter(|r| {
            if let Some(tid) = thread_id {
                if r.thread_id != tid {
                    return false;
                }
            }
            if let Some(min) = step_min {
                if r.step < min {
                    return false;
                }
            }
            if let Some(max) = step_max {
                if r.step > max {
                    return false;
                }
            }
            if let Some(types) = types {
                if !types.contains(&r.record_type) {
                    return false;
                }
            }
            true
        })
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(id: &str, step: i64) -> AgentRecord {
        AgentRecord {
            record_type: RecordType::Msg,
            id: id.into(),
            thread_id: "t1".into(),
            step,
            fields: vec![
                FieldValue::Str("user".into()),
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

    fn mem(id: &str, step: i64, key: &str, val: &str) -> AgentRecord {
        AgentRecord {
            record_type: RecordType::Mem,
            id: id.into(),
            thread_id: "t1".into(),
            step,
            fields: vec![
                FieldValue::Str(key.into()),
                FieldValue::Str(val.into()),
                FieldValue::Float(1.0),
                FieldValue::Null,
            ],
            meta: MetaFields::default(),
        }
    }

    #[test]
    fn compress_empty() {
        let r = compress_context(
            &[],
            CompressStrategy::CompletedSequences,
            2,
            None,
            None,
            false,
        );
        assert!(r.is_empty());
    }

    #[test]
    fn compress_completed_replaces_pair() {
        let recs = vec![msg("m1", 1), tool("t1", 2), res("t1", 3), msg("m2", 4)];
        let result = compress_context(
            &recs,
            CompressStrategy::CompletedSequences,
            2,
            None,
            None,
            false,
        );
        // Should have: msg, mem_cmp, msg (tool+res replaced by mem)
        assert_eq!(result.len(), 3);
        assert_eq!(result[1].record_type, RecordType::Mem);
        assert!(result[1].id.starts_with("mem_cmp_"));
    }

    #[test]
    fn compress_keep_types() {
        let recs = vec![msg("m1", 1), tool("t1", 2), res("t1", 3)];
        let result = compress_context(
            &recs,
            CompressStrategy::KeepTypes,
            2,
            Some(&["msg"]),
            None,
            false,
        );
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].record_type, RecordType::Msg);
    }

    #[test]
    fn compress_sliding_window() {
        let recs: Vec<AgentRecord> = (1..=10).map(|i| msg(&format!("m{}", i), i)).collect();
        let result = compress_context(
            &recs,
            CompressStrategy::SlidingWindow,
            2,
            None,
            Some(3),
            false,
        );
        // Should have summary + last 3
        assert_eq!(result.len(), 4); // 1 summary + 3 recent
        assert_eq!(result[0].record_type, RecordType::Mem);
    }

    #[test]
    fn dedup_mem_keeps_latest() {
        let recs = vec![
            mem("m1", 1, "key_a", "old"),
            mem("m2", 5, "key_a", "new"),
            msg("msg1", 3),
        ];
        let result = dedup_mem(&recs);
        assert_eq!(result.len(), 2); // latest mem + msg
        if let FieldValue::Str(v) = &result[0].fields[1] {
            assert_eq!(v, "new");
        }
    }

    #[test]
    fn get_latest_mem_found() {
        let recs = vec![mem("m1", 1, "key_a", "old"), mem("m2", 5, "key_a", "new")];
        let found = get_latest_mem(&recs, "key_a").unwrap();
        assert_eq!(found.step, 5);
    }

    #[test]
    fn get_latest_mem_not_found() {
        let recs = vec![mem("m1", 1, "key_a", "val")];
        assert!(get_latest_mem(&recs, "key_b").is_none());
    }

    #[test]
    fn extract_subgraph_by_type() {
        let recs = vec![msg("m1", 1), tool("t1", 2)];
        let result = extract_subgraph(&recs, None, None, None, Some(&[RecordType::Msg]));
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].record_type, RecordType::Msg);
    }

    #[test]
    fn extract_subgraph_by_step() {
        let recs = vec![msg("m1", 1), msg("m2", 5), msg("m3", 10)];
        let result = extract_subgraph(&recs, None, Some(3), Some(7), None);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].step, 5);
    }

    #[test]
    fn summarize_groups_by_thread() {
        let recs = vec![msg("m1", 1), msg("m2", 2)];
        let result = summarize_as_mem(&recs);
        assert_eq!(result.len(), 1);
        assert!(result[0].id.contains("summary"));
    }
}
