//! Chunking and merging for unlimited context windows.

use crate::agent::*;
use crate::tokens;

/// Split records into multiple payloads, each within token_budget.
/// Tool+res pairs are kept atomic.
pub fn chunk_payload(
    records: &[AgentRecord],
    token_budget: usize,
    thread_id: Option<&str>,
    meta_field_names: &[&str],
    overlap: usize,
    parent_payload_id: Option<&str>,
    session_id: Option<&str>,
) -> Vec<AgentPayload> {
    if records.is_empty() {
        return vec![AgentPayload {
            header: AgentHeader {
                thread_id: thread_id.map(String::from),
                context_window: Some(token_budget as i64),
                session_id: session_id.map(String::from),
                record_count: 0,
                ..Default::default()
            },
            records: Vec::new(),
        }];
    }

    // Group into atomic units (tool+res kept together)
    let units = build_atomic_units(records);

    // Cost per unit in tokens
    let unit_costs: Vec<usize> = units
        .iter()
        .map(|unit| {
            unit.iter()
                .map(|rec| {
                    let row = rec.encode(meta_field_names);
                    tokens::estimate_tokens(&row).max(1)
                })
                .sum()
        })
        .collect();

    // Header overhead
    let header_overhead = tokens::estimate_tokens(&format!(
        "{}\nthread: {}\ncontext_window: {}\nrecords: 9999\n",
        AGENT_MAGIC,
        thread_id.unwrap_or(""),
        token_budget
    ));
    let effective_budget = token_budget.saturating_sub(header_overhead).max(10);

    // Pack greedily
    let mut chunks: Vec<AgentPayload> = Vec::new();
    let mut current_units: Vec<usize> = Vec::new(); // unit indices
    let mut current_tokens: usize = 0;
    let mut prev_pid = parent_payload_id.map(String::from);
    let mut chunk_counter: u64 = 0;

    let flush = |unit_indices: &[usize],
                 units: &[Vec<&AgentRecord>],
                 prev_id: &Option<String>,
                 counter: &mut u64,
                 thread_id: Option<&str>,
                 token_budget: usize,
                 session_id: Option<&str>|
     -> (AgentPayload, String) {
        *counter += 1;
        let pid = format!("chunk_{:04}", counter);
        let flat: Vec<AgentRecord> = unit_indices
            .iter()
            .flat_map(|&ui| units[ui].iter().map(|r| (*r).clone()))
            .collect();
        let payload = AgentPayload {
            header: AgentHeader {
                thread_id: thread_id.map(String::from),
                context_window: Some(token_budget as i64),
                payload_id: Some(pid.clone()),
                parent_payload_id: prev_id.clone(),
                session_id: session_id.map(String::from),
                record_count: flat.len(),
                ..Default::default()
            },
            records: flat,
        };
        (payload, pid)
    };

    for (ui, cost) in unit_costs.iter().enumerate() {
        if !current_units.is_empty() && current_tokens + cost > effective_budget {
            let (payload, pid) = flush(
                &current_units,
                &units,
                &prev_pid,
                &mut chunk_counter,
                thread_id,
                token_budget,
                session_id,
            );
            chunks.push(payload);
            prev_pid = Some(pid);

            if overlap > 0 && current_units.len() > overlap {
                let start = current_units.len() - overlap;
                current_units = current_units[start..].to_vec();
                current_tokens = current_units.iter().map(|&i| unit_costs[i]).sum();
            } else {
                current_units.clear();
                current_tokens = 0;
            }
        }
        current_units.push(ui);
        current_tokens += cost;
    }

    if !current_units.is_empty() {
        let (payload, _) = flush(
            &current_units,
            &units,
            &prev_pid,
            &mut chunk_counter,
            thread_id,
            token_budget,
            session_id,
        );
        chunks.push(payload);
    }

    chunks
}

/// Build atomic units: tool+res pairs are kept together.
fn build_atomic_units(records: &[AgentRecord]) -> Vec<Vec<&AgentRecord>> {
    let mut units: Vec<Vec<&AgentRecord>> = Vec::new();
    let mut pending_tools: std::collections::HashMap<&str, Vec<usize>> =
        std::collections::HashMap::new();

    for rec in records {
        match rec.record_type {
            RecordType::Tool => {
                let ui = units.len();
                units.push(vec![rec]);
                pending_tools.entry(&rec.id).or_default().push(ui);
            }
            RecordType::Res => {
                if let Some(tool_units) = pending_tools.get_mut(rec.id.as_str()) {
                    if let Some(ui) = tool_units.first().copied() {
                        units[ui].push(rec);
                        tool_units.remove(0);
                        if tool_units.is_empty() {
                            pending_tools.remove(rec.id.as_str());
                        }
                        continue;
                    }
                }
                units.push(vec![rec]);
            }
            _ => {
                units.push(vec![rec]);
            }
        }
    }
    units
}

/// Merge chunked payloads back into a flat list of unique records.
/// Deduplicates by (id, thread_id, step).
pub fn merge_chunks(payloads: &[AgentPayload]) -> Vec<AgentRecord> {
    let mut seen: std::collections::HashSet<(String, String, i64)> =
        std::collections::HashSet::new();
    let mut result = Vec::new();

    for payload in payloads {
        for rec in &payload.records {
            let key = (rec.id.clone(), rec.thread_id.clone(), rec.step);
            if seen.insert(key) {
                result.push(rec.clone());
            }
        }
    }
    result
}

/// Build a validation error payload.
pub fn make_error_payload(msg: &str, thread_id: &str) -> AgentPayload {
    AgentPayload {
        header: AgentHeader {
            record_count: 1,
            ..Default::default()
        },
        records: vec![AgentRecord {
            record_type: RecordType::Err,
            id: "er_val_001".into(),
            thread_id: thread_id.into(),
            step: 1,
            fields: vec![
                FieldValue::Str("VALIDATION_FAILED".into()),
                FieldValue::Str(msg.into()),
                FieldValue::Str("validator".into()),
                FieldValue::Bool(false),
            ],
            meta: MetaFields::default(),
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(id: &str, step: i64, content: &str) -> AgentRecord {
        AgentRecord {
            record_type: RecordType::Msg,
            id: id.into(),
            thread_id: "t1".into(),
            step,
            fields: vec![
                FieldValue::Str("user".into()),
                FieldValue::Int(1),
                FieldValue::Str(content.into()),
                FieldValue::Int(5),
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

    #[test]
    fn chunk_empty() {
        let chunks = chunk_payload(&[], 1000, None, &[], 0, None, None);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].records.len(), 0);
    }

    #[test]
    fn chunk_fits_in_one() {
        let recs = vec![msg("m1", 1, "hi"), msg("m2", 2, "bye")];
        let chunks = chunk_payload(&recs, 10000, None, &[], 0, None, None);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].records.len(), 2);
    }

    #[test]
    fn chunk_splits_on_budget() {
        let recs: Vec<AgentRecord> = (1..=20)
            .map(|i| msg(&format!("m{}", i), i, &"x".repeat(100)))
            .collect();
        let chunks = chunk_payload(&recs, 200, None, &[], 0, None, None);
        assert!(chunks.len() > 1, "should split into multiple chunks");
        let total: usize = chunks.iter().map(|c| c.records.len()).sum();
        assert_eq!(total, 20);
    }

    #[test]
    fn chunk_keeps_tool_res_together() {
        let recs = vec![
            msg("m1", 1, "hi"),
            tool("t1", 2),
            res("t1", 3),
            msg("m2", 4, "bye"),
        ];
        let chunks = chunk_payload(&recs, 200, None, &[], 0, None, None);
        // tool and res should be in the same chunk
        for chunk in &chunks {
            let has_tool = chunk
                .records
                .iter()
                .any(|r| r.record_type == RecordType::Tool && r.id == "t1");
            let has_res = chunk
                .records
                .iter()
                .any(|r| r.record_type == RecordType::Res && r.id == "t1");
            if has_tool || has_res {
                assert!(has_tool && has_res, "tool and res must be in same chunk");
            }
        }
    }

    #[test]
    fn merge_deduplicates() {
        let p1 = AgentPayload {
            header: AgentHeader {
                record_count: 2,
                ..Default::default()
            },
            records: vec![msg("m1", 1, "hi"), msg("m2", 2, "bye")],
        };
        let p2 = AgentPayload {
            header: AgentHeader {
                record_count: 2,
                ..Default::default()
            },
            records: vec![msg("m2", 2, "bye"), msg("m3", 3, "end")],
        };
        let merged = merge_chunks(&[p1, p2]);
        assert_eq!(merged.len(), 3); // m1, m2, m3 (m2 deduped)
    }

    #[test]
    fn make_error_payload_valid() {
        let p = make_error_payload("something broke", "t1");
        assert_eq!(p.records.len(), 1);
        assert_eq!(p.records[0].record_type, RecordType::Err);
    }

    #[test]
    fn chunk_payload_ids_linked() {
        let recs: Vec<AgentRecord> = (1..=10)
            .map(|i| msg(&format!("m{}", i), i, &"x".repeat(100)))
            .collect();
        let chunks = chunk_payload(&recs, 200, Some("t1"), &[], 0, Some("parent_0"), None);
        if chunks.len() >= 2 {
            assert_eq!(
                chunks[0].header.parent_payload_id.as_deref(),
                Some("parent_0")
            );
            assert_eq!(
                chunks[1].header.parent_payload_id,
                chunks[0].header.payload_id,
            );
        }
    }
}
