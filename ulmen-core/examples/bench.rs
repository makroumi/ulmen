use std::time::Instant;
use ulmen_core::*;

fn make_records(n: usize) -> Vec<AgentRecord> {
    (0..n).map(|i| AgentRecord {
        record_type: RecordType::Msg,
        id: format!("m{}", i),
        thread_id: "t1".into(),
        step: (i + 1) as i64,
        fields: vec![
            FieldValue::Str(if i % 2 == 0 { "user" } else { "assistant" }.into()),
            FieldValue::Int((i + 1) as i64),
            FieldValue::Str(format!("Message {} with content about review.", i)),
            FieldValue::Int(12),
            FieldValue::Bool(false),
        ],
        meta: MetaFields::default(),
    }).collect()
}

fn bench<F: FnMut()>(name: &str, iters: u32, mut f: F) {
    for _ in 0..100 { f(); }
    let start = Instant::now();
    for _ in 0..iters { f(); }
    let us = start.elapsed().as_micros() as f64 / iters as f64;
    println!("  {:<40} {:>8.0} us", name, us);
}

fn main() {
    let records = make_records(1000);
    let payload = AgentPayload {
        header: AgentHeader {
            thread_id: Some("t1".into()),
            record_count: 1000,
            ..Default::default()
        },
        records: records.clone(),
    };
    let encoded = payload.encode();

    println!("ulmen-core benchmark (1000 records, release)");
    println!();

    bench("encode payload", 1000, || { let _ = payload.encode(); });
    bench("decode payload", 1000, || { let _ = AgentPayload::decode(&encoded); });
    bench("validate payload", 1000, || { let _ = validate_payload(&payload); });
    bench("validate payload (from string)", 1000, || { let _ = validate_payload_str(&encoded); });
    bench("compress (completed_sequences)", 1000, || {
        let _ = compress_context(&records, CompressStrategy::CompletedSequences, 2, None, None, false);
    });
    bench("compress (sliding_window, ws=100)", 1000, || {
        let _ = compress_context(&records, CompressStrategy::SlidingWindow, 2, None, Some(100), false);
    });
    bench("chunk (budget=2000)", 1000, || {
        let _ = chunk_payload(&records, 2000, Some("t1"), &[], 0, None, None);
    });
    bench("count_tokens", 1000, || { let _ = count_tokens(&encoded); });
    bench("dedup_mem", 1000, || { let _ = dedup_mem(&records); });

    println!();
    println!("  Payload size: {} bytes", encoded.len());
    println!("  Records: {}", records.len());
}
