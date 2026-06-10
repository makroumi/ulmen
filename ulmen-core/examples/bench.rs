use std::time::Instant;
use ulmen_core::*;

fn main() {
    let mut records = Vec::with_capacity(1000);
    for i in 0..1000u64 {
        records.push(AgentRecord {
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

    // Warmup
    for _ in 0..100 { let _ = payload.encode(); }

    let iters = 1000;
    let start = Instant::now();
    for _ in 0..iters { let _ = payload.encode(); }
    let elapsed = start.elapsed();
    let us = elapsed.as_micros() as f64 / iters as f64;
    println!("Native Rust encode 1000 records: {:.0} us ({:.2} us/rec)", us, us / 1000.0);

    let encoded = payload.encode();
    for _ in 0..100 { let _ = AgentPayload::decode(&encoded); }
    let start2 = Instant::now();
    for _ in 0..iters { let _ = AgentPayload::decode(&encoded); }
    let elapsed2 = start2.elapsed();
    let us2 = elapsed2.as_micros() as f64 / iters as f64;
    println!("Native Rust decode 1000 records: {:.0} us ({:.2} us/rec)", us2, us2 / 1000.0);
    println!("Payload size: {} bytes", encoded.len());
}
