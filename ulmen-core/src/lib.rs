//! ulmen-core: Pure Rust agent protocol engine.
//!
//! Zero external dependencies. No PyO3.
//!
//! This crate provides native Rust types and functions for the
//! ULMEN-AGENT v1 protocol: encode, decode, validate, compress,
//! chunk, repair, and token counting.
//!
//! Used directly by uldb, ulmp, ulflow as a Cargo dependency.
//! The Python API is a thin wrapper in the ulmen-python crate.
//!
//! Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
//! Licensed under BSL-1.1.

#![forbid(unsafe_code)]

pub mod agent;
pub mod chunk;
pub mod compress;
pub mod repair;
pub mod tokens;
pub mod validate;

pub use agent::*;
pub use chunk::{chunk_payload, make_error_payload, merge_chunks};
pub use compress::*;
pub use repair::parse_llm_output;
pub use tokens::{count_tokens, count_tokens_with_overhead, estimate_tokens};
pub use validate::{validate_payload, validate_payload_str, ValidationError};
