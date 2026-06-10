//! ulmen-core: Pure Rust agent protocol engine.
//!
//! Zero external dependencies. No PyO3.
//!
//! This crate provides native Rust types and functions for the
//! ULMEN-AGENT v1 protocol: encode, decode, validate, compress.
//!
//! Used directly by uldb, ulmp, ulflow as a Cargo dependency.
//! The Python API is a thin wrapper in the ulmen-python crate.
//!
//! Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
//! Licensed under BSL-1.1.

#![forbid(unsafe_code)]
#![deny(clippy::all)]

pub mod agent;
pub mod tokens;

pub use agent::*;
