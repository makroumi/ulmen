# ULMEN V1 Documentation

Ultra Lightweight Minimal Encoding Notation

Copyright (c) 2026 El Mehdi Makroumi. All rights reserved.
ULMEN is licensed under the Business Source License 1.1.
See the LICENSE file in the project root for full license information.

---

ULMEN is a serialization format built for three goals: smallest possible output, fastest possible encode and decode, and native compatibility with language model context windows.

It ships as a Python library with an optional Rust acceleration layer, and a JavaScript/TypeScript package powered by the same Rust core compiled to WebAssembly. All implementations produce byte-identical output.

---

## Documentation Map

### Getting Started

| Document | Description |
|---|---|
| [Installation](getting-started/installation.md) | Install from source, build the Rust extension, JavaScript setup |
| [Quick Start](getting-started/quickstart.md) | Working examples in under five minutes |

### Guides

| Document | Description |
|---|---|
| [Binary Format](guides/binary-format.md) | Columnar binary encoding, pool and strategy selection |
| [Text Format](guides/text-format.md) | Human-readable line-oriented encoding |
| [ULMEN](guides/ulmen.md) | LLM-native CSV surface for language model communication |
| [Compression](guides/compression.md) | Zlib, pool tuning, strategy selection, size tradeoffs |
| [JavaScript and TypeScript](guides/javascript.md) | WASM-powered encoding for Node.js, browsers, and edge runtimes |

### Reference

| Document | Description |
|---|---|
| [API Reference](reference/api.md) | Every class, method, parameter, and return type |
| [Constants](reference/constants.md) | All wire-format tags, strategy bytes, magic values |
| [Primitives](reference/primitives.md) | Low-level codec functions for custom encoders |
| [Benchmarks](reference/benchmarks.md) | Real measured size and speed numbers |

### ULMEN-AGENT

| Document | Description |
|---|---|
| [Overview](agent/overview.md) | What ULMEN-AGENT is and when to use it |
| [Specification](agent/spec.md) | Formal wire format and validation rules |
| [System Prompt](agent/system-prompt.md) | LLM system prompt for agent communication |

### Internals

| Document | Description |
|---|---|
| [Architecture](internals/architecture.md) | Codebase structure, module responsibilities |
| [Wire Format](internals/wire-format.md) | Binary format deep dive, all encoding rules |

---

## Surfaces at a Glance

| Surface | Prefix | Purpose |
|---|---|---|
| Binary | `LUMB` | Storage, IPC, network transport |
| Text | `records[N]:` | Human-readable, diff-friendly |
| ULMEN | `L\|` | LLM-native, token-efficient |
| ULMEN-AGENT | `ULMEN-AGENT v1` | Structured agentic protocol |

---

## Language Support

| Language | Installation | Acceleration | Output |
|---|---|---|---|
| Python 3.8+ | `pip install ulmen` | Rust via PyO3 | Canonical |
| JavaScript / TypeScript | `npm install ulmen` | Rust via WASM | Byte-identical |
| Node.js 16+ | `npm install ulmen` | Rust via WASM | Byte-identical |
| Browser | ES module import | Rust via WASM | Byte-identical |
| Deno | `npm:ulmen` | Rust via WASM | Byte-identical |
| Edge runtimes | `npm install ulmen` | Rust via WASM | Byte-identical |

All surfaces in all languages read and write the same binary format.
A payload written in Python can be decoded in JavaScript and vice versa.

---

## Key Numbers

Measured on 1,000 records, 10 mixed-type columns, Python 3.12, rustc 1.92.

| Format | Size | vs JSON |
|---|---:|---:|
| JSON | 145,664 bytes | 100.0% |
| ULMEN binary | 32,701 bytes | 22.4% |
| ULMEN zlib-6 | 2,453 bytes | 1.7% |
| ULMEN text | 46,779 bytes | 32.1% |
| ULMEN | 57,403 bytes | 39.4% |

| Format | Encode ms | vs JSON |
|---|---:|---|
| JSON | 1.264 | baseline |
| ULMEN binary Python | 13.579 | 0.09x |
| ULMEN binary Rust | 1.013 | 1.2x faster |
| ULMEN text Rust | 1.018 | 1.2x faster |
| ULMEN binary WASM | 0.7ms | 1.8x faster |

---

## Test Coverage

1,379 tests across unit, integration, performance, and smoke suites.
100% statement coverage across all modules.
All tests pass with and without the Rust extension.
