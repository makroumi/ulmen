# LUMEN V1 Documentation

Lightweight Universal Minimal Encoding Notation

Copyright (c) El Mehdi Makroumi. All rights reserved.
Proprietary and confidential.

---

LUMEN is a serialization format built for three goals: smallest possible output, fastest possible encode and decode, and native compatibility with language model context windows.

It ships as a pure Python library with an optional Rust acceleration layer
that is drop-in compatible and produces byte-identical output.

---

## Documentation Map

### Getting Started

| Document | Description |
|---|---|
| [Installation](getting-started/installation.md) | Install from source, build the Rust extension, verify setup |
| [Quick Start](getting-started/quickstart.md) | Working examples in under five minutes |

### Guides

| Document | Description |
|---|---|
| [Binary Format](guides/binary-format.md) | Columnar binary encoding, pool and strategy selection |
| [Text Format](guides/text-format.md) | Human-readable line-oriented encoding |
| [LUMIA](guides/lumia.md) | LLM-native CSV surface for language model communication |
| [Compression](guides/compression.md) | Zlib, pool tuning, strategy selection, size tradeoffs |

### Reference

| Document | Description |
|---|---|
| [API Reference](reference/api.md) | Every class, method, parameter, and return type |
| [Constants](reference/constants.md) | All wire-format tags, strategy bytes, magic values |
| [Primitives](reference/primitives.md) | Low-level codec functions for custom encoders |
| [Benchmarks](reference/benchmarks.md) | Real measured size and speed numbers |

### LUMEN-AGENT

| Document | Description |
|---|---|
| [Overview](agent/overview.md) | What LUMEN-AGENT is and when to use it |
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
| LUMIA | `L\|` | LLM-native, token-efficient |
| LUMEN-AGENT | `LUMEN-AGENT v1` | Structured agentic protocol |

---

## Key Numbers

Measured on 1,000 records, 10 mixed-type columns, Python 3.12, rustc 1.92.

| Format | Size | vs JSON |
|---|---:|---:|
| JSON | 145,664 bytes | 100.0% |
| LUMEN binary | 32,701 bytes | 22.4% |
| LUMEN zlib-6 | 2,453 bytes | 1.7% |
| LUMEN text | 46,779 bytes | 32.1% |
| LUMIA | 57,403 bytes | 39.4% |

| Format | Encode ms | vs JSON |
|---|---:|---|
| JSON | 1.264 | baseline |
| LUMEN binary Python | 13.579 | 0.09x |
| LUMEN binary Rust | 1.013 | 1.2x faster |
| LUMEN text Rust | 1.018 | 1.2x faster |

---

## Test Coverage

1,271 tests across unit, integration, performance, and smoke suites.
100% statement coverage across all modules.
All tests pass with and without the Rust extension.