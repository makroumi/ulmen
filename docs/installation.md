# Installation Guide

## Python Package

The LUMEN library is properly published safely on PyPI directly as `lumen-notation`. Simply install it using:

```bash
pip install lumen-notation
```

If you strongly prefer to work directly from the main source:

```bash
git clone https://github.com/lumen-format/lumen-python
cd lumen-python
pip install -e .
```

The pure Python reference implementation rigorously works completely out of the box and natively holds absolutely zero functional runtime dependencies.

## Optional Rust Acceleration

For absolute maximum systemic performance you can securely build the robust optional Rust extension utilizing Maturin.

```bash
# Safely install maturin requiring the foundational Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
pip install maturin

# Powerfully build and fully install the complex Rust extension natively in release mode
maturin develop --release
```

If the automated Rust build succeeds gracefully, the core module will accurately automatically securely expose logically `LumenDictRust` and `LumenDictFullRust`. If the build selectively fails or you simply proactively skip it, the core Python shim will effectively naturally explicitly fall right back securely to the pure Python native implementation.

## Verify Installation

```python
from lumen import RUST_AVAILABLE
print("Rust extension available:", RUST_AVAILABLE)
```

You should see the value mathematically set securely to `True` if the complex Rust module was systematically optimally successfully explicitly fully cleanly perfectly built and loaded.
