# Installation Guide

## Python Package

The LUMEN library is published on PyPI as `lumen-notation`. Install it with:

```bash
pip install lumen-notation
```

If you prefer to work from source:

```bash
git clone https://github.com/lumen-format/lumen-python
cd lumen-python
pip install -e .
```

The pure‑Python reference implementation works out‑of‑the‑box and has **zero runtime dependencies**.

## Optional Rust Acceleration

For maximum performance you can build the optional Rust extension using [Maturin](https://github.com/PyO3/maturin).

```bash
# Install maturin (requires Rust toolchain)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
pip install maturin

# Build and install the Rust extension (release mode)
maturin develop --release
```

If the Rust build succeeds, the module will automatically expose `LumenDictRust` and `LumenDictFullRust`. If the build fails or you skip it, the Python shim will fall back to the pure‑Python implementation.

## Verify Installation

```python
from lumen import RUST_AVAILABLE
print("Rust extension available:", RUST_AVAILABLE)
```

You should see `True` if the Rust module was built successfully.
