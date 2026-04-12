# Installation

## Requirements

| Requirement | Minimum version |
|---|---|
| Python | 3.8 |
| Rust (optional) | 1.70 |
| maturin (optional) | 1.4 |

---

## Python Only

Clone the repository and install in editable mode. No Rust required.
The pure Python reference implementation has zero runtime dependencies.

```bash
git clone https://github.com/makroumi/lumen
cd lumen
pip install -e .
```

Verify:

```Python
import lumen
print(lumen.__version__)   # 1.0.0
print(lumen.RUST_AVAILABLE)  # False
```

## With Rust Acceleration
The Rust extension provides 13x faster binary encode and 11x faster text
encode. It requires the Rust toolchain and maturin.

### Step 1: Install Rust
```Bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
rustc --version
```

### Step 2: Install maturin
```Bash
pip install "maturin>=1.4,<2.0"
```

### Step 3: Build and install the extension
``` Bash
maturin develop --release
```

The '--release' flag enables full compiler optimizations. Omit it only
during Rust development when compile speed matters more than runtime speed.

### Verify the Rust extension loaded
```Python
import lumen
print(lumen.RUST_AVAILABLE)   # True
print(lumen.LumenDictRust)    # <class 'lumen._lumen_rust.LumenDictRust'>
```

---

## Development Install
Install with all development dependencies:

```Bash
pip install -e ".[dev]"
```

This installs: maturin, pytest, pytest-cov, pytest-benchmark, ruff, mypy.

---

## Fallback Behaviour
If the Rust extension is not compiled or fails to load, the library
automatically falls back to the pure Python implementation. The API
is identical. No code changes required.

```Python
from lumen import RUST_AVAILABLE, LumenDictRust

# LumenDictRust is always importable.
# When Rust is unavailable it is a Python shim with the same interface.
ld = LumenDictRust(records)
```
---

## Run the Test Suite
```Bash
pytest tests/ -v
```
All 862 tests pass with and without the Rust extension.

To run the benchmark report:

```Bash
pytest tests/perf/test_benchmark.py -v -s```