# Installation

## Requirements

### Python

| Requirement | Minimum version |
|---|---|
| Python | 3.8 |
| Rust (optional) | 1.70 |
| maturin (optional) | 1.4 |

### JavaScript

| Requirement | Minimum version |
|---|---|
| Node.js | 16 |
| npm | 7 |

---

## Python

### Python Only

Clone the repository and install in editable mode. No Rust required.
The pure Python reference implementation has zero runtime dependencies.

```bash
git clone https://github.com/makroumi/ulmen
cd ulmen
pip install -e .
```
Verify:
```python
import ulmen
print(ulmen.__version__)    # 1.0.0
print(ulmen.RUST_AVAILABLE) # False
```

### With Rust Acceleration
The Rust extension provides 13x faster binary encode and 11x faster text
encode. It requires the Rust toolchain and maturin.

#### Step`: Install Rust
```Bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
rustc --version
```

#### Step 2: Install maturin
```Bash
pip install "maturin>=1.4,<2.0"
```

#### Step 3: Build and install the extension
```Bash
maturin develop --release
```
The `--release` flag enables full compiler optimizations. Omit it only
during Rust development when compile speed matters more than runtime speed.

#### Verify the Rust extension loaded
```Python
import ulmen
print(ulmen.RUST_AVAILABLE)   # True
print(ulmen.UlmenDictRust)    # <class 'ulmen._ulmen_rust.UlmenDictRust'>
```

## Development Install
Install with all development dependencies:
```bash
pip install -e ".[dev]"
```
This installs: maturin, pytest, pytest-cov, pytest-benchmark, ruff, mypy.

### Fallback Behaviour
If the Rust extension is not compiled or fails to load, the library
automatically falls back to the pure Python implementation. The API
is identical. No code changes required.
```python
from ulmen import RUST_AVAILABLE, UlmenDictRust

# UlmenDictRust is always importable.
# When Rust is unavailable it is a Python shim with the same interface.
ld = UlmenDictRust(records)
```

---

## JavaScript/TypeScript

### npm
```bash
npm install ulmen
```

### yarn
```bash
yarn add ulmen
```

### pnpm
```bash
pnpm add ulmen
```

### Verify
```javascript
import { encode } from 'ulmen';

const records = [{ id: 1, name: "test" }];
const binary = await encode(records);
console.log(binary.length); // byte count
```

### WASM Initialization
The JavaScript package bundles an 82KB WebAssembly module compiled from
the same Rust core as the Python acceleration layer. The module initializes
automatically on first use. No configuration is required.

In Node.js the WASM binary is loaded directly from the file system.
In browsers it is fetched relative to the script location.

### TypeScript
Full type definitions are included. No additional type packages required.
```typescript
import { UlmenDict, encode } from 'ulmen';

interface Record {
  id: number;
  name: string;
}

const records: Record[] = [{ id: 1, name: "Alice" }];
const binary: Uint8Array = await encode(records);
```

### Deno
```javascript
import { encode } from "npm:ulmen";

const binary = await encode([{ id: 1 }]);
```

### Browser via CDN
```HTML
<script type="module">
import { encode } from 'https://cdn.jsdelivr.net/npm/ulmen/dist/index.js';

const binary = await encode([{ id: 1, name: "test" }]);
console.log(`Encoded: ${binary.length} bytes`);
</script>
```

---

## Build from Source

### Python wheel
```bash
maturin build --release
pip install target/wheels/ulmen-*.whl
```

### JavaScript package
```bash
# Build the WASM module
cd wasm
wasm-pack build --target web --out-dir pkg

# Build the npm package
cd ../js
npm run build
```

---

## Run the Test Suite
### Python
```bash
pytest tests/ -v
pytest tests/ --cov=ulmen --cov-report=term-missing
```
100% statement coverage. All tests pass with and without
the Rust extension.

### JavaScript
```bash
cd js
npm test
```

### Performance benchmarks
```bash
pytest tests/perf/test_benchmark.py -v -s
```