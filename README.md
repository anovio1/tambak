# Phoenix Cache

A high-performance, adaptive compression library for columnar data, powered by a meticulously engineered Rust core.

## 🚀 Overview

Phoenix Cache is designed to revolutionize how structured data is stored and retrieved in analytical and time-series applications. By leveraging a custom, adaptive compression pipeline implemented in Rust, Phoenix Cache delivers superior compression ratios and blazing-fast performance compared to traditional methods, while maintaining seamless interoperability with the Python data ecosystem (especially Polars and PyArrow).

The core philosophy revolves around dynamically selecting the optimal sequence of stateless compression kernels for a given data chunk. This "intelligent planning" ensures maximum efficiency, whether your data is sparse, has low cardinality, or exhibits strong temporal locality.

## ✨ Features

*   **Adaptive Compression:** Dynamically generates an optimal compression pipeline based on data characteristics.
*   **Rust-Powered Performance:** Core compression and decompression logic is written in highly optimized, panic-free Rust.
*   **Zero-Copy FFI:** Efficiently bridges between Python (Polars/PyArrow) and Rust, minimizing memory copies at the boundary.
*   **Composable Kernels:** A modular architecture of stateless compression algorithms (Delta, RLE, LEB128, Bitpacking, Zstd, etc.).
*   **Robust Error Handling:** Panics are caught and translated into meaningful Python exceptions.
*   **Null-Aware:** First-class support for nullable data, with efficient bitmap handling.

## 📦 Installation

Phoenix Cache is distributed as a Python package with a compiled Rust extension.

1.  **Prerequisites:**
    *   [Rust Toolchain](https://www.rust-lang.org/tools/install) (latest stable recommended)
    *   Python 3.8+
    *   `maturin` (Rust-to-Python build tool):
        ```bash
        pip install maturin
        ```

2.  **Install from Source:**
    Navigate to the root directory of this repository (where `pyproject.toml` is located) and run:
    ```bash
    maturin develop --release
    ```
    This command compiles the Rust code in release mode (optimized for speed) and installs the `phoenix_cache` Python package into your current environment.

## ⚡ Quick Start (Python)

Once installed, you can start compressing and decompressing Polars Series immediately.

```python
import polars as pl
import phoenix_cache
import numpy as np

# 1. Create a sample Polars Series with various data patterns (and some nulls!)
data_with_nulls = [100, 105, 105, 106, 106, 106, 106, 1000, 1005, None, 1010, 1015, None, 1020]
original_series = pl.Series("my_column", data_with_nulls, dtype=pl.Int64)

print("Original Series:")
print(original_series)
print(f"Original Series Size: {original_series.estimated_size()} bytes")

# 2. Plan the optimal compression pipeline for the series (optional, but good for understanding)
# The planner takes raw bytes of valid data. For a real series, you might need to extract this first.
# For simplicity, we just use the original type string. The FFI layer handles byte extraction.
plan_json = phoenix_cache.plan(original_series.to_arrow().to_pylist(), original_series.dtype.base_type().__str__())
print(f"\nGenerated Plan: {plan_json}")

# 3. Compress the Polars Series
# The `compress` function handles null stripping, planning, and execution internally.
compressed_artifact = phoenix_cache.compress(original_series)

print(f"\nCompressed Artifact Size: {len(compressed_artifact)} bytes")
print(f"Compression Ratio: {original_series.estimated_size() / len(compressed_artifact):.2f}x")

# 4. Decompress the artifact back into a Polars Series
# The `decompress` function needs the original plan and type to reverse the process.
decompressed_series = phoenix_cache.decompress(
    compressed_artifact,
    plan_json, # The plan needs to be stored and retrieved with the artifact
    original_series.dtype.base_type().__str__() # Original data type is also critical
)

print("\nDecompressed Series:")
print(decompressed_series)

# 5. Verify roundtrip integrity
assert original_series.equals(decompressed_series)
print("\nRoundtrip successful: Original and Decompressed series are identical!")
```

## 📚 Documentation

Dive deeper into Phoenix Cache with our comprehensive guides:

*   **[User Guide](USER_GUIDE.md)**: How to use the Python API.
*   **[Architecture](ARCHITECTURE.md)**: Understanding the Rust core's design principles.
*   **[Developer Guide](DEVELOPER_GUIDE.md)**: Contributing to the Rust codebase.
*   **[Kernel Guide](KERNEL_GUIDE.md)**: A playbook for adding new compression kernels.

## 🤝 Contributing

We welcome contributions! Please see the [Developer Guide](DEVELOPER_GUIDE.md) for details on how to set up your environment and contribute to the Rust core.

## 📄 License

This project is licensed under the MIT OR Apache-2.0 License.
