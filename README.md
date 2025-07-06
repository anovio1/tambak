# Tambak Cache

A high-performance, adaptive compression library for columnar data, powered by a meticulously engineered Rust core.

## 🚀 Overview

Tambak Cache is designed to revolutionize how structured data is stored and retrieved in analytical and time-series applications. By leveraging a custom, adaptive compression pipeline implemented in Rust, Tambak Cache delivers superior compression ratios and blazing-fast performance compared to traditional methods, while maintaining seamless interoperability with the Python data ecosystem (especially Polars and PyArrow).

The core philosophy revolves around dynamically selecting the optimal sequence of stateless compression kernels for a given data chunk. This "intelligent planning" ensures maximum efficiency, whether your data is sparse, has low cardinality, or exhibits strong temporal locality.

## ✨ Features

*   **Adaptive Compression:** Dynamically generates an optimal compression pipeline based on data characteristics.
*   **Rust-Powered Performance:** Core compression and decompression logic is written in highly optimized, panic-free Rust.
*   **Zero-Copy FFI:** Efficiently bridges between Python (Polars/PyArrow) and Rust, minimizing memory copies at the boundary.
*   **Composable Kernels:** A modular architecture of stateless compression algorithms (Delta, RLE, LEB128, Bitpacking, Zstd, etc.).
*   **Robust Error Handling:** Panics are caught and translated into meaningful Python exceptions.
*   **Null-Aware:** First-class support for nullable data, with efficient bitmap handling.

## 📦 Installation

Tambak Cache is distributed as a Python package with a compiled Rust extension.

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
    This command compiles the Rust code in release mode (optimized for speed) and installs the `tambak_cache` Python package into your current environment.

## ⚡ Quick Start (Python)

Once installed, you can start compressing and decompressing Polars Series immediately.

```python
import polars as pl
import tambak_cache
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
plan_json = tambak_cache.plan(original_series.to_arrow().to_pylist(), original_series.dtype.base_type().__str__())
print(f"\nGenerated Plan: {plan_json}")

# 3. Compress the Polars Series
# The `compress` function handles null stripping, planning, and execution internally.
compressed_artifact = tambak_cache.compress(original_series)

print(f"\nCompressed Artifact Size: {len(compressed_artifact)} bytes")
print(f"Compression Ratio: {original_series.estimated_size() / len(compressed_artifact):.2f}x")

# 4. Decompress the artifact back into a Polars Series
# The `decompress` function needs the original plan and type to reverse the process.
decompressed_series = tambak_cache.decompress(
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

Dive deeper into Tambak Cache with our comprehensive guides:

*   **[User Guide](USER_GUIDE.md)**: How to use the Python API.
*   **[Architecture](ARCHITECTURE.md)**: Understanding the Rust core's design principles.
*   **[Developer Guide](DEVELOPER_GUIDE.md)**: Contributing to the Rust codebase.
*   **[Kernel Guide](KERNEL_GUIDE.md)**: A playbook for adding new compression kernels.

## 🤝 Contributing

We welcome contributions! Please see the [Developer Guide](DEVELOPER_GUIDE.md) for details on how to set up your environment and contribute to the Rust core.

## 📄 License

This project is licensed under the MIT OR Apache-2.0 License.

## Forward Facing

---

# **FEAT: Tambak Pipeline Planner: Configuration and Strategy Selection**
## `PlannerStrategy` and `PlannerConfig`

**Title: Tambak Pipeline Planner: Configuration and Strategy Selection**

**Introduction**

This document explains Tambak's compression planner changes:

*   **What:** `PlannerStrategy` and `PlannerConfig` for advanced pipeline optimization.
*   **Why:** Old planning chose best strategy from single-pass + 1 data sample, causing compression loss (regressions).
*   **Solution:** New multi-stage planning, allows dev to set progressively bigger samples or full data for better choices.
*   **Example Before/After:**

| Stage           | Before (Single-Stage Quick)                 | After (Multi-Stage TopNFull - Default)                 |
| :-------------- | :------------------------------------------ | :----------------------------------------------------- |
| **Stage 1**     | All candidates on small sample (e.g., 65KB) | All candidates on small sample (e.g., 128KB)           |
| **Stage 2**     | *(N/A - Best from Stage 1 chosen)*          | Top N candidates evaluated on **full dataset**         |
| **Final Choice**| Based on Stage 1 sample score               | Based on Stage 2 (full data) accuracy                  |

---

**1. `PlannerStrategy` Enum in `src/pipeline/models.rs` : Defining the Evaluation Approach**

This enum defines how the planner picks the best compression pipeline. Developers choose a strategy based on how much planning speed or compression accuracy they need.

*   **`TopNFull { top_n: usize }`**
    *   **Strategy:** Recommended default. Robust, two-stage evaluation prioritizing **accuracy**.
    *   **Process:**
        1.  **Stage 1 (Quick Filter - Sample):** Checks all candidates on a small data sample.
        2.  **Stage 2 (Precise Check - Full Data):** Re-evaluates the Top `top_n` candidates on the **entire dataset** for the final decision.
    *   **Benefit:** Gives the most accurate result, fixing issues from misleading samples.
    *   **Design Note:** Planning time depends on `top_n` times the full dataset processing.

*   **`TwoStageSample { small_sample_size: usize, large_sample_size: usize }`**
    *   **Strategy:** Two-stage, purely sample-based evaluation prioritizing **planning speed**.
    *   **Process:**
        1.  **Stage 1 (Quick Filter - Small Sample):** All candidates checked on `small_sample_size`.
        2.  **Stage 2 (Refined Check - Larger Sample):** Top N candidates checked on a `large_sample_size` (not the full dataset).
    *   **Benefit:** Faster planning, especially for very large datasets, as it avoids full-data processing.
    *   **Warning:** Can still mispredict if the larger sample isn't truly representative. Better accuracy than `Quick`, but less than `TopNFull`.

*   **`Quick { sample_size: usize }`**
    *   **Strategy:** Single-stage evaluation for **maximum planning speed**.
    *   **Process:** All candidates checked on a single `sample_size`. Best score wins.
    *   **Benefit:** Fastest planning.
    *   **Warning:** Most likely to mispredict. Use when planning speed is the highest priority and small errors are acceptable.

**2. `PlannerConfig` Struct in `src/pipeline/models.rs` : Configuring the Planning Process**

This struct holds the main settings for the pipeline planner.

```rust
// Conceptual Rust Structure
#[derive(Debug, Clone)]
pub struct PlannerConfig {
    /// The chosen evaluation `PlannerStrategy` (e.g., TopNFull, Quick).
    pub strategy: PlannerStrategy,
    
    /// List of final entropy coders (e.g., Zstd, Ans) to consider. REQUIRED.
    pub entropy_coders: Vec<Operation>,
}
```

**Configuration Capabilities:**

*   **Strategy:** Pick the `PlannerStrategy` (e.g., `PlannerStrategy::TopNFull { top_n: 5 }`).
*   **Entropy Coders:** Provide a `Vec<Operation>` (e.g., `vec![Operation::Zstd { level: 3 }]`) to specify end-stage compression options.
*   **Default:** Use `PlannerConfig::default()` for a robust `TopNFull { top_n: 3 }` strategy with standard entropy coders.
*   **Integration:** Pass this `PlannerConfig` to the top-level `plan_pipeline` function.

**Extending the System:**

*   New `Operation` types in `models.rs` are automatically considered by `generate_candidate_pipelines`.
*   More advanced configurations (e.g., custom `core_transforms`) can be added to `PlannerConfig` later.