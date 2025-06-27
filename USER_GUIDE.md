# Phoenix Cache User Guide

This guide provides a comprehensive overview of how to use the `phoenix_cache` Python library. It is designed for Python developers who want to leverage high-performance columnar data compression without diving into the underlying Rust implementation.

## 📦 Installation

To use Phoenix Cache, you need Python 3.8+ and the `maturin` build tool.

1.  **Install `maturin` (if you haven't already):**
    ```bash
    pip install maturin
    ```
2.  **Install Phoenix Cache:**
    Navigate to the root directory of the Phoenix Cache repository (where `pyproject.toml` is located) and run:
    ```bash
    maturin develop --release
    ```
    This command compiles the Rust extension module in release mode (for maximum performance) and installs the `phoenix_cache` package directly into your current Python environment. This allows you to `import phoenix_cache` in your Python scripts.

## 🚀 Core API Reference

The `phoenix_cache` library exposes three primary functions for interacting with the Rust core: `compress`, `decompress`, and `plan`.

### `phoenix_cache.compress(series: polars.Series) -> bytes`

Compresses a Polars Series into a compact binary artifact.

*   **Description:** This is the main entry point for compressing your data. It takes a Polars Series, automatically analyzes its statistical properties, strips null values, generates an optimal compression pipeline, executes that pipeline, and bundles the compressed data and validity bitmap into a single binary output.
*   **Parameters:**
    *   `series` (`polars.Series`): The Polars Series you wish to compress. It supports all standard numeric integer types (`Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`).
*   **Returns:**
    *   `bytes`: A `bytes` object representing the fully compressed artifact. This artifact contains both the compressed data and the compressed null bitmap (if any nulls were present).
*   **Important Notes:**
    *   This function handles the conversion from Polars/PyArrow to Rust's internal representation efficiently, minimizing memory copies.
    *   The generated compression *plan* is not returned by this function. You must separately call `phoenix_cache.plan()` if you intend to store the plan alongside your compressed data for later decompression. It is highly recommended to store the plan (e.g., in a separate metadata manifest) for accurate decompression.

**Example:**

```python
import polars as pl
import phoenix_cache

data = [1, 2, 3, 3, 3, 4, 5, 5, 5, 5, 6]
my_series = pl.Series("integer_column", data, dtype=pl.Int32)

compressed_artifact = phoenix_cache.compress(my_series)
print(f"Compressed size: {len(compressed_artifact)} bytes")
```

### `phoenix_cache.decompress(artifact: bytes, plan_json: str, original_dtype: str) -> polars.Series`

Decompresses a compressed binary artifact back into its original Polars Series.

*   **Description:** This function takes a compressed artifact (produced by `phoenix_cache.compress`), the original compression plan, and the original data type, to reconstruct the series. It reverses the compression pipeline and re-applies the validity bitmap (if one was originally present).
*   **Parameters:**
    *   `artifact` (`bytes`): The compressed binary artifact.
    *   `plan_json` (`str`): The JSON string representing the exact compression pipeline used during encoding. **This is crucial for correct decompression and MUST match the original plan.**
    *   `original_dtype` (`str`): The string representation of the original Polars/Arrow data type (e.g., `"Int64"`, `"UInt32"`). This is necessary to correctly interpret the decompressed bytes.
*   **Returns:**
    *   `polars.Series`: The fully reconstructed Polars Series, identical to the original (including nulls).

**Example:**

```python
import polars as pl
import phoenix_cache

original_data = [100, 101, None, 102, 103, None, 104]
original_series = pl.Series("sensor_data", original_data, dtype=pl.Int64)

# To decompress, you need the original plan and dtype.
# In a real system, you would store these as metadata with the compressed artifact.
# For this example, we generate the plan dynamically.
plan_json = phoenix_cache.plan(original_series.to_arrow().to_pylist(), original_series.dtype.base_type().__str__())
print(f"Plan used: {plan_json}")

compressed_artifact = phoenix_cache.compress(original_series)

decompressed_series = phoenix_cache.decompress(
    compressed_artifact,
    plan_json,
    original_series.dtype.base_type().__str__()
)

print("\nOriginal Series:")
print(original_series)
print("\nDecompressed Series:")
print(decompressed_series)

assert original_series.equals(decompressed_series)
print("\nDecompression successful: Series match!")
```

### `phoenix_cache.plan(data_list: list, original_dtype: str) -> str`

Generates an optimal compression pipeline plan for a given set of raw data.

*   **Description:** This function analyzes the statistical properties of a raw data chunk (specifically, a list of primitive Python types that can be interpreted as bytes) and returns a JSON string representing the optimal sequence of compression operations. This plan is used by the `compress` and `decompress` functions.
*   **Parameters:**
    *   `data_list` (`list`): A Python list containing the raw data values (e.g., `[1, 2, 3]`). This data should be the *valid, non-null* data if you've already handled nulls separately. For convenience with Polars, you can pass `series.to_arrow().to_pylist()`.
    *   `original_dtype` (`str`): The string representation of the data's original type (e.g., `"Int64"`, `"UInt32"`). This helps the planner interpret the data correctly.
*   **Returns:**
    *   `str`: A JSON string representing the generated pipeline plan.
*   **Important Notes:**
    *   This function currently operates on the *valid, non-null data*. Null handling (stripping before planning, re-applying after decompression) is managed by the `compress` and `decompress` functions, which internally call this `plan` function with the validity-stripped data.
    *   The planner's heuristics are optimized for performance and compression ratio. Future versions may introduce more advanced planning strategies.

**Example:**

```python
import polars as pl
import phoenix_cache

# Example with a list of raw data
raw_data = [100, 100, 100, 100, 100] # Constant data
plan_for_constant = phoenix_cache.plan(raw_data, "Int32")
print(f"Plan for constant data: {plan_for_constant}")
# Expected: [{"op": "rle", "params": {}}, {"op": "zstd", "params": {"level": 3}}]

raw_data_time_series = [100, 101, 103, 102, 104, 101] # Small deltas
plan_for_time_series = phoenix_cache.plan(raw_data_time_series, "Int16")
print(f"Plan for time series data: {plan_for_time_series}")
# Expected: [{"op": "delta"}, {"op": "zigzag"}, {"op": "bitpack"}, {"op": "shuffle"}, {"op": "zstd"}]
```

## ⚠️ Error Handling

Errors originating from the Rust core are caught and translated into Python `ValueError` exceptions. These exceptions will contain a descriptive message explaining the nature of the error (e.g., unsupported type, buffer mismatch, decoding failure).

You should wrap calls to `phoenix_cache` functions in `try...except ValueError` blocks for robust applications.

```python
import phoenix_cache

try:
    # Attempt to plan with an unsupported type
    phoenix_cache.plan([1, 2, 3], "UnsupportedType")
except ValueError as e:
    print(f"Caught an expected error: {e}")
```
