# Phoenix Cache: Rust Core Architecture

This document details the architectural design of the Phoenix Cache Rust core. It explains the philosophy, structure, and key patterns that enable high-performance, adaptive compression for columnar data. This is the blueprint for understanding, maintaining, and extending the library.

## 1. Core Philosophy: Separation of Concerns

The Phoenix Cache Rust core is built upon a strict adherence to the **Single Responsibility Principle (SRP)** and **Separation of Concerns (SOC)**. Each module and function has a single, well-defined job, minimizing dependencies and maximizing testability, maintainability, and performance.

Key architectural tenets:

*   **Anti-Corruption Layer (FFI):** A strict boundary isolates the Rust core from the complexities and vagaries of foreign function interfaces (FFI), particularly Python/Polars.
*   **Pipeline Model:** Compression and decompression are modeled as a sequence of discrete, pluggable steps.
*   **Stateless Kernels:** Individual compression/decompression algorithms (kernels) are pure, stateless functions.
*   **In-Place Operations (Performance):** Where feasible, algorithms operate directly on mutable buffers to avoid unnecessary memory allocations and copies.
*   **Panic-Free Design:** The Rust core is designed to be panic-free. All recoverable errors are explicitly handled via `Result` types.

## 2. High-Level Architecture Overview

The Phoenix Cache Rust core can be visualized as a highly specialized factory floor:

*   **The Lobby (FFI Layer):** Handles incoming raw materials (Python objects) and ships out finished products (Python objects). It's the only part that speaks "Python."
*   **The Manager's Office (Orchestrator):** Oversees the entire production process for each chunk of material. It decides what happens when.
*   **The Planning Department (Planner):** Analyzes raw materials and draws up detailed manufacturing plans.
*   **The Shop Floor (Executor):** Receives a plan and directs the specialized machinery (kernels) to perform the work.
*   **Specialized Machinery (Kernels):** Individual, highly optimized machines (algorithms) that perform specific transformations (e.g., Delta encoder, RLE compressor).
*   **Utility & Error Control:** Supporting services like general-purpose tools and quality control.

## 3. Detailed File Structure

The project adheres to the following modular file structure:

```
src/
├── lib.rs              # Crate root, defines the Python module, and declares all top-level modules.
|
├── ffi/                # The Anti-Corruption Layer for Python.
│   └── python.rs       # Python <-> Rust FFI logic. Handles type conversion, GIL, and error translation.
|
├── pipeline/           # The Core Workflow Management.
│   ├── mod.rs          # Public facade for the pipeline module (exports orchestrator functions).
│   ├── orchestrator.rs # The "General Contractor": Manages end-to-end compression/decompression.
│   ├── planner.rs      # The "Strategist": Analyzes data and plans optimal kernel sequences.
│   └── executor.rs     # The "Foreman": Executes a given pipeline plan on data.
|
├── kernels/            # The Stateless Compression/Decompression Algorithms.
│   ├── mod.rs          # Public facade for the kernels module (re-exports kernel functions).
│   ├── delta.rs        # Delta encoding/decoding kernel.
│   ├── rle.rs          # Run-Length encoding/decoding kernel.
│   ├── leb128.rs       # Unsigned LEB128 variable-length integer encoding/decoding.
│   ├── zigzag.rs       # Zigzag transform (converts signed to unsigned for efficient encoding).
│   ├── bitpack.rs      # Fixed-width bit-packing/unpacking.
│   ├── shuffle.rs      # Byte-shuffling for improved entropy coding.
│   └── zstd.rs         # Zstandard compression/decompression wrapper.
|
├── null_handling/      # Logic for Managing Nullability.
│   ├── mod.rs          # Public facade for null handling module.
│   └── bitmap.rs       # Handles stripping and reapplying Arrow-compatible validity bitmaps.
|
├── error.rs            # Centralized, custom error type for the entire library.
└── utils.rs            # Shared low-level utilities and FFI helpers.
```

## 4. Key Architectural Patterns & Data Flow

### A. The FFI Anti-Corruption Layer (`ffi/python.rs`)

*   **Role:** This is the only module that directly interacts with `pyo3`, `polars`, or `arrow` Python bindings. It shields the rest of the Rust core from Python's complexities (e.g., GIL management, Python object lifetimes).
*   **Responsibilities:**
    *   Converts Python objects (`PyAny`, `polars.Series`) into pure Rust primitive types (`&[u8]`, `Option<&[u8]>`, `&str`).
    *   **Releases the Python GIL** (`py.allow_threads`) before calling CPU-bound Rust code.
    *   Translates internal Rust `Result<..., PhoenixError>` into Python `PyResult<...>` (specifically, `ValueError`).
    *   Converts final Rust results (e.g., `Vec<u8>`, `Option<Vec<u8>>`) back into Python objects.

### B. The Pipeline Orchestrator (`pipeline/orchestrator.rs`)

*   **Role:** The top-level workflow manager for compression and decompression. It orchestrates calls to other modules.
*   **Responsibilities:**
    *   **Null Handling:** Before compression, it calls `null_handling/bitmap.rs` to extract the validity bitmap and obtain a contiguous buffer of *only* valid data. After decompression, it uses the bitmap to re-insert nulls.
    *   **Planning:** It calls `pipeline/planner.rs` to determine the optimal compression plan for the *valid* data.
    *   **Execution:** It calls `pipeline/executor.rs` to run the compression/decompression plan on the *valid* data.
    *   **Artifact Packaging:** It defines and manages the on-disk format (`CompressedArtifact` struct) for storing the compressed data, null bitmap, and metadata.

#### Data Flow (Compression `compress_py` in `ffi/python.rs`):

1.  `ffi/python.rs` receives `polars.Series`.
2.  `ffi/python.rs` converts `polars.Series` to `&[u8]` (data slice) and `Option<&[u8]>` (validity slice) using zero-copy.
3.  `ffi/python.rs` calls `pipeline::orchestrator::compress_chunk(data_slice, validity_slice_opt, dtype_str)`.
4.  `orchestrator.rs`:
    *   Calculates `num_valid_rows`.
    *   Compresses `validity_slice_opt` using a simple `RLE + Zstd` pipeline, yielding `compressed_nullmap`.
    *   Calls `pipeline::planner::plan_pipeline_for_type(data_slice, dtype_str)` to get `pipeline_json`.
    *   Calls `pipeline::executor::execute_compress_pipeline(data_slice, dtype_str, pipeline_json)` to get `compressed_data`.
    *   Constructs `CompressedArtifact` (`num_valid_rows`, `compressed_nullmap`, `compressed_data`).
    *   Serializes `CompressedArtifact` to `Vec<u8>`.
5.  `orchestrator.rs` returns `Result<Vec<u8>, PhoenixError>`.
6.  `ffi/python.rs` receives `Vec<u8>` and converts `PhoenixError` to `PyError`.

#### Data Flow (Decompression `decompress_py` in `ffi/python.rs`):

1.  `ffi/python.rs` receives `bytes` (artifact), `plan_json`, `original_type`.
2.  `ffi/python.rs` calls `pipeline::orchestrator::decompress_chunk(artifact_bytes, plan_json, original_type)`.
3.  `orchestrator.rs`:
    *   Deserializes `artifact_bytes` into `CompressedArtifact`.
    *   Decompresses `compressed_nullmap` using `RLE + Zstd` to get `decompressed_nullmap`.
    *   Calls `pipeline::executor::execute_decompress_pipeline(compressed_data, original_type, pipeline_json, num_valid_rows)` to get `decompressed_data`.
    *   Returns `Result<(Vec<u8>, Option<Vec<u8>>), PhoenixError>`.
4.  `ffi/python.rs` receives `(Vec<u8>, Option<Vec<u8>>)` (raw data, optional validity).
5.  `ffi/python.rs` calls `utils::reconstruct_series(...)` to convert these raw parts back to a `polars.Series`.
6.  `ffi/python.rs` calls `utils::series_to_py(...)` to convert the Rust `Series` to a Python object and returns `PyResult<PyObject>`.

### C. The Pipeline Planner (`pipeline/planner.rs`)

*   **Role:** The "brain" of the adaptive compression. Analyzes data to decide the best kernel sequence.
*   **Responsibilities:**
    *   `analyze_data<T>(data: &[T])`: Performs a **single-pass, zero-allocation** statistical analysis (e.g., constant values, delta sparsity, bit-width requirements) on a *typed slice of valid data*.
    *   `build_pipeline_from_profile(profile: &DataProfile) -> Result<String, PhoenixError>`: Translates the statistical `DataProfile` into a JSON array of pipeline operations based on predefined heuristics.
*   **Key Design:** It is **pure Rust**, operating only on `&[T]` slices and returning `String` (JSON). It has no knowledge of FFI or nulls.

### D. The Pipeline Executor (`pipeline/executor.rs`)

*   **Role:** The "foreman" that executes a given compression/decompression plan.
*   **Responsibilities:**
    *   `execute_compress_pipeline(...)` / `execute_decompress_pipeline(...)`: Parses the `pipeline_json` and sequentially calls the appropriate kernel functions.
    *   **Buffer Swapping:** Implements a critical performance optimization by using a pair of mutable buffers. Kernels write their output into one buffer, which then becomes the input for the next step, avoiding `N` allocations for an `N`-step pipeline.
    *   **Type Tracking (Decompression):** For decompression, it intelligently tracks intermediate data types (e.g., after `zigzag` conversion) using a `type_stack` to ensure kernels are called with the correct type context.
*   **Key Design:** It is **pure Rust**, accepting `&[u8]` inputs and `&mut Vec<u8>` outputs. It orchestrates calls to `kernels/` functions.

### E. The Kernels (`kernels/`)

*   **Role:** The atomic, stateless compression and decompression algorithms.
*   **Responsibilities:** Each `kernel_name.rs` file implements `encode<T>` and `decode<T>` functions.
*   **Contract:**
    *   `pub fn encode<T>(input_slice: &[T], output_buf: &mut Vec<u8>, ...) -> Result<(), PhoenixError>`
    *   `pub fn decode<T>(input_bytes: &[u8], output_buf: &mut Vec<u8>, ...) -> Result<(), PhoenixError>`
    *   They are **pure Rust**, generic over `T`, and operate on raw bytes/typed slices, writing to the provided buffer.
    *   They are **panic-free**, propagating all errors via `Result`.
    *   They typically wrap highly optimized, in-place internal helper functions.

### F. Null Handling (`null_handling/bitmap.rs`)

*   **Role:** Specialized logic for managing data validity using Arrow-compatible bitmaps.
*   **Responsibilities:**
    *   `strip_valid_data<T>(data: &[T], validity: &Bitmap) -> Result<Vec<T>, PhoenixError>`: Extracts only the valid data from a full data slice, creating a tight vector.
    *   `reapply_bitmap<T>(valid_data_buffer: Buffer<T::Native>, validity_bitmap: Bitmap) -> Result<PrimitiveArray<T>, PhoenixError>`: Reconstructs an Arrow array with nulls by applying a bitmap to a buffer of valid data.
*   **Key Design:** It is **pure Rust**, operates on raw Arrow types, and is highly performant (e.g., zero-copy reapplication).

### G. Utilities (`utils.rs`) and Errors (`error.rs`)

*   **`utils.rs`:**
    *   **Core Utilities:** Pure Rust helpers (`safe_bytes_to_typed_slice` using `bytemuck`, `typed_slice_to_bytes`, `get_element_size`).
    *   **FFI Helpers:** Functions specifically for `ffi/python.rs` to convert between Python/Polars objects and the primitive Rust types that the core library expects (`py_to_series`, `reconstruct_series`, `series_to_py`).
*   **`error.rs`:** Defines `PhoenixError`, a custom `thiserror`-driven enum for all library errors, ensuring consistent and traceable error propagation throughout the Rust core and clean conversion to `PyErr` at the FFI boundary.
