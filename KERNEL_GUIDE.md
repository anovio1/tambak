# Phoenix Cache: Kernel Development Guide

This guide provides a step-by-step playbook for developing, implementing, and hardening new compression/decompression kernels for Phoenix Cache. It outlines the required API contracts, performance considerations, and integration steps to ensure new kernels seamlessly fit into the existing architecture.

## 1. Understanding the Kernel's Role

In the Phoenix Cache architecture, a "kernel" is a **pure, stateless, and highly optimized** algorithm that performs a single, specific data transformation (e.g., Delta encoding, RLE, Bitpacking).

*   **Location:** All kernels reside in the `src/kernels/` directory.
*   **Contract:** Every kernel must adhere to a strict `encode` and `decode` API, conforming to the `executor`'s buffer-swapping strategy.
*   **Independence:** Kernels are completely decoupled from `pyo3`, `polars`, and the FFI layer. They operate purely on Rust primitive types (`&[T]`, `&[u8]`) and write to provided `Vec<u8>` buffers.
*   **Panic-Free:** All kernels **must be panic-free**. Recoverable errors are returned via `Result<..., PhoenixError>`.

## 2. The Kernel API Contract

Each kernel module (`src/kernels/my_new_kernel.rs`) must expose two primary public functions:

### `pub fn encode<T>(input_slice: &[T], output_buf: &mut Vec<u8>, ...params) -> Result<(), PhoenixError>`

*   **Purpose:** Compresses/transforms `input_slice` data.
*   **`T` (Type Parameter):** The generic type of the data (e.g., `i32`, `u64`). This allows the kernel to operate directly on typed data.
*   **`input_slice` (`&[T]`):** An immutable slice of the *typed* input data. This is typically the data coming from the previous pipeline stage (or the raw data from the orchestrator).
*   **`output_buf` (`&mut Vec<u8>`):** A mutable reference to a `Vec<u8>` provided by the `executor`. The kernel **must** write its output bytes directly into this buffer. It should `clear()` this buffer at the start of its operation and then `extend_from_slice()` or `push()` bytes into it. This is crucial for performance (buffer reuse).
*   **`...params`:** Any specific parameters required by the kernel (e.g., `bit_width` for Bitpack, `order` for Delta).
*   **Returns:** `Result<(), PhoenixError>`. On success, returns `Ok(())`. On failure, returns a specific `PhoenixError`.

### `pub fn decode<T>(input_bytes: &[u8], output_buf: &mut Vec<u8>, ...params) -> Result<(), PhoenixError>`

*   **Purpose:** Decompresses/reverses the transformation on `input_bytes`.
*   **`T` (Type Parameter):** The generic type `T` that the data will be reconstructed into.
*   **`input_bytes` (`&[u8]`):** An immutable slice of the *raw bytes* produced by the previous pipeline stage (or the compressed artifact itself).
*   **`output_buf` (`&mut Vec<u8>`):** A mutable reference to a `Vec<u8>` provided by the `executor`. The kernel **must** write its reconstructed data (as raw bytes, converted from `Vec<T>`) directly into this buffer.
*   **Returns:** `Result<(), PhoenixError>`.

## 3. The 5-Point Kernel Hardening Playbook

When implementing a new kernel (or hardening an existing prototype), follow these steps to ensure it meets production standards:

1.  **Update Public API Signatures:**
    *   Change `encode` to `pub fn encode<T>(input_slice: &[T], output_buf: &mut Vec<u8>, ...params) -> Result<(), PhoenixError>`.
    *   Change `decode` to `pub fn decode<T>(input_bytes: &[u8], output_buf: &mut Vec<u8>, ...params) -> Result<(), PhoenixError>`.
    *   Add necessary trait bounds (e.g., `PrimInt`, `bytemuck::Pod`).

2.  **Implement Core Logic (In-Place for Performance):**
    *   For algorithms that can operate in-place (like Delta, Zigzag, Shuffle), create internal `_inplace` helper functions (e.g., `encode_slice_inplace<T>(data: &mut [T])`). This avoids intermediate allocations.
    *   In the public `encode`/`decode` functions:
        *   Take `input_bytes` (`&[u8]`).
        *   Convert `input_bytes` to `Vec<T>` (mutable copy) using `crate::utils::safe_bytes_to_typed_slice(...)? .to_vec()`.
        *   Call the `_inplace` helper on this `Vec<T>`.
        *   Convert the final `Vec<T>` back to `Vec<u8>` using `crate::utils::typed_slice_to_bytes(...)`.
        *   `output_buf.extend_from_slice(...)` to write the result.
    *   For algorithms that cannot operate in-place (like LEB128, Bitpack), ensure internal helper functions efficiently write to provided buffers or return minimal structures. Avoid collecting large `Vec`s unnecessarily.

3.  **Make It Panic-Free:**
    *   **Eliminate all `.unwrap()`/`.expect()` calls.** Replace them with `ok_or_else()`, `map_err()`, and `?` operator to propagate `PhoenixError`.
    *   Handle all possible error conditions (e.g., invalid input, truncation, overflow, malformed data).

4.  **Remove FFI Dispatcher Logic:**
    *   Ensure the kernel has no `match original_type` blocks. This dispatching is handled by the `executor` or `ffi/python.rs`.
    *   Remove any direct `unsafe` blocks. Use `crate::utils::safe_bytes_to_typed_slice` for safe byte-to-type casting.
    *   Remove any `use pyo3::...` statements (except if used within `#[cfg(test)]` for mocking, though it's best to avoid even that).

5.  **Rewrite Unit Tests:**
    *   Update `#[cfg(test)]` blocks to call the new, hardened, generic public API (`encode<T>(...)`, `decode<T>(...)`).
    *   Ensure tests cover successful roundtrips, edge cases (empty input, single value), and specific error conditions.
    *   Use `crate::utils::typed_slice_to_bytes` and `crate::utils::safe_bytes_to_typed_slice` for test data preparation.

## 4. Integration Steps

Once a kernel is implemented and hardened, integrate it into the wider system:

1.  **Declare in `kernels/mod.rs`:**
    ```rust
    // src/kernels/mod.rs
    pub mod my_new_kernel;
    // Add pub use self::my_new_kernel::{encode, decode}; // If function names are unique across kernels
    ```
    *Note: We will eventually use unique `encode_my_kernel` names to avoid collisions in `mod.rs` re-exports.*

2.  **Update `executor.rs`:**
    *   Add `use super::my_new_kernel;` to `executor.rs`.
    *   Add a `match` arm in `execute_compress_pipeline` and `execute_decompress_pipeline` for your new kernel's `op` string, calling its `encode`/`decode` function with the correct parameters.

3.  **Update `planner.rs` (Optional, but usually needed):**
    *   Modify `analyze_data` (if new statistical properties are relevant).
    *   Modify `build_pipeline_from_profile` heuristics to decide when to include your new kernel in the pipeline.

4.  **Add Python Integration Tests:**
    *   Create or update tests in the top-level `tests/` directory (`test_compression.py`) that exercise a full compression/decompression roundtrip that uses your new kernel. This is the ultimate validation.

By following this guide, you can confidently add powerful new compression capabilities to Phoenix Cache.
