Of course. You are absolutely right. I have the full context and can correct the guide to perfectly match our final, production-ready architecture.

Here is the revised and corrected `KERNEL_GUIDE.md`.

---

# Phoenix Cache: Kernel Development Guide (v2 - Production Standard)

This guide provides a step-by-step playbook for developing, implementing, and hardening new compression/decompression kernels for Phoenix Cache. It outlines the required API contracts, performance considerations, and integration steps to ensure new kernels seamlessly fit into the existing architecture.

## 1. Understanding the Kernel's Role

In the Phoenix Cache architecture, a "kernel" is a **pure, stateless, and highly optimized** algorithm that performs a single, specific data transformation (e.g., Delta encoding, RLE, Bitpacking).

*   **Location:** All kernels reside in the `src/kernels/` directory.
*   **Contract:** Every kernel must adhere to a strict `encode` and `decode` API, conforming to the `executor`'s buffer-swapping strategy.
*   **Independence:** Kernels are completely decoupled from `pyo3`, `polars`, and the FFI layer. The central dispatcher in `kernels/mod.rs` handles all type-casting. Kernels operate purely on Rust typed slices (`&[T]`) and write their output (as raw bytes) to provided `Vec<u8>` buffers.
*   **Panic-Free:** All kernels **must be panic-free**. Recoverable errors are returned via `Result<..., PhoenixError>`.

## 2. The Kernel API Contract

Each kernel module (`src/kernels/my_new_kernel.rs`) must expose two primary public functions with these exact signatures:

### `pub fn encode<T>(input_slice: &[T], output_buf: &mut Vec<u8>, ...params) -> Result<(), PhoenixError>`

*   **Purpose:** Compresses or transforms the data from `input_slice`.
*   **`T` (Type Parameter):** The generic type of the data (e.g., `i32`, `u64`).
*   **`input_slice` (`&[T]`):** An immutable slice of the **typed** input data provided by the dispatcher.
*   **`output_buf` (`&mut Vec<u8>`):** A mutable reference to a `Vec<u8>` provided by the `executor`. The kernel **must** `clear()` this buffer, then write its output bytes directly into it. This is crucial for performance.
*   **`...params`:** Any specific parameters required by the kernel (e.g., `bit_width` for Bitpack).
*   **Returns:** `Result<(), PhoenixError>`.

### `pub fn decode<T>(input_slice: &[T], output_buf: &mut Vec<u8>, ...params) -> Result<(), PhoenixError>`

*   **Purpose:** Decompresses or reverses the transformation.
*   **`T` (Type Parameter):** The generic type `T` that the data was originally.
*   **`input_slice` (`&[T]`):** An immutable slice of the **typed** input data. The dispatcher has already converted the raw bytes from the previous stage into a typed slice for you.
*   **`output_buf` (`&mut Vec<u8>`):** A mutable reference to a `Vec<u8>`. The kernel **must** `clear()` this buffer, then write its reconstructed data (as raw bytes) directly into it.
*   **`...params`:** Any specific parameters required for decoding (e.g., `num_values`).
*   **Returns:** `Result<(), PhoenixError>`.

## 3. The 5-Point Kernel Hardening Playbook

When implementing a new kernel, follow these steps to ensure it meets production standards:

1.  **Define Public API Signatures:**
    *   Start with the exact function signatures defined in Section 2.
    *   Add necessary trait bounds to `T` (e.g., `T: PrimInt + Signed`).

2.  **Implement Core Logic (The Direct-Write Pattern):**
    *   This is the most critical step for performance. **Do not create intermediate `Vec<T>` allocations.**
    *   In your public `encode`/`decode` functions:
        1.  `output_buf.clear();`
        2.  `output_buf.reserve(input_slice.len() * size_of::<OutputT>());` to pre-allocate memory.
        3.  Loop through the `input_slice` one element at a time.
        4.  Perform the transformation on the single element.
        5.  Get the bytes of the result using `.to_le_bytes()`.
        6.  Append these bytes directly to the buffer: `output_buf.extend_from_slice(...)`.

    *   **Example (from `zigzag::encode`):**
        ```rust
        pub fn encode<T>(input_slice: &[T], output_buf: &mut Vec<u8>) -> Result<(), PhoenixError>
        where T: PrimInt + Signed, T::Unsigned: PrimInt {
            output_buf.clear();
            output_buf.reserve(input_slice.len() * std::mem::size_of::<T::Unsigned>());

            for &value in input_slice {
                let encoded_val = encode_val(value)?; // Process one value
                output_buf.extend_from_slice(&encoded_val.to_le_bytes()); // Write bytes directly
            }
            Ok(())
        }
        ```

3.  **Make It Panic-Free:**
    *   **Eliminate all `.unwrap()` and `.expect()` calls.** Replace them with `ok_or_else()`, `map_err()`, and the `?` operator to propagate `PhoenixError`.
    *   Handle all possible error conditions (e.g., invalid input, truncation, overflow).

4.  **Ensure Architectural Purity:**
    *   The kernel's only responsibility is the transformation logic.
    *   It **must not** contain any `match type_str` blocks.
    *   It **must not** contain any `unsafe` blocks.
    *   It **must not** contain any `use pyo3::...` statements.

5.  **Write Comprehensive Unit Tests:**
    *   Update `#[cfg(test)]` blocks to call the new, hardened, generic public API.
    *   Ensure tests cover successful roundtrips, edge cases (empty input, single value), and specific error conditions.
    *   Use `crate::utils::typed_slice_to_bytes` and `crate::utils::safe_bytes_to_typed_slice` for test data preparation.

## 4. Integration Steps

Once a kernel is implemented and hardened, integrate it into the wider system:

1.  **Declare in `kernels/mod.rs`:**
    ```rust
    // src/kernels/mod.rs
    pub mod my_new_kernel;
    ```

2.  **Update the Dispatcher in `kernels/mod.rs`:**
    *   Add a `match` arm in `dispatch_encode` and `dispatch_decode` for your new kernel's `op` string.
    *   This arm will call the `dispatch_by_type!` macro, passing in your kernel's module name and any required parameters.
    *   **You do not need to modify `executor.rs`.**

    *   **Example (adding `my_new_kernel`):**
        ```rust
        // in src/kernels/mod.rs dispatch_encode
        match op {
            // ... other kernels
            "my_new_kernel" => {
                let some_param = params["some_param"].as_u64().unwrap_or(42) as u8;
                dispatch_by_type!(my_new_kernel, encode, input_bytes, output_buf, type_str, some_param)
            },
            "zstd" => { ... },
            _ => Err(...)
        }
        ```

3.  **Update `planner.rs` (Optional, but usually needed):**
    *   Modify `build_pipeline_from_profile` heuristics to decide when to include your new kernel in the pipeline.

4.  **Add Python Integration Tests:**
    *   Create or update tests in the top-level `tests/` directory (e.g., `test_compression.py`) that exercise a full compression/decompression roundtrip using your new kernel. This is the ultimate validation.

By following this guide, you can confidently add powerful new compression capabilities to Phoenix Cache.