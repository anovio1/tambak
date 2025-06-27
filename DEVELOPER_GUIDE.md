# Phoenix Cache: Developer Guide

This guide provides instructions and best practices for developing and contributing to the Phoenix Cache Rust core. It covers environment setup, building, testing, and adhering to coding standards.

## 1. Development Environment Setup

To work on the Phoenix Cache Rust core, you'll need the following tools:

### A. Rust Toolchain

Phoenix Cache requires a recent stable version of the Rust toolchain.

1.  **Install `rustup`:**
    If you don't have Rust installed, use `rustup` (the official Rust toolchain installer):
    ```bash
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    ```
    Follow the on-screen instructions. Choose the default installation.
2.  **Update Rust:**
    It's good practice to keep your Rust toolchain up-to-date:
    ```bash
    rustup update
    ```
3.  **Add `rust-analyzer` (Optional but Recommended):**
    For excellent IDE support (e.g., in VS Code with the Rust Analyzer extension), install the language server:
    ```bash
    rustup component add rust-analyzer
    ```

### B. Python Build Tools

`maturin` is the build tool that compiles our Rust code into a Python extension module (a Python wheel).

```bash
pip install maturin
```

### C. IDE/Editor Setup

*   **VS Code:** Install the official "Rust Analyzer" extension. It provides excellent autocompletion, type hints, error checking, and navigation for Rust projects.

## 2. Building the Project

Phoenix Cache is primarily built using `maturin`.

### A. Local Development Build

For everyday development, testing, and quick iterations, use `maturin develop`:

```bash
# From the root of the repository (where pyproject.toml is)
maturin develop
```

This command performs the following:
*   Compiles the Rust code in debug mode (faster compilation, but slower runtime).
*   Installs the `phoenix_cache` Python package into your currently active Python environment in "editable" mode. This means changes to your Rust code are reflected when you re-run Python, often without needing to reinstall.

### B. Release Build (for Production)

For building optimized binaries for distribution or performance benchmarking, use the `--release` flag:

```bash
maturin develop --release
```

Or to build a wheel file for distribution:

```bash
maturin build --release
```

This compiles the Rust code with full optimizations (e.g., `opt-level=3`, LTO enabled in `Cargo.toml`), resulting in a significantly faster binary, but with longer compilation times.

## 3. Testing

Phoenix Cache employs a two-tiered testing strategy: Rust unit tests and Python integration tests.

### A. Rust Unit Tests

Rust unit tests live alongside the code they test (`#[cfg(test)]` modules). They are fast, isolated, and verify the correctness of individual Rust functions and modules in a pure Rust environment.

To run all Rust tests:

```bash
# From the root of the repository
cargo test
```

### B. Python Integration Tests

Python integration tests (typically using `pytest`) are crucial for verifying the entire stack, including the FFI boundary, data conversions, and end-to-end compression/decompression workflows as a Python user would experience them. These tests live in the top-level `tests/` directory.

To run all Python integration tests:

```bash
# From the root of the repository (after running `maturin develop` at least once)
pytest
```

## 4. Code Quality & Standards

We strive for high code quality, performance, and maintainability.

### A. Code Formatting

We use `rustfmt` to automatically format Rust code. Always run this before committing:

```bash
cargo fmt
```

### B. Linting

We use `clippy` for static analysis and linting, which helps catch common mistakes and improve idiomatic Rust. Run this regularly:

```bash
cargo clippy --all-targets -- -D warnings
```

*   The `-- -D warnings` flag treats all clippy warnings as errors, forcing issues to be addressed.

### C. Panics and Error Handling

*   **No Panics:** The Rust core **must be panic-free**. Panics unwinding across the FFI boundary are Undefined Behavior and can crash the entire Python process. All recoverable errors should be handled via the `Result<T, PhoenixError>` type.
*   **`PhoenixError`:** Use the centralized `PhoenixError` enum (`src/error.rs`) for all errors. This ensures consistent error propagation.
*   **`PyResult`:** `PyResult<T>` (which is `Result<T, PyErr>`) should *only* be used in `src/ffi/python.rs`. Core Rust logic (`pipeline`, `kernels`, `null_handling`, `utils`) must use `Result<T, PhoenixError>`.

### D. Performance

*   **Minimize Allocations:** Prioritize passing `&[u8]` or `&[T]` slices and writing to mutable output buffers (`&mut Vec<u8>`) instead of returning new `Vec`s. Use `Vec::with_capacity()` when pre-allocating is possible.
*   **Zero-Copy Where Possible:** Leverage Arrow/Polars' zero-copy capabilities at the FFI boundary.
*   **Avoid Row-by-Row Operations:** In data-intensive loops, prefer iterator-based processing, `extend_from_slice`, or batch operations over individual `push()` or builder `append()` calls.

### E. Code Structure and Documentation

*   **Modular Design:** Adhere to the established [Architecture](ARCHITECTURE.md) and file structure. Each module and function should have a clear, single responsibility.
*   **Doc Comments:** Write clear and concise doc comments for all public functions, structs, and modules using `///`. Explain inputs, outputs, error conditions, and any special considerations.
*   **Comments:** Use inline comments (`//`) to explain complex logic or critical decisions.

## 5. Contributing Workflow

1.  **Fork** the repository and **clone** your fork.
2.  **Create a new branch** for your feature or bug fix: `git checkout -b feature/my-new-feature`.
3.  **Implement your changes**, following the coding standards.
4.  **Write tests** for your changes (Rust unit tests and/or Python integration tests).
5.  Ensure all existing tests pass: `cargo test` and `pytest`.
6.  Run linters: `cargo fmt` and `cargo clippy --all-targets -- -D warnings`.
7.  **Commit your changes** with a clear and concise commit message.
8.  **Push your branch** to your fork.
9.  **Open a Pull Request** against the `main` branch of the upstream repository.

This guide should provide a solid foundation for contributing to Phoenix Cache.