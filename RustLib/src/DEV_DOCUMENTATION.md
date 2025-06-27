Understood. The request is for a complete, self-contained developer guide for an LLM to take over and finish the Rust library. This document will serve as its "prime directive"—a comprehensive blueprint covering architecture, contracts, implementation patterns, and testing philosophy. It is written to be unambiguous and actionable for a code-generation model.

---
### **Phoenix Protocol: Rust Core Library - Developer Implementation Guide (v4.5)**

**Project Codename:** `libphoenix`
**Status:** FINAL BLUEPRINT
**Owner:** Lead Architect

#### **1.0 Mission Objective**

Your primary mission is to implement the `libphoenix` Rust crate. This crate is a high-performance, stateless, and verifiable lossless compression library. It functions as a "toolbox" of computational kernels and a high-level pipeline orchestrator.

This library will be consumed by a Python package (`phoenix-cache`) which handles all I/O and application state. Your crate's public API will be exposed to Python via an FFI layer.

#### **2.0 Core Architectural Principles**

1.  **Stateless Kernels, Stateful Pipeline:** Individual compression functions (e.g., `delta`, `zigzag`) MUST be pure, stateless, and thread-safe. The `pipeline` module will orchestrate these stateless kernels to execute a stateful, multi-stage compression/decompression sequence.
2.  **Rust Owns the Recipe:** The core logic for the order and application of compression transforms resides entirely within this Rust crate, inside the `pipeline` module. Python will only tell the pipeline *what* to do (e.g., "compress this block with this recipe"), not *how* to do it step-by-step.
3.  **Safety and Explicitness:** Isolate all `unsafe` code required for FFI or byte transmutation into the `utils` and `ffi` modules. The core `kernels` and `pipeline` logic MUST be implemented in safe Rust.
4.  **Performance via Zero-Copy:** The FFI boundary should be designed for zero-copy data transfer where possible, leveraging the Arrow memory model.

#### **3.0 Required Project Structure (Filesystem Contract)**

You will implement the following file structure. Each file has a specific, non-overlapping responsibility.

```plaintext
phoenix-cache/
src/
├── kernels/                          // Pure, atomic compression kernels (stateless)
│   ├── delta.rs                     // Delta encoding/decoding kernels
│   ├── rle.rs                       // Run-Length Encoding kernels
│   ├── leb128.rs                    // LEB128 unsigned integer kernels
│   ├── sleb128.rs                   // SLEB128 signed integer kernels
│   ├── zigzag.rs                    // Zigzag encoding kernels for signed integers
│   ├── zstd.rs                      // Final compression stage using zstd
│   └── mod.rs                      // Re-exports all kernel modules
│
├── pipeline/                        // The "brain" orchestrating compression & decompression
│   ├── compress.rs                 // Decides which kernels to run, in which order, handles nulls, builds manifest
│   ├── decompress.rs               // Reads manifest, reverses kernel steps, reconstructs original data
│   ├── manifest.rs                // Logic for manifest/footer format (metadata recording kernel pipeline & settings)
│   └── mod.rs                    // Re-exports pipeline modules
│
├── utils/                         // Utility functions used across kernels & pipeline
│   ├── nulls.rs                  // Null bitmap encoding/decoding
│   ├── bytes.rs                  // Byte slice to typed slice conversions and vice versa
│   └── mod.rs                   // Re-exports utils
│
├── error.rs                      // Custom error types, e.g. PhoenixError
│
└── ffi/                         // Foreign Function Interface (Python, or other) wrappers
    ├── python.rs                 // Python FFI bindings: call pipeline compress/decompress functions
    └── mod.rs                   // Re-exports ffi modules
```

---

# PhoenixCache
This Rust library compresses and decompresses typed columnar data by applying a configurable pipeline of stateless kernels, taking raw byte slices as input and producing compressed byte vectors as output (and vice versa).


// COMPLETE scaffold for src/pipeline/planner.rs REMEMBER. (Core logicanalyzes the input data and metadata to decide the optimal sequence of compression kernels to apply, producing a pipeline plan for execution) EACH OF THESE SCAFFOLDS ARE MEANT TO BE HANDLED BY A SEPARATE TEAM. IT IS IMPORTANT THAT THEY ARE COMPLETE SCAFFOLDS


```

src/
├── lib.rs                   # Main library entry point, exposes public APIs
├── error.rs                 # Error types and handling (PhoenixError, etc.)
├── utils.rs                 # Utility functions (e.g., bytes_to_typed_slice, conversions)
├── pipeline/
│   ├── mod.rs          # The Pipeline Facade: Declares the module and its public API.
│   ├── orchestrator.rs # The General Contractor: Manages the end-to-end compression/decompression workflow.
│   ├── planner.rs      # The Strategist: Analyzes data and creates a compression plan.
│   └── executor.rs     # The Foreman: Executes a given plan by calling the kernels.
├── ffi.rs                   # will be extremely simple; it will only call the Orchestrator
├── kernels/                 # Compression kernel implementations (delta, rle, zstd, etc.)
│   ├── mod.rs               # Kernel module root, pub mod each kernel
│   ├── delta.rs             # Delta encoding/decoding kernel
│   ├── rle.rs               # Run-length encoding kernel
│   ├── zigzag.rs            # Zigzag encoding kernel
│   ├── bitpack.rs           # Bitpacking kernel
│   ├── leb128.rs            # LEB128 kernel
│   ├── shuffle.rs           # Byte shuffling kernel
│   ├── zstd.rs              # Zstd kernel wrapper
│   └── validity_mask.rs     # Null bitmap compression (e.g., RLE + Zstd for validity)
└── tests/                   # Integration and unit tests
    ├── planner_tests.rs
    ├── kernels_tests.rs
    └── integration_tests.rs
# How It Works Together

* **kernels/**
  Each kernel is *pure, stateless*, and type-generic where possible. This keeps compression logic modular.

* **null\_handling/**
  Implements efficient null bitmap encoding/decoding, separate from kernels but compatible with them.

* **pipeline/**
  This is the brain — it takes raw input data, manages null handling, and decides which kernels to apply and in what order (e.g., delta → RLE → zigzag → leb128 → zstd). It runs compression and decompression by chaining kernels accordingly.

* **ffi/**
  A *thin, minimal glue layer* that exposes the pipeline orchestrator to Python
  Future Item: A *thin, minimal glue layer* that exposes the kernel to Python

* **errors.rs**
  Central place for your error enums and conversions.

* **utils.rs**
  Shared helpers used by kernels, pipeline, and bindings.

* **lib.rs**
  The public API entrypoint, re-exporting pipeline orchestrator and possibly kernels for advanced use.


# Example API Surface in lib.rs

```rust
pub mod kernels;
pub mod null_handling;
pub mod pipeline;
pub mod errors;
pub mod utils;

pub use pipeline::{compress, decompress}; // Simplest entrypoint for users

// Optional lower-level exports if needed:
pub use kernels::{delta, rle, zigzag, leb128, sleb128, zstd};
pub use null_handling::NullBitmap;
```

---

# Next Steps I Recommend

* **Design pipeline orchestrator API** in Rust:
  Functions like `compress(data: &[u8], schema: &Schema) -> Result<Vec<u8>, PhoenixError>` that apply all kernels + null handling.

* **Implement null bitmap encoding/decoding** in Rust as a standalone module.

* **Add ergonomic Rust data structures and traits** for typed input/output (maybe Arrow integration later).

* **Write comprehensive integration tests** covering the full compression/decompression flow.

