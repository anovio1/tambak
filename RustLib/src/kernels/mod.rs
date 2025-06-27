//! This module serves as the public API for the collection of all pure, stateless
//! compression and decompression kernels.
//!
//! Each sub-module represents a distinct algorithmic transform that can be composed
//! by the `pipeline` layer to create a complete compression strategy. This is the
//! "toolbox" of the Phoenix system.

//==================================================================================
// 1. Module Declarations
//==================================================================================

/// Layer 1: Value Reduction
pub mod delta;

/// Layer 2: Sparsity Exploitation
pub mod rle;

/// Layer 3: Bit-Width Reduction
pub mod zigzag;
pub mod leb128;
pub mod sleb128; // Signed LEB128
pub mod bitpack;

/// Layer 4: Byte Distribution
pub mod shuffle;

/// Final Stage: Entropy Coding
pub mod zstd;

//==================================================================================
// 2. Public API Re-exports
//==================================================================================
// We do not re-export individual functions here. The `pipeline::executor` is the
// designated consumer of these kernels and will call them via their full path
// (e.g., `compression::delta::encode`). This keeps the dependency graph explicit
// and prevents polluting the global namespace. This file's primary role is to
// declare the module structure for the crate.
//
// --- Re-export their public functions to create a unified API ---
// This allows other modules to call `kernels::encode_delta(...)` etc.
// (Assuming we name the functions inside each module uniquely)

// Example of what this might look like once kernels are hardened:
// pub use self::delta::{encode_delta, decode_delta};
// pub use self::rle::{encode_rle, decode_rle};
// ... and so on for all kernels.