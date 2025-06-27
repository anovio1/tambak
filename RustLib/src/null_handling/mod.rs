//! This module serves as the public API for all null-handling logic within the
//! Phoenix Rust core.
//!
//! It encapsulates the various strategies for managing nullability, primarily by
//! separating data from its validity information. The core principle is to
//! transform a potentially nullable data stream into two separate, more compressible
//! streams: one containing only the valid data and another containing the validity
//_ information itself (typically as a bitmap).
//!
//! This module is PURE RUST and is completely decoupled from the FFI layer.

//==================================================================================
// 1. Module Declarations
//==================================================================================

/// The primary kernel for stripping and re-applying Arrow-compatible validity bitmaps.
pub mod bitmap;

// In the future, other null-handling strategies could be added here as separate
// modules. For example:
//
// pub mod sentinel; // A strategy for handling nulls via a sentinel value.
//

//==================================================================================
// 2. Public API Re-exports
//==================================================================================
// This section defines the public, stable API of the `null_handling` module by
// re-exporting the necessary functions and structs from its sub-modules. Other parts
// of the Rust crate should only interact with the `null_handling` module through
// these re-exported items.

pub use self::bitmap::{
    strip_valid_data, // <-- CORRECTED: No longer exporting a non-existent struct
    reapply_bitmap,
};

//==================================================================================
// 3. Unit Tests (Module-level integration tests)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*; // Import the re-exported functions
    use arrow::array::Int32Array;

    #[test]
    fn test_public_api_is_accessible() {
        // This test confirms that the re-exporting is working correctly.
        let source_array = Int32Array::from(vec![Some(10), None, Some(20)]);
        let data_slice = source_array.values();
        let validity_bitmap = source_array.validity();

        // Call the function through the `mod.rs` public API
        let result = strip_valid_data(data_slice, validity_bitmap.unwrap());
        assert!(result.is_ok());

        // CORRECTED: The result is the Vec<i32> directly.
        let stripped_vec = result.unwrap();
        assert_eq!(stripped_vec, vec![10, 20]);
    }
}