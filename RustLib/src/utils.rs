//! This module provides a set of shared, low-level utility functions used
//! throughout the Phoenix Rust core.
//!
//! Its primary responsibilities include:
//! 1.  Providing safe, validated conversions between raw byte slices and typed slices.
//! 2.  Encapsulating `unsafe` code into well-defined, narrowly-scoped functions
//!     that can be heavily audited.
//! 3.  Offering helper functions for common tasks like determining type sizes.

use num_traits::PrimInt;
use pyo3::PyResult;
use bytemuck; // Import the crate

use crate::error::PhoenixError;

//==================================================================================
// 1. Core Utility Functions
//==================================================================================

/// Safely reinterprets a byte slice as a slice of a primitive integer type.
///
/// This function is the primary gateway for converting raw bytes from the FFI layer
/// or file I/O into a workable, typed slice. It performs critical safety checks
/// before creating a zero-copy view of the data.
///
/// # Safety
/// This function uses `unsafe` to perform a pointer cast. However, it is wrapped
/// in safe checks to ensure memory safety. The caller can treat the output as safe
/// provided the input `bytes` points to valid memory for its duration.
///
/// # Args
/// * `bytes`: The input byte slice.
///
/// # Returns
/// A `Result` containing a reference to a typed slice `&[T]` on success.
///
/// # Errors
/// Returns a `PhoenixError::BufferMismatch` if the byte slice length is not
/// perfectly divisible by the size of the target type `T`.
//  CURRENT V3
pub fn safe_bytes_to_typed_slice<'a, T>(bytes: &'a [u8]) -> Result<&'a [T], PhoenixError>
where
    T: bytemuck::Pod, // Use bytemuck's trait for "Plain Old Data"
{
    bytemuck::try_cast_slice(bytes)
        .map_err(|e| {
            // Bytemuck's error type contains detailed info about the failure
            // (e.g., alignment, length, etc.), which is great for debugging.
            PhoenixError::InternalError(format!("Failed to cast byte slice: {}", e))
        })
}

//  BEFORE V2
// pub fn safe_bytes_to_typed_slice<'a, T: PrimInt>(bytes: &'a [u8]) -> Result<&'a [T], PhoenixError> {
//     let type_size = std::mem::size_of::<T>();
//     if type_size == 0 { // Should not happen for PrimInt, but good practice
//         return Ok(&[]);
//     }
//     if bytes.len() % type_size != 0 {
//         return Err(PhoenixError::BufferMismatch(bytes.len(), type_size));
//     }

//     let ptr = bytes.as_ptr() as *const T;
//     let len = bytes.len() / type_size;
//     // SAFETY: We have checked that the length is a valid multiple of the type size,
//     // and `PrimInt` types are guaranteed to be plain old data with no invalid bit
//     // patterns. The alignment is assumed to be handled by the allocator (e.g., Arrow).
//     unsafe { Ok(std::slice::from_raw_parts(ptr, len)) }
// }

// BEFORE V1
// pub unsafe fn bytes_to_typed_slice<'a, T: PrimInt>(bytes: &'a [u8]) -> PyResult<&'a [T]> {
//     let size = std::mem::size_of::<T>();
//     if bytes.len() % size != 0 {
//         return Err(PhoenixError::BufferMismatch(bytes.len(), size).into());
//     }
//     let ptr = bytes.as_ptr() as *const T;
//     let len = bytes.len() / size;
//     Ok(std::slice::from_raw_parts(ptr, len))
// }


/// Converts a slice of primitive integers into a `Vec<u8>`, respecting Little-Endian byte order.
///
/// This function performs a memory copy to create a new, owned byte vector.
///
/// # Args
/// * `data`: A slice of primitive integers.
///
/// # Returns
/// A `Vec<u8>` containing the serialized data.
pub fn typed_slice_to_bytes<T: PrimInt>(data: &[T]) -> Vec<u8> {
    data.iter().flat_map(|&val| val.to_le_bytes()).collect()
}


//BEFORE
/// Converts a slice of primitive integers into a `Vec<u8>`.
/// This involves a copy. Assumes Little-Endian.
pub fn typed_slice_to_bytes<T: PrimInt>(data: &[T]) -> Vec<u8> {
    data.iter().flat_map(|&val| val.to_le_bytes()).collect()
}

/// Returns the size in bytes of a given data type specified by its string name.
///
/// This utility is used by various parts of the pipeline to know how to handle
/// raw byte buffers without needing a generic type parameter at that moment.
///
/// # Args
/// * `type_str`: The string representation of the type (e.g., "Int64", "UInt8").
///
/// # Returns
/// A `Result` containing the size in bytes (`usize`) on success.
///
/// # Errors
/// Returns a `PhoenixError::UnsupportedType` if the string is not recognized.
pub fn get_element_size(type_str: &str) -> Result<usize, PhoenixError> {
    match type_str {
        "Int8" | "UInt8" | "Boolean" => Ok(1),
        "Int16" | "UInt16" => Ok(2),
        "Int32" | "UInt32" => Ok(4),
        "Int64" | "UInt64" => Ok(8),
        _ => Err(PhoenixError::UnsupportedType(type_str.to_string())),
    }
}

//==================================================================================
// 2. FFI Dispatcher Logic (Not Applicable)
//==================================================================================
// This is a pure utility module. It does not and should not contain any FFI
// dispatchers or `#[pyfunction]` wrappers. It is a dependency for other modules.

//==================================================================================
// 3. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safe_bytes_to_typed_slice_success() {
        let original_vec: Vec<i32> = vec![1, -2, 1_000_000];
        let bytes = typed_slice_to_bytes(&original_vec);

        let typed_slice = safe_bytes_to_typed_slice::<i32>(&bytes).unwrap();
        assert_eq!(typed_slice, original_vec.as_slice());
    }

    #[test]
    fn test_safe_bytes_to_typed_slice_mismatch_error() {
        let bytes: Vec<u8> = vec![0, 1, 2, 3, 4]; // 5 bytes, not divisible by 2 or 4

        let result_i32 = safe_bytes_to_typed_slice::<i32>(&bytes);
        assert!(result_i32.is_err());
        if let Err(PhoenixError::BufferMismatch(len, size)) = result_i32.unwrap_err() {
            assert_eq!(len, 5);
            assert_eq!(size, 4);
        } else {
            panic!("Expected BufferMismatch error");
        }

        let result_i16 = safe_bytes_to_typed_slice::<i16>(&bytes);
        assert!(result_i16.is_err());
    }

    #[test]
    fn test_typed_slice_to_bytes_endianness() {
        // Value is 258 = 0x0102
        let original_vec: Vec<u16> = vec![258];
        let bytes = typed_slice_to_bytes(&original_vec);

        // Little-Endian means the least significant byte (0x02) comes first.
        assert_eq!(bytes, vec![0x02, 0x01]);
    }

    #[test]
    fn test_get_element_size_all_types() {
        assert_eq!(get_element_size("Int8").unwrap(), 1);
        assert_eq!(get_element_size("UInt16").unwrap(), 2);
        assert_eq!(get_element_size("Int32").unwrap(), 4);
        assert_eq!(get_element_size("UInt64").unwrap(), 8);
        assert_eq!(get_element_size("Boolean").unwrap(), 1);
    }

    #[test]
    fn test_get_element_size_unsupported() {
        let result = get_element_size("Float32");
        assert!(result.is_err());
        if let Err(PhoenixError::UnsupportedType(s)) = result.unwrap_err() {
            assert_eq!(s, "Float32");
        } else {
            panic!("Expected UnsupportedType error");
        }
    }
}