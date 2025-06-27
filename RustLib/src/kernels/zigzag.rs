//! This module contains the pure, stateless kernels for performing Zig-zag
//! encoding and decoding.
//!
//! This technique is a Layer 3 (Bit-Width Reduction) transform. It is a lossless,
//! bitwise mapping of signed integers to unsigned integers that is highly effective
//! for streams of small, signed deltas. It folds the number line so that small
//! negative numbers map to small positive odd numbers, and small positive numbers
//! map to small positive even numbers. This makes the data more amenable to
-//! subsequent variable-length or bit-packing encoders.

use num_traits::{PrimInt, Signed, Unsigned};
use pyo3::PyResult;

use crate::error::PhoenixError;
use crate::utils::{bytes_to_typed_slice, typed_slice_to_bytes};

//==================================================================================
// 1. Generic Core Logic
//==================================================================================

/// Encodes a single signed integer using the Zig-zag algorithm.
///
/// The formula `(n << 1) ^ (n >> (bits - 1))` maps integers as follows:
///   0 -> 0
///  -1 -> 1
///   1 -> 2
///  -2 -> 3
///   2 -> 4
/// ...and so on.
///
/// # Type Parameters
/// * `T`: A signed primitive integer type.
///
/// # Returns
/// The corresponding unsigned integer type `T::Unsigned`.
pub fn encode_val<T>(n: T) -> T::Unsigned
where
    T: PrimInt + Signed,
    T::Unsigned: PrimInt,
{
    let bits = std::mem::size_of::<T>() * 8;
    // Shift left to make space for the sign bit, then XOR with the sign extension.
    (n.unsigned_shl(1)) ^ (n.arithmetic_shr(bits - 1)).to_unsigned().unwrap()
}

/// Decodes a single unsigned integer back to its signed representation.
///
/// This is the pure mathematical inverse of `encode_val`.
///
/// # Type Parameters
/// * `U`: An unsigned primitive integer type.
///
/// # Returns
/// The corresponding signed integer type `U::Signed`.
pub fn decode_val<U>(n: U) -> U::Signed
where
    U: PrimInt + Unsigned,
    U::Signed: PrimInt,
{
    let one = U::one();
    // Shift right to restore magnitude, then XOR with the sign bit (which is now the LSB).
    (n.unsigned_shr(1)).to_signed().unwrap() ^ -((n & one).to_signed().unwrap())
}

//==================================================================================
// 2. FFI Dispatcher Logic (No Macros)
//==================================================================================

/// The public-facing encode function for this module.
/// It safely converts the raw byte buffer to a typed slice of signed integers,
/// calls the generic encoder for each value, and returns the resulting bytes
/// of the new unsigned integer type.
pub fn encode(bytes: &[u8], original_type: &str) -> PyResult<Vec<u8>> {
    match original_type {
        "Int8" => {
            let data = unsafe { bytes_to_typed_slice::<i8>(bytes)? };
            let result: Vec<u8> = data.iter().map(|&v| encode_val(v)).collect();
            Ok(typed_slice_to_bytes(&result))
        }
        "Int16" => {
            let data = unsafe { bytes_to_typed_slice::<i16>(bytes)? };
            let result: Vec<u16> = data.iter().map(|&v| encode_val(v)).collect();
            Ok(typed_slice_to_bytes(&result))
        }
        "Int32" => {
            let data = unsafe { bytes_to_typed_slice::<i32>(bytes)? };
            let result: Vec<u32> = data.iter().map(|&v| encode_val(v)).collect();
            Ok(typed_slice_to_bytes(&result))
        }
        "Int64" => {
            let data = unsafe { bytes_to_typed_slice::<i64>(bytes)? };
            let result: Vec<u64> = data.iter().map(|&v| encode_val(v)).collect();
            Ok(typed_slice_to_bytes(&result))
        }
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string()).into()),
    }
}

/// The public-facing decode function for this module.
/// It safely converts the raw byte buffer to a typed slice of unsigned integers,
/// calls the generic decoder for each value, and returns the resulting bytes
/// of the reconstructed signed integer type.
pub fn decode(bytes: &[u8], original_type: &str) -> PyResult<Vec<u8>> {
    // Note: `original_type` here refers to the *target* signed type.
    match original_type {
        "Int8" => {
            let data = unsafe { bytes_to_typed_slice::<u8>(bytes)? };
            let result: Vec<i8> = data.iter().map(|&v| decode_val(v)).collect();
            Ok(typed_slice_to_bytes(&result))
        }
        "Int16" => {
            let data = unsafe { bytes_to_typed_slice::<u16>(bytes)? };
            let result: Vec<i16> = data.iter().map(|&v| decode_val(v)).collect();
            Ok(typed_slice_to_bytes(&result))
        }
        "Int32" => {
            let data = unsafe { bytes_to_typed_slice::<u32>(bytes)? };
            let result: Vec<i32> = data.iter().map(|&v| decode_val(v)).collect();
            Ok(typed_slice_to_bytes(&result))
        }
        "Int64" => {
            let data = unsafe { bytes_to_typed_slice::<u64>(bytes)? };
            let result: Vec<i64> = data.iter().map(|&v| decode_val(v)).collect();
            Ok(typed_slice_to_bytes(&result))
        }
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string()).into()),
    }
}

//==================================================================================
// 3. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zigzag_core_logic_i32() {
        assert_eq!(encode_val(0i32), 0u32);
        assert_eq!(encode_val(-1i32), 1u32);
        assert_eq!(encode_val(1i32), 2u32);
        assert_eq!(encode_val(-2i32), 3u32);
        assert_eq!(encode_val(2i32), 4u32);
        assert_eq!(encode_val(i32::MAX), u32::MAX - 1);
        assert_eq!(encode_val(i32::MIN), u32::MAX);

        assert_eq!(decode_val(0u32), 0i32);
        assert_eq!(decode_val(1u32), -1i32);
        assert_eq!(decode_val(2u32), 1i32);
        assert_eq!(decode_val(3u32), -2i32);
        assert_eq!(decode_val(4u32), 2i32);
        assert_eq!(decode_val(u32::MAX - 1), i32::MAX);
        assert_eq!(decode_val(u32::MAX), i32::MIN);
    }

    #[test]
    fn test_zigzag_roundtrip_vec_i16() {
        let original: Vec<i16> = vec![-5, 4, -3, 2, -1, 0, 100, -100];
        let original_bytes = typed_slice_to_bytes(&original);

        let encoded_bytes = encode(&original_bytes, "Int16").unwrap();
        
        // Manually verify one value
        let encoded_slice = unsafe { bytes_to_typed_slice::<u16>(&encoded_bytes).unwrap() };
        assert_eq!(encoded_slice[0], 9u16); // -5 -> 9
        assert_eq!(encoded_slice[1], 8u16); //  4 -> 8

        let decoded_bytes = decode(&encoded_bytes, "Int16").unwrap();
        assert_eq!(original_bytes, decoded_bytes);
    }

    #[test]
    fn test_zigzag_encode_unsupported_type() {
        let original: Vec<u32> = vec![1, 2, 3];
        let original_bytes = typed_slice_to_bytes(&original);
        let result = encode(&original_bytes, "UInt32");
        assert!(result.is_err());
    }

    #[test]
    fn test_zigzag_decode_unsupported_type() {
        let original: Vec<i32> = vec![1, 2, 3];
        let original_bytes = typed_slice_to_bytes(&original);
        let result = decode(&original_bytes, "Int32"); // Should be u32 bytes
        assert!(result.is_err());
    }
}