//! This module contains the pure, stateless, and performant kernels for performing
//! Zig-zag encoding and decoding.
//!
//! This technique is a Layer 3 (Bit-Width Reduction) transform. It is a lossless,
//! bitwise mapping of signed integers to unsigned integers. This module is
//! PURE RUST, panic-free, and has no FFI or Polars dependencies.

use num_traits::{PrimInt, Signed, Unsigned, ToPrimitive};

use crate::error::PhoenixError;
use crate::utils::typed_slice_to_bytes;

//==================================================================================
// 1. Generic Core Logic (The "Engine" - REVISED & Panic-Free)
//==================================================================================

/// Encodes a single signed integer using the Zig-zag algorithm.
/// This function is now fallible and returns a Result.
fn encode_val<T>(n: T) -> Result<T::Unsigned, PhoenixError>
where
    T: PrimInt + Signed,
    T::Unsigned: PrimInt,
{
    let bits = std::mem::size_of::<T>() * 8;
    let unsigned_n = n.to_unsigned()
        .ok_or_else(|| PhoenixError::UnsupportedType("Failed to convert to unsigned for shift".to_string()))?;
    let signed_shifted = n.arithmetic_shr(bits - 1);
    let unsigned_shifted = signed_shifted.to_unsigned()
        .ok_or_else(|| PhoenixError::UnsupportedType("Failed to convert sign extension to unsigned".to_string()))?;

    Ok((unsigned_n << 1) ^ unsigned_shifted)
}

/// Decodes a single unsigned integer back to its signed representation.
/// This function is now fallible and returns a Result.
fn decode_val<U>(n: U) -> Result<U::Signed, PhoenixError>
where
    U: PrimInt + Unsigned,
    U::Signed: PrimInt,
{
    let one = U::one();
    let shifted_n = n.unsigned_shr(1);
    let lsb = n & one;

    let signed_shifted = shifted_n.to_signed()
        .ok_or_else(|| PhoenixError::UnsupportedType("Failed to convert shifted value to signed".to_string()))?;
    let signed_lsb = lsb.to_signed()
        .ok_or_else(|| PhoenixError::UnsupportedType("Failed to convert LSB to signed".to_string()))?;

    Ok(signed_shifted ^ -(signed_lsb))
}

//==================================================================================
// 2. Public API (Generic, Performant, Decoupled - REVISED & Memory-Efficient)
//==================================================================================

/// The public-facing, generic encode function for this module.
/// It iterates through the input slice, encoding one value at a time and writing
/// the resulting bytes directly to the output buffer to avoid intermediate allocations.
pub fn encode<T>(
    input_slice: &[T],
    output_buf: &mut Vec<u8>,
) -> Result<(), PhoenixError>
where
    T: PrimInt + Signed,
    T::Unsigned: PrimInt,
{
    output_buf.clear();
    output_buf.reserve(input_slice.len() * std::mem::size_of::<T::Unsigned>());

    for &value in input_slice {
        let encoded_val = encode_val(value)?;
        output_buf.extend_from_slice(&encoded_val.to_le_bytes());
    }
    Ok(())
}

/// The public-facing, generic decode function for this module.
/// It iterates through the input slice of unsigned integers, decoding one value
/// at a time and writing the resulting bytes directly to the output buffer.
pub fn decode<U>(
    input_slice: &[U],
    output_buf: &mut Vec<u8>,
) -> Result<(), PhoenixError>
where
    U: PrimInt + Unsigned,
    U::Signed: PrimInt,
{
    output_buf.clear();
    output_buf.reserve(input_slice.len() * std::mem::size_of::<U::Signed>());

    for &value in input_slice {
        let decoded_val = decode_val(value)?;
        output_buf.extend_from_slice(&decoded_val.to_le_bytes());
    }
    Ok(())
}

//==================================================================================
// 3. Unit Tests (Revised to test the new public API pattern)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::safe_bytes_to_typed_slice;

    #[test]
    fn test_zigzag_core_logic_i32() {
        assert_eq!(encode_val(0i32).unwrap(), 0u32);
        assert_eq!(encode_val(-1i32).unwrap(), 1u32);
        assert_eq!(encode_val(1i32).unwrap(), 2u32);

        assert_eq!(decode_val(0u32).unwrap(), 0i32);
        assert_eq!(decode_val(1u32).unwrap(), -1i32);
        assert_eq!(decode_val(2u32).unwrap(), 1i32);
    }

    #[test]
    fn test_zigzag_roundtrip_i16() {
        let original: Vec<i16> = vec![-5, 4, -3, 2, -1, 0, 100, -100];
        
        // --- Test Encode ---
        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();
        
        let encoded_slice = unsafe { safe_bytes_to_typed_slice::<u16>(&encoded_bytes).unwrap() };
        assert_eq!(encoded_slice[0], 9u16); // -5 -> 9
        assert_eq!(encoded_slice[1], 8u16); //  4 -> 8

        // --- Test Decode ---
        let mut decoded_bytes = Vec::new();
        decode(encoded_slice, &mut decoded_bytes).unwrap();

        let original_as_bytes = crate::utils::typed_slice_to_bytes(&original);
        assert_eq!(decoded_bytes, original_as_bytes);
    }

    #[test]
    fn test_max_min_values_i64() {
        let original: Vec<i64> = vec![i64::MAX, i64::MIN, -1, 0, 1];
        
        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();
        
        let encoded_slice = unsafe { safe_bytes_to_typed_slice::<u64>(&encoded_bytes).unwrap() };
        
        let mut decoded_bytes = Vec::new();
        decode(encoded_slice, &mut decoded_bytes).unwrap();

        let original_as_bytes = crate::utils::typed_slice_to_bytes(&original);
        assert_eq!(decoded_bytes, original_as_bytes);
    }
}