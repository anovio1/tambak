//! This module contains the pure, stateless, and performant kernels for performing
//! Zig-zag encoding and decoding.
//!
//! This technique is a Layer 3 (Bit-Width Reduction) transform. It is a lossless,
//! bitwise mapping of signed integers to unsigned integers. This module is
//! PURE RUST, panic-free, and has no FFI or Polars dependencies.

use num_traits::{PrimInt, Signed, Unsigned};
use std::ops::{Shl, Shr, BitXor};
use bytemuck;

use crate::traits::{HasUnsigned, HasSigned};
use crate::error::PhoenixError;

//==================================================================================
// 1. Generic Core Logic (The "Engine")
//==================================================================================

/// Encodes a single signed integer using the Zig-zag algorithm.
pub fn encode_val<T>(n: T) -> T::Unsigned
where
    T: PrimInt + Signed + HasUnsigned + Shl<usize, Output = T> + Shr<usize, Output = T> + BitXor<T, Output = T>,
    T::Unsigned: PrimInt,
{
    let bits = std::mem::size_of::<T>() * 8;
    // The formula (n << 1) ^ (n >> (BITS - 1)) must be done carefully.
    // The right shift must be arithmetic.
    let shifted = (n << 1) ^ (n >> (bits - 1));
    // We can now safely cast the bit pattern to the unsigned type.
    unsafe { std::mem::transmute_copy::<T, T::Unsigned>(&shifted) }
}

/// Decodes a single unsigned integer back to its signed representation.
pub fn decode_val<U>(n: U) -> U::Signed
where
    U: PrimInt + Unsigned + HasSigned + Shr<usize, Output = U> + BitXor<U, Output = U>,
    U::Signed: PrimInt + std::ops::Neg<Output = U::Signed>,
{
    let one = U::one();
    let shifted_n = n >> 1;
    let lsb = n & one;
    // The formula is (n >> 1) ^ -(n & 1)
    let signed_shifted = unsafe { std::mem::transmute_copy::<U, U::Signed>(&shifted_n) };
    let signed_lsb = unsafe { std::mem::transmute_copy::<U, U::Signed>(&lsb) };

    signed_shifted ^ (-signed_lsb)
}

//==================================================================================
// 2. Public API (Generic, Performant, Decoupled)
//==================================================================================

/// The public-facing, generic encode function for this module.
pub fn encode<T>(
    input_slice: &[T],
    output_buf: &mut Vec<u8>,
) -> Result<(), PhoenixError>
where
    T: PrimInt + Signed + HasUnsigned + Shl<usize, Output = T> + Shr<usize, Output = T> + BitXor<T, Output = T>,
    T::Unsigned: PrimInt + bytemuck::Pod,
{
    output_buf.clear();
    output_buf.reserve(input_slice.len() * std::mem::size_of::<T::Unsigned>());

    for &value in input_slice {
        let encoded_val = encode_val(value);
        output_buf.extend_from_slice(bytemuck::bytes_of(&encoded_val));
    }
    Ok(())
}

/// The public-facing, generic decode function for this module.
pub fn decode<U>(
    input_slice: &[U],
    output_buf: &mut Vec<u8>,
) -> Result<(), PhoenixError>
where
    U: PrimInt + Unsigned + HasSigned + Shr<usize, Output = U> + BitXor<U, Output = U>,
    U::Signed: PrimInt + bytemuck::Pod + std::ops::Neg<Output = U::Signed>,
{
    output_buf.clear();
    output_buf.reserve(input_slice.len() * std::mem::size_of::<U::Signed>());

    for &value in input_slice {
        let decoded_val = decode_val(value);
        output_buf.extend_from_slice(bytemuck::bytes_of(&decoded_val));
    }
    
    #[cfg(debug_assertions)]
    println!("[ZIGZAG DECODE INPUT] First 5 values from shuffle: {:?}", &output_buf.get(0..5));
    Ok(())
}

//==================================================================================
// 3. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::{safe_bytes_to_typed_slice, typed_slice_to_bytes};

    #[test]
    fn test_zigzag_core_logic_i32() {
        assert_eq!(encode_val(0i32), 0u32);
        assert_eq!(encode_val(-1i32), 1u32);
        assert_eq!(encode_val(1i32), 2u32);

        assert_eq!(decode_val(0u32), 0i32);
        assert_eq!(decode_val(1u32), -1i32);
        assert_eq!(decode_val(2u32), 1i32);
    }

    #[test]
    fn test_zigzag_roundtrip_i16() {
        let original: Vec<i16> = vec![-5, 4, -3, 2, -1, 0, 100, -100];

        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();

        let encoded_slice = safe_bytes_to_typed_slice::<u16>(&encoded_bytes).unwrap();
        assert_eq!(encoded_slice[0], 9u16); // -5 -> 9
        assert_eq!(encoded_slice[1], 8u16); //  4 -> 8

        let mut decoded_bytes = Vec::new();
        decode(encoded_slice, &mut decoded_bytes).unwrap();

        let original_as_bytes = typed_slice_to_bytes(&original);
        assert_eq!(decoded_bytes, original_as_bytes);
    }

    #[test]
    fn test_max_min_values_i64() {
        let original: Vec<i64> = vec![i64::MAX, i64::MIN, -1, 0, 1];

        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();

        let encoded_slice = safe_bytes_to_typed_slice::<u64>(&encoded_bytes).unwrap();

        let mut decoded_bytes = Vec::new();
        decode(encoded_slice, &mut decoded_bytes).unwrap();

        let original_as_bytes = typed_slice_to_bytes(&original);
        assert_eq!(decoded_bytes, original_as_bytes);
    }
}