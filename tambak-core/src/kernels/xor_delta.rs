//! This module contains the pure, stateless, and performant kernels for performing
//! XOR delta encoding and decoding.
//!
//! This is a Layer 1 (Value Reduction) transform. It is particularly effective
//! for bit-level data, such as float values cast to integers, where it can
//! isolate bit-flips in slowly changing data.

use bytemuck;
use std::ops::BitXor;

use crate::error::tambakError;
use crate::utils::typed_slice_to_bytes;

/// Performs XOR delta encoding **in-place** on a mutable slice.
fn xor_delta_slice_inplace<T>(data: &mut [T])
where
    T: Copy + bytemuck::Pod + BitXor<Output = T>,
{
    if data.len() <= 1 {
        return;
    }
    // Iterate backwards for encoding to use original values for calculation.
    for i in (1..data.len()).rev() {
        data[i] = data[i] ^ data[i - 1];
    }
}

/// Reconstructs the original data from an XOR delta stream **in-place**.
fn xor_undelta_slice_inplace<T>(data: &mut [T])
where
    T: Copy + bytemuck::Pod + BitXor<Output = T>,
{
    if data.len() <= 1 {
        return;
    }
    // Iterate forwards to use the newly-decoded values for subsequent XORs.
    for i in 1..data.len() {
        data[i] = data[i] ^ data[i - 1];
    }
}

/// The public-facing, generic encode function for this module.
pub fn encode<T>(input_slice: &[T], output_buf: &mut Vec<u8>) -> Result<(), tambakError>
where
    T: Copy + bytemuck::Pod + BitXor<Output = T>,
{
    let mut data_vec = input_slice.to_vec();
    xor_delta_slice_inplace(&mut data_vec);
    output_buf.clear();
    output_buf.extend_from_slice(&typed_slice_to_bytes(&data_vec));
    Ok(())
}

/// The public-facing, generic decode function for this module.
pub fn decode<T>(input_bytes: &[u8], output_buf: &mut Vec<u8>) -> Result<(), tambakError>
where
    T: Copy + bytemuck::Pod + BitXor<Output = T>,
{
    if input_bytes.is_empty() {
        output_buf.clear();
        return Ok(());
    }
    if input_bytes.len() % std::mem::size_of::<T>() != 0 {
        return Err(tambakError::BufferMismatch(
            input_bytes.len(),
            std::mem::size_of::<T>(),
        ));
    }
    // bytemuck::cast_slice is zero-copy, so .to_vec() is the first allocation.
    let mut data_vec: Vec<T> = bytemuck::cast_slice(input_bytes).to_vec();
    xor_undelta_slice_inplace(&mut data_vec);
    output_buf.clear();
    output_buf.extend_from_slice(&typed_slice_to_bytes(&data_vec));
    Ok(())
}

//==================================================================================
// Unit Tests
//==================================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::safe_bytes_to_typed_slice;

    #[test]
    fn test_xor_delta_roundtrip_u32() {
        let original: Vec<u32> = vec![0b1100, 0b1101, 0b1001, 0b1011];
        let expected_encoded: Vec<u32> = vec![0b1100, 0b0001, 0b0100, 0b0010];

        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();

        let encoded_slice = safe_bytes_to_typed_slice::<u32>(&encoded_bytes).unwrap();
        assert_eq!(encoded_slice, expected_encoded.as_slice());

        let mut decoded_bytes = Vec::new();
        decode::<u32>(&encoded_bytes, &mut decoded_bytes).unwrap();

        assert_eq!(decoded_bytes, typed_slice_to_bytes(&original));
    }

    #[test]
    fn test_xor_delta_roundtrip_u64() {
        let original: Vec<u64> = vec![100, 200, 150, 250, 50];

        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();

        let mut decoded_bytes = Vec::new();
        decode::<u64>(&encoded_bytes, &mut decoded_bytes).unwrap();

        assert_eq!(decoded_bytes, typed_slice_to_bytes(&original));
    }

    #[test]
    fn test_xor_delta_empty_slice() {
        let original: Vec<u32> = vec![];
        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();
        assert!(encoded_bytes.is_empty());

        let mut decoded_bytes = Vec::new();
        decode::<u32>(&encoded_bytes, &mut decoded_bytes).unwrap();
        assert!(decoded_bytes.is_empty());
    }

    #[test]
    fn test_xor_delta_single_element_slice() {
        let original: Vec<u32> = vec![42];
        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();
        assert_eq!(encoded_bytes, typed_slice_to_bytes(&original));

        let mut decoded_bytes = Vec::new();
        decode::<u32>(&encoded_bytes, &mut decoded_bytes).unwrap();
        assert_eq!(decoded_bytes, typed_slice_to_bytes(&original));
    }

    #[test]
    fn test_decode_invalid_length_error() {
        let invalid_bytes = vec![1, 2, 3, 4, 5, 6, 7];
        let mut decoded_bytes = Vec::new();
        let result = decode::<u32>(&invalid_bytes, &mut decoded_bytes);
        assert!(result.is_err());
        assert!(matches!(result, Err(tambakError::BufferMismatch(7, 4))));
    }
}
