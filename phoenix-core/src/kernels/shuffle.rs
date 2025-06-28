//! This module contains the pure, stateless, and performant kernel for performing
//! byte-shuffling on streams of fixed-width primitive types.
//!
//! This technique is a Layer 4 (Byte Distribution) transform. It reorganizes a
//! row-oriented byte stream into a column-oriented or "byte-plane" layout.
//! This module is PURE RUST, panic-free, and uses `bytemuck` for safe casting.

use num_traits::PrimInt;
use bytemuck;

use crate::error::PhoenixError;

//==================================================================================
// 1. Generic Core Logic (The "Engine")
//==================================================================================

/// Performs byte-shuffling on a slice of primitive integers, writing to an output buffer.
fn shuffle_slice<T>(input_slice: &[T], output_buf: &mut Vec<u8>) -> Result<(), PhoenixError>
where
    T: PrimInt + bytemuck::Pod,
{
    let element_size = std::mem::size_of::<T>();
    if element_size <= 1 {
        output_buf.clear();
        output_buf.reserve(input_slice.len());
        for item in input_slice {
            output_buf.extend_from_slice(bytemuck::bytes_of(item));
        }
        return Ok(());
    }

    let num_elements = input_slice.len();
    output_buf.clear();
    output_buf.resize(num_elements * element_size, 0);

    for i in 0..element_size {
        for j in 0..num_elements {
            let byte = bytemuck::bytes_of(&input_slice[j])[i];
            output_buf[i * num_elements + j] = byte;
        }
    }

    Ok(())
}

/// Performs byte-unshuffling on a byte slice, writing to an output buffer.
fn unshuffle_slice<T>(input_bytes: &[u8], output_buf: &mut Vec<u8>) -> Result<(), PhoenixError>
where
    T: PrimInt,
{
    let element_size = std::mem::size_of::<T>();
    if element_size <= 1 {
        output_buf.clear();
        output_buf.extend_from_slice(input_bytes);
        return Ok(());
    }

    if input_bytes.len() % element_size != 0 {
        return Err(PhoenixError::BufferMismatch(input_bytes.len(), element_size));
    }

    let num_elements = input_bytes.len() / element_size;
    output_buf.clear();
    output_buf.resize(input_bytes.len(), 0);

    for i in 0..element_size {
        for j in 0..num_elements {
            let byte = input_bytes[i * num_elements + j];
            output_buf[j * element_size + i] = byte;
        }
    }

    Ok(())
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
    T: PrimInt + bytemuck::Pod,
{
    shuffle_slice(input_slice, output_buf)
}

/// The public-facing, generic decode function for this module.
pub fn decode<T>(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
) -> Result<(), PhoenixError>
where
    T: PrimInt,
{
    unshuffle_slice::<T>(input_bytes, output_buf)
}

//==================================================================================
// 3. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::typed_slice_to_bytes;

    #[test]
    fn test_shuffle_roundtrip_u16() {
        let original: Vec<u16> = vec![0x0102, 0x0304, 0x0506];

        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();

        let expected_encoded: Vec<u8> = vec![0x02, 0x04, 0x06, 0x01, 0x03, 0x05];
        assert_eq!(encoded_bytes, expected_encoded);

        let mut decoded_bytes = Vec::new();
        decode::<u16>(&encoded_bytes, &mut decoded_bytes).unwrap();

        let original_as_bytes = typed_slice_to_bytes(&original);
        assert_eq!(decoded_bytes, original_as_bytes);
    }

    #[test]
    fn test_shuffle_single_byte_type_is_noop_and_safe() {
        let original: Vec<u8> = vec![1, 2, 3, 4, 5];

        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();

        let original_as_bytes = typed_slice_to_bytes(&original);
        assert_eq!(encoded_bytes, original_as_bytes);

        let mut decoded_bytes = Vec::new();
        decode::<u8>(&encoded_bytes, &mut decoded_bytes).unwrap();
        assert_eq!(decoded_bytes, original_as_bytes);
    }

    #[test]
    fn test_decode_invalid_length_error() {
        let invalid_bytes = vec![1, 2, 3, 4, 5, 6, 7];
        let mut decoded_bytes = Vec::new();
        let result = decode::<u16>(&invalid_bytes, &mut decoded_bytes);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, PhoenixError::BufferMismatch(7, 2)));
        }
    }
}