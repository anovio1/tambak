//! This module contains the pure, stateless, and performant kernel for performing
//! byte-shuffling on streams of fixed-width primitive types.
//!
//! This technique is a Layer 4 (Byte Distribution) transform. It reorganizes a
//! row-oriented byte stream into a column-oriented or "byte-plane" layout.
//! This module is PURE RUST, panic-free, and contains no `unsafe` code blocks.

use num_traits::PrimInt;

use crate::error::PhoenixError;

//==================================================================================
// 1. Generic Core Logic (The "Engine" - REVISED & Unsafe-Free)
//==================================================================================

/// Performs byte-shuffling on a slice of primitive integers, writing to an output buffer.
///
/// This is the core implementation. It iterates through the input slice, transposing
/// the bytes into the output buffer.
fn shuffle_slice<T>(input_slice: &[T], output_buf: &mut Vec<u8>) -> Result<(), PhoenixError>
where
    T: PrimInt,
{
    let element_size = std::mem::size_of::<T>();
    if element_size <= 1 {
        // --- FIX: Replaced unsafe block with a safe implementation ---
        // Shuffling is a no-op for single-byte types. We perform a safe copy.
        output_buf.clear();
        output_buf.reserve(input_slice.len());
        for item in input_slice {
            // to_le_bytes() is optimized by the compiler for single-byte types
            // and will not perform extra allocations.
            output_buf.push(item.to_le_bytes()[0]);
        }
        return Ok(());
    }

    let num_elements = input_slice.len();
    output_buf.clear();
    output_buf.resize(num_elements * element_size, 0);

    for i in 0..element_size { // For each byte plane (0th byte, 1st byte, etc.)
        for j in 0..num_elements { // For each element in the input
            // Get the i-th byte of the j-th element
            let byte = input_slice[j].to_le_bytes()[i];
            // Place it in the correct transposed position in the output
            output_buf[i * num_elements + j] = byte;
        }
    }

    Ok(())
}

/// Performs byte-unshuffling on a byte slice, writing to an output buffer.
///
/// This is the pure mathematical inverse of `shuffle_slice`.
fn unshuffle_slice<T>(input_bytes: &[u8], output_buf: &mut Vec<u8>) -> Result<(), PhoenixError>
where
    T: PrimInt,
{
    let element_size = std::mem::size_of::<T>();
    if element_size <= 1 {
        // Unshuffling is also a no-op for single-byte types.
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

    for i in 0..element_size { // For each byte plane
        for j in 0..num_elements { // For each element
            // Get the byte from the i-th plane for the j-th element
            let byte = input_bytes[i * num_elements + j];
            // Place it back in the correct byte position of the j-th element in the output
            output_buf[j * element_size + i] = byte;
        }
    }

    Ok(())
}

//==================================================================================
// 2. Public API (Generic, Performant, Decoupled)
//==================================================================================

/// The public-facing, generic encode function for this module.
/// It takes a typed slice and writes the byte-shuffled result into the output buffer.
pub fn encode<T>(
    input_slice: &[T],
    output_buf: &mut Vec<u8>,
) -> Result<(), PhoenixError>
where
    T: PrimInt,
{
    shuffle_slice(input_slice, output_buf)
}

/// The public-facing, generic decode function for this module.
/// It takes a byte slice of shuffled data and writes the reconstructed original
/// byte layout into the provided output buffer.
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
// 3. Unit Tests (Unchanged, but now validate the safe implementation)
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
        
        assert_eq!(encoded_bytes, original);

        let mut decoded_bytes = Vec::new();
        decode::<u8>(&encoded_bytes, &mut decoded_bytes).unwrap();
        assert_eq!(decoded_bytes, original);
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