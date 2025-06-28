//! This module contains the pure, stateless, and performant kernels for performing
//! fixed-width bit-packing and unpacking.
//!
//! This technique is a Layer 3 (Bit-Width Reduction) transform. It is highly
//! effective when integer values in a stream are known to fit within a specific,
//! non-byte-aligned number of bits. It packs these values tightly into a byte
//! buffer, eliminating all padding bits. This module is PURE RUST and has no
//! FFI or Polars dependencies.

use bitvec::prelude::*;
use num_traits::{PrimInt, Unsigned, ToPrimitive};

use crate::error::PhoenixError;
use crate::utils::typed_slice_to_bytes;

//==================================================================================
// 1. Generic Core Logic (The "Engine" - CORRECTED)
//==================================================================================

/// Encodes a slice of unsigned integers into a compact bit vector.
///
/// This is the internal workhorse. It iterates through the input data, verifies
/// that each value can be represented by `bit_width`, and then appends the
/// lowest `bit_width` bits of each value to a growing bit vector.
fn encode_slice<T>(data: &[T], bit_width: u8) -> Result<BitVec<u8, Lsb0>, PhoenixError>
where
    T: PrimInt + Unsigned + ToPrimitive,
{
    if bit_width == 0 || bit_width > (std::mem::size_of::<T>() * 8) as u8 {
        return Err(PhoenixError::BitpackEncodeError(0, bit_width));
    }

    let max_val = if bit_width >= 64 { u64::MAX } else { (1u64 << bit_width) - 1 };
    let mut bit_vec = BitVec::<u8, Lsb0>::with_capacity(data.len() * bit_width as usize);

    for &val in data {
        let val_u64 = val.to_u64().ok_or_else(|| PhoenixError::UnsupportedType("Failed to convert value to u64 for bitpacking".to_string()))?;
        if val_u64 > max_val {
            return Err(PhoenixError::BitpackEncodeError(val_u64, bit_width));
        }
        bit_vec.extend_from_bitslice(&val_u64.view_bits::<Lsb0>()[..bit_width as usize]);
    }

    Ok(bit_vec)
}

/// Decodes a bit vector back into a slice of unsigned integers.
///
/// This is the internal workhorse. It reads chunks of `bit_width` from the bit
/// vector and reconstructs them into integer values.
fn decode_slice<T>(
    bits: &BitSlice<u8, Lsb0>,
    bit_width: u8, // <-- FIX: bit_width is a required parameter, not inferred.
    num_values: usize,
) -> Result<Vec<T>, PhoenixError>
where
    T: PrimInt + Unsigned + TryFrom<u64>,
{
    if bit_width == 0 {
        return if num_values == 0 { Ok(Vec::new()) } else { Err(PhoenixError::BitpackDecodeError) };
    }
    if bits.len() < num_values * bit_width as usize {
        return Err(PhoenixError::BitpackDecodeError);
    }

    let mut decoded = Vec::with_capacity(num_values);
    for chunk in bits.chunks(bit_width as usize).take(num_values) {
        let mut container = 0u64;
        container.view_bits_mut::<Lsb0>()[..chunk.len()].copy_from_bitslice(chunk);
        
        // FIX: Use safe `try_from` to prevent potential overflow on conversion.
        if let Ok(val) = T::try_from(container) {
            decoded.push(val);
        } else {
            // This indicates a logic error or data corruption.
            return Err(PhoenixError::BitpackDecodeError);
        }
    }

    Ok(decoded)
}

//==================================================================================
// 2. Public API (Generic, Performant, Decoupled)
//==================================================================================

/// The public-facing, generic encode function for this module.
/// It takes a typed slice and writes the bit-packed result into the provided output buffer.
pub fn encode<T>(
    input_slice: &[T],
    output_buf: &mut Vec<u8>,
    bit_width: u8,
) -> Result<(), PhoenixError>
where
    T: PrimInt + Unsigned + ToPrimitive,
{
    let bit_vec = encode_slice(input_slice, bit_width)?;
    output_buf.extend_from_slice(bit_vec.as_raw_slice());
    Ok(())
}

/// The public-facing, generic decode function for this module.
/// It takes a byte slice of bit-packed data and writes the reconstructed typed
/// data (as bytes) into the provided output buffer.
pub fn decode<T>(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    bit_width: u8,
    num_values: usize,
) -> Result<(), PhoenixError>
where
    T: PrimInt + Unsigned + TryFrom<u64>,
{
    let bits = BitSlice::<u8, Lsb0>::from_slice(input_bytes);
    let decoded_vec: Vec<T> = decode_slice(bits, bit_width, num_values)?;
    
    // Convert the final typed vector to bytes and extend the output buffer
    output_buf.extend_from_slice(&typed_slice_to_bytes(&decoded_vec));
    Ok(())
}

//==================================================================================
// 3. Unit Tests (REVISED - Tests now target the corrected API)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::typed_slice_to_bytes;

    #[test]
    fn test_bitpack_u32_roundtrip() {
        let original: Vec<u32> = vec![5, 6, 7, 1];
        let bit_width = 3;

        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes, bit_width).unwrap();
        
        assert_eq!(encoded_bytes.len(), 2);

        let mut decoded_as_bytes = Vec::new();
        decode::<u32>(&encoded_bytes, &mut decoded_as_bytes, bit_width, original.len()).unwrap();
        
        let original_as_bytes = typed_slice_to_bytes(&original);
        assert_eq!(decoded_as_bytes, original_as_bytes);
    }

    #[test]
    fn test_decode_truncated_buffer_error() {
        let original: Vec<u16> = vec![10, 20, 30];
        let bit_width = 5;
        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes, bit_width).unwrap();
        
        // Truncate the buffer
        encoded_bytes.pop();

        let mut decoded_bytes = Vec::new();
        let result = decode::<u16>(&encoded_bytes, &mut decoded_bytes, bit_width, original.len());
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, PhoenixError::BitpackDecodeError));
        }
    }
    
    #[test]
    fn test_decode_slice_logic_is_correct() {
        // Manually create a bitvec to test the core logic
        // Values 5 (101), 6 (110), 7 (111) with bit_width 4
        let mut bv = bitvec![u8, Lsb0;];
        bv.extend_from_bitslice(&5u8.view_bits::<Lsb0>()[..4]);
        bv.extend_from_bitslice(&6u8.view_bits::<Lsb0>()[..4]);
        bv.extend_from_bitslice(&7u8.view_bits::<Lsb0>()[..4]);
        
        let decoded_vec = decode_slice::<u8>(bv.as_bitslice(), 4, 3).unwrap();
        assert_eq!(decoded_vec, vec![5, 6, 7]);
    }
}