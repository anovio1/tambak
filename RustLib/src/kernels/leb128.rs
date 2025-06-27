//! This module contains the pure, stateless, and performant kernels for performing
//! LEB128 (Little-Endian Base 128) variable-length integer encoding and decoding.
//!
//! This technique is a Layer 3 (Bit-Width Reduction) transform, ideal for streams
//! of unsigned integers where most values are small. It is fully panic-free.

use num_traits::{PrimInt, Unsigned, ToPrimitive};
use std::io::Cursor;

use crate::error::PhoenixError;
use crate::utils::typed_slice_to_bytes;

//==================================================================================
// 1. Generic Core Logic (The "Engine" - REVISED & Panic-Free)
//==================================================================================

/// Encodes a single unsigned integer into a LEB128 byte sequence, writing to a buffer.
/// This function is now fallible and returns a Result.
fn encode_val<T>(mut value: T, buffer: &mut Vec<u8>) -> Result<(), PhoenixError>
where
    T: PrimInt + Unsigned,
{
    let zero = T::zero();
    let seven_bit_mask = T::from(0x7F)
        .ok_or_else(|| PhoenixError::Leb128DecodeError("Failed to create 7-bit mask for type".to_string()))?;
    let continuation_bit_t = T::from(0x80)
        .ok_or_else(|| PhoenixError::Leb128DecodeError("Failed to create continuation bit for type".to_string()))?;

    loop {
        let mut byte = value & seven_bit_mask;
        value = value >> 7;
        if value != zero {
            byte = byte | continuation_bit_t;
        }
        
        let byte_u8 = byte.to_u8()
            .ok_or_else(|| PhoenixError::Leb128DecodeError("Failed to convert generic integer to u8".to_string()))?;
        buffer.push(byte_u8);

        if value == zero {
            break;
        }
    }
    Ok(())
}

/// Decodes a single unsigned integer from a LEB128 byte stream cursor.
/// This function is now fully panic-free.
fn decode_val<T>(cursor: &mut Cursor<&[u8]>) -> Result<T, PhoenixError>
where
    T: PrimInt + Unsigned,
{
    let mut result = T::zero();
    let mut shift = 0;
    let total_bits = std::mem::size_of::<T>() * 8;

    loop {
        let pos = cursor.position() as usize;
        let byte = *cursor.get_ref().get(pos)
            .ok_or_else(|| PhoenixError::Leb128DecodeError("Unexpected end of buffer".to_string()))?;
        cursor.set_position((pos + 1) as u64);

        let seven_bit_payload = T::from(byte & 0x7F)
            .ok_or_else(|| PhoenixError::Leb128DecodeError("Failed to create 7-bit payload from byte".to_string()))?;
        result = result | (seven_bit_payload << shift);

        if byte & 0x80 == 0 { // Check for continuation bit
            return Ok(result);
        }

        shift += 7;
        if shift >= total_bits {
            return Err(PhoenixError::Leb128DecodeError("Integer overflow during decoding".to_string()));
        }
    }
}

//==================================================================================
// 2. Public API (Generic, Performant, Decoupled)
//==================================================================================

/// The public-facing, generic encode function for this module.
/// It takes a typed slice of unsigned integers and writes the LEB128-encoded
/// result into the provided output buffer.
pub fn encode<T>(
    input_slice: &[T],
    output_buf: &mut Vec<u8>,
) -> Result<(), PhoenixError>
where
    T: PrimInt + Unsigned,
{
    for &val in input_slice {
        encode_val(val, output_buf)?;
    }
    Ok(())
}

/// The public-facing, generic decode function for this module.
/// It takes a byte slice of LEB128-encoded data and writes the reconstructed
/// typed data (as bytes) into the provided output buffer.
pub fn decode<T>(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    num_values: usize,
) -> Result<(), PhoenixError>
where
    T: PrimInt + Unsigned,
{
    // 1. Prepare the output buffer according to our buffer-swapping contract.
    //    We clear the vector but retain its allocated capacity for reuse.
    output_buf.clear();

    // 2. Reserve the exact required space upfront. This is a crucial optimization
    //    that prevents multiple, small reallocations as we push data.
    let required_bytes = num_values * std::mem::size_of::<T>();
    output_buf.reserve(required_bytes);

    // 3. Set up the cursor for reading from the input stream.
    let mut cursor = Cursor::new(input_bytes);

    // 4. Decode values one by one and write their bytes directly.
    for _ in 0..num_values {
        // Decode one value from the LEB128 stream.
        let val: T = decode_val::<T>(&mut cursor)?;

        // THIS IS THE KEY CHANGE:
        // Instead of collecting into a `Vec<T>`, we get the little-endian
        // bytes of the decoded value and append them directly to the output buffer.
        // `to_le_bytes()` returns a stack-allocated array (e.g., `[u8; 4]` for u32),
        // which is extremely fast.
        output_buf.extend_from_slice(&val.to_le_bytes());
    }

    // 5. Final validation: ensure all input bytes were consumed. This guards
    //    against malformed input with trailing bytes.
    if (cursor.position() as usize) != input_bytes.len() {
        return Err(PhoenixError::Leb128DecodeError(
            "Did not consume entire input buffer. Trailing bytes detected.".to_string(),
        ));
    }

    Ok(())
}

//==================================================================================
// 3. Unit Tests (Unchanged, but now test the hardened, fallible logic)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::{safe_bytes_to_typed_slice, typed_slice_to_bytes};

    #[test]
    fn test_leb128_roundtrip_u32() {
        let original: Vec<u32> = vec![0, 127, 128, 1000, u32::MAX];
        let input_slice = &original;
        
        let mut encoded_bytes = Vec::new();
        encode(input_slice, &mut encoded_bytes).unwrap();
        
        let mut decoded_bytes = Vec::new();
        decode::<u32>(&encoded_bytes, &mut decoded_bytes, original.len()).unwrap();

        let original_as_bytes = typed_slice_to_bytes(&original);
        assert_eq!(decoded_bytes, original_as_bytes);
    }

    #[test]
    fn test_decode_truncated_buffer() {
        let original: Vec<u64> = vec![624485]; // Encodes to [0xE5, 0xB6, 0x13]
        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();

        let truncated_bytes = &encoded_bytes[..2];

        let mut decoded_bytes = Vec::new();
        let result = decode::<u64>(truncated_bytes, &mut decoded_bytes, 1);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Unexpected end of buffer"));
        }
    }

    #[test]
    fn test_decode_overflow_error() {
        let encoded_bytes = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
        let mut decoded_bytes = Vec::new();
        let result = decode::<u64>(&encoded_bytes, &mut decoded_bytes, 1);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Integer overflow during decoding"));
        }
    }
}