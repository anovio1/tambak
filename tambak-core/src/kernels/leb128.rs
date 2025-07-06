//! This module contains the pure, stateless, and performant kernels for performing
//! LEB128 (Little-Endian Base 128) variable-length integer encoding and decoding.
//!
//! This technique is a Layer 3 (Bit-Width Reduction) transform, ideal for streams
//! of unsigned integers where most values are small. It is fully panic-free.

use num_traits::{PrimInt, Unsigned};
use std::io::Cursor;
use bytemuck;

use crate::error::tambakError;

//==================================================================================
// 1. Public API for Single-Value Operations
//==================================================================================

/// Encodes a single unsigned integer into a LEB128 byte sequence, writing to a buffer.
/// This is the primary public function for single-value encoding.
pub fn encode_one<T>(value: T, buffer: &mut Vec<u8>) -> Result<(), tambakError>
where
    T: PrimInt + Unsigned,
{
    let zero = T::zero();
    let seven_bit_mask = T::from(0x7F)
        .ok_or_else(|| tambakError::Leb128DecodeError("Failed to create 7-bit mask for type".to_string()))?;
    let continuation_bit_t = T::from(0x80)
        .ok_or_else(|| tambakError::Leb128DecodeError("Failed to create continuation bit for type".to_string()))?;

    let mut current_value = value;
    loop {
        let mut byte = current_value & seven_bit_mask;
        current_value = current_value >> 7;
        if current_value != zero {
            byte = byte | continuation_bit_t;
        }

        let byte_u8 = byte.to_u8()
            .ok_or_else(|| tambakError::Leb128DecodeError("Failed to convert generic integer to u8".to_string()))?;
        buffer.push(byte_u8);

        if current_value == zero {
            break;
        }
    }
    Ok(())
}

/// Decodes a single unsigned integer from a LEB128 byte stream cursor.
/// This is the primary public function for single-value decoding.
pub fn decode_one<T>(cursor: &mut Cursor<&[u8]>) -> Result<T, tambakError>
where
    T: PrimInt + Unsigned,
{
    let mut result = T::zero();
    let mut shift = 0;
    let total_bits = std::mem::size_of::<T>() * 8;

    loop {
        let pos = cursor.position() as usize;
        let byte = *cursor.get_ref().get(pos)
            .ok_or_else(|| tambakError::Leb128DecodeError("Unexpected end of buffer".to_string()))?;
        cursor.set_position((pos + 1) as u64);

        let seven_bit_payload = T::from(byte & 0x7F)
            .ok_or_else(|| tambakError::Leb128DecodeError("Failed to create 7-bit payload from byte".to_string()))?;
        
        // Check if adding these 7 bits would overflow the type's capacity.
        if shift >= total_bits {
            return Err(tambakError::Leb128DecodeError("Integer overflow during decoding".to_string()));
        }

        result = result | (seven_bit_payload << shift);

        if byte & 0x80 == 0 { // No continuation bit
            // Final check: if the last byte sets bits that are out of bounds for the type, it's an overflow.
            // This happens when the number of bits is not a multiple of 7.
            if shift + 7 > total_bits && (byte >> (total_bits - shift)) > 0 {
                 return Err(tambakError::Leb128DecodeError("Integer overflow during decoding".to_string()));
            }
            return Ok(result);
        }

        shift += 7;
    }
}

//==================================================================================
// 2. Public API for Slice Operations
//==================================================================================

/// The public-facing, generic encode function for an entire slice.
pub fn encode<T>(
    input_slice: &[T],
    output_buf: &mut Vec<u8>,
) -> Result<(), tambakError>
where
    T: PrimInt + Unsigned,
{
    output_buf.clear();
    for &val in input_slice {
        encode_one(val, output_buf)?;
    }
    Ok(())
}

/// The public-facing, generic decode function for an entire slice.
pub fn decode<T>(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    num_values: usize,
) -> Result<(), tambakError>
where
    T: PrimInt + Unsigned + bytemuck::Pod,
{
    output_buf.clear();
    output_buf.reserve(num_values * std::mem::size_of::<T>());
    let mut cursor = Cursor::new(input_bytes);

    for _ in 0..num_values {
        let val: T = decode_one::<T>(&mut cursor)?;
        output_buf.extend_from_slice(bytemuck::bytes_of(&val));
    }

    if (cursor.position() as usize) != input_bytes.len() {
        return Err(tambakError::Leb128DecodeError(
            "Did not consume entire input buffer. Trailing bytes detected.".to_string(),
        ));
    }

    Ok(())
}

//==================================================================================
// 3. Unit Tests
//==================================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::typed_slice_to_bytes;

    #[test]
    fn test_leb128_roundtrip_u32() {
        let original: Vec<u32> = vec![0, 127, 128, 1000, u32::MAX];
        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();
        let mut decoded_bytes = Vec::new();
        decode::<u32>(&encoded_bytes, &mut decoded_bytes, original.len()).unwrap();
        assert_eq!(decoded_bytes, typed_slice_to_bytes(&original));
    }

    #[test]
    fn test_decode_overflow_error() {
        // This represents a value larger than u64::MAX
        let encoded_bytes = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F];
        let mut decoded_bytes = Vec::new();
        let result = decode::<u64>(&encoded_bytes, &mut decoded_bytes, 1);
        assert!(result.is_err());
        if let tambakError::Leb128DecodeError(msg) = result.unwrap_err() {
            assert!(msg.contains("overflow"));
        } else {
            panic!("Expected Leb128DecodeError::IntegerOverflow");
        }
    }
}
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::utils::typed_slice_to_bytes;

//     #[test]
//     fn test_leb128_roundtrip_u32() {
//         let original: Vec<u32> = vec![0, 127, 128, 1000, u32::MAX];
//         let input_slice = &original;

//         let mut encoded_bytes = Vec::new();
//         encode(input_slice, &mut encoded_bytes).unwrap();

//         let mut decoded_bytes = Vec::new();
//         decode::<u32>(&encoded_bytes, &mut decoded_bytes, original.len()).unwrap();

//         let original_as_bytes = typed_slice_to_bytes(&original);
//         assert_eq!(decoded_bytes, original_as_bytes);
//     }

//     #[test]
//     fn test_decode_truncated_buffer() {
//         let original: Vec<u64> = vec![624485]; // Encodes to [0xE5, 0xB6, 0x13]
//         let mut encoded_bytes = Vec::new();
//         encode(&original, &mut encoded_bytes).unwrap();

//         let truncated_bytes = &encoded_bytes[..2];

//         let mut decoded_bytes = Vec::new();
//         let result = decode::<u64>(truncated_bytes, &mut decoded_bytes, 1);
//         assert!(result.is_err());
//         if let Err(e) = result {
//             assert!(e.to_string().contains("Unexpected end of buffer"));
//         }
//     }

//     #[test]
//     fn test_decode_overflow_error() {
//         let encoded_bytes = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
//         let mut decoded_bytes = Vec::new();
//         let result = decode::<u64>(&encoded_bytes, &mut decoded_bytes, 1);
//         assert!(result.is_err());
//         if let Err(e) = result {
//             assert!(e.to_string().contains("Integer overflow during decoding"));
//         }
//     }
// }