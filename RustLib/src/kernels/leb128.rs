//! This module contains the pure, stateless kernels for performing LEB128
//! (Little-Endian Base 128) variable-length integer encoding and decoding.
//!
//! This technique is a Layer 3 (Bit-Width Reduction) transform, ideal for streams
//! of unsigned integers where most values are small but some can be large. It
//! uses a continuation bit to store integers in the minimum number of full bytes,
//! providing a good balance between compression and decoding speed.

use num_traits::{PrimInt, Unsigned};
use pyo3::PyResult;

use crate::error::PhoenixError;
use crate::utils::{bytes_to_typed_slice, typed_slice_to_bytes};

//==================================================================================
// 1. Generic Core Logic
//==================================================================================

/// Encodes a single unsigned integer into a LEB128 byte sequence.
///
/// # Type Parameters
/// * `T`: An unsigned primitive integer type.
///
/// # Args
/// * `value`: The integer value to encode.
/// * `buffer`: A mutable `Vec<u8>` to which the encoded bytes will be written.
fn encode_val<T>(mut value: T, buffer: &mut Vec<u8>)
where
    T: PrimInt + Unsigned,
{
    let zero = T::zero();
    let seven_bit_mask = T::from(0x7F).unwrap();
    let continuation_bit = 0x80;

    loop {
        let mut byte = value & seven_bit_mask;
        value = value >> 7;
        if value != zero {
            byte = byte | T::from(continuation_bit).unwrap();
        }
        buffer.push(byte.to_u8().unwrap());
        if value == zero {
            break;
        }
    }
}

/// Decodes a single unsigned integer from a LEB128 byte stream.
///
/// Reads from the `cursor` until a byte without the continuation bit is found,
/// assembling the integer from 7-bit payloads.
///
/// # Returns
/// A `Result` containing the decoded value and the number of bytes read.
fn decode_val<T>(cursor: &mut std::io::Cursor<&[u8]>) -> Result<(T, usize), PhoenixError>
where
    T: PrimInt + Unsigned,
{
    let mut result = T::zero();
    let mut shift = 0;
    let mut bytes_read = 0;

    loop {
        let pos = cursor.position() as usize;
        if pos >= cursor.get_ref().len() {
            return Err(PhoenixError::Leb128DecodeError("Unexpected end of buffer".to_string()));
        }
        let byte = cursor.get_ref()[pos];
        cursor.set_position((pos + 1) as u64);
        bytes_read += 1;

        let seven_bit_payload = T::from(byte & 0x7F).unwrap();
        result = result | (seven_bit_payload << shift);

        if byte & 0x80 == 0 { // Check for continuation bit
            return Ok((result, bytes_read));
        }

        shift += 7;
        if shift >= std::mem::size_of::<T>() * 8 {
            return Err(PhoenixError::Leb128DecodeError("Integer overflow during decoding".to_string()));
        }
    }
}

//==================================================================================
// 2. FFI Dispatcher Logic
//==================================================================================
//==================================================================================
// 2. FFI Dispatcher Logic (REVISED - No Macros)
//==================================================================================

/// The public-facing encode function for this module.
/// It safely converts the raw byte buffer to a typed slice of unsigned integers,
/// calls the generic encoder, and returns the resulting LEB128-encoded bytes.
pub fn encode(bytes: &[u8], original_type: &str) -> PyResult<Vec<u8>> {
    match original_type {
        "UInt8" => {
            let data = unsafe { bytes_to_typed_slice::<u8>(bytes)? };
            let mut buffer = Vec::new();
            for &val in data { encode_val(val, &mut buffer); }
            Ok(buffer)
        },
        "UInt16" => {
            let data = unsafe { bytes_to_typed_slice::<u16>(bytes)? };
            let mut buffer = Vec::new();
            for &val in data { encode_val(val, &mut buffer); }
            Ok(buffer)
        },
        "UInt32" => {
            let data = unsafe { bytes_to_typed_slice::<u32>(bytes)? };
            let mut buffer = Vec::new();
            for &val in data { encode_val(val, &mut buffer); }
            Ok(buffer)
        },
        "UInt64" => {
            let data = unsafe { bytes_to_typed_slice::<u64>(bytes)? };
            let mut buffer = Vec::new();
            for &val in data { encode_val(val, &mut buffer); }
            Ok(buffer)
        },
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string()).into()),
    }
}

/// The public-facing decode function for this module.
/// It calls the generic decoder for the specified type and then converts the
/// resulting typed vector back into a raw byte buffer for Python.
pub fn decode(bytes: &[u8], original_type: &str) -> PyResult<Vec<u8>> {
    match original_type {
        "UInt8" => {
            let decoded_vec: Vec<u8> = decode_slice(bytes)?;
            Ok(typed_slice_to_bytes(&decoded_vec))
        },
        "UInt16" => {
            let decoded_vec: Vec<u16> = decode_slice(bytes)?;
            Ok(typed_slice_to_bytes(&decoded_vec))
        },
        "UInt32" => {
            let decoded_vec: Vec<u32> = decode_slice(bytes)?;
            Ok(typed_slice_to_bytes(&decoded_vec))
        },
        "UInt64" => {
            let decoded_vec: Vec<u64> = decode_slice(bytes)?;
            Ok(typed_slice_to_bytes(&decoded_vec))
        },
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string()).into()),
    }
}

// Helper function required by the dispatcher above.
fn decode_slice<T: PrimInt + Unsigned>(bytes: &[u8]) -> Result<Vec<T>, PhoenixError> {
    let mut decoded = Vec::new();
    let mut cursor = std::io::Cursor::new(bytes);
    while (cursor.position() as usize) < bytes.len() {
        let (val, _) = leb128::decode_val::<T>(&mut cursor)?;
        decoded.push(val);
    }
    Ok(decoded)
}

//==================================================================================
// 3. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leb128_single_value_u64() {
        let mut buffer = Vec::new();
        encode_val(624485u64, &mut buffer);
        // 624485 = 0x98765 = 0b10011000011101100101
        // In 7-bit chunks: 01100101, 1110110, 1001100
        // LEB128 (reversed): 11100101, 10110110, 00010011
        // In hex: 0xE5, 0xB6, 0x13
        assert_eq!(buffer, vec![0xE5, 0xB6, 0x13]);

        let mut cursor = std::io::Cursor::new(&buffer[..]);
        let (decoded, bytes_read) = decode_val::<u64>(&mut cursor).unwrap();
        assert_eq!(decoded, 624485);
        assert_eq!(bytes_read, 3);
    }

    #[test]
    fn test_leb128_roundtrip_u32() {
        let original: Vec<u32> = vec![0, 127, 128, 1000, u32::MAX];
        let original_bytes = typed_slice_to_bytes(&original);

        let encoded_bytes = encode(&original_bytes, "UInt32").unwrap();
        let decoded_bytes = decode(&encoded_bytes, "UInt32").unwrap();

        assert_eq!(original_bytes, decoded_bytes);
    }

    #[test]
    fn test_leb128_decode_truncated() {
        let encoded_bytes = vec![0xE5, 0xB6]; // Missing the final 0x13 byte
        let result = decode(&encoded_bytes, "UInt64");
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Unexpected end of buffer"));
        }
    }

    #[test]
    fn test_leb128_decode_overflow() {
        // A 10-byte sequence representing a number too large for u64
        let encoded_bytes = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
        let result = decode(&encoded_bytes, "UInt64");
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Integer overflow during decoding"));
        }
    }
}