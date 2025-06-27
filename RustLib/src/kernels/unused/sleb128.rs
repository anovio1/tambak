//! This module contains the pure, stateless kernels for performing SLEB128
//! (Signed Little-Endian Base 128) variable-length integer encoding and decoding.
//!
//! This technique is a Layer 3 (Bit-Width Reduction) transform, specifically for
//! signed integer streams, which are the common output of delta encoding. It uses
//! a continuation bit to store integers in the minimum number of full bytes.

use num_traits::{PrimInt, Signed};
use pyo3::PyResult;
use std::io::{Cursor, Write};

use crate::error::PhoenixError;
use crate::utils::{bytes_to_typed_slice, typed_slice_to_bytes};

//==================================================================================
// 1. Generic Core Logic
//==================================================================================

/// Encodes a single signed integer into a SLEB128 byte sequence.
///
/// # Type Parameters
/// * `T`: A signed primitive integer type.
///
/// # Args
/// * `value`: The integer value to encode.
/// * `buffer`: A mutable `Vec<u8>` to which the encoded bytes will be written.
fn encode_val<T>(mut value: T, buffer: &mut Vec<u8>)
where
    T: PrimInt + Signed,
{
    let seven_bit_mask = T::from(0x7F).unwrap();
    let continuation_bit = 0x80;

    loop {
        let byte = (value & seven_bit_mask).to_u8().unwrap();
        // Arithmetic shift right to preserve the sign
        value = value.signed_shr(7);

        // The "done" condition checks if the remaining bits are just a sign extension
        // of the 7th bit of the byte we are about to write.
        let sign_bit_set = (byte & 0x40) != 0;
        let done = (value == T::zero() && !sign_bit_set) || (value == T::one().wrapping_neg() && sign_bit_set);

        if done {
            buffer.push(byte);
            break;
        } else {
            buffer.push(byte | continuation_bit);
        }
    }
}

/// Decodes a single signed integer from a SLEB128 byte stream.
///
/// Reads from the `cursor` until a byte without the continuation bit is found,
/// then performs sign extension if necessary.
///
/// # Returns
/// A `Result` containing the decoded value.
fn decode_val<T>(cursor: &mut Cursor<&[u8]>) -> Result<T, PhoenixError>
where
    T: PrimInt + Signed,
{
    let mut result = T::zero();
    let mut shift = 0;
    let total_bits = std::mem::size_of::<T>() * 8;

    loop {
        let pos = cursor.position() as usize;
        if pos >= cursor.get_ref().len() {
            return Err(PhoenixError::Leb128DecodeError("Unexpected end of buffer".to_string()));
        }
        let byte = cursor.get_ref()[pos];
        cursor.set_position((pos + 1) as u64);

        let seven_bit_payload = T::from(byte & 0x7F).unwrap();
        result = result | (seven_bit_payload << shift);
        shift += 7;

        if byte & 0x80 == 0 { // Check for continuation bit
            // If the sign bit of our decoded payload is set, we need to sign-extend
            if shift < total_bits && (byte & 0x40) != 0 {
                result = result | (T::one().wrapping_neg() << shift);
            }
            return Ok(result);
        }

        if shift >= total_bits {
            return Err(PhoenixError::Leb128DecodeError("Integer overflow during decoding".to_string()));
        }
    }
}

//==================================================================================
// 2. FFI Dispatcher Logic (No Macros)
//==================================================================================

/// The public-facing encode function for this module.
pub fn encode(bytes: &[u8], original_type: &str) -> PyResult<Vec<u8>> {
    match original_type {
        "Int8" => {
            let data = unsafe { bytes_to_typed_slice::<i8>(bytes)? };
            let mut buffer = Vec::new();
            for &val in data { encode_val(val, &mut buffer); }
            Ok(buffer)
        },
        "Int16" => {
            let data = unsafe { bytes_to_typed_slice::<i16>(bytes)? };
            let mut buffer = Vec::new();
            for &val in data { encode_val(val, &mut buffer); }
            Ok(buffer)
        },
        "Int32" => {
            let data = unsafe { bytes_to_typed_slice::<i32>(bytes)? };
            let mut buffer = Vec::new();
            for &val in data { encode_val(val, &mut buffer); }
            Ok(buffer)
        },
        "Int64" => {
            let data = unsafe { bytes_to_typed_slice::<i64>(bytes)? };
            let mut buffer = Vec::new();
            for &val in data { encode_val(val, &mut buffer); }
            Ok(buffer)
        },
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string()).into()),
    }
}

/// The public-facing decode function for this module.
pub fn decode(bytes: &[u8], original_type: &str) -> PyResult<Vec<u8>> {
    let mut cursor = Cursor::new(bytes);
    match original_type {
        "Int8" => {
            let mut decoded = Vec::new();
            while (cursor.position() as usize) < bytes.len() {
                decoded.push(decode_val::<i8>(&mut cursor)?);
            }
            Ok(typed_slice_to_bytes(&decoded))
        },
        "Int16" => {
            let mut decoded = Vec::new();
            while (cursor.position() as usize) < bytes.len() {
                decoded.push(decode_val::<i16>(&mut cursor)?);
            }
            Ok(typed_slice_to_bytes(&decoded))
        },
        "Int32" => {
            let mut decoded = Vec::new();
            while (cursor.position() as usize) < bytes.len() {
                decoded.push(decode_val::<i32>(&mut cursor)?);
            }
            Ok(typed_slice_to_bytes(&decoded))
        },
        "Int64" => {
            let mut decoded = Vec::new();
            while (cursor.position() as usize) < bytes.len() {
                decoded.push(decode_val::<i64>(&mut cursor)?);
            }
            Ok(typed_slice_to_bytes(&decoded))
        },
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string()).into()),
    }
}

//==================================================================================
// 3. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_single<T>(val: T)
    where
        T: PrimInt + Signed + std::fmt::Debug,
    {
        let mut buffer = Vec::new();
        encode_val(val, &mut buffer);
        let mut cursor = Cursor::new(&buffer[..]);
        let decoded = decode_val::<T>(&mut cursor).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_sleb128_edge_cases() {
        roundtrip_single(0i64);
        roundtrip_single(1i64);
        roundtrip_single(-1i64);
        roundtrip_single(63i64);
        roundtrip_single(-64i64);
        roundtrip_single(64i64);
        roundtrip_single(-65i64);
        roundtrip_single(127i64);
        roundtrip_single(-128i64);
        roundtrip_single(128i64);
        roundtrip_single(-129i64);
    }

    #[test]
    fn test_sleb128_specific_values() {
        let mut buffer = Vec::new();
        encode_val(-127i32, &mut buffer);
        assert_eq!(buffer, vec![0x81, 0x7f]); // -127 = 0b...10000001 -> 01, 1111111 -> 0x81, 0x7f
    }

    #[test]
    fn test_sleb128_roundtrip_vec_i32() {
        let original: Vec<i32> = vec![0, 1, -1, 1000, -1000, i32::MAX, i32::MIN];
        let original_bytes = typed_slice_to_bytes(&original);

        let encoded_bytes = encode(&original_bytes, "Int32").unwrap();
        let decoded_bytes = decode(&encoded_bytes, "Int32").unwrap();

        assert_eq!(original_bytes, decoded_bytes);
    }

    #[test]
    fn test_sleb128_decode_unsupported_type() {
        let bytes = vec![0u8];
        let result = decode(&bytes, "UInt32");
        assert!(result.is_err());
    }
}