//! This module contains the pure, stateless kernels for performing fixed-width
//! bit-packing and unpacking.
//!
//! This technique is highly effective when integer values in a stream are known
//! to fit within a specific, non-byte-aligned number of bits (e.g., all values
//! are between 0 and 1000, requiring only 10 bits). It packs these values
//! tightly into a byte buffer, eliminating all padding bits.

use bitvec::prelude::*;
use num_traits::{PrimInt, Unsigned};
use pyo3::PyResult;

use crate::error::PhoenixError;
use crate::utils::{bytes_to_typed_slice, typed_slice_to_bytes};

//==================================================================================
// 1. Generic Core Logic
//==================================================================================

/// Encodes a slice of unsigned integers into a compact bit vector.
///
/// This function iterates through the input data, verifies that each value can be
/// represented by `bit_width`, and then appends the lowest `bit_width` bits of
/// each value to a growing bit vector.
///
/// # Type Parameters
/// * `T`: An unsigned primitive integer type.
///
/// # Args
/// * `data`: A slice of unsigned integers to be bit-packed.
/// * `bit_width`: The number of bits to use for each value.
///
/// # Returns
/// A `Result` containing the `BitVec` on success, or an error if a value
/// exceeds the representable range of `bit_width`.
fn encode_slice<T>(data: &[T], bit_width: u8) -> Result<BitVec<u8, Lsb0>, PhoenixError>
where
    T: PrimInt + Unsigned + Into<u64>,
{
    if bit_width == 0 || bit_width > (std::mem::size_of::<T>() * 8) as u8 {
        // Using PyValueError for now, but a more specific error would be better.
        return Err(PhoenixError::BitpackEncodeError(0, bit_width)); // Placeholder error
    }

    let max_val = if bit_width >= 64 { u64::MAX } else { (1u64 << bit_width) - 1 };
    let mut bit_vec = BitVec::<u8, Lsb0>::with_capacity(data.len() * bit_width as usize);

    for &val in data {
        let val_u64: u64 = val.into();
        if val_u64 > max_val {
            return Err(PhoenixError::BitpackEncodeError(val_u64, bit_width));
        }
        // Append the lowest `bit_width` bits to the vector.
        // Lsb0 means we process from least-significant to most-significant bit.
        bit_vec.extend_from_bitslice(&val_u64.view_bits::<Lsb0>()[..bit_width as usize]);
    }

    Ok(bit_vec)
}

/// Decodes a bit vector back into a slice of unsigned integers.
///
/// This function reads chunks of `bit_width` from the bit vector and reconstructs
/// them into integer values.
///
/// # Type Parameters
/// * `T`: The target unsigned primitive integer type.
///
/// # Args
/// * `bits`: The `BitSlice` containing the packed data.
/// * `bit_width`: The number of bits used for each value during encoding.
/// * `num_values`: The exact number of values to decode.
///
/// # Returns
/// A `Result` containing the `Vec<T>` of reconstructed values, or an error if
/// the buffer is truncated.
fn decode_slice<T>(bits: &BitSlice<u8, Lsb0>, bit_width: u8, num_values: usize) -> Result<Vec<T>, PhoenixError>
where
    T: PrimInt + Unsigned + TryFrom<u64>,
{
    if bits.len() < num_values * bit_width as usize {
        return Err(PhoenixError::BitpackDecodeError);
    }

    let mut decoded = Vec::with_capacity(num_values);
    for chunk in bits.chunks(bit_width as usize).take(num_values) {
        // Load the bits into a u64 container for conversion.
        let mut container = 0u64;
        container.view_bits_mut::<Lsb0>()[..chunk.len()].copy_from_bitslice(chunk);
        
        // Attempt to convert the value from u64 to the target type T.
        // This will fail if the value is too large for T, but that shouldn't
        // happen if the bit_width was chosen correctly.
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
// 2. FFI Dispatcher Logic (REVISED - No Macros)
//==================================================================================

/// The public-facing encode function for this module.
pub fn encode(bytes: &[u8], original_type: &str, bit_width: u8) -> PyResult<Vec<u8>> {
    match original_type {
        "UInt8" => {
            let data = unsafe { bytes_to_typed_slice::<u8>(bytes)? };
            encode_slice(data, bit_width).map(|bv| bv.into_vec()).map_err(|e| e.into())
        },
        "UInt16" => {
            let data = unsafe { bytes_to_typed_slice::<u16>(bytes)? };
            encode_slice(data, bit_width).map(|bv| bv.into_vec()).map_err(|e| e.into())
        },
        "UInt32" => {
            let data = unsafe { bytes_to_typed_slice::<u32>(bytes)? };
            encode_slice(data, bit_width).map(|bv| bv.into_vec()).map_err(|e| e.into())
        },
        "UInt64" => {
            let data = unsafe { bytes_to_typed_slice::<u64>(bytes)? };
            encode_slice(data, bit_width).map(|bv| bv.into_vec()).map_err(|e| e.into())
        },
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string()).into()),
    }
}

/// The public-facing decode function for this module.
pub fn decode(bytes: &[u8], original_type: &str, bit_width: u8, num_values: usize) -> PyResult<Vec<u8>> {
    let bits = BitSlice::<u8, Lsb0>::from_slice(bytes);
    match original_type {
        "UInt8" => {
            let result: Vec<u8> = decode_slice(bits, bit_width, num_values)?;
            Ok(typed_slice_to_bytes(&result))
        },
        "UInt16" => {
            let result: Vec<u16> = decode_slice(bits, bit_width, num_values)?;
            Ok(typed_slice_to_bytes(&result))
        },
        "UInt32" => {
            let result: Vec<u32> = decode_slice(bits, bit_width, num_values)?;
            Ok(typed_slice_to_bytes(&result))
        },
        "UInt64" => {
            let result: Vec<u64> = decode_slice(bits, bit_width, num_values)?;
            Ok(typed_slice_to_bytes(&result))
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

    #[test]
    fn test_bitpack_u32_simple() {
        // Values 5, 6, 7 need 3 bits each.
        let original: Vec<u32> = vec![5, 6, 7, 1];
        let original_bytes = typed_slice_to_bytes(&original);
        let bit_width = 3;

        let encoded_bytes = encode(&original_bytes, "UInt32", bit_width).unwrap();
        
        // 5 is 101, 6 is 110, 7 is 111, 1 is 001
        // Lsb0 packs them like: 101110111001....
        // In bytes: 10111011 (0xBB), 111001.. (0x39)
        // bitvec handles padding, so we expect 2 bytes for 12 bits.
        assert_eq!(encoded_bytes.len(), 2);
        assert_eq!(encoded_bytes, vec![0b11101101, 0b00001011]); // Note: bitvec packing order

        let decoded_bytes = decode(&encoded_bytes, "UInt32", bit_width, original.len()).unwrap();
        let decoded: Vec<u32> = unsafe { bytes_to_typed_slice(&decoded_bytes).unwrap().to_vec() };

        assert_eq!(decoded, original);
    }

    #[test]
    fn test_bitpack_value_too_large() {
        let original: Vec<u32> = vec![5, 6, 8, 1]; // 8 requires 4 bits
        let original_bytes = typed_slice_to_bytes(&original);
        let bit_width = 3;

        let result = encode(&original_bytes, "UInt32", bit_width);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("exceeds bit width 3"));
        }
    }

    #[test]
    fn test_bitpack_truncated_buffer() {
        let original: Vec<u16> = vec![10, 20, 30];
        let original_bytes = typed_slice_to_bytes(&original);
        let bit_width = 5;

        let encoded_bytes = encode(&original_bytes, "UInt16", bit_width).unwrap();
        
        // Truncate the buffer by one byte
        let truncated_bytes = &encoded_bytes[..encoded_bytes.len() - 1];

        let result = decode(truncated_bytes, "UInt16", bit_width, original.len());
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("truncated buffer"));
        }
    }
}