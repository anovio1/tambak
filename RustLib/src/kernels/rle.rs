//! This module contains the pure, stateless kernels for performing Run-Length
//! Encoding (RLE) and decoding.
//!
//! This technique is a Layer 2 (Sparsity Exploitation) transform. It is highly
//! effective for data with low cardinality or long, contiguous runs of identical
//! values. This is especially common in delta-encoded streams, which frequently
//! contain long runs of zeros.
//!
//! The on-disk format is a sequence of `(value, run_length)` pairs. The `run_length`
//! is encoded using LEB128 to efficiently handle both short and very long runs.

use num_traits::PrimInt;
use pyo3::PyResult;
use std::io::{Cursor, Write};

use crate::error::PhoenixError;
use crate::utils::{bytes_to_typed_slice, typed_slice_to_bytes};
// RLE uses LEB128 for its run-length counts, so it has a dependency.
use super::leb128;

//==================================================================================
// 1. Generic Core Logic
//==================================================================================

/// Encodes a slice of primitive integers using Run-Length Encoding.
///
/// Iterates through the data, identifies contiguous runs of identical values,
/// and writes them to a byte buffer as `(value, run_length)` pairs. The `run_length`
/// is LEB128-encoded to be space-efficient.
///
/// # Type Parameters
/// * `T`: A primitive integer type.
///
/// # Args
/// * `data`: A slice of integers to be RLE-encoded.
///
/// # Returns
/// A `Result` containing the RLE-encoded byte buffer.
fn encode_slice<T>(data: &[T]) -> Result<Vec<u8>, PhoenixError>
where
    T: PrimInt + PartialEq,
{
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let mut buffer = Vec::new();
    let mut current_val = data[0];
    let mut run_count: u64 = 1;

    for &val in &data[1..] {
        if val == current_val {
            run_count += 1;
        } else {
            // Write the previous run to the buffer
            buffer.write_all(¤t_val.to_le_bytes()).unwrap();
            leb128::encode_val(run_count, &mut buffer);
            
            // Start a new run
            current_val = val;
            run_count = 1;
        }
    }

    // Write the final run
    buffer.write_all(¤t_val.to_le_bytes()).unwrap();
    leb128::encode_val(run_count, &mut buffer);

    Ok(buffer)
}

/// Decodes an RLE-encoded byte buffer back into a sequence of values.
///
/// Reads `(value, run_length)` pairs from the buffer and expands them into
/// the original data sequence.
///
/// # Type Parameters
/// * `T`: The target primitive integer type.
///
/// # Args
/// * `bytes`: The RLE-encoded byte buffer.
///
/// # Returns
/// A `Result` containing the `Vec<T>` of reconstructed values.
fn decode_slice<T>(bytes: &[u8]) -> Result<Vec<T>, PhoenixError>
where
    T: PrimInt,
{
    let mut decoded = Vec::new();
    let mut cursor = Cursor::new(bytes);
    let element_size = std::mem::size_of::<T>();

    while (cursor.position() as usize) < bytes.len() {
        // 1. Read the value
        let start = cursor.position() as usize;
        if bytes.len() < start + element_size {
            return Err(PhoenixError::RleDecodeError("Truncated buffer: cannot read value".to_string()));
        }
        let value_bytes = &bytes[start .. start + element_size];
        let value = T::from_le_bytes(value_bytes.try_into().unwrap());
        cursor.set_position((start + element_size) as u64);

        // 2. Read the LEB128-encoded run length
        let (run_length, bytes_read) = leb128::decode_val::<u64>(&mut cursor)?;
        if bytes_read == 0 {
            return Err(PhoenixError::RleDecodeError("Invalid LEB128 run length".to_string()));
        }

        // 3. Expand the run
        for _ in 0..run_length {
            decoded.push(value);
        }
    }

    Ok(decoded)
}
//==================================================================================
// 2. FFI Dispatcher Logic (REFINED - No Macros, Clearer Flow)
//==================================================================================

/// The public-facing encode function for this module.
/// It safely converts the raw byte buffer to a typed slice, calls the generic
/// encoder, and returns the resulting encoded bytes.
pub fn encode(bytes: &[u8], original_type: &str) -> PyResult<Vec<u8>> {
    match original_type {
        "Int8" | "UInt8" | "Boolean" => encode_slice(unsafe { bytes_to_typed_slice::<u8>(bytes)? }),
        "Int16" | "UInt16" => encode_slice(unsafe { bytes_to_typed_slice::<u16>(bytes)? }),
        "Int32" | "UInt32" => encode_slice(unsafe { bytes_to_typed_slice::<u32>(bytes)? }),
        "Int64" | "UInt64" => encode_slice(unsafe { bytes_to_typed_slice::<u64>(bytes)? }),
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string()).into()),
    }
    .map_err(|e| e.into()) // Convert PhoenixError to PyErr
}

/// The public-facing decode function for this module.
/// It calls the generic decoder for the specified type and then converts the
/// resulting typed vector back into a raw byte buffer for Python.
pub fn decode(bytes: &[u8], original_type: &str) -> PyResult<Vec<u8>> {
    match original_type {
        "Int8" | "UInt8" | "Boolean" => {
            let decoded_vec: Vec<u8> = decode_slice(bytes)?;
            Ok(typed_slice_to_bytes(&decoded_vec))
        },
        "Int16" | "UInt16" => {
            let decoded_vec: Vec<u16> = decode_slice(bytes)?;
            Ok(typed_slice_to_bytes(&decoded_vec))
        },
        "Int32" | "UInt32" => {
            let decoded_vec: Vec<u32> = decode_slice(bytes)?;
            Ok(typed_slice_to_bytes(&decoded_vec))
        },
        "Int64" | "UInt64" => {
            let decoded_vec: Vec<u64> = decode_slice(bytes)?;
            Ok(typed_slice_to_bytes(&decoded_vec))
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
    fn test_rle_roundtrip_i32() {
        let original: Vec<i32> = vec![5, 5, 5, 5, 8, 8, 8, 2, 9, 9, 9, 9, 9];
        let original_bytes = typed_slice_to_bytes(&original);

        let encoded_bytes = encode(&original_bytes, "Int32").unwrap();
        
        // Expected format:
        // Value 5 (4 bytes) + Run 4 (1 LEB128 byte)
        // Value 8 (4 bytes) + Run 3 (1 LEB128 byte)
        // Value 2 (4 bytes) + Run 1 (1 LEB128 byte)
        // Value 9 (4 bytes) + Run 5 (1 LEB128 byte)
        // Total size = 4*4 + 4*1 = 20 bytes
        assert_eq!(encoded_bytes.len(), 20);

        let decoded_bytes = decode(&encoded_bytes, "Int32").unwrap();
        assert_eq!(original_bytes, decoded_bytes);
    }

    #[test]
    fn test_rle_all_unique_values() {
        let original: Vec<u16> = vec![1, 2, 3, 4, 5];
        let original_bytes = typed_slice_to_bytes(&original);

        let encoded_bytes = encode(&original_bytes, "UInt16").unwrap();
        
        // Each value has a run of 1.
        // Value (2 bytes) + Run 1 (1 byte) = 3 bytes per element
        // Total size = 5 * 3 = 15 bytes
        assert_eq!(encoded_bytes.len(), 15);

        let decoded_bytes = decode(&encoded_bytes, "UInt16").unwrap();
        assert_eq!(original_bytes, decoded_bytes);
    }

    #[test]
    fn test_rle_long_run() {
        let original: Vec<u8> = vec![42; 1000]; // 1000 bytes
        let original_bytes = typed_slice_to_bytes(&original);

        let encoded_bytes = encode(&original_bytes, "UInt8").unwrap();

        // Value 42 (1 byte) + Run 1000 (2 LEB128 bytes) = 3 bytes total
        assert_eq!(encoded_bytes.len(), 3);

        let decoded_bytes = decode(&encoded_bytes, "UInt8").unwrap();
        assert_eq!(original_bytes, decoded_bytes);
    }

    #[test]
    fn test_rle_decode_corrupt_buffer() {
        // A valid value (4 bytes) but a truncated LEB128 run length
        let corrupt_bytes = vec![42, 0, 0, 0, 0b10000001]; 
        let result = decode(&corrupt_bytes, "Int32");
        assert!(result.is_err());
    }
}