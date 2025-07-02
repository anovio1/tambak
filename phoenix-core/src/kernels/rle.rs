//! This module contains the pure, stateless, and performant kernels for performing
//! Run-Length Encoding (RLE) and decoding.
//!
//! This technique is a Layer 2 (Sparsity Exploitation) transform. It is highly
//! effective for data with low cardinality or long, contiguous runs of identical
//! values. The on-disk format is a sequence of `(value, run_length)` pairs, where
//! the `run_length` is itself LEB128-encoded for space efficiency. This module is
//! PURE RUST, panic-free, and has no FFI or Polars dependencies.

use bytemuck;
use num_traits::PrimInt;
use std::io::Cursor;

use super::leb128;
use crate::error::PhoenixError;

//==================================================================================
// 1. Public API (Generic, Performant, Decoupled)
//==================================================================================

/// The public-facing, generic encode function for this module.
pub fn encode<T>(input_slice: &[T], output_buf: &mut Vec<u8>) -> Result<(), PhoenixError>
where
    T: PrimInt + PartialEq + bytemuck::Pod + std::fmt::Debug,
{
    output_buf.clear();

    if input_slice.is_empty() {
        return Ok(());
    }

    let mut current_val = input_slice[0];
    let mut run_count: u64 = 1;

    for &val in &input_slice[1..] {
        if val == current_val {
            run_count += 1;
        } else {
            output_buf.extend_from_slice(bytemuck::bytes_of(&current_val));
            leb128::encode_one(run_count, output_buf)?;
            current_val = val;
            run_count = 1;
        }
    }

    output_buf.extend_from_slice(bytemuck::bytes_of(&current_val));
    leb128::encode_one(run_count, output_buf)?;

    Ok(())
}

/// The public-facing, generic decode function for this module.
pub fn decode<T>(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    num_values: usize,
) -> Result<(), PhoenixError>
where
    T: PrimInt + bytemuck::Pod,
{
    output_buf.clear();
    if num_values == 0 {
        return Ok(());
    }
    // Pre-allocate and initialize the buffer. This is faster than repeated `extend`.
    output_buf.resize(num_values * std::mem::size_of::<T>(), 0);

    let mut cursor = Cursor::new(input_bytes);
    let element_size = std::mem::size_of::<T>();
    let mut values_decoded = 0;

    // --- THE CORRECT LOOP CONDITION ---
    // Loop until we have produced the expected number of values.
    while values_decoded < num_values {
        // Read the value for the next run.
        let value_start = cursor.position() as usize;
        let value_end = value_start + element_size;
        let value_bytes = input_bytes.get(value_start..value_end)
            .ok_or_else(|| PhoenixError::RleDecodeError(format!(
                "Input stream truncated: needed to read a value for a new run but stream ended. Decoded {} of {} values.",
                values_decoded, num_values
            )))?;
        cursor.set_position(value_end as u64);

        // Read the run length.
        let run_length = leb128::decode_one::<u64>(&mut cursor)? as usize;

        // Check for overflow before writing.
        if values_decoded + run_length > num_values {
            return Err(PhoenixError::RleDecodeError(format!(
                "Corrupt RLE stream: run of length {} would produce {} total values, but expected only {}.",
                run_length, values_decoded + run_length, num_values
            )));
        }

        // Write the run to the pre-allocated output buffer.
        for i in 0..run_length {
            let start = (values_decoded + i) * element_size;
            let end = start + element_size;
            output_buf[start..end].copy_from_slice(value_bytes);
        }
        values_decoded += run_length;
    }

    // This final check is now redundant if the loop condition is correct, but it's good for safety.
    if values_decoded != num_values {
        return Err(PhoenixError::RleDecodeError(format!(
            "Decoded to {} values, but expected {}",
            values_decoded, num_values
        )));
    }

    Ok(())
}

//==================================================================================
// 2. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::typed_slice_to_bytes;

    #[test]
    fn test_rle_roundtrip_i32() {
        let original: Vec<i32> = vec![5, 5, 5, 5, 8, 8, 8, 2, 9, 9, 9, 9, 9];

        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();

        let mut decoded_bytes = Vec::new();
        decode::<i32>(&encoded_bytes, &mut decoded_bytes, original.len()).unwrap();

        let original_as_bytes = typed_slice_to_bytes(&original);
        assert_eq!(decoded_bytes, original_as_bytes);
    }

    #[test]
    fn test_rle_long_run_u8() {
        let original: Vec<u8> = vec![42; 1000];

        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();

        let mut decoded_bytes = Vec::new();
        decode::<u8>(&encoded_bytes, &mut decoded_bytes, original.len()).unwrap();

        let original_as_bytes = typed_slice_to_bytes(&original);
        assert_eq!(decoded_bytes, original_as_bytes);
    }

    #[test]
    fn test_rle_decode_corrupt_buffer_error() {
        let corrupt_bytes = vec![42, 0, 0, 0, 0b10000001];
        let mut decoded_bytes = Vec::new();
        let result = decode::<i32>(&corrupt_bytes, &mut decoded_bytes, 1);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Unexpected end of buffer"));
        }
    }

    #[test]
    fn test_empty_slice_roundtrip() {
        let original: Vec<i64> = vec![];
        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();
        assert!(encoded_bytes.is_empty());

        let mut decoded_bytes = Vec::new();
        decode::<i64>(&encoded_bytes, &mut decoded_bytes, 0).unwrap();
        assert!(decoded_bytes.is_empty());
    }
}
