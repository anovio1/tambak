//! This module contains the pure, stateless, and performant kernels for performing
//! Run-Length Encoding (RLE) and decoding.
//!
//! This technique is a Layer 2 (Sparsity Exploitation) transform. It is highly
//! effective for data with low cardinality or long, contiguous runs of identical
//! values. The on-disk format is a sequence of `(value, run_length)` pairs, where
//! the `run_length` is itself LEB128-encoded for space efficiency. This module is
//! PURE RUST, panic-free, and has no FFI or Polars dependencies.

use num_traits::PrimInt;
use std::io::Cursor;

use crate::error::PhoenixError;
// RLE uses LEB128 for its run-length counts, so it has a dependency on the
// hardened, fallible leb128 kernel.
use super::leb128;

//==================================================================================
// 1. Public API (Generic, Performant, Decoupled)
//==================================================================================

/// The public-facing, generic encode function for this module.
/// It takes a typed slice and writes the RLE-encoded result into the provided output buffer.
pub fn encode<T>(
    input_slice: &[T],
    output_buf: &mut Vec<u8>,
) -> Result<(), PhoenixError>
where
    T: PrimInt + PartialEq,
{
    // Prepare the output buffer per our contract.
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
            // Write the previous run to the buffer
            output_buf.extend_from_slice(current_val.to_le_bytes());
            leb128::encode_val(run_count, output_buf)?;
            
            // Start a new run
            current_val = val;
            run_count = 1;
        }
    }

    // Write the final run
    output_buf.extend_from_slice(current_val.to_le_bytes());
    leb128::encode_val(run_count, output_buf)?;

    Ok(())
}

/// The public-facing, generic decode function for this module.
/// It takes a byte slice of RLE-encoded data and writes the reconstructed typed
/// data (as bytes) directly into the provided output buffer, avoiding intermediate allocations.
pub fn decode<T>(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
) -> Result<(), PhoenixError>
where
    T: PrimInt,
{
    // Prepare the output buffer per our contract.
    output_buf.clear();

    let mut cursor = Cursor::new(input_bytes);
    let element_size = std::mem::size_of::<T>();

    while (cursor.position() as usize) < input_bytes.len() {
        // 1. Read the value bytes
        let start = cursor.position() as usize;
        let end = start + element_size;
        let value_bytes = input_bytes.get(start..end)
            .ok_or_else(|| PhoenixError::RleDecodeError("Truncated buffer: cannot read value".to_string()))?;
        
        // 2. Convert bytes to value T (NOW PANIC-FREE)
        let value = T::from_le_bytes(value_bytes.try_into().map_err(|_| {
            PhoenixError::RleDecodeError(format!(
                "Slice with incorrect length to convert to type T; expected {}",
                element_size
            ))
        })?);
        cursor.set_position(end as u64);

        // 3. Read the LEB128-encoded run length
        let run_length = leb128::decode_val::<u64>(&mut cursor)?;

        // 4. Expand the run by writing the value's bytes directly to the output buffer.
        //    This is the key performance optimization.
        for _ in 0..run_length {
            output_buf.extend_from_slice(value_bytes);
        }
    }

    Ok(())
}

//==================================================================================
// 2. Unit Tests (Revised to test the new public API pattern)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::typed_slice_to_bytes;

    #[test]
    fn test_rle_roundtrip_i32() {
        let original: Vec<i32> = vec![5, 5, 5, 5, 8, 8, 8, 2, 9, 9, 9, 9, 9];
        
        // --- Test Encode ---
        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();
        
        assert_eq!(encoded_bytes.len(), 20);

        // --- Test Decode ---
        let mut decoded_bytes = Vec::new();
        decode::<i32>(&encoded_bytes, &mut decoded_bytes).unwrap();

        let original_as_bytes = typed_slice_to_bytes(&original);
        assert_eq!(decoded_bytes, original_as_bytes);
    }

    #[test]
    fn test_rle_long_run_u8() {
        let original: Vec<u8> = vec![42; 1000];
        
        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();

        assert_eq!(encoded_bytes.len(), 3);

        let mut decoded_bytes = Vec::new();
        decode::<u8>(&encoded_bytes, &mut decoded_bytes).unwrap();
        
        // In this specific test case, the decoded bytes are the original vec.
        assert_eq!(decoded_bytes, original);
    }

    #[test]
    fn test_rle_decode_corrupt_buffer_error() {
        let corrupt_bytes = vec![42, 0, 0, 0, 0b10000001]; 
        let mut decoded_bytes = Vec::new();
        let result = decode::<i32>(&corrupt_bytes, &mut decoded_bytes);
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
        decode::<i64>(&encoded_bytes, &mut decoded_bytes).unwrap();
        assert!(decoded_bytes.is_empty());
    }
}