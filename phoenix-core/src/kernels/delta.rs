//! This module contains the pure, stateless, and performant kernels for performing
//! delta encoding and decoding.
//!
//! This is a Layer 1 (Value Reduction) transform. The core algorithms are
//! implemented **in-place** for maximum performance. The public API then wraps
//! this logic to conform to the `executor`'s buffer-swapping strategy.

use crate::error::PhoenixError;
use crate::utils::typed_slice_to_bytes;

//==================================================================================
// 1. Generic Core Logic (The "Engine" - In-Place & Performant)
//==================================================================================

/// Performs delta encoding **in-place** on a mutable slice of primitive integers.
///
/// This is the most performant approach as it avoids new allocations during the
/// computation. It calculates `data[i] = data[i] - data[i - order]`.
fn encode_slice_inplace<T>(data: &mut [T], order: usize)
where
    T: Copy + bytemuck::Pod + std::ops::Sub<Output = T>,
{
    if data.len() <= order {
        return;
    }
    // Iterate backwards to use original values for calculation
    for i in (order..data.len()).rev() {
        data[i] = data[i] - data[i - order];
    }
}

/// Performs delta decoding (cumulative sum) **in-place** on a mutable slice.
///
/// This is the most performant approach for decoding. It reconstructs the original
/// values by calculating `data[i] = data[i] + data[i - order]`.
fn decode_slice_inplace<T>(data: &mut [T], order: usize)
where
    // MODIFIED: Generalize from WrappingAdd to work with floats.
    T: Copy + bytemuck::Pod + std::ops::Add<Output = T>,
{
    if data.len() <= order {
        return;
    }
    // Iterate forwards to use the newly-decoded values for subsequent sums
    for i in order..data.len() {
        data[i] = data[i] + data[i - order];
    }
}

//==================================================================================
// 2. Public API (Generic, Decoupled, and Adhering to Executor Contract)
//==================================================================================

/// The public-facing, generic encode function for this module.
/// It creates a mutable copy of the input, performs the fast in-place delta
/// encoding, and writes the result to the provided output buffer.
pub fn encode<T>(
    input_slice: &[T],
    output_buf: &mut Vec<u8>,
    order: usize,
) -> Result<(), PhoenixError>
where
    // MODIFIED: Propagate generalized trait bound.
    T: Copy + bytemuck::Pod + std::ops::Sub<Output = T>,
{
    output_buf.clear();
    // 1. Create a mutable copy. This is the one necessary allocation to enable
    //    the fast in-place algorithm while respecting the immutable input slice.
    let mut data_vec = input_slice.to_vec();

    // 2. Perform the fast, in-place encoding on our mutable copy.
    encode_slice_inplace(&mut data_vec, order);

    // 3. Write the final, modified bytes to the output buffer.
    output_buf.extend_from_slice(&typed_slice_to_bytes(&data_vec));
    Ok(())
}

/// The public-facing, generic decode function for this module.
/// It creates a mutable copy of the delta-encoded input, performs the in-place
/// decoding, and writes the reconstructed bytes to the output buffer.
pub fn decode<T>(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    order: usize,
) -> Result<(), PhoenixError>
where
    T: Copy + bytemuck::Pod + std::ops::Add<Output = T>,
{
    output_buf.clear();
    if input_bytes.len() % std::mem::size_of::<T>() != 0 {
        return Err(PhoenixError::BufferMismatch(
            input_bytes.len(),
            std::mem::size_of::<T>(),
        ));
    }
    let mut data_vec: Vec<T> = input_bytes
        .chunks_exact(std::mem::size_of::<T>())
        .map(|chunk| {
            *bytemuck::try_from_bytes::<T>(chunk).expect("Failed to convert bytes to value")
        })
        .collect();

    // 2. Perform the fast, in-place decoding.
    decode_slice_inplace(&mut data_vec, order);

    // 3. Write the final, reconstructed bytes to the output buffer.
    output_buf.extend_from_slice(&typed_slice_to_bytes(&data_vec));
    Ok(())
}

//==================================================================================
// 3. Unit Tests (Revised to test the new public API pattern)
//==================================================================================

#[cfg(test)]
mod tests {
    use crate::utils::safe_bytes_to_typed_slice;

    use super::*;

    #[test]
    fn test_public_api_encode_decode_roundtrip() {
        // This test now serves as a regression test for integer functionality.
        let original: Vec<i64> = vec![100, 110, 115, 112, 122];
        let expected_encoded: Vec<i64> = vec![100, 10, 5, -3, 10];

        // --- Test Encode ---
        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes, 1).unwrap();

        // We still need this to verify the *encoded* data is correct.
        let encoded_slice = unsafe { safe_bytes_to_typed_slice::<i64>(&encoded_bytes).unwrap() };
        assert_eq!(encoded_slice, expected_encoded.as_slice());

        // --- Test Decode ---
        let mut decoded_bytes = Vec::new();
        // Call decode with the raw `encoded_bytes` Vec<u8>
        // We must specify the type `<i64>` because it can't be inferred from `&[u8]`
        decode::<i64>(&encoded_bytes, &mut decoded_bytes, 1).unwrap();

        let original_as_bytes = typed_slice_to_bytes(&original);
        assert_eq!(decoded_bytes, original_as_bytes);
    }

    #[test]
    fn test_core_inplace_logic() {
        let original = vec![10, 20, 15, 28, 25];
        let mut buffer = original.clone();

        encode_slice_inplace(&mut buffer, 2);
        assert_eq!(buffer, vec![10, 20, 5, 8, 10]);

        decode_slice_inplace(&mut buffer, 2);
        assert_eq!(buffer, original);
    }

    #[test]
    fn test_delta_roundtrip_f64() {
        let original: Vec<f64> = vec![10.5, 12.0, 11.5, 11.5, 15.0];
        let expected_encoded: Vec<f64> = vec![10.5, 1.5, -0.5, 0.0, 3.5];

        // --- Test Encode ---
        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes, 1).unwrap();

        let encoded_slice = unsafe { safe_bytes_to_typed_slice::<f64>(&encoded_bytes).unwrap() };
        encoded_slice
            .iter()
            .zip(expected_encoded.iter())
            .for_each(|(a, b)| assert!((a - b).abs() < 1e-9));

        // --- Test Decode ---
        let mut decoded_bytes = Vec::new();
        // Call decode with the raw `encoded_bytes` Vec<u8>.
        // We must specify the type `<f64>` because it can't be inferred from `&[u8]`.
        decode::<f64>(&encoded_bytes, &mut decoded_bytes, 1).unwrap();

        let original_as_bytes = typed_slice_to_bytes(&original);
        assert_eq!(decoded_bytes, original_as_bytes);
    }
}
