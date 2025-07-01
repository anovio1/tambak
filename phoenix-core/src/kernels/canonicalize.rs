//! This module contains the pure, stateless kernel for canonicalizing floating-point zeros.
//!
//! This is a Layer 0 (Normalization) transform. Its sole purpose is to convert
//! any negative zero (`-0.0`) values into positive zero (`+0.0`). This ensures
//! a consistent bit-pattern for zero, which is critical for subsequent sparsity
//! checks and compression stages. This module is PURE RUST and panic-free.

use crate::error::PhoenixError;
use bytemuck::Pod;
use num_traits::Float;

//==================================================================================
// 1. Public API (Generic, Performant, Decoupled)
//==================================================================================

/// The public-facing encode function. It finds and replaces `-0.0` with `+0.0`.
pub fn encode<T>(input_slice: &[T], output_buf: &mut Vec<u8>) -> Result<(), PhoenixError>
where
    T: Float + Pod,
{
    // Create a mutable copy to perform the transformation.
    let mut data = input_slice.to_vec();

    for val in &mut data {
        // The core logic: if a value is both negative and zero, it's -0.0.
        // Replace it with the additive identity (positive zero).
        if val.is_sign_negative() && val.is_zero() {
            *val = T::zero();
        }
    }

    // Write the potentially modified bytes to the output buffer.
    output_buf.clear();
    output_buf.extend_from_slice(bytemuck::cast_slice(&data));
    Ok(())
}

/// The public-facing decode function. This is a one-way transform, so decode is a no-op.
pub fn decode(input_bytes: &[u8], output_buf: &mut Vec<u8>) -> Result<(), PhoenixError> {
    // Data is passed through unmodified.
    output_buf.clear();
    output_buf.extend_from_slice(input_bytes);
    Ok(())
}

//==================================================================================
// 2. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::safe_bytes_to_typed_slice;

    #[test]
    fn test_canonicalize_f32() {
        let original: Vec<f32> = vec![1.0, -1.0, 0.0, -0.0, f32::NAN];
        let mut encoded_bytes = Vec::new();

        encode(&original, &mut encoded_bytes).unwrap();

        let result_slice = safe_bytes_to_typed_slice::<f32>(&encoded_bytes).unwrap();

        // Assert that the values are correct
        assert_eq!(result_slice[0], 1.0);
        assert_eq!(result_slice[1], -1.0);
        assert_eq!(result_slice[2], 0.0);
        assert_eq!(result_slice[3], 0.0);
        assert!(result_slice[4].is_nan());

        // Assert that the bit pattern for the canonicalized zero is correct
        assert_eq!(result_slice[3].to_bits(), (0.0f32).to_bits());
        // Assert that the bit pattern for the original negative zero was different
        assert_ne!((-0.0f32).to_bits(), (0.0f32).to_bits());
    }

    #[test]
    fn test_canonicalize_f64() {
        let original: Vec<f64> = vec![0.0, -0.0];
        let mut encoded_bytes = Vec::new();

        encode(&original, &mut encoded_bytes).unwrap();
        let result_slice = safe_bytes_to_typed_slice::<f64>(&encoded_bytes).unwrap();

        assert_eq!(result_slice[0], 0.0);
        assert_eq!(result_slice[1], 0.0);
        assert_eq!(result_slice[1].to_bits(), (0.0f64).to_bits());
    }

    #[test]
    fn test_decode_is_a_noop() {
        let original_bytes = vec![1, 2, 3, 4];
        let mut decoded_bytes = Vec::new();
        decode(&original_bytes, &mut decoded_bytes).unwrap();
        assert_eq!(original_bytes, decoded_bytes);
    }

    #[test]
    fn test_empty_slice() {
        let original: Vec<f32> = vec![];
        let mut encoded_bytes = Vec::new();
        encode(&original, &mut encoded_bytes).unwrap();
        assert!(encoded_bytes.is_empty());
    }
}