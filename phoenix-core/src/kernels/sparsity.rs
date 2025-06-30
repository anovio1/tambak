//! This module contains the pure, stateless kernels for handling sparse data.
//!
//! This is a Layer 2 (Sparsity Exploitation) transform. It is not a standalone
//! compression algorithm, but a pre-processing step that splits a sparse stream
//! (containing many zeros) into a boolean mask and a dense data stream of only
//! the non-zero values. Each of these two output streams can then be compressed
//! much more effectively by subsequent pipeline stages.

use crate::error::PhoenixError;
use num_traits::{PrimInt, Zero};

/// Splits a sparse data stream into a boolean mask and a dense data vector.
///
/// # Returns
/// A tuple containing:
/// 1.  A `Vec<bool>` where `true` indicates a non-zero value.
/// 2.  A `Vec<T>` containing only the non-zero values from the input.
pub fn split_stream<T>(data: &[T]) -> Result<(Vec<bool>, Vec<T>), PhoenixError>
where
    T: PrimInt + Zero,
{
    let mut mask = Vec::with_capacity(data.len());
    let mut dense_data = Vec::with_capacity(data.len() / 2); // Pre-allocate assuming 50% sparsity

    for &value in data {
        if !value.is_zero() {
            mask.push(true);
            dense_data.push(value);
        } else {
            mask.push(false);
        }
    }
    Ok((mask, dense_data))
}

/// Reconstructs an original sparse stream from a boolean mask and a dense data vector.
///
/// # Args
/// * `mask`: The boolean mask where `true` indicates a position to insert a value.
/// * `dense_data`: The stream of non-zero values.
/// * `total_len`: The expected total length of the final reconstructed vector.
///
/// # Returns
/// The reconstructed sparse `Vec<T>`.
pub fn reconstruct_stream<T>(
    mask: &[bool],
    dense_data: &[T],
    total_len: usize,
) -> Result<Vec<T>, PhoenixError>
where
    T: PrimInt + Zero,
{
    if mask.len() != total_len {
        return Err(PhoenixError::SparsityError(format!(
            "Mask length ({}) does not match expected total length ({})",
            mask.len(),
            total_len
        )));
    }

    let mut reconstructed = Vec::with_capacity(total_len);
    let mut dense_iter = dense_data.iter();

    for &is_valid in mask {
        if is_valid {
            if let Some(&value) = dense_iter.next() {
                reconstructed.push(value);
            } else {
                return Err(PhoenixError::SparsityError(
                    "Not enough values in dense data stream to satisfy mask".to_string(),
                ));
            }
        } else {
            reconstructed.push(T::zero());
        }
    }

    if dense_iter.next().is_some() {
        return Err(PhoenixError::SparsityError(
            "Trailing data left in dense stream after mask was exhausted".to_string(),
        ));
    }

    Ok(reconstructed)
}

//==================================================================================
// Unit Tests
//==================================================================================
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sparsity_roundtrip() {
        let original: Vec<i32> = vec![0, 0, 100, 0, -50, 0, 0, 0, 200];
        let total_len = original.len();

        // 1. Split the stream
        let (mask, dense_data) = split_stream(&original).unwrap();

        // Verify the intermediate representation
        assert_eq!(
            mask,
            vec![false, false, true, false, true, false, false, false, true]
        );
        assert_eq!(dense_data, vec![100, -50, 200]);

        // 2. Reconstruct the stream
        let reconstructed = reconstruct_stream(&mask, &dense_data, total_len).unwrap();

        // Verify the final result
        assert_eq!(original, reconstructed);
    }

    #[test]
    fn test_reconstruct_errors() {
        // Error on not enough dense data
        let mask = vec![true, false, true];
        let dense_data = vec![100];
        let result = reconstruct_stream::<i32>(&mask, &dense_data, 3);
        assert!(matches!(result, Err(PhoenixError::SparsityError(_))));

        // Error on too much dense data
        let mask = vec![true, false, false];
        let dense_data = vec![100, 200];
        let result = reconstruct_stream::<i32>(&mask, &dense_data, 3);
        assert!(matches!(result, Err(PhoenixError::SparsityError(_))));
    }
}