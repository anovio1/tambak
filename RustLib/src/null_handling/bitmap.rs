//! This module contains pure, stateless, and high-performance kernels for handling
//! nullability. Its primary responsibility is to efficiently separate and
//! reconstruct data based on an Arrow-compatible validity bitmap.
//! This module is completely decoupled from Polars and the FFI layer.

use arrow::array::PrimitiveArray;
use arrow::bitmap::Bitmap;
use arrow::buffer::Buffer;
use arrow::datatypes::ArrowNumericType;
use num_traits::PrimInt;

use crate::error::PhoenixError;

//==================================================================================
// 1. Generic Core Logic (REVISED - Simplified & Performant)
//==================================================================================

/// Extracts a contiguous vector of valid data from a slice, guided by a validity bitmap.
///
/// This function performs a single pass over the data, copying only the elements
/// marked as valid by the bitmap into a new, tightly packed vector. This is the
/// core kernel used by the orchestrator when nulls are present.
///
/// # Args
/// * `data`: A slice of the full data, including values at null positions.
/// * `validity`: An Arrow `Bitmap` describing the validity of `data`.
///
/// # Returns
/// A `Result` containing a `Vec<T>` of only the valid data values.
pub fn strip_valid_data<T: PrimInt>(
    data: &[T],
    validity: &Bitmap,
) -> Result<Vec<T>, PhoenixError> {
    if data.len() != validity.len() {
        return Err(PhoenixError::BufferMismatch(data.len(), validity.len()));
    }

    let num_valid = validity.true_count();
    let mut valid_data = Vec::with_capacity(num_valid);

    // Single pass: iterate through the data and only copy if valid.
    for (i, &val) in data.iter().enumerate() {
        // SAFETY: We've already checked that lengths match.
        if unsafe { validity.get_bit_unchecked(i) } {
            valid_data.push(val);
        }
    }
    Ok(valid_data)
}

/// Re-applies a validity bitmap to a stream of valid data to reconstruct
/// the original Arrow array with its nulls. This is a zero-copy operation.
///
/// This is the high-performance, "zero-copy" way to construct an array.
/// It creates an `ArrayData` struct from the buffers directly, without iterating
/// or using a builder.
///
/// # Args
/// * `valid_data_buffer`: An Arrow `Buffer` containing the contiguous valid data.
/// * `validity_bitmap`: An Arrow `Bitmap` describing the full array's validity.
///
/// # Returns
/// A new Arrow `PrimitiveArray` with nulls correctly reinstated.
pub fn reapply_bitmap<T: ArrowNumericType>(
    valid_data_buffer: Buffer<T::Native>,
    validity_bitmap: Bitmap,
) -> Result<PrimitiveArray<T>, PhoenixError> {
    let array_data = arrow::array::ArrayData::builder(T::DATA_TYPE)
        .len(validity_bitmap.len())
        .add_buffer(valid_data_buffer)
        .nulls(Some(validity_bitmap))
        .build()
        .map_err(|e| PhoenixError::UnsupportedType(e.to_string()))?;

    Ok(PrimitiveArray::<T>::from(array_data))
}

//==================================================================================
// 2. FFI Dispatcher Logic (REMOVED)
//==================================================================================
// This module is a pure kernel library. It does not contain any FFI dispatchers.
// The responsibility for converting Polars/Python objects and calling these
// pure functions lies within the `ffi.rs` and `orchestrator.rs` modules.

//==================================================================================
// 3. Unit Tests (REVISED - Tests now target the new, simpler API)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::buffer::Buffer;

    #[test]
    fn test_strip_valid_data_kernel() {
        let source_array = Int32Array::from(vec![Some(10), None, Some(20), Some(30), None]);
        let data_slice = source_array.values().as_slice();
        let validity_bitmap = source_array.validity().unwrap(); // We know it has nulls

        let valid_data_vec = strip_valid_data(data_slice, validity_bitmap).unwrap();

        assert_eq!(valid_data_vec, vec![10, 20, 30]);
    }

    #[test]
    fn test_reapply_bitmap_kernel() {
        let valid_data: Vec<i32> = vec![10, 20, 30];
        let valid_data_buffer = Buffer::from_vec(valid_data);
        
        // The bitmap for [Some, None, Some, Some, None]
        let validity_bitmap = Bitmap::from_iter(vec![true, false, true, true, false]);

        let reconstructed_array = reapply_bitmap::<arrow::datatypes::Int32Type>(
            valid_data_buffer,
            validity_bitmap,
        ).unwrap();

        let expected_array = Int32Array::from(vec![Some(10), None, Some(20), Some(30), None]);

        assert_eq!(reconstructed_array, expected_array);
    }

    #[test]
    fn test_strip_with_mismatched_len_errors() {
        let data_slice: &[i32] = &[10, 20, 30];
        let validity_bitmap = Bitmap::from_iter(vec![true, false]); // Length 2, not 3

        let result = strip_valid_data(data_slice, &validity_bitmap);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("BufferMismatch"));
        }
    }
}