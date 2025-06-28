// --- IN: src/null_handling/bitmap.rs ---

//! This module contains pure, stateless, and high-performance kernels for handling
//! nullability, built correctly for the Arrow v55 API. It uses generics and the
//! official `NullBuffer` and non-generic `Buffer` types.

use arrow::array::{PrimitiveArray, ArrayData};
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{ArrowNumericType, ToByteSlice}; // ToByteSlice is needed for tests

use crate::error::PhoenixError;

//==================================================================================
// 1. Generic Core Logic (CORRECTED based on Buffer being non-generic)
//==================================================================================

/// Extracts the valid data from a `PrimitiveArray` into a new, dense `Buffer`.
///
/// This function iterates through the array, collects the non-null values into a
/// `Vec<T::Native>`, and then creates a byte `Buffer` from that vector.
///
/// # Args
/// * `array`: A reference to an Arrow `PrimitiveArray<T>`.
///
/// # Returns
/// A byte `Buffer` containing only the valid data values.
pub fn strip_valid_data_to_buffer<T: ArrowNumericType>(
    array: &PrimitiveArray<T>,
) -> Buffer
where
    T::Native: ToByteSlice, // Ensure the native type can be viewed as bytes
{
    // 1. Collect valid typed data. filter_map is the most efficient way.
    let valid_data: Vec<T::Native> = array.iter().filter_map(|val| val).collect();

    // 2. Create a byte Buffer from the typed Vec.
    // `Buffer::from_vec` takes a `Vec<u8>`.
    Buffer::from(valid_data.to_byte_slice())
}

/// Re-applies a `NullBuffer` to a byte `Buffer` of valid data to reconstruct
/// the original Arrow `PrimitiveArray`. This is a zero-copy operation.
///
/// # Args
/// * `valid_data_buffer`: A non-generic Arrow `Buffer` containing the contiguous valid data as bytes.
/// * `null_buffer`: An `Option<NullBuffer>`. `None` signifies an array with no nulls.
///
/// # Returns
/// A new Arrow `PrimitiveArray<T>` with nulls correctly reinstated.
pub fn reapply_bitmap<T: ArrowNumericType>(
    valid_data_buffer: Buffer, // CORRECT: Buffer is not generic
    null_buffer: Option<NullBuffer>,
) -> Result<PrimitiveArray<T>, PhoenixError> {
    // Determine the total number of elements in the final array.
    let len = if let Some(nb) = &null_buffer {
        nb.len()
    } else {
        // If no nulls, length is the number of elements in the data buffer.
        // We must divide byte length by element size.
        valid_data_buffer.len() / std::mem::size_of::<T::Native>()
    };

    let array_data = ArrayData::builder(T::DATA_TYPE)
        .len(len)
        .add_buffer(valid_data_buffer) // add_buffer correctly takes a non-generic Buffer
        .nulls(null_buffer)
        .build()
        .map_err(|e| PhoenixError::InternalError(format!("Failed to build ArrayData: {}", e)))?;

    Ok(PrimitiveArray::<T>::from(array_data))
}

//==================================================================================
// 2. Unit Tests (REVISED - Reflecting the correct Buffer API)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::Int32Type;

    #[test]
    fn test_strip_valid_data_to_buffer() {
        let source_array = Int32Array::from(vec![Some(10), None, Some(30)]);
        let expected_valid_data: Vec<i32> = vec![10, 30];

        // Call the function to get a raw byte buffer
        let buffer = strip_valid_data_to_buffer::<Int32Type>(&source_array);

        // The buffer's content should be the little-endian bytes of the expected data.
        assert_eq!(buffer.as_slice(), expected_valid_data.to_byte_slice());
    }

    #[test]
    fn test_reapply_bitmap_with_nulls() {
        let valid_data: Vec<i32> = vec![10, 30];
        // CORRECT: Create a byte Buffer from a typed slice.
        let valid_data_buffer = Buffer::from(valid_data.to_byte_slice());

        let null_buffer = NullBuffer::from(vec![true, false, true]);

        let reconstructed_array = reapply_bitmap::<Int32Type>(
            valid_data_buffer,
            Some(null_buffer),
        ).unwrap();

        let expected_array = Int32Array::from(vec![Some(10), None, Some(30)]);
        assert_eq!(reconstructed_array, expected_array);
    }

    #[test]
    fn test_reapply_bitmap_no_nulls() {
        let valid_data: Vec<i32> = vec![10, 20, 30];
        let valid_data_buffer = Buffer::from(valid_data.to_byte_slice());

        let reconstructed_array = reapply_bitmap::<Int32Type>(
            valid_data_buffer,
            None, // No nulls
        ).unwrap();

        let expected_array = Int32Array::from(vec![10, 20, 30]);
        assert_eq!(reconstructed_array, expected_array);
    }
}