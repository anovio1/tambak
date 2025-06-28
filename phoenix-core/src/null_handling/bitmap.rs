// --- IN: src/null_handling/bitmap.rs ---

//! This module contains pure, stateless, and high-performance kernels for handling
//! nullability, built correctly for the Arrow v55 API. It uses generics and the
//! official `NullBuffer` and non-generic `Buffer` types.

use arrow::array::{ArrayData, PrimitiveArray, PrimitiveBuilder};
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

pub fn strip_valid_data_to_vec<T: ArrowNumericType>(
    array: &PrimitiveArray<T>,
) -> Vec<T::Native> {
    // The iterator on PrimitiveArray yields Option<T::Native>.
    // filter_map is the most idiomatic and efficient way to collect non-null values.
    array.iter().filter_map(|val| val).collect()
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
    valid_data: Vec<T::Native>,
    null_buffer: Option<NullBuffer>,
    num_rows: usize, // The total number of rows in the final array is required.
) -> Result<PrimitiveArray<T>, PhoenixError> {
    // The PrimitiveBuilder is the correct, idiomatic, and safe way to build
    // an array from sparse data and a validity mask.
    let mut builder = PrimitiveBuilder::<T>::with_capacity(num_rows);

    if let Some(nb) = null_buffer {
        let mut valid_data_iter = valid_data.iter();
        // Iterate through the boolean validity iterator provided by the NullBuffer.
        for is_valid in nb.iter() {
            if is_valid {
                // The orchestrator guarantees that the number of `true` values in the
                // bitmap matches the length of `valid_data`, so this unwrap is safe.
                builder.append_value(*valid_data_iter.next().unwrap());
            } else {
                // The builder correctly handles appending a null slot.
                builder.append_null();
            }
        }
    } else {
        // If there are no nulls, we can just append all the values.
        for val in valid_data {
            builder.append_value(val);
        }
    }

    // The finish() method constructs the final, correct PrimitiveArray.
    Ok(builder.finish())
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
    fn test_strip_valid_data_to_vec() {
        let source_array = Int32Array::from(vec![Some(10), None, Some(30)]);
        let valid_data_vec = strip_valid_data_to_vec::<Int32Type>(&source_array);
        assert_eq!(valid_data_vec, vec![10, 30]);
    }

    #[test]
    fn test_reapply_bitmap_with_nulls() {
        let valid_data: Vec<i32> = vec![10, 30];
        let null_buffer = NullBuffer::from(vec![true, false, true]);
        let total_rows = 3;

        // Reconstruct the array using the correct builder-based function.
        let reconstructed_array = reapply_bitmap::<Int32Type>(
            valid_data,
            Some(null_buffer),
            total_rows,
        ).unwrap();

        let expected_array = Int32Array::from(vec![Some(10), None, Some(30)]);
        assert_eq!(reconstructed_array, expected_array);
    }

    #[test]
    fn test_reapply_bitmap_no_nulls() {
        let valid_data: Vec<i32> = vec![10, 20, 30];
        let total_rows = 3;

        let reconstructed_array = reapply_bitmap::<Int32Type>(
            valid_data,
            None, // No nulls
            total_rows,
        ).unwrap();

        let expected_array = Int32Array::from(vec![10, 20, 30]);
        assert_eq!(reconstructed_array, expected_array);
    }
}