// --- IN: src/null_handling/bitmap.rs ---

//! This module contains pure, stateless, and high-performance kernels for handling
//! nullability, built correctly for the Arrow v55 API. It uses generics and the
//! official `NullBuffer` and non-generic `Buffer` types.

use arrow::array::{ArrayBuilder, ArrayData, PrimitiveArray, PrimitiveBuilder};
use arrow::buffer::{Buffer, NullBuffer, ScalarBuffer};
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
pub fn strip_valid_data_to_buffer<T: ArrowNumericType>(array: &PrimitiveArray<T>) -> Buffer
where
    T::Native: ToByteSlice, // Ensure the native type can be viewed as bytes
{
    // 1. Collect valid typed data. filter_map is the most efficient way.
    let valid_data: Vec<T::Native> = array.iter().filter_map(|val| val).collect();

    // 2. Create a byte Buffer from the typed Vec.
    // `Buffer::from_vec` takes a `Vec<u8>`.
    Buffer::from(valid_data.to_byte_slice())
}

pub fn strip_valid_data_to_vec<T: ArrowNumericType>(array: &PrimitiveArray<T>) -> Vec<T::Native> {
    // The iterator on PrimitiveArray yields Option<T::Native>.
    // filter_map is the most idiomatic and efficient way to collect non-null values.
    array.iter().filter_map(|val| val).collect()
}

/// Re-applies a `NullBuffer` to a byte `Buffer` of valid data to reconstruct
/// the original Arrow `PrimitiveArray`.
/// This is the primary helper for the DENSE data path.
pub fn reapply_bitmap<T: ArrowNumericType>(
    valid_data_bytes: Vec<u8>,
    null_buffer: Option<NullBuffer>,
    num_rows: usize,
) -> Result<PrimitiveArray<T>, PhoenixError>
where
    T::Native: bytemuck::Pod,
{
    // THIS IS THE FIX for empty/all-null inputs.
    if valid_data_bytes.is_empty() {
        let mut builder = PrimitiveBuilder::<T>::with_capacity(num_rows);
        for _ in 0..num_rows {
            builder.append_null();
        }
        return Ok(builder.finish());
    }
    let valid_data: Vec<T::Native> = bytemuck::cast_slice(&valid_data_bytes).to_vec();
    reapply_bitmap_from_vec(valid_data, null_buffer, num_rows)
}

/// Re-applies a `NullBuffer` to an already-reconstructed vector of typed data.
pub fn reapply_bitmap_from_vec<T: ArrowNumericType>(
    data_vec: Vec<T::Native>, // This should be the DENSE data (only non-null values)
    null_buffer: Option<NullBuffer>,
    num_rows: usize,
) -> Result<PrimitiveArray<T>, PhoenixError> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(num_rows);

    if let Some(nb) = null_buffer {
        let mut data_iter = data_vec.iter();
        for _ in 0..num_rows {
            // THIS IS THE FIX: Check the validity bit FIRST.
            if nb.is_valid(builder.len()) {
                if let Some(&value) = data_iter.next() {
                    builder.append_value(value);
                } else {
                    return Err(PhoenixError::InternalError(
                        "Null bitmap indicates more valid values than data provided".to_string(),
                    ));
                }
            } else {
                builder.append_null();
            }
        }
    } else {
        if data_vec.len() != num_rows {
            return Err(PhoenixError::InternalError(format!(
                "Data vector length ({}) does not match total rows ({}) when no null buffer is present",
                data_vec.len(), num_rows
            )));
        }
        for &val in &data_vec {
            builder.append_value(val);
        }
    }

    Ok(builder.finish())
}

//==================================================================================
// 2. Unit Tests (REVISED - Reflecting the correct Buffer API)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::typed_slice_to_bytes; // <-- Import the utility
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

        // --- THIS IS THE FIX ---
        // Convert the typed Vec<i32> to a byte Vec<u8> before calling the function.
        let valid_data_bytes = typed_slice_to_bytes(&valid_data);

        let reconstructed_array = reapply_bitmap::<Int32Type>(
            valid_data_bytes, // Pass the correct byte vector
            Some(null_buffer),
            total_rows,
        )
        .unwrap();

        let expected_array = Int32Array::from(vec![Some(10), None, Some(30)]);
        assert_eq!(reconstructed_array, expected_array);
    }

    #[test]
    fn test_reapply_bitmap_no_nulls() {
        let valid_data: Vec<i32> = vec![10, 20, 30];
        let total_rows = 3;

        // --- THIS IS THE FIX ---
        // Convert the typed Vec<i32> to a byte Vec<u8>.
        let valid_data_bytes = typed_slice_to_bytes(&valid_data);

        let reconstructed_array = reapply_bitmap::<Int32Type>(
            valid_data_bytes, // Pass the correct byte vector
            None,             // No nulls
            total_rows,
        )
        .unwrap();

        let expected_array = Int32Array::from(vec![10, 20, 30]);
        assert_eq!(reconstructed_array, expected_array);
    }
    #[test]
    fn test_reapply_bitmap_from_vec_with_nulls() {
        // THIS IS THE FIX: Pass the DENSE data, not the sparse data.
        let dense_vec: Vec<i32> = vec![10, 30, 50];
        let null_buffer = NullBuffer::from(vec![true, false, true, false, true]);
        let total_rows = 5;

        let reconstructed_array = reapply_bitmap_from_vec::<Int32Type>(
            dense_vec, // Pass the dense vector
            Some(null_buffer),
            total_rows,
        )
        .unwrap();

        let expected_array = Int32Array::from(vec![Some(10), None, Some(30), None, Some(50)]);
        assert_eq!(reconstructed_array, expected_array);
    }

    #[test]
    fn test_reapply_bitmap_from_vec_no_nulls() {
        let data_vec: Vec<i32> = vec![10, 20, 30];
        let total_rows = 3;

        let reconstructed_array = reapply_bitmap_from_vec::<Int32Type>(
            data_vec, None, // No nulls
            total_rows,
        )
        .unwrap();

        let expected_array = Int32Array::from(vec![10, 20, 30]);
        assert_eq!(reconstructed_array, expected_array);
    }
}
