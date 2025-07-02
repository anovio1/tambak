// --- IN: src/null_handling/bitmap.rs ---

//! This module contains pure, stateless, and high-performance kernels for handling
//! nullability, built correctly for the Arrow v55 API. It uses generics and the
//! official `NullBuffer` and non-generic `Buffer` types.

use arrow::array::{
    ArrayBuilder, ArrayData, BooleanArray, BooleanBuilder, PrimitiveArray, PrimitiveBuilder,
};
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

/// Extracts the valid data from a `PrimitiveArray` into a new, dense `Vec`.
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

/// Re-applies a `NullBuffer` to a byte buffer of valid booleans to reconstruct
/// the original Arrow `BooleanArray`.
///
/// This is a specialized version of `reapply_bitmap` because `BooleanArray` is not
/// a generic `PrimitiveArray` and `BooleanType` does not implement `ArrowNumericType`.
pub fn reapply_bitmap_for_bools(
    valid_data_bytes: Vec<u8>,
    null_buffer: Option<NullBuffer>,
    num_rows: usize,
) -> Result<BooleanArray, PhoenixError> {
    // First, convert the dense byte stream back into a vector of booleans.
    let valid_bools: Vec<bool> = valid_data_bytes.iter().map(|&b| b != 0).collect();

    // Handle the all-null case, where the valid data stream is empty.
    if valid_bools.is_empty() && null_buffer.is_some() {
        let mut builder = BooleanBuilder::with_capacity(num_rows);
        for _ in 0..num_rows {
            builder.append_null();
        }
        return Ok(builder.finish());
    }

    let mut builder = BooleanBuilder::with_capacity(num_rows);
    let mut data_iter = valid_bools.iter();

    if let Some(nb) = null_buffer {
        for i in 0..num_rows {
            if nb.is_valid(i) {
                if let Some(&value) = data_iter.next() {
                    builder.append_value(value);
                } else {
                    return Err(PhoenixError::InternalError(
                        "Null bitmap indicates more valid values than boolean data provided"
                            .to_string(),
                    ));
                }
            } else {
                builder.append_null();
            }
        }
    } else {
        // No nulls, just append all values.
        if valid_bools.len() != num_rows {
            return Err(PhoenixError::InternalError(format!(
                "Boolean data vector length ({}) does not match total rows ({}) when no null buffer is present",
                valid_bools.len(), num_rows
            )));
        }
        for &val in &valid_bools {
            builder.append_value(val);
        }
    }

    Ok(builder.finish())
}

/// Extracts the valid boolean values from a `BooleanArray` into a new, dense `Vec<bool>`.
///
/// This is a specialized version of `strip_valid_data_to_vec` because `BooleanArray`
/// is not a generic `PrimitiveArray` and `BooleanType` does not implement `ArrowNumericType`.
pub fn strip_valid_bools_to_vec(array: &BooleanArray) -> Vec<bool> {
    // The iterator on BooleanArray yields Option<bool>, so the logic is identical.
    array.iter().filter_map(|val| val).collect()
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
