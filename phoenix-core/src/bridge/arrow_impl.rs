// In: src/bridge/arrow_impl.rs

use arrow::array::{Array, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::*;
use std::collections::HashMap;

use crate::error::PhoenixError;
use crate::null_handling::bitmap;
use crate::pipeline::context::PipelineInput;
use crate::types::PhoenixDataType;
use crate::utils::typed_slice_to_bytes;

/// DATA MARSHALLING
/// Prepares the initial byte streams for compression from an Arrow array.
///
/// This function extracts two kinds of streams:
/// 1. **Null mask**: If the array contains null values, it unpacks the null bitmap
///    into a plain vector of bytes where each byte is `0` (null) or `1` (valid).
/// 2. **Main data**: The non-null values themselves, extracted as contiguous bytes,
///    discarding the null slots. For primitive types, this is a packed buffer of values.
///    For booleans, it becomes a vector of `0`/`1` bytes.
///
/// The result is a `HashMap` with at least a `"main"` stream, and optionally a `"null_mask"`
/// stream if the input has nulls.
///
/// # Arguments
///
/// * `array` - A reference to a dynamically typed Arrow array.
///
/// # Returns
///
/// * `Ok(HashMap)` with stream name → raw bytes, or
/// * `Err(PhoenixError)` if the type is unsupported.
///
/// This is usually the first step of your compression pipeline, transforming Arrow's
/// internal representation into a set of flat byte streams suitable for further encoding.
pub fn arrow_to_pipeline_input(array: &dyn Array) -> Result<PipelineInput, PhoenixError> {
    // If the array contains nulls, extract its null bitmap and unpack it
    // into a plain Vec<u8> of 0s and 1s, where 1 means "valid" and 0 means "null".
    let null_mask = array.nulls().map(|nulls| {
        let boolean_buffer =
            BooleanBuffer::new(nulls.buffer().clone(), nulls.offset(), nulls.len());
        boolean_buffer.iter().map(|b| b as u8).collect::<Vec<u8>>()
    });

    // Extract the validity-stripped main data buffer.
    let main_data = {
        macro_rules! extract_valid_data {
            ($T:ty) => {{
                let primitive_array = array.as_any().downcast_ref::<PrimitiveArray<$T>>().unwrap();
                let valid_data_vec = bitmap::strip_valid_data_to_vec(primitive_array);
                typed_slice_to_bytes(&valid_data_vec).to_vec()
            }};
        }

        match array.data_type() {
            DataType::Int8 => extract_valid_data!(Int8Type),
            DataType::Int16 => extract_valid_data!(Int16Type),
            DataType::Int32 => extract_valid_data!(Int32Type),
            DataType::Int64 => extract_valid_data!(Int64Type),
            DataType::UInt8 => extract_valid_data!(UInt8Type),
            DataType::UInt16 => extract_valid_data!(UInt16Type),
            DataType::UInt32 => extract_valid_data!(UInt32Type),
            DataType::UInt64 => extract_valid_data!(UInt64Type),
            DataType::Float32 => extract_valid_data!(Float32Type),
            DataType::Float64 => extract_valid_data!(Float64Type),
            DataType::Boolean => {
                let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                let valid_bools = bitmap::strip_valid_bools_to_vec(bool_array);
                valid_bools.iter().map(|&b| b as u8).collect()
            }
            dt => {
                return Err(PhoenixError::UnsupportedType(format!(
                    "Unsupported type: {}",
                    dt
                )))
            }
        }
    };

    // 3. Assemble the final, pure `PipelineInput` struct.
    Ok(PipelineInput::new(
        main_data, // The extracted main data Vec<u8>
        null_mask, // The extracted Option<Vec<u8>>
        PhoenixDataType::from_arrow_type(array.data_type())?,
        array.len(),
        array.len() - array.null_count(),
        None, // We don't have a column name when processing a single array
    ))
}

pub fn pipeline_output_to_array(output: PipelineInput) -> Result<Box<dyn Array>, PhoenixError> {
    let total_rows = output.total_rows;
    let main_data = output.main;
    let null_buffer = output.null_mask.map(|bytes| {
        let boolean_buffer = BooleanBuffer::from_iter(bytes.iter().map(|&b| b != 0));
        NullBuffer::from(boolean_buffer)
    });

    macro_rules! reapply_and_build {
        ($T:ty) => {
            bitmap::reapply_bitmap::<$T>(main_data, null_buffer, total_rows)
                .map(|arr| Box::new(arr) as Box<dyn Array>)
        };
    }

    match output.initial_dtype {
        PhoenixDataType::Int8 => reapply_and_build!(Int8Type),
        PhoenixDataType::Int16 => reapply_and_build!(Int16Type),
        PhoenixDataType::Int32 => reapply_and_build!(Int32Type),
        PhoenixDataType::Int64 => reapply_and_build!(Int64Type),
        PhoenixDataType::UInt8 => reapply_and_build!(UInt8Type),
        PhoenixDataType::UInt16 => reapply_and_build!(UInt16Type),
        PhoenixDataType::UInt32 => reapply_and_build!(UInt32Type),
        PhoenixDataType::UInt64 => reapply_and_build!(UInt64Type),
        PhoenixDataType::Float32 => reapply_and_build!(Float32Type),
        PhoenixDataType::Float64 => reapply_and_build!(Float64Type),
        PhoenixDataType::Boolean => {
            bitmap::reapply_bitmap_for_bools(main_data, null_buffer, total_rows)
                .map(|arr| Box::new(arr) as Box<dyn Array>)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;

    #[test]
    fn test_pipeline_output_to_array_no_nulls() {
        // Arrange: Create a pure PipelineInput representing [10, 20, 30]
        let main_data = vec![10i32, 20, 30];
        let input = PipelineInput::new(
            bytemuck::cast_slice(&main_data).to_vec(),
            None, // No null mask
            PhoenixDataType::Int32,
            3,
            3,
            None,
        );

        // Act
        let result_array = pipeline_output_to_array(input).unwrap();
        let result_i32_array = result_array.as_any().downcast_ref::<Int32Array>().unwrap();

        // Assert
        assert_eq!(result_i32_array.len(), 3);
        assert_eq!(result_i32_array.null_count(), 0);
        assert_eq!(result_i32_array.values(), &[10, 20, 30]);
    }

    #[test]
    fn test_pipeline_output_to_array_with_nulls() {
        // Arrange: Create a pure PipelineInput representing [Some(10), None, Some(30)]
        let main_data = vec![10i32, 30]; // Only valid data
        let null_mask = vec![1u8, 0, 1];   // 1=valid, 0=null
        let input = PipelineInput::new(
            bytemuck::cast_slice(&main_data).to_vec(),
            Some(null_mask),
            PhoenixDataType::Int32,
            3, // total rows
            2, // valid rows
            None,
        );

        // Act
        let result_array = pipeline_output_to_array(input).unwrap();
        let result_i32_array = result_array.as_any().downcast_ref::<Int32Array>().unwrap();

        // Assert
        let expected_array = Int32Array::from(vec![Some(10), None, Some(30)]);
        assert_eq!(result_i32_array, &expected_array);
    }
}