use arrow::array::{Array, BooleanArray, PrimitiveArray};
use arrow::buffer::{BooleanBuffer};
use arrow::datatypes::*;
use bytemuck::Pod;
use num_traits::{PrimInt, Zero};
use std::collections::HashMap;

use crate::pipeline::{executor, planner};
use crate::error::PhoenixError;
use crate::null_handling::bitmap;
use crate::pipeline::models::{Operation};
use crate::pipeline::planner::PlanningContext;
use crate::pipeline::traits::{StreamTransform};
use crate::pipeline::OperationBehavior;
use crate::types::PhoenixDataType;
use crate::utils::typed_slice_to_bytes;

#[derive(Debug, Clone)]
pub struct StrategyResult {
    pub(crate) plan: Vec<Operation>,
    pub(crate) cost: usize,
}

//==============================================================================
// 1. Helper Functions
//==============================================================================

/// possibly rename this to `decompress_mask_stream` or `decompress_boolean_mask`
pub fn decompress_meta_stream(
    meta_op_info: Option<(&str, &Vec<Operation>)>,
    streams: &mut HashMap<String, Vec<u8>>,
    num_values: usize,
    initial_type: PhoenixDataType,
) -> Result<Option<Vec<u8>>, PhoenixError> {
    if let Some((stream_id, pipeline)) = meta_op_info {
        if let Some(compressed_bytes) = streams.remove(stream_id) {
            let mut type_flow = derive_forward_type_flow(initial_type, pipeline)?;
            type_flow.reverse();
            let decompressed_bytes = executor::execute_linear_decode_pipeline(
                &compressed_bytes,
                &mut type_flow,
                pipeline,
                num_values,
            )?;
            Ok(Some(decompressed_bytes))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

//  MOVED TO BRIDGE::arrow_impl
// /// DATA MARSHALLING
// /// Prepares the initial byte streams for compression from an Arrow array.
// /// 
// /// This function extracts two kinds of streams:
// /// 1. **Null mask**: If the array contains null values, it unpacks the null bitmap
// ///    into a plain vector of bytes where each byte is `0` (null) or `1` (valid).
// /// 2. **Main data**: The non-null values themselves, extracted as contiguous bytes,
// ///    discarding the null slots. For primitive types, this is a packed buffer of values.
// ///    For booleans, it becomes a vector of `0`/`1` bytes.
// ///
// /// The result is a `HashMap` with at least a `"main"` stream, and optionally a `"null_mask"`
// /// stream if the input has nulls.
// ///
// /// # Arguments
// ///
// /// * `array` - A reference to a dynamically typed Arrow array.
// ///
// /// # Returns
// ///
// /// * `Ok(HashMap)` with stream name → raw bytes, or
// /// * `Err(PhoenixError)` if the type is unsupported.
// ///
// /// This is usually the first step of your compression pipeline, transforming Arrow's
// /// internal representation into a set of flat byte streams suitable for further encoding.
pub fn prepare_initial_streams_deprecated_soon(array: &dyn Array) -> Result<HashMap<String, Vec<u8>>, PhoenixError> {
    let mut streams = HashMap::new();

    // If the array contains nulls, extract its null bitmap and unpack it
    // into a plain Vec<u8> of 0s and 1s, where 1 means "valid" and 0 means "null".
    if let Some(nulls) = array.nulls() {
        // Arrow stores null bitmaps compacted (1 bit per value). Here we unpack them.
        // --- FIX: Unpack the bitmap to match the planner's view of the data ---
        let boolean_buffer =
            BooleanBuffer::new(nulls.buffer().clone(), nulls.offset(), nulls.len());
        let unpacked_mask_bytes: Vec<u8> = boolean_buffer.iter().map(|b| b as u8).collect();
        streams.insert("null_mask".to_string(), unpacked_mask_bytes);
    }

    // Helper macro: extracts the non-null values for primitive types (e.g., Int32, Float64),
    // strips out the nulls, and converts the data into raw bytes.
    macro_rules! extract_valid_data {
        ($T:ty) => {{
            let primitive_array = array.as_any().downcast_ref::<PrimitiveArray<$T>>().unwrap();
            let valid_data_vec = bitmap::strip_valid_data_to_vec(primitive_array);
            streams.insert("main".to_string(), typed_slice_to_bytes(&valid_data_vec));
        }};
    }

    match array.data_type() {
        // For each supported primitive type, call the macro to extract packed data.
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
        // Special case: Booleans are stored bit-packed, so unpack them and store as bytes.
        DataType::Boolean => {
            let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            let valid_bools = bitmap::strip_valid_bools_to_vec(bool_array);
            let valid_bytes: Vec<u8> = valid_bools.iter().map(|&b| b as u8).collect();
            streams.insert("main".to_string(), valid_bytes);
        }
        // Anything else: return an error indicating this type isn't supported.
        dt => {
            return Err(PhoenixError::UnsupportedType(format!(
                "Unsupported type for preparation: {}",
                dt
            )))
        }
    }
    Ok(streams)
}

pub fn evaluate_sparsity_strategy<T: Pod + PrimInt + Zero>(
    data: &[T],
    context: &PlanningContext,
) -> Result<Option<StrategyResult>, PhoenixError> {
    const SPARSITY_THRESHOLD_RATIO: f32 = 0.4;
    let zero_count = data.iter().filter(|&&v| v.is_zero()).count();
    if data.is_empty() || (zero_count as f32 / data.len() as f32) < SPARSITY_THRESHOLD_RATIO {
        return Ok(None);
    }

    let non_zero_values: Vec<T> = data.iter().filter(|&&v| !v.is_zero()).cloned().collect();
    if non_zero_values.is_empty() {
        return Ok(None);
    }

    let non_zero_bytes = typed_slice_to_bytes(&non_zero_values);
    let values_plan = planner::plan_pipeline(&non_zero_bytes, context.clone())?;
    let values_cost = executor::execute_linear_encode_pipeline(
        &non_zero_bytes,
        context.physical_dtype,
        &values_plan.pipeline,
    )?
    .len();

    //======================================================================
    // --- ARCHITECTURAL FIX: RECURSIVE EMPIRICAL COST MODEL ---
    //======================================================================
    // We now recursively call the main planner on the generated boolean mask
    // to find its truly optimal pipeline and cost.
    let mask_vec: Vec<bool> = data.iter().map(|v| !v.is_zero()).collect();
    let mask_bytes: Vec<u8> = mask_vec.iter().map(|&b| b as u8).collect();

    // 1. Create a new context specifically for the boolean mask stream.
    let mask_context = PlanningContext {
        initial_dtype: PhoenixDataType::Boolean,
        physical_dtype: PhoenixDataType::Boolean,
    };

    // 2. Recursively call the planner to get the best pipeline for the mask.
    let mask_plan = planner::plan_pipeline(&mask_bytes, mask_context)?;

    // 3. Empirically find its cost by executing that optimal plan.
    let mask_cost = executor::execute_linear_encode_pipeline(
        &mask_bytes,
        PhoenixDataType::Boolean, // The physical type is Boolean
        &mask_plan.pipeline,
    )?
    .len();
    //======================================================================
    // --- END ARCHITECTURAL FIX ---
    //======================================================================

    let sparse_op = Operation::Sparsify {
        mask_stream_id: "sparsity_mask".to_string(),
        mask_pipeline: mask_plan.pipeline, // Use the optimal plan we just found
        values_pipeline: values_plan.pipeline,
    };

    Ok(Some(StrategyResult {
        plan: vec![sparse_op],
        cost: values_cost + mask_cost,
    }))
}


/// Derives the full forward type flow for a pipeline. This is the single
/// source of truth for type state during decompression.
pub fn derive_forward_type_flow(
    initial_type: PhoenixDataType,
    pipeline: &[Operation],
) -> Result<Vec<PhoenixDataType>, PhoenixError> {
    let mut type_flow = Vec::with_capacity(pipeline.len() + 1);
    type_flow.push(initial_type);
    let mut current_type = initial_type;

    for op in pipeline {
        let transform = op.transform_stream(current_type)?;
        current_type = match transform {
            StreamTransform::PreserveType => current_type,
            StreamTransform::TypeChange(new_type) => new_type,
            StreamTransform::Restructure {
                primary_output_type,
                ..
            } => primary_output_type,
        };
        type_flow.push(current_type);
    }
    Ok(type_flow)
}
