//! This module contains the v4.1 "Master Planner" and "Data Preparer".
//!
//! Its primary responsibility is to analyze an incoming Arrow Array, create a
//! unified plan, prepare the initial byte streams (valid data, nulls),
//! call the pure byte-oriented Executor, and assemble the final artifact.

use arrow::array::{Array, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::*;
use arrow::datatypes::{ArrowNumericType, ArrowPrimitiveType};
use bytemuck::Pod;
use num_traits::{Float, PrimInt, Zero};
use serde_json::Value;
use std::collections::HashMap;

use super::{executor, planner};
use crate::error::PhoenixError;
use crate::log_metric;
use crate::null_handling::bitmap;
use crate::pipeline::artifact::CompressedChunk;
use crate::utils::typed_slice_to_bytes;

#[derive(Debug, Clone)]
struct StrategyResult {
    plan: String,
    cost: usize,
}
//==================================================================================
// Orchestrator Helpers (Declarative Logic)
//==================================================================================

/// The Arrow-to-Byte-Stream Bridge.
///
/// Converts an Arrow Array into the initial set of raw byte streams required
/// by the executor. This is the primary function for decoupling the pipeline
/// from the Arrow format.
fn prepare_initial_streams(array: &dyn Array) -> Result<HashMap<String, Vec<u8>>, PhoenixError> {
    let mut streams = HashMap::new();

    // 1. Extract the validity bitmap, if it exists.
    if let Some(nulls) = array.nulls() {
        streams.insert("null_mask".to_string(), nulls.buffer().as_slice().to_vec());
    }

    // 2. Extract the dense, valid data and convert it to bytes.
    macro_rules! extract_valid_data {
        ($T:ty) => {{
            let primitive_array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<$T>>()
                .ok_or_else(|| {
                    PhoenixError::InternalError(
                        "Array type mismatch during data preparation".into(),
                    )
                })?;

            let valid_data_vec: Vec<<$T as ArrowPrimitiveType>::Native> =
                crate::null_handling::bitmap::strip_valid_data_to_vec(primitive_array);

            streams.insert(
                "main".to_string(),
                crate::utils::typed_slice_to_bytes(&valid_data_vec),
            );
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
        dt => {
            return Err(PhoenixError::UnsupportedType(format!(
                "Unsupported type for initial stream preparation: {}",
                dt
            )))
        }
    }

    Ok(streams)
}

/// Infers the final data type after a series of transformations. This is a pure,
/// declarative function that provides a single source of truth for type state.
pub(crate) fn infer_final_type(initial_type: &str, plan: &[Value]) -> Result<String, PhoenixError> {
    let mut current_type = initial_type.to_string();
    for op_config in plan {
        if let Some(op_name) = op_config["op"].as_str() {
            match op_name {
                "BitCast" => {
                    current_type = op_config["to_type"]
                        .as_str()
                        .ok_or_else(|| {
                            PhoenixError::InternalError(
                                "BitCast op in plan is missing 'to_type' field".into(),
                            )
                        })?
                        .to_string();
                }
                "zigzag" => {
                    current_type = current_type.replace("Int", "UInt");
                }
                _ => {} // Other ops don't change the type
            }
        }
    }
    Ok(current_type)
}

/// A helper to calculate the number of valid (non-null) rows by decompressing
/// the null mask if it exists.
fn calculate_num_valid_rows(
    artifact: &CompressedChunk,
    plan: &[Value],
) -> Result<usize, PhoenixError> {
    if let Some(compressed_nulls) = artifact.compressed_streams.get("null_mask") {
        let null_op = plan
            .iter()
            .find(|op| op["op"] == "ExtractNulls")
            .ok_or_else(|| {
                PhoenixError::InternalError(
                    "Artifact has nulls but plan is missing ExtractNulls op".into(),
                )
            })?;
        let null_pipeline = null_op["pipeline"].as_array().ok_or_else(|| {
            PhoenixError::InternalError("ExtractNulls op missing 'pipeline' field".into())
        })?;

        let validity_bytes = executor::execute_linear_decode_pipeline(
            compressed_nulls,
            "Boolean",
            null_pipeline,
            artifact.total_rows as usize,
        )?;
        let boolean_buffer =
            BooleanBuffer::new(validity_bytes.into(), 0, artifact.total_rows as usize);

        Ok(boolean_buffer.count_set_bits())
    } else {
        Ok(artifact.total_rows as usize)
    }
}

//
fn create_plan_for_integer_array<T>(array: &PrimitiveArray<T>) -> Result<String, PhoenixError>
where
    T: ArrowNumericType,
    T::Native: Pod + PrimInt + Zero,
{
    let original_type = T::DATA_TYPE.to_string();
    let null_plan_json = serde_json::to_string(&[
        // serde_json::json!({"op": "rle"}), // bitmap is already a compression plan
        serde_json::json!({"op": "zstd", "params": {"level": 19}}),
    ])
    .unwrap();
    let null_op = serde_json::json!({
        "op": "ExtractNulls",
        "output_stream_id": "null_mask",
        "pipeline": serde_json::from_str::<serde_json::Value>(&null_plan_json).unwrap(),
    });

    let valid_data: Vec<T::Native> = bitmap::strip_valid_data_to_vec(array);
    if valid_data.is_empty() {
        return Ok(serde_json::to_string(&[null_op]).unwrap());
    }

    let valid_data_bytes = typed_slice_to_bytes(&valid_data);
    let (dense_plan_json, dense_cost) = planner::plan_pipeline(&valid_data_bytes, &original_type)?;
    let dense_strategy = StrategyResult {
        plan: dense_plan_json,
        cost: dense_cost,
    };

    let sparse_strategy = evaluate_sparsity_strategy(&valid_data, &original_type)?;

    let mut final_plan: Vec<serde_json::Value> = vec![null_op];
    if let Some(sparse) = sparse_strategy {
        if sparse.cost < dense_strategy.cost {
            log_metric!(
                "event" = "strategy_chosen",
                "type" = original_type,
                "strategy" = "Sparse",
                "dense_cost" = dense_strategy.cost,
                "sparse_cost" = sparse.cost
            );
            final_plan.push(serde_json::from_str(&sparse.plan).unwrap());
            return Ok(serde_json::to_string(&final_plan).unwrap());
        }
    }

    log_metric!(
        "event" = "strategy_chosen",
        "type" = original_type,
        "strategy" = "Dense",
        "dense_cost" = dense_strategy.cost
    );
    let dense_plan_ops: Vec<serde_json::Value> =
        serde_json::from_str(&dense_strategy.plan).unwrap();
    final_plan.extend(dense_plan_ops);
    Ok(serde_json::to_string(&final_plan).unwrap())
}

fn create_plan_for_float_array<T, U>(array: &PrimitiveArray<T>) -> Result<String, PhoenixError>
where
    T: ArrowNumericType,
    T::Native: Pod + Float,
    U: Pod + PrimInt + Zero,
{
    let original_type = T::DATA_TYPE.to_string();
    let integer_type_str = if std::mem::size_of::<U>() == 4 {
        "UInt32"
    } else {
        "UInt64"
    };

    let mut final_plan: Vec<serde_json::Value> = vec![];

    final_plan.push(serde_json::json!({"op": "CanonicalizeZeros"}));

    // --- THIS IS THE ROBUST FIX ---
    // Manually construct the Value::Object to avoid any macro ambiguity.
    let mut bitcast_map = serde_json::Map::new();
    bitcast_map.insert("op".to_string(), Value::String("BitCast".to_string()));
    bitcast_map.insert(
        "from_type".to_string(),
        Value::String(original_type.clone()),
    );
    bitcast_map.insert(
        "to_type".to_string(),
        Value::String(integer_type_str.to_string()),
    );
    final_plan.push(Value::Object(bitcast_map));
    // --- END FIX ---

    let null_plan_json = serde_json::to_string(&[
        // serde_json::json!({"op": "rle"}), // bitmap is already a compression plan
        serde_json::json!({"op": "zstd", "params": {"level": 19}}),
    ])
    .unwrap();
    final_plan.push(serde_json::json!({
        "op": "ExtractNulls",
        "output_stream_id": "null_mask",
        "pipeline": serde_json::from_str::<serde_json::Value>(&null_plan_json).unwrap(),
    }));

    let mut valid_data_floats: Vec<T::Native> = bitmap::strip_valid_data_to_vec(array);
    if valid_data_floats.is_empty() {
        return Ok(serde_json::to_string(&final_plan).unwrap());
    }

    for val in &mut valid_data_floats {
        if val.is_sign_negative() && val.is_zero() {
            *val = T::Native::zero();
        }
    }
    let valid_data_ints: Vec<U> = bytemuck::cast_slice(&valid_data_floats).to_vec();

    let valid_data_bytes = typed_slice_to_bytes(&valid_data_ints);
    let (dense_plan_json, dense_cost) =
        planner::plan_pipeline(&valid_data_bytes, integer_type_str)?;
    let dense_strategy = StrategyResult {
        plan: dense_plan_json,
        cost: dense_cost,
    };

    let sparse_strategy = evaluate_sparsity_strategy(&valid_data_ints, integer_type_str)?;

    if let Some(sparse) = sparse_strategy {
        if sparse.cost < dense_strategy.cost {
            log_metric!(
                "event" = "strategy_chosen",
                "type" = original_type,
                "strategy" = "Sparse",
                "dense_cost" = dense_strategy.cost,
                "sparse_cost" = sparse.cost
            );
            final_plan.push(serde_json::from_str(&sparse.plan).unwrap());
            return Ok(serde_json::to_string(&final_plan).unwrap());
        }
    }

    log_metric!(
        "event" = "strategy_chosen",
        "type" = original_type,
        "strategy" = "Dense",
        "dense_cost" = dense_strategy.cost
    );
    let dense_plan_ops: Vec<serde_json::Value> =
        serde_json::from_str(&dense_strategy.plan).unwrap();
    final_plan.extend(dense_plan_ops);
    Ok(serde_json::to_string(&final_plan).unwrap())
}

fn evaluate_sparsity_strategy<T: Pod + PrimInt + Zero>(
    data: &[T],
    type_str: &str,
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
    let (values_plan, values_cost) = planner::plan_pipeline(&non_zero_bytes, type_str)?;

    let mask_plan_json = serde_json::to_string(&[
        serde_json::json!({"op": "rle"}),
        serde_json::json!({"op": "zstd", "params": {"level": 5}}),
    ])
    .unwrap();
    let mask_cost = data.len() / 8;

    let full_sparse_plan = serde_json::json!({
        "op": "Sparsify",
        "mask_stream_id": "sparsity_mask",
        "mask_pipeline": serde_json::from_str::<serde_json::Value>(&mask_plan_json).unwrap(),
        "values_pipeline": serde_json::from_str::<serde_json::Value>(&values_plan).unwrap()
    });

    Ok(Some(StrategyResult {
        plan: serde_json::to_string(&full_sparse_plan).unwrap(),
        cost: values_cost + mask_cost,
    }))
}

pub fn create_plan(array: &dyn Array) -> Result<String, PhoenixError> {
    macro_rules! dispatch {
        ($arr:expr, $T:ty, $worker:ident) => {
            $worker($arr.as_any().downcast_ref::<PrimitiveArray<$T>>().unwrap())
        };
        ($arr:expr, $T:ty, $U:ty, $worker:ident) => {
            $worker::<$T, $U>($arr.as_any().downcast_ref::<PrimitiveArray<$T>>().unwrap())
        };
    }

    match array.data_type() {
        DataType::Int8 => dispatch!(array, Int8Type, create_plan_for_integer_array),
        DataType::Int16 => dispatch!(array, Int16Type, create_plan_for_integer_array),
        DataType::Int32 => dispatch!(array, Int32Type, create_plan_for_integer_array),
        DataType::Int64 => dispatch!(array, Int64Type, create_plan_for_integer_array),
        DataType::UInt8 => dispatch!(array, UInt8Type, create_plan_for_integer_array),
        DataType::UInt16 => dispatch!(array, UInt16Type, create_plan_for_integer_array),
        DataType::UInt32 => dispatch!(array, UInt32Type, create_plan_for_integer_array),
        DataType::UInt64 => dispatch!(array, UInt64Type, create_plan_for_integer_array),
        DataType::Float32 => dispatch!(array, Float32Type, u32, create_plan_for_float_array),
        DataType::Float64 => dispatch!(array, Float64Type, u64, create_plan_for_float_array),
        dt => Err(PhoenixError::UnsupportedType(format!(
            "Unsupported type for planning: {}",
            dt
        ))),
    }
}

//==================================================================================
// Public Orchestration API
//==================================================================================
// The orchestrator is now clean, readable, and follows a clear sequence.
pub fn compress_chunk(array: &dyn Array) -> Result<Vec<u8>, PhoenixError> {
    // 1. Plan
    let plan_json = create_plan(array)?;
    let plan: Vec<Value> = serde_json::from_str(&plan_json)?;
    // let final_type = infer_final_type(&array.data_type().to_string(), &plan)?; # we include this later

    // 2. Prepare
    let initial_streams = prepare_initial_streams(array)?;

    // 3. Execute
    let compressed_streams = executor::execute_plan(
        initial_streams,
        &plan_json,
        &array.data_type().to_string(),
        array.len(),
    )?;

    // 4. Assemble
    let artifact = CompressedChunk {
        total_rows: array.len() as u64,
        original_type: array.data_type().to_string(),
        plan_json,
        compressed_streams,
    };

    artifact.to_bytes()
}

pub fn decompress_chunk(bytes: &[u8]) -> Result<Box<dyn Array>, PhoenixError> {
    // 1. Deserialize the artifact to access its metadata.
    let artifact = CompressedChunk::from_bytes(bytes)?;
    let total_rows = artifact.total_rows as usize;
    let plan: Vec<Value> = serde_json::from_str(&artifact.plan_json)?;

    // 2. Determine the final Arrow type we need to build. This is our goal.
    let final_arrow_type = artifact.original_type.clone();

    // 3. Calculate the number of valid (non-null) rows that the main data pipeline needs to produce.
    let num_valid_rows = calculate_num_valid_rows(&artifact, &plan)?;

    // 4. Execute the full decompression plan. The executor will handle parsing the artifact
    //    and running the necessary pipelines.
    let (mut decompressed_streams, _, _) = executor::decompress_plan(bytes, num_valid_rows)?;

    // 5. Extract the decompressed data streams needed for final Arrow Array reconstruction.
    let main_data = decompressed_streams.remove("main").ok_or_else(|| {
        PhoenixError::InternalError("Decompression failed to produce a 'main' stream".into())
    })?;
    let null_mask_bytes = decompressed_streams.remove("null_mask");

    // 6. Convert the raw null mask bytes into an Arrow-compatible NullBuffer.
    let null_buffer = null_mask_bytes.map(|bytes| {
        let buffer = arrow::buffer::Buffer::from(bytes);
        let boolean_buffer = BooleanBuffer::new(buffer, 0, total_rows);
        NullBuffer::from(boolean_buffer)
    });

    // 7. Re-apply the nulls to the dense data and build the final, typed Arrow Array.
    //    This is the bridge from our pure byte world back to the Arrow ecosystem.
    macro_rules! reapply_and_build {
        ($T:ty) => {{
            bitmap::reapply_bitmap::<$T>(main_data, null_buffer, total_rows)
                .map(|arr| Box::new(arr) as Box<dyn Array>)
        }};
    }

    match final_arrow_type.as_str() {
        "Int8" => reapply_and_build!(Int8Type),
        "Int16" => reapply_and_build!(Int16Type),
        "Int32" => reapply_and_build!(Int32Type),
        "Int64" => reapply_and_build!(Int64Type),
        "UInt8" => reapply_and_build!(UInt8Type),
        "UInt16" => reapply_and_build!(UInt16Type),
        "UInt32" => reapply_and_build!(UInt32Type),
        "UInt64" => reapply_and_build!(UInt64Type),
        "Float32" => reapply_and_build!(Float32Type),
        "Float64" => reapply_and_build!(Float64Type),
        _ => Err(PhoenixError::UnsupportedType(final_arrow_type)),
    }
}

pub fn get_compressed_chunk_info(
    bytes: &[u8],
) -> Result<(usize, usize, String, String), PhoenixError> {
    let artifact = CompressedChunk::from_bytes(bytes)?;
    let data_size = artifact.compressed_streams.values().map(|v| v.len()).sum();
    let header_size = bytes.len() - data_size;
    Ok((
        header_size,
        data_size,
        artifact.plan_json,
        artifact.original_type,
    ))
}
