//! This module contains the v4.1 "Master Planner" and "Data Preparer".
//!
//! Its primary responsibility is to analyze an incoming Arrow Array, create a
//! unified plan, prepare the initial byte streams (valid data, nulls),
//! call the pure byte-oriented Executor, and assemble the final artifact.

use arrow::array::{Array, PrimitiveArray};
use arrow::datatypes::*;
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

// --- create_plan and its helpers are unchanged, as their logic is sound ---
// ... (create_plan_for_integer_array, create_plan_for_float_array, etc. are here) ...
#[derive(Debug, Clone)]
struct StrategyResult {
    plan: String,
    cost: usize,
}
fn create_plan_for_integer_array<T>(array: &PrimitiveArray<T>) -> Result<String, PhoenixError>
where
    T: ArrowNumericType,
    T::Native: Pod + PrimInt + Zero,
{
    let original_type = T::DATA_TYPE.to_string();
    let null_plan_json = serde_json::to_string(&[
        serde_json::json!({"op": "rle"}),
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

    let mut final_plan: Vec<serde_json::Value> = vec![
        serde_json::json!({"op": "CanonicalizeZeros"}),
        serde_json::json!({
            "op": "BitCast",
            "from_type": &original_type,
            "to_type": integer_type_str
        }),
    ];
    let null_plan_json = serde_json::to_string(&[
        serde_json::json!({"op": "rle"}),
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

// --- NEW: Public-facing API functions now correctly implement the new contract ---

/// The public-facing compression orchestrator for a single array.
pub fn compress_chunk(array: &dyn Array) -> Result<Vec<u8>, PhoenixError> {
    // 1. The orchestrator creates the plan.
    let plan_json = create_plan(array)?;

    // 2. The orchestrator PREPARES the initial data streams.
    let mut initial_streams = HashMap::new();
    let mut main_data_bytes = Vec::new();

    if array.null_count() < array.len() {
        macro_rules! extract_valid_data {
            ($T:ty) => {{
                let primitive_array = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$T>>()
                    .ok_or_else(|| PhoenixError::InternalError("Array type mismatch".into()))?;
                let valid_data: Vec<<$T as ArrowNumericType>::Native> =
                    bitmap::strip_valid_data_to_vec(primitive_array);
                main_data_bytes = typed_slice_to_bytes(&valid_data);
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
            _ => return Err(PhoenixError::UnsupportedType(array.data_type().to_string())),
        }
    }
    initial_streams.insert("main".to_string(), main_data_bytes);

    if let Some(nulls) = array.nulls() {
        initial_streams.insert("null_mask".to_string(), nulls.buffer().as_slice().to_vec());
    }

    // 3. The orchestrator calls the PURE executor.
    let compressed_streams = executor::execute_plan(
        initial_streams,
        &plan_json,
        &array.data_type().to_string(),
        array.len(),
    )?;

    // 4. The orchestrator assembles the final artifact.
    let artifact = CompressedChunk {
        total_rows: array.len() as u64,
        original_type: array.data_type().to_string(),
        plan_json,
        compressed_streams,
    };
    artifact.to_bytes()
}

/// The public-facing decompression orchestrator for a single array.
pub fn decompress_chunk(bytes: &[u8]) -> Result<Box<dyn Array>, PhoenixError> {
    // 1. The executor does the heavy lifting of decompression, returning raw streams.
    let (mut decompressed_streams, final_type, total_rows) = executor::decompress_plan(bytes)?;

    // 2. The orchestrator is responsible for the final re-assembly into an Arrow Array.
    let main_data = decompressed_streams.remove("main").ok_or_else(|| {
        PhoenixError::InternalError("Decompression failed to produce a 'main' stream".into())
    })?;
    let null_mask = decompressed_streams.remove("null_mask");

    macro_rules! reapply_and_build {
        ($T:ty) => {{
            bitmap::reapply_bitmap::<$T>(main_data, null_mask, total_rows as usize)
                .map(|arr| Box::new(arr) as Box<dyn Array>)
        }};
    }

    match final_type.as_str() {
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
        _ => Err(PhoenixError::UnsupportedType(final_type)),
    }
}

/// Public-facing utility to inspect a compressed chunk.
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
