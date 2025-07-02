//! This module contains the v4.3 "Master Planner" and "Data Preparer".
//!
//! Its primary responsibility is to analyze an incoming Arrow Array, create a
//! unified and strongly-typed `Plan`, prepare the initial byte streams,
//! orchestrate the `Executor` to run the plan (handling meta-ops like Sparsify),
//! and assemble the final compressed artifact.

use arrow::array::{Array, BooleanArray, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::*;
use bytemuck::Pod;
use num_traits::{PrimInt, Zero};
use std::collections::HashMap;

use super::{executor, planner};
use crate::error::PhoenixError;
use crate::kernels;
use crate::log_metric;
use crate::null_handling::bitmap;
use crate::pipeline::artifact::CompressedChunk;
use crate::pipeline::models::{Operation, Plan};
use crate::pipeline::planner::PlanningContext;
use crate::pipeline::traits::TypeTransformer;
use crate::types::PhoenixDataType;
use crate::utils::typed_slice_to_bytes;

const PLAN_VERSION: u32 = 2;

#[derive(Debug, Clone)]
struct StrategyResult {
    plan: Vec<Operation>,
    cost: usize,
}

//==============================================================================
// 1. Helper Functions
//==============================================================================

fn prepare_initial_streams(array: &dyn Array) -> Result<HashMap<String, Vec<u8>>, PhoenixError> {
    let mut streams = HashMap::new();
    if let Some(nulls) = array.nulls() {
        streams.insert("null_mask".to_string(), nulls.buffer().as_slice().to_vec());
    }

    macro_rules! extract_valid_data {
        ($T:ty) => {{
            let primitive_array = array.as_any().downcast_ref::<PrimitiveArray<$T>>().unwrap();
            let valid_data_vec = bitmap::strip_valid_data_to_vec(primitive_array);
            streams.insert("main".to_string(), typed_slice_to_bytes(&valid_data_vec));
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
            let valid_bytes: Vec<u8> = valid_bools.iter().map(|&b| b as u8).collect();
            streams.insert("main".to_string(), valid_bytes);
        }
        dt => {
            return Err(PhoenixError::UnsupportedType(format!(
                "Unsupported type for preparation: {}",
                dt
            )))
        }
    }
    Ok(streams)
}

fn evaluate_sparsity_strategy<T: Pod + PrimInt + Zero>(
    data: &[T],
    context: &PlanningContext, // MODIFIED: Pass context
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
    // MODIFIED: Pass the context to the planner
    let values_plan = planner::plan_pipeline(&non_zero_bytes, context.clone())?;
    let values_cost = executor::execute_linear_encode_pipeline(
        &non_zero_bytes,
        context.physical_dtype, // Use physical_dtype for execution
        &values_plan.pipeline,
    )?
    .len();

    let mask_pipeline = vec![Operation::Rle, Operation::Zstd { level: 5 }];
    let mask_cost = data.len() / 8;

    let sparse_op = Operation::Sparsify {
        mask_stream_id: "sparsity_mask".to_string(),
        mask_pipeline,
        values_pipeline: values_plan.pipeline,
    };

    Ok(Some(StrategyResult {
        plan: vec![sparse_op],
        cost: values_cost + mask_cost,
    }))
}

/// Derives the full forward type flow for a pipeline. This is the single
/// source of truth for type state during decompression.
fn derive_forward_type_flow(
    initial_type: PhoenixDataType,
    pipeline: &[Operation],
) -> Result<Vec<PhoenixDataType>, PhoenixError> {
    let mut type_flow = Vec::with_capacity(pipeline.len() + 1);
    type_flow.push(initial_type);
    let mut current_type = initial_type;

    for op in pipeline {
        current_type = op.transform_type(current_type)?;
        type_flow.push(current_type);
    }
    Ok(type_flow)
}

//==================================================================================
// 2. Core Planning and Orchestration Logic
//==================================================================================

pub fn create_plan(array: &dyn Array) -> Result<Plan, PhoenixError> {
    let initial_dtype = PhoenixDataType::from_arrow_type(array.data_type())?;
    let mut plan_pipeline: Vec<Operation> = Vec::new();
    let mut current_physical_type = initial_dtype;

    // 1. Perform all initial type transformations FIRST.
    if let PhoenixDataType::Float32 | PhoenixDataType::Float64 = initial_dtype {
        plan_pipeline.push(Operation::CanonicalizeZeros);
        let bitcast_op = Operation::BitCast {
            to_type: if initial_dtype == PhoenixDataType::Float32 {
                PhoenixDataType::UInt32
            } else {
                PhoenixDataType::UInt64
            },
        };
        // The physical type of the data stream changes after the bitcast.
        current_physical_type = bitcast_op.transform_type(current_physical_type)?;
        plan_pipeline.push(bitcast_op);
    }

    // 2. NOW that we have the final physical type, plan for null separation.
    // --- THIS IS YOUR CORRECT LOGIC, NOW IN THE RIGHT PLACE ---
    let null_mask_context = PlanningContext {
        initial_dtype: PhoenixDataType::Boolean,
        physical_dtype: PhoenixDataType::Boolean,
    };
    // Use a non-constant sample to ensure the planner makes a good choice.
    let sample_null_mask = vec![0u8, 1u8];
    let null_mask_plan = planner::plan_pipeline(&sample_null_mask, null_mask_context)?;

    plan_pipeline.push(Operation::ExtractNulls {
        output_stream_id: "null_mask".to_string(),
        null_mask_pipeline: null_mask_plan.pipeline, // Use the generated plan
    });

    // 3. Finally, plan for the dense, validity-stripped data stream.
    let valid_data_bytes = prepare_initial_streams(array)?
        .remove("main")
        .unwrap_or_default();
    if !valid_data_bytes.is_empty() {
        // --- THIS IS THE CORE CHANGE ---
        // 1. Construct the PlanningContext.
        let context = PlanningContext {
            initial_dtype,
            physical_dtype: current_physical_type,
        };

        #[cfg(debug_assertions)]
        println!(
            "[ORCH-PLAN] Planning with context -> initial: {:?}, physical: {:?}",
            context.initial_dtype, context.physical_dtype
        );

        // 2. Call the planner with the new context.
        let dense_plan = planner::plan_pipeline(&valid_data_bytes, context.clone())?;
        let dense_cost = executor::execute_linear_encode_pipeline(
            &valid_data_bytes,
            context.physical_dtype, // Use physical type for execution
            &dense_plan.pipeline,
        )?
        .len();
        let dense_strategy = StrategyResult {
            plan: dense_plan.pipeline,
            cost: dense_cost,
        };

        macro_rules! evaluate_and_append {
            ($T:ty) => {{
                let typed_slice: &[$T] = bytemuck::try_cast_slice(&valid_data_bytes)?;
                // Pass the context to the sparsity evaluator.
                if let Some(sparse_strategy) = evaluate_sparsity_strategy(typed_slice, &context)? {
                    if sparse_strategy.cost < dense_strategy.cost {
                        log_metric!(
                            "event" = "strategy_chosen",
                            "type" = &context.initial_dtype,
                            "strategy" = "Sparse",
                            "dense_cost" = &dense_strategy.cost,
                            "sparse_cost" = &sparse_strategy.cost
                        );
                        plan_pipeline.extend(sparse_strategy.plan);
                    } else {
                        log_metric!(
                            "event" = "strategy_chosen",
                            "type" = &context.initial_dtype,
                            "strategy" = "Dense",
                            "dense_cost" = &dense_strategy.cost
                        );
                        plan_pipeline.extend(dense_strategy.plan);
                    }
                } else {
                    log_metric!(
                        "event" = "strategy_chosen",
                        "type" = &context.initial_dtype,
                        "strategy" = "Dense",
                        "dense_cost" = &dense_strategy.cost
                    );
                    plan_pipeline.extend(dense_strategy.plan);
                }
            }};
        }
        use PhoenixDataType::*;
        // Match on the physical type to correctly cast the byte slice.
        match context.physical_dtype {
            Int8 => evaluate_and_append!(i8),
            Int16 => evaluate_and_append!(i16),
            Int32 => evaluate_and_append!(i32),
            Int64 => evaluate_and_append!(i64),
            UInt8 => evaluate_and_append!(u8),
            UInt16 => evaluate_and_append!(u16),
            UInt32 => evaluate_and_append!(u32),
            UInt64 => evaluate_and_append!(u64),
            // If the physical type is still a float or boolean, it means no
            // special pre-processing was done, so we just use the dense strategy.
            _ => plan_pipeline.extend(dense_strategy.plan),
        }
    }

    let final_plan = Plan {
        plan_version: PLAN_VERSION,
        initial_type: initial_dtype,
        pipeline: plan_pipeline,
    };

    #[cfg(debug_assertions)]
    {
        println!(
            "[PLANNER] Final plan for {:?}: {:?}",
            initial_dtype, final_plan.pipeline
        );
    }

    Ok(final_plan)
}

//==================================================================================
// 3. Public Orchestration API
//==================================================================================

pub fn compress_chunk(array: &dyn Array) -> Result<Vec<u8>, PhoenixError> {
    // 1. The Orchestrator's primary job: create the complete, correct plan.
    let plan = create_plan(array)?;

    // 2. Prepare the initial byte streams from the Arrow array.
    let mut initial_streams = prepare_initial_streams(array)?;
    let mut compressed_streams = HashMap::new();

    let mut main_data_bytes = initial_streams.remove("main").unwrap_or_default();
    let mut current_physical_type = plan.initial_type;
    let mut linear_pipeline: Vec<Operation> = Vec::new();

    // --- THIS IS THE CLEANER 'Interpreter' PATTERN ---
    // 3. Execute the plan by interpreting each operation.
    for op in &plan.pipeline {
        match op {
            Operation::ExtractNulls {
                output_stream_id,
                null_mask_pipeline,
            } => {
                if let Some(null_mask_bytes) = initial_streams.get("null_mask") {
                    let compressed_nulls = executor::execute_linear_encode_pipeline(
                        null_mask_bytes,
                        PhoenixDataType::Boolean,
                        null_mask_pipeline,
                    )?;
                    compressed_streams.insert(output_stream_id.clone(), compressed_nulls);
                }
            }
            Operation::Sparsify {
                mask_stream_id,
                mask_pipeline,
                values_pipeline,
            } => {
                let (mask_vec, dense_values_bytes) =
                    kernels::dispatch_split_stream(&main_data_bytes, current_physical_type)?;
                let mask_bytes: Vec<u8> = mask_vec.iter().map(|&b| b as u8).collect();

                let compressed_mask = executor::execute_linear_encode_pipeline(
                    &mask_bytes,
                    PhoenixDataType::Boolean,
                    mask_pipeline,
                )?;
                compressed_streams.insert(mask_stream_id.clone(), compressed_mask);

                main_data_bytes = dense_values_bytes;
                // The values_pipeline becomes the new main linear pipeline.
                linear_pipeline.extend_from_slice(values_pipeline);
                // Sparsify is a terminal operation for the main stream's plan generation.
                break;
            }
            // For all other linear operations, add them to the list to be executed in one batch.
            _ => {
                linear_pipeline.push(op.clone());
            }
        }
    }

    // 4. Execute the collected linear pipeline on the (potentially) transformed data.
    if !main_data_bytes.is_empty() {
        let compressed_main = executor::execute_linear_encode_pipeline(
            &main_data_bytes,
            current_physical_type,
            &linear_pipeline,
        )?;
        compressed_streams.insert("main".to_string(), compressed_main);
    }

    // 5. Assemble the final artifact.
    let artifact = CompressedChunk {
        total_rows: array.len() as u64,
        original_type: plan.initial_type.to_string(),
        plan_json: serde_json::to_string(&plan)?,
        compressed_streams,
    };

    artifact.to_bytes()
}

// --- AUTHORITATIVE, CORRECTED `decompress_chunk` ---
pub fn decompress_chunk(bytes: &[u8]) -> Result<Box<dyn Array>, PhoenixError> {
    let artifact = CompressedChunk::from_bytes(bytes)?;
    let plan: Plan = serde_json::from_str(&artifact.plan_json)?;
    let total_rows = artifact.total_rows as usize;
    let mut streams = artifact.compressed_streams;

    // 1. Calculate the FULL forward type flow for the ENTIRE plan, ONCE.
    // This becomes the single source of truth for the type state at every step.
    let mut full_type_flow = derive_forward_type_flow(plan.initial_type, &plan.pipeline)?;
    
    // 2. Reverse it to prepare the stack for the decode pipeline.
    // The executor will pop from this stack.
    full_type_flow.reverse();

    let mut decompressed_streams = HashMap::new();
    if let Some(op) = plan
        .pipeline
        .iter()
        .find(|op| matches!(op, Operation::ExtractNulls { .. }))
    {
        if let Operation::ExtractNulls {
            output_stream_id,
            null_mask_pipeline,
        } = op
        {
            if let Some(compressed_nulls) = streams.remove(output_stream_id) {
                let mut type_flow =
                    derive_forward_type_flow(PhoenixDataType::Boolean, null_mask_pipeline)?;
                type_flow.reverse();
                let validity_bytes = executor::execute_linear_decode_pipeline(
                    &compressed_nulls,
                    &mut type_flow,
                    null_mask_pipeline,
                    total_rows,
                )?;
                decompressed_streams.insert("null_mask".to_string(), validity_bytes);
            }
        }
    }
    if let Some(op) = plan
        .pipeline
        .iter()
        .find(|op| matches!(op, Operation::Sparsify { .. }))
    {
        if let Operation::Sparsify {
            mask_stream_id,
            mask_pipeline,
            ..
        } = op
        {
            if let Some(compressed_mask) = streams.remove(mask_stream_id) {
                let num_valid_rows = decompressed_streams
                    .get("null_mask")
                    .map_or(total_rows, |nm| {
                        BooleanBuffer::new(nm.clone().into(), 0, total_rows).count_set_bits()
                    });
                let mut type_flow =
                    derive_forward_type_flow(PhoenixDataType::Boolean, mask_pipeline)?;
                type_flow.reverse();
                let mask_bytes = executor::execute_linear_decode_pipeline(
                    &compressed_mask,
                    &mut type_flow,
                    mask_pipeline,
                    num_valid_rows,
                )?;
                decompressed_streams.insert("sparsity_mask".to_string(), mask_bytes);
            }
        }
    }

    let mut main_data = streams.remove("main").unwrap_or_default();

    if !main_data.is_empty() {
        if let Some((op_idx, op)) = plan
            .pipeline
            .iter()
            .enumerate()
            .find(|(_, op)| matches!(op, Operation::Sparsify { .. }))
        {
            if let Operation::Sparsify {
                values_pipeline, ..
            } = op
            {
                let mask_bytes = decompressed_streams.get("sparsity_mask").unwrap();
                let mask_vec: Vec<bool> = mask_bytes.iter().map(|b| *b != 0).collect();
                let num_non_zero = mask_vec.iter().filter(|&&b| b).count();
                let num_valid_rows = decompressed_streams
                    .get("null_mask")
                    .map_or(total_rows, |nm| {
                        BooleanBuffer::new(nm.clone().into(), 0, total_rows).count_set_bits()
                    });
                let forward_type_flow =
                    derive_forward_type_flow(plan.initial_type, &plan.pipeline)?;
                let values_initial_type = forward_type_flow[op_idx];
                let mut values_type_flow =
                    derive_forward_type_flow(values_initial_type, values_pipeline)?;
                values_type_flow.reverse();
                let decompressed_values = executor::execute_linear_decode_pipeline(
                    &main_data,
                    &mut values_type_flow,
                    values_pipeline,
                    num_non_zero,
                )?;
                main_data = kernels::dispatch_reconstruct_stream(
                    &mask_vec,
                    &decompressed_values,
                    values_initial_type,
                    num_valid_rows,
                )?;
            }
        }

        let mut linear_pipeline: Vec<Operation> = Vec::new();
        for op in &plan.pipeline {
            if !matches!(
                op,
                Operation::Sparsify { .. } | Operation::ExtractNulls { .. }
            ) {
                linear_pipeline.push(op.clone());
            }
        }

        let mut linear_type_flow = derive_forward_type_flow(plan.initial_type, &linear_pipeline)?;
        linear_type_flow.reverse();
        let num_values_for_linear = decompressed_streams
            .get("null_mask")
            .map_or(total_rows, |nm| {
                BooleanBuffer::new(nm.clone().into(), 0, total_rows).count_set_bits()
            });

        main_data = executor::execute_linear_decode_pipeline(
            &main_data,
            &mut linear_type_flow,
            &linear_pipeline,
            num_values_for_linear,
        )?;
    }

    let null_buffer = decompressed_streams
        .get("null_mask")
        .map(|bytes| NullBuffer::from(BooleanBuffer::new(bytes.clone().into(), 0, total_rows)));
    macro_rules! reapply_and_build {
        ($T:ty) => {
            bitmap::reapply_bitmap::<$T>(main_data, null_buffer, total_rows)
                .map(|arr| Box::new(arr) as Box<dyn Array>)
        };
    }
    match plan.initial_type {
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

pub fn get_compressed_chunk_info(
    bytes: &[u8],
) -> Result<(usize, usize, String, String), PhoenixError> {
    let artifact = CompressedChunk::from_bytes(bytes)?;
    let data_size = artifact.compressed_streams.values().map(|v| v.len()).sum();
    let header_size = bytes.len() - data_size;
    let plan: Plan = serde_json::from_str(&artifact.plan_json)?;
    let pretty_plan = serde_json::to_string_pretty(&plan)?;
    Ok((header_size, data_size, pretty_plan, artifact.original_type))
}
