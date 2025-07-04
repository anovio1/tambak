// phoenix-core\src\pipeline\orchestrator\core.rs
use std::collections::HashMap;

use crate::error::PhoenixError;
use crate::kernels;
use crate::pipeline::artifact::CompressedChunk;
use crate::pipeline::context::PipelineInput;
use crate::pipeline::models::{Operation, Plan};
use crate::pipeline::orchestrator::helpers::StrategyResult;
use crate::pipeline::planner::PlanningContext;
use crate::pipeline::traits::StreamTransform;
use crate::pipeline::OperationBehavior;
use crate::pipeline::{executor, planner};
use crate::types::PhoenixDataType;

//==================================================================================
// Private Helper Functions (for 4.6 Compression)
//==================================================================================

/// Handles the early-exit case when the main data stream is empty.
/// This typically means the array was either empty or contained only nulls.
pub fn compress_empty_main_data_stream(input: &PipelineInput) -> Result<Vec<u8>, PhoenixError> {
    let mut compressed_streams = HashMap::new();
    // The pipeline will only contain the null-handling operation, if any.
    let plan_pipeline = if let Some(null_mask_bytes) = &input.null_mask {
        // Since this is a simple case, we use a fixed, robust plan for the null mask
        // instead of invoking the full planner.
        let null_mask_plan = vec![Operation::Rle, Operation::Zstd { level: 3 }];
        let compressed_nulls = executor::execute_linear_encode_pipeline(
            null_mask_bytes,
            PhoenixDataType::Boolean,
            &null_mask_plan,
        )?;
        compressed_streams.insert("null_mask".to_string(), compressed_nulls);

        // The plan must declare that this stream was extracted.
        vec![Operation::ExtractNulls {
            output_stream_id: "null_mask".to_string(),
            null_mask_pipeline: null_mask_plan,
        }]
    } else {
        #[cfg(debug_assertions)]
        println!("[DEBUG compress_empty_main_data_stream] Main data is empty. Assembling an empty-data artifact and returning early.");
        // No main data and no nulls means a truly empty array.
        vec![]
    };

    let plan = Plan {
        plan_version: 1,
        initial_type: input.initial_dtype,
        pipeline: plan_pipeline,
    };

    let artifact = CompressedChunk {
        total_rows: input.total_rows as u64,
        original_type: plan.initial_type.to_string(),
        plan_json: serde_json::to_string(&plan)?,
        compressed_streams,
    };

    artifact.to_bytes()
}

/// Preprocesses the main data stream before planning, handling float-specific transforms.
/// Returns the transformed data, the operations performed, and the new physical type.
pub fn preprocess_input_data(
    input: &PipelineInput,
) -> Result<(Vec<u8>, Vec<Operation>, PhoenixDataType), PhoenixError> {
    let mut current_physical_type = input.initial_dtype;
    let mut pipeline_prefix = Vec::new();
    let mut main_data = input.main.clone(); // Clone to take ownership

    if input.initial_dtype.is_float() {
        // The orchestrator is responsible for this pre-processing step because it turns
        // floats into a format the integer-centric planner can understand.

        // Step 1a: CanonicalizeZeros operation
        let canonicalize_op = Operation::CanonicalizeZeros;
        let mut canonicalized_buffer = Vec::with_capacity(main_data.len());
        kernels::dispatch_encode(
            &canonicalize_op,
            &main_data,
            &mut canonicalized_buffer,
            input.initial_dtype,
        )?;
        main_data = canonicalized_buffer;
        pipeline_prefix.push(canonicalize_op);

        // Step 1b: BitCast operation
        let bitcast_op = Operation::BitCast {
            to_type: if input.initial_dtype == PhoenixDataType::Float32 {
                PhoenixDataType::UInt32
            } else {
                PhoenixDataType::UInt64
            },
        };

        // Update the physical type that the planner will see.
        let transform_result = bitcast_op.transform_stream(current_physical_type)?;
        current_physical_type = match transform_result {
            StreamTransform::TypeChange(new_type) => new_type,
            _ => {
                return Err(PhoenixError::InternalError(
                    "BitCast must produce a TypeChange".to_string(),
                ))
            }
        };

        let mut bitcast_buffer = Vec::with_capacity(main_data.len());
        // The type for this dispatch is still the *original* type before the bitcast.
        kernels::dispatch_encode(
            &bitcast_op,
            &main_data,
            &mut bitcast_buffer,
            input.initial_dtype,
        )?;
        main_data = bitcast_buffer;
        pipeline_prefix.push(bitcast_op);
    }

    Ok((main_data, pipeline_prefix, current_physical_type))
}

/// Plans and compresses the null mask stream if it exists.
/// Returns the combined pipeline (prefix + null ops) and the compressed null stream.
pub fn plan_and_compress_null_stream(
    input: &PipelineInput,
    pipeline_prefix: &mut Vec<Operation>,
) -> Result<(Vec<Operation>, HashMap<String, Vec<u8>>), PhoenixError> {
    let mut compressed_streams = HashMap::new();

    if let Some(null_mask_bytes) = &input.null_mask {
        let null_context = PlanningContext {
            initial_dtype: PhoenixDataType::Boolean,
            physical_dtype: PhoenixDataType::Boolean,
        };
        let null_mask_plan = planner::plan_pipeline(null_mask_bytes, null_context)?;

        // Prepend the ExtractNulls operation to the existing prefix.
        pipeline_prefix.push(Operation::ExtractNulls {
            output_stream_id: "null_mask".to_string(),
            null_mask_pipeline: null_mask_plan.pipeline.clone(),
        });

        // Execute the compression for the null stream.
        let compressed_nulls = executor::execute_linear_encode_pipeline(
            null_mask_bytes,
            PhoenixDataType::Boolean,
            &null_mask_plan.pipeline,
        )?;
        compressed_streams.insert("null_mask".to_string(), compressed_nulls);
    }

    // Return the pipeline prefix (which now includes null handling) and the new streams map.
    Ok((pipeline_prefix.clone(), compressed_streams))
}

// Patch Helper due to lost sparsity strategy
/// Determines the optimal pipeline for the main data by evaluating and comparing
/// dense and sparse strategies.
///
/// For integer-like data, it empirically costs both a dense pipeline and a potential
/// sparse pipeline, returning the plan for the cheaper of the two. For all other
/// data types, it returns a standard dense plan.
///
/// This function encapsulates the core strategic decision for the main data stream.
pub fn plan_main_data_pipeline(
    processed_data: &[u8],
    input: &PipelineInput,
    current_physical_type: PhoenixDataType,
) -> Result<Vec<Operation>, PhoenixError> {
    let context = PlanningContext {
        initial_dtype: input.initial_dtype,
        physical_dtype: current_physical_type,
    };

    // We can only evaluate sparsity on integer-like types.
    let is_integer_like = matches!(
        current_physical_type,
        PhoenixDataType::Int8
            | PhoenixDataType::Int16
            | PhoenixDataType::Int32
            | PhoenixDataType::Int64
            | PhoenixDataType::UInt8
            | PhoenixDataType::UInt16
            | PhoenixDataType::UInt32
            | PhoenixDataType::UInt64
    );

    if is_integer_like {
        // --- This is the integer-specific path with sparse vs. dense comparison ---

        // First, plan and cost the DENSE strategy. This is our baseline.
        let dense_plan = planner::plan_pipeline(processed_data, context.clone())?;
        let dense_cost = executor::execute_linear_encode_pipeline(
            processed_data,
            context.physical_dtype,
            &dense_plan.pipeline,
        )?
        .len();
        let dense_strategy = StrategyResult {
            plan: dense_plan.pipeline,
            cost: dense_cost,
        };

        // Now, try to plan and cost a SPARSE strategy.
        let sparse_strategy_result = evaluate_sparsity_strategy_for_type(processed_data, &context)?;

        // Compare costs and return the winning pipeline.
        if let Some(sparse_strategy) = sparse_strategy_result {
            if sparse_strategy.cost < dense_strategy.cost {
                Ok(sparse_strategy.plan) // Sparse is cheaper
            } else {
                Ok(dense_strategy.plan) // Dense is cheaper
            }
        } else {
            Ok(dense_strategy.plan) // Sparse not applicable, use dense
        }
    } else {
        // --- This is the fallback path for non-integer types ---
        let dense_plan = planner::plan_pipeline(processed_data, context)?;
        Ok(dense_plan.pipeline)
    }
}

/// Helper to call the generic `evaluate_sparsity_strategy` based on the runtime type.
fn evaluate_sparsity_strategy_for_type(
    data_bytes: &[u8],
    context: &PlanningContext,
) -> Result<Option<StrategyResult>, PhoenixError> {
    use PhoenixDataType::*;
    // This function remains the same.
    match context.physical_dtype {
        Int8 => {
            let s: &[i8] = bytemuck::try_cast_slice(data_bytes)?;
            super::helpers::evaluate_sparsity_strategy(s, context)
        }
        Int16 => {
            let s: &[i16] = bytemuck::try_cast_slice(data_bytes)?;
            super::helpers::evaluate_sparsity_strategy(s, context)
        }
        Int32 => {
            let s: &[i32] = bytemuck::try_cast_slice(data_bytes)?;
            super::helpers::evaluate_sparsity_strategy(s, context)
        }
        Int64 => {
            let s: &[i64] = bytemuck::try_cast_slice(data_bytes)?;
            super::helpers::evaluate_sparsity_strategy(s, context)
        }
        UInt8 => {
            let s: &[u8] = bytemuck::try_cast_slice(data_bytes)?;
            super::helpers::evaluate_sparsity_strategy(s, context)
        }
        UInt16 => {
            let s: &[u16] = bytemuck::try_cast_slice(data_bytes)?;
            super::helpers::evaluate_sparsity_strategy(s, context)
        }
        UInt32 => {
            let s: &[u32] = bytemuck::try_cast_slice(data_bytes)?;
            super::helpers::evaluate_sparsity_strategy(s, context)
        }
        UInt64 => {
            let s: &[u64] = bytemuck::try_cast_slice(data_bytes)?;
            super::helpers::evaluate_sparsity_strategy(s, context)
        }
        _ => Ok(None),
    }
}
