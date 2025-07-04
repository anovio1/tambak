use arrow::array::{Array};
use arrow::buffer::{BooleanBuffer};

use super::helpers::*;
use crate::error::PhoenixError;
use crate::pipeline::models::{Operation, Plan};
use crate::pipeline::planner::PlanningContext;
use crate::pipeline::traits::StreamTransform;
use crate::pipeline::OperationBehavior;
use crate::pipeline::{executor, planner};
use crate::types::PhoenixDataType;

const PLAN_VERSION: u32 = 2;

/// Generates an initial candidate compression plan for a single Arrow array.
///
/// This function builds the *first-stage* pipeline by:
/// 1. Detecting the array's logical type and applying necessary type transformations
///    (e.g., bit-casting floats to integers to enable integer encoders).
/// 2. Planning separate sub-pipelines for each stream, such as:
///    - The null mask stream (if the array has nulls)
///    - The dense, validity-stripped data stream
/// 3. Optionally comparing dense vs. sparse strategies for integer-like types,
///    and choosing the lower-cost strategy based on an estimated byte size.
///
/// The output `Plan` is an initial, per-array compression pipeline that can be further refined,
/// extended, or combined with other plans downstream (e.g., at chunk level, multi-column level,
/// or using runtime statistics).
///
/// # Arguments
/// * `array` - The Arrow array to analyze and build a candidate plan for.
///
/// # Returns
/// * `Ok(Plan)` containing the planned pipeline of operations.
/// * `Err(PhoenixError)` if the type is unsupported or planning fails.
///
/// # Note
/// - This function **does not** produce a globally optimal compression plan on its own.
/// - It prepares a best-effort, type- and sparsity-aware starting point for the orchestrator
///   and executor to apply.
pub fn create_plan(array: &dyn Array) -> Result<Plan, PhoenixError> {
    println!("inside create_plan");
    // -------------------------------------------------------------------------
    // Identify the array's logical data type (e.g., Int32, Float64).
    // This logical type drives type-specific planning decisions downstream.
    let initial_dtype = PhoenixDataType::from_arrow_type(array.data_type())?;
    let mut plan_pipeline: Vec<Operation> = Vec::new();
    let mut current_physical_type = initial_dtype;

    // -------------------------------------------------------------------------
    // 1 Perform all initial type transformations FIRST.
    // Certain data types require a preprocessing step to make them suitable
    // for compression: e.g., floats often need bit-casting so integer encoders can be used.
    if let PhoenixDataType::Float32 | PhoenixDataType::Float64 = initial_dtype {
        plan_pipeline.push(Operation::CanonicalizeZeros);
        let bitcast_op = Operation::BitCast {
            to_type: if initial_dtype == PhoenixDataType::Float32 {
                PhoenixDataType::UInt32
            } else {
                PhoenixDataType::UInt64
            },
        };

        // Update our notion of the physical type after the bitcast.
        // BitCast should always produce a TypeChange, so we check that explicitly.
        let transform = bitcast_op.transform_stream(current_physical_type)?;
        current_physical_type = match transform {
            StreamTransform::TypeChange(new_type) => new_type,
            _ => {
                return Err(PhoenixError::InternalError(
                    "BitCast must produce a TypeChange".to_string(),
                ))
            }
        };
        // --- END FIX ---

        plan_pipeline.push(bitcast_op);
    }

    // -------------------------------------------------------------------------
    // 2 Plan how to handle nulls (if any).
    //
    // Arrow arrays can have nulls, tracked as a compact bitmask.
    // We unpack this null mask into a flat byte stream, then plan a sub-pipeline to compress it.
    if let Some(nulls) = array.nulls() {
        // The null mask is logically a boolean stream (1=valid, 0=null).
        let null_mask_context = PlanningContext {
            initial_dtype: PhoenixDataType::Boolean,
            physical_dtype: PhoenixDataType::Boolean,
        };

        // Unpack Arrow's compact bitmap to a Vec<u8>: [1, 0, 1, ...]
        let boolean_buffer =
            BooleanBuffer::new(nulls.buffer().clone(), nulls.offset(), nulls.len());
        let unpacked_mask_bytes: Vec<u8> = boolean_buffer.iter().map(|b| b as u8).collect();

        // Ask the planner to build a pipeline to compress the null mask.
        let null_mask_plan = planner::plan_pipeline(&unpacked_mask_bytes, null_mask_context)?;

        // Add an ExtractNulls operation: this will separate the null mask from the data stream,
        // and compress it using the planned null_mask_pipeline.
        plan_pipeline.push(Operation::ExtractNulls {
            output_stream_id: "null_mask".to_string(),
            null_mask_pipeline: null_mask_plan.pipeline,
        });
    }

    // -------------------------------------------------------------------------
    // 3 Plan the main data stream (the actual values, stripped of nulls).
    //
    // This step generates and compares alternative strategies (dense vs sparse) if applicable.
    let valid_data_bytes = prepare_initial_streams_deprecated_soon(array)?
        .remove("main")
        .unwrap_or_default();

    // ======================= PROPOSED DEBUG BLOCK =======================
    #[cfg(debug_assertions)]
    {
        println!(
            "[DEBUG create_plan] Received valid_data_bytes with len: {}",
            valid_data_bytes.len()
        );

        // THIS IS THE FIX: If there's no valid data, there's nothing to plan.
        // Return the plan we have so far (which might contain ExtractNulls).
        if valid_data_bytes.is_empty() {
            println!("[DEBUG create_plan] Empty data, returning early.");
            return Ok(Plan {
                plan_version: PLAN_VERSION,
                initial_type: initial_dtype,
                pipeline: plan_pipeline,
            });
        }
    }
    // ===================== END PROPOSED DEBUG BLOCK =====================

    if !valid_data_bytes.is_empty() {
        // Create planning context describing the starting logical type and current physical type.
        let context = PlanningContext {
            initial_dtype,
            physical_dtype: current_physical_type,
        };

        // Ask the planner to build a dense pipeline (i.e., no sparse transforms).
        let dense_plan = planner::plan_pipeline(&valid_data_bytes, context.clone())?;

        // Estimate its cost by running the pipeline and measuring compressed size.
        let dense_cost = executor::execute_linear_encode_pipeline(
            &valid_data_bytes,
            context.physical_dtype,
            &dense_plan.pipeline,
        )?
        .len();
        let dense_strategy = StrategyResult {
            plan: dense_plan.pipeline,
            cost: dense_cost,
        };

        // ---------------------------------------------------------------------
        // 3 Evaluate sparse strategy (e.g., RLE, skipping zeros) for integer-like types.
        //
        // Compare dense vs sparse costs and choose the cheaper one.
        macro_rules! evaluate_and_append {
            ($T:ty) => {{
                // Convert raw bytes into typed slice (e.g., &[i32]).
                let typed_slice: &[$T] = bytemuck::try_cast_slice(&valid_data_bytes)?;

                // Try sparse strategy; returns None if data isn't sparse enough to benefit.
                if let Some(sparse_strategy) = evaluate_sparsity_strategy(typed_slice, &context)? {
                    if sparse_strategy.cost < dense_strategy.cost {
                        // Sparse is cheaper; use its pipeline.
                        plan_pipeline.extend(sparse_strategy.plan);
                    } else {
                        // Dense is cheaper; use dense pipeline.
                        plan_pipeline.extend(dense_strategy.plan);
                    }
                } else {
                    // Sparse strategy not applicable; use dense pipeline.
                    plan_pipeline.extend(dense_strategy.plan);
                }
            }};
        }

        use PhoenixDataType::*;
        match context.physical_dtype {
            // Only run sparse evaluation for integer-like types.
            Int8 => evaluate_and_append!(i8),
            Int16 => evaluate_and_append!(i16),
            Int32 => evaluate_and_append!(i32),
            Int64 => evaluate_and_append!(i64),
            UInt8 => evaluate_and_append!(u8),
            UInt16 => evaluate_and_append!(u16),
            UInt32 => evaluate_and_append!(u32),
            UInt64 => evaluate_and_append!(u64),
            // For floats or other non-integer types: skip sparse check, use dense pipeline.
            _ => plan_pipeline.extend(dense_strategy.plan),
        }
    }

    // -------------------------------------------------------------------------
    // 4 Build and return the final candidate Plan object.
    //
    // This Plan can be refined further downstream if needed.
    let final_plan = Plan {
        plan_version: PLAN_VERSION,
        initial_type: initial_dtype,
        pipeline: plan_pipeline,
    };
    Ok(final_plan)
}
