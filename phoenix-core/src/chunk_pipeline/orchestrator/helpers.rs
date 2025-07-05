use arrow::array::Array;
use arrow::datatypes::*;
use bytemuck::Pod;
use num_traits::{PrimInt, Zero};
use std::collections::HashMap;

use crate::error::PhoenixError;
use crate::chunk_pipeline::models::Operation;
use crate::chunk_pipeline::planner::PlanningContext;
use crate::chunk_pipeline::traits::StreamTransform;
use crate::chunk_pipeline::OperationBehavior;
use crate::chunk_pipeline::{executor, planner};
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
    // ======================= DEBUG BLOCK #2 =======================
    #[cfg(debug_assertions)]
    {
        println!("[DEBUG evaluate_sparsity_strategy] COST BREAKDOWN:");
        println!("  - Non-Zero Values Cost (values_cost): {}", values_cost);
        println!("  - Sparsity Mask Cost (mask_cost):     {}", mask_cost);
        println!(
            "  - Total Sparse Cost:                  {}",
            values_cost + mask_cost
        );
    }
    // ===================== END DEBUG BLOCK #2 =====================
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
