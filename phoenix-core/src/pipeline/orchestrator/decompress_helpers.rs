// phoenix-core\src\pipeline\orchestrator\core.rs

use std::collections::HashMap;

use crate::error::PhoenixError;
use crate::kernels;
use crate::pipeline::executor;
use crate::pipeline::models::{Operation, Plan};
use crate::pipeline::orchestrator::helpers::*;
use crate::pipeline::traits::StreamTransform;
use crate::pipeline::OperationBehavior;
use crate::types::PhoenixDataType;

//==================================================================================
// Private Helper Functions (for 4.6 Decompression)
//==================================================================================

/// Decompresses only the null mask stream from the artifact.
pub fn decompress_null_mask_stream(
    plan: &Plan,
    streams: &mut HashMap<String, Vec<u8>>,
    total_rows: usize,
) -> Result<Option<Vec<u8>>, PhoenixError> {
    // This helper remains unchanged, as it was already well-factored.
    decompress_meta_stream(
        plan.pipeline.iter().find_map(|op| match op {
            Operation::ExtractNulls {
                output_stream_id,
                null_mask_pipeline,
            } => Some((output_stream_id.as_str(), null_mask_pipeline)),
            _ => None,
        }),
        streams,
        total_rows,
        PhoenixDataType::Boolean,
    )
}

/// Decompresses only the sparsity mask stream from the artifact.
pub fn decompress_sparsity_mask_stream(
    plan: &Plan,
    streams: &mut HashMap<String, Vec<u8>>,
    num_valid_rows: usize,
) -> Result<Option<Vec<u8>>, PhoenixError> {
    // This helper also remains unchanged.
    decompress_meta_stream(
        plan.pipeline.iter().find_map(|op| match op {
            Operation::Sparsify {
                mask_stream_id,
                mask_pipeline,
                ..
            } => Some((mask_stream_id.as_str(), mask_pipeline)),
            _ => None,
        }),
        streams,
        num_valid_rows,
        PhoenixDataType::Boolean,
    )
}

/// Handles the complete decompression logic for a sparse pipeline.
/// This logic is preserved from the original implementation but now lives in its own function.
pub fn decompress_sparse_pipeline(
    plan: &Plan,
    main_data: Vec<u8>,
    sparsity_mask_bytes: Option<Vec<u8>>,
    num_valid_rows: usize,
) -> Result<Vec<u8>, PhoenixError> {
    let sparsify_index = plan
        .pipeline
        .iter()
        .position(|op| matches!(op, Operation::Sparsify { .. }))
        .expect("BUG: decompress_sparse_pipeline called on a plan with no Sparsify op.");

    let pre_sparsify_pipeline = &plan.pipeline[..sparsify_index];
    let (values_pipeline, post_sparsify_pipeline) = if let Operation::Sparsify {
        values_pipeline,
        ..
    } = &plan.pipeline[sparsify_index]
    {
        (
            values_pipeline.clone(),
            plan.pipeline[sparsify_index + 1..].to_vec(),
        )
    } else {
        unreachable!();
    };

    let mut values_initial_type = plan.initial_type;
    for op in pre_sparsify_pipeline {
        let transform = op.transform_stream(values_initial_type)?;
        values_initial_type = match transform {
            StreamTransform::PreserveType => values_initial_type,
            StreamTransform::TypeChange(new_type) => new_type,
            StreamTransform::Restructure {
                primary_output_type,
                ..
            } => primary_output_type,
        };
    }

    let mut values_type_flow = derive_forward_type_flow(values_initial_type, &values_pipeline)?;
    let num_non_zero = sparsity_mask_bytes
        .as_ref()
        .map_or(0, |sm| sm.iter().filter(|&b| *b != 0).count());

    let decompressed_values = executor::execute_linear_decode_pipeline(
        &main_data,
        &mut values_type_flow,
        &values_pipeline,
        num_non_zero,
    )?;

    let mask_vec: Vec<bool> = sparsity_mask_bytes
        .ok_or_else(|| {
            PhoenixError::InternalError("Sparsity mask is missing for sparse pipeline".to_string())
        })?
        .iter()
        .map(|b| *b != 0)
        .collect();

    let mut reconstructed_data = kernels::dispatch_reconstruct_stream(
        &mask_vec,
        &decompressed_values,
        values_initial_type,
        num_valid_rows,
    )?;

    if !post_sparsify_pipeline.is_empty() {
        let mut post_type_flow =
            derive_forward_type_flow(values_initial_type, &post_sparsify_pipeline)?;
        reconstructed_data = executor::execute_linear_decode_pipeline(
            &reconstructed_data,
            &mut post_type_flow,
            &post_sparsify_pipeline,
            num_valid_rows,
        )?;
    }

    Ok(reconstructed_data)
}

/// Handles the complete decompression logic for a dense (non-sparse) pipeline.
/// This implementation includes the architect's O(N) performance optimization.
pub fn decompress_dense_pipeline(
    plan: &Plan,
    main_data: Vec<u8>,
    num_valid_rows: usize,
) -> Result<Vec<u8>, PhoenixError> {
    // 1. Correctly filter out ONLY the stream-management meta-ops to get the true linear pipeline.
    // This pipeline includes all operations that have a corresponding kernel.
    let linear_pipeline: Vec<Operation> = plan
        .pipeline
        .iter()
        .filter(|op| !matches!(op, Operation::ExtractNulls { .. }))
        .cloned()
        .collect();

    // ======================= DEBUG BLOCK (UNCHANGED) =======================
    // This will now show the correct partitioning.
    #[cfg(debug_assertions)]
    {
        // To make this useful, we also need to show the meta ops.
        let meta_ops: Vec<Operation> = plan
            .pipeline
            .iter()
            .filter(|op| matches!(op, Operation::ExtractNulls { .. }))
            .cloned()
            .collect();
        println!("[DEBUG Decompress] Analyzing DENSE pipeline:");
        println!("  - Full Plan: {:?}", plan.pipeline);
        println!("  - Deduced Meta Ops: {:?}", meta_ops);
        println!("  - Deduced Linear Pipeline: {:?}", linear_pipeline);
    }
    // =====================================================================

    if linear_pipeline.is_empty() {
        return Ok(main_data);
    }

    // 2. Correctly derive the starting type for the linear segment.
    // We iterate through the full plan and apply transforms for the meta-ops we skipped.
    let mut segment_initial_type = plan.initial_type;
    for op in &plan.pipeline {
        if matches!(op, Operation::ExtractNulls { .. }) {
            // This is a meta-op we are stepping over. Apply its type transform.
            let transform = op.transform_stream(segment_initial_type)?;
            segment_initial_type = match transform {
                StreamTransform::PreserveType => segment_initial_type,
                StreamTransform::TypeChange(new_type) => new_type,
                StreamTransform::Restructure {
                    primary_output_type,
                    ..
                } => primary_output_type,
            };
        } else {
            // We have reached the first linear operation. The type for the
            // rest of the pipeline is now fixed. We can stop deriving.
            break;
        }
    }

    // 3. Execute the correctly partitioned linear pipeline with the correctly derived starting type.
    let mut linear_type_flow = derive_forward_type_flow(segment_initial_type, &linear_pipeline)?;

    executor::execute_linear_decode_pipeline(
        &main_data,
        &mut linear_type_flow,
        &linear_pipeline,
        num_valid_rows,
    )
}
