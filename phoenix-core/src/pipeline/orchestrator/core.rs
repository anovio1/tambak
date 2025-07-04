// phoenix-core\src\pipeline\orchestrator\core.rs

use arrow::array::Array;
use std::collections::HashMap;

use crate::error::PhoenixError;
use crate::kernels;
use crate::pipeline::artifact::CompressedChunk;
use crate::pipeline::context::{PipelineInput, PipelineOutput};
use crate::pipeline::models::{Operation, Plan};
use crate::pipeline::orchestrator::compress_helpers::*;
use crate::pipeline::orchestrator::create_plan::*;
use crate::pipeline::orchestrator::decompress_helpers::*;
use crate::pipeline::orchestrator::helpers::*;
use crate::pipeline::planner::PlanningContext;
use crate::pipeline::traits::StreamTransform;
use crate::pipeline::OperationBehavior;
use crate::pipeline::{executor, planner};
use crate::types::PhoenixDataType;

//==================================================================================
// V2 Orchestration Logic (Pure, SoC-Compliant)
//==================================================================================

/// The new, pure, Arrow-agnostic core compression function.
/// This version correctly uses the existing planner and executor logic.
/// The new, pure, Arrow-agnostic core compression function.
///
/// This version follows a clean, modular structure. It acts as a high-level
/// coordinator, delegating specific tasks like preprocessing, null handling, and
/// main data compression to focused helper functions.
pub fn compress_chunk_v2(input: PipelineInput) -> Result<Vec<u8>, PhoenixError> {
    // 1. Handle the simple case of an empty array first.
    if input.main.is_empty() {
        return compress_empty_main_data_stream(&input);
    }

    // 2. Preprocess input data (e.g., float canonicalization, bit-casting).
    // This separates the initial data transformation from the main planning logic.
    let (processed_data, mut pipeline_prefix, current_physical_type) =
        preprocess_input_data(&input)?;

    // 3. Delegate the planning of the main data pipeline to the planner module.
    let planner_context = PlanningContext {
        initial_dtype: input.initial_dtype,
        physical_dtype: current_physical_type,
    };
    let main_plan = planner::plan_pipeline(&processed_data, planner_context)?;

    // 4. Plan and compress the null mask, if it exists, and combine pipelines.
    let (mut final_pipeline, mut compressed_streams) =
        plan_and_compress_null_stream(&input, &mut pipeline_prefix)?;

    // Append the main pipeline operations to the plan that now includes prefixes and nulls.
    final_pipeline.extend(main_plan.pipeline.iter().cloned());

    // 5. Execute the main data compression using the plan from the planner.
    let compressed_main = executor::execute_linear_encode_pipeline(
        &processed_data,
        current_physical_type,
        &main_plan.pipeline,
    )?;
    compressed_streams.insert("main".to_string(), compressed_main);

    // 6. Assemble and serialize the final artifact.
    let final_plan_struct = Plan {
        plan_version: 1, // Use a shared constant for this.
        initial_type: input.initial_dtype,
        pipeline: final_pipeline,
    };

    let artifact = crate::pipeline::artifact::CompressedChunk {
        total_rows: input.total_rows as u64,
        original_type: final_plan_struct.initial_type.to_string(),
        plan_json: serde_json::to_string(&final_plan_struct)?,
        compressed_streams,
    };

    artifact.to_bytes()
}

pub fn decompress_chunk_v2(bytes: &[u8]) -> Result<PipelineOutput, PhoenixError> {
    // TODO (ARCH-REFACTOR): This is a temporary shim.
    // The final architecture should have this function return a pure `PipelineData`
    // struct, with the final conversion to an Arrow Array happening in the `bridge`.
    // For now, we delegate to the old, stable implementation to de-risk the
    // bridge integration.

    decompress_chunk(bytes)
}

//==================================================================================
// 3. Public Orchestration API
//==================================================================================

pub fn compress_chunk(array: &dyn Array) -> Result<Vec<u8>, PhoenixError> {
    #[cfg(debug_assertions)]
    {
        // ======================= DEBUG PRINT #A =======================
        println!("[DEBUG compress_chunk DEBUG PRINT #A]");
        // =============================================================
    }

    let plan = create_plan(array)?;

    let mut initial_streams = prepare_initial_streams_deprecated_soon(array)?;
    let mut compressed_streams: HashMap<String, Vec<u8>> = HashMap::new();

    let mut main_data_bytes = initial_streams.remove("main").unwrap_or_default();
    #[cfg(debug_assertions)]
    {
        // ======================= DEBUG PRINT #1 =======================
        println!(
            "[DEBUG compress_chunk] After initial stream prep, main_data_bytes.len() = {}",
            main_data_bytes.len()
        );
        // =============================================================
    }

    // --- 1. Handle Meta-Op Side-Channels First ---
    if let Some(Operation::ExtractNulls {
        output_stream_id,
        null_mask_pipeline,
    }) = plan
        .pipeline
        .iter()
        .find(|op| matches!(op, Operation::ExtractNulls { .. }))
    {
        if let Some(null_mask_bytes) = initial_streams.get("null_mask") {
            let compressed_nulls = executor::execute_linear_encode_pipeline(
                null_mask_bytes,
                PhoenixDataType::Boolean,
                null_mask_pipeline,
            )?;
            compressed_streams.insert(output_stream_id.clone(), compressed_nulls);
        }
    }

    // --- 2. Partition the Main Pipeline and Determine Segment Start Type ---
    let mut linear_pipeline: Vec<Operation> = Vec::new();
    // This will be the true physical type of the data at the start of the linear segment.
    let mut segment_initial_type = plan.initial_type;
    let mut linear_segment_started = false;

    for op in &plan.pipeline {
        if matches!(op, Operation::ExtractNulls { .. }) {
            // This op runs on a different stream; we just need to account for its type transform
            // on the conceptual main data path.

            if !linear_segment_started {
                let transform_result = op.transform_stream(segment_initial_type)?;
                segment_initial_type = match transform_result {
                    StreamTransform::PreserveType => segment_initial_type,
                    StreamTransform::TypeChange(new_type) => new_type,
                    // In a linear flow, we care about the primary output type
                    StreamTransform::Restructure {
                        primary_output_type,
                        ..
                    } => primary_output_type,
                };
            }
            continue;
        }
        #[cfg(debug_assertions)]
        {
            // ======================= DEBUG PRINT #2 =======================
            println!("[DEBUG compress_chunk] After pipeline partitioning, main_data_bytes.len() = {}. is_empty() = {}", main_data_bytes.len(), main_data_bytes.is_empty());
            println!("[DEBUG compress_chunk] About to call execute_linear_encode_pipeline with initial_type: {:?}, pipeline: {:?}", segment_initial_type, linear_pipeline);
            // =============================================================
        }

        if let Operation::Sparsify {
            mask_stream_id,
            mask_pipeline,
            values_pipeline,
        } = op
        {
            // Sparsify is a branch. The operations *before* it determine the type of the data it receives.
            let (mask_vec, dense_values_bytes) =
                kernels::dispatch_split_stream(&main_data_bytes, segment_initial_type)?;
            let mask_bytes: Vec<u8> = mask_vec.iter().map(|&b| b as u8).collect();

            // Compress the mask stream.
            let compressed_mask = executor::execute_linear_encode_pipeline(
                &mask_bytes,
                PhoenixDataType::Boolean,
                mask_pipeline,
            )?;
            compressed_streams.insert(mask_stream_id.clone(), compressed_mask);

            // The main data stream is now the dense values, and its pipeline is the values_pipeline.
            main_data_bytes = dense_values_bytes;
            linear_pipeline = values_pipeline.clone();
            linear_segment_started = true;
            break; // The rest of the plan is irrelevant for the main stream after Sparsify.
        }

        // If it's not a meta-op, it's part of the main linear pipeline.
        linear_pipeline.push(op.clone());
        linear_segment_started = true;
    }

    // --- 3. Execute the Main Linear Pipeline with the Correct Context ---
    if !main_data_bytes.is_empty() {
        // ======================= PROPOSED DEBUG BLOCK #2 =======================
        #[cfg(debug_assertions)]
        {
            println!("[DEBUG Compress] EXECUTING MAIN LINEAR PIPELINE:");
            println!("  - Data Length: {}", main_data_bytes.len());
            println!("  - Segment Initial Type: {:?}", segment_initial_type);
            println!("  - Linear Pipeline to Execute: {:?}", linear_pipeline);
        }
        // ===================== END PROPOSED DEBUG BLOCK #2 =====================
        let compressed_main = executor::execute_linear_encode_pipeline(
            &main_data_bytes,
            segment_initial_type, // Pass the CORRECT starting type for the segment.
            &linear_pipeline,
        )?;
        compressed_streams.insert("main".to_string(), compressed_main);
    }

    // --- 4. Assemble the Final Artifact ---
    let artifact = CompressedChunk {
        total_rows: array.len() as u64,
        original_type: plan.initial_type.to_string(),
        plan_json: serde_json::to_string(&plan)?,
        compressed_streams,
    };

    artifact.to_bytes()
}

// --- ARCHITECTURAL NOTE (L15 Collaborator) ---
//
// This function is the result of a significant architectural refinement and its
// current structure is critical for correctness. Do not change it lightly.
//
// THE CORE PROBLEM IT SOLVES:
// During decompression, the `Executor` must be given a "work order" (a pipeline
// segment) and a "blueprint" (a type_flow stack) that are perfectly
// synchronized. Previous implementations failed because they created a single,
// global type_flow and tried to manually track state through a complex loop,
// leading to type mismatches for pipelines involving `BitCast` or `ZigZag`.
//
// REJECTED ALTERNATIVES:
// 1. A single `for op in reversed_pipeline` loop: This approach was tried and
//    rejected. It proved too complex to correctly manage the type state for
//    both dense and sparse paths, leading to subtle bugs.
// 2. Manual index math on a global `type_flow`: This was also tried and
//    rejected as it was extremely brittle and prone to off-by-one errors.
//
// THE CORRECT ARCHITECTURE ("Plan Partitioning"):
// This implementation treats the decompression plan as a series of independent
// segments. It explicitly handles the "Dense" and "Sparse" paths differently.
// For each segment, it derives a new, perfectly-scoped `type_flow` stack and
// makes a self-contained call to the `Executor`. This ensures the work order
// and blueprint are always in sync. This is the authoritative and correct model.
//
// --- END ARCHITECTURAL NOTE ---

//==================================================================================
// Decompression Orchestration (Refactored & Hardened)
//==================================================================================

/// The top-level orchestrator for decompressing a compressed chunk.
///
/// This function acts as a pure coordinator. It deserializes the artifact and then
/// dispatches to specialized helper functions based on whether the plan contains
/// a sparse or dense strategy. This structure improves readability and maintainability.
pub fn decompress_chunk(bytes: &[u8]) -> Result<PipelineOutput, PhoenixError> {
    // 1. Deserialization: The Orchestrator's core responsibility.
    let artifact = CompressedChunk::from_bytes(bytes)?;
    let plan: Plan = serde_json::from_str(&artifact.plan_json)?;
    let total_rows = artifact.total_rows as usize;
    let mut streams = artifact.compressed_streams;

    // 2. Meta Stream Decompression: Delegate to focused helpers.
    let null_mask_bytes = decompress_null_mask_stream(&plan, &mut streams, total_rows)?;
    let num_valid_rows = null_mask_bytes.as_ref().map_or(total_rows, |bytes| {
        bytes.iter().filter(|&&b| b != 0).count()
    });
    // Main data stream is required for both dense and sparse paths.
    let main_data = streams.remove("main").unwrap_or_default();

    // Early exit for all-null arrays, which have no main data stream.
    if main_data.is_empty() && num_valid_rows == 0 {
        return Ok(PipelineOutput::new(
            Vec::new(),
            null_mask_bytes,
            plan.initial_type,
            total_rows,
        ));
    }

    // 3. Dispatch to Strategy-Specific Decompressor
    let decompressed_data = if plan
        .pipeline
        .iter()
        .any(|op| matches!(op, Operation::Sparsify { .. }))
    {
        let sparsity_mask_bytes =
            decompress_sparsity_mask_stream(&plan, &mut streams, num_valid_rows)?;
        decompress_sparse_pipeline(&plan, main_data, sparsity_mask_bytes, num_valid_rows)?
    } else {
        decompress_dense_pipeline(&plan, main_data, num_valid_rows)?
    };

    // 4. Final Assembly
    Ok(PipelineOutput::new(
        decompressed_data,
        null_mask_bytes,
        plan.initial_type,
        total_rows,
    ))
}

pub fn get_compressed_chunk_info(
    bytes: &[u8],
) -> Result<(usize, usize, String, String), PhoenixError> {
    let artifact = CompressedChunk::from_bytes(bytes)?;
    let data_size = artifact.compressed_streams.values().map(|v| v.len()).sum();
    let header_size = bytes.len() - data_size;
    let plan: Plan = serde_json::from_str(&artifact.plan_json)?;
    let pretty_plan = serde_json::to_string(&plan)?;
    Ok((header_size, data_size, pretty_plan, artifact.original_type))
}
