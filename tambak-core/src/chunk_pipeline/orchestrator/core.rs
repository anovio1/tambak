// tambak-core\src\pipeline\orchestrator\core.rs

use arrow::array::Array;

use crate::error::tambakError;
use crate::kernels;
use crate::chunk_pipeline::artifact::CompressedChunk;
use crate::chunk_pipeline::context::{PipelineInput, PipelineOutput};
use crate::chunk_pipeline::models::{Operation, ChunkPlan};
use crate::chunk_pipeline::orchestrator::compress_helpers::*;
use crate::chunk_pipeline::orchestrator::decompress_helpers::*;
use crate::chunk_pipeline::executor;
use crate::types::TambakDataType;

//==================================================================================
// 3. Public Orchestration API
//==================================================================================
/// The new, pure, Arrow-agnostic core compression function.
/// This version correctly uses the existing planner and executor logic.
/// The new, pure, Arrow-agnostic core compression function.
///
/// This version follows a clean, modular structure. It acts as a high-level
/// coordinator, delegating specific tasks like preprocessing, null handling, and
/// main data compression to focused helper functions.
pub fn compress_chunk(input: PipelineInput) -> Result<Vec<u8>, tambakError> {
    // 1. Handle the simple case of an empty array first.
    if input.main.is_empty() {
        return compress_empty_main_data_stream(&input);
    }

    // 2. Preprocess input data (e.g., float canonicalization, bit-casting).
    // This separates the initial data transformation from the main planning logic.
    let (processed_data, mut pipeline_prefix, current_physical_type) =
        preprocess_input_data(&input)?;

    // 3. Delegate the entire main data planning process to the new helper.
    // This call now encapsulates the dense-vs-sparse decision.
    let main_data_pipeline =
        plan_main_data_pipeline(&processed_data, &input, current_physical_type)?;

    // 4. Plan and compress the null mask, if it exists.
    let (mut final_pipeline, mut compressed_streams) =
        plan_and_compress_null_stream(&input, &mut pipeline_prefix)?;

    // Append the WINNING main pipeline to the plan.
    final_pipeline.extend(main_data_pipeline.iter().cloned());

    // 5. Execute the main data compression based on the chosen pipeline.
    // This logic correctly handles both sparse and dense outcomes from step 3.
    if let Some(Operation::Sparsify {
        mask_stream_id,
        mask_pipeline,
        values_pipeline,
    }) = main_data_pipeline.get(0).cloned()
    {
        // Execute the sparse path
        let (mask_vec, dense_values_bytes) =
            kernels::dispatch_split_stream(&processed_data, current_physical_type)?;
        let mask_bytes: Vec<u8> = mask_vec.iter().map(|&b| b as u8).collect();

        let compressed_mask = executor::execute_linear_encode_pipeline(
            &mask_bytes,
            TambakDataType::Boolean,
            &mask_pipeline,
        )?;
        compressed_streams.insert(mask_stream_id, compressed_mask);

        let compressed_values = executor::execute_linear_encode_pipeline(
            &dense_values_bytes,
            current_physical_type,
            &values_pipeline,
        )?;
        compressed_streams.insert("main".to_string(), compressed_values);
    } else {
        // Execute the dense path
        let compressed_main = executor::execute_linear_encode_pipeline(
            &processed_data,
            current_physical_type,
            &main_data_pipeline,
        )?;
        compressed_streams.insert("main".to_string(), compressed_main);
    }

    // 6. Assemble and serialize the final artifact.
    let final_plan_struct = ChunkPlan {
        plan_version: 1,
        initial_type: input.initial_dtype,
        pipeline: final_pipeline,
    };

    let artifact = crate::chunk_pipeline::artifact::CompressedChunk {
        total_rows: input.total_rows as u64,
        original_type: final_plan_struct.initial_type.to_string(),
        plan_json: serde_json::to_string(&final_plan_struct)?,
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
pub fn decompress_chunk(bytes: &[u8]) -> Result<PipelineOutput, tambakError> {
    // 1. Deserialization: The Orchestrator's core responsibility.
    let artifact = CompressedChunk::from_bytes(bytes)?;
    let plan: ChunkPlan = serde_json::from_str(&artifact.plan_json)?;
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
) -> Result<(usize, usize, String, String), tambakError> {
    let artifact = CompressedChunk::from_bytes(bytes)?;
    let data_size = artifact.compressed_streams.values().map(|v| v.len()).sum();
    let header_size = bytes.len() - data_size;
    let plan: ChunkPlan = serde_json::from_str(&artifact.plan_json)?;
    let pretty_plan = serde_json::to_string(&plan)?;
    Ok((header_size, data_size, pretty_plan, artifact.original_type))
}
