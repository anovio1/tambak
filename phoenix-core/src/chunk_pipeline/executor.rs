// In: src/pipeline/executor.rs (FINAL, STABLE VERSION)

//! This module contains the v4.3 core execution logic for the Phoenix compression
//! and decompression pipelines.
//!
//! It acts as the "Pure Byte Engine," a non-strategic component that faithfully
//! executes a linear sequence of operations from a plan.

use crate::chunk_pipeline::models::Operation;
use crate::error::PhoenixError;
use crate::kernels;
// --- USE THE NEW TRAIT ---
use crate::chunk_pipeline::traits::{OperationBehavior, StreamTransform};
use crate::types::PhoenixDataType;
use colored::*;

// ... (your color helper functions) ...

/// Executes a linear encoding pipeline.
/// This function is updated to use the new `OperationBehavior` trait.
pub(crate) fn execute_linear_encode_pipeline(
    bytes: &[u8],
    initial_type: PhoenixDataType,
    pipeline: &[Operation],
) -> Result<Vec<u8>, PhoenixError> {
    if pipeline.is_empty() {
        return Ok(bytes.to_vec());
    }

    let mut buffer_a = bytes.to_vec();
    let mut buffer_b = Vec::with_capacity(buffer_a.len());
    let mut current_type = initial_type;

    for (i, op) in pipeline.iter().enumerate(){
        // --- START OF NEW DEBUG BLOCK ---
        #[cfg(debug_assertions)]
        {
            println!("\n");
            println!("[EXECUTOR] Executing step {}: {:?} Input Type: {:?} Input Length (bytes): {}", i, op, current_type, buffer_a.len());
            if buffer_a.len() < 64 {
                // Print small buffers to inspect them
                println!("[EXECUTOR]   Input Data (sample): {:?}", buffer_a);
            }
        }
        // --- END OF NEW DEBUG BLOCK ---
        kernels::dispatch_encode(op, &buffer_a, &mut buffer_b, current_type)?;

        // Use the new trait to determine the output type.
        let transform = op.transform_stream(current_type)?;
        current_type = match transform {
            StreamTransform::PreserveType => current_type,
            StreamTransform::TypeChange(new_type) => new_type,
            StreamTransform::Restructure { .. } => {
                return Err(PhoenixError::InternalError(
                    "Meta-operation found in linear execution path during encode.".to_string(),
                ));
            }
        };

        std::mem::swap(&mut buffer_a, &mut buffer_b);
        buffer_b.clear();
    }

    #[cfg(debug_assertions)]
    {
        println!(
            "[DEBUG] execute_linear_encode complete, initial_type={:?}, pipeline={:?}, final length={}",
            initial_type,
            pipeline,
            buffer_a.len()
        );
    }
    Ok(buffer_a)
}

/// Executes a linear decoding pipeline.
///
/// This is the original "dumb engine" implementation, restored for stability.
/// It requires a pre-calculated `type_flow` stack from the Orchestrator.
pub(crate) fn execute_linear_decode_pipeline(
    bytes: &[u8],
    type_flow: &mut Vec<PhoenixDataType>,
    pipeline: &[Operation],
    num_values: usize,
) -> Result<Vec<u8>, PhoenixError> {
    if pipeline.is_empty() {
        return Ok(bytes.to_vec());
    }
    #[cfg(debug_assertions)]
    {
        println!("\n[DEBUG] execute_linear_decode_pipeline called:");
        println!("  num_values: {}", num_values);
        println!("  Pipeline ({} ops):", pipeline.len());
        for (i, op) in pipeline.iter().enumerate() {
            println!("    [{}]: {:?}", i, op);
        }
        println!(
            "  Type flow stack (len={}): {:?}",
            type_flow.len(),
            type_flow
        );
    }

    let mut current_data = bytes.to_vec();

    let mut current_type = type_flow.pop().ok_or_else(|| {
        PhoenixError::InternalError("Type flow stack is empty at start of decode.".to_string())
    })?;

    for op in pipeline.iter().rev() {
        let target_type = type_flow.pop().ok_or_else(|| {
            PhoenixError::InternalError(format!(
                "Type flow stack exhausted prematurely while decoding op: {:?}",
                op
            ))
        })?;

        #[cfg(debug_assertions)]
        {
            println!("--------------------------------------------------");
            println!(
                "[EXECUTOR-DECODE] Op: {:?}, In-Type: {:?}, Target-Type: {:?}, In-Len: {}",
                op,
                current_type,
                target_type,
                current_data.len()
            );
        }

        let mut output_buf = Vec::new();
        kernels::dispatch_decode(
            op,
            &current_data,
            &mut output_buf,
            current_type,
            target_type,
            num_values,
        )?;

        #[cfg(debug_assertions)]
        {
            println!("[EXECUTOR-DECODE] SUCCESS -> Out-Len: {}", output_buf.len());
        }

        current_data = output_buf;
        current_type = target_type;
    }

    Ok(current_data)
}
