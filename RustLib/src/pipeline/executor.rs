//! This module contains the core execution logic for the Phoenix compression
//! and decompression pipelines.
//!
//! It acts as the "Foreman," taking a pre-defined pipeline plan (from the `planner`)
//! and executing it by calling the unified `dispatch` functions in the `kernels`
//! module. It is designed for maximum performance by minimizing memory allocations
//! through a buffer-swapping strategy. This module is PURE RUST.

use serde_json::Value;

use crate::error::PhoenixError;
use crate::kernels; // The executor's only dependency is the top-level kernels module.

//==================================================================================
// 1. Pipeline Execution Logic (REVISED - Simplified & Corrected)
//==================================================================================

/// Executes a compression pipeline using a memory-efficient buffer-swapping strategy.
///
/// This function takes a raw byte buffer and a pipeline definition, then
/// sequentially calls the `kernels::dispatch_encode` function for each step,
/// swapping between two pre-allocated buffers to avoid allocations in the hot loop.
///
/// # Args
/// * `bytes`: The raw, validity-stripped byte buffer of the original data.
/// * `original_type`: The string representation of the data's type (e.g., "Int64").
/// * `pipeline_json`: A JSON string representing a `List[PipelineOperation]`.
///
/// # Returns
/// A `Result` containing the fully compressed byte vector.
pub fn execute_compress_pipeline(
    bytes: &[u8],
    original_type: &str,
    pipeline_json: &str,
) -> Result<Vec<u8>, PhoenixError> {
    let pipeline: Vec<Value> = serde_json::from_str(pipeline_json)
        .map_err(|e| PhoenixError::UnsupportedType(format!("Invalid pipeline JSON: {}", e)))?;

    if pipeline.is_empty() {
        return Ok(bytes.to_vec());
    }

    let mut buffer_a = bytes.to_vec();
    let mut buffer_b = Vec::with_capacity(buffer_a.len()); // Initial capacity
    
    let mut input_buf = &mut buffer_a;
    let mut output_buf = &mut buffer_b;

    let mut current_type = original_type.to_string();

    for op_config in pipeline.iter() {
        output_buf.clear();

        // The executor's ONLY job is to call the dispatcher.
        // The dispatcher handles all the type casting and kernel-specific logic.
        kernels::dispatch_encode(op_config, input_buf, output_buf, &current_type)?;
        
        // If the operation changed the effective type (only zigzag does this), update it.
        if op_config["op"].as_str() == Some("zigzag") {
            current_type = current_type.replace("Int", "UInt");
        }
        
        std::mem::swap(&mut input_buf, &mut output_buf);
    }

    Ok(input_buf.to_vec())
}

/// Executes a decompression pipeline in reverse order using buffer-swapping.
pub fn execute_decompress_pipeline(
    bytes: &[u8],
    original_type: &str,
    pipeline_json: &str,
    num_values: usize,
) -> Result<Vec<u8>, PhoenixError> {
    let pipeline: Vec<Value> = serde_json::from_str(pipeline_json)
        .map_err(|e| PhoenixError::UnsupportedType(format!("Invalid pipeline JSON: {}", e)))?;

    if pipeline.is_empty() {
        return Ok(bytes.to_vec());
    }

    // --- Build the type stack robustly ---
    let mut type_stack: Vec<String> = vec![original_type.to_string()];
    for step in pipeline.iter() {
        let op = step["op"].as_str().unwrap_or("");
        let last_type = type_stack.last().ok_or_else(|| PhoenixError::UnsupportedType("Type stack logic error".to_string()))?;
        if op == "zigzag" {
            type_stack.push(last_type.replace("Int", "UInt"));
        } else {
            type_stack.push(last_type.clone());
        }
    }

    let mut buffer_a = bytes.to_vec();
    let mut buffer_b = Vec::with_capacity(bytes.len() * 2); // Decompression can expand

    let mut input_buf = &mut buffer_a;
    let mut output_buf = &mut buffer_b;

    for op_config in pipeline.iter().rev() {
        let current_type = type_stack.pop().ok_or_else(|| PhoenixError::UnsupportedType("Type stack desync during decompression".to_string()))?;
        
        output_buf.clear();

        // The executor's ONLY job is to call the dispatcher.
        kernels::dispatch_decode(op_config, input_buf, output_buf, &current_type, num_values)?;
        
        std::mem::swap(&mut input_buf, &mut output_buf);
    }

    Ok(input_buf.to_vec())
}

//==================================================================================
// 2. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::typed_slice_to_bytes;

    #[test]
    fn test_executor_full_pipeline_roundtrip() {
        let original_data: Vec<i64> = vec![100, 110, 110, 90, 105];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let original_type = "Int64";
        let num_values = original_data.len();

        // The planner would generate this JSON.
        let pipeline_json = r#"[
            {"op": "delta", "params": {"order": 1}},
            {"op": "zigzag"},
            {"op": "shuffle"},
            {"op": "zstd", "params": {"level": 5}}
        ]"#;

        let compressed_bytes = execute_compress_pipeline(
            &original_bytes,
            original_type,
            pipeline_json
        ).unwrap();

        assert!(compressed_bytes.len() < original_bytes.len());

        let decompressed_bytes = execute_decompress_pipeline(
            &compressed_bytes,
            original_type,
            pipeline_json,
            num_values
        ).unwrap();

        assert_eq!(original_bytes, decompressed_bytes);
    }
    
    #[test]
    fn test_executor_empty_pipeline() {
        let original_data: Vec<i32> = vec![1, 2, 3];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let pipeline_json = "[]"; // Empty pipeline

        let compressed_bytes = execute_compress_pipeline(&original_bytes, "Int32", pipeline_json).unwrap();
        // With an empty pipeline, the data should be identical (just copied).
        assert_eq!(compressed_bytes, original_bytes);

        let decompressed_bytes = execute_decompress_pipeline(&compressed_bytes, "Int32", pipeline_json, 3).unwrap();
        assert_eq!(decompressed_bytes, original_bytes);
    }
}