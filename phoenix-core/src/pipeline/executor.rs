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
// 1. Pipeline Execution Logic
//==================================================================================

/// Executes a compression pipeline using a memory-efficient buffer-swapping strategy.
///
/// This function takes a raw byte buffer and a pipeline definition, then
/// sequentially calls the `kernels::dispatch_encode` function for each step.
/// It tracks the logical type of the data as it passes through the pipeline.
pub fn execute_compress_pipeline(
    bytes: &[u8],
    original_type: &str,
    pipeline_json: &str,
) -> Result<Vec<u8>, PhoenixError> {
    let pipeline: Vec<Value> = serde_json::from_str(pipeline_json)
        .map_err(|e| PhoenixError::InternalError(format!("Invalid pipeline JSON: {}", e)))?;

    if pipeline.is_empty() {
        return Ok(bytes.to_vec());
    }

    let mut buffer_a = bytes.to_vec();
    let mut buffer_b = Vec::with_capacity(buffer_a.len());

    let mut input_buf = &mut buffer_a;
    let mut output_buf = &mut buffer_b;

    // This is the key to the v4.0 executor: tracking the data's logical type.
    let mut current_type = original_type.to_string();

    for op_config in pipeline.iter() {
        #[cfg(debug_assertions)]
        {
            // --- ADD THIS CHECKPOINT ---
            let op_name = op_config["op"].as_str().unwrap_or("unknown");
            println!("\n[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter");
            println!("  - Executing Op: {}", op_config);
            println!("  - Current Type: {}", &current_type);
            println!(
                "  - Input Buf (first 16 bytes): {:?}",
                &input_buf.get(..16.min(input_buf.len()))
            );
            println!(
                "  - [DEBUG] Op: {}, Input Buf Length: {}, Output Buf Length: {}",
                op_name,
                input_buf.len(),
                output_buf.len()
            );

            // If this op is bitpack, print the max value of the input buffer to diagnose errors
            if op_name == "bitpack" {
                use bytemuck::cast_slice;

                // Convert input_buf bytes to typed slice for the current_type
                match current_type.as_str() {
                    "UInt8" => {
                        let typed_input: &[u8] = cast_slice(input_buf);
                        let max_val = typed_input.iter().copied().max().unwrap_or(0);
                        println!("   [DEBUG] Bitpack input max value (u8): {}", max_val);
                    }
                    "UInt16" => {
                        let typed_input: &[u16] = cast_slice(input_buf);
                        let max_val = typed_input.iter().copied().max().unwrap_or(0);
                        println!("   [DEBUG] Bitpack input max value (u16): {}", max_val);
                    }
                    "UInt32" => {
                        let typed_input: &[u32] = cast_slice(input_buf);
                        let max_val = typed_input.iter().copied().max().unwrap_or(0);
                        println!("   [DEBUG] Bitpack input max value (u32): {}", max_val);
                    }
                    "UInt64" => {
                        let typed_input: &[u64] = cast_slice(input_buf);
                        let max_val = typed_input.iter().copied().max().unwrap_or(0);
                        println!("   [DEBUG] Bitpack input max value (u64): {}", max_val);
                    }
                    "Int8" => {
                        let typed_input: &[i8] = cast_slice(input_buf);
                        let max_val = typed_input.iter().copied().max().unwrap_or(0);
                        println!("   [DEBUG] Bitpack input max value (i8): {}", max_val);
                    }
                    "Int16" => {
                        let typed_input: &[i16] = cast_slice(input_buf);
                        let max_val = typed_input.iter().copied().max().unwrap_or(0);
                        println!("   [DEBUG] Bitpack input max value (i16): {}", max_val);
                    }
                    "Int32" => {
                        let typed_input: &[i32] = cast_slice(input_buf);
                        let max_val = typed_input.iter().copied().max().unwrap_or(0);
                        println!("   [DEBUG] Bitpack input max value (i32): {}", max_val);
                    }
                    "Int64" => {
                        let typed_input: &[i64] = cast_slice(input_buf);
                        let max_val = typed_input.iter().copied().max().unwrap_or(0);
                        println!("   [DEBUG] Bitpack input max value (i64): {}", max_val);
                    }
                    _ => {
                        println!(
                            "   [DEBUG] Bitpack op with unsupported current_type: {}",
                            current_type
                        );
                    }
                }
            }
            // --- END CHECKPOINT ---
        }

        output_buf.clear();

        kernels::dispatch_encode(op_config, input_buf, output_buf, &current_type)?;

        // After an operation, update the current type of the data in the buffer.
        // This is critical for transforms that change the data's signedness.
        if let Some(op_name) = op_config["op"].as_str() {
            if op_name == "zigzag" {
                current_type = current_type.replace("Int", "UInt");
            }
            // Other ops (delta, rle, etc.) do not change the fundamental type.
        }

        #[cfg(debug_assertions)]
        {
            println!(
                "   - [DEBUG] Before swap: input_buf.len() = {}, output_buf.len() = {}",
                input_buf.len(),
                output_buf.len()
            );
        }
        std::mem::swap(&mut input_buf, &mut output_buf);
        #[cfg(debug_assertions)]
        {
            println!(
                "   - [DEBUG] After swap: input_buf.len() = {}, output_buf.len() = {}",
                input_buf.len(),
                output_buf.len()
            );
        }
    }

    Ok(input_buf.to_vec())
}

/// Executes a decompression pipeline in reverse order using buffer-swapping.
///
/// This function is more complex than compression because it must reconstruct the
/// type of the data at each stage of the reversed pipeline.

// This function replaces the existing one in `src/pipeline/executor.rs`

pub fn execute_decompress_pipeline(
    bytes: &[u8],
    intermediate_type: &str,
    pipeline_json: &str,
    num_values: usize,
) -> Result<Vec<u8>, PhoenixError> {
    // THIS IS THE FIX: Manually map the serde_json::Error to our PhoenixError.
    let pipeline: Vec<Value> = serde_json::from_str(pipeline_json)
        .map_err(|e| PhoenixError::InternalError(format!("Invalid pipeline JSON: {}", e)))?;

    if pipeline.is_empty() {
        return Ok(bytes.to_vec());
    }

    let mut original_type = intermediate_type.to_string();
    for op_config in pipeline.iter().rev() {
        if let Some("zigzag") = op_config["op"].as_str() {
            if original_type.starts_with("UInt") {
                original_type = original_type.replace("UInt", "Int");
            }
        }
    }

    let mut type_stack: Vec<String> = Vec::with_capacity(pipeline.len());
    let mut current_type = original_type;
    for op_config in pipeline.iter() {
        type_stack.push(current_type.clone());
        if let Some("zigzag") = op_config["op"].as_str() {
            if current_type.starts_with("Int") {
                current_type = current_type.replace("Int", "UInt");
            }
        }
    }

    let mut buffer_a = bytes.to_vec();
    let mut buffer_b = Vec::with_capacity(bytes.len() * 2);
    let mut input_buf = &mut buffer_a;
    let mut output_buf = &mut buffer_b;

    for op_config in pipeline.iter().rev() {
        let type_for_decode = type_stack.pop().ok_or_else(|| {
            PhoenixError::InternalError("Type stack desync during decompression".to_string())
        })?;

        output_buf.clear();
        kernels::dispatch_decode(
            op_config,
            input_buf,
            output_buf,
            &type_for_decode,
            num_values,
        )?;
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

        // A valid plan generated by our planner for signed integers.
        let pipeline_json = r#"[
            {"op": "delta", "params": {"order": 1}},
            {"op": "zigzag"},
            {"op": "shuffle"},
            {"op": "zstd", "params": {"level": 3}}
        ]"#;

        let compressed_bytes =
            execute_compress_pipeline(&original_bytes, original_type, pipeline_json).unwrap();

        // 2. Decompress
        // The intermediate type for decompression is the type *before* the first
        // decode op (zstd), which is the type *after* the last encode op (shuffle).
        // Our type stack logic correctly handles the transformations from there.
        let decompressed_bytes = execute_decompress_pipeline(
            &compressed_bytes,
            "UInt64", // The type after zigzag is unsigned.
            pipeline_json,
            num_values,
        )
        .unwrap();

        assert_eq!(original_bytes, decompressed_bytes);
    }

    #[test]
    fn test_executor_empty_pipeline() {
        let original_data: Vec<i32> = vec![1, 2, 3];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let pipeline_json = "[]"; // Empty pipeline

        let compressed_bytes =
            execute_compress_pipeline(&original_bytes, "Int32", pipeline_json).unwrap();
        // With an empty pipeline, the data should be identical (just copied).
        assert_eq!(compressed_bytes, original_bytes);

        let decompressed_bytes =
            execute_decompress_pipeline(&compressed_bytes, "Int32", pipeline_json, 3).unwrap();
        assert_eq!(decompressed_bytes, original_bytes);
    }

    // These checkpoint tests are invaluable for debugging the flow of data
    // and types between kernels.
    #[test]
    fn checkpoint_06_after_delta() {
        let data: Vec<i16> = vec![100, 101, 103, 102];
        let bytes = bytemuck::cast_slice::<i16, u8>(&data);
        let mut output_buf = Vec::new();
        crate::kernels::delta::encode::<i16>(bytemuck::cast_slice(bytes), &mut output_buf, 1)
            .unwrap();
        let result_vec: Vec<i16> = bytemuck::cast_slice(&output_buf).to_vec();

        println!("\n--- CHECKPOINT 06: AFTER DELTA ---");
        dbg!(&result_vec); // Should be [100, 1, 2, -1]
        println!("----------------------------------\n");
        assert_eq!(result_vec, vec![100, 1, 2, -1]);
    }

    #[test]
    fn checkpoint_after_delta() {
        let data: Vec<i16> = vec![100, 101, 103, 102];
        let bytes = bytemuck::cast_slice::<i16, u8>(&data);
        let pipeline = r#"[{"op": "delta", "params": {"order": 1}}]"#;

        let output_bytes = execute_compress_pipeline(bytes, "Int16", pipeline).unwrap();
        let result_vec: &[i16] = bytemuck::cast_slice(&output_bytes);

        assert_eq!(result_vec, &[100, 1, 2, -1]);
    }

    #[test]
    fn checkpoint_after_zigzag() {
        let data: Vec<i16> = vec![100, 1, 2, -1]; // Output from delta
        let bytes = bytemuck::cast_slice::<i16, u8>(&data);
        let pipeline = r#"[{"op": "zigzag"}]"#;

        // The executor starts with Int16, and the zigzag op will change the
        // internal `current_type` to UInt16.
        let output_bytes = execute_compress_pipeline(bytes, "Int16", pipeline).unwrap();
        let result_vec: &[u16] = bytemuck::cast_slice(&output_bytes);

        assert_eq!(result_vec, &[200, 2, 4, 1]);
    }

    #[test]
    fn checkpoint_07_after_zigzag() {
        let data: Vec<i16> = vec![100, 1, 2, -1]; // Output from previous step
        let mut output_buf = Vec::new();
        crate::kernels::zigzag::encode(&data, &mut output_buf).unwrap();
        let result_vec: Vec<u16> = bytemuck::cast_slice(&output_buf).to_vec();

        println!("\n--- CHECKPOINT 07: AFTER ZIGZAG ---");
        dbg!(&result_vec); // Should be [200, 2, 4, 1]
        println!("-----------------------------------\n");
        assert_eq!(result_vec, vec![200, 2, 4, 1]);
    }

    #[test]
    fn checkpoint_09_after_bitpack() {
        // NOTE: Shuffle output is raw bytes, bitpack input is typed.
        // This checkpoint tests the bitpack kernel in isolation.
        let data: Vec<u16> = vec![200, 2, 4, 1]; // Zigzag output
        let bit_width = 8; // Planner would calculate this (for 200)
        let mut output_buf = Vec::new();
        crate::kernels::bitpack::encode(&data, &mut output_buf, bit_width).unwrap();

        println!("\n--- CHECKPOINT 09: AFTER BITPACK ---");
        dbg!(&output_buf);
        println!("------------------------------------\n");
        assert!(!output_buf.is_empty());
    }
    #[test]
    fn checkpoint_10_after_zstd() {
        // 1. Define the original data
        let data = b"some intermediate byte stream";

        // 2. Compress it
        let mut compressed_buf = Vec::new();
        crate::kernels::zstd::encode(data, &mut compressed_buf, 3).unwrap();

        // 3. Decompress it
        let mut decompressed_buf = Vec::new();
        crate::kernels::zstd::decode(&compressed_buf, &mut decompressed_buf).unwrap();

        // 4. Assert that the decompressed data is identical to the original.
        // This is the correct way to test for correctness.
        assert_eq!(data, decompressed_buf.as_slice());
    }
}
