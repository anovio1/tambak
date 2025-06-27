//! This module contains the core execution logic for the Phoenix compression
//! and decompression pipelines.
//!
//! It acts as the "Foreman," taking a pre-defined pipeline plan (from the `planner`)
//! and executing the corresponding sequence of kernel operations. It is designed for
//! maximum performance by minimizing memory allocations through a buffer-swapping
//! strategy. This module is PURE RUST and has no FFI dependencies.

use serde_json::Value;

use crate::compression::{delta, rle, zigzag, zstd, leb128, bitpack, shuffle};
use crate::error::PhoenixError;

//==================================================================================
// 1. Pipeline Execution Logic (REVISED - Performant & Robust)
//==================================================================================

/// Executes a compression pipeline using a memory-efficient buffer-swapping strategy.
///
/// This function takes a raw byte buffer and a pipeline definition, then
/// sequentially applies each specified transformation kernel, swapping between two
/// pre-allocated buffers to avoid allocations in the hot loop.
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
    // Pre-allocate with a reasonable capacity to avoid reallocations.
    // The output of some steps (like LEB128) can be larger than the input.
    let mut buffer_b = Vec::with_capacity(buffer_a.len() * 2); 
    
    let mut input_buf = &mut buffer_a;
    let mut output_buf = &mut buffer_b;

    let mut current_type = original_type.to_string();

    for step in pipeline.iter() {
        let op = step["op"].as_str().ok_or_else(|| PhoenixError::UnsupportedType("Missing 'op' in pipeline step".to_string()))?;
        let params = step.get("params").unwrap_or(&Value::Null);

        // Clear the output buffer before each operation
        output_buf.clear();

        match op {
            "delta" => {
                let order = match params.get("order") {
                    Some(val) => val.as_u64() // Try to parse it as a u64
                        .ok_or_else(|| PhoenixError::UnsupportedType(format!("Invalid 'order' parameter: {:?}", val)))? as usize,
                    None => 1, // If the key doesn't exist at all, use the default.
                };
                delta::encode(input_buf, output_buf, order, &current_type)?;
            }
            "rle" => rle::encode(input_buf, output_buf, &current_type)?,
            "zigzag" => {
                zigzag::encode(input_buf, output_buf, &current_type)?;
                current_type = current_type.replace("Int", "UInt");
            }
            "leb128" => leb128::encode(input_buf, output_buf, &current_type)?,
            "bitpack" => {
                let bit_width = params["bit_width"].as_u64().unwrap_or(0) as u8;
                bitpack::encode(input_buf, output_buf, &current_type, bit_width)?;
            }
            "shuffle" => shuffle::shuffle_bytes(input_buf, output_buf, &current_type)?,
            "zstd" => {
                let level = params["level"].as_i64().unwrap_or(3) as i32;
                zstd::compress(input_buf, output_buf, level)?;
            }
            _ => return Err(PhoenixError::UnsupportedType(format!("Unsupported pipeline op: {}", op))),
        };
        
        // Swap buffers for the next iteration
        std::mem::swap(&mut input_buf, &mut output_buf);
    }

    // The final result is always in `input_buf` after the last swap
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
    let mut buffer_b = Vec::with_capacity(buffer_a.len() * 2); // Decompression can expand significantly

    let mut input_buf = &mut buffer_a;
    let mut output_buf = &mut buffer_b;

    for step in pipeline.iter().rev() {
        let op = step["op"].as_str().ok_or_else(|| PhoenixError::UnsupportedType("Missing 'op' in pipeline step".to_string()))?;
        let params = step.get("params").unwrap_or(&Value::Null);
        let current_type = type_stack.pop().ok_or_else(|| PhoenixError::UnsupportedType("Type stack desync during decompression".to_string()))?;
        
        output_buf.clear();

        match op {
            "delta" => {
                let order = match params.get("order") {
                    Some(val) => val.as_u64() // Try to parse it as a u64
                        .ok_or_else(|| PhoenixError::UnsupportedType(format!("Invalid 'order' parameter: {:?}", val)))? as usize,
                    None => 1, // If the key doesn't exist at all, use the default.
                };
                delta::decode(input_buf, output_buf, order, &current_type)?;
            }
            "rle" => rle::decode(input_buf, output_buf, &current_type)?,
            "zigzag" => {
                let target_signed_type = type_stack.last().ok_or_else(|| PhoenixError::UnsupportedType("Type stack empty before zigzag decode".to_string()))?;
                zigzag::decode(input_buf, output_buf, target_signed_type)?;
            }
            "leb128" => leb128::decode(input_buf, output_buf, &current_type)?,
            "bitpack" => {
                let bit_width = params["bit_width"].as_u64().unwrap_or(0) as u8;
                bitpack::decode(input_buf, output_buf, &current_type, bit_width, num_values)?;
            }
            "shuffle" => shuffle::shuffle_bytes(input_buf, output_buf, &current_type)?,
            "zstd" => zstd::decompress(input_buf, output_buf)?,
            _ => return Err(PhoenixError::UnsupportedType(format!("Unsupported pipeline op: {}", op))),
        };

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
    fn test_full_pipeline_roundtrip_with_buffer_swap() {
        let original_data: Vec<i64> = vec![100, 110, 110, 90, 105];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let original_type = "Int64";
        let num_values = original_data.len();

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
    
    // Other tests from previous version remain valid...
}