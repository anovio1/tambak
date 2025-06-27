//! This module contains the core orchestration logic for the Phoenix compression
//! and decompression pipelines.
//!
//! It is responsible for parsing a pipeline definition (provided as a JSON string
//! from the Python layer) and executing the corresponding sequence of kernel
//! operations from the `compression` modules. This module is the "foreman" that
//! directs the work of the stateless kernels.

use pyo3::PyResult;
use serde_json::Value;

use crate::compression::{delta, rle, zigzag, zstd, leb128, bitpack, shuffle};
use crate::error::PhoenixError;

//==================================================================================
// 1. Pipeline Execution Logic
//==================================================================================

/// Executes a compression pipeline as defined by a JSON configuration.
///
/// This function takes a raw byte buffer and a pipeline definition, then
/// sequentially applies each specified transformation kernel.
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
) -> PyResult<Vec<u8>> {
    let pipeline: Vec<Value> = serde_json::from_str(pipeline_json)
        .map_err(|e| PhoenixError::UnsupportedType(format!("Invalid pipeline JSON: {}", e)))?;

    let mut current_bytes = bytes.to_vec();
    let mut current_type = original_type.to_string();

    for step in pipeline.iter() {
        let op = step["op"].as_str().ok_or_else(|| PhoenixError::UnsupportedType("Missing 'op' in pipeline step".to_string()))?;
        let params = step.get("params").unwrap_or(&Value::Null);

        current_bytes = match op {
            "delta" => {
                let order = params["order"].as_u64().unwrap_or(1) as usize;
                delta::encode(¤t_bytes, order, ¤t_type)?
            }
            "rle" => rle::encode(¤t_bytes, ¤t_type)?,
            "zigzag" => {
                let result = zigzag::encode(¤t_bytes, ¤t_type)?;
                // Zigzag changes the effective type from signed to unsigned
                current_type = current_type.replace("Int", "UInt");
                result
            }
            "leb128" => leb128::encode(¤t_bytes, ¤t_type)?,
            "bitpack" => {
                let bit_width = params["bit_width"].as_u64().unwrap_or(0) as u8;
                bitpack::encode(¤t_bytes, ¤t_type, bit_width)?
            }
            "shuffle" => shuffle::shuffle_bytes(¤t_bytes, ¤t_type)?,
            "zstd" => {
                let level = params["level"].as_i64().unwrap_or(3) as i32;
                zstd::compress(¤t_bytes, level)?
            }
            _ => return Err(PhoenixError::UnsupportedType(format!("Unsupported pipeline op: {}", op)).into()),
        };
    }

    Ok(current_bytes)
}

/// Executes a decompression pipeline in reverse order.
///
/// This function takes a compressed byte buffer and a pipeline definition, then
/// sequentially applies the *inverse* of each transformation kernel.
///
/// # Args
/// * `bytes`: The compressed byte buffer.
/// * `original_type`: The final, target data type string.
/// * `pipeline_json`: The JSON string used for encoding.
/// * `num_values`: The number of values in the original data, required for some decoders.
///
/// # Returns
/// A `Result` containing the fully reconstructed, original byte buffer.
pub fn execute_decompress_pipeline(
    bytes: &[u8],
    original_type: &str,
    pipeline_json: &str,
    num_values: usize,
) -> PyResult<Vec<u8>> {
    let pipeline: Vec<Value> = serde_json::from_str(pipeline_json)
        .map_err(|e| PhoenixError::UnsupportedType(format!("Invalid pipeline JSON: {}", e)))?;

    let mut current_bytes = bytes.to_vec();
    
    // We need to determine the type *before* decompression starts.
    // The type changes as we go backwards through the pipeline.
    let mut type_stack: Vec<String> = vec![original_type.to_string()];
    for step in pipeline.iter() {
        let op = step["op"].as_str().unwrap_or("");
        if op == "zigzag" {
            let last_type = type_stack.last().unwrap();
            type_stack.push(last_type.replace("Int", "UInt"));
        } else {
            type_stack.push(type_stack.last().unwrap().clone());
        }
    }

    for step in pipeline.iter().rev() {
        let op = step["op"].as_str().ok_or_else(|| PhoenixError::UnsupportedType("Missing 'op' in pipeline step".to_string()))?;
        let params = step.get("params").unwrap_or(&Value::Null);
        let current_type = type_stack.pop().unwrap();

        current_bytes = match op {
            "delta" => {
                let order = params["order"].as_u64().unwrap_or(1) as usize;
                delta::decode(¤t_bytes, order, ¤t_type)?
            }
            "rle" => rle::decode(¤t_bytes, ¤t_type)?,
            "zigzag" => {
                // The input to zigzag::decode is unsigned, but the `original_type`
                // refers to the final *signed* type.
                zigzag::decode(¤t_bytes, ¤t_type)?
            }
            "leb128" => leb128::decode(¤t_bytes, ¤t_type)?,
            "bitpack" => {
                let bit_width = params["bit_width"].as_u64().unwrap_or(0) as u8;
                bitpack::decode(¤t_bytes, ¤t_type, bit_width, num_values)?
            }
            "shuffle" => shuffle::shuffle_bytes(¤t_bytes, ¤t_type)?,
            "zstd" => {
                zstd::decompress(¤t_bytes)?
            }
            _ => return Err(PhoenixError::UnsupportedType(format!("Unsupported pipeline op: {}", op)).into()),
        };
    }

    Ok(current_bytes)
}

//==================================================================================
// 2. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::typed_slice_to_bytes;

    #[test]
    fn test_full_pipeline_roundtrip() {
        let original_data: Vec<i64> = vec![100, 110, 110, 90, 105];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let original_type = "Int64";
        let num_values = original_data.len();

        let pipeline_json = r#"[
            {"op": "delta", "params": {"order": 1}},
            {"op": "zigzag"},
            {"op": "zstd", "params": {"level": 5}}
        ]"#;

        let compressed_bytes = execute_compress_pipeline(
            &original_bytes,
            original_type,
            pipeline_json
        ).unwrap();

        // Assert that some compression happened
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
    fn test_rle_pipeline_roundtrip() {
        let original_data: Vec<u32> = vec![7, 7, 7, 7, 7, 8, 8, 7];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let original_type = "UInt32";
        let num_values = original_data.len();

        let pipeline_json = r#"[
            {"op": "rle"},
            {"op": "zstd", "params": {"level": 1}}
        ]"#;

        let compressed_bytes = execute_compress_pipeline(
            &original_bytes,
            original_type,
            pipeline_json
        ).unwrap();

        let decompressed_bytes = execute_decompress_pipeline(
            &compressed_bytes,
            original_type,
            pipeline_json,
            num_values
        ).unwrap();

        assert_eq!(original_bytes, decompressed_bytes);
    }

    #[test]
    fn test_invalid_pipeline_op() {
        let original_data: Vec<i32> = vec![1, 2, 3];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let pipeline_json = r#"[{"op": "not_a_real_op"}]"#;

        let result = execute_compress_pipeline(&original_bytes, "Int32", pipeline_json);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported pipeline op"));
    }
}