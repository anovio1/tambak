//! This module contains the v4.1 core execution logic for the Phoenix compression
//! and decompression pipelines.
//!
//! It acts as the "Pure Byte Engine," taking a map of byte streams and a unified
//! plan, and returning a map of compressed byte streams. It has no knowledge of
//! the Arrow crate.

use serde_json::Value;
use std::collections::HashMap;

use crate::error::PhoenixError;
use crate::kernels;
use crate::pipeline::artifact::CompressedChunk;

// This helper is unchanged and correct.
pub(crate) fn execute_linear_encode_pipeline(
    bytes: &[u8],
    type_str: &str,
    pipeline: &[Value],
) -> Result<(Vec<u8>, String), PhoenixError> {
    if pipeline.is_empty() {
        return Ok((bytes.to_vec(), type_str.to_string()));
    }

    #[cfg(debug_assertions)]
    println!(
        "[CHECKPOINT] Encode pipeline start: input bytes len = {}, input type = {}",
        bytes.len(),
        type_str
    );

    let mut buffer_a = bytes.to_vec();
    let mut buffer_b = Vec::with_capacity(buffer_a.len());
    let mut current_type = type_str.to_string();

    for op_config in pipeline {
        let input_type_for_op = current_type.clone();

        if let Some("BitCast") = op_config["op"].as_str() {
            current_type = op_config["to_type"]
                .as_str()
                .ok_or_else(|| {
                    PhoenixError::InternalError(
                        "BitCast op in plan is missing 'to_type' field".into(),
                    )
                })?
                .to_string();
        } else if let Some("zigzag") = op_config["op"].as_str() {
            current_type = current_type.replace("Int", "UInt");
        }

        kernels::dispatch_encode(op_config, &buffer_a, &mut buffer_b, &input_type_for_op)?;

        std::mem::swap(&mut buffer_a, &mut buffer_b);
        buffer_b.clear();
    }

    #[cfg(debug_assertions)]
    println!(
        "[CHECKPOINT] Encode pipeline end: output bytes len = {}, output type = {}",
        buffer_a.len(),
        current_type
    );

    Ok((buffer_a, current_type))
}

// --- THIS IS THE CORRECTED FUNCTION ---
pub(crate) fn execute_linear_decode_pipeline(
    bytes: &[u8],
    initial_type_before_encode: &str,
    pipeline: &[Value],
    num_values: usize,
) -> Result<Vec<u8>, PhoenixError> {
    #[cfg(debug_assertions)]
    println!(
        "[CHECKPOINT] Decode pipeline start: input bytes len = {}, initial type = {}",
        bytes.len(),
        initial_type_before_encode
    );
    if pipeline.is_empty() {
        return Ok(bytes.to_vec());
    }

    // 1. Build a list of the input types for each ENCODE stage.
    // The output of a decode step must be the same as the input to the corresponding encode step.
    let mut encode_input_types: Vec<String> = Vec::with_capacity(pipeline.len());
    let mut current_type = initial_type_before_encode.to_string();
    for op_config in pipeline.iter() {
        encode_input_types.push(current_type.clone());
        // Update type for the *next* stage's input
        if let Some("zigzag") = op_config["op"].as_str() {
            current_type = current_type.replace("Int", "UInt");
        } else if let Some("BitCast") = op_config["op"].as_str() {
            current_type = op_config["to_type"]
                .as_str()
                .ok_or_else(|| {
                    PhoenixError::InternalError("BitCast in plan missing 'to_type'".into())
                })?
                .to_string();
        }
    }

    // 2. Now execute the decode.
    let mut buffer_a = bytes.to_vec();
    let mut buffer_b = Vec::with_capacity(buffer_a.len());

    // We iterate the pipeline and the pre-calculated types in reverse.
    for (op_config, required_output_type) in
        pipeline.iter().rev().zip(encode_input_types.iter().rev())
    {
        #[cfg(debug_assertions)]
        {
            println!("\n[CHECKPOINT] DECODE DISPATCH");
            println!("  - Op: {}", op_config["op"].as_str().unwrap_or("N/A"));
            println!("  - Input Bytes Len: {}", buffer_a.len());
            println!("  - Target Output Type: {}", required_output_type);
            println!("  - Num Values: {}", num_values);
        }

        kernels::dispatch_decode(
            op_config,
            &buffer_a,
            &mut buffer_b,
            required_output_type,
            num_values,
        )?;
        std::mem::swap(&mut buffer_a, &mut buffer_b);
        buffer_b.clear();
    }

    #[cfg(debug_assertions)]
    println!(
        "[CHECKPOINT] Decode pipeline end: output bytes len = {}",
        buffer_a.len()
    );

    Ok(buffer_a)
}

pub fn execute_plan(
    mut initial_streams: HashMap<String, Vec<u8>>,
    plan_json: &str,
    initial_type: &str,
    _total_rows: usize,
) -> Result<HashMap<String, Vec<u8>>, PhoenixError> {
    let plan: Vec<Value> = serde_json::from_str(plan_json)?;
    let mut compressed_streams = HashMap::new();
    let mut main_data_bytes = initial_streams.remove("main").unwrap_or_default();
    let mut current_type = initial_type.to_string();

    let mut main_pipeline: Vec<Value> = Vec::new();

    for op_config in &plan {
        match op_config["op"].as_str() {
            Some("ExtractNulls") => {
                let stream_id = op_config["output_stream_id"].as_str().ok_or_else(|| {
                    PhoenixError::InternalError("ExtractNulls op missing 'output_stream_id'".into())
                })?;
                if let Some(null_mask_bytes) = initial_streams.get(stream_id) {
                    let pipeline = op_config["pipeline"].as_array().ok_or_else(|| {
                        PhoenixError::InternalError("ExtractNulls op missing 'pipeline'".into())
                    })?;
                    let (compressed_nulls, _) =
                        execute_linear_encode_pipeline(null_mask_bytes, "Boolean", pipeline)?;
                    compressed_streams.insert(stream_id.to_string(), compressed_nulls);
                }
            }
            Some("Sparsify") => {
                (main_data_bytes, current_type) = execute_linear_encode_pipeline(
                    &main_data_bytes,
                    &current_type,
                    &main_pipeline,
                )?;
                main_pipeline.clear();

                let mask_pipeline = op_config["mask_pipeline"].as_array().ok_or_else(|| {
                    PhoenixError::InternalError("Sparsify op missing 'mask_pipeline'".into())
                })?;
                let values_pipeline = op_config["values_pipeline"].as_array().ok_or_else(|| {
                    PhoenixError::InternalError("Sparsify op missing 'values_pipeline'".into())
                })?;
                let mask_stream_id = op_config["mask_stream_id"].as_str().ok_or_else(|| {
                    PhoenixError::InternalError("Sparsify op missing 'mask_stream_id'".into())
                })?;

                let (mask_vec, dense_values_bytes) =
                    kernels::dispatch_split_stream(&main_data_bytes, &current_type)?;

                let mask_bytes: Vec<u8> = mask_vec.iter().map(|&b| b as u8).collect();
                let (compressed_mask, _) =
                    execute_linear_encode_pipeline(&mask_bytes, "Boolean", mask_pipeline)?;
                compressed_streams.insert(mask_stream_id.to_string(), compressed_mask);

                main_data_bytes = dense_values_bytes;
                main_pipeline.extend_from_slice(values_pipeline);
            }
            _ => {
                main_pipeline.push(op_config.clone());
            }
        }
    }

    (main_data_bytes, current_type) =
        execute_linear_encode_pipeline(&main_data_bytes, &current_type, &main_pipeline)?;

    compressed_streams.insert("main".to_string(), main_data_bytes);

    Ok(compressed_streams)
}

pub fn decompress_plan(
    bytes: &[u8],
    num_valid_rows: usize,
) -> Result<(HashMap<String, Vec<u8>>, String, u64), PhoenixError> {
    let artifact = CompressedChunk::from_bytes(bytes)?;
    let full_plan: Vec<Value> = serde_json::from_str(&artifact.plan_json)?;

    let mut streams = artifact.compressed_streams;
    let mut main_data = streams
        .remove("main")
        .ok_or_else(|| PhoenixError::InternalError("Missing 'main' data stream".into()))?;

    let total_rows = artifact.total_rows as usize;
    let mut linear_pipeline: Vec<Value> = Vec::new();

    // We iterate the full plan in reverse to handle nested pipelines correctly.
    for op_config in full_plan.iter().rev() {
        match op_config["op"].as_str() {
            Some("ExtractNulls") => {} // Handled at the end.
            Some("Sparsify") => {
                // This op is a branch. We need to decompress its sub-streams first.
                let values_pipeline = op_config["values_pipeline"].as_array().ok_or_else(|| {
                    PhoenixError::InternalError("Sparsify op missing 'values_pipeline'".into())
                })?;
                let mask_stream_id = op_config["mask_stream_id"].as_str().ok_or_else(|| {
                    PhoenixError::InternalError("Sparsify op missing 'mask_stream_id'".into())
                })?;
                let compressed_mask = streams.get(mask_stream_id).ok_or_else(|| {
                    PhoenixError::InternalError(format!(
                        "Missing stream '{}' for Sparsify",
                        mask_stream_id
                    ))
                })?;
                let mask_pipeline = op_config["mask_pipeline"].as_array().ok_or_else(|| {
                    PhoenixError::InternalError("Sparsify op missing 'mask_pipeline'".into())
                })?;

                // Calculate the initial type for the values_pipeline by tracing the full plan.
                let mut initial_type_for_values = artifact.original_type.clone();
                for op in &full_plan {
                    if op == op_config {
                        break;
                    } // Stop when we reach the Sparsify op
                    if let Some("zigzag") = op["op"].as_str() {
                        initial_type_for_values = initial_type_for_values.replace("Int", "UInt");
                    } else if let Some("BitCast") = op["op"].as_str() {
                        initial_type_for_values = op["to_type"].as_str().unwrap().to_string();
                    }
                }

                // Decompress the two sub-streams.
                let decompressed_mask_bytes = execute_linear_decode_pipeline(
                    compressed_mask,
                    "Boolean",
                    mask_pipeline,
                    num_valid_rows,
                )?;
                let mask_vec: Vec<bool> = decompressed_mask_bytes.iter().map(|b| *b != 0).collect();
                let num_non_zero = mask_vec.iter().filter(|&&b| b).count();

                let decompressed_values = execute_linear_decode_pipeline(
                    &main_data,
                    &initial_type_for_values,
                    values_pipeline,
                    num_non_zero,
                )?;

                // Reconstruct the sparse data, which becomes the new `main_data`.
                main_data = kernels::dispatch_reconstruct_stream(
                    &mask_vec,
                    &decompressed_values,
                    &initial_type_for_values,
                    num_valid_rows,
                )?;
            }
            _ => {
                // This is a linear operation, add it to the front of the linear pipeline.
                linear_pipeline.insert(0, op_config.clone());
            }
        }
    }

    // Now, execute the accumulated linear pipeline on the (potentially reconstructed) main data.
    main_data = execute_linear_decode_pipeline(
        &main_data,
        &artifact.original_type,
        &linear_pipeline,
        num_valid_rows,
    )?;

    // --- Final Stream Assembly ---
    let mut decompressed_streams = HashMap::new();
    decompressed_streams.insert("main".to_string(), main_data);

    // Handle nulls last, after all main data has been reconstructed.
    if let Some(op_config) = full_plan
        .iter()
        .find(|op| op["op"].as_str() == Some("ExtractNulls"))
    {
        let stream_id = op_config["output_stream_id"].as_str().ok_or_else(|| {
            PhoenixError::InternalError("ExtractNulls op missing 'output_stream_id'".into())
        })?;
        if let Some(compressed_nulls) = streams.get(stream_id) {
            let pipeline = op_config["pipeline"].as_array().ok_or_else(|| {
                PhoenixError::InternalError("ExtractNulls op missing 'pipeline'".into())
            })?;
            let validity_bytes =
                execute_linear_decode_pipeline(compressed_nulls, "Boolean", pipeline, total_rows)?;
            decompressed_streams.insert("null_mask".to_string(), validity_bytes);
        }
    }

    Ok((
        decompressed_streams,
        artifact.original_type.clone(),
        artifact.total_rows,
    ))
}
