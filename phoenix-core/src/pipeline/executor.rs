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
use crate::utils::typed_slice_to_bytes;
use bytemuck::Pod;
use num_traits::{PrimInt, Zero};

// ... (execute_linear_encode_pipeline and execute_linear_decode_pipeline are unchanged and correct) ...
fn execute_linear_encode_pipeline(
    bytes: &[u8],
    type_str: &str,
    pipeline: &[Value],
) -> Result<(Vec<u8>, String), PhoenixError> {
    if pipeline.is_empty() {
        return Ok((bytes.to_vec(), type_str.to_string()));
    }

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
    Ok((buffer_a, current_type))
}
fn execute_linear_decode_pipeline(
    bytes: &[u8],
    final_type: &str,
    pipeline: &[Value],
    num_values: usize,
) -> Result<Vec<u8>, PhoenixError> {
    if pipeline.is_empty() {
        return Ok(bytes.to_vec());
    }

    let mut type_stack = vec![final_type.to_string()];
    for op_config in pipeline.iter().rev().skip(1) {
        let prev_type = type_stack.last().ok_or_else(|| {
            PhoenixError::InternalError("Type stack became empty unexpectedly".into())
        })?;
        let mut next_type = prev_type.clone();
        if let Some("BitCast") = op_config["op"].as_str() {
            next_type = op_config["from_type"]
                .as_str()
                .ok_or_else(|| {
                    PhoenixError::InternalError(
                        "BitCast op in plan is missing 'from_type' field".into(),
                    )
                })?
                .to_string();
        } else if let Some("zigzag") = op_config["op"].as_str() {
            next_type = prev_type.replace("UInt", "Int");
        }
        type_stack.push(next_type);
    }

    let mut buffer_a = bytes.to_vec();
    let mut buffer_b = Vec::with_capacity(buffer_a.len());

    for op_config in pipeline.iter().rev() {
        let type_for_decode = type_stack
            .pop()
            .ok_or_else(|| PhoenixError::InternalError("Type stack desync during decode".into()))?;
        kernels::dispatch_decode(
            op_config,
            &buffer_a,
            &mut buffer_b,
            &type_for_decode,
            num_values,
        )?;
        std::mem::swap(&mut buffer_a, &mut buffer_b);
        buffer_b.clear();
    }
    Ok(buffer_a)
}

/// The primary public entry point for the v4.1 Executor (Compression).
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

    for op_config in plan {
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

                macro_rules! handle_sparsity_split {
                    ($T:ty) => {{
                        let typed_slice: &[$T] = bytemuck::try_cast_slice(&main_data_bytes)?;
                        let (mask, dense_vec) = kernels::sparsity::split_stream(typed_slice)?;
                        Ok((mask, typed_slice_to_bytes(&dense_vec)))
                    }};
                }
                let (mask_vec, dense_values_bytes) = match current_type.as_str() {
                    "Int8" => handle_sparsity_split!(i8),
                    "Int16" => handle_sparsity_split!(i16),
                    "Int32" => handle_sparsity_split!(i32),
                    "Int64" => handle_sparsity_split!(i64),
                    "UInt8" => handle_sparsity_split!(u8),
                    "UInt16" => handle_sparsity_split!(u16),
                    "UInt32" => handle_sparsity_split!(u32),
                    "UInt64" => handle_sparsity_split!(u64),
                    _ => Err(PhoenixError::InternalError(
                        "Unsupported type for Sparsify".into(),
                    )),
                }?;

                let mask_bytes: Vec<u8> = mask_vec.iter().map(|&b| b as u8).collect();
                let (compressed_mask, _) =
                    execute_linear_encode_pipeline(&mask_bytes, "Boolean", mask_pipeline)?;
                compressed_streams.insert(mask_stream_id.to_string(), compressed_mask);

                main_data_bytes = dense_values_bytes;
                main_pipeline.extend_from_slice(values_pipeline);
            }
            _ => {
                main_pipeline.push(op_config);
            }
        }
    }

    (main_data_bytes, _) =
        execute_linear_encode_pipeline(&main_data_bytes, &current_type, &main_pipeline)?;
    compressed_streams.insert("main".to_string(), main_data_bytes);

    Ok(compressed_streams)
}

/// The primary public entry point for the v4.1 Executor (Decompression).
pub fn decompress_plan(
    bytes: &[u8],
) -> Result<(HashMap<String, Vec<u8>>, String, u64), PhoenixError> {
    let artifact = CompressedChunk::from_bytes(bytes)?;
    let plan: Vec<Value> = serde_json::from_str(&artifact.plan_json)?;

    let mut streams = artifact.compressed_streams;
    let mut main_data = streams
        .remove("main")
        .ok_or_else(|| PhoenixError::InternalError("Missing 'main' data stream".into()))?;
    let mut current_type = artifact.original_type.clone();
    let num_values = artifact.total_rows as usize;

    let mut linear_pipeline: Vec<Value> = Vec::new();

    for op_config in plan.iter().rev() {
        match op_config["op"].as_str() {
            Some("ExtractNulls") => { /* Handled separately */ }
            Some("Sparsify") => {
                main_data = execute_linear_decode_pipeline(
                    &main_data,
                    &current_type,
                    &linear_pipeline,
                    num_values,
                )?;
                linear_pipeline.clear();

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
                let values_pipeline = op_config["values_pipeline"].as_array().ok_or_else(|| {
                    PhoenixError::InternalError("Sparsify op missing 'values_pipeline'".into())
                })?;

                let decompressed_mask_bytes = execute_linear_decode_pipeline(
                    compressed_mask,
                    "Boolean",
                    mask_pipeline,
                    num_values,
                )?;
                let mask_vec: Vec<bool> =
                    decompressed_mask_bytes.iter().map(|&b| *b != 0).collect();
                let num_non_zero = mask_vec.iter().filter(|&&b| b).count();

                let decompressed_values = execute_linear_decode_pipeline(
                    &main_data,
                    &current_type,
                    values_pipeline,
                    num_non_zero,
                )?;

                macro_rules! handle_sparsity_reconstruct {
                    ($T:ty) => {{
                        let typed_values: &[$T] = bytemuck::try_cast_slice(&decompressed_values)?;
                        let reconstructed_vec =
                            kernels::sparsity::reconstruct_stream(&mask_vec, typed_values)?;
                        Ok(typed_slice_to_bytes(&reconstructed_vec))
                    }};
                }
                main_data = match current_type.as_str() {
                    "Int8" => handle_sparsity_reconstruct!(i8),
                    "Int16" => handle_sparsity_reconstruct!(i16),
                    "Int32" => handle_sparsity_reconstruct!(i32),
                    "Int64" => handle_sparsity_reconstruct!(i64),
                    "UInt8" => handle_sparsity_reconstruct!(u8),
                    "UInt16" => handle_sparsity_reconstruct!(u16),
                    "UInt32" => handle_sparsity_reconstruct!(u32),
                    "UInt64" => handle_sparsity_reconstruct!(u64),
                    _ => Err(PhoenixError::InternalError(
                        "Unsupported type for Sparsify reconstruct".into(),
                    )),
                }?;
            }
            _ => {
                linear_pipeline.push(op_config.clone());
            }
        }
    }

    main_data =
        execute_linear_decode_pipeline(&main_data, &current_type, &linear_pipeline, num_values)?;

    let mut decompressed_streams = HashMap::new();
    decompressed_streams.insert("main".to_string(), main_data);

    if let Some(op_config) = plan
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
                execute_linear_decode_pipeline(compressed_nulls, "Boolean", pipeline, num_values)?;
            decompressed_streams.insert("null_mask".to_string(), validity_bytes);
        }
    }

    Ok((decompressed_streams, current_type, artifact.total_rows))
}
