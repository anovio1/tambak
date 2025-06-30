//! This module contains the highest-level orchestration logic. It uses a generic
//! worker function pattern to avoid boilerplate and keep the core logic DRY.

use arrow::array::{Array, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::datatypes::*;
use num_traits::{Float, PrimInt, Zero}; // <-- Import Float

use super::{executor, planner};
use crate::error::PhoenixError;
use crate::kernels::sparsity;
use crate::null_handling::bitmap;
use crate::pipeline::artifact::CompressedChunk;
use crate::utils::typed_slice_to_bytes;

//==================================================================================
// 1. Private Helper Functions
//==================================================================================

/// A private helper to handle the complex orchestration of the Sparsity strategy.
fn orchestrate_sparsity_strategy<T>(
    data_vec: &[T],
    original_type: &str,
    intermediate_type: &str, // The type planner should use for the dense stream
    total_rows: u64,
    compressed_nullmap: Vec<u8>,
) -> Result<Vec<u8>, PhoenixError>
where
    T: bytemuck::Pod + PrimInt + Zero,
{
    let (mask_vec, dense_data_vec) = sparsity::split_stream(data_vec)?;

    let mask_plan_json = serde_json::to_string(&vec![
        serde_json::json!({"op": "rle"}),
        serde_json::json!({"op": "zstd", "params": {"level": 5}}),
    ])
    .map_err(|e| PhoenixError::InternalError(format!("Failed to serialize mask plan: {}", e)))?;
    let mask_bytes: Vec<u8> = mask_vec.iter().map(|&b| b as u8).collect();
    let compressed_mask =
        executor::execute_compress_pipeline(&mask_bytes, "Boolean", &mask_plan_json)?;

    let dense_data_bytes = typed_slice_to_bytes(&dense_data_vec);
    let dense_pipeline_json = planner::plan_pipeline(&dense_data_bytes, intermediate_type)?;
    let compressed_dense_data = executor::execute_compress_pipeline(
        &dense_data_bytes,
        intermediate_type,
        &dense_pipeline_json,
    )?;

    CompressedChunk {
        total_rows,
        original_type: original_type.to_string(),
        compressed_data: compressed_dense_data,
        pipeline_json: dense_pipeline_json,
        compressed_nullmap,
        compressed_mask: Some(compressed_mask),
        mask_pipeline_json: Some(mask_plan_json),
    }
    .to_bytes()
}

//==================================================================================
// 2. Generic Worker Functions (Reverted to Two-Worker Design)
//==================================================================================

/// Generic worker for compressing any INTEGER array type.
fn compress_integer_array<T>(array: &PrimitiveArray<T>) -> Result<Vec<u8>, PhoenixError>
where
    T: ArrowNumericType,
    T::Native: bytemuck::Pod + PrimInt + Zero,
{
    let total_rows = array.len() as u64;
    let original_type = T::DATA_TYPE.to_string();

    if array.is_empty() || array.null_count() == array.len() {
        // The existing logic for empty/all-null is correct.
        // We just need to ensure we enter it.
        let validity_bytes = array
            .nulls()
            .map_or(Vec::new(), |nb| nb.buffer().as_slice().to_vec());
        let compressed_nullmap = if validity_bytes.is_empty() {
            Vec::new()
        } else {
            zstd::stream::encode_all(validity_bytes.as_slice(), 19)
                .map_err(|e| PhoenixError::ZstdError(e.to_string()))?
        };
        return CompressedChunk {
            total_rows,
            original_type,
            compressed_data: Vec::new(),
            pipeline_json: "[]".to_string(),
            compressed_nullmap,
            compressed_mask: None,
            mask_pipeline_json: None,
        }
        .to_bytes();
    }

    const SPARSITY_THRESHOLD: f32 = 0.75;
    let zero_count = array
        .iter()
        .filter(|v| v.map_or(false, |x| x.is_zero()))
        .count();
    let sparse_ratio = (zero_count + array.null_count()) as f32 / array.len() as f32;

    let valid_data_vec: Vec<T::Native> = bitmap::strip_valid_data_to_vec(array);
    let compressed_nullmap = compress_nulls(array.nulls())?;

    if sparse_ratio > SPARSITY_THRESHOLD {
        // --- ADD THIS BLOCK FOR ROBUSTNESS ---
        if valid_data_vec.iter().all(|v| v.is_zero()) {
            let rle_plan = serde_json::to_string(&vec![
                serde_json::json!({"op": "rle"}),
                serde_json::json!({"op": "zstd", "params": {"level": 5}}),
            ])
            .map_err(|e| {
                PhoenixError::InternalError(format!("Failed to serialize mask plan: {}", e))
            })?;
            let compressed_data = executor::execute_compress_pipeline(
                &typed_slice_to_bytes(&valid_data_vec),
                &original_type,
                &rle_plan,
            )?;
            return CompressedChunk::dense(
                total_rows,
                original_type,
                compressed_data,
                rle_plan,
                compressed_nullmap,
            );
        }
        // --- END ADDITION ---

        return orchestrate_sparsity_strategy(
            &valid_data_vec,
            &original_type,
            &original_type,
            total_rows,
            compressed_nullmap,
        );
    }

    let data_bytes = typed_slice_to_bytes(&valid_data_vec);
    let pipeline_json = planner::plan_pipeline(&data_bytes, &original_type)?;
    let compressed_data =
        executor::execute_compress_pipeline(&data_bytes, &original_type, &pipeline_json)?;

    CompressedChunk::dense(
        total_rows,
        original_type,
        compressed_data,
        pipeline_json,
        compressed_nullmap,
    )
}

/// Generic worker for compressing any FLOAT array type.
fn compress_float_array<T, U>(array: &PrimitiveArray<T>) -> Result<Vec<u8>, PhoenixError>
where
    T: ArrowNumericType,
    T::Native: bytemuck::Pod + Float,
    U: bytemuck::Pod + PrimInt + Zero,
{
    let total_rows = array.len() as u64;
    let original_type = T::DATA_TYPE.to_string();

    if array.is_empty() || array.null_count() == array.len() {
        // The existing logic for empty/all-null is correct.
        // We just need to ensure we enter it.
        let validity_bytes = array
            .nulls()
            .map_or(Vec::new(), |nb| nb.buffer().as_slice().to_vec());
        let compressed_nullmap = if validity_bytes.is_empty() {
            Vec::new()
        } else {
            zstd::stream::encode_all(validity_bytes.as_slice(), 19)
                .map_err(|e| PhoenixError::ZstdError(e.to_string()))?
        };
        return CompressedChunk {
            total_rows,
            original_type,
            compressed_data: Vec::new(),
            pipeline_json: "[]".to_string(),
            compressed_nullmap,
            compressed_mask: None,
            mask_pipeline_json: None,
        }
        .to_bytes();
    }

    // --- START: THE FINAL, CORRECTED ARCHITECTURE ---

    // 1. Get valid data and handle nulls first.
    let mut valid_data_vec: Vec<T::Native> = bitmap::strip_valid_data_to_vec(array);
    let compressed_nullmap = compress_nulls(array.nulls())?;

    // 2. Canonicalize -0.0 to 0.0 on the float data.
    for val in &mut valid_data_vec {
        if val.is_sign_negative() && val.is_zero() {
            *val = T::Native::zero();
        }
    }

    // 3. Bit-cast the canonicalized float data to its integer representation.
    let integer_bits_vec: Vec<U> = bytemuck::cast_slice(&valid_data_vec).to_vec();
    let integer_type_str = if std::mem::size_of::<U>() == 4 {
        "UInt32"
    } else {
        "UInt64"
    };

    // 4. Perform the sparsity check on the FINAL integer representation.
    // This is the one and only "source of truth" for sparsity.
    const SPARSITY_THRESHOLD: f32 = 0.75;
    // We must now count nulls and zeros from different sources.
    let zero_count = integer_bits_vec.iter().filter(|&&v| v.is_zero()).count();
    let sparse_ratio = (zero_count + array.null_count()) as f32 / array.len() as f32;

    // 5. Dispatch to the correct strategy.
    if sparse_ratio > SPARSITY_THRESHOLD {
        // --- SPARSITY STRATEGY ---
        // Handle the edge case where the dense vector is empty.
        if integer_bits_vec.iter().all(|v| v.is_zero()) {
            // All valid values were zero. The dense stream is empty.
            // The most efficient representation is a simple RLE of the original data.
            let rle_plan = serde_json::to_string(&vec![
                serde_json::json!({"op": "rle"}),
                serde_json::json!({"op": "zstd", "params": {"level": 5}}),
            ])
            .map_err(|e| {
                PhoenixError::InternalError(format!("Failed to serialize mask plan: {}", e))
            })?;
            let compressed_data = executor::execute_compress_pipeline(
                &typed_slice_to_bytes(&integer_bits_vec),
                integer_type_str,
                &rle_plan,
            )?;
            return CompressedChunk::dense(
                total_rows,
                original_type,
                compressed_data,
                rle_plan,
                compressed_nullmap,
            );
        }

        return orchestrate_sparsity_strategy(
            &integer_bits_vec,
            &original_type,
            integer_type_str,
            total_rows,
            compressed_nullmap,
        );
    } else {
        // --- DENSE STRATEGY ---
        let data_bytes = typed_slice_to_bytes(&integer_bits_vec);
        let pipeline_json = planner::plan_pipeline(&data_bytes, integer_type_str)?;
        let compressed_data =
            executor::execute_compress_pipeline(&data_bytes, integer_type_str, &pipeline_json)?;

        CompressedChunk::dense(
            total_rows,
            original_type,
            compressed_data,
            pipeline_json,
            compressed_nullmap,
        )
    }
    // --- END: THE FINAL, CORRECTED ARCHITECTURE ---
}

// Helper to reduce duplication in the empty/all-null path
fn compress_nulls(nulls: Option<&NullBuffer>) -> Result<Vec<u8>, PhoenixError> {
    nulls.map_or(Ok(Vec::new()), |nb| {
        zstd::stream::encode_all(nb.buffer().as_slice(), 19)
            .map_err(|e| PhoenixError::ZstdError(e.to_string()))
    })
}

// Add helpers to CompressedChunk to reduce boilerplate
impl CompressedChunk {
    fn empty(
        total_rows: u64,
        original_type: String,
        nulls: Option<&NullBuffer>,
    ) -> Result<Vec<u8>, PhoenixError> {
        let compressed_nullmap = compress_nulls(nulls)?;
        Self {
            total_rows,
            original_type,
            compressed_data: Vec::new(),
            pipeline_json: "[]".to_string(),
            compressed_nullmap,
            compressed_mask: None,
            mask_pipeline_json: None,
        }
        .to_bytes()
    }

    fn dense(
        total_rows: u64,
        original_type: String,
        compressed_data: Vec<u8>,
        pipeline_json: String,
        compressed_nullmap: Vec<u8>,
    ) -> Result<Vec<u8>, PhoenixError> {
        Self {
            total_rows,
            original_type,
            compressed_data,
            pipeline_json,
            compressed_nullmap,
            compressed_mask: None,
            mask_pipeline_json: None,
        }
        .to_bytes()
    }
}

//==================================================================================
// 3. Public-Facing Dispatchers
//==================================================================================

//==================================================================================
// 3. Public-Facing Dispatchers
//==================================================================================

/// The public-facing compression orchestrator for a single array.
pub fn compress_chunk(array: &dyn Array) -> Result<Vec<u8>, PhoenixError> {
    macro_rules! dispatch_integer {
        ($arr:expr, $T:ty) => {
            compress_integer_array::<$T>(
                $arr.as_any().downcast_ref::<PrimitiveArray<$T>>().unwrap(),
            )
        };
    }
    macro_rules! dispatch_float {
        ($arr:expr, $T:ty, $U:ty) => {
            compress_float_array::<$T, $U>(
                $arr.as_any().downcast_ref::<PrimitiveArray<$T>>().unwrap(),
            )
        };
    }

    match array.data_type() {
        DataType::Int8 => dispatch_integer!(array, Int8Type),
        DataType::Int16 => dispatch_integer!(array, Int16Type),
        DataType::Int32 => dispatch_integer!(array, Int32Type),
        DataType::Int64 => dispatch_integer!(array, Int64Type),
        DataType::UInt8 => dispatch_integer!(array, UInt8Type),
        DataType::UInt16 => dispatch_integer!(array, UInt16Type),
        DataType::UInt32 => dispatch_integer!(array, UInt32Type),
        DataType::UInt64 => dispatch_integer!(array, UInt64Type),
        DataType::Float32 => dispatch_float!(array, Float32Type, u32),
        DataType::Float64 => dispatch_float!(array, Float64Type, u64),
        dt => Err(PhoenixError::UnsupportedType(format!(
            "Unsupported type for compression: {}",
            dt
        ))),
    }
}

/// The public-facing decompression orchestrator for a single array.
pub fn decompress_chunk(bytes: &[u8]) -> Result<Box<dyn Array>, PhoenixError> {
    let artifact = CompressedChunk::from_bytes(bytes)?;
    let original_type = &artifact.original_type;

    // 1. Handle Nulls (This logic is shared by both sparse and dense paths)
    let null_buffer = if !artifact.compressed_nullmap.is_empty() {
        let validity_bytes = zstd::stream::decode_all(artifact.compressed_nullmap.as_slice())
            .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
        let boolean_buffer = BooleanBuffer::new(
            Buffer::from(validity_bytes),
            0,
            artifact.total_rows as usize,
        );
        Some(NullBuffer::from(boolean_buffer))
    } else {
        None
    };

    // 2. Branch based on whether this is a sparse artifact
    if let (Some(compressed_mask), Some(mask_pipeline_json)) =
        (artifact.compressed_mask, artifact.mask_pipeline_json)
    {
        // --- SPARSE DECOMPRESSION PATH ---

        // 2a. Decompress the mask
        let decompressed_mask_bytes = executor::execute_decompress_pipeline(
            &compressed_mask,
            "Boolean",
            &mask_pipeline_json,
            artifact.total_rows as usize,
        )?;
        let mask_vec: Vec<bool> = decompressed_mask_bytes.iter().map(|&b| b != 0).collect();
        let num_non_zero = mask_vec.iter().filter(|&&b| b).count();

        // 2b. Decompress the dense data
        let intermediate_type = match original_type.as_str() {
            "Float32" => "UInt32",
            "Float64" => "UInt64",
            _ => original_type,
        };
        let decompressed_dense_bytes = executor::execute_decompress_pipeline(
            &artifact.compressed_data,
            intermediate_type,
            &artifact.pipeline_json,
            num_non_zero,
        )?;

        // 2c. Reconstruct the final array using a DRY macro
        macro_rules! reconstruct_and_finalize {
            // Arm for integer types
            ($T:ty, $TN:ty) => {{
                let dense_slice = bytemuck::cast_slice::<u8, $TN>(&decompressed_dense_bytes);
                let sparse_vec = sparsity::reconstruct_stream(
                    &mask_vec,
                    dense_slice,
                    artifact.total_rows as usize,
                )?;
                let final_array = bitmap::reapply_bitmap_from_vec::<$T>(
                    sparse_vec,
                    null_buffer,
                    artifact.total_rows as usize,
                )?;
                Ok(Box::new(final_array) as Box<dyn Array>)
            }};
            // Arm for float types (handles the extra bit-cast)
            ($T:ty, $TN:ty, $U:ty) => {{
                let dense_int_slice = bytemuck::cast_slice::<u8, $U>(&decompressed_dense_bytes);
                let sparse_int_vec = sparsity::reconstruct_stream(
                    &mask_vec,
                    dense_int_slice,
                    artifact.total_rows as usize,
                )?;
                let final_typed_vec: Vec<$TN> = bytemuck::cast_slice(&sparse_int_vec).to_vec();
                let final_array = bitmap::reapply_bitmap_from_vec::<$T>(
                    final_typed_vec,
                    null_buffer,
                    artifact.total_rows as usize,
                )?;
                Ok(Box::new(final_array) as Box<dyn Array>)
            }};
        }

        // The match statement is now clean, readable, and maintainable.
        match original_type.as_str() {
            "Int8" => reconstruct_and_finalize!(Int8Type, i8),
            "Int16" => reconstruct_and_finalize!(Int16Type, i16),
            "Int32" => reconstruct_and_finalize!(Int32Type, i32),
            "Int64" => reconstruct_and_finalize!(Int64Type, i64),
            "UInt8" => reconstruct_and_finalize!(UInt8Type, u8),
            "UInt16" => reconstruct_and_finalize!(UInt16Type, u16),
            "UInt32" => reconstruct_and_finalize!(UInt32Type, u32),
            "UInt64" => reconstruct_and_finalize!(UInt64Type, u64),
            "Float32" => reconstruct_and_finalize!(Float32Type, f32, u32),
            "Float64" => reconstruct_and_finalize!(Float64Type, f64, u64),
            _ => Err(PhoenixError::UnsupportedType(original_type.to_string())),
        }
    } else {
        // --- DENSE DECOMPRESSION PATH ---

        let num_valid_rows = null_buffer
            .as_ref()
            .map_or(artifact.total_rows as usize, |nb| {
                nb.len() - nb.null_count()
            });

        let decompressed_data_bytes = if !artifact.compressed_data.is_empty() {
            let intermediate_type = match original_type.as_str() {
                "Float32" => "UInt32",
                "Float64" => "UInt64",
                _ => original_type,
            };
            executor::execute_decompress_pipeline(
                &artifact.compressed_data,
                intermediate_type,
                &artifact.pipeline_json,
                num_valid_rows,
            )?
        } else {
            Vec::new()
        };

        macro_rules! reconstruct_dense_array {
            ($T:ty) => {{
                let array = bitmap::reapply_bitmap::<$T>(
                    decompressed_data_bytes,
                    null_buffer,
                    artifact.total_rows as usize,
                )?;
                Ok(Box::new(array) as Box<dyn Array>)
            }};
        }

        // This path was already fairly DRY, but the macro makes it consistent.
        match original_type.as_str() {
            "Int8" => reconstruct_dense_array!(Int8Type),
            "Int16" => reconstruct_dense_array!(Int16Type),
            "Int32" => reconstruct_dense_array!(Int32Type),
            "Int64" => reconstruct_dense_array!(Int64Type),
            "UInt8" => reconstruct_dense_array!(UInt8Type),
            "UInt16" => reconstruct_dense_array!(UInt16Type),
            "UInt32" => reconstruct_dense_array!(UInt32Type),
            "UInt64" => reconstruct_dense_array!(UInt64Type),
            "Float32" => reconstruct_dense_array!(Float32Type),
            "Float64" => reconstruct_dense_array!(Float64Type),
            _ => Err(PhoenixError::UnsupportedType(original_type.to_string())),
        }
    }
}

/// Public-facing utility to inspect a compressed chunk.
pub fn get_compressed_chunk_info(
    bytes: &[u8],
) -> Result<(usize, usize, String, String), PhoenixError> {
    let artifact = CompressedChunk::from_bytes(bytes)?;
    let data_size = artifact.compressed_data.len();
    let header_size = bytes.len() - data_size;
    Ok((
        header_size,
        data_size,
        artifact.pipeline_json,
        artifact.original_type,
    ))
}

//==================================================================================
// 3. Unit Tests
//==================================================================================

#[cfg(test)]
#[path = "orchestrator_tests.rs"]
mod tests;
