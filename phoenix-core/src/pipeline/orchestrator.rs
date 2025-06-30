//! This module contains the highest-level orchestration logic. It uses a generic
//! worker function pattern to avoid boilerplate and keep the core logic DRY.

use arrow::array::{Array, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::datatypes::*;
use std::io::{Cursor, Read, Write};

use super::{executor, planner};
use crate::error::PhoenixError;
use crate::null_handling::bitmap;
use crate::pipeline::artifact::CompressedChunk;
use crate::utils::typed_slice_to_bytes;
use num_traits::{Float, Zero};

//==================================================================================
// 2. Generic Worker Functions (The DRY Core)
//==================================================================================

/// Generic worker for compressing any INTEGER array type.
fn compress_integer_array<T>(array: &PrimitiveArray<T>) -> Result<Vec<u8>, PhoenixError>
where
    T: ArrowNumericType,
    T::Native: bytemuck::Pod,
{
    let total_rows = array.len() as u64;
    let original_type = T::DATA_TYPE.to_string();

    // Handle all-null or empty cases
    if array.null_count() == array.len() || array.is_empty() {
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
            compressed_nullmap,
            compressed_data: Vec::new(),
            pipeline_json: "[]".to_string(),
            original_type,
        }
        .to_bytes();
    }

    // 1. Deconstruct array into validity and data buffers
    let valid_data_vec: Vec<T::Native> = bitmap::strip_valid_data_to_vec(array);
    let validity_bytes_opt = array.nulls().map(|nb| nb.buffer().as_slice().to_vec());

    // 2. Compress validity buffer
    let compressed_nullmap = if let Some(validity_bytes) = validity_bytes_opt {
        zstd::stream::encode_all(validity_bytes.as_slice(), 19)
            .map_err(|e| PhoenixError::ZstdError(e.to_string()))?
    } else {
        Vec::new()
    };

    // 3. Plan and compress the main data buffer
    let valid_data_bytes = typed_slice_to_bytes(&valid_data_vec);
    let pipeline_json = planner::plan_pipeline(&valid_data_bytes, &original_type)?;
    let compressed_data =
        executor::execute_compress_pipeline(&valid_data_bytes, &original_type, &pipeline_json)?;

    // 4. Package the final artifact
    CompressedChunk {
        total_rows,
        compressed_nullmap,
        compressed_data,
        pipeline_json,
        original_type,
    }
    .to_bytes()
}

/// Generic worker for compressing any FLOAT array type.
fn compress_float_array<T, U>(array: &PrimitiveArray<T>) -> Result<Vec<u8>, PhoenixError>
where
    T: ArrowNumericType,
    U: bytemuck::Pod,
    // --- ADD THIS TRAIT BOUND ---
    // T::Native must implement the Float trait for our canonicalization logic.
    T::Native: bytemuck::Pod + Float,
{
    // For all-null or empty arrays, the integer path is sufficient and correct.
    if array.null_count() == array.len() || array.is_empty() {
        // This path is fine, as it doesn't involve bit-casting.
        return compress_integer_array(array);
    }

    let total_rows = array.len() as u64;
    let original_type = T::DATA_TYPE.to_string();

    // 1. Handle Nulls (same as integer path)
    // --- MAKE THIS MUTABLE ---
    let mut valid_data_vec: Vec<T::Native> = bitmap::strip_valid_data_to_vec(array);
    let validity_bytes_opt = array.nulls().map(|nb| nb.buffer().as_slice().to_vec());
    let compressed_nullmap = if let Some(validity_bytes) = validity_bytes_opt {
        zstd::stream::encode_all(validity_bytes.as_slice(), 19)
            .map_err(|e| PhoenixError::ZstdError(e.to_string()))?
    } else {
        Vec::new()
    };

    // --- START: Canonicalization of -0.0 ---
    // This is the critical fix. We must ensure that any negative zero is
    // converted to a positive zero to guarantee its bit pattern is all zeros.
    for val in &mut valid_data_vec {
        // The most robust way to check for -0.0 in a generic float.
        if val.is_sign_negative() && val.is_zero() {
            println!("Canonicalizing -0.0 to +0.0");
            *val = Zero::zero();
        }
    }
    // --- END: Canonicalization of -0.0 ---

    // 2. The Core Float-to-Bits Transformation (now operates on canonicalized data)
    let integer_bits_vec: Vec<U> = bytemuck::cast_slice::<T::Native, U>(&valid_data_vec).to_vec();
    let integer_bits_bytes = typed_slice_to_bytes(&integer_bits_vec);
    let integer_type_str = if std::mem::size_of::<U>() == 4 {
        "UInt32"
    } else {
        "UInt64"
    };

    // 3. Plan and Compress the INTEGER bits
    let pipeline_json = planner::plan_pipeline(&integer_bits_bytes, integer_type_str)?;
    let compressed_data =
        executor::execute_compress_pipeline(&integer_bits_bytes, integer_type_str, &pipeline_json)?;

    // 4. Package the final artifact
    CompressedChunk {
        total_rows,
        compressed_nullmap,
        compressed_data,
        pipeline_json,
        original_type,
    }
    .to_bytes()
}

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

    macro_rules! reconstruct_array {
        ($T:ty) => {{
            let array = bitmap::reapply_bitmap::<$T>(
                decompressed_data_bytes,
                null_buffer,
                artifact.total_rows as usize,
            )?;
            Ok(Box::new(array) as Box<dyn Array>)
        }};
    }

    match original_type.as_str() {
        "Int8" => reconstruct_array!(Int8Type),
        "Int16" => reconstruct_array!(Int16Type),
        "Int32" => reconstruct_array!(Int32Type),
        "Int64" => reconstruct_array!(Int64Type),
        "UInt8" => reconstruct_array!(UInt8Type),
        "UInt16" => reconstruct_array!(UInt16Type),
        "UInt32" => reconstruct_array!(UInt32Type),
        "UInt64" => reconstruct_array!(UInt64Type),
        "Float32" => reconstruct_array!(Float32Type),
        "Float64" => reconstruct_array!(Float64Type),
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string())),
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
// 3. Unit Tests (REVISED - Authoritative & Correct)
//==================================================================================

#[cfg(test)]
#[path = "orchestrator_tests.rs"]
mod tests;
