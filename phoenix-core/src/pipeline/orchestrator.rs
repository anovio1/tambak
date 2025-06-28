// --- IN: src/pipeline/orchestrator.v2.rs ---

//! This module contains the highest-level orchestration logic. It uses a generic
//! worker function pattern to avoid boilerplate and keep the core logic DRY.

use arrow::array::{Array, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::datatypes::*;
use std::io::{Cursor, Write, Read};

use crate::error::PhoenixError;
use crate::null_handling::bitmap;
use super::{planner, executor};

// (The CompressedChunk struct and its impl remain unchanged)
struct CompressedChunk {
    total_rows: u64,
    compressed_nullmap: Vec<u8>,
    compressed_data: Vec<u8>,
    pipeline_json: String,
}

// ... (The impl CompressedChunk block is unchanged and correct) ...
impl CompressedChunk {
    pub fn to_bytes(&self) -> Result<Vec<u8>, PhoenixError> {
        let nullmap_len = self.compressed_nullmap.len() as u64;
        let pipeline_len = self.pipeline_json.len() as u64;
        let mut buffer = Vec::new();
        
        buffer.write_all(&nullmap_len.to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer.write_all(&pipeline_len.to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer.write_all(&self.total_rows.to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        
        buffer.write_all(&self.compressed_nullmap).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer.write_all(self.pipeline_json.as_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer.write_all(&self.compressed_data).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        
        Ok(buffer)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PhoenixError> {
        if bytes.len() < 24 { return Err(PhoenixError::BufferMismatch(bytes.len(), 24)); }
        let mut cursor = Cursor::new(bytes);
        
        let mut u64_buf = [0u8; 8];
        cursor.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let nullmap_len = u64::from_le_bytes(u64_buf) as usize;
        cursor.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let pipeline_len = u64::from_le_bytes(u64_buf) as usize;
        cursor.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let total_rows = u64::from_le_bytes(u64_buf);

        let header_size = 24;
        let data_start = header_size + nullmap_len + pipeline_len;
        if bytes.len() < data_start { return Err(PhoenixError::BufferMismatch(bytes.len(), data_start)); }

        let compressed_nullmap = bytes[header_size..header_size + nullmap_len].to_vec();
        let pipeline_json = String::from_utf8(bytes[header_size + nullmap_len..data_start].to_vec())
            .map_err(|e| PhoenixError::UnsupportedType(e.to_string()))?;
        let compressed_data = bytes[data_start..].to_vec();

        Ok(Self { total_rows, compressed_nullmap, compressed_data, pipeline_json })
    }
}

//==================================================================================
// 2. Generic Worker Functions (The DRY Approach)
//==================================================================================

/// Generic worker function that performs the entire compression for any primitive array type.
fn compress_primitive_array<T>(array: &PrimitiveArray<T>) -> Result<Vec<u8>, PhoenixError>
where
    T: ArrowNumericType,
    T::Native: ToByteSlice,
{
    let total_rows = array.len() as u64;
    let original_type = T::DATA_TYPE.to_string();

    // --- Step 1: Deconstruct the array into its raw parts ---
    let valid_data_buffer = bitmap::strip_valid_data_to_buffer(array);
    let validity_bytes_opt = array.nulls().map(|nb| nb.buffer().as_slice().to_vec());

    // --- Step 2: Compress the validity buffer (if it exists) ---
    let compressed_nullmap = if let Some(validity_bytes) = validity_bytes_opt {
        let pipeline_json = r#"[{"op": "rle"}, {"op": "zstd", "params": {"level": 19}}]"#;
        executor::execute_compress_pipeline(&validity_bytes, "Boolean", &pipeline_json)?
    } else {
        Vec::new()
    };

    // --- Step 3: Plan and compress the main data buffer ---
    let pipeline_json = planner::plan_pipeline(valid_data_buffer.as_slice(), &original_type)?;
    let compressed_data = executor::execute_compress_pipeline(
        valid_data_buffer.as_slice(),
        &original_type,
        &pipeline_json,
    )?;

    // --- Step 4: Package the final artifact ---
    let artifact = CompressedChunk {
        total_rows,
        compressed_nullmap,
        compressed_data,
        pipeline_json,
    };

    artifact.to_bytes()
}

//==================================================================================
// 3. Public-Facing Dispatchers (Minimal Boilerplate)
//==================================================================================

/// The public-facing compression orchestrator. Its ONLY job is to downcast
/// the dynamic `Array` and dispatch to the generic worker function.
pub fn compress_chunk(array: &dyn Array) -> Result<Vec<u8>, PhoenixError> {
    // This macro performs the downcast and calls the generic worker.
    macro_rules! dispatch {
        ($arr:expr, $T:ty) => {{
            let primitive_arr = $arr.as_any().downcast_ref::<PrimitiveArray<$T>>()
                .ok_or_else(|| PhoenixError::UnsupportedType(format!("Failed to downcast to {}", stringify!($T))))?;
            compress_primitive_array(primitive_arr)
        }};
    }

    // The boilerplate is now minimal and serves a single, clear purpose: type dispatch.
    match array.data_type() {
        DataType::Int8 => dispatch!(array, Int8Type),
        DataType::Int16 => dispatch!(array, Int16Type),
        DataType::Int32 => dispatch!(array, Int32Type),
        DataType::Int64 => dispatch!(array, Int64Type),
        DataType::UInt8 => dispatch!(array, UInt8Type),
        DataType::UInt16 => dispatch!(array, UInt16Type),
        DataType::UInt32 => dispatch!(array, UInt32Type),
        DataType::UInt64 => dispatch!(array, UInt64Type),
        dt => Err(PhoenixError::UnsupportedType(format!("Unsupported type for compression: {}", dt))),
    }
}

/// The public-facing decompression orchestrator.
/// The public-facing decompression orchestrator.
pub fn decompress_chunk(bytes: &[u8], original_type: &str) -> Result<Box<dyn Array>, PhoenixError> {
    // --- Step 1: Unpack the artifact ---
    let artifact = CompressedChunk::from_bytes(bytes)?;

    // --- Step 2: Decompress the Validity Mask ---
    let null_buffer = if artifact.compressed_nullmap.is_empty() {
        None
    } else {
        // ... (logic to create null_buffer is correct from last time)
        let validity_pipeline_json = r#"[{"op": "rle"}, {"op": "zstd"}]"#;
        let validity_bytes = executor::execute_decompress_pipeline(
            &artifact.compressed_nullmap,
            "Boolean",
            validity_pipeline_json,
            artifact.total_rows as usize,
        )?;
        let byte_buffer = Buffer::from(validity_bytes);
        let boolean_buffer = BooleanBuffer::new(byte_buffer, 0, artifact.total_rows as usize);
        Some(NullBuffer::from(boolean_buffer))
    };

    // --- Step 3: Calculate num_valid_rows ---
    // --- THIS IS THE CORRECTED LINE ---
    let num_valid_rows = null_buffer.as_ref().map_or(artifact.total_rows as usize, |nb| nb.len() - nb.null_count());

    // --- Step 4: Decompress the main data ---
    let decompressed_data_bytes = executor::execute_decompress_pipeline(
        &artifact.compressed_data,
        original_type,
        &artifact.pipeline_json,
        num_valid_rows,
    )?;
    let data_buffer = Buffer::from(decompressed_data_bytes);

    // --- Step 5: Dispatch to the generic reconstruction kernel (This part was already correct) ---
    macro_rules! dispatch_reconstruct {
        ($T:ty) => {{
            let array = bitmap::reapply_bitmap::<$T>(data_buffer, null_buffer)?;
            Ok(Box::new(array) as Box<dyn Array>)
        }};
    }

    match original_type {
        "Int8" => dispatch_reconstruct!(Int8Type),
        "Int16" => dispatch_reconstruct!(Int16Type),
        "Int32" => dispatch_reconstruct!(Int32Type),
        "Int64" => dispatch_reconstruct!(Int64Type),
        "UInt8" => dispatch_reconstruct!(UInt8Type),
        "UInt16" => dispatch_reconstruct!(UInt16Type),
        "UInt32" => dispatch_reconstruct!(UInt32Type),
        "UInt64" => dispatch_reconstruct!(UInt64Type),
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string())),
    }
}

//==================================================================================
// 3. Unit Tests (REVISED to use Arrow arrays instead of Polars Series)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};

    #[test]
    fn test_orchestrator_roundtrip_with_nulls() {
        // 1. Create a native arrow-rs Array
        let original_array = Int64Array::from(vec![Some(100i64), Some(110), None, Some(110), Some(90)]);

        // 2. Compress it
        let compressed_artifact_bytes = compress_chunk(&original_array).unwrap();

        // 3. Decompress it
        let reconstructed_arrow_array = decompress_chunk(
            &compressed_artifact_bytes,
            "Int64",
        ).unwrap();

        // 4. Downcast and compare
        let reconstructed_i64_array = reconstructed_arrow_array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(&original_array, reconstructed_i64_array);
    }

    #[test]
    fn test_orchestrator_roundtrip_no_nulls() {
        let original_array = Int32Array::from(vec![10i32, 20, 30, 40]);
        
        let compressed_artifact_bytes = compress_chunk(&original_array).unwrap();

        let reconstructed_arrow_array = decompress_chunk(
            &compressed_artifact_bytes,
            "Int32",
        ).unwrap();
        
        let reconstructed_i32_array = reconstructed_arrow_array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(&original_array, reconstructed_i32_array);
    }
}