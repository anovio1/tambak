//! This module contains the highest-level orchestration logic for the entire
//! compression and decompression workflow.
//!
//! It acts as the "General Contractor," managing the end-to-end process by
//! correctly delegating to the other pipeline modules:
//! 1.  **Null Handling:** It directs the `null_handling` kernels to strip and re-apply validity.
//! 2.  **Planning:** It directs the `planner` to create an optimal compression plan.
//! 3.  **Execution:** It directs the `executor` to execute that plan on the valid data.
//! 4.  **Packaging:** It bundles the final compressed artifacts for the FFI layer.
//!
//! This module is PURE RUST and is completely decoupled from the FFI layer itself.

use polars::prelude::*;
use arrow::array::Array;
use std::io::{Cursor, Write, Read};

use crate::error::PhoenixError;
use crate::null_handling;
use super::{planner, executor};

//==================================================================================
// 1. On-Disk Artifact Structure
//==================================================================================

/// Defines the structure of the final compressed artifact for a single column chunk.
struct CompressedChunk {
    total_rows: u64,
    compressed_nullmap: Vec<u8>,
    compressed_data: Vec<u8>,
    pipeline_json: String, // The plan is now part of the artifact
}

impl CompressedChunk {
    /// Serializes the artifact into a single byte vector for storage.
    fn to_bytes(&self) -> Result<Vec<u8>, PhoenixError> {
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

    /// Deserializes a byte vector back into a structured artifact.
    fn from_bytes(bytes: &[u8]) -> Result<Self, PhoenixError> {
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
// 2. Orchestration Logic (Corrected Delegation)
//==================================================================================

/// The public-facing compression orchestrator for the PURE RUST library.
pub fn compress_chunk(series: &Series) -> Result<Vec<u8>, PhoenixError> {
    let original_type = series.dtype().to_string();
    let total_rows = series.len();

    let (valid_data_bytes, validity_bytes_opt) = null_handling::bitmap::strip_validity_to_bytes(series)?;

    let compressed_nullmap = if let Some(validity_bytes) = validity_bytes_opt {
        let pipeline_json = r#"[{"op": "rle"}, {"op": "zstd", "params": {"level": 19}}]"#;
        executor::execute_compress_pipeline(&validity_bytes, "Boolean", &pipeline_json)?
    } else {
        Vec::new()
    };

    let pipeline_json = planner::plan_pipeline(&valid_data_bytes, &original_type)?;

    let compressed_data = executor::execute_compress_pipeline(
        &valid_data_bytes,
        &original_type,
        &pipeline_json,
    )?;

    let artifact = CompressedChunk {
        total_rows: total_rows as u64,
        compressed_nullmap,
        compressed_data,
        pipeline_json,
    };

    artifact.to_bytes()
}

/// The public-facing decompression orchestrator for the PURE RUST library.
pub fn decompress_chunk(bytes: &[u8], original_type: &str) -> Result<Box<dyn Array>, PhoenixError> {
    // --- Step 1: Unpack the artifact ---
    let artifact = CompressedChunk::from_bytes(bytes)?;

    // --- Step 2: Decompress the Validity Mask ---
    let validity_bitmap = if artifact.compressed_nullmap.is_empty() {
        None
    } else {
        let validity_pipeline_json = r#"[{"op": "rle"}, {"op": "zstd"}]"#;
        let validity_bytes = executor::execute_decompress_pipeline(
            &artifact.compressed_nullmap,
            "Boolean",
            validity_pipeline_json,
            artifact.total_rows as usize,
        )?;
        Some(arrow::bitmap::Bitmap::from(validity_bytes))
    };

    // --- FIX: Calculate num_valid_rows correctly AFTER decompressing the nullmap ---
    let num_valid_rows = if let Some(bitmap) = &validity_bitmap {
        bitmap.true_count()
    } else {
        artifact.total_rows as usize
    };

    // --- Step 3: Delegate to Executor to decompress the data ---
    let decompressed_data_bytes = executor::execute_decompress_pipeline(
        &artifact.compressed_data,
        original_type,
        &artifact.pipeline_json,
        num_valid_rows, // Pass the CORRECT number of values
    )?;

    // --- Step 4: Delegate to Null Handling to reconstruct the final Arrow Array ---
    null_handling::bitmap::reapply_bitmap_to_arrow(
        &decompressed_data_bytes,
        validity_bitmap,
        original_type,
        artifact.total_rows as usize,
    )
}

//==================================================================================
// 3. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    #[test]
    fn test_orchestrator_roundtrip_with_nulls() {
        let original_series = Series::new("test", &[Some(100i64), Some(110), None, Some(110), Some(90)]);
        
        let compressed_artifact_bytes = compress_chunk(&original_series).unwrap();

        let reconstructed_arrow_array = decompress_chunk(
            &compressed_artifact_bytes,
            "Int64",
        ).unwrap();
        let reconstructed_series = Series::from_arrow("test", reconstructed_arrow_array).unwrap();

        assert!(original_series.equals(&reconstructed_series));
    }

    #[test]
    fn test_orchestrator_roundtrip_no_nulls() {
        let original_series = Series::new("test", &[10i32, 20, 30, 40]);
        
        let compressed_artifact_bytes = compress_chunk(&original_series).unwrap();

        let reconstructed_arrow_array = decompress_chunk(
            &compressed_artifact_bytes,
            "Int32",
        ).unwrap();
        let reconstructed_series = Series::from_arrow("test", reconstructed_arrow_array).unwrap();

        assert!(original_series.equals(&reconstructed_series));
    }
}