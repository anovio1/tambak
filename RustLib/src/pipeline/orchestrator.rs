//! This module contains the highest-level orchestration logic for the entire
//! compression and decompression workflow.
//!
//! It acts as the "General Contractor," managing the end-to-end process:
//! 1.  **Null Handling:** Stripping the validity/null bitmask from the input data.
//! 2.  **Planning:** Directing the `planner` to create an optimal compression plan.
//! 3.  **Execution:** Directing the `executor` to execute that plan on the valid data.
//! 4.  **Packaging:** Bundling the compressed validity mask and the compressed data
//!     into a single, final byte buffer for the FFI layer.

use pyo3::PyResult;
use std::io::{Cursor, Write, Read};

use crate::error::PhoenixError;
use super::{planner, executor};

//==================================================================================
// 1. On-Disk Artifact Structure (REVISED - Header now includes num_valid_rows)
//==================================================================================

/// Defines the structure of the final compressed artifact for a single column chunk.
/// This is the format that the orchestrator produces and consumes.
///
/// +-------------------------------------------------+
/// | 8-byte Header (u64): Length of Nullmap          |
/// +-------------------------------------------------+
/// | 8-byte Header (u64): Number of VALID data rows  | <--- FIX #1
/// +-------------------------------------------------+
/// | Compressed Nullmap Bytes (RLE + Zstd)           |
/// +-------------------------------------------------+
/// | Compressed Data Bytes (from pipeline)           |
/// +-------------------------------------------------+
///
struct CompressedArtifact {
    num_valid_rows: u64,
    compressed_nullmap: Vec<u8>,
    compressed_data: Vec<u8>,
}

impl CompressedArtifact {
    /// Serializes the artifact into a single byte vector for storage.
    /// Returns a Result to handle potential I/O errors.
    fn to_bytes(&self) -> Result<Vec<u8>, std::io::Error> { // <-- FIX #2
        let nullmap_len = self.compressed_nullmap.len() as u64;
        let mut buffer = Vec::with_capacity(16 + self.compressed_nullmap.len() + self.compressed_data.len());
        
        buffer.write_all(&nullmap_len.to_le_bytes())?;
        buffer.write_all(&self.num_valid_rows.to_le_bytes())?;
        buffer.write_all(&self.compressed_nullmap)?;
        buffer.write_all(&self.compressed_data)?;
        
        Ok(buffer)
    }

    /// Deserializes a byte vector back into a structured artifact.
    fn from_bytes(bytes: &[u8]) -> Result<Self, PhoenixError> { // <-- FIX #2
        if bytes.len() < 16 {
            return Err(PhoenixError::BufferMismatch(bytes.len(), 16));
        }
        let mut cursor = Cursor::new(bytes);
        
        let mut len_bytes = [0u8; 8];
        cursor.read_exact(&mut len_bytes).map_err(|e| PhoenixError::BufferMismatch(0,0))?; // Proper error
        let nullmap_len = u64::from_le_bytes(len_bytes) as usize;

        cursor.read_exact(&mut len_bytes).map_err(|e| PhoenixError::BufferMismatch(0,0))?; // Proper error
        let num_valid_rows = u64::from_le_bytes(len_bytes);

        let header_size = 16;
        if bytes.len() < header_size + nullmap_len {
            return Err(PhoenixError::BufferMismatch(bytes.len(), header_size + nullmap_len));
        }

        let compressed_nullmap = bytes[header_size..header_size + nullmap_len].to_vec();
        let compressed_data = bytes[header_size + nullmap_len..].to_vec();

        Ok(Self { num_valid_rows, compressed_nullmap, compressed_data })
    }
}

//==================================================================================
// 2. Orchestration Logic (REVISED - API now robust)
//==================================================================================


/// The public-facing compression orchestrator for the PURE RUST library.
///
/// # Args
/// * `data_slice`: A byte slice of the raw, validity-stripped data.
/// * `validity_slice_opt`: An optional byte slice of the Arrow-compatible validity bitmap.
/// * `original_type`: The string representation of the data's type.
pub fn compress_chunk(
    data_slice: &[u8],
    validity_slice_opt: Option<&[u8]>,
    original_type: &str,
) -> Result<Vec<u8>, PhoenixError> {
    // --- Step 1: Compress the Validity Mask ---
    let compressed_nullmap = if let Some(validity_slice) = validity_slice_opt {
        let rle_encoded = crate::compression::rle::encode(validity_slice, "Boolean")?;
        crate::compression::zstd::compress(&rle_encoded, 19)?
    } else {
        Vec::new()
    };

    // --- Step 2 & 3: Plan and Execute pipeline on the valid data ---
    let pipeline_json = planner::plan_pipeline(data_slice, original_type)?;
    let compressed_data = executor::execute_compress_pipeline(
        data_slice,
        original_type,
        &pipeline_json,
    )?;

    // --- Step 4: Package the final artifact ---
    let num_valid_rows = {
        let element_size = crate::utils::get_element_size(original_type)?;
        (data_slice.len() / element_size) as u64
    };

    let artifact = CompressedArtifact {
        num_valid_rows,
        compressed_nullmap,
        compressed_data,
    };

    Ok(artifact.to_bytes()?)
}

/// The public-facing decompression orchestrator for the PURE RUST library.
///
/// Returns pure Rust types: the reconstructed data buffer and an optional validity buffer.
pub fn decompress_chunk(
    bytes: &[u8],
    pipeline_json: &str,
    original_type: &str,
) -> Result<(Vec<u8>, Option<Vec<u8>>), PhoenixError> {
    // --- Step 1: Unpack the artifact ---
    let artifact = CompressedArtifact::from_bytes(bytes)?;

    // --- Step 2: Decompress the Validity Mask ---
    let validity_bitmask = if artifact.compressed_nullmap.is_empty() {
        None
    } else {
        let rle_decoded = crate::compression::rle::decode(&artifact.compressed_nullmap, "Boolean")?;
        let zstd_decoded = crate::compression::zstd::decompress(&rle_decoded)?;
        Some(zstd_decoded)
    };

    // --- Step 3: Execute the pipeline to decompress the data ---
    let decompressed_data_bytes = executor::execute_decompress_pipeline(
        &artifact.compressed_data,
        original_type,
        pipeline_json,
        artifact.num_valid_rows as usize,
    )?;

    Ok((decompressed_data_bytes, validity_bitmask))
}
//==================================================================================
// 3. Unit Tests (REVISED - Test now reflects correct API and logic)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    // Assuming a utility function exists to convert typed slices to bytes for testing.
    // This would live in `src/utils.rs`.
    use crate::utils::typed_slice_to_bytes;

    /// This is the primary roundtrip test for the orchestrator's core logic.
    /// It simulates a complete compress/decompress cycle with nulls.
    #[test]
    fn test_chunk_roundtrip_with_nulls() {
        // --- Arrange ---
        // 1. Define the original data and validity.
        // This represents the data *after* nulls have been stripped by the FFI layer.
        let original_valid_data: Vec<i64> = vec![100, 110, 90];
        let data_bytes = typed_slice_to_bytes(&original_valid_data);
        
        // This represents the validity bitmap for a conceptual series like [Some(100), None, Some(110), Some(90)]
        let validity_bytes = vec![0b0000_1101u8];
        let original_type = "Int64";

        // 2. Define a simple pipeline for the executor to run.
        let pipeline_json = r#"[{"op": "delta"}, {"op": "zstd"}]"#;

        // --- Act ---
        // 3. Compress the chunk.
        let compressed_bytes = compress_chunk(
            &data_bytes,
            Some(&validity_bytes),
            original_type,
        ).unwrap();

        // 4. Decompress the chunk.
        let (decompressed_data, decompressed_validity) = decompress_chunk(
            &compressed_bytes,
            pipeline_json,
            original_type,
        ).unwrap();

        // --- Assert ---
        // 5. Verify that the data and validity mask are perfectly reconstructed.
        assert_eq!(data_bytes, decompressed_data);
        assert_eq!(validity_bytes, decompressed_validity.unwrap());
    }

    /// Tests the orchestrator's behavior when the input data has no nulls.
    #[test]
    fn test_chunk_roundtrip_no_nulls() {
        // --- Arrange ---
        let original_data: Vec<i32> = vec![10, 20, 15, 30];
        let data_bytes = typed_slice_to_bytes(&original_data);
        let original_type = "Int32";
        let pipeline_json = r#"[{"op": "zstd"}]"#;

        // --- Act ---
        // Compress with `None` for the validity slice.
        let compressed_bytes = compress_chunk(
            &data_bytes,
            None, // No validity mask
            original_type,
        ).unwrap();

        let (decompressed_data, decompressed_validity) = decompress_chunk(
            &compressed_bytes,
            pipeline_json,
            original_type,
        ).unwrap();

        // --- Assert ---
        assert_eq!(data_bytes, decompressed_data);
        assert!(decompressed_validity.is_none(), "Expected no validity mask to be returned");
    }
}