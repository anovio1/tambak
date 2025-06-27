//! This module is the FFI "Anti-Corruption Layer" for the Phoenix library.
//! Its ONLY responsibility is to translate Python objects (Polars Series)
//! into pure Rust types (&[u8], &str) to pass to the core library, and to
//! translate the results back into Python objects. It contains ALL `polars`
//! and `pyo3` specific code.

use pyo3::prelude::*;
use polars::prelude::*;

use crate::pipeline::orchestrator;
use crate::ffi_utils; // Assuming ffi_utils handles PyArrow -> Series conversion

#[pyfunction]
/// The public FFI entry point for compressing a Polars Series.
/// This function extracts raw byte slices from the Series and passes them
/// to the pure Rust orchestration core, avoiding any large memory copies at this layer.
fn compress_series_ffi(series_py: &PyAny) -> PyResult<Vec<u8>> {
    // 1. Convert Python object to a Rust Polars Series (this is the FFI boundary).
    let series = ffi_utils::pyarrow_to_series(series_py)?;

    // 2. Get zero-copy slices of the underlying data and validity mask.
    let phys_series = series.to_physical_repr();
    let (data_slice, validity_slice_opt) = phys_series.to_byte_slices();
    
    let original_type = series.dtype().to_string();

    // 3. Call the pure Rust orchestrator with slices.
    orchestrator::compress_chunk(
        data_slice,
        validity_slice_opt,
        &original_type
    )
}

#[pyfunction]
/// The public FFI entry point for decompressing a byte artifact into a Polars Series.
fn decompress_series_ffi(
    py: Python,
    bytes: &[u8],
    pipeline_json: &str,
    original_type: &str,
) -> PyResult<PyObject> {
    // 1. Call the pure Rust orchestrator. It returns pure Rust types.
    let (reconstructed_data, reconstructed_validity) = orchestrator::decompress_chunk(
        bytes,
        pipeline_json,
        original_type,
    )?;

    // 2. Reconstruct a Polars Series from the pure Rust results.
    let series = ffi_utils::reconstruct_series(
        &reconstructed_data,
        reconstructed_validity,
        original_type
    )?;

    // 3. Convert the Rust Polars Series back to a Python object.
    ffi_utils::series_to_pyarrow(py, &series)
}

#[pymodule]
fn libphoenix(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compress_series_ffi, m)?)?;
    m.add_function(wrap_pyfunction!(decompress_series_ffi, m)?)?;
    Ok(())
}


/// The public-facing compression orchestrator for this module.
pub fn compress(series: &Series) -> PyResult<Vec<u8>> {
    // --- Step 1: Null Handling & Metadata Extraction ---
    let num_valid_rows = (series.len() - series.null_count()) as u64; // <-- FIX #1
    
    let (data_bytes, validity_bytes) = {
        let phys_series = series.to_physical_repr();
        let (data_bytes, validity_opt) = phys_series.to_byte_slices();
        let validity_bitmask = validity_opt.map(|v| v.as_slice().to_vec()).unwrap_or_default();
        (data_bytes.to_vec(), validity_bitmask)
    };

    // --- Step 2: Compress the Validity Mask ---
    let compressed_nullmap = if validity_bytes.is_empty() {
        Vec::new()
    } else {
        let rle_encoded = crate::compression::rle::encode(&validity_bytes, "Boolean")?;
        crate::compression::zstd::compress(&rle_encoded, 19)?
    };

    // --- Step 3 & 4: Plan and Execute pipeline on the valid data ---
    let original_type = series.dtype().to_string();
    let pipeline_json = planner::plan_pipeline(&data_bytes, &original_type)?;
    let compressed_data = executor::execute_compress_pipeline(&data_bytes, &original_type, &pipeline_json)?;

    // --- Step 5: Package the final artifact ---
    let artifact = CompressedArtifact {
        num_valid_rows,
        compressed_nullmap,
        compressed_data,
    };

    Ok(artifact.to_bytes().map_err(|e| PhoenixError::ZstdError(e.to_string()))?) // <-- FIX #2
}

/// The public-facing decompression orchestrator for this module.
/// The `num_rows` parameter is REMOVED from the signature.

pub fn decompress(
    bytes: &[u8],
    pipeline_json: &str,
    original_type: &str,
) -> PyResult<Series> { // <-- FIX #1
    // --- Step 1: Unpack the artifact ---
    let artifact = CompressedArtifact::from_bytes(bytes)?;

    // --- Step 2: Decompress the Validity Mask ---
    let validity_bitmask = if artifact.compressed_nullmap.is_empty() {
        None // No nulls were present in the original series
    } else {
        let rle_decoded = crate::compression::rle::decode(&artifact.compressed_nullmap, "Boolean")?;
        let zstd_decoded = crate::compression::zstd::decompress(&rle_decoded)?;
        Some(arrow::buffer::Buffer::from(zstd_decoded))
    };

    // --- Step 3: Execute the pipeline to decompress the data ---
    let decompressed_data_bytes = executor::execute_decompress_pipeline(
        &artifact.compressed_data,
        original_type,
        pipeline_json,
        artifact.num_valid_rows as usize, // Use self-contained row count <-- FIX #1
    )?;

    Ok((decompressed_data_bytes, validity_bitmask))
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orchestrator_roundtrip_with_nulls_and_correct_api() {
        let original_series = Series::new("test", &[Some(100i64), Some(110), None, Some(110), Some(90)]);
        
        // --- Compression ---
        // The orchestrator now handles everything internally.
        let compressed_artifact_bytes = compress_series(&original_series).unwrap();

        // --- Decompression ---
        // The pipeline would normally be stored in the manifest. We get it from the planner.
        let (data_bytes, _) = original_series.to_physical_repr().to_byte_slices();
        let pipeline_json = planner::plan_pipeline(&data_bytes.to_vec(), "Int64").unwrap();
        
        // Note the simplified, robust API call without `num_rows`.
        let decompressed_series = decompress_series(
            &compressed_artifact_bytes,
            &pipeline_json,
            "Int64",
        ).unwrap();

        // Assert that the reconstructed series is identical to the original
        assert!(original_series.equals(&decompressed_series));
    }

    #[test]
    fn test_artifact_serialization_robustness() {
        let artifact = CompressedArtifact {
            num_valid_rows: 123,
            compressed_nullmap: vec![1, 2, 3],
            compressed_data: vec![4, 5, 6, 7, 8],
        };

        let bytes = artifact.to_bytes().unwrap();
        assert_eq!(bytes.len(), 16 + 3 + 5);

        let reconstructed_artifact = CompressedArtifact::from_bytes(&bytes).unwrap();
        assert_eq!(artifact.num_valid_rows, reconstructed_artifact.num_valid_rows);
        assert_eq!(artifact.compressed_nullmap, reconstructed_artifact.compressed_nullmap);
        assert_eq!(artifact.compressed_data, reconstructed_artifact.compressed_data);
    }
}