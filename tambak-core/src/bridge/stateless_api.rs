// In: src/bridge/stateless_api.rs

use std::sync::Arc;

use crate::bridge::arrow_impl;
use crate::bridge::format::CompressionStats;
use crate::config::TambakConfig;
use crate::error::tambakError;
use crate::chunk_pipeline;
// We need this to call the peek_info function.
use crate::chunk_pipeline::artifact::CompressedChunk;
use crate::chunk_pipeline::context::PipelineOutput;
use arrow::array::Array;

/// Compresses a single Arrow Array into a byte vector.
/// This is the direct replacement for the old `orchestrator::compress_chunk`.
pub fn compress_arrow_chunk(array: &dyn Array) -> Result<Vec<u8>, tambakError> {
    // 1. Marshall the data from the Arrow world into our pure internal format.
    let pipeline_input = arrow_impl::arrow_to_pipeline_input(array)?;

    // Since this is a stateless API, we don't have a user-provided config.
    let default_config = Arc::new(TambakConfig::default());
    // 2. Call the pure pipeline engine with the pure input and default config
    chunk_pipeline::orchestrator::compress_chunk(pipeline_input, default_config)
}

/// Decompresses bytes into a single Arrow Array.
/// This is the direct replacement for the old `orchestrator::decompress_chunk`.
pub fn decompress_arrow_chunk(bytes: &[u8]) -> Result<Box<dyn Array>, tambakError> {
    // 1. Calls v2, gets a PipelineOutput back.
    let pipeline_output: PipelineOutput = chunk_pipeline::orchestrator::decompress_chunk(bytes)?;

    // 2. The bridge finishes the job.
    arrow_impl::pipeline_output_to_array(pipeline_output)
}

/// Analyzes a compressed chunk without fully decompressing the data.
/// This function acts as a simple facade over the robust `peek_info` method.
pub fn analyze_chunk(bytes: &[u8]) -> Result<CompressionStats, tambakError> {
    // 1. Delegate to the pure, efficient "peek" function in the artifact module.
    let info = CompressedChunk::peek_info(bytes)?;

    // 2. Translate the detailed HeaderInfo into the public-facing CompressionStats struct.
    Ok(CompressionStats {
        header_size: info.header_size,
        data_size: info.data_size,
        total_size: bytes.len(),
        plan_json: info.plan_json,
        original_type: info.original_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array};

    #[test]
    fn test_analyze_chunk_after_compression() {
        // 1. Arrange: Create a sample Arrow array.
        let array = Int32Array::from(vec![Some(10), Some(20), None, Some(30), Some(40)]);

        // 2. Act: Compress it using our bridge's public API.
        let compressed_bytes = compress_arrow_chunk(&array).unwrap();

        // 3. Act: Analyze the result using the function we just implemented.
        let stats = analyze_chunk(&compressed_bytes).unwrap();

        // 4. Assert: Check for reasonable results.
        assert_eq!(stats.total_size, compressed_bytes.len());
        assert_eq!(stats.header_size + stats.data_size, stats.total_size);
        assert_eq!(stats.original_type, "Int32");
        assert!(stats.plan_json.contains("ExtractNulls"));
        assert!(stats.header_size > 0);
        assert!(stats.data_size > 0);
    }
}
