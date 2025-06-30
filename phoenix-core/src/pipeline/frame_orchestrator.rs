//! This module contains the v4.0 orchestration logic for compressing and
//! decompressing entire RecordBatches ("Frames").
//!
//! It acts as a higher-level orchestrator that uses the single-column
//! `orchestrator` as a building block. Its primary responsibilities are
//! multi-column coordination and handling of batch-level structural hints.

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use std::io::{Cursor, Read, Write};
use std::sync::Arc;

use super::orchestrator as chunk_orchestrator; // The v3.9 single-column logic
use crate::error::PhoenixError;

//==================================================================================
// 1. Phoenix Frame Format (.phnx)
//==================================================================================

const MAGIC_NUMBER: &[u8] = b"PHNX";
const FRAME_VERSION: u16 = 1;

//==================================================================================
// 2. Frame Orchestration Logic
//==================================================================================

/// Compresses an entire RecordBatch into a Phoenix Frame artifact.
pub fn compress_frame(
    batch: &RecordBatch,
    _hints: &Option<super::profiler::PlannerHints>, // Placeholder for future use
) -> Result<Vec<u8>, PhoenixError> {
    let mut final_buffer = Vec::new();

    // 1. Write Frame Header
    final_buffer
        .write_all(MAGIC_NUMBER)
        .map_err(|e| PhoenixError::FrameFormatError(e.to_string()))?;
    final_buffer
        .write_all(&FRAME_VERSION.to_le_bytes())
        .map_err(|e| PhoenixError::FrameFormatError(e.to_string()))?;
    final_buffer
        .write_all(&(batch.num_columns() as u32).to_le_bytes())
        .map_err(|e| PhoenixError::FrameFormatError(e.to_string()))?;

    // TODO: Here is where batch-level pre-computation, like relinearization, would be called.

    // 2. Compress each column into a chunk and append it to the frame.
    for i in 0..batch.num_columns() {
        let array = batch.column(i);
        let chunk_bytes = chunk_orchestrator::compress_chunk(array.as_ref())?;
        final_buffer
            .write_all(&(chunk_bytes.len() as u64).to_le_bytes())
            .map_err(|e| PhoenixError::FrameFormatError(e.to_string()))?;
        final_buffer
            .write_all(&chunk_bytes)
            .map_err(|e| PhoenixError::FrameFormatError(e.to_string()))?;
    }

    Ok(final_buffer)
}

/// Decompresses a Phoenix Frame artifact back into a RecordBatch.
pub fn decompress_frame(bytes: &[u8]) -> Result<RecordBatch, PhoenixError> {
    let mut cursor = Cursor::new(bytes);

    // 1. Parse and Validate Frame Header
    let mut magic_buf = [0u8; 4];
    cursor
        .read_exact(&mut magic_buf)
        .map_err(|_| PhoenixError::FrameFormatError("Truncated magic number".to_string()))?;
    if magic_buf != MAGIC_NUMBER {
        return Err(PhoenixError::FrameFormatError(
            "Invalid magic number".to_string(),
        ));
    }

    let mut u16_buf = [0u8; 2];
    cursor
        .read_exact(&mut u16_buf)
        .map_err(|_| PhoenixError::FrameFormatError("Truncated version".to_string()))?;
    let version = u16::from_le_bytes(u16_buf);
    if version != FRAME_VERSION {
        return Err(PhoenixError::FrameFormatError(format!(
            "Unsupported frame version: {}",
            version
        )));
    }

    let mut u32_buf = [0u8; 4];
    cursor
        .read_exact(&mut u32_buf)
        .map_err(|_| PhoenixError::FrameFormatError("Truncated column count".to_string()))?;
    let num_columns = u32::from_le_bytes(u32_buf) as usize;

    // 2. Loop and decompress each chunk.
    let mut columns: Vec<Box<dyn Array>> = Vec::with_capacity(num_columns);
    let mut fields = Vec::with_capacity(num_columns);

    for i in 0..num_columns {
        let mut u64_buf = [0u8; 8];
        cursor.read_exact(&mut u64_buf).map_err(|_| {
            PhoenixError::FrameFormatError(format!("Truncated chunk length for column {}", i))
        })?;
        let chunk_len = u64::from_le_bytes(u64_buf) as usize;

        let chunk_start = cursor.position() as usize;
        let chunk_end = chunk_start + chunk_len;
        let chunk_bytes = bytes.get(chunk_start..chunk_end).ok_or_else(|| {
            PhoenixError::FrameFormatError(format!("Chunk data out of bounds for column {}", i))
        })?;

        let array = chunk_orchestrator::decompress_chunk(chunk_bytes)?;

        fields.push(Field::new(
            format!("col_{}", i),
            array.data_type().clone(),
            array.null_count() > 0,
        ));
        columns.push(array);

        cursor.set_position(chunk_end as u64);
    }

    // --- THIS IS THE FIX ---
    // Convert Vec<Box<dyn Array>> to Vec<Arc<dyn Array>> for RecordBatch::try_new
    let columns: Vec<Arc<dyn Array>> = columns.into_iter().map(|b| b.into()).collect();
    // --- END FIX ---

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| {
        PhoenixError::InternalError(format!("Failed to reconstruct RecordBatch: {}", e))
    })
}

/// Inspects a compressed Phoenix Frame and returns its diagnostic metadata.
pub fn get_frame_diagnostics(_bytes: &[u8]) -> Result<String, PhoenixError> {
    Ok("Frame diagnostics not yet implemented for v4.0.".to_string())
}

//==================================================================================
// 3. Unit Tests
//==================================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::Float64Type;

    /// A helper to create a sample RecordBatch for testing.
    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Float64, true),
        ]));

        let col_a = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ])) as Arc<dyn Array>;
        let col_b = Arc::new(Float64Array::from(vec![
            Some(1.1),
            Some(2.2),
            Some(3.3),
            None,
            Some(5.5),
        ])) as Arc<dyn Array>;

        RecordBatch::try_new(schema, vec![col_a, col_b]).unwrap()
    }

    #[test]
    fn test_frame_roundtrip_basic() {
        let original_batch = create_test_batch();
        let hints = None;

        // 1. Compress
        let compressed_bytes = compress_frame(&original_batch, &hints).expect("Compression failed");
        assert!(!compressed_bytes.is_empty());

        // 2. Decompress
        let decompressed_batch = decompress_frame(&compressed_bytes).expect("Decompression failed");

        // 3. Verify
        assert_eq!(
            original_batch.num_columns(),
            decompressed_batch.num_columns()
        );
        assert_eq!(original_batch.num_rows(), decompressed_batch.num_rows());

        // Check column data types and values
        let original_col_a = original_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let decompressed_col_a = decompressed_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(original_col_a, decompressed_col_a);

        let original_col_b = original_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let decompressed_col_b = decompressed_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(original_col_b, decompressed_col_b);
    }

    #[test]
    fn test_frame_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let original_batch = RecordBatch::new_empty(schema);
        let hints = None;

        let compressed_bytes = compress_frame(&original_batch, &hints).unwrap();
        let decompressed_batch = decompress_frame(&compressed_bytes).unwrap();

        assert_eq!(original_batch.num_rows(), 0);
        assert_eq!(decompressed_batch.num_rows(), 0);
        assert_eq!(
            original_batch.num_columns(),
            decompressed_batch.num_columns()
        );
    }

    #[test]
    fn test_decompress_corrupt_header() {
        // Test various forms of corruption
        let res1 = decompress_frame(b"BAD_MAGIC");
        assert!(matches!(res1, Err(PhoenixError::FrameFormatError(_))));

        let res2 = decompress_frame(&[b'P', b'H', b'N', b'X', 0, 2]); // Wrong version
        assert!(matches!(res2, Err(PhoenixError::FrameFormatError(_))));

        let res3 = decompress_frame(&[b'P', b'H', b'N', b'X', 0, 1, 0, 0, 0, 1, 0]); // Truncated
        assert!(matches!(res3, Err(PhoenixError::FrameFormatError(_))));
    }
}
