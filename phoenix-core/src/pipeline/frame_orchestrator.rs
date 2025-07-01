//! This module contains the v4.1 orchestration logic for compressing and
//! decompressing entire RecordBatches ("Frames").
//!
//! It acts as a higher-level orchestrator that uses the single-column
//! `orchestrator` as a building block. Its primary responsibilities are
//! multi-column coordination and handling of batch-level structural hints.

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::{json, Value};
use std::io::{Cursor, Read, Write};
use std::sync::Arc;

// Use an alias for clarity, as we now interact with the chunk-level orchestrator.
use super::orchestrator as chunk_orchestrator;
use crate::error::PhoenixError;
use crate::pipeline::artifact::CompressedChunk;

//==================================================================================
// 1. Phoenix Frame Format (.phnx)
//==================================================================================

const MAGIC_NUMBER: &[u8] = b"PHNX";
const FRAME_VERSION: u16 = 1;

//==================================================================================
// 2. Frame Orchestration Logic (v4.2 Corrected)
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

    // 2. Compress each column into a chunk and append it to the frame.
    for i in 0..batch.num_columns() {
        let array = batch.column(i);

        // CORRECTED: A single, high-level call to the chunk orchestrator.
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
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(num_columns);
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

        // CORRECTED: A single, high-level call to the chunk orchestrator.
        let array = chunk_orchestrator::decompress_chunk(chunk_bytes)?;

        fields.push(Field::new(
            format!("col_{}", i),
            array.data_type().clone(),
            array.null_count() > 0,
        ));
        columns.push(array.into());

        cursor.set_position(chunk_end as u64);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| {
        PhoenixError::InternalError(format!("Failed to reconstruct RecordBatch: {}", e))
    })
}

/// Inspects a compressed Phoenix Frame and returns its diagnostic metadata.
pub fn get_frame_diagnostics(bytes: &[u8]) -> Result<String, PhoenixError> {
    let mut cursor = Cursor::new(bytes);

    // Parse header to find number of columns
    cursor.set_position(6); // Skip magic + version
    let mut u32_buf = [0u8; 4];
    cursor
        .read_exact(&mut u32_buf)
        .map_err(|_| PhoenixError::FrameFormatError("Truncated column count".to_string()))?;
    let num_columns = u32::from_le_bytes(u32_buf) as usize;

    let mut all_diagnostics = Vec::new();

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

        // CORRECTED: Parse the chunk artifact to get its plan and other metadata.
        let chunk_artifact = CompressedChunk::from_bytes(chunk_bytes)?;
        let plan_value: Value = serde_json::from_str(&chunk_artifact.plan_json).map_err(|e| {
            PhoenixError::InternalError(format!("Failed to parse plan_json: {}", e))
        })?;

        let col_diag = json!({
            "column_index": i,
            "original_type": chunk_artifact.original_type,
            "total_rows": chunk_artifact.total_rows,
            "plan": plan_value,
            "compressed_size": chunk_bytes.len(),
            "streams": chunk_artifact.compressed_streams.iter().map(|(k, v)| (k.clone(), v.len().into())).collect::<serde_json::Map<String, Value>>(),
        });
        all_diagnostics.push(col_diag);

        cursor.set_position(chunk_end as u64);
    }

    serde_json::to_string_pretty(&all_diagnostics)
        .map_err(|e| PhoenixError::InternalError(e.to_string()))
}

//==================================================================================
// 3. Unit Tests
//==================================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array};

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

        // --- CHECKPOINT ---
        // Let's inspect the plans generated for each column before compression.
        println!("\n--- CHECKPOINT: test_frame_roundtrip_basic ---");
        let plan_col_a = chunk_orchestrator::create_plan(original_batch.column(0)).unwrap();
        let plan_col_b = chunk_orchestrator::create_plan(original_batch.column(1)).unwrap();
        println!("  - Plan for Column A (Int32): {}", plan_col_a);
        println!("  - Plan for Column B (Float64): {}", plan_col_b);
        println!("---------------------------------------------\n");
        // --- END CHECKPOINT ---

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
    fn test_get_frame_diagnostics_is_valid_json() {
        let original_batch = create_test_batch();

        // --- CHECKPOINT ---
        println!("\n--- CHECKPOINT: test_get_frame_diagnostics_is_valid_json ---");
        let plan_col_b = chunk_orchestrator::create_plan(original_batch.column(1)).unwrap();
        println!(
            "  - Pre-compression Plan for Column B (Float64): {}",
            plan_col_b
        );
        println!("-------------------------------------------------------------\n");
        // --- END CHECKPOINT ---

        let compressed_bytes = compress_frame(&original_batch, &None).unwrap();

        let diagnostics_json = get_frame_diagnostics(&compressed_bytes).unwrap();
        let parsed: Result<Value, _> = serde_json::from_str(&diagnostics_json);
        assert!(parsed.is_ok(), "Diagnostics output should be valid JSON");

        let diagnostics_array = parsed.unwrap().as_array().unwrap().to_vec();
        assert_eq!(diagnostics_array.len(), 2); // Two columns
        assert_eq!(diagnostics_array[0]["column_index"], 0);
        assert_eq!(diagnostics_array[1]["original_type"], "Float64");
        assert!(diagnostics_array[1]["streams"].is_object());
    }
}
