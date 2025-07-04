// In: src/frame_pipeline/column_strategies.rs

//! Implements the Column-Level Compression Decorator Pattern.
//!
//! These strategies are used by the higher-level FramePipeline strategies
//! to process individual columns within a RecordBatch. Their responsibility is to
//! decide if a structural transformation (like re-linearization) should be applied
//! to a column's data *before* passing the resulting byte stream to the pure
//! `chunk_pipeline` for compression.
//!
//! The result of this process includes the `FrameOperation` that describes the
//! transformation, which is used to build the file's `FramePlan`.

use super::profiler::{self, DataStructure};
use super::relinearize;
use crate::bridge::arrow_impl;
use crate::bridge::format::{ChunkManifestEntry, FrameOperation}; // Use the authoritative FrameOperation
use crate::chunk_pipeline::artifact::CompressedChunk;
use crate::chunk_pipeline::orchestrator as chunk_orchestrator;
use crate::error::PhoenixError;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;

/// **CONTRACT:** The result of compressing a single column. It contains the
/// physical chunk data and the high-level structural metadata needed by the FramePipeline.
pub struct ColumnCompressionResult {
    /// The compressed bytes of the column's data, produced by the `chunk_pipeline`.
    pub compressed_chunk: Vec<u8>,
    /// The manifest entry describing the location and size of the physical chunk.
    pub manifest_entry: ChunkManifestEntry,
    /// **The crucial contract:** The `FrameOperation` that describes how this column was
    /// structurally handled, to be included in the file's `FramePlan`.
    pub frame_operation: FrameOperation,
}

/// **CONTRACT:** A trait for an object that knows how to compress a single column
/// within a `RecordBatch` context. It delegates the final byte-stream compression
/// to the `chunk_pipeline`.
pub trait ColumnCompressor {
    fn compress_column(
        &self,
        batch: &RecordBatch,
        col_idx: usize,
    ) -> Result<ColumnCompressionResult, PhoenixError>;
}

//==================================================================================
// Concrete Implementations
//==================================================================================

// --- 1. The Base Case: StandardColumnCompressor ---
/// Compresses a column without any structural transformations.
pub struct StandardColumnCompressor;
impl ColumnCompressor for StandardColumnCompressor {
    fn compress_column(
        &self,
        batch: &RecordBatch,
        col_idx: usize,
    ) -> Result<ColumnCompressionResult, PhoenixError> {
        let column_array = batch.column(col_idx);

        // 1. Marshall Arrow Array to pure PipelineInput for the chunk_pipeline.
        let input = arrow_impl::arrow_to_pipeline_input(column_array.as_ref())?;

        // 2. Delegate compression to the pure chunk_pipeline orchestrator.
        let compressed_chunk = chunk_orchestrator::compress_chunk(input)?;
        let header_info = CompressedChunk::peek_info(&compressed_chunk)?;

        // 3. Build the result.
        Ok(ColumnCompressionResult {
            compressed_chunk,
            manifest_entry: ChunkManifestEntry {
                column_idx: col_idx as u32,
                offset_in_file: 0, // Placeholder, filled in by the bridge::Compressor
                compressed_size: 0, // Placeholder
                num_rows: header_info.total_rows,
            },
            // The FrameOperation for this is simple: it's a standard column.
            frame_operation: FrameOperation::StandardColumn {
                logical_col_idx: col_idx as u32,
            },
        })
    }
}

// --- 2. The Decorator: RelinearizationDecorator ---
/// A decorator that wraps another `ColumnCompressor` and adds the ability
/// to perform per-batch re-linearization on candidate columns.
pub struct RelinearizationDecorator<'a> {
    /// The `ColumnCompressor` (e.g., `StandardColumnCompressor`) to delegate to if
    /// re-linearization is not applicable.
    wrapped_compressor: Box<dyn ColumnCompressor>,
    /// Frame-level hints required to identify key/timestamp columns.
    hints: &'a profiler::PlannerHints,
}

impl<'a> RelinearizationDecorator<'a> {
    pub fn new(wrapped: Box<dyn ColumnCompressor>, hints: &'a profiler::PlannerHints) -> Self {
        Self {
            wrapped_compressor: wrapped,
            hints,
        }
    }
}

impl<'a> ColumnCompressor for RelinearizationDecorator<'a> {
    fn compress_column(
        &self,
        batch: &RecordBatch,
        col_idx: usize,
    ) -> Result<ColumnCompressionResult, PhoenixError> {
        // --- Decision Logic: Should we re-linearize this column? ---

        // 1. Check if the necessary hints and columns exist in this batch.
        let key_col_idx_opt = self
            .hints
            .stream_id_column
            .as_ref()
            .and_then(|n| batch.schema().index_of(n).ok());
        let ts_col_idx_opt = self
            .hints
            .timestamp_column
            .as_ref()
            .and_then(|n| batch.schema().index_of(n).ok());

        if let (Some(key_col_idx), Some(ts_col_idx)) = (key_col_idx_opt, ts_col_idx_opt) {
            // 2. Don't re-linearize the key or timestamp columns themselves.
            if col_idx == key_col_idx || col_idx == ts_col_idx {
                return self.wrapped_compressor.compress_column(batch, col_idx);
            }

            // 3. Profile the column to see if it's a candidate for multiplexing.
            let structure = profiler::discover_structure(batch.column(col_idx), batch, self.hints)?;
            if structure == DataStructure::Multiplexed {
                // --- Action: Perform re-linearization ---
                let re_linearized_array = relinearize::relinearize_single_column_in_batch(
                    batch.column(col_idx),
                    batch.column(key_col_idx),
                    batch.column(ts_col_idx),
                )?;

                // Compress the *re-linearized* data using the chunk_pipeline.
                let input = arrow_impl::arrow_to_pipeline_input(re_linearized_array.as_ref())?;
                let compressed_chunk = chunk_orchestrator::compress_chunk(input)?;
                let header_info = CompressedChunk::peek_info(&compressed_chunk)?;

                return Ok(ColumnCompressionResult {
                    compressed_chunk,
                    manifest_entry: ChunkManifestEntry {
                        column_idx: col_idx as u32,
                        offset_in_file: 0,  // Placeholder
                        compressed_size: 0, // Placeholder
                        num_rows: header_info.total_rows,
                    },
                    // The FrameOperation clearly states what was done.
                    frame_operation: FrameOperation::PerBatchRelinearizedColumn {
                        logical_value_idx: col_idx as u32,
                        key_col_idx: key_col_idx as u32,
                        timestamp_col_idx: ts_col_idx as u32,
                    },
                });
            }
        }

        // --- Delegation: If any check fails, delegate to the wrapped compressor. ---
        self.wrapped_compressor.compress_column(batch, col_idx)
    }
}
