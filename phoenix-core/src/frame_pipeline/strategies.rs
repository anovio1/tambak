// In: src/frame_pipeline/strategies.rs

use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchReader, UInt32Array};
use arrow::compute::{sort_to_indices, take, SortOptions};
use std::sync::Arc;

use super::column_strategies::{
    ColumnCompressor, RelinearizationDecorator, StandardColumnCompressor,
};
use super::profiler;
use super::{FramePipeline, FramePipelineResult};
use crate::bridge::arrow_impl;
use crate::bridge::config::CompressorConfig;
use crate::bridge::format::{ChunkManifestEntry, FrameOperation, FramePlan};
use crate::chunk_pipeline::artifact::CompressedChunk;
use crate::chunk_pipeline::orchestrator as chunk_orchestrator;
use crate::error::PhoenixError;

// Helper to compress a single array using the pure chunk pipeline.
fn compress_array_chunk(
    array: &dyn Array,
    col_idx: u32,
) -> Result<(Vec<u8>, ChunkManifestEntry), PhoenixError> {
    let input = arrow_impl::arrow_to_pipeline_input(array)?;
    let compressed_bytes = chunk_orchestrator::compress_chunk(input)?;
    let header_info = CompressedChunk::peek_info(&compressed_bytes)?;
    Ok((
        compressed_bytes,
        ChunkManifestEntry {
            column_idx: col_idx,
            offset_in_file: 0,  // Placeholder
            compressed_size: 0, // Placeholder
            num_rows: header_info.total_rows,
        },
    ))
}

// --- Strategy 1: Standard Streaming ---
pub struct StandardStreamingStrategy;
impl FramePipeline for StandardStreamingStrategy {
    fn execute(
        &self,
        reader: &mut dyn RecordBatchReader,
        _config: &CompressorConfig,
    ) -> Result<FramePipelineResult, PhoenixError> {
        let mut all_chunks = Vec::new();
        let compressor = StandardColumnCompressor;
        for batch_result in reader {
            let batch = batch_result?;
            for col_idx in 0..batch.num_columns() {
                let result = compressor.compress_column(&batch, col_idx)?;
                all_chunks.push((result.compressed_chunk, result.manifest_entry));
            }
        }
        let frame_plan = None; // No special plan needed for standard columns.
        Ok(FramePipelineResult {
            compressed_chunks_with_manifests: all_chunks,
            frame_plan,
        })
    }
}

// --- Strategy 2: Per-Batch Relinearization ---
pub struct PerBatchRelinearizationStrategy;
impl FramePipeline for PerBatchRelinearizationStrategy {
    fn execute(
        &self,
        reader: &mut dyn RecordBatchReader,
        config: &CompressorConfig,
    ) -> Result<FramePipelineResult, PhoenixError> {
        let mut all_chunks = Vec::new();
        let mut frame_ops = Vec::new();

        let hints = profiler::PlannerHints {
            stream_id_column: config.stream_id_column_name.clone(),
            timestamp_column: config.timestamp_column_name.clone(),
        };

        if hints.stream_id_column.is_none() || hints.timestamp_column.is_none() {
            return Err(PhoenixError::RelinearizationError(
                "stream_id_column and timestamp_column hints are required".to_string(),
            ));
        }

        let compressor = RelinearizationDecorator::new(Box::new(StandardColumnCompressor), &hints);

        for batch_result in reader {
            let batch = batch_result?;
            for col_idx in 0..batch.num_columns() {
                let result = compressor.compress_column(&batch, col_idx)?;
                all_chunks.push((result.compressed_chunk, result.manifest_entry));
                if frame_ops.len() < batch.num_columns() {
                    frame_ops.push(result.frame_operation);
                }
            }
        }

        frame_ops.sort_by_key(|op| match op {
            FrameOperation::StandardColumn { logical_col_idx } => *logical_col_idx,
            FrameOperation::PerBatchRelinearizedColumn {
                logical_value_idx, ..
            } => *logical_value_idx,
            _ => u32::MAX,
        });
        frame_ops.dedup();

        let frame_plan = Some(FramePlan {
            version: 1,
            operations: frame_ops,
        });
        Ok(FramePipelineResult {
            compressed_chunks_with_manifests: all_chunks,
            frame_plan,
        })
    }
}

// --- Strategy 3: Global Sorting ---
pub struct GlobalSortingStrategy;
impl FramePipeline for GlobalSortingStrategy {
    fn execute(
        &self,
        reader: &mut dyn RecordBatchReader,
        config: &CompressorConfig,
    ) -> Result<FramePipelineResult, PhoenixError> {
        let mut all_chunks = Vec::new();
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
        if batches.is_empty() {
            return Ok(FramePipelineResult {
                compressed_chunks_with_manifests: vec![],
                frame_plan: None,
            });
        }
        let full_batch = RecordBatch::concat(&batches[0].schema(), &batches)?;

        let key_col_idx = config
            .stream_id_column_name
            .as_ref()
            .and_then(|n| full_batch.schema().index_of(n).ok())
            .ok_or_else(|| {
                PhoenixError::RelinearizationError("Stream ID column hint not found".to_string())
            })?;
        let ts_col_idx = config
            .timestamp_column_name
            .as_ref()
            .and_then(|n| full_batch.schema().index_of(n).ok())
            .ok_or_else(|| {
                PhoenixError::RelinearizationError("Timestamp column hint not found".to_string())
            })?;

        let sorting_keys: Vec<ArrayRef> = vec![
            full_batch.column(key_col_idx).clone(),
            full_batch.column(ts_col_idx).clone(),
        ];
        let sorted_indices_array: Arc<UInt32Array> = Arc::new(sort_to_indices(
            &sorting_keys,
            Some(SortOptions::default()),
            None,
        )?);

        const GLOBAL_PERMUTATION_COL_IDX: u32 = u32::MAX;
        let (perm_bytes, perm_manifest) =
            compress_array_chunk(sorted_indices_array.as_ref(), GLOBAL_PERMUTATION_COL_IDX)?;
        all_chunks.push((perm_bytes, perm_manifest));

        let frame_plan = Some(FramePlan {
            version: 1,
            operations: vec![FrameOperation::GlobalSortedFile {
                key_col_idx: key_col_idx as u32,
                timestamp_col_idx: ts_col_idx as u32,
                permutation_chunk_idx: GLOBAL_PERMUTATION_COL_IDX,
            }],
        });

        for col_idx in 0..full_batch.num_columns() {
            let sorted_col = take(
                full_batch.column(col_idx),
                sorted_indices_array.as_ref(),
                None,
            )?;
            let (chunk_bytes, manifest) =
                compress_array_chunk(sorted_col.as_ref(), col_idx as u32)?;
            all_chunks.push((chunk_bytes, manifest));
        }

        Ok(FramePipelineResult {
            compressed_chunks_with_manifests: all_chunks,
            frame_plan,
        })
    }
}
