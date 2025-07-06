// In: src/frame_pipeline/strategies.rs
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, RecordBatchReader, UInt32Array};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, SchemaRef};

use super::column_strategies::{
    ColumnCompressor, RelinearizationDecorator, StandardColumnCompressor,
};
use super::profiler;
use super::util::create_index_map;
use super::{FramePipeline, FramePipelineResult};
use crate::bridge::arrow_impl;
use crate::config::{TambakConfig, TimeSeriesStrategy};
use crate::bridge::format::{ChunkManifestEntry, FrameOperation, FramePlan};
use crate::chunk_pipeline::artifact::CompressedChunk;
use crate::chunk_pipeline::orchestrator as chunk_orchestrator;
use crate::error::tambakError;

// Helper to compress a single array using the pure chunk pipeline.
fn compress_array_chunk(
    array: &dyn Array,
    col_idx: u32,
    config: Arc<TambakConfig>,
) -> Result<(Vec<u8>, ChunkManifestEntry), tambakError> {
    let input = arrow_impl::arrow_to_pipeline_input(array)?;
    let compressed_bytes = chunk_orchestrator::compress_chunk(input, config)?;
    let header_info = CompressedChunk::peek_info(&compressed_bytes)?;
    Ok((
        compressed_bytes,
        ChunkManifestEntry {
            batch_id: 0,
            column_idx: col_idx,
            offset_in_file: 0,  // Placeholder
            compressed_size: 0, // Placeholder
            num_rows: header_info.total_rows,
            partition_key: None,
        },
    ))
}

//
//==================================================================================
// --- Strategy 1: Standard Streaming ---
//==================================================================================

pub struct StandardStreamingStrategy;
impl FramePipeline for StandardStreamingStrategy {
    fn execute(
        &self,
        reader: &mut dyn RecordBatchReader,
        config: Arc<TambakConfig>,
    ) -> Result<FramePipelineResult, tambakError> {
        let mut all_chunks = Vec::new();
        let compressor = StandardColumnCompressor;
        let mut batch_id_counter: u64 = 0;

        for batch_result in reader {
            let batch = batch_result?;
            
            let chunk_size = config.chunk_size_rows;
            let mut offset = 0;
            while offset < batch.num_rows() {
                let length = std::cmp::min(chunk_size, batch.num_rows() - offset);
                let sliced_batch = batch.slice(offset, length);

                for col_idx in 0..sliced_batch.num_columns() {
                    // Pass the config down to the column compressor
                    let result = compressor.compress_column(&sliced_batch, col_idx, Arc::clone(&config))?;
                    let (chunk_bytes, mut manifest) = (result.compressed_chunk, result.manifest_entry);
                    
                    manifest.batch_id = batch_id_counter;
                    all_chunks.push((chunk_bytes, manifest));
                }
                batch_id_counter += 1;
                offset += length;
            }
        }
        let frame_plan = None; // No special plan needed for standard columns.
        Ok(FramePipelineResult {
            compressed_chunks_with_manifests: all_chunks,
            frame_plan,
        })
    }
}

//
//==================================================================================
// --- Strategy 2: Per-Batch Relinearization ---
//==================================================================================

pub struct PerBatchRelinearizationStrategy;
impl FramePipeline for PerBatchRelinearizationStrategy {
    fn execute(
        &self,
        reader: &mut dyn RecordBatchReader,
        config: Arc<TambakConfig>,
    ) -> Result<FramePipelineResult, tambakError> {
        let mut all_chunks = Vec::new();
        let mut frame_ops = Vec::new();
        let mut batch_id_counter: u64 = 0;

        let hints = profiler::PlannerHints {
            stream_id_column: config.stream_id_column_name.clone(),
            timestamp_column: config.timestamp_column_name.clone(),
        };

        if hints.stream_id_column.is_none() || hints.timestamp_column.is_none() {
            return Err(tambakError::RelinearizationError(
                "stream_id_column and timestamp_column hints are required for PerBatchRelinearizationStrategy".to_string(),
            ));
        }

        let compressor = RelinearizationDecorator::new(Box::new(StandardColumnCompressor), &hints);

        for batch_result in reader {
            let batch = batch_result?;

            let chunk_size = config.chunk_size_rows;
            let mut offset = 0;
            while offset < batch.num_rows() {
                let length = std::cmp::min(chunk_size, batch.num_rows() - offset);
                let sliced_batch = batch.slice(offset, length);

                for col_idx in 0..sliced_batch.num_columns() {
                    // Pass the config down to the column compressor
                    let result = compressor.compress_column(&sliced_batch, col_idx, Arc::clone(&config))?;

                    let (chunk_bytes, mut manifest) = (result.compressed_chunk, result.manifest_entry);
                    manifest.batch_id = batch_id_counter;
                    all_chunks.push((chunk_bytes, manifest));

                    // Frame operations are consistent across slices of a batch, so we only
                    // need to collect them once for the first slice.
                    if offset == 0 && frame_ops.len() < sliced_batch.num_columns() {
                        frame_ops.push(result.frame_operation);
                    }
                }
                batch_id_counter += 1;
                offset += length;
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

//==================================================================================
// --- Strategy 3: Partitioning ---
//==================================================================================

struct PartitionBuffer {
    batches: Vec<RecordBatch>,
    total_rows: usize,
}
impl PartitionBuffer {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            total_rows: 0,
        }
    }
    fn add_batch(&mut self, batch: RecordBatch) {
        self.total_rows += batch.num_rows();
        self.batches.push(batch);
    }
    fn clear(&mut self) {
        self.batches.clear();
        self.total_rows = 0;
    }
}

pub struct PartitioningStrategy;
const MAX_ACTIVE_PARTITIONS: usize = 1_000_000;

impl FramePipeline for PartitioningStrategy {
    fn execute(
        &self,
        reader: &mut dyn RecordBatchReader,
        config: Arc<TambakConfig>,
    ) -> Result<FramePipelineResult, tambakError> {
        // correctly uses `partition_flush_rows` instad of `chunk_size_rows`
        let (key_column, partition_flush_rows) =
            if let TimeSeriesStrategy::Partitioned {
                key_column,
                partition_flush_rows,
            } = &config.time_series_strategy
            {
                (key_column, *partition_flush_rows)
            } else {
                return Err(tambakError::InternalError(
                    "PartitioningStrategy called with invalid config".into(),
                ));
            };

        let schema = reader.schema();
        let key_col_idx = schema.index_of(key_column)?;

        if *schema.field(key_col_idx).data_type() != DataType::Int64 {
            return Err(tambakError::UnsupportedType(
                "Partitioning key column must be of type Int64.".to_string(),
            ));
        }

        let mut active_partitions: HashMap<i64, PartitionBuffer> = HashMap::new();
        let mut finished_chunks = Vec::new();
        let mut eviction_queue: BinaryHeap<(Reverse<usize>, i64)> = BinaryHeap::new();
        let mut batch_id_counter: u64 = 0;

        for batch_result in reader {
            let batch = batch_result?;
            let index_map = create_index_map(&batch, key_col_idx)?;

            for (key, indices) in index_map {
                if !active_partitions.contains_key(&key)
                    && active_partitions.len() >= MAX_ACTIVE_PARTITIONS
                {
                    while let Some((_, key_to_evict)) = eviction_queue.pop() {
                        if let Some(mut buffer_to_flush) = active_partitions.remove(&key_to_evict) {
                            flush_partition_buffer(
                                &mut buffer_to_flush,
                                key_to_evict,
                                &schema,
                                &mut finished_chunks,
                                batch_id_counter,
                                Arc::clone(&config),
                            )?;
                            batch_id_counter += 1;
                            break;
                        }
                    }
                }

                let indices_array = UInt32Array::from(indices);
                let mut new_columns = Vec::with_capacity(batch.num_columns());
                for col in batch.columns() {
                    new_columns.push(take(col.as_ref(), &indices_array, None)?);
                }
                let key_specific_batch = RecordBatch::try_new(schema.clone(), new_columns)?;

                let partition_buffer = active_partitions
                    .entry(key)
                    .or_insert_with(PartitionBuffer::new);
                partition_buffer.add_batch(key_specific_batch);
                eviction_queue.push((Reverse(partition_buffer.total_rows), key));

                if partition_buffer.total_rows >= partition_flush_rows {
                    flush_partition_buffer(
                        partition_buffer,
                        key,
                        &schema,
                        &mut finished_chunks,
                        batch_id_counter,
                        Arc::clone(&config),
                    )?;
                    batch_id_counter += 1;
                }
            }
        }

        for (key, mut partition_buffer) in active_partitions {
            if !partition_buffer.batches.is_empty() {
                flush_partition_buffer(
                    &mut partition_buffer,
                    key,
                    &schema,
                    &mut finished_chunks,
                    batch_id_counter,
                    Arc::clone(&config),
                )?;
                batch_id_counter += 1;
            }
        }

        let frame_plan = Some(FramePlan {
            version: 1,
            operations: vec![FrameOperation::PartitionedFile {
                partition_key_col_idx: key_col_idx as u32,
            }],
        });

        Ok(FramePipelineResult {
            compressed_chunks_with_manifests: finished_chunks,
            frame_plan,
        })
    }
}

fn flush_partition_buffer(
    buffer: &mut PartitionBuffer,
    key: i64,
    schema: &SchemaRef,
    out_chunks: &mut Vec<(Vec<u8>, ChunkManifestEntry)>,
    batch_id: u64,
    config: Arc<TambakConfig>,
) -> Result<(), tambakError> {
    if buffer.batches.is_empty() {
        return Ok(());
    }
    let full_batch = concat_batches(schema, &buffer.batches)?;
    for i in 0..full_batch.num_columns() {
        let (bytes, mut manifest) = compress_array_chunk(full_batch.column(i), i as u32, Arc::clone(&config))?;
        manifest.batch_id = batch_id;
        manifest.partition_key = Some(key);
        out_chunks.push((bytes, manifest));
    }
    buffer.clear();
    Ok(())
}
