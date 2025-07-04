// In: src/bridge/decompressor.rs

use crate::bridge::format::{
    ChunkManifestEntry, FileFooter, FrameOperation, FramePlan, FILE_FORMAT_VERSION, FILE_MAGIC,
};
use crate::chunk_pipeline::orchestrator::decompress_chunk;
use crate::error::PhoenixError;
use crate::frame_pipeline::relinearize;
use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchReader};
use arrow::compute::take;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use std::collections::{BTreeMap, HashMap};
use std::io::{Error as IoError, Read, Seek, SeekFrom};
use std::sync::Arc;

//==================================================================================
// I. Decompression Mode Contract
//==================================================================================

/// A trait defining the contract for a specific file decompression strategy.
/// This allows the `PhoenixBatchReader` to be agnostic of the underlying file structure.
trait DecompressionMode<R: Read + Seek>: Send {
    /// Reads and reconstructs the next `RecordBatch` according to the strategy.
    fn next_batch(&mut self) -> Option<ArrowResult<RecordBatch>>;
    /// Returns the schema of the file.
    fn schema(&self) -> SchemaRef;
}

// Helper to read and decompress a single physical chunk.
fn read_and_decompress_chunk_common<R: Read + Seek>(
    source: &mut R,
    chunk_info: &ChunkManifestEntry,
) -> ArrowResult<ArrayRef> {
    source.seek(SeekFrom::Start(chunk_info.offset_in_file))?;
    let mut chunk_buffer = vec![0; chunk_info.compressed_size as usize];
    source.read_exact(&mut chunk_buffer)?;
    let array =
        decompress_chunk(&chunk_buffer).map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
    Ok(array.into())
}

//==================================================================================
// II. Concrete Decompression Mode Implementations
//==================================================================================

// --- Mode 1: Standard Streaming (Default) ---
struct StandardStreamer<R: Read + Seek> {
    source: R,
    schema: SchemaRef,
    manifest_iter: std::collections::btree_map::IntoIter<u64, Vec<ChunkManifestEntry>>,
}

impl<R: Read + Seek> DecompressionMode<R> for StandardStreamer<R> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn next_batch(&mut self) -> Option<ArrowResult<RecordBatch>> {
        self.manifest_iter.next().map(|(_, batch_chunks)| {
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
            let mut sorted_chunks = batch_chunks;
            sorted_chunks.sort_by_key(|c| c.column_idx);

            for chunk_info in sorted_chunks {
                let array = read_and_decompress_chunk_common(&mut self.source, &chunk_info)?;
                columns.push(array);
            }
            RecordBatch::try_new(self.schema.clone(), columns)
        })
    }
}

// --- Mode 2: Per-Batch Relinearization ---
struct PerBatchRelinearizationStreamer<R: Read + Seek> {
    source: R,
    schema: SchemaRef,
    manifest_iter: std::collections::btree_map::IntoIter<u64, Vec<ChunkManifestEntry>>,
    relin_lookup: BTreeMap<u32, (u32, u32)>, // Map: value_col -> (key_col, ts_col)
}

impl<R: Read + Seek> DecompressionMode<R> for PerBatchRelinearizationStreamer<R> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn next_batch(&mut self) -> Option<ArrowResult<RecordBatch>> {
        self.manifest_iter.next().map(|(_, batch_physical_chunks)| {
            let mut columns = vec![None; self.schema.fields().len()];
            let mut decompressed_cache: HashMap<u32, ArrayRef> = HashMap::new();

            for chunk_info in &batch_physical_chunks {
                let array = read_and_decompress_chunk_common(&mut self.source, chunk_info)?;
                decompressed_cache.insert(chunk_info.column_idx, array);
            }

            for (col_idx, field) in self.schema.fields().iter().enumerate() {
                let col_idx_u32 = col_idx as u32;

                if let Some(&(key_col_idx, ts_col_idx)) = self.relin_lookup.get(&col_idx_u32) {
                    // This is a re-linearized value column. We need to reconstruct it.
                    let relin_values = decompressed_cache.get(&col_idx_u32).ok_or_else(|| {
                        ArrowError::from(IoError::new(
                            std::io::ErrorKind::InvalidData,
                            "Missing value chunk",
                        ))
                    })?;
                    let keys = decompressed_cache.get(&key_col_idx).ok_or_else(|| {
                        ArrowError::from(IoError::new(
                            std::io::ErrorKind::InvalidData,
                            "Missing key chunk",
                        ))
                    })?;
                    let timestamps = decompressed_cache.get(&ts_col_idx).ok_or_else(|| {
                        ArrowError::from(IoError::new(
                            std::io::ErrorKind::InvalidData,
                            "Missing timestamp chunk",
                        ))
                    })?;

                    let reconstructed_array = relinearize::reconstruct_relinearized_column(
                        relin_values.clone(),
                        keys.clone(),
                        timestamps.clone(),
                        field.data_type(),
                    )
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                    columns[col_idx] = Some(reconstructed_array);
                } else {
                    // This is a standard column (or a key/ts column). Just place it.
                    let array = decompressed_cache.get(&col_idx_u32).ok_or_else(|| {
                        ArrowError::from(IoError::new(
                            std::io::ErrorKind::InvalidData,
                            "Missing standard chunk",
                        ))
                    })?;
                    columns[col_idx] = Some(array.clone());
                }
            }
            let final_columns =
                columns
                    .into_iter()
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| {
                        ArrowError::from(IoError::new(
                            std::io::ErrorKind::InvalidData,
                            "Failed to assemble all columns",
                        ))
                    })?;
            RecordBatch::try_new(self.schema.clone(), final_columns)
        })
    }
}

// --- Mode 3: Global Sorting ---
struct GloballySortedStreamer<R: Read + Seek> {
    schema: SchemaRef,
    all_columns_data: Vec<ArrayRef>,
    current_row_offset: usize,
    batch_size: usize,
}

impl<R: Read + Seek> DecompressionMode<R> for GloballySortedStreamer<R> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn next_batch(&mut self) -> Option<ArrowResult<RecordBatch>> {
        let total_rows = self.all_columns_data.get(0).map_or(0, |arr| arr.len());
        if self.current_row_offset >= total_rows {
            return None;
        }

        let slice_start = self.current_row_offset;
        let num_rows_in_batch = (total_rows - slice_start).min(self.batch_size);
        self.current_row_offset += num_rows_in_batch;

        let sliced_columns: Vec<ArrayRef> = self
            .all_columns_data
            .iter()
            .map(|col| col.slice(slice_start, num_rows_in_batch))
            .collect();

        Some(RecordBatch::try_new(self.schema.clone(), sliced_columns))
    }
}

//==================================================================================
// III. The Decompressor (The Factory)
//==================================================================================

/// A high-level, stateful object that manages the decompression of a Phoenix file.
/// It reads the file metadata and acts as a factory for a streaming `RecordBatchReader`.
#[derive(Debug)]
pub struct Decompressor<R: Read + Seek> {
    source: R,
    schema: SchemaRef,
    chunk_manifest: Arc<Vec<ChunkManifestEntry>>,
    frame_plan: Option<FramePlan>,
}

impl<R: Read + Seek> Decompressor<R> {
    pub fn new(mut source: R) -> Result<Self, PhoenixError> {
        source.seek(SeekFrom::End(-8))?;
        let mut u64_buf = [0u8; 8];
        source.read_exact(&mut u64_buf)?;
        let footer_len = u64::from_le_bytes(u64_buf);
        let footer_start_pos = source.seek(SeekFrom::End(-8 - footer_len as i64))?;
        source.seek(SeekFrom::Start(footer_start_pos))?;
        let mut footer_bytes = vec![0; footer_len as usize];
        source.read_exact(&mut footer_bytes)?;

        let footer: FileFooter = serde_json::from_slice(&footer_bytes)?;

        Ok(Self {
            source,
            schema: Arc::new(footer.schema),
            chunk_manifest: Arc::new(footer.chunk_manifest),
            frame_plan: footer.frame_plan,
        })
    }

    /// Provides access to the permutation map to unsort a globally sorted file.
    /// Returns `None` if the file was not globally sorted.
    pub fn get_global_unsort_indices(&mut self) -> Result<Option<ArrayRef>, PhoenixError> {
        if let Some(FrameOperation::GlobalSortedFile {
            permutation_chunk_idx,
            ..
        }) = self.frame_plan.as_ref().and_then(|fp| fp.operations.get(0))
        {
            let perm_chunk_info = self
                .chunk_manifest
                .iter()
                .find(|c| c.column_idx == *permutation_chunk_idx)
                .ok_or_else(|| {
                    PhoenixError::FrameFormatError(
                        "Permutation chunk not found in manifest".to_string(),
                    )
                })?;

            read_and_decompress_chunk_common(&mut self.source, perm_chunk_info)
                .map(Some)
                .map_err(|e| e.into())
        } else {
            Ok(None)
        }
    }

    /// Consumes the Decompressor and returns an iterator over the decompressed record batches.
    pub fn batched(mut self) -> PhoenixBatchReader {
        let mode: Box<dyn DecompressionMode<R>> = if let Some(frame_plan) = self.frame_plan.as_ref()
        {
            if frame_plan
                .operations
                .iter()
                .any(|op| matches!(op, FrameOperation::GlobalSortedFile { .. }))
            {
                let all_cols: Vec<ArrayRef> = (0..self.schema.fields().len())
                    .map(|i| {
                        let info = self
                            .chunk_manifest
                            .iter()
                            .find(|c| c.column_idx == i as u32)
                            .unwrap();
                        read_and_decompress_chunk_common(&mut self.source, info).unwrap()
                    })
                    .collect();
                Box::new(GloballySortedStreamer {
                    schema: self.schema.clone(),
                    all_columns_data: all_cols,
                    current_row_offset: 0,
                    batch_size: 8192,
                })
            } else if frame_plan
                .operations
                .iter()
                .any(|op| matches!(op, FrameOperation::PerBatchRelinearizedColumn { .. }))
            {
                let manifest_iter = group_manifest_by_batch(&self.chunk_manifest);
                let mut relin_lookup = BTreeMap::new();
                for op in &frame_plan.operations {
                    if let FrameOperation::PerBatchRelinearizedColumn {
                        logical_value_idx,
                        key_col_idx,
                        timestamp_col_idx,
                    } = op
                    {
                        relin_lookup.insert(*logical_value_idx, (*key_col_idx, *timestamp_col_idx));
                    }
                }
                Box::new(PerBatchRelinearizationStreamer {
                    source: self.source,
                    schema: self.schema.clone(),
                    manifest_iter,
                    relin_lookup,
                })
            } else {
                Box::new(StandardStreamer {
                    source: self.source,
                    schema: self.schema.clone(),
                    manifest_iter: group_manifest_by_batch(&self.chunk_manifest),
                })
            }
        } else {
            Box::new(StandardStreamer {
                source: self.source,
                schema: self.schema.clone(),
                manifest_iter: group_manifest_by_batch(&self.chunk_manifest),
            })
        };

        PhoenixBatchReader {
            mode: Box::new(mode),
        }
    }
}

// Helper to group chunks by logical batch for streaming modes.
fn group_manifest_by_batch(
    manifest: &[ChunkManifestEntry],
) -> std::collections::btree_map::IntoIter<u64, Vec<ChunkManifestEntry>> {
    let mut grouped = BTreeMap::new();
    let mut current_batch_idx: u64 = 0;
    let mut columns_in_current_batch = std::collections::HashSet::new();
    for entry in manifest {
        if entry.column_idx == u32::MAX {
            continue;
        } // Skip special chunks like permutation map
        if columns_in_current_batch.contains(&entry.column_idx) {
            current_batch_idx += 1;
            columns_in_current_batch.clear();
        }
        columns_in_current_batch.insert(entry.column_idx);
        grouped
            .entry(current_batch_idx)
            .or_insert_with(Vec::new)
            .push(entry.clone());
    }
    grouped.into_iter()
}

//==================================================================================
// IV. The Public `RecordBatchReader` Wrapper
//==================================================================================

/// An iterator that yields `RecordBatch`es from a Phoenix file stream,
/// driven by a specific `DecompressionMode`.
pub struct PhoenixBatchReader {
    mode: Box<dyn DecompressionMode<dyn Read + Seek + Send>>,
}

impl Iterator for PhoenixBatchReader {
    type Item = ArrowResult<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        self.mode.next_batch()
    }
}

impl RecordBatchReader for PhoenixBatchReader {
    fn schema(&self) -> SchemaRef {
        self.mode.schema()
    }
}
