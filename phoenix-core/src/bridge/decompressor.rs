// In: src/bridge/decompressor.rs

use crate::bridge::arrow_impl;
use crate::bridge::format::{
    ChunkManifestEntry, FileFooter, FrameOperation, FramePlan, FILE_FORMAT_VERSION, FILE_MAGIC,
};
use crate::chunk_pipeline::orchestrator::decompress_chunk;
use crate::error::PhoenixError;
use crate::frame_pipeline::relinearize;
use arrow::array::{ArrayRef, RecordBatch, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use arrow::error::{ArrowError, Result as ArrowResult};
use std::collections::{BTreeMap, HashMap};
use std::io::{Error as IoError, Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex};

//==================================================================================
// I. Decompression Mode Contract (For Standard Streaming)
//==================================================================================

/// A trait defining the contract for a specific file decompression strategy.
/// This allows the `PhoenixBatchReader` to be agnostic of the underlying file structure.
trait DecompressionMode<R: Read + Seek>: Send {
    /// Reads and reconstructs the next `RecordBatch` according to the strategy.
    fn next_batch(&mut self) -> Option<ArrowResult<RecordBatch>>;
    /// Returns the schema of the file.
    fn schema(&self) -> SchemaRef;
}

// Helper to read and decompress a single physical chunk. Used by non-partitioned streamers.
fn read_and_decompress_chunk_common<R: Read + Seek>(
    source: &mut R,
    chunk_info: &ChunkManifestEntry,
) -> ArrowResult<ArrayRef> {
    source.seek(SeekFrom::Start(chunk_info.offset_in_file))?;
    let mut chunk_buffer = vec![0; chunk_info.compressed_size as usize];
    source.read_exact(&mut chunk_buffer)?;

    let pipeline_output =
        decompress_chunk(&chunk_buffer).map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
    let array = arrow_impl::pipeline_output_to_array(pipeline_output)
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

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

impl<R: Read + Seek + Send> DecompressionMode<R> for StandardStreamer<R> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn next_batch(&mut self) -> Option<ArrowResult<RecordBatch>> {
        self.manifest_iter.next().map(|(_, batch_chunks)| {
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
            let mut sorted_chunks = batch_chunks;
            sorted_chunks.sort_by_key(|c| c.column_idx);
            for chunk_info in sorted_chunks {
                columns.push(read_and_decompress_chunk_common(
                    &mut self.source,
                    &chunk_info,
                )?);
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
    relin_lookup: BTreeMap<u32, (u32, u32)>,
}

impl<R: Read + Seek + Send> DecompressionMode<R> for PerBatchRelinearizationStreamer<R> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn next_batch(&mut self) -> Option<ArrowResult<RecordBatch>> {
        self.manifest_iter.next().map(|(_, batch_physical_chunks)| {
            let mut columns = vec![None; self.schema.fields().len()];
            let mut decompressed_cache: HashMap<u32, ArrayRef> = HashMap::new();

            // First, decompress all physical chunks for this logical batch into a cache.
            // This prevents reading the same key/timestamp chunks multiple times.
            for chunk_info in &batch_physical_chunks {
                decompressed_cache.insert(
                    chunk_info.column_idx,
                    read_and_decompress_chunk_common(&mut self.source, chunk_info)?,
                );
            }

            // assemble the logical columns
            for (col_idx, field) in self.schema.fields().iter().enumerate() {
                let col_idx_u32 = col_idx as u32;

                if let Some(&(key_col_idx, ts_col_idx)) = self.relin_lookup.get(&col_idx_u32) {
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

//==================================================================================
// III. The Decompressor (The Factory)
//==================================================================================

/// A high-level, stateful object that manages the decompression of a Phoenix file.
/// It reads the file metadata and acts as a factory for partitioned and
/// non-partitioned readers
#[derive(Debug)]
pub struct Decompressor<R: Read + Seek> {
    source: R,
    schema: SchemaRef,
    chunk_manifest: Arc<Vec<ChunkManifestEntry>>,
    frame_plan: Option<FramePlan>,
}

impl<R: Read + Seek + Send> Decompressor<R> {
    pub fn new(mut source: R) -> Result<Self, PhoenixError> {
        source.seek(SeekFrom::Start(0))?;
        let mut magic_buf = [0u8; 4];
        source
            .read_exact(&mut magic_buf)
            .map_err(|e| PhoenixError::Io(e))?;
        if magic_buf != *FILE_MAGIC {
            return Err(PhoenixError::FrameFormatError(format!(
                "Invalid magic number. Expected `{:?}`, found `{:?}`.",
                *FILE_MAGIC, magic_buf
            )));
        }

        let mut version_buf = [0u8; 2];
        source.read_exact(&mut version_buf)?;
        let version = u16::from_le_bytes(version_buf);
        if version != FILE_FORMAT_VERSION {
            return Err(PhoenixError::FrameFormatError(format!(
                "Unsupported file format version. Expected {}, found {}.",
                FILE_FORMAT_VERSION, version
            )));
        }

        // Read footer length and perform sanity check *before* allocating memory.
        source.seek(SeekFrom::End(-8))?;
        let mut u64_buf = [0u8; 8];
        source.read_exact(&mut u64_buf)?;
        let footer_len = u64::from_le_bytes(u64_buf);
        if footer_len == 0 {
            return Err(PhoenixError::FrameFormatError(
                "Footer length is zero, invalid Phoenix file.".into(),
            ));
        }
        let current_len = source.seek(SeekFrom::End(0))?;
        if footer_len > current_len {
            return Err(PhoenixError::FrameFormatError(format!(
                "Footer length ({}) exceeds file size ({})",
                footer_len, current_len
            )));
        }

        // safe to seek and read the footer.
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

    /// Consumes the Decompressor and returns an iterator over record batches
    /// This is used for non-partitioned files.
    ///
    /// If the file is partitioned, this method will return an error. Use `.partitions()` instead.
    pub fn batched<'a>(self) -> Result<PhoenixBatchReader<'a, R>, PhoenixError>
    where
        Self: 'a,
    {
        let mode: Box<dyn DecompressionMode<R>> = if let Some(frame_plan) = self.frame_plan.as_ref()
        {
            // Check for partitioned files first and guide the user to the correct API.
            if frame_plan
                .operations
                .iter()
                .any(|op| matches!(op, FrameOperation::PartitionedFile { .. }))
            {
                return Err(PhoenixError::FrameFormatError("This is a partitioned file. Please use the `.partitions()` method instead of `.batched()`.".to_string()));
            }

            if frame_plan
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

        Ok(PhoenixBatchReader { mode })
    }

    /// Consumes the Decompressor and returns an iterator over the partitions in the file.
    /// Each item in the iterator is a `(partition_key, PartitionReader)` tuple.
    ///
    /// If the file is not partitioned, this method will return an error. Use `.batched()` instead.
    /// 
    /// **Note:** Partition order is not guaranteed
    pub fn partitions(self) -> Result<PartitionIterator<R>, PhoenixError> {
        if !self.frame_plan.as_ref().map_or(false, |fp| {
            fp.operations
                .iter()
                .any(|op| matches!(op, FrameOperation::PartitionedFile { .. }))
        }) {
            return Err(PhoenixError::FrameFormatError(
                "This is not a partitioned file. Please use the `.batched()` method.".to_string(),
            ));
        }

        // Group the entire manifest by partition key.
        let mut partition_map: HashMap<i64, Vec<ChunkManifestEntry>> = HashMap::new();
        for entry in self.chunk_manifest.iter() {
            if let Some(key) = entry.partition_key {
                partition_map.entry(key).or_default().push(entry.clone());
            }
        }

        Ok(PartitionIterator {
            source: Arc::new(Mutex::new(self.source)),
            schema: self.schema,
            partition_map_iter: partition_map.into_iter(),
        })
    }
}

/// **ARCHITECTURAL COMMENT (Robust Batch Delimitation):**
/// This function implements the robust heuristic for grouping chunks from the flat
/// manifest back into their original logical `RecordBatch`es. The previous heuristic
/// (based on `column_idx == 0`) was brittle. This new implementation is trivial and
/// correct as it relies on the explicit `batch_id` assigned during compression.
fn group_manifest_by_batch(
    manifest: &[ChunkManifestEntry],
) -> std::collections::btree_map::IntoIter<u64, Vec<ChunkManifestEntry>> {
    let mut grouped: BTreeMap<u64, Vec<ChunkManifestEntry>> = BTreeMap::new();
    for entry in manifest {
        grouped
            .entry(entry.batch_id)
            .or_default()
            .push(entry.clone());
    }
    grouped.into_iter()
}

//==================================================================================
// IV. The Public `RecordBatchReader` Wrapper
//==================================================================================

/// An iterator that yields `RecordBatch`es from a non-partitioned Phoenix file stream.
pub struct PhoenixBatchReader<'a, R: Read + Seek + Send + 'a> {
    mode: Box<dyn DecompressionMode<R> + 'a>,
}

impl<'a, R: Read + Seek + Send + 'a> Iterator for PhoenixBatchReader<'a, R> {
    type Item = ArrowResult<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        self.mode.next_batch()
    }
}

impl<'a, R: Read + Seek + Send + 'a> RecordBatchReader for PhoenixBatchReader<'a, R> {
    fn schema(&self) -> SchemaRef {
        self.mode.schema()
    }
}

//==================================================================================
// V. Partition-Specific Readers
//==================================================================================

/// An iterator that yields `(partition_key, PartitionReader)` tuples.
pub struct PartitionIterator<R: Read + Seek> {
    source: Arc<Mutex<R>>,
    schema: SchemaRef,
    partition_map_iter: std::collections::hash_map::IntoIter<i64, Vec<ChunkManifestEntry>>,
}

impl<R: Read + Seek> Iterator for PartitionIterator<R> {
    type Item = (i64, PartitionReader<R>);

    fn next(&mut self) -> Option<Self::Item> {
        self.partition_map_iter
            .next()
            .map(|(key, partition_manifest)| {
                let reader = PartitionReader {
                    source: self.source.clone(),
                    schema: self.schema.clone(),
                    // Group this partition's chunks by their batch_id to form RecordBatches
                    manifest_iter: group_manifest_by_batch(&partition_manifest),
                };
                (key, reader)
            })
    }
}

/// A `RecordBatchReader` that reads all data for a single partition.
/// This struct is designed to be sent across threads for parallel processing.
pub struct PartitionReader<R: Read + Seek> {
    source: Arc<Mutex<R>>,
    schema: SchemaRef,
    manifest_iter: std::collections::btree_map::IntoIter<u64, Vec<ChunkManifestEntry>>,
}

impl<R: Read + Seek> RecordBatchReader for PartitionReader<R> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl<R: Read + Seek> Iterator for PartitionReader<R> {
    type Item = ArrowResult<RecordBatch>;

    /// This method minimizes lock contention when reading partitions in parallel.
    ///
    /// # Steps
    /// 1. **I/O Phase (Lock Held):** Read all compressed chunk bytes for one `RecordBatch`
    ///    under a single lock acquisition.
    /// 2. **CPU Phase (Lock Released):** Release the lock and perform decompression concurrently.
    ///
    /// # Invariants
    /// Assumes `group_manifest_by_batch` groups chunks correctly and
    /// `chunk_info.column_idx` matches schema fields 1:1 for assembly.
    ///
    /// # Error Handling
    /// A poisoned `Mutex` aborts the operation to avoid inconsistent file read state.
    fn next(&mut self) -> Option<ArrowResult<RecordBatch>> {
        self.manifest_iter.next().map(|(_, batch_chunks)| {
            let mut sorted_chunks = batch_chunks;
            sorted_chunks.sort_by_key(|c| c.column_idx);

            // --- Step 1: Perform all I/O first, under a single lock. ---
            let compressed_chunks_data: Vec<Vec<u8>> = {
                let mut source_guard = self.source.lock().map_err(|e| {
                    ArrowError::ExternalError(Box::new(IoError::new(
                        std::io::ErrorKind::Other,
                        format!("Mutex was poisoned: {}", e),
                    )))
                })?;

                sorted_chunks
                    .iter()
                    .map(|chunk_info| {
                        source_guard.seek(SeekFrom::Start(chunk_info.offset_in_file))?;
                        let mut chunk_buffer = vec![0; chunk_info.compressed_size as usize];
                        source_guard.read_exact(&mut chunk_buffer)?;
                        Ok(chunk_buffer)
                    })
                    .collect::<Result<_, IoError>>()
                    .map_err(ArrowError::from)?
            }; // <-- Lock is released here as `source_guard` goes out of scope!

            // --- Step 2: Perform all CPU-bound decompression *without* holding the lock. ---
            let mut decompressed_columns = Vec::with_capacity(self.schema.fields().len());
            for chunk_bytes in compressed_chunks_data {
                let pipeline_output = decompress_chunk(&chunk_bytes)
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                let array = arrow_impl::pipeline_output_to_array(pipeline_output)
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                decompressed_columns.push(array.into());
            }

            RecordBatch::try_new(self.schema.clone(), decompressed_columns)
        })
    }
}
