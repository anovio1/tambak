// In: src/bridge/decompressor.rs

use crate::bridge::arrow_impl;
use crate::bridge::format::{
    ChunkManifestEntry, FileFooter, FrameOperation, FramePlan, FILE_FORMAT_VERSION, FILE_MAGIC,
};
use crate::chunk_pipeline::orchestrator::decompress_chunk;
use crate::error::PhoenixError;
use crate::frame_pipeline::relinearize;
use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use arrow::error::{ArrowError, Result as ArrowResult};
use std::collections::{BTreeMap, HashMap};
use std::io::{Error as IoError, Read, Seek, SeekFrom};
use std::marker::PhantomData; // ADDED
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
    ///`PhantomData` is used because this struct implements
    /// `DecompressionMode<R>`, but it does not actually store a field of type `R`.
    /// The data is loaded eagerly in the `Decompressor`. This marker tells the Rust
    /// compiler that the struct should still be treated as if it is associated with
    /// the generic type `R`, preventing an "unused type parameter" error.
    _reader: PhantomData<R>,
}

impl<R: Read + Seek + Send> DecompressionMode<R> for GloballySortedStreamer<R> {
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

impl<R: Read + Seek + Send> Decompressor<R> {
    pub fn new(mut source: R) -> Result<Self, PhoenixError> {
        source.seek(SeekFrom::Start(0))?;
        let mut magic_buf = [0u8; 4];
        source
            .read_exact(&mut magic_buf)
            .map_err(|e| PhoenixError::Io(e))?;
        if magic_buf != *FILE_MAGIC {
            // FIX #1: Dereference
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
    /// **ARCHITECTURAL COMMENT (Factory Method & Lifetime Management):**
    /// This `batched` method is the heart of the `Decompressor`. It acts as a **Factory**
    /// that consumes the `Decompressor` and produces a `PhoenixBatchReader`.
    ///
    /// It inspects the `frame_plan` and decides which concrete `DecompressionMode`
    /// implementation to create. This is the implementation of the Strategy pattern.
    ///
    /// The `<'a>` lifetime parameter is crucial.
    /// It ensures that the returned `PhoenixBatchReader<'a, R>` cannot outlive the
    /// `Decompressor`'s data it was created from. The `where Self: 'a` bound
    /// ties the lifetime of the `Decompressor` to the lifetime of the returned reader,
    /// preventing dangling references.
    pub fn batched<'a>(mut self) -> PhoenixBatchReader<'a, R>
    where
        Self: 'a,
    {
        let mode: Box<dyn DecompressionMode<R>> = if let Some(frame_plan) = self.frame_plan.as_ref()
        {
            if frame_plan
                .operations
                .iter()
                .any(|op| matches!(op, FrameOperation::GlobalSortedFile { .. }))
            {
                // This block handles the eager loading for a globally sorted file.
                // Use collect to turn an iterator of Results into a Result of a Vec.
                let all_cols_result: Result<Vec<ArrayRef>, ArrowError> = (0..self.schema.fields().len())
                    .map(|i| {
                        let info = self
                            .chunk_manifest
                            .iter()
                            .find(|c| c.column_idx == i as u32)
                            // Use ok_or_else for a better error message if a chunk is missing.
                            .ok_or_else(|| ArrowError::from(IoError::new(
                                std::io::ErrorKind::InvalidData,
                                format!("GlobalSortedFile: Manifest is missing chunk for column index {}", i)
                            )))?;
                        // Use the ? operator for clean error propagation from the I/O function.
                        read_and_decompress_chunk_common(&mut self.source, info)
                    })
                    .collect();

                // **ARCHITECTURAL COMMENT (Error Handling Trade-off):**
                // This is a deliberate design choice. If loading the globally sorted data
                // fails (e.g., file corruption), instead of panicking or returning an
                // error immediately, we fall back to the standard streamer. This is a
                // "best-effort" recovery. The `eprintln!` is critical to make this
                // non-standard behavior observable to the user for debugging.
                match all_cols_result {
                    Ok(all_columns_data) => {
                        // This is the happy path. We successfully loaded all sorted data.
                        Box::new(GloballySortedStreamer {
                            schema: self.schema.clone(),
                            all_columns_data,
                            current_row_offset: 0,
                            batch_size: 8192,
                            _reader: PhantomData,
                        })
                    }
                    Err(_e) => {
                        // If we fail to load the globally sorted data (e.g., corrupt file),
                        // gracefully fall back to the standard streamer. This is safer than panicking.
                        // A warning is logged to make this behavior observable.
                        eprintln!("[WARN] Failed to load globally sorted data, falling back to standard streaming. Error: {}", _e);
                        Box::new(StandardStreamer {
                            source: self.source,
                            schema: self.schema.clone(),
                            manifest_iter: group_manifest_by_batch(&self.chunk_manifest),
                        })
                    }
                }
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

        PhoenixBatchReader { mode }
    }
}

/// **ARCHITECTURAL COMMENT (Heuristic for Batch Delimitation):**
/// This function implements the crucial heuristic for grouping chunks from the flat
/// manifest back into their original logical `RecordBatch`es for streaming modes.
///
/// The core assumption is that the `Compressor` writes all chunks for a batch
/// sequentially, starting with `column_idx == 0`.
/// Therefore, encountering a `column_idx == 0` again signifies the start of a *new*
/// logical batch. The `!is_empty()` check is a critical guard for the very first
/// chunk in the file. This heuristic replaced a previous, buggier one based on
/// a `HashSet`, which failed for single-column, multi-batch files.
// Helper to group chunks by logical batch for streaming modes.
fn group_manifest_by_batch(
    manifest: &[ChunkManifestEntry],
) -> std::collections::btree_map::IntoIter<u64, Vec<ChunkManifestEntry>> {
    let mut grouped = BTreeMap::new();
    if manifest.is_empty() {
        return grouped.into_iter();
    }

    let mut current_batch_idx: u64 = 0;
    // Start the first batch immediately.
    grouped.insert(0, Vec::new());

    for entry in manifest {
        // Skip special chunks like the global permutation map.
        if entry.column_idx == u32::MAX {
            continue;
        }

        // A new batch starts when we see column 0 again, but only if the
        // current batch is not empty. This handles the very first chunk correctly.
        if entry.column_idx == 0 && !grouped.get(&current_batch_idx).unwrap().is_empty() {
            current_batch_idx += 1;
            grouped.insert(current_batch_idx, Vec::new());
        }

        // Add the chunk to the current batch.
        grouped
            .get_mut(&current_batch_idx)
            .unwrap()
            .push(entry.clone());
    }
    grouped.into_iter()
}

//==================================================================================
// IV. The Public `RecordBatchReader` Wrapper
//==================================================================================

/// An iterator that yields `RecordBatch`es from a Phoenix file stream,
/// driven by a specific `DecompressionMode`. This is the final public-facing
/// object returned to the user. Its implementation is simple because it delegates
/// all the hard work to the `mode` trait object.
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
