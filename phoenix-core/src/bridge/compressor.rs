// In: src/bridge/compressor.rs

use std::io::Write;
use std::mem;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::record_batch::RecordBatchReader;
use arrow_schema::Schema;

use crate::error::PhoenixError;

use crate::bridge::config::{CompressorConfig, TimeSeriesStrategy};
use crate::bridge::format::{ChunkManifestEntry, FileFooter, FILE_FORMAT_VERSION, FILE_MAGIC};

use crate::frame_pipeline::{
    FramePipeline, PartitioningStrategy, PerBatchRelinearizationStrategy, StandardStreamingStrategy,
};

/// A high-level, stateful object that manages the entire compression process.
#[derive(Debug)]
pub struct Compressor<W: Write> {
    writer: W,
    config: CompressorConfig,
    chunk_manifest: Vec<ChunkManifestEntry>,
    bytes_written: u64,
}

impl<W: Write> Compressor<W> {
    pub fn new(mut writer: W, config: CompressorConfig) -> Result<Self, PhoenixError> {
        writer.write_all(FILE_MAGIC)?;
        writer.write_all(&FILE_FORMAT_VERSION.to_le_bytes())?;
        Ok(Self {
            writer,
            config,
            chunk_manifest: Vec::new(),
            bytes_written: 6,
        })
    }

    pub fn compress(&mut self, source: &mut dyn RecordBatchReader) -> Result<(), PhoenixError> {
        let schema = source.schema();

        // --- FRAME PIPELINE FACTORY ---
        let frame_pipeline: Box<dyn FramePipeline> = match self.config.time_series_strategy {
            TimeSeriesStrategy::None => Box::new(StandardStreamingStrategy),
            TimeSeriesStrategy::PerBatchRelinearization { .. } => {
                Box::new(PerBatchRelinearizationStrategy)
            }
            TimeSeriesStrategy::Partitioned { .. } => Box::new(PartitioningStrategy),
        };

        // --- DELEGATION TO THE FRAME PIPELINE ---
        let result = frame_pipeline.execute(source, &self.config)?;

        // --- UNPACKING & WRITING CHUNKS ---
        for (chunk_bytes, mut chunk_manifest_entry) in result.compressed_chunks_with_manifests {
            let chunk_offset = self.bytes_written;
            let compressed_size = chunk_bytes.len() as u64;

            self.writer.write_all(&chunk_bytes)?;
            self.bytes_written += compressed_size;

            chunk_manifest_entry.offset_in_file = chunk_offset;
            chunk_manifest_entry.compressed_size = compressed_size;
            self.chunk_manifest.push(chunk_manifest_entry);
        }

        // --- WRITING FOOTER ---
        let footer = FileFooter {
            schema: schema.as_ref().clone(),
            chunk_manifest: mem::take(&mut self.chunk_manifest),
            writer_version: env!("CARGO_PKG_VERSION").to_string(),
            frame_plan: result.frame_plan,
        };

        let footer_bytes = serde_json::to_vec(&footer)?;
        let footer_len = footer_bytes.len() as u64;

        self.writer.write_all(&footer_bytes)?;
        self.writer.write_all(&footer_len.to_le_bytes())?;

        Ok(())
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

// Helper for `Compressor::compress` to pass Vec<RecordBatch> as a `RecordBatchReader`.
pub(crate) struct MockRecordBatchReader {
    batches: std::vec::IntoIter<RecordBatch>,
    schema: Arc<Schema>,
}

impl MockRecordBatchReader {
    pub fn new(batches: Vec<RecordBatch>, schema: Arc<Schema>) -> Self {
        Self {
            batches: batches.into_iter(),
            schema,
        }
    }
}

impl RecordBatchReader for MockRecordBatchReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
impl Iterator for MockRecordBatchReader {
    type Item = arrow::error::Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next().map(Ok)
    }
}
