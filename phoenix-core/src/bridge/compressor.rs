// In: src/bridge/compressor.rs

use crate::bridge::config::CompressorConfig;
use crate::bridge::format::{ChunkManifestEntry, FileFooter, FILE_FORMAT_VERSION, FILE_MAGIC};
use crate::bridge::stateless_api;
use crate::error::PhoenixError;
use arrow::record_batch::RecordBatchReader;
use std::io::Write;
use std::mem;

/// A high-level, stateful object that manages the entire compression process.
/// This implementation is stream-compatible and does not require the writer to be seekable.
#[derive(Debug)]
pub struct Compressor<W: Write> {
    writer: W,
    config: CompressorConfig,
    chunk_manifest: Vec<ChunkManifestEntry>,
    /// Manually tracks the number of bytes written to the underlying writer.
    bytes_written: u64,
}

impl<W: Write> Compressor<W> {
    /// Creates a new compressor with the given configuration that writes to a destination.
    /// It immediately writes the file header to the writer.
    pub fn new(mut writer: W, config: CompressorConfig) -> Result<Self, PhoenixError> {
        // Immediately write the header so our byte counter is accurate from the start.
        writer.write_all(FILE_MAGIC)?;
        writer.write_all(&FILE_FORMAT_VERSION.to_le_bytes())?;

        Ok(Self {
            writer,
            config,
            chunk_manifest: Vec::new(),
            // Start counter after the fixed-size header.
            bytes_written: (FILE_MAGIC.len() + 2) as u64,
        })
    }

    /// The primary, high-level compression method.
    /// Reads all data from the source, processes it chunk-by-chunk,
    /// and writes the compressed output to the destination.
    pub fn compress(&mut self, source: &mut dyn RecordBatchReader) -> Result<(), PhoenixError> {
        let schema = source.schema();

        // --- 2. Loop through RecordBatches from the source ---
        for batch_result in source {
            // Correctly map the ArrowError to our PhoenixError variant.
            let batch = batch_result?;

            // --- 3. For each column in the batch, compress it as a chunk ---
            for (i, array) in batch.columns().iter().enumerate() {
                // a. The chunk offset IS our current counter value.
                let chunk_offset = self.bytes_written;

                // b. Call the stateless bridge function to do the actual compression.
                let compressed_bytes = stateless_api::compress_arrow_chunk(array.as_ref())?;
                let compressed_size = compressed_bytes.len() as u64;

                // c. Write the compressed bytes AND update the counter.
                self.writer.write_all(&compressed_bytes)?;
                self.bytes_written += compressed_size;

                // d. Create a manifest entry and save it.
                self.chunk_manifest.push(ChunkManifestEntry {
                    column_idx: i as u32,
                    offset_in_file: chunk_offset,
                    compressed_size,
                    num_rows: array.len() as u64,
                });
            }
        }

        // --- 4. Write the file footer ---
        let footer = FileFooter {
            // The `FileFooter` now correctly takes a `Schema` object.
            // Our `schema_serde` helper will handle the JSON conversion.
            schema: schema.as_ref().clone(),
            // Use `mem::take` for efficiency instead of `clone`.
            chunk_manifest: mem::take(&mut self.chunk_manifest),
            writer_version: env!("CARGO_PKG_VERSION").to_string(),
        };

        let footer_bytes = serde_json::to_vec(&footer)?;
        let footer_len = footer_bytes.len() as u64;

        self.writer.write_all(&footer_bytes)?;
        self.writer.write_all(&footer_len.to_le_bytes())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::decompressor::Decompressor;
    use arrow::array::{BooleanArray, Float64Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use std::io::Cursor;
    use std::sync::Arc;

    /// A helper function to create a realistic, multi-batch, multi-column dataset for testing.
    fn create_test_batches() -> (Vec<RecordBatch>, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1_int", DataType::Int32, false),
            Field::new("c2_float", DataType::Float64, true),
            Field::new("c3_bool", DataType::Boolean, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Float64Array::from(vec![
                    Some(1.1),
                    None,
                    Some(3.3),
                    Some(4.4),
                ])),
                Arc::new(BooleanArray::from(vec![true, false, true, false])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![5, 6, 7])),
                Arc::new(Float64Array::from(vec![Some(5.5), Some(6.6), None])),
                Arc::new(BooleanArray::from(vec![false, false, true])),
            ],
        )
        .unwrap();

        (vec![batch1, batch2], schema)
    }

    /// The primary integration test, proving a full round-trip.
    #[test]
    fn test_compressor_decompressor_roundtrip() -> Result<(), PhoenixError> {
        // --- ARRANGE ---
        let (original_batches, schema) = create_test_batches();
        // Create a RecordBatchReader from our test data.
        // We use a temporary buffer and the Arrow IPC Stream Writer/Reader to create a valid reader.
        let mut writer_buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut writer_buf, &schema).unwrap();
        for batch in &original_batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
        drop(writer); // Ensure writer_buf is no longer borrowed
        let mut reader =
            arrow::ipc::reader::StreamReader::try_new(Cursor::new(writer_buf), None).unwrap();

        // Create an in-memory buffer to act as our Phoenix file.
        let file_buffer = Cursor::new(Vec::new());

        // --- ACT (COMPRESS) ---
        let config = CompressorConfig::default();
        let mut compressor = Compressor::new(file_buffer, config)?;
        compressor.compress(&mut reader)?;

        // Retrieve the written bytes from the cursor.
        let compressed_bytes = compressor.writer.into_inner();
        assert!(!compressed_bytes.is_empty());

        // --- ACT (DECOMPRESS) ---
        let read_cursor = Cursor::new(compressed_bytes);
        let decompressor = Decompressor::new(read_cursor)?;
        let decompressed_batches: Vec<RecordBatch> =
            decompressor.batched().collect::<Result<_, _>>().unwrap();

        // --- ASSERT ---
        assert_eq!(
            original_batches.len(),
            decompressed_batches.len(),
            "Should have the same number of batches"
        );

        for (original, reconstructed) in original_batches.iter().zip(decompressed_batches.iter()) {
            assert_eq!(original, reconstructed, "Batch content should be identical");
        }

        Ok(())
    }

    #[test]
    fn test_compress_empty_reader() -> Result<(), PhoenixError> {
        // --- ARRANGE ---
        let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, false)]));

        // Create an empty reader. The Arrow IPC format requires a valid header even for an empty stream.
        let mut writer_buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut writer_buf, &schema).unwrap();
        writer.finish().unwrap(); // Finish immediately to write the header/footer but no data.
        drop(writer); // Ensure writer_buf is no longer borrowed

        // --- THIS IS THE FIX ---
        // The second argument to `try_new` is an Option to override the schema from the stream,
        // which we don't need here. We just need a valid, empty stream.
        let mut reader =
            arrow::ipc::reader::StreamReader::try_new(Cursor::new(writer_buf), None).unwrap();
        // --- END FIX ---

        let file_buffer = Cursor::new(Vec::new());

        // --- ACT (COMPRESS) ---
        let config = CompressorConfig::default();
        let mut compressor = Compressor::new(file_buffer, config)?;
        compressor.compress(&mut reader)?;

        let compressed_bytes = compressor.writer.into_inner();
        // File should not be empty, it must have a header and footer.
        assert!(compressed_bytes.len() > 14);

        // --- ACT (DECOMPRESS) ---
        let read_cursor = Cursor::new(compressed_bytes);
        let decompressor = Decompressor::new(read_cursor)?;
        let decompressed_batches: Vec<RecordBatch> =
            decompressor.batched().collect::<Result<_, _>>().unwrap();

        // --- ASSERT ---
        assert!(
            decompressed_batches.is_empty(),
            "Decompressing an empty file should result in zero batches"
        );

        Ok(())
    }
}
