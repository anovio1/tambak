use super::*;
use crate::bridge::compressor::Compressor;
use crate::bridge::decompressor::Decompressor;
use arrow::array::{BooleanArray, Float64Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bridge::format::{ChunkManifestEntry, FileFooter},
        error::PhoenixError,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use std::io::Cursor;

    /// Helper function to create a valid, in-memory file for testing.
    fn create_valid_test_file_bytes() -> Vec<u8> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let footer = FileFooter {
            schema,
            chunk_manifest: vec![ChunkManifestEntry {
                column_idx: 0,
                offset_in_file: 6, // Right after header
                compressed_size: 10,
                num_rows: 100,
            }],
            writer_version: "test".into(),
        };
        let footer_json = serde_json::to_vec(&footer).unwrap();
        let footer_len = footer_json.len() as u64;

        let mut file_bytes = Vec::new();
        file_bytes.extend_from_slice(FILE_MAGIC);
        file_bytes.extend_from_slice(&FILE_FORMAT_VERSION.to_le_bytes());
        file_bytes.extend_from_slice(&[0; 10]); // Fake chunk data
        file_bytes.extend_from_slice(&footer_json);
        file_bytes.extend_from_slice(&footer_len.to_le_bytes());
        file_bytes
    }

    #[test]
    fn test_decompressor_new_on_valid_file() {
        let file_bytes = create_valid_test_file_bytes();
        let cursor = Cursor::new(file_bytes);
        let result = Decompressor::new(cursor);
        assert!(result.is_ok());
    }

    #[test]
    fn test_decompressor_new_on_empty_file() {
        let cursor = Cursor::new(vec![]);
        let result = Decompressor::new(cursor);

        // --- DEBUGGING PRINTLN ---
        println!("[DEBUG] test_empty_file: result = {:?}", result);
        // Expected output: [DEBUG] test_empty_file: result = Err(Io(...))

        // FIX: An empty file fails on I/O before we can check the format.
        // The correct error to expect is `PhoenixError::Io`.
        assert!(matches!(result, Err(PhoenixError::Io(_))));
    }

    #[test]
    fn test_decompressor_new_on_bad_magic_number() {
        let mut file_bytes = create_valid_test_file_bytes();
        file_bytes[0..4].copy_from_slice(b"BAD!");
        let cursor = Cursor::new(file_bytes);
        let result = Decompressor::new(cursor);
        assert!(matches!(result, Err(PhoenixError::FrameFormatError(_))));
        assert!(result.unwrap_err().to_string().contains("magic number"));
    }

    #[test]
    fn test_decompressor_new_on_bad_footer_length() {
        let mut file_bytes = create_valid_test_file_bytes();
        // Corrupt footer length to be larger than the file.
        let bad_len: u64 = 99999;
        let len = file_bytes.len();
        file_bytes[len - 8..].copy_from_slice(&bad_len.to_le_bytes());

        let cursor = Cursor::new(file_bytes);
        let result = Decompressor::new(cursor);
        assert!(matches!(result, Err(PhoenixError::FrameFormatError(_))));
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("exceeds file size"));
    }

    #[test]
    fn test_decompressor_new_on_corrupt_footer_json() {
        let mut file_bytes = create_valid_test_file_bytes();
        let footer_len =
            u64::from_le_bytes(file_bytes[file_bytes.len() - 8..].try_into().unwrap()) as usize;
        let corrupt_idx = file_bytes.len() - 8 - footer_len;
        file_bytes[corrupt_idx] = b'['; // Guarantees invalid JSON

        let cursor = Cursor::new(file_bytes);
        let result = Decompressor::new(cursor);

        assert!(
            matches!(result, Err(PhoenixError::SerdeJson(_))),
            "Expected a SerdeJson error, but got a different variant"
        );
    }

    #[test]
    fn test_footer_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        // Use the alias directly since we imported Schema
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);

        let chunk_manifest = vec![];

        let footer = FileFooter {
            schema: schema.clone(),
            chunk_manifest,
            writer_version: "1.0".to_string(),
        };

        let bytes = serde_json::to_vec(&footer)?;
        let read_footer: FileFooter = serde_json::from_slice(&bytes)?;

        assert_eq!(footer.schema, read_footer.schema);

        Ok(())
    }

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
