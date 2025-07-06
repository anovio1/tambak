use super::*;
use crate::bridge::compressor::Compressor;
use crate::bridge::decompressor::Decompressor;
use arrow::array::{BooleanArray, Float64Array, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn create_single_partition_test_batches() -> (Vec<RecordBatch>, SchemaRef) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("partition_key", DataType::Int64, false),
        Field::new("data", DataType::Int32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![101, 101, 101])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![101, 101])),
            Arc::new(Int32Array::from(vec![4, 5])),
        ],
    )
    .unwrap();

    (vec![batch1, batch2], schema)
}
/// Helper to create interleaved data for multiple partitions.
fn create_multi_partition_test_batches() -> (Vec<RecordBatch>, SchemaRef) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int64, false),
        Field::new("v", DataType::Int32, true),
    ]));

    // Data is interleaved to simulate a realistic stream.
    // Key 101 will have 4 rows.
    // Key 102 will have 5 rows.
    // Key 103 will have 1 row.
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 101, 102, 103])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(10),
                Some(2),
                Some(11),
                Some(100),
            ])),
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![102, 101, 102, 101, 102])),
            Arc::new(Int32Array::from(vec![
                Some(12),
                Some(3),
                None,
                Some(4),
                Some(14),
            ])),
        ],
    )
    .unwrap();

    (vec![batch1, batch2], schema)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bridge::{
            compressor::MockRecordBatchReader, format::{ChunkManifestEntry, FileFooter, FILE_FORMAT_VERSION, FILE_MAGIC}
        },
        config::{TambakConfig, TimeSeriesStrategy},
        error::tambakError,
    };
    use arrow::{
        array::Array,
        datatypes::{DataType, Field, Schema},
    };
    use hashbrown::HashMap;
    use std::io::Cursor;

    /// Helper function to create a valid, in-memory file for testing.
    fn create_valid_test_file_bytes() -> Vec<u8> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let footer = FileFooter {
            schema,
            chunk_manifest: vec![ChunkManifestEntry {
                batch_id: 0,
                column_idx: 0,
                offset_in_file: 6, // Right after header
                compressed_size: 10,
                num_rows: 100,
                partition_key: None,
            }],
            writer_version: "test".into(),
            frame_plan: None,
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
        // The correct error to expect is `tambakError::Io`.
        assert!(matches!(result, Err(tambakError::Io(_))));
    }

    #[test]
    fn test_decompressor_new_on_bad_magic_number() {
        let mut file_bytes = create_valid_test_file_bytes();
        file_bytes[0..4].copy_from_slice(b"BAD!");
        let cursor = Cursor::new(file_bytes);
        let result = Decompressor::new(cursor);
        assert!(matches!(result, Err(tambakError::FrameFormatError(_))));
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
        assert!(matches!(result, Err(tambakError::FrameFormatError(_))));
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
            matches!(result, Err(tambakError::SerdeJson(_))),
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
            frame_plan: None,
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
    fn test_compressor_decompressor_roundtrip() -> Result<(), tambakError> {
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

        // Create an in-memory buffer to act as our tambak file.
        let file_buffer = Cursor::new(Vec::new());

        // --- ACT (COMPRESS) ---
        let config = TambakConfig::default();
        let mut compressor = Compressor::new(file_buffer, config)?;
        compressor.compress(&mut reader)?;

        // Retrieve the written bytes from the cursor.
        let compressed_bytes = compressor.into_inner().into_inner();
        assert!(!compressed_bytes.is_empty());

        // --- ACT (DECOMPRESS) ---
        let read_cursor = Cursor::new(compressed_bytes);
        let decompressor = Decompressor::new(read_cursor)?;
        let decompressed_batches: Vec<RecordBatch> =
            decompressor.batched()?.collect::<Result<_, _>>().unwrap();

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
    fn test_compress_empty_reader() -> Result<(), tambakError> {
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
        let config = TambakConfig::default();
        let mut compressor = Compressor::new(file_buffer, config)?;
        compressor.compress(&mut reader)?;

        let compressed_bytes = compressor.into_inner().into_inner();
        // File should not be empty, it must have a header and footer.
        assert!(compressed_bytes.len() > 14);

        // --- ACT (DECOMPRESS) ---
        let read_cursor = Cursor::new(compressed_bytes);
        let decompressor = Decompressor::new(read_cursor)?;
        let decompressed_batches: Vec<RecordBatch> =
            decompressor.batched()?.collect::<Result<_, _>>().unwrap();

        // --- ASSERT ---
        assert!(
            decompressed_batches.is_empty(),
            "Decompressing an empty file should result in zero batches"
        );

        Ok(())
    }
    /// **API Boundary Test:**
    /// Verifies that the Compressor correctly returns an error if the user provides
    /// a partition key column name that does not exist in the schema.
    #[test]
    fn test_compressor_partitioned_invalid_key_column_error() -> Result<(), tambakError> {
        // --- ARRANGE ---
        let (batches, schema) = create_single_partition_test_batches();
        let mut reader = MockRecordBatchReader::new(batches, schema);
        let file_buffer = Cursor::new(Vec::new());

        // Configure with a column name that does not exist.
        let config = TambakConfig {
            time_series_strategy: TimeSeriesStrategy::Partitioned {
                key_column: "this_column_does_not_exist".to_string(),
                partition_flush_rows: 100,
            },
            ..Default::default()
        };

        let mut compressor = Compressor::new(file_buffer, config)?;

        // --- ACT ---
        let result = compressor.compress(&mut reader);

        // --- ASSERT ---
        assert!(
            result.is_err(),
            "Compression should fail for a non-existent key column"
        );
        // Specifically, schema.index_of() returns an ArrowError.
        assert!(matches!(result, Err(tambakError::Arrow(_))));

        Ok(())
    }

    /// **Edge Case Test:**
    /// Verifies that the partitioning strategy works correctly when the input stream
    /// contains data for only a single partition key.
    #[test]
    fn test_compressor_partitioned_with_single_key() -> Result<(), tambakError> {
        // --- ARRANGE ---
        let (batches, schema) = create_single_partition_test_batches();
        let mut reader = MockRecordBatchReader::new(batches, schema.clone());
        let file_buffer = Cursor::new(Vec::new());

        let config = TambakConfig {
            time_series_strategy: TimeSeriesStrategy::Partitioned {
                key_column: "partition_key".to_string(),
                partition_flush_rows: 100,
            },
            ..Default::default()
        };

        // --- ACT (COMPRESS) ---
        let mut compressor = Compressor::new(file_buffer, config)?;
        compressor.compress(&mut reader)?;
        let compressed_bytes = compressor.into_inner().into_inner();

        // --- ASSERT (DECOMPRESS) ---
        let read_cursor = Cursor::new(compressed_bytes);
        let decompressor = Decompressor::new(read_cursor)?;
        let mut partitions_iter = decompressor.partitions()?;

        // 1. Assert that there is exactly one partition.
        let (key, partition_reader) = partitions_iter.next().expect("Should have one partition");
        assert_eq!(key, 101);
        assert!(
            partitions_iter.next().is_none(),
            "Should be no more partitions"
        );

        // 2. Assert the content of that single partition is correct.
        let all_partition_batches: Vec<RecordBatch> =
            partition_reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(
            all_partition_batches.len(),
            1,
            "The single key should have been flushed as a single batch"
        );
        let batch = &all_partition_batches[0];
        let data_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(data_col.values(), &[1, 2, 3, 4, 5]);

        Ok(())
    }

    /// **Edge Case Test:**
    /// Verifies that the compressor produces a valid, empty file when the input
    /// reader is empty, even when configured for partitioning.
    #[test]
    fn test_compressor_partitioned_on_empty_input() -> Result<(), tambakError> {
        // --- ARRANGE ---
        // Create an empty reader with a valid schema.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "partition_key",
            DataType::Int64,
            false,
        )]));
        let mut reader = MockRecordBatchReader::new(vec![], schema);
        let file_buffer = Cursor::new(Vec::new());

        let config = TambakConfig {
            time_series_strategy: TimeSeriesStrategy::Partitioned {
                key_column: "partition_key".to_string(),
                partition_flush_rows: 100,
            },
            ..Default::default()
        };

        // --- ACT (COMPRESS) ---
        let mut compressor = Compressor::new(file_buffer, config)?;
        compressor.compress(&mut reader)?;
        let compressed_bytes = compressor.into_inner().into_inner();

        // --- ASSERT (DECOMPRESS) ---
        // A valid file should be produced, containing only a header and footer.
        assert!(compressed_bytes.len() > 14);

        let read_cursor = Cursor::new(compressed_bytes);
        let decompressor = Decompressor::new(read_cursor)?;
        let mut partitions_iter = decompressor.partitions()?;

        // The iterator over partitions should be empty.
        assert!(
            partitions_iter.next().is_none(),
            "Decompressing an empty file should result in zero partitions"
        );

        Ok(())
    }
    /// **Primary Partitioning Integration Test:**
    /// This test verifies the end-to-end partitioning flow with multiple, interleaved
    /// partition keys, ensuring that data is correctly split during compression
    /// and correctly reassembled during decompression.
    #[test]
    fn test_partitioning_roundtrip_multiple_keys() -> Result<(), tambakError> {
        // --- ARRANGE ---
        let (batches, schema) = create_multi_partition_test_batches();
        let mut reader = MockRecordBatchReader::new(batches, schema);
        let file_buffer = Cursor::new(Vec::new());

        // Configure to flush partitions aggressively to ensure some partitions span multiple batches.
        let config = TambakConfig {
            time_series_strategy: TimeSeriesStrategy::Partitioned {
                key_column: "pk".to_string(),
                partition_flush_rows: 3, // Flush after 3 rows
            },
            ..Default::default()
        };

        // --- ACT (COMPRESS) ---
        let mut compressor = Compressor::new(file_buffer, config)?;
        compressor.compress(&mut reader)?;
        let compressed_bytes = compressor.into_inner().into_inner();

        // --- ACT (DECOMPRESS) ---
        let read_cursor = Cursor::new(compressed_bytes);
        let decompressor = Decompressor::new(read_cursor)?;

        // Collect all decompressed partitions into a HashMap for easy validation.
        let mut decompressed_partitions = HashMap::new();
        for result in decompressor.partitions()? {
            let (key, partition_reader) = result;
            let batches: Vec<RecordBatch> = partition_reader.collect::<Result<_, _>>().unwrap();
            decompressed_partitions.insert(key, batches);
        }

        // --- ASSERT ---
        // 1. Check we got the right number of partitions.
        assert_eq!(decompressed_partitions.len(), 3);

        // 2. Validate content of partition 101.
        let p101_batches = decompressed_partitions.get(&101).unwrap();
        let p101_concatenated =
            arrow::compute::concat_batches(&p101_batches[0].schema(), p101_batches).unwrap();
        let p101_values = p101_concatenated
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            p101_values.iter().map(|v| v.unwrap()).collect::<Vec<_>>(),
            &[1, 2, 3, 4]
        );

        // 3. Validate content of partition 102.
        let p102_batches = decompressed_partitions.get(&102).unwrap();
        let p102_concatenated =
            arrow::compute::concat_batches(&p102_batches[0].schema(), p102_batches).unwrap();
        let p102_values = p102_concatenated
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            p102_values.iter().filter_map(|v| v).collect::<Vec<_>>(),
            &[10, 11, 12, 14]
        );
        assert_eq!(p102_concatenated.num_rows(), 5); // Ensure null was preserved
        assert!(p102_values.is_null(p102_values.len() - 2)); // The 3rd original value was null

        // 4. Validate content of partition 103.
        let p103_batches = decompressed_partitions.get(&103).unwrap();
        assert_eq!(
            p103_batches.len(),
            1,
            "Partition 103 should have only one batch"
        );
        let p103_values = p103_batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(p103_values.values(), &[100]);

        Ok(())
    }
}
