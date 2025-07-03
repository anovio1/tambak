// In: src/bridge/decompressor.rs

use crate::bridge::format::{ChunkManifestEntry, FileFooter, FILE_FORMAT_VERSION, FILE_MAGIC};
use crate::bridge::stateless_api::decompress_arrow_chunk;
use crate::error::PhoenixError;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

/// A high-level, stateful object that manages the decompression of a Phoenix file.
/// It reads the file metadata and acts as a factory for a streaming `RecordBatchReader`.
#[derive(Debug)]
pub struct Decompressor<R: Read + Seek> {
    source: R,
    schema: SchemaRef,
    grouped_manifest: BTreeMap<u64, Vec<ChunkManifestEntry>>,
}

impl<R: Read + Seek> Decompressor<R> {
    /// Creates a new decompressor from a readable and seekable source.
    pub fn new(mut source: R) -> Result<Self, PhoenixError> {
        // --- 1. Read and validate the main file header ---
        let mut magic_buf = [0u8; 4];
        source.read_exact(&mut magic_buf)?;
        if magic_buf != *FILE_MAGIC {
            return Err(PhoenixError::FrameFormatError(
                "Invalid Phoenix file magic number.".into(),
            ));
        }

        let mut u16_buf = [0u8; 2];
        source.read_exact(&mut u16_buf)?;
        let found_version = u16::from_le_bytes(u16_buf);
        if found_version != FILE_FORMAT_VERSION {
            return Err(PhoenixError::FrameFormatError(format!(
                "Unsupported file format version. Expected {}, found {}",
                FILE_FORMAT_VERSION, found_version
            )));
        }

        // --- 2. Locate and validate the footer length ---
        let total_file_size = source.seek(SeekFrom::End(0))?;
        if total_file_size < 14 {
            // 6 bytes for header + 8 bytes for footer length
            return Err(PhoenixError::FrameFormatError(
                "File is too small to contain a valid footer.".into(),
            ));
        }
        source.seek(SeekFrom::End(-8))?;
        let mut u64_buf = [0u8; 8];
        source.read_exact(&mut u64_buf)?;
        let footer_len = u64::from_le_bytes(u64_buf);

        if footer_len == 0 {
            return Err(PhoenixError::FrameFormatError(
                "Footer length is zero, invalid Phoenix file.".into(),
            ));
        }

        let footer_start_pos = total_file_size
            .checked_sub(8 + footer_len)
            .ok_or_else(|| PhoenixError::FrameFormatError(format!(
                "Invalid footer length: declared size {} (+8 bytes for length) exceeds file size {}",
                footer_len, total_file_size
            )))?;

        // --- 3. Read and deserialize the footer content ---
        source.seek(SeekFrom::Start(footer_start_pos))?;
        let mut footer_bytes = vec![0; footer_len as usize];
        source.read_exact(&mut footer_bytes)?;

        // // --- DEBUGGING PRINTLN ---
        // // Let's see exactly what the parser is being asked to parse.
        // println!(
        //     "[DEBUG] Decompressor::new - About to parse footer: {}",
        //     String::from_utf8_lossy(&footer_bytes)
        // );

        let footer: FileFooter = serde_json::from_slice(&footer_bytes)?;
        let schema: Schema = footer.schema;

        // --- 4. Process the manifest for streaming (FIX #1 APPLIED) ---
        let mut grouped_manifest = BTreeMap::new();
        let mut batch_idx: u64 = 0; // Use an explicit batch index as the key
        for chunk_entry in footer.chunk_manifest {
            // A new RecordBatch starts when we see the first column (idx 0).
            if chunk_entry.column_idx == 0 && !grouped_manifest.is_empty() {
                batch_idx += 1;
            }
            grouped_manifest
                .entry(batch_idx)
                .or_insert_with(Vec::new)
                .push(chunk_entry);
        }

        Ok(Self {
            source,
            schema: Arc::new(schema),
            grouped_manifest,
        })
    }

    /// Consumes the Decompressor and returns an iterator over the decompressed record batches.
    pub fn batched(self) -> PhoenixBatchReader<R> {
        PhoenixBatchReader {
            source: self.source,
            schema: self.schema,
            manifest_iter: self.grouped_manifest.into_iter(),
        }
    }
}

/// An iterator that yields `RecordBatch`es from a Phoenix file stream.
pub struct PhoenixBatchReader<R: Read + Seek> {
    source: R,
    schema: SchemaRef,
    manifest_iter: std::collections::btree_map::IntoIter<u64, Vec<ChunkManifestEntry>>,
}

impl<R: Read + Seek> Iterator for PhoenixBatchReader<R> {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.manifest_iter.next() {
            Some((_, batch_chunks)) => {
                let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());

                let mut sorted_batch_chunks = batch_chunks;
                sorted_batch_chunks.sort_by_key(|c| c.column_idx);

                for chunk_info in sorted_batch_chunks {
                    if let Err(e) = self.source.seek(SeekFrom::Start(chunk_info.offset_in_file)) {
                        return Some(Err(ArrowError::from(e)));
                    }
                    // Allocate a new buffer for each chunk (minor optimization)
                    let mut chunk_buffer = vec![0; chunk_info.compressed_size as usize];
                    if let Err(e) = self.source.read_exact(&mut chunk_buffer) {
                        return Some(Err(ArrowError::from(e)));
                    }

                    let decompressed_array = match decompress_arrow_chunk(&chunk_buffer) {
                        Ok(arr) => arr,
                        Err(e) => return Some(Err(ArrowError::ExternalError(Box::new(e)))),
                    };

                    columns.push(decompressed_array.into());
                }

                let batch = RecordBatch::try_new(self.schema.clone(), columns);
                Some(batch)
            }
            None => None,
        }
    }
}

impl<R: Read + Seek> RecordBatchReader for PhoenixBatchReader<R> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::format::{ChunkManifestEntry, FileFooter};
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
}
