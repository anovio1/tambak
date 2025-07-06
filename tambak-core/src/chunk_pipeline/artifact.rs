//! Defines the self-describing on-disk format for a single compressed chunk.
//! This module is the single source of truth for serialization, deserialization,
//! and efficient metadata peeking of the artifact.

use crate::bridge::format::{CHUNK_FORMAT_VERSION, CHUNK_MAGIC};
use crate::error::tambakError;
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};

//==================================================================================
// Format Constants
//==================================================================================
/// The minimum possible size of a valid chunk artifact in bytes.
const MIN_CHUNK_SIZE: usize = 18; // magic(4) + ver(2) + rows(8) + header_len(4)
/// A reasonable limit to prevent OOM attacks from malformed string/plan lengths. (16MB)
const MAX_REASONABLE_STRING_LEN: usize = 16 * 1024 * 1024;

//==================================================================================
// Public Structs
//==================================================================================

/// A struct that holds the metadata extracted from an artifact's header.
/// This is the return type for the efficient `peek_info` function, allowing inspection
/// without reading all data payloads into memory.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct HeaderInfo {
    /// The version of the chunk format that was parsed.
    pub format_version: u16,
    pub total_rows: u64,
    /// The original data type as a UTF-8 string.
    pub original_type: String,
    /// The compression plan as a UTF-8 JSON string.
    pub plan_json: String,
    /// Metadata for each stream: (stream_id, compressed_size_in_bytes).
    /// The Vec is guaranteed to be sorted by stream_id.
    pub stream_metadata: Vec<(String, usize)>,
    /// The calculated size of the entire header section in bytes.
    pub header_size: usize,
    /// The calculated total size of all compressed data streams.
    pub data_size: usize,
}

/// Represents a fully compressed column chunk in memory.
/// This struct is the target for full deserialization and the source for serialization.
#[derive(Debug, Clone, PartialEq)]
pub struct CompressedChunk {
    pub total_rows: u64,
    /// The original data type as a UTF-8 string.
    pub original_type: String,
    /// The compression plan as a UTF-8 JSON string.
    pub plan_json: String,
    pub compressed_streams: HashMap<String, Vec<u8>>,
}

//==================================================================================
// Core Implementation
//==================================================================================

impl CompressedChunk {
    /// Serializes the `CompressedChunk` into a canonical, final byte vector.
    /// This is the authoritative "writer" for the tambak chunk format. It guarantees
    /// a deterministic output by sorting stream keys.
    /// Serializes the `CompressedChunk` into a canonical, final byte vector.
    pub fn to_bytes(&self) -> Result<Vec<u8>, tambakError> {
        // A temporary buffer to build the variable-length part of the header.
        let mut header_buf = Vec::new();

        // Write non-stream metadata first. These calls return a Result, which
        // we propagate with `?` to adhere to the consultant's advice.
        write_prefixed_string(&mut header_buf, &self.original_type, 2)?;
        write_prefixed_string(&mut header_buf, &self.plan_json, 4)?;

        // CRITICAL FIX: Ensure canonical stream order for deterministic artifacts.
        // We get the keys from the HashMap, sort them alphabetically, and then
        // iterate over the sorted keys. This guarantees that the order of streams
        // in the header metadata and the order of the data payloads are ALWAYS
        // identical, regardless of HashMap's internal layout.
        let mut sorted_keys: Vec<_> = self.compressed_streams.keys().collect();
        sorted_keys.sort();

        // Write the number of streams to follow.
        header_buf
            .write_all(&(sorted_keys.len() as u16).to_le_bytes())
            .map_err(|e| tambakError::FrameFormatError(e.to_string()))?;

        // Write the metadata for each stream using the sorted keys.
        for key in &sorted_keys {
            // We know the key exists because we just got it from the map,
            // so this .unwrap() is safe and will never panic.
            let data = self.compressed_streams.get(*key).unwrap();

            write_prefixed_string(&mut header_buf, key, 2)?;
            header_buf
                .write_all(&(data.len() as u64).to_le_bytes())
                .map_err(|e| tambakError::FrameFormatError(e.to_string()))?;
        }

        // --- Final Assembly ---
        // Writing to a Vec<u8> cannot fail, so .unwrap() is acceptable here,
        // but this approach prepares the final buffer with a pre-calculated capacity.
        let final_size = MIN_CHUNK_SIZE - 4
            + header_buf.len()
            + self
                .compressed_streams
                .values()
                .map(|v| v.len())
                .sum::<usize>();
        let mut final_buf = Vec::with_capacity(final_size);

        // Write the fixed-size file header.
        final_buf.write_all(CHUNK_MAGIC).unwrap();
        final_buf
            .write_all(&CHUNK_FORMAT_VERSION.to_le_bytes())
            .unwrap();
        final_buf.write_all(&self.total_rows.to_le_bytes()).unwrap();
        final_buf
            .write_all(&(header_buf.len() as u32).to_le_bytes())
            .unwrap();

        // Append the variable-length header we just built.
        final_buf.write_all(&header_buf).unwrap();

        // Append all stream payloads IN THE SAME SORTED ORDER.
        for key in &sorted_keys {
            let data = self.compressed_streams.get(*key).unwrap();
            final_buf.write_all(data).unwrap();
        }

        Ok(final_buf)
    }

    /// Deserializes a full byte slice into a `CompressedChunk`.
    /// This reads the entire artifact, including all data payloads, into memory.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, tambakError> {
        // Reuse peek_info to parse the header and get all metadata.
        // This is a clean separation of concerns: peek handles parsing, from_bytes handles payload reading.
        let info = Self::peek_info(bytes)?;

        // FORWARD-COMPATIBILITY: Check version before proceeding with a format we understand.
        if info.format_version != CHUNK_FORMAT_VERSION {
            return Err(tambakError::FrameFormatError(format!(
                "Unsupported chunk version: expected {}, got {}",
                CHUNK_FORMAT_VERSION, info.format_version
            )));
        }

        let mut cursor = Cursor::new(bytes);
        // Seek the cursor to the beginning of the data payloads.
        cursor.set_position(info.header_size as u64);

        let map_err = |e: std::io::Error| tambakError::FrameFormatError(e.to_string());
        let mut compressed_streams = HashMap::with_capacity(info.stream_metadata.len());

        // Read each data payload based on the metadata we already parsed.
        for (id, len) in info.stream_metadata {
            let mut data_buf = vec![0; len];
            cursor.read_exact(&mut data_buf).map_err(map_err)?;
            compressed_streams.insert(id, data_buf);
        }

        // Construct the final in-memory struct.
        Ok(Self {
            total_rows: info.total_rows,
            original_type: info.original_type,
            plan_json: info.plan_json,
            compressed_streams,
        })
    }

    /// Peeks into a serialized chunk's header to efficiently extract metadata
    /// without reading the (potentially large) data payloads.
    pub fn peek_info(bytes: &[u8]) -> Result<HeaderInfo, tambakError> {
        if bytes.len() < MIN_CHUNK_SIZE {
            return Err(tambakError::FrameFormatError(format!(
                "Chunk is too small to be valid. Minimum size: {}, got: {}",
                MIN_CHUNK_SIZE,
                bytes.len()
            )));
        }

        let mut cursor = Cursor::new(bytes);
        let map_err = |e: std::io::Error| tambakError::FrameFormatError(e.to_string());

        // Read and validate the fixed-size portion of the header.
        let mut magic_buf = [0u8; 4];
        cursor.read_exact(&mut magic_buf).map_err(map_err)?;
        if magic_buf != *CHUNK_MAGIC {
            return Err(tambakError::FrameFormatError(
                "Invalid chunk magic number".into(),
            ));
        }

        let mut u16_buf = [0u8; 2];
        cursor.read_exact(&mut u16_buf).map_err(map_err)?;
        let version = u16::from_le_bytes(u16_buf);

        if version != CHUNK_FORMAT_VERSION {
            return Err(tambakError::FrameFormatError(format!(
                "Unsupported chunk version: expected {}, got {}",
                CHUNK_FORMAT_VERSION, version
            )));
        }

        let mut u64_buf = [0u8; 8];
        cursor.read_exact(&mut u64_buf).map_err(map_err)?;
        let total_rows = u64::from_le_bytes(u64_buf);

        let mut u32_buf = [0u8; 4];
        cursor.read_exact(&mut u32_buf).map_err(map_err)?;
        let header_metadata_len = u32::from_le_bytes(u32_buf) as usize;
        let total_header_size = cursor.position() as usize + header_metadata_len;

        // SECURITY: Validate that the declared header length doesn't exceed the buffer size.
        if bytes.len() < total_header_size {
            return Err(tambakError::FrameFormatError(
                "Header length exceeds buffer size".into(),
            ));
        }

        // Create a new cursor scoped to just the variable-length part of the header.
        let header_bytes = &bytes[cursor.position() as usize..total_header_size];
        let mut header_cursor = Cursor::new(header_bytes);

        // Read the variable-length header fields.
        let original_type = read_prefixed_string(&mut header_cursor, 2)?;
        let plan_json = read_prefixed_string(&mut header_cursor, 4)?;

        header_cursor.read_exact(&mut u16_buf).map_err(map_err)?;
        let stream_count = u16::from_le_bytes(u16_buf);
        let mut stream_metadata = Vec::with_capacity(stream_count as usize);
        let mut total_data_size = 0;

        // Read the stream metadata. The order is guaranteed to be sorted by the writer.
        for _ in 0..stream_count {
            let id = read_prefixed_string(&mut header_cursor, 2)?;
            header_cursor.read_exact(&mut u64_buf).map_err(map_err)?;
            let len = u64::from_le_bytes(u64_buf) as usize;
            total_data_size += len;
            stream_metadata.push((id, len));
        }

        // SECURITY: Final check that the sum of parts does not exceed the total file size.
        if total_header_size.saturating_add(total_data_size) > bytes.len() {
            return Err(tambakError::FrameFormatError(
                "Sum of declared header and data sizes exceeds buffer length.".into(),
            ));
        }

        // The sort is REMOVED here. We trust the writer's canonical ordering.

        Ok(HeaderInfo {
            format_version: version,
            total_rows,
            original_type,
            plan_json,
            stream_metadata, // This is already in sorted order.
            header_size: total_header_size,
            data_size: total_data_size,
        })
    }

    /// ERGONOMICS: Provides an iterator over the compressed streams in a guaranteed
    /// canonical (sorted by key) order.
    pub fn streams_sorted(&self) -> Vec<(&String, &Vec<u8>)> {
        let mut items: Vec<_> = self.compressed_streams.iter().collect();
        items.sort_by_key(|(k, _)| *k);
        items
    }
}

//==================================================================================
// Private Helpers
//==================================================================================

fn read_prefixed_string(
    cursor: &mut Cursor<&[u8]>,
    len_bytes: usize,
) -> Result<String, tambakError> {
    let map_err = |e: std::io::Error| tambakError::FrameFormatError(e.to_string());

    let len = match len_bytes {
        2 => {
            let mut buf = [0u8; 2];
            cursor.read_exact(&mut buf).map_err(map_err)?;
            u16::from_le_bytes(buf) as usize
        }
        4 => {
            let mut buf = [0u8; 4];
            cursor.read_exact(&mut buf).map_err(map_err)?;
            u32::from_le_bytes(buf) as usize
        }
        _ => {
            return Err(tambakError::InternalError(
                "Unsupported length prefix size".into(),
            ))
        }
    };

    // SECURITY: Validate length against a sane maximum before allocating.
    if len > MAX_REASONABLE_STRING_LEN {
        return Err(tambakError::FrameFormatError(format!(
            "String length ({}) exceeds maximum allowed size ({})",
            len, MAX_REASONABLE_STRING_LEN
        )));
    }

    let mut str_buf = vec![0; len];
    cursor.read_exact(&mut str_buf).map_err(map_err)?;
    String::from_utf8(str_buf).map_err(|e| tambakError::FrameFormatError(e.to_string()))
}

fn write_prefixed_string<W: Write>(
    writer: &mut W,
    s: &str,
    len_bytes: usize,
) -> Result<(), tambakError> {
    let len = s.len();
    if len > MAX_REASONABLE_STRING_LEN {
        return Err(tambakError::FrameFormatError(format!(
            "String length ({}) exceeds maximum allowed size ({})",
            len, MAX_REASONABLE_STRING_LEN
        )));
    }
    let map_err = |e: std::io::Error| tambakError::FrameFormatError(e.to_string());
    match len_bytes {
        2 => writer
            .write_all(&(len as u16).to_le_bytes())
            .map_err(map_err)?,
        4 => writer
            .write_all(&(len as u32).to_le_bytes())
            .map_err(map_err)?,
        _ => {
            return Err(tambakError::InternalError(
                "Unsupported length prefix size".into(),
            ))
        }
    }
    writer.write_all(s.as_bytes()).map_err(map_err)
}

//==================================================================================
// Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_chunk() -> CompressedChunk {
        let mut streams = HashMap::new();
        // Use unsorted keys to ensure sorting logic is tested
        streams.insert("zeta".to_string(), vec![9; 20]);
        streams.insert("alpha".to_string(), vec![1; 100]);

        CompressedChunk {
            total_rows: 500,
            original_type: "Float64".to_string(),
            plan_json: "{\"op\":\"Test\"}".to_string(),
            compressed_streams: streams,
        }
    }

    #[test]
    fn test_artifact_roundtrip_is_successful() {
        let original = create_test_chunk();
        let bytes = original.to_bytes().unwrap();
        let reconstructed = CompressedChunk::from_bytes(&bytes).unwrap();
        assert_eq!(original, reconstructed);
    }

    #[test]
    fn test_to_bytes_is_deterministic() {
        let chunk1 = create_test_chunk();
        let mut chunk2 = create_test_chunk();
        // Re-insert to potentially change HashMap internal order
        chunk2.compressed_streams.remove("alpha");
        chunk2
            .compressed_streams
            .insert("alpha".to_string(), vec![1; 100]);

        let bytes1 = chunk1.to_bytes().unwrap();
        let bytes2 = chunk2.to_bytes().unwrap();

        // With sorting, the output must be identical.
        assert_eq!(bytes1, bytes2);
    }

    #[test]
    fn test_peek_info_is_correct() {
        let original = create_test_chunk();
        let bytes = original.to_bytes().unwrap();
        let info = CompressedChunk::peek_info(&bytes).unwrap();

        assert_eq!(info.total_rows, 500);
        assert_eq!(info.original_type, "Float64");
        assert_eq!(info.plan_json, "{\"op\":\"Test\"}");
        assert_eq!(info.data_size, 120); // 100 + 20
        assert_eq!(info.header_size + info.data_size, bytes.len());

        // Check that stream metadata was sorted correctly
        assert_eq!(info.stream_metadata[0].0, "alpha");
        assert_eq!(info.stream_metadata[0].1, 100);
        assert_eq!(info.stream_metadata[1].0, "zeta");
        assert_eq!(info.stream_metadata[1].1, 20);
    }

    #[test]
    fn test_parsing_errors_are_handled_gracefully() {
        // Test too short
        let bytes1 = b"short";
        assert!(matches!(
            CompressedChunk::peek_info(bytes1),
            Err(tambakError::FrameFormatError(_))
        ));
        assert!(matches!(
            CompressedChunk::from_bytes(bytes1),
            Err(tambakError::FrameFormatError(_))
        ));

        // Test bad magic number
        let bytes2 = b"BAD_MAGIC_and_the_rest_is_long_enough";
        assert!(matches!(
            CompressedChunk::peek_info(bytes2),
            Err(tambakError::FrameFormatError(_))
        ));

        // Test bad version
        let mut bytes3 = create_test_chunk().to_bytes().unwrap();
        bytes3[4] = 0xFF; // Mutate the version byte
        bytes3[5] = 0xFF;
        assert!(matches!(
            CompressedChunk::peek_info(&bytes3),
            Err(tambakError::FrameFormatError(_))
        ));

        // Add this debug block
        println!("\n[DEBUG - HYPOTHESIS #1: Version Check]");
        let version_check_result = CompressedChunk::peek_info(&bytes3);
        println!(
            "Result of peek_info with bad version: {:?}",
            version_check_result
        );

        // Test truncated header
        let bytes4 = &create_test_chunk().to_bytes().unwrap()[..20];
        assert!(matches!(
            CompressedChunk::peek_info(bytes4),
            Err(tambakError::FrameFormatError(_))
        ));
    }

    #[test]
    fn test_malformed_inputs_are_rejected() {
        // Test string length exceeds buffer
        let mut bytes = create_test_chunk().to_bytes().unwrap();
        // Corrupt the length of the 'original_type' string to be huge,
        // which should cause a parsing error.
        bytes[18] = 0xFF;
        bytes[19] = 0xFF;
        let res = CompressedChunk::peek_info(&bytes);

        assert!(matches!(res, Err(tambakError::FrameFormatError(_))));
    }

    #[test]
    fn test_sorted_streams_iterator() {
        let chunk = create_test_chunk();
        let sorted_streams = chunk.streams_sorted();

        assert_eq!(sorted_streams.len(), 2);
        assert_eq!(*sorted_streams[0].0, "alpha");
        assert_eq!(*sorted_streams[1].0, "zeta");
    }
}
