//! This module defines the self-describing on-disk format for a single
//! compressed data chunk. It is the single source of truth for serialization
//! and deserialization of the artifact.
//!
//! The v4.1 format uses a unified plan and a map of compressed data streams,
//! creating a fully explicit and extensible artifact.

use crate::error::PhoenixError;
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};

/// Represents a fully compressed column chunk.
/// This struct is the in-memory representation that gets serialized to bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct CompressedChunk {
    /// The total number of rows in the original Arrow array, including nulls.
    pub total_rows: u64,
    /// The original Arrow data type as a string (e.g., "Float32", "Int64").
    pub original_type: String,
    /// The single, unified JSON plan that describes ALL transformations.
    pub plan_json: String,
    /// A map from a logical stream ID (e.g., "main", "null_mask") to its
    /// compressed byte payload.
    pub compressed_streams: HashMap<String, Vec<u8>>,
}

impl CompressedChunk {
    /// Serializes the `CompressedChunk` into a final byte vector for storage or transmission.
    pub fn to_bytes(&self) -> Result<Vec<u8>, PhoenixError> {
        let mut header_buf = Vec::new();

        // Write original_type (u16 length prefix)
        header_buf
            .write_all(&(self.original_type.len() as u16).to_le_bytes())
            .unwrap();
        header_buf.write_all(self.original_type.as_bytes()).unwrap();

        // Write plan_json (u32 length prefix)
        header_buf
            .write_all(&(self.plan_json.len() as u32).to_le_bytes())
            .unwrap();
        header_buf.write_all(self.plan_json.as_bytes()).unwrap();

        // Write stream count
        header_buf
            .write_all(&(self.compressed_streams.len() as u16).to_le_bytes())
            .unwrap();

        let mut stream_payloads = Vec::new();
        for (id, data) in &self.compressed_streams {
            // Write stream ID (u16 length prefix)
            header_buf
                .write_all(&(id.len() as u16).to_le_bytes())
                .unwrap();
            header_buf.write_all(id.as_bytes()).unwrap();
            // Write stream data length
            header_buf
                .write_all(&(data.len() as u64).to_le_bytes())
                .unwrap();
            // Collect the actual payload to be appended later
            stream_payloads.extend_from_slice(data);
        }

        // --- Final Assembly ---
        let mut final_buf = Vec::new();
        final_buf.write_all(b"CHNK").unwrap(); // Magic Number
        final_buf.write_all(&1u16.to_le_bytes()).unwrap(); // Version
        final_buf.write_all(&self.total_rows.to_le_bytes()).unwrap();
        final_buf
            .write_all(&(header_buf.len() as u32).to_le_bytes())
            .unwrap();
        final_buf.write_all(&header_buf).unwrap();
        final_buf.write_all(&stream_payloads).unwrap();

        Ok(final_buf)
    }

    /// Deserializes a byte slice into a `CompressedChunk`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PhoenixError> {
        let mut cursor = Cursor::new(bytes);
        let map_err = |e: std::io::Error| PhoenixError::FrameFormatError(e.to_string());

        // --- Fixed Header ---
        let mut magic_buf = [0u8; 4];
        cursor.read_exact(&mut magic_buf).map_err(map_err)?;
        if &magic_buf != b"CHNK" {
            return Err(PhoenixError::FrameFormatError(
                "Invalid chunk magic number".into(),
            ));
        }

        let mut u16_buf = [0u8; 2];
        cursor.read_exact(&mut u16_buf).map_err(map_err)?;
        let version = u16::from_le_bytes(u16_buf);
        if version != 1 {
            return Err(PhoenixError::FrameFormatError(format!(
                "Unsupported chunk version: {}",
                version
            )));
        }

        let mut u64_buf = [0u8; 8];
        cursor.read_exact(&mut u64_buf).map_err(map_err)?;
        let total_rows = u64::from_le_bytes(u64_buf);

        let mut u32_buf = [0u8; 4];
        cursor.read_exact(&mut u32_buf).map_err(map_err)?;
        let header_len = u32::from_le_bytes(u32_buf) as usize;

        // --- Variable-Length Header ---
        let header_end = cursor.position() as usize + header_len;
        let header_bytes = bytes
            .get(cursor.position() as usize..header_end)
            .ok_or_else(|| PhoenixError::FrameFormatError("Header out of bounds".into()))?;
        let mut header_cursor = Cursor::new(header_bytes);

        let mut read_string = |c: &mut Cursor<&[u8]>,
                               len_bytes: usize|
         -> Result<String, PhoenixError> {
            let len = match len_bytes {
                2 => {
                    let mut buf = [0u8; 2];
                    c.read_exact(&mut buf).map_err(map_err)?;
                    u16::from_le_bytes(buf) as usize
                }
                4 => {
                    let mut buf = [0u8; 4];
                    c.read_exact(&mut buf).map_err(map_err)?;
                    u32::from_le_bytes(buf) as usize
                }
                _ => unreachable!(),
            };
            let mut str_buf = vec![0; len];
            c.read_exact(&mut str_buf).map_err(map_err)?;
            String::from_utf8(str_buf).map_err(|e| PhoenixError::FrameFormatError(e.to_string()))
        };

        let original_type = read_string(&mut header_cursor, 2)?;
        let plan_json = read_string(&mut header_cursor, 4)?;

        header_cursor.read_exact(&mut u16_buf).map_err(map_err)?;
        let stream_count = u16::from_le_bytes(u16_buf);

        let mut stream_metadata = Vec::with_capacity(stream_count as usize);
        for _ in 0..stream_count {
            let id = read_string(&mut header_cursor, 2)?;
            header_cursor.read_exact(&mut u64_buf).map_err(map_err)?;
            let len = u64::from_le_bytes(u64_buf);
            stream_metadata.push((id, len));
        }

        // --- Stream Data Payloads ---
        cursor.set_position(header_end as u64);
        let mut compressed_streams = HashMap::new();
        for (id, len) in stream_metadata {
            let mut data_buf = vec![0; len as usize];
            cursor.read_exact(&mut data_buf).map_err(map_err)?;
            compressed_streams.insert(id, data_buf);
        }

        Ok(Self {
            total_rows,
            original_type,
            plan_json,
            compressed_streams,
        })
    }
}

//==================================================================================
// 2. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_artifact_roundtrip_simple() {
        let mut streams = HashMap::new();
        streams.insert("main".to_string(), vec![1, 2, 3, 4]);
        streams.insert("nulls".to_string(), vec![5, 6]);

        let original_chunk = CompressedChunk {
            total_rows: 100,
            original_type: "Int32".to_string(),
            plan_json: "[{\"op\": \"Delta\"}]".to_string(),
            compressed_streams: streams,
        };

        let bytes = original_chunk.to_bytes().unwrap();
        let reconstructed_chunk = CompressedChunk::from_bytes(&bytes).unwrap();

        assert_eq!(original_chunk, reconstructed_chunk);
    }

    #[test]
    fn test_artifact_roundtrip_empty_streams() {
        let original_chunk = CompressedChunk {
            total_rows: 0,
            original_type: "Float64".to_string(),
            plan_json: "[]".to_string(),
            compressed_streams: HashMap::new(),
        };

        let bytes = original_chunk.to_bytes().unwrap();
        let reconstructed_chunk = CompressedChunk::from_bytes(&bytes).unwrap();

        assert_eq!(original_chunk, reconstructed_chunk);
    }

    #[test]
    fn test_from_bytes_errors() {
        // Test truncated magic number
        let res1 = CompressedChunk::from_bytes(b"abc");
        assert!(matches!(res1, Err(PhoenixError::FrameFormatError(_))));

        // Test bad magic number
        let res2 = CompressedChunk::from_bytes(b"BAD_MAGIC_NUMBER");
        assert!(matches!(res2, Err(PhoenixError::FrameFormatError(_))));

        // Test truncated header
        let valid_start = b"CHNK\x01\x00\x0a\x00\x00\x00\x00\x00\x00\x00";
        let res3 = CompressedChunk::from_bytes(valid_start);
        assert!(matches!(res3, Err(PhoenixError::FrameFormatError(_))));
    }
}
