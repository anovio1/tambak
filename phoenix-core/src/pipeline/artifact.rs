//! This module defines the self-describing on-disk format for a single
//! compressed data chunk. It is the single source of truth for serialization
//! and deserialization of the artifact.

use std::io::{Cursor, Read, Write};
use crate::error::PhoenixError;

#[derive(Debug)]
pub struct CompressedChunk {
    pub total_rows: u64,
    pub compressed_nullmap: Vec<u8>,
    pub compressed_data: Vec<u8>,
    pub pipeline_json: String,
    // --- NEW FIELD ---
    pub original_type: String,
}

impl CompressedChunk {
    pub fn to_bytes(&self) -> Result<Vec<u8>, PhoenixError> {
        let nullmap_len = self.compressed_nullmap.len() as u64;
        let pipeline_len = self.pipeline_json.len() as u64;
        // --- NEW ---
        let type_len = self.original_type.len() as u64;

        // The header is now dynamic, but the length-of-lengths part is fixed.
        // Header: [nullmap_len (u64), pipeline_len (u64), type_len (u64), total_rows (u64)]
        let mut buffer = Vec::new();

        buffer.write_all(&nullmap_len.to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer.write_all(&pipeline_len.to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        // --- NEW ---
        buffer.write_all(&type_len.to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer.write_all(&self.total_rows.to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;

        // Now write the variable-length parts
        buffer.write_all(&self.compressed_nullmap).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer.write_all(self.pipeline_json.as_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        // --- NEW ---
        buffer.write_all(self.original_type.as_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;

        // Finally, write the main data payload
        buffer.write_all(&self.compressed_data).map_err(|e| PhoenixError::InternalError(e.to_string()))?;

        Ok(buffer)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PhoenixError> {
        // Header is now 4 * u64 = 32 bytes for the lengths
        if bytes.len() < 32 {
            return Err(PhoenixError::BufferMismatch(bytes.len(), 32));
        }
        let mut cursor = Cursor::new(bytes);
        let mut u64_buf = [0u8; 8];

        cursor.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let nullmap_len = u64::from_le_bytes(u64_buf) as usize;
        cursor.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let pipeline_len = u64::from_le_bytes(u64_buf) as usize;
        // --- NEW ---
        cursor.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let type_len = u64::from_le_bytes(u64_buf) as usize;
        cursor.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let total_rows = u64::from_le_bytes(u64_buf);

        let header_fixed_size = 32;
        let mut current_pos = header_fixed_size;

        let compressed_nullmap = bytes[current_pos..current_pos + nullmap_len].to_vec();
        current_pos += nullmap_len;

        let pipeline_json = String::from_utf8(bytes[current_pos..current_pos + pipeline_len].to_vec())
            .map_err(|e| PhoenixError::UnsupportedType(e.to_string()))?;
        current_pos += pipeline_len;

        // --- NEW ---
        let original_type = String::from_utf8(bytes[current_pos..current_pos + type_len].to_vec())
            .map_err(|e| PhoenixError::UnsupportedType(e.to_string()))?;
        current_pos += type_len;

        let compressed_data = bytes[current_pos..].to_vec();

        Ok(Self {
            total_rows,
            compressed_nullmap,
            compressed_data,
            pipeline_json,
            original_type,
        })
    }
}