//! This module defines the self-describing on-disk format for a single
//! compressed data chunk. It is the single source of truth for serialization
//! and deserialization of the artifact.

use crate::error::PhoenixError;
use std::io::{Cursor, Read, Write};

#[derive(Debug)]
pub struct CompressedChunk {
    pub total_rows: u64,
    pub original_type: String,
    // --- Data Fields ---
    pub compressed_data: Vec<u8>,
    pub pipeline_json: String,
    // --- Nullability Fields ---
    pub compressed_nullmap: Vec<u8>,
    // --- SCAFFOLD: Sparsity Fields (Optional) ---
    pub compressed_mask: Option<Vec<u8>>,
    pub mask_pipeline_json: Option<String>,
}

impl CompressedChunk {
    pub fn to_bytes(&self) -> Result<Vec<u8>, PhoenixError> {
        let nullmap_len = self.compressed_nullmap.len() as u64;
        let pipeline_len = self.pipeline_json.len() as u64;
        let type_len = self.original_type.len() as u64;
        // SCAFFOLD: Add lengths for optional sparsity fields
        let mask_len = self.compressed_mask.as_ref().map_or(0, |v| v.len()) as u64;
        let mask_pipeline_len = self.mask_pipeline_json.as_ref().map_or(0, |s| s.len()) as u64;

        let mut buffer = Vec::new();

        // Header: [total_rows, type_len, pipeline_len, nullmap_len, mask_len, mask_pipeline_len] all u64
        buffer
            .write_all(&self.total_rows.to_le_bytes())
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer
            .write_all(&type_len.to_le_bytes())
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer
            .write_all(&pipeline_len.to_le_bytes())
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer
            .write_all(&nullmap_len.to_le_bytes())
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer
            .write_all(&mask_len.to_le_bytes())
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer
            .write_all(&mask_pipeline_len.to_le_bytes())
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;

        // Variable-length parts
        buffer
            .write_all(self.original_type.as_bytes())
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer
            .write_all(self.pipeline_json.as_bytes())
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer
            .write_all(&self.compressed_nullmap)
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        if let Some(mask_bytes) = &self.compressed_mask {
            buffer
                .write_all(mask_bytes)
                .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        }
        if let Some(mask_pipeline) = &self.mask_pipeline_json {
            buffer
                .write_all(mask_pipeline.as_bytes())
                .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        }

        // Main data payload
        buffer
            .write_all(&self.compressed_data)
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;

        Ok(buffer)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PhoenixError> {
        const HEADER_FIXED_SIZE: usize = 6 * 8; // 6 u64s
        if bytes.len() < HEADER_FIXED_SIZE {
            return Err(PhoenixError::BufferMismatch(bytes.len(), HEADER_FIXED_SIZE));
        }
        let mut cursor = Cursor::new(bytes);
        let mut u64_buf = [0u8; 8];

        let mut read_u64 = |c: &mut Cursor<&[u8]>| -> Result<u64, PhoenixError> {
            c.read_exact(&mut u64_buf)
                .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
            Ok(u64::from_le_bytes(u64_buf))
        };

        let total_rows = read_u64(&mut cursor)?;
        let type_len = read_u64(&mut cursor)? as usize;
        let pipeline_len = read_u64(&mut cursor)? as usize;
        let nullmap_len = read_u64(&mut cursor)? as usize;
        let mask_len = read_u64(&mut cursor)? as usize;
        let mask_pipeline_len = read_u64(&mut cursor)? as usize;

        let mut current_pos = HEADER_FIXED_SIZE;
        let mut read_vec = |len: usize| -> Result<Vec<u8>, PhoenixError> {
            let end = current_pos + len;
            let slice = bytes
                .get(current_pos..end)
                .ok_or_else(|| PhoenixError::InternalError("Buffer truncated".to_string()))?;
            current_pos = end;
            Ok(slice.to_vec())
        };

        let original_type = String::from_utf8(read_vec(type_len)?)
            .map_err(|e| PhoenixError::UnsupportedType(e.to_string()))?;
        let pipeline_json = String::from_utf8(read_vec(pipeline_len)?)
            .map_err(|e| PhoenixError::UnsupportedType(e.to_string()))?;
        let compressed_nullmap = read_vec(nullmap_len)?;

        let compressed_mask = if mask_len > 0 {
            Some(read_vec(mask_len)?)
        } else {
            None
        };
        let mask_pipeline_json = if mask_pipeline_len > 0 {
            Some(
                String::from_utf8(read_vec(mask_pipeline_len)?)
                    .map_err(|e| PhoenixError::UnsupportedType(e.to_string()))?,
            )
        } else {
            None
        };

        let compressed_data = bytes[current_pos..].to_vec();

        Ok(Self {
            total_rows,
            original_type,
            compressed_data,
            pipeline_json,
            compressed_nullmap,
            compressed_mask,
            mask_pipeline_json,
        })
    }
}
