//! This module contains the orchestration logic for compressing and decompressing
//! entire DataFrames. It acts as a higher-level orchestrator that uses the
//! single-column `orchestrator` as a building block.

use std::io::{Cursor, Read, Write};
use polars::prelude::{DataFrame, Series};
use arrow::array::Array;

use crate::error::PhoenixError;
use crate::pipeline::orchestrator as series_orchestrator; // Our existing component

//==================================================================================
// 1. Phoenix Frame Format (.phnx)
//==================================================================================

/// Represents one entry in the "Table of Contents" of our frame format.
struct ColumnIndexEntry {
    name: String,
    dtype: String,
    offset: u64,
    length: u64,
}

impl ColumnIndexEntry {
    /// Serializes the index entry into a writer.
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<(), PhoenixError> {
        writer.write_all(&(self.name.len() as u16).to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        writer.write_all(self.name.as_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        writer.write_all(&(self.dtype.len() as u16).to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        writer.write_all(self.dtype.as_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        writer.write_all(&self.offset.to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        writer.write_all(&self.length.to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        Ok(())
    }

    /// Deserializes an index entry from a reader.
    fn read_from<R: Read>(reader: &mut R) -> Result<Self, PhoenixError> {
        let mut u16_buf = [0u8; 2];
        reader.read_exact(&mut u16_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let name_len = u16::from_le_bytes(u16_buf) as usize;
        let mut name_bytes = vec![0u8; name_len];
        reader.read_exact(&mut name_bytes).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let name = String::from_utf8(name_bytes).map_err(|e| PhoenixError::InternalError(e.to_string()))?;

        reader.read_exact(&mut u16_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let dtype_len = u16::from_le_bytes(u16_buf) as usize;
        let mut dtype_bytes = vec![0u8; dtype_len];
        reader.read_exact(&mut dtype_bytes).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let dtype = String::from_utf8(dtype_bytes).map_err(|e| PhoenixError::InternalError(e.to_string()))?;

        let mut u64_buf = [0u8; 8];
        reader.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let offset = u64::from_le_bytes(u64_buf);
        reader.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let length = u64::from_le_bytes(u64_buf);

        Ok(Self { name, dtype, offset, length })
    }
}

//==================================================================================
// 2. Frame Orchestration Logic
//==================================================================================

/// Takes a DataFrame and serializes it into the Phoenix Frame Format.
pub fn compress_frame(df: &DataFrame) -> Result<Vec<u8>, PhoenixError> {
    let mut column_blobs: Vec<Vec<u8>> = Vec::new();
    let mut column_index: Vec<ColumnIndexEntry> = Vec::new();

    // 1. Iterate and compress each column individually using our existing orchestrator.
    for series in df.get_columns() {
        let compressed_chunk = series_orchestrator::compress_chunk(series)?;
        column_blobs.push(compressed_chunk);
    }

    // 2. Assemble the final byte artifact.
    let mut final_buffer = Vec::new();
    final_buffer.write_all(b"PHNX").unwrap(); // Magic number
    final_buffer.write_all(&1u16.to_le_bytes()).unwrap(); // Version
    final_buffer.write_all(&(df.width() as u16).to_le_bytes()).unwrap(); // Num columns

    // Serialize the index to a temporary buffer to know its size.
    let mut index_buffer = Vec::new();
    let mut current_offset = 0u64;
    for (i, series) in df.get_columns().iter().enumerate() {
        let entry = ColumnIndexEntry {
            name: series.name().to_string(),
            dtype: series.dtype().to_string(),
            offset: current_offset,
            length: column_blobs[i].len() as u64,
        };
        entry.write_to(&mut index_buffer)?;
        current_offset += column_blobs[i].len() as u64;
    }

    // Write the index and then the data blobs.
    final_buffer.write_all(&index_buffer).unwrap();
    for blob in column_blobs {
        final_buffer.write_all(&blob).unwrap();
    }

    Ok(final_buffer)
}

/// Takes a byte slice in Phoenix Frame Format and reconstructs a DataFrame.
pub fn decompress_frame(bytes: &[u8]) -> Result<DataFrame, PhoenixError> {
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
    if &magic != b"PHNX" {
        return Err(PhoenixError::InternalError("Not a Phoenix Frame".to_string()));
    }

    let mut u16_buf = [0u8; 2];
    cursor.read_exact(&mut u16_buf).unwrap(); // Version
    cursor.read_exact(&mut u16_buf).unwrap();
    let num_columns = u16::from_le_bytes(u16_buf) as usize;

    // Deserialize the index.
    let mut index: Vec<ColumnIndexEntry> = Vec::with_capacity(num_columns);
    for _ in 0..num_columns {
        index.push(ColumnIndexEntry::read_from(&mut cursor)?);
    }

    // Decompress each column blob using our existing orchestrator.
    let mut reconstructed_series: Vec<Series> = Vec::with_capacity(num_columns);
    let data_start = cursor.position();

    for entry in index {
        let blob_end = data_start + entry.offset + entry.length;
        let blob_slice = &bytes[(data_start + entry.offset) as usize..blob_end as usize];
        
        let arrow_array = series_orchestrator::decompress_chunk(blob_slice, &entry.dtype)?;
        let series = Series::from_arrow(&entry.name, arrow_array)?;
        reconstructed_series.push(series);
    }

    DataFrame::new(reconstructed_series).map_err(PhoenixError::from)
}