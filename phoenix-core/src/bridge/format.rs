// In: src/bridge/format.rs

//! Defines all on-disk structures and constants for the Phoenix format.
//! This is the single source of truth for both the file-level container format
//! and the individual chunk format.

use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};
// =================================================================================
// === File-Level Format (Container)
// =================================================================================

/// The magic number to identify the start of a Phoenix file.
pub const FILE_MAGIC: &[u8; 4] = b"PHXF";
/// The current version of the Phoenix file format.
pub const FILE_FORMAT_VERSION: u16 = 1;

/// Metadata for a single column chunk stored within the file.
/// This is used in the footer's manifest.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChunkManifestEntry {
    pub column_idx: u32,
    pub offset_in_file: u64,
    pub compressed_size: u64,
    pub num_rows: u64,
}

/// The file footer, containing the schema and chunk manifest.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileFooter {
    pub schema: Schema,
    pub chunk_manifest: Vec<ChunkManifestEntry>,
    pub writer_version: String,
}

// =================================================================================
// === Chunk-Level Format (Payload)
// =================================================================================

/// The magic number to identify an individual Phoenix chunk.
pub const CHUNK_MAGIC: &[u8; 4] = b"CHNK";
/// The version of the individual chunk format.
pub const CHUNK_FORMAT_VERSION: u16 = 1;

/// The public-facing struct for compression analysis results, returned by `analyze_chunk`.
#[derive(Debug)]
pub struct CompressionStats {
    pub header_size: usize,
    pub data_size: usize,
    pub total_size: usize,
    pub plan_json: String,
    pub original_type: String,
}
