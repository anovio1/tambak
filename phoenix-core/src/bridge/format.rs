// In: src/bridge/format.rs

//! Defines all on-disk structures and constants for the Phoenix format.
//! This is the single source of truth for both the file-level container format
//! and the individual chunk format. It establishes the contracts for how
//! frame-level strategies and chunk-level pipelines are described.

use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};

//==================================================================================
// I. File-Level Format & FramePlan Contract
//==================================================================================

/// The magic number to identify the start of a Phoenix file.
pub const FILE_MAGIC: &[u8; 4] = b"PHXF";
/// The current version of the Phoenix file format.
pub const FILE_FORMAT_VERSION: u16 = 1;

/// Metadata for a single physical chunk stored within the file.
/// This struct is purely descriptive, containing location and size information.
/// Its `column_idx` links it to a logical column in the original schema.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChunkManifestEntry {
    pub column_idx: u32,
    pub offset_in_file: u64,
    pub compressed_size: u64,
    pub num_rows: u64,
}

/// A high-level structural operation within a `FramePlan`. This is the core
/// contract describing how `RecordBatch`es were structurally transformed.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "op", content = "params")]
pub enum FrameOperation {
    /// **Contract:** A column at `logical_col_idx` is stored as a standard chunk without any structural re-ordering.
    /// The `Decompressor` should find the corresponding `ChunkManifestEntry` and decompress it normally.
    StandardColumn { logical_col_idx: u32 },
    /// **Contract:** The values for the column at `logical_value_idx` were re-ordered ("re-linearized")
    /// within each `RecordBatch` by sorting on `key_col_idx` then `timestamp_col_idx`.
    /// The decompressor MUST use the corresponding chunks for these three columns for each batch
    /// to apply the inverse permutation and restore the original row order.
    PerBatchRelinearizedColumn {
        logical_value_idx: u32,
        key_col_idx: u32,
        timestamp_col_idx: u32,
    },
    /// **Contract:** The entire file's data was globally sorted by `key_col_idx` then `timestamp_col_idx`.
    /// All column chunks are stored in this sorted order. The chunk identified by `permutation_chunk_idx`
    /// contains the permutation map needed to restore the original row order as an optional post-processing step.
    GlobalSortedFile {
        key_col_idx: u32,
        timestamp_col_idx: u32,
        permutation_chunk_idx: u32, // The `column_idx` of the permutation map chunk.
    },
}

/// The high-level structural plan for the entire Phoenix file. This is the
/// authoritative contract for how the `Decompressor` should interpret the
/// relationship between chunks and `RecordBatch`es.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FramePlan {
    pub version: u16,
    pub operations: Vec<FrameOperation>,
}

/// The file footer, containing the schema, chunk manifest, and the crucial `FramePlan`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileFooter {
    pub schema: Schema,
    pub chunk_manifest: Vec<ChunkManifestEntry>,
    pub writer_version: String,
    /// The high-level plan describing the file's structural organization. If `None`,
    /// the file is assumed to be a simple sequence of standard columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_plan: Option<FramePlan>,
}

//==================================================================================
// II. Chunk-Level Format (from `chunk_pipeline/models.rs`)
//==================================================================================

/// The magic number to identify an individual physical Phoenix chunk.
pub const CHUNK_MAGIC: &[u8; 4] = b"CHNK";
/// The version of the individual chunk format.
pub const CHUNK_FORMAT_VERSION: u16 = 1;

/// The public-facing struct for compression analysis results, returned by `analyze_chunk`.
#[derive(Debug)]
pub struct CompressionStats {
    pub header_size: usize,
    pub data_size: usize,
    pub total_size: usize,
    pub plan_json: String, // This is the JSON of the internal `ChunkPlan` for a single stream.
    pub original_type: String,
}
