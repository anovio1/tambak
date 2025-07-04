// In: src/pipeline/context.rs

use crate::types::PhoenixDataType;

/// The pure, Arrow-agnostic representation of a SINGLE COLUMN's data to be processed.
/// This is the canonical "work order" for the pure compression engine.
///
/// REFINEMENT : We are using fixed fields instead of a HashMap.
/// This is more performant (no runtime lookups), more type-safe (Option<Vec> vs.
/// fallible .get()), and more explicit about the streams our pipeline supports.
#[derive(Debug)]
pub struct PipelineInput {
    /// The main data stream, with nulls removed.
    pub main: Vec<u8>,

    /// The unpacked null mask (1 byte per value, 1=valid, 0=null).
    /// This is `None` if the original array had no nulls.
    pub null_mask: Option<Vec<u8>>,

    /// The original, semantic data type of the column.
    pub initial_dtype: PhoenixDataType,

    /// Total number of rows in the original array slice.
    pub total_rows: usize,

    /// Number of valid (non-null) rows.
    pub num_valid_rows: usize,

    // Placeholder for diagnostics, as suggested by the consultant.
    pub column_name: Option<String>,
}

impl PipelineInput {
    /// Constructs a new, valid `PipelineInput`.
    ///
    /// This is the only way to create a PipelineInput, ensuring all necessary
    /// metadata is provided at the time of creation.
    pub fn new(
        main: Vec<u8>,
        null_mask: Option<Vec<u8>>,
        initial_dtype: PhoenixDataType,
        total_rows: usize,
        num_valid_rows: usize,
        column_name: Option<String>,
    ) -> Self {
        Self {
            main,
            null_mask,
            initial_dtype,
            total_rows,
            num_valid_rows,
            column_name,
        }
    }
}

/// The pure, Arrow-agnostic representation of a decompressed data chunk.
/// This is the "work result" delivered by the pure pipeline back to the bridge.
/// The bridge is then responsible for converting this into an Arrow Array.
#[derive(Debug)]
pub struct PipelineOutput {
    /// The raw, decompressed bytes of the main data stream.
    pub main: Vec<u8>,
    /// The raw, decompressed bytes of the null mask, if one existed.
    pub null_mask: Option<Vec<u8>>,
    /// The original data type of the column.
    pub initial_dtype: PhoenixDataType,
    /// The total number of rows (valid + null) the final array should have.
    pub total_rows: usize,
}

impl PipelineOutput {
    /// Constructs a new, valid `PipelineOutput`.
    pub fn new(
        main: Vec<u8>,
        null_mask: Option<Vec<u8>>,
        initial_dtype: PhoenixDataType,
        total_rows: usize,
    ) -> Self {
        Self {
            main,
            null_mask,
            initial_dtype,
            total_rows,
        }
    }
}
