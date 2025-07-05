// In: src/frame_pipeline/util.rs

//! Utility functions for the frame pipeline.

use crate::error::PhoenixError;
use arrow::array::{Array, Int64Array, RecordBatch};
use std::collections::HashMap;

/// Creates an index map from a `RecordBatch` based on a key column.
///
/// It scans the key column and produces a `HashMap` where each key maps to a
/// `Vec<u32>` of the row indices where that key appeared. This is a memory-efficient
/// first step for partitioning, as it avoids allocating new `RecordBatch`es.
///
/// # Returns
/// A map from each unique `i64` key to its corresponding row indices.
pub fn create_index_map(
    batch: &RecordBatch,
    key_col_idx: usize,
) -> Result<HashMap<i64, Vec<u32>>, PhoenixError> {
    let key_col = batch
        .column(key_col_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            PhoenixError::InternalError("Partition key must be a 64-bit integer array.".into())
        })?;

    let mut key_to_indices: HashMap<i64, Vec<u32>> = HashMap::new();
    for (row_idx, key) in key_col.iter().enumerate() {
        if let Some(k) = key {
            key_to_indices.entry(k).or_default().push(row_idx as u32);
        }
    }
    Ok(key_to_indices)
}