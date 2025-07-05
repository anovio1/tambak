//! This module contains the v4.0 "Time-Series Re-linearization Engine" (updated in 4.).
//!
//! This module contains the "Time-Series Re-linearization Engine" for `RecordBatch`es.
//! Its purpose is to transform RecordBatch data into a more compressible format
//! by re-ordering based on key and timestamp columns within a single batch.

use arrow::array::{as_primitive_array, Array, PrimitiveArray};
use arrow::compute::kernels::take;
use arrow::datatypes::{ArrowPrimitiveType, Int64Type};
use arrow::record_batch::RecordBatch; // Only for relinearize_all_streams, which might be deprecated or internal
use std::collections::HashMap;

use crate::error::PhoenixError;
use crate::log_metric;

use arrow::array::{ArrayRef, BooleanArray, BooleanBuilder, PrimitiveBuilder};
use arrow::datatypes::{
    BooleanType, DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use std::sync::Arc;

/// A collection of relinearized data streams for a single stream_id.
#[derive(Debug, Default)]
pub struct RelinearizedStreams {
    /// A map from a column's name to its relinearized, time-sorted data bytes.
    pub streams: HashMap<String, Vec<u8>>,
    /// The time-sorted timestamp values for this stream.
    pub timestamps: Vec<i64>,
}

/// A temporary struct to hold data during the grouping phase.
struct TempStreamData {
    timestamp: i64,
    original_index: u32,
}

// --- (Optional: Deprecate/Re-purpose `relinearize_all_streams` if no longer used by new architecture) ---
// If it's only used by `frame_pipeline::profiler` for simulation, that's fine.
// Its implementation needs to be compatible with the new structure.
/// Takes a full RecordBatch and reconstructs the independent streams for ALL columns.
///
/// This is the workhorse for the "Multiplexed Stream" strategy. It performs a
/// group-by on the `stream_id_column` and then sorts the data for all other
/// columns based on the `timestamp_column`.
///
/// # Args
/// * `batch`: The input RecordBatch, assumed to be flattened/multiplexed.
/// * `hints`: Planner hints that MUST contain the `stream_id_column` and `timestamp_column`.
///
/// # Returns
/// A `HashMap` where each key is a stream ID and the value contains all the
/// relinearized data for that stream.
pub fn relinearize_all_streams(
    batch: &RecordBatch,
    hints: &super::profiler::PlannerHints,
) -> Result<HashMap<u64, RelinearizedStreams>, PhoenixError> {
    let stream_id_col_name = hints.stream_id_column.as_ref().ok_or_else(|| {
        PhoenixError::RelinearizationError("Missing stream_id_column hint".to_string())
    })?;
    let timestamp_col_name = hints.timestamp_column.as_ref().ok_or_else(|| {
        PhoenixError::RelinearizationError("Missing timestamp_column hint".to_string())
    })?;

    // 1. Get the key columns for grouping and sorting.
    let stream_id_array = batch.column_by_name(stream_id_col_name).ok_or_else(|| {
        PhoenixError::RelinearizationError(format!(
            "Stream ID column '{}' not found",
            stream_id_col_name
        ))
    })?;
    let stream_ids = as_primitive_array::<Int64Type>(stream_id_array);

    let timestamp_array = batch.column_by_name(timestamp_col_name).ok_or_else(|| {
        PhoenixError::RelinearizationError(format!(
            "Timestamp column '{}' not found",
            timestamp_col_name
        ))
    })?;
    let timestamps = as_primitive_array::<Int64Type>(timestamp_array);

    // 2. Grouping Phase: Group original row indices by stream_id.
    let mut grouped_indices = HashMap::<i64, Vec<TempStreamData>>::new();
    for i in 0..batch.num_rows() {
        if stream_ids.is_valid(i) && timestamps.is_valid(i) {
            let stream_id = stream_ids.value(i);
            let timestamp = timestamps.value(i);
            grouped_indices
                .entry(stream_id)
                .or_default()
                .push(TempStreamData {
                    timestamp,
                    original_index: i as u32,
                });
        }
    }

    // 3. Sorting and Gathering Phase
    let mut final_streams = HashMap::<u64, RelinearizedStreams>::new();
    for (stream_id, mut data) in grouped_indices {
        // Sort by timestamp to create the correct time-series order.
        data.sort_unstable_by_key(|k| k.timestamp);

        let mut relinerized = RelinearizedStreams::default();
        relinerized.timestamps = data.iter().map(|d| d.timestamp).collect();

        // The indices we will use to "gather" data from the original columns.
        let gather_indices =
            arrow::array::UInt32Array::from_iter_values(data.iter().map(|d| d.original_index));

        // For every column in the original batch, gather its values in the new sorted order.
        for field in batch.schema().fields() {
            if field.name() == stream_id_col_name || field.name() == timestamp_col_name {
                continue;
            }
            let column_to_gather = batch.column_by_name(field.name()).unwrap();

            // Use the efficient `take` kernel from arrow-rs to perform the gather.
            let gathered_array = take::take(column_to_gather.as_ref(), &gather_indices, None)
                .map_err(|e| {
                    PhoenixError::RelinearizationError(format!(
                        "Failed to gather column '{}': {}",
                        field.name(),
                        e
                    ))
                })?;

            // We store the raw data buffer bytes. This is a zero-copy operation.
            let value_bytes = gathered_array.to_data().buffers()[0].as_slice().to_vec();
            relinerized
                .streams
                .insert(field.name().clone(), value_bytes);
        }
        final_streams.insert(stream_id as u64, relinerized);
    }

    log_metric!(
        "event" = "relinearize",
        "num_streams_found" = final_streams.len()
    );
    Ok(final_streams)
}

/// Groups and sorts a single value column from a RecordBatch based on provided key and timestamp columns.
/// This produces a single, re-linearized value array.
/// This is used on the compression side for `PerBatchRelinearization`.
pub fn relinearize_single_column_in_batch(
    value_array: &dyn Array,
    key_array: &dyn Array,
    timestamp_array: &dyn Array,
) -> Result<ArrayRef, PhoenixError> {
    let num_rows = value_array.len();
    if num_rows == 0 {
        return Ok(arrow::array::new_empty_array(value_array.data_type()));
    }

    if key_array.len() != num_rows || timestamp_array.len() != num_rows {
        return Err(PhoenixError::RelinearizationError(
            "Key, timestamp, and value arrays must have the same length in a RecordBatch."
                .to_string(),
        ));
    }

    let keys = as_primitive_array::<Int64Type>(key_array);
    let timestamps = as_primitive_array::<Int64Type>(timestamp_array);

    let mut temp_data: Vec<(i64, i64, u32)> = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        if keys.is_valid(i) && timestamps.is_valid(i) {
            temp_data.push((keys.value(i), timestamps.value(i), i as u32));
        }
    }

    temp_data.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    let gather_indices: Vec<u32> = temp_data
        .into_iter()
        .map(|(_, _, original_idx)| original_idx)
        .collect();
    let gather_array = Arc::new(arrow::array::UInt32Array::from_iter_values(gather_indices));

    let re_linearized_array =
        take::take(value_array, gather_array.as_ref(), None).map_err(|e| {
            PhoenixError::RelinearizationError(format!("Failed to re-linearize value array: {}", e))
        })?;

    Ok(re_linearized_array) // `take` returns a `Result<ArrayRef, ...>`, so this is correct
}

/// Reconstructs a single logical column from its re-linearized value array and
/// the original key/timestamp arrays for the batch.
/// This is used on the decompression side for `PerBatchRelinearization`.
pub fn reconstruct_relinearized_column(
    re_linearized_value_array: ArrayRef, // Already decompressed value array, in re-linearized order
    key_array: ArrayRef,                 // Original key column for this batch
    timestamp_array: ArrayRef,           // Original timestamp column for this batch
    original_logical_column_dtype: &DataType, // Target final Arrow DataType
) -> Result<ArrayRef, PhoenixError> {
    let num_rows = key_array.len();
    if num_rows == 0 {
        return Ok(Arc::from(arrow::array::new_empty_array(
            original_logical_column_dtype,
        )));
    }
    if timestamp_array.len() != num_rows || re_linearized_value_array.len() != num_rows {
        return Err(PhoenixError::RelinearizationError(
            "Key, timestamp, and re-linearized value arrays must have the same length for reconstruction.".to_string()
        ));
    }

    let keys = as_primitive_array::<Int64Type>(key_array.as_ref());
    let timestamps = as_primitive_array::<Int64Type>(timestamp_array.as_ref());

    let mut temp_data: Vec<(i64, i64, u32)> = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        if keys.is_valid(i) && timestamps.is_valid(i) {
            temp_data.push((keys.value(i), timestamps.value(i), i as u32));
        }
    }
    temp_data.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    let mut inverse_permutation_indices: Vec<u32> = vec![0; num_rows];
    for (sorted_pos, (_, _, original_idx)) in temp_data.into_iter().enumerate() {
        inverse_permutation_indices[original_idx as usize] = sorted_pos as u32;
    }
    let inverse_gather_array = Arc::new(arrow::array::UInt32Array::from_iter_values(
        inverse_permutation_indices,
    ));

    let reconstructed_array = take::take(
        re_linearized_value_array.as_ref(),
        inverse_gather_array.as_ref(),
        None,
    )
    .map_err(|e| {
        PhoenixError::RelinearizationError(format!(
            "Failed to reconstruct re-linearized column: {}",
            e
        ))
    })?;

    Ok(reconstructed_array.into())
}

//==================================================================================
// 3. Unit Tests
//==================================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Creates a sample multiplexed RecordBatch for testing.
    /// Data:
    /// unit_id | frame | vx
    /// --------------------
    /// 101     | 2     | 10.2 (out of order)
    /// 102     | 1     | 20.1
    /// 101     | 1     | 10.1 (out of order)
    /// 102     | 2     | 20.2
    fn create_test_multiplexed_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("unit_id", DataType::Int64, false),
            Field::new("frame", DataType::Int64, false),
            Field::new("vx", DataType::Float64, false),
        ]));

        let unit_ids = Arc::new(Int64Array::from(vec![101, 102, 101, 102]));
        let frames = Arc::new(Int64Array::from(vec![2, 1, 1, 2]));
        let vxs = Arc::new(Float64Array::from(vec![10.2, 20.1, 10.1, 20.2]));

        RecordBatch::try_new(schema, vec![unit_ids, frames, vxs]).unwrap()
    }

    #[test]
    fn test_relinearize_correctness() {
        let batch = create_test_multiplexed_batch();
        let hints = super::super::profiler::PlannerHints {
            stream_id_column: Some("unit_id".to_string()),
            timestamp_column: Some("frame".to_string()),
        };

        let result = relinearize_all_streams(&batch, &hints).unwrap();

        // Verify we found two streams.
        assert_eq!(result.len(), 2);

        // Verify stream 101
        let stream_101 = result.get(&101).unwrap();
        assert_eq!(stream_101.timestamps, vec![1, 2]); // Check timestamps are sorted
        let vx_101_bytes = stream_101.streams.get("vx").unwrap();
        let vx_101_slice: &[f64] = bytemuck::cast_slice(vx_101_bytes);
        assert_eq!(vx_101_slice, &[10.1, 10.2]); // Check vx values are sorted correctly

        // Verify stream 102
        let stream_102 = result.get(&102).unwrap();
        assert_eq!(stream_102.timestamps, vec![1, 2]);
        let vx_102_bytes = stream_102.streams.get("vx").unwrap();
        let vx_102_slice: &[f64] = bytemuck::cast_slice(vx_102_bytes);
        assert_eq!(vx_102_slice, &[20.1, 20.2]);
    }

    #[test]
    fn test_relinearize_missing_hint_error() {
        let batch = create_test_multiplexed_batch();
        let hints = super::super::profiler::PlannerHints {
            stream_id_column: None, // Missing hint
            timestamp_column: Some("frame".to_string()),
        };

        let result = relinearize_all_streams(&batch, &hints);
        assert!(result.is_err());
        assert!(matches!(result, Err(PhoenixError::RelinearizationError(_))));
    }
}
