//! This module contains the v4.0 "Time-Series Re-linearization Engine".
//!
//! Its purpose is to transform a flattened, multiplexed dataset into a collection
//! of independent, sorted time-series streams, making them highly compressible.

use arrow::array::{Array, as_primitive_array};
use arrow::datatypes::{DataType, Int64Type};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use crate::error::PhoenixError;
use crate::utils::typed_slice_to_bytes;

/// A collection of streams for a single stream_id.
#[derive(Debug, Default)]
pub struct RelinerizedStreams {
    // The key is the column name, the value is the sorted stream data for that column.
    pub streams: HashMap<String, Vec<u8>>,
    pub timestamps: Vec<i64>,
}

/// Takes a full RecordBatch and reconstructs the independent streams for ALL columns.
pub fn relinearize_all_streams(
    batch: &RecordBatch,
    hints: &super::profiler::PlannerHints,
) -> Result<HashMap<u64, RelinerizedStreams>, PhoenixError> {
    let stream_id_col_name = hints.stream_id_column.as_ref().unwrap();
    let timestamp_col_name = hints.timestamp_column.as_ref().unwrap();

    let stream_id_array = batch.column_by_name(stream_id_col_name).ok_or_else(|| PhoenixError::RelinearizationError(format!("Stream ID column '{}' not found", stream_id_col_name)))?;
    let stream_ids = as_primitive_array::<Int64Type>(stream_id_array); // Assuming i64 for now

    let timestamp_array = batch.column_by_name(timestamp_col_name).ok_or_else(|| PhoenixError::RelinearizationError(format!("Timestamp column '{}' not found", timestamp_col_name)))?;
    let timestamps = as_primitive_array::<Int64Type>(timestamp_array);

    // Grouping phase
    type TempStreamData = (i64, usize); // (timestamp, original_row_index)
    let mut grouped_data = HashMap::<i64, Vec<TempStreamData>>::new();
    for i in 0..batch.num_rows() {
        if stream_ids.is_valid(i) && timestamps.is_valid(i) {
            let stream_id = stream_ids.value(i);
            let timestamp = timestamps.value(i);
            grouped_data.entry(stream_id).or_default().push((timestamp, i));
        }
    }

    // Sorting and final assembly phase
    let mut final_streams = HashMap::<u64, RelinerizedStreams>::new();
    for (stream_id, mut data) in grouped_data {
        data.sort_by_key(|k| k.0); // Sort by timestamp

        let mut relinerized = RelinerizedStreams::default();
        relinerized.timestamps = data.iter().map(|(ts, _)| *ts).collect();
        
        for field in batch.schema().fields() {
            if field.name() == stream_id_col_name || field.name() == timestamp_col_name {
                continue;
            }
            let column = batch.column_by_name(field.name()).unwrap();
            let mut value_bytes = Vec::new();
            
            // This is a simplified gather operation. A real implementation would be more efficient.
            for (_, original_index) in &data {
                // This is slow and needs optimization, but demonstrates the principle.
                let scalar = arrow::compute::kernels::take::take(column, &arrow::array::UInt32Array::from(vec![*original_index as u32]), None).unwrap();
                let val_slice = scalar.to_data();
                value_bytes.extend_from_slice(val_slice.buffers()[0].as_slice());
            }
            relinerized.streams.insert(field.name().to_string(), value_bytes);
        }
        final_streams.insert(stream_id as u64, relinerized);
    }

    Ok(final_streams)
}