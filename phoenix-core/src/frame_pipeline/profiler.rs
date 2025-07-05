// In: src/frame_pipeline/profiler.rs

//! This module contains the "Structure Discovery Layer" for `RecordBatch`es.
//! Its purpose is to inspect the raw data and determine its fundamental layout
//! (e.g., simple, fixed-stride, or multiplexed) within the context of a `RecordBatch`.

use arrow::array::{Array, Float64Array};
use arrow::record_batch::RecordBatch;

use crate::error::PhoenixError;
// Import our canonical data type to perform the conversion.
use crate::types::PhoenixDataType;
use crate::{chunk_pipeline, log_metric};

/// A struct to hold user-provided hints that guide the planning process.
#[derive(Debug, Clone, Default)]
pub struct PlannerHints {
    pub stream_id_column: Option<String>,
    pub timestamp_column: Option<String>,
}

/// An enum representing the discovered fundamental structure of the data.
#[derive(Debug, PartialEq, Clone)]
pub enum DataStructure {
    Simple,
    FixedStride(usize),
    Multiplexed, // Indicates a candidate for per-batch re-linearization
}

/// The main entry point for the Structure Discovery Layer for a column within a RecordBatch.
pub fn discover_structure(
    array: &dyn Array,
    _full_batch_context: &RecordBatch, // Renamed to indicate it's not used yet
    hints: &PlannerHints,
) -> Result<DataStructure, PhoenixError> {
    // 1. Check for user-provided hints for multiplexed data.
    if let (Some(stream_id_col_name), Some(timestamp_col_name)) = (
        hints.stream_id_column.as_ref(),
        hints.timestamp_column.as_ref(),
    ) {
        if array.data_type().is_numeric() {
            log_metric!(
                "event" = "discover_structure",
                "outcome" = "Multiplexed",
                "reason" = "user_hint_provided",
                "stream_id_col" = stream_id_col_name,
                "timestamp_col" = timestamp_col_name
            );
            return Ok(DataStructure::Multiplexed);
        }
    }

    // 2. Empirically test for fixed-stride data using autocorrelation.
    if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
        const SAMPLE_SIZE: usize = 2048;
        let sample: Vec<f64> = float_array
            .iter()
            .take(SAMPLE_SIZE)
            .filter_map(|v| v)
            .collect();

        if sample.len() > 100 {
            // Call chunk_pipeline's stride discovery on a sample of the raw bytes
            let sample_bytes = bytemuck::cast_slice(&sample).to_vec();

            // convert Arrow to PhoenixDataType
            let phoenix_dtype = PhoenixDataType::from_arrow_type(array.data_type())?;

            if let Ok(stride) = chunk_pipeline::profiler::find_stride_by_autocorrelation(
                &sample_bytes,
                phoenix_dtype,
            ) {
                if stride > 1 {
                    log_metric!(
                        "event" = "discover_structure",
                        "outcome" = "FixedStride",
                        "stride" = &stride
                    );
                    return Ok(DataStructure::FixedStride(stride));
                }
            }
        }
    }

    // 3. Fallback: If no other structure is detected, assume it's a simple array.
    log_metric!(
        "event" = "discover_structure",
        "outcome" = "Simple",
        "reason" = "no_hints_or_strong_stride"
    );
    Ok(DataStructure::Simple)
}
