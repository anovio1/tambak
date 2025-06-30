//! This module contains the v4.0 "Structure Discovery Layer".
//!
//! Its purpose is to inspect the raw data and determine its fundamental layout
//! (e.g., simple, fixed-stride, or multiplexed) before the planner attempts
//! to create a compression pipeline. This allows the planner to select a much
//! more powerful, context-aware strategy.

use arrow::array::{Array, Float64Array};
use arrow::record_batch::RecordBatch;
use ndarray::{s, Array1};

use crate::error::PhoenixError;
use crate::log_metric;

/// A struct to hold user-provided hints that guide the planning process.
#[derive(Debug, Clone)]
pub struct PlannerHints {
    pub stream_id_column: Option<String>,
    pub timestamp_column: Option<String>,
}

/// An enum representing the discovered fundamental structure of the data.
#[derive(Debug, PartialEq, Clone)]
pub enum DataStructure {
    /// A simple, one-dimensional sequence of values.
    Simple,
    /// Data with a fixed-stride pattern, e.g., [a1, b1, c1, a2, b2, c2, ...].
    /// The value is the detected stride (e.g., 3).
    FixedStride(usize),
    /// Data that consists of multiple independent streams keyed by an ID column.
    Multiplexed,
}

/// The main entry point for the Structure Discovery Layer.
///
/// It analyzes a single array within the context of its full RecordBatch and
/// any user-provided hints to determine its structure.
pub fn discover_structure(
    array: &dyn Array,
    _full_batch_context: &RecordBatch,
    hints: &Option<PlannerHints>,
) -> Result<DataStructure, PhoenixError> {
    // 1. Check for the strongest signal: user-provided hints for multiplexed data.
    if hints.as_ref().and_then(|h| h.stream_id_column.as_ref()).is_some() {
        log_metric!("event"="discover_structure", "outcome"="Multiplexed", "reason"="user_hint_provided");
        return Ok(DataStructure::Multiplexed);
    }

    // 2. Empirically test for fixed-stride data using autocorrelation.
    // This is most meaningful for floating-point data.
    if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
        // Autocorrelation is expensive, so we run it on a sample.
        const SAMPLE_SIZE: usize = 2048;
        let sample: Vec<f64> = float_array.iter().take(SAMPLE_SIZE).filter_map(|v| v).collect();

        // We need a minimum amount of data for the analysis to be meaningful.
        const MIN_DATA_POINTS: usize = 100;
        if sample.len() > MIN_DATA_POINTS {
            if let Some(stride) = find_stride_by_autocorrelation(&sample) {
                // A stride of 1 is not a useful pattern, it's just normal correlation.
                if stride > 1 {
                    log_metric!("event"="discover_structure", "outcome"="FixedStride", "stride"=&stride);
                    return Ok(DataStructure::FixedStride(stride));
                }
            }
        }
    }

    // 3. Fallback: If no other structure is detected, assume it's a simple array.
    log_metric!("event"="discover_structure", "outcome"="Simple", "reason"="no_hints_or_strong_stride");
    Ok(DataStructure::Simple)
}

/// Analyzes a data sample to find a dominant period or cycle using autocorrelation.
///
/// # Returns
/// `Some(stride)` if a significant correlation is found for a specific lag,
/// otherwise `None`.
fn find_stride_by_autocorrelation(data: &[f64]) -> Option<usize> {
    let n = data.len();
    let data_arr = Array1::from_vec(data.to_vec());
    let mean = data_arr.mean()?; // ndarray's mean returns None for empty data
    let centered_data = data_arr - mean;

    let variance = centered_data.dot(&centered_data);
    if variance == 0.0 {
        // Data is constant, no stride to be found.
        return None;
    }

    let mut best_lag = 0;
    let mut max_corr = -1.0;

    // Check for lags from 2 up to a reasonable limit (e.g., n/4).
    // A lag of 1 is trivial, so we start at 2.
    for lag in 2..(n / 4).max(3) {
        let acf = centered_data.slice(s![..n - lag]).dot(&centered_data.slice(s![lag..]));
        if acf > max_corr {
            max_corr = acf;
            best_lag = lag;
        }
    }

    // A simple threshold to determine if the correlation is significant enough
    // to be considered a real pattern. This is a tunable heuristic.
    const CORRELATION_THRESHOLD: f64 = 0.25;
    if (max_corr / variance) > CORRELATION_THRESHOLD {
        Some(best_lag)
    } else {
        None
    }
}

//==================================================================================
// 3. Unit Tests
//==================================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_discover_multiplexed_from_hints() {
        let array = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let hints = Some(PlannerHints {
            stream_id_column: Some("unit_id".to_string()),
            timestamp_column: Some("frame".to_string()),
        });

        let structure = discover_structure(&array, &batch, &hints).unwrap();
        assert_eq!(structure, DataStructure::Multiplexed);
    }

    #[test]
    fn test_discover_simple_for_non_float_data() {
        let array = Int32Array::from(vec![1, 2, 3, 1, 2, 3]);
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let hints = None;

        let structure = discover_structure(&array, &batch, &hints).unwrap();
        assert_eq!(structure, DataStructure::Simple);
    }

    #[test]
    fn test_autocorrelation_finds_correct_stride() {
        let mut data = Vec::with_capacity(1000);
        for i in 0..200 {
            data.push(i as f64 * 10.0); // val_a
            data.push(i as f64 * -5.0); // val_b
            data.push(100.0);           // val_c
            data.push(i as f64);        // val_d
            data.push(0.0);             // val_e
        }
        let array = Float64Array::from(data);
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let hints = None;

        let structure = discover_structure(&array, &batch, &hints).unwrap();
        assert_eq!(structure, DataStructure::FixedStride(5));
    }

    #[test]
    fn test_autocorrelation_finds_no_stride_in_random_data() {
        let data: Vec<f64> = (0..1000).map(|_| rand::random()).collect();
        let array = Float64Array::from(data);
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let hints = None;

        let structure = discover_structure(&array, &batch, &hints).unwrap();
        assert_eq!(structure, DataStructure::Simple);
    }
}