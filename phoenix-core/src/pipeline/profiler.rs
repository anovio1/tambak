//! This module contains the v4.0 "Structure Discovery Layer".
//!
//! Its purpose is to inspect the raw data and determine its fundamental layout
//! (e.g., simple, fixed-stride, or multiplexed) before the planner attempts
//! to create a compression pipeline.

use arrow::array::{Array, Float64Array};
use arrow::record_batch::RecordBatch;
use ndarray::{s, Array1};
use crate::error::PhoenixError;

#[derive(Debug, Clone)]
pub struct PlannerHints {
    pub stream_id_column: Option<String>,
    pub timestamp_column: Option<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum DataStructure {
    Simple,
    FixedStride(usize),
    Multiplexed,
}

pub fn discover_structure(
    array: &dyn Array,
    _full_batch_context: &RecordBatch,
    hints: &Option<PlannerHints>
) -> Result<DataStructure, PhoenixError> {
    if hints.as_ref().and_then(|h| h.stream_id_column.as_ref()).is_some() {
        return Ok(DataStructure::Multiplexed);
    }

    if let Some(float_array) = array.as_any().downcast_ref::<Float64Array>() {
        let sample: Vec<f64> = float_array.iter().take(2048).filter_map(|v| v).collect();
        if sample.len() > 100 { // Need enough data to find a stride
            if let Some(stride) = find_stride_by_autocorrelation(&sample) {
                if stride > 1 {
                    log_metric!("event"="discover_structure", "outcome"="FixedStride", "stride"=&stride);
                    return Ok(DataStructure::FixedStride(stride));
                }
            }
        }
    }

    log_metric!("event"="discover_structure", "outcome"="Simple", "reason"="no_hints_or_strong_stride");
    Ok(DataStructure::Simple)
}

fn find_stride_by_autocorrelation(data: &[f64]) -> Option<usize> {
    let n = data.len();
    let data_arr = Array1::from_vec(data.to_vec());
    let mean = data_arr.mean().unwrap_or(0.0);
    let centered_data = data_arr - mean;

    let mut best_lag = 0;
    let mut max_corr = -1.0;

    // Check for lags from 2 up to a reasonable limit (e.g., n/4)
    for lag in 2..(n / 4).max(3) {
        let acf = centered_data.slice(s![..n-lag]).dot(&centered_data.slice(s![lag..]));
        if acf > max_corr {
            max_corr = acf;
            best_lag = lag;
        }
    }

    // A simple threshold to determine if the correlation is significant
    let variance = centered_data.dot(&centered_data);
    if variance > 0.0 && (max_corr / variance) > 0.2 {
        Some(best_lag)
    } else {
        None
    }
}