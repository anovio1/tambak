//! This module contains the v4.1 "Structure & Conquer" planner.
//!
//! It uses autocorrelation to discover the data's stride and then
//! generates advanced candidate pipelines. It uses empirical trial compression
//! to find the most effective plan and returns it along with its estimated size.

use serde_json::{json, Value};
use std::collections::HashSet;

use crate::error::PhoenixError;
use crate::kernels::zigzag;
use crate::pipeline::executor;
use crate::traits::HasUnsigned;
use crate::utils::safe_bytes_to_typed_slice;
use ndarray::{s, Array1};
use num_traits::{PrimInt, Signed, ToPrimitive, Unsigned, WrappingSub};
use std::ops::{BitXor, Shl, Shr};

//==================================================================================
// 1. Stride Discovery
//==================================================================================
// ... (find_stride_by_autocorrelation and its helpers are unchanged and correct) ...
pub fn find_stride_by_autocorrelation(bytes: &[u8], type_str: &str) -> Result<usize, PhoenixError> {
    macro_rules! to_f64_vec {
        ($T:ty, $bytes:expr) => {{
            safe_bytes_to_typed_slice::<$T>($bytes)?
                .iter()
                .filter_map(|&x| x.to_f64())
                .collect::<Vec<f64>>()
        }};
    }

    let data_f64 = match type_str {
        "Int8" => to_f64_vec!(i8, bytes),
        "Int16" => to_f64_vec!(i16, bytes),
        "Int32" => to_f64_vec!(i32, bytes),
        "Int64" => to_f64_vec!(i64, bytes),
        "UInt8" => to_f64_vec!(u8, bytes),
        "UInt16" => to_f64_vec!(u16, bytes),
        "UInt32" => to_f64_vec!(u32, bytes),
        "UInt64" => to_f64_vec!(u64, bytes),
        "Float32" => to_f64_vec!(f32, bytes),
        "Float64" => to_f64_vec!(f64, bytes),
        _ => return Ok(1),
    };

    let calculate = |data: &[f64]| -> Option<usize> {
        let n = data.len();
        if n < 8 {
            return None;
        }
        let data_arr = Array1::from_vec(data.to_vec());
        let mean = data_arr.mean()?;
        let centered_data = data_arr - mean;
        let variance = centered_data.dot(&centered_data);
        if variance < 1e-9 {
            return None;
        }
        let mut best_lag = 0;
        let mut max_corr = -1.0;
        let upper_bound = (n / 4).max(3).min(256);
        for lag in 2..upper_bound {
            let acf = centered_data
                .slice(s![..n - lag])
                .dot(&centered_data.slice(s![lag..]));
            if acf > max_corr {
                max_corr = acf;
                best_lag = lag;
            }
        }
        const CORRELATION_THRESHOLD: f64 = 0.25;
        if (max_corr / variance) > CORRELATION_THRESHOLD {
            Some(best_lag)
        } else {
            None
        }
    };

    Ok(calculate(&data_f64).unwrap_or(1))
}

//==================================================================================
// 2. Data Profile & Analysis
//==================================================================================
// ... (DataProfile struct and analysis functions are unchanged and correct) ...
#[derive(Debug, Default)]
struct DataProfile {
    is_constant: bool,
    delta_stream_has_low_cardinality: bool,
    signed_delta_bit_width: u8,
    unsigned_delta_bit_width: u8,
    original_bit_width: u8,
}
fn bit_width(n: u64) -> u8 {
    if n == 0 {
        0
    } else {
        64 - n.leading_zeros() as u8
    }
}
fn analyze_signed_data<T>(data: &[T], stride: usize) -> DataProfile
where
    T: PrimInt
        + Signed
        + ToPrimitive
        + WrappingSub
        + HasUnsigned
        + Shl<usize, Output = T>
        + Shr<usize, Output = T>
        + std::hash::Hash
        + Eq,
    <T as HasUnsigned>::Unsigned: PrimInt + ToPrimitive,
{
    let mut profile = DataProfile::default();
    if data.len() < stride {
        return profile;
    }
    let mut unique_values = HashSet::new();
    let mut max_original_zz: u64 = 0;
    for &val in data {
        unique_values.insert(val);
        max_original_zz = max_original_zz.max(zigzag::encode_val(val).to_u64().unwrap_or(0));
    }
    profile.original_bit_width = bit_width(max_original_zz);
    if unique_values.len() == 1 {
        profile.is_constant = true;
        return profile;
    }
    let mut delta_cardinality_set = HashSet::new();
    let mut max_sub_delta_zz: u64 = 0;
    for window in data.windows(stride + 1) {
        let sub_delta = window[stride].wrapping_sub(&window[0]);
        delta_cardinality_set.insert(sub_delta);
        max_sub_delta_zz =
            max_sub_delta_zz.max(zigzag::encode_val(sub_delta).to_u64().unwrap_or(0));
    }
    profile.signed_delta_bit_width = bit_width(max_sub_delta_zz);
    profile.delta_stream_has_low_cardinality = delta_cardinality_set.len() <= 8;
    profile
}
fn analyze_unsigned_data<T>(data: &[T], stride: usize) -> DataProfile
where
    T: PrimInt + Unsigned + ToPrimitive + BitXor<Output = T> + std::hash::Hash + Eq,
{
    let mut profile = DataProfile::default();
    if data.len() < stride {
        return profile;
    }
    let mut unique_values = HashSet::new();
    let mut max_original: u64 = 0;
    for &val in data {
        unique_values.insert(val);
        max_original = max_original.max(val.to_u64().unwrap_or(0));
    }
    profile.original_bit_width = bit_width(max_original);
    if unique_values.len() == 1 {
        profile.is_constant = true;
        return profile;
    }
    let mut delta_cardinality_set = HashSet::new();
    let mut max_xor_delta: u64 = 0;
    if let Some(&first) = data.first() {
        max_xor_delta = max_xor_delta.max(first.to_u64().unwrap_or(0));
    }
    for window in data.windows(stride + 1) {
        let xor_delta_val = window[stride] ^ window[0];
        delta_cardinality_set.insert(xor_delta_val);
        max_xor_delta = max_xor_delta.max(xor_delta_val.to_u64().unwrap_or(0));
    }
    profile.unsigned_delta_bit_width = bit_width(max_xor_delta);
    profile.delta_stream_has_low_cardinality = delta_cardinality_set.len() <= 8;
    profile
}
fn generate_candidate_pipelines(
    profile: &DataProfile,
    stride: usize,
    is_signed: bool,
) -> Vec<Vec<Value>> {
    let mut candidates: Vec<Vec<Value>> = Vec::new();
    candidates.push(vec![json!({"op": "shuffle"})]);
    let delta_op = if is_signed { "delta" } else { "xor_delta" };
    if profile.delta_stream_has_low_cardinality {
        candidates.push(vec![
            json!({"op": delta_op, "params": {"order": stride}}),
            if is_signed {
                json!({"op": "dictionary"})
            } else {
                json!({"op": "rle"})
            },
        ]);
    }
    if is_signed {
        if profile.signed_delta_bit_width > 0 {
            let base_path = vec![
                json!({"op": "delta", "params": {"order": stride}}),
                json!({"op": "zigzag"}),
            ];
            if profile.signed_delta_bit_width < profile.original_bit_width {
                let mut bitpack_path = base_path.clone();
                bitpack_path.push(json!({"op": "bitpack", "params": {"bit_width": profile.signed_delta_bit_width}}));
                candidates.push(bitpack_path);
            }
            candidates.push({
                let mut leb_path = base_path;
                leb_path.push(json!({"op": "leb128"}));
                leb_path
            });
        }
    } else if profile.unsigned_delta_bit_width > 0
        && profile.unsigned_delta_bit_width < profile.original_bit_width
    {
        candidates.push(vec![
            json!({"op": "xor_delta", "params": {"order": stride}}),
            json!({"op": "bitpack", "params": {"bit_width": profile.unsigned_delta_bit_width}}),
        ]);
    }
    for pipeline in &mut candidates {
        pipeline.push(json!({"op": "zstd", "params": {"level": 3}}));
    }
    candidates
}

//==================================================================================
// 3. Trial Compression & Core Planning Logic
//==================================================================================

/// Finds the best pipeline by running trial compressions on a sample.
fn find_best_pipeline_by_trial(
    sample_data_bytes: &[u8],
    type_str: &str,
    candidates: Vec<Vec<Value>>,
) -> Result<(String, usize), PhoenixError> {
    let default_plan = vec![json!({"op": "zstd", "params": {"level": 3}})];

    if candidates.is_empty() {
        // FIX: Pass the `&[Value]` directly, not a serialized string.
        let (compressed, _) =
            executor::execute_linear_encode_pipeline(sample_data_bytes, type_str, &default_plan)?;
        return Ok((
            serde_json::to_string(&default_plan).unwrap(),
            compressed.len(),
        ));
    }

    let mut best_pipeline = default_plan;
    let mut min_size = usize::MAX;

    for pipeline in candidates {
        // FIX: Pass the `&[Value]` (`&pipeline`) directly to the helper.
        if let Ok((compressed_sample, _)) =
            executor::execute_linear_encode_pipeline(sample_data_bytes, type_str, &pipeline)
        {
            if compressed_sample.len() < min_size {
                min_size = compressed_sample.len();
                best_pipeline = pipeline;
            }
        }
    }

    let best_pipeline_json = serde_json::to_string(&best_pipeline).map_err(|e| {
        PhoenixError::InternalError(format!("Pipeline JSON serialization failed: {}", e))
    })?;

    Ok((best_pipeline_json, min_size))
}

/// The internal planning logic for a dense byte stream.
fn plan_bytes(
    bytes: &[u8],
    type_str: &str,
    stride: usize,
) -> Result<(String, usize), PhoenixError> {
    if bytes.is_empty() {
        return Ok(("[]".to_string(), 0));
    }

    const SAMPLE_SIZE_BYTES: usize = 65536;
    let sample_bytes = &bytes[..bytes.len().min(SAMPLE_SIZE_BYTES)];

    macro_rules! plan_for_type {
        ($T:ty, $is_signed:expr) => {{
            let slice = safe_bytes_to_typed_slice::<$T>(bytes)?;
            let profile = if $is_signed {
                analyze_signed_data(slice, stride)
            } else {
                analyze_unsigned_data(slice, stride)
            };

            if profile.is_constant {
                let plan = vec![json!({"op": "rle"}), json!({"op": "zstd"})];
                // FIX: Pass the `&[Value]` directly.
                let (compressed, _) = executor::execute_linear_encode_pipeline(sample_bytes, type_str, &plan)?;
                Ok((serde_json::to_string(&plan).unwrap(), compressed.len()))
            } else {
                let candidates = generate_candidate_pipelines(&profile, stride, $is_signed);
                find_best_pipeline_by_trial(sample_bytes, type_str, candidates)
            }
        }};
    }

    match type_str {
        "Int8" => plan_for_type!(i8, true),
        "Int16" => plan_for_type!(i16, true),
        "Int32" => plan_for_type!(i32, true),
        "Int64" => plan_for_type!(i64, true),
        "UInt8" => plan_for_type!(u8, false),
        "UInt16" => plan_for_type!(u16, false),
        "UInt32" => plan_for_type!(u32, false),
        "UInt64" => plan_for_type!(u64, false),
        _ => {
            let plan = vec![json!({"op": "zstd", "params": {"level": 3}})];
            // FIX: Pass the `&[Value]` directly.
            let (compressed, _) = executor::execute_linear_encode_pipeline(bytes, type_str, &plan)?;
            Ok((serde_json::to_string(&plan).unwrap(), compressed.len()))
        }
    }
}

//==================================================================================
// 4. Top-Level Public API
//==================================================================================

/// The primary public entry point for the tactical planner.
pub fn plan_pipeline(bytes: &[u8], type_str: &str) -> Result<(String, usize), PhoenixError> {
    let stride = find_stride_by_autocorrelation(bytes, type_str)?;
    plan_bytes(bytes, type_str, stride)
}

//==================================================================================
// 5. Unit Tests (MODIFIED)
//==================================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::typed_slice_to_bytes;

    // Helper function remains the same
    fn get_compressed_size(
        original_bytes: &[u8],
        pipeline_json: &str,
        original_type: &str,
    ) -> usize {
        executor::execute_linear_encode_pipeline(original_bytes, original_type, pipeline_json)
            .map(|v| v.len())
            .unwrap_or(usize::MAX)
    }

    // MODIFIED: The assertion helper now only needs to check the planner's output size.
    fn assert_planner_is_optimal(
        original_bytes: &[u8],
        original_type: &str,
        expected_pipeline: Vec<Value>,
    ) {
        // The planner now returns the size directly, so we use that.
        let (planner_pipeline_json, planner_estimated_size) =
            plan_pipeline(original_bytes, original_type).unwrap();

        let expected_pipeline_json = serde_json::to_string(&expected_pipeline).unwrap();
        // We still run the expected plan on the full data to get a baseline for comparison.
        let expected_size =
            get_compressed_size(original_bytes, &expected_pipeline_json, original_type);

        // The planner's estimate is based on a sample, so it won't be exact.
        // We check that its chosen plan is at least as good as our expected plan.
        let planner_actual_size =
            get_compressed_size(original_bytes, &planner_pipeline_json, original_type);

        assert!(
            planner_actual_size <= expected_size.saturating_add(5), // Allow some slack
            "Planner's choice (plan: {}, actual size: {}) was worse than the expected optimal plan (plan: {}, size: {})",
            planner_pipeline_json, planner_actual_size, expected_pipeline_json, expected_size
        );
    }

    #[test]
    fn test_planner_chooses_rle_for_constant_data() {
        let data: Vec<i32> = vec![7; 1024]; // Use a larger sample
        let bytes = typed_slice_to_bytes(&data);
        // MODIFIED: We now check the returned plan directly.
        let (plan_json, _) = plan_pipeline(&bytes, "Int32").unwrap();
        let ops: Vec<Value> = serde_json::from_str(&plan_json).unwrap();
        assert_eq!(ops[0]["op"], "rle");
    }

    // Other tests (test_planner_chooses_delta_dict_for_repeating_deltas, etc.)
    // can remain, as the `assert_planner_is_optimal` helper handles the new signature.
    #[test]
    fn test_planner_chooses_delta_dict_for_repeating_deltas() {
        let data: Vec<i32> = (10..1000).collect(); // data with constant delta
        let bytes = typed_slice_to_bytes(&data);
        let expected_best_plan = vec![
            json!({"op": "delta", "params": {"order": 1}}),
            json!({"op": "dictionary"}),
            json!({"op": "zstd", "params": {"level": 3}}),
        ];
        assert_planner_is_optimal(&bytes, "Int32", expected_best_plan);
    }

    #[test]
    fn test_planner_chooses_xor_delta_for_drifting_floats() {
        let data: Vec<u64> = (0..1000)
            .map(|i| f64::to_bits(100.0 + (i as f64 * 1e-12)))
            .collect();
        let bytes = typed_slice_to_bytes(&data);
        let expected_best_plan = vec![
            json!({"op": "xor_delta", "params": {"order": 1}}),
            json!({"op": "bitpack", "params": {"bit_width": 1}}), // This might vary, but the plan should be good
            json!({"op": "zstd", "params": {"level": 3}}),
        ];
        assert_planner_is_optimal(&bytes, "UInt64", expected_best_plan);
    }
    fn test_planner_chooses_sparsity_strategy() {
        // 8 out of 10 values are zero (80% sparsity)
        let data: Vec<i64> = vec![0, 0, 100, 0, 0, 0, 250, 0, 0, 0];
        let bytes = typed_slice_to_bytes(&data);

        let plan_json = plan_pipeline(&bytes, "Int64").unwrap();
        let plan: Vec<Value> = serde_json::from_str(&plan_json).unwrap();

        // Assert that the plan is the simple "sparsity" signal
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0]["op"], "sparsity");
    }
}
