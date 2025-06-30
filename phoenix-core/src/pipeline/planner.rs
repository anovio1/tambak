//! This module contains the v4.0 "Structure & Conquer" planner.
//!
//! It uses the `profiler` to discover the data's fundamental structure and then
//! dispatches to a specialized planning strategy. It generates advanced candidate
//! pipelines, including higher-order delta and hybrid transforms, and uses
//! empirical trial compression to find the most effective plan.

use num_traits::{PrimInt, Signed, ToPrimitive, Unsigned, WrappingSub};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::ops::{BitXor, Shl, Shr};

use crate::error::PhoenixError;
use crate::kernels::zigzag;
use crate::pipeline::{executor, profiler};
use crate::traits::HasUnsigned;
use crate::utils::{safe_bytes_to_typed_slice, typed_slice_to_bytes};

//==================================================================================
// 1. Data Profile & Analysis
//==================================================================================

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

/// Analyzes a slice of signed integer data to build a profile for planning.
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

    const CARDINALITY_THRESHOLD: usize = 16;
    let mut unique_values = HashSet::with_capacity(CARDINALITY_THRESHOLD);
    let mut max_original_zz: u64 = 0;
    for &val in data {
        unique_values.insert(val);
        max_original_zz = max_original_zz.max(zigzag::encode_val(val).to_u64().unwrap_or(0));
        if unique_values.len() >= CARDINALITY_THRESHOLD {
            break;
        }
    }
    profile.original_bit_width = bit_width(max_original_zz);
    if unique_values.len() == 1 {
        profile.is_constant = true;
        return profile;
    }

    const DELTA_CARDINALITY_THRESHOLD: usize = 8;
    let mut delta_cardinality_set = HashSet::with_capacity(DELTA_CARDINALITY_THRESHOLD + 1);
    let mut max_sub_delta_zz: u64 = 0;

    for window in data.windows(stride + 1) {
        let sub_delta = window[stride].wrapping_sub(&window[0]);
        delta_cardinality_set.insert(sub_delta);
        max_sub_delta_zz =
            max_sub_delta_zz.max(zigzag::encode_val(sub_delta).to_u64().unwrap_or(0));
    }
    profile.signed_delta_bit_width = bit_width(max_sub_delta_zz);
    profile.delta_stream_has_low_cardinality =
        delta_cardinality_set.len() <= DELTA_CARDINALITY_THRESHOLD;

    profile
}

/// Analyzes a slice of unsigned integer data (or float bit-patterns) for planning.
fn analyze_unsigned_data<T>(data: &[T], stride: usize) -> DataProfile
where
    T: PrimInt + Unsigned + ToPrimitive + BitXor<Output = T> + std::hash::Hash + Eq,
{
    let mut profile = DataProfile::default();
    if data.len() < stride {
        return profile;
    }

    const CARDINALITY_THRESHOLD: usize = 16;
    let mut unique_values = HashSet::with_capacity(CARDINALITY_THRESHOLD);
    let mut max_original: u64 = 0;
    for &val in data {
        unique_values.insert(val);
        max_original = max_original.max(val.to_u64().unwrap_or(0));
        if unique_values.len() >= CARDINALITY_THRESHOLD {
            break;
        }
    }
    profile.original_bit_width = bit_width(max_original);
    if unique_values.len() == 1 {
        profile.is_constant = true;
        return profile;
    }

    const DELTA_CARDINALITY_THRESHOLD: usize = 8;
    let mut delta_cardinality_set = HashSet::with_capacity(DELTA_CARDINALITY_THRESHOLD + 1);
    let mut max_xor_delta: u64 = 0;

    for window in data.windows(stride + 1) {
        let xor_delta_val = window[stride] ^ window[0];
        delta_cardinality_set.insert(xor_delta_val);
        max_xor_delta = max_xor_delta.max(xor_delta_val.to_u64().unwrap_or(0));
    }
    profile.unsigned_delta_bit_width = bit_width(max_xor_delta);
    profile.delta_stream_has_low_cardinality =
        delta_cardinality_set.len() <= DELTA_CARDINALITY_THRESHOLD;

    profile
}

//==================================================================================
// 2. Candidate Pipeline Generation (The "Brain")
//==================================================================================

/// Generates a list of candidate pipelines based on the data profile and structure.
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
            let mut leb_path = base_path;
            leb_path.push(json!({"op": "leb128"}));
            candidates.push(leb_path);
        }
    } else {
        // Unsigned path
        if profile.unsigned_delta_bit_width > 0
            && profile.unsigned_delta_bit_width < profile.original_bit_width
        {
            candidates.push(vec![
                json!({"op": "xor_delta", "params": {"order": stride}}),
                json!({"op": "bitpack", "params": {"bit_width": profile.unsigned_delta_bit_width}}),
            ]);
        }
    }

    for pipeline in &mut candidates {
        pipeline.push(json!({"op": "zstd", "params": {"level": 3}}));
    }
    candidates
}

//==================================================================================
// 3. Trial Compression & Planning
//==================================================================================

fn find_best_pipeline_by_trial(
    data_bytes: &[u8],
    type_str: &str,
    candidates: Vec<Vec<Value>>,
) -> Result<String, PhoenixError> {
    if candidates.is_empty() {
        return Ok(serde_json::to_string(&vec![json!({"op": "zstd"})]).unwrap());
    }
    if candidates.len() == 1 {
        return Ok(serde_json::to_string(&candidates[0]).unwrap());
    }

    const SAMPLE_SIZE_BYTES: usize = 4096;
    let sample_data_bytes = &data_bytes[..data_bytes.len().min(SAMPLE_SIZE_BYTES)];

    let mut best_pipeline = Vec::new();
    let mut min_size = usize::MAX;

    for pipeline in candidates {
        let pipeline_json = serde_json::to_string(&pipeline).unwrap();
        if let Ok(compressed_sample) =
            executor::execute_compress_pipeline(sample_data_bytes, type_str, &pipeline_json)
        {
            if compressed_sample.len() < min_size {
                min_size = compressed_sample.len();
                best_pipeline = pipeline;
            }
        }
    }

    if best_pipeline.is_empty() {
        Ok(serde_json::to_string(&vec![json!({"op": "shuffle"}), json!({"op": "zstd"})]).unwrap())
    } else {
        serde_json::to_string(&best_pipeline).map_err(|e| {
            PhoenixError::InternalError(format!("Pipeline JSON serialization failed: {}", e))
        })
    }
}

fn plan_simple_array(bytes: &[u8], type_str: &str, stride: usize) -> Result<String, PhoenixError> {
    macro_rules! plan_for_signed_type {
        ($T:ty) => {{
            let data = safe_bytes_to_typed_slice::<$T>(bytes)?;
            let profile = analyze_signed_data(data, stride);
            if profile.is_constant {
                Ok(serde_json::to_string(&vec![json!({"op": "rle"}), json!({"op": "zstd"})]).unwrap())
            } else {
                let candidates = generate_candidate_pipelines(&profile, stride, true);
                find_best_pipeline_by_trial(bytes, type_str, candidates)
            }
        }};
    }
    macro_rules! plan_for_unsigned_type {
        ($T:ty) => {{
            let data = safe_bytes_to_typed_slice::<$T>(bytes)?;
            let profile = analyze_unsigned_data(data, stride);
            if profile.is_constant {
                Ok(serde_json::to_string(&vec![json!({"op": "rle"}), json!({"op": "zstd"})]).unwrap())
            } else {
                let candidates = generate_candidate_pipelines(&profile, stride, false);
                find_best_pipeline_by_trial(bytes, type_str, candidates)
            }
        }};
    }

    match type_str {
        "Int8" => plan_for_signed_type!(i8),
        "Int16" => plan_for_signed_type!(i16),
        "Int32" => plan_for_signed_type!(i32),
        "Int64" => plan_for_signed_type!(i64),
        "UInt32" => plan_for_unsigned_type!(u32),
        "UInt64" => plan_for_unsigned_type!(u64),
        _ => Ok(serde_json::to_string(&vec![json!({"op": "zstd"})]).unwrap()),
    }
}

//==================================================================================
// 4. Top-Level Planner Orchestrator
//==================================================================================

pub fn plan_pipeline(bytes: &[u8], type_str: &str) -> Result<String, PhoenixError> {
    // TODO: Plumb full RecordBatch context to the planner to get real structure.
    let structure = profiler::DataStructure::Simple;

    match structure {
        profiler::DataStructure::Simple => plan_simple_array(bytes, type_str, 1),
        profiler::DataStructure::FixedStride(n) => plan_simple_array(bytes, type_str, n),
        profiler::DataStructure::Multiplexed => plan_simple_array(bytes, type_str, 1),
    }
}

//==================================================================================
// 5. Unit Tests
//==================================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::typed_slice_to_bytes;

    fn get_compressed_size(
        original_bytes: &[u8],
        pipeline_json: &str,
        original_type: &str,
    ) -> usize {
        executor::execute_compress_pipeline(original_bytes, original_type, pipeline_json)
            .map(|v| v.len())
            .unwrap_or(usize::MAX)
    }

    fn assert_planner_is_optimal(
        original_bytes: &[u8],
        original_type: &str,
        expected_pipeline: Vec<Value>,
    ) {
        let planner_pipeline_json = plan_pipeline(original_bytes, original_type).unwrap();
        let planner_size =
            get_compressed_size(original_bytes, &planner_pipeline_json, original_type);
        let expected_pipeline_json = serde_json::to_string(&expected_pipeline).unwrap();
        let expected_size =
            get_compressed_size(original_bytes, &expected_pipeline_json, original_type);
        assert!(
            planner_size <= expected_size + 1,
            "Planner's choice (size {}) was worse than the expected optimal plan (size {})",
            planner_size,
            expected_size
        );
    }

    #[test]
    fn test_planner_chooses_rle_for_constant_data() {
        let data: Vec<i32> = vec![7; 8];
        let bytes = typed_slice_to_bytes(&data);
        let plan_json = plan_pipeline(&bytes, "Int32").unwrap();
        let ops: Vec<Value> = serde_json::from_str(&plan_json).unwrap();
        assert_eq!(ops[0]["op"], "rle");
    }

    #[test]
    fn test_planner_chooses_delta_dict_for_repeating_deltas() {
        let data: Vec<i32> = vec![10, 11, 12, 13, 14, 15];
        let bytes = typed_slice_to_bytes(&data);
        let expected_best_plan = vec![
            json!({"op": "delta", "params": {"order": 1}}),
            json!({"op": "dictionary"}),
            json!({"op": "zstd", "params": {"level": 3}}),
        ];
        assert_planner_is_optimal(&bytes, "Int32", expected_best_plan);
    }

    #[test]
    fn test_planner_chooses_bitpack_for_small_deltas() {
        let data: Vec<i32> = vec![100, 101, 103, 104, 106, 107, 109];
        let bytes = typed_slice_to_bytes(&data);
        let expected_best_plan = vec![
            json!({"op": "delta", "params": {"order": 1}}),
            json!({"op": "zigzag"}),
            json!({"op": "bitpack", "params": {"bit_width": 2}}),
            json!({"op": "zstd", "params": {"level": 3}}),
        ];
        assert_planner_is_optimal(&bytes, "Int32", expected_best_plan);
    }

    #[test]
    fn test_planner_chooses_xor_delta_for_drifting_floats() {
        // This data has small bit-wise differences. The bit patterns are passed as u64.
        let data: Vec<u64> = vec![
            f64::to_bits(100.0),
            f64::to_bits(100.0000000000001),
            f64::to_bits(100.0000000000002),
            f64::to_bits(100.0000000000003),
        ];
        let bytes = typed_slice_to_bytes(&data);
        // The XOR delta of these values will be very small, making bitpack optimal.
        let expected_best_plan = vec![
            json!({"op": "xor_delta", "params": {"order": 1}}),
            json!({"op": "bitpack", "params": {"bit_width": 1}}), // XOR deltas are just 1
            json!({"op": "zstd", "params": {"level": 3}}),
        ];
        assert_planner_is_optimal(&bytes, "UInt64", expected_best_plan);
    }
}
