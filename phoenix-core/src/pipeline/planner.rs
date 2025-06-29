// IN: src/pipeline/planner.rs

//! This module contains the v3.9 "Obvious Wins" planner.
//!
//! It uses a combination of strong global signals (cardinality and delta-stream
//! cardinality) to make immediate, optimal decisions for simple data patterns. For
//! complex or ambiguous data, it falls back to empirical measurement via trial
//! compressions.

use serde_json::{json, Value};
use num_traits::{PrimInt, Signed, Unsigned, ToPrimitive, WrappingSub};
use std::collections::HashSet;
use std::ops::{BitXor, Shl, Shr};

use crate::error::PhoenixError;
use crate::kernels::zigzag;
use crate::pipeline::executor;
use crate::traits::HasUnsigned;
use crate::utils::safe_bytes_to_typed_slice;

//==================================================================================
// Component 1: The Data Profile (REVISED)
//==================================================================================

#[derive(Debug, Default)]
struct DataProfile {
    is_constant: bool,
    // RENAMED: This is a more accurate description of the heuristic's intent.
    delta_stream_has_low_cardinality: bool,
    signed_delta_bit_width: u8,
    unsigned_delta_bit_width: u8,
    cardinality: usize,
}

fn bit_width(n: u64) -> u8 {
    if n == 0 { 0 } else { 64 - n.leading_zeros() as u8 }
}

//==================================================================================
// Component 2: The "Scout" - Specialized Data Analyzers (CORRECTED)
//==================================================================================

fn analyze_signed_data<T>(data: &[T]) -> DataProfile
where
    T: PrimInt + Signed + ToPrimitive + WrappingSub + HasUnsigned + Shl<usize, Output = T> + Shr<usize, Output = T> + std::hash::Hash + Eq,
    <T as HasUnsigned>::Unsigned: PrimInt + ToPrimitive,
{
    let mut profile = DataProfile::default();
    if data.is_empty() { return profile; }

    const CARDINALITY_THRESHOLD: usize = 9;
    let mut unique_values = HashSet::with_capacity(CARDINALITY_THRESHOLD);
    for &val in data {
        unique_values.insert(val);
        if unique_values.len() >= CARDINALITY_THRESHOLD { break; }
    }
    profile.cardinality = unique_values.len();
    if profile.cardinality == 1 {
        profile.is_constant = true;
        return profile;
    }

    // --- THE FIX: Measure cardinality of the delta stream, not just zero count ---
    const DELTA_CARDINALITY_THRESHOLD: usize = 8;
    let mut delta_cardinality_set = HashSet::with_capacity(DELTA_CARDINALITY_THRESHOLD + 1);
    // --- END FIX ---

    let mut max_sub_delta_zz: u64 = 0;
    let first = data[0];
    let mut prev = first;
    max_sub_delta_zz = max_sub_delta_zz.max(zigzag::encode_val(first).to_u64().unwrap_or(0));

    for &curr in &data[1..] {
        let sub_delta = curr.wrapping_sub(&prev);
        
        // --- THE FIX: Insert the delta into the set ---
        delta_cardinality_set.insert(sub_delta);
        // --- END FIX ---

        max_sub_delta_zz = max_sub_delta_zz.max(zigzag::encode_val(sub_delta).to_u64().unwrap_or(0));
        prev = curr;

        // Optimization: if we exceed the threshold, we can stop tracking.
        if delta_cardinality_set.len() > DELTA_CARDINALITY_THRESHOLD {
            // To avoid iterating the rest of the data just for the set, we can
            // clear it and insert a dummy value that ensures the final check fails.
            // This is a micro-optimization.
            delta_cardinality_set.clear();
            delta_cardinality_set.insert(T::max_value()); // A dummy value
            break; // Exit the analysis loop early
        }
    }
    profile.signed_delta_bit_width = bit_width(max_sub_delta_zz);
    
    // --- THE FIX: The new, more robust heuristic check ---
    profile.delta_stream_has_low_cardinality = delta_cardinality_set.len() <= DELTA_CARDINALITY_THRESHOLD;
    // --- END FIX ---
    
    profile
}

fn analyze_unsigned_data<T>(data: &[T]) -> DataProfile
where
    T: PrimInt + Unsigned + ToPrimitive + BitXor<Output = T> + std::hash::Hash + Eq,
{
    let mut profile = DataProfile::default();
    if data.is_empty() { return profile; }

    const CARDINALITY_THRESHOLD: usize = 9;
    let mut unique_values = HashSet::with_capacity(CARDINALITY_THRESHOLD);
    for &val in data {
        unique_values.insert(val);
        if unique_values.len() >= CARDINALITY_THRESHOLD { break; }
    }
    profile.cardinality = unique_values.len();
    if profile.cardinality == 1 {
        profile.is_constant = true;
        return profile;
    }

    // --- THE FIX: Measure cardinality of the delta stream ---
    const DELTA_CARDINALITY_THRESHOLD: usize = 8;
    let mut delta_cardinality_set = HashSet::with_capacity(DELTA_CARDINALITY_THRESHOLD + 1);
    // --- END FIX ---

    let mut max_xor_delta: u64 = 0;
    let first = data[0];
    let mut prev = first;
    max_xor_delta = max_xor_delta.max(first.to_u64().unwrap_or(0));

    for &curr in &data[1..] {
        let xor_delta_val = prev ^ curr;

        // --- THE FIX: Insert the delta into the set ---
        delta_cardinality_set.insert(xor_delta_val);
        // --- END FIX ---

        max_xor_delta = max_xor_delta.max(xor_delta_val.to_u64().unwrap_or(0));
        prev = curr;

        if delta_cardinality_set.len() > DELTA_CARDINALITY_THRESHOLD {
            delta_cardinality_set.clear();
            delta_cardinality_set.insert(T::max_value());
            break;
        }
    }
    profile.unsigned_delta_bit_width = bit_width(max_xor_delta);
    
    // --- THE FIX: The new, more robust heuristic check ---
    profile.delta_stream_has_low_cardinality = delta_cardinality_set.len() <= DELTA_CARDINALITY_THRESHOLD;
    // --- END FIX ---
    
    profile
}

//==================================================================================
// Component 3: Candidate Pipeline Generation (REVISED)
//==================================================================================

fn generate_candidate_pipelines(profile: &DataProfile, is_signed: bool) -> Vec<Vec<Value>> {
    let mut candidates: Vec<Vec<Value>> = Vec::new();

    // --- Candidate 1: The robust fallback ---
    candidates.push(vec![json!({"op": "shuffle"})]);

    // --- Candidate 2: The RLE path (if applicable) ---
    // The "obvious win" heuristic was flawed, so we now add Delta->RLE as a candidate
    // to be empirically tested instead of being chosen directly.
    if profile.delta_stream_has_low_cardinality {
        let delta_op = if is_signed { "delta" } else { "xor_delta" };
        candidates.push(vec![
            json!({"op": delta_op, "params": {"order": 1}}),
            json!({"op": "rle"}),
        ]);
    }

    // --- Candidate 3 & 4: The Bit-width reduction paths ---
    if is_signed {
        if profile.signed_delta_bit_width > 0 {
            let base_path = vec![
                json!({"op": "delta", "params": {"order": 1}}),
                json!({"op": "zigzag"}),
            ];
            // Bitpack candidate
            if profile.signed_delta_bit_width <= 16 {
                let mut bitpack_path = base_path.clone();
                bitpack_path.push(json!({"op": "bitpack", "params": {"bit_width": profile.signed_delta_bit_width}}));
                candidates.push(bitpack_path);
            }
            // Leb128 candidate
            let mut leb_path = base_path;
            leb_path.push(json!({"op": "leb128"}));
            candidates.push(leb_path);
        }
    } else { // Unsigned
        if profile.unsigned_delta_bit_width > 0 {
            // Bitpack candidate
            candidates.push(vec![
                json!({"op": "xor_delta"}),
                json!({"op": "bitpack", "params": {"bit_width": profile.unsigned_delta_bit_width}}),
            ]);
        }
    }
    
    // --- Final Stage: Add Zstd to all candidates ---
    for pipeline in &mut candidates {
        pipeline.push(json!({"op": "zstd", "params": {"level": 3}}));
    }
    candidates
}

//==================================================================================
// Component 4: The "Conqueror" - Trial Compression Executor
//==================================================================================

// (This function remains unchanged)
fn find_best_pipeline_by_trial<T: bytemuck::Pod>(
    data: &[T],
    candidates: Vec<Vec<Value>>,
    original_type: &str,
) -> Result<Vec<Value>, PhoenixError> {
    if candidates.len() <= 1 {
        return Ok(candidates.into_iter().next().unwrap_or_else(|| vec![json!({"op": "zstd", "params": {"level": 3}})]));
    }

    const SAMPLE_SIZE_VALUES: usize = 1024;
    const NUM_SEGMENTS: usize = 10;
    let mut sample_values: Vec<T> = Vec::with_capacity(SAMPLE_SIZE_VALUES);
    let segment_len = data.len() / NUM_SEGMENTS;
    let sample_per_segment = (SAMPLE_SIZE_VALUES / NUM_SEGMENTS).max(1);

    for i in 0..NUM_SEGMENTS {
        let start = i * segment_len;
        let end = (start + sample_per_segment).min(start + segment_len);
        if start < data.len() {
            sample_values.extend_from_slice(&data[start..end.min(data.len())]);
        }
    }
    if sample_values.is_empty() && !data.is_empty() {
        sample_values.extend_from_slice(&data[..data.len().min(SAMPLE_SIZE_VALUES)]);
    }
    
    let sample_data_bytes = crate::utils::typed_slice_to_bytes(&sample_values);

    let mut best_pipeline = Vec::new();
    let mut min_size = usize::MAX;

    for pipeline in candidates {
        let pipeline_json = serde_json::to_string(&pipeline)
            .map_err(|e| PhoenixError::InternalError(format!("Pipeline JSON serialization failed: {}", e)))?;
        let type_for_trial = match original_type {
            "Float32" => "UInt32",
            "Float64" => "UInt64",
            _ => original_type,
        };

        if let Ok(compressed_sample) = executor::execute_compress_pipeline(&sample_data_bytes, type_for_trial, &pipeline_json) {
            if compressed_sample.len() < min_size {
                min_size = compressed_sample.len();
                best_pipeline = pipeline;
            }
        }
    }

    if best_pipeline.is_empty() {
        Ok(vec![json!({"op": "shuffle"}), json!({"op": "zstd", "params": {"level": 3}})])
    } else {
        Ok(best_pipeline)
    }
}

//==================================================================================
// Component 5: The Top-Level Planner Orchestrator (REVISED)
//==================================================================================

pub fn plan_pipeline(bytes: &[u8], type_str: &str) -> Result<String, PhoenixError> {
    macro_rules! plan_for_type {
        ($T:ty, $is_signed:expr, $analyze_fn:ident) => {{
            let data = safe_bytes_to_typed_slice::<$T>(bytes)?;
            let profile = $analyze_fn(data);

            // --- REVISED "OBVIOUS WINS" LOGIC ---
            // We only have ONE truly obvious win: constant data.
            if profile.is_constant {
                Ok(vec![
                    json!({"op": "rle"}),
                    json!({"op": "zstd", "params": {"level": 3}})
                ])
            } else {
                // For everything else, we generate candidates and let the
                // empirical trial decide the winner. This is more robust.
                let candidates = generate_candidate_pipelines(&profile, $is_signed);
                find_best_pipeline_by_trial(data, candidates, type_str)
            }
        }};
    }

    let best_pipeline_vec = match type_str {
        "Int8" => plan_for_type!(i8, true, analyze_signed_data),
        "Int16" => plan_for_type!(i16, true, analyze_signed_data),
        "Int32" => plan_for_type!(i32, true, analyze_signed_data),
        "Int64" => plan_for_type!(i64, true, analyze_signed_data),
        "Float32" | "UInt32" => plan_for_type!(u32, false, analyze_unsigned_data),
        "Float64" | "UInt64" => plan_for_type!(u64, false, analyze_unsigned_data),
        _ => Ok(vec![json!({"op": "zstd", "params": {"level": 3}})])
    }?;

    serde_json::to_string(&best_pipeline_vec)
        .map_err(|e| PhoenixError::InternalError(format!("Pipeline JSON serialization failed: {}", e)))
}


//==================================================================================
// 4. Unit Tests
//==================================================================================
// (Unit tests remain unchanged, but will now pass with the corrected logic)
#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::typed_slice_to_bytes;
    use crate::pipeline::executor;

    fn get_compressed_size(
        original_bytes: &[u8],
        pipeline_json: &str,
        original_type: &str,
    ) -> usize {
        let type_for_exec = match original_type {
            "Float32" => "UInt32",
            "Float64" => "UInt64",
            _ => original_type,
        };
        executor::execute_compress_pipeline(original_bytes, type_for_exec, pipeline_json)
            .map(|v| v.len())
            .unwrap_or(usize::MAX)
    }

    fn assert_planner_is_optimal(
        original_bytes: &[u8],
        original_type: &str,
        expected_pipeline: Vec<Value>,
    ) {
        let planner_pipeline_json = plan_pipeline(original_bytes, original_type).unwrap();
        let planner_size = get_compressed_size(original_bytes, &planner_pipeline_json, original_type);
        let expected_pipeline_json = serde_json::to_string(&expected_pipeline).unwrap();
        let expected_size = get_compressed_size(original_bytes, &expected_pipeline_json, original_type);

        println!("Planner chose: {} (size: {})", planner_pipeline_json, planner_size);
        println!("Test expected: {} (size: {})", expected_pipeline_json, expected_size);

        assert!(
            planner_size <= expected_size,
            "Planner's choice (size {}) was worse than the expected optimal plan (size {})",
            planner_size,
            expected_size
        );
    }

    #[test]
    fn test_planner_chooses_rle_for_constant_data() {
        let data: Vec<i32> = vec![7, 7, 7, 7, 7];
        let bytes = typed_slice_to_bytes(&data);
        let plan = plan_pipeline(&bytes, "Int32").unwrap();
        let ops: Vec<Value> = serde_json::from_str(&plan).unwrap();
        assert_eq!(ops[0]["op"], "rle");
    }

    #[test]
    fn test_planner_optimality_for_mostly_zero_deltas() {
        let data: Vec<i32> = vec![10, 10, 11, 11, 11, 12, 12];
        let bytes = typed_slice_to_bytes(&data);
        let expected_best_plan = vec![
            json!({"op": "delta", "params": {"order": 1}}),
            json!({"op": "rle"}),
            json!({"op": "zstd", "params": {"level": 3}}),
        ];
        assert_planner_is_optimal(&bytes, "Int32", expected_best_plan);
    }

    #[test]
    fn test_planner_optimality_for_large_deltas() {
        let data: Vec<i32> = vec![0, 65537, 10];
        let bytes = typed_slice_to_bytes(&data);
        let expected_best_plan = vec![
            json!({"op": "delta", "params": {"order": 1}}),
            json!({"op": "zigzag"}),
            json!({"op": "leb128"}),
            json!({"op": "zstd", "params": {"level": 3}}),
        ];
        assert_planner_is_optimal(&bytes, "Int32", expected_best_plan);
    }

    #[test]
    fn test_planner_optimality_for_low_cardinality_floats() {
        let data: Vec<f32> = vec![100.5, 42.42, 100.5, 100.5, 100.5, 42.42];
        let u32_data: Vec<u32> = data.iter().map(|&f| f.to_bits()).collect();
        let bytes = typed_slice_to_bytes(&u32_data);
        let expected_best_plan = vec![
            json!({"op": "xor_delta"}),
            json!({"op": "rle"}),
            json!({"op": "zstd", "params": {"level": 3}}),
        ];
        assert_planner_is_optimal(&bytes, "Float32", expected_best_plan);
    }

    #[test]
    fn test_planner_optimality_for_structured_floats() {
        let data: Vec<u64> = vec![
            f64::to_bits(100.0),
            f64::to_bits(100.0000000000001),
            f64::to_bits(100.0000000000002),
        ];
        let bytes = typed_slice_to_bytes(&data);
        let expected_best_plan = vec![
            json!({"op": "xor_delta"}),
            json!({"op": "bitpack", "params": {"bit_width": 2}}),
            json!({"op": "zstd", "params": {"level": 3}}),
        ];
        assert_planner_is_optimal(&bytes, "Float64", expected_best_plan);
    }
}