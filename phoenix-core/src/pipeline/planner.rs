//! This module contains the v4.0 "Structure & Conquer" planner.
//!
//! It uses the `profiler` to discover the data's fundamental structure and then
//! dispatches to a specialized planning strategy. It generates advanced candidate
//! pipelines, including higher-order delta and hybrid transforms, and uses
//! empirical trial compression to find the most effective plan.

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use hashbrown::HashSet;
use num_traits::{PrimInt, Signed, ToPrimitive, Unsigned, WrappingSub, Zero};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::ops::{BitXor, Shl, Shr};

use crate::error::PhoenixError;
use crate::kernels::{sparsity, zigzag};
use crate::pipeline::{executor, profiler, relinearize};
use crate::traits::HasUnsigned;
use crate::utils::{safe_bytes_to_typed_slice, typed_slice_to_bytes};

//==================================================================================
// 1. Data Profile & Analysis (Unchanged, but now used by all strategies)
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
    #[cfg(debug_assertions)]
    println!("[PLANNER] analyze_signed_data");
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
    #[cfg(debug_assertions)]
    println!("[PLANNER] analyze_unsigned_data");
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

//==================================================================================
// 2. Candidate Pipeline Generation (Unchanged)
//==================================================================================

fn generate_candidate_pipelines(
    profile: &DataProfile,
    stride: usize,
    is_signed: bool,
) -> Vec<Vec<Value>> {
    #[cfg(debug_assertions)]
    println!("[PLANNER] generate_candidate_pipelines");

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

fn find_best_pipeline_by_trial(
    // It now receives the sample directly.
    sample_data_bytes: &[u8],
    type_str: &str,
    candidates: Vec<Vec<Value>>,
) -> Result<String, PhoenixError> {
    #[cfg(debug_assertions)]
    println!("[PLANNER] find_best_pipeline_by_trial");
    if candidates.is_empty() {
        return Ok(serde_json::to_string(&vec![json!({"op": "zstd"})]).unwrap());
    }
    if candidates.len() == 1 {
        return Ok(serde_json::to_string(&candidates[0]).unwrap());
    }
    // No longer need to sample here.
    // const SAMPLE_SIZE_BYTES: usize = 4096;
    // let sample_data_bytes = &data_bytes[..data_bytes.len().min(SAMPLE_SIZE_BYTES)];

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

fn plan_bytes(bytes: &[u8], type_str: &str, stride: usize) -> Result<String, PhoenixError> {

    if bytes.is_empty() {
        return Ok("[]".to_string());
    }

    const SAMPLE_SIZE_BYTES: usize = 65536;
    let sample_bytes = &bytes[..bytes.len().min(SAMPLE_SIZE_BYTES)];

    const SPARSITY_THRESHOLD: f32 = 0.75;

    // SCAFFOLD: This is the correct two-macro pattern.
    // The sparsity check is duplicated, which is necessary to satisfy the compiler's type checker.

    macro_rules! plan_for_signed {
        ($T:ty) => {{
            // 1. Sparsity Check
            let sample_slice = safe_bytes_to_typed_slice::<$T>(sample_bytes)?;
            let zero_count = sample_slice.iter().filter(|&&v| v.is_zero()).count();
            let zero_ratio = if sample_slice.is_empty() { 0.0 } else { zero_count as f32 / sample_slice.len() as f32 };

            if zero_ratio > SPARSITY_THRESHOLD {
                log_metric!("event"="plan_strategy", "outcome"="Sparsity", "zero_ratio"=&zero_ratio);
                return Ok(serde_json::to_string(&vec![json!({"op": "sparsity"})]).unwrap());
            }

            // 2. Dense Strategy
            let full_slice = safe_bytes_to_typed_slice::<$T>(bytes)?;
            let p = analyze_signed_data(full_slice, stride);

            if p.is_constant {
                Ok(serde_json::to_string(&vec![json!({"op": "rle"}), json!({"op": "zstd"})]).unwrap())
            } else {
                find_best_pipeline_by_trial(sample_bytes, type_str, generate_candidate_pipelines(&p, stride, true))
            }
        }};
    }

    macro_rules! plan_for_unsigned {
        ($T:ty) => {{
            // 1. Sparsity Check
            let sample_slice = safe_bytes_to_typed_slice::<$T>(sample_bytes)?;
            let zero_count = sample_slice.iter().filter(|&&v| v.is_zero()).count();
            let zero_ratio = if sample_slice.is_empty() { 0.0 } else { zero_count as f32 / sample_slice.len() as f32 };

            if zero_ratio > SPARSITY_THRESHOLD {
                log_metric!("event"="plan_strategy", "outcome"="Sparsity", "zero_ratio"=&zero_ratio);
                return Ok(serde_json::to_string(&vec![json!({"op": "sparsity"})]).unwrap());
            }

            // 2. Dense Strategy
            let full_slice = safe_bytes_to_typed_slice::<$T>(bytes)?;
            let p = analyze_unsigned_data(full_slice, stride);

            if p.is_constant {
                Ok(serde_json::to_string(&vec![json!({"op": "rle"}), json!({"op": "zstd"})]).unwrap())
            } else {
                find_best_pipeline_by_trial(sample_bytes, type_str, generate_candidate_pipelines(&p, stride, false))
            }
        }};
    }

    match type_str {
        "Int8" => plan_for_signed!(i8),
        "Int16" => plan_for_signed!(i16),
        "Int32" => plan_for_signed!(i32),
        "Int64" => plan_for_signed!(i64),
        "UInt32" => plan_for_unsigned!(u32),
        "UInt64" => plan_for_unsigned!(u64),
        // Add other unsigned types if needed, e.g., UInt8, UInt16
        _ => Ok(serde_json::to_string(&vec![json!({"op": "zstd"})]).unwrap()),
    }
}

//==================================================================================
// 4. Top-Level Planner Orchestrator (v4.0 Entry Point)
//==================================================================================

/// The main entry point for the v4.0 planner. It discovers the data's structure
/// and dispatches to the appropriate strategy.
// This function replaces the stub in `planner.rs`

/// The main entry point for the v4.0 planner. It discovers the data's structure
/// and dispatches to the appropriate strategy.
///
/// # Args
/// * `col_name`: The name of the column being planned. This is crucial for the
///               Multiplexed strategy to look up the correct data stream.
/// * `array_to_plan`: A reference to the Arrow Array data for the column.
/// * `hints`: Optional user-provided hints to guide the planner.
/// * `full_batch_context`: A reference to the entire RecordBatch, providing
///                         context for the profiler.
/// * `relinearized_data`: Pre-computed relinearized streams, required for the
///                        Multiplexed strategy.
///
/// # Returns
/// A `Result` containing the JSON string of the optimal pipeline.
pub fn plan_chunk(
    col_name: &str,
    array_to_plan: &dyn Array,
    hints: &Option<profiler::PlannerHints>,
    full_batch_context: &RecordBatch,
    relinearized_data: &Option<HashMap<u64, relinearize::RelinearizedStreams>>,
) -> Result<String, PhoenixError> {
    #[cfg(debug_assertions)]
    println!("[PLANNER] plan_chunk");
    // 1. Discover the fundamental structure of the data.
    let structure = profiler::discover_structure(array_to_plan, full_batch_context, hints)?;
    let type_str = array_to_plan.data_type().to_string();

    // --- START: THE FIX ---
    // Helper closure to perform the final, low-level planning.
    // It takes a simple byte slice, finds its internal stride, and then plans.
    let plan_simple_stream = |bytes: &[u8]| -> Result<String, PhoenixError> {
        // Re-run the profiler to find the internal stride of this simplified stream.
        let internal_stride = profiler::find_stride_by_autocorrelation(bytes, &type_str)?;
        plan_bytes(bytes, &type_str, internal_stride)
    };
    // --- END: THE FIX ---

    // 2. Dispatch to the appropriate planning strategy based on the discovered structure.
    match structure {
        profiler::DataStructure::Simple => {
            let data = array_to_plan.to_data();
            let bytes = data.buffers()[0].as_slice();
            // Don't assume stride=1. Find the internal stride of the simple data.
            plan_simple_stream(bytes)
        }
        profiler::DataStructure::FixedStride(n) => {
            // In this case, the profiler has already given us the best stride.
            // We can skip re-profiling and use `n` directly.
            let data = array_to_plan.to_data();
            let bytes = data.buffers()[0].as_slice();
            plan_bytes(bytes, &type_str, n)
        }
        profiler::DataStructure::Multiplexed => {
            let relinearized_map = relinearized_data.as_ref().ok_or_else(|| {
                PhoenixError::StructureDiscoveryError(
                    "Multiplexed planning requires relinearized data, but none was provided"
                        .to_string(),
                )
            })?;

            let largest_stream_bytes = relinearized_map
                .values()
                .filter_map(|rs| rs.streams.get(col_name))
                .max_by_key(|v| v.len())
                .ok_or_else(|| {
                    PhoenixError::StructureDiscoveryError(format!(
                        "No relinearized data found for column '{}'",
                        col_name
                    ))
                })?;

            // Don't assume stride=1. Find the internal stride of the relinearized stream.
            plan_simple_stream(largest_stream_bytes)
        }
    }
}

/// Legacy v3.9 entry point. Does not perform structure discovery.
pub fn plan_pipeline(bytes: &[u8], type_str: &str) -> Result<String, PhoenixError> {
    #[cfg(debug_assertions)]
    println!("[PLANNER] plan_pipeline");
    let stride = profiler::find_stride_by_autocorrelation(bytes, type_str)?;
    plan_bytes(bytes, type_str, stride)
}

//==================================================================================
// 5. Unit Tests (Unchanged, but now validate the corrected logic)
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

        let is_optimal = if expected_size == usize::MAX {
            planner_size < usize::MAX
        } else {
            // Allow 1 byte of slack for header/padding differences between nearly identical pipelines
            planner_size <= expected_size.saturating_add(1)
        };

        assert!(
            is_optimal,
            "Planner's choice (size {}) was worse than the expected optimal plan (size {})",
            planner_size, expected_size
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
    fn test_planner_chooses_xor_delta_for_drifting_floats() {
        let data: Vec<u64> = vec![
            f64::to_bits(100.0),
            f64::to_bits(100.0000000000001),
            f64::to_bits(100.0000000000002),
            f64::to_bits(100.0000000000003),
        ];
        let bytes = typed_slice_to_bytes(&data);
        let expected_best_plan = vec![
            json!({"op": "xor_delta", "params": {"order": 1}}),
            json!({"op": "bitpack", "params": {"bit_width": 1}}),
            json!({"op": "zstd", "params": {"level": 3}}),
        ];
        assert_planner_is_optimal(&bytes, "UInt64", expected_best_plan);
    }
    #[test]

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
