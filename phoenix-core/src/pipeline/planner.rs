// In: src/pipeline/planner.rs

//! The empirical pipeline planner for Phoenix.
//!
//! This module is the "smart" engine responsible for selecting the most
//! effective compression pipeline. It operates by:
//! 1. Generating a set of logically valid candidate pipelines.
//! 2. Empirically evaluating these candidates against the input data using a
//!    configurable strategy (`PlannerStrategy`).
//! 3. Returning the single best `Plan` for execution.
//!
//! This decouples the logic of "what is a valid pipeline?" from the heuristic
//! of "what is the best pipeline for this data?".

use crate::error::PhoenixError;
use crate::kernels::zigzag;
use crate::pipeline::context::PipelineInput;
use crate::pipeline::models::{Operation, Plan};
use crate::pipeline::{self, executor};
use crate::types::PhoenixDataType;
use crate::utils::safe_bytes_to_typed_slice;

use ndarray::{s, Array1};
use num_traits::{PrimInt, Signed, ToPrimitive, Unsigned, WrappingSub};
use std::collections::HashSet;
use std::ops::{BitXor, Shl, Shr};
use std::time::Instant;

// A const for the plan version, ensuring consistency.
const PLAN_VERSION: u32 = 1;



//==================================================================================
// A. Stand-in v4.4 (Unknown Section - For Bridge SOC) 
//==================================================================================

// pub fn create_plan(_input: &PipelineInput) -> Result<Plan, PhoenixError> {
//     // TODO: Implement the new Arrow-agnostic planner logic.
//     todo!("planner::create_plan");
// }

//==================================================================================
// 0. Planning Context (New Struct)
//==================================================================================
/// Encapsulates all necessary context for the planner to make informed decisions.
/// This includes the original semantic type and the current physical type of the data.
#[derive(Clone)]
pub(crate) struct PlanningContext {
    /// The original, semantic data type of the Arrow Array before any transformations.
    pub initial_dtype: PhoenixDataType,
    /// The current physical data type of the byte stream being processed by the planner.
    /// This might differ from `initial_dtype` (e.g., after a BitCast from Float to UInt).
    pub physical_dtype: PhoenixDataType,
    // Future context like null_count, etc., can be added here.
}

//==================================================================================
// 0.1. Logical Type (New Enum)
//==================================================================================
/// Represents the high-level semantic category of the data for planning purposes.
/// This is derived from the `initial_dtype` to guide strategic decisions.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum LogicalType {
    SignedInteger,
    UnsignedInteger,
    Float,
    Other, // Catch-all for types not explicitly handled by specific strategies
}

impl LogicalType {
    /// Derives the `LogicalType` from the `PhoenixDataType`.
    /// This mapping is crucial for the planner to understand the semantic nature
    /// of the data, regardless of its current physical representation.
    fn from_phoenix_type(dtype: PhoenixDataType) -> Self {
        use PhoenixDataType::*;
        match dtype {
            Int8 | Int16 | Int32 | Int64 => LogicalType::SignedInteger,
            UInt8 | UInt16 | UInt32 | UInt64 | Boolean => LogicalType::UnsignedInteger, // Boolean often treated as u8
            Float32 | Float64 => LogicalType::Float,
            _ => LogicalType::Other,
        }
    }
}

//==================================================================================
// 1. Stride Discovery (Refactored for Type Safety)
//==================================================================================
pub fn find_stride_by_autocorrelation(
    bytes: &[u8],
    dtype: PhoenixDataType,
) -> Result<usize, PhoenixError> {
    macro_rules! to_f64_vec {
        ($T:ty, $bytes:expr) => {{
            safe_bytes_to_typed_slice::<$T>($bytes)?
                .iter()
                .filter_map(|&x| x.to_f64())
                .collect::<Vec<f64>>()
        }};
    }

    let data_f64 = match dtype {
        PhoenixDataType::Int8 => to_f64_vec!(i8, bytes),
        PhoenixDataType::Int16 => to_f64_vec!(i16, bytes),
        PhoenixDataType::Int32 => to_f64_vec!(i32, bytes),
        PhoenixDataType::Int64 => to_f64_vec!(i64, bytes),
        PhoenixDataType::UInt8 => to_f64_vec!(u8, bytes),
        PhoenixDataType::UInt16 => to_f64_vec!(u16, bytes),
        PhoenixDataType::UInt32 => to_f64_vec!(u32, bytes),
        PhoenixDataType::UInt64 => to_f64_vec!(u64, bytes),
        PhoenixDataType::Float32 => to_f64_vec!(f32, bytes),
        PhoenixDataType::Float64 => to_f64_vec!(f64, bytes),
        PhoenixDataType::Boolean => return Ok(1), // Autocorrelation not meaningful for booleans
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
        for lag in 1..upper_bound {
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
// 2. Data Profile & Analysis (Internal helpers)
//==================================================================================
#[derive(Debug, Default)]
struct DataProfile {
    is_constant: bool,
    original_stream_has_low_cardinality: bool, // <-- ADD THIS
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
        + crate::traits::HasUnsigned
        + Shl<usize, Output = T>
        + Shr<usize, Output = T>
        + std::hash::Hash
        + Eq,
    <T as crate::traits::HasUnsigned>::Unsigned: PrimInt + ToPrimitive,
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
    profile.original_stream_has_low_cardinality = unique_values.len() <= 256;
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
    profile.original_stream_has_low_cardinality = unique_values.len() <= 256;
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
// 3. Core Planning Logic (Refactored for Strong Types)
//==================================================================================

// Start of new generate_candidate_pipelines
/// Generates candidate pipelines as `Vec<Operation>` instead of JSON.
/// 1. Handling special cases (like constant data) for efficiency.
/// 2. Calling the three stages: core, terminal, and entropy.
/// 3. Ensuring essential baseline pipelines are always included.
pub fn generate_candidate_pipelines(
    profile: &DataProfile,
    stride: usize,
    logical_type: LogicalType,
) -> Vec<Vec<Operation>> {
    // --- Optimization: Handle special cases first ---
    if profile.is_constant {
        // If all values are the same, RLE is almost certainly the best.
        // We generate only RLE-based pipelines and stop.
        let rle_prefix = vec![vec![Operation::Rle]];
        return expand_with_entropy_coders(rle_prefix);
    }

    // --- Stage 1: Generate the base set of core data transformations. ---
    let core_pipelines = generate_core_transforms(stride, logical_type);

    // --- Stage 2: Expand with applicable "terminal" transforms. ---
    let terminal_pipelines = expand_with_terminal_transforms(core_pipelines, profile, logical_type);

    // --- Stage 3: Append final entropy coders to all generated prefixes. ---
    let mut final_candidates = expand_with_entropy_coders(terminal_pipelines);

    // --- Final Touch: Ensure the simplest baselines are always present ---
    final_candidates.push(vec![Operation::Zstd { level: 3 }]);
    final_candidates.push(vec![Operation::Ans]);

    // Optional: Deduplicate the final list if there's a chance of overlap.
    // final_candidates.sort();
    // final_candidates.dedup();

    final_candidates
}

// --- STAGE 1: Core Transforms ---
fn generate_core_transforms(stride: usize, logical_type: LogicalType) -> Vec<Vec<Operation>> {
    let mut pipelines = vec![
        vec![], // Passthrough
        vec![Operation::Shuffle],
    ];

    match logical_type {
        LogicalType::SignedInteger => {
            pipelines.push(vec![Operation::Delta { order: stride }]);
            pipelines.push(vec![Operation::Delta { order: stride }, Operation::ZigZag]);
        }
        LogicalType::UnsignedInteger | LogicalType::Float => {
            // XorDelta is effective for both unsigned integers and the bit representation of floats.
            pipelines.push(vec![Operation::XorDelta]);
        }
        LogicalType::Other => {}
    }
    pipelines
}

// --- STAGE 2: Terminal Transforms ---
fn expand_with_terminal_transforms(
    base_pipelines: Vec<Vec<Operation>>,
    profile: &DataProfile,
    logical_type: LogicalType,
) -> Vec<Vec<Operation>> {
    let mut expanded = Vec::new();

    for pipe in base_pipelines {
        expanded.push(pipe.clone()); // Always include the original path

        let last_op = pipe.last();

        // If the original stream has low cardinality and this is the base passthrough path
        if profile.original_stream_has_low_cardinality && pipe.is_empty() {
            expanded.push(vec![Operation::Dictionary]);
        }

        // --- Reintroduce lost Delta+Dictionary / XorDelta+Rle combinations ---
        // This block handles transformations that apply when the delta stream has low cardinality.
        if profile.delta_stream_has_low_cardinality && last_op.is_some() {
            // Safe to unwrap because of `last_op.is_some()`
            match last_op.unwrap() {
                Operation::Delta { .. } if logical_type == LogicalType::SignedInteger => {
                    // Reintroduces: Delta {..} -> Dictionary for signed integers with low delta cardinality
                    let mut new_pipe = pipe.clone();
                    new_pipe.push(Operation::Dictionary);
                    expanded.push(new_pipe);
                }
                Operation::XorDelta
                    if logical_type == LogicalType::UnsignedInteger
                        || logical_type == LogicalType::Float =>
                {
                    // Reintroduces: XorDelta -> Rle for unsigned integers/floats with low delta cardinality
                    let mut new_pipe = pipe.clone();
                    new_pipe.push(Operation::Rle);
                    expanded.push(new_pipe);
                }
                // No special low-cardinality handling for other `last_op` types or logical types here.
                _ => {}
            }
        }

        // --- Existing Leb128 logic (after ZigZag) ---
        if last_op.is_some() && matches!(last_op.unwrap(), Operation::ZigZag) {
            let mut new_pipe = pipe.clone();
            new_pipe.push(Operation::Leb128);
            expanded.push(new_pipe);
        }

        // --- Existing BitPack logic ---
        let can_bitpack_signed = logical_type == LogicalType::SignedInteger
            && last_op.is_some()
            && matches!(last_op.unwrap(), Operation::ZigZag)
            && profile.signed_delta_bit_width > 0
            && profile.signed_delta_bit_width < profile.original_bit_width;

        let can_bitpack_unsigned = logical_type == LogicalType::UnsignedInteger
            && last_op.is_some()
            && matches!(last_op.unwrap(), Operation::XorDelta)
            && profile.unsigned_delta_bit_width > 0
            && profile.unsigned_delta_bit_width < profile.original_bit_width;

        if can_bitpack_signed || can_bitpack_unsigned {
            let bit_width = if can_bitpack_signed {
                profile.signed_delta_bit_width
            } else {
                profile.unsigned_delta_bit_width
            };
            let mut new_pipe = pipe.clone();
            new_pipe.push(Operation::BitPack { bit_width });
            expanded.push(new_pipe);
        }
    }
    expanded
}

// --- STAGE 3: Entropy Coders ---
fn expand_with_entropy_coders(prefix_pipelines: Vec<Vec<Operation>>) -> Vec<Vec<Operation>> {
    let entropy_coders = [Operation::Zstd { level: 3 }, Operation::Ans];
    let mut final_pipelines = Vec::new();

    for prefix in prefix_pipelines {
        if prefix.is_empty() {
            // Skip empty prefixes as they are handled by the final explicit baselines in the main function.
            // This prevents generating [Zstd] and [Ans] twice if the empty prefix makes it this far.
            continue;
        }
        for coder in &entropy_coders {
            let mut final_pipe = prefix.clone();
            final_pipe.push(coder.clone());
            final_pipelines.push(final_pipe);
        }
    }
    final_pipelines
}

fn find_top_n_candidates_by_trial(
    sample_data_bytes: &[u8],
    physical_dtype: PhoenixDataType,
    candidates: Vec<Vec<Operation>>,
    top_n: usize,
) -> Result<Vec<Vec<Operation>>, PhoenixError> {
    let start_overall = Instant::now();
    if candidates.is_empty() {
        return Ok(vec![]);
    }

    log::info!(
        "\n--- STAGE 1: find_top_n_candidates_by_trial EMPIRICAL SCORING ON SAMPLE (for {:?}) ---",
        physical_dtype
    );

    let mut scored_candidates = Vec::new();

    for pipeline in candidates {
        let start_candidate = Instant::now();
        if let Ok(compressed_sample) =
            executor::execute_linear_encode_pipeline(sample_data_bytes, physical_dtype, &pipeline)
        {
            {
                let duration = start_candidate.elapsed();
                log::info!(
                    "  - Candidate: {:<60} | Score (Size): {} | Time: {:.2?}",
                    format!("{:?}", pipeline),
                    compressed_sample.len(),
                    duration,
                );
            }
            scored_candidates.push((pipeline, compressed_sample.len()));
        } else {
            log::info!(
                "  - Candidate: {:<60} | Score (Size): FAILED TO EXECUTE",
                format!("{:?}", pipeline)
            );
        }
    }

    // Sort by score (size) to find the best candidates
    scored_candidates.sort_by_key(|&(_, score)| score);

    {
        let duration_overall = start_overall.elapsed();
        log::info!(
            "\n--- TOP {} CANDIDATES BY SAMPLE SIZE {:.2?}---",
            top_n, duration_overall
        );
        for (i, (pipeline, size)) in scored_candidates.iter().take(top_n).enumerate() {
            log::info!(
                "  {}. {:<60} | Size: {}",
                i + 1,
                format!("{:?}", pipeline),
                size
            );
        }
    }

    // Take the top N pipelines
    let top_pipelines = scored_candidates
        .into_iter()
        .take(top_n)
        .map(|(pipeline, _)| pipeline)
        .collect();

    Ok(top_pipelines)
}
// END of new generate_candidate_pipelines

/// Finds the best pipeline by trial, now operating on `Vec<Operation>`.
fn find_best_pipeline_by_trial(
    sample_data_bytes: &[u8],
    physical_dtype: PhoenixDataType, // Now accepts physical_dtype
    candidates: Vec<Vec<Operation>>,
) -> Result<(Vec<Operation>, usize), PhoenixError> {
    let start_overall = Instant::now();
    // The default plan is now also a strongly-typed Vec<Operation>.
    let default_plan = vec![Operation::Zstd { level: 3 }];

    if candidates.is_empty() {
        // NOTE: This assumes a refactored executor that we will build next.
        // The new executor will not return the final type string.
        let compressed = executor::execute_linear_encode_pipeline(
            sample_data_bytes,
            physical_dtype,
            &default_plan,
        )?;
        return Ok((default_plan, compressed.len()));
    }

    // --- START: ADD DEBUG LOGGING ---
    log::info!(
        "\n--- find_best_pipeline_by_trial EMPIRICAL PLANNER SCORING (for {:?}) ---",
        physical_dtype
    );

    let mut best_pipeline = default_plan;
    let mut min_size = usize::MAX;

    for pipeline in candidates {
        let start_candidate = Instant::now();
        // NOTE: This call anticipates the refactored executor.
        if let Ok(compressed_sample) =
            executor::execute_linear_encode_pipeline(sample_data_bytes, physical_dtype, &pipeline)
        {
            // --- ADD THIS PRINTLN! ---
            {
                let duration = start_candidate.elapsed();
                log::info!(
                    "  - Candidate: {:<60} | Score (Size): {} | Time: {:.2?}",
                    format!("{:?}", pipeline),
                    compressed_sample.len(),
                    duration,
                );
            }

            if compressed_sample.len() < min_size {
                min_size = compressed_sample.len();
                best_pipeline = pipeline;
            }
        } else {
            // --- (Optional but good) Log failed trials ---
            log::info!(
                "  - Candidate: {:<60} | Score (Size): FAILED TO EXECUTE",
                format!("{:?}", pipeline)
            );
        }
    }

    let duration_overall = start_overall.elapsed();
    log::info!(
        "--- Empirical scoring total time: {:.2?} ---",
        duration_overall
    );

    Ok((best_pipeline, min_size))
}

/// The internal planning function, now returns a `Vec<Operation>` and its cost.
fn plan_bytes(
    bytes: &[u8],
    context: &PlanningContext, // Now accepts PlanningContext
    stride: usize,
) -> Result<(Vec<Operation>, usize), PhoenixError> {
    if bytes.is_empty() {
        return Ok((vec![], 0));
    }

    const SAMPLE_SIZE_BYTES: usize = 65536;
    let sample_bytes = &bytes[..bytes.len().min(SAMPLE_SIZE_BYTES)];

    let logical_type = LogicalType::from_phoenix_type(context.initial_dtype); // Derive logical type

    // --- NEW: DEFENSIVE ASSERTIONS ---
    // This assertion ensures that if the original data was a float, the orchestrator
    // MUST have bit-cast it to an unsigned integer before passing it to the planner.
    // The planner's core logic should never operate on a raw float byte stream.
    debug_assert!(
        !(logical_type == LogicalType::Float && context.physical_dtype.is_float()),
        "Planner Invariant Violated: Planner received a float physical type for a float logical type. The orchestrator should have bit-cast it to an integer physical type first."
    );

    // This assertion validates our core assumption: if the logical type is signed,
    // the physical type we are operating on must also be a signed integer.
    debug_assert!(
        !(logical_type == LogicalType::SignedInteger && !context.physical_dtype.is_signed_int()),
        "Planner Invariant Violated: Logical type is SignedInteger, but physical type is not."
    );
    // --- END: DEFENSIVE ASSERTIONS ---

    macro_rules! plan_for_signed {
        ($T:ty) => {{
            let slice = safe_bytes_to_typed_slice::<$T>(bytes)?;
            let profile = analyze_signed_data(slice, stride);

            if profile.is_constant {
                let plan = vec![Operation::Rle, Operation::Zstd { level: 3 }];
                let compressed = executor::execute_linear_encode_pipeline(
                    sample_bytes,
                    context.physical_dtype,
                    &plan,
                )?;
                Ok((plan, compressed.len()))
            } else {
                let candidates = generate_candidate_pipelines(&profile, stride, logical_type);
                let sample_bytes = &bytes[..bytes.len().min(SAMPLE_SIZE_BYTES)];
                const TOP_N: usize = 3;
                let top_candidates = find_top_n_candidates_by_trial(
                    sample_bytes,
                    context.physical_dtype,
                    candidates,
                    TOP_N,
                )?;
                find_best_pipeline_by_trial(bytes, context.physical_dtype, top_candidates)
            }
        }};
    }

    macro_rules! plan_for_unsigned {
        ($T:ty) => {{
            let slice = safe_bytes_to_typed_slice::<$T>(bytes)?;
            let profile = analyze_unsigned_data(slice, stride);

            if profile.is_constant {
                let plan = vec![Operation::Rle, Operation::Zstd { level: 3 }];
                let compressed = executor::execute_linear_encode_pipeline(
                    sample_bytes,
                    context.physical_dtype,
                    &plan,
                )?;
                Ok((plan, compressed.len()))
            } else {
                let candidates = generate_candidate_pipelines(&profile, stride, logical_type);
                let sample_bytes = &bytes[..bytes.len().min(SAMPLE_SIZE_BYTES)];
                const TOP_N: usize = 3;
                let top_candidates = find_top_n_candidates_by_trial(
                    sample_bytes,
                    context.physical_dtype,
                    candidates,
                    TOP_N,
                )?;
                find_best_pipeline_by_trial(bytes, context.physical_dtype, top_candidates)
            }
        }};
    }

    use PhoenixDataType::*;
    match context.physical_dtype {
        // Use physical_dtype for type-casting the bytes
        Int8 => plan_for_signed!(i8),
        Int16 => plan_for_signed!(i16),
        Int32 => plan_for_signed!(i32),
        Int64 => plan_for_signed!(i64),
        UInt8 => plan_for_unsigned!(u8),
        UInt16 => plan_for_unsigned!(u16),
        UInt32 => plan_for_unsigned!(u32),
        UInt64 => plan_for_unsigned!(u64),
        // These fallbacks are now guarded by our debug_assert. We should not hit them
        // if the logical type was Float, as it should have been bit-cast.
        Float32 | Float64 | Boolean => {
            let plan = vec![Operation::Zstd { level: 3 }];
            let compressed =
                executor::execute_linear_encode_pipeline(bytes, context.physical_dtype, &plan)?;
            Ok((plan, compressed.len()))
        }
    }
}

//==================================================================================
// 4. Top-Level Public API (Refactored to return a `Plan` struct)
//==================================================================================

/// Analyzes a byte stream and its type to produce an optimal, self-contained `Plan`.
// In: src/pipeline/planner.rs

pub fn plan_pipeline(bytes: &[u8], context: PlanningContext) -> Result<Plan, PhoenixError> {
    // --- THE AUTHORITATIVE FIX: SPECIALIZED BOOLEAN PLANNING ---
    // The main planner is optimized for numeric data. For boolean streams,
    // the best strategy is almost always RLE followed by an entropy coder.
    // We add a special fast-path here to handle this case correctly.
    if context.physical_dtype == PhoenixDataType::Boolean {
        // --- FIX: Add more candidate pipelines for booleans ---
        let rle_only_plan = vec![Operation::Rle];
        let rle_zstd_plan = vec![Operation::Rle, Operation::Zstd { level: 19 }];
        let rle_ans_plan = vec![Operation::Rle, Operation::Ans];

        // Empirically determine the cost of each candidate.
        let rle_cost = executor::execute_linear_encode_pipeline(
            bytes,
            context.physical_dtype,
            &rle_only_plan,
        )?
        .len();

        let zstd_cost = executor::execute_linear_encode_pipeline(
            bytes,
            context.physical_dtype,
            &rle_zstd_plan,
        )?
        .len();

        let ans_cost =
            executor::execute_linear_encode_pipeline(bytes, context.physical_dtype, &rle_ans_plan)?
                .len();

        // Find the minimum cost among the three candidates.
        let mut best_pipeline = rle_only_plan;
        let mut min_cost = rle_cost;

        if zstd_cost < min_cost {
            min_cost = zstd_cost;
            best_pipeline = rle_zstd_plan;
        }
        if ans_cost < min_cost {
            best_pipeline = rle_ans_plan;
        }

        return Ok(Plan {
            plan_version: PLAN_VERSION,
            initial_type: context.initial_dtype,
            pipeline: best_pipeline,
        });
    }
    // --- END FIX ---

    // If the type is not Boolean, proceed with the original, numeric-focused planning logic.
    let stride = find_stride_by_autocorrelation(bytes, context.physical_dtype)?;
    let (pipeline, _cost) = plan_bytes(bytes, &context.clone(), stride)?;

    Ok(Plan {
        plan_version: PLAN_VERSION,
        initial_type: context.initial_dtype,
        pipeline,
    })
}
