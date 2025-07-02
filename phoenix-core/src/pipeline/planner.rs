//! This module contains the v4.3 "Structure & Conquer" planner.
//!
//! It analyzes data to discover its structure, generates candidate pipelines using
//! strongly-typed `Operation` objects, and uses empirical trial compression to
//! find the most effective plan. Its final output is a complete, type-safe
//! `Plan` struct, ready for the Executor.

use crate::error::PhoenixError;
use crate::kernels::zigzag;
use crate::pipeline::models::{Operation, Plan};
use crate::pipeline::traits::TypeTransformer;
use crate::pipeline::{self, executor};
use crate::types::PhoenixDataType;
use crate::utils::safe_bytes_to_typed_slice;
use ndarray::{s, Array1};
use num_traits::{PrimInt, Signed, ToPrimitive, Unsigned, WrappingSub};
use std::collections::HashSet;
use std::ops::{BitXor, Shl, Shr};

// A const for the plan version, ensuring consistency.
const PLAN_VERSION: u32 = 1;

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
// 2. Data Profile & Analysis (Internal helpers)
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

//==================================================================================
// 3. Core Planning Logic (Refactored for Strong Types)
//==================================================================================

/// Generates candidate pipelines as `Vec<Operation>` instead of JSON.
fn generate_candidate_pipelines(
    profile: &DataProfile,
    stride: usize,
    is_signed: bool,
) -> Vec<Vec<Operation>> {
    let mut base_pipelines: Vec<Vec<Operation>> = Vec::new();
    base_pipelines.push(vec![Operation::Shuffle]);

    let delta_op = if is_signed {
        Operation::Delta { order: stride }
    } else {
        Operation::XorDelta
    };

    if profile.delta_stream_has_low_cardinality {
        base_pipelines.push(vec![
            delta_op.clone(),
            if is_signed {
                Operation::Dictionary
            } else {
                Operation::Rle
            },
        ]);
    }

    if is_signed {
        if profile.signed_delta_bit_width > 0 {
            let base_path = vec![Operation::Delta { order: stride }, Operation::ZigZag];
            if profile.signed_delta_bit_width < profile.original_bit_width {
                let mut bitpack_path = base_path.clone();
                bitpack_path.push(Operation::BitPack {
                    bit_width: profile.signed_delta_bit_width,
                });
                base_pipelines.push(bitpack_path);
            }
            base_pipelines.push({
                let mut leb_path = base_path;
                leb_path.push(Operation::Leb128);
                leb_path
            });
        }
    } else if profile.unsigned_delta_bit_width > 0
        && profile.unsigned_delta_bit_width < profile.original_bit_width
    {
        base_pipelines.push(vec![
            Operation::XorDelta,
            Operation::BitPack {
                bit_width: profile.unsigned_delta_bit_width,
            },
        ]);
    }

    let mut final_candidates = Vec::with_capacity(base_pipelines.len() * 2);
    for pipeline in base_pipelines {
        let mut zstd_variant = pipeline.clone();
        zstd_variant.push(Operation::Zstd { level: 3 });
        final_candidates.push(zstd_variant);

        let mut ans_variant = pipeline;
        ans_variant.push(Operation::Ans);
        final_candidates.push(ans_variant);
    }

    final_candidates
}

/// Finds the best pipeline by trial, now operating on `Vec<Operation>`.
fn find_best_pipeline_by_trial(
    sample_data_bytes: &[u8],
    dtype: PhoenixDataType,
    candidates: Vec<Vec<Operation>>,
) -> Result<(Vec<Operation>, usize), PhoenixError> {
    // The default plan is now also a strongly-typed Vec<Operation>.
    let default_plan = vec![Operation::Zstd { level: 3 }];

    if candidates.is_empty() {
        // NOTE: This assumes a refactored executor that we will build next.
        // The new executor will not return the final type string.
        let compressed =
            executor::execute_linear_encode_pipeline(sample_data_bytes, dtype, &default_plan)?;
        return Ok((default_plan, compressed.len()));
    }

    let mut best_pipeline = default_plan;
    let mut min_size = usize::MAX;

    for pipeline in candidates {
        // NOTE: This call anticipates the refactored executor.
        if let Ok(compressed_sample) =
            executor::execute_linear_encode_pipeline(sample_data_bytes, dtype, &pipeline)
        {
            if compressed_sample.len() < min_size {
                min_size = compressed_sample.len();
                best_pipeline = pipeline;
            }
        }
    }

    Ok((best_pipeline, min_size))
}

/// The internal planning function, now returns a `Vec<Operation>` and its cost.
fn plan_bytes(
    bytes: &[u8],
    dtype: PhoenixDataType,
    stride: usize,
) -> Result<(Vec<Operation>, usize), PhoenixError> {
    if bytes.is_empty() {
        return Ok((vec![], 0));
    }

    const SAMPLE_SIZE_BYTES: usize = 65536;
    let sample_bytes = &bytes[..bytes.len().min(SAMPLE_SIZE_BYTES)];

    macro_rules! plan_for_signed {
        ($T:ty) => {{
            let slice = safe_bytes_to_typed_slice::<$T>(bytes)?;
            let profile = analyze_signed_data(slice, stride);

            if profile.is_constant {
                let plan = vec![Operation::Rle, Operation::Zstd { level: 3 }];
                let compressed =
                    executor::execute_linear_encode_pipeline(sample_bytes, dtype, &plan)?;
                Ok((plan, compressed.len()))
            } else {
                let candidates = generate_candidate_pipelines(&profile, stride, true);
                find_best_pipeline_by_trial(sample_bytes, dtype, candidates)
            }
        }};
    }

    macro_rules! plan_for_unsigned {
        ($T:ty) => {{
            let slice = safe_bytes_to_typed_slice::<$T>(bytes)?;
            let profile = analyze_unsigned_data(slice, stride);

            if profile.is_constant {
                let plan = vec![Operation::Rle, Operation::Zstd { level: 3 }];
                let compressed =
                    executor::execute_linear_encode_pipeline(sample_bytes, dtype, &plan)?;
                Ok((plan, compressed.len()))
            } else {
                let candidates = generate_candidate_pipelines(&profile, stride, false);
                find_best_pipeline_by_trial(sample_bytes, dtype, candidates)
            }
        }};
    }

    use PhoenixDataType::*;
    match dtype {
        Int8 => plan_for_signed!(i8),
        Int16 => plan_for_signed!(i16),
        Int32 => plan_for_signed!(i32),
        Int64 => plan_for_signed!(i64),
        UInt8 => plan_for_unsigned!(u8),
        UInt16 => plan_for_unsigned!(u16),
        UInt32 => plan_for_unsigned!(u32),
        UInt64 => plan_for_unsigned!(u64),
        _ => {
            // Fallback for floats, booleans, etc.
            let plan = vec![Operation::Zstd { level: 3 }];
            let compressed = executor::execute_linear_encode_pipeline(bytes, dtype, &plan)?;
            Ok((plan, compressed.len()))
        }
    }
}

//==================================================================================
// 4. Top-Level Public API (Refactored to return a `Plan` struct)
//==================================================================================

/// Analyzes a byte stream and its type to produce an optimal, self-contained `Plan`.
pub fn plan_pipeline(bytes: &[u8], dtype: PhoenixDataType) -> Result<Plan, PhoenixError> {
    let stride = find_stride_by_autocorrelation(bytes, dtype)?;
    let (pipeline, _cost) = plan_bytes(bytes, dtype, stride)?;

    Ok(Plan {
        plan_version: PLAN_VERSION,
        initial_type: dtype,
        pipeline,
    })
}