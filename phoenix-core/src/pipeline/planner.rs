//! This module contains the core logic for planning an optimal compression pipeline.
//!
//! The planner is a pure, stateless function that performs a rapid, single-pass
//! statistical analysis of a data chunk to create a data-driven compression plan.
//! It correctly dispatches between integer and float-bit analysis to select the
//! most effective pipeline.

use serde_json::{json, Value};
use num_traits::{AsPrimitive, PrimInt, Signed, ToPrimitive, WrappingSub};
use std::ops::{BitXor, Shl, Shr};

use crate::error::PhoenixError;
use crate::kernels::zigzag;
use crate::traits::HasUnsigned;
use crate::utils::safe_bytes_to_typed_slice;

//==================================================================================
// 1. Data Profile & Heuristics (FINAL, CORRECTED VERSION)
//==================================================================================

#[derive(Debug, Default)]
struct DataProfile {
    all_values_same: bool,
    // For Signed Integers
    delta_is_mostly_zero: bool,
    max_overall_bit_width: u8,
    // For Unsigned Integers (Float Bits)
    avg_xor_delta_leading_zeros: f32,
}

/// Safely calculates the minimum number of bits required to represent a value.
fn bit_width(n: u64) -> u8 {
    if n == 0 { 0 } else { 64 - n.leading_zeros() as u8 }
}

/// Analyzes a slice of SIGNED integer data.
fn analyze_signed_data<T>(data: &[T]) -> DataProfile
where
    T: PrimInt + Signed + ToPrimitive + WrappingSub + HasUnsigned + Shl<usize, Output = T> + Shr<usize, Output = T> + BitXor<T, Output = T>,
    <T as HasUnsigned>::Unsigned: PrimInt + ToPrimitive,
{
    let mut profile = DataProfile::default();
    if data.is_empty() { return profile; }

    let first = data[0];
    profile.all_values_same = data.iter().all(|&x| x == first);
    if profile.all_values_same { return profile; }

    let mut max_sub_delta_zz: u64 = 0;
    let mut sub_delta_zero_count: u64 = 0;
    let mut prev = first;

    for &curr in &data[1..] {
        let sub_delta = curr.wrapping_sub(&prev);
        if sub_delta.is_zero() { sub_delta_zero_count += 1; }
        let sub_delta_zz = zigzag::encode_val(sub_delta);
        max_sub_delta_zz = max_sub_delta_zz.max(sub_delta_zz.to_u64().unwrap_or(0));
        prev = curr;
    }
    
    let first_zz = zigzag::encode_val(first);
    max_sub_delta_zz = max_sub_delta_zz.max(first_zz.to_u64().unwrap_or(0));
    profile.max_overall_bit_width = bit_width(max_sub_delta_zz);
    profile.delta_is_mostly_zero = sub_delta_zero_count * 2 > data.len() as u64;
    profile
}

/// Analyzes a slice of UNSIGNED integer data (typically from float bits).
fn analyze_unsigned_data<T>(data: &[T]) -> DataProfile
where
    T: PrimInt + ToPrimitive + BitXor<Output = T>,
{
    let mut profile = DataProfile::default();
    if data.is_empty() { return profile; }

    let first = data[0];
    profile.all_values_same = data.iter().all(|&x| x == first);
    if profile.all_values_same { return profile; }

    let mut total_leading_zeros: u64 = 0;
    let mut delta_count: u64 = 0;
    let mut max_overall_val: u64 = first.to_u64().unwrap_or(0);
    let mut prev = first;

    for &curr in &data[1..] {
        // --- THIS IS THE CRITICAL FIX ---
        // Perform XOR on the native type T, THEN get leading zeros.
        // This avoids the incorrect up-casting to u64 which inflated the zero count.
        let xor_delta: T = prev ^ curr;
        total_leading_zeros += xor_delta.leading_zeros() as u64;
        // --- END FIX ---

        max_overall_val = max_overall_val.max(xor_delta.to_u64().unwrap_or(0));
        delta_count += 1;
        prev = curr;
    }
    
    max_overall_val = max_overall_val.max(first.to_u64().unwrap_or(0));
    profile.max_overall_bit_width = bit_width(max_overall_val);
    if delta_count > 0 {
        profile.avg_xor_delta_leading_zeros = total_leading_zeros as f32 / delta_count as f32;
    }
    profile
}

//==================================================================================
// 2. Pipeline Construction Logic (Unchanged, but now receives correct profile)
//==================================================================================

/// Builds the pipeline for signed integer data based on its profile.
fn build_signed_pipeline(profile: DataProfile) -> Vec<Value> {
    let mut pipeline: Vec<Value> = Vec::new();
    if profile.all_values_same {
        pipeline.push(json!({"op": "rle"}));
        return pipeline;
    }
    pipeline.push(json!({"op": "delta", "params": {"order": 1}}));
    if profile.delta_is_mostly_zero {
        pipeline.push(json!({"op": "rle"}));
    } else {
        pipeline.push(json!({"op": "zigzag"}));
        if profile.max_overall_bit_width > 16 {
            pipeline.push(json!({"op": "leb128"}));
        } else if profile.max_overall_bit_width > 0 {
            pipeline.push(json!({
                "op": "bitpack",
                "params": {"bit_width": profile.max_overall_bit_width}
            }));
        }
    }
    pipeline
}

/// Builds the pipeline for unsigned integer data (float bits) based on its profile.
fn build_unsigned_pipeline(profile: DataProfile, type_str: &str) -> Vec<Value> {
    let mut pipeline: Vec<Value> = Vec::new();
    if profile.all_values_same {
        pipeline.push(json!({"op": "rle"}));
        return pipeline;
    }
    pipeline.push(json!({"op": "xor_delta"}));
    let element_size_bits = if type_str == "u32" { 32.0 } else { 64.0 };

    if profile.avg_xor_delta_leading_zeros > (element_size_bits / 4.0) {
        pipeline.push(json!({"op": "shuffle"}));
    } else if profile.max_overall_bit_width > 0 {
        pipeline.push(json!({
            "op": "bitpack",
            "params": {"bit_width": profile.max_overall_bit_width}
        }));
    }
    pipeline
}

//==================================================================================
// 3. Public API for the Pure Rust Core (Unchanged)
//==================================================================================
pub fn plan_pipeline(bytes: &[u8], type_str: &str) -> Result<String, PhoenixError> {
    macro_rules! dispatch {
        ($T:ty, signed) => {{
            let data = safe_bytes_to_typed_slice::<$T>(bytes)?;
            let profile = analyze_signed_data(data);
            build_signed_pipeline(profile)
        }};
        ($T:ty, unsigned) => {{
            let data = safe_bytes_to_typed_slice::<$T>(bytes)?;
            let profile = analyze_unsigned_data(data);
            build_unsigned_pipeline(profile, stringify!($T))
        }};
    }

    let mut pipeline = match type_str {
        "Int8" => dispatch!(i8, signed),
        "Int16" => dispatch!(i16, signed),
        "Int32" => dispatch!(i32, signed),
        "Int64" => dispatch!(i64, signed),
        "UInt32" => dispatch!(u32, unsigned),
        "UInt64" => dispatch!(u64, unsigned),
        _ => vec![json!({"op": "zstd", "params": {"level": 3}})],
    };

    if !pipeline.is_empty() && !pipeline.iter().any(|op| op["op"] == "zstd") {
        pipeline.push(json!({"op": "zstd", "params": {"level": 3}}));
    }

    serde_json::to_string(&pipeline)
        .map_err(|e| PhoenixError::InternalError(format!("Pipeline JSON serialization failed: {}", e)))
}

//==================================================================================
// 4. Unit Tests (The tests are correct and will now pass)
//==================================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::typed_slice_to_bytes;

    fn get_op_names(pipeline_json: &str) -> Vec<String> {
        let pipeline: Vec<Value> = serde_json::from_str(pipeline_json).unwrap();
        pipeline
            .iter()
            .map(|v| v["op"].as_str().unwrap().to_string())
            .collect()
    }

    // --- Integer Path Tests ---
    #[test]
    fn test_signed_constant_is_rle() {
        let data: Vec<i32> = vec![7, 7, 7, 7, 7];
        let bytes = typed_slice_to_bytes(&data);
        let plan = plan_pipeline(&bytes, "Int32").unwrap();
        assert_eq!(get_op_names(&plan), vec!["rle", "zstd"]);
    }

    #[test]
    fn test_signed_mostly_zero_deltas_is_delta_rle() {
        let data: Vec<i32> = vec![10, 10, 11, 11, 11, 12, 12];
        let bytes = typed_slice_to_bytes(&data);
        let plan = plan_pipeline(&bytes, "Int32").unwrap();
        assert_eq!(get_op_names(&plan), vec!["delta", "rle", "zstd"]);
    }

    #[test]
    fn test_signed_small_deltas_is_bitpack() {
        let data: Vec<i16> = vec![100, 101, 103, 102, 104, 101];
        let bytes = typed_slice_to_bytes(&data);
        let plan = plan_pipeline(&bytes, "Int16").unwrap();
        assert_eq!(
            get_op_names(&plan),
            vec!["delta", "zigzag", "bitpack", "zstd"]
        );
    }

    #[test]
    fn test_signed_large_deltas_is_leb128() {
        let data: Vec<i32> = vec![0, 65537];
        let bytes = typed_slice_to_bytes(&data);
        let plan = plan_pipeline(&bytes, "Int32").unwrap();
        assert_eq!(
            get_op_names(&plan),
            vec!["delta", "zigzag", "leb128", "zstd"]
        );
    }

    // --- Unsigned / Float Path Tests ---
    #[test]
    fn test_unsigned_constant_is_rle() {
        let data: Vec<u32> = vec![12345, 12345, 12345];
        let bytes = typed_slice_to_bytes(&data);
        let plan = plan_pipeline(&bytes, "UInt32").unwrap();
        assert_eq!(get_op_names(&plan), vec!["rle", "zstd"]);
    }

    #[test]
    fn test_unsigned_small_xor_deltas_is_shuffle() {
        let data: Vec<u64> = vec![
            f64::to_bits(100.0),
            f64::to_bits(100.0000000000001),
            f64::to_bits(100.0000000000002),
        ];
        let bytes = typed_slice_to_bytes(&data);
        let plan = plan_pipeline(&bytes, "UInt64").unwrap();
        // Add debug printing to see the values
        let profile = analyze_unsigned_data(&data);
        println!("[DEBUG TEST] Profile for small_xor_deltas: {:?}", profile);
        assert_eq!(get_op_names(&plan), vec!["xor_delta", "shuffle", "zstd"]);
    }

    #[test]
    fn test_unsigned_large_xor_deltas_is_bitpack() {
        let data: Vec<u32> = vec![
            f32::to_bits(1.0),
            f32::to_bits(-200.0),
            f32::to_bits(50.5),
        ];
        let bytes = typed_slice_to_bytes(&data);
        let plan = plan_pipeline(&bytes, "UInt32").unwrap();
        // Add debug printing to see the values
        let profile = analyze_unsigned_data(&data);
        println!("[DEBUG TEST] Profile for large_xor_deltas: {:?}", profile);
        assert_eq!(get_op_names(&plan), vec!["xor_delta", "bitpack", "zstd"]);
    }
}