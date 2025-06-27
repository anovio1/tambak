//! This module contains the core logic for planning an optimal compression pipeline.
//!
//! The `PipelinePlanner` analyzes the statistical properties of a raw data chunk
//! (as a typed slice) and constructs a sequence of compression operations
//! tailored to that specific data. This is the "brain" that makes the Phoenix
//! writer adaptive and intelligent. It is aware of the full suite of available
//! compression kernels. This module is PURE RUST and has no FFI dependencies.

use num_traits::{PrimInt, Signed, ToPrimitive};
use serde_json::{json, Value};

use crate::error::PhoenixError;

//==================================================================================
// 1. Data Profile & Heuristics (REVISED - Performant & Robust)
//==================================================================================

/// A struct to hold the statistical profile of a data chunk.
#[derive(Debug, Default)]
struct DataProfile {
    is_signed: bool,
    all_values_same: bool,
    // After delta encoding:
    delta_is_mostly_zero: bool,
    max_zigzag_delta_bit_width: u8,
    // A heuristic to decide if shuffling will be effective.
    // True if deltas are small, meaning high bytes will be mostly zeros or sign extensions.
    shuffle_is_likely_effective: bool,
}

/// Analyzes a slice of data to build its statistical profile in a single,
/// performant pass without intermediate allocations.
///
/// # Returns
/// A `Result` containing the `DataProfile` on success.
fn analyze_data<T>(data: &[T]) -> Result<DataProfile, PhoenixError>
where
    T: PrimInt + Signed + ToPrimitive,
{
    let mut profile = DataProfile::default();
    profile.is_signed = T::min_value().is_negative();

    if data.is_empty() {
        return Ok(profile);
    }

    // Check for constant value (perfect for RLE)
    let first = data[0];
    profile.all_values_same = data.iter().all(|&x| x == first);
    if profile.all_values_same {
        return Ok(profile); // No need for further analysis
    }

    // --- Single-pass analysis using fold to avoid Vec allocation ---
    let (zero_count, max_zigzag_val, delta_count) = data
        .windows(2)
        .map(|w| w[1].wrapping_sub(w[0]))
        .try_fold((0, 0u64, 0), |(zeros, max_zz, count), delta| {
            let new_zeros = if delta.is_zero() { zeros + 1 } else { zeros };

            let bits = std::mem::size_of::<T>() * 8;
            let zigzag_val = (delta.unsigned_shl(1)) ^ (delta.arithmetic_shr(bits - 1))
                .to_unsigned()
                .ok_or_else(|| PhoenixError::UnsupportedType("Failed to convert to unsigned".to_string()))?;
            
            let val_u64 = zigzag_val.to_u64()
                .ok_or_else(|| PhoenixError::UnsupportedType("Failed to convert to u64".to_string()))?;
            
            Ok((new_zeros, max_zz.max(val_u64), count + 1))
        })?;

    if delta_count == 0 {
        return Ok(profile);
    }

    profile.delta_is_mostly_zero = zero_count * 2 > delta_count;
    profile.max_zigzag_delta_bit_width = if max_zigzag_val == 0 { 0 } else { 64 - max_zigzag_val.leading_zeros() as u8 };
    
    let element_size_bits = (std::mem::size_of::<T>() * 8) as u8;
    profile.shuffle_is_likely_effective = profile.max_zigzag_delta_bit_width < (element_size_bits / 2);

    Ok(profile)
}

//==================================================================================
// 2. Pipeline Construction Logic (REVISED - Panic-Free)
//==================================================================================

/// Constructs the optimal pipeline JSON based on a data profile.
/// Returns a Result to propagate potential serialization errors.
fn build_pipeline_from_profile(profile: &DataProfile) -> Result<String, PhoenixError> {
    let mut pipeline: Vec<Value> = Vec::new();

    // --- Layer 1: Value Reduction ---
    // If all values are the same, RLE is the only transform needed before Zstd.
    if profile.all_values_same {
        pipeline.push(json!({"op": "rle", "params": {}}));
    } else {
    
    // Otherwise, delta encoding is almost always beneficial for time-series.
        pipeline.push(json!({"op": "delta", "params": {"order": 1}}));
    // --- Layer 2: Sparsity Exploitation ---
    // If deltas are mostly zero, RLE is the next logical step.
        if profile.delta_is_mostly_zero {
            pipeline.push(json!({"op": "rle", "params": {}}));
        } else {
        // If not RLE, proceed to the bit-width reduction pipeline.
        
        // --- Layer 3: Bit-Width Reduction ---
            if profile.is_signed {
                pipeline.push(json!({"op": "zigzag", "params": {}}));
            }
        // Choose between Bitpacking (for uniformly small values) and LEB128 (for variable values).
        // The signed LEB128 kernel (`sleb128`) could be a distinct choice here if we didn't
        // use Zig-zag first. Since we do, the input to this stage is always unsigned.
            if profile.max_zigzag_delta_bit_width > 0 && profile.max_zigzag_delta_bit_width <= 16 {
                 pipeline.push(json!({
                    "op": "bitpack",
                    "params": {"bit_width": profile.max_zigzag_delta_bit_width}
                }));
            } else {
                pipeline.push(json!({"op": "leb128", "params": {}}));
            }
        // --- Layer 4: Byte Distribution ---
        // If shuffling is likely to help, apply it before the final compression.
        // This is most effective on fixed-width integer types after other transforms.
            if profile.shuffle_is_likely_effective {
                pipeline.push(json!({"op": "shuffle", "params": {}}));
            }
        }
    }

    // --- Final Stage: Entropy Coding ---
    pipeline.push(json!({"op": "zstd", "params": {"level": 3}}));

    serde_json::to_string(&pipeline)
        .map_err(|e| PhoenixError::UnsupportedType(format!("Pipeline JSON serialization failed: {}", e)))
}

//==================================================================================
// 3. Public API for the Pure Rust Core (NEW - Decoupled)
//==================================================================================

/// The PURE RUST entry point for the planner. It's generic and knows nothing about FFI.
/// It takes a typed slice and returns a plan.
pub fn plan_pipeline_for_type<T>(data: &[T]) -> Result<String, PhoenixError>
where
    T: PrimInt + Signed + ToPrimitive,
{
    let profile = analyze_data(data)?;
    build_pipeline_from_profile(&profile)
}

//==================================================================================
// 4. Unit Tests (REVISED - Now test the new public API)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_for_constant_data() {
        let original_data: Vec<i32> = vec![100, 100, 100, 100, 100];
        let pipeline_json = plan_pipeline_for_type(&original_data).unwrap();
        let pipeline: Vec<Value> = serde_json::from_str(&pipeline_json).unwrap();
        assert_eq!(pipeline[0]["op"], "rle");
        assert_eq!(pipeline[1]["op"], "zstd");
    }

    #[test]
    fn test_plan_for_small_deltas_bitpack_and_shuffle() {
        let original_data: Vec<i16> = vec![100, 101, 103, 102, 104, 101];
        let pipeline_json = plan_pipeline_for_type(&original_data).unwrap();
        let pipeline: Vec<Value> = serde_json::from_str(&pipeline_json).unwrap();
        let op_names: Vec<&str> = pipeline.iter().map(|v| v["op"].as_str().unwrap()).collect();
        assert_eq!(op_names, vec!["delta", "zigzag", "bitpack", "shuffle", "zstd"]);
        assert_eq!(pipeline[2]["params"]["bit_width"], 3);
    }

    #[test]
    fn test_plan_for_large_deltas_leb128_no_shuffle() {
        let original_data: Vec<i32> = vec![100, 101, 103, 100000, 100002];
        let pipeline_json = plan_pipeline_for_type(&original_data).unwrap();
        let pipeline: Vec<Value> = serde_json::from_str(&pipeline_json).unwrap();
        let op_names: Vec<&str> = pipeline.iter().map(|v| v["op"].as_str().unwrap()).collect();
        assert_eq!(op_names, vec!["delta", "zigzag", "leb128", "zstd"]);
    }

    #[test]
    fn test_analyze_data_is_performant() {
        // This test doesn't measure performance but ensures the single-pass logic works.
        let data: Vec<i64> = (0..1000).map(|i| i * (i % 5)).collect();
        let result = analyze_data(&data);
        assert!(result.is_ok());
        let profile = result.unwrap();
        assert!(!profile.all_values_same);
    }
}