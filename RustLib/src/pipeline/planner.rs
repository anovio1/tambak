//! This module contains the core logic for planning an optimal compression pipeline.
//!
//! The `PipelinePlanner` analyzes the statistical properties of a raw data chunk
//! (as a typed slice) and constructs a sequence of compression operations
//! tailored to that specific data. This is the "brain" that makes the Phoenix
//! writer adaptive and intelligent. This module is PURE RUST and has no FFI
//! or Polars dependencies.

use num_traits::{PrimInt, Signed, ToPrimitive};
use serde_json::{json, Value};

use crate::error::PhoenixError;
use crate::utils::safe_bytes_to_typed_slice;

//==================================================================================
// 1. Data Profile & Heuristics (Performant & Robust)
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
    shuffle_is_likely_effective: bool,
}

/// Analyzes a slice of data to build its statistical profile in a single,
/// performant pass without intermediate allocations.
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
        return Ok(profile);
    }

    // --- Single-pass analysis using fold to avoid Vec allocation ---
    let (zero_count, max_zigzag_val, delta_count) = data
        .windows(2)
        .map(|w| w[1].wrapping_sub(w[0]))
        .try_fold((0, 0u64, 0), |(zeros, max_zz, count), delta| {
            let new_zeros = if delta.is_zero() { zeros + 1 } else { zeros };

            let bits = std::mem::size_of::<T>() * 8;
            let zigzag_val = (delta.to_unsigned()
                .ok_or_else(|| PhoenixError::UnsupportedType("Failed to convert delta to unsigned".to_string()))? << 1)
                ^ (delta.arithmetic_shr(bits - 1))
                .to_unsigned()
                .ok_or_else(|| PhoenixError::UnsupportedType("Failed to convert sign extension to unsigned".to_string()))?;
            
            let val_u64 = zigzag_val.to_u64()
                .ok_or_else(|| PhoenixError::UnsupportedType("Failed to convert zigzag value to u64".to_string()))?;
            
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
// 2. Pipeline Construction Logic (Panic-Free)
//==================================================================================

/// Constructs the optimal pipeline JSON based on a data profile.
/// Returns a Result to propagate potential serialization errors.
fn build_pipeline_from_profile(profile: &DataProfile) -> Result<String, PhoenixError> {
    let mut pipeline: Vec<Value> = Vec::new();

    if profile.all_values_same {
        pipeline.push(json!({"op": "rle"}));
    } else {
        pipeline.push(json!({"op": "delta", "params": {"order": 1}}));
        if profile.delta_is_mostly_zero {
            pipeline.push(json!({"op": "rle"}));
        } else {
            if profile.is_signed {
                pipeline.push(json!({"op": "zigzag"}));
            }
            if profile.max_zigzag_delta_bit_width > 0 && profile.max_zigzag_delta_bit_width <= 16 {
                 pipeline.push(json!({
                    "op": "bitpack",
                    "params": {"bit_width": profile.max_zigzag_delta_bit_width}
                }));
            } else {
                pipeline.push(json!({"op": "leb128"}));
            }
            if profile.shuffle_is_likely_effective {
                pipeline.push(json!({"op": "shuffle"}));
            }
        }
    }

    pipeline.push(json!({"op": "zstd", "params": {"level": 3}}));

    serde_json::to_string(&pipeline)
        .map_err(|e| PhoenixError::UnsupportedType(format!("Pipeline JSON serialization failed: {}", e)))
}

//==================================================================================
// 3. Public API for the Pure Rust Core (Decoupled)
//==================================================================================

/// The PURE RUST entry point for the planner. It takes a raw byte buffer,
/// dispatches to the correct generic analyzer, and returns the plan.
pub fn plan_pipeline(bytes: &[u8], original_type: &str) -> Result<String, PhoenixError> {
    macro_rules! dispatch {
        ($T:ty) => {
            {
                let data = safe_bytes_to_typed_slice::<$T>(bytes)?;
                let profile = analyze_data(data)?;
                build_pipeline_from_profile(&profile)
            }
        };
    }

    match original_type {
        "Int8" => dispatch!(i8),
        "Int16" => dispatch!(i16),
        "Int32" => dispatch!(i32),
        "Int64" => dispatch!(i64),
        // Unsigned types don't need delta/zigzag, so they have a simpler path.
        // This is a placeholder for a more nuanced unsigned planner.
        "UInt8" | "UInt16" | "UInt32" | "UInt64" | "Boolean" => {
            let pipeline = vec![json!({"op": "rle"}), json!({"op": "zstd", "params": {"level": 3}})];
            serde_json::to_string(&pipeline).map_err(|e| PhoenixError::UnsupportedType(e.to_string()))
        }
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string())),
    }
}

//==================================================================================
// 4. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::typed_slice_to_bytes;

    #[test]
    fn test_plan_for_constant_data() {
        let original_data: Vec<i32> = vec![100, 100, 100, 100, 100];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let pipeline_json = plan_pipeline(&original_bytes, "Int32").unwrap();
        let pipeline: Vec<Value> = serde_json::from_str(&pipeline_json).unwrap();
        assert_eq!(pipeline[0]["op"], "rle");
        assert_eq!(pipeline[1]["op"], "zstd");
    }

    #[test]
    fn test_plan_for_small_deltas_bitpack_and_shuffle() {
        let original_data: Vec<i16> = vec![100, 101, 103, 102, 104, 101];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let pipeline_json = plan_pipeline(&original_bytes, "Int16").unwrap();
        let pipeline: Vec<Value> = serde_json::from_str(&pipeline_json).unwrap();
        let op_names: Vec<&str> = pipeline.iter().map(|v| v["op"].as_str().unwrap()).collect();
        assert_eq!(op_names, vec!["delta", "zigzag", "bitpack", "shuffle", "zstd"]);
        assert_eq!(pipeline[2]["params"]["bit_width"], 3);
    }

    #[test]
    fn test_plan_for_large_deltas_leb128_no_shuffle() {
        let original_data: Vec<i32> = vec![100, 101, 103, 100000, 100002];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let pipeline_json = plan_pipeline(&original_bytes, "Int32").unwrap();
        let pipeline: Vec<Value> = serde_json::from_str(&pipeline_json).unwrap();
        let op_names: Vec<&str> = pipeline.iter().map(|v| v["op"].as_str().unwrap()).collect();
        assert_eq!(op_names, vec!["delta", "zigzag", "leb128", "zstd"]);
    }
}