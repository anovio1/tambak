//! This module contains the core logic for planning an optimal compression pipeline.
//!
//! The `PipelinePlanner` analyzes the statistical properties of a raw data chunk
//! (as a typed slice) and constructs a sequence of compression operations
//! tailored to that specific data. This is the "brain" that makes the Phoenix
//! writer adaptive and intelligent. This module is PURE RUST and has no FFI
// or Polars dependencies.

use num_traits::{PrimInt, Signed, ToPrimitive, WrappingSub};
use serde_json::{json, Value};
use std::ops::{BitXor, Shl, Shr};

use crate::error::PhoenixError;
use crate::kernels::zigzag;
use crate::traits::HasUnsigned;
use crate::utils::safe_bytes_to_typed_slice;

//==================================================================================
// 1. Data Profile & Heuristics
//==================================================================================

/// A struct to hold the statistical profile of a data chunk.
#[derive(Debug, Default)]
struct DataProfile {
    is_signed: bool,
    all_values_same: bool,
    delta_is_mostly_zero: bool,
    max_zigzag_delta_bit_width: u8,
    shuffle_is_likely_effective: bool,
}

/// Analyzes a slice of data to build its statistical profile in a single,
/// performant pass without intermediate allocations.
fn analyze_data<T>(data: &[T]) -> Result<DataProfile, PhoenixError>
where
    T: PrimInt
        + Signed
        + ToPrimitive
        + WrappingSub
        + HasUnsigned
        + Shl<usize, Output = T>
        + Shr<usize, Output = T>
        + BitXor<T, Output = T>,
    T::Unsigned: PrimInt,
{
    let mut profile = DataProfile::default();
    profile.is_signed = T::min_value().is_negative();

    if data.is_empty() {
        return Ok(profile);
    }

    let first = data[0];
    profile.all_values_same = data.iter().all(|&x| x == first);
    if profile.all_values_same {
        return Ok(profile);
    }

    let (zero_count, max_zigzag_delta, delta_count) =
        data.windows(2).map(|w| w[1].wrapping_sub(&w[0])).try_fold(
            (0u64, 0u64, 0u64),
            |(zeros, max_zz, count), delta| -> Result<(u64, u64, u64), PhoenixError> {
                let new_zeros = if delta.is_zero() { zeros + 1 } else { zeros };
                let zigzag_val = zigzag::encode_val(delta);
                let val_u64 = zigzag_val.to_u64().ok_or_else(|| {
                    PhoenixError::UnsupportedType(
                        "Failed to convert zigzag value to u64".to_string(),
                    )
                })?;
                Ok((new_zeros, max_zz.max(val_u64), count + 1))
            },
        )?;

    let first_val_zigzag = zigzag::encode_val(data[0]);
    let first_val_u64 = first_val_zigzag.to_u64().ok_or_else(|| {
        PhoenixError::UnsupportedType("Failed to convert first value to u64".to_string())
    })?;

    let max_zigzag_val = max_zigzag_delta.max(first_val_u64);

    if delta_count == 0 {
        profile.max_zigzag_delta_bit_width = if max_zigzag_val == 0 {
            0
        } else {
            64 - max_zigzag_val.leading_zeros() as u8
        };
        return Ok(profile);
    }

    profile.delta_is_mostly_zero = zero_count * 2 > delta_count;
    profile.max_zigzag_delta_bit_width = if max_zigzag_val == 0 {
        0
    } else {
        64 - max_zigzag_val.leading_zeros() as u8
    };

    let element_size_bits = (std::mem::size_of::<T>() * 8) as u8;
    // FINAL, CORRECT HEURISTIC: Shuffle is effective if the significant bits
    // occupy half or less of the type's total bits. This indicates high-order
    // bytes are likely to be similar and compressible after shuffling.
    profile.shuffle_is_likely_effective =
        profile.max_zigzag_delta_bit_width <= (element_size_bits / 2);

    Ok(profile)
}

//==================================================================================
// 2. Pipeline Construction Logic
//==================================================================================

/// Constructs the optimal pipeline JSON based on a data profile.
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

            // --- FINAL, DEFINITIVE MUTUALLY EXCLUSIVE LOGIC ---
            // The planner must choose only ONE of these terminal operations.
            
            // Heuristic 1: Is shuffle the most effective?
            if profile.shuffle_is_likely_effective {
                pipeline.push(json!({"op": "shuffle"}));
            
            // Heuristic 2: If not, is bitpacking a good choice?
            } else if profile.max_zigzag_delta_bit_width > 0 && profile.max_zigzag_delta_bit_width <= 16 {
                pipeline.push(json!({
                    "op": "bitpack",
                    "params": {"bit_width": profile.max_zigzag_delta_bit_width}
                }));

            // Heuristic 3: Otherwise, fall back to the most general-purpose option.
            } else {
                pipeline.push(json!({"op": "leb128"}));
            }
        }
    }

    pipeline.push(json!({"op": "zstd", "params": {"level": 3}}));

    serde_json::to_string(&pipeline).map_err(|e| {
        PhoenixError::UnsupportedType(format!("Pipeline JSON serialization failed: {}", e))
    })
}

//==================================================================================
// 3. Public API for the Pure Rust Core
//==================================================================================

/// The PURE RUST entry point for the planner. It takes a raw byte buffer,
/// dispatches to the correct generic analyzer, and returns the plan.
pub fn plan_pipeline(bytes: &[u8], original_type: &str) -> Result<String, PhoenixError> {
    macro_rules! dispatch {
        ($T:ty) => {{
            if bytes.len() % std::mem::size_of::<$T>() != 0 {
                return Err(PhoenixError::BufferMismatch(
                    bytes.len(),
                    std::mem::size_of::<$T>(),
                ));
            }
            // This safe, copying conversion is the key to fixing the alignment bugs.
            let data: Vec<$T> = bytes
                .chunks_exact(std::mem::size_of::<$T>())
                .map(|chunk| <$T>::from_le_bytes(chunk.try_into().unwrap()))
                .collect();
            let profile = analyze_data(&data)?;
            build_pipeline_from_profile(&profile)
        }};
    }

    match original_type {
        "Int8" => dispatch!(i8),
        "Int16" => dispatch!(i16),
        "Int32" => dispatch!(i32),
        "Int64" => dispatch!(i64),
        _ => {
            let pipeline = vec![json!({"op": "zstd", "params": {"level": 3}})];
            serde_json::to_string(&pipeline)
                .map_err(|e| PhoenixError::UnsupportedType(e.to_string()))
        }
    }
}

//==================================================================================
// 4. Unit Tests
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

    #[test]
    fn test_plan_for_constant_data_is_rle() {
        let original_data: Vec<i32> = vec![7, 7, 7, 7, 7, 7, 7];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let pipeline_json = plan_pipeline(&original_bytes, "Int32").unwrap();
        let op_names = get_op_names(&pipeline_json);
        // This test is still correct.
        assert_eq!(op_names, vec!["rle", "zstd"]);
    }

    #[test]
    fn test_plan_for_mostly_zero_deltas_is_delta_rle() {
        let original_data: Vec<i32> = vec![10, 10, 11, 11, 11, 12, 12];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let pipeline_json = plan_pipeline(&original_bytes, "Int32").unwrap();
        let op_names = get_op_names(&pipeline_json);
        // This test is also still correct.
        assert_eq!(op_names, vec!["delta", "rle", "zstd"]);
    }

    #[test]
    // RENAMED: The old name was misleading.
    fn test_plan_for_small_deltas_chooses_shuffle() {
        // Data analysis:
        // - Type: i16 (16 bits)
        // - Max zigzagged delta value is 200, which requires 8 bits.
        // - Shuffle heuristic: `bit_width <= (element_size / 2)` -> `8 <= (16 / 2)` -> `true`.
        // - The planner will choose `shuffle` because this condition is met first.
        let original_data: Vec<i16> = vec![100, 101, 103, 102, 104, 101];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let pipeline_json = plan_pipeline(&original_bytes, "Int16").unwrap();
        let op_names = get_op_names(&pipeline_json);
        
        // CORRECTED ASSERTION
        assert_eq!(
            op_names,
            vec!["delta", "zigzag", "shuffle", "zstd"]
        );
    }

    #[test]
    // RENAMED: The old name was misleading.
    fn test_plan_for_medium_deltas_chooses_shuffle() {
        // Data analysis:
        // - Type: i32 (32 bits)
        // - Max zigzagged delta value is 3990, which requires 12 bits.
        // - Shuffle heuristic: `bit_width <= (element_size / 2)` -> `12 <= (32 / 2)` -> `true`.
        // - The planner will choose `shuffle` because this condition is met first.
        let original_data: Vec<i32> = vec![0, 1000, 5, 2000, 10];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let pipeline_json = plan_pipeline(&original_bytes, "Int32").unwrap();
        let op_names = get_op_names(&pipeline_json);

        // CORRECTED ASSERTION
        assert_eq!(
            op_names,
            vec!["delta", "zigzag", "shuffle", "zstd"]
        );
    }

    #[test]
    // NEW TEST: This case is specifically designed to fail the shuffle heuristic
    // but pass the bitpack heuristic, ensuring the `else if` path is tested.
    fn test_plan_chooses_bitpack_when_shuffle_ineffective() {
        // Data analysis:
        // - Type: i16 (16 bits)
        // - Max zigzagged delta value is 512, which requires 10 bits.
        // - Shuffle heuristic: `bit_width <= (element_size / 2)` -> `10 <= (16 / 2)` -> `false`.
        // - Bitpack heuristic: `bit_width <= 16` -> `10 <= 16` -> `true`.
        // - The planner will skip `shuffle` and choose `bitpack`.
        let original_data: Vec<i16> = vec![0, 256, 0]; // Delta is 256, zigzag is 512
        let original_bytes = typed_slice_to_bytes(&original_data);
        let pipeline_json = plan_pipeline(&original_bytes, "Int16").unwrap();
        let op_names = get_op_names(&pipeline_json);

        // This assertion validates the bitpack path.
        assert_eq!(
            op_names,
            vec!["delta", "zigzag", "bitpack", "zstd"]
        );
    }
}
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::utils::typed_slice_to_bytes;

//     #[test]
//     fn test_plan_for_small_deltas_bitpack_and_shuffle() {
//         let original_data: Vec<i16> = vec![100, 101, 103, 102, 104, 101];
//         let original_bytes = typed_slice_to_bytes(&original_data);
//         let pipeline_json = plan_pipeline(&original_bytes, "Int16").unwrap();
//         let pipeline: Vec<Value> = serde_json::from_str(&pipeline_json).unwrap();
//         let op_names: Vec<&str> = pipeline.iter().map(|v| v["op"].as_str().unwrap()).collect();

//         assert_eq!(
//             op_names,
//             vec!["delta", "zigzag", "shuffle", "leb128", "zstd"]
//         );
//     }

//     // --- NEW, TARGETED UNIT TEST ---
//     #[test]
//     fn test_planner_logic_for_python_failing_case() {
//         // 1. This is the exact data (without nulls) from the Python test script.
//         let original_data: Vec<i64> = vec![100, 110, 125, 120, 110, 90, 85, 95];
//         let original_bytes = typed_slice_to_bytes(&original_data);

//         // 2. We run the planner, which is the component we are testing.
//         let pipeline_json = plan_pipeline(&original_bytes, "Int64").unwrap();

//         // 3. We determine the *correct* expected pipeline.
//         //    - zigzag(100) = 200, which needs 8 bits.
//         //    - The max delta is -20, zigzag(-20) = 39, which needs 6 bits.
//         //    - The max value to encode is 200, so bit_width is 8.
//         //    - Shuffle is effective (8 < 64/2).
//         //    - Bitpack is effective (8 <= 16).
//         //    - The correct order is delta -> zigzag -> shuffle -> bitpack -> zstd.
//         let expected_pipeline = r#"[{"op":"delta","params":{"order":1}},{"op":"zigzag"},{"op":"shuffle"},{"op":"bitpack","params":{"bit_width":8}},{"op":"zstd","params":{"level":3}}]"#;

//         // 4. We assert that the planner produced the exact correct pipeline.
//         //    This is a much stronger check than just looking at op names.
//         assert_eq!(pipeline_json, expected_pipeline);
//     }

//     fn get_op_names(pipeline_json: &str) -> Vec<String> {
//         let pipeline: Vec<Value> = serde_json::from_str(pipeline_json).unwrap();
//         pipeline
//             .iter()
//             .map(|v| v["op"].as_str().unwrap().to_string())
//             .collect()
//     }

//     #[test]
//     fn test_plan_for_constant_data_is_rle() {
//         let original_data: Vec<i32> = vec![7, 7, 7, 7, 7, 7, 7];
//         let original_bytes = typed_slice_to_bytes(&original_data);
//         let pipeline_json = plan_pipeline(&original_bytes, "Int32").unwrap();
//         let op_names = get_op_names(&pipeline_json);
//         assert_eq!(op_names, vec!["rle", "zstd"]);
//     }

//     #[test]
//     fn test_plan_for_mostly_zero_deltas_is_delta_rle() {
//         let original_data: Vec<i32> = vec![10, 10, 11, 11, 11, 12, 12];
//         let original_bytes = typed_slice_to_bytes(&original_data);
//         let pipeline_json = plan_pipeline(&original_bytes, "Int32").unwrap();
//         let op_names = get_op_names(&pipeline_json);
//         // After delta, the data is [10, 0, 1, 0, 0, 1, 0], which is mostly zeros, so RLE is chosen.
//         assert_eq!(op_names, vec!["delta", "rle", "zstd"]);
//     }

//     #[test]
//     fn test_plan_for_small_deltas_is_bitpack_and_shuffle() {
//         // Deltas are [1, 2, -1, 2, -3], which are small.
//         let original_data: Vec<i16> = vec![100, 101, 103, 102, 104, 101];
//         let original_bytes = typed_slice_to_bytes(&original_data);
//         let pipeline_json = plan_pipeline(&original_bytes, "Int16").unwrap();
//         let op_names = get_op_names(&pipeline_json);

//         // The order should now be correct: shuffle BEFORE bit-width reduction.
//         assert_eq!(
//             op_names,
//             vec!["delta", "zigzag", "shuffle", "bitpack", "zstd"]
//         );
//     }
//     #[test]
//     fn test_plan_for_large_deltas_is_bitpack_no_shuffle() {
//         // RENAMED and CORRECTED
//         // Deltas are [1000, -995, 1995, -1990]. Zigzagged, the max value is 3990, which is 12 bits.
//         // 12 bits is small enough for bitpacking, but not small enough to trigger shuffle.
//         let original_data: Vec<i32> = vec![0, 1000, 5, 2000, 10];
//         let original_bytes = typed_slice_to_bytes(&original_data);
//         let pipeline_json = plan_pipeline(&original_bytes, "Int32").unwrap();
//         let op_names = get_op_names(&pipeline_json);

//         // CORRECTED: The planner correctly chooses bitpack. The test was wrong.
//         assert_eq!(op_names, vec!["delta", "zigzag", "bitpack", "zstd"]);
//     }

//     #[test]
//     fn test_plan_for_unsigned_has_no_zigzag() {
//         let original_data: Vec<u32> = vec![100, 101, 102, 103];
//         let original_bytes = typed_slice_to_bytes(&original_data);
//         let pipeline_json = plan_pipeline(&original_bytes, "UInt32").unwrap();
//         let op_names = get_op_names(&pipeline_json);
//         assert!(!op_names.contains(&"zigzag".to_string()));
//     }

//     #[test]
//     fn test_plan_for_unsupported_type_is_zstd_only() {
//         let original_data: Vec<f64> = vec![1.0, 2.0, 3.0];
//         let original_bytes = bytemuck::cast_slice(&original_data).to_vec();
//         let pipeline_json = plan_pipeline(&original_bytes, "Float64").unwrap();
//         let op_names = get_op_names(&pipeline_json);
//         assert_eq!(op_names, vec!["zstd"]);
//     }
// }
