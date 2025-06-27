//! This module contains the core logic for planning an optimal compression pipeline.
//!
//! The `PipelinePlanner` analyzes the statistical properties of a raw data chunk
//! (as a typed slice) and constructs a sequence of compression operations
//! tailored to that specific data. This is the "brain" that makes the Phoenix
//! writer adaptive and intelligent. It is aware of the full suite of available
//! compression kernels.

use num_traits::{PrimInt, Signed};
use serde_json::{json, Value};

use crate::error::PhoenixError;
use crate::utils::bytes_to_typed_slice;

//==================================================================================
// 1. Data Profile & Heuristics
//==================================================================================

/// A struct to hold the statistical profile of a data chunk.
#[derive(Debug, Default)]
struct DataProfile<T: PrimInt> {
    is_signed: bool,
    all_values_same: bool,
    // After delta encoding:
    delta_is_mostly_zero: bool,
    max_zigzag_delta_bit_width: u8,
    // A heuristic to decide if shuffling will be effective.
    // True if deltas are small, meaning high bytes will be mostly zeros or sign extensions.
    shuffle_is_likely_effective: bool,
    _phantom: std::marker::PhantomData<T>,
}

/// Analyzes a slice of data to build its statistical profile.
fn analyze_data<T>(data: &[T]) -> DataProfile<T>
where
    T: PrimInt + Signed,
{
    let mut profile = DataProfile::<T>::default();
    profile.is_signed = T::min_value().is_negative();

    if data.is_empty() {
        return profile;
    }

    // Check for constant value (perfect for RLE)
    let first = data[0];
    profile.all_values_same = data.iter().all(|&x| x == first);
    if profile.all_values_same {
        return profile;
    }

    // Analyze deltas
    let deltas: Vec<T> = data.windows(2).map(|w| w[1].wrapping_sub(w[0])).collect();
    if deltas.is_empty() {
        return profile;
    }
    
    let zero = T::zero();
    let zero_count = deltas.iter().filter(|&&d| d == zero).count();
    profile.delta_is_mostly_zero = zero_count * 2 > deltas.len();

    // Find max bit width needed for zig-zagged deltas
    let mut max_zigzag_val: u64 = 0;
    for delta in deltas {
        let bits = std::mem::size_of::<T>() * 8;
        let zigzag_val = (delta.unsigned_shl(1)) ^ (delta.arithmetic_shr(bits - 1)).to_unsigned().unwrap();
        let val_u64 = zigzag_val.to_u64().unwrap();
        if val_u64 > max_zigzag_val {
            max_zigzag_val = val_u64;
        }
    }
    
    profile.max_zigzag_delta_bit_width = if max_zigzag_val == 0 { 0 } else { 64 - max_zigzag_val.leading_zeros() as u8 };

    // Heuristic for shuffling: if most values fit in less than half the type's bytes,
    // shuffling will likely create highly compressible high-byte planes.
    let element_size_bits = (std::mem::size_of::<T>() * 8) as u8;
    profile.shuffle_is_likely_effective = profile.max_zigzag_delta_bit_width < (element_size_bits / 2);

    profile
}

//==================================================================================
// 2. Pipeline Construction Logic
//==================================================================================

/// Constructs the optimal pipeline JSON based on a data profile.
fn build_pipeline_from_profile<T: PrimInt>(profile: &DataProfile<T>) -> String {
    let mut pipeline: Vec<Value> = Vec::new();

    // --- Layer 1: Value Reduction ---
    // If all values are the same, RLE is the only transform needed before Zstd.
    if profile.all_values_same {
        pipeline.push(json!({"op": "rle", "params": {}}));
        pipeline.push(json!({"op": "zstd", "params": {"level": 3}}));
        return serde_json::to_string(&pipeline).unwrap();
    }
    
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

    // --- Final Stage: Entropy Coding ---
    pipeline.push(json!({
        "op": "zstd",
        "params": {"level": 3}
    }));

    serde_json::to_string(&pipeline).unwrap()
}

//==================================================================================
// 3. FFI Dispatcher Logic
//==================================================================================

/// The public-facing function for this module. It takes a raw byte buffer,
/// analyzes it, and returns the optimal pipeline configuration as a JSON string.
/// This is the primary contract for this module.
pub fn plan_pipeline(bytes: &[u8], original_type: &str) -> PyResult<String> {
    // This dispatcher only needs to handle types that can be delta-encoded.
    // Unsigned types could have a different planning path if needed.
    match original_type {
        "Int8" => {
            let data = unsafe { bytes_to_typed_slice::<i8>(bytes)? };
            let profile = analyze_data(data);
            Ok(build_pipeline_from_profile(&profile))
        }
        "Int16" => {
            let data = unsafe { bytes_to_typed_slice::<i16>(bytes)? };
            let profile = analyze_data(data);
            Ok(build_pipeline_from_profile(&profile))
        }
        "Int32" => {
            let data = unsafe { bytes_to_typed_slice::<i32>(bytes)? };
            let profile = analyze_data(data);
            Ok(build_pipeline_from_profile(&profile))
        }
        "Int64" => {
            let data = unsafe { bytes_to_typed_slice::<i64>(bytes)? };
            let profile = analyze_data(data);
            Ok(build_pipeline_from_profile(&profile))
        }
        _ => {
            // Default plan for unsupported or non-numeric types: just Zstd.
            let pipeline = vec![json!({"op": "zstd", "params": {"level": 3}})];
            Ok(serde_json::to_string(&pipeline).unwrap())
        }
    }
}

//==================================================================================
// 4. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_for_shuffle_and_bitpack() {
        // Deltas are small, signed. Zigzag makes them small unsigned.
        // Max bit width is low, so shuffle should be effective.
        let original_data: Vec<i32> = vec![1000, 1001, 1003, 1002, 1004, 1001];
        let original_bytes = crate::utils::typed_slice_to_bytes(&original_data);
        
        let pipeline_json = plan_pipeline(&original_bytes, "Int32").unwrap();
        let pipeline: Vec<Value> = serde_json::from_str(&pipeline_json).unwrap();

        assert_eq!(pipeline[0]["op"], "delta");
        assert_eq!(pipeline[1]["op"], "zigzag");
        assert_eq!(pipeline[2]["op"], "bitpack");
        assert_eq!(pipeline[3]["op"], "shuffle"); // Shuffle is chosen
        assert_eq!(pipeline[4]["op"], "zstd");
    }

    #[test]
    fn test_plan_for_no_shuffle_with_leb128() {
        // Contains a large delta, making max_bit_width large, so shuffle is not chosen.
        let original_data: Vec<i32> = vec![100, 101, 103, 100000, 100002];
        let original_bytes = crate::utils::typed_slice_to_bytes(&original_data);

        let pipeline_json = plan_pipeline(&original_bytes, "Int32").unwrap();
        let pipeline: Vec<Value> = serde_json::from_str(&pipeline_json).unwrap();

        let op_names: Vec<&str> = pipeline.iter().map(|v| v["op"].as_str().unwrap()).collect();
        
        assert_eq!(op_names, vec!["delta", "zigzag", "leb128", "zstd"]);
        // Note the absence of "shuffle"
    }

    #[test]
    fn test_plan_for_constant_data() {
        let original_data: Vec<i32> = vec![100, 100, 100, 100, 100];
        let original_bytes = crate::utils::typed_slice_to_bytes(&original_data);
        
        let pipeline_json = plan_pipeline(&original_bytes, "Int32").unwrap();
        let pipeline: Vec<Value> = serde_json::from_str(&pipeline_json).unwrap();

        // Expect a simple RLE -> Zstd pipeline
        assert_eq!(pipeline[0]["op"], "rle");
        assert_eq!(pipeline[1]["op"], "zstd");
        assert_eq!(pipeline.len(), 2);
    }

    #[test]
    fn test_plan_for_mostly_zero_deltas() {
        let original_data: Vec<i64> = vec![10, 10, 10, 11, 11, 11, 11, 12];
        let original_bytes = crate::utils::typed_slice_to_bytes(&original_data);

        let pipeline_json = plan_pipeline(&original_bytes, "Int64").unwrap();
        let pipeline: Vec<Value> = serde_json::from_str(&pipeline_json).unwrap();

        // Deltas are [0, 0, 1, 0, 0, 0, 1]. Mostly zero.
        // Expect Delta -> RLE -> Zstd
        assert_eq!(pipeline[0]["op"], "delta");
        assert_eq!(pipeline[1]["op"], "rle");
        assert_eq!(pipeline[2]["op"], "zstd");
    }

    #[test]
    fn test_plan_for_small_deltas_bitpack() {
        // Deltas are [1, 2, -1, 2, -3]. Zigzagged: [2, 4, 1, 4, 5]. Max is 5, needs 3 bits.
        let original_data: Vec<i16> = vec![100, 101, 103, 102, 104, 101];
        let original_bytes = crate::utils::typed_slice_to_bytes(&original_data);

        let pipeline_json = plan_pipeline(&original_bytes, "Int16").unwrap();
        let pipeline: Vec<Value> = serde_json::from_str(&pipeline_json).unwrap();

        // Expect Delta -> Zigzag -> Bitpack -> Zstd
        assert_eq!(pipeline[0]["op"], "delta");
        assert_eq!(pipeline[1]["op"], "zigzag");
        assert_eq!(pipeline[2]["op"], "bitpack");
        assert_eq!(pipeline[2]["params"]["bit_width"], 3);
        assert_eq!(pipeline[3]["op"], "zstd");
    }

    #[test]
    fn test_plan_for_large_deltas_leb128() {
        // Contains a large delta (1000) that would make bitpacking inefficient.
        let original_data: Vec<i32> = vec![100, 101, 103, 1103, 1102];
        let original_bytes = crate::utils::typed_slice_to_bytes(&original_data);

        let pipeline_json = plan_pipeline(&original_bytes, "Int32").unwrap();
        let pipeline: Vec<Value> = serde_json::from_str(&pipeline_json).unwrap();

        // Expect Delta -> Zigzag -> LEB128 -> Zstd
        assert_eq!(pipeline[0]["op"], "delta");
        assert_eq!(pipeline[1]["op"], "zigzag");
        assert_eq!(pipeline[2]["op"], "leb128");
        assert_eq!(pipeline[3]["op"], "zstd");
    }
    
    #[test]
    fn test_plan_for_constant_data() {
        let original_data: Vec<i32> = vec![100, 100, 100, 100, 100];
        let original_bytes = crate::utils::typed_slice_to_bytes(&original_data);
        let pipeline_json = plan_pipeline(&original_bytes, "Int32").unwrap();
        let pipeline: Vec<Value> = serde_json::from_str(&pipeline_json).unwrap();
        assert_eq!(pipeline[0]["op"], "rle");
        assert_eq!(pipeline[1]["op"], "zstd");
    }
}