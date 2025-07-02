use std::any::TypeId;

use crate::pipeline::artifact::CompressedChunk;
use crate::pipeline::orchestrator::{compress_chunk, decompress_chunk};
use crate::pipeline::{Operation, Plan};

// We also need to bring in any external test dependencies.
use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, PrimitiveArray,
};
use arrow::datatypes::ArrowNumericType;
use bytemuck::Pod;
use num_traits::Float;
use serde_json::Value;

//==============================================================================
// 3.1 The Authoritative Roundtrip Test Helper
//==============================================================================

/// A single, generic, and robust helper function to test the end-to-end
/// compression and decompression for any numeric primitive array type.
fn roundtrip_test<T>(original_array: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
    // Pod for safe byte casting, Debug for rich assertion messages.
    T::Native: Pod + std::fmt::Debug,
{
    // --- 1. Compress the Array ---
    let compressed_artifact_bytes =
        compress_chunk(original_array).expect("Compression failed during test");

    if !original_array.is_empty() {
        assert!(
            !compressed_artifact_bytes.is_empty(),
            "Compression produced zero bytes for a non-empty array"
        );
    }

    // --- 2. Decompress the Artifact ---
    let reconstructed_arrow_array =
        decompress_chunk(&compressed_artifact_bytes).expect("Decompression failed during test");

    // --- 3. Downcast and Verify ---
    let reconstructed_primitive_array = reconstructed_arrow_array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("Failed to downcast reconstructed array to the correct primitive type");

    // --- 4. Assert Equality ---
    assert_eq!(
        original_array.len(),
        reconstructed_primitive_array.len(),
        "Array length mismatch after roundtrip"
    );
    assert_eq!(
        original_array.null_count(),
        reconstructed_primitive_array.null_count(),
        "Null count mismatch after roundtrip"
    );

    for i in 0..original_array.len() {
        if original_array.is_null(i) {
            assert!(
                reconstructed_primitive_array.is_null(i),
                "Null value mismatch at index {}",
                i
            );
        } else {
            assert!(
                reconstructed_primitive_array.is_valid(i),
                "Valid value mismatch at index {}",
                i
            );
            let original_val = original_array.value(i);
            let reconstructed_val = reconstructed_primitive_array.value(i);

            // Special check for -0.0 canonicalization, which is a valid transformation.
            let type_id = TypeId::of::<T::Native>();
            if type_id == TypeId::of::<f32>() {
                let original_as_f32: f32 = bytemuck::cast(original_val);
                if original_as_f32.is_sign_negative() && original_as_f32 == 0.0 {
                    let reconstructed_as_f32: f32 = bytemuck::cast(reconstructed_val);
                    assert!(
                        !reconstructed_as_f32.is_sign_negative() && reconstructed_as_f32 == 0.0
                    );
                    continue; // Skip bit-pattern check for this specific case
                }
            } else if type_id == TypeId::of::<f64>() {
                let original_as_f64: f64 = bytemuck::cast(original_val);
                if original_as_f64.is_sign_negative() && original_as_f64 == 0.0 {
                    let reconstructed_as_f64: f64 = bytemuck::cast(reconstructed_val);
                    assert!(
                        !reconstructed_as_f64.is_sign_negative() && reconstructed_as_f64 == 0.0
                    );
                    continue; // Skip bit-pattern check for this specific case
                }
            }

            // General bit-for-bit comparison for all other values.
            assert_eq!(
                bytemuck::bytes_of(&original_val),
                bytemuck::bytes_of(&reconstructed_val),
                "Value bit-pattern mismatch at index {}: orig={:?}, recon={:?}",
                i,
                original_val,
                reconstructed_val
            );
        }
    }
}

/// A dedicated roundtrip test helper for `BooleanArray`, which is not numeric.
fn roundtrip_test_bool(original_array: &BooleanArray) {
    let compressed_artifact_bytes =
        compress_chunk(original_array).expect("Boolean compression failed");
    let reconstructed_arrow_array =
        decompress_chunk(&compressed_artifact_bytes).expect("Boolean decompression failed");
    let reconstructed_bool_array = reconstructed_arrow_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("Failed to downcast reconstructed array to BooleanArray");

    // BooleanArray implements PartialEq, so we can do a direct comparison.
    assert_eq!(original_array, reconstructed_bool_array);
}

//==============================================================================
// 3.2 Integer Type Test Cases
//==============================================================================

#[test]
fn test_roundtrip_integers_with_nulls() {
    let array = Int64Array::from(vec![Some(1000), Some(1001), None, Some(1003), Some(1003)]);
    roundtrip_test(&array);
}

#[test]
fn test_roundtrip_integers_no_nulls() {
    let array = Int32Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80]);
    roundtrip_test(&array);
}

#[test]
fn test_roundtrip_integers_all_nulls() {
    let array = Int32Array::from(vec![None, None, None, None]);
    roundtrip_test(&array);
}

#[test]
fn test_roundtrip_integers_empty() {
    let array = Int64Array::from(vec![] as Vec<i64>);
    roundtrip_test(&array);
}

#[test]
fn test_roundtrip_integers_single_value() {
    let array = Int32Array::from(vec![Some(42)]);
    roundtrip_test(&array);
}

//==============================================================================
// 3.3 Floating-Point Type Test Cases
//==============================================================================

#[test]
fn test_roundtrip_floats_with_nulls() {
    let array = Float32Array::from(vec![Some(10.5), None, Some(-20.0), Some(30.1), None]);
    roundtrip_test(&array);
}

#[test]
fn test_roundtrip_floats_no_nulls() {
    let array = Float64Array::from(vec![10.5, -20.0, 30.1, 40.9, 50.0]);
    roundtrip_test(&array);
}

#[test]
fn test_roundtrip_floats_all_nulls() {
    let array = Float64Array::from(vec![None, None, None]);
    roundtrip_test(&array);
}

#[test]
fn test_roundtrip_floats_empty() {
    let array = Float32Array::from(vec![] as Vec<f32>);
    roundtrip_test(&array);
}

#[test]
fn test_roundtrip_floats_special_values() {
    let array = Float64Array::from(vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0, 0.0]);
    roundtrip_test(&array);
}

//==============================================================================
// 4. Boolean Type Test Cases (NEW)
//==============================================================================

#[test]
fn test_roundtrip_booleans_with_nulls() {
    let array = BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]);
    roundtrip_test_bool(&array);
}

#[test]
fn test_roundtrip_booleans_no_nulls() {
    let array = BooleanArray::from(vec![true, false, true, true, false]);
    roundtrip_test_bool(&array);
}

#[test]
fn test_roundtrip_booleans_all_nulls() {
    let array = BooleanArray::from(vec![None, None, None]);
    roundtrip_test_bool(&array);
}

#[test]
fn test_roundtrip_booleans_empty() {
    let array = BooleanArray::from(vec![] as Vec<bool>);
    roundtrip_test_bool(&array);
}

//==============================================================================
// 3.4 Pipeline-Specific Tests (Validating Planner & Executor Integration)
//==============================================================================

#[test]
fn test_roundtrip_drifting_floats_triggers_xor_delta() {
    // This data has small bit-wise differences and should trigger the XOR Delta pipeline.
    let array = Float32Array::from(vec![Some(1.0), Some(1.0000001), None, Some(1.0000002)]);
    roundtrip_test(&array);
}

#[test]
fn test_roundtrip_clustered_floats_triggers_shuffle() {
    // This data has large deltas but stable high-order bytes, which should trigger Shuffle.
    let array = Float64Array::from(vec![
        Some(1000.5),
        Some(1000.1),
        Some(1000.9),
        None,
        Some(1000.4),
    ]);
    roundtrip_test(&array);
}

#[test]
fn test_roundtrip_constant_integers_triggers_rle() {
    // This data is constant and should trigger the simple RLE pipeline.
    let array = Int64Array::from(vec![Some(777), Some(777), Some(777), None, Some(777)]);
    roundtrip_test(&array);
}

//==============================================================================
// 3.5 NEW v4.2 ARCHITECTURE VALIDATION TESTS (NON-OVERLAPPING)
//==============================================================================

#[test]
fn test_sparsity_strategy_is_triggered_and_correct() {
    let original_array = Int32Array::from(vec![Some(0), Some(100), None, Some(0), Some(0)]);
    let compressed_artifact_bytes =
        compress_chunk(&original_array).expect("Sparsity compression failed");

    let artifact =
        CompressedChunk::from_bytes(&compressed_artifact_bytes).expect("Failed to parse artifact");
    let plan: Plan = serde_json::from_str(&artifact.plan_json).unwrap();

    let sparsify_op_exists = plan
        .pipeline
        .iter()
        .any(|op| matches!(op, Operation::Sparsify { .. }));

    assert!(
        sparsify_op_exists,
        "Sparsity strategy was not triggered: 'Sparsify' op is missing from the plan"
    );
    assert!(artifact.compressed_streams.contains_key("sparsity_mask"));
    roundtrip_test(&original_array);
}

#[test]
fn test_dense_strategy_is_correctly_chosen() {
    let original_array = Int64Array::from(vec![Some(100), Some(101), None, Some(103)]);
    let compressed_artifact_bytes = compress_chunk(&original_array).unwrap();

    let artifact = CompressedChunk::from_bytes(&compressed_artifact_bytes).unwrap();
    let plan: Plan = serde_json::from_str(&artifact.plan_json).unwrap();

    let sparsify_op_exists = plan
        .pipeline
        .iter()
        .any(|op| matches!(op, Operation::Sparsify { .. }));

    assert!(
        !sparsify_op_exists,
        "Sparsity strategy was incorrectly triggered for dense data"
    );
    assert!(!artifact.compressed_streams.contains_key("sparsity_mask"));
    roundtrip_test(&original_array);
}
