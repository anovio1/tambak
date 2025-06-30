use std::any::TypeId;

// This line is crucial. It imports all the public items from the parent module
// (which will be orchestrator.rs).
use super::*;

// We also need to bring in any external test dependencies.
use arrow::array::{Float32Array, Float64Array, Int32Array, Int64Array, PrimitiveArray};
use arrow::datatypes::{ArrowNumericType, Float32Type, Float64Type, Int32Type, Int64Type};
use bytemuck::Pod;

//==============================================================================
// 3.1 The Authoritative Roundtrip Test Helper
//==============================================================================

/// A single, generic, and robust helper function to test the end-to-end
/// compression and decompression for any primitive array type.
fn roundtrip_test<T>(original_array: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
    // The `Pod` trait from bytemuck is the correct, safe way to guarantee
    // that a type can be safely viewed as a slice of bytes.
    T::Native: Pod,
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

            let type_id = TypeId::of::<T::Native>();
            if type_id == TypeId::of::<f32>() {
                // For f32, use transmute to f32 for comparison.
                let original_as_f32: f32 = bytemuck::cast(original_val);
                let reconstructed_as_f32: f32 = bytemuck::cast(reconstructed_val);
                if original_as_f32.is_sign_negative() && original_as_f32 == 0.0 {
                    assert!(
                        !reconstructed_as_f32.is_sign_negative() && reconstructed_as_f32 == 0.0,
                        "Canonicalization failed: expected -0.0 to become 0.0, but got {:?}",
                        reconstructed_val
                    );
                    // Skip the bit-pattern check for this specific case.
                    continue;
                }
            } else if type_id == TypeId::of::<f64>() {
                // For f64, use transmute to f64 for comparison.
                let original_as_f64: f64 = bytemuck::cast(original_val);
                let reconstructed_as_f64: f64 = bytemuck::cast(reconstructed_val);
                if original_as_f64.is_sign_negative() && original_as_f64 == 0.0 {
                    assert!(
                        !reconstructed_as_f64.is_sign_negative() && reconstructed_as_f64 == 0.0,
                        "Canonicalization failed: expected -0.0 to become 0.0, but got {:?}",
                        reconstructed_val
                    );
                    // Skip the bit-pattern check for this specific case.
                    continue;
                }
            }

            // --- CORRECTED COMPARISON LOGIC ---
            // Use `bytemuck::bytes_of` to get a byte slice representation of the value.
            // This is safe because of the `T::Native: Pod` trait bound.
            // This works for ALL primitive types, including floats, and guarantees
            // a bit-for-bit comparison.
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
// 3.5 Sparsity Strategy Test
//==============================================================================

#[test]
fn test_sparsity_strategy_is_triggered_and_correct() {
    // Create a sparse array: 80% of the values are either 0 or NULL.
    // 5 zeros + 3 nulls = 8 sparse values out of 10 total.
    let original_array = Int32Array::from(vec![
        Some(0),
        Some(100),
        None,
        Some(0),
        Some(0),
        None,
        Some(200),
        Some(0),
        Some(0),
        None,
    ]);

    // 1. Compress the array.
    let compressed_artifact_bytes =
        compress_chunk(&original_array).expect("Sparsity compression failed");

    // 2. Verify that the sparsity path was taken by inspecting the artifact.
    // We don't need to decompress the whole thing, just parse the artifact header.
    let artifact =
        CompressedChunk::from_bytes(&compressed_artifact_bytes).expect("Failed to parse artifact");

    // THIS IS THE KEY ASSERTION:
    // If the sparsity path was taken, the artifact will have a compressed_mask.
    assert!(
        artifact.compressed_mask.is_some(),
        "Sparsity strategy was not triggered: compressed_mask is None"
    );
    assert!(
        artifact.mask_pipeline_json.is_some(),
        "Sparsity strategy was not triggered: mask_pipeline_json is None"
    );

    // 3. Verify the roundtrip correctness.
    // This ensures that even if the sparse path was taken, the data is reconstructed perfectly.
    roundtrip_test(&original_array);
}