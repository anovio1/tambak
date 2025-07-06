use std::any::TypeId;

use crate::bridge;
use crate::chunk_pipeline::artifact::CompressedChunk;
use crate::chunk_pipeline::context::{PipelineInput, PipelineOutput};
use crate::chunk_pipeline::orchestrator::{compress_chunk, decompress_chunk};
use crate::chunk_pipeline::models::{ChunkPlan, Operation};
use crate::types::TambakDataType;

// We also need to bring in any external test dependencies.
use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, PrimitiveArray,
};
use arrow::datatypes::ArrowNumericType;
use bytemuck::{cast_slice, from_bytes, Pod};

// Test Helpers
/// Test helper to simulate the bridge's marshalling from Arrow-like data to PipelineInput.
fn create_pipeline_input_from_options<T>(
    data: &[Option<T>],
    dtype: TambakDataType,
) -> PipelineInput
where
    T: Pod + Copy,
{
    let total_rows = data.len();
    let mut main_data_typed: Vec<T> = Vec::new();
    let mut null_mask_bytes: Vec<u8> = Vec::with_capacity(total_rows);
    let mut num_valid_rows = 0;

    for item in data {
        if let Some(value) = item {
            main_data_typed.push(*value);
            null_mask_bytes.push(1);
            num_valid_rows += 1;
        } else {
            null_mask_bytes.push(0);
        }
    }

    let null_mask = if num_valid_rows == total_rows {
        None // No nulls, so no mask.
    } else {
        Some(null_mask_bytes)
    };

    PipelineInput::new(
        bytemuck::cast_slice(&main_data_typed).to_vec(),
        null_mask,
        dtype,
        total_rows,
        num_valid_rows,
        Some("test_column".to_string()),
    )
}

/// Test helper to simulate the bridge's reconstruction of data from a PipelineOutput.
/// This allows us to verify the roundtrip correctness.

fn reconstruct_vec_from_output<T>(output: &PipelineOutput) -> Vec<Option<T>>
where
    T: Pod + Copy,
{
    let main_typed: &[T] = cast_slice(&output.main);
    let mut main_iter = main_typed.iter();
    let mut result = Vec::with_capacity(output.total_rows);

    if let Some(null_mask) = &output.null_mask {
        for &is_valid in null_mask.iter() {
            if is_valid == 1 {
                result.push(main_iter.next().copied());
            } else {
                result.push(None);
            }
        }
    } else {
        // No null mask means all data is valid.
        for value in main_iter {
            result.push(Some(*value));
        }
    }
    result
}

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
        bridge::compress_arrow_chunk(original_array).expect("Compression failed during test");

    if !original_array.is_empty() {
        assert!(
            !compressed_artifact_bytes.is_empty(),
            "Compression produced zero bytes for a non-empty array"
        );
    }

    // --- 2. Decompress the Artifact ---
    let reconstructed_arrow_array = bridge::decompress_arrow_chunk(&compressed_artifact_bytes)
        .expect("Decompression failed during test");

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
fn roundtrip_test_v2<T>(original_array: &PrimitiveArray<T>)
where
    T: ArrowNumericType,
    T::Native: Pod + std::fmt::Debug,
{
    // 1. Marshall to pure input
    let pipeline_input =
        crate::bridge::arrow_impl::arrow_to_pipeline_input(original_array).unwrap();

    // 2. Compress using the new v2 compression function
    let compressed_bytes = compress_chunk(pipeline_input).expect("v2 Compression failed");

    // 3. Decompress using the new v2 decompression function we are building
    let pipeline_output = decompress_chunk(&compressed_bytes).expect("v2 Decompression failed");

    // 4. Marshall back to an Arrow Array
    let reconstructed_array =
        crate::bridge::arrow_impl::pipeline_output_to_array(pipeline_output).unwrap();

    // 5. Downcast and verify (code can be copied from the old roundtrip_test)
    let reconstructed_primitive_array = reconstructed_array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap();
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
    // --- 1. Compress using the bridge API ---
    let compressed_artifact_bytes =
        bridge::compress_arrow_chunk(original_array).expect("Boolean compression failed");

    // --- 2. Decompress using the bridge API ---
    // The bridge correctly returns a generic Box<dyn Array>.
    let reconstructed_arrow_array = bridge::decompress_arrow_chunk(&compressed_artifact_bytes)
        .expect("Boolean decompression failed");

    let reconstructed_bool_array = reconstructed_arrow_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("Failed to downcast reconstructed array to BooleanArray");

    // --- 4. Assert equality ---
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
fn test_v2_api_roundtrip_with_nulls() {
    let array = Int64Array::from(vec![Some(1000), Some(1001), None, Some(1003)]);
    roundtrip_test_v2(&array);
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
    // --- 1. SETUP: Define the sparse data ---
    let mut data = vec![Some(0i32); 500];
    for i in 0..30 {
        data[100 + i] = Some((i as i32) * 10);
    }
    data[400] = None; // Ensure null handling is tested.

    // --- 2. MARSHALL: Simulate the bridge converting Arrow to pure data ---
    let pipeline_input = create_pipeline_input_from_options(&data, TambakDataType::Int32);

    // --- 3. COMPRESS: Call the pure orchestrator function ---
    let compressed_artifact_bytes =
        compress_chunk(pipeline_input).expect("Sparsity compression failed");

    // --- 4. VERIFY PLAN: Check that the planner made the correct choice ---
    let artifact =
        CompressedChunk::from_bytes(&compressed_artifact_bytes).expect("Failed to parse artifact");
    let plan: ChunkPlan = serde_json::from_str(&artifact.plan_json).unwrap();

    let sparsify_op_exists = plan
        .pipeline
        .iter()
        .any(|op| matches!(op, Operation::Sparsify { .. }));

    assert!(
        sparsify_op_exists,
        "Sparsity strategy was not triggered for empirically sparse data. Plan was: {:?}",
        plan.pipeline
    );

    // --- 5. VERIFY ROUNDTRIP: Decompress and reconstruct to ensure correctness ---
    let pipeline_output = decompress_chunk(&compressed_artifact_bytes).unwrap();
    let reconstructed_data = reconstruct_vec_from_output::<i32>(&pipeline_output);

    assert_eq!(data, reconstructed_data, "Data mismatch after roundtrip.");
}

#[test]
fn test_dense_strategy_is_correctly_chosen() {
    // --- 1. SETUP: Define the dense data ---
    let data = vec![Some(100i64), Some(101), None, Some(103)];

    // --- 2. MARSHALL: Simulate the bridge ---
    let pipeline_input = create_pipeline_input_from_options(&data, TambakDataType::Int64);

    // --- 3. COMPRESS: Call the pure orchestrator ---
    let compressed_artifact_bytes = compress_chunk(pipeline_input).unwrap();

    // --- 4. VERIFY PLAN: Check that the planner made the correct choice ---
    let artifact = CompressedChunk::from_bytes(&compressed_artifact_bytes).unwrap();
    let plan: ChunkPlan = serde_json::from_str(&artifact.plan_json).unwrap();

    let sparsify_op_exists = plan
        .pipeline
        .iter()
        .any(|op| matches!(op, Operation::Sparsify { .. }));

    assert!(
        !sparsify_op_exists,
        "Sparsity strategy was incorrectly triggered for dense data"
    );
    assert!(!artifact.compressed_streams.contains_key("sparsity_mask"));

    // --- 5. VERIFY ROUNDTRIP: Decompress and reconstruct to ensure correctness ---
    let pipeline_output = decompress_chunk(&compressed_artifact_bytes).unwrap();
    let reconstructed_data = reconstruct_vec_from_output::<i64>(&pipeline_output);

    assert_eq!(data, reconstructed_data, "Data mismatch after roundtrip.");
}
