#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::frame_pipeline::frame_orchestrator::{
        compress_frame, decompress_frame, get_frame_diagnostics,
    };
    use crate::chunk_pipeline::models::{Operation, Plan}; // For plan inspection

    use crate::types::PhoenixDataType;
    use arrow::array::{Array, Float64Array, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use serde_json::Value;

    /// A helper to create a sample RecordBatch for testing. (This remains unchanged).
    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Float64, true),
        ]));

        let col_a = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ])) as Arc<dyn Array>;
        let col_b = Arc::new(Float64Array::from(vec![
            Some(1.1),
            Some(2.2),
            Some(3.3),
            None,
            Some(5.5),
        ])) as Arc<dyn Array>;

        RecordBatch::try_new(schema, vec![col_a, col_b]).unwrap()
    }

    #[test]
    fn test_frame_roundtrip_basic() {
        let original_batch = create_test_batch();
        let hints = None;

        // --- REFACTORED ---
        // The pre-compression checkpoint is removed. A simple roundtrip test
        // should focus only on the correctness of the final output, not the
        // internal plan details.
        // --- END REFACTOR ---

        // 1. Compress
        let compressed_bytes = compress_frame(&original_batch, &hints).expect("Compression failed");
        assert!(!compressed_bytes.is_empty());

        // 2. Decompress
        let decompressed_batch = decompress_frame(&compressed_bytes).expect("Decompression failed");

        // 3. Verify (This logic is unchanged and remains the source of truth)
        assert_eq!(
            original_batch.num_columns(),
            decompressed_batch.num_columns()
        );
        assert_eq!(original_batch.num_rows(), decompressed_batch.num_rows());

        let original_col_a = original_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let decompressed_col_a = decompressed_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(original_col_a, decompressed_col_a);

        let original_col_b = original_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let decompressed_col_b = decompressed_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(original_col_b, decompressed_col_b);
    }

    #[test]
    fn test_get_frame_diagnostics_is_valid_json() {
        let original_batch = create_test_batch();

        // --- REFACTORED ---
        // The obsolete call to `create_plan` is removed. The test's purpose is to
        // validate the output of `get_frame_diagnostics`, so that's all we need.
        // --- END REFACTOR ---

        let compressed_bytes = compress_frame(&original_batch, &None).unwrap();

        let diagnostics_json = get_frame_diagnostics(&compressed_bytes).unwrap();
        let parsed: Result<Value, _> = serde_json::from_str(&diagnostics_json);
        assert!(parsed.is_ok(), "Diagnostics output should be valid JSON");

        let diagnostics_array = parsed.unwrap().as_array().unwrap().to_vec();
        assert_eq!(diagnostics_array.len(), 2); // Two columns
        assert_eq!(diagnostics_array[0]["column_index"], 0);
        assert_eq!(diagnostics_array[1]["original_type"], "Float64");
        assert!(diagnostics_array[1]["streams"].is_object());
    }

    // ==============================================================================
    // NEW TEST: This test explicitly replaces the *intent* of the old checkpoints.
    // It validates that the planner is making correct high-level choices.
    // ==============================================================================
    #[test]
    fn test_diagnostics_reveal_correct_planning_strategies() {
        let original_batch = create_test_batch();

        // 1. Compress the frame to generate the artifacts and their plans.
        let compressed_bytes = compress_frame(&original_batch, &None).unwrap();

        // 2. Use the public diagnostics API to inspect the outcome.
        let diagnostics_json = get_frame_diagnostics(&compressed_bytes).unwrap();
        let diagnostics: Vec<Value> = serde_json::from_str(&diagnostics_json).unwrap();

        // --- CHECKPOINT for Column A (Int32) ---
        let plan_json_a = &diagnostics[0]["plan"];
        let plan_a: Plan = serde_json::from_value(plan_json_a.clone()).unwrap();
        println!("\n--- CHECKPOINT (from diagnostics): test_diagnostics_reveal_correct_planning_strategies ---");
        println!("  - Plan for Column A (Int32): {:?}", plan_a.pipeline);

        // Assert that the plan for an integer column is not empty. A simple sanity check.
        assert!(
            !plan_a.pipeline.is_empty(),
            "Plan for integer column should not be empty"
        );

        // --- CHECKPOINT for Column B (Float64) ---
        let plan_json_b = &diagnostics[1]["plan"];
        let plan_b: Plan = serde_json::from_value(plan_json_b.clone()).unwrap();
        println!("  - Plan for Column B (Float64): {:?}", plan_b.pipeline);

        // Assert that the float pipeline contains the required preprocessing steps.
        // This is a much stronger guarantee than the old test provided.
        assert!(
            plan_b.pipeline.contains(&Operation::CanonicalizeZeros),
            "Float64 plan should contain CanonicalizeZeros"
        );
        assert!(
            plan_b.pipeline.iter().any(|op| matches!(
                op,
                Operation::BitCast {
                    to_type: PhoenixDataType::UInt64
                }
            )),
            "Float64 plan should contain a BitCast to UInt64"
        );
        println!("------------------------------------------------------------------------------------------\n");
    }
}
