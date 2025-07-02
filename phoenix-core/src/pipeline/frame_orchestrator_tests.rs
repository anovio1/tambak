#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::pipeline::frame_orchestrator::{
        compress_frame, decompress_frame, get_frame_diagnostics,
    };
    use crate::pipeline::orchestrator;

    use arrow::array::{Array, Float64Array, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use serde_json::Value;

    /// A helper to create a sample RecordBatch for testing.
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

        // --- CHECKPOINT ---
        // Let's inspect the plans generated for each column before compression.
        println!("\n--- CHECKPOINT: test_frame_roundtrip_basic ---");
        let plan_col_a = orchestrator::create_plan(original_batch.column(0)).unwrap();
        let plan_col_b = orchestrator::create_plan(original_batch.column(1)).unwrap();
        println!("  - Plan for Column A (Int32): {:?}", plan_col_a);
        println!("  - Plan for Column B (Float64): {:?}", plan_col_b);
        println!("---------------------------------------------\n");
        // --- END CHECKPOINT ---

        // 1. Compress
        let compressed_bytes = compress_frame(&original_batch, &hints).expect("Compression failed");
        assert!(!compressed_bytes.is_empty());

        // 2. Decompress
        let decompressed_batch = decompress_frame(&compressed_bytes).expect("Decompression failed");

        // 3. Verify
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

        // --- CHECKPOINT ---
        println!("\n--- CHECKPOINT: test_get_frame_diagnostics_is_valid_json ---");
        let plan_col_b = orchestrator::create_plan(original_batch.column(1)).unwrap();
        println!(
            "  - Pre-compression Plan for Column B (Float64): {:?}",
            plan_col_b
        );
        println!("-------------------------------------------------------------\n");
        // --- END CHECKPOINT ---

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
}
