// In: src/pipeline/planner_tests.rs

#[cfg(test)]
mod tests {
    // MODIFIED: Import PlanningContext
    use crate::chunk_pipeline::planner::{plan_pipeline, PlanningContext};
    use crate::chunk_pipeline::{executor, models::Operation};
    use crate::types::PhoenixDataType;
    use crate::utils::typed_slice_to_bytes;

    fn get_compressed_size(
        original_bytes: &[u8],
        pipeline: &[Operation],
        physical_type: PhoenixDataType, // MODIFIED: Use physical_type for execution
    ) -> usize {
        executor::execute_linear_encode_pipeline(original_bytes, physical_type, pipeline)
            .map(|v| v.len())
            .unwrap_or(usize::MAX)
    }

    fn assert_planner_is_optimal(
        original_bytes: &[u8],
        context: PlanningContext, // MODIFIED: Accept PlanningContext
        expected_pipeline: Vec<Operation>,
    ) {
        // MODIFIED: Pass context to planner
        let planner_plan = plan_pipeline(original_bytes, context.clone()).unwrap();

        let expected_size =
            get_compressed_size(original_bytes, &expected_pipeline, context.physical_dtype);

        let planner_actual_size = get_compressed_size(
            original_bytes,
            &planner_plan.pipeline,
            context.physical_dtype,
        );

        assert!(
            planner_actual_size <= expected_size.saturating_add(5),
            "Planner's choice (plan: {:?}, actual size: {}) was worse than the expected optimal plan (plan: {:?}, size: {})",
            planner_plan.pipeline, planner_actual_size, expected_pipeline, expected_size
        );
    }

    #[test]
    fn test_planner_chooses_rle_for_constant_data() {
        let data: Vec<i32> = vec![7; 1024];
        let bytes = typed_slice_to_bytes(&data);
        // MODIFIED: Create context
        let context = PlanningContext {
            initial_dtype: PhoenixDataType::Int32,
            physical_dtype: PhoenixDataType::Int32,
        };
        let plan = plan_pipeline(&bytes, context).unwrap();
        assert_eq!(
            plan.pipeline[0],
            Operation::Rle,
            "RLE was not chosen as the first operation for constant data."
        );
    }

    #[test]
    fn test_planner_chooses_delta_rle_for_constant_deltas() {
        let data: Vec<i32> = (10..1000).collect(); // Creates a stream with a constant delta of 1.
        let bytes = typed_slice_to_bytes(&data);
        // MODIFIED: Create context
        let context = PlanningContext {
            initial_dtype: PhoenixDataType::Int32,
            physical_dtype: PhoenixDataType::Int32,
        };

        let expected_best_plan = vec![
            Operation::Delta { order: 1 },
            Operation::Rle,
            Operation::Zstd { level: 3 },
        ];
        assert_planner_is_optimal(&bytes, context, expected_best_plan);
    }

    #[test]
    fn test_planner_chooses_xor_delta_for_drifting_floats() {
        // This test now correctly simulates a bit-cast float.
        let data: Vec<u64> = (0..1000)
            .map(|i| f64::to_bits(100.0 + (i as f64 * 1e-12)))
            .collect();
        let bytes = typed_slice_to_bytes(&data);
        // MODIFIED: Create context reflecting the bit-cast
        let context = PlanningContext {
            initial_dtype: PhoenixDataType::Float64, // Original type was float
            physical_dtype: PhoenixDataType::UInt64, // But planner sees u64 bytes
        };

        let expected_best_plan = vec![
            Operation::XorDelta,
            Operation::BitPack { bit_width: 1 },
            Operation::Zstd { level: 3 },
        ];

        assert_planner_is_optimal(&bytes, context, expected_best_plan);
    }

    #[test]
    fn test_planner_chooses_ans_for_skewed_data() {
        let mut data = Vec::new();
        for i in 0..1000 {
            data.push(0);
            if i % 100 == 0 {
                data.push(1);
            }
            if i % 250 == 0 {
                data.push(2);
            }
        }
        // MODIFIED: Create context
        let context = PlanningContext {
            initial_dtype: PhoenixDataType::UInt8,
            physical_dtype: PhoenixDataType::UInt8,
        };
        let plan = plan_pipeline(&data, context).unwrap();
        assert_eq!(
            plan.pipeline.last().unwrap(),
            &Operation::Ans,
            "Planner failed to choose ANS for highly skewed data. Final plan was: {:?}",
            plan.pipeline
        );
    }
}
