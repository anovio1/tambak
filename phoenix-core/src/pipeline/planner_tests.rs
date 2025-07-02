#[cfg(test)]
mod tests {
    use crate::pipeline::planner::*;
    use crate::pipeline::{executor, models::Operation}; // Corrected import path
    use crate::types::PhoenixDataType;
    use crate::utils::typed_slice_to_bytes;

    fn get_compressed_size(
        original_bytes: &[u8],
        pipeline: &[Operation],
        original_type: PhoenixDataType,
    ) -> usize {
        executor::execute_linear_encode_pipeline(original_bytes, original_type, pipeline)
            .map(|v| v.len())
            .unwrap_or(usize::MAX)
    }

    fn assert_planner_is_optimal(
        original_bytes: &[u8],
        original_type: PhoenixDataType,
        expected_pipeline: Vec<Operation>,
    ) {
        let planner_plan = plan_pipeline(original_bytes, original_type).unwrap();

        let expected_size = get_compressed_size(original_bytes, &expected_pipeline, original_type);

        let planner_actual_size =
            get_compressed_size(original_bytes, &planner_plan.pipeline, original_type);

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
        let plan = plan_pipeline(&bytes, PhoenixDataType::Int32).unwrap();
        // CORRECTED: Reverted to the simpler and more direct assert_eq!
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

        let expected_best_plan = vec![
            Operation::Delta { order: 1 },
            Operation::Rle,
            Operation::Zstd { level: 3 },
        ];
        assert_planner_is_optimal(&bytes, PhoenixDataType::Int32, expected_best_plan);
    }

    #[test]
    fn test_planner_chooses_xor_delta_for_drifting_floats() {
        let data: Vec<u64> = (0..1000)
            .map(|i| f64::to_bits(100.0 + (i as f64 * 1e-12)))
            .collect();
        let bytes = typed_slice_to_bytes(&data);

        let expected_best_plan = vec![
            Operation::XorDelta,
            Operation::BitPack { bit_width: 1 },
            Operation::Zstd { level: 3 },
        ];

        assert_planner_is_optimal(&bytes, PhoenixDataType::UInt64, expected_best_plan);
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
        let plan = plan_pipeline(&data, PhoenixDataType::UInt8).unwrap();
        assert_eq!(
            plan.pipeline.last().unwrap(),
            &Operation::Ans,
            "Planner failed to choose ANS for highly skewed data. Final plan was: {:?}",
            plan.pipeline
        );
    }
}
