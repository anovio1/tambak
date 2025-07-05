//==================================================================================
// 3. Unit Tests
//==================================================================================
#[cfg(test)]
mod tests {
    use crate::frame_pipeline::profiler::discover_structure;
    use crate::frame_pipeline::profiler::DataStructure;
    use crate::frame_pipeline::PlannerHints;

    use super::*;
    use arrow::array::{Float64Array, Int32Array};
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_discover_multiplexed_from_hints() {
        let array = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let hints = PlannerHints {
            stream_id_column: Some("unit_id".to_string()),
            timestamp_column: Some("frame".to_string()),
        };

        let structure = discover_structure(&array, &batch, &hints).unwrap();
        assert_eq!(structure, DataStructure::Multiplexed);
    }

    #[test]
    fn test_discover_simple_for_non_float_data() {
        let array = Int32Array::from(vec![1, 2, 3, 1, 2, 3]);
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let hints = PlannerHints::default();

        let structure = discover_structure(&array, &batch, &hints).unwrap();
        assert_eq!(structure, DataStructure::Simple);
    }

    #[test]
    fn test_autocorrelation_finds_correct_stride() {
        let mut data = Vec::with_capacity(1000);
        for i in 0..200 {
            data.push(i as f64 * 10.0); // val_a
            data.push(i as f64 * -5.0); // val_b
            data.push(100.0); // val_c
            data.push(i as f64); // val_d
            data.push(0.0); // val_e
        }
        let array = Float64Array::from(data);
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let hints = PlannerHints::default();

        let structure = discover_structure(&array, &batch, &hints).unwrap();
        assert_eq!(structure, DataStructure::FixedStride(5));
    }

    #[test]
    fn test_autocorrelation_finds_no_stride_in_random_data() {
        let data: Vec<f64> = (0..1000).map(|_| rand::random()).collect();
        let array = Float64Array::from(data);
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let hints = PlannerHints::default();

        let structure = discover_structure(&array, &batch, &hints).unwrap();
        assert_eq!(structure, DataStructure::Simple);
    }
}
