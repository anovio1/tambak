//==================================================================================
// 2. Unit Tests (REVISED - Reflecting the correct Buffer API)
//==================================================================================

#[cfg(test)]
mod tests {
    use crate::null_handling::bitmap::*;
    use crate::utils::typed_slice_to_bytes; // <-- Import the utility
    use arrow::array::Int32Array;
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::Int32Type;

    #[test]
    fn test_strip_valid_data_to_vec() {
        let source_array = Int32Array::from(vec![Some(10), None, Some(30)]);
        let valid_data_vec = strip_valid_data_to_vec::<Int32Type>(&source_array);
        assert_eq!(valid_data_vec, vec![10, 30]);
    }

    #[test]
    fn test_reapply_bitmap_with_nulls() {
        let valid_data: Vec<i32> = vec![10, 30];
        let null_buffer = NullBuffer::from(vec![true, false, true]);
        let total_rows = 3;

        // --- THIS IS THE FIX ---
        // Convert the typed Vec<i32> to a byte Vec<u8> before calling the function.
        let valid_data_bytes = typed_slice_to_bytes(&valid_data);

        let reconstructed_array = reapply_bitmap::<Int32Type>(
            valid_data_bytes, // Pass the correct byte vector
            Some(null_buffer),
            total_rows,
        )
        .unwrap();

        let expected_array = Int32Array::from(vec![Some(10), None, Some(30)]);
        assert_eq!(reconstructed_array, expected_array);
    }

    #[test]
    fn test_reapply_bitmap_no_nulls() {
        let valid_data: Vec<i32> = vec![10, 20, 30];
        let total_rows = 3;

        // --- THIS IS THE FIX ---
        // Convert the typed Vec<i32> to a byte Vec<u8>.
        let valid_data_bytes = typed_slice_to_bytes(&valid_data);

        let reconstructed_array = reapply_bitmap::<Int32Type>(
            valid_data_bytes, // Pass the correct byte vector
            None,             // No nulls
            total_rows,
        )
        .unwrap();

        let expected_array = Int32Array::from(vec![10, 20, 30]);
        assert_eq!(reconstructed_array, expected_array);
    }
    #[test]
    fn test_reapply_bitmap_from_vec_with_nulls() {
        // THIS IS THE FIX: Pass the DENSE data, not the sparse data.
        let dense_vec: Vec<i32> = vec![10, 30, 50];
        let null_buffer = NullBuffer::from(vec![true, false, true, false, true]);
        let total_rows = 5;

        let reconstructed_array = reapply_bitmap_from_vec::<Int32Type>(
            dense_vec, // Pass the dense vector
            Some(null_buffer),
            total_rows,
        )
        .unwrap();

        let expected_array = Int32Array::from(vec![Some(10), None, Some(30), None, Some(50)]);
        assert_eq!(reconstructed_array, expected_array);
    }

    #[test]
    fn test_reapply_bitmap_from_vec_no_nulls() {
        let data_vec: Vec<i32> = vec![10, 20, 30];
        let total_rows = 3;

        let reconstructed_array = reapply_bitmap_from_vec::<Int32Type>(
            data_vec, None, // No nulls
            total_rows,
        )
        .unwrap();

        let expected_array = Int32Array::from(vec![10, 20, 30]);
        assert_eq!(reconstructed_array, expected_array);
    }
}
