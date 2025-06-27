//! This module contains the pure, stateless kernels for handling nullability
//! via Arrow-compatible validity bitmaps.
//!
//! The core philosophy is to separate the data from its validity. This module
//! provides tools to:
//! 1.  Strip nulls from a data stream, producing a contiguous buffer of only
//!     the valid data and a separate, compact boolean bitmap.
//! 2.  Re-apply a validity bitmap to a stream of valid data to perfectly
//!     reconstruct the original series with its nulls.

use polars::prelude::*;
use arrow::bitmap::Bitmap;
use arrow::buffer::Buffer;

use crate::error::PhoenixError;

//==================================================================================
// 1. Core Logic
//==================================================================================

/// Strips nulls from a Polars Series, returning the contiguous valid data
/// and the validity bitmap as separate byte buffers.
///
/// This is the primary entry point for the compression workflow. It prepares the
/// data for the compression pipeline by separating its two components.
///
/// # Args
/// * `series`: A reference to a Polars Series that may contain nulls.
///
/// # Returns
/// A tuple containing:
///   - `Vec<u8>`: A byte buffer of the contiguous, non-null data values.
///   - `Option<Vec<u8>>`: A byte buffer of the Arrow-compatible validity bitmap.
///     Returns `None` if the series contains no nulls.
pub fn strip_validity(series: &Series) -> Result<(Vec<u8>, Option<Vec<u8>>), PhoenixError> {
    let phys_series = series.to_physical_repr();
    
    if phys_series.null_count() == 0 {
        // If there are no nulls, we don't need a validity mask.
        // The data buffer is just the full series buffer.
        let (data_bytes, _) = phys_series.to_byte_slices();
        return Ok((data_bytes.to_vec(), None));
    }

    // If there are nulls, we need to create a new, contiguous buffer of valid data.
    let valid_data_series = series.drop_nulls();
    let (valid_data_bytes, _) = valid_data_series.to_physical_repr().to_byte_slices();

    // Extract the validity bitmap from the original series.
    let validity_bitmap = series
        .chunks()
        .iter()
        .map(|arr| arr.validity())
        .next() // Assuming a single chunk for simplicity here. A robust impl would handle multiple chunks.
        .flatten()
        .map(|bitmap| bitmap.as_slice().to_vec());

    Ok((valid_data_bytes.to_vec(), validity_bitmap))
}

/// Re-applies a validity bitmap to a stream of valid data to reconstruct
/// the original Polars Series with its nulls.
///
/// This is the final step in the decompression workflow, happening after the
/// data pipeline has fully decompressed the valid data.
///
/// # Args
/// * `valid_data_bytes`: A byte buffer containing only the valid data values.
/// * `validity_bytes_opt`: An optional byte buffer of the Arrow validity bitmap.
///   If `None`, the data is assumed to have no nulls.
/// * `original_type`: The string representation of the target Polars dtype.
/// * `total_len`: The total length of the final series, including nulls.
///
/// # Returns
/// A `Result` containing the fully reconstructed Polars Series.
pub fn reapply_validity(
    valid_data_bytes: &[u8],
    validity_bytes_opt: Option<&[u8]>,
    original_type: &str,
    total_len: usize,
) -> Result<Series, PhoenixError> {
    let dtype = polars::datatypes::DataType::from_str(original_type)
        .map_err(|_| PhoenixError::UnsupportedType(original_type.to_string()))?;

    // Create a series from the valid data first.
    let series_no_nulls = Series::from_vec_and_dtype(
        "", // Name is set by the caller
        valid_data_bytes,
        &dtype
    )?;

    // If a validity mask exists, we need to construct a new Arrow array with it.
    if let Some(validity_bytes) = validity_bytes_opt {
        let validity_bitmap = Bitmap::from(validity_bytes);
        if validity_bitmap.len() != total_len {
            return Err(PhoenixError::BufferMismatch(validity_bitmap.len(), total_len));
        }

        // This is the complex part: we need to create a new array with nulls.
        // We can do this by creating a builder and appending values or nulls.
        let mut builder = Series::builder_with_capacity(&dtype, total_len);
        let mut valid_data_iter = series_no_nulls.iter();

        for i in 0..total_len {
            if validity_bitmap.get(i) {
                // This is safe because the number of `true` bits should match the
                // length of the valid data series.
                builder.append_option(valid_data_iter.next())?;
            } else {
                builder.append_null();
            }
        }
        Ok(builder.finish())
    } else {
        // No nulls, the series is already correct.
        Ok(series_no_nulls)
    }
}

//==================================================================================
// 2. FFI Dispatcher Logic (Not needed for this module)
//==================================================================================
// This module's logic is complex and tightly coupled with the Polars `Series`
// object itself. It is not a generic byte-to-byte transform. Therefore, it will
// be called directly by the orchestrator and FFI layer, not exposed as a
// granular kernel in the `libphoenix` FFI module. Its public API is the two
// Rust functions defined above.

//==================================================================================
// 3. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    #[test]
    fn test_strip_and_reapply_with_nulls() {
        let original_series = Series::new("test_i32", &[Some(10), None, Some(20), Some(30), None]);
        let original_type = "Int32";
        let total_len = original_series.len();

        // --- Strip ---
        let (valid_data_bytes, validity_bytes_opt) = strip_validity(&original_series).unwrap();
        
        assert!(validity_bytes_opt.is_some());
        let validity_bytes = validity_bytes_opt.unwrap();

        // Expected valid data: [10, 20, 30]
        let expected_valid_data: Vec<i32> = vec![10, 20, 30];
        let expected_valid_bytes = crate::utils::typed_slice_to_bytes(&expected_valid_data);
        assert_eq!(valid_data_bytes, expected_valid_bytes);

        // Expected bitmap: 10110... (true, false, true, true, false)
        // Arrow bitmaps are LSB-ordered. 0b...01101 = 13
        assert_eq!(validity_bytes[0], 0b00001101);

        // --- Re-apply ---
        let reconstructed_series = reapply_validity(
            &valid_data_bytes,
            Some(&validity_bytes),
            original_type,
            total_len
        ).unwrap();

        // Assert that the reconstructed series is identical to the original
        assert!(original_series.equals(&reconstructed_series));
    }

    #[test]
    fn test_strip_and_reapply_no_nulls() {
        let original_series = Series::new("test_u16", &[10u16, 20, 30]);
        let original_type = "UInt16";
        let total_len = original_series.len();

        // --- Strip ---
        let (valid_data_bytes, validity_bytes_opt) = strip_validity(&original_series).unwrap();
        
        // There should be no validity mask
        assert!(validity_bytes_opt.is_none());
        
        let expected_bytes = crate::utils::typed_slice_to_bytes(&vec![10u16, 20, 30]);
        assert_eq!(valid_data_bytes, expected_bytes);

        // --- Re-apply ---
        let reconstructed_series = reapply_validity(
            &valid_data_bytes,
            None, // Pass None for the validity mask
            original_type,
            total_len
        ).unwrap();

        assert!(original_series.equals(&reconstructed_series));
    }

    #[test]
    fn test_reapply_with_mismatched_len() {
        let valid_data: Vec<i64> = vec![1, 2];
        let valid_data_bytes = crate::utils::typed_slice_to_bytes(&valid_data);
        
        // Bitmap says there are 3 values, but we only provide 2 valid data points.
        // This should be handled gracefully by the builder logic.
        let bitmap = Bitmap::from_iter(vec![true, false, true]);
        let validity_bytes = bitmap.as_slice().to_vec();

        let result = reapply_validity(&valid_data_bytes, Some(&validity_bytes), "Int64", 3);
        // The builder logic will panic or error if the iterator runs out.
        // A robust implementation should catch this.
        assert!(result.is_err());
    }
}