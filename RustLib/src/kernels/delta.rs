//! This module contains the pure, stateless kernels for performing delta encoding
//! and decoding on slices of primitive integer types. It follows the Phoenix
//! architectural pattern of separating generic core logic from the FFI dispatchers.
//! Helpers are in src/utils.rs

use num_traits::{PrimInt, WrappingAdd, WrappingSub};
use pyo3::PyResult;

use crate::error::PhoenixError;
use crate::utils::{bytes_to_typed_slice, typed_slice_to_bytes};

//==================================================================================
// 1. Generic Core Logic
//==================================================================================

/// Performs delta encoding on a mutable slice of primitive integers.
///
/// The operation is performed in-place to maximize performance and avoid
/// unnecessary allocations. It calculates `data[i] = data[i] - data[i - order]`.
/// The loop proceeds in reverse to prevent using already-modified values for
/// subsequent calculations.
///
/// # Type Parameters
/// * `T`: A primitive integer type that supports wrapping subtraction.
///
/// # Args
/// * `data`: A mutable slice of integers to be delta-encoded.
/// * `order`: The step size for the delta calculation.
fn encode_slice<T>(data: &mut [T], order: usize)
where
    T: PrimInt + WrappingSub,
{
    if data.len() <= order {
        return;
    }
    // Iterate backwards to use original values for calculation
    for i in (order..data.len()).rev() {
        data[i] = data[i].wrapping_sub(&data[i - order]);
    }
}

/// Performs delta decoding (cumulative sum) on a mutable slice.
///
/// The operation is performed in-place. It reconstructs the original values by
/// calculating `data[i] = data[i] + data[i - order]`. The loop proceeds
/// forward to build upon the reconstructed values.
///
/// # Type Parameters
/// * `T`: A primitive integer type that supports wrapping addition.
///
/// # Args
/// * `data`: A mutable slice of delta-encoded integers to be decoded.
/// * `order`: The step size used during encoding. MUST match.
fn decode_slice<T>(data: &mut [T], order: usize)
where
    T: PrimInt + WrappingAdd,
{
    if data.len() <= order {
        return;
    }
    // Iterate forwards to use the newly-decoded values for subsequent sums
    for i in order..data.len() {
        data[i] = data[i].wrapping_add(&data[i - order]);
    }
}

//==================================================================================
// 2. FFI Dispatcher Logic
//==================================================================================
//==================================================================================
// 2. FFI Dispatcher Logic (REVISED - No Macros)
//==================================================================================

/// The public-facing encode function for this module. It takes raw bytes and
/// dispatches to the appropriate generic implementation.
pub fn encode(bytes: &[u8], order: usize, original_type: &str) -> PyResult<Vec<u8>> {
    match original_type {
        "Int8" => {
            let mut data = unsafe { bytes_to_typed_slice::<i8>(bytes)?.to_vec() };
            encode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        "UInt8" => {
            let mut data = unsafe { bytes_to_typed_slice::<u8>(bytes)?.to_vec() };
            encode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        "Int16" => {
            let mut data = unsafe { bytes_to_typed_slice::<i16>(bytes)?.to_vec() };
            encode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        "UInt16" => {
            let mut data = unsafe { bytes_to_typed_slice::<u16>(bytes)?.to_vec() };
            encode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        "Int32" => {
            let mut data = unsafe { bytes_to_typed_slice::<i32>(bytes)?.to_vec() };
            encode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        "UInt32" => {
            let mut data = unsafe { bytes_to_typed_slice::<u32>(bytes)?.to_vec() };
            encode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        "Int64" => {
            let mut data = unsafe { bytes_to_typed_slice::<i64>(bytes)?.to_vec() };
            encode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        "UInt64" => {
            let mut data = unsafe { bytes_to_typed_slice::<u64>(bytes)?.to_vec() };
            encode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string()).into()),
    }
}

/// The public-facing decode function for this module.
pub fn decode(bytes: &[u8], order: usize, original_type: &str) -> PyResult<Vec<u8>> {
    match original_type {
        "Int8" => {
            let mut data = unsafe { bytes_to_typed_slice::<i8>(bytes)?.to_vec() };
            decode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        "UInt8" => {
            let mut data = unsafe { bytes_to_typed_slice::<u8>(bytes)?.to_vec() };
            decode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        "Int16" => {
            let mut data = unsafe { bytes_to_typed_slice::<i16>(bytes)?.to_vec() };
            decode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        "UInt16" => {
            let mut data = unsafe { bytes_to_typed_slice::<u16>(bytes)?.to_vec() };
            decode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        "Int32" => {
            let mut data = unsafe { bytes_to_typed_slice::<i32>(bytes)?.to_vec() };
            decode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        "UInt32" => {
            let mut data = unsafe { bytes_to_typed_slice::<u32>(bytes)?.to_vec() };
            decode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        "Int64" => {
            let mut data = unsafe { bytes_to_typed_slice::<i64>(bytes)?.to_vec() };
            decode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        "UInt64" => {
            let mut data = unsafe { bytes_to_typed_slice::<u64>(bytes)?.to_vec() };
            decode_slice(&mut data, order);
            Ok(typed_slice_to_bytes(&data))
        },
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string()).into()),
    }
}

//==================================================================================
// 3. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_encode_decode_roundtrip_i64() {
        let original = vec![100, 110, 115, 112, 122];
        let mut encoded = original.clone();
        encode_slice(&mut encoded, 1);
        assert_eq!(encoded, vec![100, 10, 5, -3, 10]);

        let mut decoded = encoded.clone();
        decode_slice(&mut decoded, 1);
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_delta_order_2_i32() {
        let original = vec![10, 20, 15, 28, 25];
        let mut encoded = original.clone();
        encode_slice(&mut encoded, 2);
        assert_eq!(encoded, vec![10, 20, 5, 8, 10]);

        let mut decoded = encoded.clone();
        decode_slice(&mut decoded, 2);
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_delta_wrapping_u8() {
        let original = vec![10, 5, 250, 255, 3];
        let mut encoded = original.clone();
        encode_slice(&mut encoded, 1);
        // 5-10 = -5 -> wraps to 251
        // 250-5 = 245
        // 255-250 = 5
        // 3-255 = -252 -> wraps to 4
        assert_eq!(encoded, vec![10, 251, 245, 5, 4]);

        let mut decoded = encoded.clone();
        decode_slice(&mut decoded, 1);
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_ffi_dispatcher_encode_decode() {
        let original_i32: Vec<i32> = vec![1000, 1000, 1005, 995];
        let original_bytes = typed_slice_to_bytes(&original_i32);

        let encoded_bytes = encode(&original_bytes, 1, "Int32").unwrap();
        let decoded_bytes = decode(&encoded_bytes, 1, "Int32").unwrap();

        assert_eq!(original_bytes, decoded_bytes);
    }

    #[test]
    fn test_ffi_dispatcher_unsupported_type() {
        let bytes = vec![0u8; 8];
        let result = encode(&bytes, 1, "Float64");
        assert!(result.is_err());
    }
}