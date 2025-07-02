//! This module serves as the public API and dispatcher for the collection of all
//! pure, stateless compression and decompression kernels.
//!
//! It declares all kernel sub-modules and provides a single, unified `dispatch`
//! function for both encoding and decoding. This dispatcher is the sole entry
//! point for the `pipeline::executor`. It takes a strongly-typed `Operation`
//! and `PhoenixDataType` and calls the appropriate generic kernel implementation,
//! ensuring type safety at compile time.

use crate::error::PhoenixError;
use crate::pipeline::models::Operation;
use crate::types::PhoenixDataType;
use crate::utils::typed_slice_to_bytes;

//==================================================================================
// 1. Module Declarations
//==================================================================================

pub mod ans;
pub mod bitcast;
pub mod bitpack;
pub mod canonicalize;
pub mod delta;
pub mod dictionary;
pub mod leb128;
pub mod rle;
pub mod shuffle;
pub mod sparsity;
pub mod xor_delta;
pub mod zigzag;
pub mod zstd;

//==================================================================================
// 2. Public API (Unified Dispatchers - v4.3 Type-Safe)
//==================================================================================

/// The single, unified dispatcher for all ENCODE operations.
pub fn dispatch_encode(
    op: &Operation,
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    dtype: PhoenixDataType,
) -> Result<(), PhoenixError> {
    // This macro handles the boilerplate of converting the input `&[u8]` into a
    // typed slice `&[T]`, which is the expected input for many kernels.
    macro_rules! convert_and_exec_encode {
        ($T:ty, $kernel:ident, $($args:expr),*) => {{
            if input_bytes.len() % std::mem::size_of::<$T>() != 0 {
                return Err(PhoenixError::BufferMismatch(input_bytes.len(), std::mem::size_of::<$T>()));
            }
            // bytemuck is a safe, zero-copy cast.
            let typed_slice: &[$T] = bytemuck::try_cast_slice(input_bytes)?;
            $kernel::encode(typed_slice, $($args),*)
        }};
    }

    use Operation::*;
    use PhoenixDataType::*;

    match op {
        // --- Kernels that operate on typed slices ---
        Delta { order } => match dtype {
            Int8 => convert_and_exec_encode!(i8, delta, output_buf, *order),
            UInt8 => convert_and_exec_encode!(u8, delta, output_buf, *order),
            Int16 => convert_and_exec_encode!(i16, delta, output_buf, *order),
            UInt16 => convert_and_exec_encode!(u16, delta, output_buf, *order),
            Int32 => convert_and_exec_encode!(i32, delta, output_buf, *order),
            UInt32 => convert_and_exec_encode!(u32, delta, output_buf, *order),
            Int64 => convert_and_exec_encode!(i64, delta, output_buf, *order),
            UInt64 => convert_and_exec_encode!(u64, delta, output_buf, *order),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "Delta not supported for {:?}",
                dtype
            ))),
        },
        XorDelta => match dtype {
            UInt32 => convert_and_exec_encode!(u32, xor_delta, output_buf),
            UInt64 => convert_and_exec_encode!(u64, xor_delta, output_buf),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "XorDelta not supported for {:?}",
                dtype
            ))),
        },
        Rle => match dtype {
            Int8 => convert_and_exec_encode!(i8, rle, output_buf),
            UInt8 | Boolean => convert_and_exec_encode!(u8, rle, output_buf),
            Int16 => convert_and_exec_encode!(i16, rle, output_buf),
            UInt16 => convert_and_exec_encode!(u16, rle, output_buf),
            Int32 => convert_and_exec_encode!(i32, rle, output_buf),
            UInt32 => convert_and_exec_encode!(u32, rle, output_buf),
            Int64 => convert_and_exec_encode!(i64, rle, output_buf),
            UInt64 => convert_and_exec_encode!(u64, rle, output_buf),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "RLE not supported for {:?}",
                dtype
            ))),
        },
        ZigZag => match dtype {
            Int8 => convert_and_exec_encode!(i8, zigzag, output_buf),
            Int16 => convert_and_exec_encode!(i16, zigzag, output_buf),
            Int32 => convert_and_exec_encode!(i32, zigzag, output_buf),
            Int64 => convert_and_exec_encode!(i64, zigzag, output_buf),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "ZigZag requires a signed integer type, but got {:?}",
                dtype
            ))),
        },
        Leb128 => match dtype {
            UInt8 => convert_and_exec_encode!(u8, leb128, output_buf),
            UInt16 => convert_and_exec_encode!(u16, leb128, output_buf),
            UInt32 => convert_and_exec_encode!(u32, leb128, output_buf),
            UInt64 => convert_and_exec_encode!(u64, leb128, output_buf),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "Leb128 requires an unsigned integer type, but got {:?}",
                dtype
            ))),
        },
        BitPack { bit_width } => match dtype {
            UInt8 => convert_and_exec_encode!(u8, bitpack, output_buf, *bit_width),
            UInt16 => convert_and_exec_encode!(u16, bitpack, output_buf, *bit_width),
            UInt32 => convert_and_exec_encode!(u32, bitpack, output_buf, *bit_width),
            UInt64 => convert_and_exec_encode!(u64, bitpack, output_buf, *bit_width),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "BitPack requires an unsigned integer type, but got {:?}",
                dtype
            ))),
        },
        Shuffle => match dtype {
            Int16 | UInt16 => convert_and_exec_encode!(u16, shuffle, output_buf),
            Int32 | UInt32 | Float32 => convert_and_exec_encode!(u32, shuffle, output_buf),
            Int64 | UInt64 | Float64 => convert_and_exec_encode!(u64, shuffle, output_buf),
            Int8 | UInt8 | Boolean => {
                // No-op for 1-byte types
                output_buf.clear();
                output_buf.extend_from_slice(input_bytes);
                Ok(())
            }
        },
        Dictionary => match dtype {
            Int8 => convert_and_exec_encode!(i8, dictionary, output_buf),
            UInt8 | Boolean => convert_and_exec_encode!(u8, dictionary, output_buf),
            Int16 => convert_and_exec_encode!(i16, dictionary, output_buf),
            UInt16 => convert_and_exec_encode!(u16, dictionary, output_buf),
            Int32 => convert_and_exec_encode!(i32, dictionary, output_buf),
            UInt32 => convert_and_exec_encode!(u32, dictionary, output_buf),
            Int64 => convert_and_exec_encode!(i64, dictionary, output_buf),
            UInt64 => convert_and_exec_encode!(u64, dictionary, output_buf),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "Dictionary not supported for {:?}",
                dtype
            ))),
        },
        CanonicalizeZeros => match dtype {
            Float32 => convert_and_exec_encode!(f32, canonicalize, output_buf),
            Float64 => convert_and_exec_encode!(f64, canonicalize, output_buf),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "CanonicalizeZeros is only for float types, but got {:?}",
                dtype
            ))),
        },
        // --- Kernels that operate on raw byte streams ---
        Zstd { level } => {
            // Call the new encode function, which returns a Result<Vec<u8>>.
            let compressed_data = zstd::encode(input_bytes, *level)?;

            // Assign the new, compressed vector to the caller's output buffer.
            *output_buf = compressed_data;

            // The block must resolve to Ok(()).
            Ok(())
        }
        Ans => {
            // Call the new encode function, which returns a Result containing a Vec<u8>.
            // The '?' operator will propagate any error or unwrap the successful Ok value.
            let compressed_data = ans::encode(input_bytes)?;

            // Assign the new, compressed vector to the caller's output buffer.
            // The '*' dereferences the mutable reference to perform the assignment.
            *output_buf = compressed_data;

            // The block must resolve to the expected return type, which is likely Ok(()).
            Ok(())
        }

        // --- Kernels with special generic dispatch ---
        BitCast { to_type } => match (dtype, *to_type) {
            (Float32, UInt32) => bitcast::encode::<f32, u32>(input_bytes, output_buf),
            (Float64, UInt64) => bitcast::encode::<f64, u64>(input_bytes, output_buf),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "Unsupported BitCast from {:?} to {:?}",
                dtype, to_type
            ))),
        },

        // Meta-operations are handled by the executor, not dispatched.
        Sparsify { .. } | ExtractNulls { .. } => Err(PhoenixError::InternalError(
            "Meta-operations like Sparsify cannot be dispatched to a kernel.".to_string(),
        )),
    }
}

/// The single, unified dispatcher for all DECODE operations.
pub fn dispatch_decode(
    op: &Operation,
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    current_dtype: PhoenixDataType, // The type of the input_bytes
    target_dtype: PhoenixDataType,  // The type we expect to GET BACK
    num_values: usize,
) -> Result<(), PhoenixError> {
    #[cfg(debug_assertions)]
    {
        println!(
            "[DISPATCH-DECODE] Received: Op: {:?}, In-Type: {:?}, Target-Type: {:?}, In-Len: {}",
            op,
            current_dtype,
            target_dtype,
            input_bytes.len()
        );
    }
    // --- END CHECKPOINT ---

    macro_rules! convert_and_exec_decode {
        ($T:ty, $kernel:ident, $($args:expr),*) => {{
            if input_bytes.len() % std::mem::size_of::<$T>() != 0 {
                return Err(PhoenixError::BufferMismatch(input_bytes.len(), std::mem::size_of::<$T>()));
            }
            let typed_slice: &[$T] = bytemuck::try_cast_slice(input_bytes)?;
            $kernel::decode(typed_slice, $($args),*)
        }};
    }

    use Operation::*;
    use PhoenixDataType::*;

    match op {
        Delta { order } => match target_dtype {
            Int8 => delta::decode::<i8>(input_bytes, output_buf, *order),
            UInt8 => delta::decode::<u8>(input_bytes, output_buf, *order),
            Int16 => delta::decode::<i16>(input_bytes, output_buf, *order),
            UInt16 => delta::decode::<u16>(input_bytes, output_buf, *order),
            Int32 => delta::decode::<i32>(input_bytes, output_buf, *order),
            UInt32 => delta::decode::<u32>(input_bytes, output_buf, *order),
            Int64 => delta::decode::<i64>(input_bytes, output_buf, *order),
            UInt64 => delta::decode::<u64>(input_bytes, output_buf, *order),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "Delta not supported for {:?}",
                target_dtype
            ))),
        },
        XorDelta => match target_dtype {
            UInt32 => xor_delta::decode::<u32>(input_bytes, output_buf),
            UInt64 => xor_delta::decode::<u64>(input_bytes, output_buf),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "XorDelta not supported for {:?}",
                target_dtype
            ))),
        },
        Rle => match target_dtype {
            Int8 => rle::decode::<i8>(input_bytes, output_buf, num_values),
            UInt8 | Boolean => rle::decode::<u8>(input_bytes, output_buf, num_values),
            Int16 => rle::decode::<i16>(input_bytes, output_buf, num_values),
            UInt16 => rle::decode::<u16>(input_bytes, output_buf, num_values),
            Int32 => rle::decode::<i32>(input_bytes, output_buf, num_values),
            UInt32 => rle::decode::<u32>(input_bytes, output_buf, num_values),
            Int64 => rle::decode::<i64>(input_bytes, output_buf, num_values),
            UInt64 => rle::decode::<u64>(input_bytes, output_buf, num_values),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "RLE not supported for {:?}",
                target_dtype
            ))),
        },
        Leb128 => match target_dtype {
            UInt8 => leb128::decode::<u8>(input_bytes, output_buf, num_values),
            UInt16 => leb128::decode::<u16>(input_bytes, output_buf, num_values),
            UInt32 => leb128::decode::<u32>(input_bytes, output_buf, num_values),
            UInt64 => leb128::decode::<u64>(input_bytes, output_buf, num_values),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "Leb128 requires an unsigned integer type, but got {:?}",
                target_dtype
            ))),
        },
        BitPack { bit_width } => match target_dtype {
            UInt8 => bitpack::decode::<u8>(input_bytes, output_buf, *bit_width, num_values),
            UInt16 => bitpack::decode::<u16>(input_bytes, output_buf, *bit_width, num_values),
            UInt32 => bitpack::decode::<u32>(input_bytes, output_buf, *bit_width, num_values),
            UInt64 => bitpack::decode::<u64>(input_bytes, output_buf, *bit_width, num_values),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "BitPack requires an unsigned integer type, but got {:?}",
                target_dtype
            ))),
        },
        Shuffle => match target_dtype {
            Int16 | UInt16 => shuffle::decode::<u16>(input_bytes, output_buf),
            Int32 | UInt32 | Float32 => shuffle::decode::<u32>(input_bytes, output_buf),
            Int64 | UInt64 | Float64 => shuffle::decode::<u64>(input_bytes, output_buf),
            Int8 | UInt8 | Boolean => {
                output_buf.clear();
                output_buf.extend_from_slice(input_bytes);
                Ok(())
            }
        },
        Dictionary => match target_dtype {
            Int8 => dictionary::decode::<i8>(input_bytes, output_buf, num_values),
            UInt8 | Boolean => dictionary::decode::<u8>(input_bytes, output_buf, num_values),
            Int16 => dictionary::decode::<i16>(input_bytes, output_buf, num_values),
            UInt16 => dictionary::decode::<u16>(input_bytes, output_buf, num_values),
            Int32 => dictionary::decode::<i32>(input_bytes, output_buf, num_values),
            UInt32 => dictionary::decode::<u32>(input_bytes, output_buf, num_values),
            Int64 => dictionary::decode::<i64>(input_bytes, output_buf, num_values),
            UInt64 => dictionary::decode::<u64>(input_bytes, output_buf, num_values),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "Dictionary not supported for {:?}",
                target_dtype
            ))),
        },
        Zstd { .. } => {
            // Call the new decode function, which returns a Result<Vec<u8>>.
            let decompressed_data = zstd::decode(input_bytes)?;

            // Assign the successfully decompressed data to the output buffer.
            *output_buf = decompressed_data;

            // The block must resolve to Ok(()).
            Ok(())
        }
        Ans => {
            // Call the new decode function, which returns a Result<Vec<u8>, ...>
            // The '?' will handle the error case, propagating it up.
            let decompressed_data = ans::decode(input_bytes)?;

            // Assign the successfully decompressed data to your output buffer.
            // The '*' dereferences the mutable reference.
            *output_buf = decompressed_data;

            // The block must resolve to Ok(()) to match the expected return type.
            Ok(())
        }
        CanonicalizeZeros => canonicalize::decode(input_bytes, output_buf),
        ZigZag => match (current_dtype, target_dtype) {
            (UInt8, Int8) => convert_and_exec_decode!(u8, zigzag, output_buf),
            (UInt16, Int16) => convert_and_exec_decode!(u16, zigzag, output_buf),
            (UInt32, Int32) => convert_and_exec_decode!(u32, zigzag, output_buf),
            (UInt64, Int64) => convert_and_exec_decode!(u64, zigzag, output_buf),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "Unsupported ZigZag decode from {:?} to {:?}",
                current_dtype, target_dtype
            ))),
        },
        BitCast { .. } => match (current_dtype, target_dtype) {
            (UInt32, Float32) => bitcast::decode::<u32, f32>(input_bytes, output_buf),
            (UInt64, Float64) => bitcast::decode::<u64, f64>(input_bytes, output_buf),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "Unsupported BitCast decode from {:?} to {:?}",
                current_dtype, target_dtype
            ))),
        },
        Sparsify { .. } | ExtractNulls { .. } => Err(PhoenixError::InternalError(
            "Meta-operations like Sparsify cannot be dispatched to a kernel.".to_string(),
        )),
    }
}

//==================================================================================
// 3. Sparsity Dispatchers (Refactored for Type Safety)
//==================================================================================

/// Dispatches a `split_stream` call to the correct typed kernel.
pub fn dispatch_split_stream(
    input_bytes: &[u8],
    dtype: PhoenixDataType,
) -> Result<(Vec<bool>, Vec<u8>), PhoenixError> {
    macro_rules! handle_sparsity_split {
        ($T:ty) => {{
            let typed_slice: &[$T] = bytemuck::try_cast_slice(input_bytes)?;
            let (mask, dense_vec) = sparsity::split_stream(typed_slice)?;
            Ok((mask, typed_slice_to_bytes(&dense_vec)))
        }};
    }
    use PhoenixDataType::*;
    match dtype {
        Int8 => handle_sparsity_split!(i8),
        Int16 => handle_sparsity_split!(i16),
        Int32 => handle_sparsity_split!(i32),
        Int64 => handle_sparsity_split!(i64),
        UInt8 => handle_sparsity_split!(u8),
        UInt16 => handle_sparsity_split!(u16),
        UInt32 => handle_sparsity_split!(u32),
        UInt64 => handle_sparsity_split!(u64),
        Float32 => handle_sparsity_split!(f32),
        Float64 => handle_sparsity_split!(f64),
        _ => Err(PhoenixError::UnsupportedType(format!(
            "Sparsify is not supported for type {:?}",
            dtype
        ))),
    }
}

/// Dispatches a `reconstruct_stream` call to the correct typed kernel.
pub fn dispatch_reconstruct_stream(
    mask: &[bool],
    dense_data_bytes: &[u8],
    dtype: PhoenixDataType,
    total_len: usize,
) -> Result<Vec<u8>, PhoenixError> {
    macro_rules! handle_sparsity_reconstruct {
        ($T:ty) => {{
            let typed_values: &[$T] = bytemuck::try_cast_slice(dense_data_bytes)?;
            let reconstructed_vec = sparsity::reconstruct_stream(mask, typed_values, total_len)?;
            Ok(typed_slice_to_bytes(&reconstructed_vec))
        }};
    }
    use PhoenixDataType::*;
    match dtype {
        Int8 => handle_sparsity_reconstruct!(i8),
        Int16 => handle_sparsity_reconstruct!(i16),
        Int32 => handle_sparsity_reconstruct!(i32),
        Int64 => handle_sparsity_reconstruct!(i64),
        UInt8 => handle_sparsity_reconstruct!(u8),
        UInt16 => handle_sparsity_reconstruct!(u16),
        UInt32 => handle_sparsity_reconstruct!(u32),
        UInt64 => handle_sparsity_reconstruct!(u64),
        Float32 => handle_sparsity_reconstruct!(f32),
        Float64 => handle_sparsity_reconstruct!(f64),
        _ => Err(PhoenixError::UnsupportedType(format!(
            "Desparsify is not supported for type {:?}",
            dtype
        ))),
    }
}

//==================================================================================
// 4. Unit Tests (Refactored for New API)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{pipeline::TypeTransformer, utils::typed_slice_to_bytes};

    #[test]
    fn test_dispatch_zigzag_roundtrip_logic() {
        let original_data: Vec<i32> = vec![-1, 2, -3];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let op = Operation::ZigZag;
        let original_type = PhoenixDataType::Int32;

        // --- Encode ---
        let mut compressed_buf = Vec::new();
        dispatch_encode(&op, &original_bytes, &mut compressed_buf, original_type).unwrap();

        // --- Decode ---
        let compressed_type = op.transform_type(original_type).unwrap();
        let mut decompressed_buf = Vec::new();
        dispatch_decode(
            &op,
            &compressed_buf,
            &mut decompressed_buf,
            compressed_type, // current_dtype is UInt32
            original_type,   // target_dtype is Int32
            original_data.len(),
        )
        .unwrap();

        assert_eq!(decompressed_buf, original_bytes);
    }

    #[test]
    fn test_dispatch_bitpack_roundtrip() {
        let original_data: Vec<u32> = vec![1, 2, 3, 4, 5];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let op = Operation::BitPack { bit_width: 3 };
        let original_type = PhoenixDataType::UInt32;

        let mut compressed_buf = Vec::new();
        dispatch_encode(&op, &original_bytes, &mut compressed_buf, original_type).unwrap();

        let compressed_type = op.transform_type(original_type).unwrap();
        let mut decompressed_buf = Vec::new();
        dispatch_decode(
            &op,
            &compressed_buf,
            &mut decompressed_buf,
            compressed_type, // current_dtype is UInt32
            original_type,   // target_dtype is UInt32
            original_data.len(),
        )
        .unwrap();

        assert_eq!(decompressed_buf, original_bytes);
    }

    #[test]
    fn test_dispatch_unsupported_type_error() {
        let original_data: Vec<f32> = vec![1.0, 2.0, 3.0];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let mut output_buf = Vec::new();

        let op = Operation::ZigZag; // ZigZag doesn't support floats

        let result = dispatch_encode(
            &op,
            &original_bytes,
            &mut output_buf,
            PhoenixDataType::Float32,
        );
        assert!(result.is_err());
        if let Err(PhoenixError::UnsupportedType(msg)) = result {
            assert!(msg.contains("ZigZag"));
            assert!(msg.contains("Float32"));
        } else {
            panic!("Expected UnsupportedType error, but got {:?}", result);
        }
    }

    #[test]
    fn test_dispatch_dictionary_roundtrip() {
        let original_data: Vec<i32> = vec![10, 20, 10, 30, 20, 20];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let op = Operation::Dictionary;
        let original_type = PhoenixDataType::Int32;

        // --- Encode ---
        let mut compressed_buf = Vec::new();
        dispatch_encode(&op, &original_bytes, &mut compressed_buf, original_type).unwrap();
        assert!(!compressed_buf.is_empty());

        // --- Decode ---
        let compressed_type = op.transform_type(original_type).unwrap();
        let mut decompressed_buf = Vec::new();
        dispatch_decode(
            &op,
            &compressed_buf,
            &mut decompressed_buf,
            compressed_type, // current_dtype is Int32
            original_type,   // target_dtype is Int32
            original_data.len(),
        )
        .unwrap();

        assert_eq!(decompressed_buf, original_bytes);
    }
}
