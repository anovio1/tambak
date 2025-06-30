//! This module serves as the public API and dispatcher for the collection of all
//! pure, stateless compression and decompression kernels.
//!
//! It declares all kernel sub-modules and provides a single, unified `dispatch`
//! function for both encoding and decoding. This dispatcher is the sole entry
//! point for the `pipeline::executor`. It takes an operation definition from the
//! pipeline plan and calls the appropriate generic kernel implementation.

use crate::{error::PhoenixError};
use serde_json::Value;

// Helper function to convert input bytes to a typed Vec<T>
fn typed_vec_from_bytes<T: bytemuck::Pod>(input_bytes: &[u8]) -> Result<Vec<T>, PhoenixError> {
    if input_bytes.len() % std::mem::size_of::<T>() != 0 {
        return Err(PhoenixError::BufferMismatch(
            input_bytes.len(),
            std::mem::size_of::<T>(),
        ));
    }
    let slice: &[T] = bytemuck::try_cast_slice(input_bytes)
        .map_err(|_| PhoenixError::BufferMismatch(input_bytes.len(), std::mem::size_of::<T>()))?;
    Ok(slice.to_vec())
}

//==================================================================================
// 1. Module Declarations
//==================================================================================

pub mod ans;
pub mod bitpack;
pub mod delta;
pub mod dictionary;
pub mod leb128;
pub mod rle;
pub mod shuffle;
pub mod xor_delta;
pub mod zigzag;
pub mod zstd;

//==================================================================================
// 2. Public API (Unified Dispatchers - FINAL, CORRECTED VERSION)
//==================================================================================

/// The single, unified dispatcher for all ENCODE operations.
pub fn dispatch_encode(
    op_config: &Value,
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    type_str: &str,
) -> Result<(), PhoenixError> {
    let op = op_config["op"].as_str().ok_or_else(|| {
        PhoenixError::UnsupportedType("Missing 'op' in pipeline step".to_string())
    })?;
    let params = op_config.get("params").unwrap_or(&Value::Null);

    macro_rules! safe_convert_and_exec {
        ($T:ty, $kernel:ident, $($args:expr),*) => {{
            if input_bytes.len() % std::mem::size_of::<$T>() != 0 {
                return Err(PhoenixError::BufferMismatch(input_bytes.len(), std::mem::size_of::<$T>()));
            }
            let typed_vec: Vec<$T> = input_bytes
                .chunks_exact(std::mem::size_of::<$T>())
                .map(|chunk| <$T>::from_le_bytes(chunk.try_into().unwrap()))
                .collect();
            $kernel::encode(&typed_vec, $($args),*)
        }};
    }

    match op {
        "delta" => {
            let order = params["order"].as_u64().unwrap_or(1) as usize;
            match type_str {
                "Int8" => safe_convert_and_exec!(i8, delta, output_buf, order),
                "UInt8" => safe_convert_and_exec!(u8, delta, output_buf, order),
                "Int16" => safe_convert_and_exec!(i16, delta, output_buf, order),
                "UInt16" => safe_convert_and_exec!(u16, delta, output_buf, order),
                "Int32" => safe_convert_and_exec!(i32, delta, output_buf, order),
                "UInt32" => safe_convert_and_exec!(u32, delta, output_buf, order),
                "Int64" => safe_convert_and_exec!(i64, delta, output_buf, order),
                "UInt64" => safe_convert_and_exec!(u64, delta, output_buf, order),
                _ => Err(PhoenixError::UnsupportedType(type_str.to_string())),
            }
        }
        // --- NEW DISPATCH ARM ---
        "xor_delta" => match type_str {
            "Int32" | "UInt32" => safe_convert_and_exec!(u32, xor_delta, output_buf),
            "Int64" | "UInt64" => safe_convert_and_exec!(u64, xor_delta, output_buf),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "XOR Delta not supported for {}",
                type_str
            ))),
        },
        "rle" => {
            // RLE is not part of the float pipeline, no changes needed.
            match type_str {
                "Int8" => safe_convert_and_exec!(i8, rle, output_buf),
                "UInt8" | "Boolean" => safe_convert_and_exec!(u8, rle, output_buf),
                "Int16" => safe_convert_and_exec!(i16, rle, output_buf),
                "UInt16" => safe_convert_and_exec!(u16, rle, output_buf),
                "Int32" => safe_convert_and_exec!(i32, rle, output_buf),
                "UInt32" => safe_convert_and_exec!(u32, rle, output_buf),
                "Int64" => safe_convert_and_exec!(i64, rle, output_buf),
                "UInt64" => safe_convert_and_exec!(u64, rle, output_buf),
                _ => Err(PhoenixError::UnsupportedType(type_str.to_string())),
            }
        }
        "zigzag" | "leb128" | "bitpack" => {
            // --- NEW GUARD ---
            if type_str == "Float32" || type_str == "Float64" {
                return Err(PhoenixError::UnsupportedType(format!(
                    "Operation '{}' is not supported for float type '{}'",
                    op, type_str
                )));
            }
            // Existing integer-only logic follows
            match op {
                "zigzag" => match type_str {
                    "Int8" => safe_convert_and_exec!(i8, zigzag, output_buf),
                    "Int16" => safe_convert_and_exec!(i16, zigzag, output_buf),
                    "Int32" => safe_convert_and_exec!(i32, zigzag, output_buf),
                    "Int64" => safe_convert_and_exec!(i64, zigzag, output_buf),
                    _ => Err(PhoenixError::UnsupportedType(format!(
                        "ZigZag encode requires a signed integer type, but got {}",
                        type_str
                    ))),
                },
                "leb128" => match type_str {
                    "UInt8" => safe_convert_and_exec!(u8, leb128, output_buf),
                    "UInt16" => safe_convert_and_exec!(u16, leb128, output_buf),
                    "UInt32" => safe_convert_and_exec!(u32, leb128, output_buf),
                    "UInt64" => safe_convert_and_exec!(u64, leb128, output_buf),
                    _ => Err(PhoenixError::UnsupportedType(format!(
                        "leb128 requires an unsigned integer type, but got {}",
                        type_str
                    ))),
                },
                "bitpack" => {
                    let bit_width = params["bit_width"].as_u64().ok_or_else(|| {
                        PhoenixError::UnsupportedType(
                            "bitpack requires a 'bit_width' param".to_string(),
                        )
                    })? as u8;
                    match type_str {
                        "UInt8" => safe_convert_and_exec!(u8, bitpack, output_buf, bit_width),
                        "UInt16" => safe_convert_and_exec!(u16, bitpack, output_buf, bit_width),
                        "UInt32" => safe_convert_and_exec!(u32, bitpack, output_buf, bit_width),
                        "UInt64" => safe_convert_and_exec!(u64, bitpack, output_buf, bit_width),
                        _ => Err(PhoenixError::UnsupportedType(format!(
                            "bitpack requires an unsigned integer type, but got {}",
                            type_str
                        ))),
                    }
                }
                _ => unreachable!(), // Should not happen
            }
        }
        "shuffle" => {
            match type_str {
                "Int16" => safe_convert_and_exec!(i16, shuffle, output_buf),
                "UInt16" => safe_convert_and_exec!(u16, shuffle, output_buf),
                "Int32" => safe_convert_and_exec!(i32, shuffle, output_buf),
                "UInt32" => safe_convert_and_exec!(u32, shuffle, output_buf),
                "Int64" => safe_convert_and_exec!(i64, shuffle, output_buf),
                "UInt64" => safe_convert_and_exec!(u64, shuffle, output_buf),
                // --- NEW ---
                "Float32" => safe_convert_and_exec!(f32, shuffle, output_buf),
                "Float64" => safe_convert_and_exec!(f64, shuffle, output_buf),
                "Int8" | "UInt8" | "Boolean" => {
                    output_buf.clear();
                    output_buf.extend_from_slice(input_bytes);
                    Ok(())
                }
                _ => Err(PhoenixError::UnsupportedType(type_str.to_string())),
            }
        }
        "zstd" => {
            let level = params["level"].as_i64().unwrap_or(3) as i32;
            zstd::encode(input_bytes, output_buf, level)
        }
        "dictionary" => match type_str {
            "Int8" => safe_convert_and_exec!(i8, dictionary, output_buf),
            "UInt8" | "Boolean" => safe_convert_and_exec!(u8, dictionary, output_buf),
            "Int16" => safe_convert_and_exec!(i16, dictionary, output_buf),
            "UInt16" => safe_convert_and_exec!(u16, dictionary, output_buf),
            "Int32" => safe_convert_and_exec!(i32, dictionary, output_buf),
            "Int64" => safe_convert_and_exec!(i64, dictionary, output_buf),
            "UInt32" => safe_convert_and_exec!(u32, dictionary, output_buf),
            "UInt64" => safe_convert_and_exec!(u64, dictionary, output_buf),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "Dictionary encoding not supported for {}",
                type_str
            ))),
        },
        "ans" => {
            // This follows the established pattern for raw byte-stream kernels like zstd.
            ans::encode(input_bytes, output_buf)
        }
        _ => Err(PhoenixError::UnsupportedType(format!(
            "Unsupported encode op: {}",
            op
        ))),
    }
}

/// The single, unified dispatcher for all DECODE operations.
pub fn dispatch_decode(
    op_config: &Value,
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    type_str: &str,
    num_values: usize,
) -> Result<(), PhoenixError> {
    let op = op_config["op"].as_str().ok_or_else(|| {
        PhoenixError::UnsupportedType("Missing 'op' in pipeline step".to_string())
    })?;
    let params = op_config.get("params").unwrap_or(&Value::Null);

    // This macro is used for kernels that expect a pre-converted typed slice for decoding (e.g., delta, zigzag).
    macro_rules! safe_convert_and_exec_decode {
        ($T:ty, $kernel:ident, $($args:expr),*) => {{
            if input_bytes.len() % std::mem::size_of::<$T>() != 0 {
                return Err(PhoenixError::BufferMismatch(input_bytes.len(), std::mem::size_of::<$T>()));
            }
            let typed_vec: Vec<$T> = input_bytes
                .chunks_exact(std::mem::size_of::<$T>())
                .map(|chunk| <$T>::from_le_bytes(chunk.try_into().unwrap()))
                .collect();
            $kernel::decode(&typed_vec, $($args),*)
        }};
    }

    match op {
        "delta" => {
            let order = params["order"].as_u64().unwrap_or(1) as usize;
            match type_str {
                "Int8" => delta::decode::<i8>(input_bytes, output_buf, order),
                "UInt8" => delta::decode::<u8>(input_bytes, output_buf, order),
                "Int16" => delta::decode::<i16>(input_bytes, output_buf, order),
                "UInt16" => delta::decode::<u16>(input_bytes, output_buf, order),
                "Int32" => delta::decode::<i32>(input_bytes, output_buf, order),
                "UInt32" => delta::decode::<u32>(input_bytes, output_buf, order),
                "Int64" => delta::decode::<i64>(input_bytes, output_buf, order),
                "UInt64" => delta::decode::<u64>(input_bytes, output_buf, order),
                _ => Err(PhoenixError::UnsupportedType(type_str.to_string())),
            }
        }
        // --- NEW DISPATCH ARM ---
        "xor_delta" => match type_str {
            "Int32" | "UInt32" => xor_delta::decode::<u32>(input_bytes, output_buf),
            "Int64" | "UInt64" => xor_delta::decode::<u64>(input_bytes, output_buf),
            _ => Err(PhoenixError::UnsupportedType(format!(
                "XOR Delta not supported for {}",
                type_str
            ))),
        },
        "rle" => {
            // This kernel is not in the float pipeline, no changes needed.
            match type_str {
                "Int8" => rle::decode::<i8>(input_bytes, output_buf, num_values),
                "UInt8" | "Boolean" => rle::decode::<u8>(input_bytes, output_buf, num_values),
                "Int16" => rle::decode::<i16>(input_bytes, output_buf, num_values),
                "UInt16" => rle::decode::<u16>(input_bytes, output_buf, num_values),
                "Int32" => rle::decode::<i32>(input_bytes, output_buf, num_values),
                "UInt32" => rle::decode::<u32>(input_bytes, output_buf, num_values),
                "Int64" => rle::decode::<i64>(input_bytes, output_buf, num_values),
                "UInt64" => rle::decode::<u64>(input_bytes, output_buf, num_values),
                _ => Err(PhoenixError::UnsupportedType(type_str.to_string())),
            }
        }
        "zigzag" | "leb128" | "bitpack" => {
            // --- NEW GUARD ---
            if type_str == "Float32" || type_str == "Float64" {
                return Err(PhoenixError::UnsupportedType(format!(
                    "Operation '{}' is not supported for float type '{}'",
                    op, type_str
                )));
            }
            // Existing integer-only logic follows
            match op {
                "zigzag" => match type_str {
                    "Int8" => safe_convert_and_exec_decode!(u8, zigzag, output_buf),
                    "Int16" => safe_convert_and_exec_decode!(u16, zigzag, output_buf),
                    "Int32" => safe_convert_and_exec_decode!(u32, zigzag, output_buf),
                    "Int64" => safe_convert_and_exec_decode!(u64, zigzag, output_buf),
                    _ => Err(PhoenixError::UnsupportedType(format!(
                        "ZigZag decode requires a signed integer type string, but got {}",
                        type_str
                    ))),
                },
                "leb128" => match type_str {
                    "UInt8" => leb128::decode::<u8>(input_bytes, output_buf, num_values),
                    "UInt16" => leb128::decode::<u16>(input_bytes, output_buf, num_values),
                    "UInt32" => leb128::decode::<u32>(input_bytes, output_buf, num_values),
                    "UInt64" => leb128::decode::<u64>(input_bytes, output_buf, num_values),
                    _ => Err(PhoenixError::UnsupportedType(format!(
                        "leb128 requires an unsigned integer type, but got {}",
                        type_str
                    ))),
                },
                "bitpack" => {
                    let bit_width = params["bit_width"].as_u64().ok_or_else(|| {
                        PhoenixError::UnsupportedType(
                            "bitpack requires a 'bit_width' param".to_string(),
                        )
                    })? as u8;
                    match type_str {
                        "UInt8" => {
                            bitpack::decode::<u8>(input_bytes, output_buf, bit_width, num_values)
                        }
                        "UInt16" => {
                            bitpack::decode::<u16>(input_bytes, output_buf, bit_width, num_values)
                        }
                        "UInt32" => {
                            bitpack::decode::<u32>(input_bytes, output_buf, bit_width, num_values)
                        }
                        "UInt64" => {
                            bitpack::decode::<u64>(input_bytes, output_buf, bit_width, num_values)
                        }
                        _ => Err(PhoenixError::UnsupportedType(format!(
                            "bitpack requires an unsigned integer type, but got {}",
                            type_str
                        ))),
                    }
                }
                _ => unreachable!(),
            }
        }
        "shuffle" => {
            // This kernel takes raw bytes, so we call it directly without the macro.
            match type_str {
                "Int16" => shuffle::decode::<i16>(input_bytes, output_buf),
                "UInt16" => shuffle::decode::<u16>(input_bytes, output_buf),
                "Int32" => shuffle::decode::<i32>(input_bytes, output_buf),
                "UInt32" => shuffle::decode::<u32>(input_bytes, output_buf),
                "Int64" => shuffle::decode::<i64>(input_bytes, output_buf),
                "UInt64" => shuffle::decode::<u64>(input_bytes, output_buf),
                // --- NEW ---
                "Float32" => shuffle::decode::<f32>(input_bytes, output_buf),
                "Float64" => shuffle::decode::<f64>(input_bytes, output_buf),
                "Int8" | "UInt8" | "Boolean" => {
                    output_buf.clear();
                    output_buf.extend_from_slice(input_bytes);
                    Ok(())
                }
                _ => Err(PhoenixError::UnsupportedType(type_str.to_string())),
            }
        }
        "zstd" => zstd::decode(input_bytes, output_buf),
        "dictionary" => {
            // This follows the established pattern for type-aware decode kernels.
            match type_str {
                "Int8" => dictionary::decode::<i8>(input_bytes, output_buf, num_values),
                "UInt8" | "Boolean" => {
                    dictionary::decode::<u8>(input_bytes, output_buf, num_values)
                }
                "Int16" => dictionary::decode::<i16>(input_bytes, output_buf, num_values),
                "UInt16" => dictionary::decode::<u16>(input_bytes, output_buf, num_values),
                "Int32" => dictionary::decode::<i32>(input_bytes, output_buf, num_values),
                "Int64" => dictionary::decode::<i64>(input_bytes, output_buf, num_values),
                "UInt32" => dictionary::decode::<u32>(input_bytes, output_buf, num_values),
                "UInt64" => dictionary::decode::<u64>(input_bytes, output_buf, num_values),
                _ => Err(PhoenixError::UnsupportedType(format!(
                    "Dictionary decoding not supported for {}",
                    type_str
                ))),
            }
        }
        "ans" => {
            // This follows the established pattern for raw byte-stream kernels like zstd.
            ans::decode(input_bytes, output_buf, num_values)
        }
        _ => Err(PhoenixError::UnsupportedType(format!(
            "Unsupported decode op: {}",
            op
        ))),
    }
}

//==================================================================================
// 3. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::typed_slice_to_bytes;
    use serde_json::json;

    #[test]
    fn test_dispatch_zigzag_roundtrip_logic() {
        let original_data: Vec<i32> = vec![-1, 2, -3];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let op_config = json!({"op": "zigzag"});

        // --- Encode ---
        let mut compressed_buf = Vec::new();
        dispatch_encode(&op_config, &original_bytes, &mut compressed_buf, "Int32").unwrap();

        // --- Decode ---
        let mut decompressed_buf = Vec::new();
        // The type string for decode is the *signed* type we want to get back.
        dispatch_decode(
            &op_config,
            &compressed_buf,
            &mut decompressed_buf,
            "Int32",
            original_data.len(),
        )
        .unwrap();

        assert_eq!(decompressed_buf, original_bytes);
    }

    #[test]
    fn test_dispatch_bitpack_unsigned_only() {
        let original_data: Vec<u32> = vec![1, 2, 3, 4, 5];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let op_config = json!({"op": "bitpack", "params": {"bit_width": 3}});

        let mut compressed_buf = Vec::new();
        dispatch_encode(&op_config, &original_bytes, &mut compressed_buf, "UInt32").unwrap();

        let mut decompressed_buf = Vec::new();
        dispatch_decode(
            &op_config,
            &compressed_buf,
            &mut decompressed_buf,
            "UInt32",
            original_data.len(),
        )
        .unwrap();

        assert_eq!(decompressed_buf, original_bytes);
    }

    #[test]
    fn test_dispatch_blocks_floats_from_incompatible_kernels() {
        let original_data: Vec<f32> = vec![1.0, 2.0, 3.0];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let mut output_buf = Vec::new();

        let incompatible_ops = vec![
            json!({"op": "zigzag"}),
            json!({"op": "leb128"}),
            json!({"op": "bitpack", "params": {"bit_width": 8}}),
        ];

        for op_config in incompatible_ops {
            let result = dispatch_encode(&op_config, &original_bytes, &mut output_buf, "Float32");
            assert!(result.is_err());
            if let Err(PhoenixError::UnsupportedType(msg)) = result {
                let op_name = op_config["op"].as_str().unwrap();
                assert!(msg.contains(op_name));
                assert!(msg.contains("not supported for float"));
            } else {
                panic!("Expected UnsupportedType error, but got {:?}", result);
            }
        }
    }

    #[test]
    fn test_dispatch_dictionary_roundtrip() {
        let original_data: Vec<i32> = vec![10, 20, 10, 30, 20, 20];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let op_config = json!({"op": "dictionary"});

        // --- Encode ---
        let mut compressed_buf = Vec::new();
        dispatch_encode(&op_config, &original_bytes, &mut compressed_buf, "Int32").unwrap();
        assert!(!compressed_buf.is_empty());

        // --- Decode ---
        let mut decompressed_buf = Vec::new();
        dispatch_decode(
            &op_config,
            &compressed_buf,
            &mut decompressed_buf,
            "Int32",
            original_data.len(),
        )
        .unwrap();

        assert_eq!(decompressed_buf, original_bytes);
    }

    #[test]
    fn test_dispatch_ans_roundtrip() {
        let original_data: Vec<u8> = b"hello hello hello this is a test".to_vec();
        let op_config = json!({"op": "ans"});

        // --- Encode ---
        let mut compressed_buf = Vec::new();
        // ANS works on raw bytes, so the type string is effectively ignored.
        dispatch_encode(&op_config, &original_data, &mut compressed_buf, "UInt8").unwrap();

        // --- Decode ---
        let mut decompressed_buf = Vec::new();
        dispatch_decode(
            &op_config,
            &compressed_buf,
            &mut decompressed_buf,
            "UInt8",
            original_data.len(),
        )
        .unwrap();

        assert_eq!(decompressed_buf, original_data);
    }
}
