//! This module serves as the public API and dispatcher for the collection of all
//! pure, stateless compression and decompression kernels.
//!
//! It declares all kernel sub-modules and provides a single, unified `dispatch`
//! function for both encoding and decoding. This dispatcher is the sole entry
//! point for the `pipeline::executor`. It takes an operation definition from the
//! pipeline plan and calls the appropriate generic kernel implementation.

use serde_json::Value;
use crate::error::PhoenixError;
use crate::utils::safe_bytes_to_typed_slice;

//==================================================================================
// 1. Module Declarations
//==================================================================================

/// Layer 1: Value Reduction
pub mod delta;

/// Layer 2: Sparsity Exploitation
pub mod rle;

/// Layer 3: Bit-Width Reduction
pub mod zigzag;
pub mod leb128;
// pub mod sleb128;
pub mod bitpack;

/// Layer 4: Byte Distribution
pub mod shuffle;

/// Final Stage: Entropy Coding
pub mod zstd;


//==================================================================================
// 2. Public API (Unified Dispatchers - CORRECTED without faulty macro)
//==================================================================================

/// The single, unified dispatcher for all ENCODE operations.
pub fn dispatch_encode(
    op_config: &Value,
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    type_str: &str,
) -> Result<(), PhoenixError> {
    let op = op_config["op"].as_str().ok_or_else(|| PhoenixError::UnsupportedType("Missing 'op' in pipeline step".to_string()))?;
    let params = op_config.get("params").unwrap_or(&Value::Null);

    match op {
        "delta" => {
            let order = params["order"].as_u64().unwrap_or(1) as usize;
            match type_str {
                "Int8" => delta::encode(safe_bytes_to_typed_slice::<i8>(input_bytes)?, output_buf, order),
                "UInt8" => delta::encode(safe_bytes_to_typed_slice::<u8>(input_bytes)?, output_buf, order),
                "Int16" => delta::encode(safe_bytes_to_typed_slice::<i16>(input_bytes)?, output_buf, order),
                "UInt16" => delta::encode(safe_bytes_to_typed_slice::<u16>(input_bytes)?, output_buf, order),
                "Int32" => delta::encode(safe_bytes_to_typed_slice::<i32>(input_bytes)?, output_buf, order),
                "UInt32" => delta::encode(safe_bytes_to_typed_slice::<u32>(input_bytes)?, output_buf, order),
                "Int64" => delta::encode(safe_bytes_to_typed_slice::<i64>(input_bytes)?, output_buf, order),
                "UInt64" => delta::encode(safe_bytes_to_typed_slice::<u64>(input_bytes)?, output_buf, order),
                _ => Err(PhoenixError::UnsupportedType(type_str.to_string())),
            }
        },
        "rle" => {
            match type_str {
                "Int8" => rle::encode(safe_bytes_to_typed_slice::<i8>(input_bytes)?, output_buf),
                "UInt8" | "Boolean" => rle::encode(safe_bytes_to_typed_slice::<u8>(input_bytes)?, output_buf),
                "Int16" => rle::encode(safe_bytes_to_typed_slice::<i16>(input_bytes)?, output_buf),
                "UInt16" => rle::encode(safe_bytes_to_typed_slice::<u16>(input_bytes)?, output_buf),
                "Int32" => rle::encode(safe_bytes_to_typed_slice::<i32>(input_bytes)?, output_buf),
                "UInt32" => rle::encode(safe_bytes_to_typed_slice::<u32>(input_bytes)?, output_buf),
                "Int64" => rle::encode(safe_bytes_to_typed_slice::<i64>(input_bytes)?, output_buf),
                "UInt64" => rle::encode(safe_bytes_to_typed_slice::<u64>(input_bytes)?, output_buf),
                _ => Err(PhoenixError::UnsupportedType(type_str.to_string())),
            }
        },
        "zigzag" => {
            // ZigZag encode ONLY operates on SIGNED integers.
            match type_str {
                "Int8" => zigzag::encode(safe_bytes_to_typed_slice::<i8>(input_bytes)?, output_buf),
                "Int16" => zigzag::encode(safe_bytes_to_typed_slice::<i16>(input_bytes)?, output_buf),
                "Int32" => zigzag::encode(safe_bytes_to_typed_slice::<i32>(input_bytes)?, output_buf),
                "Int64" => zigzag::encode(safe_bytes_to_typed_slice::<i64>(input_bytes)?, output_buf),
                _ => Err(PhoenixError::UnsupportedType(format!("ZigZag encode requires a signed integer type, but got {}", type_str))),
            }
        },
        "leb128" | "bitpack" => {
            // LEB128 and BitPack ONLY operate on UNSIGNED integers.
            let bit_width = params["bit_width"].as_u64().unwrap_or(0) as u8;
            match type_str {
                "UInt8" => bitpack::encode(safe_bytes_to_typed_slice::<u8>(input_bytes)?, output_buf, bit_width),
                "UInt16" => bitpack::encode(safe_bytes_to_typed_slice::<u16>(input_bytes)?, output_buf, bit_width),
                "UInt32" => bitpack::encode(safe_bytes_to_typed_slice::<u32>(input_bytes)?, output_buf, bit_width),
                "UInt64" => bitpack::encode(safe_bytes_to_typed_slice::<u64>(input_bytes)?, output_buf, bit_width),
                _ => Err(PhoenixError::UnsupportedType(format!("{} requires an unsigned integer type, but got {}", op, type_str))),
            }
        },
        "shuffle" => {
            match type_str {
                "Int16" => shuffle::encode(safe_bytes_to_typed_slice::<i16>(input_bytes)?, output_buf),
                "UInt16" => shuffle::encode(safe_bytes_to_typed_slice::<u16>(input_bytes)?, output_buf),
                "Int32" => shuffle::encode(safe_bytes_to_typed_slice::<i32>(input_bytes)?, output_buf),
                "UInt32" => shuffle::encode(safe_bytes_to_typed_slice::<u32>(input_bytes)?, output_buf),
                "Int64" => shuffle::encode(safe_bytes_to_typed_slice::<i64>(input_bytes)?, output_buf),
                "UInt64" => shuffle::encode(safe_bytes_to_typed_slice::<u64>(input_bytes)?, output_buf),
                // No-op for 1-byte types, just copy the data.
                "Int8" | "UInt8" | "Boolean" => {
                    output_buf.clear();
                    output_buf.extend_from_slice(input_bytes);
                    Ok(())
                },
                _ => Err(PhoenixError::UnsupportedType(type_str.to_string())),
            }
        },
        "zstd" => {
            let level = params["level"].as_i64().unwrap_or(3) as i32;
            zstd::encode(input_bytes, output_buf, level)
        },
        _ => Err(PhoenixError::UnsupportedType(format!("Unsupported encode op: {}", op))),
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
    let op = op_config["op"].as_str().ok_or_else(|| PhoenixError::UnsupportedType("Missing 'op' in pipeline step".to_string()))?;
    let params = op_config.get("params").unwrap_or(&Value::Null);

    match op {
        "delta" => {
            let order = params["order"].as_u64().unwrap_or(1) as usize;
            match type_str {
                "Int8" => delta::decode(safe_bytes_to_typed_slice::<i8>(input_bytes)?, output_buf, order),
                "UInt8" => delta::decode(safe_bytes_to_typed_slice::<u8>(input_bytes)?, output_buf, order),
                "Int16" => delta::decode(safe_bytes_to_typed_slice::<i16>(input_bytes)?, output_buf, order),
                "UInt16" => delta::decode(safe_bytes_to_typed_slice::<u16>(input_bytes)?, output_buf, order),
                "Int32" => delta::decode(safe_bytes_to_typed_slice::<i32>(input_bytes)?, output_buf, order),
                "UInt32" => delta::decode(safe_bytes_to_typed_slice::<u32>(input_bytes)?, output_buf, order),
                "Int64" => delta::decode(safe_bytes_to_typed_slice::<i64>(input_bytes)?, output_buf, order),
                "UInt64" => delta::decode(safe_bytes_to_typed_slice::<u64>(input_bytes)?, output_buf, order),
                _ => Err(PhoenixError::UnsupportedType(type_str.to_string())),
            }
        },
        "rle" => {
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
        },
        "zigzag" => {
            // ZigZag decode ONLY operates on UNSIGNED integers.
            match type_str {
                "Int8" => zigzag::decode(safe_bytes_to_typed_slice::<u8>(input_bytes)?, output_buf),
                "Int16" => zigzag::decode(safe_bytes_to_typed_slice::<u16>(input_bytes)?, output_buf),
                "Int32" => zigzag::decode(safe_bytes_to_typed_slice::<u32>(input_bytes)?, output_buf),
                "Int64" => zigzag::decode(safe_bytes_to_typed_slice::<u64>(input_bytes)?, output_buf),
                _ => Err(PhoenixError::UnsupportedType(format!("ZigZag decode requires an unsigned integer type, but original type was {}", type_str))),
            }
        },
        "leb128" | "bitpack" => {
            // LEB128 and BitPack ONLY operate on UNSIGNED integers.
            let bit_width = params["bit_width"].as_u64().unwrap_or(0) as u8;
            match type_str {
                "UInt8" => bitpack::decode::<u8>(input_bytes, output_buf, bit_width, num_values),
                "UInt16" => bitpack::decode::<u16>(input_bytes, output_buf, bit_width, num_values),
                "UInt32" => bitpack::decode::<u32>(input_bytes, output_buf, bit_width, num_values),
                "UInt64" => bitpack::decode::<u64>(input_bytes, output_buf, bit_width, num_values),
                _ => Err(PhoenixError::UnsupportedType(format!("{} requires an unsigned integer type, but got {}", op, type_str))),
            }
        },
        "shuffle" => {
            match type_str {
                "Int16" => shuffle::decode::<i16>(input_bytes, output_buf),
                "UInt16" => shuffle::decode::<u16>(input_bytes, output_buf),
                "Int32" => shuffle::decode::<i32>(input_bytes, output_buf),
                "UInt32" => shuffle::decode::<u32>(input_bytes, output_buf),
                "Int64" => shuffle::decode::<i64>(input_bytes, output_buf),
                "UInt64" => shuffle::decode::<u64>(input_bytes, output_buf),
                // No-op for 1-byte types, just copy the data.
                "Int8" | "UInt8" | "Boolean" => {
                    output_buf.clear();
                    output_buf.extend_from_slice(input_bytes);
                    Ok(())
                },
                _ => Err(PhoenixError::UnsupportedType(type_str.to_string())),
            }
        },
        "zstd" => zstd::decode(input_bytes, output_buf),
        _ => Err(PhoenixError::UnsupportedType(format!("Unsupported decode op: {}", op))),
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
    fn test_dispatch_zigzag_signed_only() {
        let original_data: Vec<i32> = vec![-1, 2, -3];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let op_config = json!({"op": "zigzag"});

        let mut compressed_buf = Vec::new();
        dispatch_encode(&op_config, &original_bytes, &mut compressed_buf, "Int32").unwrap();

        let mut decompressed_buf = Vec::new();
        // Note: The type string for decode is the *original* signed type.
        // The dispatcher handles mapping this to the correct unsigned slice type.
        dispatch_decode(&op_config, &compressed_buf, &mut decompressed_buf, "Int32", original_data.len()).unwrap();

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
        dispatch_decode(&op_config, &compressed_buf, &mut decompressed_buf, "UInt32", original_data.len()).unwrap();

        assert_eq!(decompressed_buf, original_bytes);
    }
}