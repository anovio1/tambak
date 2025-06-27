//! This module serves as the public API and dispatcher for the collection of all
//! pure, stateless compression and decompression kernels.
//!
//! Its primary responsibility is to centralize all type-casting logic. It takes
//! raw byte buffers from the executor, safely converts them to the appropriate
//! typed slices, and then calls the correct generic kernel implementation.

use serde_json::Value;
use crate::error::PhoenixError;
use crate::utils::safe_bytes_to_typed_slice;

//==================================================================================
// 1. Module Declarations
//==================================================================================

pub mod delta;
pub mod rle;
pub mod zigzag;
pub mod leb128;
pub mod sleb128;
pub mod bitpack;
pub mod shuffle;
pub mod zstd;

//==================================================================================
// 2. Public API (Unified Dispatchers - with CORRECTED Two-Matcher Macro)
//==================================================================================

/// A private macro to eliminate boilerplate for dispatching to generic kernels
/// that operate on a typed slice. This is the heart of the dispatcher.
macro_rules! dispatch_by_type {
    // --- MATCHER 1: For kernels that take NO extra parameters ---
    ($kernel:ident, $func:ident, $input:expr, $output:expr, $type_str:expr) => {
        match $type_str {
            "Int8" => $kernel::$func(safe_bytes_to_typed_slice::<i8>($input)?, $output),
            "UInt8" | "Boolean" => $kernel::$func(safe_bytes_to_typed_slice::<u8>($input)?, $output),
            "Int16" => $kernel::$func(safe_bytes_to_typed_slice::<i16>($input)?, $output),
            "UInt16" => $kernel::$func(safe_bytes_to_typed_slice::<u16>($input)?, $output),
            "Int32" => $kernel::$func(safe_bytes_to_typed_slice::<i32>($input)?, $output),
            "UInt32" => $kernel::$func(safe_bytes_to_typed_slice::<u32>($input)?, $output),
            "Int64" => $kernel::$func(safe_bytes_to_typed_slice::<i64>($input)?, $output),
            "UInt64" => $kernel::$func(safe_bytes_to_typed_slice::<u64>($input)?, $output),
            _ => Err(PhoenixError::UnsupportedType(format!("Unsupported type for this operation: {}", $type_str)))
        }
    };
    // --- MATCHER 2: For kernels that DO take extra parameters ---
    ($kernel:ident, $func:ident, $input:expr, $output:expr, $type_str:expr, $($params:expr),*) => {
        match $type_str {
            "Int8" => $kernel::$func(safe_bytes_to_typed_slice::<i8>($input)?, $output, $($params),*),
            "UInt8" | "Boolean" => $kernel::$func(safe_bytes_to_typed_slice::<u8>($input)?, $output, $($params),*),
            "Int16" => $kernel::$func(safe_bytes_to_typed_slice::<i16>($input)?, $output, $($params),*),
            "UInt16" => $kernel::$func(safe_bytes_to_typed_slice::<u16>($input)?, $output, $($params),*),
            "Int32" => $kernel::$func(safe_bytes_to_typed_slice::<i32>($input)?, $output, $($params),*),
            "UInt32" => $kernel::$func(safe_bytes_to_typed_slice::<u32>($input)?, $output, $($params),*),
            "Int64" => $kernel::$func(safe_bytes_to_typed_slice::<i64>($input)?, $output, $($params),*),
            "UInt64" => $kernel::$func(safe_bytes_to_typed_slice::<u64>($input)?, $output, $($params),*),
            _ => Err(PhoenixError::UnsupportedType(format!("Unsupported type for this operation: {}", $type_str)))
        }
    };
}

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
            dispatch_by_type!(delta, encode, input_bytes, output_buf, type_str, order)
        },
        "rle" => dispatch_by_type!(rle, encode, input_bytes, output_buf, type_str),
        "zigzag" => dispatch_by_type!(zigzag, encode, input_bytes, output_buf, type_str),
        "leb128" => dispatch_by_type!(leb128, encode, input_bytes, output_buf, type_str),
        "sleb128" => dispatch_by_type!(sleb128, encode, input_bytes, output_buf, type_str),
        "bitpack" => {
            let bit_width = params["bit_width"].as_u64().unwrap_or(0) as u8;
            dispatch_by_type!(bitpack, encode, input_bytes, output_buf, type_str, bit_width)
        },
        "shuffle" => dispatch_by_type!(shuffle, encode, input_bytes, output_buf, type_str),
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
        "delta" => dispatch_by_type!(delta, decode, input_bytes, output_buf, type_str, num_values),
        "rle" => dispatch_by_type!(rle, decode, input_bytes, output_buf, type_str),
        "zigzag" => dispatch_by_type!(zigzag, decode, input_bytes, output_buf, type_str),
        "leb128" => dispatch_by_type!(leb128, decode, input_bytes, output_buf, type_str, num_values),
        "sleb128" => dispatch_by_type!(sleb128, decode, input_bytes, output_buf, type_str, num_values),
        "bitpack" => {
            let bit_width = params["bit_width"].as_u64().unwrap_or(0) as u8;
            dispatch_by_type!(bitpack, decode, input_bytes, output_buf, type_str, bit_width, num_values)
        },
        "shuffle" => dispatch_by_type!(shuffle, decode, input_bytes, output_buf, type_str),
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

    #[test]
    fn test_dispatch_rle_no_params() {
        let original_data: Vec<u16> = vec![7, 7, 7];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let op_config = json!({"op": "rle"});

        let mut compressed_buf = Vec::new();
        // This call now correctly matches the first arm of the macro
        dispatch_encode(&op_config, &original_bytes, &mut compressed_buf, "UInt16").unwrap();
        
        let mut decompressed_buf = Vec::new();
        dispatch_decode(&op_config, &compressed_buf, &mut decompressed_buf, "UInt16", original_data.len()).unwrap();

        assert_eq!(decompressed_buf, original_bytes);
    }

    #[test]
    fn test_dispatch_bitpack_with_params() {
        let original_data: Vec<u32> = vec![1, 2, 3, 4, 5];
        let original_bytes = typed_slice_to_bytes(&original_data);
        let op_config = json!({"op": "bitpack", "params": {"bit_width": 3}});

        let mut compressed_buf = Vec::new();
        // This call now correctly matches the second arm of the macro
        dispatch_encode(&op_config, &original_bytes, &mut compressed_buf, "UInt32").unwrap();

        let mut decompressed_buf = Vec::new();
        dispatch_decode(&op_config, &compressed_buf, &mut decompressed_buf, "UInt32", original_data.len()).unwrap();

        assert_eq!(decompressed_buf, original_bytes);
    }
}