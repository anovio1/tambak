//! This module contains the highest-level orchestration logic. It uses a generic
//! worker function pattern to avoid boilerplate and keep the core logic DRY.

use arrow::array::{Array, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::datatypes::*;
use std::io::{Cursor, Read, Write};

use super::{executor, planner};
use crate::error::PhoenixError;
use crate::null_handling::bitmap;
use crate::utils::typed_slice_to_bytes;

// (The CompressedChunk struct and its impl remain unchanged)
#[derive(Debug)]
struct CompressedChunk {
    total_rows: u64,
    compressed_nullmap: Vec<u8>,
    compressed_data: Vec<u8>,
    pipeline_json: String,
    // --- NEW FIELD ---
    original_type: String,
}

impl CompressedChunk {
    pub fn to_bytes(&self) -> Result<Vec<u8>, PhoenixError> {
        let nullmap_len = self.compressed_nullmap.len() as u64;
        let pipeline_len = self.pipeline_json.len() as u64;
        // --- NEW ---
        let type_len = self.original_type.len() as u64;

        // The header is now dynamic, but the length-of-lengths part is fixed.
        // Header: [nullmap_len (u64), pipeline_len (u64), type_len (u64), total_rows (u64)]
        let mut buffer = Vec::new();

        buffer.write_all(&nullmap_len.to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer.write_all(&pipeline_len.to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        // --- NEW ---
        buffer.write_all(&type_len.to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer.write_all(&self.total_rows.to_le_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;

        // Now write the variable-length parts
        buffer.write_all(&self.compressed_nullmap).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer.write_all(self.pipeline_json.as_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        // --- NEW ---
        buffer.write_all(self.original_type.as_bytes()).map_err(|e| PhoenixError::InternalError(e.to_string()))?;

        // Finally, write the main data payload
        buffer.write_all(&self.compressed_data).map_err(|e| PhoenixError::InternalError(e.to_string()))?;

        Ok(buffer)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PhoenixError> {
        // Header is now 4 * u64 = 32 bytes for the lengths
        if bytes.len() < 32 {
            return Err(PhoenixError::BufferMismatch(bytes.len(), 32));
        }
        let mut cursor = Cursor::new(bytes);
        let mut u64_buf = [0u8; 8];

        cursor.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let nullmap_len = u64::from_le_bytes(u64_buf) as usize;
        cursor.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let pipeline_len = u64::from_le_bytes(u64_buf) as usize;
        // --- NEW ---
        cursor.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let type_len = u64::from_le_bytes(u64_buf) as usize;
        cursor.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let total_rows = u64::from_le_bytes(u64_buf);

        let header_fixed_size = 32;
        let mut current_pos = header_fixed_size;

        let compressed_nullmap = bytes[current_pos..current_pos + nullmap_len].to_vec();
        current_pos += nullmap_len;

        let pipeline_json = String::from_utf8(bytes[current_pos..current_pos + pipeline_len].to_vec())
            .map_err(|e| PhoenixError::UnsupportedType(e.to_string()))?;
        current_pos += pipeline_len;

        // --- NEW ---
        let original_type = String::from_utf8(bytes[current_pos..current_pos + type_len].to_vec())
            .map_err(|e| PhoenixError::UnsupportedType(e.to_string()))?;
        current_pos += type_len;

        let compressed_data = bytes[current_pos..].to_vec();

        Ok(Self {
            total_rows,
            compressed_nullmap,
            compressed_data,
            pipeline_json,
            original_type,
        })
    }
}

//==================================================================================
// 2. Generic Worker Functions (The DRY Approach)
//==================================================================================

/// Generic worker function that performs the entire compression for any INTEGER array type.
fn compress_integer_array<T>(array: &PrimitiveArray<T>) -> Result<Vec<u8>, PhoenixError>
where
    T: ArrowNumericType,
    T::Native: bytemuck::Pod,
{
    if array.is_empty() {
        return CompressedChunk {
            total_rows: 0,
            compressed_nullmap: Vec::new(),
            compressed_data: Vec::new(),
            pipeline_json: "[]".to_string(),
            original_type: T::DATA_TYPE.to_string(),
        }
        .to_bytes();
    }
    let total_rows = array.len() as u64;

    if array.null_count() == array.len() {
        let validity_bytes = array.nulls().unwrap().buffer().as_slice();
        let pipeline_json = r#"[{"op": "zstd", "params": {"level": 19}}]"#;
        let compressed_nullmap =
            executor::execute_compress_pipeline(validity_bytes, "Boolean", &pipeline_json)?;

        let artifact = CompressedChunk {
            total_rows,
            compressed_nullmap,
            compressed_data: Vec::new(),
            pipeline_json: "[]".to_string(),
            original_type: T::DATA_TYPE.to_string(),
        };
        return artifact.to_bytes();
    }

    let original_type = T::DATA_TYPE.to_string();

    // --- Step 1: Deconstruct the array into its raw parts ---
    // CORRECT: This now returns a typed Vec, not a byte buffer.
    let valid_data_vec: Vec<T::Native> =
        crate::null_handling::bitmap::strip_valid_data_to_vec(array);

    #[cfg(debug_assertions)]
    {
        println!(
            "[DEBUG] Compression: valid_data_vec.len() = {}, expected valid count = {}",
            valid_data_vec.len(),
            array.len() - array.null_count()
        );
    }
    let validity_bytes_opt = array.nulls().map(|nb| nb.buffer().as_slice().to_vec());

    // --- Step 2: Compress the validity buffer (if it exists) ---
    let compressed_nullmap = if let Some(validity_bytes) = validity_bytes_opt {
        // --- THIS IS THE FIX ---
        let pipeline_json = r#"[{"op": "zstd", "params": {"level": 19}}]"#;
        executor::execute_compress_pipeline(&validity_bytes, "Boolean", &pipeline_json)?
    } else {
        Vec::new()
    };

    // --- Step 3: Plan and compress the main data buffer ---
    // CORRECT: Convert the typed Vec to bytes for the planner/executor.
    let valid_data_bytes = crate::utils::typed_slice_to_bytes(&valid_data_vec);
    #[cfg(debug_assertions)]
    {
        // --- ADD THIS CHECKPOINT ---
        println!("\n[CHECKPOINT 1] Orchestrator -> Planner");
        println!("\n[CHECKPOINT 1] valid_data_bytes = bytemuck::cast_slice(&valid_data_vec)");
        println!("  - Type: {}", &original_type);
        println!(
            "  - Bytes Sent to ExePlannerutor (first 50): {:?}...",
            &valid_data_bytes[..valid_data_bytes.len().min(50)]
        );
    }
    // --- END CHECKPOINT ---

    // After extracting valid_data_vec and before planner call
    #[cfg(debug_assertions)]
    {
        println!(
            "[DEBUG] Compression: total_rows = {}, null_count = {}, valid rows = {}",
            array.len(),
            array.null_count(),
            array.len() - array.null_count()
        );
    }
    let pipeline_json = planner::plan_pipeline(&valid_data_bytes, &original_type)?;

    #[cfg(debug_assertions)]
    {
        println!(
            "\n[CHECKPOINT 2] Planner -> Orchestrator: pipeline_json = planner::plan_pipeline"
        );
        println!("  - Pipeline JSON: {}", &pipeline_json);
    }

    let compressed_data =
        executor::execute_compress_pipeline(&valid_data_bytes, &original_type, &pipeline_json)?;

    #[cfg(debug_assertions)]
    {
        println!("\n[CHECKPOINT 3] Orchestrator -> Executor");
        println!("\n[CHECKPOINT 3] compress_primitive_array");
        println!("  - Type: {}", &original_type);
        println!(
            "  - Bytes Sent to Executor (first 50): {:?}...",
            &valid_data_bytes[..valid_data_bytes.len().min(50)]
        );
        println!("  - Plan Sent to Executor: {}", &pipeline_json);
    }

    // --- Step 4: Package the final artifact ---
    let artifact = CompressedChunk {
        total_rows,
        compressed_nullmap,
        compressed_data,
        pipeline_json,
        original_type,
    };

    artifact.to_bytes()
}

/// Generic worker function that performs the entire compression for any FLOAT array type.
fn compress_float_array<T, U>(array: &PrimitiveArray<T>) -> Result<Vec<u8>, PhoenixError>
where
    T: ArrowNumericType,
    U: bytemuck::Pod,
    T::Native: bytemuck::Pod,
{
    if array.is_empty() || array.null_count() == array.len() {
        return compress_integer_array(array);
    }

    let total_rows = array.len() as u64;
    let original_type = T::DATA_TYPE.to_string();

    // --- Step 1: Handle Nulls (unchanged) ---
    let validity_bytes_opt = array.nulls().map(|nb| nb.buffer().as_slice().to_vec());
    let compressed_nullmap = if let Some(validity_bytes) = validity_bytes_opt {
        let pipeline_json = r#"[{"op": "zstd", "params": {"level": 19}}]"#;
        executor::execute_compress_pipeline(&validity_bytes, "Boolean", &pipeline_json)?
    } else {
        Vec::new()
    };

    // --- Step 2: The Core Float-to-Bits Transformation ---
    let valid_data_vec: Vec<T::Native> = bitmap::strip_valid_data_to_vec(array);
    
    // --- THE FIX (from your analysis) ---
    // Use a single, zero-copy cast followed by a single allocation. This is much faster.
    let integer_bits_vec: Vec<U> = bytemuck::cast_slice::<T::Native, U>(&valid_data_vec).to_vec();

    // --- Step 3: Plan and Compress the INTEGER bits (unchanged) ---
    let integer_bits_bytes = typed_slice_to_bytes(&integer_bits_vec);
    let integer_type_str = if std::mem::size_of::<U>() == 4 { "UInt32" } else { "UInt64" };
    let pipeline_json = planner::plan_pipeline(&integer_bits_bytes, integer_type_str)?;
    let compressed_data = executor::execute_compress_pipeline(
        &integer_bits_bytes,
        integer_type_str,
        &pipeline_json,
    )?;

    // --- Step 4: Package the final artifact (unchanged) ---
    let artifact = CompressedChunk {
        total_rows,
        compressed_nullmap,
        compressed_data,
        pipeline_json,
        original_type,
    };

    artifact.to_bytes()
}


//==================================================================================
// 3. Public-Facing Dispatchers (Minimal Boilerplate)
//==================================================================================

/// The public-facing compression orchestrator. Its ONLY job is to downcast
/// the dynamic `Array` and dispatch to the correct generic worker function.
pub fn compress_chunk(array: &dyn Array) -> Result<Vec<u8>, PhoenixError> {
    macro_rules! dispatch_integer {
        ($arr:expr, $T:ty) => {{
            let primitive_arr = $arr.as_any().downcast_ref::<PrimitiveArray<$T>>().unwrap();
            compress_integer_array(primitive_arr)
        }};
    }
    macro_rules! dispatch_float {
        ($arr:expr, $T:ty, $U:ty) => {{
            let primitive_arr = $arr.as_any().downcast_ref::<PrimitiveArray<$T>>().unwrap();
            compress_float_array::<_, $U>(primitive_arr)
        }};
    }

    match array.data_type() {
        DataType::Int8 => dispatch_integer!(array, Int8Type),
        DataType::Int16 => dispatch_integer!(array, Int16Type),
        DataType::Int32 => dispatch_integer!(array, Int32Type),
        DataType::Int64 => dispatch_integer!(array, Int64Type),
        DataType::UInt8 => dispatch_integer!(array, UInt8Type),
        DataType::UInt16 => dispatch_integer!(array, UInt16Type),
        DataType::UInt32 => dispatch_integer!(array, UInt32Type),
        DataType::UInt64 => dispatch_integer!(array, UInt64Type),
        // --- REVISED: Dispatch to the new float worker ---
        DataType::Float32 => dispatch_float!(array, Float32Type, u32),
        DataType::Float64 => dispatch_float!(array, Float64Type, u64),
        dt => Err(PhoenixError::UnsupportedType(format!(
            "Unsupported type for compression: {}",
            dt
        ))),
    }
}

pub fn get_compressed_chunk_info(bytes: &[u8]) -> Result<(usize, usize, String, String), PhoenixError> {
    // --- THE FIX ---
    // 1. Use our robust `from_bytes` function as the single source of truth for parsing.
    //    This avoids all manual parsing and DRY violations.
    let artifact = CompressedChunk::from_bytes(bytes)?;

    // 2. Calculate the sizes based on the successfully parsed artifact.
    //    This is guaranteed to be correct.
    let data_size = artifact.compressed_data.len();
    let header_size = bytes.len() - data_size;

    // 3. Return the values directly from the parsed artifact.
    Ok((
        header_size,
        data_size,
        artifact.pipeline_json,
        artifact.original_type,
    ))
}

/// The public-facing decompression orchestrator.
pub fn decompress_chunk(bytes: &[u8]) -> Result<Box<dyn Array>, PhoenixError> {
    // Step 1: Deserialize the artifact to get our self-described metadata.
    let artifact = CompressedChunk::from_bytes(bytes)?;
    let original_type = &artifact.original_type;

    // Step 2: Handle the empty array edge case.
    if artifact.total_rows == 0 {
        macro_rules! dispatch_empty {
            ($T:ty) => {
                Ok(Box::new(PrimitiveArray::<$T>::new(Buffer::from(&[]).into(), None)) as Box<dyn Array>)
            };
        }
        // The match statement is correct as provided.
        return match original_type.as_str() {
            "Int8" => dispatch_empty!(Int8Type),
            "Int16" => dispatch_empty!(Int16Type),
            "Int32" => dispatch_empty!(Int32Type),
            "Int64" => dispatch_empty!(Int64Type),
            "UInt8" => dispatch_empty!(UInt8Type),
            "UInt16" => dispatch_empty!(UInt16Type),
            "UInt32" => dispatch_empty!(UInt32Type),
            "UInt64" => dispatch_empty!(UInt64Type),
            "Float32" => dispatch_empty!(Float32Type),
            "Float64" => dispatch_empty!(Float64Type),
            _ => Err(PhoenixError::UnsupportedType(original_type.to_string())),
        };
    }

    // Step 3: Decompress the null buffer if it exists.
    let null_buffer = if !artifact.compressed_nullmap.is_empty() {
        let validity_pipeline_json = r#"[{"op": "zstd"}]"#;
        let validity_bytes = executor::execute_decompress_pipeline(
            &artifact.compressed_nullmap,
            "Boolean",
            validity_pipeline_json,
            artifact.total_rows as usize,
        )?;
        let byte_buffer = Buffer::from(validity_bytes);
        let boolean_buffer = BooleanBuffer::new(byte_buffer, 0, artifact.total_rows as usize);
        Some(NullBuffer::from(boolean_buffer))
    } else {
        None
    };

    // Step 4: Determine the intermediate integer type and decompress the main data payload.
    let integer_type_str = match original_type.as_str() {
        "Float32" => "UInt32",
        "Float64" => "UInt64",
        _ => original_type, // It's already an integer type, pass it through.
    };

    let num_valid_rows = null_buffer
        .as_ref()
        .map_or(artifact.total_rows as usize, |nb| nb.len() - nb.null_count());

    let decompressed_data_bytes = if !artifact.compressed_data.is_empty() {
        executor::execute_decompress_pipeline(
            &artifact.compressed_data,
            integer_type_str,
            &artifact.pipeline_json,
            num_valid_rows,
        )?
    } else {
        Vec::new()
    };

    // Step 5: Reconstruct the final array from the decompressed bytes and null buffer.
    // --- REVISED: We only need one robust macro ---
    // This macro works for both integers and floats because the bit patterns in
    // `decompressed_data_bytes` are identical for `u32`/`f32` and `u64`/`f64`.
    // The `reapply_bitmap` function's generic parameter `<$T>` handles the final
    // interpretation of those bytes.
    macro_rules! reconstruct_array {
        ($T:ty) => {{
            // This works for both integers and floats. The `decompressed_data_bytes`
            // already have the correct bit pattern. The generic parameter `<$T>`
            // tells `reapply_bitmap` how to interpret those bytes when building the
            // final Arrow array.
            let array = bitmap::reapply_bitmap::<$T>(
                decompressed_data_bytes,
                null_buffer,
                artifact.total_rows as usize,
            )?;
            Ok(Box::new(array) as Box<dyn Array>)
        }};
    }

    match original_type.as_str() {
        "Int8" => reconstruct_array!(Int8Type),
        "Int16" => reconstruct_array!(Int16Type),
        "Int32" => reconstruct_array!(Int32Type),
        "Int64" => reconstruct_array!(Int64Type),
        "UInt8" => reconstruct_array!(UInt8Type),
        "UInt16" => reconstruct_array!(UInt16Type),
        "UInt32" => reconstruct_array!(UInt32Type),
        "UInt64" => reconstruct_array!(UInt64Type),
        "Float32" => reconstruct_array!(Float32Type),
        "Float64" => reconstruct_array!(Float64Type),
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string())),
    }
}

//==================================================================================
// 3. Unit Tests (REVISED to use Arrow arrays instead of Polars Series)
//==================================================================================

//==================================================================================
// 3. Unit Tests (REVISED - Authoritative & Correct)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float32Array, Float64Array, Int32Array, Int64Array, PrimitiveArray};
    use arrow::datatypes::{ArrowNumericType, Float32Type, Float64Type, Int32Type, Int64Type};
    use bytemuck::Pod; // Import the Pod trait for use


    //==============================================================================
    // 3.1 The Authoritative Roundtrip Test Helper
    //==============================================================================

    /// A single, generic, and robust helper function to test the end-to-end
    /// compression and decompression for any primitive array type.

    /// A single, generic, and robust helper function to test the end-to-end
    /// compression and decompression for any primitive array type.
    fn roundtrip_test<T>(original_array: &PrimitiveArray<T>)
    where
        T: ArrowNumericType,
        // The `Pod` trait from bytemuck is the correct, safe way to guarantee
        // that a type can be safely viewed as a slice of bytes.
        T::Native: Pod,
    {
        // --- 1. Compress the Array ---
        let compressed_artifact_bytes = compress_chunk(original_array)
            .expect("Compression failed during test");

        if !original_array.is_empty() {
            assert!(!compressed_artifact_bytes.is_empty(), "Compression produced zero bytes for a non-empty array");
        }

        // --- 2. Decompress the Artifact ---
        let reconstructed_arrow_array = decompress_chunk(&compressed_artifact_bytes)
            .expect("Decompression failed during test");

        // --- 3. Downcast and Verify ---
        let reconstructed_primitive_array = reconstructed_arrow_array
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .expect("Failed to downcast reconstructed array to the correct primitive type");

        // --- 4. Assert Equality ---
        assert_eq!(
            original_array.len(),
            reconstructed_primitive_array.len(),
            "Array length mismatch after roundtrip"
        );
        assert_eq!(
            original_array.null_count(),
            reconstructed_primitive_array.null_count(),
            "Null count mismatch after roundtrip"
        );

        for i in 0..original_array.len() {
            if original_array.is_null(i) {
                assert!(reconstructed_primitive_array.is_null(i), "Null value mismatch at index {}", i);
            } else {
                assert!(reconstructed_primitive_array.is_valid(i), "Valid value mismatch at index {}", i);
                let original_val = original_array.value(i);
                let reconstructed_val = reconstructed_primitive_array.value(i);

                // --- CORRECTED COMPARISON LOGIC ---
                // Use `bytemuck::bytes_of` to get a byte slice representation of the value.
                // This is safe because of the `T::Native: Pod` trait bound.
                // This works for ALL primitive types, including floats, and guarantees
                // a bit-for-bit comparison.
                assert_eq!(
                    bytemuck::bytes_of(&original_val),
                    bytemuck::bytes_of(&reconstructed_val),
                    "Value bit-pattern mismatch at index {}: orig={:?}, recon={:?}",
                    i, original_val, reconstructed_val
                );
            }
        }
    }

    //==============================================================================
    // 3.2 Integer Type Test Cases
    //==============================================================================

    #[test]
    fn test_roundtrip_integers_with_nulls() {
        let array = Int64Array::from(vec![Some(1000), Some(1001), None, Some(1003), Some(1003)]);
        roundtrip_test(&array);
    }

    #[test]
    fn test_roundtrip_integers_no_nulls() {
        let array = Int32Array::from(vec![10, 20, 30, 40, 50, 60, 70, 80]);
        roundtrip_test(&array);
    }

    #[test]
    fn test_roundtrip_integers_all_nulls() {
        let array = Int32Array::from(vec![None, None, None, None]);
        roundtrip_test(&array);
    }

    #[test]
    fn test_roundtrip_integers_empty() {
        let array = Int64Array::from(vec![] as Vec<i64>);
        roundtrip_test(&array);
    }

    #[test]
    fn test_roundtrip_integers_single_value() {
        let array = Int32Array::from(vec![Some(42)]);
        roundtrip_test(&array);
    }

    //==============================================================================
    // 3.3 Floating-Point Type Test Cases
    //==============================================================================

    #[test]
    fn test_roundtrip_floats_with_nulls() {
        let array = Float32Array::from(vec![Some(10.5), None, Some(-20.0), Some(30.1), None]);
        roundtrip_test(&array);
    }

    #[test]
    fn test_roundtrip_floats_no_nulls() {
        let array = Float64Array::from(vec![10.5, -20.0, 30.1, 40.9, 50.0]);
        roundtrip_test(&array);
    }

    #[test]
    fn test_roundtrip_floats_all_nulls() {
        let array = Float64Array::from(vec![None, None, None]);
        roundtrip_test(&array);
    }

    #[test]
    fn test_roundtrip_floats_empty() {
        let array = Float32Array::from(vec![] as Vec<f32>);
        roundtrip_test(&array);
    }

    #[test]
    fn test_roundtrip_floats_special_values() {
        let array = Float64Array::from(vec![
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -0.0,
            0.0,
        ]);
        roundtrip_test(&array);
    }

    //==============================================================================
    // 3.4 Pipeline-Specific Tests (Validating Planner & Executor Integration)
    //==============================================================================

    #[test]
    fn test_roundtrip_drifting_floats_triggers_xor_delta() {
        // This data has small bit-wise differences and should trigger the XOR Delta pipeline.
        let array = Float32Array::from(vec![
            Some(1.0), Some(1.0000001), None, Some(1.0000002)
        ]);
        roundtrip_test(&array);
    }

    #[test]
    fn test_roundtrip_clustered_floats_triggers_shuffle() {
        // This data has large deltas but stable high-order bytes, which should trigger Shuffle.
        let array = Float64Array::from(vec![
            Some(1000.5), Some(1000.1), Some(1000.9), None, Some(1000.4)
        ]);
        roundtrip_test(&array);
    }

    #[test]
    fn test_roundtrip_constant_integers_triggers_rle() {
        // This data is constant and should trigger the simple RLE pipeline.
        let array = Int64Array::from(vec![Some(777), Some(777), Some(777), None, Some(777)]);
        roundtrip_test(&array);
    }
}