// --- IN: src/pipeline/orchestrator.v2.rs ---

//! This module contains the highest-level orchestration logic. It uses a generic
//! worker function pattern to avoid boilerplate and keep the core logic DRY.

use arrow::array::{Array, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::datatypes::*;
use std::io::{Cursor, Read, Write};

use super::{executor, planner};
use crate::error::PhoenixError;
use crate::null_handling::bitmap;

// (The CompressedChunk struct and its impl remain unchanged)
#[derive(Debug)]
struct CompressedChunk {
    total_rows: u64,
    compressed_nullmap: Vec<u8>,
    compressed_data: Vec<u8>,
    pipeline_json: String,
}

// ... (The impl CompressedChunk block is unchanged and correct) ...
impl CompressedChunk {
    pub fn to_bytes(&self) -> Result<Vec<u8>, PhoenixError> {
        let nullmap_len = self.compressed_nullmap.len() as u64;
        let pipeline_len = self.pipeline_json.len() as u64;
        let mut buffer = Vec::new();

        buffer
            .write_all(&nullmap_len.to_le_bytes())
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer
            .write_all(&pipeline_len.to_le_bytes())
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer
            .write_all(&self.total_rows.to_le_bytes())
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;

        buffer
            .write_all(&self.compressed_nullmap)
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer
            .write_all(self.pipeline_json.as_bytes())
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        buffer
            .write_all(&self.compressed_data)
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;

        Ok(buffer)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PhoenixError> {
        if bytes.len() < 24 {
            return Err(PhoenixError::BufferMismatch(bytes.len(), 24));
        }
        let mut cursor = Cursor::new(bytes);

        let mut u64_buf = [0u8; 8];
        cursor
            .read_exact(&mut u64_buf)
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let nullmap_len = u64::from_le_bytes(u64_buf) as usize;
        cursor
            .read_exact(&mut u64_buf)
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let pipeline_len = u64::from_le_bytes(u64_buf) as usize;
        cursor
            .read_exact(&mut u64_buf)
            .map_err(|e| PhoenixError::InternalError(e.to_string()))?;
        let total_rows = u64::from_le_bytes(u64_buf);

        let header_size = 24;
        let data_start = header_size + nullmap_len + pipeline_len;
        if bytes.len() < data_start {
            return Err(PhoenixError::BufferMismatch(bytes.len(), data_start));
        }

        let compressed_nullmap = bytes[header_size..header_size + nullmap_len].to_vec();
        let pipeline_json =
            String::from_utf8(bytes[header_size + nullmap_len..data_start].to_vec())
                .map_err(|e| PhoenixError::UnsupportedType(e.to_string()))?;
        let compressed_data = bytes[data_start..].to_vec();

        Ok(Self {
            total_rows,
            compressed_nullmap,
            compressed_data,
            pipeline_json,
        })
    }
}

//==================================================================================
// 2. Generic Worker Functions (The DRY Approach)
//==================================================================================

/// Generic worker function that performs the entire compression for any primitive array type.
fn compress_primitive_array<T>(array: &PrimitiveArray<T>) -> Result<Vec<u8>, PhoenixError>
where
    T: ArrowNumericType,
    T::Native: ToByteSlice + bytemuck::Pod, // Add Pod bound
{
    let total_rows = array.len() as u64;

    if array.null_count() == array.len() {
        let validity_bytes = array.nulls().unwrap().buffer().as_slice();
        let pipeline_json = r#"[{"op": "rle"}, {"op": "zstd", "params": {"level": 19}}]"#;
        let compressed_nullmap =
            executor::execute_compress_pipeline(validity_bytes, "Boolean", &pipeline_json)?;

        let artifact = CompressedChunk {
            total_rows,
            compressed_nullmap,
            compressed_data: Vec::new(),
            pipeline_json: "[]".to_string(),
        };
        return artifact.to_bytes();
    }

    let original_type = T::DATA_TYPE.to_string();

    // --- Step 1: Deconstruct the array into its raw parts ---
    // CORRECT: This now returns a typed Vec, not a byte buffer.
    let valid_data_vec: Vec<T::Native> =
        crate::null_handling::bitmap::strip_valid_data_to_vec(array);
    let validity_bytes_opt = array.nulls().map(|nb| nb.buffer().as_slice().to_vec());

    // --- Step 2: Compress the validity buffer (if it exists) ---
    let compressed_nullmap = if let Some(validity_bytes) = validity_bytes_opt {
        let pipeline_json = r#"[{"op": "rle"}, {"op": "zstd", "params": {"level": 19}}]"#;
        executor::execute_compress_pipeline(&validity_bytes, "Boolean", &pipeline_json)?
    } else {
        Vec::new()
    };

    // --- Step 3: Plan and compress the main data buffer ---
    // CORRECT: Convert the typed Vec to bytes for the planner/executor.
    let valid_data_bytes = crate::utils::typed_slice_to_bytes(&valid_data_vec);

    // --- ADD THIS CHECKPOINT ---
    println!("\n[CHECKPOINT 1] Orchestrator -> Planner");
    println!("\n[CHECKPOINT 1] valid_data_bytes = bytemuck::cast_slice(&valid_data_vec)");
    println!("  - Type: {}", &original_type);
    println!(
        "  - Bytes Sent to ExePlannerutor (first 50): {:?}...",
        &valid_data_bytes[..valid_data_bytes.len().min(50)]
    );
    // --- END CHECKPOINT ---

    let pipeline_json = planner::plan_pipeline(&valid_data_bytes, &original_type)?;
    // --- ADD THIS CHECKPOINT ---
    println!("\n[CHECKPOINT 2] Planner -> Orchestrator: pipeline_json = planner::plan_pipeline");
    println!("  - Pipeline JSON: {}", &pipeline_json);
    // --- END CHECKPOINT ---
    let compressed_data =
        executor::execute_compress_pipeline(&valid_data_bytes, &original_type, &pipeline_json)?;

    // --- ADD THIS CHECKPOINT (before the call) ---
    println!("\n[CHECKPOINT 3] Orchestrator -> Executor");
    println!("\n[CHECKPOINT 3] compress_primitive_array");
    println!("  - Type: {}", &original_type);
    println!(
        "  - Bytes Sent to Executor (first 50): {:?}...",
        &valid_data_bytes[..valid_data_bytes.len().min(50)]
    );
    println!("  - Plan Sent to Executor: {}", &pipeline_json);
    // --- END CHECKPOINT ---

    // --- Step 4: Package the final artifact ---
    let artifact = CompressedChunk {
        total_rows,
        compressed_nullmap,
        compressed_data,
        pipeline_json,
    };

    artifact.to_bytes()
}

//==================================================================================
// 3. Public-Facing Dispatchers (Minimal Boilerplate)
//==================================================================================

/// The public-facing compression orchestrator. Its ONLY job is to downcast
/// the dynamic `Array` and dispatch to the generic worker function.
pub fn compress_chunk(array: &dyn Array) -> Result<Vec<u8>, PhoenixError> {
    if array.len() == 0 {
        let artifact = CompressedChunk {
            total_rows: 0,
            compressed_nullmap: Vec::new(),
            compressed_data: Vec::new(),
            pipeline_json: "[]".to_string(),
        };
        return artifact.to_bytes();
    }
    if array.len() == 0 || array.null_count() == array.len() {
        let compressed_nullmap = if let Some(null_buffer) = array.nulls() {
            let validity_bytes = null_buffer.buffer().as_slice();
            let pipeline_json = r#"[{"op": "rle"}, {"op": "zstd", "params": {"level": 19}}]"#;
            executor::execute_compress_pipeline(validity_bytes, "Boolean", &pipeline_json)?
        } else {
            Vec::new()
        };

        let artifact = CompressedChunk {
            total_rows: array.len() as u64,
            compressed_nullmap,
            compressed_data: Vec::new(),
            pipeline_json: "[]".to_string(),
        };
        return artifact.to_bytes();
    }

    // This macro performs the downcast and calls the generic worker.
    macro_rules! dispatch {
        ($arr:expr, $T:ty) => {{
            let primitive_arr = $arr
                .as_any()
                .downcast_ref::<PrimitiveArray<$T>>()
                .ok_or_else(|| {
                    PhoenixError::UnsupportedType(format!(
                        "Failed to downcast to {}",
                        stringify!($T)
                    ))
                })?;
            compress_primitive_array(primitive_arr)
        }};
    }

    // The boilerplate is now minimal and serves a single, clear purpose: type dispatch.
    match array.data_type() {
        DataType::Int8 => dispatch!(array, Int8Type),
        DataType::Int16 => dispatch!(array, Int16Type),
        DataType::Int32 => dispatch!(array, Int32Type),
        DataType::Int64 => dispatch!(array, Int64Type),
        DataType::UInt8 => dispatch!(array, UInt8Type),
        DataType::UInt16 => dispatch!(array, UInt16Type),
        DataType::UInt32 => dispatch!(array, UInt32Type),
        DataType::UInt64 => dispatch!(array, UInt64Type),
        dt => Err(PhoenixError::UnsupportedType(format!(
            "Unsupported type for compression: {}",
            dt
        ))),
    }
}

pub fn get_compressed_chunk_info(bytes: &[u8]) -> Result<(usize, usize, String), PhoenixError> {
    if bytes.len() < 24 {
        return Err(PhoenixError::BufferMismatch(bytes.len(), 24));
    }
    let mut cursor = Cursor::new(bytes);

    let mut u64_buf = [0u8; 8];
    cursor.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
    let nullmap_len = u64::from_le_bytes(u64_buf) as usize;
    cursor.read_exact(&mut u64_buf).map_err(|e| PhoenixError::InternalError(e.to_string()))?;
    let pipeline_len = u64::from_le_bytes(u64_buf) as usize;
    
    // --- NEW: Read the pipeline JSON from the buffer ---
    let header_fixed_size = 24;
    let pipeline_start = header_fixed_size + nullmap_len;
    let pipeline_end = pipeline_start + pipeline_len;

    if bytes.len() < pipeline_end {
        return Err(PhoenixError::BufferMismatch(bytes.len(), pipeline_end));
    }

    let pipeline_json = String::from_utf8(bytes[pipeline_start..pipeline_end].to_vec())
        .map_err(|e| PhoenixError::InternalError(format!("Failed to parse pipeline JSON from header: {}", e)))?;

    // Calculate sizes
    let header_size = header_fixed_size + nullmap_len + pipeline_len;
    let data_size = bytes.len() - header_size;

    Ok((header_size, data_size, pipeline_json))
}

/// The public-facing decompression orchestrator.
pub fn decompress_chunk(bytes: &[u8], original_type: &str) -> Result<Box<dyn Array>, PhoenixError> {
    let artifact = CompressedChunk::from_bytes(bytes)?;

    // --- FINAL, CORRECT GUARD CLAUSE ---
    if artifact.total_rows == 0 {
        macro_rules! dispatch_empty {
            ($T:ty) => {
                // FINAL, CORRECT, GUARANTEED METHOD:
                // Create an empty buffer from an empty slice using the `From` trait.
                Ok(
                    Box::new(PrimitiveArray::<$T>::new(Buffer::from(&[]).into(), None))
                        as Box<dyn Array>,
                )
            };
        }
        return match original_type {
            "Int8" => dispatch_empty!(Int8Type),
            "Int16" => dispatch_empty!(Int16Type),
            "Int32" => dispatch_empty!(Int32Type),
            "Int64" => dispatch_empty!(Int64Type),
            "UInt8" => dispatch_empty!(UInt8Type),
            "UInt16" => dispatch_empty!(UInt16Type),
            "UInt32" => dispatch_empty!(UInt32Type),
            "UInt64" => dispatch_empty!(UInt64Type),
            _ => Err(PhoenixError::UnsupportedType(original_type.to_string())),
        };
    }

    let null_buffer = if !artifact.compressed_nullmap.is_empty() {
        let validity_pipeline_json = r#"[{"op": "zstd"}]"#; // Corrected pipeline
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

    let num_valid_rows = null_buffer
        .as_ref()
        .map_or(artifact.total_rows as usize, |nb| {
            nb.len() - nb.null_count()
        });

    let decompressed_data_bytes = if !artifact.compressed_data.is_empty() {
        executor::execute_decompress_pipeline(
            &artifact.compressed_data,
            original_type,
            &artifact.pipeline_json,
            num_valid_rows,
        )?
    } else {
        Vec::new()
    };

    macro_rules! dispatch_reconstruct {
        ($T:ty) => {{
            let array = bitmap::reapply_bitmap::<$T>(
                decompressed_data_bytes, // Pass the Vec<u8> directly
                null_buffer,
                artifact.total_rows as usize,
            )?;
            Ok(Box::new(array) as Box<dyn Array>)
        }};
    }

    match original_type {
        "Int8" => dispatch_reconstruct!(Int8Type),
        "Int16" => dispatch_reconstruct!(Int16Type),
        "Int32" => dispatch_reconstruct!(Int32Type),
        "Int64" => dispatch_reconstruct!(Int64Type),
        "UInt8" => dispatch_reconstruct!(UInt8Type),
        "UInt16" => dispatch_reconstruct!(UInt16Type),
        "UInt32" => dispatch_reconstruct!(UInt32Type),
        "UInt64" => dispatch_reconstruct!(UInt64Type),
        _ => Err(PhoenixError::UnsupportedType(original_type.to_string())),
    }
}

//==================================================================================
// 3. Unit Tests (REVISED to use Arrow arrays instead of Polars Series)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int16Array, Int32Array, Int64Array};
    fn roundtrip_test<T>(original_array: &PrimitiveArray<T>)
    where
        T: ArrowNumericType,
        T::Native: ToByteSlice,
    {
        // 1. Compress it
        let compressed_artifact_bytes = compress_chunk(original_array).unwrap();

        // 2. Decompress it
        let reconstructed_arrow_array =
            decompress_chunk(&compressed_artifact_bytes, &T::DATA_TYPE.to_string()).unwrap();

        // 3. Downcast and compare
        let reconstructed_primitive_array = reconstructed_arrow_array
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap();

        assert_eq!(original_array, reconstructed_primitive_array);
    }

    #[test]
    fn test_orchestrator_roundtrip_with_nulls() {
        let original_array =
            Int64Array::from(vec![Some(100i64), Some(110), None, Some(110), Some(90)]);
        roundtrip_test(&original_array);
    }

    #[test]
    fn test_orchestrator_roundtrip_no_nulls() {
        let original_array = Int32Array::from(vec![10i32, 20, 30, 40]);
        roundtrip_test(&original_array);
    }

    #[test]
    fn test_orchestrator_roundtrip_all_nulls() {
        let original_array = Int32Array::from(vec![None, None, None, None]);
        roundtrip_test(&original_array);
    }

    #[test]
    fn test_orchestrator_roundtrip_empty_array() {
        let original_array = Int32Array::from(vec![] as Vec<i32>);
        roundtrip_test(&original_array);
    }

    #[test]
    fn test_orchestrator_roundtrip_single_value() {
        let original_array = Int64Array::from(vec![Some(42i64)]);
        roundtrip_test(&original_array);
    }

    #[test]
    fn test_orchestrator_roundtrip_with_nulls_b() {
        // 1. Create a native arrow-rs Array
        let original_array =
            Int64Array::from(vec![Some(100i64), Some(110), None, Some(110), Some(90)]);

        // 2. Compress it
        let compressed_artifact_bytes = compress_chunk(&original_array).unwrap();

        // 3. Decompress it
        let reconstructed_arrow_array =
            decompress_chunk(&compressed_artifact_bytes, "Int64").unwrap();

        // 4. Downcast and compare
        let reconstructed_i64_array = reconstructed_arrow_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(&original_array, reconstructed_i64_array);
    }

    #[test]
    fn test_orchestrator_roundtrip_no_nulls_b() {
        let original_array = Int32Array::from(vec![10i32, 20, 30, 40]);

        let compressed_artifact_bytes = compress_chunk(&original_array).unwrap();

        let reconstructed_arrow_array =
            decompress_chunk(&compressed_artifact_bytes, "Int32").unwrap();

        let reconstructed_i32_array = reconstructed_arrow_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(&original_array, reconstructed_i32_array);
    }

    #[test]
    fn checkpoint_01_strip_valid_data() {
        let original_array =
            Int16Array::from(vec![Some(100i16), None, Some(101), Some(103), Some(102)]);
        let valid_data_vec: Vec<i16> =
            crate::null_handling::bitmap::strip_valid_data_to_vec(&original_array);

        println!("\n--- CHECKPOINT 01: STRIPPED VALID DATA ---");
        dbg!(&valid_data_vec); // Should be [100, 101, 103, 102]
        println!("-----------------------------------------\n");
        assert!(!valid_data_vec.is_empty());
    }

    #[test]
    fn checkpoint_02_generated_plan() {
        let valid_data_vec: Vec<i16> = vec![100, 101, 103, 102];
        let valid_data_bytes = bytemuck::cast_slice(&valid_data_vec);
        let pipeline_json =
            crate::pipeline::planner::plan_pipeline(valid_data_bytes, "Int16").unwrap();

        println!("\n--- CHECKPOINT 02: GENERATED PIPELINE ---");
        dbg!(&pipeline_json); // Should be a JSON string with delta, zigzag, shuffle, bitpack, zstd
        println!("-----------------------------------------\n");
        assert!(pipeline_json.contains("shuffle"));
    }

    #[test]
    fn checkpoint_03_final_compressed_artifact() {
        let original_array =
            Int16Array::from(vec![Some(100i16), None, Some(101), Some(103), Some(102)]);
        let compressed_bytes = compress_chunk(&original_array).unwrap();
        let artifact = CompressedChunk::from_bytes(&compressed_bytes).unwrap();

        println!("\n--- CHECKPOINT 03: FINAL COMPRESSED ARTIFACT ---");
        dbg!(&artifact);
        println!("----------------------------------------------\n");
        assert_eq!(artifact.total_rows, 5);
    }

    #[test]
    fn checkpoint_04_decompressed_null_buffer() {
        let original_array =
            Int16Array::from(vec![Some(100i16), None, Some(101), Some(103), Some(102)]);
        let compressed_bytes = compress_chunk(&original_array).unwrap();
        let artifact = CompressedChunk::from_bytes(&compressed_bytes).unwrap();

        let null_buffer = if !artifact.compressed_nullmap.is_empty() {
            let validity_pipeline_json = r#"[{"op": "zstd"}]"#;
            let validity_bytes = crate::pipeline::executor::execute_decompress_pipeline(
                &artifact.compressed_nullmap,
                "Boolean",
                validity_pipeline_json,
                artifact.total_rows as usize,
            )
            .unwrap();
            let byte_buffer = Buffer::from(validity_bytes);
            Some(BooleanBuffer::new(
                byte_buffer,
                0,
                artifact.total_rows as usize,
            ))
        } else {
            None
        };

        println!("\n--- CHECKPOINT 04: DECOMPRESSED NULL BUFFER ---");
        dbg!(&null_buffer); // Should be Some(BooleanBuffer) representing [true, false, true, true, true]
        println!("-----------------------------------------------\n");
        assert!(null_buffer.is_some());
    }

    #[test]
    fn checkpoint_05_final_decompressed_data() {
        let original_array =
            Int16Array::from(vec![Some(100i16), None, Some(101), Some(103), Some(102)]);
        let compressed_bytes = compress_chunk(&original_array).unwrap();

        // Decompress just to get the final bytes
        let artifact = CompressedChunk::from_bytes(&compressed_bytes).unwrap();
        let null_buffer = if !artifact.compressed_nullmap.is_empty() {
            Some(NullBuffer::from(BooleanBuffer::new(
                Buffer::from(&[]),
                0,
                0,
            )))
        } else {
            None
        }; // Dummy
        let num_valid_rows = null_buffer
            .as_ref()
            .map_or(artifact.total_rows as usize, |nb| {
                nb.len() - nb.null_count()
            });
        let decompressed_data_bytes = crate::pipeline::executor::execute_decompress_pipeline(
            &artifact.compressed_data,
            "Int16",
            &artifact.pipeline_json,
            4, // Hardcoded for this test
        )
        .unwrap();
        let final_vec: Vec<i16> = bytemuck::cast_slice(&decompressed_data_bytes).to_vec();

        println!("\n--- CHECKPOINT 05: FINAL DECOMPRESSED DATA ---");
        dbg!(&final_vec); // Should be [100, 101, 103, 102]
        println!("----------------------------------------------\n");
        assert_eq!(final_vec, vec![100, 101, 103, 102]);
    }
}
