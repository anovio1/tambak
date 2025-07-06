//! This module contains the pure, stateless kernel for bit-casting.
//!
//! This is a Layer 0 (Normalization/Type Conversion) transform. It performs a
//! zero-copy reinterpretation of a slice's bit patterns from one type to another
//! of the same size (e.g., `f32` -> `u32`). This is critical for enabling
//! integer-based compression algorithms on float data. This module is PURE RUST,
//! panic-free, and relies on `bytemuck` for safety.

use crate::error::tambakError;
use bytemuck::Pod;

//==================================================================================
// 1. Private Core Logic
//==================================================================================

/// Internal function to perform a safe bit-cast.
fn bitcast_internal<I, O>(input_bytes: &[u8], output_buf: &mut Vec<u8>) -> Result<(), tambakError>
where
    I: Pod,
    O: Pod,
{
    if input_bytes.is_empty() {
        output_buf.clear();
        return Ok(());
    }

    // Critical safety check: The types must be the same size for a bit-cast.
    if std::mem::size_of::<I>() != std::mem::size_of::<O>() {
        return Err(tambakError::InternalError(format!(
            "Bit-cast size mismatch: Cannot cast from {} ({} bytes) to {} ({} bytes)",
            std::any::type_name::<I>(),
            std::mem::size_of::<I>(),
            std::any::type_name::<O>(),
            std::mem::size_of::<O>()
        )));
    }

    // Safely cast the raw input bytes to a slice of the *input* type.
    let input_slice: &[I] = bytemuck::try_cast_slice(input_bytes).map_err(|e| {
        tambakError::InternalError(format!("Failed to cast bytes to input type: {}", e))
    })?;

    // Safely cast the typed slice to a slice of the *output* type.
    let output_slice: &[O] = bytemuck::cast_slice(input_slice);

    // Write the final bytes to the output buffer.
    output_buf.clear();
    output_buf.extend_from_slice(bytemuck::cast_slice::<O, u8>(output_slice));
    Ok(())
}

//==================================================================================
// 2. Public API (Generic, Performant, Decoupled)
//==================================================================================

/// The public-facing encode function. Casts from type `I` to type `O`.
pub fn encode<I, O>(input_bytes: &[u8], output_buf: &mut Vec<u8>) -> Result<(), tambakError>
where
    I: Pod,
    O: Pod,
{
    // --- CHECKPOINT: BitCast Kernel Execution ---
    #[cfg(debug_assertions)]
    {
        println!("[CHECKPOINT] KERNEL EXECUTION: bitcast::encode");
        println!("  - Casting from I: {}", std::any::type_name::<I>());
        println!("  - Casting to   O: {}", std::any::type_name::<O>());
        println!("  - input_bytes.len(): {}", input_bytes.len());
    }
    // --- END CHECKPOINT ---

    let result = bitcast_internal::<I, O>(input_bytes, output_buf);

    // --- CHECKPOINT: BitCast Kernel Result ---
    #[cfg(debug_assertions)]
    {
        if result.is_ok() {
            println!("  - SUCCESS: output_buf.len() = {}", output_buf.len());
        } else {
            println!("  - FAILURE: {:?}", result);
        }
        println!("[CHECKPOINT] KERNEL EXECUTION: bitcast::encode FINISHED\n");
    }
    // --- END CHECKPOINT ---

    result
}

/// The public-facing decode function. Casts from type `I` back to type `O`.
/// The implementation is identical, just the type parameters are used differently by the caller.
pub fn decode<I, O>(input_bytes: &[u8], output_buf: &mut Vec<u8>) -> Result<(), tambakError>
where
    I: Pod,
    O: Pod,
{
    bitcast_internal::<I, O>(input_bytes, output_buf)
}

//==================================================================================
// 3. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::{safe_bytes_to_typed_slice, typed_slice_to_bytes};

    #[test]
    fn test_bitcast_f32_u32_roundtrip() {
        let original_f32: Vec<f32> = vec![1.0, -1.0, std::f32::consts::PI];
        let original_bytes = typed_slice_to_bytes(&original_f32);

        // --- Encode (f32 -> u32) ---
        let mut encoded_bytes = Vec::new();
        encode::<f32, u32>(&original_bytes, &mut encoded_bytes).unwrap();

        // Verify the bit patterns are correct
        let result_u32 = safe_bytes_to_typed_slice::<u32>(&encoded_bytes).unwrap();
        assert_eq!(result_u32[0], original_f32[0].to_bits());
        assert_eq!(result_u32[1], original_f32[1].to_bits());
        assert_eq!(result_u32[2], original_f32[2].to_bits());

        // --- Decode (u32 -> f32) ---
        let mut decoded_bytes = Vec::new();
        decode::<u32, f32>(&encoded_bytes, &mut decoded_bytes).unwrap();

        // Verify the final result matches the original
        assert_eq!(decoded_bytes, original_bytes);
    }

    #[test]
    fn test_bitcast_f64_u64_roundtrip() {
        let original_f64: Vec<f64> = vec![100.0, -100.0, std::f64::consts::E];
        let original_bytes = typed_slice_to_bytes(&original_f64);

        // --- Encode (f64 -> u64) ---
        let mut encoded_bytes = Vec::new();
        encode::<f64, u64>(&original_bytes, &mut encoded_bytes).unwrap();

        // --- Decode (u64 -> f64) ---
        let mut decoded_bytes = Vec::new();
        decode::<u64, f64>(&encoded_bytes, &mut decoded_bytes).unwrap();

        assert_eq!(decoded_bytes, original_bytes);
    }

    #[test]
    fn test_bitcast_size_mismatch_error() {
        let original_f32: Vec<f32> = vec![1.0];
        let original_bytes = typed_slice_to_bytes(&original_f32);
        let mut output_buf = Vec::new();

        // Attempt to cast a 4-byte float to an 8-byte integer
        let result = encode::<f32, u64>(&original_bytes, &mut output_buf);

        assert!(result.is_err());
        if let Err(tambakError::InternalError(msg)) = result {
            assert!(msg.contains("Bit-cast size mismatch"));
        } else {
            panic!("Expected InternalError for size mismatch");
        }
    }
}
