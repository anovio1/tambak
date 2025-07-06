//! This module contains the pure, stateless, and performant kernels for performing
//! Zstandard compression and decompression.
//!
//! This is the Final Stage transform in the compression pipeline. It takes a byte
//! buffer that has already been optimized by the preceding layers and applies a
//! high-performance, modern entropy coder to achieve the final compressed size.
//! This module is a safe, panic-free wrapper around the `zstd` crate.

use std::io::Write;
use zstd::stream::{Decoder, Encoder};

use crate::error::tambakError;

//==================================================================================
// 1. Core Logic (The "Engine")
//==================================================================================

/// Compresses a byte slice using the Zstandard algorithm, writing to an output buffer.
fn compress_slice(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    level: i32,
) -> Result<(), tambakError> {
    // println!("[DEBUG] zstd compress: input_bytes.len() = {}", input_bytes.len());
    // // Use a temporary buffer for encoding.
    // let mut temp_buf = Vec::new();
    // let mut encoder = Encoder::new(&mut temp_buf, level)
    //     .map_err(|e| tambakError::ZstdError(e.to_string()))?;
    // encoder.write_all(input_bytes)
    //     .map_err(|e| tambakError::ZstdError(e.to_string()))?;

    // // `finish` is essential to finalize the Zstd frame.
    // encoder.finish()
    //     .map_err(|e| tambakError::ZstdError(e.to_string()))?;
    // output_buf.extend_from_slice(&temp_buf);
    // println!("[DEBUG] zstd compress: output_buf.len() = {}", output_buf.len());
    // Ok(())

    // before debug code
    // We use the streaming Encoder, which writes directly to the output buffer.
    let mut encoder =
        Encoder::new(output_buf, level).map_err(|e| tambakError::ZstdError(e.to_string()))?;
    encoder
        .write_all(input_bytes)
        .map_err(|e| tambakError::ZstdError(e.to_string()))?;

    // `finish` is essential to finalize the Zstd frame.
    encoder
        .finish()
        .map_err(|e| tambakError::ZstdError(e.to_string()))?;
    Ok(())
}

/// Decompresses a Zstandard-compressed byte slice, writing to an output buffer.
fn decompress_slice(input_bytes: &[u8], output_buf: &mut Vec<u8>) -> Result<(), tambakError> {
    #[cfg(debug_assertions)]
    {
        println!(
            "[DEBUG] zstd decompress: input_bytes.len() = {}",
            input_bytes.len()
        );
    }
    // --- THIS IS THE CORRECTED, ROBUST IMPLEMENTATION ---
    // We create a streaming decoder.
    let mut decoder =
        Decoder::new(input_bytes).map_err(|e| tambakError::ZstdError(e.to_string()))?;

    // We use `std::io::copy`. This function will read all bytes from the
    // decoder and write them to the output buffer. The buffer will grow
    // as needed. This does not rely on any pre-allocation.
    std::io::copy(&mut decoder, output_buf).map_err(|e| tambakError::ZstdError(e.to_string()))?;
    #[cfg(debug_assertions)]
    {
        println!(
            "[DEBUG] zstd decompress: output_buf.len() = {}",
            output_buf.len()
        );
    }
    Ok(())

    // // before debug code
    // // --- THIS IS THE CORRECTED, ROBUST IMPLEMENTATION ---
    // // We create a streaming decoder.
    // let mut decoder = Decoder::new(input_bytes)
    //     .map_err(|e| tambakError::ZstdError(e.to_string()))?;

    // // We use `std::io::copy`. This function will read all bytes from the
    // // decoder and write them to the output buffer. The buffer will grow
    // // as needed. This does not rely on any pre-allocation.
    // std::io::copy(&mut decoder, output_buf)
    //     .map_err(|e| tambakError::ZstdError(e.to_string()))?;
    // Ok(())
}

//==================================================================================
// 2. Public API (Performant, Decoupled)
//==================================================================================

/// The public-facing encode function for this module.
// MODIFIED: The encode function now returns a Vec<u8> and prepends the length.
pub fn encode(input_bytes: &[u8], level: i32) -> Result<Vec<u8>, tambakError> {
    if input_bytes.is_empty() {
        return Ok(Vec::new());
    }

    let mut output_buf = Vec::with_capacity(input_bytes.len());

    // --- FIX: Prepend the uncompressed size ---
    let uncompressed_len: u64 = input_bytes.len() as u64;
    output_buf.extend_from_slice(&uncompressed_len.to_le_bytes());

    // Now, compress the data and append it to the buffer.
    let mut encoder = zstd::stream::Encoder::new(&mut output_buf, level)
        .map_err(|e| tambakError::ZstdError(e.to_string()))?;
    std::io::Write::write_all(&mut encoder, input_bytes)
        .map_err(|e| tambakError::ZstdError(e.to_string()))?;
    encoder
        .finish()
        .map_err(|e| tambakError::ZstdError(e.to_string()))?;

    Ok(output_buf)
}

// MODIFIED: The decode function now returns a Vec<u8> and reads the length header.
pub fn decode(input_bytes: &[u8]) -> Result<Vec<u8>, tambakError> {
    if input_bytes.is_empty() {
        return Ok(Vec::new());
    }

    // --- FIX: Read the uncompressed size header ---
    if input_bytes.len() < 8 {
        return Err(tambakError::ZstdError(
            "Input stream too short to contain size header.".to_string(),
        ));
    }
    let len_bytes: [u8; 8] = input_bytes[0..8].try_into().unwrap();
    let uncompressed_len = u64::from_le_bytes(len_bytes) as usize;

    // The actual compressed data starts *after* the header.
    let compressed_data = &input_bytes[8..];

    let mut decompressed_data = Vec::with_capacity(uncompressed_len);
    zstd::stream::copy_decode(compressed_data, &mut decompressed_data)
        .map_err(|e| tambakError::ZstdError(e.to_string()))?;

    if decompressed_data.len() != uncompressed_len {
        return Err(tambakError::ZstdError(format!(
            "Decompressed size does not match header. Expected {}, got {}.",
            uncompressed_len,
            decompressed_data.len()
        )));
    }

    Ok(decompressed_data)
}

//==================================================================================
// 3. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zstd_roundtrip_simple_text() {
        let original_bytes =
            b"hello world, this is a test of zstd compression. hello world, this is a test."
                .to_vec();

        // --- FIX: Call the new API ---
        let compressed_bytes = encode(&original_bytes, 3).unwrap();

        assert!(compressed_bytes.len() < original_bytes.len());

        // --- FIX: Call the new API ---
        let decompressed_bytes = decode(&compressed_bytes).unwrap();

        assert_eq!(original_bytes, decompressed_bytes);
    }

    #[test]
    fn test_zstd_roundtrip_highly_compressible_data() {
        let original_bytes = vec![42u8; 10_000];

        // --- FIX: Call the new API ---
        let compressed_bytes = encode(&original_bytes, 5).unwrap();

        assert!(compressed_bytes.len() < 50); // The compressed size will be slightly larger due to the 8-byte header.

        // --- FIX: Call the new API ---
        let decompressed_bytes = decode(&compressed_bytes).unwrap();

        assert_eq!(original_bytes, decompressed_bytes);
    }

    #[test]
    fn test_zstd_decompress_invalid_data() {
        let invalid_bytes = vec![1, 2, 3, 4, 5]; // This is too short to be valid.

        // --- FIX: Call the new API ---
        let result = decode(&invalid_bytes);

        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Zstd"));
        }
    }
}
