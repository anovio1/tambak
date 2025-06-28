//! This module contains the pure, stateless, and performant kernels for performing
//! Zstandard compression and decompression.
//!
//! This is the Final Stage transform in the compression pipeline. It takes a byte
//! buffer that has already been optimized by the preceding layers and applies a
//! high-performance, modern entropy coder to achieve the final compressed size.
//! This module is a safe, panic-free wrapper around the `zstd` crate.

use std::io::Write;
use zstd::stream::{Decoder, Encoder};

use crate::error::PhoenixError;

//==================================================================================
// 1. Core Logic (The "Engine")
//==================================================================================

/// Compresses a byte slice using the Zstandard algorithm, writing to an output buffer.
fn compress_slice(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    level: i32,
) -> Result<(), PhoenixError> {
    // println!("[DEBUG] zstd compress: input_bytes.len() = {}", input_bytes.len());
    // // Use a temporary buffer for encoding.
    // let mut temp_buf = Vec::new();
    // let mut encoder = Encoder::new(&mut temp_buf, level)
    //     .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
    // encoder.write_all(input_bytes)
    //     .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;

    // // `finish` is essential to finalize the Zstd frame.
    // encoder.finish()
    //     .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
    // output_buf.extend_from_slice(&temp_buf);
    // println!("[DEBUG] zstd compress: output_buf.len() = {}", output_buf.len());
    // Ok(())

    // before debug code
    // We use the streaming Encoder, which writes directly to the output buffer.
    let mut encoder =
        Encoder::new(output_buf, level).map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
    encoder
        .write_all(input_bytes)
        .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;

    // `finish` is essential to finalize the Zstd frame.
    encoder
        .finish()
        .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
    Ok(())
}

/// Decompresses a Zstandard-compressed byte slice, writing to an output buffer.
fn decompress_slice(input_bytes: &[u8], output_buf: &mut Vec<u8>) -> Result<(), PhoenixError> {
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
        Decoder::new(input_bytes).map_err(|e| PhoenixError::ZstdError(e.to_string()))?;

    // We use `std::io::copy`. This function will read all bytes from the
    // decoder and write them to the output buffer. The buffer will grow
    // as needed. This does not rely on any pre-allocation.
    std::io::copy(&mut decoder, output_buf).map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
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
    //     .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;

    // // We use `std::io::copy`. This function will read all bytes from the
    // // decoder and write them to the output buffer. The buffer will grow
    // // as needed. This does not rely on any pre-allocation.
    // std::io::copy(&mut decoder, output_buf)
    //     .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
    // Ok(())
}

//==================================================================================
// 2. Public API (Performant, Decoupled)
//==================================================================================

/// The public-facing encode function for this module.
pub fn encode(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    level: i32,
) -> Result<(), PhoenixError> {
    output_buf.clear();
    compress_slice(input_bytes, output_buf, level)
}

/// The public-facing decode function for this module.
pub fn decode(input_bytes: &[u8], output_buf: &mut Vec<u8>) -> Result<(), PhoenixError> {
    output_buf.clear();
    decompress_slice(input_bytes, output_buf)
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

        let mut compressed_bytes = Vec::new();
        encode(&original_bytes, &mut compressed_bytes, 3).unwrap();

        assert!(compressed_bytes.len() < original_bytes.len());

        let mut decompressed_bytes = Vec::new();
        decode(&compressed_bytes, &mut decompressed_bytes).unwrap();

        assert_eq!(original_bytes, decompressed_bytes);
    }

    #[test]
    fn test_zstd_roundtrip_highly_compressible_data() {
        let original_bytes = vec![42u8; 10_000];

        let mut compressed_bytes = Vec::new();
        encode(&original_bytes, &mut compressed_bytes, 5).unwrap();

        assert!(compressed_bytes.len() < 50);

        let mut decompressed_bytes = Vec::new();
        decode(&compressed_bytes, &mut decompressed_bytes).unwrap();

        assert_eq!(original_bytes, decompressed_bytes);
    }

    #[test]
    fn test_zstd_decompress_invalid_data() {
        let invalid_bytes = vec![1, 2, 3, 4, 5];
        let mut decompressed_bytes = Vec::new();
        let result = decode(&invalid_bytes, &mut decompressed_bytes);

        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Zstd"));
        }
    }
}
