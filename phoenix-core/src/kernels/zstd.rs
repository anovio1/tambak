//! This module contains the pure, stateless, and performant kernels for performing
//! Zstandard compression and decompression.
//!
//! This is the Final Stage transform in the compression pipeline. It takes a byte
//! buffer that has already been optimized by the preceding layers and applies a
//! high-performance, modern entropy coder to achieve the final compressed size.
//! This module is a safe, panic-free wrapper around the `zstd` crate.

use std::io::Write;

use crate::error::PhoenixError;

//==================================================================================
// 1. Core Logic (The "Engine")
//==================================================================================

/// Compresses a byte slice using the Zstandard algorithm, writing to an output buffer.
///
/// # Args
/// * `input_bytes`: The byte slice to be compressed.
/// * `output_buf`: A mutable vector to which the compressed bytes will be written.
/// * `level`: The compression level for Zstd (e.g., 1-22). A typical default is 3.
///
/// # Returns
/// A `Result` indicating success or failure.
fn compress_slice(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    level: i32,
) -> Result<(), PhoenixError> {
    // The zstd crate's Encoder can write directly to any `io::Write` implementation,
    // which includes `&mut Vec<u8>`. This is highly efficient as it avoids
    // creating an intermediate buffer.
    let mut encoder = zstd::Encoder::new(output_buf, level)
        .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
    encoder.write_all(input_bytes)
        .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
    // `finish` finalizes the Zstd frame and flushes the writer.
    encoder.finish()
        .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
    Ok(())
}

/// Decompresses a Zstandard-compressed byte slice, writing to an output buffer.
///
/// # Args
/// * `input_bytes`: A byte slice containing a valid Zstd frame.
/// * `output_buf`: A mutable vector to which the decompressed bytes will be written.
///
/// # Returns
/// A `Result` indicating success or failure.
fn decompress_slice(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
) -> Result<(), PhoenixError> {
    // The zstd crate's Decoder can also write directly to any `io::Write`.
    let mut decoder = zstd::Decoder::new(input_bytes)
        .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
    
    // Pre-allocate the output buffer if the decoded size is known from the frame header.
    // This is a significant performance optimization.
    let content_size = zstd::decoded_size(input_bytes).unwrap_or(0);
    output_buf.reserve(content_size);

    std::io::copy(&mut decoder, output_buf)
        .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
    Ok(())
}

//==================================================================================
// 2. Public API (Performant, Decoupled)
//==================================================================================
// Note: This kernel operates on raw bytes, so it does not need to be generic.

/// The public-facing encode function for this module.
/// It takes a byte slice and writes the Zstd-compressed result into the output buffer.
pub fn encode(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    level: i32,
) -> Result<(), PhoenixError> {
    output_buf.clear();
    compress_slice(input_bytes, output_buf, level)
}

/// The public-facing decode function for this module.
/// It takes a byte slice of Zstd-compressed data and writes the decompressed
/// bytes into the provided output buffer.
pub fn decode(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
) -> Result<(), PhoenixError> {
    output_buf.clear();
    decompress_slice(input_bytes, output_buf)
}

//==================================================================================
// 3. Unit Tests (Revised to test the new public API pattern)
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zstd_roundtrip_simple_text() {
        let original_bytes = b"hello world, this is a test of zstd compression. hello world, this is a test.".to_vec();

        // --- Test Encode ---
        let mut compressed_bytes = Vec::new();
        encode(&original_bytes, &mut compressed_bytes, 3).unwrap();
        
        assert!(compressed_bytes.len() < original_bytes.len());

        // --- Test Decode ---
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
        let invalid_bytes = vec![1, 2, 3, 4, 5]; // Not a valid Zstd frame
        let mut decompressed_bytes = Vec::new();
        let result = decode(&invalid_bytes, &mut decompressed_bytes);
        
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Zstd decompression error"));
        }
    }
}