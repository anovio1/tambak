//! This module contains the pure, stateless kernels for performing Zstandard
//! compression and decompression.
//!
//! This is the final stage in the compression pipeline. It takes a byte buffer
//! that has already been optimized by the preceding layers (delta, RLE, zigzag, etc.)
//! and applies a high-performance, modern entropy coder to achieve the final
//! compressed size. This module acts as a direct, safe wrapper around the `zstd` crate.

use pyo3::PyResult;
use std::io::{Read, Write};

use crate::error::PhoenixError;

//==================================================================================
// 1. Core Logic (Direct Wrappers)
//==================================================================================

/// Compresses a byte slice using the Zstandard algorithm.
///
/// # Args
/// * `data`: The byte slice to be compressed.
/// * `level`: The compression level for Zstd (e.g., 1-22). A typical default is 3.
///   Higher levels provide better compression at the cost of CPU time.
///
/// # Returns
/// A `Result` containing the Zstd-compressed byte vector.
pub fn compress(data: &[u8], level: i32) -> Result<Vec<u8>, PhoenixError> {
    let mut encoder = zstd::Encoder::new(Vec::new(), level)
        .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
    encoder.write_all(data)
        .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
    encoder.finish()
        .map_err(|e| PhoenixError::ZstdError(e.to_string()))
}

/// Decompresses a Zstandard-compressed byte slice.
///
/// # Args
/// * `data`: A byte slice containing a valid Zstd frame.
///
/// # Returns
/// A `Result` containing the original, decompressed bytes.
pub fn decompress(data: &[u8]) -> Result<Vec<u8>, PhoenixError> {
    let mut decoder = zstd::Decoder::new(data)
        .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
    let mut decoded_data = Vec::with_capacity(zstd::decoded_size(data).unwrap_or(0));
    decoder.read_to_end(&mut decoded_data)
        .map_err(|e| PhoenixError::ZstdError(e.to_string()))?;
    Ok(decoded_data)
}

//==================================================================================
// 2. FFI Dispatcher Logic (Simple Wrappers)
//==================================================================================
// Note: For Zstd, the functions operate on raw bytes, so no complex dispatching
// based on `original_type` is needed. The public functions can directly call
// the core logic.

/// The public-facing encode function for this module.
pub fn encode(bytes: &[u8], level: i32) -> PyResult<Vec<u8>> {
    compress(bytes, level).map_err(|e| e.into())
}

/// The public-facing decode function for this module.
pub fn decode(bytes: &[u8]) -> PyResult<Vec<u8>> {
    decompress(bytes).map_err(|e| e.into())
}

//==================================================================================
// 3. Unit Tests
//==================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zstd_roundtrip_simple_text() {
        let original_str = "hello world, this is a test of zstd compression. hello world, this is a test.";
        let original_bytes = original_str.as_bytes();

        let compressed_bytes = compress(original_bytes, 3).unwrap();
        
        // Assert that compression actually happened
        assert!(compressed_bytes.len() < original_bytes.len());

        let decompressed_bytes = decompress(&compressed_bytes).unwrap();

        assert_eq!(original_bytes, decompressed_bytes.as_slice());
    }

    #[test]
    fn test_zstd_roundtrip_highly_compressible_data() {
        let original_bytes = vec![0u8; 10_000]; // 10KB of zeros

        let compressed_bytes = compress(&original_bytes, 5).unwrap();
        
        // Expect a very high compression ratio
        assert!(compressed_bytes.len() < 50);

        let decompressed_bytes = decompress(&compressed_bytes).unwrap();

        assert_eq!(original_bytes, decompressed_bytes);
    }

    #[test]
    fn test_zstd_roundtrip_incompressible_data() {
        // Random data is effectively incompressible
        let original_bytes: Vec<u8> = (0..1000).map(|_| rand::random::<u8>()).collect();

        let compressed_bytes = compress(&original_bytes, 1).unwrap();
        
        // Zstd may add a small header, so size might be slightly larger
        // but it shouldn't be significantly larger.
        assert!(compressed_bytes.len() >= original_bytes.len());

        let decompressed_bytes = decompress(&compressed_bytes).unwrap();

        assert_eq!(original_bytes, decompressed_bytes);
    }

    #[test]
    fn test_zstd_decompress_invalid_data() {
        let invalid_bytes = vec![1, 2, 3, 4, 5]; // Not a valid Zstd frame
        let result = decompress(&invalid_bytes);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Zstd decompression error"));
        }
    }

    #[test]
    fn test_ffi_wrapper_functions() {
        let original_bytes = b"some test data for the ffi wrapper".to_vec();
        let level = 3;

        let compressed = encode(&original_bytes, level).unwrap();
        let decompressed = decode(&compressed).unwrap();

        assert_eq!(original_bytes, decompressed);
    }
}