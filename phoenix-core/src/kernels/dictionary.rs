//! This module contains the v4.0 kernel for Dictionary Encoding.
//!
//! This is a Layer 2 (Sparsity Exploitation) transform. It is highly effective
//! for data with low cardinality (a small set of unique values). It works by
//! creating a "dictionary" of the unique values and replacing the data itself
//! with a much smaller stream of indices pointing to the dictionary entries.

use crate::error::PhoenixError;
use bytemuck::{bytes_of, Pod};
use std::collections::HashMap;
use std::hash::Hash;

/// Encodes a slice of primitive data using dictionary encoding.
///
/// This function is called by the dispatcher, which has already converted a raw
/// byte buffer into a typed slice. This function's responsibility is to convert
/// that simple typed stream into the custom dictionary format.
///
/// ### On-Disk Format
/// 1.  **Dictionary Size (`u32`)**: The number of unique entries in the dictionary.
/// 2.  **Dictionary Bytes (`[u8]`)**: The tightly packed bytes of the unique values themselves.
/// 3.  **Indices (`[u32]`)**: The sequence of `u32` indices corresponding to the original data.
///
/// # Type Parameters
/// * `T`: A type that is "Plain Old Data" and can be used in a `HashMap`.
///
/// # Args
/// * `input_slice`: The slice of typed data (e.g., `&[i32]`) to encode.
/// * `output_buf`: The buffer to write the custom dictionary-encoded artifact into.
///
/// # Returns
/// An empty `Result` on success, or a `PhoenixError` on failure.
pub fn encode<T: Eq + Hash + Clone + Pod>(
    input_slice: &[T],
    output_buf: &mut Vec<u8>,
) -> Result<(), PhoenixError> {
    output_buf.clear();
    if input_slice.is_empty() {
        return Ok(());
    }

    // Using u32 for indices allows for up to ~4.2 billion unique values in a
    // single chunk, which is more than sufficient for any practical purpose.
    let mut dictionary_map = HashMap::<T, u32>::new();
    let mut dictionary_vec = Vec::new();
    let mut indices = Vec::with_capacity(input_slice.len());

    for value in input_slice {
        let index = *dictionary_map.entry(value.clone()).or_insert_with(|| {
            let next_id = dictionary_vec.len() as u32;
            dictionary_vec.push(value.clone());
            next_id
        });
        indices.push(index);
    }

    // Serialize the components into the output buffer according to the format.
    let dict_bytes = bytemuck::cast_slice(&dictionary_vec);
    output_buf.extend_from_slice(&(dictionary_vec.len() as u32).to_le_bytes());
    output_buf.extend_from_slice(dict_bytes);
    output_buf.extend_from_slice(bytemuck::cast_slice(&indices));

    Ok(())
}

/// Decodes a dictionary-encoded buffer back into its original values.
///
/// This function's responsibility is to parse the custom dictionary format from
/// `input_bytes` and reconstruct the simple, typed data stream, writing it as
/// raw bytes into `output_buf`.
///
/// # Args
/// * `input_bytes`: The dictionary-encoded byte slice in our custom format.
/// * `output_buf`: The buffer to write the decoded typed data into (as bytes).
/// * `num_values`: The total number of values expected in the final decoded output.
///
/// # Returns
/// An empty `Result` on success, or a `PhoenixError` if the buffer is corrupt
/// or malformed.
pub fn decode<T: Copy + Pod>( // Keep Copy trait bound for now
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
    num_values: usize,
) -> Result<(), PhoenixError> {
    output_buf.clear();
    if num_values == 0 {
        return Ok(());
    }

    let element_size = std::mem::size_of::<T>();
    let u32_size = std::mem::size_of::<u32>();

    // --- Parsing Dictionary Length (This part is fine) ---
    let mut cursor = 0;
    let dict_len_bytes: [u8; 4] = input_bytes
        .get(cursor..cursor + u32_size)
        .and_then(|slice| slice.try_into().ok())
        .ok_or_else(|| PhoenixError::DictionaryError("Truncated header: cannot read dictionary length".to_string()))?;
    let dict_len = u32::from_le_bytes(dict_len_bytes) as usize;
    cursor += u32_size;

    // --- DICTIONARY DESERIALIZATION: THE BULLETPROOF FIX ---
    let dict_bytes_len = dict_len * element_size;
    let dict_bytes = input_bytes
        .get(cursor..cursor + dict_bytes_len)
        .ok_or_else(|| PhoenixError::DictionaryError("Truncated dictionary data".to_string()))?;
    cursor += dict_bytes_len;

    let mut dictionary = Vec::with_capacity(dict_len);
    for chunk in dict_bytes.chunks_exact(element_size) {
        // Create a default-initialized value of T on the stack.
        // The stack is guaranteed to be aligned correctly for T.
        let mut value = T::zeroed(); 
        unsafe {
            // Get a mutable pointer to our stack-allocated, aligned value.
            let dest_ptr = &mut value as *mut T as *mut u8;
            // Get a pointer to the (potentially unaligned) source bytes.
            let src_ptr = chunk.as_ptr();
            // Perform a raw, but safe, memory copy.
            std::ptr::copy_nonoverlapping(src_ptr, dest_ptr, element_size);
        }
        dictionary.push(value);
    }

    // --- INDICES DESERIALIZATION: THE BULLETPROOF FIX ---
    let indices_bytes = input_bytes
        .get(cursor..)
        .ok_or_else(|| PhoenixError::DictionaryError("Missing indices data".to_string()))?;
    
    if indices_bytes.len() % u32_size != 0 {
        return Err(PhoenixError::DictionaryError(
            "Corrupt indices data: length not a multiple of 4".to_string(),
        ));
    }
    
    let mut indices = Vec::with_capacity(indices_bytes.len() / u32_size);
    for chunk in indices_bytes.chunks_exact(u32_size) {
        let value = u32::from_le_bytes(chunk.try_into().unwrap());
        indices.push(value);
    }
    // --- END FIX ---

    if indices.len() != num_values {
        return Err(PhoenixError::DictionaryError(format!(
            "Expected {} values, but found {} indices",
            num_values,
            indices.len()
        )));
    }

    output_buf.reserve(num_values * element_size);
    for &index in &indices {
        let value = dictionary.get(index as usize).ok_or_else(|| {
            PhoenixError::DictionaryError(format!(
                "Invalid dictionary index: {} (dictionary size is {})",
                index, dict_len
            ))
        })?;
        output_buf.extend_from_slice(bytes_of(value));
    }

    Ok(())
}

//==================================================================================
// 3. Unit Tests
//==================================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::{safe_bytes_to_typed_slice, typed_slice_to_bytes};

    #[test]
    fn test_dictionary_roundtrip_i32() {
        let original: Vec<i32> = vec![100, 200, 100, 300, 200, 200, 100];
        let mut encoded = Vec::new();
        encode(&original, &mut encoded).unwrap();

        // The dictionary should be [100, 200, 300] and there are 7 indices.
        // 3 * 4 (dict) + 7 * 4 (indices) + 4 (dict_len) = 12 + 28 + 4 = 44 bytes.
        // Original was 7 * 4 = 28 bytes. In this case, it's larger, but still correct.
        // A real-world case would have much higher repetition.
        assert!(!encoded.is_empty());

        let mut decoded_bytes = Vec::new();
        decode::<i32>(&encoded, &mut decoded_bytes, original.len()).unwrap();

        let original_bytes = typed_slice_to_bytes(&original);
        assert_eq!(decoded_bytes, original_bytes);
    }

    #[test]
    fn test_dictionary_roundtrip_u64() {
        let original: Vec<u64> = vec![5, 8, 5, 5, 5, 8];
        let mut encoded = Vec::new();
        encode(&original, &mut encoded).unwrap();

        let mut decoded_bytes = Vec::new();
        decode::<u64>(&encoded, &mut decoded_bytes, original.len()).unwrap();

        let original_bytes = typed_slice_to_bytes(&original);
        assert_eq!(decoded_bytes, original_bytes);
    }

    #[test]
    fn test_dictionary_all_unique_values() {
        let original: Vec<i16> = vec![1, 2, 3, 4, 5];
        let mut encoded = Vec::new();
        encode(&original, &mut encoded).unwrap();

        let mut decoded_bytes = Vec::new();
        decode::<i16>(&encoded, &mut decoded_bytes, original.len()).unwrap();

        let original_bytes = typed_slice_to_bytes(&original);
        assert_eq!(decoded_bytes, original_bytes);
    }

    #[test]
    fn test_empty_and_single_value_slices() {
        // Empty
        let original: Vec<i32> = vec![];
        let mut encoded = Vec::new();
        encode(&original, &mut encoded).unwrap();
        assert!(encoded.is_empty());
        let mut decoded_bytes = Vec::new();
        decode::<i32>(&encoded, &mut decoded_bytes, 0).unwrap();
        assert!(decoded_bytes.is_empty());

        // Single value
        let original: Vec<i32> = vec![42];
        let mut encoded = Vec::new();
        encode(&original, &mut encoded).unwrap();
        let mut decoded_bytes = Vec::new();
        decode::<i32>(&encoded, &mut decoded_bytes, 1).unwrap();
        assert_eq!(decoded_bytes, typed_slice_to_bytes(&original));
    }

    #[test]
    fn test_decode_errors() {
        // Truncated header
        let mut decoded_bytes = Vec::new();
        let err = decode::<i32>(&[0], &mut decoded_bytes, 1).unwrap_err();
        assert!(matches!(err, PhoenixError::DictionaryError(_)));
        assert!(err.to_string().contains("Truncated header"));

        // Truncated dictionary
        let err = decode::<i32>(&[1, 0, 0, 0, 42, 0], &mut decoded_bytes, 1).unwrap_err();
        assert!(matches!(err, PhoenixError::DictionaryError(_)));
        assert!(err.to_string().contains("Truncated dictionary data"));

        // Invalid index
        let original: Vec<i32> = vec![100]; // Dictionary will have one entry (index 0)
        let mut encoded = Vec::new();
        encode(&original, &mut encoded).unwrap();
        // Manually corrupt the index to be out of bounds
        let len = encoded.len();
        encoded[len - 4..].copy_from_slice(&5u32.to_le_bytes()); // Set index to 5
        let err = decode::<i32>(&encoded, &mut decoded_bytes, 1).unwrap_err();
        assert!(matches!(err, PhoenixError::DictionaryError(_)));
        assert!(err.to_string().contains("Invalid dictionary index: 5"));
    }
}
