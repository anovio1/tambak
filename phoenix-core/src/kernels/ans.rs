//! This module contains a production-grade kernel for Asymmetric Numeral Systems (ANS)
//! entropy coding.
//!
//! This is a modern, high-performance entropy coder that can provide superior
//! compression ratios to Huffman coding and is often faster than Arithmetic
//! Coding. This implementation uses a range-based variant (rANS), which operates
//! as a last-in, first-out (LIFO) state machine on the data.
//!
//! ## Stream Format
//!
//! The encoded bytestream is self-contained and has the following structure:
//! 1.  **Uncompressed Size (8 bytes):** The original length of the data as a `u64` little-endian integer.
//! 2.  **Frequency Header:**
//!     -   Symbol Count (2 bytes): The number of unique symbols (`u16` LE).
//!     -   Symbol Table (variable): A sequence of (symbol, frequency) pairs.
//!         -   Symbol (1 byte): The byte value.
//!         -   Frequency (2 bytes): The normalized frequency of the symbol (`u16` LE).
//! 3.  **Final rANS State (4 bytes):** The final `u32` state of the encoder (LE).
//! 4.  **Data Payload (variable):** The compressed rANS data stream.

use crate::error::PhoenixError;

// --- rANS Constants ---
const ANS_NORMALIZATION_FACTOR: u32 = 1 << 12; // 4096, the total frequency budget.
const ANS_STATE_MIN: u32 = 1 << 16; // Minimum state value before renormalization.

/// Represents the encoding/decoding information for a single symbol (byte).
#[derive(Clone, Copy, Default)]
struct AnsSymbol {
    freq: u32,
    cum_freq: u32,
}

/// Analyzes input data to build a normalized frequency model for ANS encoding.
///s
/// This function counts symbol occurrences, normalizes them to a fixed total
/// (`ANS_NORMALIZATION_FACTOR`), and prepares tables for both encoding and header serialization.
/// Analyzes input data to build a normalized frequency model for ANS encoding.
///
/// This function counts symbol occurrences and normalizes them to a fixed total
/// (`ANS_NORMALIZATION_FACTOR`) using robust integer arithmetic to avoid floating-point
/// rounding errors.
fn build_normalized_symbols(
    input_bytes: &[u8],
) -> Result<([AnsSymbol; 256], Vec<(u8, u16)>), PhoenixError> {
    if input_bytes.is_empty() {
        return Err(PhoenixError::AnsError(
            "Cannot build frequency model from empty input.".to_string(),
        ));
    }

    let mut freqs = [0u64; 256];
    for &byte in input_bytes {
        freqs[byte as usize] += 1;
    }

    let unique_symbols: Vec<_> = freqs
        .iter()
        .enumerate()
        .filter(|(_, &c)| c > 0)
        .map(|(byte, &count)| (byte as u8, count))
        .collect();

    if unique_symbols.len() <= 1 {
        return Err(PhoenixError::AnsError(
            "ANS requires at least two unique symbols.".to_string(),
        ));
    }

    let total_input_len = input_bytes.len() as u64;
    let mut norm_symbols_for_header = Vec::with_capacity(unique_symbols.len());
    let mut total_norm_freq = 0;

    // --- Integer-based Normalization ---
    // This calculates `(count * NORM_FACTOR) / total_len` for each symbol.
    // This method is robust against floating point errors. The sum of these
    // frequencies is guaranteed to be <= ANS_NORMALIZATION_FACTOR.
    for (byte, count) in &unique_symbols {
        let freq = ((count * ANS_NORMALIZATION_FACTOR as u64) / total_input_len) as u32;
        // Ensure every symbol present gets at least one slot.
        let freq = freq.max(1);
        norm_symbols_for_header.push(((*byte, freq as u16)));
        total_norm_freq += freq;
    }

    // --- Distribute Remainder ---
    // The integer division above might leave a remainder. We distribute this
    // remainder to the most frequent symbols to maintain an accurate probability model.
    let mut remainder = ANS_NORMALIZATION_FACTOR.saturating_sub(total_norm_freq);

    let num_symbols_to_distribute = norm_symbols_for_header.len();
    if remainder > 0 && num_symbols_to_distribute > 0 {
        // Sort by frequency (desc) to give remainder to most probable symbols first.
        norm_symbols_for_header.sort_unstable_by_key(|&(_, freq)| core::cmp::Reverse(freq));

        for i in 0..remainder as usize {
            // We now use the pre-calculated length, avoiding the simultaneous borrow.
            let entry = &mut norm_symbols_for_header[i % num_symbols_to_distribute];
            entry.1 += 1;
        }
    }
    // Sort back by symbol value to ensure a canonical header format.
    norm_symbols_for_header.sort_unstable_by_key(|&(byte, _)| byte);

    // --- Build Final Encoding Table ---
    let mut encode_table = [AnsSymbol::default(); 256];
    let mut cum_freq = 0;
    for (byte, freq) in &norm_symbols_for_header {
        encode_table[*byte as usize] = AnsSymbol {
            freq: *freq as u32,
            cum_freq,
        };
        cum_freq += *freq as u32;
    }

    Ok((encode_table, norm_symbols_for_header))
}

/// Encodes a byte slice using a range Asymmetric Numeral Systems model.
///
/// The encoder processes the input in reverse (LIFO) to allow the decoder
/// to produce output in the correct forward order.
///
/// # Returns
/// A `Result` containing the compressed `Vec<u8>` or an `PhoenixError`.
pub fn encode(input_bytes: &[u8]) -> Result<Vec<u8>, PhoenixError> {
    if input_bytes.is_empty() {
        return Ok(Vec::new());
    }

    let (encode_table, header_symbols) = build_normalized_symbols(input_bytes)?;

    // Pre-allocate buffer with a reasonable guess for the final size.
    let mut output_buf = Vec::with_capacity(input_bytes.len() / 2);

    // 1. Write uncompressed size header.
    let uncompressed_len: u64 = input_bytes.len().try_into().map_err(|_| {
        PhoenixError::AnsError("Input data length exceeds u64 capacity.".to_string())
    })?;
    output_buf.extend_from_slice(&uncompressed_len.to_le_bytes());

    // 2. Write frequency table header.
    let num_symbols: u16 = header_symbols.len() as u16;
    output_buf.extend_from_slice(&num_symbols.to_le_bytes());
    for (byte, freq) in &header_symbols {
        output_buf.push(*byte);
        output_buf.extend_from_slice(&freq.to_le_bytes());
    }

    // --- Core rANS Encoding Loop ---
    let mut state = ANS_STATE_MIN;
    // Temporary buffer for the encoded payload, which is built in reverse.
    let mut encoded_data = Vec::with_capacity(input_bytes.len());

    // Process input backwards (LIFO).
    for &byte in input_bytes.iter().rev() {
        let symbol = encode_table[byte as usize];

        // Renormalize state if it's too large, streaming out low bytes.
        while state >= symbol.freq * (ANS_STATE_MIN >> 4) {
            encoded_data.push((state & 0xFF) as u8);
            state >>= 8;
        }

        // "Push" the symbol's information onto the state.
        state = (state / symbol.freq) * ANS_NORMALIZATION_FACTOR
            + (state % symbol.freq)
            + symbol.cum_freq;
    }

    // 3. Write final state.
    output_buf.extend_from_slice(&state.to_le_bytes());

    // 4. Write data payload.
    // We reverse the payload because it was generated backwards. This allows
    // the decoder to read it forwards linearly.
    output_buf.extend(encoded_data.iter().rev());

    Ok(output_buf)
}

/// Decodes an ANS-encoded buffer.
///
/// This function reads the self-contained stream, reconstructs the frequency model,
/// and decodes the data until the original uncompressed size is reached.
///
/// # Returns
/// A `Result` containing the decompressed `Vec<u8>` or an `PhoenixError`.
pub fn decode(input_bytes: &[u8]) -> Result<Vec<u8>, PhoenixError> {
    if input_bytes.is_empty() {
        return Ok(Vec::new());
    }

    let mut cursor = 0;

    // Helper to read a fixed number of bytes from the input slice.
    let mut read_bytes = |len: usize| -> Result<&[u8], PhoenixError> {
        let end = cursor + len;
        let slice = input_bytes.get(cursor..end).ok_or_else(|| {
            PhoenixError::AnsError("Input stream is truncated or corrupt.".to_string())
        })?;
        cursor = end;
        Ok(slice)
    };

    // 1. Read uncompressed size header.
    let len_bytes: [u8; 8] = read_bytes(8)?.try_into().unwrap(); // Should not fail
    let num_values_to_decode = u64::from_le_bytes(len_bytes) as usize;

    if num_values_to_decode == 0 {
        return Ok(Vec::new());
    }

    // Pre-allocate the output buffer for maximum performance.
    let mut output_buf = Vec::with_capacity(num_values_to_decode);

    // 2. Read frequency table header.
    let num_symbols_bytes: [u8; 2] = read_bytes(2)?.try_into().unwrap();
    let num_symbols = u16::from_le_bytes(num_symbols_bytes) as usize;

    let mut symbol_table = [AnsSymbol::default(); 256];
    let mut decode_lookup = vec![0u8; ANS_NORMALIZATION_FACTOR as usize];
    let mut cum_freq = 0;

    for _ in 0..num_symbols {
        let byte = read_bytes(1)?[0];
        let freq_bytes: [u8; 2] = read_bytes(2)?.try_into().unwrap();
        let freq = u16::from_le_bytes(freq_bytes) as u32;

        symbol_table[byte as usize] = AnsSymbol { freq, cum_freq };

        // Fill the fast lookup table for decoding.
        let end_slot = cum_freq + freq;
        for j in (cum_freq as usize)..(end_slot as usize) {
            decode_lookup[j] = byte;
        }
        cum_freq = end_slot;
    }

    if cum_freq != ANS_NORMALIZATION_FACTOR {
        return Err(PhoenixError::AnsError(format!(
            "Corrupt header: frequencies sum to {} but should be {}.",
            cum_freq, ANS_NORMALIZATION_FACTOR
        )));
    }

    // 3. Read initial state.
    let state_bytes: [u8; 4] = read_bytes(4)?.try_into().unwrap();
    let mut state = u32::from_le_bytes(state_bytes);

    // Pointer to the start of the compressed data payload.
    let mut data_ptr = cursor;

    // --- Core rANS Decoding Loop ---
    while output_buf.len() < num_values_to_decode {
        // "Pop" a symbol's information from the state.
        let slot = state % ANS_NORMALIZATION_FACTOR;
        let byte = decode_lookup[slot as usize];
        output_buf.push(byte);

        let symbol = symbol_table[byte as usize];
        state = symbol.freq * (state / ANS_NORMALIZATION_FACTOR) + slot - symbol.cum_freq;

        // Renormalize state if it's too small, reading from the payload.
        while state < ANS_STATE_MIN {
            state = (state << 8)
                | (*input_bytes.get(data_ptr).ok_or_else(|| {
                    PhoenixError::AnsError("Input stream is truncated or corrupt.".to_string())
                })? as u32);
            data_ptr += 1;
        }
    }

    Ok(output_buf)
}

// --- Unit Tests (Updated for new API) ---
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ans_roundtrip_simple_data() {
        let original = b"mississippi river".to_vec();
        let encoded = encode(&original).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_ans_on_low_entropy_data() {
        let mut original = Vec::with_capacity(1000);
        for i in 0..1000 {
            original.push(if i % 4 == 0 { b'A' } else { b'B' });
        }
        let encoded = encode(&original).unwrap();
        assert!(encoded.len() < original.len() / 2); // Check for reasonable compression.
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_ans_on_high_entropy_data() {
        let original: Vec<u8> = (0..=255).collect();
        let encoded = encode(&original).unwrap();
        assert!(encoded.len() > original.len()); // Should expand incompressible data.
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_ans_empty_and_single_symbol() {
        // Empty case
        let original_empty: Vec<u8> = vec![];
        let encoded_empty = encode(&original_empty).unwrap();
        assert!(encoded_empty.is_empty());
        let decoded_empty = decode(&encoded_empty).unwrap();
        assert_eq!(decoded_empty, original_empty);

        // Single symbol case (should fail to encode)
        let original_single: Vec<u8> = vec![42; 10];
        let result = encode(&original_single);
        assert!(
            matches!(result, Err(PhoenixError::AnsError(msg)) if msg.contains("at least two unique symbols"))
        );
    }

    #[test]
    fn test_decode_corrupt_data_truncated() {
        let original = b"some data".to_vec();
        let encoded = encode(&original).unwrap();

        // Truncate the stream at various points.
        for i in 1..encoded.len() {
            let truncated_stream = &encoded[..i];
            let result = decode(truncated_stream);
            assert!(
                matches!(&result, Err(PhoenixError::AnsError(msg)) if msg.contains("truncated or corrupt")),
                "Failed on length {}",
                i
            );
        }
    }

    #[test]
    fn test_decode_invalid_frequency_sum() {
        let mut encoded = encode(&b"abc".to_vec()).unwrap();
        // Manually corrupt the frequency of the first symbol (byte 'a').
        // Original header: len(8) + num_sym(2) + 'a'(1) + freq(2) = 13 bytes
        let freq_pos = 8 + 2 + 1;
        encoded[freq_pos..freq_pos + 2].copy_from_slice(&1000u16.to_le_bytes());

        let result = decode(&encoded);
        assert!(
            matches!(&result, Err(PhoenixError::AnsError(msg)) if msg.contains("Corrupt header: frequencies sum to"))
        );
    }
}
