//! This module contains the v4.0 kernel for Asymmetric Numeral Systems (ANS)
//! entropy coding.
//!
//! This is a modern, high-performance entropy coder that can provide superior
//! compression ratios to Huffman coding and is often faster than Arithmetic
//! Coding. It will compete with Zstd as a final compression stage. This
//! implementation uses a range-based variant (rANS), which operates as a
//! last-in, first-out (LIFO) state machine on the data.

use crate::error::PhoenixError;

// --- rANS Constants ---
const ANS_NORMALIZATION_FACTOR: u32 = 1 << 12; // 4096
const ANS_STATE_MIN: u32 = 1 << 16;

/// Represents the encoding/decoding information for a single symbol (byte).
#[derive(Debug, Clone, Copy, Default)]
struct AnsSymbol {
    freq: u32,
    cum_freq: u32,
}

/// A pure function to count and normalize symbol frequencies.
/// This improves testability and separates concerns from the encoding logic.
fn build_normalized_symbols(input_bytes: &[u8]) -> Result<([AnsSymbol; 256], Vec<(u8, u16)>), PhoenixError> {
    if input_bytes.is_empty() {
        return Err(PhoenixError::AnsError("Input for frequency normalization cannot be empty".to_string()));
    }

    let mut freqs = [0u32; 256];
    for &byte in input_bytes {
        freqs[byte as usize] += 1;
    }

    let mut norm_symbols_for_header = Vec::with_capacity(256);
    let mut total_norm_freq = 0;
    let total_input_len = input_bytes.len() as f64;

    for (byte, &count) in freqs.iter().enumerate().filter(|(_, &c)| c > 0) {
        let freq = (((count as f64 / total_input_len) * ANS_NORMALIZATION_FACTOR as f64) as u32).max(1);
        norm_symbols_for_header.push((byte as u8, freq as u16));
        total_norm_freq += freq;
    }

    let remainder = ANS_NORMALIZATION_FACTOR.saturating_sub(total_norm_freq);
    if let Some((_, freq)) = norm_symbols_for_header.last_mut() {
        *freq = freq.saturating_add(remainder as u16);
    }

    let mut encode_table = [AnsSymbol::default(); 256];
    let mut cum_freq = 0;
    for (byte, freq) in &norm_symbols_for_header {
        encode_table[*byte as usize] = AnsSymbol { freq: *freq as u32, cum_freq };
        cum_freq += *freq as u32;
    }

    Ok((encode_table, norm_symbols_for_header))
}

/// Encodes a byte slice using a range Asymmetric Numeral Systems model.
pub fn encode(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
) -> Result<(), PhoenixError> {
    output_buf.clear();
    if input_bytes.is_empty() {
        return Ok(());
    }

    // 1. Build the frequency model.
    let (encode_table, header_symbols) = build_normalized_symbols(input_bytes)?;

    // 2. Serialize the frequency table to the output buffer.
    output_buf.push(header_symbols.len() as u8);
    for (byte, freq) in &header_symbols {
        output_buf.push(*byte);
        output_buf.extend_from_slice(&freq.to_le_bytes());
    }

    // 3. Encode the data. rANS encodes in reverse (LIFO).
    let mut state = ANS_STATE_MIN;
    let mut encoded_data = Vec::with_capacity(input_bytes.len());

    for &byte in input_bytes.iter().rev() {
        let symbol = encode_table[byte as usize];
        while state >= symbol.freq * (ANS_STATE_MIN >> 4) {
            encoded_data.push((state & 0xFF) as u8);
            state >>= 8;
        }
        state = (state / symbol.freq) * ANS_NORMALIZATION_FACTOR + (state % symbol.freq) + symbol.cum_freq;
    }

    // 4. Write final state and the encoded data stream.
    output_buf.extend_from_slice(&state.to_le_bytes());
    output_buf.extend(encoded_data.iter().rev());

    Ok(())
}

/// Decodes an ANS-encoded buffer.
pub fn decode(
    input_bytes: &[u8],
    output_buf: &mut Vec<u8>,
) -> Result<(), PhoenixError> {
    output_buf.clear();
    if input_bytes.is_empty() {
        return Ok(());
    }

    // 1. Deserialize and VALIDATE the frequency table.
    let mut cursor = 0;
    let num_symbols = *input_bytes.get(cursor).ok_or_else(|| PhoenixError::AnsError("Truncated header: cannot read symbol count".to_string()))? as usize;
    cursor += 1;

    let mut symbol_table = [AnsSymbol::default(); 256];
    let mut decode_lookup = vec![0u8; ANS_NORMALIZATION_FACTOR as usize];
    let mut total_freq_check = 0u32;

    let mut cum_freq = 0;
    for _ in 0..num_symbols {
        let byte = *input_bytes.get(cursor).ok_or_else(|| PhoenixError::AnsError("Corrupt frequency table: cannot read symbol".to_string()))?;
        cursor += 1;
        
        let freq_bytes: [u8; 2] = input_bytes.get(cursor..cursor+2)
            .ok_or_else(|| PhoenixError::AnsError("Corrupt frequency table: cannot read frequency".to_string()))?
            .try_into().map_err(|_| PhoenixError::AnsError("Corrupt frequency table: invalid slice length".to_string()))?;
        cursor += 2;
        
        let freq = u16::from_le_bytes(freq_bytes) as u32;
        if freq == 0 {
            return Err(PhoenixError::AnsError("Invalid frequency table: frequency cannot be zero".to_string()));
        }
        
        symbol_table[byte as usize] = AnsSymbol { freq, cum_freq };
        total_freq_check += freq;

        if cum_freq + freq > ANS_NORMALIZATION_FACTOR {
            return Err(PhoenixError::AnsError("Invalid frequency table: cumulative frequency exceeds normalization factor".to_string()));
        }
        for i in cum_freq..(cum_freq + freq) {
            decode_lookup[i as usize] = byte;
        }
        cum_freq += freq;
    }

    if total_freq_check != ANS_NORMALIZATION_FACTOR {
        return Err(PhoenixError::AnsError(format!("Invalid frequency table: frequencies sum to {} but should sum to {}", total_freq_check, ANS_NORMALIZATION_FACTOR)));
    }

    // 2. Read initial state and locate the start of the data stream.
    let state_bytes: [u8; 4] = input_bytes.get(cursor..cursor+4)
        .ok_or_else(|| PhoenixError::AnsError("Truncated header: cannot read initial state".to_string()))?
        .try_into().map_err(|_| PhoenixError::AnsError("Corrupt header: invalid slice length for state".to_string()))?;
    let mut state = u32::from_le_bytes(state_bytes);
    cursor += 4;
    let mut data_pos = cursor;

    // 3. Decode the data.
    loop {
        let slot = state % ANS_NORMALIZATION_FACTOR;
        let byte = decode_lookup[slot as usize];
        output_buf.push(byte);

        let symbol = symbol_table[byte as usize];
        state = symbol.freq * (state / ANS_NORMALIZATION_FACTOR) + slot - symbol.cum_freq;

        while state < ANS_STATE_MIN {
            if data_pos >= input_bytes.len() {
                output_buf.reverse();
                return Ok(());
            }
            state = (state << 8) | *input_bytes.get(data_pos).unwrap() as u32; // .unwrap() is safe due to check above
            data_pos += 1;
        }
    }
}

// --- Unit Tests ---
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ans_roundtrip_simple_data() {
        let original = b"mississippi river".to_vec();
        let mut encoded = Vec::new();
        encode(&original, &mut encoded).unwrap();
        assert!(encoded.len() < original.len(), "ANS failed to compress simple repeating text");
        let mut decoded = Vec::new();
        decode(&encoded, &mut decoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_ans_on_low_entropy_data() {
        let mut original = Vec::with_capacity(1000);
        for i in 0..1000 {
            original.push(if i % 4 == 0 { b'A' } else { b'B' });
        }
        let mut encoded = Vec::new();
        encode(&original, &mut encoded).unwrap();
        assert!(encoded.len() < original.len() / 4, "ANS compression ratio is poor for low-entropy data");
        let mut decoded = Vec::new();
        decode(&encoded, &mut decoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_ans_on_high_entropy_data() {
        let original: Vec<u8> = (0..255).collect();
        let mut encoded = Vec::new();
        encode(&original, &mut encoded).unwrap();
        assert!(encoded.len() > original.len(), "ANS should expand incompressible data");
        let mut decoded = Vec::new();
        decode(&encoded, &mut decoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_ans_empty_and_single_byte() {
        let original: Vec<u8> = vec![];
        let mut encoded = Vec::new();
        encode(&original, &mut encoded).unwrap();
        assert!(encoded.is_empty());
        let mut decoded = Vec::new();
        decode(&encoded, &mut decoded).unwrap();
        assert_eq!(decoded, original);

        let original: Vec<u8> = vec![42];
        let mut encoded = Vec::new();
        encode(&original, &mut encoded).unwrap();
        let mut decoded = Vec::new();
        decode(&encoded, &mut decoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decode_corrupt_data() {
        let mut decoded = Vec::new();
        let err1 = decode(&[0], &mut decoded).unwrap_err();
        assert!(matches!(err1, PhoenixError::AnsError(_)));

        let err2 = decode(&[1, 97], &mut decoded).unwrap_err();
        assert!(matches!(err2, PhoenixError::AnsError(_)));

        let err3 = decode(&[1, 97, 0, 1, 0, 0], &mut decoded).unwrap_err();
        assert!(matches!(err3, PhoenixError::AnsError(_)));
    }

    #[test]
    fn test_decode_invalid_frequency_sum() {
        let original = b"abc".to_vec();
        let mut encoded = Vec::new();
        encode(&original, &mut encoded).unwrap();
        
        // Corrupt the frequency of the first symbol to break the sum
        // Header: [num_symbols (u8)] [ (symbol (u8), freq (u16)) ... ]
        // Original freq for 'a' will be ~1365 (0x0555). Let's change it.
        let new_freq: u16 = 1000;
        encoded[2..4].copy_from_slice(&new_freq.to_le_bytes());

        let mut decoded = Vec::new();
        let err = decode(&encoded, &mut decoded).unwrap_err();
        assert!(matches!(err, PhoenixError::AnsError(_)));
        assert!(err.to_string().contains("frequencies sum to"));
    }
}