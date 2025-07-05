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

use std::cmp::Ordering;

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

    let mut unique_symbols: Vec<(u8, u64)> = freqs
        .iter()
        .enumerate()
        .filter(|(_, &c)| c > 0)
        .map(|(byte, &count)| (byte as u8, count))
        .collect();

    // --- DEBUG BLOCK #1 ---
    #[cfg(debug_assertions)]
    {
        println!("\n==========================================================");
        println!(
            "[ANS_DEBUG] Optimized-Robust Normalization started for stream of {} bytes.",
            input_bytes.len()
        );
        println!(
            "[ANS_DEBUG]   Found {} unique symbols.",
            unique_symbols.len()
        );
    }

    if unique_symbols.len() <= 1 {
        return Err(PhoenixError::AnsError(
            "ANS requires at least two unique symbols.".to_string(),
        ));
    }

    let total_input_len = input_bytes.len() as u64;

    // --- The Correct, Integer-Only Largest Remainder Method ---

    // Step 1: Calculate base frequencies and remainders using integer arithmetic.
    let mut norm_symbols: Vec<(u8, u16, u64)> = Vec::with_capacity(unique_symbols.len());
    let mut current_sum: u32 = 0;

    for (byte, count) in &unique_symbols {
        let base_freq = ((count * ANS_NORMALIZATION_FACTOR as u64) / total_input_len) as u16;
        let remainder = (count * ANS_NORMALIZATION_FACTOR as u64) % total_input_len;
        norm_symbols.push((*byte, base_freq, remainder));
        current_sum += base_freq as u32;
    }

    // Step 2: Distribute the deficit based on the largest remainders using an O(N) algorithm.
    let remainder_to_distribute = ANS_NORMALIZATION_FACTOR - current_sum;

    // --- DEBUG BLOCK #2 ---
    #[cfg(debug_assertions)]
    {
        println!(
            "[ANS_DEBUG]   Base sum: {}. Distributing remaining {} slots.",
            current_sum, remainder_to_distribute
        );
    }

    // --- PERFORMANCE OPTIMIZATION ---
    // Instead of a full O(N log N) sort, we use a O(N) partial sort (select_nth_unstable_by)
    // to find the `remainder_to_distribute` symbols with the largest remainders.
    if remainder_to_distribute > 0 {
        let num_to_promote = remainder_to_distribute as usize;

        // Only partition if we aren't promoting every single symbol.
        // If num_to_promote >= norm_symbols.len(), every symbol gets +1 (or more),
        // so no partitioning is needed. The loop below handles this case.
        if num_to_promote < norm_symbols.len() {
            // This partitions the slice such that the top `num_to_promote` elements
            // (by descending remainder) are at the start of the slice.
            norm_symbols.select_nth_unstable_by(num_to_promote - 1, |a, b| {
                // We want descending order of remainder (item .2), so we compare b to a.
                b.2.cmp(&a.2)
            });
        }

        // Give +1 to the top `num_to_promote` symbols. The .min() handles the
        // case where remainder_to_distribute >= norm_symbols.len().
        for i in 0..num_to_promote.min(norm_symbols.len()) {
            norm_symbols[i].1 += 1;
        }
    }
    // --- END OF OPTIMIZATION ---

    // Step 3: Handle the zero-frequency case with a zero-sum swap.
    // This logic is still essential for robustness and is unaffected by the performance optimization.
    let zero_freq_indices: Vec<usize> = norm_symbols
        .iter()
        .enumerate()
        .filter(|(_, &(_, freq, _))| freq == 0)
        .map(|(i, _)| i)
        .collect();

    if !zero_freq_indices.is_empty() {
        // --- DEBUG BLOCK #3 ---
        #[cfg(debug_assertions)]
        {
            println!(
                "[ANS_DEBUG]   Found {} symbols with zero frequency. Performing zero-sum swaps.",
                zero_freq_indices.len()
            );
        }

        // Sort by original raw count to find the richest symbols to steal from.
        unique_symbols.sort_unstable_by_key(|a| std::cmp::Reverse(a.1));
        let richest_symbols_order: Vec<u8> = unique_symbols.iter().map(|s| s.0).collect();

        for zero_idx in zero_freq_indices {
            norm_symbols[zero_idx].1 = 1; // Give 1 to the poor symbol.

            let mut stolen = false;
            for rich_byte in &richest_symbols_order {
                // Find the index of the rich symbol in our (partially sorted) norm_symbols vec
                if let Some(rich_idx) = norm_symbols.iter().position(|s| s.0 == *rich_byte) {
                    if norm_symbols[rich_idx].1 > 1 {
                        norm_symbols[rich_idx].1 -= 1; // Steal 1 from the rich symbol.
                        stolen = true;
                        break;
                    }
                }
            }
            if !stolen {
                return Err(PhoenixError::AnsError(
                    "Normalization failed: could not find a symbol with freq > 1 to rebalance zero-frequency symbols.".to_string()
                ));
            }
        }
    }

    // --- Final Assembly ---
    // Sort back by symbol value to ensure a canonical header format for the decompressor.
    norm_symbols.sort_unstable_by_key(|a| a.0);

    let header_symbols: Vec<(u8, u16)> = norm_symbols.iter().map(|(b, f, _)| (*b, *f)).collect();
    let mut encode_table = [AnsSymbol::default(); 256];
    let mut final_cum_freq: u32 = 0;

    for (byte, freq) in &header_symbols {
        encode_table[*byte as usize] = AnsSymbol {
            freq: *freq as u32,
            cum_freq: final_cum_freq,
        };
        final_cum_freq += *freq as u32;
    }

    // --- DEBUG BLOCK #4 (Final Check) ---
    #[cfg(debug_assertions)]
    {
        println!("[ANS_DEBUG]   Final calculated sum: {}", final_cum_freq);
        if final_cum_freq != ANS_NORMALIZATION_FACTOR {
            eprintln!(
                "[ANS_DEBUG]   ERROR: Mismatch! Expected {}",
                ANS_NORMALIZATION_FACTOR
            );
        } else {
            println!("[ANS_DEBUG]   SUCCESS: Sum matches normalization factor.");
        }
        println!("==========================================================");
    }

    if final_cum_freq != ANS_NORMALIZATION_FACTOR {
        return Err(PhoenixError::AnsError(format!(
            "Internal normalization error: final frequency sum was {}, expected {}. This is a bug.",
            final_cum_freq, ANS_NORMALIZATION_FACTOR
        )));
    }

    Ok((encode_table, header_symbols))
}

/// A faster version of `build_normalized_symbols` using a more efficient partial sort.
fn build_normalized_symbols_fast(
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

    let unique_symbols: Vec<(u8, u64)> = freqs
        .iter()
        .enumerate()
        .filter(|&(_, &c)| c > 0)
        .map(|(b, &c)| (b as u8, c))
        .collect();

    if unique_symbols.len() <= 1 {
        return Err(PhoenixError::AnsError(
            "ANS requires at least two unique symbols.".to_string(),
        ));
    }

    let total_input_len = input_bytes.len() as f64;
    let mut norm_symbols = Vec::with_capacity(unique_symbols.len());
    let mut current_sum = 0;

    for &(byte, count) in &unique_symbols {
        let ideal = (count as f64 / total_input_len) * ANS_NORMALIZATION_FACTOR as f64;
        let floor = ideal.floor() as u16;
        current_sum += floor as u32;
        norm_symbols.push((byte, floor, ideal - floor as f64));
    }

    let remainder = ANS_NORMALIZATION_FACTOR - current_sum;

    // CONSULTANT OPTIMIZATION: Use select_nth_unstable_by for a more efficient partial sort.
    if remainder > 0 && (remainder as usize) < norm_symbols.len() {
        norm_symbols.select_nth_unstable_by(remainder as usize - 1, |a, b| {
            b.2.partial_cmp(&a.2).unwrap_or(Ordering::Equal)
        });
        for i in 0..remainder as usize {
            norm_symbols[i].1 += 1;
        }
    }

    norm_symbols.sort_unstable_by_key(|a| a.0);

    let mut encode_table = [AnsSymbol::default(); 256];
    let mut cum_freq = 0;
    let header_symbols: Vec<(u8, u16)> = norm_symbols.iter().map(|(b, f, _)| (*b, *f)).collect();

    for &(byte, freq) in &header_symbols {
        if freq == 0 {
            return Err(PhoenixError::AnsError(
                "Normalization assigned zero frequency.".to_string(),
            ));
        }
        encode_table[byte as usize] = AnsSymbol {
            freq: freq as u32,
            cum_freq,
        };
        cum_freq += freq as u32;
    }

    debug_assert_eq!(cum_freq, ANS_NORMALIZATION_FACTOR);
    Ok((encode_table, header_symbols))
}

/// CONSULTANT OPTIMIZATION: An inlined helper for the hottest part of the encoding loop.
#[inline(always)]
fn encode_symbol(mut state: u32, symbol: &AnsSymbol, payload: &mut Vec<u8>) -> u32 {
    while state >= symbol.freq * (ANS_STATE_MIN >> 4) {
        payload.push((state & 0xFF) as u8);
        state >>= 8;
    }
    (state / symbol.freq) * ANS_NORMALIZATION_FACTOR + (state % symbol.freq) + symbol.cum_freq
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

    const MAX_DECOMPRESSION_SIZE: usize = 1 << 30; // 1 GiB
    if num_values_to_decode > MAX_DECOMPRESSION_SIZE {
        return Err(PhoenixError::AnsError(format!(
            "Corrupt header: declared decompression size ({}) exceeds the safety limit ({}).",
            num_values_to_decode, MAX_DECOMPRESSION_SIZE
        )));
    }

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
            if j >= decode_lookup.len() {
                return Err(PhoenixError::AnsError(format!(
                    "Frequency table out of bounds: cum_freq {} + freq {} exceeds normalization factor {}",
                    cum_freq, freq, ANS_NORMALIZATION_FACTOR
                )));
            }
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
pub fn encode_fast(input_bytes: &[u8]) -> Result<Vec<u8>, PhoenixError> {
    if input_bytes.is_empty() {
        return Ok(Vec::new());
    }

    let (encode_table, header_symbols) = build_normalized_symbols_fast(input_bytes)?;
    let mut output_buf = Vec::with_capacity(input_bytes.len());

    output_buf.extend_from_slice(&(input_bytes.len() as u64).to_le_bytes());
    output_buf.extend_from_slice(&(header_symbols.len() as u16).to_le_bytes());
    for (byte, freq) in &header_symbols {
        output_buf.push(*byte);
        output_buf.extend_from_slice(&freq.to_le_bytes());
    }

    let mut states = [ANS_STATE_MIN; 4];
    let mut payloads = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    let input_len = input_bytes.len();

    for (i, &byte) in input_bytes.iter().rev().enumerate() {
        // CORRECTNESS FIX: Use the validated stream index logic.
        let original_idx = input_len - 1 - i;
        let stream_idx = original_idx % 4;
        states[stream_idx] = encode_symbol(
            states[stream_idx],
            &encode_table[byte as usize],
            &mut payloads[stream_idx],
        );
    }

    for state in &states {
        output_buf.extend_from_slice(&state.to_le_bytes());
    }
    for payload in &payloads {
        output_buf.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    }
    for mut payload in payloads {
        payload.reverse();
        output_buf.append(&mut payload);
    }
    Ok(output_buf)
}

pub fn decode_fast(input_bytes: &[u8]) -> Result<Vec<u8>, PhoenixError> {
    if input_bytes.is_empty() {
        return Ok(Vec::new());
    }

    let mut cursor = 0;
    let mut read_bytes = |len: usize| -> Result<&[u8], PhoenixError> {
        let end = cursor + len;
        let slice = input_bytes.get(cursor..end).ok_or_else(|| {
            PhoenixError::AnsError("Input stream is truncated or corrupt.".to_string())
        })?;
        cursor = end;
        Ok(slice)
    };

    let num_values_to_decode = u64::from_le_bytes(read_bytes(8)?.try_into().unwrap()) as usize;
    if num_values_to_decode == 0 {
        return Ok(Vec::new());
    }

    // CONSULTANT OPTIMIZATION: Use a stack-allocated array to avoid a heap allocation.
    let mut decode_lookup = [0u8; ANS_NORMALIZATION_FACTOR as usize];
    let mut symbol_table = [AnsSymbol::default(); 256];
    let num_symbols = u16::from_le_bytes(read_bytes(2)?.try_into().unwrap()) as usize;
    let mut cum_freq = 0;

    for _ in 0..num_symbols {
        let byte = read_bytes(1)?[0];
        let freq = u16::from_le_bytes(read_bytes(2)?.try_into().unwrap()) as u32;
        if cum_freq + freq > ANS_NORMALIZATION_FACTOR {
            return Err(PhoenixError::AnsError(
                "Corrupt header: frequency sum exceeds normalization factor.".to_string(),
            ));
        }
        symbol_table[byte as usize] = AnsSymbol { freq, cum_freq };
        for j in (cum_freq as usize)..((cum_freq + freq) as usize) {
            decode_lookup[j] = byte;
        }
        cum_freq += freq;
    }
    if cum_freq != ANS_NORMALIZATION_FACTOR {
        return Err(PhoenixError::AnsError(
            "Corrupt header: frequencies do not sum correctly.".to_string(),
        ));
    }

    let mut states = [0u32; 4];
    for state in states.iter_mut() {
        *state = u32::from_le_bytes(read_bytes(4)?.try_into().unwrap());
    }
    let mut payload_lengths = [0u64; 4];
    for len in payload_lengths.iter_mut() {
        *len = u64::from_le_bytes(read_bytes(8)?.try_into().unwrap());
    }

    let mut payload_offsets = [0u64; 4];
    for i in 1..4 {
        payload_offsets[i] = payload_offsets[i - 1] + payload_lengths[i - 1];
    }
    let payload_data = &input_bytes[cursor..];
    if payload_offsets[3] + payload_lengths[3] != payload_data.len() as u64 {
        return Err(PhoenixError::AnsError(
            "Corrupt stream: payload lengths do not match remaining data size.".to_string(),
        ));
    }

    let mut data_ptrs = [0usize; 4];
    let mut output_buf = Vec::with_capacity(num_values_to_decode);

    for i in 0..num_values_to_decode {
        let stream_idx = i % 4;
        let mut state = states[stream_idx];
        let slot = state & (ANS_NORMALIZATION_FACTOR - 1);

        // CONSULTANT OPTIMIZATION: `unsafe` here is safe because `slot` is guaranteed < 4096.
        let byte = unsafe { *decode_lookup.get_unchecked(slot as usize) };
        let symbol_info = unsafe { *symbol_table.get_unchecked(byte as usize) };
        output_buf.push(byte);

        state = symbol_info.freq * (state >> 12) + slot - symbol_info.cum_freq;

        while state < ANS_STATE_MIN {
            if data_ptrs[stream_idx] >= payload_lengths[stream_idx] as usize {
                return Err(PhoenixError::AnsError(
                    "Corrupt stream: payload read overflow.".to_string(),
                ));
            }
            let data_offset = payload_offsets[stream_idx] as usize + data_ptrs[stream_idx];
            let data_byte = unsafe { *payload_data.get_unchecked(data_offset) };
            state = (state << 8) | (data_byte as u32);
            data_ptrs[stream_idx] += 1;
        }
        states[stream_idx] = state;
    }
    Ok(output_buf)
}

/// An optimized single-stream kernel. It applies micro-optimizations
/// (partial sort, inlining) but does NOT use interleaving. Its compression
/// ratio is identical to the simple kernel, but it should be faster.
pub fn encode_fast_no_interleave(input_bytes: &[u8]) -> Result<Vec<u8>, PhoenixError> {
    if input_bytes.is_empty() {
        return Ok(Vec::new());
    }

    // OPTIMIZATION: Use the faster symbol building function.
    let (encode_table, header_symbols) = build_normalized_symbols_fast(input_bytes)?;

    let mut output_buf = Vec::with_capacity(input_bytes.len() / 2);
    output_buf.extend_from_slice(&(input_bytes.len() as u64).to_le_bytes());
    output_buf.extend_from_slice(&(header_symbols.len() as u16).to_le_bytes());
    for (byte, freq) in &header_symbols {
        output_buf.push(*byte);
        output_buf.extend_from_slice(&freq.to_le_bytes());
    }

    let mut state = ANS_STATE_MIN;
    // This payload is built in reverse, just like the simple encoder.
    let mut encoded_data = Vec::with_capacity(input_bytes.len());

    for &byte in input_bytes.iter().rev() {
        // OPTIMIZATION: Use the inlined helper function.
        state = encode_symbol(state, &encode_table[byte as usize], &mut encoded_data);
    }

    output_buf.extend_from_slice(&state.to_le_bytes());
    output_buf.extend(encoded_data.iter().rev());
    Ok(output_buf)
}

/// The corresponding single-stream optimized decoder. It uses stack allocation
/// and unsafe gets for maximum performance within a single stream.
pub fn decode_fast_no_interleave(input_bytes: &[u8]) -> Result<Vec<u8>, PhoenixError> {
    if input_bytes.is_empty() {
        return Ok(Vec::new());
    }

    let mut cursor = 0;
    let mut read_bytes = |len: usize| -> Result<&[u8], PhoenixError> {
        let end = cursor + len;
        let slice = input_bytes.get(cursor..end).ok_or_else(|| {
            PhoenixError::AnsError("Input stream is truncated or corrupt.".to_string())
        })?;
        cursor = end;
        Ok(slice)
    };

    // --- Header Parsing with Optimizations ---
    let num_values_to_decode = u64::from_le_bytes(read_bytes(8)?.try_into().unwrap()) as usize;
    if num_values_to_decode == 0 {
        return Ok(Vec::new());
    }

    // Decompression bomb check
    const MAX_DECOMPRESSION_SIZE: usize = 1 << 30; // 1 GiB
    if num_values_to_decode > MAX_DECOMPRESSION_SIZE {
        return Err(PhoenixError::AnsError(format!(
            "Corrupt header: declared decompression size ({}) exceeds the safety limit ({}).",
            num_values_to_decode, MAX_DECOMPRESSION_SIZE
        )));
    }

    let mut output_buf = Vec::with_capacity(num_values_to_decode);

    let num_symbols = u16::from_le_bytes(read_bytes(2)?.try_into().unwrap()) as usize;
    let mut symbol_table = [AnsSymbol::default(); 256];

    // OPTIMIZATION: Use a stack-allocated array to avoid a heap allocation.
    let mut decode_lookup = [0u8; ANS_NORMALIZATION_FACTOR as usize];
    let mut cum_freq = 0;

    for _ in 0..num_symbols {
        let byte = read_bytes(1)?[0];
        let freq = u16::from_le_bytes(read_bytes(2)?.try_into().unwrap()) as u32;
        if cum_freq + freq > ANS_NORMALIZATION_FACTOR {
            return Err(PhoenixError::AnsError(
                "Corrupt header: frequency sum exceeds normalization factor.".to_string(),
            ));
        }
        symbol_table[byte as usize] = AnsSymbol { freq, cum_freq };
        for j in (cum_freq as usize)..((cum_freq + freq) as usize) {
            decode_lookup[j] = byte;
        }
        cum_freq += freq;
    }
    if cum_freq != ANS_NORMALIZATION_FACTOR {
        return Err(PhoenixError::AnsError(
            "Corrupt header: frequencies do not sum correctly.".to_string(),
        ));
    }

    let mut state = u32::from_le_bytes(read_bytes(4)?.try_into().unwrap());
    let mut data_ptr = cursor;

    // --- Core Decoding Loop with Optimizations ---
    while output_buf.len() < num_values_to_decode {
        let slot = state & (ANS_NORMALIZATION_FACTOR - 1);

        // OPTIMIZATION: `unsafe` is safe here because `slot` is guaranteed < 4096.
        let byte = unsafe { *decode_lookup.get_unchecked(slot as usize) };
        let symbol_info = unsafe { *symbol_table.get_unchecked(byte as usize) };
        output_buf.push(byte);

        state = symbol_info.freq * (state >> 12) + slot - symbol_info.cum_freq;

        while state < ANS_STATE_MIN {
            // Check for potential stream truncation before unsafe access
            if data_ptr >= input_bytes.len() {
                return Err(PhoenixError::AnsError(
                    "Input stream is truncated or corrupt during renormalization.".to_string(),
                ));
            }
            let data_byte = unsafe { *input_bytes.get_unchecked(data_ptr) };
            state = (state << 8) | (data_byte as u32);
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
