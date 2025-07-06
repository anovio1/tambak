// In tambak-core/benches/ans_bench.rs

use criterion::{black_box, criterion_group, criterion_main, Criterion};

// Import all three kernel variants
use tambak_cache::kernels::ans::{
    decode, decode_fast, decode_fast_no_interleave, encode, encode_fast, encode_fast_no_interleave,
};

// --- CORRECT, FULLY IMPLEMENTED MOCK DATA GENERATION ---

/// Generates a vector of highly compressible data.
fn generate_low_entropy_bytes(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let pattern = b"abcdefgABCDEFG12345";
    while data.len() < size {
        data.extend_from_slice(pattern);
    }
    data.truncate(size);
    data
}

/// Generates a vector of less compressible, more random-looking data.
fn generate_high_entropy_bytes(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let pattern: Vec<u8> = (0..=255u8).collect();
    while data.len() < size {
        data.extend_from_slice(&pattern);
    }
    data.truncate(size);
    data
}

// --- Benchmark Suite ---

const BENCH_DATA_SIZE: usize = 65536; // 64 KB

fn bench_ans_kernels(c: &mut Criterion) {
    // --- Setup Data ---
    let low_entropy_data = generate_low_entropy_bytes(BENCH_DATA_SIZE);
    let high_entropy_data = generate_high_entropy_bytes(BENCH_DATA_SIZE);

    // Prepare encoded data once for each kernel variant to benchmark decoding accurately.
    let encoded_simple_low = encode(&low_entropy_data).unwrap();
    let encoded_no_interleave_low = encode_fast_no_interleave(&low_entropy_data).unwrap();
    let encoded_fast_low = encode_fast(&low_entropy_data).unwrap();

    let encoded_simple_high = encode(&high_entropy_data).unwrap();
    let encoded_no_interleave_high = encode_fast_no_interleave(&high_entropy_data).unwrap();
    let encoded_fast_high = encode_fast(&high_entropy_data).unwrap();

    // --- Create a Benchmark Group ---
    let mut group = c.benchmark_group("ANS Kernels Comparison");
    group.throughput(criterion::Throughput::Bytes(BENCH_DATA_SIZE as u64));

    // --- Encoding Benchmarks (Low Entropy) ---
    group.bench_function("Encode [1] Simple (Low Entropy)", |b| {
        b.iter(|| black_box(encode(black_box(&low_entropy_data))))
    });
    group.bench_function("Encode [2] Fast-No-Interleave (Low Entropy)", |b| {
        b.iter(|| black_box(encode_fast_no_interleave(black_box(&low_entropy_data))))
    });
    group.bench_function("Encode [3] Fast-Interleaved (Low Entropy)", |b| {
        b.iter(|| black_box(encode_fast(black_box(&low_entropy_data))))
    });

    // --- Decoding Benchmarks (Low Entropy) ---
    group.bench_function("Decode [1] Simple (Low Entropy)", |b| {
        b.iter(|| black_box(decode(black_box(&encoded_simple_low))))
    });
    group.bench_function("Decode [2] Fast-No-Interleave (Low Entropy)", |b| {
        b.iter(|| {
            black_box(decode_fast_no_interleave(black_box(
                &encoded_no_interleave_low,
            )))
        })
    });
    group.bench_function("Decode [3] Fast-Interleaved (Low Entropy)", |b| {
        b.iter(|| black_box(decode_fast(black_box(&encoded_fast_low))))
    });

    // --- Encoding Benchmarks (High Entropy) ---
    group.bench_function("Encode [1] Simple (High Entropy)", |b| {
        b.iter(|| black_box(encode(black_box(&high_entropy_data))))
    });
    group.bench_function("Encode [2] Fast-No-Interleave (High Entropy)", |b| {
        b.iter(|| black_box(encode_fast_no_interleave(black_box(&high_entropy_data))))
    });
    group.bench_function("Encode [3] Fast-Interleaved (High Entropy)", |b| {
        b.iter(|| black_box(encode_fast(black_box(&high_entropy_data))))
    });

    // --- Decoding Benchmarks (High Entropy) ---
    group.bench_function("Decode [1] Simple (High Entropy)", |b| {
        b.iter(|| black_box(decode(black_box(&encoded_simple_high))))
    });
    group.bench_function("Decode [2] Fast-No-Interleave (High Entropy)", |b| {
        b.iter(|| {
            black_box(decode_fast_no_interleave(black_box(
                &encoded_no_interleave_high,
            )))
        })
    });
    group.bench_function("Decode [3] Fast-Interleaved (High Entropy)", |b| {
        b.iter(|| black_box(decode_fast(black_box(&encoded_fast_high))))
    });

    group.finish();
}

// These two lines generate the main function and register the benchmark group.
criterion_group!(benches, bench_ans_kernels);
criterion_main!(benches);
