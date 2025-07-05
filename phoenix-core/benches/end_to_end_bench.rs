// In phoenix-core/benches/end_to_end_bench.rs

use criterion::{black_box, criterion_group, criterion_main, Criterion};

// --- Import Rust types, NOT Python types ---
use arrow::array::{Array, UInt8Array};
use arrow::datatypes::DataType;
use std::sync::Arc;

// CORRECT: Import the pure Rust function from the bridge module.
// Adjust the path if `compress_arrow_chunk` lives elsewhere.
use phoenix_cache::bridge::stateless_api::compress_arrow_chunk;

// --- Mock Data Generation (Modified to produce Arrow Arrays) ---

/// Generates a UInt8 Arrow Array with long runs, perfect for RLE.
fn generate_rle_friendly_array(size: usize) -> Arc<dyn Array> {
    let mut data = Vec::with_capacity(size);
    for i in 0..size {
        data.push((i / 100) as u8);
    }
    Arc::new(UInt8Array::from(data))
}

/// Generates a UInt8 Arrow Array with skewed stats, perfect for ANS.
fn generate_ans_friendly_array(size: usize) -> Arc<dyn Array> {
    let mut data = Vec::with_capacity(size);
    let pattern = [0, 1, 0, 2, 0, 3, 0, 4, 0, 5];
    while data.len() < size {
        data.extend_from_slice(&pattern);
    }
    data.truncate(size);
    Arc::new(UInt8Array::from(data))
}

/// Generates a UInt8 Arrow Array with repeating patterns, perfect for Zstd's LZ77.
fn generate_zstd_friendly_array(size: usize) -> Arc<dyn Array> {
    let mut data = Vec::with_capacity(size);
    let pattern: Vec<u8> = (0..=255u8).collect();
    while data.len() < size {
        data.extend_from_slice(&pattern);
    }
    data.truncate(size);
    Arc::new(UInt8Array::from(data))
}

// --- Benchmark Suite ---

const BENCH_DATA_SIZE: usize = 131_072; // 128 KB

fn bench_e2e_flow(c: &mut Criterion) {
    let mut group = c.benchmark_group("End-to-End Compression Flow (Arrow)");
    group.throughput(criterion::Throughput::Bytes(BENCH_DATA_SIZE as u64));

    // --- Test Case 1: Data that should be dominated by RLE ---
    let rle_array = generate_rle_friendly_array(BENCH_DATA_SIZE);
    group.bench_function("E2E on RLE-Friendly Data", |b| {
        // We benchmark the pure Rust function, passing it a Rust Arrow array.
        b.iter(|| black_box(compress_arrow_chunk(black_box(rle_array.as_ref()))))
    });

    // --- Test Case 2: Data that should be dominated by ANS ---
    let ans_array = generate_ans_friendly_array(BENCH_DATA_SIZE);
    group.bench_function("E2E on ANS-Friendly Data", |b| {
        b.iter(|| black_box(compress_arrow_chunk(black_box(ans_array.as_ref()))))
    });

    // --- Test Case 3: Data that should be dominated by Zstd ---
    let zstd_array = generate_zstd_friendly_array(BENCH_DATA_SIZE);
    group.bench_function("E2E on Zstd-Friendly Data", |b| {
        b.iter(|| black_box(compress_arrow_chunk(black_box(zstd_array.as_ref()))))
    });

    group.finish();
}

criterion_group!(benches, bench_e2e_flow);
criterion_main!(benches);
