# Tambak

[![CI Status](https://img.shields.io/github/actions/workflow/status/your-org/tambak/rust.yml?branch=main&style=for-the-badge)](https://github.com/your-org/tambak/actions)
[![Crates.io](https://img.shields.io/crates/v/tambak?style=for-the-badge)](https://crates.io/crates/tambak)
[![PyPI](https://img.shields.io/pypi/v/tambak?style=for-the-badge)](https://pypi.org/project/tambak/)
[![License](https://img.shields.io/badge/License-MIT%2FApache--2.0-blue?style=for-the-badge)](./LICENSE)

An adaptive, self-describing columnar storage format that uses empirical analysis to build optimal comprression pipelines for your data. It is a library written in Rust with Python extensions built to serve as a performance-oriented foundation for analytical data.

## Key Features

-   **Intelligent Adaptive Compression:** A cost-based `Planner` analyzes each chunk of data to construct a bespoke pipeline of compression kernels, ensuring the best strategy is used for the data's unique statistical properties.
-   **Self-Describing Format:** Artifacts are fully self-contained. The file footer includes a `FramePlan` that describes the file's structure and a manifest of all data chunks, ensuring portability and eliminating the need for external metadata.
-   **Advanced Storage Strategies:** A two-tiered pipeline architecture allows for high-level structural transformations, including strategies for time-series and partitioned data that improve compression ratios.
-   **Idiomatic Python API:** A high-level, stateful Python API provides a seamless, "batteries-included" experience for data scientists and engineers, abstracting away the complexity of the Rust core.
-   **Apache Arrow Native:** Built from the ground up to work within the Arrow ecosystem for zero-copy data exchange.

## Performance Highlights

Tambak's adaptive strategies allow it to outperform standard formats like Zstd-compressed MessagePack and even Parquet on many real-world workloads. The following benchmarks were run on a sample dataset from the [Tubuin Analytics](https://github.com/anovio1/tubuin) project.

| Aspect                 | Original MPK (bytes) | Parquet (Zstd) (%) | **Tambak (Default) (%)** | **Tambak (Multiplexed) (%)** |
| :--------------------- | :------------------- | :----------------- | :-------------------- | :--------------------- |
| **unit_economy**       | 10,926,575           | 11.44              | 9.78                  | **6.37**               |
| **construction_log**   | 11,062,038           | 23.19              | 24.77                 | **10.96**              |
| **unit_state_snapshots** | 1,700,312            | 32.84              | 28.10                 | **20.71**              |
| **unit_positions**     | 6,641,915            | 49.77              | 49.73                 | **43.35**              |

*Lower percentage is better. The results show Tambak's specialized strategies establishing a new state-of-the-art for compressing the related time-series data in this domain.*

## Architecture Overview

Tambak uses a unique two-tiered pipeline architecture to achieve its flexibility and performance:

1.  **The `FramePipeline` (The Strategist):** This high-level layer operates on entire data frames. It selects a "strategy" (like the default streaming or advanced partitioning) to transform data before compression. It produces the `FramePlan` that makes the file self-describing.
2.  **The `ChunkPipeline` (The Technician):** This low-level, pure computational core operates on individual chunks of data. It uses the `Planner` to create an optimal sequence of compression kernels (e.g., `Delta` -> `Shuffle` -> `Zstd`) for any given chunk.

For a complete breakdown, please see the [**Architecture Guide (v4.6)**](./docs/ARCHITECTURE.md).

## Project Status

**Alpha:** Tambak is in the alpha stage and is being actively developed and battle-tested as the primary storage engine for the Tubuin project. The core API is stabilizing, but breaking changes are still possible.

## Getting Started

The Python API provides a high-level, stateful interface for working with files and streams.

### Example 1: Standard Streaming Compression

```python
import pyarrow as pa
import tambak_cache

# 1. Create some data and a PyArrow RecordBatchReader
data = pa.table({
    'timestamps': pa.array([1, 2, 3, 4, 5], type=pa.int64()),
    'values': pa.array([10.0, 10.1, 10.2, 12.0, 12.2], type=pa.float64())
})
reader = data.to_reader()

# 2. Compress the stream into an in-memory buffer.
buffer = pa.BufferOutputStream()
compressor = tambak_cache.Compressor(buffer)
compressor.compress(reader)

compressed_bytes = buffer.getvalue().to_pybytes()

# 3. Decompress the stream
buffer_reader = pa.BufferReader(compressed_bytes)
decompressor = tambak_cache.Decompressor(buffer_reader)
decompressed_reader = decompressor.batched()
result_table = decompressed_reader.read_all()

assert data.equals(result_table)
```

### Example 2: Partitioned Compression

Tambak can intelligently partition a stream by a key column, compressing each partition independently for massive gains on certain data shapes.

```python
import pyarrow as pa
import tambak_cache

# 1. Create data with a clear partition key (must be Int64).
data = pa.table({
    'device_id': pa.array([101, 102, 101, 102, 101], type=pa.int64()),
    'reading': pa.array([100, 500, 101, 502, 102], type=pa.int32())
})
reader = data.to_reader()

# 2. Compress the stream, configuring the partitioned strategy directly.
buffer = pa.BufferOutputStream()
compressor = tambak_cache.Compressor(
    buffer,
    time_series_strategy="partitioned",
    partition_key_column="device_id"
)
compressor.compress(reader)
compressed_bytes = buffer.getvalue().to_pybytes()

# 3. Decompress and iterate through the partitions
buffer_reader = pa.BufferReader(compressed_bytes)
decompressor = tambak_cache.Decompressor(buffer_reader)

all_partitions = {}
for key, partition_reader in decompressor.partitions():
    all_partitions[key] = partition_reader.read_all()

# Verify the data was correctly partitioned and reconstructed
assert all_partitions[101].equals(data.filter(pa.compute.field('device_id') == 101))
assert all_partitions[102].equals(data.filter(pa.compute.field('device_id') == 102))
```

## Roadmap

-   [x] Feat: Refactor to a unified `TambakConfig` and pass via `Arc` from Bridge to Planner.
-   [ ] Feat: Refactor of the Chunk Pipeline to support DAGs (`OPE Refactor`).
-   [ ] Perf: Performance optimizations for core kernels.
-   [ ] Feat: Implementation of tunable lossy compression strategies for ML workloads.
-   [ ] Feat: Expansion of the `FramePipeline` strategy library (e.g., global sort).
-   [ ] Chore: Official release on Crates.io and PyPI.

## Chunk Pipeline Roadmap

| Role | **Current Phase** | **Target Phase (Our Work)** | **Next Evolutionary Phase (Future)** | Additional Considerations |
| :--- | :--- | :--- | :--- | :--- |
| `Compressor` | Strategic Frame Assembly | (Stable for now) | Inter-Chunk Planning | |
| `Orchestrator` | Pragmatic Hybrid | **Pure Coordinator** | Parallel Task Dispatcher | Adaptive/Speculative & Distributed Execution Coordinator |
| `Planner` | Fixed Candidate Set | **DAG Builder / Synthesizer** | ML-Driven / Cost-Aware | |
| `Executor` | Linear Runner | **DAG Traversal Engine** | JIT-Compiling / Vectorized | |

## Contributing

This project is currently in early development, but feedback and ideas are welcome. Please see [CONTRIBUTING.md](./CONTRIBUTING.md) for more details.

## License

This project is licensed under either of
-   Apache License, Version 2.0, ([LICENSE-APACHE](./LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
-   MIT license ([LICENSE-MIT](./LICENSE-MIT) or http://opensource.org/licenses/MIT)
at your option.
