# **Tambak Architecture (v4.6): The Self-Describing Frame**

## Major Features
- **Clear Layer Separation** FFI, Bridge, FramePipeline, ChunkPipeline
- **Two-Tiered Planning System** FramePlan: Describes the overall file structure and transformations. ChunkPlan: Defines the sequence of compression operations applied to each chunk’s raw data
- **Flexible Compression Strategies** FramePipeline - Streaming, Partitioning, ChunkPipeline - Kernels
- **Multiplexed Stream Hanndling** Using PerBatchLinear and Partitioned
- **Self-Describing File Format** Footer includes internal structure and chunk layered compression plans

## **1. Core Philosophy**

The Tambak v4.6 architecture builds upon the stable foundation of v4.5, elevating the file format from a simple container of compressed chunks to a fully **self-describing, structured artifact**. The core philosophy remains the clean separation of concerns, but is now enhanced with a more sophisticated planning system.

1.  **The `bridge` is the Boundary:** This principle is unchanged. All interaction with the outside world (I/O, Arrow data formats, state management) is the exclusive responsibility of the `bridge` layer.

2.  **The `pipeline` is Pure:** This principle is unchanged. The `chunk_pipeline` remains a stateless, Arrow-agnostic computational core for compressing and decompressing linear byte streams.

3.  **The Plan is a Two-Tiered Contract:** This is the central evolution in v4.6. The "Plan" is no longer a single concept but a two-tiered system that describes the file's structure and contents completely.

    - **`FramePlan` (The "What"):** A new, high-level plan stored in the `FileFooter`. It declaratively describes the **structural organization of the entire file**. It answers questions like, "Was this file globally sorted?" or "Were some columns structurally transformed?"
    - **`ChunkPlan` (The "How"):** The existing, low-level plan stored in each chunk's header. It describes the **linear sequence of compression operations** applied to that specific chunk's byte stream.

4.  **The `FramePipeline` is a Strategy:** To support the `FramePlan`, we introduce the `FramePipeline` layer. This layer implements the **Strategy Pattern**, where different high-level file construction strategies (`StandardStreamingStrategy`, `PartitioningStrategy`, etc.) can be selected at runtime. Each strategy is responsible for orchestrating the structural transformations and producing the correct `FramePlan`.

---

## **2. Architecture and Data Flow**

### **2.1. High-Level Data Flow (The `Compressor` Journey)**

The data flow is now best understood as a delegation from the `Compressor` to a chosen `FramePipeline` strategy.

1.  **`Compressor` (`src/bridge/compressor.rs`):** The user interacts with the `Compressor`, providing a `RecordBatchReader` and a `CompressorConfig`. The `Compressor`'s primary role is now a **Factory and Coordinator**.

    - Based on the `config.time_series_strategy`, it instantiates a concrete `FramePipeline` strategy object (e.g., `StandardStreamingStrategy`).
    - It delegates the entire compression process to this strategy by calling `strategy.execute(reader, config)`.

2.  **`FramePipeline` Strategy (`src/frame_pipeline/strategies.rs`):**

    - The `execute` method takes control. It is responsible for consuming the `RecordBatchReader` according to its specific logic (e.g., streaming batch-by-batch, or partitioning).
    - For each column `Array` it processes, it calls the **stateless `compress_arrow_chunk` bridge API**, which in turn invokes the pure `chunk_pipeline` to produce a compressed `Vec<u8>` and a partial `ChunkManifestEntry`.
    - It gathers all compressed chunks and assembles the final, authoritative `FramePlan`.
    - It returns a `FramePipelineResult` containing the list of compressed chunks and the `FramePlan`.

3.  **Return to `Compressor`:**
    - The `Compressor` receives the `FramePipelineResult`.
    - It iterates through the compressed chunks, writes their bytes, and **finalizes their `ChunkManifestEntry`s** (filling in `offset_in_file`, `compressed_size`, and `batch_id`).
    - After all data is written, it writes the final `FileFooter`, serializing the `FramePlan` into it.

### **2.2. Component Anatomy & Contracts**

- **`Bridge Layer (src/bridge)`:** The "User-Facing Layer and I/O Manager."

  - **`compressor.rs` / `decompressor.rs`:** The stateful facades. The `Compressor` is a **Strategy Factory**. The `Decompressor` reads the `FramePlan` and acts as a **Decompression Mode Factory**.
  - **`stateless_api.rs`:** The essential **seam** between the `frame_pipeline` and the `chunk_pipeline`.
  - **`format.rs`:** The on-disk file format definition. **This is the most critical contract**, defining the `FileFooter`, `FramePlan`, and `ChunkManifestEntry`.

- **`FramePipeline Layer (src/frame_pipeline)`:** The "Structural Transformation & Strategy Layer."

  - **`mod.rs` & `strategies.rs`:** Defines the `FramePipeline` trait and its concrete implementations (e.g., `PartitioningStrategy`). Each strategy is a self-contained recipe for building a specific type of Tambak file.

- **`ChunkPipeline Layer (src/chunk_pipeline)`:** The "Pure Computational Core."
  - This layer is responsible for the low-level, linear compression of byte streams as dictated by a `ChunkPlan`. It is called by the `FramePipeline` but has no knowledge of it.

---

## **3. Detailed Internal Workflows**

While the high-level architecture describes the "what," it's crucial to document the "how" for complex internal processes.

### **3.1. `ChunkPipeline` Meta-Operation Flow (Nulls and Sparsity)**

The `chunk_pipeline::Orchestrator`'s handling of meta-operations like `ExtractNulls` or `Sparsify` is a sophisticated "split and re-plan" workflow, not a simple linear execution.

1.  **Initial Plan:** The `Planner` first generates a `ChunkPlan` for the primary data stream.
2.  **Meta-Operation Detected:** The `Orchestrator` inspects this plan. If it finds a meta-operation (e.g., `ExtractNulls`), it pauses execution of the main plan.
3.  **Split and Re-Plan:** The `Orchestrator` executes the `ExtractNulls` operation, which splits the `PipelineInput` into two byte streams: the `main_data` and a new `null_mask` stream. Crucially, the `Orchestrator` then **re-invokes the `Planner`** on the `null_mask` stream. This generates a _second, independent `ChunkPlan`_ that is optimally tailored for compressing the simple `0`s and `1`s of the null mask (e.g., likely a simple `RLE` -> `Zstd` plan).
4.  **Sub-Pipeline Execution:** The `Orchestrator` now has two `ChunkPlan`s. It calls the `Executor` to run the sub-pipeline for the null mask, and then calls the `Executor` again to run the remaining pipeline for the main data.
5.  **Artifact Assembly:** The final `CompressedChunk` artifact is assembled with the main `ChunkPlan` in its header, but contains multiple, independently compressed data streams (e.g., `main` and `null_mask`).

This recursive planning process ensures that every substream generated by a meta-operation is compressed with maximum efficiency.

### **3.2. Configuration Propagation**

User-defined configuration from the `CompressorConfig` must flow from the `bridge` layer down to the appropriate component in the `chunk_pipeline`. This flow is managed explicitly to maintain clear boundaries.

1.  **Bridge Layer (`Compressor`):** The `Compressor` owns the `CompressorConfig`. High-level structural options (like `time_series_strategy`) are used here to select the correct `FramePipeline` strategy.
2.  **Stateless API Seam:** Low-level options relevant to the chunk pipeline (e.g., a hypothetical `lossy_compression_level`) cannot be passed directly to the `chunk_pipeline`, as it must remain pure.
3.  **`PipelineInput` as the Vehicle:** Instead, these options are passed via the `PipelineInput` struct. The `arrow_impl::arrow_to_pipeline_input` function in the bridge is responsible for inspecting the `CompressorConfig` and populating a `context` field or similar within the `PipelineInput`.
4.  **`ChunkPipeline` (`Planner`):** The `chunk_pipeline::Planner` receives the `PipelineInput`. It is then responsible for reading the configuration from the input's context and using it to influence its planning decisions (e.g., adding a `Quantize` operation to the `ChunkPlan`).

This ensures a clean, one-way data flow where configuration is explicitly passed as part of the data contract, preserving the purity of the `chunk_pipeline`.
