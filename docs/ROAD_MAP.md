# Roadmap

## Bridge

---

### **C**ompressor (File-level),

#### Ideas

- The concept of an "Orchestrator" managing a "Plan" and "Executor" applies at both the chunk level and the file level. The file-level Orchestrator would also manage the global_sort or two_level_bucketing plan. Its "Executor" would be a worker queue or potentially distributed system. Restarting failed tasks is a core responsibility of this higher-level Orchestrator.

#### Prior to OPE refactor

- UserConfig pass down feat

### The `Compressor` / `Decompressor` (File-Level)

This component's domain is the _structure of the file_ and the _coordination of chunk processing_.

- **Proposed Phase (Our Target): Strategic Frame Assembly**

  - **Description:** The `Compressor` acts as a **Strategy Factory** (`StandardStreamingStrategy`, `PartitioningStrategy`, etc.) as defined in `doc_4.6._ARCHITECTURE.md`. It delegates the complex work of organizing batches and columns to a chosen `FramePipeline` strategy. The `Decompressor` mirrors this by reading the `FramePlan` and instantiating the correct readers.
  - **State:** This is the **current, mature state** of the project's file-level API. It is already quite advanced.

- **Next Evolutionary Phase: Inter-Chunk Planning & Optimization**
  - **Description:** The `Compressor` would evolve into a true **Frame-Level Planner**. Instead of just executing a user-configured strategy, it would analyze the relationships _between_ batches. For example, it might detect that a column is globally sorted across all batches and decide to compress only the first batch's data fully, while delta-encoding subsequent batches against the previous one. It could also build a global dictionary for a column across the entire file.
  - **Architectural Impact:** This would require a more powerful `FramePlan` that can describe inter-chunk dependencies. The `Decompressor` would become stateful during reads, holding previous chunks in memory to reconstruct dependent ones.

---

## Chunk Pipeline

---

Evolutionary map for the **O**rchestrator, **P**lanner, and **E**xecutor (Chunk-level).

### The `Orchestrator` (Chunk-Level)

This component's domain is the _coordination of a single chunk's compression workflow_.

- **Skipped Phase (Our Target): Pure Coordinator**

  - **Description:** The `Orchestrator` is stripped of all planning and execution logic. Its `compress_chunk` function becomes a simple, three-line coordinator: `plan = planner.plan()`, `streams = executor.execute(plan)`, `artifact = assemble(streams)`. It has zero knowledge of any specific `Operation`.
  - **State:** This is the **target of our "leapfrog" refactoring**. We are moving it from its current "Pragmatic Hybrid" state to this "Pure Coordinator" state.

- **Proposed Leapfrog Phase Evolutionary Phase: Parallel Task Dispatcher**

  - **Description:** Once the `Executor` can process a DAG, the `Orchestrator` could evolve to exploit the graph's potential for parallelism. When the `Planner` produces a DAG with independent branches (e.g., the `mask` and `values` pipelines in a `Sparsify` op), the `Orchestrator` could dispatch these branches to a thread pool for concurrent execution by multiple `Executor` instances.
  - **Architectural Impact:** The `Orchestrator` would become a `tokio` or `rayon` based task scheduler. The `Executor` would need to be `Send + Sync` and fully thread-safe.

- **Next Evolutionary Phase: Adaptive/Speculative & Distributed Execution Coordinator**

  - **Description:** The `Orchestrator` becomes a highly intelligent, real-time coordinator.
    1.  **Adaptive Execution:** It monitors the execution of the DAG. If it sees that a particular branch (e.g., compressing the `values` stream in a `Sparsify` op) is taking too long or not producing good compression, it could have the authority to _cancel_ that branch and fall back to a simpler, pre-calculated "safe" pipeline.
    2.  **Speculative Execution:** If the `Planner` produces two "very close" candidate DAGs, the `Orchestrator` could speculatively execute both in parallel and commit the results of whichever finishes first with an acceptable compression ratio. This trades CPU cycles for lower latency.
    3.  **Distributed Execution:** The `Orchestrator` could dispatch tasks not just to a local thread pool, but to a cluster of worker nodes (e.g., using a framework like `tonic`/`gRPC`). The `Executor` becomes a self-contained binary that can be deployed on these worker nodes. The `Orchestrator` manages the movement of data streams between nodes.
  - **Architectural Impact:** This is a massive shift. The `Orchestrator` would become a stateful, long-running service. The `ChunkPlan` would need to be enriched with metadata for fallbacks and cost estimates. The entire system would move towards a microservices-style architecture for compression.

### The `Planner` (Chunk-Level)

This component's domain is the _strategic analysis and construction of an optimal compression plan_.

- **Proposed Phase (Our Target): DAG Builder / Synthesizer**

  - **Description:** The `Planner`'s responsibility is elevated from generating a linear list of operations to architecting a full `ChunkPlan` DAG. It uses a **rule-based generative model** (`pipeline_generator.rs`) to synthesize this graph, recursively planning for sub-streams created by meta-operations like `Sparsify` or `Dictionary`.
  - **State:** This is the **target of our "leapfrog" refactoring**. We are moving it from its current "Fixed Candidate Set" state to this "DAG Builder" state.

- **Next Evolutionary Phase: ML-Driven & Cost-Aware**
  - **Description:** The `Planner` becomes even more sophisticated. Instead of just picking the pipeline with the smallest compressed size, it optimizes for a multi-objective cost function provided by the user (e.g., `cost = 0.7 * size + 0.3 * decompression_speed`). It would no longer rely solely on hand-crafted rules in the `pipeline_generator`. It could use:
    1.  **A Pre-trained ML Model:** A fast path that predicts the optimal DAG from a `DataProfile` without any empirical trials.
    2.  **A Genetic Algorithm:** A slower but more exhaustive path where it evolves and mutates the `ChunkPlan` DAG over several generations to find novel, highly-optimized solutions.

### The `Executor` (Chunk-Level)

This component's domain is the _faithful execution of a compression plan_.

- **Proposed Phase (Our Target): DAG Traversal Engine**

  - **Description:** The `Executor` is fundamentally re-architected to process a `ChunkPlan` DAG. It manages a state map of active data streams (`HashMap<StreamId, Vec<u8>>`) and traverses the graph, executing nodes as their input dependencies become available. It is capable of handling any fork/join data flow defined by the plan.
  - **State:** This is the **target of our "leapfrog" refactoring**. We are moving it from its current "Linear Runner" state to this "DAG Traversal Engine" state.

- **Next Evolutionary Phase: JIT-Compiling & Vectorized Engine**
  - **Description:** This represents the peak of performance. The `Executor` would analyze the `ChunkPlan` DAG and, for certain common patterns (e.g., `Delta -> ZigZag -> BitPack`), it would JIT-compile that sequence into a single, highly optimized function using a framework like `cranelift`. This function would operate on data in a tight loop, maximizing cache locality and leveraging SIMD instructions (e.g., using `std::simd` or `packed_simd_2`) for massive speedups on numeric kernels.
