// In: src/bridge/mod.rs

// ====================================================================================
// ARCHITECTURAL OVERVIEW: The Bridge Layer
// ====================================================================================
//
// The `bridge` is the sole public-facing API of the tambak library. It provides a
// stable, user-friendly interface that completely encapsulates the pure, Arrow-agnostic
// `pipeline` engine. It is the authoritative boundary between the outside world
// (e.g., Arrow data) and the internal compression logic.
//
// Data Flow (Compression):
//
//   1. [Stateful Facade (Compressor)]      -> Receives RecordBatchReader
//         |
//         `-> calls for each column array ->
//
//   2. [Stateless API (compress_arrow_chunk)] -> Receives `&dyn Array`
//         |
//         `-> a. Calls `arrow_impl` to convert `&dyn Array` -> `chunk_pipeline::context::PipelineInput`
//         |
//         `-> b. Calls the pure engine with the `PipelineInput`
//
//   3. [Pipeline Engine (chunk_pipeline::orchestrator)] -> Returns `Result<Vec<u8>>` (a serialized chunk)
//
//
// Data Flow (Decompression):
//
//   1. [Pipeline Engine (chunk_pipeline::orchestrator)] -> Receives `&[u8]` (a serialized chunk)
//         |
//         `-> Returns `Result<chunk_pipeline::context::PipelineOutput, tambakError>`
//
//   2. [Stateless API (decompress_arrow_chunk)] -> Receives `PipelineOutput` from the engine
//         |
//         `-> a. Calls `arrow_impl` to convert `PipelineOutput` -> `Box<dyn Array>`
//         |
//         `-> b. Returns `Box<dyn Array>` to the stateful facade
//
//   3. [Stateful Facade (Decompressor)]     -> Assembles `RecordBatch` and yields to user
//
// ====================================================================================
pub(crate) mod arrow_impl;
pub(crate) mod compressor;
pub mod config;
pub(crate) mod decompressor;
pub(crate) mod format;
pub mod stateless_api;

// --- High-Level Stateful API ---
pub use config::TimeSeriesStrategy;

// --- Low-Level Stateless API (for FFI and testing) ---
pub use stateless_api::{analyze_chunk, compress_arrow_chunk, decompress_arrow_chunk};

// --- Format Constants and Structs ---

#[cfg(test)]
mod tests;
