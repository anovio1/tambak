//! This module defines the highest-level processing pipeline, responsible for
//! file-level structural transformations like global sorting and per-batch
//! re-linearization. It orchestrates the lower-level `chunk_pipeline` module to compress
//! the resulting data streams.

use std::sync::Arc;

use crate::config::TambakConfig;
use crate::bridge::format::{ChunkManifestEntry, FramePlan};
use crate::error::tambakError;
use arrow::record_batch::RecordBatchReader;

//==================================================================================
// 1. Module Declarations
//==================================================================================

pub mod profiler;
pub mod relinearize; // Batch-aware profiler (reconcile with chunk pipeline)

mod column_strategies; // Column-level compression (decorator pattern)
mod strategies; // Concrete FramePipeline implementations // Batch-aware relinearization
mod util;

//==================================================================================
// 2. Public API Re-exports
//==================================================================================
pub use self::strategies::{
    PartitioningStrategy, PerBatchRelinearizationStrategy, StandardStreamingStrategy,
};

// pub use self::profiler::{find_stride_by_autocorrelation, PlannerHints};

/// **CONTRACT:** The unified result from any `FramePipeline` execution.
/// This contains all data needed by the Bridge to write the final file body and footer.
pub struct FramePipelineResult {
    pub compressed_chunks_with_manifests: Vec<(Vec<u8>, ChunkManifestEntry)>,
    pub frame_plan: Option<FramePlan>,
}

/// **CONTRACT:** The trait that all high-level file strategies must implement.
/// The Bridge will create a concrete implementation of this trait and execute it.
pub trait FramePipeline {
    fn execute(
        &self,
        reader: &mut dyn RecordBatchReader,
        config: Arc<TambakConfig>,
    ) -> Result<FramePipelineResult, tambakError>;
}

#[cfg(test)]
mod tests;
