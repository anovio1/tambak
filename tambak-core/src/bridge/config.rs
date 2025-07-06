// In: src/bridge/config.rs
/// Configuration options for compression.
/// Currently under development and not fully implemented.
/// Expected to include planner strategies, chunk sizing, and lossy compression parameters.
use arrow::datatypes::SchemaRef;
// TODO: Re-export the PlannerStrategy from the pipeline/models.rs when it exists.
// use crate::chunk_pipeline::models::PlannerStrategy;

/// Defines the output format strategy for the Compressor.
#[derive(Debug, Clone, Copy, Default)]
pub enum CompressionFormat {
    /// A single, monolithic file with a metadata footer. Ideal for analytics.
    #[default]
    ColumnarFile,
    /// Chunks are interleaved with metadata. Ideal for network streaming.
    InterleavedStream,
}

/// Defines the lossy compression strategy for a compression operation.
#[derive(Debug, Clone, Copy, Default)]
pub enum LossyConfig {
    /// No lossy compression will be applied. This is the default.
    #[default]
    Lossless,
    /// Guarantees a predictable final size by targeting a bit rate.
    FixedRate { bits_per_value: f32 },
    /// Guarantees a predictable level of quality by targeting an error tolerance.
    FixedTolerance { absolute_error: f64 },
}

/// Defines the time-series optimization strategy for compression.
/// This enum directly influences the `FramePlan` written to the file footer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TimeSeriesStrategy {
    #[default]
    None, // No special frame-level strategy. File contains only `StandardColumn` FrameOperations.
    PerBatchRelinearization, // File contains `PerBatchRelinearizedColumn` FrameOperations for applicable columns.
    /// Partitions the entire file by a key column. All data for a given key is co-located.
    Partitioned {
        /// The target column to partition by.
        partition_key_column: String,
        /// The target number of rows to buffer for a partition before flushing it as a chunk.
        partition_flush_rows: usize,
    },
}

// TODO: CompressorConfig is currently a placeholder for future configuration options.
// It is not yet fully integrated into the compression pipeline.
/// The main configuration object for the `Compressor`.
#[derive(Default, Debug)]
pub struct CompressorConfig {
    // We will use `Option` for now to make it easy to construct a default.
    pub schema: Option<SchemaRef>,
    pub format: CompressionFormat,
    pub chunk_size_rows: Option<usize>,
    pub lossy: LossyConfig,

    // === PLACEHOLDERS from Consultant Review ===
    /// If true, a footer containing the chunk manifest will be written.
    /// Ignored if format is `InterleavedStream`.
    pub include_footer: bool,

    /// If true, enables collection of detailed per-column compression statistics.
    pub enable_stats_collection: bool,

    // /// The planning strategy to use. If None, the default planner is used.
    // pub planner_strategy: Option<PlannerStrategy>,

    // --- NEW FIELDS FOR TIME-SERIES OPTIMIZATION ---
    pub time_series_strategy: TimeSeriesStrategy,
    /// Name of the column to use as the primary sorting key (e.g., "unit_id").
    /// Only relevant if `time_series_strategy` is not `None`.
    pub stream_id_column_name: Option<String>,
    /// Name of the column to use as the secondary sorting key (e.g., "timestamp").
    /// Only relevant if `time_series_strategy` is not `None`.
    pub timestamp_column_name: Option<String>,
}
