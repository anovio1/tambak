// In: src/config.rs

//! The single source of truth for all Tambak compression configuration.
//!
//! This module defines the unified `TambakConfig` struct, which is designed to be
//! created once at the application boundary (e.g., from a user's YAML file or
//! Python dictionary) and then passed down through the system via a shared,
//! read-only `Arc<TambakConfig>`.
//!
//! This approach centralizes all settings, eliminates "prop drilling," and
//! enables advanced, configurable behavior in the `Planner` and other
//! future components.

use serde::{Deserialize, Serialize};

//==================================================================================
// I. Core Configuration Enums & Structs
//==================================================================================

/// Defines the overall output file format. This is a high-level setting that
/// determines the structure of the output written by the `Compressor`.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompressionFormat {
    /// **Default:** A single, monolithic file with a metadata footer containing the
    /// chunk manifest. Ideal for analytics and writing to object stores (e.g., S3).
    /// This mode requires the writer to implement `std::io::Seek`.
    #[default]
    ColumnarFile,

    /// **Aspirational:** Chunks are written to the stream as they are produced,
    /// interleaved with their own metadata. Ideal for network streaming where
    // seeking to the end of the stream is not possible.
    InterleavedStream,
}

/// Defines the trade-off between compression speed and final file size.
///
/// This enum is the primary input to the `Planner` in the `chunk_pipeline`.
/// It allows the user to guide the planning process toward their desired outcome
/// without needing to know the specifics of the underlying compression operations.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompressionProfile {
    /// Prioritizes speed over size. Uses lower Zstd levels and avoids
    /// computationally expensive pipeline candidates like exhaustive dictionary search.
    Fast,

    /// A balance between speed and size. This is the recommended default.
    #[default]
    Balanced,

    /// Prioritizes the smallest possible file size at the cost of CPU time.
    /// Uses high Zstd levels and enables more exhaustive, and potentially slower,
    /// pipeline planning strategies.
    HighCompression,
}

/// Defines the frame-level strategy for handling time-series data.
///
/// This is a high-level structural choice that determines how `RecordBatch`es
/// are processed by the `FramePipeline` before being passed to the `chunk_pipeline`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum TimeSeriesStrategy {
    /// **Default:** No special time-series handling. Columns are compressed sequentially
    /// as they appear in each `RecordBatch`.
    None,

    /// **Per-Batch Relinearization:** Within each `RecordBatch`, value columns are
    /// re-ordered based on a sort of the `stream_id_column_name` and
    /// `timestamp_column_name`. This is effective for improving compression of
    /// multiplexed data that is locally sorted.
    PerBatchRelinearization,

    /// **Partitioned:** The entire input stream is partitioned by the `key_column`.
    /// All data for a given key is grouped and compressed together, which can
    /// dramatically improve compression for data with many distinct time series.
    /// This is a more powerful, but more memory-intensive, strategy than relinearization.
    Partitioned {
        /// The name of the column to partition the data by. Must be an Int64 type.
        key_column: String,
        /// The target number of rows to buffer for a partition before flushing it as a chunk.
        #[serde(default = "default_partition_flush_rows")]
        partition_flush_rows: usize,
    },
}

// Implement `Default` manually for `TimeSeriesStrategy` because of the struct variant.
impl Default for TimeSeriesStrategy {
    fn default() -> Self {
        TimeSeriesStrategy::None
    }
}

/// Provides a sensible default for `partition_flush_rows` for serde.
fn default_partition_flush_rows() -> usize {
    100_000
}

//==================================================================================
// II. Aspirational Feature Placeholders
//==================================================================================

// TODO: Fully implement lossy compression options.
/// Defines the lossy compression strategy for a compression operation.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default)]
pub struct LossyConfig {
    // Example fields for the future:
    // pub strategy: LossyStrategy,
    // #[serde(default)]
    // pub bits_per_value: Option<f32>,
    // #[serde(default)]
    // pub absolute_error: Option<f64>,
}

// TODO: Fully implement advanced partitioning and spilling from design documents.
/// Defines configuration for advanced partitioning strategies like global sorting
/// and two-level bucketing, including spilling to disk or cloud storage.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PartitioningConfig {
    // Example fields for the future:
    // pub spill_enabled: bool,
    // pub spill_location: Option<String>, // e.g., "s3://bucket/spills/" or "/tmp/spills/"
    // pub max_memory_mb: u64,
}

// TODO: Fully implement resource management controls.
/// Defines limits on system resources like memory and concurrency.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ResourceManagementConfig {
    // Example fields for the future:
    // pub total_memory_limit_mb: Option<usize>,
    // pub max_parallel_tasks: Option<usize>,
}

// TODO: Fully implement flexible output configurations.
/// Defines the final output destination and format.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct OutputConfig {
    // Example fields for the future:
    // pub final_output_location: String, // e.g., "s3://bucket/final/" or "/data/final/"
    // pub output_format: String, // e.g., "parquet"
    // pub compression_codec: String, // e.g., "zstd"
}

// TODO: Fully implement monitoring and logging configurations.
/// Defines settings for metrics collection and logging.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct MonitoringConfig {
    // Example fields for the future:
    // pub enable_metrics: bool,
    // pub metrics_endpoint: Option<String>,
    // pub log_level: Option<String>,
}
//==================================================================================
// III. The Unified TambakConfig
//==================================================================================

/// The single, unified configuration for the entire Tambak compression process.
/// This struct is created once and shared throughout the system via an `Arc`.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct TambakConfig {
    /// The overall output file format.
    #[serde(default)]
    pub format: CompressionFormat,

    /// The primary profile guiding compression/speed trade-offs in the Planner.
    #[serde(default)]
    pub profile: CompressionProfile,
    
    /// If true, a footer containing the chunk manifest will be written.
    /// This is essential for the `ColumnarFile` format and ignored otherwise.
    #[serde(default = "default_true")]
    pub include_footer: bool,

    /// If true, enables collection of detailed per-column compression statistics.
    /// Useful for diagnostics, but may have a minor performance impact.
    #[serde(default)]
    pub enable_stats_collection: bool,

    // --- Current, Implemented Functionality ---
    /// The frame-level strategy for handling time-series data.
    #[serde(default)]
    pub time_series_strategy: TimeSeriesStrategy,

    /// **The target number of rows per chunk.**
    /// For simple strategies (`None`, `PerBatchRelinearization`), this is used to
    /// slice large RecordBatches into smaller, memory-friendly chunks before
    /// compression. For the `Partitioned` strategy, this setting is ignored
    /// in favor of `partition_flush_rows` within the enum.
    #[serde(default = "default_chunk_size_rows")]
    pub chunk_size_rows: usize,

    /// Name of the column to use as the primary sorting key (e.g., "unit_id").
    /// Only relevant if `time_series_strategy` is `PerBatchRelinearization`.
    #[serde(default)]
    pub stream_id_column_name: Option<String>,

    /// Name of the column to use as the secondary sorting key (e.g., "timestamp").
    /// Only relevant if `time_series_strategy` is `PerBatchRelinearization`.
    #[serde(default)]
    pub timestamp_column_name: Option<String>,

    // --- Aspirational Features (Placeholders for Future Implementation) ---
    /// Configuration for lossy compression.
    #[serde(default)]
    pub lossy: Option<LossyConfig>,

    /// Configuration for advanced partitioning and spilling.
    #[serde(default)]
    pub partitioning: Option<PartitioningConfig>,

    /// Configuration for resource management (memory, concurrency).
    #[serde(default)]
    pub resource_management: Option<ResourceManagementConfig>,

    /// Configuration for the final output destination and format.
    #[serde(default)]
    pub output: Option<OutputConfig>,

    /// Configuration for monitoring and logging.
    #[serde(default)]
    pub monitoring: Option<MonitoringConfig>,
}

// Default implementation to make constructing the config easier.
impl Default for TambakConfig {
    fn default() -> Self {
        Self {
            format: CompressionFormat::default(),
            profile: CompressionProfile::default(),
            include_footer: true,
            enable_stats_collection: false,
            time_series_strategy: TimeSeriesStrategy::default(),
            chunk_size_rows: default_chunk_size_rows(),
            stream_id_column_name: None,
            timestamp_column_name: None,
            lossy: None,
            partitioning: None,
            resource_management: None,
            output: None,
            monitoring: None,
        }
    }
}

/// Helper for `serde` to default a boolean field to true.
fn default_true() -> bool {
    true
}

/// Helper for `serde` to provide a default for `chunk_size_rows`.
fn default_chunk_size_rows() -> usize {
    100_000
}