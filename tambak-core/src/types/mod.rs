//! This module defines the core, strongly-typed data representations used
//! throughout the tambak v4.3+ pipeline.
//!
//! It currently includes the canonical `tambakDataType` enum which replaces
//! fragile string-based types with a safe, serializable, and Arrow-compatible enum.
//!
//! Additional types related to metadata, validation, or encoding may be added here.

pub mod tambak_data_type;

// Future modules can be added here, for example:
// pub mod type_metadata;
// pub mod encoding;

// Re-export the main type(s) for easier access.
pub use tambak_data_type::TambakDataType;
