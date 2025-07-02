//! Defines the behavioral traits for pipeline operations.

use crate::error::PhoenixError;
use crate::pipeline::models::Operation;
use crate::types::PhoenixDataType;

/// A trait implemented by every operation, declaring how it transforms a type.
/// This decentralizes type logic and makes the system easily extensible.
pub trait TypeTransformer {
    /// Given an input type, returns the resulting output type.
    fn transform_type(&self, input: PhoenixDataType) -> Result<PhoenixDataType, PhoenixError>;
}

impl TypeTransformer for Operation {
    /// Given an input type, returns the resulting output type. This implementation
    /// serves as the single source of truth for how each kernel affects the
    /// logical data type of the stream.
    fn transform_type(&self, input: PhoenixDataType) -> Result<PhoenixDataType, PhoenixError> {
        use PhoenixDataType::*;
        match self {
            // Operations that do not change the logical data type.
            Operation::CanonicalizeZeros
            | Operation::Delta { .. }
            | Operation::XorDelta
            | Operation::Rle
            | Operation::Dictionary
            | Operation::Leb128
            | Operation::BitPack { .. }
            | Operation::Shuffle
            | Operation::Zstd { .. }
            | Operation::Ans => Ok(input),

            // Operations that explicitly change the type.
            Operation::BitCast { to_type } => Ok(*to_type),

            Operation::ZigZag => match input {
                Int8 => Ok(UInt8),
                Int16 => Ok(UInt16),
                Int32 => Ok(UInt32),
                Int64 => Ok(UInt64),
                _ => Err(PhoenixError::UnsupportedType(format!(
                    "ZigZag requires a signed integer type, but got {:?}",
                    input
                ))),
            },

            // Meta-operations do not transform the main data stream's type in a linear way.
            // They restructure the pipeline itself. The Planner is responsible for handling this.
            // Returning the input type unmodified is the correct, neutral action, as the
            // executor will recursively call the pipeline on the sub-streams.
            Operation::Sparsify { .. } | Operation::ExtractNulls { .. } => Ok(input),
        }
    }
}