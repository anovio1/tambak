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

//==================================================================================
// Unit Tests
//==================================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::models::Operation;
    use crate::types::PhoenixDataType;

    #[test]
    fn test_transform_type_preserves_for_delta() {
        let op = Operation::Delta { order: 1 };
        let input = PhoenixDataType::Int32;
        let output = op.transform_type(input).unwrap();
        assert_eq!(output, PhoenixDataType::Int32);
    }

    #[test]
    fn test_transform_type_preserves_for_rle() {
        let op = Operation::Rle;
        let input = PhoenixDataType::UInt64;
        let output = op.transform_type(input).unwrap();
        assert_eq!(output, PhoenixDataType::UInt64);
    }

    #[test]
    fn test_transform_type_changes_for_zigzag() {
        let op = Operation::ZigZag;
        let input = PhoenixDataType::Int32;
        let output = op.transform_type(input).unwrap();
        assert_eq!(output, PhoenixDataType::UInt32);
    }

    #[test]
    fn test_transform_type_changes_for_bitcast() {
        let op = Operation::BitCast {
            to_type: PhoenixDataType::UInt64,
        };
        let input = PhoenixDataType::Float64;
        let output = op.transform_type(input).unwrap();
        assert_eq!(output, PhoenixDataType::UInt64);
    }

    #[test]
    fn test_transform_type_errors_for_unsupported_zigzag() {
        let op = Operation::ZigZag;
        let input = PhoenixDataType::Float32; // ZigZag doesn't support floats
        let result = op.transform_type(input);
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(crate::error::PhoenixError::UnsupportedType(_))
        ));
    }

    #[test]
    fn test_transform_type_noop_for_meta_ops() {
        let sparsify_op = Operation::Sparsify {
            mask_stream_id: "m".to_string(),
            mask_pipeline: vec![],
            values_pipeline: vec![],
        };
        let input = PhoenixDataType::Int32;
        let output = sparsify_op.transform_type(input).unwrap();
        // Meta-ops should not change the type of the main stream in the context of the transformer.
        assert_eq!(output, PhoenixDataType::Int32);

        let extract_op = Operation::ExtractNulls {
            output_stream_id: "n".to_string(),
            null_mask_pipeline: vec![],
        };
        let output2 = extract_op.transform_type(input).unwrap();
        assert_eq!(output2, PhoenixDataType::Int32);
    }
}
