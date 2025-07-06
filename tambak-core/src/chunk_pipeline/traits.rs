// In: src/pipeline/traits.rs

//! Defines the behavioral traits for pipeline operations.
//!
//! This module moves beyond simple type transformation to
//! fully describe the structural changes an operation imparts on a data stream.
//! The `OperationBehavior` trait and its `StreamTransform` return type create an
//! explicit, machine-readable contract for every operation, including complex
//! meta-operations like `Sparsify`. This eliminates ambiguity and enables the
//! executor to be driven directly by the described behavior.

use crate::chunk_pipeline::models::Operation;
use crate::error::tambakError;
use crate::types::TambakDataType;

// --- NEW: A richer enum to describe the outcome of an operation ---
/// Describes the effect an operation has on the data stream's structure and type.
/// This is the return type for the `OperationBehavior` trait, providing a much
/// richer contract than a simple `tambakDataType`.
#[derive(Debug, PartialEq, Eq)]
pub enum StreamTransform {
    /// The operation preserves the data type of the stream.
    /// Example: Delta, Rle, Zstd.
    PreserveType,
    /// The operation explicitly changes the data type of the stream.
    /// Example: ZigZag (Int32 -> UInt32), BitCast.
    TypeChange(TambakDataType),
    /// The operation restructures the stream, creating new sub-streams.
    /// This is used by meta-operations.
    Restructure {
        /// A description of the primary output stream.
        /// For Sparsify, this would be the dense values stream.
        primary_output_type: TambakDataType,
        /// A list of new, secondary streams created by the operation.
        /// Each tuple contains the stream's purpose (e.g., "mask") and its type.
        secondary_outputs: Vec<(&'static str, TambakDataType)>,
    },
}

/// A trait implemented by every operation, declaring its full behavioral impact.
pub trait OperationBehavior {
    /// Describes the full structural and type transformation of the operation.
    fn transform_stream(&self, input: TambakDataType) -> Result<StreamTransform, tambakError>;
}

impl OperationBehavior for Operation {
    /// This implementation is the single source of truth for the behavior of all operations.
    fn transform_stream(&self, input: TambakDataType) -> Result<StreamTransform, tambakError> {
        use TambakDataType::*;
        use StreamTransform::*;

        match self {
            //======================================================================
            // Group 1: Operations that TRULY change the logical type.
            //======================================================================
            Operation::ZigZag => match input {
                Int8 => Ok(TypeChange(UInt8)),
                Int16 => Ok(TypeChange(UInt16)),
                Int32 => Ok(TypeChange(UInt32)),
                Int64 => Ok(TypeChange(UInt64)),
                _ => Err(tambakError::UnsupportedType(format!(
                    "ZigZag requires a signed integer type, but got {:?}",
                    input
                ))),
            },
            Operation::BitCast { to_type } => Ok(TypeChange(*to_type)),

            //======================================================================
            // Group 2: Operations that preserve the logical type.
            //======================================================================
            
            Operation::Ans
            | Operation::CanonicalizeZeros
            | Operation::Dictionary          
            | Operation::Leb128              
            | Operation::Rle 
            | Operation::Shuffle 
            | Operation::XorDelta
            | Operation::BitPack { .. }      
            | Operation::Delta { .. }        
            | Operation::Zstd { .. }         
             => {
                Ok(PreserveType)
            }

            //======================================================================
            // Group 4: Meta-operations that restructure the entire stream.
            //======================================================================
            Operation::Sparsify { .. } => Ok(Restructure {
                // The primary output (the `values_pipeline`) operates on the original type.
                primary_output_type: input,
                // It creates one new secondary stream: the boolean mask.
                secondary_outputs: vec![("mask", TambakDataType::Boolean)],
            }),

            Operation::ExtractNulls { .. } => Ok(Restructure {
                primary_output_type: input, // The "non-nulls" stream retains the original type
                secondary_outputs: vec![("null_mask", TambakDataType::Boolean)], // The generated null mask stream
            }),
        }
    }
}

//==================================================================================
// Unit Tests (Updated for the new architecture)
//==================================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk_pipeline::models::Operation;
    use crate::types::TambakDataType;

    #[test]
    fn test_transform_stream_preserves_type_for_various_ops() {
        // Test Delta (from old tests)
        let op = Operation::Delta { order: 1 };
        let result = op.transform_stream(TambakDataType::Int32).unwrap();
        assert_eq!(result, StreamTransform::PreserveType);

        // Test Rle (from old tests)
        let op = Operation::Rle;
        let result = op.transform_stream(TambakDataType::UInt64).unwrap();
        assert_eq!(result, StreamTransform::PreserveType);

        // Add more preserve-type operations for coverage
        let ops_to_test = vec![
            (Operation::Ans, "Ans"),
            (Operation::BitPack { bit_width: 8 }, "BitPack"),
            (Operation::CanonicalizeZeros, "CanonicalizeZeros"),
            (Operation::Dictionary, "Dictionary"),
            (Operation::Leb128, "Leb128"),
            (Operation::Shuffle, "Shuffle"),
            (Operation::XorDelta, "XorDelta"),
            (Operation::Zstd { level: 3 }, "Zstd"),
        ];
        let input_type = TambakDataType::Int32; // Arbitrary input type

    for (op, op_name) in ops_to_test {
        let result = op.transform_stream(input_type).unwrap();
        assert_eq!(
            result,
            StreamTransform::PreserveType,
                "Op {:?} should preserve type",
            op_name
        );
    }
    }

    #[test]
    fn test_transform_stream_changes_type_for_zigzag() {
        // Test ZigZag (from old tests)
        let op = Operation::ZigZag;
        let result = op.transform_stream(TambakDataType::Int32).unwrap();
        assert_eq!(result, StreamTransform::TypeChange(TambakDataType::UInt32));

        // Test other signed integer types for ZigZag
        assert_eq!(
            Operation::ZigZag
                .transform_stream(TambakDataType::Int8)
                .unwrap(),
            StreamTransform::TypeChange(TambakDataType::UInt8)
        );
        assert_eq!(
            Operation::ZigZag
                .transform_stream(TambakDataType::Int16)
                .unwrap(),
            StreamTransform::TypeChange(TambakDataType::UInt16)
        );
        assert_eq!(
            Operation::ZigZag
                .transform_stream(TambakDataType::Int64)
                .unwrap(),
            StreamTransform::TypeChange(TambakDataType::UInt64)
        );
    }

    #[test]
    fn test_transform_stream_changes_type_for_bitcast() {
        // Test BitCast (from old tests)
        let op = Operation::BitCast {
            to_type: TambakDataType::UInt64,
        };
        let result = op.transform_stream(TambakDataType::Float64).unwrap();
        assert_eq!(result, StreamTransform::TypeChange(TambakDataType::UInt64));

        // Add another BitCast scenario
        let op2 = Operation::BitCast {
            to_type: TambakDataType::Int32,
        };
        let result2 = op2.transform_stream(TambakDataType::UInt32).unwrap();
        assert_eq!(result2, StreamTransform::TypeChange(TambakDataType::Int32));
    }

    #[test]
    fn test_transform_stream_errors_for_unsupported_zigzag() {
        // Test unsupported type for ZigZag (from old tests)
        let op = Operation::ZigZag;
        let result = op.transform_stream(TambakDataType::Float32);
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(crate::error::tambakError::UnsupportedType(_))
        ));

        // Add another unsupported type for ZigZag
        let result2 = op.transform_stream(TambakDataType::Boolean);
        assert!(result2.is_err());
    }

    #[test]
    fn test_transform_stream_describes_restructure_for_sparsify_all_types() {
        let op = Operation::Sparsify {
            mask_stream_id: "sparsity_mask".to_string(),
            mask_pipeline: vec![],
            values_pipeline: vec![],
        };

        // List all tambakDataType variants you want to test
        let all_types = vec![
            TambakDataType::Int8,
            TambakDataType::Int16,
            TambakDataType::Int32,
            TambakDataType::Int64,
            TambakDataType::UInt8,
            TambakDataType::UInt16,
            TambakDataType::UInt32,
            TambakDataType::UInt64,
            TambakDataType::Float32,
            TambakDataType::Float64,
            // Add others if applicable
        ];

        for input_type in all_types {
            let result = op.transform_stream(input_type).unwrap();
            let expected = StreamTransform::Restructure {
                primary_output_type: input_type,
                secondary_outputs: vec![("mask", TambakDataType::Boolean)],
            };
            assert_eq!(result, expected, "Failed for input type: {:?}", input_type);
        }
    }

    #[test]
    fn test_transform_stream_describes_restructure_for_extract_nulls() {
        // New test for ExtractNulls meta-operation
        let op = Operation::ExtractNulls {
            output_stream_id: "null_mask_stream".to_string(), // Use actual field names
            null_mask_pipeline: vec![],
        };
        let input_type = TambakDataType::Float32; // Example input type for ExtractNulls

        let result = op.transform_stream(input_type).unwrap();
        let expected = StreamTransform::Restructure {
            primary_output_type: input_type,
            secondary_outputs: vec![("null_mask", TambakDataType::Boolean)],
        };
        assert_eq!(result, expected);
    }
}
