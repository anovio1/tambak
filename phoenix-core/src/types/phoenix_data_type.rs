//! This module defines the canonical, type-safe representation of data types
//! used throughout the Phoenix v4.3+ pipeline.

use crate::error::PhoenixError;
use arrow::datatypes::DataType as ArrowDataType;
use std::fmt;
use serde::{Serialize, Deserialize};

/// The canonical, internal representation of a data type in the Phoenix pipeline.
///
/// This enum replaces the fragile string-based type system of previous versions,
/// enabling compile-time checks and eliminating an entire class of runtime errors.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PhoenixDataType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Boolean,
}

impl PhoenixDataType {
    /// Converts an Arrow `DataType` into a `PhoenixDataType`.
    pub fn from_arrow_type(arrow_type: &ArrowDataType) -> Result<Self, PhoenixError> {
        match arrow_type {
            ArrowDataType::Int8 => Ok(Self::Int8),
            ArrowDataType::Int16 => Ok(Self::Int16),
            ArrowDataType::Int32 => Ok(Self::Int32),
            ArrowDataType::Int64 => Ok(Self::Int64),
            ArrowDataType::UInt8 => Ok(Self::UInt8),
            ArrowDataType::UInt16 => Ok(Self::UInt16),
            ArrowDataType::UInt32 => Ok(Self::UInt32),
            ArrowDataType::UInt64 => Ok(Self::UInt64),
            ArrowDataType::Float32 => Ok(Self::Float32),
            ArrowDataType::Float64 => Ok(Self::Float64),
            ArrowDataType::Boolean => Ok(Self::Boolean),
            dt => Err(PhoenixError::UnsupportedType(format!(
                "Cannot convert Arrow type {:?} to PhoenixDataType",
                dt
            ))),
        }
    }

    /// Converts a `PhoenixDataType` back into an Arrow `DataType`.
    pub fn to_arrow_type(&self) -> ArrowDataType {
        match self {
            Self::Int8 => ArrowDataType::Int8,
            Self::Int16 => ArrowDataType::Int16,
            Self::Int32 => ArrowDataType::Int32,
            Self::Int64 => ArrowDataType::Int64,
            Self::UInt8 => ArrowDataType::UInt8,
            Self::UInt16 => ArrowDataType::UInt16,
            Self::UInt32 => ArrowDataType::UInt32,
            Self::UInt64 => ArrowDataType::UInt64,
            Self::Float32 => ArrowDataType::Float32,
            Self::Float64 => ArrowDataType::Float64,
            Self::Boolean => ArrowDataType::Boolean,
        }
    }
}

/// Provides the canonical string representation for a `PhoenixDataType`.
impl fmt::Display for PhoenixDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // These string representations are part of the public contract.
        // They match the Arrow `DataType` string representation.
        write!(f, "{:?}", self)
    }
}
