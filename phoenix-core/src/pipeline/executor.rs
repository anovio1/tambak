//! This module contains the v4.3 core execution logic for the Phoenix compression
//! and decompression pipelines.
//!
//! It acts as the "Pure Byte Engine," a non-strategic component that faithfully
//! executes a linear sequence of operations from a plan.

use crate::error::PhoenixError;
use crate::kernels;
use crate::pipeline::models::Operation;
use crate::pipeline::traits::TypeTransformer;
use crate::types::PhoenixDataType;

/// Executes a linear encoding pipeline.
///
/// This function is the "dumb" workhorse. It iterates through a sequence of
/// operations, derives the type for each step using the `TypeTransformer` trait,
/// and dispatches to the correct kernel.
pub(crate) fn execute_linear_encode_pipeline(
    bytes: &[u8],
    initial_type: PhoenixDataType,
    pipeline: &[Operation],
) -> Result<Vec<u8>, PhoenixError> {
    if pipeline.is_empty() {
        return Ok(bytes.to_vec());
    }

    let mut buffer_a = bytes.to_vec();
    let mut buffer_b = Vec::with_capacity(buffer_a.len());
    let mut current_type = initial_type;

    for op in pipeline {
        // The type of the data *entering* this operation is `current_type`.
        kernels::dispatch_encode(op, &buffer_a, &mut buffer_b, current_type)?;

        // The type of the data *exiting* this operation is determined by the transformer.
        current_type = op.transform_type(current_type)?;

        std::mem::swap(&mut buffer_a, &mut buffer_b);
        buffer_b.clear();
    }

    Ok(buffer_a)
}

/// Executes a linear decoding pipeline.
///
/// This function is a "dumb engine" that requires a pre-calculated `type_flow`
/// stack from the Orchestrator, which is the single source of truth for the
/// expected output type of each decoding step.
pub(crate) fn execute_linear_decode_pipeline(
    bytes: &[u8],
    type_flow: &mut Vec<PhoenixDataType>, // A stack of the *output* types for each decode step.
    pipeline: &[Operation],
    num_values: usize,
) -> Result<Vec<u8>, PhoenixError> {
    if pipeline.is_empty() {
        return Ok(bytes.to_vec());
    }

    let mut buffer_a = bytes.to_vec();
    let mut buffer_b = Vec::with_capacity(buffer_a.len());

    // The Orchestrator provides the correctly ordered type_flow stack.
    // We just pop the target type for each reversed operation.
    for op in pipeline.iter().rev() {
        let target_type = type_flow.pop().ok_or_else(|| {
            PhoenixError::InternalError("Type flow stack exhausted prematurely.".to_string())
        })?;
        kernels::dispatch_decode(op, &buffer_a, &mut buffer_b, target_type, num_values)?;
        std::mem::swap(&mut buffer_a, &mut buffer_b);
        buffer_b.clear();
    }

    Ok(buffer_a)
}
