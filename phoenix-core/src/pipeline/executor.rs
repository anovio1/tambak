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
use colored::*;
fn color_tag(text: &str) -> String {
    if cfg!(debug_assertions) {
        text.truecolor(150, 150, 150).bold().to_string() // soft gray
    } else {
        text.to_string()
    }
}

fn color_value<T: std::fmt::Display>(val: T) -> String {
    if cfg!(debug_assertions) {
        format!("{}", val).cyan().to_string() // clean white for values
    } else {
        format!("{}", val)
    }
}

fn color_length(len: usize) -> String {
    if cfg!(debug_assertions) {
        format!("{}", len).cyan().to_string() // subtle cyan for length values
    } else {
        format!("{}", len)
    }
}

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
    #[cfg(debug_assertions)]
    {
        println!(
            "[DEBUG] execute_linear_encode complete, {}= {}, {}= {}, {}= {}",
            color_tag("initial_type"),
            color_value(format!("{:?}", initial_type)),
            color_tag("pipeline"),
            color_value(format!("{:?}", pipeline)),
            color_tag("final length"),
            color_value(buffer_a.len())
        );
    }
    Ok(buffer_a)
}

/// Executes a linear decoding pipeline.
///
/// This function is a "dumb engine" that requires a pre-calculated `type_flow`
/// stack from the Orchestrator, which is the single source of truth for the
/// expected output type of each decoding step.
// In: src/pipeline/executor.rs

pub(crate) fn execute_linear_decode_pipeline(
    bytes: &[u8],
    type_flow: &mut Vec<PhoenixDataType>,
    pipeline: &[Operation],
    num_values: usize,
) -> Result<Vec<u8>, PhoenixError> {
    if pipeline.is_empty() {
        return Ok(bytes.to_vec());
    }

    // Start with the initial compressed data.
    let mut current_data = bytes.to_vec();

    let mut current_type = type_flow.pop().ok_or_else(|| {
        PhoenixError::InternalError("Type flow stack is empty at start of decode.".to_string())
    })?;

    // Iterate through the pipeline in reverse for decoding.
    for op in pipeline.iter().rev() {
        let target_type = type_flow.pop().ok_or_else(|| {
            PhoenixError::InternalError(format!(
                "Type flow stack exhausted prematurely while decoding op: {:?}",
                op
            ))
        })?;

        #[cfg(debug_assertions)]
        {
            println!("--------------------------------------------------");
            println!(
                "[EXECUTOR-DECODE] Op: {:?}, In-Type: {:?}, Target-Type: {:?}, In-Len: {}",
                op,
                current_type,
                target_type,
                current_data.len()
            );
        }

        // The dispatcher will call the kernel, which returns a *new* Vec.
        // We create a temporary buffer for the dispatcher to write into.
        let mut output_buf = Vec::new();
        kernels::dispatch_decode(
            op,
            &current_data,
            &mut output_buf,
            current_type,
            target_type,
            num_values,
        )?;

        #[cfg(debug_assertions)]
        {
            println!("[EXECUTOR-DECODE] SUCCESS -> Out-Len: {}", output_buf.len());
        }

        // The output of this stage becomes the input for the next stage.
        current_data = output_buf;
        current_type = target_type;
    }

    // The final result is the data held in `current_data`.
    Ok(current_data)
}
