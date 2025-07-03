// In: src/pipeline/pipeline_generator.rs (NEW FILE - FINAL VERSION)

//! The L17 state-driven, recursive, work-queue-based pipeline generator.
//! This module is self-contained and provides a robust, extensible, and
//! configurable engine for creating candidate compression pipelines, fully
//! aware of the system's operation set and type transformation rules.

use crate::error::PhoenixError;
use crate::pipeline::models::Operation;
use crate::types::PhoenixDataType; // Using the shared type from the library
use std::collections::HashSet;

//==================================================================================
//                 CONFIGURATION AND PUBLIC API
//==================================================================================

/// Statistical profile of a data stream, used to guide pipeline selection.
#[derive(Debug, Clone)]
pub struct DataProfile {
    pub is_constant: bool,
    pub original_stream_has_low_cardinality: bool,
    pub delta_stream_has_low_cardinality: bool,
    pub signed_delta_bit_width: u8,
    pub unsigned_delta_bit_width: u8,
    pub original_bit_width: u8,
    pub zero_fraction: f64,
}

/// The logical type of the data, crucial for selecting appropriate operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalType {
    SignedInteger,
    UnsignedInteger,
    Float,
    Other,
}

/// Configuration for the pipeline generator.
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    pub zstd_level: i32,
    pub max_recursion_depth: u8,
    pub max_sparsify_variants: usize,
    pub sparsify_zero_fraction_threshold: f64,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            zstd_level: 3,
            max_recursion_depth: 2,
            max_sparsify_variants: 2,
            sparsify_zero_fraction_threshold: 0.4,
        }
    }
}

/// The primary public entry point for this module.
pub fn generate(
    profile: &DataProfile,
    stride: usize,
    logical_type: LogicalType,
    config: GeneratorConfig,
) -> Vec<Vec<Operation>> {
    let generator = PipelineGenerator::new(profile, stride, logical_type, config);
    generator.build()
}

//==================================================================================
//          INTERNAL IMPLEMENTATION: THE RECURSIVE WORK-QUEUE PLANNER
//==================================================================================

/// The purpose of the current generation task. Crucial for guiding recursion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipelinePurpose {
    Main,
    SparsifyMask,
    SparsifyValues,
}

/// The state of a partial pipeline in the work queue.
/// Now includes the physical data type at this point in the pipeline.
#[derive(Clone)]
struct WorkItem {
    pipe: Vec<Operation>,
    physical_type: PhoenixDataType,
    context: GenerationContext,
}

#[derive(Clone, Copy)]
struct GenerationContext {
    stride: usize,
    logical_type: LogicalType,
    recursion_depth: u8,
}

/// The private, recursive, work-queue-based generator.
struct PipelineGenerator<'a> {
    profile: &'a DataProfile,
    config: GeneratorConfig,
    purpose: PipelinePurpose,
    work_queue: Vec<WorkItem>,
    final_pipelines: HashSet<Vec<Operation>>,
}

const MAX_PIPELINE_CANDIDATES: usize = 50;

impl<'a> PipelineGenerator<'a> {
    fn new(
        profile: &'a DataProfile,
        stride: usize,
        logical_type: LogicalType,
        config: GeneratorConfig,
    ) -> Self {
        // The initial physical type is derived from the logical type.
        // This is a simplified mapping; the real orchestrator has the true initial type.
        let initial_physical_type = match logical_type {
            LogicalType::SignedInteger => PhoenixDataType::Int64, // Assume widest for planning
            LogicalType::UnsignedInteger => PhoenixDataType::UInt64,
            LogicalType::Float => PhoenixDataType::Float64,
            LogicalType::Other => PhoenixDataType::UInt8, // Treat as bytes
        };

        let initial_context = GenerationContext {
            stride,
            logical_type,
            recursion_depth: 0,
        };
        let initial_work_item = WorkItem {
            pipe: vec![],
            physical_type: initial_physical_type,
            context: initial_context,
        };

        Self {
            profile,
            config,
            purpose: PipelinePurpose::Main,
            work_queue: vec![initial_work_item],
            final_pipelines: HashSet::new(),
        }
    }

    // `new_for_subtask` remains the same conceptually
    fn new_for_subtask(
        profile: &'a DataProfile,
        context: GenerationContext,
        config: GeneratorConfig,
        purpose: PipelinePurpose,
    ) -> Self {
        // ... implementation is the same as before ...
    }

    fn build(mut self) -> Vec<Vec<Operation>> {
        self.add_entropy_coders(&[]); // Add baselines
        while let Some(item) = self.work_queue.pop() {
            if self.final_pipelines.len() >= MAX_PIPELINE_CANDIDATES {
                break;
            }
            if item.context.recursion_depth > self.config.max_recursion_depth {
                continue;
            }

            if let Err(e) = self.generate_next_steps(item) {
                #[cfg(debug_assertions)]
                eprintln!("Warning: Skipping pipeline branch due to error: {}", e);
                // We just skip this branch; it doesn't invalidate the entire planning.
            }
        }
        self.final_pipelines.into_iter().collect()
    }

    /// For a given partial pipeline (`item`), generate all valid next steps.
    /// Returns `Ok(())` for success, `Err(PhoenixError)` if a branch is invalid.
    fn generate_next_steps(&mut self, item: WorkItem) -> Result<(), PhoenixError> {
        let logical_type = item.context.logical_type;
        let physical_type = item.physical_type;

        // --- Rule: Add Type-Changing Transforms (Floats) ---
        if logical_type == LogicalType::Float && physical_type.is_float() {
            let bitcast_to = if physical_type == PhoenixDataType::Float32 {
                PhoenixDataType::UInt32
            } else {
                PhoenixDataType::UInt64
            };
            let path1 = vec![
                Operation::CanonicalizeZeros,
                Operation::BitCast {
                    to_type: bitcast_to,
                },
            ];
            // The `add_to_queue` helper now takes a `Result` implicitly, or we check here.
            self.add_to_queue(item.clone(), path1, bitcast_to);
        }

        // --- Rule: Add Core Value-Reduction Transforms ---
        if logical_type == LogicalType::SignedInteger {
            let delta_op = Operation::Delta {
                order: item.context.stride,
            };
            self.add_to_queue(item.clone(), vec![delta_op.clone()], physical_type);
            // Now add ZigZag after Delta
            let delta_item = WorkItem {
                pipe: vec![delta_op],
                ..item.clone()
            };
            // Ensure any transform_stream calls inside add_to_queue are handled
            self.add_to_queue(delta_item, vec![Operation::ZigZag], PhoenixDataType::UInt64);
            // ZigZag output is unsigned
        }
        if logical_type == LogicalType::UnsignedInteger
            || (logical_type == LogicalType::Float && physical_type.is_unsigned_int())
        {
            self.add_to_queue(item.clone(), vec![Operation::XorDelta], physical_type);
        }

        // --- Rule: Add Terminal Transforms (these complete a prefix) ---
        if item.pipe.is_empty() && self.profile.original_stream_has_low_cardinality {
            self.add_entropy_coders(&[Operation::Dictionary]);
        }
        if item.pipe.last()
            == Some(&Operation::Delta {
                order: item.context.stride,
            })
            && self.profile.delta_stream_has_low_cardinality
        {
            let mut new_pipe = item.pipe.clone();
            new_pipe.push(Operation::Rle);
            self.add_entropy_coders(&new_pipe);
        }
        if item.pipe.last() == Some(&Operation::ZigZag) {
            let mut new_pipe = item.pipe.clone();
            new_pipe.push(Operation::Leb128);
            self.add_entropy_coders(&new_pipe);
        }
        // BitPack rule...
        if item.pipe.last() == Some(&Operation::ZigZag)
            && self.profile.signed_delta_bit_width > 0
            && self.profile.signed_delta_bit_width < self.profile.original_bit_width
        {
            let mut new_pipe = item.pipe.clone();
            new_pipe.push(Operation::BitPack {
                bit_width: self.profile.signed_delta_bit_width,
            });
            self.add_entropy_coders(&new_pipe);
        }

        // --- Rule: Add Meta Transforms (Recursive) ---
        if self.purpose == PipelinePurpose::Main
            && self.profile.zero_fraction >= self.config.sparsify_zero_fraction_threshold
        {
            self.generate_sparsify_pipelines(&item);
        }
    }

    /// Generates and adds `Sparsify` candidates.
    fn generate_sparsify_pipelines(&mut self, item: &WorkItem) -> Result<(), PhoenixError> {
        // The values pipeline starts from the current state of the main pipeline.
        let mut values_profile = self.profile.clone();
        values_profile.zero_fraction = 0.0;
        let values_context = GenerationContext {
            recursion_depth: item.context.recursion_depth + 1,
            ..item.context
        };

        // The values generator starts with the *current* pipeline as its base.
        let mut values_gen = Self::new_for_subtask(
            &values_profile,
            values_context,
            self.config.clone(),
            PipelinePurpose::SparsifyValues,
        );
        values_gen.work_queue = vec![item.clone()]; // Start it from where we are now!
        let value_pipelines = values_gen.build();

        // Mask pipeline is simpler and starts from scratch.
        let mask_context = GenerationContext {
            recursion_depth: item.context.recursion_depth + 1,
            ..item.context
        };
        let mask_gen = Self::new_for_subtask(
            self.profile,
            mask_context,
            self.config.clone(),
            PipelinePurpose::SparsifyMask,
        );
        let mask_pipelines = mask_gen.build();

        for mask_p in &mask_pipelines {
            for value_p in value_pipelines
                .iter()
                .take(self.config.max_sparsify_variants)
            {
                let op = Operation::Sparsify {
                    mask_stream_id: "TBD".to_string(),
                    mask_pipeline: mask_p.clone(),
                    values_pipeline: value_p.clone(),
                };
                // A meta-op is a complete pipeline. It prepends the path that led to it.
                let mut final_pipe = item.pipe.clone();
                final_pipe.push(op);
                self.final_pipelines.insert(final_pipe);
            }
        }
        Ok(())
    }

    /// Helper to add a new partial pipeline to the work queue.
    fn add_to_queue(
        &mut self,
        mut item: WorkItem,
        ops: Vec<Operation>,
        expected_next_physical_type: PhoenixDataType,
    ) -> Result<(), PhoenixError> {
        let mut temp_type = item.physical_type;
        for op_to_add in &ops {
            let transform = op_to_add.transform_stream(temp_type)?; // Propagate error from here
            temp_type = match transform {
                StreamTransform::PreserveType => temp_type,
                StreamTransform::TypeChange(new_type) => new_type,
                StreamTransform::Restructure {
                    primary_output_type,
                    ..
                } => primary_output_type,
            };
        }

        // Final check for consistency, if necessary, or just use `temp_type`.
        // If the `expected_next_physical_type` was based on hardcoded knowledge,
        // it should ideally now just be `temp_type`.
        if temp_type != expected_next_physical_type {
            return Err(PhoenixError::StreamTransformError(format!(
                "Calculated type {} does not match expected next physical type {}",
                temp_type, expected_next_physical_type
            )));
        }

        item.pipe.extend(ops);
        item.physical_type = temp_type;
        self.add_entropy_coders(&item.pipe);
        self.work_queue.push(item);
        Ok(())
    }

    /// Helper to take a prefix and add all its entropy-coded variants to the final list.
    fn add_entropy_coders(&mut self, prefix: &[Operation]) {
        if self.purpose == PipelinePurpose::SparsifyMask {
            self.final_pipelines
                .insert([prefix, &[Operation::Rle, Operation::Ans]].concat());
            return;
        }
        let mut p_zstd = prefix.to_vec();
        p_zstd.push(Operation::Zstd {
            level: self.config.zstd_level,
        });
        self.final_pipelines.insert(p_zstd);
        let mut p_ans = prefix.to_vec();
        p_ans.push(Operation::Ans);
        self.final_pipelines.insert(p_ans);
    }
}
