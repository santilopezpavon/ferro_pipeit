mod engine;
mod persistence;

pub use engine::CacheEngine;
pub use persistence::CacheMetadata;

/// Requirement: Only omit if there are both inputs and outputs.
pub fn is_task_cacheable(task_def: &crate::models::TaskDefinition) -> bool {
    !task_def.inputs.is_empty() && !task_def.outputs.is_empty()
}