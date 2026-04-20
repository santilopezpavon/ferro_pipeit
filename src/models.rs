use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskDefinition {
    pub cmd: String,
    #[serde(default)]
    pub deps: Vec<String>,
    #[serde(default)]
    pub retries: u32,
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub inputs: HashMap<String, String>,
    #[serde(default)]
    pub outputs: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PipelineConfig {
    pub tasks: HashMap<String, TaskDefinition>,
}
