use crate::models::PipelineConfig;
use anyhow::{anyhow, Result};
use petgraph::algo::is_cyclic_directed;
use petgraph::graph::DiGraph;
use petgraph::prelude::*;
use std::collections::{HashMap, HashSet};

pub struct Dag {
    pub graph: DiGraph<String, ()>,
}

impl Dag {
    pub fn new(config: &PipelineConfig) -> Result<Self> {
        let mut graph = DiGraph::<String, ()>::new();
        let mut node_indices = HashMap::new();

        // Add all nodes
        for task_id in config.tasks.keys() {
            let idx = graph.add_node(task_id.clone());
            node_indices.insert(task_id.clone(), idx);
        }

        // Add all edges
        for (task_id, task_def) in &config.tasks {
            let target_idx = node_indices.get(task_id).unwrap();
            for dep_id in &task_def.deps {
                let source_idx = node_indices.get(dep_id)
                    .ok_or_else(|| anyhow!("Task '{}' depends on unknown task '{}'", task_id, dep_id))?;
                graph.add_edge(*source_idx, *target_idx, ());
            }
        }

        // Validate: cycle detection
        if is_cyclic_directed(&graph) {
            return Err(anyhow!("The pipeline definition contains a cycle and is not a valid DAG"));
        }

        Ok(Self { graph })
    }

    /// Returns a list of tasks that can be started based on completed tasks.
    pub fn get_ready_tasks(&self, completed_tasks: &HashSet<String>) -> Vec<String> {
        let mut ready = Vec::new();

        for node_idx in self.graph.node_indices() {
            let task_id = &self.graph[node_idx];
            if completed_tasks.contains(task_id) {
                continue;
            }

            // A task is ready if all its incoming neighbors (dependencies) are completed
            let mut deps_satisfied = true;
            for neighbor_idx in self.graph.neighbors_directed(node_idx, Incoming) {
                let dep_task_id = &self.graph[neighbor_idx];
                if !completed_tasks.contains(dep_task_id) {
                    deps_satisfied = false;
                    break;
                }
            }

            if deps_satisfied {
                ready.push(task_id.clone());
            }
        }

        ready
    }

    /// Total number of tasks in the DAG
    pub fn total_tasks(&self) -> usize {
        self.graph.node_count()
    }
}
