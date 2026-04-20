use crate::dag::Dag;
use crate::models::PipelineConfig;
use anyhow::{anyhow, Result};
use std::collections::HashSet;
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::{info, warn, error};
use std::time::Duration;

#[derive(Debug)]
enum TaskResult {
    Success(String),
    Failure(String, u32),
}

pub struct Runner {
    config: PipelineConfig,
    dag: Dag,
}

impl Runner {
    pub fn new(config: PipelineConfig, dag: Dag) -> Self {
        Self { config, dag }
    }

    pub async fn run(&self) -> Result<()> {
        let mut completed_tasks = HashSet::<String>::new();
        let mut running_tasks = HashSet::<String>::new();
        let (tx, mut rx) = mpsc::channel(100);

        let total_tasks = self.dag.total_tasks();
        let mut finished_count = 0;

        loop {
            let ready_tasks = self.dag.get_ready_tasks(&completed_tasks);

            for task_id in ready_tasks {
                if !running_tasks.contains(&task_id) {
                    let task_def = self.config.tasks.get(&task_id).unwrap().clone();

                    // Validate all declared inputs exist on disk
                    let mut missing = false;
                    for (name, path) in &task_def.inputs {
                        if !std::path::Path::new(path).exists() {
                            error!(
                                "Task '{}': input '{}' not found at '{}'",
                                task_id, name, path
                            );
                            missing = true;
                        }
                    }
                    if missing {
                        let _ = tx.try_send(TaskResult::Failure(task_id.clone(), 0));
                        continue;
                    }

                    running_tasks.insert(task_id.clone());
                    let tx_for_spawn = tx.clone();
                    let task_id_clone = task_id.clone();

                    info!("Launching task: {}", task_id);
                    tokio::spawn(async move {
                        let mut retries = 0u32;
                        loop {
                            let mut cmd = Command::new("sh");
                            cmd.arg("-c")
                                .arg(&task_def.cmd)
                                .env("TASK_NAME", &task_id_clone);

                            for (name, path) in &task_def.inputs {
                                cmd.env(format!("IN_{}", name.to_uppercase()), path);
                            }
                            for (name, path) in &task_def.outputs {
                                cmd.env(format!("OUT_{}", name.to_uppercase()), path);
                            }

                            match cmd.spawn() {
                                Ok(mut child_proc) => {
                                    let status = if let Some(t) = task_def.timeout_secs {
                                        match tokio::time::timeout(
                                            Duration::from_secs(t),
                                            child_proc.wait(),
                                        ).await {
                                            Ok(result) => result,
                                            Err(_) => {
                                                let _ = child_proc.kill().await;
                                                warn!("Task '{}' timed out after {}s", task_id_clone, t);
                                                Err(std::io::Error::new(
                                                    std::io::ErrorKind::TimedOut,
                                                    format!("Task timed out after {}s", t),
                                                ))
                                            }
                                        }
                                    } else {
                                        child_proc.wait().await
                                    };

                                    match status {
                                        Ok(s) if s.success() => {
                                            let _ = tx_for_spawn.send(TaskResult::Success(task_id_clone)).await;
                                            return;
                                        }
                                        Ok(s) => warn!("Task '{}' failed with exit status: {}", task_id_clone, s),
                                        Err(e) => warn!("Task '{}' execution error: {}", task_id_clone, e),
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to spawn task '{}': {}. Will not retry.", task_id_clone, e);
                                    let _ = tx_for_spawn.send(TaskResult::Failure(task_id_clone, 0)).await;
                                    return;
                                }
                            }

                            retries += 1;
                            if retries <= task_def.retries {
                                warn!("Retrying task '{}' ({}/{})", task_id_clone, retries, task_def.retries);
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            } else {
                                let _ = tx_for_spawn.send(TaskResult::Failure(task_id_clone, retries - 1)).await;
                                return;
                            }
                        }
                    });
                }
            }

            if finished_count >= total_tasks {
                break;
            }

            if let Some(result) = rx.recv().await {
                match result {
                    TaskResult::Success(id) => {
                        info!("Task '{}' completed successfully", id);
                        completed_tasks.insert(id.clone());
                        running_tasks.remove(&id);
                        finished_count += 1;
                    }
                    TaskResult::Failure(id, retries) => {
                        error!("Task '{}' failed after {} retries. Aborting pipeline.", id, retries);
                        return Err(anyhow!("Pipeline execution failed due to task '{}'", id));
                    }
                }
            } else {
                break;
            }
        }

        info!("Pipeline completed successfully: {}/{} tasks finished", finished_count, total_tasks);
        Ok(())
    }
}
