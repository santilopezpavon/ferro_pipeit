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
                if running_tasks.contains(&task_id) {
                    continue;
                }

                let task_def = self.config.tasks.get(&task_id).unwrap().clone();

                // validar inputs
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
                        // 🔥 parse simple: bin + args
                        let mut parts = task_def.cmd.split_whitespace();
                        let program = match parts.next() {
                            Some(p) => p,
                            None => {
                                let _ = tx_for_spawn.send(
                                    TaskResult::Failure(task_id_clone.clone(), retries)
                                ).await;
                                return;
                            }
                        };

                        let args: Vec<&str> = parts.collect();

                        let mut cmd = Command::new(program);
                        cmd.args(args);
                        cmd.env("PIPEIT_TASK_NAME_ID", &task_id_clone);

                        for (name, path) in &task_def.inputs {
                            cmd.env(format!("PIPEIT_IN_{}", name.to_uppercase()), path);
                        }

                        for (name, path) in &task_def.outputs {
                            cmd.env(format!("PIPEIT_OUT_{}", name.to_uppercase()), path);
                        }

                        match cmd.spawn() {
                            Ok(mut child) => {
                                let status = if let Some(t) = task_def.timeout_secs {
                                    match tokio::time::timeout(
                                        Duration::from_secs(t),
                                        child.wait(),
                                    )
                                    .await
                                    {
                                        Ok(res) => res,
                                        Err(_) => {
                                            let _ = child.kill().await;
                                            warn!(
                                                "Task '{}' timed out after {}s",
                                                task_id_clone, t
                                            );
                                            Err(std::io::Error::new(
                                                std::io::ErrorKind::TimedOut,
                                                "timeout",
                                            ))
                                        }
                                    }
                                } else {
                                    child.wait().await
                                };

                                match status {
                                    Ok(s) if s.success() => {
                                        let _ = tx_for_spawn
                                            .send(TaskResult::Success(task_id_clone.clone()))
                                            .await;
                                        return;
                                    }
                                    Ok(s) => {
                                        warn!(
                                            "Task '{}' failed with status {}",
                                            task_id_clone, s
                                        );
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Task '{}' execution error: {}",
                                            task_id_clone, e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to spawn task '{}': {}",
                                    task_id_clone, e
                                );
                                let _ = tx_for_spawn
                                    .send(TaskResult::Failure(task_id_clone.clone(), retries))
                                    .await;
                                return;
                            }
                        }

                        retries += 1;

                        if retries <= task_def.retries {
                            warn!(
                                "Retrying task '{}' ({}/{})",
                                task_id_clone, retries, task_def.retries
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        } else {
                            let _ = tx_for_spawn
                                .send(TaskResult::Failure(task_id_clone.clone(), retries - 1))
                                .await;
                            return;
                        }
                    }
                });
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
                        error!(
                            "Task '{}' failed after {} retries. Aborting pipeline.",
                            id, retries
                        );
                        return Err(anyhow!(
                            "Pipeline execution failed due to task '{}'",
                            id
                        ));
                    }
                }
            } else {
                break;
            }
        }

        info!(
            "Pipeline completed successfully: {}/{} tasks finished",
            finished_count, total_tasks
        );

        Ok(())
    }
}