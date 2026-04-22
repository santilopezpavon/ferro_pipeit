use crate::dag::Dag;
use crate::models::PipelineConfig;
use crate::engine::{FileEngine, TaskRunner};
use anyhow::{anyhow, Result};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn, error};
use std::time::Duration;

#[derive(Debug)]
enum TaskResult {
    Success(String),
    Failure(String, u32),
}

pub struct Runner<F: FileEngine + 'static, T: TaskRunner + 'static> {
    config: PipelineConfig,
    dag: Dag,
    file_engine: Arc<F>,
    task_runner: Arc<T>,
}

impl<F: FileEngine, T: TaskRunner> Runner<F, T> {
    pub fn new(config: PipelineConfig, dag: Dag, file_engine: F, task_runner: T) -> Self {
        Self { 
            config, 
            dag,
            file_engine: Arc::new(file_engine),
            task_runner: Arc::new(task_runner),
        }
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
                    if !self.file_engine.exists(path).await {
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
                let task_runner = Arc::clone(&self.task_runner);

                info!("Launching task: {}", task_id);

                tokio::spawn(async move {
                    let mut retries = 0u32;

                    loop {
                        let exec_result = task_runner.execute(&task_id_clone, &task_def).await;

                        match exec_result {
                            Ok(_) => {
                                let _ = tx_for_spawn.send(TaskResult::Success(task_id_clone.clone())).await;
                                return;
                            }
                            Err(e) => {
                                warn!("Task execution failed: {}", e);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{PipelineConfig, TaskDefinition};
    use crate::engine::{FileEngine, TaskRunner};
    use crate::dag::Dag;
    use async_trait::async_trait;
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;
    use anyhow::Result;

    #[derive(Default)]
    struct MockFileEngine {
        existing_files: HashSet<String>,
    }

    #[async_trait]
    impl FileEngine for MockFileEngine {
        async fn exists(&self, path: &str) -> bool {
            self.existing_files.contains(path)
        }
    }

    struct MockTaskRunner {
        failing_tasks: Mutex<HashMap<String, u32>>, // task_id -> remaining failures
    }

    impl MockTaskRunner {
        fn new(failing_tasks: HashMap<String, u32>) -> Self {
            Self {
                failing_tasks: Mutex::new(failing_tasks),
            }
        }
    }

    #[async_trait]
    impl TaskRunner for MockTaskRunner {
        async fn execute(&self, task_id: &str, _task_def: &TaskDefinition) -> Result<()> {
            let mut fails = self.failing_tasks.lock().unwrap();
            if let Some(remaining) = fails.get_mut(task_id) {
                if *remaining > 0 {
                    *remaining -= 1;
                    return Err(anyhow::anyhow!("Mock error for task {}", task_id));
                }
            }
            Ok(())
        }
    }

    fn create_test_config() -> PipelineConfig {
        let mut tasks = HashMap::new();
        tasks.insert("A".to_string(), TaskDefinition {
            cmd: "echo A".to_string(),
            deps: vec![],
            retries: 2,
            timeout_secs: Some(1),
            inputs: HashMap::new(),
            outputs: HashMap::new(),
        });
        tasks.insert("B".to_string(), TaskDefinition {
            cmd: "echo B".to_string(),
            deps: vec!["A".to_string()],
            retries: 1,
            timeout_secs: Some(1),
            inputs: HashMap::new(),
            outputs: HashMap::new(),
        });
        PipelineConfig { tasks }
    }

    #[tokio::test]
    async fn test_pipeline_deterministic_execution() {
        let config = create_test_config();
        let dag = Dag::new(&config).unwrap();
        let file_engine = MockFileEngine::default();
        let task_runner = MockTaskRunner::new(HashMap::new());

        let runner = Runner::new(config, dag, file_engine, task_runner);
        let result = runner.run().await;
        
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_halts_on_failure() {
        let config = create_test_config();
        let dag = Dag::new(&config).unwrap();
        let file_engine = MockFileEngine::default();
        
        // Task A will fail 3 times. Retries configured is 2, so it will ultimately fail.
        let mut fails = HashMap::new();
        fails.insert("A".to_string(), 3);
        let task_runner = MockTaskRunner::new(fails);

        let runner = Runner::new(config, dag, file_engine, task_runner);
        let result = runner.run().await;
        
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Pipeline execution failed due to task 'A'");
    }

    #[tokio::test]
    async fn test_pipeline_missing_inputs() {
        let mut config = create_test_config();
        let mut inputs = HashMap::new();
        inputs.insert("file_a".to_string(), "/missing/file".to_string());
        config.tasks.get_mut("A").unwrap().inputs = inputs;

        let dag = Dag::new(&config).unwrap();
        // File engine is empty, so /missing/file will not exist
        let file_engine = MockFileEngine::default();
        let task_runner = MockTaskRunner::new(HashMap::new());

        let runner = Runner::new(config, dag, file_engine, task_runner);
        let result = runner.run().await;
        
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Pipeline execution failed due to task 'A'");
    }
}