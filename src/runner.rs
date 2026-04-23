use crate::dag::Dag;
use crate::models::PipelineConfig;
use crate::engine::{FileEngine, TaskRunner};
use crate::cache::{CacheEngine, CacheMetadata, is_task_cacheable};
use anyhow::{anyhow, Result};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn, error};
use std::time::Duration;

#[derive(Debug)]
enum TaskResult {
    // Success(task_id, cache_key, content_hash)
    Success(String, String, Option<String>),
    Failure(String, u32),
}

pub struct Runner<F: FileEngine + 'static, T: TaskRunner + 'static> {
    config: PipelineConfig,
    dag: Dag,
    file_engine: Arc<F>,
    task_runner: Arc<T>,
    cache_path: String,
}

impl<F: FileEngine, T: TaskRunner> Runner<F, T> {
    pub fn new(config: PipelineConfig, dag: Dag, file_engine: F, task_runner: T) -> Self {
        Self { 
            config, 
            dag,
            file_engine: Arc::new(file_engine),
            task_runner: Arc::new(task_runner),
            cache_path: ".ferro_cache.json".to_string(),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut completed_tasks = HashSet::<String>::new();
        let mut running_tasks = HashSet::<String>::new();
        let mut cache_metadata = CacheMetadata::load(&self.cache_path).await;

        let (tx, mut rx) = mpsc::channel(100);
        let total_tasks = self.dag.total_tasks();
        let mut finished_count = 0;

        info!("Starting pipeline '{}' with cache support...", self.config.name);

        loop {
            let ready_tasks = self.dag.get_ready_tasks(&completed_tasks);

            for task_id in ready_tasks {
                if running_tasks.contains(&task_id) {
                    continue;
                }

                let task_def = self.config.tasks.get(&task_id).unwrap().clone();
                
                // --- GENERAR CLAVE ÚNICA DE CACHÉ ---
                // Usamos el nombre del pipeline y el id de la tarea
                let cache_key = CacheMetadata::generate_unique_key(&self.config.name, &task_id);

                let mut current_content_hash = None;
                if is_task_cacheable(&task_def) {
                    if let Ok(hash) = CacheEngine::generate_fingerprint(&task_def).await {
                        let outputs_exist = CacheEngine::verify_outputs_present(&task_def);
                        
                        // Buscamos por la clave compuesta (pipeline:tarea)
                        if let Some(old_hash) = cache_metadata.entries.get(&cache_key) {
                            if old_hash == &hash && outputs_exist {
                                info!("Task '{}' SKIPPED (Cache Hit: {})", task_id, cache_key);
                                completed_tasks.insert(task_id.clone());
                                finished_count += 1;
                                continue; 
                            }
                        }
                        current_content_hash = Some(hash);
                    }
                }

                // --- VALIDACIÓN DE INPUTS ---
                let mut missing = false;
                for (name, path) in &task_def.inputs {
                    if !self.file_engine.exists(path).await {
                        error!("Task '{}': input '{}' not found at '{}'", task_id, name, path);
                        missing = true;
                    }
                }

                if missing {
                    let _ = tx.try_send(TaskResult::Failure(task_id.clone(), 0));
                    continue;
                }

                // --- EJECUCIÓN ---
                running_tasks.insert(task_id.clone());
                let tx_for_spawn = tx.clone();
                let task_id_clone = task_id.clone();
                let cache_key_clone = cache_key.clone(); // Clonamos la clave para el hilo
                let task_runner = Arc::clone(&self.task_runner);

                info!("Launching task: {}", task_id);

                tokio::spawn(async move {
                    let mut retries = 0u32;
                    loop {
                        let exec_result = task_runner.execute(&task_id_clone, &task_def).await;

                        match exec_result {
                            Ok(_) => {
                                let _ = tx_for_spawn.send(TaskResult::Success(
                                    task_id_clone.clone(), 
                                    cache_key_clone, 
                                    current_content_hash
                                )).await;
                                return;
                            }
                            Err(e) => warn!("Task '{}' execution failed: {}", task_id_clone, e),
                        }

                        retries += 1;
                        if retries <= task_def.retries {
                            warn!("Retrying task '{}' ({}/{})", task_id_clone, retries, task_def.retries);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        } else {
                            let _ = tx_for_spawn.send(TaskResult::Failure(task_id_clone.clone(), retries - 1)).await;
                            return;
                        }
                    }
                });
            }

            if finished_count >= total_tasks {
                break;
            }

            // --- GESTIÓN DE RESULTADOS ---
            if !running_tasks.is_empty() {
                if let Some(result) = rx.recv().await {
                    match result {
                        TaskResult::Success(id, cache_key, new_hash) => {
                            info!("Task '{}' completed successfully", id);
                            
                            // Guardamos usando la clave específica que generamos al inicio
                            if let Some(hash) = new_hash {
                                cache_metadata.entries.insert(cache_key, hash);
                            }

                            completed_tasks.insert(id.clone());
                            running_tasks.remove(&id);
                            finished_count += 1;
                        }
                        TaskResult::Failure(id, _retries) => {
                            error!("Task '{}' failed. Aborting.", id);
                            let _ = cache_metadata.save(&self.cache_path).await;
                            return Err(anyhow!("Pipeline execution failed due to task '{}'", id));
                        }
                    }
                }
            } else {
                tokio::task::yield_now().await;
                if self.dag.get_ready_tasks(&completed_tasks).is_empty() {
                     return Err(anyhow!("Pipeline deadlock: no tasks running and no tasks ready."));
                }
            }
        }

        cache_metadata.save(&self.cache_path).await?;
        info!("Pipeline '{}' finished successfully", self.config.name);
        Ok(())
    }
}