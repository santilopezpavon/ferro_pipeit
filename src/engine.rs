use async_trait::async_trait;
use crate::models::TaskDefinition;
use anyhow::{anyhow, Result};
use std::time::Duration;
use tokio::process::Command;
use tracing::{error, warn};

#[async_trait]
pub trait FileEngine: Send + Sync {
    async fn exists(&self, path: &str) -> bool;
}

pub struct OsFileEngine;

#[async_trait]
impl FileEngine for OsFileEngine {
    async fn exists(&self, path: &str) -> bool {
        std::path::Path::new(path).exists()
    }
}

#[async_trait]
pub trait TaskRunner: Send + Sync {
    async fn execute(&self, task_id: &str, task_def: &TaskDefinition) -> Result<()>;
}

pub struct ProcessTaskRunner;

#[async_trait]
impl TaskRunner for ProcessTaskRunner {
    async fn execute(&self, task_id: &str, task_def: &TaskDefinition) -> Result<()> {
        let mut parts = task_def.cmd.split_whitespace();
        let program = match parts.next() {
            Some(p) => p,
            None => return Err(anyhow!("Empty command for task '{}'", task_id)),
        };
        let args: Vec<&str> = parts.collect();

        let mut cmd = Command::new(program);
        cmd.args(args);
        cmd.env("PIPEIT_TASK_NAME_ID", task_id);

        for (name, path) in &task_def.inputs {
            cmd.env(format!("PIPEIT_IN_{}", name.to_uppercase()), path);
        }

        for (name, path) in &task_def.outputs {
            cmd.env(format!("PIPEIT_OUT_{}", name.to_uppercase()), path);
        }

        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                error!("Failed to spawn task '{}': {}", task_id, e);
                return Err(anyhow!("Failed to spawn task '{}': {}", task_id, e));
            }
        };

        let status = if let Some(t) = task_def.timeout_secs {
            match tokio::time::timeout(Duration::from_secs(t), child.wait()).await {
                Ok(res) => res,
                Err(_) => {
                    let _ = child.kill().await;
                    warn!("Task '{}' timed out after {}s", task_id, t);
                    return Err(anyhow!("timeout"));
                }
            }
        } else {
            child.wait().await
        };

        match status {
            Ok(s) if s.success() => Ok(()),
            Ok(s) => {
                warn!("Task '{}' failed with status {}", task_id, s);
                Err(anyhow!("Task failed with exit code: {}", s))
            }
            Err(e) => {
                warn!("Task '{}' execution error: {}", task_id, e);
                Err(anyhow!("Task execution error: {}", e))
            }
        }
    }
}
