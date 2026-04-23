use sha2::{Sha256, Digest};
use tokio::fs::File;
use tokio::io::{AsyncReadExt};
use crate::models::TaskDefinition;
use anyhow::{Context, Result};
use std::collections::HashMap;

pub struct CacheEngine;

impl CacheEngine {
    /// Generates the content hash based on: Command + Script + Inputs
    pub async fn generate_fingerprint(task_def: &TaskDefinition) -> Result<String> {
        let mut hasher = Sha256::new();

        // 1. Hash of the command
        hasher.update(task_def.cmd.as_bytes());

        // 2. Hash of the executable script (if it exists locally)
        if let Some(executable) = task_def.cmd.split_whitespace().next() {
            if std::path::Path::new(executable).exists() {
                Self::hash_file(&mut hasher, executable).await?;
            }
        }

        // 3. Hash of the inputs (ordered by key for determinism)
        let mut keys: Vec<_> = task_def.inputs.keys().collect();
        keys.sort();
        for key in keys {
            let path = &task_def.inputs[key];
            Self::hash_file(&mut hasher, path)
                .await
                .with_context(|| format!("Error hashing input '{}' at path '{}'", key, path))?;
        }

        Ok(hex::encode(hasher.finalize()))
    }

    async fn hash_file(hasher: &mut Sha256, path: &str) -> Result<()> {
        let mut file = File::open(path).await?;
        let mut buffer = [0u8; 8192];
        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 { break; }
            hasher.update(&buffer[..n]);
        }
        Ok(())
    }

    /// Verifies if all declared outputs exist in the file system
    pub fn verify_outputs_present(task_def: &TaskDefinition) -> bool {
        if task_def.outputs.is_empty() { return false; }
        task_def.outputs.values().all(|p| std::path::Path::new(p).exists())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_generate_fingerprint_determinism() -> Result<()> {
        // Crear un archivo temporal para simular un input
        let mut file = NamedTempFile::new()?;
        writeln!(file, "some input data")?;
        let path = file.path().to_str().unwrap().to_string();

        let mut inputs = HashMap::new();
        inputs.insert("input1".to_string(), path.clone());

        let task = TaskDefinition {
            cmd: "echo hello".to_string(),
            inputs,
            outputs: HashMap::new(),
            deps: vec![],
            retries: 0,
            timeout_secs: None,
        };

        let hash1 = CacheEngine::generate_fingerprint(&task).await?;
        let hash2 = CacheEngine::generate_fingerprint(&task).await?;

        // El hash debe ser el mismo si nada cambia
        assert_eq!(hash1, hash2);
        Ok(())
    }

    #[tokio::test]
    async fn test_fingerprint_changes_on_input_content_change() -> Result<()> {
        let mut file = NamedTempFile::new()?;
        let path = file.path().to_str().unwrap().to_string();

        let mut inputs = HashMap::new();
        inputs.insert("input1".to_string(), path.clone());

        let task = TaskDefinition {
            cmd: "echo hello".to_string(),
            inputs,
            outputs: HashMap::new(),
            deps: vec![],
            retries: 0,
            timeout_secs: None,
        };

        // Escribir versión 1
        writeln!(file, "data v1")?;
        let hash1 = CacheEngine::generate_fingerprint(&task).await?;

        // Escribir versión 2 (sobre el mismo archivo/path)
        let mut file_rewrite = std::fs::File::create(&path)?;
        writeln!(file_rewrite, "data v2")?;
        let hash2 = CacheEngine::generate_fingerprint(&task).await?;

        assert_ne!(hash1, hash2, "Hash should change when file content changes");
        Ok(())
    }

    #[test]
    fn test_verify_outputs_present() -> Result<()> {
        let file = NamedTempFile::new()?;
        let path = file.path().to_str().unwrap().to_string();

        let mut outputs = HashMap::new();
        outputs.insert("out".to_string(), path);

        let mut task = TaskDefinition {
            cmd: "echo".to_string(),
            inputs: HashMap::new(),
            outputs,
            deps: vec![],
            retries: 0,
            timeout_secs: None,
        };

        // El archivo existe (gracias a NamedTempFile)
        assert!(CacheEngine::verify_outputs_present(&task));

        // Cambiamos a un path que no existe
        task.outputs.insert("out".to_string(), "non_existent_file.txt".to_string());
        assert!(!CacheEngine::verify_outputs_present(&task));

        Ok(())
    }
}