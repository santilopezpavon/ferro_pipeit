use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::fs;

#[derive(Default, Serialize, Deserialize)]
pub struct CacheMetadata {
    pub entries: HashMap<String, String>,
}

impl CacheMetadata {
    pub async fn load(path: &str) -> Self {
        let data = fs::read_to_string(path).await;
        match data {
            Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    pub async fn save(&self, path: &str) -> anyhow::Result<()> {
        let content = serde_json::to_string_pretty(self)?;
        fs::write(path, content).await?;
        Ok(())
    }

    pub fn generate_unique_key(pipeline_name: &str, task_id: &str) -> String {
        format!("{}:{}", pipeline_name, task_id)
    }
}