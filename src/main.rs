mod models;
mod dag;
mod engine;
mod runner;
mod cache;

use clap::Parser;
use tracing::{info, error};

use crate::models::PipelineConfig;
use crate::dag::Dag;
use crate::runner::Runner;
use crate::engine::{OsFileEngine, ProcessTaskRunner};

#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(short, long, default_value = "pipeline.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!("Loading configuration from {}", args.config);
    let config_content = tokio::fs::read_to_string(&args.config)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read config file '{}': {}", args.config, e))?;
    let config: PipelineConfig = serde_yaml::from_str(&config_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse YAML: {}", e))?;

    info!("Building DAG and validating...");
    let dag = Dag::new(&config)?;
    info!("DAG validated. Found {} tasks.", dag.total_tasks());

    let runner = Runner::new(config, dag, OsFileEngine, ProcessTaskRunner);

    info!("Starting pipeline execution...");
    let result = runner.run().await;

    info!("Cleaning up...");

    match result {
        Ok(_) => {
            info!("Pipeline finished successfully!");
            Ok(())
        }
        Err(e) => {
            error!("Pipeline failed: {}", e);
            Err(e)
        }
    }
}
