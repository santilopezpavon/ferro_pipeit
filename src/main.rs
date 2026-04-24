mod models;
mod dag;
mod engine;
mod runner;
mod cache;

use clap::Parser;
use tracing::{info, error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, fmt, EnvFilter};
use tracing_appender::rolling;
use std::path::Path;

use crate::models::PipelineConfig;
use crate::dag::Dag;
use crate::runner::Runner;
use crate::engine::{OsFileEngine, ProcessTaskRunner};

#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(short, long, default_value = "pipeline.yaml")]
    config: String,

    /// Optional file to save logs
    #[arg(short, long)]
    log_file: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Set up logging
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let stdout_layer = fmt::layer();

    if let Some(path) = &args.log_file {
        let p = Path::new(path);
        let dir = p.parent().unwrap_or_else(|| Path::new("."));
        let dir = if dir.as_os_str().is_empty() { Path::new(".") } else { dir };
        let file_name = p.file_name().unwrap_or_else(|| std::ffi::OsStr::new("ferro_pipeit.log"));
        let file_appender = rolling::never(dir, file_name);
        
        let file_layer = fmt::layer()
            .with_writer(file_appender)
            .with_ansi(false); // Disable colors in file
            
        tracing_subscriber::registry()
            .with(filter)
            .with(stdout_layer)
            .with(file_layer)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(stdout_layer)
            .init();
    }

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
