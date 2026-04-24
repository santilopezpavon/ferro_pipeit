# Ferro Pipeit

A lightweight, data-lineage-aware pipeline orchestrator written in Rust. It executes scripts defined in a YAML file, respecting dependencies between tasks and validating that every declared input file exists on disk before a task is allowed to run.

---

## Overview

Ferro Pipeit reads a `pipeline.yaml` file, builds a Directed Acyclic Graph (DAG) from the task definitions, and executes tasks as soon as their dependencies complete. Tasks that have no dependencies between them run in parallel.

State is not passed through environment variables or shared memory. Instead, each task declares the files it reads (`inputs`) and the files it produces (`outputs`). The orchestrator automatically injects the resolved file paths as environment variables and enforces that all declared inputs exist before a task starts.

---

## How It Works

### 1. DAG Validation

At startup, the orchestrator parses the pipeline YAML and constructs a dependency graph using `petgraph`. It checks that:

- All referenced task names exist.
- There are no cycles in the dependency graph.

If either check fails, the process exits with an error before any task is launched.

### 2. Data Lineage

Each task declares its data contract in the YAML:

```yaml
process:
  cmd: "python3 scripts/process.py"
  deps: ["init"]
  inputs:
    raw_data: "data/raw.csv"
  outputs:
    report:   "data/report.txt"
```

Before launching `process`, the orchestrator:

1. Verifies that `data/raw.csv` exists on disk. If it does not, the task fails immediately without executing its command.
2. Injects environment variables for every declared input and output:
   - `IN_RAW_DATA=data/raw.csv`
   - `OUT_REPORT=data/report.txt`

Scripts consume these variables directly. They never need to know the actual paths at write time.

### 3. Parallel Execution

The orchestrator continuously checks which tasks have all their dependencies marked as complete and launches them as soon as they are ready, using Tokio async tasks. Tasks with no shared dependencies run concurrently.

### 4. Retries and Timeouts

Each task supports an optional retry count and an optional timeout:

```yaml
process:
  cmd: "python3 scripts/process.py"
  retries: 2
  timeout_secs: 60
```

If a task exits with a non-zero status, it is retried up to `retries` times with a one-second delay between attempts. If `timeout_secs` is set and the task exceeds that duration, the child process is killed and the attempt is counted as a failure.

---

## Pipeline YAML Format

```yaml
name: <pipeline-name> # A unique name for this pipeline
tasks:
  <task-name>:
    cmd: "<shell command to run>"
    deps: ["<dependency-task-name>"]   # optional, list of tasks that must complete first
    retries: 0                          # optional, defaults to 0
    timeout_secs: 30                    # optional, no timeout if omitted
    inputs:
      <logical-name>: "<file path>"     # optional
    outputs:
      <logical-name>: "<file path>"     # optional
```

### Rules

- `deps` lists task names that must complete successfully before this task starts.
- `inputs` declares files this task reads. The orchestrator validates their existence before running the task.
- `outputs` declares files this task is expected to produce. The orchestrator injects their paths but does not validate them after the task runs.
- A task with no `deps` starts as soon as the pipeline begins.

---

## Environment Variables Injected into Scripts

For each `inputs` entry, the orchestrator injects:

```
IN_<NAME_IN_UPPERCASE>=<path>
```

For each `outputs` entry:

```
OUT_<NAME_IN_UPPERCASE>=<path>
```

Additionally, every task receives:

```
TASK_NAME=<task-name>
```

### Example

Given this declaration:

```yaml
inputs:
  raw_data: "data/raw.csv"
outputs:
  report:   "data/report.txt"
```

The script receives:

```
IN_RAW_DATA=data/raw.csv
OUT_REPORT=data/report.txt
TASK_NAME=process
```

Scripts in any language can consume these variables:

**Bash:**
```bash
cat "$IN_RAW_DATA" | wc -l > "$OUT_REPORT"
```

**Python:**
```python
import os
with open(os.environ["IN_RAW_DATA"]) as f:
    data = f.read()
with open(os.environ["OUT_REPORT"], "w") as f:
    f.write(data)
```

**Node.js:**
```js
const fs = require('fs');
const data = fs.readFileSync(process.env.IN_RAW_DATA, 'utf8');
fs.writeFileSync(process.env.OUT_REPORT, data);
```

---

## Getting Started

### Requirements

- Rust (stable, via [rustup](https://rustup.rs))
- The runtime dependencies of your scripts (Python, Node.js, bash, etc.)

### Build

```bash
cargo build --release
```

The compiled binary is placed at `target/release/ferro_pipeit`.

### Run

```bash
cargo run -- --config pipeline.yaml
cargo run -- -c examples/01/pipeline.yaml
cargo run -- -c examples/02/pipeline.yaml
cargo run -- -c examples/03/pipeline.yaml
```

Or with the release binary:

```bash
./target/release/ferro_pipeit --config pipeline.yaml
./target/release/ferro_pipeit -c examples/01/pipeline.yaml
./target/release/ferro_pipeit -c examples/02/pipeline.yaml
./target/release/ferro_pipeit -c examples/03/pipeline.yaml
```

### CLI Options

```
Usage: ferro_pipeit [OPTIONS]

Options:
  -c, --config <CONFIG>  Path to the pipeline YAML file [default: pipeline.yaml]
  -h, --help             Print help
  -V, --version          Print version
```

```bash
# Run the pipeline
cargo run -- --config examples/01/pipeline.yaml
```

---

## Testing & Reproducibility Architecture

Ferro Pipeit is designed with a 100% testable architecture via Dependency Injection. It isolates real system interactions behind abstract traits. 

The primary traits, located in `src/engine.rs`, are:
- **`FileEngine`**: Abstracts file system access. Its primary method `exists(path)` enables checking if inputs are valid. The production system utilizes `OsFileEngine` while the test suite simply injects an in-memory `MockFileEngine`.
- **`TaskRunner`**: Abstracts the execution of shell commands. The production system utilizes `ProcessTaskRunner` (spawning Tokio subprocesses), while tests use a `MockTaskRunner` to securely simulate successes, failures, and timeouts without invoking side effects on the host OS.

By composing the `Runner` over `<F: FileEngine, T: TaskRunner>`, the entire pipeline orchestration logic—DAG traversal, strict input validations, retries, and halts on failures—can be tested deterministically and completely decoupled from varying OS environments.

---

## Project Structure

```
ferro_pipeit/
  src/
    main.rs       Entry point. Parses CLI args, loads config, runs the pipeline.
    models.rs     Data structures: TaskDefinition, PipelineConfig.
    dag.rs        DAG construction and dependency resolution using petgraph.
    engine.rs     System abstraction traits (`FileEngine`, `TaskRunner`) and mock capabilities.
    runner.rs     Task execution, lineage validation, retry and timeout logic using Dependency Injection.
  pipeline.yaml   Pipeline definition file.
  scripts/        Example scripts consumed by the pipeline.
  Cargo.toml      Rust dependencies.
```

---

## Dependencies

| Crate              | Purpose                                |
|--------------------|----------------------------------------|
| tokio              | Async runtime and process spawning     |
| petgraph           | DAG construction and cycle detection   |
| serde + serde_yaml | YAML parsing                           |
| clap               | CLI argument parsing                   |
| anyhow             | Error propagation                      |
| tracing            | Structured logging                     |
| async-trait        | Asynchronous trait abstraction         |
