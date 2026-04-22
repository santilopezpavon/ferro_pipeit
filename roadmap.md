# Project Roadmap

This document outlines the planned evolution of the project, including upcoming features, improvements, and long‑term goals.
It is a living document and will be updated as the project progresses.

---

## Completed
- [x] Project initialization
- [x] Git repository setup
- [x] Basic pipeline functionality
- [x] Two base examples
---

## In Progress

---

## Planned Features

### Core Application Features
- [x] F001 Integrated Testing Suite & Reproducibility. Ensure your pipeline remains rock-solid as it grows. Ferro now includes a dedicated testing framework to mock file inputs, simulate task failures, and verify DAG logic, ensuring every change is safe and every pipeline execution is predictable.
- [ ] Smart Caching (CAS & Merkle Trees). Skip redundant work by identifying data by its content, not just its filename. If your inputs haven't changed, Ferro won't waste time re-running the task—it will instantly link the previous result.
- [ ] Inter-Task State Transfer: Seamlessly pass variables and small metadata between tasks. A script can export key-value pairs that become available as environment variables for all downstream dependent tasks, enabling dynamic workflows.
- [ ] Persistent Execution Logs: Keep a detailed history of every pipeline run. Ferro now automatically captures all terminal output and task statuses into structured log files, allowing for post-mortem analysis and long-term auditing of your data workflows.
- [ ] Hardened Task Isolation (Sandboxing): Guarantee "Data Lineage" by running scripts in a restricted environment. Tasks can only see the files they explicitly declared as inputs, preventing hidden dependencies and "it works on my machine" errors.
- [ ] Deep Data Contract Validation (Arrow/Parquet): Move beyond checking if a file exists. Ferro can now peek inside your data to ensure it follows the correct schema (columns, types, and nulls) before a single line of your script even runs.
- [ ] Scalable Remote Execution (gRPC & Zero-Copy): Run your pipeline across a cluster of machines. Ferro can delegate heavy tasks to remote workers and move data efficiently using high-performance networking, allowing you to scale beyond a single computer.



---

## Notes
This roadmap is subject to change based on project priorities, user feedback, and development constraints.
