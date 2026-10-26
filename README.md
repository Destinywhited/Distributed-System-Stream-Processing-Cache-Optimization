Distributed System Stream Processing & Cache Optimization

Overview

This project implements and evaluates three core components:

- Hierarchical caching: In-memory cache operations, TTL support, invalidation, and cache statistics.
- Partition rebalancing: Deterministic partition assignment using rendezvous hashing.
- Event streaming: Event validation, duplicate detection, bounded retries, dead-letter handling, and JSONL audit export.

Project Structure

Distributed-System-Stream-Processing-Cache-Optimization/
├── src/
│   └── distributed_cache/
│       ├── cache.py
│       ├── partitioning.py
│       └── streaming.py
├── tests/
│   ├── test_cache.py
│   ├── test_partitioning.py
│   └── test_streaming.py
├── scripts/
│   ├── run_evaluation.py
│   └── run_benchmarks.py
├── reports/
│   ├── evaluation_summary.json
│   ├── evaluation_findings.md
│   ├── benchmark_results.json
│   └── benchmark_results.csv
└── output/
    └── annotated_event_log.jsonl

Requirements

- Python 3.10 or newer
- No third-party Python dependencies are required for the current implementation.

Run Tests

From the project root in PowerShell:

$env:PYTHONPATH = "src"
python -m unittest discover -s tests -v

Run Functional Evaluation

python scripts/run_evaluation.py

The evaluation script writes a machine-readable summary, a findings report, and an annotated JSON Lines event log.

Run Benchmarks

python scripts/run_benchmarks.py

The benchmark script records local execution-time and throughput measurements in JSON and CSV reports.

Implementation Notes and Limitations

- The current cache implementation uses in-memory storage; a live Redis integration has not been established by these checks.
- Partition assignment is deterministic for the same partition count and worker set.
- Event idempotency state is in memory and does not persist across process restarts.
- The benchmark results represent the local execution environment and should not be interpreted as production latency guarantees.
- Distributed deployment, live Redis integration, and production service-level agreement compliance require separate validation.

Verification

The project has been tested with Python's "unittest" framework. Run the test and evaluation commands above to reproduce the results on your environment.