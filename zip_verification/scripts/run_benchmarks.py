"""Benchmark cache, partition rebalancing, and event processing."""

import csv
import json
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from distributed_cache.cache import HierarchicalCache
from distributed_cache.partitioning import PartitionRebalancer
from distributed_cache.streaming import EventProcessor


REPORTS_DIR = PROJECT_ROOT / "reports"
BENCHMARK_ITERATIONS = 1000


def percentile(values, percent):
    """Return a nearest-rank percentile in milliseconds."""
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1,
                       int((percent / 100) * len(ordered) + 0.999999) - 1))
    return ordered[index] * 1000


def measure(operation, iterations=BENCHMARK_ITERATIONS):
    """Measure individual operation latency using a monotonic clock."""
    samples = []

    for _ in range(iterations):
        start = time.perf_counter()
        operation()
        samples.append(time.perf_counter() - start)

    total_seconds = sum(samples)

    return {
        "iterations": iterations,
        "total_time_ms": round(total_seconds * 1000, 6),
        "throughput_ops_per_second": (
            round(iterations / total_seconds, 2)
            if total_seconds > 0 else None
        ),
        "latency_ms": {
            "min": round(min(samples) * 1000, 6),
            "mean": round(statistics.mean(samples) * 1000, 6),
            "p50": round(percentile(samples, 50), 6),
            "p95": round(percentile(samples, 95), 6),
            "p99": round(percentile(samples, 99), 6),
            "max": round(max(samples) * 1000, 6),
        },
    }


def main():
    REPORTS_DIR.mkdir(exist_ok=True)
    results = []

    # Cache: warm reads of a pre-populated key.
    cache = HierarchicalCache()
    cache.set("benchmark:key", {"value": 123})

    results.append({
        "benchmark": "cache_warm_read",
        "metrics": measure(lambda: cache.get("benchmark:key")),
    })

    # Cache: writes to a single key.
    write_counter = {"value": 0}

    def cache_write():
        write_counter["value"] += 1
        cache.set("benchmark:write", write_counter["value"])

    results.append({
        "benchmark": "cache_write",
        "metrics": measure(cache_write),
    })

    # Partition assignment: 1000 partitions and three workers.
    rebalancer = PartitionRebalancer(partition_count=1000)
    workers = ["worker-a", "worker-b", "worker-c"]

    results.append({
        "benchmark": "partition_rebalance_1000",
        "metrics": measure(lambda: rebalancer.rebalance(workers), iterations=20),
    })

    # Event processing: a unique event for every iteration.
    event_counter = {"value": 0}

    def event_handler(event):
        return event.payload.get("value")

    processor = EventProcessor(handler=event_handler, max_attempts=1)

    def process_event():
        event_counter["value"] += 1
        event_id = f"bench-{event_counter['value']}"
        processor.process({
            "event_id": event_id,
            "partition": event_counter["value"] % 16,
            "timestamp": time.time(),
            "payload": {"value": event_counter["value"]},
        })

    results.append({
        "benchmark": "event_processing",
        "metrics": measure(process_event),
    })

    output = {
        "project": "Distributed System Stream Processing & Cache Optimization",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "python_version": sys.version.split()[0],
            "platform": sys.platform,
            "iterations_per_standard_benchmark": BENCHMARK_ITERATIONS,
        },
        "results": results,
        "limitations": [
            "Measurements describe this local run and machine only.",
            "The cache benchmark uses in-memory storage, not a Redis server.",
            "These microbenchmarks do not establish production SLA compliance.",
        ],
    }

    json_path = REPORTS_DIR / "benchmark_results.json"
    json_path.write_text(
        json.dumps(output, indent=2),
        encoding="utf-8",
    )

    csv_path = REPORTS_DIR / "benchmark_results.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "benchmark",
                "iterations",
                "total_time_ms",
                "throughput_ops_per_second",
                "min_latency_ms",
                "mean_latency_ms",
                "p50_latency_ms",
                "p95_latency_ms",
                "p99_latency_ms",
                "max_latency_ms",
            ],
        )
        writer.writeheader()

        for result in results:
            metrics = result["metrics"]
            latency = metrics["latency_ms"]
            writer.writerow({
                "benchmark": result["benchmark"],
                "iterations": metrics["iterations"],
                "total_time_ms": metrics["total_time_ms"],
                "throughput_ops_per_second":
                    metrics["throughput_ops_per_second"],
                "min_latency_ms": latency["min"],
                "mean_latency_ms": latency["mean"],
                "p50_latency_ms": latency["p50"],
                "p95_latency_ms": latency["p95"],
                "p99_latency_ms": latency["p99"],
                "max_latency_ms": latency["max"],
            })

    print(json.dumps({
        "status": "completed",
        "json_report": str(json_path.relative_to(PROJECT_ROOT)),
        "csv_report": str(csv_path.relative_to(PROJECT_ROOT)),
        "benchmarks": [
            {
                "name": result["benchmark"],
                "iterations": result["metrics"]["iterations"],
                "p95_latency_ms":
                    result["metrics"]["latency_ms"]["p95"],
            }
            for result in results
        ],
    }, indent=2))


if __name__ == "__main__":
    main()