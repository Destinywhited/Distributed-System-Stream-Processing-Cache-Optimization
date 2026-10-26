"""Run functional evaluations and save reproducible evidence."""

import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from distributed_cache.cache import HierarchicalCache
from distributed_cache.partitioning import PartitionRebalancer
from distributed_cache.streaming import EventProcessor


REPORTS_DIR = PROJECT_ROOT / "reports"
OUTPUT_DIR = PROJECT_ROOT / "output"


def main():
    REPORTS_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)

    checks = []
    event_log = []

    def record_check(name, passed, details):
        checks.append({
            "name": name,
            "status": "PASS" if passed else "FAIL",
            "details": details,
        })

    # 1. Cache write/read and invalidation.
    cache = HierarchicalCache()
    cache.set("evaluation:user:1", {"name": "Alex"})
    cache_read = cache.get("evaluation:user:1")
    cache.invalidate("evaluation:user:1")
    cache_after_invalidation = cache.get("evaluation:user:1")

    cache_ok = (
        cache_read == {"name": "Alex"}
        and cache_after_invalidation is None
        and cache.stats().invalidations == 1
    )
    record_check(
        "cache_write_read_invalidation",
        cache_ok,
        {
            "read_value": cache_read,
            "value_after_invalidation": cache_after_invalidation,
            "invalidation_count": cache.stats().invalidations,
        },
    )

    # 2. Partition assignment and rebalancing.
    rebalancer = PartitionRebalancer(partition_count=100)
    initial_plan = rebalancer.rebalance(["worker-a", "worker-b"])
    expanded_plan = rebalancer.rebalance(
        ["worker-a", "worker-b", "worker-c"]
    )
    partition_ok = (
        len(expanded_plan.assignments) == 100
        and set(expanded_plan.assignments.values())
        <= {"worker-a", "worker-b", "worker-c"}
    )
    record_check(
        "partition_assignment_and_rebalance",
        partition_ok,
        {
            "partition_count": len(expanded_plan.assignments),
            "workers_before": initial_plan.new_owner_count,
            "workers_after": expanded_plan.new_owner_count,
            "moved_partition_count": len(expanded_plan.moved_partitions),
        },
    )

    # 3. Event processing, duplicate detection, and invalid input.
    processor = EventProcessor(
        handler=lambda event: {
            "event_id": event.event_id,
            "action": event.payload.get("action"),
        },
        max_attempts=2,
        retry_delay_seconds=0,
    )

    valid_event = {
        "event_id": "eval-001",
        "partition": 0,
        "timestamp": 1000,
        "payload": {"action": "purchase"},
    }
    first_result = processor.process(valid_event)
    duplicate_result = processor.process(valid_event)
    invalid_result = processor.process({
        "event_id": "",
        "partition": 0,
        "timestamp": 1001,
        "payload": {},
    })

    stream_ok = (
        first_result.status == "processed"
        and duplicate_result.status == "duplicate"
        and invalid_result.status == "invalid"
    )
    record_check(
        "event_processing_deduplication_validation",
        stream_ok,
        {
            "first_status": first_result.status,
            "duplicate_status": duplicate_result.status,
            "invalid_status": invalid_result.status,
            "metrics": processor.metrics,
        },
    )

    # Export audit events as JSON Lines.
    audit_path = OUTPUT_DIR / "annotated_event_log.jsonl"
    processor.export_audit_jsonl(str(audit_path))

    # Save a machine-readable evaluation summary.
    passed = sum(item["status"] == "PASS" for item in checks)
    failed = sum(item["status"] == "FAIL" for item in checks)

    summary = {
        "project": "Distributed System Stream Processing & Cache Optimization",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "evaluation_type": "functional_checks",
        "total_checks": len(checks),
        "passed": passed,
        "failed": failed,
        "status": "PASS" if failed == 0 else "FAIL",
        "checks": checks,
        "limitations": [
            "This evaluation uses the project's in-memory cache implementation.",
            "It does not establish Redis integration or distributed deployment.",
            "It does not establish compliance with a production latency SLA.",
        ],
    }

    summary_path = REPORTS_DIR / "evaluation_summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    # Save a concise human-readable findings report.
    findings = [
        "# Evaluation Summary & Findings",
        "",
        f"Generated (UTC): {summary['generated_at_utc']}",
        "",
        f"- Overall status: **{summary['status']}**",
        f"- Functional checks: {summary['total_checks']}",
        f"- Passed: {passed}",
        f"- Failed: {failed}",
        "",
        "## Results",
        "",
    ]

    for check in checks:
        findings.extend([
            f"### {check['name']}",
            f"- Status: **{check['status']}**",
            f"- Details: `{json.dumps(check['details'], sort_keys=True)}`",
            "",
        ])

    findings.extend([
        "## Limitations",
        "",
        "- The current cache evaluation uses in-memory storage.",
        "- Redis integration and multi-node deployment were not tested.",
        "- No production latency SLA is claimed by these functional checks.",
        "",
    ])

    findings_path = REPORTS_DIR / "evaluation_findings.md"
    findings_path.write_text("\n".join(findings), encoding="utf-8")

    print(json.dumps({
        "status": summary["status"],
        "total_checks": len(checks),
        "passed": passed,
        "failed": failed,
        "summary": str(summary_path.relative_to(PROJECT_ROOT)),
        "findings": str(findings_path.relative_to(PROJECT_ROOT)),
        "event_log": str(audit_path.relative_to(PROJECT_ROOT)),
    }, indent=2))

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()