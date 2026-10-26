"""Deterministic partition assignment and rebalancing."""

import hashlib
from dataclasses import dataclass


def stable_hash(value: str) -> int:
    """Generate a deterministic hash across Python processes."""
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest, 16)


@dataclass(frozen=True)
class PartitionPlan:
    assignments: dict[int, str]
    moved_partitions: tuple[int, ...]
    previous_owner_count: int
    new_owner_count: int


class PartitionRebalancer:
    """Assign partitions using rendezvous hashing."""

    def __init__(self, partition_count: int):
        if partition_count < 1:
            raise ValueError("partition_count must be at least 1")

        self.partition_count = partition_count
        self._assignments: dict[int, str] = {}

    def owner_for(self, partition: int, workers: list[str]) -> str:
        if not 0 <= partition < self.partition_count:
            raise ValueError("Partition is out of range")

        normalized = sorted(set(workers))

        if not normalized:
            raise ValueError("At least one worker is required")

        return max(
            normalized,
            key=lambda worker: stable_hash(f"{partition}:{worker}"),
        )

    def rebalance(self, workers: list[str]) -> PartitionPlan:
        normalized = sorted(set(workers))

        if not normalized:
            raise ValueError("At least one worker is required")

        previous = dict(self._assignments)

        updated = {
            partition: self.owner_for(partition, normalized)
            for partition in range(self.partition_count)
        }

        moved = tuple(
            partition
            for partition, owner in updated.items()
            if partition in previous and previous[partition] != owner
        )

        self._assignments = updated

        return PartitionPlan(
            assignments=dict(updated),
            moved_partitions=moved,
            previous_owner_count=len(set(previous.values())),
            new_owner_count=len(set(updated.values())),
        )

    @property
    def assignments(self) -> dict[int, str]:
        return dict(self._assignments)