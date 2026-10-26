import unittest

from distributed_cache.partitioning import PartitionRebalancer


class PartitioningTests(unittest.TestCase):
    def test_assigns_all_partitions(self):
        rebalancer = PartitionRebalancer(partition_count=100)
        plan = rebalancer.rebalance(
            ["worker-a", "worker-b", "worker-c"]
        )

        self.assertEqual(len(plan.assignments), 100)
        self.assertTrue(
            all(
                owner in {"worker-a", "worker-b", "worker-c"}
                for owner in plan.assignments.values()
            )
        )

    def test_assignments_are_stable(self):
        first = PartitionRebalancer(partition_count=50)
        second = PartitionRebalancer(partition_count=50)

        first.rebalance(["worker-a", "worker-b", "worker-c"])
        second.rebalance(["worker-a", "worker-b", "worker-c"])

        self.assertEqual(first.assignments, second.assignments)

    def test_adding_worker_preserves_some_assignments(self):
        rebalancer = PartitionRebalancer(partition_count=200)

        original = rebalancer.rebalance(["worker-a", "worker-b"])
        expanded = rebalancer.rebalance(
            ["worker-a", "worker-b", "worker-c"]
        )

        unchanged = sum(
            original.assignments[p] == expanded.assignments[p]
            for p in original.assignments
        )

        self.assertGreater(unchanged, 0)
        self.assertLess(unchanged, 200)

    def test_rejects_empty_worker_list(self):
        rebalancer = PartitionRebalancer(partition_count=10)

        with self.assertRaises(ValueError):
            rebalancer.rebalance([])

    def test_rejects_invalid_partition_count(self):
        with self.assertRaises(ValueError):
            PartitionRebalancer(partition_count=0)


if __name__ == "__main__":
    unittest.main()