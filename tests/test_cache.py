import unittest

from distributed_cache.cache import HierarchicalCache


class CacheTests(unittest.TestCase):
    def test_write_and_read(self):
        cache = HierarchicalCache()
        cache.set("user:1", {"name": "Alex"})

        self.assertEqual(
            cache.get("user:1"),
            {"name": "Alex"},
        )

    def test_cache_miss(self):
        cache = HierarchicalCache()

        self.assertIsNone(cache.get("missing"))

    def test_invalidation(self):
        cache = HierarchicalCache()
        cache.set("product:1", "keyboard")
        cache.invalidate("product:1")

        self.assertIsNone(cache.get("product:1"))
        self.assertEqual(cache.stats().invalidations, 1)

    def test_negative_ttl_rejected(self):
        cache = HierarchicalCache()

        with self.assertRaises(ValueError):
            cache.set("invalid", "value", -1)

    def test_hit_rate(self):
        cache = HierarchicalCache()
        cache.set("a", 1)
        cache.get("a")
        cache.get("missing")

        self.assertAlmostEqual(
            cache.stats().hit_rate,
            0.5,
        )


if __name__ == "__main__":
    unittest.main()