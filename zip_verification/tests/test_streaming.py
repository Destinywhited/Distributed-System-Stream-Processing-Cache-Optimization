import unittest

from distributed_cache.streaming import EventProcessor


def valid_event(event_id="evt-001", partition=0):
    return {
        "event_id": event_id,
        "partition": partition,
        "timestamp": 1000,
        "payload": {"action": "purchase"},
    }


class StreamingTests(unittest.TestCase):
    def test_processes_valid_event(self):
        processor = EventProcessor(
            handler=lambda event: event.payload["action"]
        )

        result = processor.process(valid_event())

        self.assertEqual(result.status, "processed")
        self.assertEqual(result.attempts, 1)
        self.assertEqual(processor.metrics["processed"], 1)

    def test_duplicate_event_is_not_processed_twice(self):
        calls = []
        processor = EventProcessor(
            handler=lambda event: calls.append(event.event_id)
        )
        event = valid_event("evt-002")

        first = processor.process(event)
        second = processor.process(event)

        self.assertEqual(first.status, "processed")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(len(calls), 1)
        self.assertEqual(processor.metrics["duplicates"], 1)

    def test_invalid_event_goes_to_dead_letter(self):
        processor = EventProcessor(handler=lambda event: None)

        result = processor.process(valid_event(event_id=""))

        self.assertEqual(result.status, "invalid")
        self.assertEqual(len(processor.dead_letter_events), 1)
        self.assertEqual(processor.metrics["failed"], 1)

    def test_retries_then_succeeds(self):
        attempts = []

        def flaky_handler(event):
            attempts.append(event.event_id)
            if len(attempts) < 2:
                raise RuntimeError("temporary failure")
            return "recovered"

        processor = EventProcessor(
            handler=flaky_handler,
            max_attempts=3,
            retry_delay_seconds=0,
            sleep=lambda seconds: None,
        )

        result = processor.process(valid_event("evt-003"))

        self.assertEqual(result.status, "processed")
        self.assertEqual(result.attempts, 2)
        self.assertEqual(len(attempts), 2)

    def test_exhausted_retries_go_to_dead_letter(self):
        processor = EventProcessor(
            handler=lambda event: (_ for _ in ()).throw(
                RuntimeError("persistent failure")
            ),
            max_attempts=2,
            retry_delay_seconds=0,
            sleep=lambda seconds: None,
        )

        result = processor.process(valid_event("evt-004"))

        self.assertEqual(result.status, "dead_letter")
        self.assertEqual(result.attempts, 2)
        self.assertEqual(len(processor.dead_letter_events), 1)
        self.assertEqual(processor.metrics["failed"], 1)

    def test_boolean_partition_is_rejected(self):
        processor = EventProcessor(handler=lambda event: None)

        result = processor.process(valid_event(partition=True))

        self.assertEqual(result.status, "invalid")
        self.assertEqual(len(processor.dead_letter_events), 1)


if __name__ == "__main__":
    unittest.main()