"""Event streaming with validation, retries, and dead-letter handling."""

import json
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class Event:
    event_id: str
    partition: int
    timestamp: float
    payload: dict[str, Any]

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Event":
        if not isinstance(raw, dict):
            raise ValueError("Event must be a dictionary")

        event_id = raw.get("event_id")
        partition = raw.get("partition")
        timestamp = raw.get("timestamp")
        payload = raw.get("payload")

        if not isinstance(event_id, str) or not event_id.strip():
            raise ValueError("event_id must be a non-empty string")

        if (
            isinstance(partition, bool)
            or not isinstance(partition, int)
            or partition < 0
        ):
            raise ValueError("partition must be a non-negative integer")

        if (
            isinstance(timestamp, bool)
            or not isinstance(timestamp, (int, float))
            or timestamp < 0
        ):
            raise ValueError("timestamp must be a non-negative number")

        if not isinstance(payload, dict):
            raise ValueError("payload must be a dictionary")

        return cls(
            event_id=event_id,
            partition=partition,
            timestamp=float(timestamp),
            payload=payload,
        )


@dataclass(frozen=True)
class ProcessingResult:
    event_id: str | None
    status: str
    attempts: int
    error: str | None = None


class EventProcessor:
    """Process events with in-memory idempotency and bounded retries."""

    def __init__(
        self,
        handler: Callable[[Event], Any],
        max_attempts: int = 3,
        retry_delay_seconds: float = 0.0,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        if retry_delay_seconds < 0:
            raise ValueError("Retry delay cannot be negative")

        self.handler = handler
        self.max_attempts = max_attempts
        self.retry_delay_seconds = retry_delay_seconds
        self.sleep = sleep

        self._processed_ids: set[str] = set()
        self.dead_letter_events: list[dict[str, Any]] = []
        self.audit_log: list[dict[str, Any]] = []

        self.metrics = {
            "accepted": 0,
            "processed": 0,
            "duplicates": 0,
            "failed": 0,
        }

    def process(self, raw_event: dict[str, Any]) -> ProcessingResult:
        try:
            event = Event.from_dict(raw_event)
        except (ValueError, TypeError) as exc:
            self.metrics["failed"] += 1

            self.dead_letter_events.append({
                "raw_event": raw_event,
                "error": str(exc),
            })

            result = ProcessingResult(
                None, "invalid", 0, str(exc)
            )
            self.audit_log.append(asdict(result))
            return result

        self.metrics["accepted"] += 1

        if event.event_id in self._processed_ids:
            self.metrics["duplicates"] += 1

            result = ProcessingResult(
                event.event_id, "duplicate", 0
            )
            self.audit_log.append(asdict(result))
            return result

        last_error = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                self.handler(event)
                self._processed_ids.add(event.event_id)
                self.metrics["processed"] += 1

                result = ProcessingResult(
                    event.event_id, "processed", attempt
                )
                self.audit_log.append(asdict(result))
                return result

            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"

                if (
                    attempt < self.max_attempts
                    and self.retry_delay_seconds > 0
                ):
                    self.sleep(self.retry_delay_seconds)

        self.metrics["failed"] += 1

        self.dead_letter_events.append({
            "event": asdict(event),
            "error": last_error,
        })

        result = ProcessingResult(
            event.event_id,
            "dead_letter",
            self.max_attempts,
            last_error,
        )
        self.audit_log.append(asdict(result))
        return result

    def export_audit_jsonl(self, path: str) -> None:
        """Export one JSON audit record per line."""
        with open(path, "w", encoding="utf-8") as stream:
            for record in self.audit_log:
                stream.write(
                    json.dumps(record, sort_keys=True) + "\n"
                )