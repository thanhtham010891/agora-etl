"""Shared policy enums used across pipeline runtime and plugins."""

from __future__ import annotations

from enum import StrEnum


class OnError(StrEnum):
    """Error-handling policy for middlewares."""

    PASSTHROUGH = "passthrough"
    DROP = "drop"
    RAISE = "raise"
    LOG = "log"


class DLQFailurePolicy(StrEnum):
    """Policy for failures while writing to the DLQ sink."""

    LOG_ONLY = "log_only"
    RAISE = "raise"


class CheckpointFailurePolicy(StrEnum):
    """Policy for checkpoint-store failures."""

    FAIL_CLOSED = "fail_closed"
    LOG_AND_CONTINUE = "log_and_continue"


class SinkFailurePolicy(StrEnum):
    """Policy for sink delivery failures after middleware processing."""

    FAIL_CLOSED = "fail_closed"
    LOG_AND_CONTINUE = "log_and_continue"


class SourceRecordFailurePolicy(StrEnum):
    """Policy for source-side record decode/deserialize failures."""

    FAIL_CLOSED = "fail_closed"
    LOG_AND_CONTINUE = "log_and_continue"


class DedupStoreFailurePolicy(StrEnum):
    """Policy for dedup-store failures inside ``DedupMiddleware``."""

    FAIL_CLOSED = "fail_closed"
    FAIL_OPEN = "fail_open"
