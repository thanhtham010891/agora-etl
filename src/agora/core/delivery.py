"""Machine-readable delivery semantics for a resolved pipeline."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from agora.core.checkpoint import CheckpointIdentityProvider, is_checkpoint_capable
from agora.core.errors import PipelineError
from agora.core.types import CheckpointFailurePolicy

if TYPE_CHECKING:
    from agora.core.source import BaseSource
    from agora.core.types import DeliveryConfig
    from agora.core.writer import Writer


class DeliveryGuarantee(StrEnum):
    """Delivery model asserted by the core runtime."""

    AT_LEAST_ONCE = "at_least_once"


class IdempotencyMode(StrEnum):
    """How a sink handles repeat delivery across a recovery window."""

    UNKNOWN = "unknown"
    NONE = "none"
    APPLICATION_MANAGED = "application_managed"
    SINK_NATIVE = "sink_native"
    TRANSACTIONAL = "transactional"


@dataclass(frozen=True, slots=True)
class DeliveryPolicy:
    """Optional pre-run requirements for source recovery and sink delivery."""

    require_replay_safe: bool = False
    require_idempotent_sinks: bool = False

    @property
    def enforced(self) -> bool:
        """Whether this policy adds any runtime requirement."""
        return self.require_replay_safe or self.require_idempotent_sinks

    def to_dict(self) -> dict[str, bool]:
        return {
            "require_replay_safe": self.require_replay_safe,
            "require_idempotent_sinks": self.require_idempotent_sinks,
        }


@dataclass(frozen=True, slots=True)
class DeliveryPolicyMismatch:
    """One actionable reason a resolved pipeline violates ``DeliveryPolicy``."""

    code: str
    message: str
    sink_name: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "code": self.code,
            "message": self.message,
            "sink_name": self.sink_name,
        }


class DeliveryPolicyMismatchError(PipelineError):
    """Raised before execution when a required delivery property is absent."""

    def __init__(
        self,
        mismatches: tuple[DeliveryPolicyMismatch, ...],
        *,
        pipeline_id: str,
        source_name: str,
    ) -> None:
        self.mismatches = mismatches
        detail = "; ".join(f"{item.code}: {item.message}" for item in mismatches)
        super().__init__(
            f"Delivery policy mismatch: {detail}",
            pipeline_id=pipeline_id,
            source_name=source_name,
            stage="delivery_policy",
        )


@dataclass(frozen=True, slots=True)
class SinkDeliveryCapability:
    """Replay and idempotency behavior declared by one sink."""

    sink_name: str
    idempotency: IdempotencyMode = IdempotencyMode.UNKNOWN
    replay_safe: bool = False
    transactionally_coupled_checkpoint: bool = False
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "sink_name": self.sink_name,
            "idempotency": self.idempotency.value,
            "replay_safe": self.replay_safe,
            "transactionally_coupled_checkpoint": self.transactionally_coupled_checkpoint,
            "notes": list(self.notes),
        }


@runtime_checkable
class SinkDeliveryCapabilityProvider(Protocol):
    """Optional sink contract for declaring replay behavior to the core."""

    def delivery_capability(self) -> SinkDeliveryCapability:
        """Return the sink's public idempotency and replay contract."""
        ...


@dataclass(frozen=True, slots=True)
class DeliveryCapability:
    """Delivery semantics for one fully resolved source-to-sink pipeline."""

    guarantee: DeliveryGuarantee
    source_checkpointing_enabled: bool
    source_identity_supported: bool
    checkpoint_failure_policy: str
    source_order_preserved: bool
    checkpoint_advances_after_handled_outcome: bool
    duplicate_delivery_possible: bool
    transactional_checkpoint_coupling: bool
    sinks: tuple[SinkDeliveryCapability, ...]
    replay_safe: bool
    risk_flags: tuple[str, ...]
    policy: DeliveryPolicy
    policy_mismatches: tuple[DeliveryPolicyMismatch, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "guarantee": self.guarantee.value,
            "source_checkpointing_enabled": self.source_checkpointing_enabled,
            "source_identity_supported": self.source_identity_supported,
            "checkpoint_failure_policy": self.checkpoint_failure_policy,
            "source_order_preserved": self.source_order_preserved,
            "checkpoint_advances_after_handled_outcome": self.checkpoint_advances_after_handled_outcome,
            "duplicate_delivery_possible": self.duplicate_delivery_possible,
            "transactional_checkpoint_coupling": self.transactional_checkpoint_coupling,
            "sinks": [sink.to_dict() for sink in self.sinks],
            "replay_safe": self.replay_safe,
            "risk_flags": list(self.risk_flags),
            "policy": self.policy.to_dict(),
            "policy_mismatches": [mismatch.to_dict() for mismatch in self.policy_mismatches],
        }


def sink_delivery_capability(sink: object) -> SinkDeliveryCapability:
    """Return a sink declaration, defaulting conservatively to unknown."""
    name = str(getattr(sink, "sink_name", type(sink).__name__))
    if not isinstance(sink, SinkDeliveryCapabilityProvider):
        return SinkDeliveryCapability(sink_name=name)
    capability = sink.delivery_capability()
    if not isinstance(capability, SinkDeliveryCapability):
        raise TypeError("delivery_capability() must return SinkDeliveryCapability")
    if capability.sink_name != name:
        return SinkDeliveryCapability(
            sink_name=name,
            idempotency=capability.idempotency,
            replay_safe=capability.replay_safe,
            transactionally_coupled_checkpoint=capability.transactionally_coupled_checkpoint,
            notes=capability.notes,
        )
    return capability


def _writer_sinks(writer: Writer[object]) -> tuple[object, ...]:
    """Find concrete sinks behind standard fan-out and router writers."""
    sinks = getattr(writer, "_sinks", None)
    if sinks is not None:
        return tuple(sinks)

    routes = getattr(writer, "_routes", None)
    default_sink = getattr(writer, "_default", None)
    if routes is None:
        return ()
    result: list[object] = []
    seen: set[int] = set()
    for route in routes:
        sink = route.sink
        if id(sink) not in seen:
            seen.add(id(sink))
            result.append(sink)
    if default_sink is not None and id(default_sink) not in seen:
        result.append(default_sink)
    return tuple(result)


def delivery_policy_mismatches(
    capability: DeliveryCapability,
) -> tuple[DeliveryPolicyMismatch, ...]:
    """Evaluate policy requirements against a resolved delivery report."""
    policy = capability.policy
    if not policy.enforced:
        return ()

    mismatches: list[DeliveryPolicyMismatch] = []
    if policy.require_replay_safe:
        if not capability.source_checkpointing_enabled:
            mismatches.append(
                DeliveryPolicyMismatch(
                    code="checkpoint_not_enabled",
                    message="require_replay_safe needs checkpointing on a checkpoint-capable source.",
                )
            )
        if not capability.source_identity_supported:
            mismatches.append(
                DeliveryPolicyMismatch(
                    code="source_identity_not_advertised",
                    message="require_replay_safe needs a source identity to validate resume input.",
                )
            )
        if not capability.sinks:
            mismatches.append(
                DeliveryPolicyMismatch(
                    code="sink_capability_not_available",
                    message="require_replay_safe needs at least one resolved sink capability.",
                )
            )
        for sink in capability.sinks:
            if not sink.replay_safe:
                mismatches.append(
                    DeliveryPolicyMismatch(
                        code="sink_not_replay_safe",
                        message=(
                            f"Sink '{sink.sink_name}' does not declare replay-safe delivery "
                            f"(idempotency={sink.idempotency.value})."
                        ),
                        sink_name=sink.sink_name,
                    )
                )

    if policy.require_idempotent_sinks:
        for sink in capability.sinks:
            if sink.idempotency in {IdempotencyMode.UNKNOWN, IdempotencyMode.NONE}:
                mismatches.append(
                    DeliveryPolicyMismatch(
                        code=(
                            "sink_idempotency_unknown"
                            if sink.idempotency == IdempotencyMode.UNKNOWN
                            else "sink_not_idempotent"
                        ),
                        message=(
                            f"Sink '{sink.sink_name}' declares "
                            f"idempotency={sink.idempotency.value}."
                        ),
                        sink_name=sink.sink_name,
                    )
                )
    return tuple(mismatches)


def enforce_delivery_policy(
    capability: DeliveryCapability,
    *,
    pipeline_id: str,
    source_name: str,
) -> None:
    """Fail before pipeline execution when configured delivery requirements do not hold."""
    if capability.policy_mismatches:
        raise DeliveryPolicyMismatchError(
            capability.policy_mismatches,
            pipeline_id=pipeline_id,
            source_name=source_name,
        )


def build_delivery_capability(
    *,
    source: BaseSource[object],
    writer: Writer[object],
    config: DeliveryConfig,
) -> DeliveryCapability:
    """Build a conservative report without claiming backend-specific guarantees."""
    policy = config.delivery_policy or DeliveryPolicy()
    checkpointing_enabled = config.checkpoint is not None and is_checkpoint_capable(source)
    identity_supported = isinstance(source, CheckpointIdentityProvider)
    sinks = tuple(sink_delivery_capability(sink) for sink in _writer_sinks(writer))

    risk_flags: list[str] = []
    if not checkpointing_enabled:
        risk_flags.append("checkpoint_not_enabled")
    elif not identity_supported:
        risk_flags.append("source_identity_not_advertised")
    if config.checkpoint_failure_policy == CheckpointFailurePolicy.LOG_AND_CONTINUE:
        risk_flags.append("checkpoint_failure_log_and_continue")
    if not sinks:
        risk_flags.append("sink_capability_not_available")
    for sink in sinks:
        if sink.idempotency == IdempotencyMode.UNKNOWN:
            risk_flags.append(f"sink:{sink.sink_name}:idempotency_unknown")
        elif sink.idempotency == IdempotencyMode.NONE:
            risk_flags.append(f"sink:{sink.sink_name}:not_idempotent")

    replay_safe = (
        checkpointing_enabled
        and identity_supported
        and bool(sinks)
        and all(sink.replay_safe for sink in sinks)
    )
    capability = DeliveryCapability(
        guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
        source_checkpointing_enabled=checkpointing_enabled,
        source_identity_supported=identity_supported,
        checkpoint_failure_policy=config.checkpoint_failure_policy.value,
        source_order_preserved=True,
        checkpoint_advances_after_handled_outcome=True,
        duplicate_delivery_possible=True,
        transactional_checkpoint_coupling=False,
        sinks=sinks,
        replay_safe=replay_safe,
        risk_flags=tuple(risk_flags),
        policy=policy,
        policy_mismatches=(),
    )
    return replace(capability, policy_mismatches=delivery_policy_mismatches(capability))


__all__ = [
    "DeliveryCapability",
    "DeliveryGuarantee",
    "DeliveryPolicy",
    "DeliveryPolicyMismatch",
    "DeliveryPolicyMismatchError",
    "IdempotencyMode",
    "SinkDeliveryCapability",
    "SinkDeliveryCapabilityProvider",
    "build_delivery_capability",
    "delivery_policy_mismatches",
    "enforce_delivery_policy",
    "sink_delivery_capability",
]
