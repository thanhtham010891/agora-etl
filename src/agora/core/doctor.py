"""Shared machine-readable contracts for ``agora doctor`` support surfaces."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from importlib.metadata import entry_points
from typing import Any, Protocol, runtime_checkable

DOCTOR_READINESS_ENTRYPOINT_GROUP = "agora.doctor.readiness"


class Status(Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


@dataclass
class CheckResult:
    name: str
    status: Status
    message: str
    detail: str = ""
    data: dict[str, Any] = field(default_factory=dict)


@dataclass
class DoctorReport:
    results: list[CheckResult] = field(default_factory=list)

    def add(self, result: CheckResult) -> None:
        self.results.append(result)

    @property
    def failed(self) -> bool:
        return any(result.status == Status.FAIL for result in self.results)

    @property
    def warned(self) -> bool:
        return any(result.status == Status.WARN for result in self.results)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable machine-readable report."""
        return {
            "failed": self.failed,
            "warned": self.warned,
            "results": [
                {
                    "name": result.name,
                    "status": result.status.value,
                    "message": result.message,
                    "detail": result.detail,
                    "data": dict(result.data),
                }
                for result in self.results
            ],
            "readiness": self._readiness_payload(),
        }

    def _readiness_payload(self) -> dict[str, Any]:
        components = [
            dict(result.data)
            for result in self.results
            if result.data.get("category") == "enterprise_readiness"
        ]
        by_backend: dict[str, list[dict[str, Any]]] = {}
        for component in components:
            backend = str(component.get("backend", "unknown"))
            by_backend.setdefault(backend, []).append(component)
        return {
            "component_count": len(components),
            "backends": {
                backend: {
                    "component_count": len(entries),
                    "failed": any(entry.get("status") == Status.FAIL.value for entry in entries),
                    "warned": any(entry.get("status") == Status.WARN.value for entry in entries),
                    "components": entries,
                }
                for backend, entries in by_backend.items()
            },
        }


@runtime_checkable
class DoctorReadinessProvider(Protocol):
    """Plugin-owned readiness provider contract consumed by ``agora doctor``."""

    backend: str
    component_types: frozenset[str]

    async def run_readiness_checks(
        self,
        pipeline_config: dict[str, Any],
    ) -> list[CheckResult]:
        """Return structured readiness checks for the configured pipeline."""


@dataclass(frozen=True, slots=True)
class DoctorReadinessProviderEntry:
    """Loaded doctor readiness provider plus its entry-point key."""

    name: str
    provider: DoctorReadinessProvider


def discover_doctor_readiness_providers() -> tuple[DoctorReadinessProviderEntry, ...]:
    """Load installed doctor readiness providers from the public entry-point group."""

    providers: list[DoctorReadinessProviderEntry] = []
    for entry_point in sorted(
        entry_points(group=DOCTOR_READINESS_ENTRYPOINT_GROUP),
        key=lambda candidate: candidate.name,
    ):
        provider = entry_point.load()
        if not isinstance(provider, DoctorReadinessProvider):
            raise TypeError(
                f"Doctor readiness provider {entry_point.name!r} from "
                f"{entry_point.value!r} does not implement the provider contract."
            )
        providers.append(
            DoctorReadinessProviderEntry(
                name=entry_point.name,
                provider=provider,
            )
        )
    return tuple(providers)


__all__ = [
    "DOCTOR_READINESS_ENTRYPOINT_GROUP",
    "CheckResult",
    "DoctorReadinessProvider",
    "DoctorReadinessProviderEntry",
    "DoctorReport",
    "Status",
    "discover_doctor_readiness_providers",
]
