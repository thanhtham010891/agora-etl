"""Async-aware dependency injection container facade."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import logstruct

from agora.core.container._assembly import populate_container_from_config
from agora.core.container._lifetime import _Lifetime
from agora.core.container._pipeline import build_pipeline_from_container
from agora.core.container._support import (
    resolve_entry,
    shutdown_registered_singletons,
    startup_registered_singletons,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora.core.pipeline import BoundPipeline


class AgoraContainer:
    """Lightweight async-aware DI container."""

    def __init__(self, name: str = "default") -> None:
        self._name = name
        self._singletons: dict[str, Any] = {}
        self._factories: dict[str, tuple[Callable[..., Any], _Lifetime]] = {}
        self._registration_order: list[str] = []
        self._config: dict[str, Any] = {}
        self._logger = logstruct.getLogger(__name__)

    def register_singleton(self, key: str, instance: Any) -> None:
        self._singletons[key] = instance
        if key not in self._registration_order:
            self._registration_order.append(key)
        self._logger.debug("container_register_singleton", container=self._name, key=key)

    def register_factory(
        self,
        key: str,
        factory: Callable[..., Any],
        *,
        singleton: bool = True,
    ) -> None:
        lifetime = _Lifetime.SINGLETON if singleton else _Lifetime.TRANSIENT
        self._factories[key] = (factory, lifetime)
        if key not in self._registration_order:
            self._registration_order.append(key)
        self._logger.debug(
            "container_register_factory",
            container=self._name,
            key=key,
            lifetime=lifetime.value,
        )

    def resolve(self, key: str, **kwargs: Any) -> Any:
        return resolve_entry(self, key, **kwargs)

    def has(self, key: str) -> bool:
        return key in self._singletons or key in self._factories

    def keys(self) -> list[str]:
        return list(self._registration_order)

    async def startup_all(self) -> None:
        await startup_registered_singletons(self, logger=self._logger)

    async def shutdown_all(self) -> None:
        await shutdown_registered_singletons(self, logger=self._logger)

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> AgoraContainer:
        pipeline_id = config.get("pipeline_id", "pipeline")
        container = cls(name=pipeline_id)
        container._config = config
        populate_container_from_config(container, config)
        return container

    def build_pipeline(self) -> BoundPipeline[Any]:
        return build_pipeline_from_container(self)

    async def __aenter__(self) -> AgoraContainer:
        await self.startup_all()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        del exc_info
        await self.shutdown_all()

    def __contains__(self, key: str) -> bool:
        return self.has(key)

    def __repr__(self) -> str:
        return (
            f"AgoraContainer(name={self._name!r}, "
            f"singletons={len(self._singletons)}, "
            f"factories={len(self._factories)})"
        )
