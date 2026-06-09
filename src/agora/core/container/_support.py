"""Lifecycle and lookup helpers for the DI container."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.core.container._lifetime import _Lifetime
from agora.core.plugin import Lifecycle

if TYPE_CHECKING:
    from agora.core.container._container import AgoraContainer


def resolve_entry(container: AgoraContainer, key: str, **kwargs: Any) -> Any:
    """Resolve a dependency by key."""
    if key in container._singletons:
        return container._singletons[key]

    entry = container._factories.get(key)
    if entry is None:
        raise KeyError(
            f"Container '{container._name}': key '{key}' is not registered. "
            f"Available: {list(container.keys())}"
        )

    factory, lifetime = entry
    instance = factory(**kwargs)
    if lifetime == _Lifetime.SINGLETON:
        container._singletons[key] = instance
    return instance


async def startup_registered_singletons(
    container: AgoraContainer,
    *,
    logger: Any,
) -> None:
    """Eagerly resolve singleton factories then run lifecycle startup in order."""
    for key in container._registration_order:
        entry = container._factories.get(key)
        if entry is not None:
            factory, lifetime = entry
            if lifetime == _Lifetime.SINGLETON and key not in container._singletons:
                try:
                    container._singletons[key] = factory()
                except Exception:
                    logger.exception(
                        "container_startup_resolve_error",
                        container=container._name,
                        key=key,
                    )
                    raise

    for key in container._registration_order:
        instance = container._singletons.get(key)
        if instance is not None and isinstance(instance, Lifecycle):
            logger.debug("container_startup", container=container._name, key=key)
            await instance.startup()


async def shutdown_registered_singletons(
    container: AgoraContainer,
    *,
    logger: Any,
) -> None:
    """Run lifecycle shutdown in reverse registration order."""
    for key in reversed(container._registration_order):
        instance = container._singletons.get(key)
        if instance is not None and isinstance(instance, Lifecycle):
            try:
                logger.debug("container_shutdown", container=container._name, key=key)
                await instance.shutdown()
            except Exception:
                logger.exception(
                    "container_shutdown_error",
                    container=container._name,
                    key=key,
                )
