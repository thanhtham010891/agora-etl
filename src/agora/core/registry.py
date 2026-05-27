"""
agora/core/registry.py
======================
Generic type-safe plugin registry with auto-discovery.

Usage::

    # Define a registry for your plugin type
    normalizer_registry: Registry[NormalizerBase] = Registry(name="normalizer")

    # Register plugins at module level
    normalizer_registry.register("source_a", NormalizerA())
    normalizer_registry.register("source_b", NormalizerB())

    # Auto-discover from a package (triggers module-level register() calls)
    normalizer_registry.load_from_package("myproject.normalizers")

    # Retrieve
    normalizer = normalizer_registry.get("source_a")

Current API::

    # Decorator-based registration
    @source_registry.plugin("my_source")
    class MySource(BaseSource[T]):
        ...

    # Factory registration (lazy instantiation)
    sink_registry.register_factory("warehouse", WarehouseSink)
    sink = sink_registry.create("warehouse", dsn="postgresql://...", table="events")

    # Entry-point discovery (third-party plugins)
    source_registry.load_entrypoints("agora.sources")

    # Protocol validation
    if not source_registry.validate(some_obj, BaseSource):
        raise PluginValidationError(...)
"""

from __future__ import annotations

import importlib
import pkgutil
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.core.errors import PluginNotFoundError, PluginValidationError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

P = TypeVar("P")

logger = logstruct.getLogger(__name__)
AGORA_PLUGIN_MANIFEST_VERSION = "0.3"
"""Version of the optional plugin manifest contract understood by this release.

This value tracks the shape of ``MANIFEST`` metadata used for diagnostics and
compatibility hints. It is intentionally separate from the ``agora-etl``
package version.
"""

# Backward-compatible alias kept for plugin packages that already import it.
# Deprecated in 0.2.0: prefer ``AGORA_PLUGIN_MANIFEST_VERSION`` in new code.
AGORA_API_VERSION = AGORA_PLUGIN_MANIFEST_VERSION


@dataclass(frozen=True, slots=True)
class RegistryItemInfo:
    key: str
    type: str
    origin: str
    package: str | None = None
    version: str | None = None
    agora_api_version: str | None = None
    compatible: bool | None = None


def _coerce_manifest(
    plugin: object,
    *,
    distribution_name: str | None,
    distribution_version: str | None,
) -> dict[str, Any] | None:
    module_name = getattr(plugin, "__module__", "")
    if not isinstance(module_name, str) or not module_name:
        return None

    package_name = module_name.split(".", 1)[0]
    try:
        package = importlib.import_module(package_name)
    except ImportError:
        if distribution_name is None and distribution_version is None:
            return None
        return {
            "package": distribution_name or package_name,
            "version": distribution_version,
            "agora_api_version": None,
            "compatible": None,
        }

    manifest = getattr(package, "MANIFEST", None)
    if manifest is None:
        if distribution_name is None and distribution_version is None:
            return None
        return {
            "package": distribution_name or package_name,
            "version": distribution_version,
            "agora_api_version": None,
            "compatible": None,
        }

    agora_api_version = getattr(manifest, "agora_api_version", None)
    compatible = (
        bool(agora_api_version == AGORA_PLUGIN_MANIFEST_VERSION)
        if agora_api_version is not None
        else None
    )
    return {
        "package": getattr(manifest, "package", None) or distribution_name or package_name,
        "version": getattr(manifest, "version", None) or distribution_version,
        "agora_api_version": agora_api_version,
        "compatible": compatible,
    }


class Registry(Generic[P]):
    """Type-safe plugin registry with entry-point auto-discovery.

    Thread safety: relies on CPython's GIL for dict atomicity. This is
    sufficient for asyncio workloads but is NOT safe under true multi-threading
    or on alternative Python implementations (PyPy, GraalPy, etc.) that do not
    provide GIL guarantees. Add a ``threading.Lock`` if you need that.

    Parameters
    ----------
    name:
        Human-readable name shown in logs/CLI (e.g. "normalizer").
    """

    def __init__(self, name: str = "registry") -> None:
        self._name = name
        self._plugins: dict[str, P] = {}
        self._factories: dict[str, Callable[..., P]] = {}
        self._registration_types: dict[str, str] = {}
        self._origins: dict[str, str] = {}
        self._metadata: dict[str, dict[str, Any]] = {}

    @property
    def name(self) -> str:
        """Human-readable registry name."""
        return self._name

    # ------------------------------------------------------------------ #
    # Registration                                                         #
    # ------------------------------------------------------------------ #

    def register(self, key: str, plugin: P) -> None:
        """Register *plugin* under *key*.

        Calling register() twice with the same key silently replaces the
        previous registration (last-write-wins — useful for overrides).
        """
        self._plugins[key] = plugin
        self._registration_types[key] = "instance"
        self._origins.setdefault(key, "manual")
        logger.debug("registry_register", registry=self._name, key=key)

    def register_factory(self, key: str, factory: Callable[..., P]) -> None:
        """Register a factory callable under *key*.

        The factory is NOT called at registration time — it is invoked
        lazily when ``create(key, **kwargs)`` is called.  This avoids
        importing optional dependencies (e.g. ``aiokafka``) at startup.

        Parameters
        ----------
        key:
            Lookup key (e.g. ``"postgres"``).
        factory:
            Callable that returns an instance of P.  May be a class
            (used as constructor) or any ``(**kwargs) -> P`` function.
        """
        self._factories[key] = factory
        self._registration_types[key] = "factory"
        self._origins.setdefault(key, "manual")
        logger.debug("registry_register_factory", registry=self._name, key=key)

    def plugin(self, key: str) -> Callable[[type[P]], type[P]]:
        """Decorator: register a class under *key* at definition time.

        Usage::

            @source_registry.plugin("my_source")
            class MySource(BaseSource[MyRecord]):
                ...

        The class itself is registered (not an instance), so callers
        use ``registry.create("my_source", **init_kwargs)`` or
        ``registry.get_or_raise("my_source")`` to obtain the class
        and instantiate manually.

        Returns the class unchanged (no wrapper).
        """

        def _decorator(cls: type[P]) -> type[P]:
            self.register(key, cls)  # type: ignore[arg-type]
            return cls

        return _decorator

    # ------------------------------------------------------------------ #
    # Lookup                                                               #
    # ------------------------------------------------------------------ #

    def get(self, key: str) -> P | None:
        """Return the plugin for *key*, or None if not registered."""
        return self._plugins.get(key)

    def get_or_raise(self, key: str) -> P:
        """Return the plugin for *key*.

        Raises
        ------
        PluginNotFoundError
            If *key* is not registered.  This exception inherits from
            ``KeyError`` for backward compatibility.
        """
        plugin = self._plugins.get(key)
        if plugin is None:
            available = self.all_keys()
            raise PluginNotFoundError(
                registry_name=self._name,
                key=key,
                available=available,
            )
        return plugin

    def create(self, key: str, **kwargs: Any) -> P:
        """Create a new instance from a registered factory or class.

        Lookup order:
        1. ``_factories[key](**kwargs)``  — factory takes precedence
        2. ``_plugins[key](**kwargs)``    — if the registered value is callable

        Raises
        ------
        PluginNotFoundError
            If *key* is not registered in either dict.
        TypeError
            If the registered value is not callable.
        """
        factory = self._factories.get(key)
        if factory is not None:
            return factory(**kwargs)

        plugin = self._plugins.get(key)
        if plugin is not None:
            if callable(plugin):
                return plugin(**kwargs)  # type: ignore[no-any-return]
            raise TypeError(
                f"Registry '{self._name}': plugin '{key}' is not callable. "
                f"Use get() for pre-built instances, or register_factory() "
                f"for lazy construction."
            )

        available = self.all_keys()
        raise PluginNotFoundError(
            registry_name=self._name,
            key=key,
            available=available,
        )

    def has(self, key: str) -> bool:
        """Return True if *key* is registered (plugin or factory)."""
        return key in self._plugins or key in self._factories

    def all_keys(self) -> list[str]:
        """Return all registered keys (insertion order, Python ≥3.7)."""
        # Merge keys from both dicts, preserving order, no duplicates.
        seen: set[str] = set()
        keys: list[str] = []
        for k in (*self._plugins, *self._factories):
            if k not in seen:
                seen.add(k)
                keys.append(k)
        return keys

    def items(self) -> Iterator[tuple[str, P]]:
        """Iterate over ``(key, plugin)`` pairs — **instances only**, not factories.

        Use ``all_items()`` when you need a full picture of the registry
        including lazy-loaded factory registrations.
        """
        return iter(self._plugins.items())

    def all_items(self) -> Iterator[tuple[str, str]]:
        """Iterate over ``(key, kind)`` pairs for *all* registrations.

        ``kind`` is ``"instance"`` for direct registrations and
        ``"factory"`` for factory-registered plugins.

        Useful for CLI listing (e.g. ``agora pipelines list``) where you
        want to show *everything* that can be resolved, not just what has
        already been instantiated::

            for key, kind in registry.all_items():
                print(f"{key}  [{kind}]")
        """
        for key in self.all_keys():
            yield (
                key,
                self._registration_types.get(
                    key,
                    "factory" if key in self._factories else "instance",
                ),
            )

    def describe_items(self) -> list[RegistryItemInfo]:
        """Return enriched registration details for CLI and diagnostics."""
        items: list[RegistryItemInfo] = []
        described_keys = self.all_keys()
        for key in self._metadata:
            if key not in described_keys:
                described_keys.append(key)

        for key in described_keys:
            metadata = self._metadata.get(key, {})
            items.append(
                RegistryItemInfo(
                    key=key,
                    type=self._registration_types.get(
                        key,
                        "factory"
                        if key in self._factories
                        else "instance"
                        if key in self._plugins
                        else "unavailable",
                    ),
                    origin=self._origins.get(key, "manual"),
                    package=metadata.get("package"),
                    version=metadata.get("version"),
                    agora_api_version=metadata.get("agora_api_version"),
                    compatible=metadata.get("compatible"),
                )
            )
        return items

    def __contains__(self, key: str) -> bool:
        return self.has(key)

    def __len__(self) -> int:
        return len(set(self._plugins) | set(self._factories))

    def __repr__(self) -> str:
        return (
            f"Registry(name={self._name!r}, "
            f"plugins={len(self._plugins)}, "
            f"factories={len(self._factories)})"
        )

    # ------------------------------------------------------------------ #
    # Validation                                                           #
    # ------------------------------------------------------------------ #

    def validate(self, instance: Any, protocol: type) -> bool:
        """Check if *instance* satisfies *protocol* (runtime_checkable).

        Returns True if the instance passes ``isinstance(instance, protocol)``.
        Useful for guarding registration::

            if not registry.validate(my_sink, BaseSink):
                raise PluginValidationError(...)
        """
        return isinstance(instance, protocol)

    def register_validated(
        self,
        key: str,
        plugin: P,
        protocol: type,
    ) -> None:
        """Register *plugin* only if it satisfies *protocol*.

        Raises
        ------
        PluginValidationError
            If the plugin does not satisfy the protocol.
        """
        if not self.validate(plugin, protocol):
            raise PluginValidationError(
                registry_name=self._name,
                key=key,
                reason=f"Does not satisfy protocol {protocol.__name__}",
            )
        self.register(key, plugin)

    # ------------------------------------------------------------------ #
    # Auto-discovery — package scanning                                    #
    # ------------------------------------------------------------------ #

    def load_from_package(self, package_name: str) -> None:
        """Import all modules in *package_name* to trigger their register() calls.

        This mirrors the auto-discovery pattern already in data-collector's
        PluginRegistry and NormalizerRegistry.

        Example::

            # In myproject/normalizers/__init__.py:
            from agora import normalizer_registry
            normalizer_registry.load_from_package("myproject.normalizers")
        """
        try:
            package = importlib.import_module(package_name)
        except ImportError:
            logger.warning(
                "registry_package_not_found",
                registry=self._name,
                package=package_name,
            )
            return

        if not hasattr(package, "__path__"):
            logger.warning(
                "registry_not_a_package",
                registry=self._name,
                package=package_name,
            )
            return

        for _, module_name, is_pkg in pkgutil.iter_modules(package.__path__):
            if not is_pkg:
                full_name = f"{package_name}.{module_name}"
                try:
                    importlib.import_module(full_name)
                    logger.debug(
                        "registry_module_loaded",
                        registry=self._name,
                        module=full_name,
                    )
                except ImportError:
                    logger.exception(
                        "registry_module_import_error",
                        registry=self._name,
                        module=full_name,
                    )

    # ------------------------------------------------------------------ #
    # Auto-discovery — setuptools entry points                             #
    # ------------------------------------------------------------------ #

    def load_entrypoints(self, group: str) -> None:
        """Discover and register plugins via setuptools entry_points.

        Third-party packages advertise plugins in ``pyproject.toml``::

            [project.entry-points."agora.sources"]
            my_source = "my_package.sources:MySource"

        Then::

            source_registry.load_entrypoints("agora.sources")

        Each entry point's ``.load()`` result is registered under the
        entry point name.  Errors are logged but do not abort discovery.
        """
        from importlib.metadata import entry_points

        eps = entry_points(group=group)
        for ep in eps:
            try:
                plugin = ep.load()
                distribution = getattr(ep, "dist", None)
                metadata = _coerce_manifest(
                    plugin,
                    distribution_name=getattr(distribution, "name", None),
                    distribution_version=getattr(distribution, "version", None),
                )
                if metadata is not None and metadata.get("compatible") is False:
                    self._metadata[ep.name] = metadata
                    self._origins[ep.name] = "entrypoint_incompatible"
                    self._registration_types[ep.name] = "unavailable"
                    logger.warning(
                        "registry_entrypoint_incompatible",
                        registry=self._name,
                        group=group,
                        name=ep.name,
                        plugin_api_version=metadata.get("agora_api_version"),
                        expected_manifest_version=AGORA_PLUGIN_MANIFEST_VERSION,
                    )
                    continue
                self.register(ep.name, plugin)
                self._origins[ep.name] = "entrypoint"
                if metadata is not None:
                    self._metadata[ep.name] = metadata
                logger.info(
                    "registry_entrypoint_loaded",
                    registry=self._name,
                    group=group,
                    name=ep.name,
                )
            except Exception:
                logger.exception(
                    "registry_entrypoint_error",
                    registry=self._name,
                    group=group,
                    name=ep.name,
                )
