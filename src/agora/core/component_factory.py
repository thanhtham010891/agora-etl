"""Config-driven component factory helpers for Agora core."""

from __future__ import annotations

import importlib
from typing import Any, ClassVar

from agora.core.errors import ConfigError


class ConfigComponentFactory:
    """Build typed Agora components from config dictionaries.

    This isolates registry lookup and config normalization from the
    container so config-driven assembly can evolve independently from
    dependency lifetime management.
    """

    _REGISTRY_MAP: ClassVar[dict[str, tuple[str, str]]] = {
        "source": ("agora.sources", "source_registry"),
        "sink": ("agora.sinks", "sink_registry"),
        "middleware": ("agora.middlewares", "middleware_registry"),
        "ai_provider": ("agora.ai", "ai_provider_registry"),
        "ai_cache": ("agora.ai", "ai_cache_registry"),
        "dedup_store": ("agora.middlewares.dedup.stores", "dedup_store_registry"),
        "dedup_strategy": (
            "agora.middlewares.dedup.strategies",
            "dedup_strategy_registry",
        ),
        "runner": ("agora.runner", "runner_registry"),
    }

    def get_registry(self, category: str) -> Any:
        """Lazily import and return the registry for *category*."""
        entry = self._REGISTRY_MAP.get(category)
        if entry is None:
            raise ConfigError(
                "Unknown component category "
                f"'{category}'. Available: {list(self._REGISTRY_MAP.keys())}"
            )
        module_path, attr_name = entry
        module = importlib.import_module(module_path)
        return getattr(module, attr_name)

    def resolve_value(self, value: Any) -> Any:
        """Recursively resolve config values, including import references.

        Supported import reference shape::

            { "import": "my_package.module:callable_name" }

        When a dict contains only the ``import`` key, it is replaced with
        the imported Python object. All other dicts/lists are traversed
        recursively so nested component kwargs can contain callables or
        pre-built objects.
        """
        if isinstance(value, list):
            return [self.resolve_value(item) for item in value]

        if isinstance(value, dict):
            if set(value.keys()) == {"import"}:
                import_path = value["import"]
                if not isinstance(import_path, str):
                    raise ConfigError(
                        "Import reference must be a string like 'my_package.module:attribute'."
                    )
                return self._import_object(import_path)
            return {key: self.resolve_value(item) for key, item in value.items()}

        return value

    def _import_object(self, import_path: str) -> Any:
        module_name, attr_name = self._split_import_path(import_path)
        try:
            module = importlib.import_module(module_name)
        except ImportError as exc:
            raise ConfigError(
                f"Cannot import module '{module_name}' from '{import_path}': {exc}"
            ) from exc

        try:
            return getattr(module, attr_name)
        except AttributeError as exc:
            raise ConfigError(
                f"Module '{module_name}' does not define attribute '{attr_name}' "
                f"referenced by '{import_path}'."
            ) from exc

    @staticmethod
    def _split_import_path(import_path: str) -> tuple[str, str]:
        if ":" in import_path:
            module_name, attr_name = import_path.split(":", 1)
        else:
            module_name, sep, attr_name = import_path.rpartition(".")
            if not sep:
                raise ConfigError(
                    "Import reference must use 'module:attribute' or 'module.attribute', "
                    f"got '{import_path}'."
                )
        if not module_name or not attr_name:
            raise ConfigError(
                "Import reference must use 'module:attribute' or 'module.attribute', "
                f"got '{import_path}'."
            )
        return module_name, attr_name

    def build_component(self, cfg: dict[str, Any], category: str) -> Any:
        """Create a component instance from a config dict."""
        if not isinstance(cfg, dict):
            raise ConfigError(f"Expected a dict for {category} config, got {type(cfg).__name__}")

        cfg = self.resolve_value(dict(cfg))
        type_key = cfg.pop("type", None)
        if type_key is None:
            raise ConfigError(f"Missing 'type' key in {category} config: {cfg}")

        registry = self.get_registry(category)
        try:
            return registry.create(type_key, **cfg)
        except TypeError as exc:
            raise ConfigError(
                f"Failed to instantiate {category} plugin '{type_key}': {exc}"
            ) from exc

    def build_middleware_component(self, cfg: dict[str, Any]) -> Any:
        """Build middleware, resolving nested AI provider/cache configs when present."""
        if not isinstance(cfg, dict):
            raise ConfigError(f"Expected a dict for middleware config, got {type(cfg).__name__}")

        mw_cfg = self.resolve_value(dict(cfg))
        mw_type = mw_cfg.get("type")
        if isinstance(mw_type, str) and mw_type.startswith("ai_"):
            provider_cfg = mw_cfg.get("provider")
            if isinstance(provider_cfg, dict):
                mw_cfg["provider"] = self.build_component(provider_cfg, "ai_provider")

            cache_cfg = mw_cfg.get("cache")
            if isinstance(cache_cfg, dict):
                normalized_cache_cfg = dict(cache_cfg)
                if "type" not in normalized_cache_cfg and "backend" in normalized_cache_cfg:
                    normalized_cache_cfg["type"] = "backend"
                mw_cfg["cache"] = self.build_component(normalized_cache_cfg, "ai_cache")

        return self.build_component(mw_cfg, "middleware")

    def build_dedup_component(self, cfg: dict[str, Any]) -> Any:
        """Build a ``DedupMiddleware`` from a config dict."""
        from agora.middlewares.dedup.middleware import DedupMiddleware

        if not isinstance(cfg, dict):
            raise ConfigError(f"Expected a dict for dedup config, got {type(cfg).__name__}")

        cfg = self.resolve_value(dict(cfg))
        key_spec = cfg.pop("key", None)
        if key_spec is None:
            raise ConfigError("Missing 'key' in dedup config")

        if isinstance(key_spec, str) and not key_spec.startswith("lambda"):
            attr = key_spec
            key_fn = lambda r, _a=attr: getattr(r, _a)  # noqa: E731
        elif callable(key_spec):
            key_fn = key_spec
        else:
            raise ConfigError(
                "'key' in dedup config must be a callable or dotted attribute "
                f"name, got {key_spec!r}"
            )

        store = None
        store_cfg = cfg.pop("store", None)
        if store_cfg is not None:
            store = self.build_component(store_cfg, "dedup_store")

        strategy = None
        strategy_cfg = cfg.pop("strategy", None)
        if strategy_cfg is not None:
            strategy = self.build_component(strategy_cfg, "dedup_strategy")

        return DedupMiddleware(key=key_fn, store=store, strategy=strategy, **cfg)


config_component_factory = ConfigComponentFactory()

__all__ = ["ConfigComponentFactory", "config_component_factory"]
