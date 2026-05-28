# Manifest Contract

_When to read this: you are building a plugin package and want to declare compatibility metadata, or you are seeing "incompatible plugin" warnings in your logs._

## Two version numbers, two different things

Agora uses two version numbers that are intentionally separate:

| Constant | Current value | Tracks |
|---|---|---|
| `agora-etl` package version | `0.1.9` | The framework release — features, fixes, API changes |
| `AGORA_PLUGIN_MANIFEST_VERSION` | `"0.3"` | The optional MANIFEST metadata schema understood by this release |

These are not the same thing and they do not move together.

`AGORA_PLUGIN_MANIFEST_VERSION` only bumps when the shape of the `MANIFEST` object that plugins can declare changes — for example, if a new required field is added or an existing field is renamed. It does not bump on every `agora-etl` release.

A plugin that was compatible with `agora-etl 0.1.7` and declares `agora_api_version = "0.3"` is still compatible with `agora-etl 0.1.9` — the manifest schema has not changed.

## What is a MANIFEST?

`MANIFEST` is an optional object you can expose at the root of your plugin package. Agora reads it during entry-point discovery to populate compatibility diagnostics and `agora plugins list` output.

It is entirely optional. A plugin without a `MANIFEST` loads normally and is reported with `compatible=None`.

### Minimal MANIFEST

```python
# my_plugin/__init__.py

from dataclasses import dataclass

@dataclass(frozen=True)
class _Manifest:
    package: str
    version: str
    agora_api_version: str

MANIFEST = _Manifest(
    package="my-plugin",
    version="1.0.0",
    agora_api_version="0.3",  # must match AGORA_PLUGIN_MANIFEST_VERSION
)
```

The object does not need to be a dataclass — any object with the right attributes works. The fields Agora reads are:

| Field | Type | Purpose |
|---|---|---|
| `package` | `str` | Display name in `agora plugins list` |
| `version` | `str` | Plugin package version shown in diagnostics |
| `agora_api_version` | `str` | Compared against `AGORA_PLUGIN_MANIFEST_VERSION` |

All fields are optional. Missing fields are treated as `None` in diagnostics.

## Compatibility matrix

| Plugin declares | Result |
|---|---|
| No `MANIFEST` | Loaded. `compatible=None` in diagnostics. No warning. |
| `MANIFEST` without `agora_api_version` | Loaded. `compatible=None` in diagnostics. |
| `MANIFEST.agora_api_version == AGORA_PLUGIN_MANIFEST_VERSION` | Loaded. `compatible=True` in diagnostics. |
| `MANIFEST.agora_api_version != AGORA_PLUGIN_MANIFEST_VERSION` | **Not loaded.** `compatible=False`. Shown in `agora plugins list` as incompatible. Warning logged. |

An incompatible plugin does not abort discovery or prevent the pipeline from starting. It is simply excluded from the active registry.

## Reading the warning

When a plugin is excluded for incompatibility, the log emits:

```
registry_entrypoint_incompatible  registry=sources  group=agora.sources
  name=my_source  plugin_api_version=0.2  expected_manifest_version=0.3
```

The fields tell you:

- `plugin_api_version` — what the plugin declared
- `expected_manifest_version` — what this release of `agora-etl` expects

To fix: update your plugin's `MANIFEST.agora_api_version` to `"0.3"` and verify the plugin still works with the current base classes.

## Checking the current value at runtime

```python
from agora.core.registry import AGORA_PLUGIN_MANIFEST_VERSION

print(AGORA_PLUGIN_MANIFEST_VERSION)  # "0.3"
```

## AGORA_API_VERSION alias

Older plugin packages may import `AGORA_API_VERSION` from `agora.core.registry`. This is a backward-compatible alias for `AGORA_PLUGIN_MANIFEST_VERSION` — both resolve to the same value.

`AGORA_API_VERSION` is deprecated as of `0.1.9`. It will be removed in `0.2.0`. New plugins should use `AGORA_PLUGIN_MANIFEST_VERSION` directly.

```python
# Old — deprecated, will be removed in 0.2.0
from agora.core.registry import AGORA_API_VERSION

# New — use this
from agora.core.registry import AGORA_PLUGIN_MANIFEST_VERSION
```

## When does AGORA_PLUGIN_MANIFEST_VERSION bump?

`AGORA_PLUGIN_MANIFEST_VERSION` bumps when the MANIFEST schema itself changes in a way that requires plugin authors to update their declarations. Examples that would trigger a bump:

- A new required field is added to MANIFEST.
- An existing field is renamed or its semantics change.
- The compatibility check logic changes in a way that would exclude previously compatible plugins.

Examples that do NOT trigger a bump:

- A new `agora-etl` feature or API is added.
- A bug fix in the runtime.
- A new entry-point group is added (the group is new, the schema is not).

The current value `"0.3"` has been stable since before `0.1.6`. It is not expected to change before `0.2.0`.

## Viewing all registered plugins

```bash
agora plugins list
```

This shows every discovered plugin across all groups, including incompatible ones, with their package, version, and compatibility status.
