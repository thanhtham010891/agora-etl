from __future__ import annotations

import ast
import importlib
import re
from dataclasses import dataclass
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[2]
DOCS_ROOT = PACKAGE_ROOT / "docs"
SOURCE_ROOT = PACKAGE_ROOT / "src" / "agora"
PLUGIN_SOURCE_ROOTS = tuple(sorted((PACKAGE_ROOT / "plugins").glob("*/src")))

PUBLIC_API_MANIFEST: dict[str, tuple[str, ...]] = {
    "agora": (
        "AgoraContainer",
        "AgoraError",
        "BaseSink",
        "BaseSource",
        "BoundPipeline",
        "Checkpoint",
        "CheckpointFailurePolicy",
        "CheckpointStore",
        "Configurable",
        "DLQFailurePolicy",
        "DLQRecord",
        "DLQSink",
        "DataPlane",
        "DedupStoreFailurePolicy",
        "FilterMiddleware",
        "InMemoryCheckpointStore",
        "IterableSource",
        "Lifecycle",
        "MapMiddleware",
        "Middleware",
        "MiddlewareStageExplain",
        "OnError",
        "Pipeline",
        "PipelineContext",
        "PipelineExplain",
        "PipelineMetrics",
        "PipelineRunSummary",
        "Plugin",
        "Registry",
        "RetryMiddleware",
        "RetryPolicy",
        "RouteMiddleware",
        "SQLiteCheckpointStore",
        "SinkDataPlaneSpec",
        "SinkFanOut",
        "SinkRouter",
        "SinkWriteExplain",
        "SourceDataPlaneSpec",
        "WriteResult",
        "Writer",
        "discover_plugins",
        "retry_async",
        "sink_data_plane_spec",
        "source_data_plane_spec",
        "ArrowBatchMiddleware",
        "ArrowCsvSource",
        "ArrowFilterMiddleware",
        "ArrowJsonLinesSource",
        "ArrowMapMiddleware",
        "ArrowNativeSink",
        "ArrowProcessBatchMiddleware",
        "BatchFailure",
        "BatchFilterMiddleware",
        "BatchMapMiddleware",
        "BatchMiddleware",
        "BatchProcessResult",
        "BatchableSource",
        "ProcessBatchMiddleware",
        "is_arrow_batch_middleware",
        "is_arrow_native_sink",
        "is_batch_capable_source",
        "Backpressure",
        "DeliveryConfig",
        "SinkFailurePolicy",
        "SourceRecordError",
        "SourceRecordFailurePolicy",
        "SourceRuntimeMetrics",
        "InMemoryTracer",
        "NoopTracer",
        "OpenTelemetryTracer",
        "MembershipKeyStore",
        "MemoryBackend",
        "SQLiteBackend",
        "StateBackend",
        "StateValue",
        "StoredValue",
        "TTLKeyValueStore",
        "state_backend_registry",
        "__version__",
    ),
    "agora.core": (
        "AgoraContainer",
        "BaseSink",
        "BaseSource",
        "BoundPipeline",
        "DataPlane",
        "FilterMiddleware",
        "IterableSource",
        "MapMiddleware",
        "Middleware",
        "Pipeline",
        "RetryMiddleware",
        "RetryPolicy",
        "RouteMiddleware",
        "SinkDataPlaneSpec",
        "SinkFanOut",
        "SinkRouter",
        "SourceDataPlaneSpec",
        "WriteResult",
        "Writer",
        "discover_plugins",
        "retry_async",
        "sink_data_plane_spec",
        "source_data_plane_spec",
        "Checkpoint",
        "CheckpointFailurePolicy",
        "CheckpointStore",
        "CheckpointValue",
        "CheckpointableSource",
        "DLQFailurePolicy",
        "DLQRecord",
        "DLQSink",
        "DedupStoreFailurePolicy",
        "InMemoryCheckpointStore",
        "OnError",
        "FailureClassification",
        "PoisonRecordClassification",
        "PoisonRecordInfo",
        "SQLiteCheckpointStore",
        "SourceRecoveryContractProvider",
        "SourceRecoveryContractSnapshot",
        "SourceRecoveryMode",
        "AGORA_CORE_API_COMPATIBILITY",
        "AGORA_CORE_API_VERSION",
        "AGORA_PLUGIN_MANIFEST_VERSION",
        "AcceptanceFinding",
        "AcceptanceGate",
        "AcceptanceReport",
        "ComponentHealthSnapshot",
        "Configurable",
        "HealthCheckable",
        "Lifecycle",
        "Plugin",
        "Registry",
        "MiddlewareStageExplain",
        "PipelineContext",
        "PipelineExplain",
        "PipelineMetrics",
        "PipelineRunSummary",
        "SinkWriteExplain",
        "AgoraError",
        "ConfigError",
        "PipelineError",
        "PluginError",
        "PluginNotFoundError",
        "PluginValidationError",
        "RegistryError",
    ),
    "agora.core.acceptance": (
        "AcceptanceFinding",
        "AcceptanceGate",
        "AcceptanceReport",
    ),
    "agora.core.health": (
        "ComponentHealthSnapshot",
        "HealthCheckable",
    ),
    "agora.core.failures": (
        "FailureClassification",
        "PoisonRecordClassification",
        "PoisonRecordInfo",
    ),
    "agora.core.recovery": (
        "SourceRecoveryContractProvider",
        "SourceRecoveryContractSnapshot",
        "SourceRecoveryMode",
    ),
    "agora.core.runtime": (
        "SOURCE_QUEUE_DONE",
        "AdaptiveBackpressureController",
        "BufferedStageSpec",
        "CheckpointState",
        "CheckpointedOutcome",
        "CommitOutcome",
        "DeliveryEngine",
        "Dropped",
        "ErroredRouted",
        "ErroredUnrouted",
        "ExecutionCoordinator",
        "HotPathMetrics",
        "PendingWrite",
        "ProcessedSourceRecord",
        "RecordDeliveryError",
        "RunState",
        "RuntimeLane",
        "RuntimePlan",
        "SourceQueueError",
        "SourceRecord",
        "SourceRuntimeAdapter",
        "WriterTransport",
        "Written",
        "build_runtime_plan",
        "make_checkpoint_state",
    ),
    "agora.core.source": (
        "BaseSource",
        "DeliveryHookSource",
        "IterableSource",
        "LimitedSource",
        "PrefetchCapableSource",
        "RuntimeMetricsSource",
        "SourceRecordError",
        "SourceRuntimeMetrics",
        "is_prefetch_capable",
        "prefetch_limit_for",
        "source_data_plane_spec",
        "source_delivery_success_callback",
        "source_runtime_metrics",
    ),
    "agora.core.sink": (
        "BaseSink",
        "BatchWritable",
        "ContextBindable",
        "SinkCapabilities",
        "SinkFanOut",
        "SinkRoute",
        "SinkRouter",
        "WriteResult",
        "bind_context_if_supported",
        "sink_capabilities",
        "sink_data_plane_spec",
        "writer_target_data_plane_specs",
    ),
    "agora.core.middleware": (
        "BatchFilterMiddleware",
        "BatchMapMiddleware",
        "FilterMiddleware",
        "MapMiddleware",
        "Middleware",
        "MiddlewareChain",
        "MiddlewareDataPlane",
        "MiddlewareFailure",
        "MiddlewareModeSpec",
        "MiddlewareProcessResult",
        "PipelinedBatchStageSpec",
        "RetryMiddleware",
        "RouteMiddleware",
    ),
    "agora.core.context": ("PipelineContext",),
    "agora.core.metrics": (
        "AIMetrics",
        "AIMiddlewareMetrics",
        "MiddlewareMetrics",
        "PipelineMetrics",
        "PipelineRunSummary",
        "RuntimeMetrics",
    ),
    "agora.core.tracing": (
        "InMemoryTracer",
        "NoopSpan",
        "NoopTracer",
        "OpenTelemetrySpan",
        "OpenTelemetryTracer",
        "PipelineTracer",
        "RecordedSpan",
        "TraceSpan",
    ),
    "agora.core.session": (
        "PipelineLifecycleController",
        "PipelineRunState",
    ),
    "agora.core.explain": (
        "MiddlewareStageExplain",
        "PipelineExplain",
        "SinkWriteExplain",
    ),
    "agora.core.container": ("AgoraContainer",),
    "agora.core.types": (
        "Backpressure",
        "CheckpointFailurePolicy",
        "DLQFailurePolicy",
        "DedupStoreFailurePolicy",
        "DeliveryConfig",
        "K",
        "OnError",
        "P",
        "PluginFactory",
        "SinkFailurePolicy",
        "SourceKey",
        "SourceRecordFailurePolicy",
        "SqlRow",
        "T",
        "U",
    ),
    "agora.core.writer": (
        "WriteResult",
        "Writer",
    ),
}

COMPAT_EXPORTS: dict[str, frozenset[str]] = {}

DEPRECATED_EXPORTS: dict[str, frozenset[str]] = {}

DISALLOWED_INTERNAL_IMPORT_PATTERNS = (
    re.compile(r"^\s*from\s+agora\.core(?:\.[A-Za-z0-9]+)*\._[A-Za-z0-9_.]+\s+import\b"),
    re.compile(r"^\s*import\s+agora\.core(?:\.[A-Za-z0-9]+)*\._[A-Za-z0-9_.]+\b"),
)


@dataclass(frozen=True, slots=True)
class DeprecatedExportPolicy:
    replacement: str
    retained_through: str
    removal_target: str
    note: str


@dataclass(frozen=True, slots=True)
class CompatExportPolicy:
    retained_through: str
    removal_target: str
    keep_in_0_4: bool
    note: str


DEPRECATED_EXPORT_POLICIES: dict[str, dict[str, DeprecatedExportPolicy]] = {}

COMPAT_EXPORT_POLICIES: dict[str, dict[str, CompatExportPolicy]] = {}


def _markdown_code_blocks(path: Path) -> list[tuple[int, str]]:
    blocks: list[tuple[int, str]] = []
    in_fence = False
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if line.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            blocks.append((lineno, line))
    return blocks


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.add(node.module)

    return modules


def test_public_api_manifests_match_frozen_surface() -> None:
    for module_name, expected_exports in PUBLIC_API_MANIFEST.items():
        module = importlib.import_module(module_name)

        assert tuple(module.__all__) == expected_exports
        assert len(module.__all__) == len(set(module.__all__))


def test_public_api_manifest_exports_exist_on_modules() -> None:
    for module_name, expected_exports in PUBLIC_API_MANIFEST.items():
        module = importlib.import_module(module_name)

        for export_name in expected_exports:
            assert hasattr(module, export_name), (
                f"{module_name} is missing public export {export_name!r} declared in __all__."
            )


def test_public_api_compatibility_exports_are_explicitly_classified() -> None:
    for module_name, expected_exports in PUBLIC_API_MANIFEST.items():
        actual_underscore_exports = {
            name for name in expected_exports if name.startswith("_") and not name.startswith("__")
        }
        expected_compat_exports = COMPAT_EXPORTS.get(module_name, frozenset())

        assert actual_underscore_exports == expected_compat_exports
        assert all(name.startswith("_") for name in expected_compat_exports)

        deprecated_exports = DEPRECATED_EXPORTS.get(module_name, frozenset())
        assert deprecated_exports.issubset(set(expected_exports))


def test_compatibility_exports_have_retention_notes() -> None:
    for module_name, compat_exports in COMPAT_EXPORTS.items():
        policies = COMPAT_EXPORT_POLICIES.get(module_name, {})

        assert compat_exports == set(policies)
        for _export_name, policy in policies.items():
            assert policy.retained_through
            assert policy.removal_target
            assert policy.note


def test_deprecated_exports_have_retention_notes() -> None:
    for module_name, deprecated_exports in DEPRECATED_EXPORTS.items():
        policies = DEPRECATED_EXPORT_POLICIES.get(module_name, {})

        assert deprecated_exports == set(policies)
        for _export_name, policy in policies.items():
            assert policy.replacement
            assert policy.retained_through
            assert policy.removal_target
            assert policy.note


def test_no_compatibility_underscore_exports_are_planned_for_0_4_carryover() -> None:
    for policies in COMPAT_EXPORT_POLICIES.values():
        assert all(policy.keep_in_0_4 is False for policy in policies.values())


def test_docs_code_examples_do_not_import_internal_core_modules() -> None:
    offenders: list[str] = []

    for path in sorted(DOCS_ROOT.rglob("*.md")):
        for lineno, line in _markdown_code_blocks(path):
            stripped = line.strip()
            if not stripped.startswith(("from ", "import ")):
                continue

            for pattern in DISALLOWED_INTERNAL_IMPORT_PATTERNS:
                if pattern.match(stripped):
                    relpath = path.relative_to(DOCS_ROOT.parent)
                    offenders.append(f"{relpath}:{lineno}: {stripped}")
                    break

    assert offenders == []


def test_plugin_sources_do_not_depend_on_internal_core_modules() -> None:
    offenders: list[str] = []

    for root in PLUGIN_SOURCE_ROOTS:
        for path in sorted(root.rglob("*.py")):
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
                stripped = line.strip()
                if not stripped.startswith(("from ", "import ")):
                    continue

                for pattern in DISALLOWED_INTERNAL_IMPORT_PATTERNS:
                    if pattern.match(stripped):
                        relpath = path.relative_to(PACKAGE_ROOT)
                        offenders.append(f"{relpath}:{lineno}: {stripped}")
                        break

    assert offenders == []


def test_builder_layer_does_not_import_runtime_internals_directly() -> None:
    pipeline_imports = _imported_modules(SOURCE_ROOT / "core" / "pipeline.py")
    pipeline_support_imports = _imported_modules(SOURCE_ROOT / "core" / "_pipeline_support.py")

    assert "agora.core.runtime" not in pipeline_imports
    assert not any(
        name == "agora.core.runtime" or name.startswith("agora.core.runtime.")
        for name in pipeline_imports
    )
    assert not any(name.startswith("agora.core.runtime._") for name in pipeline_support_imports)
    assert "agora.core.runtime" in pipeline_support_imports


def test_executor_layer_depends_on_runtime_facade_not_runtime_support_modules() -> None:
    executor_imports = _imported_modules(SOURCE_ROOT / "core" / "executor.py")
    executor_support_imports = _imported_modules(SOURCE_ROOT / "core" / "_executor_support.py")

    assert "agora.core.runtime" in executor_imports
    assert not any(name.startswith("agora.core.runtime._") for name in executor_imports)
    assert "agora.core.runtime" in executor_support_imports
    assert not any(name.startswith("agora.core.runtime._") for name in executor_support_imports)


def test_runtime_facade_does_not_reach_back_into_builder_or_executor_layers() -> None:
    runtime_imports = _imported_modules(SOURCE_ROOT / "core" / "runtime" / "__init__.py")

    assert not any(
        name == "agora.core.pipeline" or name.startswith("agora.core.pipeline.")
        for name in runtime_imports
    )
    assert not any(
        name == "agora.core.executor" or name.startswith("agora.core.executor.")
        for name in runtime_imports
    )
    assert not any(
        name.startswith(("agora.core._pipeline_support", "agora.core._executor_support"))
        for name in runtime_imports
    )
