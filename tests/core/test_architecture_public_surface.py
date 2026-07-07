from __future__ import annotations

import ast
import importlib
import re
import warnings
from dataclasses import dataclass
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[2]
DOCS_ROOT = PACKAGE_ROOT / "docs"
SOURCE_ROOT = PACKAGE_ROOT / "src" / "agora"
PLUGIN_SOURCE_ROOTS = tuple(sorted((PACKAGE_ROOT / "plugins").glob("*/src")))
PLUGIN_FAMILY_DOCS = (
    DOCS_ROOT / "plugins" / "redis.md",
    DOCS_ROOT / "plugins" / "kafka.md",
    DOCS_ROOT / "plugins" / "postgresql.md",
    DOCS_ROOT / "plugins" / "bigquery.md",
)
PUBLIC_DOCS_WORDING_GUARD_PATHS = (
    PACKAGE_ROOT / "README.md",
    PACKAGE_ROOT.parent / "agora-plugins" / "README.md",
    DOCS_ROOT / "source-of-truth.md",
    DOCS_ROOT / "plugins" / "index.md",
    DOCS_ROOT / "plugins" / "official-bundle.md",
    DOCS_ROOT / "plugins" / "production-readiness.md",
    *PLUGIN_FAMILY_DOCS,
)

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
        "DLQPayloadPolicy",
        "DLQRecord",
        "DLQSink",
        "DedupStoreFailurePolicy",
        "InMemoryCheckpointStore",
        "OnError",
        "FailureClassification",
        "PoisonRecordClassification",
        "PoisonRecordInfo",
        "PoisonRecordPolicy",
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
        "AcceptanceReportProvider",
        "ComponentHealthSnapshot",
        "Configurable",
        "HealthCheckable",
        "MetricsSnapshotProvider",
        "PrometheusMetricsProvider",
        "Lifecycle",
        "Plugin",
        "Registry",
        "MiddlewareStageExplain",
        "PipelineContext",
        "PipelineExplain",
        "PipelineMetrics",
        "PipelineRunSummary",
        "SinkWriteExplain",
        "has_acceptance_report",
        "has_metrics_snapshot",
        "has_prometheus_metrics",
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
        "AcceptanceReportProvider",
        "has_acceptance_report",
    ),
    "agora.core.dlq_policy": ("DLQPayloadPolicy",),
    "agora.core.health": (
        "ComponentHealthSnapshot",
        "HealthCheckable",
    ),
    "agora.core.failures": (
        "FailureClassification",
        "PoisonRecordClassification",
        "PoisonRecordInfo",
        "PoisonRecordPolicy",
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
        "BufferedSubmitMiddleware",
        "DrainableBufferedMiddleware",
        "DrainablePipelinedBatchMiddleware",
        "FilterMiddleware",
        "MapMiddleware",
        "Middleware",
        "MiddlewareChain",
        "MiddlewareDataPlane",
        "MiddlewareFailure",
        "MiddlewareModeSpec",
        "MiddlewareProcessResult",
        "PipeableMiddleware",
        "PipelinedBatchMiddleware",
        "PipelinedBatchStageSpec",
        "RetryMiddleware",
        "RouteMiddleware",
        "is_buffered_submit_middleware",
        "is_drainable_buffered_middleware",
        "is_drainable_pipelined_batch_middleware",
        "is_pipelined_batch_middleware",
    ),
    "agora.core.context": ("PipelineContext",),
    "agora.core.doctor": (
        "DOCTOR_READINESS_ENTRYPOINT_GROUP",
        "CheckResult",
        "DoctorReadinessProvider",
        "DoctorReadinessProviderEntry",
        "DoctorReport",
        "Status",
        "discover_doctor_readiness_providers",
    ),
    "agora.core.metrics": (
        "AIMetrics",
        "AIMiddlewareMetrics",
        "MetricsSnapshotProvider",
        "MiddlewareMetrics",
        "PipelineMetrics",
        "PipelineRunSummary",
        "PrometheusMetricsProvider",
        "RuntimeMetrics",
        "has_metrics_snapshot",
        "has_prometheus_metrics",
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

DEPRECATED_EXPORTS: dict[str, frozenset[str]] = {
    "agora": frozenset(
        {
            "BaseSink",
            "BaseSource",
            "CheckpointStore",
            "InMemoryCheckpointStore",
            "Registry",
            "SourceRecordError",
            "SourceRuntimeMetrics",
            "SQLiteCheckpointStore",
            "WriteResult",
            "Writer",
            "state_backend_registry",
        }
    )
}

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


DEPRECATED_EXPORT_POLICIES: dict[str, dict[str, DeprecatedExportPolicy]] = {
    "agora": {
        "BaseSink": DeprecatedExportPolicy(
            replacement="agora.core.sink.BaseSink",
            retained_through="0.4.x",
            removal_target="0.5.0",
            note="Sink base-class contract belongs to the sink domain facade for extension-author usage.",
        ),
        "BaseSource": DeprecatedExportPolicy(
            replacement="agora.core.source.BaseSource",
            retained_through="0.4.x",
            removal_target="0.5.0",
            note="Source base-class contract belongs to the source domain facade for extension-author usage.",
        ),
        "CheckpointStore": DeprecatedExportPolicy(
            replacement="agora.core.checkpoint.CheckpointStore",
            retained_through="0.4.x",
            removal_target="0.5.0",
            note="Checkpoint store contract belongs to the framework checkpoint facade.",
        ),
        "InMemoryCheckpointStore": DeprecatedExportPolicy(
            replacement="agora.core.checkpoint.InMemoryCheckpointStore",
            retained_through="0.4.x",
            removal_target="0.5.0",
            note="In-memory checkpoint implementation is no longer part of the preferred root onboarding story.",
        ),
        "Registry": DeprecatedExportPolicy(
            replacement="agora.core.registry.Registry",
            retained_through="0.4.x",
            removal_target="0.5.0",
            note="Registry contract belongs to the dedicated core registry facade, not the builder-first root facade.",
        ),
        "SourceRecordError": DeprecatedExportPolicy(
            replacement="agora.core.source.SourceRecordError",
            retained_through="0.4.x",
            removal_target="0.5.0",
            note="Source per-record error contract belongs to the source domain facade.",
        ),
        "SourceRuntimeMetrics": DeprecatedExportPolicy(
            replacement="agora.core.source.SourceRuntimeMetrics",
            retained_through="0.4.x",
            removal_target="0.5.0",
            note="Source runtime counters belong to the source domain facade for extension-author diagnostics.",
        ),
        "SQLiteCheckpointStore": DeprecatedExportPolicy(
            replacement="agora.core.checkpoint.SQLiteCheckpointStore",
            retained_through="0.4.x",
            removal_target="0.5.0",
            note="SQLite checkpoint implementation should be imported from the checkpoint domain facade.",
        ),
        "WriteResult": DeprecatedExportPolicy(
            replacement="agora.core.writer.WriteResult",
            retained_through="0.4.x",
            removal_target="0.5.0",
            note="Writer per-record outcome objects belong to the writer contract facade, not the builder-first root facade.",
        ),
        "Writer": DeprecatedExportPolicy(
            replacement="agora.core.writer.Writer",
            retained_through="0.4.x",
            removal_target="0.5.0",
            note="Writer protocol belongs to the dedicated writer contract facade for sink/runtime integration.",
        ),
        "state_backend_registry": DeprecatedExportPolicy(
            replacement="agora.state.state_backend_registry",
            retained_through="0.4.x",
            removal_target="0.5.0",
            note="State registry belongs to the dedicated state facade, not the builder-first root facade.",
        ),
    }
}

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
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
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


def test_docs_code_examples_do_not_import_soft_deprecated_root_exports() -> None:
    offenders: list[str] = []
    deprecated_names = DEPRECATED_EXPORTS.get("agora", frozenset())
    pending_multiline_import: tuple[Path, int] | None = None

    for path in sorted(DOCS_ROOT.rglob("*.md")):
        for lineno, line in _markdown_code_blocks(path):
            stripped = line.strip()

            if pending_multiline_import is not None:
                relpath = path.relative_to(DOCS_ROOT.parent)
                imported_name = stripped.rstrip(",)").strip()
                if imported_name in deprecated_names:
                    offenders.append(f"{relpath}:{lineno}: from agora import {imported_name}")
                if ")" in stripped:
                    pending_multiline_import = None
                continue

            if not stripped.startswith("from agora import"):
                continue

            relpath = path.relative_to(DOCS_ROOT.parent)
            if stripped == "from agora import (":
                pending_multiline_import = (path, lineno)
                continue

            imported_names = stripped.removeprefix("from agora import").split(",")
            for imported_name in imported_names:
                candidate = imported_name.strip()
                if candidate in deprecated_names:
                    offenders.append(f"{relpath}:{lineno}: {stripped}")

    assert offenders == []


def test_plugin_family_docs_include_maturity_cards() -> None:
    for path in PLUGIN_FAMILY_DOCS:
        text = path.read_text(encoding="utf-8")

        assert "## Maturity card" in text, (
            f"{path.relative_to(PACKAGE_ROOT)} is missing a maturity card"
        )
        assert "Required validation gate" in text, (
            f"{path.relative_to(PACKAGE_ROOT)} is missing the required gate row in its maturity card"
        )


def test_source_of_truth_map_exists_and_links_canonical_boundaries() -> None:
    path = DOCS_ROOT / "source-of-truth.md"
    text = path.read_text(encoding="utf-8")

    assert "Runtime Guarantees" in text
    assert "Plugin Production Readiness" in text
    assert "Plugin Contract" in text
    assert "README policy" in text


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


def test_doctor_command_orchestrates_plugin_readiness_without_direct_plugin_imports() -> None:
    doctor_imports = _imported_modules(SOURCE_ROOT / "cli" / "commands" / "doctor.py")

    assert not any(
        name == "agora_plugins" or name.startswith("agora_plugins.") for name in doctor_imports
    )


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
