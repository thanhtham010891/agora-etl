from __future__ import annotations

from agora.schema.metrics import SchemaMetrics

# ============================================================================
# SchemaMetrics Tests
# ============================================================================


def test_schema_metrics_defaults() -> None:
    """SchemaMetrics has correct default values."""
    metrics = SchemaMetrics()
    assert metrics.columns_added == 0
    assert metrics.columns_removed == 0
    assert metrics.types_widened == 0
    assert metrics.type_conflicts == 0
    assert metrics.records_observed == 0
    assert metrics.schema_version == 1


def test_schema_metrics_str_minimal() -> None:
    """SchemaMetrics.__str__() shows minimal info when no changes."""
    metrics = SchemaMetrics(records_observed=100)
    result = str(metrics)
    assert "v1" in result
    assert "100 records" in result
    assert "cols" not in result
    assert "widened" not in result


def test_schema_metrics_str_with_columns_added() -> None:
    """SchemaMetrics.__str__() shows columns added."""
    metrics = SchemaMetrics(
        columns_added=3,
        records_observed=50,
        schema_version=2,
    )
    result = str(metrics)
    assert "v2" in result
    assert "50 records" in result
    assert "+3 cols" in result


def test_schema_metrics_str_with_types_widened() -> None:
    """SchemaMetrics.__str__() shows types widened."""
    metrics = SchemaMetrics(
        types_widened=2,
        records_observed=75,
    )
    result = str(metrics)
    assert "75 records" in result
    assert "2 widened" in result


def test_schema_metrics_str_with_conflicts() -> None:
    """SchemaMetrics.__str__() shows type conflicts."""
    metrics = SchemaMetrics(
        type_conflicts=1,
        records_observed=100,
    )
    result = str(metrics)
    assert "100 records" in result
    assert "1 conflicts" in result


def test_schema_metrics_str_full() -> None:
    """SchemaMetrics.__str__() shows all metrics when present."""
    metrics = SchemaMetrics(
        columns_added=5,
        types_widened=3,
        type_conflicts=2,
        records_observed=200,
        schema_version=4,
    )
    result = str(metrics)
    assert "v4" in result
    assert "200 records" in result
    assert "+5 cols" in result
    assert "3 widened" in result
    assert "2 conflicts" in result


def test_schema_metrics_mutable() -> None:
    """SchemaMetrics fields can be updated."""
    metrics = SchemaMetrics()
    metrics.columns_added = 2
    metrics.types_widened = 1
    metrics.records_observed = 50
    metrics.schema_version = 3

    assert metrics.columns_added == 2
    assert metrics.types_widened == 1
    assert metrics.records_observed == 50
    assert metrics.schema_version == 3
