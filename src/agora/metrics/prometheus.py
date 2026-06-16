"""Public helpers for Prometheus-compatible metrics rendering.

Connector packages should import these helpers instead of reaching into
``agora.metrics.exporters`` private support modules.
"""

from agora.metrics.exporters._prometheus_support import (
    append_metric_header,
    escape_label_value,
    exporter_content_type,
    extend_lines,
    render_runtime_lane_line,
    render_runtime_signal_lines,
    render_scrape_time_line,
)

__all__ = [
    "append_metric_header",
    "escape_label_value",
    "exporter_content_type",
    "extend_lines",
    "render_runtime_lane_line",
    "render_runtime_signal_lines",
    "render_scrape_time_line",
]
