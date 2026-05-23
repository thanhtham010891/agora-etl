"""
agora/cli/console.py
====================
Shared console output for the agora CLI — powered by Rich.

All user-facing CLI output goes through ``console`` so formatting stays
consistent across all commands.

Machine-readable output (e.g. JSON from ``agora config show``) uses
``console.out()`` which bypasses Rich markup.

Usage::

    from agora.cli.console import console

    console.info("Pipeline started.")
    console.error("Cannot import 'pipelines.foo'.")
    console.section("Registered pipelines")
    console.item("ingest", "every 6h")
"""

from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# Two consoles: stdout for info/warn/header, stderr for errors
_out = Console(highlight=False)
_err = Console(stderr=True, highlight=False)


class _Console:
    """Rich-backed console for the agora CLI."""

    # ------------------------------------------------------------------ #
    # Status messages                                                      #
    # ------------------------------------------------------------------ #

    def info(self, message: str) -> None:
        """Green ✅ status line."""
        _out.print(f"[bold green]✅[/bold green]  {message}")

    def warn(self, message: str) -> None:
        """Yellow ⚠ warning line."""
        _out.print(f"[bold yellow]⚠[/bold yellow]   {message}")

    def error(self, message: str) -> None:
        """Red error panel to stderr."""
        _err.print(
            Panel(
                Text(message, style="red"),
                title="[bold red]Error[/bold red]",
                border_style="red",
                padding=(0, 1),
            )
        )

    def exception(self, message: str, exc: Exception) -> None:
        """Error panel with exception detail."""
        body = Text()
        body.append(message + "\n", style="red")
        body.append(str(exc), style="dim red")
        _err.print(
            Panel(body, title="[bold red]Error[/bold red]", border_style="red", padding=(0, 1))
        )

    def header(self, message: str) -> None:
        """Cyan ▶ header line (worker startup, run start)."""
        _out.print(f"[bold cyan]▶[/bold cyan]  {message}")

    def section(self, title: str) -> None:
        """Horizontal rule with centred title."""
        _out.rule(f"[bold cyan]{title}[/bold cyan]")

    def item(self, *columns: str) -> None:
        """Indented list item (one or more columns)."""
        row = "  [dim]·[/dim]  " + "  [dim]│[/dim]  ".join(columns)
        _out.print(row)

    def blank(self) -> None:
        """Blank line."""
        _out.print()

    def out(self, message: str) -> None:
        """Raw stdout (machine-readable output — no markup)."""
        _out.print(message, markup=False, highlight=False)

    # ------------------------------------------------------------------ #
    # Structured output                                                    #
    # ------------------------------------------------------------------ #

    def pipeline_table(
        self,
        title: str,
        rows: list[tuple[str, ...]],
        headers: tuple[str, ...] = ("Pipeline", "Schedule"),
    ) -> None:
        """Render a Rich table of pipelines."""
        table = Table(
            title=title,
            title_style="bold cyan",
            border_style="cyan",
            header_style="bold",
            show_lines=False,
            expand=False,
            padding=(0, 1),
        )
        for h in headers:
            table.add_column(h)
        for row in rows:
            table.add_row(*row)
        _out.print(table)

    def worker_header(
        self,
        module: str,
        pipelines: list[tuple[str, str]],
        health_port: int | None = None,
        health_host: str = "127.0.0.1",
        health_auth_enabled: bool = False,
    ) -> None:
        """Rich startup panel for ``agora worker``."""
        from rich.align import Align

        lines = Text()
        lines.append("module  ", style="dim")
        lines.append(f"{module}\n", style="bold white")
        lines.append("health  ", style="dim")
        if health_port:
            lines.append(f"http://{health_host}:{health_port}/health\n", style="bold green")
        else:
            lines.append("disabled\n", style="dim")
        lines.append("auth    ", style="dim")
        if health_auth_enabled:
            lines.append("bearer token required\n", style="bold yellow")
        else:
            lines.append("disabled\n", style="dim")
        lines.append("\n")

        for pid, schedule in pipelines:
            lines.append("  · ", style="cyan")
            lines.append(f"{pid:<36}", style="bold white")
            lines.append(schedule, style="dim")
            lines.append("\n")

        _out.print(
            Panel(
                Align(lines, vertical="middle"),
                title="[bold cyan]agora worker[/bold cyan]",
                border_style="cyan",
                padding=(1, 2),
            )
        )

    def run_summary(self, summary: Any) -> None:
        """Print a pipeline run summary panel."""
        from rich.align import Align

        text = Text()
        text.append("consumed  ", style="dim")
        text.append(f"{summary.records_consumed}\n", style="bold white")
        text.append("written   ", style="dim")
        text.append(f"{summary.records_written}\n", style="bold green")
        if summary.records_dropped:
            text.append("dropped   ", style="dim")
            text.append(f"{summary.records_dropped}\n", style="yellow")
        if summary.records_errored:
            text.append("errors    ", style="dim")
            text.append(f"{summary.records_errored}\n", style="red")
        text.append("elapsed   ", style="dim")
        text.append(f"{summary.elapsed_seconds:.1f}s", style="bold white")

        _out.print(
            Panel(
                Align(text, vertical="middle"),
                title="[bold green]✅ Pipeline complete[/bold green]",
                border_style="green",
                padding=(0, 2),
            )
        )

    # ------------------------------------------------------------------ #
    # Top-level help                                                       #
    # ------------------------------------------------------------------ #

    def agora_help(self, commands: list[tuple[str, str]], version: str) -> None:
        """Rich top-level help panel shown when ``agora`` is called with no args."""
        from rich.table import Table

        title = Text()
        title.append("agora ", style="bold cyan")
        title.append(version, style="dim cyan")
        title.append("  —  async ETL framework", style="dim")

        grid = Table.grid(padding=(0, 2))
        grid.add_column(style="dim")  # "agora"
        grid.add_column(style="bold white")  # command name
        grid.add_column(style="dim")  # description

        for name, desc in commands:
            grid.add_row("agora", name, desc)

        usage = Text()
        usage.append("Usage: ", style="dim")
        usage.append("agora ", style="bold cyan")
        usage.append("<command> ", style="bold white")
        usage.append("[options]\n", style="dim")

        footer = Text()
        footer.append("\nRun ", style="dim")
        footer.append("agora <command> --help", style="bold cyan")
        footer.append(" for command-specific options.", style="dim")

        from rich.console import Group

        body = Group(usage, Text("Commands:", style="bold"), grid, footer)

        _out.print(
            Panel(
                body,
                title=title,
                border_style="cyan",
                padding=(1, 2),
            )
        )

    # ------------------------------------------------------------------ #
    # agora new                                                            #
    # ------------------------------------------------------------------ #

    def new_progress(self, rel_path: str) -> None:
        """Show file creation progress during ``agora new``."""
        _out.print(f"  [dim green]✓[/dim green]  [dim]{rel_path}[/dim]")

    def new_success(self, name: str) -> None:
        """Rich success panel after project scaffold."""
        body = Text()
        body.append("Project ", style="dim")
        body.append(f"'{name}'", style="bold white")
        body.append(" created successfully!\n\n", style="dim")
        body.append("Next steps\n", style="bold")
        body.append(f"  cd {name}\n", style="bold cyan")
        body.append("  pip install -e '.[dev]'\n", style="bold cyan")
        body.append("  agora run pipelines.example --dry-run", style="bold cyan")

        _out.print(
            Panel(
                body,
                title="[bold green]✅ Project ready[/bold green]",
                border_style="green",
                padding=(1, 2),
            )
        )

    # ------------------------------------------------------------------ #
    # agora pipelines                                                      #
    # ------------------------------------------------------------------ #

    def pipelines_list(self, modules: list[str]) -> None:
        """Rich table of discoverable pipelines."""
        from rich.table import Table

        table = Table(
            title="[bold cyan]Available pipelines[/bold cyan]",
            border_style="cyan",
            header_style="bold",
            show_lines=False,
            padding=(0, 1),
        )
        table.add_column("Module", style="bold white")
        table.add_column("Run command", style="cyan")
        for m in modules:
            table.add_row(f"pipelines.{m}", f"agora run pipelines.{m}")
        _out.print(table)

    # ------------------------------------------------------------------ #
    # agora version                                                        #
    # ------------------------------------------------------------------ #

    def version_panel(self, version: str) -> None:
        """Print agora version with styling."""
        _out.print(
            f"[bold cyan]agora[/bold cyan] [bold white]{version}[/bold white]  "
            f"[dim]async ETL framework — https://pypi.org/project/agora-etl/dim]"
        )

    # ------------------------------------------------------------------ #
    # agora config                                                         #
    # ------------------------------------------------------------------ #

    def config_json(self, data: str) -> None:
        """Print JSON config with Rich syntax highlighting."""
        from rich.syntax import Syntax

        _out.print(Syntax(data, "json", theme="monokai", background_color="default"))

    # ------------------------------------------------------------------ #
    # agora plugins                                                        #
    # ------------------------------------------------------------------ #

    def plugins_table(self, data: dict[str, list[dict[str, str]]]) -> None:
        """Render all plugins in a single unified Rich table.

        ``data`` format: ``{kind: [{key, type, origin, extra, package, version, compatibility}]}``

        Columns: Key | Category | Status | Package | Version | API | Install
        """
        from rich.text import Text

        kind_styles: dict[str, tuple[str, str]] = {
            "source": ("cyan", "source"),
            "sink": ("magenta", "sink"),
            "middleware": ("yellow", "middleware"),
        }
        status_styles: dict[tuple[str, str], tuple[str, str]] = {
            ("instance", "manual"): ("bold green", "● built-in"),
            ("instance", "entrypoint"): ("bold cyan", "● installed"),
            ("factory", "manual"): ("dim cyan", "○ optional"),
            ("factory", "entrypoint"): ("cyan", "○ lazy plugin"),
        }
        compatibility_styles: dict[str, str] = {
            "ok": "bold green",
            "n/a": "dim",
        }

        table = Table(
            title="[bold cyan]Agora Plugins[/bold cyan]",
            border_style="cyan",
            header_style="bold",
            show_lines=False,
            padding=(0, 1),
        )
        table.add_column("Key", style="bold white", no_wrap=True, min_width=14)
        table.add_column("Category", no_wrap=True, min_width=10)
        table.add_column("Status", no_wrap=True, min_width=12)
        table.add_column("Package", style="dim", no_wrap=True, min_width=16)
        table.add_column("Version", style="dim", no_wrap=True, min_width=8)
        table.add_column("API", no_wrap=True, min_width=6)
        table.add_column("Install", style="dim", no_wrap=True, min_width=24)

        for kind, rows in data.items():
            k_color, k_label = kind_styles.get(kind, ("white", kind))
            for row in rows:
                status_key = (row.get("type", "instance"), row.get("origin", "manual"))
                s_style, s_label = status_styles.get(status_key, ("white", row["type"]))
                compatibility = row.get("compatibility", "n/a")
                table.add_row(
                    row["key"],
                    Text(k_label, style=f"bold {k_color}"),
                    Text(s_label, style=s_style),
                    row.get("package", ""),
                    row.get("version", ""),
                    Text(compatibility, style=compatibility_styles.get(compatibility, "yellow")),
                    row["extra"],
                )

        _out.print(table)
        _out.print()


console = _Console()
