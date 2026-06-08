from __future__ import annotations

import argparse
import json
from unittest.mock import MagicMock, patch

from agora.cli.commands.plugins import _ALL_KINDS, PluginsCommand
from agora.core.discovery import public_entrypoint_group_contracts


class _FakeConsole:
    def __init__(self) -> None:
        self.outs: list[str] = []
        self.tables: list[dict[str, list[dict[str, object]]]] = []

    def out(self, message: str) -> None:
        self.outs.append(message)

    def plugins_table(self, data: dict[str, list[dict[str, object]]]) -> None:
        self.tables.append(data)


def test_all_kinds_match_public_entrypoint_contracts() -> None:
    assert tuple(contract.kind for contract in public_entrypoint_group_contracts()) == _ALL_KINDS


def test_plugins_command_parser_accepts_runner_kind() -> None:
    cmd = PluginsCommand()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    sub = subparsers.add_parser("plugins")
    cmd.setup_parser(sub)

    args = sub.parse_args(["list", "--kind", "runner"])
    assert args.kind == "runner"


def test_plugins_command_json_output_matches_golden_contract() -> None:
    cmd = PluginsCommand()
    args = argparse.Namespace(subcommand="list", kind="runner", as_json=True)
    ctx = MagicMock()
    fake_console = _FakeConsole()
    collected = {
        "runner": [
            {
                "key": "worker_pool",
                "category": "runner",
                "group": "agora.runner",
                "registry": "runner_registry",
                "stability": "stable",
                "type": "instance",
                "origin": "manual",
                "package": "",
                "version": "",
                "manifest": "",
                "compatibility": "n/a",
                "capabilities": [],
                "extra": "agora-etl",
            }
        ]
    }

    with (
        patch("agora.cli.commands.plugins._collect", return_value=collected),
        patch("agora.cli.commands.plugins.console", fake_console),
    ):
        exit_code = cmd.execute(args, ctx)

    assert exit_code == 0
    assert fake_console.tables == []
    assert len(fake_console.outs) == 1
    payload = json.loads(fake_console.outs[0])
    assert payload == collected


def test_plugins_command_table_output_uses_all_requested_groups() -> None:
    cmd = PluginsCommand()
    args = argparse.Namespace(subcommand="list", kind=None, as_json=False)
    ctx = MagicMock()
    fake_console = _FakeConsole()
    collected = {
        "source": [{"key": "csv"}],
        "runner": [{"key": "worker_pool"}],
    }

    with (
        patch("agora.cli.commands.plugins._collect", return_value=collected),
        patch("agora.cli.commands.plugins.console", fake_console),
    ):
        exit_code = cmd.execute(args, ctx)

    assert exit_code == 0
    assert fake_console.outs == []
    assert fake_console.tables == [collected]
