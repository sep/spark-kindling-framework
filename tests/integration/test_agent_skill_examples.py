"""The Kindling agent skill's examples must work against the real framework.

The skill (packages/kindling_cli/kindling_cli/agent_skill/) is what coding
agents read before writing entities, pipes, apps and config in a domain
project, so a stale example becomes wrong code in someone's repository.

- Python blocks in each file run in order, in one fresh process with the
  framework initialized (platform standalone), sharing one namespace so later
  blocks can use names from earlier ones. A block whose first line is
  ``# illustrative`` is not run.
- YAML blocks must parse.
- Every ``kindling ...`` command in a bash block must name a real CLI
  command, and each ``--option`` it uses must appear in that command's help.
"""

import json
import re
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner
from kindling_cli.cli import cli

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
SKILL_DIR = REPO_ROOT / "packages" / "kindling_cli" / "kindling_cli" / "agent_skill"
SKILL_FILES = sorted(
    [
        SKILL_DIR / "SKILL.md",
        SKILL_DIR / "project_instructions.md",
        *(SKILL_DIR / "references").glob("*.md"),
    ]
)
# Fences may be indented (e.g. inside a numbered list item).
FENCE = re.compile(r"^([ \t]*)```(\w+)[^\n]*\n(.*?)^[ \t]*```", re.S | re.M)
ILLUSTRATIVE = "# illustrative"


def _blocks(path: Path, language: str):
    return [
        textwrap.dedent(body)
        for _indent, lang, body in FENCE.findall(path.read_text(encoding="utf-8"))
        if lang == language
    ]


def _ids(paths):
    return [str(p.relative_to(SKILL_DIR)) for p in paths]


def test_skill_files_exist():
    assert (SKILL_DIR / "SKILL.md").is_file()
    assert len(SKILL_FILES) > 1


@pytest.mark.parametrize("path", SKILL_FILES, ids=_ids(SKILL_FILES))
def test_python_examples_run(path, tmp_path):
    blocks = [b for b in _blocks(path, "python") if not b.lstrip().startswith(ILLUSTRATIVE)]
    if not blocks:
        pytest.skip("no runnable python examples")
    (tmp_path / "settings.yaml").write_text(
        "kindling:\n  telemetry:\n    logging:\n      level: WARN\n", encoding="utf-8"
    )
    runner = textwrap.dedent("""
        import json, sys, traceback
        from kindling.bootstrap import initialize_framework
        initialize_framework(
            {"platform": "standalone", "environment": "local", "config_dir": "."}
        )
        blocks = json.loads(sys.stdin.read())
        namespace = {"__name__": "skill_example"}
        for index, block in enumerate(blocks):
            try:
                exec(compile(block, f"<block {index + 1}>", "exec"), namespace)
            except Exception:
                print(f"BLOCK {index + 1} FAILED:\\n{block}", file=sys.stderr)
                traceback.print_exc()
                sys.exit(1)
        print("ALL BLOCKS OK")
        """)
    result = subprocess.run(
        [sys.executable, "-c", runner],
        input=json.dumps(blocks),
        capture_output=True,
        text=True,
        cwd=tmp_path,
        timeout=600,
        env={
            **__import__("os").environ,
            "PYTHONPATH": str(REPO_ROOT / "packages"),
        },
    )
    assert "ALL BLOCKS OK" in result.stdout, result.stderr[-6000:]


@pytest.mark.parametrize("path", SKILL_FILES, ids=_ids(SKILL_FILES))
def test_yaml_examples_parse(path):
    for block in _blocks(path, "yaml"):
        yaml.safe_load(block)


def _kindling_commands(path: Path):
    commands = []
    for block in _blocks(path, "bash"):
        joined = re.sub(r"\\\n\s*", " ", block)
        for line in joined.splitlines():
            line = line.split("#", 1)[0].strip()
            match = re.search(r"(?:^|&&\s*|uv run\s+)kindling\s+(.*)$", line)
            if match:
                commands.append(match.group(1).split())
    return commands


def _resolve(tokens):
    """Walk click groups to the command a token list names; return
    (command_path, remaining_tokens)."""
    command, path = cli, []
    for token in tokens:
        sub = getattr(command, "commands", {}).get(token)
        if sub is None:
            break
        command, path = sub, path + [token]
    return path, tokens[len(path) :]


@pytest.mark.parametrize("path", SKILL_FILES, ids=_ids(SKILL_FILES))
def test_cli_commands_exist(path):
    runner = CliRunner()
    for tokens in _kindling_commands(path):
        command_path, rest = _resolve(tokens)
        command = cli
        for name in command_path:
            command = command.commands[name]
        assert command_path and not hasattr(
            command, "commands"
        ), f"unknown or incomplete kindling command: kindling {' '.join(tokens)}"
        help_result = runner.invoke(cli, [*command_path, "--help"])
        assert help_result.exit_code == 0, help_result.output
        for option in (t.split("=", 1)[0] for t in rest if t.startswith("--")):
            assert (
                option in help_result.output
            ), f"kindling {' '.join(command_path)} has no option {option}"
