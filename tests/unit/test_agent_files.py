"""`kindling agent setup` / `repo init --agents`: install the Kindling skill
and the managed instruction block for the selected coding agents only."""

import json

import pytest
from click.testing import CliRunner
from kindling_cli import agent_files
from kindling_cli.cli import cli


def _setup(project, *args):
    return CliRunner().invoke(cli, ["agent", "setup", "--project", str(project), *args])


def test_parse_agents():
    assert agent_files.parse_agents("all") == ["claude", "codex", "copilot"]
    assert agent_files.parse_agents("none") == []
    assert agent_files.parse_agents("copilot, CLAUDE") == ["claude", "copilot"]
    with pytest.raises(ValueError, match="unknown: cursor"):
        agent_files.parse_agents("claude,cursor")


@pytest.mark.parametrize(
    "agents,skill_dirs",
    [
        (["claude"], [".claude/skills/kindling"]),
        (["codex"], [".agents/skills/kindling"]),
        (["copilot"], [".github/skills/kindling"]),
        # Copilot reads .claude/ and .agents/ skills, so no third copy.
        (["claude", "copilot"], [".claude/skills/kindling"]),
        (["claude", "codex", "copilot"], [".claude/skills/kindling", ".agents/skills/kindling"]),
    ],
)
def test_skill_directories_per_selection(agents, skill_dirs):
    assert [str(p) for p in agent_files.skill_dirs_for(agents)] == skill_dirs


def test_setup_installs_only_selected_agents(tmp_path):
    result = _setup(tmp_path, "--agents", "claude,copilot")

    assert result.exit_code == 0, result.output
    skill = tmp_path / ".claude/skills/kindling"
    assert (skill / "SKILL.md").read_text().startswith("---\nname: kindling\n")
    assert (skill / "references" / "entities.md").is_file()
    assert "Installed by `kindling agent setup`" in (skill / "SKILL.md").read_text()
    assert not (skill / "project_instructions.md").exists()
    assert (tmp_path / "CLAUDE.md").is_file()
    assert (tmp_path / ".github/copilot-instructions.md").is_file()
    assert not (tmp_path / ".agents").exists()
    assert not (tmp_path / ".github/skills").exists()
    assert not (tmp_path / "AGENTS.md").exists()
    state = json.loads((tmp_path / ".kindling-agent.json").read_text())
    assert state["agents"] == ["claude", "copilot"]


def test_instruction_block_preserves_the_teams_own_text(tmp_path):
    (tmp_path / "CLAUDE.md").write_text("# Team notes\n\nUse feature branches.\n")

    assert _setup(tmp_path, "--agents", "claude").exit_code == 0
    first = (tmp_path / "CLAUDE.md").read_text()
    assert first.startswith("# Team notes\n\nUse feature branches.\n")
    assert first.count(agent_files.KINDLING_BLOCK_BEGIN) == 1

    # Re-running replaces the block in place, never duplicates it.
    assert _setup(tmp_path).exit_code == 0
    assert (tmp_path / "CLAUDE.md").read_text() == first


def test_dropping_an_agent_removes_only_what_setup_wrote(tmp_path):
    (tmp_path / "AGENTS.md").write_text("# Ours\n")
    assert _setup(tmp_path, "--agents", "all").exit_code == 0
    assert (tmp_path / ".agents/skills/kindling/SKILL.md").is_file()

    result = _setup(tmp_path, "--agents", "claude")

    assert result.exit_code == 0, result.output
    assert not (tmp_path / ".agents/skills/kindling").exists()
    assert (tmp_path / "AGENTS.md").read_text() == "# Ours\n"  # block removed, text kept
    assert not (tmp_path / ".github/copilot-instructions.md").exists()  # was only the block
    assert (tmp_path / ".claude/skills/kindling/SKILL.md").is_file()


def test_saved_selection_is_reused_including_none(tmp_path):
    assert _setup(tmp_path, "--agents", "none").exit_code == 0
    assert _setup(tmp_path).exit_code == 0
    assert not (tmp_path / ".claude").exists()
    assert not (tmp_path / "CLAUDE.md").exists()

    assert _setup(tmp_path, "--agents", "codex").exit_code == 0
    assert _setup(tmp_path).exit_code == 0
    assert (tmp_path / "AGENTS.md").is_file()
    assert not (tmp_path / "CLAUDE.md").exists()


def test_check_reports_drift_and_exits_nonzero(tmp_path):
    assert _setup(tmp_path, "--agents", "claude").exit_code == 0
    assert _setup(tmp_path, "--check").exit_code == 0

    skill_md = tmp_path / ".claude/skills/kindling/SKILL.md"
    skill_md.write_text(skill_md.read_text() + "\nlocal edit\n")
    result = _setup(tmp_path, "--check")

    assert result.exit_code != 0
    assert "would write   .claude/skills/kindling" in result.output
    assert "local edit" in skill_md.read_text()  # --check never writes


def test_invalid_agents_value_fails(tmp_path):
    result = _setup(tmp_path, "--agents", "claude,cursor")
    assert result.exit_code != 0
    assert "unknown: cursor" in result.output


def test_repo_init_installs_selected_agents(tmp_path):
    result = CliRunner().invoke(
        cli, ["repo", "init", "shop", "--output-dir", str(tmp_path), "--agents", "codex"]
    )

    assert result.exit_code == 0, result.output
    assert (tmp_path / ".agents/skills/kindling/SKILL.md").is_file()
    assert (tmp_path / "AGENTS.md").is_file()
    assert not (tmp_path / "CLAUDE.md").exists()
    devcontainer = (tmp_path / ".devcontainer/devcontainer.json").read_text()
    assert "kindling env bootstrap && ./.venv/bin/kindling agent setup" in devcontainer


def test_instruction_block_lists_project_packages_and_apps(tmp_path):
    (tmp_path / "packages/sales").mkdir(parents=True)
    (tmp_path / "packages/sales/pyproject.toml").write_text("[project]\nname='sales'\n")
    (tmp_path / "apps/daily").mkdir(parents=True)
    (tmp_path / "apps/daily/app.py").write_text("")

    assert _setup(tmp_path, "--agents", "claude").exit_code == 0
    text = (tmp_path / "CLAUDE.md").read_text()
    assert "`packages/sales`" in text and "`apps/daily`" in text


LEGACY = (
    "<!-- Generated by kindling agent setup v0.9.26 — do not edit directly -->\n"
    "<!-- Run `kindling agent setup --force` to regenerate -->\n\n# Kindling Project Context\n"
)


def test_legacy_generated_files_are_migrated(tmp_path):
    for rel in ("CLAUDE.md", "AGENTS.md", ".github/copilot-instructions.md"):
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(LEGACY)
    (tmp_path / ".kindling-agent-version").write_text("0.9.26")

    pending = agent_files.plan(tmp_path, ["claude"], "0.14.0")
    assert "CLAUDE.md" in pending["write"]
    assert {"AGENTS.md", ".github/copilot-instructions.md", ".kindling-agent-version"} <= set(
        pending["remove"]
    )

    assert _setup(tmp_path, "--agents", "claude").exit_code == 0
    claude = (tmp_path / "CLAUDE.md").read_text()
    assert claude.startswith(agent_files.KINDLING_BLOCK_BEGIN)
    assert "--force" not in claude  # the old generated text is gone
    assert not (tmp_path / "AGENTS.md").exists()
    assert not (tmp_path / ".github/copilot-instructions.md").exists()
    assert not (tmp_path / ".kindling-agent-version").exists()
    assert _setup(tmp_path, "--check").exit_code == 0


def test_removing_block_keeps_indentation_of_team_text(tmp_path):
    (tmp_path / "AGENTS.md").write_text("    indented code block\n\nmore\n")
    assert _setup(tmp_path, "--agents", "codex").exit_code == 0
    assert _setup(tmp_path, "--agents", "none").exit_code == 0
    assert (tmp_path / "AGENTS.md").read_text() == "    indented code block\n\nmore\n"


def test_check_reports_missing_saved_selection(tmp_path):
    result = _setup(tmp_path, "--check", "--agents", "none")
    assert result.exit_code != 0
    assert "would write   .kindling-agent.json" in result.output


def test_repo_init_rejects_bad_agents_before_writing(tmp_path):
    target = tmp_path / "shop"
    result = CliRunner().invoke(
        cli, ["repo", "init", "shop", "--output-dir", str(target), "--agents", "cursor"]
    )
    assert result.exit_code != 0
    assert "unknown: cursor" in result.output
    assert not target.exists() or not any(target.iterdir())


def test_saved_windows_paths_are_normalized_for_cleanup(tmp_path):
    assert _setup(tmp_path, "--agents", "copilot").exit_code == 0
    state_path = tmp_path / ".kindling-agent.json"
    state = json.loads(state_path.read_text())
    state["installed"] = [p.replace("/", "\\") for p in state["installed"]]
    state_path.write_text(json.dumps(state))

    assert _setup(tmp_path, "--agents", "claude").exit_code == 0
    assert not (tmp_path / ".github/skills/kindling").exists()
    assert not (tmp_path / ".github/copilot-instructions.md").exists()
    assert all("\\" not in p for p in json.loads(state_path.read_text())["installed"])


def test_setup_none_on_new_directory_saves_selection(tmp_path):
    target = tmp_path / "new-project"
    result = _setup(target, "--agents", "none")
    assert result.exit_code == 0, result.output
    assert json.loads((target / ".kindling-agent.json").read_text())["agents"] == []
