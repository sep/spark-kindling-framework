"""Install the Kindling agent skill and instruction files into a project.

The skill (``agent_skill/``: SKILL.md plus references/) ships inside this
package, so a project gets the guidance that matches the Kindling CLI it
pins. Each supported coding agent reads skills and always-loaded
instructions from its own locations:

=========  ==========================  ===================================
agent      skill directory             always-loaded instructions
=========  ==========================  ===================================
claude     .claude/skills/kindling     CLAUDE.md
codex      .agents/skills/kindling     AGENTS.md
copilot    .github/skills/kindling     .github/copilot-instructions.md
=========  ==========================  ===================================

Copilot also reads ``.claude/skills`` and ``.agents/skills``, so its own
skill directory is only written when neither claude nor codex is selected;
otherwise Copilot would see the skill twice.

Instruction files may hold the team's own text: Kindling owns only the block
between ``KINDLING_BLOCK_BEGIN`` and ``KINDLING_BLOCK_END``. Skill
directories are owned outright. What was installed is recorded in
``.kindling-agent.json`` so a later run with fewer agents removes exactly
what this module wrote, and reuses the saved selection by default.
"""

import json
import re
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

AGENTS: Tuple[str, ...] = ("claude", "codex", "copilot")
STATE_FILE = ".kindling-agent.json"
SKILL_NAME = "kindling"
SKILL_SOURCE = Path(__file__).parent / "agent_skill"
KINDLING_BLOCK_BEGIN = (
    "<!-- kindling:begin (managed by `kindling agent setup`; edits inside are replaced) -->"
)
KINDLING_BLOCK_END = "<!-- kindling:end -->"

_SKILL_DIRS = {
    "claude": Path(".claude/skills") / SKILL_NAME,
    "codex": Path(".agents/skills") / SKILL_NAME,
    "copilot": Path(".github/skills") / SKILL_NAME,
}
_INSTRUCTION_FILES = {
    "claude": Path("CLAUDE.md"),
    "codex": Path("AGENTS.md"),
    "copilot": Path(".github/copilot-instructions.md"),
}
_BLOCK_RE = re.compile(
    re.escape(KINDLING_BLOCK_BEGIN) + r".*?" + re.escape(KINDLING_BLOCK_END) + r"\n?",
    re.S,
)


def parse_agents(value: str) -> List[str]:
    """Parse ``--agents``: a comma-separated subset of AGENTS, ``all`` or ``none``."""
    tokens = [t.strip().lower() for t in value.split(",") if t.strip()]
    if tokens == ["all"]:
        return list(AGENTS)
    if tokens == ["none"]:
        return []
    unknown = sorted(set(tokens) - set(AGENTS))
    if unknown or not tokens:
        raise ValueError(
            f"--agents takes a comma-separated list of {', '.join(AGENTS)}, or all / none"
            + (f"; unknown: {', '.join(unknown)}" if unknown else "")
        )
    return [agent for agent in AGENTS if agent in tokens]


def skill_dirs_for(agents: Iterable[str]) -> List[Path]:
    selected = set(agents)
    dirs = [_SKILL_DIRS[a] for a in ("claude", "codex") if a in selected]
    if "copilot" in selected and not dirs:
        dirs.append(_SKILL_DIRS["copilot"])
    return dirs


def instruction_files_for(agents: Iterable[str]) -> List[Path]:
    selected = set(agents)
    return [_INSTRUCTION_FILES[a] for a in AGENTS if a in selected]


def load_state(project_root: Path) -> Optional[Dict]:
    path = project_root / STATE_FILE
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _discover_project_context(project_root: Path) -> List[str]:
    lines: List[str] = []
    packages = sorted(
        p.name for p in (project_root / "packages").glob("*") if (p / "pyproject.toml").is_file()
    )
    if packages:
        lines.append("Packages: " + ", ".join(f"`packages/{p}`" for p in packages))
    apps = sorted(p.name for p in (project_root / "apps").glob("*") if (p / "app.py").is_file())
    if apps:
        lines.append("Apps: " + ", ".join(f"`apps/{a}`" for a in apps))
    return lines


def _skill_files() -> List[Path]:
    return sorted(
        p
        for p in SKILL_SOURCE.rglob("*")
        if p.is_file() and p.suffix == ".md" and p.name != "project_instructions.md"
    )


def _render_skill_file(source: Path, version: str) -> str:
    text = source.read_text(encoding="utf-8")
    if source.name == "SKILL.md" and text.startswith("---\n"):
        end = text.index("\n---\n", 4) + len("\n---\n")
        return (
            text[:end]
            + f"\n<!-- Installed by `kindling agent setup` from spark-kindling-cli {version}. "
            "Do not edit; rerun it to update. -->\n" + text[end:]
        )
    return text


def render_instruction_block(project_root: Path, version: str) -> str:
    body = (SKILL_SOURCE / "project_instructions.md").read_text(encoding="utf-8").strip()
    context = _discover_project_context(project_root)
    if context:
        body += "\n\n## This project\n\n" + "\n".join(f"- {line}" for line in context)
    return (
        f"{KINDLING_BLOCK_BEGIN}\n"
        f"<!-- spark-kindling-cli {version} -->\n\n"
        f"{body}\n\n"
        f"{KINDLING_BLOCK_END}\n"
    )


def _upsert_block(path: Path, block: str) -> bool:
    existing = path.read_text(encoding="utf-8") if path.is_file() else ""
    if _BLOCK_RE.search(existing):
        updated = _BLOCK_RE.sub(lambda _m: block, existing, count=1)
    elif existing.strip():
        updated = existing.rstrip("\n") + "\n\n" + block
    else:
        updated = block
    if updated == existing:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(updated, encoding="utf-8")
    return True


def _remove_block(path: Path) -> bool:
    if not path.is_file():
        return False
    existing = path.read_text(encoding="utf-8")
    if not _BLOCK_RE.search(existing):
        return False
    remaining = _BLOCK_RE.sub("", existing).strip()
    if remaining:
        path.write_text(remaining + "\n", encoding="utf-8")
    else:
        path.unlink()
    return True


def _expected_skill(version: str) -> Dict[Path, str]:
    return {s.relative_to(SKILL_SOURCE): _render_skill_file(s, version) for s in _skill_files()}


def _installed_skill(target: Path) -> Dict[Path, str]:
    if not target.is_dir():
        return {}
    return {
        p.relative_to(target): p.read_text(encoding="utf-8")
        for p in target.rglob("*")
        if p.is_file()
    }


def _write_skill(target: Path, version: str) -> bool:
    expected = _expected_skill(version)
    if _installed_skill(target) == expected:
        return False
    if target.exists():
        shutil.rmtree(target)
    for relative, content in expected.items():
        path = target / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    return True


def plan(project_root: Path, agents: List[str], version: str) -> Dict[str, List[str]]:
    """What `apply` would change, without writing: {"write": [...], "remove": [...]}."""
    state = load_state(project_root) or {}
    write: List[str] = []
    block = render_instruction_block(project_root, version)
    expected = _expected_skill(version)
    for rel in skill_dirs_for(agents):
        if _installed_skill(project_root / rel) != expected:
            write.append(str(rel))
    for rel in instruction_files_for(agents):
        path = project_root / rel
        existing = path.read_text(encoding="utf-8") if path.is_file() else ""
        match = _BLOCK_RE.search(existing)
        if not match or match.group(0).rstrip("\n") != block.rstrip("\n"):
            write.append(str(rel))
    keep = {str(p) for p in skill_dirs_for(agents)} | {
        str(p) for p in instruction_files_for(agents)
    }
    remove = [p for p in state.get("installed", []) if p not in keep]
    return {"write": write, "remove": remove}


def apply(project_root: Path, agents: List[str], version: str) -> Dict[str, List[str]]:
    """Install for `agents`, remove what an earlier run installed for others,
    and record the result. Returns {"written": [...], "removed": [...]}."""
    state = load_state(project_root) or {}
    written: List[str] = []
    removed: List[str] = []

    skill_dirs = skill_dirs_for(agents)
    instruction_files = instruction_files_for(agents)
    keep = {str(p) for p in skill_dirs} | {str(p) for p in instruction_files}

    for rel in state.get("installed", []):
        if rel in keep:
            continue
        path = project_root / rel
        if rel in {str(p) for p in _SKILL_DIRS.values()} and path.is_dir():
            shutil.rmtree(path)
            removed.append(rel)
        elif rel in {str(p) for p in _INSTRUCTION_FILES.values()} and _remove_block(path):
            removed.append(rel)

    for rel in skill_dirs:
        if _write_skill(project_root / rel, version):
            written.append(str(rel))
    block = render_instruction_block(project_root, version)
    for rel in instruction_files:
        if _upsert_block(project_root / rel, block):
            written.append(str(rel))

    new_state = {"version": version, "agents": agents, "installed": sorted(keep)}
    # Always recorded, even for no agents, so a later run without --agents
    # keeps that choice instead of falling back to all.
    (project_root / STATE_FILE).write_text(json.dumps(new_state, indent=2) + "\n", encoding="utf-8")
    return {"written": written, "removed": removed}
