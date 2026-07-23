"""Round-trip conversion between notebook cells and python source files.

The source format is the Databricks "SOURCE" notebook layout, which the
standalone platform also uses for local workspace files: cells separated by
``# COMMAND ----------`` lines, markdown cells encoded as ``# MAGIC`` comment
blocks starting with ``# MAGIC %md``. It is git-friendly (reviewable, plain
python) and survives a pull → edit → push cycle without losing cell
structure.

Cell dicts use the Jupyter shape (``cell_type``, ``source`` as a list of
keepends lines, ``metadata``; code cells also carry ``outputs`` and
``execution_count``) so they can be embedded directly in an ``.ipynb``
document or a Synapse notebook resource. ``cells_to_python_source`` also
accepts objects with ``cell_type``/``source`` attributes (e.g. the
``NotebookCell`` class in ``kindling.notebook_framework``).

Outputs and execution counts are intentionally dropped on conversion to
source: the round-trip preserves code and markdown, not run results.
"""

from __future__ import annotations

from typing import Any, Dict, List

NOTEBOOK_HEADER = "# Databricks notebook source"
CELL_SEPARATOR = "# COMMAND ----------"
_MAGIC_PREFIX = "# MAGIC "
_MAGIC_BARE = "# MAGIC"
_MD_MARKER = "# MAGIC %md"


def _cell_attr(cell: Any, name: str, default: Any) -> Any:
    if isinstance(cell, dict):
        return cell.get(name, default)
    return getattr(cell, name, default)


def _source_text(source: Any) -> str:
    if isinstance(source, list):
        return "".join(str(line) for line in source)
    return str(source or "")


def _code_cell(text: str) -> Dict[str, Any]:
    return {
        "cell_type": "code",
        "metadata": {},
        "outputs": [],
        "execution_count": None,
        "source": text.splitlines(keepends=True),
    }


def _markdown_cell(text: str) -> Dict[str, Any]:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": text.splitlines(keepends=True),
    }


def cells_to_python_source(cells: List[Any]) -> str:
    """Render notebook cells as Databricks-style python source."""
    blocks: List[str] = []
    for cell in cells:
        cell_type = _cell_attr(cell, "cell_type", "code")
        text = _source_text(_cell_attr(cell, "source", "")).rstrip("\n")
        if not text.strip():
            continue
        if cell_type == "markdown":
            lines = [_MD_MARKER]
            for line in text.split("\n"):
                lines.append(f"{_MAGIC_PREFIX}{line}".rstrip())
            blocks.append("\n".join(lines))
        else:
            blocks.append(text)

    separator = f"\n\n{CELL_SEPARATOR}\n\n"
    return NOTEBOOK_HEADER + "\n" + separator.join(blocks) + "\n"


def python_source_to_cells(source: str) -> List[Dict[str, Any]]:
    """Parse Databricks-style python source into notebook cells.

    The header line is optional. Blocks starting with ``# MAGIC %md`` become
    markdown cells; every other ``# MAGIC`` block (``%sql``, ``%run``, …) is
    preserved verbatim as a code cell so unknown magics round-trip unchanged.
    """
    lines = source.split("\n")
    if lines and lines[0].strip() == NOTEBOOK_HEADER:
        lines = lines[1:]

    blocks: List[List[str]] = [[]]
    for line in lines:
        if line.strip().startswith(CELL_SEPARATOR):
            blocks.append([])
        else:
            blocks[-1].append(line)

    cells: List[Dict[str, Any]] = []
    for block in blocks:
        while block and not block[0].strip():
            block.pop(0)
        while block and not block[-1].strip():
            block.pop()
        if not block:
            continue

        if block[0].strip().startswith(_MD_MARKER):
            md_lines: List[str] = []
            inline = block[0].strip()[len(_MD_MARKER) :].strip()
            if inline:
                md_lines.append(inline)
            for line in block[1:]:
                if line.startswith(_MAGIC_PREFIX):
                    md_lines.append(line[len(_MAGIC_PREFIX) :])
                elif line.strip() == _MAGIC_BARE:
                    md_lines.append("")
                else:
                    md_lines.append(line)
            cells.append(_markdown_cell("\n".join(md_lines)))
        else:
            cells.append(_code_cell("\n".join(block)))

    return cells
