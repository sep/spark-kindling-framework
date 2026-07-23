"""Unit tests for kindling.notebook_source — cells ↔ python source round-trip."""

from kindling.notebook_source import (
    CELL_SEPARATOR,
    NOTEBOOK_HEADER,
    cells_to_python_source,
    python_source_to_cells,
)


def _code(source):
    return {"cell_type": "code", "metadata": {}, "outputs": [], "source": source}


def _md(source):
    return {"cell_type": "markdown", "metadata": {}, "source": source}


def _texts(cells):
    return [
        (c["cell_type"], "".join(c["source"]) if isinstance(c["source"], list) else c["source"])
        for c in cells
    ]


class TestCellsToPythonSource:
    def test_single_code_cell(self):
        source = cells_to_python_source([_code("print('hi')")])
        assert source == f"{NOTEBOOK_HEADER}\nprint('hi')\n"

    def test_code_cells_are_separated(self):
        source = cells_to_python_source([_code("a = 1"), _code("b = 2")])
        assert source == f"{NOTEBOOK_HEADER}\na = 1\n\n{CELL_SEPARATOR}\n\nb = 2\n"

    def test_markdown_cell_becomes_magic_block(self):
        source = cells_to_python_source([_md("# Title\n\nBody text")])
        assert "# MAGIC %md\n# MAGIC # Title\n# MAGIC\n# MAGIC Body text" in source

    def test_jupyter_keepends_source_lists(self):
        source = cells_to_python_source([_code(["a = 1\n", "b = 2"])])
        assert "a = 1\nb = 2" in source

    def test_empty_cells_are_skipped(self):
        source = cells_to_python_source([_code(""), _code("a = 1"), _md("   ")])
        assert source.count(CELL_SEPARATOR) == 0
        assert "a = 1" in source

    def test_accepts_attribute_style_cells(self):
        class Cell:
            cell_type = "code"
            source = "x = 42"

        assert "x = 42" in cells_to_python_source([Cell()])


class TestPythonSourceToCells:
    def test_header_is_optional(self):
        with_header = python_source_to_cells(f"{NOTEBOOK_HEADER}\na = 1\n")
        without_header = python_source_to_cells("a = 1\n")
        assert _texts(with_header) == _texts(without_header) == [("code", "a = 1")]

    def test_separator_splits_code_cells(self):
        cells = python_source_to_cells(f"a = 1\n\n{CELL_SEPARATOR}\n\nb = 2\n")
        assert _texts(cells) == [("code", "a = 1"), ("code", "b = 2")]

    def test_magic_md_block_becomes_markdown(self):
        source = (
            f"# MAGIC %md\n# MAGIC # Title\n# MAGIC\n# MAGIC Body\n\n{CELL_SEPARATOR}\n\na = 1\n"
        )
        cells = python_source_to_cells(source)
        assert _texts(cells) == [("markdown", "# Title\n\nBody"), ("code", "a = 1")]

    def test_inline_md_marker_content(self):
        cells = python_source_to_cells("# MAGIC %md # Just a title\n")
        assert _texts(cells) == [("markdown", "# Just a title")]

    def test_non_md_magic_stays_code_verbatim(self):
        source = "# MAGIC %sql\n# MAGIC SELECT 1\n"
        cells = python_source_to_cells(source)
        assert _texts(cells) == [("code", "# MAGIC %sql\n# MAGIC SELECT 1")]

    def test_code_cells_have_jupyter_shape(self):
        (cell,) = python_source_to_cells("a = 1\n")
        assert cell["outputs"] == []
        assert cell["execution_count"] is None
        assert cell["source"] == ["a = 1"]


class TestRoundTrip:
    CELLS = [
        _md("# Pipeline\n\nMulti-line markdown\nsurvives the trip"),
        _code("import kindling\n\nx = 1"),
        _code("# MAGIC %sql\n# MAGIC SELECT 1"),
        _code("print(x)"),
    ]

    def test_cells_survive_roundtrip(self):
        source = cells_to_python_source(self.CELLS)
        assert _texts(python_source_to_cells(source)) == _texts(self.CELLS)

    def test_source_roundtrip_is_idempotent(self):
        source = cells_to_python_source(self.CELLS)
        again = cells_to_python_source(python_source_to_cells(source))
        assert again == source
