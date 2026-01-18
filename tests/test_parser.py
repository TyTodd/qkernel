"""Tests for the QMD parser."""

import pytest

from qkernel.parser import filter_cells, get_file_stem, parse_qmd


class TestParseQmd:
    """Tests for parse_qmd function."""

    def test_parse_simple_qmd(self, simple_qmd):
        """Test parsing a simple QMD file."""
        cells = parse_qmd(simple_qmd)

        assert len(cells) == 4

        # Check first cell
        assert cells[0].index == 0
        assert cells[0].label == "setup"
        assert cells[0].language == "python"
        assert "x = 10" in cells[0].source

        # Check second cell
        assert cells[1].index == 1
        assert cells[1].label == "compute"

        # Check third cell
        assert cells[2].index == 2
        assert cells[2].label == "loop"

        # Check fourth cell (no label)
        assert cells[3].index == 3
        assert cells[3].label is None

    def test_parse_extracts_labels(self, simple_qmd):
        """Test that labels are properly extracted from #| label: comments."""
        cells = parse_qmd(simple_qmd)

        labels = [c.label for c in cells]
        assert labels == ["setup", "compute", "loop", None]

    def test_parse_preserves_source(self, simple_qmd):
        """Test that cell source code is preserved."""
        cells = parse_qmd(simple_qmd)

        # The setup cell should have the full source
        setup_cell = cells[0]
        assert "#| label: setup" in setup_cell.source
        assert "x = 10" in setup_cell.source
        assert "y = 20" in setup_cell.source

    def test_parse_identifies_language(self, simple_qmd):
        """Test that the cell language is correctly identified."""
        cells = parse_qmd(simple_qmd)

        for cell in cells:
            assert cell.language == "python"


class TestFilterCells:
    """Tests for filter_cells function."""

    def test_filter_by_index(self, simple_qmd):
        """Test filtering cells by index."""
        cells = parse_qmd(simple_qmd)
        filtered = filter_cells(cells, ["0", "2"])

        assert len(filtered) == 2
        assert filtered[0].index == 0
        assert filtered[1].index == 2

    def test_filter_by_label(self, simple_qmd):
        """Test filtering cells by label."""
        cells = parse_qmd(simple_qmd)
        filtered = filter_cells(cells, ["setup", "loop"])

        assert len(filtered) == 2
        assert filtered[0].label == "setup"
        assert filtered[1].label == "loop"

    def test_filter_mixed_index_and_label(self, simple_qmd):
        """Test filtering with mixed indices and labels."""
        cells = parse_qmd(simple_qmd)
        filtered = filter_cells(cells, ["0", "compute", "2"])

        assert len(filtered) == 3
        assert filtered[0].index == 0
        assert filtered[1].label == "compute"
        assert filtered[2].index == 2

    def test_filter_preserves_order(self, simple_qmd):
        """Test that filtered cells are in the order specified."""
        cells = parse_qmd(simple_qmd)
        filtered = filter_cells(cells, ["loop", "setup"])

        assert len(filtered) == 2
        assert filtered[0].label == "loop"
        assert filtered[1].label == "setup"

    def test_filter_none_returns_all(self, simple_qmd):
        """Test that None selector returns all cells."""
        cells = parse_qmd(simple_qmd)
        filtered = filter_cells(cells, None)

        assert len(filtered) == len(cells)

    def test_filter_invalid_index_raises(self, simple_qmd):
        """Test that invalid index raises ValueError."""
        cells = parse_qmd(simple_qmd)

        with pytest.raises(ValueError, match="not found"):
            filter_cells(cells, ["99"])

    def test_filter_invalid_label_raises(self, simple_qmd):
        """Test that invalid label raises ValueError."""
        cells = parse_qmd(simple_qmd)

        with pytest.raises(ValueError, match="not found"):
            filter_cells(cells, ["nonexistent"])


class TestGetFileStem:
    """Tests for get_file_stem function."""

    def test_removes_qmd_extension(self):
        """Test that .qmd extension is removed."""
        assert get_file_stem("test.qmd") == "test"
        assert get_file_stem("/path/to/notebook.qmd") == "notebook"

    def test_handles_path_object(self, simple_qmd):
        """Test that Path objects are handled."""
        stem = get_file_stem(simple_qmd)
        assert stem == "simple"
