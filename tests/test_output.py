"""Tests for output handling."""

import base64
from pathlib import Path


from qkernel.output import (
    MIME_TO_EXT,
    clear_file_cache,
    get_cache_dir,
    get_cell_cache_dir,
    get_file_cache_dir,
    save_image,
)
from qkernel.parser import CodeCell


class TestCacheDirectories:
    """Tests for cache directory functions."""

    def test_get_cache_dir(self):
        """Test that cache dir is in home directory."""
        cache_dir = get_cache_dir()

        assert cache_dir == Path.home() / ".cache" / "qkernel"

    def test_get_file_cache_dir(self):
        """Test file-specific cache directory."""
        file_cache = get_file_cache_dir("myfile")

        assert file_cache == get_cache_dir() / "myfile"

    def test_get_cell_cache_dir_with_label(self):
        """Test cell cache directory uses label when available."""
        cell = CodeCell(
            index=0, label="mycell", language="python", source="", line_number=1
        )
        cell_cache = get_cell_cache_dir("myfile", cell)

        assert cell_cache == get_cache_dir() / "myfile" / "mycell"

    def test_get_cell_cache_dir_without_label(self):
        """Test cell cache directory uses index when no label."""
        cell = CodeCell(
            index=5, label=None, language="python", source="", line_number=1
        )
        cell_cache = get_cell_cache_dir("myfile", cell)

        assert cell_cache == get_cache_dir() / "myfile" / "5"


class TestClearFileCache:
    """Tests for clear_file_cache function."""

    def test_clear_creates_empty_directory(self, clean_cache):
        """Test that clear creates an empty directory."""
        clear_file_cache("testfile")

        file_cache = get_file_cache_dir("testfile")
        assert file_cache.exists()
        assert file_cache.is_dir()
        assert list(file_cache.iterdir()) == []

    def test_clear_removes_existing_files(self, clean_cache):
        """Test that clear removes existing files."""
        file_cache = get_file_cache_dir("testfile")
        file_cache.mkdir(parents=True, exist_ok=True)

        # Create some files
        (file_cache / "output.png").write_text("fake image")
        (file_cache / "subdir").mkdir()
        (file_cache / "subdir" / "file.txt").write_text("content")

        clear_file_cache("testfile")

        assert file_cache.exists()
        assert list(file_cache.iterdir()) == []


class TestSaveImage:
    """Tests for save_image function."""

    def test_save_base64_png(self, clean_cache, tmp_path):
        """Test saving a base64-encoded PNG."""
        # Create a tiny valid PNG (1x1 red pixel)
        png_data = base64.b64encode(
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
            b"\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde"
            b"\x00\x00\x00\x0cIDATx\x9cc\xf8\xcf\xc0\x00\x00\x00"
            b"\x03\x00\x01\x00\x05\xfe\xd4\x00\x00\x00\x00IEND\xaeB`\x82"
        ).decode()

        output_path = tmp_path / "test.png"
        save_image(png_data, "image/png", output_path)

        assert output_path.exists()
        assert output_path.read_bytes().startswith(b"\x89PNG")

    def test_save_svg(self, clean_cache, tmp_path):
        """Test saving an SVG (text format)."""
        svg_data = '<svg xmlns="http://www.w3.org/2000/svg"><circle r="10"/></svg>'

        output_path = tmp_path / "test.svg"
        save_image(svg_data, "image/svg+xml", output_path)

        assert output_path.exists()
        assert "<svg" in output_path.read_text()

    def test_save_creates_parent_directories(self, clean_cache, tmp_path):
        """Test that save_image creates parent directories."""
        output_path = tmp_path / "deep" / "nested" / "dir" / "image.png"

        png_data = base64.b64encode(b"fake png data").decode()
        save_image(png_data, "image/png", output_path)

        assert output_path.exists()


class TestMimeTypeMapping:
    """Tests for MIME type to extension mapping."""

    def test_common_image_types(self):
        """Test that common image types are mapped."""
        assert MIME_TO_EXT["image/png"] == "png"
        assert MIME_TO_EXT["image/jpeg"] == "jpg"
        assert MIME_TO_EXT["image/svg+xml"] == "svg"
        assert MIME_TO_EXT["image/gif"] == "gif"
