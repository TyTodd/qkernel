"""Pytest configuration and fixtures for qkernel tests."""

import shutil
from pathlib import Path

import pytest


@pytest.fixture
def artifacts_dir() -> Path:
    """Return the path to the test artifacts directory."""
    return Path(__file__).parent / "artifacts"


@pytest.fixture
def simple_qmd(artifacts_dir) -> Path:
    """Return the path to the simple test QMD file."""
    return artifacts_dir / "simple.qmd"


@pytest.fixture
def image_qmd(artifacts_dir) -> Path:
    """Return the path to the QMD file with image output."""
    return artifacts_dir / "with_image.qmd"


@pytest.fixture
def error_qmd(artifacts_dir) -> Path:
    """Return the path to the QMD file with an error cell."""
    return artifacts_dir / "error_cell.qmd"


@pytest.fixture
def multi_image_qmd(artifacts_dir) -> Path:
    """Return the path to the QMD file with multiple images."""
    return artifacts_dir / "multi_image.qmd"


@pytest.fixture
def mixed_output_qmd(artifacts_dir) -> Path:
    """Return the path to the QMD file with mixed output types."""
    return artifacts_dir / "mixed_output.qmd"


@pytest.fixture
def long_running_qmd(artifacts_dir) -> Path:
    """Return the path to the QMD file with a long-running cell."""
    return artifacts_dir / "long_running.qmd"


@pytest.fixture
def clean_cache():
    """Clean up the qkernel cache directory before and after tests."""
    cache_dir = Path.home() / ".cache" / "qkernel"

    # Clean before
    if cache_dir.exists():
        shutil.rmtree(cache_dir)

    yield cache_dir

    # Clean after
    if cache_dir.exists():
        shutil.rmtree(cache_dir)


@pytest.fixture
def clean_state():
    """Clean up the qkernel state directory before and after tests."""
    from qkernel.kernel import get_state_dir, stop_kernel, clear_state

    state_dir = get_state_dir()

    # Stop any running kernel and clean state before
    stop_kernel()
    clear_state()

    yield state_dir

    # Stop any running kernel and clean state after
    stop_kernel()
    clear_state()
