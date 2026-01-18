"""Integration tests for the CLI."""

import pytest
from click.testing import CliRunner

from qkernel.cli import cli
from qkernel.kernel import is_kernel_running, stop_kernel


@pytest.fixture
def runner():
    """Create a Click CLI test runner."""
    return CliRunner()


class TestCliHelp:
    """Tests for CLI help and basic commands."""

    def test_help(self, runner):
        """Test that --help works."""
        result = runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "qkernel" in result.output
        assert "start" in result.output
        assert "stop" in result.output
        assert "run" in result.output

    def test_version(self, runner):
        """Test that --version works."""
        result = runner.invoke(cli, ["--version"])

        assert result.exit_code == 0
        assert "0.1.0" in result.output


class TestCliStatus:
    """Tests for the status command."""

    def test_status_no_kernel(self, runner, clean_state):
        """Test status when no kernel is running."""
        result = runner.invoke(cli, ["status"])

        assert result.exit_code == 0
        assert "No kernel is running" in result.output


class TestCliStartStop:
    """Tests for start and stop commands."""

    def test_start_and_stop(self, runner, clean_state):
        """Test starting and stopping a kernel."""
        # Start
        result = runner.invoke(cli, ["start"])
        assert result.exit_code == 0
        assert "Started Kernel" in result.output
        assert is_kernel_running()

        # Stop
        result = runner.invoke(cli, ["stop"])
        assert result.exit_code == 0
        assert "stopped" in result.output
        assert not is_kernel_running()

    def test_start_when_already_running(self, runner, clean_state):
        """Test that starting when already running shows error."""
        runner.invoke(cli, ["start"])

        result = runner.invoke(cli, ["start"])

        assert result.exit_code == 1
        assert "already running" in result.output

        # Clean up
        stop_kernel()


class TestCliRestart:
    """Tests for the restart command."""

    def test_restart(self, runner, clean_state):
        """Test restarting a kernel."""
        runner.invoke(cli, ["start"])

        result = runner.invoke(cli, ["restart"])

        assert result.exit_code == 0
        assert "Kernel restarted" in result.output
        assert is_kernel_running()

        # Clean up
        stop_kernel()

    def test_restart_when_not_running(self, runner, clean_state):
        """Test restart when no kernel is running (should start one)."""
        result = runner.invoke(cli, ["restart"])

        assert result.exit_code == 0
        assert "Kernel restarted" in result.output
        assert is_kernel_running()

        # Clean up
        stop_kernel()


class TestCliRun:
    """Tests for the run command."""

    def test_run_all_cells(self, runner, clean_state, clean_cache, simple_qmd):
        """Test running all cells in a file."""
        result = runner.invoke(cli, ["run", str(simple_qmd)])

        assert result.exit_code == 0
        assert "Started Kernel" in result.output
        assert "Kernel stopped" in result.output
        assert "Setup: x=10, y=20" in result.output
        assert "Result: 30" in result.output
        assert "Step 0" in result.output
        assert "Done!" in result.output
        assert "Executed 4 cell(s)" in result.output

    def test_run_specific_cells_by_index(
        self, runner, clean_state, clean_cache, simple_qmd
    ):
        """Test running specific cells by index."""
        result = runner.invoke(cli, ["run", str(simple_qmd), "--cells", "0,2"])

        assert result.exit_code == 0
        assert "Setup: x=10, y=20" in result.output
        assert "Step 0" in result.output
        # Should not have the compute cell output
        assert "Result:" not in result.output
        assert "Executed 2 cell(s)" in result.output

    def test_run_specific_cells_by_label(
        self, runner, clean_state, clean_cache, simple_qmd
    ):
        """Test running specific cells by label."""
        result = runner.invoke(cli, ["run", str(simple_qmd), "--cells", "setup,loop"])

        assert result.exit_code == 0
        assert "Setup: x=10, y=20" in result.output
        assert "Step 0" in result.output
        assert "Executed 2 cell(s)" in result.output

    def test_run_with_error_cell(self, runner, clean_state, clean_cache, error_qmd):
        """Test running a file with an error cell."""
        result = runner.invoke(cli, ["run", str(error_qmd)])

        # Should exit with error code
        assert result.exit_code == 1
        assert "x = 10" in result.output
        assert "NameError" in result.output
        assert "1 error(s)" in result.output

    def test_run_clears_cache(self, runner, clean_state, clean_cache, simple_qmd):
        """Test that run clears the cache directory."""
        from qkernel.output import get_file_cache_dir

        # Create a file in the cache
        cache_dir = get_file_cache_dir("simple")
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / "old_file.txt").write_text("old content")

        runner.invoke(cli, ["run", str(simple_qmd)])

        # Old file should be gone
        assert not (cache_dir / "old_file.txt").exists()

    def test_run_nonexistent_file(self, runner, clean_state):
        """Test running a nonexistent file."""
        result = runner.invoke(cli, ["run", "nonexistent.qmd"])

        assert result.exit_code != 0

    def test_run_invalid_cell_selector(
        self, runner, clean_state, clean_cache, simple_qmd
    ):
        """Test running with invalid cell selector."""
        result = runner.invoke(cli, ["run", str(simple_qmd), "--cells", "nonexistent"])

        assert result.exit_code == 1
        assert "not found" in result.output


class TestCliRunWithPersistentKernel:
    """Tests for run command with persistent kernel."""

    def test_run_uses_persistent_kernel(
        self, runner, clean_state, clean_cache, simple_qmd
    ):
        """Test that run uses persistent kernel when available."""
        # Start persistent kernel
        runner.invoke(cli, ["start"])

        # Run should say "Kernel running" not "Kernel started"
        result = runner.invoke(cli, ["run", str(simple_qmd), "--cells", "setup"])

        assert result.exit_code == 0
        assert "Using Active Kernel" in result.output
        assert "Kernel still running" in result.output
        assert "Setup: x=10, y=20" in result.output

        # Clean up
        stop_kernel()

    def test_run_maintains_state_in_persistent_kernel(
        self, runner, clean_state, clean_cache, simple_qmd
    ):
        """Test that state is maintained across runs with persistent kernel."""
        runner.invoke(cli, ["start"])

        # Run setup cell
        runner.invoke(cli, ["run", str(simple_qmd), "--cells", "setup"])

        # Run compute cell (depends on setup)
        result = runner.invoke(cli, ["run", str(simple_qmd), "--cells", "compute"])

        assert result.exit_code == 0
        assert "Result: 30" in result.output

        # Clean up
        stop_kernel()


class TestCliCancel:
    """Tests for the cancel command."""

    def test_cancel_no_kernel(self, runner, clean_state):
        """Test cancel when no kernel is running."""
        result = runner.invoke(cli, ["cancel"])

        assert result.exit_code == 0
        assert "No kernel is running" in result.output

    def test_cancel_with_kernel(self, runner, clean_state):
        """Test cancel when kernel is running."""
        runner.invoke(cli, ["start"])

        result = runner.invoke(cli, ["cancel"])

        assert result.exit_code == 0
        assert "Sent interrupt to kernel" in result.output

        # Kernel should still be running
        assert is_kernel_running()

        # Clean up
        stop_kernel()


class TestCliImageEdgeCases:
    """Tests for image output edge cases."""

    def test_run_with_image(self, runner, clean_state, clean_cache, image_qmd):
        """Test running a file that generates an image."""
        result = runner.invoke(cli, ["run", str(image_qmd)])

        assert result.exit_code == 0
        assert "image(s) saved" in result.output

        # Check that image file exists
        cache_dir = clean_cache / "with_image" / "plot"
        assert cache_dir.exists()
        image_files = list(cache_dir.glob("*.png"))
        assert len(image_files) > 0

    def test_run_with_multiple_images(
        self, runner, clean_state, clean_cache, multi_image_qmd
    ):
        """Test running a file that generates multiple images in one cell."""
        result = runner.invoke(cli, ["run", str(multi_image_qmd)])

        assert result.exit_code == 0
        # Should have multiple images
        cache_dir = clean_cache / "multi_image" / "two_plots"
        assert cache_dir.exists()
        image_files = list(cache_dir.glob("*.png"))
        assert len(image_files) >= 2


class TestCliMixedOutput:
    """Tests for mixed output types."""

    def test_run_mixed_output(self, runner, clean_state, clean_cache, mixed_output_qmd):
        """Test running a file with mixed output types."""
        result = runner.invoke(cli, ["run", str(mixed_output_qmd)])

        assert result.exit_code == 0
        # Check various output types
        assert "This is stdout" in result.output
        assert "50" in result.output  # 42 + 8
        assert "Final result" in result.output
        assert "After HTML display" in result.output
        assert "multiline" in result.output
