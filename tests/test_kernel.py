"""Tests for kernel management."""

import threading
import time

import pytest

from qkernel.kernel import (
    TemporaryKernel,
    execute_code,
    interrupt_kernel,
    is_kernel_running,
    restart_kernel,
    start_kernel,
    stop_kernel,
)


class TestTemporaryKernel:
    """Tests for TemporaryKernel context manager."""

    def test_temporary_kernel_executes_code(self, clean_state):
        """Test that temporary kernel can execute code."""
        with TemporaryKernel() as client:
            output = execute_code("print('hello')", client=client)

            assert output.stdout.strip() == "hello"
            assert output.error is None

    def test_temporary_kernel_maintains_state(self, clean_state):
        """Test that state is maintained within a temporary kernel."""
        with TemporaryKernel() as client:
            execute_code("x = 42", client=client)
            output = execute_code("print(x)", client=client)

            assert output.stdout.strip() == "42"

    def test_temporary_kernel_cleans_up(self, clean_state):
        """Test that temporary kernel cleans up after context exit."""
        with TemporaryKernel() as client:
            execute_code("x = 1", client=client)

        # Should not have a persistent kernel running
        assert not is_kernel_running()


class TestPersistentKernel:
    """Tests for persistent kernel management."""

    def test_start_kernel(self, clean_state):
        """Test starting a persistent kernel."""
        state = start_kernel()

        assert state.pid > 0
        assert state.daemon_pid > 0
        assert is_kernel_running()

        # Clean up
        stop_kernel()

    def test_stop_kernel(self, clean_state):
        """Test stopping a persistent kernel."""
        start_kernel()
        assert is_kernel_running()

        result = stop_kernel()

        assert result is True
        assert not is_kernel_running()

    def test_stop_kernel_when_not_running(self, clean_state):
        """Test stopping when no kernel is running."""
        result = stop_kernel()

        assert result is False

    def test_restart_kernel(self, clean_state):
        """Test restarting a kernel."""
        state1 = start_kernel()
        pid1 = state1.pid

        state2 = restart_kernel()
        pid2 = state2.pid

        # Should have a new PID
        assert pid2 != pid1
        assert is_kernel_running()

        # Clean up
        stop_kernel()

    def test_start_kernel_when_already_running_raises(self, clean_state):
        """Test that starting when already running raises error."""
        start_kernel()

        with pytest.raises(RuntimeError, match="already running"):
            start_kernel()

        # Clean up
        stop_kernel()


class TestExecuteCode:
    """Tests for execute_code function."""

    def test_execute_captures_stdout(self, clean_state):
        """Test that stdout is captured."""
        with TemporaryKernel() as client:
            output = execute_code("print('test output')", client=client)

            assert "test output" in output.stdout

    def test_execute_captures_stderr(self, clean_state):
        """Test that stderr is captured."""
        with TemporaryKernel() as client:
            code = "import sys; sys.stderr.write('error output')"
            output = execute_code(code, client=client)

            assert "error output" in output.stderr

    def test_execute_captures_result(self, clean_state):
        """Test that execute_result is captured."""
        with TemporaryKernel() as client:
            output = execute_code("1 + 1", client=client)

            assert output.result is not None
            assert "2" in output.result.get("text/plain", "")

    def test_execute_captures_error(self, clean_state):
        """Test that errors are captured."""
        with TemporaryKernel() as client:
            output = execute_code("undefined_variable", client=client)

            assert output.error is not None
            assert output.error["ename"] == "NameError"

    def test_execute_captures_display_data(self, clean_state):
        """Test that display_data is captured."""
        with TemporaryKernel() as client:
            code = """
from IPython.display import display, HTML
display(HTML('<b>bold</b>'))
"""
            output = execute_code(code, client=client)

            assert len(output.display_data) > 0


class TestInterruptKernel:
    """Tests for kernel interrupt functionality."""

    def test_interrupt_no_kernel(self, clean_state):
        """Test interrupt when no kernel is running."""
        result = interrupt_kernel()
        assert result is False

    def test_interrupt_running_kernel(self, clean_state):
        """Test interrupt on a running kernel."""
        start_kernel()

        # Interrupt should succeed even if nothing is running
        result = interrupt_kernel()
        assert result is True

        # Kernel should still be alive
        assert is_kernel_running()

        stop_kernel()

    def test_interrupt_cancels_long_running_code(self, clean_state):
        """Test that interrupt cancels a long-running cell."""
        from qkernel.kernel import get_client

        start_kernel()
        client = get_client()

        # Start long-running code in background
        results = {}

        def run_long_code():
            try:
                output = execute_code(
                    "import time; time.sleep(30)", client=client, timeout=60
                )
                results["output"] = output
            except Exception as e:
                results["error"] = e

        thread = threading.Thread(target=run_long_code)
        thread.start()

        # Give it a moment to start
        time.sleep(0.5)

        # Interrupt
        interrupt_kernel()

        # Wait for thread to finish (should be quick after interrupt)
        thread.join(timeout=5)

        # Should have completed (with KeyboardInterrupt error or similar)
        assert "output" in results or "error" in results

        client.stop_channels()
        stop_kernel()


class TestOutputEdgeCases:
    """Tests for edge cases in output handling."""

    def test_empty_output(self, clean_state):
        """Test cell with no output."""
        with TemporaryKernel() as client:
            output = execute_code("pass", client=client)

            assert output.stdout == ""
            assert output.stderr == ""
            assert output.result is None
            assert output.error is None
            assert len(output.display_data) == 0

    def test_mixed_stdout_and_result(self, clean_state):
        """Test cell with both stdout and result."""
        with TemporaryKernel() as client:
            output = execute_code("print('hello')\n42", client=client)

            assert "hello" in output.stdout
            assert output.result is not None
            assert "42" in output.result.get("text/plain", "")

    def test_multiple_display_calls(self, clean_state):
        """Test cell with multiple display calls."""
        with TemporaryKernel() as client:
            code = """
from IPython.display import display, HTML
display(HTML('<b>first</b>'))
display(HTML('<i>second</i>'))
"""
            output = execute_code(code, client=client)

            assert len(output.display_data) == 2

    def test_large_output(self, clean_state):
        """Test cell with large output."""
        with TemporaryKernel() as client:
            output = execute_code("print('x' * 10000)", client=client)

            assert len(output.stdout) >= 10000
            assert output.error is None

    def test_unicode_output(self, clean_state):
        """Test cell with unicode output."""
        with TemporaryKernel() as client:
            output = execute_code("print('🎉 Hello 世界')", client=client)

            assert "🎉" in output.stdout
            assert "世界" in output.stdout

    def test_matplotlib_inline_produces_display_data(self, clean_state):
        """Test that matplotlib inline produces display_data (image)."""
        with TemporaryKernel() as client:
            code = """
import matplotlib.pyplot as plt
plt.figure(figsize=(4, 3))
plt.plot([1, 2, 3], [1, 4, 9])
plt.show()
"""
            output = execute_code(code, client=client)

            # Should have captured the plot as display_data
            assert len(output.display_data) > 0
            # Should contain PNG image data
            has_image = any("image/png" in data for data in output.display_data)
            assert has_image
