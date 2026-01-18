"""Kernel management for qkernel - start, stop, restart, and execute."""

import json
import os
import signal
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from jupyter_client import KernelManager
from jupyter_client.blocking import BlockingKernelClient


@dataclass
class KernelState:
    """Persistent state for a running kernel."""

    connection_file: str
    kernel_name: str
    pid: int


def get_state_dir() -> Path:
    """Get the directory for storing kernel state."""
    state_dir = Path.home() / ".local" / "share" / "qkernel"
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir


def get_state_file() -> Path:
    """Get the path to the kernel state file."""
    return get_state_dir() / "state.json"


def load_state() -> KernelState | None:
    """Load the current kernel state from disk.

    Returns:
        KernelState if a kernel is running, None otherwise
    """
    state_file = get_state_file()
    if not state_file.exists():
        return None

    try:
        data = json.loads(state_file.read_text())
        state = KernelState(**data)

        # Verify the kernel is still running
        try:
            os.kill(state.pid, 0)
        except (OSError, ProcessLookupError):
            # Process not running, clean up state
            state_file.unlink(missing_ok=True)
            return None

        # Verify connection file exists
        if not Path(state.connection_file).exists():
            state_file.unlink(missing_ok=True)
            return None

        return state
    except (json.JSONDecodeError, KeyError, TypeError):
        state_file.unlink(missing_ok=True)
        return None


def save_state(state: KernelState) -> None:
    """Save kernel state to disk."""
    state_file = get_state_file()
    state_file.write_text(json.dumps(asdict(state)))


def clear_state() -> None:
    """Remove the kernel state file."""
    get_state_file().unlink(missing_ok=True)


def start_kernel(kernel_name: str = "python3") -> KernelState:
    """Start a new Jupyter kernel.

    Args:
        kernel_name: Name of the kernel to start (default: python3)

    Returns:
        KernelState with connection info

    Raises:
        RuntimeError: If a kernel is already running
    """
    existing = load_state()
    if existing:
        raise RuntimeError(
            "A kernel is already running. Use 'qkernel stop' first or 'qkernel restart'."
        )

    km = KernelManager(kernel_name=kernel_name)
    km.start_kernel()

    state = KernelState(
        connection_file=km.connection_file,
        kernel_name=kernel_name,
        pid=km.kernel.pid,
    )
    save_state(state)

    return state


def stop_kernel() -> bool:
    """Stop the running kernel.

    Returns:
        True if a kernel was stopped, False if no kernel was running
    """
    state = load_state()
    if not state:
        return False

    try:
        # Try graceful shutdown first
        km = KernelManager()
        km.load_connection_file(state.connection_file)
        km.shutdown_kernel(now=True)
    except Exception:
        # Fall back to killing the process directly
        try:
            os.kill(state.pid, signal.SIGTERM)
        except (OSError, ProcessLookupError):
            pass

    # Clean up connection file
    try:
        Path(state.connection_file).unlink(missing_ok=True)
    except Exception:
        pass

    clear_state()
    return True


def restart_kernel(kernel_name: str = "python3") -> KernelState:
    """Restart the kernel (stop if running, then start fresh).

    Args:
        kernel_name: Name of the kernel to start

    Returns:
        KernelState with new connection info
    """
    stop_kernel()
    return start_kernel(kernel_name)


def get_client(
    state: KernelState | None = None, timeout: float | None = None
) -> BlockingKernelClient:
    """Get a blocking client connected to the running kernel.

    Args:
        state: Kernel state (if None, loads from disk)
        timeout: Timeout for shell channel operations

    Returns:
        Connected BlockingKernelClient

    Raises:
        RuntimeError: If no kernel is running
    """
    if state is None:
        state = load_state()

    if state is None:
        raise RuntimeError("No kernel is running. Use 'qkernel start' first.")

    client = BlockingKernelClient()
    client.load_connection_file(state.connection_file)
    client.start_channels()

    # Wait for kernel to be ready
    client.wait_for_ready(timeout=timeout or 30)

    return client


@dataclass
class CellOutput:
    """Represents the output from executing a cell."""

    stdout: str
    stderr: str
    result: Any | None  # execute_result data
    display_data: list[dict]  # List of display_data outputs (may contain images)
    error: dict | None  # Error info if execution failed


def execute_code(
    code: str,
    client: BlockingKernelClient | None = None,
    timeout: float | None = None,
) -> CellOutput:
    """Execute code in the kernel and return outputs.

    Args:
        code: Code to execute
        client: Kernel client (if None, creates one)
        timeout: Timeout for execution in seconds

    Returns:
        CellOutput with execution results
    """
    own_client = client is None
    if own_client:
        client = get_client(timeout=timeout)

    try:
        # Execute the code
        msg_id = client.execute(code)

        # Collect outputs
        stdout_parts = []
        stderr_parts = []
        result = None
        display_data = []
        error = None

        # Process messages until execution is complete
        while True:
            try:
                msg = client.get_iopub_msg(timeout=timeout or 600)
            except Exception:
                break

            msg_type = msg["header"]["msg_type"]
            content = msg["content"]

            # Check if this message is for our execution
            if msg.get("parent_header", {}).get("msg_id") != msg_id:
                continue

            if msg_type == "stream":
                if content["name"] == "stdout":
                    stdout_parts.append(content["text"])
                elif content["name"] == "stderr":
                    stderr_parts.append(content["text"])

            elif msg_type == "execute_result":
                result = content["data"]

            elif msg_type == "display_data":
                display_data.append(content["data"])

            elif msg_type == "error":
                error = {
                    "ename": content["ename"],
                    "evalue": content["evalue"],
                    "traceback": content["traceback"],
                }

            elif msg_type == "status":
                if content["execution_state"] == "idle":
                    break

        return CellOutput(
            stdout="".join(stdout_parts),
            stderr="".join(stderr_parts),
            result=result,
            display_data=display_data,
            error=error,
        )

    finally:
        if own_client:
            client.stop_channels()


def is_kernel_running() -> bool:
    """Check if a kernel is currently running.

    Returns:
        True if a kernel is running, False otherwise
    """
    return load_state() is not None
