"""Kernel management for qkernel - start, stop, restart, and execute."""

import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from jupyter_client import KernelManager
from jupyter_client.blocking import BlockingKernelClient


# =============================================================================
# Python Environment Detection
# =============================================================================


def _get_python_in_env(env_dir: Path) -> Path | None:
    """Get the Python executable from a virtual environment directory."""
    if not env_dir.exists():
        return None

    if sys.platform == "win32":
        candidates = [env_dir / "Scripts" / "python.exe", env_dir / "python.exe"]
    else:
        candidates = [env_dir / "bin" / "python3", env_dir / "bin" / "python"]

    for path in candidates:
        if path.exists():
            return path
    return None


def _is_venv_dir(path: Path) -> bool:
    """Check if directory is a virtual environment (has pyvenv.cfg or conda-meta)."""
    return (path / "pyvenv.cfg").exists() or (path / "conda-meta").exists()


def _find_venv_in_parents(start_dir: Path | None = None) -> Path | None:
    """Search for .venv in start_dir and all parent directories."""
    current = (start_dir or Path.cwd()).resolve()

    while True:
        venv_dir = current / ".venv"
        if venv_dir.exists() and _is_venv_dir(venv_dir):
            python = _get_python_in_env(venv_dir)
            if python:
                return python

        parent = current.parent
        if parent == current:  # Reached root
            break
        current = parent

    return None


def get_kernel_python_with_source() -> tuple[str, str]:
    """Get the Python executable to use and its source.

    Resolution order (similar to quarto-cli):
    1. QUARTO_PYTHON environment variable
    2. VIRTUAL_ENV environment variable
    3. .venv in current directory or any parent
    4. System Python (fallback)

    Returns:
        Tuple of (python_path, source_description)
    """
    # 1. QUARTO_PYTHON (explicit user preference)
    if quarto_python := os.environ.get("QUARTO_PYTHON"):
        path = Path(quarto_python)
        if path.exists():
            if path.is_dir():
                if python := _get_python_in_env(path):
                    return str(python), "QUARTO_PYTHON"
            else:
                return str(path), "QUARTO_PYTHON"

    # 2. VIRTUAL_ENV (activated environment)
    if virtual_env := os.environ.get("VIRTUAL_ENV"):
        if python := _get_python_in_env(Path(virtual_env)):
            return str(python), "VIRTUAL_ENV"

    # 3. .venv in current directory or parents
    if python := _find_venv_in_parents():
        venv_root = python.parent.parent.parent  # .venv/bin/python -> project
        return str(python), f".venv ({venv_root})"

    # 4. System Python
    return sys.executable, "system"


def get_kernel_python() -> str:
    """Get the Python executable to use for the kernel."""
    return get_kernel_python_with_source()[0]


# =============================================================================
# Kernel State Management
# =============================================================================


@dataclass
class KernelState:
    """Persistent state for a running kernel."""

    connection_file: str
    kernel_name: str
    pid: int
    daemon_pid: int
    python_path: str = ""


def get_state_dir() -> Path:
    """Get the directory for storing kernel state."""
    state_dir = Path.home() / ".local" / "share" / "qkernel"
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir


def get_state_file() -> Path:
    """Get the path to the kernel state file."""
    return get_state_dir() / "state.json"


def load_state() -> KernelState | None:
    """Load the current kernel state from disk."""
    state_file = get_state_file()
    if not state_file.exists():
        return None

    try:
        state = KernelState(**json.loads(state_file.read_text()))

        # Verify daemon is still running
        os.kill(state.daemon_pid, 0)

        # Verify connection file exists
        if not Path(state.connection_file).exists():
            raise FileNotFoundError()

        return state
    except (json.JSONDecodeError, KeyError, TypeError, OSError, FileNotFoundError):
        # Clean up invalid state
        state_file.unlink(missing_ok=True)
        return None


def save_state(state: KernelState) -> None:
    """Save kernel state to disk."""
    get_state_file().write_text(json.dumps(asdict(state)))


def clear_state() -> None:
    """Remove the kernel state file."""
    get_state_file().unlink(missing_ok=True)


def _kill_process(pid: int, force: bool = False) -> bool:
    """Attempt to kill a process. Returns True if successful."""
    try:
        os.kill(pid, signal.SIGKILL if force else signal.SIGTERM)
        return True
    except (OSError, ProcessLookupError):
        return False


def _wait_for_process_exit(pid: int, timeout: float = 5.0) -> bool:
    """Wait for a process to exit. Returns True if it exited."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
            time.sleep(0.1)
        except (OSError, ProcessLookupError):
            return True
    return False


# =============================================================================
# Kernel Lifecycle
# =============================================================================


def start_kernel(kernel_name: str = "python3") -> KernelState:
    """Start a new Jupyter kernel in a background daemon process.

    Returns:
        KernelState with connection info

    Raises:
        RuntimeError: If a kernel is already running or fails to start
    """
    if load_state():
        raise RuntimeError(
            "A kernel is already running. Use 'qkernel stop' first or 'qkernel restart'."
        )

    kernel_python = get_kernel_python()
    connection_file = str(get_state_dir() / f"kernel-{os.getpid()}.json")

    # Save initial state
    state = KernelState(
        connection_file=connection_file,
        kernel_name=kernel_name,
        pid=0,
        daemon_pid=0,
        python_path=kernel_python,
    )
    save_state(state)

    # Start daemon
    daemon = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "qkernel.daemon",
            kernel_name,
            connection_file,
            str(get_state_file()),
            kernel_python,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    state.daemon_pid = daemon.pid
    save_state(state)

    # Wait for kernel to start (daemon updates state with PID)
    for _ in range(60):
        time.sleep(0.5)
        if Path(connection_file).exists():
            try:
                data = json.loads(get_state_file().read_text())
                if data.get("pid", 0) != 0:
                    data["daemon_pid"] = daemon.pid
                    get_state_file().write_text(json.dumps(data))
                    break
            except (json.JSONDecodeError, FileNotFoundError):
                pass
    else:
        daemon.kill()
        clear_state()
        raise RuntimeError("Failed to start kernel within timeout")

    return load_state() or (_ for _ in ()).throw(RuntimeError("Failed to start kernel"))


def stop_kernel() -> bool:
    """Stop the running kernel. Returns True if a kernel was stopped."""
    state = load_state()
    if not state:
        return False

    # Kill daemon (it will shut down kernel gracefully)
    if _kill_process(state.daemon_pid):
        _wait_for_process_exit(state.daemon_pid)

    # Force kill kernel if still running
    _kill_process(state.pid)
    _kill_process(state.pid, force=True)
    _kill_process(state.daemon_pid, force=True)

    # Clean up
    Path(state.connection_file).unlink(missing_ok=True)
    clear_state()
    return True


def restart_kernel(kernel_name: str = "python3") -> KernelState:
    """Restart the kernel (stop if running, then start fresh)."""
    stop_kernel()
    return start_kernel(kernel_name)


def interrupt_kernel() -> bool:
    """Send interrupt signal to the kernel to cancel current execution.

    Returns True if interrupt was sent, False if no kernel is running.
    """
    state = load_state()
    if not state:
        return False

    # Send SIGINT to the kernel process to interrupt execution
    try:
        os.kill(state.pid, signal.SIGINT)
        return True
    except (OSError, ProcessLookupError):
        return False


def is_kernel_running() -> bool:
    """Check if a kernel is currently running."""
    return load_state() is not None


# =============================================================================
# Temporary Kernel (for one-off execution)
# =============================================================================


# Initialization code to run when kernel starts (enables inline matplotlib, etc.)
_KERNEL_INIT_CODE = """\
try:
    %matplotlib inline
except:
    pass
"""


def _run_init_code(client: BlockingKernelClient) -> None:
    """Run initialization code silently (e.g., %matplotlib inline)."""
    msg_id = client.execute(_KERNEL_INIT_CODE, silent=True)
    # Wait for execution to complete
    while True:
        try:
            msg = client.get_iopub_msg(timeout=5)
            if msg.get("parent_header", {}).get("msg_id") == msg_id:
                if msg["header"]["msg_type"] == "status":
                    if msg["content"]["execution_state"] == "idle":
                        break
        except Exception:
            break


class TemporaryKernel:
    """Context manager for a temporary kernel that cleans up on exit."""

    def __init__(self, kernel_name: str = "python3"):
        self.kernel_name = kernel_name
        self.km: KernelManager | None = None
        self.client: BlockingKernelClient | None = None
        self.python_path, source = get_kernel_python_with_source()
        self._use_custom_python = source != "system"

    def __enter__(self) -> BlockingKernelClient:
        self.km = KernelManager(kernel_name=self.kernel_name)

        if self._use_custom_python:
            self.km.kernel_spec.argv = [
                self.python_path,
                "-m",
                "ipykernel_launcher",
                "-f",
                "{connection_file}",
            ]

        self.km.start_kernel()
        self.client = self.km.client()
        self.client.start_channels()
        self.client.wait_for_ready(timeout=30)
        _run_init_code(self.client)  # Enable matplotlib inline, etc.
        return self.client

    def __exit__(self, *_):
        if self.client:
            self.client.stop_channels()
        if self.km:
            self.km.shutdown_kernel(now=True)
        return False


# =============================================================================
# Code Execution
# =============================================================================


def get_client(
    state: KernelState | None = None, timeout: float = 30
) -> BlockingKernelClient:
    """Get a client connected to the running kernel."""
    if state is None:
        state = load_state()
    if state is None:
        raise RuntimeError("No kernel is running. Use 'qkernel start' first.")

    client = BlockingKernelClient()
    client.load_connection_file(state.connection_file)
    client.start_channels()
    client.wait_for_ready(timeout=timeout)
    return client


@dataclass
class CellOutput:
    """Output from executing a cell."""

    stdout: str
    stderr: str
    result: Any | None
    display_data: list[dict]
    error: dict | None


def execute_code(
    code: str,
    client: BlockingKernelClient | None = None,
    timeout: float = 600,
) -> CellOutput:
    """Execute code in the kernel and return outputs."""
    own_client = client is None
    if own_client:
        client = get_client(timeout=timeout)

    try:
        msg_id = client.execute(code)

        stdout_parts: list[str] = []
        stderr_parts: list[str] = []
        result = None
        display_data: list[dict] = []
        error = None

        while True:
            try:
                msg = client.get_iopub_msg(timeout=timeout)
            except Exception:
                break

            if msg.get("parent_header", {}).get("msg_id") != msg_id:
                continue

            msg_type = msg["header"]["msg_type"]
            content = msg["content"]

            if msg_type == "stream":
                (stdout_parts if content["name"] == "stdout" else stderr_parts).append(
                    content["text"]
                )
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
            elif msg_type == "status" and content["execution_state"] == "idle":
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
