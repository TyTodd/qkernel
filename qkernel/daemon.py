"""Daemon process for qkernel - keeps the Jupyter kernel alive in the background."""

import json
import signal
import sys
import time
from pathlib import Path

from jupyter_client import KernelManager


def run_init_code(client) -> None:
    """Run initialization code (e.g., %matplotlib inline)."""
    try:
        msg_id = client.execute(
            "try:\n    %matplotlib inline\nexcept:\n    pass",
            silent=True,
        )
        while True:
            try:
                msg = client.get_iopub_msg(timeout=5)
                if msg.get("parent_header", {}).get("msg_id") == msg_id:
                    if (
                        msg["header"]["msg_type"] == "status"
                        and msg["content"]["execution_state"] == "idle"
                    ):
                        break
            except Exception:
                break
    except Exception:
        pass


def main():
    """Main daemon entry point."""
    if len(sys.argv) < 5:
        print("Usage: daemon.py <kernel_name> <conn_file> <state_file> <python_path>")
        sys.exit(1)

    kernel_name, conn_file, state_file, python_path = sys.argv[1:5]
    state_path = Path(state_file)

    # Create and configure kernel manager
    km = KernelManager(kernel_name=kernel_name)
    km.connection_file = conn_file
    km.write_connection_file()

    # Override kernel spec to use specified Python
    if python_path:
        km.kernel_spec.argv = [
            python_path,
            "-m",
            "ipykernel_launcher",
            "-f",
            "{connection_file}",
        ]

    # Start the kernel
    km.start_kernel()

    # Update state with kernel PID
    state = json.loads(state_path.read_text())
    state["pid"] = km.provisioner.pid if km.provisioner else 0
    state["python_path"] = python_path
    state_path.write_text(json.dumps(state))

    # Run init code (matplotlib inline, etc.)
    try:
        client = km.client()
        client.start_channels()
        client.wait_for_ready(timeout=30)
        run_init_code(client)
        client.stop_channels()
    except Exception:
        pass

    # Signal handlers for graceful shutdown
    def shutdown(*_):
        km.shutdown_kernel(now=True)
        state_path.unlink(missing_ok=True)
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # Monitor kernel and exit when it dies
    while km.is_alive():
        time.sleep(1)

    state_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
