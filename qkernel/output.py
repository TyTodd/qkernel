"""Output handling for qkernel - printing text, saving images, and progress display."""

import base64
import logging
import shutil
import sys
import threading
from dataclasses import dataclass
from pathlib import Path

from qkernel.kernel import CellOutput
from qkernel.parser import CodeCell

# Configure module logger
logger = logging.getLogger("qkernel")

# MIME type to file extension mapping
MIME_TO_EXT = {
    "image/png": "png",
    "image/jpeg": "jpg",
    "image/jpg": "jpg",
    "image/gif": "gif",
    "image/svg+xml": "svg",
    "image/webp": "webp",
    "application/pdf": "pdf",
}

# MIME types that are base64 encoded
BASE64_MIMES = {
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/gif",
    "image/webp",
    "application/pdf",
}

# ANSI codes
RESET = "\033[0m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
DIM = "\033[2m"
BOLD = "\033[1m"
CLEAR_LINE = "\033[2K\r"


# =============================================================================
# Cache Directory Management
# =============================================================================


def get_cache_dir() -> Path:
    """Get the base cache directory for qkernel."""
    return Path.home() / ".cache" / "qkernel"


def get_file_cache_dir(filename: str) -> Path:
    """Get the cache directory for a specific file."""
    return get_cache_dir() / filename


def clear_file_cache(filename: str) -> None:
    """Clear/recreate the cache directory for a file."""
    cache_dir = get_file_cache_dir(filename)
    logger.debug(f"Clearing cache directory: {cache_dir}")
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)


def get_cell_cache_dir(filename: str, cell: CodeCell) -> Path:
    """Get the cache directory for a specific cell."""
    cell_id = cell.label if cell.label else str(cell.index)
    return get_file_cache_dir(filename) / cell_id


# =============================================================================
# Image Saving
# =============================================================================


def save_image(data: str | bytes, mime_type: str, output_path: Path) -> Path:
    """Save image data to a file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if mime_type in BASE64_MIMES:
        if isinstance(data, str):
            image_bytes = base64.b64decode(data)
        else:
            image_bytes = data
        output_path.write_bytes(image_bytes)
    else:
        if isinstance(data, bytes):
            output_path.write_bytes(data)
        else:
            output_path.write_text(data, encoding="utf-8")

    logger.debug(f"Saved image: {output_path}")
    return output_path


# =============================================================================
# Animated Progress Display
# =============================================================================


@dataclass
class CellResult:
    """Result of executing a cell."""

    cell: CodeCell
    output: CellOutput
    duration_ms: int
    saved_images: list[Path]


class ProgressDisplay:
    """Animated progress display for cell execution."""

    SPINNER_FRAMES = ["◐", "◓", "◑", "◒"]
    PENDING = "◌"
    SUCCESS = "●"
    ERROR = "●"

    def __init__(self, total_cells: int, use_animation: bool = True):
        self.total_cells = total_cells
        self.use_animation = use_animation and sys.stdout.isatty()
        self.results: list[CellResult] = []
        self._spinner_thread: threading.Thread | None = None
        self._stop_spinner = threading.Event()
        self._current_cell: CodeCell | None = None
        self._spinner_frame = 0
        self._lock = threading.Lock()

    def _get_cell_label(self, cell: CodeCell) -> str:
        """Get display label for a cell."""
        return cell.label if cell.label else f"cell_{cell.index}"

    def _format_header(
        self, cell: CodeCell, icon: str, status: str, color: str = ""
    ) -> str:
        """Format a cell header line."""
        label = self._get_cell_label(cell)
        idx = f"[{cell.index}]"
        # Create a fixed-width header
        header = f"{color}{icon} ━━ {idx} {label} "
        # Pad with ━ to fill width, then add status
        padding = "━" * max(1, 50 - len(idx) - len(label) - 8)
        return f"{header}{padding} {status}{RESET}"

    def _spinner_loop(self):
        """Background thread that animates the spinner."""
        while not self._stop_spinner.wait(0.1):
            with self._lock:
                if self._current_cell:
                    self._spinner_frame = (self._spinner_frame + 1) % len(
                        self.SPINNER_FRAMES
                    )
                    icon = self.SPINNER_FRAMES[self._spinner_frame]
                    line = self._format_header(
                        self._current_cell, icon, f"{YELLOW}running...{RESET}", YELLOW
                    )
                    print(f"{CLEAR_LINE}{line}", end="", flush=True)

    def start_cell(self, cell: CodeCell):
        """Called when starting to execute a cell."""
        logger.debug(f"Starting cell {cell.index}: {self._get_cell_label(cell)}")

        with self._lock:
            self._current_cell = cell
            self._spinner_frame = 0

        if self.use_animation:
            # Show initial pending state
            line = self._format_header(
                cell, self.PENDING, f"{YELLOW}running...{RESET}", YELLOW
            )
            print(f"{line}", end="", flush=True)

            # Start spinner thread if not running
            if self._spinner_thread is None or not self._spinner_thread.is_alive():
                self._stop_spinner.clear()
                self._spinner_thread = threading.Thread(
                    target=self._spinner_loop, daemon=True
                )
                self._spinner_thread.start()
        else:
            label = self._get_cell_label(cell)
            print(f"Running cell {cell.index} [{label}]...", flush=True)

    def finish_cell(
        self,
        cell: CodeCell,
        output: CellOutput,
        duration_ms: int,
        saved_images: list[Path],
    ):
        """Called when a cell finishes executing."""
        # Stop the spinner for this cell
        with self._lock:
            self._current_cell = None

        has_error = output.error is not None
        icon = self.ERROR if has_error else self.SUCCESS
        color = RED if has_error else GREEN
        status = (
            f"{color}✗ {duration_ms}ms{RESET}"
            if has_error
            else f"{color}✓ {duration_ms}ms{RESET}"
        )

        if self.use_animation:
            # Clear the spinner line and print final status
            line = self._format_header(cell, icon, status, color)
            print(f"{CLEAR_LINE}{line}")
        else:
            status_text = "ERROR" if has_error else "OK"
            print(f"  Cell {cell.index} [{status_text}] {duration_ms}ms")

        # Print cell output (indented)
        self._print_cell_output(output, saved_images)

        # Store result
        self.results.append(CellResult(cell, output, duration_ms, saved_images))

    def _print_cell_output(self, output: CellOutput, saved_images: list[Path]):
        """Print the output of a cell, indented."""
        indent = "    "

        # stdout
        if output.stdout:
            for line in output.stdout.rstrip().split("\n"):
                print(f"{indent}{line}")

        # stderr (dimmed/red)
        if output.stderr:
            for line in output.stderr.rstrip().split("\n"):
                print(f"{indent}{DIM}{line}{RESET}")

        # execute_result
        if output.result and "text/plain" in output.result:
            text = output.result["text/plain"]
            if isinstance(text, str):
                for line in text.rstrip().split("\n"):
                    print(f"{indent}{line}")

        # Saved images
        for path in saved_images:
            print(f"{indent}{DIM}[Image: {path}]{RESET}")

        # Error traceback
        if output.error:
            print(
                f"{indent}{RED}Error: {output.error['ename']}: {output.error['evalue']}{RESET}"
            )
            for tb_line in output.error.get("traceback", []):
                # Strip ANSI from traceback for cleaner output
                clean_line = tb_line
                for line in clean_line.split("\n"):
                    print(f"{indent}{DIM}{line}{RESET}")

    def stop(self):
        """Stop any animation threads."""
        self._stop_spinner.set()
        if self._spinner_thread and self._spinner_thread.is_alive():
            self._spinner_thread.join(timeout=0.5)


# =============================================================================
# High-Level Output Functions
# =============================================================================


def process_cell_output(
    output: CellOutput,
    cell: CodeCell,
    filename: str,
) -> list[Path]:
    """Process cell output and save any images. Returns list of saved image paths."""
    saved_images = []

    for i, data in enumerate(output.display_data):
        for mime_type, ext in MIME_TO_EXT.items():
            if mime_type in data:
                cell_cache = get_cell_cache_dir(filename, cell)
                if len(output.display_data) > 1:
                    output_path = cell_cache / f"output_{i}.{ext}"
                else:
                    output_path = cell_cache / f"output.{ext}"

                save_image(data[mime_type], mime_type, output_path)
                saved_images.append(output_path)
                break

    return saved_images


def print_separator() -> None:
    """Print a separator bar."""
    print(f"{'═' * 50}")


def print_summary(cells_executed: int, images_saved: list[Path], errors: int) -> None:
    """Print a summary of the execution."""
    parts = [f"Executed {cells_executed} cell(s)"]
    if images_saved:
        parts.append(f"{len(images_saved)} image(s) saved")
    if errors:
        parts.append(f"{RED}{errors} error(s){RESET}")

    print(" • ".join(parts))

    if images_saved:
        for path in images_saved:
            print(f"  {DIM}→ {path}{RESET}")
