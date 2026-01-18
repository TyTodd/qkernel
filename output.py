"""Output handling for qkernel - printing text and saving images."""

import base64
import shutil
import sys
from pathlib import Path

from kernel import CellOutput
from parser import CodeCell


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


def get_cache_dir() -> Path:
    """Get the base cache directory for qkernel."""
    return Path.home() / ".cache" / "qkernel"


def get_file_cache_dir(filename: str) -> Path:
    """Get the cache directory for a specific file.

    Args:
        filename: The filename stem (without .qmd extension)

    Returns:
        Path to the file's cache directory
    """
    return get_cache_dir() / filename


def clear_file_cache(filename: str) -> None:
    """Clear/recreate the cache directory for a file.

    Args:
        filename: The filename stem (without .qmd extension)
    """
    cache_dir = get_file_cache_dir(filename)
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)


def get_cell_cache_dir(filename: str, cell: CodeCell) -> Path:
    """Get the cache directory for a specific cell.

    Args:
        filename: The filename stem
        cell: The code cell

    Returns:
        Path to the cell's cache directory
    """
    # Use label if available, otherwise use index
    cell_id = cell.label if cell.label else str(cell.index)
    return get_file_cache_dir(filename) / cell_id


def save_image(data: str | bytes, mime_type: str, output_path: Path) -> Path:
    """Save image data to a file.

    Args:
        data: Image data (base64 string or raw bytes/string)
        mime_type: MIME type of the image
        output_path: Path to save the image

    Returns:
        Path where the image was saved
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if mime_type in BASE64_MIMES:
        # Decode base64 data
        if isinstance(data, str):
            image_bytes = base64.b64decode(data)
        else:
            image_bytes = data
        output_path.write_bytes(image_bytes)
    else:
        # Text-based format (e.g., SVG)
        if isinstance(data, bytes):
            output_path.write_bytes(data)
        else:
            output_path.write_text(data, encoding="utf-8")

    return output_path


def process_cell_output(
    output: CellOutput,
    cell: CodeCell,
    filename: str,
    verbose: bool = True,
) -> list[Path]:
    """Process and display cell output, saving any images.

    Args:
        output: The cell execution output
        cell: The code cell that was executed
        filename: The filename stem for cache directory
        verbose: Whether to print output details

    Returns:
        List of paths where images were saved
    """
    saved_images = []
    cell_id = cell.label if cell.label else str(cell.index)

    # Print cell identifier
    if verbose:
        print(
            f"\n--- Cell {cell.index}"
            + (f" [{cell.label}]" if cell.label else "")
            + " ---"
        )

    # Print stdout
    if output.stdout:
        print(output.stdout, end="")

    # Print stderr (in red if terminal supports it)
    if output.stderr:
        if sys.stderr.isatty():
            print(f"\033[91m{output.stderr}\033[0m", end="", file=sys.stderr)
        else:
            print(output.stderr, end="", file=sys.stderr)

    # Print execute_result (text representation)
    if output.result:
        # Prefer text/plain for display
        if "text/plain" in output.result:
            print(output.result["text/plain"])

    # Process display_data (may contain images)
    for i, data in enumerate(output.display_data):
        # Check for image data
        for mime_type, ext in MIME_TO_EXT.items():
            if mime_type in data:
                # Save the image
                cell_cache = get_cell_cache_dir(filename, cell)
                if len(output.display_data) > 1:
                    output_path = cell_cache / f"output_{i}.{ext}"
                else:
                    output_path = cell_cache / f"output.{ext}"

                save_image(data[mime_type], mime_type, output_path)
                saved_images.append(output_path)

                if verbose:
                    print(f"[Image saved to: {output_path}]")
                break
        else:
            # No image found, try to print text representation
            if "text/plain" in data:
                print(data["text/plain"])
            elif "text/html" in data:
                # Just note that HTML was produced
                print("[HTML output produced]")

    # Print error if present
    if output.error:
        print(
            f"\n\033[91mError: {output.error['ename']}: {output.error['evalue']}\033[0m"
        )
        for line in output.error["traceback"]:
            # Strip ANSI codes for cleaner output in non-terminal contexts
            print(line)

    return saved_images


def print_summary(
    cells_executed: int,
    images_saved: list[Path],
    errors: int,
) -> None:
    """Print a summary of the execution.

    Args:
        cells_executed: Number of cells executed
        images_saved: List of paths where images were saved
        errors: Number of cells that had errors
    """
    print(f"\n{'=' * 40}")
    print(f"Executed {cells_executed} cell(s)")

    if images_saved:
        print(f"Saved {len(images_saved)} image(s):")
        for path in images_saved:
            print(f"  - {path}")

    if errors:
        print(f"Encountered {errors} error(s)")
