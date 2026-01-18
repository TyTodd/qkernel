"""qkernel CLI - Execute Quarto cells through a Jupyter kernel."""

import logging
import sys
import time
from pathlib import Path

import click

from qkernel.kernel import (
    TemporaryKernel,
    execute_code,
    get_client,
    interrupt_kernel,
    is_kernel_running,
    restart_kernel,
    start_kernel,
    stop_kernel,
)
from qkernel.output import (
    ProgressDisplay,
    clear_file_cache,
    print_separator,
    print_summary,
    process_cell_output,
)
from qkernel.parser import filter_cells, get_file_stem, parse_qmd

# Configure logger
logger = logging.getLogger("qkernel")


def setup_logging(verbose: bool = False):
    """Configure logging level."""
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(levelname)s: %(message)s",
    )


@click.group()
@click.version_option(version="0.1.0")
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging")
def cli(verbose: bool):
    """qkernel - Execute Quarto cells through a Jupyter kernel."""
    setup_logging(verbose)


@cli.command()
@click.option(
    "--kernel", "-k", default="python3", help="Kernel name (default: python3)"
)
def start(kernel: str):
    """Start a kernel session in the background."""
    from qkernel.kernel import get_kernel_python_with_source

    python_path, source = get_kernel_python_with_source()
    logger.debug(f"Python resolution: {python_path} (from {source})")

    try:
        state = start_kernel(kernel_name=kernel)
        print(f"Started Kernel ({python_path})")
        logger.debug(f"PID: {state.pid}, Connection: {state.connection_file}")
    except RuntimeError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
def stop():
    """Stop the current kernel session."""
    if stop_kernel():
        print("Kernel stopped")
    else:
        print("No kernel is running")


@cli.command()
def cancel():
    """Cancel the currently running cell (keeps kernel alive)."""
    if interrupt_kernel():
        print("Sent interrupt to kernel")
    else:
        print("No kernel is running")


@cli.command()
@click.option(
    "--kernel", "-k", default="python3", help="Kernel name (default: python3)"
)
def restart(kernel: str):
    """Restart the current kernel."""
    from qkernel.kernel import get_kernel_python_with_source

    python_path, source = get_kernel_python_with_source()
    logger.debug(f"Python resolution: {python_path} (from {source})")

    try:
        state = restart_kernel(kernel_name=kernel)
        print(f"Kernel restarted ({python_path})")
        logger.debug(f"PID: {state.pid}")
    except RuntimeError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option(
    "--cells", "-c", default=None, help="Cell indices or labels (comma-separated)"
)
@click.option("--timeout", "-t", default=None, type=int, help="Timeout in seconds")
@click.option(
    "--kernel", "-k", default="python3", help="Kernel name (default: python3)"
)
def run(file: str, cells: str | None, timeout: int | None, kernel: str):
    """Run cells from a Quarto file.

    Examples:
        qkernel run notebook.qmd
        qkernel run notebook.qmd --cells 0,2,5
        qkernel run notebook.qmd --cells setup,plot
    """
    file_path = Path(file)
    filename = get_file_stem(file_path)

    # Clear cache
    logger.debug(f"Clearing cache for {filename}")
    clear_file_cache(filename)

    # Parse QMD
    try:
        all_cells = parse_qmd(file_path)
    except Exception as e:
        click.echo(f"Error parsing {file}: {e}", err=True)
        sys.exit(1)

    if not all_cells:
        print(f"No code cells found in {file}")
        return

    logger.debug(f"Found {len(all_cells)} code cell(s) in {file}")

    # Filter cells
    if cells:
        selectors = [s.strip() for s in cells.split(",")]
        try:
            cells_to_run = filter_cells(all_cells, selectors)
        except ValueError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)
    else:
        cells_to_run = all_cells

    logger.debug(f"Running {len(cells_to_run)} cell(s)")

    # Execute cells
    all_saved_images: list[Path] = []
    error_count = 0
    progress = ProgressDisplay(len(cells_to_run))

    def run_cells(client):
        nonlocal error_count
        for cell in cells_to_run:
            progress.start_cell(cell)
            start_time = time.time()

            output = execute_code(cell.source, client=client, timeout=timeout)
            saved_images = process_cell_output(output, cell, filename)
            all_saved_images.extend(saved_images)

            duration_ms = int((time.time() - start_time) * 1000)
            progress.finish_cell(cell, output, duration_ms, saved_images)

            if output.error:
                error_count += 1

    kernel_message = ""
    interrupted = False
    using_persistent = is_kernel_running()
    temp_kernel = None

    try:
        if using_persistent:
            # Use persistent kernel
            from qkernel.kernel import load_state

            state = load_state()
            print(f"Using Active Kernel ({state.python_path})")
            print_separator()
            logger.debug(f"Using persistent kernel PID: {state.pid}")

            client = get_client(timeout=timeout)
            try:
                run_cells(client)
            finally:
                client.stop_channels()
            kernel_message = "Kernel still running"
        else:
            # Start temporary kernel
            from qkernel.kernel import get_kernel_python_with_source

            python_path, source = get_kernel_python_with_source()
            logger.debug(f"Python resolution: {python_path} (from {source})")

            print(f"Started Kernel ({python_path})")
            print_separator()

            temp_kernel = TemporaryKernel(kernel_name=kernel)
            client = temp_kernel.__enter__()
            try:
                run_cells(client)
            finally:
                temp_kernel.__exit__(None, None, None)

            kernel_message = "Kernel stopped"

    except KeyboardInterrupt:
        interrupted = True
        progress.stop()
        print()  # Newline after ^C
        if using_persistent:
            # Interrupt the kernel but keep it running
            interrupt_kernel()
            kernel_message = "Interrupted - Kernel still running"
        else:
            # Stop the temporary kernel
            if temp_kernel:
                temp_kernel.__exit__(None, None, None)
            kernel_message = "Interrupted - Kernel stopped"
    except Exception as e:
        progress.stop()
        if temp_kernel:
            temp_kernel.__exit__(None, None, None)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        progress.stop()

    # Summary
    print()
    print_separator()
    print(kernel_message)
    cells_completed = len(progress.results) if hasattr(progress, "results") else 0
    print_summary(cells_completed, all_saved_images, error_count)

    if interrupted:
        sys.exit(130)  # Standard exit code for Ctrl+C
    if error_count > 0:
        sys.exit(1)


@cli.command()
def status():
    """Check if a kernel is running."""
    if is_kernel_running():
        from qkernel.kernel import load_state

        state = load_state()
        print(f"Using Active Kernel ({state.python_path})")
        logger.debug(f"  PID: {state.pid}")
        logger.debug(f"  Connection: {state.connection_file}")
    else:
        print("No kernel is running")


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
