"""qkernel CLI - Execute Quarto cells through a Jupyter kernel."""

import sys
from pathlib import Path

import click

from kernel import (
    execute_code,
    get_client,
    is_kernel_running,
    restart_kernel,
    start_kernel,
    stop_kernel,
)
from output import clear_file_cache, print_summary, process_cell_output
from parser import filter_cells, get_file_stem, parse_qmd


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """qkernel - Execute Quarto cells through a Jupyter kernel.

    Start a persistent kernel session, then run cells from .qmd files.
    """
    pass


@cli.command()
@click.option(
    "--kernel",
    "-k",
    default="python3",
    help="Kernel name to start (default: python3)",
)
def start(kernel: str):
    """Start a kernel session in the background.

    All following commands will run in this kernel until stopped.
    """
    try:
        state = start_kernel(kernel_name=kernel)
        click.echo(f"Started {kernel} kernel (PID: {state.pid})")
        click.echo(f"Connection file: {state.connection_file}")
    except RuntimeError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
def stop():
    """Stop the current kernel session."""
    if stop_kernel():
        click.echo("Kernel stopped.")
    else:
        click.echo("No kernel is running.")


@cli.command()
@click.option(
    "--kernel",
    "-k",
    default="python3",
    help="Kernel name to start (default: python3)",
)
def restart(kernel: str):
    """Restart the current kernel (stop if running, then start fresh)."""
    try:
        state = restart_kernel(kernel_name=kernel)
        click.echo(f"Restarted {kernel} kernel (PID: {state.pid})")
    except RuntimeError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option(
    "--cells",
    "-c",
    default=None,
    help="Comma-separated cell indices (0-based) or labels to execute",
)
@click.option(
    "--timeout",
    "-t",
    default=None,
    type=int,
    help="Timeout in seconds for cell execution",
)
@click.option(
    "--kernel",
    "-k",
    default="python3",
    help="Kernel name if starting a new kernel (default: python3)",
)
def run(file: str, cells: str | None, timeout: int | None, kernel: str):
    """Run cells from a Quarto file.

    FILE: Path to the .qmd file to execute.

    Examples:

        # Run all cells in a file
        qkernel run notebook.qmd

        # Run specific cells by index (0-based among code cells)
        qkernel run notebook.qmd --cells 0,2,5

        # Run specific cells by label
        qkernel run notebook.qmd --cells setup,analysis,plot

        # Mix indices and labels
        qkernel run notebook.qmd --cells 0,setup,2

        # Run with timeout
        qkernel run notebook.qmd --timeout 600
    """
    file_path = Path(file)
    filename = get_file_stem(file_path)

    # Clear the cache directory for this file
    click.echo(f"Clearing cache for {filename}...")
    clear_file_cache(filename)

    # Parse the QMD file
    try:
        all_cells = parse_qmd(file_path)
    except Exception as e:
        click.echo(f"Error parsing {file}: {e}", err=True)
        sys.exit(1)

    if not all_cells:
        click.echo(f"No code cells found in {file}")
        return

    click.echo(f"Found {len(all_cells)} code cell(s) in {file}")

    # Filter cells if specified
    if cells:
        selectors = [s.strip() for s in cells.split(",")]
        try:
            cells_to_run = filter_cells(all_cells, selectors)
        except ValueError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)
    else:
        cells_to_run = all_cells

    click.echo(f"Running {len(cells_to_run)} cell(s)...")

    # Check if we need to start a temporary kernel
    had_kernel = is_kernel_running()
    temp_kernel = False

    if not had_kernel:
        click.echo(f"Starting temporary {kernel} kernel...")
        try:
            start_kernel(kernel_name=kernel)
            temp_kernel = True
        except RuntimeError as e:
            click.echo(f"Error starting kernel: {e}", err=True)
            sys.exit(1)

    # Get a client connection
    try:
        client = get_client(timeout=timeout)
    except RuntimeError as e:
        click.echo(f"Error connecting to kernel: {e}", err=True)
        if temp_kernel:
            stop_kernel()
        sys.exit(1)

    # Execute cells and collect results
    all_saved_images = []
    error_count = 0

    try:
        for cell in cells_to_run:
            output = execute_code(cell.source, client=client, timeout=timeout)
            saved_images = process_cell_output(output, cell, filename)
            all_saved_images.extend(saved_images)

            if output.error:
                error_count += 1

    finally:
        # Clean up client
        client.stop_channels()

        # Stop temporary kernel if we started one
        if temp_kernel:
            click.echo("\nStopping temporary kernel...")
            stop_kernel()

    # Print summary
    print_summary(len(cells_to_run), all_saved_images, error_count)

    # Exit with error code if there were errors
    if error_count > 0:
        sys.exit(1)


@cli.command()
def status():
    """Check if a kernel is currently running."""
    if is_kernel_running():
        from kernel import load_state

        state = load_state()
        click.echo("Kernel is running:")
        click.echo(f"  Name: {state.kernel_name}")
        click.echo(f"  PID: {state.pid}")
        click.echo(f"  Connection file: {state.connection_file}")
    else:
        click.echo("No kernel is running.")


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
