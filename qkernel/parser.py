"""QMD file parser for extracting code cells with labels and indices."""

import re
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class CodeCell:
    """Represents a code cell extracted from a QMD file."""

    index: int  # 0-based index among code cells
    label: str | None  # From #| label: xxx
    language: str  # python, r, julia, etc.
    source: str  # Cell source code (without the fence markers)
    line_number: int  # Line number in the original file


def parse_cell_options(source: str, language: str) -> dict:
    """Parse cell options from #| comments at the start of the cell.

    Args:
        source: The cell source code
        language: The language of the cell (determines comment character)

    Returns:
        Dictionary of cell options
    """
    # Determine comment character based on language
    comment_chars = {
        "python": "#",
        "r": "#",
        "julia": "#",
        "bash": "#",
        "sh": "#",
        "sql": "--",
        "javascript": "//",
        "js": "//",
    }
    comment = comment_chars.get(language.lower(), "#")

    # Pattern to match #| option lines
    option_pattern = rf"^{re.escape(comment)}\|\s*(.+)$"

    options = {}
    yaml_lines = []

    for line in source.splitlines():
        match = re.match(option_pattern, line)
        if match:
            yaml_lines.append(match.group(1))
        else:
            # Stop at first non-option line
            break

    if yaml_lines:
        yaml_str = "\n".join(yaml_lines)
        try:
            options = yaml.safe_load(yaml_str)
            if not isinstance(options, dict):
                options = {}
        except yaml.YAMLError:
            options = {}

    return options


def parse_qmd(file_path: str | Path) -> list[CodeCell]:
    """Parse a QMD file and extract all code cells.

    Args:
        file_path: Path to the QMD file

    Returns:
        List of CodeCell objects in order of appearance
    """
    file_path = Path(file_path)
    content = file_path.read_text(encoding="utf-8")

    cells = []
    code_cell_index = 0

    # Regex to match code fences with language specification
    # Matches: ```{python}, ```{r}, etc. (with optional attributes)
    # Captures: (1) backticks, (2) language, (3) optional attributes
    start_pattern = re.compile(r"^(\s*)(```+)\s*\{([a-zA-Z][a-zA-Z0-9]*)(.*?)\}\s*$")
    end_pattern_template = r"^{indent}(`{{3,}})\s*$"

    lines = content.splitlines()
    i = 0

    while i < len(lines):
        line = lines[i]
        start_match = start_pattern.match(line)

        if start_match:
            indent = start_match.group(1)
            backticks = start_match.group(2)
            language = start_match.group(3)
            # attributes = start_match.group(4)  # Could parse for inline options

            cell_start_line = i + 1  # 1-based line number
            backtick_count = len(backticks)

            # Find the closing fence (must match indent and backtick count)
            end_pattern = re.compile(
                rf"^{re.escape(indent)}`{{{backtick_count},}}\s*$"
            )

            source_lines = []
            i += 1

            while i < len(lines):
                if end_pattern.match(lines[i]):
                    break
                source_lines.append(lines[i])
                i += 1

            source = "\n".join(source_lines)

            # Parse cell options to get label
            options = parse_cell_options(source, language)
            label = options.get("label")

            cells.append(
                CodeCell(
                    index=code_cell_index,
                    label=label,
                    language=language,
                    source=source,
                    line_number=cell_start_line,
                )
            )
            code_cell_index += 1

        i += 1

    return cells


def filter_cells(
    cells: list[CodeCell], selectors: list[str] | None
) -> list[CodeCell]:
    """Filter cells by index or label.

    Args:
        cells: List of all code cells
        selectors: List of selectors (indices as strings or labels)
                   If None, returns all cells

    Returns:
        Filtered list of cells in the order specified by selectors
    """
    if selectors is None:
        return cells

    # Build lookup maps
    by_index = {cell.index: cell for cell in cells}
    by_label = {cell.label: cell for cell in cells if cell.label}

    result = []
    for selector in selectors:
        # Try to parse as integer index
        try:
            idx = int(selector)
            if idx in by_index:
                result.append(by_index[idx])
            else:
                raise ValueError(f"Cell index {idx} not found (max: {len(cells) - 1})")
        except ValueError:
            # Not an integer, treat as label
            if selector in by_label:
                result.append(by_label[selector])
            else:
                available_labels = [c.label for c in cells if c.label]
                raise ValueError(
                    f"Cell label '{selector}' not found. "
                    f"Available labels: {available_labels}"
                )

    return result


def get_file_stem(file_path: str | Path) -> str:
    """Get the filename without extension for cache directory naming.

    Args:
        file_path: Path to the QMD file

    Returns:
        Filename stem (without .qmd extension)
    """
    path = Path(file_path)
    # Handle .qmd extension
    if path.suffix.lower() == ".qmd":
        return path.stem
    return path.name
