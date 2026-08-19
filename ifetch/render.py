"""Terminal formatting helpers.

Deliberately dependency-free. A backup tool that people run on a NAS over SSH
should not pull in a rendering library, and the formatting needed here - sizes,
durations, aligned tables, a colour that disables itself when piped - is a
couple of dozen lines.
"""

from __future__ import annotations

import os
import shutil
import sys
from typing import Any, Callable, Iterable, List, Optional, Sequence

_UNITS = ("B", "KB", "MB", "GB", "TB", "PB")


def human_bytes(value: Optional[int], precision: int = 1) -> str:
    """Format a byte count for humans. ``None`` reads as unknown, not zero."""
    if value is None:
        return "unknown"
    size = float(value)
    if size < 1024:
        return f"{int(size)} B"
    for unit in _UNITS[1:]:
        size /= 1024.0
        if size < 1024 or unit == _UNITS[-1]:
            return f"{size:.{precision}f} {unit}"
    return f"{size:.{precision}f} PB"


def human_duration(seconds: Optional[float]) -> str:
    """Format a duration, rounding to a granularity that is honest.

    An estimate presented as "1h 23m 45s" implies a precision the underlying
    throughput guess does not have, so larger values are deliberately coarser.
    """
    if seconds is None:
        return "unknown"
    seconds = max(0.0, float(seconds))
    if seconds < 1:
        return "under a second"
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"


def supports_colour(stream: Any = None) -> bool:
    """True when it is safe to emit ANSI colour.

    Honours ``NO_COLOR`` (the de-facto standard) and refuses when the stream is
    not a terminal, so piping to a file or a log never embeds escape codes.
    """
    stream = stream if stream is not None else sys.stdout
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("TERM") == "dumb":
        return False
    try:
        return bool(stream.isatty())
    except (AttributeError, ValueError):
        return False


_CODES = {
    "reset": "\033[0m",
    "bold": "\033[1m",
    "dim": "\033[2m",
    "red": "\033[31m",
    "green": "\033[32m",
    "yellow": "\033[33m",
    "blue": "\033[34m",
    "cyan": "\033[36m",
}


def colour(text: str, name: str, enabled: bool = True) -> str:
    if not enabled or name not in _CODES:
        return text
    return f"{_CODES[name]}{text}{_CODES['reset']}"


def terminal_width(default: int = 100) -> int:
    try:
        return shutil.get_terminal_size((default, 24)).columns
    except (OSError, ValueError):
        return default


def rule(char: str = "=", width: Optional[int] = None) -> str:
    return char * min(terminal_width(), width or terminal_width())


def table(
    headers: Sequence[str],
    rows: Iterable[Sequence[Any]],
    align: Optional[Sequence[str]] = None,
    max_width: Optional[int] = None,
) -> str:
    """Render an aligned text table.

    Columns are sized to their content, then the *widest* column is truncated
    if the whole table would exceed the terminal. Truncating the widest (rather
    than the last) keeps a long path from squeezing out the size and status
    columns that give it meaning.
    """
    rendered = [[("" if c is None else str(c)) for c in row] for row in rows]
    if not rendered:
        rendered = []

    columns = len(headers)
    widths = [len(h) for h in headers]
    for row in rendered:
        for i in range(min(columns, len(row))):
            widths[i] = max(widths[i], len(row[i]))

    limit = max_width or terminal_width()
    overflow = sum(widths) + 2 * (columns - 1) - limit
    if overflow > 0:
        widest = widths.index(max(widths))
        widths[widest] = max(12, widths[widest] - overflow)

    alignment = list(align or ["<"] * columns)
    alignment += ["<"] * (columns - len(alignment))

    def fmt(cells: Sequence[str]) -> str:
        out = []
        for i in range(columns):
            cell = cells[i] if i < len(cells) else ""
            if len(cell) > widths[i]:
                # Keep the tail of an over-long path: the filename is what
                # identifies it, the leading directories are usually shared.
                cell = "..." + cell[-(widths[i] - 3):] if widths[i] > 3 else cell[: widths[i]]
            out.append(f"{cell:{alignment[i]}{widths[i]}}")
        return "  ".join(out).rstrip()

    lines = [fmt(list(headers)), "  ".join("-" * w for w in widths)]
    lines.extend(fmt(row) for row in rendered)
    return "\n".join(lines)


def key_values(pairs: Sequence[Sequence[Any]], indent: str = "  ") -> str:
    """Render aligned ``label: value`` lines."""
    width = max((len(str(k)) for k, _ in pairs), default=0)
    return "\n".join(f"{indent}{str(k):<{width}}  {v}" for k, v in pairs)


def plural(count: int, singular: str, plural_form: Optional[str] = None) -> str:
    """Pick the right word for a count.

    Small thing, but "1 local files would be replaced" reads as a bug in a
    report whose whole job is to be trusted.
    """
    if count == 1:
        return singular
    return plural_form if plural_form is not None else singular + "s"


def say_once() -> Callable[[str], bool]:
    """Return a filter that lets each distinct sentence through only once.

    Refusal notices assemble their text from overlapping sources - a headline
    reason, a list of alternatives, a list of underlying problems - and the same
    sentence often reaches all three. Printed three times it reads as padding
    and buries the parts that actually differ, in a report whose entire job is
    to be read carefully under stress.

    Matching is on *containment*, not equality, because the headline is usually
    built by appending an underlying reason to a lead-in: the repeat arrives as
    a substring of what was already printed, and equality catches none of them.

    Usage::

        fresh = say_once()
        if fresh(headline):
            lines.append(headline)
        lines.extend(f"  - {a}" for a in alternatives if fresh(a))
    """
    said: List[str] = []

    def fresh(text: str) -> bool:
        key = " ".join(str(text).split()).rstrip(".").casefold()
        if not key or any(key in seen for seen in said):
            return False
        said.append(key)
        return True

    return fresh
