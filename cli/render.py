"""Output rendering for the `lks` CLI.

Two output modes:
  - "table" (default) — Rich tables when `rich` is installed, plain ASCII fallback otherwise
  - "json"            — pretty-printed JSON for piping into jq / scripts

Every command calls exactly one renderer in this module. Keeping rendering
isolated means CLI command bodies stay focused on HTTP + flow control.
"""
from __future__ import annotations

import json
import sys
from typing import Any, Iterable, Optional, Sequence

try:
    from rich.console import Console
    from rich.table import Table
    _HAS_RICH = True
    _console = Console()
    _err_console = Console(stderr=True)
except Exception:  # pragma: no cover — fallback path
    _HAS_RICH = False
    _console = None
    _err_console = None


def echo(msg: str) -> None:
    """Stdout writer. Uses Rich console when present so colour markup survives."""
    if _HAS_RICH and _console is not None:
        _console.print(msg)
    else:
        print(msg)


def error(msg: str) -> None:
    """Stderr writer for diagnostics — never mixes with the data stream on stdout."""
    if _HAS_RICH and _err_console is not None:
        _err_console.print(f"[red]error:[/red] {msg}")
    else:
        print(f"error: {msg}", file=sys.stderr)


def emit_json(data: Any) -> None:
    """Emit JSON to stdout. Sort keys for deterministic diffs."""
    print(json.dumps(data, indent=2, sort_keys=True, default=str))


def emit_table(
    rows: Sequence[dict[str, Any]],
    columns: Sequence[str],
    title: Optional[str] = None,
) -> None:
    """Render a sequence of dicts as a table.

    Missing keys render as empty strings — never KeyError. None values render
    as "-" so empty cells stay visually distinct from missing data.
    """
    if not rows:
        echo(f"(no rows){' — ' + title if title else ''}")
        return

    if _HAS_RICH and _console is not None:
        table = Table(title=title, show_lines=False, header_style="bold cyan")
        for col in columns:
            table.add_column(col)
        for r in rows:
            table.add_row(*[_fmt_cell(r.get(c)) for c in columns])
        _console.print(table)
        return

    # Plain-text fallback. Compute column widths from data so output stays aligned
    # even when fields vary in length (better UX than a fixed-width grid).
    widths = {c: max(len(c), *(len(_fmt_cell(r.get(c))) for r in rows)) for c in columns}
    sep = "  "
    header = sep.join(c.ljust(widths[c]) for c in columns)
    print(header)
    print(sep.join("-" * widths[c] for c in columns))
    for r in rows:
        print(sep.join(_fmt_cell(r.get(c)).ljust(widths[c]) for c in columns))
    if title:
        print(f"\n{title}")


def emit_kv(pairs: Iterable[tuple[str, Any]], title: Optional[str] = None) -> None:
    """Render a flat key/value report. Used for single-record displays
    (e.g. install status header before the step table)."""
    items = list(pairs)
    if _HAS_RICH and _console is not None:
        table = Table(title=title, show_header=False, box=None)
        table.add_column("key", style="bold")
        table.add_column("value")
        for k, v in items:
            table.add_row(str(k), _fmt_cell(v))
        _console.print(table)
        return

    if title:
        print(title)
    width = max((len(str(k)) for k, _ in items), default=0)
    for k, v in items:
        print(f"  {str(k).ljust(width)}  {_fmt_cell(v)}")


def _fmt_cell(value: Any) -> str:
    """Coerce any JSON value to a single-line table cell string."""
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (list, tuple)):
        return ", ".join(_fmt_cell(v) for v in value) if value else "-"
    if isinstance(value, dict):
        # Most dict-shaped cells in our data are tiny — render as k=v pairs.
        return ", ".join(f"{k}={_fmt_cell(v)}" for k, v in value.items()) if value else "-"
    return str(value)
