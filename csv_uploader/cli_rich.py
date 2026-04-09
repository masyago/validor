"""Shared Rich console configuration for CLI scripts.

Keep styling consistent across CSV generator and uploader.

Expose a `make_console()` factory so non-CLI callers (e.g., Streamlit) can
capture the same output into an in-memory buffer.
"""

from rich.console import Console
from rich.theme import Theme

custom_theme = Theme(
    {"success": "green", "error": "bold red", "warning": "yellow"}
)


def make_console(
    *,
    file=None,
    width: int = 100,
    force_terminal: bool | None = None,
    color_system: str | None = "auto",
    record: bool = False,
    highlight: bool | None = None,
) -> Console:

    return Console(
        theme=custom_theme,
        width=width,
        file=file,
        force_terminal=force_terminal,
        color_system=color_system,
        record=record,
        highlight=highlight,
    )


console = make_console()
