"""Shared Rich console configuration for CLI scripts.

Keep styling consistent across generator/uploader.
"""

from rich.console import Console
from rich.theme import Theme

custom_theme = Theme(
    {"success": "green", "error": "bold red", "warning": "yellow"}
)
console = Console(theme=custom_theme, width=100)
