"""Helpers for tool-level unit tests."""

from types import SimpleNamespace
from typing import Callable, Dict, Tuple


def capture_tools() -> Tuple[Dict[str, Callable], SimpleNamespace]:
    """Capture tools registered via the FastMCP tool decorator."""
    tools: Dict[str, Callable] = {}

    def tool(**_kwargs):
        def decorator(func: Callable) -> Callable:
            tools[func.__name__] = func
            return func

        return decorator

    return tools, SimpleNamespace(tool=tool)
