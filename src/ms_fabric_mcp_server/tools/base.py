# ABOUTME: Base utilities for MCP tool implementation.
# ABOUTME: Provides error handling, response formatting, and logging helpers.
"""Base utilities for MCP tool implementation.

This module provides common utilities used across all Fabric MCP tools
including error handling, response formatting, and logging helpers.
"""

import logging
from typing import Any, Dict, Callable
from functools import wraps

logger = logging.getLogger(__name__)


def handle_tool_errors(tool_func: Callable) -> Callable:
    """Decorator to standardize error handling across tools.
    
    This decorator catches exceptions raised by tool functions and returns
    structured error responses instead of raising, making tools more resilient
    and providing consistent error handling for LLM interactions.
    
    Args:
        tool_func: The tool function to wrap with error handling.
        
    Returns:
        Wrapped function that returns error dict instead of raising.
        
    Example:
        ```python
        @handle_tool_errors
        def my_tool(param: str) -> dict:
            # Tool implementation that might raise exceptions
            result = risky_operation(param)
            return format_success_response(result)
        ```
    """
    @wraps(tool_func)
    def wrapper(*args, **kwargs) -> Dict[str, Any]:
        try:
            return tool_func(*args, **kwargs)
        except Exception as exc:
            logger.error(f"Error in {tool_func.__name__}: {exc}", exc_info=True)
            return {
                "status": "error",
                "message": f"Unexpected error: {exc}"
            }
    return wrapper


def format_success_response(
    data: Any = None,
    message: str = "Operation successful"
) -> Dict[str, Any]:
    """Format a successful tool response with consistent structure.
    
    Args:
        data: The result data to include in the response. Can be dict, list, str, etc.
        message: Human-readable success message describing the operation result.
        
    Returns:
        Dictionary with status="success", message, and optional data field.
        
    Example:
        ```python
        return format_success_response(
            data={"workspace_id": "123", "name": "My Workspace"},
            message="Workspace created successfully"
        )
        ```
    """
    response = {
        "status": "success",
        "message": message
    }
    if data is not None:
        response["data"] = data
    return response


def format_error_response(
    message: str,
    error_code: str = None
) -> Dict[str, Any]:
    """Format an error tool response with consistent structure.
    
    Args:
        message: Human-readable error message explaining what went wrong.
        error_code: Optional error code for programmatic error handling.
        
    Returns:
        Dictionary with status="error", message, and optional error_code.
        
    Example:
        ```python
        return format_error_response(
            message="Workspace not found: 'MyWorkspace'",
            error_code="WORKSPACE_NOT_FOUND"
        )
        ```
    """
    response = {
        "status": "error",
        "message": message
    }
    if error_code:
        response["error_code"] = error_code
    return response


def log_tool_invocation(tool_name: str, **params) -> None:
    """Log tool invocation with sanitized parameters.
    
    Logs tool invocation at INFO level with parameter values, excluding
    sensitive data like tokens, credentials, or file contents.
    
    Args:
        tool_name: Name of the tool being invoked.
        **params: Tool parameters to log (sensitive values will be redacted).
        
    Example:
        ```python
        def my_tool(workspace_name: str, api_token: str):
            log_tool_invocation("my_tool", workspace_name=workspace_name, api_token="***")
            # ... tool implementation
        ```
    """
    # Sanitize sensitive parameters
    sanitized_params = {}
    sensitive_keys = {"token", "password", "secret", "key", "credential", "content"}
    
    for key, value in params.items():
        if any(sensitive in key.lower() for sensitive in sensitive_keys):
            sanitized_params[key] = "***REDACTED***"
        elif isinstance(value, str) and len(value) > 100:
            # Truncate long strings (likely file content)
            sanitized_params[key] = f"{value[:100]}... (truncated)"
        else:
            sanitized_params[key] = value
    
    logger.info(f"Tool invocation: {tool_name}", extra={"params": sanitized_params})


def validate_required_params(**params) -> None:
    """Validate that required parameters are provided and not empty.
    
    Raises:
        ValueError: If any parameter is None or empty string.
        
    Example:
        ```python
        def my_tool(workspace_name: str, item_name: str):
            validate_required_params(workspace_name=workspace_name, item_name=item_name)
            # ... rest of implementation
        ```
    """
    for param_name, param_value in params.items():
        if param_value is None or (isinstance(param_value, str) and not param_value.strip()):
            raise ValueError(f"Required parameter '{param_name}' is missing or empty")
