"""Decorators and utilities for MCP tool call logging."""

import functools
import time
import logging
from typing import Any, Callable, Optional, Dict

logger = logging.getLogger(__name__)


def logged_tool(
    log_service_getter: Callable[[], Any],
    get_context: Optional[Callable[[], Dict[str, Any]]] = None,
):
    """Decorator for logging MCP tool calls.

    Usage:
        @logged_tool(lambda: get_log_service())
        async def my_tool(ctx, **args):
            ...

    Args:
        log_service_getter: Callable that returns the MCPLogService instance
        get_context: Optional callable that returns context data to log

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            service = log_service_getter()
            if service is None or not service.enabled:
                return await func(*args, **kwargs)

            tool_name = func.__name__
            context = get_context() if get_context else None

            start_time = time.time()
            log_id = await service.log_tool_start(
                tool_name=tool_name,
                params=kwargs,
                context=context,
            )

            try:
                result = await func(*args, **kwargs)
                duration_ms = int((time.time() - start_time) * 1000)
                await service.log_tool_success(
                    log_id=log_id,
                    result=result,
                    duration_ms=duration_ms,
                )
                return result
            except Exception as e:
                duration_ms = int((time.time() - start_time) * 1000)
                await service.log_tool_error(
                    log_id=log_id,
                    error=e,
                    duration_ms=duration_ms,
                )
                raise

        return wrapper
    return decorator


async def log_tool_call(
    service: Any,
    tool_name: str,
    params: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
) -> int:
    """Helper to log a tool call start.

    Args:
        service: MCPLogService instance
        tool_name: Name of the tool
        params: Tool parameters
        context: Optional context data

    Returns:
        Log ID for tracking
    """
    if service is None or not service.enabled:
        return -1

    return await service.log_tool_start(
        tool_name=tool_name,
        params=params,
        context=context,
    )


async def log_tool_result(
    service: Any,
    log_id: int,
    result: Any,
    start_time: float,
) -> None:
    """Helper to log a successful tool result.

    Args:
        service: MCPLogService instance
        log_id: ID from log_tool_start
        result: Tool result
        start_time: Start time from time.time()
    """
    if service is None or not service.enabled or log_id < 0:
        return

    duration_ms = int((time.time() - start_time) * 1000)
    await service.log_tool_success(
        log_id=log_id,
        result=result,
        duration_ms=duration_ms,
    )


async def log_tool_failure(
    service: Any,
    log_id: int,
    error: Exception,
    start_time: float,
) -> None:
    """Helper to log a tool failure.

    Args:
        service: MCPLogService instance
        log_id: ID from log_tool_start
        error: Exception that occurred
        start_time: Start time from time.time()
    """
    if service is None or not service.enabled or log_id < 0:
        return

    duration_ms = int((time.time() - start_time) * 1000)
    await service.log_tool_error(
        log_id=log_id,
        error=error,
        duration_ms=duration_ms,
    )
