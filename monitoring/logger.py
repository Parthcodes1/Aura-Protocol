"""
Structured logging using structlog.
Supports JSON (production) and pretty (development) output modes.
"""
from __future__ import annotations

import logging
import sys
from typing import Any, Optional

import structlog
from rich.console import Console
from rich.logging import RichHandler

_initialized = False
_console = Console(stderr=True)


def setup_logging(level: str = "INFO", fmt: str = "json", agent_id: str = "") -> None:
    """Configure structlog + stdlib logging. Call once at startup."""
    global _initialized
    if _initialized:
        return
    _initialized = True

    log_level = getattr(logging, level.upper(), logging.INFO)

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if agent_id:
        shared_processors.insert(0, structlog.processors.CallsiteParameterAdder(
            parameters=[
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.LINENO,
            ]
        ))

    if fmt == "json":
        # Production: JSON to stdout
        structlog.configure(
            processors=shared_processors + [
                structlog.processors.dict_tracebacks,
                structlog.processors.JSONRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(log_level),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(sys.stdout),
            cache_logger_on_first_use=True,
        )
        logging.basicConfig(
            format="%(message)s",
            stream=sys.stdout,
            level=log_level,
        )
    else:
        # Development: rich pretty output
        structlog.configure(
            processors=shared_processors + [
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            wrapper_class=structlog.make_filtering_bound_logger(log_level),
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )
        formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.dev.ConsoleRenderer(colors=True),
            ],
            foreign_pre_chain=shared_processors,
        )
        handler = RichHandler(console=_console, rich_tracebacks=True, markup=False)
        handler.setFormatter(formatter)
        root_logger = logging.getLogger()
        root_logger.handlers = [handler]
        root_logger.setLevel(log_level)

    # Bind global context
    structlog.contextvars.bind_contextvars(agent=agent_id)


def get_logger(name: str) -> Any:
    """Return a bound structlog logger for `name`."""
    return structlog.get_logger(name)
