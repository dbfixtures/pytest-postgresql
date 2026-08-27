"""Collection of executors."""

from .noop import NoopExecutor
from .proc import PostgreSQLExecutor

__all__ = ["NoopExecutor", "PostgreSQLExecutor"]
