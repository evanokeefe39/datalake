"""Orchestration — the Dagster code location for the datalake lakehouse.

Import `orchestration.definitions` for the loaded definitions; this package
deliberately re-exports nothing, because a re-export here shadows the `defs`
subpackage attribute and the shadowing is silent (ADR-0015).
"""
