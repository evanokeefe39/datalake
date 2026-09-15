"""SQLite half of the canonical schema catalog — the ops.sqlite contract.

`ops.sqlite` holds operational state: identity (creators, profiles), the merge
ledger, the media cache, and nothing else. Analytical state lives in DuckDB and
its catalog is `orchestration.defs.platform.schemas`.

Each table is declared once as a :class:`Table` spec (ordered columns +
per-column constraints + table-level constraints), and the DDL the runtime
executes is derived from that spec — so the DDL cannot drift from the catalog.

The spec model itself (`Column`, `Table`) is shared by both dialects and is
defined here, in the package the pipeline depends on, rather than in either
consumer.

`creator_merges`' DDL travels here; its ledger *operations* are authored in the
`services-extraction` workstream out of `migrations/migrate_curated_creator_merge.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:  # pragma: no cover - typing only
    import sqlite3


# ── Connection contract ─────────────────────────────────────────────────────


@runtime_checkable
class ConnectionFactory(Protocol):
    """Anything that can hand out a row-factory sqlite3 connection.

    The orchestration layer passes a Dagster `SQLiteResource`; the dashboard
    passes its own handle. Declaring the shape here is what keeps this package
    from importing the orchestration layer — the dependency runs one way.

    `get_connection` returns a connection the CALLER owns and must close.
    """

    def get_connection(self) -> sqlite3.Connection:  # pragma: no cover - protocol
        ...

# ── Spec model (shared with the DuckDB catalog) ─────────────────────────────


@dataclass(frozen=True)
class Column:
    """One column: its canonical type plus DDL constraints.

    ``sql_type`` is the type name reported by the target DB's introspection.
    ``default`` is a raw SQL literal (e.g. ``"'instagram'"``, ``"0"``,
    ``"FALSE"``, ``"CURRENT_TIMESTAMP"``). ``references`` is the full clause
    after ``REFERENCES`` (e.g. ``"creators(id) ON DELETE CASCADE"``).
    """

    sql_type: str
    not_null: bool = False
    default: str | None = None
    primary_key: bool = False
    autoincrement: bool = False
    references: str | None = None


@dataclass(frozen=True)
class Table:
    """A table spec: ordered columns + table-level constraints.

    ``primary_key`` is a composite primary key, used only when no single
    column carries ``primary_key=True``. ``unique`` and ``indexes`` are
    table-level constraints/indexes.
    """

    columns: dict[str, Column]
    primary_key: tuple[str, ...] = ()
    unique: tuple[tuple[str, ...], ...] = ()
    indexes: tuple[tuple[str, str], ...] = ()  # (index_name, "col_a, col_b")


# ── SQLite (data/ops.sqlite) ────────────────────────────────────────────────

# Two retirements are recorded here because a spec is what lets a dropped table
# be recreated:
#
#   "media_metadata" — RETIRED 2026-09-15 (W9). It cached Gemini File-API
#   uploads for a permanently retired provider; the live path resolves media to
#   scrape-time cached local bytes instead. Archived at
#   data/lake/archive/media_metadata/.
#
#   "prompt_registry" — RETIRED 2026-09-15. Provenance now rides on the
#   bronze/silver rows (ADR-0011), so the hash of the current prompt needs no
#   table; `check_prompt_currency` compares claims against the prompt module's
#   `CURRENT_PROMPT_HASH` alone. Archived at
#   data/lake/archive/prompt_registry/.
_SQLITE_SPECS: dict[str, Table] = {
    "media_cache": Table(
        columns={
            "cache_key": Column("TEXT", primary_key=True),
            "local_path": Column("TEXT", not_null=True),
            "content_type": Column("TEXT"),
            "size_bytes": Column("INTEGER"),
            "fetched_at": Column("TEXT", not_null=True),
            "source_url": Column("TEXT"),
        },
    ),
    "creators": Table(
        columns={
            "id": Column("INTEGER", primary_key=True),
            "name": Column("TEXT", not_null=True),
            "created_at": Column("TEXT", not_null=True),
            "updated_at": Column("TEXT", not_null=True),
        },
    ),
    "profiles": Table(
        columns={
            "platform": Column("TEXT", not_null=True),
            "handle": Column("TEXT", not_null=True),
            "profile_url": Column("TEXT", not_null=True),
            "results_type": Column("TEXT", not_null=True, default="'details'"),
            "results_limit": Column("INTEGER", not_null=True, default="1"),
            "enabled": Column("INTEGER", not_null=True, default="1"),
            "tier": Column("TEXT", not_null=True, default="'tier1'"),
            "creator_id": Column(
                "INTEGER", not_null=True, references="creators(id) ON DELETE CASCADE"
            ),
            "updated_at": Column("TEXT", not_null=True),
        },
        primary_key=("platform", "handle"),
    ),
    "creator_merges": Table(
        columns={
            "merged_creator_id": Column("INTEGER", primary_key=True),
            "merged_creator_name": Column("TEXT", not_null=True),
            "surviving_creator_id": Column("INTEGER", not_null=True),
            "handle": Column("TEXT", not_null=True),
            "merged_at": Column("TEXT", not_null=True),
            "reversed_at": Column("TEXT"),
        },
    ),
}

# ── Derived type-only map (backward-compatible shape) ───────────────────────

SQLITE_TABLES: dict[str, dict[str, str]] = {
    name: {col: spec.sql_type for col, spec in table.columns.items()}
    for name, table in _SQLITE_SPECS.items()
}

# ── DDL builder ─────────────────────────────────────────────────────────────


def _column_def(col: Column) -> str:
    """Render a single column's DDL fragment (type + constraints)."""
    parts = [col.sql_type]
    if col.primary_key:
        parts.append("PRIMARY KEY")
        if col.autoincrement:
            parts.append("AUTOINCREMENT")
    if col.not_null:
        parts.append("NOT NULL")
    if col.default is not None:
        parts.append(f"DEFAULT {col.default}")
    if col.references is not None:
        parts.append(f"REFERENCES {col.references}")
    return " ".join(parts)


def _render_table(name: str, table: Table) -> str:
    """Render a ``CREATE TABLE IF NOT EXISTS`` statement from a Table spec."""
    clauses = [f"    {cname} {_column_def(col)}" for cname, col in table.columns.items()]
    if table.primary_key:
        clauses.append(f"    PRIMARY KEY ({', '.join(table.primary_key)})")
    for uniq in table.unique:
        clauses.append(f"    UNIQUE({', '.join(uniq)})")
    return f"CREATE TABLE IF NOT EXISTS {name} (\n" + ",\n".join(clauses) + "\n)"


def _render_indexes(name: str, table: Table) -> list[str]:
    """Render ``CREATE INDEX IF NOT EXISTS`` statements for a table."""
    return [
        f"CREATE INDEX IF NOT EXISTS {idx_name} ON {name}({cols})"
        for idx_name, cols in table.indexes
    ]


def sqlite_ddl(name: str) -> str:
    """Return the ``CREATE TABLE IF NOT EXISTS`` statement for a SQLite table."""
    return _render_table(name, _SQLITE_SPECS[name])


def sqlite_index_ddl(name: str) -> list[str]:
    """Return any ``CREATE INDEX IF NOT EXISTS`` statements for a SQLite table."""
    return _render_indexes(name, _SQLITE_SPECS[name])


def sqlite_ddl_for(*names: str) -> str:
    """Return DDL (tables + their indexes) for the given SQLite tables.

    Statements are ``;``-terminated so the result can be passed to
    ``sqlite3.Connection.executescript``.
    """
    statements: list[str] = []
    for name in names:
        statements.append(sqlite_ddl(name))
        statements.extend(sqlite_index_ddl(name))
    return ";\n".join(statements) + ";" if statements else ""


def sqlite_all_ddl() -> str:
    """Return DDL (tables + indexes) for every SQLite table in the catalog."""
    return sqlite_ddl_for(*_SQLITE_SPECS)
