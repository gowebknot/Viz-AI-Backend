"""Turn the schema an agent pushed into the JSON the direct flow saves.

The shape is a contract (CLAUDE.md invariant 5). It must match `get_schema_structure`:

    {"tables": [{"name", "catalog", "schema", "table",
                 "columns": [{"name", "type", "enum_values"?}],
                 "primary_keys": {"constrained_columns": [...]},
                 "foreign_keys": [{"column", "references"}]}],
     "min_date": "YYYY-MM-DD" | None, "max_date": ...}

The LLM service builds its prompts from this JSON, so the types are written the way the direct
flow writes them (str() of the SQLAlchemy type: VARCHAR(100), NUMERIC(12, 2), TIMESTAMP, ARRAY).
The spelling was taken from the direct flow itself, type by type (tests/connector_integration/
golden_types.json, produced by running get_schema_structure on a table with every common type).

Pure functions on the gateway's plain dataclasses: no database, no gateway import.
"""

from __future__ import annotations

import re
from typing import Any

SUPPORTED_DIALECTS = frozenset({"postgres"})
MAX_TABLES = 5000
MAX_COLUMNS = 100_000

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate(payload: Any) -> str | None:
    """Why this schema must not be saved, or None when it is fine."""
    if payload.dialect not in SUPPORTED_DIALECTS:
        return f"dialect '{payload.dialect}' is not supported yet (supported: {', '.join(sorted(SUPPORTED_DIALECTS))})"
    if not payload.tables:
        return "the schema has no tables (is the database empty, or are all tables excluded?)"
    if len(payload.tables) > MAX_TABLES:
        return f"the schema has {len(payload.tables)} tables; the limit is {MAX_TABLES}"
    columns = sum(len(t.columns) for t in payload.tables)
    if columns > MAX_COLUMNS:
        return f"the schema has {columns} columns; the limit is {MAX_COLUMNS}"
    if any(not t.name for t in payload.tables):
        return "the schema contains a table without a name"
    return None


def map_schema(payload: Any) -> dict[str, Any]:
    """The pushed schema in the direct flow's JSON shape."""
    # Tables in several schemas can share a name. The agent sets search_path to the configured
    # schemas, so a bare name resolves; keep it when unique and qualify only the clashes.
    seen: dict[str, int] = {}
    for table in payload.tables:
        seen[table.name] = seen.get(table.name, 0) + 1

    tables: list[dict[str, Any]] = []
    for table in payload.tables:
        clash = seen[table.name] > 1 and bool(table.schema)
        columns = []
        for column in table.columns:
            entry: dict[str, Any] = {"name": column.name, "type": normalize_type(column.type)}
            if column.enum_values:
                entry["enum_values"] = list(column.enum_values)
                if not column.type.endswith("[]"):
                    # The direct flow reflects a PostgreSQL enum as VARCHAR(longest label).
                    entry["type"] = f"VARCHAR({max(len(label) for label in column.enum_values)})"
            columns.append(entry)
        tables.append(
            {
                "name": f"{table.schema}.{table.name}" if clash else table.name,
                "catalog": None,
                "schema": table.schema if clash else None,
                "table": table.name,
                "columns": columns,
                "primary_keys": {"constrained_columns": list(table.primary_key)},
                "foreign_keys": [
                    {"column": fk.column, "references": fk.references_table} for fk in table.foreign_keys
                ],
            }
        )
    return {
        "tables": tables,
        "min_date": payload.min_date if payload.min_date and _DATE.match(payload.min_date) else None,
        "max_date": payload.max_date if payload.max_date and _DATE.match(payload.max_date) else None,
    }


# ---- types -------------------------------------------------------------------

# PostgreSQL's format_type() name -> what str(sqlalchemy_type) gives in the direct flow.
_SIMPLE = {
    "smallint": "SMALLINT",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "numeric": "NUMERIC",
    "real": "REAL",
    "double precision": "DOUBLE PRECISION",
    "money": "MONEY",
    "character varying": "VARCHAR",
    "text": "TEXT",
    "bytea": "BYTEA",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "interval": "INTERVAL",
    "uuid": "UUID",
    "json": "JSON",
    "jsonb": "JSONB",
    "inet": "INET",
    "cidr": "CIDR",
    "macaddr": "MACADDR",
    "tsvector": "TSVECTOR",
    "oid": "OID",
    "bit": "BIT",
}
_LENGTH = re.compile(r"^(character varying|character)\((\d+)\)$")
_NUMERIC = re.compile(r"^numeric\((\d+),\s*(\d+)\)$")
_TIME = re.compile(r"^(timestamp|time)(\(\d+\))? (with|without) time zone$")
_BIT = re.compile(r"^bit( varying)?\(\d+\)$")


def normalize_type(pg_type: str) -> str:
    """PostgreSQL's format_type() name, spelled as the direct flow spells it.

    Timestamps and times lose their precision and time zone (TIMESTAMP, TIME), arrays are ARRAY,
    bit strings BIT. A type the direct flow cannot name (it writes NULL for xml or point, DOMAIN
    for a domain) keeps its real PostgreSQL name: that is strictly more informative for the LLM.
    """
    name = pg_type.strip()
    lowered = name.lower()
    if lowered.endswith("[]"):
        return "ARRAY"
    if lowered in _SIMPLE:
        return _SIMPLE[lowered]
    if match := _LENGTH.match(lowered):
        return f"{'VARCHAR' if match.group(1) == 'character varying' else 'CHAR'}({match.group(2)})"
    if lowered == "character":
        return "CHAR(1)"
    if match := _NUMERIC.match(lowered):
        return f"NUMERIC({match.group(1)}, {match.group(2)})"
    if match := _TIME.match(lowered):
        return match.group(1).upper()
    if lowered in ("timestamp", "time"):
        return lowered.upper()
    if _BIT.match(lowered) or lowered == "bit varying":
        return "BIT"
    return name
