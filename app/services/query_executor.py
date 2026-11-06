"""Runs SQL on a connector datasource. The direct flow never comes here.

A connector row (connection_mode == "connector") has no connection string: its SQL goes through
the gateway to the agent running in the client's environment. Each place that would otherwise
open a database starts with `if is_connector_row(row):` and leaves its direct code untouched.

The gateway package is imported only after a gateway is known to be running, so the backend still
runs without it installed when the feature is off.

Errors:
    ConnectorSqlFailed   the database ran the SQL and returned an error. The caller answers the way
                         its direct path answers a SQL error.
    ConnectorQueryError  anything else, with the HTTP status to answer with (503 offline, 504
                         timeout, 400 rejected, 429 busy, 413 too large). The SQL may be fine.

Never log row values here. The gateway logs a hash of the SQL, timings and row counts.
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any

from fastapi import HTTPException, status

from app.core.settings import settings

logger = logging.getLogger(__name__)

# Same first words as the agent's guard (agent/internal/guard/guard.go).
_ALLOWED_FIRST_WORDS = {"select", "with", "values", "table"}


class ConnectorQueryError(HTTPException):
    """A connector query that could not produce a SQL result, with the status to answer with."""


class ConnectorSqlFailed(Exception):
    """The client's database returned an error for this SQL (syntax, unknown column, ...)."""


def is_connector_row(row: Any) -> bool:
    return getattr(row, "connection_mode", None) == "connector"


def require_direct(row: Any, feature: str) -> None:
    """For a feature that only works with a live connection VizAI opens itself."""
    if is_connector_row(row):
        raise ConnectorQueryError(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{feature} is not available for a connector datasource.",
        )


def _skip_quoted(sql: str, i: int) -> int | None:
    """If a comment or quoted text starts at i, return the index just after it. None otherwise."""
    if sql.startswith("--", i):
        end = sql.find("\n", i)
        return len(sql) if end < 0 else end + 1
    if sql.startswith("/*", i):
        depth, i = 1, i + 2  # Postgres block comments nest
        while i < len(sql) and depth:
            if sql.startswith("/*", i):
                depth, i = depth + 1, i + 2
            elif sql.startswith("*/", i):
                depth, i = depth - 1, i + 2
            else:
                i += 1
        return i
    char = sql[i]
    if char in "'\"":
        i += 1
        while i < len(sql):
            if sql[i] == char:
                if sql[i + 1 : i + 2] == char:  # doubled quote inside the text
                    i += 2
                    continue
                return i + 1
            i += 1
        return i
    if char == "$":
        end_tag = sql.find("$", i + 1)
        if end_tag > 0:
            tag = sql[i : end_tag + 1]
            if all(c.isalnum() or c in "_$" for c in tag):
                close = sql.find(tag, end_tag + 1)
                return len(sql) if close < 0 else close + len(tag)
    return None


def validate_read_only_sql(sql: str) -> None:
    """VizAI's own check, the first of the read-only layers: one statement that reads.

    The agent's guard is the real gatekeeper and checks far more. This stops a bad statement
    before it leaves VizAI. Raises ConnectorQueryError (400).
    """
    first_word = ""
    statement_ended = False
    i = 0
    while i < len(sql):
        after = _skip_quoted(sql, i)
        if after is not None:
            is_comment = sql.startswith(("--", "/*"), i)
            if not is_comment and statement_ended:
                raise _rejected("Only a single SQL statement is allowed.")
            if not is_comment and not first_word:
                raise _rejected("Only read queries are allowed.")
            i = after
            continue
        char = sql[i]
        if char == ";":
            statement_ended = True
        elif not char.isspace():
            if statement_ended:
                raise _rejected("Only a single SQL statement is allowed.")
            if not first_word and char != "(":
                end = i
                while end < len(sql) and (sql[end].isalnum() or sql[end] == "_"):
                    end += 1
                first_word = sql[i:end].lower() or char
                if first_word not in _ALLOWED_FIRST_WORDS:
                    raise _rejected("Only read queries are allowed.")
                i = end
                continue
        i += 1
    if not first_word:
        raise _rejected("The SQL statement is empty.")


def _rejected(message: str) -> ConnectorQueryError:
    return ConnectorQueryError(status_code=status.HTTP_400_BAD_REQUEST, detail=message)


def _target(row: Any) -> tuple[Any, str]:
    """The running gateway and this row's connector id, or a 503."""
    from app.connector_integration.gateway_host import get_gateway

    gateway = get_gateway()
    if gateway is None or not row.connector_id:
        raise ConnectorQueryError(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="The connector service is not available.",
        )
    return gateway, str(row.connector_id)


def ensure_online(row: Any) -> None:
    """Fail fast (503) when no agent is connected, before doing work that needs one."""
    gateway, connector_id = _target(row)
    if not gateway.is_online(connector_id):
        raise ConnectorQueryError(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="connector is offline (no agent instance connected)",
        )


def _translate(exc: Exception) -> Exception:
    """A gateway error, as what this module raises. Gateway messages hold no SQL, rows or secrets."""
    from vizai_connector_gateway.errors import QueryFailed, RateLimited

    if isinstance(exc, QueryFailed):
        return ConnectorSqlFailed(exc.message)
    headers = {"Retry-After": str(math.ceil(exc.retry_after_s))} if isinstance(exc, RateLimited) else None
    return ConnectorQueryError(status_code=exc.http_status, detail=exc.message, headers=headers)


def _max_rows() -> int | None:
    return settings.CONNECTOR_QUERY_MAX_ROWS if settings.CONNECTOR_QUERY_MAX_ROWS > 0 else None


def _log_if_truncated(row: Any, result: Any) -> None:
    if result.truncated:
        logger.warning(
            "connector result cut off at a limit: connection=%s rows=%d request=%s",
            row.id, result.row_count, result.request_id,
        )


def run_sync(row: Any, sql: str) -> Any:
    """Run SQL from synchronous code in a worker thread (a FastAPI `def` route).

    Must not be called on the event loop; async code uses run_async.
    """
    validate_read_only_sql(sql)
    gateway, connector_id = _target(row)
    from vizai_connector_gateway.errors import ConnectorError

    try:
        result = gateway.execute_sync(connector_id, sql, max_rows=_max_rows())
    except ConnectorError as exc:
        raise _translate(exc) from exc
    _log_if_truncated(row, result)
    return result


async def run_async(row: Any, sql: str) -> Any:
    """Run SQL from async code."""
    validate_read_only_sql(sql)
    gateway, connector_id = _target(row)
    from vizai_connector_gateway.errors import ConnectorError

    try:
        result = await gateway.execute(connector_id, sql, max_rows=_max_rows())
    except ConnectorError as exc:
        raise _translate(exc) from exc
    _log_if_truncated(row, result)
    return result


def saved_physical_schema(row: Any) -> dict[str, list[dict[str, Any]]]:
    """Tables and columns from the schema the agent pushed, as {table: [{name, type}, ...]}."""
    if not row.db_schema:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The connector has not sent its database structure yet.",
        )
    saved = json.loads(row.db_schema)
    return {
        table["name"]: [{"name": c["name"], "type": c["type"]} for c in table["columns"]]
        for table in saved.get("tables", [])
    }
