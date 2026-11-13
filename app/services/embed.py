"""
Embed Service

Handles creation, validation, and revocation of share tokens for
dashboard embedding. Implements HMAC-SHA256 signing, share-token persistence,
stateless embed JWT sessions, and embed data retrieval.
"""

import hashlib
import hmac
import logging
import time
from datetime import date as _date_type, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Optional
from uuid import UUID, uuid4

from fastapi import HTTPException, status
from sqlalchemy import and_
from sqlalchemy.orm import Session

from app.core.settings import settings
from app.models.schema_models import (
    DashboardModel,
    DashboardChartsModel,
    ChartModel,
    DatabaseConnectionModel,
    ShareTokenModel,
    UserProjectRoleModel,
)
from app.services.allowed_domains import get_allowed_domains_list
from app.services.query_executor import (
    ConnectorSqlFailed,
    is_connector_row,
    run_async as run_connector_query,
)
from app.services.app_service import normalize_domain
from app.utils.embed_jwt import (
    decode_embed_jwt_for_refresh,
    issue_embed_jwt,
)

logger = logging.getLogger(__name__)


def _json_safe_value(value: Any) -> Any:
    """Convert DB-driver values to JSON-serializable primitives."""
    if value is None:
        return None
    if isinstance(value, (datetime, _date_type)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return str(value)
    return value


def _json_safe_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Sanitize a full row dict for JSON serialization."""
    return {k: _json_safe_value(v) for k, v in row.items()}


def _get_embed_secret() -> str:
    """Get the server secret used for HMAC signing."""
    secret = settings.EMBED_SERVER_SECRET or settings.SECRET_KEY
    if not secret:
        raise RuntimeError("No server secret configured for embed token signing")
    return secret


def _sign_token(token_id: str, dashboard_id: str) -> str:
    """
    Create HMAC-SHA256 signature for a token.

    Args:
        token_id: The token UUID as string
        dashboard_id: The dashboard UUID as string

    Returns:
        Hex-encoded HMAC signature
    """
    secret = _get_embed_secret()
    message = f"{token_id}{dashboard_id}"
    signature = hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return signature


def _verify_signature(token_id: str, dashboard_id: str, signature: str) -> bool:
    """Verify an HMAC-SHA256 signature for a token."""
    expected = _sign_token(token_id, dashboard_id)
    return hmac.compare_digest(expected, signature)


def _build_embed_url(token_id: str, base_url: str) -> str:
    """Build the embed URL from token ID."""
    return f"{base_url}/api/v1/embed/{token_id}"


def _build_iframe_snippet(embed_url: str) -> str:
    """Build the ready-to-paste iframe snippet."""
    return (
        f'<iframe src="{embed_url}" '
        f'width="100%" height="600" '
        f'frameborder="0" '
        f'style="border: none; border-radius: 8px;" '
        f'allowfullscreen '
        f'loading="lazy">'
        f"</iframe>"
    )


def check_origin(origin: str | None, allowed_domains: list[str]) -> tuple[bool, str]:
    """
    Check if the request origin is in the allowed domains list.
    
    STEP 11: Origin header check (domain-lock enforcement)
    
    Args:
        origin: The Origin header value (may be None for direct browser access)
        allowed_domains: List of bare hostnames from the token's snapshot
    
    Returns:
        Tuple of (is_allowed, reason_string)
    """

    if not origin:
        return True, "origin_null_allowed_for_testing"
    
    origin_hostname = normalize_domain(origin)
    normalized_allowed = [normalize_domain(d) for d in allowed_domains]
    
    if origin_hostname in normalized_allowed:
        return True, "allowed"
    
    return False, "rejected"


async def refresh_embed_jwt(
    raw_jwt: str,
    share_token_id: UUID,
    db: Session,
    origin: str | None = None,
) -> dict:
    """
    Refresh embed session: verify current JWT (signature + grace), recheck share
    token in DB, issue a new 30-minute JWT.
    """
    claims = decode_embed_jwt_for_refresh(raw_jwt)
    if claims.share_token_id != share_token_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "jwt_invalid",
                "message": "Share token does not match session.",
            },
        )

    share = await validate_embed_token(share_token_id, db, origin=origin)
    logger.info(
        f"[EMBED] Embed JWT refreshed for share_token {str(share_token_id)[:8]}..."
    )
    return issue_embed_jwt(share)


async def create_share_token(
    dashboard_id: UUID,
    user_id: UUID,
    expires_in_days: Optional[int],
    base_url: str,
    db: Session,
) -> dict:
    """
    Create or return an existing share token for a dashboard.

    Args:
        dashboard_id: UUID of the dashboard
        user_id: UUID of the requesting user
        expires_in_days: Optional expiry in days (None = no expiry)
        base_url: Server base URL for constructing embed links
        db: SQLAlchemy session

    Returns:
        dict with token details
    """
    start_time = time.time()

    logger.info(
        f"[EMBED][STEP 6] Shareable link requested — "
        f"dashboard {str(dashboard_id)[:8]}..."
    )

  
    dashboard = db.query(DashboardModel).filter(
        DashboardModel.id == dashboard_id
    ).first()

    if not dashboard:
        logger.error(
            f"[EMBED][STEP 6] Validation failed — dashboard {str(dashboard_id)[:8]}... not found"
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dashboard not found",
        )

   
    user_role = db.query(UserProjectRoleModel).filter(
        and_(
            UserProjectRoleModel.user_id == user_id,
            UserProjectRoleModel.project_id == dashboard.project_id,
        )
    ).first()

    role_name = "owner" if (user_role and user_role.is_owner) else "member"

   
    allowed_domains = get_allowed_domains_list(dashboard_id, db)
    logger.info(
        f"[EMBED][STEP 6] Shareable link requested — dashboard {str(dashboard_id)[:8]}..., "
        f"allowed domains: {allowed_domains}"
    )

    logger.info(
        f"[EMBED] Validation passed — user {str(user_id)[:8]}..., "
        f"dashboard {str(dashboard_id)[:8]}..., role {role_name}"
    )


    existing_token = db.query(ShareTokenModel).filter(
        and_(
            ShareTokenModel.dashboard_id == dashboard_id,
            ShareTokenModel.is_active == True,
        )
    ).first()

    if existing_token:
       
        if existing_token.expires_at is None or existing_token.expires_at > datetime.now(timezone.utc):
            embed_url = _build_embed_url(str(existing_token.token_id), base_url)
            duration_ms = int((time.time() - start_time) * 1000)
            logger.info(
                f"[EMBED] Existing active token returned for dashboard "
                f"{str(dashboard_id)[:8]}... — duration: {duration_ms}ms"
            )
            return {
                "message": "Existing share token returned",
                "token": {
                    "token_id": existing_token.token_id,
                    "dashboard_id": existing_token.dashboard_id,
                    "embed_url": embed_url,
                    "iframe_snippet": _build_iframe_snippet(embed_url),
                    "is_active": existing_token.is_active,
                    "created_at": existing_token.created_at,
                    "expires_at": existing_token.expires_at,
                    "access_count": existing_token.access_count,
                    "allowed_domains_snapshot": existing_token.allowed_domains_snapshot,
                },
            }


    token_id = uuid4()
    logger.info(
        f"[EMBED][STEP 7] Token generated — token: {str(token_id)[:8]}..., "
        f"dashboard: {str(dashboard_id)[:8]}..., domains: {allowed_domains}"
    )


    signature = _sign_token(str(token_id), str(dashboard_id))

   
    expires_at = None
    if expires_in_days is not None and expires_in_days > 0:
        expires_at = datetime.now(timezone.utc) + timedelta(days=expires_in_days)

    share_token = ShareTokenModel(
        token_id=token_id,
        dashboard_id=dashboard_id,
        created_by=user_id,
        hmac_signature=signature,
        is_active=True,
        expires_at=expires_at,
        allowed_domains_snapshot=allowed_domains if allowed_domains else None,
    )

    db.add(share_token)
    db.commit()
    db.refresh(share_token)

    duration_ms = int((time.time() - start_time) * 1000)
    logger.info(
        f"[EMBED][STEP 7] Token persisted to DB — "
        f"expires_at: {expires_at}, domains: {allowed_domains} — duration: {duration_ms}ms"
    )

    embed_url = _build_embed_url(str(token_id), base_url)


    iframe_snippet = _build_iframe_snippet(embed_url)
    logger.info(
        f"[EMBED][STEP 8] Snippet returned to client for dashboard {str(dashboard_id)[:8]}..."
    )

    return {
        "message": "Share token created successfully",
        "token": {
            "token_id": share_token.token_id,
            "dashboard_id": share_token.dashboard_id,
            "embed_url": embed_url,
            "iframe_snippet": iframe_snippet,
            "is_active": share_token.is_active,
            "created_at": share_token.created_at,
            "expires_at": share_token.expires_at,
            "access_count": share_token.access_count,
            "allowed_domains_snapshot": share_token.allowed_domains_snapshot,
        },
    }


async def revoke_share_token(
    dashboard_id: UUID,
    user_id: UUID,
    db: Session,
) -> dict:
    """
    Revoke all active share tokens for a dashboard.
    Sets is_active = false (does not delete for audit trail).

    Args:
        dashboard_id: UUID of the dashboard
        user_id: UUID of the requesting user
        db: SQLAlchemy session

    Returns:
        dict with revocation result
    """
    start_time = time.time()
    dashboard = db.query(DashboardModel).filter(
        DashboardModel.id == dashboard_id
    ).first()

    if not dashboard:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dashboard not found",
        )

    active_tokens = db.query(ShareTokenModel).filter(
        and_(
            ShareTokenModel.dashboard_id == dashboard_id,
            ShareTokenModel.is_active == True,
        )
    ).all()

    if not active_tokens:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No active share token found for this dashboard",
        )

    for token in active_tokens:
        token.is_active = False

    db.commit()

    duration_ms = int((time.time() - start_time) * 1000)
    logger.info(
        f"[EMBED] Share token(s) revoked for dashboard {str(dashboard_id)[:8]}... "
        f"by user {str(user_id)[:8]}... — count: {len(active_tokens)} — duration: {duration_ms}ms"
    )

    return {
        "message": "Share token revoked successfully",
        "dashboard_id": dashboard_id,
    }


async def get_share_token(
    dashboard_id: UUID,
    db: Session,
    base_url: str,
) -> Optional[dict]:
    """
    Get the active share token for a dashboard (if any).

    Args:
        dashboard_id: UUID of the dashboard
        db: SQLAlchemy session
        base_url: Server base URL

    Returns:
        dict with token details or None
    """
    token = db.query(ShareTokenModel).filter(
        and_(
            ShareTokenModel.dashboard_id == dashboard_id,
            ShareTokenModel.is_active == True,
        )
    ).first()

    if not token:
        return None


    if token.expires_at and token.expires_at < datetime.now(timezone.utc):
        return None

    embed_url = _build_embed_url(str(token.token_id), base_url)
    return {
        "token_id": token.token_id,
        "dashboard_id": token.dashboard_id,
        "embed_url": embed_url,
        "iframe_snippet": _build_iframe_snippet(embed_url),
        "is_active": token.is_active,
        "created_at": token.created_at,
        "expires_at": token.expires_at,
        "access_count": token.access_count,
    }


async def validate_embed_token(
    token_id: UUID,
    db: Session,
    origin: str | None = None,
) -> ShareTokenModel:
    """
    Validate an embed token for rendering or data requests.
    

    Args:
        token_id: UUID of the token
        db: SQLAlchemy session
        origin: Origin header from the request (for domain-lock)

    Returns:
        ShareTokenModel if valid

    Raises:
        HTTPException 403 if invalid
    """
    start_time = time.time()


    logger.info(
        f"[EMBED][STEP 10] Embed request — token: {str(token_id)[:8]}..., origin: {origin}"
    )

    token = db.query(ShareTokenModel).filter(
        ShareTokenModel.token_id == token_id
    ).first()

    if not token:
        logger.error(
            f"[EMBED][STEP 12] Token validation — result: invalid, reason: not_found"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "embed_invalid",
                "message": "This embed link is invalid or has expired.",
            },
        )

    if not token.is_active:
        logger.error(
            f"[EMBED][STEP 12] Token validation — result: invalid, reason: revoked"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "embed_invalid",
                "message": "This embed link is invalid or has expired.",
            },
        )

    if token.expires_at and token.expires_at < datetime.now(timezone.utc):
        logger.error(
            f"[EMBED][STEP 12] Token validation — result: invalid, reason: expired "
            f"(expires_at={token.expires_at})"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "embed_invalid",
                "message": "This embed link is invalid or has expired.",
            },
        )


    if not _verify_signature(str(token.token_id), str(token.dashboard_id), token.hmac_signature):
        logger.error(
            f"[EMBED][STEP 12] Token validation — result: invalid, reason: hmac_mismatch"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "embed_invalid",
                "message": "This embed link is invalid or has expired.",
            },
        )

    logger.info(
        f"[EMBED][STEP 12] Token validation — result: valid"
    )

    allowed_domains = token.allowed_domains_snapshot or []
    if allowed_domains:
        is_allowed, result = check_origin(origin, allowed_domains)
        logger.info(
            f"[EMBED][STEP 11] Origin check — origin: {origin}, "
            f"allowed: {allowed_domains}, result: {result}"
        )
        if not is_allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "error": "domain_not_allowed",
                    "message": "This embed is not authorized for this domain.",
                },
            )
    else:
        logger.info(
            f"[EMBED][STEP 11] Origin check — no allowed_domains_snapshot, "
            f"skipping domain lock"
        )

   
    token.last_accessed = datetime.now(timezone.utc)
    token.access_count += 1
    db.commit()

    duration_ms = int((time.time() - start_time) * 1000)
    logger.info(
        f"[EMBED] Token valid — dashboard {str(token.dashboard_id)[:8]}... "
        f"authorized for embed — duration: {duration_ms}ms"
    )

    return token


async def get_embed_dashboard_data_by_dashboard_id(
    dashboard_id: UUID,
    db: Session,
) -> dict:
    """Dashboard metadata for embed (title + charts) by dashboard id."""
    start_time = time.time()

    dashboard = db.query(DashboardModel).filter(
        DashboardModel.id == dashboard_id
    ).first()

    if not dashboard:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dashboard not found",
        )

    dashboard_charts = (
        db.query(DashboardChartsModel)
        .filter(DashboardChartsModel.dashboard_id == dashboard.id)
        .all()
    )

    charts = []
    for dc in dashboard_charts:
        chart = db.query(ChartModel).filter(ChartModel.id == dc.chart_id).first()
        if chart:
            charts.append({
                "id": str(chart.id),
                "title": chart.title,
                "chart_type": chart.chart_type,
                "query": chart.query,
                "connection_id": str(dc.database_connection_id),
                "is_time_based": chart.is_time_based,
                "x_axis": chart.x_axis,
                "y_axis": chart.y_axis,
            })

    duration_ms = int((time.time() - start_time) * 1000)
    logger.info(
        f"[EMBED][STEP 8] Embed HTML rendered for dashboard "
        f"{str(dashboard_id)[:8]}... — {len(charts)} charts — duration: {duration_ms}ms"
    )

    return {
        "dashboard_title": dashboard.title,
        "dashboard_id": str(dashboard.id),
        "charts": charts,
    }


async def get_embed_dashboard_data(
    token: ShareTokenModel,
    db: Session,
) -> dict:
    """Dashboard metadata using a validated share token."""
    return await get_embed_dashboard_data_by_dashboard_id(token.dashboard_id, db)


def _validate_iso_date(value: str, param_name: str) -> str:
    """
    Validate and normalise an ISO date string (YYYY-MM-DD).
    Raises HTTPException 400 if the format is invalid.
    """
    from datetime import date as _dt
    try:
        parsed = _dt.fromisoformat(value)
        return parsed.isoformat()  
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_date_param",
                "message": f"'{param_name}' must be a valid ISO date (YYYY-MM-DD).",
            },
        )


def _build_date_filtered_query(
    base_query: str,
    date_column: str,
    db_type: Optional[str],
    start_date: str,
    end_date: str,
) -> str:
    """
    Wrap *base_query* as a subquery and append a dialect-aware
    date-range WHERE clause using bind parameters (:start_date / :end_date).
    """
    db_type_lower = (db_type or "").lower()

    if "mysql" in db_type_lower or "mariadb" in db_type_lower:
        cast_expr = f'DATE(`{date_column}`)'  
    elif "mssql" in db_type_lower or "sqlserver" in db_type_lower:
        cast_expr = f'CAST([{date_column}] AS DATE)'
    elif "sqlite" in db_type_lower:
        cast_expr = f'DATE("{date_column}")'
    elif "databricks" in db_type_lower:
        cast_expr = f'CAST(`{date_column}` AS DATE)'
    else:
        cast_expr = f'"{date_column}"::date'

    return (
        f'SELECT * FROM ({base_query.strip().rstrip(";")}) AS _embed_subq '
        f'WHERE {cast_expr} BETWEEN :start_date AND :end_date'
    )


async def _connector_infer_date_column(connection, chart) -> Optional[str]:
    """The first column of the chart's query, when the chart has no stored x_axis."""
    clean_query = chart.query.strip().rstrip(";")
    probe = await run_connector_query(
        connection, f"SELECT * FROM ({clean_query}) AS _embed_infer_subq LIMIT 1"
    )
    return probe.column_names[0] if probe.column_names else None


async def _connector_embed_chart_data(
    connection,
    chart,
    chart_id: UUID,
    start_date: Optional[str],
    end_date: Optional[str],
) -> dict:
    """get_embed_chart_data_by_dashboard for a connector datasource, with the same response."""
    wants_date_filter = bool(start_date) and bool(end_date)
    try:
        date_col = chart.x_axis
        if wants_date_filter and not date_col:
            date_col = await _connector_infer_date_column(connection, chart)

        sql_to_run = chart.query
        apply_date_filter = wants_date_filter and bool(date_col)
        if apply_date_filter:
            start_date_val = _validate_iso_date(start_date, "start_date")
            end_date_val = _validate_iso_date(end_date, "end_date")
            # The gateway takes SQL text with no bind parameters. These two values are safe to
            # inline: _validate_iso_date returned them as YYYY-MM-DD.
            sql_to_run = (
                _build_date_filtered_query(
                    base_query=chart.query,
                    date_column=date_col,
                    db_type=connection.db_type,
                    start_date=start_date_val,
                    end_date=end_date_val,
                )
                .replace(":start_date", f"'{start_date_val}'")
                .replace(":end_date", f"'{end_date_val}'")
            )
        outcome = await run_connector_query(connection, sql_to_run)
    except ConnectorSqlFailed as e:
        logger.error(f"[EMBED] Connector chart query FAILED for chart {str(chart_id)[:8]}...")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch chart data",
        ) from e

    response = {
        "success": True,
        "data": [_json_safe_row(row) for row in outcome.as_dicts()],
        "chart_id": str(chart_id),
        "chart_type": chart.chart_type,
        "title": chart.title,
        "x_axis": chart.x_axis,
        "y_axis": chart.y_axis,
        "is_time_based": chart.is_time_based,
    }
    if outcome.truncated:
        response["truncated"] = True
    return response


async def _connector_embed_date_range(connection, chart) -> dict:
    """get_embed_chart_date_range for a connector datasource, with the same response.

    The expressions are the Postgres ones: the agent supports Postgres only for now.
    """
    date_col = chart.x_axis
    try:
        if not date_col:
            date_col = await _connector_infer_date_column(connection, chart)
        if not date_col:
            return {
                "min_date": None,
                "max_date": None,
                "date_column": None,
                "is_time_based": bool(chart.is_time_based),
            }
        clean_query = chart.query.strip().rstrip(";")
        outcome = await run_connector_query(
            connection,
            f'SELECT MIN("{date_col}"::date) AS min_date, MAX("{date_col}"::date) AS max_date '
            f'FROM ({clean_query}) AS _embed_range_subq',
        )
    except ConnectorSqlFailed:
        # Like the direct path: a date range that cannot be worked out is not an error.
        return {"min_date": None, "max_date": None, "date_column": date_col, "is_time_based": True}

    min_date = None
    max_date = None
    if outcome.rows:
        raw_min, raw_max = outcome.rows[0][0], outcome.rows[0][1]
        if raw_min is not None:
            min_date = _json_safe_value(raw_min)
            if isinstance(min_date, str) and "T" in min_date:
                min_date = min_date[:10]
        if raw_max is not None:
            max_date = _json_safe_value(raw_max)
            if isinstance(max_date, str) and "T" in max_date:
                max_date = max_date[:10]
    return {"min_date": min_date, "max_date": max_date, "date_column": date_col, "is_time_based": True}


async def get_embed_chart_data_by_dashboard(
    dashboard_id: UUID,
    chart_id: UUID,
    db: Session,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """
    Get chart data for an embedded dashboard using dashboard_id from JWT claims.
    Verifies chart membership via DashboardChartsModel, then runs the chart query.

    When *start_date* and *end_date* are provided (ISO YYYY-MM-DD strings) and
    the chart is time-based, the query is wrapped in a subquery with a
    parameterised WHERE clause — the stored query is never mutated.
    """
    start_time = time.time()

    dc = db.query(DashboardChartsModel).filter(
        and_(
            DashboardChartsModel.dashboard_id == dashboard_id,
            DashboardChartsModel.chart_id == chart_id,
        )
    ).first()

    if not dc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "chart_not_in_dashboard",
                "message": "Chart not found in this dashboard",
            },
        )

    chart = db.query(ChartModel).filter(ChartModel.id == chart_id).first()
    if not chart:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Chart not found",
        )

   
    connection = db.query(DatabaseConnectionModel).filter(
        DatabaseConnectionModel.id == dc.database_connection_id
    ).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database connection not found",
        )
    
    if is_connector_row(connection):
        return await _connector_embed_chart_data(connection, chart, chart_id, start_date, end_date)

    date_col = chart.x_axis
    apply_date_filter_intent = bool(start_date) and bool(end_date)

    logger.info(
        f"[EMBED][FILTER] Date filter check — chart {str(chart_id)[:8]}... "
        f"start_date={start_date!r} end_date={end_date!r} "
        f"is_time_based={chart.is_time_based!r} x_axis={chart.x_axis!r} "
        f"filter_intent={apply_date_filter_intent}"
    )

   
    try:
        from app.core.db import external_engine_manager
        from cryptography.fernet import Fernet
        import sqlalchemy

  
        fernet = Fernet(settings.ENCRYPTION_KEY.encode())
        decrypted_conn_str = fernet.decrypt(
            connection.db_connection_string.encode()
        ).decode()

        ext_engine = external_engine_manager.get_engine(
            connection_id=dc.database_connection_id,
            connection_string=decrypted_conn_str,
            db_type=connection.db_type,
        )

     
        if apply_date_filter_intent and not date_col:
            clean_query = chart.query.strip().rstrip(";")
            db_type_lower = (connection.db_type or "").lower()
            infer_sql = f"SELECT * FROM ({clean_query}) AS _embed_infer_subq LIMIT 1"
            if "mssql" in db_type_lower or "sqlserver" in db_type_lower:
                infer_sql = f"SELECT TOP 1 * FROM ({clean_query}) AS _embed_infer_subq"
            logger.info(
                f"[EMBED][FILTER] x_axis not stored — running inference query for chart "
                f"{str(chart_id)[:8]}..."
            )
            with ext_engine.connect() as conn:
                result = conn.execute(sqlalchemy.text(infer_sql))
                keys = list(result.keys())
                if keys:
                    date_col = keys[0]
                    logger.info(
                        f"[EMBED][FILTER] Inferred date column from query result: {date_col!r}"
                    )
                else:
                    logger.warning(
                        f"[EMBED][FILTER] Inference query returned no columns — "
                        f"cannot apply date filter for chart {str(chart_id)[:8]}..."
                    )

    
        apply_date_filter = apply_date_filter_intent and bool(date_col)

        if apply_date_filter_intent and not apply_date_filter:
            logger.warning(
                f"[EMBED][FILTER] Date filter SKIPPED — chart {str(chart_id)[:8]}... "
                f"reason=no_date_column_resolved "
                f"(x_axis={chart.x_axis!r}, is_time_based={chart.is_time_based!r})"
            )

        if apply_date_filter:
           
            start_date_val = _validate_iso_date(start_date, "start_date")
            end_date_val   = _validate_iso_date(end_date,   "end_date")
            sql_to_run = _build_date_filtered_query(
                base_query=chart.query,
                date_column=date_col,
                db_type=connection.db_type,
                start_date=start_date_val,
                end_date=end_date_val,
            )
            bind_params = {"start_date": start_date_val, "end_date": end_date_val}
            logger.info(
                f"[EMBED][FILTER] Date filter APPLIED — chart {str(chart_id)[:8]}... "
                f"date_column={date_col!r} range={start_date_val} → {end_date_val}"
            )
            logger.debug(
                f"[EMBED][FILTER] Filtered SQL for chart {str(chart_id)[:8]}...:\n{sql_to_run}"
            )
        else:
            sql_to_run  = chart.query
            bind_params = {}
        with ext_engine.connect() as conn:
            result = conn.execute(
                sqlalchemy.text(sql_to_run),
                bind_params,
            )
            rows = [_json_safe_row(dict(row._mapping)) for row in result]

        duration_ms = int((time.time() - start_time) * 1000)
        logger.info(
            f"[EMBED][STEP 10] Chart data fetched — chart {str(chart_id)[:8]}... "
            f"— {len(rows)} rows — date_filter: {apply_date_filter} — duration: {duration_ms}ms"
        )

        return {
            "success": True,
            "data": rows,
            "chart_id": str(chart_id),
            "chart_type": chart.chart_type,
            "title": chart.title,
            "x_axis": chart.x_axis,
            "y_axis": chart.y_axis,
            "is_time_based": chart.is_time_based,
        }

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        logger.error(
            f"[EMBED][STEP 10] Chart data fetch FAILED for chart {str(chart_id)[:8]}... "
            f"— duration: {duration_ms}ms — error: {str(e)}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch chart data",
        )


async def get_embed_chart_data(
    token: ShareTokenModel,
    chart_id: UUID,
    db: Session,
) -> dict:
    """Legacy entry point — delegates to dashboard-scoped fetch."""
    return await get_embed_chart_data_by_dashboard(
        token.dashboard_id, chart_id, db
    )


async def get_embed_chart_date_range(
    dashboard_id: UUID,
    chart_id: UUID,
    db: Session,
) -> dict:
    """
    Discover the min and max date values for a time-based chart.


    If the chart is not time-based, or the date column is unknown,
    both ``min_date`` and ``max_date`` will be ``None``.
    """
    start_time = time.time()

    # Verify chart membership
    dc = db.query(DashboardChartsModel).filter(
        and_(
            DashboardChartsModel.dashboard_id == dashboard_id,
            DashboardChartsModel.chart_id == chart_id,
        )
    ).first()

    if not dc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "chart_not_in_dashboard",
                "message": "Chart not found in this dashboard",
            },
        )

    chart = db.query(ChartModel).filter(ChartModel.id == chart_id).first()
    if not chart:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Chart not found",
        )

    connection = db.query(DatabaseConnectionModel).filter(
        DatabaseConnectionModel.id == dc.database_connection_id
    ).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database connection not found",
        )

    if is_connector_row(connection):
        return await _connector_embed_date_range(connection, chart)

    date_col = chart.x_axis
    db_type   = (connection.db_type or "").lower()

    try:
        from app.core.db import external_engine_manager
        from cryptography.fernet import Fernet
        import sqlalchemy

        fernet = Fernet(settings.ENCRYPTION_KEY.encode())
        decrypted_conn_str = fernet.decrypt(
            connection.db_connection_string.encode()
        ).decode()

        ext_engine = external_engine_manager.get_engine(
            connection_id=dc.database_connection_id,
            connection_string=decrypted_conn_str,
            db_type=connection.db_type,
        )

     
        clean_query = chart.query.strip().rstrip(";")
        if not date_col:
            infer_sql = f"SELECT * FROM ({clean_query}) AS _embed_infer_subq LIMIT 1"
            if "mssql" in db_type or "sqlserver" in db_type:
                infer_sql = f"SELECT TOP 1 * FROM ({clean_query}) AS _embed_infer_subq"
            with ext_engine.connect() as conn:
                result = conn.execute(sqlalchemy.text(infer_sql))
                keys = list(result.keys())
                if keys:
                    date_col = keys[0]

        if not date_col:
            return {
                "min_date": None,
                "max_date": None,
                "date_column": None,
                "is_time_based": bool(chart.is_time_based),
            }

  
        if "mysql" in db_type or "mariadb" in db_type:
            min_expr = f'MIN(DATE(`{date_col}`))'  
            max_expr = f'MAX(DATE(`{date_col}`))'  
        elif "mssql" in db_type or "sqlserver" in db_type:
            min_expr = f'MIN(CAST([{date_col}] AS DATE))'
            max_expr = f'MAX(CAST([{date_col}] AS DATE))'
        elif "sqlite" in db_type:
            min_expr = f'MIN(DATE("{date_col}"))'
            max_expr = f'MAX(DATE("{date_col}"))'
        elif "databricks" in db_type:
            min_expr = f'MIN(CAST(`{date_col}` AS DATE))'
            max_expr = f'MAX(CAST(`{date_col}` AS DATE))'
        
        elif "oracle" in db_type:
            min_expr = f'MIN(TRUNC("{date_col}"))'
            max_expr = f'MAX(TRUNC("{date_col}"))'
        else:
        
            min_expr = f'MIN("{date_col}"::date)'
            max_expr = f'MAX("{date_col}"::date)'

        range_sql = (
            f'SELECT {min_expr} AS min_date, {max_expr} AS max_date '
            f'FROM ({clean_query}) AS _embed_range_subq'
        )

        ext_engine = external_engine_manager.get_engine(
            connection_id=dc.database_connection_id,
            connection_string=decrypted_conn_str,
            db_type=connection.db_type,
        )
        with ext_engine.connect() as conn:
            row = conn.execute(sqlalchemy.text(range_sql)).fetchone()

        min_date = None
        max_date = None
        if row:
            raw_min, raw_max = row[0], row[1]
            if raw_min is not None:
                min_date = _json_safe_value(raw_min)

                if isinstance(min_date, str) and "T" in min_date:
                    min_date = min_date[:10]
            if raw_max is not None:
                max_date = _json_safe_value(raw_max)
                if isinstance(max_date, str) and "T" in max_date:
                    max_date = max_date[:10]

        duration_ms = int((time.time() - start_time) * 1000)
        logger.info(
            f"[EMBED] Date range discovered — chart {str(chart_id)[:8]}... "
            f"min={min_date} max={max_date} — duration: {duration_ms}ms"
        )

        return {
            "min_date": min_date,
            "max_date": max_date,
            "date_column": date_col,
            "is_time_based": True,
        }

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        logger.error(
            f"[EMBED] Date range discovery FAILED for chart {str(chart_id)[:8]}... "
            f"— duration: {duration_ms}ms — error: {str(e)}",
            exc_info=True,
        )
        return {
            "min_date": None,
            "max_date": None,
            "date_column": date_col,
            "is_time_based": True,
        }
