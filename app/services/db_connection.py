"""
Module for managing database connections in the project.

This module contains API routes for creating, retrieving, updating, and deleting
database connections for specific projects. It interacts with the database to store
and retrieve connection details securely, including connection strings, usernames,
passwords, and schemas. Encryption is used to protect sensitive data like passwords
and connection strings.

Key Features:
- Create a new database connection for a project.
- Retrieve all database connections associated with a project.
- Update an existing database connection.
- Delete a database connection.

Security:
- Sensitive data such as passwords and connection strings are encrypted before storage.
- Only authorized users with the appropriate permissions (ADD_DATASOURCE, VIEW_DATASOURCE,
  EDIT_DATASOURCE, DELETE_DATASOURCE) can access these operations.

Modules and Functions:
- create_database_connection: Creates and stores a new database connection for a project.
- get_connections: Retrieves all database connections associated with a specific project.
- update_db_connection: Updates an existing database connection.
- delete_db_connection: Deletes a database connection by its ID.

Exception Handling:
- HTTPExceptions are raised in case of errors, such as unsupported
  database types or invalid connections.
- If a user attempts to access a connection they don't have permission for, an exception is raised.

Module Dependencies:
- FastAPI: Provides the web framework for handling API routes.
- SQLAlchemy: Interacts with the database to store and retrieve connection data.
- app.utils.crypt: Provides encryption and decryption utilities for sensitive data.
- app.utils.schema_structure: Extracts schema structure from the database.
- app.utils.extract_table_name: Extracts table names from the database.
- app.utils.token_parser: Provides token-based user authentication and permission checks.
- app.utils.access: Manages user permissions and access control.

This module ensures that database connections are managed securely, and provides an interface for
users with the correct permissions to interact with these connections.
"""

from uuid import UUID, uuid4
from urllib.parse import urlparse, quote_plus, parse_qs, urlencode, urlunparse
import re
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from fastapi import HTTPException, status, Depends, Request, Response
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text
import requests

from app.core.db import get_db
from app.models.schema_models import ConnectionTableNameModel, DatabaseConnectionModel
from app.schemas import (
    DBConnectionRequest,
    DBConnectionResponse,
    UpdateDBConnectionRequest,
)
from app.utils.crypt import encrypt_string, decrypt_string
from app.utils.schema_structure import get_schema_structure, get_salesforce_schema_structure
from app.utils.extract_table_name import extract_table_names
from app.services.ds_graph import DSGraphBuilder

# Salesforce support is optional - only import if available
try:
    from app.services.salesforce_client import salesforce_client_manager
    SALESFORCE_AVAILABLE = True
except ImportError:
    salesforce_client_manager = None
    SALESFORCE_AVAILABLE = False
from app.utils.token_parser import get_current_user
from app.utils.constants import Permissions as Permission
from app.utils.access import require_permission


from fastapi import FastAPI, WebSocket, BackgroundTasks, HTTPException
from urllib.parse import urlparse, quote_plus, parse_qs
from uuid import uuid4
import asyncio, json
import logging

logger = logging.getLogger(__name__)

progress_queues: dict[str, asyncio.Queue] = {}


def _normalize_databricks_workspace_host(workspace_url: str) -> str:
    """
    Convert a Databricks workspace URL into a SQLAlchemy-compatible host string.
    Accepts values such as:
    - dbc-xxxx.cloud.databricks.com
    - https://dbc-xxxx.cloud.databricks.com
    """
    raw = (workspace_url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = (parsed.netloc or parsed.path or "").strip().strip("/")
    return host


def _extract_databricks_endpoint_ids(http_path: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract Databricks SQL warehouse ID or cluster ID from HTTP path.

    Returns:
        (warehouse_id, cluster_id)
    """
    normalized = (http_path or "").strip()
    if not normalized:
        return None, None

    warehouse_match = re.search(r"/sql/1\.0/warehouses/([^/?]+)", normalized)
    if warehouse_match:
        return warehouse_match.group(1), None

    # Typical cluster path example:
    # /sql/protocolv1/o/<org_id>/<cluster_id>
    cluster_match = re.search(r"/sql/protocolv1/o/[^/]+/([^/?]+)", normalized)
    if cluster_match:
        return None, cluster_match.group(1)

    return None, None


def _ensure_databricks_compute_running(
    workspace_host: str,
    access_token: str,
    http_path: str,
    timeout_seconds: int = 180,
) -> None:
    """
    Ensure Databricks SQL warehouse or cluster is running before use.
    """
    warehouse_id, cluster_id = _extract_databricks_endpoint_ids(http_path)
    if not warehouse_id and not cluster_id:
        logger.warning("Could not infer Databricks warehouse/cluster ID from http_path; skipping auto-start.")
        return

    base_url = f"https://{workspace_host}"
    headers = {"Authorization": f"Bearer {access_token}"}
    deadline = time.time() + timeout_seconds

    if warehouse_id:
        warehouse_url = f"{base_url}/api/2.0/sql/warehouses/{warehouse_id}"
        state = None
        try:
            resp = requests.get(warehouse_url, headers=headers, timeout=20)
            resp.raise_for_status()
            state = (resp.json().get("state") or "").upper()
        except Exception as e:
            logger.warning(f"Failed to fetch Databricks warehouse state: {e}")

        if state != "RUNNING":
            logger.info(f"Starting Databricks SQL warehouse {warehouse_id} (current_state={state or 'unknown'})")
            requests.post(f"{warehouse_url}/start", headers=headers, timeout=20).raise_for_status()

        while time.time() < deadline:
            resp = requests.get(warehouse_url, headers=headers, timeout=20)
            resp.raise_for_status()
            state = (resp.json().get("state") or "").upper()
            if state == "RUNNING":
                logger.info(f"Databricks SQL warehouse {warehouse_id} is RUNNING")
                return
            if state in {"DELETED", "DELETING"}:
                raise HTTPException(status_code=400, detail=f"Databricks warehouse {warehouse_id} is not available (state={state}).")
            time.sleep(5)

        raise HTTPException(status_code=504, detail=f"Timed out waiting for Databricks warehouse {warehouse_id} to start.")

    if cluster_id:
        cluster_get_url = f"{base_url}/api/2.0/clusters/get"
        cluster_start_url = f"{base_url}/api/2.0/clusters/start"
        state = None
        try:
            resp = requests.get(cluster_get_url, headers=headers, params={"cluster_id": cluster_id}, timeout=20)
            resp.raise_for_status()
            state = (resp.json().get("state") or "").upper()
        except Exception as e:
            logger.warning(f"Failed to fetch Databricks cluster state: {e}")

        if state != "RUNNING":
            logger.info(f"Starting Databricks cluster {cluster_id} (current_state={state or 'unknown'})")
            requests.post(
                cluster_start_url,
                headers=headers,
                json={"cluster_id": cluster_id},
                timeout=20,
            ).raise_for_status()

        while time.time() < deadline:
            resp = requests.get(cluster_get_url, headers=headers, params={"cluster_id": cluster_id}, timeout=20)
            resp.raise_for_status()
            state = (resp.json().get("state") or "").upper()
            if state == "RUNNING":
                logger.info(f"Databricks cluster {cluster_id} is RUNNING")
                return
            if state in {"ERROR", "TERMINATED"}:
                raise HTTPException(status_code=400, detail=f"Databricks cluster {cluster_id} is not available (state={state}).")
            time.sleep(5)

        raise HTTPException(status_code=504, detail=f"Timed out waiting for Databricks cluster {cluster_id} to start.")

async def extract_tables_in_background(task_id: str, connection_string: str, db_entry_id, db_type: str = None, salesforce_credentials: dict = None):
    # ensure the async queue exists and frontend can connect immediately
    queue = progress_queues.setdefault(task_id, asyncio.Queue())

    logger.debug(f"Starting background schema extraction for task_id: {task_id}, db_entry_id: {db_entry_id}, db_type: {db_type}")

    loop = asyncio.get_running_loop()

    # Run blocking schema extraction in a thread pool
    # get_schema_structure will use loop.call_soon_threadsafe(queue.put_nowait, msg)
    schema_structure = None
    try:
        logger.debug(f"Running schema extraction in executor for task_id: {task_id}")

        # Salesforce requires different schema extraction
        if db_type == "salesforce" and salesforce_credentials:
            schema_structure = await loop.run_in_executor(
                None,
                get_salesforce_schema_structure,
                salesforce_credentials,
                queue,
                loop
            )
        else:
            schema_structure = await loop.run_in_executor(
                None,                       # default ThreadPoolExecutor
                get_schema_structure,       # blocking function that now takes (connection_string, queue, loop)
                connection_string,
                queue,
                loop
            )
        logger.debug(f"Schema extraction completed for task_id: {task_id}, got schema: {schema_structure is not None}")
    except Exception as e:
        error_msg = f"Schema extraction failed: {str(e)}"
        logger.error(f"Schema extraction failed for task_id: {task_id}: {error_msg}", exc_info=True)
        await queue.put({"type": "error", "message": error_msg})
        # Don't save anything if extraction failed
        schema_structure = None

    # Save schema to DB only if extraction was successful
    # Create a new session for this background task to avoid holding onto request session
    from app.core.db import SessionLocal
    db = SessionLocal()
    try:
        if schema_structure is not None:
            try:
                # Validate that schema has tables before saving
                if isinstance(schema_structure, dict) and schema_structure.get("tables"):
                    num_tables = len(schema_structure.get("tables", []))
                    logger.debug(f"Schema has {num_tables} tables, attempting to save to DB for task_id: {task_id}")

                    db_entry = db.query(DatabaseConnectionModel).filter(DatabaseConnectionModel.id == db_entry_id).first()
                    if db_entry:
                        schema_json = json.dumps(schema_structure)
                        ds_graph_json = json.dumps(DSGraphBuilder(db_type=db_type).build(schema_structure))
                        logger.debug(f"Saving schema to DB entry {db_entry_id}, schema size: {len(schema_json)} bytes")
                        db_entry.db_schema = schema_json
                        db_entry.ds_graph_json = ds_graph_json
                        db.commit()
                        logger.info(f"Successfully saved schema with {num_tables} tables to DB entry {db_entry_id}")
                        await queue.put({"type": "info", "message": f"Successfully saved schema with {num_tables} tables"})
                        await queue.put({"type": "graph_completed", "message": "Data source graph generated"})
                    else:
                        error_msg = f"Database entry not found for ID: {db_entry_id}"
                        logger.error(error_msg)
                        await queue.put({"type": "error", "message": error_msg})
                else:
                    error_msg = "Schema extraction returned empty or invalid data. Not saving to database."
                    logger.warning(f"{error_msg} Schema structure: {type(schema_structure)}, has tables: {schema_structure.get('tables') if isinstance(schema_structure, dict) else 'N/A'}")
                    await queue.put({"type": "error", "message": error_msg})
            except Exception as e:
                logger.error(f"Failed saving schema to DB for task_id: {task_id}: {str(e)}", exc_info=True)
                await queue.put({"type": "error", "message": f"Failed saving schema to DB: {e}"})
                db.rollback()
        else:
            error_msg = "Schema extraction failed - no schema data to save"
            logger.error(f"{error_msg} for task_id: {task_id}")
            await queue.put({"type": "error", "message": error_msg})
    finally:
        # Always close the session to prevent connection leaks
        db.close()
        logger.debug(f"Background task session closed for task_id: {task_id}")

    # notify completion and sentinel
    logger.info(f"Schema extraction task completed for task_id: {task_id}")
    await queue.put({"type": "completed", "taskId": task_id})
    await queue.put(None)

    # CLEANUP: remove queue so we don't leak memory (safe - WS should have consumed sentinel)
    progress_queues.pop(task_id, None)


# @require_permission(Permission.ADD_DATASOURCE)xs
# async def create_database_connection(
#     project_id: UUID,
#     token_payload: dict,
#     data: DBConnectionRequest,
#     db: Session,
# ):
#     """
#     Creates and stores a new database connection for a given project.

#     Args:
#         project_id (UUID): The project to which this DB connection belongs.
#         token_payload (dict): Decoded token payload for permission checks.
#         data (DBConnectionRequest): Input data for creating the DB connection.
#         db (Session): SQLAlchemy DB session.

#     Returns:
#         DBConnectionResponse: Response containing the created DB connection ID.

#     Raises:
#         HTTPException: On invalid DB type or parsing errors.
#     """
#     if data.connection_string:
#         parsed_url = urlparse(data.connection_string)
#         # Normalize parsed components to safe strings
#         raw_username = parsed_url.username or ""
#         raw_password = parsed_url.password or ""
#         raw_host = parsed_url.hostname or ""
#         raw_port = parsed_url.port
#         raw_path = parsed_url.path or ""
#         raw_query = parsed_url.query or ""

#         # Determine final database name: prefer path, else fallback to provided db_name
#         parsed_db_name = raw_path.lstrip("/")
#         final_db_name = parsed_db_name or (data.db_name or "")

#         if not raw_host:
#             raise HTTPException(
#                 status_code=400, detail="Host is required in connection string"
#             )
#         if not final_db_name:
#             raise HTTPException(
#                 status_code=400,
#                 detail="Database name is required (missing in URL and payload)",
#             )

#         # Ensure quote_plus always gets a string
#         encoded_password = quote_plus(str(raw_password))

#         connection_string = (
#             f"{parsed_url.scheme}://{raw_username}:{encoded_password}@"
#             f"{raw_host}{':' + str(raw_port) if raw_port else ''}"
#             f"/{final_db_name}"
#             f"{'?' + raw_query if raw_query else ''}"
#         )

#         db_type = data.db_type
#         schema_structure = get_schema_structure(connection_string)

#         username = raw_username
#         password = str(raw_password)
#         host = raw_host
#         db_name = final_db_name

#     else:
#         db_type = (data.db_type or "").lower()
#         username = data.name or ""
#         password = data.password or ""
#         host = data.host or ""
#         db_name = data.db_name or ""

#         if not host:
#             raise HTTPException(status_code=400, detail="Host is required")
#         if not db_name:
#             raise HTTPException(status_code=400, detail="Database name is required")

#         # Parse host and port from host string (format: "host:port" or just "host")
#         # SQLAlchemy connection strings support port in the format: scheme://user:pass@host:port/db
#         if ":" in host and not host.startswith("["):  # IPv6 addresses start with [
#             # Host already contains port (e.g., "localhost:3306")
#             host_part = host
#         else:
#             # Host doesn't contain port, use default ports
#             if db_type == "postgres":
#                 host_part = f"{host}:5432"
#             elif db_type == "mysql":
#                 host_part = f"{host}:3306"
#             else:
#                 host_part = host

#         if db_type == "postgres":
#             connection_string = (
#                 f"postgresql://{username}:{quote_plus(str(password))}@{host_part}/{db_name}"
#             )
#         elif db_type == "mysql":
#             connection_string = f"mysql+pymysql://{username}:{quote_plus(str(password))}@{host_part}/{db_name}"
#         else:
#             raise HTTPException(status_code=400, detail="Unsupported database type.")

#         schema_structure = get_schema_structure(connection_string)

#     existing = (
#         db.query(DatabaseConnectionModel)
#         .filter(DatabaseConnectionModel.connection_name == data.connection_name)
#         .first()
#     )
#     if existing:
#         raise HTTPException(status_code=400, detail="Connection already exists")

#     db_entry = DatabaseConnectionModel(
#         id=uuid4(),
#         connection_name=data.connection_name,
#         db_connection_string=encrypt_string(connection_string),
#         db_schema=json.dumps(schema_structure),
#         db_username=username,
#         db_password=encrypt_string(password),
#         db_host_link=host,
#         db_name=db_name,
#         project_id=project_id,
#         consent_given=(
#             bool(data.consent_given) if data.consent_given is not None else False
#         ),
#         # db_description=data.db_description,
#         db_type=db_type,
#     )

#     db.add(db_entry)
#     db.flush()

#     table_names = extract_table_names(connection_string)
#     for table_name in table_names:
#         db.add(
#             ConnectionTableNameModel(
#                 table_name=table_name,
#                 connection_id=db_entry.id,
#             )
#         )

#     db.commit()

#     return DBConnectionResponse(db_entry_id=db_entry.id)

def parse_and_encode_connection_string(connection_string: str) -> str:
    """
    Parse a connection string and reconstruct it with proper URL encoding for all components.
    This handles special characters in passwords, usernames, and other URL components.
    
    The function manually parses the connection string to avoid issues with special characters
    that break standard URL parsing (like #, @, :, etc. in passwords).
    
    Args:
        connection_string: The raw connection string that may contain special characters
        
    Returns:
        Properly URL-encoded connection string
    """
    # Pattern: scheme://[username[:password]@]host[:port][/path][?query][#fragment]
    # We need to manually parse to handle special chars in password that break urlparse
    
    # Match scheme (e.g., "oracle+oracledb://", "postgresql://")
    scheme_match = re.match(r'^([^:]+://)', connection_string)
    if not scheme_match:
        # Not a valid URL format, return as-is
        return connection_string
    
    scheme = scheme_match.group(1)
    rest = connection_string[len(scheme):]
    
    # Remove fragment if present (everything after #) - but preserve it if it's part of password
    # We'll handle this by finding the last @ before / or ? to separate auth from host
    # The fragment separator # should only be considered if it's after the host part
    
    # Find positions of key separators
    at_positions = [i for i, char in enumerate(rest) if char == '@']
    query_pos = rest.find('?')
    
    # Determine the boundary between auth and host.
    # Passwords may legally contain / and #, so we must NOT use slash_pos as a
    # boundary — doing so would mistake a / inside the password for the start of
    # the path, resulting in the wrong @ being chosen as the auth/host separator.
    # Hostnames and ports can never contain @, so the last @ that appears before
    # the query-string marker (?) is always the correct auth/host separator.
    boundary = query_pos if query_pos != -1 else len(rest)
    
    # Find the last @ before the boundary (this separates auth from host)
    auth_end_pos = -1
    for at_pos in reversed(at_positions):
        if at_pos < boundary:
            auth_end_pos = at_pos
            break
    
    if auth_end_pos != -1:
        # We have authentication part
        auth_part = rest[:auth_end_pos]
        host_part = rest[auth_end_pos + 1:]
        
        # Split username and password (password may contain :, @, #, etc.)
        colon_pos = auth_part.find(':')
        if colon_pos != -1:
            username = auth_part[:colon_pos]
            password = auth_part[colon_pos + 1:]
            # URL encode username and password (quote_plus handles special chars)
            username_encoded = quote_plus(username, safe='')
            password_encoded = quote_plus(password, safe='')
            auth_encoded = f"{username_encoded}:{password_encoded}"
        else:
            # Only username, no password
            username_encoded = quote_plus(auth_part, safe='')
            auth_encoded = username_encoded
    else:
        # No authentication
        auth_encoded = ""
        host_part = rest
    
    # Parse host_part: host[:port][/path][?query]
    # Extract query string (before any # fragment)
    if '?' in host_part:
        query_start = host_part.find('?')
        host_path = host_part[:query_start]
        query_part = host_part[query_start + 1:]
        # Remove fragment from query if present
        if '#' in query_part:
            query_part = query_part[:query_part.find('#')]
        # Parse and re-encode query parameters
        query_params = parse_qs(query_part)
        encoded_query = urlencode(query_params, doseq=True)
    else:
        # Remove fragment if present
        if '#' in host_part:
            host_path = host_part[:host_part.find('#')]
        else:
            host_path = host_part
        encoded_query = ""
    
    # Split host:port from path
    if '/' in host_path:
        host_port, path = host_path.split('/', 1)
        path = '/' + path
    else:
        host_port = host_path
        path = ""
    
    # Reconstruct URL
    if auth_encoded:
        netloc = f"{auth_encoded}@{host_port}"
    else:
        netloc = host_port
    
    # Reconstruct full URL
    encoded_url = urlunparse((
        scheme.rstrip('://'),
        netloc,
        path,
        "",
        encoded_query,
        ""
    ))
    
    return encoded_url


async def create_database_connection(
    project_id: UUID,
    token_payload: dict,
    data: DBConnectionRequest,
    db: Session,
    background_tasks: BackgroundTasks
):
    # --- Parse connection string or construct from fields ---
    if data.connection_string:
        # Parse and properly encode the connection string
        connection_string = parse_and_encode_connection_string(data.connection_string)
        
        # If connection string is provided, parse it to extract components
        # For Oracle, expect format: oracle+oracledb://username:password@host:port/?service_name=service_name
        parsed_url = urlparse(connection_string)
        username = parsed_url.username or ""
        password = parsed_url.password or ""
        host = parsed_url.hostname or ""
        port = parsed_url.port
        
        # Check if it's Oracle
        if parsed_url.scheme and "oracle" in parsed_url.scheme.lower():
            db_type = "oracledb"
            # Extract service_name from query params
            query_params = parse_qs(parsed_url.query)
            service_name = query_params.get("service_name", [None])[0] or parsed_url.path.lstrip("/") or data.db_name or ""
            db_name = service_name
            # Use username/password from URL if available, otherwise from data fields
            username = username or data.username or data.name or ""
            password = password or data.password or ""
            # connection_string is already properly encoded by parse_and_encode_connection_string above
        elif parsed_url.scheme and "databricks" in parsed_url.scheme.lower():
            db_type = "databricks"
            query_params = parse_qs(parsed_url.query or "")
            catalog_name = (query_params.get("catalog", [None])[0] or "").strip()
            schema_name = (query_params.get("schema", [None])[0] or "").strip()
            http_path = (query_params.get("http_path", [None])[0] or "").strip()
            token_password = parsed_url.password or ""
            db_name = f"{catalog_name}.{schema_name}" if catalog_name and schema_name else (data.db_name or "")
            if not host or not db_name or not http_path or not token_password:
                raise HTTPException(
                    status_code=400,
                    detail="Databricks connection string must include host, http_path, catalog, schema and token password",
                )
            username = username or "token"
            password = token_password or data.password or ""
        else:
            # Other database types
            db_name = parsed_url.path.lstrip("/") or data.db_name or ""
            if not host or not db_name:
                raise HTTPException(status_code=400, detail="Host and database name are required")
            # Use username/password from URL if available, otherwise from data fields
            username = username or data.username or data.name or ""
            password = password or data.password or ""
            # connection_string is already correctly encoded by parse_and_encode_connection_string.
            # Rebuilding it from parts would drop query parameters (e.g. ?sslmode=require),
            # so we use the encoded string directly and only re-encode the password when the
            # username/password came from data fields rather than the URL.
            if not parsed_url.username and (data.username or data.name or data.password):
                query_suffix = f"?{parsed_url.query}" if parsed_url.query else ""
                connection_string = (
                    f"{parsed_url.scheme}://{username}:{quote_plus(str(password))}@"
                    f"{host}{':' + str(port) if port else ''}/{db_name}{query_suffix}"
                )
            db_type = data.db_type
    else:
        # Construct connection string from individual fields
        db_type = (data.db_type or "").lower()
        password = data.password or ""
        host = data.host or ""
        db_name = data.db_name or ""
        
        if db_type == "postgres":
            username = data.username or data.name or ""
            if not all([username, password, host, db_name]):
                raise HTTPException(status_code=400, detail="PostgreSQL requires username (or name), password, host, and database name")

            # Build query-string params from optional SSL / schema / extra fields
            query_params: dict = {}
            schema_name = (getattr(data, "schema_name", None) or "").strip()
            if schema_name:
                query_params["options"] = f"-csearch_path={schema_name}"
            if getattr(data, "use_ssl", None):
                query_params.setdefault("sslmode", "require")
            extra = (getattr(data, "additional_params", None) or "").strip()
            if extra:
                # additional_params may be "sslmode=require&connect_timeout=10" style
                for kv in extra.split("&"):
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        query_params[k.strip()] = v.strip()
                    elif kv.strip():
                        query_params[kv.strip()] = ""
            query_suffix = ("?" + urlencode(query_params)) if query_params else ""
            connection_string = f"postgresql://{username}:{quote_plus(str(password))}@{host}/{db_name}{query_suffix}"
        elif db_type == "mysql":
            username = data.username or data.name or ""
            if not all([username, password, host, db_name]):
                raise HTTPException(status_code=400, detail="MySQL requires username (or name), password, host, and database name")
            # Build SSL / extra params for MySQL too
            query_params_mysql: dict = {}
            if getattr(data, "use_ssl", None):
                query_params_mysql["ssl"] = "true"
            extra_mysql = (getattr(data, "additional_params", None) or "").strip()
            if extra_mysql:
                for kv in extra_mysql.split("&"):
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        query_params_mysql[k.strip()] = v.strip()
            query_suffix_mysql = ("?" + urlencode(query_params_mysql)) if query_params_mysql else ""
            connection_string = f"mysql+pymysql://{username}:{quote_plus(str(password))}@{host}/{db_name}{query_suffix_mysql}"
        elif db_type == "oracledb":
            # For Oracle, db_name is actually the service_name
            service_name = db_name
            username = data.username or data.name or ""
            port = "1521"  # Default Oracle port
            if ":" in host:
                # Host contains port
                host, port = host.split(":", 1)

            if not all([username, password, host, service_name]):
                raise HTTPException(status_code=400, detail="Oracle requires username (or name), password, host, and service_name (db_name)")

            # Build Oracle connection string in the specified format
            connection_string = (
                f"oracle+oracledb://{username}:{quote_plus(str(password))}@{host}:{port}/"
                f"?service_name={service_name}"
            )
        elif db_type == "salesforce":
            # Check if Salesforce support is available
            if not SALESFORCE_AVAILABLE:
                raise HTTPException(
                    status_code=400,
                    detail="Salesforce integration is not available. Please install simple-salesforce: pip install simple-salesforce"
                )

            # Salesforce uses OAuth2 session-based authentication only
            # Frontend sends: session_id (OAuth access_token) and instance_url
            session_id = getattr(data, 'session_id', None) or ""
            instance_url = getattr(data, 'instance_url', None) or ""

            # Validate required fields
            if not session_id:
                raise HTTPException(
                    status_code=400,
                    detail="Salesforce requires session_id (OAuth access_token)"
                )

            if not instance_url:
                raise HTTPException(
                    status_code=400,
                    detail="Salesforce requires instance_url (e.g., https://na45.salesforce.com)"
                )

            # Validate instance_url is NOT login.salesforce.com (that's the auth endpoint, not the API)
            instance_url_lower = instance_url.lower()
            if "login.salesforce.com" in instance_url_lower or "test.salesforce.com" in instance_url_lower:
                raise HTTPException(
                    status_code=400,
                    detail="instance_url must be your Salesforce instance URL (e.g., https://na45.salesforce.com), not the login URL"
                )

            # Ensure instance_url has https:// prefix
            if not instance_url.startswith("https://"):
                if instance_url.startswith("http://"):
                    instance_url = instance_url.replace("http://", "https://")
                else:
                    instance_url = f"https://{instance_url}"

            # Validate Salesforce OAuth session before creating the connection
            logger.info(f"Validating Salesforce OAuth session for instance: {instance_url}")
            test_result = salesforce_client_manager.test_connection(
                session_id=session_id,
                instance_url=instance_url,
            )

            if not test_result.get("success"):
                error_msg = test_result.get("error", "Salesforce OAuth session validation failed")
                logger.error(f"Salesforce connection validation failed: {error_msg}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Salesforce connection failed: {error_msg}"
                )

            org_name = test_result.get('org_name', 'Unknown')
            logger.info(f"Salesforce OAuth session validated successfully for org: {org_name}")

            # For Salesforce OAuth2, we use the existing fields:
            # - db_username: Not used (set to org_name for display)
            # - db_password: Stores session_id (encrypted)
            # - db_host_link: Stores instance_url
            # - db_name: Not used for Salesforce
            # - db_connection_string: Placeholder URL for identification
            connection_string = f"salesforce://{instance_url}"
            username = org_name  # Store org name for display purposes
            password = session_id  # Store session_id in password field (will be encrypted)
            host = instance_url
            db_name = None
        elif db_type == "databricks":
            workspace_url = getattr(data, "workspace_url", None) or data.host or ""
            http_path = getattr(data, "http_path", None) or ""
            catalog_name = getattr(data, "catalog_name", None) or ""
            schema_name = getattr(data, "schema_name", None) or ""
            access_token = getattr(data, "access_token", None) or data.password or ""

            host = _normalize_databricks_workspace_host(workspace_url)
            if not all([host, http_path.strip(), catalog_name.strip(), schema_name.strip(), access_token.strip()]):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Databricks requires workspace_url, http_path, catalog_name, "
                        "schema_name, and access_token"
                    ),
                )

            normalized_http_path = http_path.strip()
            if not normalized_http_path.startswith("/"):
                normalized_http_path = f"/{normalized_http_path}"
            endpoint_kind = "warehouse" if "/sql/1.0/warehouses/" in normalized_http_path else "cluster"
            logger.info(
                "Creating Databricks connection using %s HTTP path",
                endpoint_kind,
            )

            # Try to auto-start Databricks compute endpoint if it is down.
            _ensure_databricks_compute_running(
                workspace_host=host,
                access_token=access_token.strip(),
                http_path=normalized_http_path,
            )

            username = "token"
            password = access_token
            db_name = f"{catalog_name.strip()}.{schema_name.strip()}"
            host = host
            connection_string = (
                f"databricks://token:{quote_plus(str(access_token))}@{host}"
                f"?http_path={quote_plus(normalized_http_path, safe='/')}"
                f"&catalog={quote_plus(catalog_name.strip())}"
                f"&schema={quote_plus(schema_name.strip())}"
            )
        else:
            raise HTTPException(status_code=400, detail="Unsupported database type.")

    # Check for duplicate connection
    existing = db.query(DatabaseConnectionModel).filter(
        DatabaseConnectionModel.connection_name == data.connection_name
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Connection already exists")

    # Create DB entry without schema
    db_name_value = db_name
    db_entry = DatabaseConnectionModel(
        id=uuid4(),
        connection_name=data.connection_name,
        db_connection_string=encrypt_string(connection_string),
        db_schema=None,  # schema will be filled in background
        db_username=username,
        db_password=encrypt_string(password),
        db_host_link=host,
        db_name=db_name_value,
        project_id=project_id,
        consent_given=bool(data.consent_given) if data.consent_given is not None else False,
        db_type=db_type,
    )
    db.add(db_entry)
    db.commit()
    db.refresh(db_entry)

    # --- Create task ID and start background schema extraction ---
    task_id = str(uuid4())
    # Don't pass db session to background task - it will create its own to avoid connection leaks

    # Handle Salesforce differently - pass OAuth2 credentials instead of connection string
    if db_type == "salesforce":
        # For schema extraction, pass the OAuth2 credentials
        # Note: password contains session_id, host contains instance_url
        salesforce_creds = {
            "session_id": password,  # password field stores session_id for Salesforce
            "instance_url": host,    # host field stores instance_url for Salesforce
        }
        background_tasks.add_task(
            extract_tables_in_background,
            task_id,
            connection_string,
            db_entry.id,
            db_type,
            salesforce_creds
        )
        # For Salesforce, we don't know the table count upfront
        tables_count = 0
    else:
        background_tasks.add_task(extract_tables_in_background, task_id, connection_string, db_entry.id, db_type)
        # Do NOT call extract_table_names() here — it opens a live DB connection
        # synchronously and blocks the HTTP response, causing a client-side timeout
        # on slow or remote databases. The background task already extracts and
        # stores the full schema; the count is updated from there.
        tables_count = 0

    # --- Return immediately ---
    return {"taskId": task_id, "tablesCount": tables_count, "connectionId": str(db_entry.id)}




@require_permission(Permission.VIEW_DATASOURCE)
async def get_connections(
    project_id: UUID,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Retrieves all database connections for a project.

    Args:
        project_id (UUID): Project identifier.
        request (Request): FastAPI request object.
        response (Response): FastAPI response object.
        db (Session): SQLAlchemy DB session.
        token_payload (dict): Authenticated user's token payload.

    Returns:
        dict: List of decrypted database connections.
    """
    try:
        user_id_str = token_payload.get("sub")
        if not user_id_str:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload",
            )

        UUID(user_id_str)  # Validate UUID

        connections = (
            db.query(DatabaseConnectionModel)
            .filter(DatabaseConnectionModel.project_id == project_id)
            .all()
        )

        connections_list = []
        for conn in connections:
            if conn.connection_mode == "connector":
                # A connector datasource has no credentials: VizAI never holds the client's
                # database login, so there is nothing to decrypt. Same keys as below.
                connections_list.append(
                    {
                        "id": str(conn.id),
                        "project_id": str(conn.project_id),
                        "db_connection_string": None,
                        "db_schema": conn.db_schema,
                        "ds_graph_json": conn.ds_graph_json,
                        "db_username": None,
                        "db_password": None,
                        "db_host_link": None,
                        "db_name": None,
                        "db_type": conn.db_type,
                        "name": conn.connection_name,
                        "consent_given": conn.consent_given,
                        "has_ds_graph": bool(conn.ds_graph_json),
                        "connection_mode": "connector",
                        "connector_id": str(conn.connector_id) if conn.connector_id else None,
                        "status": conn.status,
                    }
                )
                continue
            if conn.db_type == "spreadsheet":
                decrypted_password = conn.db_password
                decrypted_connection_string = conn.db_connection_string
            else:
                decrypted_password = decrypt_string(conn.db_password)
                decrypted_connection_string = decrypt_string(conn.db_connection_string)

            connections_list.append(
                {
                    "id": str(conn.id),
                    "project_id": str(conn.project_id),
                    "db_connection_string": decrypted_connection_string,
                    "db_schema": conn.db_schema,
                    "ds_graph_json": conn.ds_graph_json,
                    "db_username": conn.db_username,
                    "db_password": decrypted_password,
                    "db_host_link": conn.db_host_link,
                    "db_name": conn.db_name,
                    "db_type": conn.db_type,
                    "name": conn.connection_name,
                    "consent_given": conn.consent_given,
                    "has_ds_graph": bool(conn.ds_graph_json),
                }
            )

        return {
            "message": "Connections retrieved successfully",
            "connections": connections_list,
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)
        ) from e


@require_permission(Permission.EDIT_DATASOURCE)
async def update_db_connection(
    connection_id: UUID,
    data: UpdateDBConnectionRequest,
    db: Session,
    token_payload: dict,
):
    """
    Updates an existing database connection.

    Args:
        connection_id (UUID): ID of the DB connection to update.
        data (UpdateDBConnectionRequest): Updated connection details.
        db (Session): SQLAlchemy DB session.

    Returns:
        dict: Success message on update.

    Raises:
        HTTPException: If connection is not found.
    """
    db_connection = (
        db.query(DatabaseConnectionModel)
        .filter(DatabaseConnectionModel.id == connection_id)
        .first()
    )
    if not db_connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database connection not found",
        )

    # A connector row never holds credentials, and its db_type stays the real dialect.
    if db_connection.connection_mode == "connector" and any(
        (
            data.db_connection_string,
            data.db_username,
            data.db_password,
            data.db_host_link,
            data.db_name,
            data.db_type,
        )
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A connector datasource has no connection details to change.",
        )

    if data.connection_name:
        db_connection.connection_name = data.connection_name
    if data.db_connection_string:
        # Parse and encode the connection string to handle special characters
        encoded_connection_string = parse_and_encode_connection_string(data.db_connection_string)
        db_connection.db_connection_string = encrypt_string(encoded_connection_string)
    if data.db_schema:
        db_connection.db_schema = data.db_schema
    if data.db_username:
        db_connection.db_username = data.db_username
    if data.db_password:
        db_connection.db_password = data.db_password
    if data.db_host_link:
        db_connection.db_host_link = data.db_host_link
    if data.db_name:
        db_connection.db_name = data.db_name
    if data.db_type:
        db_connection.db_type = data.db_type

    # Invalidate cached engine if connection string changed
    if data.db_connection_string:
        from app.core.db import external_engine_manager
        external_engine_manager.invalidate_engine(connection_id)
        logger.info(f"Invalidated engine cache for updated connection {connection_id}")

    db.commit()
    db.refresh(db_connection)

    return {"message": "Database connection updated successfully"}


@require_permission(Permission.DELETE_DATASOURCE)
async def delete_db_connection(
    connection_id: UUID,
    db: Session,
    token_payload: dict,
):
    """
    Deletes a database connection by ID.

    Args:
        connection_id (UUID): ID of the DB connection to delete.
        db (Session): SQLAlchemy DB session.

    Returns:
        dict: Success message on deletion.

    Raises:
        HTTPException: If the connection is not found.
    """
    db_connection = (
        db.query(DatabaseConnectionModel)
        .filter(DatabaseConnectionModel.id == connection_id)
        .first()
    )
    if not db_connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database connection not found",
        )

    # Invalidate cached engine before deletion
    from app.core.db import external_engine_manager
    external_engine_manager.invalidate_engine(connection_id)
    logger.info(f"Invalidated engine cache for deleted connection {connection_id}")

    db.delete(db_connection)
    db.commit()

    return {"message": "Database connection deleted successfully"}


@require_permission(Permission.VIEW_DATASOURCE)
async def get_connection_ds_graph(
    connection_id: UUID,
    db: Session,
    token_payload: dict,
):
    db_connection = (
        db.query(DatabaseConnectionModel)
        .filter(DatabaseConnectionModel.id == connection_id)
        .first()
    )
    if not db_connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database connection not found",
        )

    if not db_connection.ds_graph_json:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source graph not available yet. Complete setup first.",
        )

    try:
        graph_payload = json.loads(db_connection.ds_graph_json)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Stored DS graph is invalid: {str(e)}",
        ) from e

    return {
        "message": "DS graph retrieved successfully",
        "connection_id": str(db_connection.id),
        "connection_name": db_connection.connection_name,
        "db_type": db_connection.db_type,
        "graph": graph_payload,
    }


async def get_connection_stats(
    project_id: UUID,
    db: Session,
    token_payload: dict,
):
    """
    Get statistics for database connections in a project.
    
    Args:
        project_id (UUID): The project ID.
        db (Session): The database session.
        token_payload (dict): The token payload.
        
    Returns:
        dict: Statistics about database connections including:
            - total_connections: Total number of connections
            - active_connections: Number of connections with status=True
            - inactive_connections: Number of connections with status=False
            - project_id: The project ID
    """
    from app.models.schema_models import UserProjectRoleModel, UserModel, ProjectModel
    
    user_id = UUID(token_payload.get("sub"))
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User ID not found in token payload"
        )
    
    # Verify project exists
    project = db.query(ProjectModel).filter(ProjectModel.id == project_id).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    # Verify user has access to this project
    user_role = (
        db.query(UserProjectRoleModel)
        .filter(
            UserProjectRoleModel.user_id == user_id,
            UserProjectRoleModel.project_id == project_id,
        )
        .first()
    )
    
    user = db.query(UserModel).filter(UserModel.id == user_id).first()
    
    if not user_role and not user.is_super:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User does not have access to this project"
        )
    
    # Get all connections for the project
    all_connections = (
        db.query(DatabaseConnectionModel)
        .filter(DatabaseConnectionModel.project_id == project_id)
        .all()
    )
    
    total_connections = len(all_connections)
    active_connections = sum(1 for conn in all_connections if conn.status is True)
    inactive_connections = sum(1 for conn in all_connections if conn.status is False or conn.status is None)
    
    return {
        "total_connections": total_connections,
        "active_connections": active_connections,
        "inactive_connections": inactive_connections,
        "project_id": str(project_id)
    }


async def check_and_update_connections(
    project_id: UUID,
    db: Session,
    token_payload: dict,
) -> Dict[str, Any]:
    """
    Check all database connections for a project and update their status.
    
    This function attempts to connect to each database, updates the status field
    (True if connection succeeds, False if it fails), and updates the last_checked
    timestamp.
    
    Args:
        project_id (UUID): The project ID.
        db (Session): The database session.
        token_payload (dict): The token payload.
        
    Returns:
        dict: Results of connection checks including status updates.
    """
    from app.models.schema_models import UserProjectRoleModel, UserModel, ProjectModel
    
    user_id = UUID(token_payload.get("sub"))
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User ID not found in token payload"
        )
    
    # Verify project exists
    project = db.query(ProjectModel).filter(ProjectModel.id == project_id).first()
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found"
        )
    
    # Verify user has access to this project
    user_role = (
        db.query(UserProjectRoleModel)
        .filter(
            UserProjectRoleModel.user_id == user_id,
            UserProjectRoleModel.project_id == project_id,
        )
        .first()
    )
    
    user = db.query(UserModel).filter(UserModel.id == user_id).first()
    
    if not user_role and not user.is_super:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User does not have access to this project"
        )
    
    # Get all connections for the project
    connections = (
        db.query(DatabaseConnectionModel)
        .filter(DatabaseConnectionModel.project_id == project_id)
        .all()
    )
    
    if not connections:
        return {
            "message": "No database connections found for this project",
            "project_id": str(project_id),
            "total_checked": 0,
            "successful_connections": 0,
            "failed_connections": 0,
            "results": []
        }
    
    results = []
    successful = 0
    failed = 0
    
    for connection in connections:
        check_result = await _test_single_connection(connection, db)
        results.append(check_result)
        
        if check_result["status"]:
            successful += 1
        else:
            failed += 1
    
    return {
        "message": "Connection check completed",
        "project_id": str(project_id),
        "total_checked": len(connections),
        "successful_connections": successful,
        "failed_connections": failed,
        "results": results
    }


async def _test_single_connection(
    connection: DatabaseConnectionModel,
    db: Session,
) -> Dict[str, Any]:
    """
    Test a single database connection and update its status.

    Args:
        connection (DatabaseConnectionModel): The database connection to test.
        db (Session): The database session for updating the record.

    Returns:
        dict: Result of the connection test.
    """
    connection_id = str(connection.id)
    connection_name = connection.connection_name
    current_time = datetime.utcnow()

    test_engine = None
    try:
        # A connector datasource has no engine to open: its agent dials out to the gateway, so
        # "connected" means an agent instance is connected right now.
        if connection.connection_mode == "connector":
            from app.connector_integration.gateway_host import get_gateway

            gateway = get_gateway()
            online = bool(gateway and connection.connector_id and gateway.is_online(str(connection.connector_id)))
            connection.status = online
            connection.last_checked = current_time
            db.commit()
            return {
                "connection_id": connection_id,
                "connection_name": connection_name,
                "status": online,
                "last_checked": current_time.isoformat(),
                "error_message": None if online else "Connector is offline (no agent is connected)",
            }

        # Handle Salesforce connections differently
        if connection.db_type == "salesforce":
            if not SALESFORCE_AVAILABLE:
                raise Exception("Salesforce integration is not available. Please install simple-salesforce.")

            # Test Salesforce connection using the client manager
            # For Salesforce OAuth2: db_password = session_id (encrypted), db_host_link = instance_url
            decrypted_session_id = decrypt_string(connection.db_password) if connection.db_password else ""

            test_result = salesforce_client_manager.test_connection(
                session_id=decrypted_session_id,
                instance_url=connection.db_host_link,
            )

            if test_result.get("success"):
                connection.status = True
                connection.last_checked = current_time
                db.commit()

                return {
                    "connection_id": connection_id,
                    "connection_name": connection_name,
                    "status": True,
                    "last_checked": current_time.isoformat(),
                    "error_message": None
                }
            else:
                raise Exception(test_result.get("error", "Salesforce connection failed"))

        # Standard database connection test
        decrypted_connection_string = decrypt_string(connection.db_connection_string)

        # Create a test engine
        create_engine_kwargs = {
            "pool_pre_ping": True,
        }
        if connection.db_type not in ("oracledb", "oracle", "databricks"):
            create_engine_kwargs["connect_args"] = {"connect_timeout": 10}

        test_engine = create_engine(
            decrypted_connection_string,
            **create_engine_kwargs
        )

        # Attempt to connect and execute a simple query
        with test_engine.connect() as conn:
            # Execute a simple query to verify the connection works
            conn.execute(text("SELECT 1"))

        # Connection successful - update status to True
        connection.status = True
        connection.last_checked = current_time
        db.commit()

        return {
            "connection_id": connection_id,
            "connection_name": connection_name,
            "status": True,
            "last_checked": current_time.isoformat(),
            "error_message": None
        }

    except Exception as e:
        # Connection failed - update status to False
        connection.status = False
        connection.last_checked = current_time
        db.commit()

        error_msg = str(e)
        logger.error(
            f"Database connection test failed for {connection_name} (ID: {connection_id}): {error_msg}",
            exc_info=True
        )
        # Truncate long error messages
        if len(error_msg) > 200:
            error_msg = error_msg[:200] + "..."

        return {
            "connection_id": connection_id,
            "connection_name": connection_name,
            "status": False,
            "last_checked": current_time.isoformat(),
            "error_message": error_msg
        }
    finally:
        # Dispose test engine to prevent connection leaks
        if test_engine is not None:
            test_engine.dispose()
