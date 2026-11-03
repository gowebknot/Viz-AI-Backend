# from sqlalchemy import create_engine, inspect

# from datetime import datetime, timedelta
# from app.utils.crypt import decrypt_string

# def get_schema_structure(connection_string: str):
#     engine = create_engine(connection_string)
#     inspector = inspect(engine)

#     schema_info = {"tables": []}
#     # max_date = datetime.now().date()
#     # min_date = max_date - timedelta(days=183) 
#     min_date= datetime.fromisoformat("2003-01-06")
#     max_date= datetime.fromisoformat("2005-06-11")

#     try:
#         with engine.connect() as connection:
#             for table_name in inspector.get_table_names():
#                 columns = inspector.get_columns(table_name)
#                 primary_keys = inspector.get_pk_constraint(table_name)
#                 foreign_keys = [
#                     {"column": fk["constrained_columns"][0], "references": fk["referred_table"]}
#                     for fk in inspector.get_foreign_keys(table_name)
#                 ]

#                 schema_info["tables"].append({
#                     "name": table_name,
#                     "columns": [
#                         {"name": col["name"], "type": str(col["type"])}
#                         for col in columns
#                     ],
#                     "primary_keys": primary_keys,
#                     "foreign_keys": foreign_keys
#                 })

#         schema_info["min_date"] = min_date.isoformat()
#         schema_info["max_date"] = max_date.isoformat()
#         print(f"Database Date Range: Min Date: {min_date}, Max Date: {max_date}")

#     except Exception as e:
#         print(f"Error fetching schema information: {e}. Returning schema info with default date range.")
#         schema_info["min_date"] = None
#         schema_info["max_date"] = None

#     return schema_info

# blocking sync function — runs in a thread
from sqlalchemy import create_engine, inspect, text
from datetime import datetime
from typing import Optional, Dict, Any, List
import json
import logging
import re
from urllib.parse import urlparse, parse_qs

try:
    from simple_salesforce import Salesforce
    from simple_salesforce.exceptions import SalesforceAuthenticationFailed
    SALESFORCE_AVAILABLE = True
except ImportError:
    SALESFORCE_AVAILABLE = False

# Set up logger
logger = logging.getLogger(__name__)

def _extract_enum_map(connection, db_type: str, **kwargs) -> Dict[str, List[str]]:
    """
    Dispatcher that extracts enum/constrained-value columns for the given db_type.

    Returns a dict whose keys are either:
      - "table.column" and "schema.table.column" (PostgreSQL)
      - A "table.column" composite key (MySQL / Oracle, e.g. "units.status")

    Values are the ordered list of allowed string literals.
    Always returns {} on any failure (fail-open).
    """
    try:
        db_lower = (db_type or "").lower()

        if db_lower in ("postgres", "postgresql", "pg"):
            # Key by table and column. SQLAlchemy often reports these columns
            # as USER-DEFINED or ENUM, so the type name is not in that string.
            rows = connection.execute(text(
                """
                SELECT n.nspname AS schema_name,
                       c.relname AS table_name,
                       a.attname AS column_name,
                       e.enumlabel AS label
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_type t ON t.oid = a.atttypid
                JOIN pg_enum e ON e.enumtypid = t.oid
                WHERE a.attnum > 0
                  AND NOT a.attisdropped
                  AND c.relkind IN ('r', 'p', 'f')
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY n.nspname, c.relname, a.attname, e.enumsortorder
                """
            )).mappings().all()
            grouped: Dict[str, List[str]] = {}
            for row in rows:
                schema_name = str(row["schema_name"]).lower()
                table_name = str(row["table_name"]).lower()
                column_name = str(row["column_name"]).lower()
                label = str(row["label"])
                qualified = f"{schema_name}.{table_name}.{column_name}"
                grouped.setdefault(qualified, []).append(label)
            result: Dict[str, List[str]] = {}
            for qualified, labels in grouped.items():
                result[qualified] = labels
                _schema, table_name, column_name = qualified.split(".", 2)
                unqualified = f"{table_name}.{column_name}"
                # Keep the first schema when the same table.column exists in two schemas.
                result.setdefault(unqualified, labels)
            return result

        elif db_lower in ("mysql",):
            rows = connection.execute(text(
                """
                SELECT table_name, column_name, column_type
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND column_type LIKE 'enum(%)'
                """
            )).mappings().all()
            result = {}
            for row in rows:
                raw = str(row["column_type"])  # e.g. "enum('a','b','c')"
                inner = re.sub(r"^enum\((.+)\)$", r"\1", raw, flags=re.IGNORECASE)
                values = [
                    v.strip().strip("'").strip('"')
                    for v in inner.split(",")
                    if v.strip().strip("'").strip('"')
                ]
                if values:
                    tbl = str(row["table_name"]).lower()
                    col = str(row["column_name"]).lower()
                    result[f"{tbl}.{col}"] = values
            return result

        elif db_lower in ("oracledb", "oracle"):
            oracle_schema = kwargs.get("oracle_schema", "")
            schema_filter = ""
            params: Dict[str, Any] = {}
            if oracle_schema:
                schema_filter = "AND UPPER(cc.owner) = :owner AND UPPER(ck.owner) = :owner"
                params["owner"] = oracle_schema.upper()
            rows = connection.execute(text(
                f"""
                SELECT cc.table_name, cc.column_name, ck.search_condition
                FROM all_cons_columns cc
                JOIN all_constraints ck
                  ON cc.constraint_name = ck.constraint_name
                 AND cc.owner = ck.owner
                WHERE ck.constraint_type = 'C'
                  AND UPPER(ck.search_condition) LIKE '%IN (%'
                  {schema_filter}
                """
            ), params).mappings().all()
            result = {}
            in_pattern = re.compile(
                r"\bIN\s*\(\s*('(?:[^'\\]|\\.)*'(?:\s*,\s*'(?:[^'\\]|\\.)*')*)\s*\)",
                re.IGNORECASE,
            )
            for row in rows:
                search_cond = str(row.get("search_condition") or "")
                m = in_pattern.search(search_cond)
                if not m:
                    continue
                values = [
                    v.strip().strip("'")
                    for v in m.group(1).split(",")
                    if v.strip().strip("'")
                ]
                if values:
                    tbl = str(row["table_name"]).lower()
                    col = str(row["column_name"]).lower()
                    result[f"{tbl}.{col}"] = values
            return result

        elif db_lower in ("databricks",):
            return {}

        else:
            return {}

    except Exception as exc:
        logger.warning("_extract_enum_map failed for db_type=%s: %s", db_type, exc)
        return {}


# ---- get_schema_structure (blocking; run in thread) ----
def get_schema_structure(connection_string: str, queue, loop):
    engine = None
    schema_info = {"tables": []}
    schema_info["min_date"] = None
    schema_info["max_date"] = None

    try:
        # Parse connection string to detect database type
        parsed_url = urlparse(connection_string)
        is_oracle = parsed_url.scheme and "oracle" in parsed_url.scheme.lower()
        is_databricks = parsed_url.scheme and "databricks" in parsed_url.scheme.lower()
        oracle_schema = parsed_url.username if is_oracle else None
        query_params = parse_qs(parsed_url.query or "")
        databricks_catalog = (query_params.get("catalog", [None])[0] or "").strip() if is_databricks else ""
        databricks_schema = (query_params.get("schema", [None])[0] or "").strip() if is_databricks else ""
        
        if is_databricks:
            logger.info(
                "Starting schema extraction - DB Type: Databricks, Catalog: %s, Schema: %s",
                databricks_catalog,
                databricks_schema,
            )
        else:
            logger.info(f"Starting schema extraction - DB Type: {'Oracle' if is_oracle else 'Other'}, Schema: {oracle_schema}")
        
        engine = create_engine(connection_string, pool_pre_ping=True)
        inspector = inspect(engine)
        
        logger.info("Engine created successfully, connecting to database...")

        with engine.connect() as connection:
            logger.info("Database connection established")

            # Determine db_type string for enum extraction
            _scheme = (parsed_url.scheme or "").lower()
            if is_oracle:
                _db_type_for_enum = "oracledb"
            elif is_databricks:
                _db_type_for_enum = "databricks"
            elif "mysql" in _scheme or "pymysql" in _scheme or "mariadb" in _scheme:
                _db_type_for_enum = "mysql"
            elif "postgres" in _scheme or "psycopg" in _scheme:
                _db_type_for_enum = "postgres"
            else:
                _db_type_for_enum = _scheme

            # Extract enum map once per connection (fail-open)
            enum_map = _extract_enum_map(
                connection,
                _db_type_for_enum,
                oracle_schema=oracle_schema,
            )
            if enum_map:
                logger.info(
                    "Enum map extracted: %d enum types/columns for db_type=%s",
                    len(enum_map),
                    _db_type_for_enum,
                )

            # Databricks: use information_schema to preserve 3-level namespace
            if is_databricks:
                if not databricks_catalog or not databricks_schema:
                    error_msg = "Databricks connection string is missing catalog or schema query parameters."
                    logger.error(error_msg)
                    loop.call_soon_threadsafe(queue.put_nowait, {"type": "error", "message": error_msg})
                    raise Exception(error_msg)
                if not re.match(r"^[A-Za-z0-9_]+$", databricks_catalog):
                    raise Exception("Invalid Databricks catalog name format")
                if not re.match(r"^[A-Za-z0-9_]+$", databricks_schema):
                    raise Exception("Invalid Databricks schema name format")

                # Databricks Unity Catalog does not always use 'BASE TABLE'.
                # Common table_type values include MANAGED/EXTERNAL/VIEW.
                tables_sql = text(
                    f"""
                    SELECT table_catalog, table_schema, table_name, table_type
                    FROM {databricks_catalog}.information_schema.tables
                    WHERE lower(table_schema) = lower(:schema_name)
                      AND table_type IN (
                        'MANAGED',
                        'EXTERNAL',
                        'FOREIGN',
                        'STREAMING_TABLE',
                        'MANAGED_SHALLOW_CLONE',
                        'EXTERNAL_SHALLOW_CLONE',
                        'BASE TABLE'
                      )
                    ORDER BY table_name
                    """
                )
                table_rows = connection.execute(
                    tables_sql, {"schema_name": databricks_schema}
                ).mappings().all()

                # Fallback: some environments expose only VIEW rows or unexpected table_type values.
                if not table_rows:
                    fallback_sql = text(
                        f"""
                        SELECT table_catalog, table_schema, table_name, table_type
                        FROM {databricks_catalog}.information_schema.tables
                        WHERE table_schema = :schema_name
                        ORDER BY table_name
                        """
                    )
                    table_rows = connection.execute(
                        fallback_sql, {"schema_name": databricks_schema}
                    ).mappings().all()

                table_names = [r.get("table_name") for r in table_rows]
                logger.info(
                    "Found %d Databricks tables in %s.%s",
                    len(table_names),
                    databricks_catalog,
                    databricks_schema,
                )
            # For Oracle, try with schema first
            elif is_oracle and oracle_schema:
                logger.info(f"Oracle detected, trying to get tables from schema: {oracle_schema.upper()}")
                try:
                    table_names = inspector.get_table_names(schema=oracle_schema.upper())
                    logger.info(f"Found {len(table_names)} tables using schema '{oracle_schema.upper()}'")
                    if not table_names:
                        logger.warning(f"No tables found with schema '{oracle_schema.upper()}', trying without schema...")
                        table_names = inspector.get_table_names()
                        logger.info(f"Found {len(table_names)} tables without schema")
                except Exception as schema_error:
                    logger.warning(f"Error getting tables with schema '{oracle_schema.upper()}': {schema_error}, trying without schema...")
                    table_names = inspector.get_table_names()
                    logger.info(f"Found {len(table_names)} tables without schema")
            else:
                table_names = inspector.get_table_names()
                logger.info(f"Found {len(table_names)} tables")

            total_tables = len(table_names)

            if total_tables == 0:
                error_msg = "No tables found in database"
                logger.error(error_msg)
                if is_oracle:
                    error_msg += f". For Oracle, ensure schema '{oracle_schema}' is correct and has tables."
                loop.call_soon_threadsafe(queue.put_nowait, {
                    "type": "error",
                    "message": error_msg
                })
                raise Exception(error_msg)

            # Immediately inform WS of total tables (frontend can show ETA)
            loop.call_soon_threadsafe(queue.put_nowait, {
                "type": "started",
                "totalTables": total_tables,
                "message": f"Found {total_tables} tables"
            })

            logger.info(f"Starting to extract {total_tables} tables...")

            for idx, table_name in enumerate(table_names, start=1):
                try:
                    logger.debug(f"Extracting table {idx}/{total_tables}: {table_name}")

                    # Databricks column extraction from information_schema
                    if is_databricks:
                        columns_sql = text(
                            f"""
                            SELECT column_name, data_type
                            FROM {databricks_catalog}.information_schema.columns
                            WHERE lower(table_schema) = lower(:schema_name)
                              AND lower(table_name) = lower(:table_name)
                            ORDER BY ordinal_position
                            """
                        )
                        column_rows = connection.execute(
                            columns_sql,
                            {"schema_name": databricks_schema, "table_name": table_name},
                        ).mappings().all()
                        columns = [{"name": col["column_name"], "type": str(col["data_type"])} for col in column_rows]
                        # Databricks PK/FK extraction from information_schema
                        pk_sql = text(
                            f"""
                            SELECT kcu.column_name
                            FROM {databricks_catalog}.information_schema.table_constraints tc
                            JOIN {databricks_catalog}.information_schema.key_column_usage kcu
                              ON tc.constraint_catalog = kcu.constraint_catalog
                             AND tc.constraint_schema = kcu.constraint_schema
                             AND tc.constraint_name = kcu.constraint_name
                             AND tc.table_catalog = kcu.table_catalog
                             AND tc.table_schema = kcu.table_schema
                             AND tc.table_name = kcu.table_name
                            WHERE tc.table_schema = :schema_name
                              AND tc.table_name = :table_name
                              AND tc.constraint_type = 'PRIMARY KEY'
                            ORDER BY kcu.ordinal_position
                            """
                        )
                        try:
                            pk_rows = connection.execute(
                                pk_sql,
                                {"schema_name": databricks_schema, "table_name": table_name},
                            ).mappings().all()
                            primary_keys = {
                                "constrained_columns": [r["column_name"] for r in pk_rows if r.get("column_name")]
                            }
                        except Exception as pk_error:
                            logger.warning(
                                "Databricks PK lookup failed for %s.%s.%s: %s",
                                databricks_catalog,
                                databricks_schema,
                                table_name,
                                pk_error,
                            )
                            primary_keys = {"constrained_columns": []}

                        fk_sql = text(
                            f"""
                            SELECT
                                src_kcu.column_name AS constrained_column,
                                dst_kcu.table_catalog AS referred_table_catalog,
                                dst_kcu.table_schema AS referred_table_schema,
                                dst_kcu.table_name AS referred_table_name,
                                dst_kcu.column_name AS referred_column_name
                            FROM {databricks_catalog}.information_schema.table_constraints tc
                            JOIN {databricks_catalog}.information_schema.referential_constraints rc
                              ON tc.constraint_catalog = rc.constraint_catalog
                             AND tc.constraint_schema = rc.constraint_schema
                             AND tc.constraint_name = rc.constraint_name
                            JOIN {databricks_catalog}.information_schema.key_column_usage src_kcu
                              ON tc.constraint_catalog = src_kcu.constraint_catalog
                             AND tc.constraint_schema = src_kcu.constraint_schema
                             AND tc.constraint_name = src_kcu.constraint_name
                             AND tc.table_catalog = src_kcu.table_catalog
                             AND tc.table_schema = src_kcu.table_schema
                             AND tc.table_name = src_kcu.table_name
                            LEFT JOIN {databricks_catalog}.information_schema.key_column_usage dst_kcu
                              ON rc.unique_constraint_catalog = dst_kcu.constraint_catalog
                             AND rc.unique_constraint_schema = dst_kcu.constraint_schema
                             AND rc.unique_constraint_name = dst_kcu.constraint_name
                             AND src_kcu.ordinal_position = dst_kcu.ordinal_position
                            WHERE tc.table_schema = :schema_name
                              AND tc.table_name = :table_name
                              AND tc.constraint_type = 'FOREIGN KEY'
                            ORDER BY src_kcu.ordinal_position
                            """
                        )
                        try:
                            fk_rows = connection.execute(
                                fk_sql,
                                {"schema_name": databricks_schema, "table_name": table_name},
                            ).mappings().all()
                        except Exception as fk_error:
                            logger.warning(
                                "Databricks FK lookup failed for %s.%s.%s: %s",
                                databricks_catalog,
                                databricks_schema,
                                table_name,
                                fk_error,
                            )
                            fk_rows = []

                        foreign_keys = []
                        for row in fk_rows:
                            constrained_column = row.get("constrained_column")
                            referred_table_name = row.get("referred_table_name")
                            if not constrained_column or not referred_table_name:
                                continue

                            referred_catalog = row.get("referred_table_catalog")
                            referred_schema = row.get("referred_table_schema")
                            referred_column = row.get("referred_column_name")
                            referred_parts = [p for p in [referred_catalog, referred_schema, referred_table_name] if p]

                            foreign_keys.append(
                                {
                                    "column": constrained_column,
                                    "references": ".".join(referred_parts) if referred_parts else referred_table_name,
                                    "referred_column": referred_column,
                                }
                            )

                        qualified_table_name = f"{databricks_catalog}.{databricks_schema}.{table_name}"
                    # Get table schema - for Oracle, specify schema if available
                    elif is_oracle and oracle_schema:
                        columns = inspector.get_columns(table_name, schema=oracle_schema.upper())
                        primary_keys = inspector.get_pk_constraint(table_name, schema=oracle_schema.upper())
                        foreign_keys_raw = inspector.get_foreign_keys(table_name, schema=oracle_schema.upper())
                        qualified_table_name = table_name
                    else:
                        columns = inspector.get_columns(table_name)
                        primary_keys = inspector.get_pk_constraint(table_name)
                        foreign_keys_raw = inspector.get_foreign_keys(table_name)
                        qualified_table_name = table_name

                    if not is_databricks:
                        # Process foreign keys safely
                        foreign_keys = []
                        for fk in foreign_keys_raw:
                            if fk.get("constrained_columns") and len(fk["constrained_columns"]) > 0:
                                foreign_keys.append({
                                    "column": fk["constrained_columns"][0],
                                    "references": fk.get("referred_table", "")
                                })

                    if is_databricks:
                        serialized_columns = columns  # already dicts from information_schema
                    else:
                        serialized_columns = []
                        for col in columns:
                            col_type_str = str(col["type"])
                            col_entry: Dict[str, Any] = {
                                "name": col["name"],
                                "type": col_type_str,
                            }
                            # PostgreSQL: labels are keyed by table.column from pg_catalog.
                            if _db_type_for_enum == "postgres":
                                bare_table = table_name.split(".")[-1]
                                enum_labels = enum_map.get(
                                    f"{table_name}.{col['name']}".lower()
                                ) or enum_map.get(
                                    f"{bare_table}.{col['name']}".lower()
                                )
                                if enum_labels:
                                    col_entry["enum_values"] = enum_labels
                            else:
                                # MySQL / Oracle: match on "table_name.col_name" composite key
                                tbl_col_key = f"{table_name}.{col['name']}".lower()
                                if tbl_col_key in enum_map:
                                    col_entry["enum_values"] = enum_map[tbl_col_key]
                            serialized_columns.append(col_entry)

                    schema_info["tables"].append({
                        "name": qualified_table_name,
                        "catalog": databricks_catalog if is_databricks else None,
                        "schema": databricks_schema if is_databricks else (oracle_schema.upper() if is_oracle and oracle_schema else None),
                        "table": table_name,
                        "columns": serialized_columns,
                        "primary_keys": primary_keys,
                        "foreign_keys": foreign_keys
                    })

                    logger.debug(f"Successfully extracted table {table_name} with {len(columns)} columns")

                    # thread-safe progress push
                    loop.call_soon_threadsafe(queue.put_nowait, {
                        "type": "progress",
                        "completedTables": idx,
                        "totalTables": total_tables,
                        "tableName": table_name,
                        "message": f"Extracting table {idx}/{total_tables}"
                    })
                except Exception as table_error:
                    error_msg = f"Error extracting table '{table_name}': {str(table_error)}"
                    logger.error(error_msg)
                    loop.call_soon_threadsafe(queue.put_nowait, {
                        "type": "warning",
                        "message": error_msg
                    })
                    # Still add the table with minimal info
                    schema_info["tables"].append({
                        "name": table_name,
                        "columns": [],
                        "primary_keys": {},
                        "foreign_keys": [],
                        "error": str(table_error)
                    })

            # Discover actual date range from date/datetime columns (best-effort, fail-open)
            if not is_databricks:
                _min_dates: List[str] = []
                _max_dates: List[str] = []
                for tbl_entry in schema_info["tables"]:
                    if tbl_entry.get("error"):
                        continue
                    tbl_nm = tbl_entry.get("table") or tbl_entry.get("name", "")
                    if not tbl_nm:
                        continue
                    date_cols = [
                        c["name"] for c in (tbl_entry.get("columns") or [])
                        if isinstance(c, dict) and re.search(r"\b(DATE|DATETIME|TIMESTAMP)\b", c.get("type", ""), re.IGNORECASE)
                    ]
                    if not date_cols:
                        continue
                    for dc in date_cols[:1]:  # one column per table is enough
                        try:
                            _schema_prefix = ""
                            if is_oracle and oracle_schema:
                                _schema_prefix = f'"{oracle_schema.upper()}".'
                            row = connection.execute(
                                text(f'SELECT MIN("{dc}") AS mn, MAX("{dc}") AS mx FROM {_schema_prefix}"{tbl_nm}"')
                            ).mappings().first()
                            if row and row["mn"] and row["mx"]:
                                _min_dates.append(str(row["mn"])[:10])
                                _max_dates.append(str(row["mx"])[:10])
                        except Exception:
                            pass
                if _min_dates:
                    schema_info["min_date"] = min(_min_dates)
                    schema_info["max_date"] = max(_max_dates)

            logger.info(f"Schema extraction completed successfully: {len(schema_info['tables'])} tables extracted")

    except Exception as e:
        error_msg = f"Error fetching schema information: {str(e)}"
        logger.error(error_msg, exc_info=True)
        loop.call_soon_threadsafe(queue.put_nowait, {
            "type": "error",
            "message": error_msg
        })
        schema_info["min_date"] = None
        schema_info["max_date"] = None
        # Don't raise - return empty schema so caller can handle it
    finally:
        if engine:
            try:
                engine.dispose()
                logger.info("Engine disposed")
            except Exception as e:
                logger.warning(f"Error disposing engine: {e}")

    return schema_info


# ---- Salesforce schema extraction (blocking; run in thread) ----
def get_salesforce_schema_structure(credentials: Dict[str, Any], queue, loop) -> Dict[str, Any]:
    """
    Extract schema structure from Salesforce using the describe() API.

    Maps Salesforce objects to tables and fields to columns.
    Uses OAuth2 session-based authentication only.

    Args:
        credentials: Dict with session_id (OAuth access_token) and instance_url
        queue: asyncio.Queue for progress updates
        loop: asyncio event loop for thread-safe queue puts

    Returns:
        Schema info dict with tables (Salesforce objects) and their fields
    """
    if not SALESFORCE_AVAILABLE:
        error_msg = "simple-salesforce is not installed. Cannot extract Salesforce schema."
        logger.error(error_msg)
        loop.call_soon_threadsafe(queue.put_nowait, {
            "type": "error",
            "message": error_msg
        })
        return {"tables": [], "min_date": None, "max_date": None}

    schema_info = {"tables": [], "min_date": None, "max_date": None}

    sf = None
    try:
        logger.info("Starting Salesforce schema extraction")

        # Create Salesforce client using OAuth2 (session_id + instance_url)
        instance_url = credentials.get("instance_url")
        session_id = credentials.get("session_id")

        if not session_id or not instance_url:
            raise ValueError("Salesforce requires session_id and instance_url for OAuth2 authentication")

        sf = Salesforce(instance_url=instance_url, session_id=session_id)

        logger.info("Salesforce client created successfully")

        # Get list of all accessible objects
        describe_result = sf.describe()
        sobjects = describe_result.get("sobjects", [])

        # Filter to queryable objects (skip non-queryable system objects)
        queryable_objects = [
            obj for obj in sobjects
            if obj.get("queryable", False) and not obj.get("name", "").endswith("__History")
        ]

        # Common standard objects to prioritize
        priority_objects = {
            "Account", "Contact", "Opportunity", "Lead", "Case",
            "Campaign", "Task", "Event", "User", "Product2",
            "Pricebook2", "PricebookEntry", "Order", "OrderItem",
            "Contract", "Quote", "Asset", "OpportunityLineItem"
        }

        # Sort: priority objects first, then custom objects, then others
        def sort_key(obj):
            name = obj.get("name", "")
            if name in priority_objects:
                return (0, name)
            elif name.endswith("__c"):  # Custom objects
                return (1, name)
            else:
                return (2, name)

        queryable_objects.sort(key=sort_key)

        # Limit to reasonable number for schema extraction
        MAX_OBJECTS = 100
        objects_to_process = queryable_objects[:MAX_OBJECTS]
        total_objects = len(objects_to_process)

        if total_objects == 0:
            error_msg = "No queryable Salesforce objects found"
            logger.error(error_msg)
            loop.call_soon_threadsafe(queue.put_nowait, {
                "type": "error",
                "message": error_msg
            })
            return {"tables": [], "min_date": None, "max_date": None}

        logger.info(f"Found {total_objects} queryable Salesforce objects")

        # Notify frontend of total objects
        loop.call_soon_threadsafe(queue.put_nowait, {
            "type": "started",
            "totalTables": total_objects,
            "message": f"Found {total_objects} Salesforce objects"
        })

        # Extract schema for each object
        for idx, obj in enumerate(objects_to_process, start=1):
            object_name = obj.get("name", "")

            try:
                logger.debug(f"Extracting object {idx}/{total_objects}: {object_name}")

                # Get detailed object description
                obj_describe = getattr(sf, object_name).describe()

                # Extract fields
                fields = obj_describe.get("fields", [])
                columns = []
                for field in fields:
                    field_name = field.get("name", "")
                    field_type = field.get("type", "string")
                    field_label = field.get("label", field_name)

                    # Map Salesforce types to SQL-like types for consistency
                    type_mapping = {
                        "id": "VARCHAR(18)",
                        "string": "VARCHAR(255)",
                        "picklist": "VARCHAR(255)",
                        "multipicklist": "TEXT",
                        "textarea": "TEXT",
                        "email": "VARCHAR(255)",
                        "phone": "VARCHAR(40)",
                        "url": "VARCHAR(255)",
                        "int": "INTEGER",
                        "double": "DOUBLE",
                        "currency": "DECIMAL(18,2)",
                        "percent": "DECIMAL(5,2)",
                        "boolean": "BOOLEAN",
                        "date": "DATE",
                        "datetime": "DATETIME",
                        "time": "TIME",
                        "reference": "VARCHAR(18)",
                        "base64": "BLOB",
                        "address": "TEXT",
                        "location": "TEXT",
                    }
                    mapped_type = type_mapping.get(field_type, "VARCHAR(255)")

                    col_entry_sf: Dict[str, Any] = {
                        "name": field_name,
                        "type": mapped_type,
                        "salesforce_type": field_type,
                        "label": field_label,
                    }
                    if field_type in ("picklist", "multipicklist"):
                        active_values = [
                            pv["value"]
                            for pv in field.get("picklistValues", [])
                            if pv.get("active", True)
                        ]
                        if active_values:
                            col_entry_sf["enum_values"] = active_values
                    columns.append(col_entry_sf)

                # Extract relationships (foreign keys)
                foreign_keys = []
                for field in fields:
                    if field.get("type") == "reference":
                        ref_to = field.get("referenceTo", [])
                        if ref_to:
                            foreign_keys.append({
                                "column": field.get("name", ""),
                                "references": ref_to[0] if ref_to else "",
                                "relationship_name": field.get("relationshipName", ""),
                            })

                # Extract primary key (always Id for Salesforce)
                primary_keys = {
                    "constrained_columns": ["Id"],
                    "name": f"{object_name}_pk"
                }

                schema_info["tables"].append({
                    "name": object_name,
                    "columns": columns,
                    "primary_keys": primary_keys,
                    "foreign_keys": foreign_keys,
                    "label": obj.get("label", object_name),
                    "custom": obj.get("custom", False),
                })

                logger.debug(f"Extracted {object_name} with {len(columns)} fields")

                # Send progress update
                loop.call_soon_threadsafe(queue.put_nowait, {
                    "type": "progress",
                    "completedTables": idx,
                    "totalTables": total_objects,
                    "tableName": object_name,
                    "message": f"Extracting object {idx}/{total_objects}"
                })

            except Exception as obj_error:
                error_msg = f"Error extracting object '{object_name}': {str(obj_error)}"
                logger.error(error_msg)
                loop.call_soon_threadsafe(queue.put_nowait, {
                    "type": "warning",
                    "message": error_msg
                })
                # Add object with minimal info
                schema_info["tables"].append({
                    "name": object_name,
                    "columns": [],
                    "primary_keys": {},
                    "foreign_keys": [],
                    "error": str(obj_error)
                })

        logger.info(f"Salesforce schema extraction completed: {len(schema_info['tables'])} objects extracted")

    except SalesforceAuthenticationFailed as e:
        error_msg = f"Salesforce authentication failed: {str(e)}"
        logger.error(error_msg)
        loop.call_soon_threadsafe(queue.put_nowait, {
            "type": "error",
            "message": error_msg
        })

    except Exception as e:
        error_msg = f"Error fetching Salesforce schema: {str(e)}"
        logger.error(error_msg, exc_info=True)
        loop.call_soon_threadsafe(queue.put_nowait, {
            "type": "error",
            "message": error_msg
        })

    return schema_info
