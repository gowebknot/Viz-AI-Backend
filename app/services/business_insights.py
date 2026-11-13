"""
Business Insights Service Module

This module provides AI-powered business insights generation by:
1. Analyzing database schema to identify key business metrics
2. Using LangChain to generate KPI-focused SQL queries
3. Executing queries against the database
4. Using LLM to analyze results and generate insights

The service leverages LangChain for structured LLM interactions and
provides comprehensive business intelligence based on actual data.
"""

import asyncio
import json
import logging
from typing import Dict, List, Any
from uuid import UUID

import redis
from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text
from langchain_openai import ChatOpenAI

from app.models.schema_models import (
    DatabaseConnectionModel,
    UserProjectRoleModel,
    UserModel,
)
from app.utils.crypt import decrypt_string
from app.core.settings import settings
from app.services.query_executor import (
    ConnectorQueryError,
    ConnectorSqlFailed,
    ensure_online as ensure_connector_online,
    is_connector_row,
    require_direct,
    run_async as run_connector_query,
)
import os

logger = logging.getLogger(__name__)

REDIS_TTL_SECONDS = 3600  # 1 hour

# ---------------------------------------------------------------------------
# Schema trimming – send only what the LLM needs (table + column names +
# data type).  Dropping nullable, lengths, and other metadata cuts token
# usage significantly on large schemas.
# ---------------------------------------------------------------------------
def _trim_schema(schema_info: Dict[str, Any]) -> Dict[str, Any]:
    trimmed = []
    for table in schema_info.get("tables", []):
        cols = []
        for col in table.get("columns", []):
            entry: Dict[str, Any] = {"name": col.get("column_name") or col.get("name", "")}
            dtype = col.get("data_type") or col.get("type", "")
            if dtype:
                entry["type"] = dtype
            cols.append(entry)
        trimmed.append({"table_name": table.get("table_name", ""), "columns": cols})
    return {"tables": trimmed}


redis_url = settings.REDIS_URL
if redis_url:
    redis_client = redis.from_url(redis_url, decode_responses=True)
else:
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)


async def generate_business_insights_service(
    db: Session,
    token_payload: dict,
    database_connection_id: UUID,
) -> Dict[str, Any]:
    """
    Generate business insights by analyzing database schema and executing KPI queries.
    
    Workflow:
    1. Fetch database connection details
    2. Load pre-extracted schema from database (stored during connection creation)
    3. Use LangChain/LLM to identify KPIs and generate SQL queries
    4. Execute queries against the database
    5. Use LLM to analyze results and generate insights
    
    Args:
        db (Session): Database session
        token_payload (dict): User authentication token payload
        database_connection_id (UUID): ID of database connection to analyze
        
    Returns:
        dict: Business insights including KPIs, query results, and AI analysis
        
    Note:
        OpenAI API key is loaded from environment variable OPENAI_API_KEY
    """
    try:
        user_id = UUID(token_payload.get("sub"))
        if not user_id:
            raise ValueError("User ID not found in token payload")
        
        # Fetch database connection
        db_connection = (
            db.query(DatabaseConnectionModel)
            .filter(DatabaseConnectionModel.id == database_connection_id)
            .first()
        )
        
        if not db_connection:
            raise HTTPException(
                status_code=404, 
                detail="Database connection not found"
            )
        
        # Verify user has access to this project
        user_role = (
            db.query(UserProjectRoleModel)
            .filter(
                UserProjectRoleModel.user_id == user_id,
                UserProjectRoleModel.project_id == db_connection.project_id,
            )
            .first()
        )
        
        user = db.query(UserModel).filter(UserModel.id == user_id).first()
        
        if not user_role and not user.is_super:
            raise HTTPException(
                status_code=403,
                detail="User does not have access to this project"
            )
        
        # Check Redis cache first
        cache_key = f"business_insights:{db_connection.project_id}"
        try:
            cached_insights = redis_client.get(cache_key)
            if cached_insights:
                logger.debug(
                    "Returning cached business insights for project %s",
                    db_connection.project_id,
                )
                return json.loads(cached_insights)
        except redis.RedisError as redis_err:
            logger.warning("Redis unavailable, proceeding without cache: %s", redis_err)
        
        logger.info(
            "Generating business insights for database: %s", db_connection.connection_name
        )
        
        # Step 1: Get database schema from stored schema (already extracted during connection creation)
        if not db_connection.db_schema:
            raise HTTPException(
                status_code=400,
                detail="Database schema not found. Please reconnect the database."
            )
        
        try:
            schema_info = json.loads(db_connection.db_schema)
            logger.debug(f"Loaded schema with {len(schema_info.get('tables', []))} tables")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse stored schema: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Invalid schema format in database connection"
            ) from e
        
        # Step 2: Use LLM to generate KPI queries (SQL or SOQL based on db_type)
        # Ensure db_type has a default value
        db_type = db_connection.db_type or "postgres"
        
        kpi_queries = await generate_kpi_queries_with_llm(
            schema_info=schema_info,
            db_type=db_type,
        )
        logger.info("Generated %d KPI queries", len(kpi_queries))

        # Step 3: Execute queries against database
        # Salesforce uses simple-salesforce (SOQL); SQL DBs use SQLAlchemy
        if db_type == "salesforce":
            from app.services.soql_executor import execute_salesforce_kpi_queries
            query_results = await execute_salesforce_kpi_queries(
                db_connection=db_connection,
                queries=kpi_queries,
            )
        else:
            query_results = await execute_kpi_queries(
                db_connection=db_connection,
                queries=kpi_queries
            )
        
        # Step 4: Use LLM to analyze results and generate insights
        business_insights = await generate_insights_from_results(
            schema_info=schema_info,
            query_results=query_results,
        )
        
        result = {
            "message": "Business insights generated successfully",
            "database_name": db_connection.connection_name,
            "database_type": db_connection.db_type or "postgres",
            "kpis_analyzed": len(kpi_queries),
            "kpi_queries": kpi_queries,
            "query_results": query_results,
            "insights": business_insights,
        }

        # Store in Redis cache
        try:
            redis_client.setex(cache_key, REDIS_TTL_SECONDS, json.dumps(result))
            logger.debug(
                "Stored business insights in cache for project %s with TTL %s seconds",
                db_connection.project_id,
                REDIS_TTL_SECONDS,
            )
        except redis.RedisError as redis_err:
            logger.warning("Failed to cache business insights: %s", redis_err)

        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating business insights: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate business insights: {str(e)}"
        ) from e


async def extract_database_schema(db_connection: DatabaseConnectionModel) -> Dict[str, Any]:
    """
    Extract database schema information including tables, columns, and data types.

    NOTE: This is a fallback method. The primary method is to use the pre-extracted
    schema stored in db_connection.db_schema during connection creation.
    This function is kept for compatibility and schema refresh scenarios.

    Args:
        db_connection: Database connection model

    Returns:
        dict: Schema information with tables and columns
    """
    require_direct(db_connection, "Reading the live database structure")

    try:
        # Decrypt the connection string before using it
        decrypted_connection_string = decrypt_string(db_connection.db_connection_string)

        # Use external engine manager for connection pooling
        from app.core.db import external_engine_manager
        engine = external_engine_manager.get_engine(
            connection_id=db_connection.id,
            connection_string=decrypted_connection_string,
            db_type=db_connection.db_type
        )
        
        with engine.connect() as connection:
            # Get all tables
            if db_connection.db_type == "postgres":
                tables_query = text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE'
                """)
            elif db_connection.db_type == "mysql":
                tables_query = text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = DATABASE()
                    AND table_type = 'BASE TABLE'
                """)
            else:
                tables_query = text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_type = 'BASE TABLE'
                """)
            
            result = connection.execute(tables_query)
            tables = [row[0] for row in result]
            
            # Get columns for each table
            schema_info = {"tables": []}
            
            for table in tables:
                if db_connection.db_type == "postgres":
                    columns_query = text(f"""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_name = '{table}'
                        AND table_schema = 'public'
                        ORDER BY ordinal_position
                    """)
                elif db_connection.db_type == "mysql":
                    columns_query = text(f"""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_name = '{table}'
                        AND table_schema = DATABASE()
                        ORDER BY ordinal_position
                    """)
                else:
                    columns_query = text(f"""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_name = '{table}'
                        ORDER BY ordinal_position
                    """)
                
                col_result = connection.execute(columns_query)
                columns = [
                    {
                        "name": row[0],
                        "type": row[1],
                        "nullable": row[2]
                    }
                    for row in col_result
                ]
                
                schema_info["tables"].append({
                    "table_name": table,
                    "columns": columns
                })

        # Engine is managed by external_engine_manager, no dispose() needed
        return schema_info
        
    except Exception as e:
        logger.error(f"Error extracting database schema: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to extract database schema: {str(e)}"
        ) from e


async def generate_kpi_queries_with_llm(
    schema_info: Dict[str, Any],
    db_type: str,
) -> List[Dict[str, str]]:
    """
    Use LLM to analyze schema and generate 10 KPI-focused SQL queries.
    
    Args:
        schema_info: Database schema information
        db_type: Type of database (postgres, mysql, etc.)
        
    Returns:
        list: List of KPI queries with title, description, and SQL
        
    Note:
        OpenAI API key is loaded from environment variable OPENAI_API_KEY
    """
    try:
        # Default to postgres if db_type is None
        if not db_type:
            db_type = "postgres"
            logger.warning("Database type not specified, defaulting to postgres")
        db_type = db_type.lower()
        
        # Configure OpenAI API from settings (fallback supports legacy OPENAPI_API_KEY key name)
        openai_api_key = (
            (settings.OPENAI_API_KEY or "").strip()
            or (settings.OPENAPI_API_KEY or "").strip()
        )
        if not openai_api_key:
            raise HTTPException(
                status_code=500,
                detail="OpenAI API key is not configured. Set OPENAI_API_KEY in .env (or legacy OPENAPI_API_KEY)."
            )
        llm = ChatOpenAI(
            model="gpt-5.4-mini",
            temperature=0.1,
            max_tokens=1500,
            api_key=openai_api_key,
        )

        soql_guidelines = """
Write queries ONLY using SOQL (Salesforce Object Query Language) syntax - NOT SQL.
CRITICAL SOQL-specific requirements:
- CRITICAL: SOQL is NOT SQL - use SOQL syntax only
- CRITICAL: NEVER use AS keyword for aliases - SOQL does NOT support AS. Use direct aliases (e.g., SELECT COUNT(Id) count NOT SELECT COUNT(Id) AS count)
- CRITICAL: NEVER use JOIN - use relationship queries (e.g., SELECT Account.Name FROM Contact)
- CRITICAL: Use COUNT() not COUNT(*) (e.g., SELECT COUNT() FROM Account)
- CRITICAL: NEVER end queries with semicolons (;)
- CRITICAL: Use [MIN_DATE] and [MAX_DATE] placeholders for date filters (e.g., WHERE CreatedDate >= [MIN_DATE] AND CreatedDate <= [MAX_DATE])
- Standard objects: Account, Contact, Opportunity, Lead, Case, Campaign, Order. Custom objects end with __c
- Use LIMIT for row limiting (e.g., LIMIT 100)
- HAVING can only be used with GROUP BY and aggregate queries
- Do not use LIMIT with SELECT COUNT()
- Subqueries are only allowed as relationship queries, not arbitrary nested SELECT statements
- Avoid large IN clause lists (Salesforce has governor limits)
- Field-to-field comparisons are not supported
- DISTINCT keyword is not supported except COUNT_DISTINCT(field)
- OFFSET has limits (max 2000) and should be used carefully
- Do not use SQL functions in WHERE (e.g., YEAR(), MONTH())
- Arithmetic expressions in SELECT are not supported
- CRITICAL: ORDER BY cannot use column aliases - repeat the full expression (e.g., ORDER BY COUNT(Id) DESC not ORDER BY count DESC)
- Include IsDeleted = false in WHERE clause to exclude soft-deleted records (for most objects)
- In aggregate queries, all non-aggregate fields in SELECT must appear in GROUP BY
- CRITICAL: SOQL does NOT support CASE WHEN / CASE expressions. For conditional counts (e.g. "count where field = true") use two separate queries: one with WHERE field = true (COUNT(Id) ... WHERE field = true) and one for total (COUNT(Id) ... ), or use COUNT(Id) with WHERE only - never SUM(CASE WHEN ...).
- CRITICAL: GROUP BY in SOQL is restricted - many custom fields (picklist __c, lookup __c, etc.) "can not be grouped in a query call". Do NOT use GROUP BY on custom fields like Design_Category_Required__c. For distribution by category use a simple SELECT with LIMIT (e.g. SELECT Field__c FROM Obj WHERE Field__c != null LIMIT n) or avoid that KPI; use GROUP BY only on standard groupable fields when needed.
- No UNION, INTERSECT, EXCEPT - SOQL does not support these
- No COALESCE, NVL, IFNULL - SOQL does not support these
- For null checks use = null and != null (not IS NULL / IS NOT NULL)
- Booleans: use true and false (lowercase, no quotes)
- SELECT * is NOT supported - always list field names explicitly
- Date functions: use CALENDAR_MONTH(), CALENDAR_YEAR(), CALENDAR_QUARTER() for grouping by time
- Aggregate functions: COUNT(), SUM(), AVG(), MIN(), MAX() with GROUP BY
- Date format: YYYY-MM-DD or use placeholders [MIN_DATE], [MAX_DATE]
- Alias syntax: SELECT field alias_name NOT SELECT field AS alias_name (SOQL does not support AS keyword)
""".strip()

        oracle_guidelines = """
Write queries ONLY using syntax compatible with Oracle database.
CRITICAL Oracle-specific syntax requirements:
- CRITICAL: NEVER use LIMIT clause - Oracle does NOT support LIMIT. Always use FETCH FIRST n ROWS ONLY instead (e.g., ORDER BY col DESC FETCH FIRST 10 ROWS ONLY, NOT LIMIT 10)
- CRITICAL: NEVER end queries with semicolons (;) - Oracle will throw "query didn't properly ended" error. Your query must end WITHOUT a semicolon. Example: ORDER BY col DESC (correct) NOT ORDER BY col DESC; (wrong)
- CRITICAL: ORDER BY clause CANNOT use column aliases - you MUST repeat the full expression (e.g., SELECT SUM(amount) AS total ... ORDER BY SUM(amount) DESC, NOT ORDER BY total DESC)
- CRITICAL: GROUP BY clause CANNOT use column aliases - you MUST repeat the full expression (e.g., SELECT TO_CHAR(date_col, 'YYYY-MM') AS month_year ... GROUP BY TO_CHAR(date_col, 'YYYY-MM'), NOT GROUP BY month_year)
- CRITICAL: For date comparisons in WHERE clause, ALWAYS use TO_DATE() - never use string literals directly (e.g., WHERE date_col BETWEEN TO_DATE('2024-01-01', 'YYYY-MM-DD') AND TO_DATE('2024-12-31', 'YYYY-MM-DD'), NOT WHERE date_col BETWEEN '2024-01-01' AND '2024-12-31')
- Use TO_DATE('YYYY-MM-DD', 'YYYY-MM-DD') for date literals (e.g., TO_DATE('2024-01-01', 'YYYY-MM-DD'))
- Use TRUNC(date_column, 'MONTH') or TRUNC(date_column, 'YEAR') for date truncation instead of DATE_TRUNC
- Use SYSDATE for current date/time, not CURRENT_DATE or NOW()
- Use FETCH FIRST n ROWS ONLY for row limiting after ORDER BY (e.g., ORDER BY col DESC FETCH FIRST 10 ROWS ONLY)
- Use ROWNUM <= n for row limiting in subqueries (e.g., WHERE ROWNUM <= 10)
- Use double quotes (") for identifiers if needed, but prefer unquoted identifiers
- Use NVL() or COALESCE() for null handling
- Use || for string concatenation (e.g., 'Hello' || 'World')
- Use TO_CHAR(date_column, 'YYYY-MM') for date formatting
- Date arithmetic: date_column + INTERVAL '1' MONTH or date_column + 30 (days)
- Use EXTRACT(YEAR FROM date_column) for extracting date parts
- Use ROUND() for rounding numbers, not ROUND() for dates
- Use DUAL table for calculations (e.g., SELECT SYSDATE FROM DUAL)
""".strip()

        dialect_guidance = ""
        if db_type in {"oracle", "oracledb"}:
            dialect_guidance = f"\n- ORACLE RULES:\n{oracle_guidelines}\n"
        elif db_type == "salesforce":
            dialect_guidance = f"\n- SOQL RULES:\n{soql_guidelines}\n"

        query_type_label = "SOQL query" if db_type == "salesforce" else f"{db_type.upper()} SQL query"

        # Trim schema before injecting into prompt to reduce token usage
        trimmed_schema = _trim_schema(schema_info)

        # Create comprehensive prompt for KPI generation, injecting dialect guidance when needed
        prompt = f"""
You are a business intelligence expert. Analyze the following database schema and generate 7 important KPI (Key Performance Indicator) {"SOQL" if db_type == "salesforce" else "SQL"} queries that would provide valuable business insights FOR THIS SPECIFIC {"Salesforce org" if db_type == "salesforce" else "database"}.

Database Type: {db_type}

Database Schema (USER'S BUSINESS DATA - tables/objects and columns/fields):
{json.dumps(trimmed_schema, indent=2)}

IMPORTANT INSTRUCTIONS:
- This is a USER'S BUSINESS {"Salesforce org" if db_type == "salesforce" else "DATABASE"}, NOT a VizAI internal database
- DO NOT reference tables like "user", "project", "dashboard", "chart" unless they actually exist in the schema above
- ONLY use tables/objects and columns/fields that are explicitly listed in the schema above
- Generate queries relevant to THIS SPECIFIC business domain based on the table/object names and columns you see

For each KPI, provide:
1. A clear title for the KPI
2. A brief description of what business insight it provides
3. A valid {query_type_label} to calculate this KPI

Focus on KPIs based on the actual tables in the schema:
- If there are sales/transactions tables: revenue, growth, averages
- If there are customer tables: customer count, retention, acquisition
- If there are product tables: top products, inventory, performance
- If there are order tables: order volumes, fulfillment metrics
- Time-based trends using date/timestamp columns you find
- Distribution analysis across categories you find

Return the response as a JSON array with this exact structure:
[
  {{
    "kpi_title": "Total Revenue",
    "description": "Calculate the total revenue across all transactions",
    "sql_query": "SELECT SUM(amount) as total_revenue FROM sales"
  }},
  ...
]

{"CRITICAL FOR SALESFORCE: SOQL does NOT support AS keyword. Use SELECT COUNT(Id) count NOT SELECT COUNT(Id) AS count. Example: SELECT SUM(Amount) total_revenue FROM Opportunity" if db_type == "salesforce" else ""}

CRITICAL REQUIREMENTS:
- Generate exactly 7 KPIs
- Use ONLY tables/objects and columns/fields from the schema provided above
- Queries must be syntactically correct for {db_type} (use {db_type}-specific syntax){dialect_guidance}
- For PostgreSQL: Use INTERVAL '30 days', DATE_TRUNC, etc.
- For MySQL: Use DATE_SUB, DATE_FORMAT, etc.
- For Salesforce: Use SOQL only - NO AS keyword, NO CASE WHEN (use two queries for conditional counts), NO GROUP BY on custom picklist/lookup fields, [MIN_DATE]/[MAX_DATE] placeholders, COUNT() not COUNT(*), no JOIN
- Use appropriate aggregations (SUM, COUNT, AVG, MAX, MIN)
- Include GROUP BY where necessary
- Use meaningful aliases for all calculated fields
- Ensure all column references exist in the schema

Return ONLY the JSON array, no additional text or markdown.
DO NOT include explanations, self-corrections, or any other text.
DO NOT include markdown code fences.
Just the raw JSON array starting with [ and ending with ].
"""
        
        logger.debug("Generating KPI queries with LLM...")
        response = await llm.ainvoke(prompt)

        # Parse JSON response
        response_text = (response.content or "").strip()

        # Remove markdown code blocks if present
        if response_text.startswith("```json"):
            response_text = response_text.split("```json")[1]
        if response_text.startswith("```"):
            response_text = response_text.split("```")[1]
        if "```" in response_text:
            response_text = response_text.split("```")[0]

        response_text = response_text.strip()

        # Try to extract JSON array if there's extra text
        if response_text.startswith("[") and "]" in response_text:
            # Find the closing bracket of the JSON array
            bracket_count = 0
            json_end = 0
            for i, char in enumerate(response_text):
                if char == "[":
                    bracket_count += 1
                elif char == "]":
                    bracket_count -= 1
                    if bracket_count == 0:
                        json_end = i + 1
                        break
            if json_end > 0:
                response_text = response_text[:json_end]
        
        kpi_queries = json.loads(response_text)
        
        logger.debug(f"Generated {len(kpi_queries)} KPI queries")
        return kpi_queries
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM response as JSON: {str(e)}")
        logger.error(f"Response text (first 500 chars): {response_text[:500]}")
        raise HTTPException(
            status_code=500,
            detail="Failed to parse KPI queries from LLM response"
        ) from e
    except Exception as e:
        logger.error(f"Error generating KPI queries: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate KPI queries: {str(e)}"
        ) from e


async def execute_kpi_queries(
    db_connection: DatabaseConnectionModel,
    queries: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    """
    Execute all KPI queries against the database and collect results.

    Args:
        db_connection: Database connection model
        queries: List of KPI queries to execute

    Returns:
        list: Query results with KPI data
    """
    if is_connector_row(db_connection):
        return await _execute_kpi_queries_via_connector(db_connection, queries)

    try:
        # Decrypt the connection string before using it
        decrypted_connection_string = decrypt_string(db_connection.db_connection_string)

        # Use external engine manager for connection pooling
        from app.core.db import external_engine_manager
        engine = external_engine_manager.get_engine(
            connection_id=db_connection.id,
            connection_string=decrypted_connection_string,
            db_type=db_connection.db_type
        )
        logger.info("Executing %d KPI queries concurrently (SQL)", len(queries))

        def _run_single_query(kpi: Dict[str, str]) -> Dict[str, Any]:
            """Execute one KPI query synchronously (called via asyncio.to_thread)."""
            try:
                with engine.connect() as connection:
                    logger.debug(f"Executing query for KPI: {kpi['kpi_title']}")
                    query_result = connection.execute(text(kpi["sql_query"]))
                    columns = query_result.keys()
                    rows = [dict(zip(columns, row)) for row in query_result.fetchall()]
                    serializable_rows = []
                    for row in rows:
                        serializable_row = {}
                        for key, value in row.items():
                            if value is None:
                                serializable_row[key] = None
                            elif isinstance(value, (int, float, str, bool)):
                                serializable_row[key] = value
                            else:
                                serializable_row[key] = str(value)
                        serializable_rows.append(serializable_row)
                    return {
                        "kpi_title": kpi["kpi_title"],
                        "description": kpi["description"],
                        "query": kpi["sql_query"],
                        "success": True,
                        "data": serializable_rows,
                        "row_count": len(serializable_rows),
                    }
            except Exception as query_error:
                logger.error(f"Error executing query for {kpi['kpi_title']}: {str(query_error)}")
                return {
                    "kpi_title": kpi["kpi_title"],
                    "description": kpi["description"],
                    "query": kpi["sql_query"],
                    "success": False,
                    "error": str(query_error),
                    "data": [],
                    "row_count": 0,
                }

        # Run all KPI queries in parallel – each gets its own thread+connection
        results = list(
            await asyncio.gather(*[asyncio.to_thread(_run_single_query, kpi) for kpi in queries])
        )

        successful_queries = sum(1 for r in results if r["success"])
        logger.info("Executed %d/%d KPI queries successfully", successful_queries, len(queries))

        return results
        
    except Exception as e:
        logger.error(f"Error executing KPI queries: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to execute KPI queries: {str(e)}"
        ) from e


async def _execute_kpi_queries_via_connector(
    db_connection: DatabaseConnectionModel,
    queries: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    """execute_kpi_queries for a connector datasource: same result entries as the direct path."""
    ensure_connector_online(db_connection)

    async def run_one(kpi: Dict[str, str]) -> Dict[str, Any]:
        base = {
            "kpi_title": kpi["kpi_title"],
            "description": kpi["description"],
            "query": kpi["sql_query"],
        }
        try:
            outcome = await run_connector_query(db_connection, kpi["sql_query"])
        except (ConnectorSqlFailed, ConnectorQueryError) as query_error:
            message = getattr(query_error, "detail", None) or str(query_error)
            logger.warning("Connector KPI query failed for connection %s", db_connection.id)
            return {**base, "success": False, "error": message, "data": [], "row_count": 0}
        rows = [
            {
                key: value if value is None or isinstance(value, (int, float, str, bool)) else str(value)
                for key, value in row.items()
            }
            for row in outcome.as_dicts()
        ]
        return {**base, "success": True, "data": rows, "row_count": len(rows)}

    return list(await asyncio.gather(*[run_one(kpi) for kpi in queries]))


async def generate_insights_from_results(
    schema_info: Dict[str, Any],
    query_results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Use LLM to analyze query results and generate business insights.
    
    Args:
        schema_info: Database schema information
        query_results: Results from executed KPI queries
        
    Returns:
        dict: Business insights and recommendations
        
    Note:
        OpenAI API key is loaded from environment variable OPENAI_API_KEY
    """
    try:
        # Configure OpenAI API from settings (fallback supports legacy OPENAPI_API_KEY key name)
        openai_api_key = (
            (settings.OPENAI_API_KEY or "").strip()
            or (settings.OPENAPI_API_KEY or "").strip()
        )
        if not openai_api_key:
            raise HTTPException(
                status_code=500,
                detail="OpenAI API key is not configured. Set OPENAI_API_KEY in .env (or legacy OPENAPI_API_KEY)."
            )
        llm = ChatOpenAI(
            model="gpt-5.4-mini",
            temperature=0.1,
            max_tokens=2500,
            api_key=openai_api_key,
        )

        # Prepare query results summary – cap each KPI to 3 rows to limit context size
        results_summary = []
        for result in query_results:
            if result["success"]:
                results_summary.append({
                    "kpi": result["kpi_title"],
                    "description": result["description"],
                    "data": result["data"][:3],
                })
        
        # Create prompt for insight generation
        prompt = f"""
You are a business analyst. Analyze the following KPI query results and provide comprehensive business insights.

KPI Results:
{json.dumps(results_summary, indent=2)}

Provide a detailed analysis including:

1. **Executive Summary**: A brief overview of the key findings (2-3 sentences)

2. **Key Metrics Analysis**: For each major KPI, explain:
   - What the numbers mean
   - Whether they indicate positive or negative trends (and label it explicitly as `trend`)
   - Set `trend` strictly as:
     - `positive` if the metric is improving/increasing/performing better than before
     - `negative` if the metric is worsening/decreasing/performing worse than before
     - `neutral` if the trend is unclear or mixed
   - Potential business implications
   - The reasoning behind your interpretation (what data points led to this conclusion)

3. **Insights & Patterns**: Identify:
   - Notable trends or patterns
   - Anomalies or unexpected results
   - Correlations between different metrics
   - The analytical reasoning that led to these insights

4. **Recommendations**: Provide 3-5 actionable recommendations based on the data

5. **Areas of Concern**: Highlight any metrics that require immediate attention

Return the response as a JSON object with this structure:
{{
  "executive_summary": "string",
  "reasoning": "string (explain your overall analytical approach and key data points that informed your analysis)",
  "key_metrics": [
    {{
      "kpi_name": "string",
      "value_interpretation": "string",
      "business_impact": "string",
      "trend": "positive|negative|neutral",
      "reasoning": "string (explain what specific data points or patterns led to this interpretation)"
    }}
  ],
  "insights_and_patterns": [
    {{
      "insight": "string",
      "reasoning": "string (explain the analytical process and data evidence supporting this insight)"
    }}
  ],
  "recommendations": [
    {{
      "priority": "high|medium|low",
      "title": "string",
      "description": "string",
      "reasoning": "string (explain which metrics or insights led to this recommendation)"
    }}
  ],
  "areas_of_concern": [
    {{
      "concern": "string",
      "reasoning": "string (explain what data points indicate this is a concern)"
    }}
  ]
}}

Be specific, data-driven, and actionable in your analysis.

Return ONLY the JSON object, no additional text.
DO NOT include explanations, self-corrections, thinking process, or any other text.
DO NOT include markdown code fences.
Just the raw JSON object starting with {{ and ending with }}.
"""
        
        logger.debug("Generating business insights with LLM...")
        response = await llm.ainvoke(prompt)
        
        # Parse JSON response
        response_text = (response.content or "").strip()
        
        # Remove markdown code blocks if present
        if response_text.startswith("```json"):
            response_text = response_text.split("```json")[1]
        if response_text.startswith("```"):
            response_text = response_text.split("```")[1]
        if "```" in response_text:
            response_text = response_text.split("```")[0]
        
        response_text = response_text.strip()
        
        # Try to extract JSON object if there's extra text
        # Look for the JSON object pattern
        if response_text.startswith("{") and "}" in response_text:
            # Find the closing brace of the JSON object
            brace_count = 0
            json_end = 0
            for i, char in enumerate(response_text):
                if char == "{":
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                    if brace_count == 0:
                        json_end = i + 1
                        break
            if json_end > 0:
                response_text = response_text[:json_end]
        
        insights = json.loads(response_text)
        
        logger.debug("Business insights generated successfully")
        return insights
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse insights response as JSON: {str(e)}")
        logger.error(f"Response text (first 1000 chars): {response_text[:1000]}")
        raise HTTPException(
            status_code=500,
            detail="Failed to parse business insights from LLM response"
        ) from e
    except Exception as e:
        logger.error(f"Error generating business insights: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate business insights: {str(e)}"
        ) from e

