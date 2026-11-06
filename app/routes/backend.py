"""
Backend routes for managing projects, dashboards, users, roles, permissions,
and database connections. All routes are prefixed with `/api/v1/backend` and
use dependency injection for DB session and user authentication.
"""

from uuid import UUID
from typing import Optional
import asyncio
import traceback
import logging

logger = logging.getLogger(__name__)
from fastapi import (
    APIRouter,
    status,
    Response,
    Depends,
    Request,
    Path,
    HTTPException,
    Body,
    Query,
    BackgroundTasks,
    UploadFile,
    WebSocket,
    WebSocketDisconnect
)
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.core.db import get_db, get_pool_status
from app.utils.token_parser import get_current_user

# Schema imports
from app.schemas import (
    ProjectRequest,
    UpdateProjectRequest,
    UpdateUserRequest,
    CreateSuperUserRequest,
    DBConnectionRequest,
    UpdateDBConnectionRequest,
    DBConnectionResponse,
    CreateUserProjectRequest,
    CreateUserProjectResponse,
    ListAllUsersProjectResponse,
    ListAllRolesProjectResponse,
    CreateDashboardRequest,
    CreateDashboardResponse,
    UpdateDashboardRequest,
    AddUserDashboardRequest,
    AddUserDashboardResponse,
    CreateRoleRequest,
    CreateRoleResponse,
    UpdateRoleRequest,
    ListAllPermissionsResponse,
    BlackListTableNameRequest,
    ReadDataRequest,
    RequestAccess,
    SaveChartRequest,
    UpdateRequestAccess,
    SaveChartToDashboardRequest,
    QueryRequest,
    Nl2SQLChatRequest,
    UpdateFavoriteChartRequest,
    TrinoQueryRequest,
    TrinoQueryResponse,
    QueryExecutionRequest,
    SubmitOntologyAnswersRequest,
    EnrichmentChatRequest,
    EnrichmentChatResponse,
    DashboardStatsResponse,
    PinnedChartsCountResponse,
    BusinessInsightsRequest,
    BusinessInsightsResponse,
    ProjectInsightsResponse,
    LatestBusinessInsightResponse,
    ConnectionStatsResponse,
    ConnectionCheckResponse,
    SaveHomeInsightRequest,
    SaveHomeInsightResponse,
    GetHomeInsightsResponse,
    OntologyVersionResponse,
    StartOntologyEnrichmentResponse,
    GenerateKpiQueriesRequest,
    RegenerateKpiRequest,
    GenerateKpiQueriesResponse,
)

# Service imports
from app.services.project import (
    create_project,
    get_projects,
    list_all_roles_project,
    create_dashboard,
    list_all_permissions,
    create_role,
    list_users_all_dashboard,
    delete_dashboard,
    update_project,
    delete_project,
    update_dashboard,
    update_role,
    delete_role,
    get_project_owner_service,
    get_dashboard_owner_service,
    blacklist_service,
    update_blacklist_service,
    read_data_service,
    generate_kpi_queries_service,
    regenerate_kpi_service,
)

from app.services.db_connection import (
    create_database_connection,
    get_connections,
    get_connection_ds_graph,
    update_db_connection,
    delete_db_connection,
    get_connection_stats,
    check_and_update_connections,
    progress_queues
)

from app.services.userService import (
    create_user_project,
    list_all_users_project,
    add_user_to_dashboard,
    get_user_details,
    update_user,
    delete_user,
    create_super_user_service,
    get_super_user_service,
    get_users_dashboard_service,
    get_favorites_service,
    get_dashboard_stats_service,
)

from app.services.chart import (
    request_access_service,
    update_request_access_service,
    get_access_requests_service,
    get_charts_service,
    save_chart_service,
    save_chart_to_dashboard_service,
    get_charts_for_dashboard_service,
    get_user_dashboard_charts_service,
    delete_chart_service,
    delete_chart_from_dashboard_service,
    update_favorite_chart_service,
    get_favorite_charts_service,
    get_user_favorite_charts_service,
    get_pinned_charts_count_service,
    filter_charts_service,
)

from app.services.business_insights import generate_business_insights_service
from app.services.project_insights import (
    generate_project_insights_service,
    get_latest_business_insight_service,
)

from app.services.query_executor import ConnectorQueryError
from app.utils.access import ensure_datasource_access
from app.services.generate_queries import (
    generate_and_store_charts,
    execute_external_query,
)

from app.services.nl2sql import generate_nl_sql
from app.services.ontology import (
    bootstrap_ontology,
    get_latest_ontology,
    get_latest_ontology_ttl,
    start_enrichment,
    enrichment_chat_message,
    submit_enrichment_answers,
    validate_latest_ontology_ttl,
    process_pbit_upload,
    generate_catalog,
    get_catalog_status,
)

from app.services.multiple_db_generate_queries import generate_trino_queries_service

from app.services.home_insights import (
    save_home_insight_service,
    get_home_insights_service,
    delete_home_insight_service,
)


backend_router = APIRouter(prefix="/api/v1/backend", tags=["backend"])


@backend_router.get("/health/pool-status")
async def get_pool_status_endpoint():
    """
    Get database connection pool status for monitoring.
    Returns pool metrics including size, checked in/out connections, and overflow.
    """
    try:
        status_data = get_pool_status()

        utilization_pct = (status_data['checked_out'] / (status_data['pool_size'] + status_data['max_overflow'])) * 100

        return {
            "status": "healthy" if utilization_pct < 80 else "warning" if utilization_pct < 95 else "critical",
            "pool": status_data,
            "utilization_percentage": round(utilization_pct, 2),
            "available_connections": status_data['pool_size'] + status_data['max_overflow'] - status_data['checked_out']
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get pool status: {str(e)}"
        )


@backend_router.post("/create-project", status_code=status.HTTP_201_CREATED)
async def create_project_route(
    project: ProjectRequest,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Create a new project.
    Args:s
        project (ProjectRequest): The project data.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The created project.
    """
    return await create_project(project, db, token_payload)


@backend_router.post("/database/{project_id}", response_model=DBConnectionResponse)
async def add_database_connection(
    project_id: UUID,
    data: DBConnectionRequest,
    background_tasks:BackgroundTasks,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Create a new database connection.
    Args:
        project_id (UUID): The project ID.
        data (DBConnectionRequest): The database connection data.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The created database connection.
    """
    return await create_database_connection(project_id, token_payload, data, db,background_tasks)

async def _wait_for_disconnect(ws: WebSocket) -> None:
    while (await ws.receive())["type"] != "websocket.disconnect":
        pass


@backend_router.websocket("/ws/progress/{task_id}")
async def ws_progress(ws: WebSocket, task_id: str):
    await ws.accept()
    queue = progress_queues.get(task_id)
    if not queue:
        await ws.send_json({"type": "error", "message": "Invalid taskId"})
        await ws.close()
        return
    # Wait for the next message or the client leaving, whichever comes first. A connector's task can
    # wait days for the client to install, and a handler stuck on the queue alone would never notice
    # the client leaving, and would hang the server's shutdown. The queue stays registered, so a
    # reloaded page can subscribe again and nothing is lost.
    gone = asyncio.ensure_future(_wait_for_disconnect(ws))
    getter = None
    try:
        while True:
            getter = asyncio.ensure_future(queue.get())
            await asyncio.wait({getter, gone}, return_when=asyncio.FIRST_COMPLETED)
            if not getter.done():
                logger.debug(f"Client disconnected from task {task_id}")
                break
            msg = getter.result()
            if msg is None:
                break  # worker finished
            await ws.send_json(msg)
    except WebSocketDisconnect:
        logger.debug(f"Client disconnected from task {task_id}")
    finally:
        gone.cancel()
        if getter is not None and not getter.done():
            getter.cancel()
        try:
            await ws.close()
        except:
            pass


@backend_router.get(
    "/connections/{project_id}", status_code=status.HTTP_200_OK, response_model=dict
)
async def get_connections_route(
    project_id: UUID = Path(..., description="Project ID to get connections for"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Get all connections for a project.
    Args:
        project_id (UUID): The project ID.
        request (Request): The request object.
        response (Response): The response object.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The connections for the project.
    """
    return await get_connections(project_id, db, token_payload)


@backend_router.get(
    "/connections/{connection_id}/ds-graph", status_code=status.HTTP_200_OK
)
async def get_connection_ds_graph_route(
    connection_id: UUID = Path(..., description="Connection ID to get DS graph for"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await get_connection_ds_graph(connection_id, db, token_payload)


@backend_router.post(
    "/connections/{connection_id}/ontology/bootstrap",
    status_code=status.HTTP_200_OK,
    response_model=OntologyVersionResponse,
)
async def bootstrap_ontology_route(
    connection_id: UUID,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await bootstrap_ontology(connection_id, db, token_payload)


@backend_router.get(
    "/connections/{connection_id}/ontology/latest",
    status_code=status.HTTP_200_OK,
    response_model=OntologyVersionResponse,
)
async def get_latest_ontology_route(
    connection_id: UUID,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await get_latest_ontology(connection_id, db, token_payload)


@backend_router.post(
    "/connections/{connection_id}/ontology/catalog/generate",
    status_code=status.HTTP_200_OK,
)
async def generate_catalog_route(
    connection_id: UUID,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await generate_catalog(connection_id, db, token_payload)


@backend_router.get(
    "/connections/{connection_id}/ontology/catalog/status",
    status_code=status.HTTP_200_OK,
)
async def get_catalog_status_route(
    connection_id: UUID,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await get_catalog_status(connection_id, db, token_payload)


@backend_router.get(
    "/connections/{connection_id}/ontology/latest.ttl",
    status_code=status.HTTP_200_OK,
    response_class=PlainTextResponse,
)
async def get_latest_ontology_ttl_route(
    connection_id: UUID,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await get_latest_ontology_ttl(connection_id, db, token_payload)


@backend_router.get(
    "/connections/{connection_id}/ontology/latest.ttl/validate",
    status_code=status.HTTP_200_OK,
)
async def validate_latest_ontology_ttl_route(
    connection_id: UUID,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await validate_latest_ontology_ttl(connection_id, db, token_payload)


@backend_router.post(
    "/connections/{connection_id}/ontology/enrichment/start",
    status_code=status.HTTP_200_OK,
    response_model=StartOntologyEnrichmentResponse,
)
async def start_enrichment_route(
    connection_id: UUID,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await start_enrichment(connection_id, db, token_payload)


@backend_router.post(
    "/connections/{connection_id}/ontology/enrichment/{session_id}/chat",
    status_code=status.HTTP_200_OK,
    response_model=EnrichmentChatResponse,
)
async def enrichment_chat_route(
    connection_id: UUID,
    session_id: UUID,
    payload: EnrichmentChatRequest,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await enrichment_chat_message(connection_id, session_id, payload.message, db, token_payload)


@backend_router.post(
    "/connections/{connection_id}/ontology/enrichment/{session_id}/apply",
    status_code=status.HTTP_200_OK,
    response_model=OntologyVersionResponse,
)
async def apply_enrichment_route(
    connection_id: UUID,
    session_id: UUID,
    payload: SubmitOntologyAnswersRequest,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    normalized_answers = [{"question_id": a.question_id, "answer": a.answer} for a in payload.answers]
    return await submit_enrichment_answers(connection_id, session_id, normalized_answers, db, token_payload)


@backend_router.get(
    "/connections/{project_id}/stats",
    status_code=status.HTTP_200_OK,
    response_model=ConnectionStatsResponse,
)
async def get_connection_stats_route(
    project_id: UUID = Path(..., description="Project ID to get connection stats for"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Get database connection statistics for a project.
    
    This endpoint provides statistics about database connections including:
    - Total number of connections
    - Number of active connections (status=True)
    - Number of inactive connections (status=False)
    
    Args:
        project_id (UUID): The project ID.
        db (Session): The database session.
        token_payload (dict): The token payload.
        
    Returns:
        ConnectionStatsResponse: Statistics about database connections.
    """
    return await get_connection_stats(project_id, db, token_payload)


@backend_router.post(
    "/connections/{project_id}/check",
    status_code=status.HTTP_200_OK,
    response_model=ConnectionCheckResponse,
)
async def check_connections_route(
    project_id: UUID = Path(..., description="Project ID to check connections for"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Check all database connections for a project and update their status.
    
    This endpoint tests each database connection by attempting to connect to it.
    For each connection, it:
    - Tests if the connection can be established
    - Updates the 'status' field (True if successful, False if failed)
    - Updates the 'last_checked' timestamp with the current time
    - Returns detailed results for each connection including error messages if any
    
    Args:
        project_id (UUID): The project ID.
        db (Session): The database session.
        token_payload (dict): The token payload.
        
    Returns:
        ConnectionCheckResponse: Results of connection checks with updated status.
    """
    return await check_and_update_connections(project_id, db, token_payload)


@backend_router.get("/projects", status_code=status.HTTP_200_OK)
async def get_projects_route(
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Get all projects.
    Args:
        request (Request): The request object.
        response (Response): The response object.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The projects.
    """
    return await get_projects(db, token_payload)


@backend_router.post(
    "/projects/{project_id}/users",
    status_code=status.HTTP_201_CREATED,
    response_model=CreateUserProjectResponse,
)
async def add_user_project(
    project_id: UUID = Path(..., description="Project ID to add user to"),
    data: CreateUserProjectRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Add a user to a project.
    Args:
        project_id (UUID): The project ID.
        data (CreateUserProjectRequest): The user data.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The created user.
    """
    return await create_user_project(data, db, token_payload, project_id)


@backend_router.get(
    "/projects/{project_id}/users",
    status_code=status.HTTP_200_OK,
    response_model=ListAllUsersProjectResponse,
)
async def list_all_users(
    project_id: UUID = Path(..., description="Project ID to list all users for"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    List all users for a project.
    Args:
        project_id (UUID): The project ID.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The users for the project.
    """
    return await list_all_users_project(project_id, db, token_payload)


@backend_router.get(
    "/projects/{project_id}/roles",
    status_code=status.HTTP_200_OK,
    response_model=ListAllRolesProjectResponse,
)
async def list_all_roles(
    project_id: UUID = Path(..., description="Project ID to list all roles for"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    List all roles for a project.
    Args:
        project_id (UUID): The project ID.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The roles for the project.
    """
    return await list_all_roles_project(project_id, db, token_payload)


@backend_router.post(
    "/projects/{project_id}/dashboard",
    status_code=status.HTTP_201_CREATED,
    response_model=CreateDashboardResponse,
)
async def dashboard(
    project_id: UUID = Path(..., description="Project ID to create dashboard for"),
    data: CreateDashboardRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Create a new dashboard.
    Args:
        project_id (UUID): The project ID.
        data (CreateDashboardRequest): The dashboard data.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The created dashboard.
    """
    return await create_dashboard(data, db, token_payload, project_id)


@backend_router.get(
    "/permissions",
    status_code=status.HTTP_200_OK,
    response_model=ListAllPermissionsResponse,
)
async def list_permissions(
    db: Session = Depends(get_db),
):
    """
    List all permissions.
    Args:
        db (Session): The database session.
    Returns:
        dict: The permissions.
    """
    return await list_all_permissions(db)


@backend_router.post(
    "/projects/{project_id}/roles",
    status_code=status.HTTP_201_CREATED,
    response_model=CreateRoleResponse,
)
async def create_roles(
    project_id: UUID = Path(..., description="Project ID to create role for"),
    data: CreateRoleRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Create a new role.
    Args:
        project_id (UUID): The project ID.
        data (CreateRoleRequest): The role data.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The created role.
    """
    return await create_role(data, db, token_payload, project_id)


@backend_router.post(
    "/projects/{project_id}/dashboard/user",
    status_code=status.HTTP_201_CREATED,
    response_model=AddUserDashboardResponse,
)
async def add_user_dashboard(
    project_id: UUID = Path(..., description="Project ID to add user to"),
    data: AddUserDashboardRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Add a user to a dashboard.
    Args:
        project_id (UUID): The project ID.
        data (AddUserDashboardRequest): The user data.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The created user.
    """
    return await add_user_to_dashboard(project_id, data, db, token_payload)


@backend_router.get(
    "/projects/{project_id}/users/dashboard", status_code=status.HTTP_200_OK
)
async def list_all_users_dashboard(
    project_id: UUID = Path(..., description="Project ID to list all users for"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    List all users for a dashboard.
    Args:
        project_id (UUID): The project ID.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The users for the dashboard.
    """
    return await list_users_all_dashboard(project_id, db, token_payload)


@backend_router.delete(
    "/projects/{project_id}/dashboard/{dashboard_id}", status_code=status.HTTP_200_OK
)
async def delete_dashboards(
    project_id: UUID = Path(..., description="Project ID to delete user dashboard for"),
    dashboard_id: UUID = Path(
        ..., description="Dashboard ID to delete user dashboard for"
    ),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Delete a user from a dashboard.
    Args:
        project_id (UUID): The project ID.
        dashboard_id (UUID): The dashboard ID.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The deleted user.
    """
    return await delete_dashboard(project_id, dashboard_id, db, token_payload)


@backend_router.get("/user_profile", status_code=status.HTTP_200_OK)
async def get_current_user_details(
    db: Session = Depends(get_db), token_payload: dict = Depends(get_current_user)
):
    """
    Get the current user's details.
    Args:
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The current user's details.
    """
    return await get_user_details(db, token_payload)


@backend_router.patch("/projects/{project_id}", status_code=status.HTTP_200_OK)
async def update(
    project_id: UUID = Path(..., description="Project ID to update"),
    data: UpdateProjectRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Update a project.
    Args:
        project_id (UUID): The project ID.
        data (UpdateProjectRequest): The project data.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The updated project.
    """
    return await update_project(project_id, data, db, token_payload)


@backend_router.delete("/projects/{project_id}", status_code=status.HTTP_200_OK)
async def delete(
    project_id: UUID = Path(..., description="Project ID to delete"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Delete a project.
    Args:
        project_id (UUID): The project ID.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The deleted project.
    """
    return await delete_project(project_id, db, token_payload)


@backend_router.patch(
    "/projects/{project_id}/dashboard/{dashboard_id}", status_code=status.HTTP_200_OK
)
async def update_dashboard_route(
    project_id: UUID = Path(..., description="Project ID to update"),
    dashboard_id: UUID = Path(..., description="Dashboard ID to update"),
    data: UpdateDashboardRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Update a dashboard.
    Args:
        project_id (UUID): The project ID.
        dashboard_id (UUID): The dashboard ID.
        data (UpdateDashboardRequest): The dashboard data.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The updated dashboard.
    """
    return await update_dashboard(project_id, dashboard_id, data, db, token_payload)


@backend_router.patch(
    "/projects/{project_id}/role/{role_id}", status_code=status.HTTP_200_OK
)
async def update_role_route(
    project_id: UUID = Path(..., description="Project ID to update"),
    role_id: UUID = Path(..., description="Role ID to update"),
    data: UpdateRoleRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Update a role.
    Args:
        project_id (UUID): The project ID.
        role_id (UUID): The role ID.
        data (UpdateRoleRequest): The role data.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The updated role.
    """
    return await update_role(project_id, role_id, data, db, token_payload)


@backend_router.delete(
    "/projects/{project_id}/role/{role_id}", status_code=status.HTTP_200_OK
)
async def delete_role_route(
    project_id: UUID = Path(..., description="Project ID to delete"),
    role_id: UUID = Path(..., description="Role ID to delete"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Delete a role.
    Args:
        project_id (UUID): The project ID.
        role_id (UUID): The role ID.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The deleted role.
    """
    return await delete_role(project_id, role_id, db, token_payload)


@backend_router.patch(
    "/projects/{project_id}/users/{user_id}", status_code=status.HTTP_200_OK
)
async def update_user_route(
    project_id: UUID = Path(..., description="Project ID to update user for"),
    user_id: UUID = Path(..., description="User ID to update"),
    data: UpdateUserRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Update a user.
    Args:
        project_id (UUID): The project ID.
        user_id (UUID): The user ID.
        data (UpdateUserRequest): The user data.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The updated user.
    """
    return await update_user(project_id, user_id, data, db, token_payload)


@backend_router.delete(
    "/projects/{project_id}/users/{user_id}", status_code=status.HTTP_200_OK
)
async def delete_user_route(
    project_id: UUID = Path(..., description="Project ID to delete user for"),
    user_id: UUID = Path(..., description="User ID to delete"),
    token_payload: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Delete a user.
    Args:
        project_id (UUID): The project ID.
        user_id (UUID): The user ID.
        db (Session): The database session.
    Returns:
        dict: The deleted user.
    """
    return await delete_user(project_id, user_id, token_payload, db)


@backend_router.patch("/connections/{connection_id}", status_code=status.HTTP_200_OK)
async def update_connection_route(
    connection_id: UUID = Path(..., description="Connection ID to update"),
    data: UpdateDBConnectionRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Update a database connection.
    Args:
        connection_id (UUID): The connection ID.
        data (UpdateDBConnectionRequest): The connection data.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The updated connection.
    """
    return await update_db_connection(
        connection_id=connection_id,
        data=data,
        db=db,
        token_payload=token_payload,
    )


@backend_router.delete("/connections/{connection_id}", status_code=status.HTTP_200_OK)
async def delete_connection_route(
    connection_id: UUID = Path(..., description="Connection ID to delete"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Delete a database connection.
    Args:
        connection_id (UUID): The connection ID.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The deleted connection.
    """
    return await delete_db_connection(connection_id, db, token_payload)


@backend_router.post("/super-user", status_code=status.HTTP_201_CREATED)
async def create_super_user(
    data: CreateSuperUserRequest,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Create a new super user.
    Args:
        data (CreateSuperUserRequest): The super user data.
        db (Session): The database session.
    Returns:
        dict: The created super user.
    """
    return await create_super_user_service(data, db, token_payload)


@backend_router.get("/super-user", status_code=status.HTTP_200_OK)
async def get_super_user(
    db: Session = Depends(get_db), token_payload: dict = Depends(get_current_user)
):
    """
    Get the super user.
    Args:
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The super user.
    """
    return await get_super_user_service(db, token_payload)


@backend_router.get("/dashboard/{dashboard_id}/users", status_code=status.HTTP_200_OK)
async def get_users_dashboard(
    dashboard_id: UUID = Path(..., description="Dashboard ID to get users for"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Get the users for a dashboard.
    Args:
        dashboard_id (UUID): The dashboard ID.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The users for the dashboard.
    """
    return await get_users_dashboard_service(dashboard_id, db, token_payload)


@backend_router.get("/projects/{project_id}/owners", status_code=status.HTTP_200_OK)
async def get_project_owner(
    project_id: UUID = Path(..., description="Project ID to get owner for"),
    db: Session = Depends(get_db),
):
    """
    Get the owner for a project.
    Args:
        project_id (UUID): The project ID.
        db (Session): The database session.
    Returns:
        dict: The owner for the project.
    """
    return await get_project_owner_service(project_id, db)


@backend_router.get("/dashboards/{dashboard_id}/owners", status_code=status.HTTP_200_OK)
async def get_dashboard_owner(
    dashboard_id: UUID = Path(..., description="Dashboard ID to get owner for"),
    db: Session = Depends(get_db),
):
    """
    Get the owner for a dashboard.
    Args:
        dashboard_id (UUID): The dashboard ID.
        db (Session): The database session.
    Returns:`̀
        dict: The owner for the dashboard.
    """
    return await get_dashboard_owner_service(dashboard_id, db)


@backend_router.get("/favorites", status_code=status.HTTP_200_OK)
async def get_favorites(
    db: Session = Depends(get_db), token_payload: dict = Depends(get_current_user)
):
    """
    Get the favorites for a user.
    Args:
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The favorites for the user.
    """
    return await get_favorites_service(db, token_payload)


@backend_router.post("/projects/{project_id}/blacklist", status_code=status.HTTP_200_OK)
async def blacklist(
    project_id: UUID = Path(..., description="Project ID to blacklist tables for"),
    data: BlackListTableNameRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Blacklist tables for a project.
    Args:
        project_id (UUID): The project ID.
        data (BlackListTableNameRequest): The table names to blacklist.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The blacklisted tables response.
    """
    return await blacklist_service(project_id, data, db, token_payload)


@backend_router.patch(
    "/projects/{project_id}/blacklist", status_code=status.HTTP_200_OK
)
async def update_blacklist(
    project_id: UUID = Path(
        ..., description="Project ID to update blacklist tables for"
    ),
    data: BlackListTableNameRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Update blacklist tables for a project.
    Args:
        project_id (UUID): The project ID.
        data (BlackListTableNameRequest): The table names to update blacklist.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The updated blacklisted tables response.
    """
    return await update_blacklist_service(project_id, data, db, token_payload)


@backend_router.patch("/grant-access", status_code=status.HTTP_200_OK)
async def grant_access(
    data: ReadDataRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Grant access to a project.
    Args:
        data (ReadDataRequest): The grant access data.
        db (Session): The database session.
        token_payload (dict): The token payload.
    Returns:
        dict: The grant access response.
    """
    return await read_data_service(data, db, token_payload)


@backend_router.post(
    "/projects/{project_id}/request-access", status_code=status.HTTP_200_OK
)
@backend_router.post("/projects/{project_id}/request-access")
async def request_access(
    project_id: UUID = Path(..., description="Project ID to request access for"),
    data: RequestAccess = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Request access to a specific project.

    Args:
        project_id (UUID): The ID of the project to request access for.
        data (RequestAccess): Request access payload.
        db (Session): Database session.
        token_payload (dict): Authenticated user payload.

    Returns:
        dict: Response from the request access service.
    """
    return await request_access_service(project_id, data, db, token_payload)


@backend_router.post("/generate_charts/{project_id}/{datasource_connection_id}")
async def generate_charts(
    project_id: UUID,
    datasource_connection_id: UUID,
    request: QueryRequest,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    try:
        charts = await generate_and_store_charts(
            db, datasource_connection_id, project_id, request, token_payload
        )

        return {
            "success": True,
            "generated_charts": [
                {
                    "id": str(chart.id),
                    "title": chart.title,
                    "query": chart.query,
                    "chart_type": chart.chart_type,
                    "relevance": chart.relevance,
                    "is_time_based": chart.is_time_based,
                    "report": chart.report,
                }
                for chart in charts
            ],
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@backend_router.post("/nl2sql/generate/{datasource_connection_id}")
async def generate_and_save_route(
    data: Nl2SQLChatRequest = Body(...),
    db: Session = Depends(get_db),
    datasource_connection_id: UUID = Path(...),
    token_payload: dict = Depends(get_current_user),
):
    """
    Generate and store SQL queries from natural language input.

    Args:
        data (Nl2SQLChatRequest): Natural language input for query generation.
        db (Session): Database session.
        datasource_connection_id (UUID): Connection ID for the target datasource.
        token_payload (dict): Authenticated user payload.

    Returns:
        dict: Generated SQL query and related metadata.
    """
    user_id_str = token_payload.get("sub")
    if not user_id_str:
        raise HTTPException(status_code=401, detail="Unauthorized")

    user_id = UUID(user_id_str)
    return await generate_nl_sql(data, db, user_id, datasource_connection_id)


@backend_router.post("/excecute-query/{datasource_connection_id}/")
def execute_query(
    datasource_connection_id: UUID,
    db: Session = Depends(get_db),
    request: QueryExecutionRequest = Body(...),
    token_payload: dict = Depends(get_current_user),
):
    """
    Execute a raw SQL query against the specified datasource.

    Args:
        query_id (UUID): Query identifier.
        datasource_connection_id (UUID): Datasource connection ID.
        db (Session): Database session.
        request (QueryExecutionRequest): Query to execute.
        token_payload (dict): Authenticated user payload.

    Returns:
        dict: Query execution result.
    """
    # Only people in the datasource's project may query it. Outside the try below, so a 403 stays a 403.
    ensure_datasource_access(db, token_payload, datasource_connection_id)
    try:
        return execute_external_query(
            db=db,
            datasource_connection_id=datasource_connection_id,
            query_input=request.query,
            token_payload=token_payload,
            from_date=request.from_date,
            to_date=request.to_date,
            response_format=request.response_format,
            x_axis=request.x_axis,
            y_axis=request.y_axis,
        )
    except ConnectorQueryError:
        # Keep its status (503 offline, 504 timeout, 429 busy, ...): the catch-all below would make it a 500.
        raise

    except SQLAlchemyError as e:
        # Explicitly re-raise the exception with context
        raise HTTPException(
            status_code=500, detail=f"Database execution error: {str(e)}"
        ) from e

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"ValueError: {str(e)}") from e

    except Exception as e:
        # Log the full exception with stack trace
        logger.exception("Unexpected error executing query", exc_info=True)
        # Explicitly re-raise the unexpected exception with more context
        raise HTTPException(status_code=500, detail="Unexpected error occurred") from e


@backend_router.post(
    "/generate_multiple_db_queries/{project_id}/", response_model=TrinoQueryResponse
)
async def generate_queries_route(
    project_id: UUID,
    request: TrinoQueryRequest,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Generate Trino SQL queries for multiple database connections.

    Args:
        project_id (UUID): Project identifier.
        request (TrinoQueryRequest): Payload including connection IDs and other context.
        db (Session): Database session.
        token_payload (dict): Authenticated user payload.

    Returns:
        TrinoQueryResponse: Generated SQL queries for each connection.
    """
    if not request.connection_ids:
        raise HTTPException(status_code=400, detail="Connection IDs are required")

    return await generate_trino_queries_service(
        db=db, project_id=project_id, token_payload=token_payload, request=request
    )


@backend_router.patch(
    "/projects/{project_id}/request-access/{request_id}", status_code=status.HTTP_200_OK
)
async def update_request_access(
    project_id: UUID = Path(..., description="Project ID to update request access for"),
    request_id: UUID = Path(..., description="Request ID to update"),
    data: UpdateRequestAccess = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Update the request access for a specific project.

    Args:
        project_id (UUID): The ID of the project.
        request_id (UUID): The ID of the request to update.
        data (UpdateRequestAccess): The data to update the request access with.
        db (Session): The database session.
        token_payload (dict): The authenticated user's token payload.

    Returns:
        dict: The updated request access data.
    """
    return await update_request_access_service(
        project_id, request_id, data, db, token_payload
    )


@backend_router.get(
    "/projects/{project_id}/request-access", status_code=status.HTTP_200_OK
)
async def get_request_access(
    project_id: UUID = Path(..., description="Project ID to get request access for"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Retrieve the request access for a specific project.

    Args:
        project_id (UUID): The ID of the project to get the request access for.
        db (Session): The database session.
        token_payload (dict): The authenticated user's token payload.

    Returns:
        dict: The request access data for the specified project.
    """
    return await get_access_requests_service(project_id, db, token_payload)


# @backend_router.get("/charts", status_code=status.HTTP_200_OK)
# async def get_charts(
#     db: Session = Depends(get_db),
#     token_payload: dict = Depends(get_current_user)
# ):
#     return await get_charts_service(db, token_payload)


@backend_router.post(
    "/projects/{project_id}/save-chart", status_code=status.HTTP_200_OK
)
async def save_chart(
    project_id: UUID = Path(..., description="Project ID to save chart for"),
    data: SaveChartRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Save a chart for the specified project.

    Args:
        project_id (UUID): The ID of the project to save the chart for.
        data (SaveChartRequest): The chart data to save.
        db (Session): The database session.
        token_payload (dict): The payload of the current authenticated user.

    Returns:
        dict: The response containing the saved chart details.
    """
    return await save_chart_service(project_id, data, db, token_payload)


@backend_router.post("/charts/save-to-dashboard", status_code=status.HTTP_200_OK)
async def save_chart_to_dashboard(
    data: SaveChartToDashboardRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Save a chart to the specified dashboard.

    Args:
        data (SaveChartToDashboardRequest): The chart data and dashboard ID to save to.
        db (Session): The database session.
        token_payload (dict): The payload of the current authenticated user.

    Returns:
        dict: The response indicating the chart has been saved to the dashboard.
    """
    return await save_chart_to_dashboard_service(data, db, token_payload)


@backend_router.get("/dashboards/user/charts", status_code=status.HTTP_200_OK)
async def get_user_dashboard_charts(
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Get all charts from dashboards that the current user is a member of.
    """
    return await get_user_dashboard_charts_service(db, token_payload)


@backend_router.get("/dashboards/{dashboard_id}/charts", status_code=status.HTTP_200_OK)
async def get_charts_for_dashboard(
    dashboard_id: UUID = Path(..., description="Dashboard ID to get charts for"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Get all charts for the specified dashboard.

    Args:
        dashboard_id (UUID): The ID of the dashboard to retrieve charts for.
        db (Session): The database session.
        token_payload (dict): The payload of the current authenticated user.

    Returns:
        list: A list of charts associated with the specified dashboard.
    """
    return await get_charts_for_dashboard_service(dashboard_id, db, token_payload)


@backend_router.post(
    "/dashboards/{dashboard_id}/generate-kpi-queries",
    status_code=status.HTTP_200_OK,
    response_model=GenerateKpiQueriesResponse,
)
async def generate_kpi_queries_route(
    dashboard_id: UUID = Path(..., description="Dashboard ID to generate KPI queries for"),
    data: GenerateKpiQueriesRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Generate (or return cached) KPI infographic queries for an Autopilot Dashboard.

    On first call the LLM service analyses the database schema and KPI goals to
    produce N aggregation SQL descriptors. Results are saved to the dashboard row
    and returned. Subsequent calls return the stored list without hitting the LLM
    unless force=True is passed.
    """
    return await generate_kpi_queries_service(
        dashboard_id=dashboard_id,
        connection_id=data.connection_id,
        db_schema=data.db_schema,
        db_type=data.db_type,
        num_kpis=data.num_kpis,
        force=data.force,
        db=db,
        token_payload=token_payload,
    )


@backend_router.post(
    "/dashboards/{dashboard_id}/regenerate-kpi",
    status_code=status.HTTP_200_OK,
)
async def regenerate_kpi_route(
    dashboard_id: UUID = Path(..., description="Dashboard ID"),
    data: RegenerateKpiRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Regenerate a single KPI query after it failed or returned 0.
    """
    return await regenerate_kpi_service(
        dashboard_id=dashboard_id,
        connection_id=data.connection_id,
        label=data.label,
        failed_query=data.failed_query,
        db=db,
        token_payload=token_payload,
    )


@backend_router.delete(
    "/charts/{chart_id}", status_code=status.HTTP_200_OK
)
async def delete_chart(
    chart_id: UUID = Path(..., description="Chart ID to delete"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Delete a chart entirely from the system.

    Args:
        chart_id (UUID): The ID of the chart to delete.
        db (Session): The database session.
        token_payload (dict): The payload of the current authenticated user.

    Returns:
        dict: A response indicating the chart has been successfully deleted.
    """
    return await delete_chart_service(chart_id, db, token_payload)


@backend_router.delete(
    "/dashboards/{dashboard_id}/charts/{chart_id}", status_code=status.HTTP_200_OK
)
async def delete_chart_from_dashboard(
    dashboard_id: UUID = Path(..., description="Dashboard ID to delete chart from"),
    chart_id: UUID = Path(..., description="Chart ID to delete"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Delete a chart from the specified dashboard.

    Args:
        dashboard_id (UUID): The ID of the dashboard to delete the chart from.
        chart_id (UUID): The ID of the chart to delete.
        db (Session): The database session.
        token_payload (dict): The payload of the current authenticated user.

    Returns:
        dict: A response indicating the chart has been successfully deleted.
    """
    return await delete_chart_from_dashboard_service(
        dashboard_id, chart_id, db, token_payload
    )


@backend_router.patch("/charts/favorite", status_code=status.HTTP_200_OK)
async def update_favorite_chart(
    data: UpdateFavoriteChartRequest = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Update the favorite status of a chart.

    Args:
        data (UpdateFavoriteChartRequest): The data to update the favorite status of a chart.
        db (Session): The database session.
        token_payload (dict): The payload of the current authenticated user.

    Returns:
        dict: The updated favorite chart details.
    """
    return await update_favorite_chart_service(data, db, token_payload)


@backend_router.get("/charts/favorite", status_code=status.HTTP_200_OK)
async def get_favorite_charts(
    db: Session = Depends(get_db), token_payload: dict = Depends(get_current_user)
):
    """
    Get all favorite charts for the authenticated user.

    Args:
        db (Session): The database session.
        token_payload (dict): The payload of the current authenticated user.

    Returns:
        list: A list of favorite charts for the authenticated user.
    """
    return await get_favorite_charts_service(db, token_payload)


@backend_router.get(
    "/users/charts/favorite", status_code=status.HTTP_200_OK
)
async def get_user_favorite_charts(
    
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """Retrieve favorite charts for the specified user."""

    return await get_user_favorite_charts_service(db, token_payload)


@backend_router.get(
    "/charts/pinned/count",
    status_code=status.HTTP_200_OK,
    response_model=PinnedChartsCountResponse,
)
async def get_pinned_charts_count(
    db: Session = Depends(get_db), token_payload: dict = Depends(get_current_user)
):
    """
    Get the count of pinned (favorite) charts for the authenticated user.

    Args:
        db (Session): The database session.
        token_payload (dict): The payload of the current authenticated user.

    Returns:
        PinnedChartsCountResponse: The count of pinned charts for the authenticated user.
    """
    return await get_pinned_charts_count_service(db, token_payload)


@backend_router.get("/charts", status_code=status.HTTP_200_OK)
async def get_charts(
    project_id: UUID = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user)
):
    """
    Get all charts for the authenticated user, optionally filtered by project.

    Args:
        project_id (UUID): Optional project ID to filter charts by.
        db (Session): The database session.
        token_payload (dict): The payload of the current authenticated user.

    Returns:
        dict: A dictionary containing charts for the authenticated user.
    """
    return await get_charts_service(db, token_payload, project_id)


@backend_router.get("/charts/filter", status_code=status.HTTP_200_OK)
async def filter_charts(
    dashboard_id: UUID = None,
    database_connection_id: UUID = None,
    status: str = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Filter charts based on multiple optional query parameters.
    
    All filter parameters are optional and can be combined:
    - dashboard_id: Filter charts by specific dashboard UUID
    - database_connection_id: Filter charts by database connection UUID
    - status: Filter charts by status ("draft" or "published")
    
    Status definitions:
    - "draft": Charts not added to any dashboard
    - "published": Charts added to at least one dashboard
    - omitted/null: All charts (no status filter)
    
    Examples:
    - /charts/filter - Get all charts
    - /charts/filter?status=draft - Get only draft charts
    - /charts/filter?dashboard_id=xxx - Get charts in specific dashboard
    - /charts/filter?dashboard_id=xxx&status=published - Combined filters

    Args:
        dashboard_id (UUID): Optional UUID of the dashboard to filter by
        database_connection_id (UUID): Optional UUID of the database connection to filter by
        status (str): Optional status filter ("draft" or "published")
        db (Session): The database session
        token_payload (dict): The payload of the current authenticated user

    Returns:
        dict: Filtered charts with metadata including:
            - charts: List of filtered chart objects
            - total_count: Number of charts returned
            - filters_applied: Summary of applied filters
    """
    return await filter_charts_service(
        db=db,
        token_payload=token_payload,
        dashboard_id=dashboard_id,
        database_connection_id=database_connection_id,
        status=status,
    )


@backend_router.get(
    "/dashboard-stats", status_code=status.HTTP_200_OK, response_model=DashboardStatsResponse
)
async def get_dashboard_stats(
    db: Session = Depends(get_db), token_payload: dict = Depends(get_current_user)
):
    """
    Get dashboard statistics for the authenticated user including:
    - Total projects user is part of
    - Active dashboards user has access to
    - Total data sources across user's projects
    - Total team members across user's projects

    Args:
        db (Session): The database session.
        token_payload (dict): The payload of the current authenticated user.

    Returns:
        DashboardStatsResponse: Dashboard statistics for the authenticated user.
    """
    return await get_dashboard_stats_service(db, token_payload)


@backend_router.post(
    "/business-insights",
    status_code=status.HTTP_200_OK,
    response_model=BusinessInsightsResponse,
)
async def generate_business_insights(
    data: BusinessInsightsRequest,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Generate AI-powered business insights from a database connection.

    This endpoint performs a comprehensive analysis:
    1. Extracts the database schema (tables, columns, data types)
    2. Uses AI (LangChain + LLM) to identify 10 key business KPIs
    3. Generates optimized SQL queries for each KPI
    4. Executes queries against the connected database
    5. Uses AI to analyze results and generate actionable insights

    The insights include:
    - Executive summary of findings
    - Detailed analysis of key metrics
    - Identified patterns and trends
    - Actionable business recommendations
    - Areas of concern requiring attention

    Args:
        data (BusinessInsightsRequest): Contains database_connection_id and optional api_key
        db (Session): The database session
        token_payload (dict): The payload of the current authenticated user

    Returns:
        BusinessInsightsResponse: Comprehensive business insights including:
            - Generated KPI queries
            - Query execution results
            - AI-generated insights and recommendations

    Example:
        POST /api/v1/backend/business-insights
        {
            "database_connection_id": "550e8400-e29b-41d4-a716-446655440000"
        }
        
    Note:
        Gemini API key must be configured in backend environment:
        GEMINI_API_KEY=your-api-key-here
    """
    return await generate_business_insights_service(
        db=db,
        token_payload=token_payload,
        database_connection_id=data.database_connection_id,
    )


@backend_router.post(
    "/projects/{project_id}/business-insights",
    status_code=status.HTTP_200_OK,
    response_model=ProjectInsightsResponse,
)
async def generate_project_business_insights(
    project_id: UUID = Path(..., description="Project ID to analyze"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Generate comprehensive AI-powered business insights for ALL databases in a project.

    This endpoint performs a complete project-wide analysis:
    1. Fetches all database connections in the project
    2. For each database:
       - Loads cached schema from db_connection.db_schema
       - Uses AI to identify 10 key business KPIs
       - Generates optimized SQL queries for each KPI
       - Executes queries against the database
       - Uses AI to analyze results and generate insights
    3. Consolidates insights across all databases
    4. Uses AI to generate unified strategic recommendations

    The response includes:
    - Individual insights for each database (full analysis per DB)
    - Consolidated project-level strategic analysis
    - Overall business health assessment (excellent/good/fair/poor)
    - Cross-database patterns and correlations
    - Top 5 strategic priorities (ranked by impact)
    - Risk assessment (critical and moderate risks)
    - Business opportunities with potential impact

    Args:
        project_id (UUID): Project ID to analyze (from URL path)
        db (Session): The database session
        token_payload (dict): The payload of the current authenticated user

    Returns:
        ProjectInsightsResponse: Comprehensive project-wide business insights including:
            - Insights from each database in the project
            - Consolidated strategic analysis across all data sources
            - Business health score
            - Strategic priorities
            - Risk assessment
            - Opportunities

    Example:
        POST /api/v1/backend/projects/2f96eace-5738-4c9f-afba-155203ee1434/business-insights
        
        # No request body required - project_id comes from URL
        # Or optionally send empty body: {}
        
    Note:
        - Gemini API key must be configured: GEMINI_API_KEY in .env
        - This can take 30-120 seconds depending on number of databases
        - Each database is analyzed independently
        - Continues even if some databases fail
    """
    return await generate_project_insights_service(
        db=db,
        token_payload=token_payload,
        project_id=project_id,
    )


# @backend_router.get(
#     "/business-insights/latest",
#     status_code=status.HTTP_200_OK,
#     response_model=LatestBusinessInsightResponse,
# )
# async def get_latest_business_insight_route(
#     project_id: UUID = Query(..., description="Project ID to filter business insights"),
#     user_id: Optional[UUID] = Query(
#         None, description="User ID to filter (defaults to current user)"
#     ),
#     db: Session = Depends(get_db),
#     token_payload: dict = Depends(get_current_user),
# ):
#     """
#     Fetch the latest generated business insight for a specific user and project.
#     """
#     return await get_latest_business_insight_service(
#         db=db,
#         token_payload=token_payload,
#         project_id=project_id,
#         user_id=user_id,
#     )


@backend_router.get("/admin/engine-stats", status_code=status.HTTP_200_OK)
async def get_engine_stats(
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Get statistics about external database engine cache.

    Returns cache metrics including:
    - Number of cached engines
    - Maximum cache size
    - Cache utilization percentage
    - List of cached connection IDs

    This endpoint is useful for monitoring engine pool performance
    and identifying potential memory issues.

    Returns:
        dict: Engine cache statistics
    """
    from app.core.db import external_engine_manager
    return external_engine_manager.get_cache_stats()


@backend_router.post(
    "/home-insights",
    status_code=status.HTTP_201_CREATED,
    response_model=SaveHomeInsightResponse,
)
async def save_home_insight(
    data: SaveHomeInsightRequest,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Save an insight to the user's home page.
    
    This endpoint allows users to pin important insights to their home page
    for quick access and monitoring.
    
    Args:
        data (SaveHomeInsightRequest): Insight data including title, description, type, etc.
        db (Session): The database session
        token_payload (dict): The authenticated user's token payload
        
    Returns:
        SaveHomeInsightResponse: The saved insight with its ID and metadata
        
    Example:
        POST /api/v1/backend/home-insights
        {
            "project_id": "550e8400-e29b-41d4-a716-446655440000",
            "title": "Revenue Growth Trend",
            "description": "Monthly revenue has increased by 15% over the last quarter",
            "insight_type": "positive",
            "category": "Revenue",
            "impact": "High",
            "source": "Sales Database"
        }
    """
    return await save_home_insight_service(
        data=data.dict(),
        db=db,
        token_payload=token_payload
    )


@backend_router.get(
    "/home-insights",
    status_code=status.HTTP_200_OK,
    response_model=GetHomeInsightsResponse,
)
async def get_home_insights(
    project_id: Optional[UUID] = Query(None, description="Filter by project ID"),
    limit: int = Query(10, description="Maximum number of insights to return", ge=1, le=50),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Get all saved home insights for the current user.
    
    This endpoint retrieves insights that the user has pinned to their home page,
    ordered by most recent first.
    
    Args:
        project_id (Optional[UUID]): Optional project ID to filter insights
        limit (int): Maximum number of insights to return (1-50, default 10)
        db (Session): The database session
        token_payload (dict): The authenticated user's token payload
        
    Returns:
        GetHomeInsightsResponse: List of home insights and total count
        
    Example:
        GET /api/v1/backend/home-insights?limit=5
        GET /api/v1/backend/home-insights?project_id=550e8400-e29b-41d4-a716-446655440000
    """
    return await get_home_insights_service(
        db=db,
        token_payload=token_payload,
        project_id=project_id,
        limit=limit
    )


@backend_router.delete(
    "/home-insights/{insight_id}",
    status_code=status.HTTP_200_OK,
)
async def delete_home_insight(
    insight_id: UUID = Path(..., description="ID of the insight to delete"),
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    """
    Delete a saved home insight.
    
    This endpoint removes an insight from the user's home page.
    Only the user who created the insight can delete it.
    
    Args:
        insight_id (UUID): The ID of the insight to delete
        db (Session): The database session
        token_payload (dict): The authenticated user's token payload
        
    Returns:
        dict: Confirmation message
        
    Example:
        DELETE /api/v1/backend/home-insights/550e8400-e29b-41d4-a716-446655440000
    """
    return await delete_home_insight_service(
        insight_id=insight_id,
        db=db,
        token_payload=token_payload
    )


@backend_router.post(
    "/connections/{connection_id}/pbit-upload",
    status_code=status.HTTP_200_OK,
)
async def upload_pbit_file(
    connection_id: UUID = Path(..., description="Connection ID to import PBIT measures into"),
    file: UploadFile = None,
    db: Session = Depends(get_db),
    token_payload: dict = Depends(get_current_user),
):
    return await process_pbit_upload(connection_id, file, db, token_payload)
