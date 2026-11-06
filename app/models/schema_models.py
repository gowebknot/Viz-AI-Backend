"""
This module defines the SQLAlchemy ORM models for the Viz-AI-Backend application.

The models represent the database schema and relationships for various entities in the system,
including users, projects, roles, permissions, dashboards, charts, database connections,
and access requests. These models are used to interact with the database and enforce
business logic through relationships and constraints.

Classes:
    UserModel: Represents a user in the system.
    RolePermissionModel: Represents the relationship between roles and permissions.
    UserDashboardModel: Represents the relationship between users and dashboards.
    UserChartModel: Represents the relationship between users and charts.
    ApiKeyModel: Represents an API key associated with a user.
    ProjectModel: Represents a project in the system.
    RoleModel: Represents a role in the system.
    UserProjectRoleModel: Represents the mapping between users, projects, and roles.
    PermissionModel: Represents a permission in the system.
    DashboardModel: Represents a dashboard in the system.
    ChartModel: Represents a chart in the system.
    DashboardChartsModel: Represents the relationship between dashboards and charts.
    DatabaseConnectionModel: Represents a database connection in the system.
    ConnectionTableNameModel: Represents a table name in a database connection.
    RoleTableNameModel: Represents a table name in a role.
    RelatedDatabaseModel: Represents a related database in the system.
    ChartAccessRequestModel: Represents a request to access a chart.
    ResponseTimeModel: Represents response time metrics for API endpoints.
"""

from datetime import datetime
from uuid import uuid4

from sqlalchemy import Boolean, Column, DateTime, Double, Index, Integer, Integer, Numeric, Date, BigInteger
from sqlalchemy import Enum as SqlEnum
from sqlalchemy import ForeignKey, String, Text, func
from sqlalchemy.dialects.postgresql import ARRAY, UUID, JSONB
from sqlalchemy.orm import relationship

from app.core.base import Base


ChartStatusEnum = SqlEnum("draft", "published", name="chart_status")


class UserModel(Base):
    """
    Represents a user in the system.
    """

    __tablename__ = "user"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    username = Column(String, unique=True, nullable=False)
    password = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=True, onupdate=func.now())
    is_super = Column(Boolean, nullable=True, default=False)

    project_roles = relationship(
        "UserProjectRoleModel", back_populates="user", cascade="all, delete-orphan"
    )
    api_key = relationship(
        "ApiKeyModel",
        back_populates="user",
        uselist=False,
        cascade="all, delete-orphan",
    )
    dashboards_shared = relationship(
        "UserDashboardModel", back_populates="user", cascade="all, delete-orphan"
    )
    charts_shared = relationship(
        "UserChartModel", back_populates="user", cascade="all, delete-orphan"
    )
    dashboards = relationship(
        "DashboardModel", back_populates="user", cascade="all, delete-orphan"
    )
    charts = relationship(
        "ChartModel", back_populates="user", cascade="all, delete-orphan"
    )
    projects = relationship(
        "ProjectModel",
        back_populates="super_user",
        foreign_keys="ProjectModel.super_user_id",
    )
    sent_chart_access_requests = relationship(
        "ChartAccessRequestModel",
        back_populates="requester",
        foreign_keys="ChartAccessRequestModel.requested_by",
        cascade="all, delete-orphan",
    )

    reviewed_chart_access_requests = relationship(
        "ChartAccessRequestModel",
        back_populates="reviewer_user",
        foreign_keys="ChartAccessRequestModel.reviewer",
        cascade="all, delete-orphan",
    )
    business_insights = relationship(
        "BusinessInsightModel", back_populates="user", cascade="all, delete-orphan"
    )

    home_insights = relationship(
        "HomeInsightModel", back_populates="user", cascade="all, delete-orphan"
    )

class RolePermissionModel(Base):
    """
    Represents the relationship between roles and permissions.
    """

    __tablename__ = "role_permission"

    role_id = Column(UUID(as_uuid=True), ForeignKey("role.id"), primary_key=True)
    permission_id = Column(
        UUID(as_uuid=True), ForeignKey("permission.id"), primary_key=True
    )

    role = relationship("RoleModel", back_populates="role_permissions")
    permission = relationship("PermissionModel", back_populates="role_permissions")


class UserDashboardModel(Base):
    """
    Represents the relationship between users and dashboards.
    """

    __tablename__ = "user_dashboard"

    user_id = Column(
        UUID(as_uuid=True), ForeignKey("user.id", ondelete="CASCADE"), primary_key=True
    )
    dashboard_id = Column(
        UUID(as_uuid=True),
        ForeignKey("dashboard.id", ondelete="CASCADE"),
        primary_key=True,
    )
    can_write = Column(Boolean, nullable=False, default=False)
    can_read = Column(Boolean, nullable=False, default=True)
    can_delete = Column(Boolean, nullable=False, default=False)
    is_owner = Column(Boolean, nullable=True, default=False)
    is_favorite = Column(Boolean, nullable=True, default=False)

    user = relationship("UserModel", back_populates="dashboards_shared")
    dashboard = relationship("DashboardModel", back_populates="users")


class UserChartModel(Base):
    """
    Represents the relationship between users and charts.
    """

    __tablename__ = "user_chart"

    user_id = Column(
        UUID(as_uuid=True), ForeignKey("user.id", ondelete="CASCADE"), primary_key=True
    )
    chart_id = Column(
        UUID(as_uuid=True), ForeignKey("chart.id", ondelete="CASCADE"), primary_key=True
    )
    can_write = Column(Boolean, nullable=False, default=False)
    can_read = Column(Boolean, nullable=False, default=True)
    can_delete = Column(Boolean, nullable=False, default=False)
    is_favorite = Column(Boolean, nullable=True, default=False)
    database_connection_id = Column(
        UUID(as_uuid=True),
        ForeignKey("database_connection.id", ondelete="CASCADE"),
        nullable=False,
    )
    database_connection = relationship(
        "DatabaseConnectionModel", back_populates="user_charts"
    )
    user = relationship("UserModel", back_populates="charts_shared")
    chart = relationship("ChartModel", back_populates="users")


class ApiKeyModel(Base):
    """
    Represents an API key associated with a user.
    """

    __tablename__ = "api_key"

    user_id = Column(
        UUID(as_uuid=True), ForeignKey("user.id", ondelete="CASCADE"), primary_key=True
    )
    api_key = Column(String, nullable=False)
    secret_key = Column(String, nullable=False)
    project_id = Column(
        UUID(as_uuid=True), ForeignKey("project.id", ondelete="CASCADE"), nullable=False
    )

    user = relationship("UserModel", back_populates="api_key")
    project = relationship("ProjectModel", back_populates="api_keys")


class ProjectModel(Base):
    """
    Represents a project in the system.
    """

    __tablename__ = "project"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    name = Column(String, nullable=False)
    super_user_id = Column(UUID(as_uuid=True), ForeignKey("user.id"))
    description = Column(Text)
    primary_domain = Column(String(255), nullable=False)
    additional_kpis = Column(String(500), nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())


    project_roles = relationship(
        "UserProjectRoleModel", back_populates="project", cascade="all, delete-orphan"
    )
    api_keys = relationship(
        "ApiKeyModel", back_populates="project", cascade="all, delete-orphan"
    )
    roles = relationship(
        "RoleModel", back_populates="project", cascade="all, delete-orphan"
    )
    super_user = relationship("UserModel", back_populates="projects")
    dashboards = relationship(
        "DashboardModel", back_populates="project", cascade="all, delete-orphan"
    )
    database_connections = relationship(
        "DatabaseConnectionModel",
        back_populates="project",
        cascade="all, delete-orphan",
    )
    access_requests = relationship(
        "ChartAccessRequestModel",
        back_populates="project",
        cascade="all, delete-orphan",
    )
    business_insights = relationship(
        "BusinessInsightModel", back_populates="project", cascade="all, delete-orphan"
    )
    home_insights = relationship(
            "HomeInsightModel", back_populates="project", cascade="all, delete-orphan"
        )

class RoleModel(Base):
    """
    Represents a role in the system.
    """

    __tablename__ = "role"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    name = Column(String, unique=True, nullable=False)
    description = Column(Text, nullable=True)
    project_id = Column(UUID(as_uuid=True), ForeignKey("project.id"), nullable=True)
    is_global = Column(Boolean, nullable=True, default=False)

    project_roles = relationship(
        "UserProjectRoleModel", back_populates="role", cascade="all, delete-orphan"
    )
    role_permissions = relationship(
        "RolePermissionModel", back_populates="role", cascade="all, delete-orphan"
    )
    project = relationship("ProjectModel", back_populates="roles")
    # Add this relationship to connect with RoleTableNameModel
    role_table_names = relationship(
        "RoleTableNameModel", back_populates="role", cascade="all, delete-orphan"
    )


class UserProjectRoleModel(Base):
    """
    Represents the mapping between users, projects, and roles.
    """

    __tablename__ = "user_project_role"

    user_id = Column(UUID(as_uuid=True), ForeignKey("user.id"), primary_key=True)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey("project.id", ondelete="CASCADE"),
        primary_key=True,
    )
    role_id = Column(UUID(as_uuid=True), ForeignKey("role.id"), primary_key=True)
    is_owner = Column(Boolean, nullable=True, default=False)

    user = relationship("UserModel", back_populates="project_roles")
    project = relationship("ProjectModel", back_populates="project_roles")
    role = relationship("RoleModel", back_populates="project_roles")


class PermissionModel(Base):
    """
    Represents a permission in the system.
    """

    __tablename__ = "permission"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    type = Column(String, nullable=False)

    role_permissions = relationship(
        "RolePermissionModel", back_populates="permission", cascade="all, delete-orphan"
    )


class DashboardModel(Base):
    """
    Represents a dashboard in the system.
    """

    __tablename__ = "dashboard"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    title = Column(String, nullable=False)
    project_id = Column(UUID(as_uuid=True), ForeignKey("project.id"), nullable=False)
    description = Column(Text)
    created_by = Column(UUID(as_uuid=True), ForeignKey("user.id"), nullable=False)
    is_autopilot = Column(Boolean, nullable=False, default=False)
    kpi_goals = Column(Text, nullable=True)
    kpi_queries = Column(JSONB, nullable=True)
    user = relationship("UserModel", back_populates="dashboards")
    project = relationship("ProjectModel", back_populates="dashboards")
    users = relationship(
        "UserDashboardModel",
        back_populates="dashboard",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    charts = relationship(
        "DashboardChartsModel", back_populates="dashboard", cascade="all, delete-orphan"
    )


class ChartModel(Base):
    """
    Represents a chart in the system.
    """

    __tablename__ = "chart"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    title = Column(String, nullable=False)
    query = Column(Text, nullable=False)
    report = Column(Text, nullable=True)
    type = Column(String, nullable=False)
    relevance = Column(Double, nullable=False)
    is_time_based = Column(Boolean, nullable=True)
    chart_type = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    is_user_generated = Column(Boolean, nullable=False, default=False)
    created_by = Column(UUID(as_uuid=True), ForeignKey("user.id"), nullable=False)
    status = Column(ChartStatusEnum, nullable=False, server_default="draft")
    x_axis = Column(String, nullable=True)
    y_axis = Column(String, nullable=True)
    user = relationship("UserModel", back_populates="charts")
    users = relationship(
        "UserChartModel", back_populates="chart", cascade="all, delete-orphan"
    )
    dashboard_charts = relationship(
        "DashboardChartsModel", back_populates="chart", cascade="all, delete-orphan"
    )


class DashboardChartsModel(Base):
    """
    Represents the relationship between dashboards and charts.
    """

    __tablename__ = "dashboard_chart"

    dashboard_id = Column(
        UUID(as_uuid=True), ForeignKey("dashboard.id"), primary_key=True
    )
    chart_id = Column(UUID(as_uuid=True), ForeignKey("chart.id"), primary_key=True)
    database_connection_id = Column(
        UUID(as_uuid=True),
        ForeignKey("database_connection.id", ondelete="CASCADE"),
        nullable=False,
    )

    database_connection = relationship(
        "DatabaseConnectionModel", back_populates="dashboard_charts"
    )
    dashboard = relationship("DashboardModel", back_populates="charts")
    chart = relationship("ChartModel", back_populates="dashboard_charts")


class DatabaseConnectionModel(Base):
    """
    Represents a database connection in the system.
    """

    __tablename__ = "database_connection"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    connection_name = Column(String, nullable=False)
    # NULL for connector datasources: VizAI never holds the client's database login there.
    db_connection_string = Column(Text, nullable=True)
    db_schema = Column(String, nullable=True)
    ds_graph_json = Column(Text, nullable=True)
    db_username = Column(String, nullable=True)
    db_password = Column(String, nullable=True)
    db_host_link = Column(String, nullable=True)
    db_name = Column(String, nullable=True)
    project_id = Column(UUID(as_uuid=True), ForeignKey("project.id"), nullable=False)
    db_type = Column(String, nullable=True)
    consent_given = Column(Boolean, nullable=True, default=False)
    status = Column(Boolean, nullable=True,default=False)
    last_checked = Column(DateTime, nullable=True)
    # "direct": VizAI opens the database itself (the connection string above).
    # "connector": a connector agent in the client's network runs the queries (see
    # app/connector_integration). db_type stays the real dialect in both modes.
    connection_mode = Column(String(16), nullable=False, default="direct", server_default="direct")
    connector_id = Column(
        UUID(as_uuid=True),
        ForeignKey("external_connector.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    project = relationship("ProjectModel", back_populates="database_connections")

    connection_table_names = relationship(
        "ConnectionTableNameModel",
        back_populates="connection",
        cascade="all, delete-orphan",
    )

    related_databases = relationship(
        "RelatedDatabaseModel",
        back_populates="connection",
        cascade="all, delete-orphan",
        foreign_keys="[RelatedDatabaseModel.connection_id]",
    )

    user_charts = relationship(
        "UserChartModel",
        back_populates="database_connection",
        cascade="all, delete-orphan",
    )

    chart_access_requests = relationship(
        "ChartAccessRequestModel",
        back_populates="database_connection",
        cascade="all, delete-orphan",
        foreign_keys="[ChartAccessRequestModel.database_connection_id]",
    )
    dashboard_charts = relationship(
        "DashboardChartsModel",
        back_populates="database_connection",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return (
            f"<DatabaseConnectionModel(id={self.id}, connection_name={self.connection_name}, "
            f"db_type={self.db_type})>"
        )


class ExternalConnectorModel(Base):
    """
    A connector agent that runs in a client's network and dials out to the gateway.

    VizAI keeps only a SHA-256 hash of the connector's token (shown once at creation) and
    never any database credentials. status: "offline" | "online" | "revoked".
    """

    __tablename__ = "external_connector"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    project_id = Column(UUID(as_uuid=True), ForeignKey("project.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String, nullable=False)
    token_hash = Column(String(64), nullable=False)
    status = Column(String(16), nullable=False, default="offline", server_default="offline")
    dialect = Column(String, nullable=True)
    agent_version = Column(String, nullable=True)
    protocol_version = Column(String, nullable=True)
    schema_hash = Column(String, nullable=True)
    last_seen_at = Column(DateTime, nullable=True)
    created_by = Column(UUID(as_uuid=True), ForeignKey("user.id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    revoked_at = Column(DateTime, nullable=True)

    def __repr__(self):
        return f"<ExternalConnectorModel(id={self.id}, name={self.name}, status={self.status})>"


class ConnectionTableNameModel(Base):
    """
    Represents a table name in a database connection.
    """

    __tablename__ = "connection_table_name"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    table_name = Column(String, nullable=False)
    connection_id = Column(
        UUID(as_uuid=True),
        ForeignKey("database_connection.id", ondelete="CASCADE"),
        nullable=False,
    )

    connection = relationship(
        "DatabaseConnectionModel", back_populates="connection_table_names"
    )

    # Add this relationship to connect with RoleTableNameModel
    role_table_names = relationship(
        "RoleTableNameModel",
        back_populates="connection_table_name",
        cascade="all, delete-orphan",
    )


class RoleTableNameModel(Base):
    """
    Represents a table name in a role.
    """

    __tablename__ = "role_table_name"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    role_id = Column(
        UUID(as_uuid=True), ForeignKey("role.id", ondelete="CASCADE"), nullable=False
    )
    table_name_id = Column(
        UUID(as_uuid=True),
        ForeignKey("connection_table_name.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Fix these relationships
    connection_table_name = relationship(
        "ConnectionTableNameModel", back_populates="role_table_names"
    )
    role = relationship("RoleModel", back_populates="role_table_names")


class RelatedDatabaseModel(Base):
    """
    Represents a related database in the system.
    This model connects two database connections as related.
    """

    __tablename__ = "related_database"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)

    connection_id = Column(
        UUID(as_uuid=True),
        ForeignKey("database_connection.id", ondelete="CASCADE"),
        nullable=False,
    )
    related_connection_id = Column(
        UUID(as_uuid=True),
        ForeignKey("database_connection.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Explicit relationships with foreign_keys specified
    connection = relationship(
        "DatabaseConnectionModel",
        foreign_keys=[connection_id],
        back_populates="related_databases",
    )

    related_connection = relationship(
        "DatabaseConnectionModel", foreign_keys=[related_connection_id], uselist=False
    )

    def __repr__(self):
        return (
            f"<RelatedDatabaseModel(id={self.id}, connection_id={self.connection_id},"
            f"related_connection_id={self.related_connection_id})>"
        )


class ChartAccessRequestModel(Base):
    """
    Represents a request to access a chart.
    """

    __tablename__ = "chart_access_request"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    title = Column(String, nullable=False)
    query = Column(Text, nullable=False)
    report = Column(Text, nullable=True)
    type = Column(String, nullable=False)
    relevance = Column(Double, nullable=False)
    is_time_based = Column(Boolean, nullable=True)
    chart_type = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    is_user_generated = Column(Boolean, nullable=False, default=False)
    created_by = Column(UUID(as_uuid=True), ForeignKey("user.id"), nullable=False)
    requested_by = Column(UUID(as_uuid=True), ForeignKey("user.id"), nullable=False)
    status = Column(
        SqlEnum("PENDING", "APPROVED", "REJECTED", name="access_status"), nullable=False
    )
    reviewer = Column(UUID(as_uuid=True), ForeignKey("user.id"), nullable=True)
    project_id = Column(UUID(as_uuid=True), ForeignKey("project.id"), nullable=False)
    database_connection_id = Column(
        UUID(as_uuid=True),
        ForeignKey("database_connection.id", ondelete="CASCADE"),
        nullable=False,
    )
    database_connection = relationship(
        "DatabaseConnectionModel", back_populates="chart_access_requests"
    )

    project = relationship("ProjectModel", back_populates="access_requests")

    requester = relationship(
        "UserModel",
        foreign_keys=[requested_by],
        back_populates="sent_chart_access_requests",
    )

    reviewer_user = relationship(
        "UserModel",
        foreign_keys=[reviewer],
        back_populates="reviewed_chart_access_requests",
    )


class BusinessInsightModel(Base):
    """
    Stores generated business insights per project and user.
    """

    __tablename__ = "business_insights"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    executive_summary = Column(String, nullable=False)
    key_metrics = Column(String, nullable=False)
    insights_and_patterns = Column(String, nullable=False)
    recommendations = Column(String, nullable=False)
    areas_of_concern = Column(String, nullable=False)
    project_id = Column(
        UUID(as_uuid=True), ForeignKey("project.id", ondelete="CASCADE"), nullable=False
    )
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("user.id", ondelete="CASCADE"), nullable=False
    )
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    project = relationship("ProjectModel", back_populates="business_insights")
    user = relationship("UserModel", back_populates="business_insights")


class ResponseTimeModel(Base):
    """
    Represents response time metrics for API endpoints.
    """

    __tablename__ = "response_times"

    id = Column(UUID, primary_key=True, index=True)
    url = Column(String, index=True)
    avg_process_time = Column(Double)
    last_process_time = Column(Double)
    request_count = Column(Double, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class HomeInsightModel(Base):
    """
    Stores saved insights that are pinned to the home page.
    """

    __tablename__ = "home_insights"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("user.id", ondelete="CASCADE"), nullable=False
    )
    project_id = Column(
        UUID(as_uuid=True), ForeignKey("project.id", ondelete="CASCADE"), nullable=False
    )
    title = Column(String, nullable=False)
    description = Column(Text, nullable=False)
    insight_type = Column(String, nullable=False)  # "positive", "negative", "opportunity"
    category = Column(String, nullable=False)
    impact = Column(String, nullable=False)  # "High", "Medium", "Low"
    source = Column(String, nullable=True)  # Database name or "Project-wide"
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    user = relationship("UserModel", back_populates="home_insights")
    project = relationship("ProjectModel", back_populates="home_insights")


class ShareTokenModel(Base):
    """
    Stores share tokens for embedding dashboards in external sites.
    Each token grants unauthenticated, read-only access to a specific dashboard
    when accessed via the embed route.
    """

    __tablename__ = "share_tokens"

    token_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    dashboard_id = Column(
        UUID(as_uuid=True),
        ForeignKey("dashboard.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_by = Column(
        UUID(as_uuid=True), ForeignKey("user.id"), nullable=False
    )
    hmac_signature = Column(Text, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    expires_at = Column(DateTime(timezone=True), nullable=True)
    last_accessed = Column(DateTime(timezone=True), nullable=True)
    access_count = Column(Integer, nullable=False, default=0)
    allowed_domains_snapshot = Column(ARRAY(Text), nullable=True)

    __table_args__ = (
        Index("idx_share_tokens_dashboard", "dashboard_id"),
        Index("idx_share_tokens_active", "is_active", "expires_at"),
    )

    dashboard = relationship("DashboardModel", backref="share_tokens", passive_deletes=True)
    creator = relationship("UserModel")



class AppModel(Base):
    """
    Represents a registered external application (e.g. a customer's website)
    that can embed dashboards via domain-locked iframe links.
    """

    __tablename__ = "apps"

    app_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    created_by = Column(
        UUID(as_uuid=True), ForeignKey("user.id"), nullable=False
    )
    company_name = Column(Text, nullable=False)
    domain_url = Column(Text, nullable=False)  # bare hostname, e.g. "fedex.com"
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_apps_created_by", "created_by"),
        Index("idx_apps_active", "is_active"),
    )

    creator = relationship("UserModel")



class DashboardAllowedDomainModel(Base):
    """
    Join table linking dashboards to allowed app domains.
    A dashboard can have multiple allowed domains, and each domain
    is an app registered by the user.
    """

    __tablename__ = "dashboard_allowed_domains"

    dashboard_id = Column(
        UUID(as_uuid=True),
        ForeignKey("dashboard.id", ondelete="CASCADE"),
        primary_key=True,
    )
    app_id = Column(
        UUID(as_uuid=True),
        ForeignKey("apps.app_id", ondelete="CASCADE"),
        primary_key=True,
    )
    added_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    dashboard = relationship("DashboardModel")
    app = relationship("AppModel")



class OntologyVersionModel(Base):
    """
    Stores ontology versions generated for a datasource connection.
    """

    __tablename__ = "ontology_version"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    datasource_connection_id = Column(
        UUID(as_uuid=True),
        ForeignKey("database_connection.id", ondelete="CASCADE"),
        nullable=False,
    )
    version_number = Column(Integer, nullable=False, default=1)
    version_label = Column(String, nullable=False)
    status = Column(String, nullable=False, default="draft")
    is_base = Column(Boolean, nullable=False, default=False)
    ontology_json = Column(Text, nullable=False)
    ontology_ttl = Column(Text, nullable=True)
    graph_json = Column(Text, nullable=False)
    created_by = Column(UUID(as_uuid=True), ForeignKey("user.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class OntologyEnrichmentSessionModel(Base):
    """
    Stores one enrichment Q&A session tied to an ontology version.
    """

    __tablename__ = "ontology_enrichment_session"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    datasource_connection_id = Column(
        UUID(as_uuid=True),
        ForeignKey("database_connection.id", ondelete="CASCADE"),
        nullable=False,
    )
    base_ontology_version_id = Column(
        UUID(as_uuid=True),
        ForeignKey("ontology_version.id", ondelete="CASCADE"),
        nullable=False,
    )
    status = Column(String, nullable=False, default="in_progress")
    questions_json = Column(Text, nullable=False, default="[]")
    answers_json = Column(Text, nullable=False, default="{}")
    created_by = Column(UUID(as_uuid=True), ForeignKey("user.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)


# ─── Observability Models ────────────────────────────────────────────────────

LLMTraceStatusEnum = SqlEnum("success", "error", "timeout", "cancelled", name="llm_trace_status")
AIServiceEnum = SqlEnum(
    "probe_mode", "ai_assistant", "text_enhancement", "ontology_refinement",
    "insights_generation", "query_generation", "chart_creation",
    "ontology_sync_table", "ontology_generate_table_description", "ontology_generate_column_description",
    "other",
    name="ai_service_type",
)


class LLMPricingModel(Base):
    """Provider pricing table — used to compute estimated cost per trace."""

    __tablename__ = "llm_pricing"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    provider = Column(String(50), nullable=False)
    model_name = Column(String(100), nullable=False)
    prompt_cost_per_1k = Column(Numeric(12, 8), nullable=False, default=0)
    completion_cost_per_1k = Column(Numeric(12, 8), nullable=False, default=0)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class LLMTraceModel(Base):
    """One record per LLM call. Central trace table for AI observability."""

    __tablename__ = "llm_trace"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    session_id = Column(String(256), nullable=True, index=True)
    chart_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey("project.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey("user.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    ai_service = Column(AIServiceEnum, nullable=False, default="probe_mode")
    llm_provider = Column(String(50), nullable=True)
    model_name = Column(String(100), nullable=True)
    prompt_tokens = Column(Integer, nullable=True, default=0)
    completion_tokens = Column(Integer, nullable=True, default=0)
    total_tokens = Column(Integer, nullable=True, default=0)
    estimated_cost_usd = Column(Numeric(12, 8), nullable=True, default=0)
    latency_ms = Column(Integer, nullable=True)
    prompt_text = Column(Text, nullable=True)
    completion_text = Column(Text, nullable=True)
    sql_generated = Column(Text, nullable=True)
    sql_retries = Column(Integer, nullable=True, default=0)
    schema_tables_used = Column(JSONB, nullable=True)
    agent_steps = Column(JSONB, nullable=True)
    status = Column(LLMTraceStatusEnum, nullable=False, default="success")
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), index=True)

    project = relationship("ProjectModel", foreign_keys=[project_id])
    user = relationship("UserModel", foreign_keys=[user_id])


class DailyUsageRollupModel(Base):
    """Hourly-aggregated token and cost roll-up per project / user / service."""

    __tablename__ = "daily_usage_rollup"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    rollup_date = Column(Date, nullable=False, index=True)
    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey("project.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    user_id = Column(
        UUID(as_uuid=True),
        ForeignKey("user.id", ondelete="SET NULL"),
        nullable=True,
    )
    ai_service = Column(AIServiceEnum, nullable=False)
    model_name = Column(String(100), nullable=True)
    total_calls = Column(Integer, nullable=False, default=0)
    total_prompt_tokens = Column(BigInteger, nullable=False, default=0)
    total_completion_tokens = Column(BigInteger, nullable=False, default=0)
    total_cost_usd = Column(Numeric(14, 8), nullable=False, default=0)
    error_count = Column(Integer, nullable=False, default=0)
    avg_latency_ms = Column(Integer, nullable=True)
    computed_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    project = relationship("ProjectModel", foreign_keys=[project_id])
    user = relationship("UserModel", foreign_keys=[user_id])
