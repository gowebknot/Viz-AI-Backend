"""add external_connector and connector mode on database_connection

Additive. Existing datasources become connection_mode='direct' and behave exactly as before.
A connector datasource (connection_mode='connector') has no connection string: the client's
database login stays in the client's network, with the connector agent.

Revision ID: b8d4f2a6c1e3
Revises: f1a2b3c4d5e6
Create Date: 2026-11-03 17:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "b8d4f2a6c1e3"
down_revision: Union[str, None] = "f1a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "external_connector",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "project_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("project.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="offline"),
        sa.Column("dialect", sa.String(), nullable=True),
        sa.Column("agent_version", sa.String(), nullable=True),
        sa.Column("protocol_version", sa.String(), nullable=True),
        sa.Column("schema_hash", sa.String(), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(), nullable=True),
        sa.Column(
            "created_by",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("user.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("revoked_at", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_external_connector_project_id", "external_connector", ["project_id"])

    op.add_column(
        "database_connection",
        sa.Column("connection_mode", sa.String(length=16), nullable=False, server_default="direct"),
    )
    op.add_column(
        "database_connection",
        sa.Column(
            "connector_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("external_connector.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_index("ix_database_connection_connector_id", "database_connection", ["connector_id"])
    op.alter_column("database_connection", "db_connection_string", existing_type=sa.Text(), nullable=True)


def downgrade() -> None:
    bind = op.get_bind()
    connector_rows = bind.execute(
        sa.text("SELECT count(*) FROM database_connection WHERE db_connection_string IS NULL")
    ).scalar()
    if connector_rows:
        # Connector datasources have no connection string, so the column cannot become NOT NULL
        # again. Deleting them silently would destroy datasources; make the operator do it.
        raise RuntimeError(
            f"{connector_rows} datasource(s) have no connection string (connector datasources). "
            "Delete them in VizAI first, then downgrade."
        )
    op.alter_column("database_connection", "db_connection_string", existing_type=sa.Text(), nullable=False)
    op.drop_index("ix_database_connection_connector_id", table_name="database_connection")
    op.drop_column("database_connection", "connector_id")
    op.drop_column("database_connection", "connection_mode")
    op.drop_index("ix_external_connector_project_id", table_name="external_connector")
    op.drop_table("external_connector")
