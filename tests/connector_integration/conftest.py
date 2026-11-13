"""Fixtures for the connector integration tests.

The pure tests (schema mapper, tokens) run anywhere. The tests that use `db` write to a
database, so they REFUSE to run without VIZAI_TEST_DATABASE_URL (a throwaway PostgreSQL with the
pgvector extension, e.g. the pgvector/pgvector:pg16 image) and point the app at it. They never
use the DB_URI from .env, and they TRUNCATE tables in that database.
"""

from __future__ import annotations

import base64
import os
import sys
from pathlib import Path

import pytest

TEST_DB = os.environ.get("VIZAI_TEST_DATABASE_URL")
if TEST_DB:
    # Settings are read when app.core.settings is first imported: override before that.
    os.environ["DB_URI"] = TEST_DB
for _key, _value in {
    "SECRET_KEY": "test-secret",
    "REFRESH_SECRET_KEY": "test-refresh",
    "ALGORITHM": "HS256",
    "ACCESS_TOKEN_EXPIRE_MINUTES": "30",
    "REFRESH_TOKEN_EXPIRE_DAYS": "7",
    "GEMINI_API_KEY": "fake",
    "OPENAI_API_KEY": "fake",
    "ENCRYPTION_KEY": base64.urlsafe_b64encode(os.urandom(32)).decode(),
}.items():
    os.environ.setdefault(_key, _value)

# The gateway package may not be installed: use the sibling source tree.
_gateway_src = Path(__file__).resolve().parents[3] / "vizai-connector" / "gateway"
if _gateway_src.is_dir() and str(_gateway_src) not in sys.path:
    sys.path.append(str(_gateway_src))


def _require_test_db() -> None:
    if not TEST_DB:
        pytest.skip("set VIZAI_TEST_DATABASE_URL to a throwaway PostgreSQL to run the database tests")


@pytest.fixture(scope="session")
def _schema():
    _require_test_db()
    from sqlalchemy import text

    import app.models.knowledge_graph_models  # noqa: F401
    import app.models.schema_models  # noqa: F401
    from app.core.base import Base
    from app.core.db import engine

    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
    Base.metadata.create_all(engine)


@pytest.fixture
def db(_schema):
    from sqlalchemy import text

    from app.core.db import SessionLocal

    session = SessionLocal()
    session.execute(text('TRUNCATE external_connector, database_connection, project, "user" RESTART IDENTITY CASCADE'))
    session.commit()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def user(db):
    from app.models.schema_models import UserModel

    row = UserModel(username="admin", password="x", email="admin@example.com", is_super=True)
    db.add(row)
    db.commit()
    return row


@pytest.fixture
def project(db, user):
    from app.models.schema_models import ProjectModel

    row = ProjectModel(name="Acme", super_user_id=user.id, primary_domain="retail")
    db.add(row)
    db.commit()
    return row
