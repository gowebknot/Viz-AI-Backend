from contextlib import asynccontextmanager
import logging
import os
import sys

from fastapi import FastAPI, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.middleware.response_time import ResponseTimeMiddleware
from app.middleware.request_logging import RequestResponseLoggingMiddleware

# Import routers and constants
from app.routes.auth import auth_router
from app.routes.backend import backend_router
from app.routes.llm import llm_router
from app.routes.embed import share_token_router, embed_router
from app.routes.apps import apps_router
from app.routes.allowed_domains import allowed_domains_router
from app.routes.observability import observability_router
from app.routes.ontology import router as ontology_router
from app.routes.knowledge_graph import knowledge_graph_router
from app.services.knowledge_graph import neo4j_keepalive
from app.connector_integration.gateway_host import gateway_enabled, start_gateway, stop_gateway
from app.utils.constants import (
    ALLOWED_ORIGINS,
    ALLOWED_CREDENTIALS,
    ALLOWED_METHODS,
    ALLOWED_HEADERS,
)
from sqlalchemy.orm import Session

# Configure logging - set to INFO by default, can be overridden with LOG_LEVEL env var
log_level_name = os.getenv('LOG_LEVEL', 'INFO').upper()
log_level = getattr(logging, log_level_name, logging.INFO)

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Set SQLAlchemy engine logging to WARNING to reduce noise
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager for startup and shutdown events.

    Handles:
    - Graceful shutdown of external database engine pool
    """
    # Startup
    logger.info("Starting VizAI Backend...")
    neo4j_keepalive.start()
    await start_gateway()  # no-op unless CONNECTOR_GATEWAY_ENABLED=true
    yield
    # Shutdown
    logger.info("Shutting down VizAI Backend...")
    await stop_gateway()
    await neo4j_keepalive.stop()
    from app.core.db import external_engine_manager, engine
    external_engine_manager.dispose_all()
    logger.info("All external engines disposed successfully")
    engine.dispose()
    logger.info("Main database engine disposed successfully")
    from app.services.knowledge_graph.neo4j_client import close_driver
    await close_driver()
    logger.info("Neo4j AuraDB driver closed successfully")


# For SQLAlchemy session, if used elsewhere
# Create a single FastAPI app instance with lifespan
app = FastAPI(lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=ALLOWED_CREDENTIALS,
    allow_methods=ALLOWED_METHODS,
    allow_headers=ALLOWED_HEADERS,
)


# Include routers for your endpoints
app.include_router(auth_router)  # Include auth-related endpoints
app.include_router(backend_router)
app.include_router(llm_router)  # Include backend-related endpoints
app.include_router(share_token_router)  # Include share token management endpoints
app.include_router(embed_router)  # Include public embed endpoints
app.include_router(apps_router)  # Include app registration endpoints
app.include_router(allowed_domains_router)  # Include allowed domains endpoints
app.include_router(observability_router)    # Include observability endpoints
app.include_router(ontology_router, prefix="/api/v1/ontology", tags=["Data Ontology Explorer"])
app.include_router(knowledge_graph_router)  # PDF knowledge graphs
if gateway_enabled():
    from app.connector_integration.routes import connector_router

    app.include_router(connector_router)  # client connector onboarding

# Embedded dashboard ECharts bundle (built via VIZ-AI-FRONTEND `npm run build:embed`)
_embed_static_dir = os.path.join(os.path.dirname(__file__), "static", "embed")
if os.path.isdir(_embed_static_dir):
    app.mount(
        "/api/v1/embed/assets",
        StaticFiles(directory=_embed_static_dir),
        name="embed_assets",
    )

app.add_middleware(RequestResponseLoggingMiddleware)
# app.add_middleware(ResponseTimeMiddleware)


@app.get("/")
async def root():
    return {"message": "hello"}

@app.websocket("/ws/test")
async def ws_test(ws: WebSocket):
    await ws.accept()
    await ws.send_text("Hello WS!")
    await ws.close()
