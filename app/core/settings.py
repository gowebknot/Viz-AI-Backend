"""
This module contains the settings for the application.
"""

from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    This class contains the settings for the application.
    """

    DB_URI: str
    SECRET_KEY: str
    REFRESH_SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int
    REFRESH_TOKEN_EXPIRE_DAYS: int
    # LLM_URI: str
    ENCRYPTION_KEY: str
    GEMINI_API_KEY: str
    # Keep both for backward compatibility: some modules currently reference OPENAPI_API_KEY.
    OPENAI_API_KEY: Optional[str] = None
    OPENAPI_API_KEY: Optional[str] = None
    REDIS_URL: Optional[str] = None
    EMBED_SERVER_SECRET: Optional[str] = None  # Used for HMAC-signing embed tokens; falls back to SECRET_KEY
    EMBED_JWT_EXPIRE_MINUTES: int = 30  # Stateless embed session JWT lifetime

    # Connector flow (app/connector_integration). Off by default: nothing below has any effect
    # until CONNECTOR_GATEWAY_ENABLED=true. The gateway keeps its sessions in this process, so
    # run ONE uvicorn worker while it is on. Other gateway tunables (rate limits, metrics) are
    # read from the process environment as CONNECTOR_GATEWAY_* (see the gateway README).
    CONNECTOR_GATEWAY_ENABLED: bool = False
    CONNECTOR_GATEWAY_HOST: Optional[str] = None
    CONNECTOR_GATEWAY_PORT: Optional[int] = None
    CONNECTOR_GATEWAY_TLS_CERT_FILE: Optional[str] = None
    CONNECTOR_GATEWAY_TLS_KEY_FILE: Optional[str] = None
    CONNECTOR_GATEWAY_REQUIRE_TLS: Optional[bool] = None
    CONNECTOR_GATEWAY_METRICS_PORT: Optional[int] = None
    # The address clients put in their connector settings (what they dial), e.g. connect.vizai.ai:443
    CONNECTOR_PUBLIC_GATEWAY_ADDRESS: Optional[str] = None
    # What the install screen tells a client to run (used to build the settings shown in the UI).
    # CONNECTOR_IMAGE: the connector image without a tag; CONNECTOR_RELEASE_VERSION is the tag.
    # CONNECTOR_INSTALL_ENV: extra VIZAI_* settings for the client's command, comma-separated, for
    # local testing only, e.g. VIZAI_TLS_CA_FILE=/path/to/ca.pem
    CONNECTOR_DOWNLOAD_URL: str = "https://downloads.vizai.ai/connector"
    CONNECTOR_IMAGE: str = "vizai/connector"
    CONNECTOR_RELEASE_VERSION: str = "0.7.0"
    CONNECTOR_INSTALL_ENV: Optional[str] = None
    # Most rows one connector query may return (the agent has its own cap, VIZAI_MAX_ROWS; the lower
    # one wins). A result over the cap is returned cut off, with "truncated": true. 0 = gateway default.
    CONNECTOR_QUERY_MAX_ROWS: int = 50000
    # RAG (pgvector) — document chunks for SQL generation
    EMBEDDING_MODEL: str = "text-embedding-3-small"
    VECTOR_DIM: int = 1536
    RAG_TOP_K: int = 5

    class Config:
        """
        This class contains the configuration for the settings.
        """

        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings(_env_file=".env", _env_file_encoding="utf-8")
