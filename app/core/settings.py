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
