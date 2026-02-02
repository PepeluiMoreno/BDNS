"""
Configuracion de la aplicacion BDNS.

Carga settings desde variables de entorno o .env
"""
import os
from functools import lru_cache
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Settings de la aplicacion."""

    # Database
    DATABASE_URL: str = "postgresql://bdns_etl:bdns_etl_2024@localhost:5432/bdns"

    # Redis (opcional, para cache)
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = None

    # GraphQL
    GRAPHQL_URL: str = "http://localhost:8000/graphql"

    # Telegram
    TELEGRAM_BOT_TOKEN: Optional[str] = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """Obtiene settings cacheados."""
    return Settings()
