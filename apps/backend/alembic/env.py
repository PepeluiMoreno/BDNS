from logging.config import fileConfig
import sys
from pathlib import Path
from dotenv import load_dotenv
from os import environ
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import URL

from alembic import context

# root real del proyecto
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# archivo .env principal
load_dotenv(PROJECT_ROOT / ".env")

# archivo específico del entorno
environment = environ.get("ENVIRONMENT", "development")
env_file = PROJECT_ROOT / f".env.{environment}"

# cargar variables específicas del entorno
load_dotenv(env_file, override=True)


# -----------------------------
# Importar modelos
# -----------------------------
from bdns_core.db.models import Base  # tu Base de SQLAlchemy

# -----------------------------
# Alembic Config object
# -----------------------------
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

# -----------------------------
# URL de base de datos
# -----------------------------
DATABASE_URL = environ["DATABASE_URL"]  # url síncrona para Alembic

# -----------------------------
# Modo offline
# -----------------------------
def run_migrations_offline() -> None:
    context.configure(
        url=DATABASE_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


# -----------------------------
# Modo online
# -----------------------------
def run_migrations_online() -> None:
    connectable = engine_from_config(
        {},
        url=DATABASE_URL,
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()