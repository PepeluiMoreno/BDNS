"""
Manager de conexiones a base de datos para BDNS.

Basado en sipi_core/db/sessions/manager.py pero adaptado para BDNS:
- Sin schemas multiples (usa public por defecto)
- Configuracion especifica de BDNS
"""

from typing import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool, AsyncAdaptedQueuePool
from pathlib import Path
import os

from dotenv import load_dotenv

# Cargar variables de entorno
env_dev = Path(__file__).parent.parent.parent / ".env.development"
env_file = Path(__file__).parent.parent.parent / ".env"

if env_dev.exists():
    load_dotenv(env_dev)
else:
    load_dotenv(env_file)


class DatabaseConfig:
    """Configuracion de conexion a base de datos para BDNS."""

    @staticmethod
    def get_database_url(async_mode: bool = False) -> str:
        """
        Obtiene la URL de la base de datos desde variables de entorno.

        Prioridad:
        1. DATABASE_URL_ASYNC (si async_mode=True)
        2. DATABASE_URL
        3. Construccion desde variables individuales
        """
        if async_mode:
            url = os.getenv("DATABASE_URL_ASYNC")
            if url:
                return url

        database_url = os.getenv("DATABASE_URL")

        if not database_url:
            user = os.getenv("DB_USER_LOCAL", "postgres")
            password = os.getenv("DB_PASSWORD_LOCAL", "postgres")
            host = os.getenv("DB_HOST_LOCAL", "localhost")
            port = os.getenv("DB_PORT_LOCAL", "5432")
            db = os.getenv("DB_NAME_LOCAL", "bdns")
            database_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"

        # Normalizar URL
        database_url = database_url.replace("postgresql+asyncpg://", "postgresql://")
        database_url = database_url.replace("postgresql+psycopg2://", "postgresql://")

        if async_mode:
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://")
        else:
            database_url = database_url.replace("postgresql://", "postgresql+psycopg2://")

        return database_url

    @staticmethod
    def get_pool_config() -> dict:
        """Obtiene configuracion del pool de conexiones."""
        return {
            "pool_size": int(os.getenv("POOL_SIZE", "20")),
            "max_overflow": int(os.getenv("POOL_MAX_OVERFLOW", "10")),
            "pool_timeout": int(os.getenv("POOL_TIMEOUT", "30")),
            "pool_recycle": 1800,  # 30 minutos
        }

    @staticmethod
    def get_echo() -> bool:
        """Obtiene si se debe hacer echo de SQL."""
        return os.getenv("SQLALCHEMY_ECHO", "false").lower() == "true"


class SyncDatabaseManager:
    """Manager para conexiones sincronas a la base de datos."""

    def __init__(self, database_url: str = None, echo: bool = None, **pool_kwargs):
        self.database_url = database_url or DatabaseConfig.get_database_url(async_mode=False)

        pool_config = DatabaseConfig.get_pool_config()
        pool_config.update(pool_kwargs)

        self.engine = create_engine(
            self.database_url,
            poolclass=QueuePool,
            echo=echo if echo is not None else DatabaseConfig.get_echo(),
            **pool_config
        )

        self.session_maker = sessionmaker(
            self.engine,
            class_=Session,
            expire_on_commit=False
        )

    def close(self):
        """Cierra el motor y libera conexiones."""
        self.engine.dispose()

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """
        Context manager para obtener una sesion sincrona.

        Uso:
            with sync_db_manager.session() as session:
                session.query(...)
        """
        session = self.session_maker()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_session(self) -> Session:
        """
        Obtiene una sesion directamente (sin context manager).
        NOTA: Recuerda cerrar la sesion manualmente.
        """
        return self.session_maker()


class AsyncDatabaseManager:
    """Manager para conexiones asincronas a la base de datos."""

    def __init__(self, database_url: str = None, echo: bool = None, **pool_kwargs):
        self.database_url = database_url or DatabaseConfig.get_database_url(async_mode=True)

        pool_config = DatabaseConfig.get_pool_config()
        pool_config.update(pool_kwargs)

        self.engine = create_async_engine(
            self.database_url,
            poolclass=AsyncAdaptedQueuePool,
            echo=echo if echo is not None else DatabaseConfig.get_echo(),
            **pool_config
        )

        self.session_maker = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

    async def close(self):
        """Cierra el motor y libera conexiones."""
        await self.engine.dispose()

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Context manager para obtener una sesion asincrona.

        Uso:
            async with db_manager.session() as session:
                await session.execute(...)
        """
        async with self.session_maker() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    async def get_session(self) -> AsyncSession:
        """
        Obtiene una sesion directamente (sin context manager).
        NOTA: Recuerda cerrar la sesion manualmente.
        """
        return self.session_maker()


def create_sync_manager(echo: bool = None) -> SyncDatabaseManager:
    """Crea un manager de conexiones sincronas."""
    return SyncDatabaseManager(echo=echo)


def create_async_manager(echo: bool = None) -> AsyncDatabaseManager:
    """Crea un manager de conexiones asincronas."""
    return AsyncDatabaseManager(echo=echo)


# Instancias globales
db_manager = create_async_manager()
sync_db_manager = create_sync_manager()
