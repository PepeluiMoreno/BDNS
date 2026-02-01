"""
Configuracion de sesion de base de datos para BDNS.

Este modulo es un wrapper de compatibilidad que reutiliza bdns_core.db.manager.
Mantiene la API existente para no romper codigo que ya usa get_session().
"""

import sys
from pathlib import Path

# Agregar bdns_core al path si no esta
bdns_root = Path(__file__).parent.parent.parent
if str(bdns_root) not in sys.path:
    sys.path.insert(0, str(bdns_root))

from bdns_core.db.manager import (
    sync_db_manager,
    db_manager,
    DatabaseConfig,
)

# Re-exportar para compatibilidad con codigo existente
engine = sync_db_manager.engine
SessionLocal = sync_db_manager.session_maker


def get_session():
    """
    Context manager para obtener una sesion sincrona.

    Uso:
        with get_session() as session:
            session.query(...)
    """
    return sync_db_manager.session()


def get_session_direct():
    """
    Obtiene una sesion directamente (sin context manager).
    NOTA: Recuerda cerrar la sesion manualmente.
    """
    return sync_db_manager.get_session()


def get_async_session():
    """
    Context manager para obtener una sesion asincrona.

    Uso:
        async with get_async_session() as session:
            await session.execute(...)
    """
    return db_manager.session()


# Exportaciones
__all__ = [
    "engine",
    "SessionLocal",
    "get_session",
    "get_session_direct",
    "get_async_session",
    "sync_db_manager",
    "db_manager",
    "DatabaseConfig",
]
