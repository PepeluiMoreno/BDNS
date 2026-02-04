# bdns_core/mixins/audit.py
"""
Mixin de auditoria simplificado para BDNS.

A diferencia de sipi_core, no usa FK a tabla de usuarios.
Solo registra timestamps y nombre del proceso que crea/modifica.
"""

from datetime import datetime
from sqlalchemy import Column, DateTime, String


class AuditMixin:
    """
    Mixin de auditoria simplificado.

    Campos:
        created_at: Fecha de creacion
        updated_at: Fecha de ultima actualizacion
        created_by: Nombre del proceso/usuario que creo el registro
        updated_by: Nombre del proceso/usuario que actualizo el registro
    """

    created_at = Column(
        DateTime,
        default=datetime.utcnow,
        nullable=False,
        index=True,
        comment="Fecha de creacion del registro"
    )

    updated_at = Column(
        DateTime,
        onupdate=datetime.utcnow,
        nullable=True,
        comment="Fecha de ultima actualizacion"
    )

    created_by = Column(
        String(50),
        nullable=True,
        comment="Proceso o usuario que creo el registro"
    )

    updated_by = Column(
        String(50),
        nullable=True,
        comment="Proceso o usuario que actualizo el registro"
    )
