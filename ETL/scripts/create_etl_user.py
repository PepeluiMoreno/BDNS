#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Crear usuario ETL del sistema.

Este script crea el usuario 'etl_system' que se usa para auditar
todos los registros creados por el proceso ETL.

Uso:
    python ETL/scripts/create_etl_user.py
"""

import sys
import uuid
from pathlib import Path

# Agregar el directorio raiz al path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.db.session import get_session
from app.db.models import ETLUser

ETL_USERNAME = "etl_system"
ETL_DESCRIPTION = "Usuario del sistema para procesos ETL automatizados"


def main():
    """Crear usuario ETL si no existe."""
    print(f"Verificando usuario ETL: {ETL_USERNAME}")

    with get_session() as session:
        # Verificar si ya existe
        existing = session.query(ETLUser).filter_by(nombre=ETL_USERNAME).first()

        if existing:
            print(f"[OK] Usuario ETL ya existe:")
            print(f"     ID: {existing.id}")
            print(f"     Nombre: {existing.nombre}")
            print(f"     Creado: {existing.created_at}")
            return 0

        # Crear nuevo usuario
        user = ETLUser(
            id=str(uuid.uuid4()),
            nombre=ETL_USERNAME
        )
        session.add(user)
        session.commit()

        print(f"[OK] Usuario ETL creado:")
        print(f"     ID: {user.id}")
        print(f"     Nombre: {user.nombre}")
        return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except Exception as e:
        print(f"[ERROR] No se pudo crear usuario ETL: {e}")
        sys.exit(1)