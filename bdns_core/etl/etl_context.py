# bdns_core/etl/etl_context.py
"""
Contexto ETL para BDNS.

Proporciona el nombre del usuario/proceso ETL del sistema.
A diferencia de sipi, no usa UUID ni FK - solo un nombre string.
"""

ETL_USERNAME = "etl_system"


def get_etl_user_name() -> str:
    """
    Retorna el nombre del usuario ETL del sistema.

    Este nombre se usa para el campo created_by/updated_by en los registros.

    Returns:
        str: Nombre del usuario ETL ("etl_system")
    """
    return ETL_USERNAME
