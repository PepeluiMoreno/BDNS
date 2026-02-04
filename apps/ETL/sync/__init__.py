# ETL/sync/__init__.py
"""
Modulo de sincronizacion mensual con BDNS.

Detecta cambios en concesiones de los ultimos N meses (default: 48)
y aplica los cambios (INSERT/UPDATE/DELETE) a la BD local.
"""
