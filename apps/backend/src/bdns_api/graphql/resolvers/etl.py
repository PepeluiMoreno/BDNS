"""
Resolvers para ejecutar y monitorear el ETL desde GraphQL.
"""

import asyncio
import subprocess
import os
import sys
from datetime import datetime
from typing import List, Optional
from pathlib import Path

from bdns_api.graphql.types.etl import (
    ETLExecution,
    ETLStepResult,
    ETLStatus,
    ETLStep,
    ETLConfig,
)

# Rutas del proyecto
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
ETL_DIR = PROJECT_ROOT / "ETL"

# Almacenamiento en memoria de ejecuciones (en producción usar base de datos)
_executions: dict = {}
_execution_counter = 0


async def get_etl_config() -> ETLConfig:
    """Obtiene la configuración disponible del ETL."""
    current_year = datetime.now().year
    
    # Pasos disponibles del ETL
    steps = [
        ETLStep(
            name="extract_convocatorias",
            module="convocatorias/extract/extract_convocatorias.py",
            description="Extraer convocatorias desde API BDNS",
            enabled=True,
            depends_on=["load_catalogos"],
        ),
        ETLStep(
            name="extract_concesiones",
            module="concesiones/extract/extract_concesiones.py",
            description="Extraer concesiones desde API BDNS",
            enabled=True,
            depends_on=["load_catalogos"],
        ),
        ETLStep(
            name="load_catalogos",
            module="catalogos/load/load_all_catalogos.py",
            description="Cargar catálogos base",
            enabled=True,
            depends_on=[],
        ),
    ]
    
    return ETLConfig(
        available_steps=steps,
        min_year=2015,
        max_year=current_year,
        current_year=current_year,
    )


async def launch_etl_update(
    year: int,
    month: Optional[int] = None,
    only_steps: Optional[List[str]] = None,
    skip_steps: Optional[List[str]] = None,
) -> ETLExecution:
    """
    Lanza una actualización del ETL para un año y mes específico.
    
    Args:
        year: Año a actualizar
        month: Mes opcional (para sincronización mensual)
        only_steps: Lista de pasos a ejecutar (si está vacío, ejecuta todos)
        skip_steps: Lista de pasos a saltar
    
    Returns:
        ETLExecution con información de la ejecución
    """
    global _execution_counter
    _execution_counter += 1
    
    execution_id = str(_execution_counter)
    started_at = datetime.now()
    
    # Crear ejecución inicial
    execution = ETLExecution(
        id=execution_id,
        year=year,
        month=month,
        status=ETLStatus.RUNNING,
        started_at=started_at,
        completed_at=None,
        total_duration=None,
        steps_executed=0,
        steps_total=0,
        steps_failed=0,
        error_message=None,
        created_by="admin",  # En producción obtener del usuario autenticado
        steps_results=[],
    )
    
    _executions[execution_id] = execution
    
    # Ejecutar ETL en background
    asyncio.create_task(_run_etl_background(execution_id, year, month, only_steps, skip_steps))
    
    return execution


async def _run_etl_background(
    execution_id: str,
    year: int,
    month: Optional[int],
    only_steps: Optional[List[str]],
    skip_steps: Optional[List[str]],
) -> None:
    """Ejecuta el ETL en background."""
    try:
        execution = _executions[execution_id]
        
        # Construir comando
        cmd = [
            sys.executable,
            "-m",
            "ETL.run_etl",
            "--year",
            str(year),
        ]
        
        if month:
            cmd.extend(["--month", str(month)])
        
        if only_steps:
            cmd.extend(["--only", ",".join(only_steps)])
        
        if skip_steps:
            cmd.extend(["--skip", ",".join(skip_steps)])
        
        # Preparar entorno
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PROJECT_ROOT)
        
        # Ejecutar ETL
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(ETL_DIR.parent),
            env=env,
        )
        
        stdout, stderr = await process.communicate()
        
        # Procesar resultados
        if process.returncode == 0:
            execution.status = ETLStatus.SUCCESS
            execution.steps_executed = 1  # Simplificado
            execution.steps_total = 1
            execution.steps_failed = 0
        else:
            execution.status = ETLStatus.FAILED
            execution.steps_failed = 1
            execution.error_message = stderr.decode() if stderr else "Error desconocido"
        
        execution.completed_at = datetime.now()
        execution.total_duration = (
            execution.completed_at - execution.started_at
        ).total_seconds()
        
    except Exception as e:
        execution = _executions[execution_id]
        execution.status = ETLStatus.FAILED
        execution.error_message = str(e)
        execution.completed_at = datetime.now()
        execution.total_duration = (
            execution.completed_at - execution.started_at
        ).total_seconds()


async def get_etl_execution(execution_id: str) -> Optional[ETLExecution]:
    """Obtiene información de una ejecución del ETL."""
    return _executions.get(execution_id)


async def get_etl_executions(
    limit: int = 100,
    offset: int = 0,
) -> List[ETLExecution]:
    """Obtiene lista de ejecuciones del ETL."""
    executions = list(_executions.values())
    # Ordenar por fecha de inicio descendente
    executions.sort(key=lambda e: e.started_at, reverse=True)
    return executions[offset : offset + limit]


async def cancel_etl_execution(execution_id: str) -> bool:
    """Cancela una ejecución del ETL."""
    execution = _executions.get(execution_id)
    if execution and execution.status == ETLStatus.RUNNING:
        execution.status = ETLStatus.CANCELLED
        execution.completed_at = datetime.now()
        execution.total_duration = (
            execution.completed_at - execution.started_at
        ).total_seconds()
        return True
    return False


