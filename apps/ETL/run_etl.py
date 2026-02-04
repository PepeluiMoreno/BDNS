#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CLI principal para ejecutar la ETL de BDNS.

Uso:
    python -m ETL.run_etl --year 2024
    python -m ETL.run_etl --year 2024 --dry-run
    python -m ETL.run_etl --year 2024 --only load_catalogos,extract_concesiones
    python -m ETL.run_etl --list

Opciones:
    --year YEAR       Ano a procesar (requerido excepto con --list)
    --dry-run         Solo muestra los pasos sin ejecutar
    --only STEPS      Solo ejecutar pasos especificos (separados por coma)
    --skip STEPS      Saltar pasos especificos (separados por coma)
    --list            Listar todos los pasos disponibles
    --config PATH     Ruta al archivo de configuracion YAML
"""

import argparse
import os
import subprocess
import sys
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from enum import Enum

# Agregar raiz del proyecto al path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Preparar entorno para subprocesos
SUBPROCESS_ENV = os.environ.copy()
SUBPROCESS_ENV["PYTHONPATH"] = str(PROJECT_ROOT)


class OnError(Enum):
    """Accion en caso de error."""
    STOP = "STOP"
    CONTINUE = "CONTINUE"
    SKIP_DEPENDENTS = "SKIP_DEPENDENTS"


@dataclass
class ETLStep:
    """Definicion de un paso ETL."""
    name: str
    module: str
    description: str
    depends_on: List[str]
    on_error: OnError
    enabled: bool
    args: List[str]


@dataclass
class StepResult:
    """Resultado de ejecucion de un paso."""
    name: str
    status: str  # "SUCCESS", "FAILED", "SKIPPED"
    duration: float
    message: str = ""


def load_config(config_path: Path) -> List[ETLStep]:
    """Carga la configuracion desde YAML."""
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    steps = []
    for step_data in config.get("steps", []):
        steps.append(ETLStep(
            name=step_data["name"],
            module=step_data["module"],
            description=step_data.get("description", ""),
            depends_on=step_data.get("depends_on", []),
            on_error=OnError(step_data.get("on_error", "STOP")),
            enabled=step_data.get("enabled", True),
            args=step_data.get("args", []),
        ))

    return steps


def resolve_args(args: List[str], variables: Dict[str, str]) -> List[str]:
    """Resuelve variables en los argumentos."""
    resolved = []
    for arg in args:
        for key, value in variables.items():
            arg = arg.replace(f"{{{key}}}", str(value))
        resolved.append(arg)
    return resolved


def get_execution_order(steps: List[ETLStep], only: Optional[Set[str]] = None, skip: Optional[Set[str]] = None) -> List[ETLStep]:
    """
    Determina el orden de ejecucion basado en dependencias.

    Usa ordenamiento topologico para respetar dependencias.
    """
    # Filtrar pasos
    step_map = {s.name: s for s in steps}
    enabled_steps = [s for s in steps if s.enabled]

    if only:
        # Solo ejecutar pasos especificados y sus dependencias
        to_include = set()
        for name in only:
            if name in step_map:
                to_include.add(name)
                # Agregar dependencias recursivamente
                queue = list(step_map[name].depends_on)
                while queue:
                    dep = queue.pop(0)
                    if dep in step_map and dep not in to_include:
                        to_include.add(dep)
                        queue.extend(step_map[dep].depends_on)
        enabled_steps = [s for s in enabled_steps if s.name in to_include]

    if skip:
        enabled_steps = [s for s in enabled_steps if s.name not in skip]

    # Ordenamiento topologico
    in_degree = {s.name: 0 for s in enabled_steps}
    enabled_names = {s.name for s in enabled_steps}

    for step in enabled_steps:
        for dep in step.depends_on:
            if dep in enabled_names:
                in_degree[step.name] += 1

    queue = [s for s in enabled_steps if in_degree[s.name] == 0]
    result = []

    while queue:
        step = queue.pop(0)
        result.append(step)

        for other in enabled_steps:
            if step.name in other.depends_on:
                in_degree[other.name] -= 1
                if in_degree[other.name] == 0:
                    queue.append(other)

    if len(result) != len(enabled_steps):
        raise ValueError("Dependencias ciclicas detectadas en la configuracion")

    return result


def run_step(step: ETLStep, variables: Dict[str, str], dry_run: bool = False) -> StepResult:
    """Ejecuta un paso ETL."""
    start_time = datetime.now()

    # Construir comando
    module_path = PROJECT_ROOT / "ETL" / step.module
    args = resolve_args(step.args, variables)
    cmd = [sys.executable, str(module_path)] + args

    print(f"\n{'='*60}")
    print(f"PASO: {step.name}")
    print(f"{'='*60}")
    print(f"Descripcion: {step.description}")
    print(f"Modulo: {step.module}")
    print(f"Comando: {' '.join(cmd)}")

    if dry_run:
        print("[DRY-RUN] No se ejecuta")
        return StepResult(
            name=step.name,
            status="SKIPPED",
            duration=0,
            message="Dry run"
        )

    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            env=SUBPROCESS_ENV,
            capture_output=False,
            text=True,
        )

        duration = (datetime.now() - start_time).total_seconds()

        if result.returncode == 0:
            print(f"\n[OK] {step.name} completado en {duration:.1f}s")
            return StepResult(
                name=step.name,
                status="SUCCESS",
                duration=duration,
            )
        else:
            print(f"\n[ERROR] {step.name} fallo con codigo {result.returncode}")
            return StepResult(
                name=step.name,
                status="FAILED",
                duration=duration,
                message=f"Exit code: {result.returncode}"
            )

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        print(f"\n[EXCEPTION] {step.name}: {e}")
        return StepResult(
            name=step.name,
            status="FAILED",
            duration=duration,
            message=str(e)
        )


def list_steps(steps: List[ETLStep]):
    """Muestra todos los pasos disponibles."""
    print("\nPasos ETL disponibles:")
    print("=" * 80)
    print(f"{'Nombre':<25} {'Habilitado':<12} {'Descripcion'}")
    print("-" * 80)

    for step in steps:
        enabled = "Si" if step.enabled else "No"
        deps = f" (deps: {', '.join(step.depends_on)})" if step.depends_on else ""
        print(f"{step.name:<25} {enabled:<12} {step.description}{deps}")


def main():
    parser = argparse.ArgumentParser(
        description="CLI para ejecutar ETL de BDNS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--year", type=int, help="Ano a procesar")
    parser.add_argument("--dry-run", action="store_true", help="Solo mostrar pasos sin ejecutar")
    parser.add_argument("--only", type=str, help="Solo ejecutar pasos especificos (separados por coma)")
    parser.add_argument("--skip", type=str, help="Saltar pasos especificos (separados por coma)")
    parser.add_argument("--list", action="store_true", help="Listar todos los pasos")
    parser.add_argument("--config", type=str, default="ETL/config/etl_steps.yaml", help="Ruta al archivo de configuracion")

    args = parser.parse_args()

    # Cargar configuracion
    config_path = PROJECT_ROOT / args.config
    if not config_path.exists():
        print(f"Error: No se encontro el archivo de configuracion: {config_path}")
        sys.exit(1)

    steps = load_config(config_path)

    # Listar pasos
    if args.list:
        list_steps(steps)
        sys.exit(0)

    # Validar ano
    if not args.year:
        parser.error("--year es requerido (excepto con --list)")

    # Variables para sustitucion
    variables = {
        "year": str(args.year),
    }

    # Filtros
    only = set(args.only.split(",")) if args.only else None
    skip = set(args.skip.split(",")) if args.skip else None

    # Obtener orden de ejecucion
    try:
        execution_order = get_execution_order(steps, only, skip)
    except ValueError as e:
        print(f"Error en configuracion: {e}")
        sys.exit(1)

    if not execution_order:
        print("No hay pasos para ejecutar")
        sys.exit(0)

    # Mostrar plan
    print("\n" + "=" * 60)
    print(f"ETL BDNS - Ano {args.year}")
    print("=" * 60)
    print(f"\nPasos a ejecutar ({len(execution_order)}):")
    for i, step in enumerate(execution_order, 1):
        print(f"  {i}. {step.name}")

    if args.dry_run:
        print("\n[DRY-RUN MODE] No se ejecutaran los pasos")

    # Ejecutar pasos
    results: List[StepResult] = []
    failed_steps: Set[str] = set()
    start_time = datetime.now()

    for step in execution_order:
        # Verificar si dependencias fallaron
        deps_failed = any(dep in failed_steps for dep in step.depends_on)

        if deps_failed:
            print(f"\n[SKIP] {step.name} - dependencias fallaron")
            results.append(StepResult(
                name=step.name,
                status="SKIPPED",
                duration=0,
                message="Dependencias fallaron"
            ))
            if step.on_error == OnError.SKIP_DEPENDENTS:
                failed_steps.add(step.name)
            continue

        result = run_step(step, variables, args.dry_run)
        results.append(result)

        if result.status == "FAILED":
            failed_steps.add(step.name)

            if step.on_error == OnError.STOP:
                print(f"\n[STOP] Deteniendo ETL por error en {step.name}")
                break

    # Resumen
    total_duration = (datetime.now() - start_time).total_seconds()

    print("\n" + "=" * 60)
    print("RESUMEN ETL")
    print("=" * 60)

    success = sum(1 for r in results if r.status == "SUCCESS")
    failed = sum(1 for r in results if r.status == "FAILED")
    skipped = sum(1 for r in results if r.status == "SKIPPED")

    print(f"Total:    {len(results)} pasos")
    print(f"Exitosos: {success}")
    print(f"Fallidos: {failed}")
    print(f"Saltados: {skipped}")
    print(f"Duracion: {total_duration:.1f}s")

    if failed > 0:
        print("\nPasos fallidos:")
        for r in results:
            if r.status == "FAILED":
                print(f"  - {r.name}: {r.message}")

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
